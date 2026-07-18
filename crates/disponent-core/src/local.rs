//! The local backend: run the agent on this machine, in tmux — the same
//! shape as an exe.dev worker but the "environment" is a managed work dir
//! plus a `tmux -L disponent` session named after the session uid. START sets
//! the work dir up and opens a shell; the [`AgentAdapter`](crate::agent::AgentAdapter)
//! then launches the agent into that shell (its [`Compute`] surface).
//! `interrupt` stops the agent's work, `kill` ends the tmux session and keeps
//! the work dir for inspection; REAP removes the work dir too. Survey lists the
//! tmux sessions, so reconcile adopts local runs a previous disponent left behind.
//!
//! ## The holder path (M1, `notes/owning-the-terminal.md` §5/§9)
//!
//! Gated by `DISPONENT_LOCAL_HOLDER`, the local backend runs the agent under the
//! first-party pty holder (`disponent hold`) instead of a `tmux` session. The
//! clone/worktree/setup/runner machinery is **shared byte-for-byte** — only the
//! final launch differs (a `disponent hold … -- <runner>` daemon instead of
//! `tmux new-session … <runner>`), and [`compute`](LocalTmux::compute) hands
//! back a [`HolderCompute`] (dial the socket for send/capture/observe/kill)
//! instead of a [`LocalCompute`] (shell out to `tmux`). tmux stays the default;
//! the flag only selects the alternative, so the tmux path is untouched.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use serde_json::json;

use crate::backend::{
    run_argv, shq, Compute, EnvProvider, Provision, StartRequest, TemplateHandle, TemplateSpec,
};
use crate::observe::{StreamChunk, TerminalStream};

/// tmux session names for disponent workers: `dsp-<session uid>`.
const SESSION_PREFIX: &str = "dsp-";

#[derive(Clone)]
pub struct LocalTmux {
    /// The tmux socket name (`tmux -L …`) — separate from the user's own server.
    socket: String,
    /// Work dirs live here, one per session uid.
    root: PathBuf,
    /// The agent command line; the brief is appended as its final argument.
    agent_cmd: String,
    dry_run: bool,
    /// Run the agent under the first-party pty holder instead of tmux
    /// (`DISPONENT_LOCAL_HOLDER`, M1). Off by default — tmux stays the default
    /// backend; the flag only selects the alternative launch + Compute surface.
    holder: bool,
}

impl LocalTmux {
    /// The real backend, `DISPONENT_LOCAL_*` env overrides honored.
    pub fn from_env() -> Self {
        let var = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        LocalTmux {
            socket: var("DISPONENT_LOCAL_SOCKET", "disponent"),
            root: PathBuf::from(var(
                "DISPONENT_LOCAL_ROOT",
                &format!("{home}/.disponent/work"),
            )),
            agent_cmd: var(
                "DISPONENT_LOCAL_AGENT",
                &format!(
                    "claude {}",
                    var("DISPONENT_CLAUDE_FLAGS", "--dangerously-skip-permissions")
                ),
            ),
            dry_run: std::env::var("DISPONENT_LOCAL_DRY_RUN").is_ok(),
            // Any non-empty value opts in (design names `DISPONENT_LOCAL_HOLDER=dsp-hold`).
            holder: std::env::var("DISPONENT_LOCAL_HOLDER")
                .map(|v| !v.is_empty())
                .unwrap_or(false),
        }
    }

    /// Every command fabricated, nothing spawned — the engine tests' backend.
    pub fn dry_run() -> Self {
        LocalTmux {
            dry_run: true,
            ..LocalTmux::from_env()
        }
    }

    /// A real backend sandboxed for tests: own socket + root, a stand-in agent.
    pub fn sandboxed(socket: &str, root: PathBuf, agent_cmd: &str) -> Self {
        LocalTmux {
            socket: socket.to_string(),
            root,
            agent_cmd: agent_cmd.to_string(),
            dry_run: false,
            holder: false,
        }
    }

    /// A real, holder-backed backend sandboxed for tests: the M1 path with its
    /// own root (holder sockets live under `<root>/sock`).
    pub fn sandboxed_holder(root: PathBuf, agent_cmd: &str) -> Self {
        LocalTmux {
            socket: "disponent".to_string(),
            root,
            agent_cmd: agent_cmd.to_string(),
            dry_run: false,
            holder: true,
        }
    }

    /// Whether the holder path is selected (`DISPONENT_LOCAL_HOLDER`).
    pub fn uses_holder(&self) -> bool {
        self.holder
    }

    /// Where per-session holder sockets live: `<root>/sock/<uid>.sock`. Kept
    /// under the managed root so it's self-contained (reap/tests) and distinct
    /// from the tmux socket namespace.
    fn holder_dir(&self) -> PathBuf {
        self.root.join("sock")
    }

    /// The holder-path handle: names the socket to dial + the work dir, and
    /// marks `holder` so [`compute`](LocalTmux::compute) picks [`HolderCompute`].
    fn holder_handle(&self, uid: &str) -> serde_json::Value {
        json!({
            "holder": true,
            "holderSock": self.holder_dir().join(format!("{uid}.sock")),
            "workDir": self.root.join(uid),
        })
    }

    /// Launch the agent under `disponent hold` (daemonized, reparented to init)
    /// instead of a tmux session — the M1 swap. Reuses the same `runner` the
    /// tmux path built; only the launcher differs. The `disponent` binary is the
    /// currently-running executable (honest error if it can't be located).
    fn launch_holder(&self, uid: &str, runner: &Path) -> anyhow::Result<()> {
        let exe =
            std::env::current_exe().context("locate the disponent binary to launch the holder")?;
        let sock_dir = self.holder_dir();
        std::fs::create_dir_all(&sock_dir)
            .with_context(|| format!("mkdir {}", sock_dir.display()))?;
        // `--daemonize` double-forks: the process we spawn exits 0 immediately
        // and the reparented grandchild holds the pty. Null stdio so `status()`
        // doesn't block on the daemon keeping the inherited pipe write-ends open.
        let status = Command::new(&exe)
            .arg("hold")
            .arg(uid)
            .arg("--daemonize")
            .arg("--socket-dir")
            .arg(&sock_dir)
            .arg("--")
            .arg(runner)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| anyhow!("launch holder ({}): {e}", exe.display()))?;
        if !status.success() {
            bail!("`disponent hold {uid}` exited {status}");
        }
        // The daemon binds its socket asynchronously; wait (bounded) for it so
        // the engine can dial immediately after START returns.
        let sock = sock_dir.join(format!("{uid}.sock"));
        for _ in 0..250 {
            if sock.exists() {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        bail!("holder socket {} never appeared", sock.display())
    }

    fn tmux(&self, args: &[&str]) -> anyhow::Result<String> {
        let mut argv = vec!["tmux".to_string(), "-L".to_string(), self.socket.clone()];
        argv.extend(args.iter().map(|s| s.to_string()));
        run_argv(&argv, None)
    }

    fn session_name(uid: &str) -> String {
        format!("{SESSION_PREFIX}{uid}")
    }

    fn handle(&self, uid: &str) -> serde_json::Value {
        json!({
            "tmux": Self::session_name(uid),
            "socket": self.socket,
            "workDir": self.root.join(uid),
        })
    }

    /// A snapshot of the worker's terminal (poll-grade observation, scraped).
    /// Kept as an inherent method so tests can probe a handle directly.
    pub fn capture(&self, handle: &serde_json::Value) -> anyhow::Result<String> {
        if self.dry_run {
            return Ok(String::new());
        }
        let name = handle["tmux"]
            .as_str()
            .ok_or_else(|| anyhow!("handle has no 'tmux': {handle}"))?;
        self.tmux(&["capture-pane", "-p", "-t", name])
    }
}

/// `owner/repo` is a gh slug; anything with a scheme, a colon, or an existing
/// path is for `git clone` directly.
fn is_gh_slug(repo: &str) -> bool {
    repo.split('/').count() == 2
        && !repo.contains(':')
        && !repo.starts_with('.')
        && !repo.starts_with('/')
        && !std::path::Path::new(repo).exists()
}

/// If `repo` names a local git repository (a `/`, `.` or `~` path, or an
/// existing dir, that holds a `.git`), its canonical path — else None. This is
/// the only shape `git worktree add` can apply to; a remote slug/URL can't.
fn local_git_repo(repo: &str) -> Option<PathBuf> {
    let expanded = match repo.strip_prefix("~/") {
        Some(rest) => PathBuf::from(std::env::var("HOME").ok()?).join(rest),
        None => PathBuf::from(repo),
    };
    let canon = std::fs::canonicalize(&expanded).ok()?;
    canon.join(".git").exists().then_some(canon)
}

impl EnvProvider for LocalTmux {
    fn kind(&self) -> &'static str {
        "local"
    }

    fn requires_template(&self) -> bool {
        false
    }

    fn ensure_template(&self, spec: &TemplateSpec) -> anyhow::Result<TemplateHandle> {
        // Local runs on this machine — there's no image to build, so TEMPLATE is
        // an honest no-op that names the (unused) template through.
        Ok(TemplateHandle {
            name: spec.name.clone(),
        })
    }

    fn start(&self, req: &StartRequest) -> anyhow::Result<Provision> {
        // The handle shape follows the selected launcher: a holder handle names
        // the socket to dial, a tmux handle names the session.
        let handle = if self.holder {
            self.holder_handle(&req.session_uid)
        } else {
            self.handle(&req.session_uid)
        };
        if self.dry_run {
            return Ok(Provision { handle, url: None });
        }

        let mut handle = handle;
        let work = self.root.join(&req.session_uid);
        let task = work.join("task");
        std::fs::create_dir_all(&task).with_context(|| format!("mkdir {}", task.display()))?;
        std::fs::write(work.join("brief.md"), &req.brief)?;

        let sh = |script: &str, dir: &std::path::Path, stage: &str| -> anyhow::Result<()> {
            let out = Command::new("bash")
                .arg("-c")
                .arg(script)
                .current_dir(dir)
                .output()
                .map_err(|e| anyhow!("{stage}: spawn bash: {e}"))?;
            if !out.status.success() {
                bail!("{stage}: {}", String::from_utf8_lossy(&out.stderr).trim());
            }
            Ok(())
        };
        // A predicate variant: did the command succeed? (used to probe which
        // start-point a fetch_remote worktree should cut off).
        let sh_ok = |script: &str, dir: &std::path::Path| -> bool {
            Command::new("bash")
                .arg("-c")
                .arg(script)
                .current_dir(dir)
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false)
        };

        // clone (or worktree) → setup, the same order as the remote setup. The
        // agent is NOT launched here — the engine `spawn`s it onto the shell this
        // opens. `isolation: "worktree"` on a LOCAL git repo adds a worktree off
        // it instead of cloning; a fresh branch per session (`disponent/<uid>`,
        // unique because uids are UUIDv7) keeps it isolated and removable on
        // reap. A worktree requested against a remote repo can't apply — a fresh
        // clone is still an isolated dir, so fall through honestly rather than
        // pretend it's a worktree.
        let want_worktree = req.isolation.as_deref() == Some("worktree");
        if let Some(repo) = req.repo.as_deref().filter(|r| !r.is_empty()) {
            match want_worktree.then(|| local_git_repo(repo)).flatten() {
                Some(parent) => {
                    // git worktree add needs the target to not already exist.
                    std::fs::remove_dir_all(&task).ok();
                    let parent_s = parent.display().to_string();
                    let task_s = task.display().to_string();
                    let git_ref = req.git_ref.as_deref().filter(|r| !r.is_empty());
                    let cmd = if req.fetch_remote {
                        // Teleport path: fetch the branch from the workspace's
                        // origin and cut the worktree off THAT remote tip (pm's
                        // worktreeAddRemote), not off local HEAD — so a branch
                        // that only exists on the remote is checked out at its
                        // pushed tip. Needs a named ref (the branch to fetch).
                        let branch = git_ref.ok_or_else(|| {
                            anyhow!("fetch_remote worktree needs a git_ref (the branch to fetch)")
                        })?;
                        sh(
                            &format!("git -C {} fetch origin {}", shq(&parent_s), shq(branch)),
                            &work,
                            "fetch",
                        )?;
                        let ref_exists = |full: &str| {
                            sh_ok(
                                &format!(
                                    "git -C {} show-ref --verify --quiet {}",
                                    shq(&parent_s),
                                    shq(full)
                                ),
                                &work,
                            )
                        };
                        if ref_exists(&format!("refs/heads/{branch}")) {
                            // an existing local branch — check it out as-is
                            format!(
                                "git -C {} worktree add {} {}",
                                shq(&parent_s),
                                shq(&task_s),
                                shq(branch),
                            )
                        } else if ref_exists(&format!("refs/remotes/origin/{branch}")) {
                            // create the local branch off the fetched origin ref
                            format!(
                                "git -C {} worktree add -b {} {} {}",
                                shq(&parent_s),
                                shq(branch),
                                shq(&task_s),
                                shq(&format!("origin/{branch}")),
                            )
                        } else {
                            // origin doesn't have it either — fall back to a
                            // branch off HEAD, honestly (no remote tip to use).
                            format!(
                                "git -C {} worktree add -B {} {}",
                                shq(&parent_s),
                                shq(branch),
                                shq(&task_s),
                            )
                        }
                    } else {
                        // Default: a named git_ref selects the worktree's branch
                        // with `-B` (create-or-reset off HEAD, so re-dispatching
                        // the same ref doesn't fail on an existing branch); no
                        // ref → a fresh, uid-unique `disponent/<uid>` branch via
                        // `-b`.
                        let branch_flag = match git_ref {
                            Some(git_ref) => format!("-B {}", shq(git_ref)),
                            None => {
                                format!("-b {}", shq(&format!("disponent/{}", req.session_uid)))
                            }
                        };
                        format!(
                            "git -C {} worktree add {} {}",
                            shq(&parent_s),
                            branch_flag,
                            shq(&task_s),
                        )
                    };
                    sh(&cmd, &work, "worktree")?;
                    // Record the parent repo so reap can deregister the worktree
                    // (a bare rm -rf would leave a dangling registration).
                    handle["worktreeRepo"] = json!(parent.display().to_string());
                }
                None => {
                    let clone = if is_gh_slug(repo) {
                        format!("gh repo clone {} task", shq(repo))
                    } else {
                        format!("git clone {} task", shq(repo))
                    };
                    std::fs::remove_dir_all(&task).ok();
                    sh(&clone, &work, "clone")?;
                }
            }
        }
        if let Some(setup) = req.setup.as_deref().filter(|s| !s.is_empty()) {
            sh(setup, &task, "setup")?;
        }

        // The runner opens a shell in the task dir with telemetry wired, then
        // `exec bash` so the pane stays interactive — the engine's agent launch
        // rides in as keystrokes afterward (the brief is `cat`-ed at that point,
        // never on the tmux command string), mirroring the remote convention.
        let otel = req
            .otel_endpoint
            .as_deref()
            .map(|e| format!("{}\n", crate::otel::worker_env(e, &req.session_uid)))
            .unwrap_or_default();
        let runner = work.join("run.sh");
        std::fs::write(
            &runner,
            format!("#!/usr/bin/env bash\ncd \"$(dirname \"$0\")/task\"\n{otel}exec bash\n"),
        )?;
        sh(
            &format!("chmod +x {}", shq(&runner.display().to_string())),
            &work,
            "runner",
        )?;

        // The only step that differs between the tmux and holder paths: same
        // runner, different holder of the pty. tmux stays the default.
        if self.holder {
            self.launch_holder(&req.session_uid, &runner)?;
        } else {
            self.tmux(&[
                "new-session",
                "-d",
                "-s",
                &Self::session_name(&req.session_uid),
                "-x",
                "220",
                "-y",
                "50",
                &runner.display().to_string(),
            ])?;
        }
        Ok(Provision { handle, url: None })
    }

    fn compute(&self, handle: &serde_json::Value) -> anyhow::Result<Box<dyn Compute>> {
        // Key off the handle, not `self.holder`: a session provisioned under one
        // launcher is always driven by the matching Compute, even if the flag
        // flipped since (reconcile-adopted sessions carry their own handle).
        if handle.get("holder").and_then(|v| v.as_bool()) == Some(true) {
            return Ok(Box::new(HolderCompute {
                dev: self.clone(),
                handle: handle.clone(),
            }));
        }
        Ok(Box::new(LocalCompute {
            dev: self.clone(),
            handle: handle.clone(),
        }))
    }

    fn launch_spec(&self, _req: &StartRequest) -> Option<crate::agent::LaunchSpec> {
        // The claude-code adapter composes the command; the env supplies its
        // config — the agent command line and where START wrote the brief. The
        // adapter's launch is `spawn`ed (as keystrokes) into the shell START
        // opened, with the brief `cat`-ed from the work dir at run time.
        Some(crate::agent::LaunchSpec {
            agent_cmd: self.agent_cmd.clone(),
            brief_ref: "\"$(cat ../brief.md)\"".to_string(),
        })
    }

    fn reap(&self, handle: &serde_json::Value) -> anyhow::Result<()> {
        if self.dry_run {
            return Ok(());
        }
        if let Some(dir) = handle["workDir"].as_str() {
            // Only remove dirs we manage — never follow a doctored handle
            // outside the root.
            let dir = PathBuf::from(dir);
            if dir.starts_with(&self.root) && dir != self.root {
                // A worktree session's task dir is registered in the parent
                // repo — deregister it there first, so a plain rm doesn't leave
                // a dangling worktree behind. (The branch stays: it holds the
                // agent's committed work for the operator to keep or discard.)
                if let Some(repo) = handle["worktreeRepo"].as_str() {
                    let task = dir.join("task").display().to_string();
                    let git = |args: &[&str]| {
                        let mut argv = vec!["git".to_string(), "-C".to_string(), repo.to_string()];
                        argv.extend(args.iter().map(|s| s.to_string()));
                        let _ = run_argv(&argv, None);
                    };
                    git(&["worktree", "remove", "--force", &task]);
                    git(&["worktree", "prune"]);
                }
                std::fs::remove_dir_all(&dir).ok();
            }
        }
        Ok(())
    }

    /// Diff the session's work dir to see whether it shipped anything. The task
    /// dir (`<workDir>/task`) is a git repo (a clone or a worktree); a change
    /// is either uncommitted tree changes or commits the session's branch made
    /// past the base it forked from. A pure-prompt session with no git repo has
    /// no baseline to diff — return `None` (honest omission) rather than guess.
    fn delivery_signal(&self, handle: &serde_json::Value) -> Option<bool> {
        // No work dir (dry-run, or a doctored handle) = nothing to diff.
        let work = handle["workDir"].as_str()?;
        let task = PathBuf::from(work).join("task");
        // Only a git work dir carries a baseline; without `.git` we can't judge.
        if !task.join(".git").exists() {
            return None;
        }
        let git = |args: &[&str]| -> Option<String> {
            let mut argv = vec![
                "git".to_string(),
                "-C".to_string(),
                task.display().to_string(),
            ];
            argv.extend(args.iter().map(|s| s.to_string()));
            run_argv(&argv, None).ok()
        };
        // Uncommitted work in the tree is the plainest evidence of a diff.
        if !git(&["status", "--porcelain"])?.trim().is_empty() {
            return Some(true);
        }
        // Committed work: HEAD sits on a commit no OTHER branch/remote ref
        // reaches — i.e. the session's branch advanced past its fork point. If
        // some other ref still contains HEAD, the branch never moved.
        let head_ref = git(&["symbolic-ref", "--quiet", "HEAD"]).unwrap_or_default();
        let head_ref = head_ref.trim();
        let contains = git(&[
            "for-each-ref",
            "--contains",
            "HEAD",
            "--format=%(refname)",
            "refs/heads",
            "refs/remotes",
        ])?;
        let advanced = contains
            .lines()
            .map(str::trim)
            .filter(|r| !r.is_empty())
            .all(|r| r == head_ref);
        Some(advanced)
    }

    fn survey(&self) -> anyhow::Result<Vec<(String, serde_json::Value)>> {
        if self.dry_run {
            return Ok(vec![]);
        }
        // Holder path: the resident holders are the `<uid>.sock` files in the
        // socket dir — scan it the way the tmux path scrapes `tmux ls`. A missing
        // dir means nothing running (not an error).
        if self.holder {
            let Ok(entries) = std::fs::read_dir(self.holder_dir()) else {
                return Ok(vec![]);
            };
            return Ok(entries
                .filter_map(|e| {
                    let name = e.ok()?.file_name();
                    let uid = name.to_str()?.strip_suffix(".sock")?.to_string();
                    let handle = self.holder_handle(&uid);
                    Some((uid, handle))
                })
                .collect());
        }
        // No tmux server on this socket = nothing running (not an error).
        let Ok(listing) = self.tmux(&["list-sessions", "-F", "#S"]) else {
            return Ok(vec![]);
        };
        Ok(listing
            .lines()
            .filter_map(|name| name.strip_prefix(SESSION_PREFIX))
            .map(|uid| (uid.to_string(), self.handle(uid)))
            .collect())
    }
}

/// The agent's working directory (`<workDir>/task`) from a session handle — the
/// shared task-dir resolution for one-shot `run`, used by both the tmux and
/// holder backends.
fn task_dir(handle: &serde_json::Value) -> anyhow::Result<PathBuf> {
    handle
        .get("workDir")
        .and_then(|d| d.as_str())
        .map(|d| PathBuf::from(d).join("task"))
        .ok_or_else(|| anyhow!("handle has no 'workDir': {}", handle))
}

/// Run a one-shot `bash -c <cmd>` in `task` (the agent's working directory),
/// returning trimmed stdout — the shared `run` mechanism for both backends.
fn run_oneshot(task: &Path, cmd: &str) -> anyhow::Result<String> {
    let out = Command::new("bash")
        .arg("-c")
        .arg(cmd)
        .current_dir(task)
        .output()
        .map_err(|e| anyhow!("run: spawn bash: {e}"))?;
    if !out.status.success() {
        bail!("run: {}", String::from_utf8_lossy(&out.stderr).trim());
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// The INTERACT surface for one local worker: a clone of the backend config
/// plus the worker's handle (its `tmux` session name + `workDir`).
struct LocalCompute {
    dev: LocalTmux,
    handle: serde_json::Value,
}

impl LocalCompute {
    fn session(&self) -> anyhow::Result<&str> {
        self.handle["tmux"]
            .as_str()
            .ok_or_else(|| anyhow!("handle has no 'tmux': {}", self.handle))
    }

    /// Type a line into the pane and press Enter — the shared mechanism for
    /// spawning the agent and for relaying supervisor input.
    fn send_line(&self, line: &str) -> anyhow::Result<()> {
        if self.dev.dry_run {
            return Ok(());
        }
        let name = self.session()?;
        self.dev.tmux(&["send-keys", "-t", name, "-l", line])?;
        self.dev
            .tmux(&["send-keys", "-t", name, "Enter"])
            .map(|_| ())
    }
}

impl Compute for LocalCompute {
    fn run(&self, cmd: &str) -> anyhow::Result<String> {
        if self.dev.dry_run {
            return Ok(String::new());
        }
        // One-shot in the session's task dir (the agent's working directory).
        let task = task_dir(&self.handle)?;
        run_oneshot(&task, cmd)
    }

    fn spawn(&self, cmd: &str) -> anyhow::Result<()> {
        // Launch the agent as a foreground process in the pane's shell.
        self.send_line(cmd) // straitjacket-allow:duplication — parallel Compute trait impls; transport differs, boilerplate intentionally mirrors HolderCompute
    }

    fn send(&self, input: &str) -> anyhow::Result<()> {
        self.send_line(input)
    }

    fn capture(&self) -> anyhow::Result<String> {
        LocalTmux::capture(&self.dev, &self.handle)
    }

    fn interrupt(&self) -> anyhow::Result<()> {
        if self.dev.dry_run {
            return Ok(());
        }
        // Interrupt the agent (C-c); the pane's shell survives and the work dir
        // is untouched.
        let name = self.session()?;
        self.dev.tmux(&["send-keys", "-t", name, "C-c"]).map(|_| ())
    }

    fn kill(&self) -> anyhow::Result<()> {
        if self.dev.dry_run {
            return Ok(());
        }
        let name = self.session()?;
        // Already-gone is stopped enough.
        let _ = self.dev.tmux(&["kill-session", "-t", name]);
        Ok(())
    }

    fn workspace_link(&self) -> anyhow::Result<Option<String>> {
        // The agent runs in <workDir>/task (START creates and cds there), so
        // that's what the editor should open. A doctored/absent handle yields no
        // link rather than a panic.
        let Some(work_dir) = self.handle["workDir"].as_str() else {
            return Ok(None);
        };
        Ok(Some(format!("vscode://file{work_dir}/task")))
    }
}

/// The INTERACT surface for a holder-backed local worker (M1): dial the pty
/// holder's socket for each verb. The agent runs under `disponent hold`, so
/// `send`/`spawn` write `Input` frames, `capture` renders the byte-exact ring,
/// `observe_stream` is the live exact channel, `interrupt` is `C-c` (`0x03`),
/// and `kill` is a `Signal` control frame — no `tmux` on this path.
///
/// Roles (M2a, design §6): the resident `observe_stream` and `capture` dial as
/// **readers** so they never contend for the writer; the input-bearing verbs
/// (`send`/`spawn`/`interrupt`) take the single **writer** lock momentarily and
/// fail honestly if a human interactive attacher holds it. `kill`'s `Signal` is
/// ungated, so a reader connection stops a session even under a human writer.
struct HolderCompute {
    dev: LocalTmux,
    handle: serde_json::Value,
}

impl HolderCompute {
    fn sock(&self) -> anyhow::Result<PathBuf> {
        self.handle["holderSock"]
            .as_str()
            .map(PathBuf::from)
            .ok_or_else(|| anyhow!("handle has no 'holderSock': {}", self.handle))
    }

    /// A reader connection — the read-only role (`capture`, `observe_stream`,
    /// and the ungated `kill` control frame). Never contends for the writer.
    fn connect(&self) -> anyhow::Result<disponent_hold::Client> {
        disponent_hold::Client::connect(&self.sock()?)
    }

    /// A momentary **writer** connection for input-bearing verbs (`send`,
    /// `spawn`, `interrupt`). Errors honestly if a human interactive attacher
    /// holds the writer lock ("writer channel held by an interactive
    /// attacher") — the design's reject-with-reason rule, not a silent drop.
    fn connect_writer(&self) -> anyhow::Result<disponent_hold::Client> {
        disponent_hold::Client::connect_writer(&self.sock()?)
    }

    /// The agent's working directory (`<workDir>/task`), for one-shot `run`.
    fn task_dir(&self) -> anyhow::Result<PathBuf> {
        task_dir(&self.handle)
    }

    /// Write a line to the held shell + newline — the holder analogue of tmux
    /// `send-keys -l <line> Enter`, the shared mechanism for spawning the agent
    /// and relaying supervisor input.
    fn send_line(&self, line: &str) -> anyhow::Result<()> {
        if self.dev.dry_run {
            return Ok(());
        }
        let mut bytes = line.as_bytes().to_vec();
        bytes.push(b'\n');
        // Take the writer lock momentarily; it releases when this connection
        // drops. Fails honestly if a human holds it.
        self.connect_writer()?.send_input(&bytes)
    }
}

impl Compute for HolderCompute {
    fn run(&self, cmd: &str) -> anyhow::Result<String> {
        if self.dev.dry_run {
            return Ok(String::new());
        }
        // One-shot in the session's task dir — independent of the held pty,
        // exactly as the tmux path's `run` shells out to bash.
        let task = self.task_dir()?;
        run_oneshot(&task, cmd)
    }

    fn spawn(&self, cmd: &str) -> anyhow::Result<()> {
        // Type the composed agent command into the held shell (the holder execs
        // the runner's `bash`), mirroring the tmux path.
        self.send_line(cmd)
    }

    fn send(&self, input: &str) -> anyhow::Result<()> {
        self.send_line(input)
    }

    fn capture(&self) -> anyhow::Result<String> {
        if self.dev.dry_run {
            return Ok(String::new());
        }
        // The holder replays its ring as `Data` frames on connect; read until
        // the stream goes quiet (a short read timeout) — that drained ring is
        // the current byte-exact snapshot, the scraped-shaped back-compat view.
        let mut c = self.connect()?;
        c.set_read_timeout(Some(Duration::from_millis(150)))?;
        let mut acc: Vec<u8> = Vec::new();
        loop {
            match c.read_frame() {
                Ok(Some(disponent_hold::ServerFrame::Data(d))) => acc.extend_from_slice(&d),
                Ok(Some(disponent_hold::ServerFrame::Heartbeat)) => {}
                Ok(Some(disponent_hold::ServerFrame::Exit(_))) | Ok(None) => break,
                Err(e) => {
                    // A read timeout (WouldBlock/TimedOut) is "ring drained" — the
                    // normal end of a snapshot, not a fault.
                    if let Some(io) = e.downcast_ref::<std::io::Error>() {
                        use std::io::ErrorKind::{TimedOut, WouldBlock};
                        if matches!(io.kind(), WouldBlock | TimedOut) {
                            break;
                        }
                    }
                    return Err(e);
                }
            }
        }
        Ok(String::from_utf8_lossy(&acc).into_owned())
    }

    fn observe_stream(&self) -> anyhow::Result<Option<TerminalStream>> {
        if self.dev.dry_run {
            return Ok(None);
        }
        // A dedicated reader connection forwards holder frames onto a channel;
        // the stream drops → the sender drops → the reader exits on the next
        // frame (heartbeats bound the wait to ~5s even on an idle session).
        let client = self.connect()?;
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || holder_stream_reader(client, tx));
        Ok(Some(TerminalStream::new(rx)))
    }

    fn observes_exact(&self) -> bool {
        // The holder reads the pty byte-exact — this surface is the exact tier.
        true
    }

    fn interrupt(&self) -> anyhow::Result<()> {
        if self.dev.dry_run {
            return Ok(());
        }
        // C-c (0x03) on the pty; the held shell survives, the work dir untouched.
        // Typing C-c is a writer act — take the lock momentarily (fails honestly
        // if a human holds it).
        self.connect_writer()?.interrupt()
    }

    fn kill(&self) -> anyhow::Result<()> {
        if self.dev.dry_run {
            return Ok(());
        }
        // SIGKILL the child's process group via the holder control frame — the
        // holder itself lingers until reaped (design §5), same as a tmux pane's
        // shell surviving `kill-session`'s target process. `Signal` is ungated
        // by the writer lock, so kill works even while a human holds the writer;
        // a plain reader connection suffices.
        self.connect()?.kill()
    }

    fn workspace_link(&self) -> anyhow::Result<Option<String>> {
        let Some(work_dir) = self.handle["workDir"].as_str() else {
            return Ok(None);
        };
        Ok(Some(format!("vscode://file{work_dir}/task")))
    }
}

/// Drain a holder connection's server frames onto a [`StreamChunk`] channel
/// until the child exits or the consumer goes away.
fn holder_stream_reader(mut client: disponent_hold::Client, tx: mpsc::Sender<StreamChunk>) {
    loop {
        match client.read_frame() {
            Ok(Some(disponent_hold::ServerFrame::Data(d))) => {
                if tx.send(StreamChunk::Data(d)).is_err() {
                    break;
                }
            }
            Ok(Some(disponent_hold::ServerFrame::Heartbeat)) => {
                // Forward as liveness so a dropped receiver is noticed promptly.
                if tx.send(StreamChunk::Heartbeat).is_err() {
                    break;
                }
            }
            Ok(Some(disponent_hold::ServerFrame::Exit(e))) => {
                let chunk = match e {
                    disponent_hold::Exit::Code(c) => StreamChunk::Exit {
                        code: Some(c),
                        signal: None,
                    },
                    disponent_hold::Exit::Signal(s) => StreamChunk::Exit {
                        code: None,
                        signal: Some(s),
                    },
                };
                let _ = tx.send(chunk);
                break;
            }
            Ok(None) | Err(_) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn gh_slugs_vs_clonable_urls_and_paths() {
        assert!(is_gh_slug("zmaril/entl"));
        assert!(!is_gh_slug("https://github.com/zmaril/entl"));
        assert!(!is_gh_slug("git@github.com:zmaril/entl.git"));
        assert!(!is_gh_slug("./some/local"));
        assert!(!is_gh_slug("/abs/path"));
        assert!(!is_gh_slug("plain"));
    }

    #[test]
    fn handles_name_the_session_and_stay_under_root() {
        let b = LocalTmux::dry_run();
        let h = b.handle("abc-123");
        assert_eq!(h["tmux"], "dsp-abc-123");
        assert!(h["workDir"].as_str().unwrap().ends_with("abc-123"));
    }

    // Run git in `dir`, panicking (with output) on failure. `-c user.*` keeps
    // commits working on a machine with no git identity configured.
    fn git(dir: &std::path::Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .args(["-c", "user.email=t@t", "-c", "user.name=t"])
            .args(args)
            .current_dir(dir)
            .output()
            .expect("spawn git");
        assert!(
            out.status.success(),
            "git {args:?} in {}: {}",
            dir.display(),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    // A `fetch_remote` worktree dispatch must fetch the named branch from the
    // workspace's origin and cut the worktree off THAT remote tip — the pushed
    // commit, not local HEAD (pm's teleport provisioning). Transport-agnostic:
    // this is the tmux path, but the git plumbing is what's under test.
    #[test]
    fn fetch_remote_worktree_checks_out_the_pushed_ref_not_local_head() {
        let base = std::env::temp_dir().join(format!("dsp-fetchremote-{}", Uuid::now_v7()));
        let seed = base.join("seed");
        let clone = base.join("clone");
        std::fs::create_dir_all(&base).unwrap();

        // A bare "origin" plus a seed working repo: base commit on `main`
        // (pushed), then a distinctive marker on branch `pm/task-1-demo`
        // (pushed). The clone is taken BEFORE the branch exists on the remote,
        // so it can only reach the branch by fetching — proving the fetch path.
        git(&base, &["init", "--bare", "-b", "main", "remote.git"]);
        git(&base, &["init", "-b", "main", "seed"]);
        std::fs::write(seed.join("base.txt"), "base").unwrap();
        git(&seed, &["add", "-A"]);
        git(&seed, &["commit", "-m", "base"]);
        git(&seed, &["remote", "add", "origin", "../remote.git"]);
        git(&seed, &["push", "-u", "origin", "main"]);

        // Clone now — the clone knows only `main`, not the branch we push next.
        git(&base, &["clone", "remote.git", "clone"]);
        let clone_main_head = git(&clone, &["rev-parse", "HEAD"]);

        git(&seed, &["checkout", "-b", "pm/task-1-demo"]);
        std::fs::write(seed.join("TELEPORT_MARKER.txt"), "teleport").unwrap();
        git(&seed, &["add", "-A"]);
        git(&seed, &["commit", "-m", "marker"]);
        git(&seed, &["push", "origin", "pm/task-1-demo"]);
        let pushed_head = git(&seed, &["rev-parse", "HEAD"]);

        // Provision a worktree dispatch off the CLONE (its origin is the bare
        // repo). A sandboxed tmux backend on its own socket; a stub agent (the
        // agent isn't launched by start()).
        let socket = format!("dsp-fr-{}", std::process::id());
        let root = base.join("work");
        let backend = LocalTmux::sandboxed(&socket, root.clone(), "echo stub-agent");
        let uid = Uuid::now_v7().to_string();
        let req = StartRequest {
            session_uid: uid.clone(),
            template: None,
            repo: Some(clone.display().to_string()),
            isolation: Some("worktree".into()),
            git_ref: Some("pm/task-1-demo".into()),
            fetch_remote: true,
            setup: None,
            brief: "b".into(),
            otel_endpoint: None,
        };

        let provisioned = backend.start(&req).expect("start");
        // Tear the tmux session + worktree down no matter what the asserts do.
        let cleanup = || {
            let _ = Command::new("tmux")
                .args(["-L", &socket, "kill-server"])
                .output();
            let _ = backend.reap(&provisioned.handle);
            let _ = std::fs::remove_dir_all(&base);
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let task = root.join(&uid).join("task");
            // fetch + off-origin: the worktree carries the PUSHED commit (marker
            // present) and its HEAD is the pushed tip — NOT the clone's local
            // `main` HEAD (which never saw the marker).
            assert!(
                task.join("TELEPORT_MARKER.txt").exists(),
                "worktree should hold the branch's pushed marker file"
            );
            let worktree_head = git(&task, &["rev-parse", "HEAD"]);
            assert_eq!(
                worktree_head, pushed_head,
                "worktree HEAD is the pushed tip"
            );
            assert_ne!(
                worktree_head, clone_main_head,
                "worktree HEAD must not be the clone's local main HEAD"
            );
        }));
        cleanup();
        result.unwrap();
    }

    #[test]
    fn launch_spec_cats_the_brief() {
        // The adapter spawns the composed command onto the shell START opened;
        // it must carry the brief in from the work dir, not on the tmux command
        // string. The env supplies the agent command + brief location.
        let b = LocalTmux::sandboxed("s", PathBuf::from("/tmp/x"), "myagent --flag");
        let req = StartRequest {
            session_uid: "u".into(),
            template: None,
            repo: None,
            isolation: None,
            git_ref: None,
            fetch_remote: false,
            setup: None,
            brief: "b".into(),
            otel_endpoint: None,
        };
        assert_eq!(
            b.launch_spec(&req).unwrap().command(),
            "myagent --flag \"$(cat ../brief.md)\""
        );
    }

    // ── M1 holder path ──────────────────────────────────────────────────────
    //
    // These drive a real `disponent_hold::Holder` (over `/bin/sh`, no spawned
    // `disponent` binary) through the `HolderCompute` surface, asserting the two
    // headline wins (exact frames + a real exit code) and the send/interrupt/kill
    // channel — plus a flag-OFF regression guard that today's scraped tmux path
    // is untouched.

    use std::collections::BTreeMap;
    use std::path::Path;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Instant;

    use crate::observe::Observation;
    use disponent_hold::{Config, Holder};

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    fn scratch_dir() -> PathBuf {
        // Unique per call: pid keeps it distinct across test binaries, the atomic
        // counter across calls within one. (Deliberately terser than
        // disponent-hold's roundtrip `scratch_dir` — no shared cross-crate
        // test-util is worth the plumbing for a two-line scaffold.)
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("dsp-m1-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Start a holder over `/bin/sh -c <script>`, socket in `dir` (bound
    /// synchronously by `Holder::start`).
    fn start_holder(uid: &str, script: &str, dir: &Path) -> Holder {
        let mut env = BTreeMap::new();
        env.insert("PATH".into(), "/usr/bin:/bin:/usr/sbin:/sbin".into());
        env.insert("TERM".into(), "xterm-256color".into());
        Holder::start(Config {
            uid: uid.to_string(),
            argv: vec!["/bin/sh".into(), "-c".into(), script.into()],
            cwd: None,
            env,
            socket_dir: Some(dir.to_path_buf()),
            ring_bytes: 256 * 1024,
            size: Default::default(),
        })
        .unwrap()
    }

    /// A `HolderCompute` dialing a test holder's socket in `dir`.
    fn holder_compute(dir: &Path, uid: &str) -> HolderCompute {
        HolderCompute {
            dev: LocalTmux::sandboxed_holder(dir.to_path_buf(), "agent"),
            handle: json!({
                "holder": true,
                "holderSock": dir.join(format!("{uid}.sock")),
                "workDir": dir.join(uid),
            }),
        }
    }

    /// Poll `drain_observations` until `f` says done or the deadline passes.
    fn drive<F: FnMut(&Observation) -> bool>(stream: &TerminalStream, secs: u64, mut f: F) -> bool {
        let deadline = Instant::now() + Duration::from_secs(secs);
        while Instant::now() < deadline {
            for o in stream.drain_observations() {
                if f(&o) {
                    return true;
                }
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        false
    }

    #[test]
    fn holder_observe_stream_is_exact_and_carries_output() {
        let dir = scratch_dir();
        let holder = start_holder("obs", r#"printf 'exact-hello\n'; sleep 0.3; exit 0"#, &dir);
        let c = holder_compute(&dir, "obs");
        assert!(c.observes_exact(), "the holder surface is the exact tier");

        let stream = c
            .observe_stream()
            .unwrap()
            .expect("a holder provides a live stream");
        let mut data = String::new();
        let saw_it = drive(&stream, 5, |o| {
            if o.kind == "raw" {
                // The whole point of the holder path: exact, not scraped.
                assert_eq!(o.fidelity, "exact", "holder frames are exact");
                data.push_str(o.payload["payload"]["data"].as_str().unwrap_or(""));
            }
            data.contains("exact-hello")
        });
        assert!(
            saw_it,
            "the exact stream must carry the child's output, got {data:?}"
        );
        drop(holder);
    }

    #[test]
    fn holder_stream_surfaces_the_real_exit_code() {
        let dir = scratch_dir();
        let holder = start_holder("exit3", r#"exit 3"#, &dir);
        let c = holder_compute(&dir, "exit3");
        let stream = c.observe_stream().unwrap().unwrap();

        let mut code = None;
        let got = drive(&stream, 5, |o| {
            if o.kind == "exit" {
                assert_eq!(o.fidelity, "exact");
                code = o.payload["payload"]["code"].as_i64();
                return true;
            }
            false
        });
        assert!(got, "the holder must surface the child's exit");
        assert_eq!(code, Some(3), "the REAL exit code, not an inference");
        drop(holder);
    }

    #[test]
    fn without_the_flag_the_local_surface_is_scraped_and_not_streaming() {
        // Flag OFF → LocalCompute: no exact stream, scraped tier. The regression
        // guard that today's tmux path is byte-for-byte unchanged.
        let b = LocalTmux::sandboxed("s", PathBuf::from("/tmp/x"), "agent");
        assert!(!b.uses_holder());
        let handle = json!({"tmux": "dsp-u", "socket": "s", "workDir": "/tmp/x/u"});
        let c = b.compute(&handle).unwrap();
        assert!(
            !c.observes_exact(),
            "the tmux surface is scraped, not exact"
        );
        assert!(
            c.observe_stream().unwrap().is_none(),
            "no exact stream without the holder"
        );
    }

    #[test]
    fn a_holder_handle_selects_the_holder_surface() {
        let dir = scratch_dir();
        let holder = start_holder("sel", r#"sleep 2; exit 0"#, &dir);
        let b = LocalTmux::sandboxed_holder(dir.clone(), "agent");
        assert!(b.uses_holder());
        let handle = json!({
            "holder": true,
            "holderSock": dir.join("sel.sock"),
            "workDir": dir.join("sel"),
        });
        let c = b.compute(&handle).unwrap();
        assert!(
            c.observes_exact(),
            "a holder handle → the exact holder surface"
        );
        assert!(c.observe_stream().unwrap().is_some());
        drop(holder);
    }

    #[test]
    fn holder_send_reaches_the_child_and_interrupt_delivers_ctrl_c() {
        let dir = scratch_dir();
        // Echo a line read from stdin, then trap SIGINT to prove C-c landed.
        let holder = start_holder(
            "io",
            r#"read x; echo "got:$x"; trap 'echo INTERRUPTED; exit 0' INT; sleep 5"#,
            &dir,
        );
        let c = holder_compute(&dir, "io");
        let stream = c.observe_stream().unwrap().unwrap();

        c.send("world").unwrap(); // send_line → "world\n" over an Input frame
        let mut acc = String::new();
        let mut sent_int = false;
        let done = drive(&stream, 8, |o| {
            if o.kind == "raw" {
                acc.push_str(o.payload["payload"]["data"].as_str().unwrap_or(""));
            }
            // Once the child echoed our input, fire the interrupt.
            if acc.contains("got:world") && !sent_int {
                c.interrupt().unwrap();
                sent_int = true;
            }
            acc.contains("INTERRUPTED")
        });
        assert!(
            sent_int,
            "send() must reach the child's stdin (got {acc:?})"
        );
        assert!(done, "interrupt() must deliver C-c / SIGINT (got {acc:?})");
        drop(holder);
    }

    #[test]
    fn holder_kill_signals_the_child_via_the_control_frame() {
        let dir = scratch_dir();
        let holder = start_holder("kill", r#"sleep 30"#, &dir);
        let c = holder_compute(&dir, "kill");
        let stream = c.observe_stream().unwrap().unwrap();

        c.kill().unwrap(); // SIGKILL to the child pgroup via the Signal frame
        let mut signal = None;
        let got = drive(&stream, 5, |o| {
            if o.kind == "exit" {
                signal = o.payload["payload"]["signal"].as_i64();
                return true;
            }
            false
        });
        assert!(got, "kill must end the child and surface its exit");
        assert_eq!(signal, Some(9), "SIGKILL death → signal 9");
        drop(holder);
    }

    #[test]
    fn holder_send_takes_the_writer_and_fails_when_a_human_holds_it() {
        let dir = scratch_dir();
        // The child echoes the one line it reads, so a successful send is
        // observable.
        let holder = start_holder("wsend", r#"read x; echo "got:[$x]"; sleep 2; exit 0"#, &dir);
        let c = holder_compute(&dir, "wsend");
        let sock = dir.join("wsend.sock");

        // With no human writer, send takes the writer momentarily and reaches
        // the child.
        let stream = c.observe_stream().unwrap().unwrap(); // a resident reader
        c.send("hello").unwrap();
        let mut acc = String::new();
        let saw = drive(&stream, 5, |o| {
            if o.kind == "raw" {
                acc.push_str(o.payload["payload"]["data"].as_str().unwrap_or(""));
            }
            acc.contains("got:[hello]")
        });
        assert!(
            saw,
            "send must reach the child when no writer is held (got {acc:?})"
        );

        // Now a human-style interactive attacher holds the writer lock.
        let human = disponent_hold::Client::connect_writer(&sock).unwrap();
        assert_eq!(human.granted_role(), disponent_hold::Role::Writer);

        // send must now fail honestly rather than silently drop.
        let err = c.send("world").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("writer channel held"),
            "send must reject with a clear reason when a human holds the writer, got {msg:?}"
        );

        drop(human);
        drop(holder);
    }
}
