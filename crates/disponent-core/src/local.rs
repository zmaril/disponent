//! The local backend: run the agent on this machine, in tmux — the same
//! shape as an exe.dev worker but the "environment" is a managed work dir
//! plus a `tmux -L disponent` session named after the session uid. START sets
//! the work dir up and opens a shell; the [`AgentAdapter`](crate::agent::AgentAdapter)
//! then launches the agent into that shell (its [`Compute`] surface).
//! `interrupt` stops the agent's work, `kill` ends the tmux session and keeps
//! the work dir for inspection; REAP removes the work dir too. Survey lists the
//! tmux sessions, so reconcile adopts local runs a previous disponent left behind.

use std::path::PathBuf;
use std::process::Command;

use anyhow::{anyhow, bail, Context};
use serde_json::json;

use crate::backend::{
    run_argv, shq, Compute, EnvProvider, Provision, StartRequest, TemplateHandle, TemplateSpec,
};

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
        }
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
        let handle = self.handle(&req.session_uid);
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
                    // A named git_ref selects the worktree's branch with `-B`
                    // (create-or-reset off HEAD, so re-dispatching the same ref
                    // doesn't fail on an existing branch); no ref → a fresh,
                    // uid-unique `disponent/<uid>` branch via `-b`.
                    let branch_flag = match req.git_ref.as_deref().filter(|r| !r.is_empty()) {
                        Some(git_ref) => format!("-B {}", shq(git_ref)),
                        None => format!("-b {}", shq(&format!("disponent/{}", req.session_uid))),
                    };
                    let cmd = format!(
                        "git -C {} worktree add {} {}",
                        shq(&parent.display().to_string()),
                        branch_flag,
                        shq(&task.display().to_string()),
                    );
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
        Ok(Provision { handle, url: None })
    }

    fn compute(&self, handle: &serde_json::Value) -> anyhow::Result<Box<dyn Compute>> {
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
        let task = self
            .handle
            .get("workDir")
            .and_then(|d| d.as_str())
            .map(|d| PathBuf::from(d).join("task"))
            .ok_or_else(|| anyhow!("handle has no 'workDir': {}", self.handle))?;
        let out = Command::new("bash")
            .arg("-c")
            .arg(cmd)
            .current_dir(&task)
            .output()
            .map_err(|e| anyhow!("run: spawn bash: {e}"))?;
        if !out.status.success() {
            bail!("run: {}", String::from_utf8_lossy(&out.stderr).trim());
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    fn spawn(&self, cmd: &str) -> anyhow::Result<()> {
        // Launch the agent as a foreground process in the pane's shell.
        self.send_line(cmd)
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

#[cfg(test)]
mod tests {
    use super::*;

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
            setup: None,
            brief: "b".into(),
            otel_endpoint: None,
        };
        assert_eq!(
            b.launch_spec(&req).unwrap().command(),
            "myagent --flag \"$(cat ../brief.md)\""
        );
    }
}
