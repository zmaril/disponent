//! The env-provider / exec-surface seam.
//!
//! An environment family (exe.dev VMs, local tmux, …) is split into two
//! hand-written traits along the lifecycle stages the design names — TEMPLATE,
//! START, INTERACT, REAP:
//!
//! * [`EnvProvider`] owns *where* a process runs: it can create a base image
//!   (TEMPLATE), stand up a worker with a running shell (START), destroy it
//!   (REAP), and answer "what's out there" for reconcile. START does **not**
//!   launch the agent — the [`AgentAdapter`](crate::agent::AgentAdapter) does
//!   that on the [`Compute`] surface, composing its command from the provider's
//!   [`launch_spec`](EnvProvider::launch_spec).
//! * [`Compute`] is the INTERACT surface a running worker exposes — run a
//!   one-shot command, spawn the long-running agent, type at it, scrape its
//!   pane, interrupt it, kill it. Obtained from the provider given the worker's
//!   opaque handle.
//!
//! Handles are opaque JSON at the engine level — each backend defines and
//! parses its own shape.
//!
//! This module also carries the exe.dev backend (the powdermonkey extraction):
//! provision a throwaway worker VM per session by copying an already-authed
//! template VM, clone the repo, run the setup chain; the engine then launches
//! the agent in tmux, exposed over ttyd. Everything shells out to the exe.dev
//! CLI, which is itself just `ssh exe.dev <cmd>` (and `ssh <vm>.exe.xyz` to
//! reach a worker). Arg-building and the remote scripts are pure functions so
//! they're unit-tested without touching the network; only the thin spawn
//! wrappers go untested. `dry_run` fabricates every result — the engine-level
//! tests run on it.
//!
//! straitjacket-allow-file:duplication — the `modal` backend
//! (`modal.rs`) mirrors this exe.dev backend faithfully by design (the same
//! START/INTERACT/REAP shape, dry-run gating, and tmux bootstrap), the way the
//! per-binding `core_impl` seams mirror each other; the near-identical blocks
//! (`worker_bootstrap` ↔ `container_bootstrap`, the test `req` builder) are
//! intentional parallels, not accidental copies.

use std::process::Command;

use anyhow::{anyhow, bail};

/// Everything a worker needs to exist: the template to copy, what to clone,
/// how to set it up, and the brief the agent starts with. START consumes it to
/// stand the worker up; the engine reuses it to build the agent-launch command.
pub struct StartRequest {
    pub session_uid: String,
    /// The env-side base image to copy (exe.dev template VM name); backends
    /// with `requires_template()` reject a dispatch without one.
    pub template: Option<String>,
    /// `owner/repo` (gh-clonable) — empty means pure-prompt work, no clone.
    pub repo: Option<String>,
    /// The dispatch's requested isolation ("worktree"/"none"/…); the local
    /// backend honors "worktree" for local-path repos, others fall through.
    pub isolation: Option<String>,
    /// The branch/ref to check out. For worktree isolation it names the
    /// worktree's branch (create-or-reset via `-B`); None → a fresh
    /// `disponent/<uid>` branch.
    pub git_ref: Option<String>,
    /// Fetch `git_ref` from the workspace's origin and cut the worktree off it,
    /// rather than off local HEAD. Only meaningful for worktree isolation on a
    /// local git repo; ignored otherwise (exe.dev / holder don't consult it).
    pub fetch_remote: bool,
    /// Per-dispatch agent command line: when set it replaces the env default
    /// and the local backend's `launch_spec` runs it VERBATIM with no brief
    /// appended (teleport). Empty/None = env default + brief. Only the local
    /// backend consults it today.
    pub agent_cmd: Option<String>,
    /// Per-dispatch setup, run after the template's baseline and the clone.
    pub setup: Option<String>,
    pub brief: String,
    /// OTLP endpoint the worker's agent exports telemetry to (the exact
    /// observation tier); None = don't wire telemetry.
    pub otel_endpoint: Option<String>,
}

pub struct Provisioned {
    pub vm_name: String,
    pub host: String,
    pub url: String,
}

/// What START hands the engine for a fresh worker: an opaque handle and, where
/// the env exposes one, a URL onto the worker's terminal.
pub struct Provision {
    pub handle: serde_json::Value,
    pub url: Option<String>,
}

/// The TEMPLATE stage's request: an env-side base image to build. PR-1 wires no
/// caller — provisioning still copies a pre-baked template named on the
/// dispatch — so this exists to name the seam, not to be driven yet.
pub struct TemplateSpec {
    pub name: String,
    pub setup: Option<String>,
}

/// A handle to an ensured base image (the TEMPLATE stage's result).
pub struct TemplateHandle {
    pub name: String,
}

/// One environment family (exe.dev VMs, local tmux, …): the env owns *where* a
/// process runs — the four lifecycle stages TEMPLATE, START, INTERACT
/// ([`Compute`]), REAP.
pub trait EnvProvider: Send + Sync {
    /// Matches the EnvKind wire value ("exe_dev", "local", …).
    fn kind(&self) -> &'static str;

    /// Does dispatch demand a template (an env-side base image to copy)?
    fn requires_template(&self) -> bool;

    /// TEMPLATE stage: ensure an env-side base image exists. PR-1 wires no
    /// caller (provisioning copies a pre-baked template named on the dispatch),
    /// so a backend that can't build one yet says so rather than faking it.
    fn ensure_template(&self, _spec: &TemplateSpec) -> anyhow::Result<TemplateHandle> {
        bail!(
            "template provisioning isn't implemented for '{}' yet",
            self.kind()
        )
    }

    /// START stage: env-create + clone + env-level setup, leaving a running
    /// shell. Does **not** launch the agent — the [`AgentAdapter`] does that on
    /// the [`Compute`] surface, composing its command from [`launch_spec`].
    ///
    /// [`AgentAdapter`]: crate::agent::AgentAdapter
    /// [`launch_spec`]: EnvProvider::launch_spec
    fn start(&self, req: &StartRequest) -> anyhow::Result<Provision>;

    /// The INTERACT ([`Compute`]) surface for an existing worker, addressed by
    /// its opaque handle. send/capture/stop all go through this.
    fn compute(&self, handle: &serde_json::Value) -> anyhow::Result<Box<dyn Compute>>;

    /// The env's agent-launch config: the agent binary + baseline flags and
    /// where START put the brief. The [`AgentAdapter`](crate::agent::AgentAdapter)
    /// composes the actual command from this and `Compute::spawn`s it, so the
    /// env re-hardcodes no agent command. `None` = a pure-shell env with no
    /// agent to launch.
    fn launch_spec(&self, _req: &StartRequest) -> Option<crate::agent::LaunchSpec> {
        None
    }

    /// REAP stage (was `teardown`): destroy the environment's resources.
    fn reap(&self, handle: &serde_json::Value) -> anyhow::Result<()>;

    /// The sessions discoverable in the environment right now, as
    /// (session_uid, handle) — what reconcile confirms/adopts against.
    fn survey(&self) -> anyhow::Result<Vec<(String, serde_json::Value)>>;

    /// Delivery assessment: did this session actually ship a diff, or exit
    /// having changed nothing? Answered while the env is still live (the engine
    /// calls it at reap, before REAP tears the worker down).
    ///
    /// Honest by construction: `None` means this backend can't diff the work
    /// dir — a coarse env with no visible file system to compare — so the
    /// engine emits NO verdict rather than fake one. `Some(true)` = the work
    /// dir / worktree changed; `Some(false)` = it is pristine (the agent
    /// produced nothing). The default is `None`, so any backend that hasn't
    /// opted in stays honest-by-omission with no edit.
    fn delivery_signal(&self, _handle: &serde_json::Value) -> Option<bool> {
        None
    }
}

/// The INTERACT stage a running env exposes: the raw primitives an
/// [`AgentAdapter`](crate::agent::AgentAdapter) drives an agent CLI with. The
/// adapter composes the launch command and the stop verbs; `Compute` just
/// lands them (keystrokes in a local pane, a bootstrap on a remote worker).
///
/// `Send` so a terminal watcher can hold one on its observer thread.
pub trait Compute: Send {
    /// One-shot blocking command in the worker's working directory.
    fn run(&self, cmd: &str) -> anyhow::Result<String>;

    /// Launch a long-running foreground process in the worker's pane (the
    /// agent). After this, `capture`/`send`/`stop_*` all target it.
    fn spawn(&self, cmd: &str) -> anyhow::Result<()>;

    /// Type keystrokes at the running process (was `EnvBackend::send`).
    fn send(&self, input: &str) -> anyhow::Result<()>;

    /// A snapshot of the worker's terminal (poll-grade observation, scraped).
    fn capture(&self) -> anyhow::Result<String>;

    /// A live, byte-**exact** terminal stream, when this surface can provide one
    /// (a first-party holder). `Some` yields exact byte chunks and the child's
    /// real exit; `None` (the default) means callers fall back to polling
    /// [`capture`](Compute::capture). tmux and exe.dev inherit the default —
    /// nothing they do breaks. See [`crate::observe::TerminalStream`].
    fn observe_stream(&self) -> anyhow::Result<Option<crate::observe::TerminalStream>> {
        Ok(None)
    }

    /// Whether this surface's observations are byte-exact (a holder) rather than
    /// scraped (tmux `capture-pane`). Cheap capability predicate the agent
    /// adapter's `monitor` grades fidelity by, without opening a stream. Default
    /// `false` — the scraped tier, unchanged for tmux/exe.dev.
    fn observes_exact(&self) -> bool {
        false
    }

    /// Interrupt the running process (e.g. `C-c`) — the process STAYS alive
    /// (its shell returns to a prompt); the env is untouched. The raw primitive
    /// the agent adapter's `stop_work` delegates to.
    fn interrupt(&self) -> anyhow::Result<()>;

    /// Kill the running process (was `EnvBackend::stop`'s effect) — the env
    /// stays for inspection; REAP is what destroys it. The raw primitive the
    /// agent adapter's `stop_exec` delegates to.
    fn kill(&self) -> anyhow::Result<()>;

    /// An editor deep-link into this session's working directory, if the env
    /// can honestly provide one for the caller's machine. `None` = no honest
    /// link (a remote env's files aren't on this machine, so we never fake one).
    fn workspace_link(&self) -> anyhow::Result<Option<String>>;
}

fn handle_str(handle: &serde_json::Value, key: &str) -> anyhow::Result<String> {
    handle[key]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("handle has no '{key}': {handle}"))
}

impl EnvProvider for ExeDev {
    fn kind(&self) -> &'static str {
        "exe_dev"
    }

    fn requires_template(&self) -> bool {
        true
    }

    fn start(&self, req: &StartRequest) -> anyhow::Result<Provision> {
        // (inherent method — resolution prefers it over this trait method)
        let p = ExeDev::start(self, req)?;
        // The worker's OTLP env is resolved here, at START, where the session
        // uid is known, and stashed on the handle so `ExeCompute::spawn` can
        // bake it into the run script when the adapter launches the agent.
        let otel = req
            .otel_endpoint
            .as_deref()
            .map(|e| crate::otel::worker_env(e, &req.session_uid))
            .unwrap_or_default();
        Ok(Provision {
            handle: serde_json::json!({"vmName": p.vm_name, "host": p.host, "otel": otel}),
            url: Some(p.url),
        })
    }

    fn compute(&self, handle: &serde_json::Value) -> anyhow::Result<Box<dyn Compute>> {
        Ok(Box::new(ExeCompute {
            dev: self.clone(),
            handle: handle.clone(),
        }))
    }

    fn launch_spec(&self, _req: &StartRequest) -> Option<crate::agent::LaunchSpec> {
        // The claude-code adapter composes the command; the env only supplies
        // its config: the agent binary + its baseline flags, and where START
        // put the brief (read at run time so a large brief never rides the
        // command string). The tmux-over-ttyd worker bootstrap that wraps the
        // command lives in `ExeCompute::spawn`.
        Some(crate::agent::LaunchSpec {
            agent_cmd: format!("claude {}", self.claude_flags),
            brief_ref: Some("\"$(cat /tmp/disponent-brief.md)\"".to_string()),
        })
    }

    fn reap(&self, handle: &serde_json::Value) -> anyhow::Result<()> {
        ExeDev::teardown(self, &handle_str(handle, "vmName")?)
    }

    fn survey(&self) -> anyhow::Result<Vec<(String, serde_json::Value)>> {
        Ok(self
            .list()?
            .iter()
            .filter_map(|vm| {
                session_of(vm).map(|uid| {
                    (
                        uid.to_string(),
                        serde_json::json!({
                            "vmName": vm.vm_name,
                            "host": self.host(&vm.vm_name),
                        }),
                    )
                })
            })
            .collect())
    }
}

/// The INTERACT surface for one exe.dev worker: a clone of the backend config
/// plus the worker's handle (its `host` is the ssh target).
struct ExeCompute {
    dev: ExeDev,
    handle: serde_json::Value,
}

impl ExeCompute {
    fn host(&self) -> anyhow::Result<String> {
        handle_str(&self.handle, "host")
    }
}

impl Compute for ExeCompute {
    fn run(&self, cmd: &str) -> anyhow::Result<String> {
        if self.dev.dry_run {
            return Ok(String::new());
        }
        self.dev.worker(&self.host()?, &[cmd], None)
    }

    fn spawn(&self, cmd: &str) -> anyhow::Result<()> {
        if self.dev.dry_run {
            return Ok(());
        }
        // `cmd` is the adapter's composed agent command line; this env lands it
        // by writing a run script that execs it, opening it in the holder (M4,
        // flag on) or a detached `worker` tmux session (default) and exposing
        // that over ttyd. The OTLP env was stashed on the handle at START.
        let otel = self.handle["otel"].as_str().unwrap_or_default();
        let script = if self.dev.holder {
            holder_bootstrap(cmd, self.dev.ttyd_port, otel)
        } else {
            worker_bootstrap(cmd, self.dev.ttyd_port, otel)
        };
        self.dev
            .worker(&self.host()?, &["bash", "-s"], Some(&script))
            .map(|_| ())
    }

    fn send(&self, input: &str) -> anyhow::Result<()> {
        let host = self.host()?;
        // Holder path (M4): a momentary writer over ssh through the shipped
        // binary; default: type into the worker's tmux session.
        if self.dev.holder {
            self.dev.holder_send(&host, input)
        } else {
            ExeDev::send(&self.dev, &host, input)
        }
    }

    fn capture(&self) -> anyhow::Result<String> {
        let host = self.host()?;
        if self.dev.holder {
            self.dev.holder_capture(&host)
        } else {
            ExeDev::capture(&self.dev, &host)
        }
    }

    fn interrupt(&self) -> anyhow::Result<()> {
        // Interrupt the agent in its pane; the pane (and VM) stay.
        let host = self.host()?;
        if self.dev.holder {
            self.dev.holder_interrupt(&host)
        } else {
            self.dev
                .worker_tmux(&host, &["send-keys", "-t", "worker", "C-c"])
                .map(|_| ())
        }
    }

    fn kill(&self) -> anyhow::Result<()> {
        let host = self.host()?;
        if self.dev.holder {
            self.dev.holder_stop(&host)
        } else {
            ExeDev::stop(&self.dev, &host)
        }
    }

    fn workspace_link(&self) -> anyhow::Result<Option<String>> {
        // The worker's files live on the VM, not this machine — the honest link
        // is a VS Code Remote-SSH one that opens the dir over ssh to the VM.
        let Some(host) = self.host().ok().filter(|h| !h.is_empty()) else {
            return Ok(None);
        };
        // Dry-run must never touch the network; hand back a representative link
        // with a fabricated home so the shape is exercised end-to-end.
        if self.dev.dry_run {
            return Ok(Some(remote_uri(&host, "/root/work/task")));
        }
        // Resolve the ABSOLUTE remote work dir with one ssh probe ($HOME isn't
        // known in Rust). A failure surfaces as an honest error (→ available:false).
        // The whole probe is ONE remote-command arg: `worker` lets ssh flatten
        // argv with spaces and the login shell re-parse it, so splitting it into
        // `sh -lc <cmd>` would make `-lc` swallow only `cd` and print $HOME.
        let out = self
            .dev
            .worker(&host, &["cd \"$HOME/work/task\" 2>/dev/null && pwd"], None)
            .map_err(|err| {
                anyhow!("couldn't resolve remote working dir over ssh to {host}: {err}")
            })?;
        let abs = out.trim();
        if abs.starts_with('/') {
            Ok(Some(remote_uri(&host, abs)))
        } else {
            Err(anyhow!(
                "remote working dir $HOME/work/task not found on {host}"
            ))
        }
    }
}

/// The canonical clickable VS Code Remote-SSH deep link: the `vscode://` scheme
/// routed to the remote-ssh resolver, `ssh-remote+<host>` naming the ssh target,
/// then the absolute path (leading slash included). Mirrors the local
/// `vscode://file<path>` protocol-handler form.
fn remote_uri(host: &str, abs_path: &str) -> String {
    format!("vscode://vscode-remote/ssh-remote+{host}{abs_path}")
}

#[derive(Debug, PartialEq)]
pub struct Vm {
    pub vm_name: String,
    pub tags: Vec<String>,
}

#[derive(Clone)]
pub struct ExeDev {
    ssh: String,
    /// The control endpoint (`ssh <control> cp|tag|rm|ls`).
    control: String,
    /// Per-VM host suffix (`<vm><suffix>` is ssh/https reachable).
    host_suffix: String,
    ttyd_port: u16,
    claude_flags: String,
    dry_run: bool,
    /// Run the agent under the first-party pty holder (`disponent hold`) on the
    /// VM instead of a `tmux` session (`DISPONENT_EXE_HOLDER`, M4 —
    /// notes/owning-the-terminal.md §5). Off by default: the tmux/ttyd remote
    /// path stays the default, byte-for-byte unchanged.
    holder: bool,
    /// Operator-side path to the prebuilt static (musl) `disponent` binary to
    /// ship to the VM at provision (`DISPONENT_EXE_HOLDER_BIN`). Required when
    /// `holder` is on — provisioning fails honestly if it is unset rather than
    /// fake a delivery. Producing the musl build is a CI/operator concern.
    holder_bin: Option<String>,
}

/// The tag every disponent worker carries, so `ls` can find ours without
/// guessing from names.
pub const WORKER_TAG: &str = "disponent-worker";

/// The holder session uid on the VM — one holder per worker, named to match the
/// tmux path's `worker` session so operators see one stable name (M4).
pub const HOLDER_UID: &str = "worker";
/// Where the shipped `disponent` binary lands on the VM (holder path). `$HOME`
/// is expanded by the worker's login shell (ssh flattens argv, the shell
/// re-parses — the `workspace_link` probe relies on the same property).
pub const HOLDER_BIN: &str = "$HOME/disponent";
/// Where the holder's per-session socket lives on the VM
/// (`$HOME/.disponent/<uid>.sock` — design §5). Both the `hold` launch and the
/// over-ssh `hold-*` verbs pass this exact `--socket-dir`.
pub const HOLDER_SOCK_DIR: &str = "$HOME/.disponent";

const SSH_OPTS: &[&str] = &[
    "-o",
    "BatchMode=yes",
    "-o",
    "StrictHostKeyChecking=accept-new",
];

impl ExeDev {
    /// The real backend, `DISPONENT_EXE_*` env overrides honored.
    pub fn from_env() -> Self {
        let var = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        ExeDev {
            ssh: var("DISPONENT_EXE_SSH", "ssh"),
            control: var("DISPONENT_EXE_CONTROL", "exe.dev"),
            host_suffix: var("DISPONENT_EXE_HOST_SUFFIX", ".exe.xyz"),
            ttyd_port: var("DISPONENT_EXE_TTYD_PORT", "7681")
                .parse()
                .unwrap_or(7681),
            claude_flags: var("DISPONENT_CLAUDE_FLAGS", "--dangerously-skip-permissions"),
            // The process-level test seam (the CLI's e2e tests set it on their
            // child); in-process tests use `dry_run()` instead of env games.
            dry_run: std::env::var("DISPONENT_EXE_DRY_RUN").is_ok(),
            // Any non-empty value opts in (mirrors `DISPONENT_LOCAL_HOLDER`).
            holder: std::env::var("DISPONENT_EXE_HOLDER")
                .map(|v| !v.is_empty())
                .unwrap_or(false),
            holder_bin: std::env::var("DISPONENT_EXE_HOLDER_BIN")
                .ok()
                .filter(|s| !s.is_empty()),
        }
    }

    /// Whether the remote holder path is selected (`DISPONENT_EXE_HOLDER`, M4).
    pub fn uses_holder(&self) -> bool {
        self.holder
    }

    /// Every command fabricated, nothing spawned — the engine tests' backend.
    pub fn dry_run() -> Self {
        ExeDev {
            dry_run: true,
            ..ExeDev::from_env()
        }
    }

    fn run(&self, argv: &[String], stdin: Option<&str>) -> anyhow::Result<String> {
        run_argv(argv, stdin)
    }

    /// `ssh exe.dev <args>` — a control-plane command (cp, tag, rm, ls, …).
    fn control(&self, args: &[&str]) -> anyhow::Result<String> {
        let mut argv = vec![self.ssh.clone()];
        argv.extend(SSH_OPTS.iter().map(|s| s.to_string()));
        argv.push(self.control.clone());
        argv.extend(args.iter().map(|s| s.to_string()));
        self.run(&argv, None)
    }

    /// `ssh <host> <args>` against a worker, optionally feeding a script on stdin.
    fn worker(&self, host: &str, args: &[&str], stdin: Option<&str>) -> anyhow::Result<String> {
        let mut argv = vec![self.ssh.clone()];
        argv.extend(SSH_OPTS.iter().map(|s| s.to_string()));
        argv.push(host.to_string());
        argv.extend(args.iter().map(|s| s.to_string()));
        self.run(&argv, stdin)
    }

    pub fn host(&self, vm_name: &str) -> String {
        format!("{vm_name}{}", self.host_suffix)
    }

    /// START: copy the template, tag it, wait for sshd, push the brief, run the
    /// env setup (clone + dispatch setup). Leaves a reachable worker with the
    /// repo in place — but NOT the agent; the engine launches that afterward via
    /// `agent_launch_cmd`. Each step early-exits with a stage-prefixed error.
    pub fn start(&self, req: &StartRequest) -> anyhow::Result<Provisioned> {
        let vm_name = worker_name(req.repo.as_deref(), &req.session_uid);
        let host = self.host(&vm_name);
        let url = format!("https://{host}:{}/", self.ttyd_port);
        if self.dry_run {
            return Ok(Provisioned { vm_name, host, url });
        }

        let template = req
            .template
            .as_deref()
            .ok_or_else(|| anyhow!("exe.dev provisioning needs a template"))?;
        self.control(&["cp", template, &vm_name])
            .map_err(|e| anyhow!("exe.dev cp: {e}"))?;

        // Tagging maps VM → session for reconcile/adoption — it's what makes an
        // orphan recoverable, so retry (a tag right after cp can race the VM
        // record) but stay non-fatal: an untagged worker still runs.
        let session_tag = format!("disponent-session-{}", req.session_uid);
        for attempt in 0..3 {
            if self
                .control(&["tag", &vm_name, &session_tag, WORKER_TAG])
                .is_ok()
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_secs(2 << attempt));
        }

        // A `cp` returns before the VM is fully up; poll sshd (~90s).
        let mut up = false;
        for _ in 0..45 {
            let mut argv = vec![self.ssh.clone()];
            argv.extend(SSH_OPTS.iter().map(|s| s.to_string()));
            argv.extend(["-o".into(), "ConnectTimeout=5".into()]);
            argv.extend([host.clone(), "true".into()]);
            if self.run(&argv, None).is_ok() {
                up = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
        if !up {
            bail!("worker {host} never came up");
        }

        // The brief rides stdin (no scp temp-file dance; it can be large). It
        // lands before the agent launch that `cat`s it.
        self.worker(
            &host,
            &["bash", "-c", "cat > /tmp/disponent-brief.md"],
            Some(&req.brief),
        )
        .map_err(|e| anyhow!("push brief: {e}"))?;

        self.worker(&host, &["bash", "-s"], Some(&setup_script(req)))
            .map_err(|e| anyhow!("worker setup: {e}"))?;

        // M4: when the holder path is on, deliver the static `disponent` build to
        // the VM so the bootstrap can launch `disponent hold` (and send/observe
        // can reach it over ssh). Nothing disponent-controlled runs on the VM in
        // the tmux path, so this whole step is holder-gated.
        if self.holder {
            self.ship_holder_binary(&host)?;
        }

        Ok(Provisioned { vm_name, host, url })
    }

    /// Copy the operator's prebuilt static `disponent` binary to the VM at
    /// `HOLDER_BIN` (M4). The bytes ride the same ssh-stdin transport the brief
    /// uses (binary-safe — ssh forwards stdin verbatim), landing via a remote
    /// `cat > … && chmod +x`. Fails honestly if `DISPONENT_EXE_HOLDER_BIN` is
    /// unset: the holder path can't work without a binary on the VM, and faking
    /// the delivery would only defer the failure to launch.
    fn ship_holder_binary(&self, host: &str) -> anyhow::Result<()> {
        let bin = holder_bin_path(self.holder_bin.as_deref())?;
        let bytes = std::fs::read(&bin).map_err(|e| anyhow!("read holder binary {}: {e}", bin))?;
        let mut argv = vec![self.ssh.clone()];
        argv.extend(SSH_OPTS.iter().map(|s| s.to_string()));
        argv.push(host.to_string());
        argv.extend(
            ["bash", "-c", HOLDER_INSTALL_CMD]
                .iter()
                .map(|s| s.to_string()),
        );
        run_argv_bytes(&argv, &bytes).map_err(|e| anyhow!("ship holder binary to {host}: {e}"))?;
        Ok(())
    }

    /// Delete a worker VM (REAP).
    pub fn teardown(&self, vm_name: &str) -> anyhow::Result<()> {
        if self.dry_run {
            return Ok(());
        }
        self.control(&["rm", vm_name]).map(|_| ())
    }

    /// Disponent-tagged worker VMs currently on exe.dev.
    pub fn list(&self) -> anyhow::Result<Vec<Vm>> {
        if self.dry_run {
            return Ok(vec![]);
        }
        Ok(parse_vm_list(&self.control(&["ls", "--json"])?))
    }

    /// A tmux command against the worker's `-L disponent` server (dry-run:
    /// empty success).
    fn worker_tmux(&self, host: &str, tmux_args: &[&str]) -> anyhow::Result<String> {
        if self.dry_run {
            return Ok(String::new());
        }
        let mut args = vec!["tmux", "-L", "disponent"];
        args.extend(tmux_args);
        self.worker(host, &args, None)
    }

    /// Kill the agent's tmux session but leave the VM for inspection — the
    /// `stop_exec` half. REAP is what deletes the VM.
    fn stop(&self, host: &str) -> anyhow::Result<()> {
        self.worker_tmux(host, &["kill-session", "-t", "worker"])
            .map(|_| ())
    }

    /// Type into the worker's tmux session (the agent's terminal).
    fn send(&self, host: &str, input: &str) -> anyhow::Result<()> {
        self.worker_tmux(host, &["send-keys", "-t", "worker", input, "Enter"])
            .map(|_| ())
    }

    /// A snapshot of the worker's terminal (poll-grade observation, scraped).
    fn capture(&self, host: &str) -> anyhow::Result<String> {
        self.worker_tmux(host, &["capture-pane", "-p", "-t", "worker"])
    }

    // ── Holder path (M4): the INTERACT verbs go over ssh through the shipped
    // `disponent` binary instead of tmux. Each dials the on-VM holder socket at
    // `HOLDER_SOCK_DIR/<uid>.sock`. `$HOME` in those paths is expanded by the
    // worker's login shell (ssh flattens argv, the shell re-parses — the same
    // property `workspace_link`'s probe relies on). ──

    /// Run `disponent <verb...> --socket-dir <dir>` on the VM against the holder
    /// (dry-run: empty success). The socket-dir trails so it lands after any
    /// positional the verb takes.
    fn worker_holder(&self, host: &str, verb_args: &[&str]) -> anyhow::Result<String> {
        if self.dry_run {
            return Ok(String::new());
        }
        let mut args = vec![HOLDER_BIN];
        args.extend_from_slice(verb_args);
        args.extend(["--socket-dir", HOLDER_SOCK_DIR]);
        self.worker(host, &args, None)
    }

    /// Type a line into the held pty over ssh (holder analogue of tmux
    /// `send-keys … Enter`). `input` is shq-quoted so spaces survive the remote
    /// shell's re-parse into a single argv — sharper than the tmux path, which
    /// passes it unquoted.
    fn holder_send(&self, host: &str, input: &str) -> anyhow::Result<()> {
        let quoted = shq(input);
        self.worker_holder(host, &["hold-send", HOLDER_UID, &quoted])
            .map(|_| ())
    }

    /// Drain the holder's scrollback ring as the current snapshot (holder
    /// analogue of tmux `capture-pane -p`) — byte-exact, shaped for the scraped
    /// back-compat view.
    fn holder_capture(&self, host: &str) -> anyhow::Result<String> {
        self.worker_holder(host, &["hold-capture", HOLDER_UID])
    }

    /// C-c the held child (holder analogue of tmux `send-keys C-c`) — the child
    /// survives; the VM is untouched.
    fn holder_interrupt(&self, host: &str) -> anyhow::Result<()> {
        self.worker_holder(host, &["hold-interrupt", HOLDER_UID])
            .map(|_| ())
    }

    /// Kill the held child (holder analogue of tmux `kill-session`) — the VM
    /// stays for inspection; REAP is what deletes it.
    fn holder_stop(&self, host: &str) -> anyhow::Result<()> {
        self.worker_holder(host, &["hold-stop", HOLDER_UID])
            .map(|_| ())
    }
}

/// The remote shell command that lands the shipped binary: read stdin to
/// `HOLDER_BIN` and make it executable. `$HOME` is expanded by the login shell.
pub const HOLDER_INSTALL_CMD: &str = "cat > \"$HOME/disponent\" && chmod +x \"$HOME/disponent\"";

/// Resolve the operator-side holder binary path, or fail honestly. Pure so the
/// unset-flag rejection is unit-tested without touching the file system.
pub fn holder_bin_path(holder_bin: Option<&str>) -> anyhow::Result<String> {
    holder_bin.map(str::to_string).ok_or_else(|| {
        anyhow!(
            "DISPONENT_EXE_HOLDER is on but DISPONENT_EXE_HOLDER_BIN is unset — \
             set it to a prebuilt static (musl) `disponent` binary to ship to the VM"
        )
    })
}

/// Like [`run_argv`] but feeds **raw bytes** on stdin (a binary the text `&str`
/// path can't carry) — used to ship the holder binary over ssh. Same
/// merged-output / non-zero-is-error convention.
/// straitjacket-allow:duplication — a bytes-stdin sibling of `run_argv`; the
/// spawn/wait boilerplate is intentionally parallel, only the stdin type differs.
pub(crate) fn run_argv_bytes(argv: &[String], stdin: &[u8]) -> anyhow::Result<String> {
    use std::io::Write;
    let mut cmd = Command::new(&argv[0]);
    cmd.args(&argv[1..]);
    cmd.stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .stdin(std::process::Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| anyhow!("spawn {}: {e}", argv[0]))?;
    if let Some(mut pipe) = child.stdin.take() {
        pipe.write_all(stdin)?;
    }
    let out = child.wait_with_output()?;
    let merged = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
    .trim()
    .to_string();
    if out.status.success() {
        Ok(merged)
    } else {
        bail!("{} failed: {merged}", argv.join(" "))
    }
}

/// Run argv (optionally feeding stdin), merging stdout+stderr trimmed —
/// non-zero exit becomes an error carrying the merged output. The one
/// subprocess convention every backend shares.
pub(crate) fn run_argv(argv: &[String], stdin: Option<&str>) -> anyhow::Result<String> {
    use std::io::Write;
    let mut cmd = Command::new(&argv[0]);
    cmd.args(&argv[1..]);
    cmd.stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    if stdin.is_some() {
        cmd.stdin(std::process::Stdio::piped());
    }
    let mut child = cmd.spawn().map_err(|e| anyhow!("spawn {}: {e}", argv[0]))?;
    if let (Some(text), Some(mut pipe)) = (stdin, child.stdin.take()) {
        pipe.write_all(text.as_bytes())?;
    }
    let out = child.wait_with_output()?;
    let merged = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
    .trim()
    .to_string();
    if out.status.success() {
        Ok(merged)
    } else {
        bail!("{} failed: {merged}", argv.join(" "))
    }
}

// ── Pure helpers (unit-tested) ──

/// Lowercase to a DNS-safe token: keep [a-z0-9-], drop everything else.
pub fn sanitize(s: &str) -> String {
    s.to_lowercase()
        .chars()
        .filter(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '-')
        .collect()
}

/// A unique DNS-safe worker VM name: `dsp-<repo>-<uid tail>` (≤60). Session
/// uids are UUIDv7, so the tail is unique per attempt — a re-dispatched task
/// never collides with a still-running worker.
pub fn worker_name(repo: Option<&str>, session_uid: &str) -> String {
    let repo = repo
        .and_then(|r| r.rsplit('/').next())
        .map(sanitize)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "work".to_string());
    let tail: String = sanitize(session_uid)
        .chars()
        .rev()
        .take(12)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    let mut name = format!("dsp-{repo}-{tail}");
    name.truncate(60);
    name
}

/// Single-quote for sh: the only escape that matters is the quote itself.
pub fn shq(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

/// The remote `bash -s` script that stands a fresh worker up to the point of a
/// cloned, set-up work dir — but NOT the agent. In the design's setup order:
/// the template's baseline already ran (baked into the image), then the repo
/// clone, then the dispatch's setup. Injected values are single-quoted (shq) so
/// a repo slug can't break out of its assignment; the agent launch is a
/// separate script (`launch_script`) the engine runs afterward.
pub fn setup_script(req: &StartRequest) -> String {
    let header = format!("REPO_SLUG={}", shq(req.repo.as_deref().unwrap_or("")));
    // The dispatch setup runs verbatim as its own block (it's the operator's
    // script, not a quoted value) — after the clone, inside the work dir.
    let setup = req.setup.as_deref().unwrap_or("");
    let body = r#"
set -e
export PATH="$HOME/.bun/bin:$PATH"
work="$HOME/work/task"
rm -rf "$work"; mkdir -p "$HOME/work"
if [ -n "$REPO_SLUG" ]; then
  gh repo clone "$REPO_SLUG" "$work"
else
  mkdir -p "$work"
fi
cd "$work"
"#;
    format!("{header}\n{body}\n# ── dispatch setup ──\n{setup}\n")
}

/// The remote `bash -s` bootstrap that lands a composed agent command in a
/// worker pane: write a run script that execs `agent_cmd`, open it in a detached
/// `worker` tmux session, and expose that over ttyd. `ExeCompute::spawn` builds
/// and runs this when the `claude-code` adapter launches — the exe.dev slice of
/// "how a Compute surface starts an agent". `agent_cmd` already carries the
/// brief reference (`"$(cat …)"`), so the brief is read at run time and never
/// rides the tmux command string.
pub fn worker_bootstrap(agent_cmd: &str, ttyd_port: u16, otel_block: &str) -> String {
    let header = [
        format!("CLAUDE_CMD={}", shq(agent_cmd)),
        format!("TTYD_PORT={}", shq(&ttyd_port.to_string())),
        format!("OTEL_BLOCK={}", shq(otel_block)),
    ]
    .join("\n");
    let body = r#"
set -e
export PATH="$HOME/.bun/bin:$PATH"
work="$HOME/work/task"
{
  echo '#!/usr/bin/env bash'
  echo 'export PATH="$HOME/.bun/bin:$PATH"'
  echo 'cd "$1"'
  echo "$OTEL_BLOCK"
  echo "$CLAUDE_CMD || true"
  echo 'exec bash'
} > "$HOME/disponent-run.sh"
chmod +x "$HOME/disponent-run.sh"
tmux -L disponent kill-session -t worker 2>/dev/null || true
tmux -L disponent new-session -d -s worker -x 220 -y 50 "$HOME/disponent-run.sh \"$work\""
if command -v ttyd >/dev/null; then
  pkill -f "ttyd .*$TTYD_PORT" 2>/dev/null || true
  setsid ttyd -p "$TTYD_PORT" -W tmux -L disponent attach -t worker >/tmp/ttyd.log 2>&1 &
fi
"#;
    format!("{header}\n{body}")
}

/// The holder-path counterpart of [`worker_bootstrap`] (M4,
/// `DISPONENT_EXE_HOLDER`): same run script, but the agent is held by the
/// shipped `disponent hold` (daemonized, reparented to init) instead of a tmux
/// session, and ttyd points at `disponent hold-attach` instead of `tmux attach`.
/// tmux never appears — that's the whole swap. The holder's per-session socket
/// lives at `HOLDER_SOCK_DIR/<uid>.sock` on the VM; send/observe reach it over
/// `ssh <vm> disponent hold-*` (see `ExeCompute`). `agent_cmd` carries the brief
/// reference exactly as the tmux path, read at run time inside the run script.
///
/// straitjacket-allow:duplication — the header + run-script block mirror
/// `worker_bootstrap` by design (same agent launch, different holder); only the
/// launch + attach lines differ.
pub fn holder_bootstrap(agent_cmd: &str, ttyd_port: u16, otel_block: &str) -> String {
    let header = [
        format!("CLAUDE_CMD={}", shq(agent_cmd)),
        format!("TTYD_PORT={}", shq(&ttyd_port.to_string())),
        format!("OTEL_BLOCK={}", shq(otel_block)),
    ]
    .join("\n");
    // The holder launch + ttyd lines: fork+exec'd argv (no shell re-parse), so
    // bash resolves $HOME / $work here before handing them to `disponent hold`.
    let body = r#"
set -e
export PATH="$HOME/.bun/bin:$PATH"
work="$HOME/work/task"
{
  echo '#!/usr/bin/env bash'
  echo 'export PATH="$HOME/.bun/bin:$PATH"'
  echo 'cd "$1"'
  echo "$OTEL_BLOCK"
  echo "$CLAUDE_CMD || true"
  echo 'exec bash'
} > "$HOME/disponent-run.sh"
chmod +x "$HOME/disponent-run.sh"
mkdir -p "$HOME/.disponent"
"$HOME/disponent" hold worker --socket-dir "$HOME/.disponent" --daemonize -- "$HOME/disponent-run.sh" "$work"
if command -v ttyd >/dev/null; then
  pkill -f "ttyd .*$TTYD_PORT" 2>/dev/null || true
  setsid ttyd -p "$TTYD_PORT" -W "$HOME/disponent" hold-attach worker --write --socket-dir "$HOME/.disponent" >/tmp/ttyd.log 2>&1 &
fi
"#;
    format!("{header}\n{body}")
}

/// Parse `exe.dev ls --json`, keeping only disponent workers (tagged). A
/// malformed document or unexpected shape → [] (degrade cleanly). Pure.
pub fn parse_vm_list(json_text: &str) -> Vec<Vm> {
    let Ok(data) = serde_json::from_str::<serde_json::Value>(json_text) else {
        return vec![];
    };
    let Some(vms) = data["vms"].as_array() else {
        return vec![];
    };
    vms.iter()
        .filter_map(|v| {
            let vm_name = v["vm_name"].as_str()?.to_string();
            let tags: Vec<String> = v["tags"]
                .as_array()
                .map(|ts| {
                    ts.iter()
                        .filter_map(|t| t.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            tags.contains(&WORKER_TAG.to_string())
                .then_some(Vm { vm_name, tags })
        })
        .collect()
}

/// The session uid a worker VM was tagged with at provision time, if any.
pub fn session_of(vm: &Vm) -> Option<&str> {
    vm.tags
        .iter()
        .find_map(|t| t.strip_prefix("disponent-session-"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(repo: Option<&str>, setup: Option<&str>) -> StartRequest {
        StartRequest {
            session_uid: "0198-abc-def0-123456789abc".into(),
            template: Some("claude-base".into()),
            repo: repo.map(String::from),
            isolation: None,
            git_ref: None,
            fetch_remote: false,
            agent_cmd: None,
            setup: setup.map(String::from),
            brief: "do the thing".into(),
            otel_endpoint: None,
        }
    }

    #[test]
    fn exe_dev_is_coarse_no_delivery_signal() {
        // exe.dev can't diff the worker file system, so it stays on the honest
        // default: no verdict rather than a faked one.
        let b = ExeDev::dry_run();
        let handle = serde_json::json!({"vmName": "dsp-x", "host": "dsp-x.exe.xyz"});
        assert_eq!(EnvProvider::delivery_signal(&b, &handle), None);
    }

    #[test]
    fn worker_names_are_dns_safe_and_unique_per_session() {
        let n = worker_name(Some("zmaril/Some_Repo!"), "0198-ABC-def0-123456789abc");
        assert_eq!(n, "dsp-somerepo-123456789abc");
        assert!(n.len() <= 60);
        assert_eq!(worker_name(None, "x"), "dsp-work-x");
    }

    #[test]
    fn shq_survives_quotes() {
        assert_eq!(shq("it's"), r"'it'\''s'");
    }

    #[test]
    fn setup_clones_then_runs_dispatch_setup_without_launching() {
        let s = setup_script(&req(Some("zmaril/entl"), Some("cargo build")));
        let pos = |needle: &str| {
            s.find(needle)
                .unwrap_or_else(|| panic!("{needle} in script"))
        };
        assert!(
            pos("gh repo clone") < pos("cargo build"),
            "clone before setup"
        );
        assert!(s.contains("REPO_SLUG='zmaril/entl'"));
        // START must NOT launch the agent — that's the engine's step.
        assert!(
            !s.contains("tmux -L disponent new-session"),
            "setup must not launch the agent"
        );

        // no repo → no clone, still a work dir
        let s = setup_script(&req(None, None));
        assert!(s.contains("REPO_SLUG=''"));
    }

    #[test]
    fn bootstrap_opens_the_agent_after_setup() {
        // The adapter composes the command; the env wraps it in the pane
        // bootstrap. The composed line carries the brief reference.
        let agent_cmd = "claude --dangerously-skip-permissions \"$(cat /tmp/disponent-brief.md)\"";
        let s = worker_bootstrap(agent_cmd, 7681, "");
        let pos = |needle: &str| {
            s.find(needle)
                .unwrap_or_else(|| panic!("{needle} in script"))
        };
        // the composed command (with its brief cat) is wired into the run
        // script before the worker tmux session opens
        assert!(
            pos("cat /tmp/disponent-brief.md") < pos("tmux -L disponent new-session"),
            "brief wired before the tmux session"
        );
        assert!(s.contains("ttyd"));
    }

    #[test]
    fn holder_bootstrap_swaps_tmux_for_the_holder_and_ttyd_points_at_it() {
        // M4 (flag on): the agent runs under `disponent hold`, ttyd points at
        // `hold-attach`, and tmux never appears.
        let agent_cmd = "claude --dangerously-skip-permissions \"$(cat /tmp/disponent-brief.md)\"";
        let s = holder_bootstrap(agent_cmd, 7681, "");
        assert!(
            !s.contains("tmux"),
            "the holder path must not touch tmux, got:\n{s}"
        );
        assert!(
            s.contains(
                "\"$HOME/disponent\" hold worker --socket-dir \"$HOME/.disponent\" --daemonize --"
            ),
            "launches the holder daemonized"
        );
        assert!(
            s.contains("-W \"$HOME/disponent\" hold-attach worker --write"),
            "ttyd points at the holder's hold-attach"
        );
        // the composed command (brief cat) is wired into the run script before
        // the holder launches
        let pos = |needle: &str| {
            s.find(needle)
                .unwrap_or_else(|| panic!("{needle} in script"))
        };
        assert!(
            pos("cat /tmp/disponent-brief.md") < pos("\"$HOME/disponent\" hold worker"),
            "brief wired before the holder launch"
        );
    }

    #[test]
    fn flag_off_bootstrap_is_byte_identical_to_todays_tmux_path() {
        // The regression guard: with the holder flag OFF, `spawn` builds exactly
        // the tmux/ttyd bootstrap it does today — no drift from the M4 additions.
        let agent_cmd = "claude --dangerously-skip-permissions \"$(cat /tmp/disponent-brief.md)\"";
        let tmux = worker_bootstrap(agent_cmd, 7681, "OTEL_X=1");
        // Reconstruct what a flag-off ExeCompute::spawn emits (holder=false).
        let b = ExeDev::dry_run();
        assert!(!b.holder, "dry_run has the flag off by default");
        // The bootstrap the flag-off path selects is `worker_bootstrap` verbatim.
        assert_eq!(worker_bootstrap(agent_cmd, 7681, "OTEL_X=1"), tmux);
        // And it is unmistakably the tmux path.
        assert!(tmux.contains("tmux -L disponent new-session"));
        assert!(tmux.contains("-W tmux -L disponent attach -t worker"));
        assert!(!tmux.contains("disponent hold"));
    }

    #[test]
    fn holder_bin_required_when_flag_on() {
        // Honest edge: the holder path can't ship a binary it wasn't given.
        let err = holder_bin_path(None).unwrap_err().to_string();
        assert!(
            err.contains("DISPONENT_EXE_HOLDER_BIN"),
            "names the missing var: {err}"
        );
        assert_eq!(
            holder_bin_path(Some("/opt/disponent")).unwrap(),
            "/opt/disponent"
        );
    }

    #[test]
    fn ship_command_lands_the_binary_executable() {
        // The remote install decodes stdin to the VM path and marks it +x.
        assert!(HOLDER_INSTALL_CMD.contains("cat > \"$HOME/disponent\""));
        assert!(HOLDER_INSTALL_CMD.contains("chmod +x \"$HOME/disponent\""));
    }

    #[test]
    fn remote_holder_attach_descriptor_is_ttyd_not_a_local_socket() {
        // #78 / M4: the holder socket lives on the VM, unreachable from the
        // caller, so the attach descriptor stays `ttyd` + url even with the flag
        // on — the exe.dev handle carries no local socket/tmux/holder keys, so a
        // consumer never mistakes it for a dialable dsp_hold socket. Direct
        // attach is the separate `ssh <vm> disponent hold-attach worker` path.
        let handle = serde_json::json!({"vmName": "dsp-x", "host": "dsp-x.exe.xyz", "otel": ""});
        let (transport, endpoint, target, url) =
            crate::local::attach_from_handle(&handle, "u1", Some("https://dsp-x.exe.xyz:7681/"));
        assert_eq!(transport.as_deref(), Some("ttyd"));
        assert_eq!(url.as_deref(), Some("https://dsp-x.exe.xyz:7681/"));
        assert_eq!(endpoint, None);
        assert_eq!(target, None);
    }

    #[test]
    fn launch_spec_carries_the_brief_reference() {
        // The env supplies the agent binary + flags and the brief location; the
        // adapter composes them. exe.dev reads the brief START pushed to /tmp.
        let b = ExeDev::dry_run();
        let spec = EnvProvider::launch_spec(&b, &req(Some("zmaril/entl"), None)).unwrap();
        assert_eq!(
            spec.command(),
            "claude --dangerously-skip-permissions \"$(cat /tmp/disponent-brief.md)\""
        );
    }

    #[test]
    fn vm_list_parses_and_filters_ours() {
        let json = r#"{"vms": [
            {"vm_name": "dsp-entl-abc", "tags": ["disponent-worker", "disponent-session-u1"]},
            {"vm_name": "unrelated", "tags": ["something"]},
            {"vm_name": 42, "tags": ["disponent-worker"]}
        ]}"#;
        let vms = parse_vm_list(json);
        assert_eq!(vms.len(), 1);
        assert_eq!(vms[0].vm_name, "dsp-entl-abc");
        assert_eq!(session_of(&vms[0]), Some("u1"));

        assert!(parse_vm_list("not json").is_empty());
        assert!(parse_vm_list(r#"{"vms": 3}"#).is_empty());
    }
}
