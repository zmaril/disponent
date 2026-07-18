//! The Modal backend: dispatch a worker to a [Modal](https://modal.com) sandbox
//! instead of an exe.dev VM. It coexists with the exe.dev backend — Modal is the
//! favored initial *remote* target, exe.dev stays registered until told otherwise.
//!
//! The lifecycle maps onto Modal primitives (verified against the Modal SDK):
//!
//! | disponent stage      | Modal primitive                                   |
//! |----------------------|---------------------------------------------------|
//! | TEMPLATE             | `modal.Image` (debian_slim + git/tmux + claude)   |
//! | START (Compute)      | `modal.Sandbox.create(...)`, re-attach `from_id`  |
//! | INTERACT (exec)      | `sandbox.exec(...)` → ContainerProcess            |
//! | workspace persist    | `modal.Volume.from_name` mounted at the work dir  |
//! | workspace_link       | `sb.tunnels()[port].url` (encrypted_ports HTTPS)  |
//! | credentials          | `modal.Secret` (never in the schema)              |
//! | REAP                 | `sb.terminate()`                                  |
//! | SURVEY               | `Sandbox.list(app_id=, tags=…)` filtered to ours  |
//!
//! Integration path: rather than a native Rust gRPC client, this shells out to an
//! embedded Python driver ([`modal_driver.py`], `include_str!`-bundled), exactly
//! the way [`ExeDev`](crate::backend::ExeDev) shells to `ssh`. The Rust side keeps
//! all policy — it composes the tmux command lines and passes them through the
//! driver's generic `exec` verb; the driver only owns the Modal SDK calls. Auth is
//! `MODAL_TOKEN_ID` / `MODAL_TOKEN_SECRET` in the env (their presence gates live vs
//! dry-run). `dry_run` fabricates every result — the engine-level tests run on it
//! and never invoke the Python driver.
//!
//! Send/capture reuse the proven tmux-in-container approach: the image installs
//! tmux, `spawn` opens the agent under `tmux -L disponent new-session -s worker`,
//! and send/capture/interrupt/kill are each one `exec` of a tmux command line.
//! Honest refinement path: Modal hands back exact ContainerProcess exit codes and
//! separated stdout/stderr, so a future `disponent hold` holder binary uploaded
//! into the sandbox would give exact frames instead of scraped tmux capture — but
//! the holder crate is unmerged (#53/#56), so this spike scrapes tmux like exe.dev.

use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{anyhow, bail};
use serde_json::{json, Value};

use crate::backend::{setup_script, shq, Compute, EnvProvider, Provision, StartRequest};

/// The embedded Python Modal driver, materialized to a temp path at run time.
const DRIVER_PY: &str = include_str!("modal_driver.py");

/// The tmux socket the in-container worker session lives on (mirrors exe.dev).
const TMUX_SOCKET: &str = "disponent";
/// The tmux session name every worker's agent runs in.
const WORKER_SESSION: &str = "worker";

#[derive(Clone)]
pub struct Modal {
    /// The Modal App sandboxes are created under / surveyed within.
    app: String,
    /// Default image/template name when a dispatch names none (informational —
    /// the driver builds debian_slim regardless, tagging by this name).
    image: Option<String>,
    /// Hard wall-clock timeout for a sandbox (seconds).
    timeout: u32,
    /// Scale-to-zero idle timeout (seconds), if configured.
    idle_timeout: Option<u32>,
    /// A workspace port to expose over an encrypted tunnel; `Some` enables
    /// `workspace_link`.
    workspace_port: Option<u16>,
    /// A named Modal Volume mounted at `workdir` for workspace persistence.
    volume: Option<String>,
    /// The mount path for the volume / the repo work dir.
    workdir: String,
    /// A named Modal Secret injected for credentials (kept out of the schema).
    secret: Option<String>,
    /// The claude CLI's baseline flags (shared `DISPONENT_CLAUDE_FLAGS`).
    claude_flags: String,
    /// The python interpreter that runs the driver.
    python: String,
    dry_run: bool,
}

impl Modal {
    /// The real backend, `DISPONENT_MODAL_*` env overrides honored. Live only
    /// when BOTH Modal tokens are present; otherwise dry-run (honest: no token,
    /// no live sandbox).
    pub fn from_env() -> Self {
        let var = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        let opt = |k: &str| std::env::var(k).ok().filter(|s| !s.is_empty());
        let have_tokens =
            std::env::var("MODAL_TOKEN_ID").is_ok() && std::env::var("MODAL_TOKEN_SECRET").is_ok();
        Modal {
            app: var("DISPONENT_MODAL_APP", "disponent"),
            image: opt("DISPONENT_MODAL_IMAGE"),
            timeout: var("DISPONENT_MODAL_TIMEOUT", "3600")
                .parse()
                .unwrap_or(3600),
            idle_timeout: opt("DISPONENT_MODAL_IDLE_TIMEOUT").and_then(|v| v.parse().ok()),
            workspace_port: opt("DISPONENT_MODAL_WORKSPACE_PORT").and_then(|v| v.parse().ok()),
            volume: opt("DISPONENT_MODAL_VOLUME"),
            workdir: var("DISPONENT_MODAL_WORKDIR", "/root/work/task"),
            secret: opt("DISPONENT_MODAL_SECRET"),
            claude_flags: var("DISPONENT_CLAUDE_FLAGS", "--dangerously-skip-permissions"),
            python: var("DISPONENT_MODAL_PYTHON", "python3"),
            // Live needs both tokens; the env seam forces dry-run for tests.
            dry_run: !have_tokens || std::env::var("DISPONENT_MODAL_DRY_RUN").is_ok(),
        }
    }

    /// Every command fabricated, nothing spawned — the engine tests' backend.
    /// Deterministic fake handles, a workspace port set so the tunnel/link path
    /// is exercised.
    pub fn dry_run() -> Self {
        Modal {
            dry_run: true,
            workspace_port: Some(8080),
            ..Modal::from_env()
        }
    }

    /// The image/template name a dispatch resolves to: the dispatch's own
    /// `template`, else the backend default.
    fn image_name(&self, req: &StartRequest) -> Option<String> {
        req.template.clone().or_else(|| self.image.clone())
    }

    /// The `sandbox_create` driver request — pure, so it's unit-tested without
    /// touching the driver. `otel` is the resolved worker OTLP env block.
    fn sandbox_create_request(&self, req: &StartRequest, otel: &str) -> Value {
        json!({
            "app": self.app,
            "image": self.image_name(req),
            "setup": req.setup,
            "timeout": self.timeout,
            "idle_timeout": self.idle_timeout,
            "workspace_port": self.workspace_port,
            "volume": self.volume,
            "workdir": self.workdir,
            "secret": self.secret,
            "session_uid": req.session_uid,
            "otel": otel,
        })
    }

    /// The `exec` driver request for one command in a sandbox — pure.
    fn exec_request(sandbox_id: &str, argv: &[&str], stdin: Option<&str>) -> Value {
        json!({
            "sandboxId": sandbox_id,
            "argv": argv,
            "stdin": stdin,
        })
    }

    /// Materialize the embedded driver to a stable temp path (write-if-absent or
    /// on content drift, so a version bump refreshes it) and return the path.
    fn ensure_driver(&self) -> anyhow::Result<PathBuf> {
        let path = std::env::temp_dir().join("disponent-modal-driver.py");
        let fresh = std::fs::read_to_string(&path)
            .map(|c| c == DRIVER_PY)
            .unwrap_or(false);
        if !fresh {
            std::fs::write(&path, DRIVER_PY)
                .map_err(|e| anyhow!("write modal driver to {}: {e}", path.display()))?;
        }
        Ok(path)
    }

    /// Invoke `python3 modal_driver.py <verb>` with the request on stdin, parse
    /// the one JSON response on stdout. A driver `{"ok":false}` or non-zero exit
    /// surfaces as an HONEST error (`modal driver: <error>`) — never faked.
    fn driver(&self, verb: &str, request: &Value) -> anyhow::Result<Value> {
        let path = self.ensure_driver()?;
        let mut child = Command::new(&self.python)
            .arg(&path)
            .arg(verb)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| anyhow!("spawn {} (modal driver): {e}", self.python))?;
        if let Some(mut pipe) = child.stdin.take() {
            pipe.write_all(request.to_string().as_bytes())?;
        }
        let out = child.wait_with_output()?;
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        let value: Value = serde_json::from_str(stdout.trim()).map_err(|e| {
            anyhow!("modal driver: unparseable response ({e}); stdout={stdout} stderr={stderr}")
        })?;
        if value["ok"].as_bool() != Some(true) {
            let msg = value["error"].as_str().unwrap_or("unknown error");
            bail!("modal driver: {msg}");
        }
        Ok(value)
    }
}

/// A deterministic fake sandbox id derived from the session uid (dry-run) — the
/// UUIDv7 tail keeps it unique per attempt, mirroring exe.dev's `worker_name`.
pub fn fake_sandbox_id(session_uid: &str) -> String {
    let tail: String = session_uid
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .take(12)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("sb-{tail}")
}

/// The tmux argv (against the worker's `-L disponent` server) for typing a line
/// into the agent's pane and pressing Enter — pure, unit-tested.
pub fn tmux_send_argv(input: &str) -> Vec<String> {
    vec![
        "tmux",
        "-L",
        TMUX_SOCKET,
        "send-keys",
        "-t",
        WORKER_SESSION,
        input,
        "Enter",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

/// The tmux argv snapshotting the worker's pane (poll-grade, scraped) — pure.
pub fn tmux_capture_argv() -> Vec<String> {
    vec![
        "tmux",
        "-L",
        TMUX_SOCKET,
        "capture-pane",
        "-p",
        "-t",
        WORKER_SESSION,
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

/// The tmux argv interrupting the agent in its pane (C-c; the pane survives).
pub fn tmux_interrupt_argv() -> Vec<String> {
    vec![
        "tmux",
        "-L",
        TMUX_SOCKET,
        "send-keys",
        "-t",
        WORKER_SESSION,
        "C-c",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

/// The tmux argv killing the worker session (the sandbox stays for REAP).
pub fn tmux_kill_argv() -> Vec<String> {
    vec![
        "tmux",
        "-L",
        TMUX_SOCKET,
        "kill-session",
        "-t",
        WORKER_SESSION,
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

/// The in-container `bash -c` bootstrap that lands a composed agent command in a
/// worker pane: write a run script that execs `agent_cmd`, open it in a detached
/// `worker` tmux session. Unlike exe.dev there's no ttyd — Modal exposes the pane
/// over the encrypted tunnel instead. `agent_cmd` already carries the brief
/// reference, so the brief is read at run time and never rides the tmux string.
pub fn container_bootstrap(agent_cmd: &str, otel_block: &str) -> String {
    let header = [
        format!("CLAUDE_CMD={}", shq(agent_cmd)),
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
"#;
    format!("{header}\n{body}")
}

impl EnvProvider for Modal {
    fn kind(&self) -> &'static str {
        "modal"
    }

    fn requires_template(&self) -> bool {
        // The template names the Modal image — force the dispatch to carry one,
        // matching exe.dev and the TEMPLATE→Image mapping story.
        true
    }

    fn ensure_template(
        &self,
        spec: &crate::backend::TemplateSpec,
    ) -> anyhow::Result<crate::backend::TemplateHandle> {
        if self.dry_run {
            return Ok(crate::backend::TemplateHandle {
                name: spec.name.clone(),
            });
        }
        self.driver(
            "image_build",
            &json!({"name": spec.name, "setup": spec.setup}),
        )?;
        Ok(crate::backend::TemplateHandle {
            name: spec.name.clone(),
        })
    }

    fn start(&self, req: &StartRequest) -> anyhow::Result<Provision> {
        // The worker's OTLP env is resolved here (uid known) and stashed on the
        // handle so `ModalCompute::spawn` can bake it into the run script.
        let otel = req
            .otel_endpoint
            .as_deref()
            .map(|e| crate::otel::worker_env(e, &req.session_uid))
            .unwrap_or_default();

        if self.dry_run {
            // Deterministic fake handle; the workspace URL is set when a port is
            // configured, so the tunnel/link path is exercised end-to-end.
            let sandbox_id = fake_sandbox_id(&req.session_uid);
            let workspace_url = self
                .workspace_port
                .map(|p| format!("https://{sandbox_id}-{p}.modal.host"));
            return Ok(Provision {
                handle: json!({
                    "sandboxId": sandbox_id,
                    "app": self.app,
                    "workspaceUrl": workspace_url,
                    "otel": otel,
                }),
                url: workspace_url,
            });
        }

        // Live: create the sandbox on the built image, push the brief, run the
        // env setup (clone + dispatch setup). The agent is NOT launched here —
        // the AgentAdapter `spawn`s it onto the Compute surface afterward.
        let created = self.driver("sandbox_create", &self.sandbox_create_request(req, &otel))?;
        let sandbox_id = created["sandboxId"]
            .as_str()
            .ok_or_else(|| anyhow!("modal driver: sandbox_create returned no sandboxId"))?
            .to_string();
        let workspace_url = created["workspaceUrl"].as_str().map(str::to_string);

        // The brief rides stdin into /tmp (it can be large), matching exe.dev.
        self.driver(
            "exec",
            &Modal::exec_request(
                &sandbox_id,
                &["bash", "-c", "cat > /tmp/disponent-brief.md"],
                Some(&req.brief),
            ),
        )?;
        self.driver(
            "exec",
            &Modal::exec_request(&sandbox_id, &["bash", "-s"], Some(&setup_script(req))),
        )?;

        Ok(Provision {
            handle: json!({
                "sandboxId": sandbox_id,
                "app": self.app,
                "workspaceUrl": workspace_url,
                "otel": otel,
            }),
            url: workspace_url,
        })
    }

    fn compute(&self, handle: &Value) -> anyhow::Result<Box<dyn Compute>> {
        Ok(Box::new(ModalCompute {
            dev: self.clone(),
            handle: handle.clone(),
        }))
    }

    fn launch_spec(&self, _req: &StartRequest) -> Option<crate::agent::LaunchSpec> {
        // The claude-code adapter composes the command; the env supplies the
        // agent binary + baseline flags and where START put the brief. The
        // tmux-in-container bootstrap that wraps it lives in `ModalCompute::spawn`.
        Some(crate::agent::LaunchSpec {
            agent_cmd: format!("claude {}", self.claude_flags),
            brief_ref: "\"$(cat /tmp/disponent-brief.md)\"".to_string(),
        })
    }

    fn reap(&self, handle: &Value) -> anyhow::Result<()> {
        if self.dry_run {
            return Ok(());
        }
        let sandbox_id = handle_sandbox_id(handle)?;
        self.driver("sandbox_terminate", &json!({"sandboxId": sandbox_id}))
            .map(|_| ())
    }

    fn survey(&self) -> anyhow::Result<Vec<(String, Value)>> {
        if self.dry_run {
            return Ok(vec![]);
        }
        let listed = self.driver("sandbox_list", &json!({"app": self.app}))?;
        let Some(sandboxes) = listed["sandboxes"].as_array() else {
            return Ok(vec![]);
        };
        Ok(sandboxes
            .iter()
            .filter_map(|sb| {
                let sandbox_id = sb["sandboxId"].as_str()?;
                let uid = sb["sessionUid"].as_str()?;
                Some((
                    uid.to_string(),
                    json!({"sandboxId": sandbox_id, "app": self.app}),
                ))
            })
            .collect())
    }
}

/// Read the sandbox id off an opaque handle.
fn handle_sandbox_id(handle: &Value) -> anyhow::Result<String> {
    handle["sandboxId"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("handle has no 'sandboxId': {handle}"))
}

/// The INTERACT surface for one Modal sandbox: a clone of the backend config
/// plus the worker's handle (its `sandboxId` is the exec target).
struct ModalCompute {
    dev: Modal,
    handle: Value,
}

impl ModalCompute {
    fn sandbox_id(&self) -> anyhow::Result<String> {
        handle_sandbox_id(&self.handle)
    }

    /// One `exec` through the driver; returns stdout, bailing on a non-zero exit
    /// with the separated streams (Modal keeps them apart).
    fn exec(&self, argv: &[&str], stdin: Option<&str>) -> anyhow::Result<String> {
        let sandbox_id = self.sandbox_id()?;
        let resp = self
            .dev
            .driver("exec", &Modal::exec_request(&sandbox_id, argv, stdin))?;
        let code = resp["exitCode"].as_i64().unwrap_or(0);
        let stdout = resp["stdout"].as_str().unwrap_or_default().to_string();
        if code != 0 {
            let stderr = resp["stderr"].as_str().unwrap_or_default();
            bail!(
                "modal exec ({}) exit {code}: {stdout}{stderr}",
                argv.join(" ")
            );
        }
        Ok(stdout)
    }

    /// An `exec` of a tmux command line (built on the Rust side) — the mechanism
    /// send/capture/interrupt/kill all share.
    fn tmux_exec(&self, argv: &[String]) -> anyhow::Result<String> {
        let borrowed: Vec<&str> = argv.iter().map(String::as_str).collect();
        self.exec(&borrowed, None)
    }
}

impl Compute for ModalCompute {
    fn run(&self, cmd: &str) -> anyhow::Result<String> {
        if self.dev.dry_run {
            return Ok(String::new());
        }
        self.exec(&["bash", "-lc", cmd], None)
    }

    fn spawn(&self, cmd: &str) -> anyhow::Result<()> {
        if self.dev.dry_run {
            return Ok(());
        }
        // `cmd` is the adapter's composed agent command line; land it by opening
        // the tmux-in-container worker session on it. OTLP env was stashed at START.
        let otel = self.handle["otel"].as_str().unwrap_or_default();
        let script = container_bootstrap(cmd, otel);
        self.exec(&["bash", "-c", &script], None).map(|_| ())
    }

    fn send(&self, input: &str) -> anyhow::Result<()> {
        if self.dev.dry_run {
            return Ok(());
        }
        self.tmux_exec(&tmux_send_argv(input)).map(|_| ())
    }

    fn capture(&self) -> anyhow::Result<String> {
        if self.dev.dry_run {
            return Ok(String::new());
        }
        self.tmux_exec(&tmux_capture_argv())
    }

    fn interrupt(&self) -> anyhow::Result<()> {
        if self.dev.dry_run {
            return Ok(());
        }
        self.tmux_exec(&tmux_interrupt_argv()).map(|_| ())
    }

    fn kill(&self) -> anyhow::Result<()> {
        if self.dev.dry_run {
            return Ok(());
        }
        self.tmux_exec(&tmux_kill_argv()).map(|_| ())
    }

    fn workspace_link(&self) -> anyhow::Result<Option<String>> {
        // The honest workspace link is the sandbox's encrypted tunnel URL, set on
        // the handle at START when a workspace port was exposed. No port → no
        // tunnel → honest None (never a faked link). This is the real improvement
        // over exe.dev's Remote-SSH deep link.
        Ok(self.handle["workspaceUrl"]
            .as_str()
            .filter(|u| !u.is_empty())
            .map(str::to_string))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(repo: Option<&str>) -> StartRequest {
        StartRequest {
            session_uid: "0198-abc-def0-123456789abc".into(),
            template: Some("claude-base".into()),
            repo: repo.map(String::from),
            isolation: None,
            git_ref: None,
            fetch_remote: false,
            setup: Some("cargo build".into()),
            brief: "do the thing".into(),
            otel_endpoint: None,
        }
    }

    #[test]
    fn sandbox_create_request_carries_the_mapping() {
        let m = Modal::dry_run();
        let r = m.sandbox_create_request(&req(Some("zmaril/entl")), "OTEL=1");
        // template → image name, dispatch setup rides along, tokens gate elsewhere.
        assert_eq!(r["app"], m.app.as_str());
        assert_eq!(r["image"], "claude-base");
        assert_eq!(r["setup"], "cargo build");
        assert_eq!(r["timeout"], m.timeout);
        assert_eq!(r["session_uid"], "0198-abc-def0-123456789abc");
        // dry_run() sets a workspace port so the tunnel path is exercised.
        assert_eq!(r["workspace_port"], 8080);
        assert_eq!(r["otel"], "OTEL=1");
    }

    #[test]
    fn image_falls_back_to_backend_default_then_none() {
        let mut m = Modal::dry_run();
        let mut r = req(None);
        r.template = None;
        assert!(m.image_name(&r).is_none(), "no template, no default → None");
        m.image = Some("baked".into());
        assert_eq!(m.image_name(&r).as_deref(), Some("baked"));
        r.template = Some("dispatch-img".into());
        assert_eq!(
            m.image_name(&r).as_deref(),
            Some("dispatch-img"),
            "the dispatch template wins over the default"
        );
    }

    #[test]
    fn exec_request_is_exact_argv_not_a_shell_string() {
        let r = Modal::exec_request("sb-1", &["tmux", "-L", "disponent", "kill-server"], None);
        assert_eq!(r["sandboxId"], "sb-1");
        assert_eq!(r["argv"][0], "tmux");
        assert_eq!(r["argv"][3], "kill-server");
        assert!(r["stdin"].is_null());
    }

    #[test]
    fn tmux_command_lines_target_the_worker_session() {
        assert_eq!(
            tmux_send_argv("hello"),
            vec![
                "tmux",
                "-L",
                "disponent",
                "send-keys",
                "-t",
                "worker",
                "hello",
                "Enter"
            ]
        );
        assert_eq!(
            tmux_capture_argv(),
            vec![
                "tmux",
                "-L",
                "disponent",
                "capture-pane",
                "-p",
                "-t",
                "worker"
            ]
        );
        assert_eq!(
            tmux_interrupt_argv(),
            vec![
                "tmux",
                "-L",
                "disponent",
                "send-keys",
                "-t",
                "worker",
                "C-c"
            ]
        );
        assert_eq!(
            tmux_kill_argv(),
            vec!["tmux", "-L", "disponent", "kill-session", "-t", "worker"]
        );
    }

    #[test]
    fn bootstrap_opens_the_agent_without_ttyd() {
        let agent_cmd = "claude --dangerously-skip-permissions \"$(cat /tmp/disponent-brief.md)\"";
        let s = container_bootstrap(agent_cmd, "OTEL=1");
        let pos = |needle: &str| {
            s.find(needle)
                .unwrap_or_else(|| panic!("{needle} in script"))
        };
        assert!(
            pos("cat /tmp/disponent-brief.md") < pos("tmux -L disponent new-session"),
            "brief wired before the tmux session opens"
        );
        // Modal exposes the pane over the tunnel, not ttyd.
        assert!(!s.contains("ttyd"), "modal uses the tunnel, not ttyd");
        assert!(s.contains("OTEL=1"));
    }

    #[test]
    fn fake_sandbox_id_is_deterministic_and_uid_derived() {
        assert_eq!(
            fake_sandbox_id("0198-abc-def0-123456789abc"),
            "sb-123456789abc"
        );
        // stable across calls, unique per uid tail
        assert_eq!(fake_sandbox_id("x"), fake_sandbox_id("x"));
        assert_ne!(fake_sandbox_id("aaaa1"), fake_sandbox_id("aaaa2"));
    }

    #[test]
    fn workspace_link_some_with_port_none_without() {
        // A handle carrying a tunnel URL → Some(url); no URL → honest None.
        let with = ModalCompute {
            dev: Modal::dry_run(),
            handle: json!({"sandboxId": "sb-1", "workspaceUrl": "https://sb-1-8080.modal.host"}),
        };
        assert_eq!(
            with.workspace_link().unwrap().as_deref(),
            Some("https://sb-1-8080.modal.host")
        );
        let without = ModalCompute {
            dev: Modal::dry_run(),
            handle: json!({"sandboxId": "sb-1", "workspaceUrl": Value::Null}),
        };
        assert_eq!(without.workspace_link().unwrap(), None);
    }

    #[test]
    fn dry_run_start_fabricates_a_handle_with_tunnel() {
        let m = Modal::dry_run();
        let p = m.start(&req(Some("zmaril/entl"))).unwrap();
        assert_eq!(p.handle["sandboxId"], "sb-123456789abc");
        // dry_run sets a workspace port, so the URL/link path is exercised.
        assert!(p.url.as_deref().unwrap().starts_with("https://"));
        assert_eq!(p.handle["workspaceUrl"], p.url.clone().unwrap().as_str());
    }
}
