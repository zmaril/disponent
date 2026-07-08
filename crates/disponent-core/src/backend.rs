//! The exe.dev backend (the powdermonkey extraction): provision a throwaway
//! worker VM per session by copying an already-authed template VM, clone the
//! repo, run the setup chain, launch the agent in tmux, expose it over ttyd.
//!
//! Everything shells out to the exe.dev CLI, which is itself just `ssh exe.dev
//! <cmd>` (and `ssh <vm>.exe.xyz` to reach a worker). Arg-building and the
//! remote bootstrap script are pure functions so they're unit-tested without
//! touching the network; only the thin spawn wrappers go untested. `dry_run`
//! fabricates every result — the engine-level tests run on it.

use std::process::Command;

use anyhow::{anyhow, bail};

/// Everything a worker needs to exist: the template to copy, what to clone,
/// how to set it up, and the brief the agent starts with.
pub struct ProvisionRequest {
    pub session_uid: String,
    /// The env-side base image to copy (exe.dev template VM name); backends
    /// with `requires_template()` reject a dispatch without one.
    pub template: Option<String>,
    /// `owner/repo` (gh-clonable) — empty means pure-prompt work, no clone.
    pub repo: Option<String>,
    /// Per-dispatch setup, run after the template's baseline and the clone.
    pub setup: Option<String>,
    pub brief: String,
}

pub struct Provisioned {
    pub vm_name: String,
    pub host: String,
    pub url: String,
}

/// One environment family (exe.dev VMs, local tmux, …): provision workers,
/// poke them, tear them down, and answer "what's out there" for reconcile.
/// Handles are opaque JSON at the engine level — each backend defines and
/// parses its own shape.
pub trait EnvBackend: Send + Sync {
    /// Matches the EnvKind wire value ("exe_dev", "local", …).
    fn kind(&self) -> &'static str;

    /// Does dispatch demand a template (an env-side base image to copy)?
    fn requires_template(&self) -> bool;

    fn provision(&self, req: &ProvisionRequest) -> anyhow::Result<Provision>;

    /// Stop the agent, keep the environment for inspection (cancel's half).
    fn stop(&self, handle: &serde_json::Value) -> anyhow::Result<()>;

    fn send(&self, handle: &serde_json::Value, input: &str) -> anyhow::Result<()>;

    /// Destroy the environment's resources (reap's half).
    fn teardown(&self, handle: &serde_json::Value) -> anyhow::Result<()>;

    /// The sessions discoverable in the environment right now, as
    /// (session_uid, handle) — what reconcile confirms/adopts against.
    fn survey(&self) -> anyhow::Result<Vec<(String, serde_json::Value)>>;
}

/// What a backend hands the engine for a fresh worker.
pub struct Provision {
    pub handle: serde_json::Value,
    pub url: Option<String>,
}

fn handle_str(handle: &serde_json::Value, key: &str) -> anyhow::Result<String> {
    handle[key]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("handle has no '{key}': {handle}"))
}

impl EnvBackend for ExeDev {
    fn kind(&self) -> &'static str {
        "exe_dev"
    }

    fn requires_template(&self) -> bool {
        true
    }

    fn provision(&self, req: &ProvisionRequest) -> anyhow::Result<Provision> {
        // (inherent method — resolution prefers it over this trait method)
        let p = ExeDev::provision(self, req)?;
        Ok(Provision {
            handle: serde_json::json!({"vmName": p.vm_name, "host": p.host}),
            url: Some(p.url),
        })
    }

    fn stop(&self, handle: &serde_json::Value) -> anyhow::Result<()> {
        ExeDev::stop(self, &handle_str(handle, "host")?)
    }

    fn send(&self, handle: &serde_json::Value, input: &str) -> anyhow::Result<()> {
        ExeDev::send(self, &handle_str(handle, "host")?, input)
    }

    fn teardown(&self, handle: &serde_json::Value) -> anyhow::Result<()> {
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

#[derive(Debug, PartialEq)]
pub struct Vm {
    pub vm_name: String,
    pub tags: Vec<String>,
}

pub struct ExeDev {
    ssh: String,
    /// The control endpoint (`ssh <control> cp|tag|rm|ls`).
    control: String,
    /// Per-VM host suffix (`<vm><suffix>` is ssh/https reachable).
    host_suffix: String,
    ttyd_port: u16,
    claude_flags: String,
    dry_run: bool,
}

/// The tag every disponent worker carries, so `ls` can find ours without
/// guessing from names.
pub const WORKER_TAG: &str = "disponent-worker";

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
        }
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

    /// Copy the template, tag it, wait for sshd, push the brief, bootstrap.
    /// Each step early-exits with a stage-prefixed error.
    pub fn provision(&self, req: &ProvisionRequest) -> anyhow::Result<Provisioned> {
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

        // The brief rides stdin (no scp temp-file dance; it can be large).
        self.worker(
            &host,
            &["bash", "-c", "cat > /tmp/disponent-brief.md"],
            Some(&req.brief),
        )
        .map_err(|e| anyhow!("push brief: {e}"))?;

        self.worker(&host, &["bash", "-s"], Some(&bootstrap_script(self, req)))
            .map_err(|e| anyhow!("worker bootstrap: {e}"))?;

        Ok(Provisioned { vm_name, host, url })
    }

    /// Delete a worker VM.
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

    /// Stop the agent (kill its tmux session) but leave the VM for inspection —
    /// cancel's half of the cancel/reap split; reap is what deletes the VM.
    pub fn stop(&self, host: &str) -> anyhow::Result<()> {
        self.worker_tmux(host, &["kill-session", "-t", "worker"])
            .map(|_| ())
    }

    /// Type into the worker's tmux session (the agent's terminal).
    pub fn send(&self, host: &str, input: &str) -> anyhow::Result<()> {
        self.worker_tmux(host, &["send-keys", "-t", "worker", input, "Enter"])
            .map(|_| ())
    }

    /// A snapshot of the worker's terminal (poll-grade observation, scraped).
    pub fn capture(&self, host: &str) -> anyhow::Result<String> {
        self.worker_tmux(host, &["capture-pane", "-p", "-t", "worker"])
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

/// The remote `bash -s` script that turns a fresh worker into a running
/// session, in the design's setup order: the template's baseline already ran
/// (it's baked into the image), then the repo clone, then the dispatch's
/// setup, then the agent in a detached tmux session exposed over ttyd.
/// Injected values are single-quoted (shq) so a repo slug or setup line can't
/// break out of its assignment; the brief is `cat`-ed at run time so it never
/// rides a tmux command string.
pub fn bootstrap_script(backend: &ExeDev, req: &ProvisionRequest) -> String {
    let header = [
        format!("REPO_SLUG={}", shq(req.repo.as_deref().unwrap_or(""))),
        format!("CLAUDE_FLAGS={}", shq(&backend.claude_flags)),
        format!("TTYD_PORT={}", shq(&backend.ttyd_port.to_string())),
    ]
    .join("\n");
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
    let launch = r#"
{
  echo '#!/usr/bin/env bash'
  echo 'export PATH="$HOME/.bun/bin:$PATH"'
  echo 'cd "$1"'
  echo "claude $CLAUDE_FLAGS \"\$(cat /tmp/disponent-brief.md)\" || true"
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
    format!("{header}\n{body}\n# ── dispatch setup ──\n{setup}\n{launch}")
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

    fn req(repo: Option<&str>, setup: Option<&str>) -> ProvisionRequest {
        ProvisionRequest {
            session_uid: "0198-abc-def0-123456789abc".into(),
            template: Some("claude-base".into()),
            repo: repo.map(String::from),
            setup: setup.map(String::from),
            brief: "do the thing".into(),
        }
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
    fn bootstrap_script_orders_clone_setup_agent() {
        let b = ExeDev::dry_run();
        let s = bootstrap_script(&b, &req(Some("zmaril/entl"), Some("cargo build")));
        let pos = |needle: &str| {
            s.find(needle)
                .unwrap_or_else(|| panic!("{needle} in script"))
        };
        assert!(
            pos("gh repo clone") < pos("cargo build"),
            "clone before setup"
        );
        assert!(
            pos("cargo build") < pos("tmux -L disponent new-session"),
            "setup before agent"
        );
        assert!(s.contains("REPO_SLUG='zmaril/entl'"));
        assert!(s.contains("cat /tmp/disponent-brief.md"));

        // no repo → no clone, still a work dir
        let s = bootstrap_script(&b, &req(None, None));
        assert!(s.contains("REPO_SLUG=''"));
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
