//! The agent-adapter seam: HOW a specific agent CLI runs on top of a
//! [`Compute`] surface.
//!
//! An [`EnvProvider`](crate::backend::EnvProvider) owns *where* a process runs
//! (TEMPLATE/START/REAP) and exposes the [`Compute`] INTERACT surface. An
//! [`AgentAdapter`] owns *how you drive one agent's CLI* on that surface — its
//! whole lifecycle: make the binary present (install), make credentials present
//! (auth), launch it (start), feed it briefs/follow-ups (prompt), read its state
//! (monitor), collect its result (output), and the two stop verbs the design
//! names — stop_work (interrupt the current work, process stays) and stop_exec
//! (kill the process, the env stays until reap).
//!
//! The split mirrors backend selection: a provider is picked by env-kind, an
//! adapter by the catalog `agent` string. That's the whole point — a second
//! agent CLI is a NEW ADAPTER, not a new backend. The provider composes a
//! [`LaunchSpec`] from its env config (the agent binary + baseline flags, and
//! the brief file it already wrote); the adapter composes the command line and
//! hands it to `Compute::spawn`, which lands it wherever that env's pane lives
//! (local tmux, remote tmux-over-ssh). The adapter re-hardcodes nothing
//! env-specific.

use anyhow::Result;

use crate::backend::Compute;

/// Everything an adapter needs to compose an agent's launch command on a
/// [`Compute`] surface WITHOUT knowing which env it's on. The provider fills it
/// from env config + the catalog, so the adapter hardcodes no env details.
pub struct LaunchSpec {
    /// The agent binary plus its baseline flags, ready to prefix the brief
    /// argv. This is the env's own config, not the adapter's: local's
    /// `agent_cmd` (default `claude --dangerously-skip-permissions`), exe.dev's
    /// `claude <claude_flags>`.
    pub agent_cmd: String,
    /// A shell substitution that expands to the brief as a single argv token.
    /// The provider wrote the brief file during START and names where to read
    /// it — local `"$(cat ../brief.md)"`, exe.dev `"$(cat /tmp/disponent-brief.md)"`.
    /// Passing the *reference* (not the text) keeps a large brief off the
    /// command string, exactly as before.
    pub brief_ref: String,
}

impl LaunchSpec {
    /// The composed agent command line: the agent binary + flags, with the
    /// brief as its final argv. This is what the adapter spawns; each env's
    /// `Compute::spawn` decides whether it lands as keystrokes in a local tmux
    /// pane or as a bootstrap on a remote worker.
    pub fn command(&self) -> String {
        format!("{} {}", self.agent_cmd, self.brief_ref)
    }
}

/// A poll-grade read of the agent's state, scraped from its terminal — the
/// observation the watcher already produces. PR-3 wires the #25 detectors that
/// turn a pane into exact state/usage; for now this carries the raw pane and is
/// honestly marked `scraped`.
pub struct AgentObservation {
    /// Observation fidelity — `scraped` until real detectors land (PR-3).
    pub fidelity: &'static str,
    /// The current terminal snapshot; the watcher diffs it into timeline events.
    pub pane: String,
}

/// The agent's final result / artifacts. Honest-minimal until PR-3+ wires real
/// artifact fetch via `Compute::run` (still unwired from PR-1): `available` is
/// false and `detail` says why, rather than faking a result.
pub struct AgentOutput {
    pub available: bool,
    pub detail: Option<String>,
}

/// How one agent CLI is driven on a [`Compute`] surface. Selected by the
/// catalog `agent` string, the way an `EnvProvider` is selected by env-kind.
pub trait AgentAdapter: Send + Sync {
    /// Matches the catalog `agent` string (e.g. `"claude-code"`).
    fn agent(&self) -> &'static str;

    /// Ensure the agent CLI is present on the worker. Host-provided locally /
    /// baked into the template on exe.dev, so an honest no-op today — see the
    /// impls. A stage that genuinely can't run must fail saying what's missing,
    /// never fake success.
    fn install(&self, c: &dyn Compute) -> Result<()>;

    /// Ensure the agent's credentials are present. Sourced from config /
    /// templates, NEVER the schema. Honest no-op today (see the impls).
    fn auth(&self, c: &dyn Compute) -> Result<()>;

    /// Launch the agent process: compose the command from the [`LaunchSpec`]
    /// and `Compute::spawn` it. The brief rides the launch argv (`brief_ref`),
    /// so START-time delivery is the same argv it always was.
    fn start(&self, c: &dyn Compute, launch: &LaunchSpec) -> Result<()>;

    /// Deliver a brief / follow-up to the running agent (was the engine's
    /// `send` → `Compute::send`).
    fn prompt(&self, c: &dyn Compute, text: &str) -> Result<()>;

    /// Read the agent's current state from its terminal. Today wraps the
    /// existing capture-scrape at `scraped` fidelity; PR-3 adds the detectors.
    fn monitor(&self, c: &dyn Compute) -> Result<AgentObservation>;

    /// Collect the agent's final result / artifacts. Honest-minimal now.
    fn output(&self, c: &dyn Compute) -> Result<AgentOutput>;

    /// Tell the agent to stop its current work (interrupt) — the process stays
    /// alive. Delegates to the [`Compute`] interrupt primitive.
    fn stop_work(&self, c: &dyn Compute) -> Result<()>;

    /// Kill the agent process — the env stays for inspection until reap.
    /// Delegates to the [`Compute`] kill primitive.
    fn stop_exec(&self, c: &dyn Compute) -> Result<()>;
}

/// The `claude-code` adapter: drive the claude CLI on any [`Compute`] surface.
/// It owns the agent's whole lifecycle; the env only says *where* (via the
/// provider) and hands over the [`LaunchSpec`] pieces.
pub struct ClaudeCode;

impl AgentAdapter for ClaudeCode {
    fn agent(&self) -> &'static str {
        "claude-code"
    }

    fn install(&self, _c: &dyn Compute) -> Result<()> {
        // Honest no-op: the claude CLI is host-provided locally (on `PATH`) and
        // baked into the exe.dev template image. Neither env installs it at
        // dispatch time today, so there's nothing to do — and nothing to fake.
        // PR-3+ can add a real presence check via `Compute::run` here.
        Ok(())
    }

    fn auth(&self, _c: &dyn Compute) -> Result<()> {
        // Honest no-op: credentials come from config / the authed template VM,
        // never the schema (secrets stay out of the ledger). Locally the CLI
        // uses the host's existing login; on exe.dev the template is pre-authed.
        Ok(())
    }

    fn start(&self, c: &dyn Compute, launch: &LaunchSpec) -> Result<()> {
        // Compose `claude <flags> "<brief-ref>"` and let the env's Compute::spawn
        // land it — keystrokes into the local tmux pane, or the tmux-over-ttyd
        // worker bootstrap on exe.dev. The brief rides the argv exactly as before.
        c.spawn(&launch.command())
    }

    fn prompt(&self, c: &dyn Compute, text: &str) -> Result<()> {
        c.send(text)
    }

    fn monitor(&self, c: &dyn Compute) -> Result<AgentObservation> {
        // For now the agent's state is whatever we can scrape off its pane —
        // the same capture the watcher already diffs. Marked `scraped` because
        // that's what it is; PR-3's detectors upgrade the fidelity.
        Ok(AgentObservation {
            fidelity: "scraped",
            pane: c.capture()?,
        })
    }

    fn output(&self, _c: &dyn Compute) -> Result<AgentOutput> {
        // Honest-minimal: `Compute::run` (the one-shot needed to fetch a real
        // result/artifact list) is unwired from PR-1, so we don't fabricate an
        // outcome. PR-3+ wires artifact fetch here.
        Ok(AgentOutput {
            available: false,
            detail: Some("agent output collection isn't wired yet".to_string()),
        })
    }

    fn stop_work(&self, c: &dyn Compute) -> Result<()> {
        // Interrupt the running work; the process (and env) stay.
        c.interrupt()
    }

    fn stop_exec(&self, c: &dyn Compute) -> Result<()> {
        // Kill the agent process; the env stays until reap destroys it.
        c.kill()
    }
}

#[cfg(test)]
mod tests;
