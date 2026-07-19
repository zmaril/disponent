use super::*;
use crate::backend::EnvProvider;
use crate::local::LocalTmux;

/// A dry-run local Compute: every verb is a fabricated success, so the adapter's
/// orchestration is exercised without a tmux server or a model.
fn compute() -> Box<dyn Compute> {
    let backend = LocalTmux::dry_run();
    let handle = serde_json::json!({"tmux": "dsp-u", "socket": "s", "workDir": "/tmp/x/u"});
    backend.compute(&handle).unwrap()
}

#[test]
fn launch_spec_composes_flags_then_brief_argv() {
    let spec = LaunchSpec {
        agent_cmd: "claude --dangerously-skip-permissions".into(),
        brief_ref: Some("\"$(cat ../brief.md)\"".into()),
    };
    // Exactly the line the env used to bake in: agent + flags, brief as final argv.
    assert_eq!(
        spec.command(),
        "claude --dangerously-skip-permissions \"$(cat ../brief.md)\""
    );
    // With no brief_ref (a per-dispatch agent_cmd override), the command runs
    // verbatim — nothing appended.
    let verbatim = LaunchSpec {
        agent_cmd: "claude --teleport abc123".into(),
        brief_ref: None,
    };
    assert_eq!(verbatim.command(), "claude --teleport abc123");
}

#[test]
fn claude_code_names_the_catalog_agent() {
    assert_eq!(ClaudeCode.agent(), "claude-code");
}

#[test]
fn lifecycle_verbs_run_over_a_compute_surface() {
    let c = compute();
    let spec = LaunchSpec {
        agent_cmd: "claude".into(),
        brief_ref: Some("\"$(cat ../brief.md)\"".into()),
    };
    // install/auth are honest no-ops (host-provided / template-baked), not fakes.
    ClaudeCode.install(&*c).unwrap();
    ClaudeCode.auth(&*c).unwrap();
    // start launches the composed command; prompt delivers a follow-up.
    ClaudeCode.start(&*c, &spec).unwrap();
    ClaudeCode.prompt(&*c, "how's it going?").unwrap();
    // stop verbs delegate to the Compute primitives.
    ClaudeCode.stop_work(&*c).unwrap();
    ClaudeCode.stop_exec(&*c).unwrap();
}

#[test]
fn monitor_is_scraped_and_output_is_honestly_unwired() {
    let c = compute();
    let obs = ClaudeCode.monitor(&*c).unwrap();
    assert_eq!(obs.fidelity, "scraped", "a pane scrape is not exact");

    let out = ClaudeCode.output(&*c).unwrap();
    assert!(
        !out.available,
        "output collection isn't wired yet — don't fake a result"
    );
    assert!(out.detail.is_some());
}
