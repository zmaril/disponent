//! The Modal backed lifecycle over the dry-run backend (nothing spawned, the
//! Python driver is never invoked): dispatch provisions in the background and the
//! session reaches `running` with a sandbox handle + a tunnel url; a template-less
//! backed dispatch is rejected (Modal `requires_template`); send records the
//! supervisor message; cancel stops the agent but keeps the sandbox; reap tears
//! down. Mirrors `backend_lifecycle.rs` (exe.dev), the second registered backend.

use std::time::Duration;

use disponent_core::mcp_generated::{DispatchSpec, DisponentMcp, Session};
use disponent_core::modal::Modal;
use disponent_core::Engine;

mod common;

fn spec() -> DispatchSpec {
    serde_json::from_value(serde_json::json!({
        "brief": "say hi and exit",
        "env": "modal",
        "repo": "zmaril/entl",
        "template": "claude-base",
    }))
    .unwrap()
}

fn wait_for(engine: &Engine, uid: &str, state: &str) -> Session {
    common::wait_for(engine, uid, state, Duration::from_secs(5))
}

#[test]
fn modal_dispatch_runs_sends_cancels_reaps() {
    let engine = Engine::with_backend(Modal::dry_run());

    // a backed dispatch demands a template (Modal names the image with it)
    let mut no_template = spec();
    no_template.template = None;
    let err = engine.dispatch(no_template).unwrap_err().to_string();
    assert!(err.contains("template"), "{err}");

    let session = engine.dispatch(spec()).unwrap();
    let running = wait_for(&engine, &session.uid, "running");
    assert!(running.started_at.is_some());

    // the handle carries the deterministic dry-run sandbox id + a tunnel url
    let handle = running.env_handle.clone().unwrap();
    let sandbox_id = handle["sandboxId"].as_str().unwrap();
    assert!(
        sandbox_id.starts_with("sb-"),
        "sandbox id shape: {sandbox_id}"
    );
    assert_eq!(handle["app"], "disponent");
    // dry_run() exposes a workspace port, so START surfaces the tunnel URL — the
    // honest workspace link Modal can give that exe.dev can't.
    let url = running.url.as_deref().unwrap();
    assert!(url.starts_with("https://"), "tunnel url: {url}");
    assert_eq!(handle["workspaceUrl"].as_str().unwrap(), url);

    // a Manager send to the running worker mints a Message (recipient=worker,
    // sender=manager) and projects a `mail` breadcrumb on its timeline (exact).
    let minted = engine
        .send(
            "how's it going?".into(),
            Some(serde_json::from_value(serde_json::json!({"sessions": [session.uid]})).unwrap()),
            None,
            None,
        )
        .unwrap();
    assert_eq!(minted.len(), 1);
    assert_eq!(minted[0].sender, "manager");
    assert_eq!(minted[0].recipient, "worker");
    assert_eq!(minted[0].session_uid, session.uid);
    let last = engine
        .events(
            Some(serde_json::from_value(serde_json::json!({"sessionUid": session.uid})).unwrap()),
            Some(4),
            None,
        )
        .unwrap();
    assert_eq!(last[0].kind, "mail");
    assert_eq!(last[0].fidelity, "exact");
    assert_eq!(last[0].payload["payload"]["messageId"], minted[0].id);

    // cancel stops the agent, keeps the sandbox handle; reap clears the board
    let cancelled = engine.cancel(session.uid.clone()).unwrap();
    assert_eq!(cancelled.state, "cancelled");
    assert!(
        cancelled.env_handle.is_some(),
        "sandbox kept for inspection"
    );
    // a send to a non-running anchor still records a durable Message (the
    // recipient pulls it) — it just isn't delivered to a live prompt.
    let after_cancel = engine
        .send(
            "hello?".into(),
            Some(serde_json::from_value(serde_json::json!({"sessions": [session.uid]})).unwrap()),
            None,
            None,
        )
        .unwrap();
    assert_eq!(after_cancel.len(), 1);
    let reaped = engine.reap(session.uid.clone()).unwrap();
    assert!(reaped.reaped_at.is_some());
}

#[test]
fn modal_survey_lists_nothing_in_dry_run() {
    // The dry-run backend lists no sandboxes — a running session's worker is gone,
    // so reconcile marks it lost (same honest shape as exe.dev).
    let engine = Engine::with_backend(Modal::dry_run());
    let session = engine.dispatch(spec()).unwrap();
    wait_for(&engine, &session.uid, "running");

    let report = engine.reconcile().unwrap();
    assert_eq!(report.lost, 1);
    assert_eq!(report.confirmed, 0);
    let s = engine.session(session.uid).unwrap().unwrap();
    assert_eq!(s.state, "lost");
}
