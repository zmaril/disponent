//! The backed lifecycle over the dry-run backend (nothing spawned): dispatch
//! provisions in the background and the session reaches `running` with a
//! handle + url; send records the supervisor message; cancel stops the agent
//! but keeps the VM; reap tears down; reconcile marks vanished workers lost.
//!
//! straitjacket-allow-file:duplication — `modal_lifecycle.rs` mirrors this
//! exe.dev lifecycle test faithfully by design (same `spec`/`wait_for` helpers
//! and the same dispatch → running → send → cancel → reap dance over the
//! dry-run backend); the parallel blocks are intentional, per-backend copies.

use std::time::Duration;

use disponent_core::backend::ExeDev;
use disponent_core::mcp_generated::{DispatchSpec, DisponentMcp, Session};
use disponent_core::Engine;

mod common;

fn spec() -> DispatchSpec {
    serde_json::from_value(serde_json::json!({
        "brief": "say hi and exit",
        "env": "exe-dev",
        "repo": "zmaril/entl",
        "template": "claude-base",
    }))
    .unwrap()
}

fn wait_for(engine: &Engine, uid: &str, state: &str) -> Session {
    common::wait_for(engine, uid, state, Duration::from_secs(5))
}

#[test]
fn backed_dispatch_runs_sends_cancels_reaps() {
    let engine = Engine::with_backend(ExeDev::dry_run());

    // a backed dispatch demands a template
    let mut no_template = spec();
    no_template.template = None;
    let err = engine.dispatch(no_template).unwrap_err().to_string();
    assert!(err.contains("template"), "{err}");

    let session = engine.dispatch(spec()).unwrap();
    let running = wait_for(&engine, &session.uid, "running");
    assert!(running.started_at.is_some());
    let handle = running.env_handle.clone().unwrap();
    assert_eq!(
        handle["host"],
        format!("{}.exe.xyz", handle["vmName"].as_str().unwrap())
    );
    assert!(running.url.as_deref().unwrap().starts_with("https://"));

    // the timeline so far: accepted log → provisioning → running → worker-up log
    let states: Vec<String> = engine
        .events(
            Some(serde_json::from_value(serde_json::json!({"sessionUid": session.uid})).unwrap()),
            None,
            None,
        )
        .unwrap()
        .iter()
        .map(|e| e.kind.clone())
        .collect();
    assert_eq!(states, ["log", "state", "state", "log"]);

    // send lands on the running worker and records the supervisor message
    engine
        .send(session.uid.clone(), "how's it going?".into())
        .unwrap();
    let last = engine
        .events(
            Some(serde_json::from_value(serde_json::json!({"sessionUid": session.uid})).unwrap()),
            Some(4),
            None,
        )
        .unwrap();
    assert_eq!(last[0].kind, "message");
    assert_eq!(last[0].payload["payload"]["role"], "supervisor");

    // cancel stops the agent, keeps the handle; reap clears the board
    let cancelled = engine.cancel(session.uid.clone()).unwrap();
    assert_eq!(cancelled.state, "cancelled");
    assert!(cancelled.env_handle.is_some(), "VM kept for inspection");
    assert!(engine.send(session.uid.clone(), "hello?".into()).is_err());
    let reaped = engine.reap(session.uid.clone()).unwrap();
    assert!(reaped.reaped_at.is_some());
}

#[test]
fn coarse_backend_emits_no_delivery_verdict() {
    // exe.dev can't diff the worker's file system, so its delivery_signal is the
    // honest default None — reap must emit NO delivery event (no faked verdict).
    let engine = Engine::with_backend(ExeDev::dry_run());
    let session = engine.dispatch(spec()).unwrap();
    wait_for(&engine, &session.uid, "running");
    engine.reap(session.uid.clone()).unwrap();

    let events = engine
        .events(
            Some(
                serde_json::from_value(serde_json::json!({"sessionUid": session.uid.clone()}))
                    .unwrap(),
            ),
            None,
            None,
        )
        .unwrap();
    assert!(
        !events
            .iter()
            .any(|e| e.kind == "raw" && e.payload["payload"]["source"] == "delivery"),
        "coarse backend must not fake a delivery verdict"
    );
    let reaped = engine.session(session.uid).unwrap().unwrap();
    assert!(
        reaped.exit_detail.is_none(),
        "no delivery note on a coarse reap: {:?}",
        reaped.exit_detail
    );
}

#[test]
fn reconcile_marks_vanished_workers_lost() {
    let engine = Engine::with_backend(ExeDev::dry_run());
    let session = engine.dispatch(spec()).unwrap();
    wait_for(&engine, &session.uid, "running");

    // the dry-run backend lists no VMs — a running session's worker is gone
    let report = engine.reconcile().unwrap();
    assert_eq!(report.lost, 1);
    assert_eq!(report.confirmed, 0);
    let s = engine.session(session.uid.clone()).unwrap().unwrap();
    assert_eq!(s.state, "lost");
    assert!(s.ended_at.is_some());

    // idempotent: already-lost sessions don't count again
    let again = engine.reconcile().unwrap();
    assert_eq!(again.lost, 0);
}

#[test]
fn unbacked_engine_still_queues() {
    let engine = Engine::new();
    // no backend: the same spec queues instead of provisioning (and doesn't
    // demand a template)
    let mut s = spec();
    s.template = None;
    let session = engine.dispatch(s).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let still = engine.session(session.uid).unwrap().unwrap();
    assert_eq!(still.state, "queued");
}
