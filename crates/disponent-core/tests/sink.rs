//! The sink gate: an engine opened over a SQLite file mirrors the catalog and
//! every ledger change; the file is readable as plain SQL afterwards.

use disponent_core::mcp_generated::{DispatchSpec, DisponentMcp};
use disponent_core::Engine;

fn tmpfile() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("disponent-sink-{}.sqlite3", std::process::id()))
}

#[test]
fn sqlite_mirror_tracks_the_ledger() {
    let path = tmpfile();
    let _ = std::fs::remove_file(&path);

    // no backends: the row set below stays deterministic (nothing provisions)
    let engine = Engine::open_with(Some(path.to_str().unwrap()), vec![]).unwrap();
    // dispatch to `local` — unbacked, so the row set is deterministic (no
    // background provisioner racing the assertions below)
    let spec: DispatchSpec = serde_json::from_value(serde_json::json!({
        "brief": "prove the mirror",
        "env": "local",
        "labels": {"suite": "sink"},
    }))
    .unwrap();
    let session = engine.dispatch(spec).unwrap();
    engine.cancel(session.uid.clone()).unwrap();
    engine.reap(session.uid.clone()).unwrap();
    drop(engine);

    let conn = rusqlite::Connection::open(&path).unwrap();
    let one = |sql: &str| -> String { conn.query_row(sql, [], |r| r.get::<_, String>(0)).unwrap() };
    let count = |sql: &str| -> i64 { conn.query_row(sql, [], |r| r.get(0)).unwrap() };

    // the shipped catalog seeded (idempotently — reopen below re-seeds)
    assert_eq!(count("SELECT count(*) FROM environments"), 3);
    assert_eq!(count("SELECT count(*) FROM offerings"), 9);
    assert_eq!(
        count("SELECT count(*) FROM offerings WHERE is_default = 1"),
        3
    );
    assert!(count("SELECT count(*) FROM env_capabilities") > 0);

    // the dispatch row, resolved
    assert_eq!(one("SELECT brief FROM dispatches"), "prove the mirror");
    assert_eq!(one("SELECT agent_name FROM dispatches"), "claude-code");
    assert_eq!(one("SELECT model_id FROM dispatches"), "claude-opus-4-8");
    assert_eq!(one("SELECT labels FROM dispatches"), r#"{"suite":"sink"}"#);

    // the session's final state (upserts converged on one row)
    assert_eq!(count("SELECT count(*) FROM sessions"), 1);
    assert_eq!(one("SELECT state FROM sessions"), "cancelled");
    assert_eq!(
        count("SELECT count(*) FROM sessions WHERE reaped_at IS NOT NULL"),
        1
    );

    // the timeline: the dispatch log + the cancel transition, twin-columned
    assert_eq!(count("SELECT count(*) FROM events"), 2);
    assert_eq!(
        one("SELECT payload_kind FROM events WHERE idx = 1"),
        "state"
    );
    assert_eq!(
        one("SELECT json_extract(payload, '$.to') FROM events WHERE idx = 1"),
        "cancelled"
    );

    // reopening re-seeds without duplicating (upserts are idempotent)
    drop(conn);
    let _engine = Engine::open(Some(path.to_str().unwrap())).unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    let count = |sql: &str| -> i64 { conn.query_row(sql, [], |r| r.get(0)).unwrap() };
    assert_eq!(count("SELECT count(*) FROM environments"), 3);
    assert_eq!(count("SELECT count(*) FROM env_capabilities"), 23);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn reopen_rehydrates_the_ledger() {
    let path = std::env::temp_dir().join(format!(
        "disponent-rehydrate-{}.sqlite3",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);

    let (uid, dispatch_id, message_id) = {
        let engine = Engine::open_with(Some(path.to_str().unwrap()), vec![]).unwrap();
        let spec: DispatchSpec = serde_json::from_value(serde_json::json!({
            "brief": "outlive the process",
            "env": "local",
            "title": "rehydrate me",
            "tags": ["projectA"],
            "labels": {"k": "v"},
        }))
        .unwrap();
        let s = engine.dispatch(spec).unwrap();
        // a control-plane message: disponent owns it, so the mirror is its
        // durability (§11) — it must survive the restart below.
        let minted = engine
            .send(
                "use bun".into(),
                Some(serde_json::from_value(serde_json::json!({ "tags": ["projectA"] })).unwrap()),
                None,
                Some("package-manager".into()),
            )
            .unwrap();
        assert_eq!(minted.len(), 1, "tag resolved the queued session");
        engine.cancel(s.uid.clone()).unwrap();
        (s.uid, s.dispatch_id, minted[0].id.clone())
    }; // first disponent dies

    let engine = Engine::open_with(Some(path.to_str().unwrap()), vec![]).unwrap();
    let session = engine.session(uid.clone()).unwrap().expect("rehydrated");
    assert_eq!(session.state, "cancelled");
    assert_eq!(session.dispatch_id, dispatch_id);

    // the message rehydrated (durability is the mirror), and ack works on it
    let inbox = engine
        .messages(Some(
            serde_json::from_value(serde_json::json!({ "sessionUid": uid })).unwrap(),
        ))
        .unwrap();
    assert_eq!(inbox.len(), 1, "the message came back");
    assert_eq!(inbox[0].id, message_id);
    assert_eq!(inbox[0].body, "use bun");
    assert_eq!(inbox[0].topic.as_deref(), Some("package-manager"));
    assert!(inbox[0].acked_at.is_none());
    engine.ack(message_id.clone()).unwrap();
    let acked = engine
        .messages(Some(
            serde_json::from_value(serde_json::json!({ "sessionUid": uid })).unwrap(),
        ))
        .unwrap();
    assert!(acked[0].acked_at.is_some(), "ack stamped after rehydrate");

    // the dispatch row came back too: env-filtered listing works through it
    let by_env = engine
        .sessions(Some(
            serde_json::from_value(serde_json::json!({"env": "local"})).unwrap(),
        ))
        .unwrap();
    assert_eq!(by_env.len(), 1);

    // events kept order and their payload envelopes: dispatch log, the send's
    // mail breadcrumb, then the cancel transition
    let events = engine
        .events(
            Some(serde_json::from_value(serde_json::json!({"sessionUid": uid})).unwrap()),
            None,
            None,
        )
        .unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].kind, "log");
    assert_eq!(events[1].kind, "mail");
    assert_eq!(events[1].payload["payload"]["messageId"], message_id);
    assert_eq!(events[2].payload["kind"], "state");
    assert_eq!(events[2].payload["payload"]["to"], "cancelled");

    // lifecycle ops work on rehydrated rows, and the result survives ANOTHER restart
    let reaped = engine.reap(uid.clone()).unwrap();
    assert!(reaped.reaped_at.is_some());
    drop(engine);
    let third = Engine::open_with(Some(path.to_str().unwrap()), vec![]).unwrap();
    assert!(third.session(uid).unwrap().unwrap().reaped_at.is_some());

    let _ = std::fs::remove_file(&path);
}
