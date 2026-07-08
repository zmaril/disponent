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

    let engine = Engine::open(Some(path.to_str().unwrap())).unwrap();
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
    assert_eq!(count("SELECT count(*) FROM environments"), 2);
    assert_eq!(count("SELECT count(*) FROM offerings"), 6);
    assert_eq!(
        count("SELECT count(*) FROM offerings WHERE is_default = 1"),
        2
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
    assert_eq!(count("SELECT count(*) FROM environments"), 2);
    assert_eq!(count("SELECT count(*) FROM env_capabilities"), 15);

    let _ = std::fs::remove_file(&path);
}
