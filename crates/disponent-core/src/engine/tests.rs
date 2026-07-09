use super::*;
use crate::mcp_generated::DisponentMcp;

fn spec(env: &str) -> DispatchSpec {
    serde_json::from_value(json!({"brief": "do the thing", "env": env})).unwrap()
}

#[test]
fn dispatch_resolves_defaults_and_queues() {
    let engine = Engine::new();
    let session = engine.dispatch(spec("exe-dev")).unwrap();
    assert_eq!(session.state, "queued");
    assert!(session.reaped_at.is_none());
    let fetched = engine.session(session.uid.clone()).unwrap().unwrap();
    assert_eq!(fetched.dispatch_id, session.dispatch_id);
}

#[test]
fn dispatch_rejects_unknown_env_and_uncatalogued_combos() {
    let engine = Engine::new();
    assert!(engine.dispatch(spec("nonesuch")).is_err());

    let mut bad = spec("local");
    bad.agent = Some("codex".into());
    assert!(engine.dispatch(bad.clone()).is_err());
    bad.unchecked = Some(true);
    assert_eq!(engine.dispatch(bad).unwrap().state, "queued");
}

#[test]
fn cancel_then_reap_walks_the_lifecycle() {
    let engine = Engine::new();
    let s = engine.dispatch(spec("local")).unwrap();
    let cancelled = engine.cancel(s.uid.clone()).unwrap();
    assert_eq!(cancelled.state, "cancelled");
    assert!(cancelled.ended_at.is_some());
    assert!(engine.cancel(s.uid.clone()).is_err(), "already terminal");
    let reaped = engine.reap(s.uid.clone()).unwrap();
    assert!(reaped.reaped_at.is_some());
    assert!(engine.reap(s.uid).is_err(), "already reaped");
}

#[test]
fn reap_on_a_live_session_cancels_first() {
    let engine = Engine::new();
    let s = engine.dispatch(spec("local")).unwrap();
    let reaped = engine.reap(s.uid).unwrap();
    assert_eq!(reaped.state, "cancelled");
    assert!(reaped.reaped_at.is_some());
}

#[test]
fn the_ledger_guards_the_state_machine() {
    let engine = Engine::new();
    // exe-dev has no backend here, so the session parks at queued and we can
    // drive the ledger through transitions by hand without racing provisioning.
    let uid = engine.dispatch(spec("exe-dev")).unwrap().uid;
    let uid2 = engine.dispatch(spec("exe-dev")).unwrap().uid;
    let mut ledger = engine.ledger.lock().unwrap();

    // A legal walk all the way to a terminal state succeeds.
    ledger.transition(&uid, "provisioning").unwrap();
    ledger.transition(&uid, "running").unwrap();
    ledger.transition(&uid, "needs_input").unwrap();
    ledger.transition(&uid, "running").unwrap();
    let (done, _) = ledger.transition(&uid, "completed").unwrap();
    assert_eq!(done.state, "completed");
    assert!(done.ended_at.is_some());

    // A terminal state has no way forward — reviving it is rejected.
    let err = ledger.transition(&uid, "running").unwrap_err();
    assert!(
        err.to_string().contains("illegal transition"),
        "expected an illegal-transition error, got: {err}"
    );

    // And a fresh session can't skip states (queued straight to running).
    assert!(ledger.transition(&uid2, "running").is_err());
    // queued -> cancelled is a legal live-state exit, though.
    let (cancelled, _) = ledger.transition(&uid2, "cancelled").unwrap();
    assert_eq!(cancelled.state, "cancelled");
    assert!(ledger.transition(&uid2, "running").is_err());
}

#[test]
fn events_filter_and_cursor_page() {
    let engine = Engine::new();
    let a = engine.dispatch(spec("local")).unwrap();
    let b = engine.dispatch(spec("exe-dev")).unwrap();
    engine.cancel(b.uid.clone()).unwrap();

    // b has a log + a state event; a has just the log
    let only_b: EventOptions = serde_json::from_value(json!({"sessionUid": b.uid})).unwrap();
    let events = engine.events(Some(only_b.clone()), None, None).unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].kind, "state");
    assert_eq!(events[1].idx, 1, "the DTO carries its timeline position");
    assert_eq!(events[1].payload["payload"]["to"], "cancelled");

    // cursor: after=1 means "I've seen one", so only the state event remains
    let page = engine.events(Some(only_b), Some(1), Some(10)).unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].kind, "state");

    let a_events = engine
        .events(
            Some(serde_json::from_value(json!({"sessionUid": a.uid})).unwrap()),
            None,
            None,
        )
        .unwrap();
    assert_eq!(a_events.len(), 1);
    assert_eq!(a_events[0].kind, "log");
}

#[test]
fn sessions_filter_by_env_and_state() {
    let engine = Engine::new();
    engine.dispatch(spec("local")).unwrap();
    let b = engine.dispatch(spec("exe-dev")).unwrap();
    engine.cancel(b.uid).unwrap();

    let by_env = |env: &str| -> Vec<Session> {
        engine
            .sessions(Some(serde_json::from_value(json!({"env": env})).unwrap()))
            .unwrap()
    };
    assert_eq!(by_env("local").len(), 1);
    assert_eq!(by_env("exe-dev").len(), 1);
    assert_eq!(by_env("exe-dev")[0].state, "cancelled");

    let queued = engine
        .sessions(Some(
            serde_json::from_value(json!({"state": "queued"})).unwrap(),
        ))
        .unwrap();
    assert_eq!(queued.len(), 1);
}

#[test]
fn driver_plan_emits_ddl_then_ordered_rows_and_pages() {
    let engine = Engine::new();
    let s = engine.dispatch(spec("local")).unwrap();
    engine.cancel(s.uid).unwrap();

    let all = engine.driver_plan(None, None, Some(1000)).unwrap();
    let creates = all
        .iter()
        .filter(|s| s.sql.starts_with("CREATE TABLE"))
        .count();
    assert_eq!(creates, SQLITE_TABLES.len(), "DDL for every table");
    // rows follow dependency order: the dispatch's upsert before the session's
    let pos = |needle: &str| all.iter().position(|s| s.sql.contains(needle)).unwrap();
    assert!(pos("INSERT INTO \"dispatches\"") < pos("INSERT INTO \"sessions\""));
    assert!(pos("INSERT INTO \"sessions\"") < pos("INSERT INTO \"events\""));

    // the cursor pages the same sequence
    let (a, b) = (
        engine.driver_plan(None, None, Some(5)).unwrap(),
        engine.driver_plan(None, Some(5), Some(1000)).unwrap(),
    );
    assert_eq!(a.len(), 5);
    assert_eq!(a.last().unwrap().sql, all[4].sql);
    assert_eq!(b.first().unwrap().sql, all[5].sql);
    assert_eq!(a.len() + b.len(), all.len());

    // postgres flavor speaks $n placeholders
    let pg_opts: DriverPlanOptions =
        serde_json::from_value(json!({"dialect": "postgres"})).unwrap();
    let pg = engine.driver_plan(Some(pg_opts), None, Some(1000)).unwrap();
    assert!(pg.iter().any(|s| s.sql.contains("$1")));

    assert!(engine
        .driver_plan(
            Some(serde_json::from_value(json!({"dialect": "mongodb"})).unwrap()),
            None,
            None,
        )
        .is_err());
}
