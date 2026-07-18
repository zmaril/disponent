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

// ── The exact-terminal watcher (M1 payoff) ───────────────────────────────────
//
// A holder-backed session's watcher consumes the live byte-exact stream (fidelity
// `exact`) instead of polling `capture` (fidelity `scraped`), and the child's real
// exit self-transitions the session completed/failed — an observation, not a reap.
// These drive a real `disponent_hold::Holder` over `/bin/sh` through the actual
// engine watcher + collector, plus an `observe_stream: None` double as the
// scraped-path regression guard.
//
// straitjacket-allow-file:duplication — `start_holder` here is the same short
// holder scaffold local.rs's tests use; per that module's note an intra-crate
// test scaffold isn't worth a shared test-util, so the parallel copy is
// intentional (mirrors the modal/exe.dev allow-file precedent in backend.rs).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use disponent_hold::{Config, Holder};

use crate::agent::{AgentAdapter, ClaudeCode};
use crate::backend::{Compute, EnvProvider, Provision, StartRequest};
use crate::local::LocalTmux;

static WATCH_COUNTER: AtomicU32 = AtomicU32::new(0);

fn watch_scratch() -> PathBuf {
    let n = WATCH_COUNTER.fetch_add(1, AtomicOrdering::SeqCst);
    let dir = std::env::temp_dir().join(format!("dsp-watch-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Start a holder over `/bin/sh -c <script>`, socket bound synchronously in `dir`.
/// The scaffold intentionally mirrors local.rs's identical test helper — per that
/// module's note, an intra-crate test scaffold isn't worth a shared test-util.
fn start_holder(uid: &str, script: &str, dir: &Path) -> Holder {
    let mut env = BTreeMap::new();
    env.insert("PATH".into(), "/usr/bin:/bin:/usr/sbin:/sbin".into());
    env.insert("TERM".into(), "xterm-256color".into());
    Holder::start(Config {
        uid: uid.to_string(),
        argv: vec!["/bin/sh".into(), "-c".into(), script.into()],
        cwd: None,
        env,
        socket_dir: Some(dir.to_path_buf()),
        ring_bytes: 256 * 1024,
        size: Default::default(),
    })
    .unwrap()
}

/// Put a `running`, unreaped session on the ledger so the collector folds its
/// observations (mirrors what provisioning does, without a live backend).
fn seed_running(engine: &Engine, uid: &str) {
    let session: Session =
        serde_json::from_value(json!({"uid": uid, "dispatchId": "d", "state": "running"})).unwrap();
    engine.ledger.lock().unwrap().sessions.push(session);
}

/// Poll the ledger until `f` sees what it wants or the deadline passes.
fn wait_until<F: FnMut(&Ledger) -> bool>(engine: &Engine, secs: u64, mut f: F) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        if f(&engine.ledger.lock().unwrap()) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    false
}

/// Stand up a real holder over `<script>`, put a `running` session on a fresh
/// engine, and point the engine watcher at the holder-backed handle — the whole
/// exact-tier fixture the assertions below drive. Returns the engine (its
/// collector running) and the holder (kept alive until the test drops it).
fn holder_watch(uid: &str, script: &str) -> (Engine, Holder) {
    let dir = watch_scratch();
    let holder = start_holder(uid, script, &dir);
    let engine = Engine::new();
    seed_running(&engine, uid);
    let backend: Arc<dyn EnvProvider> = Arc::new(LocalTmux::sandboxed_holder(dir.clone(), "agent"));
    let adapter: Arc<dyn AgentAdapter> = Arc::new(ClaudeCode);
    let handle = json!({
        "holder": true,
        "holderSock": dir.join(format!("{uid}.sock")),
        "workDir": dir.join(uid),
    });
    watch_session(
        &engine.observers,
        Duration::from_millis(40),
        backend,
        adapter,
        uid,
        handle,
        LifecyclePolicy::default(),
    );
    (engine, holder)
}

#[test]
fn holder_backed_watch_lands_exact_frames_and_completes_on_a_zero_exit() {
    let uid = "wexact";
    let (engine, holder) = holder_watch(uid, r#"printf 'exact-frame\n'; sleep 0.2; exit 0"#);

    // The child's byte-exact output lands as an `exact` raw/terminal event —
    // never a `scraped` one (the whole point of the holder tier).
    let saw_exact = wait_until(&engine, 8, |l| {
        l.events.iter().any(|e| {
            e.session_uid == uid
                && e.kind == "raw"
                && e.fidelity == "exact"
                && e.payload["payload"]["data"]
                    .as_str()
                    .is_some_and(|d| d.contains("exact-frame"))
        })
    });
    assert!(
        saw_exact,
        "the watcher must land exact frames in the ledger"
    );
    assert!(
        !engine
            .ledger
            .lock()
            .unwrap()
            .events
            .iter()
            .any(|e| e.session_uid == uid && e.fidelity == "scraped"),
        "a holder-backed session must not be scraped-polled"
    );

    // The real exit (code 0) self-transitions the session to completed, records
    // an exact exit event, and does NOT reap — the record persists.
    let completed = wait_until(&engine, 8, |l| {
        l.sessions
            .iter()
            .any(|s| s.uid == uid && s.state == "completed")
    });
    assert!(completed, "a zero exit must complete the session");
    let l = engine.ledger.lock().unwrap();
    let s = l.sessions.iter().find(|s| s.uid == uid).unwrap();
    assert_eq!(s.state, "completed");
    assert!(s.ended_at.is_some(), "a terminal state stamps ended_at");
    assert!(
        s.reaped_at.is_none(),
        "observation only — nothing self-reaps"
    );
    assert_eq!(s.exit_detail.as_deref(), Some("exit code 0"));
    assert!(
        l.events
            .iter()
            .any(|e| e.session_uid == uid && e.kind == "exit" && e.fidelity == "exact"),
        "an exact exit event is recorded"
    );
    drop(l);
    drop(holder);
}

#[test]
fn holder_backed_watch_fails_the_session_on_a_nonzero_exit() {
    let uid = "wfail";
    let (engine, holder) = holder_watch(uid, r#"exit 3"#);

    let failed = wait_until(&engine, 8, |l| {
        l.sessions
            .iter()
            .any(|s| s.uid == uid && s.state == "failed")
    });
    assert!(failed, "a nonzero exit must fail the session");
    let l = engine.ledger.lock().unwrap();
    let s = l.sessions.iter().find(|s| s.uid == uid).unwrap();
    assert_eq!(s.state, "failed");
    assert_eq!(
        s.exit_detail.as_deref(),
        Some("exit code 3"),
        "the REAL exit code is recorded, not an inference"
    );
    assert!(s.reaped_at.is_none(), "failed is observed, not reaped");
    drop(l);
    drop(holder);
}

/// A backend whose Compute has no live stream (`observe_stream: None`) and a
/// canned pane — the scraped-path regression double (tmux/exe.dev shape).
struct ScrapedBackend {
    pane: String,
}

struct ScrapedCompute {
    pane: String,
}

impl EnvProvider for ScrapedBackend {
    fn kind(&self) -> &'static str {
        "scraped-test"
    }
    fn requires_template(&self) -> bool {
        false
    }
    fn start(&self, _req: &StartRequest) -> anyhow::Result<Provision> {
        unreachable!("watcher-only double")
    }
    fn compute(&self, _handle: &serde_json::Value) -> anyhow::Result<Box<dyn Compute>> {
        Ok(Box::new(ScrapedCompute {
            pane: self.pane.clone(),
        }))
    }
    fn reap(&self, _handle: &serde_json::Value) -> anyhow::Result<()> {
        Ok(())
    }
    fn survey(&self) -> anyhow::Result<Vec<(String, serde_json::Value)>> {
        Ok(vec![])
    }
}

impl Compute for ScrapedCompute {
    fn run(&self, _cmd: &str) -> anyhow::Result<String> {
        Ok(String::new())
    }
    fn spawn(&self, _cmd: &str) -> anyhow::Result<()> {
        Ok(())
    }
    fn send(&self, _input: &str) -> anyhow::Result<()> {
        Ok(())
    }
    fn capture(&self) -> anyhow::Result<String> {
        // A pane snapshot, the scraped shape — no `observe_stream` override, so
        // the default `None` keeps the watcher on the polling path.
        Ok(self.pane.clone())
    }
    fn interrupt(&self) -> anyhow::Result<()> {
        Ok(())
    }
    fn kill(&self) -> anyhow::Result<()> {
        Ok(())
    }
    fn workspace_link(&self) -> anyhow::Result<Option<String>> {
        Ok(None)
    }
}

#[test]
fn a_stream_less_surface_still_uses_scraped_polling() {
    // observe_stream == None → the watcher polls `capture` and emits `scraped`
    // events, never `exact`, and never self-transitions. The regression guard
    // that the tmux/exe.dev path is untouched by the holder wiring.
    let uid = "wscraped";
    let engine = Engine::new();
    seed_running(&engine, uid);
    let backend: Arc<dyn EnvProvider> = Arc::new(ScrapedBackend {
        pane: "pane line A\npane line B".into(),
    });
    let adapter: Arc<dyn AgentAdapter> = Arc::new(ClaudeCode);
    watch_session(
        &engine.observers,
        Duration::from_millis(40),
        backend,
        adapter,
        uid,
        json!({}),
        LifecyclePolicy::default(),
    );

    let saw_scraped = wait_until(&engine, 8, |l| {
        l.events
            .iter()
            .any(|e| e.session_uid == uid && e.kind == "raw" && e.fidelity == "scraped")
    });
    assert!(saw_scraped, "a stream-less surface must be scraped-polled");
    let l = engine.ledger.lock().unwrap();
    assert!(
        !l.events
            .iter()
            .any(|e| e.session_uid == uid && e.fidelity == "exact"),
        "no exact frames without a holder stream"
    );
    assert_eq!(
        l.sessions.iter().find(|s| s.uid == uid).unwrap().state,
        "running",
        "scraped polling never self-transitions on its own"
    );

    // The double honestly declines a live stream (the trait default), which is
    // exactly what routes the watcher onto the scraped path above.
    assert!(ScrapedCompute {
        pane: String::new(),
    }
    .observe_stream()
    .unwrap()
    .is_none());
}
