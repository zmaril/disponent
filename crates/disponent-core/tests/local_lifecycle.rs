//! The local backend against a REAL tmux (skipped when tmux is absent): a
//! sandboxed socket + root and a stand-in agent, so the whole lifecycle runs
//! without a network or a model. Covers provision (work dir, brief, runner,
//! tmux session), send, cancel-keeps-the-dir, reap-removes-it, and a fresh
//! engine adopting a survivor via survey.

use std::sync::Arc;
use std::time::{Duration, Instant};

use disponent_core::backend::EnvBackend;
use disponent_core::local::LocalTmux;
use disponent_core::mcp_generated::{DispatchSpec, DisponentMcp, Session};
use disponent_core::Engine;

mod common;

fn have_tmux() -> bool {
    std::process::Command::new("tmux")
        .arg("-V")
        .output()
        .is_ok()
}

fn sandbox(tag: &str) -> (String, std::path::PathBuf) {
    let socket = format!("dsp-test-{}-{tag}", std::process::id());
    let root = std::env::temp_dir().join(format!("disponent-local-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    (socket, root)
}

/// The stand-in agent: proves the brief arrived, then idles like a real one.
const FAKE_AGENT: &str = "echo AGENT_STARTED; cat ../brief.md; sleep 600; echo";

fn spec(brief: &str) -> DispatchSpec {
    serde_json::from_value(serde_json::json!({"brief": brief, "env": "local"})).unwrap()
}

fn wait_for(engine: &Engine, uid: &str, state: &str) -> Session {
    common::wait_for(engine, uid, state, Duration::from_secs(10))
}

#[test]
fn local_lifecycle_on_real_tmux() {
    if !have_tmux() {
        eprintln!("tmux not installed; skipping");
        return;
    }
    let (socket, root) = sandbox("life");
    let backend = LocalTmux::sandboxed(&socket, root.clone(), FAKE_AGENT);
    let engine = Engine::with_backend(backend);

    // no template demanded locally
    let session = engine.dispatch(spec("local brief: say hi")).unwrap();
    let running = wait_for(&engine, &session.uid, "running");
    let handle = running.env_handle.clone().unwrap();
    assert_eq!(handle["tmux"], format!("dsp-{}", session.uid));
    assert!(running.url.is_none(), "no ttyd locally (yet)");

    // the work dir materialized: brief written, runner ran the fake agent
    let work = root.join(&session.uid);
    assert!(work.join("brief.md").exists());
    assert!(work.join("task").is_dir());
    let probe = LocalTmux::sandboxed(&socket, root.clone(), FAKE_AGENT);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let pane = probe.capture(&handle).unwrap();
        if pane.contains("AGENT_STARTED") && pane.contains("local brief: say hi") {
            break;
        }
        assert!(Instant::now() < deadline, "agent never started: {pane}");
        std::thread::sleep(Duration::from_millis(50));
    }

    // send types into the session
    engine
        .send(session.uid.clone(), "echo FOLLOWUP".into())
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if probe.capture(&handle).unwrap().contains("echo FOLLOWUP") {
            break;
        }
        assert!(Instant::now() < deadline, "send never landed");
        std::thread::sleep(Duration::from_millis(50));
    }

    // cancel kills the tmux session but keeps the dir for inspection
    engine.cancel(session.uid.clone()).unwrap();
    assert!(probe.survey().unwrap().is_empty(), "tmux session stopped");
    assert!(work.exists(), "work dir kept after cancel");

    // reap removes the dir too
    engine.reap(session.uid.clone()).unwrap();
    assert!(!work.exists(), "work dir removed by reap");
}

#[test]
fn fresh_engine_adopts_a_local_survivor() {
    if !have_tmux() {
        eprintln!("tmux not installed; skipping");
        return;
    }
    let (socket, root) = sandbox("adopt");
    let engine = Engine::with_backend(LocalTmux::sandboxed(&socket, root.clone(), FAKE_AGENT));
    let session = engine.dispatch(spec("survive me")).unwrap();
    wait_for(&engine, &session.uid, "running");
    drop(engine); // the first disponent dies; tmux keeps running

    let second = Engine::with_backend(LocalTmux::sandboxed(&socket, root.clone(), FAKE_AGENT));
    let report = second.reconcile().unwrap();
    assert_eq!(report.adopted, 1, "the survivor came home");
    let adopted = second.session(session.uid.clone()).unwrap().unwrap();
    assert_eq!(adopted.state, "running");
    assert_eq!(
        adopted.env_handle.as_ref().unwrap()["tmux"],
        format!("dsp-{}", session.uid)
    );

    // and a second reconcile confirms rather than re-adopts
    let again = second.reconcile().unwrap();
    assert_eq!((again.adopted, again.confirmed), (0, 1));

    second.reap(session.uid).unwrap();
    let probe: Arc<dyn EnvBackend> =
        Arc::new(LocalTmux::sandboxed(&socket, root.clone(), FAKE_AGENT));
    assert!(probe.survey().unwrap().is_empty());
    let _ = std::fs::remove_dir_all(&root);
}
