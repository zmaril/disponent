//! The local backend against a REAL tmux (skipped when tmux is absent): a
//! sandboxed socket + root and a stand-in agent, so the whole lifecycle runs
//! without a network or a model. Covers provision (work dir, brief, runner,
//! tmux session), send, cancel-keeps-the-dir, reap-removes-it, and a fresh
//! engine adopting a survivor via survey.

use std::sync::Arc;
use std::time::{Duration, Instant};

use disponent_core::backend::EnvProvider;
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

fn have_git() -> bool {
    std::process::Command::new("git")
        .arg("--version")
        .output()
        .is_ok()
}

/// A throwaway git repo with one commit, so `git worktree add -b …` has a HEAD.
fn seed_repo(dir: &std::path::Path) {
    std::fs::create_dir_all(dir).unwrap();
    let git = |args: &[&str]| {
        let ok = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .unwrap()
            .status
            .success();
        assert!(ok, "git {args:?} failed");
    };
    git(&["init", "-q"]);
    git(&["config", "user.email", "t@t.test"]);
    git(&["config", "user.name", "t"]);
    std::fs::write(dir.join("README.md"), "hi\n").unwrap();
    git(&["add", "-A"]);
    git(&["commit", "-qm", "seed"]);
}

/// A freshly seeded throwaway source repo to add worktrees off of.
fn seeded_src(tag: &str) -> std::path::PathBuf {
    let src = std::env::temp_dir().join(format!("disponent-{tag}-src-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&src);
    seed_repo(&src);
    src
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

    // The transport-neutral attach descriptor: a local tmux worker is reachable
    // as (socket, dsp-<uid>) under the `tmux` transport, with no web fallback.
    assert_eq!(running.attach_transport.as_deref(), Some("tmux"));
    assert_eq!(running.attach_endpoint.as_deref(), Some(socket.as_str()));
    assert_eq!(running.attach_target, Some(format!("dsp-{}", session.uid)));
    assert!(running.attach_url.is_none());

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

    // send to a concrete live worker (a `sessions` target) still lands on its
    // prompt via the interact backend delivery — the legacy send behavior.
    engine
        .send(
            "echo FOLLOWUP".into(),
            Some(serde_json::from_value(serde_json::json!({"sessions": [session.uid]})).unwrap()),
            None,
            None,
        )
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
fn worktree_isolation_adds_and_removes_a_worktree() {
    if !have_tmux() || !have_git() {
        eprintln!("tmux/git not installed; skipping");
        return;
    }
    let (socket, root) = sandbox("wt");
    let src = seeded_src("wt");

    let engine = Engine::with_backend(LocalTmux::sandboxed(&socket, root.clone(), FAKE_AGENT));
    let spec: DispatchSpec = serde_json::from_value(serde_json::json!({
        "brief": "worktree me",
        "env": "local",
        "repo": src.display().to_string(),
        "isolation": "worktree",
        "gitRef": "feature/x",
    }))
    .unwrap();

    let session = engine.dispatch(spec).unwrap();
    let running = wait_for(&engine, &session.uid, "running");
    let handle = running.env_handle.clone().unwrap();
    // The handle records the parent repo so reap can deregister the worktree.
    assert_eq!(
        handle["worktreeRepo"],
        src.canonicalize().unwrap().display().to_string()
    );

    // The task dir is a git worktree, not a clone: `.git` is a file (a gitdir
    // pointer), and the parent repo lists it + the requested branch.
    let work = root.join(&session.uid);
    let task = work.join("task");
    assert!(
        task.join(".git").is_file(),
        "worktree .git is a pointer file"
    );
    let worktree_list = || {
        let out = std::process::Command::new("git")
            .args(["-C", &src.display().to_string(), "worktree", "list"])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).into_owned()
    };
    let list = worktree_list();
    assert!(
        list.contains(&task.display().to_string()),
        "parent lists the worktree:\n{list}"
    );
    assert!(
        list.contains("feature/x"),
        "worktree checked out the requested ref:\n{list}"
    );

    // Cancel keeps everything (same as the clone case).
    engine.cancel(session.uid.clone()).unwrap();
    assert!(task.join(".git").is_file(), "worktree kept after cancel");

    // Reap deregisters the worktree in the parent AND removes the dir.
    engine.reap(session.uid.clone()).unwrap();
    assert!(!work.exists(), "work dir removed by reap");
    let list = worktree_list();
    assert!(
        !list.contains(&task.display().to_string()),
        "worktree deregistered from parent (no dangling registration):\n{list}"
    );
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&src);
}

/// The delivery verdict event a reaped session recorded, if any.
fn delivery_event(engine: &Engine, uid: &str) -> Option<disponent_core::mcp_generated::Event> {
    engine
        .events(
            Some(serde_json::from_value(serde_json::json!({"sessionUid": uid})).unwrap()),
            None,
            None,
        )
        .unwrap()
        .into_iter()
        .find(|e| e.kind == "raw" && e.payload["payload"]["source"] == "delivery")
}

fn worktree_session(engine: &Engine, src: &std::path::Path) -> Session {
    let spec: DispatchSpec = serde_json::from_value(serde_json::json!({
        "brief": "worktree me",
        "env": "local",
        "repo": src.display().to_string(),
        "isolation": "worktree",
    }))
    .unwrap();
    let session = engine.dispatch(spec).unwrap();
    wait_for(engine, &session.uid, "running");
    session
}

#[test]
fn reap_flags_an_empty_worktree_session() {
    if !have_tmux() || !have_git() {
        eprintln!("tmux/git not installed; skipping");
        return;
    }
    let (socket, root) = sandbox("empty");
    let src = seeded_src("empty");

    let engine = Engine::with_backend(LocalTmux::sandboxed(&socket, root.clone(), FAKE_AGENT));
    let session = worktree_session(&engine, &src);

    // The fake agent never touches the tree → nothing shipped. Reap assesses the
    // worktree while it's still live, before teardown removes it.
    engine.reap(session.uid.clone()).unwrap();

    let ev = delivery_event(&engine, &session.uid).expect("a delivery verdict event");
    assert_eq!(ev.fidelity, "derived");
    assert_eq!(ev.payload["payload"]["data"]["verdict"], "empty");
    assert_eq!(ev.payload["payload"]["data"]["worktree_changed"], false);
    assert_eq!(ev.payload["payload"]["data"]["artifacts"], 0);

    let reaped = engine.session(session.uid).unwrap().unwrap();
    assert!(
        reaped
            .exit_detail
            .as_deref()
            .unwrap_or_default()
            .contains("empty diff"),
        "exit_detail reflects the empty verdict: {:?}",
        reaped.exit_detail
    );

    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&src);
}

#[test]
fn reap_flags_a_shipped_worktree_session() {
    if !have_tmux() || !have_git() {
        eprintln!("tmux/git not installed; skipping");
        return;
    }
    let (socket, root) = sandbox("shipped");
    let src = seeded_src("shipped");

    let engine = Engine::with_backend(LocalTmux::sandboxed(&socket, root.clone(), FAKE_AGENT));
    let session = worktree_session(&engine, &src);

    // Stand in for the agent's output: an uncommitted change in the worktree.
    let task = root.join(&session.uid).join("task");
    std::fs::write(task.join("NEW.md"), "the agent's work\n").unwrap();

    engine.reap(session.uid.clone()).unwrap();

    let ev = delivery_event(&engine, &session.uid).expect("a delivery verdict event");
    assert_eq!(ev.fidelity, "derived");
    assert_eq!(ev.payload["payload"]["data"]["verdict"], "shipped");
    assert_eq!(ev.payload["payload"]["data"]["worktree_changed"], true);

    let reaped = engine.session(session.uid).unwrap().unwrap();
    assert!(
        reaped
            .exit_detail
            .as_deref()
            .unwrap_or_default()
            .contains("shipped"),
        "exit_detail reflects the shipped verdict: {:?}",
        reaped.exit_detail
    );

    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&src);
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
    let probe: Arc<dyn EnvProvider> =
        Arc::new(LocalTmux::sandboxed(&socket, root.clone(), FAKE_AGENT));
    assert!(probe.survey().unwrap().is_empty());
    let _ = std::fs::remove_dir_all(&root);
}
