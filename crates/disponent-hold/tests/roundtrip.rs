//! Integration tests for the M0 holder: drive it through its library API
//! (fast, no spawned `disponent` process) and assert byte-exact round-trip,
//! scrollback replay, multi-reader fan-out, and resize.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use disponent_hold::protocol::ServerFrame;
use disponent_hold::{Client, Config, Exit, Holder, Role};

static COUNTER: AtomicU32 = AtomicU32::new(0);

/// A unique scratch socket dir per test (isolated, never a real path).
fn scratch_dir() -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "dsp-hold-test-{}-{}-{}",
        std::process::id(),
        n,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn sh(uid: &str, script: &str, dir: &Path) -> Config {
    Config {
        uid: uid.to_string(),
        argv: vec!["/bin/sh".to_string(), "-c".to_string(), script.to_string()],
        cwd: None,
        env: {
            let mut e = BTreeMap::new();
            e.insert(
                "PATH".to_string(),
                "/usr/bin:/bin:/usr/sbin:/sbin".to_string(),
            );
            e.insert("TERM".to_string(), "xterm-256color".to_string());
            e
        },
        socket_dir: Some(dir.to_path_buf()),
        ring_bytes: 256 * 1024,
        size: Default::default(),
    }
}

#[test]
fn round_trip_input_output_and_exit_code() {
    let dir = scratch_dir();
    let holder = Holder::start(sh(
        "rt",
        r#"echo hello; read x; echo "got:$x"; exit 3"#,
        &dir,
    ))
    .unwrap();

    // Run the whole client interaction on a worker thread and wait on it with a
    // generous deadline, so a genuine hang surfaces as a clean failure instead
    // of blocking the suite forever.
    let (tx, rx) = std::sync::mpsc::channel();
    let dir2 = dir.clone();
    let worker = std::thread::spawn(move || {
        let _ = tx.send(client_round_trip(&dir2));
    });
    let observed = match rx.recv_timeout(Duration::from_secs(15)) {
        Ok(r) => r.expect("client round-trip"),
        Err(_) => panic!("round-trip did not complete within the deadline (pty-timing hang?)"),
    };
    let _ = worker.join();

    assert_eq!(observed, Exit::Code(3), "child exit code must propagate");
    assert_eq!(observed.process_code(), 3);

    // The holder also records it for late callers / the engine.
    assert_eq!(holder.wait_for_exit(), Exit::Code(3));
}

/// Drive one client through the full input/output round-trip and return the
/// child's exit as observed on the stream.
///
/// The holder does **not** guarantee the last `Data` chunk is flushed before
/// the `Exit` frame: the reader thread (pty → `Data`) and the watcher thread
/// (`waitpid` → `Exit`) race to enqueue onto each client's queue, and the child
/// runs `echo "got:$x"; exit 3` back-to-back. So rather than assume `got:world`
/// strictly precedes the exit (the old `read_until` did, and bailed on the
/// `Exit` frame under load), drain frames accumulating `Data` and capturing the
/// `Exit`, and stop only once BOTH have been seen — tolerating either order.
fn client_round_trip(dir: &Path) -> anyhow::Result<Exit> {
    // This client sends input, so it must hold the writer lock (M2a).
    let mut c = Client::connect_uid_as(Some(dir), "rt", Role::Writer)?;

    // "hello" is printed before the child blocks on `read`, so it cannot race
    // the exit — a plain read-until is safe here.
    let seen = c.read_until(b"hello")?;
    anyhow::ensure!(
        String::from_utf8_lossy(&seen).contains("hello"),
        "expected greeting, got {:?}",
        String::from_utf8_lossy(&seen)
    );

    c.send_input(b"world\n")?;

    let mut acc = Vec::new();
    let mut exit = None;
    loop {
        match c.read_frame()? {
            Some(ServerFrame::Data(d)) => acc.extend_from_slice(&d),
            Some(ServerFrame::Heartbeat) => {}
            Some(ServerFrame::Exit(e)) => exit = Some(e),
            None => break,
        }
        if contains(&acc, b"got:world") && exit.is_some() {
            break;
        }
    }
    anyhow::ensure!(
        contains(&acc, b"got:world"),
        "expected the echoed input, got {:?}",
        String::from_utf8_lossy(&acc)
    );
    exit.ok_or_else(|| anyhow::anyhow!("stream closed before an Exit frame"))
}

/// Substring test over byte slices (the client's own `contains` is private).
fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    needle.is_empty()
        || (needle.len() <= haystack.len() && haystack.windows(needle.len()).any(|w| w == needle))
}

#[test]
fn ring_replays_output_produced_before_connect() {
    let dir = scratch_dir();
    // Emit a marker, then linger so the holder is still up when we attach late.
    let holder = Holder::start(sh(
        "replay",
        r#"echo replay-marker-42; sleep 2; exit 0"#,
        &dir,
    ))
    .unwrap();

    // Let the reader thread drain the marker into the ring BEFORE we connect.
    std::thread::sleep(Duration::from_millis(400));

    let mut c = Client::connect_uid(Some(&dir), "replay").unwrap();
    let replayed = c.read_until(b"replay-marker-42").unwrap();
    assert!(
        String::from_utf8_lossy(&replayed).contains("replay-marker-42"),
        "the ring must replay bytes produced before this client attached"
    );
    drop(holder);
}

#[test]
fn two_concurrent_readers_both_get_live_data() {
    let dir = scratch_dir();
    let holder = Holder::start(sh(
        "multi",
        r#"sleep 0.5; echo shared-broadcast-line; sleep 2; exit 0"#,
        &dir,
    ))
    .unwrap();

    // Both attach before the line is printed, so it arrives live to each.
    let mut a = Client::connect_uid(Some(&dir), "multi").unwrap();
    let mut b = Client::connect_uid(Some(&dir), "multi").unwrap();

    let ga = a.read_until(b"shared-broadcast-line").unwrap();
    let gb = b.read_until(b"shared-broadcast-line").unwrap();
    assert!(String::from_utf8_lossy(&ga).contains("shared-broadcast-line"));
    assert!(String::from_utf8_lossy(&gb).contains("shared-broadcast-line"));
    drop(holder);
}

#[test]
fn resize_reaches_the_child() {
    let dir = scratch_dir();
    // The child prints its controlling-tty size after we've had time to resize.
    let holder = Holder::start(sh(
        "resize",
        r#"sleep 0.6; stty size; sleep 0.3; exit 0"#,
        &dir,
    ))
    .unwrap();

    // Resizing is a writer act (M2a), so hold the writer lock.
    let mut c = Client::connect_uid_as(Some(&dir), "resize", Role::Writer).unwrap();
    // Resize before the child reads its size. The ioctl path must not error.
    c.send_resize(90, 30).expect("resize ioctl must succeed");

    // Collect all output through exit and check the child observed 30 rows x 90 cols.
    let mut acc = Vec::new();
    loop {
        match c.read_frame().unwrap() {
            Some(ServerFrame::Data(d)) => acc.extend_from_slice(&d),
            Some(ServerFrame::Heartbeat) => {}
            Some(ServerFrame::Exit(_)) => break,
            None => break,
        }
    }
    let out = String::from_utf8_lossy(&acc);
    assert!(
        out.contains("30 90"),
        "child's stty size should reflect the resize, got: {out:?}"
    );
    drop(holder);
}

// ---------------------------------------------------------------------------
// M2a: the single-writer / multi-reader lock (design §6).
// ---------------------------------------------------------------------------

/// Retry a writer connection until it is granted or the deadline passes — the
/// holder frees the lock asynchronously when the previous writer's socket EOFs,
/// so a fresh acquire may need a beat.
fn connect_writer_when_free(dir: &Path, uid: &str, secs: u64) -> Client {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if let Ok(c) = Client::connect_writer(&disponent_hold::socket_path(Some(dir), uid)) {
            return c;
        }
        if std::time::Instant::now() >= deadline {
            panic!("writer lock never freed within {secs}s");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn first_writer_is_granted_and_a_second_is_denied_read_only() {
    let dir = scratch_dir();
    let holder = Holder::start(sh("wlock", r#"sleep 3; exit 0"#, &dir)).unwrap();

    // The first Writer request wins the lock.
    let w1 = Client::connect_uid_as(Some(&dir), "wlock", Role::Writer).unwrap();
    assert_eq!(w1.granted_role(), Role::Writer, "first writer is granted");
    assert!(!w1.writer_busy());

    // A second, concurrent Writer request is admitted as a read-only Reader.
    let w2 = Client::connect_uid_as(Some(&dir), "wlock", Role::Writer).unwrap();
    assert_eq!(
        w2.granted_role(),
        Role::Reader,
        "a second writer is denied and admitted read-only"
    );
    assert!(w2.writer_busy(), "the denial is reported as writer_busy");

    // A plain reader is of course still a reader.
    let r = Client::connect_uid(Some(&dir), "wlock").unwrap();
    assert_eq!(r.granted_role(), Role::Reader);
    assert!(!r.writer_busy());

    drop(w1);
    drop(holder);
}

#[test]
fn only_the_writers_input_reaches_the_child() {
    let dir = scratch_dir();
    // The child echoes the single line it reads from stdin.
    let holder = Holder::start(sh(
        "wonly",
        r#"read x; echo "READ:[$x]"; sleep 2; exit 0"#,
        &dir,
    ))
    .unwrap();

    // An observer reader watches the output.
    let mut obs = Client::connect_uid(Some(&dir), "wonly").unwrap();

    // A read-only client's input must be ignored by the holder.
    let mut reader = Client::connect_uid(Some(&dir), "wonly").unwrap();
    assert_eq!(reader.granted_role(), Role::Reader);
    reader.send_input(b"from-reader\n").unwrap();

    // Give the holder a beat to (not) process the reader's ignored input.
    std::thread::sleep(Duration::from_millis(200));

    // The writer's input is what reaches the child's `read x`.
    let mut writer = Client::connect_uid_as(Some(&dir), "wonly", Role::Writer).unwrap();
    assert_eq!(writer.granted_role(), Role::Writer);
    writer.send_input(b"from-writer\n").unwrap();

    let seen = obs.read_until(b"READ:[").unwrap();
    // Drain a little more so the whole echoed line is captured.
    let mut acc = seen;
    let more = obs.read_until(b"]").unwrap_or_default();
    acc.extend_from_slice(&more);
    let out = String::from_utf8_lossy(&acc);
    assert!(
        out.contains("from-writer"),
        "the writer's input must reach the child, got {out:?}"
    );
    assert!(
        !out.contains("from-reader"),
        "a reader's input must NOT reach the child, got {out:?}"
    );
    drop(holder);
}

#[test]
fn writer_lock_frees_when_the_writer_disconnects() {
    let dir = scratch_dir();
    let holder = Holder::start(sh(
        "wfree",
        r#"read x; echo "READ:[$x]"; sleep 2; exit 0"#,
        &dir,
    ))
    .unwrap();

    // First writer takes the lock, then leaves.
    let w1 = Client::connect_uid_as(Some(&dir), "wfree", Role::Writer).unwrap();
    assert_eq!(w1.granted_role(), Role::Writer);
    // While w1 holds it, a fresh writer request is denied.
    let denied = Client::connect_uid_as(Some(&dir), "wfree", Role::Writer).unwrap();
    assert_eq!(denied.granted_role(), Role::Reader);
    drop(denied);
    drop(w1);

    // After w1 disconnects the lock frees, so a new writer can acquire it and
    // its input reaches the child.
    let mut w2 = connect_writer_when_free(&dir, "wfree", 5);
    let mut obs = Client::connect_uid(Some(&dir), "wfree").unwrap();
    w2.send_input(b"second-writer\n").unwrap();
    let seen = obs.read_until(b"second-writer").unwrap();
    assert!(String::from_utf8_lossy(&seen).contains("second-writer"));
    drop(holder);
}

#[test]
fn connect_writer_errors_when_the_lock_is_held() {
    let dir = scratch_dir();
    let holder = Holder::start(sh("werr", r#"sleep 3; exit 0"#, &dir)).unwrap();

    // Hold the writer with a long-lived interactive-style attacher.
    let held = Client::connect_uid_as(Some(&dir), "werr", Role::Writer).unwrap();
    assert_eq!(held.granted_role(), Role::Writer);

    // The engine's `send` path (connect_writer) must fail honestly, not drop.
    let msg = match Client::connect_writer(&disponent_hold::socket_path(Some(&dir), "werr")) {
        Ok(_) => panic!("a second writer connection must be denied, not granted"),
        Err(e) => e.to_string(),
    };
    assert!(
        msg.contains("writer channel held"),
        "expected an honest writer-held error, got {msg:?}"
    );

    drop(held);
    // Once freed, connect_writer succeeds.
    let ok = connect_writer_when_free(&dir, "werr", 5);
    assert_eq!(ok.granted_role(), Role::Writer);
    drop(holder);
}

#[test]
fn a_reader_still_gets_live_data_while_a_writer_drives() {
    let dir = scratch_dir();
    let holder = Holder::start(sh(
        "rw",
        r#"sleep 0.4; echo shared-line-xyz; sleep 2; exit 0"#,
        &dir,
    ))
    .unwrap();

    // A writer and a reader both attach before the line is printed.
    let _w = Client::connect_uid_as(Some(&dir), "rw", Role::Writer).unwrap();
    let mut r = Client::connect_uid(Some(&dir), "rw").unwrap();
    let seen = r.read_until(b"shared-line-xyz").unwrap();
    assert!(
        String::from_utf8_lossy(&seen).contains("shared-line-xyz"),
        "a reader must keep receiving live Data alongside a writer"
    );
    drop(holder);
}
