//! Integration tests for the M0 holder: drive it through its library API
//! (fast, no spawned `disponent` process) and assert byte-exact round-trip,
//! scrollback replay, multi-reader fan-out, and resize.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use disponent_hold::protocol::ServerFrame;
use disponent_hold::{Client, Config, Exit, Holder};

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

    let mut c = Client::connect_uid(Some(&dir), "rt").unwrap();
    let seen = c.read_until(b"hello").unwrap();
    assert!(
        String::from_utf8_lossy(&seen).contains("hello"),
        "expected greeting, got {:?}",
        String::from_utf8_lossy(&seen)
    );

    c.send_input(b"world\n").unwrap();
    let seen = c.read_until(b"got:world").unwrap();
    assert!(String::from_utf8_lossy(&seen).contains("got:world"));

    let exit = c.read_to_exit().unwrap();
    assert_eq!(exit, Exit::Code(3), "child exit code must propagate");
    assert_eq!(exit.process_code(), 3);

    // The holder also records it for late callers / the engine.
    assert_eq!(holder.wait_for_exit(), Exit::Code(3));
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

    let mut c = Client::connect_uid(Some(&dir), "resize").unwrap();
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
