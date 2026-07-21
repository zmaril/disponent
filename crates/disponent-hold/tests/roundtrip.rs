//! Integration tests for the holder: drive it through its library API (fast, no
//! spawned `disponent` process) and assert byte-exact round-trip, scrollback
//! replay, multi-reader fan-out, resize, the M2a writer lock, and the M3
//! opt-in vt100 screen-restore repaint (vs. the unchanged raw-ring replay).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use disponent_hold::protocol::ServerFrame;
use disponent_hold::{Client, Config, Exit, Holder, Restore, Role};

static COUNTER: AtomicU32 = AtomicU32::new(0);

/// A unique scratch socket dir per test (isolated, never a real path). Kept
/// short: the socket path underneath is `<dir>/<uid>.sock`, and macOS caps a
/// unix socket path at `SUN_LEN` (104) — well under Linux's 108. pid keeps it
/// distinct across test binaries, the atomic counter across calls within one,
/// so no timestamp is needed (which would blow the budget on macOS's long
/// `/var/folders/...` TMPDIR).
fn scratch_dir() -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("dsp-hold-{}-{}", std::process::id(), n));
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
/// The holder now guarantees the child's final `Data` is flushed to each client
/// *before* the `Exit` frame: the reader drains the pty to EOF and only then
/// emits `Exit` (the watcher merely records the reaped status). So even though
/// the child runs `echo "got:$x"; exit 3` back-to-back, `got:world` must already
/// be in hand the moment `Exit` arrives — assert exactly that, and that nothing
/// follows the `Exit`.
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
        match c.read_frame() {
            Ok(Some(ServerFrame::Data(d))) => {
                // Any Data after the Exit frame would break the ordering guarantee.
                anyhow::ensure!(
                    exit.is_none(),
                    "a Data frame arrived AFTER Exit — ordering guarantee violated"
                );
                acc.extend_from_slice(&d);
            }
            Ok(Some(ServerFrame::Heartbeat)) => {}
            Ok(Some(ServerFrame::Exit(e))) => {
                // The final output must already be in hand when Exit arrives.
                anyhow::ensure!(
                    contains(&acc, b"got:world"),
                    "Exit arrived before the child's final output — got {:?}",
                    String::from_utf8_lossy(&acc)
                );
                exit = Some(e);
                // Give any (erroneous) straggler Data a chance to surface.
                c.set_read_timeout(Some(Duration::from_millis(300)))?;
            }
            Ok(None) => break,
            // Post-Exit read timeout: we've waited long enough to be sure no
            // straggler Data follows. Before Exit, a read error is a real fault.
            Err(_) if exit.is_some() => break,
            Err(e) => return Err(e),
        }
    }
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

// ---------------------------------------------------------------------------
// Ordering guarantee: the child's final `Data` must always reach a client
// before the `Exit` frame. The reader drains the pty to EOF and only then
// emits `Exit` (the watcher merely records the reaped status), so `Exit` is
// strictly the last frame per client. The previous reader/watcher race
// reproduced at ~20%; these tests must be clean across many iterations.
// ---------------------------------------------------------------------------

/// Collect every frame from a fresh client until the stream is spent. Returns
/// the accumulated `Data` bytes, whether any `Data` arrived *after* `Exit`
/// (an ordering violation), and the observed exit. A blocking read is used up
/// to `Exit`; a short timeout afterwards lets any erroneous straggler surface.
fn collect_frames(dir: &Path, uid: &str) -> (Vec<u8>, bool, Option<Exit>) {
    let mut c = Client::connect_uid(Some(dir), uid).unwrap();
    let mut acc = Vec::new();
    let mut exit = None;
    let mut data_after_exit = false;
    loop {
        match c.read_frame() {
            Ok(Some(ServerFrame::Data(d))) => {
                if exit.is_some() {
                    data_after_exit = true;
                }
                acc.extend_from_slice(&d);
            }
            Ok(Some(ServerFrame::Heartbeat)) => {}
            Ok(Some(ServerFrame::Exit(e))) => {
                exit = Some(e);
                c.set_read_timeout(Some(Duration::from_millis(300)))
                    .unwrap();
            }
            Ok(None) => break,
            // After Exit, a read timeout means no straggler Data is coming.
            Err(_) if exit.is_some() => break,
            Err(e) => panic!("read error before exit: {e}"),
        }
    }
    (acc, data_after_exit, exit)
}

#[test]
fn final_data_precedes_exit_under_load() {
    // A child that writes a burst then exits immediately — the exact shape that
    // let the watcher's Exit race ahead of the reader's last Data.
    const ITERS: usize = 40;
    let script = r#"head -c 6000 /dev/zero | tr '\0' A; echo END; exit 7"#;

    // Background CPU load to widen the scheduling window the old race needed.
    let stop = std::sync::Arc::new(AtomicU32::new(0));
    let mut burners = Vec::new();
    for _ in 0..4 {
        let stop = std::sync::Arc::clone(&stop);
        burners.push(std::thread::spawn(move || {
            let mut x: u64 = 0;
            while stop.load(Ordering::Relaxed) == 0 {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
                std::hint::black_box(x);
            }
        }));
    }

    let mut failures = Vec::new();
    for i in 0..ITERS {
        let dir = scratch_dir();
        let uid = format!("ord{i}");
        let holder = Holder::start(sh(&uid, script, &dir)).unwrap();

        // Run the client on a worker so a hang surfaces as a failure, not a
        // suite-wide block.
        let (tx, rx) = std::sync::mpsc::channel();
        let dir2 = dir.clone();
        let uid2 = uid.clone();
        std::thread::spawn(move || {
            let _ = tx.send(collect_frames(&dir2, &uid2));
        });
        let (acc, data_after_exit, exit) = match rx.recv_timeout(Duration::from_secs(15)) {
            Ok(v) => v,
            Err(_) => {
                failures.push(format!("iter {i}: client did not complete (hang?)"));
                drop(holder);
                continue;
            }
        };

        if data_after_exit {
            failures.push(format!("iter {i}: a Data frame arrived AFTER Exit"));
        }
        if !contains(&acc, b"AAAAAEND") {
            failures.push(format!(
                "iter {i}: burst/END missing before Exit (len {})",
                acc.len()
            ));
        }
        match exit {
            Some(Exit::Code(7)) => {}
            other => failures.push(format!("iter {i}: expected exit code 7, got {other:?}")),
        }
        drop(holder);
    }

    stop.store(1, Ordering::Relaxed);
    for b in burners {
        let _ = b.join();
    }

    assert!(
        failures.is_empty(),
        "ordering guarantee violated in {}/{} iterations:\n{}",
        failures.len(),
        ITERS,
        failures.join("\n")
    );
}

#[test]
fn large_final_output_fully_precedes_exit() {
    // A final burst larger than one 16 KiB Data frame (MAX_PAYLOAD) — it spans
    // several frames, all of which must arrive before Exit.
    let dir = scratch_dir();
    let n = 40_000usize; // > 2 * 16 KiB
    let script = format!(r#"head -c {n} /dev/zero | tr '\0' Z; echo TAIL-MARK; exit 5"#);
    let holder = Holder::start(sh("big", &script, &dir)).unwrap();

    let (acc, data_after_exit, exit) = collect_frames(&dir, "big");

    assert!(
        !data_after_exit,
        "no Data frame may follow Exit (multi-chunk drain)"
    );
    let zeds = acc.iter().filter(|&&b| b == b'Z').count();
    assert_eq!(
        zeds, n,
        "the entire multi-frame burst must arrive before Exit"
    );
    assert!(
        contains(&acc, b"TAIL-MARK"),
        "the trailing marker must arrive before Exit"
    );
    assert_eq!(exit, Some(Exit::Code(5)), "exit code must still propagate");
    drop(holder);
}

// ---------------------------------------------------------------------------
// M3: human screen restore via shpool_vt100 (design §6/§7). An opt-in
// `restore:"screen"` attach gets a clean `contents_formatted()` repaint instead
// of the raw byte ring; the default (and the engine's `exact` observer) still
// gets the byte-exact raw ring, and the vt100 screen tracks the pty geometry.
// ---------------------------------------------------------------------------

/// Drain a client's initial restore buffer: read `Data` frames until a short
/// read timeout (the holder sends the whole restore buffer at once on attach,
/// then the child lingers), accumulating the bytes. Stops on timeout/EOF/Exit.
fn drain_initial(c: &mut Client, ms: u64) -> Vec<u8> {
    c.set_read_timeout(Some(Duration::from_millis(ms))).unwrap();
    let mut acc = Vec::new();
    loop {
        match c.read_frame() {
            Ok(Some(ServerFrame::Data(d))) => acc.extend_from_slice(&d),
            Ok(Some(ServerFrame::Heartbeat)) => {}
            Ok(Some(ServerFrame::Exit(_))) | Ok(None) => break,
            Err(_) => break, // timeout — the restore buffer is fully drained
        }
    }
    acc
}

#[test]
fn screen_restore_repaints_while_raw_replays_the_exact_ring() {
    let dir = scratch_dir();
    // A full-screen sequence with NO newlines (so ONLCR can't rewrite it): red
    // "hello" on row 1, then jump to row 2 col 5 and print "world". The child
    // itself never emits a clear-screen or a bare cursor-home — those only
    // appear in a vt100 repaint, which is what distinguishes the two tiers.
    let holder = Holder::start(sh(
        "restore",
        "printf '\\033[31mhello\\033[0m\\033[2;5Hworld'; sleep 3",
        &dir,
    ))
    .unwrap();

    // Let the reader thread drain the child's output into BOTH the raw ring and
    // the vt100 screen before we attach.
    std::thread::sleep(Duration::from_millis(400));

    // A default (raw) reader — this is exactly the engine observer's path — gets
    // the byte-exact ring: the child's escapes verbatim, nothing added. This is
    // the regression guard: the raw tier must be unchanged.
    let mut raw = Client::connect_uid(Some(&dir), "restore").unwrap();
    let raw_buf = drain_initial(&mut raw, 500);
    assert_eq!(
        raw_buf, b"\x1b[31mhello\x1b[0m\x1b[2;5Hworld",
        "the raw ring must replay the child's exact bytes, unchanged"
    );

    // A `restore:"screen"` reader gets a vt100 repaint instead: it is prefixed
    // with show-cursor + SGR-reset + cursor-home + clear (`\x1b[?25h\x1b[m\x1b[H\x1b[J`)
    // — bytes the child never wrote — and reproduces the final screen content.
    let mut scr =
        Client::connect_uid_restore(Some(&dir), "restore", Role::Reader, Restore::Screen).unwrap();
    let scr_buf = drain_initial(&mut scr, 500);
    assert!(
        scr_buf.starts_with(b"\x1b[?25h\x1b[m\x1b[H\x1b[J"),
        "the repaint must start with the contents_formatted reset prefix, got {:?}",
        String::from_utf8_lossy(&scr_buf)
    );
    assert!(
        contains(&scr_buf, b"\x1b[J") && contains(&scr_buf, b"\x1b[H"),
        "the repaint must carry the clear + cursor-home the raw stream lacks"
    );
    assert!(
        contains(&scr_buf, b"hello") && contains(&scr_buf, b"world"),
        "the repaint must reproduce the final screen content, got {:?}",
        String::from_utf8_lossy(&scr_buf)
    );
    // And the repaint is emphatically NOT the raw ring.
    assert_ne!(
        scr_buf, raw_buf,
        "a screen-restore attach must not receive the raw ring"
    );
    assert!(
        !contains(&raw_buf, b"\x1b[J"),
        "the raw ring must not contain the repaint's clear-screen"
    );
    drop(holder);
}

#[test]
fn screen_restore_reflects_a_resize_of_the_vt100_screen() {
    let dir = scratch_dir();
    // Write "BOTTOM" at row 20 of the default 24-row screen, then linger.
    let holder =
        Holder::start(sh("restoresz", "printf '\\033[20;1HBOTTOM'; sleep 3", &dir)).unwrap();
    std::thread::sleep(Duration::from_millis(400));

    // Before any resize, the 24-row screen holds "BOTTOM" on row 20.
    let mut before =
        Client::connect_uid_restore(Some(&dir), "restoresz", Role::Reader, Restore::Screen)
            .unwrap();
    let before_buf = drain_initial(&mut before, 500);
    assert!(
        contains(&before_buf, b"BOTTOM"),
        "row-20 content must be present on the 24-row screen, got {:?}",
        String::from_utf8_lossy(&before_buf)
    );

    // A writer shrinks the pty to 10 rows. The holder resizes the vt100 screen
    // in lockstep, so row 20 falls off the now-10-row grid.
    let mut w = Client::connect_uid_as(Some(&dir), "restoresz", Role::Writer).unwrap();
    assert_eq!(w.granted_role(), Role::Writer);
    w.send_resize(90, 10).unwrap();
    // Give the holder a beat to apply the resize to the screen.
    std::thread::sleep(Duration::from_millis(300));

    // A fresh screen-restore attach now repaints a 10-row screen — "BOTTOM"
    // (row 20) is gone, proving the resize updated the vt100 dimensions.
    let mut after =
        Client::connect_uid_restore(Some(&dir), "restoresz", Role::Reader, Restore::Screen)
            .unwrap();
    let after_buf = drain_initial(&mut after, 500);
    assert!(
        !contains(&after_buf, b"BOTTOM"),
        "after shrinking to 10 rows the row-20 content must be gone, got {:?}",
        String::from_utf8_lossy(&after_buf)
    );
    drop(holder);
}
