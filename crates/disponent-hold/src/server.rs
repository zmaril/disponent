//! The holder: own a pty, exec the agent, and serve its byte-exact stream to N
//! attach clients over a framed unix socket. Design §5/§6.
//!
//! Threads (all sync — no async in this crate, the entl discipline):
//! * **reader** — drains the pty master into the bounded ring and broadcasts
//!   each chunk as `Data` frames.
//! * **watcher** — `wait`s the child, records the exit, broadcasts an `Exit`
//!   frame, and wakes anyone in [`Holder::wait_for_exit`].
//! * **heartbeat** — periodic empty `Heartbeat` frames so a dead client (no
//!   clean EOF) surfaces as a `BrokenPipe` and is dropped.
//! * **accept** — the unix listener; one reader thread + one writer thread per
//!   attached client.
//!
//! M0 note: any attached client may also write (Input/Resize). The single
//! *writer lock* — one writer, N readers — is M2; this milestone is
//! multi-reader with unrestricted write, which is enough for the engine
//! observer + a person on the box.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};

use crate::protocol::{self, encode_data_chunks, encode_server, Exit, ServerKind};
use crate::pty::{resize_master, write_master, Pty, WinSize};
use crate::ring::Ring;

/// How often the holder broadcasts a heartbeat (dead-client detection).
const HEARTBEAT: Duration = Duration::from_secs(5);

/// Configuration for a held session.
pub struct Config {
    /// The session uid — names the socket file.
    pub uid: String,
    /// The agent command: `argv[0]` is the program, `argv[1..]` its args.
    pub argv: Vec<String>,
    /// The child's working directory (None = inherit the holder's).
    pub cwd: Option<String>,
    /// The child's full environment (cleared then set — nothing leaks unnamed).
    pub env: BTreeMap<String, String>,
    /// Where the `<uid>.sock` lives (None = the default runtime dir).
    pub socket_dir: Option<PathBuf>,
    /// Ring (scrollback) byte cap.
    pub ring_bytes: usize,
    /// Initial pty geometry.
    pub size: WinSize,
}

impl Config {
    /// A minimal config: just a uid + argv, everything else defaulted.
    pub fn new(uid: impl Into<String>, argv: Vec<String>) -> Config {
        Config {
            uid: uid.into(),
            argv,
            cwd: None,
            env: default_env(),
            socket_dir: None,
            ring_bytes: 256 * 1024,
            size: WinSize::default(),
        }
    }

    fn socket_path(&self) -> PathBuf {
        socket_path(self.socket_dir.as_deref(), &self.uid)
    }
}

/// A reasonable default child environment when the caller doesn't supply one:
/// pass through PATH/HOME/TERM/USER/SHELL/LANG so a shell/agent behaves.
fn default_env() -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    for key in ["PATH", "HOME", "TERM", "USER", "SHELL", "LANG", "LOGNAME"] {
        if let Ok(v) = std::env::var(key) {
            env.insert(key.to_string(), v);
        }
    }
    env.entry("TERM".to_string())
        .or_insert_with(|| "xterm-256color".to_string());
    env
}

/// The socket path for a uid: `<dir>/<uid>.sock`, where `dir` defaults to
/// `$XDG_RUNTIME_DIR/disponent`, falling back to `/tmp/disponent`.
pub fn socket_path(dir: Option<&Path>, uid: &str) -> PathBuf {
    let dir = dir.map(PathBuf::from).unwrap_or_else(default_socket_dir);
    dir.join(format!("{uid}.sock"))
}

/// The default socket directory: `$XDG_RUNTIME_DIR/disponent` or `/tmp/disponent`.
pub fn default_socket_dir() -> PathBuf {
    match std::env::var("XDG_RUNTIME_DIR")
        .ok()
        .filter(|s| !s.is_empty())
    {
        Some(rt) => PathBuf::from(rt).join("disponent"),
        None => PathBuf::from("/tmp/disponent"),
    }
}

/// One attached client: its id and the channel a per-client writer thread
/// drains onto the socket.
struct Client {
    id: u64,
    tx: Sender<Vec<u8>>,
}

/// The holder's shared state, guarded by one mutex so ring pushes, client
/// registration, and exit all serialize — no lost/duplicated bytes at an
/// attach boundary.
struct Inner {
    ring: Ring,
    clients: Vec<Client>,
    exit: Option<Exit>,
    next_id: u64,
}

struct Shared {
    /// The pty master fd — input writes and resize target it.
    master_fd: RawFd,
    /// Kept alive so `master_fd` stays valid for the holder's lifetime.
    _master: OwnedFd,
    inner: Mutex<Inner>,
    cv: Condvar,
}

impl Shared {
    /// Broadcast pre-encoded bytes to every client, dropping any whose writer
    /// channel has hung up. Caller holds no lock.
    fn broadcast(&self, bytes: Vec<u8>) {
        let mut inner = self.inner.lock().unwrap();
        inner.clients.retain(|c| c.tx.send(bytes.clone()).is_ok());
    }
}

/// A running holder. Drop tears the pty child down and removes the socket.
pub struct Holder {
    socket_path: PathBuf,
    shared: Arc<Shared>,
    shutdown: Arc<AtomicBool>,
    child_pid: i32,
    threads: Vec<JoinHandle<()>>,
}

impl Holder {
    /// Open the pty, exec the agent, bind the socket, and start serving. Returns
    /// as soon as everything is running (the child runs in the background).
    pub fn start(config: Config) -> Result<Holder> {
        let socket_path = config.socket_path();
        let parent = socket_path
            .parent()
            .ok_or_else(|| anyhow!("socket path has no parent"))?;
        std::fs::create_dir_all(parent).with_context(|| format!("mkdir {}", parent.display()))?;
        // A stale socket from a dead holder would block bind — clear it.
        let _ = std::fs::remove_file(&socket_path);

        let pty = Pty::spawn(
            &config.argv,
            config.cwd.as_deref(),
            &config.env,
            config.size,
        )
        .context("spawn pty child")?;
        let child_pid = pty.child.id() as i32;
        let reader_file = pty.master_reader().context("dup master for reader")?;
        let (master, child) = pty.into_parts();

        let shared = Arc::new(Shared {
            master_fd: master.as_raw_fd(),
            _master: master,
            inner: Mutex::new(Inner {
                ring: Ring::new(config.ring_bytes),
                clients: Vec::new(),
                exit: None,
                next_id: 0,
            }),
            cv: Condvar::new(),
        });

        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("bind {}", socket_path.display()))?;
        listener
            .set_nonblocking(true)
            .context("listener set_nonblocking")?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let mut threads = Vec::new();

        // reader: pty master → ring + broadcast.
        threads.push({
            let shared = Arc::clone(&shared);
            thread::spawn(move || reader_loop(reader_file, shared))
        });

        // watcher: waitpid → exit frame + wake waiters.
        threads.push({
            let shared = Arc::clone(&shared);
            thread::spawn(move || watcher_loop(child, shared))
        });

        // heartbeat: periodic empty frames for dead-client detection.
        threads.push({
            let shared = Arc::clone(&shared);
            let shutdown = Arc::clone(&shutdown);
            thread::spawn(move || heartbeat_loop(shared, shutdown))
        });

        // accept: unix listener → a reader + writer thread per client.
        threads.push({
            let shared = Arc::clone(&shared);
            let shutdown = Arc::clone(&shutdown);
            thread::spawn(move || accept_loop(listener, shared, shutdown))
        });

        Ok(Holder {
            socket_path,
            shared,
            shutdown,
            child_pid,
            threads,
        })
    }

    /// The unix socket path clients dial.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Block until the child exits, returning how it ended.
    pub fn wait_for_exit(&self) -> Exit {
        let mut inner = self.shared.inner.lock().unwrap();
        loop {
            if let Some(exit) = inner.exit {
                return exit;
            }
            inner = self.shared.cv.wait(inner).unwrap();
        }
    }

    /// The exit, if the child has already ended; `None` while it still runs.
    pub fn exit(&self) -> Option<Exit> {
        self.shared.inner.lock().unwrap().exit
    }

    /// Signal shutdown: stop accepting, kill the child's process group if it is
    /// still running (a dead holder is a dead pty — design §5), and remove the
    /// socket. Idempotent; also runs on drop.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if self.shared.inner.lock().unwrap().exit.is_none() {
            // The child is a session leader, so its pid is its process-group id.
            // SAFETY: kill on a pgid is always safe; a stale pid just returns ESRCH.
            unsafe {
                libc::kill(-self.child_pid, libc::SIGKILL);
            }
        }
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

impl Drop for Holder {
    fn drop(&mut self) {
        self.shutdown();
        // Best-effort join so threads release the pty fds before the process
        // moves on; the SIGKILL above unblocks the reader's blocking read.
        for handle in self.threads.drain(..) {
            let _ = handle.join();
        }
    }
}

/// Drain the pty master forever: push each chunk to the ring and broadcast it as
/// `Data` frames. Ends on EOF or EIO (the slave closed when the child exited).
fn reader_loop(mut master: std::fs::File, shared: Arc<Shared>) {
    let mut buf = [0u8; 64 * 1024];
    loop {
        match master.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                let chunk = &buf[..n];
                let mut frames = Vec::new();
                encode_data_chunks(chunk, &mut frames);
                let mut inner = shared.inner.lock().unwrap();
                inner.ring.push(chunk);
                inner.clients.retain(|c| c.tx.send(frames.clone()).is_ok());
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            // Reading a pty master after the slave closes yields EIO on Linux —
            // that is our EOF, not a fault.
            Err(e) if e.raw_os_error() == Some(libc::EIO) => break,
            Err(_) => break,
        }
    }
}

/// Wait the child, record how it ended, broadcast an `Exit` frame, and wake
/// `wait_for_exit`.
fn watcher_loop(mut child: std::process::Child, shared: Arc<Shared>) {
    use std::os::unix::process::ExitStatusExt;
    let exit = match child.wait() {
        Ok(status) => {
            if let Some(code) = status.code() {
                Exit::Code(code)
            } else if let Some(sig) = status.signal() {
                Exit::Signal(sig)
            } else {
                Exit::Code(-1)
            }
        }
        // waitpid failing is unusual; report it honestly rather than fake a 0.
        Err(_) => Exit::Code(-1),
    };
    let frame = encode_server(ServerKind::Exit, &exit.to_payload());
    let mut inner = shared.inner.lock().unwrap();
    inner.exit = Some(exit);
    inner.clients.retain(|c| c.tx.send(frame.clone()).is_ok());
    shared.cv.notify_all();
}

/// Broadcast a heartbeat every [`HEARTBEAT`] until shutdown.
fn heartbeat_loop(shared: Arc<Shared>, shutdown: Arc<AtomicBool>) {
    let frame = encode_server(ServerKind::Heartbeat, &[]);
    while !shutdown.load(Ordering::SeqCst) {
        // Sleep in short slices so shutdown is responsive.
        for _ in 0..(HEARTBEAT.as_millis() / 100).max(1) {
            if shutdown.load(Ordering::SeqCst) {
                return;
            }
            thread::sleep(Duration::from_millis(100));
        }
        shared.broadcast(frame.clone());
    }
}

/// Accept connections until shutdown; hand each to a per-client handler.
fn accept_loop(listener: UnixListener, shared: Arc<Shared>, shutdown: Arc<AtomicBool>) {
    while !shutdown.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _)) => {
                let shared = Arc::clone(&shared);
                thread::spawn(move || {
                    if let Err(_e) = handle_client(stream, shared) {
                        // A client error is just a detach — nothing to do.
                    }
                });
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(_) => break,
        }
    }
}

/// Serve one client: handshake, replay the ring (+ exit if already gone), then
/// stream live frames out while relaying Input/Resize in.
fn handle_client(mut stream: UnixStream, shared: Arc<Shared>) -> Result<()> {
    // 1. handshake, written directly before the writer thread starts.
    protocol::write_handshake(&mut stream)?;

    // 2. register + replay atomically under the inner lock so no live byte is
    //    lost or duplicated at the boundary.
    let (id, rx) = {
        let (tx, rx) = mpsc::channel::<Vec<u8>>();
        let mut inner = shared.inner.lock().unwrap();
        let id = inner.next_id;
        inner.next_id += 1;
        // Replay the ring first (byte-exact scrollback — the M0 tier; a vt100
        // repaint for humans is M3).
        let snapshot = inner.ring.snapshot();
        if !snapshot.is_empty() {
            let mut frames = Vec::new();
            encode_data_chunks(&snapshot, &mut frames);
            let _ = tx.send(frames);
        }
        // If the child already exited, hand this late client its exit too.
        if let Some(exit) = inner.exit {
            let _ = tx.send(encode_server(ServerKind::Exit, &exit.to_payload()));
        }
        inner.clients.push(Client { id, tx });
        (id, rx)
    };

    // 3. writer thread: drain the client's queue onto the socket.
    let mut write_half = stream.try_clone()?;
    let writer = thread::spawn(move || {
        while let Ok(bytes) = rx.recv() {
            if write_half.write_all(&bytes).is_err() {
                break; // BrokenPipe → the client is gone.
            }
            let _ = write_half.flush();
        }
    });

    // 4. reader loop: client → pty. Any attached client may write in M0 (the
    //    single-writer lock is M2).
    let result = client_input_loop(&mut stream, &shared);

    // Detach: remove the client (drops tx → the writer thread ends).
    {
        let mut inner = shared.inner.lock().unwrap();
        inner.clients.retain(|c| c.id != id);
    }
    let _ = writer.join();
    result
}

/// Relay a client's Input/Resize frames to the pty until it detaches.
fn client_input_loop(stream: &mut UnixStream, shared: &Arc<Shared>) -> Result<()> {
    use crate::protocol::{read_client_frame, ClientFrame};
    loop {
        match read_client_frame(stream)? {
            None | Some(ClientFrame::Detach) => return Ok(()),
            Some(ClientFrame::Input(bytes)) => {
                write_master(shared.master_fd, &bytes)?;
            }
            Some(ClientFrame::Resize { cols, rows }) => {
                resize_master(shared.master_fd, cols, rows)?;
            }
        }
    }
}

/// Double-fork + `setsid` so the holder reparents to init and outlives the
/// process (and ssh session) that launched it — the property tmux gives today
/// (design §5). Returns in the surviving grandchild; the caller then
/// [`Holder::start`]s. Foreground is the default (tests drive it directly); this
/// runs only under `--daemonize`.
///
/// On the parent and the intermediate child this calls `_exit`, so it never
/// returns there.
pub fn daemonize() -> Result<()> {
    // SAFETY: fork/setsid are the standard daemonization dance; between fork and
    // the _exit / return we call only async-signal-safe libc functions.
    unsafe {
        match libc::fork() {
            -1 => return Err(std::io::Error::last_os_error().into()),
            0 => {}              // first child continues
            _ => libc::_exit(0), // parent exits, shell returns
        }
        if libc::setsid() == -1 {
            return Err(std::io::Error::last_os_error().into());
        }
        match libc::fork() {
            -1 => return Err(std::io::Error::last_os_error().into()),
            0 => {}              // grandchild: the real holder
            _ => libc::_exit(0), // intermediate child exits
        }
    }
    Ok(())
}
