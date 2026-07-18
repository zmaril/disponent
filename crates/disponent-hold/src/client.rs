//! The attach client: dial a holder's socket, then either drive it
//! programmatically ([`Client`], used by the engine/pm/tests) or run the
//! interactive terminal loop ([`attach`], the `disponent hold-attach` CLI).
//!
//! [`attach`] is the precursor to M2's full `attach` op. It puts the local tty
//! in raw mode behind an RAII guard that restores it on **every** return path
//! (the lesson from shpool's `tty.rs` — forget it and the human's terminal
//! wedges), forwards stdin as `Input` frames, tracks `SIGWINCH` into `Resize`
//! frames, prints `Data` to stdout, and exits propagating the child's code when
//! the `Exit` frame arrives.

use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};

use crate::protocol::{
    encode_client, encode_resize, encode_signal, read_handshake, read_server_frame, ClientKind,
    Exit, ServerFrame,
};
use crate::server::socket_path;

/// A programmatic attach client: the handshake is consumed on connect, then
/// frames flow. Cloneable read/write halves let a caller read `Data` on one
/// thread and send `Input` on another.
pub struct Client {
    stream: UnixStream,
}

impl Client {
    /// Connect to a holder socket by path, consuming the handshake.
    pub fn connect(path: &Path) -> Result<Client> {
        let mut stream =
            UnixStream::connect(path).with_context(|| format!("connect {}", path.display()))?;
        read_handshake(&mut stream).context("read handshake")?;
        Ok(Client { stream })
    }

    /// Connect by uid, resolving the socket the same way the holder does.
    pub fn connect_uid(socket_dir: Option<&Path>, uid: &str) -> Result<Client> {
        Client::connect(&socket_path(socket_dir, uid))
    }

    /// Read the next server frame, or `None` at a clean EOF.
    pub fn read_frame(&mut self) -> Result<Option<ServerFrame>> {
        Ok(read_server_frame(&mut self.stream)?)
    }

    /// Send raw input bytes to the pty.
    pub fn send_input(&mut self, bytes: &[u8]) -> Result<()> {
        self.stream
            .write_all(&encode_client(ClientKind::Input, bytes))?;
        self.stream.flush()?;
        Ok(())
    }

    /// Request a resize.
    pub fn send_resize(&mut self, cols: u16, rows: u16) -> Result<()> {
        self.stream.write_all(&encode_resize(cols, rows))?;
        self.stream.flush()?;
        Ok(())
    }

    /// Send a detach frame (the holder drops us cleanly).
    pub fn detach(&mut self) -> Result<()> {
        self.stream
            .write_all(&encode_client(ClientKind::Detach, &[]))?;
        self.stream.flush()?;
        Ok(())
    }

    /// Deliver signal `sig` to the held child's process group (M1) — the
    /// control-frame stop the engine's `kill`/`stop_exec` rides.
    pub fn send_signal(&mut self, sig: i32) -> Result<()> {
        self.stream.write_all(&encode_signal(sig))?;
        self.stream.flush()?;
        Ok(())
    }

    /// Kill the held child (SIGKILL to its process group).
    pub fn kill(&mut self) -> Result<()> {
        self.send_signal(libc::SIGKILL)
    }

    /// Interrupt the held child (`C-c`) — the byte `0x03` on the pty, exactly
    /// what a terminal sends; the child's shell returns to a prompt.
    pub fn interrupt(&mut self) -> Result<()> {
        self.send_input(&[0x03])
    }

    /// Bound reads with a timeout so a snapshot read (drain the ring, then
    /// stop) doesn't block forever waiting for the next live frame.
    pub fn set_read_timeout(&self, dur: Option<std::time::Duration>) -> Result<()> {
        self.stream.set_read_timeout(dur)?;
        Ok(())
    }

    /// A cloned handle over the same connection.
    pub fn try_clone(&self) -> Result<Client> {
        Ok(Client {
            stream: self.stream.try_clone()?,
        })
    }

    /// Read `Data` frames until the payload contains `needle`, returning all
    /// bytes read. Errors on an `Exit` before the needle is seen. A test/engine
    /// convenience.
    pub fn read_until(&mut self, needle: &[u8]) -> Result<Vec<u8>> {
        let mut acc = Vec::new();
        loop {
            match self.read_frame()? {
                Some(ServerFrame::Data(d)) => {
                    acc.extend_from_slice(&d);
                    if contains(&acc, needle) {
                        return Ok(acc);
                    }
                }
                Some(ServerFrame::Heartbeat) => {}
                Some(ServerFrame::Exit(e)) => {
                    anyhow::bail!(
                        "child exited ({e:?}) before {:?} appeared",
                        String::from_utf8_lossy(needle)
                    )
                }
                None => anyhow::bail!(
                    "holder closed before {:?} appeared",
                    String::from_utf8_lossy(needle)
                ),
            }
        }
    }

    /// Read frames until the `Exit`, returning how the child ended.
    pub fn read_to_exit(&mut self) -> Result<Exit> {
        loop {
            match self.read_frame()? {
                Some(ServerFrame::Exit(e)) => return Ok(e),
                Some(_) => {}
                None => anyhow::bail!("holder closed before an exit frame"),
            }
        }
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || needle.len() > haystack.len() {
        return needle.is_empty();
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// SIGWINCH latch — set by the signal handler, drained by the resize thread.
static WINCH: AtomicBool = AtomicBool::new(false);

extern "C" fn on_winch(_sig: libc::c_int) {
    WINCH.store(true, Ordering::SeqCst);
}

/// RAII terminal-mode guard: raw on enter, original restored on drop — the
/// single most important correctness property of an attach client (shpool
/// `tty.rs`). Restores on normal return, `?` early-return, and panic-unwind.
struct RawGuard {
    fd: RawFd,
    orig: libc::termios,
    active: bool,
}

impl RawGuard {
    /// Put `fd` in raw mode if it is a tty; a no-op guard otherwise (so piping
    /// stdin, as tests do, still works).
    fn enter(fd: RawFd) -> io::Result<RawGuard> {
        // SAFETY: isatty takes an fd and no pointer.
        if unsafe { libc::isatty(fd) } != 1 {
            return Ok(RawGuard {
                fd,
                orig: unsafe { std::mem::zeroed() },
                active: false,
            });
        }
        // SAFETY: tcgetattr fills a valid termios; tcsetattr reads a valid one.
        unsafe {
            let mut orig: libc::termios = std::mem::zeroed();
            if libc::tcgetattr(fd, &mut orig) != 0 {
                return Err(io::Error::last_os_error());
            }
            let mut raw = orig;
            libc::cfmakeraw(&mut raw);
            if libc::tcsetattr(fd, libc::TCSANOW, &raw) != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(RawGuard {
                fd,
                orig,
                active: true,
            })
        }
    }
}

impl Drop for RawGuard {
    fn drop(&mut self) {
        if self.active {
            // SAFETY: restoring the saved termios on the same fd.
            unsafe {
                libc::tcsetattr(self.fd, libc::TCSANOW, &self.orig);
            }
        }
    }
}

/// Current window size of `fd` (TIOCGWINSZ), or None if it is not a tty.
fn win_size(fd: RawFd) -> Option<(u16, u16)> {
    // SAFETY: ws is a valid out-param for the ioctl.
    unsafe {
        let mut ws: libc::winsize = std::mem::zeroed();
        if libc::ioctl(fd, libc::TIOCGWINSZ as _, &mut ws) == 0 && ws.ws_col != 0 {
            Some((ws.ws_col, ws.ws_row))
        } else {
            None
        }
    }
}

/// Run the interactive attach loop against a holder socket, returning the
/// process exit code to propagate. Restores the terminal before returning.
pub fn attach(socket_dir: Option<&Path>, uid: &str) -> Result<i32> {
    let client = Client::connect_uid(socket_dir, uid)?;
    let stdin_fd = io::stdin().as_raw_fd();
    let stdout_fd = io::stdout().as_raw_fd();

    // Raw mode with guaranteed restore (dropped when this fn returns).
    let _guard = RawGuard::enter(stdin_fd).context("enter raw mode")?;

    // Install the SIGWINCH handler.
    // SAFETY: registering a static extern "C" handler for SIGWINCH.
    unsafe {
        libc::signal(libc::SIGWINCH, on_winch as *const () as usize);
    }

    // Send the initial size so the child matches this terminal immediately.
    let mut writer = client.try_clone()?;
    if let Some((cols, rows)) = win_size(stdout_fd) {
        let _ = writer.send_resize(cols, rows);
    }

    // stdin → Input frames (background; dies with the process on exit).
    {
        let mut input = client.try_clone()?;
        std::thread::spawn(move || {
            let mut buf = [0u8; 4096];
            let mut stdin = io::stdin();
            loop {
                match stdin.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        if input.send_input(&buf[..n]).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
    }

    // SIGWINCH → Resize frames (background).
    {
        let mut resizer = writer.try_clone()?;
        std::thread::spawn(move || loop {
            if WINCH.swap(false, Ordering::SeqCst) {
                if let Some((cols, rows)) = win_size(stdout_fd) {
                    let _ = resizer.send_resize(cols, rows);
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        });
    }

    // Output loop (this thread): Data → stdout, exit on the Exit frame.
    let mut reader = client;
    let mut out = io::stdout();
    let exit = loop {
        match reader.read_frame()? {
            Some(ServerFrame::Data(d)) => {
                out.write_all(&d)?;
                out.flush()?;
            }
            Some(ServerFrame::Heartbeat) => {}
            Some(ServerFrame::Exit(e)) => break e,
            None => break Exit::Code(0),
        }
    };
    // The guard drops here, restoring the terminal before we return the code.
    Ok(exit.process_code())
}
