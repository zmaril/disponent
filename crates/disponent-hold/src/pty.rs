//! The pty + child: open a pty pair, fork+exec the agent as a session leader
//! with the slave as its controlling terminal, and expose the master for
//! read/write/resize. Design §5 — the holder core.
//!
//! For M0 the pty handling is deliberately the *small* part (the hard vt100
//! restore is M3), so this uses `libc` directly — already in the tree, zero new
//! dependency — rather than `nix` / `portable-pty` / `shpool_pty`. The
//! fork+exec discipline rides `std::process::Command` with a `pre_exec` hook,
//! so only async-signal-safe calls (`setsid`, `ioctl`) run between fork and
//! exec; `Command` handles cwd + a cleared env safely.

use std::collections::BTreeMap;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};

/// A held pty: the master fd (read pty output, write input, resize) plus the
/// child process running under the slave.
pub struct Pty {
    master: OwnedFd,
    pub child: Child,
}

/// The child's controlling-terminal geometry at launch.
#[derive(Debug, Clone, Copy)]
pub struct WinSize {
    pub cols: u16,
    pub rows: u16,
}

impl Default for WinSize {
    fn default() -> WinSize {
        WinSize { cols: 80, rows: 24 }
    }
}

impl Pty {
    /// Open a pty and fork+exec `argv` under it as a session leader.
    ///
    /// * `argv[0]` is the program; `argv[1..]` its args.
    /// * `cwd` (if set) is the child's working directory.
    /// * `env` fully replaces the child's environment (we clear + set, so the
    ///   holder's own env never leaks unless named).
    /// * `size` is the initial window size.
    pub fn spawn(
        argv: &[String],
        cwd: Option<&str>,
        env: &BTreeMap<String, String>,
        size: WinSize,
    ) -> io::Result<Pty> {
        if argv.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty argv"));
        }
        let (master, slave) = open_pty(size)?;
        // The slave fd we hand pre_exec (for TIOCSCTTY). CLOEXEC so it does not
        // leak past the exec; it stays open through pre_exec, which is enough.
        set_cloexec(slave.as_raw_fd())?;
        let slave_raw = slave.as_raw_fd();

        let mut cmd = Command::new(&argv[0]);
        cmd.args(&argv[1..]);
        // Three independent dups of the slave for stdin/stdout/stderr; Command
        // dup2's each onto 0/1/2 (clearing CLOEXEC on those) so they survive the
        // exec as the child's controlling terminal.
        cmd.stdin(Stdio::from(slave.try_clone()?));
        cmd.stdout(Stdio::from(slave.try_clone()?));
        cmd.stderr(Stdio::from(slave.try_clone()?));
        if let Some(dir) = cwd {
            cmd.current_dir(dir);
        }
        cmd.env_clear();
        cmd.envs(env);

        // SAFETY: the closure runs post-fork, pre-exec, and calls only
        // async-signal-safe libc functions on a captured raw fd.
        unsafe {
            cmd.pre_exec(move || {
                // Become a session leader — detach from the holder's session so
                // the child owns its own process group / controlling terminal.
                if libc::setsid() == -1 {
                    return Err(io::Error::last_os_error());
                }
                // Acquire the slave as this session's controlling terminal.
                if libc::ioctl(slave_raw, libc::TIOCSCTTY as _, 0) == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }

        let child = cmd.spawn()?;
        // Parent no longer needs the slave (the child holds its own dups).
        drop(slave);
        Ok(Pty { master, child })
    }

    /// A dup of the master fd wrapped as a blocking file for the reader thread.
    pub fn master_reader(&self) -> io::Result<std::fs::File> {
        let dup = self.master.try_clone()?;
        Ok(std::fs::File::from(dup))
    }

    /// Consume the pty into its owned master fd (for input/resize) and the child.
    pub fn into_parts(self) -> (OwnedFd, Child) {
        (self.master, self.child)
    }
}

/// Write bytes to the pty master (client input → the agent's stdin).
pub fn write_master(fd: RawFd, data: &[u8]) -> io::Result<()> {
    let mut off = 0;
    while off < data.len() {
        // SAFETY: fd is a valid open master fd; slice is in-bounds.
        let n = unsafe {
            libc::write(
                fd,
                data[off..].as_ptr() as *const libc::c_void,
                data.len() - off,
            )
        };
        if n < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        off += n as usize;
    }
    Ok(())
}

/// Resize the pty (TIOCSWINSZ on the master) — a client `Resize` frame lands here.
pub fn resize_master(fd: RawFd, cols: u16, rows: u16) -> io::Result<()> {
    let ws = libc::winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: fd is a valid master fd; &ws is a valid winsize for the duration.
    let rc = unsafe { libc::ioctl(fd, libc::TIOCSWINSZ as _, &ws) };
    if rc == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Open a pty pair with the given initial size, returning `(master, slave)`.
fn open_pty(size: WinSize) -> io::Result<(OwnedFd, OwnedFd)> {
    let mut master: RawFd = -1;
    let mut slave: RawFd = -1;
    let mut ws = libc::winsize {
        ws_row: size.rows,
        ws_col: size.cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // The winsize/termios params are `*mut` on Apple's libc but `*const` on
    // Linux's. A `*mut` raw pointer coerces to `*const`, so passing one
    // satisfies both platforms; going through a pointer local (rather than
    // `&mut ws` at the call site) also avoids Linux clippy's
    // `unnecessary_mut_passed`, since there the param is `*const`.
    let ws_ptr: *mut libc::winsize = &mut ws;
    // SAFETY: out-params are valid locals; termios null = defaults; ws_ptr is a
    // valid winsize pointer for the duration of the call.
    let rc = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            ws_ptr,
        )
    };
    if rc == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: openpty returned two fresh, owned fds.
    Ok(unsafe { (OwnedFd::from_raw_fd(master), OwnedFd::from_raw_fd(slave)) })
}

/// Set FD_CLOEXEC on a fd.
fn set_cloexec(fd: RawFd) -> io::Result<()> {
    // SAFETY: fd is a valid open fd; F_GETFD/F_SETFD take no pointer args.
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFD);
        if flags == -1 {
            return Err(io::Error::last_os_error());
        }
        if libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) == -1 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}
