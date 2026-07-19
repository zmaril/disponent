//! `disponent-hold` — a first-party headless pty session holder.
//!
//! This is milestone **M0** of `notes/owning-the-terminal.md`: the skeleton
//! holder disponent owns instead of renting a terminal from tmux. It opens a
//! pty, execs an agent under it as a session leader, drains the pty byte-exact
//! into a bounded scrollback ring, and serves that stream to N attach clients
//! over a framed unix socket — replaying the ring on connect, broadcasting live
//! `Data` frames, and reporting the child's **real exit code** in an `Exit`
//! frame when it dies. Local-only, foreground or `--daemonize`.
//!
//! What M0 buys (design §4): terminal frames become byte-**exact** rather than
//! `capture-pane`-scraped, and the session gets a real exit status — the two
//! highest-value wins, at the lowest-risk surface. It shares nothing heavy with
//! the engine; wiring it in behind a flag on the new `Compute`/monitor seam is
//! **M1**, deliberately out of this crate.
//!
//! Honesty (AGENTS.md): a dead holder is a dead pty. The holder buys fidelity
//! and a unified channel, not crash-persistence — the same failure tmux, ttyd,
//! and shpool all have.
//!
//! ## Shape
//!
//! * [`Config`] / [`Holder`] — open the pty, exec the agent, serve the socket.
//! * [`Client`] — dial a holder programmatically (the engine/pm/tests).
//! * [`attach`] — the interactive `disponent attach` terminal loop
//!   (reader-default; `--write` requests the writer lock — M2a; `--restore`
//!   asks for a clean vt100 screen repaint on reattach — M3).
//! * [`protocol`] — the framed wire format (see its module docs).

pub mod client;
pub mod protocol;
mod pty;
mod ring;
pub mod server;

pub use client::{attach, Client};
pub use protocol::{Exit, Restore, Role, ServerFrame};
pub use pty::WinSize;
pub use server::{daemonize, default_socket_dir, socket_path, Config, Holder};
