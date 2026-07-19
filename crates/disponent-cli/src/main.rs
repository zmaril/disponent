//! The disponent CLI. `disponent mcp` is the stdio MCP server over an
//! in-process engine; `disponent hold` / `disponent attach` are the headless
//! pty holder and its attach client (notes/owning-the-terminal.md). `attach` is
//! reader-default; `--write` requests the single writer lock (M2a).

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

use disponent_hold::{attach, Client, Config, Holder, ServerFrame};

mod mcp_server;

#[derive(Parser)]
#[command(name = "disponent", version, about = "Dispatch work to coding agents")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Serve the MCP tools over stdio against an in-process engine.
    Mcp {
        /// Surface scope: supervisor = the full surface; worker = observe-only
        /// (only the read-only tools — a dispatched agent can watch, not spawn).
        #[arg(long, value_enum, default_value_t = Role::Supervisor)]
        role: Role,

        /// Where the ledger mirrors: omitted = the managed SQLite file
        /// (~/.disponent/disponent.sqlite3), "none" = memory only, anything
        /// else = a SQLite path.
        #[arg(long)]
        sink: Option<String>,

        /// The worker session this server is bound to (the env sets this when it
        /// wires a `--role worker` endpoint). It self-scopes the two worker
        /// writes: `send` is recipient-forced to this session's Manager and
        /// `ack` may only touch this session's own inbox. Ignored for
        /// supervisor servers.
        #[arg(long)]
        bound_session: Option<String>,
    },

    /// Hold a pty for an agent: open a pty, exec the command as a session
    /// leader, and stream its byte-exact output over a framed unix socket with
    /// scrollback replay + real exit code (M0). Foreground by default.
    ///
    /// Example: `disponent hold my-uid --cwd /work -- claude --print`.
    Hold {
        /// The session uid — names the socket (`<socket-dir>/<uid>.sock`).
        uid: String,

        /// The child's working directory (default: inherit the holder's).
        #[arg(long)]
        cwd: Option<String>,

        /// Where the socket lives (default: $XDG_RUNTIME_DIR/disponent or
        /// /tmp/disponent).
        #[arg(long)]
        socket_dir: Option<PathBuf>,

        /// Scrollback ring size in bytes.
        #[arg(long, default_value_t = 256 * 1024)]
        ring_bytes: usize,

        /// Double-fork + setsid so the holder outlives this shell (a dead holder
        /// is still a dead pty — same as tmux). Default: foreground.
        #[arg(long)]
        daemonize: bool,

        /// The agent command, after `--`: `argv[0]` is the program.
        #[arg(last = true, required = true)]
        argv: Vec<String>,
    },

    /// Attach to a held session. Reader by default: streams the session's
    /// output but does NOT forward your stdin (design §6). Pass `--write` to
    /// request the single writer lock and drive the session — forwarding stdin
    /// and resizes; if another attacher already holds the writer you stay
    /// read-only. Exits with the child's code; restores the terminal on every
    /// exit path.
    ///
    /// `hold-attach` is a back-compat alias.
    #[command(visible_alias = "hold-attach")]
    Attach {
        /// The session uid to attach to.
        uid: String,

        /// Where the socket lives (must match the holder's `--socket-dir`).
        #[arg(long)]
        socket_dir: Option<PathBuf>,

        /// Request the writer lock (drive the session). Reader-only otherwise.
        #[arg(long, visible_alias = "take")]
        write: bool,
    },

    /// One-shot: type a line at a held session (a momentary writer that releases
    /// on exit), appending Enter — the holder analogue of `tmux send-keys …
    /// Enter`. Rejects honestly if a human attacher holds the writer. This is the
    /// remote INTERACT verb the engine drives over `ssh <vm> disponent hold-send`
    /// (M4, notes/owning-the-terminal.md §5).
    HoldSend {
        /// The session uid to send to.
        uid: String,
        /// The line to type (Enter is appended).
        input: String,
        /// Where the socket lives (must match the holder's `--socket-dir`).
        #[arg(long)]
        socket_dir: Option<PathBuf>,
    },

    /// One-shot: drain a held session's scrollback ring to stdout as the current
    /// snapshot — the holder analogue of `tmux capture-pane -p`. The engine's
    /// remote `capture` rides `ssh <vm> disponent hold-capture` (M4).
    HoldCapture {
        /// The session uid to snapshot.
        uid: String,
        /// Where the socket lives (must match the holder's `--socket-dir`).
        #[arg(long)]
        socket_dir: Option<PathBuf>,
    },

    /// One-shot: interrupt a held session (C-c on the pty) — the child survives.
    /// The holder analogue of `tmux send-keys C-c`; the engine's remote
    /// `interrupt` rides `ssh <vm> disponent hold-interrupt` (M4).
    HoldInterrupt {
        /// The session uid to interrupt.
        uid: String,
        /// Where the socket lives (must match the holder's `--socket-dir`).
        #[arg(long)]
        socket_dir: Option<PathBuf>,
    },

    /// One-shot: kill a held session's child (SIGKILL to its group) — the VM
    /// stays for inspection. The holder analogue of `tmux kill-session`; the
    /// engine's remote `kill` rides `ssh <vm> disponent hold-stop` (M4).
    HoldStop {
        /// The session uid to kill.
        uid: String,
        /// Where the socket lives (must match the holder's `--socket-dir`).
        #[arg(long)]
        socket_dir: Option<PathBuf>,
    },
}

#[derive(Clone, Copy, PartialEq, ValueEnum)]
pub enum Role {
    Supervisor,
    Worker,
}

fn main() -> anyhow::Result<()> {
    match Cli::parse().cmd {
        Cmd::Mcp {
            role,
            sink,
            bound_session,
        } => mcp_server::serve(role, sink.as_deref(), bound_session),
        Cmd::Hold {
            uid,
            cwd,
            socket_dir,
            ring_bytes,
            daemonize,
            argv,
        } => {
            let code = run_hold(uid, cwd, socket_dir, ring_bytes, daemonize, argv)?;
            std::process::exit(code);
        }
        Cmd::Attach {
            uid,
            socket_dir,
            write,
        } => {
            let code = attach(socket_dir.as_deref(), &uid, write)?;
            std::process::exit(code);
        }
        Cmd::HoldSend {
            uid,
            input,
            socket_dir,
        } => {
            // A momentary writer: send the line + Enter, then drop (releasing the
            // lock). `connect_writer_uid` errors honestly if a human holds it.
            let mut bytes = input.into_bytes();
            bytes.push(b'\n');
            Client::connect_writer_uid(socket_dir.as_deref(), &uid)?.send_input(&bytes)
        }
        Cmd::HoldCapture { uid, socket_dir } => hold_capture(socket_dir.as_deref(), &uid),
        Cmd::HoldInterrupt { uid, socket_dir } => {
            Client::connect_writer_uid(socket_dir.as_deref(), &uid)?.interrupt()
        }
        Cmd::HoldStop { uid, socket_dir } => {
            // Kill is the ungated control frame — a reader connection carries it.
            Client::connect_uid(socket_dir.as_deref(), &uid)?.kill()
        }
    }
}

/// Drain a holder's replayed ring to stdout, stopping when the stream goes quiet
/// (a short read timeout) — the byte-exact snapshot behind `hold-capture`.
/// Mirrors the engine's in-process `HolderCompute::capture`.
fn hold_capture(socket_dir: Option<&std::path::Path>, uid: &str) -> anyhow::Result<()> {
    use std::io::Write;
    let mut c = Client::connect_uid(socket_dir, uid)?;
    c.set_read_timeout(Some(std::time::Duration::from_millis(150)))?;
    let mut out = std::io::stdout();
    loop {
        match c.read_frame() {
            Ok(Some(ServerFrame::Data(d))) => out.write_all(&d)?,
            Ok(Some(ServerFrame::Heartbeat)) => {}
            Ok(Some(ServerFrame::Exit(_))) | Ok(None) => break,
            Err(e) => {
                // A read timeout (WouldBlock/TimedOut) means the ring is drained —
                // the normal end of a snapshot, not a fault. The drain-to-timeout
                // loop mirrors the engine's in-process `HolderCompute::capture` by
                // design (same holder ring, different transport).
                if let Some(io) = e.downcast_ref::<std::io::Error>() {
                    // straitjacket-allow:duplication — intentional parallel of HolderCompute::capture
                    use std::io::ErrorKind::{TimedOut, WouldBlock};
                    if matches!(io.kind(), WouldBlock | TimedOut) {
                        break;
                    }
                }
                return Err(e);
            }
        }
    }
    out.flush()?;
    Ok(())
}

/// Run the holder in the foreground (or daemonized), blocking until the child
/// exits, and return its exit code for the process to propagate.
fn run_hold(
    uid: String,
    cwd: Option<String>,
    socket_dir: Option<PathBuf>,
    ring_bytes: usize,
    daemonize: bool,
    argv: Vec<String>,
) -> anyhow::Result<i32> {
    if daemonize {
        // Reparent to init so the holder outlives this shell / ssh session.
        disponent_hold::daemonize()?;
    }
    let config = Config {
        uid,
        argv,
        cwd,
        // The operator's environment is what the agent should inherit.
        env: std::env::vars().collect::<BTreeMap<_, _>>(),
        socket_dir,
        ring_bytes,
        size: Default::default(),
    };
    let holder = Holder::start(config)?;
    let exit = holder.wait_for_exit();
    Ok(exit.process_code())
}
