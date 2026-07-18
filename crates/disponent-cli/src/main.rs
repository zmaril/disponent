//! The disponent CLI. `disponent mcp` is the stdio MCP server over an
//! in-process engine; `disponent hold` / `disponent attach` are the headless
//! pty holder and its attach client (notes/owning-the-terminal.md). `attach` is
//! reader-default; `--write` requests the single writer lock (M2a).

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

use disponent_hold::{attach, Config, Holder};

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
}

#[derive(Clone, Copy, PartialEq, ValueEnum)]
pub enum Role {
    Supervisor,
    Worker,
}

fn main() -> anyhow::Result<()> {
    match Cli::parse().cmd {
        Cmd::Mcp { role, sink } => mcp_server::serve(role, sink.as_deref()),
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
    }
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
