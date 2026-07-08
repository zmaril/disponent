//! The disponent CLI. One subcommand so far: `disponent mcp`, the stdio MCP
//! server over an in-process engine (the @manual serveMcp of the op surface).

use clap::{Parser, Subcommand, ValueEnum};

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
}

#[derive(Clone, Copy, PartialEq, ValueEnum)]
pub enum Role {
    Supervisor,
    Worker,
}

fn main() -> anyhow::Result<()> {
    match Cli::parse().cmd {
        Cmd::Mcp { role, sink } => mcp_server::serve(role, sink.as_deref()),
    }
}
