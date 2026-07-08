# disponent

Dispatch work to coding agents — locally in tmux or on remote environments
(exe.dev first) — and watch it run: capability-graded observation, a session
ledger you can sink anywhere, and an MCP surface so a supervising agent can
drive it.

The schema is the single source of truth: [`schema/disponent.tsp`](./schema/disponent.tsp)
(a [fluessig](https://github.com/zmaril/fluessig) catalog) generates the DDL,
the MCP tools, and eventually every language binding. Design and phasing live
in [`notes/design.md`](./notes/design.md).

## Status

The MVP topology is live: `disponent mcp` on an exe.dev VM, driven over
ssh-stdio by a supervising Claude Code, dispatching sibling worker VMs that
run claude in tmux behind a ttyd URL — plus a local backend that does the
same thing on your own machine. Every ledger change mirrors into a managed
SQLite file, `disponent_driver_plan` emits the whole state as an executable
plan for sqlite/postgres/duckdb, and reconcile adopts workers a crashed
disponent left behind. Early and moving fast; expect the surface to change.

## Install

From source (Rust stable):

```sh
git clone https://github.com/zmaril/disponent && cd disponent
cargo build --release        # → target/release/disponent
```

Regenerating the schema-derived code needs a [fluessig](https://github.com/zmaril/fluessig)
checkout (`../fluessig` or `FLUESSIG_DIR=<path>`): `scripts/gen.sh`.

## Usage

Serve the MCP tools over stdio against an in-process engine:

```sh
disponent mcp                      # full surface, managed SQLite ledger (~/.disponent)
disponent mcp --role worker        # observe-only (dispatched agents can't recurse)
disponent mcp --sink none          # memory-only ledger
```

Add it to a Claude Code MCP config — local or over ssh to wherever disponent runs:

```sh
claude mcp add disponent -- disponent mcp
claude mcp add disponent -- ssh -o BatchMode=yes my-supervisor.exe.xyz disponent mcp
```

Then dispatch with `disponent_dispatch`:

```json
{"spec": {"brief": "fix the flaky test in ci.yml", "env": "local", "repo": "owner/repo"}}
```

`env: "exe-dev"` needs a `template` — the name of an already-authed exe.dev VM
to copy (claude + gh auth, tmux, ttyd baked in). Observe with
`disponent_sessions` / `disponent_events`, type at a worker with
`disponent_send`, stop-but-keep with `disponent_cancel`, and destroy + archive
with `disponent_reap`. Sessions run until somebody reaps them.

## Contributing

Commits and PR titles follow [Conventional Commits](https://www.conventionalcommits.org)
(`type(scope): summary`); CI gates on rustfmt, clippy `-D warnings`, the test
suite, and zero drift in generated artifacts (`scripts/gen.sh`). Development
guidance for agents and humans alike lives in [AGENTS.md](./AGENTS.md).

## License

[MIT](./LICENSE)
