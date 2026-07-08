# disponent

Dispatch work to coding agents — locally or to remote environments (exe.dev
first) — and watch it run: capability-graded observation, a session ledger you
can sink anywhere, and an MCP surface so a supervising agent can drive it.

The schema is the single source of truth: [`schema/disponent.tsp`](./schema/disponent.tsp)
(a [fluessig](https://github.com/zmaril/fluessig) catalog) generates the DDL,
the MCP tools, and eventually every language binding. Design and phasing live
in [`notes/design.md`](./notes/design.md).

## Status

Phase 3 — the exe.dev backend is wired. `disponent mcp` serves the full
generated tool surface over stdio; every ledger change mirrors into a managed
SQLite file (`~/.disponent/`, or `--sink <path>` / `--sink none`); and a
dispatch to `exe-dev` with a `template` (an already-authed template VM name)
provisions a throwaway worker in the background — copy the template, clone the
repo, run the dispatch's setup, start claude in tmux behind a ttyd URL. Cancel
stops the agent but keeps the VM for inspection; reap tears it down; reconcile
confirms/loses/adopts against `exe.dev ls` (environments are the source of
truth). `disponent_driver_plan` emits the whole state as an executable plan for
sqlite/postgres/duckdb. Next: the live MVP topology (phase 4) and the local
tmux backend.

## Try it

```sh
cargo build --release
./target/release/disponent mcp            # stdio MCP server, full surface
./target/release/disponent mcp --role worker   # observe-only surface

cargo test                                # engine tests + the stdio round-trip
scripts/gen.sh                            # regenerate from schema/disponent.tsp
                                          # (needs a fluessig checkout: ../fluessig
                                          #  or FLUESSIG_DIR=<path>)
```

Add it to a Claude Code MCP config to see the tools live:

```json
{"mcpServers": {"disponent": {"command": "/path/to/disponent", "args": ["mcp"]}}}
```
