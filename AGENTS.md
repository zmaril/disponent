# AGENTS.md

Guidance for coding agents working in this repo. For the *why* and the roadmap,
see [notes/design.md](./notes/design.md).

## What disponent is

A library (Rust core, bindings later) for dispatching work to coding agents —
locally in tmux or on remote environments like exe.dev — and monitoring it,
capability-graded. Environments are the source of truth; disponent's ledger is
a reconciled cache (memory by default, mirrored to SQLite via the driver plan).
An MCP surface is exposed by default so a supervising agent can drive it;
worker-role servers are observe-only, so dispatched agents can't recurse.

## Layout

```
crates/disponent-schema   the schema's single source of truth: entities, the event
                          union, and the op surface, AUTHORED as Rust derives with the
                          fluessig Rust-derive front end. Its two emit bins print the
                          catalog/api JSON. A codegen-time TOOL crate — its own nested
                          [workspace], excluded from the default cargo set; reaches the
                          fluessig derive crates via a gitignored `fluessig/` symlink.
schema/*.json             emitted catalog/api/docs — generated, committed.
crates/disponent-core     the engine (sync Rust): shipped catalog, ledger,
                          generated schema_gen.rs + mcp_generated.rs.
crates/disponent-cli      the `disponent` binary; `disponent mcp` is the stdio server.
crates/disponent-node     napi binding: the engine in-process in Node/Bun (generated
                          surface + hand-written core_impl seam; builds via @napi-rs/cli,
                          excluded from the default cargo set).
notes/design.md           the design doc (phases, MVP topology, decisions).
scripts/gen.sh            the regen chain (needs fluessig: ../fluessig or FLUESSIG_DIR).
```

## Build & test

```sh
cargo build --release
cargo test                # engine unit tests + the end-to-end stdio round-trip

# the napi addon -> .node + index.js + index.d.ts
cd crates/disponent-node && bun install && bun run build
bun run test              # the binding lifecycle over dry-run backends
cargo fmt --all --check && cargo clippy --all-targets -- -D warnings
scripts/gen.sh            # crates/disponent-schema → every generated artifact
```

## Conventions

- **One schema mechanism.** Change the data model or the op surface by editing
  `crates/disponent-schema` (the fluessig Rust-derive front end), then run
  `scripts/gen.sh` and commit the regenerated artifacts. Don't edit
  `schema_gen.rs`, `mcp_generated.rs`, or `schema/*.json` by hand. Regenerating
  needs a fluessig checkout (`../fluessig` or `FLUESSIG_DIR`) at the SHA pinned in
  CI (`FLUESSIG_REF`) / `crates/disponent-core/Cargo.toml` — no TypeSpec or Node.
- **The core stays synchronous.** Async is a per-binding concern (the entl
  discipline). The MCP stdio loop is a plain blocking read loop.
- **Honest capability edges.** An op a phase hasn't reached yet fails with a
  message saying what's missing (`no live env backend yet`) — it never fakes
  success. Same for observation fidelity: mark events exact/derived/scraped
  truthfully.
- **Secrets never enter the schema.** Endpoints are addresses; credentials live
  in config/templates outside the ledger.
- **Reap is the only exit.** Sessions run until someone reaps them; reap on a
  live session cancels first. "Done" belongs to the application developer.

## Working agreement

- **Do not commit, open PRs, or merge unless told.** Branch before committing on `main`.
- **Do not modify production unless told.**
- Report outcomes honestly — if tests fail, say so with the output.
