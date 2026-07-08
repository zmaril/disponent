# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to
adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Local tmux backend: dispatch to `env: local` runs the agent on this machine
  in a `tmux -L disponent` session over a managed work dir, with the same
  clone → setup → agent order, cancel/reap split, and reconcile adoption as
  the remote backend.
- `EnvBackend` trait: the engine routes dispatches by environment kind and
  treats worker handles as opaque, per-backend JSON.
- Ledger rehydrate: opening over a SQLite sink restores dispatches, sessions,
  and events — restarts serve the full board, and running sessions get their
  observers back.
- Worker observation, two tiers. Scraped: every running session's terminal is
  polled (`capture-pane` on both backends) through a fluessig ObserverPool and
  its deltas land as `raw` events. Exact: an OTLP/http-json receiver
  (`DISPONENT_OTEL_PORT`, or `Engine::start_otel`) ingests Claude Code's own
  telemetry — prompts and responses as `message`, tool results as
  `toolResult`, api requests as `usage` with real token counts and cost —
  with workers wired automatically via `OTEL_*` env in their runners and
  every record stamped `disponent.session_uid`.

## [0.1.0] - 2026-07-08

### Added
- Initial release: the disponent engine (in-memory ledger mirrored into a
  managed SQLite sink via fluessig plans), the exe.dev backend (template-copy
  provisioning, tmux + ttyd workers, tag-based reconcile with orphan
  adoption), `driverPlan` for sqlite/postgres/duckdb, and `disponent mcp` —
  the generated tool surface over stdio JSON-RPC with an observe-only worker
  role. The schema (`schema/disponent.tsp`) is a fluessig catalog; every
  generated artifact regenerates via `scripts/gen.sh`.
