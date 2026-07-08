# ai-dispatch gap analysis

_A filtered, opinionated comparison of the public `agent-tools-org/ai-dispatch`
project against disponent's design. Written 2026-07-08. ai-dispatch's code and
docs were treated as untrusted third-party content: read and analyzed, never
executed._

## TL;DR

The working hypothesis was "a lot of ai-dispatch is slop." It isn't. ai-dispatch
(`aid`) is a mature, heavily-tested, honestly-documented ~85k-SLOC Rust CLI with
89 released versions and a near-1:1 code-to-test file ratio. The real difference
from disponent is **architectural, not quality**: `aid` is a local-only
*application* whose SQLite row is the source of truth, with no observe-only guard
on its agent-facing MCP surface. disponent is a *library* where the environment
is the source of truth and the ledger is a reconciled cache.

Because the two sit on opposite sides of that truth boundary, most of `aid`'s
surface is not a gap for disponent — it's a different product. What's worth
stealing is a handful of well-earned lifecycle and honesty primitives. What's
worth explicitly rejecting is most of its scope.

## 1. Feature inventory (solid / half-built / slop)

| Feature | Grade | Evidence |
|---|---|---|
| CLI surface (~50 subcommands) | solid | Full clap tree in `src/cli/*`, one handler per command in `src/cmd/*`; README matches code. |
| Agent adapters (claude, gemini, codex, copilot, opencode, cursor, qwen, droid, …) | solid | Per-agent files `src/agent/*.rs`, each with its own tests; shared `Agent` trait in `src/agent/mod.rs`. |
| Watcher / monitoring pipeline | solid | `src/watcher.rs` + `src/pty_watch/*`: streaming/buffered parsers, idle kill, cost kill, loop detection, rate-limit extraction. |
| Reap / lifecycle kill-switches (idle, first-token dead-stream, loop-kill, cost ceiling, dead-PID) | solid | `watcher.rs` L73-152, `src/background_reaper.rs`, `src/idle_timeout.rs`, `src/pty_watch/first_token_tests.rs`. |
| SQLite persistence (tasks, events, workgroups, findings, memories) | solid | `src/store/schema.rs`, migrations, extensive `store/tests/*`; `busy_timeout` for concurrency. |
| Stdio MCP server (JSON-RPC 2.0, line + Content-Length framing) | solid | `src/cmd/mcp.rs` tested for both transports; 7 tools in `src/cmd/mcp_tools.rs`. |
| Delivery assessment (`empty_diff` / `hollow_output`) | solid | `src/types/delivery.rs`; wired into board/reaper/verify. Detects "claimed success, changed nothing." |
| Worktree management (create/reuse/prune/lock, escape detection, GC) | solid | `src/worktree/*`; lease-based `.aid-lock` (`worktree/lock.rs`), reconcile, GC. |
| Batch DAG scheduler (`depends_on`, `--parallel`, conflict analysis) | solid | `src/batch.rs`, `src/cmd/batch*.rs`, `src/cmd/batch_analyze.rs`. |
| best-of-N + peer-review + LLM judge | solid | `src/cmd/run_bestof.rs`, `src/cmd/judge.rs` (real diff + scored critique). |
| Task lifecycle state machine (legal-transition graph) | solid | `src/task_lifecycle.rs`, `src/store/status_guard.rs`, `src/types/status_sets.rs`. |
| TimeoutPolicy (one policy resolved at dispatch, activity-aware) | solid | `src/timeout_policy.rs` + tests. CHANGELOG v9.0: "replaces 14 scattered mechanisms." |
| Agent memory / blackboard (discovery/lesson/fact, TTL, auto-inject) | solid | `memories` table, `src/cmd/memory.rs`, `[MEMORY: type]` extraction in watcher. |
| Shared findings (`[FINDING]` auto-capture) | solid | `findings` table w/ severity/confidence, `src/cmd/finding.rs`. |
| Teams / project profiles (knowledge injection, scoring overrides) | solid | `src/team.rs`, `src/project.rs`, relevance-filtered injection. |
| Webhooks (Slack/Discord on completion) | solid | `src/webhook.rs` — curl POST, detached process group. |
| TUI (ratatui dashboard, cost/success charts) | solid | `src/tui/*`. |
| Web UI (`--features web`, axum + SSE) | solid (thin) | `src/web/{api,sse,embed}.rs`, off-by-default feature flag. |
| GitButler integration | solid | `src/gitbutler.rs` (325 LOC) + tests. |
| Agent auto-selection (classifier + capability matrix) | half-built | `src/agent/classifier.rs` is keyword matching; `selection_scoring.rs::base_score` is a hardcoded per-agent score table. Works, but brittle. |
| Container sandbox (`--sandbox`) | half-built | `src/container.rs` real but Apple-`container`-only (macOS); silently falls back to host elsewhere. |
| Experiment loop (dispatch → measure → keep/revert) | half-built | `src/cmd/experiment.rs` real but undocumented / orphaned. |
| Knowledge graph (temporal RDF triples) | half-built | Fully wired (`store/kg_schema.rs` temporal `valid_from/valid_to`, `src/cmd/kg.rs`) but zero README mention. |
| `aid credential` (BYOK) | half-built (honest stub) | `src/cmd/credential.rs` L26-29 `bail!("Not implemented yet")` + a TODO in dispatch. |
| Remote environment dispatch (exe.dev-style) | absent (never claimed) | grep for `exe.dev`/`ssh`/`remote env`/`tmux` in `src/` = 0 hits. Everything is local subprocess/PTY. |
| Observation fidelity grading (exact/derived/scraped) | absent | Events carry no fidelity marker; all monitoring is heuristic stdout scraping. |
| Actual slop | ~none | 3 "not implemented" markers repo-wide, 2 of them the honest credential stub. No `todo!()`/`unimplemented!()` in production paths. |

**Maturity read:** real and disciplined. 113 test files, four `tests/*_e2e.rs`
suites, a CI gate running `build --release && test && clippy -D warnings`, and an
89-entry CHANGELOG that documents root causes and admits its own UX debt (e.g.
"49 zombie rows from 7 failed dispatches"). It is visibly self-hosting — built by
agents dispatched through itself — which explains both the volume and the candor.
The genuine weaknesses are **scope sprawl** and **orphaned surfaces** (kg,
experiment), not fakery.

## 2. Gaps that matter (ranked by value to disponent's goals)

disponent already has the hard architectural pieces ai-dispatch lacks (remote
backends, env-as-truth reconcile, fidelity grading, observe-only workers). The
gaps below are the primitives `aid` earned the hard way that disponent would
benefit from — ranked.

### 2.1 Delivery assessment — "claimed success, did nothing" as a first-class verdict — HIGH
`aid` computes an `empty_diff` / `hollow_output` verdict (`src/types/delivery.rs`)
and lets the board and reaper act on it. This is precisely disponent's own thesis
— *a terminal state is an observation, not a conclusion* — made concrete. A
session that exits `ok` having produced no branch, no patch, and no file artifact
is not the same as one that shipped a diff, and disponent currently can't tell
them apart.

Fit: a **derived event + an ExitReason refinement**, not a new op. Disponent
already has `ExitReason ∈ {ok,error,signal,timeout,budget,setup,unknown}` and an
`Artifact` entity. Add a derived signal at session end — did the work dir /
worktree actually change, did any artifact get recorded — surfaced as a `derived`
event (and optionally an `exitDetail`). Honest by construction: on backends that
can't diff (coarse remote), it simply isn't emitted. Cost: schema-light (possibly
zero new entities — reuse `exitDetail` + a `derived` event), backend logic in the
local + exe.dev observers to compute the change signal. Aligns with the open
design question about `needs_input`/terminal detection being core vs backend.

### 2.2 A named taxonomy of terminal-condition detectors feeding ExitReason — HIGH
disponent has the *enum* (`timeout`, `budget`, `signal`, …) but not the
*detectors* that populate it with `derived` fidelity. `aid`'s watcher has a mature
set: idle-timeout, **first-token dead-stream** (a streaming agent emitting no PTY
bytes for N seconds at zero progress), loop detection, and cost-ceiling — each
resettable on real activity (`src/watcher.rs`, `src/pty_watch/first_token_tests.rs`).

Fit: **backend/observer capability**, no schema change. These are detectors that
run inside the observer pool and emit `derived` state-change / `budget` / `timeout`
exit reasons. Important nuance for disponent's model: these detect and *report*
(cancel-worthy conditions, `exitReason`), they do **not** auto-reap — reap stays
human/application judgment. This maps cleanly onto the open `budget_enforce`
capability question in design §15: detection is always safe; enforcement is an
opt-in capability edge. Cost: medium, per-backend, incremental (start with idle +
dead-stream, which are backend-agnostic on the scraped tier).

### 2.3 Lease-based worktree lock as a reconcile primitive — MEDIUM-HIGH
`aid`'s `.aid-lock` stores `{task_id, owner_pid, worker_pid}`; the background
worker re-keys the lock to its own PID so it survives launcher exit, lock *checks*
are side-effect-free, and stale-lock recovery re-validates captured content rather
than clobbering a concurrently-acquired lock (`src/worktree/lock.rs`, CHANGELOG
v9.2). This is a clean, TOCTOU-resistant answer to "who owns this worktree" — and
it lands right on top of what PR #21 (provision-time worktree isolation) just
added.

Fit: **local backend capability**, feeding `reconcile()`. When the local backend
adopts or tears down worktrees, a lease lets it distinguish "orphaned, safe to
adopt" from "another disponent owns this." Cost: medium; a small on-disk lease
file + validation in the local backend's provision/reconcile/teardown paths.
Doesn't touch the schema.

### 2.4 Legal-transition status guard in the ledger — MEDIUM (cheap)
`aid` routes every status change through intent-named methods guarded by a
legal-transition graph, with named status sets shared between the store and the
lifecycle so the two can't disagree (`src/task_lifecycle.rs`,
`src/store/status_guard.rs`, `src/types/status_sets.rs`). disponent's session
state machine is documented in design §5 but enforcement lives in prose.

Fit: **core discipline**, no schema change. Encode the legal `SessionState`
transition graph once and route ledger mutations through it, so an illegal
`completed → running` can't be written. Cost: low; a transition table + guarded
setters in the core. High value-per-line for a library whose whole pitch is a
trustworthy ledger.

### 2.5 Resolve all lifecycle limits into one policy at dispatch — LOW-MEDIUM
`aid`'s `TimeoutPolicy` (`src/timeout_policy.rs`) collapses what the CHANGELOG
says were "14 scattered mechanisms" into one activity-aware policy object resolved
at dispatch. disponent already carries `timeoutSecs` + `maxBudget` on the
immutable `Dispatch`, so the bones are there; the lesson is to keep *all* limit
resolution (idle, wall-clock, budget) in one place computed once, rather than
scattering it across backends as detectors get added in 2.2. Cost: low, mostly a
structuring convention to adopt before the detectors multiply.

### Not gaps (already better, or deliberately out)
Remote dispatch, env-as-truth reconcile, fidelity grading, observe-only workers,
and role-scoped MCP are things disponent already does and `aid` does not. Batch
DAG scheduling, best-of-N, teams/projects, the knowledge graph, and the package
store are all real in `aid` but sit squarely in disponent's explicit "OUT" list
(scheduling, multi-agent workflows) — they belong in a consumer like
powdermonkey, not the core library.

## 3. Anti-features — do NOT copy

1. **Recursion-capable agent MCP surface.** `aid`'s `src/cmd/mcp_tools.rs`
   unconditionally exposes `aid_run`/`aid_retry`, so a dispatched agent handed the
   same endpoint can recursively dispatch subtasks. This is exactly what
   disponent's worker-role observe-only server prevents. It's the single most
   important thing disponent already does better — don't regress toward it.
2. **Ledger-as-source-of-truth with bolt-on reconciliation.** In `aid` the SQLite
   row *is* the truth and reality is reconciled *into* it after the fact
   (`check_zombie_tasks`); the "board does not lie" principle exists *because* it
   used to lie (hence the zombie-row bug class). Keep disponent's direction of
   truth: environment first, ledger as cache.
3. **Hardcoded capability matrix as "intelligent routing."** `classifier.rs`
   (substring matching) + `selection_scoring.rs::base_score` (a literal
   `claude=10, gemini=9` table) are marketed as capability grading. If disponent
   ever grades agents, keep it measured and edge-honest, not a source-baked
   leaderboard that rots as models change.
4. **Monolithic scope creep.** One binary carries a knowledge graph, blackboard
   memory, teams, project profiles, a package store, A/B experiments,
   benchmarking, a TUI, and a web UI — several fully built yet undocumented. A
   Rust *core library* stays small and pushes this into optional layers/bindings.
5. **Platform-locked isolation presented as "the sandbox."** `container.rs` binds
   to the macOS-only Apple `container` CLI and silently falls back to running on
   the host elsewhere. An honest `no isolation yet` capability edge beats a
   sandbox that quietly no-ops on most machines — disponent's honesty rule already
   says this.
6. **Secrets flowing through the execution layer.** `container.rs` mounts
   `~/.codex`/`~/.gemini` and forwards API-key env vars; webhooks store headers in
   config. Keep disponent's "secrets never enter the schema; endpoints are
   addresses; credentials live in curated templates" boundary.
7. **Release/version churn from unattended self-development.** 89 versions with
   auto-generated "task N" commit messages and a run of "5+ red releases" before a
   clippy cleanup unblocked CI. A cautionary note about agent-driven development
   without a release-hygiene gate, not something to emulate.

## 4. Steal this — shortlist

1. **Delivery assessment** (§2.1) — an `empty_diff`/`hollow_output`-style derived
   signal so a `completed` session that produced nothing is distinguishable from
   one that shipped a diff. Highest alignment with disponent's honesty thesis;
   schema-light.
2. **Dead-stream + idle terminal-condition detectors** (§2.2) — the two
   backend-agnostic detectors from `aid`'s watcher, emitted as `derived` events /
   exit reasons. Detect and report, never auto-reap.
3. **Lease-based worktree lock** (§2.3) — a TOCTOU-resistant ownership lease for
   local worktrees, feeding `reconcile()`. Lands directly on top of PR #21.
4. **Legal-transition status guard** (§2.4) — encode the `SessionState`
   transition graph once and route all ledger writes through it. Cheap, high
   value for a trust-the-ledger library.
5. **One resolved lifecycle policy at dispatch** (§2.5) — keep idle/wall-clock/
   budget limit resolution in one place computed once, before the detectors in §2
   multiply.

Items 1, 2, and 4 are the ones that pay for themselves quickly; 3 pairs naturally
with work already in flight; 5 is a structuring convention to adopt early.
