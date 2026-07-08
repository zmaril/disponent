# disponent — design

*Draft 2, 2026-07-07. One doc: the design, the TypeSpec, and the fluessig changes it forces.
Draft 1 folded in the first grilling round: extraction-from-powdermonkey, SQLite+tmux defaults,
string briefs, role-scoped MCP, reaping, and the shipped catalog. Draft 2 adds templates +
setup (reference-only, declarative repo kept, containers as headroom, secrets stay in
templates/config).*

Disponent dispatches work to coding agents. You hand it a brief and a target
environment; it launches the agent, watches it however that environment allows,
and gives you a typed, queryable record of what happened. Local processes,
exe.dev worker VMs, Claude Code web sessions — one surface over all of them,
from any language.

The name is German: a *Disponent* is the dispatcher in a logistics operation —
the person who decides which truck carries which load, and who tracks every
shipment on the board. Sibling of [fluessig](https://github.com/zmaril/fluessig)
(flüssig), consumer #2 of it — and the reason fluessig moved out of entl.

## 1. What disponent is / is not

**Is:**
- A **library** (Rust core; Node/Python/Ruby via fluessig bindgen — the entl
  pattern) for dispatching one unit of work to one coding agent in one
  environment, and observing it.
- A **session ledger**: typed tables of environments, dispatches, sessions,
  events, artifacts, usage — held in memory and, **by default, mirrored into a
  local SQLite file** through the built-in sink (the entl driver-plan pattern;
  point it at Postgres/DuckDB or switch it to memory-only instead). Generated
  ORM read planes let consumers query their copy with their own tools.
- An **MCP server by default**: the dispatch surface exposed as MCP tools, so
  any MCP client — including a coding agent — can dispatch and monitor agents.

**Is not:**
- A planner. No goals, milestones, phases, task decomposition, or "is the work
  actually done" semantics. Powdermonkey keeps all of that; disponent is the
  layer under it.
- A scheduler. No queues, priorities, retries, concurrency caps, or cron. You
  call `dispatch()`, it dispatches now or errors. Consumers schedule.
- A daemon. Disponent lives inside your process. Durability comes from the
  environments themselves (§3) plus the default SQLite mirror, not from a
  background service.
- A store you administer. The default SQLite mirror is a file disponent manages
  for you; anything fancier is the consumer's choice of sink.

## 2. Position in the family

| project | role |
|---|---|
| fluessig | the schema language + generator both engines are built from |
| entl | the **read** plane: a repo's history as queryable data |
| disponent | the **write** plane: work going out to agents, as queryable data |
| powdermonkey | the opinionated product on top: planning + supervision. **Consumer #1, by extraction**: disponent is pulled *out of* powdermonkey (its `dispatch.ts` / `exe-dev.ts` / session-liveness reconcile are deleted into disponent calls), the same way fluessig was pulled out of entl — the live consumer keeps the library honest, then it grows into the default dispatch library in any language |

The architectural rhyme with entl is deliberate and load-bearing: entl says
*"the store is a derived cache of the repo"*; disponent says *"memory is a
derived cache of the environments"* (§3). Same engine shape, same sink
machinery, same binding story, same docs pipeline.

## 3. The load-bearing idea: environments are the source of truth

A library that holds sessions only in memory has an obvious objection: the
process exits, the sessions are lost. The answer is that **disponent never owned
the sessions in the first place** — the environments did:

- an exe.dev VM keeps running; it is listable (`exe.dev ls`) and reattachable;
- a Claude Code web session keeps running; it is listable via its API;
- a local agent runs inside a **named tmux session** (the powdermonkey trick,
  promoted to a hard requirement of the local backend) — it survives the host
  process and is listable (`tmux ls`) and reattachable.

So disponent's memory is a **reconciled cache over environment reality**. On
`open()` (and on demand via `reconcile()`), disponent probes each configured
environment, re-adopts sessions it recognizes — recognition works because every
resource disponent creates is **labeled with the dispatch id** (tmux session
name, VM name, web-session metadata) — and marks sessions it can no longer find
as `lost`.

What this buys:
- No daemon, no lockfile, no state directory. Two processes can both `open()`
  and see the same session reality.
- Crash-safety for free where the env provides it.

What it costs, honestly:
- The cache rebuilds *state*, not *history* — but history survives restarts
  anyway, because the default SQLite mirror (§9) was writing all along. Run
  memory-only and streamed events are gone; that's the trade you opted into.
- Reconstruction fidelity is capability-graded: an exe.dev VM re-adopts with
  its full tmux scrollback; a web session re-adopts with whatever its API
  reports. The local backend runs agents **in tmux, always** — that is what
  makes local sessions env-reality rather than children of a mortal process;
  no-tmux local dispatch is not a supported mode.

## 4. Concepts and data model

Forge-namespacing lesson from entl applied: the vocabulary is env-generic; env
specifics ride in typed `handle`/`detail` JSON fields, never as columns.

| entity | what it is | key |
|---|---|---|
| `Environment` | somewhere work can run: local, exe.dev, Claude Code web, custom | slug |
| `Template` | a reusable starting state a session boots from: an exe.dev template VM, a container image (§6) | name |
| `Agent` | a coding agent program: claude-code, codex, … | name |
| `Model` | a model an agent can run with | id |
| `Offering` | the availability triple: this env runs this agent with this model | (env, agent, model) |
| `Capability` | one thing an env can do, with detail (see §6) | (env, capability) |
| `Dispatch` | the immutable request: brief + workspace + selection + limits | uuid |
| `Session` | one running instance of a dispatch (1 dispatch : N sessions — retries and resumes make new sessions) | uuid |
| `Event` | one observation on a session's timeline | (session, idx) |
| `Artifact` | something the work produced: branch, PR, patch, file, report | (session, idx) |
| `Usage` | token/cost accounting per session per model, best-effort | (session, model) |

**Dispatch vs Session** — a Dispatch is a value: what was asked, where, with
what limits. A Session is an attempt at it. Retry = new session, same dispatch.
Resume (where the env supports it) = new session linked by `resumedFrom`. The
dispatch is never mutated; all lifecycle lives on sessions.

**The workspace spec** stays dumb on purpose: `repo` (a URL or path), `ref`,
`isolation` (none / worktree / container / vm). Disponent provisions what the
environment's capability says it can (a worktree locally, a clone on a VM);
anything richer — monorepo subpaths, setup scripts — goes in the brief or in
env config, not the schema, until a second consumer demands otherwise.

## 5. Session lifecycle

```
            ┌────────────► cancelled
            │
queued → provisioning → running ⇄ needs_input
            │               │
            │               ├──► completed
            │               ├──► failed
            └───────────────┴──► lost
```

- `queued` exists only as the instant between `dispatch()` returning and the
  backend picking up — there is no queue (§1); it's a state, not a place.
- `needs_input` is the agent parked at a prompt (powdermonkey's `needsInput`,
  promoted into the state machine). Only envs with the `interact` capability
  can leave it via `send()`; on others it's terminal-in-practice and the
  consumer's problem to surface.
- `lost` means reconciliation can no longer find the session in its
  environment. Lost is not failed: a later `reconcile()` may recover it (VM was
  rebooting); consumers decide when lost is dead.
- Terminal states carry `exit`: an env-generic outcome (ok / error / signal /
  timeout / budget) plus raw detail.
- **Nothing reaps itself.** Terminal states are observations, not conclusions:
  the session record and its env resources persist until someone calls
  `reap()` — tear down what the `teardown` capability requires, stamp
  `reapedAt`, archive the row. "Done" is the application developer's judgment
  (powdermonkey's commit-trailer reconciliation, a human's eyeball, CI going
  green); disponent only ever *reports* and *reaps*. A session runs forever
  until somebody reaps it.

## 6. Environments and the capability model

An environment is: a **kind** (which backend code drives it), an **endpoint /
config**, and its **offerings** and **capabilities**. These come from a
**curated catalog shipped inside the library** — a data file regenerated every
release, the same way agent CLIs ship current model lists. Hard-coded and kept
fresh is a release chore, not a probing problem; config extends or overrides it
for `custom` envs. `refresh()` probes **reachability and liveness**, never
existence. `dispatch()` validates the (env, agent, model) triple against the
catalog and refuses unknown combos — `unchecked: true` skips the gate and lets
the env's own error be the report.

Capabilities are a closed enum — a fixed vocabulary disponent's own behavior
branches on — with an open `detail` JSON per row for env-specific texture.
(Open-string capabilities age into a stringly-typed swamp; a closed enum that
grows by release is the fluessig way: change the tsp, regen.)

The v1 capability vocabulary:

| capability | meaning |
|---|---|
| `dispatch` | can start work (everything has this; present for symmetry/probing) |
| `interact` | `send()` can deliver input to a running session |
| `observe_stream` | events arrive as a live feed (PTY/tmux pipe) |
| `observe_poll` | events arrive by polling (API, scrape); detail carries granularity |
| `list_sessions` | env can enumerate its sessions → reconcile can re-adopt |
| `resume` | a finished/lost session can be resumed into a new one |
| `cancel` | can stop a running session |
| `teardown` | env resources need explicit destruction (VMs) and disponent does it |
| `isolation_worktree` / `isolation_container` / `isolation_vm` | which workspace isolations it can provision |
| `templates` | can boot sessions from a named template; detail lists supported kinds (vm_image, container_image) |
| `artifact_fetch` | can retrieve produced files/patches, not just pointers |
| `usage_report` | reports token/cost usage |

The launch matrix, per today's reality (MVP order: **exe.dev first**, local
second; Claude Code web / `claude --remote` are documented targets with no v1
code):

| | exe.dev VM (MVP) | local (tmux) | Claude Code web (future) |
|---|---|---|---|
| observe | poll (ssh/tmux capture) | stream (PTY pipe) | poll (API) |
| interact | yes (tmux send-keys) | yes | no |
| list/reconcile | exe.dev ls | tmux ls | sessions API |
| resume | yes (VM persists) | yes (`claude -r`) | yes (API) |
| teardown | `exe.dev rm` (the orphan-GC lesson) | kill tmux session | n/a |
| isolation | vm | worktree | managed |

Backends are a trait in the core (`EnvBackend`): probe, launch, observe, send,
cancel, list, teardown. `custom` envs implement the same trait out-of-tree (in
Rust v1; a config-driven shell-command backend is the escape hatch worth
considering, since exe.dev support is itself just ssh).

### Templates and setup

A **Template** is a named, reusable starting state a session boots from —
powdermonkey's "copy the already-authed template VM" promoted to a first-class
concept. A template has a **kind** that lowers per backend:

| kind | realized as | v1 |
|---|---|---|
| `vm_image` | an exe.dev template VM, copied per dispatch | yes (the MVP path) |
| `container_image` | a container image, run per dispatch | schema headroom only — no container backend in v1 |

Three rules keep templates simple and safe:

1. **Reference-only.** Disponent never builds templates. You curate them
   out-of-band — install the dev tools, `claude` login, `gh auth`, git
   identity, by hand or by your own scripts — and disponent copies/runs what
   you name. This keeps interactive logins, snapshotting APIs, and staleness
   rebuilds entirely out of scope.
2. **Templates are where credentials live.** The authed template VM is the
   secret store; `DispatchSpec` carries no secrets by construction, so nothing
   sensitive can land in the ledger or any sink. (Env config may carry
   endpoint addresses and key *paths* — never key material in the schema.)
3. **Setup is a script, run in order.** Provisioning executes, in sequence:
   the template's baseline `setup` (tool-level: extra installs, config) → the
   **declarative repo clone** from `repo`/`ref` (disponent does this uniformly,
   and the ledger stays queryable by repo) → the dispatch's own `setup`
   (task-level: pull extra repos, install deps, seed data) → the agent starts.
   Setup output streams as `log` events during `provisioning`; a failing setup
   fails the session with `exitReason: setup` — and per §5, nothing reaps
   itself, so the half-provisioned VM sticks around for debugging until reaped.

## 7. Monitoring: events, fidelity, honesty

"We monitor the agents' progress how we can." The event model is built around
that *how we can*:

- **One normalized vocabulary** (`state`, `message`, `tool_call`, `tool_result`,
  `log`, `usage`, `artifact`, `raw`) — payloads are a **tagged union** (a new
  fluessig capability, §13.1).
- **Every event carries `fidelity`**: `exact` (parsed from a structured
  transcript), `derived` (inferred — e.g. "idle after output" → needs_input),
  `scraped` (tmux capture heuristics). Consumers can filter to what they trust.
- **`raw` passthrough**: whatever the env emitted that didn't normalize, kept
  verbatim. Normalization must be lossy-with-a-receipt, never lossy-silently.
- **No fabrication**: an env that only supports coarse polling produces few
  events. Disponent does not interpolate. (Powdermonkey's deeper principle —
  progress is what lands on main, never self-reporting — stays in powdermonkey;
  disponent reports observation, consumers decide truth.)

Observation is a poll loop in the core (`@stream events()` — fluessig's stream
shape is poll-based, which is exactly right); bindings turn it into async
iterators/callbacks, same as entl's `changes()`.

## 8. The op surface

See the TypeSpec (§12) for the full signatures; the shape:

- `open(options?)` @ctor — configure environments (config object / file / env).
- `environments()`, `refresh(env?)` — discovery; refresh re-probes offerings +
  capabilities.
- `dispatch(spec) → Session` — the point of the library.
- `session(id)`, `sessions(filter?)` — the ledger.
- `events(options?)` @stream — the merged, filterable event feed.
- `send(sessionId, input)` — capability-gated interactivity.
- `cancel(sessionId)`, `resume(sessionId) → Session`.
- `reap(sessionId) → Session` — tear down env resources, stamp `reapedAt`,
  archive (§5).
- `reconcile() → ReconcileReport` — re-adopt env reality (§3).
- `driverPlan(options?)` @stream — the built-in sync: dialect-agnostic upsert
  statements for every table, exactly entl's sink mechanism.
- `wait(sessionId, timeoutSecs)` @manual — blocking wait, hand-written per
  binding (GVL/event-loop specifics, like entl's `watch`).
- `serveMcp(options?)` @manual — start the MCP server over this instance.

## 9. Storage and sync

Identical machinery to entl, on purpose:

- The core holds the tables in memory, shaped by the generated schema module.
- **The default sink is SQLite**: `open()` starts the mirror at a managed local
  path unless configured otherwise (`sink = "none"` for memory-only, or any
  dialect/DSN the driver plan speaks). Zero-config durability; the ledger file
  is disponent's to manage.
- `driverPlan()` streams upsert/delete statements for any dialect fluessig
  speaks (Postgres/SQLite/DuckDB) — consumers mirror into their store with the
  same thin executors entl's sinks use (`sync.ts` over PGlite, etc.).
- The generated ORM read planes (SQLAlchemy models, Drizzle schema, typed table
  enums) ship with each binding, so "store it how you like" comes with types.
- Secrets (env credentials, tokens) live in **config, not in the schema** —
  there is nothing to accidentally sync. `Environment.endpoint` is an address,
  never a credential.

## 10. MCP, on by default

The op surface projects to MCP tools — `disponent_dispatch`,
`disponent_sessions`, `disponent_events`, `disponent_send`, `disponent_cancel`,
`disponent_reap`, … — generated from the same api layer that generates the
bindings (§13.2). `serveMcp()` starts a stdio (v1) server over the already-open
instance. **stdio is also the remote transport**: an MCP client configured with
`command: ssh, args: [<host>, disponent, mcp]` reaches a disponent running on
any machine ssh reaches — no HTTP server, no auth story beyond ssh's, in the
house style where the exe.dev CLI is itself just ssh. This is exactly how the
MVP supervisor connects (§14).

Recursive delegation is the point — **for the right principals**. The rule:
humans and supervisor agents dispatch; dispatched coding agents do not. The MCP
surface is **role-scoped**:

- **supervisor** (the default for a server you start yourself): the full
  surface — dispatch, send, cancel, resume, reap, observe. A configured env is
  a consented env; supervisors reach all of them out of the box.
- **worker** (what a dispatched agent gets, when its env wires disponent's MCP
  into it at all): observation only — `session`, `sessions`, `events`. No
  dispatch, no send, no cancel, no reap. Workers are leaf nodes by
  construction.

Enforcement is which server instance the agent can reach (the worker's env gets
a worker-role endpoint or nothing), not agent good behavior. `viaMcpDepth`
stays on the dispatch record as the audit trail and a belt-and-braces ceiling,
but the role is the actual gate.

Stream ops don't map to MCP's request/response tools: `events` projects as a
cursor tool (`disponent_events(after, limit)`) — the generator handles the
stream→cursor lowering (§13.2).

## 11. Runtime architecture

- **disponent-core stays synchronous** (the entl rule) — with concurrency
  *inside*: the **observer pool** (§13.5). One supervised thread per active
  session does that backend's slow I/O (ssh scrape, PTY read, HTTP poll) and
  funnels normalized events into a single bounded channel; `events()` drains
  it. The public surface stays poll-shaped; a slow exe.dev scrape stalls only
  its own session's observer, never the feed. Head-of-line blocking solved
  without tokio in the core.
- **Bindings via fluessig bindgen** — node/python/ruby generated surfaces over
  a hand-written `core_impl`, byte-for-byte the entl pattern. A CLI
  (`disponent dispatch / sessions / events / mcp`) comes with the core for
  shell users and as the ssh-stdio MCP entrypoint (§10); the CLI is a consumer,
  not the product.
- **One instance, one process**; multiple instances tolerate each other because
  the envs are the truth and resources are labeled (§3). No cross-process
  locking in v1.

## 12. The TypeSpec (disponent.tsp, draft)

Everything below compiles against fluessig today **except** the two marked
constructs (`union EventPayload`, the `@readonly`/`@destructive` op hints),
which are the required fluessig changes of §13.

```typespec
import "@fluessig/typespec";
using Fluessig;

// ── scalars ──
/** Disponent-minted identifier (UUIDv7). */
scalar DispatchId extends string;
scalar SessionUid extends string;
/** Money in integer cents, USD. */
scalar Cents extends int64;

// ── enums (wire values are the stored strings) ──
enum EnvKind { local, exe_dev, claude_code_web, custom }
enum SessionState {
  queued, provisioning, running, needs_input,
  completed, failed, cancelled, lost,
}
enum ExitReason { ok, error, signal, timeout, budget, setup, unknown }
enum IsolationKind { none, worktree, container, vm }
enum TemplateKind { vm_image, container_image }
enum CapabilityKind {
  dispatch, interact, observe_stream, observe_poll, list_sessions,
  resume, cancel, teardown, isolation_worktree, isolation_container,
  isolation_vm, templates, artifact_fetch, usage_report,
}
enum EventKind { state, message, tool_call, tool_result, log, usage, artifact, raw }
enum Fidelity { exact, derived, scraped }
enum ArtifactKind { branch, pull_request, patch, file, report, url }

// ── the environment side ──
/** Somewhere work can run. Config supplies these; probing fills offerings + capabilities. */
@entity @name("environments")
model Environment {
  @key slug: string;
  kind: EnvKind;
  displayName?: string;
  /** Address only — never a credential (secrets live in config). */
  endpoint?: url;
  /** Availability triples — which agent×model combos this env runs. */
  @name("offerings") offerings: Offering[];
  /** What this env can do (closed vocabulary; open detail). */
  @name("env_capabilities") @edge(CapabilityDetail) capabilities: Capability[];
  lastProbedAt?: utcDateTime;
}

/** A reusable starting state, curated out-of-band (auth baked in by hand).
 * Reference-only: disponent copies/runs what you name, never builds (§6). */
@entity @name("templates")
model Template {
  @key name: string;
  kind: TemplateKind;
  /** exe.dev template VM name, container image ref, … */
  locator: string;
  /** Baseline setup script — runs first, before the repo clone and the dispatch's setup. */
  setup?: string;
  note?: string;
}

/** A coding agent program (claude-code, codex, …). */
@entity @name("agents")
model Agent {
  @key name: string;
  version?: string;
}

/** A model an agent can run with. */
@entity @name("models")
model Model {
  @key id: string;          // e.g. "claude-opus-4-8"
  provider?: string;
  family?: string;
}

/** env × agent × model availability. */
@entity @name("offerings")
model Offering {
  @key @fk(#["env_slug"]) env: Environment;
  @key @fk(#["agent_name"]) agent: Agent;
  @key @fk(#["model_id"]) model: Model;
  isDefault: boolean = false;
}

/** One capability row (the edge target of Environment.capabilities). */
@entity @name("capabilities")
model Capability {
  @key capability: CapabilityKind;
}
/** Edge properties on env_capabilities. */
model CapabilityDetail {
  /** Env-specific texture: poll granularity, isolation limits, … */
  detail?: Json;
}

// ── the work side ──
/** The immutable request. Never mutated after dispatch(); lifecycle lives on sessions. */
@entity @name("dispatches")
model Dispatch {
  @key id: DispatchId;
  createdAt: utcDateTime;
  title?: string;
  /** The brief — the whole task spec, free-form. Structure belongs to consumers. */
  brief: string;
  /** Workspace: URL or local path; empty = no repo (pure-prompt work). */
  repo?: string;
  ref?: string;
  isolation: IsolationKind = "none";
  @fk(#["template_name"]) template?: Template;
  /** Per-dispatch setup script — runs after the template's setup and the repo clone. */
  setup?: string;
  @fk(#["env_slug"]) env: Environment;
  @fk(#["agent_name"]) agent: Agent;
  @fk(#["model_id"]) model?: Model;
  timeoutSecs?: int32;
  maxBudget?: Cents;
  /** MCP recursion depth: 0 = dispatched by the host program. */
  viaMcpDepth: int32 = 0;
  /** Consumer labels, opaque to disponent. */
  labels?: Json;
  /** Attempts, oldest first. */
  @compose @name("sessions") sessions: Session[];
}

/** One attempt at a dispatch, mirroring one env-side resource. */
@entity @name("sessions")
model Session {
  @key uid: SessionUid;
  @fk(#["dispatch_id"]) dispatch: Dispatch;
  state: SessionState;
  /** The env's own handle(s): tmux session name, VM name, web session id/url. */
  envHandle?: Json;
  /** Human-facing view URL when the env has one (ttyd, web session page). */
  url?: url;
  @fk(#["resumed_from"]) resumedFrom?: Session;
  startedAt?: utcDateTime;
  endedAt?: utcDateTime;
  exitReason?: ExitReason;
  exitDetail?: string;
  /** Set by reap(): resources torn down, row archived. Null = still on the board. */
  reapedAt?: utcDateTime;
  @compose @name("session_events") @edge(EventAt) events: Event[];
  @compose @name("artifacts") artifacts: Artifact[];
  @compose @name("usage") usage: Usage[];
}

/** One observation. Payload is a tagged union — NEEDS fluessig §13.1. */
@entity @name("events")
model Event {
  ts: utcDateTime;
  kind: EventKind;
  fidelity: Fidelity;
  payload: EventPayload;
}
/** Edge: position on the session timeline. */
model EventAt {
  @key idx: int64;
}

// —— NEEDS fluessig §13.1: tagged unions ——
union EventPayload {
  state: StateChange,
  message: AgentMessage,
  toolCall: ToolCallInfo,
  toolResult: ToolResultInfo,
  log: LogLine,
  usage: UsageDelta,
  artifact: ArtifactRef,
  raw: RawObservation,
}
model StateChange { from: SessionState; to: SessionState; }
model AgentMessage { role: string; text: string; }
model ToolCallInfo { tool: string; input?: Json; }
model ToolResultInfo { tool: string; ok: boolean; output?: string; }
model LogLine { line: string; }
model UsageDelta { modelId?: string; inputTokens?: int64; outputTokens?: int64; costCents?: Cents; }
model ArtifactRef { artifactIdx: int64; }
model RawObservation { source: string; data: Json; }

/** Something the session produced. */
@entity @name("artifacts")
model Artifact {
  @key @fk(#["session_uid"]) session: Session;
  @key idx: int64;
  kind: ArtifactKind;
  /** Branch name, PR URL, path, … — a pointer, not the bytes. */
  locator: string;
  meta?: Json;
}

/** Best-effort accounting, per session per model. */
@entity @name("usage")
model Usage {
  @key @fk(#["session_uid"]) session: Session;
  @key @fk(#["model_id"]) model: Model;
  inputTokens: int64 = 0;
  outputTokens: int64 = 0;
  costCents: Cents = 0;
}

// ── DTOs (op-surface value structs) ──
model OpenOptions {
  configPath?: string;
  /** Default: a managed local SQLite file. "none" = memory-only; any driver-plan DSN otherwise. */
  sink?: string;
}
model DispatchSpec {
  brief: string;
  env: string;
  agent?: string;          // default: env's default offering
  model?: string;
  title?: string;
  repo?: string;
  ref?: string;
  isolation?: IsolationKind;
  /** Named template to boot from (§6). */
  template?: string;
  /** Per-dispatch setup script. No secrets — those live in the template. */
  setup?: string;
  timeoutSecs?: int32;
  maxBudget?: Cents;
  /** Skip catalog validation of (env, agent, model); the env's own error becomes the report. */
  unchecked?: boolean;
  labels?: Json;
}
model SessionFilter { env?: string; state?: SessionState; dispatchId?: DispatchId; }
model EventOptions { sessionUid?: SessionUid; afterIdx?: int64; kinds?: EventKind[]; }
model ReconcileReport {
  adopted: int32; confirmed: int32; lost: int32; tornDown: int32;
}
enum McpRole { supervisor, worker }
model McpOptions {
  /** Transport: "stdio" (v1). */
  transport?: string;
  /** Surface scope: supervisor = full; worker = observe-only (§10). Default supervisor. */
  role?: McpRole;
  /** Belt-and-braces viaMcpDepth ceiling (default 1). */
  maxDepth?: int32;
}
model DriverPlanOptions { dialect?: string; tables?: string[]; }
model Statement { sql: string; params: Json; }

// ── the op surface ──
interface Disponent {
  @ctor open(options?: OpenOptions): void;

  // —— NEEDS fluessig §13.2: @readonly/@destructive → MCP tool annotations ——
  @readonly environments(): Environment[];
  refresh(envSlug?: string): Environment[];

  dispatch(spec: DispatchSpec): Session;
  @readonly session(uid: SessionUid): Session | null;
  @readonly sessions(filter?: SessionFilter): Session[];
  @stream events(options?: EventOptions): Event;

  send(sessionUid: SessionUid, input: string): void;
  @destructive cancel(sessionUid: SessionUid): Session;
  resume(sessionUid: SessionUid): Session;
  @destructive reap(sessionUid: SessionUid): Session;

  reconcile(): ReconcileReport;
  @stream driverPlan(options?: DriverPlanOptions): Statement;

  /** Blocking wait — hand-written per binding (event-loop/GVL specifics). */
  @manual wait(sessionUid: SessionUid, timeoutSecs: int32): Session;
  /** Long-running MCP server over this instance — hand-written per binding. */
  @manual serveMcp(options?: McpOptions): void;
}
```

## 13. Required fluessig changes

Ordered by how much they block; each is generally useful, none is
disponent-shaped-only. This is the intentional part: disponent exists partly to
force fluessig to grow past its first consumer.

**13.1 Tagged unions (blocks the event model).** Today the emitter lowers
unions in Layer A but the api layer rejects everything except `T | null`, and
no codec handles them. Needed: TypeSpec named unions → `catalog.json`/
`api.json` as `{union: {name, variants: {tag → type}}}` → every projection:
Rust enums (serde-tagged), TS discriminated unions, Python tagged types, SQL as
`(kind, payload json)` twin columns, JSON Schema `oneOf` + discriminator for
MCP. Scope: emitter passthrough (small), loader/IR (moderate), five backends
(the bulk). This is v1-blocking for disponent.

**13.2 The MCP projection (the point of "MCP by default").** A new fluessig-gen
backend: api layer → an MCP tool manifest (tool per op, JSON Schema per DTO,
docs from the tsp doc comments — the same porting rule as everywhere else) + a
generated Rust dispatch shim over the core trait, so a tiny embedded server
crate (stdio v1) serves any fluessig-described engine. Two new op decorators,
`@readonly` and `@destructive`, flow into MCP tool annotations (`readOnlyHint`,
`destructiveHint`) — and are useful documentation on the existing bindings too.
Lowering rules: `@ctor` doesn't project (the server holds the open instance);
`@stream` projects as a cursor tool (`after`/`limit` params, the poll shape is
already cursor-friendly); `@manual` doesn't project. entl gets this for free:
`entl serve-mcp` exposing `query`/`tables`/`file_at` is an obvious follow-up
and the proof the projection isn't disponent-shaped.

**13.3 Sink executor — RESOLVED, no lift needed.** On inspection the generic
half already lives in fluessig: `fluessig::data::SqlCodec` (Mutation/
Transaction → a topologically-ordered Plan of `{sql, params}` steps, per
dialect, executes nothing) is the whole contract. entl's `driver.rs` turned
out to be entl-specific plumbing on top — the Arrow `ChangeBatch` adapter and
entl's `Sink` vocabulary — which would be an invented abstraction to lift, not
an extracted one. Disponent assembles `Transaction`s from its in-memory rows →
`SqlCodec::plan()` → the same thin per-language executors entl's sinks use.
(fluessig#5 closed with this rationale.)

**13.4 (small) `Json` as a first-class fluessig scalar.** entl.tsp already
declares it locally; promote it into `@fluessig/typespec` so every catalog
stops re-declaring it.

**13.5 The observer pool (runtime, shared with entl).** The §11 concurrency
answer — sync public API, supervised observer threads inside, one mpsc funnel,
`@stream` ops drain the channel — is not disponent-shaped either. It is the
generic runtime for "N slow sources → one poll-shaped stream," and it belongs
in fluessig next to `data.rs` (fluessig's charter already includes the
runtime: *language + generator + data-marshalling runtime*). Shape: a
`fluessig::observe` module — spawn/adopt/reap observers keyed by subject id,
panic isolation per observer, a bounded mpsc, and a drain call that IS the
generated stream shape. Disponent consumes it for sessions; **entl consumes it
the day `watch` grows from one repo to a fleet of repos** — same pool, subjects
are repos instead of sessions. Build it in fluessig from the start so the
second consumer is a config change, not a port.

## 14. MVP, then v1

**The MVP is one topology, run for real:**

```
 laptop                              exe.dev
┌─────────────────────┐            ┌──────────────────────────────┐
│ Claude Code         │  MCP over  │ VM: disponent mcp            │
│ (the supervisor)    │──ssh stdio─│  (in tmux; SQLite ledger)    │
└─────────────────────┘            │        │ exe_dev backend     │
                                   │        ▼                     │
                                   │ VM: claude in tmux ── work   │
                                   │ VM: claude in tmux ── work   │
                                   └──────────────────────────────┘
```

Disponent runs **on an exe.dev VM** (inside tmux — the system is self-similar:
disponent's own process is exactly the kind of resource it manages). The
supervisor is a **local Claude Code session** reaching it as a stdio MCP server
**over ssh** — `command: ssh, args: [<vm>, disponent, mcp]` — which is why v1
needs no HTTP transport at all: ssh *is* the remote transport, the same way the
exe.dev CLI is just ssh. Dispatch provisions **sibling exe.dev VMs**, each
running a fresh Claude instance in tmux (the powdermonkey provisioning recipe,
verbatim). VM→control-plane provisioning is **verified working** — the
dispatching VM just needs an exe.dev key added to it. The board survives the
laptop closing: the ledger, the sessions, and disponent itself all live in the
fleet.

The MVP exercises, end to end: the exe.dev backend, role-scoped MCP
(supervisor dispatching, workers as leaf nodes), reconcile (restart disponent's
VM and re-adopt the fleet), reap (VM teardown — the orphan-GC lesson), and the
SQLite ledger. **Claude Code web and `claude --remote` are out entirely for
now** — documented as future backends, no code.

**v1 around it:**
In: **extraction against powdermonkey as consumer #1** (its `dispatch.ts` /
`exe-dev.ts` / session-liveness reconcile delete into disponent calls — the
scope ruler for everything else); exe.dev + local(tmux) backends; dispatch /
sessions / events / send / cancel / resume / reap / reconcile; SQLite-by-default
ledger + driverPlan sync; the shipped catalog; string briefs; node binding +
CLI; role-scoped MCP stdio server (supervisor/worker); fluessig 13.1–13.2 +
13.5 (13.3 if the lift is clean).
Out (deliberately): claude-code-web and `claude --remote` backends,
a container backend (the template schema is ready for it; no runtime),
template *building* (reference-only, §6), scheduling/queueing, multi-agent
workflows, structured briefs, artifact *content* fetching (pointers only),
HTTP MCP transport (ssh-stdio covers remote), python/ruby bindings (the
generator makes them cheap to add once the surface settles), any cross-process
coordination beyond env-reality reconciliation.

## 15. Open questions

Settled in draft 1 (recorded in place above): tmux is a hard requirement of the
local backend; SQLite is the default sink; briefs are strings; the catalog is
shipped + curated; MCP is role-scoped (supervisors dispatch, workers observe);
sessions run until reaped; monitoring collects whatever each env allows and
evolves. Settled in draft 2: templates are reference-only (curated out-of-band,
credentials live there, never in DispatchSpec); declarative repo/ref stays
alongside setup scripts; containers are schema headroom, not a v1 backend.

Still open:

1. Is `needs_input` detection (idle-after-output heuristics) core or backend?
2. Does `resume` belong on Dispatch (new session under same dispatch) — chosen
   here — or is a resumed session a *new dispatch* with provenance?
3. Cursor semantics for `events` over MCP when memory was rebuilt (§3): with
   the SQLite mirror on (the default), `idx` continuity comes from the mirror;
   memory-only mode still restarts `idx`. Accept, or add a sequence epoch?
4. Should the capability vocabulary include `budget_enforce` (env can hard-stop
   on spend) vs disponent-side soft limits only?
5. How does a `custom` env ship? Rust trait only, or a declarative
   shell-command backend (probe/launch/observe as configured argv templates)?
6. The default SQLite path: per-project (`.disponent/ledger.db`, the
   powdermonkey shape) or per-user (XDG state dir)? Per-project fits the
   repo-centric workflow; per-user fits "one board for everything I've got
   running."
