// disponent.tsp — the disponent catalog in fluessig's language (notes/design.md §12).
// The schema's single source of truth: entities (the session ledger), the tagged
// event union, and the op surface that becomes the bindings and the MCP tools.
// The decorator library resolves relative to a staging dir at emit time —
// scripts/gen.sh symlinks fluessig's typespec/ next to a copy of this file
// (the entl convention; keeps the tsp identical wherever it compiles).
import "./typespec/lib.tsp";
using Fluessig;

// ── scalars ──
/** Disponent-minted identifier (UUIDv7). */
scalar DispatchId extends string;
scalar SessionUid extends string;
/** Money in integer cents, USD. */
scalar Cents extends int64;

// ── enums (wire values are the stored strings) ──
enum EnvKind {
  local,
  exe_dev,
  claude_code_web,
  custom,
}
enum SessionState {
  queued,
  provisioning,
  running,
  needs_input,
  completed,
  failed,
  cancelled,
  lost,
}
enum ExitReason {
  ok,
  error,
  signal,
  timeout,
  budget,
  setup,
  `unknown`, // reserved word in TypeSpec; backticked, wire value stays "unknown"
}
enum IsolationKind {
  none,
  worktree,
  container,
  vm,
}
enum TemplateKind {
  vm_image,
  container_image,
}
enum CapabilityKind {
  dispatch,
  interact,
  observe_stream,
  observe_poll,
  list_sessions,
  resume,
  cancel,
  teardown,
  isolation_worktree,
  isolation_container,
  isolation_vm,
  templates,
  artifact_fetch,
  usage_report,
}
enum EventKind {
  state,
  message,
  tool_call,
  tool_result,
  log,
  usage,
  artifact,
  raw,
}
enum Fidelity {
  exact,
  derived,
  scraped,
}
enum ArtifactKind {
  branch,
  pull_request,
  patch,
  file,
  report,
  url,
}
enum McpRole {
  supervisor,
  worker,
}

// ── the environment side ──
/** Somewhere work can run. Config supplies these; the shipped catalog fills offerings + capabilities. */
@entity
@name("environments")
model Environment {
  @key slug: string;
  kind: EnvKind;
  displayName?: string;

  /** Address only — never a credential (secrets live in config/templates). */
  endpoint?: url;

  /** What this env can do (closed vocabulary; open detail). */
  @name("env_capabilities")
  @edge(CapabilityDetail)
  capabilities: Capability[];

  lastProbedAt?: utcDateTime;
}

/** A reusable starting state, curated out-of-band (auth baked in by hand).
 * Reference-only: disponent copies/runs what you name, never builds. */
@entity
@name("templates")
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
@entity
@name("agents")
model Agent {
  @key name: string;
  version?: string;
}

/** A model an agent can run with. */
@entity
@name("models")
model AgentModel {
  @key id: string; // e.g. "claude-opus-4-8"
  provider?: string;
  family?: string;
}

/** env × agent × model availability (from the shipped catalog + config). */
@entity
@name("offerings")
model Offering {
  @key @fk(#["env_slug"]) env: Environment;
  @key @fk(#["agent_name"]) agent: Agent;
  @key @fk(#["model_id"]) `model`: AgentModel;
  isDefault: boolean = false;
}

/** One capability row (the edge target of Environment.capabilities). */
@entity
@name("capabilities")
model Capability {
  @key capability: CapabilityKind;
}
/** Edge properties on env_capabilities. */
model CapabilityDetail {
  /** Env-specific texture: poll granularity, supported template kinds, … */
  detail?: Json;
}

// ── the work side ──
/** The immutable request. Never mutated after dispatch(); lifecycle lives on sessions. */
@entity
@name("dispatches")
model Dispatch {
  @key id: DispatchId;
  createdAt: utcDateTime;
  title?: string;

  /** The brief — the whole task spec, free-form. Structure belongs to consumers. */
  brief: string;

  /** Workspace: URL or local path; empty = no repo (pure-prompt work). */
  repo?: string;

  gitRef?: string;
  isolation: IsolationKind = IsolationKind.none;

  @fk(#["template_name"]) template?: Template;

  /** Per-dispatch setup script — runs after the template's setup and the repo clone. */
  setup?: string;

  @fk(#["env_slug"]) env: Environment;
  @fk(#["agent_name"]) agent: Agent;
  @fk(#["model_id"]) `model`?: AgentModel;
  timeoutSecs?: int32;
  maxBudget?: Cents;

  /** MCP recursion depth: 0 = dispatched by the host program. */
  viaMcpDepth: int32 = 0;

  /** Consumer labels, opaque to disponent. */
  labels?: Json;
}

/** One attempt at a dispatch, mirroring one env-side resource. */
@entity
@name("sessions")
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
}

/** One observation on a session's timeline (containment rides the key: the
 * session FK + the position, like Artifact/Usage — to-manys are queries). */
@entity
@name("events")
model Event {
  @key @fk(#["session_uid"]) session: Session;
  @key idx: int64;
  ts: utcDateTime;
  kind: EventKind;
  fidelity: Fidelity;
  payload: EventPayload;
}

/** One observation's body — the union's variant tag is the wire discriminator. */
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
model StateChange {
  from: SessionState;
  to: SessionState;
}
model AgentMessage {
  role: string;
  text: string;
}
model ToolCallInfo {
  tool: string;
  input?: Json;
}
model ToolResultInfo {
  tool: string;
  ok: boolean;
  output?: string;
}
model LogLine {
  line: string;
}
model UsageDelta {
  modelId?: string;
  inputTokens?: int64;
  outputTokens?: int64;
  costCents?: Cents;
}
model ArtifactRef {
  artifactIdx: int64;
}
model RawObservation {
  source: string;
  data: Json;
}

/** Something the session produced. */
@entity
@name("artifacts")
model Artifact {
  @key @fk(#["session_uid"]) session: Session;
  @key idx: int64;
  kind: ArtifactKind;

  /** Branch name, PR URL, path, … — a pointer, not the bytes. */
  locator: string;

  meta?: Json;
}

/** Best-effort accounting, per session per model. */
@entity
@name("usage")
model Usage {
  @key @fk(#["session_uid"]) session: Session;
  @key @fk(#["model_id"]) `model`: AgentModel;
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
  agent?: string; // default: env's default offering
  `model`?: string;
  title?: string;
  repo?: string;
  gitRef?: string;
  isolation?: IsolationKind;

  /** Named template to boot from. */
  template?: string;

  /** Per-dispatch setup script. No secrets — those live in the template. */
  setup?: string;

  timeoutSecs?: int32;
  maxBudget?: Cents;

  /** Skip catalog validation of (env, agent, model); the env's own error becomes the report. */
  unchecked?: boolean;

  labels?: Json;
}
model SessionFilter {
  env?: string;
  state?: SessionState;
  dispatchId?: DispatchId;
}

/** An editor link into a session's working directory. */
model WorkspaceLink {
  /** The session this link addresses. */
  sessionUid: SessionUid;
  /** True when an honest editor link exists for this session's backend. */
  available: boolean;
  /** A deep link into the working dir: vscode://file/… for local backends, or vscode://vscode-remote/ssh-remote+… for remote ssh backends (exe.dev). Absent when unavailable. */
  url?: url;
  /** When available=false, an honest explanation of why (capability edge). */
  detail?: string;
}
model EventOptions {
  sessionUid?: SessionUid;
  afterIdx?: int64;
  kinds?: EventKind[];
}
model ReconcileReport {
  adopted: int32;
  confirmed: int32;
  lost: int32;
  tornDown: int32;
}
model McpOptions {
  /** Transport: "stdio" (v1). */
  transport?: string;

  /** Surface scope: supervisor = full; worker = observe-only. Default supervisor. */
  role?: McpRole;

  /** Belt-and-braces viaMcpDepth ceiling (default 1). */
  maxDepth?: int32;
}
model DriverPlanOptions {
  dialect?: string;
  tables?: string[];
}
model Statement {
  sql: string;
  params: Json;
}

// ── the op surface ──
interface Disponent {
  @ctor open(options?: OpenOptions): void;

  @readonly environments(): Environment[];
  refresh(envSlug?: string): Environment[];

  /** The offerings table: every env × agent × model the catalog knows, each
   * flagged `isDefault` when it's the pick for a dispatch that names only the
   * environment. Lets a consumer enumerate what can run where without reaching
   * for the raw driver plan. */
  @readonly offerings(): Offering[];

  dispatch(spec: DispatchSpec): Session;
  @readonly session(uid: SessionUid): Session | null;
  @readonly sessions(filter?: SessionFilter): Session[];

  /** Return an editor link (VS Code deep link) into the session's working directory, when the backend can honestly provide one. */
  @readonly workspaceLink(sessionUid: SessionUid): WorkspaceLink;
  @readonly @stream events(options?: EventOptions): Event;

  send(sessionUid: SessionUid, input: string): void;
  @destructive cancel(sessionUid: SessionUid): Session;
  resume(sessionUid: SessionUid): Session;
  @destructive reap(sessionUid: SessionUid): Session;

  reconcile(): ReconcileReport;
  @readonly @stream driverPlan(options?: DriverPlanOptions): Statement;

  /** Blocking wait — hand-written per binding (event-loop/GVL specifics). */
  @manual wait(sessionUid: SessionUid, timeoutSecs: int32): Session;

  /** Long-running MCP server over this instance — hand-written per binding. */
  @manual serveMcp(options?: McpOptions): void;
}
