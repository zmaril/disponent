// straitjacket-allow-file:duplication — a full session-ledger schema is inherently
// repetitive: the union variant bodies (StateChange / AgentMessage / …) and the
// op-surface DTOs share small scalar field blocks BY DESIGN. These are DISTINCT
// value structs with distinct fields, not a copy that wants a helper.
//! disponent-schema — disponent's COMPLETE schema (12 entities, the
//! `env_capabilities` edge, the tagged `EventPayload` union, its observe-only /
//! destructive MCP op hints, and the full op surface), authored with the
//! `fluessig` Rust derive front end. This is disponent's single source of truth
//! for the data model and op surface, replacing the former `schema/disponent.tsp`.
//!
//! `scripts/gen.sh` runs the two emit bins over this crate to produce
//! `schema/{catalog,api}.json`, then hands them to `fluessig-gen`, which
//! regenerates `schema_gen.rs`, `mcp_generated.rs`, the schema docs, and the three
//! binding surfaces. Don't edit those generated files by hand — edit this crate and
//! run `scripts/gen.sh`.
//!
//! Notable front-end features this schema exercises:
//!
//! * **`#[derive(Union)]` + `catalog!{ unions: [...] }`** — disponent's
//!   `union EventPayload` (nine variants: state / message / toolCall / toolResult /
//!   log / usage / artifact / raw / mail). A field typed by the union
//!   (`Event.payload`) lowers to `TypeRef::Union` (twin `payload_kind` + `payload`
//!   columns) and, when an op transitively references it, `api.json`'s `unions`.
//! * **`#[fluessig(readonly)]`** — the nine observe-only ops (`environments`,
//!   `offerings`, `capabilities`, `session`, `sessions`, `workspaceLink`, `events`,
//!   `messages`, `driverPlan`) → `api.json` `"readonly": true` → the MCP
//!   `readOnlyHint`. Composes with the op kind (`events` / `driverPlan` are
//!   `@readonly @stream`).
//! * **`#[fluessig(destructive)]`** — `cancel` / `reap` → `"destructive": true` →
//!   the MCP `destructiveHint`.

use fluessig_derive::{catalog, export, Edge, Entity, Enum, Id, Record, Scalar, Union};

// ═════════════════════════════════════════════════════════════════════════════
// Stock-type markers — zero-dep stand-ins the derive maps to built-in scalars
// (the entl-fixture convention). The derive reads *types* (tokens), never values.
// ═════════════════════════════════════════════════════════════════════════════

/// Stand-in for `chrono::DateTime<Utc>` — the derive maps `DateTime<_>` to the
/// `utcDateTime` scalar.
pub struct DateTime<Tz>(core::marker::PhantomData<Tz>);
/// The `Utc` timezone marker (only its name matters to the derive).
pub struct Utc;
/// The stock `Json` scalar (base `string`) — `Session.envHandle`, `RawObservation.data`, …
pub struct Json;
/// The stock `url` scalar (base `string`) — `Environment.endpoint`, `Session.url`,
/// `WorkspaceLink.url`. A lowercase marker so its name lowers verbatim to `url`.
#[allow(non_camel_case_types)]
pub struct url;

// ═════════════════════════════════════════════════════════════════════════════
// Scalars — disponent's minted ids + money. DispatchId/…/FanoutId refine `string`;
// `Cents` refines `int64` (which itself roots at `numeric` — the field-usage base).
// ═════════════════════════════════════════════════════════════════════════════

/// Disponent-minted identifier (UUIDv7).
#[derive(Scalar)]
#[fluessig(extends = "string")]
pub struct DispatchId(pub String);
#[derive(Scalar)]
#[fluessig(extends = "string")]
pub struct SessionUid(pub String);
/// Disponent-minted message id (UUIDv7).
#[derive(Scalar)]
#[fluessig(extends = "string")]
pub struct MessageId(pub String);
/// Disponent-minted fan-out id (UUIDv7): one broadcast, shared by its N Messages.
#[derive(Scalar)]
#[fluessig(extends = "string")]
pub struct FanoutId(pub String);
/// Money in integer cents, USD.
#[derive(Scalar)]
#[fluessig(extends = "int64")]
pub struct Cents(pub i64);

// ═════════════════════════════════════════════════════════════════════════════
// Enums — wire values are the (snake_case) stored member names.
// ═════════════════════════════════════════════════════════════════════════════

#[derive(Enum, Clone, Copy)]
#[fluessig(rename_all = "snake_case")]
pub enum EnvKind {
    Local,
    ExeDev,
    Modal,
    ClaudeCodeWeb,
    Custom,
}

#[derive(Enum, Clone, Copy)]
#[fluessig(rename_all = "snake_case")]
pub enum SessionState {
    Queued,
    Provisioning,
    Running,
    NeedsInput,
    Completed,
    Failed,
    Cancelled,
    Lost,
}

#[derive(Enum, Clone, Copy)]
#[fluessig(rename_all = "snake_case")]
pub enum ExitReason {
    Ok,
    Error,
    Signal,
    Timeout,
    Budget,
    Setup,
    Unknown,
}

#[derive(Enum, Clone, Copy)]
#[fluessig(rename_all = "snake_case")]
pub enum IsolationKind {
    None,
    Worktree,
    Container,
    Vm,
}

#[derive(Enum, Clone, Copy)]
#[fluessig(rename_all = "snake_case")]
pub enum TemplateKind {
    VmImage,
    ContainerImage,
}

#[derive(Enum, Clone, Copy)]
#[fluessig(rename_all = "snake_case")]
pub enum CapabilityKind {
    Dispatch,
    Interact,
    ObserveStream,
    ObservePoll,
    ListSessions,
    Resume,
    Cancel,
    Teardown,
    IsolationWorktree,
    IsolationContainer,
    IsolationVm,
    Templates,
    ArtifactFetch,
    UsageReport,
}

#[derive(Enum, Clone, Copy)]
#[fluessig(rename_all = "snake_case")]
pub enum EventKind {
    State,
    Message,
    ToolCall,
    ToolResult,
    Log,
    Usage,
    Artifact,
    Raw,
    Mail,
}

/// The three principals a message can move between (manager↔worker comms).
#[derive(Enum, Clone, Copy)]
#[fluessig(rename_all = "snake_case")]
pub enum Party {
    Manager,
    Worker,
    User,
}

#[derive(Enum, Clone, Copy)]
#[fluessig(rename_all = "snake_case")]
pub enum Fidelity {
    Exact,
    Derived,
    Scraped,
}

#[derive(Enum, Clone, Copy)]
#[fluessig(rename_all = "snake_case")]
pub enum ArtifactKind {
    Branch,
    PullRequest,
    Patch,
    File,
    Report,
    Url,
}

#[derive(Enum, Clone, Copy)]
#[fluessig(rename_all = "snake_case")]
pub enum McpRole {
    Supervisor,
    Worker,
}

/// How an external terminal reaches a session's live terminal — the discriminator
/// a consumer (pm) switches on to decide how to attach, instead of assuming tmux.
#[derive(Enum, Clone, Copy)]
#[fluessig(rename_all = "snake_case")]
pub enum AttachTransport {
    /// A reachable local tmux server: `attachEndpoint` = the socket, `attachTarget`
    /// = the tmux session name.
    Tmux,
    /// A disponent pty-holder: `attachEndpoint` = the holder's unix socket path,
    /// `attachTarget` = the session uid.
    DspHold,
    /// A ttyd/web fallback: `attachUrl` carries the address.
    Ttyd,
}

// ═════════════════════════════════════════════════════════════════════════════
// The tagged union — the whole point of this acid test (feature A).
// ═════════════════════════════════════════════════════════════════════════════

/// One observation's body — the union's variant tag is the wire discriminator.
#[derive(Union)]
pub enum EventPayload {
    State(StateChange),
    Message(AgentMessage),
    ToolCall(ToolCallInfo),
    ToolResult(ToolResultInfo),
    Log(LogLine),
    Usage(UsageDelta),
    Artifact(ArtifactRef),
    Raw(RawObservation),
    Mail(MailRef),
}

// ── union variant bodies (value structs) ──

#[derive(Record)]
pub struct StateChange {
    pub from: SessionState,
    pub to: SessionState,
}
#[derive(Record)]
pub struct AgentMessage {
    pub role: String,
    pub text: String,
}
#[derive(Record)]
pub struct ToolCallInfo {
    pub tool: String,
    pub input: Option<Json>,
}
#[derive(Record)]
pub struct ToolResultInfo {
    pub tool: String,
    pub ok: bool,
    pub output: Option<String>,
}
#[derive(Record)]
pub struct LogLine {
    pub line: String,
}
#[derive(Record)]
pub struct UsageDelta {
    pub model_id: Option<String>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cost_cents: Option<Cents>,
}
#[derive(Record)]
pub struct ArtifactRef {
    pub artifact_idx: i64,
}
/// Pointer + the fields a reader needs to triage a `mail` event without
/// fetching the Message: direction (sender/recipient), the fan-out it belongs
/// to, and its topic (so a reader can group by topic for latest-wins).
#[derive(Record)]
pub struct MailRef {
    pub message_id: MessageId,
    pub sender: Party,
    pub recipient: Party,
    pub fanout_id: FanoutId,
    pub topic: Option<String>,
}
#[derive(Record)]
pub struct RawObservation {
    pub source: String,
    pub data: Json,
}

// ═════════════════════════════════════════════════════════════════════════════
// The environment side
// ═════════════════════════════════════════════════════════════════════════════

/// Somewhere work can run. Config supplies these; the shipped catalog fills offerings + capabilities.
#[derive(Entity)]
#[fluessig(name = "environments")]
pub struct Environment {
    #[key]
    pub slug: String,
    pub kind: EnvKind,
    pub display_name: Option<String>,
    /// Address only — never a credential (secrets live in config/templates).
    pub endpoint: Option<url>,
    pub last_probed_at: Option<DateTime<Utc>>,
}

/// One capability row (the edge target of Environment.capabilities).
#[derive(Entity)]
#[fluessig(name = "capabilities")]
pub struct Capability {
    #[key]
    pub capability: CapabilityKind,
}

/// What this env can do (closed vocabulary; open detail).
#[derive(Edge)]
#[fluessig(name = "env_capabilities", edge(from = Environment, to = Capability, expose = "capabilities"))]
pub struct CapabilityDetail {
    pub slug: Id<Environment>,
    pub capability: Id<Capability>,
    /// Env-specific texture: poll granularity, supported template kinds, …
    pub detail: Option<Json>,
}

/// A reusable starting state, curated out-of-band (auth baked in by hand).
/// Reference-only: disponent copies/runs what you name, never builds.
#[derive(Entity)]
#[fluessig(name = "templates")]
pub struct Template {
    #[key]
    pub name: String,
    pub kind: TemplateKind,
    /// exe.dev template VM name, container image ref, …
    pub locator: String,
    /// Baseline setup script — runs first, before the repo clone and the dispatch's setup.
    pub setup: Option<String>,
    pub note: Option<String>,
}

/// A coding agent program (claude-code, codex, …).
#[derive(Entity)]
#[fluessig(name = "agents")]
pub struct Agent {
    #[key]
    pub name: String,
    pub version: Option<String>,
}

/// A model an agent can run with.
#[derive(Entity)]
#[fluessig(name = "models")]
pub struct AgentModel {
    #[key]
    pub id: String,
    pub provider: Option<String>,
    pub family: Option<String>,
}

/// env × agent × model availability (from the shipped catalog + config).
#[derive(Entity)]
#[fluessig(name = "offerings")]
pub struct Offering {
    #[key]
    pub env_slug: Id<Environment>,
    #[key]
    pub agent_name: Id<Agent>,
    #[key]
    pub model_id: Id<AgentModel>,
    pub is_default: bool,
}

// ═════════════════════════════════════════════════════════════════════════════
// The work side
// ═════════════════════════════════════════════════════════════════════════════

/// The immutable request. Never mutated after dispatch(); lifecycle lives on sessions.
#[derive(Entity)]
#[fluessig(name = "dispatches")]
pub struct Dispatch {
    #[key]
    pub id: DispatchId,
    pub created_at: DateTime<Utc>,
    pub title: Option<String>,
    /// The brief — the whole task spec, free-form. Structure belongs to consumers.
    pub brief: String,
    /// Workspace: URL or local path; empty = no repo (pure-prompt work).
    pub repo: Option<String>,
    pub git_ref: Option<String>,
    pub isolation: IsolationKind,
    /// Fetch gitRef from the workspace's origin and cut the worktree off it,
    /// rather than off local HEAD.
    pub fetch_remote: Option<bool>,
    /// Per-dispatch agent command line; replaces the env default and runs
    /// verbatim (the brief is NOT appended). For teleport-style launches like
    /// `claude --teleport <id>` that carry no prompt.
    pub agent_cmd: Option<String>,
    pub template_name: Option<Id<Template>>,
    /// Per-dispatch setup script — runs after the template's setup and the repo clone.
    pub setup: Option<String>,
    pub env_slug: Id<Environment>,
    pub agent_name: Id<Agent>,
    pub model_id: Option<Id<AgentModel>>,
    pub timeout_secs: Option<i32>,
    pub max_budget: Option<Cents>,
    /// MCP recursion depth: 0 = dispatched by the host program.
    pub via_mcp_depth: i32,
    /// Selection tags — the PRIMARY handle the Manager addresses a message
    /// fan-out to (notes/manager-worker-comms.md §8). A session inherits its
    /// dispatch's tags. Flat strings, indexable; distinct from the opaque
    /// `labels: Json`, which stays for arbitrary consumer metadata.
    pub tags: Option<Vec<String>>,
    /// Consumer labels, opaque to disponent.
    pub labels: Option<Json>,
}

/// One attempt at a dispatch, mirroring one env-side resource.
#[derive(Entity)]
#[fluessig(name = "sessions")]
pub struct Session {
    #[key]
    pub uid: SessionUid,
    pub dispatch_id: Id<Dispatch>,
    pub state: SessionState,
    /// The env's own handle(s): tmux session name, VM name, web session id/url.
    pub env_handle: Option<Json>,
    /// Transport-neutral attach descriptor (flat): how an external terminal reaches
    /// this session's live terminal. `attachTransport` is the discriminator a
    /// consumer switches on; the other three carry the address for that transport
    /// (tmux → endpoint+target; dsp-hold → endpoint+target; ttyd → url). All null
    /// when the env exposes no live terminal. Replaces #32's tmux-named scalars.
    ///
    /// Flat scalars rather than a nested `Attach` object: a nested returned struct
    /// trips the fluessig Magnus/Ruby emitter (an unwrapped nested record makes the
    /// `Option<Attach>` getter fail to compile, E0599) — same reason #32 stayed
    /// flat. Node and Python handle nesting; Ruby does not, and every binding must
    /// build. See notes/owning-the-terminal.md §10.
    pub attach_transport: Option<AttachTransport>,
    /// Socket path (tmux server socket / holder unix socket), or an ssh target.
    /// Set for `tmux` and `dsp_hold`; pairs with `attachTarget`.
    pub attach_endpoint: Option<String>,
    /// tmux session name (dsp-uid for local tmux workers), or the session uid
    /// (holder). Populated together with `attachEndpoint`.
    pub attach_target: Option<String>,
    /// ttyd/web fallback address — set for the `ttyd` transport.
    pub attach_url: Option<url>,
    /// Human-facing view URL when the env has one (ttyd, web session page).
    pub url: Option<url>,
    pub resumed_from: Option<Id<Session>>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub exit_reason: Option<ExitReason>,
    pub exit_detail: Option<String>,
    /// Set by reap(): resources torn down, row archived. Null = still on the board.
    pub reaped_at: Option<DateTime<Utc>>,
}

/// One observation on a session's timeline (containment rides the key: the
/// session FK + the position, like Artifact/Usage — to-manys are queries).
#[derive(Entity)]
#[fluessig(name = "events")]
pub struct Event {
    #[key]
    pub session_uid: Id<Session>,
    #[key]
    pub idx: i64,
    pub ts: DateTime<Utc>,
    pub kind: EventKind,
    pub fidelity: Fidelity,
    pub payload: EventPayload,
}

/// Something the session produced.
#[derive(Entity)]
#[fluessig(name = "artifacts")]
pub struct Artifact {
    #[key]
    pub session_uid: Id<Session>,
    #[key]
    pub idx: i64,
    pub kind: ArtifactKind,
    /// Branch name, PR URL, path, … — a pointer, not the bytes.
    pub locator: String,
    pub meta: Option<Json>,
}

/// Best-effort accounting, per session per model.
#[derive(Entity)]
#[fluessig(name = "usage")]
pub struct Usage {
    #[key]
    pub session_uid: Id<Session>,
    #[key]
    pub model_id: Id<AgentModel>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cost_cents: Cents,
}

/// One message dropped in one inbox — the manager↔worker communication
/// primitive (notes/manager-worker-comms.md). The Manager mints these addressed
/// to a worker or the user; a worker mints them addressed (implicitly) to its
/// Manager. Disponent owns these rows — no environment backs them, so reconcile
/// skips them and durability is the SQLite mirror (design §11).
#[derive(Entity)]
#[fluessig(name = "messages")]
pub struct Message {
    #[key]
    pub id: MessageId,
    pub created_at: DateTime<Utc>,
    pub sender: Party,
    pub recipient: Party,
    /// The worker session this message rides. EVERY message anchors to exactly
    /// one session's timeline — that is how both parties pull it via `events`:
    /// a Manager→worker note rides the recipient's timeline, a worker→Manager
    /// question rides the sender's, a Manager→user escalation rides the timeline
    /// of the worker it is about.
    pub session_uid: Id<Session>,
    /// The payload — free-form text, like the brief. Structure is the consumer's.
    pub body: String,
    /// Threading: the message this one replies to (an answer to a question, an
    /// escalation of a worker's message). Null for an unsolicited directive.
    /// Walking `inReplyTo` reconstructs a question's whole life — no status enum.
    pub in_reply_to: Option<Id<Message>>,
    /// One logical Manager broadcast → N Messages that all share this id. A
    /// single-recipient send still gets one (a fan-out of one). Counting acks over
    /// a `fanoutId` is how the Manager sees "N of M picked up the directive."
    pub fanout_id: FanoutId,
    /// Supersession key. A newer fan-out carrying the same `topic` supersedes
    /// older same-topic messages in an inbox: a reader acts on the LATEST message
    /// per (recipient, topic) and skips the stale ones. Null = standalone.
    pub topic: Option<String>,
    /// Stamped by the recipient's `ack`: received/handled. Manager-observable.
    /// Null = delivered (readable on the feed) but not yet acknowledged.
    pub acked_at: Option<DateTime<Utc>>,
}

// ═════════════════════════════════════════════════════════════════════════════
// DTOs — op-surface value structs
// ═════════════════════════════════════════════════════════════════════════════

#[derive(Record)]
pub struct OpenOptions {
    pub config_path: Option<String>,
    /// Default: a managed local SQLite file. "none" = memory-only; any driver-plan DSN otherwise.
    pub sink: Option<String>,
}
#[derive(Record)]
pub struct DispatchSpec {
    pub brief: String,
    pub env: String,
    pub agent: Option<String>,
    pub model: Option<String>,
    pub title: Option<String>,
    pub repo: Option<String>,
    pub git_ref: Option<String>,
    pub isolation: Option<IsolationKind>,
    /// Fetch gitRef from the workspace's origin and cut the worktree off it,
    /// rather than off local HEAD.
    pub fetch_remote: Option<bool>,
    /// Per-dispatch agent command line; replaces the env default and runs
    /// verbatim (the brief is NOT appended). For teleport-style launches like
    /// `claude --teleport <id>` that carry no prompt.
    pub agent_cmd: Option<String>,
    /// Named template to boot from.
    pub template: Option<String>,
    /// Per-dispatch setup script. No secrets — those live in the template.
    pub setup: Option<String>,
    pub timeout_secs: Option<i32>,
    pub max_budget: Option<Cents>,
    /// Skip catalog validation of (env, agent, model); the env's own error becomes the report.
    pub unchecked: Option<bool>,
    /// Selection tags a message fan-out can address this session by (§8).
    pub tags: Option<Vec<String>>,
    pub labels: Option<Json>,
}
#[derive(Record)]
pub struct SessionFilter {
    pub env: Option<String>,
    pub state: Option<SessionState>,
    pub dispatch_id: Option<DispatchId>,
}
/// An editor link into a session's working directory.
#[derive(Record)]
pub struct WorkspaceLink {
    /// The session this link addresses.
    pub session_uid: SessionUid,
    /// True when an honest editor link exists for this session's backend.
    pub available: bool,
    /// A deep link into the working dir: vscode://file/… for local backends, or vscode://vscode-remote/ssh-remote+… for remote ssh backends (exe.dev). Absent when unavailable.
    pub url: Option<url>,
    /// When available=false, an honest explanation of why (capability edge).
    pub detail: Option<String>,
}
#[derive(Record)]
pub struct EventOptions {
    pub session_uid: Option<SessionUid>,
    pub after_idx: Option<i64>,
    pub kinds: Option<Vec<EventKind>>,
}
/// Where a Manager-sent message goes. A worker never fills this: the
/// worker-role server defaults recipient = its Manager, anchored to the bound
/// session (deferred to the MCP layer). A Manager sets exactly one destination.
#[derive(Record)]
pub struct SendTarget {
    /// PRIMARY (§8): every live session whose dispatch carries any of these tags.
    /// `tags:["projectA"]` reaches all projectA workers without enumerating uids.
    /// Resolved to concrete recipients at send time (a snapshot, never kept live).
    pub tags: Option<Vec<String>>,
    /// Precise fallback: exact recipients by session uid.
    pub sessions: Option<Vec<SessionUid>>,
    /// Escalate to the human above the Manager, about this worker session (§10).
    pub user: Option<SessionUid>,
}
/// Filter for the `messages` read. Absent fields don't constrain.
#[derive(Record)]
pub struct MessagesFilter {
    /// All Messages of one fan-out — the Manager's ack-progress view.
    pub fanout_id: Option<FanoutId>,
    /// Only messages addressed to this party (inbox scoping).
    pub recipient: Option<Party>,
    /// Only messages anchored to this session's timeline.
    pub session_uid: Option<SessionUid>,
    /// Only messages carrying this topic.
    pub topic: Option<String>,
    /// Collapse to the newest message per (recipient, topic) in scope — the
    /// read-side latest-wins convention (§7). Standalone (null-topic) messages
    /// are always kept.
    pub latest_per_topic: Option<bool>,
}
#[derive(Record)]
pub struct ReconcileReport {
    pub adopted: i32,
    pub confirmed: i32,
    pub lost: i32,
    pub torn_down: i32,
}
#[derive(Record)]
pub struct McpOptions {
    /// Transport: "stdio" (v1).
    pub transport: Option<String>,
    /// Surface scope: supervisor = full; worker = observe-only. Default supervisor.
    pub role: Option<McpRole>,
    /// Belt-and-braces viaMcpDepth ceiling (default 1).
    pub max_depth: Option<i32>,
}
#[derive(Record)]
pub struct DriverPlanOptions {
    pub dialect: Option<String>,
    pub tables: Option<Vec<String>>,
}
#[derive(Record)]
pub struct Statement {
    pub sql: String,
    pub params: Json,
}
/// What an environment can do: one row per (env, capability) the catalog
/// advertises. Mirrors the env_capabilities edge as a flat, returnable value
/// struct (the closed CapabilityKind vocabulary, plus open detail).
#[derive(Record)]
pub struct EnvCapability {
    pub env_slug: String,
    pub capability: CapabilityKind,
    /// Env-specific texture (poll granularity, supported template kinds, …), when known.
    pub detail: Option<Json>,
}

// ═════════════════════════════════════════════════════════════════════════════
// The op surface — features B (@readonly) + C (@destructive), composing with kind
// ═════════════════════════════════════════════════════════════════════════════

/// An open disponent instance. A unit-ish struct keeps the op root a *type*.
pub struct Disponent {
    _private: (),
}

#[export]
impl Disponent {
    #[fluessig(ctor)]
    pub fn open(options: Option<OpenOptions>) -> Self {
        let _ = options;
        Disponent { _private: () }
    }

    #[fluessig(readonly)]
    pub fn environments(&self) -> Vec<Environment> {
        Vec::new()
    }

    pub fn refresh(&self, env_slug: Option<String>) -> Vec<Environment> {
        let _ = env_slug;
        Vec::new()
    }

    /// The offerings table: every env × agent × model the catalog knows, each
    /// flagged `isDefault` when it's the pick for a dispatch that names only the
    /// environment. Lets a consumer enumerate what can run where without reaching
    /// for the raw driver plan.
    #[fluessig(readonly)]
    pub fn offerings(&self) -> Vec<Offering> {
        Vec::new()
    }

    /// Per-env capabilities: what each environment can do, one row per
    /// (env, capability) the catalog advertises. Lets a consumer grade backends
    /// by what they support without reaching for the raw driver plan.
    #[fluessig(readonly)]
    pub fn capabilities(&self) -> Vec<EnvCapability> {
        Vec::new()
    }

    pub fn dispatch(&self, spec: DispatchSpec) -> Session {
        let _ = spec;
        unimplemented!()
    }

    #[fluessig(readonly)]
    pub fn session(&self, uid: SessionUid) -> Option<Session> {
        let _ = uid;
        None
    }

    #[fluessig(readonly)]
    pub fn sessions(&self, filter: Option<SessionFilter>) -> Vec<Session> {
        let _ = filter;
        Vec::new()
    }

    /// Return an editor link (VS Code deep link) into the session's working directory, when the backend can honestly provide one.
    #[fluessig(readonly)]
    pub fn workspace_link(&self, session_uid: SessionUid) -> WorkspaceLink {
        let _ = session_uid;
        unimplemented!()
    }

    #[fluessig(readonly, stream)]
    pub fn events(&self, options: Option<EventOptions>) -> impl Iterator<Item = Event> {
        let _ = options;
        std::iter::empty()
    }

    /// The one messaging primitive (notes/manager-worker-comms.md §6). A Manager
    /// `to` names a tagged worker subset (fan-out) or the user; recipients resolve
    /// at send time to a concrete list (a snapshot, never kept live). One Message
    /// is minted per matched session, all sharing a freshly minted `fanoutId`;
    /// `topic` (optional) is the supersession key for latest-wins (§7). Returns the
    /// Messages minted. Delivery is the reader's `events` pull; a message to a
    /// concrete live worker is also delivered best-effort to its prompt on an
    /// interact-capable env (the legacy `send` behavior, now one backend delivery).
    /// Worker self-send (recipient forced to the Manager) is a worker-role MCP
    /// concern, deferred — the core send is the Manager surface.
    pub fn send(
        &self,
        body: String,
        to: Option<SendTarget>,
        in_reply_to: Option<MessageId>,
        topic: Option<String>,
    ) -> Vec<Message> {
        let _ = (body, to, in_reply_to, topic);
        Vec::new()
    }

    /// Acknowledge a message you received (received/handled): stamps `ackedAt`,
    /// which the Manager observes across a `fanoutId` to see "N of M acted" (§7).
    /// Idempotent.
    pub fn ack(&self, message_id: MessageId) {
        let _ = message_id;
    }

    /// Read Messages, filtered. The Manager's fan-out ack-progress view
    /// (`{fanoutId}`) and a recipient's inbox (`{recipient, sessionUid}`, optionally
    /// `latestPerTopic` for the read-side latest-wins collapse, §7).
    #[fluessig(readonly)]
    pub fn messages(&self, filter: Option<MessagesFilter>) -> Vec<Message> {
        let _ = filter;
        Vec::new()
    }

    #[fluessig(destructive)]
    pub fn cancel(&self, session_uid: SessionUid) -> Session {
        let _ = session_uid;
        unimplemented!()
    }

    pub fn resume(&self, session_uid: SessionUid) -> Session {
        let _ = session_uid;
        unimplemented!()
    }

    #[fluessig(destructive)]
    pub fn reap(&self, session_uid: SessionUid) -> Session {
        let _ = session_uid;
        unimplemented!()
    }

    pub fn reconcile(&self) -> ReconcileReport {
        unimplemented!()
    }

    #[fluessig(readonly, stream)]
    pub fn driver_plan(
        &self,
        options: Option<DriverPlanOptions>,
    ) -> impl Iterator<Item = Statement> {
        let _ = options;
        std::iter::empty()
    }

    /// Blocking wait — hand-written per binding (event-loop/GVL specifics).
    #[fluessig(manual)]
    pub fn wait(&self, session_uid: SessionUid, timeout_secs: i32) -> Session {
        let _ = (session_uid, timeout_secs);
        unimplemented!()
    }

    /// Long-running MCP server over this instance — hand-written per binding.
    #[fluessig(manual)]
    pub fn serve_mcp(&self, options: Option<McpOptions>) {
        let _ = options;
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// The exporter — replaces `(cd emitter && node emit.mjs ../schema/disponent.tsp)`.
// ═════════════════════════════════════════════════════════════════════════════

catalog! {
    name: "disponent.tsp",
    version: "0",
    entities: [
        Environment, Capability, Template, Agent, AgentModel, Offering,
        Dispatch, Session, Event, Artifact, Usage, Message,
    ],
    edges: [CapabilityDetail],
    records: [
        // union variant bodies
        StateChange, AgentMessage, ToolCallInfo, ToolResultInfo, LogLine,
        UsageDelta, ArtifactRef, RawObservation, MailRef,
        // op-surface DTOs
        OpenOptions, DispatchSpec, SessionFilter, WorkspaceLink, EventOptions,
        SendTarget, MessagesFilter, ReconcileReport, McpOptions, DriverPlanOptions,
        Statement, EnvCapability,
    ],
    enums: [
        EnvKind, SessionState, ExitReason, IsolationKind, TemplateKind,
        CapabilityKind, EventKind, Party, Fidelity, ArtifactKind, McpRole,
        AttachTransport,
    ],
    unions: [EventPayload],
    scalars: [DispatchId, SessionUid, MessageId, FanoutId, Cents],
    api: [Disponent],
}
