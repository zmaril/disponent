//! The engine: an in-memory ledger behind the generated `DisponentMcp` trait,
//! mirrored into the sink (SQLite by default) as fluessig plans.
//!
//! Dispatch routes by environment kind to a registered backend (exe.dev VMs,
//! local tmux) and provisions on a background thread; a kind with no backend
//! queues honestly. Environments stay the source of truth — the ledger is the
//! reconciled cache, and `reconcile()` confirms/loses/adopts against each
//! backend's survey. Ops a version can't do yet (resume) say so instead of
//! pretending.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail};
use chrono::{SecondsFormat, Utc};
use fluessig::data::{Mutation, Transaction};
use fluessig::observe::ObserverPool;
use fluessig::sql::Dialect;
use serde_json::json;
use uuid::Uuid;

use crate::agent::{AgentAdapter, ClaudeCode};
use crate::backend::{EnvProvider, ExeDev, StartRequest};
use crate::catalog::{self, upsert};
use crate::local::LocalTmux;
use crate::mcp_generated::{
    DispatchSpec, DriverPlanOptions, EnvCapability, Environment, Event, EventOptions, Message,
    MessagesFilter, Offering, ReconcileReport, SendTarget, Session, SessionFilter, Statement,
    WorkspaceLink,
};
use crate::modal::Modal;
use crate::observe::Observation;
use crate::schema_gen::{TableSchema, DUCKDB_TABLES, PG_TABLES, SQLITE_TABLES};
use crate::sink::Sink;
use crate::status::{legal_transition, TERMINAL};
use watch::{collect, watch_session};

/// Page size for the stream cursors when the caller doesn't pass `limit`.
const DEFAULT_PAGE: usize = 100;

pub struct Engine {
    ledger: Arc<Mutex<Ledger>>,
    /// One backend per environment kind; a kind with no backend queues honestly.
    backends: Vec<Arc<dyn EnvProvider>>,
    /// One adapter per agent, selected by the resolved `agent` string; an agent
    /// with no adapter queues honestly (running a worker needs both a backend
    /// and an adapter). A second agent CLI is a new adapter, not a new backend.
    adapters: Vec<Arc<dyn AgentAdapter>>,
    /// Terminal observers (one thread per watched session) funneling into the
    /// collector, which folds observations into the ledger.
    observers: Arc<ObserverPool<Observation>>,
    observe_interval: Duration,
    collector_stop: Arc<AtomicBool>,
    /// OTLP receiver endpoints workers get wired to: one reachable from local
    /// workers, one from remote ones. None = that tier is off.
    otel_local: Option<String>,
    otel_public: Option<String>,
}

#[derive(Default)]
struct Ledger {
    environments: Vec<Environment>,
    dispatches: Vec<DispatchRow>,
    sessions: Vec<Session>,
    /// Append-only, in observation order across all sessions — the stream the
    /// `events` cursor pages over (`after` = items already consumed).
    events: Vec<Event>,
    /// Control-plane manager↔worker messages. Disponent owns these (no env
    /// backs them): reconcile skips them, durability is the sink mirror.
    messages: Vec<Message>,
    sink: Sink,
}

/// The immutable request (never mutated after dispatch; lifecycle is sessions').
struct DispatchRow {
    id: String,
    created_at: String,
    spec: DispatchSpec,
    /// Catalog-resolved: what will actually run (spec.agent/model are the ask).
    agent: String,
    model: Option<String>,
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// #28: the one place a dispatch's lifecycle limits (idle, wall-clock, budget)
/// are resolved, so #25's timeout/budget detectors consume a single resolution
/// rather than re-reading limits ad hoc. PR-1 resolves and logs it; enforcement
/// is future work.
#[derive(Clone, Debug)]
struct LifecyclePolicy {
    /// Max quiet time before a running session is considered stalled. The #25
    /// idle-timeout detector reads this as its threshold.
    idle_secs: u64,
    /// Max quiet time after start before the stream is considered dead. The #25
    /// first-token dead-stream detector reads this as its threshold.
    first_token_secs: u64,
    /// Max total wall-clock lifetime.
    wall_secs: u64,
    /// Budget ceiling, as the dispatch stated it (opaque money string for now).
    max_budget: Option<String>,
}

impl Default for LifecyclePolicy {
    fn default() -> Self {
        // Sane defaults for a session with no explicit limits: idle out after 5
        // minutes of quiet, flag a dead stream one minute after start with no
        // first token, cap the whole run at an hour.
        LifecyclePolicy {
            idle_secs: 300,
            first_token_secs: 60,
            wall_secs: 3600,
            max_budget: None,
        }
    }
}

impl LifecyclePolicy {
    /// Resolve from the immutable dispatch row: `timeout_secs` sets the
    /// wall-clock ceiling when given; the rest fall back to defaults.
    fn resolve(timeout_secs: Option<i32>, max_budget: Option<String>) -> Self {
        let defaults = LifecyclePolicy::default();
        LifecyclePolicy {
            wall_secs: timeout_secs
                .filter(|s| *s > 0)
                .map(|s| s as u64)
                .unwrap_or(defaults.wall_secs),
            max_budget,
            ..defaults
        }
    }

    /// A one-line summary for the session timeline.
    fn summary(&self) -> String {
        format!(
            "lifecycle policy: idle={}s first_token={}s wall={}s budget={}",
            self.idle_secs,
            self.first_token_secs,
            self.wall_secs,
            self.max_budget.as_deref().unwrap_or("none")
        )
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine {
    /// Memory-only, no backends (tests, throwaways). `open` is the front door.
    pub fn new() -> Self {
        Engine::assemble(
            Ledger {
                environments: catalog::environments(),
                ..Ledger::default()
            },
            Vec::new(),
        )
    }

    /// An engine over the shipped catalog with the real backends (exe.dev +
    /// local tmux). `sink`: `None` = the managed SQLite file (~/.disponent),
    /// `"none"` = memory only, anything else = a SQLite path. The OTel
    /// receiver starts when `DISPONENT_OTEL_PORT` is set (remote workers
    /// reach it via `DISPONENT_OTEL_PUBLIC_URL`, when that's reachable).
    pub fn open(sink: Option<&str>) -> anyhow::Result<Self> {
        let mut engine = Engine::open_with(
            sink,
            vec![
                Arc::new(ExeDev::from_env()),
                Arc::new(LocalTmux::from_env()),
                Arc::new(Modal::from_env()),
            ],
        )?;
        if let Ok(port) = std::env::var("DISPONENT_OTEL_PORT") {
            let port: u16 = port
                .parse()
                .map_err(|_| anyhow!("DISPONENT_OTEL_PORT: not a port: {port}"))?;
            engine.start_otel(port)?;
            engine.otel_public = std::env::var("DISPONENT_OTEL_PUBLIC_URL").ok();
        }
        Ok(engine)
    }

    /// The composable front door: any sink spec, any backend set. A sink that
    /// remembers earlier runs rehydrates the ledger — restarts serve the full
    /// board, not just what reconcile can re-discover live — and running
    /// sessions get their terminal observers back.
    pub fn open_with(
        sink: Option<&str>,
        backends: Vec<Arc<dyn EnvProvider>>,
    ) -> anyhow::Result<Self> {
        let mut sink = Sink::open(sink)?;
        sink.apply(&catalog::seed_tx())?;
        let mut ledger = Ledger {
            environments: catalog::environments(),
            ..Ledger::default()
        };
        if let Some(restored) = sink.restore()? {
            // The shipped catalog stays the baseline; stored rows contribute
            // only what the seed doesn't know (probe timestamps).
            for env in &mut ledger.environments {
                if let Some(saved) = restored.environments.iter().find(|e| e.slug == env.slug) {
                    env.last_probed_at = saved.last_probed_at.clone();
                }
            }
            ledger.dispatches = restored
                .dispatches
                .into_iter()
                .map(|d| DispatchRow {
                    id: d.id,
                    created_at: d.created_at,
                    spec: d.spec,
                    agent: d.agent,
                    model: d.model,
                })
                .collect();
            ledger.sessions = restored.sessions;
            ledger.events = restored.events;
            ledger.messages = restored.messages;
        }
        ledger.sink = sink;
        let engine = Engine::assemble(ledger, backends);
        // Rehydrated running sessions get watched again.
        #[allow(clippy::type_complexity)]
        let watchable: Vec<(String, serde_json::Value, Option<String>, Option<String>)> = {
            let ledger = engine.ledger.lock().unwrap();
            ledger
                .sessions
                .iter()
                .filter(|s| s.state == "running" && s.reaped_at.is_none())
                .filter_map(|s| {
                    s.env_handle.clone().map(|h| {
                        (
                            s.uid.clone(),
                            h,
                            ledger.env_kind_of(&s.uid),
                            ledger.agent_of(&s.uid),
                        )
                    })
                })
                .collect()
        };
        for (uid, handle, kind, agent) in watchable {
            if let (Some(backend), Some(adapter)) = (
                kind.and_then(|k| engine.backend_for(&k)),
                agent.and_then(|a| engine.adapter_for(&a)),
            ) {
                // Rehydrated watchers don't carry the dispatch's limits forward;
                // policy is logged/stored only in PR-1, so a default is honest.
                engine.watch(&uid, backend, adapter, handle, LifecyclePolicy::default());
            }
        }
        Ok(engine)
    }

    /// Memory-only over one injected backend (the dry-run tests' front door).
    pub fn with_backend<B: EnvProvider + 'static>(backend: B) -> Self {
        Engine::assemble(
            Ledger {
                environments: catalog::environments(),
                ..Ledger::default()
            },
            vec![Arc::new(backend)],
        )
    }

    /// The common tail of every constructor: the observer pool and the
    /// collector thread that folds its drain into the ledger.
    fn assemble(ledger: Ledger, backends: Vec<Arc<dyn EnvProvider>>) -> Self {
        let ledger = Arc::new(Mutex::new(ledger));
        let observers = Arc::new(ObserverPool::new(1024));
        let collector_stop = Arc::new(AtomicBool::new(false));
        {
            let ledger = Arc::clone(&ledger);
            let observers = Arc::clone(&observers);
            let stop = Arc::clone(&collector_stop);
            std::thread::spawn(move || collect(ledger, observers, stop));
        }
        let observe_interval = Duration::from_millis(
            std::env::var("DISPONENT_OBSERVE_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5000),
        );
        Engine {
            ledger,
            backends,
            // The shipped adapters: today just claude-code. Selection is by the
            // catalog `agent` string, mirroring backend selection by env-kind.
            adapters: vec![Arc::new(ClaudeCode)],
            observers,
            observe_interval,
            collector_stop,
            otel_local: None,
            otel_public: None,
        }
    }

    /// Start the OTLP/http-json receiver on `port` (0 = ephemeral); local
    /// workers get wired to the bound endpoint. Returns the bound port.
    pub fn start_otel(&mut self, port: u16) -> anyhow::Result<u16> {
        let ledger = Arc::clone(&self.ledger);
        let bound = crate::otel::serve(port, move |e| {
            let mut l = ledger.lock().unwrap();
            // Only sessions we know get a timeline; a stale worker's late
            // telemetry after reap is dropped, not resurrected.
            if l.sessions.iter().any(|s| s.uid == e.session_uid) {
                let ev = l.push_event_graded(&e.session_uid, &e.kind, &e.fidelity, e.payload);
                let _ = l.mirror(vec![event_mutation(&ev)]);
            }
        })?;
        self.otel_local = Some(format!("http://127.0.0.1:{bound}"));
        Ok(bound)
    }

    /// The OTLP endpoint workers of this backend kind should export to.
    fn otel_endpoint_for(&self, kind: &str) -> Option<String> {
        if kind == "local" {
            self.otel_local.clone()
        } else {
            self.otel_public.clone()
        }
    }

    /// Watch a running session's terminal: capture on an interval, emit what
    /// changed as scraped raw events. Idempotent per session (pool-enforced).
    fn watch(
        &self,
        uid: &str,
        backend: Arc<dyn EnvProvider>,
        adapter: Arc<dyn AgentAdapter>,
        handle: serde_json::Value,
        policy: LifecyclePolicy,
    ) {
        watch_session(
            &self.observers,
            self.observe_interval,
            backend,
            adapter,
            uid,
            handle,
            policy,
        );
    }

    fn backend_for(&self, kind: &str) -> Option<Arc<dyn EnvProvider>> {
        self.backends.iter().find(|b| b.kind() == kind).cloned()
    }

    fn adapter_for(&self, agent: &str) -> Option<Arc<dyn AgentAdapter>> {
        self.adapters.iter().find(|a| a.agent() == agent).cloned()
    }

    /// The (backend, adapter) pair that drives a session's worker, resolved off
    /// a locked ledger from the session's dispatch (env-kind → backend, agent →
    /// adapter). Either is `None` when nothing's registered for it — the caller
    /// then behaves honestly (no reachable worker). The one resolution the
    /// interact ops (send/cancel/reap) share.
    #[allow(clippy::type_complexity)]
    fn routing(
        &self,
        ledger: &Ledger,
        uid: &str,
    ) -> (Option<Arc<dyn EnvProvider>>, Option<Arc<dyn AgentAdapter>>) {
        (
            ledger.env_kind_of(uid).and_then(|k| self.backend_for(&k)),
            ledger.agent_of(uid).and_then(|a| self.adapter_for(&a)),
        )
    }

    /// Worker-role `send` (notes/manager-worker-comms.md §9): recipient forced
    /// to the Manager, sender = the bound worker session, anchored to that
    /// session. The worker-role MCP server calls this instead of the Manager
    /// `send` so a worker never names a recipient (no addressing a sibling).
    pub fn worker_send(
        &self,
        bound_session: &str,
        body: String,
        in_reply_to: Option<String>,
        topic: Option<String>,
    ) -> anyhow::Result<Vec<Message>> {
        messaging::worker_send(self, bound_session, body, in_reply_to, topic)
    }

    /// Worker-role `ack` (§9): only a message in the bound session's own inbox.
    pub fn worker_ack(&self, bound_session: &str, message_id: String) -> anyhow::Result<()> {
        messaging::worker_ack(self, bound_session, message_id)
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.collector_stop.store(true, Ordering::Relaxed);
        self.observers.shutdown();
    }
}

// The terminal-watcher subsystem (watch_session + collect, both tiers) lives in
// `watch.rs` — see `mod watch` below — to keep this file under the size budget.

// ── ledger rows → sink mutations (columns follow the generated schema) ──

fn environment_mutation(e: &Environment) -> Mutation {
    upsert(
        "environments",
        &["slug", "kind", "display_name", "endpoint", "last_probed_at"],
        vec![vec![
            json!(e.slug),
            json!(e.kind),
            json!(e.display_name),
            json!(e.endpoint),
            json!(e.last_probed_at),
        ]],
    )
}

fn session_mutation(s: &Session) -> Mutation {
    upsert(
        "sessions",
        &[
            "uid",
            "dispatch_id",
            "state",
            "env_handle",
            "attach_transport",
            "attach_endpoint",
            "attach_target",
            "attach_url",
            "url",
            "resumed_from",
            "started_at",
            "ended_at",
            "exit_reason",
            "exit_detail",
            "reaped_at",
        ],
        vec![vec![
            json!(s.uid),
            json!(s.dispatch_id),
            json!(s.state),
            s.env_handle.clone().unwrap_or(serde_json::Value::Null),
            json!(s.attach_transport),
            json!(s.attach_endpoint),
            json!(s.attach_target),
            json!(s.attach_url),
            json!(s.url),
            json!(s.resumed_from),
            json!(s.started_at),
            json!(s.ended_at),
            json!(s.exit_reason),
            json!(s.exit_detail),
            json!(s.reaped_at),
        ]],
    )
}

/// The union rides as twin columns: `payload_kind` = the envelope's tag,
/// `payload` = the variant body.
fn event_mutation(e: &Event) -> Mutation {
    upsert(
        "events",
        &[
            "session_uid",
            "idx",
            "ts",
            "kind",
            "fidelity",
            "payload_kind",
            "payload",
        ],
        vec![vec![
            json!(e.session_uid),
            json!(e.idx),
            json!(e.ts),
            json!(e.kind),
            json!(e.fidelity),
            e.payload["kind"].clone(),
            e.payload["payload"].clone(),
        ]],
    )
}

impl DispatchRow {
    fn mutation(&self) -> Mutation {
        upsert(
            "dispatches",
            &[
                "id",
                "created_at",
                "title",
                "brief",
                "repo",
                "git_ref",
                "isolation",
                "fetch_remote",
                "agent_cmd",
                "template_name",
                "setup",
                "env_slug",
                "agent_name",
                "model_id",
                "timeout_secs",
                "max_budget",
                "via_mcp_depth",
                "tags",
                "labels",
            ],
            vec![vec![
                json!(self.id),
                json!(self.created_at),
                json!(self.spec.title),
                json!(self.spec.brief),
                json!(self.spec.repo),
                json!(self.spec.git_ref),
                json!(self.spec.isolation.as_deref().unwrap_or("none")),
                json!(self.spec.fetch_remote.unwrap_or(false)),
                json!(self.spec.agent_cmd),
                json!(self.spec.template),
                json!(self.spec.setup),
                json!(self.spec.env),
                json!(self.agent),
                json!(self.model),
                json!(self.spec.timeout_secs),
                json!(self.spec.max_budget),
                json!(0),
                json!(self.spec.tags),
                self.spec.labels.clone().unwrap_or(serde_json::Value::Null),
            ]],
        )
    }
}

impl Ledger {
    fn session_mut(&mut self, uid: &str) -> anyhow::Result<&mut Session> {
        self.sessions
            .iter_mut()
            .find(|s| s.uid == uid)
            .ok_or_else(|| anyhow!("no session {uid}"))
    }

    fn push_event(&mut self, session_uid: &str, kind: &str, payload: serde_json::Value) -> Event {
        // engine-witnessed facts are exact by definition
        self.push_event_graded(session_uid, kind, "exact", payload)
    }

    fn push_event_graded(
        &mut self,
        session_uid: &str,
        kind: &str,
        fidelity: &str,
        payload: serde_json::Value,
    ) -> Event {
        let idx = self
            .events
            .iter()
            .filter(|e| e.session_uid == session_uid)
            .count() as i64;
        let event = Event {
            session_uid: session_uid.to_string(),
            idx,
            ts: now(),
            kind: kind.to_string(),
            fidelity: fidelity.to_string(),
            payload,
        };
        self.events.push(event.clone());
        event
    }

    fn transition(&mut self, uid: &str, to: &str) -> anyhow::Result<(Session, Event)> {
        let from = self.session_mut(uid)?.state.clone();
        if !legal_transition(&from, to) {
            bail!("illegal transition: {from} -> {to}");
        }
        let session = self.session_mut(uid)?;
        session.state = to.to_string();
        if TERMINAL.contains(&to) {
            session.ended_at = Some(now());
        }
        let snapshot = session.clone();
        let event = self.push_event(
            uid,
            "state",
            json!({"kind": "state", "payload": {"from": from, "to": to}}),
        );
        Ok((snapshot, event))
    }

    /// Mirror mutations into the sink. The ledger has already moved when this
    /// runs; a sink failure is reported (the caller's op errors) rather than
    /// rolled back — reconcile-time replay is the recovery story, not undo.
    fn mirror(&mut self, mutations: Vec<Mutation>) -> anyhow::Result<()> {
        self.sink.apply(&Transaction { mutations })
    }

    /// The environment kind a session runs in (via its dispatch's env slug).
    fn env_kind_of(&self, uid: &str) -> Option<String> {
        let session = self.sessions.iter().find(|s| s.uid == uid)?;
        let dispatch = self
            .dispatches
            .iter()
            .find(|d| d.id == session.dispatch_id)?;
        self.environments
            .iter()
            .find(|e| e.slug == dispatch.spec.env)
            .map(|e| e.kind.clone())
    }

    /// The catalog-resolved agent a session runs (via its dispatch) — how the
    /// engine picks the [`AgentAdapter`](crate::agent::AgentAdapter) to drive it.
    fn agent_of(&self, uid: &str) -> Option<String> {
        let session = self.sessions.iter().find(|s| s.uid == uid)?;
        self.dispatches
            .iter()
            .find(|d| d.id == session.dispatch_id)
            .map(|d| d.agent.clone())
    }
}

/// The background half of a backed dispatch: provision the worker, then flip
/// the session to running (or failed) — unless someone cancelled/reaped it
/// mid-provision, in which case the fresh worker is torn down, not adopted.
fn provision_worker(
    ledger: Arc<Mutex<Ledger>>,
    backend: Arc<dyn EnvProvider>,
    adapter: Arc<dyn AgentAdapter>,
    req: StartRequest,
    observers: Arc<ObserverPool<Observation>>,
    observe_interval: Duration,
    policy: LifecyclePolicy,
) {
    let uid = req.session_uid.clone();
    {
        let mut l = ledger.lock().unwrap();
        // Cancelled/reaped before we even started? Don't resurrect it.
        match l.session_mut(&uid) {
            Ok(s) if s.state == "queued" => {}
            _ => return,
        }
        match l.transition(&uid, "provisioning") {
            Ok((s, e)) => {
                let _ = l.mirror(vec![session_mutation(&s), event_mutation(&e)]);
            }
            Err(_) => return,
        }
    }

    // START stands the worker up (env-create + clone + setup), then the agent
    // adapter drives the agent's launch on the fresh Compute surface: make the
    // CLI present (install), make creds present (auth), launch it (start) with
    // the command it composes from the provider's LaunchSpec — the brief rides
    // that launch argv, so no separate prompt is needed at provision. A failure
    // at any stage becomes the session's setup error.
    let started = (|| -> anyhow::Result<crate::backend::Provision> {
        let p = backend.start(&req)?;
        if let Some(launch) = backend.launch_spec(&req) {
            let compute = backend.compute(&p.handle)?;
            adapter.install(&*compute)?;
            adapter.auth(&*compute)?;
            adapter.start(&*compute, &launch)?;
        }
        Ok(p)
    })();

    match started {
        Ok(p) => {
            let mut l = ledger.lock().unwrap();
            let Ok(session) = l.session_mut(&uid) else {
                let _ = backend.reap(&p.handle);
                return;
            };
            if session.state != "provisioning" {
                let _ = backend.reap(&p.handle);
                return;
            }
            // Route the flip through the guard so it can't skip the state
            // machine and so exactly one "state" event is emitted.
            let state_ev = match l.transition(&uid, "running") {
                Ok((_, e)) => e,
                Err(_) => {
                    let _ = backend.reap(&p.handle);
                    return;
                }
            };
            let session = l.session_mut(&uid).expect("just transitioned to running");
            session.started_at = Some(now());
            session.env_handle = Some(p.handle.clone());
            let (transport, endpoint, target, attach_url) =
                crate::local::attach_from_handle(&p.handle, &uid, p.url.as_deref());
            session.attach_transport = transport;
            session.attach_endpoint = endpoint;
            session.attach_target = target;
            session.attach_url = attach_url;
            session.url = p.url.clone();
            let snapshot = session.clone();
            // The worker-up log also carries the resolved lifecycle policy (#28):
            // one place the limits were decided, folded into an existing event so
            // the timeline shape is unchanged.
            let log_ev = l.push_event(
                &uid,
                "log",
                json!({"kind": "log", "payload":
                    {"line": format!("worker up: {}{} [{}]", p.handle,
                        p.url.as_deref().map(|u| format!(" ({u})")).unwrap_or_default(),
                        policy.summary())}}),
            );
            let _ = l.mirror(vec![
                session_mutation(&snapshot),
                event_mutation(&state_ev),
                event_mutation(&log_ev),
            ]);
            drop(l);
            // #28: the resolved policy is also carried into the watcher (the
            // single resolution site) so #25's detectors can consume it later.
            watch_session(
                &observers,
                observe_interval,
                backend,
                adapter,
                &uid,
                p.handle,
                policy,
            );
        }
        Err(e) => {
            let mut l = ledger.lock().unwrap();
            let Ok((_, ev)) = l.transition(&uid, "failed") else {
                return;
            };
            let Ok(session) = l.session_mut(&uid) else {
                return;
            };
            session.exit_reason = Some("setup".to_string());
            session.exit_detail = Some(e.to_string());
            let snapshot = session.clone();
            let _ = l.mirror(vec![session_mutation(&snapshot), event_mutation(&ev)]);
        }
    }
}

impl crate::mcp_generated::DisponentMcp for Engine {
    fn environments(&self) -> anyhow::Result<Vec<Environment>> {
        Ok(self.ledger.lock().unwrap().environments.clone())
    }

    fn offerings(&self) -> anyhow::Result<Vec<Offering>> {
        // Straight off the shipped catalog (the flattened env×agent×model table);
        // no ledger state, so no lock needed.
        Ok(catalog::OFFERINGS
            .iter()
            .map(|o| Offering {
                env_slug: o.env.to_string(),
                agent_name: o.agent.to_string(),
                model_id: o.model.to_string(),
                is_default: o.is_default,
            })
            .collect())
    }

    fn capabilities(&self) -> anyhow::Result<Vec<EnvCapability>> {
        // Straight off the shipped catalog (the same static per-env capability
        // data seeded into the env_capabilities edge); one row per (env,
        // capability) the backend actually advertises — no ledger state, no lock.
        // detail is None: the catalog carries no per-capability texture today.
        Ok(catalog::CAPABILITIES
            .iter()
            .flat_map(|ec| {
                ec.capabilities.iter().map(|cap| EnvCapability {
                    env_slug: ec.env.to_string(),
                    capability: cap.to_string(),
                    detail: None,
                })
            })
            .collect())
    }

    fn refresh(&self, env_slug: Option<String>) -> anyhow::Result<Vec<Environment>> {
        // No live probing until backends land (phase 3); a refresh re-stamps
        // the catalog rows so callers can see the engine looked.
        let mut ledger = self.ledger.lock().unwrap();
        let stamp = now();
        let mut out = Vec::new();
        for env in ledger.environments.iter_mut() {
            if env_slug.as_deref().is_none_or(|s| s == env.slug) {
                env.last_probed_at = Some(stamp.clone());
                out.push(env.clone());
            }
        }
        if out.is_empty() {
            if let Some(slug) = env_slug {
                bail!("no environment '{slug}'");
            }
        }
        ledger.mirror(out.iter().map(environment_mutation).collect())?;
        Ok(out)
    }

    fn dispatch(&self, spec: DispatchSpec) -> anyhow::Result<Session> {
        let mut ledger = self.ledger.lock().unwrap();
        if !ledger.environments.iter().any(|e| e.slug == spec.env) {
            bail!("no environment '{}'", spec.env);
        }

        // Resolve agent/model from the catalog; `unchecked` skips validation
        // and lets the environment's own error become the report.
        let (default_agent, default_model) = catalog::default_offering(&spec.env)
            .map(|(a, m)| (a.to_string(), m.to_string()))
            .unzip();
        let agent =
            spec.agent.clone().or(default_agent).ok_or_else(|| {
                anyhow!("no agent given and '{}' has no default offering", spec.env)
            })?;
        let model = spec.model.clone().or(if spec.agent.is_none() {
            default_model
        } else {
            None
        });
        if !spec.unchecked.unwrap_or(false)
            && !catalog::offered(&spec.env, &agent, model.as_deref())
        {
            bail!(
                "the catalog has no offering ({}, {agent}, {}) — pass unchecked to try anyway",
                spec.env,
                model.as_deref().unwrap_or("*"),
            );
        }

        // Will anything actually run this? A kind with a registered backend
        // provisions; everything else queues honestly.
        let env_kind = ledger
            .environments
            .iter()
            .find(|e| e.slug == spec.env)
            .map(|e| e.kind.clone())
            .unwrap_or_default();
        let backend = self.backend_for(&env_kind);
        if let Some(b) = &backend {
            if b.requires_template() && spec.template.is_none() {
                bail!(
                    "dispatch to '{}' needs `template`: an env-side base image to copy",
                    spec.env
                );
            }
        }
        // The agent adapter is selected by the resolved `agent`, the way the
        // backend is selected by env-kind. Running a worker needs BOTH: an env
        // to stand up and an adapter to drive the agent on it.
        let adapter = self.adapter_for(&agent);
        let runnable = backend.is_some() && adapter.is_some();

        let dispatch = DispatchRow {
            id: Uuid::now_v7().to_string(),
            created_at: now(),
            spec,
            agent,
            model,
        };
        let session = Session {
            uid: Uuid::now_v7().to_string(),
            dispatch_id: dispatch.id.clone(),
            state: "queued".to_string(),
            env_handle: None,
            attach_transport: None,
            attach_endpoint: None,
            attach_target: None,
            attach_url: None,
            url: None,
            resumed_from: None,
            started_at: None,
            ended_at: None,
            exit_reason: None,
            exit_detail: None,
            reaped_at: None,
        };
        let accepted = if runnable {
            "dispatch accepted; provisioning a worker"
        } else if backend.is_none() {
            "dispatch accepted; queued (no live env backend)"
        } else {
            "dispatch accepted; queued (no adapter for the agent)"
        };
        let event = ledger.push_event(
            &session.uid,
            "log",
            json!({"kind": "log", "payload": {"line": accepted}}),
        );
        let provision = runnable.then(|| StartRequest {
            session_uid: session.uid.clone(),
            template: dispatch.spec.template.clone(),
            repo: dispatch.spec.repo.clone(),
            isolation: dispatch.spec.isolation.clone(),
            git_ref: dispatch.spec.git_ref.clone(),
            fetch_remote: dispatch.spec.fetch_remote.unwrap_or(false),
            agent_cmd: dispatch.spec.agent_cmd.clone(),
            setup: dispatch.spec.setup.clone(),
            brief: dispatch.spec.brief.clone(),
            otel_endpoint: self.otel_endpoint_for(&env_kind),
        });
        // #28: resolve the lifecycle limits ONCE, here, off the immutable
        // dispatch row — the single site later stall/timeout/budget detectors
        // read rather than re-deriving limits ad hoc.
        let policy =
            LifecyclePolicy::resolve(dispatch.spec.timeout_secs, dispatch.spec.max_budget.clone());
        let mutations = vec![
            dispatch.mutation(),
            session_mutation(&session),
            event_mutation(&event),
        ];
        ledger.dispatches.push(dispatch);
        ledger.sessions.push(session.clone());
        ledger.mirror(mutations)?;
        drop(ledger);

        if let (Some(req), Some(backend), Some(adapter)) = (provision, backend, adapter) {
            let ledger = Arc::clone(&self.ledger);
            let observers = Arc::clone(&self.observers);
            let interval = self.observe_interval;
            std::thread::spawn(move || {
                provision_worker(ledger, backend, adapter, req, observers, interval, policy)
            });
        }
        Ok(session)
    }

    fn session(&self, uid: String) -> anyhow::Result<Option<Session>> {
        let ledger = self.ledger.lock().unwrap();
        Ok(ledger.sessions.iter().find(|s| s.uid == uid).cloned())
    }

    fn workspace_link(&self, session_uid: String) -> anyhow::Result<WorkspaceLink> {
        // The DTO carries the "no honest link" verdict rather than erroring, so
        // callers get one shape whether the backend is local, remote, or absent.
        let unavailable = |detail: String| WorkspaceLink {
            session_uid: session_uid.clone(),
            available: false,
            url: None,
            detail: Some(detail),
        };
        let (kind, handle) = {
            let ledger = self.ledger.lock().unwrap();
            let Some(session) = ledger.sessions.iter().find(|s| s.uid == session_uid) else {
                return Ok(unavailable(format!("no such session {session_uid}")));
            };
            (ledger.env_kind_of(&session_uid), session.env_handle.clone())
        };
        let (Some(kind), Some(handle)) = (kind, handle) else {
            return Ok(unavailable(format!(
                "session {session_uid} has no reachable worker to open"
            )));
        };
        let Some(backend) = self.backend_for(&kind) else {
            return Ok(unavailable(format!(
                "no live backend for environment kind '{kind}'"
            )));
        };
        let link = backend.compute(&handle).and_then(|c| c.workspace_link());
        match link {
            Ok(Some(url)) => Ok(WorkspaceLink {
                session_uid,
                available: true,
                url: Some(url),
                detail: None,
            }),
            Ok(None) => Ok(unavailable(format!(
                "this backend ('{kind}') has no local workspace path to open"
            ))),
            // A backend that tried and failed to resolve a link (e.g. the VM is
            // unreachable over ssh) surfaces its reason as honest detail rather
            // than erroring the op — one DTO shape for every outcome.
            Err(e) => Ok(unavailable(e.to_string())),
        }
    }

    fn sessions(&self, filter: Option<SessionFilter>) -> anyhow::Result<Vec<Session>> {
        let ledger = self.ledger.lock().unwrap();
        let filter = filter.unwrap_or_default();
        let env_dispatches: Option<Vec<&str>> = filter.env.as_deref().map(|env| {
            ledger
                .dispatches
                .iter()
                .filter(|d| d.spec.env == env)
                .map(|d| d.id.as_str())
                .collect()
        });
        Ok(ledger
            .sessions
            .iter()
            .filter(|s| filter.state.as_deref().is_none_or(|st| st == s.state))
            .filter(|s| {
                filter
                    .dispatch_id
                    .as_deref()
                    .is_none_or(|d| d == s.dispatch_id)
            })
            .filter(|s| {
                env_dispatches
                    .as_ref()
                    .is_none_or(|ids| ids.contains(&s.dispatch_id.as_str()))
            })
            .cloned()
            .collect())
    }

    fn events(
        &self,
        options: Option<EventOptions>,
        after: Option<i64>,
        limit: Option<u32>,
    ) -> anyhow::Result<Vec<Event>> {
        let ledger = self.ledger.lock().unwrap();
        let options = options.unwrap_or_default();
        // `after` is the count of matching items the caller has already seen —
        // stable because the event log is append-only and the filter is fixed.
        let skip = usize::try_from(after.unwrap_or(0).max(0)).unwrap_or(usize::MAX);
        let limit = limit.map(|l| l as usize).unwrap_or(DEFAULT_PAGE);
        Ok(ledger
            .events
            .iter()
            .filter(|e| {
                options
                    .session_uid
                    .as_deref()
                    .is_none_or(|u| u == e.session_uid)
            })
            .filter(|e| options.after_idx.is_none_or(|i| e.idx > i))
            .filter(|e| options.kinds.as_ref().is_none_or(|ks| ks.contains(&e.kind)))
            .skip(skip)
            .take(limit)
            .cloned()
            .collect())
    }

    /// The one messaging primitive (notes/manager-worker-comms.md §6); the body
    /// lives in [`messaging`] to keep this file under the size budget.
    fn send(
        &self,
        body: String,
        to: Option<SendTarget>,
        in_reply_to: Option<String>,
        topic: Option<String>,
    ) -> anyhow::Result<Vec<Message>> {
        messaging::send(self, body, to, in_reply_to, topic)
    }

    fn ack(&self, message_id: String) -> anyhow::Result<()> {
        messaging::ack(self, message_id)
    }

    fn messages(&self, filter: Option<MessagesFilter>) -> anyhow::Result<Vec<Message>> {
        messaging::messages(self, filter)
    }

    fn cancel(&self, session_uid: String) -> anyhow::Result<Session> {
        let mut ledger = self.ledger.lock().unwrap();
        let (backend, adapter) = self.routing(&ledger, &session_uid);
        let session = ledger.session_mut(&session_uid)?;
        let state = session.state.clone();
        if TERMINAL.contains(&state.as_str()) {
            bail!("session {session_uid} is already {state}");
        }
        // Stop the agent but keep the environment for inspection — reap deletes
        // it. Graceful-then-hard: the adapter interrupts the running work
        // (stop_work), then kills the process (stop_exec); the env stays either
        // way.
        if let (Some(backend), Some(adapter), Some(handle)) =
            (backend, adapter, session.env_handle.clone())
        {
            let stop = backend.compute(&handle).and_then(|c| {
                adapter.stop_work(&*c)?;
                adapter.stop_exec(&*c)
            });
            if let Err(e) = stop {
                let ev = ledger.push_event(
                    &session_uid,
                    "log",
                    json!({"kind": "log", "payload":
                        {"line": format!("stop agent (non-fatal): {e}")}}),
                );
                let _ = ledger.mirror(vec![event_mutation(&ev)]);
            }
        }
        let (session, event) = ledger.transition(&session_uid, "cancelled")?;
        ledger.mirror(vec![session_mutation(&session), event_mutation(&event)])?;
        self.observers.reap(&session_uid);
        Ok(session)
    }

    fn resume(&self, session_uid: String) -> anyhow::Result<Session> {
        let mut ledger = self.ledger.lock().unwrap();
        ledger.session_mut(&session_uid)?;
        bail!("resume isn't supported yet (re-dispatch instead; resumable envs are future work)")
    }

    fn reap(&self, session_uid: String) -> anyhow::Result<Session> {
        let mut ledger = self.ledger.lock().unwrap();
        let (backend, adapter) = self.routing(&ledger, &session_uid);
        let session = ledger.session_mut(&session_uid)?;
        if session.reaped_at.is_some() {
            bail!("session {session_uid} is already reaped");
        }
        let handle = session.env_handle.clone();
        let live = !TERMINAL.contains(&session.state.as_str());

        // Delivery assessment: while the env is still LIVE (before REAP destroys
        // the work dir), ask the backend whether the session shipped a diff. A
        // coarse backend that can't diff returns None → nothing recorded (honest
        // by omission). We only READ here; recording is deferred until after REAP
        // succeeds, so a failed-then-retried reap can't double-emit.
        // Detect-and-report only — no state change hangs off this.
        let delivery = match (backend.as_ref(), handle.as_ref()) {
            (Some(b), Some(h)) => b.delivery_signal(h),
            _ => None,
        };

        // Reap = the agent killed (if still live) THEN resources destroyed THEN
        // the row archived — a REAP failure errors out with the board unchanged,
        // so reap can be retried. A terminal session already had its process
        // stopped (cancel/lost), so skip stop_exec there.
        if let (Some(backend), Some(handle)) = (backend.as_ref(), handle.as_ref()) {
            if live {
                if let (Ok(compute), Some(adapter)) = (backend.compute(handle), adapter.as_ref()) {
                    let _ = adapter.stop_exec(&*compute);
                }
            }
            backend
                .reap(handle)
                .map_err(|e| anyhow!("reap {handle}: {e} (reap again to retry)"))?;
        }

        let mut mutations = Vec::new();
        // Now the env is gone: safe to commit the delivery verdict. "shipped" if
        // the work dir changed OR the session recorded any artifact; "empty"
        // otherwise. One derived event, plus a refinement of exit_detail that
        // APPENDS to (never clobbers) whatever the exit reason already set.
        if let Some(changed) = delivery {
            let artifacts = ledger
                .events
                .iter()
                .filter(|e| e.session_uid == session_uid && e.kind == "artifact")
                .count();
            let shipped = changed || artifacts > 0;
            let verdict = if shipped { "shipped" } else { "empty" };
            let ev = ledger.push_event_graded(
                &session_uid,
                "raw",
                "derived",
                json!({"kind": "raw", "payload": {"source": "delivery", "data": {
                    "worktree_changed": changed,
                    "artifacts": artifacts,
                    "verdict": verdict,
                }}}),
            );
            mutations.push(event_mutation(&ev));
            let note = if shipped {
                "delivery: shipped"
            } else {
                "delivery: empty diff, no artifacts"
            };
            let session = ledger.session_mut(&session_uid)?;
            session.exit_detail = Some(match session.exit_detail.take() {
                Some(prev) if !prev.is_empty() => format!("{prev}; {note}"),
                _ => note.to_string(),
            });
        }

        // Reap on a live session cancels it first — one call always clears the board.
        let session = ledger.session_mut(&session_uid)?;
        if !TERMINAL.contains(&session.state.as_str()) {
            let (_, event) = ledger.transition(&session_uid, "cancelled")?;
            mutations.push(event_mutation(&event));
        }
        let session = ledger.session_mut(&session_uid)?;
        session.reaped_at = Some(now());
        let snapshot = session.clone();
        mutations.push(session_mutation(&snapshot));
        ledger.mirror(mutations)?;
        self.observers.reap(&session_uid);
        Ok(snapshot)
    }

    /// Environments are the source of truth; the ledger is the cache. Per
    /// backend: confirm sessions whose workers still exist, mark the ones
    /// whose workers vanished `lost`, adopt discovered workers the ledger has
    /// never heard of (a previous disponent's), and tear down workers backing
    /// already-reaped sessions. A session whose kind has no backend here is
    /// left alone — we can't see its environment, so we don't judge it.
    fn reconcile(&self) -> anyhow::Result<ReconcileReport> {
        let mut report = ReconcileReport {
            adopted: 0,
            confirmed: 0,
            lost: 0,
            torn_down: 0,
        };
        // kind → (uid → discovered handle), surveyed outside the ledger lock
        let mut discovered: std::collections::HashMap<
            &str,
            std::collections::HashMap<String, serde_json::Value>,
        > = std::collections::HashMap::new();
        for b in &self.backends {
            discovered.insert(b.kind(), b.survey()?.into_iter().collect());
        }

        struct Row {
            uid: String,
            state: String,
            reaped: bool,
            kind: Option<String>,
            handle: Option<serde_json::Value>,
        }
        let mut ledger = self.ledger.lock().unwrap();
        let rows: Vec<Row> = ledger
            .sessions
            .iter()
            .map(|s| Row {
                uid: s.uid.clone(),
                state: s.state.clone(),
                reaped: s.reaped_at.is_some(),
                kind: ledger.env_kind_of(&s.uid),
                handle: s.env_handle.clone(),
            })
            .collect();
        let mut mutations = Vec::new();
        for row in &rows {
            let (Some(handle), Some(seen)) = (
                &row.handle,
                row.kind.as_deref().and_then(|k| discovered.get(k)),
            ) else {
                continue;
            };
            let exists = seen.contains_key(&row.uid);
            if row.reaped {
                if exists {
                    let backend = self.backend_for(row.kind.as_deref().unwrap()).unwrap();
                    if backend.reap(handle).is_ok() {
                        report.torn_down += 1;
                    }
                }
            } else if exists {
                report.confirmed += 1;
            } else if !TERMINAL.contains(&row.state.as_str()) {
                let (s, e) = ledger.transition(&row.uid, "lost")?;
                mutations.push(session_mutation(&s));
                mutations.push(event_mutation(&e));
                report.lost += 1;
            }
        }

        // Adoption: a discovered worker whose session the ledger doesn't know —
        // some earlier disponent dispatched it; it's ours now.
        for (kind, found) in &discovered {
            let Some(env_slug) = ledger
                .environments
                .iter()
                .find(|e| e.kind == *kind)
                .map(|e| e.slug.clone())
            else {
                continue;
            };
            for (session_uid, handle) in found {
                if ledger.sessions.iter().any(|s| &s.uid == session_uid) {
                    continue;
                }
                let dispatch = DispatchRow {
                    id: Uuid::now_v7().to_string(),
                    created_at: now(),
                    spec: serde_json::from_value(json!({
                        "brief": format!("[adopted] worker {handle} found in {env_slug}"),
                        "env": env_slug,
                    }))?,
                    agent: "claude-code".to_string(),
                    model: None,
                };
                // Reconstruct the attach descriptor from the surveyed handle:
                // local tmux → the (socket, session) pair; a holder → its socket +
                // uid; exe.dev has no url at survey time, so its terminal isn't
                // reachable here (ttyd url is not surveyed) → all null.
                let (transport, endpoint, target, attach_url) =
                    crate::local::attach_from_handle(handle, session_uid, None);
                let session = Session {
                    uid: session_uid.clone(),
                    dispatch_id: dispatch.id.clone(),
                    state: "running".to_string(),
                    env_handle: Some(handle.clone()),
                    attach_transport: transport,
                    attach_endpoint: endpoint,
                    attach_target: target,
                    attach_url,
                    url: None,
                    resumed_from: None,
                    started_at: None,
                    ended_at: None,
                    exit_reason: None,
                    exit_detail: None,
                    reaped_at: None,
                };
                let event = ledger.push_event(
                    session_uid,
                    "log",
                    json!({"kind": "log", "payload":
                        {"line": format!("adopted from {env_slug} ({handle})")}}),
                );
                mutations.push(dispatch.mutation());
                mutations.push(session_mutation(&session));
                mutations.push(event_mutation(&event));
                ledger.dispatches.push(dispatch);
                ledger.sessions.push(session);
                if let (Some(backend), Some(adapter)) =
                    (self.backend_for(kind), self.adapter_for("claude-code"))
                {
                    // An adopted worker carries no dispatch limits we can trust;
                    // policy is logged/stored only in PR-1, so a default is
                    // honest. Adoption assumes the claude-code agent (as the
                    // adopted dispatch row records).
                    watch_session(
                        &self.observers,
                        self.observe_interval,
                        backend,
                        adapter,
                        session_uid,
                        handle.clone(),
                        LifecyclePolicy::default(),
                    );
                }
                report.adopted += 1;
            }
        }
        ledger.mirror(mutations)?;
        Ok(report)
    }

    /// The full current state as an executable plan for any SQL dialect:
    /// CREATE TABLEs first, then catalog + ledger rows in dependency order.
    /// This is how consumers mirror disponent into their own store.
    fn driver_plan(
        &self,
        options: Option<DriverPlanOptions>,
        after: Option<i64>,
        limit: Option<u32>,
    ) -> anyhow::Result<Vec<Statement>> {
        let options = options.unwrap_or_default();
        let (dialect, ddl): (Dialect, &[TableSchema]) =
            match options.dialect.as_deref().unwrap_or("sqlite") {
                "sqlite" => (Dialect::Sqlite, SQLITE_TABLES),
                "postgres" => (Dialect::Postgres, PG_TABLES),
                "duckdb" => (Dialect::Duckdb, DUCKDB_TABLES),
                other => bail!("unknown dialect '{other}' (sqlite | postgres | duckdb)"),
            };
        let wanted = |table: &str| {
            options
                .tables
                .as_ref()
                .is_none_or(|ts| ts.iter().any(|t| t == table))
        };

        let mut statements: Vec<Statement> = ddl
            .iter()
            .filter(|t| wanted(t.name))
            .map(|t| Statement {
                sql: t.ddl.replace("__table__", t.name),
                params: serde_json::Value::Array(vec![]),
            })
            .collect();

        let ledger = self.ledger.lock().unwrap();
        let mut tx = catalog::seed_tx();
        // live rows replace the seed's environments (same table, later wins on upsert)
        for env in &ledger.environments {
            tx.mutations.push(environment_mutation(env));
        }
        for d in &ledger.dispatches {
            tx.mutations.push(d.mutation());
        }
        for s in &ledger.sessions {
            tx.mutations.push(session_mutation(s));
        }
        for e in &ledger.events {
            tx.mutations.push(event_mutation(e));
        }
        for m in &ledger.messages {
            tx.mutations.push(messaging::message_mutation(m));
        }
        tx.mutations.retain(|m| wanted(&m.table));

        let plan = crate::sink::codec(dialect)?
            .plan(&tx)
            .map_err(|e| anyhow!("driver plan: {e}"))?;
        statements.extend(plan.steps.iter().flat_map(|step| {
            step.statements.iter().map(|s| Statement {
                sql: s.sql.clone(),
                params: serde_json::Value::Array(s.params.clone()),
            })
        }));

        let skip = usize::try_from(after.unwrap_or(0).max(0)).unwrap_or(usize::MAX);
        let limit = limit.map(|l| l as usize).unwrap_or(DEFAULT_PAGE);
        Ok(statements.into_iter().skip(skip).take(limit).collect())
    }
}

// The derive belongs on the structs, but those are generated (mcp_generated.rs
// is fluessig output we don't edit), so the impls live here by hand.
#[allow(clippy::derivable_impls)]
impl Default for SessionFilter {
    fn default() -> Self {
        SessionFilter {
            env: None,
            state: None,
            dispatch_id: None,
        }
    }
}

#[allow(clippy::derivable_impls)]
impl Default for EventOptions {
    fn default() -> Self {
        EventOptions {
            session_uid: None,
            after_idx: None,
            kinds: None,
        }
    }
}

#[allow(clippy::derivable_impls)]
impl Default for DriverPlanOptions {
    fn default() -> Self {
        DriverPlanOptions {
            dialect: None,
            tables: None,
        }
    }
}

mod messaging;
mod watch;

#[cfg(test)]
mod tests;
