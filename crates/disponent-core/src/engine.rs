//! The engine: an in-memory ledger behind the generated `DisponentMcp` trait,
//! mirrored into the sink (SQLite by default) as fluessig plans.
//!
//! Dispatch routes by environment kind to a registered backend (exe.dev VMs,
//! local tmux) and provisions on a background thread; a kind with no backend
//! queues honestly. Environments stay the source of truth — the ledger is the
//! reconciled cache, and `reconcile()` confirms/loses/adopts against each
//! backend's survey. Ops a version can't do yet (resume) say so instead of
//! pretending.

use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail};
use chrono::{SecondsFormat, Utc};
use fluessig::data::{Mutation, Transaction};
use fluessig::sql::Dialect;
use serde_json::json;
use uuid::Uuid;

use crate::backend::{EnvBackend, ExeDev, ProvisionRequest};
use crate::catalog::{self, upsert};
use crate::local::LocalTmux;
use crate::mcp_generated::{
    DispatchSpec, DriverPlanOptions, Environment, Event, EventOptions, ReconcileReport, Session,
    SessionFilter, Statement,
};
use crate::schema_gen::{TableSchema, DUCKDB_TABLES, PG_TABLES, SQLITE_TABLES};
use crate::sink::Sink;

/// Session states with no way forward — reap archives them, nothing revives them.
const TERMINAL: &[&str] = &["completed", "failed", "cancelled", "lost"];

/// Page size for the stream cursors when the caller doesn't pass `limit`.
const DEFAULT_PAGE: usize = 100;

pub struct Engine {
    ledger: Arc<Mutex<Ledger>>,
    /// One backend per environment kind; a kind with no backend queues honestly.
    backends: Vec<Arc<dyn EnvBackend>>,
}

#[derive(Default)]
struct Ledger {
    environments: Vec<Environment>,
    dispatches: Vec<DispatchRow>,
    sessions: Vec<Session>,
    /// Append-only, in observation order across all sessions — the stream the
    /// `events` cursor pages over (`after` = items already consumed).
    events: Vec<Event>,
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

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine {
    /// Memory-only, no backends (tests, throwaways). `open` is the front door.
    pub fn new() -> Self {
        Engine {
            ledger: Arc::new(Mutex::new(Ledger {
                environments: catalog::environments(),
                ..Ledger::default()
            })),
            backends: Vec::new(),
        }
    }

    /// An engine over the shipped catalog with the real backends (exe.dev +
    /// local tmux). `sink`: `None` = the managed SQLite file (~/.disponent),
    /// `"none"` = memory only, anything else = a SQLite path.
    pub fn open(sink: Option<&str>) -> anyhow::Result<Self> {
        Engine::open_with(
            sink,
            vec![
                Arc::new(ExeDev::from_env()),
                Arc::new(LocalTmux::from_env()),
            ],
        )
    }

    /// The composable front door: any sink spec, any backend set.
    pub fn open_with(
        sink: Option<&str>,
        backends: Vec<Arc<dyn EnvBackend>>,
    ) -> anyhow::Result<Self> {
        let mut sink = Sink::open(sink)?;
        sink.apply(&catalog::seed_tx())?;
        Ok(Engine {
            ledger: Arc::new(Mutex::new(Ledger {
                environments: catalog::environments(),
                sink,
                ..Ledger::default()
            })),
            backends,
        })
    }

    /// Memory-only over one injected backend (the dry-run tests' front door).
    pub fn with_backend<B: EnvBackend + 'static>(backend: B) -> Self {
        Engine {
            backends: vec![Arc::new(backend)],
            ..Engine::new()
        }
    }

    fn backend_for(&self, kind: &str) -> Option<Arc<dyn EnvBackend>> {
        self.backends.iter().find(|b| b.kind() == kind).cloned()
    }
}

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
                "template_name",
                "setup",
                "env_slug",
                "agent_name",
                "model_id",
                "timeout_secs",
                "max_budget",
                "via_mcp_depth",
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
                json!(self.spec.template),
                json!(self.spec.setup),
                json!(self.spec.env),
                json!(self.agent),
                json!(self.model),
                json!(self.spec.timeout_secs),
                json!(self.spec.max_budget),
                json!(0),
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
            fidelity: "exact".to_string(),
            payload,
        };
        self.events.push(event.clone());
        event
    }

    fn transition(&mut self, uid: &str, to: &str) -> anyhow::Result<(Session, Event)> {
        let from = self.session_mut(uid)?.state.clone();
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
}

/// The background half of a backed dispatch: provision the worker, then flip
/// the session to running (or failed) — unless someone cancelled/reaped it
/// mid-provision, in which case the fresh worker is torn down, not adopted.
fn provision_worker(
    ledger: Arc<Mutex<Ledger>>,
    backend: Arc<dyn EnvBackend>,
    req: ProvisionRequest,
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

    match backend.provision(&req) {
        Ok(p) => {
            let mut l = ledger.lock().unwrap();
            let Ok(session) = l.session_mut(&uid) else {
                let _ = backend.teardown(&p.handle);
                return;
            };
            if session.state != "provisioning" {
                let _ = backend.teardown(&p.handle);
                return;
            }
            session.state = "running".to_string();
            session.started_at = Some(now());
            session.env_handle = Some(p.handle.clone());
            session.url = p.url.clone();
            let snapshot = session.clone();
            let state_ev = l.push_event(
                &uid,
                "state",
                json!({"kind": "state", "payload": {"from": "provisioning", "to": "running"}}),
            );
            let log_ev = l.push_event(
                &uid,
                "log",
                json!({"kind": "log", "payload":
                    {"line": format!("worker up: {}{}", p.handle,
                        p.url.as_deref().map(|u| format!(" ({u})")).unwrap_or_default())}}),
            );
            let _ = l.mirror(vec![
                session_mutation(&snapshot),
                event_mutation(&state_ev),
                event_mutation(&log_ev),
            ]);
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
            url: None,
            resumed_from: None,
            started_at: None,
            ended_at: None,
            exit_reason: None,
            exit_detail: None,
            reaped_at: None,
        };
        let accepted = if backend.is_some() {
            "dispatch accepted; provisioning a worker"
        } else {
            "dispatch accepted; queued (no live env backend)"
        };
        let event = ledger.push_event(
            &session.uid,
            "log",
            json!({"kind": "log", "payload": {"line": accepted}}),
        );
        let provision = backend.is_some().then(|| ProvisionRequest {
            session_uid: session.uid.clone(),
            template: dispatch.spec.template.clone(),
            repo: dispatch.spec.repo.clone(),
            setup: dispatch.spec.setup.clone(),
            brief: dispatch.spec.brief.clone(),
        });
        let mutations = vec![
            dispatch.mutation(),
            session_mutation(&session),
            event_mutation(&event),
        ];
        ledger.dispatches.push(dispatch);
        ledger.sessions.push(session.clone());
        ledger.mirror(mutations)?;
        drop(ledger);

        if let (Some(req), Some(backend)) = (provision, backend) {
            let ledger = Arc::clone(&self.ledger);
            std::thread::spawn(move || provision_worker(ledger, backend, req));
        }
        Ok(session)
    }

    fn session(&self, uid: String) -> anyhow::Result<Option<Session>> {
        let ledger = self.ledger.lock().unwrap();
        Ok(ledger.sessions.iter().find(|s| s.uid == uid).cloned())
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

    fn send(&self, session_uid: String, input: String) -> anyhow::Result<()> {
        let (state, handle, backend) = {
            let mut ledger = self.ledger.lock().unwrap();
            let kind = ledger.env_kind_of(&session_uid);
            let session = ledger.session_mut(&session_uid)?;
            (
                session.state.clone(),
                session.env_handle.clone(),
                kind.and_then(|k| self.backend_for(&k)),
            )
        };
        if state != "running" {
            bail!("can't send to session {session_uid}: state is {state}, not running");
        }
        let (Some(backend), Some(handle)) = (backend, handle) else {
            bail!("session {session_uid} has no reachable worker");
        };
        // The env round-trip happens outside the ledger lock; the event records after.
        backend.send(&handle, &input)?;
        let mut ledger = self.ledger.lock().unwrap();
        let event = ledger.push_event(
            &session_uid,
            "message",
            json!({"kind": "message", "payload": {"role": "supervisor", "text": input}}),
        );
        ledger.mirror(vec![event_mutation(&event)])?;
        Ok(())
    }

    fn cancel(&self, session_uid: String) -> anyhow::Result<Session> {
        let mut ledger = self.ledger.lock().unwrap();
        let kind = ledger.env_kind_of(&session_uid);
        let session = ledger.session_mut(&session_uid)?;
        let state = session.state.clone();
        if TERMINAL.contains(&state.as_str()) {
            bail!("session {session_uid} is already {state}");
        }
        // Stop the agent but keep the environment for inspection — reap deletes it.
        let backend = kind.and_then(|k| self.backend_for(&k));
        if let (Some(backend), Some(handle)) = (backend, session.env_handle.clone()) {
            if let Err(e) = backend.stop(&handle) {
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
        Ok(session)
    }

    fn resume(&self, session_uid: String) -> anyhow::Result<Session> {
        let mut ledger = self.ledger.lock().unwrap();
        ledger.session_mut(&session_uid)?;
        bail!("resume isn't supported yet (re-dispatch instead; resumable envs are future work)")
    }

    fn reap(&self, session_uid: String) -> anyhow::Result<Session> {
        let mut ledger = self.ledger.lock().unwrap();
        let kind = ledger.env_kind_of(&session_uid);
        let session = ledger.session_mut(&session_uid)?;
        if session.reaped_at.is_some() {
            bail!("session {session_uid} is already reaped");
        }
        // Reap = resources torn down THEN the row archived — a teardown failure
        // errors out with the board unchanged, so reap can be retried.
        let backend = kind.and_then(|k| self.backend_for(&k));
        if let (Some(backend), Some(handle)) = (backend, session.env_handle.clone()) {
            backend
                .teardown(&handle)
                .map_err(|e| anyhow!("teardown {handle}: {e} (reap again to retry)"))?;
        }
        // Reap on a live session cancels it first — one call always clears the board.
        let session = ledger.session_mut(&session_uid)?;
        let mut mutations = Vec::new();
        if !TERMINAL.contains(&session.state.as_str()) {
            let (_, event) = ledger.transition(&session_uid, "cancelled")?;
            mutations.push(event_mutation(&event));
        }
        let session = ledger.session_mut(&session_uid)?;
        session.reaped_at = Some(now());
        let snapshot = session.clone();
        mutations.push(session_mutation(&snapshot));
        ledger.mirror(mutations)?;
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
                    if backend.teardown(handle).is_ok() {
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
                let session = Session {
                    uid: session_uid.clone(),
                    dispatch_id: dispatch.id.clone(),
                    state: "running".to_string(),
                    env_handle: Some(handle.clone()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp_generated::DisponentMcp;

    fn spec(env: &str) -> DispatchSpec {
        serde_json::from_value(json!({"brief": "do the thing", "env": env})).unwrap()
    }

    #[test]
    fn dispatch_resolves_defaults_and_queues() {
        let engine = Engine::new();
        let session = engine.dispatch(spec("exe-dev")).unwrap();
        assert_eq!(session.state, "queued");
        assert!(session.reaped_at.is_none());
        let fetched = engine.session(session.uid.clone()).unwrap().unwrap();
        assert_eq!(fetched.dispatch_id, session.dispatch_id);
    }

    #[test]
    fn dispatch_rejects_unknown_env_and_uncatalogued_combos() {
        let engine = Engine::new();
        assert!(engine.dispatch(spec("nonesuch")).is_err());

        let mut bad = spec("local");
        bad.agent = Some("codex".into());
        assert!(engine.dispatch(bad.clone()).is_err());
        bad.unchecked = Some(true);
        assert_eq!(engine.dispatch(bad).unwrap().state, "queued");
    }

    #[test]
    fn cancel_then_reap_walks_the_lifecycle() {
        let engine = Engine::new();
        let s = engine.dispatch(spec("local")).unwrap();
        let cancelled = engine.cancel(s.uid.clone()).unwrap();
        assert_eq!(cancelled.state, "cancelled");
        assert!(cancelled.ended_at.is_some());
        assert!(engine.cancel(s.uid.clone()).is_err(), "already terminal");
        let reaped = engine.reap(s.uid.clone()).unwrap();
        assert!(reaped.reaped_at.is_some());
        assert!(engine.reap(s.uid).is_err(), "already reaped");
    }

    #[test]
    fn reap_on_a_live_session_cancels_first() {
        let engine = Engine::new();
        let s = engine.dispatch(spec("local")).unwrap();
        let reaped = engine.reap(s.uid).unwrap();
        assert_eq!(reaped.state, "cancelled");
        assert!(reaped.reaped_at.is_some());
    }

    #[test]
    fn events_filter_and_cursor_page() {
        let engine = Engine::new();
        let a = engine.dispatch(spec("local")).unwrap();
        let b = engine.dispatch(spec("exe-dev")).unwrap();
        engine.cancel(b.uid.clone()).unwrap();

        // b has a log + a state event; a has just the log
        let only_b: EventOptions = serde_json::from_value(json!({"sessionUid": b.uid})).unwrap();
        let events = engine.events(Some(only_b.clone()), None, None).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].kind, "state");
        assert_eq!(events[1].idx, 1, "the DTO carries its timeline position");
        assert_eq!(events[1].payload["payload"]["to"], "cancelled");

        // cursor: after=1 means "I've seen one", so only the state event remains
        let page = engine.events(Some(only_b), Some(1), Some(10)).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].kind, "state");

        let a_events = engine
            .events(
                Some(serde_json::from_value(json!({"sessionUid": a.uid})).unwrap()),
                None,
                None,
            )
            .unwrap();
        assert_eq!(a_events.len(), 1);
        assert_eq!(a_events[0].kind, "log");
    }

    #[test]
    fn sessions_filter_by_env_and_state() {
        let engine = Engine::new();
        engine.dispatch(spec("local")).unwrap();
        let b = engine.dispatch(spec("exe-dev")).unwrap();
        engine.cancel(b.uid).unwrap();

        let by_env = |env: &str| -> Vec<Session> {
            engine
                .sessions(Some(serde_json::from_value(json!({"env": env})).unwrap()))
                .unwrap()
        };
        assert_eq!(by_env("local").len(), 1);
        assert_eq!(by_env("exe-dev").len(), 1);
        assert_eq!(by_env("exe-dev")[0].state, "cancelled");

        let queued = engine
            .sessions(Some(
                serde_json::from_value(json!({"state": "queued"})).unwrap(),
            ))
            .unwrap();
        assert_eq!(queued.len(), 1);
    }

    #[test]
    fn driver_plan_emits_ddl_then_ordered_rows_and_pages() {
        let engine = Engine::new();
        let s = engine.dispatch(spec("local")).unwrap();
        engine.cancel(s.uid).unwrap();

        let all = engine.driver_plan(None, None, Some(1000)).unwrap();
        let creates = all
            .iter()
            .filter(|s| s.sql.starts_with("CREATE TABLE"))
            .count();
        assert_eq!(creates, SQLITE_TABLES.len(), "DDL for every table");
        // rows follow dependency order: the dispatch's upsert before the session's
        let pos = |needle: &str| all.iter().position(|s| s.sql.contains(needle)).unwrap();
        assert!(pos("INSERT INTO \"dispatches\"") < pos("INSERT INTO \"sessions\""));
        assert!(pos("INSERT INTO \"sessions\"") < pos("INSERT INTO \"events\""));

        // the cursor pages the same sequence
        let (a, b) = (
            engine.driver_plan(None, None, Some(5)).unwrap(),
            engine.driver_plan(None, Some(5), Some(1000)).unwrap(),
        );
        assert_eq!(a.len(), 5);
        assert_eq!(a.last().unwrap().sql, all[4].sql);
        assert_eq!(b.first().unwrap().sql, all[5].sql);
        assert_eq!(a.len() + b.len(), all.len());

        // postgres flavor speaks $n placeholders
        let pg_opts: DriverPlanOptions =
            serde_json::from_value(json!({"dialect": "postgres"})).unwrap();
        let pg = engine.driver_plan(Some(pg_opts), None, Some(1000)).unwrap();
        assert!(pg.iter().any(|s| s.sql.contains("$1")));

        assert!(engine
            .driver_plan(
                Some(serde_json::from_value(json!({"dialect": "mongodb"})).unwrap()),
                None,
                None,
            )
            .is_err());
    }
}
