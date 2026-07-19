//! The hand-written half of the binding: `DisponentCore` (the generated napi
//! trait) implemented over the engine. The engine speaks the MCP-layer DTOs
//! (wire strings, serde_json values); the napi layer speaks typed enums and
//! JSON-as-string carriers — this file is the seam that converts between
//! them, plus the two poll-stream dressings (events, driverPlan).
//!
//! straitjacket-allow-file:duplication — the per-binding core_impl seam is
//! deliberately parallel across the node/python/ruby bindings.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use disponent_core::mcp_generated::{self as mcp, DisponentMcp};
use disponent_core::Engine;

use crate::generated::*;
// The shared streaming contract (`Poll`/`PollStream`) the poll-stream dressings
// below reference bare; generated.rs imports it privately from the fluessig-runtime
// crate, so the glob above doesn't re-export it — bring it into scope explicitly.
use fluessig_runtime::{Poll, PollStream};

/// wire string ↔ napi enum, one macro per vocabulary (the enums are generated
/// numeric napi enums; the engine stores the tsp wire strings).
macro_rules! wire_enum {
    ($ty:ty { $($variant:ident => $wire:literal),+ $(,)? }) => {
        impl WireEnum for $ty {
            fn to_wire(self) -> &'static str {
                match self { $(<$ty>::$variant => $wire),+ }
            }
            fn from_wire(s: &str) -> anyhow::Result<Self> {
                match s {
                    $($wire => Ok(<$ty>::$variant),)+
                    other => bail!("unknown {}: {other}", stringify!($ty)),
                }
            }
        }
    };
}

pub(crate) trait WireEnum: Sized {
    fn to_wire(self) -> &'static str;
    fn from_wire(s: &str) -> anyhow::Result<Self>;
}

wire_enum!(SessionState {
    Queued => "queued", Provisioning => "provisioning", Running => "running",
    NeedsInput => "needs_input", Completed => "completed", Failed => "failed",
    Cancelled => "cancelled", Lost => "lost",
});
wire_enum!(EnvKind {
    Local => "local", ExeDev => "exe_dev", Modal => "modal",
    ClaudeCodeWeb => "claude_code_web", Custom => "custom",
});
wire_enum!(ExitReason {
    Ok => "ok", Error => "error", Signal => "signal", Timeout => "timeout",
    Budget => "budget", Setup => "setup", Unknown => "unknown",
});
wire_enum!(EventKind {
    State => "state", Message => "message", ToolCall => "tool_call",
    ToolResult => "tool_result", Log => "log", Usage => "usage",
    Artifact => "artifact", Raw => "raw", Mail => "mail",
});
wire_enum!(Party { Manager => "manager", Worker => "worker", User => "user" });
wire_enum!(Fidelity { Exact => "exact", Derived => "derived", Scraped => "scraped" });
wire_enum!(IsolationKind {
    None => "none", Worktree => "worktree", Container => "container", Vm => "vm",
});
wire_enum!(CapabilityKind {
    Dispatch => "dispatch", Interact => "interact", ObserveStream => "observe_stream",
    ObservePoll => "observe_poll", ListSessions => "list_sessions", Resume => "resume",
    Cancel => "cancel", Teardown => "teardown", IsolationWorktree => "isolation_worktree",
    IsolationContainer => "isolation_container", IsolationVm => "isolation_vm",
    Templates => "templates", ArtifactFetch => "artifact_fetch", UsageReport => "usage_report",
});

// ── engine DTO → napi DTO ──

fn session_out(s: mcp::Session) -> anyhow::Result<Session> {
    Ok(Session {
        uid: s.uid,
        dispatch_id: s.dispatch_id,
        state: SessionState::from_wire(&s.state)?,
        env_handle: s.env_handle.map(|v| v.to_string()),
        attach_tmux_socket: s.attach_tmux_socket,
        attach_tmux_session: s.attach_tmux_session,
        url: s.url,
        resumed_from: s.resumed_from,
        started_at: s.started_at,
        ended_at: s.ended_at,
        exit_reason: s
            .exit_reason
            .as_deref()
            .map(ExitReason::from_wire)
            .transpose()?,
        exit_detail: s.exit_detail,
        reaped_at: s.reaped_at,
    })
}

fn event_out(e: mcp::Event) -> anyhow::Result<Event> {
    Ok(Event {
        session_uid: e.session_uid,
        idx: e.idx,
        ts: e.ts,
        kind: EventKind::from_wire(&e.kind)?,
        fidelity: Fidelity::from_wire(&e.fidelity)?,
        payload: e.payload.to_string(),
    })
}

fn workspace_link_out(w: mcp::WorkspaceLink) -> WorkspaceLink {
    WorkspaceLink {
        session_uid: w.session_uid,
        available: w.available,
        url: w.url,
        detail: w.detail,
    }
}

fn environment_out(e: mcp::Environment) -> anyhow::Result<Environment> {
    Ok(Environment {
        slug: e.slug,
        kind: EnvKind::from_wire(&e.kind)?,
        display_name: e.display_name,
        endpoint: e.endpoint,
        last_probed_at: e.last_probed_at,
    })
}

fn offering_out(o: mcp::Offering) -> Offering {
    Offering {
        env_slug: o.env_slug,
        agent_name: o.agent_name,
        model_id: o.model_id,
        is_default: o.is_default,
    }
}

fn env_capability_out(c: mcp::EnvCapability) -> anyhow::Result<EnvCapability> {
    Ok(EnvCapability {
        env_slug: c.env_slug,
        capability: CapabilityKind::from_wire(&c.capability)?,
        detail: c.detail.map(|v| v.to_string()),
    })
}

fn message_out(m: mcp::Message) -> anyhow::Result<Message> {
    Ok(Message {
        id: m.id,
        created_at: m.created_at,
        sender: Party::from_wire(&m.sender)?,
        recipient: Party::from_wire(&m.recipient)?,
        session_uid: m.session_uid,
        body: m.body,
        in_reply_to: m.in_reply_to,
        fanout_id: m.fanout_id,
        topic: m.topic,
        acked_at: m.acked_at,
    })
}

// ── napi DTO → engine DTO ──

fn send_target_in(t: SendTarget) -> mcp::SendTarget {
    mcp::SendTarget {
        tags: t.tags,
        sessions: t.sessions,
        user: t.user,
    }
}

fn messages_filter_in(f: MessagesFilter) -> mcp::MessagesFilter {
    mcp::MessagesFilter {
        fanout_id: f.fanout_id,
        recipient: f.recipient.map(|p| p.to_wire().to_string()),
        session_uid: f.session_uid,
        topic: f.topic,
        latest_per_topic: f.latest_per_topic,
    }
}

fn spec_in(spec: DispatchSpec) -> anyhow::Result<mcp::DispatchSpec> {
    Ok(mcp::DispatchSpec {
        brief: spec.brief,
        env: spec.env,
        agent: spec.agent,
        model: spec.model,
        title: spec.title,
        repo: spec.repo,
        git_ref: spec.git_ref,
        isolation: spec.isolation.map(|i| i.to_wire().to_string()),
        fetch_remote: spec.fetch_remote,
        template: spec.template,
        setup: spec.setup,
        timeout_secs: spec.timeout_secs,
        max_budget: spec.max_budget,
        unchecked: spec.unchecked,
        tags: spec.tags,
        labels: spec
            .labels
            .map(|raw| serde_json::from_str(&raw).context("labels: not valid JSON"))
            .transpose()?,
    })
}

fn filter_in(f: SessionFilter) -> mcp::SessionFilter {
    mcp::SessionFilter {
        env: f.env,
        state: f.state.map(|s| s.to_wire().to_string()),
        dispatch_id: f.dispatch_id,
    }
}

fn event_options_in(o: EventOptions) -> mcp::EventOptions {
    mcp::EventOptions {
        session_uid: o.session_uid,
        after_idx: o.after_idx,
        kinds: o
            .kinds
            .map(|ks| ks.into_iter().map(|k| k.to_wire().to_string()).collect()),
    }
}

// ── the core ──

pub struct DisponentImpl {
    pub(crate) engine: Arc<Engine>,
}

impl DisponentCore for DisponentImpl {
    fn open(options: Option<OpenOptions>) -> anyhow::Result<Self> {
        let options = options.unwrap_or(OpenOptions {
            config_path: None,
            sink: None,
        });
        if options.config_path.is_some() {
            bail!("configPath isn't supported yet — environments come from the shipped catalog");
        }
        Ok(DisponentImpl {
            engine: Arc::new(Engine::open(options.sink.as_deref())?),
        })
    }

    fn environments(&self) -> anyhow::Result<Vec<Environment>> {
        self.engine
            .environments()?
            .into_iter()
            .map(environment_out)
            .collect()
    }

    fn offerings(&self) -> anyhow::Result<Vec<Offering>> {
        Ok(self
            .engine
            .offerings()?
            .into_iter()
            .map(offering_out)
            .collect())
    }

    fn capabilities(&self) -> anyhow::Result<Vec<EnvCapability>> {
        self.engine
            .capabilities()?
            .into_iter()
            .map(env_capability_out)
            .collect()
    }

    fn refresh(&self, env_slug: Option<String>) -> anyhow::Result<Vec<Environment>> {
        self.engine
            .refresh(env_slug)?
            .into_iter()
            .map(environment_out)
            .collect()
    }

    fn dispatch(&self, spec: DispatchSpec) -> anyhow::Result<Session> {
        session_out(self.engine.dispatch(spec_in(spec)?)?)
    }

    fn session(&self, uid: String) -> anyhow::Result<Option<Session>> {
        DisponentMcp::session(self.engine.as_ref(), uid)?
            .map(session_out)
            .transpose()
    }

    fn sessions(&self, filter: Option<SessionFilter>) -> anyhow::Result<Vec<Session>> {
        self.engine
            .sessions(filter.map(filter_in))?
            .into_iter()
            .map(session_out)
            .collect()
    }

    fn workspace_link(&self, session_uid: String) -> anyhow::Result<WorkspaceLink> {
        Ok(workspace_link_out(DisponentMcp::workspace_link(
            self.engine.as_ref(),
            session_uid,
        )?))
    }

    fn events(&self, options: Option<EventOptions>) -> anyhow::Result<Box<dyn PollStream<Event>>> {
        Ok(Box::new(EventStream {
            engine: Arc::clone(&self.engine),
            options: options.map(event_options_in),
            cursor: Mutex::new(0),
            buffer: Mutex::new(VecDeque::new()),
        }))
    }

    fn send(
        &self,
        body: String,
        to: Option<SendTarget>,
        in_reply_to: Option<String>,
        topic: Option<String>,
    ) -> anyhow::Result<Vec<Message>> {
        self.engine
            .send(body, to.map(send_target_in), in_reply_to, topic)?
            .into_iter()
            .map(message_out)
            .collect()
    }

    fn ack(&self, message_id: String) -> anyhow::Result<()> {
        self.engine.ack(message_id)
    }

    fn messages(&self, filter: Option<MessagesFilter>) -> anyhow::Result<Vec<Message>> {
        self.engine
            .messages(filter.map(messages_filter_in))?
            .into_iter()
            .map(message_out)
            .collect()
    }

    fn cancel(&self, session_uid: String) -> anyhow::Result<Session> {
        session_out(self.engine.cancel(session_uid)?)
    }

    fn resume(&self, session_uid: String) -> anyhow::Result<Session> {
        session_out(self.engine.resume(session_uid)?)
    }

    fn reap(&self, session_uid: String) -> anyhow::Result<Session> {
        session_out(self.engine.reap(session_uid)?)
    }

    fn reconcile(&self) -> anyhow::Result<ReconcileReport> {
        let r = DisponentMcp::reconcile(self.engine.as_ref())?;
        Ok(ReconcileReport {
            adopted: r.adopted,
            confirmed: r.confirmed,
            lost: r.lost,
            torn_down: r.torn_down,
        })
    }

    fn driver_plan(
        &self,
        options: Option<DriverPlanOptions>,
    ) -> anyhow::Result<Box<dyn PollStream<Statement>>> {
        // A plan is finite — materialize it up front, page by page, so the
        // stream just drains (Closed at the end, unlike the endless events).
        let options = options.map(|o| mcp::DriverPlanOptions {
            dialect: o.dialect,
            tables: o.tables,
        });
        let mut items = VecDeque::new();
        loop {
            let page =
                self.engine
                    .driver_plan(options.clone(), Some(items.len() as i64), Some(500))?;
            let got = page.len();
            items.extend(page);
            if got < 500 {
                break;
            }
        }
        Ok(Box::new(PlanStream {
            items: Mutex::new(items),
        }))
    }
}

/// The session-event feed: page the engine's cursor, block up to `timeout`
/// when nothing is ready. Never closes — sessions run until reaped, and the
/// feed outlives any one of them.
struct EventStream {
    engine: Arc<Engine>,
    options: Option<mcp::EventOptions>,
    cursor: Mutex<i64>,
    buffer: Mutex<VecDeque<mcp::Event>>,
}

impl PollStream<Event> for EventStream {
    fn poll(&self, timeout: Duration) -> Poll<Event> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(e) = self.buffer.lock().unwrap().pop_front() {
                match event_out(e) {
                    Ok(v) => return Poll::Item(v),
                    Err(_) => continue, // engine vocabulary is ours; unreachable
                }
            }
            let after = *self.cursor.lock().unwrap();
            let page = DisponentMcp::events(
                self.engine.as_ref(),
                self.options.clone(),
                Some(after),
                Some(64),
            )
            .unwrap_or_default();
            if page.is_empty() {
                if Instant::now() >= deadline {
                    return Poll::Idle;
                }
                std::thread::sleep(Duration::from_millis(50));
            } else {
                *self.cursor.lock().unwrap() = after + page.len() as i64;
                self.buffer.lock().unwrap().extend(page);
            }
        }
    }
}

struct PlanStream {
    items: Mutex<VecDeque<mcp::Statement>>,
}

impl PollStream<Statement> for PlanStream {
    fn poll(&self, _timeout: Duration) -> Poll<Statement> {
        match self.items.lock().unwrap().pop_front() {
            Some(s) => Poll::Item(Statement {
                sql: s.sql,
                params: s.params.to_string(),
            }),
            None => Poll::Closed,
        }
    }
}

/// Is this napi-level state one reap can't revive?
pub(crate) fn is_terminal(state: SessionState) -> bool {
    matches!(
        state,
        SessionState::Completed
            | SessionState::Failed
            | SessionState::Cancelled
            | SessionState::Lost
    )
}
