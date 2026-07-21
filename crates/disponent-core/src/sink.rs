//! The default sink: a managed local SQLite file, kept in step with the ledger
//! by replaying fluessig plans (SqlCodec compiles typed mutations into ordered
//! `{sql, params}`; we execute each plan step in one SQLite transaction).
//!
//! "Managed" means disponent owns the file's lifecycle and schema; `sink:
//! "none"` opts out (memory only), any other value is taken as a SQLite path.
//! Non-SQL stores ride the same Mutation stream someday (fluessig#11).

use anyhow::{anyhow, Context};
use fluessig::data::{SqlCodec, Transaction};
use fluessig::sql::Dialect;
use rusqlite::Connection;
use serde_json::json;

use crate::mcp_generated::{DispatchSpec, Environment, Event, Message, Session};
use crate::schema_gen::SQLITE_TABLES;

/// The emitted catalog — the same source the generated code came from, loaded
/// at runtime for SqlCodec (statement generation + topological ordering).
pub const CATALOG_JSON: &str = include_str!("../../../schema/catalog.json");

#[derive(Default)]
pub enum Sink {
    #[default]
    None,
    Sqlite {
        conn: Connection,
        codec: SqlCodec,
    },
}

/// Where the managed file lives when the caller doesn't say: ~/.disponent/.
fn managed_path() -> anyhow::Result<std::path::PathBuf> {
    let home = std::env::var_os("HOME").ok_or_else(|| anyhow!("HOME is unset"))?;
    let dir = std::path::Path::new(&home).join(".disponent");
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    Ok(dir.join("disponent.sqlite3"))
}

pub fn codec(dialect: Dialect) -> anyhow::Result<SqlCodec> {
    let catalog = fluessig::load_catalog(CATALOG_JSON)
        .map_err(|e| anyhow!("embedded catalog.json: {e:?}"))?;
    Ok(SqlCodec::new(&catalog, dialect))
}

impl Sink {
    /// `None` = the managed default; `"none"` = memory only; anything else is
    /// a SQLite file path.
    pub fn open(spec: Option<&str>) -> anyhow::Result<Sink> {
        let path = match spec {
            Some("none") => return Ok(Sink::None),
            Some(p) => std::path::PathBuf::from(p),
            None => managed_path()?,
        };
        let conn =
            Connection::open(&path).with_context(|| format!("opening sink {}", path.display()))?;
        for t in SQLITE_TABLES {
            conn.execute_batch(&t.ddl.replace("__table__", t.name))
                .with_context(|| format!("creating table {}", t.name))?;
        }
        Ok(Sink::Sqlite {
            conn,
            codec: codec(Dialect::Sqlite)?,
        })
    }

    /// Replay one transaction into the mirror. Each plan step runs inside its
    /// own SQLite transaction (the plan's atomicity contract).
    pub fn apply(&mut self, tx: &Transaction) -> anyhow::Result<()> {
        let Sink::Sqlite { conn, codec } = self else {
            return Ok(());
        };
        let plan = codec.plan(tx).map_err(|e| anyhow!("sink plan: {e}"))?;
        for step in &plan.steps {
            let sqlite_tx = conn.transaction()?;
            for stmt in &step.statements {
                let params: Vec<rusqlite::types::Value> =
                    stmt.params.iter().map(sql_value).collect();
                sqlite_tx
                    .execute(&stmt.sql, rusqlite::params_from_iter(params))
                    .with_context(|| format!("sink: {}", stmt.sql))?;
            }
            sqlite_tx.commit()?;
        }
        Ok(())
    }

    /// Everything the mirror remembers, for rehydrating a fresh engine's
    /// ledger — the other half of `apply`. `None` sinks remember nothing.
    pub fn restore(&self) -> anyhow::Result<Option<Restored>> {
        let Sink::Sqlite { conn, .. } = self else {
            return Ok(None);
        };
        let text = |row: &rusqlite::Row, i: usize| -> Option<String> { row.get(i).ok().flatten() };
        let jsonv = |s: Option<String>| -> Option<serde_json::Value> {
            s.and_then(|raw| serde_json::from_str(&raw).ok())
        };

        let mut restored = Restored::default();

        let mut q = conn.prepare(
            "SELECT slug, kind, display_name, endpoint, last_probed_at FROM environments",
        )?;
        let rows = q.query_map([], |r| {
            Ok(Environment {
                slug: r.get(0)?,
                kind: r.get(1)?,
                display_name: r.get(2)?,
                endpoint: r.get(3)?,
                last_probed_at: r.get(4)?,
            })
        })?;
        restored.environments = rows.collect::<Result<_, _>>()?;

        let mut q = conn.prepare(
            "SELECT id, created_at, title, brief, repo, git_ref, isolation, template_name, \
             setup, env_slug, agent_name, model_id, timeout_secs, max_budget, labels, tags, \
             fetch_remote \
             FROM dispatches ORDER BY rowid",
        )?;
        let rows = q.query_map([], |r| {
            // The stored row keeps the RESOLVED agent/model; the original
            // ask's unset fields are gone — reconstruct the spec as resolved.
            let spec = serde_json::from_value(json!({
                "brief": r.get::<_, String>(3)?,
                "env": r.get::<_, String>(9)?,
                "agent": text(r, 10),
                "model": text(r, 11),
                "title": text(r, 2),
                "repo": text(r, 4),
                "gitRef": text(r, 5),
                "isolation": text(r, 6),
                "template": text(r, 7),
                "setup": text(r, 8),
                "timeoutSecs": r.get::<_, Option<i64>>(12).ok().flatten(),
                "maxBudget": text(r, 13),
                "labels": jsonv(text(r, 14)),
                "tags": jsonv(text(r, 15)),
                "fetchRemote": r.get::<_, Option<bool>>(16).ok().flatten(),
            }))
            .expect("a stored dispatch row deserializes");
            Ok(RestoredDispatch {
                id: r.get(0)?,
                created_at: r.get(1)?,
                spec,
                agent: r.get(10)?,
                model: r.get(11)?,
            })
        })?;
        restored.dispatches = rows.collect::<Result<_, _>>()?;

        let mut q = conn.prepare(
            "SELECT uid, dispatch_id, state, env_handle, attach_transport, \
             attach_endpoint, attach_target, attach_url, url, resumed_from, \
             started_at, ended_at, exit_reason, exit_detail, reaped_at \
             FROM sessions ORDER BY rowid",
        )?;
        let rows = q.query_map([], |r| {
            Ok(Session {
                uid: r.get(0)?,
                dispatch_id: r.get(1)?,
                state: r.get(2)?,
                env_handle: jsonv(text(r, 3)),
                attach_transport: r.get(4)?,
                attach_endpoint: r.get(5)?,
                attach_target: r.get(6)?,
                attach_url: r.get(7)?,
                url: r.get(8)?,
                resumed_from: r.get(9)?,
                started_at: r.get(10)?,
                ended_at: r.get(11)?,
                exit_reason: r.get(12)?,
                exit_detail: r.get(13)?,
                reaped_at: r.get(14)?,
            })
        })?;
        restored.sessions = rows.collect::<Result<_, _>>()?;

        // rowid preserves cross-session observation order (the events cursor's
        // contract); the twin columns fold back into the payload envelope.
        let mut q = conn.prepare(
            "SELECT session_uid, idx, ts, kind, fidelity, payload_kind, payload \
             FROM events ORDER BY rowid",
        )?;
        let rows = q.query_map([], |r| {
            let body = text(r, 6)
                .map(|raw| serde_json::from_str(&raw).unwrap_or(serde_json::Value::String(raw)))
                .unwrap_or(serde_json::Value::Null);
            Ok(Event {
                session_uid: r.get(0)?,
                idx: r.get(1)?,
                ts: r.get(2)?,
                kind: r.get(3)?,
                fidelity: r.get(4)?,
                payload: json!({"kind": r.get::<_, String>(5)?, "payload": body}),
            })
        })?;
        restored.events = rows.collect::<Result<_, _>>()?;

        // Control-plane messages: disponent owns them (no env backs them), so
        // the mirror is their durability — rehydrate them in send order (rowid).
        let mut q = conn.prepare(
            "SELECT id, created_at, sender, recipient, session_uid, body, in_reply_to, \
             fanout_id, topic, acked_at FROM messages ORDER BY rowid",
        )?;
        let rows = q.query_map([], |r| {
            Ok(Message {
                id: r.get(0)?,
                created_at: r.get(1)?,
                sender: r.get(2)?,
                recipient: r.get(3)?,
                session_uid: r.get(4)?,
                body: r.get(5)?,
                in_reply_to: r.get(6)?,
                fanout_id: r.get(7)?,
                topic: r.get(8)?,
                acked_at: r.get(9)?,
            })
        })?;
        restored.messages = rows.collect::<Result<_, _>>()?;

        Ok(Some(restored))
    }
}

/// A sink's memory, ledger-shaped (dispatches carry the resolved agent/model
/// alongside the reconstructed spec — the engine's DispatchRow split).
#[derive(Default)]
pub struct Restored {
    pub environments: Vec<Environment>,
    pub dispatches: Vec<RestoredDispatch>,
    pub sessions: Vec<Session>,
    pub events: Vec<Event>,
    pub messages: Vec<Message>,
}

pub struct RestoredDispatch {
    pub id: String,
    pub created_at: String,
    pub spec: DispatchSpec,
    pub agent: String,
    pub model: Option<String>,
}

/// JSON param → SQLite value. Structured values (objects/arrays) land as their
/// JSON text — the twin-column convention stores union bodies that way.
fn sql_value(v: &serde_json::Value) -> rusqlite::types::Value {
    use rusqlite::types::Value as Sql;
    match v {
        serde_json::Value::Null => Sql::Null,
        serde_json::Value::Bool(b) => Sql::Integer(*b as i64),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Sql::Integer(i)
            } else {
                Sql::Real(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        serde_json::Value::String(s) => Sql::Text(s.clone()),
        other => Sql::Text(other.to_string()),
    }
}
