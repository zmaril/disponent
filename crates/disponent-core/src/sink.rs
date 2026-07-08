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
