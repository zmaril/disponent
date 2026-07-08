//! The shipped catalog: which environments exist, what they can do, and which
//! agent × model combinations each one offers. Hard-coded and kept current by
//! hand (notes/design.md §5) — config can add environments on top, but the
//! curated baseline rides with the library so a fresh install knows the world.

use fluessig::data::{Mutation, Op, Transaction};
use serde_json::{json, Value};

use crate::mcp_generated::Environment;

/// One env × agent × model row. `is_default` marks the pick when a dispatch
/// names only the environment.
pub struct Offering {
    pub env: &'static str,
    pub agent: &'static str,
    pub model: &'static str,
    pub is_default: bool,
}

/// What an environment can do (mirrors the CapabilityKind vocabulary).
pub struct EnvCapabilities {
    pub env: &'static str,
    pub capabilities: &'static [&'static str],
}

pub const OFFERINGS: &[Offering] = &[
    Offering {
        env: "local",
        agent: "claude-code",
        model: "claude-opus-4-8",
        is_default: true,
    },
    Offering {
        env: "local",
        agent: "claude-code",
        model: "claude-sonnet-5",
        is_default: false,
    },
    Offering {
        env: "local",
        agent: "claude-code",
        model: "claude-haiku-4-5",
        is_default: false,
    },
    Offering {
        env: "exe-dev",
        agent: "claude-code",
        model: "claude-opus-4-8",
        is_default: true,
    },
    Offering {
        env: "exe-dev",
        agent: "claude-code",
        model: "claude-sonnet-5",
        is_default: false,
    },
    Offering {
        env: "exe-dev",
        agent: "claude-code",
        model: "claude-haiku-4-5",
        is_default: false,
    },
];

pub const CAPABILITIES: &[EnvCapabilities] = &[
    EnvCapabilities {
        env: "local",
        capabilities: &[
            "dispatch",
            "interact",
            "observe_poll",
            "list_sessions",
            "cancel",
            "teardown",
            "isolation_worktree",
        ],
    },
    EnvCapabilities {
        env: "exe-dev",
        capabilities: &[
            "dispatch",
            "interact",
            "observe_poll",
            "list_sessions",
            "cancel",
            "teardown",
            "isolation_vm",
            "templates",
        ],
    },
];

/// The baseline environment rows (as the MCP-surface DTO — the flat shape).
pub fn environments() -> Vec<Environment> {
    vec![
        Environment {
            slug: "local".into(),
            kind: "local".into(),
            display_name: Some("This machine (tmux)".into()),
            endpoint: None,
            last_probed_at: None,
        },
        Environment {
            slug: "exe-dev".into(),
            kind: "exe_dev".into(),
            display_name: Some("exe.dev VMs".into()),
            endpoint: Some("ssh://exe.dev".into()),
            last_probed_at: None,
        },
    ]
}

/// The default (agent, model) for an environment, if the catalog has one.
pub fn default_offering(env: &str) -> Option<(&'static str, &'static str)> {
    OFFERINGS
        .iter()
        .find(|o| o.env == env && o.is_default)
        .map(|o| (o.agent, o.model))
}

/// Is (env, agent[, model]) a combination the catalog knows?
pub fn offered(env: &str, agent: &str, model: Option<&str>) -> bool {
    OFFERINGS
        .iter()
        .any(|o| o.env == env && o.agent == agent && model.is_none_or(|m| m == o.model))
}

pub(crate) fn upsert(table: &str, columns: &[&str], rows: Vec<Vec<Value>>) -> Mutation {
    Mutation {
        table: table.to_string(),
        op: Op::Upsert,
        columns: columns.iter().map(|c| c.to_string()).collect(),
        rows,
    }
}

/// The shipped catalog as sink rows — applied on every open (idempotent upserts).
pub fn seed_tx() -> Transaction {
    let mut agents: Vec<&str> = OFFERINGS.iter().map(|o| o.agent).collect();
    agents.dedup();
    let mut models: Vec<&str> = OFFERINGS.iter().map(|o| o.model).collect();
    models.sort_unstable();
    models.dedup();
    let mut caps: Vec<&str> = CAPABILITIES
        .iter()
        .flat_map(|c| c.capabilities)
        .copied()
        .collect();
    caps.sort_unstable();
    caps.dedup();

    Transaction {
        mutations: vec![
            upsert(
                "agents",
                &["name"],
                agents.iter().map(|a| vec![json!(a)]).collect(),
            ),
            upsert(
                "models",
                &["id"],
                models.iter().map(|m| vec![json!(m)]).collect(),
            ),
            upsert(
                "environments",
                &["slug", "kind", "display_name", "endpoint"],
                environments()
                    .iter()
                    .map(|e| {
                        vec![
                            json!(e.slug),
                            json!(e.kind),
                            json!(e.display_name),
                            json!(e.endpoint),
                        ]
                    })
                    .collect(),
            ),
            upsert(
                "capabilities",
                &["capability"],
                caps.iter().map(|c| vec![json!(c)]).collect(),
            ),
            upsert(
                "env_capabilities",
                // detail is (wrongly) part of the generated PK and NULLs never
                // conflict-match, so seed it as "" to keep the upsert idempotent
                &["slug", "detail", "capability"],
                CAPABILITIES
                    .iter()
                    .flat_map(|ec| {
                        ec.capabilities
                            .iter()
                            .map(|cap| vec![json!(ec.env), json!(""), json!(cap)])
                    })
                    .collect(),
            ),
            upsert(
                "offerings",
                &["env_slug", "agent_name", "model_id", "is_default"],
                OFFERINGS
                    .iter()
                    .map(|o| {
                        vec![
                            json!(o.env),
                            json!(o.agent),
                            json!(o.model),
                            json!(o.is_default),
                        ]
                    })
                    .collect(),
            ),
        ],
    }
}
