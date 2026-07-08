//! disponent-core: dispatch work to coding agents.
//!
//! The engine is synchronous and in-process (the entl discipline: async is a
//! per-binding concern). Environments are the source of truth; the ledger here
//! is the reconciled cache of what they told us. Phase 1 ships the walking
//! skeleton: the shipped catalog, the in-memory ledger, and the generated MCP
//! surface — env backends land in phase 3.

pub mod backend;
pub mod catalog;
pub mod engine;
pub mod local;
pub mod mcp_generated;
pub mod observe;
pub mod otel;
pub mod schema_gen;
pub mod sink;

pub use engine::Engine;
