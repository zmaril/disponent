//! PyO3 binding for the disponent engine. The sync Rust engine runs in-process
//! inside CPython; the blocking `wait()` releases the GIL (`py.detach`). The
//! PyO3 surface is GENERATED (`generated.rs`, from the fluessig catalog's op
//! layer — pyclasses, kwargs-flattened methods, iterator dressing), and the
//! engine wiring is hand-written once in `core_impl.rs`. This file holds only
//! the bespoke bits: the module init and the `@manual` op `wait()` (blocking
//! poll-until-terminal — event-loop specifics the shape templates exclude).
//! The other `@manual` op, `serveMcp`, is deliberately absent: a Python host
//! wanting MCP runs the `disponent` CLI.
//!
//! straitjacket-allow-file:duplication — the `@manual wait()` poll loop mirrors
//! the node binding's by design (parallel per-binding seams).

mod core_impl;
mod generated;

pub use generated::*;

use std::sync::Arc;
use std::time::{Duration, Instant};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use core_impl::{is_terminal, DisponentImpl};

fn err(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

// ---- @manual: wait (blocking poll until the session settles) ----
// A second #[pymethods] block extends the generated `Disponent` class (needs
// pyo3's `multiple-pymethods` feature).

#[pymethods]
impl Disponent {
    /// Block (GIL released) until the session reaches a terminal state or
    /// `timeout_secs` passes; returns the latest snapshot either way — read
    /// `state` to tell which.
    #[pyo3(signature = (session_uid, timeout_secs))]
    fn wait(&self, py: Python<'_>, session_uid: String, timeout_secs: i32) -> PyResult<Session> {
        let core: Arc<DisponentImpl> = self.core.clone();
        py.detach(move || -> anyhow::Result<Session> {
            let deadline = Instant::now() + Duration::from_secs(timeout_secs.max(0) as u64);
            loop {
                let session = DisponentCore::session(core.as_ref(), session_uid.clone())?
                    .ok_or_else(|| anyhow::anyhow!("no session {session_uid}"))?;
                // Terminal, or out of patience: hand back the latest snapshot
                // either way — the caller reads `state` to tell which.
                if is_terminal(session.state) || Instant::now() >= deadline {
                    return Ok(session);
                }
                std::thread::sleep(Duration::from_millis(250));
            }
        })
        .map_err(err)
    }
}

/// The compiled module: `disponent._disponent` (the `disponent` package re-exports it).
#[pymodule]
fn _disponent(m: &Bound<'_, PyModule>) -> PyResult<()> {
    generated::register(m)
}
