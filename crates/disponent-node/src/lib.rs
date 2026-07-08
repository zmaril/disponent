//! Node-API binding for the disponent engine (napi-rs). The sync Rust engine
//! runs in-process inside Node/Bun; the napi surface is GENERATED
//! (`generated.rs`, from the fluessig catalog's op layer — classes,
//! AsyncTasks→Promises, poll-stream dressing), and the engine wiring is
//! hand-written once in `core_impl.rs`. This file holds only what stays
//! bespoke: `version()` and the `@manual` op `wait()` (blocking
//! poll-until-terminal — event-loop specifics the shape templates exclude).
//! The other `@manual` op, `serveMcp`, is deliberately absent here: a Node
//! host wanting MCP runs the `disponent` CLI.

mod core_impl;
mod generated;

pub use generated::*;

use std::sync::Arc;
use std::time::{Duration, Instant};

use napi::bindgen_prelude::AsyncTask;
use napi::{Env, Task};
use napi_derive::napi;

use core_impl::{is_terminal, DisponentImpl};

fn err(e: impl std::fmt::Display) -> napi::Error {
    napi::Error::from_reason(e.to_string())
}

/// Set an environment variable IN THE NATIVE PROCESS. Exists because Bun's
/// `process.env` assignments update only the JS-side snapshot — a Rust engine
/// reading `std::env::var` never sees them — so backend knobs
/// (`DISPONENT_EXE_DRY_RUN`, `DISPONENT_CLAUDE_FLAGS`, …) set from JS must
/// cross through here BEFORE constructing `Disponent`.
#[napi]
pub fn set_env(key: String, value: String) {
    std::env::set_var(key, value);
}

/// Sanity probe — confirms the native addon loaded and links disponent-core.
#[napi]
pub fn version() -> String {
    format!(
        "disponent-node {} (engine ready)",
        env!("CARGO_PKG_VERSION")
    )
}

// ---- @manual: wait (blocking poll until the session settles) ----

pub struct WaitTask {
    core: Arc<DisponentImpl>,
    session_uid: String,
    timeout_secs: i32,
}

impl Task for WaitTask {
    type Output = Session;
    type JsValue = Session;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let deadline = Instant::now() + Duration::from_secs(self.timeout_secs.max(0) as u64);
        loop {
            let session = DisponentCore::session(self.core.as_ref(), self.session_uid.clone())
                .map_err(err)?
                .ok_or_else(|| err(format!("no session {}", self.session_uid)))?;
            // Terminal, or out of patience: hand back the latest snapshot
            // either way — the caller reads `state` to tell which.
            if is_terminal(session.state) || Instant::now() >= deadline {
                return Ok(session);
            }
            std::thread::sleep(Duration::from_millis(250));
        }
    }

    fn resolve(&mut self, _env: Env, o: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(o)
    }
}

#[napi]
impl Disponent {
    /// Block (off the event loop) until the session reaches a terminal state
    /// or `timeoutSecs` passes; resolves the latest snapshot either way.
    #[napi(ts_return_type = "Promise<Session>")]
    pub fn wait(&self, session_uid: String, timeout_secs: i32) -> AsyncTask<WaitTask> {
        AsyncTask::new(WaitTask {
            core: self.core.clone(),
            session_uid,
            timeout_secs,
        })
    }
}
