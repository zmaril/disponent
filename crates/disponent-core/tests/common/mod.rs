//! Shared helpers for the integration suites.

use std::time::{Duration, Instant};

use disponent_core::mcp_generated::{DisponentMcp, Session};
use disponent_core::Engine;

/// Poll until the background provisioner settles the session, or time out.
pub fn wait_for(engine: &Engine, uid: &str, state: &str, timeout: Duration) -> Session {
    let deadline = Instant::now() + timeout;
    loop {
        let s = engine.session(uid.to_string()).unwrap().unwrap();
        if s.state == state {
            return s;
        }
        assert!(
            Instant::now() < deadline,
            "stuck in {} waiting for {state} ({:?} {:?})",
            s.state,
            s.exit_reason,
            s.exit_detail
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}
