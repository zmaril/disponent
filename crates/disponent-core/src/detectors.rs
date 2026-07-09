//! Terminal-condition detectors (#25): pure state machines that watch a
//! running session's observation stream and REPORT terminal-*candidate*
//! conditions. They never reap, cancel, or transition session state — reap
//! stays the only exit (AGENTS.md). A detector's job is to name a condition and
//! the [`ExitReason`](crate::mcp_generated) a supervising agent *might* reap
//! with; the decision belongs to that supervisor, not to disponent.
//!
//! Backend-agnostic, scraped-tier: the machines consume only two inputs per
//! poll — the current clock and whether the pane changed (activity) — so they
//! run over any [`Compute`](crate::backend::Compute) surface and stay
//! deterministically testable (the real-clock read happens at the watcher call
//! site, never in here). Two ship in this PR:
//!
//! * **idle-timeout** — no activity for `idle_secs` → candidate
//!   `ExitReason::timeout`. Resettable: any activity restarts the idle clock.
//! * **first-token dead-stream** — after start, if no first token appears within
//!   `first_token_secs` → candidate `ExitReason::error`. Latches once the first
//!   token is seen (stops watching for the rest of the session).
//!
//! Deferred — honest follow-ups, NOT fake stubs that pretend to work:
//! * `// #25 follow-up:` loop-detection needs repeated-output analysis (a
//!   history of pane deltas), not just this poll's activity bit.
//! * `// #25 follow-up:` cost-ceiling needs real usage events, which are
//!   scraped/absent today; enforcement maps to a future `budget_enforce`
//!   capability edge and is explicitly out of scope here.

use std::time::{Duration, Instant};

use serde_json::json;

use crate::observe::Observation;

/// A detected terminal-*candidate* condition. Reporting only: it names the
/// condition and the `ExitReason` a supervisor could reap with — disponent
/// never acts on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectedCondition {
    /// Machine name of the condition (e.g. `"idle-timeout"`).
    pub condition: &'static str,
    /// The `ExitReason` wire value a supervisor might reap with — the honest
    /// one for this condition, not a guess dressed up as certainty.
    pub candidate_exit_reason: &'static str,
    /// Human-readable summary for the session timeline.
    pub detail: String,
}

impl DetectedCondition {
    /// As a `derived`-fidelity observation, mirroring how `reap()` records its
    /// delivery verdict: kind `"raw"`, `source: "detector"`. Riding the existing
    /// observation → events pipeline means the watcher appends it exactly like a
    /// scraped terminal delta — no new event path, no state change.
    pub fn observation(&self) -> Observation {
        Observation {
            kind: "raw".to_string(),
            fidelity: "derived".to_string(),
            payload: json!({"kind": "raw", "payload": {
                "source": "detector",
                "data": {
                    "condition": self.condition,
                    "candidate_exit_reason": self.candidate_exit_reason,
                    "detail": self.detail,
                },
            }}),
        }
    }
}

/// Idle-timeout: fire once when the quiet span since the last activity first
/// reaches `threshold`. Resettable — any activity restarts the idle clock and
/// re-arms the detector, so a session that goes quiet again is reported again.
#[derive(Debug)]
pub struct IdleTimeout {
    threshold: Duration,
    /// Clock of the last activity (seeded to the first poll's `now`).
    last_activity: Option<Instant>,
    /// Latched for the current quiet span so we emit once, not every poll.
    fired: bool,
}

impl IdleTimeout {
    pub fn new(idle_secs: u64) -> Self {
        IdleTimeout {
            threshold: Duration::from_secs(idle_secs),
            last_activity: None,
            fired: false,
        }
    }

    /// Feed one poll. `now` is the current clock; `activity` is whether the pane
    /// changed since the previous poll. Returns a condition exactly once per
    /// quiet span — when it first reaches `threshold`. Activity resets the idle
    /// clock and re-arms the detector.
    pub fn observe(&mut self, now: Instant, activity: bool) -> Option<DetectedCondition> {
        let since = *self.last_activity.get_or_insert(now);
        if activity {
            self.last_activity = Some(now);
            self.fired = false;
            return None;
        }
        if self.fired {
            return None;
        }
        let quiet = now.saturating_duration_since(since);
        if quiet >= self.threshold {
            self.fired = true;
            return Some(DetectedCondition {
                condition: "idle-timeout",
                // Quiet-past-threshold on a session that WAS producing output is
                // a timeout of productive work — the honest ExitReason a
                // supervisor would reap with.
                candidate_exit_reason: "timeout",
                detail: format!(
                    "no terminal activity for {}s (idle threshold {}s)",
                    quiet.as_secs(),
                    self.threshold.as_secs()
                ),
            });
        }
        None
    }
}

/// First-token dead-stream: after the agent starts, fire once if no first token
/// (any activity) appears within `threshold`. Latches permanently once the
/// first token is seen — a stream that came alive is not dead, and we stop
/// watching it for the rest of the session.
#[derive(Debug)]
pub struct FirstTokenDeadStream {
    threshold: Duration,
    /// Clock of the first poll — the "since start" baseline.
    started: Option<Instant>,
    /// Once true the detector is done: the stream produced output.
    seen_first_token: bool,
    /// Latched so the dead-stream condition emits once, not every poll.
    fired: bool,
}

impl FirstTokenDeadStream {
    pub fn new(first_token_secs: u64) -> Self {
        FirstTokenDeadStream {
            threshold: Duration::from_secs(first_token_secs),
            started: None,
            seen_first_token: false,
            fired: false,
        }
    }

    /// Feed one poll. The first `activity` seen is the first token: the detector
    /// latches and never fires again. Otherwise, once `threshold` elapses since
    /// start with still no token, report the dead stream once.
    pub fn observe(&mut self, now: Instant, activity: bool) -> Option<DetectedCondition> {
        let start = *self.started.get_or_insert(now);
        if self.seen_first_token {
            return None;
        }
        if activity {
            self.seen_first_token = true;
            return None;
        }
        if self.fired {
            return None;
        }
        let elapsed = now.saturating_duration_since(start);
        if elapsed >= self.threshold {
            self.fired = true;
            return Some(DetectedCondition {
                condition: "first-token-dead-stream",
                // An agent that launched but produced NO first token never got
                // going — that reads as a failed start (error), not a timeout of
                // work in progress (which idle-timeout covers). Honest pick.
                candidate_exit_reason: "error",
                detail: format!(
                    "no first token {}s after start (first-token threshold {}s)",
                    elapsed.as_secs(),
                    self.threshold.as_secs()
                ),
            });
        }
        None
    }
}

/// The per-session detector set the watcher drives. Holds both backend-agnostic
/// detectors so the watcher feeds one `(now, activity)` per poll and appends
/// whatever fired.
#[derive(Debug)]
pub struct Detectors {
    idle: IdleTimeout,
    dead_stream: FirstTokenDeadStream,
}

impl Detectors {
    /// Build from the session's resolved [`LifecyclePolicy`] thresholds (#28):
    /// the single dispatch-time resolution, read here rather than re-derived.
    pub fn new(idle_secs: u64, first_token_secs: u64) -> Self {
        Detectors {
            idle: IdleTimeout::new(idle_secs),
            dead_stream: FirstTokenDeadStream::new(first_token_secs),
        }
    }

    /// Feed one poll's `(now, activity)`; return every condition detected this
    /// poll. Each detector fires at most once per its own latch rule, so this is
    /// empty on the vast majority of polls.
    pub fn observe(&mut self, now: Instant, activity: bool) -> Vec<DetectedCondition> {
        [
            self.idle.observe(now, activity),
            self.dead_stream.observe(now, activity),
        ]
        .into_iter()
        .flatten()
        .collect()
    }
}

#[cfg(test)]
mod tests;
