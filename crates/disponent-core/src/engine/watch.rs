//! The terminal-watcher subsystem, kept out of `engine.rs` so that file stays
//! under the size budget. A child module sees its parent's private items, so
//! these free functions reach `Ledger` internals directly.
//!
//! Two tiers, chosen per session by the [`Compute`](crate::backend::Compute) it
//! runs on:
//!
//! * **Exact** — a holder-backed surface hands back a live, byte-exact
//!   [`TerminalStream`](crate::observe::TerminalStream). We consume THAT: exact
//!   frames land in the ledger at `fidelity: "exact"`, and the child's REAL exit
//!   self-transitions the session to `completed`/`failed` (design §5 — an
//!   observation, never a reap; the record persists until someone `reap()`s it).
//! * **Scraped** — every other surface (tmux, exe.dev, modal) has no live
//!   stream, so we poll `capture` on an interval and diff the pane into
//!   `scraped` events, exactly as before. Nothing on that path changed.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::json;

use super::{event_mutation, session_mutation, Ledger, LifecyclePolicy};
use crate::agent::AgentAdapter;
use crate::backend::EnvProvider;
use crate::detectors::Detectors;
use crate::observe::{self, Observation};
use fluessig::observe::{Event as ObsEvent, ObserverPool, Poll};

/// One session's terminal watcher joins the pool (the provisioner and
/// reconcile-adoption call this off the Engine). A holder-backed session is
/// watched at the exact tier (its live stream); everything else polls at the
/// scraped tier — the branch is invisible to the caller.
pub(super) fn watch_session(
    observers: &ObserverPool<Observation>,
    interval: Duration,
    backend: Arc<dyn EnvProvider>,
    adapter: Arc<dyn AgentAdapter>,
    uid: &str,
    handle: serde_json::Value,
    // #28: the single resolved policy, read here for #25's detector thresholds
    // (idle_secs, first_token_secs). Detection only — no enforcement.
    policy: LifecyclePolicy,
) {
    // Exact tier (the M1 payoff): a holder-backed Compute offers a live,
    // byte-exact stream — consume it (exact frames + the child's real exit)
    // instead of polling `capture`. A `None` stream (tmux, exe.dev, modal,
    // dry-run) falls through to the unchanged scraped poller below.
    if let Ok(Some(stream)) = backend.compute(&handle).and_then(|c| c.observe_stream()) {
        observe::spawn_exact_observer(observers, uid, interval, stream);
        return;
    }

    let mut last = String::new();
    // #25: per-session terminal-condition detectors, thresholds from the one
    // resolved LifecyclePolicy. Pure state machines fed the real clock + an
    // activity bit each poll; they REPORT candidate exits, they never reap.
    let mut detectors = Detectors::new(policy.idle_secs, policy.first_token_secs);
    observers.spawn(uid.to_string(), interval, move || {
        // The agent's state read goes through the adapter's `monitor` (which
        // wraps the compute-surface capture at scraped fidelity).
        let pane = backend
            .compute(&handle)
            .and_then(|c| Ok(adapter.monitor(&*c)?.pane))
            .map_err(|e| e.to_string())?;
        let delta = observe::terminal_delta(&last, &pane);
        last = pane;
        // Activity = the pane changed this poll. Feed (real clock, activity) to
        // the detectors; any fired condition rides the same observation pipeline
        // as the scraped delta, landing as a derived event. Idle resets on
        // activity; dead-stream latches on the first token.
        let activity = delta.is_some();
        let mut items: Vec<Observation> = delta
            .map(observe::terminal_observation)
            .into_iter()
            .collect();
        for condition in detectors.observe(Instant::now(), activity) {
            items.push(condition.observation());
        }
        Ok(Poll::Items(items))
    });
}

/// Map a holder exit payload to `(target state, exit_reason, exit_detail)`: a
/// clean `code: 0` completes; a nonzero code or a signal death fails — and the
/// real code/signal is recorded so "why" survives on the row, not just "that".
fn exit_outcome(payload: &serde_json::Value) -> (&'static str, String, String) {
    let body = &payload["payload"];
    match (body["code"].as_i64(), body["signal"].as_i64()) {
        (Some(0), _) => ("completed", "exit".into(), "exit code 0".into()),
        (Some(code), _) => ("failed", "exit".into(), format!("exit code {code}")),
        (_, Some(sig)) => ("failed", "signal".into(), format!("killed by signal {sig}")),
        _ => ("failed", "exit".into(), "exited abnormally".into()),
    }
}

/// The collector: fold drained observations into the ledger until stopped.
/// Observer failures become log events — a watcher dying is a fact about the
/// session, not a silent gap.
pub(super) fn collect(
    ledger: Arc<Mutex<Ledger>>,
    observers: Arc<ObserverPool<Observation>>,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::Relaxed) {
        let drained = observers.drain();
        if !drained.is_empty() {
            let mut l = ledger.lock().unwrap();
            let mut mutations = Vec::new();
            for ev in drained {
                match ev {
                    ObsEvent::Item { subject, item } => {
                        if l.sessions
                            .iter()
                            .any(|s| s.uid == subject && s.reaped_at.is_none())
                        {
                            // A holder's exact "exit" observation is the child's
                            // REAL termination: fold the event, then self-
                            // transition the session completed/failed off the true
                            // code and record it (§5 — an observation, not a reap;
                            // the row persists). Scraped observers never emit
                            // "exit", so only the exact tier reaches this.
                            let outcome =
                                (item.kind == "exit").then(|| exit_outcome(&item.payload));
                            let e = l.push_event_graded(
                                &subject,
                                &item.kind,
                                &item.fidelity,
                                item.payload,
                            );
                            mutations.push(event_mutation(&e));
                            if let Some((to, reason, detail)) = outcome {
                                if let Ok((_, state_ev)) = l.transition(&subject, to) {
                                    if let Ok(session) = l.session_mut(&subject) {
                                        session.exit_reason = Some(reason);
                                        session.exit_detail = Some(detail);
                                        mutations.push(session_mutation(&session.clone()));
                                    }
                                    mutations.push(event_mutation(&state_ev));
                                }
                            }
                        }
                    }
                    ObsEvent::Failed { subject, error } => {
                        let e = l.push_event(
                            &subject,
                            "log",
                            json!({"kind": "log", "payload":
                                {"line": format!("terminal observer stopped: {error}")}}),
                        );
                        mutations.push(event_mutation(&e));
                    }
                    ObsEvent::Ended { .. } => {}
                }
            }
            let _ = l.mirror(mutations);
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}
