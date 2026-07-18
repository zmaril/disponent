//! Terminal observation, the scraped tier: each running session gets an
//! observer thread (fluessig's ObserverPool) that polls the backend's
//! `capture` and emits what changed since last look as `raw` events —
//! honest fidelity: "scraped". The exact tier (Claude Code's own telemetry)
//! lives in [`crate::otel`].
//!
//! ## The exact terminal tier (M1, `notes/owning-the-terminal.md` §4)
//!
//! When a [`Compute`](crate::backend::Compute) surface is backed by the
//! first-party holder (`disponent hold`, gated by `DISPONENT_LOCAL_HOLDER`), it
//! can hand back a live [`TerminalStream`] instead of only a poll-scraped pane.
//! A stream yields the pty's **byte-exact** output and the child's **real**
//! [exit](StreamChunk::Exit) — so the same `raw` events carry `fidelity: "exact"`
//! and the process exit code is a fact, not an inference. This module owns the
//! stream *type* + the exact-tier observation constructors; the holder-frame →
//! [`StreamChunk`] bridge lives in [`crate::local`] (which owns the holder
//! dependency), keeping this module transport-agnostic.

use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::Duration;

use fluessig::observe::{ObserverPool, Poll};
use serde_json::json;

/// One observed change, ready to become an events-table row.
pub struct Observation {
    pub kind: String,
    pub fidelity: String,
    /// The full payload envelope ({"kind": tag, "payload": body}).
    pub payload: serde_json::Value,
}

/// What's new in `current` relative to `previous`, terminal-style: panes
/// scroll up, so the largest suffix of the old capture that prefixes the new
/// one marks where fresh output starts. In-place redraws (spinners, TUIs)
/// defeat overlap detection — then the whole pane is the observation.
pub fn terminal_delta(previous: &str, current: &str) -> Option<String> {
    if current == previous || current.trim().is_empty() {
        return None;
    }
    let old: Vec<&str> = previous.lines().collect();
    let new: Vec<&str> = current.lines().collect();
    for k in (1..=old.len().min(new.len())).rev() {
        if old[old.len() - k..] == new[..k] {
            let fresh = new[k..].join("\n");
            return if fresh.trim().is_empty() {
                None
            } else {
                Some(fresh)
            };
        }
    }
    Some(current.to_string())
}

/// The delta as an events-row observation.
pub fn terminal_observation(delta: String) -> Observation {
    Observation {
        kind: "raw".to_string(),
        fidelity: "scraped".to_string(),
        payload: json!({"kind": "raw", "payload":
            {"source": "terminal", "data": delta}}),
    }
}

/// A chunk of exact output off the ended pty is the same `raw`/`terminal`
/// envelope as [`terminal_observation`], but stamped `fidelity: "exact"` — the
/// bytes came straight off the pty master, not diffed from a scraped snapshot.
pub fn exact_terminal_observation(data: String) -> Observation {
    Observation {
        kind: "raw".to_string(),
        fidelity: "exact".to_string(),
        payload: json!({"kind": "raw", "payload":
            {"source": "terminal", "data": data}}),
    }
}

/// The child's real termination as an `exact` observation: kind `"exit"`,
/// carrying the true exit `code` (or the `signal` that killed it). This is the
/// honesty win M0 unlocked and M1 surfaces — nothing else on the local path
/// knows a session's real exit; `exec bash` keeps the tmux pane alive forever.
///
/// NB: this records the exit as a timeline *fact*; transitioning the session's
/// state to completed/failed off it is a coordinated engine change (a follow-up
/// — see the PR), so it stays a noted event here, not a state mutation.
pub fn exit_observation(code: Option<i32>, signal: Option<i32>) -> Observation {
    Observation {
        kind: "exit".to_string(),
        fidelity: "exact".to_string(),
        payload: json!({"kind": "exit", "payload":
            {"source": "terminal", "code": code, "signal": signal}}),
    }
}

/// One item off a live [`TerminalStream`]: byte-exact output, a periodic
/// keepalive, or the child's real termination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamChunk {
    /// Raw pty bytes, byte-exact.
    Data(Vec<u8>),
    /// A holder keepalive — carries no output, but proves the stream is alive
    /// (and lets a reader notice its consumer has gone away).
    Heartbeat,
    /// The child ended: `code` on a normal exit, `signal` on a signal death.
    Exit {
        code: Option<i32>,
        signal: Option<i32>,
    },
}

/// A live, byte-exact terminal stream from a first-party holder — the exact-tier
/// alternative to polling [`capture`](crate::backend::Compute::capture). A
/// background reader (owned by the holder-backed `Compute`) feeds
/// [`StreamChunk`]s down a channel; consumers turn them into `exact`
/// [`Observation`]s.
///
/// The engine's current watch loop polls `capture()` on an interval and can't
/// hold a persistent stream across polls, so wiring this into that loop (so the
/// exact frames land in the ledger) is a coordinated engine change tracked as a
/// follow-up; today the stream is consumed directly (pm, tests, and the
/// observation constructors below).
pub struct TerminalStream {
    rx: Receiver<StreamChunk>,
}

impl TerminalStream {
    /// Wrap a channel of chunks fed by a holder reader. Transport-agnostic: the
    /// holder-frame → [`StreamChunk`] mapping happens on the sender side.
    pub fn new(rx: Receiver<StreamChunk>) -> TerminalStream {
        TerminalStream { rx }
    }

    /// Block for the next chunk, or `None` once the sender (and so the holder
    /// connection) is gone. The dedicated-consumer / test entry point.
    pub fn recv(&self) -> Option<StreamChunk> {
        self.rx.recv().ok()
    }

    /// Take the next chunk if one is ready, without blocking. `Ok(None)` = the
    /// stream is empty but still open; `Err` distinguishes "empty" from
    /// "disconnected" for callers that care.
    pub fn try_recv(&self) -> Result<StreamChunk, TryRecvError> {
        self.rx.try_recv()
    }

    /// Drain every currently-ready chunk into `exact`-fidelity observations
    /// (empty `Data` and `Heartbeat`s carry nothing, so they drop out). Non-
    /// blocking — returns what's available right now, the shape an interval
    /// observer wants.
    pub fn drain_observations(&self) -> Vec<Observation> {
        let mut out = Vec::new();
        while let Ok(chunk) = self.rx.try_recv() {
            if let Some(obs) = chunk_observation(chunk) {
                out.push(obs);
            }
        }
        out
    }
}

/// Map one chunk to an `exact` observation, or `None` for the no-payload kinds.
fn chunk_observation(chunk: StreamChunk) -> Option<Observation> {
    match chunk {
        StreamChunk::Data(bytes) if !bytes.is_empty() => Some(exact_terminal_observation(
            String::from_utf8_lossy(&bytes).into_owned(),
        )),
        StreamChunk::Data(_) | StreamChunk::Heartbeat => None,
        StreamChunk::Exit { code, signal } => Some(exit_observation(code, signal)),
    }
}

/// Spawn a pool observer that consumes a holder's live [`TerminalStream`] — the
/// exact tier the M1 follow-up wires into the engine. Each interval it drains
/// whatever chunks are ready into `exact` [`Observation`]s (byte-exact frames,
/// and the child's real [`Exit`](StreamChunk::Exit)); when the stream yields its
/// terminal exit — or the holder connection closes — it ends the subject with
/// [`Poll::Done`], so the observer thread stops (no leak) and the collector can
/// fold the exit into a completed/failed transition. Draining is non-blocking,
/// so a quiet stream costs nothing; the holder's `Exit` is what actually ends it.
pub fn spawn_exact_observer(
    observers: &ObserverPool<Observation>,
    uid: &str,
    interval: Duration,
    stream: TerminalStream,
) {
    observers.spawn(uid.to_string(), interval, move || {
        let mut items = Vec::new();
        let mut ended = false;
        loop {
            match stream.try_recv() {
                Ok(chunk) => {
                    if matches!(chunk, StreamChunk::Exit { .. }) {
                        ended = true;
                    }
                    if let Some(obs) = chunk_observation(chunk) {
                        items.push(obs);
                    }
                }
                // Empty = drained but still open (keep observing); Disconnected =
                // the holder reader is gone (an exit we already saw, or a dropped
                // connection) — end the subject either way.
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    ended = true;
                    break;
                }
            }
        }
        Ok(if ended {
            Poll::Done(items)
        } else {
            Poll::Items(items)
        })
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deltas_track_a_scrolling_terminal() {
        assert_eq!(terminal_delta("", "hello"), Some("hello".into()));
        assert_eq!(terminal_delta("hello", "hello"), None);
        // scroll: old tail prefixes new capture — only the fresh tail emits
        assert_eq!(terminal_delta("a\nb\nc", "b\nc\nd\ne"), Some("d\ne".into()));
        // in-place redraw: no overlap — the whole pane is the observation
        assert_eq!(
            terminal_delta("spinner |", "spinner /"),
            Some("spinner /".into())
        );
        // whitespace-only churn is not an observation
        assert_eq!(terminal_delta("a\nb", "b\n\n  "), None);
        assert_eq!(terminal_delta("x", ""), None);
    }

    #[test]
    fn observations_wrap_the_raw_envelope() {
        let o = terminal_observation("new line".into());
        assert_eq!(o.kind, "raw");
        assert_eq!(o.fidelity, "scraped");
        assert_eq!(o.payload["kind"], "raw");
        assert_eq!(o.payload["payload"]["source"], "terminal");
        assert_eq!(o.payload["payload"]["data"], "new line");
    }

    #[test]
    fn exact_terminal_observation_is_exact_fidelity() {
        // Same envelope as the scraped one, but honestly marked exact — the
        // whole point of the holder path (design §4).
        let o = exact_terminal_observation("byte-exact".into());
        assert_eq!(o.kind, "raw");
        assert_eq!(o.fidelity, "exact");
        assert_eq!(o.payload["payload"]["source"], "terminal");
        assert_eq!(o.payload["payload"]["data"], "byte-exact");
    }

    #[test]
    fn exit_observation_carries_the_real_code() {
        let o = exit_observation(Some(3), None);
        assert_eq!(o.kind, "exit");
        assert_eq!(o.fidelity, "exact");
        assert_eq!(o.payload["payload"]["code"], 3);
        assert_eq!(o.payload["payload"]["signal"], serde_json::Value::Null);

        let s = exit_observation(None, Some(9));
        assert_eq!(s.payload["payload"]["signal"], 9);
    }

    #[test]
    fn stream_drains_data_and_exit_into_exact_observations() {
        let (tx, rx) = std::sync::mpsc::channel();
        let stream = TerminalStream::new(rx);
        tx.send(StreamChunk::Data(b"hello".to_vec())).unwrap();
        tx.send(StreamChunk::Heartbeat).unwrap(); // no-payload → dropped
        tx.send(StreamChunk::Data(Vec::new())).unwrap(); // empty → dropped
        tx.send(StreamChunk::Exit {
            code: Some(3),
            signal: None,
        })
        .unwrap();

        let obs = stream.drain_observations();
        assert_eq!(obs.len(), 2, "one data + one exit; heartbeat/empty dropped");
        assert_eq!(obs[0].fidelity, "exact");
        assert_eq!(obs[0].payload["payload"]["data"], "hello");
        assert_eq!(obs[1].kind, "exit");
        assert_eq!(obs[1].payload["payload"]["code"], 3);
    }
}
