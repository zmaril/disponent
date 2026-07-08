//! Terminal observation, the scraped tier: each running session gets an
//! observer thread (fluessig's ObserverPool) that polls the backend's
//! `capture` and emits what changed since last look as `raw` events —
//! honest fidelity: "scraped". The exact tier (Claude Code's own telemetry)
//! lives in [`crate::otel`].

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
}
