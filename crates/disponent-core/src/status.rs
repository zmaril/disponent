//! The session state machine, in one place.
//!
//! Design §5 (notes/design.md) is the prose source of truth:
//!
//! ```text
//!             ┌────────────► cancelled
//!             │
//! queued → provisioning → running ⇄ needs_input
//!             │               │
//!             │               ├──► completed
//!             │               ├──► failed
//!             └───────────────┴──► lost
//! ```
//!
//! Every non-terminal state can also be `cancelled` (a consumer's `cancel()` /
//! reap-cancels-first) and `lost` (reconcile can no longer find the worker), so
//! those edges are drawn from each live state. Terminal states carry no outgoing
//! edges — reap archives them, nothing revives them (`resume()` is future work,
//! and even then lands a *new* session linked by `resumedFrom`, never a
//! terminal→live transition on the same row).

/// Session states with no way forward — reap archives them, nothing revives them.
pub const TERMINAL: &[&str] = &["completed", "failed", "cancelled", "lost"];

/// Is `state` a terminal state (no outgoing transitions)?
pub fn is_terminal(state: &str) -> bool {
    TERMINAL.contains(&state)
}

/// The legal outgoing edges for each live state. Terminal states are absent
/// (no key) — they have no successors. An unknown `from` also has no entry, so
/// `legal_transition` rejects it.
fn successors(from: &str) -> Option<&'static [&'static str]> {
    Some(match from {
        "queued" => &["provisioning", "cancelled", "lost"],
        "provisioning" => &["running", "failed", "cancelled", "lost"],
        "running" => &["needs_input", "completed", "failed", "cancelled", "lost"],
        "needs_input" => &["running", "completed", "failed", "cancelled", "lost"],
        _ => return None,
    })
}

/// Does the state machine permit `from -> to`? Rejects unknown state strings on
/// either end and every terminal `from` (which has no successors).
pub fn legal_transition(from: &str, to: &str) -> bool {
    successors(from).is_some_and(|edges| edges.contains(&to))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documented_edges_are_legal() {
        for (from, to) in [
            ("queued", "provisioning"),
            ("provisioning", "running"),
            ("running", "needs_input"),
            ("needs_input", "running"),
            ("running", "completed"),
            ("running", "failed"),
            ("provisioning", "lost"),
            ("running", "lost"),
            ("queued", "cancelled"),
            ("provisioning", "cancelled"),
            ("running", "cancelled"),
            ("needs_input", "cancelled"),
        ] {
            assert!(legal_transition(from, to), "{from} -> {to} should be legal");
        }
    }

    #[test]
    fn terminal_states_have_no_successors() {
        for from in TERMINAL {
            for to in ["running", "queued", "completed", "cancelled"] {
                assert!(
                    !legal_transition(from, to),
                    "{from} -> {to} should be illegal (terminal has no way forward)"
                );
            }
        }
    }

    #[test]
    fn unknown_states_are_rejected() {
        assert!(!legal_transition("bogus", "running"));
        assert!(!legal_transition("running", "bogus"));
        assert!(
            !legal_transition("queued", "running"),
            "no skip past provisioning"
        );
    }
}
