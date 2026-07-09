//! Deterministic detector tests: the state machines take the clock as an input,
//! so every scenario is driven with synthetic `Instant`s — no real sleeps.

use super::*;

/// A base instant plus `secs` — the synthetic clock the tests advance by hand.
fn at(base: Instant, secs: u64) -> Instant {
    base + Duration::from_secs(secs)
}

#[test]
fn idle_fires_once_after_the_threshold() {
    let base = Instant::now();
    let mut idle = IdleTimeout::new(30);

    // First poll seeds the idle clock; still quiet, under threshold → nothing.
    assert_eq!(idle.observe(base, false), None);
    assert_eq!(idle.observe(at(base, 29), false), None);

    // At/after the threshold it fires exactly one candidate-timeout condition.
    let fired = idle.observe(at(base, 30), false).expect("idle should fire");
    assert_eq!(fired.condition, "idle-timeout");
    assert_eq!(fired.candidate_exit_reason, "timeout");

    // Latched: a further quiet poll does NOT re-emit (one event per quiet span).
    assert_eq!(idle.observe(at(base, 40), false), None);
}

#[test]
fn idle_resets_on_activity_and_can_fire_again() {
    let base = Instant::now();
    let mut idle = IdleTimeout::new(30);

    assert_eq!(idle.observe(base, false), None);
    // Activity resets the idle clock (and re-arms the latch).
    assert_eq!(idle.observe(at(base, 20), true), None);
    // 20s + 25s = 45s absolute, but only 25s since the reset → still quiet.
    assert_eq!(idle.observe(at(base, 45), false), None);
    // 20s + 30s = 50s absolute → 30s since the reset → fires again.
    let fired = idle
        .observe(at(base, 50), false)
        .expect("re-fires after reset");
    assert_eq!(fired.condition, "idle-timeout");
    assert_eq!(fired.candidate_exit_reason, "timeout");
}

#[test]
fn dead_stream_fires_when_no_first_token_appears() {
    let base = Instant::now();
    let mut dead = FirstTokenDeadStream::new(60);

    assert_eq!(dead.observe(base, false), None);
    assert_eq!(dead.observe(at(base, 59), false), None);
    let fired = dead
        .observe(at(base, 60), false)
        .expect("dead stream should fire");
    assert_eq!(fired.condition, "first-token-dead-stream");
    assert_eq!(fired.candidate_exit_reason, "error");

    // Latched: it reports once, not every subsequent quiet poll.
    assert_eq!(dead.observe(at(base, 120), false), None);
}

#[test]
fn dead_stream_latches_once_the_first_token_is_seen() {
    let base = Instant::now();
    let mut dead = FirstTokenDeadStream::new(60);

    assert_eq!(dead.observe(base, false), None);
    // First token arrives before the threshold → the stream is alive.
    assert_eq!(dead.observe(at(base, 10), true), None);
    // Even long past the threshold with no further output, it never fires: a
    // stream that came alive is not dead.
    assert_eq!(dead.observe(at(base, 300), false), None);
}

#[test]
fn detector_set_reports_conditions_as_derived_observations() {
    let base = Instant::now();
    // idle 30s, first-token 60s: at 30s idle fires; the dead-stream is still
    // under its own threshold, so exactly one condition comes back.
    let mut detectors = Detectors::new(30, 60);

    assert!(detectors.observe(base, false).is_empty());
    let conditions = detectors.observe(at(base, 30), false);
    assert_eq!(conditions.len(), 1);
    assert_eq!(conditions[0].condition, "idle-timeout");

    // The observation is derived-fidelity and carries the candidate reason so a
    // supervisor can decide to reap — detection only.
    let obs = conditions[0].observation();
    assert_eq!(obs.kind, "raw");
    assert_eq!(obs.fidelity, "derived");
    assert_eq!(obs.payload["payload"]["source"], "detector");
    assert_eq!(obs.payload["payload"]["data"]["condition"], "idle-timeout");
    assert_eq!(
        obs.payload["payload"]["data"]["candidate_exit_reason"],
        "timeout"
    );
}

#[test]
fn detector_set_can_report_both_conditions_in_one_poll() {
    let base = Instant::now();
    // Equal thresholds: a fully silent session trips both at the same poll.
    let mut detectors = Detectors::new(45, 45);

    assert!(detectors.observe(base, false).is_empty());
    let mut conditions = detectors.observe(at(base, 45), false);
    conditions.sort_by_key(|c| c.condition);
    assert_eq!(conditions.len(), 2);
    assert_eq!(conditions[0].condition, "first-token-dead-stream");
    assert_eq!(conditions[1].condition, "idle-timeout");
}
