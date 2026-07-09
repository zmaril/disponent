//! The manager↔worker messaging primitive (notes/manager-worker-comms.md): a
//! Manager `send` mints one Message per resolved recipient sharing one
//! `fanoutId`; tag selection snapshots the live sessions at send time; the
//! `messages` read offers the (recipient, topic) latest-wins collapse; `ack`
//! stamps a message handled. These run over the memory engine (no backend): a
//! no-backend dispatch queues, and a queued session is a live recipient.

use disponent_core::mcp_generated::{DispatchSpec, DisponentMcp, MessagesFilter, SendTarget};
use disponent_core::Engine;

/// A dispatch to the shipped `local` env carrying selection tags. With no
/// backend registered the session queues (a live, non-terminal recipient).
fn tagged_spec(tags: &[&str]) -> DispatchSpec {
    serde_json::from_value(serde_json::json!({
        "brief": "tagged worker",
        "env": "local",
        "tags": tags,
    }))
    .unwrap()
}

fn to(value: serde_json::Value) -> SendTarget {
    serde_json::from_value(value).unwrap()
}

#[test]
fn send_mints_one_message_per_recipient_sharing_a_fanout_id() {
    let engine = Engine::new();
    let a = engine.dispatch(tagged_spec(&["projectA"])).unwrap();
    let b = engine.dispatch(tagged_spec(&["projectA"])).unwrap();

    let minted = engine
        .send(
            Some(to(serde_json::json!({ "sessions": [a.uid, b.uid] }))),
            "use bun, not npm".into(),
            None,
            Some("package-manager".into()),
        )
        .unwrap();

    assert_eq!(minted.len(), 2, "one Message per recipient");
    // one shared fanoutId (a broadcast), distinct per-message ids
    assert_eq!(minted[0].fanout_id, minted[1].fanout_id, "shared fanoutId");
    assert_ne!(minted[0].id, minted[1].id, "distinct message ids");
    for m in &minted {
        assert_eq!(m.sender, "manager");
        assert_eq!(m.recipient, "worker");
        assert_eq!(m.topic.as_deref(), Some("package-manager"));
        assert!(m.acked_at.is_none(), "unacked at send");
    }

    // each rides its anchor session's timeline as an exact `mail` event
    for (m, anchor) in minted.iter().zip([&a.uid, &b.uid]) {
        let opts = serde_json::from_value(serde_json::json!({ "sessionUid": anchor })).unwrap();
        let events = engine.events(Some(opts), None, None).unwrap();
        let mail = events
            .iter()
            .find(|e| e.kind == "mail")
            .expect("a mail event");
        assert_eq!(mail.fidelity, "exact");
        assert_eq!(mail.payload["payload"]["messageId"], m.id);
        assert_eq!(mail.payload["payload"]["fanoutId"], m.fanout_id);
    }
}

#[test]
fn tag_selection_snapshots_the_live_sessions_at_send_time() {
    let engine = Engine::new();
    let a = engine.dispatch(tagged_spec(&["projectA"])).unwrap();
    let _b = engine.dispatch(tagged_spec(&["projectB"])).unwrap();

    // a tag fan-out resolves ONLY the matching-tag sessions live at send time.
    let first = engine
        .send(
            Some(to(serde_json::json!({ "tags": ["projectA"] }))),
            "first".into(),
            None,
            Some("pm".into()),
        )
        .unwrap();
    assert_eq!(first.len(), 1, "only the one projectA session");
    assert_eq!(first[0].session_uid, a.uid);

    // a session that joins projectA AFTER the send is not retroactively added
    // to the earlier fan-out; the next fan-out reaches it.
    let c = engine.dispatch(tagged_spec(&["projectA"])).unwrap();
    let second = engine
        .send(
            Some(to(serde_json::json!({ "tags": ["projectA"] }))),
            "second".into(),
            None,
            Some("pm".into()),
        )
        .unwrap();
    let hit: Vec<&str> = second.iter().map(|m| m.session_uid.as_str()).collect();
    assert_eq!(hit.len(), 2, "the snapshot now includes the late joiner");
    assert!(hit.contains(&a.uid.as_str()) && hit.contains(&c.uid.as_str()));
    assert_ne!(first[0].fanout_id, second[0].fanout_id, "distinct fan-outs");

    // an unmatched tag mints nothing (honest, not an error)
    let none = engine
        .send(
            Some(to(serde_json::json!({ "tags": ["nope"] }))),
            "nobody".into(),
            None,
            None,
        )
        .unwrap();
    assert!(none.is_empty());
}

#[test]
fn messages_read_collapses_latest_per_recipient_topic() {
    let engine = Engine::new();
    let a = engine.dispatch(tagged_spec(&["projectA"])).unwrap();

    // two same-topic directives to the same recipient, newest wins
    engine
        .send(
            Some(to(serde_json::json!({ "sessions": [a.uid] }))),
            "use pnpm".into(),
            None,
            Some("package-manager".into()),
        )
        .unwrap();
    let bun = engine
        .send(
            Some(to(serde_json::json!({ "sessions": [a.uid] }))),
            "actually, use bun".into(),
            None,
            Some("package-manager".into()),
        )
        .unwrap();
    // a standalone (no-topic) message is never collapsed
    engine
        .send(
            Some(to(serde_json::json!({ "sessions": [a.uid] }))),
            "unrelated note".into(),
            None,
            None,
        )
        .unwrap();

    let inbox_filter: MessagesFilter = serde_json::from_value(serde_json::json!({
        "recipient": "worker",
        "sessionUid": a.uid,
    }))
    .unwrap();

    // without the collapse, all three are present
    let all = engine.messages(Some(inbox_filter.clone())).unwrap();
    assert_eq!(all.len(), 3);

    // with latestPerTopic, the stale pnpm directive is superseded by bun; the
    // no-topic note survives.
    let latest_filter: MessagesFilter = serde_json::from_value(serde_json::json!({
        "recipient": "worker",
        "sessionUid": a.uid,
        "latestPerTopic": true,
    }))
    .unwrap();
    let latest = engine.messages(Some(latest_filter)).unwrap();
    assert_eq!(latest.len(), 2, "pnpm superseded by bun; note kept");
    let bodies: Vec<&str> = latest.iter().map(|m| m.body.as_str()).collect();
    assert!(bodies.contains(&"actually, use bun"));
    assert!(bodies.contains(&"unrelated note"));
    assert!(!bodies.contains(&"use pnpm"), "stale directive dropped");
    assert!(latest.iter().any(|m| m.id == bun[0].id));
}

#[test]
fn ack_stamps_a_message_and_shows_fanout_progress() {
    let engine = Engine::new();
    let a = engine.dispatch(tagged_spec(&["x"])).unwrap();
    let b = engine.dispatch(tagged_spec(&["x"])).unwrap();

    let minted = engine
        .send(
            Some(to(serde_json::json!({ "tags": ["x"] }))),
            "directive".into(),
            None,
            None,
        )
        .unwrap();
    assert_eq!(minted.len(), 2);
    let fanout = minted[0].fanout_id.clone();

    // one recipient acks
    engine.ack(minted[0].id.clone()).unwrap();
    // ack is idempotent (re-ack keeps the first stamp)
    engine.ack(minted[0].id.clone()).unwrap();

    let progress: MessagesFilter =
        serde_json::from_value(serde_json::json!({ "fanoutId": fanout })).unwrap();
    let rows = engine.messages(Some(progress)).unwrap();
    let acked = rows.iter().filter(|m| m.acked_at.is_some()).count();
    assert_eq!(acked, 1, "1 of 2 picked up the directive");

    // the acked one is the recipient we acked; the other is still pending
    let one = rows.iter().find(|m| m.id == minted[0].id).unwrap();
    assert!(one.acked_at.is_some());
    let _ = (a, b);

    // acking an unknown message is an honest error
    assert!(engine.ack("no-such-id".into()).is_err());
}

#[test]
fn send_with_no_target_is_an_honest_capability_edge() {
    let engine = Engine::new();
    // The core send is the Manager surface; worker self-send (recipient forced
    // to the Manager) isn't wired yet — a targetless send says so, never fakes.
    let err = engine.send(None, "hi".into(), None, None).unwrap_err();
    assert!(
        err.to_string().contains("worker self-send isn't wired yet"),
        "unexpected: {err}"
    );
}
