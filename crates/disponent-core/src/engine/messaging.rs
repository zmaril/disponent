//! The manager↔worker messaging primitive (notes/manager-worker-comms.md): the
//! `send` / `ack` / `messages` handlers, kept out of `engine.rs` so that file
//! stays under the size budget. A child module sees its parent's private items,
//! so these free functions reach `Engine`/`Ledger` internals directly and the
//! trait methods in `engine.rs` are thin delegators.
//!
//! Messages are the first ledger-owned control-plane entity (§11): disponent
//! mints them, no environment backs them, reconcile skips them, and durability
//! is the SQLite mirror.

use anyhow::{anyhow, bail};
use fluessig::data::Mutation;
use serde_json::json;
use uuid::Uuid;

use super::{event_mutation, now, Engine, Ledger};
use crate::catalog::upsert;
use crate::mcp_generated::{Message, MessagesFilter, SendTarget};
use crate::status::TERMINAL;

/// A control-plane Message row: disponent owns these (§11); mirrored like any
/// other entity, restored on rehydrate, skipped by reconcile.
pub(super) fn message_mutation(m: &Message) -> Mutation {
    upsert(
        "messages",
        &[
            "id",
            "created_at",
            "sender",
            "recipient",
            "session_uid",
            "body",
            "in_reply_to",
            "fanout_id",
            "topic",
            "acked_at",
        ],
        vec![vec![
            json!(m.id),
            json!(m.created_at),
            json!(m.sender),
            json!(m.recipient),
            json!(m.session_uid),
            json!(m.body),
            json!(m.in_reply_to),
            json!(m.fanout_id),
            json!(m.topic),
            json!(m.acked_at),
        ]],
    )
}

/// The read-side latest-wins collapse (§7): within scope, the newest Message
/// per (recipient, topic) supersedes older same-topic ones. `messages` is in
/// append (chronological) order, so the last occurrence per key is the newest.
/// Standalone messages (no topic) are never collapsed.
fn latest_per_topic(messages: Vec<Message>) -> Vec<Message> {
    use std::collections::HashMap;
    let mut latest: HashMap<(String, String), usize> = HashMap::new();
    for (i, m) in messages.iter().enumerate() {
        if let Some(topic) = &m.topic {
            latest.insert((m.recipient.clone(), topic.clone()), i);
        }
    }
    messages
        .into_iter()
        .enumerate()
        .filter(|(i, m)| match &m.topic {
            Some(topic) => latest.get(&(m.recipient.clone(), topic.clone())) == Some(i),
            None => true,
        })
        .map(|(_, m)| m)
        .collect()
}

/// The dispatch tags a session inherits (via its dispatch), empty if none.
fn tags_of(ledger: &Ledger, uid: &str) -> Vec<String> {
    ledger
        .sessions
        .iter()
        .find(|s| s.uid == uid)
        .and_then(|s| ledger.dispatches.iter().find(|d| d.id == s.dispatch_id))
        .and_then(|d| d.spec.tags.clone())
        .unwrap_or_default()
}

/// Live sessions (not terminal, not reaped) whose dispatch carries ANY of
/// `tags` — the send-time snapshot a tag fan-out resolves to (§8). A later tag
/// change never retroactively adds or removes an existing fan-out's recipients.
fn live_sessions_with_tags(ledger: &Ledger, tags: &[String]) -> Vec<String> {
    ledger
        .sessions
        .iter()
        .filter(|s| s.reaped_at.is_none() && !TERMINAL.contains(&s.state.as_str()))
        .filter(|s| {
            let owned = tags_of(ledger, &s.uid);
            tags.iter().any(|t| owned.contains(t))
        })
        .map(|s| s.uid.clone())
        .collect()
}

/// The one messaging primitive (§6). The core send is the Manager surface:
/// sender = manager, recipient resolved from `to`. Worker self-send (recipient
/// forced to the Manager, anchored to the bound session) is a worker-role MCP
/// concern — not wired here yet.
pub(super) fn send(
    engine: &Engine,
    to: Option<SendTarget>,
    body: String,
    in_reply_to: Option<String>,
    topic: Option<String>,
) -> anyhow::Result<Vec<Message>> {
    let target = to.ok_or_else(|| {
        anyhow!(
            "send needs a target (tags, sessions, or user); \
             worker self-send isn't wired yet"
        )
    })?;

    // Resolve the recipient party + the concrete anchor sessions. A tag
    // predicate is snapshotted at send time (§8) — a later tag change never
    // retroactively changes an existing fan-out's recipients. Exactly one
    // destination.
    let (recipient, anchors): (&str, Vec<String>) = {
        let ledger = engine.ledger.lock().unwrap();
        match (&target.user, &target.sessions, &target.tags) {
            (Some(uid), None, None) => ("user", vec![uid.clone()]),
            (None, Some(uids), None) => ("worker", uids.clone()),
            (None, None, Some(tags)) => ("worker", live_sessions_with_tags(&ledger, tags)),
            (None, None, None) => bail!("send target is empty: set tags, sessions, or user"),
            _ => bail!("send target must name exactly one of tags, sessions, or user"),
        }
    };

    // One fanoutId shared by every Message this send mints — a single recipient
    // is a fan-out of one. An unmatched tag set mints nothing (honest, not an
    // error); an explicit uid the ledger doesn't know errors.
    let created_at = now();
    let fanout_id = Uuid::now_v7().to_string();
    let mut minted = Vec::new();
    {
        let mut ledger = engine.ledger.lock().unwrap();
        let mut mutations = Vec::new();
        for anchor in &anchors {
            if !ledger.sessions.iter().any(|s| &s.uid == anchor) {
                bail!("no session {anchor}");
            }
            let message = Message {
                id: Uuid::now_v7().to_string(),
                created_at: created_at.clone(),
                sender: "manager".to_string(),
                recipient: recipient.to_string(),
                session_uid: anchor.clone(),
                body: body.clone(),
                in_reply_to: in_reply_to.clone(),
                fanout_id: fanout_id.clone(),
                topic: topic.clone(),
                acked_at: None,
            };
            // Project a `mail` breadcrumb on the anchor timeline (exact — a
            // record of disponent's own send, no env mediates it, §11).
            let event = ledger.push_event(
                anchor,
                "mail",
                json!({"kind": "mail", "payload": {
                    "messageId": message.id,
                    "sender": message.sender,
                    "recipient": message.recipient,
                    "fanoutId": message.fanout_id,
                    "topic": message.topic,
                }}),
            );
            mutations.push(message_mutation(&message));
            mutations.push(event_mutation(&event));
            ledger.messages.push(message.clone());
            minted.push(message);
        }
        ledger.mirror(mutations)?;
    }

    // Best-effort backend delivery (§6): a message to an EXPLICIT concrete
    // worker (`sessions`) also lands on its live prompt via the interact
    // capability — the legacy `send` behavior, now one possible delivery. A tag
    // fan-out stays pull-only (§7); a non-running/unreachable anchor just keeps
    // the durable Message for the recipient to pull.
    if target.sessions.is_some() {
        for anchor in &anchors {
            let (running, handle, backend, adapter) = {
                let ledger = engine.ledger.lock().unwrap();
                let (backend, adapter) = engine.routing(&ledger, anchor);
                let session = ledger.sessions.iter().find(|s| &s.uid == anchor);
                (
                    session.map(|s| s.state == "running").unwrap_or(false),
                    session.and_then(|s| s.env_handle.clone()),
                    backend,
                    adapter,
                )
            };
            if !running {
                continue;
            }
            if let (Some(backend), Some(handle), Some(adapter)) = (backend, handle, adapter) {
                if let Ok(compute) = backend.compute(&handle) {
                    let _ = adapter.prompt(&*compute, &body);
                }
            }
        }
    }
    Ok(minted)
}

/// Stamp a message received/handled (§7). Idempotent; a re-ack keeps the first
/// timestamp. Manager-observable across a `fanoutId`.
pub(super) fn ack(engine: &Engine, message_id: String) -> anyhow::Result<()> {
    let mut ledger = engine.ledger.lock().unwrap();
    let idx = ledger
        .messages
        .iter()
        .position(|m| m.id == message_id)
        .ok_or_else(|| anyhow!("no message {message_id}"))?;
    if ledger.messages[idx].acked_at.is_none() {
        ledger.messages[idx].acked_at = Some(now());
    }
    let snapshot = ledger.messages[idx].clone();
    ledger.mirror(vec![message_mutation(&snapshot)])?;
    Ok(())
}

/// Read Messages, filtered. The Manager's fan-out ack view (`{fanoutId}`) and a
/// recipient's inbox (`{recipient, sessionUid}`); `latestPerTopic` applies the
/// read-side latest-wins collapse per (recipient, topic), §7.
pub(super) fn messages(
    engine: &Engine,
    filter: Option<MessagesFilter>,
) -> anyhow::Result<Vec<Message>> {
    let ledger = engine.ledger.lock().unwrap();
    let f = filter.unwrap_or_default();
    let out: Vec<Message> = ledger
        .messages
        .iter()
        .filter(|m| f.fanout_id.as_deref().is_none_or(|x| x == m.fanout_id))
        .filter(|m| f.recipient.as_deref().is_none_or(|x| x == m.recipient))
        .filter(|m| f.session_uid.as_deref().is_none_or(|x| x == m.session_uid))
        .filter(|m| {
            f.topic
                .as_deref()
                .is_none_or(|x| Some(x) == m.topic.as_deref())
        })
        .cloned()
        .collect();
    if f.latest_per_topic.unwrap_or(false) {
        Ok(latest_per_topic(out))
    } else {
        Ok(out)
    }
}

#[allow(clippy::derivable_impls)]
impl Default for MessagesFilter {
    fn default() -> Self {
        MessagesFilter {
            fanout_id: None,
            recipient: None,
            session_uid: None,
            topic: None,
            latest_per_topic: None,
        }
    }
}
