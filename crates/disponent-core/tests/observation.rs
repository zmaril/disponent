//! Observation end-to-end: a real tmux worker's terminal output lands in the
//! event stream as scraped raw events (the pool → collector path), and an
//! OTLP/http-json export posted at the receiver lands as exact events.

use std::io::{Read, Write};
use std::time::{Duration, Instant};

use disponent_core::local::LocalTmux;
use disponent_core::mcp_generated::{DisponentMcp, Event};
use disponent_core::Engine;

mod common;

fn have_tmux() -> bool {
    std::process::Command::new("tmux")
        .arg("-V")
        .output()
        .is_ok()
}

fn events_of(engine: &Engine, uid: &str) -> Vec<Event> {
    engine
        .events(
            Some(serde_json::from_value(serde_json::json!({"sessionUid": uid})).unwrap()),
            None,
            Some(1000),
        )
        .unwrap()
}

#[test]
fn terminal_output_becomes_scraped_events() {
    if !have_tmux() {
        eprintln!("tmux not installed; skipping");
        return;
    }
    std::env::set_var("DISPONENT_OBSERVE_INTERVAL_MS", "100");
    let socket = format!("dsp-test-{}-obs", std::process::id());
    let root = std::env::temp_dir().join(format!("disponent-obs-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    // an agent that talks over time, like a real one
    let agent = "echo FIRST_WORDS; sleep 1; echo LATER_WORDS; sleep 600; echo";
    let engine = Engine::with_backend(LocalTmux::sandboxed(&socket, root.clone(), agent));

    let session = engine
        .dispatch(
            serde_json::from_value(serde_json::json!({"brief": "talk", "env": "local"})).unwrap(),
        )
        .unwrap();
    common::wait_for(&engine, &session.uid, "running", Duration::from_secs(10));

    // both utterances arrive as raw scraped events, in order
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let scraped: String = events_of(&engine, &session.uid)
            .iter()
            .filter(|e| e.kind == "raw" && e.fidelity == "scraped")
            .map(|e| {
                e.payload["payload"]["data"]
                    .as_str()
                    .unwrap_or("")
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        if scraped.contains("FIRST_WORDS") && scraped.contains("LATER_WORDS") {
            assert!(
                scraped.find("FIRST_WORDS") < scraped.find("LATER_WORDS"),
                "observation order holds"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "terminal never observed: {scraped}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // after reap the watcher is gone: no further scraped events accumulate
    engine.reap(session.uid.clone()).unwrap();
    let count = |uid: &str| {
        events_of(&engine, uid)
            .iter()
            .filter(|e| e.fidelity == "scraped")
            .count()
    };
    let after_reap = count(&session.uid);
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(
        count(&session.uid),
        after_reap,
        "observer reaped with session"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn otlp_posts_become_exact_events() {
    let mut engine = Engine::with_backend(LocalTmux::dry_run());
    let port = engine.start_otel(0).unwrap();

    let session = engine
        .dispatch(
            serde_json::from_value(serde_json::json!({"brief": "observe me", "env": "local"}))
                .unwrap(),
        )
        .unwrap();
    common::wait_for(&engine, &session.uid, "running", Duration::from_secs(5));

    // one export carrying an api_request + a user_prompt for our session,
    // plus a record for a session nobody knows (must be dropped)
    let body = serde_json::json!({"resourceLogs": [
        {
            "resource": {"attributes": [
                {"key": "disponent.session_uid", "value": {"stringValue": session.uid}}
            ]},
            "scopeLogs": [{"logRecords": [
                {"attributes": [
                    {"key": "event.name", "value": {"stringValue": "claude_code.api_request"}},
                    {"key": "model", "value": {"stringValue": "claude-opus-4-8"}},
                    {"key": "input_tokens", "value": {"intValue": "900"}},
                    {"key": "output_tokens", "value": {"intValue": "120"}},
                    {"key": "cost_usd", "value": {"doubleValue": 0.15}}
                ]},
                {"attributes": [
                    {"key": "event.name", "value": {"stringValue": "claude_code.user_prompt"}},
                    {"key": "prompt_length", "value": {"intValue": "9"}}
                ]}
            ]}]
        },
        {
            "resource": {"attributes": [
                {"key": "disponent.session_uid", "value": {"stringValue": "nobody-home"}}
            ]},
            "scopeLogs": [{"logRecords": [
                {"attributes": [
                    {"key": "event.name", "value": {"stringValue": "claude_code.user_prompt"}}
                ]}
            ]}]
        }
    ]})
    .to_string();

    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    write!(
        stream,
        "POST /v1/logs HTTP/1.1\r\nHost: disponent\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
    .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let events = events_of(&engine, &session.uid);
        let usage = events.iter().find(|e| e.kind == "usage");
        let prompt = events
            .iter()
            .find(|e| e.kind == "message" && e.payload["payload"]["role"] == "user");
        if let (Some(u), Some(p)) = (usage, prompt) {
            assert_eq!(u.fidelity, "exact");
            assert_eq!(u.payload["payload"]["inputTokens"], 900);
            assert_eq!(u.payload["payload"]["costCents"], "15");
            assert_eq!(p.payload["payload"]["text"], "[prompt, 9 chars]");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "otel events never landed: {events:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // the unknown session got nothing
    assert!(events_of(&engine, "nobody-home").is_empty());
}
