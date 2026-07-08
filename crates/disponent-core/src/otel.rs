//! The exact observation tier: Claude Code speaks OpenTelemetry, and its log
//! events are disponent's event vocabulary wearing OTLP — user_prompt and
//! assistant_response are `message`s, tool_result is `toolResult`,
//! api_request is `usage` with real token counts and cost. Workers are
//! launched with `OTEL_LOGS_EXPORTER=otlp` pointed at this receiver and
//! `OTEL_RESOURCE_ATTRIBUTES=disponent.session_uid=<uid>` stamping every
//! record, so ingestion is: parse OTLP/http-json, read the uid off the
//! resource, fold each record into the session's timeline at `exact`
//! fidelity. Anything unmapped survives as a `raw` event — nothing observed
//! is dropped.

use std::collections::BTreeMap;

use anyhow::anyhow;
use serde_json::{json, Value};

/// The resource attribute the launch env stamps on every worker record.
pub const SESSION_ATTR: &str = "disponent.session_uid";

/// The env block a worker's runner exports to wire its agent to `endpoint`.
/// Only claude reads these today; they're inert for other agents.
pub fn worker_env(endpoint: &str, session_uid: &str) -> String {
    [
        "export CLAUDE_CODE_ENABLE_TELEMETRY=1".to_string(),
        "export OTEL_LOGS_EXPORTER=otlp".to_string(),
        "export OTEL_METRICS_EXPORTER=none".to_string(),
        "export OTEL_EXPORTER_OTLP_PROTOCOL=http/json".to_string(),
        format!(
            "export OTEL_EXPORTER_OTLP_ENDPOINT={}",
            crate::backend::shq(endpoint)
        ),
        format!("export OTEL_RESOURCE_ATTRIBUTES=disponent.session_uid={session_uid}"),
        "export OTEL_LOGS_EXPORT_INTERVAL=2000".to_string(),
    ]
    .join("\n")
}

/// One ingested record: which session, and the events-row shape.
pub struct Ingested {
    pub session_uid: String,
    pub kind: String,
    pub fidelity: String,
    pub payload: Value,
}

fn attr_map(attrs: Option<&Vec<Value>>) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    for kv in attrs.into_iter().flatten() {
        let Some(key) = kv["key"].as_str() else {
            continue;
        };
        let v = &kv["value"];
        let val = if let Some(s) = v["stringValue"].as_str() {
            json!(s)
        } else if let Some(s) = v["intValue"].as_str() {
            // OTLP JSON encodes int64 as strings
            s.parse::<i64>().map(|i| json!(i)).unwrap_or(json!(s))
        } else if let Some(i) = v["intValue"].as_i64() {
            json!(i)
        } else if let Some(f) = v["doubleValue"].as_f64() {
            json!(f)
        } else if let Some(b) = v["boolValue"].as_bool() {
            json!(b)
        } else {
            v.clone()
        };
        out.insert(key.to_string(), val);
    }
    out
}

/// claude_code event → (kind, payload envelope). Pure; unit-tested. Live
/// exports carry bare names (`api_request`); the docs show them prefixed
/// (`claude_code.api_request`) — accept both.
pub fn map_event(name: &str, attrs: &BTreeMap<String, Value>) -> (String, Value) {
    let s = |k: &str| attrs.get(k).and_then(Value::as_str).unwrap_or_default();
    let n = |k: &str| attrs.get(k).and_then(Value::as_i64);
    match name.strip_prefix("claude_code.").unwrap_or(name) {
        "user_prompt" => {
            let text = attrs
                .get("prompt")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| format!("[prompt, {} chars]", n("prompt_length").unwrap_or(0)));
            (
                "message".into(),
                json!({"kind": "message", "payload": {"role": "user", "text": text}}),
            )
        }
        "assistant_response" => {
            let text = attrs
                .get("response")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| {
                    format!("[response, {} chars]", n("response_length").unwrap_or(0))
                });
            (
                "message".into(),
                json!({"kind": "message", "payload": {"role": "assistant", "text": text}}),
            )
        }
        "tool_result" => (
            "tool_result".into(),
            json!({"kind": "toolResult", "payload": {
                "tool": s("tool_name"),
                "ok": attrs.get("success").and_then(Value::as_bool)
                    .or_else(|| attrs.get("success").and_then(Value::as_str).map(|v| v == "true"))
                    .unwrap_or(false),
                "output": format!("{}ms{}", n("duration_ms").unwrap_or(0),
                    if s("error_type").is_empty() { String::new() }
                    else { format!(" ({})", s("error_type")) }),
            }}),
        ),
        "tool_decision" => (
            "tool_call".into(),
            json!({"kind": "toolCall", "payload": {
                "tool": s("tool_name"),
                "input": {"decision": s("decision"), "source": s("source")},
            }}),
        ),
        "api_request" => (
            "usage".into(),
            json!({"kind": "usage", "payload": {
                "modelId": s("model"),
                "inputTokens": n("input_tokens").unwrap_or(0),
                "outputTokens": n("output_tokens").unwrap_or(0),
                "costCents": attrs.get("cost_usd").and_then(Value::as_f64)
                    .map(|c| (c * 100.0).round() as i64).unwrap_or(0).to_string(),
            }}),
        ),
        other => (
            "raw".into(),
            json!({"kind": "raw", "payload":
                {"source": other, "data": Value::Object(attrs.clone().into_iter().collect())}}),
        ),
    }
}

/// Parse one OTLP/http-json logs export into ingestible events. Records with
/// no session stamp are dropped (they're not a worker of ours).
pub fn parse_logs_export(body: &Value) -> Vec<Ingested> {
    let mut out = Vec::new();
    for rl in body["resourceLogs"].as_array().into_iter().flatten() {
        let resource = attr_map(rl["resource"]["attributes"].as_array());
        let Some(session_uid) = resource.get(SESSION_ATTR).and_then(Value::as_str) else {
            continue;
        };
        for sl in rl["scopeLogs"].as_array().into_iter().flatten() {
            for rec in sl["logRecords"].as_array().into_iter().flatten() {
                let attrs = attr_map(rec["attributes"].as_array());
                let name = attrs
                    .get("event.name")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .or_else(|| rec["body"]["stringValue"].as_str().map(str::to_string))
                    .unwrap_or_default();
                if name.is_empty() {
                    continue;
                }
                let (kind, payload) = map_event(&name, &attrs);
                out.push(Ingested {
                    session_uid: session_uid.to_string(),
                    kind,
                    fidelity: "exact".to_string(),
                    payload,
                });
            }
        }
    }
    out
}

/// Serve OTLP/http-json on `port` (0 = ephemeral), handing every ingested
/// event to `sink`. Returns the bound port; the listener runs on its own
/// thread for the life of the process.
pub fn serve<F>(port: u16, sink: F) -> anyhow::Result<u16>
where
    F: Fn(Ingested) + Send + 'static,
{
    let server = tiny_http::Server::http(("127.0.0.1", port))
        .map_err(|e| anyhow!("otel receiver on port {port}: {e}"))?;
    let bound = match server.server_addr() {
        tiny_http::ListenAddr::IP(a) => a.port(),
        _ => port,
    };
    std::thread::spawn(move || {
        for mut request in server.incoming_requests() {
            let mut body = String::new();
            let _ = std::io::Read::read_to_string(request.as_reader(), &mut body);
            let ok = request.url().ends_with("/v1/logs")
                && serde_json::from_str::<Value>(&body)
                    .map(|v| {
                        for e in parse_logs_export(&v) {
                            sink(e);
                        }
                    })
                    .is_ok();
            // OTLP/HTTP success is an empty JSON object; anything else 400s.
            let (code, payload) = if ok { (200, "{}") } else { (400, "{}") };
            let _ = request.respond(
                tiny_http::Response::from_string(payload)
                    .with_status_code(code)
                    .with_header(
                        tiny_http::Header::from_bytes(
                            &b"Content-Type"[..],
                            &b"application/json"[..],
                        )
                        .expect("static header"),
                    ),
            );
        }
    });
    Ok(bound)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kv(k: &str, v: Value) -> Value {
        let tagged = match &v {
            Value::String(s) => json!({"stringValue": s}),
            Value::Number(x) if x.is_i64() => json!({"intValue": x.to_string()}),
            Value::Number(x) => json!({"doubleValue": x}),
            Value::Bool(b) => json!({"boolValue": b}),
            other => other.clone(),
        };
        json!({"key": k, "value": tagged})
    }

    fn export(uid: &str, records: Vec<Vec<Value>>) -> Value {
        json!({"resourceLogs": [{
            "resource": {"attributes": [kv(SESSION_ATTR, json!(uid))]},
            "scopeLogs": [{"logRecords":
                records.into_iter().map(|attributes| json!({"attributes": attributes})).collect::<Vec<_>>()
            }]
        }]})
    }

    #[test]
    fn api_requests_become_usage_events() {
        let body = export(
            "u1",
            vec![vec![
                kv("event.name", json!("claude_code.api_request")),
                kv("model", json!("claude-opus-4-8")),
                kv("input_tokens", json!(1200)),
                kv("output_tokens", json!(300)),
                kv("cost_usd", json!(0.42)),
            ]],
        );
        let events = parse_logs_export(&body);
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!((e.session_uid.as_str(), e.kind.as_str()), ("u1", "usage"));
        assert_eq!(e.fidelity, "exact");
        assert_eq!(e.payload["payload"]["modelId"], "claude-opus-4-8");
        assert_eq!(e.payload["payload"]["inputTokens"], 1200);
        assert_eq!(e.payload["payload"]["costCents"], "42");
    }

    #[test]
    fn prompts_redact_gracefully_and_tools_map() {
        let body = export(
            "u1",
            vec![
                vec![
                    kv("event.name", json!("claude_code.user_prompt")),
                    kv("prompt_length", json!(11)),
                ],
                vec![
                    kv("event.name", json!("claude_code.tool_result")),
                    kv("tool_name", json!("Bash")),
                    kv("success", json!(true)),
                    kv("duration_ms", json!(88)),
                ],
                vec![kv("event.name", json!("claude_code.compaction"))],
            ],
        );
        let events = parse_logs_export(&body);
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].payload["payload"]["text"], "[prompt, 11 chars]");
        assert_eq!(events[1].kind, "tool_result");
        assert_eq!(events[1].payload["payload"]["tool"], "Bash");
        assert_eq!(events[1].payload["payload"]["ok"], true);
        assert_eq!(events[2].kind, "raw", "unmapped events survive as raw");
        // the raw source is the normalized (bare) name, like live exports use
        assert_eq!(events[2].payload["payload"]["source"], "compaction");
    }

    #[test]
    fn unstamped_records_are_not_ours() {
        let body = json!({"resourceLogs": [{
            "resource": {"attributes": []},
            "scopeLogs": [{"logRecords": [
                {"attributes": [kv("event.name", json!("claude_code.user_prompt"))]}
            ]}]
        }]});
        assert!(parse_logs_export(&body).is_empty());
    }

    #[test]
    fn worker_env_wires_the_exporter() {
        let env = worker_env("http://127.0.0.1:4318", "u-9");
        assert!(env.contains("CLAUDE_CODE_ENABLE_TELEMETRY=1"));
        assert!(env.contains("OTEL_EXPORTER_OTLP_ENDPOINT='http://127.0.0.1:4318'"));
        assert!(env.contains("disponent.session_uid=u-9"));
    }
}
