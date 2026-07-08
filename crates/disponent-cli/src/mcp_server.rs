//! The stdio MCP transport: newline-delimited JSON-RPC 2.0 in, same out.
//! Tool names, schemas, and argument decoding are all generated
//! (disponent_core::mcp_generated); this file is only the wire loop and the
//! role gate.

use std::io::{BufRead, Write};

use disponent_core::mcp_generated;
use disponent_core::Engine;
use serde_json::{json, Value};

use crate::Role;

/// The protocol revision we answer with when the client doesn't name one.
const PROTOCOL_VERSION: &str = "2024-11-05";

pub fn serve(role: Role, sink: Option<&str>) -> anyhow::Result<()> {
    let engine = Engine::open(sink)?;
    let tools = tools_for(role);
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout().lock();

    let mut reader = stdin.lock();
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => break, // EOF: the client hung up
            Ok(_) => {}
            // Transient stdin hiccups (a fifo or nonblocking pipe can surface
            // WouldBlock) must not kill a server with live sessions behind it.
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) =>
            {
                std::thread::sleep(std::time::Duration::from_millis(20));
                continue;
            }
            Err(e) => return Err(e.into()),
        }
        if line.trim().is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                respond(
                    &mut stdout,
                    error(Value::Null, -32700, &format!("parse error: {e}")),
                )?;
                continue;
            }
        };
        // Notifications (no id) get no response, per JSON-RPC.
        let Some(id) = msg.get("id").cloned() else {
            continue;
        };
        let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
        let params = msg.get("params").cloned().unwrap_or(json!({}));
        let reply = match method {
            "initialize" => {
                let version = params
                    .get("protocolVersion")
                    .and_then(Value::as_str)
                    .unwrap_or(PROTOCOL_VERSION);
                result(
                    id,
                    json!({
                        "protocolVersion": version,
                        "capabilities": {"tools": {}},
                        "serverInfo": {
                            "name": "disponent",
                            "version": env!("CARGO_PKG_VERSION"),
                        },
                    }),
                )
            }
            "ping" => result(id, json!({})),
            "tools/list" => result(id, json!({"tools": tools})),
            "tools/call" => {
                let name = params.get("name").and_then(Value::as_str).unwrap_or("");
                let args = params.get("arguments").cloned().unwrap_or(json!({}));
                if !tools.iter().any(|t| t["name"] == name) {
                    error(id, -32602, &format!("unknown tool: {name}"))
                } else {
                    match mcp_generated::dispatch(&engine, name, &args) {
                        Ok(v) => result(
                            id,
                            json!({"content": [{"type": "text",
                                "text": serde_json::to_string_pretty(&v)?}]}),
                        ),
                        // Tool-level failures ride inside the result, isError
                        // set — the JSON-RPC error channel is for protocol faults.
                        Err(e) => result(
                            id,
                            json!({"content": [{"type": "text", "text": e.to_string()}],
                                "isError": true}),
                        ),
                    }
                }
            }
            _ => error(id, -32601, &format!("method not found: {method}")),
        };
        respond(&mut stdout, reply)?;
    }
    Ok(())
}

/// The generated manifest, gated by role: a worker sees only the tools whose
/// manifest entry carries readOnlyHint (observe, never act).
fn tools_for(role: Role) -> Vec<Value> {
    let manifest: Value =
        serde_json::from_str(mcp_generated::TOOLS_JSON).expect("generated manifest parses");
    manifest["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .filter(|t| role == Role::Supervisor || t["annotations"]["readOnlyHint"] == json!(true))
        .cloned()
        .collect()
}

fn result(id: Value, body: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": body})
}

fn error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

fn respond(out: &mut impl Write, reply: Value) -> anyhow::Result<()> {
    serde_json::to_writer(&mut *out, &reply)?;
    out.write_all(b"\n")?;
    out.flush()?;
    Ok(())
}
