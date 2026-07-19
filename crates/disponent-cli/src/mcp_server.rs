//! The stdio MCP transport: newline-delimited JSON-RPC 2.0 in, same out.
//! Tool names, schemas, and argument decoding are all generated
//! (disponent_core::mcp_generated); this file is only the wire loop and the
//! role gate.

use std::io::{BufRead, Write};

use anyhow::{anyhow, bail};
use disponent_core::mcp_generated;
use disponent_core::Engine;
use serde_json::{json, Value};

use crate::Role;

/// The protocol revision we answer with when the client doesn't name one.
const PROTOCOL_VERSION: &str = "2024-11-05";

/// The worker-writable tool names — the ops the schema flags `workerHint`
/// (`#[fluessig(worker)]`), read straight from the generated manifest. Today
/// that's exactly `send` + `ack`; the set is schema-owned, not hardcoded here.
/// The surface gate ([`tools_for`]) widens on the same flag, and the worker-role
/// server intercepts these to self-scope them (§9); the no-dispatch invariant is
/// that no `workerHint` op is a dispatch/spawn/reach-another-session op.
fn worker_hint_tools() -> Vec<String> {
    let manifest: Value =
        serde_json::from_str(mcp_generated::TOOLS_JSON).expect("generated manifest parses");
    manifest["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .filter(|t| t["annotations"]["workerHint"] == json!(true))
        .filter_map(|t| t["name"].as_str().map(String::from))
        .collect()
}

pub fn serve(role: Role, sink: Option<&str>, bound_session: Option<String>) -> anyhow::Result<()> {
    let engine = Engine::open(sink)?;
    let tools = tools_for(role);
    let worker_writes = worker_hint_tools();
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
                    match call_tool(
                        &engine,
                        role,
                        bound_session.as_deref(),
                        &worker_writes,
                        name,
                        &args,
                    ) {
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

/// The generated manifest, gated by role. A supervisor sees everything; a
/// worker sees the read-only observe tools (`readOnlyHint`) PLUS the ops the
/// schema flags worker-safe (`workerHint`) — today its two self-scoped writes,
/// `send` (up to its Manager) and `ack` (its own inbox). Every dispatch/spawn/
/// reach-another-session op carries neither hint, so it stays off the worker
/// surface — the no-recursion invariant holds by tool absence.
///
/// The worker-writable set is now DECLARED IN THE SCHEMA via `#[fluessig(worker)]`
/// → the MCP `workerHint` annotation (the sibling of `@readonly`→`readOnlyHint`),
/// not a hardcoded name list here. The invariant is checkable in CI and moves
/// with the schema (see notes/manager-worker-comms.md §5).
fn tools_for(role: Role) -> Vec<Value> {
    let manifest: Value =
        serde_json::from_str(mcp_generated::TOOLS_JSON).expect("generated manifest parses");
    manifest["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .filter(|t| {
            role == Role::Supervisor
                || t["annotations"]["readOnlyHint"] == json!(true)
                || t["annotations"]["workerHint"] == json!(true)
        })
        .cloned()
        .collect()
}

/// Route a tool call. A worker's `workerHint` writes (`send`/`ack`) are
/// intercepted and self-scoped to its bound session (§9); everything else runs
/// the generated Manager surface. The intercept is what forces a worker's `send`
/// to its Manager and confines its `ack` to its own inbox — a worker can never
/// name a recipient. `worker_writes` is the schema-flagged set from the manifest.
fn call_tool(
    engine: &Engine,
    role: Role,
    bound_session: Option<&str>,
    worker_writes: &[String],
    name: &str,
    args: &Value,
) -> anyhow::Result<Value> {
    if role == Role::Worker && worker_writes.iter().any(|w| w == name) {
        worker_call(engine, bound_session, name, args)
    } else {
        mcp_generated::dispatch(engine, name, args)
    }
}

/// A worker's self-scoped `send`/`ack`, resolved from the server-bound session
/// identity rather than anything the worker supplies. `send` forces recipient =
/// the Manager and rejects an explicit `to` (a worker addresses no one); `ack`
/// is confined to the bound session's own inbox by the engine.
fn worker_call(
    engine: &Engine,
    bound_session: Option<&str>,
    name: &str,
    args: &Value,
) -> anyhow::Result<Value> {
    let bound = bound_session.ok_or_else(|| {
        anyhow!("this worker server has no bound session; start it with --bound-session <uid>")
    })?;
    match name {
        "disponent_send" => {
            // The worker names no recipient — the binding is what says who it is.
            if args.get("to").is_some_and(|v| !v.is_null()) {
                bail!(
                    "a worker send takes no recipient — it always goes to your Manager; drop `to`"
                );
            }
            let body = args
                .get("body")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("missing required argument: body"))?
                .to_string();
            let in_reply_to = args
                .get("inReplyTo")
                .and_then(Value::as_str)
                .map(String::from);
            let topic = args.get("topic").and_then(Value::as_str).map(String::from);
            Ok(serde_json::to_value(engine.worker_send(
                bound,
                body,
                in_reply_to,
                topic,
            )?)?)
        }
        "disponent_ack" => {
            let message_id = args
                .get("messageId")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("missing required argument: messageId"))?
                .to_string();
            engine.worker_ack(bound, message_id)?;
            Ok(Value::Null)
        }
        // WORKER_WRITE_TOOLS and this match are kept in lockstep by construction.
        other => bail!("no worker intercept for {other}"),
    }
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
