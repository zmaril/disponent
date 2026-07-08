//! End-to-end over the real wire: spawn `disponent mcp`, speak newline-delimited
//! JSON-RPC on its stdio, and walk the whole tool flow — initialize, list,
//! dispatch, observe, reap. Plus the role gate: a worker can look, not act.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use serde_json::{json, Value};

struct Server {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: i64,
}

impl Server {
    fn start(role: &str) -> Server {
        let mut child = Command::new(env!("CARGO_BIN_EXE_disponent"))
            .args(["mcp", "--role", role, "--sink", "none"])
            .env("DISPONENT_EXE_DRY_RUN", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn disponent mcp");
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let mut server = Server {
            child,
            stdin,
            stdout,
            next_id: 0,
        };
        let init = server.request("initialize", json!({"protocolVersion": "2025-03-26"}));
        assert_eq!(init["protocolVersion"], "2025-03-26", "echoes the client's");
        assert_eq!(init["serverInfo"]["name"], "disponent");
        server.notify("notifications/initialized");
        server
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let msg = json!({"jsonrpc": "2.0", "id": self.next_id,
            "method": method, "params": params});
        writeln!(self.stdin, "{msg}").unwrap();
        let mut line = String::new();
        self.stdout.read_line(&mut line).unwrap();
        let reply: Value = serde_json::from_str(&line).expect("one JSON-RPC reply per line");
        assert_eq!(reply["id"], json!(self.next_id));
        assert!(
            reply.get("error").is_none(),
            "unexpected protocol error: {reply}"
        );
        reply["result"].clone()
    }

    fn call(&mut self, tool: &str, args: Value) -> (Value, bool) {
        let result = self.request("tools/call", json!({"name": tool, "arguments": args}));
        let is_error = result["isError"] == json!(true);
        let text = result["content"][0]["text"].as_str().unwrap().to_string();
        let body = serde_json::from_str(&text).unwrap_or(Value::String(text));
        (body, is_error)
    }

    fn notify(&mut self, method: &str) {
        let msg = json!({"jsonrpc": "2.0", "method": method});
        writeln!(self.stdin, "{msg}").unwrap();
    }

    fn tool_names(&mut self) -> Vec<String> {
        self.request("tools/list", json!({}))["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn supervisor_walks_the_whole_flow() {
    let mut server = Server::start("supervisor");

    let names = server.tool_names();
    assert_eq!(names.len(), 13, "the full generated surface: {names:?}");
    assert!(names.contains(&"disponent_dispatch".to_string()));

    let (envs, err) = server.call("disponent_environments", json!({}));
    assert!(!err);
    let slugs: Vec<&str> = envs
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["slug"].as_str().unwrap())
        .collect();
    assert_eq!(slugs, ["local", "exe-dev"], "the shipped catalog");

    // the offerings table: env × agent × model, one row flagged default per env
    let (offerings, err) = server.call("disponent_offerings", json!({}));
    assert!(!err);
    let rows = offerings.as_array().unwrap();
    assert!(rows.iter().any(|o| o["envSlug"] == "local"
        && o["agentName"] == "claude-code"
        && o["modelId"] == "claude-opus-4-8"
        && o["isDefault"] == json!(true)));
    assert_eq!(
        rows.iter()
            .filter(|o| o["isDefault"] == json!(true))
            .count(),
        2,
        "one default per environment (local, exe-dev)"
    );

    let (session, err) = server.call(
        "disponent_dispatch",
        json!({"spec": {"brief": "say hi", "env": "exe-dev", "template": "claude-base"}}),
    );
    assert!(!err, "{session}");
    assert_eq!(
        session["state"], "queued",
        "accepted; provisioning is async"
    );
    let uid = session["uid"].as_str().unwrap().to_string();

    // the dry-run provisioner races this read; the accepted log is always first
    let (events, err) = server.call("disponent_events", json!({"options": {"sessionUid": uid}}));
    assert!(!err);
    assert!(!events.as_array().unwrap().is_empty());
    assert_eq!(events[0]["kind"], "log");

    // a tool failure rides in-band: isError, not a protocol error
    let (msg, err) = server.call(
        "disponent_dispatch",
        json!({"spec": {"brief": "x", "env": "nonesuch"}}),
    );
    assert!(err);
    assert!(msg.as_str().unwrap().contains("no environment"));

    let (reaped, err) = server.call("disponent_reap", json!({"sessionUid": uid}));
    assert!(!err);
    assert_eq!(reaped["state"], "cancelled");
    assert!(reaped["reapedAt"].is_string());

    let (report, err) = server.call("disponent_reconcile", json!({}));
    assert!(!err);
    assert_eq!(report["adopted"], 0);
}

#[test]
fn worker_sees_only_the_readonly_surface() {
    let mut server = Server::start("worker");

    let names = server.tool_names();
    assert_eq!(
        names,
        [
            "disponent_environments",
            "disponent_offerings",
            "disponent_session",
            "disponent_sessions",
            "disponent_events",
            "disponent_driver_plan",
        ],
        "observe-only: exactly the readonly tools"
    );

    // calling a hidden tool is a protocol-level rejection, not a silent no-op
    server.next_id += 1;
    let msg = json!({"jsonrpc": "2.0", "id": server.next_id, "method": "tools/call",
        "params": {"name": "disponent_dispatch",
                   "arguments": {"spec": {"brief": "x", "env": "local"}}}});
    writeln!(server.stdin, "{msg}").unwrap();
    let mut line = String::new();
    server.stdout.read_line(&mut line).unwrap();
    let reply: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(reply["error"]["code"], json!(-32602), "{reply}");
}
