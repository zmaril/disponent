// The binding end-to-end over dry-run backends (env set by the test script):
// the whole lifecycle from JS — dispatch, wait, observe, send, cancel, reap —
// plus the two streams and the enum/JSON seams.
import { expect, test } from "bun:test";
import {
  CapabilityKind,
  Disponent,
  EnvKind,
  EventKind,
  Party,
  SessionState,
  setEnv,
  version,
} from "./index.js";

// Backend dry-run flags must cross into the NATIVE process env — under Bun,
// process.env assignments never reach the Rust engine (setEnv exists for
// exactly this; see its doc).
setEnv("DISPONENT_EXE_DRY_RUN", "1");
setEnv("DISPONENT_LOCAL_DRY_RUN", "1");

test("the addon loads and links the engine", () => {
  expect(version()).toContain("disponent-node");
});

test("the whole lifecycle from JS", async () => {
  const d = new Disponent({ sink: "none" });

  const envs = await d.environments();
  expect(envs.map((e) => e.slug)).toEqual(["local", "exe-dev", "modal"]);
  expect(envs[0].kind).toBe(EnvKind.Local);

  // the offerings table: env × agent × model, is_default flags the pick
  const offerings = await d.offerings();
  expect(offerings.map((o) => [o.envSlug, o.agentName, o.modelId, o.isDefault])).toContainEqual([
    "local",
    "claude-code",
    "claude-opus-4-8",
    true,
  ]);
  expect(offerings.filter((o) => o.isDefault).map((o) => o.envSlug)).toEqual([
    "local",
    "exe-dev",
    "modal",
  ]);

  // per-env capabilities: one row per (env, capability) the catalog advertises
  const caps = await d.capabilities();
  expect(caps.some((c) => c.envSlug === "local" && c.capability === CapabilityKind.Dispatch)).toBe(
    true,
  );
  // exe-dev advertises VM isolation; local does not
  expect(
    caps.some((c) => c.envSlug === "exe-dev" && c.capability === CapabilityKind.IsolationVm),
  ).toBe(true);
  expect(
    caps.some((c) => c.envSlug === "local" && c.capability === CapabilityKind.IsolationVm),
  ).toBe(false);

  const session = await d.dispatch({
    brief: "say hi from node",
    env: "local",
    labels: JSON.stringify({ suite: "node" }),
  });
  expect(session.state).toBe(SessionState.Queued);

  // wait() rides the dry-run provisioner to running; running is not
  // terminal, so wait() holds its whole (1s) timeout and hands back the
  // latest snapshot
  const running = await d.wait(session.uid, 1);
  expect(running.state).toBe(SessionState.Running);
  expect(JSON.parse(running.envHandle!).tmux).toBe(`dsp-${session.uid}`);

  // the event stream pages the timeline; payloads are JSON text
  const events = d.events({ sessionUid: session.uid });
  const first = await events.next();
  expect(first!.kind).toBe(EventKind.Log);
  expect(JSON.parse(first!.payload).payload.line).toContain("dispatch accepted");
  const second = await events.next();
  expect(second!.kind).toBe(EventKind.State);

  // send is the manager↔worker messaging primitive: a `sessions` target mints
  // one Message per recipient and (best-effort) prompts the live worker.
  const minted = await d.send({ sessions: [session.uid] }, "how goes it?");
  expect(minted.length).toBe(1);
  expect(minted[0].sender).toBe(Party.Manager);
  expect(minted[0].recipient).toBe(Party.Worker);

  const inbox = await d.messages({ sessionUid: session.uid });
  expect(inbox.length).toBe(1);
  await d.ack(minted[0].id);
  const acked = await d.messages({ sessionUid: session.uid });
  expect(acked[0].ackedAt).not.toBeNull();

  const cancelled = await d.cancel(session.uid);
  expect(cancelled.state).toBe(SessionState.Cancelled);
  const reaped = await d.reap(session.uid);
  expect(reaped.reapedAt).toBeString();

  // filters cross the enum seam
  const done = await d.sessions({ state: SessionState.Cancelled });
  expect(done.length).toBe(1);

  const report = await d.reconcile();
  expect(report.adopted).toBe(0);
});

test("driverPlan drains to null: DDL first, then rows", async () => {
  const d = new Disponent({ sink: "none" });
  await d.dispatch({ brief: "row fodder", env: "local" });

  const plan = d.driverPlan({ dialect: "sqlite" });
  const statements: string[] = [];
  for (let s = await plan.next(); s !== null; s = await plan.next()) {
    statements.push(s.sql);
    expect(JSON.parse(s.params)).toBeArray();
  }
  expect(statements[0]).toStartWith("CREATE TABLE");
  expect(statements.some((s) => s.includes('INSERT INTO "dispatches"'))).toBe(true);
});

test("bad inputs fail loudly at the seam", async () => {
  const d = new Disponent({ sink: "none" });
  expect(d.dispatch({ brief: "x", env: "local", labels: "not json" })).rejects.toThrow("labels");
  expect(() => new Disponent({ configPath: "/tmp/nope.toml" })).toThrow("configPath");
});
