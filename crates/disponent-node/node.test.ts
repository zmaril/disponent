// The binding end-to-end over dry-run backends (env set by the test script):
// the whole lifecycle from JS — dispatch, wait, observe, send, cancel, reap —
// plus the two streams and the enum/JSON seams.
import { expect, test } from "bun:test";
import {
  Disponent,
  EnvKind,
  EventKind,
  SessionState,
  version,
} from "./index.js";

test("the addon loads and links the engine", () => {
  expect(version()).toContain("disponent-node");
});

test("the whole lifecycle from JS", async () => {
  const d = new Disponent({ sink: "none" });

  const envs = await d.environments();
  expect(envs.map((e: any) => e.slug)).toEqual(["local", "exe-dev"]);
  expect(envs[0].kind).toBe(EnvKind.Local);

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

  await d.send(session.uid, "how goes it?");

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
  expect(d.dispatch({ brief: "x", env: "local", labels: "not json" })).rejects.toThrow(
    "labels",
  );
  expect(() => new Disponent({ configPath: "/tmp/nope.toml" })).toThrow(
    "configPath",
  );
});
