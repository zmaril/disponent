"""The binding end-to-end over dry-run backends (flags set in conftest): the
whole lifecycle from Python — dispatch, wait, observe, send, cancel, reap —
plus the two streams and the enum/JSON seams. Mirrors the node bun test.

    uv venv && uv pip install --group dev
    .venv/bin/maturin develop
    .venv/bin/python -m pytest tests/
"""
import json

import conftest  # noqa: F401  (imports for its env-var side effects)
import pytest

from disponent import CapabilityKind, Disponent, EnvKind, EventKind, SessionState


def test_whole_lifecycle():
    d = Disponent(sink="none")

    envs = d.environments()
    assert [e.slug for e in envs] == ["local", "exe-dev"]
    assert envs[0].kind == EnvKind.Local

    # per-env capabilities: one row per (env, capability) the catalog advertises
    caps = d.capabilities()
    assert any(
        c.env_slug == "local" and c.capability == CapabilityKind.Dispatch for c in caps
    )
    # exe-dev advertises VM isolation; local does not
    assert any(
        c.env_slug == "exe-dev" and c.capability == CapabilityKind.IsolationVm
        for c in caps
    )
    assert not any(
        c.env_slug == "local" and c.capability == CapabilityKind.IsolationVm
        for c in caps
    )

    session = d.dispatch(
        brief="say hi from python",
        env="local",
        labels=json.dumps({"suite": "python"}),
    )
    assert session.state == SessionState.Queued

    # wait() rides the dry-run provisioner to running; running is not terminal,
    # so wait() holds its whole (1s) timeout and hands back the latest snapshot.
    running = d.wait(session.uid, 1)
    assert running.state == SessionState.Running
    assert json.loads(running.env_handle)["tmux"] == f"dsp-{session.uid}"

    # the event stream pages the timeline; payloads are JSON text
    events = d.events(session_uid=session.uid)
    first = next(events)
    assert first.kind == EventKind.Log
    assert "dispatch accepted" in json.loads(first.payload)["payload"]["line"]
    second = next(events)
    assert second.kind == EventKind.State

    d.send(session.uid, "how goes it?")

    cancelled = d.cancel(session.uid)
    assert cancelled.state == SessionState.Cancelled
    reaped = d.reap(session.uid)
    assert reaped.reaped_at is not None

    # filters cross the enum seam
    done = d.sessions(state=SessionState.Cancelled)
    assert len(done) == 1

    report = d.reconcile()
    assert report.adopted == 0


def test_driver_plan_drains():
    d = Disponent(sink="none")
    d.dispatch(brief="row fodder", env="local")

    stmts = list(d.driver_plan(dialect="sqlite"))
    assert stmts[0].sql.startswith("CREATE TABLE")
    assert any('INSERT INTO "dispatches"' in s.sql for s in stmts)
    for s in stmts:
        assert isinstance(json.loads(s.params), list)


def test_bad_inputs_fail_at_the_seam():
    d = Disponent(sink="none")
    with pytest.raises(Exception) as bad_labels:
        d.dispatch(brief="x", env="local", labels="not json")
    assert "labels" in str(bad_labels.value)

    with pytest.raises(Exception) as bad_ctor:
        Disponent(config_path="/tmp/nope.toml")
    assert "configPath" in str(bad_ctor.value)
