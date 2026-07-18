#!/usr/bin/env python3
"""disponent's Modal driver — the subprocess bridge the `modal` env-kind shells to.

Mirrors how the exe.dev backend shells to `ssh`: the Rust `Modal` provider writes
this file to a stable temp path and invokes `python3 <path> <verb>`, passing ONE
JSON request object on stdin. The driver imports the Modal SDK, authenticates from
`MODAL_TOKEN_ID` / `MODAL_TOKEN_SECRET` in the environment, performs the verb, and
prints exactly ONE JSON response object on stdout.

Honest capability edges: on ANY exception the driver prints
`{"ok": false, "error": "<message>"}` and exits non-zero, so the Rust side surfaces
a real error (`modal driver: <error>`) rather than a faked success. The driver
never invents a sandbox id or a URL it didn't get from Modal.

The Rust side keeps all policy: it composes the tmux command lines for
spawn/send/capture/interrupt/kill and passes them through the generic `exec` verb.
The driver only needs the lifecycle verbs plus `exec`.

Verbs (argv[1]) and their request/response JSON shapes
======================================================

image_build   Build/reference a Modal image (the TEMPLATE stage).
  request : {
    "name": str,                 # template/image name (informational tag)
    "setup": str | null,         # extra `run_commands` line(s), run after baseline
    "claude_install": str | null # override the claude-CLI install command
  }
  response: { "ok": true, "image": str }   # `image` echoes the name that was built

sandbox_create   Create a sandbox (the START stage / Compute surface).
  request : {
    "app": str,                  # Modal App name (App.lookup, create_if_missing)
    "image": str | null,         # image name/tag to build the sandbox on
    "setup": str | null,         # extra image run_commands
    "claude_install": str | null,
    "timeout": int,              # hard wall-clock timeout (seconds)
    "idle_timeout": int | null,  # scale-to-zero idle timeout (seconds)
    "workspace_port": int | null,# encrypted_port to expose (enables the tunnel)
    "volume": str | null,        # Volume.from_name(create_if_missing) name
    "workdir": str | null,       # mount path for the volume (repo work dir)
    "secret": str | null,        # Secret.from_name to inject (credentials)
    "session_uid": str,          # tagged onto the sandbox for survey()
    "tags": { str: str } | null  # extra tags merged with the worker tag
  }
  response: {
    "ok": true,
    "sandboxId": str,            # persist this; re-attach via Sandbox.from_id
    "workspaceUrl": str | null   # tunnel URL when a workspace_port was exposed
  }

exec   Run one command in an existing sandbox (the generic INTERACT primitive).
  request : {
    "sandboxId": str,
    "argv": [str, ...],          # exact argv (NOT a shell string); e.g. a tmux line
    "stdin": str | null          # optional stdin fed to the process
  }
  response: {
    "ok": true,
    "exitCode": int,             # exact ContainerProcess returncode
    "stdout": str,               # separated stream (Modal keeps them apart)
    "stderr": str
  }

sandbox_terminate   Terminate a sandbox (the REAP stage).
  request : { "sandboxId": str }
  response: { "ok": true }

sandbox_list   List disponent worker sandboxes for reconcile (the SURVEY stage).
  request : {
    "app": str,
    "tag": str | null            # worker tag to filter on (default disponent-worker)
  }
  response: {
    "ok": true,
    "sandboxes": [ { "sandboxId": str, "sessionUid": str | null }, ... ]
  }
"""

import json
import sys

# The tag every disponent worker sandbox carries, so `sandbox_list` finds ours
# without guessing from names. Mirrors backend::WORKER_TAG on the Rust side.
WORKER_TAG = "disponent-worker"
# The default command that installs the claude CLI into the image. Overridable
# per-request so the image mapping can track the CLI's install story.
DEFAULT_CLAUDE_INSTALL = "npm install -g @anthropic-ai/claude-code"


def _fail(message):
    """Print an honest error envelope and exit non-zero (never fake success)."""
    print(json.dumps({"ok": False, "error": str(message)}))
    sys.exit(1)


def _build_image(modal, req):
    """debian_slim + git/tmux + node + the claude CLI + optional dispatch setup.

    Mirrors the exe.dev template's baked baseline: the tools a tmux-in-container
    worker needs, then the agent CLI, then the dispatch's own setup line.
    """
    image = (
        modal.Image.debian_slim()
        .apt_install("git", "tmux", "curl", "ca-certificates", "nodejs", "npm")
        .run_commands(req.get("claude_install") or DEFAULT_CLAUDE_INSTALL)
    )
    setup = req.get("setup")
    if setup:
        image = image.run_commands(setup)
    return image


def image_build(modal, req):
    # Building references/materializes the image; we return the name as the
    # handle. A real build error propagates out as an honest failure.
    _build_image(modal, req)
    return {"ok": True, "image": req["name"]}


def sandbox_create(modal, req):
    app = modal.App.lookup(req["app"], create_if_missing=True)
    image = _build_image(modal, req)

    kwargs = {"app": app, "image": image, "timeout": req["timeout"]}
    if req.get("idle_timeout") is not None:
        kwargs["idle_timeout"] = req["idle_timeout"]

    # Workspace persistence: a named volume mounted at the repo work dir.
    volume = req.get("volume")
    workdir = req.get("workdir")
    if volume and workdir:
        kwargs["volumes"] = {workdir: modal.Volume.from_name(volume, create_if_missing=True)}

    # Credentials never enter the schema — they ride as a Modal Secret.
    secret = req.get("secret")
    if secret:
        kwargs["secrets"] = [modal.Secret.from_name(secret)]

    # A workspace port exposes an HTTPS tunnel (encrypted_ports) → workspace_link.
    port = req.get("workspace_port")
    if port:
        kwargs["encrypted_ports"] = [port]

    sb = modal.Sandbox.create(**kwargs)

    # Tag for reconcile/adoption: the worker tag + the session uid + any extras.
    tags = {WORKER_TAG: "1", "disponent-session": req["session_uid"]}
    tags.update(req.get("tags") or {})
    try:
        sb.set_tags(tags)
    except Exception:
        # An untagged sandbox still runs; tagging is best-effort like exe.dev's.
        pass

    workspace_url = None
    if port:
        # The tunnel's HTTPS URL onto the exposed workspace port.
        workspace_url = sb.tunnels()[port].url

    return {"ok": True, "sandboxId": sb.object_id, "workspaceUrl": workspace_url}


def exec_(modal, req):
    sb = modal.Sandbox.from_id(req["sandboxId"])
    proc = sb.exec(*req["argv"])
    stdin = req.get("stdin")
    if stdin is not None:
        proc.stdin.write(stdin)
        proc.stdin.write_eof()
    # Modal keeps the streams separate and hands back an exact exit code — the
    # refinement a future holder binary would build on.
    stdout = proc.stdout.read()
    stderr = proc.stderr.read()
    proc.wait()
    return {
        "ok": True,
        "exitCode": proc.returncode,
        "stdout": stdout,
        "stderr": stderr,
    }


def sandbox_terminate(modal, req):
    sb = modal.Sandbox.from_id(req["sandboxId"])
    sb.terminate()
    return {"ok": True}


def sandbox_list(modal, req):
    app = modal.App.lookup(req["app"], create_if_missing=True)
    tag = req.get("tag") or WORKER_TAG
    out = []
    for sb in modal.Sandbox.list(app_id=app.app_id, tags={tag: "1"}):
        try:
            tags = sb.get_tags()
        except Exception:
            tags = {}
        out.append(
            {"sandboxId": sb.object_id, "sessionUid": tags.get("disponent-session")}
        )
    return {"ok": True, "sandboxes": out}


VERBS = {
    "image_build": image_build,
    "sandbox_create": sandbox_create,
    "exec": exec_,
    "sandbox_terminate": sandbox_terminate,
    "sandbox_list": sandbox_list,
}


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in VERBS:
        _fail("usage: modal_driver.py <%s>" % "|".join(VERBS))
    try:
        req = json.load(sys.stdin)
    except Exception as err:  # noqa: BLE001 — surface any parse failure honestly
        _fail("bad request json: %s" % err)

    try:
        import modal
    except Exception as err:  # noqa: BLE001 — missing SDK is an honest failure
        _fail("modal SDK not importable (pip install modal): %s" % err)

    try:
        result = VERBS[sys.argv[1]](modal, req)
    except Exception as err:  # noqa: BLE001 — any SDK/network error is honest
        _fail(err)

    print(json.dumps(result))


if __name__ == "__main__":
    main()
