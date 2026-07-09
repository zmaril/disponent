# disponent — owning the terminal session

*Design note, 2026-07-09. Should disponent hold its agents' terminal sessions
itself — persistence, attach, send — instead of leaning on tmux (local) and
ttyd (remote), or should it adopt [shpool](https://github.com/shell-pool/shpool)?
This doc answers concretely: what runs where, in what language, and how hard it
actually is. No implementation — a decision and a migration path. Grounded in
today's code (HEAD `a932bc5`) and a read of shpool's source (HEAD `901e18c`).*

## 1. The question, and the honest short answer

disponent dispatches coding agents and monitors them. Today a "terminal session"
is not disponent's — it is a **tmux** session locally and an **exe.dev VM + ttyd**
remotely. disponent shells out to those holders: `tmux send-keys` to talk,
`tmux capture-pane` to look, and *nothing at all* to attach — the live byte-stream
is somebody else's job (powdermonkey spawns `tmux attach`; a human opens the ttyd
URL). The user wants tmux gone.

The short answer this doc argues: **build a small first-party headless holder
(`dsp-hold`), reuse shpool's two hardest crates (`shpool_pty`, `shpool_vt100`)
rather than reinvent them, and do *not* adopt shpool the daemon nor the
shpool-plus-attachment hybrid.** The win disponent actually wants — terminal
frames as `exact` events, real exit codes, ledger-native discovery, and *many
observers plus one writer* — is **separable from, and cheaper than**, the
human-reattach polish that is shpool's real leverage. Own the cheap high-value
half; borrow shpool's crates for the expensive half when you reach it.

The uncomfortable part, stated up front so the rest of the doc can be honest: a
holder is a resident process, and **a dead holder is a dead pty** — the same
failure tmux, ttyd, and shpool all have. "Own it" does not buy crash-persistence,
because nobody has it. It buys fidelity and a unified channel.

## 2. What holds a session today — and the "no daemon" stance

There is **no disponent daemon**. The engine is in-process: `disponent mcp` builds
one `Engine::open(sink)` and runs a blocking stdio loop (`crates/disponent-core/src/mcp_server.rs:17`);
the napi/PyO3/Magnus bindings hold an `Arc<DisponentImpl>` for the host process
(powdermonkey embeds it in-process — no second process, one shared engine,
`powdermonkey/src/server/disponent.ts:32`). The engine's threads die with it
(`Drop for Engine`, `engine.rs:258`). design.md §11 states the rule outright:
disponent-core stays synchronous, concurrency lives in an observer pool, and the
environment — not disponent — is the durable holder.

So **what holds a session between engine invocations is the tmux server itself**
(local) and **the exe.dev VM + its own tmux + ttyd** (remote), re-discovered by
`tmux ls` / `exe.dev ls`. The ledger is a reconciled cache over them (design.md §3).

| | local (`LocalTmux`) | exe.dev (`ExeDev`) |
|---|---|---|
| holder | `tmux -L disponent` server, session `dsp-<uid>` (`local.rs:223`) | VM + remote `tmux -L disponent` session `worker`, wrapped by `ttyd` (`backend.rs:449`) |
| send | `tmux send-keys -l <input>` + `Enter` (`local.rs:249`) | same over `ssh <vm> tmux …` (`backend.rs:363`) |
| observe | `tmux capture-pane -p` (`local.rs:88`) | `ssh <vm> tmux capture-pane -p` (`backend.rs:369`) |
| attach | *not disponent's* — pm spawns `tmux attach` | *not disponent's* — human opens `https://<host>:7681` (ttyd, `Session.url`) |
| discover | `tmux list-sessions` (`local.rs:304`) | VM tags `disponent-session-<uid>` via `exe.dev ls` (`backend.rs:124`) |
| exit status | **none** — `exec bash` keeps the pane alive; death only inferred at reconcile → `lost` (`engine.rs:1028`) | same |

Two things fall out of this table and drive the rest of the doc:

- **No binary disponent controls runs on the exe.dev VM.** The remote backend
  drives stock `git`/`gh`/`bash`/`tmux`/`ttyd` over ssh. Owning the terminal
  remotely means, for the first time, *shipping a disponent artifact to the VM*.
- **disponent already owns send and a scraped snapshot, but not the live stream
  and not exit status.** The `-x 220 -y 50` fixed geometry (`local.rs:229`) is a
  tell: disponent is renting a terminal it can't resize per-attacher.

## 3. The seam — `EnvBackend` is poll-only, attach lives outside

Both backends implement one trait (`crates/disponent-core/src/backend.rs:49`):

```rust
trait EnvBackend: Send + Sync {
    fn provision(&self, req: &ProvisionRequest) -> Result<Provision>;
    fn stop(&self, handle: &Value) -> Result<()>;          // cancel's half
    fn send(&self, handle: &Value, input: &str) -> Result<()>;
    fn teardown(&self, handle: &Value) -> Result<()>;      // reap's half
    fn survey(&self) -> Result<Vec<(String, Value)>>;      // reconcile discovery
    fn capture(&self, handle: &Value) -> Result<String>;   // the ONLY observation
    fn workspace_link(&self, _: &Value) -> Result<Option<String>> { Ok(None) }
}
```

The only observation method is `capture` — a whole-pane pull. The engine wraps it
in `watch_session` on a 5 s poll (`engine.rs:267`) and turns each pane snapshot
into an event by diffing suffixes (`observe.rs`, see §4). **There is no streaming,
no read-since-offset, no exit code, and no attach.** design.md's own capability
matrix *aspires* to `stream (PTY pipe)` locally, but the trait is poll-only for
both backends. A first-party holder is precisely the thing that lets this trait
grow a real streaming edge.

There is no `attach` op in the schema either (`schema/disponent.tsp:409`): the op
surface is `dispatch/send/cancel/resume/reap/wait/events/sessions/workspaceLink`.
The paused disponent#32 work added typed *pointers* to the tmux session
(`attachTmuxSocket`, `attachTmuxSession` on `Session`) so pm could reach the pane
without parsing the opaque `envHandle` blob — but that is a pointer to an external
holder, not an attach channel disponent owns. §10 says what becomes of it.

## 4. What "owning it" actually buys

Four wins, each tied to a concrete gap above:

1. **Terminal frames become `exact`, not `scraped`.** Today the *only* `scraped`
   producer is `observe.rs`: it diffs `capture-pane` snapshots with a
   largest-suffix heuristic (`terminal_delta`) and stamps every frame
   `kind:"raw", fidelity:"scraped"`. That heuristic loses data on in-place
   redraws (it emits the whole pane) and is blind between polls. A holder reads
   the pty **byte-exact**, so the same `raw` events carry `fidelity:"exact"` —
   which is the entire point of the fidelity taxonomy (design.md §7). The screen
   emulator (§7) is *not* needed for this: exact frames are just the raw byte
   stream; the emulator is only for pretty human reattach. **The two wins are
   separable, and the valuable one is the cheap one.**
2. **Real exit statuses.** Nothing self-transitions a session to `completed`
   today — `exec bash` keeps the pane alive and `wait()` effectively resolves only
   on timeout/cancel/reconcile-`lost` (`disponent-node/src/lib.rs:63`). A holder
   `waitpid`s the child and knows the code. This closes the `wait` gap and the
   `lost`-vs-`completed` ambiguity — arguably a bigger honesty win than fidelity.
3. **One dependency fewer, and no rented-terminal hacks.** tmux gone locally; the
   `-x 220 -y 50` fixed geometry gone; disponent stops being hostage to a tmux
   version/config on every box.
4. **Ledger-native discovery and one unified channel.** `sessions` stops being a
   `tmux ls` scrape; attach, send, and observe collapse from three disjoint
   mechanisms (send-keys, capture-pane, external ttyd/tmux-attach) into one holder
   protocol that the engine, pm, and a human on the box all speak.

## 5. Topology — which processes run where

The design keeps disponent's "no central daemon" property by making the holder
**per session**, not a singleton engine daemon. This is self-similar to tmux (the
tmux *server* is the resident holder; the disponent engine is transient) and to
disponent's own self-similar MVP (design.md §14).

**Component: `dsp-hold`.** A small **Rust** program — shipped as a subcommand of
the one `disponent` binary (`disponent hold …`), not a separate artifact, so there
is exactly one static build to deliver to a VM. It links a new `disponent-hold`
module that shares nothing heavy with the engine except types; the pty and
screen-restore logic come from shpool's crates (§7, §9). Its job:

```
disponent hold <uid> -- <agent argv…>
  ├─ openpty (shpool_pty), fork+exec the agent as session leader under the pty
  ├─ listen on a unix socket:  $XDG_RUNTIME_DIR/disponent/<uid>.sock  (local)
  │                            $HOME/.disponent/<uid>.sock            (remote VM)
  ├─ drain the pty forever into a bounded raw ring (+ optional vt100 spool)
  ├─ accept attach connections: N readers + at most 1 writer (§6)
  └─ waitpid the child → record exit status, emit an Exit frame, linger until reaped
```

Lifetime: `dsp-hold` **double-forks / setsid** at launch so it reparents to init
and outlives the engine, pm's `bun --watch` restarts, and the ssh session that
spawned it — exactly the property tmux gives today. Restart story, stated plainly:
**if `dsp-hold` dies, its pty and child die with it** (SIGHUP), scrollback is lost,
and reconcile marks the session `lost` — identical to a tmux-server death or an
shpool-daemon death (§7). The holder buys fidelity, not immortality.

**Local deployment (operator laptop / powdermonkey).**

```
 pm (Bun) ── napi ──▶ disponent engine (in-process, transient)
   │                        │  survey() = scan $XDG_RUNTIME_DIR/disponent/*.sock
   │                        │  send/observe/attach = dial <uid>.sock
   │                        ▼
   │                  dsp-hold <uid>  (resident, reparented to init)
   │                        └─ pty ─▶ claude (the agent)
   └── /pty WebSocket ──────────────▶ dials <uid>.sock directly (§6)
 human on the box:  disponent attach <uid>  ─▶ dials <uid>.sock
```

No long-lived disponent daemon appears. The engine remains transient and
in-process; the *holders* are the resident processes, discovered by scanning the
socket directory the way `survey` scans `tmux ls` today.

**Remote deployment (exe.dev VM).**

```
 laptop Claude Code ── ssh stdio ──▶ disponent mcp (on the VM, in a holder too)
                                          │  dispatches sibling VMs
                                          ▼
 each worker VM:  disponent hold <uid> -- claude …   (replaces tmux new-session)
                     ├─ pty ─▶ claude
                     ├─ $HOME/.disponent/<uid>.sock
                     └─ ttyd -W disponent attach <uid>   ── browser humans, VM:7681
 engine attach/observe:  ssh <vm> disponent hold-attach <uid>   (stdio ⇄ socket)
```

The VM binary is the **same static `disponent` build** (musl), delivered at
provision time by `exe.dev cp` (the control plane already copies files — the
bootstrap script is scp'd today). The bootstrap launches `disponent hold …`
instead of `tmux new-session … ttyd`. **Attach traverses ssh with no
port-forward**: `ssh <vm> disponent hold-attach <uid>` connects the holder's
socket to the ssh stdio pipe — self-similar to how the supervisor already reaches
the VM as `ssh <vm> disponent mcp` (design.md §14: "ssh *is* the remote
transport"). ttyd stays as the zero-client browser path for a human, now pointed
at the holder instead of tmux; it can retire later if `dsp-hold` grows its own web
mode.

**Why Rust, why one binary.** disponent-core is sync Rust (the entl rule);
`dsp-hold` sharing that toolchain means one static artifact to ship to a VM and
zero new runtime deps there. shpool proves the model is a few thousand lines of
Rust over `nix`/`libc`. Anything other than Rust here would mean a second artifact
on the VM and a second language in a repo whose whole schema story is
Rust-generated — not justified.

## 6. Protocol — the attach stream

disponent controls both ends of every attach (engine, pm's server, and its own
CLI), which lets the protocol be **simpler than shpool's** — shpool contorts around
an unframed input stream and a human's SIGWINCH; disponent can frame both
directions because it wrote both clients.

**Framing.** One unix stream (or ssh stdio pipe). A msgpack (or length-prefixed
JSON, to match disponent's stdio-JSON idiom) **control channel** carries the
handshake, resize, and writer-lock requests; **pty bytes ride chunk frames** —
`1 byte kind | 4-byte LE len | payload`, kinds `Data / Exit / Heartbeat`, 16 KiB
cap (shpool's shape, `shpool-protocol/src/lib.rs:381`, worth copying wholesale).

**Attachers: N readers + 1 writer.** This is the deliberate divergence from
shpool, which allows exactly one attacher and returns `Busy` on contention
(`server.rs:506`) — the opposite of an observe-only supervising agent. In
disponent:
- The **engine keeps one resident reader** open per session — this *is* the
  observer that produces the `exact` `raw` frame events. "Resident attachment"
  means the observer never lets go; frames flow into the ledger continuously.
- A **human `disponent attach <uid>`** is a reader by default; `--write` (or
  `--take`) requests the single writer lock, which `send` also takes momentarily.
- `send(uid, input)` opens a writer connection, writes the bytes (+ `Enter`),
  releases — the same channel, not a separate `send-keys` path.

**Resize.** A `{resize:{cols,rows}}` control message; the holder `TIOCSWINSZ`-es
the master. Because disponent frames input, it avoids shpool's
socket-per-SIGWINCH hack (`attach.rs:421`) entirely — a real simplification it
earns by owning the client.

**Replay / scrollback.** On attach the holder first writes a restore buffer.
Two tiers, matching the two consumers: a **raw ring** replay for the exact-event
observer (byte-exact, trivial), and — only for a human who asks — a **vt100
screen repaint** via `shpool_vt100::contents_formatted()` so vim/the agent TUI
redraw cleanly instead of vomiting escapes (§7). MVP ships the raw ring and
defers the emulator.

**pm's `/pty` bridge is nearly free.** pm's browser contract is *already*
transport-agnostic: binary pty output out, JSON `{type:"input"}` / `{type:"resize"}`
in, a `session-ended` control frame (`powdermonkey/src/server/app.ts:702`,
`ShellTerminal.tsx:130`). Only pm's *server* attach implementation is
tmux-specific — it spawns a Bun pty running `tmux attach` (`session-pty.ts:145`).
Owning the holder swaps that one implementation: the `/pty` server dials the
holder socket (local) or `ssh <vm> disponent hold-attach` (remote) and shuttles
Data chunks → `ws.send`, `{input}` → writer bytes, `{resize}` → resize control.
**The browser code does not change.** This is exactly what pm#162 already
reshapes (`resolveLocalAttachTarget`, `AttachTarget`); §10 threads it through.

**Prefer first-class agent channels over pty injection.** Pty `send` is the
lowest-fidelity way to talk to an agent — it fakes keystrokes. Where the agent
exposes a real control surface (Claude Code's MCP, a stdin JSONL protocol, a
cloud session API), `send` should ride *that*, and the pty writer becomes the
fallback for agents that only have a terminal. disponent already grades the
inbound direction this way: Claude Code's OTel telemetry is the intended `exact`
tier (`engine.rs:466`), and pty frames are the fallback for agents without it.
The holder's writer lock is the terminal-shaped floor, not the ceiling.

## 7. What shpool gives free — read from its code

shpool is one long-lived **daemon** holding **one pty per session** in an
in-memory `HashMap` (`server.rs:75`), a thread per connection, and an always-on
shell→client thread that drains the pty into a live terminal emulator even while
detached (`shell.rs:260`). It is **~4.2k LOC of its own core** over two off-repo
crates that hold the hardest logic (`shpool_pty`, `shpool_vt100`). Crucially it is
**not crash-persistent**: daemon death loses every session (§1's honesty applies
to shpool too).

The genuinely hard edge cases shpool has already solved — the reason not to write
a holder from a blank page:

1. **Screen restore via a real VT100 emulator, not a byte ring**
   (`session_restore.rs:86`, `shpool_vt100::contents_formatted()`) — a raw ring
   replays garbage for full-screen apps. This is the single biggest hidden cost,
   and it lives in an *external MIT/Apache crate you can just depend on.*
2. **Prompt/startup sentinel handshake** to discard shell-init noise before
   recording (`shell_inject.rs:144`, `shell.rs:573`) — two sentinels printed by
   re-running the daemon with a magic env var, not `echo`ed.
3. **The emacs resize "jiggle"** — resize one cell too big, wait ~50 ms, resize to
   real (`shell.rs:326`). Pure empirical terminal lore.
4. **Termios raw-mode restore on every exit path** via an RAII guard dropped even
   in the stuck-IO `process::exit` fallback (`tty.rs:127`, `protocol.rs:450`) —
   forget it and the human's terminal wedges.
5. **Dead-client detection with no clean EOF** — periodic heartbeats +
   BrokenPipe-as-hangup (`shell.rs:413`).
6. **Exit-status propagation** via a `waitpid` watcher → `ExitStatus` frame
   (`server.rs:1042`).
7. SSH_AUTH_SOCK re-linking across reattach, cross-UID peer checks, keybinding
   detach interception (`server.rs:573`, `:1337`, `shell.rs:862`).

**But most of that list is human-interactive.** Items 2–4 and 7 exist because
shpool holds a *login shell for a person*. disponent holds a *headless agent*: it
does not need prompt-prefix injection, the emacs jiggle, or SSH_AUTH_SOCK
re-linking for the *observer* path — only a human `disponent attach` wants termios
restore (4) and screen repaint (1). The headless core disponent actually needs —
accept loop, one pty, byte shuttle, exit status, resize — is **~1.5–2k of
shpool's 4.2k LOC**, and the expensive emulator behind it (1) is a crate, not code
to write.

What shpool does **not** give, at any price: crash-persistence, multiple
concurrent viewers, and any control plane beyond human-CLI-shaped msgpack ops. Its
single-attacher `Busy` model is baked into a coarse `inner` mutex
(`server.rs:427`), not a config flag — and it is the exact model disponent's
observe-only worker role must *not* have.

### The restore emulator — which crate, and where ghostty fits

The screen-restore engine (item 1) is the one piece worth *buying* rather than
writing, so the choice of crate matters. Four candidates, all for the same job —
parse the pty byte stream into a screen model and dump the current screen back
out as escape sequences on reattach:

| Crate | Lang | Screen model | Repaint (dump→escapes) | Build cost in a Rust musl static binary |
|---|---|---|---|---|
| **`shpool_vt100`** | Rust | yes | **yes** — `contents_formatted()` | one `Cargo.toml` line; cross-compiles with everything else |
| `alacritty_terminal` | Rust | yes (grid + damage) | no built-in serializer (render-oriented) | cargo-native, but you'd write the dump-to-escapes yourself |
| `vte` | Rust | no (parser only) | no | cargo-native, but only the primitive under the others |
| **`libghostty-vt`** | **Zig** | yes | **yes, richer** — a `Formatter` with a `VT` mode that restores cursor, SGR, OSC-8 links, palette, modes, scrolling region, tabstops, pwd | Zig 0.15.x toolchain + C-FFI + a *second* static lib to cross-compile to musl |

**ghostty is the higher-fidelity engine** — SIMD parser, broad Unicode coverage, fuzz/Valgrind-tested, and its formatter's `VT` mode emits a *more* complete restore stream than `vt100`'s (it replays modes, palette, and the scrolling region, which vt100 doesn't). `libghostty-vt` is real today — MIT, a committed C ABI (`include/ghostty/vt.h` + a `Formatter` API) and a `libghostty-vt.a` static-lib target, with Rust bindings already on crates.io (`libghostty-vt` / `libghostty-vt-sys`). It does exactly the parse→state→repaint round-trip this holder needs; the formatter is a superset of `contents_formatted()`. Caveat: the C ABI is still pre-1.0 ("breaking changes expected").

The cost is entirely on disponent's build axis — and that is the axis §5's
one-static-binary-to-the-VM story turns on. `libghostty-vt-sys` shells out to
`zig build`, so adopting it drags a **Zig 0.15.x toolchain onto every build
host**, an **FFI seam**, a **second static lib to cross-compile to musl** (Zig is
good at musl, but threading the Cargo target triple through to `zig build
--target …musl` is unproven here), and a **pre-1.0 C ABI pinned to a Ghostty
commit** to track. That is a lot to carry for the *deferrable, human-only* half
of the design (M3, §9) — the exact-frame and exit-status wins (§4) need no
emulator at all.

**Call:** start on `shpool_vt100` — pure Rust, MIT, free musl cross-compile, and
the exact mechanism shpool uses for reattach-repaint. Treat the emulator as a
**swappable engine behind the restore-buffer seam**, not a foundation: because
the seam is just "feed bytes, ask for a repaint," `libghostty-vt` can slot in
later — worth it if repaint fidelity ever becomes limiting (full-screen agent
TUIs leaning hard on palette/modes) or once it ships a stable tag — without
touching the rest of the holder.

## 8. The hybrid — shpool for persistence, disponent for attach/send

A real contender on paper: let shpool hold the pty, let disponent do
attach/send/observe on top. It fails on shpool's own architecture:

- **The single `inner` mutex.** disponent's resident observer connection and a
  human attacher would both need shpool's one attach slot; the second gets `Busy`
  (`server.rs:506`). The multi-observer model — the whole reason to own this — is
  precisely what shpool's core forbids.
- **No second consumer of the spool.** shpool exposes neither its raw bytes nor
  its vt100 spool to a *second* reader over its protocol. To observe through
  shpool you'd attach and scrape its rendered output — no better than
  `capture-pane`, and now through an extra hop.
- **Still an extra dependency on every VM.** tmux is ubiquitous; shpool is not.
  The hybrid trades a universal dep for a niche one *and* keeps a holder you don't
  control.

Verdict: the hybrid inherits shpool's human-single-attacher model, the one thing
most in conflict with disponent's design. **The good half of "hybrid" is reusing
shpool's *crates* (`shpool_pty`, `shpool_vt100`) under a first-party holder** —
which §9 does. The daemon itself is the wrong seam.

## 9. Effort — milestones, hard parts, the MVP cut

Sizes are relative complexity, not calendar. Each milestone is independently
shippable behind a flag.

| Milestone | Scope | Size | Unlocks |
|---|---|---|---|
| **M0 — skeleton holder** | `disponent hold` subcommand: `shpool_pty` openpty + exec, unix socket, 1 reader byte-shuttle, `waitpid` → Exit frame. Local only. | S–M | exact-frame stream + real exit code, behind a flag |
| **M1 — engine integration** | Widen `EnvBackend` with a streaming `observe`/`attach` (byte channel + exit); `LocalTmux` gains a `dsp-hold` sibling; observer records `raw` frames as `exact`; `wait()` resolves on real completion; `send` → writer connection. | M | fidelity + exit-status wins land in the ledger |
| **M2 — writer lock + multi-reader + pm** | N readers, 1 writer lock; `disponent attach` CLI (termios RAII from shpool's pattern); pm `/pty` dials the socket (reworks pm#162). | M | humans + pm on the holder; tmux no longer on the local attach path |
| **M3 — human screen restore** | Pull in `shpool_vt100` (swappable for `libghostty-vt` — see §7); `--restore` repaint + resize jiggle for `disponent attach` and pm. | M (mostly integration) | clean full-screen-app reattach |
| **M4 — remote** | Ship the static `disponent` build to the VM at provision; bootstrap launches `disponent hold` instead of tmux; attach over `ssh <vm> disponent hold-attach`; ttyd points at the holder. | M–L | tmux gone remotely too |

**The genuinely hard parts, called out:**
- **Screen restore (M3)** — don't write it, depend on `shpool_vt100`. The width-1024
  memory footgun and the reattach race shpool flags (`shell.rs:520`) come along;
  budget for them.
- **Remote delivery + version skew (M4)** — the VM binary and the engine must agree
  on the protocol; a `VersionHeader` handshake (shpool's, `protocol.rs:208`) is
  cheap insurance. Delivering a static musl build over `exe.dev cp` is new surface
  disponent has never had (nothing disponent-controlled runs on the VM today).
- **Writer-lock contention (M2)** — define what happens when `send` wants the writer
  while a human holds it: queue, preempt, or reject. Pick reject-with-reason
  (honest-edges rule).
- **Daemon-restart recovery — there is none, by design.** Dead holder = dead pty;
  reconcile marks `lost`. Document it; don't pretend otherwise (this matches
  today's tmux/ttyd and shpool exactly).

**MVP cut: M0 + M1, local only, raw-ring replay, single reader (the engine
observer), tmux still the default backend.** Gate the holder behind
`DISPONENT_LOCAL_HOLDER=dsp-hold`. This ships the two highest-value wins — exact
frames and real exit codes — at the lowest-risk surface, with no remote delivery,
no human-restore emulator, and tmux still there as the fallback. Everything past
M1 is polish and reach, each flag-gated.

## 10. Recommendation and migration path

**Recommendation.** Build `dsp-hold` as a `disponent` subcommand, reusing
`shpool_pty` + `shpool_vt100`. Do **not** adopt shpool the daemon (single-attacher
human model fights the observe-only worker role and is still not
crash-persistent). Do **not** do the shpool+attachment hybrid (§8 — inherits that
same model through an extra hop and an extra VM dep). The value disponent wants is
separable from and cheaper than shpool's human-reattach leverage; own the cheap
half, borrow the crates for the expensive half.

**Migration from today's tmux backend — additive, flag-gated, reversible:**

1. Land M0–M1 behind `DISPONENT_LOCAL_HOLDER`. Both `tmux` and `dsp-hold`
   implement the widened `EnvBackend`, so the engine is holder-agnostic — the
   choice is one env var, and tmux stays the default until M2 proves out.
2. **Reshape disponent#32 / pm#162 to a transport-neutral attach descriptor.** The
   paused work exposes tmux-named scalars (`attachTmuxSocket`,
   `attachTmuxSession`) — do not ship those as the public wire shape; they bake
   tmux into the ledger. Replace with a neutral descriptor on `Session`:

   ```
   attach: { transport: "tmux" | "dsp-hold" | "ttyd",
             endpoint?: string,   // socket path, or ssh target
             target?:   string,   // tmux session name, or <uid>
             url?:      url }      // ttyd/web fallback
   ```

   pm switches on `attach.transport` to decide how to dial: `tmux` preserves
   today's `tmux attach`, `dsp-hold` dials the holder socket, `ttyd`/web falls to
   `url`. pm#162's `AttachTarget` generalizes from `{socket, tmuxSession}` to this
   shape; `resolveLocalAttachTarget` reads the neutral descriptor. **Caveat from
   #32:** the reason #32 used two flat scalars was a fluessig Magnus/Ruby emitter
   bug on nested returned structs (a `Option<TmuxTarget>` getter fails to compile,
   E0599). If that bug still bites, the neutral descriptor lands as flat scalars
   (`attachTransport`, `attachEndpoint`, `attachTarget`, plus the existing `url`)
   rather than a nested object — same information, emitter-safe. Either way the
   field names stop saying "tmux."

3. M2 flips pm's local attach to the holder; tmux drops off the local *attach*
   path while still available as a dispatch backend.
4. M4 ships the holder to exe.dev; ttyd re-points at it; the remote tmux session
   goes away. Once local and remote both run on `dsp-hold`, flip the default and
   retire the tmux backend (or keep it as a `custom`-style escape hatch).

At no point is there a flag day: the neutral descriptor and the holder-agnostic
trait mean tmux and `dsp-hold` coexist, and every step is one env var away from
rolling back.

## 11. Open questions

1. **Per-session vs per-host holder.** Per-session (above) is simplest and
   self-similar to process-per-agent; per-host saves file descriptors but
   reintroduces a singleton daemon — the thing §2 works to avoid. Start
   per-session.
2. **Raw-ring size.** shpool defaults to 500 lines of *rendered* spool; a byte
   ring needs a byte cap. Pick a default (say 256 KiB, matching pm's scrollback
   ring) and make it configurable.
3. **Does structured agent telemetry make pty frames secondary?** Claude Code's
   OTel is already the `exact` tier (`engine.rs:466`); the holder's frames are the
   fallback for agents without it. If most dispatched agents speak MCP/telemetry,
   the holder's observe path is a safety net, not the primary sensor — which
   argues for shipping M0–M1 (exit status + a byte stream) and *not* rushing M3.
4. **Subcommand vs separate binary.** `disponent hold` keeps one artifact to ship
   to a VM (recommended) but bloats the main binary slightly; a separate
   `dsp-hold` bin is leaner per-process but two things to deliver. One binary
   wins on the remote-delivery cost.
