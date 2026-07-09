# disponent — manager↔worker communication

*Draft 3, 2026-07-08. A design note, sibling to [design.md](./design.md) and
[ai-dispatch-comparison.md](./ai-dispatch-comparison.md). It proposes two flows —
a targeted directive channel down to workers, and a question channel up from
workers — collapsed onto a **single symmetric message primitive**. Nothing here
changes `schema/disponent.tsp` or any generated file yet; it is the argument for
the schema change, written first. Draft 1 modelled the flows with a zoo of ops
(`notify`/`ask`/`answer`/`escalate`) and a `Question` entity; draft 2 replaced
all of that with one `Message` entity and one `send`. Draft 3 folds in the
user's decisions: **tags** as the primary selection handle, a **fan-out id with
topic-scoped latest-wins** so a thousand workers don't burn usage on superseded
directives, **`ack` restored** alongside the read cursor, **pull-only** delivery
(push is a non-goal), and a **relaxed** security framing (the real guard is
no-dispatch, not anti-spoofing).*

Read [design.md](./design.md) for the model this builds on: environments are the
source of truth (§3), sessions run until reaped (§5), events carry a `fidelity`
grade (§7), and the MCP surface is **role-scoped** — the Manager gets the full
surface, workers observe only, so a dispatched agent cannot recurse (§10). (This
note uses **Manager** for the supervising principal design §10 calls the
"supervisor"; workers are unchanged.) The one invariant to protect is
no-recursion: a worker must never dispatch or spawn another agent. §9 shows why
that survives letting workers talk back.

## 1. Motivation — the two flows

Two things the MVP topology (design §14: a Manager Claude reaching a fleet of
worker VMs over stdio MCP) cannot express today, both about *communication
between the Manager and running workers*:

**(a) Targeted preference fan-out (down).** The user tells the Manager a
preference mid-flight — "use `bun`, not `npm`, everywhere on projectA" — and the
Manager wants to hand that to a **selected subset** of the running workers (the
ones on projectA), not broadcast it to the whole fleet and not restart anyone.
Each selected worker picks the preference up **through its own MCP surface**, on
an inbox it pulls on its own cadence. Today the only downward channel is
`send(sessionUid, input)` — one session, a Manager-only write that shoves raw
text at a prompt. There is no "these five hundred sessions," no durable record
the worker pulls when it chooses, and nothing a worker is allowed to read as a
first-class message.

**(b) Question escalation (up).** A worker hits an ambiguity it cannot resolve —
"the migration will drop a column with data in it; proceed?" — and needs to
surface the question **upward**. The Manager then decides, per question, whether
to **answer it itself** (it knows the convention) or **forward it to the human**
(only the user can authorize dropping data). This is a *may-or-may-not*
escalation: most questions die at the Manager, some reach the user. Today a
worker has no upward channel at all — the worker-role server exposes `session`,
`sessions`, `events`, `offerings` and nothing else, all read-only. A worker
cannot say anything; it can only be watched.

Both flows are the same act — **one principal putting a message in another's
inbox** — seen from two directions. Draft 1 missed that and grew an op per
direction and per verb. This design names the act once.

## 2. What exists today (grounding)

Quoting the shipped surface so the addition is an honest delta, not an invention.

- **Ops** (`schema/disponent.tsp`, `interface Disponent`): `dispatch`,
  `session` `@readonly`, `sessions` `@readonly`, `events` `@readonly @stream`,
  `send(sessionUid, input)` (a plain write — neither `@readonly` nor
  `@destructive`), `cancel` `@destructive`, `resume`, `reap` `@destructive`,
  `reconcile`, `driverPlan`, and the two `@manual` ops `wait(sessionUid,
  timeoutSecs)` and `serveMcp(options)`.
- **The event feed.** `events(options?: EventOptions)` is a cursor stream;
  `EventOptions { sessionUid?, afterIdx?, kinds? }`. Each `Event` is
  `{ session, idx, ts, kind: EventKind, fidelity: Fidelity, payload:
  EventPayload }`. `EventKind ∈ {state, message, tool_call, tool_result, log,
  usage, artifact, raw}`, `Fidelity ∈ {exact, derived, scraped}`, and
  `EventPayload` is the tagged union. **`afterIdx` is already a read cursor** —
  §7 leans on it for delivery, so the channel needs no new read op (`ack` is a
  small separate write, §7).
- **`send` today.** `send(sessionUid: SessionUid, input: string): void` — a
  Manager-only write that injects text into one running session's prompt (the
  `interact` capability). This design **generalizes that op** into the symmetric
  message primitive (§3, §6); it keeps the name, gains addressing, and delivers
  to an inbox instead of shoving at a prompt.
- **Labels today.** `Dispatch.labels: Json` is an opaque per-dispatch tag bag
  (`DispatchSpec.labels` feeds it). There is **no** first-class tag field and no
  session-level label; §4 adds `tags` as the flat, indexable selection handle.
- **The role gate — the load-bearing detail.** Worker observe-only is enforced
  in exactly one place, `crates/disponent-cli/src/mcp_server.rs::tools_for`:

  ```rust
  // The generated manifest, gated by role: a worker sees only the tools whose
  // manifest entry carries readOnlyHint (observe, never act).
  .filter(|t| role == Role::Supervisor
              || t["annotations"]["readOnlyHint"] == json!(true))
  ```

  `readOnlyHint` comes from the `@readonly` decorator, lowered by fluessig's MCP
  projection into `annotations` in `TOOLS_JSON`
  (`crates/disponent-core/src/mcp_generated.rs`). So the worker surface is
  **exactly the read-only tools**, one boolean per op. A worker-*writable* op is
  invisible under that gate — the constraint §5 solves.

## 3. The collapse — one message, one send (plus a lightweight ack)

The whole design is one entity and one primary op:

- **One `Message` entity** (§4): a durable record of "who put what in whose
  inbox," anchored to a worker session's timeline so both parties pull it
  through the `events`/`wait` path they already have.
- **One `send`** (§6), used by both roles, its behavior differentiated **only by
  who the sender's address book contains**:
  - The **Manager** has the full view. It can `send` to any worker subset (by
    tag — that is preference fan-out) and to the **user**.
  - A **worker** knows exactly one address: **its Manager**. The worker-role
    server fills the recipient in from the bound session; the worker supplies
    **no recipient at all**.
- **A lightweight `ack`** (§7): a worker's explicit "received/handled," which the
  Manager can observe — the part that makes a fan-out to a thousand workers
  legible.

Every verb from draft 1 falls out of this with nothing added:

| draft 1 | now |
|---|---|
| `notify(target, body)` (fan-out) | Manager `send` to a tagged subset |
| `ask(body)` (worker question) | worker `send` (→ its Manager) |
| `answer(qid, body)` | Manager `send` to that worker |
| `escalate(qid, note)` | Manager `send` to the **user** |
| `Question` entity + `status` enum | a `Message` + its `inReplyTo` chain |

A "question" is a worker message to the Manager; "answering" is the Manager
messaging back; "escalation" is the Manager messaging the user. Threading
(`inReplyTo`) is the only structure needed to relate them, and pm reconstructs a
question's life by walking the reply chain — no `status` machine, no dedicated
entity. (`ack` survives from draft 1 as a first-class op, §7 — the read cursor
handles delivery, `ack` reports handling; the two are not redundant.)

## 4. Data model

One new entity, one new enum, one new scalar, one new event variant, and a `tags`
field on the existing `Dispatch` — sketched in the `schema/disponent.tsp`
conventions (`@entity @name(...)`, `@key`, `@fk(#[...])`, doc comments that flow
to the docs site and MCP tool text). `Message` is a **control-plane** row; unlike
`Session`, no environment backs it (§11).

```typespec
// ── scalars ──
/** Disponent-minted message id (UUIDv7). */
scalar MessageId extends string;
/** Disponent-minted fan-out id (UUIDv7): one broadcast, shared by its N Messages. */
scalar FanoutId extends string;

// ── enums (wire values are the stored strings) ──
/** The three principals a message can move between. */
enum Party {
  manager,  // the managing agent/human that dispatched the session
  worker,   // a dispatched, leaf-node agent
  user,     // the human above the Manager (escalation target)
}

/** One message dropped in one inbox. The Manager mints these addressed to a
 * worker or the user; a worker mints them addressed (implicitly) to its
 * Manager. Disponent owns these rows — no environment backs them (§11). */
@entity
@name("messages")
model Message {
  @key id: MessageId;
  createdAt: utcDateTime;

  sender: Party;
  recipient: Party;

  /** The worker session this message rides. EVERY message anchors to exactly
   * one session's timeline — that is how both parties pull it via `events`
   * (§7): a Manager→worker note rides the recipient's timeline, a
   * worker→Manager question rides the sender's, a Manager→user escalation rides
   * the timeline of the worker it is about. */
  @fk(#["session_uid"]) session: Session;

  /** The payload — free-form text, like the brief. Structure is the consumer's. */
  body: string;

  /** Threading: the message this one replies to (an answer to a question, an
   * escalation of a worker's message). Null for an unsolicited directive.
   * Walking `inReplyTo` reconstructs a question's whole life — no status enum. */
  @fk(#["in_reply_to"]) inReplyTo?: Message;

  /** One logical Manager broadcast → N Messages that all share this id. A
   * single-recipient send still gets one (a fan-out of one). Counting acks over
   * a `fanoutId` is how the Manager sees "N of M picked up the directive" (§7). */
  fanoutId: FanoutId;

  /** Supersession key. A newer fan-out carrying the same `topic` supersedes
   * older same-topic messages in an inbox: a worker reading its inbox acts on
   * the LATEST message per topic and skips the stale ones, so a thousand
   * workers don't burn usage on a directive already overtaken (§7). Null =
   * standalone, never superseded. */
  topic?: string;

  /** Stamped by the recipient's `ack` (§7): received/handled. Manager-observable.
   * Null = delivered (readable on the feed) but not yet acknowledged. */
  ackedAt?: utcDateTime;
}
```

**Tags on sessions.** Selection (§8) addresses a fan-out by tag, so add a flat,
indexable tag set to the immutable `Dispatch` (a session inherits its dispatch's
tags). This is distinct from the pre-existing opaque `labels: Json`, which stays
for arbitrary consumer metadata; `tags` are exactly what selection matches on.

```typespec
// added to model Dispatch (and DispatchSpec):
/** Selection tags — the PRIMARY handle the Manager addresses a fan-out to (§8).
 * A session inherits its dispatch's tags. Flat strings, indexable; distinct
 * from the opaque `labels: Json`. */
tags?: string[];
```

No `Inbox`, `Subscription`, or `Question` entity: an inbox **is** a query over
`messages` filtered by recipient and anchor (to-manys are queries in this
schema, like `Event`/`Artifact`/`Usage`):

| inbox | the query | who reads it |
|---|---|---|
| a worker's | `recipient=worker AND session=self` | the worker (self-scoped, §9) |
| the Manager's | `recipient=manager` | the Manager (full `events`) |
| the user's | `recipient=user` | pm, surfaced to the human (§10) |

### The new event variant

So both flows show up on the one feed pm already renders (design §7), a message
projects onto its anchor session's timeline as a new `EventKind`:

```typespec
enum EventKind {
  state, message, tool_call, tool_result, log, usage, artifact, raw,
  mail,   // a control-plane Message landed on this session's timeline
}

union EventPayload {
  // … existing variants …
  mail: MailRef,
}
/** Pointer + the fields a reader needs to triage without fetching the Message:
 * direction (sender/recipient), the fan-out it belongs to, and its topic (so a
 * worker can group by topic for latest-wins, §7). */
model MailRef {
  messageId: MessageId;
  sender: Party;
  recipient: Party;
  fanoutId: FanoutId;
  topic?: string;
}
```

The payload is a **pointer** (the id) plus the triage fields, matching
`ArtifactRef`; the `Message` row is the record, the `mail` event is the timeline
breadcrumb the pull path reads. `EventKind.message` (an agent's own transcript
line) and `EventKind.mail` (a control-plane message) stay distinct — one is
observed, the other is minted. Fidelity of every `mail` event is `exact` (§11).

**Does the recipient know who it is from?** Yes, in two parts. The `mail` event
surfaces the coarse `sender: Party` (`manager` / `worker` / `user`), and the
*specific* counterpart is the event's anchor `session` (`Event.session`): a
worker→Manager message rides the sending worker's timeline, so when the Manager
reads its inbox each `mail` event's anchor names **which** worker asked —
exactly the uid it needs to answer or escalate correctly (§10). A worker's own
inbox is the mirror image: every inbound message is `sender: manager` (there is
one Manager, so the role alone identifies the sender — the field is present but
constant), anchored to the worker's own session.

## 5. The role gate, refined — `@worker` gates exactly two ops

The smallest possible change. Add one op-level decorator, `@worker`, meaning
*"projected into the worker surface even though it is not read-only."* It lowers
(fluessig MCP projection, design §13.2) to a new annotation `workerHint: true`
alongside `readOnlyHint` / `destructiveHint`. The gate becomes:

```rust
.filter(|t| role == Role::Supervisor
            || t["annotations"]["readOnlyHint"] == json!(true)
            || t["annotations"]["workerHint"]   == json!(true))
```

`@worker` is applied to **exactly two** ops — `send` and `ack` — and to nothing
else, ever:

| op | `readOnly` | `worker` | `destructive` | worker sees it? |
|---|---|---|---|---|
| `dispatch` | | | | **no** |
| `send` | | yes | | yes (write; recipient defaulted to Manager, §9) |
| `ack` | | yes | | yes (write; own inbox only, §9) |
| `cancel`, `reap` | | | yes | **no** |
| `resume` | | | | **no** |
| `sessions`, `events`, `offerings` | yes | | | yes (observe / read inbox) |

The worker surface stays tiny: two self-scoped writes (`send` up to its Manager,
`ack` on its own inbox) plus the read-only observe tools it already had. Reading
messages is the existing `events`/`wait` (already `@readonly`, already
worker-visible) — no new read op. The no-recursion invariant becomes one
checkable line: **`send` and `ack` are the only `@worker` ops, and neither is
`@destructive` nor `dispatch`.**

## 6. Op surface

`send` is generalized from today's `send(sessionUid, input): void` into the
symmetric primitive; `ack` is added. Reading is the existing `events` / `wait`.

```typespec
/** Where a Manager-sent message goes. A worker never fills this (§9): the
 * worker-role server defaults recipient = its Manager, anchored to the bound
 * session. A Manager sets exactly one destination. */
model SendTarget {
  /** PRIMARY (§8): every live session whose dispatch carries any of these tags.
   * `tags:["projectA"]` reaches all projectA workers without enumerating uids. */
  tags?: string[];
  /** Precise fallback: exact recipients by session uid. */
  sessions?: SessionUid[];
  /** Escalate to the human above the Manager, about this worker session (§10). */
  user?: SessionUid;
}

interface Disponent {
  // … unchanged ops …

  /** The one messaging primitive, used by both roles.
   *  - Manager: `to` names a tagged worker subset (fan-out) or the user.
   *  - worker:  `to` is filled in server-side — recipient = the Manager of the
   *    bound session, anchored to that session; the worker names no one.
   * A multi-recipient send mints one Message per matched session, all sharing a
   * freshly minted `fanoutId`; `topic` (optional) is the supersession key for
   * latest-wins (§7). Returns the Messages minted. Delivery is the reader's
   * `events` pull; `ack` (below) is the recipient's acknowledgement. */
  @worker send(
    to?: SendTarget,
    body: string,
    inReplyTo?: MessageId,
    topic?: string,
  ): Message[];

  /** Acknowledge a message you received (received/handled). Self-scoped: a
   * worker acks only its own inbox (§9); stamps `ackedAt`, which the Manager
   * observes across a `fanoutId` to see "N of M acted" (§7). Idempotent. */
  @worker ack(messageId: MessageId): void;
}
```

Reading, both roles, is unchanged tooling:

- A **worker** reads its inbox with `events` — self-scoped to its bound session
  (§9), filtered `kinds: ["mail"]`, resuming from its `afterIdx` cursor; or
  blocks on `wait`. It groups by `topic` for latest-wins (§7), then `ack`s.
- The **Manager** reads worker→Manager mail with the same `events`, unrestricted
  (it already watches every session feed): `events(kinds: ["mail"])` across the
  fleet. It watches a fan-out's progress by querying `messages` on `fanoutId`.

`send`'s old prompt-injection meaning (push text at a live prompt via `interact`)
is not lost — it becomes one possible *backend delivery* of a `mail` message on
an `interact`-capable env. But the **contract is pull**: `send` records a
`Message` and projects a `mail` event; the recipient pulls it. Disponent does
not promise to interrupt a running agent (design §7: "Disponent does not
interpolate").

## 7. Delivery — read cursor, ack, and latest-wins

Delivery is **pull-based, and it stays that way** (decided, §14.1): `send`
writes a `Message` and projects a `mail` event; the recipient pulls it through
`events`/`wait` on its own cadence. Disponent never pushes into a worker's
tmux/PTY. If the Manager ever needs a worker to *stop* — flow control,
backpressure — that is done by **pausing or stopping the worker's process**
(`cancel`/`reap`, or a future pause), not by a push/backpressure channel.
Push is a non-goal.

Two positions doing two different jobs:

- **The read cursor — delivery.** Each `mail` event has a monotonic `Event.idx`
  on its session timeline (the existing mechanism — messages need no private
  sequence column). A reader resumes from the last `idx` it saw. This is
  **at-least-once and idempotent by construction**: reading is non-mutating, so
  a reader that reconnects and re-reads from an older `afterIdx` simply sees the
  message again and advances. Nothing to double-apply; every message is readable
  until the reader moves past it.
- **`ack` — acknowledgement.** A worker calls `ack(messageId)` when it has
  received/handled a message; that stamps `ackedAt`, which the **Manager can
  observe** (the read cursor is private to the reader and invisible to the
  Manager). This matters precisely at **fan-out scale**: after a directive to a
  thousand workers, `messages WHERE fanoutId=F AND ackedAt IS NOT NULL` tells
  the Manager how many have picked it up — a real progress view the cursor alone
  cannot give. `ack` is a `@worker` op, self-scoped (a worker acks only its own
  inbox), and idempotent.

Restoring `ack` (draft 1 had it, draft 2 dropped it) is deliberate: the cursor
guarantees *delivery*, `ack` reports *handling*. They are not redundant — one is
the reader's business, the other is the Manager's.

**Latest-wins — don't burn usage on stale directives.** A fan-out carries a
`topic`. Within an inbox, a newer message on the same `topic` supersedes older
ones: a worker reading its inbox groups `mail` by `topic` (surfaced on the event,
no fetch needed) and acts only on the newest per topic, skipping the superseded.
So when the Manager sends "use pnpm" and then "actually, use bun" to
`tags:["projectA"]` with the same `topic:"package-manager"`, a worker that reads
both acts once — on bun — and a thousand workers do not each act twice and waste
a turn. Superseded messages are not deleted (the timeline is append-only); they
are simply ignored by the read-side convention. (Scope of supersession is a
genuine sub-question — see §14.7.)

- **Ordering.** Per-inbox FIFO by `Event.idx` on the anchor timeline. No
  cross-session order is promised: a fan-out to N workers is N independent
  timeline appends.
- **Wait-based reads (later).** The blocking analogue is the existing `@manual
  wait(sessionUid, timeoutSecs)`; a worker-scoped `wait` that returns on the
  next `mail` event is a v1.1 nicety. Polling `events` on the observer cadence
  covers the MVP.

Fidelity of `mail` events is `exact` — they are records of disponent's own
first-party `send` calls, not observations of an environment inferred from a
transcript (`derived`) or a tmux capture (`scraped`). No env mediates them, so
`exact` is the truthful grade (§11).

## 8. Selection targeting — tags first

The Manager addresses a fan-out by **tag**, with uid as the precise fallback:

1. **By tag (primary).** With a thousand agents tagged `projectA` and a thousand
   `projectB`, `tags:["projectA"]` reaches the first group without enumerating a
   thousand uids — enumeration is exactly the thing to avoid at fleet scale.
   `SendTarget.tags` resolves at send time to `sessions WHERE dispatch.tags ⊇
   predicate AND state ∈ live`.
2. **By session uid (fallback).** `SendTarget.sessions` names exact recipients —
   the precise handle for "just these three," and the ground the ledger records.
3. **By capability grade — rejected.** An env's `CapabilityKind` set describes
   the *environment*, not the task; too coarse to select workers. Not a selector.

Whichever handle is used, the durable record is the **concrete recipient list** —
one `Message` per matched session, all sharing one `fanoutId` — frozen at send
time. A tag predicate is resolved *then*, not kept live: late-joining sessions do
not retroactively receive an old directive (honest, less-surprising semantics; a
newer session gets the *next* fan-out, and topic-scoped latest-wins keeps it from
acting on a stale one it never saw). Tags build on the `tags: string[]` added to
`Dispatch`/`DispatchSpec` (§4), distinct from the opaque `labels: Json`.

Worker send needs no targeting: its recipient is always its Manager (§9).

## 9. Security posture — the one guard is no-dispatch

The invariant (design §10): *humans and Manager agents dispatch; dispatched
agents are leaf nodes.* This design lets workers talk back while keeping that
one guarantee. What a compromised or adversarial worker can and cannot do:

**CAN** (its whole surface): read its own sessions/events/offerings (today);
read **its own** inbox (`events`, self-scoped, `kinds:["mail"]`); `send` a
message up to its Manager; `ack` a message in its own inbox.

**CANNOT**: `dispatch` or spawn any session — **not on the worker surface**;
`cancel` / `reap` / `resume` — not on the surface. That is the load-bearing line.

Two layers, in priority order:

1. **Tool projection — the real no-recursion guard.** The worker-role server
   projects only `readOnlyHint || workerHint` tools (§5). `dispatch` / `cancel` /
   `reap` / `resume` carry neither, so a worker **physically cannot** spawn or
   drive any session. The invariant holds by tool absence, and it is checkable
   in CI (§5).
2. **Server-side recipient defaulting — a convenience, not a guard.** A worker's
   `send` needs no recipient because the worker-role server fills it in from the
   bound session's Manager (bind it with `boundSession?: SessionUid` on
   `McpOptions`, set when the env wires the worker's endpoint alongside
   `role: worker`); `events`/`wait` likewise default "self" to that session. We
   are **not** defending against a worker that *spoofs* a recipient —
   sender-spoofing is out of the threat model (a worker that wanted to misbehave
   has easier targets, and the Manager reads every feed anyway). The defaulting
   is ergonomics: one less address a worker must carry, and tidy inbox scoping —
   not a security-critical gate.

So the property worth stating plainly is narrow: **a worker has no op that
dispatches or spawns another agent.** Everything else — who it messages, which
feed it reads — is convenience on top of that one guarantee, and the design does
not over-index on locking it down.

## 10. Escalation to the human — building on pm's feed + composer

pm#158 built, on top of disponent's `send`/`events` ops, a **send-composer** (a
box that calls `send`) and an **event feed** (a live render of `events`). The
escalation flow rides both, adding no new pm↔disponent transport:

1. A worker `send`s up. This mints a `Message{sender:worker, recipient:manager}`
   and a `mail` event on the worker's timeline. pm's feed **already tails
   `events`**, so the question surfaces the moment it lands — the feed just
   learns to render a `mail` event from a worker as a "needs a decision" card
   rather than a log line.
2. The Manager (the Claude driving pm, or the human at the pm UI) sees it and
   picks one of two affordances the composer grows — both of them *the same
   `send`*, differently addressed:
   - **Answer** → `send({sessions:[wk]}, body, inReplyTo: q)`. A
     `Message{sender:manager, recipient:worker}` lands on the worker's timeline;
     the worker pulls it off the same feed it is already reading.
   - **Escalate** → `send({user: wk}, body, inReplyTo: q)`. A
     `Message{sender:manager, recipient:user}` is minted; pm surfaces it in a
     "For you" queue (a filtered view of `messages WHERE recipient=user`). When
     the human answers there, the Manager relays a `send({sessions:[wk]}, …,
     inReplyTo: q)` — **and it may reformat or reinterpret the human's words
     first**; what the worker receives is the Manager's message, `sender:manager`.

The worker does **not** learn where an answer came from, by design (§14.6): the
Manager may reshape the answer, and a worker might not even know a user exists.
The may-or-may-not nature is recorded honestly on the **Manager side** by *where
the message went*: a question with a `recipient=user` message in its `inReplyTo`
chain was escalated; one answered straight from the Manager was not. pm reads
that off the `Message` rows — no `status` field, no `escalate` op, just the
addresses.

## 11. Persistence in the ledger and reconciliation

`Message` rows live in the memory ledger and mirror to SQLite through
`driverPlan()` exactly like every other entity — one new table, upserted by the
same thin executors (design §9). Mechanical.

The **honest tension** is with design §3, *environments are the source of
truth.* Sessions reconcile because the env owns them (a tmux session, a VM).
**Messages have no env behind them** — disponent mints them, disponent *is*
their source of truth. They are the first ledger-owned control-plane entity in
the system. Stated plainly:

- **`reconcile()` does not touch them.** There is nothing in an environment to
  re-adopt a message from. Reconcile still re-adopts sessions; messages persist
  across it, anchored by session uid. When a session goes `lost` and later
  reconciles back, its inbox is still attached — an upside of disponent owning
  it.
- **Durability is the SQLite mirror, not env reality.** With the default sink
  on, messages survive a Manager restart. **Memory-only mode loses them** on
  exit — the same trade design §3 already names for streamed events. We do not
  pretend otherwise.
- **A reaped session's messages.** `reap()` archives the session; its messages
  archive with it, anchored to it. An unanswered worker question on a reaped
  session is simply an unread `mail` whose anchor is gone — pm filters reaped
  anchors out of the live queue.

**Decision (§14.4): accepted for the MVP.** Messages being the first
ledger-owned entity the environment does not back is a real asymmetry, but a
benign one — durability is the mirror, reconcile skips them, and if a future
backend ever gives messages an env home (a real mailbox on the VM) the model can
adopt it then. Fine for now, cheaply changed later.

This is also why every `mail` event is graded **`exact`**: it is a record of
disponent's own `send` call, with a row to prove it, no scrape or derived
reconstruction between the act and the record.

## 12. Phased implementation

Ordered smallest-first; the MVP is the minimum that delivers **both** flows.

**MVP — both flows, one primitive, pull-based.**
- Schema: the `Message` entity (with `fanoutId`, `topic`, `ackedAt`); the `Party`
  enum; the `FanoutId`/`MessageId` scalars; the `mail` `EventKind` /
  `EventPayload` variant; `tags: string[]` on `Dispatch`/`DispatchSpec`. Regen.
- The `@worker` decorator + fluessig's `workerHint` lowering (design §13.2), and
  the one-line `tools_for` gate extension (§5), gating `send` and `ack`.
- Generalize `send` to `send(to?, body, inReplyTo?, topic?): Message[]` with
  tag/uid/user addressing, per-send `fanoutId` minting, and worker recipient
  defaulting; add `ack`.
- `McpOptions.boundSession` + worker self-scoping for `send` (recipient) and
  `events`/`wait` (inbox).
- The topic-scoped latest-wins read convention (worker groups its inbox by
  `topic`, acts on the newest, then `ack`s).
- pm renders the `mail` event, shows fan-out ack progress, and grows the
  answer/escalate affordances on the pm#158 composer (§10).

Enough for: user → Manager → `send({tags:[…]}, …, topic)` → a fleet reads inbox,
acts on the latest, acks; and worker `send` → Manager reads → `send` back **or**
`send` to user → human → Manager relays back.

**v1.1 — ergonomics.**
- A worker-scoped blocking `wait` that returns on the next `mail` event (§7).
- Server-marked supersession (a `supersededBy` pointer) if pm wants to gray out
  stale cards rather than rely on the read-side convention (§14.7).

**Out (deliberately / non-goals).**
- **Push / backpressure delivery** — a non-goal (§7): flow control is stopping or
  pausing the worker's process, not a push channel.
- A `Question`/`status` machine (replaced by messages + `inReplyTo`).
- worker→worker messaging (no dispatch/spawn on the worker surface, §9).
- Structured message bodies (strings, like briefs — structure is the consumer's,
  design §4).
- Capability-graded targeting (§8 rejects it as a selector).

## 13. Two worked examples

### (a) Preference fan-out to a thousand workers — down

User, mid-flight, to the Manager: *"Use pnpm everywhere on projectA, not npm."*
About a thousand live workers carry the tag `projectA` (inherited from their
dispatch); another thousand carry `projectB`.

```text
Manager:  send(
            { tags: ["projectA"] },                 // a tag, not a thousand uids
            "Use pnpm, not npm, for all package operations.",
            topic: "package-manager"                // the supersession key
          )
          // resolves the tag → the ~1000 live projectA sessions at send time;
          // mints one Message per session, ALL sharing one fanoutId.
          → [ Message{ id:m1, fanoutId:f1, topic:"package-manager",
                       sender:manager, recipient:worker, session:wk-0001 },
              … ~1000 rows, fanoutId=f1 … ]
          // projects: a mail event on each projectA timeline (exact). projectB untouched.

worker wk-0007:  events({ sessionUid:<self>, afterIdx:<cursor>, kinds:["mail"] })
          // self-scoped: server resolves <self> to boundSession=wk-0007
          → [ Event{ kind:mail, payload:{ messageId:m7, fanoutId:f1,
                                          topic:"package-manager", sender:manager } } ]
          // groups inbox by topic, acts on the LATEST per topic → applies pnpm
          ack("m7")                                 // stamps ackedAt; Manager-visible

Manager:  // watches the fan-out land without polling each worker:
          messages({ fanoutId:"f1" })  → ~1000 rows, 640 with ackedAt set
          // a real "640 of 1000 picked it up" progress view.
```

Then the user changes their mind — latest-wins earns its keep:

```text
Manager:  send({ tags:["projectA"] }, "Scratch that — bun, not pnpm.",
               topic: "package-manager")            // SAME topic → supersedes f1
          → ~1000 new Messages, fanoutId=f2, topic="package-manager"

worker wk-0007:  events(...)   // now sees BOTH m7 (f1) and the f2 message, same topic
          // latest-wins: acts once, on bun (f2), skips the superseded pnpm (f1).
          // It does NOT burn a turn applying pnpm and then bun — one action.
          ack(<f2 message>)
```

No restart, no broadcast to projectB, no thousand-uid enumeration. Tags address
the group, the `fanoutId` gives the Manager an ack-based progress view, and
topic-scoped latest-wins keeps a worker from spending usage on a directive
already overtaken.

### (b) Question escalation — up

Worker wk-B is mid-migration and unsure.

```text
worker wk-B:  send("Migration 0007 drops `users.legacy_id`, which still has data. Proceed?")
          // no `to` — the worker-role server fills recipient=manager, session=wk-B
          → Message{ id:q1, fanoutId:fq, sender:worker, recipient:manager, session:wk-B }
          // projects: mail@wk-B  (fidelity: exact)

// pm's feed (tailing `events`, per pm#158) renders q1 as a "needs a decision" card.

Manager:  events({ sessionUid: "wk-B", kinds: ["mail"] })  → sees q1
          // The Manager judges: dropping data needs the human.
Manager:  send({ user: "wk-B" }, "wk-B wants to drop users.legacy_id (has data). Approve?",
               inReplyTo: q1)
          → Message{ id:e1, sender:manager, recipient:user, session:wk-B, inReplyTo:q1 }
          // pm surfaces e1 in the "For you" queue.

// Human answers in pm. The Manager relays — reshaping the wording as it sees fit:
Manager:  send({ sessions: ["wk-B"] }, "No — keep the column, backfill instead.",
               inReplyTo: q1)
          → Message{ id:a1, sender:manager, recipient:worker, session:wk-B, inReplyTo:q1 }

worker wk-B:  events({ sessionUid: <self>, afterIdx: <cursor>, kinds: ["mail"] })
          → [ Event{ kind:mail, payload:{ messageId:a1, sender:manager } } ]
          ack("a1")
          // reads a1 → keeps the column. It never knew a human, not the Manager,
          // answered — the escalation was invisible to it (§14.6).
```

Had the Manager known the convention, it skips the `user` send and answers wk-B
directly — same inbox delivery, no human paged. The difference is one address
(`user` vs `sessions`), and the `inReplyTo` chain records which happened. That is
the may-or-may-not escalation, with far fewer moving parts than draft 1's
`ask`/`answer`/`escalate`.

## 14. Decisions and open questions

Resolved (folded into the body above):

1. **Push vs pull — DECIDED: pull-only.** Pull is the model. Making a worker stop
   (flow control) is done by pausing or stopping its process (`cancel`/`reap`, or
   a future pause), not by a push/backpressure channel. Push is a non-goal
   (§7, §12).
2. **Read-receipt — DECIDED: keep `ack`.** Restored as a first-class `@worker`
   op. The read cursor is *delivery*; `ack` is *acknowledgement*; counting acks
   over a `fanoutId` gives the fan-out progress view the cursor alone cannot
   (§7).
3. **`boundSession` provenance — DECIDED (relaxed).** Since sender-spoofing is
   out of the threat model (§9), the binding is a convenience, not a
   security-critical guard; its provenance need only be good enough to default
   "self" correctly (env-injected config, or the tmux session name disponent
   already labels, design §3). Not a blocker.
4. **Ledger asymmetry — DECIDED: accepted for the MVP.** Messages are the first
   ledger-owned entity the env doesn't back; reconcile skips them and durability
   is the mirror (§11). Fine for now, cheaply changed later.
5. **Fan-out atomicity — DECIDED: acceptable, softened by latest-wins.** A
   partial fan-out (process died mid-send) is fine: re-sending the same
   `(tags, topic)` supersedes the partial via latest-wins (§7), so a redo is
   safe. No fan-out-completion transaction needed for the MVP.
6. **Escalation provenance — DECIDED: relayed answers are `sender: manager`, no
   origin surfaced.** The worker need not know where an answer came from — the
   Manager may reformat or reinterpret it before relaying, and a worker might not
   even know a user exists (§10). No `origin` marker; add one later only in the
   unlikely event a worker must treat a human-authored answer differently.

Still open:

7. **Supersession scope.** Latest-wins is defined per (inbox, topic): a worker
   ignores older same-topic messages in *its own* inbox (§7). Two genuine edges:
   (a) is `topic` a free string the Manager coins per directive, or should it be
   a light namespace to avoid accidental collisions between unrelated fan-outs
   that happen to reuse a word? (b) should disponent stamp a `supersededBy`
   pointer when a newer same-topic fan-out lands (so pm can gray out stale cards
   server-side), or stay purely read-side? Leaning free-string topic +
   read-side for the MVP; revisit if pm wants server-marked supersession.
8. **The anchor for a fleet-wide user note.** Every `Message` anchors to a
   session (§4). An escalation is always about a worker, so that is fine — but a
   Manager note to the user *not* about any one worker has no anchor. Do we ever
   need one, or is "the user hears from the Manager only about workers" an
   acceptable limit? Leaning acceptable; the Manager talks to its own user
   outside disponent.
