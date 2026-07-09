# disponent — manager↔worker communication

*Draft 2, 2026-07-08. A design note, sibling to [design.md](./design.md) and
[ai-dispatch-comparison.md](./ai-dispatch-comparison.md). It proposes two flows —
a targeted directive channel down to workers, and a question channel up from
workers — collapsed onto a **single symmetric message primitive**. Nothing here
changes `schema/disponent.tsp` or any generated file yet; it is the argument for
the schema change, written first. Draft 1 modelled the two flows with a zoo of
ops (`notify`/`ask`/`answer`/`escalate`) and a separate `Question` entity; draft
2 replaces all of that with one `Message` entity and one `send`, on the insight
that **addressing does the security work**.*

Read [design.md](./design.md) for the model this builds on: environments are the
source of truth (§3), sessions run until reaped (§5), events carry a `fidelity`
grade (§7), and the MCP surface is **role-scoped** — the Manager gets the full
surface, workers observe only, so a dispatched agent cannot recurse (§10). (This
note uses **Manager** for the supervising principal design §10 calls the
"supervisor"; workers are unchanged.) The no-recursion invariant is the thing
most at risk from letting workers talk back; §9 shows why the collapse preserves
it *more* cleanly than draft 1 did.

## 1. Motivation — the two flows

Two things the MVP topology (design §14: a Manager Claude reaching a fleet of
worker VMs over stdio MCP) cannot express today, both about *communication
between the Manager and running workers*:

**(a) Targeted preference fan-out (down).** The user tells the Manager a
preference mid-flight — "use `bun`, not `npm`, everywhere" — and the Manager
wants to hand that to a **selected subset** of the running workers (the ones
touching package config), not broadcast it to the whole fleet and not restart
anyone. Each selected worker picks the preference up **through its own MCP
surface**, on an inbox it pulls on its own cadence. Today the only downward
channel is `send(sessionUid, input)` — one session, a Manager-only write that
shoves raw text at a prompt. There is no "these five sessions," no durable
record the worker pulls when it chooses, and nothing a worker is allowed to read
as a first-class message.

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
direction and per verb. Draft 2 names the act once.

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
  §7 leans on this so the messaging channel needs no new read op and no ack.
- **`send` today.** `send(sessionUid: SessionUid, input: string): void` — a
  Manager-only write that injects text into one running session's prompt (the
  `interact` capability). Draft 2 **generalizes this op** into the symmetric
  message primitive (§3, §6); it stays the same name, gains addressing, and
  delivers to an inbox instead of shoving at a prompt.
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

## 3. The collapse — one message, one send, addressing does the security work

The whole design is one entity and one op:

- **One `Message` entity** (§4): a durable record of "who put what in whose
  inbox," anchored to a worker session's timeline so both parties pull it
  through the `events`/`wait` path they already have.
- **One `send`** (§6), used by both roles, its behavior differentiated **only by
  who the sender's address book contains**:
  - The **Manager** has the full view. It can `send` to any worker (a selected
    subset — that is preference fan-out) and to the **user**.
  - A **worker** knows exactly one address: **its Manager**. The worker-role
    server resolves the recipient to "the Manager of this bound session"
    server-side; the worker supplies **no recipient at all**.

Every verb from draft 1 falls out of this with nothing added:

| draft 1 | draft 2 |
|---|---|
| `notify(target, body)` (fan-out) | Manager `send` to a worker subset |
| `ask(body)` (worker question) | worker `send` (→ its Manager) |
| `answer(qid, body)` | Manager `send` to that worker |
| `escalate(qid, note)` | Manager `send` to the **user** |
| `Question` entity, `status`, `ack` op | a `Message`, its `inReplyTo`, and a read cursor |

A "question" is a worker message to the Manager; "answering" is the Manager
messaging back; "escalation" is the Manager messaging the user. Threading
(`inReplyTo`) is the only structure needed to relate them, and pm reconstructs a
question's life by walking the reply chain — no `status` machine, no dedicated
entity.

**Addressing does the security work.** A worker cannot express any destination
but its Manager — it has no vocabulary for a sibling worker or the environment.
So "workers can't message each other" and "workers can't recurse" are not
policed at call time; they are **structural**, a property of what a worker can
even say (§9).

## 4. Data model

One new entity, one new enum, one new event variant — sketched in the
`schema/disponent.tsp` conventions (`@entity @name(...)`, `@key`, `@fk(#[...])`,
doc comments that flow to the docs site and MCP tool text). `Message` is a
**control-plane** row; unlike `Session`, no environment backs it (§11).

```typespec
// ── scalars ──
/** Disponent-minted message id (UUIDv7). */
scalar MessageId extends string;

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
}
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
model MailRef { messageId: MessageId; sender: Party; recipient: Party; }
```

The payload is a **pointer** (the id + direction), matching `ArtifactRef`; the
`Message` row is the record, the `mail` event is the timeline breadcrumb the
pull path reads. `EventKind.message` (an agent's own transcript line) and
`EventKind.mail` (a control-plane message) stay distinct — one is observed,
the other is minted. Fidelity of every `mail` event is `exact` (§11).

## 5. The role gate, refined — `@worker` gates exactly one op

The smallest possible change. Add one op-level decorator, `@worker`, meaning
*"projected into the worker surface even though it is not read-only."* It lowers
(fluessig MCP projection, design §13.2) to a new annotation `workerHint: true`
alongside `readOnlyHint` / `destructiveHint`. The gate becomes:

```rust
.filter(|t| role == Role::Supervisor
            || t["annotations"]["readOnlyHint"] == json!(true)
            || t["annotations"]["workerHint"]   == json!(true))
```

`@worker` is applied to **exactly one** op — `send` — and to nothing else, ever:

| op | `readOnly` | `worker` | `destructive` | worker sees it? |
|---|---|---|---|---|
| `dispatch` | | | | **no** |
| `send` | | yes | | yes (the one write; recipient forced, §9) |
| `cancel`, `reap` | | | yes | **no** |
| `resume` | | | | **no** |
| `sessions`, `events`, `offerings` | yes | | | yes (observe / read inbox) |

Compared to draft 1, the worker surface *shrinks*: draft 1 needed `ask` **and**
`ack` on the worker surface; draft 2 needs just `send`, because reading is the
existing `events`/`wait` (already `@readonly`, so already worker-visible) and
there is no ack (§7). The no-recursion invariant becomes one checkable line:
**`send` is the only `@worker` op, and it is not `@destructive` and not
`dispatch`.**

## 6. Op surface

`send` is generalized from today's `send(sessionUid, input): void` into the
symmetric primitive. Nothing else is added — reading is the existing `events` /
`wait`.

```typespec
/** Where a Manager-sent message goes. A worker never fills this (§9): the
 * worker-role server forces recipient = its Manager, anchored to the bound
 * session. A Manager sets exactly one destination. */
model SendTarget {
  /** Worker recipients by session uid — the selection primitive (§8). */
  sessions?: SessionUid[];
  /** Sugar: a label predicate resolved to a session set at send time (§8). */
  labelsMatch?: Json;
  /** Escalate to the human above the Manager, about this worker session (§10). */
  user?: SessionUid;
}

interface Disponent {
  // … unchanged ops …

  /** The one messaging primitive, used by both roles. Addressing does the
   * security work (§9):
   *  - Manager: `to` names a worker subset (fan-out) or the user (escalation).
   *  - worker:  `to` is IGNORED. The worker-role server forces recipient = the
   *    Manager of the bound session, anchored to that session — the worker names
   *    no one. `@worker` makes this the single write a worker gets.
   * Returns the Message(s) minted — one per recipient on a fan-out. Read the
   * other side via `events`/`wait` (§7); there is no separate deliver or ack. */
  @worker send(to?: SendTarget, body: string, inReplyTo?: MessageId): Message[];
}
```

Reading, both roles, is unchanged tooling:

- A **worker** reads its inbox with `events` — self-scoped to its bound session
  (§9), filtered `kinds: ["mail"]`, resuming from its `afterIdx` cursor; or
  blocks on `wait`.
- The **Manager** reads worker→Manager mail with the same `events`, unrestricted
  (it already watches every session feed): `events(kinds: ["mail"])` across the
  fleet, or `events(sessionUid: wk, kinds: ["mail"])` for one worker.

`send`'s old prompt-injection meaning (push text at a live prompt via `interact`)
is not lost — it becomes one possible *backend delivery* of a `mail` message on
an `interact`-capable env. But the **contract is pull**: `send` records a
`Message` and projects a `mail` event; the recipient pulls it. Disponent does
not promise to interrupt a running agent (design §7: "Disponent does not
interpolate").

## 7. Delivery — the read cursor replaces ack

Delivery is **pull-based**. `send` does not push into the recipient's tmux/PTY;
it writes a `Message`, projects a `mail` event on the anchor session's timeline,
and lets the reader pull it through `events`/`wait` on its own cadence. This
keeps a worker a leaf that *reads a surface*, never a target disponent drives.

Draft 1 had a separate `ack` op and an `ackedAt` column. Draft 2 **drops both**,
because the event feed already carries the only cursor that matters:

- **The cursor is `events(afterIdx)`.** Each `mail` event has a monotonic
  `Event.idx` on its session timeline (the existing mechanism — messages need no
  private sequence column). A reader resumes from the last `idx` it saw.
- **At-least-once, idempotent by construction.** Reading is non-mutating: a
  reader that reconnects and re-reads from an older `afterIdx` simply sees the
  message again and moves its cursor forward. There is nothing to double-apply,
  so no ack is needed to make re-delivery safe. *Every message is readable until
  the reader advances past it; re-reading is free.* That is the whole guarantee,
  and it is strictly simpler than an ack round-trip that a worker could forget to
  send anyway.
- **Ordering.** Per-inbox FIFO by `Event.idx` on the anchor timeline. No
  cross-session order is promised: a fan-out to five workers is five independent
  timeline appends.
- **What is lost, honestly.** With no `ack`, the Manager gets no free
  read-receipt — it cannot tell from the ledger whether a worker *pulled* a
  directive. If it needs confirmation, that confirmation is **just another
  message**: the worker `send`s "applied bun" back up, like anything else. We
  do not build a bespoke ack channel for what the primitive already expresses.
- **Wait-based reads (later).** The blocking analogue is the existing `@manual
  wait(sessionUid, timeoutSecs)`; a worker-scoped `wait` that returns on the
  next `mail` event is the v1.1 nicety. Polling `events` on the observer cadence
  covers the MVP.

Fidelity of `mail` events is `exact` — they are records of disponent's own
first-party `send` calls, not observations of an environment inferred from a
transcript (`derived`) or a tmux capture (`scraped`). No env mediates them, so
`exact` is the truthful grade (§11).

## 8. Selection targeting

The Manager must name "these workers." Three candidate handles exist:

1. **By session uid** — `Session.uid`, the ledger's own key. Precise, unambiguous.
2. **By label** — `Dispatch.labels: Json` is already the consumer's opaque tag
   bag; a predicate over it ("touches package.json") is the natural selector.
3. **By capability grade** — an env's `CapabilityKind` set. Too coarse for
   *worker* selection (it describes the environment, not the task). Rejected as a
   selector.

**Recommendation: session uid is the primitive; label match is sugar over it.**
`SendTarget.sessions` is the ground truth the ledger records — one `Message` per
uid, so the audit trail is always per-recipient. `SendTarget.labelsMatch` is
resolved to a uid set **at send time** (`sessions WHERE labels ⊇ predicate AND
state ∈ live`) and then behaves identically. The durable record is always the
concrete recipient list, never a live predicate: a Manager sends to who was live
*then*, frozen into the `Message` rows. Late-joining sessions do not
retroactively receive an old directive — the honest, less-surprising semantics.

Worker send needs no targeting at all: its recipient is always its Manager (§9).

## 9. Security posture — addressing is the enforcement

The invariant (design §10): *humans and Manager agents dispatch; dispatched
agents are leaf nodes.* Draft 2 lets workers talk back, and preserves the
invariant **through what a worker can address**, not through which write ops
exist. What a compromised or adversarial worker can and cannot do:

**CAN** (its whole surface): read its own sessions/events/offerings (today);
read **its own** inbox (`events`, self-scoped, `kinds:["mail"]`); `send` exactly
one thing — a message **to its Manager**.

**CANNOT**: `dispatch` (spawn any session) — not on the worker surface; `send`
to a **sibling worker** or the **environment** — it has no way to *name* one, and
its recipient is server-forced (below); `send` to the **user** — only the
Manager escalates; `cancel` / `reap` / `resume` — not on the surface; read
another session's feed — `events` is self-scoped for a worker.

Two enforcement layers, both structural, neither trusting agent good behavior:

1. **Tool projection (existing, extended by one bit).** The worker-role server
   projects only `readOnlyHint || workerHint` tools (§5). `dispatch` / `cancel` /
   `reap` / `resume` carry neither, so they are physically absent — the same
   mechanism that keeps `dispatch` off the worker surface today.
2. **Server-side addressing (the new, and the whole point).** `send` is
   `@worker`, so a worker sees it — but the worker-role server is bound to one
   session identity at launch (add `boundSession?: SessionUid` to `McpOptions`,
   set when the env wires the worker's endpoint alongside `role: worker`).
   For a worker, the server **ignores any `to` argument** and forces
   `recipient = manager`, `session = boundSession`. Likewise `events`/`wait`
   resolve "self" from `boundSession`, never from a caller argument. So a worker
   that maliciously passes `to: {sessions:["sibling"]}` still sends only to its
   Manager, and one that passes `sessionUid: "sibling"` to `events` still reads
   only its own feed. **A worker literally cannot name another party.**

That is why the collapse is *safer* than draft 1, not just smaller: draft 1
protected the fleet by keeping the write ops off the worker surface and hoping
the surface stayed minimal; draft 2 puts one write on the surface but makes the
dangerous argument — the recipient — unspeakable by a worker. The property to
test in CI: `send` is the only `@worker` op; and the worker-role server, given
any `to`/`sessionUid`, resolves to the bound session and its Manager.

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
     the worker pulls it off the same feed it is already reading. The composer
     for an answer is the send-composer with its target set to the asking worker.
   - **Escalate** → `send({user: wk}, body, inReplyTo: q)`. A
     `Message{sender:manager, recipient:user}` is minted; pm surfaces it in a
     "For you" queue (a filtered view of `messages WHERE recipient=user`). When
     the human answers there, pm calls `send({sessions:[wk]}, …, inReplyTo: q)`
     on the Manager's behalf, and the worker's inbox receives it identically. The
     worker never knows whether a machine or a person answered.

The may-or-may-not nature is recorded honestly by *where the message went*: a
question with a `recipient=user` message in its reply chain was escalated; one
answered straight from the Manager was not. pm reads that off the `Message` rows
— no `status` field, no `escalate` op, just the addresses.

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
  exit — the same trade design §3 already names for streamed events. A directive
  sent in a memory-only session is as durable as an event in one; we do not
  pretend otherwise.
- **A reaped session's messages.** `reap()` archives the session; its messages
  archive with it, anchored to it. An unanswered worker question on a reaped
  session is simply an unread `mail` in the Manager's inbox whose anchor is gone
  — pm filters reaped anchors out of the live queue.

Because these rows carry no env handle, they never desync from an environment —
no scrape, no derived reconstruction — which is exactly why every `mail` event
is graded **`exact`**: it is a record of disponent's own `send` call, with a row
to prove it. (Contrast: if a future backend tried to *detect* a worker asking a
question by scraping its terminal for "should I proceed?", that inference would
be `derived` — but the `send` op makes that unnecessary, keeping the channel
`exact`.)

## 12. Phased implementation

Ordered smallest-first; the MVP is the minimum that delivers **both** flows.

**MVP — both flows, one primitive, pull-based.**
- Schema: the `Message` entity; the `Party` enum; the `mail` `EventKind` /
  `EventPayload` variant. Regen.
- The `@worker` decorator + fluessig's `workerHint` lowering (design §13.2), and
  the one-line `tools_for` gate extension (§5).
- Generalize `send` to `send(to?, body, inReplyTo?): Message[]` with Manager
  addressing (sessions list + user) and the worker recipient-forcing.
- `McpOptions.boundSession` + worker self-scoping for both `send` (recipient) and
  `events`/`wait` (inbox) (§9).
- pm renders the `mail` event and grows the answer/escalate affordances on the
  pm#158 composer (§10).

Enough for: user → Manager → `send({sessions:[…]})` → worker reads inbox; and
worker `send` → Manager reads → `send` back **or** `send` to user → human →
`send` back.

**v1.1 — ergonomics.**
- `labelsMatch` selection sugar (§8), resolved to a uid set at send time.
- A worker-scoped blocking `wait` that returns on the next `mail` event (§7).

**Later / maybe.**
- Push delivery (nudge an `interact`-capable worker's prompt when a `mail`
  lands) — deferred; it breaks the pull/leaf model (§6, §7). Kept an open
  question (§14), not a plan.
- Capability-graded targeting, if a second consumer asks (§8 rejects it as
  primary).

**Out (deliberately).** A `Question`/`status` machine (replaced by messages +
`inReplyTo`); a separate `ack` op (replaced by the read cursor, §7);
worker→worker messaging (unaddressable by construction, §9); structured message
bodies (strings, like briefs — structure is the consumer's, design §4).

## 13. Two worked examples

### (a) Preference fan-out — down

User, mid-flight, to the Manager Claude: *"Actually, use bun everywhere, not
npm."* Three workers are live; two touch package config (labeled
`{area: "pkg"}`), one writes docs.

```text
Manager:  sessions({ state: "running" })
          → [wk-A {labels:{area:"pkg"}}, wk-B {labels:{area:"pkg"}}, wk-C {labels:{area:"docs"}}]
Manager:  send(
            { sessions: ["wk-A", "wk-B"] },      // MVP: explicit uids
            "Use bun, not npm, for all package operations."
          )
          → [ Message{id:m1, sender:manager, recipient:worker, session:wk-A},
              Message{id:m2, sender:manager, recipient:worker, session:wk-B} ]
          // projects: mail@wk-A, mail@wk-B  (fidelity: exact). wk-C untouched.

worker wk-A:  events({ sessionUid: <self>, afterIdx: <cursor>, kinds: ["mail"] })
          // self-scoped: the server resolves <self> to boundSession=wk-A
          → [ Event{ kind:mail, idx:42, payload:{ messageId:m1, sender:manager } } ]
          // reads m1.body, applies it, advances its cursor past idx 42.
          // No ack. wk-B pulls m2 whenever it next polls.
```

No restart, no broadcast, wk-C never saw it. The Manager gets no read-receipt; if
it wants one, wk-A just `send`s "switched to bun" back up.

### (b) Question escalation — up

Worker wk-B is mid-migration and unsure.

```text
worker wk-B:  send("Migration 0007 drops `users.legacy_id`, which still has data. Proceed?")
          // no `to` — the worker-role server forces recipient=manager, session=wk-B
          → Message{ id:q1, sender:worker, recipient:manager, session:wk-B }
          // projects: mail@wk-B  (fidelity: exact)

// pm's feed (tailing `events`, per pm#158) renders q1 as a "needs a decision" card.

Manager:  events({ sessionUid: "wk-B", kinds: ["mail"] })  → sees q1
          // The Manager judges: dropping data needs the human.
Manager:  send({ user: "wk-B" }, "wk-B wants to drop users.legacy_id (has data). Approve?",
               inReplyTo: q1)
          → Message{ id:e1, sender:manager, recipient:user, session:wk-B, inReplyTo:q1 }
          // pm surfaces e1 in the "For you" queue.

// Human, in pm, answers. pm calls send on the Manager's behalf:
Manager:  send({ sessions: ["wk-B"] }, "No — keep the column, backfill instead.",
               inReplyTo: q1)
          → Message{ id:a1, sender:manager, recipient:worker, session:wk-B, inReplyTo:q1 }
          // projects: mail@wk-B

worker wk-B:  events({ sessionUid: <self>, afterIdx: <cursor>, kinds: ["mail"] })
          → [ Event{ kind:mail, payload:{ messageId:a1, sender:manager } } ]
          // reads a1 → keeps the column. It never knew a human, not the Manager,
          // answered — the escalation was invisible to it.
```

Had the Manager known the convention, it skips the `user` send and answers wk-B
directly — same inbox delivery, no human paged. The difference between the two is
one address (`user` vs `sessions`), and the `inReplyTo` chain records which
happened. That is the may-or-may-not escalation, with far fewer moving parts than
draft 1's `ask`/`answer`/`escalate`.

## 14. Open questions

1. **Push vs pull delivery.** MVP is pull (the reader calls `events`). Some
   directives are urgent ("stop touching auth.rs"); is pull ever too slow, and if
   so is the answer just today's prompt-injecting `send` on an `interact` env as
   a delivery mode (§6), rather than a new mechanism? Leaning pull-first.
2. **No read-receipt — is that ever a gap?** §7 drops `ack`, so the Manager
   cannot see delivery without a reply. If pm's fan-out UI genuinely needs
   "delivered to N of M," is a lightweight read-cursor readback worth
   reintroducing, or does an explicit reply message suffice? Leaning: reply
   suffices; revisit if the UI proves otherwise.
3. **`boundSession` provenance.** §9 binds the worker server to a session uid at
   launch. How is that uid delivered without becoming a spoofable argument —
   env-injected config, a launch-time token, or the tmux session name disponent
   already labels (design §3)? The last is appealing but couples binding to the
   local backend.
4. **The anchor for a fleet-wide user note.** Every `Message` anchors to a
   session (§4). An escalation is always about a worker, so that is fine — but a
   Manager note to the user *not* about any one worker has no anchor. Do we ever
   need one, or is "the user hears from the Manager only about workers" an
   acceptable limit? Leaning acceptable; the Manager talks to its own user
   outside disponent.
5. **Fan-out atomicity.** `send` to N workers mints N rows. If the process dies
   mid-fan-out (memory-only, no mirror flush), a subset is delivered. Acceptable
   (at-least-once per recipient is independent), or does pm need a fan-out id to
   detect partials? Leaning acceptable.
