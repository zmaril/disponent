# disponent — supervisor↔worker communication

*Draft 1, 2026-07-08. A design note, sibling to [design.md](./design.md) and
[ai-dispatch-comparison.md](./ai-dispatch-comparison.md). It proposes two new
flows — a targeted directive channel down to workers, and a question channel up
from workers — grounded in the op surface that ships today (`send`, `events`,
`wait`, role-scoped MCP). Nothing here changes `schema/disponent.tsp` or any
generated file yet; it is the argument for the schema change, written first.*

Read [design.md](./design.md) for the model this builds on: environments are the
source of truth (§3), sessions run until reaped (§5), events carry a
`fidelity` grade (§7), and the MCP surface is **role-scoped** — supervisors get
the full surface, workers observe only, so a dispatched agent cannot recurse
(§10). This note is careful about that last invariant; it is the thing most at
risk from widening the worker surface.

## 1. Motivation — the two flows

Two things the MVP topology (design §14: a supervisor Claude reaching a fleet of
worker VMs over stdio MCP) cannot express today, both about *communication
between the supervisor and running workers*:

**(a) Targeted preference fan-out (down).** The user tells the supervisor a
preference mid-flight — "use `bun`, not `npm`, everywhere" — and the supervisor
wants to hand that to a **selected subset** of the running workers (the ones
touching package config), not broadcast it to the whole fleet and not restart
anyone. Each selected worker picks the preference up **through its own MCP
surface**, on a subscription/inbox it polls or waits on. Today the only
downward channel is `send(sessionUid, input)` — one session, and it is a
supervisor-only write that shoves raw text at a prompt. There is no "these
five sessions," no durable record the worker can pull on its own cadence, and
nothing a worker is allowed to read.

**(b) Question escalation (up).** A worker hits an ambiguity it cannot resolve —
"the migration will drop a column with data in it; proceed?" — and needs to
surface the question **upward**. The supervisor then decides, per question,
whether to **answer it itself** (it knows the convention) or **forward it to the
human** (only the user can authorize dropping data). This is a
*may-or-may-not* escalation: most questions die at the supervisor, some reach
the user. Today a worker has no upward channel at all — the worker-role server
exposes `session`, `sessions`, `events`, `offerings` and nothing else, all
read-only. A worker cannot say anything; it can only be watched.

Both flows are **control-plane messaging between principals**, not observation
of an environment. That distinction drives every decision below, especially
§5 (fidelity) and §9 (reconciliation), because it makes these the first
disponent entities the *environment does not own*.

## 2. What exists today (grounding)

Quoting the shipped surface so the additions are honest deltas, not inventions.

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
  `EventPayload` is the tagged union (state / message / toolCall / toolResult /
  log / usage / artifact / raw).
- **The role gate — the load-bearing detail.** Worker observe-only is enforced
  in exactly one place, `crates/disponent-cli/src/mcp_server.rs::tools_for`:

  ```rust
  // The generated manifest, gated by role: a worker sees only the tools whose
  // manifest entry carries readOnlyHint (observe, never act).
  .filter(|t| role == Role::Supervisor
              || t["annotations"]["readOnlyHint"] == json!(true))
  ```

  `readOnlyHint` comes from the `@readonly` decorator in the tsp, lowered by
  fluessig's MCP projection into `annotations` in `TOOLS_JSON`
  (`crates/disponent-core/src/mcp_generated.rs`). So today the worker surface is
  **exactly the read-only tools**, computed from one boolean per op. There is no
  third state. This is clean, and it is the constraint §3 has to solve: a
  worker-*writable* op (post a question, ack a message) is a write, so under
  today's gate it is invisible to workers. We must not fix that by making
  `ask`/`ack` `@readonly` (a lie — they mutate the ledger), nor by handing
  workers the supervisor surface. We need a **new, narrower** worker-write edge.

## 3. The role gate, refined — a third op annotation

The single most important change, and the smallest. Add one op-level decorator,
`@worker`, meaning *"projected into the worker surface even though it is not
read-only."* It lowers (fluessig MCP projection, design §13.2) to a new
annotation `workerHint: true` alongside the existing `readOnlyHint` /
`destructiveHint`. The gate becomes:

```rust
.filter(|t| role == Role::Supervisor
            || t["annotations"]["readOnlyHint"] == json!(true)
            || t["annotations"]["workerHint"]   == json!(true))
```

Everything the invariant depends on now reads off the annotations, declaratively:

| op | `readOnly` | `worker` | `destructive` | worker sees it? |
|---|---|---|---|---|
| `dispatch` | | | | **no** |
| `send` | | | | **no** |
| `notify` (new, §4) | | | | **no** |
| `cancel`, `reap` | | | ✓ | **no** |
| `answer`, `escalate` (new) | | | | **no** |
| `sessions`, `events`, `offerings` | ✓ | | | yes (observe) |
| `questions` (new, §4) | ✓ | | | yes (observe) |
| `inbox` (new, §4) | ✓ | | | yes (observe) |
| `ack` (new, §4) | | ✓ | | yes (write, narrow) |
| `ask` (new, §4) | | ✓ | | yes (write, narrow) |

`@worker` is applied to **exactly two** new write ops (`ask`, `ack`), and to
nothing else — ever. `dispatch`, `send`, `notify`, `cancel`, `reap`, `answer`,
`escalate` stay off the worker surface by construction, so §7's security posture
falls straight out of the table. The no-recursion invariant is preserved *and
made auditable*: the property "workers cannot spawn or drive other sessions" is
now "no dispatch/send/notify/cancel/reap op carries `@worker`," checkable by
reading the schema.

## 4. Data model

Three new entities, sketched in the `schema/disponent.tsp` conventions
(`@entity @name(...)`, `@key`, `@fk(#[...])`, doc comments that flow to the docs
site and the MCP tool descriptions). These are **control-plane** rows; unlike
`Session`, no environment backs them (see §9).

```typespec
// ── scalars ──
/** Disponent-minted message id (UUIDv7). */
scalar MessageId extends string;
/** Disponent-minted question id (UUIDv7). */
scalar QuestionId extends string;

// ── enums (wire values are the stored strings) ──
enum MessageKind {
  directive,   // a supervisor preference/instruction pushed to a worker inbox
  answer,      // the reply to a worker question, delivered back into its inbox
}
enum QuestionStatus {
  open,        // asked, not yet acted on
  answered,    // the supervisor answered it directly
  escalated,   // forwarded to the human; awaiting a human answer
  withdrawn,   // the asking session ended/was reaped before resolution
}

/** A control-plane message aimed at one session's inbox. The supervisor mints
 * these (directives, and answers to questions); a worker only ever reads and
 * acks them. Disponent owns these rows — no environment backs them (§9). */
@entity
@name("messages")
model Message {
  @key id: MessageId;
  createdAt: utcDateTime;
  kind: MessageKind;

  /** The recipient. A message targets exactly one session; fan-out to a
   * selection (§6) mints one Message per selected session, so acks and
   * delivery are per-recipient. */
  @fk(#["session_uid"]) session: Session;

  /** The payload — free-form text, like the brief. Structure is the consumer's. */
  body: string;

  /** For an `answer`, the question it closes; null for a `directive`. */
  @fk(#["in_reply_to"]) inReplyTo?: Question;

  /** Monotonic per-session inbox position — the worker's read cursor rides
   * this, exactly like Event.idx rides the session timeline. */
  seq: int64;

  /** Set when the worker acks (§5). Null = unread/unacked. */
  ackedAt?: utcDateTime;
}

/** A question raised by a worker and surfaced up to the supervisor. The worker
 * mints it (`ask`, §4); the supervisor resolves it (`answer` / `escalate`). */
@entity
@name("questions")
model Question {
  @key id: QuestionId;
  askedAt: utcDateTime;

  /** The session that asked. Bound by the worker-role server (§7), never a
   * caller-supplied uid. */
  @fk(#["session_uid"]) session: Session;

  body: string;
  status: QuestionStatus = QuestionStatus.open;

  /** The supervisor's or human's answer text; null while open/escalated. */
  answer?: string;
  answeredAt?: utcDateTime;

  /** Who answered: "supervisor" or "human" — provenance for the escalation
   * audit trail, not a principal table (secrets/identities stay out, design §9). */
  answeredBy?: string;
}
```

No separate `Inbox` or `Subscription` entity is needed: an inbox **is** the
query `messages WHERE session_uid = self ORDER BY seq` (to-manys are queries in
this schema — the same choice `Event`/`Artifact`/`Usage` already make, keyed by
`session` FK + a position). A "subscription" is just a worker holding a read
cursor over that query, which is the `inbox` op (§4 ops). This keeps the model
as flat as the rest of the ledger.

### New event-union variants

So both flows show up on the one feed pm already renders (design §7), the
control-plane actions project onto the session timeline as new `EventKind`s and
`EventPayload` variants:

```typespec
enum EventKind {
  state, message, tool_call, tool_result, log, usage, artifact, raw,
  inbound,   // a directive/answer landed in this session's inbox
  question,  // this session asked a question
  resolution,// a question this session asked was answered or escalated
}

union EventPayload {
  // … existing variants …
  inbound: InboundRef,      // { messageId, kind }
  question: QuestionRef,    // { questionId }
  resolution: ResolutionRef,// { questionId, status, answeredBy? }
}
model InboundRef { messageId: MessageId; kind: MessageKind; }
model QuestionRef { questionId: QuestionId; }
model ResolutionRef { questionId: QuestionId; status: QuestionStatus; answeredBy?: string; }
```

The payload is a **pointer** (an id), matching `ArtifactRef { artifactIdx }` —
the row is the record, the event is the timeline breadcrumb. Fidelity for all
three is discussed in §5.

### Op surface

Supervisor ops (never `@worker`, so never on the worker surface):

```typespec
/** Fan a directive out to a selected subset of sessions (§6). Mints one
 * Message per resolved recipient and returns them. Supervisor-only: it writes
 * to inboxes, so it must never carry @worker. */
notify(target: NotifyTarget, body: string): Message[];

/** The open/escalated questions awaiting a decision. Read-only, so workers can
 * see it too — but a worker only ever sees its OWN (§7 scopes the query). */
@readonly questions(filter?: QuestionFilter): Question[];

/** Answer a question directly. Sets status=answered and mints an `answer`
 * Message into the asking session's inbox, closing the loop. Supervisor-only. */
answer(questionId: QuestionId, body: string): Question;

/** Forward a question to the human instead of answering (may-or-may-not
 * escalation, §8). Sets status=escalated; the human's later answer arrives via
 * `answer`. Supervisor-only. */
escalate(questionId: QuestionId, note?: string): Question;
```

Worker ops (`inbox`/`questions` read-only; `ask`/`ack` carry `@worker`):

```typespec
/** This session's inbox, as a cursor stream — same shape as `events`
 * (afterSeq/limit lower to the MCP cursor tool, design §13.2). Self-scoped:
 * NO sessionUid argument. The worker-role server answers for its bound session
 * only (§7). Read-only → visible to workers by the existing gate. */
@readonly @stream inbox(options?: InboxOptions): Message;

/** Acknowledge a delivered message (stamps ackedAt; idempotent on messageId).
 * A write, but narrow and self-scoped → @worker puts it on the worker surface
 * without opening dispatch/send. */
@worker ack(messageId: MessageId): void;

/** Raise a question to the supervisor. Mints a Question for the bound session
 * and a `question` event on its timeline. @worker, self-scoped — a worker can
 * only ask AS itself, never post for another session. */
@worker ask(body: string): Question;
```

```typespec
model NotifyTarget {
  /** Explicit recipients — the primary selection primitive (§6). */
  sessions?: SessionUid[];
  /** Sugar: a label predicate resolved to a session set at send time (§6). */
  labelsMatch?: Json;
}
model QuestionFilter { status?: QuestionStatus; }
model InboxOptions { afterSeq?: int64; kinds?: MessageKind[]; }
```

`notify`/`answer`/`escalate` take an explicit `sessionUid`/`questionId` because
the **supervisor** may address any session it dispatched — that is its whole
job. `inbox`/`ack`/`ask` take **none**, because a worker may address only
itself; the identity comes from the server binding, not the wire (§7).

## 5. Delivery and ack semantics

Delivery is **pull-based**, matching disponent's non-injection stance (design
§7: "Disponent does not interpolate"). `notify` does **not** push text into the
worker's tmux/PTY the way `send` does; it writes a `Message` row and lets the
worker read it on the worker's own cadence, through `inbox`. The worker chooses
when to look — polling `inbox(afterSeq)` or, later, blocking on it. This keeps
the worker a leaf that *reads a surface*, never a target disponent drives, and
it means there is no env-side artifact to keep in sync (§9).

- **Ordering.** Per-session FIFO by `Message.seq`, a monotonic per-inbox
  counter minted exactly like `Event.idx`. `inbox(afterSeq)` returns messages
  in `seq` order after the cursor. No cross-session ordering is promised: a
  `notify` fan-out to five sessions is five independent inbox appends.
- **At-least-once, with idempotent ack.** The cursor is the delivery guarantee:
  a worker that reconnects and re-reads from a stale `afterSeq` sees a message
  again. `ack(messageId)` is idempotent (a second ack is a no-op on an already
  stamped row), so re-delivery is safe. We do **not** promise exactly-once —
  that would require the worker to durably persist its cursor, which disponent
  cannot enforce inside someone else's agent. Honest guarantee: *every message
  is readable until acked; acking is safe to repeat.*
- **Ack vs read.** Two distinct positions. The **read cursor** (`afterSeq`) is
  the worker's private progress marker; `ackedAt` is a **ledger-visible**
  acknowledgement the supervisor can see (`messages WHERE ackedAt IS NULL`
  answers "who hasn't picked this up"). A worker may read without acking (it saw
  the directive) and ack later (it applied it) — the two-marker split lets pm
  distinguish "delivered" from "acknowledged," which matters for the fan-out UI.
- **Wait-based delivery (later).** The blocking analogue of `inbox` is a
  `waitInbox(timeoutSecs)`, hand-written per binding exactly like the existing
  `@manual wait(sessionUid, timeoutSecs)` — same event-loop/GVL concerns. It is
  a v1.1 nicety; polling `inbox` on the observer cadence covers the MVP.

## 6. Selection targeting

The supervisor must name "these workers." Three candidate handles exist:

1. **By session uid** — `Session.uid`, the ledger's own key. Precise, unambiguous.
2. **By label** — `Dispatch.labels: Json` is already the consumer's opaque tag
   bag; a predicate over it ("touches package.json") is the natural selector.
3. **By capability grade** — an env's `CapabilityKind` set. Too coarse for
   *worker* selection (it describes the environment, not the task), and it would
   conflate "can be sent to" with "should hear this preference." Rejected as a
   selection mechanism; capability still gates *whether* `notify` can reach a
   session at all (§7).

**Recommendation: session uid is the primitive; label match is sugar over it.**
`NotifyTarget.sessions` is the ground truth the ledger records — one `Message`
per uid, so acks and the audit trail are always per-recipient. `labelsMatch` is
resolved to a uid set **at send time** (`sessions WHERE labels ⊇ predicate AND
state ∈ live`) and then behaves identically. This means the durable record is
always the concrete recipient list, never a live predicate that would re-evaluate
as the fleet changes — a supervisor sends to who was live *then*, and that set is
frozen into the `Message` rows. Late-joining sessions do not retroactively
receive an old directive; that is the honest and less surprising semantics.

## 7. Security posture — preserving no-recursion

The invariant (design §10): *humans and supervisor agents dispatch; dispatched
agents are leaf nodes.* Widening the worker surface must not dent it. What a
compromised or adversarial worker can and cannot do, after this change:

**CAN** (its whole surface): read its own sessions/events/offerings (today);
read **its own** inbox (`inbox`); ack its own messages (`ack`); post questions
**as itself** (`ask`); list its own open questions (`questions`).

**CANNOT**: `dispatch` (spawn any session) — not on the worker surface;
`send` or `notify` (push to *any* session, including a sibling or itself) —
supervisor-only, the "send to other workers" hole stays closed; `cancel` /
`reap` / `resume` (drive lifecycle); `answer` / `escalate` (resolve questions —
only the supervisor decides). A worker cannot manufacture a directive into
another worker's inbox, because minting a `directive` Message is `notify`, which
it does not have.

Two enforcement layers, both structural, neither trusting agent good behavior:

1. **Tool projection (existing, extended).** The worker-role server projects
   only `readOnlyHint || workerHint` tools (§3). `notify`/`answer`/`escalate`/
   `dispatch`/`send`/`cancel`/`reap` carry neither annotation, so they are
   physically absent from the worker's tool list — the same mechanism that keeps
   `dispatch` off the worker surface today, extended to the new writes.
2. **Server-side session binding (new requirement).** The worker ops are
   **self-scoped**: they take no `sessionUid`. The worker-role server is bound to
   one session identity at launch — add `boundSession?: SessionUid` to
   `McpOptions` (set when the env wires the worker's MCP endpoint, alongside
   `role: worker`). `inbox`/`ack`/`ask`/`questions` resolve "self" from
   `boundSession`, never from arguments. Without this, a worker handed a
   worker-role endpoint could pass a *sibling's* uid and read that inbox or post
   as it. Binding closes that; a worker literally cannot name another session.

So the widened surface adds *inbox-read + question-post, scoped to self* and
nothing more. The property to test in CI (a natural straitjacket check): the set
of `@worker`-annotated ops is exactly `{ask, ack}`, and no op is both `@worker`
and one of {`dispatch`, `send`, `notify`, `cancel`, `reap`}.

## 8. Escalation to the human — building on pm's feed + composer

pm#158 built, on top of disponent's `send`/`events` ops, a **send-composer** (a
box that calls `send` into a session) and an **event feed** (a live render of
`events`). The escalation flow rides both, adding no new pm↔disponent transport:

1. A worker `ask`s. This mints a `Question` and a `question` event on the
   session timeline. pm's **event feed already tails `events`**, so the question
   surfaces in the feed the moment it lands — no new subscription, just a new
   `EventKind` the feed learns to render (as a highlighted "needs a decision"
   card rather than a log line).
2. The supervisor (the Claude driving pm, or the human at the pm UI) sees it and
   picks one of two affordances the composer grows:
   - **Answer** → calls `answer(questionId, body)`. Under the hood this mints an
     `answer` Message back into the worker's inbox (`kind: answer, inReplyTo`),
     so the loop closes on the same inbox channel the worker is already reading.
     The composer for an answer is the send-composer **retargeted** from "raw
     text via `send`" to "an answer via `answer`" — same box, different op, so
     the answer is a first-class ledger row, not an untracked prompt injection.
   - **Escalate** → calls `escalate(questionId, note?)`, setting
     `status=escalated`. In pm this is the question **leaving the supervisor's
     lane and entering the human's**: it moves to a "For you" queue in the UI
     (still just a filtered view of `questions WHERE status=escalated`). When the
     human answers there, pm calls the same `answer` op with
     `answeredBy: "human"`, and the worker's inbox receives it identically. The
     worker never knows or cares whether a machine or a person answered — the
     escalation is entirely a supervisor/UI concern, which is exactly where
     design §7 says truth-judgment belongs.

The may-or-may-not nature is honest: `escalate` is a **distinct op from
`answer`**, so the ledger records *whether* a question was escalated, and
`answeredBy` records *who* ultimately answered. pm gets a clean audit of which
questions reached the human without disponent guessing.

## 9. Persistence in the ledger and reconciliation

Messages, questions, and inbox positions live in the memory ledger and mirror to
SQLite through `driverPlan()` exactly like every other entity — three new tables
in the generated schema, upserted by the same thin executors (design §9). That
part is mechanical.

The **honest tension** is with design §3's load-bearing idea, *environments are
the source of truth.* Sessions reconcile because the env owns them (a tmux
session, a VM). **Messages and questions have no env behind them** — disponent
mints them, disponent *is* their source of truth. They are the first
ledger-owned control-plane entities in the system. Consequences, stated plainly:

- **`reconcile()` does not touch them.** There is nothing in an environment to
  re-adopt an inbox from. Reconcile still re-adopts sessions; the control-plane
  rows simply persist across it, keyed by session uid. When a session goes
  `lost` and later reconciles back, its inbox and open questions are still
  attached — a genuine upside of disponent owning them.
- **Durability is the SQLite mirror, not env reality.** With the default sink
  on, messages/questions survive a supervisor restart. **Memory-only mode loses
  them** on exit — the same trade design §3 already names for streamed events
  ("run memory-only and streamed events are gone"). We do not pretend
  otherwise; a directive sent in a memory-only session is as durable as an
  event in one.
- **A reaped session's control-plane rows.** `reap()` archives the session;
  its messages/questions archive with it. Open questions on a reaped session
  transition to `withdrawn` (the asker is gone), so the supervisor's
  `questions` list does not accrete answerable-but-orphaned entries.

Because these rows carry no env handle, they never desync from an environment —
there is no scrape, no derived reconstruction. Which is the cleanest possible
setup for their fidelity grade (§5's cousin):

**Fidelity of the new events.** `inbound`, `question`, and `resolution` events
are all **`exact`**. They are not observations of an environment inferred from a
transcript (`derived`) or a tmux capture (`scraped`); they are records of
disponent's own first-party API calls — a `notify`, an `ask`, an `answer`
happened, in disponent, with a row to prove it. Grading them `exact` is the
truthful call precisely *because* no environment mediates them. (Contrast: if a
future backend tried to *detect* that a worker had read a directive by scraping
its terminal for a quote of the text, that inference would be `derived` — but
the `ack` op makes that unnecessary, keeping the whole channel `exact`.)

## 10. Phased implementation

Ordered smallest-first; the MVP is the minimum that delivers **both** flows end
to end.

**MVP — both flows, uid-targeted, poll-based.**
- Schema: `Message` + `Question` entities; the three new `EventKind`s /
  `EventPayload` variants; `MessageKind` / `QuestionStatus` enums. Regen.
- The `@worker` decorator + fluessig's `workerHint` lowering (design §13.2),
  and the one-line `tools_for` gate extension (§3).
- Ops: `notify` (sessions-list target only — no label sugar yet), `inbox`
  (poll, self-scoped), `ack`, `ask`, `questions`, `answer`, `escalate`.
- `McpOptions.boundSession` + worker-role self-scoping (§7).
- pm renders the three new events and grows the answer/escalate affordances on
  the pm#158 composer (§8).

This is enough for: user → supervisor → `notify(sessions=[…])` → worker `inbox`
→ `ack`; and worker `ask` → feed → `answer` **or** `escalate` → human → `answer`.

**v1.1 — ergonomics.**
- `labelsMatch` selection sugar (§6), resolved to a uid set at send time.
- `waitInbox(timeoutSecs)` `@manual`, the blocking analogue of `inbox` (§5).
- `answeredBy` provenance surfaced in pm's audit view.

**Later / maybe.**
- Push delivery (inject a directive into the agent's context) — deliberately
  deferred; it breaks the pull/leaf-node model (§5) and needs a per-backend
  `interact` capability. Keep it an open question (§12), not a plan.
- Capability-graded targeting, if a second consumer ever asks (§6 rejects it as
  primary).

**Out (deliberately).** Threaded conversations (a question is one round-trip,
not a chat); worker→worker messaging (the no-recursion invariant forbids it,
§7); structured message bodies (strings, like briefs — structure is the
consumer's, design §4); message TTL/expiry beyond the reap-driven `withdrawn`.

## 11. Two worked examples

### (a) Preference fan-out — down

User, mid-flight, to the supervisor Claude: *"Actually, use bun everywhere, not
npm."* Three workers are live; two touch package config (labeled
`{area: "pkg"}`), one is writing docs.

```text
supervisor:  sessions({ state: "running" })
             → [wk-A {labels:{area:"pkg"}}, wk-B {labels:{area:"pkg"}}, wk-C {labels:{area:"docs"}}]
supervisor:  notify(
               { sessions: ["wk-A", "wk-B"] },     // MVP: explicit uids
               "Use bun, not npm, for all package operations."
             )
             → [ Message{id:m1, session:wk-A, kind:directive, seq:7},
                 Message{id:m2, session:wk-B, kind:directive, seq:4} ]
             // two rows, two timelines; wk-C untouched.
             // events: inbound@wk-A, inbound@wk-B  (fidelity: exact)

worker wk-A: inbox({ afterSeq: 6 })                // its own poll, its own cadence
             → [ Message{id:m1, kind:directive, body:"Use bun…", seq:7} ]
worker wk-A: ack("m1")                             // stamps ackedAt; @worker, self-scoped
             // Message m1.ackedAt set; supervisor's
             // `messages WHERE ackedAt IS NULL` no longer lists m1 for wk-A.
```

wk-B picks m2 up whenever it next polls; until it acks, the supervisor sees it as
delivered-not-acknowledged. No restart, no broadcast, wk-C never saw it.

### (b) Question escalation — up

Worker wk-B is mid-migration and unsure.

```text
worker wk-B: ask("Migration 0007 drops `users.legacy_id`, which still has data. Proceed?")
             → Question{ id:q1, session:wk-B, status:open }
             // event: question@wk-B  (fidelity: exact)

// pm's event feed (tailing `events`, per pm#158) renders q1 as a
// "needs a decision" card in wk-B's lane.

supervisor:  questions({ status: "open" })  → [ Question{id:q1, …} ]
             // The supervisor Claude judges: dropping data needs the human.
supervisor:  escalate("q1", "Data loss — user must confirm.")
             → Question{ id:q1, status:escalated }
             // event: resolution@wk-B { status:escalated }
             // pm moves q1 into the "For you" queue.

// Human, in pm, clicks Answer on q1:
pm (human):  answer("q1", "No — keep the column, backfill instead.")
             → Question{ id:q1, status:answered, answeredBy:"human" }
             // mints Message{ id:m3, session:wk-B, kind:answer, inReplyTo:q1, seq:5 }
             // events: resolution@wk-B { status:answered, answeredBy:"human" }, inbound@wk-B

worker wk-B: inbox({ afterSeq: 4 })
             → [ Message{id:m3, kind:answer, body:"No — keep the column…", inReplyTo:q1 } ]
worker wk-B: ack("m3")
             // wk-B proceeds without dropping the column. It never knew a human,
             // not the supervisor, answered — the escalation was invisible to it.
```

Had the supervisor known the convention, step 2 would have been
`answer("q1", "…")` directly — same inbox delivery, `answeredBy:"supervisor"`,
the human never paged. That is the may-or-may-not escalation, recorded honestly.

## 12. Open questions

1. **Push vs pull delivery.** MVP is pull (worker reads `inbox`). Some
   directives are urgent ("stop touching auth.rs"); is a pull-only channel ever
   enough, or does urgency force a push path — and if so, is it just `send`
   (raw text, supervisor already has it) rather than a new mechanism? Leaning
   pull-only; flagged because urgency is the obvious objection.
2. **Read cursor vs ack — is two markers one too many?** §5 splits read
   (`afterSeq`) from ack (`ackedAt`). If pm only ever needs "delivered," ack is
   dead weight; if it needs "applied," read is. Keep both until pm's UI says
   which it uses.
3. **Does `escalate` belong in disponent at all?** It is arguably pure pm — a
   view over `questions`. The case for keeping it in the core: `answeredBy`
   provenance and the escalated/answered distinction are ledger facts worth
   recording once, not per-consumer. Revisit if a second consumer models
   escalation differently.
4. **`boundSession` provenance.** §7 binds the worker server to a session uid at
   launch. How is that uid delivered to the worker env without becoming a
   spoofable argument — env-injected config, a launch-time token, the tmux
   session name disponent already labels (design §3)? The last is appealing
   (disponent already owns that label) but couples binding to the local backend.
5. **Fan-out atomicity.** `notify` to N sessions mints N rows. If the process
   dies mid-fan-out (memory-only, no mirror flush), a subset is delivered. Is
   partial fan-out acceptable (yes, at-least-once per recipient is independent),
   or does pm need a fan-out id to detect partials? Leaning acceptable.
6. **Question fidelity if a backend ever infers one.** §9 grades questions
   `exact` because `ask` is a first-party call. If a future backend tried to
   *detect* an agent asking a question by scraping its output (an agent that
   doesn't call `ask` but prints "should I proceed?"), that would be a `derived`
   question with no `QuestionId` the worker can be answered through. Out of scope
   now; noting it so the `exact`-only assumption is a recorded choice.
