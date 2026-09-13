---
title: Member inbox model — crew members as durable identities woken by an inbox, with peer DM as envelopes
status: draft
author: CrysisDeu
created: 2026-09-13
last-audited: 2026-09-13
audited-at: c0307ef08
doc-pr: 10528
revision: 2
implementation-prs: [10535]
tracking-issues: [10527, 10499, 10539]
supersedes: []
superseded-by: []
---

# RFC: Member inbox model — crew members as durable identities woken by an inbox, with peer DM as envelopes

> Companion to `docs/system-specs/modules/session-control.md` (today's member fence),
> `docs/request-for-change/rfc-perpetual-agent.md` (wake authority must live outside
> the agent) and the member→human escalation envelope of
> [#8613](https://github.com/kirodotdev/KiroCrew/pull/8613). Revision 1 of this
> document proposed a narrow allow so members could `session_send` each other; the
> owner's decision after the design discussion is to change the model instead. That
> earlier proposal is recorded under *Alternatives considered*.

- Status: draft
- Author: CrysisDeu
- Code baseline: origin/main `c0307ef08` (2026-09-13). Every symbol named below was read in that tree.
- Tracking: [#10527](https://github.com/kirodotdev/KiroCrew/issues/10527) (members cannot DM each other), [#10499](https://github.com/kirodotdev/KiroCrew/issues/10499) (loops dead after restart).

## Summary

Today a crew member **is a session**: a long-lived chat slot `member-<slug>` whose
transcript is the model's context, where every stimulus — the human typing, a cron
tick, a nudge cycle, another session's `session_send`, a worker's completion — is
injected as a user-role turn, and whose "heartbeat" is a loop hung on the slot. The
model is simple and it is why members break in the ways they broke this week.

This RFC moves members to an **inbox model**:

- a member is a **durable identity** (slug, agent, permissions), not a slot;
- stimuli arrive as **typed envelopes** in an append-only **inbox** on disk;
- a **scheduler** per member wakes it when the inbox is non-empty, on a timer, or on a hook;
- each **wake** is a bounded turn in an ephemeral execution context bound to the
  identity: it drains the inbox, reads the **ledger**, acts, writes the ledger and
  the **outbox**, and ends;
- the DM thread the human reads on the Members page is a **projection** of inbox
  and outbox, not the model's context;
- peer DM becomes `peer_send(to, body)` producing a `peer_dm` envelope with a causal
  hop count, a pair rate limit and a human-gated reset, mirrored into both projections.

The change replaces three ad-hoc wake mechanisms (autonudge loops on member slots,
member cron jobs, `session_send` queueing) with one scheduler that survives a
gateway restart because its state is the inbox, not a task in memory.

## Motivation — evidence from one week

All items are from the same deployment during 2026-09-08 → 2026-09-13; identifiers
are sanitized.

1. **A gateway restart killed every loop.** After one restart all 29 persisted
   AutoNudge / babysit loops were `active: false` with no `stopped_reason`; the
   scheduler did not resume them and re-arming was refused as a phantom "manual"
   stop ([#10499](https://github.com/kirodotdev/KiroCrew/issues/10499)). Members
   that relied on those loops for their heartbeat were silently dead until a
   person noticed. Root cause is structural: the loop's liveness *is* a task in the
   gateway process, so the process dying is the loop dying.
2. **Member threads cannot host a loop, so members run shadow sessions.**
   `autonudge_authz` refuses automation armed from outside a member-mode slot (by
   design — only a self-arm is admitted), and in practice `monitor_start` from a
   member thread returned "requested" while nothing was armed. The workaround that
   emerged is a **proxy session** per member whose only job is to be a heartbeat and
   write a patrol log the member reads on demand. Every member now has a second
   session, invisible in the roster, that exists because the first one cannot wake
   itself reliably.
3. **A transport error took a member's context with it.** One upstream stream drop
   ended six in-flight worker turns at once. The member that had dispatched them
   was mid-turn too; its context — what it had asked, what it was waiting for — was
   lost with the turn and had to be rebuilt from transcripts by hand.
4. **Members cannot hand each other work.** The conductor member tried to pass the
   day's first task to the autofix member and was refused `not_creator` by the
   creator fence in `authorize_target`; the owner copy-pasted the task between the
   two threads ([#10527](https://github.com/kirodotdev/KiroCrew/issues/10527)).
5. **One transcript carries everything.** A member's thread interleaves the owner's
   questions, "nothing new" patrol cycles, worker completion envelopes and cron
   notifications. The model's context is that transcript, so it survives only by
   compaction, and compaction is where the week's state gets lost.
6. **Provenance is a text prefix.** `session_send` prepends
   `[sent by session <key> via session_send]`; cron rows carry `[Cron notification …]`;
   nudges carry `[auto-nudge cycle N]`. The model is asked to trust the prefix to
   tell a colleague from the owner. Nothing structural distinguishes them at the
   point where the model decides whether a message counts as an instruction.

Each of these is a symptom of the same coupling: **the member's identity, state,
context, mailbox and heartbeat are all the same object — a live chat slot.**

## Verified facts this design rests on

- The refusal in item 4 is not a member-specific rule. It is the generic creator
  fence at the tail of `authorize_target` (`src/kiro_crew/dashboard/session_control.py`):
  `_caller_is_ownership_fenced` returns `True` for every member caller, and a member
  DM slot is minted by `api_member_thread` (`dashboard/handlers/members.py`) through
  `get_or_create_slot` with an empty `_created_by`, so it can never equal a peer's key.
- `send_to_target` (same module) has no rate limit, no hop count and no steer path.
  It builds `_SEND_PROVENANCE + sanitize_outbound(body)` and calls
  `_ChatSlot.enqueue_or_run_prompt` (`dashboard/state.py`): idle target → a user row
  and a new turn; busy target → `queue_append`, drained by `_start_next_queued_turn`
  (`dashboard/chat_runner.py`) after the running turn, re-validated by
  `_drop_stale_admissions`. One slot runs one turn at a time (`_ChatSlot.running`).
- Row roles already distinguish some provenance at the *transcript* layer — `inject`
  for cron and recovery, `subagent` for completions, `nudge` for loop cycles, `user`
  for both humans and `session_send` — but the model receives all of them as turns.
- Member auto-grants (`_MEMBER_DASHBOARD_GRANTS`, `src/kiro_crew/agent.py`) include
  `session_send`/`session_stop` because the creator fence confines them to worker
  sessions the member opened. The conductor set (`_CONDUCTOR_DASHBOARD_GRANTS`)
  withholds `session_send`: "the server-side gates bound WHICH target is reachable;
  nothing bounds WHAT is sent."
- Durable per-session state already exists in two shapes: `kiro_crew.session_ledger`
  (goal / phase / next / tried / artifacts, re-injected into monitor cycles) and the
  conductor work ledger `kiro_crew.work_ledger` (`ConductorRecord`, `WorkItem`,
  `WorkEvent`; the Issue Radar crew ledger is built on it). Both are written by the
  agent and read back as a snapshot — exactly the "state outside the context" this
  RFC generalizes.
- `rfc-perpetual-agent.md` already establishes that wake authority must live outside
  the agent and that an agent-self-reported wake time is untrusted.

## Prior art (brief)

The full six-system survey from revision 1 is summarized here; URLs are in the
comparison table.

- **Claude Code agent teams**: every teammate has an on-disk JSON inbox; messages are
  delivered automatically and read between tool calls, or start a new turn when the
  teammate is idle; the receiver is told the message came from an agent and it never
  counts as user consent; per-sender rate limit, duplicate drop, 50-message cap.
  This is the closest shape to what is proposed.
- **OpenClaw `sessions_send`**: the injected prompt is tagged
  `[Inter-session message … isUser=false]` with structured provenance
  `inter_session`; a reply-back loop is bounded at a fixed 5 turns (cap 20);
  `agentToAgent.allow` must match on both sides.
- **Grok Bot**: named Bots addressed by `@Name`; "a Bot can send an asynchronous
  message to another Bot. The receiving Bot **wakes**, handles the request, and can
  reply later"; a human DM pre-empts background work; the handoff is visible in the
  conversation. No hop bound documented.
- **Google A2A**: `message/send` returns a `Message` or a stateful `Task`
  (SUBMITTED → WORKING → INPUT_REQUIRED → COMPLETED); results by poll, SSE or
  webhook push; no loop protection in the spec.
- **OpenAI Agents SDK / CrewAI / AutoGen**: handoffs and delegation are
  synchronous within one run; `max_turns` / `max_iter` / termination conditions
  bound loops; not a durable-identity model.

| | Addressing | Delivery | Authz | Loop bound | Human view |
|---|---|---|---|---|---|
| [Claude Code teams](https://code.claude.com/docs/en/agent-teams) | name / agentId | inbox file, auto-delivered, idle → new turn | any teammate; receiver told it is an agent | per-sender rate, dedupe, 50-msg cap | per-agent panel |
| [OpenClaw](https://docs.openclaw.ai/concepts/session-tool) | sessionKey / label / agentId | `queued` or `steered`, `isUser=false` provenance | `agentToAgent.allow` both sides | 5-turn ping-pong, cap 20 | both transcripts + announce |
| [Grok Bot](https://docs.x.ai/grok-bot/chat-and-collaboration) | `@Name` | async wake, reply later | one owner's Bots | none documented | in-conversation handoff, *Needs attention* |
| [A2A](https://a2a-protocol.org/latest/specification/) | Agent Card | `message/send` → Message/Task; poll/SSE/webhook | card `securitySchemes` | none | `INPUT_REQUIRED` surfaced by client |
| **Kiro Crew today** | slot key | user-row injection; idle → turn, busy → queue | creator fence | none | one transcript, everything interleaved |
| **Kiro Crew proposed** | member slug | typed envelope in on-disk inbox; scheduler wakes | envelope kind + identity | causal hop + per-member budget + pair rate, human-gated | projection with kind badges, unread |

## The model

### Identity

A member is a record, not a slot: `slug`, bound `agent` template, permissions
(the member grant set; `peer_dm.accept` / `peer_dm.send` opt-outs), memory store.
The record already exists in the crew config and in `members.py` (`member_dir(slug)`,
the `dm.json` binding); what changes is that the slot is no longer the thing the
identity *is*. A slot is a transient projection host and, during a wake, a
transient execution context.

### Inbox

An append-only on-disk queue under `member_dir(slug)/inbox/`, one file per
envelope, written with the same atomic-write discipline as `dm.json`. Envelope:

```json
{
  "id": "env_01J…",
  "from": "user" | "member:<slug>" | "worker:<slot-key>" | "cron:<job-id>" | "hook:<hook-id>" | "system",
  "to": "member:<slug>",
  "kind": "user_dm" | "session_dm" | "peer_dm" | "worker_report" | "wake_timer" | "hook" | "escalation_reply" | "system",
  "hop": 0,
  "created_at": "2026-09-13T10:00:00Z",
  "body": "…",
  "refs": {"pair_id": "…", "in_reply_to": "env_…", "worker": "chat-…", "escalation_id": "…"},
  "attempts": 0,
  "acked_at": null
}
```

Kinds and their producers:

| kind | produced by | wake priority |
|---|---|---|
| `user_dm` | the human typing in the projection (dashboard or bound channel) | immediate |
| `session_dm` | another session's `session_send`, translated by the shim (not the owner) | normal |
| `peer_dm` | another member's `peer_send` | normal |
| `worker_report` | a worker session the member created finishing a turn (its assistant reply) or ending | normal |
| `wake_timer` | the member's own timer (replaces member cron jobs and self-armed loops) | normal, coalesced |
| `hook` | inbound webhook bound to the member | normal |
| `escalation_reply` | the owner answering an escalation card (#8613) | immediate |
| `system` | gateway events: restart notice, dead-letter notice, config change | normal |

Envelopes are immutable once written; acknowledgement is a separate marker
(`acked_at`, written by the wake runner at the end of a clean wake — never by a
tool the model calls). Nothing in the inbox is ever edited by the model.

### Outbox and projection

Every wake's reply is an **outbox row** under `member_dir(slug)/outbox/`, same
envelope shape with `from: "member:<slug>"`. The Members page thread is a
**projection**: the time-ordered merge of inbox and outbox rows for that member,
rendered with kind badges. The projection is what the human reads; it is *not*
what the model reads. The model reads the ledger snapshot plus the envelopes
drained in this wake (see *Wake*).

Consequences: the projection can carry patrol noise collapsed by default without
that noise ever entering the model's context; an unread badge is finally possible
(unread = outbox rows newer than the human's last read marker); and the thread
survives any number of gateway restarts because it is two directories on disk,
not a slot's JSONL that has to be rehydrated.

### Ledger

The member's state between wakes: `kiro_crew.session_ledger` shape (goal, phase,
`next`, tried/rejected, artifacts) extended with a **rolling summary** (a few
hundred tokens the member maintains about what it is doing and what it is waiting
for) and the worker roster it owns. Conductor-class members additionally keep
their `kiro_crew.work_ledger` records. The ledger is the single source of state;
the projection is history, the inbox is pending input. A wake that ends without
writing the ledger is a bug the runner reports as a `system` envelope.

### Scheduler

One scheduler per member, owned by the gateway, with three triggers:

1. **inbox non-empty** — `user_dm` and `escalation_reply` wake immediately; other
   kinds wake after a short coalescing window (default 5 s) so a burst of
   `worker_report`s drains as one wake;
2. **interval** — `wake_timer` envelopes are minted by the scheduler on the
   member's configured cadence (this is the member's cron, moved inside);
3. **hook** — an inbound webhook bound to the member mints a `hook` envelope.

Wakes are **serial per member** by default: one wake runs at a time, and a
trigger that fires during a wake is simply the next wake. That includes a
`user_dm`: a message the owner types while a wake is running does **not** steer or
preempt it — it is the next wake, started as soon as the running one ends (bounded
by `WAKE_WALL_SECS`, 600 s), with no coalescing delay. "Immediately" in the
trigger list therefore means *without the coalescing window*, and the latency the
owner sees is seconds when the member is idle and at most one wake's remaining
budget when it is busy. Whether the owner should be able to interrupt a running
wake is an open decision (below); M0–M2 ship the queue-only behaviour, which is
what the wake tests pin. Optional **lanes** let a
member declare that a kind (say `worker_report`) may be processed by a
concurrent wake with its own ledger section; laned wakes never share a ledger
write. The scheduler's own state is derivable from disk (unacked envelopes +
member config), so a restart rebuilds it by scanning `member_dir(*)/inbox/` —
there is no in-memory row to flip to `active: false`.

### Wake

A wake is a bounded turn in an **ephemeral execution context** bound to the
member identity for its duration:

- **Prompt** = member prompt (`_MEMBER_HOW_YOU_WORK`) + briefing + ledger snapshot
  + the drained envelopes rendered as a typed list (kind, from, hop, body). The
  envelope list is data with structure the model cannot confuse with an owner
  instruction; `user_dm` is the only kind the prompt frames as the owner speaking.
- **Tools**: the member grant set, minus `session_send`, plus two member tools:
  `outbox_send(body, refs?)` (a reply into the projection) and
  `peer_send(to, body, refs?)` (a `peer_dm` to another member). Acking is the
  runner's alone: it acks the whole drained batch when the turn ends cleanly, so an
  early ack cannot lose work when the turn then fails, and a model-issued ack would
  otherwise be a no-op the runner repeats. A model's free-form `refs` is stripped of
  the keys the gateway owns (`completed`, `in_reply_to`, `wake_key`, `delivery`,
  `pair_id`, …) before it is stored.
  `session_create` / `session_read_message` / `session_stop` remain for workers.
- **Budget**: a per-wake tool-call cap and wall-clock cap (config, defaults 40
  calls / 10 min). Hitting either ends the wake cleanly: the runner writes a
  `system` envelope ("wake ended at budget; N envelopes re-queued") and the
  drained-but-unacked envelopes redeliver on the next wake.
- **End**: the runner persists the ledger diff, acks, and discards the context.
  Nothing of the wake survives except ledger, outbox and SEL.

The execution context is a slot created with `mode="member-wake"`, `_created_by =
"member:<slug>"`, hidden from the sessions list, closed at end of wake; it reuses
the existing runner (`_run_chat`) and the existing member pin so nothing about
agent binding changes. Sixteen wakes of the same member reuse the same *identity*
and *ledger*, never the same slot.

### Configuration

```yaml
members:                       # per-member entries, keyed by slug
  radar:
    inbox_model: true          # opt this member in (default false)
    wake_interval_secs: 1800   # optional wake_timer cadence (floor 60)
    peer_dm: {accept: true, send: true}
member_peer_dm:                # crew-wide kill switch
  enabled: true
```

`members` and `member_peer_dm` are modelled top-level sections of `KiroCrewConfig`
(plain mappings, like `hooks`): the loader neither warns about them as unrecognized
nor coerces them, and every reader validates the shape at the point of use and
fails closed — an unreadable or malformed config leaves a member on the session
model and refuses `peer_send` with `peer_dm_config_degraded`. The numeric bounds
are **constants**, not config (`MAX_ATTEMPTS` 3, `WAKE_WALL_SECS` 600, hop cap 6,
12 unattended sends per member, 12 per pair per 10 minutes): no operator has
asked for other values, a config key is honoured forever, and the open decision
on their size is settled from SEL data first.

## Peer DM as envelopes

`peer_send(to="member-<slug>" | "<slug>", body, refs?)` — available in a member
wake, including the conductor's — does the following in one admission:

1. resolves `to` to a member identity (`member_slot_key`, `is_member_session_key`);
   unknown → `peer_dm_target_unknown` (404);
2. checks the global switch `member_peer_dm.enabled` → `peer_dm_disabled`
   (403), the receiver's `peer_dm.accept` and the sender's `peer_dm.send` →
   `peer_dm_opted_out` (403, reason names which side);
3. same workspace → else `peer_dm_workspace_mismatch` (403);
4. computes `hop` = 1 + max hop of the `peer_dm` envelopes this wake drained (0 if
   none); `hop > 6` (constant) → `peer_dm_hop_limit`
   (429) with reason text telling the sender to report to the owner instead;
5. per-member unattended budget: `peer_dm` envelopes sent by this member since the
   last `user_dm` in **its own** inbox, cap 12 (constant) →
   `peer_dm_budget_exhausted` (429);
6. pair rate limit, token bucket per unordered pair, default 12 per 10 min →
   `peer_dm_rate_limited` (429, `retry_after`);
7. `sanitize_outbound(body)`, size cap → `message_empty` / `message_too_long` (400);
8. writes the sender's outbox mirror (`delivery: pending`, carrying the whole
   envelope) and then the envelope to the receiver's inbox, then stamps the
   mirror `delivered` — the mirror is the send journal, not a rendering trick,
   and the scheduler's start reconciles any mirror a crash left `pending`
   (stamps it when the receiver has a trace of the id, otherwise completes the
   send from the journal, or marks it `failed`);
9. SEL `log_tool_invocation(tool_name="peer_send", metadata={from, to, hop, pair_id})`;
   every refusal above is `log_api_access(operation="peer_send", outcome="denied")`
   with the code.

**Resets.** Hop and budget are **human-gated**: a `user_dm` envelope in **either**
member's inbox resets the pair's hop chain, and a `user_dm` in a member's own
inbox resets that member's budget. Nothing else resets them — not elapsed time,
not `wake_timer`, `hook`, `worker_report` or `system` envelopes, and not a
`peer_dm`. A cron-driven member can therefore start a chain (`hop = 1`) but can
neither extend one past the cap nor refill its own budget; an unattended crew of N
members exchanges at most N × 12 messages and then goes silent until a person
types. That silence is the intended failure mode, and each member's next
`wake_timer` wake reports it to the owner via `outbox_send`.

**Why the conductor may `peer_send`.** The objection recorded in
`_CONDUCTOR_DASHBOARD_GRANTS` — nothing bounds what is sent — applied to
`session_send`, which runs arbitrary text as another session's user turn. A
`peer_dm` envelope is not a user turn: the receiving wake sees it as a typed
colleague message under its own grants, and those grants are the member set. The
worst case is a member wasting a bounded wake. That is what makes the grant safe
for the conductor too.

**Workers.** Workers created by a member stay ordinary sessions owned only by that
member (unchanged fence). What changes is how their output reaches the member: a
worker's assistant reply and its end-of-session are written as `worker_report`
envelopes into the creator's inbox, so members stop polling `session_read_message`
and stop needing a loop to notice a worker finished.

## Unifying the wake mechanisms

Today a member can be woken by (a) a self-armed autonudge / `monitor_start` loop,
(b) an agent cron job whose session key is the member slot, (c) a `session_send`
queued into the slot, and (d) a human typing. Under this RFC all four are
scheduler triggers producing envelopes: (a) and (b) become `wake_timer`, (c)
becomes `peer_dm` (from a member) or `session_dm` (from any other session that is
still allowed to reach a member — its own kind, labelled not-the-owner in the wake
prompt and unable to refill the budget; never `system`, which only the gateway
writes), (d) becomes `user_dm`. The scheduler is the only thing
that starts a member wake.

AutoNudge itself is **not** removed: it stays the mechanism for ordinary chat
sessions and for babysit loops the user arms on a PR. What ends is arming it on a
member slot, which was never fully admitted anyway (`autonudge_authz` admits only
self-arm) and which #10499 showed does not survive a restart. Member cron jobs
migrate to `wake_timer` cadences on the member record; the cron UI shows them as
the member's schedule, not as separate jobs.

## Failure semantics

| Failure | Today | Under this RFC |
|---|---|---|
| Gateway restart | loops die, `active: false` tombstones, member silent (#10499) | inbox and ledger are on disk; scheduler rebuilds from unacked envelopes and member cadences; pending wakes resume; no tombstone exists to block a re-arm |
| Wake crashes mid-turn (transport drop, OOM, exception) | turn lost with its context; queued rows may drain into a confused next turn | drained envelopes stay unacked; `attempts += 1`; they redeliver on the next wake with a `system` envelope naming the failure — except an envelope a `reply` row already names in `in_reply_to` (the wake replied, then died before its ack), which is acked without a second turn so the owner never reads the same answer twice; ledger is whatever the last clean wake wrote |
| Poison envelope (crashes every wake) | repeats until someone notices | after `MAX_ATTEMPTS` (3, constant) the envelope moves to `member-inbox/<slug>/dead-letter/`, a `system` envelope tells the member, and an escalation to the owner is filed through the #8613 path |
| Worker dies | member's loop, if alive, may notice on its next poll | `worker_report` envelope with `kind: ended, reason` arrives; member acts on the next wake |
| Member opted out between send and wake | n/a | envelope is dropped at drain with a `system` note in the sender's projection (same discipline as `_drop_stale_admissions`) |
| Hop / budget / rate cap hit | n/a | typed 429 to the sender; SEL row; the sender is told to report to the owner |

## Human UX

- The Members page thread becomes the **projection**: `user_dm` and the member's
  `outbox_send` replies render as the conversation; `peer_dm`, `worker_report`,
  `wake_timer` and `system` rows render as compact badged rows collapsed by
  default, expandable per kind. The human sees *that* the member talked to a peer
  and can read what was said, without it looking like the human said it.
- **Unread** is an outbox row newer than the human's last-read marker; the roster
  gets the badge it has been declaring "cannot occur here".
- Typing into the projection mints a `user_dm` and wakes the member immediately,
  so it still feels like a conversation. The reply is the wake's `outbox_send`.
- The rolling summary in the ledger gives continuity: a member answering "what
  did you do today" reads its own summary and outbox, not a compacted transcript.
- Other chat surfaces (ChatPane, SideChat, channel renderers) receive the
  projection as plain text with the kind in brackets; the structured card is
  Members-page-only in M1.

## Migration

- **M0 — inbox store, scheduler, wake runner, the three member tools, and the
  peer admission chain**, behind a per-member flag `members.<slug>.inbox_model:
  true`. The bounds ship WITH the first delivery path, never after it: `peer_send`
  (hop, budget, rate, opt-outs, typed refusals, SEL) is in M0 so no path exists in
  which a flagged pair can message unbounded. A flagged member's slot stops
  accepting turns directly: the human's message in the thread becomes a
  `user_dm` from `user` and wakes the member immediately; a wake runs in an
  ephemeral `member-<slug>.wake-<n>` slot; the reply is an outbox row mirrored
  into the thread. `session_send` **to** a flagged member's slot is translated at
  `send_to_target`, before the creator fence and only for flagged targets:
  a **member caller** goes through the same `peer_send` admission as the tool
  (so the `session_send` caller receives the peer-DM refusal codes —
  `peer_dm_target_unknown` 404, `peer_dm_self_target` 400, `peer_dm_disabled`
  403, `peer_dm_target_not_flagged` 409, `peer_dm_opted_out` 403,
  `peer_dm_hop_limit` 429, `peer_dm_budget_exhausted` 429,
  `peer_dm_rate_limited` 429, `peer_dm_config_degraded` 503 when the peer-DM
  config cannot be read or is malformed, `message_empty` / `message_too_long`
  400 — as `SessionControlError`s with those codes); **any other caller** is authorized by
  `authorize_target` exactly as today and its text becomes a `session_dm` from
  `session:<key>` — its own kind, so "another session, not the owner" is a
  property of the kind rather than of a field, and it never refills the budget
  (only a `user_dm` does). Translated sends are NEVER the `system`
  kind: `system` is reserved for gateway-originated notices (dead-letter, budget
  end), so an outside caller cannot dress its text as one. Tests: envelope
  round-trip; restart rebuild from disk; crash → redeliver with `attempts`;
  dead-letter after N + owner notification; serial wakes with a dirty re-run;
  wall-clock budget ends a wake cleanly; hop inherited across a relay through a
  third member and refused past the cap; budget exhausted and refilled only by a
  `user_dm` from `user` (not by `wake_timer` / `hook` / `worker_report` / `system`
  / a session's `user_dm` / elapsed time); pair rate shared both ways; mirror
  present in both stores; SEL on allow and every refusal; opt-out on either side;
  the translated non-member send is labelled not-the-owner.
- **M1 — projection UI and worker reports**: envelope kind badges, unread marker,
  `worker_report` production from worker sessions, the `member-wake` slot hidden
  on every surface, `GET /api/members/{slug}/projection`. Tests: both projections
  render the pair; role parity across chat surfaces; unread marker.
- **M2 — the conductor and autofix members on the inbox model** (in scope, owner
  decision). Flip `members.kirocrew-conductor.inbox_model: true` (and the autofix
  member) on a real pod; the conductor's patrol becomes a `wake_timer` (1200 s)
  instead of a proxy session's nudge loop; the worker sessions it creates report
  back as `worker_report` envelopes, so `session_read_message` polling stops; the
  owner's DM is a `user_dm` → immediate wake; briefing and ledger snapshot are
  injected per wake. Acceptance evidence on the pod: a timer-fired wake in which
  the conductor reads a `worker_report` and acts; `user_dm` to an idle member →
  wake within seconds (to a busy member: the next wake, right after the running
  one ends); conductor `peer_send` to the autofix member with the autofix
  projection showing it; a gateway restart with pending envelopes → drained
  after restart, no tombstone. The conductor ⇄ autofix hand-off from the
  motivation is the demo.
- **M3 — retire** proxy sessions, member-slot autonudge arming and member cron
  jobs; remove the `session_send` translation shim once no unflagged member remains;
  update `session-control.md`, the member protocol text and
  `require_memory_delegation`'s wording ("ask the owner or Crew coordinator to
  assign work to another member"). Tracked as a follow-up, not part of this
  series: [#10539](https://github.com/kirodotdev/KiroCrew/issues/10539).

## Alternatives considered

**Keep the session model and add a narrow allow (revision 1 of this RFC).** Admit
member→member `session_send` before the creator fence, add a hop count in row
meta, a per-member budget and a pair rate limit, mirror the exchange into both
transcripts. Rejected by the owner after discussion: it fixes the symptom in item
4 but leaves the coupling that produced items 1, 2, 3, 5 and 6 — the member is
still a slot, its heartbeat is still a loop on that slot, provenance is still a
text prefix the model has to trust, and the transcript is still the context. Two
review rounds on that draft each found a way the loop bound leaked (a timed reset;
an undefined "human turn"), which is itself evidence that bounding loops on top of
the injection model is fighting the model. The pieces that were right — causal
hop, human-gated reset, mirror, typed refusals — carry over here as envelope
semantics.

**A pure A2A-style external protocol.** Give every member an Agent Card and speak
`message/send` between them. Over-general for one gateway: it adds discovery,
auth schemes and a task state machine to solve intra-process delivery, and the
spec has no loop bound at all. The A2A bridge already exists as a channel for
*external* peers and stays there.

**Steer instead of queue.** Deliver peer messages into a running turn. Rejected:
a peer must not interrupt a peer. Whether the *human* may — a `user_dm` steering
or preempting a running wake instead of waiting for it — is open decision 6; in
M0–M2 a `user_dm` only skips the coalescing window, it never enters a running wake.

## Open decisions for the owner

1. **Serial vs laned wakes** as the default. Recommendation: serial, with lanes
   opt-in per kind for members that own many workers.
2. **Default hop cap**: 6 (recommended, sized to ask → clarify → confirm → report)
   vs 4.
3. **`wake_timer` cadence**: per-member config (recommended for M0) vs adaptive
   (back off while the inbox stays empty, tighten after a `user_dm`).
4. **Workers stay sessions**: yes (recommended) — a worker is a task, not an
   identity; it needs a transcript, not an inbox. Revisit only if workers start
   needing to be addressed by peers.
5. **Projection content**: outbox rows only (recommended — the wake's internal tool
   calls stay in SEL and the ephemeral slot's transcript, reachable from the row)
   vs the full wake transcript inline.
6. **Does a `user_dm` preempt a running wake?** Queue-only (recommended for
   M0–M2, and what ships: the owner's message is the next wake, started the moment
   the running one ends, worst case one `WAKE_WALL_SECS` later) vs *steer* (inject
   the owner's text into the running wake's turn, the way a dashboard steer reaches
   a running session — cheap to add on top of the queue, but the running wake's
   ledger write and reply then answer two prompts at once) vs *preempt* (stop the
   running wake, leave its envelopes pending for redelivery, start the owner's wake
   — the running work is lost and its attempt count rises, so a chatty owner can
   dead-letter a member's own work). This overlaps decision 1 only partially: lanes
   would let a `user_dm` run beside a long patrol wake without touching it, at the
   cost of two concurrent ledger writers.
