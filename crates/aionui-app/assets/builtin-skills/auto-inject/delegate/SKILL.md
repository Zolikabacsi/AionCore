---
name: delegate
description: Cross-team and cross-agent delegation - dispatch a task into a team's engagement or ask an agent by name; consolidated replies route back to your conversation.
---

# Cross-Team & Cross-Agent Delegation Skill

Hand work that lives OUTSIDE your own team to a **team** or an **agent** —
by **name** (or `assistant_id`) — and get the result back in your own
conversation. Teams are the primary targets: dispatching to "Marketing"
convenes that team's engagement for your project and its lead assigns the
Copywriter/Researcher roles internally (`delegate targets` first to see
exact names). A standalone assistant is still addressable when its
`allow_delegation = 1`.

## Rules

1. Use this only when the user explicitly asks for delegation, OR when your
   role description says "delegate to X for this kind of task". Never
   delegate on your own initiative outside an explicit dispatcher pattern.
2. Address targets by **team name**, **agent name** (case-insensitive prefix
   match) or `assistant_id`. Names are resolved server-side via `targets`;
   never pass a conversation id, slot id, or wildcard.
3. `to` must name exactly one team or agent. There is no broadcast.
4. Never pass, inline, export, echo, or set any `AIONUI_...` environment
   variable.
5. Commands must directly call `"$AIONUI_HELPER_BIN" delegate ...`. Pass
   payloads through stdin heredocs. Do not write payload JSON files to disk.
6. Team members: coordinate INSIDE your team with team tasks and
   `team send-message`. This skill is for work outside your team — you MAY
   dispatch to ANOTHER team's engagement (name the team in `to`) or to any
   delegating agent. Cross-team loops are stopped server-side: a
   `cycle_detected` or `depth_exceeded` response means stop (Rules 9/10).
7. A team dispatch lands as the engagement's **root task** (your
   `expected_output` becomes its success criteria) and runs on that team's
   board. Dispatching to the same team again REUSES the engagement — a
   follow-up is not a cycle. If your conversation has no project, the team's
   default engagement is used.
8. On `rate_limited`, STOP delivering. The two parties are spinning against
   each other. Tell the user, do not retry.
9. If the response is `cycle_detected`, STOP. The target is already on your
   delegation chain. Do not retry.
10. If `ask` returns `sync_timeout`, surface that to the user as "I asked
    X but didn't get a reply within N seconds".
11. Word results precisely. The ack only means the work was ACCEPTED —
    never claim anything completed or read. A team-dispatch ack carries the
    engagement's `root_task_id`; the team's consolidated result arrives later
    as a new user-role message in your conversation. End your turn; the
    result wakes you. Don't poll.

## Delegating a task (async)

```bash
"$AIONUI_HELPER_BIN" delegate dispatch <<'JSON'
{
  "to": "Marketing",
  "message": "Draft a 3-tweet launch thread for the new product.",
  "expected_output": "Three publish-ready tweets with an approval call from the team's reviewer.",
  "files": ["/abs/path/to/brief.md"]
}
JSON
```

The team runs the root task inside its own engagement; their lead
consolidates and replies — the answer arrives as a new user-role message in
YOUR conversation (because `reply_to` defaults to your conversation id).

## Asking a question (sync, assistant-only)

`ask` targets **assistants only** — a team name is not askable and reports
as not found. Use only when you cannot proceed without the answer. Your turn
is suspended until the target replies or the timeout fires.

```bash
"$AIONUI_HELPER_BIN" delegate ask <<'JSON'
{
  "to": "CMedO",
  "question": "Is this copy safe to publish?",
  "options": ["approve", "reject", "needs_legal_review"],
  "timeout_seconds": 120
}
JSON
```

The reply is in `data.reply` of the response envelope.

## Finding a target

```bash
"$AIONUI_HELPER_BIN" delegate targets <<'JSON'
{ "q": "marketing" }
JSON
```

Returns **teams** (`kind: "Team"`) and delegating assistants
(`kind: "Assistant"`, `allow_delegation = 1`), optionally filtered by
substring. When a specialist (e.g. "CMO") belongs to a team, prefer
dispatching to the TEAM — its lead assigns work internally.

## Replying to a delegated task

If a delegated task arrives with `reply_to` in its envelope block,
deliver your response by addressing your own `delegate dispatch` to the
team or agent named in `reply_to` (or to the conversation id in `reply_to`
via `session send-message` if the conversation id is what you have).

## Constraints

- Depth ceiling: 3 (CEO → team → member → leaf). If `depth` would
  exceed 3, the runtime returns `depth_exceeded`. Reply inline instead of
  delegating further when you see a high `depth` on the incoming envelope.
- Cycle protection: server-side. If you ever get `cycle_detected`, report
  to the user and stop delegating along that path.
- If your squad's preset makes a reviewer's VETO binding: a VETO in any
  reply is surfaced to the user immediately and stops further dispatch on
  that chain.
- File paths must be absolute. Cross-workspace relative paths silently
  resolve against the recipient's directory.

## When this skill is unavailable

If for any reason this skill is not loaded but you still need to
delegate, run `"$AIONUI_HELPER_BIN" delegate capabilities` for the full
contract. The CLI is the single source of truth for shapes — this skill
just documents the workflow.

## Exact schemas

For full field tables, error-code meanings, and the precise stdin
payload shape:

```bash
"$AIONUI_HELPER_BIN" delegate capabilities
```
