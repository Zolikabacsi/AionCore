---
name: delegate
description: Cross-agent delegation - dispatch a task to or ask another agent by name; replies route back to your conversation. Use for cross-squad work or when your role calls for hierarchical delegation.
---

# Cross-Agent Delegation Skill

Deliver work to another agent — by **name** or by **assistant_id** — and
receive their reply back in your conversation. CrewAI-style hierarchical
delegation: any agent whose `allow_delegation = 1` can address any other.

## Rules

1. Use this only when the user explicitly asks for delegation, OR when your
   role description says "delegate to X for this kind of task". Never
   delegate on your own initiative outside an explicit dispatcher pattern.
2. Address targets by **name** (case-insensitive prefix match) or by
   `assistant_id`. Names are resolved server-side via `targets`; never
   pass a conversation id, slot id, or wildcard.
3. `to` must name exactly one agent. There is no broadcast.
4. Never pass, inline, export, echo, or set any `AIONUI_...` environment
   variable.
5. Commands must directly call `"$AIONUI_HELPER_BIN" delegate ...`. Pass
   payloads through stdin heredocs. Do not write payload JSON files to disk.
6. Team members: coordinate INSIDE your team with team tasks and
   `team send-message`. This skill is for work outside your team — you MAY
   dispatch to ANOTHER team's engagement (name the team in `to`) or to any
   delegating agent. Cross-team loops are stopped server-side: a
   `cycle_detected` or `depth_exceeded` response means stop (Rules 7/9).
7. On `rate_limited`, STOP delivering. The two agents are spinning
   against each other. Tell the user, do not retry.
8. Word results precisely. `delivered` means "delivered to the target's
   conversation; their turn will start when the runtime frees up".
   `queued` means "their runtime is busy; will retry until it frees up".
   `created_and_delivered` means "no prior conversation existed for this
   target; the runtime created one and delivered". Never claim a message
   was read.
9. If the response is `cycle_detected`, STOP. The target is already on
   your delegation chain. Do not retry.
10. If `ask` returns `sync_timeout`, surface that to the user as "I asked
    X but didn't get a reply within N seconds".

## Delegating a task (async)

```bash
"$AIONUI_HELPER_BIN" delegate dispatch <<'JSON'
{
  "to": "CMO",
  "message": "Draft a 3-tweet launch thread for the new product.",
  "files": ["/abs/path/to/brief.md"]
}
JSON
```

The recipient runs in their own conversation. Their reply arrives as a
new user-role message in YOUR conversation (because `reply_to` defaults to
your conversation id).

## Asking a question (sync)

Use only when you cannot proceed without the answer. Your turn is
suspended until the target replies or the timeout fires.

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

Returns every agent whose `allow_delegation = 1`, optionally filtered by
substring.

## Replying to a delegated task

If a delegated task arrives with `reply_to` in its envelope block,
deliver your response by addressing your own `delegate dispatch` to the
agent named in `reply_to` (or to the conversation id in `reply_to` via
`session send-message` if the conversation id is what you have).

## Constraints

- Depth ceiling: 3 (CEO → CMO → Copywriter → leaf). If `depth` would
  exceed 3, the runtime returns `depth_exceeded`. Reply inline instead of
  delegating further when you see a high `depth` on the incoming envelope.
- Cycle protection: server-side. If you ever get `cycle_detected`, report
  to the user and stop delegating along that path.
- CMedO's VETO is binding. If you receive a VETO in any reply, surface to
  the user immediately and stop further dispatch on that chain.
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
