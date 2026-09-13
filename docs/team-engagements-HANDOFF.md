# Team Engagements — Implementation Handoff

> Self-contained resume point for a fresh session with **no** prior context.
> Last updated: **Phase 2b complete** (branch `feat/team-engagements-phase2b`, tip `d3c4885e`; stacked on 2a `51a98983`). NOT pushed/deployed. NEXT: Phase 3/4.

## What we are building
CrewAI-style **project-based cooperative team engagements** for AionUi/AionCore. A **Team** is a reusable *template* (member assistant-definitions + roles, no runtime state). A **TeamEngagement** is an instance of a Team bound to one **Project**, owning its own isolated member conversations, mailbox, and task board. The same team can run in many projects, fully isolated. This replaces the old `teams.project_id` single-binding model.

- **Spec (binding authority):** `docs/superpowers/specs/2026-09-08-project-scoped-team-engagements-design.md` — read §3 (terminology), §4 (data model), §5 (delegation bridge), §7 (CrewAI task/context), §9 (isolation), §16 (Phase-2 carry-overs).
- **Plans (execution):** `docs/superpowers/plans/` — `…phase1-foundation.md`, `…phase2a-engagement-routing.md`, `…phase2b-engagement-members.md`.

## Where things stand (branches on `origin`, all branched off `main`@`ce3cc10a`)
| branch | holds | state |
|---|---|---|
| `main` | untouched | `ce3cc10a` (== origin/main) |
| `feat/delegate` | the (previously uncommitted) **delegation subsystem** WIP + minimal build fixes | pushed, `544ccc04` |
| `feat/team-engagements-phase1` | engagement **foundation**: migration 045 (`team_engagements`, `team_engagement_members`, nullable `engagement_id` cols), `ITeamRepository` engagement CRUD + race-safe `find_or_create_engagement`, user-scoped engagement reads, `ensure_engagement` seam | pushed, `ea6c5d8b` |
| `feat/team-engagements-phase2a` | **engagement routing**: migration 046 (lifecycle cols + NULL backfill), `engagement_id` persisted on task/mail writes **and** session reads scoped by it, `find_or_create`/`create_engagement` team-**ownership guard**, delegate sender-guard reads canonical `teamId` | pushed, `ff9ef31b` |
| `feat/team-engagements-phase2b` | **per-engagement members**: `team_engagement_members` repo layer, materialize per-engagement member conversations (own slot_id/conversation_id in the engagement workspace), member readers/management + session/lock maps re-keyed to `engagement_id`, task-board mutation gating, project-switch via engagement (`teams.project_id` = legacy active pointer), e2e isolation + teardown-cleanup tests | **local** `d3c4885e` (14 commits over 2a; NOT pushed) |

Stacking: `phase2a` is built **on top of** `feat/delegate` (which is on top of `phase1`). Phase 2b should branch from `feat/team-engagements-phase2a`.

## DONE so far (Phases 1 & 2a)
- Engagement entity + CRUD + `find_or_create_engagement(user_id, team_id, project_id, …)` (idempotent per `(team,project)`, race-safe via `ON CONFLICT`, ownership-guarded: non-owner cannot create/read an engagement for a team they don't own).
- Runtime **write** path stamps `team_tasks.engagement_id` / `mailbox.engagement_id` with the session's resolved engagement; **read** path (`peek_unread`, task board list/find) filters by that same `engagement_id` → per-engagement isolation of the session's own tasks/mail.
- Legacy single-project teams stay correct via the sentinel: their default engagement `id == team_id` (migration 045 backfill; 046 backfills NULL rows written in between).
- Delegate `sender_is_team` guard fixed (was reading snake_case `team_id` the runtime never writes → now reads canonical `teamId`).
- Tests: `cargo test -p aionui-db -p aionui-team -p aionui-delegate` all green (~1550 assertions); `clippy -D warnings` clean for all three crates.

## DONE so far (Phases 1, 2a & 2b)
- **Phase 2b — per-engagement members (branch `feat/team-engagements-phase2b`, tip `d3c4885e`):** `team_engagement_members` repo layer (`upsert`/`list`/`get_by_slot`/`get_by_conversation`); `ensure_engagement_members` materializes one member conversation per `(engagement, template_slot)` in the engagement's workspace; scheduler + session/lock maps (`sessions`, `ensure_session_locks`) keyed by `engagement_id` (add_agent_locks intentionally team-keyed — guards the shared roster template); `resolve_runtime_agent` routes all member management/restart/config ops to the active engagement member (no-template-fall-through guard, legacy fallback); task-board mutation paths engagement-gated; `update_team_project` switches via `ensure_engagement` (prior engagement untouched, prior session stopped); teardown (`remove_team`) deletes engagement+member rows and kills engagement-member conversations. `teams.agents` is now a pure template; `teams.project_id` = legacy active-engagement pointer. e2e `tests/engagement_members.rs` (mutation-tested) proves two-projects-same-team isolation + template-independence. `cargo test -p aionui-db -p aionui-team` green; clippy `-D warnings` clean.
- Consumed from the 2a carry-forward list: **I2** (resolve-fallback severity — non-NotFound now `error!`), **session/lock re-key** (done in 2b), **task-board mutation re-scope** (done, defense-in-depth at task_board layer), **teardown/engagement-row cleanup** (found+fixed at final review). Still open for later phases: **§11 UI** (engagement read endpoints + `TeamEngagementSelector`), **delegate-guard flip / team→engagement bridge** (Phase 4), **per-request project selector on routes** (Phase 5 — `ensure_session` still keys off `team.project_id`), reconcile-during-remove multi-engagement regression test (scheduler pause seam not exposed).

## NEXT: Phase 3 / Phase 4 (branch from `feat/team-engagements-phase2b`)
Phase 2b is COMPLETE and unblocks Phase 4 (the delegation bridge can now convene a team's engagement with its member slots materialized). See the spec §16 phase list for Phase 3 scope; Phase 4 wires the delegate sender-guard→engagement bridge (and then the sender-guard behavior flip noted above).
<details><summary>Superseded: original Phase 2b NEXT (kept for history)</summary>

1. `team_engagement_members` repository layer.
2. Materialize per-engagement member slot instances (one `conversation_id` per member per engagement).
3. Switch member READERS to the engagement's members (not `teams.agents`).
4. Deprecate single-team-project rebind reads (`teams.project_id`/`workspace_mode`/`update_team_project`).
5. Cross-engagement isolation end-to-end + template independence.

</details>

## Must carry forward (from Phase 2a review) — note: I2 / session-lock re-key / task-board re-scope were CONSUMED in 2b (see above); remaining live items: §11 UI, delegate-guard flip, per-request project selector (Phase 5)
- **I2 — resolve-fallback severity:** `aionui-team/src/session.rs` (`ensure_session` engagement resolve, ~line 590) falls back to `team.id` on a `find_or_create_engagement` error with only a `warn!`. A genuine DB failure would then silently mis-stamp writes. Make it `error!` (or fail the session start) on non-`NotFound` errors.
- **Session/lock map re-key (deferred 4c):** `service.rs` `sessions`/`add_agent_locks`/`ensure_session_locks` are still keyed by `team_id` (was ~:168/172/175). Re-keying to `engagement_id` + threading `project_id` through `ensure_session`/`get_run_state`/routes is **behaviorally inert until members are per-engagement**, so it co-lands naturally with Phase 2b steps 1–3. Do it in 2b.
- **task_board mutation paths** (`update_task`/`append_to_blocks`/`remove_from_blocked_by`) still team-scoped (safe today: globally-unique task ids + upstream engagement gate). Re-scope in the 2b re-key for defense-in-depth.
- **§11 UI:** engagement-scoped read endpoints + the `TeamEngagementSelector` in AionUi (`pages/team/TeamPage.tsx`, repurpose the existing `TeamProjectSwitcher`) = Phase 5.
- **Delegate sender guard behavior flip:** after the fix, a team-member conversation calling `delegate dispatch` now returns `SenderIsTeam` (guard was inert before). Confirm no shipped preset relies on member→solo-dispatch before enabling Phase 4's team→engagement bridge.
- **Pre-existing repo `cargo fmt` drift** (~38 files, incl. `aionui-delegate/service.rs` and `aionui-team/service.rs`) predates this work; `just push` formats. Not ours to clean.

## Key invariants to preserve
- `team_id` remains on `team_tasks`/`mailbox`/`teams` (engagement scoping is **additive**, not a replacement).
- `team_engagements.id == team_id` for the default/legacy single engagement (sentinel project `__none__`); real projects get a distinct uuid-v7 `generate_id()`.
- Ownership: any caller-supplied `team_id`/`engagement_id` path MUST verify `teams.user_id == authenticated user` (guard exists at `create_engagement`/`find_engagement`; extend if new entry points are added).
- Domain-crate conventions (AionCore `AGENTS.md`): `service.rs` no axum import; API types in `aionui-api-types`; migration `NNN_*.sql` `IF NOT EXISTS`; `just push` gate.

## Resume commands
```bash
cd /home/zoltan/repos/AionCore
git fetch origin
git switch feat/team-engagements-phase2a        # or: git switch -c feat/team-engagements-phase2b (from here)
git log --oneline main..HEAD                     # see the phase1→2a history
# Gates: cargo test -p aionui-db -p aionui-team -p aionui-delegate
#       cargo clippy -p aionui-db -p aionui-team -p aionui-delegate --all-targets -- -D warnings
```
Note: these branches are **not deployed** — the running AionUi uses the bundled `aioncore` built from `main`; Phase 1/2a are additive and not yet wired to the delegation bridge (Phase 4), so behavior for real users is unchanged until Phase 4+ and a rebuild.
