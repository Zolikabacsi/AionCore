# Team Engagements — Implementation Handoff

> Self-contained resume point for a fresh session with **no** prior context.
> Last updated: Phase 2a complete (HEAD `ff9ef31b`, branch `feat/team-engagements-phase2a`).

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

Stacking: `phase2a` is built **on top of** `feat/delegate` (which is on top of `phase1`). Phase 2b should branch from `feat/team-engagements-phase2a`.

## DONE so far (Phases 1 & 2a)
- Engagement entity + CRUD + `find_or_create_engagement(user_id, team_id, project_id, …)` (idempotent per `(team,project)`, race-safe via `ON CONFLICT`, ownership-guarded: non-owner cannot create/read an engagement for a team they don't own).
- Runtime **write** path stamps `team_tasks.engagement_id` / `mailbox.engagement_id` with the session's resolved engagement; **read** path (`peek_unread`, task board list/find) filters by that same `engagement_id` → per-engagement isolation of the session's own tasks/mail.
- Legacy single-project teams stay correct via the sentinel: their default engagement `id == team_id` (migration 045 backfill; 046 backfills NULL rows written in between).
- Delegate `sender_is_team` guard fixed (was reading snake_case `team_id` the runtime never writes → now reads canonical `teamId`).
- Tests: `cargo test -p aionui-db -p aionui-team -p aionui-delegate` all green (~1550 assertions); `clippy -D warnings` clean for all three crates.

## NEXT: Phase 2b (branch from `feat/team-engagements-phase2a`)
Run the plan `docs/superpowers/plans/2026-09-08-team-engagements-phase2b-engagement-members.md` with the **subagent-driven-development** skill (fresh implementer subagent per task + per-task review, exactly as Phases 1/2a were executed). Tasks:
1. `team_engagement_members` repository layer.
2. Materialize per-engagement member slot instances (one `conversation_id` per member per engagement).
3. Switch member READERS to the engagement's members (not `teams.agents`).
4. Deprecate single-team-project rebind reads (`teams.project_id`/`workspace_mode`/`update_team_project`).
5. Cross-engagement isolation end-to-end + template independence.

## Must carry forward (from Phase 2a review) — fold into 2b or Phase 4, do not silently drop
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
