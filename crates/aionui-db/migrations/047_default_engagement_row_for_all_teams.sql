-- Migration 047: ensure every (non-archived) team owns a default engagement row.
--
-- Migration 045 backfilled `team_engagements` (id = team.id) only for teams that
-- existed then, and `create_team` now inserts it for new teams. Teams created
-- between 045 and this fix have no default engagement row, so engagement-scoped
-- runtime reads (`peek_unread_by_engagement`, `list_*_by_engagement`, gated on
-- `EXISTS(team_engagements …)`) returned nothing for them. Backfill the missing
-- sentinel rows using 045's shape (default process/status, COALESCE'd project).

INSERT OR IGNORE INTO team_engagements
    (id, user_id, team_id, project_id, workspace, process, status, created_at, updated_at)
SELECT
    t.id,
    t.user_id,
    t.id,
    COALESCE(t.project_id, '__none__'),
    t.workspace,
    'hierarchical',
    'active',
    t.created_at,
    t.updated_at
FROM teams t
WHERE t.archived_at IS NULL
  AND NOT EXISTS (SELECT 1 FROM team_engagements e WHERE e.id = t.id);
