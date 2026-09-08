-- Migration 045: project-scoped team engagements (additive foundation).
--
--   * team_engagements        — a team bound to ONE project; owns runtime state.
--   * team_engagement_members— per-engagement member slot instances.
--   * team_tasks.engagement_id / mailbox.engagement_id — nullable re-key column,
--     backfilled to a per-team DEFAULT engagement (id = team.id).
--
-- team_id remains authoritative; this phase adds engagement scoping alongside it.
-- No runtime code reads engagement_id yet.

CREATE TABLE IF NOT EXISTS team_engagements (
    id          TEXT    PRIMARY KEY NOT NULL,
    user_id     TEXT    NOT NULL,
    team_id     TEXT    NOT NULL,
    project_id  TEXT    NOT NULL,
    workspace   TEXT    NOT NULL DEFAULT '',
    process     TEXT    NOT NULL DEFAULT 'hierarchical'
                        CHECK (process IN ('sequential','hierarchical')),
    status      TEXT    NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active','archived')),
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_team_engagements_team ON team_engagements(team_id, status);
CREATE INDEX IF NOT EXISTS idx_team_engagements_project ON team_engagements(team_id, project_id);
-- exactly one engagement per (team, project)
CREATE UNIQUE INDEX IF NOT EXISTS uq_team_engagements_team_project
    ON team_engagements(team_id, project_id);

CREATE TABLE IF NOT EXISTS team_engagement_members (
    engagement_id     TEXT NOT NULL REFERENCES team_engagements(id),
    team_id           TEXT NOT NULL,
    template_slot     TEXT NOT NULL,
    slot_id           TEXT NOT NULL,
    conversation_id   TEXT NOT NULL,
    role              TEXT NOT NULL,
    status            TEXT,
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL,
    UNIQUE(engagement_id, template_slot)
);
CREATE INDEX IF NOT EXISTS idx_team_engagement_members_eng
    ON team_engagement_members(engagement_id);

ALTER TABLE team_tasks ADD COLUMN engagement_id TEXT;
ALTER TABLE mailbox    ADD COLUMN engagement_id TEXT;
CREATE INDEX IF NOT EXISTS idx_team_tasks_engagement ON team_tasks(engagement_id);
CREATE INDEX IF NOT EXISTS idx_mailbox_engagement    ON mailbox(engagement_id);

-- Backfill: one default engagement per existing non-archived team, id = team.id.
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
WHERE t.archived_at IS NULL;

UPDATE team_tasks SET engagement_id = team_id WHERE engagement_id IS NULL;
UPDATE mailbox    SET engagement_id = team_id WHERE engagement_id IS NULL;
