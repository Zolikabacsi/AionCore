-- Migration 046: engagement lifecycle columns (spec §4.1).
--
--   * folder_id                    — optional drive folder backing the engagement.
--   * origin                       — 'user' (default) or 'delegated' creation path.
--   * created_by_conversation_id   — originating conversation when delegated.
--   * reply_to                     — correlation id for reply-driven engagements.
--
-- Additive only; SQLite ADD COLUMN with NOT NULL requires a constant default
-- ('user' satisfies origin). No runtime code reads these yet.

ALTER TABLE team_engagements ADD COLUMN folder_id TEXT;
ALTER TABLE team_engagements ADD COLUMN origin TEXT NOT NULL DEFAULT 'user'
    CHECK (origin IN ('user','delegated'));
ALTER TABLE team_engagements ADD COLUMN created_by_conversation_id TEXT;
ALTER TABLE team_engagements ADD COLUMN reply_to TEXT;

-- Close the write/read gap for rows the runtime created between 045 and this
-- branch, which wrote engagement_id = NULL (045's backfill predated them) but
-- are now read under `WHERE engagement_id = ?`. Attribute them to the owning
-- team's default engagement (045 set team_engagements.id = teams.id), scoped to
-- teams that actually have that default engagement so archived/orphaned rows
-- keep NULL rather than dangling.
UPDATE team_tasks
    SET engagement_id = team_id
  WHERE engagement_id IS NULL
    AND team_id IN (SELECT id FROM team_engagements);
UPDATE mailbox
    SET engagement_id = team_id
  WHERE engagement_id IS NULL
    AND team_id IN (SELECT id FROM team_engagements);
