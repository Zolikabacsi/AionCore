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
