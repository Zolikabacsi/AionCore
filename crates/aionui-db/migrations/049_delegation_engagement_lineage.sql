-- Migration 049: engagement lineage for team-dispatch guards (spec §5.9 / §9).
--
-- `delegation_envelopes.target_engagement_id` records the engagement a TEAM
-- dispatch convened (NULL for assistant dispatches). A team hop has no
-- `target_assistant_id`, so its cycle/depth accounting is keyed on the
-- engagement instead of the (assistant-scoped) `target_assistant_id` column
-- the assistant LIVE_CHAIN keeps using unchanged. Nullable → assistant rows and
-- every existing row are unaffected.
ALTER TABLE delegation_envelopes ADD COLUMN target_engagement_id TEXT;
