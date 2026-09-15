-- Migration 044: support `aionui-delegate`.
--
-- Adds:
--   * assistant_definitions.allow_delegation  — per-agent flag. Set to 1 on
--     the four squad Leads + CEO so they can address each other and their
--     own specialists by name.
--   * delegation_envelopes — per-dispatch record. Tracks root, target,
--     depth, parent chain for cycle detection, and async reply tracking.
--   * suspended_turns — sync-mode bookkeeping. Tracks a caller's parked
--     turn until the target replies or the timeout fires.

ALTER TABLE assistant_definitions ADD COLUMN allow_delegation INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS delegation_envelopes (
    id                    TEXT PRIMARY KEY NOT NULL,
    user_id               TEXT NOT NULL,
    root_conversation_id  TEXT NOT NULL,
    target_conversation_id TEXT NOT NULL,
    target_assistant_id   TEXT NOT NULL,
    depth                 INTEGER NOT NULL DEFAULT 0,
    parent_envelope_id    TEXT REFERENCES delegation_envelopes(id),
    status                TEXT NOT NULL
        CHECK(status IN ('pending','delivered','replied','sync_pending','sync_replied','sync_timeout','expired','rejected')),
    reply_envelope_id     TEXT REFERENCES delegation_envelopes(id),
    reply_content         TEXT,
    expires_at            INTEGER NOT NULL,
    created_at            INTEGER NOT NULL,
    updated_at            INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_delegation_envelopes_root
    ON delegation_envelopes(root_conversation_id);
CREATE INDEX IF NOT EXISTS idx_delegation_envelopes_target
    ON delegation_envelopes(target_conversation_id, status);
CREATE INDEX IF NOT EXISTS idx_delegation_envelopes_chain
    ON delegation_envelopes(root_conversation_id, depth);

CREATE TABLE IF NOT EXISTS suspended_turns (
    id                TEXT PRIMARY KEY NOT NULL,
    envelope_id       TEXT NOT NULL REFERENCES delegation_envelopes(id),
    conversation_id   TEXT NOT NULL,
    turn_id           TEXT NOT NULL,
    wake_condition    TEXT NOT NULL
        CHECK(wake_condition IN ('reply_received','timeout')),
    expires_at        INTEGER NOT NULL,
    created_at        INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_suspended_turns_envelope
    ON suspended_turns(envelope_id);
CREATE INDEX IF NOT EXISTS idx_suspended_turns_conversation
    ON suspended_turns(conversation_id, turn_id);
