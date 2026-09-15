-- Migration 048: CrewAI task-context columns on team_tasks (spec §7.1).
--
--   * expected_output — what the task is meant to produce (set on create).
--   * result          — the task's outcome, captured on completion (Task 2).
--   * input_context   — upstream inputs fed to the task (Task 3).
--
-- Additive only; nullable TEXT, no default. SQLite ADD COLUMN is not
-- idempotent and has no IF NOT EXISTS (matches migration 046's precedent).
-- result/input_context are written by later tasks; here they exist as columns.

ALTER TABLE team_tasks ADD COLUMN expected_output TEXT;
ALTER TABLE team_tasks ADD COLUMN result TEXT;
ALTER TABLE team_tasks ADD COLUMN input_context TEXT;
