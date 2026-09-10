use crate::error::DbError;
use crate::models::{MailboxMessageRow, TeamEngagementMemberRow, TeamEngagementRow, TeamRow, TeamTaskRow};

/// Sort/paging direction for the activity feed cursor queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageDirection {
    /// Newest first; `load more` walks toward older rows.
    Desc,
    /// Oldest first; `load more` walks toward newer rows.
    Asc,
}

/// Keyset-pagination cursor. Rows strictly beyond `(created_at, id)` in the
/// requested direction are returned. `id` is compared lexicographically to
/// match the `ORDER BY ... id` tiebreak.
#[derive(Debug, Clone)]
pub struct ActivityCursor {
    pub created_at: i64,
    pub id: String,
}

/// Parameters for updating a team record.
#[derive(Debug, Clone, Default)]
pub struct UpdateTeamParams {
    pub name: Option<String>,
    pub workspace: Option<String>,
    pub agents: Option<String>,
    pub lead_agent_id: Option<String>,
    pub session_mode: Option<String>,
    /// Project binding (project-bind side branch); `Some` sets the column.
    pub project_id: Option<String>,
    pub folder_id: Option<String>,
}

/// Parameters for updating a task record.
#[derive(Debug, Clone, Default)]
pub struct UpdateTaskParams {
    pub status: Option<String>,
    pub description: Option<String>,
    pub owner: Option<String>,
    pub blocked_by: Option<String>,
    pub metadata: Option<String>,
}

/// Data access abstraction for team collaboration tables.
///
/// Covers three tables: `teams`, `mailbox`, and `team_tasks`.
///
/// Object-safe via `async_trait` to support `Arc<dyn ITeamRepository>`.
#[async_trait::async_trait]
pub trait ITeamRepository: Send + Sync {
    // ── Team CRUD ────────────────────────────────────────────────────

    /// Inserts a new team record.
    async fn create_team(&self, row: &TeamRow) -> Result<(), DbError>;

    /// Returns all teams for startup/session restore.
    async fn list_teams_for_restore(&self) -> Result<Vec<TeamRow>, DbError>;

    /// Returns teams owned by `user_id`, ordered by creation time ascending.
    async fn list_teams_by_user(&self, user_id: &str) -> Result<Vec<TeamRow>, DbError>;

    /// Returns a single team owned by `user_id`, or `None` if not found.
    async fn get_team(&self, user_id: &str, team_id: &str) -> Result<Option<TeamRow>, DbError>;

    /// Returns a single team for startup/session restore.
    async fn get_team_for_restore(&self, team_id: &str) -> Result<Option<TeamRow>, DbError>;

    /// Updates a team by id with the provided fields.
    /// Returns `DbError::NotFound` if absent.
    async fn update_team(&self, user_id: &str, team_id: &str, params: &UpdateTeamParams) -> Result<(), DbError>;

    /// Deletes a team by id. Returns `DbError::NotFound` if absent.
    async fn delete_team(&self, user_id: &str, team_id: &str) -> Result<(), DbError>;

    // ── Mailbox ──────────────────────────────────────────────────────

    /// Writes a message to the mailbox.
    async fn write_message(&self, user_id: &str, row: &MailboxMessageRow) -> Result<(), DbError>;

    /// Atomically reads all unread messages for `to_agent_id` in a team
    /// and marks them as read. Uses `BEGIN IMMEDIATE` for atomicity.
    async fn read_unread_and_mark(
        &self,
        user_id: &str,
        team_id: &str,
        to_agent_id: &str,
    ) -> Result<Vec<MailboxMessageRow>, DbError>;

    /// Reads all unread messages for `to_agent_id` without marking them as read.
    async fn peek_unread(
        &self,
        user_id: &str,
        team_id: &str,
        to_agent_id: &str,
    ) -> Result<Vec<MailboxMessageRow>, DbError>;

    /// Reads the requested unread messages for `to_agent_id` without marking
    /// them as read. Missing or already-read IDs are omitted. Rows are ordered
    /// like `peek_unread` (`created_at ASC, id ASC`) so callers may rely on FIFO
    /// order regardless of the order of `ids`.
    async fn peek_unread_by_ids(
        &self,
        user_id: &str,
        team_id: &str,
        to_agent_id: &str,
        ids: &[String],
    ) -> Result<Vec<MailboxMessageRow>, DbError>;

    /// Marks the given message IDs as read. IDs that don't exist are silently ignored.
    async fn mark_read_batch(&self, user_id: &str, team_id: &str, ids: &[String]) -> Result<(), DbError>;

    /// Returns message history for an agent, optionally limited.
    /// Messages are ordered by `created_at` ascending.
    async fn get_history(
        &self,
        user_id: &str,
        team_id: &str,
        to_agent_id: &str,
        limit: Option<i64>,
    ) -> Result<Vec<MailboxMessageRow>, DbError>;

    /// Returns the most recent messages for the whole team, ordered by
    /// `created_at` descending and capped at `limit`. Backs the read-only
    /// team activity view (all recipients, not a single mailbox).
    async fn list_messages_by_team(&self, team_id: &str, limit: i64) -> Result<Vec<MailboxMessageRow>, DbError>;

    /// Keyset-paginated team-wide messages for the activity feed. Returns up to
    /// `limit` rows strictly beyond `cursor` in `direction` order (no cursor =
    /// first page). Ordered `(created_at, id)` per direction.
    async fn list_messages_by_team_paged(
        &self,
        team_id: &str,
        cursor: Option<ActivityCursor>,
        direction: PageDirection,
        limit: i64,
    ) -> Result<Vec<MailboxMessageRow>, DbError>;

    /// Returns the message rows with the given ids, ordered by `created_at`
    /// descending. Used to build full payloads after a batch read-mark.
    /// An empty `ids` slice yields an empty result without querying.
    async fn list_messages_by_ids(&self, ids: &[String]) -> Result<Vec<MailboxMessageRow>, DbError>;

    /// Deletes all mailbox messages belonging to a team.
    async fn delete_mailbox_by_team(&self, user_id: &str, team_id: &str) -> Result<(), DbError>;

    // ── Tasks ────────────────────────────────────────────────────────

    /// Creates a new task.
    async fn create_task(&self, user_id: &str, row: &TeamTaskRow) -> Result<(), DbError>;

    /// Finds a task by exact id within a team.
    async fn find_task_by_id(
        &self,
        user_id: &str,
        team_id: &str,
        task_id: &str,
    ) -> Result<Option<TeamTaskRow>, DbError>;

    /// Updates a task by id with the provided fields.
    /// Returns `DbError::NotFound` if absent.
    async fn update_task(
        &self,
        user_id: &str,
        team_id: &str,
        task_id: &str,
        params: &UpdateTaskParams,
    ) -> Result<(), DbError>;

    /// Returns all tasks for a team, ordered by `created_at` ascending.
    async fn list_tasks(&self, user_id: &str, team_id: &str) -> Result<Vec<TeamTaskRow>, DbError>;

    /// Keyset-paginated team tasks for the activity feed (user-scoped). Up to
    /// `limit` rows strictly beyond `cursor` in `direction` order.
    async fn list_tasks_paged(
        &self,
        user_id: &str,
        team_id: &str,
        cursor: Option<ActivityCursor>,
        direction: PageDirection,
        limit: i64,
    ) -> Result<Vec<TeamTaskRow>, DbError>;

    /// Returns the task rows with the given ids within a team (user-scoped),
    /// ordered by `created_at` descending. Used to resolve dependency
    /// (`blocked_by`) subjects for tasks that may lie outside the loaded
    /// activity page. An empty `ids` slice yields an empty result without
    /// querying. Unknown ids are silently ignored.
    async fn list_tasks_by_ids(
        &self,
        user_id: &str,
        team_id: &str,
        ids: &[String],
    ) -> Result<Vec<TeamTaskRow>, DbError>;

    /// Appends `blocked_task_id` to the `blocks` JSON array of `task_id`.
    /// This is a transactional JSON array append operation.
    async fn append_to_blocks(
        &self,
        user_id: &str,
        team_id: &str,
        task_id: &str,
        blocked_task_id: &str,
    ) -> Result<(), DbError>;

    /// Removes `unblocked_task_id` from the `blocked_by` JSON array of `task_id`.
    /// This is a transactional JSON array removal operation.
    async fn remove_from_blocked_by(
        &self,
        user_id: &str,
        team_id: &str,
        task_id: &str,
        unblocked_task_id: &str,
    ) -> Result<(), DbError>;

    /// Deletes all tasks belonging to a team.
    async fn delete_tasks_by_team(&self, user_id: &str, team_id: &str) -> Result<(), DbError>;

    // ── Engagements ──────────────────────────────────────────────────
    //
    // Declared with default "not implemented" bodies so non-SQLite test doubles
    // that don't exercise engagement CRUD still satisfy the trait; the real
    // `SqliteTeamRepository` overrides every method and is authoritative.

    /// Inserts a new engagement for a `(team, project)` pair. Fails with
    /// `NotFound` unless `user_id` owns `team_id` (P2-1 ownership guard).
    async fn create_engagement(
        &self,
        _user_id: &str,
        _team_id: &str,
        _project_id: &str,
        _workspace: &str,
    ) -> Result<TeamEngagementRow, DbError> {
        Err(DbError::NotFound("create_engagement not implemented".to_string()))
    }

    /// Returns the engagement binding `team_id` to `project_id` owned by
    /// `user_id`, or `None`. Scoped to `user_id` for data isolation.
    async fn find_engagement(
        &self,
        _user_id: &str,
        _team_id: &str,
        _project_id: &str,
    ) -> Result<Option<TeamEngagementRow>, DbError> {
        Err(DbError::NotFound("find_engagement not implemented".to_string()))
    }

    /// Returns all engagements for `user_id` in a team, oldest first.
    async fn list_engagements(&self, _user_id: &str, _team_id: &str) -> Result<Vec<TeamEngagementRow>, DbError> {
        Err(DbError::NotFound("list_engagements not implemented".to_string()))
    }

    /// Returns the engagement for `(team_id, project_id)`, creating it if absent.
    /// Tolerant of the `uq_team_engagements_team_project` race.
    async fn find_or_create_engagement(
        &self,
        _user_id: &str,
        _team_id: &str,
        _project_id: &str,
        _workspace: &str,
    ) -> Result<TeamEngagementRow, DbError> {
        Err(DbError::NotFound(
            "find_or_create_engagement not implemented".to_string(),
        ))
    }

    /// Returns all tasks bound to `engagement_id` owned by `user_id`, oldest
    /// first. Scoped via the engagement's owner (`team_engagements.user_id`).
    async fn list_tasks_by_engagement(
        &self,
        _user_id: &str,
        _engagement_id: &str,
    ) -> Result<Vec<TeamTaskRow>, DbError> {
        Err(DbError::NotFound(
            "list_tasks_by_engagement not implemented".to_string(),
        ))
    }

    /// Returns all mailbox messages bound to `engagement_id` owned by `user_id`,
    /// oldest first. Scoped via the engagement's owner (`team_engagements.user_id`).
    async fn list_messages_by_engagement(
        &self,
        _user_id: &str,
        _engagement_id: &str,
    ) -> Result<Vec<MailboxMessageRow>, DbError> {
        Err(DbError::NotFound(
            "list_messages_by_engagement not implemented".to_string(),
        ))
    }

    // ── Engagement-scoped runtime reads (Phase 2a Task 4b) ───────────────
    //
    // The session runtime must read ONLY the rows stamped with its own
    // engagement, never every row in the team, so two projects' sessions of one
    // team cannot observe each other's mail/tasks. These mirror the `team_id`
    // read variants above but filter on `engagement_id`. Default bodies return
    // `NotFound` (non-SQLite doubles that don't exercise them are unaffected);
    // the real `SqliteTeamRepository` and the session mocks override them.
    //
    // For legacy single-engagement teams `engagement_id == team_id`, so these
    // return the same rows the team-scoped variants would (no behavior change).

    /// Engagement-scoped `peek_unread`: unread rows for `to_agent_id` bound to
    /// `engagement_id`, ordered FIFO.
    async fn peek_unread_by_engagement(
        &self,
        _user_id: &str,
        _engagement_id: &str,
        _to_agent_id: &str,
    ) -> Result<Vec<MailboxMessageRow>, DbError> {
        Err(DbError::NotFound(
            "peek_unread_by_engagement not implemented".to_string(),
        ))
    }

    /// Engagement-scoped `peek_unread_by_ids`.
    async fn peek_unread_by_ids_by_engagement(
        &self,
        _user_id: &str,
        _engagement_id: &str,
        _to_agent_id: &str,
        _ids: &[String],
    ) -> Result<Vec<MailboxMessageRow>, DbError> {
        Err(DbError::NotFound(
            "peek_unread_by_ids_by_engagement not implemented".to_string(),
        ))
    }

    /// Engagement-scoped `read_unread_and_mark`.
    async fn read_unread_and_mark_by_engagement(
        &self,
        _user_id: &str,
        _engagement_id: &str,
        _to_agent_id: &str,
    ) -> Result<Vec<MailboxMessageRow>, DbError> {
        Err(DbError::NotFound(
            "read_unread_and_mark_by_engagement not implemented".to_string(),
        ))
    }

    /// Engagement-scoped `mark_read_batch`.
    async fn mark_read_batch_by_engagement(
        &self,
        _user_id: &str,
        _engagement_id: &str,
        _ids: &[String],
    ) -> Result<(), DbError> {
        Err(DbError::NotFound(
            "mark_read_batch_by_engagement not implemented".to_string(),
        ))
    }

    /// Engagement-scoped `get_history`.
    async fn get_history_by_engagement(
        &self,
        _user_id: &str,
        _engagement_id: &str,
        _to_agent_id: &str,
        _limit: Option<i64>,
    ) -> Result<Vec<MailboxMessageRow>, DbError> {
        Err(DbError::NotFound(
            "get_history_by_engagement not implemented".to_string(),
        ))
    }

    /// Engagement-scoped `find_task_by_id`.
    async fn find_task_by_engagement(
        &self,
        _user_id: &str,
        _engagement_id: &str,
        _task_id: &str,
    ) -> Result<Option<TeamTaskRow>, DbError> {
        Err(DbError::NotFound("find_task_by_engagement not implemented".to_string()))
    }

    // ── Engagement members (Phase 2b Task 1) ───────────────────────────
    //
    // Per-engagement member rows live in `team_engagement_members` (unique on
    // `(engagement_id, template_slot)`). Default bodies return `NotFound` so
    // non-SQLite doubles still satisfy the trait; the real
    // `SqliteTeamRepository` overrides every method.

    /// Inserts or updates a member row keyed on `(engagement_id, template_slot)`.
    async fn upsert_engagement_member(&self, _row: &TeamEngagementMemberRow) -> Result<(), DbError> {
        Err(DbError::NotFound(
            "upsert_engagement_member not implemented".to_string(),
        ))
    }

    /// Returns all member rows for `engagement_id` owned by `user_id`, oldest
    /// first. Scoped via the engagement's owner (`team_engagements.user_id`).
    async fn list_engagement_members(
        &self,
        _user_id: &str,
        _engagement_id: &str,
    ) -> Result<Vec<TeamEngagementMemberRow>, DbError> {
        Err(DbError::NotFound("list_engagement_members not implemented".to_string()))
    }

    /// Resolves a member by `(engagement_id, slot_id)` (the runtime slot id).
    ///
    /// NOT ownership-checked: an internal resolve-by-key helper for use on
    /// already-authorized paths only.
    async fn get_engagement_member_by_slot(
        &self,
        _engagement_id: &str,
        _slot_id: &str,
    ) -> Result<Option<TeamEngagementMemberRow>, DbError> {
        Err(DbError::NotFound(
            "get_engagement_member_by_slot not implemented".to_string(),
        ))
    }

    /// Resolves a member by `conversation_id`.
    ///
    /// NOT ownership-checked: an internal resolve-by-key helper for use on
    /// already-authorized paths only.
    async fn get_engagement_member_by_conversation(
        &self,
        _conversation_id: &str,
    ) -> Result<Option<TeamEngagementMemberRow>, DbError> {
        Err(DbError::NotFound(
            "get_engagement_member_by_conversation not implemented".to_string(),
        ))
    }

    /// Deletes every member row belonging to a team's engagements. Called by
    /// `remove_team` BEFORE `delete_engagements_by_team` (the FK ceiling
    /// `team_engagement_members.engagement_id -> team_engagements.id`), so a
    /// removed team leaves no member-row leak. Scoped to `user_id` + `team_id`
    /// (data isolation); a legacy team has no member rows and deletes cleanly.
    /// Default `NotFound` so non-SQLite doubles that don't exercise teardown are
    /// unaffected; `remove_team` treats the error as best-effort.
    async fn delete_engagement_members_by_team(&self, _user_id: &str, _team_id: &str) -> Result<(), DbError> {
        Err(DbError::NotFound(
            "delete_engagement_members_by_team not implemented".to_string(),
        ))
    }

    /// Deletes every engagement row bound to a team. Called by `remove_team`
    /// AFTER `delete_engagement_members_by_team`. A legacy team's single default
    /// engagement (id == team_id, minted by the 045 backfill) is removed with the
    /// team, which is correct. Scoped to `user_id` + `team_id` (data isolation).
    async fn delete_engagements_by_team(&self, _user_id: &str, _team_id: &str) -> Result<(), DbError> {
        Err(DbError::NotFound(
            "delete_engagements_by_team not implemented".to_string(),
        ))
    }
}
