use std::sync::Arc;

use aionui_api_types::TeamTaskChange;
use aionui_common::{generate_id, now_ms};
use aionui_db::ITeamRepository;
use aionui_db::UpdateTaskParams;
use aionui_db::models::TeamTaskRow;
use tracing::{debug, warn};

use crate::activity_mapping::task_to_response;
use crate::error::TeamError;
use crate::events::TeamEventEmitter;
use crate::types::{TaskStatus, TeamTask};

/// Upper bound (bytes) on a materialized task `input_context`. Context passing
/// is a naive newest-first concat of upstream results; see
/// [`build_input_context`] for the cap's upgrade path.
const MAX_INPUT_CONTEXT_CHARS: usize = 8000;

pub struct TaskBoard {
    repo: Arc<dyn ITeamRepository>,
    /// Optional real-time emitter. When present, task create/update broadcast
    /// `team.taskChanged`. Absent in unit tests that use [`TaskBoard::new`]
    /// directly.
    events: Option<Arc<TeamEventEmitter>>,
    user_id: String,
    /// Active engagement (`team × project` binding) that every task created
    /// through this board belongs to. Stamped on `TeamTaskRow` so the runtime
    /// no longer leaves `engagement_id` NULL (Phase 1 legacy).
    engagement_id: Option<String>,
}

/// Optional fields for task update.
#[derive(Debug, Clone, Default)]
pub struct TaskUpdate {
    pub status: Option<TaskStatus>,
    pub description: Option<String>,
    pub owner: Option<String>,
    pub blocked_by: Option<Vec<String>>,
    pub metadata: Option<serde_json::Value>,
}

impl TaskBoard {
    pub fn new(repo: Arc<dyn ITeamRepository>) -> Self {
        Self::new_for_user(repo, "system_default_user")
    }

    pub fn new_for_user(repo: Arc<dyn ITeamRepository>, user_id: impl Into<String>) -> Self {
        Self {
            repo,
            events: None,
            user_id: user_id.into(),
            engagement_id: None,
        }
    }

    /// Attaches a real-time event emitter for `team.taskChanged` broadcasts.
    pub fn with_events(mut self, events: Arc<TeamEventEmitter>) -> Self {
        self.events = Some(events);
        self
    }

    /// Pin every task created through this board to a specific engagement id.
    /// Set once at `TeamSession::start` from the team's resolved engagement.
    pub fn with_engagement(mut self, engagement_id: impl Into<String>) -> Self {
        self.engagement_id = Some(engagement_id.into());
        self
    }

    /// Engagement-scoped task lookup used by the runtime. A board with no
    /// engagement (unit tests) keeps the legacy team-wide find.
    async fn find_task(&self, team_id: &str, task_id: &str) -> Result<Option<TeamTaskRow>, TeamError> {
        let row = match &self.engagement_id {
            Some(engagement) => {
                self.repo
                    .find_task_by_engagement(&self.user_id, engagement, task_id)
                    .await?
            }
            None => self.repo.find_task_by_id(&self.user_id, team_id, task_id).await?,
        };
        Ok(row)
    }

    pub async fn create_task(
        &self,
        team_id: &str,
        subject: &str,
        description: Option<&str>,
        owner: Option<&str>,
        blocked_by: &[String],
        expected_output: Option<&str>,
    ) -> Result<TeamTask, TeamError> {
        for dep_id in blocked_by {
            // `create_task` is engagement-gated upstream: the dep loop below
            // (via `find_task`) rejects a dependency outside this engagement,
            // so the `blocks` edge we append can only ever point at an
            // in-engagement task.
            let dep = self.find_task(team_id, dep_id).await?;
            if dep.is_none() {
                return Err(TeamError::BlockedTaskNotFound(dep_id.clone()));
            }
        }

        let task_id = generate_id();
        let now = now_ms();
        let blocked_by_json = serde_json::to_string(blocked_by)?;

        let row = TeamTaskRow {
            id: task_id.clone(),
            team_id: team_id.to_owned(),
            subject: subject.to_owned(),
            description: description.map(str::to_owned),
            status: TaskStatus::Pending.to_string(),
            owner: owner.map(str::to_owned),
            blocked_by: blocked_by_json,
            blocks: "[]".to_owned(),
            metadata: None,
            created_at: now,
            updated_at: now,
            engagement_id: self.engagement_id.clone(),
            expected_output: expected_output.map(str::to_owned),
            result: None,
            input_context: None,
        };

        self.repo.create_task(&self.user_id, &row).await?;

        for dep_id in blocked_by {
            self.repo
                .append_to_blocks(&self.user_id, team_id, dep_id, &task_id)
                .await?;
        }

        debug!(team_id, task_id = %task_id, subject, "task created");

        let task = TeamTask::from_row(&row).map_err(TeamError::Json)?;
        if let Some(events) = &self.events {
            events.broadcast_task_changed(task_to_response(&task), TeamTaskChange::Created);
        }
        Ok(task)
    }

    /// Updates a task. When the board is pinned to an engagement, the
    /// `find_task` gate below rejects any task outside that engagement with
    /// `TaskNotFound` before the repo mutation runs, so the update is
    /// engagement-scoped even though the repo call stays `team_id`-scoped
    /// (task ids are globally unique — the read-gate is the invariant).
    pub async fn update_task(&self, team_id: &str, task_id: &str, update: &TaskUpdate) -> Result<TeamTask, TeamError> {
        let existing = self
            .find_task(team_id, task_id)
            .await?
            .ok_or_else(|| TeamError::TaskNotFound(task_id.to_owned()))?;

        let params = UpdateTaskParams {
            status: update.status.map(|s| s.to_string()),
            description: update.description.clone(),
            owner: update.owner.clone(),
            blocked_by: update.blocked_by.as_ref().map(serde_json::to_string).transpose()?,
            metadata: update.metadata.as_ref().map(serde_json::to_string).transpose()?,
        };

        self.repo.update_task(&self.user_id, team_id, task_id, &params).await?;

        if update.status == Some(TaskStatus::Completed) {
            self.check_unblocks(team_id, task_id, &existing).await?;
        }

        let updated = self
            .find_task(team_id, task_id)
            .await?
            .ok_or_else(|| TeamError::TaskNotFound(task_id.to_owned()))?;

        debug!(team_id, task_id, "task updated");

        let task = TeamTask::from_row(&updated).map_err(TeamError::Json)?;
        // Deletion is modeled as an update to `status=deleted`, not a separate
        // removed event; the frontend filters/removes by status.
        if let Some(events) = &self.events {
            events.broadcast_task_changed(task_to_response(&task), TeamTaskChange::Updated);
        }
        Ok(task)
    }

    /// Stamps a task's `result` (Phase 3a capture at turn finalize). Engagement-
    /// gated like [`update_task`](Self::update_task): the `find_task` read-gate
    /// rejects rows outside this board's engagement before the repo write.
    pub async fn set_task_result(&self, team_id: &str, task_id: &str, result: &str) -> Result<(), TeamError> {
        self.find_task(team_id, task_id)
            .await?
            .ok_or_else(|| TeamError::TaskNotFound(task_id.to_owned()))?;
        self.repo.set_task_result(&self.user_id, task_id, result).await?;
        debug!(team_id, task_id, "task result captured");
        Ok(())
    }

    /// Task rows visible to this board's scope (engagement-pinned when set,
    /// otherwise the whole team). Mirrors [`TaskBoard::find_task`]'s scoping.
    async fn scoped_list_tasks(&self, team_id: &str) -> Result<Vec<TeamTaskRow>, TeamError> {
        let rows = match &self.engagement_id {
            Some(engagement) => self.repo.list_tasks_by_engagement(&self.user_id, engagement).await?,
            None => self.repo.list_tasks(&self.user_id, team_id).await?,
        };
        Ok(rows)
    }

    /// Materializes a ready task's `input_context`: the concat of the results
    /// captured by its completed upstream dependencies (bounded to
    /// [`MAX_INPUT_CONTEXT_CHARS`]) plus a minimal brief anchor derived from the
    /// task itself. Persists the result and returns it, or `Ok(None)` (writing
    /// nothing) when the task is not ready, already has a context, or has no
    /// completed upstream with a result — so a task with nothing to pass stays
    /// NULL and its owner's wake is unchanged.
    ///
    /// Read-gated like [`update_task`](Self::update_task): `find_task` rejects
    /// rows outside this board's engagement before any compute or write.
    pub async fn materialize_input_context(&self, team_id: &str, task_id: &str) -> Result<Option<String>, TeamError> {
        let row = self
            .find_task(team_id, task_id)
            .await?
            .ok_or_else(|| TeamError::TaskNotFound(task_id.to_owned()))?;

        // Ready = pending with no remaining blockers. The completed dep was
        // already removed from `blocked_by` by `check_unblocks` before this
        // runs, so an empty list here is the ready signal.
        let ready = row.status == TaskStatus::Pending.to_string()
            && serde_json::from_str::<Vec<String>>(&row.blocked_by)
                .map(|b| b.is_empty())
                .unwrap_or(false);
        if !ready || row.input_context.is_some() {
            return Ok(None);
        }

        // A task's upstream deps are the rows whose `blocks` array lists it;
        // only completed ones with a non-empty `result` feed context forward.
        let all = self.scoped_list_tasks(team_id).await?;
        let mut deps: Vec<&TeamTaskRow> = all
            .iter()
            .filter(|t| {
                serde_json::from_str::<Vec<String>>(&t.blocks)
                    .map(|b| b.iter().any(|id| id == task_id))
                    .unwrap_or(false)
            })
            .filter(|t| {
                t.status == TaskStatus::Completed.to_string()
                    && t.result.as_deref().is_some_and(|r| !r.trim().is_empty())
            })
            .collect();
        if deps.is_empty() {
            return Ok(None);
        }
        // Oldest first; the cap keeps the newest on overflow.
        deps.sort_by_key(|t| t.created_at);

        let context = build_input_context(&row, &deps);
        self.repo
            .set_task_input_context(&self.user_id, task_id, &context)
            .await?;
        debug!(team_id, task_id, len = context.len(), "materialized task input_context");
        Ok(Some(context))
    }

    pub async fn list_tasks(&self, team_id: &str) -> Result<Vec<TeamTask>, TeamError> {
        let rows = match &self.engagement_id {
            Some(engagement) => self.repo.list_tasks_by_engagement(&self.user_id, engagement).await?,
            None => self.repo.list_tasks(&self.user_id, team_id).await?,
        };
        let tasks = rows.iter().filter_map(|r| TeamTask::from_row(r).ok()).collect();
        Ok(tasks)
    }

    /// Unblocks every downstream task listed in `completed_row.blocks`.
    ///
    /// Engagement scoping (defense-in-depth): task ids are globally unique, so
    /// the board's engagement read-gate (`find_task`) is the actual invariant.
    /// The completing task reached here only through an engagement-gated
    /// `find_task` (see [`TaskBoard::update_task`]), and every id in `blocks`
    /// was appended by [`TaskBoard::create_task`] after its dependency loop
    /// validated that dep via the same gate — so `blocks` is in-engagement by
    /// construction. We still re-check each downstream id through `find_task`
    /// before mutating it, so a hand-crafted cross-engagement edge can never
    /// unblock a task outside this engagement. No-engagement boards (unit
    /// tests) keep the legacy team-wide find, so behavior is unchanged.
    async fn check_unblocks(
        &self,
        team_id: &str,
        completed_task_id: &str,
        completed_row: &TeamTaskRow,
    ) -> Result<(), TeamError> {
        let blocks: Vec<String> = serde_json::from_str(&completed_row.blocks)?;
        for downstream_id in &blocks {
            // Engagement gate: skip downstream tasks that are not visible to
            // this board's engagement (out-of-engagement rows read as `None`).
            if self.find_task(team_id, downstream_id).await?.is_none() {
                debug!(
                    completed = completed_task_id,
                    skipped = %downstream_id,
                    "skipping out-of-engagement downstream task on unblock"
                );
                continue;
            }
            self.repo
                .remove_from_blocked_by(&self.user_id, team_id, downstream_id, completed_task_id)
                .await?;
            debug!(
                completed = completed_task_id,
                unblocked = %downstream_id,
                "dependency unblocked"
            );
            // Phase 3a context passing: when this downstream has just become
            // ready, materialize its completed upstream results into
            // `input_context` so the downstream owner's wake prompt can carry
            // them. Best-effort: a context read/write must never abort the
            // completing task's own unblock (the `?` above already handled the
            // dependency-removal invariant this is downstream of).
            if let Err(err) = self.materialize_input_context(team_id, downstream_id).await {
                warn!(
                    completed = completed_task_id,
                    unblocked = %downstream_id,
                    error = %err,
                    "failed to materialize downstream input_context on ready"
                );
            }
            // Broadcast the downstream task's changed dependency set so the
            // activity board and the downstream-owner wake path both observe it
            // is now (potentially) actionable. Non-fatal: a missing row or parse
            // failure must not abort the completing task's own update.
            if let Some(events) = &self.events
                && let Ok(Some(row)) = self.find_task(team_id, downstream_id).await
                && let Ok(task) = TeamTask::from_row(&row)
            {
                events.broadcast_task_changed(task_to_response(&task), TeamTaskChange::Updated);
            }
        }
        Ok(())
    }
}

/// Assemble a ready task's `input_context`: a `[[BRIEF]]` anchor (the task's own
/// subject + description — v1 has no cleaner "engagement brief" source in
/// `check_unblocks`, and the brief forbids adding a column) followed by an
/// `[[UPSTREAM]]` section of `- {dep_id}: {dep.result}` lines for its completed
/// dependencies, ordered oldest→newest and bounded to
/// [`MAX_INPUT_CONTEXT_CHARS`].
fn build_input_context(task: &TeamTaskRow, deps_asc: &[&TeamTaskRow]) -> String {
    let mut brief = task.subject.clone();
    if let Some(desc) = task.description.as_deref().map(str::trim).filter(|d| !d.is_empty()) {
        brief.push('\n');
        brief.push_str(desc);
    }
    let prefix = format!("[[BRIEF]]\n{brief}\n\n[[UPSTREAM]]\n");
    let lines: Vec<String> = deps_asc
        .iter()
        .map(|d| format!("- {}: {}", d.id, d.result.as_deref().unwrap_or("").trim_end()))
        .collect();
    cap_upstream(&prefix, &lines)
}

/// Bounded `[[UPSTREAM]]` render. Keeps the newest dependency lines and drops
/// the oldest on overflow (dependencies are passed in oldest→newest).
///
/// ponytail: naive byte cap, oldest-dep truncation, no per-result summarization.
/// The whole-thing scan is O(deps^2) renders but `deps` is tiny. Upgrade to
/// per-dependency summarization only if measured token pressure ever demands
/// more than a length bound.
fn cap_upstream(prefix: &str, lines_asc: &[String]) -> String {
    let total = lines_asc.len();
    let mut kept: Vec<&str> = Vec::new();
    let mut dropped = 0usize;
    for line in lines_asc.iter().rev() {
        let mut trial = kept.clone();
        trial.insert(0, line.as_str());
        let trial_dropped = total - trial.len();
        if render_input_context(prefix, &trial, trial_dropped).len() <= MAX_INPUT_CONTEXT_CHARS {
            kept = trial;
            continue;
        }
        if !kept.is_empty() {
            // This and every older line no longer fit; stop, keep the newest.
            dropped = total - kept.len();
            break;
        }
        // The single newest result already blows the cap on its own: hard
        // truncate it so at least its head passes forward.
        let budget = MAX_INPUT_CONTEXT_CHARS.saturating_sub(prefix.len() + 1);
        let mut cut = budget.min(line.len());
        while cut > 0 && !line.is_char_boundary(cut) {
            cut -= 1;
        }
        kept.push(&line[..cut]);
        dropped = total - 1;
        break;
    }
    render_input_context(prefix, &kept, dropped)
}

fn render_input_context(prefix: &str, kept_asc: &[&str], dropped: usize) -> String {
    let mut s = String::from(prefix);
    if dropped > 0 {
        s.push_str(&format!("…{dropped} older upstream result(s) truncated\n"));
    }
    for line in kept_asc {
        s.push_str(line);
        s.push('\n');
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::MockTeamRepo;
    use aionui_api_types::{TeamTaskChangedPayload, WebSocketMessage};
    use aionui_realtime::EventBroadcaster;

    struct RecordingBroadcaster {
        events: std::sync::Mutex<Vec<WebSocketMessage<serde_json::Value>>>,
    }

    impl RecordingBroadcaster {
        fn new() -> Self {
            Self {
                events: std::sync::Mutex::new(vec![]),
            }
        }

        fn events(&self) -> Vec<WebSocketMessage<serde_json::Value>> {
            self.events.lock().unwrap().clone()
        }
    }

    impl EventBroadcaster for RecordingBroadcaster {
        fn broadcast(&self, event: WebSocketMessage<serde_json::Value>) {
            self.events.lock().unwrap().push(event);
        }
    }

    fn board_with_events(repo: Arc<MockTeamRepo>) -> (TaskBoard, Arc<RecordingBroadcaster>) {
        let bc = Arc::new(RecordingBroadcaster::new());
        let emitter = Arc::new(TeamEventEmitter::new(
            "t1".into(),
            "system_default_user".into(),
            bc.clone(),
        ));
        (TaskBoard::new(repo).with_events(emitter), bc)
    }

    fn task_changes(bc: &RecordingBroadcaster) -> Vec<TeamTaskChangedPayload> {
        bc.events()
            .into_iter()
            .filter(|e| e.name == "team.taskChanged")
            .map(|e| serde_json::from_value(e.data).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn create_task_broadcasts_created() {
        let repo = Arc::new(MockTeamRepo::new());
        let (board, bc) = board_with_events(repo);

        let task = board.create_task("t1", "Build", None, None, &[], None).await.unwrap();

        let changes = task_changes(&bc);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].change, TeamTaskChange::Created);
        assert_eq!(changes[0].task.id, task.id);
        assert_eq!(changes[0].task.status, "pending");
    }

    #[tokio::test]
    async fn update_task_broadcasts_updated_including_deleted() {
        let repo = Arc::new(MockTeamRepo::new());
        let (board, bc) = board_with_events(repo);

        let task = board.create_task("t1", "Build", None, None, &[], None).await.unwrap();
        board
            .update_task(
                "t1",
                &task.id,
                &TaskUpdate {
                    status: Some(TaskStatus::Deleted),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let updated: Vec<_> = task_changes(&bc)
            .into_iter()
            .filter(|c| c.change == TeamTaskChange::Updated)
            .collect();
        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0].task.id, task.id);
        assert_eq!(updated[0].task.status, "deleted");
    }

    #[tokio::test]
    async fn complete_task_broadcasts_downstream_unblock() {
        let repo = Arc::new(MockTeamRepo::new());
        let (board, bc) = board_with_events(repo);

        let a = board.create_task("t1", "A", None, None, &[], None).await.unwrap();
        let b = board
            .create_task("t1", "B", None, None, std::slice::from_ref(&a.id), None)
            .await
            .unwrap();

        board
            .update_task(
                "t1",
                &a.id,
                &TaskUpdate {
                    status: Some(TaskStatus::Completed),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Completing A must broadcast B's unblock so the board/feed and the
        // downstream-owner wake path both observe B is now actionable.
        let b_updates: Vec<_> = task_changes(&bc)
            .into_iter()
            .filter(|c| c.task.id == b.id && c.change == TeamTaskChange::Updated)
            .collect();
        assert_eq!(b_updates.len(), 1, "completing A must broadcast B's unblock");
        assert!(
            b_updates[0].task.blocked_by.is_empty(),
            "B.blocked_by should be empty after A completes"
        );
    }

    #[tokio::test]
    async fn no_emitter_does_not_panic_and_emits_nothing() {
        let repo = Arc::new(MockTeamRepo::new());
        let board = TaskBoard::new(repo);
        let task = board.create_task("t1", "Build", None, None, &[], None).await.unwrap();
        board
            .update_task(
                "t1",
                &task.id,
                &TaskUpdate {
                    status: Some(TaskStatus::Completed),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        // No broadcaster attached: nothing to assert beyond not panicking.
    }

    // -- Helper ---------------------------------------------------------------

    async fn create_simple_task(board: &TaskBoard, team_id: &str, subject: &str) -> TeamTask {
        board
            .create_task(team_id, subject, None, None, &[], None)
            .await
            .unwrap()
    }

    // -- Tests ----------------------------------------------------------------

    #[tokio::test]
    async fn create_task_no_dependencies() {
        let repo = Arc::new(MockTeamRepo::new());
        let board = TaskBoard::new(repo);

        let task = create_simple_task(&board, "t1", "Implement feature").await;
        assert_eq!(task.subject, "Implement feature");
        assert_eq!(task.status, TaskStatus::Pending);
        assert!(task.blocked_by.is_empty());
        assert!(task.blocks.is_empty());
    }

    #[tokio::test]
    async fn create_task_with_owner_and_description() {
        let repo = Arc::new(MockTeamRepo::new());
        let board = TaskBoard::new(repo);

        let task = board
            .create_task("t1", "Design API", Some("REST endpoints"), Some("a1"), &[], None)
            .await
            .unwrap();
        assert_eq!(task.description.as_deref(), Some("REST endpoints"));
        assert_eq!(task.owner.as_deref(), Some("a1"));
    }

    #[tokio::test]
    async fn create_task_with_dependencies() {
        let repo = Arc::new(MockTeamRepo::new());
        let board = TaskBoard::new(repo.clone());

        let task_a = create_simple_task(&board, "t1", "Task A").await;
        let task_b = board
            .create_task("t1", "Task B", None, None, std::slice::from_ref(&task_a.id), None)
            .await
            .unwrap();

        assert_eq!(task_b.blocked_by, vec![task_a.id.clone()]);

        let updated_a = repo
            .find_task_by_id("system_default_user", "t1", &task_a.id)
            .await
            .unwrap()
            .unwrap();
        let blocks_a: Vec<String> = serde_json::from_str(&updated_a.blocks).unwrap();
        assert_eq!(blocks_a, vec![task_b.id]);
    }

    #[tokio::test]
    async fn create_task_nonexistent_dependency_fails() {
        let repo = Arc::new(MockTeamRepo::new());
        let board = TaskBoard::new(repo);

        let result = board
            .create_task("t1", "X", None, None, &["nonexistent".into()], None)
            .await;
        assert!(matches!(result, Err(TeamError::BlockedTaskNotFound(_))));
    }

    #[tokio::test]
    async fn update_task_status() {
        let repo = Arc::new(MockTeamRepo::new());
        let board = TaskBoard::new(repo);

        let task = create_simple_task(&board, "t1", "Work").await;
        let updated = board
            .update_task(
                "t1",
                &task.id,
                &TaskUpdate {
                    status: Some(TaskStatus::InProgress),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(updated.status, TaskStatus::InProgress);
    }

    #[tokio::test]
    async fn update_task_description_and_owner() {
        let repo = Arc::new(MockTeamRepo::new());
        let board = TaskBoard::new(repo);

        let task = create_simple_task(&board, "t1", "Work").await;
        let updated = board
            .update_task(
                "t1",
                &task.id,
                &TaskUpdate {
                    description: Some("New desc".into()),
                    owner: Some("a2".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(updated.description.as_deref(), Some("New desc"));
        assert_eq!(updated.owner.as_deref(), Some("a2"));
    }

    #[tokio::test]
    async fn update_nonexistent_task_fails() {
        let repo = Arc::new(MockTeamRepo::new());
        let board = TaskBoard::new(repo);

        let result = board.update_task("t1", "nonexistent", &TaskUpdate::default()).await;
        assert!(matches!(result, Err(TeamError::TaskNotFound(_))));
    }

    #[tokio::test]
    async fn complete_task_unblocks_downstream() {
        let repo = Arc::new(MockTeamRepo::new());
        let board = TaskBoard::new(repo);

        let task_a = create_simple_task(&board, "t1", "A").await;
        let task_b = board
            .create_task("t1", "B", None, None, std::slice::from_ref(&task_a.id), None)
            .await
            .unwrap();

        assert_eq!(task_b.blocked_by, vec![task_a.id.clone()]);

        board
            .update_task(
                "t1",
                &task_a.id,
                &TaskUpdate {
                    status: Some(TaskStatus::Completed),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let tasks = board.list_tasks("t1").await.unwrap();
        let b = tasks.iter().find(|t| t.id == task_b.id).unwrap();
        assert!(b.blocked_by.is_empty());
    }

    #[tokio::test]
    async fn complete_task_unblocks_multiple_downstream() {
        let repo = Arc::new(MockTeamRepo::new());
        let board = TaskBoard::new(repo);

        let task_a = create_simple_task(&board, "t1", "A").await;
        let task_b = board
            .create_task("t1", "B", None, None, std::slice::from_ref(&task_a.id), None)
            .await
            .unwrap();
        let task_c = board
            .create_task("t1", "C", None, None, std::slice::from_ref(&task_a.id), None)
            .await
            .unwrap();

        board
            .update_task(
                "t1",
                &task_a.id,
                &TaskUpdate {
                    status: Some(TaskStatus::Completed),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let tasks = board.list_tasks("t1").await.unwrap();
        let b = tasks.iter().find(|t| t.id == task_b.id).unwrap();
        let c = tasks.iter().find(|t| t.id == task_c.id).unwrap();
        assert!(b.blocked_by.is_empty());
        assert!(c.blocked_by.is_empty());
    }

    #[tokio::test]
    async fn partial_unblock_preserves_other_dependencies() {
        let repo = Arc::new(MockTeamRepo::new());
        let board = TaskBoard::new(repo);

        let task_a = create_simple_task(&board, "t1", "A").await;
        let task_x = create_simple_task(&board, "t1", "X").await;
        let task_b = board
            .create_task("t1", "B", None, None, &[task_a.id.clone(), task_x.id.clone()], None)
            .await
            .unwrap();

        assert_eq!(task_b.blocked_by.len(), 2);

        board
            .update_task(
                "t1",
                &task_a.id,
                &TaskUpdate {
                    status: Some(TaskStatus::Completed),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let tasks = board.list_tasks("t1").await.unwrap();
        let b = tasks.iter().find(|t| t.id == task_b.id).unwrap();
        assert_eq!(b.blocked_by, vec![task_x.id]);
    }

    #[tokio::test]
    async fn complete_task_no_downstream_is_noop() {
        let repo = Arc::new(MockTeamRepo::new());
        let board = TaskBoard::new(repo);

        let task = create_simple_task(&board, "t1", "Standalone").await;
        let updated = board
            .update_task(
                "t1",
                &task.id,
                &TaskUpdate {
                    status: Some(TaskStatus::Completed),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(updated.status, TaskStatus::Completed);
    }

    #[tokio::test]
    async fn list_tasks_empty() {
        let repo = Arc::new(MockTeamRepo::new());
        let board = TaskBoard::new(repo);

        let tasks = board.list_tasks("t1").await.unwrap();
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn list_tasks_returns_all() {
        let repo = Arc::new(MockTeamRepo::new());
        let board = TaskBoard::new(repo);

        create_simple_task(&board, "t1", "A").await;
        create_simple_task(&board, "t1", "B").await;
        create_simple_task(&board, "t2", "C").await;

        let tasks = board.list_tasks("t1").await.unwrap();
        assert_eq!(tasks.len(), 2);
    }

    // -- Phase 3a: input_context build/cap (pure helpers) ---------------------

    fn dep_row(id: &str, created: i64, result: Option<&str>) -> TeamTaskRow {
        TeamTaskRow {
            id: id.into(),
            team_id: "t1".into(),
            subject: id.into(),
            description: None,
            status: "completed".into(),
            owner: None,
            blocked_by: "[]".into(),
            blocks: r#"["B"]"#.into(),
            metadata: None,
            created_at: created,
            updated_at: created,
            engagement_id: None,
            expected_output: None,
            result: result.map(str::to_owned),
            input_context: None,
        }
    }

    fn ready_task() -> TeamTaskRow {
        TeamTaskRow {
            id: "B".into(),
            team_id: "t1".into(),
            subject: "Do B".into(),
            description: None,
            status: "pending".into(),
            owner: None,
            blocked_by: "[]".into(),
            blocks: "[]".into(),
            metadata: None,
            created_at: 0,
            updated_at: 0,
            engagement_id: None,
            expected_output: None,
            result: None,
            input_context: None,
        }
    }

    #[test]
    fn build_input_context_keeps_every_dep_under_the_cap() {
        let deps = [
            dep_row("A", 1, Some("oldest result")),
            dep_row("X", 2, Some("newest result")),
        ];
        let refs: Vec<&TeamTaskRow> = deps.iter().collect();
        let ctx = build_input_context(&ready_task(), &refs);
        assert!(ctx.contains("- A: oldest result"), "{ctx}");
        assert!(ctx.contains("- X: newest result"), "{ctx}");
        assert!(!ctx.contains("truncated"), "no overflow, nothing dropped:\n{ctx}");
    }

    #[test]
    fn cap_keeps_newest_dep_and_drops_oldest_on_overflow() {
        let big = "y".repeat(MAX_INPUT_CONTEXT_CHARS);
        let deps = [dep_row("A", 1, Some(&big)), dep_row("X", 2, Some("newest short"))];
        let refs: Vec<&TeamTaskRow> = deps.iter().collect();
        let ctx = build_input_context(&ready_task(), &refs);
        assert!(ctx.len() <= MAX_INPUT_CONTEXT_CHARS, "cap respected: {}", ctx.len());
        assert!(ctx.contains("newest short"), "newest dep kept:\n{ctx}");
        assert!(!ctx.contains('y'), "oldest (huge) dep dropped:\n{ctx}");
        assert!(ctx.contains("truncated"), "overflow recorded:\n{ctx}");
    }
}
