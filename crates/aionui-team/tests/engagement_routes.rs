//! Phase 5a Task 2 — engagement list / create(find-or-create) / manage(PATCH)
//! service surface + its ownership and cross-team / cross-user guards.
//!
//! Service-level (aionui-team has no axum router test harness; team routes are
//! validated through the service, mirroring `engagement_members.rs`). These
//! exercise the REAL `SqliteTeamRepository` via `init_database_memory()` and
//! the real `ensure_engagement`/`list_engagements_for_team`/
//! `update_team_engagement`, so the security assertions are against production
//! data-scoping, not a mock:
//!
//! - list returns only the caller's team engagements; an unowned team is
//!   indistinguishable from a missing one (`TeamNotFound`, no existence leak).
//! - create (find-or-create) reuses the SAME engagement for (team, project) and
//!   mints a DIFFERENT one for a second project.
//! - PATCH process/status is reflected; an archived engagement stays listable.
//! - PATCH an engagement that belongs to a DIFFERENT team (same user) or to
//!   ANOTHER user → `TeamNotFound` and the row is unchanged (the 2a IDOR
//!   carry-forward: no unguarded engagement_id write path).
//! - invalid process value → `InvalidRequest`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use aionui_ai_agent::agent_task::AgentInstance;
use aionui_ai_agent::types::BuildTaskOptions;
use aionui_ai_agent::{AgentError, IWorkerTaskManager};
use aionui_api_types::{
    CreateTeamRequest, GetConfigOptionsResponse, TeamAgentInput, TeamMcpSelection, WebSocketMessage,
};
use aionui_common::{AgentKillReason, TimestampMs};
use aionui_db::models::{MailboxMessageRow, MessageRow, TeamEngagementMemberRow, TeamTaskRow};
use aionui_db::{
    ActivityCursor, ITeamRepository, PageDirection, SqliteAgentMetadataRepository, SqliteAssistantDefinitionRepository,
    SqliteAssistantOverlayRepository, SqliteProjectStore, SqliteProviderRepository, SqliteTeamRepository,
    init_database_memory,
};
use aionui_project::{ProjectService, canonical};
use aionui_realtime::EventBroadcaster;
use aionui_team::ActivityKind;
use aionui_team::ports::{
    AgentTurnCancellationPort, AgentTurnExecutionError, AgentTurnExecutionPort, AgentTurnOutcome, AgentTurnRequest,
    AgentTurnStatus, TeamAssistantCatalogEntry, TeamAssistantCatalogPort,
};
use aionui_team::{
    TeamConversationCreateRequest, TeamConversationCreateResult, TeamConversationProvisioningPort, TeamError,
    TeamMcpSnapshotResolution, TeamProjectionMessageStore, TeamSessionService,
};
use async_trait::async_trait;

// ── Test doubles: only the conversation-store seam (composition layer) ───────

struct NullBroadcaster;
impl EventBroadcaster for NullBroadcaster {
    fn broadcast(&self, _msg: WebSocketMessage<serde_json::Value>) {}
}

struct NoopTurnPort;
#[async_trait]
impl AgentTurnExecutionPort for NoopTurnPort {
    async fn run_agent_turn(&self, request: AgentTurnRequest) -> Result<AgentTurnOutcome, AgentTurnExecutionError> {
        Ok(AgentTurnOutcome {
            conversation_id: request.conversation_id,
            turn_id: "turn-noop".into(),
            status: AgentTurnStatus::Completed,
            runtime: None,
        })
    }
}

struct NoopCancellationPort;
#[async_trait]
impl AgentTurnCancellationPort for NoopCancellationPort {
    async fn cancel_agent_turn(
        &self,
        _user_id: &str,
        _conversation_id: &str,
        _turn_id: &str,
    ) -> Result<(), AgentTurnExecutionError> {
        Ok(())
    }
}

#[derive(Default)]
struct NoopTaskManager;
#[async_trait]
impl IWorkerTaskManager for NoopTaskManager {
    fn get_task(&self, _conversation_id: &str) -> Option<AgentInstance> {
        None
    }
    async fn get_or_build_task(&self, _: &str, _: BuildTaskOptions) -> Result<AgentInstance, AgentError> {
        Err(AgentError::internal("noop"))
    }
    fn kill(&self, _c: &str, _r: Option<AgentKillReason>) -> Result<(), AgentError> {
        Ok(())
    }
    fn kill_and_wait(
        &self,
        _c: &str,
        _r: Option<AgentKillReason>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(std::future::ready(()))
    }
    async fn clear(&self) {}
    fn active_count(&self) -> usize {
        0
    }
    fn collect_idle(&self, _: TimestampMs) -> Vec<String> {
        Vec::new()
    }
}

struct EmptyCatalog;
#[async_trait]
impl TeamAssistantCatalogPort for EmptyCatalog {
    async fn list_team_selectable_assistants(
        &self,
        _user_id: &str,
    ) -> Result<Vec<TeamAssistantCatalogEntry>, TeamError> {
        Ok(Vec::new())
    }
}

#[derive(Default)]
struct RecordingConversationPort {
    extras: Mutex<HashMap<String, serde_json::Value>>,
}

impl RecordingConversationPort {
    fn merge_extra(&self, conversation_id: &str, patch: serde_json::Value) -> Result<(), TeamError> {
        let mut extras = self.extras.lock().unwrap();
        let extra = extras
            .get_mut(conversation_id)
            .ok_or_else(|| TeamError::AgentNotFound(conversation_id.to_owned()))?;
        if let (Some(target), Some(source)) = (extra.as_object_mut(), patch.as_object()) {
            for (key, value) in source {
                target.insert(key.clone(), value.clone());
            }
        }
        Ok(())
    }
}

#[async_trait]
impl TeamConversationProvisioningPort for RecordingConversationPort {
    async fn create_team_conversation(
        &self,
        request: TeamConversationCreateRequest,
    ) -> Result<TeamConversationCreateResult, TeamError> {
        let id = aionui_common::generate_id();
        let mut extra = request.extra;
        let workspace = match extra.get("workspace").and_then(serde_json::Value::as_str) {
            Some(value) if !value.trim().is_empty() => value.to_owned(),
            _ => self.create_team_temp_workspace(&request.user_id, "api").await?,
        };
        extra["workspace"] = serde_json::Value::String(workspace.clone());
        self.extras.lock().unwrap().insert(id.clone(), extra);
        Ok(TeamConversationCreateResult {
            conversation_id: id,
            workspace,
        })
    }

    async fn conversation_workspace(&self, conversation_id: &str) -> Result<Option<String>, TeamError> {
        Ok(self
            .extras
            .lock()
            .unwrap()
            .get(conversation_id)
            .and_then(|extra| extra.get("workspace"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned))
    }

    async fn conversation_assistant_id(&self, conversation_id: &str) -> Result<Option<String>, TeamError> {
        Ok(self.extras.lock().unwrap().get(conversation_id).and_then(|extra| {
            extra
                .get("assistant_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        }))
    }

    async fn update_conversation_project_binding(
        &self,
        conversation_id: &str,
        _project_id: Option<String>,
        _folder_id: Option<String>,
        workspace: Option<String>,
    ) -> Result<(), TeamError> {
        match workspace {
            Some(workspace) => self.merge_extra(conversation_id, serde_json::json!({ "workspace": workspace })),
            None => Ok(()),
        }
    }

    async fn create_team_temp_workspace(&self, _user_id: &str, tag: &str) -> Result<String, TeamError> {
        let path = unique_temp_dir(&format!("engagement-api-temp-{tag}"));
        std::fs::create_dir_all(&path).unwrap();
        Ok(path.to_string_lossy().into_owned())
    }

    async fn patch_runtime_config(&self, conversation_id: &str, patch: serde_json::Value) -> Result<(), TeamError> {
        self.merge_extra(conversation_id, patch)
    }

    async fn save_acp_runtime_mode(&self, conversation_id: &str, mode: &str) -> Result<(), TeamError> {
        self.patch_runtime_config(conversation_id, serde_json::json!({ "session_mode": mode }))
            .await
    }

    async fn get_config_options(&self, _conversation_id: &str) -> Result<GetConfigOptionsResponse, TeamError> {
        Ok(GetConfigOptionsResponse {
            config_options: Vec::new(),
        })
    }

    async fn warmup_agent_process(
        &self,
        _user_id: &str,
        _conversation_id: &str,
        _task_manager: &Arc<dyn IWorkerTaskManager>,
    ) -> Result<(), TeamError> {
        Ok(())
    }

    async fn resolve_assistant_mcp_selection(
        &self,
        _user_id: &str,
        _assistant_id: &str,
    ) -> Result<Option<TeamMcpSelection>, TeamError> {
        Ok(Some(TeamMcpSelection::default()))
    }

    async fn resolve_conversation_mcp_snapshot(
        &self,
        _user_id: &str,
        _conversation_id: &str,
        _assistant_id: Option<&str>,
    ) -> Result<TeamMcpSnapshotResolution, TeamError> {
        Ok(TeamMcpSnapshotResolution::default())
    }

    async fn delete_team_conversation(&self, _user_id: &str, conversation_id: &str) -> Result<(), TeamError> {
        self.extras
            .lock()
            .unwrap()
            .remove(conversation_id)
            .map(|_| ())
            .ok_or_else(|| TeamError::AgentNotFound(conversation_id.to_owned()))
    }
}

#[async_trait]
impl TeamProjectionMessageStore for RecordingConversationPort {
    fn mint_message_id(&self) -> String {
        aionui_common::generate_id()
    }
    async fn find_projected_message(&self, _c: &str, _m: &str, _t: &str) -> Result<Option<MessageRow>, TeamError> {
        Ok(None)
    }
    async fn insert_projected_message(&self, _row: &MessageRow) -> Result<(), TeamError> {
        Ok(())
    }
}

// ── Harness ─────────────────────────────────────────────────────────────────

struct Harness {
    repo: Arc<SqliteTeamRepository>,
    svc: Arc<TeamSessionService>,
    projects: Arc<ProjectService>,
    _db: aionui_db::Database,
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{prefix}-{}", aionui_common::generate_id()))
}

impl Harness {
    async fn new(user: &str) -> Self {
        let db = init_database_memory().await.unwrap();
        let pool = db.pool().clone();
        sqlx::query(
            "INSERT INTO users (id, user_type, username, password_hash, status, session_generation, created_at, updated_at) \
             VALUES (?1, 'local', ?1, 'hash', 'active', 0, 1, 1)",
        )
        .bind(user)
        .execute(&pool)
        .await
        .unwrap();

        let repo = Arc::new(SqliteTeamRepository::new(pool.clone()));
        let port = Arc::new(RecordingConversationPort::default());
        let svc = TeamSessionService::new(
            Arc::clone(&repo) as Arc<dyn ITeamRepository>,
            Arc::new(SqliteAgentMetadataRepository::new(pool.clone())),
            Arc::new(EmptyCatalog),
            Arc::new(SqliteAssistantDefinitionRepository::new(pool.clone())),
            Arc::new(SqliteAssistantOverlayRepository::new(pool.clone())),
            Arc::new(SqliteProviderRepository::new(pool.clone())),
            Arc::clone(&port) as Arc<dyn TeamConversationProvisioningPort>,
            port.clone() as Arc<dyn TeamProjectionMessageStore>,
            Arc::new(NullBroadcaster),
            Arc::new(NoopTaskManager),
            Arc::new(NoopTurnPort),
            Arc::new(NoopCancellationPort),
            Arc::new(PathBuf::from("/bin/true")),
        );
        let projects = Arc::new(ProjectService::new(
            Arc::new(SqliteProjectStore::new(pool.clone())) as Arc<dyn aionui_db::IProjectStore>,
            unique_temp_dir("engagement-api-projects"),
        ));
        Self {
            repo,
            svc,
            projects,
            _db: db,
        }
    }

    /// A real 1-member (lead) team created through the service, project unbound.
    async fn create_team(&self, user: &str, name: &str) -> aionui_api_types::TeamResponse {
        let workspace = unique_temp_dir("engagement-api-team");
        std::fs::create_dir_all(&workspace).unwrap();
        self.svc
            .create_team(
                user,
                CreateTeamRequest {
                    name: name.into(),
                    agents: vec![TeamAgentInput {
                        name: "Lead".into(),
                        role: "lead".into(),
                        backend: Some("acp".into()),
                        model: "claude".into(),
                        assistant_id: None,
                        conversation_id: None,
                    }],
                    workspace: Some(workspace.to_string_lossy().into_owned()),
                    project_id: None,
                },
            )
            .await
            .unwrap()
    }

    async fn create_project(&self, user: &str) -> String {
        self.svc.with_project_service(self.projects.clone());
        let dir = unique_temp_dir("engagement-api-proj");
        std::fs::create_dir_all(&dir).unwrap();
        self.projects
            .create_standard(user, canonical::to_file_uri(&dir).unwrap())
            .await
            .unwrap()
            .project
            .project_id
    }

    async fn engagement_members(&self, user: &str, engagement: &str) -> Vec<TeamEngagementMemberRow> {
        self.repo.list_engagement_members(user, engagement).await.unwrap()
    }
}

// ── list ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn list_returns_callers_engagements_and_hides_unowned_team() {
    let owner = "user-api-list";
    let stranger = "user-api-list-stranger";
    let h = Harness::new(owner).await;
    let team = h.create_team(owner, "Listed").await;
    let project = h.create_project(owner).await;

    let created = h.svc.ensure_engagement(owner, &team.id, &project).await.unwrap();
    let listed = h.svc.list_engagements_for_team(owner, &team.id).await.unwrap();

    assert!(
        listed.iter().any(|e| e.id == created.id && e.team_id == team.id),
        "caller sees their own engagement: {listed:?}"
    );
    // An unowned team is indistinguishable from a missing one (no existence leak).
    let err = h
        .svc
        .list_engagements_for_team(stranger, &team.id)
        .await
        .expect_err("stranger cannot list another user's team engagements");
    assert!(
        matches!(err, TeamError::TeamNotFound(_)),
        "unowned team must surface as TeamNotFound, got {err:?}"
    );
}

// ── create (find-or-create) ────────────────────────────────────────────────

#[tokio::test]
async fn create_is_find_or_create_and_mints_distinct_per_project() {
    let user = "user-api-create";
    let h = Harness::new(user).await;
    let team = h.create_team(user, "FindOrCreate").await;
    let p1 = h.create_project(user).await;
    let p2 = h.create_project(user).await;

    let first = h.svc.ensure_engagement(user, &team.id, &p1).await.unwrap();
    let again = h.svc.ensure_engagement(user, &team.id, &p1).await.unwrap();
    let second_project = h.svc.ensure_engagement(user, &team.id, &p2).await.unwrap();

    assert_eq!(first.id, again.id, "same (team, project) reuses one engagement");
    assert_ne!(
        first.id, second_project.id,
        "a different project mints a different engagement"
    );
    assert_eq!(again.project_id, p1);
    assert_eq!(again.team_id, team.id);

    // Members are materialized for the engagement (find-or-create reuses them too).
    assert!(
        !h.engagement_members(user, &first.id).await.is_empty(),
        "ensure_engagement materializes the engagement's members"
    );
    let list = h.svc.list_engagements_for_team(user, &team.id).await.unwrap();
    let for_p1: Vec<_> = list.iter().filter(|e| e.project_id == p1).collect();
    assert_eq!(for_p1.len(), 1, "no duplicate engagement for the same project");
}

// ── patch (manage) ──────────────────────────────────────────────────────────

#[tokio::test]
async fn patch_updates_process_and_archived_stays_listable() {
    let user = "user-api-patch";
    let h = Harness::new(user).await;
    let team = h.create_team(user, "Patched").await;
    let project = h.create_project(user).await;
    let engagement = h.svc.ensure_engagement(user, &team.id, &project).await.unwrap();

    let after_process = h
        .svc
        .update_team_engagement(user, &team.id, &engagement.id, Some("sequential"), None)
        .await
        .unwrap();
    assert_eq!(after_process.process, "sequential", "process reflected in response");

    let after_status = h
        .svc
        .update_team_engagement(user, &team.id, &engagement.id, Some("hierarchical"), Some("archived"))
        .await
        .unwrap();
    assert_eq!(after_status.process, "hierarchical");
    assert_eq!(after_status.status, "archived");

    // Persisted, not just returned.
    let row = h
        .repo
        .find_engagement_by_id(user, &engagement.id)
        .await
        .unwrap()
        .expect("engagement still present");
    assert_eq!(row.process, "hierarchical");
    assert_eq!(row.status, "archived");

    // An archived engagement is still listable (spec §6 archived reactivates ⇒ it stays visible).
    let list = h.svc.list_engagements_for_team(user, &team.id).await.unwrap();
    assert!(
        list.iter().any(|e| e.id == engagement.id && e.status == "archived"),
        "archived engagement remains listable: {list:?}"
    );
}

#[tokio::test]
async fn patch_engagement_from_other_team_is_not_found_and_unchanged() {
    let user = "user-api-crossteam";
    let h = Harness::new(user).await;
    let team_a = h.create_team(user, "A").await;
    let team_b = h.create_team(user, "B").await;
    let project = h.create_project(user).await;
    // Same project, different teams ⇒ two distinct engagements.
    let eng_a = h.svc.ensure_engagement(user, &team_a.id, &project).await.unwrap();
    h.svc.ensure_engagement(user, &team_b.id, &project).await.unwrap();
    assert_ne!(eng_a.id, {
        // sanity: the two teams' engagements differ even for the same project
        h.repo
            .find_engagement(user, &team_b.id, &project)
            .await
            .unwrap()
            .unwrap()
            .id
    });

    // PATCH team B with team A's engagement id → rejected, and A unchanged.
    let err = h
        .svc
        .update_team_engagement(user, &team_b.id, &eng_a.id, Some("sequential"), None)
        .await
        .expect_err("engagement must belong to the :id team");
    assert!(
        matches!(err, TeamError::TeamNotFound(_)),
        "cross-team write must 404, got {err:?}"
    );
    let row = h
        .repo
        .find_engagement_by_id(user, &eng_a.id)
        .await
        .unwrap()
        .expect("engagement still present");
    assert_ne!(row.process, "sequential", "cross-team PATCH must not mutate the row");
}

#[tokio::test]
async fn patch_engagement_you_do_not_own_is_not_found_and_unchanged() {
    let owner = "user-api-owner";
    let attacker = "user-api-attacker";
    let h = Harness::new(owner).await;
    let team = h.create_team(owner, "Victim").await;
    let project = h.create_project(owner).await;
    let engagement = h.svc.ensure_engagement(owner, &team.id, &project).await.unwrap();

    // Attacker (different user) targets the victim's team + engagement id.
    let err = h
        .svc
        .update_team_engagement(attacker, &team.id, &engagement.id, Some("sequential"), None)
        .await
        .expect_err("attacker cannot patch a team they do not own");
    assert!(
        matches!(err, TeamError::TeamNotFound(_)),
        "cross-user write must 404, got {err:?}"
    );
    let row = h
        .repo
        .find_engagement_by_id(owner, &engagement.id)
        .await
        .unwrap()
        .expect("engagement still present");
    assert_ne!(row.process, "sequential", "cross-user PATCH must not mutate the row");
}

#[tokio::test]
async fn patch_invalid_process_is_bad_request() {
    let user = "user-api-invalid";
    let h = Harness::new(user).await;
    let team = h.create_team(user, "Invalid").await;
    let project = h.create_project(user).await;
    let engagement = h.svc.ensure_engagement(user, &team.id, &project).await.unwrap();

    let err = h
        .svc
        .update_team_engagement(user, &team.id, &engagement.id, Some("diagonal"), None)
        .await
        .expect_err("invalid process must be rejected");
    assert!(
        matches!(err, TeamError::InvalidRequest(_)),
        "invalid process must 400, got {err:?}"
    );
    // Unchanged.
    let row = h
        .repo
        .find_engagement_by_id(user, &engagement.id)
        .await
        .unwrap()
        .expect("engagement still present");
    assert_ne!(row.process, "diagonal");
}

#[tokio::test]
async fn patch_unknown_engagement_is_not_found() {
    let user = "user-api-unknown";
    let h = Harness::new(user).await;
    let team = h.create_team(user, "Unknown").await;
    h.create_project(user).await;

    let err = h
        .svc
        .update_team_engagement(user, &team.id, "no-such-engagement", Some("sequential"), None)
        .await
        .expect_err("unknown engagement id must be rejected");
    assert!(
        matches!(err, TeamError::TeamNotFound(_)),
        "unknown engagement must 404, got {err:?}"
    );
}

// ── engagement-scoped reads (members / tasks / mailbox) — Phase 5a Task 3 ────
//
// The point of the whole feature (spec §12): two engagements of ONE team must
// never see each other's rows through the engagement-scoped reads. Every read
// route goes through the SAME `load_owned_engagement` guard as the PATCH, so a
// cross-team or cross-user `:engagement_id` is `TeamNotFound` before any row is
// touched. Assertions compare SPECIFIC per-engagement ids (not just "non-empty")
// so a shared-row leak fails the test.

impl Harness {
    /// Seeds one task on `engagement`'s board (with expected_output + result).
    async fn seed_task(&self, user: &str, team_id: &str, engagement: &str, task_id: &str) {
        self.repo
            .create_task(
                user,
                &TeamTaskRow {
                    id: task_id.into(),
                    team_id: team_id.into(),
                    subject: format!("task for {engagement}"),
                    description: None,
                    status: "pending".into(),
                    owner: None,
                    blocked_by: "[]".into(),
                    blocks: "[]".into(),
                    metadata: None,
                    created_at: 1,
                    updated_at: 1,
                    engagement_id: Some(engagement.into()),
                    expected_output: Some(format!("expected of {task_id}")),
                    result: Some(format!("result of {task_id}")),
                    input_context: None,
                },
            )
            .await
            .unwrap();
    }

    /// Seeds one mailbox message on `engagement`.
    async fn seed_message(&self, user: &str, team_id: &str, engagement: &str, msg_id: &str) {
        self.repo
            .write_message(
                user,
                &MailboxMessageRow {
                    id: msg_id.into(),
                    team_id: team_id.into(),
                    to_agent_id: format!("to-{engagement}"),
                    from_agent_id: "lead".into(),
                    msg_type: "message".into(),
                    content: format!("mail for {engagement}"),
                    summary: None,
                    files: None,
                    read: false,
                    created_at: 1,
                    engagement_id: Some(engagement.into()),
                },
            )
            .await
            .unwrap();
    }

    /// Team engaged in two projects; returns the two engagement ids.
    async fn two_engagements(&self, user: &str, name: &str) -> (String, String, String) {
        let team = self.create_team(user, name).await;
        let p1 = self.create_project(user).await;
        let p2 = self.create_project(user).await;
        let e1 = self.svc.ensure_engagement(user, &team.id, &p1).await.unwrap();
        let e2 = self.svc.ensure_engagement(user, &team.id, &p2).await.unwrap();
        assert_ne!(e1.id, e2.id, "two projects yield two distinct engagements");
        (team.id, e1.id, e2.id)
    }

    /// Seeds a task on `engagement` with an explicit `created_at` (for cursor
    /// ordering), distinct from the fixed-ts `seed_task`.
    async fn seed_task_at(&self, user: &str, team_id: &str, engagement: &str, task_id: &str, ts: i64) {
        self.repo
            .create_task(
                user,
                &TeamTaskRow {
                    id: task_id.into(),
                    team_id: team_id.into(),
                    subject: format!("task {task_id}"),
                    description: None,
                    status: "pending".into(),
                    owner: None,
                    blocked_by: "[]".into(),
                    blocks: "[]".into(),
                    metadata: None,
                    created_at: ts,
                    updated_at: ts,
                    engagement_id: Some(engagement.into()),
                    expected_output: None,
                    result: None,
                    input_context: None,
                },
            )
            .await
            .unwrap();
    }

    /// Seeds a mailbox message on `engagement` with an explicit `created_at`.
    async fn seed_message_at(&self, user: &str, team_id: &str, engagement: &str, msg_id: &str, ts: i64) {
        self.repo
            .write_message(
                user,
                &MailboxMessageRow {
                    id: msg_id.into(),
                    team_id: team_id.into(),
                    to_agent_id: "worker".into(),
                    from_agent_id: "lead".into(),
                    msg_type: "message".into(),
                    content: format!("mail {msg_id}"),
                    summary: None,
                    files: None,
                    read: false,
                    created_at: ts,
                    engagement_id: Some(engagement.into()),
                },
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn members_are_isolated_per_engagement() {
    let user = "user-read-members";
    let h = Harness::new(user).await;
    let (team, e1, e2) = h.two_engagements(user, "IsoMembers").await;

    let m1 = h.svc.list_engagement_members(user, &team, &e1).await.unwrap();
    let m2 = h.svc.list_engagement_members(user, &team, &e2).await.unwrap();
    assert!(!m1.is_empty() && !m2.is_empty(), "each engagement has members");

    // e1's rows must be exactly e1's; none of e2's slot/conversation ids leak in.
    let c1: std::collections::HashSet<_> = m1.iter().map(|m| m.conversation_id.clone()).collect();
    let c2: std::collections::HashSet<_> = m2.iter().map(|m| m.conversation_id.clone()).collect();
    let s1: std::collections::HashSet<_> = m1.iter().map(|m| m.slot_id.clone()).collect();
    let s2: std::collections::HashSet<_> = m2.iter().map(|m| m.slot_id.clone()).collect();
    assert!(
        c1.is_disjoint(&c2),
        "e1 and e2 conversation_ids must be disjoint: {c1:?} vs {c2:?}"
    );
    assert!(
        s1.is_disjoint(&s2),
        "e1 and e2 slot_ids must be disjoint: {s1:?} vs {s2:?}"
    );

    // Cross-check against the raw repo: each route returns ONLY its engagement.
    let e1_convos = h.engagement_members(user, &e1).await;
    assert_eq!(
        c1,
        e1_convos.iter().map(|r| r.conversation_id.clone()).collect(),
        "e1 members route returns exactly e1's rows"
    );
}

#[tokio::test]
async fn tasks_are_isolated_per_engagement_and_include_expected_output_result() {
    let user = "user-read-tasks";
    let h = Harness::new(user).await;
    let (team, e1, e2) = h.two_engagements(user, "IsoTasks").await;
    h.seed_task(user, &team, &e1, "task-e1").await;
    h.seed_task(user, &team, &e2, "task-e2").await;

    let t1 = h.svc.list_engagement_tasks(user, &team, &e1).await.unwrap();
    let t2 = h.svc.list_engagement_tasks(user, &team, &e2).await.unwrap();

    assert_eq!(
        t1.iter().map(|t| t.id.clone()).collect::<Vec<_>>(),
        vec!["task-e1".to_string()],
        "e1 tasks returns ONLY e1's task"
    );
    assert_eq!(
        t2.iter().map(|t| t.id.clone()).collect::<Vec<_>>(),
        vec!["task-e2".to_string()],
        "e2 tasks returns ONLY e2's task"
    );
    // The engagement-scoped projection carries expected_output + result.
    assert_eq!(t1[0].expected_output.as_deref(), Some("expected of task-e1"));
    assert_eq!(t1[0].result.as_deref(), Some("result of task-e1"));
}

#[tokio::test]
async fn mailbox_is_isolated_per_engagement() {
    let user = "user-read-mailbox";
    let h = Harness::new(user).await;
    let (team, e1, e2) = h.two_engagements(user, "IsoMailbox").await;
    h.seed_message(user, &team, &e1, "mail-e1").await;
    h.seed_message(user, &team, &e2, "mail-e2").await;

    let m1 = h.svc.list_engagement_mailbox(user, &team, &e1).await.unwrap();
    let m2 = h.svc.list_engagement_mailbox(user, &team, &e2).await.unwrap();
    assert_eq!(
        m1.iter().map(|m| m.id.clone()).collect::<Vec<_>>(),
        vec!["mail-e1".to_string()],
        "e1 mailbox returns ONLY e1's message"
    );
    assert_eq!(
        m2.iter().map(|m| m.id.clone()).collect::<Vec<_>>(),
        vec!["mail-e2".to_string()],
        "e2 mailbox returns ONLY e2's message"
    );
}

#[tokio::test]
async fn empty_engagement_reads_return_empty_lists() {
    let user = "user-read-empty";
    let h = Harness::new(user).await;
    let (team, e1, _) = h.two_engagements(user, "EmptyReads").await;
    // No tasks/mailbox seeded on e1.
    assert!(h.svc.list_engagement_tasks(user, &team, &e1).await.unwrap().is_empty());
    assert!(
        h.svc
            .list_engagement_mailbox(user, &team, &e1)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn reads_reject_cross_user_and_foreign_team_engagement_id() {
    let owner = "user-read-owner";
    let attacker = "user-read-attacker";
    let h = Harness::new(owner).await;
    let (team, e1, _) = h.two_engagements(owner, "GuardedReads").await;
    h.seed_task(owner, &team, &e1, "task-e1").await;

    // Cross-user: attacker reads the victim's engagement under the victim's team.
    for err in [
        h.svc.list_engagement_members(attacker, &team, &e1).await.unwrap_err(),
        h.svc.list_engagement_tasks(attacker, &team, &e1).await.unwrap_err(),
        h.svc.list_engagement_mailbox(attacker, &team, &e1).await.unwrap_err(),
    ] {
        assert!(
            matches!(err, TeamError::TeamNotFound(_)),
            "cross-user read must 404, got {err:?}"
        );
    }

    // Foreign team: same user, a different team id than the engagement's owner.
    let other_team = h.create_team(owner, "Other").await;
    for err in [
        h.svc
            .list_engagement_members(owner, &other_team.id, &e1)
            .await
            .unwrap_err(),
        h.svc
            .list_engagement_tasks(owner, &other_team.id, &e1)
            .await
            .unwrap_err(),
        h.svc
            .list_engagement_mailbox(owner, &other_team.id, &e1)
            .await
            .unwrap_err(),
    ] {
        assert!(
            matches!(err, TeamError::TeamNotFound(_)),
            "engagement id bound to a foreign :id team must 404, got {err:?}"
        );
    }
}

// ── engagement-scoped unified activity feed (list_engagement_activity) ───────
//
// Phase 5c Task 2: the engagement twin of `list_team_activity`. It must produce
// the SAME merged/cursor/limit/direction shape, but ONLY for one engagement's
// rows, behind the SAME `load_owned_engagement` ownership guard. Assertions use
// SPECIFIC ids (not just "non-empty") so a cross-engagement leak or a divergent
// cursor scheme fails the test.

#[tokio::test]
async fn engagement_activity_returns_only_its_engagement_merged_ordered() {
    let user = "user-act-iso";
    let h = Harness::new(user).await;
    let (team, e1, e2) = h.two_engagements(user, "ActIso").await;
    // e1: interleaved mail@1000,3000 + task@2000,4000; e2 gets distinct rows.
    h.seed_message_at(user, &team, &e1, "m-e1-1", 1000).await;
    h.seed_message_at(user, &team, &e1, "m-e1-2", 3000).await;
    h.seed_task_at(user, &team, &e1, "k-e1-1", 2000).await;
    h.seed_task_at(user, &team, &e1, "k-e1-2", 4000).await;
    h.seed_message_at(user, &team, &e2, "m-e2-1", 5000).await;
    h.seed_task_at(user, &team, &e2, "k-e2-1", 6000).await;

    let page = h
        .svc
        .list_engagement_activity(user, &team, &e1, None, PageDirection::Desc, ActivityKind::All, 10)
        .await
        .unwrap();
    // desc merge of e1 only: k-e1-2(4000), m-e1-2(3000), k-e1-1(2000), m-e1-1(1000).
    assert_eq!(
        page.items.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
        ["k-e1-2", "m-e1-2", "k-e1-1", "m-e1-1"],
        "e1 page must be e1's merged rows, desc-ordered, nothing else"
    );
    assert!(!page.has_more, "all e1 rows fit within limit");

    // e2 is fully disjoint — no e1 id appears in e2's page and vice versa.
    let page2 = h
        .svc
        .list_engagement_activity(user, &team, &e2, None, PageDirection::Desc, ActivityKind::All, 10)
        .await
        .unwrap();
    assert_eq!(
        page2.items.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
        ["k-e2-1", "m-e2-1"],
        "e2 page returns ONLY e2's rows"
    );
}

#[tokio::test]
async fn engagement_activity_paginates_via_next_cursor_excluding_prior_page() {
    let user = "user-act-cursor";
    let h = Harness::new(user).await;
    let (team, e1, _) = h.two_engagements(user, "ActCursor").await;
    h.seed_message_at(user, &team, &e1, "m1", 1000).await;
    h.seed_message_at(user, &team, &e1, "m2", 3000).await;
    h.seed_task_at(user, &team, &e1, "k1", 2000).await;
    h.seed_task_at(user, &team, &e1, "k2", 4000).await;

    let page = h
        .svc
        .list_engagement_activity(user, &team, &e1, None, PageDirection::Desc, ActivityKind::All, 3)
        .await
        .unwrap();
    // desc top-3: k2(4000), m2(3000), k1(2000); m1(1000) deferred.
    assert_eq!(
        page.items.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
        ["k2", "m2", "k1"]
    );
    assert!(page.has_more);
    let c = page.next_cursor.expect("has_more implies a next cursor");
    assert_eq!(
        (c.ts, c.id.as_str()),
        (2000, "k1"),
        "cursor is the last item's (ts, id)"
    );

    let page2 = h
        .svc
        .list_engagement_activity(
            user,
            &team,
            &e1,
            Some(ActivityCursor {
                created_at: c.ts,
                id: c.id,
            }),
            PageDirection::Desc,
            ActivityKind::All,
            3,
        )
        .await
        .unwrap();
    assert_eq!(
        page2.items.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
        ["m1"],
        "second page excludes every page-1 id"
    );
    assert!(!page2.has_more);
}

#[tokio::test]
async fn engagement_activity_honors_kind_filter_and_direction() {
    let user = "user-act-kind";
    let h = Harness::new(user).await;
    let (team, e1, _) = h.two_engagements(user, "ActKind").await;
    h.seed_message_at(user, &team, &e1, "m1", 1000).await;
    h.seed_task_at(user, &team, &e1, "k1", 2000).await;
    h.seed_message_at(user, &team, &e1, "m2", 3000).await;

    let tasks_only = h
        .svc
        .list_engagement_activity(user, &team, &e1, None, PageDirection::Desc, ActivityKind::Task, 10)
        .await
        .unwrap();
    assert_eq!(
        tasks_only.items.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
        ["k1"],
        "kind=task returns only tasks"
    );

    let msgs_only = h
        .svc
        .list_engagement_activity(user, &team, &e1, None, PageDirection::Desc, ActivityKind::Message, 10)
        .await
        .unwrap();
    assert_eq!(
        msgs_only.items.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
        ["m2", "m1"],
        "kind=message returns only messages"
    );

    let asc = h
        .svc
        .list_engagement_activity(user, &team, &e1, None, PageDirection::Asc, ActivityKind::All, 10)
        .await
        .unwrap();
    assert_eq!(
        asc.items.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
        ["m1", "k1", "m2"],
        "direction=asc orders oldest first"
    );
}

#[tokio::test]
async fn engagement_activity_rejects_foreign_team_and_cross_user() {
    let owner = "user-act-owner";
    let attacker = "user-act-attacker";
    let h = Harness::new(owner).await;
    let (team, e1, _) = h.two_engagements(owner, "ActGuard").await;
    h.seed_task_at(owner, &team, &e1, "k-e1", 2000).await;

    // Cross-user: attacker reads the victim's engagement under the victim's team.
    let err = h
        .svc
        .list_engagement_activity(attacker, &team, &e1, None, PageDirection::Desc, ActivityKind::All, 10)
        .await
        .expect_err("attacker must not read another user's engagement activity");
    assert!(
        matches!(err, TeamError::TeamNotFound(_)),
        "cross-user must 404, got {err:?}"
    );

    // Foreign team: same user, a different team id than the engagement's owner.
    let other_team = h.create_team(owner, "Other").await;
    let err = h
        .svc
        .list_engagement_activity(
            owner,
            &other_team.id,
            &e1,
            None,
            PageDirection::Desc,
            ActivityKind::All,
            10,
        )
        .await
        .expect_err("engagement id bound to a foreign :id team must 404");
    assert!(
        matches!(err, TeamError::TeamNotFound(_)),
        "cross-team must 404, got {err:?}"
    );
}

#[tokio::test]
async fn engagement_activity_empty_engagement_is_valid_empty_page() {
    let user = "user-act-empty";
    let h = Harness::new(user).await;
    let (team, e1, _) = h.two_engagements(user, "ActEmpty").await;
    // No rows seeded on e1.
    let page = h
        .svc
        .list_engagement_activity(user, &team, &e1, None, PageDirection::Desc, ActivityKind::All, 10)
        .await
        .expect("an empty engagement is a valid (empty) page, not an error");
    assert!(page.items.is_empty());
    assert!(!page.has_more);
    assert!(page.next_cursor.is_none());
}
