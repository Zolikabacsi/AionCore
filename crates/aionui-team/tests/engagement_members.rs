//! Phase 2b Task 5 — the cross-engagement isolation + template-independence
//! Definition-of-Done proof.
//!
//! Unlike the per-area tests (which prove each slice separately), this file
//! wires the REAL end-to-end stack — `init_database_memory()` + real
//! `SqliteTeamRepository` + real `SqliteProjectStore`/`ProjectService` + the
//! real `TeamSessionService`/`TeamAgentProvisioner` — and asserts the FULL
//! picture at once:
//!
//! 1. One team engaged in two projects simultaneously owns DISJOINT member
//!    conversation/slot sets, each bound to its own engagement workspace.
//! 2. Two live sessions of the same team share no task-board or mailbox state
//!    (reads AND a cross-engagement mutation attempt, via the engagement-gated
//!    boards of Phase 2a/3d).
//! 3. A template edit (`add_agent`) never retro-alers a running, non-active
//!    engagement's materialized members — only the ACTIVE engagement and
//!    freshly engaged projects see the new member.
//! 4. A project switch tears down the prior engagement's live session (the
//!    regression deferred from Task 4's A→B test).
//!
//! Only the conversation-provisioning seam is a recording double (it stands in
//! for the composition-layer `TeamConversationAdapters`, which live above this
//! crate); every id, workspace binding, engagement stamp and roster merge the
//! assertions read comes from real production code paths.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};

use aionui_ai_agent::agent_task::AgentInstance;
use aionui_ai_agent::types::BuildTaskOptions;
use aionui_ai_agent::{AgentError, IWorkerTaskManager};
use aionui_api_types::{
    AddAgentRequest, CreateTeamRequest, GetConfigOptionsResponse, TeamAgentInput, TeamMcpSelection, WebSocketMessage,
};
use aionui_common::{AgentKillReason, TimestampMs};
use aionui_db::models::MessageRow;
use aionui_db::{
    ITeamRepository, SqliteAgentMetadataRepository, SqliteAssistantDefinitionRepository,
    SqliteAssistantOverlayRepository, SqliteProjectStore, SqliteProviderRepository, SqliteTeamRepository,
    init_database_memory,
};
use aionui_project::{ProjectService, canonical};
use aionui_realtime::EventBroadcaster;
use aionui_team::ports::{
    AgentTurnCancellationPort, AgentTurnExecutionError, AgentTurnExecutionPort, AgentTurnOutcome, AgentTurnRequest,
    AgentTurnStatus, TeamAssistantCatalogEntry, TeamAssistantCatalogPort,
};
use aionui_team::types::{MailboxMessageType, Team, TeamTask};
use aionui_team::{
    TeamConversationCreateRequest, TeamConversationCreateResult, TeamConversationProvisioningPort, TeamError,
    TeamMcpSnapshotResolution, TeamProjectionMessageStore, TeamSession, TeamSessionService,
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

/// Records `conversation_id -> extra` for every minted member conversation so
/// tests can assert the workspace + engagement stamp the REAL provisioner wrote.
#[derive(Default)]
struct RecordingConversationPort {
    extras: Mutex<HashMap<String, serde_json::Value>>,
}

impl RecordingConversationPort {
    fn extra_of(&self, conversation_id: &str) -> Option<serde_json::Value> {
        self.extras.lock().unwrap().get(conversation_id).cloned()
    }

    fn workspace_of(&self, conversation_id: &str) -> Option<String> {
        self.extra_of(conversation_id).and_then(|extra| {
            extra
                .get("workspace")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
    }

    fn engagement_of(&self, conversation_id: &str) -> Option<String> {
        self.extra_of(conversation_id).and_then(|extra| {
            extra
                .get("engagementId")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
    }

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
            _ => self.create_team_temp_workspace(&request.user_id, "t5").await?,
        };
        extra["workspace"] = serde_json::Value::String(workspace.clone());
        self.extras.lock().unwrap().insert(id.clone(), extra);
        Ok(TeamConversationCreateResult {
            conversation_id: id,
            workspace,
        })
    }

    async fn conversation_workspace(&self, conversation_id: &str) -> Result<Option<String>, TeamError> {
        Ok(self.workspace_of(conversation_id))
    }

    async fn conversation_assistant_id(&self, conversation_id: &str) -> Result<Option<String>, TeamError> {
        Ok(self.extra_of(conversation_id).and_then(|extra| {
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
        let path = unique_temp_dir(&format!("engagement-t5-temp-{tag}"));
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
    port: Arc<RecordingConversationPort>,
    projects: Arc<ProjectService>,
    _db: aionui_db::Database,
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{prefix}-{}", aionui_common::generate_id()))
}

impl Harness {
    /// A fresh DB + the REAL team service (real `SqliteTeamRepository`, real
    /// agent-metadata/assistant/provider repos) wired to the recording
    /// conversation port. The acting user is seeded because the project tables
    /// carry a `users(id)` FK.
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
            unique_temp_dir("engagement-t5-projects"),
        ));
        Self {
            repo,
            svc,
            port,
            projects,
            _db: db,
        }
    }

    /// A 2-member team (lead + teammate) created through the real `create_team`
    /// (template conversations provisioned into the team workspace), project
    /// pointer left unbound.
    async fn create_two_member_team(&self, user: &str, name: &str) -> aionui_api_types::TeamResponse {
        let workspace = unique_temp_dir("engagement-t5-team");
        std::fs::create_dir_all(&workspace).unwrap();
        self.svc
            .create_team(
                user,
                CreateTeamRequest {
                    name: name.into(),
                    agents: vec![
                        TeamAgentInput {
                            name: "Lead".into(),
                            role: "lead".into(),
                            backend: Some("acp".into()),
                            model: "claude".into(),
                            assistant_id: None,
                            conversation_id: None,
                        },
                        TeamAgentInput {
                            name: "Mate".into(),
                            role: "teammate".into(),
                            backend: Some("acp".into()),
                            model: "claude".into(),
                            assistant_id: None,
                            conversation_id: None,
                        },
                    ],
                    workspace: Some(workspace.to_string_lossy().into_owned()),
                    project_id: None,
                },
            )
            .await
            .unwrap()
    }

    async fn create_project(&self, user: &str) -> (String, String) {
        self.svc.with_project_service(self.projects.clone());
        let dir = unique_temp_dir("engagement-t5-proj");
        std::fs::create_dir_all(&dir).unwrap();
        let project_id = self
            .projects
            .create_standard(user, canonical::to_file_uri(&dir).unwrap())
            .await
            .unwrap()
            .project
            .project_id;
        (project_id, dir.to_string_lossy().into_owned())
    }

    async fn engagement_members(
        &self,
        user: &str,
        engagement: &str,
    ) -> Vec<aionui_db::models::TeamEngagementMemberRow> {
        self.repo.list_engagement_members(user, engagement).await.unwrap()
    }

    async fn team_domain(&self, user: &str, team_id: &str, project_id: &str) -> Team {
        let row = self.repo.get_team(user, team_id).await.unwrap().unwrap();
        let mut team = Team::from_row(&row).unwrap();
        team.project_id = Some(project_id.to_owned());
        team
    }

    async fn start_session(&self, user: &str, team_id: &str, project_id: &str) -> TeamSession {
        let team = self.team_domain(user, team_id, project_id).await;
        TeamSession::start(
            team,
            Arc::clone(&self.repo) as Arc<dyn ITeamRepository>,
            Arc::new(NullBroadcaster),
            Arc::new(PathBuf::from("/bin/true")),
            Arc::new(NoopTaskManager),
            Arc::new(NoopTurnPort),
            Arc::new(NoopCancellationPort),
            Arc::new(RecordingConversationPort::default()),
            user.to_owned(),
            Weak::<TeamSessionService>::new(),
        )
        .await
        .expect("TeamSession::start")
    }
}

fn slot_ids(members: &[aionui_db::models::TeamEngagementMemberRow]) -> HashSet<String> {
    members.iter().map(|m| m.slot_id.clone()).collect()
}

fn conversation_ids(members: &[aionui_db::models::TeamEngagementMemberRow]) -> HashSet<String> {
    members.iter().map(|m| m.conversation_id.clone()).collect()
}

fn roster_runtime_ids(roster: &[aionui_team::types::TeamAgent]) -> (HashSet<String>, HashSet<String>) {
    (
        roster.iter().map(|a| a.slot_id.clone()).collect(),
        roster.iter().map(|a| a.conversation_id.clone()).collect(),
    )
}

fn lead_of(members: &[aionui_db::models::TeamEngagementMemberRow]) -> &aionui_db::models::TeamEngagementMemberRow {
    members
        .iter()
        .find(|m| m.role == "lead")
        .expect("lead member materialized")
}

// ── Scenario 1: simultaneous engagements own disjoint, workspace-bound members

#[tokio::test]
async fn two_concurrent_engagements_own_disjoint_workspace_bound_members() {
    let user = "user-t5-iso";
    let h = Harness::new(user).await;
    let team = h.create_two_member_team(user, "Isolated").await;
    let (p1, ws1) = h.create_project(user).await;
    let (p2, ws2) = h.create_project(user).await;

    let eng1 = h.svc.ensure_engagement(user, &team.id, &p1).await.unwrap();
    let eng2 = h.svc.ensure_engagement(user, &team.id, &p2).await.unwrap();
    assert_ne!(eng1.id, eng2.id, "two projects mint two engagements");
    assert_ne!(eng1.id, team.id);
    assert_ne!(eng2.id, team.id);
    assert_eq!(eng1.workspace, ws1, "engagement 1 is bound to P1's folder");
    assert_eq!(eng2.workspace, ws2, "engagement 2 is bound to P2's folder");

    let members1 = h.engagement_members(user, &eng1.id).await;
    let members2 = h.engagement_members(user, &eng2.id).await;
    assert_eq!(members1.len(), 2, "both template slots materialized for P1");
    assert_eq!(members2.len(), 2, "both template slots materialized for P2");

    // The stable template identity is shared; the runtime identity never is.
    let template_slots: HashSet<String> = team.assistants.iter().map(|a| a.slot_id.clone()).collect();
    assert_eq!(
        members1.iter().map(|m| m.template_slot.clone()).collect::<HashSet<_>>(),
        template_slots,
        "engagement 1 covers exactly the template slots"
    );
    assert_eq!(
        members2.iter().map(|m| m.template_slot.clone()).collect::<HashSet<_>>(),
        template_slots,
        "engagement 2 covers exactly the template slots"
    );

    let convs1 = conversation_ids(&members1);
    let convs2 = conversation_ids(&members2);
    assert!(
        convs1.is_disjoint(&convs2),
        "no member conversation is shared across engagements: {convs1:?} vs {convs2:?}"
    );
    assert!(
        slot_ids(&members1).is_disjoint(&slot_ids(&members2)),
        "no runtime slot id is shared across engagements"
    );
    let template_convs: HashSet<String> = team.assistants.iter().map(|a| a.conversation_id.clone()).collect();
    assert!(
        convs1.is_disjoint(&template_convs) && convs2.is_disjoint(&template_convs),
        "an engagement never reuses the shared template conversations"
    );

    // Each engagement's members are bound to THAT engagement's workspace and
    // carry that engagement's stamp.
    for member in &members1 {
        assert_eq!(
            h.port.workspace_of(&member.conversation_id).as_deref(),
            Some(ws1.as_str()),
            "P1 member conversation bound to P1's workspace"
        );
        assert_eq!(
            h.port.engagement_of(&member.conversation_id).as_deref(),
            Some(eng1.id.as_str()),
            "P1 member conversation stamped with P1's engagement"
        );
    }
    for member in &members2 {
        assert_eq!(
            h.port.workspace_of(&member.conversation_id).as_deref(),
            Some(ws2.as_str()),
            "P2 member conversation bound to P2's workspace"
        );
        assert_eq!(
            h.port.engagement_of(&member.conversation_id).as_deref(),
            Some(eng2.id.as_str()),
            "P2 member conversation stamped with P2's engagement"
        );
    }
    // The template's own conversations stay on the team workspace.
    for agent in &team.assistants {
        assert_eq!(
            h.port.workspace_of(&agent.conversation_id).as_deref(),
            Some(team.workspace.as_str()),
            "template conversation must not be rebound by engagement materialization"
        );
    }

    h.svc.stop_sessions_for_user(user);
}

// ── Scenario 2: two live sessions share no board/mailbox state (read + write)

#[tokio::test]
async fn concurrent_engagement_sessions_share_no_mail_or_board_state() {
    let user = "user-t5-state";
    let h = Harness::new(user).await;
    let team = h.create_two_member_team(user, "State Isolation").await;
    let (p1, _ws1) = h.create_project(user).await;
    let (p2, _ws2) = h.create_project(user).await;
    let eng1 = h.svc.ensure_engagement(user, &team.id, &p1).await.unwrap();
    let eng2 = h.svc.ensure_engagement(user, &team.id, &p2).await.unwrap();
    let members1 = h.engagement_members(user, &eng1.id).await;
    let members2 = h.engagement_members(user, &eng2.id).await;

    // Two live sessions of the SAME team, driven by their engagement rosters.
    let sess1 = h.start_session(user, &team.id, &p1).await;
    let sess2 = h.start_session(user, &team.id, &p2).await;
    assert_eq!(sess1.engagement_id(), eng1.id);
    assert_eq!(sess2.engagement_id(), eng2.id);

    let (roster1_slots, roster1_convs) = roster_runtime_ids(&sess1.scheduler().list_agents().await);
    assert_eq!(
        roster1_slots,
        slot_ids(&members1),
        "session 1 drives P1's engagement runtime slots"
    );
    assert_eq!(roster1_convs, conversation_ids(&members1));
    let (roster2_slots, _) = roster_runtime_ids(&sess2.scheduler().list_agents().await);
    assert_eq!(roster2_slots, slot_ids(&members2));
    assert!(
        roster1_slots.is_disjoint(&roster2_slots),
        "the two live rosters share no slot"
    );

    // Mail addressed to P1's lead runtime slot.
    let lead1 = lead_of(&members1);
    sess1
        .mailbox()
        .write(
            &team.id,
            &lead1.slot_id,
            "user",
            MailboxMessageType::Message,
            "p1 only",
            None,
        )
        .await
        .unwrap();

    // P1's session reads it; P2's session reads NOTHING for the SAME team id
    // and the SAME recipient slot id — only the engagement differs, so any
    // team-keyed read would leak it (the pre-Phase-2b behavior).
    let mine = sess1.mailbox().peek_unread(&team.id, &lead1.slot_id).await.unwrap();
    assert_eq!(mine.len(), 1);
    assert_eq!(mine[0].content, "p1 only");
    assert!(
        sess2
            .mailbox()
            .peek_unread(&team.id, &lead1.slot_id)
            .await
            .unwrap()
            .is_empty(),
        "P2's session must not observe P1's mail"
    );
    let team_wide = h.repo.list_messages_by_team(&team.id, 100).await.unwrap();
    assert!(
        team_wide.iter().any(|m| m.content == "p1 only"),
        "control: the row exists team-wide, P2's empty peek is engagement scoping, not a missing write"
    );
    assert!(
        h.repo
            .list_messages_by_engagement(user, &eng2.id)
            .await
            .unwrap()
            .is_empty(),
        "P2's engagement owns no mail rows"
    );

    // Task on P1's board.
    let task1 = sess1
        .scheduler()
        .create_task("p1 ship", None, None, &[], None)
        .await
        .unwrap();
    let stored = h
        .repo
        .find_task_by_id(user, &team.id, &task1.id)
        .await
        .unwrap()
        .expect("task persisted");
    assert_eq!(
        stored.engagement_id.as_deref(),
        Some(eng1.id.as_str()),
        "P1's task is stamped with P1's engagement"
    );
    let board1: Vec<String> = sess1
        .scheduler()
        .list_tasks()
        .await
        .unwrap()
        .iter()
        .map(|t| t.id.clone())
        .collect();
    let board2: Vec<String> = sess2
        .scheduler()
        .list_tasks()
        .await
        .unwrap()
        .iter()
        .map(|t| t.id.clone())
        .collect();
    assert_eq!(board1, vec![task1.id.clone()]);
    assert!(board2.is_empty(), "P2's board must not list P1's task");

    // Write-side (Task 3d): P2's engagement-gated board must reject a mutation
    // of P1's task AND leave the stored row untouched.
    let err = sess2
        .scheduler()
        .update_task(&task1.id, Some("completed"), None, None, None)
        .await
        .expect_err("cross-engagement task mutation must be rejected");
    assert!(
        matches!(err, TeamError::TaskNotFound(ref id) if id == &task1.id),
        "got {err:?}"
    );
    let unchanged = h
        .repo
        .find_task_by_id(user, &team.id, &task1.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.status, "pending", "rejected mutation must not touch the row");

    // Positive control: the owning engagement mutates the same task fine.
    let owned: TeamTask = sess1
        .scheduler()
        .update_task(&task1.id, Some("in_progress"), None, None, None)
        .await
        .unwrap();
    assert_eq!(owned.status.to_string(), "in_progress");

    sess1.stop();
    sess2.stop();
    h.svc.stop_sessions_for_user(user);
}

// ── Scenario 3: template edits reach NEW engagements only ───────────────────

#[tokio::test]
async fn template_edit_does_not_retroactively_alter_a_running_engagement() {
    let user = "user-t5-tpl";
    let h = Harness::new(user).await;
    let team = h.create_two_member_team(user, "Template Independence").await;
    let (p1, _ws1) = h.create_project(user).await;
    let (p2, ws2) = h.create_project(user).await;
    let (p3, ws3) = h.create_project(user).await;

    // Engagement P1 materializes first, then P2 becomes the ACTIVE engagement
    // through the real project-switch path.
    let eng1 = h.svc.ensure_engagement(user, &team.id, &p1).await.unwrap();
    h.svc.update_team_project(user, &team.id, &p2).await.unwrap();
    let eng2 = h
        .repo
        .find_or_create_engagement(user, &team.id, &p2, &ws2)
        .await
        .unwrap()
        .id;

    let members1_before = h.engagement_members(user, &eng1.id).await;
    assert_eq!(members1_before.len(), 2);
    let template_slots_before: Vec<String> = svc_template_slots(&h, user, &team.id).await;

    // Template edit while P1 runs (inactive) and P2 is active.
    let added = h
        .svc
        .add_agent(
            user,
            &team.id,
            AddAgentRequest {
                name: "Bot3".into(),
                role: "teammate".into(),
                backend: Some("acp".into()),
                model: "claude".into(),
                assistant_id: None,
            },
        )
        .await
        .unwrap();

    // The running engagement P1 is UNCHANGED: no retroactive third member.
    let members1_after = h.engagement_members(user, &eng1.id).await;
    assert_eq!(members1_after.len(), 2, "template edit must not add a member to P1");
    assert_eq!(
        conversation_ids(&members1_after),
        conversation_ids(&members1_before),
        "P1's existing member conversations are the same rows"
    );
    // The added member's runtime identity is the ACTIVE engagement's: its own
    // fresh conversation, bound to the active workspace and stamped with it.
    assert_eq!(
        h.port.workspace_of(&added.conversation_id).as_deref(),
        Some(ws2.as_str()),
        "the new member's conversation is bound to the ACTIVE engagement's workspace"
    );
    assert_eq!(
        h.port.engagement_of(&added.conversation_id).as_deref(),
        Some(eng2.as_str()),
        "the new member's conversation is stamped with the ACTIVE engagement"
    );
    assert!(
        members1_after.iter().all(|m| m.slot_id != added.slot_id),
        "the newly added runtime member must NOT exist in the untouched engagement"
    );

    // The ACTIVE engagement sees it (by design) — this is the other half of the
    // discriminating pair: only the non-active engagement was asserted frozen.
    let members2 = h.engagement_members(user, &eng2).await;
    assert_eq!(members2.len(), 3, "the active engagement materializes the new member");
    let new_template_slot = svc_new_teammate_slot(&h, user, &team.id, &template_slots_before).await;
    let added_in_active = members2
        .iter()
        .find(|m| m.template_slot == new_template_slot)
        .expect("new template slot materialized in the active engagement");
    assert_eq!(added_in_active.conversation_id, added.conversation_id);

    // A FRESHLY engaged project materializes WITH the new member.
    let eng3 = h.svc.ensure_engagement(user, &team.id, &p3).await.unwrap();
    let members3 = h.engagement_members(user, &eng3.id).await;
    assert_eq!(members3.len(), 3, "a new engagement sees the edited template");
    assert!(
        members3.iter().any(|m| m.template_slot == new_template_slot),
        "P3 materializes the new template slot"
    );
    for member in &members3 {
        assert_eq!(
            h.port.workspace_of(&member.conversation_id).as_deref(),
            Some(ws3.as_str()),
            "P3's members are bound to P3's workspace"
        );
    }
    assert!(
        conversation_ids(&members3).is_disjoint(&conversation_ids(&members1_after)),
        "P3's member conversations are fresh, never P1's"
    );

    h.svc.stop_sessions_for_user(user);
}

async fn svc_template_slots(h: &Harness, user: &str, team_id: &str) -> Vec<String> {
    h.svc
        .get_team(user, team_id)
        .await
        .unwrap()
        .assistants
        .iter()
        .map(|a| a.slot_id.clone())
        .collect()
}

async fn svc_new_teammate_slot(h: &Harness, user: &str, team_id: &str, before: &[String]) -> String {
    h.svc
        .get_team(user, team_id)
        .await
        .unwrap()
        .assistants
        .iter()
        .find(|a| !before.contains(&a.slot_id))
        .expect("template gained exactly one slot")
        .slot_id
        .clone()
}

// ── Scenario 4: project switch tears down the PRIOR engagement's session ────

#[tokio::test]
async fn project_switch_stops_the_prior_engagement_session() {
    let user = "user-t5-switch";
    let h = Harness::new(user).await;
    let team = h.create_two_member_team(user, "Switch Stop").await;
    let (p1, _ws1) = h.create_project(user).await;
    let (p2, ws2) = h.create_project(user).await;

    // Start a live session on engagement P1 via the real service path.
    h.svc.update_team_project(user, &team.id, &p1).await.unwrap();
    h.svc.ensure_session(user, &team.id).await.unwrap();
    let eng1 = h
        .repo
        .find_or_create_engagement(user, &team.id, &p1, &team.workspace)
        .await
        .unwrap()
        .id;
    let scheduler1 = h.svc.get_session_scheduler(&team.id).expect("P1 session is live");
    let (roster1_slots, _) = roster_runtime_ids(&scheduler1.list_agents().await);
    assert_eq!(roster1_slots, slot_ids(&h.engagement_members(user, &eng1).await));

    // Switch projects: the prior engagement's session must be torn down.
    h.svc.update_team_project(user, &team.id, &p2).await.unwrap();
    assert!(
        h.svc.get_session_scheduler(&team.id).is_none(),
        "the prior engagement's live session must be stopped by the switch"
    );

    // The new engagement's session runs on its OWN runtime members.
    h.svc.ensure_session(user, &team.id).await.unwrap();
    let scheduler2 = h.svc.get_session_scheduler(&team.id).expect("P2 session is live");
    let (roster2_slots, roster2_convs) = roster_runtime_ids(&scheduler2.list_agents().await);
    let eng2 = h
        .repo
        .find_or_create_engagement(user, &team.id, &p2, &ws2)
        .await
        .unwrap()
        .id;
    let members2 = h.engagement_members(user, &eng2).await;
    assert_eq!(roster2_slots, slot_ids(&members2), "P2 session drives P2's members");
    assert_eq!(roster2_convs, conversation_ids(&members2));
    assert!(
        roster1_slots.is_disjoint(&roster2_slots),
        "the switched session shares no runtime slot with the prior engagement"
    );

    h.svc.stop_sessions_for_user(user);
}
