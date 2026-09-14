//! Phase 4a Task 1 — the team-side "convene engagement" seam.
//!
//! `TeamSessionService::convene_delegated_task` is what the delegation bridge
//! adapter will call (Phase 4a Tasks 2/3 route dispatches through it). This
//! file proves the seam on the REAL stack (`init_database_memory()` + real
//! `SqliteTeamRepository` + real `ProjectService` + real provisioner): it
//! finds-or-creates the (team, project) engagement, creates a root task owned
//! by the engagement's lead with `expected_output`, and writes the composed
//! envelope into the lead's engagement mailbox. Only the conversation-store
//! seam is a recording double (the composition-layer adapters live above this
//! crate), mirroring `engagement_members.rs`.

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
use aionui_team::{
    TeamConversationCreateRequest, TeamConversationCreateResult, TeamConversationProvisioningPort, TeamError,
    TeamMcpSnapshotResolution, TeamProjectionMessageStore, TeamSessionService,
};
use async_trait::async_trait;

// ── Test doubles: only the composition-layer seams (same as engagement_members)

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
            _ => self.create_team_temp_workspace(&request.user_id, "conv").await?,
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
        if let Some(workspace) = workspace
            && let Some(extra) = self.extras.lock().unwrap().get_mut(conversation_id)
        {
            extra["workspace"] = serde_json::Value::String(workspace);
        }
        Ok(())
    }

    async fn create_team_temp_workspace(&self, _user_id: &str, tag: &str) -> Result<String, TeamError> {
        let path = unique_temp_dir(&format!("delegation-convene-temp-{tag}"));
        std::fs::create_dir_all(&path).unwrap();
        Ok(path.to_string_lossy().into_owned())
    }

    async fn patch_runtime_config(&self, conversation_id: &str, patch: serde_json::Value) -> Result<(), TeamError> {
        let mut extras = self.extras.lock().unwrap();
        if let Some(extra) = extras.get_mut(conversation_id)
            && let (Some(target), Some(source)) = (extra.as_object_mut(), patch.as_object())
        {
            for (key, value) in source {
                target.insert(key.clone(), value.clone());
            }
        }
        Ok(())
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
            unique_temp_dir("delegation-convene-projects"),
        ));
        Self {
            repo,
            svc,
            projects,
            _db: db,
        }
    }

    async fn create_two_member_team(&self, user: &str, name: &str) -> aionui_api_types::TeamResponse {
        let workspace = unique_temp_dir("delegation-convene-team");
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

    async fn create_project(&self, user: &str) -> String {
        self.svc.with_project_service(self.projects.clone());
        let dir = unique_temp_dir("delegation-convene-proj");
        std::fs::create_dir_all(&dir).unwrap();
        self.projects
            .create_standard(user, canonical::to_file_uri(&dir).unwrap())
            .await
            .unwrap()
            .project
            .project_id
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn convene_reuses_engagement_creates_lead_root_task_and_envelopes_mailbox() {
    let user = "u1";
    let h = Harness::new(user).await;
    let team = h.create_two_member_team(user, "Bridge Team").await;
    let project = h.create_project(user).await;
    let envelope_text = "[[AION_DELEGATE]]\nenvelope_id: env-1\nreply_to: conv-9\n[[/AION_DELEGATE]]\n\ninvestigate";

    let first = h
        .svc
        .convene_delegated_task(
            user,
            &team.id,
            &project,
            "subject one",
            "description one",
            Some("done when X"),
            envelope_text,
        )
        .await
        .expect("convene ok");
    let second = h
        .svc
        .convene_delegated_task(user, &team.id, &project, "subject two", "", None, envelope_text)
        .await
        .expect("convene ok again");

    // §6 find-or-create: same (team, project) → same engagement, distinct root tasks.
    assert_eq!(first.engagement_id, second.engagement_id);
    assert_ne!(first.root_task_id, second.root_task_id);
    assert_eq!(first.lead_slot_id, second.lead_slot_id);

    let members = h
        .repo
        .list_engagement_members(user, &first.engagement_id)
        .await
        .unwrap();
    let lead = members.iter().find(|m| m.role == "lead").expect("lead member");
    assert_eq!(first.lead_slot_id, lead.slot_id);

    // Root task lives on the engagement board, owned by the lead, with the
    // declared success criteria.
    let task = h
        .repo
        .find_task_by_engagement(user, &first.engagement_id, &first.root_task_id)
        .await
        .unwrap()
        .expect("root task persisted");
    assert_eq!(task.subject, "subject one");
    assert_eq!(task.owner.as_deref(), Some(lead.slot_id.as_str()));
    assert_eq!(task.expected_output.as_deref(), Some("done when X"));

    // The lead's engagement mailbox carries the composed envelope text.
    let mail = h
        .repo
        .peek_unread_by_engagement(user, &first.engagement_id, &lead.slot_id)
        .await
        .unwrap();
    assert_eq!(mail.len(), 2);
    assert!(
        mail.iter()
            .all(|m| m.to_agent_id == lead.slot_id && m.content.contains(envelope_text)),
        "lead mailbox rows must carry the envelope: {mail:?}"
    );
}

#[tokio::test]
async fn convene_by_non_owner_is_rejected_without_side_effects() {
    let user = "u1";
    let h = Harness::new(user).await;
    let team = h.create_two_member_team(user, "Bridge Team").await;
    let project = h.create_project(user).await;

    let intruder = "intruder";
    let err = h
        .svc
        .convene_delegated_task(intruder, &team.id, &project, "steal", "", None, "env")
        .await
        .expect_err("non-owner must be rejected");
    // Same convention as the read paths: a missing team and another user's team
    // both surface as TeamNotFound, so team existence is never leaked.
    assert!(
        matches!(&err, TeamError::TeamNotFound(id) if *id == team.id),
        "expected TeamNotFound, got {err:?}"
    );
}
