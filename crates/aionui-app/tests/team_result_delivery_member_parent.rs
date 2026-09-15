//! Phase 4c FINAL-FIX — team-aware result return (spec §8/§10), end-to-end.
//!
//! The Critical: a convene from a TEAM MEMBER stores `reply_to` = that member's
//! own (team-owned) conversation. The 4b return leg delivered it via
//! `ConversationService::send_message`, which FORBIDS team-owned conversations —
//! so the child result was silently undeliverable (newly reachable once 4c
//! admitted team-member senders). The fix routes a team-member parent into the
//! parent's OWN engagement mailbox via the team seam instead.
//!
//! This test drives the REAL `DelegatedResultDeliveryAdapter` (the fixed code)
//! against a REAL `TeamSessionService` whose engagement members were materialized
//! by the real 4a convene seam, and asserts the consolidated result lands in the
//! parent member's mailbox exactly once and is NOT posted through `send_message`.
//! On the pre-fix adapter the same call hits `send_message` on a team-owned
//! conversation → `Forbidden` → no mailbox row, so this test fails there and
//! passes after the fix.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use aionui_ai_agent::agent_task::AgentInstance;
use aionui_ai_agent::types::BuildTaskOptions;
use aionui_ai_agent::{AgentError, IWorkerTaskManager};
use aionui_api_types::{GetConfigOptionsResponse, TeamMcpSelection, WebSocketMessage};
use aionui_app::DelegatedResultDeliveryAdapter;
use aionui_common::{AgentKillReason, TimestampMs, now_ms};
use aionui_conversation::service::ConversationService;
use aionui_conversation::skill_resolver::{ResolvedAgentSkill, SkillResolver};
use aionui_db::models::{MessageRow, TeamRow};
use aionui_db::{
    IConversationRepository, ITeamRepository, SqliteAcpSessionRepository, SqliteAgentMetadataRepository,
    SqliteAssistantDefinitionRepository, SqliteAssistantOverlayRepository, SqliteConversationRepository,
    SqliteProviderRepository, SqliteTeamRepository, init_database_memory,
};
use aionui_realtime::EventBroadcaster;
use aionui_team::ports::{
    AgentTurnCancellationPort, AgentTurnExecutionError, AgentTurnExecutionPort, AgentTurnOutcome, AgentTurnRequest,
    AgentTurnStatus, TeamAssistantCatalogEntry, TeamAssistantCatalogPort,
};
use aionui_team::types::Team;
use aionui_team::{
    DelegatedResultDelivery, TeamAgent, TeamConversationCreateRequest, TeamConversationCreateResult,
    TeamConversationProvisioningPort, TeamError, TeamMcpSnapshotResolution, TeamProjectionMessageStore, TeamSession,
    TeamSessionService, TeammateRole,
};
use async_trait::async_trait;
use serde_json::Value;
use sqlx::Row;

const USER: &str = "u-member-ret";
const TEAM: &str = "t-member-ret";

// ── Composition-layer seam doubles (real team service, only these stubbed) ───

struct NullBroadcaster;
impl EventBroadcaster for NullBroadcaster {
    fn broadcast(&self, _msg: WebSocketMessage<Value>) {}
}

struct NoSkills;
#[async_trait]
impl SkillResolver for NoSkills {
    async fn auto_inject_names(&self) -> Vec<String> {
        Vec::new()
    }
    async fn resolve_skills(&self, _names: &[String]) -> Vec<ResolvedAgentSkill> {
        Vec::new()
    }
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
    async fn cancel_agent_turn(&self, _u: &str, _c: &str, _t: &str) -> Result<(), AgentTurnExecutionError> {
        Ok(())
    }
}

#[derive(Default)]
struct NoopTaskManager;
#[async_trait]
impl IWorkerTaskManager for NoopTaskManager {
    fn get_task(&self, _c: &str) -> Option<AgentInstance> {
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
    async fn list_team_selectable_assistants(&self, _u: &str) -> Result<Vec<TeamAssistantCatalogEntry>, TeamError> {
        Ok(Vec::new())
    }
}

#[derive(Default)]
struct RecordingPort {
    extras: Mutex<HashMap<String, serde_json::Value>>,
}

#[async_trait]
impl TeamConversationProvisioningPort for RecordingPort {
    async fn create_team_conversation(
        &self,
        request: TeamConversationCreateRequest,
    ) -> Result<TeamConversationCreateResult, TeamError> {
        let id = aionui_common::generate_id();
        let workspace = std::env::temp_dir()
            .join(format!("member-ret-{}", id))
            .to_string_lossy()
            .into_owned();
        self.extras.lock().unwrap().insert(id.clone(), request.extra);
        Ok(TeamConversationCreateResult {
            conversation_id: id,
            workspace,
        })
    }
    async fn conversation_workspace(&self, _c: &str) -> Result<Option<String>, TeamError> {
        Ok(None)
    }
    async fn conversation_assistant_id(&self, _c: &str) -> Result<Option<String>, TeamError> {
        Ok(None)
    }
    async fn update_conversation_project_binding(
        &self,
        _c: &str,
        _p: Option<String>,
        _f: Option<String>,
        _w: Option<String>,
    ) -> Result<(), TeamError> {
        Ok(())
    }
    async fn create_team_temp_workspace(&self, _u: &str, tag: &str) -> Result<String, TeamError> {
        Ok(std::env::temp_dir()
            .join(format!("member-ret-temp-{tag}"))
            .to_string_lossy()
            .into_owned())
    }
    async fn patch_runtime_config(&self, _c: &str, _p: serde_json::Value) -> Result<(), TeamError> {
        Ok(())
    }
    async fn save_acp_runtime_mode(&self, _c: &str, _m: &str) -> Result<(), TeamError> {
        Ok(())
    }
    async fn get_config_options(&self, _c: &str) -> Result<GetConfigOptionsResponse, TeamError> {
        Ok(GetConfigOptionsResponse {
            config_options: Vec::new(),
        })
    }
    async fn warmup_agent_process(
        &self,
        _u: &str,
        _c: &str,
        _t: &Arc<dyn IWorkerTaskManager>,
    ) -> Result<(), TeamError> {
        Ok(())
    }
    async fn resolve_assistant_mcp_selection(&self, _u: &str, _a: &str) -> Result<Option<TeamMcpSelection>, TeamError> {
        Ok(Some(TeamMcpSelection::default()))
    }
    async fn resolve_conversation_mcp_snapshot(
        &self,
        _u: &str,
        _c: &str,
        _a: Option<&str>,
    ) -> Result<TeamMcpSnapshotResolution, TeamError> {
        Ok(TeamMcpSnapshotResolution::default())
    }
    async fn delete_team_conversation(&self, _u: &str, c: &str) -> Result<(), TeamError> {
        self.extras
            .lock()
            .unwrap()
            .remove(c)
            .map(|_| ())
            .ok_or_else(|| TeamError::AgentNotFound(c.to_owned()))
    }
    async fn latest_assistant_text(&self, _c: &str) -> Result<Option<String>, TeamError> {
        Ok(None)
    }
}

#[async_trait]
impl TeamProjectionMessageStore for RecordingPort {
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

fn two_agents() -> Vec<TeamAgent> {
    vec![
        TeamAgent {
            slot_id: "lead-1".into(),
            name: "Leader".into(),
            role: TeammateRole::Lead,
            conversation_id: "conv-lead".into(),
            backend: "acp".into(),
            model: "claude".into(),
            assistant_id: None,
            status: None,
            conversation_type: None,
            cli_path: None,
        },
        TeamAgent {
            slot_id: "worker-1".into(),
            name: "Worker".into(),
            role: TeammateRole::Teammate,
            conversation_id: "conv-worker".into(),
            backend: "acp".into(),
            model: "claude".into(),
            assistant_id: None,
            status: None,
            conversation_type: None,
            cli_path: None,
        },
    ]
}

struct Harness {
    adapter: DelegatedResultDeliveryAdapter,
    team_repo: Arc<SqliteTeamRepository>,
    conv_repo: Arc<SqliteConversationRepository>,
    lead_slot_id: String,
    parent_engagement_id: String,
    _db: aionui_db::Database,
    _svc: Arc<TeamSessionService>,
    _session: Arc<TeamSession>,
}

impl Harness {
    async fn new() -> Self {
        let db = init_database_memory().await.unwrap();
        let pool = db.pool().clone();
        sqlx::query(
            "INSERT INTO users (id, user_type, username, password_hash, status, session_generation, created_at, updated_at) \
             VALUES (?1, 'local', ?1, 'hash', 'active', 0, 1, 1)",
        )
        .bind(USER)
        .execute(&pool)
        .await
        .unwrap();

        let team_repo = Arc::new(SqliteTeamRepository::new(pool.clone()));
        let agents = two_agents();
        team_repo
            .create_team(&TeamRow {
                id: TEAM.to_owned(),
                user_id: USER.to_owned(),
                name: TEAM.to_owned(),
                workspace: "/tmp/member-ret".to_owned(),
                workspace_mode: "shared".to_owned(),
                agents: serde_json::to_string(&agents).unwrap(),
                lead_agent_id: Some("lead-1".to_owned()),
                session_mode: None,
                agents_version: "1.0.1".to_owned(),
                created_at: now_ms(),
                updated_at: now_ms(),
                project_id: None,
                folder_id: None,
            })
            .await
            .unwrap();

        let conv_repo = Arc::new(SqliteConversationRepository::new(pool.clone()));
        let task_manager: Arc<dyn IWorkerTaskManager> = Arc::new(NoopTaskManager);
        let conversation_service = ConversationService::new(
            std::env::temp_dir(),
            Arc::new(NullBroadcaster),
            Arc::new(NoSkills),
            task_manager.clone(),
            conv_repo.clone() as Arc<dyn IConversationRepository>,
            Arc::new(SqliteAgentMetadataRepository::new(pool.clone())) as Arc<dyn aionui_db::IAgentMetadataRepository>,
            Arc::new(SqliteAcpSessionRepository::new(pool.clone())) as Arc<dyn aionui_db::IAcpSessionRepository>,
        );

        let port = Arc::new(RecordingPort::default());
        let svc = TeamSessionService::new(
            Arc::clone(&team_repo) as Arc<dyn ITeamRepository>,
            Arc::new(SqliteAgentMetadataRepository::new(pool.clone())),
            Arc::new(EmptyCatalog),
            Arc::new(SqliteAssistantDefinitionRepository::new(pool.clone())),
            Arc::new(SqliteAssistantOverlayRepository::new(pool.clone())),
            Arc::new(SqliteProviderRepository::new(pool.clone())),
            Arc::clone(&port) as Arc<dyn TeamConversationProvisioningPort>,
            Arc::clone(&port) as Arc<dyn TeamProjectionMessageStore>,
            Arc::new(NullBroadcaster),
            Arc::clone(&task_manager) as Arc<dyn IWorkerTaskManager>,
            Arc::new(NoopTurnPort),
            Arc::new(NoopCancellationPort),
            Arc::new(PathBuf::from("/bin/true")),
        );

        // Convene a root with NO reply target just to MATERIALIZE the engagement
        // members (real 4a seam); the lead member's conversation is then a real
        // team-owned (member) conversation — the shape the 4c sender inversion
        // makes a `reply_to` for a delegated child.
        let convened = svc
            .convene_delegated_task(
                USER,
                TEAM,
                "__none__",
                "seed",
                "",
                None,
                "[[AION_DELEGATE]]\nenvelope_id: seed\n[[/AION_DELEGATE]]\n\nx",
                None,
                0,
            )
            .await
            .expect("seed convene");

        // Start the engagement session so the runtime is live for the wake.
        let row = team_repo.get_team(USER, TEAM).await.unwrap().unwrap();
        let team = Team::from_row(&row).unwrap();
        let session = Arc::new(
            TeamSession::start(
                team,
                Arc::clone(&team_repo) as Arc<dyn ITeamRepository>,
                Arc::new(NullBroadcaster),
                Arc::new(PathBuf::from("/bin/true")),
                Arc::clone(&task_manager) as Arc<dyn IWorkerTaskManager>,
                Arc::new(NoopTurnPort),
                Arc::new(NoopCancellationPort),
                Arc::clone(&port) as Arc<dyn TeamProjectionMessageStore>,
                USER.to_owned(),
                Arc::downgrade(&svc),
            )
            .await
            .expect("start session"),
        );

        let members = team_repo
            .list_engagement_members(USER, &convened.engagement_id)
            .await
            .unwrap();
        let lead = members.iter().find(|m| m.role == "lead").expect("lead member");

        // The REAL fixed adapter, wired to the team seam.
        let adapter = DelegatedResultDeliveryAdapter::new(conversation_service, task_manager).with_team_service(&svc);

        Self {
            adapter,
            team_repo,
            conv_repo,
            lead_slot_id: lead.slot_id.clone(),
            parent_engagement_id: convened.engagement_id.clone(),
            _db: db,
            _svc: svc,
            _session: session,
        }
    }

    async fn member_mailbox_rows(&self) -> Vec<String> {
        self.team_repo
            .peek_unread_by_engagement(USER, &self.parent_engagement_id, &self.lead_slot_id)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.content)
            .collect()
    }

    async fn posted_messages(&self, conversation_id: &str) -> usize {
        self.conv_repo
            .raw_query(
                "SELECT id FROM messages WHERE conversation_id = ?1",
                vec![conversation_id.into()],
            )
            .await
            .unwrap()
            .len()
    }
}

/// A team-member parent result is routed into the parent member's engagement
/// mailbox (woken) exactly once — NOT through the team-rejecting `send_message`.
#[tokio::test]
async fn team_member_parent_result_lands_in_engagement_mailbox_not_send_message() {
    let h = Harness::new().await;
    // The lead member's own conversation id is the convene `reply_to` the 4c
    // sender inversion produces (team-owned).
    let members = h
        .team_repo
        .list_engagement_members(USER, &h.parent_engagement_id)
        .await
        .unwrap();
    let lead_conv = members
        .iter()
        .find(|m| m.role == "lead")
        .unwrap()
        .conversation_id
        .clone();

    h.adapter
        .deliver_result(USER, &lead_conv, "child-eng-1", "PARENT MEMBER RESULT")
        .await
        .expect("a team-member parent result must land (pre-fix this hit send_message and Forbidden'd)");

    let rows = h.member_mailbox_rows().await;
    let result_rows: Vec<&String> = rows.iter().filter(|c| c.contains("PARENT MEMBER RESULT")).collect();
    assert_eq!(
        result_rows.len(),
        1,
        "exactly one result mailbox row for the parent member (no double post)"
    );
    let body = &result_rows[0];
    assert!(
        body.contains("[[AION_DELEGATE]]") && body.contains("kind: Result"),
        "the envelope marker the parent member's runtime recognizes is present:\n{body}"
    );

    assert_eq!(
        h.posted_messages(&lead_conv).await,
        0,
        "no message is posted via the team-rejecting send_message path"
    );
}

/// Control: a non-member (user) parent keeps the `send_message` write and never
/// touches the team mailbox — the user/assistant path stays byte-identical.
#[tokio::test]
async fn user_parent_result_still_goes_through_send_message() {
    let h = Harness::new().await;
    // Seed a real (non-member) user conversation to reply to.
    h.conv_repo
        .raw_execute(
            "INSERT INTO conversations (id,user_id,name,type,extra,status,pinned,created_at,updated_at) \
             VALUES ('user-parent',?1,'p','acp','{}','finished',0,0,0)",
            vec![USER.into()],
        )
        .await
        .unwrap();

    h.adapter
        .deliver_result(USER, "user-parent", "child-eng-1", "USER PARENT RESULT")
        .await
        .expect("user-parent delivery succeeds via send_message");

    // The team member's mailbox must NOT receive this result (routing went to
    // send_message, not the team seam).
    let rows = h.member_mailbox_rows().await;
    assert!(
        !rows.iter().any(|c| c.contains("USER PARENT RESULT")),
        "a user parent must not write into the team mailbox"
    );
    let posted = h
        .conv_repo
        .raw_query(
            "SELECT content FROM messages WHERE conversation_id = 'user-parent' AND position = 'right'",
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(posted.len(), 1, "exactly one inbound message posted via send_message");
    assert!(
        posted[0].get::<String, _>("content").contains("USER PARENT RESULT"),
        "the consolidated result is the posted body"
    );
}
