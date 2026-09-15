//! Phase 4a Task 3: a `delegate dispatch` whose resolved target is a TEAM
//! routes through the `TeamEngagementBridge` (convene engagement + root task)
//! instead of the assistant delivery path. Assistant targets must keep the
//! pre-4a path untouched (bridge never called). Real `DelegateService` +
//! recording mock bridge; only the bridge is a double.

use std::sync::Arc;
use std::sync::Mutex;

use aionui_ai_agent::agent_task::AgentInstance;
use aionui_ai_agent::types::BuildTaskOptions;
use aionui_ai_agent::{AgentError, IWorkerTaskManager};
use aionui_api_types::{
    DelegateDeliveryStatus, DelegateDispatchRequest, DelegateEnvelopeBlock, DelegateTargetKind, WebSocketMessage,
};
use aionui_common::{AgentKillReason, TimestampMs};
use aionui_conversation::service::ConversationService;
use aionui_conversation::skill_resolver::{ResolvedAgentSkill, SkillResolver};
use aionui_db::{
    IConversationRepository, SqliteAcpSessionRepository, SqliteAgentMetadataRepository, SqliteConversationRepository,
    SqliteSettingsRepository, init_database_memory,
};
use aionui_delegate::bridge::{BridgeError, ConveneRootTask, ConvenedEngagement, TeamEngagementBridge};
use aionui_delegate::error::DelegateError;
use aionui_delegate::service::DelegateService;
use aionui_realtime::EventBroadcaster;
use async_trait::async_trait;

// ── Doubles ──────────────────────────────────────────────────────────────────

struct NullBroadcaster;
impl EventBroadcaster for NullBroadcaster {
    fn broadcast(&self, _msg: WebSocketMessage<serde_json::Value>) {}
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

#[derive(Debug, Clone, Default)]
struct RecordedCall {
    user_id: String,
    team_id: String,
    project_id: String,
    subject: String,
    description: String,
    expected_output: Option<String>,
    envelope_reply_to: Option<String>,
    envelope_depth: u32,
}

/// Records every convene call; optionally fails with a fixed `BridgeError`.
struct RecordingBridge {
    calls: Mutex<Vec<RecordedCall>>,
    fail_with: Option<BridgeError>,
}

impl RecordingBridge {
    fn ok() -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            fail_with: None,
        })
    }
    fn failing(err: BridgeError) -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            fail_with: Some(err),
        })
    }
}

#[async_trait]
impl TeamEngagementBridge for RecordingBridge {
    async fn convene_engagement(
        &self,
        user_id: &str,
        team_id: &str,
        project_id: &str,
        root: ConveneRootTask<'_>,
        envelope: &DelegateEnvelopeBlock,
    ) -> Result<ConvenedEngagement, BridgeError> {
        self.calls.lock().unwrap().push(RecordedCall {
            user_id: user_id.into(),
            team_id: team_id.into(),
            project_id: project_id.into(),
            subject: root.subject.into(),
            description: root.description.into(),
            expected_output: root.expected_output.map(Into::into),
            envelope_reply_to: envelope.reply_to.clone(),
            envelope_depth: envelope.depth,
        });
        match &self.fail_with {
            Some(err) => Err(match err {
                BridgeError::TeamNotFound => BridgeError::TeamNotFound,
                BridgeError::NotOwner => BridgeError::NotOwner,
                BridgeError::ProjectNotFound => BridgeError::ProjectNotFound,
                BridgeError::Delivery(m) => BridgeError::Delivery(m.clone()),
            }),
            None => Ok(ConvenedEngagement {
                engagement_id: "eng-9".into(),
                root_task_id: "task-9".into(),
                lead_slot_id: "lead-9".into(),
            }),
        }
    }
}

// ── Harness ──────────────────────────────────────────────────────────────────

async fn service_with(bridge: Arc<RecordingBridge>) -> (DelegateService, Arc<SqliteConversationRepository>) {
    let db = init_database_memory().await.expect("memory db");
    let pool = db.pool().clone();
    let conversation_repo = Arc::new(SqliteConversationRepository::new(pool.clone()));
    let task_manager: Arc<dyn IWorkerTaskManager> = Arc::new(NoopTaskManager);
    let conversation_service = ConversationService::new(
        std::env::temp_dir(),
        Arc::new(NullBroadcaster),
        Arc::new(NoSkills),
        task_manager.clone(),
        conversation_repo.clone() as Arc<dyn IConversationRepository>,
        Arc::new(SqliteAgentMetadataRepository::new(pool.clone())) as Arc<dyn aionui_db::IAgentMetadataRepository>,
        Arc::new(SqliteAcpSessionRepository::new(pool.clone())) as Arc<dyn aionui_db::IAcpSessionRepository>,
    );
    conversation_repo
        .raw_execute(
            "INSERT OR IGNORE INTO users (id, username, password_hash, created_at, updated_at) \
             VALUES ('user-1','user-1','x',0,0)",
            vec![],
        )
        .await
        .expect("seed user");
    let svc = DelegateService::new(
        conversation_service,
        conversation_repo.clone() as Arc<dyn IConversationRepository>,
        Arc::new(SqliteSettingsRepository::new(pool)) as Arc<dyn aionui_db::ISettingsRepository>,
        Arc::new(NullBroadcaster),
        task_manager,
        bridge,
    );
    (svc, conversation_repo)
}

async fn insert_caller(r: &SqliteConversationRepository, id: &str, project_id: Option<&str>) {
    let (proj_sql, proj_val) = match project_id {
        Some(p) => (", project_id", p.to_owned()),
        None => ("", String::new()),
    };
    r.raw_execute(
        &format!(
            "INSERT INTO conversations (id,user_id,name,type,extra,status,pinned,created_at,updated_at{proj_sql}) \
             VALUES (?1,'user-1','caller','acp','{{}}','finished',0,0,0{})",
            if proj_sql.is_empty() { "" } else { ",?2" }
        ),
        if proj_sql.is_empty() {
            vec![id.into()]
        } else {
            vec![id.into(), proj_val]
        },
    )
    .await
    .unwrap();
}

async fn insert_team(r: &SqliteConversationRepository, id: &str, name: &str) {
    r.raw_execute(
        "INSERT INTO teams (id, user_id, name, created_at, updated_at) VALUES (?1,'user-1',?2,0,0)",
        vec![id.into(), name.into()],
    )
    .await
    .unwrap();
}

async fn insert_assistant(r: &SqliteConversationRepository, def_id: &str, asst_id: &str, name: &str) {
    r.raw_execute(
        "INSERT INTO assistant_definitions (id, user_id, assistant_id, source, owner_type, name, name_i18n, \
            description_i18n, avatar_type, agent_id, rule_resource_type, recommended_prompts, recommended_prompts_i18n, \
            default_model_mode, default_permission_mode, default_thought_level_mode, default_skills_mode, \
            default_skill_ids, custom_skill_names, default_disabled_builtin_skill_ids, default_mcps_mode, \
            default_mcp_ids, allow_delegation, created_at, updated_at) \
         VALUES (?1,'user-1',?2,'user','user',?3,'{}','{}','none','53861a53','none','[]','{}', \
            'auto','auto','auto','auto','[]','[]','[]','auto','[]',1,0,0)",
        vec![def_id.into(), asst_id.into(), name.into()],
    )
    .await
    .unwrap();
}

fn dispatch_req(to: &str, message: &str) -> DelegateDispatchRequest {
    DelegateDispatchRequest {
        to: to.into(),
        message: message.into(),
        files: Vec::new(),
        reply_to: None,
        depth: None,
        expected_output: None,
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn team_dispatch_convenes_via_bridge_and_skips_assistant_path() {
    let bridge = RecordingBridge::ok();
    let (svc, r) = service_with(bridge.clone()).await;
    insert_caller(&r, "conv-1", Some("proj-1")).await;
    insert_team(&r, "team-1", "Growth").await;

    let mut req = dispatch_req("Growth", "research the Q4 market\nsecond line");
    req.expected_output = Some("a ranked list of 5 competitors".into());

    let resp = svc
        .dispatch("user-1", "conv-1", "CEO", req)
        .await
        .expect("team dispatch ok");

    let calls = bridge.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1, "bridge must be called exactly once");
    let call = &calls[0];
    assert_eq!(call.user_id, "user-1");
    assert_eq!(call.team_id, "team-1");
    assert_eq!(call.project_id, "proj-1", "caller's project must be forwarded");
    assert_eq!(call.subject, "research the Q4 market", "subject = first line");
    assert_eq!(call.description, "research the Q4 market\nsecond line");
    assert_eq!(call.expected_output.as_deref(), Some("a ranked list of 5 competitors"));
    assert_eq!(call.envelope_reply_to.as_deref(), Some("conv-1"));
    assert_eq!(call.envelope_depth, 0);

    // Response carries the convened ids (delivery info of the engagement path).
    assert_eq!(resp.engagement_id.as_deref(), Some("eng-9"));
    assert_eq!(resp.root_task_id.as_deref(), Some("task-9"));
    assert_eq!(resp.status, DelegateDeliveryStatus::Delivered);
    assert_eq!(resp.to_assistant_id, "team-1");

    // Assistant delivery path bypassed: no delegated room was created for the
    // team (only the caller row exists).
    let rooms = r
        .raw_query(
            "SELECT id FROM conversations WHERE json_valid(extra) \
             AND json_extract(extra, '$.delegated_from') = 'conv-1'",
            vec![],
        )
        .await
        .unwrap();
    assert!(rooms.is_empty(), "team dispatch must not create a delegated room");
}

#[tokio::test]
async fn team_dispatch_without_caller_project_uses_none_sentinel() {
    let bridge = RecordingBridge::ok();
    let (svc, r) = service_with(bridge.clone()).await;
    insert_caller(&r, "conv-1", None).await;
    insert_team(&r, "team-1", "Growth").await;

    svc.dispatch("user-1", "conv-1", "CEO", dispatch_req("Growth", "work it"))
        .await
        .expect("sentinel dispatch ok");

    let calls = bridge.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0].project_id, "__none__",
        "project-less caller must convene the team's default (sentinel) engagement"
    );
}

#[tokio::test]
async fn assistant_dispatch_does_not_call_bridge() {
    let bridge = RecordingBridge::ok();
    let (svc, r) = service_with(bridge.clone()).await;
    insert_caller(&r, "conv-1", Some("proj-1")).await;
    insert_team(&r, "team-1", "Growth").await;
    insert_assistant(&r, "asstdef-1", "custom-1", "CMO").await;

    let resp = svc
        .dispatch("user-1", "conv-1", "CEO", dispatch_req("CMO", "do it"))
        .await
        .expect("assistant dispatch must keep working unchanged");

    assert!(
        bridge.calls.lock().unwrap().is_empty(),
        "assistant dispatch must not touch the team bridge"
    );
    assert_eq!(resp.to_assistant_id, "asstdef-1");
    assert!(resp.engagement_id.is_none(), "assistant path carries no engagement ids");
    assert!(resp.root_task_id.is_none());
    // The assistant path went through ensure_target_conversation: the
    // (re)used delegated room exists for this caller.
    let rooms = r
        .raw_query(
            "SELECT id FROM conversations WHERE json_valid(extra) \
             AND json_extract(extra, '$.delegated_from') = 'conv-1' \
             AND json_extract(extra, '$.preset_assistant_id') = 'asstdef-1'",
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(rooms.len(), 1, "assistant path must create the delegated room");
}

#[tokio::test]
async fn team_dispatch_maps_team_not_found_to_target_not_found() {
    let bridge = RecordingBridge::failing(BridgeError::TeamNotFound);
    let (svc, r) = service_with(bridge.clone()).await;
    insert_caller(&r, "conv-1", Some("proj-1")).await;
    insert_team(&r, "team-1", "Growth").await;

    let err = svc
        .dispatch("user-1", "conv-1", "CEO", dispatch_req("Growth", "work it"))
        .await
        .expect_err("bridge TeamNotFound must surface");
    assert!(
        matches!(&err, DelegateError::TargetNotFound { query } if query == "Growth"),
        "expected TargetNotFound(Growth), got {err:?}"
    );
    assert_eq!(err.code(), aionui_api_types::DelegateToolErrorCode::TargetNotFound);
}

#[tokio::test]
async fn team_dispatch_at_max_depth_is_rejected_before_bridge() {
    let bridge = RecordingBridge::ok();
    let (svc, r) = service_with(bridge.clone()).await;
    insert_caller(&r, "conv-1", Some("proj-1")).await;
    insert_team(&r, "team-1", "Growth").await;

    let mut req = dispatch_req("Growth", "work it");
    req.depth = Some(aionui_delegate::MAX_DEPTH + 1);
    let err = svc
        .dispatch("user-1", "conv-1", "CEO", req)
        .await
        .expect_err("depth guard must reject");
    assert!(
        matches!(err, DelegateError::DepthExceeded { .. }),
        "expected DepthExceeded, got {err:?}"
    );
    assert!(bridge.calls.lock().unwrap().is_empty(), "bridge must not be called");
}

#[tokio::test]
async fn team_target_resolves_and_routes_not_target_not_found() {
    // Carryover (Task 2): a team-name dispatch must no longer be rejected as
    // TargetNotFound by the assistant-only guard — it must reach the bridge.
    let bridge = RecordingBridge::ok();
    let (svc, r) = service_with(bridge.clone()).await;
    insert_caller(&r, "conv-1", Some("proj-1")).await;
    insert_team(&r, "team-1", "Growth").await;

    assert_eq!(
        svc.resolve_target("user-1", "Growth").await.unwrap().kind,
        DelegateTargetKind::Team
    );
    svc.dispatch("user-1", "conv-1", "CEO", dispatch_req("Growth", "work it"))
        .await
        .expect("team dispatch routes");
    assert_eq!(bridge.calls.lock().unwrap().len(), 1);
}
