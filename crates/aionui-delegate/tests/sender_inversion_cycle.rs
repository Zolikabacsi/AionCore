//! Phase 4c Task 1 — cross-engagement cycle safety pieces that live on the
//! delegate (consumer) side of the bridge, tested WITHOUT flipping the sender
//! gate. `SenderIsTeam` still rejects team senders, so a real cycle is not yet
//! reachable; these tests build + verify the guard at the seam/bridge level:
//!
//!  * depth threading: a team dispatch carries the caller's `depth` onto the
//!    `ConveneRootTask` (and the envelope block) so the team seam can persist
//!    it and the lead's later hop can increment it.
//!  * the member-lineage predicate is a `TeamEngagementBridge` trait method
//!    with a safe default (`Ok(false)`) so existing / unwired doubles stay
//!    green and a non-overridden bridge never reports a cycle.
//!  * the MAX-depth boundary helper the convene entry (Task 2) will call.

use std::sync::Arc;
use std::sync::Mutex;

use aionui_ai_agent::agent_task::AgentInstance;
use aionui_ai_agent::types::BuildTaskOptions;
use aionui_ai_agent::{AgentError, IWorkerTaskManager};
use aionui_api_types::{DelegateDispatchRequest, DelegateEnvelopeBlock, WebSocketMessage};
use aionui_common::{AgentKillReason, TimestampMs};
use aionui_conversation::service::ConversationService;
use aionui_conversation::skill_resolver::{ResolvedAgentSkill, SkillResolver};
use aionui_db::{
    IConversationRepository, SqliteAcpSessionRepository, SqliteAgentMetadataRepository, SqliteConversationRepository,
    SqliteSettingsRepository, init_database_memory,
};
use aionui_delegate::MAX_DEPTH;
use aionui_delegate::bridge::{ConveneRootTask, ConvenedEngagement, NoopTeamEngagementBridge, TeamEngagementBridge};
use aionui_delegate::service::DelegateService;
use aionui_realtime::EventBroadcaster;
use async_trait::async_trait;

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

/// Records the depth the dispatch hands to the convene seam, on both the
/// root-task struct and the envelope block.
#[derive(Default)]
struct DepthRecordingBridge {
    roots: Mutex<Vec<u32>>,
    envelopes: Mutex<Vec<u32>>,
}

#[async_trait]
impl TeamEngagementBridge for DepthRecordingBridge {
    async fn convene_engagement(
        &self,
        _user_id: &str,
        _team_id: &str,
        _project_id: &str,
        root: ConveneRootTask<'_>,
        envelope: &DelegateEnvelopeBlock,
    ) -> Result<ConvenedEngagement, aionui_delegate::bridge::BridgeError> {
        self.roots.lock().unwrap().push(root.depth);
        self.envelopes.lock().unwrap().push(envelope.depth);
        Ok(ConvenedEngagement {
            engagement_id: "eng-1".into(),
            root_task_id: "task-1".into(),
            lead_slot_id: "lead-1".into(),
        })
    }
}

async fn service_with(bridge: Arc<dyn TeamEngagementBridge>) -> (DelegateService, Arc<SqliteConversationRepository>) {
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

fn dispatch_req(to: &str, message: &str, depth: Option<u32>) -> DelegateDispatchRequest {
    DelegateDispatchRequest {
        to: to.into(),
        message: message.into(),
        files: Vec::new(),
        reply_to: None,
        depth,
        expected_output: None,
    }
}

/// The caller's incoming `depth` is threaded onto the `ConveneRootTask` handed
/// to the seam (and the envelope block), so the team side can persist it and
/// the lead's onward hop increments it.
#[tokio::test]
async fn team_dispatch_threads_caller_depth_onto_root_task() {
    let bridge = Arc::new(DepthRecordingBridge::default());
    let (svc, r) = service_with(bridge.clone()).await;
    insert_caller(&r, "conv-1", Some("proj-1")).await;
    insert_team(&r, "team-1", "Growth").await;

    // A user dispatch defaults to depth 0 (no envelope).
    svc.dispatch("user-1", "conv-1", "CEO", dispatch_req("Growth", "work it", None))
        .await
        .expect("depth-0 team dispatch ok");
    // A nested caller carries an explicit depth.
    let mut nested = dispatch_req("Growth", "work it again", Some(2));
    nested.reply_to = Some("conv-1".into());
    svc.dispatch("user-1", "conv-1", "CEO", nested)
        .await
        .expect("depth-2 team dispatch ok");

    assert_eq!(
        *bridge.roots.lock().unwrap(),
        vec![0, 2],
        "root task must carry the caller's depth"
    );
    assert_eq!(
        *bridge.envelopes.lock().unwrap(),
        vec![0, 2],
        "envelope block must carry the same depth"
    );
}

/// The member-lineage predicate is a bridge trait method; the default
/// implementation reports "not a member" (`Ok(false)`) so a bridge that has
/// not wired the team-side resolution (Noop, or an unwired test double) never
/// reports a cycle and existing dispatches are unaffected.
#[tokio::test]
async fn predicate_default_reports_not_a_member() {
    let member = NoopTeamEngagementBridge
        .conversation_is_member_of_engagement("u1", "conv-1", "team-1", "proj-1")
        .await
        .expect("default predicate never errors");
    assert!(!member, "unwired bridge must default to no-cycle");
}

/// MAX-depth boundary helper the convene entry (Task 2) calls for the NEXT
/// hop: a chain may reach depth == MAX_DEPTH, the hop past it exceeds.
#[test]
fn depth_exceeds_max_boundary() {
    assert!(
        !aionui_delegate::depth_exceeds_max(MAX_DEPTH),
        "depth == MAX is allowed"
    );
    assert!(
        aionui_delegate::depth_exceeds_max(MAX_DEPTH + 1),
        "depth > MAX must be flagged"
    );
    assert!(!aionui_delegate::depth_exceeds_max(0), "depth 0 is never exceeded");
}
