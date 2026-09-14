//! Phase 4c — cross-engagement cycle safety on the delegate (consumer) side of
//! the bridge, around the flipped sender boundary (Task 2):
//!
//!  * depth threading: a team dispatch carries the caller's `depth` onto the
//!    `ConveneRootTask` (and the envelope block) so the team seam can persist
//!    it and the lead's later hop can increment it.
//!  * the member-lineage predicate is a `TeamEngagementBridge` trait method
//!    with a safe default (`Ok(false)`) so existing / unwired doubles stay
//!    green and a non-overridden bridge never reports a cycle.
//!  * the MAX-depth boundary helper the convene entry calls.
//!  * sender inversion: a team-member sender dispatching to a TEAM runs the
//!    predicate + server-derived depth+1 before convene (`CycleDetected` /
//!    `DepthExceeded`); dispatching to an ASSISTANT uses the existing
//!    per-root deliver path with the bridge untouched; non-team senders are
//!    unchanged. Plus the shipped-guidance guard for Rule 6.

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

async fn insert_conversation(r: &SqliteConversationRepository, id: &str, extra: &str, project_id: Option<&str>) {
    let (proj_sql, proj_val) = match project_id {
        Some(p) => (", project_id", p.to_owned()),
        None => ("", String::new()),
    };
    r.raw_execute(
        &format!(
            "INSERT INTO conversations (id,user_id,name,type,extra,status,pinned,created_at,updated_at{proj_sql}) \
             VALUES (?1,'user-1','caller','acp',?2,'finished',0,0,0{})",
            if proj_sql.is_empty() { "" } else { ",?3" }
        ),
        if proj_sql.is_empty() {
            vec![id.into(), extra.into()]
        } else {
            vec![id.into(), extra.into(), proj_val]
        },
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

// ── Task 2: sender inversion + cycle guard wiring ────────────────────────────

/// Configurable bridge double: answers the member-lineage predicate and the
/// member's current chain depth, and records every convene (with the depth
/// handed to the seam) plus call counts for the two guard queries.
struct InversionBridge {
    is_member: bool,
    current_depth: u32,
    convened: Mutex<Vec<u32>>,
    predicate_calls: Mutex<Vec<(String, String, String, String)>>,
    depth_calls: Mutex<Vec<String>>,
}

impl InversionBridge {
    fn new(is_member: bool, current_depth: u32) -> Arc<Self> {
        Arc::new(Self {
            is_member,
            current_depth,
            convened: Mutex::new(Vec::new()),
            predicate_calls: Mutex::new(Vec::new()),
            depth_calls: Mutex::new(Vec::new()),
        })
    }
    fn total_calls(&self) -> usize {
        self.convened.lock().unwrap().len()
            + self.predicate_calls.lock().unwrap().len()
            + self.depth_calls.lock().unwrap().len()
    }
}

#[async_trait]
impl TeamEngagementBridge for InversionBridge {
    async fn convene_engagement(
        &self,
        _user_id: &str,
        _team_id: &str,
        _project_id: &str,
        root: ConveneRootTask<'_>,
        _envelope: &aionui_api_types::DelegateEnvelopeBlock,
    ) -> Result<ConvenedEngagement, aionui_delegate::bridge::BridgeError> {
        self.convened.lock().unwrap().push(root.depth);
        Ok(ConvenedEngagement {
            engagement_id: "eng-1".into(),
            root_task_id: "task-1".into(),
            lead_slot_id: "lead-1".into(),
        })
    }

    async fn conversation_is_member_of_engagement(
        &self,
        user_id: &str,
        conversation_id: &str,
        team_id: &str,
        project_id: &str,
    ) -> Result<bool, aionui_delegate::bridge::BridgeError> {
        self.predicate_calls.lock().unwrap().push((
            user_id.into(),
            conversation_id.into(),
            team_id.into(),
            project_id.into(),
        ));
        Ok(self.is_member)
    }

    async fn conversation_current_depth(
        &self,
        _user_id: &str,
        conversation_id: &str,
    ) -> Result<u32, aionui_delegate::bridge::BridgeError> {
        self.depth_calls.lock().unwrap().push(conversation_id.into());
        Ok(self.current_depth)
    }
}

fn team_sender_extra() -> &'static str {
    r#"{"teamId":"team-src"}"#
}

/// A team-member sender dispatching to a TEAM is no longer rejected; it
/// convenes at the member's server-derived chain depth + 1 (NOT the
/// agent-supplied `req.depth`, which is a debug aid and cannot bound a loop).
#[tokio::test]
async fn team_sender_dispatch_to_team_convenes_at_depth_plus_one() {
    let bridge = InversionBridge::new(false, 1);
    let (svc, r) = service_with(bridge.clone()).await;
    insert_conversation(&r, "lead-1", team_sender_extra(), Some("proj-1")).await;
    insert_team(&r, "team-2", "Research").await;

    svc.dispatch(
        "user-1",
        "lead-1",
        "Lead",
        dispatch_req("Research", "cross-team work", None),
    )
    .await
    .expect("team-member sender may now dispatch to another team");

    assert_eq!(
        *bridge.convened.lock().unwrap(),
        vec![2],
        "must convene at current_depth(1) + 1"
    );
    assert_eq!(
        *bridge.predicate_calls.lock().unwrap(),
        vec![("user-1".into(), "lead-1".into(), "team-2".into(), "proj-1".into())],
        "member-lineage predicate must run before convene"
    );
}

/// The caller is already a member of the target engagement (A dispatches back
/// into its own engagement): `CycleDetected`, convene never reached.
#[tokio::test]
async fn team_sender_member_of_target_engagement_gets_cycle_detected() {
    let bridge = InversionBridge::new(true, 1);
    let (svc, r) = service_with(bridge.clone()).await;
    insert_conversation(&r, "lead-1", team_sender_extra(), Some("proj-1")).await;
    insert_team(&r, "team-2", "Research").await;

    let err = svc
        .dispatch("user-1", "lead-1", "Lead", dispatch_req("Research", "loop", None))
        .await
        .expect_err("self-member dispatch must be rejected");
    assert!(
        matches!(err, aionui_delegate::error::DelegateError::CycleDetected { .. }),
        "expected CycleDetected, got {err:?}"
    );
    assert!(bridge.convened.lock().unwrap().is_empty(), "must not convene on cycle");
}

/// A member at MAX_DEPTH dispatching onward is bounded: `DepthExceeded`.
#[tokio::test]
async fn team_sender_at_max_depth_gets_depth_exceeded() {
    let bridge = InversionBridge::new(false, MAX_DEPTH);
    let (svc, r) = service_with(bridge.clone()).await;
    insert_conversation(&r, "lead-1", team_sender_extra(), Some("proj-1")).await;
    insert_team(&r, "team-2", "Research").await;

    let err = svc
        .dispatch("user-1", "lead-1", "Lead", dispatch_req("Research", "too deep", None))
        .await
        .expect_err("hop past MAX_DEPTH must be rejected");
    assert!(
        matches!(err, aionui_delegate::error::DelegateError::DepthExceeded { depth, max }
            if depth == MAX_DEPTH + 1 && max == MAX_DEPTH),
        "expected DepthExceeded({}), got {err:?}",
        MAX_DEPTH + 1
    );
    assert!(bridge.convened.lock().unwrap().is_empty(), "must not convene at cap");
}

/// The ASSISTANT leg of the superset: a team-member sender dispatching to an
/// assistant uses the EXISTING per-root deliver path — the bridge is never
/// touched, and the member's own conversation is the chain root governing the
/// existing LIVE_CHAIN / depth / rate guards.
#[tokio::test]
async fn team_sender_dispatch_to_assistant_bypasses_bridge_entirely() {
    let bridge = InversionBridge::new(true, MAX_DEPTH);
    let (svc, r) = service_with(bridge.clone()).await;
    insert_conversation(&r, "lead-1", team_sender_extra(), Some("proj-1")).await;
    insert_team(&r, "team-2", "Research").await;
    insert_assistant(&r, "asstdef-1", "custom-1", "Writer").await;

    let resp = svc
        .dispatch("user-1", "lead-1", "Lead", dispatch_req("Writer", "write it", None))
        .await
        .expect("team-member sender may dispatch to an assistant");

    assert_eq!(resp.to_assistant_id, "asstdef-1", "existing assistant path, unchanged");
    assert_eq!(bridge.total_calls(), 0, "assistant leg must not touch the team bridge");
}

/// Control: a non-team (user-rooted) sender keeps the 4a/4b behavior — the
/// request's own depth is threaded verbatim and the team-side guards (written
/// only for team senders) never run.
#[tokio::test]
async fn user_sender_to_team_unchanged_guards_not_called() {
    let bridge = InversionBridge::new(true, MAX_DEPTH);
    let (svc, r) = service_with(bridge.clone()).await;
    insert_conversation(&r, "conv-1", "{}", Some("proj-1")).await;
    insert_team(&r, "team-2", "Research").await;

    svc.dispatch("user-1", "conv-1", "CEO", dispatch_req("Research", "work it", Some(2)))
        .await
        .expect("user dispatch unchanged");
    assert_eq!(
        *bridge.convened.lock().unwrap(),
        vec![2],
        "user-root carries req.depth verbatim"
    );
    assert!(
        bridge.predicate_calls.lock().unwrap().is_empty(),
        "non-team sender skips member predicate"
    );
    assert!(
        bridge.depth_calls.lock().unwrap().is_empty(),
        "non-team sender skips depth derivation"
    );
}

/// Shipped guidance must match the capability (Phase-6 coupling): Rule 6's
/// "do NOT use this skill" team prohibition is gone. The description budget
/// itself is guarded by `aionui-extension`'s injection-budget test.
#[test]
fn delegate_skill_rule6_no_longer_prohibits_team_senders() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../aionui-app/assets/builtin-skills/auto-inject/delegate/SKILL.md"
    );
    let text = std::fs::read_to_string(path).expect("delegate SKILL.md readable");
    assert!(
        !text.contains("do NOT use this skill"),
        "Rule 6 prohibition is now false guidance"
    );
    assert!(
        text.contains("team send-message"),
        "Rule 6 must still route intra-team work to team tools"
    );
}
