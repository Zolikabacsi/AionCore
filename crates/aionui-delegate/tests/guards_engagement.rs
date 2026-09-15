//! Phase 4b Task 2: the cycle/depth/rate guards for a TEAM dispatch are
//! re-keyed to the ENGAGEMENT (spec §5.9 / §9), while the assistant dispatch
//! guards stay byte-identical.
//!
//! Three things these tests pin down, none of which the old
//! conversation/assistant-keyed guards got right for teams:
//!   * the team rate bucket is engagement-scoped, so team traffic never drains
//!     the assistant target bucket (a team has no `target_assistant_id`);
//!   * a team hop is recorded against its `target_engagement_id`, NOT the
//!     assistant-scoped `target_assistant_id`, so a repeat team dispatch can
//!     never be counted as a self-cycle on the existing engagement (§6 reuse)
//!     and can never false-cycle a later assistant dispatch to a same-id target;
//!   * the same team + project dispatched twice is a legitimate follow-up on the
//!     one engagement, not a cycle, while `MAX_DEPTH` is still enforced.

use std::sync::Arc;
use std::sync::Mutex;

use sqlx::Row;

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
use aionui_delegate::bridge::{ConveneRootTask, ConvenedEngagement, TeamEngagementBridge};
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

/// Emulates spec §6 find-or-create: `UNIQUE(team, project)` → a STABLE
/// engagement id per `(team, project)`, so a same-team + same-project follow-up
/// lands on the one engagement (and a different project is a different
/// engagement). Records the team ids it was asked to convene.
#[derive(Default)]
struct FindOrCreateBridge {
    convened_teams: Mutex<Vec<String>>,
}

#[async_trait]
impl TeamEngagementBridge for FindOrCreateBridge {
    async fn convene_engagement(
        &self,
        _user_id: &str,
        team_id: &str,
        project_id: &str,
        _root: ConveneRootTask<'_>,
        _envelope: &DelegateEnvelopeBlock,
    ) -> Result<ConvenedEngagement, aionui_delegate::bridge::BridgeError> {
        self.convened_teams.lock().unwrap().push(team_id.to_owned());
        Ok(ConvenedEngagement {
            engagement_id: format!("eng-{team_id}-{project_id}"),
            root_task_id: format!("task-{team_id}-{project_id}"),
            lead_slot_id: "lead-1".into(),
        })
    }
}

// ── Harness ──────────────────────────────────────────────────────────────────

async fn service_with(bridge: Arc<FindOrCreateBridge>) -> (DelegateService, Arc<SqliteConversationRepository>) {
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

// ── Rate guard, engagement-scoped ──────────────────────────────────────────────

/// A team hop has no assistant id, so its rate budget must NOT live in the
/// assistant `(caller, target_assistant_id)` namespace: dispatching the SAME
/// team to saturation must not drain the budget for an assistant that happens
/// to share the team's id. Before the re-key the team bucket was keyed on the
/// bare `team_id` (== the assistant's definition id here), so the assistant
/// dispatch was wrongly rate-limited.
#[tokio::test]
async fn team_rate_bucket_is_separate_from_assistant_target_bucket() {
    let bridge = Arc::new(FindOrCreateBridge::default());
    let (svc, r) = service_with(bridge.clone()).await;
    // Team id and assistant definition id are deliberately the SAME string so a
    // target_assistant_id-keyed bucket would collide across the two paths.
    insert_team(&r, "shared-id", "Growth").await;
    insert_assistant(&r, "shared-id", "custom-1", "CMO").await;
    insert_caller(&r, "conv-1", Some("proj-1")).await;

    // Saturate the caller→team engagement bucket (PAIR_LIMIT = 10).
    for _ in 0..aionui_delegate::rate_limit::PAIR_LIMIT {
        svc.dispatch("user-1", "conv-1", "CEO", dispatch_req("Growth", "work it"))
            .await
            .expect("team follow-up allowed up to the pair cap");
    }

    // The assistant target shares the same id but its own bucket, so it must
    // still be allowed — proving the team bucket is engagement-scoped, not
    // `(caller, target_assistant_id)`-keyed.
    let resp = svc
        .dispatch("user-1", "conv-1", "CEO", dispatch_req("CMO", "do it"))
        .await
        .expect("assistant dispatch must not be drained by team traffic");
    assert_eq!(resp.to_assistant_id, "shared-id", "assistant dispatch still routes");
}

/// Exceeding the engagement window still rejects, with the specific typed error
/// (never a vague failure).
#[tokio::test]
async fn team_dispatch_over_rate_window_rejects_rate_limited() {
    let bridge = Arc::new(FindOrCreateBridge::default());
    let (svc, r) = service_with(bridge.clone()).await;
    insert_team(&r, "team-1", "Growth").await;
    insert_caller(&r, "conv-1", Some("proj-1")).await;

    for _ in 0..aionui_delegate::rate_limit::PAIR_LIMIT {
        svc.dispatch("user-1", "conv-1", "CEO", dispatch_req("Growth", "work it"))
            .await
            .expect("within window");
    }
    let err = svc
        .dispatch("user-1", "conv-1", "CEO", dispatch_req("Growth", "work it"))
        .await
        .expect_err("over the pair cap must reject");
    assert!(
        matches!(&err, DelegateError::RateLimited { from, to } if from == "conv-1" && to == "team-1"),
        "expected RateLimited(conv-1, team-1), got {err:?}"
    );
    assert_eq!(err.code(), aionui_api_types::DelegateToolErrorCode::RateLimited);
}

// ── Cycle guard, engagement-attributed ─────────────────────────────────────────

/// A team hop is recorded against its `target_engagement_id`, NOT the
/// assistant-scoped `target_assistant_id`. The engagement column only exists
/// after migration 049, and the assistant column is empty for the team row.
#[tokio::test]
async fn team_hop_is_attributed_to_its_engagement_not_the_assistant_index() {
    let bridge = Arc::new(FindOrCreateBridge::default());
    let (svc, r) = service_with(bridge.clone()).await;
    insert_team(&r, "team-1", "Growth").await;
    insert_caller(&r, "conv-1", Some("proj-1")).await;

    svc.dispatch("user-1", "conv-1", "CEO", dispatch_req("Growth", "work it"))
        .await
        .expect("team dispatch convenes");

    let rows = r
        .raw_query(
            "SELECT target_assistant_id, target_engagement_id FROM delegation_envelopes WHERE root_conversation_id = ?1",
            vec!["conv-1".into()],
        )
        .await
        .expect("engagement lineage column must exist");
    let row = rows.into_iter().next().expect("one audit envelope persisted");
    let asst: String = row.get("target_assistant_id");
    let eng: Option<String> = row.get("target_engagement_id");
    assert!(
        asst.is_empty(),
        "team row must NOT occupy the assistant target index, got {asst:?}"
    );
    assert_eq!(
        eng.as_deref(),
        Some("eng-team-1-proj-1"),
        "team hop is keyed on its engagement"
    );
}

/// A repeat team dispatch onto the SAME engagement is a legitimate follow-up
/// (§5), never a self-cycle, and reuses the one engagement.
#[tokio::test]
async fn same_team_same_project_followup_is_not_a_cycle() {
    let bridge = Arc::new(FindOrCreateBridge::default());
    let (svc, r) = service_with(bridge.clone()).await;
    insert_team(&r, "team-1", "Growth").await;
    insert_caller(&r, "conv-1", Some("proj-1")).await;

    let first = svc
        .dispatch("user-1", "conv-1", "CEO", dispatch_req("Growth", "first"))
        .await
        .expect("first team dispatch convenes");
    let second = svc
        .dispatch("user-1", "conv-1", "CEO", dispatch_req("Growth", "second"))
        .await
        .expect("follow-up on the same engagement is not a cycle");

    assert_eq!(first.engagement_id.as_deref(), Some("eng-team-1-proj-1"));
    assert_eq!(
        second.engagement_id.as_deref(),
        first.engagement_id.as_deref(),
        "same (team, project) reuses the one engagement (§6)"
    );
    assert_eq!(bridge.convened_teams.lock().unwrap().len(), 2);
}

/// The re-key must not pollute the assistant chain: a team row (whose
/// `target_assistant_id` is now empty) must never be matched by the assistant
/// cycle detector, so a later assistant dispatch to a target sharing the team's
/// id is not false-cycled. Before the change the team row carried the team id in
/// `target_assistant_id` and tripped `cycle_detected`.
#[tokio::test]
async fn team_row_does_not_false_cycle_a_later_assistant_dispatch() {
    let bridge = Arc::new(FindOrCreateBridge::default());
    let (svc, r) = service_with(bridge.clone()).await;
    insert_team(&r, "dup-id", "Growth").await;
    insert_assistant(&r, "dup-id", "custom-1", "CMO").await;
    insert_caller(&r, "conv-1", Some("proj-1")).await;

    svc.dispatch("user-1", "conv-1", "CEO", dispatch_req("Growth", "team work"))
        .await
        .expect("team dispatch convenes (live team row under conv-1)");

    // Same-root assistant dispatch whose definition id equals the team's id.
    let resp = svc
        .dispatch("user-1", "conv-1", "CEO", dispatch_req("CMO", "assistant work"))
        .await
        .expect("assistant dispatch must not be false-cycled by a team row");
    assert_eq!(resp.to_assistant_id, "dup-id");
}

// ── Depth guard retained (MAX_DEPTH=3) ───────────────────────────────────────

/// A team dispatch over `MAX_DEPTH` is still rejected before the bridge runs.
#[tokio::test]
async fn team_dispatch_over_max_depth_is_rejected() {
    let bridge = Arc::new(FindOrCreateBridge::default());
    let (svc, r) = service_with(bridge.clone()).await;
    insert_team(&r, "team-1", "Growth").await;
    insert_caller(&r, "conv-1", Some("proj-1")).await;

    let mut req = dispatch_req("Growth", "work it");
    req.depth = Some(MAX_DEPTH + 1);
    let err = svc
        .dispatch("user-1", "conv-1", "CEO", req)
        .await
        .expect_err("depth guard must reject");
    assert!(
        matches!(err, DelegateError::DepthExceeded { depth, max } if depth == MAX_DEPTH + 1 && max == MAX_DEPTH),
        "expected DepthExceeded({MAX_DEPTH}+1, {MAX_DEPTH}), got {err:?}"
    );
    assert!(bridge.convened_teams.lock().unwrap().is_empty(), "bridge must not run");
}

/// `MAX_DEPTH` is still reachable for a team hop (a dispatch at exactly the cap
/// passes) — the retained bound, not silently loosened.
#[tokio::test]
async fn team_dispatch_at_max_depth_is_allowed() {
    let bridge = Arc::new(FindOrCreateBridge::default());
    let (svc, r) = service_with(bridge.clone()).await;
    insert_team(&r, "team-1", "Growth").await;
    insert_caller(&r, "conv-1", Some("proj-1")).await;

    let mut req = dispatch_req("Growth", "work it");
    req.depth = Some(MAX_DEPTH);
    svc.dispatch("user-1", "conv-1", "CEO", req)
        .await
        .expect("dispatch at the depth cap is allowed");
    assert_eq!(bridge.convened_teams.lock().unwrap().len(), 1);
}
