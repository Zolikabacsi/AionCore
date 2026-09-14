//! Phase 4a Task 2: the delegation roster is a NON-BREAKING SUPERSET — the
//! existing assistant targets are unchanged, and the user's teams are appended
//! as resolvable/listable targets. Routing to the bridge is Task 3.

use std::sync::Arc;

use aionui_ai_agent::agent_task::AgentInstance;
use aionui_ai_agent::types::BuildTaskOptions;
use aionui_ai_agent::{AgentError, IWorkerTaskManager};
use aionui_api_types::{DelegateTargetKind, WebSocketMessage};
use aionui_common::{AgentKillReason, TimestampMs};
use aionui_conversation::service::ConversationService;
use aionui_conversation::skill_resolver::{ResolvedAgentSkill, SkillResolver};
use aionui_db::{
    IConversationRepository, SqliteAcpSessionRepository, SqliteAgentMetadataRepository, SqliteConversationRepository,
    SqliteSettingsRepository, init_database_memory,
};
use aionui_delegate::service::DelegateService;
use aionui_realtime::EventBroadcaster;
use async_trait::async_trait;

use aionui_delegate::bridge::NoopTeamEngagementBridge;

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

async fn service() -> (DelegateService, Arc<SqliteConversationRepository>) {
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
        Arc::new(NoopTeamEngagementBridge),
    );
    (svc, conversation_repo)
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

async fn insert_team(r: &SqliteConversationRepository, id: &str, name: &str, archived: bool) {
    let sql = if archived {
        "INSERT INTO teams (id, user_id, name, created_at, updated_at, archived_at) VALUES (?1,'user-1',?2,0,0,1000)"
    } else {
        "INSERT INTO teams (id, user_id, name, created_at, updated_at) VALUES (?1,'user-1',?2,0,0)"
    };
    r.raw_execute(sql, vec![id.into(), name.into()]).await.unwrap();
}

use aionui_api_types::DelegateTargetsQuery;

#[tokio::test]
async fn list_targets_returns_assistant_and_team_superset() {
    let (svc, r) = service().await;
    insert_assistant(&r, "asstdef-1", "custom-1", "CMO").await;
    insert_team(&r, "team-1", "Growth", false).await;

    let resp = svc
        .list_targets("user-1", DelegateTargetsQuery::default())
        .await
        .unwrap();
    let names: Vec<&str> = resp.items.iter().map(|t| t.name.as_str()).collect();
    // Assistants first (stable order), teams appended.
    assert_eq!(names, vec!["CMO", "Growth"]);

    let asst = &resp.items[0];
    assert_eq!(asst.kind, DelegateTargetKind::Assistant);
    assert_eq!(asst.team_id, None, "assistant keeps no team id");
    assert_eq!(asst.assistant_id, "asstdef-1");
    assert_eq!(asst.backend, "53861a53");
    assert_eq!(asst.name, "CMO");
    assert_eq!(asst.description, None);

    let team = &resp.items[1];
    assert_eq!(team.kind, DelegateTargetKind::Team);
    assert_eq!(team.team_id.as_deref(), Some("team-1"));
    assert_eq!(team.name, "Growth");
}

#[tokio::test]
async fn archived_team_is_not_listed() {
    let (svc, r) = service().await;
    insert_assistant(&r, "asstdef-1", "custom-1", "CMO").await;
    insert_team(&r, "team-arch", "Growth", true).await;
    let resp = svc
        .list_targets("user-1", DelegateTargetsQuery::default())
        .await
        .unwrap();
    assert!(resp.items.iter().all(|t| t.kind != DelegateTargetKind::Team));
}

#[tokio::test]
async fn assistant_only_user_gets_unchanged_list() {
    let (svc, r) = service().await;
    insert_assistant(&r, "asstdef-1", "custom-1", "CMO").await;
    let resp = svc
        .list_targets("user-1", DelegateTargetsQuery::default())
        .await
        .unwrap();
    assert_eq!(resp.items.len(), 1);
    let t = &resp.items[0];
    assert_eq!(t.kind, DelegateTargetKind::Assistant);
    assert_eq!(t.assistant_id, "asstdef-1");
    assert_eq!(t.name, "CMO");
    assert_eq!(t.backend, "53861a53");
    assert_eq!(t.team_id, None);
}

#[tokio::test]
async fn resolve_team_name_by_exact_and_prefix() {
    let (svc, r) = service().await;
    insert_assistant(&r, "asstdef-1", "custom-1", "CMO").await;
    insert_team(&r, "team-1", "Growth Squad", false).await;

    // Exact team name → kind Team + team_id.
    let hit = svc.resolve_target("user-1", "Growth Squad").await.unwrap();
    assert_eq!(hit.kind, DelegateTargetKind::Team);
    assert_eq!(hit.id, "team-1");
    assert_eq!(hit.name, "Growth Squad");

    // Case-insensitive prefix, unambiguous → team.
    let hit = svc.resolve_target("user-1", "growth").await.unwrap();
    assert_eq!(hit.kind, DelegateTargetKind::Team);
    assert_eq!(hit.id, "team-1");

    // Assistant still resolves unchanged.
    let hit = svc.resolve_target("user-1", "CMO").await.unwrap();
    assert_eq!(hit.kind, DelegateTargetKind::Assistant);
    assert_eq!(hit.id, "asstdef-1");
}

#[tokio::test]
async fn shared_name_prefix_is_ambiguous() {
    let (svc, r) = service().await;
    insert_assistant(&r, "asstdef-1", "custom-1", "FooBar").await;
    insert_team(&r, "team-1", "FooBaz", false).await;

    // "Foo" is a non-exact prefix of both → ambiguous (do not silently pick).
    let err = svc.resolve_target("user-1", "Foo").await.unwrap_err();
    assert!(
        matches!(err, aionui_delegate::error::DelegateError::AmbiguousTarget { .. }),
        "expected ambiguity, got {err:?}"
    );
}

#[tokio::test]
async fn assistant_id_direct_hit_wins() {
    let (svc, r) = service().await;
    insert_assistant(&r, "asstdef-direct", "custom-9", "Zeta").await;
    let hit = svc.resolve_target("user-1", "asstdef-direct").await.unwrap();
    assert_eq!(hit.kind, DelegateTargetKind::Assistant);
    assert_eq!(hit.id, "asstdef-direct");
}

#[tokio::test]
async fn empty_roster_yields_target_not_found() {
    let (svc, _r) = service().await;
    let err = svc.resolve_target("user-1", "Nothing").await.unwrap_err();
    assert!(matches!(
        err,
        aionui_delegate::error::DelegateError::TargetNotFound { .. }
    ));
}
