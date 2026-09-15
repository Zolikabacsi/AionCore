//! Phase 4b Task 1 app-adapter test: `DelegatedResultDeliveryAdapter` posts the
//! consolidated engagement result as an inbound agent→user message into the
//! delegating caller conversation via the app's REAL conversation write
//! (`ConversationService::send_message`), owner-scoped. Spec §10: a missing /
//! non-owned caller conversation must NOT be posted to (the error surfaces so
//! the team layer warns and keeps the result on the task).

use std::sync::Arc;

use aionui_ai_agent::agent_task::AgentInstance;
use aionui_ai_agent::types::BuildTaskOptions;
use aionui_ai_agent::{AgentError, IWorkerTaskManager};
use aionui_api_types::WebSocketMessage;
use aionui_app::DelegatedResultDeliveryAdapter;
use aionui_common::{AgentKillReason, TimestampMs};
use aionui_conversation::service::ConversationService;
use aionui_conversation::skill_resolver::{ResolvedAgentSkill, SkillResolver};
use aionui_db::{
    IConversationRepository, SqliteAcpSessionRepository, SqliteAgentMetadataRepository, SqliteConversationRepository,
    init_database_memory,
};
use aionui_realtime::EventBroadcaster;
use aionui_team::DelegatedResultDelivery;
use async_trait::async_trait;
use sqlx::Row;

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

const USER: &str = "caller-user";

struct Harness {
    adapter: DelegatedResultDeliveryAdapter,
    repo: Arc<SqliteConversationRepository>,
    _db: aionui_db::Database,
}

impl Harness {
    async fn new() -> Self {
        let db = init_database_memory().await.unwrap();
        let pool = db.pool().clone();
        let repo = Arc::new(SqliteConversationRepository::new(pool.clone()));
        repo.raw_execute(
            "INSERT OR IGNORE INTO users (id, username, password_hash, created_at, updated_at) VALUES (?1,?1,'x',0,0)",
            vec![USER.into()],
        )
        .await
        .unwrap();
        let task_manager: Arc<dyn IWorkerTaskManager> = Arc::new(NoopTaskManager);
        let conversation_service = ConversationService::new(
            std::env::temp_dir(),
            Arc::new(NullBroadcaster),
            Arc::new(NoSkills),
            task_manager.clone(),
            repo.clone() as Arc<dyn IConversationRepository>,
            Arc::new(SqliteAgentMetadataRepository::new(pool.clone())) as Arc<dyn aionui_db::IAgentMetadataRepository>,
            Arc::new(SqliteAcpSessionRepository::new(pool)) as Arc<dyn aionui_db::IAcpSessionRepository>,
        );
        let adapter = DelegatedResultDeliveryAdapter::new(conversation_service, task_manager);
        Self { adapter, repo, _db: db }
    }

    async fn insert_caller(&self, id: &str, user_id: &str) {
        self.repo
            .raw_execute(
                "INSERT OR IGNORE INTO users (id, username, password_hash, created_at, updated_at) VALUES (?1,?1,'x',0,0)",
                vec![user_id.into()],
            )
            .await
            .unwrap();
        self.repo
            .raw_execute(
                "INSERT INTO conversations (id,user_id,name,type,extra,status,pinned,created_at,updated_at) \
                 VALUES (?1,?2,'caller','acp','{}','finished',0,0,0)",
                vec![id.into(), user_id.into()],
            )
            .await
            .unwrap();
    }

    async fn posted_contents(&self, conversation_id: &str) -> Vec<String> {
        let rows = self
            .repo
            .raw_query(
                "SELECT content FROM messages WHERE conversation_id = ?1 AND position = 'right' ORDER BY created_at",
                vec![conversation_id.into()],
            )
            .await
            .unwrap();
        rows.iter().map(|r| r.get::<String, _>("content").clone()).collect()
    }
}

#[tokio::test]
async fn deliver_result_posts_inbound_agent_to_user_message_to_caller() {
    let h = Harness::new().await;
    h.insert_caller("caller-conv", USER).await;

    h.adapter
        .deliver_result(USER, "caller-conv", "eng-1", "TEAM CONSOLIDATED RESULT")
        .await
        .expect("delivery into an owned caller conversation succeeds");

    let posted = h.posted_contents("caller-conv").await;
    assert_eq!(posted.len(), 1, "exactly one inbound message posted");
    let body = &posted[0];
    assert!(
        body.contains("TEAM CONSOLIDATED RESULT"),
        "the consolidated result text is the message body:\n{body}"
    );
    assert!(
        body.contains("[[AION_DELEGATE]]"),
        "carries the envelope marker:\n{body}"
    );
    assert!(
        body.contains("engagement_id: eng-1"),
        "carries the engagement correlation:\n{body}"
    );
}

#[tokio::test]
async fn deliver_result_to_missing_caller_does_not_post() {
    let h = Harness::new().await;
    // No `ghost-conv` row exists.
    let error = h
        .adapter
        .deliver_result(USER, "ghost-conv", "eng-1", "result")
        .await
        .expect_err("a missing caller conversation must surface an error (spec §10)");
    assert!(error.to_string().to_lowercase().contains("not found"), "{error}");
}

#[tokio::test]
async fn deliver_result_to_another_users_caller_does_not_post() {
    let h = Harness::new().await;
    h.insert_caller("other-conv", "someone-else").await;

    let error = h
        .adapter
        .deliver_result(USER, "other-conv", "eng-1", "result")
        .await
        .expect_err("cross-user delivery must be refused (spec §10)");
    assert!(error.to_string().to_lowercase().contains("not found"), "{error}");

    assert!(
        h.posted_contents("other-conv").await.is_empty(),
        "no orphan message is written into another user's conversation"
    );
}
