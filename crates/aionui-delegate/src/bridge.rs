//! Consumer-side port for the delegation → team-engagement bridge (Phase 4a).
//!
//! `aionui-delegate` must not depend on `aionui-team` (same-layer domain
//! crates talk through traits — AGENTS.md). This module declares the port;
//! `aionui-app` provides the adapter that forwards to
//! `TeamSessionService::convene_delegated_task`.

use aionui_api_types::DelegateEnvelopeBlock;
use async_trait::async_trait;

/// Errors surfaced by the team side of the bridge. Mapping onto
/// `DelegateError` happens at the dispatch site (Task 3), not here.
#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("team not found")]
    TeamNotFound,
    #[error("caller does not own the team")]
    NotOwner,
    #[error("project not found")]
    ProjectNotFound,
    #[error("team engagement delivery failed: {0}")]
    Delivery(String),
}

/// The root task to create on the engagement's board, owned by the lead.
pub struct ConveneRootTask<'a> {
    pub subject: &'a str,
    pub description: &'a str,
    pub expected_output: Option<&'a str>,
    /// Caller's current delegation-chain depth, threaded across the engagement
    /// boundary so the seam can persist it and the lead's onward hop increment
    /// it (Phase 4c, spec §5.9). `0` for a top-level user dispatch.
    pub depth: u32,
}

/// Identifiers of the convened engagement, returned to the dispatch site.
#[derive(Debug, Clone)]
pub struct ConvenedEngagement {
    pub engagement_id: String,
    pub root_task_id: String,
    pub lead_slot_id: String,
}

#[async_trait]
pub trait TeamEngagementBridge: Send + Sync {
    /// Find-or-create the team's engagement for `project_id`, ensure members,
    /// create a root task owned by the lead, and enqueue an envelope to the
    /// lead's engagement mailbox. Returns the engagement id + created task id.
    async fn convene_engagement(
        &self,
        user_id: &str,
        team_id: &str,
        project_id: &str,
        root: ConveneRootTask<'_>,
        envelope: &DelegateEnvelopeBlock,
    ) -> Result<ConvenedEngagement, BridgeError>;

    /// Cross-engagement cycle predicate (Phase 4c, spec §5.9): is
    /// `conversation_id` (the caller about to dispatch) already a member of
    /// `team_id`'s engagement for `project_id`? The dispatch site calls this
    /// BEFORE convening and rejects a `true` with `CycleDetected`.
    ///
    /// Answered team-side via the composition adapter over the existing
    /// `ITeamRepository::get_engagement_member_by_conversation` (no
    /// delegate→team Cargo dep). The default `Ok(false)` keeps a bridge that
    /// has not wired the resolution (Noop, non-team builds, existing doubles)
    /// from ever reporting a cycle, so the additions are inert until Task 2.
    async fn conversation_is_member_of_engagement(
        &self,
        _user_id: &str,
        _conversation_id: &str,
        _team_id: &str,
        _project_id: &str,
    ) -> Result<bool, BridgeError> {
        Ok(false)
    }

    /// Server-side current delegation-chain depth of `conversation_id` when it
    /// is a convened engagement member (Phase 4c, spec §5.9): the
    /// `delegate_depth` persisted on the engagement's delegated root task(s).
    /// A team-member onward dispatch must derive its depth from this instead
    /// of the agent-supplied `req.depth` (a debug aid an LLM can drop or
    /// lie about, which would let a loop slip past `MAX_DEPTH`). `Ok(0)` when
    /// the conversation is not a convened member — it is its own chain root —
    /// and as the trait default, so unwired doubles never shift depths.
    async fn conversation_current_depth(&self, _user_id: &str, _conversation_id: &str) -> Result<u32, BridgeError> {
        Ok(0)
    }
}

/// Default double: the bridge is never wired for unit tests / non-team builds.
/// Returns `Delivery` so a mis-wired dispatch fails loudly instead of hanging.
pub struct NoopTeamEngagementBridge;

#[async_trait]
impl TeamEngagementBridge for NoopTeamEngagementBridge {
    async fn convene_engagement(
        &self,
        _user_id: &str,
        _team_id: &str,
        _project_id: &str,
        _root: ConveneRootTask<'_>,
        _envelope: &DelegateEnvelopeBlock,
    ) -> Result<ConvenedEngagement, BridgeError> {
        Err(BridgeError::Delivery("team engagement bridge not wired".to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::{BridgeError, ConveneRootTask, ConvenedEngagement, NoopTeamEngagementBridge, TeamEngagementBridge};
    use crate::service::DelegateService;
    use aionui_ai_agent::agent_task::AgentInstance;
    use aionui_ai_agent::types::BuildTaskOptions;
    use aionui_ai_agent::{AgentError, IWorkerTaskManager};
    use aionui_api_types::{DelegateEnvelopeBlock, DelegateEnvelopeKind, WebSocketMessage};
    use aionui_common::{AgentKillReason, TimestampMs};
    use aionui_conversation::service::ConversationService;
    use aionui_conversation::skill_resolver::{ResolvedAgentSkill, SkillResolver};
    use aionui_db::{
        SqliteAcpSessionRepository, SqliteAgentMetadataRepository, SqliteConversationRepository,
        SqliteSettingsRepository, init_database_memory,
    };
    use aionui_realtime::EventBroadcaster;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

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

    #[derive(Default)]
    struct RecordingBridge {
        pub calls: Mutex<Vec<(String, String, String, String)>>,
    }

    #[async_trait]
    impl TeamEngagementBridge for RecordingBridge {
        async fn convene_engagement(
            &self,
            user_id: &str,
            team_id: &str,
            project_id: &str,
            root: ConveneRootTask<'_>,
            _envelope: &DelegateEnvelopeBlock,
        ) -> Result<ConvenedEngagement, BridgeError> {
            self.calls
                .lock()
                .unwrap()
                .push((user_id.into(), team_id.into(), project_id.into(), root.subject.into()));
            Ok(ConvenedEngagement {
                engagement_id: "eng-1".into(),
                root_task_id: "task-1".into(),
                lead_slot_id: "lead-1".into(),
            })
        }
    }

    async fn build_service(bridge: Arc<dyn TeamEngagementBridge>) -> DelegateService {
        let db = init_database_memory().await.unwrap();
        let pool = db.pool().clone();
        let conversation_repo = Arc::new(SqliteConversationRepository::new(pool.clone()));
        let task_manager: Arc<dyn IWorkerTaskManager> = Arc::new(NoopTaskManager);
        let conversation_service = ConversationService::new(
            std::env::temp_dir(),
            Arc::new(NullBroadcaster),
            Arc::new(NoSkills),
            task_manager.clone(),
            conversation_repo.clone() as Arc<dyn aionui_db::IConversationRepository>,
            Arc::new(SqliteAgentMetadataRepository::new(pool.clone())) as Arc<dyn aionui_db::IAgentMetadataRepository>,
            Arc::new(SqliteAcpSessionRepository::new(pool.clone())) as Arc<dyn aionui_db::IAcpSessionRepository>,
        );
        DelegateService::new(
            conversation_service,
            conversation_repo as Arc<dyn aionui_db::IConversationRepository>,
            Arc::new(SqliteSettingsRepository::new(pool)) as Arc<dyn aionui_db::ISettingsRepository>,
            Arc::new(NullBroadcaster),
            task_manager,
            bridge,
        )
    }

    #[tokio::test]
    async fn delegate_service_accepts_team_bridge_and_bridge_is_constructible() {
        let mock = Arc::new(RecordingBridge::default());
        let service = build_service(mock.clone()).await;

        // Stored for Task 3; dispatch behavior must not call it yet.
        let convened = service
            .team_bridge
            .convene_engagement(
                "u1",
                "team-1",
                "proj-1",
                ConveneRootTask {
                    subject: "do the thing",
                    description: "",
                    expected_output: None,
                    depth: 0,
                },
                &DelegateEnvelopeBlock {
                    kind: DelegateEnvelopeKind::Dispatch,
                    from_agent_id: "a1".into(),
                    from_agent_name: "Caller".into(),
                    reply_to: Some("conv-1".into()),
                    depth: 0,
                    envelope_id: "env-1".into(),
                    workspace: "/tmp".into(),
                    created_at_ms: 0,
                },
            )
            .await
            .unwrap();
        assert_eq!(convened.engagement_id, "eng-1");
        assert_eq!(convened.root_task_id, "task-1");
        assert_eq!(convened.lead_slot_id, "lead-1");
        assert_eq!(mock.calls.lock().unwrap().len(), 1);

        // Default no-op double compiles and is injectable for existing tests.
        let noop_service = build_service(Arc::new(NoopTeamEngagementBridge)).await;
        let _ = noop_service;
    }
}
