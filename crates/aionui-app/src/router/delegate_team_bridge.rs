//! Composition-layer adapter wiring the delegate bridge port (consumer side,
//! `aionui_delegate::bridge`) to the team engagement seam (provider side,
//! `TeamSessionService::convene_delegated_task`). The two domain crates are
//! same-layer and must not depend on each other (AGENTS.md); this file is the
//! only place that knows both.

use std::sync::Arc;

use aionui_api_types::DelegateEnvelopeBlock;
use aionui_delegate::bridge::{BridgeError, ConveneRootTask, ConvenedEngagement, TeamEngagementBridge};
use aionui_delegate::service::compose_delivery_body;
use aionui_team::{TeamError, TeamSessionService};
use async_trait::async_trait;

pub struct TeamEngagementBridgeAdapter {
    team_service: Arc<TeamSessionService>,
}

impl TeamEngagementBridgeAdapter {
    pub fn new(team_service: Arc<TeamSessionService>) -> Self {
        Self { team_service }
    }
}

#[async_trait]
impl TeamEngagementBridge for TeamEngagementBridgeAdapter {
    async fn convene_engagement(
        &self,
        user_id: &str,
        team_id: &str,
        project_id: &str,
        root: ConveneRootTask<'_>,
        envelope: &DelegateEnvelopeBlock,
    ) -> Result<ConvenedEngagement, BridgeError> {
        let mut body = String::new();
        if !root.description.is_empty() {
            body.push_str(root.description);
            body.push('\n');
        }
        if let Some(expected_output) = root.expected_output {
            body.push_str("\nExpected output: ");
            body.push_str(expected_output);
            body.push('\n');
        }
        let envelope_payload = compose_delivery_body(envelope, &body);
        let convened = self
            .team_service
            .convene_delegated_task(
                user_id,
                team_id,
                project_id,
                root.subject,
                root.description,
                root.expected_output,
                &envelope_payload,
                envelope.reply_to.as_deref(),
                root.depth,
            )
            .await
            .map_err(map_team_error)?;
        Ok(ConvenedEngagement {
            engagement_id: convened.engagement_id,
            root_task_id: convened.root_task_id,
            lead_slot_id: convened.lead_slot_id,
        })
    }

    async fn conversation_is_member_of_engagement(
        &self,
        user_id: &str,
        conversation_id: &str,
        team_id: &str,
        project_id: &str,
    ) -> Result<bool, BridgeError> {
        self.team_service
            .conversation_is_member_of_engagement(user_id, conversation_id, team_id, project_id)
            .await
            .map_err(map_team_error)
    }

    async fn conversation_current_depth(&self, user_id: &str, conversation_id: &str) -> Result<u32, BridgeError> {
        self.team_service
            .conversation_current_depth(user_id, conversation_id)
            .await
            .map_err(map_team_error)
    }
}

fn map_team_error(error: TeamError) -> BridgeError {
    match error {
        // The seam guards ownership user-scoped and deliberately collapses
        // "other user's team" into TeamNotFound (never leaks existence).
        TeamError::TeamNotFound(_) => BridgeError::TeamNotFound,
        TeamError::Forbidden(_) => BridgeError::NotOwner,
        TeamError::InvalidRequest(message) if message.contains("project") => BridgeError::ProjectNotFound,
        other => BridgeError::Delivery(other.to_string()),
    }
}
