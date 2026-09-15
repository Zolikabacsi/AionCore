//! Composition-layer adapter for the delegated engagement result return
//! (Phase 4b §8, spec §10; team-parent routing added in the Phase 4c final-fix).
//! Implements the `aionui-team` `DelegatedResultDelivery` port over the app's
//! real conversation write so `aionui-team` never depends on
//! `aionui-conversation`/`aionui-session-message` (same-layer domain crates meet
//! only through traits; this file is the only place that knows both sides).
//!
//! Routing (spec §8/§10):
//!  * a caller whose conversation is a TEAM MEMBER (a convene whose `reply_to`
//!    defaulted to the dispatching member's own team-owned conversation) is
//!    delivered into that member's ENGAGEMENT MAILBOX through the team seam —
//!    `ConversationService::send_message` FORBIDS team-owned conversations, so
//!    the user/assistant write can never land a team-parent result.
//!  * every other caller (a user or assistant conversation) is delivered via
//!    `ConversationService::send_message` (the human-send path), byte-identical
//!    to the Phase 4b behavior: a missing / non-owned conversation 404s
//!    (`NotFound`) and no orphan post is made — the returned error is what the
//!    team layer logs (and keeps the result on the task).
//!
//! The team-aware decision lives HERE (the adapter sees both the conversation
//! write and the team service); the team→its-own-mailbox delivery stays inside
//! `aionui-team`, so the layering rule (team must not depend on conversation)
//! is respected.

use std::sync::{Arc, Weak};

use aionui_ai_agent::IWorkerTaskManager;
use aionui_api_types::SendMessageRequest;
use aionui_conversation::ConversationService;
use aionui_team::{DelegatedResultDelivery, TeamError, TeamSessionService};
use async_trait::async_trait;

pub struct DelegatedResultDeliveryAdapter {
    conversation_service: ConversationService,
    task_manager: Arc<dyn IWorkerTaskManager>,
    /// Weak to break the `TeamSessionService` ↔ adapter reference cycle (the
    /// service owns the adapter via `with_result_delivery`). Unset in unit tests
    /// that only exercise the user/assistant `send_message` path.
    team_service: Option<Weak<TeamSessionService>>,
}

impl DelegatedResultDeliveryAdapter {
    pub fn new(conversation_service: ConversationService, task_manager: Arc<dyn IWorkerTaskManager>) -> Self {
        Self {
            conversation_service,
            task_manager,
            team_service: None,
        }
    }

    /// Wire the team seam so a TEAM-MEMBER parent result is routed into its
    /// engagement mailbox instead of the team-rejecting `send_message`. A
    /// `Weak` breaks the ownership cycle (the service holds this adapter).
    #[must_use]
    pub fn with_team_service(mut self, team_service: &Arc<TeamSessionService>) -> Self {
        self.team_service = Some(Arc::downgrade(team_service));
        self
    }
}

#[async_trait]
impl DelegatedResultDelivery for DelegatedResultDeliveryAdapter {
    async fn deliver_result(
        &self,
        user_id: &str,
        caller_conversation_id: &str,
        engagement_id: &str,
        text: &str,
    ) -> Result<(), TeamError> {
        let content = compose_result_body(engagement_id, text);
        // Team-aware routing: a team-member parent must go through the team
        // mailbox (`send_message` FORBIDS team-owned conversations). The team
        // seam resolves the parent session by engagement + verifies `user_id`,
        // so a missing parent surfaces as an error (result stays on the task).
        if let Some(team_service) = self.team_service.as_ref().and_then(Weak::upgrade)
            && let Some((parent_engagement_id, parent_slot_id, parent_team_id)) =
                team_service.delegated_parent_member(caller_conversation_id).await?
        {
            return team_service
                .deliver_child_result(
                    user_id,
                    &parent_engagement_id,
                    &parent_slot_id,
                    &parent_team_id,
                    &content,
                    None,
                )
                .await;
        }
        let request = SendMessageRequest {
            content,
            files: Vec::new(),
            sessions: Vec::new(),
            inject_skills: Vec::new(),
            hidden: false,
        };
        self.conversation_service
            .send_message(user_id, caller_conversation_id, request, &self.task_manager)
            .await
            .map(|_| ())
            .map_err(|error| TeamError::InvalidRequest(format!("delegated result delivery: {error}")))
    }
}

/// Wrap the consolidated result in the `[[AION_DELEGATE]]` envelope shape the
/// caller's session-message handling already recognizes (line-prefix parse,
/// first-match), with the engagement correlation inside the block.
fn compose_result_body(engagement_id: &str, text: &str) -> String {
    format!("[[AION_DELEGATE]]\nkind: Result\nengagement_id: {engagement_id}\n[[/AION_DELEGATE]]\n\n{text}")
}
