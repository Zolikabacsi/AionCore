//! Composition-layer adapter for the delegated engagement result return
//! (Phase 4b §8, spec §10). Implements the `aionui-team` `DelegatedResultDelivery`
//! port over the app's real conversation write so `aionui-team` never depends
//! on `aionui-conversation`/`aionui-session-message` (same-layer domain crates
//! meet only through traits; this file is the only place that knows both sides).
//!
//! The write mirrors the async delegate reply path: the consolidated result is
//! delivered into the caller conversation via `ConversationService::send_message`
//! (the human-send path), which is what cross-session delivery itself uses. That
//! call already enforces spec §10: a missing / non-owned conversation 404s
//! (`NotFound`) and a team-owned one is rejected (`Forbidden`), so no orphan
//! post is ever made — the returned error is what the team layer logs.

use std::sync::Arc;

use aionui_ai_agent::IWorkerTaskManager;
use aionui_api_types::SendMessageRequest;
use aionui_conversation::ConversationService;
use aionui_team::{DelegatedResultDelivery, TeamError};
use async_trait::async_trait;

pub struct DelegatedResultDeliveryAdapter {
    conversation_service: ConversationService,
    task_manager: Arc<dyn IWorkerTaskManager>,
}

impl DelegatedResultDeliveryAdapter {
    pub fn new(conversation_service: ConversationService, task_manager: Arc<dyn IWorkerTaskManager>) -> Self {
        Self {
            conversation_service,
            task_manager,
        }
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
