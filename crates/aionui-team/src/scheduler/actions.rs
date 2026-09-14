use super::TeammateManager;
use crate::error::TeamError;
use crate::types::{MailboxMessage, MailboxMessageType, TeammateRole};

impl TeammateManager {
    pub async fn create_task(
        &self,
        subject: &str,
        description: Option<&str>,
        owner: Option<&str>,
        blocked_by: &[String],
        expected_output: Option<&str>,
    ) -> Result<crate::types::TeamTask, TeamError> {
        self.task_board
            .create_task(&self.team_id, subject, description, owner, blocked_by, expected_output)
            .await
    }

    pub async fn update_task(
        &self,
        task_id: &str,
        status: Option<&str>,
        description: Option<String>,
        owner: Option<String>,
        blocked_by: Option<Vec<String>>,
    ) -> Result<crate::types::TeamTask, TeamError> {
        use crate::task_board::TaskUpdate;
        use crate::types::TaskStatus;

        let parsed_status = status.and_then(TaskStatus::parse);

        // Sequential gate (spec §7.2): at most one task may be `InProgress` in the
        // engagement at a time. Reject a start of a *different* task while another
        // is already in progress. The same task re-marking itself in-progress is
        // idempotent and allowed; `hierarchical` skips this check entirely.
        if self.is_sequential()
            && parsed_status == Some(TaskStatus::InProgress)
            && let Some(current) = self.task_board.in_progress_task(&self.team_id).await?
            && current.id != task_id
        {
            return Err(TeamError::SequentialBusy {
                task_id: task_id.to_owned(),
                current_task_id: current.id,
            });
        }

        let update = TaskUpdate {
            status: parsed_status,
            description,
            owner,
            blocked_by,
            ..Default::default()
        };
        self.task_board.update_task(&self.team_id, task_id, &update).await
    }

    /// Finalizes an agent turn by marking the slot idle. The leader re-wake and
    /// idle-notification bookkeeping live in [`mark_idle`](Self::mark_idle).
    pub async fn finalize_turn(&self, slot_id: &str) -> Result<Option<String>, TeamError> {
        self.mark_idle(slot_id, None).await
    }

    /// Records that `slot_id` completed `task_id` on its own account, so the
    /// task's `result` can be captured from the slot's final assistant message
    /// when the turn finalizes (Phase 3a). The completion MCP tool fires
    /// mid-turn — before the assistant text is reliably projected — hence the
    /// deferral to finalize.
    pub async fn note_completion_for_result(&self, slot_id: &str, task_id: &str) {
        self.pending_task_results
            .lock()
            .await
            .entry(slot_id.to_owned())
            .or_default()
            .push(task_id.to_owned());
    }

    /// Drains the slot's pending completions for result capture at turn
    /// finalize. Empty when the slot completed no task this turn.
    pub(crate) async fn take_pending_task_results(&self, slot_id: &str) -> Vec<String> {
        self.pending_task_results
            .lock()
            .await
            .remove(slot_id)
            .unwrap_or_default()
    }

    pub async fn request_shutdown_agent(
        &self,
        from_slot_id: &str,
        target_slot_id: &str,
        reason: Option<&str>,
    ) -> Result<MailboxMessage, TeamError> {
        let from_role = {
            let slots = self.slots.lock().await;
            let slot = slots
                .get(from_slot_id)
                .ok_or_else(|| TeamError::AgentNotFound(from_slot_id.to_owned()))?;
            slot.agent.role
        };

        if from_role != TeammateRole::Lead {
            return Err(TeamError::InvalidRequest("only lead can shutdown agents".into()));
        }

        {
            let slots = self.slots.lock().await;
            let target = slots
                .get(target_slot_id)
                .ok_or_else(|| TeamError::AgentNotFound(target_slot_id.to_owned()))?;
            if target.agent.role == TeammateRole::Lead {
                return Err(TeamError::InvalidRequest("cannot shutdown the team lead".into()));
            }
        }

        self.mailbox
            .write(
                &self.team_id,
                target_slot_id,
                from_slot_id,
                MailboxMessageType::ShutdownRequest,
                reason.unwrap_or("shutdown requested"),
                None,
            )
            .await
    }
}
