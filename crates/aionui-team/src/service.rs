mod describe_support;
mod response_builder;
pub(crate) mod spawn_support;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, Weak};

use aionui_ai_agent::{ActiveLeaseRegistry, AgentError, AgentInstance, IWorkerTaskManager, IdleCleanupCoordinator};
use aionui_api_types::ChatFileRef;
use aionui_api_types::{
    AddAgentRequest, AssistantMcpBindingChanged, CreateTeamRequest, GetConfigOptionsResponse,
    InterruptTeamAgentRequest, SetConfigOptionRequest, SetConfigOptionResponse, TeamActivityCursor,
    TeamActivityPageResponse, TeamAgentResponse, TeamAgentRuntimeStatus, TeamContextResetAvailability,
    TeamContextResetResponse, TeamContextResetRuntimeStatus, TeamContextResetStatus, TeamInterruptAgentResponse,
    TeamMailboxMessageResponse, TeamResponse, TeamRunAckResponse, TeamRunStateResponse, TeamSessionBinding,
    TeamSessionPhase, TeamSessionStatus, TeamSessionStatusPayload, TeamTaskResponse, TeamToolCall,
    TeamToolContextResponse, TeamToolErrorCode, TeamToolErrorPayload, TeamToolTransport, WebSocketMessage,
};
use aionui_common::constants::TEAM_NO_PROJECT_SENTINEL;
use aionui_common::{AgentKillReason, ConversationStatus, TimestampMs, generate_id, now_ms};
use aionui_db::models::TeamRow;
use aionui_db::{
    ActivityCursor, IAgentMetadataRepository, IAssistantDefinitionRepository, IAssistantOverlayRepository,
    IProviderRepository, ITeamRepository, IUserOrderStore, OrderItemRef, OrderItemType, PageDirection,
    UpdateTeamParams,
};
use aionui_project::{ProjectService, canonical};
use aionui_realtime::EventBroadcaster;
use dashmap::DashMap;
use tracing::{debug, info, warn};

use crate::activity_mapping::{
    mailbox_row_to_response, message_row_to_activity_item, sort_activity_items, task_row_to_activity_item,
    task_to_response,
};
use crate::error::TeamError;
use crate::event_loop::{AgentLoopContext, EventLoopRegistrationError};
use crate::events::{
    TEAM_CREATED_EVENT, TEAM_REMOVED_EVENT, TEAM_RENAMED_EVENT, TEAM_SESSION_STATUS_CHANGED_EVENT, TeamEventEmitter,
};
use crate::mailbox::Mailbox;
use crate::member_runtime::{
    AttachLease, AttachOutcome, AttachWaiter, BeginRemove, MemberRuntimeFailure, MemberRuntimeSnapshot, ReserveAttach,
};
use crate::message_projection::TeamProjectionMessageStore;
use crate::ports::{
    AgentTurnCancellationPort, AgentTurnExecutionPort, NativeSlashCommandPort, NoopNativeSlashCommandPort,
    TeamAssistantCatalogPort, TeamToolCapabilityPort, UnknownTeamToolCapabilityPort,
};
use crate::prompt_dump::TeamPromptDumpConfig;
use crate::provisioning::{TeamAgentProvisioner, TeamConversationProvisioningPort};
use crate::result_delivery::{DelegatedResultDelivery, delegated_result_metadata};
use crate::runtime_tools::{
    ResolvedTeamToolContext, agent_for_conversation, error_payload, execute_with_scheduler, role_to_tool_role,
};
use crate::session::{
    AgentMessageQueueResult, TeamSession, attach_member_runtime, attach_member_runtime_after_kill,
    spawn_attach_agent_process_bg,
};
use crate::task_board::TaskBoard;
use crate::team_run::TeamRunManager;
use crate::types::{
    MailboxMessageType, Team, TeamAgent, TeamEngagement, TeamEngagementConvened, TeamTask, TeammateRole,
};
use crate::work_coordinator::{
    McpRefreshDisposition, ObserveMessagesResult, RuntimeConstraint, RuntimeRestartRejection,
};
use crate::work_source::WorkSource;
use crate::workspace::validate_create_workspace_path;

/// Default number of activity items returned when the client omits `limit`.
pub const DEFAULT_ACTIVITY_LIMIT: i64 = 500;
/// Hard upper bound for the activity `limit` query parameter.
pub const MAX_ACTIVITY_LIMIT: i64 = 1000;
/// Upper bound on how many task ids one dependency-resolution request may
/// look up, to bound query size regardless of client input.
pub const MAX_TASK_ID_LOOKUP: usize = 200;

/// Which item kinds the unified activity feed returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityKind {
    /// Merged messages and tasks.
    All,
    /// Messages only.
    Message,
    /// Tasks only.
    Task,
}

pub(crate) fn inherit_team_workspace(extra: &mut serde_json::Value, workspace: &str) {
    if !workspace.trim().is_empty() {
        extra["workspace"] = serde_json::Value::String(workspace.to_owned());
    }
}

/// Why a member's model selection is being persisted. Decides whether a runtime
/// that is mid-start may block the write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelPersistTrigger {
    /// A direct preference update from the model endpoint. Nothing has been
    /// applied yet, so a runtime that is mid-start is a legitimate reason to
    /// refuse — the caller can retry once it settles.
    ExplicitRequest,
    /// The member's runtime has already accepted the switch through the generic
    /// config-option path. Persistence must go through regardless of runtime
    /// state: refusing would leave the roster disagreeing with a live runtime.
    RuntimeConfirmed,
}

impl ModelPersistTrigger {
    fn as_str(self) -> &'static str {
        match self {
            Self::ExplicitRequest => "explicit_request",
            Self::RuntimeConfirmed => "runtime_confirmed",
        }
    }
}

struct SessionEntry {
    session: Arc<TeamSession>,
    slow_monitor_handle: tokio::task::JoinHandle<()>,
}

pub struct TeamIdleCleanupCoordinator {
    service: Arc<TeamSessionService>,
    active_leases: Arc<ActiveLeaseRegistry>,
}

impl TeamIdleCleanupCoordinator {
    pub fn new(service: Arc<TeamSessionService>, active_leases: Arc<ActiveLeaseRegistry>) -> Self {
        Self { service, active_leases }
    }
}

#[async_trait::async_trait]
impl IdleCleanupCoordinator for TeamIdleCleanupCoordinator {
    async fn cleanup_idle_conversations(
        &self,
        idle_conversation_ids: Vec<String>,
        idle_threshold_ms: TimestampMs,
    ) -> Vec<String> {
        self.service
            .cleanup_idle_team_runtime_tasks(idle_conversation_ids, &self.active_leases, idle_threshold_ms)
            .await
    }
}

struct MemberRuntimeReconcileWork {
    agent: TeamAgent,
    waiter: AttachWaiter,
    owner: Option<AttachLease>,
}

/// Outcome of the shared engagement-member lookup: `Fallback` means resolve the
/// management op through the `teams.agents` template (legacy / no-project team or a
/// member-read error); `Match` carries the ACTIVE engagement's member row.
enum ActiveMember {
    Fallback,
    Match(aionui_db::models::TeamEngagementMemberRow),
}

pub struct TeamSessionService {
    repo: Arc<dyn ITeamRepository>,
    agent_metadata_repo: Arc<dyn IAgentMetadataRepository>,
    assistant_catalog: Arc<dyn TeamAssistantCatalogPort>,
    assistant_definition_repo: Arc<dyn IAssistantDefinitionRepository>,
    assistant_overlay_repo: Arc<dyn IAssistantOverlayRepository>,
    provider_repo: Arc<dyn IProviderRepository>,
    conversation_port: Arc<dyn TeamConversationProvisioningPort>,
    projection_store: Arc<dyn TeamProjectionMessageStore>,
    broadcaster: Arc<dyn EventBroadcaster>,
    task_manager: Arc<dyn IWorkerTaskManager>,
    turn_port: Arc<dyn AgentTurnExecutionPort>,
    cancellation_port: Arc<dyn AgentTurnCancellationPort>,
    capability_port: Arc<dyn TeamToolCapabilityPort>,
    /// Native slash-command recognizer injected into each `TeamSession`
    /// (ELECTRON-3RN). No-op by default (see `NoopNativeSlashCommandPort`).
    slash_command_port: Arc<dyn NativeSlashCommandPort>,
    backend_binary_path: Arc<PathBuf>,
    prompt_dump: TeamPromptDumpConfig,
    sessions: Arc<DashMap<String, SessionEntry>>,
    /// Per-team mutex serializing membership mutations with session startup so
    /// callers cannot read-modify-write the `agents` JSON or rebuild a runtime
    /// session from a stale roster snapshot.
    ///
    /// Intentionally keyed by `team_id` (NOT engagement): the roster lives on the
    /// shared `teams.agents` template row, so membership RMW is a team-scoped
    /// operation even though `sessions` moved to engagement scope. Two
    /// engagements of one team must still serialize on one lock or they lose
    /// updates to the same row.
    add_agent_locks: Arc<DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Per-team mutex serializing `ensure_session` so concurrent callers cannot
    /// race and start two sessions for the same team.
    ensure_session_locks: Arc<DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Project-bind side branch (optional). `None` → team binding is a no-op,
    /// so team create/read behaves exactly as before.
    project_service: Arc<RwLock<Option<Arc<ProjectService>>>>,
    /// Sidebar ordering store (optional). Set → `remove_team` cascade-deletes the
    /// team's `user_order` rows (design §4.3, path 2). `None` → no-op, so team
    /// deletion behaves exactly as before.
    user_order: Arc<RwLock<Option<Arc<dyn IUserOrderStore>>>>,
    /// Delegated-engagement result-return port (Phase 4b §8), late-injected by
    /// the composition layer exactly like `project_service`/`user_order`.
    /// `None` → a convened root's completion logs a warn and posts nothing;
    /// non-delegated tasks never touch it.
    result_delivery: Arc<RwLock<Option<Arc<dyn DelegatedResultDelivery>>>>,
    /// Back-pointer used by [`TeamSession::spawn_agent`] to reach DB-facing
    /// orchestration without threading the service through every session method.
    /// Stored as `Weak` so the session map does not create a strong cycle with
    /// the service that owns it. Set once during [`TeamSessionService::new`]
    /// via [`Arc::new_cyclic`].
    self_ref: Weak<TeamSessionService>,
}

/// End marker of the `[[AION_DELEGATE]]` envelope block, as composed by
/// `aionui_delegate::service::compose_delivery_body`. The seam stamps its
/// correlation ids *before* this marker (inside the block) so the
/// user-controlled body text that follows can never shadow them: the repo's
/// line-prefix parse convention is first-match (see
/// `aionui-session-message` e2e `extract_reply_to`).
const DELEGATE_ENVELOPE_END_MARKER: &str = "[[/AION_DELEGATE]]";

/// Insert the seam-owned `engagement_id`/`root_task_id` lines into a
/// caller-composed delegate envelope block. A payload without the block
/// marker is not a delegate envelope and is returned unchanged (the seam
/// never formats envelopes).
fn stamp_envelope_correlation(payload: &str, engagement_id: &str, root_task_id: &str) -> String {
    let stamp = format!("engagement_id: {engagement_id}\nroot_task_id: {root_task_id}\n");
    match payload.find(DELEGATE_ENVELOPE_END_MARKER) {
        Some(idx) => format!("{}{}{}", &payload[..idx], stamp, &payload[idx..]),
        None => payload.to_owned(),
    }
}

impl TeamSessionService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        repo: Arc<dyn ITeamRepository>,
        agent_metadata_repo: Arc<dyn IAgentMetadataRepository>,
        assistant_catalog: Arc<dyn TeamAssistantCatalogPort>,
        assistant_definition_repo: Arc<dyn IAssistantDefinitionRepository>,
        assistant_overlay_repo: Arc<dyn IAssistantOverlayRepository>,
        provider_repo: Arc<dyn IProviderRepository>,
        conversation_port: Arc<dyn TeamConversationProvisioningPort>,
        projection_store: Arc<dyn TeamProjectionMessageStore>,
        broadcaster: Arc<dyn EventBroadcaster>,
        task_manager: Arc<dyn IWorkerTaskManager>,
        turn_port: Arc<dyn AgentTurnExecutionPort>,
        cancellation_port: Arc<dyn AgentTurnCancellationPort>,
        backend_binary_path: Arc<PathBuf>,
    ) -> Arc<Self> {
        Self::new_with_prompt_dump(
            repo,
            agent_metadata_repo,
            assistant_catalog,
            assistant_definition_repo,
            assistant_overlay_repo,
            provider_repo,
            conversation_port,
            projection_store,
            broadcaster,
            task_manager,
            turn_port,
            cancellation_port,
            Arc::new(NoopNativeSlashCommandPort),
            Arc::new(UnknownTeamToolCapabilityPort),
            backend_binary_path,
            TeamPromptDumpConfig::disabled(),
        )
    }

    /// Construct with an explicit backend-capability resolver while keeping
    /// the default no-op slash catalog and disabled prompt dump.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_capability_port(
        repo: Arc<dyn ITeamRepository>,
        agent_metadata_repo: Arc<dyn IAgentMetadataRepository>,
        assistant_catalog: Arc<dyn TeamAssistantCatalogPort>,
        assistant_definition_repo: Arc<dyn IAssistantDefinitionRepository>,
        assistant_overlay_repo: Arc<dyn IAssistantOverlayRepository>,
        provider_repo: Arc<dyn IProviderRepository>,
        conversation_port: Arc<dyn TeamConversationProvisioningPort>,
        projection_store: Arc<dyn TeamProjectionMessageStore>,
        broadcaster: Arc<dyn EventBroadcaster>,
        task_manager: Arc<dyn IWorkerTaskManager>,
        turn_port: Arc<dyn AgentTurnExecutionPort>,
        cancellation_port: Arc<dyn AgentTurnCancellationPort>,
        capability_port: Arc<dyn TeamToolCapabilityPort>,
        backend_binary_path: Arc<PathBuf>,
    ) -> Arc<Self> {
        Self::new_with_prompt_dump(
            repo,
            agent_metadata_repo,
            assistant_catalog,
            assistant_definition_repo,
            assistant_overlay_repo,
            provider_repo,
            conversation_port,
            projection_store,
            broadcaster,
            task_manager,
            turn_port,
            cancellation_port,
            Arc::new(NoopNativeSlashCommandPort),
            capability_port,
            backend_binary_path,
            TeamPromptDumpConfig::disabled(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_prompt_dump(
        repo: Arc<dyn ITeamRepository>,
        agent_metadata_repo: Arc<dyn IAgentMetadataRepository>,
        assistant_catalog: Arc<dyn TeamAssistantCatalogPort>,
        assistant_definition_repo: Arc<dyn IAssistantDefinitionRepository>,
        assistant_overlay_repo: Arc<dyn IAssistantOverlayRepository>,
        provider_repo: Arc<dyn IProviderRepository>,
        conversation_port: Arc<dyn TeamConversationProvisioningPort>,
        projection_store: Arc<dyn TeamProjectionMessageStore>,
        broadcaster: Arc<dyn EventBroadcaster>,
        task_manager: Arc<dyn IWorkerTaskManager>,
        turn_port: Arc<dyn AgentTurnExecutionPort>,
        cancellation_port: Arc<dyn AgentTurnCancellationPort>,
        slash_command_port: Arc<dyn NativeSlashCommandPort>,
        capability_port: Arc<dyn TeamToolCapabilityPort>,
        backend_binary_path: Arc<PathBuf>,
        prompt_dump: TeamPromptDumpConfig,
    ) -> Arc<Self> {
        Arc::new_cyclic(|weak| Self {
            repo,
            agent_metadata_repo,
            assistant_catalog,
            assistant_definition_repo,
            assistant_overlay_repo,
            provider_repo,
            conversation_port,
            projection_store,
            broadcaster,
            task_manager,
            turn_port,
            cancellation_port,
            slash_command_port,
            capability_port,
            backend_binary_path,
            prompt_dump,
            sessions: Arc::new(DashMap::new()),
            add_agent_locks: Arc::new(DashMap::new()),
            ensure_session_locks: Arc::new(DashMap::new()),
            project_service: Arc::new(RwLock::new(None)),
            user_order: Arc::new(RwLock::new(None)),
            result_delivery: Arc::new(RwLock::new(None)),
            self_ref: weak.clone(),
        })
    }

    pub(crate) fn provisioner(&self) -> TeamAgentProvisioner {
        TeamAgentProvisioner::new(
            self.repo.clone(),
            self.agent_metadata_repo.clone(),
            self.assistant_catalog.clone(),
            self.provider_repo.clone(),
            self.conversation_port.clone(),
            self.capability_port.clone(),
        )
    }

    /// Apply an assistant MCP binding event to matching members in active team
    /// sessions. Persisted snapshots update immediately; ready idle runtimes are
    /// rebuilt now, while active work records a deferred refresh.
    pub async fn handle_assistant_mcp_binding_changed(&self, event: AssistantMcpBindingChanged) {
        let sessions = self
            .sessions
            .iter()
            .filter(|entry| entry.session.user_id() == event.user_id)
            .map(|entry| Arc::clone(&entry.session))
            .collect::<Vec<_>>();
        for session in sessions {
            let agents = session.scheduler().list_agents().await;
            for agent in agents
                .into_iter()
                .filter(|agent| agent.assistant_id.as_deref() == Some(event.assistant_id.as_str()))
            {
                self.refresh_member_mcp_binding(&session, &event.user_id, &agent).await;
            }
        }
    }

    /// Re-resolve the MCP binding of EVERY member in EVERY active session.
    ///
    /// Recovery path for when binding-change events were missed rather than
    /// observed — the shared event bus can drop events under load, and a dropped
    /// event would otherwise leave a member running a stale MCP set until its
    /// next attach. Idempotent: members whose fingerprint already matches take
    /// the `Unchanged` branch and are left alone.
    pub async fn reconcile_all_assistant_mcp_bindings(&self) {
        let sessions = self
            .sessions
            .iter()
            .map(|entry| Arc::clone(&entry.session))
            .collect::<Vec<_>>();
        let session_count = sessions.len();
        let mut member_count = 0usize;
        for session in sessions {
            let user_id = session.user_id().to_owned();
            for agent in session.scheduler().list_agents().await {
                member_count += 1;
                self.refresh_member_mcp_binding(&session, &user_id, &agent).await;
            }
        }
        info!(
            session_count,
            member_count, "reconciled assistant MCP bindings across active team sessions"
        );
    }

    /// Refresh one member's persisted MCP snapshot and decide what to do with its
    /// runtime: leave dormant/failed slots alone, defer while attaching or
    /// removing, and restart a ready idle runtime so it picks the new set up.
    async fn refresh_member_mcp_binding(&self, session: &Arc<TeamSession>, user_id: &str, agent: &TeamAgent) {
        let fingerprint = match self.provisioner().refresh_agent_mcp_snapshot(user_id, agent).await {
            Ok(Some(fingerprint)) => fingerprint,
            Ok(None) => return,
            Err(error) => {
                warn!(
                    team_id = session.team_id(),
                    slot_id = agent.slot_id,
                    assistant_id = agent.assistant_id.as_deref().unwrap_or_default(),
                    error = %error,
                    "assistant MCP snapshot refresh failed"
                );
                return;
            }
        };
        match session.member_runtimes().snapshot(&agent.slot_id) {
            MemberRuntimeSnapshot::Absent
            | MemberRuntimeSnapshot::Failed { .. }
            | MemberRuntimeSnapshot::SessionStopped => {}
            MemberRuntimeSnapshot::Attaching { .. } | MemberRuntimeSnapshot::Removing { .. } => {
                session
                    .work_coordinator()
                    .defer_mcp_refresh(&agent.slot_id, &fingerprint);
            }
            MemberRuntimeSnapshot::Ready => {
                match session
                    .work_coordinator()
                    .request_mcp_refresh(&agent.slot_id, &fingerprint)
                {
                    McpRefreshDisposition::Unchanged | McpRefreshDisposition::Deferred => {}
                    McpRefreshDisposition::RestartNow => {
                        if let Err(error) = self
                            .restart_agent_runtime_for_mcp_refresh(user_id, session.team_id(), &agent.slot_id)
                            .await
                        {
                            session
                                .work_coordinator()
                                .defer_mcp_refresh(&agent.slot_id, &fingerprint);
                            warn!(
                                team_id = session.team_id(),
                                slot_id = agent.slot_id,
                                error = %error,
                                "assistant MCP runtime refresh deferred after restart race"
                            );
                        }
                    }
                }
            }
        }
    }

    /// Inject the project-bind service (project-bind side branch). When unset,
    /// binding/backfill are no-ops.
    pub fn with_project_service(&self, project_service: Arc<ProjectService>) {
        if let Ok(mut guard) = self.project_service.write() {
            *guard = Some(project_service);
        }
    }

    /// Inject the sidebar ordering store so `remove_team` cascade-deletes the
    /// team's `user_order` rows (design §4.3, path 2). When unset, the cascade is
    /// a no-op. Member conversations are handled separately by the conversation
    /// delete hook (they route through `ConversationService::delete`).
    pub fn with_user_order_store(&self, user_order: Arc<dyn IUserOrderStore>) {
        if let Ok(mut guard) = self.user_order.write() {
            *guard = Some(user_order);
        }
    }

    /// Inject the delegated-engagement result-return port (Phase 4b §8, spec
    /// §10). When unset, a completed delegated root warns and the result stays
    /// only on the task; ordinary team tasks never reach this.
    pub fn with_result_delivery(&self, result_delivery: Arc<dyn DelegatedResultDelivery>) {
        if let Ok(mut guard) = self.result_delivery.write() {
            *guard = Some(result_delivery);
        }
    }

    /// The result-return port, resolved by the session's capture hook at
    /// finalize time. `None` → not wired (test / non-app builds).
    pub(crate) fn result_delivery(&self) -> Option<Arc<dyn DelegatedResultDelivery>> {
        self.result_delivery.read().ok().and_then(|guard| guard.clone())
    }

    /// Best-effort cascade of a removed team's `user_order` row (design §4.3,
    /// path 2). Store unset → no-op. An error is logged, not propagated: an
    /// orphan `team` row self-heals on read (the pinned group only emits teams
    /// present in the live aggregate), so it must never block team deletion.
    async fn remove_team_order_row(&self, user_id: &str, team_id: &str) {
        let store = self.user_order.read().ok().and_then(|guard| guard.clone());
        let Some(store) = store else { return };
        let item = OrderItemRef::new(OrderItemType::Team, team_id);
        if let Err(err) = store.remove_item(user_id, &item).await {
            warn!(
                user_id = %user_id,
                team_id = %team_id,
                error = %err,
                "sidebar: failed to cascade-delete user_order row for removed team"
            );
        }
    }

    /// Resolve a team workspace into `(project_id, folder_id)`. Best-effort:
    /// missing service / empty workspace / bad URI / resolve error → `(None, None)`,
    /// logged at `warn`. Never affects team create/read. LEGACY: only seeds the
    /// team's default/active-project marker; runtime member routing is per
    /// engagement, never via a single-team-project rebind of shared conversations.
    async fn resolve_binding_best_effort(&self, user_id: &str, workspace: &str) -> (Option<String>, Option<String>) {
        let project_service = self.project_service.read().ok().and_then(|guard| guard.clone());
        let Some(project_service) = project_service else {
            return (None, None);
        };
        if workspace.trim().is_empty() {
            return (None, None);
        }
        let uri = match canonical::to_file_uri(Path::new(workspace)) {
            Ok(uri) => uri,
            Err(err) => {
                warn!(error = err.code(), "team project bind skipped: bad workspace uri");
                return (None, None);
            }
        };
        match project_service.resolve_existing(user_id, uri).await {
            Ok(out) => (Some(out.project.project_id), Some(out.folder.folder_id)),
            Err(err) => {
                warn!(error = err.code(), "team project bind skipped");
                (None, None)
            }
        }
    }

    /// Resolve a project_id into the team's workspace folder path and folder_id.
    /// Errors if the project does not exist or has no workspace folder.
    async fn resolve_project_workspace(&self, user_id: &str, project_id: &str) -> Result<(String, String), TeamError> {
        let project_service = self.project_service.read().ok().and_then(|guard| guard.clone());
        let Some(project_service) = project_service else {
            return Err(TeamError::InvalidRequest("project service unavailable".into()));
        };
        let detail = project_service
            .get_project(user_id, project_id)
            .await
            .map_err(|err| TeamError::InvalidRequest(format!("failed to resolve project: {err}")))?;
        let workspace_entry = detail
            .explorer
            .entries
            .iter()
            .find(|e| e.role == "workspace")
            .ok_or_else(|| TeamError::InvalidRequest("project has no workspace folder".into()))?;
        let path = canonical::uri_to_path(&workspace_entry.folder.resource_uri)
            .map_err(|err| TeamError::InvalidRequest(format!("invalid project workspace uri: {err}")))?;
        Ok((
            path.to_string_lossy().into_owned(),
            workspace_entry.folder.folder_id.clone(),
        ))
    }

    /// Lazily backfill `teams.project_id`/`folder_id` on read. Best-effort;
    /// no-op when already bound, workspace empty, or service unset.
    async fn backfill_team_binding_best_effort(&self, row: &TeamRow) {
        if row.project_id.is_some() || row.workspace.trim().is_empty() {
            return;
        }
        let (Some(project_id), Some(folder_id)) = self.resolve_binding_best_effort(&row.user_id, &row.workspace).await
        else {
            return;
        };
        let params = UpdateTeamParams {
            project_id: Some(project_id),
            folder_id: Some(folder_id),
            ..Default::default()
        };
        if let Err(err) = self.repo.update_team(&row.user_id, &row.id, &params).await {
            warn!(team_id = %row.id, error = %err, "team project bind: backfill update failed");
        }
    }

    async fn load_owned_team(&self, user_id: &str, team_id: &str) -> Result<Team, TeamError> {
        let row = self.load_owned_team_row(user_id, team_id).await?;
        Ok(Team::from_row(&row)?)
    }

    async fn load_owned_team_row(&self, user_id: &str, team_id: &str) -> Result<TeamRow, TeamError> {
        self.repo
            .get_team(user_id, team_id)
            .await?
            .ok_or_else(|| TeamError::TeamNotFound(team_id.into()))
    }

    pub(crate) async fn team_owner_user_id(&self, team_id: &str) -> Result<String, TeamError> {
        let row = self
            .repo
            .get_team_for_restore(team_id)
            .await?
            .ok_or_else(|| TeamError::TeamNotFound(team_id.into()))?;
        Ok(row.user_id)
    }

    /// Returns the most recent team-wide mailbox messages (all recipients),
    /// newest first, for the read-only activity view. `limit` is clamped to
    /// `[1, MAX_ACTIVITY_LIMIT]`. Ownership is enforced first via a scoped
    /// lookup: a missing team and another user's team both surface as
    /// `TeamNotFound`, so team existence is never leaked across users.
    pub async fn list_team_mailbox(
        &self,
        user_id: &str,
        team_id: &str,
        limit: i64,
    ) -> Result<Vec<TeamMailboxMessageResponse>, TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        let clamped = limit.clamp(1, MAX_ACTIVITY_LIMIT);
        let rows = self.repo.list_messages_by_team(team_id, clamped).await?;
        let responses: Vec<TeamMailboxMessageResponse> = rows.iter().map(mailbox_row_to_response).collect();
        info!(kind = "team", team_id, count = responses.len(), "team mailbox listed");
        Ok(responses)
    }

    /// Returns the team's tasks, newest first (`created_at` DESC, `id` as a
    /// stable secondary key), truncated to a clamped `limit`, for the
    /// read-only activity view. Reuses the existing ASC `list_tasks` and sorts
    /// in the service. Ownership is enforced first.
    pub async fn list_team_tasks(
        &self,
        user_id: &str,
        team_id: &str,
        limit: i64,
    ) -> Result<Vec<TeamTaskResponse>, TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        let clamped = limit.clamp(1, MAX_ACTIVITY_LIMIT);
        let rows = self.repo.list_tasks(user_id, team_id).await?;
        let mut tasks: Vec<TeamTask> = rows.iter().filter_map(|r| TeamTask::from_row(r).ok()).collect();
        tasks.sort_by(|a, b| b.created_at.cmp(&a.created_at).then_with(|| b.id.cmp(&a.id)));
        tasks.truncate(clamped as usize);
        let responses: Vec<TeamTaskResponse> = tasks.iter().map(task_to_response).collect();
        info!(kind = "team", team_id, count = responses.len(), "team tasks listed");
        Ok(responses)
    }

    /// Returns the team's tasks matching `ids` (newest first), for resolving
    /// dependency (`blocked_by`) subjects that may lie outside the loaded
    /// activity page. Ownership is enforced first; `ids` is clamped to
    /// `MAX_TASK_ID_LOOKUP`. An empty `ids` yields an empty result.
    pub async fn list_team_tasks_by_ids(
        &self,
        user_id: &str,
        team_id: &str,
        ids: &[String],
    ) -> Result<Vec<TeamTaskResponse>, TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let capped = &ids[..ids.len().min(MAX_TASK_ID_LOOKUP)];
        let rows = self.repo.list_tasks_by_ids(user_id, team_id, capped).await?;
        let tasks: Vec<TeamTask> = rows.iter().filter_map(|r| TeamTask::from_row(r).ok()).collect();
        let responses: Vec<TeamTaskResponse> = tasks.iter().map(task_to_response).collect();
        info!(
            kind = "team",
            team_id,
            count = responses.len(),
            "team tasks resolved by ids"
        );
        Ok(responses)
    }

    /// Returns one keyset-paginated page of the unified activity feed (messages
    /// and/or tasks per `kind`), ordered by `(created_at, id)` in `direction`.
    /// Ownership is enforced first, so another user's team is indistinguishable
    /// from a missing one (`TeamNotFound`). For `kind = All`, each stream is
    /// fetched up to `limit` rows and merged; the global top-`limit` is
    /// mathematically complete (any item newer/older than the cursor is within
    /// its own stream's top-`limit`). `has_more` is conservative: a full sub-
    /// query or a post-merge truncation both flag "possibly more".
    pub async fn list_team_activity(
        &self,
        user_id: &str,
        team_id: &str,
        cursor: Option<ActivityCursor>,
        direction: PageDirection,
        kind: ActivityKind,
        limit: i64,
    ) -> Result<TeamActivityPageResponse, TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        let limit = limit.clamp(1, MAX_ACTIVITY_LIMIT);

        let (mut items, mailbox_full, tasks_full) = match kind {
            ActivityKind::Message => {
                let rows = self
                    .repo
                    .list_messages_by_team_paged(team_id, cursor.clone(), direction, limit)
                    .await?;
                let full = rows.len() as i64 == limit;
                (
                    rows.iter().map(message_row_to_activity_item).collect::<Vec<_>>(),
                    full,
                    false,
                )
            }
            ActivityKind::Task => {
                let rows = self
                    .repo
                    .list_tasks_paged(user_id, team_id, cursor.clone(), direction, limit)
                    .await?;
                let full = rows.len() as i64 == limit;
                (
                    rows.iter().filter_map(task_row_to_activity_item).collect::<Vec<_>>(),
                    false,
                    full,
                )
            }
            ActivityKind::All => {
                let msgs = self
                    .repo
                    .list_messages_by_team_paged(team_id, cursor.clone(), direction, limit)
                    .await?;
                let tasks = self
                    .repo
                    .list_tasks_paged(user_id, team_id, cursor.clone(), direction, limit)
                    .await?;
                let mailbox_full = msgs.len() as i64 == limit;
                let tasks_full = tasks.len() as i64 == limit;
                let mut merged: Vec<_> = msgs
                    .iter()
                    .map(message_row_to_activity_item)
                    .chain(tasks.iter().filter_map(task_row_to_activity_item))
                    .collect();
                sort_activity_items(&mut merged, direction);
                (merged, mailbox_full, tasks_full)
            }
        };

        // Truncate to the top `limit`; whether we cut anything feeds `has_more`.
        let truncated = items.len() as i64 > limit;
        items.truncate(limit as usize);

        let has_more = mailbox_full || tasks_full || truncated;
        let next_cursor = if has_more {
            items.last().map(|i| TeamActivityCursor {
                ts: i.created_at,
                id: i.id.clone(),
            })
        } else {
            None
        };

        info!(
            kind = "team",
            team_id,
            count = items.len(),
            first_page = cursor.is_none(),
            "team activity listed"
        );

        Ok(TeamActivityPageResponse {
            items,
            next_cursor,
            has_more,
        })
    }

    pub async fn renew_active_lease(
        &self,
        user_id: &str,
        team_id: &str,
        active_leases: &ActiveLeaseRegistry,
    ) -> Result<(), TeamError> {
        let team = match self.repo.get_team(user_id, team_id).await {
            Ok(Some(row)) => Team::from_row(&row).map_err(TeamError::from),
            Ok(None) => Err(TeamError::TeamNotFound(team_id.to_owned())),
            Err(error) => Err(TeamError::Database(error)),
        };
        let team = match team {
            Ok(team) => team,
            Err(error @ TeamError::TeamNotFound(_)) => {
                debug!(
                    kind = "team",
                    team_id,
                    user_id,
                    error = %error,
                    "Team active lease renew rejected"
                );
                return Err(error);
            }
            Err(error) => {
                warn!(
                    kind = "team",
                    team_id,
                    user_id,
                    error = %error,
                    "Team active lease renew failed"
                );
                return Err(error);
            }
        };

        let conversation_ids = team
            .agents
            .iter()
            .map(|agent| agent.conversation_id.as_str())
            .filter(|conversation_id| !conversation_id.trim().is_empty());
        let (covered_count, expires_at) = active_leases.renew_many(conversation_ids);

        debug!(
            kind = "team",
            team_id, covered_count, expires_at, "Team active lease renewed"
        );
        Ok(())
    }

    /// Restore sessions for all existing teams. Called once at app startup
    /// so that MCP servers are available before any user sends a message.
    pub async fn restore_all_sessions(&self) {
        let teams = match self.repo.list_teams_for_restore().await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, "failed to list teams for session restore");
                return;
            }
        };
        for team in &teams {
            if let Err(e) = self.ensure_session_inner(&team.id, None).await {
                tracing::warn!(team_id = %team.id, error = %e, "failed to restore session on startup");
                continue;
            }
        }
        if !teams.is_empty() {
            tracing::info!(count = teams.len(), "team sessions restored on startup");
        }
    }

    /// Find-or-create the engagement binding `team_id` to `project_id`,
    /// reusing the same project→workspace resolution as `create_team`.
    ///
    /// `project_id` may be the [`TEAM_NO_PROJECT_SENTINEL`] (a project-less
    /// caller, Phase 4a pre-selector). A sentinel request maps to the team's
    /// *default* engagement — `COALESCE(teams.project_id, '__none__')`, the
    /// exact row `create_team` and migrations 045/047 backfill — so it is
    /// convened/reused, never an orphan `__none__` row the team runtime does
    /// not poll. The workspace comes from the resolved project, or from the
    /// team template row when the default engagement is itself sentinel.
    pub async fn ensure_engagement(
        &self,
        user_id: &str,
        team_id: &str,
        project_id: &str,
    ) -> Result<TeamEngagement, TeamError> {
        // Ownership first: a missing team and another user's team both surface
        // as `TeamNotFound`, and the sentinel path needs the team row anyway.
        let team_row = self.load_owned_team_row(user_id, team_id).await?;
        let project_id = if project_id == TEAM_NO_PROJECT_SENTINEL {
            team_row
                .project_id
                .as_deref()
                .unwrap_or(TEAM_NO_PROJECT_SENTINEL)
                .to_owned()
        } else {
            project_id.to_owned()
        };
        let workspace = if project_id == TEAM_NO_PROJECT_SENTINEL {
            team_row.workspace.clone()
        } else {
            let (workspace, _folder_id) = self.resolve_project_workspace(user_id, &project_id).await?;
            workspace
        };
        let row = self
            .repo
            .find_or_create_engagement(user_id, team_id, &project_id, &workspace)
            .await?;
        // Materialize this engagement's own member conversations (one per template
        // slot), each bound to the engagement workspace. Idempotent. The team row
        // is ownership-scoped by `get_team`, matching the guard already applied to
        // the engagement lookup above.
        let team = Team::from_row(&team_row)?;
        self.provisioner()
            .ensure_engagement_members(user_id, &row, &team)
            .await?;
        Ok(TeamEngagement::from_row(&row))
    }

    /// "Convene engagement" seam for the delegation bridge (Phase 4a): bind the
    /// team to the project's engagement (find-or-create, members materialized),
    /// create the root task on that engagement's board owned by the lead, and
    /// enqueue the caller-composed `[[AION_DELEGATE]]` envelope into the lead's
    /// engagement mailbox. `envelope_payload` is pre-composed by the caller
    /// (via `compose_delivery_body`) — this seam never formats envelopes; it
    /// only stamps the engagement/root-task correlation ids it creates into
    /// the `[[AION_DELEGATE]]` block (see [`stamp_envelope_correlation`]).
    ///
    /// Ownership: the team is loaded user-scoped FIRST, so a missing team and
    /// another user's team both surface as `TeamNotFound` (existence is never
    /// leaked, same convention as the read paths).
    ///
    /// `reply_to` (the delegating caller conversation from the 4a envelope) is
    /// correlated onto the ROOT task's `metadata` as
    /// `{delegate_reply_to, engagement_id}` so the Phase 3a result-capture hook
    /// can return the consolidated result to the caller without a second
    /// lookup (spec §8, Phase 4b ruling 1). No reply target → no metadata →
    /// the completion path is byte-identical to Phase 3a.
    #[allow(clippy::too_many_arguments)]
    pub async fn convene_delegated_task(
        &self,
        user_id: &str,
        team_id: &str,
        project_id: &str,
        subject: &str,
        description: &str,
        expected_output: Option<&str>,
        envelope_payload: &str,
        reply_to: Option<&str>,
    ) -> Result<TeamEngagementConvened, TeamError> {
        self.load_owned_team_row(user_id, team_id).await?;
        let engagement = self.ensure_engagement(user_id, team_id, project_id).await?;
        let members = self.repo.list_engagement_members(user_id, &engagement.id).await?;
        let lead = members
            .iter()
            .find(|member| member.role == "lead")
            .ok_or_else(|| TeamError::InvalidRequest(format!("engagement {} has no lead member", engagement.id)))?;

        let emitter = Arc::new(TeamEventEmitter::new(
            team_id.to_owned(),
            user_id.to_owned(),
            self.broadcaster.clone(),
        ));
        let task = TaskBoard::new_for_user(self.repo.clone(), user_id)
            .with_events(emitter.clone())
            .with_engagement(engagement.id.clone())
            .with_task_metadata(reply_to.map(|reply_to| delegated_result_metadata(reply_to, &engagement.id)))
            .create_task(
                team_id,
                subject,
                Some(description),
                Some(lead.slot_id.as_str()),
                &[],
                expected_output,
            )
            .await?;
        // Spec §5 step 4 (Task-1 deferral #4): stamp the correlation ids this
        // seam just created into the envelope. They go *inside* the
        // `[[AION_DELEGATE]]` block (before the closing marker) so they share
        // `reply_to`'s shadow-safe position: the repo's line-prefix parse is
        // first-match (see aionui-session-message e2e `extract_reply_to`), and
        // the user-controlled body text comes after the block, so a crafted
        // `root_task_id:` line in a message can never shadow the real one.
        // A payload without the marker (not a delegate envelope) is left
        // byte-identical — the seam does not format envelopes.
        let envelope_payload = stamp_envelope_correlation(envelope_payload, &engagement.id, &task.id);
        Mailbox::new_for_user(self.repo.clone(), user_id)
            .with_events(emitter)
            .with_engagement(engagement.id.clone())
            .write(
                team_id,
                &lead.slot_id,
                "user",
                MailboxMessageType::Message,
                &envelope_payload,
                Some(subject),
            )
            .await?;

        info!(
            kind = "team",
            team_id,
            engagement_id = %engagement.id,
            task_id = %task.id,
            lead_slot_id = %lead.slot_id,
            "delegated task convened on engagement"
        );
        Ok(TeamEngagementConvened {
            engagement_id: engagement.id,
            root_task_id: task.id,
            lead_slot_id: lead.slot_id.clone(),
        })
    }

    pub async fn create_team(&self, user_id: &str, req: CreateTeamRequest) -> Result<TeamResponse, TeamError> {
        if req.agents.is_empty() {
            return Err(TeamError::InvalidRequest("at least one agent is required".into()));
        }
        if req
            .agents
            .iter()
            .any(|agent| agent.conversation_id.as_deref().is_some_and(|id| !id.trim().is_empty()))
        {
            return Err(TeamError::InvalidRequest(
                "creating Team agents from existing conversations are no longer supported; omit agents[].conversation_id"
                    .into(),
            ));
        }

        // Resolve workspace from explicit project binding, explicit workspace path,
        // or leave blank to derive from the leader agent's conversation.
        let (shared_workspace, initial_project_id, initial_folder_id) =
            match (req.project_id.as_deref(), req.workspace.as_deref()) {
                (Some(project_id), _) => {
                    let (workspace, folder_id) = self.resolve_project_workspace(user_id, project_id).await?;
                    (Some(workspace), Some(project_id.to_owned()), Some(folder_id))
                }
                (None, Some(workspace)) if !workspace.is_empty() => {
                    (Some(validate_create_workspace_path(workspace)?), None, None)
                }
                _ => (None, None, None),
            };

        let team_id = generate_id();
        let now = now_ms();

        let provisioned = self
            .provisioner()
            .provision_initial_agents(user_id, &team_id, &req.agents, shared_workspace.as_deref())
            .await?;
        let agents = provisioned.agents;
        let lead_agent_id = provisioned.lead_agent_id;
        let team_workspace = provisioned.team_workspace;
        let agents_json = serde_json::to_string(&agents)?;

        // Project binding: prefer an explicit project_id request, then fall back
        // to resolving from the workspace path.
        let (project_id, folder_id) = match (initial_project_id, initial_folder_id) {
            (Some(pid), Some(fid)) => (Some(pid), Some(fid)),
            _ => self.resolve_binding_best_effort(user_id, &team_workspace).await,
        };

        let row = TeamRow {
            id: team_id.clone(),
            user_id: user_id.to_owned(),
            name: req.name.clone(),
            workspace: team_workspace.clone(),
            workspace_mode: "shared".into(),
            agents: agents_json,
            lead_agent_id: lead_agent_id.clone(),
            session_mode: None,
            agents_version: "1.0.1".into(),
            created_at: now,
            updated_at: now,
            project_id,
            folder_id,
        };
        self.repo.create_team(&row).await?;

        let team = Team {
            id: team_id,
            name: req.name,
            workspace: team_workspace,
            agents,
            lead_agent_id,
            project_id: row.project_id.clone(),
            created_at: now,
            updated_at: now,
        };

        info!(
            team_id = %team.id,
            workspace_source = if req.project_id.is_some() {
                "project_derived"
            } else if shared_workspace.is_some() {
                "user_supplied"
            } else {
                "auto_from_leader"
            },
            agent_count = team.agents.len(),
            "Team created"
        );

        self.broadcast_team_created(user_id, &team.id, &team.name);

        self.build_team_response(user_id, &team).await
    }

    pub async fn list_teams(&self, user_id: &str) -> Result<Vec<TeamResponse>, TeamError> {
        let rows = self.repo.list_teams_by_user(user_id).await?;
        let mut teams = Vec::with_capacity(rows.len());
        for row in &rows {
            match Team::from_row(row) {
                Ok(team) => match self.build_team_response(user_id, &team).await {
                    Ok(resp) => teams.push(resp),
                    Err(e) => {
                        tracing::warn!(team_id = %row.id, error = %e, "skipping team with build error");
                    }
                },
                Err(e) => {
                    tracing::warn!(team_id = %row.id, error = %e, "skipping team with invalid agents JSON");
                }
            }
        }
        Ok(teams)
    }

    pub async fn get_team(&self, user_id: &str, team_id: &str) -> Result<TeamResponse, TeamError> {
        let lock = self
            .add_agent_locks
            .entry(team_id.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;
        let row = self.load_owned_team_row(user_id, team_id).await?;
        // Project-bind side branch: lazily backfill binding only when a single
        // team is opened (never during list_teams / lease renew).
        self.backfill_team_binding_best_effort(&row).await;
        let team = Team::from_row(&row)?;
        // Deliberately does NOT reconcile legacy model facts. That repair reads
        // three extra tables PER MEMBER, and this is a plain read endpoint the
        // frontend hits whenever a team is opened. Session start owns the repair
        // (`ensure_session`), which is the point where a stale roster would
        // actually feed a rebuilt runtime.
        self.build_team_response(user_id, &team).await
    }

    pub async fn remove_team(&self, user_id: &str, team_id: &str) -> Result<(), TeamError> {
        let team = self.load_owned_team(user_id, team_id).await?;

        self.stop_team_runtime_and_agents(team_id, &team, AgentKillReason::TeamDeleted)
            .await;

        for agent in &team.agents {
            let _ = self
                .conversation_port
                .delete_team_conversation(user_id, &agent.conversation_id)
                .await;
        }

        // A project-bound team's runtime lives in its engagement-member
        // conversations, which are NOT in `team.agents` and were not deleted above.
        // Enumerate this team's (user-scoped) engagements, delete their member
        // conversations, then drop the member + engagement rows so a removed team
        // leaves no engagement leak. Legacy / no-project teams have no member rows
        // and their single default engagement (id == team_id, from the 045 backfill)
        // is correctly removed with the team. All deletes are user- AND team-scoped
        // and best-effort (a non-engagement-aware repo double returns `NotFound`,
        // which must not block deletion).
        let engagements = self.repo.list_engagements(user_id, team_id).await.unwrap_or_default();
        for engagement in &engagements {
            let members = self
                .repo
                .list_engagement_members(user_id, &engagement.id)
                .await
                .unwrap_or_default();
            for member in &members {
                let _ = self
                    .conversation_port
                    .delete_team_conversation(user_id, &member.conversation_id)
                    .await;
            }
        }
        // Member rows before engagement rows: FK
        // `team_engagement_members.engagement_id -> team_engagements.id`.
        let _ = self.repo.delete_engagement_members_by_team(user_id, team_id).await;
        let _ = self.repo.delete_engagements_by_team(user_id, team_id).await;

        self.repo.delete_mailbox_by_team(user_id, team_id).await?;
        self.repo.delete_tasks_by_team(user_id, team_id).await?;
        self.repo.delete_team(user_id, team_id).await?;

        // Cascade the team's sidebar ordering row (design §4.3, path 2). Members'
        // conversation rows are dropped by the conversation delete hook via the
        // `delete_team_conversation` calls above. Best-effort: an orphan `team`
        // row self-heals on read (the pinned group only emits teams present in
        // the live aggregate), so it never blocks deletion.
        self.remove_team_order_row(user_id, team_id).await;

        info!(team_id = %team_id, "Team removed");
        self.broadcast_team_removed(user_id, team_id);
        Ok(())
    }

    /// Tear down a team's live runtime and every member agent process WITHOUT
    /// deleting any data. Shared by `remove_team` (which then drops the rows)
    /// and `stop_team_processes` (archive, which keeps them). Best-effort: a
    /// stuck kill is bounded by a 3s timeout, mirroring the delete path.
    async fn stop_team_runtime_and_agents(&self, team_id: &str, team: &Team, reason: AgentKillReason) {
        // Build the kill list from BOTH identity spaces:
        //   1. the `teams.agents` TEMPLATE conversations (legacy / no-project
        //      teams run on these; they are also the live roster there), and
        //   2. every live session's scheduler roster, which for a PROJECT-BOUND
        //      team carries the engagement-member runtime conversations — the
        //      processes actually running, distinct from the template.
        // Template ids are added first and in template order (d115 relies on it);
        // roster ids are appended only if not already present. For a legacy team
        // the roster equals the template, so the appended set is empty and the
        // behavior is byte-identical to before.
        let mut kill_ids: Vec<String> = team.agents.iter().map(|agent| agent.conversation_id.clone()).collect();
        for entry in self.sessions.iter() {
            if entry.session.team_id() != team_id {
                continue;
            }
            for agent in entry.session.scheduler().list_agents().await {
                if !kill_ids.contains(&agent.conversation_id) {
                    kill_ids.push(agent.conversation_id);
                }
            }
        }

        // Stop every engagement's live session for this team and drop its
        // membership/ensure lock entries (maps are engagement-keyed now) AFTER the
        // roster has been captured above.
        self.stop_sessions_for_team(team_id);

        let kill_futures: Vec<_> = kill_ids
            .iter()
            .map(|conversation_id| self.task_manager.kill_and_wait(conversation_id, Some(reason)))
            .collect();

        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            futures_util::future::join_all(kill_futures),
        )
        .await;
    }

    /// Archive-time teardown: stop the team runtime and kill every member agent
    /// process, but keep all rows intact (the archive flip lives in the sidebar
    /// service). Mirrors the process-stopping half of `remove_team` so an
    /// archived team stops streaming just like a deleted one; unarchiving
    /// cold-starts a fresh runtime.
    pub async fn stop_team_processes(&self, user_id: &str, team_id: &str) -> Result<(), TeamError> {
        let team = self.load_owned_team(user_id, team_id).await?;
        self.stop_team_runtime_and_agents(team_id, &team, AgentKillReason::Archived)
            .await;
        Ok(())
    }

    pub async fn rename_team(&self, user_id: &str, team_id: &str, name: &str) -> Result<(), TeamError> {
        self.load_owned_team(user_id, team_id).await?;

        self.repo
            .update_team(
                user_id,
                team_id,
                &UpdateTeamParams {
                    name: Some(name.to_owned()),
                    ..Default::default()
                },
            )
            .await?;
        self.broadcast_team_renamed(user_id, team_id, name);
        Ok(())
    }

    /// Switch the team's project by moving its ACTIVE engagement: persist the new
    /// default/active project on the team row (read by `resolve_engagement_id`),
    /// tear down the previously-active engagement's runtime, and
    /// `ensure_engagement` the new project's engagement (which materializes its
    /// OWN member conversations in the new workspace). The shared template member
    /// conversations and any previously-active engagement's members are left
    /// untouched — a project change is an engagement switch, not a rebind.
    pub async fn update_team_project(
        &self,
        user_id: &str,
        team_id: &str,
        project_id: &str,
    ) -> Result<TeamResponse, TeamError> {
        let team = self.load_owned_team(user_id, team_id).await?;

        // Resolve the new project's workspace folder.
        let (new_workspace, new_folder_id) = self.resolve_project_workspace(user_id, project_id).await?;

        // Tear down the PREVIOUSLY-active engagement's runtime BEFORE the flip
        // (the session map is engagement-keyed; stop_sessions_for_team sweeps every
        // engagement session for this team) so the next session start resolves and
        // drives the new engagement.
        self.stop_team_runtime_and_agents(team_id, &team, AgentKillReason::RuntimeRestart)
            .await;

        // Persist the new default/active-project marker on the team row.
        self.repo
            .update_team(
                user_id,
                team_id,
                &UpdateTeamParams {
                    project_id: Some(project_id.to_owned()),
                    folder_id: Some(new_folder_id.clone()),
                    workspace: Some(new_workspace.clone()),
                    ..Default::default()
                },
            )
            .await?;

        // Switch the team's ACTIVE engagement to the new project by creating/using
        // that project's engagement with its OWN member conversations bound to the
        // new workspace. This replaces the deprecated single-team-project rebind
        // that mutated each shared template member conversation's project binding;
        // per-engagement members are independent, so a project change must never
        // retroactively touch a previously-active engagement's members.
        self.ensure_engagement(user_id, team_id, project_id).await?;

        self.broadcast_team_list_changed(user_id, team_id, "updated");

        // Reload and return the updated team.
        self.get_team(user_id, team_id).await
    }

    async fn resolve_runtime_agent(
        &self,
        user_id: &str,
        team: &Team,
        engagement: &str,
        slot_id: &str,
    ) -> Result<(TeamAgent, Option<aionui_db::models::TeamEngagementMemberRow>), TeamError> {
        // Mirrors `TeamSession::engagement_member_agents`: only a NON-EMPTY member
        // list switches to engagement runtime identity. An empty list (legacy /
        // no-project team) OR a member-read failure (test doubles that do not
        // implement it, genuine DB error) falls back to the template lookup, so a
        // management op never breaks where the template lookup used to succeed.
        match self
            .active_engagement_member(user_id, engagement, slot_id, |member, key| member.slot_id == key)
            .await?
        {
            ActiveMember::Fallback => {
                let agent = self.template_runtime_agent(team, slot_id).await?;
                Ok((agent, None))
            }
            ActiveMember::Match(member) => {
                let agent = self.template_runtime_agent(team, &member.template_slot).await?;
                let mut merged = agent;
                merged.slot_id = member.slot_id.clone();
                merged.conversation_id = member.conversation_id.clone();
                Ok((merged, Some(member)))
            }
        }
    }

    /// Slot-keyed counterpart of [`resolve_runtime_agent`] for callers that hold a
    /// runtime `conversation_id` (the config-option readers) instead of a `slot_id`.
    /// Identical engagement-member semantics and the same no-template-fall-through
    /// safety guard: a materialized engagement whose members do not carry this
    /// conversation is never resolved against the template.
    async fn resolve_runtime_agent_by_conversation(
        &self,
        user_id: &str,
        team: &Team,
        engagement: &str,
        conversation_id: &str,
    ) -> Result<(TeamAgent, Option<aionui_db::models::TeamEngagementMemberRow>), TeamError> {
        match self
            .active_engagement_member(user_id, engagement, conversation_id, |member, key| {
                member.conversation_id == key
            })
            .await?
        {
            ActiveMember::Fallback => {
                let agent = self
                    .template_runtime_agent_by_conversation(team, conversation_id)
                    .await?;
                Ok((agent, None))
            }
            ActiveMember::Match(member) => {
                let agent = self.template_runtime_agent(team, &member.template_slot).await?;
                let mut merged = agent;
                merged.slot_id = member.slot_id.clone();
                merged.conversation_id = member.conversation_id.clone();
                Ok((merged, Some(member)))
            }
        }
    }

    /// Shared member-list decision for both `resolve_runtime_agent` entry points.
    /// `Ok(Fallback)` means "resolve via the template" (empty roster or a read
    /// error); `Ok(Match)` yields the engagement member; a materialized roster that
    /// does not contain `key` is `AgentNotFound` and must NOT fall through to the
    /// template — that could destroy / act on the wrong conversation.
    async fn active_engagement_member(
        &self,
        user_id: &str,
        engagement: &str,
        key: &str,
        matches: impl Fn(&aionui_db::models::TeamEngagementMemberRow, &str) -> bool,
    ) -> Result<ActiveMember, TeamError> {
        let members = match self.repo.list_engagement_members(user_id, engagement).await {
            Ok(m) if !m.is_empty() => m,
            Ok(_) => return Ok(ActiveMember::Fallback),
            Err(err) => {
                warn!(
                    engagement,
                    error = %err,
                    "engagement member read failed during management op; falling back to template roster"
                );
                return Ok(ActiveMember::Fallback);
            }
        };
        match members.into_iter().find(|m| matches(m, key)) {
            Some(member) => Ok(ActiveMember::Match(member)),
            None => Err(TeamError::AgentNotFound(key.to_owned())),
        }
    }

    async fn template_runtime_agent(&self, team: &Team, slot_id: &str) -> Result<TeamAgent, TeamError> {
        team.agents
            .iter()
            .find(|a| a.slot_id == slot_id)
            .cloned()
            .ok_or_else(|| TeamError::AgentNotFound(slot_id.to_owned()))
    }

    async fn template_runtime_agent_by_conversation(
        &self,
        team: &Team,
        conversation_id: &str,
    ) -> Result<TeamAgent, TeamError> {
        team.agents
            .iter()
            .find(|a| a.conversation_id == conversation_id)
            .cloned()
            .ok_or_else(|| TeamError::AgentNotFound(conversation_id.to_owned()))
    }

    pub async fn add_agent(
        &self,
        user_id: &str,
        team_id: &str,
        req: AddAgentRequest,
    ) -> Result<TeamAgentResponse, TeamError> {
        let engagement = self.engagement_key(user_id, team_id).await?;
        let lock = self
            .add_agent_locks
            .entry(team_id.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;

        let row = self.load_owned_team_row(user_id, team_id).await?;
        let mut team = Team::from_row(&row)?;
        let agent = self.provisioner().add_agent(user_id, &row, &mut team, req).await?;

        // The new agent is added to the `teams.agents` template above. A project
        // bound team additionally materializes it into the ACTIVE engagement's
        // member roster (so the engagement drives its own runtime slot /
        // conversation), without touching concurrently-running engagements
        // (Task 5: a template edit must not retro-alter another engagement).
        // Legacy / no-project teams stay template-only, byte-identical to before.
        if let Some(project_id) = team.project_id.clone() {
            let engagement_row = self
                .repo
                .find_or_create_engagement(user_id, team_id, &project_id, &team.workspace)
                .await?;
            self.provisioner()
                .ensure_engagement_members(user_id, &engagement_row, &team)
                .await?;
        }
        // The runtime identity the live session / scheduler must key on: the
        // active engagement's member slot when materialized, otherwise the
        // template slot (legacy). A member-read failure is treated as "no
        // members" (template fallback), matching `resolve_runtime_agent`.
        // Returning the runtime slot keeps the response handle consistent with
        // the slot id the UI uses for later ops (3b).
        let members = self
            .repo
            .list_engagement_members(user_id, &engagement)
            .await
            .unwrap_or_default();
        let runtime_agent = members
            .iter()
            .find(|m| m.template_slot == agent.slot_id)
            .map(|m| {
                let mut merged = agent.clone();
                merged.slot_id = m.slot_id.clone();
                merged.conversation_id = m.conversation_id.clone();
                merged
            })
            .unwrap_or_else(|| agent.clone());

        if let Some(session) = self.sessions.get(&engagement).map(|e| Arc::clone(&e.session)) {
            let reservation = session.reserve_dynamic_member_attach(&runtime_agent);
            session.add_manual_agent(&runtime_agent).await?;
            let service = self
                .self_ref
                .upgrade()
                .ok_or_else(|| TeamError::InvalidRequest("add_agent requires a live TeamSessionService".into()))?;
            self.broadcast_agent_runtime_status(
                user_id,
                team_id,
                &runtime_agent,
                TeamAgentRuntimeStatus::Pending,
                None,
            );
            spawn_attach_agent_process_bg(
                service,
                session,
                user_id.to_owned(),
                runtime_agent.clone(),
                self.task_manager.clone(),
                reservation,
                // User-initiated add: failures surface inline, do not wake leader.
                false,
            );
            info!(
                team_id = %team_id,
                slot_id = %runtime_agent.slot_id,
                assistant_id = %runtime_agent.assistant_id.as_deref().unwrap_or(""),
                role = %runtime_agent.role,
                notification_written = true,
                wake_requested = true,
                "manual teammate added"
            );
        } else {
            TeamEventEmitter::new(team_id.to_owned(), user_id.to_owned(), self.broadcaster.clone())
                .broadcast_agent_spawned(&runtime_agent);
            info!(
                team_id = %team_id,
                slot_id = %runtime_agent.slot_id,
                assistant_id = %runtime_agent.assistant_id.as_deref().unwrap_or(""),
                role = %runtime_agent.role,
                notification_written = false,
                wake_requested = false,
                "manual teammate added"
            );
        }

        self.build_agent_response(user_id, team_id, &runtime_agent).await
    }

    pub async fn remove_agent(&self, user_id: &str, team_id: &str, slot_id: &str) -> Result<(), TeamError> {
        let engagement = self.engagement_key(user_id, team_id).await?;
        let lock = self
            .add_agent_locks
            .entry(team_id.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let (removed, template_slot_key, session, removal_lease) = {
            let _guard = lock.lock().await;
            let team = self.load_owned_team(user_id, team_id).await?;
            let (removed, member) = self.resolve_runtime_agent(user_id, &team, &engagement, slot_id).await?;
            if removed.role == crate::types::TeammateRole::Lead {
                return Err(TeamError::InvalidRequest("cannot remove the team lead".into()));
            }
            // Template rows are keyed by `template_slot`; legacy / no-member teams
            // resolve that to the incoming slot id itself.
            let template_slot_key = member
                .as_ref()
                .map(|m| m.template_slot.clone())
                .unwrap_or_else(|| slot_id.to_owned());
            let session = self.sessions.get(&engagement).map(|entry| Arc::clone(&entry.session));
            let removal = session
                .as_ref()
                .map(|session| session.member_runtimes().begin_remove(slot_id));
            let removal_lease = match removal {
                Some(BeginRemove::Start(lease)) => Some(lease),
                Some(BeginRemove::Join(waiter)) => {
                    drop(_guard);
                    return match waiter.wait().await {
                        AttachOutcome::Removed => Ok(()),
                        AttachOutcome::Failed(failure) => Err(TeamError::MemberRuntimeFailed {
                            team_id: team_id.to_owned(),
                            slot_id: removed.slot_id,
                            conversation_id: removed.conversation_id,
                            public_reason: failure.public_reason,
                        }),
                        AttachOutcome::Ready | AttachOutcome::SessionStopped => {
                            Err(TeamError::SessionNotFound(team_id.to_owned()))
                        }
                    };
                }
                Some(BeginRemove::Absent | BeginRemove::SessionStopped) | None => None,
            };
            (removed, template_slot_key, session, removal_lease)
        };

        // Cancellation and process cleanup intentionally happen without the
        // membership lock. Concurrent ensure calls observe Removing and join
        // the same registry operation instead of starting a replacement.
        if let Some(session) = &session {
            session.event_loops().remove(slot_id);
        }
        self.task_manager
            .kill_and_wait(&removed.conversation_id, Some(AgentKillReason::TeamDeleted))
            .await;

        let persist_result = {
            let _guard = lock.lock().await;
            let mut current = self.load_owned_team(user_id, team_id).await?;
            current.agents.retain(|agent| agent.slot_id != template_slot_key);
            let agents_json = serde_json::to_string(&current.agents)?;
            self.repo
                .update_team(
                    user_id,
                    team_id,
                    &UpdateTeamParams {
                        agents: Some(agents_json),
                        ..Default::default()
                    },
                )
                .await
        };

        if let Err(error) = persist_result {
            if let (Some(session), Some(lease)) = (&session, removal_lease.as_ref()) {
                session
                    .member_runtimes()
                    .restore_attach_required_after_remove_persist_error(
                        lease,
                        MemberRuntimeFailure {
                            classification: "membership_persist_failed",
                            public_reason: "Agent runtime needs to restart after membership update failed".to_owned(),
                        },
                    );
                self.refresh_member_runtime_status(session).await;
            }
            return Err(error.into());
        }

        let published_session = self.sessions.get(&engagement).map(|entry| Arc::clone(&entry.session));
        let active_session = if let Some(current) = published_session {
            let current_removal_lease = if session.as_ref().is_some_and(|captured| Arc::ptr_eq(captured, &current)) {
                removal_lease
            } else {
                match current.member_runtimes().begin_remove(slot_id) {
                    BeginRemove::Start(lease) => Some(lease),
                    BeginRemove::Join(waiter) => {
                        let _ = waiter.wait().await;
                        None
                    }
                    BeginRemove::Absent | BeginRemove::SessionStopped => None,
                }
            };
            current.event_loops().remove(slot_id);
            self.task_manager
                .kill_and_wait(&removed.conversation_id, Some(AgentKillReason::TeamDeleted))
                .await;
            match current.scheduler().remove_agent(slot_id).await {
                Ok(_) | Err(TeamError::AgentNotFound(_)) => {}
                Err(error) => return Err(error),
            }
            if let Some(lease) = current_removal_lease.as_ref() {
                current.member_runtimes().finish_remove(lease);
            }
            Some(current)
        } else {
            None
        };

        if let Err(error) = self
            .conversation_port
            .delete_team_conversation(user_id, &removed.conversation_id)
            .await
        {
            warn!(
                team_id,
                slot_id,
                conversation_id = %removed.conversation_id,
                error = %error,
                "removed team member conversation cleanup failed"
            );
        }

        if let Some(session) = active_session.filter(|session| self.capture_published_session(session).is_some()) {
            session.notify_leader_membership_removed(&removed).await?;
            self.refresh_member_runtime_status(&session).await;
            info!(
                team_id = %team_id,
                slot_id = %removed.slot_id,
                assistant_id = %removed.assistant_id.as_deref().unwrap_or(""),
                role = %removed.role,
                notification_written = true,
                wake_requested = true,
                "manual teammate removed"
            );
        } else {
            TeamEventEmitter::new(team_id.to_owned(), user_id.to_owned(), self.broadcaster.clone())
                .broadcast_agent_removed(slot_id);
            info!(
                team_id = %team_id,
                slot_id = %removed.slot_id,
                assistant_id = %removed.assistant_id.as_deref().unwrap_or(""),
                role = %removed.role,
                notification_written = false,
                wake_requested = false,
                "manual teammate removed"
            );
        }

        Ok(())
    }

    pub async fn rename_agent(&self, user_id: &str, team_id: &str, slot_id: &str, name: &str) -> Result<(), TeamError> {
        let engagement = self.engagement_key(user_id, team_id).await?;
        let lock = self
            .add_agent_locks
            .entry(team_id.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;

        let mut team = self.load_owned_team(user_id, team_id).await?;

        // Resolve the runtime slot id the UI sends to its template row (the name
        // lives in `teams.agents`); legacy / no-member teams resolve to themselves.
        let (_, member) = self.resolve_runtime_agent(user_id, &team, &engagement, slot_id).await?;
        let template_slot = member.as_ref().map(|m| m.template_slot.as_str()).unwrap_or(slot_id);

        let normalized = crate::scheduler::normalize_name(name);
        if normalized.is_empty() {
            return Err(TeamError::InvalidRequest(
                "rename_agent.name is empty after normalization".into(),
            ));
        }

        // Uniqueness check against all other agents in the team.
        let has_conflict = team
            .agents
            .iter()
            .any(|a| a.slot_id != template_slot && crate::scheduler::normalize_name(&a.name) == normalized);
        if has_conflict {
            return Err(TeamError::DuplicateAgentName(name.to_owned()));
        }

        let agent = team
            .agents
            .iter_mut()
            .find(|a| a.slot_id == template_slot)
            .ok_or_else(|| TeamError::AgentNotFound(slot_id.into()))?;
        agent.name = name.to_owned();

        let agents_json = serde_json::to_string(&team.agents)?;
        self.repo
            .update_team(
                user_id,
                team_id,
                &UpdateTeamParams {
                    agents: Some(agents_json),
                    ..Default::default()
                },
            )
            .await?;

        if let Some(session) = self.sessions.get(&engagement).map(|e| Arc::clone(&e.session)) {
            // The live scheduler is keyed by the runtime slot the caller sent.
            let _ = session.rename_agent(slot_id, name).await;
        }

        Ok(())
    }

    pub async fn update_agent_model(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
        model: &str,
    ) -> Result<(), TeamError> {
        let model = model.trim();
        if model.is_empty() {
            return Err(TeamError::InvalidRequest("model must not be empty".into()));
        }
        self.persist_member_model_selection(user_id, team_id, slot_id, model, ModelPersistTrigger::ExplicitRequest)
            .await
    }

    /// Record that a team member's model is now `model`, in every place a rebuilt
    /// member runtime reads it from.
    ///
    /// Sole implementation on purpose. A model switch has to land in three places
    /// — the conversation's persisted runtime state, the team roster, and the live
    /// session's in-memory agent — and both entry points (the explicit model
    /// endpoint and the generic config-option path) must update all three.
    /// Previously each did half and the frontend chained them, so a failure
    /// between the two calls left the runtime switched and the roster stale.
    async fn persist_member_model_selection(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
        model: &str,
        trigger: ModelPersistTrigger,
    ) -> Result<(), TeamError> {
        let engagement = self.engagement_key(user_id, team_id).await?;
        let lock = self
            .add_agent_locks
            .entry(team_id.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;
        let mut team = self.load_owned_team(user_id, team_id).await?;
        // Resolve the runtime identity the caller's slot refers to: the engagement's
        // own conversation (the confirmed-model persist target) and the template row
        // (the roster source of truth). Legacy / no-member teams resolve to the
        // template row, byte-identical to before.
        let (resolved, member) = self.resolve_runtime_agent(user_id, &team, &engagement, slot_id).await?;
        // Only an explicit preference update is refused mid-start. When the
        // runtime has ALREADY accepted the switch, refusing here would drop the
        // persistence and silently revert the member on its next rebuild.
        if trigger == ModelPersistTrigger::ExplicitRequest
            && self.member_runtime_is_starting(team_id, &resolved.slot_id)
        {
            return Err(Self::member_runtime_starting_error(team_id, &resolved));
        }
        let template_slot = member.as_ref().map(|m| m.template_slot.as_str()).unwrap_or(slot_id);
        let agent = team
            .agents
            .iter_mut()
            .find(|agent| agent.slot_id == template_slot)
            .ok_or_else(|| TeamError::AgentNotFound(slot_id.to_owned()))?;
        // Persist against the ACTIVE engagement's member conversation, never the
        // template's original conversation or another engagement's.
        let conversation_id = resolved.conversation_id.clone();

        self.conversation_port
            .persist_confirmed_model(&conversation_id, model)
            .await?;
        agent.model = model.to_owned();
        self.repo
            .update_team(
                user_id,
                team_id,
                &UpdateTeamParams {
                    agents: Some(serde_json::to_string(&team.agents)?),
                    ..Default::default()
                },
            )
            .await?;

        if let Some(session) = self.sessions.get(&engagement).map(|entry| Arc::clone(&entry.session)) {
            session.update_agent_model(slot_id, model).await?;
        }
        info!(
            team_id,
            slot_id,
            conversation_id,
            model,
            trigger = trigger.as_str(),
            "team agent model preference persisted"
        );
        Ok(())
    }

    async fn reconcile_legacy_team_models(
        &self,
        user_id: &str,
        team_id: &str,
        engagement_key: &str,
        team: &mut Team,
    ) -> Result<(), TeamError> {
        let mut roster_changed = false;
        let mut repaired = Vec::new();

        for agent in &mut team.agents {
            let facts = self
                .conversation_port
                .conversation_model_facts(&agent.conversation_id)
                .await?;
            let Some(model) = facts
                .confirmed_model_id
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
            else {
                continue;
            };
            let seed_changed = facts.runtime_seed_model_id.as_deref() != Some(model.as_str());
            if seed_changed {
                self.conversation_port
                    .patch_runtime_config(&agent.conversation_id, serde_json::json!({ "current_model_id": model }))
                    .await?;
            }
            let agent_changed = agent.model != model;
            if agent_changed {
                agent.model.clone_from(&model);
                roster_changed = true;
            }
            if seed_changed || agent_changed {
                repaired.push((agent.slot_id.clone(), model));
            }
        }

        if roster_changed {
            self.repo
                .update_team(
                    user_id,
                    team_id,
                    &UpdateTeamParams {
                        agents: Some(serde_json::to_string(&team.agents)?),
                        ..Default::default()
                    },
                )
                .await?;
        }
        if let Some(session) = self
            .sessions
            .get(engagement_key)
            .map(|entry| Arc::clone(&entry.session))
        {
            for (slot_id, model) in &repaired {
                session.update_agent_model(slot_id, model).await?;
            }
        }
        if !repaired.is_empty() {
            info!(
                team_id,
                repaired_agent_count = repaired.len(),
                "reconciled legacy team model facts"
            );
        }
        Ok(())
    }

    /// Start the team's MCP server and rebuild every agent process so it
    /// carries a fresh `team_mcp_stdio_config` pointing at the new server.
    ///
    /// Flow (mcp.md §4.3):
    /// 1. Start `TeamSession` (opens the MCP TCP server).
    /// 2. For each agent: persist `team_mcp_stdio_config` into
    ///    `conversation.extra` → `task_manager.kill_and_wait(conv_id, TeamMcpRebuild)`
    ///    → `TeamConversationProvisioningPort::warmup_agent_process(...)`
    ///    rebuilds the ACP process with
    ///    the new extra.
    /// 3. Spawn per-agent event loops that drain the mailbox whenever notified.
    /// 4. Only insert into `sessions` after every step above succeeds — on
    ///    any failure, stop the session and leave the map untouched so a
    ///    retry can start cleanly.
    pub async fn ensure_session(&self, user_id: &str, team_id: &str) -> Result<(), TeamError> {
        self.load_owned_team_row(user_id, team_id).await?;
        self.ensure_session_inner(team_id, Some(user_id)).await
    }

    async fn ensure_session_inner(&self, team_id: &str, requested_user_id: Option<&str>) -> Result<(), TeamError> {
        // Runtime maps are keyed by the team's ACTIVE engagement (not team_id) so
        // two projects' sessions of one team do not collide. This cheap pre-read
        // only derives that key so the membership lock can be engagement-keyed;
        // the authoritative roster is still read under the lock below.
        let engagement = match self.repo.get_team_for_restore(team_id).await.ok().flatten() {
            Some(row) => match Team::from_row(&row) {
                Ok(team) => TeamSession::resolve_engagement_id(&self.repo, &row.user_id, &team).await,
                Err(_) => team_id.to_owned(),
            },
            None => team_id.to_owned(),
        };

        let membership_lock = self
            .add_agent_locks
            .entry(team_id.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let membership_guard = membership_lock.lock().await;

        let row = match self.repo.get_team_for_restore(team_id).await {
            Ok(Some(row)) => row,
            Ok(None) => {
                if let Some(user_id) = requested_user_id {
                    self.broadcast_session_status(
                        user_id,
                        team_id,
                        TeamSessionStatus::Failed,
                        Some(TeamSessionPhase::LoadingTeam),
                        |p| {
                            p.error = Some(format!("team not found: {team_id}"));
                        },
                    );
                }
                return Err(TeamError::TeamNotFound(team_id.into()));
            }
            Err(e) => {
                if let Some(user_id) = requested_user_id {
                    self.broadcast_session_status(
                        user_id,
                        team_id,
                        TeamSessionStatus::Failed,
                        Some(TeamSessionPhase::LoadingTeam),
                        |p| {
                            p.error = Some(e.to_string());
                        },
                    );
                }
                return Err(e.into());
            }
        };
        let user_id = row.user_id.clone();
        let mut team = Team::from_row(&row)?;
        self.reconcile_legacy_team_models(&user_id, team_id, &engagement, &mut team)
            .await?;

        // The attach/reconcile/broadcast roster MUST be the same merged view the
        // scheduler drives (engagement-materialized runtime slot/conversation ids,
        // falling back to the template for legacy/no-member teams). Reading it from
        // the live scheduler — not `team.agents` — keeps the registry, scheduler,
        // and status broadcasts keyed on engagement runtime ids consistently, so a
        // project-bound team never attaches/binds/duplicates the shared template.
        if let Some(session) = self.sessions.get(&engagement).map(|entry| Arc::clone(&entry.session)) {
            let agents_snapshot = session.scheduler().list_agents().await;
            let work = self
                .reserve_member_runtime_reconciliation(&session, &agents_snapshot)
                .await?;
            drop(membership_guard);
            return self
                .complete_member_runtime_reconciliation(team_id, &engagement, &user_id, session, work)
                .await;
        }

        let lock = self
            .ensure_session_locks
            .entry(engagement.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let ensure_guard = lock.lock().await;

        if let Some(session) = self.sessions.get(&engagement).map(|entry| Arc::clone(&entry.session)) {
            let agents_snapshot = session.scheduler().list_agents().await;
            let work = self
                .reserve_member_runtime_reconciliation(&session, &agents_snapshot)
                .await?;
            drop(membership_guard);
            drop(ensure_guard);
            return self
                .complete_member_runtime_reconciliation(team_id, &engagement, &user_id, session, work)
                .await;
        }

        self.broadcast_session_status(
            &user_id,
            team_id,
            TeamSessionStatus::Starting,
            Some(TeamSessionPhase::LoadingTeam),
            |_| {},
        );

        self.broadcast_session_status(
            &user_id,
            team_id,
            TeamSessionStatus::Starting,
            Some(TeamSessionPhase::StartingBridge),
            |_| {},
        );

        // Guarantee a project-bound team's engagement members exist before the
        // session's scheduler is built from them (`engagement_member_agents`).
        // Materialization is normally done at bind time via `ensure_engagement`,
        // but a team created project-bound (or restored before its first bind
        // call) reaches session start without member rows; without this the
        // session would silently fall back to the shared template roster and
        // mis-route member messages. Best-effort: repo doubles whose
        // `find_or_create_engagement` / `ensure_engagement_members` are
        // unimplemented fall through to the same template fallback, so legacy
        // behavior is preserved. Ownership is already enforced by the owned
        // team row read above.
        if let Some(project_id) = team.project_id.as_deref() {
            match self
                .repo
                .find_or_create_engagement(&user_id, team_id, project_id, &team.workspace)
                .await
            {
                Ok(row) => {
                    if let Err(err) = self
                        .provisioner()
                        .ensure_engagement_members(&user_id, &row, &team)
                        .await
                    {
                        tracing::warn!(
                            team_id,
                            engagement_id = %row.id,
                            error = %err,
                            "failed to ensure engagement members at session start; runtime falls back to template roster"
                        );
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        team_id,
                        error = %err,
                        "failed to resolve engagement at session start; runtime falls back to template roster"
                    );
                }
            }
        }

        let session = match TeamSession::start_with_prompt_dump(
            team,
            self.repo.clone(),
            self.broadcaster.clone(),
            self.backend_binary_path.clone(),
            self.task_manager.clone(),
            self.turn_port.clone(),
            self.cancellation_port.clone(),
            self.projection_store.clone(),
            user_id.clone(),
            self.self_ref.clone(),
            self.prompt_dump.clone(),
        )
        .await
        {
            Ok(session) => Arc::new(session.with_slash_command_port(self.slash_command_port.clone())),
            Err(e) => {
                self.broadcast_session_status(
                    &user_id,
                    team_id,
                    TeamSessionStatus::Failed,
                    Some(TeamSessionPhase::StartingBridge),
                    |p| {
                        p.error = Some(e.to_string());
                    },
                );
                return Err(e);
            }
        };

        self.broadcast_session_status(
            &user_id,
            team_id,
            TeamSessionStatus::Starting,
            Some(TeamSessionPhase::AttachingAgents),
            |_| {},
        );

        let service = self
            .self_ref
            .upgrade()
            .ok_or_else(|| TeamError::InvalidRequest("team service is shutting down".to_owned()))?;

        // Leader-only warmup: only the lead slot is attached at first start.
        // Teammates stay dormant (Absent in the registry) until a delivery
        // lazily wakes them (spec 5.1). Read the roster from the session's
        // scheduler (the merged engagement view), never the `team.agents`
        // template, so the leader is attached and bound under the engagement's
        // runtime `slot_id`/`conversation_id`.
        let member_roster = session.scheduler().list_agents().await;
        let Some(leader) = member_roster
            .iter()
            .find(|agent| agent.role == TeammateRole::Lead)
            .cloned()
        else {
            let error = TeamError::InvalidRequest("team has no lead agent".to_owned());
            self.broadcast_session_status(
                &user_id,
                team_id,
                TeamSessionStatus::Failed,
                Some(TeamSessionPhase::AttachingAgents),
                |p| p.error = Some(error.to_string()),
            );
            session.stop();
            return Err(error);
        };

        // Publish the session BEFORE attaching so the single attach path
        // (`attach_member_runtime`) observes it as the current published
        // session. Drop the startup guards before awaiting the attach: that
        // path re-acquires the membership lock (via `refresh_member_runtime_status`)
        // and the ensure lock (via `cleanup_stale_member_runtime_task` on
        // failure), so holding them here would deadlock. Concurrent ensures
        // that were blocked on membership_guard now observe the published
        // session and take the reconciliation path instead of cold-starting.
        let slow_monitor_handle = Self::spawn_slow_monitor(session.clone());
        let entry = SessionEntry {
            session: session.clone(),
            slow_monitor_handle,
        };
        self.sessions.insert(session.engagement_id().to_owned(), entry);
        drop(membership_guard);
        drop(ensure_guard);

        self.broadcast_agent_runtime_status(&user_id, team_id, &leader, TeamAgentRuntimeStatus::Pending, None);
        let leader_outcome = match session.member_runtimes().reserve_attach(&leader.slot_id, false) {
            ReserveAttach::Start(lease) => {
                attach_member_runtime(
                    Arc::clone(&service),
                    session.clone(),
                    user_id.clone(),
                    leader.clone(),
                    self.task_manager.clone(),
                    lease,
                    // Leader cold-start failure bubbles to a session-level Failed
                    // (full-screen card), not an inline per-member notice.
                    false,
                )
                .await
            }
            ReserveAttach::Join(waiter) | ReserveAttach::Removing(waiter) => waiter.wait().await,
            ReserveAttach::AlreadyReady => AttachOutcome::Ready,
            ReserveAttach::SessionStopped => AttachOutcome::SessionStopped,
        };

        match leader_outcome {
            AttachOutcome::Ready | AttachOutcome::Removed => {}
            AttachOutcome::Failed(failure) => {
                self.broadcast_session_status(
                    &user_id,
                    team_id,
                    TeamSessionStatus::Failed,
                    Some(TeamSessionPhase::AttachingAgents),
                    |p| p.error = Some(failure.public_reason.clone()),
                );
                session.stop();
                self.sessions.remove(session.engagement_id());
                return Err(TeamError::MemberRuntimeFailed {
                    team_id: team_id.to_owned(),
                    slot_id: leader.slot_id.clone(),
                    conversation_id: leader.conversation_id.clone(),
                    public_reason: failure.public_reason,
                });
            }
            AttachOutcome::SessionStopped => {
                session.stop();
                self.sessions.remove(session.engagement_id());
                return Err(TeamError::InvalidRequest(
                    "team session stopped during leader warmup".to_owned(),
                ));
            }
        }

        // Teammates start dormant; the leader's Ready was already broadcast by
        // its successful attach.
        for agent in member_roster.iter().filter(|a| a.role != TeammateRole::Lead) {
            self.broadcast_agent_runtime_status(&user_id, team_id, agent, TeamAgentRuntimeStatus::Dormant, None);
        }

        self.broadcast_session_status(
            &user_id,
            team_id,
            TeamSessionStatus::Starting,
            Some(TeamSessionPhase::Recovering),
            |_| {},
        );

        if let Err(err) = session.try_start_recovery_drain("ensure_session_ready").await {
            warn!(
                team_id,
                error = %err,
                "team recovery scan failed after session ensure"
            );
        }

        self.broadcast_session_status(&user_id, team_id, TeamSessionStatus::Ready, None, |p| {
            p.server_count = Some(member_roster.len());
        });

        Ok(())
    }

    async fn reserve_member_runtime_reconciliation(
        &self,
        session: &Arc<TeamSession>,
        agents: &[TeamAgent],
    ) -> Result<Vec<MemberRuntimeReconcileWork>, TeamError> {
        let scheduler_slots = session
            .scheduler()
            .list_agents()
            .await
            .into_iter()
            .map(|agent| agent.slot_id)
            .collect::<HashSet<_>>();
        let mut work = Vec::new();

        for agent in agents {
            if !scheduler_slots.contains(&agent.slot_id) {
                session.scheduler().add_agent(agent).await;
            }
            let snapshot = session.member_runtimes().snapshot(&agent.slot_id);
            // Skip dormant teammates: an Absent non-lead member was never
            // triggered, so a re-ensure (second warmupSession, model switch,
            // retry) must NOT wake it, or it would punch through lazy warmup
            // (spec 5.1). The leader, Ready-repair, Failed-retry, and in-flight
            // members still reconcile.
            if matches!(snapshot, MemberRuntimeSnapshot::Absent) && agent.role != TeammateRole::Lead {
                continue;
            }
            let reservation = match snapshot {
                MemberRuntimeSnapshot::Ready if self.task_manager.get_task(&agent.conversation_id).is_none() => {
                    session.member_runtimes().reserve_repair(&agent.slot_id)
                }
                MemberRuntimeSnapshot::Ready => ReserveAttach::AlreadyReady,
                _ => session.member_runtimes().reserve_attach(&agent.slot_id, true),
            };

            match reservation {
                ReserveAttach::Start(owner) => {
                    self.broadcast_agent_runtime_status(
                        session.user_id(),
                        session.team_id(),
                        agent,
                        TeamAgentRuntimeStatus::Pending,
                        None,
                    );
                    work.push(MemberRuntimeReconcileWork {
                        agent: agent.clone(),
                        waiter: owner.waiter(),
                        owner: Some(owner),
                    });
                }
                ReserveAttach::Join(waiter) | ReserveAttach::Removing(waiter) => {
                    info!(
                        team_id = session.team_id(),
                        slot_id = agent.slot_id,
                        conversation_id = agent.conversation_id,
                        operation_id = waiter.operation_id(),
                        generation = session.generation(),
                        duration_ms = 0,
                        error_classification = "none",
                        "team member runtime reconciliation waiting"
                    );
                    work.push(MemberRuntimeReconcileWork {
                        agent: agent.clone(),
                        waiter,
                        owner: None,
                    });
                }
                ReserveAttach::AlreadyReady => {}
                ReserveAttach::SessionStopped => {
                    return Err(TeamError::InvalidRequest(
                        "team session stopped during reconciliation reservation".to_owned(),
                    ));
                }
            }
        }
        Ok(work)
    }

    async fn complete_member_runtime_reconciliation(
        &self,
        team_id: &str,
        engagement_key: &str,
        user_id: &str,
        session: Arc<TeamSession>,
        work: Vec<MemberRuntimeReconcileWork>,
    ) -> Result<(), TeamError> {
        // Session-level `Starting` is leader-scoped (spec 5.4/5.5): only a
        // leader (re)attach may raise the overlay. Reconciliation that touches
        // only teammates (Ready-repair, Failed-retry of a non-lead member) keeps
        // its progress inline via `agentRuntimeStatusChanged`.
        if work.iter().any(|item| item.agent.role == TeammateRole::Lead) {
            self.publish_member_runtime_starting_if_current(&session);
        }
        let mut waiters = Vec::with_capacity(work.len());
        for item in work {
            let waiter = item.waiter;
            if let Some(owner) = item.owner {
                tokio::spawn(attach_member_runtime(
                    self.self_ref
                        .upgrade()
                        .ok_or_else(|| TeamError::InvalidRequest("team service is shutting down".to_owned()))?,
                    Arc::clone(&session),
                    user_id.to_owned(),
                    item.agent.clone(),
                    self.task_manager.clone(),
                    owner,
                    // Reconciliation repairs runtimes without a fresh delivery to
                    // re-delegate; failures surface via runtime status only.
                    false,
                ));
            }
            waiters.push((item.agent, waiter));
        }

        let outcomes = futures_util::future::join_all(
            waiters
                .into_iter()
                .map(|(agent, waiter)| async move { (agent, waiter.wait().await) }),
        )
        .await;

        let membership_lock = self
            .add_agent_locks
            .entry(team_id.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _membership_guard = membership_lock.lock().await;
        // ponytail: removal-during-reconcile race is guarded by this team-keyed
        // `add_agent_lock`; a dedicated multi-engagement regression test lands with
        // the Task 5 cross-engagement suite (deliberately deferred, not forgotten).
        // Race closure (Phase 2b 3c): this lock is the team-keyed `add_agent_locks`
        // entry `add`/`remove`/`rename` take. `reserve` already ran under it, and
        // `remove_agent` marks the member Removing under it, so `reserve` joins the
        // removal waiter instead of re-attaching. The removal's roster-mutating
        // steps (begin_remove / persist / finish_remove) are lock-guarded; the only
        // mutations outside the lock (`scheduler.remove_agent`, `event_loops.remove`)
        // can only DELETE roster entries, never re-add — so this post-await
        // membership check always observes a mid-reconcile removal and skips it; a
        // removed agent can never be resurrected by reconciliation.
        // Team-existence guard: if the team row was deleted while attaching,
        // abort. The per-member membership check below keys on the LIVE runtime
        // roster (the scheduler's merged engagement view), NOT the `teams.agents`
        // template — reconcile work agents carry engagement runtime slot ids that
        // would never match the template slots.
        if self.repo.get_team(user_id, team_id).await?.is_none() {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        let live_roster = session.scheduler().list_agents().await;
        let current_slots = live_roster
            .iter()
            .map(|agent| agent.slot_id.as_str())
            .collect::<HashSet<_>>();
        let current_session = self
            .sessions
            .get(engagement_key)
            .ok_or_else(|| TeamError::SessionNotFound(team_id.to_owned()))?;
        if !Arc::ptr_eq(&current_session.session, &session) {
            return Err(TeamError::SessionNotFound(team_id.to_owned()));
        }

        for (agent, outcome) in outcomes {
            if !current_slots.contains(agent.slot_id.as_str()) {
                continue;
            }
            match outcome {
                AttachOutcome::Ready | AttachOutcome::Removed => {}
                AttachOutcome::Failed(failure) => {
                    // Session-level Failed / the whole-team error are leader-scoped
                    // (spec 5.4/5.5): only a leader failure raises the full-screen
                    // failure card and blocks the team. A teammate reconciliation
                    // failure stays inline — its `agentRuntimeStatusChanged=failed`
                    // already fired in `attach_member_runtime` — and the team stays
                    // usable because the leader is ready. `ensure_session` (invoked
                    // on mount, model switches, and before sends via warmupSession)
                    // must not fail just because an unrelated teammate is broken.
                    if agent.role == TeammateRole::Lead {
                        self.broadcast_session_status(
                            user_id,
                            team_id,
                            TeamSessionStatus::Failed,
                            Some(TeamSessionPhase::AttachingAgents),
                            |payload| payload.error = Some(failure.public_reason.clone()),
                        );
                        return Err(TeamError::MemberRuntimeFailed {
                            team_id: team_id.to_owned(),
                            slot_id: agent.slot_id,
                            conversation_id: agent.conversation_id,
                            public_reason: failure.public_reason,
                        });
                    }
                }
                AttachOutcome::SessionStopped => {
                    return Err(TeamError::InvalidRequest(
                        "team session stopped during reconciliation".to_owned(),
                    ));
                }
            }
        }

        self.broadcast_session_status(user_id, team_id, TeamSessionStatus::Ready, None, |payload| {
            payload.server_count = Some(live_roster.len());
        });
        Ok(())
    }

    pub(crate) async fn cleanup_stale_member_runtime_task(
        &self,
        captured_session: &TeamSession,
        conversation_id: &str,
    ) {
        let lock = self
            .ensure_session_locks
            .entry(captured_session.engagement_id().to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;
        if self
            .sessions
            .get(captured_session.engagement_id())
            .is_some_and(|entry| !std::ptr::eq(entry.session.as_ref(), captured_session))
        {
            return;
        }
        self.task_manager
            .kill_and_wait(conversation_id, Some(AgentKillReason::TeamMcpRebuild))
            .await;
    }

    pub async fn get_conversation_config_options(
        &self,
        user_id: &str,
        team_id: &str,
        conversation_id: &str,
    ) -> Result<GetConfigOptionsResponse, TeamError> {
        let row = self.load_owned_team_row(user_id, team_id).await?;

        let team = Team::from_row(&row)?;
        // Resolve the caller's runtime conversation through the ACTIVE engagement's
        // members so a project-bound team reads its own member (whose runtime
        // conversation the UI carries) rather than 404-ing on the template. Legacy /
        // no-member teams fall back to the template row, byte-identical to before.
        let engagement = TeamSession::resolve_engagement_id(&self.repo, user_id, &team).await;
        let member = self
            .resolve_runtime_agent_by_conversation(user_id, &team, &engagement, conversation_id)
            .await?
            .0;
        if self.member_runtime_is_starting(team_id, &member.slot_id) {
            return Err(Self::member_runtime_starting_error(team_id, &member));
        }

        self.conversation_port.get_config_options(conversation_id).await
    }

    pub async fn set_conversation_config_option(
        &self,
        user_id: &str,
        team_id: &str,
        conversation_id: &str,
        option_id: &str,
        request: SetConfigOptionRequest,
    ) -> Result<SetConfigOptionResponse, TeamError> {
        let row = self.load_owned_team_row(user_id, team_id).await?;
        let team = Team::from_row(&row)?;
        // Resolve the caller's runtime conversation through the ACTIVE engagement's
        // members (same switch as the config-option reader): a project-bound team
        // writes its own member conversation, and the model persist that follows is
        // keyed by the engagement runtime slot. Legacy / no-member teams fall back to
        // the template row, byte-identical to before.
        let engagement = TeamSession::resolve_engagement_id(&self.repo, user_id, &team).await;
        let member = self
            .resolve_runtime_agent_by_conversation(user_id, &team, &engagement, conversation_id)
            .await?
            .0;
        if self.member_runtime_is_starting(team_id, &member.slot_id) {
            return Err(Self::member_runtime_starting_error(team_id, &member));
        }

        let options = self.conversation_port.get_config_options(conversation_id).await?;
        let is_global_mode = member.role == TeammateRole::Lead
            && options.config_options.iter().any(|option| {
                option.id == option_id && (option.category.as_deref() == Some("mode") || option.id == "mode")
            });
        if is_global_mode
            && let Some(starting_member) = self
                .global_mode_roster(team_id, &team)
                .await
                .into_iter()
                .find(|agent| self.member_runtime_is_starting(team_id, &agent.slot_id))
        {
            return Err(Self::member_runtime_starting_error(team_id, &starting_member));
        }
        // Matches the frontend's own model-option lookup (category first, then a
        // literal `model` id), so both sides agree on which option is the model.
        let is_model_option = options.config_options.iter().any(|option| {
            option.id == option_id && (option.category.as_deref() == Some("model") || option.id == "model")
        });
        let slot_id = member.slot_id.clone();
        // Captured before the call moves `request`. This is the value to persist,
        // NOT the option's `current_value` in the response: a `PendingNextTurn`
        // confirmation deliberately still reads back the OLD value, so echoing the
        // readback would persist the model the user just switched away from.
        let requested_model = is_model_option.then(|| request.value.trim().to_owned());

        let response = self
            .conversation_port
            .set_config_option(conversation_id, option_id, request)
            .await?;

        // The runtime accepted the switch, so the roster and the persisted
        // conversation state must follow — including for `PendingNextTurn`, where
        // the value governs from the next turn and would otherwise be lost on the
        // next rebuild. A persistence failure must NOT be reported as a failed
        // switch, because the switch already happened; log it and let the
        // session-start reconcile repair the roster.
        if let Some(model) = requested_model.filter(|value| !value.is_empty())
            && let Err(error) = self
                .persist_member_model_selection(
                    user_id,
                    team_id,
                    &slot_id,
                    &model,
                    ModelPersistTrigger::RuntimeConfirmed,
                )
                .await
        {
            warn!(
                team_id,
                slot_id,
                conversation_id,
                model,
                error = %error,
                "team member model switch applied but could not be persisted"
            );
        }

        Ok(response)
    }

    fn member_runtime_is_starting(&self, team_id: &str, slot_id: &str) -> bool {
        // Engagement-keyed map: scan by team (team_id-only check; the starting
        // member belongs to the team's single active session).
        self.sessions
            .iter()
            .find(|entry| entry.session.team_id() == team_id)
            .and_then(|entry| entry.session.work_coordinator().slot_snapshot(slot_id))
            .is_some_and(|snapshot| matches!(snapshot.runtime_constraint, RuntimeConstraint::Starting { .. }))
    }

    /// The roster the global-mode "another member is starting" guard must scan.
    /// For a project-bound team the runtime coordinator is keyed by the
    /// engagement-member `slot_id`s, so the guard has to check the LIVE session
    /// roster, not the `teams.agents` template (whose slot ids never match the
    /// coordinator → the refusal would silently no-op). With no live session the
    /// coordinator is empty anyway, so falling back to the template is
    /// byte-identical to the old template-only check.
    async fn global_mode_roster(&self, team_id: &str, team: &Team) -> Vec<TeamAgent> {
        match self.published_session(team_id) {
            Ok(session) => session.scheduler().list_agents().await,
            Err(_) => team.agents.clone(),
        }
    }

    fn member_runtime_starting_error(team_id: &str, member: &TeamAgent) -> TeamError {
        TeamError::MemberRuntimeStarting {
            team_id: team_id.to_owned(),
            slot_id: member.slot_id.clone(),
            conversation_id: member.conversation_id.clone(),
        }
    }

    fn broadcast_session_status<F>(
        &self,
        user_id: &str,
        team_id: &str,
        status: TeamSessionStatus,
        phase: Option<TeamSessionPhase>,
        customize: F,
    ) where
        F: FnOnce(&mut TeamSessionStatusPayload),
    {
        let mut payload = TeamSessionStatusPayload {
            team_id: team_id.to_owned(),
            status,
            phase,
            server_count: None,
            error: None,
        };
        customize(&mut payload);
        // Session-level status drives the full-screen warmup overlay (leader-only,
        // spec 5.4/5.5). This is a low-volume lifecycle boundary per team, so log
        // at info for production diagnosability — the reason is already the
        // sanitized public failure text, never a raw payload.
        info!(
            team_id = %payload.team_id,
            status = ?payload.status,
            phase = ?payload.phase,
            server_count = ?payload.server_count,
            error = payload.error.as_deref().unwrap_or(""),
            "team session status broadcast"
        );
        // Keep per-user scoping so the overlay event reaches only the owning
        // user's WebSocket subscribers.
        let mut value = serde_json::to_value(payload).expect("serialize team session status payload");
        value["user_id"] = serde_json::Value::String(user_id.to_owned());
        let event = WebSocketMessage::new(TEAM_SESSION_STATUS_CHANGED_EVENT, value);
        self.broadcaster.broadcast(event);
    }

    fn spawn_slow_monitor(session: Arc<TeamSession>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let snapshot = session.work_coordinator().snapshot();
                session.team_run_manager().publish_snapshot_update(&snapshot);
            }
        })
    }

    fn broadcast_team_created(&self, user_id: &str, team_id: &str, team_name: &str) {
        info!(team_id = %team_id, event_name = TEAM_CREATED_EVENT, "team event broadcast");
        self.broadcaster.broadcast(WebSocketMessage::new(
            TEAM_CREATED_EVENT,
            serde_json::json!({ "user_id": user_id, "team_id": team_id, "team_name": team_name }),
        ));
        self.broadcast_team_list_changed(user_id, team_id, "created");
    }

    fn broadcast_team_removed(&self, user_id: &str, team_id: &str) {
        info!(team_id = %team_id, event_name = TEAM_REMOVED_EVENT, "team event broadcast");
        self.broadcaster.broadcast(WebSocketMessage::new(
            TEAM_REMOVED_EVENT,
            serde_json::json!({ "user_id": user_id, "team_id": team_id }),
        ));
        self.broadcast_team_list_changed(user_id, team_id, "removed");
    }

    fn broadcast_team_renamed(&self, user_id: &str, team_id: &str, team_name: &str) {
        info!(team_id = %team_id, event_name = TEAM_RENAMED_EVENT, "team event broadcast");
        self.broadcaster.broadcast(WebSocketMessage::new(
            TEAM_RENAMED_EVENT,
            serde_json::json!({ "user_id": user_id, "team_id": team_id, "team_name": team_name }),
        ));
        self.broadcast_team_list_changed(user_id, team_id, "renamed");
    }

    fn broadcast_team_list_changed(&self, user_id: &str, team_id: &str, action: &str) {
        info!(team_id = %team_id, event_name = crate::events::TEAM_LIST_CHANGED_EVENT, action, "team event broadcast");
        self.broadcaster.broadcast(WebSocketMessage::new(
            crate::events::TEAM_LIST_CHANGED_EVENT,
            serde_json::json!({ "user_id": user_id, "team_id": team_id, "action": action }),
        ));
    }

    pub(crate) fn broadcast_agent_runtime_status(
        &self,
        user_id: &str,
        team_id: &str,
        agent: &TeamAgent,
        status: TeamAgentRuntimeStatus,
        error: Option<String>,
    ) {
        TeamEventEmitter::new(team_id.to_owned(), user_id.to_owned(), self.broadcaster.clone())
            .broadcast_agent_runtime_status(agent, status, error);
    }

    /// Register an event loop for an attaching agent.
    ///
    /// Called from `attach_member_runtime` (the single attach path used by
    /// leader cold-start, reconciliation, `add_agent`, `spawn_agent`, and lazy
    /// wakeup) after the agent process warms up, so it gets its own drain loop.
    pub(crate) fn register_event_loop(
        &self,
        session: &Arc<TeamSession>,
        slot_id: &str,
    ) -> Result<bool, EventLoopRegistrationError> {
        let registry = session.event_loops();

        let ctx = AgentLoopContext {
            team_id: session.team_id().to_owned(),
            slot_id: slot_id.to_owned(),
            user_id: session.user_id().to_owned(),
            session: session.clone(),
            scheduler: session.scheduler().clone(),
            mailbox: session.mailbox().clone(),
            turn_port: self.turn_port.clone(),
            registry: registry.clone(),
        };
        match registry.spawn(slot_id, ctx) {
            Ok(()) => {
                info!(
                    team_id = session.team_id(),
                    slot_id,
                    generation = session.generation(),
                    "agent event loop registered"
                );
                Ok(true)
            }
            Err(EventLoopRegistrationError::Duplicate) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub async fn get_session_user_id(&self, team_id: &str) -> Option<String> {
        // Engagement-keyed map: resolve by team via scan. A team's engagements
        // share one owner, so any match returns the right user.
        self.sessions
            .iter()
            .find(|entry| entry.session.team_id() == team_id)
            .map(|e| e.session.user_id().to_owned())
    }

    /// The conversation port this service was wired with. A `TeamSession`
    /// reaches the latest-assistant read through the weak service back-ref it
    /// already holds, instead of getting a new constructor dependency
    /// (Phase 3a task result capture).
    pub(crate) fn conversation_port(&self) -> Arc<dyn TeamConversationProvisioningPort> {
        self.conversation_port.clone()
    }

    pub(crate) fn capture_published_session(&self, expected: &TeamSession) -> Option<Arc<TeamSession>> {
        self.sessions
            .get(expected.engagement_id())
            .and_then(|entry| std::ptr::eq(entry.session.as_ref(), expected).then(|| Arc::clone(&entry.session)))
    }

    /// Run a synchronous side effect only while `expected` is still the
    /// published session. Keeping the map guard alive through `action`
    /// serializes the effect with session removal/replacement.
    pub(crate) fn with_published_session<R>(
        &self,
        expected: &TeamSession,
        action: impl FnOnce(&TeamSession) -> R,
    ) -> Option<R> {
        let entry = self.sessions.get(expected.engagement_id())?;
        std::ptr::eq(entry.session.as_ref(), expected).then(|| action(&entry.session))
    }

    pub(crate) fn publish_member_runtime_ready_if_current(&self, expected: &TeamSession, agent: &TeamAgent) -> bool {
        self.with_published_session(expected, |_| {
            self.broadcast_agent_runtime_status(
                expected.user_id(),
                expected.team_id(),
                agent,
                TeamAgentRuntimeStatus::Ready,
                None,
            );
        })
        .is_some()
    }

    pub(crate) fn publish_member_runtime_starting_if_current(&self, expected: &TeamSession) -> bool {
        self.with_published_session(expected, |_| {
            self.broadcast_session_status(
                expected.user_id(),
                expected.team_id(),
                TeamSessionStatus::Starting,
                Some(TeamSessionPhase::AttachingAgents),
                |_| {},
            );
        })
        .is_some()
    }

    pub(crate) fn publish_member_runtime_failed_if_current(&self, expected: &TeamSession, reason: &str) -> bool {
        self.with_published_session(expected, |_| {
            self.broadcast_session_status(
                expected.user_id(),
                expected.team_id(),
                TeamSessionStatus::Failed,
                Some(TeamSessionPhase::AttachingAgents),
                |payload| payload.error = Some(reason.to_owned()),
            );
        })
        .is_some()
    }

    pub(crate) async fn refresh_member_runtime_status(&self, expected: &TeamSession) {
        let membership_lock = self
            .add_agent_locks
            .entry(expected.team_id().to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _membership_guard = membership_lock.lock().await;
        let Ok(Some(row)) = self.repo.get_team(expected.user_id(), expected.team_id()).await else {
            return;
        };
        let Ok(team) = Team::from_row(&row) else {
            return;
        };

        // Session-level status is leader-scoped: "Ready = leader ready = team
        // usable" (spec 5.2). It drives the full-screen warmup overlay, which
        // must reflect the leader only (spec 5.4/5.5). Teammate runtimes
        // (dormant/pending/ready/failed) are surfaced per-member via
        // `agentRuntimeStatusChanged` and must NOT flip the session status, or
        // the overlay would resurface on lazy wakeup / add-member and a teammate
        // failure would raise the full-screen failure card.
        let Some(leader) = team.agents.iter().find(|agent| agent.role == TeammateRole::Lead) else {
            // No lead in the roster is a malformed team; bootstrap already
            // reports it. Nothing to publish here.
            return;
        };

        match expected.member_runtimes().snapshot(&leader.slot_id) {
            MemberRuntimeSnapshot::Ready => {
                let _ = self.with_published_session(expected, |_| {
                    self.broadcast_session_status(
                        expected.user_id(),
                        expected.team_id(),
                        TeamSessionStatus::Ready,
                        None,
                        |payload| {
                            payload.server_count = Some(team.agents.len());
                        },
                    );
                });
            }
            MemberRuntimeSnapshot::Failed { failure, .. } => {
                self.publish_member_runtime_failed_if_current(expected, &failure.public_reason);
            }
            // An in-flight leader attach/remove (cold start, repair, retry) is
            // the only case that legitimately raises the overlay again. The lead
            // cannot be removed, so `Removing` is defensive.
            MemberRuntimeSnapshot::Attaching { .. } | MemberRuntimeSnapshot::Removing { .. } => {
                self.publish_member_runtime_starting_if_current(expected);
            }
            // The leader is attached at bootstrap and never dormant in steady
            // state; treat a stray Absent defensively as in-flight rather than
            // prematurely declaring Ready.
            MemberRuntimeSnapshot::Absent => {
                self.publish_member_runtime_starting_if_current(expected);
            }
            MemberRuntimeSnapshot::SessionStopped => {}
        }
    }

    pub async fn get_run_state(&self, user_id: &str, team_id: &str) -> Result<TeamRunStateResponse, TeamError> {
        let team = self.load_owned_team(user_id, team_id).await?;
        let engagement = TeamSession::resolve_engagement_id(&self.repo, user_id, &team).await;
        let session = self.sessions.get(&engagement).map(|entry| Arc::clone(&entry.session));
        let Some(session) = session else {
            return Ok(TeamRunStateResponse {
                session_generation: None,
                active_run: None,
                slot_work: Vec::new(),
            });
        };
        let snapshot = session.work_coordinator().snapshot();
        let active_run = session.team_run_manager().current_payload(&snapshot).filter(|run| {
            matches!(
                run.status,
                aionui_api_types::TeamRunStatus::Accepted
                    | aionui_api_types::TeamRunStatus::Running
                    | aionui_api_types::TeamRunStatus::Cancelling
            )
        });
        let slot_work = snapshot.slots.iter().map(TeamRunManager::slot_payload).collect();
        Ok(TeamRunStateResponse {
            session_generation: Some(snapshot.session_generation),
            active_run,
            slot_work,
        })
    }

    pub fn get_session_scheduler(&self, team_id: &str) -> Option<Arc<crate::scheduler::TeammateManager>> {
        // Engagement-keyed map: resolve by team via scan (team_id-only API; no
        // user/project context to compute the engagement key from). Single-active
        // behavior: one live session per team.
        self.sessions
            .iter()
            .find(|entry| entry.session.team_id() == team_id)
            .map(|e| e.session.scheduler().clone())
    }

    pub async fn resolve_team_tool_context(
        &self,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<ResolvedTeamToolContext, TeamToolErrorPayload> {
        let Some(binding_lookup) = self
            .conversation_port
            .lookup_team_binding_by_conversation(conversation_id)
            .await
            .map_err(|error| error_payload(TeamToolErrorCode::RuntimeContextMissing, error.to_string()))?
        else {
            return Err(error_payload(
                TeamToolErrorCode::ConversationNotFound,
                "conversation not found",
            ));
        };

        if binding_lookup.user_id != user_id {
            return Err(error_payload(
                TeamToolErrorCode::PermissionDenied,
                "conversation does not belong to user",
            ));
        }

        let Some(team_id) = binding_lookup.team_id.clone() else {
            return Ok(ResolvedTeamToolContext {
                response: TeamToolContextResponse {
                    in_team: false,
                    conversation_id: conversation_id.to_owned(),
                    team_id: None,
                    team_name: None,
                    slot_id: None,
                    role: None,
                    agent_name: None,
                    transport: None,
                    allowed_tools: Vec::new(),
                },
                context: None,
            });
        };

        let team_row = self
            .repo
            .get_team(user_id, &team_id)
            .await
            .map_err(|error| error_payload(TeamToolErrorCode::RuntimeContextMissing, error.to_string()))?
            .ok_or_else(|| error_payload(TeamToolErrorCode::TeamNotFound, "team not found"))?;

        let binding = TeamSessionBinding {
            team_id: team_id.clone(),
            slot_id: binding_lookup.slot_id,
            role: binding_lookup.role,
            runtime_seed: Default::default(),
            mcp: None,
        };
        let agents: Vec<crate::types::TeamAgent> = serde_json::from_str(&team_row.agents)
            .map_err(|error| error_payload(TeamToolErrorCode::RuntimeContextMissing, error.to_string()))?;
        let agent = agent_for_conversation(&agents, conversation_id, &binding)?;
        let context = crate::tool_executor::TeamToolContext {
            team_id: team_id.clone(),
            caller_slot_id: agent.slot_id.clone(),
            caller_role: agent.role,
            user_id: Some(user_id.to_owned()),
            conversation_id: Some(conversation_id.to_owned()),
            transport: TeamToolTransport::CliAssumed,
        };
        let allowed_tools = aionui_api_types::team_tool_descriptors_for_role(role_to_tool_role(agent.role))
            .into_iter()
            .map(|descriptor| descriptor.name)
            .collect::<Vec<_>>();
        Ok(ResolvedTeamToolContext {
            response: TeamToolContextResponse {
                in_team: true,
                conversation_id: conversation_id.to_owned(),
                team_id: Some(team_id),
                team_name: Some(team_row.name),
                slot_id: Some(agent.slot_id.clone()),
                role: Some(role_to_tool_role(agent.role)),
                agent_name: Some(agent.name.clone()),
                transport: Some(TeamToolTransport::CliAssumed),
                allowed_tools,
            },
            context: Some(context),
        })
    }

    pub async fn execute_team_tool(
        &self,
        context: &crate::tool_executor::TeamToolContext,
        call: TeamToolCall,
    ) -> Result<serde_json::Value, TeamToolErrorPayload> {
        let scheduler = self
            .get_session_scheduler(&context.team_id)
            .ok_or_else(|| error_payload(TeamToolErrorCode::TeamNotFound, "active team session not found"))?;
        execute_with_scheduler(&scheduler, &self.self_ref, context, call).await
    }

    #[cfg(test)]
    fn session_has_slow_monitor(&self, team_id: &str) -> bool {
        self.sessions
            .get(team_id)
            .map(|entry| !entry.slow_monitor_handle.is_finished())
            .unwrap_or(false)
    }

    #[cfg(test)]
    fn session_count_for_test(&self) -> usize {
        self.sessions.len()
    }

    pub async fn stop_session(&self, user_id: &str, team_id: &str) -> Result<(), TeamError> {
        let team = self.load_owned_team(user_id, team_id).await?;
        let engagement = TeamSession::resolve_engagement_id(&self.repo, user_id, &team).await;
        self.stop_session_unchecked(&engagement);
        Ok(())
    }

    pub fn stop_sessions_for_user(&self, user_id: &str) -> usize {
        let keys: Vec<String> = self
            .sessions
            .iter()
            .filter(|entry| entry.session.user_id() == user_id)
            .map(|entry| entry.key().clone())
            .collect();
        let stopped = keys.len();
        for key in keys {
            self.stop_session_unchecked(&key);
        }
        stopped
    }

    fn stop_session_unchecked(&self, engagement_key: &str) {
        if let Some((_, entry)) = self.sessions.remove(engagement_key) {
            entry.slow_monitor_handle.abort();
            entry.session.stop();
        }
    }

    /// Resolve the runtime-map key for a team's ACTIVE engagement from its
    /// project binding, using the same logic a `TeamSession` stamps on its own
    /// writes: `team.project_id == None → team_id` (legacy sentinel), else the
    /// find-or-create engagement id. Prefer `session.engagement_id()` wherever a
    /// session is already loaded (avoids re-resolving / re-creating the row).
    async fn engagement_key(&self, user_id: &str, team_id: &str) -> Result<String, TeamError> {
        let team = self.load_owned_team(user_id, team_id).await?;
        Ok(TeamSession::resolve_engagement_id(&self.repo, user_id, &team).await)
    }

    /// Stop every live session belonging to `team_id` (across all its
    /// engagements now that the map is engagement-keyed) and drop the
    /// engagement's membership/ensure lock entries so deletion does not leak
    /// map entries. Returns the engagement keys touched.
    fn stop_sessions_for_team(&self, team_id: &str) -> Vec<String> {
        let keys: Vec<String> = self
            .sessions
            .iter()
            .filter(|entry| entry.session.team_id() == team_id)
            .map(|entry| entry.key().clone())
            .collect();
        // `sessions`/`ensure_session_locks` are engagement-keyed; `add_agent_locks`
        // stays team-keyed (roster RMW is team-scoped), so drop it once by team id.
        self.add_agent_locks.remove(team_id);
        for key in &keys {
            self.stop_session_unchecked(key);
            self.ensure_session_locks.remove(key);
        }
        keys
    }

    pub async fn cleanup_idle_team_runtime_tasks(
        &self,
        idle_conversation_ids: Vec<String>,
        active_leases: &ActiveLeaseRegistry,
        idle_threshold_ms: TimestampMs,
    ) -> Vec<String> {
        if idle_conversation_ids.is_empty() {
            return Vec::new();
        }

        let idle_conversation_set: HashSet<String> = idle_conversation_ids.iter().cloned().collect();
        let now = now_ms();
        let mut handled_conversations = HashSet::new();
        let mut cleanup_teams = Vec::new();

        for entry in self.sessions.iter() {
            let engagement_key = entry.key().clone();
            let session = Arc::clone(&entry.session);
            let team_id = session.team_id().to_owned();
            let agents = session.scheduler().list_agents().await;
            let matched_idle_count = agents
                .iter()
                .filter(|agent| idle_conversation_set.contains(&agent.conversation_id))
                .count();
            if matched_idle_count == 0 {
                continue;
            }

            for agent in &agents {
                handled_conversations.insert(agent.conversation_id.clone());
            }

            if session.team_run_manager().current_active_run_id().is_some() {
                debug!(
                    team_id,
                    matched_idle_count, "team idle cleanup skipped because team run is active"
                );
                continue;
            }

            if agents
                .iter()
                .any(|agent| active_leases.active_until(&agent.conversation_id).is_some())
            {
                debug!(
                    team_id,
                    matched_idle_count, "team idle cleanup skipped because at least one member has an active lease"
                );
                continue;
            }

            if !agents.iter().all(|agent| {
                self.task_manager
                    .get_task(&agent.conversation_id)
                    .map(|task| is_idle_collectable_team_member(&task, now, idle_threshold_ms))
                    .unwrap_or(true)
            }) {
                debug!(
                    team_id,
                    matched_idle_count, "team idle cleanup skipped because at least one member runtime task is active"
                );
                continue;
            }

            cleanup_teams.push((engagement_key, team_id, agents, matched_idle_count));
        }

        for (engagement_key, team_id, agents, matched_idle_count) in cleanup_teams {
            info!(
                team_id,
                matched_idle_count,
                member_count = agents.len(),
                "team idle cleanup stopping idle team session"
            );
            info!(team_id, reason = "idle_cleanup", "broadcasting team session stopped");
            if let Some(entry) = self.sessions.get(&engagement_key) {
                self.broadcast_session_status(
                    entry.session.user_id(),
                    &team_id,
                    TeamSessionStatus::Stopped,
                    None,
                    |_| {},
                );
            }
            self.stop_session_unchecked(&engagement_key);
            for agent in agents {
                self.task_manager
                    .kill_and_wait(&agent.conversation_id, Some(AgentKillReason::IdleTimeout))
                    .await;
            }
        }

        idle_conversation_ids
            .into_iter()
            .filter(|conversation_id| !handled_conversations.contains(conversation_id))
            .collect()
    }

    pub async fn send_message(
        &self,
        user_id: &str,
        team_id: &str,
        content: &str,
        files: Option<Vec<ChatFileRef>>,
    ) -> Result<TeamRunAckResponse, TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        self.ensure_session_inner(team_id, Some(user_id)).await?;
        let (content, files) = self.resolve_message_attachments(user_id, content, files).await?;
        let session = self.published_session(team_id)?;
        session.send_message(&content, files).await
    }

    pub async fn send_message_to_agent(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
        content: &str,
        files: Option<Vec<ChatFileRef>>,
    ) -> Result<TeamRunAckResponse, TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        self.ensure_session_inner(team_id, Some(user_id)).await?;
        let (content, files) = self.resolve_message_attachments(user_id, content, files).await?;
        let session = self.published_session(team_id)?;
        session.send_message_to_agent(slot_id, &content, files).await
    }

    pub async fn interrupt_agent(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
        request: InterruptTeamAgentRequest,
    ) -> Result<TeamInterruptAgentResponse, TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        self.ensure_session_inner(team_id, Some(user_id)).await?;
        let (message, files) = self
            .resolve_message_attachments(user_id, &request.message, request.files)
            .await?;
        self.published_session(team_id)?
            .interrupt_agent_from_user(slot_id, &message, files, request.reason, request.queued_policy)
            .await
    }

    fn published_session(&self, team_id: &str) -> Result<Arc<TeamSession>, TeamError> {
        // Engagement-keyed map: resolve by team via scan. The send/MCP paths that
        // only carry `team_id` operate on the team's single active session
        // (per-engagement routing needs the Phase-5 project selector).
        self.sessions
            .iter()
            .find(|entry| entry.session.team_id() == team_id)
            .map(|entry| Arc::clone(&entry.session))
            .ok_or_else(|| TeamError::SessionNotFound(team_id.to_owned()))
    }

    pub(crate) async fn peek_agent_messages(
        &self,
        team_id: &str,
        slot_id: &str,
    ) -> Result<crate::session::AgentInboxPeek, TeamError> {
        self.published_session(team_id)?.peek_agent_messages(slot_id).await
    }

    pub(crate) async fn observe_agent_messages(
        &self,
        team_id: &str,
        slot_id: &str,
        expected_batch_id: &str,
        message_ids: &[String],
    ) -> Result<ObserveMessagesResult, TeamError> {
        self.published_session(team_id)?
            .observe_agent_messages(slot_id, expected_batch_id, message_ids)
            .await
    }

    /// Resolve a send's attachments to absolute paths and re-inline them into
    /// the content (`[[AION_FILES]]` form) at the team send boundary. Atomic;
    /// empty/absent `files` is a no-op needing no project service.
    async fn resolve_message_attachments(
        &self,
        user_id: &str,
        content: &str,
        files: Option<Vec<ChatFileRef>>,
    ) -> Result<(String, Option<Vec<String>>), TeamError> {
        let files = match files {
            Some(files) if !files.is_empty() => files,
            _ => return Ok((content.to_owned(), None)),
        };
        let project = self
            .project_service
            .read()
            .ok()
            .and_then(|guard| guard.clone())
            .ok_or_else(|| {
                TeamError::InvalidRequest("project service unavailable; cannot resolve file attachments".into())
            })?;
        let upload_root = std::env::temp_dir().join("aionui");
        let resolved = project
            .resolve_chat_message(user_id, content, &files, &upload_root)
            .await
            .map_err(|err| TeamError::InvalidRequest(err.to_string()))?;
        Ok((resolved.content, Some(resolved.files)))
    }

    /// Directed retry/wakeup for a single member runtime (dormant or failed),
    /// reusing the one attach path. Backs the send-box "retry start" entry.
    /// `reserve_attach(slot, true)` retries a `Failed` member; a dormant
    /// (`Absent`) member is attached fresh. Non-blocking: the attach runs in
    /// the background and any preserved unread mailbox rows are re-drained by
    /// the member's event loop via `reconcile_mailbox`.
    pub async fn attach_agent_runtime(&self, user_id: &str, team_id: &str, slot_id: &str) -> Result<(), TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        self.ensure_session_inner(team_id, Some(user_id)).await?;
        let session = self.published_session(team_id)?;
        let agent = session.scheduler().get_agent(slot_id).await?;
        let service = self
            .self_ref
            .upgrade()
            .ok_or_else(|| TeamError::InvalidRequest("team service is shutting down".to_owned()))?;
        let reservation = session.member_runtimes().reserve_attach(slot_id, true);
        self.broadcast_agent_runtime_status(user_id, team_id, &agent, TeamAgentRuntimeStatus::Pending, None);
        spawn_attach_agent_process_bg(
            service,
            Arc::clone(&session),
            user_id.to_owned(),
            agent,
            self.task_manager.clone(),
            reservation,
            // Directed user retry: failures surface inline, do not wake the leader.
            false,
        );
        Ok(())
    }

    /// Force-rebuild one team member runtime while preserving its conversation
    /// and resume anchor. This waits for the shared attach path to reach a
    /// terminal outcome so callers only receive success once the runtime is
    /// ready.
    pub async fn restart_agent_runtime(&self, user_id: &str, team_id: &str, slot_id: &str) -> Result<(), TeamError> {
        self.restart_agent_runtime_inner(user_id, team_id, slot_id, false).await
    }

    pub(crate) async fn restart_agent_runtime_for_mcp_refresh(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
    ) -> Result<(), TeamError> {
        self.restart_agent_runtime_inner(user_id, team_id, slot_id, true).await
    }

    async fn restart_agent_runtime_inner(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
        allow_queued: bool,
    ) -> Result<(), TeamError> {
        let team = self.load_owned_team(user_id, team_id).await?;
        let engagement = TeamSession::resolve_engagement_id(&self.repo, user_id, &team).await;
        // Resolve the runtime identity through the ACTIVE engagement's members so a
        // project-bound team (whose UI carries engagement runtime slot ids) restarts
        // its own member instead of 404-ing on the template. Legacy / no-member teams
        // fall back to the template slot, byte-identical to before.
        let requested_agent = self
            .resolve_runtime_agent(user_id, &team, &engagement, slot_id)
            .await?
            .0;
        if allow_queued {
            self.ensure_session_inner(team_id, Some(user_id)).await?;
        }
        let session = {
            let entry = self
                .sessions
                .get(&engagement)
                .ok_or_else(|| TeamError::RuntimeNotReady {
                    conversation_id: requested_agent.conversation_id.clone(),
                })?;
            Arc::clone(&entry.session)
        };
        let agent = session.scheduler().get_agent(slot_id).await?;
        let service = self
            .self_ref
            .upgrade()
            .ok_or_else(|| TeamError::InvalidRequest("team service is shutting down".to_owned()))?;
        let busy_error = || TeamError::MemberBusy {
            team_id: team_id.to_owned(),
            slot_id: slot_id.to_owned(),
            conversation_id: agent.conversation_id.clone(),
        };
        if !allow_queued {
            match session.member_runtimes().snapshot(slot_id) {
                MemberRuntimeSnapshot::Ready => {}
                MemberRuntimeSnapshot::Attaching { .. } => {
                    return Err(TeamError::MemberRuntimeStarting {
                        team_id: team_id.to_owned(),
                        slot_id: slot_id.to_owned(),
                        conversation_id: agent.conversation_id.clone(),
                    });
                }
                MemberRuntimeSnapshot::Removing { .. } => {
                    return Err(TeamError::InvalidRequest(format!(
                        "team member runtime is being removed: {slot_id}"
                    )));
                }
                MemberRuntimeSnapshot::Absent
                | MemberRuntimeSnapshot::Failed { .. }
                | MemberRuntimeSnapshot::SessionStopped => {
                    return Err(TeamError::RuntimeNotReady {
                        conversation_id: agent.conversation_id.clone(),
                    });
                }
            }
        }
        let restart_gate = if allow_queued {
            session.work_coordinator().begin_mcp_runtime_restart(slot_id)
        } else {
            session.work_coordinator().begin_runtime_restart(slot_id)
        }
        .map_err(|rejection| match rejection {
            RuntimeRestartRejection::Busy => busy_error(),
            RuntimeRestartRejection::Removing => {
                TeamError::InvalidRequest(format!("team member runtime is being removed: {slot_id}"))
            }
            RuntimeRestartRejection::SessionStopped => TeamError::SessionNotFound(team_id.to_owned()),
        })?;

        let lease = match session.member_runtimes().reserve_restart(slot_id) {
            ReserveAttach::Start(lease) => lease,
            ReserveAttach::Join(_) | ReserveAttach::AlreadyReady => {
                session.work_coordinator().abort_runtime_restart(slot_id, &restart_gate);
                return Err(busy_error());
            }
            ReserveAttach::Removing(_) => {
                session.work_coordinator().abort_runtime_restart(slot_id, &restart_gate);
                return Err(TeamError::InvalidRequest(format!(
                    "team member runtime is being removed: {slot_id}"
                )));
            }
            ReserveAttach::SessionStopped => {
                session.work_coordinator().abort_runtime_restart(slot_id, &restart_gate);
                return Err(TeamError::SessionNotFound(team_id.to_owned()));
            }
        };

        self.broadcast_agent_runtime_status(user_id, team_id, &agent, TeamAgentRuntimeStatus::Pending, None);
        info!(
            team_id,
            slot_id,
            conversation_id = agent.conversation_id,
            "team member runtime restart requested"
        );
        match attach_member_runtime(
            service,
            Arc::clone(&session),
            user_id.to_owned(),
            agent.clone(),
            self.task_manager.clone(),
            lease,
            false,
        )
        .await
        {
            AttachOutcome::Ready => Ok(()),
            AttachOutcome::Failed(failure) => Err(TeamError::MemberRuntimeFailed {
                team_id: team_id.to_owned(),
                slot_id: slot_id.to_owned(),
                conversation_id: agent.conversation_id,
                public_reason: failure.public_reason,
            }),
            AttachOutcome::Removed => Err(TeamError::AgentNotFound(slot_id.to_owned())),
            AttachOutcome::SessionStopped => Err(TeamError::SessionNotFound(team_id.to_owned())),
        }
    }

    /// Reset one team member's ACP resume anchor and synchronously rebuild its
    /// runtime. Conversation metadata and visible history are deliberately
    /// retained; only the backend thread identity is cleared.
    pub async fn clear_agent_context(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
    ) -> Result<TeamContextResetResponse, TeamError> {
        self.clear_agent_context_inner(user_id, team_id, slot_id).await
    }

    /// MCP calls originate from an already-running team session. Keeping this
    /// variant free of `ensure_session` avoids a recursive async type through
    /// `TeamMcpServer::start` while retaining the same reset orchestration.
    pub(crate) async fn clear_agent_context_in_session(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
    ) -> Result<TeamContextResetResponse, TeamError> {
        self.clear_agent_context_inner(user_id, team_id, slot_id).await
    }

    async fn clear_agent_context_inner(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
    ) -> Result<TeamContextResetResponse, TeamError> {
        let team = self.load_owned_team(user_id, team_id).await?;
        let engagement = TeamSession::resolve_engagement_id(&self.repo, user_id, &team).await;
        // Context reset kills + clears the ACTIVE engagement's member conversation,
        // so resolve the runtime slot the caller sent to the engagement's own
        // conversation_id (never the template's or another engagement's). Legacy /
        // no-member teams resolve to the template row, byte-identical to before.
        let agent = self
            .resolve_runtime_agent(user_id, &team, &engagement, slot_id)
            .await?
            .0;
        let session = self.sessions.get(&engagement).map(|entry| Arc::clone(&entry.session));
        let capability = self
            .context_reset_capability_for_session(user_id, &agent, session.as_deref())
            .await?;
        match capability.availability {
            TeamContextResetAvailability::Ready => {}
            TeamContextResetAvailability::LeaderNotTargetable => {
                return Err(TeamError::ContextResetLeaderNotTargetable {
                    team_id: team_id.to_owned(),
                    slot_id: slot_id.to_owned(),
                    conversation_id: agent.conversation_id,
                });
            }
            TeamContextResetAvailability::Unsupported => {
                return Err(TeamError::MemberUnsupported {
                    team_id: team_id.to_owned(),
                    slot_id: slot_id.to_owned(),
                    conversation_id: agent.conversation_id,
                    backend: agent.backend,
                });
            }
            availability => {
                return Err(TeamError::ContextResetUnavailable {
                    team_id: team_id.to_owned(),
                    slot_id: slot_id.to_owned(),
                    conversation_id: agent.conversation_id,
                    availability,
                });
            }
        }
        let session = session.ok_or_else(|| TeamError::ContextResetUnavailable {
            team_id: team_id.to_owned(),
            slot_id: slot_id.to_owned(),
            conversation_id: agent.conversation_id.clone(),
            availability: TeamContextResetAvailability::SessionStopped,
        })?;
        let service = self
            .self_ref
            .upgrade()
            .ok_or_else(|| TeamError::InvalidRequest("team service is shutting down".to_owned()))?;
        let busy_error = || TeamError::MemberBusy {
            team_id: team_id.to_owned(),
            slot_id: slot_id.to_owned(),
            conversation_id: agent.conversation_id.clone(),
        };
        let restart_gate = session
            .work_coordinator()
            .begin_runtime_restart(slot_id)
            .map_err(|rejection| match rejection {
                RuntimeRestartRejection::Busy => busy_error(),
                RuntimeRestartRejection::Removing => TeamError::ContextResetUnavailable {
                    team_id: team_id.to_owned(),
                    slot_id: slot_id.to_owned(),
                    conversation_id: agent.conversation_id.clone(),
                    availability: TeamContextResetAvailability::Removing,
                },
                RuntimeRestartRejection::SessionStopped => TeamError::ContextResetUnavailable {
                    team_id: team_id.to_owned(),
                    slot_id: slot_id.to_owned(),
                    conversation_id: agent.conversation_id.clone(),
                    availability: TeamContextResetAvailability::SessionStopped,
                },
            })?;

        let preserved_unread_count = match session.mailbox().peek_unread(team_id, slot_id).await {
            Ok(messages) => messages.len(),
            Err(error) => {
                session.work_coordinator().abort_runtime_restart(slot_id, &restart_gate);
                return Err(error);
            }
        };

        let lease = match session.member_runtimes().reserve_restart(slot_id) {
            ReserveAttach::Start(lease) => lease,
            ReserveAttach::Join(_) | ReserveAttach::AlreadyReady => {
                session.work_coordinator().abort_runtime_restart(slot_id, &restart_gate);
                return Err(busy_error());
            }
            ReserveAttach::Removing(_) => {
                session.work_coordinator().abort_runtime_restart(slot_id, &restart_gate);
                return Err(TeamError::ContextResetUnavailable {
                    team_id: team_id.to_owned(),
                    slot_id: slot_id.to_owned(),
                    conversation_id: agent.conversation_id.clone(),
                    availability: TeamContextResetAvailability::Removing,
                });
            }
            ReserveAttach::SessionStopped => {
                session.work_coordinator().abort_runtime_restart(slot_id, &restart_gate);
                return Err(TeamError::ContextResetUnavailable {
                    team_id: team_id.to_owned(),
                    slot_id: slot_id.to_owned(),
                    conversation_id: agent.conversation_id.clone(),
                    availability: TeamContextResetAvailability::SessionStopped,
                });
            }
        };
        let operation_id = lease.operation_id();

        self.broadcast_agent_runtime_status(user_id, team_id, &agent, TeamAgentRuntimeStatus::Pending, None);
        info!(
            team_id,
            slot_id,
            conversation_id = agent.conversation_id,
            operation_id,
            preserved_unread_count,
            "team member context reset requested"
        );
        self.task_manager
            .kill_and_wait(&agent.conversation_id, Some(AgentKillReason::TeamContextReset))
            .await;
        let cleared = match self
            .conversation_port
            .clear_context_anchor(user_id, &agent.conversation_id)
            .await
        {
            Ok(cleared) => cleared,
            Err(error) => {
                let recovery = attach_member_runtime_after_kill(
                    Arc::clone(&service),
                    Arc::clone(&session),
                    user_id.to_owned(),
                    agent.clone(),
                    self.task_manager.clone(),
                    lease,
                    false,
                )
                .await;
                warn!(
                    team_id,
                    slot_id,
                    conversation_id = agent.conversation_id,
                    operation_id,
                    recovery_ready = matches!(recovery, AttachOutcome::Ready),
                    error = %error,
                    "team member context reset failed before anchor clear"
                );
                return Err(error);
            }
        };
        if !cleared {
            let recovery = attach_member_runtime_after_kill(
                Arc::clone(&service),
                Arc::clone(&session),
                user_id.to_owned(),
                agent.clone(),
                self.task_manager.clone(),
                lease,
                false,
            )
            .await;
            let runtime_status = if matches!(recovery, AttachOutcome::Ready) {
                TeamContextResetRuntimeStatus::Ready
            } else {
                TeamContextResetRuntimeStatus::Failed
            };
            warn!(
                team_id,
                slot_id,
                conversation_id = agent.conversation_id,
                operation_id,
                preserved_unread_count,
                runtime_ready = runtime_status == TeamContextResetRuntimeStatus::Ready,
                "team member context reset was not applied"
            );
            return Ok(TeamContextResetResponse {
                reset_status: TeamContextResetStatus::NotApplied,
                runtime_status,
                preserved_unread_count,
            });
        }

        info!(
            team_id,
            slot_id,
            conversation_id = agent.conversation_id,
            operation_id,
            preserved_unread_count,
            "team member context reset anchor cleared"
        );
        let role_prompt_error = session.scheduler().require_role_prompt(slot_id).await.err();
        if let Some(error) = &role_prompt_error {
            warn!(
                team_id,
                slot_id,
                conversation_id = agent.conversation_id,
                operation_id,
                error = %error,
                "team member context reset could not schedule role prompt reinjection"
            );
        }

        let attach_outcome = attach_member_runtime_after_kill(
            service,
            Arc::clone(&session),
            user_id.to_owned(),
            agent.clone(),
            self.task_manager.clone(),
            lease,
            false,
        )
        .await;
        let runtime_status = if role_prompt_error.is_none() && matches!(attach_outcome, AttachOutcome::Ready) {
            TeamContextResetRuntimeStatus::Ready
        } else {
            TeamContextResetRuntimeStatus::Failed
        };
        if let Err(error) = session.project_context_reset_notice(slot_id, runtime_status).await {
            warn!(
                team_id,
                slot_id,
                conversation_id = agent.conversation_id,
                operation_id,
                error = %error,
                "team member context reset notice projection failed"
            );
        }
        if runtime_status == TeamContextResetRuntimeStatus::Ready {
            info!(
                team_id,
                slot_id,
                conversation_id = agent.conversation_id,
                operation_id,
                preserved_unread_count,
                "team member context reset completed"
            );
        } else {
            warn!(
                team_id,
                slot_id,
                conversation_id = agent.conversation_id,
                operation_id,
                preserved_unread_count,
                "team member context reset completed but runtime attach failed"
            );
        }
        Ok(TeamContextResetResponse {
            reset_status: TeamContextResetStatus::Completed,
            runtime_status,
            preserved_unread_count,
        })
    }

    pub async fn cancel_run(
        &self,
        user_id: &str,
        team_id: &str,
        team_run_id: &str,
        target_slot_id: Option<String>,
        reason: Option<String>,
    ) -> Result<(), TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        self.ensure_session_inner(team_id, Some(user_id)).await?;
        let session = self.published_session(team_id)?;
        session.cancel_run(team_run_id, target_slot_id, reason).await
    }

    pub async fn cancel_child_turn(
        &self,
        user_id: &str,
        team_id: &str,
        team_run_id: &str,
        slot_id: &str,
        reason: Option<String>,
    ) -> Result<(), TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        self.ensure_session_inner(team_id, Some(user_id)).await?;
        let session = self.published_session(team_id)?;
        session.cancel_child_turn(team_run_id, slot_id, reason).await
    }

    pub async fn pause_slot_work(
        &self,
        user_id: &str,
        team_id: &str,
        team_run_id: &str,
        slot_id: &str,
        reason: Option<String>,
    ) -> Result<(), TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        self.ensure_session_inner(team_id, Some(user_id)).await?;
        let session = self.published_session(team_id)?;
        session.pause_slot_work(team_run_id, slot_id, reason).await
    }

    pub async fn set_session_mode(&self, user_id: &str, team_id: &str, mode: &str) -> Result<(), TeamError> {
        let team = self.load_owned_team(user_id, team_id).await?;
        // Route the "another member is starting" guard through the LIVE engagement
        // roster (see `global_mode_roster`): a project-bound team's coordinator is
        // keyed by engagement slot ids, so checking the `teams.agents` template
        // silently no-ops the refusal. Legacy / no-session teams fall back to the
        // template, byte-identical to before.
        if let Some(starting_member) = self
            .global_mode_roster(team_id, &team)
            .await
            .into_iter()
            .find(|agent| self.member_runtime_is_starting(team_id, &agent.slot_id))
        {
            return Err(Self::member_runtime_starting_error(team_id, &starting_member));
        }
        let provisioner = self.provisioner();
        self.repo
            .update_team(
                user_id,
                team_id,
                &UpdateTeamParams {
                    session_mode: Some(mode.to_owned()),
                    ..Default::default()
                },
            )
            .await?;

        for agent in &team.agents {
            let mode_applied = match self.task_manager.get_task(&agent.conversation_id) {
                Some(instance) => match set_active_agent_session_mode(&instance, mode).await {
                    Ok(()) => true,
                    Err(e) => {
                        warn!(
                            team_id,
                            slot_id = %agent.slot_id,
                            conversation_id = %agent.conversation_id,
                            error = %e,
                            "failed to set session mode on agent"
                        );
                        false
                    }
                },
                None => true,
            };
            if mode_applied && let Err(e) = provisioner.update_session_mode_seed(agent, mode).await {
                warn!(
                    team_id,
                    slot_id = %agent.slot_id,
                    conversation_id = %agent.conversation_id,
                    error = %e,
                    "failed to persist team session mode seed"
                );
            }
        }

        Ok(())
    }

    pub async fn send_agent_message_from_agent(
        &self,
        team_id: &str,
        from_slot_id: &str,
        to_slot_id: &str,
        content: &str,
        files: Option<Vec<String>>,
    ) -> Result<AgentMessageQueueResult, TeamError> {
        let session = self.published_session(team_id)?;
        session
            .send_agent_message_from_agent(from_slot_id, to_slot_id, content, files)
            .await
    }

    pub async fn interrupt_agent_from_agent(
        &self,
        team_id: &str,
        from_slot_id: &str,
        to_slot_id: &str,
        message: &str,
        files: Option<Vec<String>>,
        reason: Option<String>,
    ) -> Result<TeamInterruptAgentResponse, TeamError> {
        self.published_session(team_id)?
            .interrupt_agent_from_agent(from_slot_id, to_slot_id, message, files, reason)
            .await
    }

    pub async fn shutdown_agent_in_session(
        &self,
        team_id: &str,
        caller_slot_id: &str,
        target_slot_id: &str,
        reason: Option<String>,
    ) -> Result<(), TeamError> {
        let session = self.published_session(team_id)?;
        session.shutdown_agent(caller_slot_id, target_slot_id, reason).await
    }

    pub(crate) async fn wake_leader_after_recovery_message(
        &self,
        team_id: &str,
        source_slot_id: &str,
        source: WorkSource,
    ) -> Result<(), TeamError> {
        let session = self.published_session(team_id)?;
        session.wake_leader_after_recovery_message(source_slot_id, source).await
    }
}

async fn set_active_agent_session_mode(instance: &AgentInstance, mode: &str) -> Result<(), AgentError> {
    #[allow(unreachable_patterns)]
    match instance {
        AgentInstance::Acp(_) => instance.set_config_option("mode", mode).await.map(|_| ()),
        AgentInstance::Aionrs(manager) => manager.set_mode(mode).await,
        _ => instance.set_config_option("mode", mode).await.map(|_| ()),
    }
}

fn is_idle_collectable_team_member(task: &AgentInstance, now: TimestampMs, idle_threshold_ms: TimestampMs) -> bool {
    if !matches!(
        task.status(),
        None | Some(ConversationStatus::Pending | ConversationStatus::Finished)
    ) {
        return false;
    }
    now.saturating_sub(task.last_activity_at()) > idle_threshold_ms
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use aionui_ai_agent::types::{BuildTaskOptions, SendMessageData};
    use aionui_ai_agent::{
        ActiveLeaseRegistry, AgentError, AgentInstance, AgentSendError, AgentStreamEvent, IAgentTask, IMockAgent,
        IWorkerTaskManager, IdleCleanupCoordinator,
    };
    use aionui_api_types::{
        AddAgentRequest, ConfigOptionConfirmation, SetConfigOptionRequest, SetConfigOptionResponse,
        TeamContextResetAvailability, TeamContextResetRuntimeStatus, TeamContextResetStatus, TeamRunTargetRole,
    };
    use aionui_common::{AgentKillReason, AgentType, ConversationStatus, TimestampMs, now_ms};
    use aionui_db::{IConversationRepository, ITeamRepository};
    use tokio::sync::broadcast;

    use super::TeamIdleCleanupCoordinator;
    use crate::member_runtime::{MemberRuntimeFailure, ReserveAttach};
    use crate::test_utils::workspace_harness::{
        setup_with_factory_metadata_team_repo_and_conversation_repo,
        setup_with_factory_metadata_team_repo_conversation_repo_and_broadcaster,
        setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager, setup_with_team_repo,
        setup_with_team_repo_and_conversation_repo, single_agent_team_request,
    };
    use crate::types::MailboxMessageType;
    use crate::work_coordinator::{CausalBinding, EnqueueRequest, ReconcileDecision, RuntimeConstraint};
    use crate::work_source::WorkSource;
    use crate::{TeamError, TeamSession};

    struct ModeSettingAgent {
        conversation_id: String,
        agent_type: AgentType,
        mode_result: Mutex<Result<(), String>>,
        event_tx: broadcast::Sender<AgentStreamEvent>,
        status: Option<ConversationStatus>,
        last_activity_at: TimestampMs,
    }

    impl ModeSettingAgent {
        fn accepts_mode(conversation_id: &str) -> Self {
            Self::new(conversation_id, Ok(()))
        }

        fn rejects_mode(conversation_id: &str, message: &str) -> Self {
            Self::new(conversation_id, Err(message.to_owned()))
        }

        fn new(conversation_id: &str, mode_result: Result<(), String>) -> Self {
            let (event_tx, _) = broadcast::channel(1);
            Self {
                conversation_id: conversation_id.to_owned(),
                agent_type: AgentType::Acp,
                mode_result: Mutex::new(mode_result),
                event_tx,
                status: None,
                last_activity_at: now_ms(),
            }
        }

        fn idle_finished(conversation_id: &str) -> Self {
            Self::accepts_mode(conversation_id)
                .with_status(Some(ConversationStatus::Finished))
                .with_last_activity(now_ms() - 600_000)
        }

        fn idle_pending_aionrs(conversation_id: &str) -> Self {
            Self::accepts_mode(conversation_id)
                .with_agent_type(AgentType::Aionrs)
                .with_status(Some(ConversationStatus::Pending))
                .with_last_activity(now_ms() - 600_000)
        }

        fn with_agent_type(mut self, agent_type: AgentType) -> Self {
            self.agent_type = agent_type;
            self
        }

        fn with_status(mut self, status: Option<ConversationStatus>) -> Self {
            self.status = status;
            self
        }

        fn with_last_activity(mut self, last_activity_at: TimestampMs) -> Self {
            self.last_activity_at = last_activity_at;
            self
        }
    }

    #[async_trait::async_trait]
    impl IAgentTask for ModeSettingAgent {
        fn agent_type(&self) -> AgentType {
            self.agent_type
        }

        fn conversation_id(&self) -> &str {
            &self.conversation_id
        }

        fn workspace(&self) -> &str {
            "/tmp/aioncore-team-mode-test"
        }

        fn status(&self) -> Option<ConversationStatus> {
            self.status
        }

        fn last_activity_at(&self) -> TimestampMs {
            self.last_activity_at
        }

        fn subscribe(&self) -> broadcast::Receiver<AgentStreamEvent> {
            self.event_tx.subscribe()
        }

        async fn send_message(&self, _data: SendMessageData) -> Result<(), AgentSendError> {
            Ok(())
        }

        async fn cancel(&self) -> Result<(), AgentError> {
            Ok(())
        }

        fn kill(&self, _reason: Option<AgentKillReason>) -> Result<(), AgentError> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl IMockAgent for ModeSettingAgent {
        async fn set_config_option(&self, option_id: &str, value: &str) -> Result<SetConfigOptionResponse, AgentError> {
            assert_eq!(option_id, "mode");
            assert_eq!(value, "read-only");
            match self.mode_result.lock().unwrap().clone() {
                Ok(()) => Ok(SetConfigOptionResponse {
                    confirmation: ConfigOptionConfirmation::Observed,
                    config_options: None,
                }),
                Err(message) => Err(AgentError::bad_request(message)),
            }
        }
    }

    struct StaticTaskManager {
        tasks: HashMap<String, AgentInstance>,
    }

    impl StaticTaskManager {
        fn new(tasks: HashMap<String, AgentInstance>) -> Self {
            Self { tasks }
        }
    }

    #[async_trait::async_trait]
    impl IWorkerTaskManager for StaticTaskManager {
        fn get_task(&self, conversation_id: &str) -> Option<AgentInstance> {
            self.tasks.get(conversation_id).cloned()
        }

        async fn get_or_build_task(
            &self,
            _conversation_id: &str,
            _options: BuildTaskOptions,
        ) -> Result<AgentInstance, AgentError> {
            Err(AgentError::internal("static task manager does not build tasks"))
        }

        fn kill(&self, _conversation_id: &str, _reason: Option<AgentKillReason>) -> Result<(), AgentError> {
            Ok(())
        }

        fn kill_and_wait(
            &self,
            _conversation_id: &str,
            _reason: Option<AgentKillReason>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
            Box::pin(std::future::ready(()))
        }

        async fn clear(&self) {}

        fn active_count(&self) -> usize {
            self.tasks.len()
        }

        fn collect_idle(&self, _idle_threshold_ms: TimestampMs) -> Vec<String> {
            Vec::new()
        }
    }

    struct MutableTaskManager {
        tasks: Mutex<HashMap<String, AgentInstance>>,
        kills: Mutex<Vec<String>>,
    }

    impl MutableTaskManager {
        fn new() -> Self {
            Self {
                tasks: Mutex::new(HashMap::new()),
                kills: Mutex::new(Vec::new()),
            }
        }

        fn insert_mode_agent(&self, conversation_id: &str) {
            self.tasks.lock().unwrap().insert(
                conversation_id.to_owned(),
                AgentInstance::Mock(Arc::new(ModeSettingAgent::accepts_mode(conversation_id))),
            );
        }

        fn insert_idle_finished_agent(&self, conversation_id: &str) {
            self.tasks.lock().unwrap().insert(
                conversation_id.to_owned(),
                AgentInstance::Mock(Arc::new(ModeSettingAgent::idle_finished(conversation_id))),
            );
        }

        fn insert_idle_pending_aionrs_agent(&self, conversation_id: &str) {
            self.tasks.lock().unwrap().insert(
                conversation_id.to_owned(),
                AgentInstance::Mock(Arc::new(ModeSettingAgent::idle_pending_aionrs(conversation_id))),
            );
        }

        fn remove(&self, conversation_id: &str) {
            self.tasks.lock().unwrap().remove(conversation_id);
        }

        fn reset_kills(&self) {
            self.kills.lock().unwrap().clear();
        }

        fn kills(&self) -> Vec<String> {
            self.kills.lock().unwrap().clone()
        }
    }

    fn two_agent_team_request(name: &str) -> aionui_api_types::CreateTeamRequest {
        aionui_api_types::CreateTeamRequest {
            name: name.into(),
            agents: vec![
                aionui_api_types::TeamAgentInput {
                    name: "Lead".into(),
                    role: "lead".into(),
                    backend: Some("acp".into()),
                    model: "claude".into(),
                    assistant_id: None,
                    conversation_id: None,
                },
                aionui_api_types::TeamAgentInput {
                    name: "Worker".into(),
                    role: "teammate".into(),
                    backend: Some("acp".into()),
                    model: "claude".into(),
                    assistant_id: None,
                    conversation_id: None,
                },
            ],
            workspace: None,
            project_id: None,
        }
    }

    fn team_with_aionrs_worker_request(name: &str) -> aionui_api_types::CreateTeamRequest {
        let mut request = two_agent_team_request(name);
        request.agents.push(aionui_api_types::TeamAgentInput {
            name: "Butler".into(),
            role: "teammate".into(),
            backend: Some("aionrs".into()),
            model: "claude-sonnet".into(),
            assistant_id: None,
            conversation_id: None,
        });
        request
    }

    fn mark_member_runtime_ready(session: &TeamSession, slot_id: &str) {
        let lease = match session.member_runtimes().reserve_attach(slot_id, false) {
            ReserveAttach::Start(lease) => lease,
            other => panic!("ready-state seed must start, got {other:?}"),
        };
        assert!(session.member_runtimes().commit_ready(&lease));
        session
            .work_coordinator()
            .set_runtime_constraint(slot_id, RuntimeConstraint::Ready);
    }

    #[async_trait::async_trait]
    impl IWorkerTaskManager for MutableTaskManager {
        fn get_task(&self, conversation_id: &str) -> Option<AgentInstance> {
            self.tasks.lock().unwrap().get(conversation_id).cloned()
        }

        async fn get_or_build_task(
            &self,
            _conversation_id: &str,
            _options: BuildTaskOptions,
        ) -> Result<AgentInstance, AgentError> {
            Err(AgentError::internal("mutable task manager does not build tasks"))
        }

        fn kill(&self, conversation_id: &str, _reason: Option<AgentKillReason>) -> Result<(), AgentError> {
            self.kills.lock().unwrap().push(conversation_id.to_owned());
            self.remove(conversation_id);
            Ok(())
        }

        fn kill_and_wait(
            &self,
            conversation_id: &str,
            reason: Option<AgentKillReason>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
            let _ = self.kill(conversation_id, reason);
            Box::pin(std::future::ready(()))
        }

        async fn clear(&self) {
            self.tasks.lock().unwrap().clear();
        }

        fn active_count(&self) -> usize {
            self.tasks.lock().unwrap().len()
        }

        fn collect_idle(&self, _idle_threshold_ms: TimestampMs) -> Vec<String> {
            Vec::new()
        }
    }

    #[tokio::test]
    async fn session_has_slow_monitor() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Slow Monitor"))
            .await
            .unwrap();

        svc.ensure_session("user-test", &created.id).await.unwrap();

        assert!(svc.session_has_slow_monitor(&created.id));
        svc.stop_session("user-test", &created.id).await.unwrap();
    }

    #[tokio::test]
    async fn stop_sessions_for_user_keeps_other_user_sessions() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let owned = svc
            .create_team("user-test", single_agent_team_request("Owned Session"))
            .await
            .unwrap();
        let other = svc
            .create_team("user-other", single_agent_team_request("Other Session"))
            .await
            .unwrap();

        svc.ensure_session("user-test", &owned.id).await.unwrap();
        svc.ensure_session("user-other", &other.id).await.unwrap();

        assert_eq!(svc.stop_sessions_for_user("user-test"), 1);
        assert_eq!(svc.session_count_for_test(), 1);
        assert!(!svc.session_has_slow_monitor(&owned.id));
        assert!(svc.session_has_slow_monitor(&other.id));

        svc.stop_session("user-other", &other.id).await.unwrap();
    }

    #[tokio::test]
    async fn ensure_session_emits_agent_runtime_ready_after_member_warmup() {
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_and_broadcaster();
        let created = svc
            .create_team("user-test", single_agent_team_request("Runtime Events"))
            .await
            .unwrap();
        let assistant = created.assistants.first().expect("team assistant");

        svc.ensure_session("user-test", &created.id).await.unwrap();

        let events = broadcaster.events_by_name("team.agentRuntimeStatusChanged");
        let statuses: Vec<&str> = events
            .iter()
            .map(|event| event.data.get("status").and_then(serde_json::Value::as_str).unwrap())
            .collect();

        assert_eq!(statuses, vec!["pending", "ready"]);
        assert_eq!(
            events[0].data.get("team_id").and_then(serde_json::Value::as_str),
            Some(created.id.as_str())
        );
        assert_eq!(
            events[0].data.get("slot_id").and_then(serde_json::Value::as_str),
            Some(assistant.slot_id.as_str())
        );
        assert_eq!(
            events[0]
                .data
                .get("conversation_id")
                .and_then(serde_json::Value::as_str),
            Some(assistant.conversation_id.as_str())
        );
    }

    #[tokio::test]
    async fn ensure_session_repairs_only_missing_member_runtime_in_place() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Runtime Repair"))
            .await
            .unwrap();
        let lead = created.assistants.iter().find(|agent| agent.role == "lead").unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();

        svc.ensure_session("user-test", &created.id).await.unwrap();
        // Leader-only warmup: only the lead runtime exists after first start;
        // the worker stays dormant (spec 5.1), so repair now targets the lead.
        task_manager.insert_mode_agent(&lead.conversation_id);
        task_manager.reset_kills();
        let original_session = Arc::clone(&svc.sessions.get(&created.id).expect("session").session);
        let original_generation = original_session.generation();
        // Simulate the lead runtime disappearing so reconciliation repairs it in place.
        task_manager.remove(&lead.conversation_id);

        svc.ensure_session("user-test", &created.id).await.unwrap();

        let current_session = Arc::clone(&svc.sessions.get(&created.id).expect("session").session);
        assert!(Arc::ptr_eq(&original_session, &current_session));
        assert_eq!(current_session.generation(), original_generation);
        assert_eq!(task_manager.kills(), vec![lead.conversation_id.clone()]);
        assert!(current_session.event_loops().has(&lead.slot_id));
        // The dormant worker is never woken by reconciliation (spec 5.1).
        assert!(!current_session.event_loops().has(&worker.slot_id));

        let events = broadcaster.events_by_name("team.agentRuntimeStatusChanged");
        let lead_statuses: Vec<&str> = events
            .iter()
            .filter(|event| {
                event.data.get("slot_id").and_then(serde_json::Value::as_str) == Some(lead.slot_id.as_str())
            })
            .map(|event| event.data.get("status").and_then(serde_json::Value::as_str).unwrap())
            .collect();
        assert_eq!(lead_statuses, vec!["pending", "ready", "pending", "ready"]);

        let worker_statuses: Vec<&str> = events
            .iter()
            .filter(|event| {
                event.data.get("slot_id").and_then(serde_json::Value::as_str) == Some(worker.slot_id.as_str())
            })
            .map(|event| event.data.get("status").and_then(serde_json::Value::as_str).unwrap())
            .collect();
        assert_eq!(worker_statuses, vec!["dormant"]);
    }

    #[tokio::test]
    async fn restart_agent_runtime_forces_a_ready_member_through_the_attach_chain() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", single_agent_team_request("Runtime Restart Ready"))
            .await
            .unwrap();
        let lead = created.assistants.first().unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        task_manager.insert_mode_agent(&lead.conversation_id);
        task_manager.reset_kills();

        svc.restart_agent_runtime("user-test", &created.id, &lead.slot_id)
            .await
            .unwrap();

        assert_eq!(task_manager.kills(), vec![lead.conversation_id.clone()]);
        let statuses = broadcaster
            .events_by_name("team.agentRuntimeStatusChanged")
            .into_iter()
            .filter(|event| {
                event.data.get("slot_id").and_then(serde_json::Value::as_str) == Some(lead.slot_id.as_str())
            })
            .filter_map(|event| {
                event
                    .data
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .collect::<Vec<_>>();
        assert_eq!(statuses, vec!["pending", "ready", "pending", "ready"]);
    }

    #[tokio::test]
    async fn restart_agent_runtime_rejects_absent_and_failed_members() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Runtime Restart Dormant"))
            .await
            .unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        task_manager.reset_kills();

        let absent_error = svc
            .restart_agent_runtime("user-test", &created.id, &worker.slot_id)
            .await
            .unwrap_err();
        assert!(matches!(
            absent_error,
            TeamError::RuntimeNotReady { conversation_id }
                if conversation_id == worker.conversation_id
        ));
        assert!(task_manager.kills().is_empty());

        let session = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        let failed_lease = match session.member_runtimes().reserve_restart(&worker.slot_id) {
            ReserveAttach::Start(lease) => lease,
            other => panic!("failed state seed must start, got {other:?}"),
        };
        let attaching_error = svc
            .restart_agent_runtime("user-test", &created.id, &worker.slot_id)
            .await
            .unwrap_err();
        assert!(matches!(
            attaching_error,
            TeamError::MemberRuntimeStarting {
                team_id,
                slot_id,
                conversation_id,
            } if team_id == created.id
                && slot_id == worker.slot_id
                && conversation_id == worker.conversation_id
        ));
        assert!(task_manager.kills().is_empty());

        assert!(session.member_runtimes().commit_failed(
            &failed_lease,
            MemberRuntimeFailure {
                classification: "transport",
                public_reason: "Agent runtime failed to start".to_owned(),
            },
        ));
        session.work_coordinator().set_runtime_constraint(
            &worker.slot_id,
            RuntimeConstraint::Failed {
                operation_id: failed_lease.operation_id(),
                classification: "transport",
            },
        );
        task_manager.reset_kills();

        let failed_error = svc
            .restart_agent_runtime("user-test", &created.id, &worker.slot_id)
            .await
            .unwrap_err();
        assert!(matches!(
            failed_error,
            TeamError::RuntimeNotReady { conversation_id }
                if conversation_id == worker.conversation_id
        ));
        assert!(task_manager.kills().is_empty());
    }

    #[tokio::test]
    async fn restart_agent_runtime_does_not_start_an_unpublished_team_session() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", single_agent_team_request("Runtime Restart Starting"))
            .await
            .unwrap();
        let lead = created.assistants.first().unwrap();

        let error = svc
            .restart_agent_runtime("user-test", &created.id, &lead.slot_id)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TeamError::RuntimeNotReady { conversation_id }
                if conversation_id == lead.conversation_id
        ));
        assert!(!svc.sessions.contains_key(&created.id));
        assert!(task_manager.kills().is_empty());
    }

    #[tokio::test]
    async fn restart_agent_runtime_rejects_active_work_without_killing_the_runtime() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", single_agent_team_request("Runtime Restart Busy"))
            .await
            .unwrap();
        let lead = created.assistants.first().unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        task_manager.insert_mode_agent(&lead.conversation_id);
        let session = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        let lease = session
            .work_coordinator()
            .acquire_enqueue(EnqueueRequest {
                slot_id: lead.slot_id.clone(),
                role: TeamRunTargetRole::Lead,
                source: WorkSource::UserMessage,
                binding: CausalBinding::UserVisible,
            })
            .unwrap();
        session
            .work_coordinator()
            .commit_enqueue(&lease, Some("message-1".to_owned()))
            .unwrap();
        assert!(matches!(
            session.work_coordinator().next(&lead.slot_id),
            ReconcileDecision::Claim(_)
        ));
        task_manager.reset_kills();

        let error = svc
            .restart_agent_runtime("user-test", &created.id, &lead.slot_id)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TeamError::MemberBusy {
                team_id,
                slot_id,
                conversation_id,
            } if team_id == created.id
                && slot_id == lead.slot_id
                && conversation_id == lead.conversation_id
        ));
        assert!(task_manager.kills().is_empty());
    }

    #[tokio::test]
    async fn restart_agent_runtime_rejects_a_stopped_member_registry() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", single_agent_team_request("Runtime Restart Stopped"))
            .await
            .unwrap();
        let lead = created.assistants.first().unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        task_manager.insert_mode_agent(&lead.conversation_id);
        let session = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        assert!(session.member_runtimes().stop());
        task_manager.reset_kills();

        let error = svc
            .restart_agent_runtime("user-test", &created.id, &lead.slot_id)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TeamError::RuntimeNotReady { conversation_id }
                if conversation_id == lead.conversation_id
        ));
        assert!(task_manager.kills().is_empty());
    }

    #[tokio::test]
    async fn starting_member_blocks_only_its_config_and_model_updates() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", two_agent_team_request("Starting Config Gate"))
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let lead = created.assistants.iter().find(|agent| agent.role == "lead").unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        let session = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        session
            .work_coordinator()
            .set_runtime_constraint(&lead.slot_id, RuntimeConstraint::Ready);
        session
            .work_coordinator()
            .set_runtime_constraint(&worker.slot_id, RuntimeConstraint::Starting { operation_id: 42 });

        let lead_result = svc
            .set_conversation_config_option(
                "user-test",
                &created.id,
                &lead.conversation_id,
                "model",
                SetConfigOptionRequest {
                    value: "ready-slot-model".to_owned(),
                },
            )
            .await
            .expect("a Ready slot remains configurable");
        assert_eq!(lead_result.confirmation, ConfigOptionConfirmation::Observed);

        let global_mode_error = svc
            .set_conversation_config_option(
                "user-test",
                &created.id,
                &lead.conversation_id,
                "mode",
                SetConfigOptionRequest {
                    value: "full_auto".to_owned(),
                },
            )
            .await
            .expect_err("leader mode must not partially update while another member is Starting");
        assert!(matches!(
            global_mode_error,
            TeamError::MemberRuntimeStarting { ref slot_id, .. } if slot_id == &worker.slot_id
        ));

        let config_error = svc
            .set_conversation_config_option(
                "user-test",
                &created.id,
                &worker.conversation_id,
                "model",
                SetConfigOptionRequest {
                    value: "blocked-model".to_owned(),
                },
            )
            .await
            .expect_err("a Starting slot must reject config changes");
        assert!(matches!(
            config_error,
            TeamError::MemberRuntimeStarting { ref slot_id, .. } if slot_id == &worker.slot_id
        ));

        let model_error = svc
            .update_agent_model("user-test", &created.id, &worker.slot_id, "blocked-model")
            .await
            .expect_err("the model endpoint must share the Starting gate");
        assert!(matches!(
            model_error,
            TeamError::MemberRuntimeStarting { ref slot_id, .. } if slot_id == &worker.slot_id
        ));
    }

    #[tokio::test]
    async fn session_mode_update_is_rejected_before_persistence_when_any_member_is_starting() {
        let (svc, repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", two_agent_team_request("Starting Mode Gate"))
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        let session = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        session
            .work_coordinator()
            .set_runtime_constraint(&worker.slot_id, RuntimeConstraint::Starting { operation_id: 42 });
        let before = repo
            .get_team("user-test", &created.id)
            .await
            .unwrap()
            .expect("team row")
            .session_mode;

        let error = svc
            .set_session_mode("user-test", &created.id, "full_auto")
            .await
            .expect_err("global mode must wait for every member runtime");

        assert!(matches!(
            error,
            TeamError::MemberRuntimeStarting { ref slot_id, .. } if slot_id == &worker.slot_id
        ));
        let after = repo
            .get_team("user-test", &created.id)
            .await
            .unwrap()
            .expect("team row")
            .session_mode;
        assert_eq!(after, before);
    }

    #[tokio::test]
    async fn clear_agent_context_resets_anchor_and_runtime_state_without_membership_noise() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Clear Context"))
            .await
            .unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let session = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        mark_member_runtime_ready(&session, &worker.slot_id);
        assert!(session.scheduler().take_needs_role_prompt(&worker.slot_id).await);
        session
            .mailbox()
            .write(
                &created.id,
                &worker.slot_id,
                "lead",
                MailboxMessageType::Message,
                "stale unread",
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            session
                .mailbox()
                .peek_unread(&created.id, &worker.slot_id)
                .await
                .unwrap()
                .len(),
            1
        );
        svc.set_session_mode("user-test", &created.id, "full_auto")
            .await
            .unwrap();
        let original_mode = conv_repo.get_extra(&worker.conversation_id).unwrap()["session_mode"].clone();
        let original_model = worker.model.clone();
        task_manager.insert_mode_agent(&worker.conversation_id);
        task_manager.reset_kills();
        let spawned_before = broadcaster.events_by_name("team.agentSpawned").len();
        let removed_before = broadcaster.events_by_name("team.agentRemoved").len();
        let notices_before = broadcaster.events_by_name("team.teammateMessage").len();

        let outcome = svc
            .clear_agent_context_in_session("user-test", &created.id, &worker.slot_id)
            .await
            .unwrap();

        assert_eq!(outcome.reset_status, TeamContextResetStatus::Completed);
        assert_eq!(outcome.runtime_status, TeamContextResetRuntimeStatus::Ready);
        assert_eq!(outcome.preserved_unread_count, 1);
        assert_eq!(task_manager.kills(), vec![worker.conversation_id.clone()]);
        let extra = conv_repo.get_extra(&worker.conversation_id).unwrap();
        assert_eq!(extra["mock_acp_session_id"], serde_json::Value::Null);
        assert_eq!(extra["session_mode"], original_mode);
        assert_eq!(
            svc.get_team("user-test", &created.id)
                .await
                .unwrap()
                .assistants
                .into_iter()
                .find(|agent| agent.slot_id == worker.slot_id)
                .unwrap()
                .model,
            original_model
        );
        let unread = session
            .mailbox()
            .peek_unread(&created.id, &worker.slot_id)
            .await
            .unwrap();
        assert_eq!(unread.len(), 1);
        assert_eq!(unread[0].content, "stale unread");
        assert!(session.scheduler().take_needs_role_prompt(&worker.slot_id).await);
        assert_eq!(broadcaster.events_by_name("team.agentSpawned").len(), spawned_before);
        assert_eq!(broadcaster.events_by_name("team.agentRemoved").len(), removed_before);
        let notices = broadcaster.events_by_name("team.teammateMessage");
        assert_eq!(notices.len(), notices_before + 1);
        let notice: aionui_api_types::TeamContextResetNotice = serde_json::from_str(
            notices
                .last()
                .and_then(|event| event.data.get("content"))
                .and_then(serde_json::Value::as_str)
                .expect("semantic reset notice"),
        )
        .unwrap();
        assert_eq!(notice.kind, "context_reset");
        assert_eq!(notice.member_name, worker.name);
        assert_eq!(notice.runtime_status, TeamContextResetRuntimeStatus::Ready);
    }

    #[tokio::test]
    async fn clear_agent_context_rejects_queued_work_without_kill_or_anchor_change() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Clear Busy"))
            .await
            .unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let session = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        mark_member_runtime_ready(&session, &worker.slot_id);
        let lease = session
            .work_coordinator()
            .acquire_enqueue(EnqueueRequest {
                slot_id: worker.slot_id.clone(),
                role: TeamRunTargetRole::Teammate,
                source: WorkSource::UserMessage,
                binding: CausalBinding::UserVisible,
            })
            .unwrap();
        session
            .work_coordinator()
            .commit_enqueue(&lease, Some("queued-message".to_owned()))
            .unwrap();
        task_manager.reset_kills();

        let error = svc
            .clear_agent_context_in_session("user-test", &created.id, &worker.slot_id)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TeamError::ContextResetUnavailable {
                availability: TeamContextResetAvailability::Busy,
                ..
            }
        ));
        assert!(task_manager.kills().is_empty());
        assert_eq!(
            conv_repo.get_extra(&worker.conversation_id).unwrap()["mock_acp_session_id"],
            "anchor"
        );
    }

    #[tokio::test]
    async fn clear_agent_context_rejects_stopped_session_without_starting_or_mutating_it() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Clear Stopped"))
            .await
            .unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();

        let error = svc
            .clear_agent_context("user-test", &created.id, &worker.slot_id)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TeamError::ContextResetUnavailable {
                availability: TeamContextResetAvailability::SessionStopped,
                ..
            }
        ));
        assert!(!svc.sessions.contains_key(&created.id));
        assert!(task_manager.kills().is_empty());
        assert_eq!(
            conv_repo.get_extra(&worker.conversation_id).unwrap()["mock_acp_session_id"],
            "anchor"
        );
    }

    #[tokio::test]
    async fn clear_agent_context_reports_completed_when_fresh_runtime_attach_fails() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Clear Partial"))
            .await
            .unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let session = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        mark_member_runtime_ready(&session, &worker.slot_id);
        task_manager.insert_mode_agent(&worker.conversation_id);
        conv_repo.mark_runtime_attach_failed(&worker.conversation_id);

        let outcome = svc
            .clear_agent_context("user-test", &created.id, &worker.slot_id)
            .await
            .unwrap();

        assert_eq!(outcome.reset_status, TeamContextResetStatus::Completed);
        assert_eq!(outcome.runtime_status, TeamContextResetRuntimeStatus::Failed);
        assert_eq!(
            conv_repo.get_extra(&worker.conversation_id).unwrap()["mock_acp_session_id"],
            serde_json::Value::Null
        );
        let notices = broadcaster.events_by_name("team.teammateMessage");
        let notice: aionui_api_types::TeamContextResetNotice = serde_json::from_str(
            notices
                .last()
                .and_then(|event| event.data.get("content"))
                .and_then(serde_json::Value::as_str)
                .expect("partial-success semantic reset notice"),
        )
        .unwrap();
        assert_eq!(notice.runtime_status, TeamContextResetRuntimeStatus::Failed);
    }

    #[tokio::test]
    async fn clear_agent_context_rejects_wrong_owner_and_aionrs_before_kill() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", team_with_aionrs_worker_request("Clear Unsupported"))
            .await
            .unwrap();
        let butler = created.assistants.iter().find(|agent| agent.name == "Butler").unwrap();
        let lead = created.assistants.iter().find(|agent| agent.role == "lead").unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        task_manager.insert_mode_agent(&lead.conversation_id);
        task_manager.reset_kills();

        let ownership_error = svc
            .clear_agent_context("other-user", &created.id, &butler.slot_id)
            .await
            .unwrap_err();
        assert!(matches!(ownership_error, TeamError::TeamNotFound(_)));
        assert!(task_manager.kills().is_empty());

        let refreshed = svc.get_team("user-test", &created.id).await.unwrap();
        let leader_capability = &refreshed
            .assistants
            .iter()
            .find(|agent| agent.slot_id == lead.slot_id)
            .unwrap()
            .context_reset;
        assert!(!leader_capability.supported);
        assert_eq!(
            leader_capability.availability,
            TeamContextResetAvailability::LeaderNotTargetable
        );
        let unsupported_capability = &refreshed
            .assistants
            .iter()
            .find(|agent| agent.slot_id == butler.slot_id)
            .unwrap()
            .context_reset;
        assert!(!unsupported_capability.supported);
        assert_eq!(
            unsupported_capability.availability,
            TeamContextResetAvailability::Unsupported
        );

        let leader_error = svc
            .clear_agent_context("user-test", &created.id, &lead.slot_id)
            .await
            .unwrap_err();
        assert!(matches!(
            leader_error,
            TeamError::ContextResetLeaderNotTargetable { .. }
        ));
        assert!(task_manager.kills().is_empty());

        let unsupported = svc
            .clear_agent_context("user-test", &created.id, &butler.slot_id)
            .await
            .unwrap_err();
        assert!(matches!(unsupported, TeamError::MemberUnsupported { backend, .. } if backend == "aionrs"));
        assert!(task_manager.kills().is_empty());
    }

    #[tokio::test]
    async fn clear_messages_follow_normal_team_send_paths_without_resetting_context() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, repo, _task_manager, conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let mut request = two_agent_team_request("Clear Slash");
        request.agents[1].name = "My Worker".into();
        let created = svc.create_team("user-test", request).await.unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        let lead = created.assistants.iter().find(|agent| agent.role == "lead").unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        task_manager.insert_mode_agent(&lead.conversation_id);
        task_manager.insert_mode_agent(&worker.conversation_id);
        task_manager.reset_kills();

        let leader_ack = svc
            .send_message("user-test", &created.id, "/clear", None)
            .await
            .unwrap();
        let member_ack = svc
            .send_message_to_agent("user-test", &created.id, &worker.slot_id, "/clear", None)
            .await
            .unwrap();
        let named_ack = svc
            .send_message("user-test", &created.id, " \t/clear\t  My Worker \t", None)
            .await
            .unwrap();

        assert_eq!(leader_ack.run.target_slot_id, lead.slot_id);
        assert_eq!(named_ack.run.target_slot_id, lead.slot_id);
        let worker_messages = repo
            .list_messages_by_ids(std::slice::from_ref(&member_ack.message_id))
            .await
            .unwrap();
        assert!(
            worker_messages
                .iter()
                .any(|message| { message.id == member_ack.message_id && message.content == "/clear" })
        );
        assert_eq!(
            conv_repo.get_extra(&lead.conversation_id).unwrap()["mock_acp_session_id"],
            "anchor"
        );
        assert_eq!(
            conv_repo.get_extra(&worker.conversation_id).unwrap()["mock_acp_session_id"],
            "anchor"
        );
        assert!(broadcaster.events_by_name("team.teammateMessage").is_empty());
    }

    #[tokio::test]
    async fn no_work_reconciliation_rejects_replaced_session() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("No work replacement"))
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let old = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        svc.stop_session("user-test", &created.id).await.unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();

        let result = svc
            .complete_member_runtime_reconciliation(&created.id, &created.id, "user-test", old, Vec::new())
            .await;
        assert!(matches!(result, Err(crate::TeamError::SessionNotFound(_))));
    }

    #[tokio::test]
    async fn replaced_session_cannot_publish_dynamic_runtime_ready() {
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_and_broadcaster();
        let created = svc
            .create_team("user-test", single_agent_team_request("Runtime ready generation fence"))
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let old = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        let agent = old.scheduler().list_agents().await.remove(0);

        svc.stop_session("user-test", &created.id).await.unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let ready_before = broadcaster
            .events_by_name("team.agentRuntimeStatusChanged")
            .into_iter()
            .filter(|event| event.data.get("status").and_then(serde_json::Value::as_str) == Some("ready"))
            .count();

        assert!(!svc.publish_member_runtime_ready_if_current(&old, &agent));
        let ready_after = broadcaster
            .events_by_name("team.agentRuntimeStatusChanged")
            .into_iter()
            .filter(|event| event.data.get("status").and_then(serde_json::Value::as_str) == Some("ready"))
            .count();
        assert_eq!(ready_after, ready_before, "old generations must not publish Ready");
    }

    #[tokio::test]
    async fn stale_cleanup_does_not_kill_replacement_runtime() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", single_agent_team_request("Cleanup fence"))
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let old = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        svc.stop_session("user-test", &created.id).await.unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let conversation_id = created.assistants[0].conversation_id.clone();
        task_manager.insert_mode_agent(&conversation_id);

        svc.cleanup_stale_member_runtime_task(&old, &conversation_id).await;

        assert!(task_manager.get_task(&conversation_id).is_some());
    }

    #[tokio::test]
    async fn idle_cleanup_stops_team_session_and_kills_all_members_when_team_is_collectable() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Idle Cleanup"))
            .await
            .unwrap();
        let lead = created.assistants.iter().find(|agent| agent.role == "lead").unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        task_manager.insert_idle_finished_agent(&lead.conversation_id);
        task_manager.insert_idle_finished_agent(&worker.conversation_id);

        svc.ensure_session("user-test", &created.id).await.unwrap();

        let unhandled = svc
            .cleanup_idle_team_runtime_tasks(vec![lead.conversation_id.clone()], &ActiveLeaseRegistry::new(), 300_000)
            .await;

        assert!(unhandled.is_empty());
        assert_eq!(svc.session_count_for_test(), 0);
        assert_eq!(task_manager.active_count(), 0);
    }

    #[tokio::test]
    async fn idle_cleanup_broadcasts_team_session_stopped() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Idle Cleanup Stopped Broadcast"))
            .await
            .unwrap();
        let lead = created.assistants.iter().find(|agent| agent.role == "lead").unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        task_manager.insert_idle_finished_agent(&lead.conversation_id);
        task_manager.insert_idle_finished_agent(&worker.conversation_id);

        svc.ensure_session("user-test", &created.id).await.unwrap();

        let unhandled = svc
            .cleanup_idle_team_runtime_tasks(vec![lead.conversation_id.clone()], &ActiveLeaseRegistry::new(), 300_000)
            .await;

        assert!(unhandled.is_empty());
        assert_eq!(svc.session_count_for_test(), 0);
        assert_eq!(task_manager.active_count(), 0);

        let stopped_events: Vec<_> = broadcaster
            .events_by_name("team.sessionStatusChanged")
            .into_iter()
            .filter(|event| event.data.get("status").and_then(serde_json::Value::as_str) == Some("stopped"))
            .collect();
        assert_eq!(
            stopped_events.len(),
            1,
            "idle cleanup must broadcast exactly one stopped status"
        );
        assert_eq!(
            stopped_events[0]
                .data
                .get("team_id")
                .and_then(serde_json::Value::as_str),
            Some(created.id.as_str())
        );
    }

    #[tokio::test]
    async fn explicit_stop_session_does_not_broadcast_team_session_stopped() {
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_and_broadcaster();
        let created = svc
            .create_team(
                "user-test",
                single_agent_team_request("Explicit Stop No Stopped Broadcast"),
            )
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();

        svc.stop_session("user-test", &created.id).await.unwrap();

        let stopped_count = broadcaster
            .events_by_name("team.sessionStatusChanged")
            .into_iter()
            .filter(|event| event.data.get("status").and_then(serde_json::Value::as_str) == Some("stopped"))
            .count();
        assert_eq!(stopped_count, 0, "explicit stop must not broadcast a stopped status");
    }

    #[test]
    fn idle_collectable_team_member_accepts_idle_pending_aionrs_runtime() {
        let task = AgentInstance::Mock(Arc::new(ModeSettingAgent::idle_pending_aionrs("aionrs-idle")));

        assert!(super::is_idle_collectable_team_member(&task, now_ms(), 300_000));
    }

    #[test]
    fn idle_collectable_team_member_rejects_running_aionrs_runtime() {
        let task = AgentInstance::Mock(Arc::new(
            ModeSettingAgent::accepts_mode("aionrs-running")
                .with_agent_type(AgentType::Aionrs)
                .with_status(Some(ConversationStatus::Running))
                .with_last_activity(now_ms() - 600_000),
        ));

        assert!(!super::is_idle_collectable_team_member(&task, now_ms(), 300_000));
    }

    #[tokio::test]
    async fn idle_cleanup_stops_team_session_when_aionrs_member_is_idle_pending() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", team_with_aionrs_worker_request("Idle Aionrs Cleanup"))
            .await
            .unwrap();
        let lead = created.assistants.iter().find(|agent| agent.role == "lead").unwrap();
        let acp_worker = created.assistants.iter().find(|agent| agent.name == "Worker").unwrap();
        let aionrs_worker = created.assistants.iter().find(|agent| agent.name == "Butler").unwrap();
        task_manager.insert_idle_finished_agent(&lead.conversation_id);
        task_manager.insert_idle_finished_agent(&acp_worker.conversation_id);
        task_manager.insert_idle_pending_aionrs_agent(&aionrs_worker.conversation_id);

        svc.ensure_session("user-test", &created.id).await.unwrap();

        let unhandled = svc
            .cleanup_idle_team_runtime_tasks(
                vec![lead.conversation_id.clone(), acp_worker.conversation_id.clone()],
                &ActiveLeaseRegistry::new(),
                300_000,
            )
            .await;

        assert!(unhandled.is_empty());
        assert_eq!(svc.session_count_for_test(), 0);
        assert_eq!(task_manager.active_count(), 0);
    }

    #[tokio::test]
    async fn team_idle_cleanup_coordinator_delegates_to_team_service() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Idle Coordinator"))
            .await
            .unwrap();
        for agent in &created.assistants {
            task_manager.insert_idle_finished_agent(&agent.conversation_id);
        }

        svc.ensure_session("user-test", &created.id).await.unwrap();
        let coordinator = TeamIdleCleanupCoordinator::new(svc.clone(), Arc::new(ActiveLeaseRegistry::new()));

        let unhandled = coordinator
            .cleanup_idle_conversations(vec![created.assistants[0].conversation_id.clone()], 300_000)
            .await;

        assert!(unhandled.is_empty());
        assert_eq!(svc.session_count_for_test(), 0);
        assert_eq!(task_manager.active_count(), 0);
    }

    #[tokio::test]
    async fn manual_add_agent_in_active_session_emits_runtime_ready_after_background_attach() {
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_and_broadcaster();
        let created = svc
            .create_team("user-test", single_agent_team_request("Manual Runtime Events"))
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();

        let added = svc
            .add_agent(
                "user-test",
                &created.id,
                AddAgentRequest {
                    name: "Worker".to_owned(),
                    role: "teammate".to_owned(),
                    backend: Some("acp".to_owned()),
                    model: "claude".to_owned(),
                    assistant_id: None,
                },
            )
            .await
            .unwrap();

        let events = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let events = broadcaster.events_by_name("team.agentRuntimeStatusChanged");
                let added_events: Vec<_> = events
                    .into_iter()
                    .filter(|event| {
                        event.data.get("slot_id").and_then(serde_json::Value::as_str) == Some(added.slot_id.as_str())
                    })
                    .collect();
                if added_events
                    .iter()
                    .any(|event| event.data.get("status").and_then(serde_json::Value::as_str) == Some("ready"))
                {
                    break added_events;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("runtime ready event should be emitted");
        let statuses: Vec<&str> = events
            .iter()
            .map(|event| event.data.get("status").and_then(serde_json::Value::as_str).unwrap())
            .collect();

        assert_eq!(statuses, vec!["pending", "ready"]);
        let session = Arc::clone(&svc.sessions.get(&created.id).expect("session").session);
        assert_eq!(session.event_loops().len(), 2, "new slot must own one event loop");
        assert!(session.event_loops().has(&added.slot_id));
        assert_eq!(
            session.member_runtimes().snapshot(&added.slot_id),
            crate::member_runtime::MemberRuntimeSnapshot::Ready
        );
    }

    #[tokio::test]
    async fn dynamic_member_is_reserved_before_membership_event() {
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_and_broadcaster();
        let created = svc
            .create_team("user-test", single_agent_team_request("Reservation ordering"))
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let session = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        let observed = Arc::new(std::sync::Mutex::new(None));
        let observed_for_event = Arc::clone(&observed);
        broadcaster.set_observer(Arc::new(move |event| {
            if event.name != crate::events::TEAM_AGENT_SPAWNED_EVENT {
                return;
            }
            let slot_id = event
                .data
                .get("assistant")
                .and_then(|assistant| assistant.get("slot_id"))
                .and_then(serde_json::Value::as_str);
            if let Some(slot_id) = slot_id {
                *observed_for_event.lock().unwrap() = Some(session.member_runtimes().snapshot(slot_id));
            }
        }));

        svc.add_agent(
            "user-test",
            &created.id,
            AddAgentRequest {
                name: "Worker".to_owned(),
                role: "teammate".to_owned(),
                backend: Some("acp".to_owned()),
                model: "claude".to_owned(),
                assistant_id: None,
            },
        )
        .await
        .unwrap();

        assert!(matches!(
            observed.lock().unwrap().as_ref(),
            Some(
                crate::member_runtime::MemberRuntimeSnapshot::Attaching { .. }
                    | crate::member_runtime::MemberRuntimeSnapshot::Ready
            )
        ));
    }

    #[tokio::test]
    async fn set_session_mode_persists_team_mode_and_new_agents_inherit_it() {
        let (svc, repo, _task_manager, conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Team Mode Seed"))
            .await
            .unwrap();

        svc.set_session_mode("user-test", &created.id, "full_auto")
            .await
            .unwrap();

        let row = repo
            .get_team("user-test", &created.id)
            .await
            .unwrap()
            .expect("team row");
        assert_eq!(row.session_mode.as_deref(), Some("full_auto"));

        let added = svc
            .add_agent(
                "user-test",
                &created.id,
                AddAgentRequest {
                    name: "Worker".to_owned(),
                    role: "teammate".to_owned(),
                    backend: Some("acp".to_owned()),
                    model: "claude".to_owned(),
                    assistant_id: None,
                },
            )
            .await
            .unwrap();
        let extra = conv_repo
            .get_extra(&added.conversation_id)
            .expect("added conversation extra");

        assert_eq!(
            extra.get("session_mode").and_then(serde_json::Value::as_str),
            Some("full_auto")
        );
    }

    #[tokio::test]
    async fn set_session_mode_does_not_persist_agent_seed_when_active_runtime_rejects_mode() {
        let accepting_conversation_id = "conv-accepts";
        let rejecting_conversation_id = "conv-rejects";
        let task_manager = Arc::new(StaticTaskManager::new(HashMap::from([
            (
                accepting_conversation_id.to_owned(),
                AgentInstance::Mock(Arc::new(ModeSettingAgent::accepts_mode(accepting_conversation_id))),
            ),
            (
                rejecting_conversation_id.to_owned(),
                AgentInstance::Mock(Arc::new(ModeSettingAgent::rejects_mode(
                    rejecting_conversation_id,
                    "Value 'read-only' is not selectable for config option 'mode'",
                ))),
            ),
        ])));
        let (svc, repo, _task_manager, conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager);
        let created = svc
            .create_team("user-test", single_agent_team_request("Partial Mode Seed"))
            .await
            .unwrap();
        let mut row = repo
            .get_team("user-test", &created.id)
            .await
            .unwrap()
            .expect("team row");
        row.agents = serde_json::json!([
            {
                "slot_id": "slot-accepts",
                "name": "Codex CLI",
                "role": "lead",
                "conversation_id": accepting_conversation_id,
                "backend": "codex",
                "model": "openai.gpt-5.5",
                "assistant_id": "bare:codex"
            },
            {
                "slot_id": "slot-rejects",
                "name": "Claude Code",
                "role": "teammate",
                "conversation_id": rejecting_conversation_id,
                "backend": "claude",
                "model": "global.anthropic.claude-opus-4-8",
                "assistant_id": "bare:claude"
            }
        ])
        .to_string();
        repo.update_team(
            "user-test",
            &created.id,
            &aionui_db::UpdateTeamParams {
                agents: Some(row.agents),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        conv_repo
            .create(&aionui_db::models::ConversationRow {
                id: accepting_conversation_id.to_owned(),
                user_id: "user-test".to_owned(),
                name: "Codex CLI".to_owned(),
                r#type: AgentType::Acp.serde_name().to_owned(),
                extra: serde_json::json!({
                    "current_mode_id": "default",
                    "session_mode": "default"
                })
                .to_string(),
                model: None,
                status: Some("pending".to_owned()),
                source: None,
                channel_chat_id: None,
                pinned: false,
                pinned_at: None,
                created_at: now_ms(),
                updated_at: now_ms(),
                project_id: None,
                folder_id: None,
                name_source: None,
            })
            .await
            .unwrap();
        conv_repo
            .create(&aionui_db::models::ConversationRow {
                id: rejecting_conversation_id.to_owned(),
                user_id: "user-test".to_owned(),
                name: "Claude Code".to_owned(),
                r#type: AgentType::Acp.serde_name().to_owned(),
                extra: serde_json::json!({
                    "current_mode_id": "default",
                    "session_mode": "default"
                })
                .to_string(),
                model: None,
                status: Some("pending".to_owned()),
                source: None,
                channel_chat_id: None,
                pinned: false,
                pinned_at: None,
                created_at: now_ms(),
                updated_at: now_ms(),
                project_id: None,
                folder_id: None,
                name_source: None,
            })
            .await
            .unwrap();

        svc.set_session_mode("user-test", &created.id, "read-only")
            .await
            .unwrap();

        let team = repo
            .get_team("user-test", &created.id)
            .await
            .unwrap()
            .expect("team row");
        assert_eq!(team.session_mode.as_deref(), Some("read-only"));

        let accepting_extra = conv_repo.get_extra(accepting_conversation_id).unwrap();
        assert_eq!(
            accepting_extra.get("session_mode").and_then(serde_json::Value::as_str),
            Some("read-only")
        );

        let rejecting_extra = conv_repo.get_extra(rejecting_conversation_id).unwrap();
        assert_eq!(
            rejecting_extra.get("session_mode").and_then(serde_json::Value::as_str),
            Some("default")
        );
    }

    #[tokio::test]
    async fn run_state_returns_none_without_session_and_does_not_create_session() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Run State"))
            .await
            .unwrap();
        svc.stop_session("user-test", &created.id).await.unwrap();

        assert_eq!(svc.session_count_for_test(), 0);

        let state = svc.get_run_state("user-test", &created.id).await.unwrap();

        assert!(state.active_run.is_none());
        assert_eq!(svc.session_count_for_test(), 0);
    }

    #[tokio::test]
    async fn config_options_returns_snapshot_without_creating_team_session() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Config Options"))
            .await
            .unwrap();
        let conversation_id = &created.assistants[0].conversation_id;

        assert_eq!(svc.session_count_for_test(), 0);

        let options = svc
            .get_conversation_config_options("user-test", &created.id, conversation_id)
            .await
            .unwrap();

        assert_eq!(options.config_options[0].id, "model");
        assert_eq!(svc.session_count_for_test(), 0);
    }

    #[tokio::test]
    async fn run_state_returns_current_active_payload() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Active Run State"))
            .await
            .unwrap();

        let ack = svc.send_message("user-test", &created.id, "hello", None).await.unwrap();
        let state = svc.get_run_state("user-test", &created.id).await.unwrap();
        let active_run = state.active_run.expect("active run state");

        assert_eq!(active_run.team_id, created.id);
        assert_eq!(active_run.team_run_id, ack.run.team_run_id);
        assert_eq!(active_run.status, ack.run.status);
        assert_eq!(active_run.target_slot_id, ack.run.target_slot_id);
        assert_eq!(active_run.target_role, ack.run.target_role);
        assert_eq!(active_run.queued_intent_count, 1);
        assert_eq!(active_run.slot_work.len(), 1);
        assert_eq!(active_run.slot_work[0].slot_id, ack.run.slot_work[0].slot_id);
    }

    #[tokio::test]
    async fn config_options_return_member_runtime_snapshot() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Team Config"))
            .await
            .unwrap();
        let conversation_id = created.assistants[0].conversation_id.clone();

        let response = svc
            .get_conversation_config_options("user-test", &created.id, &conversation_id)
            .await
            .unwrap();

        let model = response
            .config_options
            .iter()
            .find(|option| option.id == "model")
            .expect("model config option");
        assert_eq!(model.current_value.as_deref(), Some("claude"));
    }

    #[tokio::test]
    async fn config_options_reports_runtime_not_ready_for_member_conversation() {
        let (svc, _repo, _task_manager, conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Team Config Pending"))
            .await
            .unwrap();
        let conversation_id = created.assistants[0].conversation_id.clone();
        conv_repo.mark_runtime_not_ready(&conversation_id);

        let err = svc
            .get_conversation_config_options("user-test", &created.id, &conversation_id)
            .await
            .expect_err("member runtime readiness should be reported distinctly");

        assert!(matches!(
            err,
            crate::error::TeamError::RuntimeNotReady {
                conversation_id: ref id
            } if id == &conversation_id
        ));
    }

    #[tokio::test]
    async fn config_options_reject_non_member_conversation() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Team Config Reject"))
            .await
            .unwrap();

        let err = svc
            .get_conversation_config_options("user-test", &created.id, "other-conversation")
            .await
            .expect_err("non-member conversation must be rejected");

        assert!(matches!(err, crate::error::TeamError::AgentNotFound(_)));
    }

    #[tokio::test]
    async fn config_options_reject_cross_user_access() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Team Config Owner"))
            .await
            .unwrap();
        let conversation_id = created.assistants[0].conversation_id.clone();

        let err = svc
            .get_conversation_config_options("other-user", &created.id, &conversation_id)
            .await
            .expect_err("team config options must reject cross-user access");

        assert!(matches!(err, crate::error::TeamError::TeamNotFound(_)));
    }

    /// The `sessions` runtime map is keyed by the team's ACTIVE engagement, not
    /// team_id: two engagements (distinct projects) of ONE team coexist under
    /// distinct keys, and no entry is ever placed under the raw team id. Uses a
    /// real `SqliteTeamRepository` so `find_or_create_engagement` mints distinct
    /// engagement ids (the mock repos cannot, which is why they fall back to
    /// `team_id`).
    #[tokio::test]
    async fn session_map_is_keyed_by_engagement_not_team() {
        use aionui_db::{SqliteTeamRepository, UpdateTeamParams, init_database_memory};

        let db = init_database_memory().await.unwrap();
        let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
        let svc = setup_with_team_repo(repo.clone());
        let user = "user-test";

        let created = svc
            .create_team(user, single_agent_team_request("Engaged"))
            .await
            .unwrap();
        let team_id = created.id.clone();

        // Bind the team to project A, then start its session.
        repo.update_team(
            user,
            &team_id,
            &UpdateTeamParams {
                project_id: Some("proj-a".to_owned()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        svc.ensure_session(user, &team_id).await.unwrap();
        let eng_a = repo
            .find_or_create_engagement(user, &team_id, "proj-a", &created.workspace)
            .await
            .unwrap()
            .id;

        // Re-bind to project B and start that engagement's session too.
        repo.update_team(
            user,
            &team_id,
            &UpdateTeamParams {
                project_id: Some("proj-b".to_owned()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        svc.ensure_session(user, &team_id).await.unwrap();
        let eng_b = repo
            .find_or_create_engagement(user, &team_id, "proj-b", &created.workspace)
            .await
            .unwrap()
            .id;

        assert_ne!(eng_a, eng_b, "distinct projects mint distinct engagements");
        assert_ne!(eng_a, team_id);
        assert_ne!(eng_b, team_id);
        // Both engagements are representable independently in the session map.
        assert!(
            svc.sessions.contains_key(&eng_a),
            "engagement A session keyed by its id"
        );
        assert!(
            svc.sessions.contains_key(&eng_b),
            "engagement B session keyed by its id"
        );
        // The raw team id is never a key (that is the collision being removed).
        assert!(!svc.sessions.contains_key(&team_id), "no session keyed by team_id");
        assert_eq!(svc.session_count_for_test(), 2, "two engagement sessions coexist");

        svc.stop_sessions_for_user(user);
    }

    /// Task 3b — the `ensure_session` SERVICE boundary is the integration point:
    /// a project-bound team with NO pre-seeded members must have its engagement
    /// members materialized and its scheduler (and the attach/reconcile/broadcast
    /// roster) driven by the engagement's runtime `slot_id`/`conversation_id`,
    /// never the shared `teams.agents` template. A second `ensure_session` runs the
    /// reconcile path and must NOT duplicate the template identity into the merged
    /// scheduler. Real `SqliteTeamRepository` so engagement ids/members are minted.
    #[tokio::test]
    async fn ensure_session_drives_scheduler_from_engagement_members() {
        use aionui_db::{SqliteTeamRepository, UpdateTeamParams, init_database_memory};

        let db = init_database_memory().await.unwrap();
        let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
        let svc = setup_with_team_repo(repo.clone());
        let user = "user-test";

        let created = svc
            .create_team(user, single_agent_team_request("Roster Isolation"))
            .await
            .unwrap();
        let team_id = created.id.clone();
        let template_slot = created.assistants[0].slot_id.clone();
        let template_conv = created.assistants[0].conversation_id.clone();

        // Bind to a project without pre-materializing members.
        repo.update_team(
            user,
            &team_id,
            &UpdateTeamParams {
                project_id: Some("proj-a".to_owned()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        svc.ensure_session(user, &team_id).await.unwrap();

        let engagement = repo
            .find_or_create_engagement(user, &team_id, "proj-a", &created.workspace)
            .await
            .unwrap()
            .id;

        // Materialization happened at the boundary (no members were seeded first).
        let members = repo.list_engagement_members(user, &engagement).await.unwrap();
        assert_eq!(members.len(), 1, "member materialized at ensure_session boundary");
        assert_ne!(
            members[0].slot_id, template_slot,
            "materialized member carries a fresh runtime slot"
        );
        assert_ne!(
            members[0].conversation_id, template_conv,
            "materialized member carries a fresh runtime conversation"
        );

        let session = svc
            .sessions
            .get(&engagement)
            .map(|entry| Arc::clone(&entry.session))
            .expect("session keyed by engagement");
        let roster = session.scheduler().list_agents().await;
        assert_eq!(roster.len(), 1, "single-member roster preserved");
        assert_eq!(
            roster[0].slot_id, members[0].slot_id,
            "scheduler carries the engagement runtime slot"
        );
        assert_eq!(
            roster[0].conversation_id, members[0].conversation_id,
            "scheduler carries the engagement runtime conversation"
        );
        assert_ne!(
            roster[0].slot_id, template_slot,
            "no template identity in the scheduler"
        );

        // Second ensure_session exercises the reconcile path. The template identity
        // must not be (re)injected; the roster stays the single engagement member.
        svc.ensure_session(user, &team_id).await.unwrap();
        let roster2 = session.scheduler().list_agents().await;
        assert_eq!(roster2.len(), 1, "reconcile did not add a template-identity duplicate");
        assert!(
            roster2.iter().all(|a| a.slot_id != template_slot),
            "no template slot leaked into the scheduler after reconcile"
        );

        svc.stop_sessions_for_user(user);
    }

    /// Task 3c (destructive/isolation, most important): a team engaged in two
    /// projects has two distinct member-conversation sets. `remove_agent` against
    /// the ACTIVE engagement (identified by the runtime slot the UI sends) must
    /// destroy ONLY that engagement's member conversation and must never touch a
    /// concurrently-running engagement's member conversation or the template's
    /// original member conversation. Real `SqliteTeamRepository` so engagements and
    /// members are minted per engagement.
    #[tokio::test]
    async fn remove_agent_destroys_only_active_engagement_member_conversation() {
        use aionui_db::{SqliteTeamRepository, UpdateTeamParams, init_database_memory};

        let db = init_database_memory().await.unwrap();
        let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
        let (svc, conv_repo) = setup_with_team_repo_and_conversation_repo(repo.clone());
        let user = "user-test";

        let created = svc
            .create_team(user, two_agent_team_request("Isolation"))
            .await
            .unwrap();
        let team_id = created.id.clone();
        let teammate_template_slot = created.assistants[1].slot_id.clone();
        let teammate_template_conv = created.assistants[1].conversation_id.clone();

        let bind = |project: &'static str| {
            let repo = repo.clone();
            let team_id = team_id.clone();
            async move {
                repo.update_team(
                    "user-test",
                    &team_id,
                    &UpdateTeamParams {
                        project_id: Some(project.to_owned()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            }
        };

        bind("proj-a").await;
        svc.ensure_session(user, &team_id).await.unwrap();
        let eng_a = repo
            .find_or_create_engagement(user, &team_id, "proj-a", &created.workspace)
            .await
            .unwrap()
            .id;
        bind("proj-b").await;
        svc.ensure_session(user, &team_id).await.unwrap();
        let eng_b = repo
            .find_or_create_engagement(user, &team_id, "proj-b", &created.workspace)
            .await
            .unwrap()
            .id;

        let member_a = repo
            .list_engagement_members(user, &eng_a)
            .await
            .unwrap()
            .into_iter()
            .find(|m| m.template_slot == teammate_template_slot)
            .expect("engagement A materialized the teammate");
        let member_b = repo
            .list_engagement_members(user, &eng_b)
            .await
            .unwrap()
            .into_iter()
            .find(|m| m.template_slot == teammate_template_slot)
            .expect("engagement B materialized the teammate");
        assert_ne!(
            member_a.conversation_id, member_b.conversation_id,
            "engagements own distinct member conversations"
        );

        // Make engagement A active again (project re-bound to A), then remove the
        // teammate via its engagement-A RUNTIME slot id.
        bind("proj-a").await;
        svc.ensure_session(user, &team_id).await.unwrap();
        svc.remove_agent(user, &team_id, &member_a.slot_id).await.unwrap();

        assert!(
            conv_repo.get_extra(&member_a.conversation_id).is_none(),
            "active engagement's member conversation is destroyed"
        );
        assert!(
            conv_repo.get_extra(&member_b.conversation_id).is_some(),
            "concurrently-running engagement's member conversation must survive"
        );
        assert!(
            repo.get_engagement_member_by_conversation(&member_b.conversation_id)
                .await
                .unwrap()
                .is_some(),
            "engagement B's member row is untouched"
        );
        assert!(
            conv_repo.get_extra(&teammate_template_conv).is_some(),
            "the template's original member conversation is never the removal target"
        );

        svc.stop_sessions_for_user(user);
    }

    /// Final-review fix (CRITICAL): `remove_team` must tear down the PROJECT-BOUND
    /// runtime — the engagement-member conversations AND the `team_engagements` /
    /// `team_engagement_members` rows — not just the `teams.agents` template. A
    /// legacy/no-project team keeps its old behavior (covered by `d115`).
    ///
    /// RED before the fix: only the template conversation is deleted and no
    /// engagement/member row is removed, so `member.conversation_id` still resolves
    /// and the engagement rows survive the team's deletion (a leak).
    #[tokio::test]
    async fn remove_team_tears_down_engagement_member_runtime_and_rows() {
        use aionui_db::{SqliteTeamRepository, UpdateTeamParams, init_database_memory};

        let db = init_database_memory().await.unwrap();
        let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
        let (svc, conv_repo) = setup_with_team_repo_and_conversation_repo(repo.clone());
        let user = "user-test";

        let created = svc.create_team(user, two_agent_team_request("Teardown")).await.unwrap();
        let team_id = created.id.clone();
        let template_slot = created.assistants[1].slot_id.clone();
        let template_conv = created.assistants[1].conversation_id.clone();

        repo.update_team(
            user,
            &team_id,
            &UpdateTeamParams {
                project_id: Some("proj-a".to_owned()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        svc.ensure_session(user, &team_id).await.unwrap();
        let engagement = repo
            .find_or_create_engagement(user, &team_id, "proj-a", &created.workspace)
            .await
            .unwrap()
            .id;
        let member = repo
            .list_engagement_members(user, &engagement)
            .await
            .unwrap()
            .into_iter()
            .find(|m| m.template_slot == template_slot)
            .expect("engagement materialized the teammate");
        assert_ne!(member.conversation_id, template_conv);
        assert!(
            conv_repo.get_extra(&member.conversation_id).is_some(),
            "engagement-member conversation is live before removal"
        );

        svc.remove_team(user, &team_id).await.unwrap();

        assert!(
            conv_repo.get_extra(&member.conversation_id).is_none(),
            "engagement-member conversation is deleted by remove_team, not just the template"
        );
        assert!(
            repo.list_engagement_members(user, &engagement)
                .await
                .unwrap()
                .is_empty(),
            "member rows are deleted with the team"
        );
        assert!(
            repo.list_engagements(user, &team_id).await.unwrap().is_empty(),
            "engagement rows are deleted with the team"
        );
        assert!(svc.get_team(user, &team_id).await.is_err(), "the team row is gone");

        svc.stop_sessions_for_user(user);
    }

    /// Final-review fix (IMPORTANT): the `set_session_mode` global-mode "another
    /// member is starting" guard must resolve member identity through the LIVE
    /// engagement roster, not the `teams.agents` template — otherwise, for a
    /// project-bound team the refusal silently no-ops (the coordinator is keyed by
    /// engagement slot ids, which the template slot ids never match).
    ///
    /// RED before the fix: the guard scans only template slot ids, misses the
    /// engagement runtime slot carrying `Starting`, and the mode switch proceeds.
    #[tokio::test]
    async fn set_session_mode_refuses_while_engagement_member_runtime_is_starting() {
        use aionui_db::{SqliteTeamRepository, UpdateTeamParams, init_database_memory};

        let db = init_database_memory().await.unwrap();
        let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
        let (svc, _conv_repo) = setup_with_team_repo_and_conversation_repo(repo.clone());
        let user = "user-test";

        let created = svc
            .create_team(user, two_agent_team_request("Mode Guard"))
            .await
            .unwrap();
        let team_id = created.id.clone();
        let template_slot = created.assistants[1].slot_id.clone();

        repo.update_team(
            user,
            &team_id,
            &UpdateTeamParams {
                project_id: Some("proj-a".to_owned()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        svc.ensure_session(user, &team_id).await.unwrap();
        let engagement = repo
            .find_or_create_engagement(user, &team_id, "proj-a", &created.workspace)
            .await
            .unwrap()
            .id;
        let member_slot = repo
            .list_engagement_members(user, &engagement)
            .await
            .unwrap()
            .into_iter()
            .find(|m| m.template_slot == template_slot)
            .expect("engagement materialized the teammate")
            .slot_id;
        assert_ne!(
            member_slot, template_slot,
            "runtime slot is the engagement slot, not the template"
        );

        let session = Arc::clone(
            &svc.sessions
                .get(&engagement)
                .expect("session keyed by engagement")
                .session,
        );
        session
            .work_coordinator()
            .set_runtime_constraint(&member_slot, RuntimeConstraint::Starting { operation_id: 1 });

        let error = svc
            .set_session_mode(user, &team_id, "plan")
            .await
            .expect_err("global-mode switch must refuse while an engagement member runtime is starting");
        assert!(
            matches!(&error, TeamError::MemberRuntimeStarting { slot_id, .. } if slot_id == &member_slot),
            "the refusal must name the engagement runtime slot, got {error:?}"
        );

        svc.stop_sessions_for_user(user);
    }

    /// Task 3c: a rename against the ACTIVE engagement's RUNTIME slot resolves the
    /// engagement member (not the template slot), succeeds, and persists the name
    /// to the template row — while leaving the other engagement's member
    /// conversation intact (rename has no conversation side effect).
    #[tokio::test]
    async fn rename_agent_resolves_runtime_slot_from_active_engagement() {
        use aionui_db::{SqliteTeamRepository, UpdateTeamParams, init_database_memory};

        let db = init_database_memory().await.unwrap();
        let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
        let (svc, conv_repo) = setup_with_team_repo_and_conversation_repo(repo.clone());
        let user = "user-test";

        let created = svc
            .create_team(user, two_agent_team_request("Rename Isolation"))
            .await
            .unwrap();
        let team_id = created.id.clone();
        let teammate_template_slot = created.assistants[1].slot_id.clone();

        for project in ["proj-a", "proj-b"] {
            repo.update_team(
                user,
                &team_id,
                &UpdateTeamParams {
                    project_id: Some(project.to_owned()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            svc.ensure_session(user, &team_id).await.unwrap();
        }
        let eng_b = repo
            .find_or_create_engagement(user, &team_id, "proj-b", &created.workspace)
            .await
            .unwrap()
            .id;
        let member_b = repo
            .list_engagement_members(user, &eng_b)
            .await
            .unwrap()
            .into_iter()
            .find(|m| m.template_slot == teammate_template_slot)
            .expect("B materialized teammate");

        // Re-engage A (active) and rename the teammate via its engagement-A runtime slot.
        repo.update_team(
            user,
            &team_id,
            &UpdateTeamParams {
                project_id: Some("proj-a".to_owned()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        svc.ensure_session(user, &team_id).await.unwrap();
        let eng_a = repo
            .find_or_create_engagement(user, &team_id, "proj-a", &created.workspace)
            .await
            .unwrap()
            .id;
        let member_a = repo
            .list_engagement_members(user, &eng_a)
            .await
            .unwrap()
            .into_iter()
            .find(|m| m.template_slot == teammate_template_slot)
            .expect("A materialized teammate");

        // A runtime slot id is NOT in the template; the old template lookup 404'd.
        assert_ne!(member_a.slot_id, teammate_template_slot);
        svc.rename_agent(user, &team_id, &member_a.slot_id, "Renamed Bot")
            .await
            .expect("rename resolves the engagement runtime slot");

        // Name persists to the template row (the source of truth for names).
        let after = svc.get_team(user, &team_id).await.unwrap();
        let renamed = after
            .assistants
            .iter()
            .find(|a| a.slot_id == teammate_template_slot)
            .expect("template row keyed by template slot survives rename");
        assert_eq!(renamed.name, "Renamed Bot");
        // The other engagement's member conversation is untouched by a rename.
        assert!(
            conv_repo.get_extra(&member_b.conversation_id).is_some(),
            "rename must not delete another engagement's member conversation"
        );

        svc.stop_sessions_for_user(user);
    }

    /// Task 3c (legacy fallback): a no-project team has no materialized members,
    /// so management ops resolve via the `teams.agents` template and behave
    /// exactly as before — the runtime slot IS the template slot.
    #[tokio::test]
    async fn management_ops_fall_back_to_template_for_no_project_team() {
        use aionui_db::{SqliteTeamRepository, init_database_memory};

        let db = init_database_memory().await.unwrap();
        let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
        let (svc, conv_repo) = setup_with_team_repo_and_conversation_repo(repo.clone());
        let user = "user-test";

        let created = svc.create_team(user, two_agent_team_request("Legacy")).await.unwrap();
        let team_id = created.id.clone();
        let teammate_slot = created.assistants[1].slot_id.clone();
        let teammate_conv = created.assistants[1].conversation_id.clone();

        svc.ensure_session(user, &team_id).await.unwrap();
        assert!(
            repo.list_engagement_members(user, &team_id).await.unwrap().is_empty(),
            "no-project team materializes no engagement members"
        );

        // rename via template slot works as before.
        svc.rename_agent(user, &team_id, &teammate_slot, "Old Bot")
            .await
            .unwrap();
        let after = svc.get_team(user, &team_id).await.unwrap();
        assert_eq!(
            after
                .assistants
                .iter()
                .find(|a| a.slot_id == teammate_slot)
                .map(|a| a.name.as_str()),
            Some("Old Bot")
        );

        // remove via template slot tears down the template's own conversation.
        svc.remove_agent(user, &team_id, &teammate_slot).await.unwrap();
        assert!(
            conv_repo.get_extra(&teammate_conv).is_none(),
            "legacy remove deletes the template member conversation"
        );
        let after = svc.get_team(user, &team_id).await.unwrap();
        assert!(after.assistants.iter().all(|a| a.slot_id != teammate_slot));

        svc.stop_sessions_for_user(user);
    }

    /// Task 5 propagation (as far as 3c touches it): `add_agent` binds the new
    /// member to the ACTIVE engagement only and must NOT retro-alter a
    /// concurrently-running engagement. Full template→running-engagement
    /// propagation is Task 5's job; this asserts 3c did not start deleting /
    /// injecting into the wrong engagement. Cross-ref: Task 5 invariant.
    #[tokio::test]
    async fn add_agent_materializes_into_active_engagement_only() {
        use aionui_db::{SqliteTeamRepository, UpdateTeamParams, init_database_memory};

        let db = init_database_memory().await.unwrap();
        let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
        let (svc, _conv_repo) = setup_with_team_repo_and_conversation_repo(repo.clone());
        let user = "user-test";

        let created = svc
            .create_team(user, two_agent_team_request("Propagation"))
            .await
            .unwrap();
        let team_id = created.id.clone();

        // Engage B (running), then A (becomes active).
        for project in ["proj-b", "proj-a"] {
            repo.update_team(
                user,
                &team_id,
                &UpdateTeamParams {
                    project_id: Some(project.to_owned()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            svc.ensure_session(user, &team_id).await.unwrap();
        }
        let eng_a = repo
            .find_or_create_engagement(user, &team_id, "proj-a", &created.workspace)
            .await
            .unwrap()
            .id;
        let eng_b = repo
            .find_or_create_engagement(user, &team_id, "proj-b", &created.workspace)
            .await
            .unwrap()
            .id;
        let b_before = repo.list_engagement_members(user, &eng_b).await.unwrap();

        let added = svc
            .add_agent(
                user,
                &team_id,
                AddAgentRequest {
                    name: "New Bot".into(),
                    role: "teammate".into(),
                    backend: Some("acp".into()),
                    model: "claude".into(),
                    assistant_id: None,
                },
            )
            .await
            .unwrap();

        // Active engagement A gained a member for the new agent; the response carries
        // the runtime slot (engagement's own, not the template's).
        let a_after = repo.list_engagement_members(user, &eng_a).await.unwrap();
        assert!(
            a_after.iter().any(|m| m.slot_id == added.slot_id),
            "new agent materialized into the ACTIVE engagement"
        );
        assert_ne!(added.slot_id, created.assistants[0].slot_id);

        // Concurrently-running engagement B is untouched (no retro-propagation).
        let b_after = repo.list_engagement_members(user, &eng_b).await.unwrap();
        assert_eq!(
            b_after.len(),
            b_before.len(),
            "add_agent must not retro-alter another running engagement"
        );
        assert!(
            b_after.iter().all(|m| m.slot_id != added.slot_id),
            "new member must not leak into engagement B"
        );

        svc.stop_sessions_for_user(user);
    }

    /// Task 3c (finding #2): `update_agent_model` against the ACTIVE engagement's
    /// RUNTIME slot must persist the confirmed model to that engagement member's
    /// OWN conversation, never the shared template's conversation. Uses a real
    /// `SqliteTeamRepository` so the member is minted with a fresh runtime
    /// conversation distinct from the template's, and the fake conversation port to
    /// observe which conversation `persist_confirmed_model` patched. Fails if the
    /// code persisted the template conversation instead.
    #[tokio::test]
    async fn update_agent_model_persists_to_active_engagement_member_conversation() {
        use aionui_db::{SqliteTeamRepository, UpdateTeamParams, init_database_memory};

        let db = init_database_memory().await.unwrap();
        let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
        let (svc, conv_repo) = setup_with_team_repo_and_conversation_repo(repo.clone());
        let user = "user-test";

        let created = svc
            .create_team(user, two_agent_team_request("Model Persist"))
            .await
            .unwrap();
        let team_id = created.id.clone();
        let teammate_template_slot = created.assistants[1].slot_id.clone();
        let teammate_template_conv = created.assistants[1].conversation_id.clone();

        repo.update_team(
            user,
            &team_id,
            &UpdateTeamParams {
                project_id: Some("proj-a".to_owned()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        svc.ensure_session(user, &team_id).await.unwrap();
        let engagement = repo
            .find_or_create_engagement(user, &team_id, "proj-a", &created.workspace)
            .await
            .unwrap()
            .id;
        let member = repo
            .list_engagement_members(user, &engagement)
            .await
            .unwrap()
            .into_iter()
            .find(|m| m.template_slot == teammate_template_slot)
            .expect("engagement A materialized the teammate");
        assert_ne!(
            member.conversation_id, teammate_template_conv,
            "member has its own runtime conversation"
        );

        svc.update_agent_model(user, &team_id, &member.slot_id, "gpt-5.6-sol")
            .await
            .unwrap();

        let member_model = conv_repo.get_extra(&member.conversation_id).unwrap()["current_model_id"].clone();
        assert_eq!(
            member_model,
            serde_json::Value::String("gpt-5.6-sol".into()),
            "confirmed model persisted to the ACTIVE engagement member conversation"
        );
        let template_model = conv_repo.get_extra(&teammate_template_conv).unwrap()["current_model_id"].clone();
        assert_ne!(
            template_model,
            serde_json::Value::String("gpt-5.6-sol".into()),
            "the template conversation must NOT be the persist target"
        );

        svc.stop_sessions_for_user(user);
    }

    /// Task 3c (finding #3): context reset against the ACTIVE engagement's RUNTIME
    /// slot must clear/kills that member's OWN conversation and leave a
    /// concurrently-running engagement's member conversation intact (and never the
    /// template's). Mirrors `remove_agent_destroys_only_active_engagement_member_conversation`;
    /// observable via the fake port's ACP resume anchor (`mock_acp_session_id`).
    #[tokio::test]
    async fn clear_agent_context_resets_only_active_engagement_member_conversation() {
        use aionui_db::{SqliteTeamRepository, UpdateTeamParams, init_database_memory};

        let db = init_database_memory().await.unwrap();
        let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
        let (svc, conv_repo) = setup_with_team_repo_and_conversation_repo(repo.clone());
        let user = "user-test";

        let created = svc
            .create_team(user, two_agent_team_request("Clear Isolation"))
            .await
            .unwrap();
        let team_id = created.id.clone();
        let teammate_template_slot = created.assistants[1].slot_id.clone();
        let teammate_template_conv = created.assistants[1].conversation_id.clone();

        let bind = |project: &'static str| {
            let repo = repo.clone();
            let team_id = team_id.clone();
            async move {
                repo.update_team(
                    user,
                    &team_id,
                    &UpdateTeamParams {
                        project_id: Some(project.to_owned()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            }
        };

        bind("proj-a").await;
        svc.ensure_session(user, &team_id).await.unwrap();
        let eng_a = repo
            .find_or_create_engagement(user, &team_id, "proj-a", &created.workspace)
            .await
            .unwrap()
            .id;
        bind("proj-b").await;
        svc.ensure_session(user, &team_id).await.unwrap();
        let eng_b = repo
            .find_or_create_engagement(user, &team_id, "proj-b", &created.workspace)
            .await
            .unwrap()
            .id;

        let member_of = |engagement: &str| {
            let repo = repo.clone();
            let engagement = engagement.to_owned();
            let template_slot = teammate_template_slot.clone();
            async move {
                repo.list_engagement_members(user, &engagement)
                    .await
                    .unwrap()
                    .into_iter()
                    .find(|m| m.template_slot == template_slot)
                    .expect("engagement materialized the teammate")
            }
        };
        let member_a = member_of(&eng_a).await;
        let member_b = member_of(&eng_b).await;
        assert_ne!(member_a.conversation_id, member_b.conversation_id);

        // Re-engage A (active) and bring its member runtime up to Ready so the
        // reset's restart gate is satisfied, then clear via the engagement-A runtime slot.
        bind("proj-a").await;
        svc.ensure_session(user, &team_id).await.unwrap();
        let session = Arc::clone(&svc.sessions.get(&eng_a).unwrap().session);
        mark_member_runtime_ready(&session, &member_a.slot_id);

        let outcome = svc
            .clear_agent_context(user, &team_id, &member_a.slot_id)
            .await
            .unwrap();
        assert_eq!(outcome.reset_status, TeamContextResetStatus::Completed);

        assert_eq!(
            conv_repo.get_extra(&member_a.conversation_id).unwrap()["mock_acp_session_id"],
            serde_json::Value::Null,
            "ACTIVE engagement member's resume anchor was cleared"
        );
        assert_ne!(
            conv_repo.get_extra(&member_b.conversation_id).unwrap()["mock_acp_session_id"],
            serde_json::Value::Null,
            "concurrently-running engagement's member conversation must survive intact"
        );
        assert_ne!(
            conv_repo.get_extra(&teammate_template_conv).unwrap()["mock_acp_session_id"],
            serde_json::Value::Null,
            "the template's original conversation is never the clear target"
        );

        svc.stop_sessions_for_user(user);
    }

    /// Task 4 — deprecate the single-team-project REBIND. `update_team_project`
    /// must switch the team's ACTIVE engagement by creating/using the new
    /// project's engagement (with its OWN member conversations in that
    /// project's workspace), NOT by mutating the shared template member
    /// conversations or a previously-active engagement's members. Real
    /// `SqliteTeamRepository` (engagement ids/members) + real
    /// `SqliteProjectStore`/`ProjectService` (project→workspace resolution).
    #[tokio::test]
    async fn update_team_project_switches_engagement_without_rebinding_shared_members() {
        use aionui_db::{IProjectStore, SqliteProjectStore, SqliteTeamRepository, init_database_memory};

        let db = init_database_memory().await.unwrap();
        let pool = db.pool().clone();
        let repo = Arc::new(SqliteTeamRepository::new(pool.clone()));

        // Project tables carry a users(id) FK; seed the owner as in production.
        sqlx::query(
            "INSERT INTO users (id, user_type, username, password_hash, status, session_generation, created_at, updated_at) \
             VALUES ('user-switch', 'local', 'user-switch', 'hash', 'active', 0, 1, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let (svc, conv_repo) = setup_with_team_repo_and_conversation_repo(repo.clone());
        let user = "user-switch";

        // Create the team BEFORE injecting the project service so the create-time
        // bind side-branch is a no-op and the team starts unbound.
        let created = svc
            .create_team(user, single_agent_team_request("Switcher"))
            .await
            .unwrap();
        let team_id = created.id.clone();
        let template_conv = created.assistants[0].conversation_id.clone();
        let template_workspace_before = conv_repo
            .get_extra(&template_conv)
            .and_then(|e| {
                e.get("workspace")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .expect("template conversation has a workspace");

        let project_store: Arc<dyn IProjectStore> = Arc::new(SqliteProjectStore::new(pool.clone()));
        let project_service = Arc::new(aionui_project::ProjectService::new(
            project_store,
            std::env::temp_dir().join(format!("aionui-team-switch-root-{}", aionui_common::generate_id())),
        ));
        svc.with_project_service(project_service.clone());

        let dir_a = std::env::temp_dir().join(format!("aionui-team-switch-a-{}", aionui_common::generate_id()));
        let dir_b = std::env::temp_dir().join(format!("aionui-team-switch-b-{}", aionui_common::generate_id()));
        std::fs::create_dir_all(&dir_a).unwrap();
        std::fs::create_dir_all(&dir_b).unwrap();
        let project_a = project_service
            .create_standard(user, aionui_project::canonical::to_file_uri(&dir_a).unwrap())
            .await
            .unwrap()
            .project
            .project_id;
        let project_b = project_service
            .create_standard(user, aionui_project::canonical::to_file_uri(&dir_b).unwrap())
            .await
            .unwrap()
            .project
            .project_id;

        // 1) Switch the team to project A through the real entry point.
        svc.update_team_project(user, &team_id, &project_a).await.unwrap();
        let eng_a = repo
            .find_or_create_engagement(user, &team_id, &project_a, &dir_a.to_string_lossy())
            .await
            .unwrap()
            .id;
        let members_a = repo.list_engagement_members(user, &eng_a).await.unwrap();
        assert_eq!(members_a.len(), 1, "switching to A materializes A's engagement members");
        assert_ne!(
            members_a[0].conversation_id, template_conv,
            "engagement member is a fresh conversation, not the shared template"
        );
        assert_eq!(
            conv_repo.get_extra(&members_a[0].conversation_id).unwrap()["workspace"],
            dir_a.to_string_lossy().as_ref(),
            "A's member conversation is bound to A's workspace"
        );
        let a_conv = members_a[0].conversation_id.clone();
        let a_workspace_before = conv_repo.get_extra(&a_conv).unwrap()["workspace"]
            .as_str()
            .unwrap()
            .to_owned();

        // 2) Switch the team to project B.
        svc.update_team_project(user, &team_id, &project_b).await.unwrap();
        let eng_b = repo
            .find_or_create_engagement(user, &team_id, &project_b, &dir_b.to_string_lossy())
            .await
            .unwrap()
            .id;
        let members_b = repo.list_engagement_members(user, &eng_b).await.unwrap();
        assert_eq!(members_b.len(), 1, "switching to B materializes B's engagement members");
        let b_conv = members_b[0].conversation_id.clone();
        assert_ne!(b_conv, a_conv, "B's engagement uses its OWN member conversation id");
        assert_ne!(
            b_conv, template_conv,
            "B's member is not the shared template conversation"
        );
        assert_eq!(
            conv_repo.get_extra(&b_conv).unwrap()["workspace"],
            dir_b.to_string_lossy().as_ref(),
            "B's member conversation is bound to B's workspace"
        );

        // Task 5 invariant: switching projects must NOT retroactively mutate the
        // previously-active engagement's members/conversations.
        let members_a_after = repo.list_engagement_members(user, &eng_a).await.unwrap();
        assert_eq!(members_a_after.len(), 1, "A still has exactly its own member");
        assert_eq!(
            members_a_after[0].conversation_id, a_conv,
            "A's member conversation id is unchanged by the switch to B"
        );
        assert_eq!(
            conv_repo.get_extra(&a_conv).unwrap()["workspace"],
            a_workspace_before.as_str(),
            "A's member conversation workspace was NOT rebound to B"
        );

        // The deprecated rebind mutated the shared template member conversation;
        // the per-engagement switch must leave it untouched.
        assert_eq!(
            conv_repo.get_extra(&template_conv).and_then(|e| e
                .get("workspace")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)),
            Some(template_workspace_before),
            "the shared template conversation workspace must not be rebound by a project switch"
        );

        svc.stop_sessions_for_user(user);
    }
}
