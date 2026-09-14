//! Cross-agent delegation service.
//!
//! Phase 2 implementation. Phase 1 stub is in `service_v0.rs` (renamed to
//! `_phase1_stub.rs`) — this file supersedes it. Service-level responsibilities:
//!
//! - resolve a target by name or assistant_id (`resolve_target`, `list_targets`);
//! - dispatch (async) — create or reuse a target conversation, deliver the
//!   task, persist an envelope row;
//! - ask (sync) — same, plus park the caller's turn, poll for the reply,
//!   resume the caller's turn.
//!
//! Cycle and depth checks live here. The conversation runtime calls into this
//! service for `complete_sync` when a delegated turn finishes.

use std::sync::Arc;

use aionui_ai_agent::task_manager::IWorkerTaskManager;
use aionui_api_types::{
    AssistantConversationRequest, ChatFileRef, CreateConversationRequest, DelegateAskRequest, DelegateAskResponse,
    DelegateDeliveryStatus, DelegateDispatchRequest, DelegateDispatchResponse, DelegateEnvelopeBlock,
    DelegateEnvelopeKind, DelegateTarget, DelegateTargetKind, DelegateTargetsQuery, DelegateTargetsResponse,
    ResolvedDelegateTarget, SendMessageRequest,
};
use aionui_common::{AgentType, ConversationSource, now_ms};
use aionui_conversation::service::ConversationService;
use aionui_db::{IConversationRepository, ISettingsRepository};
use aionui_realtime::EventBroadcaster;
use sqlx::Row;
use tracing::{info, warn};
use uuid::Uuid;

use crate::bridge::{BridgeError, ConveneRootTask, TeamEngagementBridge};
use crate::error::DelegateError;
use crate::queue::DelegateQueue;
use crate::rate_limit::{DelegateRateLimiter, RateVerdict};
use crate::turn_suspend::{SuspendedTurn, TurnSuspendRegistry};
use crate::{DEFAULT_SYNC_TIMEOUT_SECONDS, MAX_DEPTH, QUEUE_TTL_MS};

/// Live (non-expired) envelopes on a delegation chain, used to reject a
/// repeat dispatch to the same target (`cycle_detected`). `expired` rows are
/// ignored both by status and by `expires_at`, so a completed/expired
/// dispatch never blocks a legitimate follow-up.
const LIVE_CHAIN_SQL: &str = "SELECT target_assistant_id FROM delegation_envelopes \
    WHERE root_conversation_id = ?1 AND expires_at > ?2 \
    AND status IN ('pending','delivered','replied','sync_pending','sync_replied')";

/// Deepest live chain depth for a root conversation (sync `ask` path).
const CHAIN_DEPTH_SQL: &str =
    "SELECT MAX(depth) AS max_depth FROM delegation_envelopes WHERE root_conversation_id = ?1 AND expires_at > ?2";

/// The caller's own existing delegated room for an exact target, matched on
/// structured `extra` fields (not a greedy substring `LIKE`) so a room created
/// for a different conversation is never hijacked.
const REUSE_ROOM_SQL: &str = "SELECT id FROM conversations \
    WHERE user_id = ?1 AND archived_at IS NULL AND json_valid(extra) \
    AND json_extract(extra, '$.preset_assistant_id') = ?2 \
    AND json_extract(extra, '$.delegated_from') = ?3 \
    ORDER BY updated_at DESC LIMIT 1";

/// Maps a delegation target (assistant-definition id, what `resolve_target`
/// returns) to the `assistant_id` the conversation-creation path resolves
/// (`get_by_assistant_id_for_user` matches this column, not the definition id).
/// Without this translation the delegated room is created with no
/// `backend`/`agent_id` binding and the ACP factory rejects it on first send.
const ASSISTANT_ID_FOR_DEFINITION_SQL: &str =
    "SELECT assistant_id FROM assistant_definitions WHERE user_id = ?1 AND id = ?2 LIMIT 1";

/// The caller conversation's workspace, so a delegated room inherits the
/// project context (create-time `bind_project_best_effort` maps the folder back
/// to its project). Without this the room gets an empty temp workspace and the
/// lead re-scans the filesystem instead of working the project.
const CALLER_WORKSPACE_SQL: &str = "SELECT json_extract(extra, '$.workspace') AS ws FROM conversations WHERE id = ?1";

pub struct DelegateService {
    pub queue: Arc<DelegateQueue>,
    pub rate_limiter: Arc<DelegateRateLimiter>,
    pub suspend: Arc<TurnSuspendRegistry>,
    pub conversation_service: ConversationService,
    pub(crate) conversation_repo: Arc<dyn IConversationRepository>,
    pub(crate) settings_repo: Arc<dyn ISettingsRepository>,
    #[allow(dead_code)]
    pub(crate) broadcaster: Arc<dyn EventBroadcaster>,
    pub(crate) task_manager: Arc<dyn IWorkerTaskManager>,
    /// Team-engagement bridge port (Phase 4a): `dispatch` routes team targets
    /// through it (see [`Self::dispatch_to_team`]).
    pub(crate) team_bridge: Arc<dyn TeamEngagementBridge>,
}

impl DelegateService {
    pub fn new(
        conversation_service: ConversationService,
        conversation_repo: Arc<dyn IConversationRepository>,
        settings_repo: Arc<dyn ISettingsRepository>,
        broadcaster: Arc<dyn EventBroadcaster>,
        task_manager: Arc<dyn IWorkerTaskManager>,
        team_bridge: Arc<dyn TeamEngagementBridge>,
    ) -> Self {
        use crate::queue::{DelegateQueue, SystemClock};
        use crate::rate_limit::DelegateRateLimiter;
        let clock: Arc<dyn crate::queue::Clock> = Arc::new(SystemClock);
        Self {
            queue: Arc::new(DelegateQueue::new(clock.clone())),
            rate_limiter: Arc::new(DelegateRateLimiter::new(clock)),
            suspend: Arc::new(TurnSuspendRegistry::new()),
            conversation_service,
            conversation_repo,
            settings_repo,
            broadcaster,
            task_manager,
            team_bridge,
        }
    }

    // -----------------------------------------------------------------------
    // Feature toggle
    // -----------------------------------------------------------------------

    pub async fn is_enabled_for(&self, user_id: &str) -> bool {
        match self.settings_repo.get_settings(user_id).await {
            Ok(Some(settings)) => settings.cross_session_message_enabled,
            Ok(None) => true,
            Err(error) => {
                warn!(
                    user_id,
                    error = %error,
                    "delegate toggle lookup failed; treating the feature as enabled"
                );
                true
            }
        }
    }

    // -----------------------------------------------------------------------
    // Name resolution
    // -----------------------------------------------------------------------

    pub async fn resolve_target(&self, user_id: &str, query: &str) -> Result<ResolvedDelegateTarget, DelegateError> {
        let q = query.trim();
        if q.is_empty() {
            return Err(DelegateError::SchemaValidation {
                reason: "`to` must not be empty".to_owned(),
            });
        }
        let sql = "SELECT id, name FROM assistant_definitions WHERE user_id = ?1 AND allow_delegation = 1";
        let rows = self
            .conversation_repo
            .raw_query(sql, vec![user_id.to_owned()])
            .await
            .map_err(|e| DelegateError::TransportUnavailable { reason: e.to_string() })?;

        let mut exact: Vec<ResolvedDelegateTarget> = Vec::new();
        let mut prefix: Vec<ResolvedDelegateTarget> = Vec::new();
        for row in rows {
            let id = row.try_get::<String, _>("id").unwrap_or_default();
            let name = row.try_get::<String, _>("name").unwrap_or_default();
            if id == q {
                // Direct assistant_id hit wins over name match.
                return Ok(ResolvedDelegateTarget {
                    kind: DelegateTargetKind::Assistant,
                    id,
                    name,
                });
            }
            if name == q {
                exact.push(ResolvedDelegateTarget {
                    kind: DelegateTargetKind::Assistant,
                    id,
                    name,
                });
            } else if name.to_lowercase().starts_with(&q.to_lowercase()) {
                prefix.push(ResolvedDelegateTarget {
                    kind: DelegateTargetKind::Assistant,
                    id,
                    name,
                });
            }
        }

        // Team targets: same exact/case-insensitive-prefix matching, appended to
        // the assistant candidate lists so the (exact, prefix) match arms below
        // behave identically for assistant-only users (teams add zero rows) and
        // resolve a team name to `kind=Team`.
        let team_sql = "SELECT id, name FROM teams WHERE user_id = ?1 AND archived_at IS NULL";
        let team_rows = self
            .conversation_repo
            .raw_query(team_sql, vec![user_id.to_owned()])
            .await
            .map_err(|e| DelegateError::TransportUnavailable { reason: e.to_string() })?;
        for row in team_rows {
            let id = row.try_get::<String, _>("id").unwrap_or_default();
            let name = row.try_get::<String, _>("name").unwrap_or_default();
            if name == q {
                exact.push(ResolvedDelegateTarget {
                    kind: DelegateTargetKind::Team,
                    id,
                    name,
                });
            } else if name.to_lowercase().starts_with(&q.to_lowercase()) {
                prefix.push(ResolvedDelegateTarget {
                    kind: DelegateTargetKind::Team,
                    id,
                    name,
                });
            }
        }

        // Deterministic tie-break for an exact-name collision between an
        // assistant and a team: the ASSISTANT wins, because until Phase 4a
        // Task 3 a team target is not dispatchable, so an assistant that
        // resolved this name before must keep resolving (non-breaking superset).
        // Genuine same-kind ambiguity is preserved (two exact teams, or the
        // prefix arms below, still error as before). Assistant-only users see
        // `exact` filtered to a no-op (all already Assistant).
        if exact.iter().any(|t| t.kind == DelegateTargetKind::Assistant) {
            exact.retain(|t| t.kind == DelegateTargetKind::Assistant);
        }

        match (exact.len(), prefix.len()) {
            (0, 0) => Err(DelegateError::TargetNotFound { query: q.to_owned() }),
            (1, _) => Ok(exact.into_iter().next().unwrap()),
            (_, 1) => Ok(prefix.into_iter().next().unwrap()),
            (_, n) if n > 1 => Err(DelegateError::AmbiguousTarget {
                query: q.to_owned(),
                candidates: prefix.into_iter().map(|t| t.name).collect(),
            }),
            _ => Err(DelegateError::TargetNotFound { query: q.to_owned() }),
        }
    }

    /// Sync-ask-only guard: teams are dispatchable via the bridge (Task 3) but
    /// a *sync* ask against a team is not wired (result return is 4b), so keep
    /// reporting it as not found for `ask`.
    async fn assistant_target(&self, user_id: &str, query: &str) -> Result<String, DelegateError> {
        let resolved = self.resolve_target(user_id, query).await?;
        if resolved.kind != DelegateTargetKind::Assistant {
            return Err(DelegateError::TargetNotFound {
                query: query.trim().to_owned(),
            });
        }
        Ok(resolved.id)
    }

    pub async fn list_targets(
        &self,
        user_id: &str,
        query: DelegateTargetsQuery,
    ) -> Result<DelegateTargetsResponse, DelegateError> {
        // LIMIT is applied per source (assistant definitions, then teams); the
        // merged list can therefore exceed `limit`. Assistants are emitted first
        // so an assistant-only user's roster is byte-identical to the pre-4a
        // single-source result (the teams query adds zero rows).
        let limit = query.limit.unwrap_or(50).min(500);
        let sql = "SELECT id, name, description, agent_id FROM assistant_definitions WHERE user_id = ?1 AND allow_delegation = 1 ORDER BY name LIMIT ?2";
        let rows = self
            .conversation_repo
            .raw_query(sql, vec![user_id.to_owned(), limit.to_string()])
            .await
            .map_err(|e| DelegateError::TransportUnavailable { reason: e.to_string() })?;
        let mut items: Vec<DelegateTarget> = rows
            .into_iter()
            .filter_map(|r| {
                let assistant_id = r.try_get::<String, _>("id").ok()?;
                let name = r.try_get::<String, _>("name").ok()?;
                let backend = r.try_get::<String, _>("agent_id").unwrap_or_default();
                let description = r.try_get::<Option<String>, _>("description").ok().flatten();
                Some(DelegateTarget {
                    assistant_id,
                    name,
                    backend,
                    description,
                    kind: DelegateTargetKind::Assistant,
                    team_id: None,
                })
            })
            .filter(|t| {
                query
                    .q
                    .as_ref()
                    .map(|q| t.name.to_lowercase().contains(&q.to_lowercase()))
                    .unwrap_or(true)
            })
            .collect();

        let team_sql = "SELECT id, name FROM teams WHERE user_id = ?1 AND archived_at IS NULL ORDER BY name LIMIT ?2";
        let team_rows = self
            .conversation_repo
            .raw_query(team_sql, vec![user_id.to_owned(), limit.to_string()])
            .await
            .map_err(|e| DelegateError::TransportUnavailable { reason: e.to_string() })?;
        let teams: Vec<DelegateTarget> = team_rows
            .into_iter()
            .filter_map(|r| {
                let id = r.try_get::<String, _>("id").ok()?;
                let name = r.try_get::<String, _>("name").ok()?;
                Some(DelegateTarget {
                    // The team id doubles as `assistant_id` so existing consumers
                    // that read a single id field stay usable; `kind=Team` +
                    // `team_id` disambiguate.
                    assistant_id: id.clone(),
                    name,
                    backend: String::new(),
                    description: None,
                    kind: DelegateTargetKind::Team,
                    team_id: Some(id),
                })
            })
            .filter(|t| {
                query
                    .q
                    .as_ref()
                    .map(|q| t.name.to_lowercase().contains(&q.to_lowercase()))
                    .unwrap_or(true)
            })
            .collect();
        items.extend(teams);

        Ok(DelegateTargetsResponse { items })
    }

    // -----------------------------------------------------------------------
    // Async dispatch
    // -----------------------------------------------------------------------

    pub async fn dispatch(
        &self,
        user_id: &str,
        from_conversation_id: &str,
        from_agent_name: &str,
        req: DelegateDispatchRequest,
    ) -> Result<DelegateDispatchResponse, DelegateError> {
        if !self.is_enabled_for(user_id).await {
            return Err(DelegateError::FeatureDisabled);
        }

        let sender_row = self
            .conversation_repo
            .get(user_id, from_conversation_id)
            .await
            .map_err(|e| DelegateError::TransportUnavailable { reason: e.to_string() })?
            .ok_or_else(|| DelegateError::TransportUnavailable {
                reason: format!("sender conversation {from_conversation_id} not found"),
            })?;
        if team_id_from_extra(&sender_row.extra).is_some() {
            return Err(DelegateError::SenderIsTeam {
                id: from_conversation_id.to_owned(),
            });
        }
        if req.message.trim().is_empty() {
            return Err(DelegateError::SchemaValidation {
                reason: "`message` must not be empty".to_owned(),
            });
        }

        let resolved = self.resolve_target(user_id, &req.to).await?;
        // Phase 4a: a team target convenes the engagement through the bridge
        // port; the assistant delivery path below stays byte-identical.
        if resolved.kind == DelegateTargetKind::Team {
            return self
                .dispatch_to_team(
                    user_id,
                    from_conversation_id,
                    from_agent_name,
                    &sender_row,
                    &resolved,
                    req,
                )
                .await;
        }
        let target_assistant_id = resolved.id;
        if !self
            .is_delegation_enabled_for_assistant(user_id, &target_assistant_id)
            .await?
        {
            return Err(DelegateError::DelegationDisabledForTarget {
                assistant_id: target_assistant_id.clone(),
            });
        }

        if let RateVerdict::Tripped { .. } = self
            .rate_limiter
            .check_and_record(from_conversation_id, &target_assistant_id)
        {
            return Err(DelegateError::RateLimited {
                from: from_conversation_id.to_owned(),
                to: target_assistant_id.clone(),
            });
        }

        let reply_to = req.reply_to.clone().unwrap_or_else(|| from_conversation_id.to_owned());
        if reply_to != from_conversation_id {
            let reply_row = self
                .conversation_repo
                .get(user_id, &reply_to)
                .await
                .map_err(|e| DelegateError::TransportUnavailable { reason: e.to_string() })?
                .ok_or_else(|| DelegateError::ReplyTargetNotOwned { id: reply_to.clone() })?;
            if team_id_from_extra(&reply_row.extra).is_some() {
                return Err(DelegateError::ReplyTargetNotOwned { id: reply_to.clone() });
            }
        }

        let depth = req.depth.unwrap_or(0);
        if depth > MAX_DEPTH {
            return Err(DelegateError::DepthExceeded { depth, max: MAX_DEPTH });
        }
        if self
            .find_envelope_on_chain(from_conversation_id, &target_assistant_id)
            .await?
            .is_some()
        {
            return Err(DelegateError::CycleDetected {
                root: from_conversation_id.to_owned(),
                target: target_assistant_id.clone(),
            });
        }

        let (target_conversation_id, created) = self
            .ensure_target_conversation(user_id, &target_assistant_id, from_conversation_id)
            .await?;
        if target_conversation_id == from_conversation_id {
            return Err(DelegateError::TargetIsSelf {
                id: target_conversation_id.clone(),
            });
        }

        let envelope_id = Uuid::now_v7().to_string();
        let from_agent_id =
            extract_assistant_id_from_extra_value(&serde_json::from_str(&sender_row.extra).unwrap_or_default())
                .unwrap_or_default();
        let block = DelegateEnvelopeBlock {
            kind: DelegateEnvelopeKind::Dispatch,
            from_agent_id,
            from_agent_name: from_agent_name.to_owned(),
            reply_to: Some(reply_to.clone()),
            depth,
            envelope_id: envelope_id.clone(),
            workspace: "unknown (differs from yours)".to_owned(),
            created_at_ms: now_ms(),
        };
        let composed = compose_delivery_body(&block, &req.message);

        let deliver_result = self
            .deliver_now(user_id, &target_conversation_id, composed.clone(), req.files.clone())
            .await;

        match deliver_result {
            Ok(_) => {
                self.persist_envelope(
                    &envelope_id,
                    user_id,
                    from_conversation_id,
                    &target_conversation_id,
                    &target_assistant_id,
                    depth,
                    "delivered",
                )
                .await?;
                info!(
                    envelope_id,
                    from = from_conversation_id,
                    to = %target_conversation_id,
                    depth,
                    "delegate dispatch delivered"
                );
                Ok(DelegateDispatchResponse {
                    status: if created {
                        DelegateDeliveryStatus::CreatedAndDelivered
                    } else {
                        DelegateDeliveryStatus::Delivered
                    },
                    to_conversation_id: target_conversation_id,
                    to_assistant_id: target_assistant_id,
                    envelope_id,
                    depth,
                    engagement_id: None,
                    root_task_id: None,
                })
            }
            Err(transient_reason) => {
                self.queue.push(crate::queue::PendingDelegate {
                    envelope_id: envelope_id.clone(),
                    to_conversation_id: target_conversation_id.clone(),
                    to_assistant_id: target_assistant_id.clone(),
                    user_id: user_id.to_owned(),
                    from_conversation_id: from_conversation_id.to_owned(),
                    message: composed,
                    depth,
                    expires_at_ms: now_ms() + QUEUE_TTL_MS,
                })?;
                self.persist_envelope(
                    &envelope_id,
                    user_id,
                    from_conversation_id,
                    &target_conversation_id,
                    &target_assistant_id,
                    depth,
                    "pending",
                )
                .await?;
                warn!(
                    envelope_id,
                    transient_reason, "delegate dispatch queued; target not ready"
                );
                Ok(DelegateDispatchResponse {
                    status: DelegateDeliveryStatus::Queued,
                    to_conversation_id: target_conversation_id,
                    to_assistant_id: target_assistant_id,
                    envelope_id,
                    depth,
                    engagement_id: None,
                    root_task_id: None,
                })
            }
        }
    }

    /// Team-target branch of [`Self::dispatch`] (Phase 4a): find-or-create the
    /// team's engagement for the caller's project and create the root task via
    /// the bridge port instead of delivering into an assistant conversation.
    ///
    /// Guards kept from the assistant path: feature toggle + sender checks
    /// (already applied by the caller), rate limit, depth. Ownership is
    /// enforced by the team seam (spec §5). Cycle/depth re-keying to the
    /// engagement is Phase 4b; `ponytail: 4a guards stay conversation-keyed`.
    ///
    /// Project resolution (spec §5 step 1): the sender conversation's
    /// `project_id`; when the caller has no project, the sentinel
    /// [`aionui_common::constants::TEAM_NO_PROJECT_SENTINEL`] is passed so the
    /// team's backfilled default engagement (migrations 045/047) is
    /// convened/reused — §10's hard "error without a project" is deferred to
    /// the Phase 5 project selector.
    async fn dispatch_to_team(
        &self,
        user_id: &str,
        from_conversation_id: &str,
        from_agent_name: &str,
        sender_row: &aionui_db::models::ConversationRow,
        target: &ResolvedDelegateTarget,
        req: DelegateDispatchRequest,
    ) -> Result<DelegateDispatchResponse, DelegateError> {
        let team_id = &target.id;
        if let RateVerdict::Tripped { .. } = self.rate_limiter.check_and_record(from_conversation_id, team_id) {
            return Err(DelegateError::RateLimited {
                from: from_conversation_id.to_owned(),
                to: team_id.clone(),
            });
        }

        let reply_to = req.reply_to.clone().unwrap_or_else(|| from_conversation_id.to_owned());
        if reply_to != from_conversation_id {
            let reply_row = self
                .conversation_repo
                .get(user_id, &reply_to)
                .await
                .map_err(|e| DelegateError::TransportUnavailable { reason: e.to_string() })?
                .ok_or_else(|| DelegateError::ReplyTargetNotOwned { id: reply_to.clone() })?;
            if team_id_from_extra(&reply_row.extra).is_some() {
                return Err(DelegateError::ReplyTargetNotOwned { id: reply_to.clone() });
            }
        }

        let depth = req.depth.unwrap_or(0);
        if depth > MAX_DEPTH {
            return Err(DelegateError::DepthExceeded { depth, max: MAX_DEPTH });
        }

        let project_id = sender_row
            .project_id
            .as_deref()
            .filter(|p| !p.trim().is_empty())
            .unwrap_or(aionui_common::constants::TEAM_NO_PROJECT_SENTINEL)
            .to_owned();
        let envelope_id = Uuid::now_v7().to_string();
        let from_agent_id =
            extract_assistant_id_from_extra_value(&serde_json::from_str(&sender_row.extra).unwrap_or_default())
                .unwrap_or_default();
        let block = DelegateEnvelopeBlock {
            kind: DelegateEnvelopeKind::Dispatch,
            from_agent_id,
            from_agent_name: from_agent_name.to_owned(),
            reply_to: Some(reply_to.clone()),
            depth,
            envelope_id: envelope_id.clone(),
            workspace: "unknown (differs from yours)".to_owned(),
            created_at_ms: now_ms(),
        };
        let root = ConveneRootTask {
            subject: first_line(&req.message),
            description: &req.message,
            expected_output: req.expected_output.as_deref(),
        };

        let convened = self
            .team_bridge
            .convene_engagement(user_id, team_id, &project_id, root, &block)
            .await
            .map_err(|e| map_bridge_error(e, &target.name))?;

        // Audit row keeps the (caller-rooted) chain visible to the guards; the
        // engagement's own board owns the real task state.
        self.persist_envelope(
            &envelope_id,
            user_id,
            from_conversation_id,
            &convened.engagement_id,
            team_id,
            depth,
            "delivered",
        )
        .await?;
        info!(
            envelope_id,
            from = from_conversation_id,
            team_id,
            engagement_id = %convened.engagement_id,
            root_task_id = %convened.root_task_id,
            depth,
            "delegate dispatch convened team engagement"
        );
        Ok(DelegateDispatchResponse {
            status: DelegateDeliveryStatus::Delivered,
            to_conversation_id: convened.engagement_id.clone(),
            to_assistant_id: team_id.clone(),
            envelope_id,
            depth,
            engagement_id: Some(convened.engagement_id),
            root_task_id: Some(convened.root_task_id),
        })
    }

    // -----------------------------------------------------------------------
    // Sync ask
    // -----------------------------------------------------------------------

    pub async fn ask(
        &self,
        user_id: &str,
        from_conversation_id: &str,
        from_turn_id: &str,
        from_agent_name: &str,
        req: DelegateAskRequest,
    ) -> Result<DelegateAskResponse, DelegateError> {
        if !self.is_enabled_for(user_id).await {
            return Err(DelegateError::FeatureDisabled);
        }
        let timeout_seconds = req.timeout_seconds.unwrap_or(DEFAULT_SYNC_TIMEOUT_SECONDS);

        let target_assistant_id = self.assistant_target(user_id, &req.to).await?;
        if !self
            .is_delegation_enabled_for_assistant(user_id, &target_assistant_id)
            .await?
        {
            return Err(DelegateError::DelegationDisabledForTarget {
                assistant_id: target_assistant_id.clone(),
            });
        }

        let depth = self.current_chain_depth(from_conversation_id).await.unwrap_or(0);
        if depth + 1 > MAX_DEPTH {
            return Err(DelegateError::DepthExceeded {
                depth: depth + 1,
                max: MAX_DEPTH,
            });
        }

        if self
            .find_envelope_on_chain(from_conversation_id, &target_assistant_id)
            .await?
            .is_some()
        {
            return Err(DelegateError::CycleDetected {
                root: from_conversation_id.to_owned(),
                target: target_assistant_id.clone(),
            });
        }

        let (target_conversation_id, _created) = self
            .ensure_target_conversation(user_id, &target_assistant_id, from_conversation_id)
            .await?;
        if target_conversation_id == from_conversation_id {
            return Err(DelegateError::TargetIsSelf {
                id: target_conversation_id.clone(),
            });
        }

        let envelope_id = Uuid::now_v7().to_string();
        let from_agent_id = self
            .extract_sender_assistant_id(from_conversation_id)
            .await
            .unwrap_or_default();
        let block = DelegateEnvelopeBlock {
            kind: DelegateEnvelopeKind::Ask,
            from_agent_id,
            from_agent_name: from_agent_name.to_owned(),
            reply_to: Some(from_conversation_id.to_owned()),
            depth: depth + 1,
            envelope_id: envelope_id.clone(),
            workspace: "unknown (differs from yours)".to_owned(),
            created_at_ms: now_ms(),
        };
        let body = compose_delivery_body(&block, &req.question);

        self.suspend.park(SuspendedTurn {
            envelope_id: envelope_id.clone(),
            caller_conversation_id: from_conversation_id.to_owned(),
            caller_turn_id: from_turn_id.to_owned(),
            target_conversation_id: target_conversation_id.clone(),
            created_at_ms: now_ms(),
            expires_at_ms: now_ms() + (timeout_seconds as i64 * 1000),
        });

        self.persist_envelope(
            &envelope_id,
            user_id,
            from_conversation_id,
            &target_conversation_id,
            &target_assistant_id,
            depth + 1,
            "sync_pending",
        )
        .await?;

        if let Err(e) = self
            .deliver_now(user_id, &target_conversation_id, body, Vec::new())
            .await
        {
            warn!(
                envelope_id,
                error = %e,
                "sync ask initial delivery failed; will rely on drainer"
            );
        }

        let started = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(timeout_seconds);
        let reply = loop {
            if started.elapsed() >= timeout {
                self.complete_sync(&envelope_id, "[sync timed out]").await;
                return Err(DelegateError::SyncTimeout { timeout_seconds });
            }
            match self.read_reply(&envelope_id).await? {
                Some(reply) => break reply,
                None => {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            }
        };

        self.complete_sync(&envelope_id, &reply).await;

        Ok(DelegateAskResponse {
            envelope_id,
            to_conversation_id: target_conversation_id,
            reply,
            depth: depth + 1,
            elapsed_ms: started.elapsed().as_millis() as u64,
        })
    }

    // -----------------------------------------------------------------------
    // Sync reply handling
    // -----------------------------------------------------------------------

    pub async fn read_reply(&self, envelope_id: &str) -> Result<Option<String>, DelegateError> {
        let rows = self
            .conversation_repo
            .raw_query(
                "SELECT reply_content FROM delegation_envelopes WHERE id = ?1",
                vec![envelope_id.to_owned()],
            )
            .await
            .map_err(|e| DelegateError::TransportUnavailable { reason: e.to_string() })?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|r| r.try_get::<Option<String>, _>("reply_content").ok())
            .flatten())
    }

    pub async fn complete_sync(&self, envelope_id: &str, reply: &str) {
        let _ = self
            .conversation_repo
            .raw_execute(
                "UPDATE delegation_envelopes SET status = 'sync_replied', reply_content = ?1, updated_at = ?2 WHERE id = ?3",
                vec![reply.to_owned(), now_ms().to_string(), envelope_id.to_owned()],
            )
            .await;
        self.suspend.resolve(envelope_id);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    async fn is_delegation_enabled_for_assistant(
        &self,
        user_id: &str,
        assistant_id: &str,
    ) -> Result<bool, DelegateError> {
        let rows = self
            .conversation_repo
            .raw_query(
                "SELECT allow_delegation FROM assistant_definitions WHERE user_id = ?1 AND id = ?2",
                vec![user_id.to_owned(), assistant_id.to_owned()],
            )
            .await
            .map_err(|e| DelegateError::TransportUnavailable { reason: e.to_string() })?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|r| r.try_get::<i64, _>("allow_delegation").ok())
            .map(|v| v != 0)
            .unwrap_or(false))
    }

    async fn ensure_target_conversation(
        &self,
        user_id: &str,
        target_assistant_id: &str,
        from_conversation_id: &str,
    ) -> Result<(String, bool), DelegateError> {
        // Reuse the caller's own existing delegated room for this exact
        // target: match on the structured `extra` fields (preset_assistant_id
        // + delegated_from) rather than a greedy substring LIKE, so a room
        // created for another conversation is never hijacked.
        let rows = self
            .conversation_repo
            .raw_query(
                REUSE_ROOM_SQL,
                vec![
                    user_id.to_owned(),
                    target_assistant_id.to_owned(),
                    from_conversation_id.to_owned(),
                ],
            )
            .await
            .map_err(|e| DelegateError::TransportUnavailable { reason: e.to_string() })?;
        if let Some(id) = rows.into_iter().next().and_then(|r| r.try_get::<String, _>("id").ok()) {
            return Ok((id, false));
        }

        // The conversation-creation path resolves an assistant by its
        // `assistant_id` column (custom-*), but the delegation roster hands us
        // the definition id (asstdef_*). Translate, else the room is created
        // without a backend/agent_id binding and fails on first send.
        let assistant_lookup = self
            .conversation_repo
            .raw_query(
                ASSISTANT_ID_FOR_DEFINITION_SQL,
                vec![user_id.to_owned(), target_assistant_id.to_owned()],
            )
            .await
            .map_err(|e| DelegateError::TransportUnavailable { reason: e.to_string() })?;
        let create_assistant_id = assistant_lookup
            .into_iter()
            .next()
            .and_then(|r| r.try_get::<String, _>("assistant_id").ok())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| target_assistant_id.to_owned());

        // Inherit the caller's project/workspace so the lead works the same
        // project instead of an empty temp dir.
        let caller_ws = self
            .conversation_repo
            .raw_query(CALLER_WORKSPACE_SQL, vec![from_conversation_id.to_owned()])
            .await
            .ok()
            .and_then(|rows| {
                rows.into_iter().next().and_then(|r| {
                    r.try_get::<Option<String>, _>("ws")
                        .ok()
                        .flatten()
                        .filter(|s| !s.is_empty())
                })
            });
        let mut req_extra = serde_json::json!({
            "delegated_from": from_conversation_id,
            "preset_assistant_id": target_assistant_id,
        });
        if let Some(ws) = caller_ws.as_deref() {
            req_extra["workspace"] = serde_json::Value::String(ws.to_owned());
        }

        let req = CreateConversationRequest {
            r#type: Some(AgentType::Acp),
            name: Some(format!("{target_assistant_id} (delegated)")),
            model: None,
            assistant: Some(AssistantConversationRequest {
                id: create_assistant_id,
                locale: Some("en-US".to_owned()),
                conversation_overrides: None,
            }),
            source: Some(ConversationSource::Aionui),
            channel_chat_id: None,
            extra: req_extra,
        };
        let resp = self
            .conversation_service
            .create(user_id, req)
            .await
            .map_err(|e| DelegateError::TransportUnavailable { reason: e.to_string() })?;
        Ok((resp.id, true))
    }

    async fn find_envelope_on_chain(
        &self,
        root_conversation_id: &str,
        target_assistant_id: &str,
    ) -> Result<Option<String>, DelegateError> {
        let rows = self
            .conversation_repo
            .raw_query(
                LIVE_CHAIN_SQL,
                vec![root_conversation_id.to_owned(), now_ms().to_string()],
            )
            .await
            .map_err(|e| DelegateError::TransportUnavailable { reason: e.to_string() })?;
        for r in rows {
            if let Ok(id) = r.try_get::<String, _>("target_assistant_id")
                && id == target_assistant_id
            {
                return Ok(Some(id));
            }
        }
        Ok(None)
    }

    async fn current_chain_depth(&self, from_conversation_id: &str) -> Option<u32> {
        let rows = self
            .conversation_repo
            .raw_query(
                CHAIN_DEPTH_SQL,
                vec![from_conversation_id.to_owned(), now_ms().to_string()],
            )
            .await
            .ok()?;
        rows.into_iter()
            .next()
            .and_then(|r| r.try_get::<Option<i64>, _>("max_depth").ok())
            .flatten()
            .map(|d| d as u32)
    }

    async fn extract_sender_assistant_id(&self, from_conversation_id: &str) -> Option<String> {
        let rows = self
            .conversation_repo
            .raw_query(
                "SELECT extra FROM conversations WHERE id = ?1",
                vec![from_conversation_id.to_owned()],
            )
            .await
            .ok()?;
        let row = rows.into_iter().next()?;
        let extra_str: String = row.try_get("extra").unwrap_or_else(|_| "{}".to_owned());
        let extra: serde_json::Value = serde_json::from_str(&extra_str).ok()?;
        extract_assistant_id_from_extra_value(&extra)
    }

    #[allow(clippy::too_many_arguments)]
    async fn persist_envelope(
        &self,
        envelope_id: &str,
        user_id: &str,
        root_conversation_id: &str,
        target_conversation_id: &str,
        target_assistant_id: &str,
        depth: u32,
        status: &str,
    ) -> Result<(), DelegateError> {
        let now = now_ms().to_string();
        let expires_at = (now_ms() + QUEUE_TTL_MS).to_string();
        self.conversation_repo
            .raw_execute(
                "INSERT OR REPLACE INTO delegation_envelopes (id, user_id, root_conversation_id, target_conversation_id, target_assistant_id, depth, status, expires_at, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                vec![
                    envelope_id.to_owned(),
                    user_id.to_owned(),
                    root_conversation_id.to_owned(),
                    target_conversation_id.to_owned(),
                    target_assistant_id.to_owned(),
                    depth.to_string(),
                    status.to_owned(),
                    expires_at,
                    now.clone(),
                    now,
                ],
            )
            .await
            .map_err(|e| DelegateError::TransportUnavailable { reason: e.to_string() })?;
        Ok(())
    }

    /// Deliver a composed message into the target conversation. Maps onto
    /// `conversation_service.send_message` with the task_manager handle
    /// from the router state.
    pub(crate) async fn deliver_now(
        &self,
        user_id: &str,
        target_conversation_id: &str,
        content: String,
        files: Vec<String>,
    ) -> Result<(), String> {
        let request = SendMessageRequest {
            content,
            files: files.into_iter().map(|path| ChatFileRef::Local { path }).collect(),
            sessions: Vec::new(),
            inject_skills: Vec::new(),
            hidden: false,
        };
        self.conversation_service
            .send_message(user_id, target_conversation_id, request, &self.task_manager)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

/// Map a bridge failure onto the delegate error surface (dispatch site of the
/// team branch). Reuses existing `DelegateError` variants only:
/// `TeamNotFound`/`NotOwner` → `TargetNotFound` (404; the team side already
/// collapses "other user's team" into not-found, so existence never leaks and
/// `NotOwner` maps the same way). `ProjectNotFound`/`Delivery` →
/// `TransportUnavailable` (the convene itself failed, not the target lookup).
fn map_bridge_error(error: BridgeError, team_name: &str) -> DelegateError {
    match error {
        BridgeError::TeamNotFound | BridgeError::NotOwner => DelegateError::TargetNotFound {
            query: team_name.to_owned(),
        },
        BridgeError::ProjectNotFound => DelegateError::TransportUnavailable {
            reason: format!("team '{team_name}' could not resolve the project engagement"),
        },
        BridgeError::Delivery(reason) => DelegateError::TransportUnavailable { reason },
    }
}

/// Root-task subject for a convened task: the message's first non-empty line
/// (the task label the request carries).
/// ponytail: no length cap; truncate when noisy subjects show up in boards.
fn first_line(message: &str) -> &str {
    message
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("")
}

fn team_id_from_extra(extra: &str) -> Option<String> {
    // Canonical key is `teamId` (camelCase) — provisioned by the team runtime
    // and read by session-message via the same shared helper. The previous
    // hand-rolled `team_id` (snake_case) lookup never matched, leaving the
    // sender-is-team guard permanently inert (spec §16 P2-4). Reuse the single
    // source of truth so this cannot drift again.
    aionui_conversation::session_mentions::team_id_from_extra_str(extra)
}

fn extract_assistant_id_from_extra_value(extra: &serde_json::Value) -> Option<String> {
    extra
        .get("preset_assistant_id")
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned)
        .or_else(|| {
            extra
                .get("assistant_id")
                .and_then(|v| v.as_str())
                .map(ToOwned::to_owned)
        })
}

pub fn compose_delivery_body(block: &DelegateEnvelopeBlock, body: &str) -> String {
    format!(
        "[[AION_DELEGATE]]\n\
         from_agent_id: {from_id}\n\
         from_agent_name: {from_name}\n\
         reply_to: {reply_to}\n\
         depth: {depth}\n\
         envelope_id: {envelope_id}\n\
         workspace: {workspace}\n\
         created_at_ms: {created_at}\n\
         kind: {kind:?}\n\
         [[/AION_DELEGATE]]\n\n\
         {body}",
        from_id = block.from_agent_id,
        from_name = block.from_agent_name,
        reply_to = block.reply_to.clone().unwrap_or_default(),
        depth = block.depth,
        envelope_id = block.envelope_id,
        workspace = block.workspace,
        created_at = block.created_at_ms,
        kind = block.kind,
        body = body,
    )
}

// Avoid the unused-imports warning when AgentType aliases from the two
// re-exports differ.
#[allow(dead_code)]
fn _alias_check(_a: AgentType, _s: ConversationSource) {}

#[cfg(test)]
#[path = "service_test.rs"]
mod service_test;
