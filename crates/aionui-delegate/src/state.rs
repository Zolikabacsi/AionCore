//! RouterState for the delegate service. Holds Arc-wrapped dependencies only;
//! construction happens in `aionui-app`'s `build_*_state()` (AGENTS.md DI
//! convention).

use std::sync::Arc;

use aionui_ai_agent::RuntimeTokenService;
use aionui_ai_agent::task_manager::IWorkerTaskManager;
use aionui_conversation::service::ConversationService;
use aionui_db::IConversationRepository;
use aionui_db::ISettingsRepository;
use aionui_realtime::EventBroadcaster;

use crate::bridge::TeamEngagementBridge;
use crate::queue::DelegateQueue;
use crate::rate_limit::DelegateRateLimiter;
use crate::service::DelegateService;
use crate::turn_suspend::TurnSuspendRegistry;

#[derive(Clone)]
pub struct DelegateRouterState {
    pub service: Arc<DelegateService>,
    pub queue: Arc<DelegateQueue>,
    pub rate_limiter: Arc<DelegateRateLimiter>,
    pub suspend: Arc<TurnSuspendRegistry>,
    pub conversation_service: ConversationService,
    pub conversation_repo: Arc<dyn IConversationRepository>,
    pub settings_repo: Arc<dyn ISettingsRepository>,
    pub broadcaster: Arc<dyn EventBroadcaster>,
    pub runtime_token_service: Arc<RuntimeTokenService>,
    pub task_manager: Arc<dyn IWorkerTaskManager>,
    /// Team-engagement bridge port; called from dispatch in Phase 4a Task 3.
    pub team_bridge: Arc<dyn TeamEngagementBridge>,
}
