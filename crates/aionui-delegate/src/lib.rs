//! CrewAI-style hierarchical delegation for AionUi.
//!
//! Two primitives: `delegate dispatch` (async) and `delegate ask` (sync),
//! both addressed by name/assistant_id, with `reply_to` envelope routing
//! and depth/cycle protection.

pub mod bridge;
pub mod error;
pub mod rate_limit;
pub mod state;
pub mod queue;
pub mod service;
pub mod turn_suspend;
pub mod routes;

#[cfg(test)]
mod tests;

/// Maximum delegation chain depth. CEO (depth 0) → CMO (depth 1) →
/// Copywriter (depth 2) → leaf (depth 3). At depth 3 the recipient must
/// reply inline instead of delegating further.
pub const MAX_DEPTH: u32 = 3;

/// Default sync-mode timeout in seconds.
pub const DEFAULT_SYNC_TIMEOUT_SECONDS: u64 = 120;

/// Default TTL for queued async deliveries.
pub const QUEUE_TTL_MS: i64 = 5 * 60 * 1000;
