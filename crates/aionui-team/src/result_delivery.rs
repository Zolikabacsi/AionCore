//! Port + metadata contract for the consolidated engagement-result return
//! (Phase 4b, spec §8).
//!
//! When a convened engagement's ROOT task completes, the lead's consolidated
//! result must go back to the delegating caller conversation. `aionui-team`
//! cannot reach the conversation/message write directly (same-layer domain
//! crates meet only through traits — AGENTS.md), so it declares this port and
//! the app composes an adapter over its conversation write, mirroring the 4a
//! `TeamEngagementBridge` split.

use async_trait::async_trait;

use crate::error::TeamError;

/// Task-metadata key carrying the caller conversation for result return. A
/// task is "the engagement root for a delegated convening" iff its metadata
/// carries this key (ruling 1; written by `convene_delegated_task`).
pub const DELEGATE_REPLY_TO_KEY: &str = "delegate_reply_to";
/// Task-metadata key carrying the engagement the root task belongs to.
pub const ENGAGEMENT_ID_KEY: &str = "engagement_id";
/// Task-metadata key carrying the caller's delegation-chain depth at convene
/// time (Phase 4c). Threaded across the engagement boundary so the lead's
/// onward hop can increment it and the depth cap stays authoritative (spec §5.9).
pub const DELEGATE_DEPTH_KEY: &str = "delegate_depth";

/// Correlation metadata stamped on the ROOT task at convene time so the
/// result-capture path can find the caller without a second lookup, and so a
/// later engagement hop can resume the depth budget the caller arrived on.
pub fn delegated_result_metadata(reply_to: &str, engagement_id: &str, depth: u32) -> serde_json::Value {
    serde_json::json!({
        DELEGATE_REPLY_TO_KEY: reply_to,
        ENGAGEMENT_ID_KEY: engagement_id,
        DELEGATE_DEPTH_KEY: depth,
    })
}

/// Depth the lead must pass to the NEXT engagement hop: the root task's stored
/// caller depth + 1, saturating at `u32::MAX`. `0` when the metadata is absent
/// or carries no depth key (an ordinary / legacy task — the chain root).
pub fn next_depth_from_metadata(metadata: Option<&serde_json::Value>) -> u32 {
    metadata
        .and_then(|value| value.get(DELEGATE_DEPTH_KEY))
        .and_then(serde_json::Value::as_u64)
        .map_or(0, |depth| u32::try_from(depth).unwrap_or(u32::MAX).saturating_add(1))
}

/// The caller depth a root task was convened AT, read straight from metadata
/// (`0` when absent — an ordinary/legacy task or a no-reply convene). Unlike
/// [`next_depth_from_metadata`] this does NOT increment: it answers "what is
/// this engagement's current chain depth" for the sender-inversion guard
/// (Phase 4c, spec §5.9).
pub fn delegated_depth_from_metadata(metadata: Option<&serde_json::Value>) -> u32 {
    metadata
        .and_then(|value| value.get(DELEGATE_DEPTH_KEY))
        .and_then(serde_json::Value::as_u64)
        .map_or(0, |depth| u32::try_from(depth).unwrap_or(u32::MAX))
}

/// Extract the caller conversation id from a task's metadata. `None` for
/// ordinary team tasks (no metadata / no key) → the capture path is
/// byte-identical to Phase 3a for them.
pub fn delegated_reply_to(metadata: Option<&serde_json::Value>) -> Option<String> {
    metadata?
        .get(DELEGATE_REPLY_TO_KEY)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

/// Extract the engagement id from a root task's metadata (falls back to the
/// caller-supplied default when the key is absent or empty).
pub fn delegated_engagement_id(metadata: Option<&serde_json::Value>, fallback: &str) -> String {
    metadata
        .and_then(|value| value.get(ENGAGEMENT_ID_KEY))
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(fallback)
        .to_owned()
}

/// Delivers a convened engagement's consolidated root-task result back to the
/// delegating caller conversation. Implemented in `aionui-app` over the
/// composition's conversation/message write. Spec §10: when the caller
/// conversation is missing or not owned by `user_id`, the adapter must NOT
/// post (the result stays stored on the task; the caller warns).
#[async_trait]
pub trait DelegatedResultDelivery: Send + Sync {
    async fn deliver_result(
        &self,
        user_id: &str,
        caller_conversation_id: &str,
        engagement_id: &str,
        text: &str,
    ) -> Result<(), TeamError>;
}

/// Default double: accepts everything silently. Used where no composition
/// wiring exists (unit tests that don't exercise the return path).
pub struct NoopDelegatedResultDelivery;

#[async_trait]
impl DelegatedResultDelivery for NoopDelegatedResultDelivery {
    async fn deliver_result(
        &self,
        _user_id: &str,
        _caller_conversation_id: &str,
        _engagement_id: &str,
        _text: &str,
    ) -> Result<(), TeamError> {
        Ok(())
    }
}
