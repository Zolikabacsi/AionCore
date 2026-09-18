//! Crate-owned error type. Mapped to `DelegateToolErrorPayload` / HTTP status
//! only at the route boundary (AGENTS.md).

use aionui_api_types::DelegateToolErrorCode;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DelegateError {
    #[error("target agent not found by name or id: {query}")]
    TargetNotFound { query: String },

    #[error("target name matched multiple agents: {query} -> {candidates:?}")]
    AmbiguousTarget {
        query: String,
        candidates: Vec<String>,
    },

    #[error("target assistant {assistant_id} has delegation disabled")]
    DelegationDisabledForTarget { assistant_id: String },

    #[error("sender conversation {id} has delegation disabled")]
    DelegationDisabledForSender { id: String },

    #[error("sender and target resolve to the same conversation: {id}")]
    TargetIsSelf { id: String },

    #[error("reply_to conversation {id} is not owned by caller")]
    ReplyTargetNotOwned { id: String },

    #[error("delegation cycle detected: root={root}, target={target}")]
    CycleDetected { root: String, target: String },

    #[error("delegation depth {depth} exceeds max {max}")]
    DepthExceeded { depth: u32, max: u32 },

    #[error("delegation rate limit tripped on pair ({from}, {to})")]
    RateLimited { from: String, to: String },

    #[error("pending-delivery queue is full")]
    QueueFull,

    #[error("delegation feature is disabled for this user")]
    FeatureDisabled,

    #[error("runtime auth failed")]
    RuntimeAuthFailed,

    #[error("stdin payload does not match the schema: {reason}")]
    SchemaValidation { reason: String },

    #[error("delivery transport unavailable: {reason}")]
    TransportUnavailable { reason: String },

    #[error("sync reply did not arrive within {timeout_seconds}s")]
    SyncTimeout { timeout_seconds: u64 },
}

impl DelegateError {
    pub fn code(&self) -> DelegateToolErrorCode {
        use DelegateToolErrorCode as C;
        match self {
            Self::TargetNotFound { .. } => C::TargetNotFound,
            Self::AmbiguousTarget { .. } => C::AmbiguousTarget,
            Self::DelegationDisabledForTarget { .. } => C::DelegationDisabledForTarget,
            Self::DelegationDisabledForSender { .. } => C::DelegationDisabledForSender,
            Self::TargetIsSelf { .. } => C::TargetIsSelf,
            Self::ReplyTargetNotOwned { .. } => C::ReplyTargetNotOwned,
            Self::CycleDetected { .. } => C::CycleDetected,
            Self::DepthExceeded { .. } => C::DepthExceeded,
            Self::RateLimited { .. } => C::RateLimited,
            Self::QueueFull => C::QueueFull,
            Self::FeatureDisabled => C::FeatureDisabled,
            Self::RuntimeAuthFailed => C::RuntimeAuthFailed,
            Self::SchemaValidation { .. } => C::SchemaValidationFailed,
            Self::TransportUnavailable { .. } => C::TransportUnavailable,
            Self::SyncTimeout { .. } => C::SyncTimeout,
        }
    }

    /// HTTP status for the API boundary. Mirrors `SessionMessageError::http_status`.
    pub fn http_status(&self) -> u16 {
        use DelegateToolErrorCode as C;
        match self.code() {
            C::TargetNotFound | C::AmbiguousTarget => 404,
            C::DelegationDisabledForTarget
            | C::DelegationDisabledForSender
            | C::TargetIsSelf
            | C::ReplyTargetNotOwned => 403,
            C::CycleDetected | C::DepthExceeded => 409,
            C::RateLimited => 429,
            C::SyncTimeout => 408,
            _ => 400,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_mapping_round_trip() {
        for (err, expected) in [
            (
                DelegateError::TargetNotFound { query: "x".into() },
                DelegateToolErrorCode::TargetNotFound,
            ),
            (
                DelegateError::DepthExceeded { depth: 4, max: 3 },
                DelegateToolErrorCode::DepthExceeded,
            ),
            (
                DelegateError::CycleDetected {
                    root: "a".into(),
                    target: "b".into(),
                },
                DelegateToolErrorCode::CycleDetected,
            ),
        ] {
            assert_eq!(err.code(), expected);
        }
    }
}
