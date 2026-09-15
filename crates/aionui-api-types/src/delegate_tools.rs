//! Wire types for the `aionui-delegate` crate: agent-to-agent delegation
//! requests, responses, error codes, and the `delegate` envelope block that
//! rides alongside a delivered message body.
//!
//! Modeled on `session_tools.rs` (cross-session messaging) but with address
//! resolution by name/assistant_id and a `reply_to` envelope so the recipient
//! knows where to report back.

use aionui_common::TimestampMs;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Bumped whenever the wire shape changes in a backwards-incompatible way.
pub const DELEGATE_TOOLS_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DelegateToolErrorCode {
    /// The `to` value resolved to zero agents.
    TargetNotFound,
    /// The `to` value resolved to more than one agent.
    AmbiguousTarget,
    /// Target agent's `allow_delegation = 0`.
    DelegationDisabledForTarget,
    /// Caller's `allow_delegation = 0`.
    DelegationDisabledForSender,
    /// Caller conversation is the target conversation (self-dispatch).
    TargetIsSelf,
    /// `reply_to` resolved to a conversation the caller doesn't own.
    ReplyTargetNotOwned,
    /// Walking the chain detected a loop.
    CycleDetected,
    /// Depth would exceed `MAX_DEPTH`.
    DepthExceeded,
    /// Rate limit tripped on the (sender, target) pair or globally.
    RateLimited,
    /// Pending-delivery queue is full.
    QueueFull,
    /// User feature toggle is off.
    FeatureDisabled,
    /// Runtime token missing or invalid.
    RuntimeAuthFailed,
    /// Stdin payload didn't match schema.
    SchemaValidationFailed,
    /// Backend transport unavailable.
    TransportUnavailable,
    /// Sync-mode reply did not arrive before `timeout_seconds`.
    SyncTimeout,
    /// Sync mode requested but the target does not support suspension.
    SyncNotSupported,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DelegateToolErrorPayload {
    pub code: DelegateToolErrorCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl DelegateToolErrorPayload {
    pub fn new(code: DelegateToolErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: None,
        }
    }

    pub fn with_details(self, details: Value) -> Self {
        Self {
            details: Some(details),
            ..self
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegateCliMeta {
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegateCliEnvelope<T> {
    pub success: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<DelegateToolErrorPayload>,
    pub meta: DelegateCliMeta,
}

impl<T> DelegateCliEnvelope<T> {
    pub fn success(data: T, command: Option<String>) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
            meta: DelegateCliMeta {
                schema_version: DELEGATE_TOOLS_SCHEMA_VERSION,
                command,
            },
        }
    }

    pub fn failure(error: DelegateToolErrorPayload, command: Option<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(error),
            meta: DelegateCliMeta {
                schema_version: DELEGATE_TOOLS_SCHEMA_VERSION,
                command,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Request / response payloads
// ---------------------------------------------------------------------------

/// Body of `POST /api/runtime/delegate/dispatch` and the stdin payload of the
/// `delegate dispatch` CLI subcommand.
///
/// `to` is resolved server-side by exact name, case-insensitive prefix, then
/// `assistant_id`. `reply_to` defaults to the caller's conversation if absent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegateDispatchRequest {
    pub to: String,
    pub message: String,
    #[serde(default)]
    pub files: Vec<String>,
    /// Conversation to receive the target's reply. Defaults to the caller's
    /// conversation. Must be owned by the same `user_id`.
    #[serde(default)]
    pub reply_to: Option<String>,
    /// Optional override; the runtime computes the actual `depth` from the
    /// envelope chain. Setting this manually is a debug aid.
    #[serde(default)]
    pub depth: Option<u32>,
    /// Optional success criteria. On a team-target dispatch this becomes the
    /// engagement root task's `expected_output` (spec §5 step 4); ignored on
    /// the assistant path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_output: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DelegateDeliveryStatus {
    Delivered,
    Queued,
    CreatedAndDelivered,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegateDispatchResponse {
    pub status: DelegateDeliveryStatus,
    /// The conversation that received (or will receive) the message. For
    /// `CreatedAndDelivered` this is a fresh conversation created for the
    /// target.
    pub to_conversation_id: String,
    pub to_assistant_id: String,
    pub envelope_id: String,
    pub depth: u32,
    /// Team dispatch only: the engagement convened/reused by the bridge.
    /// `None` (omitted from the wire payload) for the assistant path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engagement_id: Option<String>,
    /// Team dispatch only: the root task created on the engagement board.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_task_id: Option<String>,
}

/// Body of `POST /api/runtime/delegate/ask` (sync mode).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegateAskRequest {
    pub to: String,
    pub question: String,
    /// Optional enum constraint to bias the recipient's reply shape (e.g.,
    /// `["approve", "reject"]`). The runtime does NOT enforce — it only
    /// passes it through.
    #[serde(default)]
    pub options: Vec<String>,
    /// Timeout in seconds. Defaults to `DEFAULT_SYNC_TIMEOUT_SECONDS` (120).
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegateAskResponse {
    pub envelope_id: String,
    pub to_conversation_id: String,
    pub reply: String,
    pub depth: u32,
    pub elapsed_ms: u64,
}

/// Body of `GET /api/runtime/delegate/targets` and stdin payload of the
/// `delegate targets` CLI subcommand. Returns every agent whose
/// `allow_delegation = 1`, optionally filtered by `q` (case-insensitive
/// substring on name).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DelegateTargetsQuery {
    #[serde(default)]
    pub q: Option<String>,
    #[serde(default)]
    pub limit: Option<u32>,
}

/// Discriminates a delegation roster entry. `Assistant` is the original
/// (and default) kind; `Team` targets were added in Phase 4a as a non-breaking
/// superset so a caller can resolve/list a team by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DelegateTargetKind {
    #[default]
    Assistant,
    Team,
}

impl DelegateTargetKind {
    /// Used as the `skip_serializing_if` predicate so an `Assistant` target's
    /// wire payload carries no `kind` key (byte-identical to pre-4a).
    pub fn is_assistant(&self) -> bool {
        *self == Self::Assistant
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegateTarget {
    pub assistant_id: String,
    pub name: String,
    pub backend: String,
    pub description: Option<String>,
    /// Target kind. Defaults to `Assistant` on read (pre-4a payloads lack it)
    /// and is omitted from the wire for assistants so their serialized bytes are
    /// unchanged; only `Team` entries emit `"kind":"team"`.
    #[serde(default, skip_serializing_if = "DelegateTargetKind::is_assistant")]
    pub kind: DelegateTargetKind,
    /// Populated only for `Team` entries; `None` for assistants. Omitted from
    /// the wire when absent so assistant responses stay byte-identical.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
}

/// Outcome of `resolve_target`: the matched roster entry with its kind, so a
/// caller can tell an assistant definition id from a team id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedDelegateTarget {
    pub kind: DelegateTargetKind,
    /// Assistant definition id (`asstdef_*`) for `Assistant`; team id for `Team`.
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegateTargetsResponse {
    pub items: Vec<DelegateTarget>,
}

// ---------------------------------------------------------------------------
// The on-the-wire envelope block. Mirrors `session_message_block` so the
// recipient's parser (frontend `sessionMarkers.ts` plus the agent's
// `session-message` skill text) handles it uniformly, while the
// `delegate`-specific lines (`depth`, `delegation_chain`) are only acted on by
// agents that have the `delegate` skill loaded.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegateEnvelopeBlock {
    /// True for the [[AION_DELEGATE]] marker prefix.
    pub kind: DelegateEnvelopeKind,
    /// The agent who issued the dispatch.
    pub from_agent_id: String,
    pub from_agent_name: String,
    /// The conversation the dispatcher wants the reply to land in. None means
    /// "reply inline to the user".
    pub reply_to: Option<String>,
    /// Current depth in the delegation chain. 0 = root caller.
    pub depth: u32,
    /// Envelope id, for audit / correlation.
    pub envelope_id: String,
    /// Working-directory mismatch hint, mirrors `session-message`'s `workspace:`
    /// line.
    pub workspace: String,
    /// Wall-clock timestamp of dispatch, ms.
    pub created_at_ms: TimestampMs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DelegateEnvelopeKind {
    Dispatch,
    Ask,
}

// ---------------------------------------------------------------------------
// CLI tool descriptor registry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DelegateToolName {
    DelegateDispatch,
    DelegateAsk,
    DelegateTargets,
}

impl DelegateToolName {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DelegateDispatch => "delegate_dispatch",
            Self::DelegateAsk => "delegate_ask",
            Self::DelegateTargets => "delegate_targets",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "delegate_dispatch" => Self::DelegateDispatch,
            "delegate_ask" => Self::DelegateAsk,
            "delegate_targets" => Self::DelegateTargets,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegateToolDescriptor {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub cli_command: Vec<String>,
    pub when: String,
    pub input_summary: String,
}

#[derive(Debug, Clone)]
struct DelegateToolSpec {
    name: DelegateToolName,
    description: &'static str,
    input_schema: Value,
    cli_command: &'static [&'static str],
    when: &'static str,
    input_summary: &'static str,
}

fn delegate_tool_specs() -> Vec<DelegateToolSpec> {
    vec![
        DelegateToolSpec {
            name: DelegateToolName::DelegateTargets,
            description: "List agents available for delegation (those with allow_delegation=1). \
                           Use it when you need to look up the exact assistant_id before dispatching.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "q": { "type": "string", "description": "Optional case-insensitive substring filter on name." },
                    "limit": { "type": "integer", "description": "Max items (default 50, capped at 500)." }
                },
                "required": [],
                "additionalProperties": false
            }),
            cli_command: &["targets"],
            when: "You need to confirm a target's assistant_id before delegating to them.",
            input_summary: "optional q / limit",
        },
        DelegateToolSpec {
            name: DelegateToolName::DelegateDispatch,
            description: "Async fan-out: deliver a task to another agent by name or assistant_id. \
                           Recipient runs in their own conversation. Replies arrive as new user-role \
                           messages in the conversation specified by `reply_to` (default: your conversation).",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "to": { "type": "string", "description": "Target agent name (case-insensitive prefix match) or assistant_id." },
                    "message": { "type": "string", "description": "Task body the recipient will execute." },
                    "files": { "type": "array", "items": { "type": "string" }, "description": "Optional absolute file paths to attach." },
                    "reply_to": { "type": "string", "description": "Optional conversation_id that should receive the reply. Default: your conversation." },
                    "depth": { "type": "integer", "description": "Optional chain depth override (debug aid)." },
                    "expected_output": { "type": "string", "description": "Optional success criteria; on a team target it becomes the engagement root task's expected_output." }
                },
                "required": ["to", "message"],
                "additionalProperties": false
            }),
            cli_command: &["dispatch"],
            when: "Work that takes more than one agent turn (drafting, audit, build). Reply is asynchronous.",
            input_summary: "{ to, message, optional files / reply_to / depth }",
        },
        DelegateToolSpec {
            name: DelegateToolName::DelegateAsk,
            description: "Sync round-trip: park your turn, target runs one turn, their reply text is \
                           returned in-line. Use only for short yes/no questions you cannot proceed without.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "to": { "type": "string", "description": "Target agent name (case-insensitive prefix match) or assistant_id." },
                    "question": { "type": "string", "description": "The question to deliver to the recipient." },
                    "options": { "type": "array", "items": { "type": "string" }, "description": "Optional enum-like constraint to bias the recipient's reply shape. Not enforced." },
                    "timeout_seconds": { "type": "integer", "description": "Optional. Default 120s." }
                },
                "required": ["to", "question"],
                "additionalProperties": false
            }),
            cli_command: &["ask"],
            when: "You need a quick answer (yes/no, approve/reject, one fact) before continuing. Do NOT use for tasks that take more than one turn.",
            input_summary: "{ to, question, optional options / timeout_seconds }",
        },
    ]
}

pub fn delegate_tool_descriptors() -> Vec<DelegateToolDescriptor> {
    delegate_tool_specs()
        .into_iter()
        .map(|spec| DelegateToolDescriptor {
            name: spec.name.as_str().to_owned(),
            description: spec.description.to_owned(),
            input_schema: spec.input_schema,
            cli_command: spec.cli_command.iter().map(|s| s.to_string()).collect(),
            when: spec.when.to_owned(),
            input_summary: spec.input_summary.to_owned(),
        })
        .collect()
}

pub fn delegate_tool_descriptor(name: &str) -> Option<DelegateToolDescriptor> {
    let parsed = DelegateToolName::parse(name)?;
    delegate_tool_specs()
        .into_iter()
        .find(|spec| spec.name == parsed)
        .map(|spec| DelegateToolDescriptor {
            name: spec.name.as_str().to_owned(),
            description: spec.description.to_owned(),
            input_schema: spec.input_schema,
            cli_command: spec.cli_command.iter().map(|s| s.to_string()).collect(),
            when: spec.when.to_owned(),
            input_summary: spec.input_summary.to_owned(),
        })
}

pub fn tool_name_for_delegate_cli_path(path: &[String]) -> Option<DelegateToolName> {
    delegate_tool_specs()
        .into_iter()
        .find(|spec| spec.cli_command == path.iter().map(String::as_str).collect::<Vec<_>>())
        .map(|spec| spec.name)
}
