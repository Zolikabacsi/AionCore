//! HTTP routes for the delegate service. Three runtime endpoints
//! (token-authenticated) and one for the front-end `mentionable` picker.
//!
//! - `POST /api/runtime/delegate/dispatch` — async fan-out.
//! - `POST /api/runtime/delegate/ask`      — sync round-trip.
//! - `GET  /api/runtime/delegate/targets`  — name resolution.
//!
//! Auth: `x-aionui-user-id`, `x-aionui-conversation-id`, `x-aionui-runtime-token`,
//! same channel as `session-message`.

use aionui_ai_agent::{RuntimeTokenScope, TEAM_RUNTIME_TOKEN_SESSION_GENERATION};
use aionui_api_types::{
    DelegateAskRequest, DelegateAskResponse, DelegateCliEnvelope, DelegateDispatchRequest,
    DelegateDispatchResponse, DelegateTarget, DelegateTargetsQuery, DelegateTargetsResponse,
    DelegateToolErrorCode, DelegateToolErrorPayload,
};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::error::DelegateError;
use crate::state::DelegateRouterState;

const HEADER_USER_ID: &str = "x-aionui-user-id";
const HEADER_CONVERSATION_ID: &str = "x-aionui-conversation-id";
const HEADER_RUNTIME_TOKEN: &str = "x-aionui-runtime-token";

pub fn delegate_routes(state: DelegateRouterState) -> Router {
    Router::new()
        .route("/api/runtime/delegate/dispatch", post(dispatch))
        .route("/api/runtime/delegate/ask", post(ask))
        .route("/api/runtime/delegate/targets", get(targets))
        .with_state(state)
}

struct RuntimeCaller {
    user_id: String,
    conversation_id: String,
}

fn runtime_caller(
    state: &DelegateRouterState,
    headers: &HeaderMap,
) -> Result<RuntimeCaller, DelegateError> {
    let user_id = required_header(headers, HEADER_USER_ID)?;
    let conversation_id = required_header(headers, HEADER_CONVERSATION_ID)?;
    let token = required_header(headers, HEADER_RUNTIME_TOKEN)?;
    state
        .runtime_token_service
        .validate(
            Some(&token),
            &user_id,
            &conversation_id,
            RuntimeTokenScope::ConversationHelper,
            TEAM_RUNTIME_TOKEN_SESSION_GENERATION,
        )
        .map_err(|_| DelegateError::RuntimeAuthFailed)?;
    Ok(RuntimeCaller {
        user_id,
        conversation_id,
    })
}

fn required_header(headers: &HeaderMap, name: &'static str) -> Result<String, DelegateError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| DelegateError::SchemaValidation {
            reason: format!("missing header: {name}"),
        })
}

async fn dispatch(
    State(state): State<DelegateRouterState>,
    headers: HeaderMap,
    Json(request): Json<DelegateDispatchRequest>,
) -> (StatusCode, Json<DelegateCliEnvelope<DelegateDispatchResponse>>) {
    let command = Some("delegate dispatch".to_owned());
    let caller = match runtime_caller(&state, &headers) {
        Ok(caller) => caller,
        Err(_) => return unauthorized(command),
    };
    let from_name = match state
        .conversation_repo
        .get(&caller.user_id, &caller.conversation_id)
        .await
        .ok()
        .flatten()
    {
        Some(row) => row.name,
        None => caller.conversation_id.clone(),
    };
    match state
        .service
        .dispatch(&caller.user_id, &caller.conversation_id, &from_name, request)
        .await
    {
        Ok(data) => (
            StatusCode::OK,
            Json(DelegateCliEnvelope::success(data, command)),
        ),
        Err(error) => envelope_failure(error, command),
    }
}

async fn ask(
    State(state): State<DelegateRouterState>,
    headers: HeaderMap,
    Json(request): Json<DelegateAskRequest>,
) -> (StatusCode, Json<DelegateCliEnvelope<DelegateAskResponse>>) {
    let command = Some("delegate ask".to_owned());
    let caller = match runtime_caller(&state, &headers) {
        Ok(caller) => caller,
        Err(_) => return unauthorized(command),
    };
    let from_name = match state
        .conversation_repo
        .get(&caller.user_id, &caller.conversation_id)
        .await
        .ok()
        .flatten()
    {
        Some(row) => row.name,
        None => caller.conversation_id.clone(),
    };
    let from_turn_id = match state
        .conversation_service
        .runtime_summary_for(&caller.conversation_id)
        .await
        .turn_id
    {
        Some(t) => t,
        None => {
            return envelope_failure(
                DelegateError::SchemaValidation {
                    reason: "caller conversation has no active turn_id".to_owned(),
                },
                command,
            );
        }
    };
    match state
        .service
        .ask(
            &caller.user_id,
            &caller.conversation_id,
            &from_turn_id,
            &from_name,
            request,
        )
        .await
    {
        Ok(data) => (
            StatusCode::OK,
            Json(DelegateCliEnvelope::success(data, command)),
        ),
        Err(error) => envelope_failure(error, command),
    }
}

async fn targets(
    State(state): State<DelegateRouterState>,
    headers: HeaderMap,
    Query(query): Query<DelegateTargetsQuery>,
) -> (StatusCode, Json<DelegateCliEnvelope<DelegateTargetsResponse>>) {
    let command = Some("delegate targets".to_owned());
    let caller = match runtime_caller(&state, &headers) {
        Ok(caller) => caller,
        Err(_) => return unauthorized(command),
    };
    match state.service.list_targets(&caller.user_id, query).await {
        Ok(data) => (
            StatusCode::OK,
            Json(DelegateCliEnvelope::success(data, command)),
        ),
        Err(error) => envelope_failure(error, command),
    }
}

fn unauthorized<T>(command: Option<String>) -> (StatusCode, Json<DelegateCliEnvelope<T>>) {
    (
        StatusCode::UNAUTHORIZED,
        Json(DelegateCliEnvelope::failure(
            DelegateToolErrorPayload::new(
                DelegateToolErrorCode::RuntimeAuthFailed,
                "runtime auth failed",
            ),
            command,
        )),
    )
}

fn envelope_failure<T>(
    error: DelegateError,
    command: Option<String>,
) -> (StatusCode, Json<DelegateCliEnvelope<T>>) {
    let status = StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::BAD_REQUEST);
    (
        status,
        Json(DelegateCliEnvelope::failure(
            DelegateToolErrorPayload::new(error.code(), error.to_string()),
            command,
        )),
    )
}

// Re-export the targets type so the route compiles against the renamed alias
// without needing to import it at every callsite.
#[allow(dead_code)]
type _UnusedDelegateTarget = DelegateTarget;
