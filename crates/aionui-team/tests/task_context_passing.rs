//! Phase 3a Task 2 — capture a completed task's `result` from the assignee's
//! final assistant message, correlated at turn finalize (controller ruling:
//! the completion tool fires mid-turn, so the capture must NOT happen at the
//! tool call; it happens when the slot's turn finalizes).
//!
//! Wiring: real `SqliteTeamRepository` over `init_database_memory()` + real
//! `TeamSessionService`/`TeamSession` + real MCP `team_task_update` tool call;
//! only the conversation-read seam (the composition-layer
//! `TeamConversationProvisioningPort`) is a recording double, same pattern as
//! `engagement_members.rs`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use aionui_ai_agent::agent_task::AgentInstance;
use aionui_ai_agent::types::BuildTaskOptions;
use aionui_ai_agent::{AgentError, IWorkerTaskManager};
use aionui_api_types::{GetConfigOptionsResponse, TeamMcpSelection, WebSocketMessage};
use aionui_common::{AgentKillReason, TimestampMs, generate_id, now_ms};
use aionui_db::models::{MessageRow, TeamRow};
use aionui_db::{
    ITeamRepository, SqliteAgentMetadataRepository, SqliteAssistantDefinitionRepository,
    SqliteAssistantOverlayRepository, SqliteProviderRepository, SqliteTeamRepository, init_database_memory,
};
use aionui_realtime::EventBroadcaster;
use aionui_team::mcp::protocol::{read_frame, write_frame};
use aionui_team::ports::{
    AgentTurnCancellationPort, AgentTurnExecutionError, AgentTurnExecutionPort, AgentTurnOutcome, AgentTurnRequest,
    AgentTurnStatus, TeamAssistantCatalogEntry, TeamAssistantCatalogPort,
};
use aionui_team::types::Team;
use aionui_team::{
    TeamAgent, TeamConversationCreateRequest, TeamConversationCreateResult, TeamConversationProvisioningPort,
    TeamError, TeamMcpSnapshotResolution, TeamProjectionMessageStore, TeamSession, TeamSessionService, TeammateRole,
};
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::net::TcpStream;

// Task 3 (context passing) uses these directly.
use aionui_team::build_wake_payload;
use aionui_team::types::TaskStatus;
use aionui_team::{TaskBoard, TaskUpdate};
use std::collections::HashSet;

const USER: &str = "u-result";
const TEAM: &str = "t-result";

// ── Test doubles: only the composition-layer seams ─────────────────────────

struct NullBroadcaster;
impl EventBroadcaster for NullBroadcaster {
    fn broadcast(&self, _msg: WebSocketMessage<Value>) {}
}

struct NoopTurnPort;
#[async_trait]
impl AgentTurnExecutionPort for NoopTurnPort {
    async fn run_agent_turn(&self, request: AgentTurnRequest) -> Result<AgentTurnOutcome, AgentTurnExecutionError> {
        Ok(AgentTurnOutcome {
            conversation_id: request.conversation_id,
            turn_id: "turn-noop".into(),
            status: AgentTurnStatus::Completed,
            runtime: None,
        })
    }
}

struct NoopCancellationPort;
#[async_trait]
impl AgentTurnCancellationPort for NoopCancellationPort {
    async fn cancel_agent_turn(
        &self,
        _user_id: &str,
        _conversation_id: &str,
        _turn_id: &str,
    ) -> Result<(), AgentTurnExecutionError> {
        Ok(())
    }
}

#[derive(Default)]
struct NoopTaskManager;
#[async_trait]
impl IWorkerTaskManager for NoopTaskManager {
    fn get_task(&self, _conversation_id: &str) -> Option<AgentInstance> {
        None
    }
    async fn get_or_build_task(&self, _: &str, _: BuildTaskOptions) -> Result<AgentInstance, AgentError> {
        Err(AgentError::internal("noop"))
    }
    fn kill(&self, _c: &str, _r: Option<AgentKillReason>) -> Result<(), AgentError> {
        Ok(())
    }
    fn kill_and_wait(
        &self,
        _c: &str,
        _r: Option<AgentKillReason>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(std::future::ready(()))
    }
    async fn clear(&self) {}
    fn active_count(&self) -> usize {
        0
    }
    fn collect_idle(&self, _: TimestampMs) -> Vec<String> {
        Vec::new()
    }
}

struct EmptyCatalog;
#[async_trait]
impl TeamAssistantCatalogPort for EmptyCatalog {
    async fn list_team_selectable_assistants(
        &self,
        _user_id: &str,
    ) -> Result<Vec<TeamAssistantCatalogEntry>, TeamError> {
        Ok(Vec::new())
    }
}

/// Stands in for the composition-layer `TeamConversationAdapters`: the only
/// behavior exercised here is the new `latest_assistant_text` read.
#[derive(Default)]
struct RecordingPort {
    assistant_texts: Mutex<HashMap<String, String>>,
}

impl RecordingPort {
    fn set_text(&self, conversation_id: &str, text: &str) {
        self.assistant_texts
            .lock()
            .unwrap()
            .insert(conversation_id.to_owned(), text.to_owned());
    }
}

#[async_trait]
impl TeamConversationProvisioningPort for RecordingPort {
    async fn create_team_conversation(
        &self,
        _request: TeamConversationCreateRequest,
    ) -> Result<TeamConversationCreateResult, TeamError> {
        Err(TeamError::InvalidRequest("unused".into()))
    }

    async fn conversation_workspace(&self, _conversation_id: &str) -> Result<Option<String>, TeamError> {
        Ok(None)
    }

    async fn conversation_assistant_id(&self, _conversation_id: &str) -> Result<Option<String>, TeamError> {
        Ok(None)
    }

    async fn update_conversation_project_binding(
        &self,
        _conversation_id: &str,
        _project_id: Option<String>,
        _folder_id: Option<String>,
        _workspace: Option<String>,
    ) -> Result<(), TeamError> {
        Ok(())
    }

    async fn create_team_temp_workspace(&self, _user_id: &str, _team_id: &str) -> Result<String, TeamError> {
        Err(TeamError::InvalidRequest("unused".into()))
    }

    async fn patch_runtime_config(&self, _conversation_id: &str, _patch: Value) -> Result<(), TeamError> {
        Ok(())
    }

    async fn save_acp_runtime_mode(&self, _conversation_id: &str, _mode: &str) -> Result<(), TeamError> {
        Ok(())
    }

    async fn get_config_options(&self, _conversation_id: &str) -> Result<GetConfigOptionsResponse, TeamError> {
        Ok(GetConfigOptionsResponse {
            config_options: Vec::new(),
        })
    }

    async fn warmup_agent_process(
        &self,
        _user_id: &str,
        _conversation_id: &str,
        _task_manager: &Arc<dyn IWorkerTaskManager>,
    ) -> Result<(), TeamError> {
        Ok(())
    }

    async fn resolve_assistant_mcp_selection(
        &self,
        _user_id: &str,
        _assistant_id: &str,
    ) -> Result<Option<TeamMcpSelection>, TeamError> {
        Ok(Some(TeamMcpSelection::default()))
    }

    async fn resolve_conversation_mcp_snapshot(
        &self,
        _user_id: &str,
        _conversation_id: &str,
        _assistant_id: Option<&str>,
    ) -> Result<TeamMcpSnapshotResolution, TeamError> {
        Ok(TeamMcpSnapshotResolution::default())
    }

    async fn delete_team_conversation(&self, _user_id: &str, _conversation_id: &str) -> Result<(), TeamError> {
        Ok(())
    }

    async fn latest_assistant_text(&self, conversation_id: &str) -> Result<Option<String>, TeamError> {
        Ok(self.assistant_texts.lock().unwrap().get(conversation_id).cloned())
    }
}

#[async_trait]
impl TeamProjectionMessageStore for RecordingPort {
    fn mint_message_id(&self) -> String {
        generate_id()
    }
    async fn find_projected_message(&self, _c: &str, _m: &str, _t: &str) -> Result<Option<MessageRow>, TeamError> {
        Ok(None)
    }
    async fn insert_projected_message(&self, _row: &MessageRow) -> Result<(), TeamError> {
        Ok(())
    }
}

// ── MCP client helpers (same pattern as e2e_team_flow.rs) ───────────────────

async fn mcp_connect(port: u16, auth_token: &str, slot_id: &str) -> TcpStream {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .expect("tcp connect to TeamMcpServer");
    let init_req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "auth_token": auth_token,
            "slot_id": slot_id,
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "task-context-test", "version": "0.1" }
        }
    });
    write_frame(&mut stream, &serde_json::to_vec(&init_req).unwrap())
        .await
        .unwrap();
    let resp: Value = serde_json::from_slice(&read_frame(&mut stream).await.unwrap()).unwrap();
    assert!(
        resp["result"]["serverInfo"]["name"].is_string(),
        "initialize failed: {resp}"
    );
    stream
}

async fn mcp_call_tool(stream: &mut TcpStream, id: u64, tool: &str, args: Value) -> Value {
    let req = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/call",
        "params": { "name": tool, "arguments": args }
    });
    write_frame(stream, &serde_json::to_vec(&req).unwrap()).await.unwrap();
    serde_json::from_slice(&read_frame(stream).await.unwrap()).unwrap()
}

fn is_mcp_error(resp: &Value) -> bool {
    resp["result"]["isError"].as_bool().unwrap_or(false)
}

fn mcp_task_id(resp: &Value) -> String {
    let text = resp["result"]["content"][0]["text"].as_str().unwrap_or("");
    let parsed: Value = serde_json::from_str(text).expect("tool text is JSON");
    parsed["task"]["task_id"].as_str().expect("task_id present").to_owned()
}

// ── Harness ────────────────────────────────────────────────────────────────

fn two_agents() -> Vec<TeamAgent> {
    vec![
        TeamAgent {
            slot_id: "lead-1".into(),
            name: "Leader".into(),
            role: TeammateRole::Lead,
            conversation_id: "conv-lead".into(),
            backend: "acp".into(),
            model: "claude".into(),
            assistant_id: None,
            status: None,
            conversation_type: None,
            cli_path: None,
        },
        TeamAgent {
            slot_id: "worker-1".into(),
            name: "Worker".into(),
            role: TeammateRole::Teammate,
            conversation_id: "conv-worker".into(),
            backend: "acp".into(),
            model: "claude".into(),
            assistant_id: None,
            status: None,
            conversation_type: None,
            cli_path: None,
        },
    ]
}

struct Harness {
    repo: Arc<SqliteTeamRepository>,
    session: Arc<TeamSession>,
    port: Arc<RecordingPort>,
    _svc: Arc<TeamSessionService>,
    _db: aionui_db::Database,
}

impl Harness {
    async fn new() -> Self {
        let db = init_database_memory().await.unwrap();
        let pool = db.pool().clone();
        let repo = Arc::new(SqliteTeamRepository::new(pool.clone()));
        let agents = two_agents();
        repo.create_team(&TeamRow {
            id: TEAM.to_owned(),
            user_id: USER.to_owned(),
            name: TEAM.to_owned(),
            workspace: "/tmp/task-context-passing".to_owned(),
            workspace_mode: "shared".to_owned(),
            agents: serde_json::to_string(&agents).unwrap(),
            lead_agent_id: Some("lead-1".to_owned()),
            session_mode: None,
            agents_version: "1.0.1".to_owned(),
            created_at: now_ms(),
            updated_at: now_ms(),
            project_id: None,
            folder_id: None,
        })
        .await
        .unwrap();

        let port = Arc::new(RecordingPort::default());
        let broadcaster: Arc<dyn EventBroadcaster> = Arc::new(NullBroadcaster);
        let svc = TeamSessionService::new(
            Arc::clone(&repo) as Arc<dyn ITeamRepository>,
            Arc::new(SqliteAgentMetadataRepository::new(pool.clone())),
            Arc::new(EmptyCatalog),
            Arc::new(SqliteAssistantDefinitionRepository::new(pool.clone())),
            Arc::new(SqliteAssistantOverlayRepository::new(pool.clone())),
            Arc::new(SqliteProviderRepository::new(pool.clone())),
            Arc::clone(&port) as Arc<dyn TeamConversationProvisioningPort>,
            Arc::clone(&port) as Arc<dyn TeamProjectionMessageStore>,
            Arc::new(NullBroadcaster),
            Arc::new(NoopTaskManager),
            Arc::new(NoopTurnPort),
            Arc::new(NoopCancellationPort),
            Arc::new(PathBuf::from("/bin/true")),
        );

        let row = repo.get_team(USER, TEAM).await.unwrap().unwrap();
        let team = Team::from_row(&row).unwrap();
        let session = Arc::new(
            TeamSession::start(
                team,
                Arc::clone(&repo) as Arc<dyn ITeamRepository>,
                broadcaster,
                Arc::new(PathBuf::from("/bin/true")),
                Arc::new(NoopTaskManager),
                Arc::new(NoopTurnPort),
                Arc::new(NoopCancellationPort),
                Arc::clone(&port) as Arc<dyn TeamProjectionMessageStore>,
                USER.to_owned(),
                Arc::downgrade(&svc),
            )
            .await
            .expect("TeamSession::start"),
        );

        Self {
            repo,
            session,
            port,
            _svc: svc,
            _db: db,
        }
    }

    /// Lead assigns `subject` to worker-1; worker completes it via the real MCP
    /// tool. Returns the task id.
    async fn worker_completes_assigned_task(&self, subject: &str) -> String {
        let cfg = self.session.mcp_stdio_config("worker-1");
        let mut lead = mcp_connect(cfg.port, &cfg.token, "lead-1").await;
        let resp = mcp_call_tool(
            &mut lead,
            1,
            "team_task_create",
            json!({ "subject": subject, "owner": "worker-1" }),
        )
        .await;
        assert!(!is_mcp_error(&resp), "task create failed: {resp}");
        let task_id = mcp_task_id(&resp);

        let mut worker = mcp_connect(cfg.port, &cfg.token, "worker-1").await;
        let resp = mcp_call_tool(
            &mut worker,
            2,
            "team_task_update",
            json!({ "task_id": task_id, "status": "completed" }),
        )
        .await;
        assert!(!is_mcp_error(&resp), "task complete failed: {resp}");
        task_id
    }

    async fn task_result(&self, task_id: &str) -> Option<String> {
        self.repo
            .find_task_by_id(USER, TEAM, task_id)
            .await
            .unwrap()
            .expect("task exists")
            .result
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// The assignee's turn-final assistant text becomes the completed task's
/// `result`; nothing is written at the tool call itself.
#[tokio::test]
async fn assignee_turn_finalize_captures_task_result() {
    let h = Harness::new().await;
    let task_id = h.worker_completes_assigned_task("Build the thing").await;

    // At the tool call the assistant text was not final yet → no result.
    assert_eq!(
        h.task_result(&task_id).await,
        None,
        "result must NOT be captured at the completion tool call"
    );

    h.port.set_text("conv-worker", "Done: feature shipped.");
    let _wake = h
        .session
        .on_agent_finish("conv-worker", false)
        .await
        .expect("turn finalize still succeeds");

    assert_eq!(
        h.task_result(&task_id).await.as_deref(),
        Some("Done: feature shipped."),
        "finalize captures the assignee's final assistant text"
    );

    h.session.stop();
}

/// Legacy/hierarchical no-op: a slot completes a task but no assistant text is
/// readable (test double has none) → `result` stays NULL and finalize is
/// unaffected.
#[tokio::test]
async fn finalize_without_assistant_text_leaves_result_null() {
    let h = Harness::new().await;
    let task_id = h.worker_completes_assigned_task("Investigate").await;

    let _wake = h
        .session
        .on_agent_finish("conv-worker", false)
        .await
        .expect("finalize succeeds even with no assistant text");

    assert_eq!(
        h.task_result(&task_id).await,
        None,
        "capture must be a no-op when no assistant text exists"
    );

    h.session.stop();
}

// ── Task 3: materialize ready `input_context` + inject into the wake ─────────
//
// Real `SqliteTeamRepository` + real `TaskBoard` pinned to an engagement. A
// completed upstream task's captured `result` must flow into the downstream's
// `input_context` when it transitions to ready, and the wake renderer must
// surface it under a `## Upstream Results` section. No upstream → context stays
// NULL and the wake output is byte-identical to the baseline.

const CTX_USER: &str = "u-ctx";
const CTX_TEAM: &str = "t-ctx";

async fn ctx_board() -> (TaskBoard, Arc<SqliteTeamRepository>, aionui_db::Database) {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
    repo.create_team(&TeamRow {
        id: CTX_TEAM.to_owned(),
        user_id: CTX_USER.to_owned(),
        name: CTX_TEAM.to_owned(),
        workspace: String::new(),
        workspace_mode: "shared".to_owned(),
        agents: "[]".to_owned(),
        lead_agent_id: None,
        session_mode: None,
        agents_version: "1.0.1".to_owned(),
        created_at: now_ms(),
        updated_at: now_ms(),
        project_id: None,
        folder_id: None,
    })
    .await
    .unwrap();
    let e = repo
        .find_or_create_engagement(CTX_USER, CTX_TEAM, "proj-ctx", "/ws/ctx")
        .await
        .unwrap();
    let board =
        TaskBoard::new_for_user(repo.clone() as Arc<dyn ITeamRepository>, CTX_USER).with_engagement(e.id.clone());
    (board, repo, db)
}

/// Completing an upstream task with a captured result materializes that result
/// into the now-ready downstream's `input_context`.
#[tokio::test]
async fn ready_downstream_materializes_upstream_result_into_input_context() {
    let (board, repo, _db) = ctx_board().await;
    let a = board.create_task(CTX_TEAM, "A", None, None, &[], None).await.unwrap();
    let b = board
        .create_task(
            CTX_TEAM,
            "Implement B",
            Some("the downstream task"),
            None,
            std::slice::from_ref(&a.id),
            None,
        )
        .await
        .unwrap();

    repo.set_task_result(CTX_USER, &a.id, "A did X").await.unwrap();
    board
        .update_task(
            CTX_TEAM,
            &a.id,
            &TaskUpdate {
                status: Some(TaskStatus::Completed),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let b_row = repo.find_task_by_id(CTX_USER, CTX_TEAM, &b.id).await.unwrap().unwrap();
    let ctx = b_row
        .input_context
        .expect("ready downstream with a completed-result upstream gets an input_context");
    assert!(ctx.contains("A did X"), "must carry the upstream result:\n{ctx}");
    assert!(ctx.contains(&a.id), "must name the upstream dep id:\n{ctx}");
    assert!(ctx.contains("[[UPSTREAM]]"), "must have an upstream section:\n{ctx}");
}

/// A >cap upstream result is bounded to `MAX_INPUT_CONTEXT_CHARS`.
#[tokio::test]
async fn input_context_is_capped_at_max_chars() {
    let (board, repo, _db) = ctx_board().await;
    let a = board.create_task(CTX_TEAM, "A", None, None, &[], None).await.unwrap();
    let b = board
        .create_task(CTX_TEAM, "B", None, None, std::slice::from_ref(&a.id), None)
        .await
        .unwrap();

    let huge = "x".repeat(20_000);
    repo.set_task_result(CTX_USER, &a.id, &huge).await.unwrap();
    board
        .update_task(
            CTX_TEAM,
            &a.id,
            &TaskUpdate {
                status: Some(TaskStatus::Completed),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let ctx = repo
        .find_task_by_id(CTX_USER, CTX_TEAM, &b.id)
        .await
        .unwrap()
        .unwrap()
        .input_context
        .expect("cap path still records the (truncated) upstream result");
    assert!(ctx.len() <= 8000, "must be capped at 8000 bytes, got {}", ctx.len());
    assert!(ctx.contains("[[UPSTREAM]]"), "capped context keeps the section:\n{ctx}");
}

/// A downstream that becomes ready with NO upstream result stays NULL (no
/// context write) — hierarchical/legacy teams are unaffected.
#[tokio::test]
async fn ready_without_completed_upstream_result_leaves_input_context_null() {
    let (board, repo, _db) = ctx_board().await;
    let a = board.create_task(CTX_TEAM, "A", None, None, &[], None).await.unwrap();
    let b = board
        .create_task(CTX_TEAM, "B", None, None, std::slice::from_ref(&a.id), None)
        .await
        .unwrap();

    // A completes WITHOUT a captured result (legacy / no-member turn).
    board
        .update_task(
            CTX_TEAM,
            &a.id,
            &TaskUpdate {
                status: Some(TaskStatus::Completed),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let b_row = repo.find_task_by_id(CTX_USER, CTX_TEAM, &b.id).await.unwrap().unwrap();
    let blocked_by: Vec<String> = serde_json::from_str(&b_row.blocked_by).unwrap();
    assert!(blocked_by.is_empty(), "B is unblocked by A's completion");
    assert!(
        b_row.input_context.is_none(),
        "no upstream result → no context materialized, got {:?}",
        b_row.input_context
    );
}

// -- Wake renderer injection --------------------------------------------------

fn wake_agent() -> TeamAgent {
    TeamAgent {
        slot_id: "w1".into(),
        name: "Worker".into(),
        role: TeammateRole::Teammate,
        conversation_id: "conv-w1".into(),
        backend: "acp".into(),
        model: "claude".into(),
        assistant_id: None,
        status: None,
        conversation_type: None,
        cli_path: None,
    }
}

fn roster(ids: &[&str]) -> HashSet<String> {
    ids.iter().map(|id| (*id).to_owned()).collect()
}

#[test]
fn wake_payload_renders_upstream_results_section() {
    let ctx = "[[BRIEF]]\nB\n\n[[UPSTREAM]]\n- aaa11111: A did X\n";
    let payload = build_wake_payload(&wake_agent(), &[], &[], &roster(&["w1"]), Some(ctx));
    assert!(
        payload.contains("## Upstream Results"),
        "section header present:\n{payload}"
    );
    assert!(payload.contains("A did X"), "upstream text carried:\n{payload}");
}

#[test]
fn wake_payload_without_upstream_is_unchanged() {
    let none = build_wake_payload(&wake_agent(), &[], &[], &roster(&["w1"]), None);
    let empty = build_wake_payload(&wake_agent(), &[], &[], &roster(&["w1"]), Some(""));
    assert!(
        !none.contains("## Upstream Results"),
        "no upstream → no section:\n{none}"
    );
    assert_eq!(
        none, empty,
        "None and empty render identically (byte-identical baseline)"
    );
}
