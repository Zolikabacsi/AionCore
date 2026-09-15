//! Phase 4b Task 1 — consolidated engagement result return (spec §8/§10).
//!
//! When the ROOT task created by `convene_delegated_task` completes and its
//! result is captured at turn finalize, the consolidated result is delivered
//! to the delegating caller conversation through the `DelegatedResultDelivery`
//! port (best-effort: failures warn, never break finalize). Tasks WITHOUT the
//! convene-stamped `delegate_reply_to` metadata (ordinary team tasks, legacy
//! roots convened without a reply target) must NEVER trigger a delivery.
//!
//! Wiring mirrors `task_context_passing.rs`: real `SqliteTeamRepository` over
//! `init_database_memory()` + real `TeamSessionService`/`TeamSession` + real
//! MCP `team_task_update`; only the composition-layer seams
//! (`TeamConversationProvisioningPort`, `DelegatedResultDelivery`) are doubles.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aionui_ai_agent::agent_task::AgentInstance;
use aionui_ai_agent::types::BuildTaskOptions;
use aionui_ai_agent::{AgentError, IWorkerTaskManager};
use aionui_api_types::{GetConfigOptionsResponse, TeamMcpSelection, WebSocketMessage};
use aionui_common::{AgentKillReason, TimestampMs, now_ms};
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
    DelegatedResultDelivery, TeamAgent, TeamConversationCreateRequest, TeamConversationCreateResult,
    TeamConversationProvisioningPort, TeamError, TeamMcpSnapshotResolution, TeamProjectionMessageStore, TeamSession,
    TeamSessionService, TeammateRole,
};
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::net::TcpStream;

const USER: &str = "u-ret";
const TEAM: &str = "t-ret";
const CALLER_CONV: &str = "caller-conv";

// ── Test doubles: composition-layer seams only ─────────────────────────────

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

/// Provisioning seam + the `latest_assistant_text` read the 3a capture path
/// uses. Conversation ids are recorded so the test can set the lead's final
/// assistant text by the id the engagement member was materialized with.
#[derive(Default)]
struct RecordingPort {
    extras: Mutex<HashMap<String, serde_json::Value>>,
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

fn unique_temp_dir(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{prefix}-{}", aionui_common::generate_id()))
}

#[async_trait]
impl TeamConversationProvisioningPort for RecordingPort {
    async fn create_team_conversation(
        &self,
        request: TeamConversationCreateRequest,
    ) -> Result<TeamConversationCreateResult, TeamError> {
        let id = aionui_common::generate_id();
        let mut extra = request.extra;
        let workspace = match extra.get("workspace").and_then(serde_json::Value::as_str) {
            Some(value) if !value.trim().is_empty() => value.to_owned(),
            _ => self.create_team_temp_workspace(&request.user_id, "conv").await?,
        };
        extra["workspace"] = serde_json::Value::String(workspace.clone());
        self.extras.lock().unwrap().insert(id.clone(), extra);
        Ok(TeamConversationCreateResult {
            conversation_id: id,
            workspace,
        })
    }

    async fn conversation_workspace(&self, conversation_id: &str) -> Result<Option<String>, TeamError> {
        Ok(self
            .extras
            .lock()
            .unwrap()
            .get(conversation_id)
            .and_then(|extra| extra.get("workspace"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned))
    }

    async fn conversation_assistant_id(&self, conversation_id: &str) -> Result<Option<String>, TeamError> {
        Ok(self.extras.lock().unwrap().get(conversation_id).and_then(|extra| {
            extra
                .get("assistant_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        }))
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

    async fn create_team_temp_workspace(&self, _user_id: &str, tag: &str) -> Result<String, TeamError> {
        let path = unique_temp_dir(&format!("delegated-result-temp-{tag}"));
        std::fs::create_dir_all(&path).unwrap();
        Ok(path.to_string_lossy().into_owned())
    }

    async fn patch_runtime_config(&self, _conversation_id: &str, _patch: serde_json::Value) -> Result<(), TeamError> {
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

    async fn delete_team_conversation(&self, _user_id: &str, conversation_id: &str) -> Result<(), TeamError> {
        self.extras
            .lock()
            .unwrap()
            .remove(conversation_id)
            .map(|_| ())
            .ok_or_else(|| TeamError::AgentNotFound(conversation_id.to_owned()))
    }

    async fn latest_assistant_text(&self, conversation_id: &str) -> Result<Option<String>, TeamError> {
        Ok(self.assistant_texts.lock().unwrap().get(conversation_id).cloned())
    }
}

#[async_trait]
impl TeamProjectionMessageStore for RecordingPort {
    fn mint_message_id(&self) -> String {
        aionui_common::generate_id()
    }
    async fn find_projected_message(&self, _c: &str, _m: &str, _t: &str) -> Result<Option<MessageRow>, TeamError> {
        Ok(None)
    }
    async fn insert_projected_message(&self, _row: &MessageRow) -> Result<(), TeamError> {
        Ok(())
    }
}

/// Records every `deliver_result` call; optional forced failure to prove the
/// hook is best-effort.
#[derive(Default)]
struct RecordingDelivery {
    calls: Mutex<Vec<(String, String, String, String)>>,
    fail: std::sync::atomic::AtomicBool,
}

impl RecordingDelivery {
    fn calls(&self) -> Vec<(String, String, String, String)> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl DelegatedResultDelivery for RecordingDelivery {
    async fn deliver_result(
        &self,
        user_id: &str,
        caller_conversation_id: &str,
        engagement_id: &str,
        text: &str,
    ) -> Result<(), TeamError> {
        self.calls.lock().unwrap().push((
            user_id.to_owned(),
            caller_conversation_id.to_owned(),
            engagement_id.to_owned(),
            text.to_owned(),
        ));
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(TeamError::Forbidden("delivery failed on purpose".into()));
        }
        Ok(())
    }
}

// ── MCP helpers (same pattern as task_context_passing.rs) ──────────────────

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
            "clientInfo": { "name": "delegated-result-test", "version": "0.1" }
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
    svc: Arc<TeamSessionService>,
    session: Arc<TeamSession>,
    port: Arc<RecordingPort>,
    delivery: Arc<RecordingDelivery>,
    lead_slot_id: String,
    lead_conversation_id: String,
    _db: aionui_db::Database,
}

impl Harness {
    /// Convene a root task (via the real 4a seam) with `reply_to`, then bring
    /// up the engagement's runtime session. `engagement_id` is the no-project
    /// sentinel, which resolves to the team's default engagement (id == team id)
    /// — the same engagement the session pins its board to.
    async fn new(reply_to: Option<&str>) -> Self {
        let db = init_database_memory().await.unwrap();
        let pool = db.pool().clone();
        sqlx::query(
            "INSERT INTO users (id, user_type, username, password_hash, status, session_generation, created_at, updated_at) \
             VALUES (?1, 'local', ?1, 'hash', 'active', 0, 1, 1)",
        )
        .bind(USER)
        .execute(&pool)
        .await
        .unwrap();

        let repo = Arc::new(SqliteTeamRepository::new(pool.clone()));
        let agents = two_agents();
        repo.create_team(&TeamRow {
            id: TEAM.to_owned(),
            user_id: USER.to_owned(),
            name: TEAM.to_owned(),
            workspace: "/tmp/delegated-result-return".to_owned(),
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
        let delivery = Arc::new(RecordingDelivery::default());
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
        svc.with_result_delivery(Arc::clone(&delivery) as Arc<dyn DelegatedResultDelivery>);

        let envelope = format!(
            "[[AION_DELEGATE]]\nenvelope_id: env-1\nreply_to: {CALLER_CONV}\n[[/AION_DELEGATE]]\n\nconsolidate the thing"
        );
        let convened = svc
            .convene_delegated_task(
                USER,
                TEAM,
                "__none__",
                "root subject",
                "description",
                None,
                &envelope,
                reply_to,
                0,
            )
            .await
            .expect("convene ok");
        assert_eq!(convened.engagement_id, TEAM, "sentinel convenes the team default");

        let row = repo.get_team(USER, TEAM).await.unwrap().unwrap();
        let team = Team::from_row(&row).unwrap();
        let session = Arc::new(
            TeamSession::start(
                team,
                Arc::clone(&repo) as Arc<dyn ITeamRepository>,
                Arc::new(NullBroadcaster),
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

        let members = repo
            .list_engagement_members(USER, &convened.engagement_id)
            .await
            .unwrap();
        let lead = members.iter().find(|m| m.role == "lead").expect("lead member");
        assert_eq!(convened.lead_slot_id, lead.slot_id);

        Self {
            repo,
            svc,
            session,
            port,
            delivery,
            lead_slot_id: lead.slot_id.clone(),
            lead_conversation_id: lead.conversation_id.clone(),
            _db: db,
        }
    }

    async fn task_row(&self, task_id: &str) -> aionui_db::models::TeamTaskRow {
        self.repo
            .find_task_by_engagement(USER, TEAM, task_id)
            .await
            .unwrap()
            .expect("task exists")
    }

    async fn complete_via_mcp(&self, task_id: &str) {
        let cfg = self.session.mcp_stdio_config(&self.lead_slot_id);
        let mut lead = mcp_connect(cfg.port, &cfg.token, &self.lead_slot_id).await;
        let resp = mcp_call_tool(
            &mut lead,
            1,
            "team_task_update",
            json!({ "task_id": task_id, "status": "completed" }),
        )
        .await;
        assert!(!is_mcp_error(&resp), "task complete failed: {resp}");
    }

    async fn create_via_mcp(&self, subject: &str) -> String {
        let cfg = self.session.mcp_stdio_config(&self.lead_slot_id);
        let mut lead = mcp_connect(cfg.port, &cfg.token, &self.lead_slot_id).await;
        let resp = mcp_call_tool(&mut lead, 1, "team_task_create", json!({ "subject": subject })).await;
        assert!(!is_mcp_error(&resp), "task create failed: {resp}");
        let text = resp["result"]["content"][0]["text"].as_str().unwrap_or("");
        let parsed: Value = serde_json::from_str(text).expect("tool text is JSON");
        parsed["task"]["task_id"].as_str().expect("task_id present").to_owned()
    }

    async fn finalize_lead(&self) {
        let _wake = self
            .session
            .on_agent_finish(&self.lead_conversation_id, false)
            .await
            .expect("finalize succeeds");
    }

    async fn wait_for_calls(&self, n: usize) {
        for _ in 0..100 {
            if self.delivery.calls.lock().unwrap().len() >= n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// Convening a delegated root with a reply target stamps the correlation into
/// the ROOT task's metadata, and completing it (result captured at finalize)
/// delivers the consolidated result to the caller conversation ONCE.
#[tokio::test]
async fn root_task_completion_delivers_consolidated_result_to_caller() {
    let h = Harness::new(Some(CALLER_CONV)).await;

    // Ruling 1: the convene seam associates root ↔ reply_to via metadata.
    let convened = h
        .svc
        .convene_delegated_task(
            USER,
            TEAM,
            "__none__",
            "again",
            "d",
            None,
            "[[AION_DELEGATE]]\nenvelope_id: env-2\n[[/AION_DELEGATE]]\n\nx",
            Some(CALLER_CONV),
            0,
        )
        .await
        .unwrap();
    let root = h.task_row(&convened.root_task_id).await;
    let metadata: Value = serde_json::from_str(root.metadata.as_deref().expect("metadata stamped")).unwrap();
    assert_eq!(metadata["delegate_reply_to"], json!(CALLER_CONV));
    assert_eq!(metadata["engagement_id"], json!(TEAM));

    h.complete_via_mcp(&convened.root_task_id).await;
    h.port.set_text(&h.lead_conversation_id, "TEAM CONSOLIDATED RESULT");
    h.finalize_lead().await;
    h.wait_for_calls(1).await;

    assert_eq!(
        h.delivery.calls(),
        vec![(
            USER.to_owned(),
            CALLER_CONV.to_owned(),
            TEAM.to_owned(),
            "TEAM CONSOLIDATED RESULT".to_owned()
        )],
        "deliver exactly once with the caller conversation and result text"
    );
    let done = h.task_row(&convened.root_task_id).await;
    assert_eq!(done.result.as_deref(), Some("TEAM CONSOLIDATED RESULT"));

    h.session.stop();
}

/// Control: a task WITHOUT `delegate_reply_to` metadata — an ordinary team
/// task and a legacy convene with no reply target — never triggers delivery,
/// and its result capture still works.
#[tokio::test]
async fn non_delegated_completions_never_deliver() {
    // Convene WITHOUT a reply target: no metadata, no delivery.
    let h = Harness::new(None).await;
    let convened = h
        .svc
        .convene_delegated_task(
            USER,
            TEAM,
            "__none__",
            "legacy root",
            "",
            None,
            "[[AION_DELEGATE]]\nenvelope_id: env-3\n[[/AION_DELEGATE]]\n\nx",
            None,
            0,
        )
        .await
        .unwrap();
    assert!(
        h.task_row(&convened.root_task_id).await.metadata.is_none(),
        "no reply target → no correlation metadata"
    );

    // An ordinary (agent-created) task on the same board.
    let ordinary = h.create_via_mcp("ordinary work").await;

    h.complete_via_mcp(&convened.root_task_id).await;
    h.complete_via_mcp(&ordinary).await;
    h.port.set_text(&h.lead_conversation_id, "NOTHING TO RETURN");
    h.finalize_lead().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(h.delivery.calls().is_empty(), "metadata-less tasks must not deliver");
    assert_eq!(
        h.task_row(&convened.root_task_id).await.result.as_deref(),
        Some("NOTHING TO RETURN")
    );

    h.session.stop();
}

/// A delivery failure is best-effort: it warns, never breaks finalize, and the
/// result is still persisted on the task.
#[tokio::test]
async fn delivery_failure_does_not_break_finalize() {
    let h = Harness::new(Some(CALLER_CONV)).await;
    h.delivery.fail.store(true, std::sync::atomic::Ordering::SeqCst);

    let convened = h
        .svc
        .convene_delegated_task(
            USER,
            TEAM,
            "__none__",
            "failing root",
            "",
            None,
            "[[AION_DELEGATE]]\nenvelope_id: env-4\n[[/AION_DELEGATE]]\n\nx",
            Some(CALLER_CONV),
            0,
        )
        .await
        .unwrap();

    h.complete_via_mcp(&convened.root_task_id).await;
    h.port.set_text(&h.lead_conversation_id, "RESULT DESPITE FAILURE");
    h.finalize_lead().await;
    h.wait_for_calls(1).await;

    assert_eq!(h.delivery.calls().len(), 1, "the attempt was made");
    assert_eq!(
        h.task_row(&convened.root_task_id).await.result.as_deref(),
        Some("RESULT DESPITE FAILURE"),
        "the result stays stored on the task"
    );

    h.session.stop();
}

/// No final assistant text → capture (and therefore delivery) is a no-op:
/// the §8 return follows the result, never precedes it.
#[tokio::test]
async fn finalize_without_assistant_text_delivers_nothing() {
    let h = Harness::new(Some(CALLER_CONV)).await;
    let convened = h
        .svc
        .convene_delegated_task(
            USER,
            TEAM,
            "__none__",
            "silent root",
            "",
            None,
            "[[AION_DELEGATE]]\nenvelope_id: env-5\n[[/AION_DELEGATE]]\n\nx",
            Some(CALLER_CONV),
            0,
        )
        .await
        .unwrap();

    h.complete_via_mcp(&convened.root_task_id).await;
    h.finalize_lead().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(h.delivery.calls().is_empty(), "no result captured → nothing returned");
    assert_eq!(h.task_row(&convened.root_task_id).await.result, None);

    h.session.stop();
}
