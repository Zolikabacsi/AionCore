//! Phase 2a task 4 — engagement routing.
//!
//! Proves the runtime no longer leaves `engagement_id` NULL on session writes:
//! a task and a mail created through a live `TeamSession` carry that session's
//! resolved engagement, and two different projects' sessions of the SAME team
//! key their writes to DISTINCT engagements.
//!
//! Uses a real `SqliteTeamRepository` (the mock repos cannot mint distinct
//! engagement ids — their `find_or_create_engagement` is unimplemented, which
//! is exactly why the session resolver falls back to `team_id` for legacy
//! single-engagement teams).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};

use aionui_ai_agent::AgentError;
use aionui_ai_agent::IWorkerTaskManager;
use aionui_ai_agent::agent_task::AgentInstance;
use aionui_ai_agent::types::BuildTaskOptions;
use aionui_api_types::WebSocketMessage;
use aionui_common::{AgentKillReason, TimestampMs, now_ms};
use aionui_db::models::MessageRow;
use aionui_db::{ITeamRepository, SqliteTeamRepository, init_database_memory};
use aionui_realtime::EventBroadcaster;
use aionui_team::ports::{
    AgentTurnCancellationPort, AgentTurnExecutionError, AgentTurnExecutionPort, AgentTurnOutcome, AgentTurnRequest,
    AgentTurnStatus,
};
use aionui_team::types::{Team, TeamAgent, TeammateRole};
use aionui_team::{TeamError, TeamProjectionMessageStore, TeamSession, TeamSessionService};
use async_trait::async_trait;
use serde_json::Value;

// ── Minimal no-op ports (session is only used for task/mail writes) ──────────

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

struct NoopProjectionStore;
#[async_trait]
impl TeamProjectionMessageStore for NoopProjectionStore {
    fn mint_message_id(&self) -> String {
        "msg-noop".into()
    }
    async fn find_projected_message(&self, _c: &str, _m: &str, _t: &str) -> Result<Option<MessageRow>, TeamError> {
        Ok(None)
    }
    async fn insert_projected_message(&self, _row: &MessageRow) -> Result<(), TeamError> {
        Ok(())
    }
}

#[derive(Default)]
struct StubTaskManager {
    tasks: Mutex<HashMap<String, AgentInstance>>,
}
#[async_trait]
impl IWorkerTaskManager for StubTaskManager {
    fn get_task(&self, conversation_id: &str) -> Option<AgentInstance> {
        self.tasks.lock().unwrap().get(conversation_id).cloned()
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

// ── Fixtures ────────────────────────────────────────────────────────────────

fn lead_agent() -> TeamAgent {
    TeamAgent {
        slot_id: "lead-1".into(),
        name: "Lead".into(),
        role: TeammateRole::Lead,
        conversation_id: "conv-lead".into(),
        backend: "acp".into(),
        model: "test".into(),
        assistant_id: None,
        status: None,
        conversation_type: None,
        cli_path: None,
    }
}

/// Seed a real `teams` row (parent-check triggers + repo ownership guards
/// require it) and return a domain `Team` bound to `project_id`.
async fn seed_team(repo: &Arc<SqliteTeamRepository>, team_id: &str, user: &str, project_id: Option<&str>) {
    let agents = serde_json::to_string(&vec![lead_agent()]).unwrap();
    repo.create_team(&aionui_db::models::TeamRow {
        id: team_id.into(),
        user_id: user.into(),
        name: "routing".into(),
        workspace: "/tmp/routing".into(),
        workspace_mode: "shared".into(),
        agents,
        lead_agent_id: Some("lead-1".into()),
        session_mode: None,
        agents_version: "1.0.1".into(),
        created_at: now_ms(),
        updated_at: now_ms(),
        project_id: project_id.map(str::to_owned),
        folder_id: None,
    })
    .await
    .unwrap();
}

fn team_domain(team_id: &str, project_id: Option<&str>) -> Team {
    Team {
        id: team_id.into(),
        name: "routing".into(),
        workspace: "/tmp/routing".into(),
        agents: vec![lead_agent()],
        lead_agent_id: Some("lead-1".into()),
        project_id: project_id.map(str::to_owned),
        created_at: now_ms(),
        updated_at: now_ms(),
    }
}

async fn start_session(repo: Arc<dyn ITeamRepository>, team: Team, user: &str) -> TeamSession {
    TeamSession::start(
        team,
        repo,
        Arc::new(NullBroadcaster),
        Arc::new(PathBuf::from("/bin/true")),
        Arc::new(StubTaskManager::default()),
        Arc::new(NoopTurnPort),
        Arc::new(NoopCancellationPort),
        Arc::new(NoopProjectionStore),
        user.into(),
        Weak::<TeamSessionService>::new(),
    )
    .await
    .expect("TeamSession::start")
}

// ── Tests ───────────────────────────────────────────────────────────────────

/// A task + a mail created during a session carry that session's engagement id.
#[tokio::test]
async fn session_task_and_mail_carry_active_engagement() {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
    let user = "u1";
    seed_team(&repo, "team-r", user, Some("proj-a")).await;
    let expected = repo
        .find_or_create_engagement(user, "team-r", "proj-a", "/tmp/routing")
        .await
        .unwrap()
        .id;

    let session = start_session(repo.clone(), team_domain("team-r", Some("proj-a")), user).await;
    assert_eq!(
        session.engagement_id(),
        expected,
        "session resolves the project's engagement"
    );

    let task = session
        .scheduler()
        .create_task("Ship it", None, None, &[])
        .await
        .unwrap();
    let mail = session
        .mailbox()
        .write(
            "team-r",
            "lead-1",
            "user",
            aionui_team::types::MailboxMessageType::Message,
            "hi",
            None,
        )
        .await
        .unwrap();

    let stored_task = repo.find_task_by_id(user, "team-r", &task.id).await.unwrap().unwrap();
    assert_eq!(stored_task.engagement_id.as_deref(), Some(expected.as_str()));
    let stored_msgs = repo.list_messages_by_engagement(user, &expected).await.unwrap();
    assert!(
        stored_msgs.iter().any(|m| m.id == mail.id),
        "mail written under the session engagement"
    );

    session.stop();
}

/// Two different projects' sessions of the same team key to distinct engagements.
#[tokio::test]
async fn two_projects_key_to_distinct_engagements() {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
    let user = "u1";
    // team row itself has no project (mirrors multi-engagement teams where the
    // engagement, not the team column, scopes the runtime).
    seed_team(&repo, "team-m", user, None).await;
    let eng_a = repo
        .find_or_create_engagement(user, "team-m", "proj-a", "/tmp/a")
        .await
        .unwrap()
        .id;
    let eng_b = repo
        .find_or_create_engagement(user, "team-m", "proj-b", "/tmp/b")
        .await
        .unwrap()
        .id;
    assert_ne!(eng_a, eng_b);

    // The session resolver derives the engagement from the team's project; start
    // one session per project (overriding team.project_id via the domain Team).
    let sess_a = start_session(repo.clone(), team_domain("team-m", Some("proj-a")), user).await;
    let sess_b = start_session(repo.clone(), team_domain("team-m", Some("proj-b")), user).await;
    assert_eq!(sess_a.engagement_id(), eng_a);
    assert_eq!(sess_b.engagement_id(), eng_b);
    assert_ne!(sess_a.engagement_id(), sess_b.engagement_id());

    let task_a = sess_a.scheduler().create_task("A task", None, None, &[]).await.unwrap();
    let task_b = sess_b.scheduler().create_task("B task", None, None, &[]).await.unwrap();
    sess_a
        .mailbox()
        .write(
            "team-m",
            "lead-1",
            "user",
            aionui_team::types::MailboxMessageType::Message,
            "a mail",
            None,
        )
        .await
        .unwrap();
    sess_b
        .mailbox()
        .write(
            "team-m",
            "lead-1",
            "user",
            aionui_team::types::MailboxMessageType::Message,
            "b mail",
            None,
        )
        .await
        .unwrap();

    let tasks_a = repo.list_tasks_by_engagement(user, &eng_a).await.unwrap();
    let tasks_b = repo.list_tasks_by_engagement(user, &eng_b).await.unwrap();
    assert_eq!(
        tasks_a.iter().map(|t| t.id.clone()).collect::<Vec<_>>(),
        vec![task_a.id.clone()]
    );
    assert_eq!(
        tasks_b.iter().map(|t| t.id.clone()).collect::<Vec<_>>(),
        vec![task_b.id.clone()]
    );

    let msgs_a = repo.list_messages_by_engagement(user, &eng_a).await.unwrap();
    let msgs_b = repo.list_messages_by_engagement(user, &eng_b).await.unwrap();
    assert_eq!(msgs_a.len(), 1, "project A engagement has exactly its own mail");
    assert_eq!(msgs_b.len(), 1, "project B engagement has exactly its own mail");
    assert_ne!(msgs_a[0].content, msgs_b[0].content);

    sess_a.stop();
    sess_b.stop();
}

/// Legacy single-engagement team (no project) resolves to `engagement_id == team_id`
/// — i.e. exactly the pre-Phase-2a behaviour, just now explicitly stamped.
#[tokio::test]
async fn legacy_no_project_team_resolves_to_team_id() {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
    let user = "u1";
    seed_team(&repo, "team-legacy", user, None).await;

    let session = start_session(repo.clone(), team_domain("team-legacy", None), user).await;
    assert_eq!(
        session.engagement_id(),
        "team-legacy",
        "legacy team engagement key == team_id"
    );

    let task = session
        .scheduler()
        .create_task("legacy", None, None, &[])
        .await
        .unwrap();
    let stored = repo
        .find_task_by_id(user, "team-legacy", &task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.engagement_id.as_deref(), Some("team-legacy"));

    session.stop();
}
