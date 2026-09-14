//! Phase 3b Task 1 — DAG acyclicity on `create_task` / `update_task` (spec §7.3).
//!
//! Today the board validates that `blocked_by` deps EXIST but not that the
//! resulting graph is acyclic, so mutually-blocking tasks can deadlock. These
//! black-box tests run against a real SQLite DB (same harness style as
//! `task_board_integration.rs`) and assert the SPECIFIC `CyclicDependency`
//! variant (not merely `is_err()`, per AGENTS.md bad-path rule) plus that the
//! rejected update leaves the stored `blocked_by` byte-unchanged.
//!
//! Phase 3b Task 3 — `sequential` process mode (spec §7.2): exactly one
//! `InProgress` task per engagement, with deterministic next-pick, enforced at
//! the member's in-progress transition (`TeammateManager::update_task`);
//! `hierarchical` (default) still runs concurrent in-progress tasks unchanged.

use std::sync::Arc;

use aionui_api_types::WebSocketMessage;
use aionui_common::now_ms;
use aionui_db::models::TeamRow;
use aionui_db::{ITeamRepository, SqliteTeamRepository, init_database_memory};
use aionui_realtime::EventBroadcaster;
use aionui_team::{
    Mailbox, TaskBoard, TaskProcess, TaskStatus, TaskUpdate, TeamAgent, TeamError, TeammateManager, TeammateRole,
};

const USER: &str = "system_default_user";
const TEAM: &str = "t-dag";

struct NullBroadcaster;
impl EventBroadcaster for NullBroadcaster {
    fn broadcast(&self, _msg: WebSocketMessage<serde_json::Value>) {}
}

async fn setup() -> (TaskBoard, Arc<SqliteTeamRepository>, aionui_db::Database) {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
    create_team(&repo).await;
    let board = TaskBoard::new_for_user(repo.clone() as Arc<dyn ITeamRepository>, USER);
    (board, repo, db)
}

async fn create_team(repo: &Arc<SqliteTeamRepository>) {
    repo.create_team(&TeamRow {
        id: TEAM.to_owned(),
        user_id: USER.to_owned(),
        name: TEAM.to_owned(),
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
}

/// A manager pinned to its own engagement board ("e-<process>"), sharing one
/// repo with the caller, with a single `worker-1` teammate.
fn manager_for(board: Arc<TaskBoard>, repo: Arc<SqliteTeamRepository>, process: TaskProcess) -> TeammateManager {
    let agents = vec![TeamAgent {
        slot_id: "worker-1".into(),
        name: "Worker1".into(),
        role: TeammateRole::Teammate,
        conversation_id: "conv-worker-1".into(),
        backend: "acp".into(),
        model: "claude".into(),
        assistant_id: None,
        status: None,
        conversation_type: None,
        cli_path: None,
    }];
    TeammateManager::new(
        TEAM.to_owned(),
        USER.to_owned(),
        &agents,
        Arc::new(Mailbox::new_for_user(repo as Arc<dyn ITeamRepository>, USER)),
        board,
        Arc::new(NullBroadcaster),
        process,
    )
}

async fn sequential_setup(process: TaskProcess) -> (TeammateManager, Arc<TaskBoard>, aionui_db::Database) {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
    create_team(&repo).await;
    // Team-scoped board stands in for one engagement's board (the runtime pins
    // it per-engagement at session start; the sequential gate is driven by the
    // manager's process, not by the pin).
    let board = Arc::new(TaskBoard::new_for_user(repo.clone() as Arc<dyn ITeamRepository>, USER));
    let mgr = manager_for(board.clone(), repo, process);
    (mgr, board, db)
}

// -- Update path: the case where real cycles appear ---------------------------

#[tokio::test]
async fn update_adding_back_edge_is_rejected_and_leaves_edges_unchanged() {
    let (board, repo, _db) = setup().await;

    let a = board.create_task(TEAM, "A", None, None, &[], None).await.unwrap();
    let b = board
        .create_task(TEAM, "B", None, None, std::slice::from_ref(&a.id), None)
        .await
        .unwrap();
    // A is currently free; B is blocked by A. A -> B edge would close a cycle.
    assert!(a.blocked_by.is_empty());

    let result = board
        .update_task(
            TEAM,
            &a.id,
            &TaskUpdate {
                blocked_by: Some(vec![b.id.clone()]),
                ..Default::default()
            },
        )
        .await;

    match result {
        Err(TeamError::CyclicDependency {
            ref task_id,
            ref dependency,
        }) => {
            assert_eq!(task_id, &a.id);
            assert_eq!(dependency, &b.id);
        }
        other => panic!("expected CyclicDependency, got {other:?}"),
    }

    // Rejected without writing: A's stored blocked_by must still be empty.
    let stored = repo.find_task_by_id(USER, TEAM, &a.id).await.unwrap().unwrap();
    assert_eq!(stored.blocked_by, "[]", "rejected update must not mutate edges");
}

#[tokio::test]
async fn update_self_edge_is_rejected() {
    let (board, repo, _db) = setup().await;
    let a = board.create_task(TEAM, "A", None, None, &[], None).await.unwrap();

    let result = board
        .update_task(
            TEAM,
            &a.id,
            &TaskUpdate {
                blocked_by: Some(vec![a.id.clone()]),
                ..Default::default()
            },
        )
        .await;

    match result {
        Err(TeamError::CyclicDependency {
            ref task_id,
            ref dependency,
        }) => {
            assert_eq!(task_id, &a.id);
            assert_eq!(dependency, &a.id, "self-edge dependency is the task itself");
        }
        other => panic!("expected CyclicDependency, got {other:?}"),
    }

    let stored = repo.find_task_by_id(USER, TEAM, &a.id).await.unwrap().unwrap();
    assert_eq!(stored.blocked_by, "[]", "rejected self-edge update must not persist");
}

#[tokio::test]
async fn update_longer_cycle_is_rejected() {
    let (board, _repo, _db) = setup().await;
    let a = board.create_task(TEAM, "A", None, None, &[], None).await.unwrap();
    let b = board
        .create_task(TEAM, "B", None, None, std::slice::from_ref(&a.id), None)
        .await
        .unwrap();
    let c = board
        .create_task(TEAM, "C", None, None, std::slice::from_ref(&b.id), None)
        .await
        .unwrap();

    // A blocked by C would close A -> B -> C -> A.
    let result = board
        .update_task(
            TEAM,
            &a.id,
            &TaskUpdate {
                blocked_by: Some(vec![c.id.clone()]),
                ..Default::default()
            },
        )
        .await;
    let err = result.expect_err("cyclic update must be rejected");
    match err {
        TeamError::CyclicDependency { task_id, dependency } => {
            assert_eq!(task_id, a.id);
            assert_eq!(dependency, c.id);
        }
        other => panic!("expected CyclicDependency, got {other:?}"),
    }
}

// -- Positive cases: valid DAGs must still be accepted ------------------------

#[tokio::test]
async fn linear_chain_is_accepted() {
    let (board, _repo, _db) = setup().await;
    let a = board.create_task(TEAM, "A", None, None, &[], None).await.unwrap();
    let b = board
        .create_task(TEAM, "B", None, None, std::slice::from_ref(&a.id), None)
        .await
        .unwrap();
    let c = board
        .create_task(TEAM, "C", None, None, std::slice::from_ref(&b.id), None)
        .await
        .unwrap();
    // A -> B -> C is a valid DAG; adding it via create must not be flagged.
    assert_eq!(c.blocked_by, vec![b.id.clone()]);
}

#[tokio::test]
async fn diamond_is_accepted() {
    let (board, _repo, _db) = setup().await;
    let a = board.create_task(TEAM, "A", None, None, &[], None).await.unwrap();
    let b = board
        .create_task(TEAM, "B", None, None, std::slice::from_ref(&a.id), None)
        .await
        .unwrap();
    let c = board
        .create_task(TEAM, "C", None, None, std::slice::from_ref(&a.id), None)
        .await
        .unwrap();
    // D blocked by both B and C: a diamond, not a cycle.
    let d = board
        .create_task(TEAM, "D", None, None, &[b.id.clone(), c.id.clone()], None)
        .await
        .unwrap();
    assert_eq!(d.blocked_by.len(), 2);
}

#[tokio::test]
async fn update_replacing_blocked_by_with_valid_edges_is_accepted() {
    let (board, _repo, _db) = setup().await;
    let a = board.create_task(TEAM, "A", None, None, &[], None).await.unwrap();
    let b = board.create_task(TEAM, "B", None, None, &[], None).await.unwrap();
    let c = board
        .create_task(TEAM, "C", None, None, std::slice::from_ref(&a.id), None)
        .await
        .unwrap();

    // Re-point C from A onto B: still acyclic.
    let updated = board
        .update_task(
            TEAM,
            &c.id,
            &TaskUpdate {
                blocked_by: Some(vec![b.id.clone()]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.blocked_by, vec![b.id]);
}

// -- Phase 3b Task 3: `sequential` — exactly one in-progress task -------------

#[tokio::test]
async fn sequential_engagement_allows_only_one_in_progress_task() {
    let (mgr, board, _db) = sequential_setup(TaskProcess::Sequential).await;

    let a = board
        .create_task(TEAM, "A", None, Some("worker-1"), &[], None)
        .await
        .unwrap();
    let b = board
        .create_task(TEAM, "B", None, Some("worker-1"), &[], None)
        .await
        .unwrap();

    // A starts; it is the engagement's single in-progress task, B stays Pending.
    let started = mgr
        .update_task(&a.id, Some("in_progress"), None, None, None)
        .await
        .unwrap();
    assert_eq!(started.status, TaskStatus::InProgress);
    let current = board.in_progress_task(TEAM).await.unwrap();
    assert_eq!(current.as_ref().map(|t| t.id.as_str()), Some(a.id.as_str()));
    let next = board.next_sequential_ready(TEAM).await.unwrap();
    assert_eq!(next.as_ref().map(|t| t.id.as_str()), Some(b.id.as_str()));

    // Starting B while A is in progress must be rejected with the typed busy
    // error, and B must stay Pending (only one task in progress).
    let err = mgr
        .update_task(&b.id, Some("in_progress"), None, None, None)
        .await
        .expect_err("second in_progress start must be rejected in sequential mode");
    match err {
        TeamError::SequentialBusy {
            ref task_id,
            ref current_task_id,
        } => {
            assert_eq!(task_id, &b.id);
            assert_eq!(current_task_id, &a.id);
        }
        other => panic!("expected SequentialBusy, got {other:?}"),
    }
    assert_eq!(
        board
            .list_tasks(TEAM)
            .await
            .unwrap()
            .into_iter()
            .find(|t| t.id == b.id)
            .unwrap()
            .status,
        TaskStatus::Pending,
        "rejected start must leave B pending"
    );
    let current = board.in_progress_task(TEAM).await.unwrap();
    assert_eq!(current.as_ref().map(|t| t.id.as_str()), Some(a.id.as_str()));

    // Re-marking the already-in-progress task A is idempotent (not a busy error).
    mgr.update_task(&a.id, Some("in_progress"), None, None, None)
        .await
        .expect("same task re-marking itself in_progress is allowed");

    // Completing A clears the in-progress slot; B is the sole next ready task
    // and can now start.
    let completed = mgr
        .update_task(&a.id, Some("completed"), None, None, None)
        .await
        .unwrap();
    assert_eq!(completed.status, TaskStatus::Completed);
    assert!(board.in_progress_task(TEAM).await.unwrap().is_none());
    let next = board.next_sequential_ready(TEAM).await.unwrap();
    assert_eq!(next.as_ref().map(|t| t.id.as_str()), Some(b.id.as_str()));
    let started_b = mgr
        .update_task(&b.id, Some("in_progress"), None, None, None)
        .await
        .unwrap();
    assert_eq!(started_b.status, TaskStatus::InProgress);
}

#[tokio::test]
async fn next_sequential_ready_skips_blocked_and_picks_oldest() {
    let (_mgr, board, _db) = sequential_setup(TaskProcess::Sequential).await;

    let b = board
        .create_task(TEAM, "B", None, Some("worker-1"), &[], None)
        .await
        .unwrap();
    let c = board
        .create_task(TEAM, "C", None, Some("worker-1"), std::slice::from_ref(&b.id), None)
        .await
        .unwrap();
    assert!(!c.blocked_by.is_empty());

    let next = board.next_sequential_ready(TEAM).await.unwrap();
    assert_eq!(
        next.as_ref().map(|t| t.id.as_str()),
        Some(b.id.as_str()),
        "blocked C must not be picked"
    );

    board
        .update_task(
            TEAM,
            &b.id,
            &TaskUpdate {
                status: Some(TaskStatus::InProgress),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(
        board.next_sequential_ready(TEAM).await.unwrap().is_none(),
        "no pending-ready task remains while B in-progress blocks C"
    );
}

#[tokio::test]
async fn hierarchical_default_allows_concurrent_in_progress_tasks() {
    let (mgr, board, _db) = sequential_setup(TaskProcess::Hierarchical).await;

    let a = board
        .create_task(TEAM, "A", None, Some("worker-1"), &[], None)
        .await
        .unwrap();
    let b = board
        .create_task(TEAM, "B", None, Some("worker-1"), &[], None)
        .await
        .unwrap();

    mgr.update_task(&a.id, Some("in_progress"), None, None, None)
        .await
        .expect("hierarchical: first start allowed");
    mgr.update_task(&b.id, Some("in_progress"), None, None, None)
        .await
        .expect("hierarchical: a second concurrent in_progress task must remain allowed (default unchanged)");

    let statuses: Vec<TaskStatus> = board
        .list_tasks(TEAM)
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.status)
        .collect();
    assert_eq!(
        statuses.iter().filter(|s| **s == TaskStatus::InProgress).count(),
        2,
        "both tasks run concurrently under the default process"
    );
}
