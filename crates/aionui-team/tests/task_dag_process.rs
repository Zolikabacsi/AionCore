//! Phase 3b Task 1 — DAG acyclicity on `create_task` / `update_task` (spec §7.3).
//!
//! Today the board validates that `blocked_by` deps EXIST but not that the
//! resulting graph is acyclic, so mutually-blocking tasks can deadlock. These
//! black-box tests run against a real SQLite DB (same harness style as
//! `task_board_integration.rs`) and assert the SPECIFIC `CyclicDependency`
//! variant (not merely `is_err()`, per AGENTS.md bad-path rule) plus that the
//! rejected update leaves the stored `blocked_by` byte-unchanged.

use std::sync::Arc;

use aionui_common::now_ms;
use aionui_db::models::TeamRow;
use aionui_db::{ITeamRepository, SqliteTeamRepository, init_database_memory};
use aionui_team::{TaskBoard, TaskUpdate, TeamError};

const USER: &str = "system_default_user";
const TEAM: &str = "t-dag";

async fn setup() -> (TaskBoard, Arc<SqliteTeamRepository>, aionui_db::Database) {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
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
    let board = TaskBoard::new_for_user(repo.clone() as Arc<dyn ITeamRepository>, USER);
    (board, repo, db)
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
