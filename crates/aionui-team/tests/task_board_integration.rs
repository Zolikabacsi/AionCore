//! Black-box integration tests for `TaskBoard` service.
//!
//! Exercises the service layer against a real SQLite database.
//!
//! Covers test-plan items:
//! - TK-1..TK-4 (create tasks: no deps, single dep, multi-dep, nonexistent dep)
//! - TU-1..TU-5 (update status, description, owner, nonexistent)
//! - CU-1..CU-4 (check_unblocks: single, multiple, partial, no downstream)
//! - TT-1..TT-3 (list tasks, empty, with deps)
//! - DC-4 (blockedBy/blocks bidirectional consistency)

use std::sync::Arc;

use aionui_common::now_ms;
use aionui_db::models::{TeamRow, TeamTaskRow};
use aionui_db::{ITeamRepository, SqliteTeamRepository, init_database_memory};
use aionui_team::{TaskBoard, TaskStatus, TaskUpdate, TeamError};

const USER: &str = "system_default_user";

async fn repo_with_team() -> (Arc<SqliteTeamRepository>, aionui_db::Database) {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
    repo.create_team(&TeamRow {
        id: "t1".to_owned(),
        user_id: USER.to_owned(),
        name: "t1".to_owned(),
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
    (repo, db)
}

async fn setup() -> (TaskBoard, aionui_db::Database) {
    let (repo, db) = repo_with_team().await;
    (TaskBoard::new(repo as Arc<dyn ITeamRepository>), db)
}

/// Builds a raw task row so a test can plant a dependency edge that the
/// engagement-gated `create_task` path could never produce.
fn task_row(id: &str, engagement: Option<&str>, blocked_by: &[String], blocks: &[String]) -> TeamTaskRow {
    let now = now_ms();
    TeamTaskRow {
        id: id.to_owned(),
        team_id: "t1".to_owned(),
        subject: id.to_owned(),
        description: None,
        status: "pending".to_owned(),
        owner: None,
        blocked_by: serde_json::to_string(blocked_by).unwrap(),
        blocks: serde_json::to_string(blocks).unwrap(),
        metadata: None,
        created_at: now,
        updated_at: now,
        engagement_id: engagement.map(str::to_owned),
        expected_output: None,
        result: None,
        input_context: None,
    }
}

// -- TK: Create tasks ---------------------------------------------------------

#[tokio::test]
async fn tk1_create_task_no_dependencies() {
    let (board, _db) = setup().await;
    let task = board
        .create_task("t1", "Implement feature", None, None, &[], None)
        .await
        .unwrap();
    assert_eq!(task.subject, "Implement feature");
    assert_eq!(task.status, TaskStatus::Pending);
    assert!(task.blocked_by.is_empty());
    assert!(task.blocks.is_empty());
}

#[tokio::test]
async fn tk1b_create_task_surfaces_expected_output_on_response() {
    let (board, _db) = setup().await;
    let task = board
        .create_task("t1", "Write tests", None, None, &[], Some("A green test suite"))
        .await
        .unwrap();
    assert_eq!(task.expected_output.as_deref(), Some("A green test suite"));
    let resp = aionui_team::activity_mapping::task_to_response(&task);
    assert_eq!(resp.expected_output.as_deref(), Some("A green test suite"));
    assert_eq!(resp.result, None);
    assert_eq!(resp.input_context, None);
}

#[tokio::test]
async fn tk2_create_task_with_single_dependency() {
    let (board, _db) = setup().await;
    let task_a = board.create_task("t1", "Task A", None, None, &[], None).await.unwrap();
    let task_b = board
        .create_task("t1", "Task B", None, None, std::slice::from_ref(&task_a.id), None)
        .await
        .unwrap();
    assert_eq!(task_b.blocked_by, vec![task_a.id.clone()]);

    let tasks = board.list_tasks("t1").await.unwrap();
    let a = tasks.iter().find(|t| t.id == task_a.id).unwrap();
    assert_eq!(a.blocks, vec![task_b.id]);
}

#[tokio::test]
async fn tk3_create_task_with_multiple_dependencies() {
    let (board, _db) = setup().await;
    let a = board.create_task("t1", "A", None, None, &[], None).await.unwrap();
    let b = board.create_task("t1", "B", None, None, &[], None).await.unwrap();
    let c = board
        .create_task("t1", "C", None, None, &[a.id.clone(), b.id.clone()], None)
        .await
        .unwrap();
    assert_eq!(c.blocked_by.len(), 2);

    let tasks = board.list_tasks("t1").await.unwrap();
    let a_updated = tasks.iter().find(|t| t.id == a.id).unwrap();
    let b_updated = tasks.iter().find(|t| t.id == b.id).unwrap();
    assert!(a_updated.blocks.contains(&c.id));
    assert!(b_updated.blocks.contains(&c.id));
}

#[tokio::test]
async fn tk4_create_task_nonexistent_dependency_fails() {
    let (board, _db) = setup().await;
    let result = board
        .create_task("t1", "X", None, None, &["nonexistent".into()], None)
        .await;
    assert!(result.is_err());
}

// -- TU: Update tasks ---------------------------------------------------------

#[tokio::test]
async fn tu1_update_status_pending_to_in_progress() {
    let (board, _db) = setup().await;
    let task = board.create_task("t1", "Work", None, None, &[], None).await.unwrap();
    let updated = board
        .update_task(
            "t1",
            &task.id,
            &TaskUpdate {
                status: Some(TaskStatus::InProgress),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.status, TaskStatus::InProgress);
}

#[tokio::test]
async fn tu2_update_status_to_completed_triggers_unblock() {
    let (board, _db) = setup().await;
    let a = board.create_task("t1", "A", None, None, &[], None).await.unwrap();
    let b = board
        .create_task("t1", "B", None, None, std::slice::from_ref(&a.id), None)
        .await
        .unwrap();

    board
        .update_task(
            "t1",
            &a.id,
            &TaskUpdate {
                status: Some(TaskStatus::Completed),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let tasks = board.list_tasks("t1").await.unwrap();
    let b_updated = tasks.iter().find(|t| t.id == b.id).unwrap();
    assert!(b_updated.blocked_by.is_empty());
}

#[tokio::test]
async fn tu3_update_description() {
    let (board, _db) = setup().await;
    let task = board.create_task("t1", "Work", None, None, &[], None).await.unwrap();
    let updated = board
        .update_task(
            "t1",
            &task.id,
            &TaskUpdate {
                description: Some("Updated description".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.description.as_deref(), Some("Updated description"));
}

#[tokio::test]
async fn tu4_update_owner() {
    let (board, _db) = setup().await;
    let task = board.create_task("t1", "Work", None, None, &[], None).await.unwrap();
    let updated = board
        .update_task(
            "t1",
            &task.id,
            &TaskUpdate {
                owner: Some("agent-2".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.owner.as_deref(), Some("agent-2"));
}

#[tokio::test]
async fn tu5_update_nonexistent_task_fails() {
    let (board, _db) = setup().await;
    let result = board.update_task("t1", "nonexistent", &TaskUpdate::default()).await;
    assert!(result.is_err());
}

// -- CU: Check unblocks ------------------------------------------------------

#[tokio::test]
async fn cu1_complete_unblocks_single_downstream() {
    let (board, _db) = setup().await;
    let a = board.create_task("t1", "A", None, None, &[], None).await.unwrap();
    let b = board
        .create_task("t1", "B", None, None, std::slice::from_ref(&a.id), None)
        .await
        .unwrap();

    board
        .update_task(
            "t1",
            &a.id,
            &TaskUpdate {
                status: Some(TaskStatus::Completed),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let tasks = board.list_tasks("t1").await.unwrap();
    let b_updated = tasks.iter().find(|t| t.id == b.id).unwrap();
    assert!(b_updated.blocked_by.is_empty());
}

#[tokio::test]
async fn cu2_complete_unblocks_multiple_downstream() {
    let (board, _db) = setup().await;
    let a = board.create_task("t1", "A", None, None, &[], None).await.unwrap();
    let b = board
        .create_task("t1", "B", None, None, std::slice::from_ref(&a.id), None)
        .await
        .unwrap();
    let c = board
        .create_task("t1", "C", None, None, std::slice::from_ref(&a.id), None)
        .await
        .unwrap();

    board
        .update_task(
            "t1",
            &a.id,
            &TaskUpdate {
                status: Some(TaskStatus::Completed),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let tasks = board.list_tasks("t1").await.unwrap();
    let b_updated = tasks.iter().find(|t| t.id == b.id).unwrap();
    let c_updated = tasks.iter().find(|t| t.id == c.id).unwrap();
    assert!(b_updated.blocked_by.is_empty());
    assert!(c_updated.blocked_by.is_empty());
}

#[tokio::test]
async fn cu3_partial_unblock_preserves_other_deps() {
    let (board, _db) = setup().await;
    let a = board.create_task("t1", "A", None, None, &[], None).await.unwrap();
    let x = board.create_task("t1", "X", None, None, &[], None).await.unwrap();
    let b = board
        .create_task("t1", "B", None, None, &[a.id.clone(), x.id.clone()], None)
        .await
        .unwrap();

    board
        .update_task(
            "t1",
            &a.id,
            &TaskUpdate {
                status: Some(TaskStatus::Completed),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let tasks = board.list_tasks("t1").await.unwrap();
    let b_updated = tasks.iter().find(|t| t.id == b.id).unwrap();
    assert_eq!(b_updated.blocked_by, vec![x.id]);
}

#[tokio::test]
async fn cu4_complete_no_downstream_is_noop() {
    let (board, _db) = setup().await;
    let task = board.create_task("t1", "Solo", None, None, &[], None).await.unwrap();
    let updated = board
        .update_task(
            "t1",
            &task.id,
            &TaskUpdate {
                status: Some(TaskStatus::Completed),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.status, TaskStatus::Completed);
}

// -- TT: List tasks -----------------------------------------------------------

#[tokio::test]
async fn tt1_list_all_tasks() {
    let (board, _db) = setup().await;
    board.create_task("t1", "A", None, None, &[], None).await.unwrap();
    board.create_task("t1", "B", None, None, &[], None).await.unwrap();
    let tasks = board.list_tasks("t1").await.unwrap();
    assert_eq!(tasks.len(), 2);
}

#[tokio::test]
async fn tt2_list_empty() {
    let (board, _db) = setup().await;
    let tasks = board.list_tasks("t1").await.unwrap();
    assert!(tasks.is_empty());
}

#[tokio::test]
async fn tt3_list_includes_dependency_info() {
    let (board, _db) = setup().await;
    let a = board.create_task("t1", "A", None, None, &[], None).await.unwrap();
    let b = board
        .create_task("t1", "B", None, None, std::slice::from_ref(&a.id), None)
        .await
        .unwrap();
    let tasks = board.list_tasks("t1").await.unwrap();
    let b_found = tasks.iter().find(|t| t.id == b.id).unwrap();
    assert_eq!(b_found.blocked_by, vec![a.id.clone()]);
    let a_found = tasks.iter().find(|t| t.id == a.id).unwrap();
    assert!(a_found.blocks.contains(&b.id));
}

// -- DC-4: Bidirectional consistency ------------------------------------------

#[tokio::test]
async fn dc4_blocked_by_blocks_bidirectional_consistency() {
    let (board, _db) = setup().await;
    let a = board.create_task("t1", "A", None, None, &[], None).await.unwrap();
    let b = board
        .create_task("t1", "B", None, None, std::slice::from_ref(&a.id), None)
        .await
        .unwrap();

    let tasks = board.list_tasks("t1").await.unwrap();
    let a_found = tasks.iter().find(|t| t.id == a.id).unwrap();
    let b_found = tasks.iter().find(|t| t.id == b.id).unwrap();
    assert!(a_found.blocks.contains(&b.id));
    assert!(b_found.blocked_by.contains(&a.id));
}

// -- Engagement-scoped mutations (Phase 2b Task 3d, defense-in-depth) ---------

/// A board pinned to engagement E1 must reject an update to a task that belongs
/// to a sibling engagement E2 of the same team (read-gate -> TaskNotFound),
/// and must leave that task byte-identical.
#[tokio::test]
async fn engagement_update_rejects_cross_engagement_task() {
    let (repo, _db) = repo_with_team().await;
    let e1 = repo
        .find_or_create_engagement(USER, "t1", "proj-a", "/ws/a")
        .await
        .unwrap();
    let e2 = repo
        .find_or_create_engagement(USER, "t1", "proj-b", "/ws/b")
        .await
        .unwrap();

    let board1 = TaskBoard::new(repo.clone() as Arc<dyn ITeamRepository>).with_engagement(e1.id.as_str());
    let task = board1.create_task("t1", "InE1", None, None, &[], None).await.unwrap();

    let board2 = TaskBoard::new(repo.clone() as Arc<dyn ITeamRepository>).with_engagement(e2.id.as_str());
    let result = board2
        .update_task(
            "t1",
            &task.id,
            &TaskUpdate {
                status: Some(TaskStatus::Completed),
                ..Default::default()
            },
        )
        .await;
    assert!(
        matches!(result, Err(TeamError::TaskNotFound(_))),
        "cross-engagement update must be rejected"
    );

    let still = repo
        .find_task_by_engagement(USER, &e1.id, &task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(still.status, "pending", "E1 task must be unmutated");
}

/// Completing a task must not unblock a downstream task that lives in a
/// different engagement. The edge (C.blocks -> D) is hand-planted because the
/// engagement-gated `create_task` dependency loop could never build it.
#[tokio::test]
async fn engagement_unblock_skips_cross_engagement_downstream() {
    let (repo, _db) = repo_with_team().await;
    let e1 = repo
        .find_or_create_engagement(USER, "t1", "proj-a", "/ws/a")
        .await
        .unwrap();
    let e2 = repo
        .find_or_create_engagement(USER, "t1", "proj-b", "/ws/b")
        .await
        .unwrap();

    let c = task_row("C", Some(&e1.id), &[], &["D".to_owned()]);
    let d = task_row("D", Some(&e2.id), &["C".to_owned()], &[]);
    repo.create_task(USER, &c).await.unwrap();
    repo.create_task(USER, &d).await.unwrap();

    let board1 = TaskBoard::new(repo.clone() as Arc<dyn ITeamRepository>).with_engagement(e1.id.as_str());
    board1
        .update_task(
            "t1",
            "C",
            &TaskUpdate {
                status: Some(TaskStatus::Completed),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // C is done, but D belongs to E2 -> its blocked_by must be untouched.
    let d_after = repo.find_task_by_engagement(USER, &e2.id, "D").await.unwrap().unwrap();
    assert_eq!(
        d_after.blocked_by, r#"["C"]"#,
        "cross-engagement downstream must not be unblocked"
    );
}
