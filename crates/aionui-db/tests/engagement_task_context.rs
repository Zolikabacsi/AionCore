//! Phase 3a / Task 1: CrewAI task-context columns (`expected_output`,
//! `result`, `input_context`) round-trip through the real `SqliteTeamRepository`.
//!
//! `expected_output` is set on the create path; `result`/`input_context` are
//! columns only in this task (Tasks 2/3 populate them), so they read back `None`.

use aionui_common::now_ms;
use aionui_db::models::TeamTaskRow;
use aionui_db::{ITeamRepository, SqliteTeamRepository, init_database_memory};

async fn seed_team(pool: &sqlx::SqlitePool, id: &str, user_id: &str) {
    sqlx::query(
        "INSERT INTO teams (id,user_id,name,workspace,workspace_mode,agents,agents_version,created_at,updated_at) \
         VALUES (?1,?2,'T','','shared','[]','1.0.0',0,0)",
    )
    .bind(id)
    .bind(user_id)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn expected_output_round_trips_and_result_input_context_are_none() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());
    seed_team(db.pool(), "team-x", "u1").await;

    let engagement = repo
        .find_or_create_engagement("u1", "team-x", "proj-a", "/ws/a")
        .await
        .unwrap();

    let now = now_ms();
    let task = TeamTaskRow {
        id: "tk1".into(),
        team_id: "team-x".into(),
        subject: "Ship".into(),
        description: None,
        status: "pending".into(),
        owner: None,
        blocked_by: "[]".into(),
        blocks: "[]".into(),
        metadata: None,
        created_at: now,
        updated_at: now,
        engagement_id: Some(engagement.id.clone()),
        expected_output: Some("A passing test suite".into()),
        result: None,
        input_context: None,
    };
    repo.create_task("u1", &task).await.unwrap();

    let found = repo
        .find_task_by_engagement("u1", &engagement.id, "tk1")
        .await
        .unwrap()
        .expect("task exists");

    assert_eq!(found.expected_output.as_deref(), Some("A passing test suite"));
    assert_eq!(found.result, None);
    assert_eq!(found.input_context, None);
}
