use aionui_db::{ITeamRepository, SqliteTeamRepository, init_database_memory};

#[tokio::test]
async fn find_or_create_is_idempotent_per_team_project() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());

    let a = repo
        .find_or_create_engagement("u1", "team-x", "proj-a", "/ws/a")
        .await
        .unwrap();
    let b = repo
        .find_or_create_engagement("u1", "team-x", "proj-a", "/ws/a")
        .await
        .unwrap();
    assert_eq!(a.id, b.id, "same (team,project) must reuse the engagement");

    let c = repo
        .find_or_create_engagement("u1", "team-x", "proj-b", "/ws/b")
        .await
        .unwrap();
    assert_ne!(a.id, c.id, "different project => different engagement");

    let all = repo.list_engagements("u1", "team-x").await.unwrap();
    assert_eq!(all.len(), 2);
}

#[tokio::test]
async fn engagement_row_surfaces_lifecycle_columns() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());

    let row = repo
        .find_or_create_engagement("u1", "team-x", "proj-a", "/ws/a")
        .await
        .unwrap();

    assert_eq!(row.origin, "user", "origin defaults to 'user' on insert");
    assert_eq!(row.folder_id, None);
    assert_eq!(row.created_by_conversation_id, None);
    assert_eq!(row.reply_to, None);

    // round-trip through the read-back path (find_engagement uses SELECT *).
    let found = repo
        .find_engagement("team-x", "proj-a")
        .await
        .unwrap()
        .expect("engagement exists");
    assert_eq!(found.id, row.id);
    assert_eq!(found.origin, "user");
    assert_eq!(found.folder_id, None);
}
