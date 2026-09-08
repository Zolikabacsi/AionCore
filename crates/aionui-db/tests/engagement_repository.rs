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
