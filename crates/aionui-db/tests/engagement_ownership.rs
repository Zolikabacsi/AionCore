use aionui_db::{DbError, ITeamRepository, SqliteTeamRepository, init_database_memory};

// P2-1 ownership guard (spec §9, §16). A `team_engagements` row is keyed on
// `team_id`, so create/find MUST be rejected unless the caller owns the team,
// and reads must be scoped to the caller's `user_id`. Seed two teams with
// distinct owners.
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
async fn owner_can_create_and_find_engagement_for_their_team() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());
    seed_team(db.pool(), "team-alice", "alice").await;

    let created = repo
        .create_engagement("alice", "team-alice", "proj-a", "/ws/a")
        .await
        .expect("owner may create an engagement for their own team");

    let found = repo
        .find_engagement("alice", "team-alice", "proj-a")
        .await
        .unwrap()
        .expect("owner may read their own engagement");
    assert_eq!(found.id, created.id);

    let or_create = repo
        .find_or_create_engagement("alice", "team-alice", "proj-a", "/ws/a")
        .await
        .expect("owner may find-or-create their own engagement");
    assert_eq!(or_create.id, created.id);
}

#[tokio::test]
async fn non_owner_cannot_create_engagement_for_another_users_team() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());
    seed_team(db.pool(), "team-alice", "alice").await;

    let err = repo
        .create_engagement("bob", "team-alice", "proj-a", "/ws/a")
        .await
        .expect_err("non-owner must be rejected before any write");
    assert!(
        matches!(err, DbError::NotFound(_)),
        "expected NotFound (ownership guard), got: {err:?}"
    );

    // Fail-closed invariant: nothing was written under the wrong team.
    let leaked = repo.find_engagement("alice", "team-alice", "proj-a").await.unwrap();
    assert!(leaked.is_none(), "rejected create must not persist an engagement");
}

#[tokio::test]
async fn non_owner_find_or_create_never_returns_another_users_engagement() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());
    seed_team(db.pool(), "team-alice", "alice").await;

    // Alice owns the team and has an engagement on the same (team, project).
    let alice_eng = repo
        .find_or_create_engagement("alice", "team-alice", "proj-a", "/ws/a")
        .await
        .unwrap();

    // Bob calling find-or-create for Alice's team must be rejected, not handed
    // Alice's engagement.
    let result = repo
        .find_or_create_engagement("bob", "team-alice", "proj-a", "/ws/a")
        .await;
    assert!(
        matches!(result, Err(DbError::NotFound(_))),
        "non-owner find_or_create must be rejected, got: {result:?}"
    );
    if let Ok(bob_eng) = result {
        assert_ne!(
            bob_eng.id, alice_eng.id,
            "non-owner must not receive owner's engagement"
        );
    }
}
