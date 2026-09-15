use aionui_db::{ITeamRepository, SqliteTeamRepository, init_database_memory};

// create_engagement now enforces P2-1 team ownership, so the team must exist.
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
async fn find_or_create_is_idempotent_per_team_project() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());
    seed_team(db.pool(), "team-x", "u1").await;

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
    seed_team(db.pool(), "team-x", "u1").await;

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
        .find_engagement("u1", "team-x", "proj-a")
        .await
        .unwrap()
        .expect("engagement exists");
    assert_eq!(found.id, row.id);
    assert_eq!(found.origin, "user");
}

fn member_row(engagement_id: &str, team_id: &str, slot: &str) -> aionui_db::models::TeamEngagementMemberRow {
    aionui_db::models::TeamEngagementMemberRow {
        engagement_id: engagement_id.to_owned(),
        team_id: team_id.to_owned(),
        template_slot: slot.to_owned(),
        slot_id: format!("{slot}-runtime"),
        conversation_id: format!("{slot}-conv"),
        role: "teammate".to_owned(),
        status: None,
        created_at: 0,
        updated_at: 0,
    }
}

/// `delete_engagement_members_by_team` / `delete_engagements_by_team` must remove
/// exactly one team's rows (user- AND team-scoped), leave another user's team
/// intact, and respect the member-before-engagement FK order. Also asserts a
/// legacy team's default engagement (id == team_id) deletes cleanly with no
/// member rows present.
#[tokio::test]
async fn delete_by_team_is_scoped_and_respects_member_fk_order() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());
    let pool = db.pool();

    // Two owners, one team each.
    for (uid, team) in [("u1", "team-1"), ("u2", "team-2")] {
        seed_team(pool, team, uid).await;
    }
    let eng1 = repo
        .find_or_create_engagement("u1", "team-1", "proj-a", "/ws/a")
        .await
        .unwrap();
    repo.find_or_create_engagement("u1", "team-1", "proj-b", "/ws/b")
        .await
        .unwrap();
    let eng2 = repo
        .find_or_create_engagement("u2", "team-2", "proj-a", "/ws/a")
        .await
        .unwrap();
    for m in [
        member_row(&eng1.id, "team-1", "s1"),
        member_row(&eng1.id, "team-1", "s2"),
    ] {
        repo.upsert_engagement_member(&m).await.unwrap();
    }
    repo.upsert_engagement_member(&member_row(&eng2.id, "team-2", "s1"))
        .await
        .unwrap();

    // Legacy team: default engagement row (id == team_id) from the 045 backfill,
    // no members. Seed it directly so we can prove it deletes without FK pain.
    sqlx::query(
        "INSERT INTO team_engagements (id,user_id,team_id,project_id,workspace,process,status,created_at,updated_at) \
         VALUES ('team-legacy','u1','team-legacy','__none__','','hierarchical','active',0,0)",
    )
    .execute(pool)
    .await
    .unwrap();
    seed_team(pool, "team-legacy", "u1").await;

    repo.delete_engagement_members_by_team("u1", "team-1").await.unwrap();
    repo.delete_engagements_by_team("u1", "team-1").await.unwrap();

    // Target team fully cleared.
    assert!(repo.list_engagements("u1", "team-1").await.unwrap().is_empty());
    assert!(repo.list_engagement_members("u1", &eng1.id).await.unwrap().is_empty());
    // Another owner's team/engagement/member survive.
    assert_eq!(repo.list_engagements("u2", "team-2").await.unwrap().len(), 1);
    assert_eq!(repo.list_engagement_members("u2", &eng2.id).await.unwrap().len(), 1);
    // Legacy team (no members) deletes cleanly (engagement row only).
    repo.delete_engagement_members_by_team("u1", "team-legacy")
        .await
        .unwrap();
    repo.delete_engagements_by_team("u1", "team-legacy").await.unwrap();
    assert!(repo.list_engagements("u1", "team-legacy").await.unwrap().is_empty());
}

#[tokio::test]
async fn find_engagement_by_id_reads_process_and_is_user_scoped() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());
    seed_team(db.pool(), "team-p", "u1").await;
    seed_team(db.pool(), "team-q", "u2").await;

    let mine = repo
        .find_or_create_engagement("u1", "team-p", "proj", "/ws")
        .await
        .unwrap();
    let theirs = repo
        .find_or_create_engagement("u2", "team-q", "proj", "/ws")
        .await
        .unwrap();

    // Default process is hierarchical.
    let found = repo.find_engagement_by_id("u1", &mine.id).await.unwrap().expect("row");
    assert_eq!(found.process, "hierarchical");

    // Flip to sequential and re-read by id.
    sqlx::query("UPDATE team_engagements SET process = 'sequential' WHERE id = ?")
        .bind(&mine.id)
        .execute(db.pool())
        .await
        .unwrap();
    let seq = repo.find_engagement_by_id("u1", &mine.id).await.unwrap().expect("row");
    assert_eq!(seq.process, "sequential");

    // Cross-user isolation: u1 cannot read u2's engagement id.
    assert!(repo.find_engagement_by_id("u1", &theirs.id).await.unwrap().is_none());
    // Unknown id -> None (not an error).
    assert!(repo.find_engagement_by_id("u1", "nope").await.unwrap().is_none());
}

#[tokio::test]
async fn update_engagement_sets_process_and_status_user_scoped() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());
    seed_team(db.pool(), "team-p", "u1").await;
    seed_team(db.pool(), "team-q", "u2").await;

    let mine = repo
        .find_or_create_engagement("u1", "team-p", "proj", "/ws")
        .await
        .unwrap();
    assert_eq!(mine.process, "hierarchical", "default process");

    // Flip process only; status untouched.
    repo.update_engagement("u1", &mine.id, Some("sequential"), None)
        .await
        .unwrap();
    let after = repo.find_engagement_by_id("u1", &mine.id).await.unwrap().expect("row");
    assert_eq!(after.process, "sequential");
    assert_eq!(after.status, "active");
    assert!(after.updated_at >= mine.updated_at);

    // Archive; process preserved.
    repo.update_engagement("u1", &mine.id, None, Some("archived"))
        .await
        .unwrap();
    let after = repo.find_engagement_by_id("u1", &mine.id).await.unwrap().expect("row");
    assert_eq!(after.process, "sequential");
    assert_eq!(after.status, "archived");

    // Cross-user update -> NotFound and the row is unchanged.
    let err = repo
        .update_engagement("u2", &mine.id, Some("hierarchical"), None)
        .await
        .expect_err("other user must not update");
    assert!(matches!(err, aionui_db::DbError::NotFound(_)));
    let untouched = repo.find_engagement_by_id("u1", &mine.id).await.unwrap().expect("row");
    assert_eq!(untouched.process, "sequential");

    // Invalid process value rejected by the CHECK constraint (a DbError, not Ok).
    let bad = repo.update_engagement("u1", &mine.id, Some("bogus"), None).await;
    assert!(bad.is_err(), "CHECK must reject an invalid process value");
}
