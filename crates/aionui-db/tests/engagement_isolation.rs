use aionui_db::{ITeamRepository, SqliteTeamRepository, init_database_memory};

// The team_tasks / mailbox parent-check triggers (030_user_scope.sql) require a
// real `teams` row, so seed one. Tasks/mail are inserted via raw SQL with an
// explicit `engagement_id` because the runtime create paths do not persist it
// yet (Phase 2). This test proves the data-layer isolation invariant.
async fn seed_team(pool: &sqlx::SqlitePool, id: &str) {
    sqlx::query(
        "INSERT INTO teams (id,user_id,name,workspace,workspace_mode,agents,agents_version,created_at,updated_at) \
         VALUES (?1,'u1','T','','shared','[]','1.0.0',0,0)",
    )
    .bind(id)
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_task(pool: &sqlx::SqlitePool, id: &str, team_id: &str, eng: &str) {
    sqlx::query(
        "INSERT INTO team_tasks (id,team_id,subject,status,blocked_by,blocks,engagement_id,created_at,updated_at) \
         VALUES (?1,?2,'s','pending','[]','[]',?3,0,0)",
    )
    .bind(id)
    .bind(team_id)
    .bind(eng)
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_message(pool: &sqlx::SqlitePool, id: &str, team_id: &str, eng: &str) {
    sqlx::query(
        "INSERT INTO mailbox (id,team_id,to_agent_id,from_agent_id,type,content,read,engagement_id,created_at) \
         VALUES (?1,?2,'a1','a2','message','hi',0,?3,0)",
    )
    .bind(id)
    .bind(team_id)
    .bind(eng)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn tasks_and_mail_are_isolated_per_engagement() {
    let db = init_database_memory().await.unwrap();
    let pool = db.pool();
    let repo = SqliteTeamRepository::new(pool.clone());
    seed_team(pool, "team-x").await;

    let ea = repo
        .find_or_create_engagement("u1", "team-x", "proj-a", "/ws/a")
        .await
        .unwrap();
    let eb = repo
        .find_or_create_engagement("u1", "team-x", "proj-b", "/ws/b")
        .await
        .unwrap();
    assert_ne!(ea.id, eb.id, "different projects => different engagements");

    insert_task(pool, "t1", "team-x", &ea.id).await;
    insert_task(pool, "t2", "team-x", &eb.id).await;
    insert_message(pool, "m1", "team-x", &ea.id).await;
    insert_message(pool, "m2", "team-x", &eb.id).await;

    let tasks_a = repo.list_tasks_by_engagement("u1", &ea.id).await.unwrap();
    let tasks_b = repo.list_tasks_by_engagement("u1", &eb.id).await.unwrap();
    assert_eq!(
        tasks_a.iter().map(|r| r.id.clone()).collect::<Vec<_>>(),
        vec!["t1".to_string()]
    );
    assert_eq!(
        tasks_b.iter().map(|r| r.id.clone()).collect::<Vec<_>>(),
        vec!["t2".to_string()]
    );

    let msgs_a = repo.list_messages_by_engagement("u1", &ea.id).await.unwrap();
    let msgs_b = repo.list_messages_by_engagement("u1", &eb.id).await.unwrap();
    assert_eq!(
        msgs_a.iter().map(|r| r.id.clone()).collect::<Vec<_>>(),
        vec!["m1".to_string()]
    );
    assert_eq!(
        msgs_b.iter().map(|r| r.id.clone()).collect::<Vec<_>>(),
        vec!["m2".to_string()]
    );

    // Negative: a non-owning user_id sees nothing even with the real engagement
    // id, proving the team_engagements.user_id ownership guard.
    let stolen_tasks = repo
        .list_tasks_by_engagement("attacker_user", &ea.id)
        .await
        .unwrap();
    assert!(
        stolen_tasks.is_empty(),
        "non-owner must not read another user's engagement tasks"
    );
    let stolen_msgs = repo
        .list_messages_by_engagement("attacker_user", &ea.id)
        .await
        .unwrap();
    assert!(
        stolen_msgs.is_empty(),
        "non-owner must not read another user's engagement mail"
    );
}
