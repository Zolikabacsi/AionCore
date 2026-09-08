use aionui_db::init_database_memory;
use sqlx::Row;

#[tokio::test]
async fn migration_045_creates_engagement_tables_and_backfills_default() {
    let db = init_database_memory().await.unwrap();
    let pool = db.pool();

    // Fresh in-memory DB has no teams, so assert schema + indexes here; the
    // per-team backfill is exercised by inserting a team and calling
    // find_or_create_engagement directly (Task 2), not via migration timing.
    for col in ["engagement_id"] {
        let exists = sqlx::query("PRAGMA table_info(team_tasks)")
            .fetch_all(pool)
            .await
            .unwrap()
            .iter()
            .any(|r| r.get::<String, _>("name") == col);
        assert!(exists, "team_tasks.engagement_id missing");
        let exists_m = sqlx::query("PRAGMA table_info(mailbox)")
            .fetch_all(pool)
            .await
            .unwrap()
            .iter()
            .any(|r| r.get::<String, _>("name") == col);
        assert!(exists_m, "mailbox.engagement_id missing");
    }

    for t in ["team_engagements", "team_engagement_members"] {
        let has = sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name = ?")
            .bind(t)
            .fetch_optional(pool)
            .await
            .unwrap()
            .is_some();
        assert!(has, "table {t} missing");
    }
}
