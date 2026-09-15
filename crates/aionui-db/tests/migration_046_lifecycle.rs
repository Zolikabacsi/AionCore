use aionui_db::init_database_memory;
use sqlx::Row;

#[tokio::test]
async fn migration_046_adds_engagement_lifecycle_columns() {
    let db = init_database_memory().await.unwrap();
    let pool = db.pool();

    // Fresh in-memory DB has no rows, so assert schema only (mirror migration_045 style).
    let cols: Vec<String> = sqlx::query("PRAGMA table_info(team_engagements)")
        .fetch_all(pool)
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<String, _>("name"))
        .collect();

    for col in ["folder_id", "origin", "created_by_conversation_id", "reply_to"] {
        assert!(cols.iter().any(|c| c == col), "missing column {col}");
    }
}
