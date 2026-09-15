use aionui_db::models::TeamEngagementMemberRow;
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

fn member(
    engagement_id: &str,
    team_id: &str,
    template_slot: &str,
    slot_id: &str,
    conversation_id: &str,
) -> TeamEngagementMemberRow {
    TeamEngagementMemberRow {
        engagement_id: engagement_id.to_string(),
        team_id: team_id.to_string(),
        template_slot: template_slot.to_string(),
        slot_id: slot_id.to_string(),
        conversation_id: conversation_id.to_string(),
        role: "worker".to_string(),
        status: None,
        created_at: 0,
        updated_at: 0,
    }
}

#[tokio::test]
async fn upsert_roundtrips_through_all_reads() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());
    seed_team(db.pool(), "team-a", "u1").await;
    let eng = repo
        .find_or_create_engagement("u1", "team-a", "proj-a", "/ws")
        .await
        .unwrap();

    repo.upsert_engagement_member(&member(&eng.id, "team-a", "slot-0", "rt-1", "conv-1"))
        .await
        .unwrap();

    let listed = repo.list_engagement_members("u1", &eng.id).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].conversation_id, "conv-1");
    assert_eq!(listed[0].template_slot, "slot-0");

    let by_slot = repo
        .get_engagement_member_by_slot(&eng.id, "rt-1")
        .await
        .unwrap()
        .expect("member resolvable by runtime slot_id");
    assert_eq!(by_slot.conversation_id, "conv-1");

    let by_conv = repo
        .get_engagement_member_by_conversation("conv-1")
        .await
        .unwrap()
        .expect("member resolvable by conversation_id");
    assert_eq!(by_conv.engagement_id, eng.id);
}

#[tokio::test]
async fn upsert_twice_updates_conflicting_row() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());
    seed_team(db.pool(), "team-a", "u1").await;
    let eng = repo
        .find_or_create_engagement("u1", "team-a", "proj-a", "/ws")
        .await
        .unwrap();

    repo.upsert_engagement_member(&member(&eng.id, "team-a", "slot-0", "rt-1", "conv-1"))
        .await
        .unwrap();
    repo.upsert_engagement_member(&member(&eng.id, "team-a", "slot-0", "rt-2", "conv-2"))
        .await
        .unwrap();

    let listed = repo.list_engagement_members("u1", &eng.id).await.unwrap();
    assert_eq!(listed.len(), 1, "same template_slot must upsert, not duplicate");
    assert_eq!(listed[0].conversation_id, "conv-2");
    assert_eq!(listed[0].slot_id, "rt-2");
    assert!(
        repo.get_engagement_member_by_slot(&eng.id, "rt-1")
            .await
            .unwrap()
            .is_none(),
        "old slot_id must be gone after upsert"
    );
}

#[tokio::test]
async fn members_are_isolated_per_engagement() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());
    seed_team(db.pool(), "team-a", "u1").await;
    let e1 = repo
        .find_or_create_engagement("u1", "team-a", "proj-1", "/ws1")
        .await
        .unwrap();
    let e2 = repo
        .find_or_create_engagement("u1", "team-a", "proj-2", "/ws2")
        .await
        .unwrap();

    repo.upsert_engagement_member(&member(&e1.id, "team-a", "slot-0", "rt-1", "conv-1"))
        .await
        .unwrap();
    repo.upsert_engagement_member(&member(&e2.id, "team-a", "slot-0", "rt-2", "conv-2"))
        .await
        .unwrap();

    // Same template_slot, distinct engagement => distinct conversation_ids.
    let l1 = repo.list_engagement_members("u1", &e1.id).await.unwrap();
    let l2 = repo.list_engagement_members("u1", &e2.id).await.unwrap();
    assert_eq!(l1.len(), 1);
    assert_eq!(l2.len(), 1);
    assert_eq!(l1[0].conversation_id, "conv-1");
    assert_eq!(l2[0].conversation_id, "conv-2");

    let s1 = repo
        .get_engagement_member_by_slot(&e1.id, "rt-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s1.conversation_id, "conv-1");
    assert!(
        repo.get_engagement_member_by_slot(&e1.id, "rt-2")
            .await
            .unwrap()
            .is_none(),
        "e2's slot must not resolve inside e1"
    );
}

#[tokio::test]
async fn members_are_scoped_by_owner() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());
    seed_team(db.pool(), "team-a", "u1").await;
    seed_team(db.pool(), "team-b", "u2").await;
    let mine = repo
        .find_or_create_engagement("u1", "team-a", "proj-1", "/ws")
        .await
        .unwrap();
    let theirs = repo
        .find_or_create_engagement("u2", "team-b", "proj-1", "/ws")
        .await
        .unwrap();

    repo.upsert_engagement_member(&member(&theirs.id, "team-b", "slot-0", "rt-x", "conv-x"))
        .await
        .unwrap();

    // u1 must not see u2's engagement members, even when passing the foreign id.
    let leaked = repo.list_engagement_members("u1", &theirs.id).await.unwrap();
    assert!(leaked.is_empty(), "cross-user member read must be blocked");

    let owned = repo.list_engagement_members("u1", &mine.id).await.unwrap();
    assert!(owned.is_empty(), "u1's own engagement has no members yet");
}
