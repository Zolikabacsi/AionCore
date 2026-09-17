//! Keyset-paginated engagement-scoped repo reads for the activity feed.
//!
//! Mirrors the team-scoped paged tests in `team_repository.rs`, but scoped to a
//! single engagement: identical cursor / direction / ordering / limit semantics
//! plus the `team_engagements.user_id` owner guard.

use std::sync::Arc;

use aionui_common::now_ms;
use aionui_db::models::{MailboxMessageRow, TeamRow, TeamTaskRow};
use aionui_db::{ActivityCursor, ITeamRepository, PageDirection, SqliteTeamRepository, init_database_memory};

const OWNER: &str = "user-a";

async fn repo() -> (Arc<dyn ITeamRepository>, aionui_db::Database) {
    let db = init_database_memory().await.unwrap();
    let r = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
    (r as Arc<dyn ITeamRepository>, db)
}

fn make_team(id: &str, user_id: &str) -> TeamRow {
    let now = now_ms();
    TeamRow {
        id: id.into(),
        user_id: user_id.into(),
        name: id.into(),
        workspace: String::new(),
        workspace_mode: "shared".into(),
        agents:
            r#"[{"slot_id":"a1","name":"Lead","role":"lead","conversation_id":"conv-1","backend":"claude","model":""}]"#
                .into(),
        lead_agent_id: Some("a1".into()),
        session_mode: None,
        agents_version: "1.0.1".into(),
        created_at: now,
        updated_at: now,
        project_id: None,
        folder_id: None,
    }
}

fn msg(id: &str, team_id: &str, engagement_id: &str, ts: i64) -> MailboxMessageRow {
    MailboxMessageRow {
        id: id.into(),
        team_id: team_id.into(),
        to_agent_id: "a1".into(),
        from_agent_id: "lead".into(),
        msg_type: "message".into(),
        content: format!("content-{id}"),
        summary: None,
        files: None,
        read: false,
        created_at: ts,
        engagement_id: Some(engagement_id.into()),
    }
}

fn task(id: &str, team_id: &str, engagement_id: &str, ts: i64) -> TeamTaskRow {
    TeamTaskRow {
        id: id.into(),
        team_id: team_id.into(),
        subject: format!("subject-{id}"),
        description: None,
        status: "pending".into(),
        owner: None,
        blocked_by: "[]".into(),
        blocks: "[]".into(),
        metadata: None,
        created_at: ts,
        updated_at: ts,
        engagement_id: Some(engagement_id.into()),
        expected_output: None,
        result: None,
        input_context: None,
    }
}

fn ids<T, F: Fn(&T) -> &str>(rows: &[T], get: F) -> Vec<&str> {
    rows.iter().map(get).collect()
}

// ── Mailbox ──────────────────────────────────────────────────────────

#[tokio::test]
async fn messages_paged_returns_only_engagement_a_in_order_desc_asc() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", OWNER)).await.unwrap();
    let a = repo.create_engagement(OWNER, "t1", "proj-a", "/a").await.unwrap().id;
    let b = repo.create_engagement(OWNER, "t1", "proj-b", "/b").await.unwrap().id;

    // Five rows on A (incl. a created_at tie to exercise the id tiebreak) plus
    // two on B that must never appear in an A page.
    for (id, ts) in [("m1", 1000), ("m2", 2000), ("m3", 3000), ("m4", 3000), ("m5", 4000)] {
        repo.write_message(OWNER, &msg(id, "t1", &a, ts)).await.unwrap();
    }
    repo.write_message(OWNER, &msg("b1", "t1", &b, 2500)).await.unwrap();
    repo.write_message(OWNER, &msg("b2", "t1", &b, 5000)).await.unwrap();

    let page1 = repo
        .list_messages_by_engagement_paged(OWNER, &a, None, PageDirection::Desc, 2)
        .await
        .unwrap();
    assert_eq!(ids(&page1, |r| r.id.as_str()), ["m5", "m4"]);

    let cursor = ActivityCursor {
        created_at: 3000,
        id: "m4".into(),
    };
    let page2 = repo
        .list_messages_by_engagement_paged(OWNER, &a, Some(cursor), PageDirection::Desc, 2)
        .await
        .unwrap();
    assert_eq!(ids(&page2, |r| r.id.as_str()), ["m3", "m2"]);

    // Asc from the oldest, limit clamp honored (only B is interleaved, so the
    // asc page must skip b1/b2 entirely).
    let asc = repo
        .list_messages_by_engagement_paged(OWNER, &a, None, PageDirection::Asc, 2)
        .await
        .unwrap();
    assert_eq!(ids(&asc, |r| r.id.as_str()), ["m1", "m2"]);
}

#[tokio::test]
async fn messages_paged_next_page_excludes_prior_with_limit_clamp() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", OWNER)).await.unwrap();
    let a = repo.create_engagement(OWNER, "t1", "proj-a", "/a").await.unwrap().id;
    for (id, ts) in [("m1", 1000), ("m2", 2000), ("m3", 3000)] {
        repo.write_message(OWNER, &msg(id, "t1", &a, ts)).await.unwrap();
    }
    // limit larger than the table: returns all three, no error.
    let all = repo
        .list_messages_by_engagement_paged(OWNER, &a, None, PageDirection::Asc, 10)
        .await
        .unwrap();
    assert_eq!(ids(&all, |r| r.id.as_str()), ["m1", "m2", "m3"]);
    let last = all.last().unwrap();
    let cursor = ActivityCursor {
        created_at: last.created_at,
        id: last.id.clone(),
    };
    let next = repo
        .list_messages_by_engagement_paged(OWNER, &a, Some(cursor), PageDirection::Asc, 10)
        .await
        .unwrap();
    assert!(next.is_empty(), "page past the end must be empty");
}

// ── Tasks ────────────────────────────────────────────────────────────

#[tokio::test]
async fn tasks_paged_returns_only_engagement_a_with_cursor() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", OWNER)).await.unwrap();
    let a = repo.create_engagement(OWNER, "t1", "proj-a", "/a").await.unwrap().id;
    let b = repo.create_engagement(OWNER, "t1", "proj-b", "/b").await.unwrap().id;
    for (id, ts) in [("k1", 1000), ("k2", 2000), ("k3", 3000)] {
        repo.create_task(OWNER, &task(id, "t1", &a, ts)).await.unwrap();
    }
    repo.create_task(OWNER, &task("kb1", "t1", &b, 1500)).await.unwrap();

    let page1 = repo
        .list_tasks_by_engagement_paged(OWNER, &a, None, PageDirection::Desc, 2)
        .await
        .unwrap();
    assert_eq!(ids(&page1, |r| r.id.as_str()), ["k3", "k2"]);

    let cursor = ActivityCursor {
        created_at: 2000,
        id: "k2".into(),
    };
    let page2 = repo
        .list_tasks_by_engagement_paged(OWNER, &a, Some(cursor), PageDirection::Desc, 2)
        .await
        .unwrap();
    assert_eq!(ids(&page2, |r| r.id.as_str()), ["k1"]);
}

// ── Owner / engagement isolation ─────────────────────────────────────

#[tokio::test]
async fn cross_user_engagement_returns_empty() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", OWNER)).await.unwrap();
    let a = repo.create_engagement(OWNER, "t1", "proj-a", "/a").await.unwrap().id;
    repo.write_message(OWNER, &msg("m1", "t1", &a, 1000)).await.unwrap();
    repo.create_task(OWNER, &task("k1", "t1", &a, 1000)).await.unwrap();

    // A different user querying the same engagement_id leaks nothing.
    let rows = repo
        .list_messages_by_engagement_paged("user-b", &a, None, PageDirection::Desc, 10)
        .await
        .unwrap();
    assert!(rows.is_empty());
    let tasks = repo
        .list_tasks_by_engagement_paged("user-b", &a, None, PageDirection::Desc, 10)
        .await
        .unwrap();
    assert!(tasks.is_empty());
}

#[tokio::test]
async fn foreign_or_orphan_engagement_id_returns_empty() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", OWNER)).await.unwrap();
    let a = repo.create_engagement(OWNER, "t1", "proj-a", "/a").await.unwrap().id;
    repo.write_message(OWNER, &msg("m1", "t1", &a, 1000)).await.unwrap();
    repo.create_task(OWNER, &task("k1", "t1", &a, 1000)).await.unwrap();

    // Unknown engagement id (belongs to nobody) yields an empty page, not A's
    // data: the EXISTS owner gate fails so the whole query is empty.
    let rows = repo
        .list_messages_by_engagement_paged(OWNER, "does-not-exist", None, PageDirection::Desc, 10)
        .await
        .unwrap();
    assert!(rows.is_empty());
    let tasks = repo
        .list_tasks_by_engagement_paged(OWNER, "does-not-exist", None, PageDirection::Desc, 10)
        .await
        .unwrap();
    assert!(tasks.is_empty());

    // A mailbox row stamped with an engagement_id that has no team_engagements
    // row is invisible even to the team owner (orphan guard mirrors
    // `list_messages_by_engagement`).
    repo.write_message(OWNER, &msg("orphan", "t1", "ghost-eng", 9000))
        .await
        .unwrap();
    let ghost = repo
        .list_messages_by_engagement_paged(OWNER, "ghost-eng", None, PageDirection::Desc, 10)
        .await
        .unwrap();
    assert!(ghost.is_empty());
}

#[tokio::test]
async fn legacy_sentinel_engagement_id_resolves_via_seeded_team_engagement() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", OWNER)).await.unwrap();

    // Legacy single-engagement teams stamp rows with engagement_id == team_id.
    repo.write_message(OWNER, &msg("m1", "t1", "t1", 1000)).await.unwrap();
    repo.create_task(OWNER, &task("k1", "t1", "t1", 1000)).await.unwrap();

    // `create_team` seeds a sentinel `team_engagements` row keyed on the team_id,
    // so the owner EXISTS gate is satisfied and the sentinel engagement is
    // visible to the team owner (documented behavior; same as the non-paged
    // `list_messages_by_engagement`).
    let got = repo
        .list_messages_by_engagement_paged(OWNER, "t1", None, PageDirection::Desc, 10)
        .await
        .unwrap();
    assert_eq!(ids(&got, |r| r.id.as_str()), ["m1"]);
    let got_tasks = repo
        .list_tasks_by_engagement_paged(OWNER, "t1", None, PageDirection::Desc, 10)
        .await
        .unwrap();
    assert_eq!(ids(&got_tasks, |r| r.id.as_str()), ["k1"]);

    // The sentinel is still owner-scoped: a different user gets nothing.
    let other = repo
        .list_messages_by_engagement_paged("user-b", "t1", None, PageDirection::Desc, 10)
        .await
        .unwrap();
    assert!(other.is_empty());
}
