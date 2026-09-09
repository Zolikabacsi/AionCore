//! DB-backed regression tests for the delegation chain + room-reuse queries.
//!
//! Guards the root-cause fix for the "false cycle_detected" incident: an
//! expired-but-still-`delivered` async envelope must NOT block a legitimate
//! follow-up dispatch, and room reuse must be scoped to the caller's own
//! delegation (never a room created for a different conversation).

use sqlx::Row;

use super::{ASSISTANT_ID_FOR_DEFINITION_SQL, CALLER_WORKSPACE_SQL, CHAIN_DEPTH_SQL, LIVE_CHAIN_SQL, REUSE_ROOM_SQL};
use aionui_db::{IConversationRepository, SqliteConversationRepository, init_database_memory};

// Fixed clock so live/expired envelopes are deterministic.
const NOW: i64 = 1_788_800_000_000;
const LIVE: i64 = NOW + 60_000; // expires after now  → counts
const STALE: i64 = NOW - 60_000; // expired before now → must be ignored

async fn repo() -> SqliteConversationRepository {
    let db = init_database_memory().await.expect("memory db");
    let r = SqliteConversationRepository::new(db.pool().clone());
    // `conversations.user_id` FKs to `users.id`; seed a minimal user.
    r.raw_execute(
        "INSERT OR IGNORE INTO users (id, username, password_hash, created_at, updated_at) VALUES ('user-1','user-1','x',0,0)",
        vec![],
    )
    .await
    .expect("seed user");
    r
}

async fn insert_envelope(r: &SqliteConversationRepository, id: &str, root: &str, asst: &str, depth: i64, expires: i64) {
    r.raw_execute(
        "INSERT INTO delegation_envelopes (id, user_id, root_conversation_id, target_conversation_id, target_assistant_id, depth, status, expires_at, created_at, updated_at) \
         VALUES (?1,'user-1',?2,'tc-1',?3,?4,'delivered',?5,0,0)",
        vec![id.into(), root.into(), asst.into(), depth.to_string(), expires.to_string()],
    )
    .await
    .unwrap();
}

async fn live_targets(r: &SqliteConversationRepository, root: &str) -> Vec<String> {
    r.raw_query(LIVE_CHAIN_SQL, vec![root.into(), NOW.to_string()])
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>("target_assistant_id"))
        .collect()
}

#[tokio::test]
async fn expired_envelope_does_not_block_followup_dispatch() {
    let r = repo().await;
    // Live dispatch to CMO must still be seen as a cycle.
    insert_envelope(&r, "e1", "root1", "asst-CMO", 0, LIVE).await;
    // Expired `delivered` envelope to COO must NOT be seen (the bug).
    insert_envelope(&r, "e2", "root1", "asst-COO", 0, STALE).await;

    let targets = live_targets(&r, "root1").await;
    assert!(
        targets.contains(&"asst-CMO".to_string()),
        "live envelope should still block (cycle)"
    );
    assert!(
        !targets.contains(&"asst-COO".to_string()),
        "expired envelope must NOT block re-dispatch"
    );
}

#[tokio::test]
async fn chain_depth_ignores_expired_envelopes() {
    let r = repo().await;
    insert_envelope(&r, "e1", "root2", "asst-A", 0, LIVE).await;
    insert_envelope(&r, "e2", "root2", "asst-B", 2, STALE).await; // deep but expired

    let rows = r
        .raw_query(CHAIN_DEPTH_SQL, vec!["root2".into(), NOW.to_string()])
        .await
        .unwrap();
    let max_depth = rows
        .into_iter()
        .next()
        .and_then(|row| row.get::<Option<i64>, _>("max_depth"))
        .unwrap_or(-1);
    assert_eq!(max_depth, 0, "only the live envelope's depth should count");
}

async fn insert_room(r: &SqliteConversationRepository, id: &str, asst: &str, caller: &str, archived: bool) {
    let extra = format!(r#"{{"preset_assistant_id":"{asst}","delegated_from":"{caller}"}}"#);
    let sql = if archived {
        "INSERT INTO conversations (id,user_id,name,type,extra,status,pinned,created_at,updated_at,archived_at) \
         VALUES (?1,'user-1','n','acp',?2,'finished',0,0,1000,2000)"
    } else {
        "INSERT INTO conversations (id,user_id,name,type,extra,status,pinned,created_at,updated_at) \
         VALUES (?1,'user-1','n','acp',?2,'finished',0,0,1000)"
    };
    r.raw_execute(sql, vec![id.into(), extra]).await.unwrap();
}

async fn reuse_room_id(r: &SqliteConversationRepository, asst: &str, caller: &str) -> Option<String> {
    r.raw_query(REUSE_ROOM_SQL, vec!["user-1".into(), asst.into(), caller.into()])
        .await
        .unwrap()
        .into_iter()
        .next()
        .map(|row| row.get::<String, _>("id"))
}

#[tokio::test]
async fn room_reuse_is_scoped_to_the_caller_not_another_conversation() {
    let r = repo().await;
    insert_room(&r, "room-mine", "asst-CMO", "caller-1", false).await;
    insert_room(&r, "room-other", "asst-CMO", "caller-2", false).await;

    // caller-1 must reuse its own room, never caller-2's.
    assert_eq!(
        reuse_room_id(&r, "asst-CMO", "caller-1").await.as_deref(),
        Some("room-mine")
    );
    assert_eq!(
        reuse_room_id(&r, "asst-CMO", "caller-2").await.as_deref(),
        Some("room-other")
    );
    // An unrelated caller gets no reuse (fresh room).
    assert_eq!(reuse_room_id(&r, "asst-CMO", "caller-3").await, None);
}

#[tokio::test]
async fn archived_room_is_never_reused() {
    let r = repo().await;
    insert_room(&r, "room-arch", "asst-CMO", "caller-1", true).await;
    assert_eq!(reuse_room_id(&r, "asst-CMO", "caller-1").await, None);
}

/// A delegable assistant definition whose `id` (asstdef_*) differs from its
/// `assistant_id` column (custom-*). The delegate resolves targets by `id`,
/// but `conversation_service.create` looks up by `assistant_id`; the
/// translation query must bridge that gap or the delegated room is created
/// without a backend/agent_id binding.
async fn insert_delegable_definition(r: &SqliteConversationRepository, def_id: &str, asst_id: &str) {
    r.raw_execute(
        "INSERT INTO assistant_definitions (id, user_id, assistant_id, source, owner_type, name, name_i18n, \
            description_i18n, avatar_type, agent_id, rule_resource_type, recommended_prompts, recommended_prompts_i18n, \
            default_model_mode, default_permission_mode, default_thought_level_mode, default_skills_mode, \
            default_skill_ids, custom_skill_names, default_disabled_builtin_skill_ids, default_mcps_mode, \
            default_mcp_ids, allow_delegation, created_at, updated_at) \
         VALUES (?1,'user-1',?2,'user','user','CMO','{}','{}','none','53861a53','none','[]','{}', \
            'auto','auto','auto','auto','[]','[]','[]','auto','[]',1,0,0)",
        vec![def_id.into(), asst_id.into()],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn delegation_definition_id_translates_to_assistant_id_for_create() {
    let r = repo().await;
    insert_delegable_definition(&r, "asstdef-XYZ", "custom-1788-CMO").await;

    let rows = r
        .raw_query(
            ASSISTANT_ID_FOR_DEFINITION_SQL,
            vec!["user-1".into(), "asstdef-XYZ".into()],
        )
        .await
        .unwrap();
    let got = rows.into_iter().next().map(|row| row.get::<String, _>("assistant_id"));
    assert_eq!(
        got.as_deref(),
        Some("custom-1788-CMO"),
        "create needs the assistant_id column value, not the definition id"
    );
}

#[tokio::test]
async fn delegated_room_inherits_caller_workspace_for_project_context() {
    let r = repo().await;
    r.raw_execute(
        "INSERT INTO conversations (id,user_id,name,type,extra,status,pinned,created_at,updated_at) \
         VALUES ('caller1','user-1','n','acp','{\"workspace\":\"/home/proj/drszepkuti_hu\"}','finished',0,0,0)",
        vec![],
    )
    .await
    .unwrap();
    // A caller with no workspace yields None (falls back to temp provisioning).
    r.raw_execute(
        "INSERT INTO conversations (id,user_id,name,type,extra,status,pinned,created_at,updated_at) \
         VALUES ('caller2','user-1','n','acp','{}','finished',0,0,0)",
        vec![],
    )
    .await
    .unwrap();

    async fn ws(r: &SqliteConversationRepository, id: &str) -> Option<String> {
        r.raw_query(CALLER_WORKSPACE_SQL, vec![id.into()])
            .await
            .unwrap()
            .into_iter()
            .next()
            .and_then(|row| row.get::<Option<String>, _>("ws"))
    }
    assert_eq!(ws(&r, "caller1").await.as_deref(), Some("/home/proj/drszepkuti_hu"));
    assert_eq!(ws(&r, "caller2").await, None);
}

// P2-4: the sender-is-team guard reads the canonical `teamId` marker.
#[test]
fn team_guard_reads_canonical_teamid_key() {
    // A team-owned conversation (camelCase teamId, as provisioning writes) is
    // detected — previously the snake_case lookup made this guard inert.
    assert_eq!(
        super::team_id_from_extra(r#"{"teamId":"team-1"}"#).as_deref(),
        Some("team-1")
    );
    // A plain solo conversation is not team-owned.
    assert_eq!(super::team_id_from_extra(r#"{"backend":"opencode"}"#), None);
}
