//! In-memory pending-delivery queue for the async `delegate dispatch` path.
//!
//! When the target conversation is busy (turn in progress, runtime restarting,
//! etc.) the dispatch is enqueued here, and a background drainer re-attempts
//! delivery. Lost on restart, same policy as `aionui-session-message`'s queue.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use crate::error::DelegateError;

/// Per-target cap. Matches `session-message`'s policy.
pub const PER_TARGET_LIMIT: usize = 20;
/// Cap across all targets.
pub const GLOBAL_LIMIT: usize = 200;
/// Single message TTL. 5 minutes — tighter than `session-message`'s 10 because
/// delegation replies usually matter more time-sensitively (caller may be
/// waiting in a sync round-trip or a UI display).
pub const TTL_MS: i64 = 5 * 60 * 1000;

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        aionui_common::now_ms()
    }
}

pub struct TestClock {
    now_ms: Mutex<i64>,
}

impl TestClock {
    pub fn new(now_ms: i64) -> Self {
        Self {
            now_ms: Mutex::new(now_ms),
        }
    }
    pub fn advance(&self, ms: i64) {
        *self.now_ms.lock().expect("test clock lock") += ms;
    }
}

impl Clock for TestClock {
    fn now_ms(&self) -> i64 {
        *self.now_ms.lock().expect("test clock lock")
    }
}

#[derive(Debug, Clone)]
pub struct PendingDelegate {
    pub envelope_id: String,
    pub to_conversation_id: String,
    pub to_assistant_id: String,
    pub user_id: String,
    pub from_conversation_id: String,
    /// Full delivery content, recipient block already prepended.
    pub message: String,
    pub depth: u32,
    pub expires_at_ms: i64,
}

#[derive(Default)]
struct QueueState {
    by_target: HashMap<String, VecDeque<PendingDelegate>>,
    total: usize,
}

pub struct DelegateQueue {
    state: Mutex<QueueState>,
    clock: Arc<dyn Clock>,
}

impl DelegateQueue {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            state: Mutex::new(QueueState::default()),
            clock,
        }
    }

    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    pub fn push(&self, item: PendingDelegate) -> Result<(), DelegateError> {
        let mut state = self.lock();
        if state.total >= GLOBAL_LIMIT {
            return Err(DelegateError::QueueFull);
        }
        let bucket = state.by_target.entry(item.to_conversation_id.clone()).or_default();
        if bucket.len() >= PER_TARGET_LIMIT {
            return Err(DelegateError::QueueFull);
        }
        bucket.push_back(item);
        state.total += 1;
        Ok(())
    }

    pub fn drop_expired(&self) -> usize {
        let now = self.clock.now_ms();
        let mut state = self.lock();
        let mut dropped = 0;
        for bucket in state.by_target.values_mut() {
            let before = bucket.len();
            bucket.retain(|item| item.expires_at_ms > now);
            dropped += before - bucket.len();
        }
        state.by_target.retain(|_, bucket| !bucket.is_empty());
        state.total -= dropped;
        dropped
    }

    pub fn snapshot_heads(&self) -> Vec<PendingDelegate> {
        self.lock()
            .by_target
            .values()
            .filter_map(|bucket| bucket.front().cloned())
            .collect()
    }

    pub fn pop_head(&self, to_conversation_id: &str) {
        let mut state = self.lock();
        let Some(bucket) = state.by_target.get_mut(to_conversation_id) else {
            return;
        };
        if bucket.pop_front().is_some() {
            state.total -= 1;
        }
        if state.by_target.get(to_conversation_id).is_some_and(VecDeque::is_empty) {
            state.by_target.remove(to_conversation_id);
        }
    }

    pub fn clear_for(&self, to_conversation_id: &str) -> usize {
        let mut state = self.lock();
        let removed = state.by_target.remove(to_conversation_id).map_or(0, |b| b.len());
        state.total -= removed;
        removed
    }

    pub fn is_empty(&self) -> bool {
        self.lock().total == 0
    }

    pub fn total_len(&self) -> usize {
        self.lock().total
    }

    pub fn len_for(&self, to_conversation_id: &str) -> usize {
        self.lock().by_target.get(to_conversation_id).map_or(0, VecDeque::len)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, QueueState> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
