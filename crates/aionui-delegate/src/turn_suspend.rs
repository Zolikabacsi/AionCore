//! Suspended-turn registry for sync-mode `delegate ask`.
//!
//! When a caller invokes `delegate ask`, the runtime:
//!   1. creates the target conversation (if needed),
//!   2. enqueues the question,
//!   3. parks the caller's currently-running turn,
//!   4. lets the target run a turn,
//!   5. injects the target's reply into the caller's suspended turn,
//!   6. resumes the caller's turn.
//!
//! This registry is the in-memory bookkeeping for steps 3-6. It does NOT touch
//! the conversation runtime directly; it only stores the parked-turn
//! descriptor. The actual turn-park / resume is wired in `routes.rs` and
//! `service.rs` where the conversation runtime is available.
//!
//! Lost on restart — same policy as the queue. A sync ask in flight at
//! restart-time will time out and resume with a "sync timed out" message.

use std::collections::HashMap;
use std::sync::Mutex;

use aionui_common::TimestampMs;

#[derive(Debug, Clone)]
pub struct SuspendedTurn {
    pub envelope_id: String,
    pub caller_conversation_id: String,
    pub caller_turn_id: String,
    pub target_conversation_id: String,
    pub created_at_ms: TimestampMs,
    pub expires_at_ms: i64,
}

#[derive(Default)]
struct RegistryState {
    by_envelope: HashMap<String, SuspendedTurn>,
    by_caller_turn: HashMap<String, String>, // caller_turn_id -> envelope_id
}

pub struct TurnSuspendRegistry {
    state: Mutex<RegistryState>,
}

impl TurnSuspendRegistry {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(RegistryState::default()),
        }
    }


    pub fn park(&self, suspended: SuspendedTurn) {
        let mut state = self.lock();
        state
            .by_caller_turn
            .insert(suspended.caller_turn_id.clone(), suspended.envelope_id.clone());
        state.by_envelope.insert(suspended.envelope_id.clone(), suspended);
    }

    pub fn get_by_envelope(&self, envelope_id: &str) -> Option<SuspendedTurn> {
        self.lock().by_envelope.get(envelope_id).cloned()
    }

    pub fn get_by_caller_turn(&self, caller_turn_id: &str) -> Option<SuspendedTurn> {
        let state = self.lock();
        let envelope_id = state.by_caller_turn.get(caller_turn_id)?;
        state.by_envelope.get(envelope_id).cloned()
    }

    /// Mark a sync ask as resolved and remove it from the registry. Returns
    /// the suspended-turn descriptor so the caller can resume the turn.
    pub fn resolve(&self, envelope_id: &str) -> Option<SuspendedTurn> {
        let mut state = self.lock();
        let removed = state.by_envelope.remove(envelope_id)?;
        state.by_caller_turn.remove(&removed.caller_turn_id);
        Some(removed)
    }

    /// Drop expired suspended turns (sync timeout). Returns the list of
    /// envelopes that timed out so the caller can synthesise a timeout reply.
    pub fn drop_expired(&self, now_ms: i64) -> Vec<SuspendedTurn> {
        let mut state = self.lock();
        let expired: Vec<String> = state
            .by_envelope
            .iter()
            .filter(|(_, s)| s.expires_at_ms <= now_ms)
            .map(|(id, _)| id.clone())
            .collect();
        let mut out = Vec::with_capacity(expired.len());
        for id in expired {
            if let Some(removed) = state.by_envelope.remove(&id) {
                state.by_caller_turn.remove(&removed.caller_turn_id);
                out.push(removed);
            }
        }
        out
    }

    pub fn len(&self) -> usize {
        self.lock().by_envelope.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().by_envelope.is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RegistryState> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Default for TurnSuspendRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make(envelope_id: &str, caller_turn_id: &str, expires_at: i64) -> SuspendedTurn {
        SuspendedTurn {
            envelope_id: envelope_id.to_owned(),
            caller_conversation_id: "c1".into(),
            caller_turn_id: caller_turn_id.to_owned(),
            target_conversation_id: "c2".into(),
            created_at_ms: 0,
            expires_at_ms: expires_at,
        }
    }

    #[test]
    fn park_and_resolve_round_trip() {
        let r = TurnSuspendRegistry::new();
        r.park(make("e1", "t1", 1_000_000));
        let s = r.get_by_envelope("e1").unwrap();
        assert_eq!(s.caller_turn_id, "t1");
        let s2 = r.get_by_caller_turn("t1").unwrap();
        assert_eq!(s2.envelope_id, "e1");
        let resolved = r.resolve("e1").unwrap();
        assert_eq!(resolved.envelope_id, "e1");
        assert!(r.get_by_envelope("e1").is_none());
        assert!(r.get_by_caller_turn("t1").is_none());
    }

    #[test]
    fn drop_expired_yields_timeouts() {
        let r = TurnSuspendRegistry::new();
        r.park(make("live", "t_live", 2_000_000));
        r.park(make("stale", "t_stale", 1_000_000));
        let timed_out = r.drop_expired(1_500_000);
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0].envelope_id, "stale");
        assert!(r.get_by_envelope("live").is_some());
        assert!(r.get_by_envelope("stale").is_none());
    }
}
