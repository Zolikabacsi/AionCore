//! Unit tests for queue, rate_limit, turn_suspend, envelope composer.
//! DB-touching tests live in `service_test.rs` once Phase 3 wires
//! `deliver_now` against the real conversation runtime.

#[cfg(test)]
mod queue {
    use crate::queue::{
        Clock, DelegateQueue, PendingDelegate, SystemClock, TestClock, GLOBAL_LIMIT, PER_TARGET_LIMIT,
    };
    use std::sync::Arc;

    fn make(to: &str, user_id: &str, from: &str, expires_at_ms: i64) -> PendingDelegate {
        PendingDelegate {
            envelope_id: format!("env-{to}"),
            to_conversation_id: to.to_owned(),
            to_assistant_id: format!("assist-{to}"),
            user_id: user_id.to_owned(),
            from_conversation_id: from.to_owned(),
            message: "hello".to_owned(),
            depth: 0,
            expires_at_ms,
        }
    }

    #[test]
    fn push_and_pop_round_trip() {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let q = DelegateQueue::new(clock);
        q.push(make("c1", "u1", "from1", i64::MAX)).unwrap();
        q.push(make("c1", "u1", "from1", i64::MAX)).unwrap();
        q.push(make("c2", "u1", "from1", i64::MAX)).unwrap();
        assert_eq!(q.total_len(), 3);
        let heads = q.snapshot_heads();
        assert_eq!(heads.len(), 2);
        q.pop_head("c1");
        assert_eq!(q.len_for("c1"), 1);
        assert_eq!(q.len_for("c2"), 1);
    }

    #[test]
    fn per_target_limit_enforced() {
        let clock: Arc<dyn Clock> = Arc::new(TestClock::new(0));
        let q = DelegateQueue::new(clock);
        for _ in 0..PER_TARGET_LIMIT {
            q.push(make("c1", "u1", "from1", i64::MAX)).unwrap();
        }
        let result = q.push(make("c1", "u1", "from1", i64::MAX));
        assert!(matches!(result, Err(crate::error::DelegateError::QueueFull)));
    }

    #[test]
    fn global_limit_enforced() {
        let clock: Arc<dyn Clock> = Arc::new(TestClock::new(0));
        let q = DelegateQueue::new(clock);
        let mut count = 0;
        loop {
            let target = format!("c{count}");
            match q.push(make(&target, "u1", "from1", i64::MAX)) {
                Ok(_) => count += 1,
                Err(_) => break,
            }
        }
        assert_eq!(count, GLOBAL_LIMIT);
    }

    #[test]
    fn drop_expired_removes_only_expired() {
        let clock: Arc<dyn Clock> = Arc::new(TestClock::new(1000));
        let q = DelegateQueue::new(clock);
        q.push(make("c1", "u1", "from1", 500)).unwrap();
        q.push(make("c1", "u1", "from1", 1500)).unwrap();
        q.push(make("c2", "u1", "from1", 400)).unwrap();
        let dropped = q.drop_expired();
        assert_eq!(dropped, 2);
        assert_eq!(q.total_len(), 1);
        assert_eq!(q.len_for("c1"), 1);
    }
}

#[cfg(test)]
mod rate_limit {
    use crate::queue::{Clock, TestClock};
    use crate::rate_limit::{DelegateRateLimiter, RateVerdict, OUTBOUND_LIMIT, PAIR_LIMIT};
    use std::sync::Arc;

    fn make() -> (DelegateRateLimiter, Arc<dyn Clock>) {
        let clock: Arc<dyn Clock> = Arc::new(TestClock::new(0));
        (DelegateRateLimiter::new(clock.clone()), clock)
    }

    #[test]
    fn pair_limit_trips_first() {
        let (rl, _clock) = make();
        for _ in 0..PAIR_LIMIT {
            assert_eq!(rl.check_and_record("from", "target"), RateVerdict::Allowed);
        }
        let verdict = rl.check_and_record("from", "target");
        assert!(matches!(verdict, RateVerdict::Tripped { gate: "pair", .. }));
    }

    #[test]
    fn outbound_limit_trips_after_pair() {
        let (rl, _clock) = make();
        for i in 0..PAIR_LIMIT {
            assert!(matches!(
                rl.check_and_record("from", &format!("t{i}")),
                RateVerdict::Allowed
            ));
        }
        for i in PAIR_LIMIT..OUTBOUND_LIMIT {
            assert!(matches!(
                rl.check_and_record("from", &format!("t{i}")),
                RateVerdict::Allowed
            ));
        }
        let verdict = rl.check_and_record("from", "t-final");
        assert!(matches!(verdict, RateVerdict::Tripped { gate: "outbound", .. }));
    }

    #[test]
    fn advance_window_resets_counters() {
        // Use a TestClock directly so we can advance and verify reset.
        let test_clock = Arc::new(TestClock::new(0));
        let rl = DelegateRateLimiter::new(test_clock.clone());
        for _ in 0..PAIR_LIMIT {
            rl.check_and_record("from", "target");
        }
        assert!(matches!(
            rl.check_and_record("from", "target"),
            RateVerdict::Tripped { .. }
        ));
        test_clock.advance(crate::rate_limit::WINDOW_MS + 1);
        assert_eq!(
            rl.check_and_record("from", "target"),
            RateVerdict::Allowed
        );
    }
}

#[cfg(test)]
mod turn_suspend {
    use crate::turn_suspend::{SuspendedTurn, TurnSuspendRegistry};
    use aionui_common::TimestampMs;

    #[test]
    fn park_resolve_lifecycle() {
        let r = TurnSuspendRegistry::new();
        r.park(SuspendedTurn {
            envelope_id: "e1".into(),
            caller_conversation_id: "c1".into(),
            caller_turn_id: "t1".into(),
            target_conversation_id: "c2".into(),
            created_at_ms: 0,
            expires_at_ms: 1_000_000,
        });
        assert_eq!(r.len(), 1);
        assert!(r.get_by_envelope("e1").is_some());
        assert!(r.get_by_caller_turn("t1").is_some());
        let resolved = r.resolve("e1").unwrap();
        assert_eq!(resolved.envelope_id, "e1");
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn drop_expired_yields_only_stale() {
        let r = TurnSuspendRegistry::new();
        let now = 1_500_000;
        r.park(SuspendedTurn {
            envelope_id: "live".into(),
            caller_conversation_id: "c1".into(),
            caller_turn_id: "t_live".into(),
            target_conversation_id: "c2".into(),
            created_at_ms: now,
            expires_at_ms: now + 100_000,
        });
        r.park(SuspendedTurn {
            envelope_id: "stale".into(),
            caller_conversation_id: "c1".into(),
            caller_turn_id: "t_stale".into(),
            target_conversation_id: "c2".into(),
            created_at_ms: now,
            expires_at_ms: now - 100,
        });
        let timed_out = r.drop_expired(now);
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0].envelope_id, "stale");
        assert_eq!(r.len(), 1);
    }
}

#[cfg(test)]
mod envelope {
    use crate::service::compose_delivery_body;
    use aionui_api_types::{DelegateEnvelopeBlock, DelegateEnvelopeKind};
    use aionui_common::TimestampMs;

    #[test]
    fn body_renders_block_with_terminator() {
        let block = DelegateEnvelopeBlock {
            kind: DelegateEnvelopeKind::Dispatch,
            from_agent_id: "a1".into(),
            from_agent_name: "CEO".into(),
            reply_to: Some("caller-conv".into()),
            depth: 2,
            envelope_id: "env-xyz".into(),
            workspace: "same".into(),
            created_at_ms: 123,
        };
        let body = compose_delivery_body(&block, "do the thing");
        assert!(body.starts_with("[[AION_DELEGATE]]"));
        assert!(body.contains("[[/AION_DELEGATE]]"));
        assert!(body.contains("do the thing"));
        assert!(body.contains("env-xyz"));
        assert!(body.contains("depth: 2"));
        assert!(body.contains("Dispatch"));
    }
}
