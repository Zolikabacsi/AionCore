//! Loop and storm protection for cross-agent delegation.
//!
//! Mirrors `aionui-session-message::rate_limit` but tracks (sender_conv,
//! target_assistant_id) pairs so a busy caller fanning out to one specific
//! assistant gets capped without blocking fan-out to others. Two sliding
//! windows: per-caller outbound, per-pair.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::queue::Clock;

pub const WINDOW_MS: i64 = 10 * 60 * 1000;
/// Per-conversation outbound cap within the window.
pub const OUTBOUND_LIMIT: u32 = 20;
/// Per-(from, target_assistant) cap within the window.
pub const PAIR_LIMIT: u32 = 10;

#[derive(Debug, PartialEq, Eq)]
pub enum RateVerdict {
    Allowed,
    Tripped { gate: &'static str, window_count: u32 },
}

#[derive(Default)]
struct Windows {
    outbound: HashMap<String, Vec<i64>>,
    pair: HashMap<(String, String), Vec<i64>>,
}

pub struct DelegateRateLimiter {
    windows: Mutex<Windows>,
    clock: Arc<dyn Clock>,
}

impl DelegateRateLimiter {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            windows: Mutex::new(Windows::default()),
            clock,
        }
    }

    pub fn check_and_record(&self, from_conv: &str, target_assistant_id: &str) -> RateVerdict {
        let now = self.clock.now_ms();
        let cutoff = now - WINDOW_MS;
        let mut windows = self.windows.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        let outbound = windows.outbound.entry(from_conv.to_owned()).or_default();
        outbound.retain(|at| *at > cutoff);
        let outbound_count = outbound.len() as u32;

        let pair = windows
            .pair
            .entry((from_conv.to_owned(), target_assistant_id.to_owned()))
            .or_default();
        pair.retain(|at| *at > cutoff);
        let pair_count = pair.len() as u32;

        if pair_count >= PAIR_LIMIT {
            return RateVerdict::Tripped {
                gate: "pair",
                window_count: pair_count,
            };
        }
        if outbound_count >= OUTBOUND_LIMIT {
            return RateVerdict::Tripped {
                gate: "outbound",
                window_count: outbound_count,
            };
        }

        pair.push(now);
        windows
            .outbound
            .get_mut(from_conv)
            .expect("outbound entry was just created")
            .push(now);
        RateVerdict::Allowed
    }
}
