use moka::{ops::compute::Op, sync::Cache};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::inner::core::limiter::{Decision, Limiter};

#[derive(Clone, Copy)]
struct TokenBucketData {
    last_seen: f64,
    tokens: f64,
}

pub struct TokenBucketMemory {
    bucket_size: f64,
    refill_rate_per_sec: f64,
    data: Cache<String, TokenBucketData>,
}

impl TokenBucketMemory {
    pub fn new(bucket_size: f64, refill_rate_per_sec: f64, max_capacity: u64) -> Self {
        assert!(
            bucket_size.is_finite() && bucket_size > 0.0,
            "bucket_size must be finite and greater than 0"
        );
        assert!(
            refill_rate_per_sec.is_finite() && refill_rate_per_sec > 0.0,
            "refill_rate_per_sec must be finite and greater than 0"
        );
        assert!(max_capacity > 0, "max_capacity must be greater than 0");

        Self {
            bucket_size,
            refill_rate_per_sec,
            data: Cache::builder().max_capacity(max_capacity).build(),
        }
    }
}

impl Limiter for TokenBucketMemory {
    fn check(&self, key: String, cost: f64) -> Decision {
        let mut decision = Decision {
            allow: false,
            remaining: None,
            retry_after: None,
        };
        if !cost.is_finite() || cost < 0.0 {
            return decision;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        self.data.entry(key).and_compute_with(|e| {
            let mut state = match e {
                Some(entry) => entry.into_value(),
                None => TokenBucketData {
                    last_seen: now,
                    tokens: self.bucket_size,
                },
            };
            let elapsed = now - state.last_seen;
            state.tokens =
                (state.tokens + elapsed * self.refill_rate_per_sec).min(self.bucket_size);
            if state.tokens.ge(&cost) {
                state.tokens -= cost;
                state.last_seen = now;
                decision.allow = true;
                Op::Put(state)
            } else {
                let rem_to_allow = cost - state.tokens;
                let time_to_retry = (rem_to_allow / self.refill_rate_per_sec).ceil().max(1.0);
                decision.retry_after = Some(time_to_retry);
                decision.remaining = Some(state.tokens);
                Op::Nop
            }
        });
        decision
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bench_allow() {
        let key = "user_123".to_string();
        let limiter = TokenBucketMemory::new(100.0, 10.0, 1_000);
        let result = limiter.check(key, 1.0);
        assert!(result.allow, "Should allow user to pass through")
    }

    #[test]
    fn bench_saturated() {
        let limiter = TokenBucketMemory::new(100.0, 10.0, 100_000);
        let key = "user_123".to_string();

        for _ in 0..100 {
            limiter.check(key.clone(), 1.0);
        }
        let result = limiter.check(key, 1.0);
        assert!(!result.allow, "Should not allow to pass through")
    }

    #[test]
    #[should_panic]
    fn should_panic() {
        TokenBucketMemory::new(-34.0, 15.0, 100);
    }

    #[test]
    #[should_panic]
    fn should_panic2() {
        TokenBucketMemory::new(100.0, -15.0, 100);
    }

    #[test]
    #[should_panic]
    fn should_panic3() {
        TokenBucketMemory::new(100.0, 15.0, 0);
    }
}
