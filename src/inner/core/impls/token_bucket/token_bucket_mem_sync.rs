use moka::{ops::compute::Op, sync::Cache};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{Decision, Limiter, inner::core::impls::com::check};

#[derive(Clone, Copy)]
struct TokenBucketData {
    last_seen: f64,
    tokens: f64,
}

pub struct TokenBucketMemorySync {
    bucket_size: f64,
    refill_rate_per_sec: f64,
    data: Cache<String, TokenBucketData>,
}

impl TokenBucketMemorySync {
    pub fn new(bucket_size: f64, refill_rate_per_sec: f64, max_capacity: u64) -> Self {
        check(bucket_size, refill_rate_per_sec);
        assert!(max_capacity > 0, "max_capacity must be greater than 0");

        Self {
            bucket_size,
            refill_rate_per_sec,
            data: Cache::builder().max_capacity(max_capacity).build(),
        }
    }
}
impl Limiter for TokenBucketMemorySync {
    fn check(&mut self, key: &str, cost: f64) -> Decision {
        let mut decision = Decision {
            allow: false,
            remaining: None,
            retry_after: None,
        };

        if !cost.is_finite() || cost < 0.0 || cost > self.bucket_size {
            return decision;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        self.data.entry(key.to_owned()).and_compute_with(|e| {
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

    fn exhaust() -> Decision {
        let mut limiter = TokenBucketMemorySync::new(3.0, 0.001, 100_000);
        let key = "user_123";

        for _ in 0..3 {
            limiter.check(key, 1.0);
        }
        limiter.check(key, 1.0)
    }

    fn normal(size: f64, refill: f64, cap: u64) -> Decision {
        let key = "user_123";
        let mut limiter = TokenBucketMemorySync::new(size, refill, cap);
        limiter.check(key, 1.0)
    }

    #[test]
    fn bench_allow() {
        let result = normal(100.0, 10.0, 1_000);
        assert!(result.allow, "Should allow user to pass through")
    }

    #[test]
    fn should_deny() {
        let result = exhaust();
        assert!(!result.allow, "Should not allow to pass through")
    }

    #[test]
    #[should_panic]
    fn should_panic() {
        normal(-34.0, 15.0, 100);
    }

    #[test]
    #[should_panic]
    fn should_panic2() {
        normal(100.0, -15.0, 100);
    }

    #[test]
    #[should_panic]
    fn should_panic3() {
        normal(100.0, 15.0, 0);
    }

    #[test]
    fn should_return_valid_data1() {
        let result = exhaust();
        assert!(
            !result.allow && result.retry_after.is_some() && result.remaining.is_some(),
            "Should return retry_after and remaining after deny"
        )
    }

    #[test]
    fn should_return_valid_data2() {
        let result = normal(100.0, 10.0, 1_000);
        assert!(
            result.allow && result.retry_after.is_none() && result.remaining.is_none(),
            "Should return None after allow"
        )
    }
}
