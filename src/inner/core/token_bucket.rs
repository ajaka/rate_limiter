use moka::{ops::compute::Op, sync::Cache};
use std::time::{Duration, Instant};

use crate::inner::core::limiter::Limiter;

#[derive(Clone, Copy)]
struct TokenBucketData {
    last_seen: Instant,
    tokens: f64,
}

pub struct TokenBucketAlgorithm {
    bucket_size: f64,
    refill_rate_per_sec: f64,
    data: Cache<String, TokenBucketData>,
}

impl TokenBucketAlgorithm {
    pub fn new(bucket_size: f64, refill_rate_per_sec: f64, max_capacity: u64) -> Self {
        Self {
            bucket_size,
            refill_rate_per_sec,
            data: Cache::builder()
                .time_to_idle(Duration::from_secs(120))
                .max_capacity(max_capacity)
                .build(),
        }
    }
}

impl Limiter for TokenBucketAlgorithm {
    fn check(&self, key: String, cost: f64) -> bool {
        let mut allow = false;
        let now = Instant::now();
        self.data.entry(key).and_compute_with(|e| {
            let mut state = match e {
                Some(entry) => entry.into_value(),
                None => TokenBucketData {
                    last_seen: now,
                    tokens: self.bucket_size,
                },
            };
            let elapsed = now.duration_since(state.last_seen).as_secs_f64();
            state.tokens =
                (state.tokens + elapsed * self.refill_rate_per_sec).min(self.bucket_size);
            if state.tokens.ge(&cost) {
                state.tokens -= cost;
                state.last_seen = now;
                allow = true;
                Op::Put(state)
            } else {
                Op::Nop
            }
        });
        allow
    }
}
