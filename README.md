# rate_limiter

A distributed rate limiter written in Rust, implementing the **token bucket algorithm** with two storage backends:

- **In-memory** — `TokenBucketMemorySync` using [Moka](https://github.com/moka-rs/moka) for high-performance concurrent access
- **Redis** — `TokenBucketRedisSync` using atomic Lua scripts for distributed coordination

Both implement a common `Limiter` trait for consistent APIs.

## Features

- **Token bucket algorithm** — Smooth rate limiting with burst support
- **Two backends** — Local (Moka cache) or distributed (Redis)
- **Thread-safe** — Sync implementations using `&mut self`
- **Zero-copy decisions** — Returns `Decision { allow, retry_after, remaining }`
- **Atomic Redis operations** — Lua script prevents race conditions
- **Automatic expiry** — Redis keys auto-expire after bucket drains
- **Input validation** — Panics on invalid config (negative rates, zero capacity, etc.)

## Installation

```toml
[dependencies]
rate_limiter = { path = "../rate_limiter" }
```

Requires Rust 1.85+ (edition 2024).

## Quick Start

### In-Memory (Single Instance)

```rust
use rate_limiter::{TokenBucketMemorySync, Limiter};

// 100 tokens max, refill 10 tokens/sec, up to 10_000 unique keys
let mut limiter = TokenBucketMemorySync::new(100.0, 10.0, 10_000);

let decision = limiter.check("user_123", 1.0);
if decision.allow {
    println!("Request allowed!");
} else {
    println!("Rate limited. Retry after {:.1}s", decision.retry_after.unwrap());
}
```

### Redis (Distributed)

```rust
use rate_limiter::{TokenBucketRedisSync, Limiter};
use redis::Client;

let client = Client::open("redis://127.0.0.1/").unwrap();
let mut conn = client.get_connection().unwrap();

// 1000 tokens max, refill 50 tokens/sec
let mut limiter = TokenBucketRedisSync::new(1000.0, 50.0, &mut conn);

let decision = limiter.check("api_key_abc", 5.0);
if decision.allow {
    // Process request
} else {
    // Return 429 with Retry-After header
}
```

## API

### `Limiter` Trait

```rust
pub trait Limiter {
    fn check(&mut self, key: &str, cost: f64) -> Decision;
}
```

| Parameter | Description                                    |
| --------- | ---------------------------------------------- |
| `key`     | Unique identifier (user ID, API key, IP, etc.) |
| `cost`    | Tokens to consume (must be ≤ `bucket_size`)    |

### `Decision` Struct

```rust
pub struct Decision {
    pub allow: bool,
    pub retry_after: Option<f64>,  // Seconds until request would succeed
    pub remaining: Option<f64>,    // Tokens currently available
}
```

| Field         | When `allow=true` | When `allow=false` |
| ------------- | ----------------- | ------------------ |
| `allow`       | `true`            | `false`            |
| `retry_after` | `None`            | `Some(seconds)`    |
| `remaining`   | `None`            | `Some(tokens)`     |

### `TokenBucketMemorySync`

```rust
pub fn new(bucket_size: f64, refill_rate_per_sec: f64, max_capacity: u64) -> Self
```

| Parameter             | Description                                        |
| --------------------- | -------------------------------------------------- |
| `bucket_size`         | Maximum tokens in bucket (burst capacity)          |
| `refill_rate_per_sec` | Tokens added per second                            |
| `max_capacity`        | Maximum unique keys to track (Moka cache capacity) |

### `TokenBucketRedisSync`

```rust
pub fn new<'a, C>(bucket_size: f64, refill_rate: f64, conn: &'a mut C) -> Self
where C: redis::ConnectionLike
```

| Parameter     | Description                                     |
| ------------- | ----------------------------------------------- |
| `bucket_size` | Maximum tokens in bucket                        |
| `refill_rate` | Tokens added per second                         |
| `conn`        | Mutable Redis connection (any `ConnectionLike`) |

## Algorithm Details

The token bucket works as follows:

1. Each key has a bucket with `tokens` (current) and `last_seen` (timestamp)
2. On `check(key, cost)`:
   - Calculate `elapsed = now - last_seen`
   - Refill: `tokens = min(bucket_size, tokens + elapsed * refill_rate)`
   - If `tokens >= cost`: deduct cost, allow request
   - Else: deny, compute `retry_after = ceil((cost - tokens) / refill_rate)`

**Redis implementation** uses a single atomic Lua script:

- Reads both keys (`{key}:t` for tokens, `{key}:l` for last_seen)
- Computes refill and checks allowance
- On allow: writes new values + sets TTL = `ceil(bucket_size / refill_rate)`
- On deny: returns retry info without writing

## Configuration Guide

| Use Case                                        | bucket_size | refill_rate   | Example                   |
| ----------------------------------------------- | ----------- | ------------- | ------------------------- |
| API rate limit (100 req/min)                    | 100         | 100/60 ≈ 1.67 | `new(100.0, 1.67, ...)`   |
| Burst allowance (1000 burst, 100/sec sustained) | 1000        | 100           | `new(1000.0, 100.0, ...)` |
| Strict throttle (10 req/sec, no burst)          | 10          | 10            | `new(10.0, 10.0, ...)`    |
| Login attempts (5 per minute)                   | 5           | 5/60 ≈ 0.083  | `new(5.0, 0.083, ...)`    |

## Testing

```bash
cargo test
```

## License

MIT
