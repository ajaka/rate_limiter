use redis::Script;
use std::sync::OnceLock;

const RATE_LIMIT_SCRIPT: &str = r#"
    local data = redis.call('MGET', KEYS[1] .. ':t', KEYS[1] .. ':l')
    local now = tonumber(ARGV[1])
    local bucket_size = tonumber(ARGV[2])
    local refill_rate = tonumber(ARGV[3])
    local cost = tonumber(ARGV[4])

    local tokens = tonumber(data[1]) or bucket_size
    local last = tonumber(data[2]) or now

    local elapsed = math.max(0, now - last)
    tokens = math.min(bucket_size, tokens + elapsed * refill_rate)

    if tokens >= cost then
        tokens = tokens - cost
        local ttl = math.ceil(bucket_size / refill_rate)
        redis.call('MSET', KEYS[1] .. ':t', tokens, KEYS[1] .. ':l', now)
        redis.call('EXPIRE', KEYS[1] .. ':t', ttl)
        redis.call('EXPIRE', KEYS[1] .. ':l', ttl)
        return {1}
    else
        return {0, (cost - tokens) / refill_rate, tokens}
    end
"#;

// Global OnceLock to store the pre-loaded Redis Script object
static LAZY_SCRIPT: OnceLock<Script> = OnceLock::new();

/// Returns a reference to the initialized Redis Script object
pub fn get_script() -> &'static Script {
    LAZY_SCRIPT.get_or_init(|| {
        // Creates a Script wrapper. The redis crate handles SHA1 generation internally.
        Script::new(RATE_LIMIT_SCRIPT)
    })
}

pub fn check(bucket_size: f64, refill_rate: f64) {
    assert!(
        bucket_size.is_finite() && bucket_size > 0.0,
        "bucket_size must be finite and greater than 0"
    );
    assert!(
        refill_rate.is_finite() && refill_rate > 0.0,
        "refill_rate_per_sec must be finite and greater than 0"
    );
}
