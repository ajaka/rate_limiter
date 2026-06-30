use std::time::{SystemTime, UNIX_EPOCH};

use redis::{ConnectionLike, ParsingError, RedisError};

use crate::{
    Decision, Limiter,
    inner::core::impls::com::{check, get_script},
};

pub struct TokenBucketRedisSync<'a, C>
where
    C: ConnectionLike,
{
    bucket_size: f64,
    refill_rate: f64,
    conn: &'a mut C,
}

impl<'a, C> TokenBucketRedisSync<'a, C>
where
    C: ConnectionLike,
{
    pub fn new(bucket_size: f64, refill_rate: f64, conn: &'a mut C) -> Self {
        check(bucket_size, refill_rate);
        Self {
            bucket_size,
            refill_rate,
            conn,
        }
    }
}

impl<'a, C> Limiter for TokenBucketRedisSync<'a, C>
where
    C: ConnectionLike,
{
    fn check(&mut self, key: &str, cost: f64) -> Decision {
        let script = get_script();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let res: Result<Decision, RedisError> = script
            .key(key)
            .arg(now)
            .arg(self.bucket_size)
            .arg(self.refill_rate)
            .arg(cost)
            .invoke(self.conn);
        res.unwrap()
    }
}
