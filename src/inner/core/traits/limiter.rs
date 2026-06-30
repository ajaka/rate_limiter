use redis::{ErrorKind, FromRedisValue, ParsingError, RedisError, RedisResult, Value};

#[derive(Debug, PartialEq)]
pub struct Decision {
    pub allow: bool,
    pub retry_after: Option<f64>,
    pub remaining: Option<f64>,
}
pub trait Limiter {
    fn check(&mut self, key: &str, cost: f64) -> Decision;
}

impl FromRedisValue for Decision {
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let mut decision = Decision {
            allow: false,
            remaining: None,
            retry_after: None,
        };

        let vec: Vec<f64> = FromRedisValue::from_redis_value(v)?;

        match vec.as_slice() {
            [allowed] if *allowed == 1.0 => {
                decision.allow = true;
                Ok(decision)
            }

            [allowed, retry_after, remaining_tokens] if *allowed == 0.0 => {
                decision.retry_after = Some(*retry_after);
                decision.remaining = Some(*remaining_tokens);
                Ok(decision)
            }
            _ => Err(ParsingError::from("unexpected array format from redis")),
        }
    }
}
