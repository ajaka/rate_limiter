pub struct Decision {
    pub allow: bool,
    pub retry_after: Option<f64>,
    pub remaining: Option<f64>,
}

pub trait Limiter {
    fn check(&self, key: String, cost: f64) -> Decision;
}
