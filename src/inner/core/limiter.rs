pub trait Limiter {
    fn check(&self, key: String, cost: f64) -> bool;
}
