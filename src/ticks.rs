use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct Ticks {
    period: Option<Duration>,
    #[cfg(feature = "period_bias")]
    bias: f64,
    next_at: Option<Instant>,
}

impl Default for Ticks {
    fn default() -> Self {
        Self::new()
    }
}

impl Ticks {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            period: None,
            #[cfg(feature = "period_bias")]
            bias: 0.0,
            next_at: None,
        }
    }

    #[must_use]
    pub const fn with_period(mut self, period: Duration) -> Self {
        self.period = Some(period);
        self
    }

    #[cfg(feature = "period_bias")]
    #[must_use]
    pub const fn with_bias(mut self, bias: f64) -> Self {
        self.bias = bias;
        self
    }

    pub fn start(&mut self) {
        if self.period.is_some() && self.next_at.is_none() {
            self.reschedule();
        }
    }

    pub fn reschedule(&mut self) {
        if let Some(period) = self.period {
            let actual_period = self.apply_bias(period);
            self.next_at = Some(Instant::now() + actual_period);
        }
    }

    #[must_use]
    pub fn reached(&self) -> bool {
        self.next_at
            .is_some_and(|next_at| Instant::now() >= next_at)
    }

    #[must_use]
    pub fn time_left(&self) -> Option<Duration> {
        self.next_at.map(|next_at| {
            let now = Instant::now();
            if now >= next_at {
                Duration::ZERO
            } else {
                next_at - now
            }
        })
    }

    #[cfg(feature = "period_bias")]
    fn apply_bias(&self, period: Duration) -> Duration {
        if self.bias == 0.0 {
            return period;
        }

        use rand::Rng;
        let mut rng = rand::rng();
        let factor = 1.0 + rng.random_range(-self.bias..=self.bias);
        Duration::from_secs_f64(period.as_secs_f64() * factor)
    }

    #[cfg(not(feature = "period_bias"))]
    #[allow(clippy::unused_self)]
    const fn apply_bias(&self, period: Duration) -> Duration {
        period
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_period() {
        let ticks = Ticks::new();
        assert!(!ticks.reached());
        assert!(ticks.time_left().is_none());
    }

    #[test]
    fn test_with_period() {
        let mut ticks = Ticks::new().with_period(Duration::from_millis(10));
        ticks.start();

        assert!(!ticks.reached());
        assert!(ticks.time_left().is_some());

        std::thread::sleep(Duration::from_millis(15));
        assert!(ticks.reached());
    }

    #[test]
    fn test_reschedule() {
        let mut ticks = Ticks::new().with_period(Duration::from_millis(10));
        ticks.start();

        std::thread::sleep(Duration::from_millis(15));
        assert!(ticks.reached());

        ticks.reschedule();
        assert!(!ticks.reached());
    }
}
