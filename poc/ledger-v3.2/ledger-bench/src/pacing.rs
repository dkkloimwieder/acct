//! Open-loop arrival pacing (the acct-0at4.8 architecture-agnostic core,
//! carried from the v3.1 harness).
//!
//! Closed-loop load — N callers each *submit-then-wait* — suffers **coordinated
//! omission**: when the system stalls, the callers stop generating, so the
//! stall never enters the latency distribution. The reported tail is fiction
//! exactly where it matters.
//!
//! The fix (wrk2 / Gil-Tene): drive a fixed offered rate on an **absolute
//! schedule** and measure each submission's latency from its *intended*
//! send-time, not from when a free caller happened to issue it. The schedule
//! is NEVER rebased to `now` when a caller falls behind; a late fire returns
//! an intended instant in the past, so the caller's `ack - intended` latency
//! books the backlog — the honest saturation signal.

use std::time::Instant as StdInstant;

use rand::rngs::StdRng;
use rand::Rng;
use rand_distr::Exp1;
use tokio::time::Instant as TokioInstant;

/// Arrival process shaping the inter-submission spacing under a target rate.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum ArrivalProcess {
    /// Deterministic: fire exactly every `interval`. Lowest-variance offered
    /// load; an A/B against Poisson isolates burst effects.
    Uniform,
    /// Poisson: exponential inter-arrival with mean `interval`. Superposing
    /// the caller pool yields an aggregate Poisson process of the target rate
    /// — the memoryless arrival model the saturation methodology assumes.
    #[default]
    Poisson,
}

impl ArrivalProcess {
    pub fn label(self) -> &'static str {
        match self {
            ArrivalProcess::Uniform => "uniform",
            ArrivalProcess::Poisson => "poisson",
        }
    }
}

/// One caller's absolute-schedule pacer. `None` = closed-loop (no pacing).
pub struct Pacer {
    /// Mean per-caller inter-arrival (seconds): `callers / target_rate`.
    interval_secs: f64,
    process: ArrivalProcess,
    /// The next scheduled fire instant. Advances by one step per `wait_next`;
    /// never rebased to `now`.
    next_fire: TokioInstant,
}

impl Pacer {
    /// Build a per-caller pacer for a total offered `target_rate` (trx/s)
    /// spread across `callers`, or `None` for full-blast closed-loop
    /// (`target_rate` unset or 0). `caller_id` staggers the initial phase
    /// across one interval so the pool's first arrivals spread instead of
    /// thundering at the barrier. `start` is the caller's post-barrier epoch.
    pub fn new(
        target_rate: Option<u64>,
        callers: usize,
        caller_id: usize,
        process: ArrivalProcess,
        start: TokioInstant,
    ) -> Option<Self> {
        let rate = target_rate.filter(|r| *r > 0)?;
        let callers = callers.max(1);
        let interval_secs = callers as f64 / rate as f64;
        let stagger = interval_secs * (caller_id as f64 / callers as f64);
        Some(Self {
            interval_secs,
            process,
            next_fire: start + std::time::Duration::from_secs_f64(stagger),
        })
    }

    /// Await the next scheduled arrival; return its **intended** send-time
    /// (std clock, for latency arithmetic against `Instant::now()`). The
    /// schedule advances by one step and is never rebased: a caller that is
    /// behind gets an instant in the past, immediately.
    pub async fn wait_next(&mut self, rng: &mut StdRng) -> StdInstant {
        let intended = self.next_fire;
        let now = TokioInstant::now();
        if intended > now {
            tokio::time::sleep_until(intended).await;
        }
        self.next_fire = intended + self.step(rng);
        intended.into_std()
    }

    /// The next inter-arrival gap.
    fn step(&self, rng: &mut StdRng) -> std::time::Duration {
        let secs = match self.process {
            ArrivalProcess::Uniform => self.interval_secs,
            // Exp1 samples Exp(rate=1); scaling by the mean interval yields
            // Exp(rate=1/interval), i.e. a Poisson process of that rate.
            ArrivalProcess::Poisson => {
                let x: f64 = rng.sample(Exp1);
                self.interval_secs * x
            }
        };
        std::time::Duration::from_secs_f64(secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn unset_or_zero_rate_is_closed_loop() {
        let start = TokioInstant::now();
        assert!(Pacer::new(None, 4, 0, ArrivalProcess::Poisson, start).is_none());
        assert!(Pacer::new(Some(0), 4, 0, ArrivalProcess::Poisson, start).is_none());
        assert!(Pacer::new(Some(100), 4, 0, ArrivalProcess::Poisson, start).is_some());
    }

    #[test]
    fn uniform_step_is_the_mean_interval() {
        // 4 callers at 400 trx/s ⇒ 10ms per-caller interval, deterministically.
        let p = Pacer::new(Some(400), 4, 0, ArrivalProcess::Uniform, TokioInstant::now()).unwrap();
        let mut rng = StdRng::seed_from_u64(1);
        let step = p.step(&mut rng);
        assert!((step.as_secs_f64() - 0.010).abs() < 1e-9);
    }

    #[test]
    fn poisson_steps_average_the_mean_interval() {
        let p = Pacer::new(Some(400), 4, 0, ArrivalProcess::Poisson, TokioInstant::now()).unwrap();
        let mut rng = StdRng::seed_from_u64(42);
        let n = 100_000;
        let sum: f64 = (0..n).map(|_| p.step(&mut rng).as_secs_f64()).sum();
        let mean = sum / n as f64;
        assert!((mean - 0.010).abs() < 0.0005, "mean={mean}");
    }

    #[tokio::test(start_paused = true)]
    async fn behind_schedule_returns_intended_in_the_past() {
        // A stalled caller books intended times that fall behind `now` — the
        // coordinated-omission guarantee.
        let start = TokioInstant::now();
        let mut p = Pacer::new(Some(1000), 1, 0, ArrivalProcess::Uniform, start).unwrap();
        let mut rng = StdRng::seed_from_u64(7);
        tokio::time::advance(std::time::Duration::from_millis(100)).await;
        let now_std = TokioInstant::now().into_std();
        let first = p.wait_next(&mut rng).await;
        assert!(first <= now_std, "intended must be at/behind now when stalled");
    }
}
