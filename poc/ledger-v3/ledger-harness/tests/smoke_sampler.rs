//! Smoke test: spawn the sampler for 1s against poc_v3, assert it
//! captured at least one tick without errors.

#[path = "../src/sampler.rs"]
mod sampler;

use sampler::PgLocksSampler;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

#[tokio::test]
#[ignore = "needs running poc_v3"]
async fn sampler_captures_ticks() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect("postgres://acct:acct_dev@localhost:5111/poc_v3")
        .await
        .expect("connect");

    let s = PgLocksSampler::spawn(pool, 100).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let report = s.shutdown().await;

    assert!(
        report.samples_taken >= 8,
        "expected ~10 samples in 1.1s, got {}",
        report.samples_taken
    );
    assert_eq!(report.poll_errors, 0);
    assert!(report.duration_ms >= 1000);
}
