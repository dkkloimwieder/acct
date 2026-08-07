//! The feed-consumer loop: ensure the slot exists, then poll
//! `ingest_once` — sleeping only when the slot had nothing to deliver.
//!
//! Exactly one instance owns the slot (single-consumer posture; the slot has
//! one cursor). Configuration via environment:
//!
//!   FEED_DSN          postgres DSN            (default: dev poc_v3_2)
//!   FEED_SLOT         replication slot name   (default: ledger_feed)
//!   FEED_PUBLICATION  publication name        (default: ledger_feed)
//!   FEED_BATCH        peek message budget     (default: 10000)
//!   FEED_POLL_MS      idle sleep between polls (default: 250)

use ledger_feed::FeedConsumer;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let dsn = env_or("FEED_DSN", "postgres://acct:acct_dev@localhost:5111/poc_v3_2");
    let slot = env_or("FEED_SLOT", "ledger_feed");
    let publication = env_or("FEED_PUBLICATION", "ledger_feed");
    let batch: i32 = env_or("FEED_BATCH", "10000").parse().expect("FEED_BATCH must be an int");
    let poll_ms: u64 = env_or("FEED_POLL_MS", "250").parse().expect("FEED_POLL_MS must be an int");

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect to feed database");

    let consumer = FeedConsumer::new(pool, slot, publication);
    if consumer.ensure_slot().await.expect("ensure replication slot") {
        println!("feed: created slot {}", consumer.slot());
    }
    println!("feed: consuming slot {} (batch {batch}, idle poll {poll_ms}ms)", consumer.slot());

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("feed: shutting down");
                return;
            }
            report = consumer.ingest_once(batch) => {
                // An errored tick (deadlock-detector victim under a close
                // sweep, transient connection loss) is retried, not fatal:
                // the cursor did not advance, so the next peek re-delivers
                // the batch and the idempotent apply converges.
                let report = match report {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("feed: ingest error (retrying): {e}");
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        continue;
                    }
                };
                if report.inserts > 0 {
                    println!(
                        "feed: {} events, {} pools marked, {} floors lowered, {} pools throttled, cursor {}",
                        report.inserts,
                        report.pools_marked,
                        report.floors_lowered,
                        report.pools_throttled,
                        report.advanced_to.as_deref().unwrap_or("unchanged"),
                    );
                }
                if report.messages == 0 {
                    tokio::time::sleep(Duration::from_millis(poll_ms)).await;
                }
            }
        }
    }
}
