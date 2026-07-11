//! Hard footprint budget: no bench run is allowed to grow the database +
//! pg_wal beyond a fixed cap (default 20 GB). The recalc engine's
//! generation-append bookkeeping makes a high-rate backdating workload
//! write-amplify quadratically (every backdate re-costs the settled tail
//! behind it, appending a new settlement generation per affected depletion) —
//! left unbounded that fills the disk, wedges the feed on ENOSPC, and takes
//! the whole cluster down with it. The watchdog polls the footprint and trips
//! a shared abort the moment the cap is crossed; every long-running loop in
//! the harness (callers, drainers, feed, workers, quiesce) checks it and
//! bails loudly instead of grinding on.
//!
//! The same abort carries persistent-error trips (feed or workers erroring on
//! every consecutive attempt — e.g. ENOSPC, a dropped slot): fail fast with
//! the real error, never hang until a timeout.

use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;

pub const DEFAULT_MAX_FOOTPRINT_GB: u64 = 20;

/// Shared run-abort latch: first trip wins, reason preserved.
#[derive(Clone, Default)]
pub struct AbortSignal {
    tripped: Arc<AtomicBool>,
    reason: Arc<Mutex<Option<String>>>,
}

impl AbortSignal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn trip(&self, reason: String) {
        if !self.tripped.swap(true, Ordering::SeqCst) {
            eprintln!("ABORT: {reason}");
            *self.reason.lock().expect("abort reason lock") = Some(reason);
        }
    }

    pub fn is_tripped(&self) -> bool {
        self.tripped.load(Ordering::SeqCst)
    }

    pub fn reason(&self) -> Option<String> {
        self.reason.lock().expect("abort reason lock").clone()
    }
}

/// Current database + WAL footprint in bytes.
pub async fn footprint_bytes(pool: &PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT pg_database_size(current_database()) \
              + (SELECT COALESCE(sum(size), 0) FROM pg_ls_waldir())::bigint",
    )
    .fetch_one(pool)
    .await
}

/// Poll the footprint every 2s; trip `abort` the moment it crosses
/// `max_bytes`. Runs until `abort` trips (its own or anyone else's).
pub fn spawn_footprint_watchdog(
    pool: PgPool,
    max_bytes: i64,
    abort: AbortSignal,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if abort.is_tripped() {
                return;
            }
            match footprint_bytes(&pool).await {
                Ok(bytes) if bytes > max_bytes => {
                    abort.trip(format!(
                        "footprint budget exceeded: db+wal = {:.2} GB > cap {:.2} GB — \
                         stopping the run (the recalc adjustment storm is the usual cause; \
                         lower --rate / --backdate-pct / --backdate-window-secs)",
                        bytes as f64 / 1e9,
                        max_bytes as f64 / 1e9,
                    ));
                    return;
                }
                Ok(_) => {}
                Err(e) => {
                    // A watchdog that cannot measure must not fly blind.
                    abort.trip(format!("footprint watchdog query failed: {e}"));
                    return;
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    })
}

/// Bump-on-error / reset-on-success counter that trips the abort after
/// `limit` consecutive failures.
pub struct ConsecutiveErrorTrip {
    label: &'static str,
    limit: u32,
    count: u32,
    abort: AbortSignal,
}

impl ConsecutiveErrorTrip {
    pub fn new(label: &'static str, limit: u32, abort: AbortSignal) -> Self {
        Self { label, limit, count: 0, abort }
    }

    pub fn ok(&mut self) {
        self.count = 0;
    }

    pub fn error(&mut self, err: impl std::fmt::Display) {
        self.count += 1;
        if self.count >= self.limit {
            self.abort.trip(format!(
                "{}: {} consecutive errors, latest: {err}",
                self.label, self.count
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abort_first_trip_wins() {
        let a = AbortSignal::new();
        assert!(!a.is_tripped());
        a.trip("first".into());
        a.trip("second".into());
        assert!(a.is_tripped());
        assert_eq!(a.reason().as_deref(), Some("first"));
    }

    #[test]
    fn consecutive_errors_trip_only_unbroken_runs() {
        let a = AbortSignal::new();
        let mut t = ConsecutiveErrorTrip::new("feed", 3, a.clone());
        t.error("e1");
        t.error("e2");
        t.ok();
        t.error("e3");
        t.error("e4");
        assert!(!a.is_tripped());
        t.error("e5");
        assert!(a.is_tripped());
        assert!(a.reason().unwrap().contains("feed"));
    }
}
