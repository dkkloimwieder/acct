//! Two-gauge sampler (recalc-c D8): G1 = feed cursor / WAL-retention lag,
//! G2 = recalc settlement lag (a = per-pool staleness, b = drift magnitude
//! bound, c = dirty-set depth). Sampled on an interval into JSONL plus an
//! in-memory summary of maxima — the quiet-backlog chain's observability.

use serde::Serialize;
use sqlx::PgPool;
use std::io::Write;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Default, Serialize)]
pub struct GaugeSample {
    pub t_ms: u64,
    /// G1 — bytes between pg_current_wal_lsn and confirmed_flush_lsn.
    pub g1_lag_bytes: Option<i64>,
    /// G1 — WAL retained from restart_lsn (the slot's pin).
    pub g1_retained_wal_bytes: Option<i64>,
    /// G2c — dirty-set depth.
    pub g2c_dirty_pools: i64,
    /// G2c — age of the oldest un-drained mark (ms).
    pub g2c_oldest_mark_ms: Option<i64>,
    /// G2a — pools with unsettled physical events.
    pub g2a_lagging_pools: i64,
    /// G2b — total unsettled physical events across pools.
    pub g2b_unsettled_events: i64,
    /// G2b — gross value of the unsettled tail (the forced-close move bound).
    pub g2b_unsettled_gross: i64,
    /// Backpressure (recalc-c §5) — pools currently throttled.
    pub bp_throttled_pools: i64,
    /// Backpressure — largest per-pool unsettled-event counter.
    pub bp_max_backlog: i64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct GaugeSummary {
    pub samples: u64,
    pub max_g1_lag_bytes: i64,
    pub max_g1_retained_wal_bytes: i64,
    pub max_g2c_dirty_pools: i64,
    pub max_g2a_lagging_pools: i64,
    pub max_g2b_unsettled_events: i64,
    pub max_g2b_unsettled_gross: i64,
    pub max_bp_throttled_pools: i64,
    pub max_bp_max_backlog: i64,
}

pub async fn sample(pool: &PgPool, t_ms: u64) -> GaugeSample {
    let g1: Option<(i64, i64)> = sqlx::query_as(
        "SELECT lag_bytes, retained_wal_bytes FROM feed_lag",
    )
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();
    let (dirty, oldest_ms): (i64, Option<i64>) = sqlx::query_as(
        "SELECT dirty_pools, \
                (extract(epoch FROM clock_timestamp() - oldest_enqueued_at) * 1000)::bigint \
           FROM recalc_queue_depth",
    )
    .fetch_one(pool)
    .await
    .unwrap_or((0, None));
    // Strict-method pools only: wac pools never grow settlement state (their
    // hot path is authoritative), so the view reports their whole stream as
    // an unsettled tail — the same fifo/lifo scoping the close gate applies.
    let (lagging, events, gross): (i64, i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE l.unsettled_events > 0), \
                COALESCE(sum(l.unsettled_events), 0)::bigint, \
                COALESCE(sum(l.unsettled_gross_value), 0)::bigint \
           FROM recalc_pool_lag l \
           JOIN pool p ON p.id = l.pool_id AND p.method IN ('fifo', 'lifo')",
    )
    .fetch_one(pool)
    .await
    .unwrap_or((0, 0, 0));
    let (throttled, max_backlog): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM recalc_backpressure), \
                (SELECT COALESCE(max(pending_events), 0) FROM recalc_backlog)",
    )
    .fetch_one(pool)
    .await
    .unwrap_or((0, 0));
    GaugeSample {
        t_ms,
        g1_lag_bytes: g1.map(|(l, _)| l),
        g1_retained_wal_bytes: g1.map(|(_, r)| r),
        g2c_dirty_pools: dirty,
        g2c_oldest_mark_ms: oldest_ms,
        g2a_lagging_pools: lagging,
        g2b_unsettled_events: events,
        g2b_unsettled_gross: gross,
        bp_throttled_pools: throttled,
        bp_max_backlog: max_backlog,
    }
}

pub struct Sampler {
    handle: JoinHandle<GaugeSummary>,
    stop_tx: watch::Sender<bool>,
}

impl Sampler {
    /// Sample every `interval` into `out_path` (JSONL) until stopped.
    pub fn spawn(pool: PgPool, interval: Duration, out_path: std::path::PathBuf) -> Self {
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let start = Instant::now();
            let mut out = std::io::BufWriter::new(
                std::fs::File::create(&out_path).expect("create gauge log"),
            );
            let mut summary = GaugeSummary::default();
            loop {
                let s = sample(&pool, start.elapsed().as_millis() as u64).await;
                summary.samples += 1;
                summary.max_g1_lag_bytes =
                    summary.max_g1_lag_bytes.max(s.g1_lag_bytes.unwrap_or(0));
                summary.max_g1_retained_wal_bytes = summary
                    .max_g1_retained_wal_bytes
                    .max(s.g1_retained_wal_bytes.unwrap_or(0));
                summary.max_g2c_dirty_pools = summary.max_g2c_dirty_pools.max(s.g2c_dirty_pools);
                summary.max_g2a_lagging_pools =
                    summary.max_g2a_lagging_pools.max(s.g2a_lagging_pools);
                summary.max_g2b_unsettled_events =
                    summary.max_g2b_unsettled_events.max(s.g2b_unsettled_events);
                summary.max_g2b_unsettled_gross =
                    summary.max_g2b_unsettled_gross.max(s.g2b_unsettled_gross);
                summary.max_bp_throttled_pools =
                    summary.max_bp_throttled_pools.max(s.bp_throttled_pools);
                summary.max_bp_max_backlog = summary.max_bp_max_backlog.max(s.bp_max_backlog);
                let line = serde_json::to_string(&s).expect("serialize gauge sample");
                let _ = writeln!(out, "{line}");
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    _ = stop_rx.changed() => break,
                }
            }
            let _ = out.flush();
            summary
        });
        Self { handle, stop_tx }
    }

    pub async fn shutdown(self) -> GaugeSummary {
        let _ = self.stop_tx.send(true);
        self.handle.await.expect("gauge sampler task")
    }
}
