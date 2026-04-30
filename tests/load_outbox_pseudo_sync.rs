//! `acct-yjn` — Shape-L: pseudo-sync caller via LISTEN/NOTIFY.
//!
//! Variant of shape J. Same writers, same workload, same drain worker.
//! The new behavior: writers BLOCK on a notification matching their
//! row id before declaring the call done. Worker emits
//! `pg_notify('ledger_outbox_done', '{"id":N,"status":"ok"|"failed",...}')`
//! per row inside the drain tx (delivered on commit, race-free).
//!
//! From the caller's perspective this is *pseudo-sync*: the call only
//! returns when the ledger has actually committed (or definitively
//! failed). Execution is decoupled — single-writer worker amortizes
//! commits — but the caller still gets a synchronous-style
//! "did this commit?" answer.
//!
//! ### Listener topology — single shared dispatcher
//!
//! One `PgListener` task subscribes to the channel and dispatches
//! per-id outcomes via in-process oneshot channels to the waiting
//! writer task. Apples-to-apples connection count vs F (one extra
//! pool conn for the listener) and avoids 100×PgListener noise.
//!
//! ### Race handling
//!
//! Between INSERT-commit and `dispatcher.wait_for(id)`, the worker
//! can race ahead and emit the NOTIFY before the writer registers.
//! The dispatcher therefore stores a per-id state machine where
//! whichever side (writer-waits / notify-arrives) is second satisfies
//! the rendezvous. Late-arriving writers find the outcome buffered.
//!
//! ### Headline metric
//!
//! `caller_total_us` = enqueue_us + wait_us. This is the writer's
//! whole wall-clock per ledger commit. Compared to F (direct sync,
//! caller p99 ≈ 8.25s) and J (async outbox, caller p99 = 108ms but
//! ledger lag p50 ≈ 186s — caller doesn't see it).
//!
//! Run via `./scripts/run-perf-baseline.sh`:
//!
//!   T4_BINARY=load_outbox_pseudo_sync \
//!     T4_CONFIGS="100:5:20" \
//!     T4_BASELINE_RUNS=3 \
//!     T4_DURATION_SECS=300 \
//!     ./scripts/run-perf-baseline.sh
//!
//! Env knobs (defaults shown, parity with shape J unless noted):
//!
//!   T4_DURATION_SECS=30
//!   T4_WRITERS=32
//!   T4_EVENTS_MIN=5
//!   T4_EVENTS_MAX=20
//!   T4_BENCH_SKUS=50
//!   T4_OUTBOX_BATCH_SIZE=1000
//!   T4_OUTBOX_IDLE_SLEEP_MS=1
//!   T4_OUTBOX_DEPTH_INTERVAL_S=5
//!   T4_DRAIN_TIMEOUT_S=30
//!   T4_PSEUDO_SYNC_TIMEOUT_S=30   per-call notify-wait fallback
//!   T4_NOTIFY_CHANNEL=ledger_outbox_done

mod common;

use common::outbox_worker::{DrainConfig, DrainStats, super_batched_drain_loop};
use common::*;
use serde_json::json;
use sqlx::postgres::PgListener;
use std::collections::HashMap;
use std::env;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;

#[derive(Clone, Copy)]
struct LocPair {
    main_id: i64,
    out_id: i64,
}

#[derive(Clone, Debug)]
struct Outcome {
    status: String,
    #[allow(dead_code)]
    sqlstate: Option<String>,
}

/// Per-id state: either a writer is waiting (sender stored) or a notify
/// arrived first (outcome buffered). First-arriver inserts; second-
/// arriver consumes.
enum SlotState {
    WaitingForNotify(oneshot::Sender<Outcome>),
    NotifyArrived(Outcome),
}

#[derive(Clone)]
struct Dispatcher {
    slots: Arc<Mutex<HashMap<i64, SlotState>>>,
}

impl Dispatcher {
    fn new() -> Self {
        Self {
            slots: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Called by the listener task on each NOTIFY.
    fn deliver(&self, id: i64, outcome: Outcome) {
        let mut g = self.slots.lock().unwrap();
        match g.remove(&id) {
            Some(SlotState::WaitingForNotify(tx)) => {
                let _ = tx.send(outcome);
            }
            Some(SlotState::NotifyArrived(_)) | None => {
                g.insert(id, SlotState::NotifyArrived(outcome));
            }
        }
    }

    /// Called by writer after INSERT-commit. Returns immediately if a
    /// notify already arrived; otherwise installs a oneshot and awaits.
    async fn wait_for(&self, id: i64, timeout: Duration) -> Result<Outcome, ()> {
        let rx = {
            let mut g = self.slots.lock().unwrap();
            match g.remove(&id) {
                Some(SlotState::NotifyArrived(o)) => return Ok(o),
                Some(SlotState::WaitingForNotify(_)) => {
                    // Should not happen — same id reused before consumed.
                    panic!("dispatcher slot collision for id {id}");
                }
                None => {
                    let (tx, rx) = oneshot::channel();
                    g.insert(id, SlotState::WaitingForNotify(tx));
                    rx
                }
            }
        };
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(o)) => Ok(o),
            Ok(Err(_)) | Err(_) => {
                // Cleanup our slot if still present.
                let mut g = self.slots.lock().unwrap();
                g.remove(&id);
                Err(())
            }
        }
    }
}

fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    if x == 0 {
        x = 0xdead_beef_cafe_babe;
    }
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

fn fresh_uuid_str(rng: &mut u64) -> String {
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        xorshift(rng) as u32,
        xorshift(rng) as u16,
        xorshift(rng) as u16,
        xorshift(rng) as u16,
        xorshift(rng) & 0xffff_ffff_ffff,
    )
}

async fn setup_load_fixture(pool: &sqlx::PgPool, n_skus: usize) -> Vec<LocPair> {
    for i in 1..=n_skus {
        let code = format!("BENCH-{:03}", i);
        sqlx::query(
            "INSERT INTO skus (code, uom, standard_cost) VALUES ($1, 'EA', 1)
             ON CONFLICT (code) DO NOTHING",
        )
        .bind(&code)
        .execute(pool)
        .await
        .expect("insert BENCH sku");
    }

    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
         SELECT 'stock_available', 'qty', s.id, l.id, 'debit'
           FROM skus s, locations l
          WHERE s.code   LIKE 'BENCH-%'
            AND l.code   IN ('MAIN', 'OUT')
            AND NOT EXISTS (
              SELECT 1 FROM accounts a
               WHERE a.kind        = 'stock_available'
                 AND a.sku_id      = s.id
                 AND a.location_id = l.id
                 AND NOT a.is_closed
            )",
    )
    .execute(pool)
    .await
    .expect("create BENCH accounts");

    sqlx::query(
        "UPDATE accounts SET debits_total = 10000
           FROM skus s, locations l
          WHERE accounts.sku_id      = s.id
            AND accounts.location_id = l.id
            AND s.code LIKE 'BENCH-%'
            AND accounts.kind = 'stock_available'
            AND accounts.debits_total < 10000",
    )
    .execute(pool)
    .await
    .expect("pre-balance BENCH accounts");

    let rows: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT a.id, s.code, l.code
           FROM accounts a
           JOIN skus      s ON s.id = a.sku_id
           JOIN locations l ON l.id = a.location_id
          WHERE a.kind = 'stock_available'
            AND s.code LIKE 'BENCH-%'
          ORDER BY s.code, l.code",
    )
    .fetch_all(pool)
    .await
    .expect("lookup BENCH accounts");

    let mut by_sku: HashMap<String, (Option<i64>, Option<i64>)> = HashMap::new();
    for (id, sku, loc) in rows {
        let entry = by_sku.entry(sku).or_insert((None, None));
        match loc.as_str() {
            "MAIN" => entry.0 = Some(id),
            "OUT" => entry.1 = Some(id),
            _ => {}
        }
    }
    let mut pairs: Vec<LocPair> = by_sku
        .into_values()
        .filter_map(|(m, o)| match (m, o) {
            (Some(main_id), Some(out_id)) => Some(LocPair { main_id, out_id }),
            _ => None,
        })
        .collect();
    pairs.sort_by_key(|p| p.main_id);
    pairs
}

fn pct(sorted: &[u32], q: f64) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((q * (sorted.len() as f64)).floor() as usize).min(sorted.len() - 1);
    sorted[idx]
}

#[tokio::test]
#[ignore = "load test — runs T4_DURATION_SECS (default 30); see file header"]
async fn outbox_pseudo_sync_shape_l() {
    let duration_secs: u64 = env::var("T4_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    let n_writers: u32 = env::var("T4_WRITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32);
    let events_min: usize = env::var("T4_EVENTS_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let events_max: usize = env::var("T4_EVENTS_MAX")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    let n_skus: usize = env::var("T4_BENCH_SKUS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    let outbox_batch_size: i64 = env::var("T4_OUTBOX_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let outbox_idle_sleep_ms: u64 = env::var("T4_OUTBOX_IDLE_SLEEP_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let depth_interval_s: u64 = env::var("T4_OUTBOX_DEPTH_INTERVAL_S")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let drain_timeout_s: u64 = env::var("T4_DRAIN_TIMEOUT_S")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    let pseudo_sync_timeout_s: u64 = env::var("T4_PSEUDO_SYNC_TIMEOUT_S")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    let notify_channel: String =
        env::var("T4_NOTIFY_CHANNEL").unwrap_or_else(|_| "ledger_outbox_done".to_string());
    assert!(
        events_min <= events_max && events_min >= 1,
        "T4_EVENTS_MIN must be >=1 and <= T4_EVENTS_MAX"
    );
    let duration = Duration::from_secs(duration_secs);

    // Pool: writers + 1 drainer + 1 listener + depth sampler(1) + headroom.
    let pool = connect_test_db_with(n_writers + 8).await;
    reset_to_fixture(&pool).await;

    eprintln!("T4: setting up load fixture (n_skus={n_skus})");
    let setup_t0 = Instant::now();
    let pairs = setup_load_fixture(&pool, n_skus).await;
    let setup_dur = setup_t0.elapsed();
    eprintln!(
        "T4: setup complete in {:.2}s, {} (sku,loc) pairs ready",
        setup_dur.as_secs_f64(),
        pairs.len()
    );
    assert!(!pairs.is_empty(), "load fixture produced no pairs");
    let pairs: Arc<Vec<LocPair>> = Arc::new(pairs);

    eprintln!(
        "T4 shape L (pseudo-sync): writers={} duration={}s events={}-{} pool_size={} drain_batch={} idle_sleep={}ms channel={} sync_timeout={}s",
        n_writers,
        duration_secs,
        events_min,
        events_max,
        pairs.len(),
        outbox_batch_size,
        outbox_idle_sleep_ms,
        notify_channel,
        pseudo_sync_timeout_s,
    );

    let _ = sqlx::query("SELECT pg_stat_statements_reset()")
        .execute(&pool)
        .await;
    let stat_db_before = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
        "SELECT xact_commit, xact_rollback, blks_read, blks_hit, deadlocks
           FROM pg_stat_database WHERE datname = current_database()",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or((0, 0, 0, 0, 0));
    let stat_io_before = sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64, i64)>(
        "SELECT
           COALESCE(SUM(reads),     0)::BIGINT,
           COALESCE(SUM(read_bytes),0)::BIGINT,
           COALESCE(SUM(writes),    0)::BIGINT,
           COALESCE(SUM(write_bytes),0)::BIGINT,
           COALESCE(SUM(extends),   0)::BIGINT,
           COALESCE(SUM(hits),      0)::BIGINT,
           COALESCE(SUM(fsyncs),    0)::BIGINT
         FROM pg_stat_io WHERE backend_type = 'client backend'",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or((0, 0, 0, 0, 0, 0, 0));
    let wal_lsn_before: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
        .fetch_one(&pool)
        .await
        .unwrap_or_default();
    let deadlocks_before = stat_db_before.4;

    let ok_count = Arc::new(AtomicU64::new(0));
    let err_count = Arc::new(AtomicU64::new(0));
    let timeout_count = Arc::new(AtomicU64::new(0));
    let event_count = Arc::new(AtomicU64::new(0));

    // ---- Spawn the listener task BEFORE writers, so we don't miss any
    //      NOTIFY between the first INSERT and the first listen poll.
    let dispatcher = Dispatcher::new();
    let listener_stop = Arc::new(AtomicBool::new(false));
    let listener_handle = {
        let pool = pool.clone();
        let dispatcher = dispatcher.clone();
        let stop = listener_stop.clone();
        let channel = notify_channel.clone();
        tokio::spawn(async move {
            let mut listener = PgListener::connect_with(&pool)
                .await
                .expect("PgListener::connect_with");
            listener
                .listen(&channel)
                .await
                .expect("PgListener::listen");
            loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let n = match tokio::time::timeout(
                    Duration::from_millis(200),
                    listener.recv(),
                )
                .await
                {
                    Ok(Ok(n)) => n,
                    Ok(Err(e)) => {
                        eprintln!("listener recv error: {e}");
                        break;
                    }
                    Err(_) => continue, // poll stop flag
                };
                let payload = n.payload();
                // Parse minimal JSON. Fields: id (number), status (string), sqlstate (optional string).
                let v: serde_json::Value = match serde_json::from_str(payload) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("listener parse error: {e} payload={payload}");
                        continue;
                    }
                };
                let id = v.get("id").and_then(|x| x.as_i64()).unwrap_or(-1);
                let status = v
                    .get("status")
                    .and_then(|x| x.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let sqlstate = v
                    .get("sqlstate")
                    .and_then(|x| x.as_str())
                    .map(String::from);
                if id < 0 {
                    continue;
                }
                dispatcher.deliver(id, Outcome { status, sqlstate });
            }
        })
    };

    // ---- Spawn the drain worker (single, super-batched, with notify).
    let drain_to_empty = Arc::new(AtomicBool::new(false));
    let hard_stop = Arc::new(AtomicBool::new(false));
    let worker_handle = {
        let pool = pool.clone();
        let cfg = DrainConfig {
            batch_size: outbox_batch_size,
            idle_sleep_ms: outbox_idle_sleep_ms,
            notify_channel: Some(notify_channel.clone()),
        };
        let stop = drain_to_empty.clone();
        let hs = hard_stop.clone();
        tokio::spawn(async move { super_batched_drain_loop(pool, cfg, stop, hs).await })
    };

    // ---- Spawn the depth sampler.
    let depth_samples: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
    let depth_stop = Arc::new(AtomicBool::new(false));
    let depth_handle = {
        let pool = pool.clone();
        let samples = depth_samples.clone();
        let stop = depth_stop.clone();
        tokio::spawn(async move {
            loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let depth: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM ledger_outbox WHERE status = 'pending'",
                )
                .fetch_one(&pool)
                .await
                .unwrap_or(0);
                samples.lock().unwrap().push(depth);
                eprintln!(
                    "T4_DEPTH_SAMPLE: t={:>4}s depth={}",
                    samples.lock().unwrap().len() as u64 * depth_interval_s,
                    depth
                );
                tokio::time::sleep(Duration::from_secs(depth_interval_s)).await;
            }
        })
    };

    // ---- Spawn writers.
    let start = Instant::now();
    let mut handles = Vec::with_capacity(n_writers as usize);
    for w in 0..n_writers {
        let pool_w = pool.clone();
        let pairs_w = pairs.clone();
        let ok_w = ok_count.clone();
        let err_w = err_count.clone();
        let timeout_w = timeout_count.clone();
        let ev_w = event_count.clone();
        let dispatcher_w = dispatcher.clone();
        let pseudo_sync_to = Duration::from_secs(pseudo_sync_timeout_s);
        handles.push(tokio::spawn(async move {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let mut rng = nanos ^ ((w as u64) << 32) ^ 0x9b54_7d27_cafe_d00d;
            let mut enqueue_lats: Vec<u32> = Vec::new();
            let mut wait_lats: Vec<u32> = Vec::new();
            let mut total_lats: Vec<u32> = Vec::new();
            let span = (events_max - events_min + 1) as u64;

            while start.elapsed() < duration {
                let n_events = events_min + (xorshift(&mut rng) % span) as usize;
                let mut events = Vec::with_capacity(n_events);
                for _ in 0..n_events {
                    let pair = pairs_w[(xorshift(&mut rng) as usize) % pairs_w.len()];
                    let (debit_id, credit_id) = if (xorshift(&mut rng) & 1) == 0 {
                        (pair.out_id, pair.main_id)
                    } else {
                        (pair.main_id, pair.out_id)
                    };
                    let key = fresh_uuid_str(&mut rng);
                    events.push(make_event(
                        "bin_move",
                        debit_id,
                        credit_id,
                        1,
                        "2026-04-15",
                        &key,
                    ));
                }
                let batch = json!(events);

                let t_enqueue_start = Instant::now();
                let res: sqlx::Result<i64> = sqlx::query_scalar(
                    "INSERT INTO ledger_outbox (events) VALUES ($1) RETURNING id",
                )
                .bind(&batch)
                .fetch_one(&pool_w)
                .await;
                let enqueue_dur = t_enqueue_start.elapsed();
                let enqueue_us = enqueue_dur.as_micros().min(u32::MAX as u128) as u32;
                enqueue_lats.push(enqueue_us);

                let id = match res {
                    Ok(id) => id,
                    Err(e) => {
                        eprintln!("writer {w} INSERT error: {e}");
                        err_w.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };

                let t_wait_start = Instant::now();
                let outcome = dispatcher_w.wait_for(id, pseudo_sync_to).await;
                let wait_dur = t_wait_start.elapsed();
                let wait_us = wait_dur.as_micros().min(u32::MAX as u128) as u32;
                let total_us = (enqueue_dur + wait_dur)
                    .as_micros()
                    .min(u32::MAX as u128) as u32;
                wait_lats.push(wait_us);
                total_lats.push(total_us);

                match outcome {
                    Ok(o) => {
                        if o.status == "ok" {
                            ok_w.fetch_add(1, Ordering::Relaxed);
                            ev_w.fetch_add(n_events as u64, Ordering::Relaxed);
                        } else {
                            // The drain reported failure; count under err.
                            err_w.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(()) => {
                        // Timeout fallback: poll the row.
                        timeout_w.fetch_add(1, Ordering::Relaxed);
                        let row: Option<(String, Option<String>)> = sqlx::query_as(
                            "SELECT status, error_sqlstate FROM ledger_outbox WHERE id = $1",
                        )
                        .bind(id)
                        .fetch_optional(&pool_w)
                        .await
                        .unwrap_or(None);
                        match row {
                            Some((status, _)) if status == "committed" => {
                                ok_w.fetch_add(1, Ordering::Relaxed);
                                ev_w.fetch_add(n_events as u64, Ordering::Relaxed);
                            }
                            _ => {
                                err_w.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
            (enqueue_lats, wait_lats, total_lats)
        }));
    }

    let mut all_enqueue_lats: Vec<u32> = Vec::new();
    let mut all_wait_lats: Vec<u32> = Vec::new();
    let mut all_total_lats: Vec<u32> = Vec::new();
    for h in handles {
        let (e, w, t) = h.await.expect("writer panic");
        all_enqueue_lats.extend(e);
        all_wait_lats.extend(w);
        all_total_lats.extend(t);
    }
    let writer_elapsed = start.elapsed();
    eprintln!(
        "T4: writers stopped at t={:.2}s, signaling drain_to_empty",
        writer_elapsed.as_secs_f64()
    );

    // ---- Bounded grace drain.
    drain_to_empty.store(true, Ordering::Relaxed);
    let drain_t0 = Instant::now();
    let drain_outcome = tokio::time::timeout(
        Duration::from_secs(drain_timeout_s),
        worker_handle,
    )
    .await;
    let drain_stats: DrainStats = match drain_outcome {
        Ok(j) => j
            .expect("drain worker panic")
            .expect("super_batched_drain_loop"),
        Err(_) => {
            eprintln!(
                "T4: drain timeout after {}s — forcing hard_stop",
                drain_timeout_s
            );
            hard_stop.store(true, Ordering::Relaxed);
            let committed: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM ledger_outbox WHERE status = 'committed'",
            )
            .fetch_one(&pool)
            .await
            .unwrap_or(0);
            let failed: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM ledger_outbox WHERE status = 'failed'",
            )
            .fetch_one(&pool)
            .await
            .unwrap_or(0);
            DrainStats {
                batches: 0,
                rows_committed: committed as u64,
                rows_failed: failed as u64,
                duration_ms: drain_timeout_s * 1000,
                super_batch_attempts: 0,
                super_batch_successes: 0,
                super_batch_fallbacks: 0,
            }
        }
    };
    let drain_secs = drain_t0.elapsed().as_secs_f64();
    eprintln!(
        "T4: drain phase ended in {:.2}s — batches={} committed={} failed={} super_batch_attempts={} super_batch_successes={} super_batch_fallbacks={}",
        drain_secs,
        drain_stats.batches,
        drain_stats.rows_committed,
        drain_stats.rows_failed,
        drain_stats.super_batch_attempts,
        drain_stats.super_batch_successes,
        drain_stats.super_batch_fallbacks,
    );

    listener_stop.store(true, Ordering::Relaxed);
    let _ = listener_handle.await;
    depth_stop.store(true, Ordering::Relaxed);
    depth_handle.abort();
    let _ = depth_handle.await;
    let max_depth = depth_samples
        .lock()
        .unwrap()
        .iter()
        .copied()
        .max()
        .unwrap_or(0);

    let total_elapsed = start.elapsed();

    let queue_lats_raw: Vec<i64> = sqlx::query_scalar(
        "SELECT (EXTRACT(EPOCH FROM (committed_at - enqueued_at)) * 1000000)::BIGINT
           FROM ledger_outbox
          WHERE status = 'committed'",
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();
    let mut queue_lats: Vec<u32> = queue_lats_raw
        .into_iter()
        .map(|us| us.clamp(0, u32::MAX as i64) as u32)
        .collect();
    queue_lats.sort_unstable();

    let committed_events: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(jsonb_array_length(events)), 0)::BIGINT
           FROM ledger_outbox WHERE status = 'committed'",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or(0);

    let stat_db_after = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
        "SELECT xact_commit, xact_rollback, blks_read, blks_hit, deadlocks
           FROM pg_stat_database WHERE datname = current_database()",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or((0, 0, 0, 0, 0));
    let deadlocks_after = stat_db_after.4;

    let stat_io_after = sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64, i64)>(
        "SELECT
           COALESCE(SUM(reads),     0)::BIGINT,
           COALESCE(SUM(read_bytes),0)::BIGINT,
           COALESCE(SUM(writes),    0)::BIGINT,
           COALESCE(SUM(write_bytes),0)::BIGINT,
           COALESCE(SUM(extends),   0)::BIGINT,
           COALESCE(SUM(hits),      0)::BIGINT,
           COALESCE(SUM(fsyncs),    0)::BIGINT
         FROM pg_stat_io WHERE backend_type = 'client backend'",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or((0, 0, 0, 0, 0, 0, 0));

    let wal_lsn_after: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
        .fetch_one(&pool)
        .await
        .unwrap_or_default();
    let wal_bytes_delta: i64 = if !wal_lsn_before.is_empty() && !wal_lsn_after.is_empty() {
        sqlx::query_scalar("SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)::BIGINT")
            .bind(&wal_lsn_after)
            .bind(&wal_lsn_before)
            .fetch_one(&pool)
            .await
            .unwrap_or(0)
    } else {
        0
    };

    let top_queries: Vec<(String, i64, f64, f64)> = sqlx::query_as(
        "SELECT left(query, 80) AS q, calls::BIGINT,
                total_exec_time, mean_exec_time
           FROM pg_stat_statements
          WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            AND query NOT ILIKE 'SELECT pg_stat_%'
            AND query NOT ILIKE 'SELECT xact_commit%'
          ORDER BY total_exec_time DESC
          LIMIT 10",
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    let final_status: Vec<(String, i64)> = sqlx::query_as(
        "SELECT status, COUNT(*)::BIGINT FROM ledger_outbox GROUP BY status ORDER BY status",
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();
    let final_committed: i64 = final_status
        .iter()
        .find(|(s, _)| s == "committed")
        .map(|(_, c)| *c)
        .unwrap_or(0);
    let final_failed: i64 = final_status
        .iter()
        .find(|(s, _)| s == "failed")
        .map(|(_, c)| *c)
        .unwrap_or(0);
    let final_pending: i64 = final_status
        .iter()
        .find(|(s, _)| s == "pending")
        .map(|(_, c)| *c)
        .unwrap_or(0);

    let ok = ok_count.load(Ordering::Relaxed);
    let err = err_count.load(Ordering::Relaxed);
    let timeouts = timeout_count.load(Ordering::Relaxed);
    let events = event_count.load(Ordering::Relaxed);
    let total = ok + err;

    all_enqueue_lats.sort_unstable();
    all_wait_lats.sort_unstable();
    all_total_lats.sort_unstable();
    let e_p50 = pct(&all_enqueue_lats, 0.50);
    let e_p95 = pct(&all_enqueue_lats, 0.95);
    let e_p99 = pct(&all_enqueue_lats, 0.99);
    let e_p99_9 = pct(&all_enqueue_lats, 0.999);
    let e_max = *all_enqueue_lats.last().unwrap_or(&0);

    let w_p50 = pct(&all_wait_lats, 0.50);
    let w_p95 = pct(&all_wait_lats, 0.95);
    let w_p99 = pct(&all_wait_lats, 0.99);
    let w_p99_9 = pct(&all_wait_lats, 0.999);
    let w_max = *all_wait_lats.last().unwrap_or(&0);

    let t_p50 = pct(&all_total_lats, 0.50);
    let t_p95 = pct(&all_total_lats, 0.95);
    let t_p99 = pct(&all_total_lats, 0.99);
    let t_p99_9 = pct(&all_total_lats, 0.999);
    let t_max = *all_total_lats.last().unwrap_or(&0);

    let q_p50 = pct(&queue_lats, 0.50);
    let q_p95 = pct(&queue_lats, 0.95);
    let q_p99 = pct(&queue_lats, 0.99);
    let q_p99_9 = pct(&queue_lats, 0.999);
    let q_max = *queue_lats.last().unwrap_or(&0);

    let xact_commit_d = stat_db_after.0 - stat_db_before.0;
    let xact_rollbk_d = stat_db_after.1 - stat_db_before.1;
    let blks_read_d = stat_db_after.2 - stat_db_before.2;
    let blks_hit_d = stat_db_after.3 - stat_db_before.3;
    let io_reads_d = stat_io_after.0 - stat_io_before.0;
    let io_rbytes_d = stat_io_after.1 - stat_io_before.1;
    let io_writes_d = stat_io_after.2 - stat_io_before.2;
    let io_wbytes_d = stat_io_after.3 - stat_io_before.3;
    let io_extends_d = stat_io_after.4 - stat_io_before.4;
    let io_hits_d = stat_io_after.5 - stat_io_before.5;
    let io_fsyncs_d = stat_io_after.6 - stat_io_before.6;

    eprintln!("===================== T4 PERF SUMMARY (shape L: pseudo-sync) =====================");
    eprintln!(
        "duration_s={:.2} (writers_phase={:.2}s drain_phase={:.2}s) writers={} events={}-{} pool={} (outbox+notify)",
        total_elapsed.as_secs_f64(),
        writer_elapsed.as_secs_f64(),
        drain_secs,
        n_writers,
        events_min,
        events_max,
        pairs.len()
    );
    eprintln!(
        "batches: total={} ok={} err={} timeouts={} pseudo_sync_throughput={:.1}/s",
        total,
        ok,
        err,
        timeouts,
        total as f64 / writer_elapsed.as_secs_f64()
    );
    eprintln!(
        "events:  enqueued={} committed={} commit_throughput={:.1}/s  <-- HEADLINE",
        events,
        committed_events,
        committed_events as f64 / total_elapsed.as_secs_f64()
    );
    eprintln!(
        "enqueue_us: p50={} p95={} p99={} p99.9={} max={} (n={})",
        e_p50, e_p95, e_p99, e_p99_9, e_max, all_enqueue_lats.len()
    );
    eprintln!(
        "wait_us:    p50={} p95={} p99={} p99.9={} max={} (n={})",
        w_p50, w_p95, w_p99, w_p99_9, w_max, all_wait_lats.len()
    );
    eprintln!(
        "total_us:   p50={} p95={} p99={} p99.9={} max={} (n={})  <-- caller wall-clock",
        t_p50, t_p95, t_p99, t_p99_9, t_max, all_total_lats.len()
    );
    eprintln!(
        "queue_us:   p50={} p95={} p99={} p99.9={} max={} (n={})",
        q_p50, q_p95, q_p99, q_p99_9, q_max, queue_lats.len()
    );
    eprintln!(
        "outbox: max_pending_depth={} drain_batches={} committed={} failed={} pending={}",
        max_depth,
        drain_stats.batches,
        final_committed,
        final_failed,
        final_pending
    );
    eprintln!(
        "deadlocks: delta={} ({} -> {})",
        deadlocks_after - deadlocks_before,
        deadlocks_before,
        deadlocks_after
    );
    eprintln!(
        "pg_stat_database: xact_commit_delta={} xact_rollback_delta={} blks_read_delta={} blks_hit_delta={}",
        xact_commit_d, xact_rollbk_d, blks_read_d, blks_hit_d
    );
    eprintln!(
        "pg_stat_io (client backend): reads_delta={} read_bytes_delta={} writes_delta={} write_bytes_delta={} extends_delta={} hits_delta={} fsyncs_delta={}",
        io_reads_d, io_rbytes_d, io_writes_d, io_wbytes_d, io_extends_d, io_hits_d, io_fsyncs_d
    );
    eprintln!(
        "wal_bytes_delta={} (lsn {} -> {})",
        wal_bytes_delta, wal_lsn_before, wal_lsn_after
    );
    eprintln!("pg_stat_statements (top 10):");
    for (q, calls, total_ms, mean_ms) in &top_queries {
        eprintln!(
            "  calls={:>10} total_ms={:>10.1} mean_ms={:>8.3}  query={}",
            calls, total_ms, mean_ms, q
        );
    }
    eprintln!("=======================================================================");

    eprintln!(
        "T4_CSV_HEADER: duration_s,writers,batches_total,batches_ok,batches_err,events_total,throughput_bps,throughput_evps,p50_us,p95_us,p99_us,p999_us,max_us,deadlocks_delta,xact_commit_delta,xact_rollback_delta,blks_read_delta,blks_hit_delta,io_reads_delta,io_read_bytes_delta,io_writes_delta,io_write_bytes_delta,io_extends_delta,io_hits_delta,io_fsyncs_delta,wal_bytes_delta,queue_p50_us,queue_p95_us,queue_p99_us,queue_p999_us,queue_max_us,max_outbox_depth,final_committed,final_failed,drain_secs,committed_events,commit_evps,wait_p50_us,wait_p95_us,wait_p99_us,wait_p999_us,wait_max_us,total_p50_us,total_p95_us,total_p99_us,total_p999_us,total_max_us,timeouts"
    );
    eprintln!(
        "T4_CSV_VALUES: {:.3},{},{},{},{},{},{:.3},{:.3},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.3},{},{:.3},{},{},{},{},{},{},{},{},{},{},{}",
        writer_elapsed.as_secs_f64(),
        n_writers,
        total,
        ok,
        err,
        events,
        total as f64 / writer_elapsed.as_secs_f64(),
        events as f64 / writer_elapsed.as_secs_f64(),
        e_p50, e_p95, e_p99, e_p99_9, e_max,
        deadlocks_after - deadlocks_before,
        xact_commit_d, xact_rollbk_d, blks_read_d, blks_hit_d,
        io_reads_d, io_rbytes_d, io_writes_d, io_wbytes_d, io_extends_d, io_hits_d, io_fsyncs_d,
        wal_bytes_delta,
        q_p50, q_p95, q_p99, q_p99_9, q_max,
        max_depth,
        final_committed,
        final_failed,
        drain_secs,
        committed_events,
        committed_events as f64 / total_elapsed.as_secs_f64(),
        w_p50, w_p95, w_p99, w_p99_9, w_max,
        t_p50, t_p95, t_p99, t_p99_9, t_max,
        timeouts,
    );

    assert_eq!(
        deadlocks_after - deadlocks_before,
        0,
        "deadlock counter rose during run"
    );
    assert!(total > 0, "no batches enqueued");
    assert!(final_committed > 0, "worker committed nothing");
}
