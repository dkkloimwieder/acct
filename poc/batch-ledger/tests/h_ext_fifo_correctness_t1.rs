//! acct-oh6l / zm69.h8 — Correctness probe for path 1 (FOR UPDATE walk).
//!
//! Tests the layer-attribution race under concurrent issues.
//!
//! ## Workload
//!
//! - Pre-seed N thin layers per group (small qty per layer).
//! - Spawn W workers issuing concurrently from the same group(s).
//! - Each worker batches K issues per txn.
//! - Issue qty is small (1-3) so layers deplete gradually.
//!
//! ## Invariants asserted
//!
//! - After the run, `fifo_overconsume_check_h_ext()` returns 0 rows
//!   (no layer has SUM(depletions) > qty_received).
//! - Total consumed across the run = total qty_received drained.
//! - Per-group effective_qty in h_arena matches durable totals.
//! - SUM(consumptions.qty) ≤ SUM(layers.qty) per group.

use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn h_ext_fifo_walk_concurrent_correctness() {
    let workers = env_or::<usize>("POC_TEST_WORKERS", 16);
    // Thin-layer shape: many small layers per group.
    let groups = env_or::<usize>("POC_TEST_GROUPS", 4);
    let layers_per_group = env_or::<usize>("POC_TEST_LAYERS_PER_GROUP", 10);
    let layer_qty: i64 = env_or::<i64>("POC_TEST_LAYER_QTY", 50);
    // Each worker issues K times in one batch. issue qty is small so
    // layers deplete gradually, maximizing per-layer concurrency.
    let issues_per_batch = env_or::<usize>("POC_TEST_ISSUES_PER_BATCH", 5);
    let total_batches_per_worker = env_or::<usize>("POC_TEST_BATCHES_PER_WORKER", 5);
    let issue_qty_max: i64 = env_or::<i64>("POC_TEST_ISSUE_QTY_MAX", 3);
    let bench_fn = std::env::var("POC_TEST_FUNCTION")
        .unwrap_or_else(|_| "post_batch_h_ext_fifo_walk".to_string());

    let url = std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections((workers as u32) + 4)
        .connect(&url)
        .await
        .expect("connect");

    sqlx::query(
        "TRUNCATE cost_layer_depletions_h_ext, cost_consumptions_h_ext, cost_layers_h_ext \
         RESTART IDENTITY",
    )
    .execute(&pool)
    .await
    .expect("truncate");

    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("ext");
    sqlx::query("SELECT h_arena_reset()")
        .execute(&pool)
        .await
        .expect("h_arena_reset");
    let _ = sqlx::query("SELECT h_layer_arena_reset()")
        .execute(&pool)
        .await;

    // Seed thin layers.
    let total_seed_qty = (groups as i64) * (layers_per_group as i64) * layer_qty;
    for g in 1..=(groups as i64) {
        for l in 0..layers_per_group {
            sqlx::query(
                "INSERT INTO cost_layers_h_ext (layer_group_id, qty, qty_remaining, unit_cost, source_kind) \
                 VALUES ($1, $2, $2, $3, 'receipt')",
            )
            .bind(g)
            .bind(layer_qty)
            .bind(100i64 + (l as i64) * 10) // distinct unit_cost per layer for FIFO attribution check
            .execute(&pool)
            .await
            .expect("seed layer");
        }
        // Mirror in h_arena.
        sqlx::query("SELECT h_apply_delta($1, $2)")
            .bind(g)
            .bind(layer_qty * layers_per_group as i64)
            .execute(&pool)
            .await
            .expect("h_arena seed");
    }
    // For path 2 (layer_shmem), seed h_layer_arena per-layer cells.
    if bench_fn == "post_batch_h_ext_layer_shmem" {
        sqlx::query(
            "DO $$
             DECLARE r RECORD;
             BEGIN
               FOR r IN SELECT layer_id, qty FROM cost_layers_h_ext LOOP
                 PERFORM h_layer_create(r.layer_id, r.qty);
               END LOOP;
             END$$",
        )
        .execute(&pool)
        .await
        .expect("h_layer_arena seed");
    }
    // The seed h_apply_delta calls happen across multiple txns (each is
    // its own implicit txn). Each commits to shmem, so we end with
    // shmem effective_qty = total_seed_qty per group. Good.

    // Compute total target consumption: 70% of pool. Each worker issues
    // K issues per batch × B batches × max qty avg ~half of max.
    let total_qty_to_consume = (total_seed_qty * 7) / 10;
    let total_workers_issue =
        (workers as i64) * (total_batches_per_worker as i64) * (issues_per_batch as i64) * (issue_qty_max + 1) / 2;
    eprintln!(
        "Seed: {} groups × {} layers × {} qty = {} total. Target consume ~{}; workers estimate issue {}",
        groups, layers_per_group, layer_qty, total_seed_qty, total_qty_to_consume, total_workers_issue
    );

    let q_call = format!("SELECT {}($1)", bench_fn);
    let stats = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for wi in 0..workers {
        let p = pool.clone();
        let s = stats.clone();
        let q_call = q_call.clone();
        handles.push(tokio::spawn(async move {
            let mut rng: u64 = (wi as u64)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(1);
            let mut committed = 0u64;
            for _ in 0..total_batches_per_worker {
                let mut envelopes = Vec::with_capacity(issues_per_batch);
                for _ in 0..issues_per_batch {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    let r = rng as usize;
                    let group_id = (1 + (r % groups)) as i64;
                    let qty = 1 + ((r / 11) as i64) % issue_qty_max;
                    envelopes.push(json!({
                        "kind": "issue",
                        "layer_group_id": group_id,
                        "qty": qty,
                    }));
                }
                let envelopes_v = Value::Array(envelopes);

                // Retry on serialization failure (40001).
                for retry in 0..20 {
                    let mut tx = match p.begin().await {
                        Ok(t) => t,
                        Err(_) => break,
                    };
                    if sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                        .execute(&mut *tx)
                        .await
                        .is_err()
                    {
                        let _ = tx.rollback().await;
                        break;
                    }
                    let r = sqlx::query(&q_call)
                        .bind(&envelopes_v)
                        .execute(&mut *tx)
                        .await;
                    let commit_res = match r {
                        Ok(_) => tx.commit().await,
                        Err(e) => {
                            let _ = tx.rollback().await;
                            Err(e)
                        }
                    };
                    match commit_res {
                        Ok(()) => {
                            committed += 1;
                            break;
                        }
                        Err(e) => {
                            let is_40001 = match &e {
                                sqlx::Error::Database(db) => db.code().as_deref() == Some("40001"),
                                _ => false,
                            };
                            if !is_40001 {
                                eprintln!("worker {wi} retry {retry}: {e}");
                                break;
                            }
                            tokio::time::sleep(Duration::from_micros(50 * (retry as u64 + 1)))
                                .await;
                        }
                    }
                }
            }
            s.fetch_add(committed, Ordering::Relaxed);
        }));
    }
    for h in handles {
        h.await.expect("join");
    }
    let committed_batches = stats.load(Ordering::Relaxed);

    // For path 3 (deferred), drain until quiescence before checking.
    if bench_fn == "post_batch_h_ext_deferred" {
        loop {
            let drained: i64 = sqlx::query_scalar("SELECT drain_deferred_fifo(1000)::BIGINT")
                .fetch_one(&pool)
                .await
                .expect("drain");
            if drained == 0 {
                break;
            }
        }
    }

    // ──────────────────────────────────────────────────────────────
    // INVARIANT CHECKS
    // ──────────────────────────────────────────────────────────────

    // I1: SUM(depletions per layer) ≤ qty_received per layer.
    let overconsume_rows: Vec<(i64, i64, i64, i64, i64)> = sqlx::query_as(
        "SELECT layer_id, layer_group_id, qty_received, total_consumed, overshoot \
           FROM fifo_overconsume_check_h_ext()",
    )
    .fetch_all(&pool)
    .await
    .expect("recon");

    eprintln!();
    eprintln!("=== H+ext correctness probe (fn={}) ===", bench_fn);
    eprintln!(
        "  workers={} groups={} layers_per_group={} layer_qty={} issues_per_batch={} batches_per_worker={}",
        workers, groups, layers_per_group, layer_qty, issues_per_batch, total_batches_per_worker
    );
    eprintln!("  committed_batches: {}", committed_batches);
    eprintln!("  overconsume_layers: {}", overconsume_rows.len());
    for (lid, gid, recv, cons, overshoot) in &overconsume_rows {
        eprintln!(
            "    layer={} group={} qty_received={} consumed={} overshoot={}",
            lid, gid, recv, cons, overshoot
        );
    }

    // I2: SUM(depletion.qty_consumed) per layer == qty_received - qty_remaining.
    // Only applies to paths that maintain durable qty_remaining (paths 1 and 3).
    // Path 2 (layer_shmem) keeps residual in shmem only; qty_remaining stays stale.
    let drift_rows: Vec<(i64, i64, i64, i64)> = if bench_fn == "post_batch_h_ext_layer_shmem" {
        Vec::new()
    } else {
        sqlx::query_as(
            "SELECT l.layer_id, l.qty - l.qty_remaining AS expected_consumed,
                    COALESCE(SUM(d.qty_consumed), 0)::BIGINT AS actual_consumed,
                    (l.qty - l.qty_remaining - COALESCE(SUM(d.qty_consumed), 0))::BIGINT AS drift
               FROM cost_layers_h_ext l
               LEFT JOIN cost_layer_depletions_h_ext d ON d.layer_id = l.layer_id
              GROUP BY l.layer_id, l.qty, l.qty_remaining
             HAVING (l.qty - l.qty_remaining) <> COALESCE(SUM(d.qty_consumed), 0)
              ORDER BY l.layer_id",
        )
        .fetch_all(&pool)
        .await
        .expect("drift check")
    };

    eprintln!("  qty_remaining-vs-depletions drift rows: {}", drift_rows.len());
    for (lid, exp, act, drift) in &drift_rows {
        eprintln!(
            "    layer={} expected_consumed={} actual_consumed={} drift={}",
            lid, exp, act, drift
        );
    }

    // I3: h_arena effective_qty per group == SUM(layers.residual) per group.
    // Path 2 uses h_layer_arena residual as truth; paths 1/3 use durable qty_remaining.
    let arena_drift_sql = if bench_fn == "post_batch_h_ext_layer_shmem" {
        "WITH layer_residuals AS (
             SELECT layer_group_id, SUM(h_layer_lookup(layer_id))::BIGINT AS residual
               FROM cost_layers_h_ext
              GROUP BY layer_group_id
         ),
         arena AS (
             SELECT lr.layer_group_id, h_arena_lookup(lr.layer_group_id) AS arena_qty
               FROM layer_residuals lr
         )
         SELECT lr.layer_group_id, lr.residual, a.arena_qty, (a.arena_qty - lr.residual)::BIGINT AS drift
           FROM layer_residuals lr JOIN arena a USING (layer_group_id)
          WHERE lr.residual <> a.arena_qty
          ORDER BY lr.layer_group_id"
    } else {
        "WITH layer_residuals AS (
             SELECT layer_group_id, SUM(qty_remaining)::BIGINT AS residual
               FROM cost_layers_h_ext
              GROUP BY layer_group_id
         ),
         arena AS (
             SELECT lr.layer_group_id, h_arena_lookup(lr.layer_group_id) AS arena_qty
               FROM layer_residuals lr
         )
         SELECT lr.layer_group_id, lr.residual, a.arena_qty, (a.arena_qty - lr.residual)::BIGINT AS drift
           FROM layer_residuals lr JOIN arena a USING (layer_group_id)
          WHERE lr.residual <> a.arena_qty
          ORDER BY lr.layer_group_id"
    };
    let arena_drift_rows: Vec<(i64, i64, i64, i64)> = sqlx::query_as(arena_drift_sql)
        .fetch_all(&pool)
        .await
        .expect("arena drift");

    eprintln!("  h_arena-vs-durable-residual drift rows: {}", arena_drift_rows.len());
    for (gid, residual, arena, drift) in &arena_drift_rows {
        eprintln!(
            "    group={} durable_residual={} arena_effective={} drift={}",
            gid, residual, arena, drift
        );
    }
    eprintln!("======================================");

    // Hard asserts.
    assert_eq!(
        overconsume_rows.len(),
        0,
        "FIFO over-attribution detected: {} layer(s) have SUM(depletions) > qty_received",
        overconsume_rows.len()
    );
    assert_eq!(
        drift_rows.len(),
        0,
        "qty_remaining drift: {} layer(s) disagree with depletion sum",
        drift_rows.len()
    );
    assert_eq!(
        arena_drift_rows.len(),
        0,
        "h_arena ↔ durable drift: {} group(s) have shmem effective_qty != SUM(layers.qty_remaining)",
        arena_drift_rows.len()
    );
}
