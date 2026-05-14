//! acct-cpe8 — sub 8 FIFO_N_BUCKETS graceful overflow.
//!
//! When all FIFO_N_BUCKETS=16384 buckets in the FIFO arena are occupied
//! at insert time, `fifo_apply_batch_maximal` previously raised
//! pgrx::error! and aborted the batch. With sub 8 the unhosted pool is
//! routed through a **durable-only** path: no shmem cell, no ring, no
//! pending_drain. Receipts INSERT directly into `cost_layers`. Issues
//! SPI-walk the pool's durable rows under `FOR UPDATE` and stage their
//! consumption through `accum_drain` for the Phase 9e UPDATE.
//!
//! Other pools in the same batch are unaffected and stay on the fast
//! cell-hosted path.
//!
//! Coverage:
//!
//! - N1 receipt + issue end-to-end through durable-only:
//!   * receipt produces a fresh `cost_layers` row.
//!   * issue walks it via SPI and produces a matching `cost_layer_depletions`.
//!   * posting_lines.amount = qty × unit_cost (strict per-unit, no
//!     averaging).
//!   * `fifo_arena_recon()` returns no row for the durable-only pool
//!     (no shmem cell exists).
//!
//! - N2 mixed batch — one durable-only pool + one cell-hosted pool in
//!   the same batch:
//!   * cell-hosted pool's recon drift stays 0.
//!   * durable-only pool's cost_layers grows correctly.
//!
//! - N3 multiple durable-only pools in one batch — each gets its own
//!   layer row and depletion.
//!
//! - N4 idempotent replay of a durable-only envelope — second call with
//!   the same idempotency_key returns `idempotent_replay` and does NOT
//!   re-consume the layer.
//!
//! - N5 durable-only issue with shortage (no prior receipts) — raises a
//!   clear error mentioning bucket exhaustion.
//!
//! Destructive (manipulates FIFO_ARENA shmem occupancy across all
//! buckets via `fifo_force_arena_full`), so `#[ignore]`'d to keep it
//! out of the default parallel test runs. Run explicitly via:
//!
//! ```
//! cargo test --test fifo_durable_only_t1 -- --ignored --test-threads=1
//! ```

use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn db_url() -> String {
    std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
}

fn unique_accounts() -> (i64, i64, i64) {
    let u = Uuid::new_v4();
    let bytes = u.as_bytes();
    // Distinct base from inline (1e12), wac maximal (1.5e12), maximal F
    // (2.1e12), drain (2.5e12), recon (2.9e12), crash-recovery (3.3e12),
    // spill (3.7e12) — durable_only claims 4.1e12.
    let base = 4_100_000_000_000_i64
        + ((bytes[0] as i64) << 24
            | (bytes[1] as i64) << 16
            | (bytes[2] as i64) << 8
            | (bytes[3] as i64))
            .abs()
            % 100_000_000;
    (base, base + 1, base + 2)
}

async fn pool() -> sqlx::PgPool {
    let p = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&p)
        .await
        .expect("ext");
    p
}

async fn seed_accounts(p: &sqlx::PgPool, ids: &[(i64, &str, &str)]) {
    for (id, code, kind) in ids {
        sqlx::query(
            "INSERT INTO accounts (id, code, kind, currency) \
             VALUES ($1::bigint, $2 || '-' || $1::TEXT, $3::account_kind, 'USD') \
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(*id)
        .bind(*code)
        .bind(*kind)
        .execute(p)
        .await
        .expect("seed account");
    }
}

async fn cleanup(p: &sqlx::PgPool, ids: &[i64]) {
    let _ = sqlx::query(
        "DELETE FROM cost_layer_depletions \
         WHERE layer_id IN (SELECT id FROM cost_layers WHERE pool_account_id = ANY($1::bigint[]))",
    )
    .bind(ids)
    .execute(p)
    .await;
    let _ = sqlx::query("DELETE FROM cost_layers WHERE pool_account_id = ANY($1::bigint[])")
        .bind(ids)
        .execute(p)
        .await;
    for id in ids {
        let _ = sqlx::query(
            "DELETE FROM posting_lines WHERE debit_account_id = $1::bigint OR credit_account_id = $1::bigint",
        )
        .bind(*id)
        .execute(p)
        .await;
    }
    let _ = sqlx::query("DELETE FROM accounts WHERE id = ANY($1::bigint[])")
        .bind(ids)
        .execute(p)
        .await;
}

async fn fill_arena(p: &sqlx::PgPool) -> i64 {
    let row = sqlx::query("SELECT fifo_force_arena_full() AS n")
        .fetch_one(p)
        .await
        .expect("fifo_force_arena_full");
    row.get::<i64, _>("n")
}

async fn release_sentinels(p: &sqlx::PgPool) -> i64 {
    let row = sqlx::query("SELECT fifo_release_sentinels() AS n")
        .fetch_one(p)
        .await
        .expect("fifo_release_sentinels");
    row.get::<i64, _>("n")
}

async fn run_maximal(
    p: &sqlx::PgPool,
    envelopes: serde_json::Value,
) -> Vec<(i32, String, Option<i64>)> {
    sqlx::query(
        "SELECT envelope_idx, status, posting_line_id \
         FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(envelopes)
    .fetch_all(p)
    .await
    .expect("run_maximal")
    .into_iter()
    .map(|r| {
        (
            r.get::<i32, _>("envelope_idx"),
            r.get::<String, _>("status"),
            r.get::<Option<i64>, _>("posting_line_id"),
        )
    })
    .collect()
}

async fn try_run_maximal(
    p: &sqlx::PgPool,
    envelopes: serde_json::Value,
) -> Result<Vec<(i32, String, Option<i64>)>, sqlx::Error> {
    let q = sqlx::query(
        "SELECT envelope_idx, status, posting_line_id \
         FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(envelopes);
    Ok(q.fetch_all(p)
        .await?
        .into_iter()
        .map(|r| {
            (
                r.get::<i32, _>("envelope_idx"),
                r.get::<String, _>("status"),
                r.get::<Option<i64>, _>("posting_line_id"),
            )
        })
        .collect())
}

async fn pool_layers(p: &sqlx::PgPool, pool_id: i64) -> Vec<(i64, i64, i64)> {
    sqlx::query(
        "SELECT id, qty_remaining, unit_cost FROM cost_layers \
         WHERE pool_account_id = $1::bigint ORDER BY receipt_date, id",
    )
    .bind(pool_id)
    .fetch_all(p)
    .await
    .expect("pool_layers")
    .into_iter()
    .map(|r| {
        (
            r.get::<i64, _>("id"),
            r.get::<i64, _>("qty_remaining"),
            r.get::<i64, _>("unit_cost"),
        )
    })
    .collect()
}

async fn pool_depletions_total(p: &sqlx::PgPool, pool_id: i64) -> (i64, i64) {
    let row = sqlx::query(
        "SELECT COALESCE(SUM(qty_consumed), 0)::bigint AS q, \
                COALESCE(SUM(cost_amount), 0)::bigint  AS c \
         FROM cost_layer_depletions \
         WHERE layer_id IN (SELECT id FROM cost_layers WHERE pool_account_id = $1::bigint)",
    )
    .bind(pool_id)
    .fetch_one(p)
    .await
    .expect("pool_depletions_total");
    (row.get::<i64, _>("q"), row.get::<i64, _>("c"))
}

async fn cell_in_recon(p: &sqlx::PgPool, pool_id: i64) -> bool {
    let row = sqlx::query(
        "SELECT 1 AS one FROM fifo_arena_recon() \
         WHERE pool_account_id = $1::bigint LIMIT 1",
    )
    .bind(pool_id)
    .fetch_optional(p)
    .await
    .expect("cell_in_recon");
    row.is_some()
}

async fn recon_drift_for(p: &sqlx::PgPool, pool_id: i64) -> Option<i64> {
    let row = sqlx::query(
        "SELECT drift FROM fifo_arena_recon() \
         WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_optional(p)
    .await
    .expect("recon_drift_for");
    row.and_then(|r| r.get::<Option<i64>, _>("drift"))
}

/// N1 — A single durable-only pool: receipt + issue end-to-end via
/// the SPI path. Verifies that:
///
/// * cost_layers grows on receipt.
/// * cost_layer_depletions populates on issue with strict per-unit costing.
/// * posting_lines.amount matches qty × unit_cost.
/// * fifo_arena_recon() returns no row for this pool (no shmem cell
///   was allocated).
#[tokio::test]
#[ignore]
async fn n1_durable_only_receipt_and_issue() {
    let p = pool().await;
    let (pool_id, ap_id, cogs_id) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    let marked = fill_arena(&p).await;
    assert!(
        marked > 0,
        "N1 setup: fill_arena should have marked some sentinels (got {})",
        marked
    );

    // Receipt — durable-only path INSERTs to cost_layers via Phase 9c.
    let recv_key = Uuid::new_v4().to_string();
    let recv_envelopes = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_receipt",
            "debit_account_id": pool_id, "credit_account_id": ap_id,
            "qty": 100, "unit_cost": 1234,
            "idempotency_key": recv_key,
            "business_date": "2026-05-14",
        }
    ]);
    let recv_res = run_maximal(&p, recv_envelopes).await;
    assert_eq!(recv_res.len(), 1);
    assert_eq!(recv_res[0].1, "committed");

    let layers = pool_layers(&p, pool_id).await;
    assert_eq!(layers.len(), 1, "N1: receipt produced one cost_layers row");
    assert_eq!(layers[0].1, 100, "N1: layer qty_remaining = 100");
    assert_eq!(layers[0].2, 1234, "N1: layer unit_cost = 1234");

    // No shmem cell — the pool was routed durable-only.
    assert!(
        !cell_in_recon(&p, pool_id).await,
        "N1: durable-only pool must NOT appear in fifo_arena_recon"
    );

    // Issue — durable-only path SPI-walks cost_layers under FOR UPDATE
    // in Phase 8.6 and stages the consume via accum_drain → Phase 9e
    // UPDATE.
    let iss_key = Uuid::new_v4().to_string();
    let iss_envelopes = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_issue",
            "debit_account_id": cogs_id, "credit_account_id": pool_id,
            "qty": 40,
            "idempotency_key": iss_key,
            "business_date": "2026-05-14",
        }
    ]);
    let iss_res = run_maximal(&p, iss_envelopes).await;
    assert_eq!(iss_res.len(), 1);
    assert_eq!(iss_res[0].1, "committed");
    let pl_id = iss_res[0].2.expect("N1: issue posting_line_id");

    // The single layer should now have qty_remaining = 60.
    let layers = pool_layers(&p, pool_id).await;
    assert_eq!(layers.len(), 1, "N1: still one layer after partial issue");
    assert_eq!(layers[0].1, 60, "N1: qty_remaining drained to 60");

    // cost_layer_depletions covers the consumed qty + amount.
    let (dq, dc) = pool_depletions_total(&p, pool_id).await;
    assert_eq!(dq, 40, "N1: depletion total qty = 40");
    assert_eq!(dc, 40 * 1234, "N1: depletion total cost = 40 × 1234");

    // posting_lines.amount matches qty × unit_cost.
    let amt: i64 = sqlx::query("SELECT amount FROM posting_lines WHERE id = $1::bigint")
        .bind(pl_id)
        .fetch_one(&p)
        .await
        .unwrap()
        .get("amount");
    assert_eq!(
        amt,
        40 * 1234,
        "N1: issue posting_lines.amount = qty × unit_cost"
    );

    // Cleanup.
    release_sentinels(&p).await;
    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// N2 — Mixed batch: one cell-hosted pool (claimed BEFORE filling the
/// arena) and one durable-only pool (in the same batch, AFTER fill).
///
/// The cell-hosted pool's recon drift must stay 0; the durable-only
/// pool grows its cost_layers and stays absent from recon.
#[tokio::test]
#[ignore]
async fn n2_mixed_cell_hosted_and_durable_only() {
    let p = pool().await;
    let (hosted_pool, ap_id, cogs_id) = unique_accounts();
    let (durable_pool, _, _) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (hosted_pool, "fifo_hosted", "inv_value_raw"),
            (durable_pool, "fifo_durable", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    // Claim hosted_pool's cell first (still on the fast path).
    let warmup_recv = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_receipt",
            "debit_account_id": hosted_pool, "credit_account_id": ap_id,
            "qty": 50, "unit_cost": 500,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    let _ = run_maximal(&p, warmup_recv).await;
    assert!(
        cell_in_recon(&p, hosted_pool).await,
        "N2 setup: hosted pool must have a shmem cell after warmup"
    );

    // Fill remaining arena → next NEW pool gets durable-only.
    fill_arena(&p).await;

    // Pre-seed durable_pool with a receipt in its own batch — the
    // documented Phase 8.6 limitation: same-batch RYW is not supported
    // (issue's SPI walk runs before Phase 9c's INSERT), so the issue
    // below would not see this receipt's layer if they shared a batch.
    let pre_seed = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_receipt",
            "debit_account_id": durable_pool, "credit_account_id": ap_id,
            "qty": 80, "unit_cost": 700,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    let _ = run_maximal(&p, pre_seed).await;
    assert!(
        !cell_in_recon(&p, durable_pool).await,
        "N2 setup: durable_pool must NOT have a shmem cell"
    );

    // Mixed batch:
    //   env 0: receipt to hosted_pool (still cell-hosted; cell already exists)
    //   env 1: issue from hosted_pool
    //   env 2: issue from durable_pool (prior batch's layer is visible)
    let mixed = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_receipt",
            "debit_account_id": hosted_pool, "credit_account_id": ap_id,
            "qty": 20, "unit_cost": 600,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        },
        {
            "envelope_idx": 1, "kind": "fifo_issue",
            "debit_account_id": cogs_id, "credit_account_id": hosted_pool,
            "qty": 30,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        },
        {
            "envelope_idx": 2, "kind": "fifo_issue",
            "debit_account_id": cogs_id, "credit_account_id": durable_pool,
            "qty": 30,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    let res = run_maximal(&p, mixed).await;
    assert_eq!(res.len(), 3);
    for r in &res {
        assert_eq!(r.1, "committed", "N2: envelope {} committed", r.0);
    }

    // Force drain so the cell-hosted issue's pending_drain entries flush
    // to cost_layers.qty_remaining (otherwise the in-shmem ring has the
    // updated qty but durable rows still read at receipt time).
    sqlx::query("SELECT fifo_force_drain_tick()")
        .execute(&p)
        .await
        .unwrap();

    // hosted_pool: warmup 50@500 + recv 20@600 - issue 30 (consumes 30 of
    // the 50@500 layer first → remaining 20@500 + 20@600).
    let hosted = pool_layers(&p, hosted_pool).await;
    assert_eq!(hosted.len(), 2, "N2: hosted pool has 2 layers");
    assert_eq!(
        hosted[0].1, 20,
        "N2: first hosted layer (500-uc) drained to 20"
    );
    assert_eq!(
        hosted[1].1, 20,
        "N2: second hosted layer (600-uc) untouched at 20"
    );
    let hosted_drift = recon_drift_for(&p, hosted_pool).await;
    assert_eq!(
        hosted_drift,
        Some(0),
        "N2: cell-hosted pool drift=0 after mixed batch"
    );

    // durable_pool: recv 80@700 - issue 30 → one layer at 50@700.
    let durable = pool_layers(&p, durable_pool).await;
    assert_eq!(durable.len(), 1, "N2: durable_pool has 1 layer");
    assert_eq!(
        durable[0].1, 50,
        "N2: durable layer drained from 80 to 50"
    );
    assert!(
        !cell_in_recon(&p, durable_pool).await,
        "N2: durable_pool must NOT appear in fifo_arena_recon"
    );

    let (dq, dc) = pool_depletions_total(&p, durable_pool).await;
    assert_eq!(dq, 30);
    assert_eq!(dc, 30 * 700);

    release_sentinels(&p).await;
    cleanup(&p, &[hosted_pool, durable_pool, ap_id, cogs_id]).await;
}

/// N3 — Multiple distinct durable-only pools in one batch. Each pool
/// gets its own layer, and each issue walks its own pool independently.
#[tokio::test]
#[ignore]
async fn n3_multiple_durable_only_pools_one_batch() {
    let p = pool().await;
    let (pool_a, ap_id, cogs_id) = unique_accounts();
    let (pool_b, _, _) = unique_accounts();
    let (pool_c, _, _) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_a, "fifo_a", "inv_value_raw"),
            (pool_b, "fifo_b", "inv_value_raw"),
            (pool_c, "fifo_c", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    fill_arena(&p).await;

    let env = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_receipt",
            "debit_account_id": pool_a, "credit_account_id": ap_id,
            "qty": 10, "unit_cost": 100,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        },
        {
            "envelope_idx": 1, "kind": "fifo_receipt",
            "debit_account_id": pool_b, "credit_account_id": ap_id,
            "qty": 20, "unit_cost": 200,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        },
        {
            "envelope_idx": 2, "kind": "fifo_receipt",
            "debit_account_id": pool_c, "credit_account_id": ap_id,
            "qty": 30, "unit_cost": 300,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    let res = run_maximal(&p, env).await;
    assert_eq!(res.len(), 3);
    for r in &res {
        assert_eq!(r.1, "committed");
    }

    for (pid, qty, uc) in [(pool_a, 10, 100), (pool_b, 20, 200), (pool_c, 30, 300)] {
        let layers = pool_layers(&p, pid).await;
        assert_eq!(layers.len(), 1, "N3: pool {} has 1 layer", pid);
        assert_eq!(layers[0].1, qty);
        assert_eq!(layers[0].2, uc);
        assert!(
            !cell_in_recon(&p, pid).await,
            "N3: pool {} must NOT appear in fifo_arena_recon",
            pid
        );
    }

    // Issue from each pool in a second batch — first-batch receipts are
    // already INSERTed and visible to second-batch SPI walks.
    let env2 = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_issue",
            "debit_account_id": cogs_id, "credit_account_id": pool_a,
            "qty": 4,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        },
        {
            "envelope_idx": 1, "kind": "fifo_issue",
            "debit_account_id": cogs_id, "credit_account_id": pool_b,
            "qty": 5,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        },
        {
            "envelope_idx": 2, "kind": "fifo_issue",
            "debit_account_id": cogs_id, "credit_account_id": pool_c,
            "qty": 6,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    let res2 = run_maximal(&p, env2).await;
    assert_eq!(res2.len(), 3);
    for r in &res2 {
        assert_eq!(r.1, "committed");
    }

    for (pid, expected_rem) in [(pool_a, 6), (pool_b, 15), (pool_c, 24)] {
        let layers = pool_layers(&p, pid).await;
        assert_eq!(layers.len(), 1);
        assert_eq!(
            layers[0].1, expected_rem,
            "N3: pool {} drained to expected residual",
            pid
        );
    }

    release_sentinels(&p).await;
    cleanup(&p, &[pool_a, pool_b, pool_c, ap_id, cogs_id]).await;
}

/// N4 — Idempotent replay of a durable-only envelope. The second call
/// with the same idempotency_key short-circuits via Phase 2's replay
/// detection (which runs BEFORE the cell-vs-durable routing) and does
/// NOT re-consume the layer.
#[tokio::test]
#[ignore]
async fn n4_durable_only_idempotent_replay() {
    let p = pool().await;
    let (pool_id, ap_id, cogs_id) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    fill_arena(&p).await;

    // Receipt.
    let recv = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_receipt",
            "debit_account_id": pool_id, "credit_account_id": ap_id,
            "qty": 100, "unit_cost": 1000,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    let _ = run_maximal(&p, recv).await;

    // Issue with a specific idempotency_key.
    let iss_key = Uuid::new_v4().to_string();
    let iss = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_issue",
            "debit_account_id": cogs_id, "credit_account_id": pool_id,
            "qty": 30,
            "idempotency_key": iss_key,
            "business_date": "2026-05-14",
        }
    ]);
    let first = run_maximal(&p, iss.clone()).await;
    assert_eq!(first[0].1, "committed");
    let layer_after_first = pool_layers(&p, pool_id).await;
    assert_eq!(layer_after_first[0].1, 70);
    let (dq_first, _) = pool_depletions_total(&p, pool_id).await;
    assert_eq!(dq_first, 30);

    // Replay: same idempotency_key → idempotent_replay, no further
    // consume, no new depletion row.
    let second = run_maximal(&p, iss).await;
    assert_eq!(second[0].1, "idempotent_replay");
    assert_eq!(
        second[0].2, first[0].2,
        "N4: replay returns the same posting_line_id"
    );

    let layer_after_second = pool_layers(&p, pool_id).await;
    assert_eq!(
        layer_after_second[0].1, 70,
        "N4: replay did not re-consume the layer"
    );
    let (dq_second, _) = pool_depletions_total(&p, pool_id).await;
    assert_eq!(
        dq_second, 30,
        "N4: cost_layer_depletions count unchanged after replay"
    );

    release_sentinels(&p).await;
    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// N5 — durable-only issue against a pool with no prior receipts.
/// The SPI walk finds 0 rows; the remaining qty > 0 path raises a
/// pgrx::error! mentioning bucket exhaustion + the offending pool.
#[tokio::test]
#[ignore]
async fn n5_durable_only_shortage_raises_clear_error() {
    let p = pool().await;
    let (pool_id, _ap_id, cogs_id) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    fill_arena(&p).await;

    let iss = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_issue",
            "debit_account_id": cogs_id, "credit_account_id": pool_id,
            "qty": 5,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    let res = try_run_maximal(&p, iss).await;
    assert!(
        res.is_err(),
        "N5: durable-only issue without prior receipt must error"
    );
    let err_msg = format!("{}", res.unwrap_err());
    assert!(
        err_msg.contains("durable-only path exhausted")
            || err_msg.contains("bucket exhaustion")
            || err_msg.contains("short by 5"),
        "N5: error message should explain the durable-only shortage \
         (got: {})",
        err_msg
    );

    release_sentinels(&p).await;
    cleanup(&p, &[pool_id, cogs_id]).await;
}
