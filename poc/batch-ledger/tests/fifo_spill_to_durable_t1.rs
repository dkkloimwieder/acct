//! acct-b8ub — sub 3 FIFO MAX_LAYERS spill-to-durable.
//!
//! When `n_layers == MAX_LAYERS` at receipt time, the overflowing receipt
//! INSERTs to durable `cost_layers` only — the ring is left untouched.
//! The cell flips `overflow_active = 1` and captures the first spilled
//! `layer_id` as the watermark. Issues consume the in-shmem head first,
//! then SPI-walk durable rows with `id >= watermark` in FIFO order.
//!
//! Strict per-unit FIFO preserved. No averaging, no precision loss.
//!
//! Coverage:
//!
//! - S1 receipt-only burst past `MAX_LAYERS`: shmem stops at the cap,
//!   `cost_layers` grows beyond, recon reports `shmem_spilled_qty > 0`
//!   and `drift = 0`.
//! - S2 issue drains pool entirely (in-shmem ring + spilled tail):
//!   spill walk consumes the tail in FIFO order; recon collapses to 0.
//! - S3 mixed receipt + issue across the spill boundary: drift stays 0
//!   at each quiescence checkpoint.
//! - S4 idempotent replay of a spill-deferred issue: re-call with the
//!   same idempotency_key returns `idempotent_replay` without
//!   re-consuming the spilled tail.

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
    // (2.1e12), drain (2.5e12), recon (2.9e12), crash-recovery (3.3e12)
    // — spill claims 3.7e12.
    let base = 3_700_000_000_000_i64
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

async fn force_drain(p: &sqlx::PgPool) {
    sqlx::query("SELECT fifo_force_drain_tick()")
        .execute(p)
        .await
        .expect("force drain");
}

async fn max_layers(p: &sqlx::PgPool) -> i64 {
    sqlx::query("SELECT fifo_max_layers() AS ml")
        .fetch_one(p)
        .await
        .expect("max_layers")
        .get::<i64, _>("ml")
}

/// Recon row tuple (shmem_live, shmem_pending, shmem_total, shmem_spilled,
/// durable_qty, drift).
async fn recon_row(
    p: &sqlx::PgPool,
    pool_id: i64,
) -> Option<(i64, i64, i64, i64, Option<i64>, Option<i64>)> {
    let row = sqlx::query(
        "SELECT shmem_live_qty, shmem_pending_qty, shmem_total_qty, \
                shmem_spilled_qty, durable_qty, drift \
         FROM fifo_arena_recon() \
         WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_optional(p)
    .await
    .expect("recon");
    row.map(|r| {
        (
            r.get::<i64, _>("shmem_live_qty"),
            r.get::<i64, _>("shmem_pending_qty"),
            r.get::<i64, _>("shmem_total_qty"),
            r.get::<i64, _>("shmem_spilled_qty"),
            r.get::<Option<i64>, _>("durable_qty"),
            r.get::<Option<i64>, _>("drift"),
        )
    })
}

async fn cost_layers_count(p: &sqlx::PgPool, pool_id: i64) -> i64 {
    sqlx::query(
        "SELECT COUNT(*)::bigint AS n FROM cost_layers \
         WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_one(p)
    .await
    .expect("count cost_layers")
    .get::<i64, _>("n")
}

async fn cost_layers_active_count(p: &sqlx::PgPool, pool_id: i64) -> i64 {
    sqlx::query(
        "SELECT COUNT(*)::bigint AS n FROM cost_layers \
         WHERE pool_account_id = $1::bigint AND qty_remaining > 0",
    )
    .bind(pool_id)
    .fetch_one(p)
    .await
    .expect("count active cost_layers")
    .get::<i64, _>("n")
}

/// Build a receipt envelope batch JSONB. Each receipt is qty=10,
/// unit_cost=1000 — small enough that 300 receipts comfortably fit
/// in a single JSONB array but distinct enough that we can verify
/// counts via aggregation.
fn receipt_batch(
    pool_id: i64,
    ap_id: i64,
    n: usize,
    business_date: &str,
) -> serde_json::Value {
    let mut envelopes: Vec<serde_json::Value> = Vec::with_capacity(n);
    for i in 0..n {
        envelopes.push(serde_json::json!({
            "envelope_idx": i as i32,
            "kind": "fifo_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 10,
            "unit_cost": 1000,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": business_date,
        }));
    }
    serde_json::Value::Array(envelopes)
}

/// S1 — receipt-only burst past MAX_LAYERS. Shmem caps at the ring's
/// cap; cost_layers grows beyond; recon reports shmem_spilled_qty > 0
/// and drift = 0.
#[tokio::test]
async fn s1_receipt_overflow_caps_shmem_and_spills_durable() {
    let p = pool().await;
    let ml = max_layers(&p).await as usize; // 256 in current PoC
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

    let n_total = ml + 14; // 14 spilled receipts
    let envelopes = receipt_batch(pool_id, ap_id, n_total, "2026-05-14");
    let results = run_maximal(&p, envelopes).await;
    assert_eq!(
        results.len(),
        n_total,
        "S1: all envelopes processed"
    );
    for (i, status, pl_id) in &results {
        assert_eq!(status, "committed", "S1: env {i} committed");
        assert!(pl_id.is_some(), "S1: env {i} has posting_line_id");
    }
    force_drain(&p).await;

    let total_durable_qty = (n_total as i64) * 10;
    let ring_qty = (ml as i64) * 10;
    let spilled_qty = total_durable_qty - ring_qty;

    let row = recon_row(&p, pool_id).await.expect("S1: cell exists");
    let (live, pending, total, spilled, durable, drift) = row;
    assert_eq!(live, ring_qty, "S1: ring capped at MAX_LAYERS * 10");
    assert_eq!(pending, 0, "S1: no consume → no pending");
    assert_eq!(total, ring_qty);
    assert_eq!(
        spilled,
        spilled_qty,
        "S1: shmem_spilled_qty == (n_total - ml) * 10"
    );
    assert_eq!(
        durable,
        Some(total_durable_qty),
        "S1: durable = all receipts"
    );
    assert_eq!(
        drift,
        Some(0),
        "S1: drift = durable - spilled - shmem_total = 0"
    );

    // Sanity: cost_layers actually has all n_total rows (durable INSERT
    // happened for spilled receipts too).
    let cl_count = cost_layers_count(&p, pool_id).await;
    assert_eq!(cl_count, n_total as i64, "S1: cost_layers count = n_total");

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// S2 — issue drains the pool entirely. In-shmem ring goes head-first;
/// spill walk picks up the tail. Recon collapses to 0 at quiescence.
#[tokio::test]
async fn s2_issue_drains_in_shmem_then_spill_tail() {
    let p = pool().await;
    let ml = max_layers(&p).await as usize;
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

    let n_total = ml + 14;
    let recv = receipt_batch(pool_id, ap_id, n_total, "2026-05-14");
    let _ = run_maximal(&p, recv).await;
    force_drain(&p).await;

    // Issue the full pool: ml*10 from ring + 14*10 from spilled tail.
    let issue_qty = (n_total as i64) * 10;
    let issue = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_issue",
            "debit_account_id": cogs_id,
            "credit_account_id": pool_id,
            "qty": issue_qty,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    let issue_results = run_maximal(&p, issue).await;
    assert_eq!(issue_results.len(), 1);
    let (_, status, pl_id) = &issue_results[0];
    assert_eq!(status, "committed", "S2: issue committed");
    assert!(pl_id.is_some(), "S2: issue has posting_line_id");

    // Sanity: posting_line.amount = full pool cost (ml * 10 * 1000 +
    // 14 * 10 * 1000).
    let pl_amount: i64 = sqlx::query("SELECT amount FROM posting_lines WHERE id = $1::bigint")
        .bind(pl_id.unwrap())
        .fetch_one(&p)
        .await
        .expect("pl amount")
        .get("amount");
    assert_eq!(
        pl_amount,
        issue_qty * 1000,
        "S2: posting_line.amount = full pool cost (in-shmem + spill summed)"
    );

    // cost_layer_depletions should cover EVERY layer (256 in-shmem + 14
    // spilled = 270 depletions).
    let dp_count: i64 = sqlx::query(
        "SELECT COUNT(*)::bigint AS n FROM cost_layer_depletions \
         WHERE issue_posting_line_id = $1::bigint",
    )
    .bind(pl_id.unwrap())
    .fetch_one(&p)
    .await
    .expect("dp count")
    .get::<i64, _>("n");
    assert_eq!(
        dp_count, n_total as i64,
        "S2: depletions covers every consumed layer (in-shmem + spill)"
    );

    force_drain(&p).await;

    // Pool fully drained.
    let row = recon_row(&p, pool_id).await.expect("S2: cell exists");
    let (live, pending, total, spilled, durable, drift) = row;
    assert_eq!(live, 0, "S2: ring fully drained");
    assert_eq!(pending, 0, "S2: pending flushed");
    assert_eq!(total, 0);
    assert_eq!(spilled, 0, "S2: spilled tail consumed");
    assert_eq!(durable, Some(0), "S2: all cost_layers drained");
    assert_eq!(drift, Some(0));

    // No active cost_layers anywhere.
    assert_eq!(
        cost_layers_active_count(&p, pool_id).await,
        0,
        "S2: every cost_layers row has qty_remaining = 0"
    );

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// S3 — mixed receipt + issue across the spill boundary. Sticky-spill
/// keeps subsequent receipts on the durable-only path even after some
/// in-shmem ring slots free up. Recon drift = 0 at each quiescence.
#[tokio::test]
async fn s3_mixed_traffic_across_spill_boundary() {
    let p = pool().await;
    let ml = max_layers(&p).await as usize;
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

    // Step 1: fill ring + push 4 over → overflow_active.
    let burst = receipt_batch(pool_id, ap_id, ml + 4, "2026-05-14");
    let _ = run_maximal(&p, burst).await;
    force_drain(&p).await;

    // Drift = 0 at first quiescence; spilled = 4 receipts × 10.
    let row1 = recon_row(&p, pool_id).await.expect("S3.1: cell");
    assert_eq!(row1.5, Some(0), "S3.1: drift=0 after initial overflow");
    assert_eq!(row1.3, 40, "S3.1: 4 spilled receipts × 10");

    // Step 2: issue 500 (each layer is qty=10, so 50 layers fully
    // consumed). Ring drops from ml × 10 to (ml - 50) × 10.
    let issue = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_issue",
            "debit_account_id": cogs_id,
            "credit_account_id": pool_id,
            "qty": 500,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    let _ = run_maximal(&p, issue).await;
    force_drain(&p).await;

    let row2 = recon_row(&p, pool_id).await.expect("S3.2: cell");
    let (_l2, _pen2, total2, spilled2, _d2, drift2) = row2;
    assert_eq!(drift2, Some(0), "S3.2: drift=0 after 500-issue");
    assert_eq!(spilled2, 40, "S3.2: spilled tail untouched (ring still had qty)");
    assert_eq!(
        total2,
        (ml as i64 - 50) * 10,
        "S3.2: ring lost 500 qty (50 fully-consumed layers)"
    );

    // Step 3: push 5 more receipts. Sticky-spill: even though the ring
    // has 50 free slots (50 layers were freed by the 500-issue),
    // overflow_active=1 keeps the new receipts on the durable-only path.
    let more = receipt_batch(pool_id, ap_id, 5, "2026-05-14");
    let _ = run_maximal(&p, more).await;
    force_drain(&p).await;

    let row3 = recon_row(&p, pool_id).await.expect("S3.3: cell");
    let (live3, _pen3, _t3, spilled3, _d3, drift3) = row3;
    assert_eq!(drift3, Some(0), "S3.3: drift=0 after sticky-spill receipts");
    assert_eq!(
        live3,
        (ml as i64 - 50) * 10,
        "S3.3: ring unchanged — sticky-spill kept new receipts in durable"
    );
    assert_eq!(
        spilled3,
        40 + 5 * 10,
        "S3.3: spilled grew by new receipts"
    );

    // Step 4: issue larger than the ring's remaining qty — forces spill
    // walk to consume part of the spilled tail.
    let ring_qty_at_step4 = (ml as i64 - 50) * 10;
    let spilled_qty_at_step4 = (40 + 5 * 10) as i64;
    // Pool total = ring + spilled; issue ring + 40 (half of spilled).
    let issue_qty: i64 = ring_qty_at_step4 + 40;
    assert!(
        issue_qty > ring_qty_at_step4,
        "S3.4 precondition: issue must require spill"
    );
    assert!(
        issue_qty < ring_qty_at_step4 + spilled_qty_at_step4,
        "S3.4 precondition: issue must NOT exhaust pool (leave residual spill)"
    );
    let issue2 = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_issue",
            "debit_account_id": cogs_id,
            "credit_account_id": pool_id,
            "qty": issue_qty,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    let _ = run_maximal(&p, issue2).await;
    force_drain(&p).await;

    let row4 = recon_row(&p, pool_id).await.expect("S3.4: cell");
    let (live4, pen4, _total4, spilled4, durable4, drift4) = row4;
    assert_eq!(drift4, Some(0), "S3.4: drift=0 after spill-consume issue");
    assert_eq!(pen4, 0, "S3.4: pending flushed");
    // After: ring fully consumed (live4 = 0) + 40 of the 90 spilled
    // consumed (spilled4 = 50). Total remaining = 50.
    let remaining =
        ring_qty_at_step4 + spilled_qty_at_step4 - issue_qty;
    assert_eq!(
        live4 + spilled4,
        remaining,
        "S3.4: total remaining = ring + spilled (drained FIFO head-first)"
    );
    assert_eq!(
        durable4,
        Some(remaining),
        "S3.4: durable matches ring + spilled"
    );
    assert_eq!(live4, 0, "S3.4: ring fully drained by this issue");
    assert_eq!(
        spilled4,
        spilled_qty_at_step4 - 40,
        "S3.4: spilled tail consumed exactly 40 (issue overshot ring by 40)"
    );

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// S4 — idempotent replay of an issue whose spill consume already
/// committed. Phase 2's posting_lines replay-detect path returns
/// `idempotent_replay` without entering Phase 8 / 8.5, so the spilled
/// tail is NOT double-consumed.
#[tokio::test]
async fn s4_spill_issue_idempotent_replay() {
    let p = pool().await;
    let ml = max_layers(&p).await as usize;
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

    // Setup: 10 over the cap → overflow_active, 10 spilled × 10 = 100
    // spilled qty.
    let recv = receipt_batch(pool_id, ap_id, ml + 10, "2026-05-14");
    let _ = run_maximal(&p, recv).await;
    force_drain(&p).await;

    // First issue: drain entire ring + half the spill (force spill walk).
    let issue_qty = (ml as i64) * 10 + 50;
    let idem = Uuid::new_v4().to_string();
    let issue = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_issue",
            "debit_account_id": cogs_id,
            "credit_account_id": pool_id,
            "qty": issue_qty,
            "idempotency_key": idem,
            "business_date": "2026-05-14",
        }
    ]);
    let first = run_maximal(&p, issue.clone()).await;
    assert_eq!(first[0].1, "committed", "S4: first call committed");
    let first_pl = first[0].2.unwrap();
    force_drain(&p).await;

    let row_after_first = recon_row(&p, pool_id).await.expect("S4: cell");
    let pool_qty_after_first =
        row_after_first.0 + row_after_first.3; // live + spilled
    let expected_remaining = (ml as i64 + 10) * 10 - issue_qty;
    assert_eq!(
        pool_qty_after_first, expected_remaining,
        "S4: post-first-issue pool = receipts - issue"
    );
    assert_eq!(row_after_first.5, Some(0));

    // Replay: same idempotency_key, same envelope. Phase 2 detects, no
    // Phase 8 mutation.
    let replay = run_maximal(&p, issue).await;
    assert_eq!(
        replay[0].1, "idempotent_replay",
        "S4: second call detected as replay"
    );
    assert_eq!(
        replay[0].2,
        Some(first_pl),
        "S4: replay returns the original posting_line_id"
    );
    force_drain(&p).await;

    // Pool state unchanged by replay.
    let row_after_replay = recon_row(&p, pool_id).await.expect("S4: cell");
    assert_eq!(
        row_after_replay.0 + row_after_replay.3,
        expected_remaining,
        "S4: replay must not double-consume"
    );
    assert_eq!(row_after_replay.5, Some(0));

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}
