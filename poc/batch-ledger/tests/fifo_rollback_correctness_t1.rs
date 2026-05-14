//! acct-majp / M10 FIFO arena rollback correctness gate.
//!
//! ## A1 gate (this commit)
//!
//! `fifo_apply_batch_maximal` mutates shmem state IMMEDIATELY during the
//! apply call:
//!
//! - Phase 4: `insert_fifo_cell` allocates a bucket under arena
//!   EXCLUSIVE (creates occupancy + key, transitions seeded).
//! - Phase 8: `push_layer` advances the ring tail; `consume_from_head`
//!   advances the head and decrements layer qty; `push_pending_drain`
//!   stages an entry for the bgworker.
//! - Phase 8 (overflow path): flips `overflow_active`, CAS-sets
//!   `overflow_first_spilled_layer_id` watermark.
//! - Phase 10 `stage_apply` for balance/qty (WAC arena) — already
//!   covered by acct-4e91's XactCallback path.
//!
//! Phase 8 / 8 (overflow) mutations are not transactional. If the
//! surrounding PG transaction ROLLBACKs after the apply call, the
//! durable side rolls back cleanly (`posting_lines` / `cost_layers` /
//! `cost_layer_depletions` disappear) but the shmem ring retains the
//! pushed layer / advanced head. `fifo_arena_recon` reports the
//! divergence.
//!
//! ## V1 — minimal: single receipt, ROLLBACK
//!
//! `BEGIN; fifo_apply_batch_maximal(receipt qty=100); ROLLBACK;`
//!
//! Pre-fix: bucket allocated + ring contains a layer with qty=100, but
//! `cost_layers` is empty. Recon row exists with `shmem_live=100`,
//! `durable_qty=NULL` (pool absent from cost_layers).
//!
//! ## V2 — recon-coupled: pre-existing layer + receipt + ROLLBACK
//!
//! Commit a receipt (qty=100) first to establish a durable layer +
//! shmem cell. Then `BEGIN; receipt qty=50; ROLLBACK;`. Pre-fix: shmem
//! ring shows 150 (100 + 50), durable shows 100; drift = 100 - 150 =
//! -50.
//!
//! ## V3 — invariant: COMMIT path applies normally
//!
//! Same receipt apply but with COMMIT. Drift = 0 both pre- and
//! post-fix. Pins the "we didn't break the happy path" half of the A2
//! contract.
//!
//! ## Post-A2 polarity flip
//!
//! When A2 lands (per-cell ring deltas staged into PENDING_STACK,
//! replayed at xact_commit, discarded at xact_abort), the V1/V2
//! assertions flip from `drift != 0` to `drift == 0` (or no recon
//! row at all). V3's assertion is invariant across the flip. The
//! polarity flip commit modifies this file in place.
//!
//! Destructive of shmem state for the synthetic pool keys; uses a
//! disjoint base (`4_500_000_000_000`) from other FIFO test binaries.

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
    // Distinct base from inline (1e12), WAC maximal (1.5e12), maximal F
    // (2.1e12), drain (2.5e12), recon (2.9e12), crash-recovery (3.3e12),
    // spill (3.7e12), durable_only (4.1e12). Rollback claims 4.5e12.
    let base = 4_500_000_000_000_i64
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

async fn recon_row(
    p: &sqlx::PgPool,
    pool_id: i64,
) -> Option<(i64, i64, i64, Option<i64>, Option<i64>)> {
    let row = sqlx::query(
        "SELECT shmem_live_qty, shmem_pending_qty, shmem_total_qty, durable_qty, drift \
         FROM fifo_arena_recon() \
         WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_optional(p)
    .await
    .expect("recon row");
    row.map(|r| {
        (
            r.get::<i64, _>("shmem_live_qty"),
            r.get::<i64, _>("shmem_pending_qty"),
            r.get::<i64, _>("shmem_total_qty"),
            r.get::<Option<i64>, _>("durable_qty"),
            r.get::<Option<i64>, _>("drift"),
        )
    })
}

/// V1 — minimal: BEGIN; fifo_receipt qty=100; ROLLBACK.
///
/// **A1 polarity (pre-fix):** the rolled-back receipt leaks a layer
/// into the shmem ring. Recon row exists with `shmem_live_qty=100`
/// and `durable_qty=NULL` (cost_layers row rolled back).
///
/// Post-A2 this assertion FLIPS: either no recon row (ring empty), or
/// row with `shmem_live_qty=0`.
#[tokio::test]
async fn v1_rollback_leaks_layer_to_ring() {
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

    // Sanity: no shmem cell for this synthetic pool yet.
    let pre = recon_row(&p, pool_id).await;
    assert!(
        pre.is_none(),
        "V1 precondition: synthetic pool {pool_id} should have no shmem cell. \
         Got recon row {pre:?}. If this fires, the base-prefix isolation \
         (4_500_000_000_000) collided with another test."
    );

    // Apply a receipt inside a transaction, then ROLLBACK.
    let mut tx = p.begin().await.expect("begin tx");
    let envelopes = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 100,
            "unit_cost": 1000,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }
    ]);
    sqlx::query(
        "SELECT envelope_idx, status, posting_line_id \
         FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(&envelopes)
    .fetch_all(&mut *tx)
    .await
    .expect("apply receipt");
    tx.rollback().await.expect("rollback");

    // Durable side: cost_layers row rolled back, posting_lines empty.
    let durable_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM cost_layers WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_one(&p)
    .await
    .expect("count cost_layers");
    assert_eq!(durable_rows, 0, "V1: cost_layers rolled back as expected");

    let posting_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM posting_lines \
         WHERE debit_account_id = $1::bigint OR credit_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_one(&p)
    .await
    .expect("count posting_lines");
    assert_eq!(posting_rows, 0, "V1: posting_lines rolled back as expected");

    // Shmem side: PRE-A2 — the receipt's layer leaks into the ring.
    let post = recon_row(&p, pool_id).await;
    assert!(
        post.is_some(),
        "V1 A1: expected recon row to exist showing the leaked layer \
         (cell allocated by Phase 4 + layer pushed by Phase 8). Got None — \
         either A2 already landed and this gate's polarity should flip, or \
         the cell allocation path is unexpectedly deferred. Found: {post:?}"
    );
    let (live, pending, total, durable, drift) = post.unwrap();
    assert_eq!(
        live, 100,
        "V1 A1 BUG CONFIRMED: ring retains the rolled-back receipt's layer \
         (shmem_live={live}, expected 100). Phase 8's push_layer leaked."
    );
    assert_eq!(pending, 0, "V1: no pending_drain for receipt-only batch");
    assert_eq!(total, 100, "V1: shmem_total = live + pending = 100");
    assert_eq!(
        durable, None,
        "V1: durable_qty is NULL because cost_layers row was rolled back"
    );
    assert_eq!(
        drift, None,
        "V1: drift is NULL when durable is NULL (recon's pool_exists CTE \
         produces no row for empty cost_layers)"
    );

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// V2 — recon-coupled: committed receipt + rolled-back receipt.
///
/// Establishes a durable+shmem baseline (100), then runs a receipt in
/// a rolled-back transaction (+50). Pre-fix: shmem 150 > durable 100;
/// drift = 100 - 150 = -50.
///
/// Post-A2: shmem 100 = durable 100; drift = 0.
#[tokio::test]
async fn v2_rollback_recon_shows_drift() {
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

    // Establish a baseline layer (committed).
    let baseline = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 100,
            "unit_cost": 1000,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }
    ]);
    sqlx::query(
        "SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(&baseline)
    .fetch_all(&p)
    .await
    .expect("baseline receipt");

    // Sanity baseline: drift=0 at quiescence.
    let baseline_row = recon_row(&p, pool_id).await.expect("baseline cell");
    assert_eq!(
        baseline_row.0, 100,
        "V2 precondition: baseline shmem_live = 100 (got {})",
        baseline_row.0
    );
    assert_eq!(
        baseline_row.3,
        Some(100),
        "V2 precondition: baseline durable_qty = 100 (got {:?})",
        baseline_row.3
    );
    assert_eq!(
        baseline_row.4,
        Some(0),
        "V2 precondition: baseline drift = 0 (got {:?})",
        baseline_row.4
    );

    // Apply a second receipt inside a transaction, then ROLLBACK.
    let mut tx = p.begin().await.expect("begin tx");
    let rolled_back = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 50,
            "unit_cost": 1200,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }
    ]);
    sqlx::query(
        "SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(&rolled_back)
    .fetch_all(&mut *tx)
    .await
    .expect("apply receipt 2");
    tx.rollback().await.expect("rollback");

    // Durable still 100 (the second receipt's INSERT rolled back).
    let durable_sum: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(qty_remaining), 0)::bigint FROM cost_layers \
         WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_one(&p)
    .await
    .expect("durable sum");
    assert_eq!(durable_sum, 100, "V2: durable still 100 post-rollback");

    // Shmem ring: PRE-A2 has 100 + 50 = 150.
    let post = recon_row(&p, pool_id).await.expect("V2 cell present");
    let (live, _pending, _total, durable, drift) = post;
    assert_eq!(
        live, 150,
        "V2 A1 BUG CONFIRMED: ring retains rolled-back layer \
         (shmem_live={live}, expected 100+50=150)"
    );
    assert_eq!(durable, Some(100), "V2: durable_qty = 100");
    assert_eq!(
        drift,
        Some(-50),
        "V2 A1 BUG CONFIRMED: drift = durable - shmem_total = 100 - 150 = -50; got {drift:?}"
    );

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// V3 — invariant: COMMIT path applies the receipt normally.
///
/// Same envelope shape as V1 but the transaction commits. Drift=0 both
/// pre- and post-A2; pins "happy path still works" in the regression
/// net.
#[tokio::test]
async fn v3_commit_applies_receipt_normally() {
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

    let mut tx = p.begin().await.expect("begin tx");
    let envelopes = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 250,
            "unit_cost": 1100,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }
    ]);
    sqlx::query(
        "SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(&envelopes)
    .fetch_all(&mut *tx)
    .await
    .expect("apply");
    tx.commit().await.expect("commit");

    let post = recon_row(&p, pool_id).await.expect("V3 cell present");
    let (live, pending, _total, durable, drift) = post;
    assert_eq!(live, 250, "V3: shmem_live=250 after commit");
    assert_eq!(pending, 0, "V3: no pending");
    assert_eq!(durable, Some(250), "V3: durable=250 after commit");
    assert_eq!(drift, Some(0), "V3: drift=0 after commit");

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}
