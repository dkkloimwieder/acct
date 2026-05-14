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

/// Build a receipt envelope batch JSONB. qty=10, unit_cost=1000 per
/// envelope.
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

// ─── Q2 / Q3 verification cases ───────────────────────────────────────
//
// The acct-majp issue description hypothesises:
//
// - **Q3**: Phase 8.6 durable_only path is clean by construction — no
//   cell allocated (Phase 4 returned None), no ring mutation, only SPI
//   + local Vec. Rollback should leave nothing in shmem.
//
// - **Q2**: Phase 8.5 spill walk on a pre-existing overflow_active
//   cell does only SPI (`SELECT FOR UPDATE` + `UPDATE qty_remaining`)
//   + local Vec. No ring mutation, no overflow flag changes. Rollback
//   should leave shmem state identical to pre-rollback snapshot.
//
// These cases verify the hypotheses empirically on the unfixed extension.
// They are invariant across A2: post-fix, they continue to pass — the
// durable_only and pure-spill-walk paths never had a leak, A2's deferral
// machinery just doesn't apply to them.

/// V4 — Q3 verification: rollback on Phase 8.6 durable_only path leaves
/// zero shmem state.
///
/// Procedure: occupy every bucket with sentinels via
/// `fifo_force_arena_full`; the real pool's first Phase 4
/// `insert_fifo_cell` returns None; the apply routes via Phase 8.6.
/// ROLLBACK; assert no recon row appears for the real pool (no cell was
/// ever allocated) and the durable side rolled back cleanly.
///
/// Destructive: marks all empty buckets across the arena. Cleanup
/// releases via `fifo_release_sentinels`. `#[ignore]`'d to keep out of
/// the default parallel suite.
#[tokio::test]
#[ignore]
async fn v4_q3_rollback_on_durable_only_path_no_shmem_leak() {
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

    // Sanity: real pool has no cell yet.
    let pre = recon_row(&p, pool_id).await;
    assert!(
        pre.is_none(),
        "V4 precondition: pool {pool_id} should have no shmem cell pre-fill. Got {pre:?}"
    );

    // Saturate the arena. Real pool will be unable to acquire a cell.
    let marked: i64 = sqlx::query("SELECT fifo_force_arena_full() AS n")
        .fetch_one(&p)
        .await
        .expect("fill arena")
        .get::<i64, _>("n");
    assert!(marked > 0, "V4: fifo_force_arena_full should mark some buckets");

    // Apply a receipt to the real pool inside a transaction, then
    // ROLLBACK. Should route via Phase 8.6 durable_only (no cell
    // allocation, no ring mutation).
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
            "business_date": "2026-05-14",
        }
    ]);
    let result: Result<_, _> = sqlx::query(
        "SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(&envelopes)
    .fetch_all(&mut *tx)
    .await;
    // Apply must succeed under durable_only routing.
    let _ = result.expect("V4: durable_only apply should succeed");
    tx.rollback().await.expect("rollback");

    // Durable side: rolled back.
    let cl: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM cost_layers WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_one(&p)
    .await
    .expect("cl count");
    assert_eq!(cl, 0, "V4: cost_layers rolled back");

    // Shmem side: zero state for real pool. Recon's `pool_account_id` filter
    // skips sentinel-key buckets (key_lo != 0 → defensive skip in recon's
    // Phase 1 walk per fifo.rs:2375).
    let post = recon_row(&p, pool_id).await;
    assert!(
        post.is_none(),
        "V4 Q3 VERIFIED: durable_only routing leaves zero shmem state \
         on the real pool — no cell was ever allocated. Got recon row: {post:?}. \
         If this fires post-fix, the A2 implementation accidentally allocated \
         a cell for the durable_only path."
    );

    // Cleanup sentinels so other tests can run.
    let cleared: i64 = sqlx::query("SELECT fifo_release_sentinels() AS n")
        .fetch_one(&p)
        .await
        .expect("release sentinels")
        .get::<i64, _>("n");
    assert!(cleared > 0, "V4: should clear at least one sentinel");

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// V5 — Q2 verification: rollback of an issue that walks only Phase 8.5
/// spill tail leaves shmem state identical to pre-rollback.
///
/// Procedure:
/// 1. Commit `MAX_LAYERS + 10` receipts → ring caps at MAX_LAYERS,
///    overflow_active=1, watermark set, 10 layers spilled to durable.
/// 2. Commit an issue draining the entire ring head → ring empty,
///    overflow_active still 1, durable tail (10 layers × qty=10) intact.
/// 3. Force drain to flush pending_drain into durable
///    cost_layers.qty_remaining.
/// 4. Snapshot recon: `shmem_live=0, shmem_pending=0, shmem_spilled=100,
///    durable=100, drift=0`.
/// 5. `BEGIN; apply issue qty=50;` → pure Phase 8.5 spill walk (ring is
///    empty so Phase 8 consume_from_head returns nothing; 8.5 walks 5
///    spilled layers via SPI `SELECT FOR UPDATE` + `UPDATE qty_remaining`).
///    `ROLLBACK;`
/// 6. Assert: post-rollback recon matches pre-rollback snapshot. Phase
///    8.5's only mutations were SPI-side (rolled back) + local Vec
///    (dies with the txn).
///
/// Invariant across A2: same outcome pre- and post-fix.
#[tokio::test]
async fn v5_q2_rollback_on_pure_spill_walk_no_shmem_delta() {
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

    // Step 1: receipts that trip overflow_active.
    let n_total = ml + 10;
    let recv = receipt_batch(pool_id, ap_id, n_total, "2026-05-14");
    sqlx::query(
        "SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(&recv)
    .fetch_all(&p)
    .await
    .expect("setup receipts");

    // Step 2: issue draining the ENTIRE in-shmem ring (qty = ml * 10).
    // Phase 8 consume_from_head drains the ring fully; 8.5 not yet
    // touched at this stage.
    let drain_qty = (ml as i64) * 10;
    let drain = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_issue",
            "debit_account_id": cogs_id,
            "credit_account_id": pool_id,
            "qty": drain_qty,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    sqlx::query(
        "SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(&drain)
    .fetch_all(&p)
    .await
    .expect("drain ring");

    // Step 3: flush pending_drain. After this, in-shmem ring is empty
    // (shmem_live=0, shmem_pending=0); durable cost_layers.qty_remaining
    // reflects the spill tail (10 × 10 = 100).
    force_drain(&p).await;

    // Step 4: snapshot.
    let snap = recon_row(&p, pool_id).await.expect("V5 cell present pre");
    let (live_pre, pending_pre, total_pre, durable_pre, drift_pre) = snap;
    assert_eq!(live_pre, 0, "V5 setup: ring drained, shmem_live=0");
    assert_eq!(pending_pre, 0, "V5 setup: drain flushed, shmem_pending=0");
    assert_eq!(total_pre, 0, "V5 setup: shmem_total=0");
    assert_eq!(
        durable_pre,
        Some(100),
        "V5 setup: durable = 10 spilled × qty 10 (got {durable_pre:?})"
    );
    assert_eq!(drift_pre, Some(0), "V5 setup: drift=0 at quiescence");

    // Step 5: pure spill walk inside a rolled-back txn.
    let mut tx = p.begin().await.expect("begin tx");
    let issue_pure_spill = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_issue",
            "debit_account_id": cogs_id,
            "credit_account_id": pool_id,
            "qty": 50,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    sqlx::query(
        "SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(&issue_pure_spill)
    .fetch_all(&mut *tx)
    .await
    .expect("pure spill issue");
    tx.rollback().await.expect("rollback");

    // Step 6: durable side rolled back — qty_remaining unchanged.
    let durable_post: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(qty_remaining), 0)::bigint FROM cost_layers \
         WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_one(&p)
    .await
    .expect("durable post");
    assert_eq!(
        durable_post, 100,
        "V5: durable cost_layers.qty_remaining rolled back to pre-txn"
    );

    // Shmem must match snapshot. Phase 8.5's local accum_drain Vec
    // died with the function frame; ring + overflow flags were never
    // touched.
    let post = recon_row(&p, pool_id).await.expect("V5 cell present post");
    assert_eq!(
        post, snap,
        "V5 Q2 VERIFIED: pure Phase 8.5 spill walk leaves zero shmem \
         delta on rollback. Pre={snap:?} Post={post:?}. \
         If this fires post-fix, A2 accidentally added shmem state to \
         the spill walk path."
    );

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}
