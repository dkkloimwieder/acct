//! acct-majp / acct-b3vs — M10 FIFO arena rollback correctness.
//!
//! ## Pre-A2 baseline (acct-majp A1, commits 0827461 + a859d3f)
//!
//! `fifo_apply_batch_maximal` mutated shmem state IMMEDIATELY during the
//! apply call (Phase 8 ring push / consume / pending_drain push / overflow
//! flip). On ROLLBACK the durable side cleaned up but shmem retained the
//! state — `fifo_arena_recon` flagged drift. V1/V2 were the gate tests
//! confirming the bug; V3/V4/V5 the invariants that already held.
//!
//! ## A2 (acct-b3vs)
//!
//! `fifo_apply_batch_maximal` Phase 8 now applies against a per-cell
//! `CellShadow` held in a thread_local `FIFO_PENDING_STACK`. Mutations
//! are staged as `LayerOp` (Push / Consume / PendingDrainPush /
//! OverflowActivate). On `xact_commit`, sorted cell-idx walk acquires
//! each cell EXCLUSIVE and replays. On `xact_abort` shadows discard.
//! SubXactCallback handles SAVEPOINT / ROLLBACK TO.
//!
//! Phase 4 cell allocation stays in-place (bucket leak under abort is
//! bounded). Phase 8.5 / 8.6 stay unchanged (already SPI-only +
//! local-Vec).
//!
//! ## Polarity post-A2
//!
//! - V1: shmem_live=0 (ring empty); recon row may exist (cell allocated
//!   by Phase 4) but reports zero state.
//! - V2: shmem_live=100 (baseline preserved); drift=0.
//! - V3 / V4 / V5: invariant — same outcome pre- and post-A2.
//!
//! ## New cases added by A2
//!
//! - V6: SAVEPOINT s1; apply; ROLLBACK TO s1; COMMIT → no shmem state.
//! - V7: nested savepoint mixed commit/rollback → only s1's apply lands.
//! - V8: ROLLBACK TO s1 then re-apply → only the re-apply lands.
//! - V9: in-batch overflow_active flip rolled back → real cell flag=0.
//! - V10: mixed cell-hosted + durable_only partial rollback.
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
/// **Post-A2 polarity:** the rolled-back receipt's layer push lives only
/// in `FIFO_PENDING_STACK`. On `xact_abort` the stack is discarded; the
/// real ring never sees the push. Phase 4 still allocated the cell, so
/// a recon row may exist (cell occupied) but all `shmem_*_qty` columns
/// are zero. Durable side cleaned up.
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

    // Shmem side: post-A2 — shadow discarded on abort. Phase 4 cell
    // allocation stayed in-place, so a recon row may exist showing the
    // cell is allocated but empty.
    let post = recon_row(&p, pool_id).await;
    match post {
        Some((live, pending, total, durable, drift)) => {
            assert_eq!(
                live, 0,
                "V1 A2: ring empty after rollback (cell allocated by Phase 4 \
                 but Phase 8 ops stayed in shadow). shmem_live={live}"
            );
            assert_eq!(pending, 0, "V1 A2: no pending_drain — receipt-only batch staged nothing");
            assert_eq!(total, 0, "V1 A2: shmem_total = 0");
            assert_eq!(
                durable, None,
                "V1 A2: durable_qty NULL because cost_layers row was rolled back"
            );
            assert_eq!(
                drift, None,
                "V1 A2: drift NULL when durable is NULL"
            );
        }
        None => {
            // Acceptable: if recon's CTE produces no row for empty cells,
            // the absence is also a pass signal.
        }
    }

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

    // Shmem ring: post-A2 — the rolled-back receipt stayed in shadow
    // (xact_abort discarded). Real ring shows only the baseline 100.
    let post = recon_row(&p, pool_id).await.expect("V2 cell present");
    let (live, _pending, _total, durable, drift) = post;
    assert_eq!(
        live, 100,
        "V2 A2: ring unchanged from baseline after rollback \
         (shmem_live={live}, expected 100). The rolled-back receipt's \
         Push op stayed in FIFO_PENDING_STACK and was discarded by \
         xact_abort."
    );
    assert_eq!(durable, Some(100), "V2 A2: durable_qty = 100");
    assert_eq!(
        drift,
        Some(0),
        "V2 A2: drift = 0; rolled-back ops never reached the real ring; \
         got {drift:?}"
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

// ─── A2 (acct-b3vs) — savepoint + overflow + mixed-routing cases ──────
//
// V6/V7/V8 exercise SubXactCallback frame management. V9 verifies the
// OverflowActivate op is correctly deferred through commit/rollback.
// V10 mixes Phase 4 cell-hosted with Phase 8.6 durable_only in one txn.

/// V6 — BEGIN; SAVEPOINT s1; apply receipt; ROLLBACK TO s1; COMMIT.
///
/// SubXactCallback START_SUB pushes a frame; the apply lands in s1's
/// frame. ROLLBACK TO s1 → ABORT_SUB pops the frame (discarding the
/// shadow) then PG re-pushes s1 → START_SUB inserts a fresh empty
/// frame. COMMIT → top-level COMMIT_SUB merges empty s1' into top
/// (no-op) then xact_commit drains top-frame (empty) → no replay.
///
/// Net: no shmem state for this pool. cost_layers also clean (PG
/// rolled back the subxact's INSERTs).
#[tokio::test]
async fn v6_savepoint_rollback_to_discards_apply() {
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
    sqlx::query("SAVEPOINT s1")
        .execute(&mut *tx)
        .await
        .expect("savepoint s1");
    let envelopes = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 75,
            "unit_cost": 1000,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    sqlx::query(
        "SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(&envelopes)
    .fetch_all(&mut *tx)
    .await
    .expect("apply within s1");
    sqlx::query("ROLLBACK TO SAVEPOINT s1")
        .execute(&mut *tx)
        .await
        .expect("rollback to s1");
    tx.commit().await.expect("commit top-level");

    let cl: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM cost_layers WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_one(&p)
    .await
    .expect("cl");
    assert_eq!(cl, 0, "V6: durable cost_layers rolled back by ROLLBACK TO");

    let post = recon_row(&p, pool_id).await;
    if let Some((live, pending, total, _, _)) = post {
        assert_eq!(live, 0, "V6: shmem_live=0 (shadow discarded by ABORT_SUB)");
        assert_eq!(pending, 0, "V6: no pending");
        assert_eq!(total, 0, "V6: shmem_total=0");
    }
    // None is also acceptable (cell allocated by Phase 4 but recon CTE
    // may not surface a row when ring is empty + no durable).

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// V7 — nested savepoint, inner aborts, outer commits.
///
/// BEGIN; SAVEPOINT s1; receipt A (qty=80); SAVEPOINT s2; receipt B
/// (qty=20); ROLLBACK TO s2; COMMIT;
///
/// Frame timeline:
/// - START_SUB s1 → push frame_s1
/// - apply A → shadow_A in frame_s1
/// - START_SUB s2 → push frame_s2 (inherits A's ring from frame_s1)
/// - apply B → shadow_B in frame_s2 (B's qty applied to inherited ring)
/// - ROLLBACK TO s2 → ABORT_SUB pops frame_s2 (discards B);
///   re-START_SUB s2 → push empty frame_s2'
/// - COMMIT:
///   - COMMIT_SUB s2' → merge empty frame_s2' into frame_s1 (no-op)
///   - COMMIT_SUB s1 → merge frame_s1 (with shadow_A) into top frame
///   - xact_commit → replay shadow_A's ops → push layer A; ring shows 80.
///
/// Net: live=80, durable=80, drift=0. B never applied.
#[tokio::test]
async fn v7_nested_savepoint_mixed_commit_rollback() {
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
    sqlx::query("SAVEPOINT s1").execute(&mut *tx).await.expect("s1");
    let recv_a = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_receipt",
            "debit_account_id": pool_id, "credit_account_id": ap_id,
            "qty": 80, "unit_cost": 1000,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    sqlx::query(
        "SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(&recv_a)
    .fetch_all(&mut *tx)
    .await
    .expect("apply A under s1");

    sqlx::query("SAVEPOINT s2").execute(&mut *tx).await.expect("s2");
    let recv_b = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_receipt",
            "debit_account_id": pool_id, "credit_account_id": ap_id,
            "qty": 20, "unit_cost": 1100,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    sqlx::query(
        "SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(&recv_b)
    .fetch_all(&mut *tx)
    .await
    .expect("apply B under s2");

    sqlx::query("ROLLBACK TO SAVEPOINT s2")
        .execute(&mut *tx)
        .await
        .expect("rollback to s2");
    tx.commit().await.expect("commit");

    let durable: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(qty_remaining), 0)::bigint FROM cost_layers \
         WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_one(&p)
    .await
    .expect("durable");
    assert_eq!(durable, 80, "V7: only A committed; durable=80");

    let post = recon_row(&p, pool_id).await.expect("V7 cell");
    assert_eq!(
        post.0, 80,
        "V7: live=80 (B's ops in s2's frame were discarded by ABORT_SUB)"
    );
    assert_eq!(post.3, Some(80), "V7: durable_qty=80");
    assert_eq!(post.4, Some(0), "V7: drift=0");

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// V8 — apply, ROLLBACK TO, re-apply, COMMIT.
///
/// BEGIN; SAVEPOINT s1; receipt A (qty=42); ROLLBACK TO s1; receipt B
/// (qty=99); COMMIT;
///
/// Net: only B applied. ROLLBACK TO discards A's shadow; receipt B
/// (still under re-pushed s1) commits to top frame on COMMIT, drains
/// at xact_commit → live=99.
#[tokio::test]
async fn v8_savepoint_rollback_then_apply_persists() {
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
    sqlx::query("SAVEPOINT s1").execute(&mut *tx).await.expect("s1");
    let recv_a = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_receipt",
            "debit_account_id": pool_id, "credit_account_id": ap_id,
            "qty": 42, "unit_cost": 1000,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    sqlx::query(
        "SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(&recv_a)
    .fetch_all(&mut *tx)
    .await
    .expect("apply A");

    sqlx::query("ROLLBACK TO SAVEPOINT s1")
        .execute(&mut *tx)
        .await
        .expect("rollback to s1");

    let recv_b = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_receipt",
            "debit_account_id": pool_id, "credit_account_id": ap_id,
            "qty": 99, "unit_cost": 1200,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    sqlx::query(
        "SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(&recv_b)
    .fetch_all(&mut *tx)
    .await
    .expect("apply B");

    tx.commit().await.expect("commit");

    let durable: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(qty_remaining), 0)::bigint FROM cost_layers \
         WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_one(&p)
    .await
    .expect("durable");
    assert_eq!(durable, 99, "V8: only B committed; durable=99");

    let post = recon_row(&p, pool_id).await.expect("V8 cell");
    assert_eq!(post.0, 99, "V8: live=99");
    assert_eq!(post.3, Some(99), "V8: durable_qty=99");
    assert_eq!(post.4, Some(0), "V8: drift=0");

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// V9 — overflow transition rolled back.
///
/// Setup: commit MAX_LAYERS receipts (ring full, overflow_active=0
/// still — push_layer returned true on the last one because n_layers
/// went from MAX-1 to MAX exactly, no further pushes attempted yet).
/// Then `BEGIN; receipt`. The (MAX_LAYERS+1)-th push in shadow returns
/// false; shadow's overflow_active flips to 1; ops record
/// OverflowActivate. `ROLLBACK`.
///
/// Post-A2: real cell's overflow_active stays at 0 (the
/// OverflowActivate op stayed in shadow, was discarded by xact_abort).
/// fifo_overflow_state() returns (false, 0).
///
/// Pre-A2 this would have returned (true, lid) — the apply-time
/// `b.overflow_active.store(1, Release)` mutated the real cell
/// directly.
#[tokio::test]
async fn v9_overflow_transition_rolled_back() {
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

    // Fill ring to MAX_LAYERS exactly (committed).
    let recv = receipt_batch(pool_id, ap_id, ml, "2026-05-14");
    sqlx::query(
        "SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(&recv)
    .fetch_all(&p)
    .await
    .expect("fill ring");

    // Sanity: overflow_active still 0 (no overflow yet).
    let pre: Option<(bool, i64)> = sqlx::query(
        "SELECT overflow_active, watermark FROM fifo_overflow_state($1::bigint)",
    )
    .bind(pool_id)
    .fetch_optional(&p)
    .await
    .expect("pre overflow")
    .map(|r| {
        (
            r.get::<bool, _>("overflow_active"),
            r.get::<i64, _>("watermark"),
        )
    });
    assert_eq!(
        pre,
        Some((false, 0)),
        "V9 setup: cell present, overflow_active still 0 (got {pre:?})"
    );

    // Rolled-back receipt that should trip overflow.
    let mut tx = p.begin().await.expect("begin");
    let extra = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_receipt",
            "debit_account_id": pool_id, "credit_account_id": ap_id,
            "qty": 10, "unit_cost": 1500,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    sqlx::query(
        "SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(&extra)
    .fetch_all(&mut *tx)
    .await
    .expect("apply overflow-causing receipt");
    tx.rollback().await.expect("rollback");

    // Post-A2: overflow_active stayed at 0 (shadow discarded).
    let post: Option<(bool, i64)> = sqlx::query(
        "SELECT overflow_active, watermark FROM fifo_overflow_state($1::bigint)",
    )
    .bind(pool_id)
    .fetch_optional(&p)
    .await
    .expect("post overflow")
    .map(|r| {
        (
            r.get::<bool, _>("overflow_active"),
            r.get::<i64, _>("watermark"),
        )
    });
    assert_eq!(
        post,
        Some((false, 0)),
        "V9 A2: overflow_active stays 0 post-rollback (got {post:?}). \
         Pre-A2 this would have been (true, lid) because Phase 8 \
         mutated the real cell at apply time."
    );

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// V10 — mixed cell-hosted + durable_only partial rollback.
///
/// Saturate the arena except for one pre-warmed cell. The pre-warmed
/// pool (cell-hosted) and a fresh pool (no cell — Phase 8.6
/// durable_only) get rolled-back receipts in the same txn. Assert:
/// - Cell-hosted pool's shmem_live=0 (shadow discarded).
/// - Durable_only pool has no cost_layers row + no recon row.
///
/// Destructive (uses fifo_force_arena_full); `#[ignore]`'d.
#[tokio::test]
#[ignore]
async fn v10_mixed_cell_hosted_and_durable_only_partial_rollback() {
    let p = pool().await;
    let (cell_pool, ap_id, cogs_id) = unique_accounts();
    let (durable_pool, _, _) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (cell_pool, "fifo_pool_a", "inv_value_raw"),
            (durable_pool, "fifo_pool_b", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    // Pre-warm the cell-hosted pool with a committed receipt.
    let warm = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_receipt",
            "debit_account_id": cell_pool, "credit_account_id": ap_id,
            "qty": 50, "unit_cost": 1000,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    sqlx::query(
        "SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(&warm)
    .fetch_all(&p)
    .await
    .expect("warm cell");

    // Saturate arena so durable_only's pool gets no cell.
    let marked: i64 = sqlx::query("SELECT fifo_force_arena_full() AS n")
        .fetch_one(&p)
        .await
        .expect("fill")
        .get::<i64, _>("n");
    assert!(marked > 0);

    // Rolled-back receipts: one to cell-hosted, one to durable_only.
    let mut tx = p.begin().await.expect("begin");
    let mixed = serde_json::json!([
        {
            "envelope_idx": 0, "kind": "fifo_receipt",
            "debit_account_id": cell_pool, "credit_account_id": ap_id,
            "qty": 30, "unit_cost": 1100,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        },
        {
            "envelope_idx": 1, "kind": "fifo_receipt",
            "debit_account_id": durable_pool, "credit_account_id": ap_id,
            "qty": 25, "unit_cost": 1200,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-14",
        }
    ]);
    sqlx::query(
        "SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(&mixed)
    .fetch_all(&mut *tx)
    .await
    .expect("mixed apply");
    tx.rollback().await.expect("rollback");

    // Cell-hosted pool: shadow discarded; baseline 50 preserved.
    let post_cell = recon_row(&p, cell_pool).await.expect("cell pool");
    assert_eq!(post_cell.0, 50, "V10: cell-hosted shmem_live=50 (rolled-back receipt discarded)");
    assert_eq!(post_cell.3, Some(50), "V10: cell-hosted durable=50");
    assert_eq!(post_cell.4, Some(0), "V10: cell-hosted drift=0");

    // Durable-only pool: no cell, no cost_layers row.
    let dur_cl: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM cost_layers WHERE pool_account_id = $1::bigint",
    )
    .bind(durable_pool)
    .fetch_one(&p)
    .await
    .expect("durable cl");
    assert_eq!(dur_cl, 0, "V10: durable_only pool has no cost_layers after rollback");

    let post_dur = recon_row(&p, durable_pool).await;
    assert!(
        post_dur.is_none(),
        "V10: durable_only pool has no recon row (no cell allocated). Got {post_dur:?}"
    );

    let cleared: i64 = sqlx::query("SELECT fifo_release_sentinels() AS n")
        .fetch_one(&p)
        .await
        .expect("release")
        .get::<i64, _>("n");
    assert!(cleared > 0);

    cleanup(&p, &[cell_pool, durable_pool, ap_id, cogs_id]).await;
}
