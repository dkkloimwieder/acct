//! acct-a3rj — FIFO concurrent over-consume regression net (R-MB6).
//!
//! ## Scope
//!
//! Regression test for the concurrent over-consume condition in A2
//! (acct-b3vs, shipped at commit `dff20bd`) at
//! `poc/ledger-extension/src/fifo.rs:1175-1179`:
//!
//! ```text
//! LayerOp::Consume { qty } => {
//!     // Results discarded — depletion records were written at
//!     // apply time against shadow-time slice identities.
//!     let _ = consume_from_head(ring, *qty);
//! }
//! ```
//!
//! At apply time, each backend stages its own `CellShadow` from a
//! pre-snapshot of the ring and inserts `cost_layer_depletions` rows
//! against shadow-time slice identities (Phase 9d at fifo.rs:2287). On
//! `xact_commit` the staged `LayerOp::Consume` replays
//! `consume_from_head` for ring-state side-effect only; the result
//! (slices consumed + shortage) is discarded.
//!
//! Under concurrent issues exhausting the SAME thin layer:
//!
//! - Backend A snapshots L1 with `qty_remaining=N`, stages
//!   `Consume(N)` + `PendingDrainPush(L1, N)`, inserts a depletion row
//!   for `{layer=L1, qty=N}`.
//! - Backend B snapshots L1 with `qty_remaining=N` (its shadow is
//!   thread-local; A's stage is invisible). Stages `Consume(N)` +
//!   `PendingDrainPush(L1, N)`. Inserts a depletion row for
//!   `{layer=L1, qty=N}`.
//! - Both commit. Ring replay: A drains L1 to 0 + pushes pending
//!   entry for N. B's `consume_from_head` finds empty ring, returns
//!   `shortage=N` (discarded), B's `PendingDrainPush` adds a second
//!   pending entry for N.
//! - bgworker drain accumulates pending entries for L1 (`SUM=2N`)
//!   and issues `UPDATE cost_layers SET qty_remaining = qty_remaining
//!   - 2N WHERE id = L1`. With `qty_remaining=N` and CHECK
//!   `qty_remaining >= 0`, this fails. Drain entries are logged then
//!   dropped.
//!
//! The depletion-side signal is unambiguous: `SUM(cost_layer_depletions
//! .qty_consumed for L1) = 2N > N = L1.original_qty`.
//!
//! ## Why existing tests miss this
//!
//! - `t2::r_mb3_four_backend_concurrent_issues_all_commit` pre-seeds a
//!   1000-qty baseline and issues 4×100 — never exhausts.
//! - `bench_fifo_*` pre-seeds 5×1M per pool — never exhausts under
//!   load.
//! - `t4::r_sp3_mixed` is single-backend; no concurrent-against-same-
//!   layer pattern.
//! - `property_fifo_rollback` pre-seeds 10M baseline.
//!
//! "Thin-layer concurrent exhaustion" is the shape they were missing.
//!
//! ## Detection
//!
//! Phase B (commit `fdad1c3`) added `cost_layers.qty_received` (the
//! immutable receipt-side anchor populated at INSERT and never
//! decremented) plus `fifo_overconsume_check()`, a pure-SQL function
//! that returns one row per layer where `SUM(qty_consumed) >
//! qty_received` with overshoot magnitude. R-MB6 exercises the
//! concurrent-exhaustion shape end-to-end and asserts:
//!
//! - Both txns commit (no SQL error during commit).
//! - `cost_layer_depletions` has 2 rows for L1 summing to `2N` (the
//!   recorded over-attribution).
//! - `cost_layers.qty_received for L1 = N` (the immutable anchor).
//! - `fifo_overconsume_check()` returns one row identifying L1 with
//!   `overshoot = N`.
//!
//! If a future rewrite of the FIFO arena (Approach E in-place +
//! needs_repair, Approach B OCC PreCommit validation, etc.) eliminates
//! the over-consume at the source rather than detecting it post-hoc,
//! R-MB6 flips: the `2 depletion rows for L1` and
//! `fifo_overconsume_check returns one row` assertions need updating
//! to reflect "one txn commits, the other gets a serialization
//! failure" or whichever shape the new approach exhibits.
//!
//! Companion to t5's R-CR2 (un-drained pending_drain loss profile) —
//! both pin a real correctness condition that A2 records but cannot
//! itself surface; this one surfaced via the schema + recon extension
//! in Phase B, R-CR2 still awaits a recovery-path fix.
//!
//! Base prefix `6_900_000_000_000` (disjoint from t1 4.5e12 / t2 4.9e12
//! / t3 5.3e12 / t4 5.7e12 / t5 6.1e12 / property 6.5e12).

use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn db_url() -> String {
    std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
}

fn unique_accounts() -> (i64, i64, i64) {
    let u = Uuid::new_v4();
    let bytes = u.as_bytes();
    let base = 6_900_000_000_000_i64
        + ((bytes[0] as i64) << 24
            | (bytes[1] as i64) << 16
            | (bytes[2] as i64) << 8
            | (bytes[3] as i64))
            .abs()
            % 100_000_000;
    (base, base + 1, base + 2)
}

async fn backend_pool() -> sqlx::PgPool {
    let p = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(15))
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&p)
        .await
        .expect("ext");
    p
}

async fn admin_pool() -> sqlx::PgPool {
    let p = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(15))
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

async fn force_drain(p: &sqlx::PgPool) {
    // Drain may FAIL on the over-consume scenario when bgworker tries to
    // UPDATE cost_layers SET qty_remaining = qty_remaining - SUM(pending)
    // and hits the CHECK constraint. Best-effort — swallow errors and
    // let the assertions below characterize the resulting state.
    let _ = sqlx::query("SELECT fifo_force_drain_tick()").execute(p).await;
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

async fn durable_qty(p: &sqlx::PgPool, pool_id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(SUM(qty_remaining), 0)::bigint FROM cost_layers \
         WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_one(p)
    .await
    .expect("durable qty")
}

async fn layer_id_for_pool(p: &sqlx::PgPool, pool_id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT id FROM cost_layers WHERE pool_account_id = $1::bigint ORDER BY id ASC LIMIT 1",
    )
    .bind(pool_id)
    .fetch_one(p)
    .await
    .expect("layer id")
}

async fn depletion_sum_for_layer(p: &sqlx::PgPool, layer_id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(SUM(qty_consumed), 0)::bigint FROM cost_layer_depletions \
         WHERE layer_id = $1::bigint",
    )
    .bind(layer_id)
    .fetch_one(p)
    .await
    .expect("depletion sum")
}

async fn depletion_count_for_layer(p: &sqlx::PgPool, layer_id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM cost_layer_depletions WHERE layer_id = $1::bigint",
    )
    .bind(layer_id)
    .fetch_one(p)
    .await
    .expect("depletion count")
}

async fn qty_received_for_layer(p: &sqlx::PgPool, layer_id: i64) -> i64 {
    sqlx::query_scalar("SELECT qty_received FROM cost_layers WHERE id = $1::bigint")
        .bind(layer_id)
        .fetch_one(p)
        .await
        .expect("qty_received")
}

/// One row per layer where SUM(depletions) > qty_received. Returns
/// `(layer_id, pool_account_id, qty_received, total_consumed, overshoot)`.
async fn overconsume_rows(p: &sqlx::PgPool) -> Vec<(i64, i64, i64, i64, i64)> {
    sqlx::query(
        "SELECT layer_id, pool_account_id, qty_received, total_consumed, overshoot \
         FROM fifo_overconsume_check() ORDER BY layer_id",
    )
    .fetch_all(p)
    .await
    .expect("fifo_overconsume_check")
    .into_iter()
    .map(|r| {
        (
            r.get::<i64, _>("layer_id"),
            r.get::<i64, _>("pool_account_id"),
            r.get::<i64, _>("qty_received"),
            r.get::<i64, _>("total_consumed"),
            r.get::<i64, _>("overshoot"),
        )
    })
    .collect()
}

fn receipt_envelope(pool_id: i64, ap_id: i64, qty: i64, unit_cost: i64) -> serde_json::Value {
    serde_json::json!([{
        "envelope_idx": 0,
        "kind": "fifo_receipt",
        "debit_account_id": pool_id,
        "credit_account_id": ap_id,
        "qty": qty,
        "unit_cost": unit_cost,
        "idempotency_key": Uuid::new_v4().to_string(),
        "business_date": "2026-05-14",
    }])
}

fn issue_envelope(pool_id: i64, cogs_id: i64, qty: i64) -> serde_json::Value {
    serde_json::json!([{
        "envelope_idx": 0,
        "kind": "fifo_issue",
        "debit_account_id": cogs_id,
        "credit_account_id": pool_id,
        "qty": qty,
        "idempotency_key": Uuid::new_v4().to_string(),
        "business_date": "2026-05-14",
    }])
}

async fn apply_committed(p: &sqlx::PgPool, envelopes: &serde_json::Value) {
    sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
        .bind(envelopes)
        .fetch_all(p)
        .await
        .expect("apply committed");
}

// ─── R-MB6 ─────────────────────────────────────────────────────────────

/// **R-MB6** — Concurrent over-consume against a single thin layer.
///
/// Setup: receive qty=100, force_drain → one durable `cost_layers` row
/// `L1{qty_remaining=100, qty_received=100}`.
///
/// Workload: two backends, each opens a tx, applies a `fifo_issue
/// qty=100`, then commits. Tasks synchronize on a barrier so both apply
/// phases complete before either commits — each backend's shadow
/// snapshots the same pre-commit ring state (L1=100). Both apply
/// phases insert `cost_layer_depletions` rows for `{layer=L1, qty=100}`.
///
/// Asserted invariants (the regression net for the over-consume
/// condition + Phase B detection):
///
/// 1. Both txns commit without error.
/// 2. `cost_layer_depletions` has exactly two rows for L1.
/// 3. `SUM(qty_consumed for L1) = 200`.
/// 4. `cost_layers.qty_received for L1 = 100` — the immutable
///    receipt-side anchor (Phase B).
/// 5. `fifo_overconsume_check()` returns one row for L1 with
///    `qty_received=100, total_consumed=200, overshoot=100` (Phase B).
/// 6. After best-effort `force_drain`, `cost_layers.qty_remaining for
///    L1` stays in `[0, 100]` (CHECK constraint floors the
///    decrement). `fifo_arena_recon().drift` may surface non-zero
///    pending-vs-durable lag — that's a separate signal from the
///    over-consume invariant.
///
/// If a future FIFO-arena rewrite eliminates the over-consume at the
/// source, assertions 2 + 3 + 5 must be updated (one txn commits, the
/// other gets serialization failure / pre-commit validation error).
#[tokio::test]
async fn r_mb6_concurrent_exhaustion_over_consumes_thin_layer() {
    let admin = admin_pool().await;
    let a = backend_pool().await;
    let b = backend_pool().await;
    let (pool_id, ap_id, cogs_id) = unique_accounts();
    seed_accounts(
        &admin,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    // Baseline: thin layer L1 = qty 100, fully drained to durable so
    // both backends snapshot the same pre-commit state.
    apply_committed(&admin, &receipt_envelope(pool_id, ap_id, 100, 1000)).await;
    force_drain(&admin).await;
    let pre_durable = durable_qty(&admin, pool_id).await;
    assert_eq!(pre_durable, 100, "R-MB6 setup: durable L1 = 100");
    let l1_id = layer_id_for_pool(&admin, pool_id).await;
    let pre_deps = depletion_count_for_layer(&admin, l1_id).await;
    assert_eq!(pre_deps, 0, "R-MB6 setup: zero depletions before workload");

    // Coordination: both apply phases complete (and their depletion
    // INSERTs land) before either commits. Each backend's shadow has
    // independently snapshotted L1=100; neither sees the other's
    // staged Consume.
    let barrier_applied = Arc::new(Barrier::new(2));
    let barrier_done = Arc::new(Barrier::new(2));

    let pool_id_c = pool_id;
    let cogs_id_c = cogs_id;
    let ba1 = barrier_applied.clone();
    let bd1 = barrier_done.clone();
    let a_task = tokio::spawn(async move {
        let envs = issue_envelope(pool_id_c, cogs_id_c, 100);
        let mut tx = a.begin().await.expect("A begin");
        sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
            .bind(&envs)
            .fetch_all(&mut *tx)
            .await
            .expect("A apply");
        ba1.wait().await; // both applied — both depletion rows inserted
        tx.commit().await.expect("A commit");
        bd1.wait().await;
    });

    let pool_id_c2 = pool_id;
    let cogs_id_c2 = cogs_id;
    let ba2 = barrier_applied.clone();
    let bd2 = barrier_done.clone();
    let b_task = tokio::spawn(async move {
        let envs = issue_envelope(pool_id_c2, cogs_id_c2, 100);
        let mut tx = b.begin().await.expect("B begin");
        sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
            .bind(&envs)
            .fetch_all(&mut *tx)
            .await
            .expect("B apply");
        ba2.wait().await; // both applied
        tx.commit().await.expect("B commit");
        bd2.wait().await;
    });

    a_task.await.expect("A task");
    b_task.await.expect("B task");

    // Invariant 1: BOTH committed (already asserted via expect() above).
    // Invariant 2: depletion sum for L1 is 200 — both apply phases
    // inserted a row against L1 from their independent shadow snapshots.
    let post_deps_count = depletion_count_for_layer(&admin, l1_id).await;
    assert_eq!(
        post_deps_count, 2,
        "R-MB6: exactly two depletion rows for L1 (one per backend); \
         both inserts succeeded against shadow-time slice identity. \
         Got {post_deps_count}"
    );
    let post_deps_sum = depletion_sum_for_layer(&admin, l1_id).await;
    assert_eq!(
        post_deps_sum, 200,
        "R-MB6: SUM(cost_layer_depletions.qty_consumed for L1) = 200; \
         L1's original qty was 100. Over-consume recorded. Got {post_deps_sum}"
    );

    // Invariant 3: depletion sum exceeds the layer's original qty.
    // Pinning this here keeps R-MB6 honest about what the workload
    // produces; Phase B's `fifo_overconsume_check` (invariant 6 below)
    // is the canonical detection.
    let l1_original_qty: i64 = 100;
    assert!(
        post_deps_sum > l1_original_qty,
        "R-MB6: SUM(depletions)={post_deps_sum} > original_qty={l1_original_qty}. \
         A2 records the over-consume in cost_layer_depletions; \
         fifo_overconsume_check (invariant 6) detects it."
    );

    // Best-effort drain. May fail internally on the CHECK
    // qty_remaining >= 0; not asserted either way — bounded by
    // invariant 4 below.
    force_drain(&admin).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    force_drain(&admin).await;

    // Invariant 4a: `cost_layers.qty_remaining` for L1 never goes
    // negative — the CHECK constraint guarantees this. Either 0
    // (one pending entry drained, second one blocked) or 100 (whole
    // batch blocked).
    let post_durable = durable_qty(&admin, pool_id).await;
    assert!(
        post_durable >= 0,
        "R-MB6: qty_remaining CHECK guarantees non-negative; got {post_durable}"
    );
    assert!(
        post_durable <= 100,
        "R-MB6: qty_remaining for L1 cannot exceed its original 100; got {post_durable}"
    );

    // Invariant 4b: recon row exists and reports its own view. We
    // don't assert a specific drift value because the drain failure
    // mode (whole-batch vs per-entry) determines whether pending
    // entries are still in shmem at observation time. The point of
    // R-MB6 is: SUM(depletions) > original is the canonical truth
    // and no current view surfaces it.
    let recon = recon_row(&admin, pool_id).await.expect("R-MB6 cell present");
    let (_live, _pending, _total, durable, _drift) = recon;
    assert_eq!(
        durable,
        Some(post_durable),
        "R-MB6: recon.durable_qty matches direct SUM(cost_layers.qty_remaining)"
    );

    // Invariant 5 (Phase B): the new `qty_received` column anchors the
    // immutable receipt-side value. With L1 received at qty=100 it is
    // 100 and not decremented by depletions.
    let l1_received = qty_received_for_layer(&admin, l1_id).await;
    assert_eq!(
        l1_received, 100,
        "R-MB6 Phase B: cost_layers.qty_received for L1 = 100 (immutable receipt-side anchor)"
    );

    // Invariant 6 (Phase B): `fifo_overconsume_check()` detects the
    // over-attribution. Without this function the over-consume was
    // invisible — `qty_remaining` non-negativity hides it (drain
    // failures get logged + dropped) and `fifo_arena_recon.drift`
    // measures pending-vs-durable lag, not depletion-vs-received.
    let oc = overconsume_rows(&admin).await;
    let oc_for_l1: Vec<_> = oc.iter().filter(|(lid, _, _, _, _)| *lid == l1_id).collect();
    assert_eq!(
        oc_for_l1.len(),
        1,
        "R-MB6 Phase B: fifo_overconsume_check fires exactly once for L1; got {} rows. \
         Detection signal is the canonical truth, not derivative of drain success.",
        oc_for_l1.len()
    );
    let (_, oc_pool, oc_received, oc_consumed, oc_overshoot) = oc_for_l1[0];
    assert_eq!(*oc_pool, pool_id, "R-MB6 Phase B: pool_account_id surfaced");
    assert_eq!(*oc_received, 100, "R-MB6 Phase B: qty_received reported = 100");
    assert_eq!(*oc_consumed, 200, "R-MB6 Phase B: total_consumed reported = 200");
    assert_eq!(*oc_overshoot, 100, "R-MB6 Phase B: overshoot = 100 (200 - 100)");

    // Diagnostic: print final state under --nocapture for at-a-glance
    // confirmation that the over-consume signal looks right.
    eprintln!(
        "R-MB6 state: L1 received=100, SUM(depletions)={post_deps_sum}, \
         qty_remaining={post_durable}, recon=({:?}), overconsume_rows={:?}",
        recon, oc
    );

    cleanup(&admin, &[pool_id, ap_id, cogs_id]).await;
}
