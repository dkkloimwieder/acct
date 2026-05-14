//! acct-a3rj — FIFO concurrent over-consume gap (R-MB6).
//!
//! ## Scope
//!
//! Documents the concurrent over-consume gap in A2 (acct-b3vs, shipped at
//! commit `dff20bd`) at `poc/ledger-extension/src/fifo.rs:1175-1179`:
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
//!   dropped → drift surfaces.
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
//! "Thin-layer concurrent exhaustion" is the missing shape.
//!
//! ## Polarity
//!
//! R-MB6 documents CURRENT A2 behavior:
//!
//! - Both txns commit successfully (no SQL error during commit).
//! - `cost_layer_depletions` has 2 rows for L1, each `qty_consumed=N`,
//!   summing to `2N`.
//! - The SUM exceeds the layer's original qty `N` — over-consume is
//!   recorded but undetected by any current check.
//!
//! When acct-a3rj Phase B lands (add `cost_layers.qty_received`
//! column populated at INSERT + new recon check `SUM(depletions) <=
//! qty_received`), assertion polarities flip: depletion SUM > received
//! must be surfaced by the recon function rather than silently
//! over-counting.
//!
//! Pattern matches t5's R-CR2 "documents the gap" shape — silent-
//! failure profile pinned now, regression test once detection ships.
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
/// `L1{qty_remaining=100, original=100}`.
///
/// Workload: two backends, each opens a tx, applies a `fifo_issue
/// qty=100`, then commits. Tasks synchronize on a barrier so both apply
/// phases complete before either commits — each backend's shadow
/// snapshots the same pre-commit ring state (L1=100). Both apply
/// phases insert `cost_layer_depletions` rows for `{layer=L1, qty=100}`.
///
/// Current (gap) behavior assertions (these document the gap; flip
/// polarity when Phase B's recon check lands):
///
/// 1. Both txns commit without error.
/// 2. `SUM(cost_layer_depletions.qty_consumed for L1) = 200` —
///    over-consume recorded.
/// 3. `SUM > L1.original_qty=100` — the gap signal.
/// 4. After best-effort `force_drain`:
///    - `cost_layers.qty_remaining for L1` either stays at 100
///      (drain batch hit the CHECK constraint, was rolled back +
///      logged) OR partially advances (drain decremented some but
///      not all entries). Either way it never goes negative.
///    - No existing recon check fires on the
///      `SUM(depletions) > original_qty` invariant violation. The
///      `fifo_arena_recon().drift` may or may not surface a non-
///      zero value depending on whether pending_drain entries are
///      still in shmem.
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

    // Invariant 3: the gap signal. Over-consume is recorded but the
    // current schema has no `cost_layers.qty_received` column to
    // compare against; no existing recon check surfaces this.
    let l1_original_qty: i64 = 100;
    assert!(
        post_deps_sum > l1_original_qty,
        "R-MB6 GAP SIGNAL: SUM(depletions)={post_deps_sum} > original_qty={l1_original_qty}. \
         A2 records the over-consume in cost_layer_depletions but no recon \
         check fires on this invariant (acct-a3rj Phase B will add one)."
    );

    // Best-effort drain. May fail internally on the CHECK
    // qty_remaining >= 0; not asserted either way — observed state
    // documented below.
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

    // GAP DOCUMENTATION: print the state so it shows in --nocapture.
    eprintln!(
        "R-MB6 gap state: L1 original=100, SUM(depletions)={post_deps_sum}, \
         qty_remaining={post_durable}, recon=({:?})",
        recon
    );

    cleanup(&admin, &[pool_id, ap_id, cogs_id]).await;
}
