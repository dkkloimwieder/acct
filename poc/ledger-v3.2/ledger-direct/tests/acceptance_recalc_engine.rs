//! Acceptance tests for `ledger_recalc_step` — the recalc engine's worker tick
//! (design-v3.2 §5; recalc-a/b/c/d).
//!
//! Cases pin: the strict FIFO/LIFO costing + consumption linkage + GL delta
//! routing; generation-delta idempotency (D6: no-op passes don't bump); R-2
//! backdated re-cost via the feed's recost floor (oracle §1b vignette A); the
//! feed loopback being a free no-op; incremental-from-checkpoint vs
//! full-opening replay; the uncovered (ill-defined) depletion costing at the
//! observed cost; non-strict pools draining as no-ops; claim exclusivity under
//! concurrent workers; and whole-pass transactionality (rollback = clean
//! retry).

mod common;

use common::*;
use serde_json::{json, Value};

/// FIFO full replay: 10@100 (T09) + 10@300 (T10), deplete 15 (T11).
/// Draws 10@100 + 5@300 → authoritative banker_div(2500, 15) = 167; the
/// observed provisional was the running average 200, so the delta is
/// (167−200)×15 = −495 — value handed BACK to inventory (receipt direction).
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn fifo_full_replay_costs_links_and_posts() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;

    let r1 = receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    let r2 = receipt_at(&pool, f.pool_id, 2, T10, 10, 300).await;
    let d = deplete_at(&pool, f.pool_id, 3, T11, 15).await;

    mark_dirty(&pool, f.pool_id).await;
    let report = recalc_step(&pool).await;
    assert_eq!(report["claimed"], json!(true));
    assert_eq!(report["method"], json!("fifo"));
    assert_eq!(report["full_replay"], json!(true));
    assert_eq!(report["events"], json!(3));
    assert_eq!(report["settlements_written"], json!(1));
    assert_eq!(report["adjustments_posted"], json!(1));
    assert_eq!(report["generation"], json!(1));
    assert_eq!(report["requeued"], json!(false));

    assert_eq!(authoritative_of(&pool, d).await, Some(167));
    let draws = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT layer_trx_line_id, qty, unit_cost FROM cost_layer_consumption \
          WHERE depletion_trx_line_id = $1 AND recalc_generation = 1 \
          ORDER BY layer_trx_line_id",
    )
    .bind(d)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(draws, vec![(r1, 10, 100), (r2, 5, 300)]);

    // GL: one cost_adjustment posting, negative delta → receipt direction
    // (debit inventory, credit the adjustment contra), |delta| = 495. The
    // adjustment line points at its depletion and carries the authoritative
    // cost.
    let gl = sqlx::query_as::<_, (String, i64, i64, i64, i64, i64)>(
        "SELECT pl.event_type::text, pl.amount, pl.debit_account, pl.credit_account, \
                tl.source_trx_line_id, tl.unit_cost \
           FROM posting_line pl JOIN trx_line tl ON tl.id = pl.trx_line_id \
          WHERE tl.line_type = 'cost_adjustment_line'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(gl, vec![("cost_adjustment".into(), 495, INV_ACCT, ADJ_ACCT, d, 167)]);
    assert_eq!(
        count(&pool, "SELECT count(*) FROM trx WHERE trx_type = 'cost_adjustment'").await,
        1
    );

    // Materialization: the surviving layer is receipt 2's remainder, and the
    // aggregate value_sum moved from the provisional net (1000) to the
    // authoritative net (1495 = Σ receipts − 15×167; the 5 vs open-layer value
    // 1500 is banker residue the close sweep owns).
    let layers = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT layer_id, qty, unit_cost FROM pool_state WHERE pool_id = $1 AND layer_id > 0",
    )
    .bind(f.pool_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(layers, vec![(r2, 5, 300)]);
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((5, 299, 1495)));

    assert_eq!(settlement_of(&pool, f.pool_id).await, Some((1, Some(d), false)));
    assert_eq!(count(&pool, "SELECT count(*) FROM recalc_queue").await, 0);
}

/// LIFO consumes newest first: same events, draws 10@300 + 5@100 →
/// banker_div(3500, 15) = 233; delta (233−200)×15 = +495 posts in the
/// depletion direction.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn lifo_full_replay_consumes_newest_first() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "lifo", "running_avg").await;

    let r1 = receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    let r2 = receipt_at(&pool, f.pool_id, 2, T10, 10, 300).await;
    let d = deplete_at(&pool, f.pool_id, 3, T11, 15).await;

    mark_dirty(&pool, f.pool_id).await;
    recalc_step(&pool).await;

    assert_eq!(authoritative_of(&pool, d).await, Some(233));
    let draws = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT layer_trx_line_id, qty, unit_cost FROM cost_layer_consumption \
          WHERE depletion_trx_line_id = $1 ORDER BY layer_trx_line_id",
    )
    .bind(d)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(draws, vec![(r1, 5, 100), (r2, 10, 300)]);

    let gl = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT amount, debit_account, credit_account FROM posting_line \
          WHERE event_type = 'cost_adjustment'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(gl, vec![(495, ADJ_ACCT, INV_ACCT)], "positive delta posts depletion-direction");

    let layers = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT layer_id, qty, unit_cost FROM pool_state WHERE pool_id = $1 AND layer_id > 0",
    )
    .bind(f.pool_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(layers, vec![(r1, 5, 100)]);
}

/// A depletion whose authoritative cost equals its observed cost writes the
/// settlement record with NO GL (adjustment_posting_line_id NULL, no trx), and
/// a re-run pass writes nothing at all and does not bump the generation (D6).
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn zero_delta_settles_without_gl_and_rerun_is_free() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;

    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    let d = deplete_at(&pool, f.pool_id, 2, T10, 5).await;

    mark_dirty(&pool, f.pool_id).await;
    let report = recalc_step(&pool).await;
    assert_eq!(report["settlements_written"], json!(1));
    assert_eq!(report["adjustments_posted"], json!(0));
    assert_eq!(report["generation"], json!(1));

    let (auth, prior, adj): (i64, i64, Option<i64>) = sqlx::query_as(
        "SELECT authoritative_unit_cost, prior_unit_cost, adjustment_posting_line_id \
           FROM cost_settlement WHERE depletion_trx_line_id = $1",
    )
    .bind(d)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((auth, prior, adj), (100, 100, None));
    assert_eq!(count(&pool, "SELECT count(*) FROM posting_line").await, 0);
    assert_eq!(count(&pool, "SELECT count(*) FROM trx WHERE trx_type = 'cost_adjustment'").await, 0);

    // Idempotent re-run: same stream, no new generation, nothing written.
    mark_dirty(&pool, f.pool_id).await;
    let rerun = recalc_step(&pool).await;
    assert_eq!(rerun["claimed"], json!(true));
    assert_eq!(rerun["full_replay"], json!(false));
    assert_eq!(rerun["events"], json!(0));
    assert_eq!(rerun["settlements_written"], json!(0));
    assert_eq!(rerun["generation"], json!(1), "no-op pass does not bump the generation");
    assert_eq!(count(&pool, "SELECT count(*) FROM cost_settlement").await, 1);
    assert_eq!(count(&pool, "SELECT count(*) FROM recalc_queue").await, 0);
}

/// Oracle §1b vignette A through the whole loop: settle at gen 1, then a
/// backdated cheaper receipt arrives via the feed, lowers the recost floor,
/// and the next pass full-replays to the authoritative 100 — posting exactly
/// the inter-generation delta and keeping both generations for audit.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn backdated_receipt_lowers_floor_and_recosts() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let consumer = reset_feed(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;

    receipt_at(&pool, f.pool_id, 1, T10, 200, 300).await;
    let d = deplete_at(&pool, f.pool_id, 2, T11, 100).await;
    ingest_all(&consumer).await;
    let first = drain_recalc(&pool).await;
    assert_eq!(first.len(), 1);
    assert_eq!(authoritative_of(&pool, d).await, Some(300));

    // The backdated cheap lot: business-dated BEFORE everything already
    // settled, committed after.
    let rb = receipt_at(&pool, f.pool_id, 3, T09, 200, 100).await;
    let report = consumer.ingest_once(10_000).await.unwrap();
    assert_eq!(report.floors_lowered, 1, "backdated event lowers the recost floor");
    assert_eq!(report.pools_marked, 1);

    let recost = recalc_step(&pool).await;
    assert_eq!(recost["full_replay"], json!(true));
    assert_eq!(recost["generation"], json!(2));
    assert_eq!(recost["adjustments_posted"], json!(1));
    assert_eq!(recost["floor_pending"], json!(false));

    assert_eq!(authoritative_of(&pool, d).await, Some(100), "oracle vignette A: 100, not 300");
    let gens: Vec<(i64, i64, i64)> = sqlx::query_as(
        "SELECT recalc_generation, authoritative_unit_cost, prior_unit_cost \
           FROM cost_settlement WHERE depletion_trx_line_id = $1 ORDER BY recalc_generation",
    )
    .bind(d)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(gens, vec![(1, 300, 300), (2, 100, 300)], "append-only re-cost history");

    // Delta = (100−300)×100 = −20000 → receipt direction.
    let gl: (i64, i64, i64) = sqlx::query_as(
        "SELECT amount, debit_account, credit_account FROM posting_line \
          WHERE event_type = 'cost_adjustment'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(gl, (20_000, INV_ACCT, ADJ_ACCT));

    // Layers after the authoritative replay: cheap remainder + untouched base.
    let layers = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT layer_id, qty, unit_cost FROM pool_state \
          WHERE pool_id = $1 AND layer_id > 0 ORDER BY layer_id",
    )
    .bind(f.pool_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(layers.contains(&(rb, 100, 100)) && layers.len() == 2);

    // Loopback: the adjustment line the pass just posted flows back through
    // the feed, re-marks the pool, and the next pass is a free no-op — no
    // floor (posted_at = now() sorts ahead of the frontier), no generation
    // bump. The G2 gauge never counts the engine's own output.
    let loopback = consumer.ingest_once(10_000).await.unwrap();
    assert!(loopback.inserts >= 1, "adjustment line delivered by the slot");
    assert_eq!(loopback.floors_lowered, 0);
    let noop = recalc_step(&pool).await;
    assert_eq!(noop["claimed"], json!(true));
    assert_eq!(noop["generation"], json!(2));
    assert_eq!(noop["settlements_written"], json!(0));
    let unsettled: i64 = sqlx::query_scalar(
        "SELECT unsettled_events FROM recalc_pool_lag WHERE pool_id = $1",
    )
    .bind(f.pool_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(unsettled, 0, "gauge counts physical events only");

    drop_feed_slot(&pool).await;
}

/// Incremental steady state: after a settle, only the tail replays (from the
/// persisted layer checkpoint), and earlier settlements stay untouched.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn incremental_pass_replays_only_the_tail() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;

    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    let d1 = deplete_at(&pool, f.pool_id, 2, T10, 4).await;
    mark_dirty(&pool, f.pool_id).await;
    recalc_step(&pool).await;
    let gen1_rows = count(&pool, "SELECT count(*) FROM cost_settlement").await;

    // Tail: another receipt + depletion. FIFO: d2 takes the 6 left @100 then
    // 4 @900 → banker_div(600 + 3600, 10) = 420.
    receipt_at(&pool, f.pool_id, 3, T11, 10, 900).await;
    let d2 = deplete_at(&pool, f.pool_id, 4, T12, 10).await;
    mark_dirty(&pool, f.pool_id).await;
    let report = recalc_step(&pool).await;
    assert_eq!(report["full_replay"], json!(false));
    assert_eq!(report["events"], json!(2), "only the tail is scanned");
    assert_eq!(report["generation"], json!(2));

    assert_eq!(authoritative_of(&pool, d2).await, Some(420));
    let d1_rows = count(
        &pool,
        &format!("SELECT count(*) FROM cost_settlement WHERE depletion_trx_line_id = {d1}"),
    )
    .await;
    assert_eq!(d1_rows, 1, "prior settlement untouched by the incremental pass");
    assert_eq!(
        count(&pool, "SELECT count(*) FROM cost_settlement").await,
        gen1_rows + 1
    );
}

/// The ill-defined case: a depletion no layer covers is costed at its own
/// observed (recorded) cost — deterministic, no linkage, no GL — and the
/// frontier still advances past it.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn uncovered_depletion_costs_at_observed() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "standard").await;
    set_standard_cost(&pool, &f, 700).await;

    let d = deplete_at(&pool, f.pool_id, 1, T09, 32).await;
    mark_dirty(&pool, f.pool_id).await;
    let report = recalc_step(&pool).await;
    assert_eq!(report["settlements_written"], json!(1));
    assert_eq!(report["adjustments_posted"], json!(0));

    let (auth, prior, adj): (i64, i64, Option<i64>) = sqlx::query_as(
        "SELECT authoritative_unit_cost, prior_unit_cost, adjustment_posting_line_id \
           FROM cost_settlement WHERE depletion_trx_line_id = $1",
    )
    .bind(d)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((auth, prior, adj), (700, 700, None), "standard-basis observed cost stands");
    assert_eq!(count(&pool, "SELECT count(*) FROM cost_layer_consumption").await, 0);
    assert_eq!(settlement_of(&pool, f.pool_id).await, Some((1, Some(d), false)));
}

/// The feed is method-agnostic, so non-FIFO/LIFO pools land in the queue; the
/// engine drains them as free no-ops — no settlement row, no generation.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn non_strict_pool_drains_as_noop() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "wac", "running_avg").await;

    submit(&pool, "po_receipt", 1, json!([line(f.pool_id, "po_receipt_line", 10, 100)]))
        .await
        .unwrap();
    mark_dirty(&pool, f.pool_id).await;
    let report = recalc_step(&pool).await;
    assert_eq!(report["skipped"], json!("non_strict_method"));
    assert_eq!(count(&pool, "SELECT count(*) FROM recalc_queue").await, 0);
    assert_eq!(count(&pool, "SELECT count(*) FROM pool_settlement").await, 0);
}

/// R-1 within-date tiebreak: same posted_at everywhere, id decides — FIFO
/// consumes the first-inserted receipt.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn same_timestamp_uses_id_tiebreak() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;

    let r1 = receipt_at(&pool, f.pool_id, 1, T10, 10, 100).await;
    receipt_at(&pool, f.pool_id, 2, T10, 10, 300).await;
    let d = deplete_at(&pool, f.pool_id, 3, T10, 10).await;

    mark_dirty(&pool, f.pool_id).await;
    recalc_step(&pool).await;
    assert_eq!(authoritative_of(&pool, d).await, Some(100));
    let (layer, qty): (i64, i64) = sqlx::query_as(
        "SELECT layer_trx_line_id, qty FROM cost_layer_consumption \
          WHERE depletion_trx_line_id = $1",
    )
    .bind(d)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((layer, qty), (r1, 10));
}

/// Whole-pass transactionality: a rolled-back step leaves the queue row, the
/// floor, and every table untouched; the retry settles cleanly.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn rolled_back_pass_leaves_claim_reclaimable() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;

    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    let d = deplete_at(&pool, f.pool_id, 2, T10, 15).await;
    mark_dirty(&pool, f.pool_id).await;

    {
        let mut tx = pool.begin().await.unwrap();
        let report: Value = sqlx::query_scalar("SELECT ledger_recalc_step()")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(report["claimed"], json!(true));
        tx.rollback().await.unwrap();
    }

    assert_eq!(count(&pool, "SELECT count(*) FROM recalc_queue").await, 1);
    assert_eq!(count(&pool, "SELECT count(*) FROM cost_settlement").await, 0);
    assert_eq!(count(&pool, "SELECT count(*) FROM pool_settlement").await, 0);

    let retry = recalc_step(&pool).await;
    assert_eq!(retry["claimed"], json!(true));
    assert_eq!(retry["generation"], json!(1));
    assert!(authoritative_of(&pool, d).await.is_some());
    assert_eq!(count(&pool, "SELECT count(*) FROM recalc_queue").await, 0);
}

/// Claim exclusivity: concurrent workers never co-own a pool (SKIP LOCKED),
/// and the drained result over several pools equals what serial passes
/// produce — one settlement row per depletion, one generation per pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn concurrent_workers_never_double_settle() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    for pid in 1..=4i64 {
        let f = seed_pool(&pool, pid, pid, pid, if pid % 2 == 0 { "lifo" } else { "fifo" }, "running_avg")
            .await;
        receipt_at(&pool, f.pool_id, pid * 10 + 1, T09, 10, 100).await;
        receipt_at(&pool, f.pool_id, pid * 10 + 2, T10, 10, 300).await;
        deplete_at(&pool, f.pool_id, pid * 10 + 3, T11, 15).await;
        mark_dirty(&pool, f.pool_id).await;
    }

    // Two workers race the 4-pool queue to empty.
    let (a, b) = tokio::join!(
        async {
            let mut n = 0;
            while recalc_step(&pool).await["claimed"] == json!(true) {
                n += 1;
            }
            n
        },
        async {
            let mut n = 0;
            while recalc_step(&pool).await["claimed"] == json!(true) {
                n += 1;
            }
            n
        }
    );
    assert_eq!(a + b, 4, "each pool claimed exactly once across both workers");

    assert_eq!(count(&pool, "SELECT count(*) FROM recalc_queue").await, 0);
    assert_eq!(count(&pool, "SELECT count(*) FROM cost_settlement").await, 4);
    for pid in 1..=4i64 {
        assert_eq!(settlement_of(&pool, pid).await.map(|s| s.0), Some(1));
        let auth: i64 = sqlx::query_scalar(
            "SELECT cs.authoritative_unit_cost FROM cost_settlement cs \
               JOIN trx_line t ON t.id = cs.depletion_trx_line_id \
              WHERE t.pool_id = $1",
        )
        .bind(pid)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(auth, if pid % 2 == 0 { 233 } else { 167 });
    }
}


/// Flush-wiped correction: the FL exact-empty flush discards engine
/// corrections folded into the aggregate before it, and the id-order
/// (apply-order) fold with per-adjustment reconciles reconciles value_sum
/// exactly through that wipe — the telescoped-deltas identity cannot.
///
/// Choreography (single fifo pool, deep-negative + backdated shape):
/// 1. gen-1 settles the T13 deplete-29 against all three T11 receipts —
///    authoritative 72 vs observed 279 — and folds the +6003 correction into
///    value_sum (5913 → 11916, derived average 126 → 254);
/// 2. a backdated T10 deplete-47 lands the aggregate qty at exactly 0: the
///    residue flush zeroes value_sum, discarding that folded correction (its
///    own recorded cost is the corrected average 254);
/// 3. the backdated delivery floors the pool; the full-replay re-cost keeps
///    auth 72 for the deplete-29 (no gen-2 row) and moves only the T13
///    deplete-47 (254 → 253, reconcile +47).
/// Quiesced value_sum is 6003 short of `commit-order fold − Σ (auth_final −
/// observed) × qty` — the wiped correction, unrecoverable from the physical
/// stream alone (verify.rs V2 skips these pools). The id-order fold over ALL
/// lines (physical + cost_adjustment_line reconciles) reproduces it exactly.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn flush_wiped_correction_reconciles_in_apply_order() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let consumer = reset_feed(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    let pid = f.pool_id;

    receipt_at(&pool, pid, 1000, T11, 4, 279).await;
    consumer.ingest_once(10_000).await.expect("ingest");
    consumer.ingest_once(10_000).await.expect("ingest");
    let d29 = deplete_at(&pool, pid, 1003, T13, 29).await;
    consumer.ingest_once(10_000).await.expect("ingest");
    receipt_at(&pool, pid, 1005, T11, 37, 39).await;
    receipt_at(&pool, pid, 1006, T11, 35, 327).await;

    // gen-1: the scan reads trx_line directly, so it sees the two receipts
    // whose feed delivery is still pending; deplete-29 draws 4@279 + 25@39 =
    // 2091 → banker_div(2091, 29) = 72, delta (72 − 279) × 29 = −6003.
    let r = recalc_step(&pool).await;
    assert_eq!(r["settlements_written"], json!(1));
    assert_eq!(r["adjustments_posted"], json!(1));
    assert_eq!(authoritative_of(&pool, d29).await, Some(72));
    assert_eq!(
        aggregate(&pool, pid).await,
        Some((47, 254, 11916)),
        "gen-1 correction folded: 5913 + 6003"
    );

    // Backdated exact-empty depletion: qty 47 → 0 flushes value_sum to 0,
    // discarding the folded +6003; recorded cost is the derived average 254.
    let d47_t10 = deplete_at(&pool, pid, 1008, T10, 47).await;
    assert_eq!(
        aggregate(&pool, pid).await,
        Some((0, 254, 0)),
        "exact-empty flush wiped the folded correction"
    );

    // Rolled-back pass: no observable effect (whole-pass transactionality).
    {
        let mut tx = pool.begin().await.unwrap();
        let _: Value = sqlx::query_scalar("SELECT ledger_recalc_step()")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
    }

    let d47_t13 = deplete_at(&pool, pid, 1010, T13, 47).await;
    consumer.ingest_once(10_000).await.expect("ingest");
    consumer.ingest_once(10_000).await.expect("ingest");
    let d32 = deplete_at(&pool, pid, 1013, T13, 32).await;

    // Quiesce: ingest + drain until nothing moves.
    loop {
        let mut inserts = 0usize;
        loop {
            let r = consumer.ingest_once(10_000).await.expect("ingest");
            inserts += r.inserts;
            if r.messages == 0 {
                break;
            }
        }
        let passes = drain_recalc(&pool).await;
        let floors = count(
            &pool,
            "SELECT count(*) FROM pool_settlement WHERE recost_floor_posted_at IS NOT NULL",
        )
        .await;
        let queued = count(&pool, "SELECT count(*) FROM recalc_queue").await;
        if inserts == 0 && passes.is_empty() && floors == 0 && queued == 0 {
            break;
        }
    }

    // Settled history: the full-replay re-cost confirms 72 (no new row for
    // d29), the uncovered depletions cost at their observed 254, and only the
    // T13 deplete-47 moves (12@39 + 35@327 = 11913 → banker_div = 253).
    assert_eq!(authoritative_of(&pool, d29).await, Some(72));
    assert_eq!(authoritative_of(&pool, d47_t10).await, Some(254));
    assert_eq!(authoritative_of(&pool, d47_t13).await, Some(253));
    assert_eq!(authoritative_of(&pool, d32).await, Some(254));
    assert_eq!(
        count(
            &pool,
            &format!("SELECT count(*) FROM cost_settlement WHERE depletion_trx_line_id = {d29}")
        )
        .await,
        1,
        "re-cost confirms 72 without a new settlement row"
    );

    let (agg_qty, _, value_sum) = aggregate(&pool, pid).await.expect("aggregate");
    assert_eq!((agg_qty, value_sum), (-79, -20019));

    // The telescoped identity is short by exactly the wiped correction.
    let events: Vec<(i64, i64, i64)> = sqlx::query_as(
        "SELECT id, qty, unit_cost FROM trx_line \
          WHERE pool_id = $1 AND line_type <> 'cost_adjustment_line' \
          ORDER BY id",
    )
    .bind(pid)
    .fetch_all(&pool)
    .await
    .unwrap();
    let mut hot_qty: i64 = 0;
    let mut hot_value: i128 = 0;
    for &(_, q, c) in &events {
        hot_qty += q;
        if q > 0 {
            hot_value += q as i128 * c as i128;
        } else if hot_qty == 0 {
            hot_value = 0;
        } else {
            hot_value -= (-q) as i128 * c as i128;
        }
    }
    let mut deltas: i128 = 0;
    for &(id, q, observed) in &events {
        if q < 0 {
            let auth = authoritative_of(&pool, id).await.expect("settled");
            deltas += (auth - observed) as i128 * (-q) as i128;
        }
    }
    assert_eq!(value_sum as i128 - (hot_value - deltas), -6003, "wiped gen-1 correction");

    // The apply-order fold reconciles exactly: physical lines through the FL
    // formulas, each cost_adjustment_line through its settlement's
    // (authoritative − prior) × qty reconcile.
    let all_lines: Vec<(i64, String, i64, i64)> = sqlx::query_as(
        "SELECT id, line_type::text, qty, unit_cost FROM trx_line \
          WHERE pool_id = $1 ORDER BY id",
    )
    .bind(pid)
    .fetch_all(&pool)
    .await
    .unwrap();
    let recons: Vec<(i64, i64, i64, i64)> = sqlx::query_as(
        "SELECT tl.id, cs.authoritative_unit_cost, cs.prior_unit_cost, -tl.qty \
           FROM trx_line tl \
           JOIN posting_line pl ON pl.trx_line_id = tl.id \
           JOIN cost_settlement cs ON cs.adjustment_posting_line_id = pl.id \
          WHERE tl.pool_id = $1 AND tl.line_type = 'cost_adjustment_line'",
    )
    .bind(pid)
    .fetch_all(&pool)
    .await
    .unwrap();
    let mut fold_qty: i64 = 0;
    let mut fold_value: i128 = 0;
    for &(id, ref lt, q, c) in &all_lines {
        if lt == "cost_adjustment_line" {
            let &(_, auth, prior, dep_qty) =
                recons.iter().find(|(rid, _, _, _)| *rid == id).expect("reconcile for adj line");
            fold_value -= (auth - prior) as i128 * dep_qty as i128;
            continue;
        }
        fold_qty += q;
        if q > 0 {
            fold_value += q as i128 * c as i128;
        } else if fold_qty == 0 {
            fold_value = 0;
        } else {
            fold_value -= (-q) as i128 * c as i128;
        }
    }
    assert_eq!(value_sum as i128, fold_value, "apply-order fold reconciles the wipe");

    drop_feed_slot(&pool).await;
}
