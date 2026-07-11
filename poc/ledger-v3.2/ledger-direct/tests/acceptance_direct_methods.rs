//! Acceptance tests for `ledger_submit_trx` per-method dispatch (design-v3.2
//! §3/§4): WAC via the SPIKE-B commutative CTEs (strict, gated, posts GL),
//! FIFO/LIFO via alt-C appends (unconditional, flagged negatives, no GL),
//! STD/specific via the ledger-core path (variance leg, K=1 layers), plus the
//! wire-contract failure modes and the multi-line mixed dispatch.

mod common;

use common::*;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn direct_methods() {
    let pool = connect_pool().await;

    // ── WAC: strict aggregate via the commutative CTEs ──────────────────
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "wac", "running_avg").await;

    let trx1 = submit(&pool, "po_receipt", 1, json!([line(f.pool_id, "po_receipt_line", 10, 100)]))
        .await
        .expect("wac receipt 1");
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((10, 100, 1000)), "first receipt");

    submit(&pool, "po_receipt", 2, json!([line(f.pool_id, "po_receipt_line", 30, 50)]))
        .await
        .expect("wac receipt 2");
    // value 1000 + 1500 = 2500 over qty 40 -> banker_div = 62 (62.5 half-even).
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((40, 62, 2500)), "running average");

    // Receipt posting: amount qty*cost, receipt pair as-is.
    let (amt, deb, cred): (i64, i64, i64) = sqlx::query_as(
        "SELECT p.amount, p.debit_account, p.credit_account FROM posting_line p \
          JOIN trx_line t ON t.id = p.trx_line_id WHERE t.trx_id = $1",
    )
    .bind(trx1)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((amt, deb, cred), (1000, INV_ACCT, AP_ACCT), "receipt posting");

    // Depletion at the running average, pair swapped.
    let trx3 =
        submit(&pool, "inv_adjustment", 3, json!([line(f.pool_id, "inv_adjustment_line", -4, 0)]))
            .await
            .expect("wac deplete");
    // amount 4*62 = 248; value 2500-248 = 2252; qty 36; avg re-derived 63.
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((36, 63, 2252)), "post-depletion");
    let (amt, deb, cred, tl_uc): (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT p.amount, p.debit_account, p.credit_account, t.unit_cost \
           FROM posting_line p JOIN trx_line t ON t.id = p.trx_line_id WHERE t.trx_id = $1",
    )
    .bind(trx3)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((amt, deb, cred), (248, ADJ_ACCT, INV_ACCT), "depletion posting swaps pair");
    assert_eq!(tl_uc, 62, "depletion records the pre-update running average");

    // Gate: depleting more than on-hand raises 23000 and writes NOTHING.
    let trx_before = count(&pool, "SELECT count(*) FROM trx").await;
    let err = submit(
        &pool,
        "inv_adjustment",
        4,
        json!([line(f.pool_id, "inv_adjustment_line", -100, 0)]),
    )
    .await
    .expect_err("wac overdraw must raise");
    assert_eq!(sqlstate(&err), "23000", "insufficient inventory SQLSTATE");
    assert_eq!(count(&pool, "SELECT count(*) FROM trx").await, trx_before, "nothing written");
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((36, 63, 2252)), "aggregate untouched");

    // trx_line.posted_at denorm matches trx.posted_at (recalc-d D1).
    let mismatches = count(
        &pool,
        "SELECT count(*) FROM trx_line tl JOIN trx t ON t.id = tl.trx_id \
          WHERE tl.posted_at IS DISTINCT FROM t.posted_at",
    )
    .await;
    assert_eq!(mismatches, 0, "posted_at denorm");

    // ── FIFO: alt-C append — no GL, no gate, flagged negatives ─────────
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;

    submit(&pool, "po_receipt", 10, json!([line(f.pool_id, "po_receipt_line", 5, 100)]))
        .await
        .expect("fifo receipt");
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((5, 100, 500)));
    assert_eq!(count(&pool, "SELECT count(*) FROM posting_line").await, 0, "no cost leg (alt C)");

    // Deplete beyond on-hand: SUCCEEDS, aggregate goes negative (flagged, not
    // rejected), average preserved, observed cost = pre-update average.
    let trx_over =
        submit(&pool, "inv_adjustment", 11, json!([line(f.pool_id, "inv_adjustment_line", -8, 0)]))
            .await
            .expect("fifo overdraw must succeed");
    assert_eq!(
        aggregate(&pool, f.pool_id).await,
        Some((-3, 100, -300)),
        "negative flagged aggregate"
    );
    let observed: i64 =
        sqlx::query_scalar("SELECT unit_cost FROM trx_line WHERE trx_id = $1")
            .bind(trx_over)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(observed, 100, "observed cost = running average");
    assert_eq!(count(&pool, "SELECT count(*) FROM posting_line").await, 0, "still no cost leg");

    // Zero-crossing replenishment (§3.6): -300 value catches up through the
    // receipt; the average re-derives from the corrected book value.
    submit(&pool, "po_receipt", 12, json!([line(f.pool_id, "po_receipt_line", 10, 200)]))
        .await
        .expect("fifo zero-crossing receipt");
    // qty -3 + 10 = 7; value -300 + 2000 = 1700; banker_div(1700, 7) = 243.
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((7, 243, 1700)), "zero-crossing math");

    // Depletion of a never-received pool upserts a negative aggregate.
    let f2 = seed_pool(&pool, 2, 2, 2, "fifo", "running_avg").await;
    let trx_ghost =
        submit(&pool, "inv_adjustment", 13, json!([line(f2.pool_id, "inv_adjustment_line", -5, 0)]))
            .await
            .expect("fifo depletion of empty pool must succeed");
    assert_eq!(aggregate(&pool, f2.pool_id).await, Some((-5, 0, 0)), "ghost depletion aggregate");
    let observed: i64 = sqlx::query_scalar("SELECT unit_cost FROM trx_line WHERE trx_id = $1")
        .bind(trx_ghost)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(observed, 0, "no basis to observe — cost 0, recalc assigns authoritative");

    // ── LIFO standard-basis observed cost ───────────────────────────────
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "lifo", "standard").await;
    set_standard_cost(&pool, &f, 70).await;
    submit(&pool, "po_receipt", 20, json!([line(f.pool_id, "po_receipt_line", 5, 100)]))
        .await
        .expect("lifo receipt");
    let trx_std =
        submit(&pool, "inv_adjustment", 21, json!([line(f.pool_id, "inv_adjustment_line", -2, 0)]))
            .await
            .expect("lifo standard-basis deplete");
    let observed: i64 = sqlx::query_scalar("SELECT unit_cost FROM trx_line WHERE trx_id = $1")
        .bind(trx_std)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(observed, 70, "standard-basis observed cost");
    // value_sum depletes at the standard basis: 500 - 2*70 = 360.
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((3, 120, 360)), "std-basis value math");

    // Missing standard_cost on a standard-basis depletion fails loud.
    let f3 = seed_pool(&pool, 3, 3, 3, "fifo", "standard").await;
    let err = submit(
        &pool,
        "inv_adjustment",
        22,
        json!([line(f3.pool_id, "inv_adjustment_line", -1, 0)]),
    )
    .await
    .expect_err("standard-basis depletion without standard_cost must raise");
    assert_eq!(sqlstate(&err), "22000", "MissingStandardCost SQLSTATE");

    // ── STD: main leg at standard + variance leg (ledger-core path) ────
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "std", "running_avg").await;
    set_standard_cost(&pool, &f, 50).await;

    let trx_std = submit(&pool, "po_receipt", 30, json!([line(f.pool_id, "po_receipt_line", 10, 60)]))
        .await
        .expect("std receipt");
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((10, 50, 500)), "aggregate mirrors std");
    let postings: Vec<(String, i64, i64, i64)> = sqlx::query_as(
        "SELECT p.event_type::text, p.amount, p.debit_account, p.credit_account \
           FROM posting_line p JOIN trx_line t ON t.id = p.trx_line_id \
          WHERE t.trx_id = $1 ORDER BY p.id",
    )
    .bind(trx_std)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        postings,
        vec![
            ("inventory_receipt".into(), 500, INV_ACCT, AP_ACCT),
            ("variance".into(), 100, VAR_ACCT, AP_ACCT), // unfavorable 10*(60-50)
        ],
        "std receipt legs"
    );

    // Favorable variance flips direction.
    let trx_fav = submit(&pool, "po_receipt", 31, json!([line(f.pool_id, "po_receipt_line", 10, 40)]))
        .await
        .expect("std favorable receipt");
    let (deb, cred): (i64, i64) = sqlx::query_as(
        "SELECT p.debit_account, p.credit_account FROM posting_line p \
           JOIN trx_line t ON t.id = p.trx_line_id \
          WHERE t.trx_id = $1 AND p.event_type = 'variance'",
    )
    .bind(trx_fav)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((deb, cred), (AP_ACCT, VAR_ACCT), "favorable variance flips");

    // Depletion at standard; gate holds.
    submit(&pool, "inv_adjustment", 32, json!([line(f.pool_id, "inv_adjustment_line", -4, 0)]))
        .await
        .expect("std deplete");
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((16, 50, 800)), "std deplete");
    let err = submit(
        &pool,
        "inv_adjustment",
        33,
        json!([line(f.pool_id, "inv_adjustment_line", -100, 0)]),
    )
    .await
    .expect_err("std overdraw must raise");
    assert_eq!(sqlstate(&err), "23000");

    // Variance needed but variance_acct NULL fails loud.
    sqlx::query("UPDATE posting_account_map SET variance_acct = NULL WHERE sku_id = $1")
        .bind(f.sku_id)
        .execute(&pool)
        .await
        .unwrap();
    let err = submit(&pool, "po_receipt", 34, json!([line(f.pool_id, "po_receipt_line", 1, 60)]))
        .await
        .expect_err("variance without variance_acct must raise");
    assert_eq!(sqlstate(&err), "22000", "MissingVarianceAccount SQLSTATE");

    // ── specific: K=1 layer lifecycle (ledger-core path) ────────────────
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "specific", "running_avg").await;

    let trx_r = submit(&pool, "po_receipt", 40, json!([line(f.pool_id, "po_receipt_line", 1, 777)]))
        .await
        .expect("specific receipt");
    let receipt_tl: i64 = sqlx::query_scalar("SELECT id FROM trx_line WHERE trx_id = $1")
        .bind(trx_r)
        .fetch_one(&pool)
        .await
        .unwrap();
    let layer: (i64, i64) = sqlx::query_as(
        "SELECT layer_id, unit_cost FROM pool_state WHERE pool_id = $1 AND layer_id > 0",
    )
    .bind(f.pool_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(layer, (receipt_tl, 777), "layer_id = receipt trx_line.id");

    // K=1: second receipt while stocked raises.
    let err = submit(&pool, "po_receipt", 41, json!([line(f.pool_id, "po_receipt_line", 1, 800)]))
        .await
        .expect_err("second specific receipt must raise");
    assert_eq!(sqlstate(&err), "23000", "SpecificPoolOccupied SQLSTATE");

    // Depletion consumes the layer and links it.
    let trx_d =
        submit(&pool, "inv_adjustment", 42, json!([line(f.pool_id, "inv_adjustment_line", -1, 0)]))
            .await
            .expect("specific deplete");
    let (uc, src): (i64, Option<i64>) =
        sqlx::query_as("SELECT unit_cost, source_trx_line_id FROM trx_line WHERE trx_id = $1")
            .bind(trx_d)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((uc, src), (777, Some(receipt_tl)), "specific depletion links its layer");
    assert_eq!(
        count(&pool, "SELECT count(*) FROM pool_state WHERE layer_id > 0").await,
        0,
        "layer deleted on consumption"
    );

    // ── wire-contract failure modes ─────────────────────────────────────
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "wac", "running_avg").await;

    // Unknown pool.
    let err = submit(&pool, "po_receipt", 50, json!([line(999, "po_receipt_line", 1, 10)]))
        .await
        .expect_err("unknown pool must raise");
    assert_eq!(sqlstate(&err), "42704", "UnknownPool SQLSTATE");

    // Missing posting_account_map fails loud EVEN for an alt-C fifo append
    // (recalc needs the config later — §4 fail-loud at submit time).
    for stmt in [
        "INSERT INTO sku (id, code, name) VALUES (9, 'SKU-9', 'no-map sku')",
        "INSERT INTO location (id, code, name) VALUES (9, 'LOC-9', 'no-map loc')",
        "INSERT INTO pool (id, sku_id, location_id, method) VALUES (9, 9, 9, 'fifo')",
    ] {
        sqlx::query(stmt).execute(&pool).await.unwrap();
    }
    let err = submit(&pool, "po_receipt", 51, json!([line(9, "po_receipt_line", 1, 10)]))
        .await
        .expect_err("missing posting_account_map must raise");
    assert_eq!(sqlstate(&err), "22000", "MissingPostingAccounts SQLSTATE");

    // Idempotency: duplicate (trx_type, source_id) raises unique violation.
    submit(&pool, "po_receipt", 52, json!([line(f.pool_id, "po_receipt_line", 1, 10)]))
        .await
        .expect("first submission");
    let err = submit(&pool, "po_receipt", 52, json!([line(f.pool_id, "po_receipt_line", 1, 10)]))
        .await
        .expect_err("duplicate source_id must raise");
    assert_eq!(sqlstate(&err), "23505", "idempotency key SQLSTATE");

    // ── multi-line mixed submission: one trx, per-method dispatch ──────
    reset_state(&pool).await;
    let f_std = seed_pool(&pool, 1, 1, 1, "std", "running_avg").await;
    let f_wac = seed_pool(&pool, 2, 2, 2, "wac", "running_avg").await;
    let f_fifo = seed_pool(&pool, 3, 3, 3, "fifo", "running_avg").await;
    set_standard_cost(&pool, &f_std, 50).await;

    let trx_id = submit(
        &pool,
        "po_receipt",
        60,
        json!([
            line(f_fifo.pool_id, "po_receipt_line", 7, 30),
            line(f_std.pool_id, "po_receipt_line", 10, 60),
            line(f_wac.pool_id, "po_receipt_line", 4, 25),
        ]),
    )
    .await
    .expect("mixed multi-line submission");

    assert_eq!(count(&pool, "SELECT count(*) FROM trx").await, 1, "one trx");
    assert_eq!(
        count(&pool, &format!("SELECT count(*) FROM trx_line WHERE trx_id = {trx_id}")).await,
        3,
        "three lines share the trx"
    );
    assert_eq!(aggregate(&pool, f_std.pool_id).await, Some((10, 50, 500)));
    assert_eq!(aggregate(&pool, f_wac.pool_id).await, Some((4, 25, 100)));
    assert_eq!(aggregate(&pool, f_fifo.pool_id).await, Some((7, 30, 210)));
    // GL: std main + std variance + wac receipt = 3 postings; fifo posts none.
    assert_eq!(count(&pool, "SELECT count(*) FROM posting_line").await, 3, "mixed GL legs");

    // Same-pool line order is preserved: receipt then deplete in one call.
    let trx_seq = submit(
        &pool,
        "inv_adjustment",
        61,
        json!([
            line(f_wac.pool_id, "inv_adjustment_line", 6, 75),
            line(f_wac.pool_id, "inv_adjustment_line", -5, 0),
        ]),
    )
    .await
    .expect("same-pool receipt-then-deplete");
    // 4@25 + 6@75 = 550/10 = 55; deplete 5 at 55 -> qty 5, value 275.
    assert_eq!(aggregate(&pool, f_wac.pool_id).await, Some((5, 55, 275)), "in-order same-pool");
    let n_lines =
        count(&pool, &format!("SELECT count(*) FROM trx_line WHERE trx_id = {trx_seq}")).await;
    assert_eq!(n_lines, 2);

    // Zero-qty lines are no-ops.
    let trx_zero = submit(
        &pool,
        "inv_adjustment",
        62,
        json!([
            line(f_wac.pool_id, "inv_adjustment_line", 0, 10),
            line(f_wac.pool_id, "inv_adjustment_line", 2, 10),
        ]),
    )
    .await
    .expect("zero-qty line skipped");
    assert_eq!(
        count(&pool, &format!("SELECT count(*) FROM trx_line WHERE trx_id = {trx_zero}")).await,
        1,
        "only the non-zero line lands"
    );

    println!("acceptance_direct_methods: all assertions passed");
}
