//! acct-2ttr.3 §9.2 acceptance: per-method correctness of ledger_submit_trx_c.
//!
//! Covers the method scenarios from design-v3.1 §9.2:
//!   - FIFO receipt(s) + depletion: aggregate running-average, provisional cost on
//!     the depletion trx_line, NULL source_trx_line_id, zero layer_id>0 rows.
//!   - WAC parity with FIFO provisional.
//!   - STD receipt: trx_line at standard, variance posting for actual-vs-standard;
//!     no-standard_cost → RAISE; depletion at standard.
//!   - specific receipt + depletion: layer materialization + source link.
//!   - FIFO provisional_basis = running_avg vs standard.
//!   - 'standard'-basis FIFO with no standard_cost → RAISE.

mod common;

use common::*;

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn fifo_receipts_running_average_then_provisional_depletion() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;

    // 10 @ 100, then 10 @ 200 → running average 150.
    submit(&pool, "po_receipt", 1, vec![receipt(&f, 10, 100)]).await.expect("r1");
    submit(&pool, "po_receipt", 2, vec![receipt(&f, 10, 200)]).await.expect("r2");
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((20, 150)));

    // Deplete 5 at the provisional (running average) cost.
    let dep = submit(&pool, "transfer_shipment", 3, vec![depletion(&f, 5)]).await.expect("dep");

    let lines = trx_lines(&pool, dep).await;
    assert_eq!(lines.len(), 1);
    let (qty, unit_cost, src) = lines[0];
    assert_eq!(qty, -5);
    assert_eq!(unit_cost, 150, "depletion records the running-average provisional");
    assert_eq!(src, None, "§3.5: provisional FIFO leaves source_trx_line_id NULL");

    assert_eq!(layer_count(&pool, f.pool_id).await, 0, "Path C FIFO materializes no layers");
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((15, 150)));

    let posts = posting_lines(&pool, dep).await;
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].0, "inventory_depletion");
    assert_eq!(posts[0].1, 750, "5 × 150");
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn two_lines_same_pool_one_submission_succeeds_and_coalesces() {
    // acct-036x regression: a single submission with two lines on the same pool
    // used to crash the direct bulk aggregate UPSERT ("ON CONFLICT DO UPDATE
    // command cannot affect row a second time"). ledger-core now coalesces the
    // per-line aggregate mutations to one per pool, so it must succeed with the
    // aggregate reflecting both lines (matching what the routed path produces).
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;

    // Two receipts on the SAME pool in ONE submission: 10 @ 100 + 10 @ 200.
    let trx = submit(&pool, "po_receipt", 1, vec![receipt(&f, 10, 100), receipt(&f, 10, 200)])
        .await
        .expect("two-line same-pool submission must succeed (acct-036x)");

    // Both lines are recorded for audit; the aggregate is collapsed and cumulative.
    assert_eq!(trx_lines(&pool, trx).await.len(), 2);
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((20, 150)));
    assert_eq!(layer_count(&pool, f.pool_id).await, 0, "Path C FIFO materializes no layers");
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn wac_matches_fifo_provisional() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "wac", "running_avg").await;

    submit(&pool, "po_receipt", 1, vec![receipt(&f, 10, 100)]).await.expect("r1");
    submit(&pool, "po_receipt", 2, vec![receipt(&f, 10, 200)]).await.expect("r2");
    let dep = submit(&pool, "transfer_shipment", 3, vec![depletion(&f, 5)]).await.expect("dep");

    let lines = trx_lines(&pool, dep).await;
    assert_eq!(lines[0].1, 150, "WAC depletion at running average");
    assert_eq!(lines[0].2, None);
    assert_eq!(layer_count(&pool, f.pool_id).await, 0);
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((15, 150)));
    assert_eq!(posting_lines(&pool, dep).await[0].1, 750);
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn std_receipt_emits_variance_posting() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "std", "running_avg").await;
    seed_standard_cost(&pool, f.sku_id, f.loc_id, 100).await;

    // Receipt 10 @ actual 120 → trx_line at std 100, variance 10×(120−100)=200.
    let r = submit(&pool, "po_receipt", 1, vec![receipt_std(&f, 10, 120)]).await.expect("r");

    let lines = trx_lines(&pool, r).await;
    assert_eq!(lines[0].1, 100, "trx_line recorded at standard cost");

    let posts = posting_lines(&pool, r).await;
    assert_eq!(posts.len(), 2, "receipt + variance legs");
    assert_eq!(posts[0].0, "inventory_receipt");
    assert_eq!(posts[0].1, 1000, "10 × 100 std");
    assert_eq!(posts[1].0, "variance");
    assert_eq!(posts[1].1, 200);
    assert_eq!(posts[1].2, f.var_acct, "unfavorable variance debits PPV account");
    assert_eq!(posts[1].3, f.ap_acct);

    assert_eq!(aggregate(&pool, f.pool_id).await, Some((10, 100)));

    // Depletion at standard cost.
    let dep = submit(&pool, "transfer_shipment", 2, vec![depletion(&f, 4)]).await.expect("dep");
    assert_eq!(trx_lines(&pool, dep).await[0].1, 100);
    let dposts = posting_lines(&pool, dep).await;
    assert_eq!(dposts.len(), 1);
    assert_eq!(dposts[0].1, 400);
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn std_without_standard_cost_raises() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "std", "running_avg").await;
    // No standard_cost row seeded.
    let err = submit(&pool, "po_receipt", 1, vec![receipt_std(&f, 10, 120)]).await;
    assert!(err.is_err(), "STD receipt without standard_cost must RAISE");
    let msg = format!("{}", err.unwrap_err());
    assert!(msg.contains("missing standard cost"), "unexpected error: {msg}");
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn specific_receipt_then_depletion_materializes_and_links() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "specific", "running_avg").await;

    // Specific receipt qty=1 @ 500 materializes one layer (layer_id = its trx_line.id).
    let r = submit(&pool, "po_receipt", 1, vec![receipt(&f, 1, 500)]).await.expect("r");
    assert_eq!(layer_count(&pool, f.pool_id).await, 1, "specific receipt materializes a layer");
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((1, 500)));

    // The layer's id is the receipt's trx_line.id.
    let layer_id: i64 = sqlx::query_scalar(
        "SELECT layer_id FROM pool_state WHERE pool_id = $1 AND layer_id > 0",
    )
    .bind(f.pool_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let receipt_tl: i64 =
        sqlx::query_scalar("SELECT id FROM trx_line WHERE trx_id = $1").bind(r).fetch_one(&pool).await.unwrap();
    assert_eq!(layer_id, receipt_tl, "layer_id equals the receipt trx_line.id");

    // Depletion consumes the layer and links back to it.
    let dep = submit(&pool, "transfer_shipment", 2, vec![depletion(&f, 1)]).await.expect("dep");
    let lines = trx_lines(&pool, dep).await;
    assert_eq!(lines[0].1, 500, "depletion at the layer's cost");
    assert_eq!(lines[0].2, Some(layer_id), "source_trx_line_id links the consumed layer");
    assert_eq!(layer_count(&pool, f.pool_id).await, 0, "consumed layer deleted");
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn fifo_standard_basis_depletes_at_standard_not_average() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "standard").await;
    seed_standard_cost(&pool, f.sku_id, f.loc_id, 90).await;

    // Receipt drives the running average to 100, but standard basis ignores it.
    submit(&pool, "po_receipt", 1, vec![receipt(&f, 10, 100)]).await.expect("r");
    let dep = submit(&pool, "transfer_shipment", 2, vec![depletion(&f, 5)]).await.expect("dep");

    let lines = trx_lines(&pool, dep).await;
    assert_eq!(lines[0].1, 90, "standard basis records standard_cost, not the 100 running avg");
    assert_eq!(lines[0].2, None);
    assert_eq!(layer_count(&pool, f.pool_id).await, 0);
    assert_eq!(posting_lines(&pool, dep).await[0].1, 450, "5 × 90");
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn fifo_standard_basis_without_standard_cost_raises_on_depletion() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "standard").await;
    // No standard_cost: the FIFO running-avg receipt succeeds...
    submit(&pool, "po_receipt", 1, vec![receipt(&f, 10, 100)]).await.expect("receipt ok");
    // ...but the standard-basis depletion has nothing to price against.
    let err = submit(&pool, "transfer_shipment", 2, vec![depletion(&f, 5)]).await;
    assert!(err.is_err(), "standard-basis depletion without standard_cost must RAISE");
    assert!(format!("{}", err.unwrap_err()).contains("missing standard cost"));
}
