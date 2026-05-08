//! Property test for the Phase E1 FIFO subledger (acct-cbss).
//! Generates random workloads of receipts + issues against SKU-FIF
//! and asserts SEVEN invariants per case:
//!
//!   I1  Every applied receipt produces exactly one cost_layers row
//!       linked back to its receipt_posting_line_id.
//!
//!   I2  Every applied issue produces ≥1 cost_layer_depletions rows
//!       linked to its posting_line_id, summing to the issue quantity.
//!
//!   I3  Each depletion row's cost_amount = depleted_quantity ×
//!       cost_layers.unit_cost (per-layer ROUND to BIGINT cents).
//!
//!   I4  Per (product, location): residual = SUM(original_quantity)
//!       − SUM(depleted_quantity) ≥ 0 always (no over-consumption).
//!
//!   I5  posting_line.amount on each issue equals the SUM of its
//!       cost_layer_depletions.cost_amount values (FIFO weighted).
//!
//!   I6  Layer rows are immutable: the count of cost_layers rows
//!       seen at the end equals the count of receipt operations that
//!       successfully applied (no UPDATEs that recomputed counts;
//!       no DELETEs blocked by the append-only trigger).
//!
//!   I7  posting_line_inventory.cost_layer_id on every issue's
//!       value-leg points at one of the layer_ids that was consumed
//!       in that issue (the first one walked).
//!
//! Acts as the regression net for the FIFO subledger: dispatcher
//! amount and depletion writeback stay coherent across arbitrary
//! receive/issue mixes. PROPTEST_CASES env var controls case count
//! (default 100). --test-threads=1 required since the test resets
//! the shared DB between scenarios.

mod common;

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use serde_json::json;
use sqlx::PgPool;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;

#[derive(Debug, Clone)]
enum Op {
    /// FIFO receipt of `qty` units at `unit_cost`. Posts a 2-event batch
    /// (qty leg + value leg) with reason='po_receipt'.
    Receive { qty: i64, unit_cost: i64 },
    /// FIFO issue of `qty` units. Posts a 2-event batch with reason='scrap'.
    /// Skipped at runtime if on-hand qty < requested.
    Issue { qty: i64 },
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (1i64..15, 5i64..50).prop_map(|(qty, unit_cost)| Op::Receive { qty, unit_cost }),
        (1i64..10).prop_map(|qty| Op::Issue { qty }),
    ]
}

fn arb_op_seq() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(arb_op(), 1..10)
}

struct FifScaffold {
    sku: String,
    loc: String,
    inv_raw: i64,
    stock_avail: i64,
    ap_unsettled: i64,
    ven_qty: i64,
    inv_adj_expense: i64,
    void_qty: i64,
}

async fn fresh_vendor_uuid(pool: &PgPool, label: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency)
         VALUES ($1, $1, 'USD') RETURNING id::text",
    )
    .bind(label)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn scaffold_fif(pool: &PgPool, label_seed: usize) -> FifScaffold {
    let sku: String = sqlx::query_scalar("SELECT id::text FROM skus WHERE code = 'SKU-FIF'")
        .fetch_one(pool)
        .await
        .unwrap();
    let loc: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code = 'MAIN'")
        .fetch_one(pool)
        .await
        .unwrap();
    let vendor =
        fresh_vendor_uuid(pool, &format!("VEND-FIF-PROP-{label_seed}")).await;

    let inv_raw: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, sku_id, location_id, normal_side)
         VALUES ('inv_value_raw', 'value', 'USD', $1::UUID, $2::UUID, 'debit')
         RETURNING id",
    )
    .bind(&sku)
    .bind(&loc)
    .fetch_one(pool)
    .await
    .unwrap();

    let ap_unsettled: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ('ap_unsettled', 'value', 'USD', $1::UUID, 'credit')
         RETURNING id",
    )
    .bind(&vendor)
    .fetch_one(pool)
    .await
    .unwrap();

    let ven_qty: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, counterparty_id, normal_side)
         VALUES ('vendor_pool', 'qty', $1::UUID, 'credit')
         RETURNING id",
    )
    .bind(&vendor)
    .fetch_one(pool)
    .await
    .unwrap();

    let stock_avail = common::account_id_stock_available(pool, "SKU-FIF", "MAIN").await;
    let inv_adj_expense =
        common::account_id_by_kind_currency(pool, "inv_adj_expense", Some("USD")).await;
    let void_qty = common::account_id_by_kind_currency(pool, "creation_void", None).await;

    FifScaffold {
        sku,
        loc,
        inv_raw,
        stock_avail,
        ap_unsettled,
        ven_qty,
        inv_adj_expense,
        void_qty,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn property_fifo_consistency() {
    let pool = common::connect_test_db().await;

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(TEST_PROPTEST_CASES_DEFAULT);

    let mut runner = proptest::test_runner::TestRunner::default();
    let strategy = arb_op_seq();

    for case_idx in 0..cases as usize {
        common::reset_to_fixture(&pool).await;

        let tree = strategy.new_tree(&mut runner).expect("strategy.new_tree");
        let ops: Vec<Op> = tree.current();

        let label = format!("fifo_consistency#{case_idx}");
        let s = scaffold_fif(&pool, case_idx).await;

        let mut on_hand: i64 = 0;
        let mut applied_receipts: i64 = 0;
        let mut applied_issues: Vec<i64> = Vec::new(); // posting_line ids of issues

        for (i, op) in ops.iter().enumerate() {
            // Stagger business_dates so receipt_date ordering is deterministic
            // within a case. Stay within the partition window (2026-01..2027-12).
            let day = 1 + (i as i64 % 27);
            let business_date = format!("2026-04-{day:02}");

            match *op {
                Op::Receive { qty, unit_cost } => {
                    let qty_key = common::fresh_uuid(&pool).await;
                    let val_key = common::fresh_uuid(&pool).await;
                    let amount = qty * unit_cost;
                    let res = common::call_post_posting_lines(
                        &pool,
                        json!([
                            common::make_event(
                                "po_receipt",
                                s.stock_avail,
                                s.ven_qty,
                                qty,
                                &business_date,
                                &qty_key,
                            ),
                            common::make_event_with_qty(
                                "po_receipt",
                                s.inv_raw,
                                s.ap_unsettled,
                                amount,
                                qty,
                                &business_date,
                                &val_key,
                            ),
                        ]),
                        false,
                    )
                    .await;
                    if res.is_ok() {
                        on_hand += qty;
                        applied_receipts += 1;
                    }
                }
                Op::Issue { qty } => {
                    if qty > on_hand || qty <= 0 {
                        continue;
                    }
                    let qty_key = common::fresh_uuid(&pool).await;
                    let val_key = common::fresh_uuid(&pool).await;
                    let res = common::call_post_posting_lines(
                        &pool,
                        json!([
                            common::make_event(
                                "scrap",
                                s.void_qty,
                                s.stock_avail,
                                qty,
                                &business_date,
                                &qty_key,
                            ),
                            common::make_event_with_qty(
                                "scrap",
                                s.inv_adj_expense,
                                s.inv_raw,
                                0,
                                qty,
                                &business_date,
                                &val_key,
                            ),
                        ]),
                        false,
                    )
                    .await;
                    if res.is_ok() {
                        on_hand -= qty;
                        let pl_id: i64 = sqlx::query_scalar(
                            "SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID",
                        )
                        .bind(&val_key)
                        .fetch_one(&pool)
                        .await
                        .unwrap();
                        applied_issues.push(pl_id);
                    }
                }
            }
        }

        if applied_receipts == 0 {
            // Nothing exercised; skip.
            continue;
        }

        // ============================================================
        // I1: every receipt → exactly one cost_layers row.
        // ============================================================
        let layer_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::BIGINT FROM cost_layers WHERE product_id = $1::UUID",
        )
        .bind(&s.sku)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            layer_count, applied_receipts,
            "[{label}] I1: layer count {layer_count} ≠ applied receipts {applied_receipts}"
        );

        // ============================================================
        // I2: every issue → ≥1 depletion rows summing to issue qty.
        // ============================================================
        for pl_id in &applied_issues {
            let row: (i64, String, String) = sqlx::query_as(
                "SELECT COUNT(*)::BIGINT,
                        COALESCE(SUM(depleted_quantity), 0)::TEXT,
                        pl.qty::TEXT
                   FROM cost_layer_depletions d
                   JOIN posting_lines pl ON pl.id = d.posting_line_id
                  WHERE d.posting_line_id = $1
                  GROUP BY pl.qty",
            )
            .bind(pl_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            let depl_count = row.0;
            let depl_sum_str = row.1;
            let issue_qty_str = row.2;
            assert!(
                depl_count >= 1,
                "[{label}] I2 (pl={pl_id}): expected ≥1 depletion, got {depl_count}"
            );
            // Compare leading digit string — qty is positive integer in our gen.
            let depl_sum: f64 = depl_sum_str.parse().unwrap();
            let issue_qty: f64 = issue_qty_str.parse().unwrap();
            assert!(
                (depl_sum - issue_qty).abs() < 1e-6,
                "[{label}] I2 (pl={pl_id}): SUM(depleted) {depl_sum} ≠ issue qty {issue_qty}"
            );
        }

        // ============================================================
        // I3: per-row cost_amount = depleted_quantity × layer.unit_cost
        // (per-layer ROUND).
        // ============================================================
        let bad_cost_amount: Vec<(i64, String, String, i64)> = sqlx::query_as(
            "SELECT d.depletion_id,
                    d.depleted_quantity::TEXT,
                    cl.unit_cost::TEXT,
                    d.cost_amount
               FROM cost_layer_depletions d
               JOIN cost_layers cl
                 ON cl.layer_id = d.layer_id AND cl.receipt_date = d.layer_receipt_date
              WHERE d.cost_amount <> ROUND(d.depleted_quantity * cl.unit_cost)::BIGINT",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(
            bad_cost_amount.is_empty(),
            "[{label}] I3 violation: cost_amount ≠ ROUND(qty × unit_cost): {bad_cost_amount:?}"
        );

        // ============================================================
        // I4: residual ≥ 0 per layer.
        // ============================================================
        let neg_residual: Vec<(i64, String, String)> = sqlx::query_as(
            "SELECT cl.layer_id,
                    cl.original_quantity::TEXT,
                    COALESCE(
                      (SELECT SUM(d.depleted_quantity)::TEXT
                         FROM cost_layer_depletions d
                        WHERE d.layer_id = cl.layer_id
                          AND d.layer_receipt_date = cl.receipt_date),
                      '0'
                    )
               FROM cost_layers cl
              WHERE cl.product_id = $1::UUID
                AND cl.original_quantity < COALESCE(
                  (SELECT SUM(d.depleted_quantity)
                     FROM cost_layer_depletions d
                    WHERE d.layer_id = cl.layer_id
                      AND d.layer_receipt_date = cl.receipt_date),
                  0
                )",
        )
        .bind(&s.sku)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(
            neg_residual.is_empty(),
            "[{label}] I4 violation — over-consumed layers: {neg_residual:?}"
        );

        // ============================================================
        // I5: posting_line.amount on issue = SUM(depletions cost_amount).
        // ============================================================
        for pl_id in &applied_issues {
            let pl_amount: i64 =
                sqlx::query_scalar("SELECT amount FROM posting_lines WHERE id = $1")
                    .bind(pl_id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            let depl_sum: i64 = sqlx::query_scalar(
                "SELECT COALESCE(SUM(cost_amount), 0)::BIGINT
                   FROM cost_layer_depletions WHERE posting_line_id = $1",
            )
            .bind(pl_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(
                pl_amount, depl_sum,
                "[{label}] I5 violation (pl={pl_id}): amount {pl_amount} ≠ SUM(depletions) {depl_sum}"
            );
        }

        // ============================================================
        // I6: cost_layers count matches receipt count exactly.
        // The append-only trigger blocks UPDATE/DELETE; no path other
        // than apply_event INSERT can change the count. This is
        // largely the same proof as I1, but stating it as immutability
        // closes the contract.
        // ============================================================
        let final_layer_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::BIGINT FROM cost_layers WHERE product_id = $1::UUID",
        )
        .bind(&s.sku)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            final_layer_count, applied_receipts,
            "[{label}] I6: end-of-case layer count {final_layer_count} drifted from \
             applied receipts {applied_receipts} — append-only trigger may have failed"
        );

        // ============================================================
        // I7: pli.cost_layer_id on issue points at a consumed layer.
        // ============================================================
        for pl_id in &applied_issues {
            let stamped: Option<i64> = sqlx::query_scalar(
                "SELECT cost_layer_id FROM posting_line_inventory WHERE posting_line_id = $1",
            )
            .bind(pl_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert!(
                stamped.is_some(),
                "[{label}] I7 (pl={pl_id}): cost_layer_id must be stamped on issues"
            );
            let consumed: Vec<i64> = sqlx::query_scalar(
                "SELECT layer_id FROM cost_layer_depletions WHERE posting_line_id = $1",
            )
            .bind(pl_id)
            .fetch_all(&pool)
            .await
            .unwrap();
            assert!(
                consumed.contains(&stamped.unwrap()),
                "[{label}] I7 (pl={pl_id}): stamped layer_id {} not in consumed set {consumed:?}",
                stamped.unwrap()
            );
        }
    }
}
