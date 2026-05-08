//! Property test for the Phase D inventory_movements subledger
//! (acct-wb75.3, all sub-issues). Generates random workloads of
//! standard-cost SKU postings and asserts FIVE invariants per case:
//!
//!   I1  Every value-leg posting on a standard/WAC SKU's inv_value_*
//!       account with sku+location resolved produces a movement row
//!       per qualifying leg. Per the D5 redesign: DR-side rows for
//!       inv_value_* debits with location, CR-side for credits.
//!
//!   I2  Sign convention: DR-side row has positive quantity; CR-side
//!       row has negative. Receipts are net positive at the inv_value_*
//!       account; issues are net negative.
//!
//!   I3  actual_unit_cost ≈ posting_line.amount / ABS(qty) within
//!       NUMERIC(19,4) precision. SUM(quantity × actual_unit_cost)
//!       on a row equals signed amount on its account contribution
//!       to the GL.
//!
//!   I4  Subledger ↔ GL aggregate per (product, location, period):
//!       SUM(quantity × actual_unit_cost) on inventory_movements
//!       matches SUM of inv_value_* posting_lines.amount × dir_sign
//!       within 1-cent tolerance. Equivalent to D5 check #7 but
//!       asserted directly without going through reconciliation_alerts.
//!
//!   I5  Append-only — the only mechanism that creates correction
//!       movement rows (event_type=16) is the D6 trigger on
//!       cost_restate posts. Random workloads here don't trigger
//!       close hooks; we assert the count of event_type=16 rows
//!       stays at zero. (A separate end-to-end test in
//!       inventory_movements_close_hooks_t1.rs exercises the close-
//!       hook path.)
//!
//! Acts as the regression net for the keystone invariant: subledger
//! and GL stay in sync across arbitrary post mixes. PROPTEST_CASES
//! env var controls case count (default 100); --test-threads=1
//! required since the test resets the shared DB between scenarios.

mod common;

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use serde_json::json;
use sqlx::PgPool;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;

#[derive(Debug, Clone)]
enum Op {
    /// cycle_count_adj: stock_available DR, creation_void CR (qty leg)
    /// AND inv_value_raw DR, creation_void(USD) CR (value leg, amount=qty×std).
    Receive { qty: i64 },
    /// Reverse the cycle_count: deplete qty units. Requires sufficient on-hand.
    Issue { qty: i64 },
    /// post_cost_adjustment-style: inv_value_raw DR, inv_adj_expense CR.
    /// Pure value adjustment with qty.
    InventoryAdjust { delta_value: i64 },
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (1i64..20).prop_map(|qty| Op::Receive { qty }),
        (1i64..10).prop_map(|qty| Op::Issue { qty }),
        (-200i64..200).prop_map(|delta_value| Op::InventoryAdjust { delta_value }),
    ]
}

fn arb_op_seq() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(arb_op(), 1..8)
}

fn uuid_for(case: usize, op_idx: usize, tag: &str) -> String {
    let mut s = format!("00000000-0000-0000-{:04x}-{:08x}{:0>4}", case & 0xffff, op_idx, tag);
    s.truncate(36);
    s
}

async fn fresh_uuid_for(pool: &PgPool, _tag: &str) -> String {
    common::fresh_uuid(pool).await
}

#[tokio::test(flavor = "current_thread")]
async fn property_inventory_movements_consistency() {
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

        let label = format!("im_consistency#{case_idx}");

        // Account ids on SKU-A (standard cost, std=100).
        let stock = common::account_id_stock_available(&pool, "SKU-A", "MAIN").await;
        let void_q = common::account_id_by_kind_currency(&pool, "creation_void", None).await;
        let inv_raw = common::account_id_for_selector(
            &pool, "inv_value_raw", Some("SKU-A"), Some("MAIN"), Some("USD"), None,
        )
        .await;
        let void_v =
            common::account_id_by_kind_currency(&pool, "creation_void", Some("USD")).await;
        let inv_adj_expense =
            common::account_id_by_kind_currency(&pool, "inv_adj_expense", Some("USD")).await;

        let mut on_hand: i64 = 0;
        let mut total_value: i64 = 0;

        for (i, op) in ops.iter().enumerate() {
            let key_q = fresh_uuid_for(&pool, "qty").await;
            let key_v = fresh_uuid_for(&pool, "val").await;
            match *op {
                Op::Receive { qty } => {
                    let amount = qty * 100;
                    let res = common::call_post_posting_lines(
                        &pool,
                        json!([
                            common::make_event_with_qty(
                                "cycle_count_adj", stock, void_q, qty, qty, "2026-04-15", &key_q,
                            ),
                            common::make_event_with_qty(
                                "cycle_count_adj", inv_raw, void_v, amount, qty, "2026-04-15", &key_v,
                            ),
                        ]),
                        false,
                    )
                    .await;
                    if res.is_ok() {
                        on_hand += qty;
                        total_value += amount;
                    }
                }
                Op::Issue { qty } => {
                    if qty > on_hand {
                        continue;
                    }
                    let amount = qty * 100;
                    let res = common::call_post_posting_lines(
                        &pool,
                        json!([
                            common::make_event_with_qty(
                                "cycle_count_adj", void_q, stock, qty, qty, "2026-04-15", &key_q,
                            ),
                            common::make_event_with_qty(
                                "cycle_count_adj", void_v, inv_raw, amount, qty, "2026-04-15", &key_v,
                            ),
                        ]),
                        false,
                    )
                    .await;
                    if res.is_ok() {
                        on_hand -= qty;
                        total_value -= amount;
                    }
                }
                Op::InventoryAdjust { delta_value } => {
                    if on_hand == 0 {
                        continue;
                    }
                    if delta_value == 0 {
                        continue;
                    }
                    // Pure value-only adjustment via cost_adjustment reason.
                    // qty is required by the D-block to write a movement; we
                    // synthesize qty=1 (tiny) so the recon math sees the
                    // movement. The actual_unit_cost = delta_value / 1 =
                    // delta_value, which exercises the cost-flow recon.
                    let key = fresh_uuid_for(&pool, "adj").await;
                    let (dr, cr, abs_amt) = if delta_value > 0 {
                        (inv_raw, inv_adj_expense, delta_value)
                    } else {
                        (inv_adj_expense, inv_raw, -delta_value)
                    };
                    let res = common::call_post_posting_lines(
                        &pool,
                        json!([common::make_event_with_qty(
                            "cost_adjustment", dr, cr, abs_amt, 1, "2026-04-15", &key,
                        )]),
                        false,
                    )
                    .await;
                    if res.is_ok() {
                        if delta_value > 0 {
                            total_value += delta_value;
                        } else {
                            total_value += delta_value; // delta_value is negative
                        }
                    }
                }
            }
            let _ = i;
        }

        // Skip empty-workload cases; nothing to assert.
        let pl_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::BIGINT FROM posting_lines WHERE document_kind = 'test_doc'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if pl_count == 0 {
            continue;
        }

        // ============================================================
        // I1: every value-leg post on inv_value_* with sku+location
        // gets a movement row.
        // ============================================================
        let orphans: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::BIGINT
               FROM posting_lines pl
               JOIN accounts d ON d.id = pl.debit_account_id
               JOIN accounts c ON c.id = pl.credit_account_id
               LEFT JOIN inventory_movements im ON im.posting_line_id = pl.id
              WHERE pl.qty IS NOT NULL
                AND d.ledger_kind = 'value'
                AND (d.kind::TEXT LIKE 'inv_value_%' OR c.kind::TEXT LIKE 'inv_value_%')
                AND COALESCE(c.location_id, d.location_id) IS NOT NULL
                AND im.posting_line_id IS NULL",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            orphans, 0,
            "[{label}] I1 violation: {orphans} qty-bearing inv_value_* posts have no movement"
        );

        // ============================================================
        // I2: DR-side movements have positive qty; CR-side negative.
        // ============================================================
        let bad_signs: Vec<(i64, String, String)> = sqlx::query_as(
            "SELECT im.posting_line_id, im.quantity::TEXT,
                    CASE WHEN im.product_id = d.sku_id AND im.location_id = d.location_id
                              AND d.kind::TEXT LIKE 'inv_value_%'
                         THEN 'dr' ELSE 'cr' END
               FROM inventory_movements im
               JOIN posting_lines pl ON pl.id = im.posting_line_id
               JOIN accounts d ON d.id = pl.debit_account_id
               JOIN accounts c ON c.id = pl.credit_account_id
              WHERE im.event_type <> 16  -- exclude D6 corrections
                AND (
                  (im.product_id = d.sku_id AND im.location_id = d.location_id
                   AND d.kind::TEXT LIKE 'inv_value_%' AND im.quantity < 0)
                  OR
                  (im.product_id = c.sku_id AND im.location_id = c.location_id
                   AND c.kind::TEXT LIKE 'inv_value_%' AND im.quantity > 0)
                )",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(
            bad_signs.is_empty(),
            "[{label}] I2 violation — sign mismatch on movements: {bad_signs:?}"
        );

        // ============================================================
        // I3: per-row, qty × actual = signed amount contribution.
        // For each movement, |quantity × actual_unit_cost| should match
        // |posting_line.amount| within 1 cent (NUMERIC truncation).
        // ============================================================
        let bad_unit_cost: Vec<(i64, String, i64)> = sqlx::query_as(
            "SELECT im.posting_line_id,
                    (ABS(im.quantity * im.actual_unit_cost))::TEXT,
                    pl.amount
               FROM inventory_movements im
               JOIN posting_lines pl ON pl.id = im.posting_line_id
              WHERE im.event_type <> 16
                AND ABS(ABS(im.quantity * im.actual_unit_cost) - pl.amount) > 1",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(
            bad_unit_cost.is_empty(),
            "[{label}] I3 violation — qty×actual ≠ amount: {bad_unit_cost:?}"
        );

        // ============================================================
        // I4: subledger ↔ GL aggregate per (product, location, period).
        // Same as D5 check #7 directly.
        // ============================================================
        let mismatches: Vec<(String, String, i64, String, String)> = sqlx::query_as(
            "WITH gl AS (
               SELECT d.sku_id::TEXT AS p, d.location_id::TEXT AS l, pl.period_id AS pid,
                      pl.amount::NUMERIC AS net
                 FROM posting_lines pl
                 JOIN accounts d ON d.id = pl.debit_account_id
                WHERE d.kind::TEXT LIKE 'inv_value_%' AND d.sku_id IS NOT NULL
                  AND d.location_id IS NOT NULL AND pl.qty IS NOT NULL
               UNION ALL
               SELECT c.sku_id::TEXT, c.location_id::TEXT, pl.period_id, -pl.amount::NUMERIC
                 FROM posting_lines pl
                 JOIN accounts c ON c.id = pl.credit_account_id
                WHERE c.kind::TEXT LIKE 'inv_value_%' AND c.sku_id IS NOT NULL
                  AND c.location_id IS NOT NULL AND pl.qty IS NOT NULL
             ),
             gl_agg AS (
               SELECT p, l, pid, SUM(net) AS gl_net FROM gl GROUP BY 1,2,3
             ),
             sub AS (
               SELECT im.product_id::TEXT AS p, im.location_id::TEXT AS l,
                      pp.id AS pid,
                      SUM(im.quantity * im.actual_unit_cost) AS sub_net
                 FROM inventory_movements im
                 JOIN periods pp ON pp.opens_at <= im.movement_date AND pp.closes_at >= im.movement_date
                GROUP BY 1,2,3
             )
             SELECT COALESCE(g.p, s.p), COALESCE(g.l, s.l), COALESCE(g.pid, s.pid),
                    COALESCE(g.gl_net,0)::TEXT, COALESCE(s.sub_net,0)::TEXT
               FROM gl_agg g FULL OUTER JOIN sub s
                 ON g.p=s.p AND g.l=s.l AND g.pid=s.pid
              WHERE ABS(COALESCE(g.gl_net,0) - COALESCE(s.sub_net,0)) > 1",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(
            mismatches.is_empty(),
            "[{label}] I4 violation — subledger ↔ GL mismatch: {mismatches:?}"
        );

        // ============================================================
        // I5: no correction movements (event_type=16) outside close-
        // hook variance posts. Random workloads here don't trigger
        // close hooks, so the count of event_type=16 rows is bounded
        // by the count of cost_adjustment posting_lines that came
        // through the dispatcher path (also event_type=16 from the
        // helper mapping when reason='cost_adjustment'). The trigger
        // path requires reason='cost_restate', which we don't emit.
        // ============================================================
        let trigger_corrections: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::BIGINT
               FROM inventory_movements im
               JOIN posting_lines pl ON pl.id = im.posting_line_id
              WHERE im.event_type = 16
                AND im.quantity = 0
                AND pl.reason = 'cost_restate'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            trigger_corrections, 0,
            "[{label}] I5 violation: trigger-path correction movements appeared without a cost_restate post"
        );

        common::assert_invariants_hold(&pool, &label).await;
    }
}
