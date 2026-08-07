//! Exact-mode reference-oracle differential (acct-0at4.4, design-v3.1 §9.2).
//!
//! Drives the SAME operation sequence through the independent `ledger-oracle`
//! model AND through the real `ledger_submit_trx_c`, asserting **byte-for-byte**
//! field equality — aggregate `(qty, unit_cost, value_sum)`, every `trx_line`
//! `(qty, unit_cost, source_trx_line_id)`, every `posting_line`
//! `(event_type, amount, debit, credit)`, layer count, AND error-for-error
//! (which `LedgerError` surfaces, matched on the SQL message).
//!
//! This is strictly stronger than `acceptance_direct_methods.rs` (which checks the
//! SQL path against hand-written expectations) and than the §9.4 direct-vs-routed
//! diff (which shares `ledger-core` with both flavors and so cannot catch a bug
//! both inherit). The model re-derives §3 from the spec with independent
//! half-to-even rounding, so a `ledger-core` costing/rounding/ordering bug shows
//! up here as a model-vs-SQL disagreement.
//!
//! `#[ignore]` like its siblings: needs `poc_v3_1` with `ledger_direct_c`
//! installed. Run with `--ignored --test-threads=1`.

mod common;

use std::collections::BTreeSet;

use common::*;
use ledger_core::{LineType, PoolMethod, ProvisionalBasis, TrxLineRequest};
use ledger_oracle::{uniform_accounts, ExpectedSource, ModelLedger};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use sqlx::PgPool;

/// One submitted line: pool, operation kind, signed qty, asserted cost.
#[derive(Clone, Copy)]
struct LineSpec {
    pool_id: i64,
    line_type: LineType,
    qty: i64,
    unit_cost: i64,
}

fn receipt_line(pool_id: i64, qty: i64, unit_cost: i64) -> LineSpec {
    LineSpec { pool_id, line_type: LineType::PoReceiptLine, qty, unit_cost }
}
fn deplete_line(pool_id: i64, qty: i64) -> LineSpec {
    LineSpec { pool_id, line_type: LineType::TransferShipmentLine, qty: -qty, unit_cost: 0 }
}

fn method_of(s: &str) -> PoolMethod {
    match s {
        "fifo" => PoolMethod::Fifo,
        "lifo" => PoolMethod::Lifo,
        "wac" => PoolMethod::Wac,
        "std" => PoolMethod::Std,
        "specific" => PoolMethod::Specific,
        other => panic!("unknown method {other}"),
    }
}
fn basis_of(s: &str) -> ProvisionalBasis {
    match s {
        "running_avg" => ProvisionalBasis::RunningAvg,
        "standard" => ProvisionalBasis::Standard,
        other => panic!("unknown basis {other}"),
    }
}

/// Runs both implementations in lockstep and diffs after every submission.
struct Diff {
    pool: PgPool,
    model: ModelLedger,
    /// Real `trx_line.id`s of every successful line, in global insertion order —
    /// the model's flat ordinals index into this to resolve specific links.
    line_ids: Vec<i64>,
}

impl Diff {
    async fn new() -> Self {
        let pool = connect_pool().await;
        reset_state(&pool).await;
        Diff { pool, model: ModelLedger::new(), line_ids: Vec::new() }
    }

    /// Seed a pool in the DB and the model with matching config. `accounts=false`
    /// deletes the `posting_account_map` row (models a missing mapping);
    /// `standard_cost` seeds both sides when `Some`.
    #[allow(clippy::too_many_arguments)]
    async fn seed(
        &mut self,
        pool_id: i64,
        sku: i64,
        loc: i64,
        method: &str,
        basis: &str,
        accounts: bool,
        standard_cost: Option<i64>,
    ) -> Fixture {
        let f = seed_pool(&self.pool, pool_id, sku, loc, method, basis).await;
        if let Some(sc) = standard_cost {
            seed_standard_cost(&self.pool, sku, loc, sc).await;
        }
        let acc = if accounts {
            Some(uniform_accounts(f.inv_acct, f.ap_acct, f.var_acct))
        } else {
            sqlx::query("DELETE FROM posting_account_map WHERE sku_id = $1 AND location_id = $2")
                .bind(sku)
                .bind(loc)
                .execute(&self.pool)
                .await
                .expect("delete posting_account_map");
            None
        };
        self.model.add_pool(pool_id, sku, loc, method_of(method), basis_of(basis), acc, standard_cost);
        f
    }

    /// Submit one trx through both, then assert byte-for-byte agreement.
    async fn submit(&mut self, trx_type: &str, source_id: i64, specs: &[LineSpec]) {
        let reqs: Vec<TrxLineRequest> = specs
            .iter()
            .map(|s| TrxLineRequest {
                pool_id: s.pool_id,
                line_type: s.line_type,
                source_id: None,
                qty: s.qty,
                unit_cost: s.unit_cost,
            })
            .collect();
        let json_lines: Vec<serde_json::Value> = specs
            .iter()
            .map(|s| {
                serde_json::json!({
                    "pool_id": s.pool_id,
                    "line_type": s.line_type.as_sql(),
                    "qty": s.qty,
                    "unit_cost": s.unit_cost,
                })
            })
            .collect();

        let model_res = self.model.apply(&reqs);
        let sql_res = submit(&self.pool, trx_type, source_id, json_lines).await;

        match model_res {
            Err(e) => {
                let sql_err = sql_res.err().unwrap_or_else(|| {
                    panic!("model predicted {e:?} but ledger_submit_trx_c succeeded (source_id {source_id})")
                });
                let msg = sql_err.to_string();
                assert!(
                    msg.contains(&e.message()),
                    "error-for-error mismatch (source_id {source_id}): SQL said '{msg}', model expected substring '{}'",
                    e.message()
                );
            }
            Ok(applied) => {
                let trx_id = sql_res.unwrap_or_else(|err| {
                    panic!("model succeeded but ledger_submit_trx_c erred (source_id {source_id}): {err}")
                });

                // trx_line rows, in ascending id order.
                let db_lines = trx_lines(&self.pool, trx_id).await;
                assert_eq!(
                    db_lines.len(),
                    applied.lines.len(),
                    "trx_line count (source_id {source_id})"
                );
                for (i, (exp, (dq, duc, dsrc))) in
                    applied.lines.iter().zip(db_lines.iter()).enumerate()
                {
                    assert_eq!(*dq, exp.qty, "line {i} qty (source_id {source_id})");
                    assert_eq!(*duc, exp.unit_cost, "line {i} unit_cost (source_id {source_id})");
                    match exp.source {
                        ExpectedSource::Null => {
                            assert_eq!(*dsrc, None, "line {i} source_trx_line_id (source_id {source_id})")
                        }
                        ExpectedSource::ReceiptFlatIndex(k) => assert_eq!(
                            *dsrc,
                            Some(self.line_ids[k]),
                            "line {i} specific source link (source_id {source_id})"
                        ),
                    }
                }

                // Record the real ids (same ascending-id order the model streams).
                let ids: Vec<i64> =
                    sqlx::query_scalar("SELECT id FROM trx_line WHERE trx_id = $1 ORDER BY id")
                        .bind(trx_id)
                        .fetch_all(&self.pool)
                        .await
                        .expect("read trx_line ids");
                assert_eq!(ids.len(), applied.lines.len());
                self.line_ids.extend(ids);
                assert_eq!(
                    self.line_ids.len(),
                    self.model.lines_emitted(),
                    "flat-ordinal stream drift (source_id {source_id})"
                );

                // posting_line rows, in ascending id order.
                let db_posts = posting_lines(&self.pool, trx_id).await;
                assert_eq!(
                    db_posts.len(),
                    applied.postings.len(),
                    "posting_line count (source_id {source_id})"
                );
                for (i, (exp, (et, amt, deb, cred))) in
                    applied.postings.iter().zip(db_posts.iter()).enumerate()
                {
                    assert_eq!(et, exp.event_type, "posting {i} event_type (source_id {source_id})");
                    assert_eq!(*amt, exp.amount, "posting {i} amount (source_id {source_id})");
                    assert_eq!(*deb, exp.debit_account, "posting {i} debit (source_id {source_id})");
                    assert_eq!(*cred, exp.credit_account, "posting {i} credit (source_id {source_id})");
                }
            }
        }

        // Aggregate + layer count agree for every touched pool, on both success
        // and (unchanged) error paths.
        let touched: BTreeSet<i64> = specs.iter().map(|s| s.pool_id).collect();
        for p in touched {
            let db_agg = aggregate_with_value(&self.pool, p).await;
            let model_agg = self.model.aggregate(p).map(|a| (a.qty, a.unit_cost, a.value_sum));
            assert_eq!(db_agg, model_agg, "aggregate mismatch on pool {p} (source_id {source_id})");
            let db_layers = layer_count(&self.pool, p).await;
            assert_eq!(
                db_layers as usize,
                self.model.layer_count(p),
                "layer_count mismatch on pool {p} (source_id {source_id})"
            );
        }
    }
}

// ── §9.2 scenarios, driven differentially ──────────────────────────────────

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn fifo_running_average_then_provisional_depletion() {
    let mut d = Diff::new().await;
    d.seed(1, 1, 1, "fifo", "running_avg", true, None).await;
    d.submit("po_receipt", 1, &[receipt_line(1, 10, 100)]).await;
    d.submit("po_receipt", 2, &[receipt_line(1, 10, 200)]).await;
    d.submit("transfer_shipment", 3, &[deplete_line(1, 5)]).await;
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn wac_matches_fifo_provisional() {
    let mut d = Diff::new().await;
    d.seed(1, 1, 1, "wac", "running_avg", true, None).await;
    d.submit("po_receipt", 1, &[receipt_line(1, 10, 100)]).await;
    d.submit("po_receipt", 2, &[receipt_line(1, 10, 200)]).await;
    d.submit("transfer_shipment", 3, &[deplete_line(1, 5)]).await;
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn two_lines_same_pool_one_submission_coalesces() {
    let mut d = Diff::new().await;
    d.seed(1, 1, 1, "fifo", "running_avg", true, None).await;
    d.submit("po_receipt", 1, &[receipt_line(1, 10, 100), receipt_line(1, 10, 200)]).await;
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn std_receipt_variance_then_depletion() {
    let mut d = Diff::new().await;
    d.seed(1, 1, 1, "std", "running_avg", true, Some(100)).await;
    d.submit("po_receipt", 1, &[receipt_line(1, 10, 120)]).await; // unfavorable variance
    d.submit("po_receipt", 2, &[receipt_line(1, 5, 80)]).await; // favorable variance
    d.submit("transfer_shipment", 3, &[deplete_line(1, 4)]).await;
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn std_without_standard_cost_raises() {
    let mut d = Diff::new().await;
    d.seed(1, 1, 1, "std", "running_avg", true, None).await;
    d.submit("po_receipt", 1, &[receipt_line(1, 10, 120)]).await; // model + SQL both raise
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn missing_posting_account_map_raises() {
    let mut d = Diff::new().await;
    d.seed(1, 1, 1, "fifo", "running_avg", false, None).await;
    d.submit("po_receipt", 1, &[receipt_line(1, 10, 100)]).await;
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn insufficient_inventory_raises() {
    let mut d = Diff::new().await;
    d.seed(1, 1, 1, "wac", "running_avg", true, None).await;
    d.submit("po_receipt", 1, &[receipt_line(1, 3, 100)]).await;
    d.submit("transfer_shipment", 2, &[deplete_line(1, 5)]).await; // 5 > 3
    // Rolled back on both sides — a subsequent legal op still agrees.
    d.submit("transfer_shipment", 3, &[deplete_line(1, 2)]).await;
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn specific_receipt_depletion_link_and_k1() {
    let mut d = Diff::new().await;
    d.seed(1, 1, 1, "specific", "running_avg", true, None).await;
    d.submit("po_receipt", 1, &[receipt_line(1, 1, 500)]).await; // flat ordinal 0
    d.submit("po_receipt", 2, &[receipt_line(1, 1, 700)]).await; // K=1 rejection
    d.submit("transfer_shipment", 3, &[deplete_line(1, 1)]).await; // links ordinal 0
    d.submit("transfer_shipment", 4, &[deplete_line(1, 1)]).await; // empty → insufficient
    d.submit("po_receipt", 5, &[receipt_line(1, 1, 700)]).await; // re-stock
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn fifo_standard_basis_over_book_negative_value_sum() {
    let mut d = Diff::new().await;
    d.seed(1, 1, 1, "fifo", "standard", true, Some(150)).await;
    d.submit("po_receipt", 1, &[receipt_line(1, 10, 100)]).await;
    // Over-book: 7 @ standard 150 = 1050 > 1000 book → value_sum −50 at qty 3.
    d.submit("transfer_shipment", 2, &[deplete_line(1, 7)]).await;
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn fifo_standard_basis_missing_cost_raises_on_depletion() {
    let mut d = Diff::new().await;
    d.seed(1, 1, 1, "fifo", "standard", true, None).await;
    d.submit("po_receipt", 1, &[receipt_line(1, 10, 100)]).await; // receipt ok
    d.submit("transfer_shipment", 2, &[deplete_line(1, 5)]).await; // depletion raises
}

// ── Randomized differential — the independence payoff ───────────────────────

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn randomized_mixed_methods_sequence_stays_byte_identical() {
    // A long deterministic sequence across mixed methods, single-line submissions,
    // with depletions that genuinely under-run (exercising error-for-error and the
    // running-average evolution under banker rounding). Every submission is diffed
    // byte-for-byte; any ledger-core costing/rounding/ordering bug the SQL path
    // inherits diverges from the independent model here.
    let mut d = Diff::new().await;
    // Four aggregate pools with distinct cost behavior.
    d.seed(1, 1, 1, "wac", "running_avg", true, None).await;
    d.seed(2, 2, 1, "fifo", "running_avg", true, None).await;
    d.seed(3, 3, 1, "fifo", "standard", true, Some(500)).await;
    d.seed(4, 4, 1, "std", "running_avg", true, Some(250)).await;
    let pools = [1i64, 2, 3, 4];

    let mut rng = StdRng::seed_from_u64(0x0A7_04_04);
    let n_ops = std::env::var("ORACLE_OPS").ok().and_then(|s| s.parse().ok()).unwrap_or(300u32);
    for source_id in 1..=(n_ops as i64) {
        let pool_id = pools[rng.random_range(0..pools.len())];
        // 45% depletions — frequent enough to force real under-runs.
        let (spec, trx_type) = if rng.random_bool(0.45) {
            (deplete_line(pool_id, rng.random_range(1..=30)), "transfer_shipment")
        } else {
            (receipt_line(pool_id, rng.random_range(1..=40), rng.random_range(1..=1000)), "po_receipt")
        };
        d.submit(trx_type, source_id, &[spec]).await;
    }
}
