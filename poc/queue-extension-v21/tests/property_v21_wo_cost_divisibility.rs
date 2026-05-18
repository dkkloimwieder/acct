//! L1 / acct-gx1z.1.13: lock in the integer-division contract at
//! wo_complete's output unit-cost computation.
//!
//! `committer.rs:1148` computes per-output-unit cost as
//! `wo_cost_local / event.qty` (BIGINT integer division, truncation
//! toward zero). When qty doesn't divide wo_cost_local evenly, the
//! residual is dropped on the floor at the PoC layer. In the v0.2
//! ledger this residual surfaces at WO close via variance_wo_close.
//! The behavior is INTENTIONAL per the cost-allocation spec; this
//! test locks in the contract so any change is deliberate.
//!
//! Each scenario:
//!   - seed FIFO components with known qty × unit_cost so the total
//!     wo_cost_local at the output step is exact
//!   - enqueue a wo_complete whose output qty does NOT divide
//!     wo_cost_local
//!   - assert posted unit_cost = floor(wo_cost_local / output_qty)
//!   - assert posted total = output_qty × unit_cost (residual visible
//!     as |wo_cost_local - posted_total|)
//!
//! Run via:
//!   cargo test --release --test property_v21_wo_cost_divisibility \
//!     --features pg18,test_hooks --no-default-features -- --ignored --nocapture

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state, seed_method_assignments};
use sqlx::{PgPool, Row};
use std::time::{Duration, Instant};
use uuid::Uuid;

async fn seed_fifo_layer(pool: &PgPool, sku: i64, loc: i64, qty: i64, unit_cost: i64) {
    sqlx::query(
        "INSERT INTO poc_v21_cost_layers \
            (sku_id, location_id, qty, unit_cost, born_at, born_seq, source_kind, \
             correlation_id, user_tx_xid, committer_tx_id, superbatch_id) \
         VALUES ($1, $2, $3, $4, now() - interval '1 minute', 1, 'po_receipt', \
                 gen_random_uuid(), '1'::xid8, 1, 1)",
    )
    .bind(sku)
    .bind(loc)
    .bind(qty)
    .bind(unit_cost)
    .execute(pool)
    .await
    .expect("seed cost_layer");
}

async fn enqueue_wo_complete(
    pool: &PgPool,
    cid: Uuid,
    wo_id: i64,
    op_id: i64,
    components: &[(i64, i64, i64)],
    output: (i64, i64, i64),
    doc_chrono: i64,
) {
    let comps: Vec<serde_json::Value> = components
        .iter()
        .map(|(s, l, q)| serde_json::json!([s, l, q]))
        .collect();
    let payload = serde_json::json!({
        "wip_account": [wo_id, op_id],
        "components": comps,
        "output": [output.0, output.1, output.2],
        "doc_chrono": doc_chrono,
        "document_id": 8_800_000_i64 + doc_chrono,
    });
    let mut sku_keys: Vec<serde_json::Value> = components
        .iter()
        .map(|(s, l, _)| serde_json::json!([s, l]))
        .collect();
    sku_keys.push(serde_json::json!([output.0, output.1]));
    let pool_keys = serde_json::json!({ "sku": sku_keys, "wip": [[wo_id, op_id]] });
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, 'wo_complete', $2::jsonb, $3::jsonb, false)")
        .bind(cid)
        .bind(&payload)
        .bind(&pool_keys)
        .execute(pool)
        .await
        .expect("enqueue wo_complete");
}

async fn wait_for_committed(pool: &PgPool, cid: Uuid, timeout: Duration) -> Option<String> {
    let start = Instant::now();
    loop {
        let row = sqlx::query(
            "SELECT state::text \
               FROM poc_v21_submission_status \
              WHERE correlation_id = $1",
        )
        .bind(cid)
        .fetch_optional(pool)
        .await
        .expect("query submission_status");
        if let Some(r) = row {
            let state: String = r.get(0);
            if state == "failed" || state == "committed" || state == "replayed" {
                return Some(state);
            }
        }
        if start.elapsed() > timeout {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn query_output_posting(
    pool: &PgPool,
    cid: Uuid,
    output_sku: i64,
    output_loc: i64,
) -> (i64, i64) {
    let row = sqlx::query(
        "SELECT pl.amount, pli.qty \
           FROM poc_v21_posting_lines pl \
           JOIN poc_v21_posting_line_inventory pli USING (posting_line_id) \
          WHERE pl.correlation_id = $1 \
            AND pli.sku_id = $2 \
            AND pli.location_id = $3 \
            AND pli.qty > 0",
    )
    .bind(cid)
    .bind(output_sku)
    .bind(output_loc)
    .fetch_one(pool)
    .await
    .expect("query output posting_line");
    let amount: i64 = row.get(0);
    let qty: i64 = row.get(1);
    (amount, qty)
}

/// One divisibility scenario.
///   components: (sku, qty, unit_cost). All distinct SKUs.
///   output: (sku, qty) — must NOT collide with any component sku.
///
/// Asserts: posted unit_cost = floor(total_cost / output_qty); posted
/// amount = output_qty × posted_unit_cost; residual = total_cost -
/// posted_amount; 0 <= residual < output_qty.
async fn run_case(
    pool: &PgPool,
    label: &str,
    components: &[(i64, i64, i64)],
    output_sku: i64,
    output_qty: i64,
    wo_id: i64,
    doc_chrono: i64,
) {
    let loc = 1i64;

    // Validate caller passed distinct SKUs.
    let mut all_skus: Vec<i64> = components.iter().map(|(s, _, _)| *s).collect();
    all_skus.push(output_sku);
    let mut sorted = all_skus.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        sorted.len(), all_skus.len(),
        "[{label}] all SKUs (components + output) must be distinct: got {all_skus:?}"
    );

    let assignments: Vec<(i64, &str)> =
        all_skus.iter().map(|s| (*s, "fifo")).collect();
    seed_method_assignments(pool, &assignments).await;

    let mut total_cost: i64 = 0;
    for (sku, qty, unit_cost) in components {
        seed_fifo_layer(pool, *sku, loc, *qty, *unit_cost).await;
        total_cost += qty * unit_cost;
    }

    let expected_unit_cost = total_cost / output_qty;
    let expected_amount = output_qty * expected_unit_cost;
    let expected_residual = total_cost - expected_amount;

    let cid = Uuid::new_v4();
    let components_payload: Vec<(i64, i64, i64)> =
        components.iter().map(|(s, q, _)| (*s, loc, *q)).collect();
    enqueue_wo_complete(
        pool,
        cid,
        wo_id,
        100,
        &components_payload,
        (output_sku, loc, output_qty),
        doc_chrono,
    )
    .await;

    let state = wait_for_committed(pool, cid, Duration::from_secs(5))
        .await
        .unwrap_or_else(|| panic!("[{label}] no terminal status in 5s"));
    assert_eq!(state, "committed", "[{label}] expected committed, got {state}");

    let (actual_amount, actual_qty) =
        query_output_posting(pool, cid, output_sku, loc).await;
    assert_eq!(actual_qty, output_qty, "[{label}] output qty mismatch");
    assert_eq!(
        actual_amount, expected_amount,
        "[{label}] amount: expected {expected_amount} (={output_qty} × {expected_unit_cost}), got {actual_amount}; \
         total_cost={total_cost}, expected_residual={expected_residual}"
    );
    let actual_residual = total_cost - actual_amount;
    assert_eq!(actual_residual, expected_residual, "[{label}] residual mismatch");
    assert!(
        actual_residual >= 0,
        "[{label}] residual must be non-negative: got {actual_residual}"
    );
    assert!(
        actual_residual < output_qty,
        "[{label}] residual ({actual_residual}) must be < output_qty ({output_qty})"
    );
    println!(
        "[{label}] OK: total_cost={total_cost} ÷ output_qty={output_qty} → unit_cost={expected_unit_cost}, amount={expected_amount}, residual={expected_residual}"
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn wo_cost_residue_is_truncated_per_spec() {
    let pool = connect_pool().await;

    // Cases — each runs against a freshly-reset state so the seeded
    // FIFO layers don't interfere across cases. Distinct SKU numbering
    // per case so seed_method_assignments doesn't UPSERT the same row
    // twice within one call.
    let cases: &[(&str, &[(i64, i64, i64)], i64, i64, i64, i64)] = &[
        // (label, components: &[(sku, qty, unit_cost)], output_sku, output_qty, wo_id, doc_chrono)
        // total=10 / out=3 → unit=3, amount=9, residual=1
        ("A_10div3",         &[(100, 2, 5)],            101, 3,  10000, 100),
        // total=12 / out=5 → unit=2, amount=10, residual=2
        ("B_12div5",         &[(110, 3, 4)],            111, 5,  10001, 101),
        // total=15 / out=7 → unit=2, amount=14, residual=1
        ("C_15div7",         &[(120, 5, 3)],            121, 7,  10002, 102),
        // total=14 / out=11 → unit=1, amount=11, residual=3
        ("D_14div11",        &[(130, 7, 2)],            131, 11, 10003, 103),
        // total=143 / out=17 → unit=8, amount=136, residual=7 (single comp)
        ("E_143div17",       &[(140, 11, 13)],          141, 17, 10004, 104),
        // multi-component. total = 6*5 + 4*7 = 30+28 = 58 / out=9 → unit=6, amount=54, residual=4
        ("F_58div9_multi",   &[(150, 6, 5), (151, 4, 7)], 152, 9,  10005, 105),
        // divisible edge: total=20 / out=4 → unit=5, amount=20, residual=0 (no truncation, no false positive)
        ("G_20div4_clean",   &[(160, 4, 5)],            161, 4,  10006, 106),
    ];

    for (label, components, output_sku, output_qty, wo_id, doc_chrono) in cases {
        reset_state(&pool).await;
        run_case(
            &pool,
            label,
            components,
            *output_sku,
            *output_qty,
            *wo_id,
            *doc_chrono,
        )
        .await;
    }
}
