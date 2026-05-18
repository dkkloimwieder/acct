//! E3 (acct-gx1z.1.3) property test: malformed payloads route to
//! state='failed' with error_code='payload_parse_error'.
//!
//! Pre-fix: `read_event_from_staging` defaulted missing/malformed
//! fields to 0 via `.and_then(|v| v.as_i64()).unwrap_or(0)`. A
//! payload missing `sku_id` silently produced an envelope with
//! `sku_id=0`, routed to pool (0, 0). If a sentinel SKU 0 ever
//! existed, downstream FK joins would pass and the envelope would
//! commit with corrupt-but-plausible data.
//!
//! Post-fix: universally-required fields (sku_id, location_id, qty,
//! doc_chrono, document_id; plus wo_id/op_id and component / output
//! triples in wo_complete) use `.ok_or_else(...)?`. Missing or
//! non-integer fields surface as `Err(String)` that the caller
//! routes through the parse_errors loop into `submission_status`
//! as `state='failed'`, `error_code='payload_parse_error'`,
//! `error_detail = {"phase":"payload_parse","detail":<msg>}`.
//!
//! Run via:
//!   cargo test --release --test property_v21_payload_parse_fail_loud \
//!     --features pg18,test_hooks --no-default-features -- --ignored --nocapture
//!
//! PROPTEST_CASES default 60; each case enqueues one malformed
//! envelope and waits for terminal status.

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state};
use proptest::prelude::*;
use sqlx::PgPool;
use sqlx::Row;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Clone)]
enum Malformation {
    PoReceiptNoSku,
    PoReceiptNoLocation,
    PoReceiptNoQty,
    PoReceiptNoDocChrono,
    PoReceiptNoDocumentId,
    PoReceiptSkuString,
    PoReceiptQtyString,
    WoCompleteNoWip,
    WoCompleteShortWip,
    WoCompleteEmptyComponents,
    WoCompleteWoIdString,
    WoCompleteOutputSkuString,
    WoCompleteComponentQtyString,
}

fn malformation_strategy() -> impl Strategy<Value = Malformation> {
    prop_oneof![
        Just(Malformation::PoReceiptNoSku),
        Just(Malformation::PoReceiptNoLocation),
        Just(Malformation::PoReceiptNoQty),
        Just(Malformation::PoReceiptNoDocChrono),
        Just(Malformation::PoReceiptNoDocumentId),
        Just(Malformation::PoReceiptSkuString),
        Just(Malformation::PoReceiptQtyString),
        Just(Malformation::WoCompleteNoWip),
        Just(Malformation::WoCompleteShortWip),
        Just(Malformation::WoCompleteEmptyComponents),
        Just(Malformation::WoCompleteWoIdString),
        Just(Malformation::WoCompleteOutputSkuString),
        Just(Malformation::WoCompleteComponentQtyString),
    ]
}

fn build_payload(
    malformation: &Malformation,
    doc_chrono: i64,
) -> (String, serde_json::Value, serde_json::Value) {
    use Malformation::*;
    let well_formed_pool_keys = serde_json::json!({ "sku": [[1, 1]], "wip": [] });
    match malformation {
        PoReceiptNoSku => (
            "po_receipt".to_string(),
            serde_json::json!({
                "location_id": 1, "qty": 5, "unit_cost": 100,
                "doc_chrono": doc_chrono, "document_id": 7_000_000 + doc_chrono,
            }),
            well_formed_pool_keys,
        ),
        PoReceiptNoLocation => (
            "po_receipt".to_string(),
            serde_json::json!({
                "sku_id": 1, "qty": 5, "unit_cost": 100,
                "doc_chrono": doc_chrono, "document_id": 7_000_000 + doc_chrono,
            }),
            well_formed_pool_keys,
        ),
        PoReceiptNoQty => (
            "po_receipt".to_string(),
            serde_json::json!({
                "sku_id": 1, "location_id": 1, "unit_cost": 100,
                "doc_chrono": doc_chrono, "document_id": 7_000_000 + doc_chrono,
            }),
            well_formed_pool_keys,
        ),
        PoReceiptNoDocChrono => (
            "po_receipt".to_string(),
            serde_json::json!({
                "sku_id": 1, "location_id": 1, "qty": 5, "unit_cost": 100,
                "document_id": 7_000_000 + doc_chrono,
            }),
            well_formed_pool_keys,
        ),
        PoReceiptNoDocumentId => (
            "po_receipt".to_string(),
            serde_json::json!({
                "sku_id": 1, "location_id": 1, "qty": 5, "unit_cost": 100,
                "doc_chrono": doc_chrono,
            }),
            well_formed_pool_keys,
        ),
        PoReceiptSkuString => (
            "po_receipt".to_string(),
            serde_json::json!({
                "sku_id": "not-an-int", "location_id": 1, "qty": 5, "unit_cost": 100,
                "doc_chrono": doc_chrono, "document_id": 7_000_000 + doc_chrono,
            }),
            well_formed_pool_keys,
        ),
        PoReceiptQtyString => (
            "po_receipt".to_string(),
            serde_json::json!({
                "sku_id": 1, "location_id": 1, "qty": "five", "unit_cost": 100,
                "doc_chrono": doc_chrono, "document_id": 7_000_000 + doc_chrono,
            }),
            well_formed_pool_keys,
        ),
        WoCompleteNoWip => (
            "wo_complete".to_string(),
            serde_json::json!({
                "components": [[1, 1, 5]],
                "output": [2, 1, 1],
                "doc_chrono": doc_chrono, "document_id": 7_000_000 + doc_chrono,
            }),
            serde_json::json!({ "sku": [[1, 1], [2, 1]], "wip": [[10, 100]] }),
        ),
        WoCompleteShortWip => (
            "wo_complete".to_string(),
            serde_json::json!({
                "wip_account": [10],
                "components": [[1, 1, 5]],
                "output": [2, 1, 1],
                "doc_chrono": doc_chrono, "document_id": 7_000_000 + doc_chrono,
            }),
            serde_json::json!({ "sku": [[1, 1], [2, 1]], "wip": [[10, 100]] }),
        ),
        WoCompleteEmptyComponents => (
            "wo_complete".to_string(),
            serde_json::json!({
                "wip_account": [10, 100],
                "components": [],
                "output": [2, 1, 1],
                "doc_chrono": doc_chrono, "document_id": 7_000_000 + doc_chrono,
            }),
            serde_json::json!({ "sku": [[2, 1]], "wip": [[10, 100]] }),
        ),
        WoCompleteWoIdString => (
            "wo_complete".to_string(),
            serde_json::json!({
                "wip_account": ["wo-not-int", 100],
                "components": [[1, 1, 5]],
                "output": [2, 1, 1],
                "doc_chrono": doc_chrono, "document_id": 7_000_000 + doc_chrono,
            }),
            serde_json::json!({ "sku": [[1, 1], [2, 1]], "wip": [[10, 100]] }),
        ),
        WoCompleteOutputSkuString => (
            "wo_complete".to_string(),
            serde_json::json!({
                "wip_account": [10, 100],
                "components": [[1, 1, 5]],
                "output": ["not-int", 1, 1],
                "doc_chrono": doc_chrono, "document_id": 7_000_000 + doc_chrono,
            }),
            serde_json::json!({ "sku": [[1, 1]], "wip": [[10, 100]] }),
        ),
        WoCompleteComponentQtyString => (
            "wo_complete".to_string(),
            serde_json::json!({
                "wip_account": [10, 100],
                "components": [[1, 1, "not-int"]],
                "output": [2, 1, 1],
                "doc_chrono": doc_chrono, "document_id": 7_000_000 + doc_chrono,
            }),
            serde_json::json!({ "sku": [[1, 1], [2, 1]], "wip": [[10, 100]] }),
        ),
    }
}

async fn wait_for_status(
    pool: &PgPool,
    cid: Uuid,
    timeout: Duration,
) -> Option<(String, Option<String>)> {
    let start = Instant::now();
    loop {
        let row = sqlx::query(
            "SELECT state::text, error_code::text \
               FROM poc_v21_submission_status \
              WHERE correlation_id = $1",
        )
        .bind(cid)
        .fetch_optional(pool)
        .await
        .expect("query submission_status");

        if let Some(r) = row {
            let state: String = r.get(0);
            let error_code: Option<String> = r.get(1);
            if state == "failed" || state == "committed" || state == "replayed" {
                return Some((state, error_code));
            }
        }
        if start.elapsed() > timeout {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn run_scenario(pool: &PgPool, malformation: Malformation) -> Result<(), String> {
    reset_state(pool).await;
    let (event_type, payload, pool_keys) = build_payload(&malformation, 1);
    let cid = Uuid::new_v4();

    sqlx::query("SELECT poc_v21_enqueue($1::uuid, $2, $3::jsonb, $4::jsonb, false)")
        .bind(cid)
        .bind(&event_type)
        .bind(&payload)
        .bind(&pool_keys)
        .execute(pool)
        .await
        .map_err(|e| format!("enqueue: {e}"))?;

    let (state, error_code) = wait_for_status(pool, cid, Duration::from_secs(5))
        .await
        .ok_or_else(|| format!("malformation {malformation:?} did not reach terminal state in 5s"))?;

    if state != "failed" {
        return Err(format!(
            "malformation {malformation:?} should route to state='failed', got '{state}'"
        ));
    }
    if error_code.as_deref() != Some("payload_parse_error") {
        return Err(format!(
            "malformation {malformation:?} should carry error_code='payload_parse_error', got {error_code:?}"
        ));
    }
    Ok(())
}

#[test]
#[ignore]
fn prop_v21_payload_parse_fail_loud() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let pool = runtime.block_on(connect_pool());

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let mut runner = proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        cases,
        max_shrink_iters: 0,
        ..proptest::test_runner::Config::default()
    });
    let strategy = malformation_strategy();
    runner
        .run(&strategy, |malformation| {
            let pool = pool.clone();
            runtime
                .block_on(async move { run_scenario(&pool, malformation).await })
                .map_err(|e| proptest::test_runner::TestCaseError::fail(e))?;
            Ok(())
        })
        .expect("proptest run");
}
