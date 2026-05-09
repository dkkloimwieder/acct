//! acct-wb75.4.1 — Phase F1: property test for post_journal_entry.
//!
//! Random sequences of JE batches against a pool of value-side
//! non-SKU accounts. Verifies:
//!
//! 1. Each successful batch produces 1 journal_entries header row
//!    and N journal_entry_lines rows (matching N posting_lines).
//! 2. Per-account net Δ matches expected (sum of debits - sum of
//!    credits across all batches).
//! 3. Idempotent replay returns the same doc id, no duplicate
//!    posting_lines or journal_entry_lines.
//! 4. Common::assert_invariants_hold after every successful batch
//!    (I1-I7).
//! 5. No inventory_movements / posting_line_inventory /
//!    posting_lines_provisional rows produced (journal_entry stays
//!    out of cost-event paths).
//! 6. Per-leg `posting_lines.qty` is NULL (JE is value-only).

mod common;

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use serde_json::json;
use sqlx::PgPool;
use std::collections::HashMap;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;

#[derive(Debug, Clone)]
struct JeLine {
    debit_idx: usize,
    credit_idx: usize,
    amount: i64,
}

#[derive(Debug, Clone)]
struct JeBatch {
    lines: Vec<JeLine>,
    replay: bool,
}

fn arb_line(num_d: usize, num_c: usize) -> impl Strategy<Value = JeLine> {
    (0..num_d, 0..num_c, 1i64..=100_000).prop_map(move |(d, c, amount)| JeLine {
        debit_idx: d,
        credit_idx: c,
        amount,
    })
}

fn arb_batch(num_d: usize, num_c: usize) -> impl Strategy<Value = JeBatch> {
    (
        proptest::collection::vec(arb_line(num_d, num_c), 1..=4),
        proptest::bool::weighted(0.15),
    )
        .prop_map(|(lines, replay)| JeBatch { lines, replay })
}

fn arb_seq(num_d: usize, num_c: usize) -> impl Strategy<Value = Vec<JeBatch>> {
    proptest::collection::vec(arb_batch(num_d, num_c), 5..=15)
}

const ACTOR: &str = "00000000-0000-0000-0000-000000000010";

async fn fresh_uuid(pool: &PgPool) -> String {
    sqlx::query_scalar("SELECT gen_random_uuid()::text")
        .fetch_one(pool)
        .await
        .expect("uuid")
}

async fn account_balance(pool: &PgPool, id: i64) -> (i64, i64) {
    sqlx::query_as("SELECT debits_total, credits_total FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

async fn account_pool_usd_debit_side(pool: &PgPool) -> Vec<i64> {
    // accounts that can absorb arbitrary debits without violating
    // accounts_check: debit-normal + unrestricted.
    sqlx::query_scalar(
        "SELECT id FROM accounts
          WHERE ledger_kind = 'value'
            AND currency = 'USD'
            AND sku_id IS NULL
            AND NOT is_closed
            AND normal_side IN ('debit', 'unrestricted')
          ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("debit pool")
}

async fn account_pool_usd_credit_side(pool: &PgPool) -> Vec<i64> {
    // accounts that can absorb arbitrary credits without violating
    // accounts_check: credit-normal + unrestricted.
    sqlx::query_scalar(
        "SELECT id FROM accounts
          WHERE ledger_kind = 'value'
            AND currency = 'USD'
            AND sku_id IS NULL
            AND NOT is_closed
            AND normal_side IN ('credit', 'unrestricted')
          ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("credit pool")
}

async fn call_je(
    pool: &PgPool,
    lines: serde_json::Value,
    business_date: &str,
    idempotency_key: &str,
) -> sqlx::Result<String> {
    sqlx::query_scalar(
        "SELECT post_journal_entry($1, $2::DATE, $3::UUID, $4::UUID, NULL)::TEXT",
    )
    .bind(lines)
    .bind(business_date)
    .bind(ACTOR)
    .bind(idempotency_key)
    .fetch_one(pool)
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn property_journal_entry_invariants_hold() {
    let pool = common::connect_test_db().await;

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(TEST_PROPTEST_CASES_DEFAULT);

    let mut runner = proptest::test_runner::TestRunner::default();

    for case_idx in 0..cases {
        common::reset_to_fixture(&pool).await;

        let dr_accts = account_pool_usd_debit_side(&pool).await;
        let cr_accts = account_pool_usd_credit_side(&pool).await;
        assert!(
            dr_accts.len() >= 2 && cr_accts.len() >= 2,
            "fixture must seed >= 2 USD value-side non-SKU accounts on each side"
        );
        let strategy = arb_seq(dr_accts.len(), cr_accts.len());
        let tree = strategy.new_tree(&mut runner).expect("strategy.new_tree");
        let batches: Vec<JeBatch> = tree.current();

        let label = format!("je#{case_idx}");

        // Per-account expected debits/credits delta tracking.
        let mut expected_d: HashMap<i64, i64> = HashMap::new();
        let mut expected_c: HashMap<i64, i64> = HashMap::new();

        // Snapshot starting balances so we measure the Δ this case adds.
        let mut start_d: HashMap<i64, i64> = HashMap::new();
        let mut start_c: HashMap<i64, i64> = HashMap::new();
        for a in dr_accts.iter().chain(cr_accts.iter()) {
            let (d, c) = account_balance(&pool, *a).await;
            start_d.insert(*a, d);
            start_c.insert(*a, c);
        }

        let mut docs_seen: Vec<(String, Vec<JeLine>)> = Vec::new();

        for (batch_idx, batch) in batches.iter().enumerate() {
            // Build lines payload. dr/cr come from disjoint pools, but
            // an unrestricted account can be in both (variance_*) — drop
            // any such collision.
            let lines_payload: Vec<serde_json::Value> = batch
                .lines
                .iter()
                .filter(|l| dr_accts[l.debit_idx] != cr_accts[l.credit_idx])
                .map(|l| {
                    json!({
                        "debit_account_id": dr_accts[l.debit_idx],
                        "credit_account_id": cr_accts[l.credit_idx],
                        "amount": l.amount
                    })
                })
                .collect();
            if lines_payload.is_empty() {
                continue;
            }

            let key = fresh_uuid(&pool).await;
            let payload = serde_json::Value::Array(lines_payload.clone());

            // Pick a date inside the open Apr/May/Jun window from the
            // fixture (closed_at IS NULL). Cycle by batch_idx.
            let bd = match batch_idx % 3 {
                0 => "2026-04-15",
                1 => "2026-05-15",
                _ => "2026-06-15",
            };

            let doc = match call_je(&pool, payload.clone(), bd, &key).await {
                Ok(d) => d,
                Err(e) => panic!("[{label}/batch{batch_idx}] post_journal_entry failed: {e}"),
            };

            // Update expected balance deltas.
            for line in batch
                .lines
                .iter()
                .filter(|l| dr_accts[l.debit_idx] != cr_accts[l.credit_idx])
            {
                *expected_d.entry(dr_accts[line.debit_idx]).or_insert(0) += line.amount;
                *expected_c.entry(cr_accts[line.credit_idx]).or_insert(0) += line.amount;
            }

            // Optionally replay: must return same doc, no duplicates.
            if batch.replay {
                let doc2 = call_je(&pool, payload, bd, &key).await.expect("replay ok");
                assert_eq!(doc, doc2, "[{label}/batch{batch_idx}] replay returned different doc");
            }

            docs_seen.push((doc.clone(), batch.lines.clone()));

            // Per-batch: posting_lines count = N lines.
            let expected_pl: i64 = batch
                .lines
                .iter()
                .filter(|l| dr_accts[l.debit_idx] != cr_accts[l.credit_idx])
                .count() as i64;
            let actual_pl: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)::BIGINT FROM posting_lines WHERE document_id = $1::UUID",
            )
            .bind(&doc)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(
                actual_pl, expected_pl,
                "[{label}/batch{batch_idx}] posting_lines count mismatch: {actual_pl} vs {expected_pl}"
            );

            // qty must be NULL on every JE posting_line (value-only).
            let n_with_qty: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)::BIGINT FROM posting_lines
                  WHERE document_id = $1::UUID AND qty IS NOT NULL",
            )
            .bind(&doc)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(n_with_qty, 0, "[{label}/batch{batch_idx}] JE posting_lines must have qty IS NULL");

            // No subledger / extension rows.
            let n_im: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)::BIGINT FROM inventory_movements im
                 JOIN posting_lines pl ON pl.id = im.posting_line_id
                 WHERE pl.document_id = $1::UUID",
            )
            .bind(&doc)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(n_im, 0);

            let n_pli: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)::BIGINT FROM posting_line_inventory pli
                 JOIN posting_lines pl ON pl.id = pli.posting_line_id
                 WHERE pl.document_id = $1::UUID",
            )
            .bind(&doc)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(n_pli, 0);

            let n_prov: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)::BIGINT FROM posting_lines_provisional plp
                 JOIN posting_lines pl ON pl.id = plp.posting_line_id
                 WHERE pl.document_id = $1::UUID",
            )
            .bind(&doc)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(n_prov, 0);

            common::assert_invariants_hold(&pool, &format!("{label}/b{batch_idx}")).await;
        }

        // Final per-account Δ check.
        for a in dr_accts.iter().chain(cr_accts.iter()) {
            let (d, c) = account_balance(&pool, *a).await;
            let d0 = *start_d.get(a).unwrap();
            let c0 = *start_c.get(a).unwrap();
            let exp_d = *expected_d.get(a).unwrap_or(&0);
            let exp_c = *expected_c.get(a).unwrap_or(&0);
            assert_eq!(
                d - d0,
                exp_d,
                "[{label}] account {a} debits Δ wrong (got {}, expected {exp_d})",
                d - d0
            );
            assert_eq!(
                c - c0,
                exp_c,
                "[{label}] account {a} credits Δ wrong (got {}, expected {exp_c})",
                c - c0
            );
        }

        // Final: no JE doc has duplicated lines after all the replays.
        for (doc, lines) in &docs_seen {
            let n: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)::BIGINT FROM journal_entry_lines WHERE journal_entry_id = $1::UUID",
            )
            .bind(doc)
            .fetch_one(&pool)
            .await
            .unwrap();
            let expected = lines
                .iter()
                .filter(|l| dr_accts[l.debit_idx] != cr_accts[l.credit_idx])
                .count() as i64;
            assert_eq!(
                n, expected,
                "[{label}] doc {doc} has {n} lines (expected {expected}) — replay duplicated?"
            );
        }
    }
}
