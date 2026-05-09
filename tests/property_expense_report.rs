//! acct-wb75.4.3 — Phase F3: property test for post_expense_report.
//!
//! Random sequences of expense-report batches across a small pool of
//! employees, expense accounts, and per-employee ap_employee
//! settlement accounts. Verifies:
//!
//! 1. Each successful batch produces 1 expense_reports header row and
//!    N expense_report_lines + N posting_lines.
//! 2. Per-employee ap_employee Δ credits = SUM(amount) over lines
//!    settled to that employee's ap_employee account.
//! 3. Per-expense-account Δ debits matches sum routed through it.
//! 4. Idempotent replay returns same doc id, no duplicates.
//! 5. assert_invariants_hold (I1-I7) after every successful batch.
//! 6. No inventory_movements / posting_line_inventory /
//!    posting_lines_provisional rows produced.
//! 7. Per-leg `posting_lines.qty` is NULL.
//!
//! Settlement is always ap_employee (credit-normal, no pre-funding
//! needed). Cash settlement is exercised in the integration matrix.

mod common;

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use serde_json::json;
use sqlx::PgPool;
use std::collections::HashMap;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;
const NUM_EMPLOYEES: usize = 3;

#[derive(Debug, Clone)]
struct ErLine {
    expense_idx: usize,
    amount: i64,
}

#[derive(Debug, Clone)]
struct ErBatch {
    employee_idx: usize,
    lines: Vec<ErLine>,
    replay: bool,
}

fn arb_line(num_accts: usize) -> impl Strategy<Value = ErLine> {
    (0..num_accts, 1i64..=100_000).prop_map(|(expense_idx, amount)| ErLine {
        expense_idx,
        amount,
    })
}

fn arb_batch(num_emps: usize, num_accts: usize) -> impl Strategy<Value = ErBatch> {
    (
        0..num_emps,
        proptest::collection::vec(arb_line(num_accts), 1..=4),
        proptest::bool::weighted(0.15),
    )
        .prop_map(|(employee_idx, lines, replay)| ErBatch {
            employee_idx,
            lines,
            replay,
        })
}

fn arb_seq(num_emps: usize, num_accts: usize) -> impl Strategy<Value = Vec<ErBatch>> {
    proptest::collection::vec(arb_batch(num_emps, num_accts), 5..=15)
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

async fn fresh_employee_with_ap(pool: &PgPool, code: &str) -> (String, i64) {
    let emp: String = sqlx::query_scalar(
        "INSERT INTO employees (code, name, currency) VALUES ($1, $2, 'USD') RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Employee {code}"))
    .fetch_one(pool)
    .await
    .unwrap();

    let ap_emp: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ('ap_employee'::account_kind, 'value'::ledger_kind, 'USD', $1::UUID, 'credit'::balance_direction)
         RETURNING id",
    )
    .bind(&emp)
    .fetch_one(pool)
    .await
    .unwrap();

    (emp, ap_emp)
}

/// Pool of debit-side USD value-ledger non-SKU non-counterparty
/// accounts, suitable for absorbing arbitrary expense-report debits.
async fn account_pool_usd_debit_side(pool: &PgPool) -> Vec<i64> {
    sqlx::query_scalar(
        "SELECT id FROM accounts
          WHERE ledger_kind = 'value'
            AND currency = 'USD'
            AND sku_id IS NULL
            AND counterparty_id IS NULL
            AND NOT is_closed
            AND normal_side IN ('debit', 'unrestricted')
          ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("debit pool")
}

async fn call_er(
    pool: &PgPool,
    employee_id: &str,
    lines: serde_json::Value,
    settlement: i64,
    business_date: &str,
    idempotency_key: &str,
) -> sqlx::Result<String> {
    sqlx::query_scalar(
        "SELECT post_expense_report($1::UUID, 'USD', $2, $3, $4::DATE, $5::UUID, $6::UUID, NULL, NULL)::TEXT",
    )
    .bind(employee_id)
    .bind(lines)
    .bind(settlement)
    .bind(business_date)
    .bind(ACTOR)
    .bind(idempotency_key)
    .fetch_one(pool)
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn property_expense_report_invariants_hold() {
    let pool = common::connect_test_db().await;

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(TEST_PROPTEST_CASES_DEFAULT);

    let mut runner = proptest::test_runner::TestRunner::default();

    for case_idx in 0..cases {
        common::reset_to_fixture(&pool).await;

        // Scaffold N employees with their per-employee ap_employee accounts.
        let mut employees: Vec<(String, i64)> = Vec::new();
        for i in 0..NUM_EMPLOYEES {
            let code = format!("E-PROP-{case_idx}-{i}");
            employees.push(fresh_employee_with_ap(&pool, &code).await);
        }

        let exp_accts = account_pool_usd_debit_side(&pool).await;
        assert!(
            exp_accts.len() >= 3,
            "fixture must seed >= 3 USD value-side debit-able non-SKU accounts"
        );

        let strategy = arb_seq(employees.len(), exp_accts.len());
        let tree = strategy.new_tree(&mut runner).expect("strategy.new_tree");
        let batches: Vec<ErBatch> = tree.current();

        let label = format!("er#{case_idx}");

        let mut expected_d: HashMap<i64, i64> = HashMap::new();
        let mut expected_c_ap: HashMap<i64, i64> = HashMap::new();

        let mut start_d: HashMap<i64, i64> = HashMap::new();
        let mut start_c: HashMap<i64, i64> = HashMap::new();
        for a in exp_accts.iter().chain(employees.iter().map(|(_, ap)| ap)) {
            let (d, c) = account_balance(&pool, *a).await;
            start_d.insert(*a, d);
            start_c.insert(*a, c);
        }

        let mut docs_seen: Vec<(String, Vec<ErLine>)> = Vec::new();

        for (batch_idx, batch) in batches.iter().enumerate() {
            let (employee_id, ap_emp) = &employees[batch.employee_idx];

            // Drop lines whose expense_account collides with the
            // employee's ap_employee (rare — exp_accts pool excludes
            // counterparty_id IS NOT NULL — but defensively guard).
            let lines_payload: Vec<serde_json::Value> = batch
                .lines
                .iter()
                .filter(|l| exp_accts[l.expense_idx] != *ap_emp)
                .map(|l| {
                    json!({
                        "expense_account_id": exp_accts[l.expense_idx],
                        "amount": l.amount
                    })
                })
                .collect();
            if lines_payload.is_empty() {
                continue;
            }

            let key = fresh_uuid(&pool).await;
            let payload = serde_json::Value::Array(lines_payload.clone());

            let bd = match batch_idx % 3 {
                0 => "2026-04-15",
                1 => "2026-05-15",
                _ => "2026-06-15",
            };

            let doc = match call_er(&pool, employee_id, payload.clone(), *ap_emp, bd, &key).await {
                Ok(d) => d,
                Err(e) => panic!("[{label}/batch{batch_idx}] post_expense_report failed: {e}"),
            };

            let mut posting_count_expected: i64 = 0;
            for line in batch.lines.iter().filter(|l| exp_accts[l.expense_idx] != *ap_emp) {
                *expected_d.entry(exp_accts[line.expense_idx]).or_insert(0) += line.amount;
                *expected_c_ap.entry(*ap_emp).or_insert(0) += line.amount;
                posting_count_expected += 1;
            }

            if batch.replay {
                let doc2 = call_er(&pool, employee_id, payload, *ap_emp, bd, &key)
                    .await
                    .expect("replay ok");
                assert_eq!(
                    doc, doc2,
                    "[{label}/batch{batch_idx}] replay returned different doc"
                );
            }

            docs_seen.push((doc.clone(), batch.lines.clone()));

            let actual_pl: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)::BIGINT FROM posting_lines WHERE document_id = $1::UUID",
            )
            .bind(&doc)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(
                actual_pl, posting_count_expected,
                "[{label}/batch{batch_idx}] posting_lines count mismatch: {actual_pl} vs {posting_count_expected}"
            );

            let n_with_qty: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)::BIGINT FROM posting_lines
                  WHERE document_id = $1::UUID AND qty IS NOT NULL",
            )
            .bind(&doc)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(
                n_with_qty, 0,
                "[{label}/batch{batch_idx}] expense_report posting_lines must have qty IS NULL"
            );

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

        // Final per-account Δ debits.
        for a in exp_accts.iter() {
            let (d, _) = account_balance(&pool, *a).await;
            let d0 = *start_d.get(a).unwrap();
            let exp_d = *expected_d.get(a).unwrap_or(&0);
            assert_eq!(
                d - d0,
                exp_d,
                "[{label}] account {a} debits Δ wrong (got {}, expected {exp_d})",
                d - d0
            );
        }
        // Final per-employee-ap Δ credits.
        for (employee_id, ap) in &employees {
            let (_, c) = account_balance(&pool, *ap).await;
            let c0 = *start_c.get(ap).unwrap();
            let exp_c = *expected_c_ap.get(ap).unwrap_or(&0);
            assert_eq!(
                c - c0,
                exp_c,
                "[{label}] employee {employee_id} ap_employee Δ credits wrong (got {}, expected {exp_c})",
                c - c0
            );
        }

        for (doc, lines) in &docs_seen {
            let n: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)::BIGINT FROM expense_report_lines WHERE expense_report_id = $1::UUID",
            )
            .bind(doc)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert!(
                n >= 1 && n as usize <= lines.len(),
                "[{label}] doc {doc} has {n} lines (raw input had {}) — replay duplicated?",
                lines.len()
            );
        }
    }
}
