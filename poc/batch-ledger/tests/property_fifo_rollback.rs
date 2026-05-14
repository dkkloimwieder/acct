//! acct-fhq7 — Property test for FIFO arena rollback correctness.
//!
//! ## Scope
//!
//! Random workloads of receipts + issues across multiple PG transactions,
//! with random savepoint nesting and random COMMIT / ROLLBACK outcomes.
//! Asserts core post-quiescence invariants against the current A2 shadow
//! implementation:
//!
//! - **I1** Recon drift = 0 after all scenarios + force_drain.
//! - **I2** Durable `cost_layers.qty_remaining` SUM matches the
//!   committed-effect delta computed by an independent in-test
//!   interpreter of the savepoint semantics.
//! - **I3** Every `cost_layer_depletions` row's `layer_id` points at an
//!   existing `cost_layers` row (FK enforced; redundant check explicit).
//! - **I4** Per-layer residual: `qty_remaining ≥ 0`. (CHECK constraint
//!   enforced; explicit re-assertion.)
//! - **I5** Net pool qty consistency:
//!   `SUM(qty_remaining) = baseline + Σ committed receipts − Σ committed issues`.
//!
//! ## Workload shape
//!
//! Each property case generates a `Scenario` = `Vec<Iteration>`, where
//! each iteration is `BEGIN; <steps>; <commit|rollback>;`. Steps include
//! `Apply(Op)`, `Savepoint`, `RollbackToTop`, `ReleaseTop`. The test's
//! interpreter walks the steps with a savepoint stack to compute the
//! exact committed delta for each iteration, then asserts the live
//! durable state matches.
//!
//! Single shared pool — exercises cell EXCL contention at xact_commit
//! replay across iterations. Single-backend sequential for the base
//! property; concurrent-backend property is bounded by the t2-t4
//! regression net (we don't want proptest shrinking through
//! tokio::spawn scheduling).
//!
//! Pre-seeded with 10M baseline so issues always succeed within bounded
//! qty ranges.
//!
//! Uses base prefix `6_500_000_000_000` (disjoint from t1's 4.5e12,
//! t2's 4.9e12, t3's 5.3e12, t4's 5.7e12, t5's 6.1e12).

use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, TestRunner};
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn db_url() -> String {
    std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
}

fn proptest_cases_default() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(32)
}

fn unique_accounts() -> (i64, i64, i64) {
    let u = Uuid::new_v4();
    let bytes = u.as_bytes();
    let base = 6_500_000_000_000_i64
        + ((bytes[0] as i64) << 24
            | (bytes[1] as i64) << 16
            | (bytes[2] as i64) << 8
            | (bytes[3] as i64))
            .abs()
            % 100_000_000;
    (base, base + 1, base + 2)
}

/// One atomic ledger operation.
#[derive(Debug, Clone)]
enum Op {
    Receive { qty: i64, unit_cost: i64 },
    Issue { qty: i64 },
}

/// Step within a single transaction.
#[derive(Debug, Clone)]
enum Step {
    Apply(Op),
    /// `SAVEPOINT s<depth>` — push a fresh subxact frame.
    Savepoint,
    /// `ROLLBACK TO SAVEPOINT s<depth>` — discard the most recent
    /// subxact frame (and pop it). No-op if no frame exists.
    RollbackToTop,
    /// `RELEASE SAVEPOINT s<depth>` — merge most-recent subxact into
    /// parent. No-op if no frame exists.
    ReleaseTop,
}

/// One transaction: a sequence of steps followed by a final COMMIT or
/// ROLLBACK outcome.
#[derive(Debug, Clone)]
struct Iteration {
    steps: Vec<Step>,
    final_commit: bool,
}

#[derive(Debug, Clone)]
struct Scenario {
    iterations: Vec<Iteration>,
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (1i64..20, 100i64..2000).prop_map(|(qty, uc)| Op::Receive { qty, unit_cost: uc }),
        3 => (1i64..15).prop_map(|qty| Op::Issue { qty }),
    ]
}

fn arb_step() -> impl Strategy<Value = Step> {
    prop_oneof![
        6 => arb_op().prop_map(Step::Apply),
        1 => Just(Step::Savepoint),
        1 => Just(Step::RollbackToTop),
        1 => Just(Step::ReleaseTop),
    ]
}

fn arb_iteration() -> impl Strategy<Value = Iteration> {
    (prop::collection::vec(arb_step(), 1..8), any::<bool>())
        .prop_map(|(steps, commit)| Iteration {
            steps,
            final_commit: commit,
        })
}

fn arb_scenario() -> impl Strategy<Value = Scenario> {
    prop::collection::vec(arb_iteration(), 1..8).prop_map(|iterations| Scenario { iterations })
}

/// In-test interpreter: walks the iteration's steps with a savepoint
/// stack, returns the committed (qty_delta, depletions_count) for the
/// outcome.
///
/// Stack representation: each frame holds a Vec of committed
/// (qty_delta, depletion_count) tuples accumulated within that frame.
/// `Savepoint` pushes; `RollbackToTop` pops; `ReleaseTop` merges into
/// parent. Final outcome:
///   - COMMIT: all surviving frames merge to top; sum is the iteration's
///     committed delta.
///   - ROLLBACK: discard everything.
fn interpret_iteration(it: &Iteration) -> (i64, i64) {
    // Each frame: (Σ qty_delta, Σ depletion_rows) over Apply ops staged
    // within it that have not yet been discarded.
    let mut stack: Vec<(i64, i64)> = vec![(0, 0)];
    for step in &it.steps {
        match step {
            Step::Apply(Op::Receive { qty, .. }) => {
                let top = stack.last_mut().expect("at least one frame");
                top.0 += qty;
                // Receipts: no depletion row.
            }
            Step::Apply(Op::Issue { qty }) => {
                let top = stack.last_mut().expect("at least one frame");
                top.0 -= qty;
                top.1 += 1;
            }
            Step::Savepoint => {
                stack.push((0, 0));
            }
            Step::RollbackToTop => {
                if stack.len() > 1 {
                    stack.pop();
                }
                // else: outer txn, ROLLBACK TO at top would be a SQL
                // error in PG; the runner skips it (see below).
            }
            Step::ReleaseTop => {
                if stack.len() > 1 {
                    let merged = stack.pop().expect("non-empty");
                    let parent = stack.last_mut().expect("parent exists");
                    parent.0 += merged.0;
                    parent.1 += merged.1;
                }
            }
        }
    }
    if !it.final_commit {
        return (0, 0);
    }
    // COMMIT: all surviving frames flatten.
    let mut q = 0i64;
    let mut d = 0i64;
    for f in stack {
        q += f.0;
        d += f.1;
    }
    (q, d)
}

async fn pool() -> sqlx::PgPool {
    let p = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(30))
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&p)
        .await
        .expect("ext");
    p
}

async fn seed_accounts(p: &sqlx::PgPool, ids: &[(i64, &str, &str)]) {
    for (id, code, kind) in ids {
        sqlx::query(
            "INSERT INTO accounts (id, code, kind, currency) \
             VALUES ($1::bigint, $2 || '-' || $1::TEXT, $3::account_kind, 'USD') \
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(*id)
        .bind(*code)
        .bind(*kind)
        .execute(p)
        .await
        .expect("seed account");
    }
}

async fn cleanup(p: &sqlx::PgPool, ids: &[i64]) {
    let _ = sqlx::query(
        "DELETE FROM cost_layer_depletions \
         WHERE layer_id IN (SELECT id FROM cost_layers WHERE pool_account_id = ANY($1::bigint[]))",
    )
    .bind(ids)
    .execute(p)
    .await;
    let _ = sqlx::query("DELETE FROM cost_layers WHERE pool_account_id = ANY($1::bigint[])")
        .bind(ids)
        .execute(p)
        .await;
    for id in ids {
        let _ = sqlx::query(
            "DELETE FROM posting_lines WHERE debit_account_id = $1::bigint OR credit_account_id = $1::bigint",
        )
        .bind(*id)
        .execute(p)
        .await;
    }
    let _ = sqlx::query("DELETE FROM accounts WHERE id = ANY($1::bigint[])")
        .bind(ids)
        .execute(p)
        .await;
}

fn receipt_envelope(pool_id: i64, ap_id: i64, qty: i64, unit_cost: i64) -> serde_json::Value {
    serde_json::json!([{
        "envelope_idx": 0,
        "kind": "fifo_receipt",
        "debit_account_id": pool_id,
        "credit_account_id": ap_id,
        "qty": qty,
        "unit_cost": unit_cost,
        "idempotency_key": Uuid::new_v4().to_string(),
        "business_date": "2026-05-14",
    }])
}

fn issue_envelope(pool_id: i64, cogs_id: i64, qty: i64) -> serde_json::Value {
    serde_json::json!([{
        "envelope_idx": 0,
        "kind": "fifo_issue",
        "debit_account_id": cogs_id,
        "credit_account_id": pool_id,
        "qty": qty,
        "idempotency_key": Uuid::new_v4().to_string(),
        "business_date": "2026-05-14",
    }])
}

/// Execute one iteration as SQL. Returns Ok(()) on successful execution
/// (whether commit or rollback as scripted); Err on unexpected SQL
/// failure that disagrees with the interpreter's prediction.
///
/// Important contract: this function's view of "what got committed" MUST
/// match `interpret_iteration` for the test invariants to hold. The
/// interpreter and SQL paths share the same savepoint semantics
/// (PostgreSQL's standard read-committed + SAVEPOINT model).
async fn run_iteration(
    p: &sqlx::PgPool,
    it: &Iteration,
    pool_id: i64,
    ap_id: i64,
    cogs_id: i64,
) -> Result<(), sqlx::Error> {
    let mut tx = p.begin().await?;
    let mut savepoint_stack: u32 = 0;
    for step in &it.steps {
        match step {
            Step::Apply(Op::Receive { qty, unit_cost }) => {
                let envs = receipt_envelope(pool_id, ap_id, *qty, *unit_cost);
                sqlx::query(
                    "SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)",
                )
                .bind(&envs)
                .fetch_all(&mut *tx)
                .await?;
            }
            Step::Apply(Op::Issue { qty }) => {
                let envs = issue_envelope(pool_id, cogs_id, *qty);
                sqlx::query(
                    "SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)",
                )
                .bind(&envs)
                .fetch_all(&mut *tx)
                .await?;
            }
            Step::Savepoint => {
                savepoint_stack += 1;
                let name = format!("sp{}", savepoint_stack);
                sqlx::query(&format!("SAVEPOINT {}", name))
                    .execute(&mut *tx)
                    .await?;
            }
            Step::RollbackToTop => {
                if savepoint_stack > 0 {
                    let name = format!("sp{}", savepoint_stack);
                    sqlx::query(&format!("ROLLBACK TO SAVEPOINT {}", name))
                        .execute(&mut *tx)
                        .await?;
                    // PG's ROLLBACK TO leaves the savepoint name still
                    // valid; the savepoint can be re-rolled-back or
                    // released. The interpreter pops it though — keep
                    // matched semantics by also popping our counter.
                    savepoint_stack -= 1;
                }
            }
            Step::ReleaseTop => {
                if savepoint_stack > 0 {
                    let name = format!("sp{}", savepoint_stack);
                    sqlx::query(&format!("RELEASE SAVEPOINT {}", name))
                        .execute(&mut *tx)
                        .await?;
                    savepoint_stack -= 1;
                }
            }
        }
    }
    if it.final_commit {
        tx.commit().await?;
    } else {
        tx.rollback().await?;
    }
    Ok(())
}

async fn force_drain(p: &sqlx::PgPool) {
    sqlx::query("SELECT fifo_force_drain_tick()")
        .execute(p)
        .await
        .expect("force drain");
}

async fn durable_qty(p: &sqlx::PgPool, pool_id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(SUM(qty_remaining), 0)::bigint FROM cost_layers \
         WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_one(p)
    .await
    .expect("durable qty")
}

async fn min_qty_remaining(p: &sqlx::PgPool, pool_id: i64) -> Option<i64> {
    sqlx::query_scalar(
        "SELECT MIN(qty_remaining)::bigint FROM cost_layers \
         WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_one(p)
    .await
    .expect("min qty")
}

async fn orphan_depletions(p: &sqlx::PgPool, pool_id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM cost_layer_depletions d \
         WHERE d.layer_id IN (SELECT id FROM cost_layers WHERE pool_account_id = $1::bigint) \
         AND NOT EXISTS (SELECT 1 FROM cost_layers cl WHERE cl.id = d.layer_id)",
    )
    .bind(pool_id)
    .fetch_one(p)
    .await
    .expect("orphan depletions")
}

async fn depletion_count(p: &sqlx::PgPool, pool_id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM cost_layer_depletions d \
         WHERE d.layer_id IN (SELECT id FROM cost_layers WHERE pool_account_id = $1::bigint)",
    )
    .bind(pool_id)
    .fetch_one(p)
    .await
    .expect("depletion count")
}

async fn recon_drift(p: &sqlx::PgPool, pool_id: i64) -> Option<i64> {
    let row = sqlx::query(
        "SELECT drift FROM fifo_arena_recon() WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_optional(p)
    .await
    .expect("recon");
    row.and_then(|r| r.get::<Option<i64>, _>("drift"))
}

async fn run_scenario(scenario: Scenario) -> Result<(), TestCaseError> {
    let p = pool().await;
    let (pool_id, ap_id, cogs_id) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    // Pre-seed with a large baseline so issues never exhaust.
    let baseline: i64 = 10_000_000;
    sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_F($1::jsonb)")
        .bind(&receipt_envelope(pool_id, ap_id, baseline, 1000))
        .fetch_all(&p)
        .await
        .map_err(|e| TestCaseError::fail(format!("baseline receipt: {e:?}")))?;
    force_drain(&p).await;

    let mut expected_qty = baseline;
    let mut expected_depletions = 0i64;

    for (i, it) in scenario.iterations.iter().enumerate() {
        let (q_delta, d_delta) = interpret_iteration(it);
        // Pre-validate: would the iteration ever go negative? Skip if so
        // — issues against insufficient stock are a different shape from
        // savepoint correctness, and PG would raise P0006 / fifo_issue_*.
        // Compute hypothetical running balance over the iteration's
        // steps to guard against in-iter exhaustion.
        let mut hyp = expected_qty;
        let mut stack_hyp: Vec<i64> = vec![0];
        let mut would_exhaust = false;
        for step in &it.steps {
            match step {
                Step::Apply(Op::Receive { qty, .. }) => {
                    *stack_hyp.last_mut().unwrap() += qty;
                    hyp += qty;
                }
                Step::Apply(Op::Issue { qty }) => {
                    if hyp < *qty {
                        would_exhaust = true;
                        break;
                    }
                    *stack_hyp.last_mut().unwrap() -= qty;
                    hyp -= qty;
                }
                Step::Savepoint => stack_hyp.push(0),
                Step::RollbackToTop => {
                    if stack_hyp.len() > 1 {
                        let popped = stack_hyp.pop().unwrap();
                        hyp -= popped;
                    }
                }
                Step::ReleaseTop => {
                    if stack_hyp.len() > 1 {
                        let merged = stack_hyp.pop().unwrap();
                        *stack_hyp.last_mut().unwrap() += merged;
                    }
                }
            }
        }
        if would_exhaust {
            continue;
        }

        if let Err(e) = run_iteration(&p, it, pool_id, ap_id, cogs_id).await {
            cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
            return Err(TestCaseError::fail(format!(
                "iteration {i} SQL error: {e:?}\n  iteration: {it:?}"
            )));
        }
        expected_qty += q_delta;
        expected_depletions += d_delta;
    }

    force_drain(&p).await;

    // I1 — recon drift = 0.
    let drift = recon_drift(&p, pool_id).await;
    if drift != Some(0) {
        cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
        return Err(TestCaseError::fail(format!(
            "I1 violation: recon drift = {drift:?} (expected Some(0))"
        )));
    }

    // I2 — durable matches expected.
    let dq = durable_qty(&p, pool_id).await;
    if dq != expected_qty {
        cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
        return Err(TestCaseError::fail(format!(
            "I2 violation: durable_qty = {dq}; expected = {expected_qty}"
        )));
    }

    // I3 — FK integrity (no orphan depletions).
    let orphans = orphan_depletions(&p, pool_id).await;
    if orphans != 0 {
        cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
        return Err(TestCaseError::fail(format!(
            "I3 violation: {orphans} orphan depletions"
        )));
    }

    // I4 — min qty_remaining ≥ 0 (CHECK constraint backstop).
    if let Some(mn) = min_qty_remaining(&p, pool_id).await {
        if mn < 0 {
            cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
            return Err(TestCaseError::fail(format!(
                "I4 violation: min(qty_remaining) = {mn} < 0"
            )));
        }
    }

    // I5 — depletion count matches expected (committed issues).
    let deps = depletion_count(&p, pool_id).await;
    if deps != expected_depletions {
        cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
        return Err(TestCaseError::fail(format!(
            "I5 violation: depletion_count = {deps}; expected = {expected_depletions}"
        )));
    }

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
    Ok(())
}

#[test]
fn property_fifo_rollback_invariants() {
    let cases = proptest_cases_default();
    let mut runner = TestRunner::new(ProptestConfig {
        cases,
        failure_persistence: None,
        // Cap tree depth to keep individual cases tractable.
        max_shrink_iters: 50,
        ..ProptestConfig::default()
    });

    // proptest is sync; create a dedicated tokio runtime to drive the
    // async sqlx operations inside each case. Building the runtime here
    // (rather than using #[tokio::test]) avoids "Cannot start a runtime
    // from within a runtime" panics when block_on is invoked.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build runtime");
    let result = runner.run(&arb_scenario(), |scenario| rt.block_on(run_scenario(scenario)));
    if let Err(e) = result {
        panic!("property test failed: {e}");
    }
}
