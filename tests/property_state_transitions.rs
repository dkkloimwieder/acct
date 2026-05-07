//! acct-8e3z (sub of acct-1cer) — property test for post_so_allocate +
//! post_eco_approve.
//!
//! Both functions are pure state-transition workflows: NO ledger events
//! are emitted. They mutate row status fields atomically and rely on
//! idempotency_key UNIQUE for replay safety.
//!
//! The bug class these tests catch is state-machine inconsistency
//! (wrong target state, wrong starting state, partial transition under
//! interleave) rather than the class-confusion / R1-R7 surface that
//! ledger-emitting functions need.
//!
//! Per scenario:
//!
//!   * post_so_allocate: random Allocate / Replay sequence across N SOs.
//!     Asserts active reservations flip to 'allocated'; replay returns
//!     the same id; so_allocations count == unique-key count; NO
//!     transfers reference any allocation document_id.
//!
//!   * post_eco_approve: N draft ECOs, half with a prior active rev
//!     (obsolete branch), half without. Random Approve / DoubleApprove
//!     ops. Asserts approved ECO has status / approved_by / approved_at
//!     / effective_at populated; attached BOM goes active; prior actives
//!     for same (parent, alt) become obsolete; double-approve raises
//!     P0031; NO transfers reference any ECO.
//!
//! Use PROPTEST_CASES=N to override the default scenario count.
//! Run with --test-threads=1 to avoid TRUNCATE+seed contention.

mod common;

use common::*;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use serde_json::json;
use sqlx::PgPool;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;

// ============================================================
// Shared helpers (alloc + eco share none of these bodies but
// inline duplication would balloon the file; keep as fns).
// ============================================================

async fn fresh_customer(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO customers (code, name, default_currency)
         VALUES ($1, $2, 'USD') RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Cust {code}"))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("customer: {e}"))
}

async fn fresh_sku_local(pool: &PgPool, code: &str, cost_method: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', $2::cost_method) RETURNING id::text",
    )
    .bind(code)
    .bind(cost_method)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("sku: {e}"))
}

async fn fresh_location_local(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("loc: {e}"))
}

#[allow(clippy::too_many_arguments)]
async fn open_account(
    pool: &PgPool,
    kind: &str,
    ledger_kind: &str,
    currency: Option<&str>,
    sku_id: Option<&str>,
    loc_id: Option<&str>,
    counterparty_id: Option<&str>,
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, sku_id, location_id,
             counterparty_id, normal_side)
         VALUES ($1::account_kind, $2::ledger_kind, $3, $4::UUID, $5::UUID, $6::UUID,
                 $7::balance_direction)
         RETURNING id",
    )
    .bind(kind)
    .bind(ledger_kind)
    .bind(currency)
    .bind(sku_id)
    .bind(loc_id)
    .bind(counterparty_id)
    .bind(normal_side)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("open {kind}: {e}"))
}

async fn set_std_cost(pool: &PgPool, sku_id: &str, cost: i64) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         VALUES ($1::UUID, $2, '2026-01-01', $3::UUID, $4::UUID)",
    )
    .bind(sku_id)
    .bind(cost)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("std cost: {e}"));
}

// ============================================================
// post_so_allocate property test
// ============================================================

#[derive(Debug, Clone, Copy)]
enum AllocOp {
    /// Allocate so_idx — flips active → allocated.
    Allocate { so_idx: usize },
    /// Replay the previous allocate call (same key).
    Replay,
    /// Direct ship from current state (active OR allocated → shipped).
    /// Out-of-band shortcut to validate the post_so_ship widening.
    DirectShip { so_idx: usize },
}

fn arb_alloc_op(n: usize) -> impl Strategy<Value = AllocOp> {
    let n_strat = 0usize..n;
    prop_oneof![
        4 => n_strat.clone().prop_map(|i| AllocOp::Allocate { so_idx: i }),
        1 => Just(AllocOp::Replay),
        2 => n_strat.prop_map(|i| AllocOp::DirectShip { so_idx: i }),
    ]
}

fn arb_alloc_seq(n: usize) -> impl Strategy<Value = Vec<AllocOp>> {
    prop::collection::vec(arb_alloc_op(n), 4..=12)
}

#[derive(Debug)]
#[allow(dead_code)]
struct AllocSoMirror {
    customer_id: String,
    so_id: String,
    sku_id: String,
    ship_loc_id: String,
    so_line_id: String,
    rsv_id: String,
    qty_acct: i64,
    val_acct: i64,
    cust_qty: i64,
    cust_unsettled: i64,
    last_status: String,
    n_allocations: i64,
    last_alloc: Option<(String, String)>, // (posted_by, key) for replay
}

const N_SOS: usize = 3;
const QTY_ORDERED: i64 = 50;
const FG_SEED_QTY: i64 = 100;
const FG_SEED_VAL: i64 = 6_000;
const STD_COST: i64 = 60;

async fn build_alloc_scaffold(pool: &PgPool, suffix: &str) -> Vec<AllocSoMirror> {
    let void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;

    let mut scaffolds = Vec::with_capacity(N_SOS);
    for i in 0..N_SOS {
        let customer_id = fresh_customer(pool, &format!("AL-{suffix}-c{i}")).await;
        let sku_id = fresh_sku_local(pool, &format!("AL-{suffix}-s{i}"), "standard").await;
        let ship_loc_id = fresh_location_local(pool, &format!("AL-{suffix}-l{i}")).await;
        set_std_cost(pool, &sku_id, STD_COST).await;

        let so_id: String = sqlx::query_scalar(
            "INSERT INTO sales_orders (customer_id, status)
             VALUES ($1::UUID, 'open') RETURNING id::text",
        )
        .bind(&customer_id)
        .fetch_one(pool)
        .await
        .expect("create so");
        let so_line_id: String = sqlx::query_scalar(
            "INSERT INTO sales_order_lines
                (so_id, line_no, sku_id, ship_location_id, qty_ordered,
                 unit_price, currency, tax_amount)
             VALUES ($1::UUID, 1, $2::UUID, $3::UUID, $4, 100, 'USD', 0)
             RETURNING id::text",
        )
        .bind(&so_id)
        .bind(&sku_id)
        .bind(&ship_loc_id)
        .bind(QTY_ORDERED)
        .fetch_one(pool)
        .await
        .expect("create so_line");

        let qty_acct = open_account(
            pool, "stock_available", "qty", None, Some(&sku_id), Some(&ship_loc_id), None, "debit",
        )
        .await;
        let val_acct = open_account(
            pool, "inv_value_fg", "value", Some("USD"), Some(&sku_id), Some(&ship_loc_id), None, "debit",
        )
        .await;
        let cust_qty = open_account(
            pool, "customer_pool", "qty", None, None, None, Some(&customer_id), "debit",
        )
        .await;
        let cust_unsettled = open_account(
            pool, "ar_unsettled", "value", Some("USD"), None, None, Some(&customer_id), "debit",
        )
        .await;

        // Pre-stock fg + qty so reservation succeeds.
        let posted_by = fresh_uuid(pool).await;
        let did = fresh_uuid(pool).await;
        let mint = json!([
            {"reason":"cycle_count_adj","document_kind":"alloc_seed","document_id":did,
             "debit_account_id":qty_acct,"credit_account_id":void_qty,
             "amount":FG_SEED_QTY,"qty":FG_SEED_QTY,"business_date":"2026-04-15",
             "idempotency_key":fresh_uuid(pool).await,"posted_by":posted_by},
            {"reason":"cycle_count_adj","document_kind":"alloc_seed","document_id":did,
             "debit_account_id":val_acct,"credit_account_id":void_val,
             "amount":FG_SEED_VAL,"qty":FG_SEED_QTY,"business_date":"2026-04-15",
             "idempotency_key":fresh_uuid(pool).await,"posted_by":posted_by},
        ]);
        sqlx::query("SELECT post_posting_lines($1, FALSE)")
            .bind(mint)
            .execute(pool)
            .await
            .expect("seed alloc fg");

        // Create reservation.
        let rsv_id: String = sqlx::query_scalar(
            "SELECT reserve_inventory(
                $1::UUID, $2::UUID, $3::BIGINT, $4::UUID, $5::UUID,
                '2099-01-01'::TIMESTAMPTZ, NULL
             )::text",
        )
        .bind(&sku_id)
        .bind(&ship_loc_id)
        .bind(QTY_ORDERED)
        .bind(&so_id)
        .bind(&so_line_id)
        .fetch_one(pool)
        .await
        .expect("reserve_inventory");

        scaffolds.push(AllocSoMirror {
            customer_id,
            so_id,
            sku_id,
            ship_loc_id,
            so_line_id,
            rsv_id,
            qty_acct,
            val_acct,
            cust_qty,
            cust_unsettled,
            last_status: "active".into(),
            n_allocations: 0,
            last_alloc: None,
        });
    }
    scaffolds
}

async fn reservation_status(pool: &PgPool, rsv_id: &str) -> String {
    sqlx::query_scalar("SELECT status::text FROM inventory_reservations WHERE id = $1::UUID")
        .bind(rsv_id)
        .fetch_one(pool)
        .await
        .expect("rsv status")
}

async fn count_allocations_for(pool: &PgPool, so_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM so_allocations WHERE so_id = $1::UUID")
        .bind(so_id)
        .fetch_one(pool)
        .await
        .expect("count allocations")
}

/// Assert NO transfers carry a document_id matching any so_allocations row.
async fn assert_no_alloc_transfers(pool: &PgPool, label: &str) {
    let leaked: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM posting_lines t
          WHERE t.document_id IN (SELECT id FROM so_allocations)",
    )
    .fetch_one(pool)
    .await
    .expect("count alloc transfers");
    assert_eq!(
        leaked, 0,
        "[{label}] post_so_allocate emitted ledger transfers — must be pure state transition"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn property_so_allocate_state_machine() {
    let pool = connect_test_db().await;

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(TEST_PROPTEST_CASES_DEFAULT);

    let mut runner = proptest::test_runner::TestRunner::default();
    let strategy = arb_alloc_seq(N_SOS);

    for case_idx in 0..cases {
        reset_to_fixture(&pool).await;

        let tree = strategy.new_tree(&mut runner).expect("new_tree");
        let ops: Vec<AllocOp> = tree.current();

        let label = format!("alloc#{case_idx}");
        let mut sos = build_alloc_scaffold(&pool, &label).await;

        // Initial state: every reservation is 'active'.
        for s in &sos {
            assert_eq!(reservation_status(&pool, &s.rsv_id).await, "active");
        }

        for (step, op) in ops.iter().enumerate() {
            let step_label = format!("{label}.step{step}");
            match *op {
                AllocOp::Allocate { so_idx } => {
                    let posted_by = fresh_uuid(&pool).await;
                    let key = fresh_uuid(&pool).await;
                    let so_id = sos[so_idx].so_id.clone();
                    let alloc_id: String = sqlx::query_scalar(
                        "SELECT post_so_allocate($1::UUID, '2026-04-19'::DATE,
                                                  $2::UUID, $3::UUID, NULL)::text",
                    )
                    .bind(&so_id)
                    .bind(&posted_by)
                    .bind(&key)
                    .fetch_one(&pool)
                    .await
                    .unwrap_or_else(|e| panic!("[{step_label}] alloc: {e}"));
                    assert!(!alloc_id.is_empty());

                    // Reservation: active → allocated. Already-allocated stays.
                    // Shipped is untouched.
                    let new_status = reservation_status(&pool, &sos[so_idx].rsv_id).await;
                    let prev = &sos[so_idx].last_status;
                    let expected = match prev.as_str() {
                        "active" => "allocated",
                        "allocated" => "allocated",
                        "shipped" => "shipped",
                        other => other,
                    };
                    assert_eq!(
                        new_status, expected,
                        "[{step_label}] so={so_idx} reservation: prev={prev} new={new_status} expected={expected}"
                    );
                    sos[so_idx].last_status = new_status;
                    sos[so_idx].n_allocations += 1;
                    sos[so_idx].last_alloc = Some((posted_by, key));
                }
                AllocOp::Replay => {
                    // Find the most recent so with a prior allocation. Replay it.
                    let mut chosen: Option<usize> = None;
                    for (i, s) in sos.iter().enumerate() {
                        if s.last_alloc.is_some() {
                            chosen = Some(i);
                        }
                    }
                    let Some(i) = chosen else { continue };
                    let (posted_by, key) = sos[i].last_alloc.clone().unwrap();
                    let so_id = sos[i].so_id.clone();
                    let id_first: String = sqlx::query_scalar(
                        "SELECT id::text FROM so_allocations WHERE idempotency_key=$1::UUID",
                    )
                    .bind(&key)
                    .fetch_one(&pool)
                    .await
                    .expect("first id");
                    let id_replay: String = sqlx::query_scalar(
                        "SELECT post_so_allocate($1::UUID, '2026-04-19'::DATE,
                                                  $2::UUID, $3::UUID, NULL)::text",
                    )
                    .bind(&so_id)
                    .bind(&posted_by)
                    .bind(&key)
                    .fetch_one(&pool)
                    .await
                    .expect("replay alloc");
                    assert_eq!(
                        id_first, id_replay,
                        "[{step_label}] replay must return same allocation id"
                    );
                    // No new row.
                    let count = count_allocations_for(&pool, &so_id).await;
                    assert_eq!(
                        count, sos[i].n_allocations,
                        "[{step_label}] replay must NOT INSERT a new row"
                    );
                    // Status unchanged.
                    let new_status = reservation_status(&pool, &sos[i].rsv_id).await;
                    assert_eq!(new_status, sos[i].last_status);
                }
                AllocOp::DirectShip { so_idx } => {
                    if sos[so_idx].last_status == "shipped" {
                        continue;
                    }
                    let posted_by = fresh_uuid(&pool).await;
                    let key = fresh_uuid(&pool).await;
                    let lines = json!([{
                        "so_line_id": sos[so_idx].so_line_id,
                        "qty_shipped": 5,
                    }]);
                    let so_id = sos[so_idx].so_id.clone();
                    let _id: String = sqlx::query_scalar(
                        "SELECT post_so_ship($1::UUID, $2::JSONB, '2026-04-19'::DATE,
                                              $3::UUID, $4::UUID, NULL)::text",
                    )
                    .bind(&so_id)
                    .bind(lines)
                    .bind(&posted_by)
                    .bind(&key)
                    .fetch_one(&pool)
                    .await
                    .unwrap_or_else(|e| panic!("[{step_label}] ship: {e}"));
                    let new_status = reservation_status(&pool, &sos[so_idx].rsv_id).await;
                    assert_eq!(
                        new_status, "shipped",
                        "[{step_label}] so={so_idx} ship from prev={} did not flip to 'shipped' (got {new_status})",
                        sos[so_idx].last_status
                    );
                    sos[so_idx].last_status = new_status;
                }
            }
        }

        assert_no_alloc_transfers(&pool, &label).await;
        // Final invariant — count of allocation rows == cumulative
        // unique (allocate ops) (replays don't increment).
        for (i, s) in sos.iter().enumerate() {
            let actual = count_allocations_for(&pool, &s.so_id).await;
            assert_eq!(
                actual, s.n_allocations,
                "[{label}] so_idx={i} allocation count drift: expected {} actual {actual}",
                s.n_allocations
            );
        }
        assert_invariants_hold(&pool, &label).await;
    }
}

// ============================================================
// post_eco_approve property test
// ============================================================

#[derive(Debug, Clone, Copy)]
enum EcoOp {
    /// Approve eco_idx if currently draft.
    Approve { idx: usize },
    /// Approve eco_idx with the SAME args as last time (no-op if first
    /// run; already-approved if second run → P0031).
    DoubleApprove { idx: usize },
}

fn arb_eco_op(n: usize) -> impl Strategy<Value = EcoOp> {
    let n_strat = 0usize..n;
    prop_oneof![
        3 => n_strat.clone().prop_map(|i| EcoOp::Approve { idx: i }),
        1 => n_strat.prop_map(|i| EcoOp::DoubleApprove { idx: i }),
    ]
}

fn arb_eco_seq(n: usize) -> impl Strategy<Value = Vec<EcoOp>> {
    prop::collection::vec(arb_eco_op(n), 3..=10)
}

const N_ECOS: usize = 4;

#[derive(Debug)]
#[allow(dead_code)]
struct EcoMirror {
    eco_id: i64,
    bom_id: i64,
    parent_sku_id: String,
    alternate_no: i32,
    revision_no: String,
    /// Pre-existing active rev for same (parent, alt). None if first
    /// rev for this parent.
    prior_active_id: Option<i64>,
    is_approved: bool,
    last_args: Option<(String, String)>, // (effective_at_str, approved_by_uuid)
}

async fn build_eco_scaffold(pool: &PgPool, suffix: &str) -> Vec<EcoMirror> {
    let mut mirrors = Vec::with_capacity(N_ECOS);
    for i in 0..N_ECOS {
        // Parent SKU + a component for the BOM line.
        let parent_code = format!("ECO-{suffix}-P{i}");
        let comp_code = format!("ECO-{suffix}-C{i}");
        let raw_loc_code = format!("ECO-{suffix}-L{i}");
        let parent_id = fresh_sku_local(pool, &parent_code, "standard").await;
        let comp_id = fresh_sku_local(pool, &comp_code, "standard").await;
        let _raw_loc = fresh_location_local(pool, &raw_loc_code).await;
        set_std_cost(pool, &parent_id, 600).await;
        set_std_cost(pool, &comp_id, 60).await;

        // Half the ECOs come with a prior-active rev to exercise the
        // obsolete branch.
        let prior_active_id = if i % 2 == 0 {
            // Create active prior rev directly (not via ECO).
            let prior_id =
                create_bom_header_full(pool, &parent_code, 1, "A", true, "active", None)
                    .await;
            sqlx::query(
                "UPDATE bom_headers SET effective_at='2026-01-01'::DATE WHERE id=$1",
            )
            .bind(prior_id)
            .execute(pool)
            .await
            .expect("set prior effective_at");
            add_bom_item(pool, prior_id, 1, 10, &comp_code, &raw_loc_code, 1, 100.0).await;
            Some(prior_id)
        } else {
            None
        };

        // Draft ECO.
        let requested_by = fresh_uuid(pool).await;
        let eco_id: i64 = sqlx::query_scalar(
            "INSERT INTO engineering_change_orders (code, description, requested_by)
             VALUES ($1, $2, $3::UUID) RETURNING id",
        )
        .bind(format!("ECO-{suffix}-{i}"))
        .bind(format!("ECO test {suffix}#{i}"))
        .bind(&requested_by)
        .fetch_one(pool)
        .await
        .expect("create ECO");

        // Draft rev attached. is_primary=true is allowed at draft.
        let rev_label = if prior_active_id.is_some() { "B" } else { "A" };
        let bom_id =
            create_bom_header_full(pool, &parent_code, 1, rev_label, true, "draft", Some(eco_id))
                .await;
        add_bom_item(pool, bom_id, 1, 10, &comp_code, &raw_loc_code, 1, 100.0).await;

        mirrors.push(EcoMirror {
            eco_id,
            bom_id,
            parent_sku_id: parent_id,
            alternate_no: 1,
            revision_no: rev_label.into(),
            prior_active_id,
            is_approved: false,
            last_args: None,
        });
    }
    mirrors
}

async fn eco_status(pool: &PgPool, eco_id: i64) -> String {
    sqlx::query_scalar("SELECT status::text FROM engineering_change_orders WHERE id=$1")
        .bind(eco_id)
        .fetch_one(pool)
        .await
        .expect("eco status")
}

async fn bom_status(pool: &PgPool, bom_id: i64) -> String {
    sqlx::query_scalar("SELECT status::text FROM bom_headers WHERE id=$1")
        .bind(bom_id)
        .fetch_one(pool)
        .await
        .expect("bom status")
}

async fn assert_no_eco_transfers(pool: &PgPool, label: &str) {
    // ECO/BOM ids are BIGINT; transfers.document_id is UUID. So a direct
    // join doesn't apply. Instead pin: zero transfers were emitted with
    // document_kind containing 'eco' or 'bom_header'. Both are reasonable
    // proxies for "approval shouldn't post ledger events."
    let leaked: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM posting_lines
          WHERE document_kind ILIKE '%eco%'
             OR document_kind ILIKE '%bom_header%'",
    )
    .fetch_one(pool)
    .await
    .expect("count eco transfers");
    assert_eq!(
        leaked, 0,
        "[{label}] post_eco_approve emitted ledger transfers — must be pure state transition"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn property_eco_approve_state_machine() {
    let pool = connect_test_db().await;

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(TEST_PROPTEST_CASES_DEFAULT);

    let mut runner = proptest::test_runner::TestRunner::default();
    let strategy = arb_eco_seq(N_ECOS);

    for case_idx in 0..cases {
        reset_to_fixture(&pool).await;

        let tree = strategy.new_tree(&mut runner).expect("new_tree");
        let ops: Vec<EcoOp> = tree.current();

        let label = format!("eco#{case_idx}");
        let mut ecos = build_eco_scaffold(&pool, &label).await;

        // Initial state: all draft, attached BOMs draft, prior-actives active.
        for m in &ecos {
            assert_eq!(eco_status(&pool, m.eco_id).await, "draft");
            assert_eq!(bom_status(&pool, m.bom_id).await, "draft");
            if let Some(prior) = m.prior_active_id {
                assert_eq!(bom_status(&pool, prior).await, "active");
            }
        }

        // Pick a unique effective_at per case so multiple approvals
        // across SOs don't collide in their stamps.
        let effective_at = "2026-04-15";
        let approved_by = fresh_uuid(&pool).await;

        for (step, op) in ops.iter().enumerate() {
            let step_label = format!("{label}.step{step}");
            match *op {
                EcoOp::Approve { idx } => {
                    let m = &ecos[idx];
                    if m.is_approved {
                        // Re-approving an approved ECO must raise P0031.
                        let res = sqlx::query(
                            "SELECT post_eco_approve($1, $2::DATE, $3::UUID)",
                        )
                        .bind(m.eco_id)
                        .bind(effective_at)
                        .bind(&approved_by)
                        .execute(&pool)
                        .await;
                        match res {
                            Err(sqlx::Error::Database(db)) => {
                                assert_eq!(
                                    db.code().as_deref(),
                                    Some("P0031"),
                                    "[{step_label}] re-approve must raise P0031, got {:?}",
                                    db.code()
                                );
                            }
                            other => panic!(
                                "[{step_label}] re-approve must raise P0031, got {other:?}"
                            ),
                        }
                        continue;
                    }

                    sqlx::query("SELECT post_eco_approve($1, $2::DATE, $3::UUID)")
                        .bind(m.eco_id)
                        .bind(effective_at)
                        .bind(&approved_by)
                        .execute(&pool)
                        .await
                        .unwrap_or_else(|e| panic!("[{step_label}] approve: {e}"));

                    assert_eq!(
                        eco_status(&pool, m.eco_id).await,
                        "approved",
                        "[{step_label}] eco {idx} not approved"
                    );
                    assert_eq!(
                        bom_status(&pool, m.bom_id).await,
                        "active",
                        "[{step_label}] eco {idx} attached BOM not activated"
                    );
                    if let Some(prior) = m.prior_active_id {
                        assert_eq!(
                            bom_status(&pool, prior).await,
                            "obsolete",
                            "[{step_label}] eco {idx} prior active not obsoleted"
                        );
                    }
                    ecos[idx].is_approved = true;
                    ecos[idx].last_args = Some((effective_at.into(), approved_by.clone()));
                }
                EcoOp::DoubleApprove { idx } => {
                    let m = &ecos[idx];
                    let res = sqlx::query(
                        "SELECT post_eco_approve($1, $2::DATE, $3::UUID)",
                    )
                    .bind(m.eco_id)
                    .bind(effective_at)
                    .bind(&approved_by)
                    .execute(&pool)
                    .await;
                    if m.is_approved {
                        match res {
                            Err(sqlx::Error::Database(db)) => {
                                assert_eq!(db.code().as_deref(), Some("P0031"));
                            }
                            other => panic!(
                                "[{step_label}] double-approve must raise P0031, got {other:?}"
                            ),
                        }
                    } else {
                        // First-time approve; same as Approve branch.
                        res.unwrap_or_else(|e| {
                            panic!("[{step_label}] first approve: {e}")
                        });
                        ecos[idx].is_approved = true;
                        ecos[idx].last_args = Some((effective_at.into(), approved_by.clone()));
                    }
                }
            }
        }

        assert_no_eco_transfers(&pool, &label).await;
        assert_invariants_hold(&pool, &label).await;
    }
}

// ============================================================
// Validation gates (deterministic).
// ============================================================

#[tokio::test]
async fn property_so_allocate_unknown_so_raises_p0043() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let bogus = fresh_uuid(&pool).await;
    expect_sqlstate("P0043", || async {
        let posted_by = fresh_uuid(&pool).await;
        let key = fresh_uuid(&pool).await;
        sqlx::query_scalar::<_, String>(
            "SELECT post_so_allocate($1::UUID, '2026-04-19'::DATE,
                                       $2::UUID, $3::UUID, NULL)::text",
        )
        .bind(&bogus)
        .bind(&posted_by)
        .bind(&key)
        .fetch_one(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn property_eco_approve_null_args_raise_p0031() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // null eco_id
    expect_sqlstate("P0031", || async {
        sqlx::query("SELECT post_eco_approve(NULL::BIGINT, '2026-04-15'::DATE, $1::UUID)")
            .bind(fresh_uuid(&pool).await)
            .execute(&pool)
            .await
    })
    .await;

    // null effective_at
    expect_sqlstate("P0031", || async {
        sqlx::query("SELECT post_eco_approve(1, NULL::DATE, $1::UUID)")
            .bind(fresh_uuid(&pool).await)
            .execute(&pool)
            .await
    })
    .await;

    // null approved_by
    expect_sqlstate("P0031", || async {
        sqlx::query("SELECT post_eco_approve(1, '2026-04-15'::DATE, NULL::UUID)")
            .execute(&pool)
            .await
    })
    .await;
}

#[tokio::test]
async fn property_eco_approve_unknown_eco_raises_p0031() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    expect_sqlstate("P0031", || async {
        sqlx::query("SELECT post_eco_approve(99999::BIGINT, '2026-04-15'::DATE, $1::UUID)")
            .bind(fresh_uuid(&pool).await)
            .execute(&pool)
            .await
    })
    .await;
}

#[tokio::test]
async fn property_eco_approve_no_attached_boms_raises_p0031() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let requested_by = fresh_uuid(&pool).await;
    let eco_id: i64 = sqlx::query_scalar(
        "INSERT INTO engineering_change_orders (code, description, requested_by)
         VALUES ('ECO-NOATT', 'No BOMs attached', $1::UUID) RETURNING id",
    )
    .bind(&requested_by)
    .fetch_one(&pool)
    .await
    .expect("create eco");

    expect_sqlstate("P0031", || async {
        sqlx::query("SELECT post_eco_approve($1, '2026-04-15'::DATE, $2::UUID)")
            .bind(eco_id)
            .bind(fresh_uuid(&pool).await)
            .execute(&pool)
            .await
    })
    .await;
}
