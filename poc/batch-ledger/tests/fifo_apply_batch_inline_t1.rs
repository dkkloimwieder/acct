//! acct-vh5y — fifo_apply_batch_inline correctness tests.
//!
//! Routes through post_batch_fifo_maximal_inline (mig 0023; Rust pg_extern
//! fifo_apply_batch_inline). Mirrors fifo_shmem_correctness_maximal_t1 so
//! both paths have parallel correctness pinning before bench compare.
//!
//! Behavioural contract (per probe scope): no transfers; only fifo_receipt
//! + fifo_issue. Balance/qty routed via stage_apply (shmem) matching mig
//! 0022 so ledger_balance_lookup assertions hold unchanged.

use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn db_url() -> String {
    std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
}

fn unique_accounts() -> (i64, i64, i64) {
    let u = Uuid::new_v4();
    let bytes = u.as_bytes();
    // Distinct base from t1 (1_000_000_000_000) and WAC maximal t1
    // (1_500_000_000_000) so parallel binaries do not collide.
    let base = 1_900_000_000_000_i64
        + ((bytes[0] as i64) << 24
            | (bytes[1] as i64) << 16
            | (bytes[2] as i64) << 8
            | (bytes[3] as i64))
            .abs()
            % 100_000_000;
    (base, base + 1, base + 2)
}

async fn pool() -> sqlx::PgPool {
    let p = PgPoolOptions::new()
        .max_connections(16)
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
    // Drop dependent FIFO rows first to avoid FK conflicts.
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

async fn lookup(p: &sqlx::PgPool, acct: i64) -> (Option<i64>, Option<i64>) {
    let row = sqlx::query(
        "SELECT balance, qty FROM ledger_balance_lookup($1::bigint, 1, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .fetch_one(p)
    .await
    .expect("lookup");
    (row.get("balance"), row.get("qty"))
}

async fn wait_for_drain(p: &sqlx::PgPool, max_ms: u64) {
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_millis(max_ms) {
        let dirty: i64 = sqlx::query_scalar("SELECT ledger_shmem_dirty_count()")
            .fetch_one(p)
            .await
            .unwrap_or(0);
        if dirty == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Sum cost_amount on cost_layer_depletions whose issue_posting_line_id
/// matches the given posting_line id. Used to assert SUM(depletion cost)
/// == posting_line.amount for issues.
async fn sum_depl_cost_for_pl(p: &sqlx::PgPool, pl_id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(SUM(cost_amount), 0)::bigint FROM cost_layer_depletions \
         WHERE issue_posting_line_id = $1::bigint",
    )
    .bind(pl_id)
    .fetch_one(p)
    .await
    .expect("sum depl")
}

/// T1 — single batch: 1 receipt then 1 issue (pre-existing chain).
/// Two batches: first creates layer, second consumes from pre-existing.
#[tokio::test]
async fn t1_receipt_then_issue_lands_correctly() {
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

    sqlx::query("SELECT envelope_idx FROM post_batch_fifo_maximal_inline($1::jsonb)")
        .bind(serde_json::json!([{
            "envelope_idx": 0,
            "kind": "fifo_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 100,
            "unit_cost": 1000,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }]))
        .execute(&p)
        .await
        .expect("receipt");

    let (bal, qty) = lookup(&p, pool_id).await;
    assert_eq!(bal, Some(100_000), "T1: pool after receipt");
    assert_eq!(qty, Some(100), "T1: pool qty after receipt");

    let res = sqlx::query(
        "SELECT envelope_idx, posting_line_id \
         FROM post_batch_fifo_maximal_inline($1::jsonb)",
    )
    .bind(serde_json::json!([{
        "envelope_idx": 0,
        "kind": "fifo_issue",
        "debit_account_id": cogs_id,
        "credit_account_id": pool_id,
        "qty": 40,
        "idempotency_key": Uuid::new_v4().to_string(),
        "business_date": "2026-05-13",
    }]))
    .fetch_all(&p)
    .await
    .expect("issue");
    let issue_pl_id: i64 = res[0].get("posting_line_id");

    let (bal, qty) = lookup(&p, pool_id).await;
    assert_eq!(bal, Some(60_000), "T1: pool after issue (60 @ 1000)");
    assert_eq!(qty, Some(60), "T1: pool qty after issue");

    let (cogs_bal, _) = lookup(&p, cogs_id).await;
    assert_eq!(cogs_bal, Some(40_000), "T1: COGS captures FIFO-priced cost");

    let depl_sum = sum_depl_cost_for_pl(&p, issue_pl_id).await;
    assert_eq!(depl_sum, 40_000, "T1: SUM(depletion cost) = pl.amount");

    wait_for_drain(&p, 2000).await;
    let drift: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(ABS(drift)), 0)::bigint FROM ledger_shmem_recon() \
         WHERE account_id = ANY($1::bigint[])",
    )
    .bind(&[pool_id, ap_id, cogs_id][..])
    .fetch_one(&p)
    .await
    .expect("recon");
    assert_eq!(drift, 0, "T1: recon drift 0 across touched accounts");

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// T2 — cross-batch FIFO ordering: 3 receipts at varying unit_costs
/// across separate batches, then 1 issue that consumes from earlier
/// layers first.
#[tokio::test]
async fn t2_cross_batch_fifo_ordering() {
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

    let dates = ["2026-05-10", "2026-05-11", "2026-05-12"];
    for (i, (qty, uc)) in [(100i64, 1000i64), (50, 1200), (50, 800)].iter().enumerate() {
        sqlx::query("SELECT envelope_idx FROM post_batch_fifo_maximal_inline($1::jsonb)")
            .bind(serde_json::json!([{
                "envelope_idx": 0,
                "kind": "fifo_receipt",
                "debit_account_id": pool_id,
                "credit_account_id": ap_id,
                "qty": *qty,
                "unit_cost": *uc,
                "idempotency_key": Uuid::new_v4().to_string(),
                "business_date": dates[i],
            }]))
            .execute(&p)
            .await
            .expect("receipt");
    }

    // Issue 130 qty — should consume 100 @ 1000 (layer 1, oldest by date)
    // + 30 @ 1200 (layer 2). NOT 50 @ 800 (layer 3, newest by date).
    let res = sqlx::query(
        "SELECT envelope_idx, posting_line_id \
         FROM post_batch_fifo_maximal_inline($1::jsonb)",
    )
    .bind(serde_json::json!([{
        "envelope_idx": 0,
        "kind": "fifo_issue",
        "debit_account_id": cogs_id,
        "credit_account_id": pool_id,
        "qty": 130,
        "idempotency_key": Uuid::new_v4().to_string(),
        "business_date": "2026-05-13",
    }]))
    .fetch_all(&p)
    .await
    .expect("issue");
    let issue_pl_id: i64 = res[0].get("posting_line_id");

    let expected_cost = 100 * 1000 + 30 * 1200; // 100_000 + 36_000 = 136_000
    let (cogs_bal, _) = lookup(&p, cogs_id).await;
    assert_eq!(
        cogs_bal,
        Some(expected_cost),
        "T2: FIFO consumes oldest layers first (not weighted-avg)"
    );

    // Pool: started at 200_000 / 200; -136_000 / -130 → 64_000 / 70.
    // 50 @ 1200 has 20 remaining + 50 @ 800 → 20*1200 + 50*800 = 24_000 + 40_000 = 64_000. ✓
    let (bal, qty) = lookup(&p, pool_id).await;
    assert_eq!(bal, Some(64_000), "T2: pool balance after FIFO drain");
    assert_eq!(qty, Some(70), "T2: pool qty after drain");

    let depl_sum = sum_depl_cost_for_pl(&p, issue_pl_id).await;
    assert_eq!(depl_sum, expected_cost, "T2: SUM(depl cost) = pl.amount");

    // Layer residuals: oldest fully drained, next layer partial, newest untouched.
    let layers: Vec<(i64, i64, i64)> = sqlx::query(
        "SELECT id, qty_remaining, unit_cost FROM cost_layers \
         WHERE pool_account_id = $1::bigint ORDER BY receipt_date, id",
    )
    .bind(pool_id)
    .fetch_all(&p)
    .await
    .expect("layers")
    .into_iter()
    .map(|r| {
        (
            r.get::<i64, _>("id"),
            r.get::<i64, _>("qty_remaining"),
            r.get::<i64, _>("unit_cost"),
        )
    })
    .collect();
    assert_eq!(layers.len(), 3, "T2: 3 layers persisted");
    assert_eq!(layers[0].1, 0, "T2: oldest fully drained");
    assert_eq!(layers[1].1, 20, "T2: middle partial");
    assert_eq!(layers[2].1, 50, "T2: newest untouched");

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// T3 — in-batch sentinel resolution: 2 receipts + 1 issue in ONE batch.
/// Tests the negative-sentinel → real_layer_id mapping in the wrapper.
#[tokio::test]
async fn t3_in_batch_sentinel_resolution() {
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

    let envelopes = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 100,
            "unit_cost": 1000,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        },
        {
            "envelope_idx": 1,
            "kind": "fifo_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 50,
            "unit_cost": 1200,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        },
        {
            "envelope_idx": 2,
            "kind": "fifo_issue",
            "debit_account_id": cogs_id,
            "credit_account_id": pool_id,
            "qty": 120,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        },
    ]);
    let res = sqlx::query(
        "SELECT envelope_idx, posting_line_id, status \
         FROM post_batch_fifo_maximal_inline($1::jsonb) ORDER BY envelope_idx",
    )
    .bind(envelopes)
    .fetch_all(&p)
    .await
    .expect("mixed batch");
    assert_eq!(res.len(), 3, "T3: 3 rows returned");
    for r in &res {
        let st: String = r.get("status");
        assert_eq!(st, "committed", "T3: all envelopes committed");
    }
    let issue_pl_id: i64 = res[2].get("posting_line_id");

    // Issue 120: takes 100 @ 1000 (in-batch sentinel -1) + 20 @ 1200
    // (in-batch sentinel -2). Cost = 100_000 + 24_000 = 124_000.
    let expected = 100_000 + 24_000;
    let (cogs_bal, _) = lookup(&p, cogs_id).await;
    assert_eq!(cogs_bal, Some(expected), "T3: in-batch FIFO drain");

    let (bal, qty) = lookup(&p, pool_id).await;
    assert_eq!(bal, Some(36_000), "T3: pool balance (30 @ 1200)");
    assert_eq!(qty, Some(30), "T3: pool qty");

    let depl_sum = sum_depl_cost_for_pl(&p, issue_pl_id).await;
    assert_eq!(depl_sum, expected, "T3: SUM(depl cost) = pl.amount");

    // 2 depletion rows from sentinel→real_layer_id resolution.
    let depl_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM cost_layer_depletions \
         WHERE issue_posting_line_id = $1::bigint",
    )
    .bind(issue_pl_id)
    .fetch_one(&p)
    .await
    .expect("depl count");
    assert_eq!(depl_rows, 2, "T3: 2 depletion rows resolved from sentinels");

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// T4 — idempotent replay: same idempotency_key submitted twice.
/// Second batch returns `idempotent_replay`; no double cost_layer
/// or depletion side-effects; shmem unchanged.
#[tokio::test]
async fn t4_idempotent_replay() {
    let p = pool().await;
    let (pool_id, ap_id, _) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
        ],
    )
    .await;

    let idem = Uuid::new_v4();
    let envelopes = serde_json::json!([{
        "envelope_idx": 0,
        "kind": "fifo_receipt",
        "debit_account_id": pool_id,
        "credit_account_id": ap_id,
        "qty": 100,
        "unit_cost": 1000,
        "idempotency_key": idem.to_string(),
        "business_date": "2026-05-13",
    }]);

    sqlx::query("SELECT envelope_idx FROM post_batch_fifo_maximal_inline($1::jsonb)")
        .bind(&envelopes)
        .execute(&p)
        .await
        .expect("first apply");

    let (bal, qty) = lookup(&p, pool_id).await;
    assert_eq!(bal, Some(100_000), "T4: first apply lands");
    assert_eq!(qty, Some(100), "T4: first apply qty");

    let layers_before: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM cost_layers WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_one(&p)
    .await
    .expect("layer count before");
    assert_eq!(layers_before, 1, "T4: 1 layer after first apply");

    let res = sqlx::query("SELECT envelope_idx, status FROM post_batch_fifo_maximal_inline($1::jsonb)")
        .bind(&envelopes)
        .fetch_all(&p)
        .await
        .expect("replay");
    let status: String = res[0].get("status");
    assert_eq!(status, "idempotent_replay", "T4: replay status");

    let (bal, qty) = lookup(&p, pool_id).await;
    assert_eq!(bal, Some(100_000), "T4: replay did NOT double-apply balance");
    assert_eq!(qty, Some(100), "T4: replay did NOT double-apply qty");

    let layers_after: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM cost_layers WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_one(&p)
    .await
    .expect("layer count after");
    assert_eq!(layers_after, 1, "T4: replay did NOT insert duplicate layer");

    cleanup(&p, &[pool_id, ap_id]).await;
}

/// T5 — multi-writer fan-in: 8 concurrent backends each post 25
/// receipts to one shared pool. FOR UPDATE on cost_layers serializes
/// any overlapping issue (none here — pure receipts) and the per-
/// account stage_apply lands coupled writes via the shmem hash table.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn t5_multi_writer_fan_in_coupled_writes() {
    let p = pool().await;
    let (pool_id, ap_id, _) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
        ],
    )
    .await;

    let writers = 8;
    let envelopes_per_writer: i64 = 25;
    let qty_per_envelope: i64 = 100;
    let unit_cost: i64 = 1000;

    let mut handles = Vec::new();
    for _ in 0..writers {
        let url = db_url();
        let pid = pool_id;
        let aid = ap_id;
        handles.push(tokio::spawn(async move {
            let local_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await
                .expect("writer connect");
            for _ in 0..envelopes_per_writer {
                let envelopes = serde_json::json!([{
                    "envelope_idx": 0,
                    "kind": "fifo_receipt",
                    "debit_account_id": pid,
                    "credit_account_id": aid,
                    "qty": qty_per_envelope,
                    "unit_cost": unit_cost,
                    "idempotency_key": Uuid::new_v4().to_string(),
                    "business_date": "2026-05-13",
                }]);
                let _ = sqlx::query(
                    "SELECT envelope_idx FROM post_batch_fifo_maximal_inline($1::jsonb)",
                )
                .bind(envelopes)
                .execute(&local_pool)
                .await;
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }

    let expected_qty = writers as i64 * envelopes_per_writer * qty_per_envelope;
    let expected_bal = expected_qty * unit_cost;

    let (bal, qty) = lookup(&p, pool_id).await;
    assert_eq!(qty, Some(expected_qty), "T5: pool qty");
    assert_eq!(bal, Some(expected_bal), "T5: pool balance");

    // Confirm cost_layers reflects every receipt (no rows lost or doubled).
    let layer_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM cost_layers WHERE pool_account_id = $1::bigint",
    )
    .bind(pool_id)
    .fetch_one(&p)
    .await
    .expect("layer count");
    assert_eq!(
        layer_count,
        (writers as i64) * envelopes_per_writer,
        "T5: 1 layer per envelope"
    );

    wait_for_drain(&p, 5000).await;
    let drift: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(ABS(drift)), 0)::bigint FROM ledger_shmem_recon() \
         WHERE account_id = ANY($1::bigint[])",
    )
    .bind(&[pool_id, ap_id][..])
    .fetch_one(&p)
    .await
    .expect("recon");
    assert_eq!(drift, 0, "T5: recon drift 0 after concurrent fan-in");

    cleanup(&p, &[pool_id, ap_id]).await;
}
