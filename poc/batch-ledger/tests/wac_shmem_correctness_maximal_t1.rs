//! acct-2g9w — post_batch_wac_shmem_maximal correctness tests.
//!
//! Mirrors `wac_shmem_correctness_t1.rs` but routes through
//! `post_batch_wac_shmem_maximal` (mig 0018). The maximal variant pushes
//! WAC running-avg dispatch fully into Rust via
//! `ledger_dispatch_wac_batch`. Behavioural contract is identical to
//! mig 0014:
//!
//! - Same envelope shape (transfer / wac_receipt / wac_issue).
//! - Same per-envelope status return shape.
//! - Same idempotency-replay semantics (replay returns the pre-existing
//!   posting_line_id).
//! - Same in-batch running-average semantics for non-replay envelopes.
//!
//! One documented divergence: replays are pre-filtered before the
//! dispatcher runs, so replay envelopes do NOT contribute to in-batch
//! running avg. T4 single-envelope replay tests this exact path (post-
//! filter the batch is empty → dispatcher receives `[]` → no INSERT,
//! no stage_apply, but replay status row is still returned by the SQL
//! wrapper).

use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn db_url() -> String {
    std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
}

fn unique_accounts() -> (i64, i64, i64, i64) {
    let u = Uuid::new_v4();
    let bytes = u.as_bytes();
    // Offset base from the t1 file's range so concurrent runs don't collide.
    let base = 1_500_000_000_000_i64
        + ((bytes[0] as i64) << 24
            | (bytes[1] as i64) << 16
            | (bytes[2] as i64) << 8
            | (bytes[3] as i64))
            .abs()
            % 100_000_000;
    (base, base + 1, base + 2, base + 3)
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

/// T1 — receipt then issue in two batches; pool ends at 60% balance/qty.
#[tokio::test]
async fn t1_receipt_then_issue_lands_correctly() {
    let p = pool().await;
    let (pool_id, ap_id, cogs_id, _) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_id, "wac_pool_max", "inv_value_raw"),
            (ap_id, "ap_max", "credit_normal"),
            (cogs_id, "cogs_max", "debit_normal"),
        ],
    )
    .await;

    sqlx::query("SELECT envelope_idx FROM post_batch_wac_shmem_maximal($1::jsonb)")
        .bind(serde_json::json!([{
            "envelope_idx": 0,
            "kind": "wac_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 100,
            "unit_cost": 1000,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }]))
        .execute(&p)
        .await
        .expect("receipt batch");

    let (bal, qty) = lookup(&p, pool_id).await;
    assert_eq!(bal, Some(100_000), "T1: pool balance after receipt");
    assert_eq!(qty, Some(100), "T1: pool qty after receipt");

    sqlx::query("SELECT envelope_idx FROM post_batch_wac_shmem_maximal($1::jsonb)")
        .bind(serde_json::json!([{
            "envelope_idx": 0,
            "kind": "wac_issue",
            "debit_account_id": cogs_id,
            "credit_account_id": pool_id,
            "qty": 40,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }]))
        .execute(&p)
        .await
        .expect("issue batch");

    let (bal, qty) = lookup(&p, pool_id).await;
    assert_eq!(bal, Some(60_000), "T1: pool balance after issue (40 @ avg 1000)");
    assert_eq!(qty, Some(60), "T1: pool qty after issue");

    let (cogs_bal, _) = lookup(&p, cogs_id).await;
    assert_eq!(cogs_bal, Some(40_000), "T1: COGS captures issued cost");

    wait_for_drain(&p, 2000).await;

    let drift: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(ABS(drift)), 0)::bigint FROM ledger_shmem_recon() \
         WHERE account_id = ANY($1::bigint[])",
    )
    .bind(&[pool_id, ap_id, cogs_id][..])
    .fetch_one(&p)
    .await
    .expect("recon");
    assert_eq!(drift, 0, "T1: recon drift must be 0 across all touched accounts");

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// T2 — cross-batch sequencing: 3 receipts at varying unit_costs then 1 issue.
#[tokio::test]
async fn t2_cross_batch_running_avg_via_shmem() {
    let p = pool().await;
    let (pool_id, ap_id, cogs_id, _) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_id, "wac_pool_max", "inv_value_raw"),
            (ap_id, "ap_max", "credit_normal"),
            (cogs_id, "cogs_max", "debit_normal"),
        ],
    )
    .await;

    for (qty, uc) in [(100i64, 1000i64), (50, 1200), (50, 800)] {
        sqlx::query("SELECT envelope_idx FROM post_batch_wac_shmem_maximal($1::jsonb)")
            .bind(serde_json::json!([{
                "envelope_idx": 0,
                "kind": "wac_receipt",
                "debit_account_id": pool_id,
                "credit_account_id": ap_id,
                "qty": qty,
                "unit_cost": uc,
                "idempotency_key": Uuid::new_v4().to_string(),
                "business_date": "2026-05-13",
            }]))
            .execute(&p)
            .await
            .expect("receipt");
    }

    let (bal, qty) = lookup(&p, pool_id).await;
    assert_eq!(bal, Some(200_000), "T2: pool balance after 3 receipts");
    assert_eq!(qty, Some(200), "T2: pool qty after 3 receipts");

    wait_for_drain(&p, 2000).await;

    sqlx::query("SELECT envelope_idx FROM post_batch_wac_shmem_maximal($1::jsonb)")
        .bind(serde_json::json!([{
            "envelope_idx": 0,
            "kind": "wac_issue",
            "debit_account_id": cogs_id,
            "credit_account_id": pool_id,
            "qty": 80,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }]))
        .execute(&p)
        .await
        .expect("issue");

    let (bal, qty) = lookup(&p, pool_id).await;
    assert_eq!(bal, Some(120_000), "T2: pool balance after issue (200_000 - 80*1000)");
    assert_eq!(qty, Some(120), "T2: pool qty after issue");

    let (cogs_bal, _) = lookup(&p, cogs_id).await;
    assert_eq!(cogs_bal, Some(80_000), "T2: COGS = issued qty × running_avg");

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// T3 — in-batch running avg: 2 receipts + 1 issue in ONE batch.
#[tokio::test]
async fn t3_in_batch_running_avg() {
    let p = pool().await;
    let (pool_id, ap_id, cogs_id, _) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_id, "wac_pool_max", "inv_value_raw"),
            (ap_id, "ap_max", "credit_normal"),
            (cogs_id, "cogs_max", "debit_normal"),
        ],
    )
    .await;

    let envelopes = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "wac_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 100,
            "unit_cost": 1000,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        },
        {
            "envelope_idx": 1,
            "kind": "wac_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 100,
            "unit_cost": 1500,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        },
        {
            "envelope_idx": 2,
            "kind": "wac_issue",
            "debit_account_id": cogs_id,
            "credit_account_id": pool_id,
            "qty": 50,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        },
    ]);
    sqlx::query("SELECT envelope_idx FROM post_batch_wac_shmem_maximal($1::jsonb)")
        .bind(envelopes)
        .execute(&p)
        .await
        .expect("mixed batch");

    let (bal, qty) = lookup(&p, pool_id).await;
    assert_eq!(bal, Some(187_500), "T3: pool balance after 2 receipts + 1 issue");
    assert_eq!(qty, Some(150), "T3: pool qty");

    let (cogs_bal, _) = lookup(&p, cogs_id).await;
    assert_eq!(
        cogs_bal,
        Some(62_500),
        "T3: COGS uses in-batch running avg (1250), not first (1000) or last (1500)"
    );

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// T4 — idempotent replay: same idempotency_key submitted twice. Second
/// batch returns `idempotent_replay`; shmem not double-applied.
#[tokio::test]
async fn t4_idempotent_replay() {
    let p = pool().await;
    let (pool_id, ap_id, _, _) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_id, "wac_pool_max", "inv_value_raw"),
            (ap_id, "ap_max", "credit_normal"),
        ],
    )
    .await;

    let idem = Uuid::new_v4();
    let envelopes = serde_json::json!([{
        "envelope_idx": 0,
        "kind": "wac_receipt",
        "debit_account_id": pool_id,
        "credit_account_id": ap_id,
        "qty": 100,
        "unit_cost": 1000,
        "idempotency_key": idem.to_string(),
        "business_date": "2026-05-13",
    }]);

    sqlx::query("SELECT envelope_idx FROM post_batch_wac_shmem_maximal($1::jsonb)")
        .bind(&envelopes)
        .execute(&p)
        .await
        .expect("first apply");

    let (bal, qty) = lookup(&p, pool_id).await;
    assert_eq!(bal, Some(100_000), "T4: first apply lands");
    assert_eq!(qty, Some(100), "T4: first apply qty");

    let res = sqlx::query(
        "SELECT envelope_idx, status FROM post_batch_wac_shmem_maximal($1::jsonb)",
    )
    .bind(&envelopes)
    .fetch_all(&p)
    .await
    .expect("replay");
    let status: String = res[0].get("status");
    assert_eq!(status, "idempotent_replay", "T4: replay status");

    let (bal, qty) = lookup(&p, pool_id).await;
    assert_eq!(bal, Some(100_000), "T4: replay did NOT double-apply balance");
    assert_eq!(qty, Some(100), "T4: replay did NOT double-apply qty");

    cleanup(&p, &[pool_id, ap_id]).await;
}

/// T5 — multi-writer fan-in: 8 concurrent backends each post 50 receipts.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn t5_multi_writer_fan_in_coupled_writes() {
    let p = pool().await;
    let (pool_id, ap_id, _, _) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_id, "wac_pool_max", "inv_value_raw"),
            (ap_id, "ap_max", "credit_normal"),
        ],
    )
    .await;

    let writers = 8;
    let envelopes_per_writer: i64 = 50;
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
                    "kind": "wac_receipt",
                    "debit_account_id": pid,
                    "credit_account_id": aid,
                    "qty": qty_per_envelope,
                    "unit_cost": unit_cost,
                    "idempotency_key": Uuid::new_v4().to_string(),
                    "business_date": "2026-05-13",
                }]);
                let _ = sqlx::query(
                    "SELECT envelope_idx FROM post_batch_wac_shmem_maximal($1::jsonb)",
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

    let unit_check = bal.unwrap() / qty.unwrap();
    assert_eq!(unit_check, unit_cost, "T5: implied unit_cost matches");

    wait_for_drain(&p, 5000).await;

    let drift: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(ABS(drift)), 0)::bigint FROM ledger_shmem_recon() \
         WHERE account_id = ANY($1::bigint[])",
    )
    .bind(&[pool_id, ap_id][..])
    .fetch_one(&p)
    .await
    .expect("recon");
    assert_eq!(drift, 0, "T5: recon drift must be 0 after concurrent fan-in");

    cleanup(&p, &[pool_id, ap_id]).await;
}
