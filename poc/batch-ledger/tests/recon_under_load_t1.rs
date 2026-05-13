//! acct-7eph / M10.C5 — `ledger_shmem_recon` snapshot consistency
//! under concurrent writers.
//!
//! # Background
//!
//! `ledger_shmem_recon()` (extension function) takes the SHARED LWLock,
//! walks all occupied buckets, and per-cell loads the `(balance, qty)`
//! pair. Phase 2 (SPI lookup against `posting_lines`) runs outside
//! the lock.
//!
//! Pre-B4-prep this was a torn-read site: each cell loaded `balance`
//! and `qty` as separate `AtomicI64`s, so concurrent writers could
//! cause recon to observe a `(balance, qty)` pair that was never a
//! real coupled state. The recon caller would then see a `drift` value
//! that wasn't a function of any real apply.
//!
//! Post-B4-prep the cell read is a single `AtomicU128.load(Acquire)` +
//! `unpack_bal_qty` (cf. `Bucket::balance_qty` and I11 in INVARIANTS.md).
//! Each recon observation is therefore a real coupled snapshot.
//!
//! # Test protocol
//!
//! Create one real accounts row (so `ledger_shmem_recon` returns a
//! non-NULL `ledger_balance`). Worker thread A loops
//! `ledger_apply_balance_delta(+1000, +1)` against the cell. Worker
//! thread B loops `ledger_shmem_recon()` and records each observed
//! `(shmem_balance, shmem_qty, drift)` triple for that account.
//!
//! Invariants (post-B4-prep):
//! - **R1.a** every observed `(shmem_balance, shmem_qty)` pair satisfies
//!   the coupled invariant `shmem_balance % 1000 == 0 && shmem_qty *
//!   1000 == shmem_balance`. Failure = torn read at the recon site.
//! - **R1.b** every observed `drift` equals `shmem_balance - 0`
//!   (no `posting_lines` rows exist for this account), so each drift
//!   is also a multiple of 1000.
//! - **R2** post-quiescence (workers joined): one final recon shows
//!   `(shmem_balance, shmem_qty) = (N*1000, N)` exactly where N is
//!   the total successful apply count.
//!
//! # Location
//!
//! Sibling of `seqlock_torn_read_t1.rs`. The extension crate is pgrx-
//! cdylib without sqlx/tokio; we drive the live extension via SQL.

use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn db_url() -> String {
    std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
}

/// Create one accounts row with a unique code (UUID-derived) so the test
/// can run alongside other test binaries without colliding. Returns the
/// auto-generated `id`.
async fn create_unique_account(pool: &sqlx::PgPool) -> i64 {
    let code = format!("recon_t1_{}", Uuid::new_v4().simple());
    let row = sqlx::query(
        "INSERT INTO accounts (code, currency, kind) \
         VALUES ($1, 'USD', 'debit_normal') RETURNING id",
    )
    .bind(&code)
    .fetch_one(pool)
    .await
    .expect("create accounts row");
    row.get::<i64, _>("id")
}

/// R1 — concurrent recon observations during sustained writer load
/// must all be coupled `(balance, qty)` pairs satisfying `balance =
/// qty * 1000`. Failure = torn read at the recon read site.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn r1_recon_consistency_under_concurrent_writers() {
    let url = db_url();
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("ext");

    let acct = create_unique_account(&pool).await;
    // ledger_shmem_recon filters to the PoC convention (1, 1, 1).
    let period: i32 = 1;

    // No prior shmem cell exists. The first apply will lazy-load from
    // rollup (empty for this fresh account_id), seeding at (delta, qty).
    let stop = Arc::new(AtomicBool::new(false));
    let writer_ops = Arc::new(AtomicU64::new(0));
    let recon_reads = Arc::new(AtomicU64::new(0));
    let torn_count = Arc::new(AtomicU64::new(0));
    let bad_drift_count = Arc::new(AtomicU64::new(0));
    let samples: Arc<std::sync::Mutex<Vec<(i64, i64, Option<i64>)>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    // Spawn 4 writers + 2 recon-reader.
    let n_writers = 4;
    let mut handles = Vec::new();
    for _ in 0..n_writers {
        let url = url.clone();
        let stop = stop.clone();
        let ops = writer_ops.clone();
        let h = tokio::spawn(async move {
            let p = PgPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await
                .expect("w connect");
            while !stop.load(Ordering::Relaxed) {
                if sqlx::query(
                    "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 1000, 1)",
                )
                .bind(acct)
                .bind(period)
                .execute(&p)
                .await
                .is_ok()
                {
                    ops.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        handles.push(h);
    }

    for _ in 0..2 {
        let url = url.clone();
        let stop = stop.clone();
        let reads = recon_reads.clone();
        let torn = torn_count.clone();
        let bad_drift = bad_drift_count.clone();
        let samples = samples.clone();
        let h = tokio::spawn(async move {
            let p = PgPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await
                .expect("r connect");
            while !stop.load(Ordering::Relaxed) {
                // Recon returns one row per occupied cell at (1, 1, 1);
                // filter to our test account inside the query so we
                // don't pull all other tests' synthetic-offset cells.
                let rows = sqlx::query(
                    "SELECT shmem_balance, shmem_qty, drift \
                       FROM ledger_shmem_recon() \
                      WHERE account_id = $1",
                )
                .bind(acct)
                .fetch_all(&p)
                .await;
                if let Ok(rs) = rows {
                    for r in rs {
                        let bal: i64 = r.get("shmem_balance");
                        let qty: i64 = r.get("shmem_qty");
                        let drift: Option<i64> = r.get("drift");
                        reads.fetch_add(1, Ordering::Relaxed);
                        // R1.a — coupled (balance, qty) pair.
                        if bal % 1000 != 0 || qty.wrapping_mul(1000) != bal {
                            torn.fetch_add(1, Ordering::Relaxed);
                            if let Ok(mut v) = samples.lock() {
                                if v.len() < 20 {
                                    v.push((bal, qty, drift));
                                }
                            }
                        }
                        // R1.b — drift equals shmem_balance (no posting_lines
                        // for this account), so drift is a multiple of 1000.
                        if let Some(d) = drift {
                            if d != bal {
                                bad_drift.fetch_add(1, Ordering::Relaxed);
                            }
                            if d % 1000 != 0 {
                                // Already counted as torn (bal % 1000 != 0
                                // implies d % 1000 != 0 since d == bal),
                                // but assert explicitly for safety.
                                torn.fetch_add(0, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        });
        handles.push(h);
    }

    let secs: u64 = std::env::var("R1_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(12);
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if torn_count.load(Ordering::Relaxed) > 0 {
            break;
        }
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }

    let w = writer_ops.load(Ordering::Relaxed);
    let r = recon_reads.load(Ordering::Relaxed);
    let t = torn_count.load(Ordering::Relaxed);
    let bd = bad_drift_count.load(Ordering::Relaxed);

    eprintln!(
        "R1 summary: writers={n_writers} recon_readers=2 \
         writer_ops={w} recon_observations={r} \
         torn_pairs={t} bad_drift={bd} duration_secs={secs} \
         account_id={acct}"
    );
    if !samples.lock().unwrap().is_empty() {
        eprintln!("R1 torn samples (up to 20):");
        for (b, q, d) in samples.lock().unwrap().iter() {
            eprintln!(
                "  balance={b} qty={q} drift={d:?}  bal%1000={}  qty*1000-bal={}",
                b % 1000,
                q.wrapping_mul(1000) - b
            );
        }
    }

    assert_eq!(
        t, 0,
        "R1.a: recon observed {t} torn (balance, qty) pairs out of {r} observations \
         (writer_ops={w}); samples printed above. Post-B4-prep this must be 0 \
         (recon site reads `Bucket::balance_qty` as a single AtomicU128 load)."
    );
    assert_eq!(
        bd, 0,
        "R1.b: recon observed {bd} drift values inconsistent with shmem_balance \
         (drift should equal shmem_balance since no posting_lines exist for this \
         account). Reads should be self-consistent within a single recon row."
    );
}

/// R2 — post-quiescence recon agrees with apply count exactly.
/// After workers join, the cell's `(balance, qty)` must equal
/// `(N*1000, N)` where N is the successful apply count.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn r2_post_quiescence_recon_exact() {
    let url = db_url();
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("ext");

    let acct = create_unique_account(&pool).await;
    // ledger_shmem_recon filters to the PoC convention (1, 1, 1).
    let period: i32 = 1;

    // 4 writers × 200 applies each = 800 expected applies.
    let n_writers: usize = 4;
    let per_writer: usize = 200;
    let mut handles = Vec::new();
    let writer_ok = Arc::new(AtomicU64::new(0));
    for _ in 0..n_writers {
        let url = url.clone();
        let ok = writer_ok.clone();
        let h = tokio::spawn(async move {
            let p = PgPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await
                .expect("w connect");
            for _ in 0..per_writer {
                if sqlx::query(
                    "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 1000, 1)",
                )
                .bind(acct)
                .bind(period)
                .execute(&p)
                .await
                .is_ok()
                {
                    ok.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        handles.push(h);
    }
    for h in handles {
        let _ = h.await;
    }

    let n_ok = writer_ok.load(Ordering::Relaxed) as i64;
    assert!(
        n_ok >= (n_writers * per_writer) as i64,
        "R2: expected at least {} successful applies, got {n_ok}",
        n_writers * per_writer
    );

    let row = sqlx::query(
        "SELECT shmem_balance, shmem_qty, drift FROM ledger_shmem_recon() \
         WHERE account_id = $1",
    )
    .bind(acct)
    .fetch_one(&pool)
    .await
    .expect("post-quiescence recon");

    let bal: i64 = row.get("shmem_balance");
    let qty: i64 = row.get("shmem_qty");
    let drift: Option<i64> = row.get("drift");

    eprintln!(
        "R2 summary: writers={n_writers} per_writer={per_writer} \
         applies_ok={n_ok} balance={bal} qty={qty} drift={drift:?}"
    );

    let expected_bal = n_ok * 1000;
    let expected_qty = n_ok;
    assert_eq!(
        bal, expected_bal,
        "R2: post-quiescence balance mismatch: expected {expected_bal} got {bal} \
         (applies_ok={n_ok}); lost update detected"
    );
    assert_eq!(
        qty, expected_qty,
        "R2: post-quiescence qty mismatch: expected {expected_qty} got {qty} \
         (applies_ok={n_ok}); lost update detected"
    );
    // drift == shmem_balance since no posting_lines exist for acct.
    assert_eq!(
        drift,
        Some(expected_bal),
        "R2: drift mismatch: expected Some({expected_bal}) got {drift:?}"
    );
}
