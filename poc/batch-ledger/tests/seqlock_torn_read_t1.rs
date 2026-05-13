//! acct-zo4t / M10.B4-prep — torn-read prevention on Bucket (balance, qty).
//!
//! # Background
//!
//! M9's hot path (`try_update_existing`) acquires `PgLwLock::share()`
//! and does `balance.fetch_add` then `qty.fetch_add` as two separate
//! atomic ops. Readers (`ledger_balance_lookup`, `ledger_shmem_recon`,
//! `do_drain_tick`, `ledger_shmem_dirty_count`, `ledger_shmem_drained_count`)
//! load `balance` and `qty` as two separate atomic loads.
//!
//! Under concurrent writers, a reader sampling between W1's
//! `balance.fetch_add` and W1's `qty.fetch_add` observes a `(balance, qty)`
//! pair that was never a real coupled state. For simple-balance
//! workloads with `qty_delta=0` the bug is invisible. For WAC (B4 —
//! `post_batch_wac_shmem`) where `unit_cost = pool_value / pool_qty`,
//! a torn read produces a wrong unit cost: silent variance leak.
//!
//! # Test protocol
//!
//! Writers apply `(+1000, +1)` repeatedly on the same cell. The legal
//! coupled states form `{ (k*1000, k) for k ∈ ℕ }`. Any observation
//! that does NOT satisfy `balance % 1000 == 0 && qty * 1000 == balance`
//! is a torn read — provable bug.
//!
//! # Falsification
//!
//! Pre-B4-prep: T2 should observe ≥1 torn read within seconds.
//! Post-B4-prep (AtomicU128 packed): T2 must observe 0 torn reads
//! across millions of observations.
//!
//! # Location
//!
//! Sibling of `rollback_correctness_t1.rs` / `invariants_t1.rs` /
//! `transactional_t1.rs`. The `ledger-extension` crate is pgrx-cdylib
//! without sqlx/tokio; we drive the live extension via SQL from this
//! sqlx-based crate.

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

fn synthetic_offset() -> i64 {
    let u = Uuid::new_v4();
    let bytes = u.as_bytes();
    let hash = ((bytes[0] as i64) << 24)
        | ((bytes[1] as i64) << 16)
        | ((bytes[2] as i64) << 8)
        | (bytes[3] as i64);
    900_000_000_i64 + (hash.abs() % 10_000_000)
}

/// T1 — single-writer correctness. Sequential apply, lookup after each;
/// each observation must be exactly `(n*1000, n)` for the n-th apply.
/// Sanity that the basic invariant holds when there's no concurrency.
#[tokio::test]
async fn t1_single_writer_correctness() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("ext");

    let acct = synthetic_offset();
    let period: i32 = 9_001;

    // Sanity: no prior cell.
    let pre: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("pre");
    let pre_bal = pre.unwrap_or(0);
    let pre_qty: i64 = sqlx::query_scalar(
        "SELECT COALESCE(qty, 0) FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("pre qty");

    for k in 1..=20i64 {
        sqlx::query(
            "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 1000, 1)",
        )
        .bind(acct)
        .bind(period)
        .execute(&pool)
        .await
        .expect("apply");

        let row = sqlx::query(
            "SELECT balance, qty FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
        )
        .bind(acct)
        .bind(period)
        .fetch_one(&pool)
        .await
        .expect("lookup");
        let bal: Option<i64> = row.get("balance");
        let qty: Option<i64> = row.get("qty");
        assert_eq!(
            bal,
            Some(pre_bal + k * 1000),
            "T1: bal mismatch at k={k}: expected {}, got {bal:?}",
            pre_bal + k * 1000
        );
        assert_eq!(
            qty,
            Some(pre_qty + k),
            "T1: qty mismatch at k={k}: expected {}, got {qty:?}",
            pre_qty + k
        );
    }
}

/// Drive one writer task: loop `apply(+1000, +1)` for `duration`.
async fn writer_task(
    url: String,
    acct: i64,
    period: i32,
    stop: Arc<AtomicBool>,
    ops_counter: Arc<AtomicU64>,
) {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("writer connect");
    while !stop.load(Ordering::Relaxed) {
        let res = sqlx::query(
            "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 1000, 1)",
        )
        .bind(acct)
        .bind(period)
        .execute(&pool)
        .await;
        match res {
            Ok(_) => {
                ops_counter.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                // Pool exhaustion or transient — continue.
            }
        }
    }
}

/// Drive one reader task: loop `ledger_balance_lookup` and record any
/// pair that violates `balance % 1000 == 0 && qty * 1000 == balance`.
async fn reader_task(
    url: String,
    acct: i64,
    period: i32,
    pre_bal: i64,
    pre_qty: i64,
    stop: Arc<AtomicBool>,
    reads_counter: Arc<AtomicU64>,
    torn_counter: Arc<AtomicU64>,
    sample_capture: Arc<std::sync::Mutex<Vec<(i64, i64)>>>,
) {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("reader connect");
    while !stop.load(Ordering::Relaxed) {
        let row = sqlx::query(
            "SELECT balance, qty FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
        )
        .bind(acct)
        .bind(period)
        .fetch_one(&pool)
        .await;
        if let Ok(r) = row {
            let bal: Option<i64> = r.get("balance");
            let qty: Option<i64> = r.get("qty");
            if let (Some(b), Some(q)) = (bal, qty) {
                let delta_bal = b - pre_bal;
                let delta_qty = q - pre_qty;
                if delta_bal % 1000 != 0 || delta_qty * 1000 != delta_bal {
                    torn_counter.fetch_add(1, Ordering::Relaxed);
                    if let Ok(mut v) = sample_capture.lock() {
                        if v.len() < 20 {
                            v.push((b, q));
                        }
                    }
                }
                reads_counter.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// T2 — **THE FALSIFICATION GATE.** Concurrent writers + readers on
/// one cell. Each apply adds `(+1000, +1)`; the legal coupled state
/// set is `{(pre_bal + k*1000, pre_qty + k)}`. Any reader observation
/// outside this set is a torn read — pre-B4-prep this should fire
/// within seconds; post-B4-prep this must NEVER fire over the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn t2_torn_read_probe() {
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

    let acct = synthetic_offset();
    let period: i32 = 9_002;

    // Capture pre-state. Other test binaries may have touched the same
    // synthetic-offset range, so use delta-relative invariants.
    let row = sqlx::query(
        "SELECT balance, qty FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("pre");
    let pre_bal: i64 = row.get::<Option<i64>, _>("balance").unwrap_or(0);
    let pre_qty: i64 = row.get::<Option<i64>, _>("qty").unwrap_or(0);

    // Prime the cell with one apply so subsequent writers all hit the
    // SHARED-LWLock fast path (try_update_existing's fetch_add pair).
    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 1000, 1)",
    )
    .bind(acct)
    .bind(period)
    .execute(&pool)
    .await
    .expect("prime");

    let stop = Arc::new(AtomicBool::new(false));
    let writer_ops = Arc::new(AtomicU64::new(0));
    let reader_reads = Arc::new(AtomicU64::new(0));
    let torn_count = Arc::new(AtomicU64::new(0));
    let torn_samples: Arc<std::sync::Mutex<Vec<(i64, i64)>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    // Spawn 8 writers, 4 readers.
    let n_writers = 8;
    let n_readers = 4;
    let mut handles = Vec::new();
    for _ in 0..n_writers {
        let h = tokio::spawn(writer_task(
            url.clone(),
            acct,
            period,
            stop.clone(),
            writer_ops.clone(),
        ));
        handles.push(h);
    }
    for _ in 0..n_readers {
        let h = tokio::spawn(reader_task(
            url.clone(),
            acct,
            period,
            pre_bal,
            pre_qty,
            stop.clone(),
            reader_reads.clone(),
            torn_count.clone(),
            torn_samples.clone(),
        ));
        handles.push(h);
    }

    // Run window. Long enough to catch a torn read in the pre-fix
    // build with high confidence; bounded so CI doesn't hang.
    let run_secs: u64 = std::env::var("T2_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(15);
    let deadline = Instant::now() + Duration::from_secs(run_secs);
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Early exit if we've already seen a torn read (pre-fix path).
        if torn_count.load(Ordering::Relaxed) > 0 {
            break;
        }
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }

    let w = writer_ops.load(Ordering::Relaxed);
    let r = reader_reads.load(Ordering::Relaxed);
    let t = torn_count.load(Ordering::Relaxed);
    let samples = torn_samples.lock().unwrap().clone();

    eprintln!(
        "T2 summary: writers={n_writers} readers={n_readers} \
         writer_ops={w} reader_reads={r} torn_count={t} \
         duration_secs={run_secs}"
    );
    if !samples.is_empty() {
        eprintln!("T2 torn samples (up to 20):");
        for (b, q) in &samples {
            let db = b - pre_bal;
            let dq = q - pre_qty;
            eprintln!(
                "  (balance={b}, qty={q}) delta=({db}, {dq}) \
                 db%1000={} dq*1000-db={}",
                db % 1000,
                dq * 1000 - db
            );
        }
    }

    assert_eq!(
        t, 0,
        "T2: observed {t} torn reads out of {r} observations \
         (writer_ops={w}); samples printed above. \
         This is the M10.B4-prep falsification gate — pre-fix this \
         test must fail, post-fix this assertion must hold."
    );
}

/// T3 — M3 sanity rerun: 8 writers × 100 applies on shared cell;
/// final balance == 800_000, qty == 800. Pinning that the fix doesn't
/// regress lost-update absence.
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn t3_eight_writer_composition() {
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

    let acct = synthetic_offset();
    let period: i32 = 9_003;

    let row = sqlx::query(
        "SELECT balance, qty FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("pre");
    let pre_bal: i64 = row.get::<Option<i64>, _>("balance").unwrap_or(0);
    let pre_qty: i64 = row.get::<Option<i64>, _>("qty").unwrap_or(0);

    let n_writers: i64 = 8;
    let per_writer: i64 = 100;

    let mut handles = Vec::new();
    for _ in 0..n_writers {
        let url = url.clone();
        let h = tokio::spawn(async move {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await
                .expect("w connect");
            for _ in 0..per_writer {
                sqlx::query(
                    "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 1000, 1)",
                )
                .bind(acct)
                .bind(period)
                .execute(&pool)
                .await
                .expect("apply");
            }
        });
        handles.push(h);
    }
    for h in handles {
        let _ = h.await;
    }

    let row = sqlx::query(
        "SELECT balance, qty FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("post");
    let post_bal: Option<i64> = row.get("balance");
    let post_qty: Option<i64> = row.get("qty");

    let expected_bal = pre_bal + n_writers * per_writer * 1000;
    let expected_qty = pre_qty + n_writers * per_writer;
    assert_eq!(
        post_bal,
        Some(expected_bal),
        "T3: balance lost-update — expected {expected_bal} got {post_bal:?}"
    );
    assert_eq!(
        post_qty,
        Some(expected_qty),
        "T3: qty lost-update — expected {expected_qty} got {post_qty:?}"
    );
}

/// T5 — pathological load: 16 writers + 16 readers on same cell for a
/// short window. Bounded-time progress: no livelock, all readers
/// observe coupled pairs only.
#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
async fn t5_high_concurrency_bounded() {
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

    let acct = synthetic_offset();
    let period: i32 = 9_005;

    let row = sqlx::query(
        "SELECT balance, qty FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("pre");
    let pre_bal: i64 = row.get::<Option<i64>, _>("balance").unwrap_or(0);
    let pre_qty: i64 = row.get::<Option<i64>, _>("qty").unwrap_or(0);

    // Prime.
    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 1000, 1)",
    )
    .bind(acct)
    .bind(period)
    .execute(&pool)
    .await
    .expect("prime");

    let stop = Arc::new(AtomicBool::new(false));
    let writer_ops = Arc::new(AtomicU64::new(0));
    let reader_reads = Arc::new(AtomicU64::new(0));
    let torn_count = Arc::new(AtomicU64::new(0));
    let torn_samples = Arc::new(std::sync::Mutex::new(Vec::new()));

    let mut handles = Vec::new();
    for _ in 0..16 {
        handles.push(tokio::spawn(writer_task(
            url.clone(),
            acct,
            period,
            stop.clone(),
            writer_ops.clone(),
        )));
    }
    for _ in 0..16 {
        handles.push(tokio::spawn(reader_task(
            url.clone(),
            acct,
            period,
            pre_bal,
            pre_qty,
            stop.clone(),
            reader_reads.clone(),
            torn_count.clone(),
            torn_samples.clone(),
        )));
    }

    let run_secs: u64 = std::env::var("T5_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    tokio::time::sleep(Duration::from_secs(run_secs)).await;
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }

    let t = torn_count.load(Ordering::Relaxed);
    let r = reader_reads.load(Ordering::Relaxed);
    let w = writer_ops.load(Ordering::Relaxed);
    eprintln!("T5 summary: writers=16 readers=16 writer_ops={w} reader_reads={r} torn_count={t}");
    assert_eq!(
        t, 0,
        "T5: high-concurrency torn reads = {t} (of {r} observations); bounded-time correctness violated"
    );
}
