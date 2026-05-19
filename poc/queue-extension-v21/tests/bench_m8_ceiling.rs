//! acct-cyu6 — simple ceiling probe: 100k PO receipts, two SKU distributions,
//! persisted payload, fire-and-forget submission, bulk terminal-state poll.
//!
//! Single-replication probe to see what the v2.1 architecture actually does
//! when pushed with a simple workload at current GUCs. NO 5-replication, NO
//! GUC sweep, NO N-sweep — that's M8.2/M8.3 territory.
//!
//! Phase 1: generator writes deterministic JSONL to bench/payloads/.
//!   UUIDv5 over namespace + "po-{dist}-N{count}:{iter}" → byte-stable file.
//! Phase 2: replay runner reads JSONL, fires open-loop, polls terminal state
//!   post-submission, writes results JSON to bench/results-m8-ceiling/.
//!
//! Run:
//!   docker restart acct-postgres && \
//!   taskset -c 0-7 cargo test --release --test bench_m8_ceiling \
//!     --features pg18 --no-default-features \
//!     -- --ignored --nocapture --test-threads=1
//!
//! Env knobs:
//!   POC_CYU6_N        — backend count (default = nproc)
//!   POC_CYU6_COUNT    — envelopes per cell (default 100_000)
//!   POC_CYU6_DRAIN_S  — terminal-state drain timeout secs (default 120)

#![cfg(test)]

#[path = "common/pg_locks_sampler.rs"]
mod pg_locks_sampler;

use pg_locks_sampler::PgLocksSampler;
use hdrhistogram::Histogram;
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

const POC_DSN: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc_queue_v21";
// Stable namespace for UUIDv5 derivation — DO NOT change once payloads exist.
// Value is `uuid::Uuid::new_v5(&NAMESPACE_DNS, b"poc-v21-ceiling-bench")`.
const NAMESPACE: Uuid = Uuid::from_bytes([
    0x1f, 0x6f, 0x5b, 0xb9, 0x2c, 0xd0, 0x5b, 0xc4,
    0xb7, 0x3e, 0x4a, 0x1c, 0x52, 0x9e, 0x8a, 0x77,
]);

fn env_n() -> usize {
    std::env::var("POC_CYU6_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(8)
        })
}

fn env_count() -> usize {
    std::env::var("POC_CYU6_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000)
}

fn env_drain_timeout() -> Duration {
    Duration::from_secs(
        std::env::var("POC_CYU6_DRAIN_S")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(120),
    )
}

// ──────────────────────────────────────────────────────────────────────
// Distribution
// ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum Distribution {
    Single,             // sku=1 only
    Uniform(usize),     // sku = (iter % n) + 1
    Normal(usize),      // sku ~ Gaussian(mean=n/2, sigma=n/6) clipped to [1, n]
}

/// Legacy alias kept so historical cyu6_run_uniform_1000 test still compiles.
#[allow(non_upper_case_globals)]
const Uniform1000: Distribution = Distribution::Uniform(1000);

impl Distribution {
    fn name(&self) -> String {
        match self {
            Distribution::Single => "single".to_string(),
            Distribution::Uniform(n) => {
                // Keep legacy `uniform_1000` filename for back-compat with
                // existing 100k/500k payload files.
                if *n == 1000 {
                    "uniform_1000".to_string()
                } else {
                    format!("uniform_{}", n)
                }
            }
            Distribution::Normal(n) => format!("normal_{}", n),
        }
    }
    fn sku_for(&self, iter: usize) -> i64 {
        match self {
            Distribution::Single => 1,
            Distribution::Uniform(n) => ((iter % n) as i64) + 1,
            Distribution::Normal(n) => {
                // Box-Muller via two iter-derived pseudo-uniform values.
                // Deterministic per iter so payload files are byte-stable.
                let s1 = (iter.wrapping_mul(2654435761).wrapping_add(0x9e3779b9)) as u64;
                let s2 = (iter.wrapping_mul(0xbf58476d1ce4e5b9).wrapping_add(0x94d049bb133111eb)) as u64;
                let u1 = ((s1 % 0xFFFFFF) as f64 + 1.0) / (0xFFFFFF as f64 + 1.0);
                let u2 = ((s2 % 0xFFFFFF) as f64 + 1.0) / (0xFFFFFF as f64 + 1.0);
                let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
                let mean = (*n as f64) / 2.0;
                let sigma = (*n as f64) / 6.0;
                let raw = mean + z * sigma;
                let clipped = raw.round().max(1.0).min(*n as f64) as i64;
                clipped
            }
        }
    }
}

// ──────────────────────────────────────────────────────────────────────
// Envelope on disk
// ──────────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct Envelope {
    correlation_id: Uuid,
    event_type: String,
    payload: serde_json::Value,
    pool_keys: serde_json::Value,
}

fn build_envelope(dist: Distribution, count: usize, iter: usize) -> Envelope {
    let sku = dist.sku_for(iter);
    let document_id = 9_000_000_i64 + iter as i64;
    let payload = serde_json::json!({
        "sku_id": sku,
        "location_id": 1,
        "qty": 1,
        "unit_cost": 100,
        "issue_id": iter as i64,
        "business_date_jdate": 20221,
        "doc_chrono": iter as i64,
        "document_id": document_id,
    });
    let pool_keys = serde_json::json!({
        "sku": [[sku, 1]],
        "wip": [],
    });
    let cid_seed = format!("po-{}-N{}:{}", dist.name(), count, iter);
    let correlation_id = Uuid::new_v5(&NAMESPACE, cid_seed.as_bytes());
    Envelope {
        correlation_id,
        event_type: "po_receipt".to_string(),
        payload,
        pool_keys,
    }
}

fn payload_path(dist: Distribution, count: usize) -> PathBuf {
    PathBuf::from(format!(
        "bench/payloads/po-{}-N{}.jsonl",
        dist.name(),
        count
    ))
}

fn meta_path(dist: Distribution, count: usize) -> PathBuf {
    PathBuf::from(format!(
        "bench/payloads/po-{}-N{}.meta.json",
        dist.name(),
        count
    ))
}

// ──────────────────────────────────────────────────────────────────────
// Phase 1: generator
// ──────────────────────────────────────────────────────────────────────

fn generate(dist: Distribution, count: usize) -> std::io::Result<()> {
    let path = payload_path(dist, count);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let started = Instant::now();
    let mut hasher = Sha256::new();
    let mut buf = String::with_capacity(64 * 1024);
    use std::io::Write;
    let file = std::fs::File::create(&path)?;
    let mut out = std::io::BufWriter::with_capacity(1 << 20, file);
    for iter in 0..count {
        let e = build_envelope(dist, count, iter);
        // Compose JSONL line. Use compact serialization (no indent) for
        // byte stability. Field order in the wrapping object matters for
        // sha256 stability — write in fixed order.
        buf.clear();
        let line = serde_json::json!({
            "correlation_id": e.correlation_id.to_string(),
            "event_type": e.event_type,
            "payload": e.payload,
            "pool_keys": e.pool_keys,
        });
        let s = serde_json::to_string(&line).expect("serialize");
        buf.push_str(&s);
        buf.push('\n');
        hasher.update(buf.as_bytes());
        out.write_all(buf.as_bytes())?;
    }
    out.flush()?;
    drop(out);
    let sha = hasher.finalize();
    let sha_hex = sha
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>();
    let elapsed_ms = started.elapsed().as_millis() as u64;
    let meta = serde_json::json!({
        "distribution": dist.name(),
        "count": count,
        "sha256": sha_hex,
        "elapsed_ms": elapsed_ms,
        "first_correlation_id": build_envelope(dist, count, 0).correlation_id.to_string(),
        "last_correlation_id": build_envelope(dist, count, count - 1).correlation_id.to_string(),
    });
    std::fs::write(meta_path(dist, count), serde_json::to_string_pretty(&meta)?)?;
    eprintln!(
        "==> generated {} ({} bytes, {} envelopes, sha256={}…)  in {}ms",
        path.display(),
        std::fs::metadata(&path)?.len(),
        count,
        &sha_hex[0..16],
        elapsed_ms,
    );
    Ok(())
}

fn read_payload(path: &Path) -> std::io::Result<Vec<Envelope>> {
    use std::io::BufRead;
    let f = std::fs::File::open(path)?;
    let r = std::io::BufReader::with_capacity(1 << 20, f);
    let mut out: Vec<Envelope> = Vec::new();
    for line in r.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value =
            serde_json::from_str(&line).expect("payload line is JSON");
        let cid_s = v["correlation_id"].as_str().expect("correlation_id");
        let envelope = Envelope {
            correlation_id: Uuid::parse_str(cid_s).expect("cid uuid"),
            event_type: v["event_type"].as_str().expect("event_type").to_string(),
            payload: v["payload"].clone(),
            pool_keys: v["pool_keys"].clone(),
        };
        out.push(envelope);
    }
    Ok(out)
}

// ──────────────────────────────────────────────────────────────────────
// Counter snapshot (compact copy from m82_statistical.rs)
// ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
struct Counters {
    superbatch_count: i64,
    total_envelopes: i64,
    max_envelope_count: i64,
    histogram_buckets: [i64; 8],
    force_pack_count: i64,
    ticks_total: i64,
    cross_sb_for_update_waits: i64,
    committer_drains_total: i64,
    committer_pipeline_ns_total: i64,
    committer_pipeline_count: i64,
    committer_takeover_count: i64,
    committer_tx_failures: i64,
    eject_count: i64,
    backpressure_count: i64,
}

async fn snap(pool: &PgPool) -> Counters {
    let mut c = Counters::default();
    let router: Vec<(String, f64)> =
        sqlx::query_as("SELECT stat_name, stat_value FROM poc_v21_router_stats()")
            .fetch_all(pool)
            .await
            .unwrap_or_default();
    for (k, v) in router {
        let vi = v as i64;
        match k.as_str() {
            "superbatch_count" => c.superbatch_count = vi,
            "total_envelopes" => c.total_envelopes = vi,
            "max_envelope_count" => c.max_envelope_count = vi,
            "force_pack_count" => c.force_pack_count = vi,
            "ticks_total" => c.ticks_total = vi,
            "cross_sb_for_update_waits" => c.cross_sb_for_update_waits = vi,
            s if s.starts_with("histogram_bucket_") => {
                if let Some(idx) = s.strip_prefix("histogram_bucket_").and_then(|t| t.parse::<usize>().ok()) {
                    if idx < 8 {
                        c.histogram_buckets[idx] = vi;
                    }
                }
            }
            _ => {}
        }
    }
    let committer: Vec<(String, f64)> =
        sqlx::query_as("SELECT stat_name, stat_value FROM poc_v21_committer_stats()")
            .fetch_all(pool)
            .await
            .unwrap_or_default();
    for (k, v) in committer {
        let vi = v as i64;
        match k.as_str() {
            "committer_drains_total" => c.committer_drains_total = vi,
            "committer_takeover_count" => c.committer_takeover_count = vi,
            "committer_tx_failures" => c.committer_tx_failures = vi,
            _ => {}
        }
    }
    c.committer_pipeline_ns_total =
        sqlx::query_scalar("SELECT poc_v21_committer_pipeline_ns_total()")
            .fetch_one(pool)
            .await
            .unwrap_or(0);
    c.committer_pipeline_count =
        sqlx::query_scalar("SELECT poc_v21_committer_pipeline_count()")
            .fetch_one(pool)
            .await
            .unwrap_or(0);
    c.eject_count = sqlx::query_scalar("SELECT poc_v21_eject_count()")
        .fetch_one(pool)
        .await
        .unwrap_or(0);
    c.backpressure_count = sqlx::query_scalar("SELECT poc_v21_backpressure_count()")
        .fetch_one(pool)
        .await
        .unwrap_or(0);
    c
}

fn delta(after: &Counters, before: &Counters) -> Counters {
    let mut buckets = [0i64; 8];
    for i in 0..8 {
        buckets[i] = after.histogram_buckets[i] - before.histogram_buckets[i];
    }
    Counters {
        superbatch_count: after.superbatch_count - before.superbatch_count,
        total_envelopes: after.total_envelopes - before.total_envelopes,
        // max_envelope_count is a lifetime max — report after-value, not delta
        max_envelope_count: after.max_envelope_count,
        histogram_buckets: buckets,
        force_pack_count: after.force_pack_count - before.force_pack_count,
        ticks_total: after.ticks_total - before.ticks_total,
        cross_sb_for_update_waits: after.cross_sb_for_update_waits - before.cross_sb_for_update_waits,
        committer_drains_total: after.committer_drains_total - before.committer_drains_total,
        committer_pipeline_ns_total: after.committer_pipeline_ns_total - before.committer_pipeline_ns_total,
        committer_pipeline_count: after.committer_pipeline_count - before.committer_pipeline_count,
        committer_takeover_count: after.committer_takeover_count - before.committer_takeover_count,
        committer_tx_failures: after.committer_tx_failures - before.committer_tx_failures,
        eject_count: after.eject_count - before.eject_count,
        backpressure_count: after.backpressure_count - before.backpressure_count,
    }
}

// ──────────────────────────────────────────────────────────────────────
// Phase 2: replay runner
// ──────────────────────────────────────────────────────────────────────

async fn build_pool(max_conns: u32) -> PgPool {
    PgPoolOptions::new()
        .max_connections(max_conns)
        .acquire_timeout(Duration::from_secs(60))
        .connect(POC_DSN)
        .await
        .expect("connect")
}

async fn reset_tables(pool: &PgPool) {
    // Drain shmem then TRUNCATE — mirrors m8_runner::reset_state without
    // the router_stats_reset (we want fresh counters, do it after).
    let drain_start = Instant::now();
    loop {
        let in_flight: i64 = sqlx::query_scalar(
            "SELECT (COALESCE((poc_v21_staging_state_counts()->>'pending')::bigint, 0) \
                   + COALESCE((poc_v21_staging_state_counts()->>'processing')::bigint, 0) \
                   + COALESCE((poc_v21_staging_state_counts()->>'routed')::bigint, 0))",
        )
        .fetch_one(pool)
        .await
        .unwrap_or(0);
        if in_flight == 0 {
            break;
        }
        if drain_start.elapsed() > Duration::from_secs(300) {
            eprintln!("reset_tables: drain didn't settle in 300s; in_flight={in_flight} — proceeding (TRUNCATE may deadlock)");
            break;
        }
        if drain_start.elapsed().as_secs() % 10 == 0 && drain_start.elapsed().as_millis() % 1000 < 100 {
            eprintln!("reset_tables: in_flight={in_flight} (drain {:.1}s)", drain_start.elapsed().as_secs_f64());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    sqlx::query(
        "TRUNCATE poc_v21_cost_layers, poc_v21_cost_depletions, \
                  poc_v21_cost_consumptions, poc_v21_posting_lines, \
                  poc_v21_posting_line_inventory, poc_v21_submission_status, \
                  poc_v21_pool_locks, poc_v21_wip_pool_locks, poc_v21_avg_pool_state, \
                  poc_v21_persistent_staging, poc_v21_standard_costs, \
                  poc_v21_sku_method_assignments \
                  RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await
    .expect("truncate");
    sqlx::query("SELECT poc_v21_router_stats_reset()")
        .execute(pool)
        .await
        .expect("router_stats_reset");
}

/// Pre-seed standard_costs at unit_cost=100 for the SKUs in this distribution.
/// PO receipts at standard cost don't strictly need a pre-existing standard
/// (the receipt establishes inventory), but the dispatcher reads
/// poc_v21_standard_costs for non-receipt events on the same SKU; pre-seed
/// to match prior bench convention.
async fn pre_seed(pool: &PgPool, dist: Distribution) {
    let skus: Vec<i64> = match dist {
        Distribution::Single => vec![1],
        Distribution::Uniform(n) => (1..=(n as i64)).collect(),
        Distribution::Normal(n) => (1..=(n as i64)).collect(),
    };
    // CRITICAL: assign every SKU to 'std' method explicitly. Default is
    // FIFO; without this row every PO receipt goes through FIFO and
    // hydrate_fifo_layers grows O(N²) per pool.
    let methods: Vec<String> = skus.iter().map(|_| "std".to_string()).collect();
    sqlx::query(
        "INSERT INTO poc_v21_sku_method_assignments (sku_id, method_id) \
         SELECT s, m FROM UNNEST($1::bigint[], $2::text[]) AS t(s, m) \
         ON CONFLICT (sku_id) DO UPDATE SET method_id = EXCLUDED.method_id",
    )
    .bind(&skus)
    .bind(&methods)
    .execute(pool)
    .await
    .expect("seed sku_method_assignments");
    sqlx::query(
        "INSERT INTO poc_v21_standard_costs (sku_id, location_id, unit_cost, effective_from) \
         SELECT s, 1, 100, now() - interval '1 day' \
           FROM UNNEST($1::bigint[]) AS t(s)",
    )
    .bind(&skus)
    .execute(pool)
    .await
    .expect("seed standard_costs");
}

struct RunResult {
    distribution: String,
    payload_sha256: Option<String>,
    n_envelopes: usize,
    n_backends: usize,
    submission_wall_ns: u128,
    drain_wall_ns: u128,
    end_to_end_wall_ns: u128,
    submitted: usize,
    submit_errors: usize,
    committed: usize,
    failed: usize,
    missing: usize,
    latency_hist: Histogram<u64>,
    counters: Counters,
    top_wait_event: Option<(String, String, i64)>,
}

async fn run_replay(
    pool: Arc<PgPool>,
    dist: Distribution,
    count: usize,
    n_backends: usize,
) -> RunResult {
    let payload_path = payload_path(dist, count);
    let meta_path = meta_path(dist, count);
    let envelopes = read_payload(&payload_path).expect("read payload");
    assert_eq!(envelopes.len(), count, "payload count mismatch");
    eprintln!(
        "==> [{}] read {} envelopes from {}",
        dist.name(),
        envelopes.len(),
        payload_path.display()
    );

    let payload_sha256 = std::fs::read_to_string(&meta_path)
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .and_then(|v| v.get("sha256").and_then(|x| x.as_str()).map(|s| s.to_string()));

    reset_tables(&pool).await;
    pre_seed(&pool, dist).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let counters_before = snap(&pool).await;
    let sampler = PgLocksSampler::spawn((*pool).clone(), 200).await;

    // Shared envelope index — atomic counter; each backend fetch_adds.
    let envelopes = Arc::new(envelopes);
    let cursor = Arc::new(AtomicUsize::new(0));
    // Per-envelope enqueue-return timestamp (ns since UNIX epoch). Vec
    // wrapped in a thread-safe slab — each backend writes its own slot.
    let enqueue_returned_ns: Arc<Vec<std::sync::Mutex<u64>>> = Arc::new(
        (0..count).map(|_| std::sync::Mutex::new(0)).collect(),
    );
    let submit_errors = Arc::new(AtomicUsize::new(0));

    let t_first = Instant::now();
    let mut handles = Vec::with_capacity(n_backends);
    for _backend_idx in 0..n_backends {
        let pool = pool.clone();
        let envelopes = envelopes.clone();
        let cursor = cursor.clone();
        let enqueue_returned_ns = enqueue_returned_ns.clone();
        let submit_errors = submit_errors.clone();
        handles.push(tokio::spawn(async move {
            let mut last_idx: usize = 0;
            loop {
                let idx = cursor.fetch_add(1, Ordering::Relaxed);
                if idx >= envelopes.len() {
                    break;
                }
                let e = &envelopes[idx];
                let res = sqlx::query(
                    "SELECT poc_v21_enqueue($1::uuid, $2::text, $3::jsonb, $4::jsonb, false)",
                )
                .bind(e.correlation_id)
                .bind(&e.event_type)
                .bind(&e.payload)
                .bind(&e.pool_keys)
                .execute(&*pool)
                .await;
                let ns = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);
                if res.is_ok() {
                    if let Ok(mut g) = enqueue_returned_ns[idx].lock() {
                        *g = ns;
                    }
                } else {
                    submit_errors.fetch_add(1, Ordering::Relaxed);
                }
                last_idx = idx;
            }
            last_idx
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let submission_wall = t_first.elapsed();
    let submission_returned_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    eprintln!(
        "==> [{}] submission complete: {} submitted ({} errors) in {:.2}s = {:.0} evps",
        dist.name(),
        count - submit_errors.load(Ordering::Relaxed),
        submit_errors.load(Ordering::Relaxed),
        submission_wall.as_secs_f64(),
        (count - submit_errors.load(Ordering::Relaxed)) as f64 / submission_wall.as_secs_f64(),
    );

    // Bulk poll terminal state until count reached or drain timeout fires.
    let drain_timeout = env_drain_timeout();
    let drain_start = Instant::now();
    let cids: Vec<Uuid> = envelopes.iter().map(|e| e.correlation_id).collect();
    let mut last_terminal: i64 = -1;
    let mut last_log = Instant::now();
    loop {
        let terminal: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::BIGINT FROM poc_v21_submission_status \
              WHERE correlation_id = ANY($1::uuid[]) \
                AND state IN ('committed', 'failed', 'replayed')",
        )
        .bind(&cids)
        .fetch_one(&*pool)
        .await
        .unwrap_or(0);
        if terminal != last_terminal && last_log.elapsed() > Duration::from_secs(2) {
            eprintln!(
                "    [{}] terminal={} / {} (drain {:.1}s elapsed)",
                dist.name(),
                terminal,
                count,
                drain_start.elapsed().as_secs_f64(),
            );
            last_terminal = terminal;
            last_log = Instant::now();
        }
        if terminal as usize >= count {
            break;
        }
        if drain_start.elapsed() > drain_timeout {
            eprintln!(
                "    [{}] drain timeout {:.1}s reached; terminal={}/{} — proceeding to measurement",
                dist.name(),
                drain_timeout.as_secs_f64(),
                terminal,
                count,
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let drain_wall = drain_start.elapsed();
    let end_to_end_wall = t_first.elapsed();

    let sampler_report = sampler.shutdown().await;
    let top_wait_event = sampler_report.top_wait_event();

    let counters_after = snap(&pool).await;
    let counters = delta(&counters_after, &counters_before);

    // Pull per-envelope (correlation_id, state, processed_at_ns) via a
    // single bulk SELECT. processed_at is committer-side timestamp;
    // latency = processed_at - enqueue_returned_at.
    eprintln!("    [{}] bulk-polling terminal state for {} envelopes...", dist.name(), count);
    // EXTRACT(EPOCH ...) returns DOUBLE PRECISION; cast to f64 cleanly.
    let rows: Vec<(Uuid, String, Option<f64>)> = match sqlx::query_as(
        "SELECT correlation_id, state::text, \
                (EXTRACT(EPOCH FROM processed_at) * 1e9)::DOUBLE PRECISION \
                  AS processed_at_ns \
           FROM poc_v21_submission_status \
          WHERE correlation_id = ANY($1::uuid[])",
    )
    .bind(&cids)
    .fetch_all(&*pool)
    .await
    {
        Ok(v) => v,
        Err(e) => {
            eprintln!("    [{}] bulk-poll error: {e}", dist.name());
            Vec::new()
        }
    };
    eprintln!("    [{}] bulk-poll returned {} rows", dist.name(), rows.len());

    // Build per-correlation_id lookup.
    let mut by_cid: std::collections::HashMap<Uuid, (String, Option<u64>)> =
        std::collections::HashMap::with_capacity(count);
    for (cid, state, processed_ns) in rows {
        by_cid.insert(cid, (state, processed_ns.map(|n| n as u64)));
    }
    eprintln!("    [{}] by_cid populated with {} entries", dist.name(), by_cid.len());

    let mut latency_hist =
        Histogram::<u64>::new_with_bounds(1, 600_000_000, 3).expect("hist bounds");
    let mut committed = 0usize;
    let mut failed = 0usize;
    let mut missing = 0usize;
    for (idx, e) in envelopes.iter().enumerate() {
        let enq_ns = *enqueue_returned_ns[idx].lock().expect("lock");
        if enq_ns == 0 {
            // Submission failed for this envelope (rare; counted in submit_errors).
            missing += 1;
            continue;
        }
        match by_cid.get(&e.correlation_id) {
            Some((state, Some(proc_ns))) => {
                if state == "committed" || state == "replayed" {
                    let dt = proc_ns.saturating_sub(enq_ns);
                    let dt_us = (dt / 1000).max(1).min(600_000_000);
                    let _ = latency_hist.record(dt_us);
                    committed += 1;
                } else {
                    failed += 1;
                }
            }
            Some((_, None)) => {
                // Reached terminal state but committer didn't write
                // processed_at — treat as failed/edge case.
                failed += 1;
            }
            None => {
                missing += 1;
            }
        }
    }

    // submission window: first vs last successful enqueue_returned_ns.
    // We didn't capture the FIRST returned ns explicitly; t_first is the
    // wall-clock instant before spawning. submission_wall covers from
    // spawn through last enqueue returned. That's close enough.
    let _ = submission_returned_ns;

    RunResult {
        distribution: dist.name().to_string(),
        payload_sha256,
        n_envelopes: count,
        n_backends,
        submission_wall_ns: submission_wall.as_nanos(),
        drain_wall_ns: drain_wall.as_nanos(),
        end_to_end_wall_ns: end_to_end_wall.as_nanos(),
        submitted: count - submit_errors.load(Ordering::Relaxed),
        submit_errors: submit_errors.load(Ordering::Relaxed),
        committed,
        failed,
        missing,
        latency_hist,
        counters,
        top_wait_event,
    }
}

fn write_result(r: &RunResult) -> std::io::Result<PathBuf> {
    let out_dir = PathBuf::from("bench/results-m8-ceiling");
    std::fs::create_dir_all(&out_dir)?;
    // Suffix: N (backend count) + status-mode (caller_intx / committer_lazy).
    // POC_CYU6_TAG env can override the suffix entirely (e.g. for ad-hoc
    // GUC sweeps). Keeps prior runs' output files from being clobbered.
    let suffix = std::env::var("POC_CYU6_TAG").unwrap_or_else(|_| {
        let mode = std::env::var("POC_CYU6_STATUS_MODE").unwrap_or_else(|_| "caller_intx".to_string());
        format!("_N{}backends_{}", r.n_backends, mode)
    });
    let path = out_dir.join(format!(
        "po-{}-N{}{}.json",
        r.distribution, r.n_envelopes, suffix
    ));
    let submission_evps = if r.submission_wall_ns > 0 {
        (r.submitted as f64) * 1e9 / r.submission_wall_ns as f64
    } else {
        0.0
    };
    let drain_evps = if r.drain_wall_ns > 0 {
        (r.committed as f64) * 1e9 / r.drain_wall_ns as f64
    } else {
        0.0
    };
    let e2e_evps = if r.end_to_end_wall_ns > 0 {
        (r.committed as f64) * 1e9 / r.end_to_end_wall_ns as f64
    } else {
        0.0
    };
    let h = &r.latency_hist;
    let json = serde_json::json!({
        "issue": "acct-cyu6",
        "distribution": r.distribution,
        "payload_sha256": r.payload_sha256,
        "n_envelopes": r.n_envelopes,
        "n_backends": r.n_backends,
        "submission": {
            "wall_ms": r.submission_wall_ns / 1_000_000,
            "rate_evps": submission_evps,
            "submitted": r.submitted,
            "errors": r.submit_errors,
        },
        "drain": {
            "wall_ms": r.drain_wall_ns / 1_000_000,
            "rate_evps": drain_evps,
        },
        "end_to_end": {
            "wall_ms": r.end_to_end_wall_ns / 1_000_000,
            "rate_evps": e2e_evps,
        },
        "outcomes": {
            "committed": r.committed,
            "failed": r.failed,
            "missing": r.missing,
        },
        "latency_e2e_us": {
            "min": h.min(),
            "p50": h.value_at_quantile(0.50),
            "p90": h.value_at_quantile(0.90),
            "p99": h.value_at_quantile(0.99),
            "p999": h.value_at_quantile(0.999),
            "max": h.max(),
            "count": h.len(),
        },
        "router": {
            "superbatch_count": r.counters.superbatch_count,
            "total_envelopes": r.counters.total_envelopes,
            "avg_envelopes_per_sb": if r.counters.superbatch_count > 0 {
                r.counters.total_envelopes as f64 / r.counters.superbatch_count as f64
            } else { 0.0 },
            "max_envelope_count_lifetime": r.counters.max_envelope_count,
            "envelope_histogram_buckets": r.counters.histogram_buckets,
            "ticks_total": r.counters.ticks_total,
            "force_pack_count": r.counters.force_pack_count,
            "cross_sb_for_update_waits": r.counters.cross_sb_for_update_waits,
        },
        "committer": {
            "drains_total": r.counters.committer_drains_total,
            "pipeline_ns_total": r.counters.committer_pipeline_ns_total,
            "pipeline_count": r.counters.committer_pipeline_count,
            "avg_pipeline_ns_per_drain": if r.counters.committer_pipeline_count > 0 {
                r.counters.committer_pipeline_ns_total as f64 / r.counters.committer_pipeline_count as f64
            } else { 0.0 },
            "tx_failures": r.counters.committer_tx_failures,
            "takeover_count": r.counters.committer_takeover_count,
            "eject_count": r.counters.eject_count,
            "backpressure_count": r.counters.backpressure_count,
        },
        "top_wait_event": r.top_wait_event.as_ref().map(|(wet, we, c)| {
            serde_json::json!({"wait_event_type": wet, "wait_event": we, "sum_backends": c})
        }),
        "gucs_assumed": {
            "batch_size_max": 50,
            "router_window_size": 1000,
            "batch_window_us_DEAD_CODE": 500,
        },
    });
    std::fs::write(&path, serde_json::to_string_pretty(&json)?)?;
    eprintln!(
        "==> [{}] wrote {} — submit={:.0} evps  drain={:.0} evps  e2e={:.0} evps  p50={}µs p99={}µs",
        r.distribution,
        path.display(),
        submission_evps,
        drain_evps,
        e2e_evps,
        h.value_at_quantile(0.50),
        h.value_at_quantile(0.99),
    );
    Ok(path)
}

// ──────────────────────────────────────────────────────────────────────
// Test bodies
// ──────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn cyu6_generate_payloads() {
    let count = env_count();
    generate(Distribution::Single, count).expect("generate single");
    generate(Distribution::Uniform(1000), count).expect("generate uniform_1000");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore]
async fn cyu6_run_single() {
    let count = env_count();
    let n = env_n();
    if !payload_path(Distribution::Single, count).exists() {
        eprintln!("==> generating missing payload");
        generate(Distribution::Single, count).expect("generate single");
    }
    let pool = Arc::new(build_pool((n as u32) + 8).await);
    let r = run_replay(pool, Distribution::Single, count, n).await;
    write_result(&r).expect("write result");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore]
async fn cyu6_run_uniform_1000() {
    let count = env_count();
    let n = env_n();
    if !payload_path(Distribution::Uniform(1000), count).exists() {
        eprintln!("==> generating missing payload");
        generate(Distribution::Uniform(1000), count).expect("generate uniform_1000");
    }
    let pool = Arc::new(build_pool((n as u32) + 8).await);
    let r = run_replay(pool, Distribution::Uniform(1000), count, n).await;
    write_result(&r).expect("write result");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore]
async fn cyu6_run_both() {
    let count = env_count();
    let n = env_n();
    if !payload_path(Distribution::Single, count).exists() {
        generate(Distribution::Single, count).expect("generate single");
    }
    if !payload_path(Distribution::Uniform(1000), count).exists() {
        generate(Distribution::Uniform(1000), count).expect("generate uniform_1000");
    }
    let pool = Arc::new(build_pool((n as u32) + 8).await);
    let r1 = run_replay(pool.clone(), Distribution::Single, count, n).await;
    write_result(&r1).expect("write single result");
    let r2 = run_replay(pool, Distribution::Uniform(1000), count, n).await;
    write_result(&r2).expect("write uniform_1000 result");
}

// ──────────────────────────────────────────────────────────────────────
// acct-pl3b: batch-ingress replay (poc_v21_enqueue_batch)
// ──────────────────────────────────────────────────────────────────────

fn env_batch_size() -> usize {
    std::env::var("POC_CYU6_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000)
}

async fn run_replay_batch(
    pool: Arc<PgPool>,
    dist: Distribution,
    count: usize,
    batch_size: usize,
    n_backends: usize,
) -> RunResult {
    use std::sync::atomic::AtomicUsize;
    let payload_path_buf = payload_path(dist, count);
    let meta_path_buf = meta_path(dist, count);
    let envelopes = read_payload(&payload_path_buf).expect("read payload");
    assert_eq!(envelopes.len(), count, "payload count mismatch");
    eprintln!(
        "==> [{} batch={}] read {} envelopes from {}",
        dist.name(),
        batch_size,
        envelopes.len(),
        payload_path_buf.display()
    );

    let payload_sha256 = std::fs::read_to_string(&meta_path_buf)
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .and_then(|v| v.get("sha256").and_then(|x| x.as_str()).map(|s| s.to_string()));

    reset_tables(&pool).await;
    pre_seed(&pool, dist).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let counters_before = snap(&pool).await;
    let sampler = PgLocksSampler::spawn((*pool).clone(), 200).await;

    let envelopes = Arc::new(envelopes);
    // Shared batch-cursor: each backend fetch_adds batch_size to claim a
    // contiguous slice of the payload.
    let cursor = Arc::new(AtomicUsize::new(0));
    let enqueue_returned_ns: Arc<Vec<std::sync::Mutex<u64>>> =
        Arc::new((0..count).map(|_| std::sync::Mutex::new(0)).collect());
    let submit_errors = Arc::new(AtomicUsize::new(0));

    let t_first = Instant::now();
    let mut handles = Vec::with_capacity(n_backends);
    for _backend_idx in 0..n_backends {
        let pool = pool.clone();
        let envelopes = envelopes.clone();
        let cursor = cursor.clone();
        let enqueue_returned_ns = enqueue_returned_ns.clone();
        let submit_errors = submit_errors.clone();
        handles.push(tokio::spawn(async move {
            loop {
                let start = cursor.fetch_add(batch_size, Ordering::Relaxed);
                if start >= envelopes.len() {
                    break;
                }
                let end = (start + batch_size).min(envelopes.len());
                let slice = &envelopes[start..end];

                // Build JSONB array for this batch.
                let arr: Vec<serde_json::Value> = slice
                    .iter()
                    .map(|e| {
                        serde_json::json!({
                            "correlation_id": e.correlation_id.to_string(),
                            "event_type": e.event_type,
                            "payload": e.payload,
                            "pool_keys": e.pool_keys,
                        })
                    })
                    .collect();
                let envelopes_json = serde_json::Value::Array(arr);

                let res =
                    sqlx::query("SELECT poc_v21_enqueue_batch($1::jsonb, false)")
                        .bind(&envelopes_json)
                        .execute(&*pool)
                        .await;
                let ns = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);
                if res.is_ok() {
                    for idx in start..end {
                        if let Ok(mut g) = enqueue_returned_ns[idx].lock() {
                            *g = ns;
                        }
                    }
                } else {
                    submit_errors.fetch_add(end - start, Ordering::Relaxed);
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let submission_wall = t_first.elapsed();
    eprintln!(
        "==> [{} batch={}] submission complete: {} submitted ({} errors) in {:.2}s = {:.0} evps",
        dist.name(),
        batch_size,
        count - submit_errors.load(Ordering::Relaxed),
        submit_errors.load(Ordering::Relaxed),
        submission_wall.as_secs_f64(),
        (count - submit_errors.load(Ordering::Relaxed)) as f64 / submission_wall.as_secs_f64(),
    );

    let drain_timeout = env_drain_timeout();
    let drain_start = Instant::now();
    let cids: Vec<Uuid> = envelopes.iter().map(|e| e.correlation_id).collect();
    let mut last_log = Instant::now();
    let mut last_terminal: i64 = -1;
    loop {
        let terminal: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::BIGINT FROM poc_v21_submission_status \
              WHERE correlation_id = ANY($1::uuid[]) \
                AND state IN ('committed', 'failed', 'replayed')",
        )
        .bind(&cids)
        .fetch_one(&*pool)
        .await
        .unwrap_or(0);
        if terminal != last_terminal && last_log.elapsed() > Duration::from_secs(2) {
            eprintln!(
                "    [{} batch={}] terminal={} / {} (drain {:.1}s elapsed)",
                dist.name(),
                batch_size,
                terminal,
                count,
                drain_start.elapsed().as_secs_f64(),
            );
            last_terminal = terminal;
            last_log = Instant::now();
        }
        if terminal as usize >= count {
            break;
        }
        if drain_start.elapsed() > drain_timeout {
            eprintln!(
                "    [{} batch={}] drain timeout {:.1}s; terminal={}/{}",
                dist.name(),
                batch_size,
                drain_timeout.as_secs_f64(),
                terminal,
                count,
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let drain_wall = drain_start.elapsed();
    let end_to_end_wall = t_first.elapsed();

    let sampler_report = sampler.shutdown().await;
    let top_wait_event = sampler_report.top_wait_event();

    let counters_after = snap(&pool).await;
    let counters = delta(&counters_after, &counters_before);

    let rows: Vec<(Uuid, String, Option<f64>)> = match sqlx::query_as(
        "SELECT correlation_id, state::text, \
                (EXTRACT(EPOCH FROM processed_at) * 1e9)::DOUBLE PRECISION \
                  AS processed_at_ns \
           FROM poc_v21_submission_status \
          WHERE correlation_id = ANY($1::uuid[])",
    )
    .bind(&cids)
    .fetch_all(&*pool)
    .await
    {
        Ok(v) => v,
        Err(e) => {
            eprintln!("    [{} batch={}] bulk-poll error: {e}", dist.name(), batch_size);
            Vec::new()
        }
    };
    let mut by_cid: std::collections::HashMap<Uuid, (String, Option<u64>)> =
        std::collections::HashMap::with_capacity(count);
    for (cid, state, processed_ns) in rows {
        by_cid.insert(cid, (state, processed_ns.map(|n| n as u64)));
    }

    let mut latency_hist =
        Histogram::<u64>::new_with_bounds(1, 600_000_000, 3).expect("hist bounds");
    let mut committed = 0usize;
    let mut failed = 0usize;
    let mut missing = 0usize;
    for (idx, e) in envelopes.iter().enumerate() {
        let enq_ns = *enqueue_returned_ns[idx].lock().expect("lock");
        if enq_ns == 0 {
            missing += 1;
            continue;
        }
        match by_cid.get(&e.correlation_id) {
            Some((state, Some(proc_ns))) => {
                if state == "committed" || state == "replayed" {
                    let dt = proc_ns.saturating_sub(enq_ns);
                    let dt_us = (dt / 1000).max(1).min(600_000_000);
                    let _ = latency_hist.record(dt_us);
                    committed += 1;
                } else {
                    failed += 1;
                }
            }
            _ => {
                missing += 1;
            }
        }
    }

    RunResult {
        distribution: format!("{}_batch{}", dist.name(), batch_size),
        payload_sha256,
        n_envelopes: count,
        n_backends,
        submission_wall_ns: submission_wall.as_nanos(),
        drain_wall_ns: drain_wall.as_nanos(),
        end_to_end_wall_ns: end_to_end_wall.as_nanos(),
        submitted: count - submit_errors.load(Ordering::Relaxed),
        submit_errors: submit_errors.load(Ordering::Relaxed),
        committed,
        failed,
        missing,
        latency_hist,
        counters,
        top_wait_event,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore]
async fn cyu6_batch_run_single() {
    let count = env_count();
    let n = env_n();
    let bs = env_batch_size();
    if !payload_path(Distribution::Single, count).exists() {
        generate(Distribution::Single, count).expect("generate single");
    }
    let pool = Arc::new(build_pool((n as u32) + 8).await);
    let r = run_replay_batch(pool, Distribution::Single, count, bs, n).await;
    write_result(&r).expect("write batch result");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore]
async fn cyu6_batch_run_uniform_1000() {
    let count = env_count();
    let n = env_n();
    let bs = env_batch_size();
    if !payload_path(Distribution::Uniform(1000), count).exists() {
        generate(Distribution::Uniform(1000), count).expect("generate uniform_1000");
    }
    let pool = Arc::new(build_pool((n as u32) + 8).await);
    let r = run_replay_batch(pool, Distribution::Uniform(1000), count, bs, n).await;
    write_result(&r).expect("write batch result");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore]
async fn cyu6_batch_run_both() {
    let count = env_count();
    let n = env_n();
    let bs = env_batch_size();
    if !payload_path(Distribution::Single, count).exists() {
        generate(Distribution::Single, count).expect("generate single");
    }
    if !payload_path(Distribution::Uniform(1000), count).exists() {
        generate(Distribution::Uniform(1000), count).expect("generate uniform_1000");
    }
    let pool = Arc::new(build_pool((n as u32) + 8).await);
    let r1 = run_replay_batch(pool.clone(), Distribution::Single, count, bs, n).await;
    write_result(&r1).expect("write batch single");
    let r2 = run_replay_batch(pool, Distribution::Uniform(1000), count, bs, n).await;
    write_result(&r2).expect("write batch uniform_1000");
}

// ──────────────────────────────────────────────────────────────────────
// acct-pl3b sweep: ingress_batch × router_sb_max × count × sku_spread × shape
// ──────────────────────────────────────────────────────────────────────

async fn set_router_sb_max(pool: &PgPool, v: i32) {
    let _ = sqlx::query(&format!("ALTER SYSTEM SET poc_v21.batch_size_max = {v}"))
        .execute(pool).await;
    let _ = sqlx::query("SELECT pg_reload_conf()").execute(pool).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore]
async fn pl3b_sweep() {
    // Cells: (count, spread, shape, ingress_batch, router_sb_max).
    // shape: "uniform" | "normal" | "single"
    let counts: Vec<usize> = std::env::var("POC_PL3B_COUNTS")
        .unwrap_or_else(|_| "100000,500000".to_string())
        .split(',').filter_map(|s| s.parse().ok()).collect();
    let spreads: Vec<usize> = std::env::var("POC_PL3B_SPREADS")
        .unwrap_or_else(|_| "100,500,1000".to_string())
        .split(',').filter_map(|s| s.parse().ok()).collect();
    let shapes: Vec<String> = std::env::var("POC_PL3B_SHAPES")
        .unwrap_or_else(|_| "uniform,normal".to_string())
        .split(',').map(|s| s.to_string()).collect();
    let ibatches: Vec<usize> = std::env::var("POC_PL3B_INGRESS")
        .unwrap_or_else(|_| "100,1000".to_string())
        .split(',').filter_map(|s| s.parse().ok()).collect();
    let rsbmaxes: Vec<i32> = std::env::var("POC_PL3B_RSBMAX")
        .unwrap_or_else(|_| "50,500,5000".to_string())
        .split(',').filter_map(|s| s.parse().ok()).collect();
    let n_backends = env_n();

    eprintln!("==> pl3b_sweep config:");
    eprintln!("    counts={:?} spreads={:?} shapes={:?} ingress={:?} router_sb={:?} N={}",
              counts, spreads, shapes, ibatches, rsbmaxes, n_backends);
    let total_cells = counts.len() * spreads.len() * shapes.len() * ibatches.len() * rsbmaxes.len();
    eprintln!("    {} total cells", total_cells);

    // Admin pool for ALTER SYSTEM (postgres DB).
    let admin_dsn = "postgres://acct:acct_dev@localhost:5111/postgres";
    let admin = PgPoolOptions::new().max_connections(2).connect(admin_dsn).await
        .expect("admin connect");
    // Test pool.
    let pool = Arc::new(build_pool((n_backends as u32) + 8).await);

    let mut summary: Vec<serde_json::Value> = Vec::new();
    let sweep_t0 = Instant::now();
    let mut idx = 0usize;
    for &count in &counts {
        for &spread in &spreads {
            for shape_s in &shapes {
                let dist: Distribution = match (shape_s.as_str(), spread) {
                    ("single", _) => Distribution::Single,
                    ("uniform", n) => Distribution::Uniform(n),
                    ("normal", n) => Distribution::Normal(n),
                    _ => continue,
                };
                if !payload_path(dist, count).exists() {
                    eprintln!("==> generating {} count={}", dist.name(), count);
                    generate(dist, count).expect("generate");
                }
                for &ibatch in &ibatches {
                    for &rsbmax in &rsbmaxes {
                        idx += 1;
                        eprintln!("\n===== [cell {}/{}] count={} spread={} shape={} ingress={} router_sb={} =====",
                                  idx, total_cells, count, spread, shape_s, ibatch, rsbmax);
                        set_router_sb_max(&admin, rsbmax).await;

                        let r = run_replay_batch(pool.clone(), dist, count, ibatch, n_backends).await;
                        let tag = format!("_pl3b_c{}_s{}_sh{}_ib{}_rsb{}",
                                          count, spread, shape_s, ibatch, rsbmax);
                        // Persist with custom tag.
                        let outdir = PathBuf::from("bench/results-m8-ceiling/pl3b-sweep");
                        std::fs::create_dir_all(&outdir).ok();
                        let path = outdir.join(format!("{}{}.json", r.distribution, tag));
                        let submit_evps = if r.submission_wall_ns > 0 {
                            r.submitted as f64 * 1e9 / r.submission_wall_ns as f64
                        } else { 0.0 };
                        let drain_evps = if r.drain_wall_ns > 0 {
                            r.committed as f64 * 1e9 / r.drain_wall_ns as f64
                        } else { 0.0 };
                        let e2e_evps = if r.end_to_end_wall_ns > 0 {
                            r.committed as f64 * 1e9 / r.end_to_end_wall_ns as f64
                        } else { 0.0 };
                        let cell_summary = serde_json::json!({
                            "cell_index": idx,
                            "count": count,
                            "spread": spread,
                            "shape": shape_s,
                            "ingress_batch": ibatch,
                            "router_sb_max": rsbmax,
                            "n_backends": n_backends,
                            "submit_evps": submit_evps,
                            "drain_evps": drain_evps,
                            "e2e_evps": e2e_evps,
                            "submission_wall_ms": r.submission_wall_ns / 1_000_000,
                            "drain_wall_ms": r.drain_wall_ns / 1_000_000,
                            "e2e_wall_ms": r.end_to_end_wall_ns / 1_000_000,
                            "committed": r.committed,
                            "failed": r.failed,
                            "missing": r.missing,
                            "submit_errors": r.submit_errors,
                            "avg_envelopes_per_sb": if r.counters.superbatch_count > 0 {
                                r.counters.total_envelopes as f64 / r.counters.superbatch_count as f64
                            } else { 0.0 },
                            "sb_count": r.counters.superbatch_count,
                            "max_envelope_count": r.counters.max_envelope_count,
                            "top_wait_event": r.top_wait_event.as_ref().map(|(wet,we,c)|
                                format!("{}/{}({})", wet, we, c)),
                        });
                        std::fs::write(&path, serde_json::to_string_pretty(&cell_summary).unwrap()).ok();
                        eprintln!("  → submit={:.0} drain={:.0} e2e={:.0} avgSB={:.1} ({})",
                            submit_evps, drain_evps, e2e_evps,
                            cell_summary["avg_envelopes_per_sb"].as_f64().unwrap_or(0.0),
                            path.display());
                        summary.push(cell_summary);

                        // Brief rest between cells.
                        tokio::time::sleep(Duration::from_secs(3)).await;
                    }
                }
            }
        }
    }
    let sweep_wall = sweep_t0.elapsed();
    let summary_path = PathBuf::from("bench/results-m8-ceiling/pl3b-sweep/_summary.json");
    let summary_obj = serde_json::json!({
        "sweep_wall_secs": sweep_wall.as_secs_f64(),
        "cells_run": summary.len(),
        "cells": summary,
    });
    std::fs::write(&summary_path, serde_json::to_string_pretty(&summary_obj).unwrap()).ok();
    eprintln!("\n==> sweep complete in {:.0}s — summary at {}",
              sweep_wall.as_secs_f64(), summary_path.display());
}
