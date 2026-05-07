//! T5 — Conformance fixture (input/expected-output triples).
//!
//! Per doc Appendix B item 4: a wide table-driven semantic regression
//! net for `post_posting_lines`. ~100 cases live in `tests/data/conformance.json`.
//! Each case names itself; on failure the harness reports which cases
//! diverged from their declared expectation.
//!
//! Two test functions:
//!
//!   - `conformance_cases` — runs every case in batched mode.
//!   - `batch_vs_split_equivalence` — runs the `also_split: true`
//!     subset twice (once batched, once split into single-event calls)
//!     and checks the documented end-state difference.

mod common;

use common::*;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;

// ---------- Case schema ----------

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    preconditions: Vec<Precondition>,
    events: Vec<EventInput>,
    #[serde(default)]
    override_closed_period: bool,
    expected: Expected,
    #[serde(default)]
    also_split: bool,
    #[serde(default)]
    expected_split: Option<ExpectedSplit>,
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum Precondition {
    CloseAccount {
        selector: Selector,
    },
    SeedStock {
        sku_code: String,
        location_code: String,
        qty: i64,
    },
    PostPostingLines {
        events: Vec<EventInput>,
        #[serde(default)]
        override_closed_period: bool,
    },
}

#[derive(Deserialize, Clone)]
struct Selector {
    kind: String,
    #[serde(default)]
    sku_code: Option<String>,
    #[serde(default)]
    location_code: Option<String>,
    #[serde(default)]
    currency: Option<String>,
    #[serde(default)]
    routing_op: Option<i32>,
}

#[derive(Deserialize, Clone)]
struct EventInput {
    reason: String,
    debit: Selector,
    credit: Selector,
    /// Optional after acct-0ig: cost-relevant value-side events use
    /// `qty` instead. Cases providing neither pass through and let
    /// the function raise (used for negative cases).
    #[serde(default)]
    amount: Option<i64>,
    /// Bifurcated input contract per acct-0ig: cost-relevant value-side
    /// events carry `qty` and the function computes amount internally.
    #[serde(default)]
    qty: Option<i64>,
    business_date: String,
    #[serde(default)]
    idempotency_tag: Option<String>,
    #[serde(default)]
    document_line_id: Option<String>,
    #[serde(default)]
    routing_op: Option<i32>,
    #[serde(default)]
    counterparty_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Expected {
    Ok {
        results: Vec<ResultExpect>,
        #[serde(default)]
        deltas: Vec<DeltaExpect>,
    },
    Error {
        sqlstate: String,
    },
}

#[derive(Deserialize)]
struct ResultExpect {
    index: i32,
    result: String, // "ok" or "exists"
}

#[derive(Deserialize, Clone)]
struct DeltaExpect {
    selector: Selector,
    #[serde(default)]
    debits: i64,
    #[serde(default)]
    credits: i64,
}

#[derive(Deserialize)]
struct ExpectedSplit {
    per_call: Vec<String>, // "ok", "exists", or "error:<SQLSTATE>"
    #[serde(default)]
    deltas: Vec<DeltaExpect>,
}

// ---------- Local UUID generator (xorshift; non-crypto) ----------

fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    if x == 0 {
        x = 0xdead_beef_cafe_babe;
    }
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

fn fresh_uuid_str(rng: &mut u64) -> String {
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        xorshift(rng) as u32,
        xorshift(rng) as u16,
        xorshift(rng) as u16,
        xorshift(rng) as u16,
        xorshift(rng) & 0xffff_ffff_ffff,
    )
}

// ---------- Resolution / building helpers ----------

async fn resolve_selector(pool: &sqlx::PgPool, sel: &Selector) -> i64 {
    account_id_for_selector(
        pool,
        &sel.kind,
        sel.sku_code.as_deref(),
        sel.location_code.as_deref(),
        sel.currency.as_deref(),
        sel.routing_op,
    )
    .await
}

/// Build a JSON event object from an `EventInput`. `idempotency_keys`
/// is a case-scoped map: events sharing an `idempotency_tag` get the
/// same UUID; tagless events get a fresh UUID.
async fn build_event_json(
    pool: &sqlx::PgPool,
    ev: &EventInput,
    idempotency_keys: &mut HashMap<String, String>,
    rng: &mut u64,
) -> Value {
    let debit_id = resolve_selector(pool, &ev.debit).await;
    let credit_id = resolve_selector(pool, &ev.credit).await;
    let key = match &ev.idempotency_tag {
        Some(tag) => idempotency_keys
            .entry(tag.clone())
            .or_insert_with(|| fresh_uuid_str(rng))
            .clone(),
        None => fresh_uuid_str(rng),
    };
    let mut obj = json!({
        "reason":            ev.reason,
        "document_kind":     "conformance_doc",
        "document_id":       fresh_uuid_str(rng),
        "debit_account_id":  debit_id,
        "credit_account_id": credit_id,
        "business_date":     ev.business_date,
        "idempotency_key":   key,
        "posted_by":         fresh_uuid_str(rng),
    });
    let m = obj.as_object_mut().unwrap();
    if let Some(a) = ev.amount {
        m.insert("amount".into(), json!(a));
    }
    if let Some(q) = ev.qty {
        m.insert("qty".into(), json!(q));
    }
    if let Some(v) = &ev.document_line_id {
        m.insert("document_line_id".into(), json!(v));
    }
    if let Some(op) = ev.routing_op {
        m.insert("routing_op".into(), json!(op));
    }
    if let Some(cp) = &ev.counterparty_id {
        m.insert("counterparty_id".into(), json!(cp));
    }
    obj
}

async fn apply_precondition(
    pool: &sqlx::PgPool,
    pc: &Precondition,
    idempotency_keys: &mut HashMap<String, String>,
    rng: &mut u64,
) {
    match pc {
        Precondition::CloseAccount { selector } => {
            let id = resolve_selector(pool, selector).await;
            sqlx::query(
                "UPDATE accounts SET is_closed = TRUE, closed_at = clock_timestamp() WHERE id = $1",
            )
            .bind(id)
            .execute(pool)
            .await
            .expect("close_account precondition");
        }
        Precondition::SeedStock {
            sku_code,
            location_code,
            qty,
        } => {
            seed_stock(pool, sku_code, location_code, *qty).await;
        }
        Precondition::PostPostingLines {
            events,
            override_closed_period,
        } => {
            let mut built = Vec::with_capacity(events.len());
            for ev in events {
                built.push(build_event_json(pool, ev, idempotency_keys, rng).await);
            }
            let result = call_post_posting_lines(pool, json!(built), *override_closed_period).await;
            result.expect("post_posting_lines precondition");
        }
    }
}

async fn ids_for_deltas(pool: &sqlx::PgPool, deltas: &[DeltaExpect]) -> Vec<(i64, i64, i64)> {
    let mut out = Vec::with_capacity(deltas.len());
    for d in deltas {
        out.push((resolve_selector(pool, &d.selector).await, d.debits, d.credits));
    }
    out
}

/// Compare actual vs expected results JSON. Returns a description of
/// any diff; empty string on match.
fn diff_results(actual: &Value, expected: &[ResultExpect]) -> Option<String> {
    let arr = actual.as_array().ok_or("results not an array").ok()?;
    if arr.len() != expected.len() {
        return Some(format!(
            "result count: expected {}, got {}",
            expected.len(),
            arr.len()
        ));
    }
    for (i, exp) in expected.iter().enumerate() {
        let got_idx = arr[i].get("index").and_then(|v| v.as_i64()).unwrap_or(-1);
        let got_res = arr[i].get("result").and_then(|v| v.as_str()).unwrap_or("");
        if got_idx as i32 != exp.index || got_res != exp.result {
            return Some(format!(
                "result[{i}]: expected (index={}, result={}), got (index={}, result={})",
                exp.index, exp.result, got_idx, got_res
            ));
        }
    }
    None
}

// ---------- Per-case runner ----------

async fn run_case(pool: &sqlx::PgPool, case: &Case) -> Result<(), String> {
    reset_to_fixture(pool).await;
    let mut idempotency_keys: HashMap<String, String> = HashMap::new();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let mut rng = nanos ^ 0x12345_6789a_bcdef ^ (case.name.len() as u64) << 32;

    for pc in &case.preconditions {
        apply_precondition(pool, pc, &mut idempotency_keys, &mut rng).await;
    }

    let before = snapshot_balances(pool).await;

    let mut event_jsons = Vec::with_capacity(case.events.len());
    for ev in &case.events {
        event_jsons.push(build_event_json(pool, ev, &mut idempotency_keys, &mut rng).await);
    }
    let batch_json = json!(event_jsons);
    let result = call_post_posting_lines(pool, batch_json, case.override_closed_period).await;

    match (&case.expected, &result) {
        (Expected::Ok { results, deltas }, Ok(actual)) => {
            if let Some(msg) = diff_results(actual, results) {
                return Err(msg);
            }
            let after = snapshot_balances(pool).await;
            let expected_deltas = ids_for_deltas(pool, deltas).await;
            check_deltas(&before, &after, &expected_deltas)?;
        }
        (Expected::Ok { .. }, Err(e)) => {
            return Err(format!("expected ok, got error: {e}"));
        }
        (Expected::Error { sqlstate }, Err(e)) => {
            let actual = e
                .as_database_error()
                .and_then(|d| d.code().map(|c| c.into_owned()))
                .unwrap_or_default();
            if actual != *sqlstate {
                return Err(format!(
                    "expected SQLSTATE {sqlstate}, got {actual}: {e}"
                ));
            }
            let after = snapshot_balances(pool).await;
            if before != after {
                let mut diffs = Vec::new();
                for (id, (db_before, cr_before)) in &before {
                    if let Some((db_after, cr_after)) = after.get(id) {
                        if db_after != db_before || cr_after != cr_before {
                            diffs.push(format!(
                                "  acct {id}: ({db_before},{cr_before}) -> ({db_after},{cr_after})"
                            ));
                        }
                    }
                }
                return Err(format!(
                    "error case must roll back fully, but balances changed:\n{}",
                    diffs.join("\n")
                ));
            }
        }
        (Expected::Error { sqlstate }, Ok(actual)) => {
            return Err(format!(
                "expected error SQLSTATE {sqlstate}, got ok: {actual}"
            ));
        }
    }
    Ok(())
}

fn check_deltas(
    before: &HashMap<i64, (i64, i64)>,
    after: &HashMap<i64, (i64, i64)>,
    expected: &[(i64, i64, i64)],
) -> Result<(), String> {
    let expected_map: HashMap<i64, (i64, i64)> =
        expected.iter().map(|(id, d, c)| (*id, (*d, *c))).collect();
    let mut errs = Vec::new();
    for (id, before_v) in before {
        let after_v = after.get(id).unwrap_or(before_v);
        let actual_delta = (after_v.0 - before_v.0, after_v.1 - before_v.1);
        let expected_delta = expected_map.get(id).copied().unwrap_or((0, 0));
        if actual_delta != expected_delta {
            errs.push(format!(
                "  acct {id}: expected delta=({},{}), got=({},{})",
                expected_delta.0, expected_delta.1, actual_delta.0, actual_delta.1
            ));
        }
    }
    if errs.is_empty() {
        Ok(())
    } else {
        Err(format!("balance deltas:\n{}", errs.join("\n")))
    }
}

fn load_cases() -> Vec<Case> {
    let raw = include_str!("data/conformance.json");
    serde_json::from_str(raw).expect("parse tests/data/conformance.json")
}

#[tokio::test]
async fn conformance_cases() {
    let pool = connect_test_db().await;
    let cases = load_cases();
    eprintln!("conformance: {} cases", cases.len());
    let mut failed: Vec<String> = Vec::new();
    for case in &cases {
        if let Err(msg) = run_case(&pool, case).await {
            failed.push(format!("[{}] {msg}", case.name));
        }
    }
    if !failed.is_empty() {
        panic!(
            "{} of {} cases failed:\n  {}",
            failed.len(),
            cases.len(),
            failed.join("\n  ")
        );
    }
}

// ---------- Batch vs split equivalence ----------

type SplitOutcome = (
    Vec<Result<Value, String>>,
    HashMap<i64, (i64, i64)>,
    HashMap<i64, (i64, i64)>,
);

async fn run_split(pool: &sqlx::PgPool, case: &Case) -> SplitOutcome {
    reset_to_fixture(pool).await;
    let mut idempotency_keys: HashMap<String, String> = HashMap::new();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let mut rng = nanos ^ 0xfeed_face_dead_beef ^ ((case.name.len() as u64) << 16);

    for pc in &case.preconditions {
        apply_precondition(pool, pc, &mut idempotency_keys, &mut rng).await;
    }
    let before = snapshot_balances(pool).await;

    let mut per_call = Vec::with_capacity(case.events.len());
    for ev in &case.events {
        let event_json = build_event_json(pool, ev, &mut idempotency_keys, &mut rng).await;
        let r = call_post_posting_lines(pool, json!([event_json]), case.override_closed_period).await;
        match r {
            Ok(v) => per_call.push(Ok(v)),
            Err(e) => {
                let code = e
                    .as_database_error()
                    .and_then(|d| d.code().map(|c| c.into_owned()))
                    .unwrap_or_default();
                per_call.push(Err(code));
            }
        }
    }
    let after = snapshot_balances(pool).await;
    (per_call, before, after)
}

fn check_split_per_call(
    actual: &[Result<Value, String>],
    expected: &[String],
) -> Result<(), String> {
    if actual.len() != expected.len() {
        return Err(format!(
            "split: expected {} calls, got {}",
            expected.len(),
            actual.len()
        ));
    }
    for (i, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
        match (a, e.as_str()) {
            (Ok(v), "ok") => {
                let r = v.get(0).and_then(|x| x.get("result")).and_then(|x| x.as_str());
                if r != Some("ok") {
                    return Err(format!("split call[{i}]: expected ok, got result={:?}", r));
                }
            }
            (Ok(v), "exists") => {
                let r = v.get(0).and_then(|x| x.get("result")).and_then(|x| x.as_str());
                if r != Some("exists") {
                    return Err(format!("split call[{i}]: expected exists, got result={:?}", r));
                }
            }
            (Err(actual_code), exp) if exp.starts_with("error:") => {
                let want = &exp[6..];
                if actual_code != want {
                    return Err(format!(
                        "split call[{i}]: expected SQLSTATE {want}, got {actual_code}"
                    ));
                }
            }
            (a, e) => {
                return Err(format!("split call[{i}]: expected {e}, got {a:?}"));
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn batch_vs_split_equivalence() {
    let pool = connect_test_db().await;
    let cases = load_cases();
    let split_cases: Vec<&Case> = cases.iter().filter(|c| c.also_split).collect();
    eprintln!(
        "batch_vs_split: {} cases tagged also_split",
        split_cases.len()
    );
    let mut failed: Vec<String> = Vec::new();
    for case in &split_cases {
        let (per_call, before, after) = run_split(&pool, case).await;

        let (expected_per_call, expected_deltas_src): (Vec<String>, Vec<DeltaExpect>) =
            match (&case.expected_split, &case.expected) {
                (Some(es), _) => (es.per_call.clone(), es.deltas.clone()),
                (None, Expected::Ok { results, deltas }) => (
                    results.iter().map(|r| r.result.clone()).collect(),
                    deltas.clone(),
                ),
                (None, Expected::Error { .. }) => {
                    failed.push(format!(
                        "[{}] also_split=true on an error case requires expected_split",
                        case.name
                    ));
                    continue;
                }
            };

        if let Err(msg) = check_split_per_call(&per_call, &expected_per_call) {
            failed.push(format!("[{}] {msg}", case.name));
            continue;
        }

        let expected_deltas = ids_for_deltas(&pool, &expected_deltas_src).await;
        if let Err(msg) = check_deltas(&before, &after, &expected_deltas) {
            failed.push(format!("[{}] split deltas: {msg}", case.name));
        }
    }
    if !failed.is_empty() {
        panic!(
            "{} of {} split cases failed:\n  {}",
            failed.len(),
            split_cases.len(),
            failed.join("\n  ")
        );
    }
}
