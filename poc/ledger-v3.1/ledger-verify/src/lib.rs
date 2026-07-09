//! Conservation-invariant SQL sweep for ledger-v3.1 (acct-0at4.5).
//!
//! A cheap set of reconciliations run *after* a test or a bench, as a
//! post-condition on whatever end state the workload left. It catches whole bug
//! classes that no single unit test targets — a torn `pool_state` write, a
//! dropped `posting_line`, a book value that drifted from its lines — by
//! reconciling the aggregate cache (`pool_state.layer_id = 0`) against the
//! append-only `trx_line` / `posting_line` stream that produced it.
//!
//! The checks (design-v3.1 §3.1 / §14.1 / §15; FEEDBACK-TESTING #2):
//!
//!   C1  qty conservation      pool_state.qty == Σ trx_line.qty per pool.
//!   C2a value accumulator     wac/fifo/lifo: value_sum == Σ(qty×unit_cost)
//!                             over the trx_lines since the pool last emptied
//!                             (the running-sum accumulator resets value_sum to
//!                             0 at qty 0, §3.1; standard-basis depletions let it
//!                             go legitimately NEGATIVE while qty>0, §15 — so the
//!                             reconciliation is `== net posted`, never `>= 0`).
//!   C2b value recompute       std/specific: value_sum == qty×unit_cost exactly
//!                             (these methods recompute book value each op).
//!   C2c value well-formed     every aggregate row ties together:
//!                             qty==0 ⟹ value_sum==0; qty>0 ⟹
//!                             unit_cost == banker_div(value_sum, qty).
//!   C3  no orphans            every trx has ≥1 trx_line; every (non-seed)
//!                             trx_line has ≥1 posting_line.
//!   C4  posting integrity     inventory-leg amount == |qty|×unit_cost; no
//!                             self-posting (debit==credit); amount ≥ 0; the
//!                             cross-account debit/credit totals close.
//!   C5  idempotency           no duplicate (trx_type, source_id).
//!
//! NOT checked — the "every completed submission has exactly one trx XOR a
//! recorded drop reason" invariant from FEEDBACK-TESTING #2 needs a caller-side
//! durable intent log (the ARCH roll-up #6 outbox, tracked under acct-0at4.3)
//! that does not exist yet: nothing records submission intent, so a submission
//! that never produced a trx is presently indistinguishable from one never made.
//! C5 covers the verifiable half (no *duplicate* trx). See the crate's tests and
//! acct-0at4.5 for the deferral.
//!
//! Deep-pool seeding (ledger-harness `seed::deepen`, §10.5) writes `trx_line`
//! receipts and `pool_state` layer rows but NO `posting_line` (it simulates
//! strict-mode layer state a future recalc would verify, not GL-posted state).
//! The sweep is deep-seed-robust: C1/C2 read `trx_line` (which the seed writes)
//! and C3 exempts a trx_line that is a seed layer receipt — identified
//! structurally as `∃ pool_state.layer_id = trx_line.id` (Path C never
//! materializes layer rows on the hot path, so only the seed creates them).

use sqlx::{PgConnection, PgPool, Row};

/// One reconciliation failure. `check` is a stable slug; `detail` names the pool
/// / row and the two sides that disagreed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    pub check: &'static str,
    pub detail: String,
}

/// Cap on rows reported per check, so a wholesale-broken DB yields a bounded,
/// readable report instead of tens of thousands of lines.
const MAX_PER_CHECK: usize = 20;

/// Run every conservation check against `pool` and return all violations (empty
/// = clean). Reads run inside a single READ ONLY REPEATABLE READ snapshot so a
/// live routed committer advancing between two of the sweep's queries cannot
/// manufacture a phantom cross-query mismatch.
pub async fn run_conservation_sweep(pool: &PgPool) -> Result<Vec<Violation>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;

    let mut out = Vec::new();
    check_qty_conservation(&mut tx, &mut out).await?;
    check_value_accumulator(&mut tx, &mut out).await?;
    check_value_recompute(&mut tx, &mut out).await?;
    check_value_wellformed(&mut tx, &mut out).await?;
    check_orphans(&mut tx, &mut out).await?;
    check_posting_integrity(&mut tx, &mut out).await?;
    check_idempotency(&mut tx, &mut out).await?;

    // Read-only; nothing to persist.
    tx.rollback().await?;
    Ok(out)
}

/// Convenience wrapper for test call sites: `Ok(())` when clean, otherwise a
/// single formatted error listing every violation. Composes with `.expect(...)`.
pub async fn assert_conservation_holds(pool: &PgPool) -> Result<(), String> {
    match run_conservation_sweep(pool).await {
        Ok(v) if v.is_empty() => Ok(()),
        Ok(v) => Err(format_violations(&v)),
        Err(e) => Err(format!("conservation sweep query error: {e}")),
    }
}

/// Human-readable multi-line rendering of a violation list.
pub fn format_violations(v: &[Violation]) -> String {
    let mut s = format!("{} conservation violation(s):", v.len());
    for x in v {
        s.push_str(&format!("\n  [{}] {}", x.check, x.detail));
    }
    s
}

// ── C1: qty conservation ────────────────────────────────────────────
//
// The aggregate on-hand (layer_id = 0) must equal the signed sum of every
// trx_line for the pool — receipts (+qty) and depletions (−qty). A FULL OUTER
// JOIN catches drift in either direction, including an aggregate row with no
// backing lines or lines with no aggregate. Deep-seed writes matching trx_line
// receipts, so this holds on seeded pools too.
async fn check_qty_conservation(
    conn: &mut PgConnection,
    out: &mut Vec<Violation>,
) -> Result<(), sqlx::Error> {
    let rows = sqlx::query(
        // SUM(bigint) is NUMERIC; the signed line sum equals the bigint on-hand,
        // so ::bigint is safe and lets the row decode as i64.
        "WITH line_sum AS (SELECT pool_id, SUM(qty)::bigint AS qsum FROM trx_line GROUP BY pool_id),
              agg AS (SELECT pool_id, qty FROM pool_state WHERE layer_id = 0)
         SELECT COALESCE(a.pool_id, l.pool_id) AS pool_id,
                COALESCE(a.qty, 0)  AS agg_qty,
                COALESCE(l.qsum, 0) AS line_qty
         FROM agg a FULL OUTER JOIN line_sum l ON a.pool_id = l.pool_id
         WHERE COALESCE(a.qty, 0) <> COALESCE(l.qsum, 0)
         ORDER BY 1 LIMIT $1",
    )
    .bind((MAX_PER_CHECK + 1) as i64)
    .fetch_all(&mut *conn)
    .await?;
    for r in rows.iter().take(MAX_PER_CHECK) {
        let pool_id: i64 = r.get("pool_id");
        let agg: i64 = r.get("agg_qty");
        let line: i64 = r.get("line_qty");
        out.push(Violation {
            check: "C1_qty_conservation",
            detail: format!("pool {pool_id}: aggregate qty {agg} != Σ trx_line.qty {line}"),
        });
    }
    Ok(())
}

// ── C2a: value_sum accumulator (wac / fifo / lifo) ──────────────────
//
// For the running-average methods, value_sum accumulates posted book value and
// is reset to 0 when the pool empties (wac.rs). So it equals Σ(qty×unit_cost)
// over the trx_lines *since the pool last reached qty 0* — or over all lines if
// it never emptied. The window finds the last line at which cumulative qty hit
// 0; value_sum must equal the qty×unit_cost sum of every later line. The sum is
// NUMERIC to avoid bigint overflow on deep pools and is compared as text so no
// arbitrary-precision Rust type is needed. value_sum may be NEGATIVE here
// (standard-basis over-book, §15) — the reconciliation matches that exactly.
async fn check_value_accumulator(
    conn: &mut PgConnection,
    out: &mut Vec<Violation>,
) -> Result<(), sqlx::Error> {
    let rows = sqlx::query(
        "WITH ordered AS (
             SELECT tl.pool_id, tl.id, tl.qty, tl.unit_cost,
                    SUM(tl.qty) OVER (PARTITION BY tl.pool_id ORDER BY tl.id
                                      ROWS UNBOUNDED PRECEDING) AS cum_qty
             FROM trx_line tl
             JOIN pool p ON p.id = tl.pool_id
             WHERE p.method IN ('wac','fifo','lifo')
         ),
         last_empty AS (
             SELECT pool_id, MAX(id) AS empty_id FROM ordered WHERE cum_qty = 0 GROUP BY pool_id
         ),
         tail AS (
             SELECT o.pool_id, SUM(o.qty::numeric * o.unit_cost::numeric) AS tail_vs
             FROM ordered o LEFT JOIN last_empty e ON e.pool_id = o.pool_id
             WHERE o.id > COALESCE(e.empty_id, 0)
             GROUP BY o.pool_id
         ),
         agg AS (
             SELECT ps.pool_id, ps.value_sum
             FROM pool_state ps JOIN pool p ON p.id = ps.pool_id
             WHERE ps.layer_id = 0 AND p.method IN ('wac','fifo','lifo')
         )
         SELECT COALESCE(a.pool_id, t.pool_id) AS pool_id,
                COALESCE(a.value_sum, 0)        AS value_sum,
                COALESCE(t.tail_vs, 0)::text    AS expected
         FROM agg a FULL OUTER JOIN tail t ON a.pool_id = t.pool_id
         WHERE COALESCE(a.value_sum, 0)::numeric <> COALESCE(t.tail_vs, 0)
         ORDER BY 1 LIMIT $1",
    )
    .bind((MAX_PER_CHECK + 1) as i64)
    .fetch_all(&mut *conn)
    .await?;
    for r in rows.iter().take(MAX_PER_CHECK) {
        let pool_id: i64 = r.get("pool_id");
        let value_sum: i64 = r.get("value_sum");
        let expected: String = r.get("expected");
        out.push(Violation {
            check: "C2a_value_accumulator",
            detail: format!(
                "pool {pool_id}: value_sum {value_sum} != Σ(qty×unit_cost) since last-empty {expected}"
            ),
        });
    }
    Ok(())
}

// ── C2b: value_sum recompute (std / specific) ───────────────────────
//
// STD and specific recompute book value as qty×unit_cost each op (standard.rs /
// specific.rs), so the aggregate must satisfy value_sum == qty×unit_cost
// exactly. NUMERIC guards against overflow in the product.
async fn check_value_recompute(
    conn: &mut PgConnection,
    out: &mut Vec<Violation>,
) -> Result<(), sqlx::Error> {
    let rows = sqlx::query(
        "SELECT ps.pool_id, ps.qty, ps.unit_cost, ps.value_sum
         FROM pool_state ps JOIN pool p ON p.id = ps.pool_id
         WHERE ps.layer_id = 0 AND p.method IN ('std','specific')
           AND ps.value_sum::numeric <> ps.qty::numeric * ps.unit_cost::numeric
         ORDER BY 1 LIMIT $1",
    )
    .bind((MAX_PER_CHECK + 1) as i64)
    .fetch_all(&mut *conn)
    .await?;
    for r in rows.iter().take(MAX_PER_CHECK) {
        let pool_id: i64 = r.get("pool_id");
        let qty: i64 = r.get("qty");
        let uc: i64 = r.get("unit_cost");
        let vs: i64 = r.get("value_sum");
        out.push(Violation {
            check: "C2b_value_recompute",
            detail: format!("pool {pool_id}: value_sum {vs} != qty {qty} × unit_cost {uc}"),
        });
    }
    Ok(())
}

// ── C2c: aggregate value_sum ↔ unit_cost well-formedness ─────────────
//
// Every aggregate row must tie its three cost columns together, for all methods:
// an empty pool carries value_sum 0 (the §3.1 reset), and a stocked pool's
// stored unit_cost is exactly the banker's-rounded average of its value_sum.
// Checked in Rust against ledger-core's own `banker_div` (the single division
// site) so there is zero risk of a re-implemented SQL rounding disagreeing.
async fn check_value_wellformed(
    conn: &mut PgConnection,
    out: &mut Vec<Violation>,
) -> Result<(), sqlx::Error> {
    let rows: Vec<(i64, i64, i64, i64)> =
        sqlx::query_as("SELECT pool_id, qty, unit_cost, value_sum FROM pool_state WHERE layer_id = 0 ORDER BY pool_id")
            .fetch_all(&mut *conn)
            .await?;
    for (pool_id, qty, unit_cost, value_sum) in rows {
        if out.iter().filter(|v| v.check == "C2c_value_wellformed").count() >= MAX_PER_CHECK {
            break;
        }
        if qty < 0 {
            out.push(Violation {
                check: "C2c_value_wellformed",
                detail: format!("pool {pool_id}: negative aggregate qty {qty} (§3.6 no-negative-inventory)"),
            });
        } else if qty == 0 {
            if value_sum != 0 {
                out.push(Violation {
                    check: "C2c_value_wellformed",
                    detail: format!("pool {pool_id}: qty 0 but value_sum {value_sum} (expected 0 after empty-reset §3.1)"),
                });
            }
        } else {
            let expected = ledger_core::banker_div(value_sum as i128, qty);
            if unit_cost != expected {
                out.push(Violation {
                    check: "C2c_value_wellformed",
                    detail: format!(
                        "pool {pool_id}: unit_cost {unit_cost} != banker_div(value_sum {value_sum}, qty {qty}) = {expected}"
                    ),
                });
            }
        }
    }
    Ok(())
}

// ── C3: no orphan trx / trx_line ────────────────────────────────────
//
// (a) every trx has at least one trx_line, and (b) every trx_line has at least
// one posting_line — except a deep-seed layer receipt, exempt structurally by
// `∃ pool_state.layer_id = tl.id` (only the seed creates layer rows; Path C's
// hot path never does), since the seed writes no posting_line by design.
async fn check_orphans(
    conn: &mut PgConnection,
    out: &mut Vec<Violation>,
) -> Result<(), sqlx::Error> {
    let trx_no_line = sqlx::query(
        "SELECT t.id FROM trx t
         WHERE NOT EXISTS (SELECT 1 FROM trx_line tl WHERE tl.trx_id = t.id)
         ORDER BY 1 LIMIT $1",
    )
    .bind((MAX_PER_CHECK + 1) as i64)
    .fetch_all(&mut *conn)
    .await?;
    for r in trx_no_line.iter().take(MAX_PER_CHECK) {
        let id: i64 = r.get("id");
        out.push(Violation { check: "C3_orphan_trx", detail: format!("trx {id} has no trx_line") });
    }

    let line_no_posting = sqlx::query(
        "SELECT tl.id FROM trx_line tl
         WHERE NOT EXISTS (SELECT 1 FROM posting_line pl WHERE pl.trx_line_id = tl.id)
           AND NOT EXISTS (SELECT 1 FROM pool_state ps WHERE ps.layer_id = tl.id)
         ORDER BY 1 LIMIT $1",
    )
    .bind((MAX_PER_CHECK + 1) as i64)
    .fetch_all(&mut *conn)
    .await?;
    for r in line_no_posting.iter().take(MAX_PER_CHECK) {
        let id: i64 = r.get("id");
        out.push(Violation {
            check: "C3_orphan_trx_line",
            detail: format!("trx_line {id} has no posting_line (and is not a deep-seed layer receipt)"),
        });
    }
    Ok(())
}

// ── C4: posting-line integrity + double-entry ───────────────────────
//
// The GL-side guards, all cheap:
//   - inventory-leg amount == |qty|×unit_cost (variance legs excluded — their
//     amount is |qty×(actual−std)|, standard.rs);
//   - no self-posting (debit_account == credit_account nets an account's
//     movement to zero while a naive Σdebit==Σcredit still balances) and no
//     negative amount;
//   - the cross-account debit/credit totals close (Σ all debits == Σ all
//     credits). In the single-amount / two-account posting model this is
//     structural, but it is asserted as a guard against a future multi-leg
//     schema regressing the double-entry property.
async fn check_posting_integrity(
    conn: &mut PgConnection,
    out: &mut Vec<Violation>,
) -> Result<(), sqlx::Error> {
    let bad_amount = sqlx::query(
        "SELECT pl.id FROM posting_line pl JOIN trx_line tl ON tl.id = pl.trx_line_id
         WHERE pl.event_type IN ('inventory_receipt','inventory_depletion')
           AND pl.amount::numeric <> ABS(tl.qty::numeric) * tl.unit_cost::numeric
         ORDER BY 1 LIMIT $1",
    )
    .bind((MAX_PER_CHECK + 1) as i64)
    .fetch_all(&mut *conn)
    .await?;
    for r in bad_amount.iter().take(MAX_PER_CHECK) {
        let id: i64 = r.get("id");
        out.push(Violation {
            check: "C4_posting_amount",
            detail: format!("posting_line {id}: inventory-leg amount != |qty|×unit_cost"),
        });
    }

    let degenerate = sqlx::query(
        "SELECT id, amount, debit_account, credit_account FROM posting_line
         WHERE debit_account = credit_account OR amount < 0
         ORDER BY 1 LIMIT $1",
    )
    .bind((MAX_PER_CHECK + 1) as i64)
    .fetch_all(&mut *conn)
    .await?;
    for r in degenerate.iter().take(MAX_PER_CHECK) {
        let id: i64 = r.get("id");
        let amount: i64 = r.get("amount");
        let d: i64 = r.get("debit_account");
        let c: i64 = r.get("credit_account");
        let why = if d == c { format!("self-posting to account {d}") } else { format!("negative amount {amount}") };
        out.push(Violation { check: "C4_posting_degenerate", detail: format!("posting_line {id}: {why}") });
    }

    // Trial-balance closure across all accounts (structural guard).
    let imbalance: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(signed), 0)::bigint FROM (
             SELECT amount AS signed FROM posting_line
             UNION ALL
             SELECT -amount FROM posting_line
         ) x",
    )
    .fetch_one(&mut *conn)
    .await?;
    if imbalance != 0 {
        out.push(Violation {
            check: "C4_trial_balance",
            detail: format!("Σ debits − Σ credits = {imbalance} (double-entry does not close)"),
        });
    }
    Ok(())
}

// ── C5: idempotency ─────────────────────────────────────────────────
//
// (trx_type, source_id) is the ledger idempotency key (schema 0003 UNIQUE). No
// pair may recur. This verifies the constraint held; the "trx exists iff the
// submission completed" half is deferred — no caller-side intent log exists yet
// (crate-level docs; acct-0at4.5 / acct-0at4.3 #6).
async fn check_idempotency(
    conn: &mut PgConnection,
    out: &mut Vec<Violation>,
) -> Result<(), sqlx::Error> {
    let dups = sqlx::query(
        "SELECT trx_type::text AS tt, source_id, count(*) AS n
         FROM trx GROUP BY trx_type, source_id HAVING count(*) > 1
         ORDER BY 2 LIMIT $1",
    )
    .bind((MAX_PER_CHECK + 1) as i64)
    .fetch_all(&mut *conn)
    .await?;
    for r in dups.iter().take(MAX_PER_CHECK) {
        let tt: String = r.get("tt");
        let source_id: i64 = r.get("source_id");
        let n: i64 = r.get("n");
        out.push(Violation {
            check: "C5_idempotency_dup",
            detail: format!("({tt}, source_id {source_id}) has {n} trx rows (idempotency key not unique)"),
        });
    }
    Ok(())
}
