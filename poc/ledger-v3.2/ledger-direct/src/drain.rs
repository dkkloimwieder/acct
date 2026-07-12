//! `ledger_staging_drain` — the staging-table committer (SPIKE-A shape,
//! design-v3.2 §3).
//!
//! Claims up to `limit` pending `ledger_inbox` rows with `FOR UPDATE SKIP
//! LOCKED ORDER BY id`, applies each through the SAME per-method dispatch as
//! `ledger_submit_trx` (`submit::apply_submission`), and marks each row's
//! outcome. The caller (a committer connection or worker) loops this call; K
//! such loops coalesce K batches into K commits — the fsync amortization that
//! is the staging path's structural win.
//!
//! Failure isolation: each submission applies inside a subtransaction
//! (`PgTryBuilder`). A failing submission — strict-method qty gate, duplicate
//! (trx_type, source_id), bad payload — rolls back only its own writes and
//! marks its row 'failed' with the error text; the rest of the batch proceeds.
//! The status updates commit atomically with the batch's ledger writes, so a
//! mid-drain crash commits nothing and the rows are re-claimed cleanly (WAL is
//! the recovery protocol).
//!
//! Cross-committer same-pool apply order is not guaranteed (per-committer FIFO
//! only). Benign under alt C: costing chronology is recalc's R-1 re-sort on
//! (pool_id, posted_at, id), and the qty aggregate delta is commutative. The
//! observed provisional cost a racing depletion records is order-sensitive —
//! and already provisional (design-v3.1 §16 re-derivation).
//!
//! Backpressure (recalc-c §5) gates ADMISSION only: the `ledger_inbox` BEFORE
//! INSERT trigger rejects enqueues to throttled pools; this drain applies
//! already-admitted envelopes unchecked — accepted work is never
//! retroactively failed.

use chrono::{DateTime, Utc};
use pgrx::pg_sys::panic::CaughtError;
use pgrx::prelude::*;

use crate::submit::{apply_submission, bail, LineJson};

/// Saved state for an internal subtransaction (the plpgsql exception-block
/// pattern): `PgTryBuilder` alone only catches the unwind and flushes the
/// error state — it does NOT undo the failed submission's writes. The
/// subtransaction is what makes the rollback real; the saved memory context
/// and resource owner are restored on both exit paths.
struct SubTx {
    memcx: pgrx::pg_sys::MemoryContext,
    owner: pgrx::pg_sys::ResourceOwner,
}

fn subtx_begin() -> SubTx {
    unsafe {
        let saved = SubTx {
            memcx: pgrx::pg_sys::CurrentMemoryContext,
            owner: pgrx::pg_sys::CurrentResourceOwner,
        };
        pgrx::pg_sys::BeginInternalSubTransaction(std::ptr::null());
        pgrx::pg_sys::MemoryContextSwitchTo(saved.memcx);
        saved
    }
}

fn subtx_release(saved: SubTx) {
    unsafe {
        pgrx::pg_sys::ReleaseCurrentSubTransaction();
        pgrx::pg_sys::MemoryContextSwitchTo(saved.memcx);
        pgrx::pg_sys::CurrentResourceOwner = saved.owner;
    }
}

fn subtx_rollback(saved: SubTx) {
    unsafe {
        pgrx::pg_sys::RollbackAndReleaseCurrentSubTransaction();
        pgrx::pg_sys::MemoryContextSwitchTo(saved.memcx);
        pgrx::pg_sys::CurrentResourceOwner = saved.owner;
    }
}

/// One claimed inbox row, decoded and ready to apply.
struct Claimed {
    id: i64,
    trx_type: String,
    source_id: i64,
    posted_at: DateTime<Utc>,
    lines: Vec<LineJson>,
}

/// Claim up to `p_limit` pending rows (SKIP LOCKED, id order), apply each in a
/// subtransaction, mark done/failed, and return the number of rows claimed
/// (the committer loop's progress signal; 0 = queue empty).
#[pg_extern]
fn ledger_staging_drain(p_limit: i32) -> i64 {
    let limit = i64::from(p_limit.max(1));

    // 1. Claim. posted_at travels as epoch microseconds (dependency-free
    //    round-trip; the apply re-renders RFC3339 for the CTE casts).
    let raw: Vec<(i64, String, i64, i64, serde_json::Value)> =
        Spi::connect_mut(|client| -> Result<_, pgrx::spi::Error> {
            let mut table = client.update(
                "SELECT id, trx_type::text, source_id, \
                        (extract(epoch FROM posted_at) * 1000000)::bigint, lines \
                   FROM ledger_inbox \
                  WHERE status = 'pending' \
                  ORDER BY id \
                    FOR UPDATE SKIP LOCKED \
                  LIMIT $1",
                None,
                &[limit.into()],
            )?;
            let mut out = Vec::new();
            while let Some(row) = table.next() {
                out.push((
                    row.get::<i64>(1)?.expect("ledger_inbox.id NOT NULL"),
                    row.get::<String>(2)?.expect("ledger_inbox.trx_type NOT NULL"),
                    row.get::<i64>(3)?.expect("ledger_inbox.source_id NOT NULL"),
                    row.get::<i64>(4)?.expect("ledger_inbox.posted_at NOT NULL"),
                    row.get::<pgrx::JsonB>(5)?.expect("ledger_inbox.lines NOT NULL").0,
                ));
            }
            Ok(out)
        })
        .unwrap_or_else(|e| {
            bail(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("ledger_staging_drain: claim failed: {e}"),
            )
        });

    if raw.is_empty() {
        return 0;
    }
    let claimed_count = raw.len() as i64;

    // 2. Decode. A row whose payload does not decode is a 'failed' outcome,
    //    not a batch abort.
    let mut done_ids: Vec<i64> = Vec::with_capacity(raw.len());
    let mut failed_ids: Vec<i64> = Vec::new();
    let mut failed_errors: Vec<String> = Vec::new();
    let mut ready: Vec<Claimed> = Vec::with_capacity(raw.len());

    for (id, trx_type, source_id, posted_at_us, lines_json) in raw {
        let posted_at = match DateTime::<Utc>::from_timestamp_micros(posted_at_us) {
            Some(t) => t,
            None => {
                failed_ids.push(id);
                failed_errors.push(format!("posted_at out of range: {posted_at_us}us"));
                continue;
            }
        };
        match serde_json::from_value::<Vec<LineJson>>(lines_json) {
            Ok(lines) => ready.push(Claimed { id, trx_type, source_id, posted_at, lines }),
            Err(e) => {
                failed_ids.push(id);
                failed_errors.push(format!("lines decode failed: {e}"));
            }
        }
    }

    // 3. Apply each submission in its own subtransaction: an error rolls back
    //    only that submission's writes and records the message. PgTryBuilder
    //    flushes the caught error state; the explicit subtransaction is what
    //    undoes the writes.
    for c in ready {
        let sub = subtx_begin();
        let outcome: Result<i64, String> = PgTryBuilder::new(|| {
            Ok(apply_submission(&c.trx_type, c.source_id, c.posted_at, &c.lines, false))
        })
        .catch_others(|cause| match &cause {
            CaughtError::PostgresError(report) | CaughtError::ErrorReport(report) => {
                Err(report.message().to_string())
            }
            CaughtError::RustPanic { ereport, .. } => Err(ereport.message().to_string()),
        })
        .execute();

        match outcome {
            Ok(_trx_id) => {
                subtx_release(sub);
                done_ids.push(c.id);
            }
            Err(msg) => {
                subtx_rollback(sub);
                failed_ids.push(c.id);
                failed_errors.push(msg);
            }
        }
    }

    // 4. Record outcomes (same tx as the ledger writes).
    Spi::connect_mut(|client| -> Result<(), pgrx::spi::Error> {
        if !done_ids.is_empty() {
            client.update(
                "UPDATE ledger_inbox \
                    SET status = 'done', drained_at = clock_timestamp() \
                  WHERE id = ANY($1::bigint[])",
                None,
                &[done_ids.into()],
            )?;
        }
        if !failed_ids.is_empty() {
            client.update(
                "UPDATE ledger_inbox \
                    SET status = 'failed', error = f.msg, drained_at = clock_timestamp() \
                   FROM UNNEST($1::bigint[], $2::text[]) AS f(fid, msg) \
                  WHERE ledger_inbox.id = f.fid",
                None,
                &[failed_ids.into(), failed_errors.into()],
            )?;
        }
        Ok(())
    })
    .unwrap_or_else(|e| {
        bail(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("ledger_staging_drain: outcome update failed: {e}"),
        )
    });

    claimed_count
}
