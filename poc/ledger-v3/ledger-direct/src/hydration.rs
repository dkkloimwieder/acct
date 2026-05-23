//! Snapshot hydration (design-v3 §4.2 step 4).
//!
//! Three SPI reads against the touched pool set:
//!   1. pool_state — bulk layer read, ORDER BY (pool_id, layer_seq) (free
//!      from the (pool_id, layer_seq) PK index)
//!   2. pool       — per-pool method
//!   3. trx_line   — per-pool MAX(trx_seq) (§13.1 option a)
//!
//! Caller must already hold `pool_lock` FOR UPDATE on every pool_id —
//! otherwise reads can race a concurrent committer and produce a stale
//! snapshot.

use std::collections::HashMap;

use ledger_core::{PoolMethod, PoolStateRow, Snapshot};
use pgrx::prelude::*;

/// Hydrate a [`Snapshot`] for the given pool_ids.
///
/// Pools that exist in `pool` but have no rows in `pool_state` (e.g.,
/// fully-depleted FIFO/LIFO pool, or any STD pool) are present in
/// `method_of` with no entry in `pools` — matches Snapshot's invariant
/// that an empty pool is omitted from the layers map.
///
/// Pools present in `pool_ids` but missing from `pool` raise
/// `pgrx::spi::Error::CursorNotFound`-style propagation via the caller's
/// `LedgerError::UnknownPool` translation: this fn does not raise on its
/// own, it just returns whatever the DB has.
#[allow(dead_code)] // wired by submit::ledger_submit_trx (acct-v4xz)
pub fn hydrate_snapshot(pool_ids: &[i64]) -> Result<Snapshot, pgrx::spi::Error> {
    if pool_ids.is_empty() {
        return Ok(Snapshot::default());
    }

    // Defensive sort + dedup — protects the §13.1 MAX-scan plan from
    // adversarial input and matches the deadlock-free locking order.
    let mut ids: Vec<i64> = pool_ids.to_vec();
    ids.sort_unstable();
    ids.dedup();

    // ── 1. pool_state ──────────────────────────────────────────────
    let mut pools: HashMap<i64, Vec<PoolStateRow>> = HashMap::new();
    Spi::connect(|client| -> Result<(), pgrx::spi::Error> {
        let mut t = client.select(
            "SELECT pool_id, layer_seq, qty, unit_cost, last_trx_line_id \
               FROM pool_state \
              WHERE pool_id = ANY($1::bigint[]) \
              ORDER BY pool_id, layer_seq",
            None,
            &[ids.clone().into()],
        )?;
        while let Some(row) = t.next() {
            let pid: i64 = row.get::<i64>(1)?.unwrap_or(0);
            let layer_seq: i64 = row.get::<i64>(2)?.unwrap_or(0);
            let qty: i64 = row.get::<i64>(3)?.unwrap_or(0);
            let unit_cost: i64 = row.get::<i64>(4)?.unwrap_or(0);
            let last_trx_line_id: i64 = row.get::<i64>(5)?.unwrap_or(0);
            pools.entry(pid).or_default().push(PoolStateRow {
                layer_seq,
                qty,
                unit_cost,
                last_trx_line_id,
            });
        }
        Ok(())
    })?;

    // Defensive per-pool re-sort by layer_seq. The SQL ORDER BY is the
    // contract; the client-side sort is the enforcement.
    for v in pools.values_mut() {
        v.sort_by_key(|r| r.layer_seq);
    }

    // ── 2. pool.method ──────────────────────────────────────────────
    let mut method_of: HashMap<i64, PoolMethod> = HashMap::new();
    Spi::connect(|client| -> Result<(), pgrx::spi::Error> {
        let mut t = client.select(
            "SELECT id, method::text FROM pool WHERE id = ANY($1::bigint[])",
            None,
            &[ids.clone().into()],
        )?;
        while let Some(row) = t.next() {
            let pid: i64 = row.get::<i64>(1)?.unwrap_or(0);
            let m: String = row.get::<String>(2)?.unwrap_or_default();
            let method = match m.as_str() {
                "fifo" => PoolMethod::Fifo,
                "lifo" => PoolMethod::Lifo,
                "wac" => PoolMethod::Wac,
                "wac_periodic" => PoolMethod::WacPeriodic,
                "std" => PoolMethod::Std,
                "specific" => PoolMethod::Specific,
                _ => continue, // unknown enum value — surfaces as UnknownPool downstream
            };
            method_of.insert(pid, method);
        }
        Ok(())
    })?;

    // ── 3. MAX(trx_seq) per pool ───────────────────────────────────
    // Pools with zero trx_line rows are omitted from the result; default
    // to 0 in the snapshot map.
    let mut max_trx_seq_of: HashMap<i64, i64> =
        ids.iter().map(|&pid| (pid, 0i64)).collect();
    Spi::connect(|client| -> Result<(), pgrx::spi::Error> {
        let mut t = client.select(
            "SELECT pool_id, COALESCE(MAX(trx_seq), 0) \
               FROM trx_line \
              WHERE pool_id = ANY($1::bigint[]) \
              GROUP BY pool_id",
            None,
            &[ids.into()],
        )?;
        while let Some(row) = t.next() {
            let pid: i64 = row.get::<i64>(1)?.unwrap_or(0);
            let max_seq: i64 = row.get::<i64>(2)?.unwrap_or(0);
            max_trx_seq_of.insert(pid, max_seq);
        }
        Ok(())
    })?;

    Ok(Snapshot {
        pools,
        method_of,
        max_trx_seq_of,
        std_cost_of: HashMap::new(),
    })
}
