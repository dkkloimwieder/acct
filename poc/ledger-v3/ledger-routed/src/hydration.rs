//! Snapshot hydration (design-v3 §5.4 step 7).
//!
//! Port of `ledger-direct/src/hydration.rs` (locked plan §G Q3:
//! copy-paste). Three SPI reads against the touched pool set:
//!   1. pool_state — bulk layer read, ORDER BY (pool_id, layer_seq)
//!      (free from the (pool_id, layer_seq) PK index)
//!   2. pool       — per-pool method
//!   3. trx_line   — per-pool MAX(trx_seq) (§13.1 option a)
//!
//! Caller must already hold `pool_lock` FOR UPDATE on every pool_id —
//! otherwise reads can race a concurrent committer and produce a stale
//! snapshot.

use std::collections::HashMap;

use ledger_core::{PoolMethod, PoolStateRow, Snapshot};
use pgrx::prelude::*;

#[allow(dead_code)] // wired by committer::process_commit_group (acct-usn2)
pub fn hydrate_snapshot(pool_ids: &[i64]) -> Result<Snapshot, pgrx::spi::Error> {
    if pool_ids.is_empty() {
        return Ok(Snapshot::default());
    }

    let mut ids: Vec<i64> = pool_ids.to_vec();
    ids.sort_unstable();
    ids.dedup();

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

    for v in pools.values_mut() {
        v.sort_by_key(|r| r.layer_seq);
    }

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
                "std" => PoolMethod::Std,
                "specific" => PoolMethod::Specific,
                _ => continue,
            };
            method_of.insert(pid, method);
        }
        Ok(())
    })?;

    let mut max_trx_seq_of: HashMap<i64, i64> = ids.iter().map(|&pid| (pid, 0i64)).collect();
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
