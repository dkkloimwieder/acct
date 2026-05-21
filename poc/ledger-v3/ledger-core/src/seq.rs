//! Per-pool monotonic trx_seq counter helper.
//!
//! Shared by every method module. Increments `Snapshot.max_trx_seq_of[pool_id]`
//! (creating with 0 if absent) and returns the new value. Pools should have
//! been seeded by the caller's hydration step via
//! `SELECT pool_id, COALESCE(MAX(trx_seq), 0) FROM trx_line WHERE pool_id = ANY($1) GROUP BY pool_id`
//! (design-v3 §13.1 option a).

use crate::error::LedgerError;
use crate::snapshot::Snapshot;

pub(crate) fn next_trx_seq(snapshot: &mut Snapshot, pool_id: i64) -> Result<i64, LedgerError> {
    let entry = snapshot.max_trx_seq_of.entry(pool_id).or_insert(0);
    *entry = entry.checked_add(1).ok_or_else(|| LedgerError::Overflow {
        detail: format!("trx_seq overflow on pool {}", pool_id),
    })?;
    Ok(*entry)
}
