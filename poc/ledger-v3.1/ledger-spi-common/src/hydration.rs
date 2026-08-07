//! Snapshot hydration (direct §5.1 steps 3-5 / routed §6.4 step 8).
//!
//! Reads, for the touched pool set, everything ledger-core needs:
//!   1. pool routing — `method`, `provisional_basis`, `(sku_id, location_id)`.
//!   2. aggregate row (`layer_id = 0`) for every pool — one row per pool. This
//!      is the only pool_state read FIFO/LIFO/WAC/STD make, so lock-hold time is
//!      independent of how many historical receipts the pool has seen — the
//!      constant-vs-depth property the PoC exists to validate (§5.2).
//!   3. materialized layer rows (`layer_id > 0`) ONLY for specific pools (K=1
//!      each). Skipped entirely when no specific pool is touched.
//!   4. standard_cost — joined on `(sku_id, location_id)`, only when the touched
//!      set contains a pool that needs it (STD, or a 'standard'-basis FIFO/LIFO
//!      pool). A pool in that subset with no matching standard_cost row simply
//!      has no entry in `standard_cost_of`; ledger-core raises
//!      `MissingStandardCost` when (and only when) it tries to use it.
//!
//! Caller must already hold `pool_lock` FOR UPDATE on every pool_id — otherwise
//! reads can race a concurrent committer and produce a stale snapshot.

use ledger_core::{PoolMethod, PoolStateRow, PostingAccounts, ProvisionalBasis, Snapshot};
use pgrx::prelude::*;

/// Hydrate a [`Snapshot`] for the given pool_ids.
///
/// A pool_id present in `lines` but absent from `pool` lands with no entry in
/// `method_of`; ledger-core then surfaces it as `LedgerError::UnknownPool`.
pub fn hydrate_snapshot(pool_ids: &[i64]) -> Result<Snapshot, pgrx::spi::Error> {
    if pool_ids.is_empty() {
        return Ok(Snapshot::default());
    }

    let mut ids: Vec<i64> = pool_ids.to_vec();
    ids.sort_unstable();
    ids.dedup();

    let mut snapshot = Snapshot::default();

    // ── 1. pool routing info ───────────────────────────────────────
    Spi::connect(|client| -> Result<(), pgrx::spi::Error> {
        let mut t = client.select(
            "SELECT id, method::text, provisional_basis::text, sku_id, location_id \
               FROM pool \
              WHERE id = ANY($1::bigint[])",
            None,
            &[ids.clone().into()],
        )?;
        while let Some(row) = t.next() {
            let pid: i64 = row.get::<i64>(1)?.unwrap_or(0);
            let m: String = row.get::<String>(2)?.unwrap_or_default();
            let basis: String = row.get::<String>(3)?.unwrap_or_default();
            let sku_id: i64 = row.get::<i64>(4)?.unwrap_or(0);
            let location_id: i64 = row.get::<i64>(5)?.unwrap_or(0);

            let method = match m.as_str() {
                "fifo" => PoolMethod::Fifo,
                "lifo" => PoolMethod::Lifo,
                "wac" => PoolMethod::Wac,
                "std" => PoolMethod::Std,
                "specific" => PoolMethod::Specific,
                _ => continue, // unknown enum value — surfaces as UnknownPool downstream
            };
            snapshot.method_of.insert(pid, method);

            let pb = match basis.as_str() {
                "standard" => ProvisionalBasis::Standard,
                _ => ProvisionalBasis::RunningAvg,
            };
            snapshot.provisional_basis_of.insert(pid, pb);
            snapshot.sku_location_of.insert(pid, (sku_id, location_id));
        }
        Ok(())
    })?;

    // ── 2. aggregate row (layer_id = 0) for every pool ─────────────
    Spi::connect(|client| -> Result<(), pgrx::spi::Error> {
        let mut t = client.select(
            "SELECT pool_id, qty, unit_cost, value_sum \
               FROM pool_state \
              WHERE pool_id = ANY($1::bigint[]) AND layer_id = 0",
            None,
            &[ids.clone().into()],
        )?;
        while let Some(row) = t.next() {
            let pid: i64 = row.get::<i64>(1)?.unwrap_or(0);
            let qty: i64 = row.get::<i64>(2)?.unwrap_or(0);
            let unit_cost: i64 = row.get::<i64>(3)?.unwrap_or(0);
            let value_sum: i64 = row.get::<i64>(4)?.unwrap_or(0);
            snapshot
                .pools
                .entry(pid)
                .or_default()
                .push(PoolStateRow { layer_id: 0, qty, unit_cost, value_sum });
        }
        Ok(())
    })?;

    // ── 3. specific layer rows (layer_id > 0), specific pools only ──
    let specific: Vec<i64> = ids
        .iter()
        .copied()
        .filter(|pid| snapshot.method_of.get(pid).copied() == Some(PoolMethod::Specific))
        .collect();

    if !specific.is_empty() {
        Spi::connect(|client| -> Result<(), pgrx::spi::Error> {
            let mut t = client.select(
                "SELECT pool_id, layer_id, qty, unit_cost, value_sum \
                   FROM pool_state \
                  WHERE pool_id = ANY($1::bigint[]) AND layer_id > 0 \
                  ORDER BY pool_id, layer_id",
                None,
                &[specific.into()],
            )?;
            while let Some(row) = t.next() {
                let pid: i64 = row.get::<i64>(1)?.unwrap_or(0);
                let layer_id: i64 = row.get::<i64>(2)?.unwrap_or(0);
                let qty: i64 = row.get::<i64>(3)?.unwrap_or(0);
                let unit_cost: i64 = row.get::<i64>(4)?.unwrap_or(0);
                let value_sum: i64 = row.get::<i64>(5)?.unwrap_or(0);
                snapshot
                    .pools
                    .entry(pid)
                    .or_default()
                    .push(PoolStateRow { layer_id, qty, unit_cost, value_sum });
            }
            Ok(())
        })?;
    }

    // ── 4. standard_cost (only when a touched pool needs it) ────────
    let needs_standard: Vec<i64> = ids
        .iter()
        .copied()
        .filter(|pid| {
            let method = snapshot.method_of.get(pid).copied();
            match method {
                Some(PoolMethod::Std) => true,
                Some(PoolMethod::Fifo) | Some(PoolMethod::Lifo) => {
                    snapshot.provisional_basis_of.get(pid).copied()
                        == Some(ProvisionalBasis::Standard)
                }
                _ => false,
            }
        })
        .collect();

    if !needs_standard.is_empty() {
        Spi::connect(|client| -> Result<(), pgrx::spi::Error> {
            let mut t = client.select(
                "SELECT p.id, sc.unit_cost \
                   FROM pool p \
                   JOIN standard_cost sc \
                     ON sc.sku_id = p.sku_id AND sc.location_id = p.location_id \
                  WHERE p.id = ANY($1::bigint[])",
                None,
                &[needs_standard.into()],
            )?;
            while let Some(row) = t.next() {
                let pid: i64 = row.get::<i64>(1)?.unwrap_or(0);
                let unit_cost: i64 = row.get::<i64>(2)?.unwrap_or(0);
                snapshot.standard_cost_of.insert(pid, unit_cost);
            }
            Ok(())
        })?;
    }

    // ── 5. posting accounts — every touched pool (§3.7) ────────────
    // Joined on the pool's (sku_id, location_id), like standard_cost. Needed for
    // every pool: each line posts a journal row. A pool with no posting_account_map
    // row simply gets no entry here; ledger-core raises MissingPostingAccounts when
    // it processes the line (fail-loud).
    Spi::connect(|client| -> Result<(), pgrx::spi::Error> {
        let mut t = client.select(
            "SELECT p.id, \
                    m.receipt_debit, m.receipt_credit, \
                    m.transfer_debit, m.transfer_credit, \
                    m.build_debit, m.build_credit, \
                    m.scrap_debit, m.scrap_credit, \
                    m.adjustment_debit, m.adjustment_credit, \
                    m.revaluation_debit, m.revaluation_credit, \
                    m.variance_acct \
               FROM pool p \
               JOIN posting_account_map m \
                 ON m.sku_id = p.sku_id AND m.location_id = p.location_id \
              WHERE p.id = ANY($1::bigint[])",
            None,
            &[ids.clone().into()],
        )?;
        while let Some(row) = t.next() {
            let pid: i64 = row.get::<i64>(1)?.unwrap_or(0);
            let g = |c: usize| -> i64 { row.get::<i64>(c).ok().flatten().unwrap_or(0) };
            snapshot.posting_accounts_of.insert(
                pid,
                PostingAccounts {
                    receipt: (g(2), g(3)),
                    transfer: (g(4), g(5)),
                    build: (g(6), g(7)),
                    scrap: (g(8), g(9)),
                    adjustment: (g(10), g(11)),
                    revaluation: (g(12), g(13)),
                    variance: row.get::<i64>(14)?,
                },
            );
        }
        Ok(())
    })?;

    Ok(snapshot)
}
