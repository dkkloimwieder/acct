//! acct-sw4i: shared-memory ledger balance rollup with bgworker drain.
//!
//! Milestone 1: scaffolding only. Exposes `ledger_extension_version()` so
//! `CREATE EXTENSION ledger_extension` is testable end-to-end before any
//! shmem / CAS / bgworker code lands.
//!
//! Later milestones (per state-2026-05-12-acct-togd-bench-complete-ready-for-sw4i):
//!   2. Shmem hash allocation via RequestAddinShmemSpace + LWLock tranche
//!   3. ledger_apply_balance_delta(account_id, period_id, currency, ledger_kind, amount_delta, qty_delta)
//!   4. balance(account_id) SQL reader (shmem hit, durable rollup fallback)
//!   5. bgworker drain to account_balances_rollup
//!   6. Custom WAL RM + redo for crash recovery
//!   7. Recon hook: shmem vs SUM(posting_lines) at quiescence
//!   8. Integrate with PoC's post_batch apply path
//!   9. Bench vs bench_fan_in / bench_fan_out / bench_wac_fan

use pgrx::prelude::*;

pgrx::pg_module_magic!();

#[pg_extern]
fn ledger_extension_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn version_string_is_nonempty() {
        let v = crate::ledger_extension_version();
        assert!(!v.is_empty());
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
