//! ledger-direct-c: pgrx 0.18 extension for ledger-v3.1 Path C (direct flavor).
//!
//! Authoritative spec: `poc/design_research/design-v3.1.md` §5.
//!
//! Exposes one SPI function — `ledger_submit_trx_c(trx_type, source_id, posted_at, lines)`
//! — which executes the full ledger work synchronously inside the caller's user-tx:
//! sorted `pool_lock` acquisition, bulk aggregate hydration, ledger-core dispatch
//! (`plan_apply_provisional` for FIFO/LIFO; strict for WAC/STD/specific), ordered bulk
//! writes. One PG transaction per submission, one fsync, caller-visible failures.
//!
//! Path C direct allocates no shared memory: every submission's work happens entirely
//! in the caller's backend, so `_PG_init` is a no-op beyond pgrx wiring. The routed
//! flavor (ledger-routed-c) is the one that needs shmem regions.
//!
//! Module map:
//! - `pool_lock`        — singleton-loop optimistic FOR UPDATE (§5.1 step 2)
//! - `hydration`        — snapshot read: pool routing + aggregate + standard_cost (§5.1 steps 3-5)
//! - `bulk_write`       — UNNEST INSERT/UPSERT/DELETE helpers (§5.1 step 8)
//! - `ledger_error_map` — LedgerError → ereport!(ERROR, ...) at the SPI boundary
//! - `submit`           — `ledger_submit_trx_c` orchestration (§5.1 steps 1-9)

#![allow(unexpected_cfgs)]

use pgrx::prelude::*;

::pgrx::pg_module_magic!();

pub(crate) mod bulk_write;
pub(crate) mod hydration;
pub(crate) mod ledger_error_map;
pub(crate) mod pool_lock;
pub(crate) mod submit;

// ── _PG_init ────────────────────────────────────────────────────────
//
// Path C direct is shmem-free, so this hook only exists to give PG
// something to invoke when the .so loads. No GUCs, no
// shared_preload_libraries — there is no shmem to allocate.
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {}

/// One-line smoke banner. Used by Phase 2 acceptance to confirm the
/// extension loads cleanly and the SPI surface is wired.
#[pg_extern]
fn ledger_direct_c_hello() -> String {
    format!(
        "ledger_direct_c {} — Path C: synchronous in-tx ledger_submit_trx_c (provisional cost)",
        env!("CARGO_PKG_VERSION"),
    )
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn hello_pg_extern_reachable() {
        let banner =
            Spi::get_one::<String>("SELECT ledger_direct_c_hello()").expect("SPI call failed");
        let banner = banner.expect("hello fn returned NULL");
        assert!(banner.starts_with("ledger_direct_c "), "unexpected banner: {banner:?}");
        assert!(banner.contains("Path C"), "banner missing path identifier: {banner:?}");
    }
}

/// Required by `cargo pgrx test`. Empty for Path C direct — no GUCs, no
/// shared_preload_libraries.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
