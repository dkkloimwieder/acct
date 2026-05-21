//! ledger-direct: pgrx 0.18 extension for ledger-v3 Path A.
//!
//! Authoritative spec: `poc/design_research/design-v3.md` §4.
//!
//! Exposes one SPI function — `ledger_submit_trx(trx_type, source_id, posted_at, lines)`
//! — which executes the 8-step pipeline (sorted pool_lock acquisition, bulk pool_state
//! snapshot read, ledger-core plan_apply, ordered bulk writes) inside the caller's
//! user-tx. One PG transaction per submission, one fsync, caller-visible failures.
//!
//! Path A allocates no shared memory: every submission's work happens entirely in
//! the caller's backend, so `_PG_init` is a no-op beyond pgrx wiring. Path B
//! (ledger-routed) is the one that needs shmem regions.
//!
//! Modules added by follow-up beads issues:
//! - `pool_lock`        — acct-bvps (singleton-loop FOR UPDATE per §4.2 step 3)
//! - `hydration`        — acct-1ucl (snapshot read per §4.2 step 4)
//! - `bulk_write`       — acct-iir7 (UNNEST INSERT/UPSERT/UPDATE/DELETE helpers)
//! - `submit`           — acct-v4xz (8-step orchestration)

#![allow(unexpected_cfgs)]

use pgrx::prelude::*;

pgrx::pg_module_magic!();

pub(crate) mod bulk_write;
pub(crate) mod hydration;
pub(crate) mod ledger_error_map;
pub(crate) mod pool_lock;
pub(crate) mod submit;

// ── _PG_init ────────────────────────────────────────────────────────
//
// Path A is shmem-free, so this hook only exists to give PG something
// to invoke when the .so is loaded via LOAD or via the implicit load
// from a pg_extern call. No GUC registration today either — the
// 8-step orchestration (acct-v4xz) can register `ledger_direct.*`
// GUCs at that point if any prove necessary.

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {}

/// One-line scaffolding banner. Used by Phase 2 acceptance to confirm
/// the extension loads cleanly and the SPI surface is wired.
#[pg_extern]
fn ledger_direct_hello() -> String {
    format!(
        "ledger_direct {} (acct-bnhr scaffolding) — Path A: synchronous in-tx ledger_submit_trx",
        env!("CARGO_PKG_VERSION"),
    )
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    /// Smoke test: the hello fn returns a stable prefix. Guards against
    /// CARGO_PKG_VERSION drift breaking the literal as a contract while
    /// still asserting the SPI surface is reachable.
    #[pg_test]
    fn hello_pg_extern_reachable() {
        let banner =
            Spi::get_one::<String>("SELECT ledger_direct_hello()").expect("SPI call failed");
        let banner = banner.expect("hello fn returned NULL");
        assert!(
            banner.starts_with("ledger_direct "),
            "unexpected banner: {banner:?}"
        );
        assert!(
            banner.contains("Path A"),
            "banner missing path identifier: {banner:?}"
        );
    }
}

/// Required by `cargo pgrx test`. Returns the postgresql.conf lines to
/// add for tests. Empty for Path A — no GUCs to set, no
/// shared_preload_libraries because there is no shmem to allocate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
