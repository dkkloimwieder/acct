//! ledger-routed-c: pgrx 0.18 extension for ledger-v3.1 Path C (routed flavor).
//!
//! Authoritative spec: `poc/design_research/design-v3.1.md` §6.
//!
//! Exposes `ledger_enqueue_trx_c(trx_type, source_id, posted_at, lines)` — pushes a
//! submission descriptor to a shmem staging queue and returns a shmem-local submission_id
//! (no DB write at enqueue). A router BGWorker groups submissions by pool overlap
//! (union-find) into commit_groups; a committer BGWorker pool processes each group in one
//! PG tx, dispatching to `plan_apply_provisional` and bulk-writing once.
//!
//! Unlike `ledger-routed` (Path B strict), Path C's committer uses **drop-and-continue**,
//! not pristine-snapshot replay — Path C has no cross-trx hot-path state dependency
//! (design-v3.1 §14.2).
//!
//! Scaffold stub (P1.1, acct-2ttr.1). Shmem layout + enqueue land in P3.1 (acct-2ttr.4);
//! router in P3.2 (acct-2ttr.5); committer in P3.3 (acct-2ttr.6); recovery in P3.4 (acct-2ttr.7).

#![allow(unexpected_cfgs)]

use pgrx::prelude::*;

::pgrx::pg_module_magic!();

/// Scaffold smoke function; replaced by the real SPI + BGWorker surface in P3.
#[pg_extern]
fn ledger_routed_c_scaffold_version() -> &'static str {
    "ledger-v3.1 routed-c scaffold (P1.1)"
}
