//! ledger-routed: pgrx 0.18 extension for ledger-v3 Path B.
//!
//! Authoritative spec: `poc/design_research/design-v3.md` §5.
//!
//! Caller invokes `ledger_enqueue_trx(trx_type, source_id, posted_at, lines)` inside
//! their own user-tx (or as a standalone call). The function pushes a descriptor
//! into the shmem staging queue and returns. A router BGWorker scans the staging
//! window, groups submissions by pool-overlap into commit_groups via union-find, and
//! dispatches to a committer BGWorker pool. Each committer claims a commit_group,
//! hydrates a snapshot, runs `ledger_core::plan_apply` per submission (with pristine-
//! snapshot replay on per-submission failure), bulk-writes trx/trx_line/pool_state/
//! posting_line, and COMMITs. One fsync per commit_group; amortizes by N.
//!
//! Modules added by follow-up beads issues:
//! - `shmem.rs`        — acct-29a1 (StagingQueue + CommitterQueue + arena structs)
//! - `identity.rs`     — acct-29a1 (CommitterIdentitySlot — verbatim port from v21)
//! - `arena.rs`        — acct-17p5 (bump + LIFO freelist — verbatim port from v21)
//! - `payload.rs`      — acct-damm (PocV3Submission + PocV3Line serde_json encoding)
//! - `enqueue.rs`      — acct-mgb7 (ledger_enqueue_trx pg_extern)
//! - `router.rs`       — acct-zedi (window scan + union-find + commit_group emit)
//! - `committer.rs`    — acct-usn2 (claim + hydrate + pristine-replay + bulk-write + COMMIT)
//! - `bulk_write.rs`   — acct-me64 (UNNEST helpers, same SQL shape as ledger-direct)
//! - `cleanup.rs`      — acct-qsga (three-case CAS — verbatim port from v21)
//! - `recovery.rs`     — acct-r2ud (router-orphan boot sweep; postmaster restart trivial)
//! - `stats.rs`        — acct-hmwr (observability pg_externs)
//!
//! `pgrx::pg_module_magic!()`, `_PG_init`, and the shmem reservation are added in
//! acct-29a1; this file is the workspace stub.
