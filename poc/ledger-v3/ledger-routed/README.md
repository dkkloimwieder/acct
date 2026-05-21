# ledger-routed

pgrx 0.18 extension implementing **Path B** of ledger-v3 (design-v3 §5). The caller's enqueue SPI returns immediately after staging a submission in shmem; a router BGWorker groups submissions by pool-overlap into commit_groups; a committer BGWorker pool processes commit_groups in its own PG transactions. **N submissions per fsync** — amortizes the durability cost across the commit_group.

## API surface

Enqueue SPI:

```sql
SELECT ledger_enqueue_trx(
    p_trx_type   trx_type,
    p_source_id  bigint,
    p_posted_at  timestamptz,
    p_lines      <line array>
) RETURNS bigint;  -- shmem-local submission_id (internal; per design-v3 §5.1)
```

Caller polls for completion via `SELECT EXISTS (SELECT 1 FROM trx WHERE trx_type=$1 AND source_id=$2)` — per design-v3 §10.5, the existence of the `trx` row is the only durable signal that a submission was recorded. No `submission_status` table; in-flight submissions evaporate cleanly on postmaster crash (no DB orphans).

Observability pg_externs exposed by `src/stats.rs`: `ledger_routed_router_superbatch_count`, `router_total_submissions`, `committer_drains_total`, `eject_total_count`, `committer_pipeline_ns_total/count`, commit_group histogram, arena outstanding. GUCs in `src/lib.rs`: `router_window_size`, `batch_size_max`, `batch_window_us`, `committer_count`, `committer_lease_ms`, `max_eject_count`, `caller_tx_timeout_ms`, `snapshot_layer_limit_per_pool`, `queue_full_timeout_ms`.

## How exercised

`shared_preload_libraries=ledger_routed` is required for shmem reservation, so installation involves a container restart:

```bash
bash scripts/install-routed.sh                 # acct-ms8v — edits postgresql.conf + restarts
bash scripts/run-tests.sh --path routed        # acct-21yx
```

**Cluster-per-binary is essential here** — BGWorkers persist state (identity slots, in-flight commit_groups, eject counters) across test runs within one Postgres lifetime. The harness restarts `acct-postgres` between each test binary to guarantee clean shmem state. See [bd memory `v21-test-harness-isolation-via-cluster-per-binary`]. Recovery scenarios (postmaster kill, committer kill via test_hooks, router death) are exercised by `acceptance_routed_postmaster_restart` / `acceptance_routed_orphan_recovery`.

## Source layout (filled in by follow-up issues)

Carried verbatim from `poc/queue-extension-v21/`:
- `src/arena.rs` — bump + LIFO freelist (acct-17p5)
- `src/identity.rs` — `CommitterIdentitySlot` (PID-recycling-safe; acct-29a1)
- `src/cleanup.rs` — three-case CAS handling (acct-qsga)

Rewritten for v3:
- `src/shmem.rs` — `StagingQueue` + `CommitterQueue` + `SpilloverArena` (acct-29a1, with v3 field renames per plan §E)
- `src/payload.rs` — `PocV3Submission` + `PocV3Line` serde_json encoding (acct-damm)
- `src/enqueue.rs` — `ledger_enqueue_trx` SPI (acct-mgb7)
- `src/router.rs` — window scan + union-find + commit_group emit (acct-zedi)
- `src/committer.rs` — claim + hydrate + pristine-replay + bulk-write + COMMIT (acct-usn2)
- `src/bulk_write.rs` — same SQL shape as `ledger-direct` (acct-me64)
- `src/recovery.rs` — router-orphan boot sweep (acct-r2ud)
- `src/stats.rs` — observability accessors (acct-hmwr)

Features: `pg18` (default), `pg_test`, `test_hooks` — same gating pattern as `ledger-direct`.
