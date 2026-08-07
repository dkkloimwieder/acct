# ledger-v3.1 measurement environment baseline

_Captured `2026-07-10T11-00-30Z` by `bench/capture-env.sh` (`acct-0at4.10.2`). LIVE `pg_settings`,
not `db/postgresql.conf` — the base conf is overridden by `postgresql.auto.conf`._

## Host & container

| Item | Value |
|---|---|
| Kernel | `Linux 7.0.0-22-generic` |
| CPUs (host) | 8 |
| RAM (host) | 60.9 GiB |
| Load @ capture | `5.09 2.23 1.09 2/2760 2867478` |
| Container CPU pin | **unpinned** (no cpuset), NanoCpus=`0` |
| Container memlock | `-1/-1` |
| PG data volume | `/var/lib/docker/volumes/acct_acct-pgdata/_data` (WAL shares this mount — **not** isolated) |
| pgbouncer | pool_mode=`transaction`, default_pool_size=`24` |

> **Noisy daily-driver host** (`project_pocv3_bench_host_is_noisy_workstation`): this is a
> workstation, not an isolated bench box; background load (Chrome) swings absolute throughput
> ~2×. Trust load-robust structural ratios; gate timed runs on a quiet host (`common.sh
> wait_for_quiet_host`). The `Load @ capture` above stamps this snapshot's host state.

## fsync latency — WAL device (pg_test_fsync -s 2)

**fdatasync ≈ 551.816 ops/sec** (Linux default WAL sync). Full `pg_test_fsync`:

```
2 seconds per test
O_DIRECT supported on this platform for open_datasync and open_sync.

Compare file sync methods using one 8kB write:
(in "wal_sync_method" preference order, except fdatasync is Linux's default)
        open_datasync                       231.539 ops/sec    4319 usecs/op
        fdatasync                           551.816 ops/sec    1812 usecs/op
        fsync                               157.140 ops/sec    6364 usecs/op
        fsync_writethrough                              n/a
        open_sync                           159.308 ops/sec    6277 usecs/op

Compare file sync methods using two 8kB writes:
(in "wal_sync_method" preference order, except fdatasync is Linux's default)
        open_datasync                       115.270 ops/sec    8675 usecs/op
        fdatasync                           547.868 ops/sec    1825 usecs/op
        fsync                               158.240 ops/sec    6320 usecs/op
        fsync_writethrough                              n/a
        open_sync                            78.642 ops/sec   12716 usecs/op

Compare open_sync with different write sizes:
(This is designed to compare the cost of writing 16kB in different write
open_sync sizes.)
         1 * 16kB open_sync write           157.071 ops/sec    6367 usecs/op
         2 *  8kB open_sync writes           79.557 ops/sec   12570 usecs/op
         4 *  4kB open_sync writes           39.469 ops/sec   25336 usecs/op
         8 *  2kB open_sync writes           19.622 ops/sec   50962 usecs/op
        16 *  1kB open_sync writes            9.787 ops/sec  102172 usecs/op

Test if fsync on non-write file descriptor is honored:
(If the times are similar, fsync() can sync data written on a different
descriptor.)
        write, fsync, close                 155.621 ops/sec    6426 usecs/op
        write, close, fsync                 158.916 ops/sec    6293 usecs/op

Non-sync'ed 8kB writes:
        write                           2012483.469 ops/sec       0 usecs/op
```

## PostgreSQL settings (live)

| Setting | Value | Unit |
|---|---|---|
| `autovacuum` | on |  |
| `autovacuum_naptime` | 30 | s |
| `autovacuum_vacuum_scale_factor` | 0.05 |  |
| `backend_flush_after` | 64 | 8kB |
| `bgwriter_delay` | 200 | ms |
| `bgwriter_lru_maxpages` | 200 |  |
| `checkpoint_completion_target` | 0.9 |  |
| `checkpoint_timeout` | 900 | s |
| `commit_delay` | 20 |  |
| `commit_siblings` | 5 |  |
| `effective_cache_size` | 3145728 | 8kB |
| `effective_io_concurrency` | 256 |  |
| `fsync` | on |  |
| `full_page_writes` | on |  |
| `huge_pages` | try |  |
| `io_combine_limit` | 16 | 8kB |
| `io_method` | io_uring |  |
| `jit` | on |  |
| `maintenance_work_mem` | 2097152 | kB |
| `max_connections` | 500 |  |
| `max_locks_per_transaction` | 128 |  |
| `max_wal_size` | 8192 | MB |
| `min_wal_size` | 2048 | MB |
| `server_version` | 18.3 (Debian 18.3-1.pgdg13+1) |  |
| `shared_buffers` | 1048576 | 8kB |
| `shared_preload_libraries` | pg_stat_statements, pg_cron, ledger_routed_c |  |
| `synchronous_commit` | on |  |
| `track_io_timing` | on |  |
| `wal_buffers` | 8192 | 8kB |
| `wal_compression` | lz4 |  |
| `wal_level` | replica |  |
| `work_mem` | 65536 | kB |

## Routed committer GUCs (live)

| GUC | Value |
|---|---|
| `ledger_routed_c.committer_count` | 4 |
| `ledger_routed_c.affinity_scheme` | 0 |
| `ledger_routed_c.batch_size_max` | 200 |
| `ledger_routed_c.batch_window_us` | 500 |
| `ledger_routed_c.router_pack_disjoint` | on |
| `ledger_routed_c.wake_on_enqueue` | off |

## Operator knobs surfaced but NOT applied (shared infra)

These change the **shared** acct-postgres container / acct-root compose, which every PoC
stream uses. They are documented here as deliberate operator choices, not silently applied.

- **CPU pinning** — the container is unpinned, so the OS scheduler migrates PG backends
  across all 8 cores and co-schedules them with host load. To pin, add `cpuset:` (or
  `cpus:`) to the acct-root `docker-compose` service and recreate — but recreating
  acct-postgres wipes every runtime-installed `.so` and boot-blocks on the preloaded routed
  extensions (`recreating-the-shared-acct-postgres-container`), so coordinate across streams.
- **autovacuum off for short cells** — `autovacuum=on` (naptime 30s) is correct for soak
  runs; for short (<1 min) throughput cells where a vacuum cycle would add variance, an
  operator may `ALTER SYSTEM SET autovacuum=off; <restart>`, run, then `RESET` + restart.
  Not applied by default — it mutates the shared container and hides wraparound pressure that
  the `acct-0at4.10.3` drift soak specifically wants to observe.
- **WAL isolation** — WAL is on the data volume, not a separate device; the fsync latency
  above is the reported alternative (FEEDBACK #10).
