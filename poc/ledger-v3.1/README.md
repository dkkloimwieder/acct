# ledger-v3.1 — cost-ledger PoC, Path C (provisional hot path)

Implements **Path C** of design-v3.1: the *provisional hot path with deferred recalc/close*.
FIFO/LIFO depletions record a **provisional** unit_cost (the pool's running average, or a
standing standard cost) and touch only the aggregate row of `pool_state` (`layer_id = 0`) —
never iterating layer rows on the hot path. Authoritative FIFO/LIFO reconciliation
(recalc/close) is **out of scope** (§7, §13).

Spec: [`../design_research/design-v3.1.md`](../design_research/design-v3.1.md). The companion
`ledger-v3` workspace implements Paths A/B (direct + routed **strict-mode**). This is a fresh,
self-contained workspace — not an extension of `ledger-v3` — that copy-adapts its proven
skeleton (schema, ledger-core dispatch, shmem/router/committer stack).

## What the PoC measures
1. **Direct flavor** — per-trx lock-hold time for FIFO/LIFO is **constant w.r.t. pool depth**
   (validated at depths 10/100/1000). The architectural premise.
2. **Routed flavor** — 1000 concurrent submissions to one hot FIFO pool collapse into one
   commit_group → one `pool_lock` acquisition + one aggregate UPDATE.

## Posture
- **Separate database**: `poc_v3_1` on `localhost:5111` (shares the dev container, own DB name).
- **Separate workspace**: standalone `cargo build` / `cargo test`; nothing imports from `acct`
  or `ledger-v3`, and they don't import from here.
- **Separate migrations** under `db/migrations/`, applied via `sqlx-cli`.
- pgrx `=0.18.0`, edition 2024, rust-version 1.87.
- **Numeric model**: `BIGINT` at 1e-6 fixed precision; WAC running average via banker's-rounding
  integer division (`numeric::banker_div`, i128 intermediate).

## Setup
```bash
bash scripts/create-poc-v3-1-db.sh    # create poc_v3_1 (idempotent)
bash scripts/run-migrations.sh        # apply migrations 0001-0005
cargo build                           # workspace builds
```

## Direct flavor (P2) — build, install, test
```bash
bash scripts/install-direct-c.sh                 # build + load ledger_direct_c into poc_v3_1
bash scripts/run-tests.sh --path direct          # cluster-per-binary integration suite
```
`ledger_submit_trx_c(trx_type, source_id, posted_at, lines jsonb) RETURNS bigint` runs the
§5.1 pipeline synchronously in the caller's tx: optimistic `pool_lock` FOR UPDATE → aggregate
hydration → `ledger_core::plan_apply_provisional` → ordered bulk write → `trx.id`.

## Routed flavor (P3.1 enqueue + P3.2 router) — build, install, test
```bash
bash scripts/install-routed-c.sh                 # build + preload ledger_routed_c, CREATE in poc_v3_1
bash scripts/run-tests.sh --path routed          # cluster-per-binary integration suite
```
`ledger_enqueue_trx_c(trx_type, source_id, posted_at, lines jsonb) RETURNS bigint` stages a
descriptor (incl. the caller's `user_tx_xid`) into the shmem staging queue and returns a
shmem-local submission_id — **no DB write at enqueue** (§6.1). P3.1 shipped the shmem foundation
(staging + committer queues, spillover arena, committer identity registry — §6.2), the arena
allocator, the payload codec, GUCs (`ledger_routed_c.*`), and BGWorker registration.

P3.2 implements the **router** BGWorker (§6.3): each tick head-scans the staging queue up to
`router_window_size` (skipping eject-cooldown slots), gates on `batch_window_us`, union-finds
candidates by pool overlap into connected components, chunks any component over `batch_size_max`
preserving enqueue order, and emits each chunk as a commit_group (CAS staging `pending→processing
→routed`, push a committer-queue entry `empty→ready`). There is **no PoolSeqTable and no
order-sensitive no-split case** — Path C records provisional aggregate updates, so any component
may be split across commit_groups and provisional unit_costs are allowed to differ across
orderings (§9.4, §14.2).

P3.3 implements the **committer** pool (§6.4, default 4 workers): claim a commit_group via CAS
identity election → `pg_xact_status` triage (eject in-progress callers, drop aborted) →
**pre-flight dedup** against `trx` + within-batch (first wins) → pool-id union → `pool_lock` FOR
UPDATE → hydrate → **drop-and-continue** apply (process submissions in enqueue order against one
working snapshot; a submission whose `plan_apply_provisional` fails is dropped, the rest continue
— **no pristine-snapshot replay**, §14.2) → batch write → COMMIT → cleanup. The write collapses a
whole commit_group's depletions into **one aggregate UPDATE per pool** (the final working-snapshot
state), one `pool_lock` acquisition, and one fsync — the §6.7 batching win.

P3.4 adds **recovery + committer SQL error handling**. The router runs a boot-recovery sweep
(§6.5) at startup: it re-stamps interrupted data-before-flag stores, takes over commit_groups
whose owning committer died (CQ `in_flight→ready`, reclaimed by a live committer — the committer's
pre-flight dedup is the recovery source of truth, no pristine-replay), and reverts orphaned staging
entries. The committer wraps its lock→hydrate→apply→write phase in a subtransaction (§6.8): a
transient SQLSTATE (40P01 deadlock / 40001 serialization) is retried with exponential backoff (≤5);
a non-retryable SQLSTATE — or an exhausted retry budget — **poisons** the commit_group (terminal CQ
state `valid==4` dead-letter, submissions lost).
Observability: `ledger_routed_c_committer_{drains,pool_lock_acquisitions,aggregate_upserts,trx_committed,dedup_skips,dropped_submissions,tx_failures,poisoned,deadlock_retries}_total()`
(`poisoned` / `deadlock_retries` new in P3.4) plus `ledger_routed_c_committer_takeover_count()`,
and `ledger_routed_c_committer_queue_state_counts()` (now incl. `poisoned`) /
`ledger_routed_c_ready_commit_groups()` / `ledger_routed_c_recovery_complete()`.

> Preloading both `ledger_routed` (v3) and `ledger_routed_c` (v3.1) plus the other PoC streams'
> workers needs headroom: the dev container's `max_worker_processes` was raised to 32 so the full
> 4-committer pool starts (see `acct-8cn2`).

## Harness (P4) — measurement binary

A plain `sqlx` + `tokio` client (not pgrx) that drives the installed extensions
and emits JSON reports. Build with `cargo build --release -p ledger-harness`.

```bash
# 1. Seed a pool universe, optionally deep-seeding layer rows (§10.5). Path C's
#    hot path never makes FIFO/LIFO layers, so deep pools come from direct SQL.
ledger-harness seed-pools --count 10000 --method-mix all-fifo --depth 1000

# 2. Drive a scenario in one of the three §10.0 submission modes.
ledger-harness run --scenario s7 --mode direct-per-call --duration 30s
ledger-harness run --scenario s7 --mode direct-batched --batch-size 50 --duration 30s
ledger-harness run --scenario s7 --mode routed --duration 30s

# 3. Cross-flavor equivalence: identical input → identical aggregate qty (§11.1).
ledger-harness equivalence --scenario s7 --callers 8 --submissions-per-caller 50
```

- **Three submission modes** (§10.0): `direct-per-call` (one user-tx per
  `ledger_submit_trx_c`), `direct-batched` (N calls in one user-tx, §5.5
  commit/lock amortization), `routed` (`ledger_enqueue_trx_c` → committer pool,
  §6 cross-caller aggregation).
- **Scenarios S1–S8** (§10.6): S1–S4 shallow receipt workloads (baseline,
  routing amortization, complexity); S5/S6 1000-caller shallow FIFO (hot-pool vs
  disjoint); **S7/S8 deep-pool FIFO depletions — Path C's home field** (§11.2).
- **Headline metric**: per-trx ack latency captured per seeded `--depth`. The
  in-function critical section is dominated by pool_lock hold time and depth is
  the only thing varying across a seed sweep, so flat latency across depths
  10→1000 confirms the constant-lock-hold premise. `bench/run-lockhold-sweep.sh`
  runs that sweep.
- **Routed observability**: the JSON `routed` block carries the
  `ledger_routed_c_committer_*` deltas — `commit_group_size_avg` and the ratio
  of trx committed to pool_lock acquisitions / aggregate upserts quantify the
  §6.7 batching win — plus the P3.4 `poisoned` / `deadlock_retries` / `takeover`
  counts.
- **1000-caller scenarios (S5/S7/S8)** are driven through a pgbouncer
  transaction pool (`bench/setup-pgbouncer.sh up`) because the dev container's
  io_uring memlock ceiling can't hold 1000 direct backends (`acct-8cn2`); the
  bench runners point `--dsn` at the pooler for those scenarios. `--max-callers`
  caps concurrency for pooler-less smoke runs (the cap is recorded in the report).
- `bench/`: `run-lockhold-sweep.sh` (§11.2), `run-crossover.sh` (§11.4 mode ×
  scenario matrix), `run-equivalence.sh` (§11.1), `setup-pgbouncer.sh`. Every
  harness invocation is hard-`timeout`-wrapped. **The actual bake-off RESULTS +
  PoC report are P5 (`acct-2ttr.9`)**; P4 delivers the machinery.

> Known limitation (filed separately): `ledger_submit_trx_c` (direct) emits one
> aggregate UPSERT row per line, so a single submission must touch **distinct**
> pools — listing the same pool twice fails the bulk UPSERT. The routed committer
> coalesces per pool and is unaffected. The harness generates distinct pools per
> submission accordingly (§5.1 "touched pool_ids … dedup").

## Crates
- `ledger-core` — pure Rust, no pgrx: per-method state transitions + provisional dispatch (§8). ✓
- `ledger-direct-c` — pgrx extension: `ledger_submit_trx_c` (§5). ✓
- `ledger-routed-c` — pgrx extension: `ledger_enqueue_trx_c` + shmem (§6.1/§6.2) + router (§6.3) + committer (§6.4) + recovery (§6.5) + SQL error handling (§6.8). ✓ (P3.4)
- `ledger-harness` — multi-session measurement binary (§10). ✓ (P4)

## Phases — epic `acct-2ttr`
| Phase | bd issue      | Goal | Status |
|-------|---------------|------|--------|
| P1.1  | `acct-2ttr.1` | scaffold workspace + schema migrations | ✓ |
| P1.2  | `acct-2ttr.2` | ledger-core (methods + provisional dispatch + unit tests) | ✓ |
| P2    | `acct-2ttr.3` | ledger-direct-c (`ledger_submit_trx_c`) | ✓ |
| P3.1  | `acct-2ttr.4` | ledger-routed-c shmem + `ledger_enqueue_trx_c` | ✓ |
| P3.2  | `acct-2ttr.5` | router BGWorker (window scan + union-find affinity) | ✓ |
| P3.3  | `acct-2ttr.6` | committer pool (provisional dispatch, drop-and-continue) | ✓ |
| P3.4  | `acct-2ttr.7` | recovery + committer SQL error handling | ✓ |
| P4    | `acct-2ttr.8` | harness (3 submission modes + deep-pool seeding + lock-hold metric) | ✓ |
| P5    | `acct-2ttr.9` | characterization & PoC report ([`results/POC-REPORT.md`](results/POC-REPORT.md)) | ✓ |

Stream label `stream:ledger-v3.1`; administrative pause gate `acct-1wyk` (`ledger-v3.1-PAUSE`).

## Deliberately omitted (design-v3.1 §13)
Recalc/close (authoritative FIFO/LIFO reconciliation), negative inventory, multi-currency,
effective-dated standard costs, account-balance denormalization, period-close mechanics,
webhook delivery, multi-tenant isolation, routed caller observability beyond harness polling.
