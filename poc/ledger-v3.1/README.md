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

## Routed flavor (P3.1) — build, install, test
```bash
bash scripts/install-routed-c.sh                 # build + preload ledger_routed_c, CREATE in poc_v3_1
bash scripts/run-tests.sh --path routed          # cluster-per-binary integration suite
```
`ledger_enqueue_trx_c(trx_type, source_id, posted_at, lines jsonb) RETURNS bigint` stages a
descriptor (incl. the caller's `user_tx_xid`) into the shmem staging queue and returns a
shmem-local submission_id — **no DB write at enqueue** (§6.1). P3.1 ships the shmem foundation
(staging + committer queues, spillover arena, committer identity registry — §6.2), the arena
allocator, the payload codec, GUCs (`ledger_routed_c.*`), and BGWorker registration with
lifecycle-only **shells**: the router scan/affinity grouping (P3.2), the committer pipeline
(P3.3), and recovery/error handling (P3.4) are not yet implemented. There is **no PoolSeqTable**
— Path C drops the strict cross-window FIFO ordering the v3 strict path needed (§14.2).

> Preloading both `ledger_routed` (v3) and `ledger_routed_c` (v3.1) plus the other PoC streams'
> workers needs headroom: the dev container's `max_worker_processes` was raised to 32 so the full
> 4-committer pool starts (see `acct-8cn2`).

## Crates
- `ledger-core` — pure Rust, no pgrx: per-method state transitions + provisional dispatch (§8). ✓
- `ledger-direct-c` — pgrx extension: `ledger_submit_trx_c` (§5). ✓
- `ledger-routed-c` — pgrx extension: `ledger_enqueue_trx_c` + shmem + BGWorker shells (§6). ◐ (P3.1)
- `ledger-harness` — multi-session measurement binary (§10).

## Phases — epic `acct-2ttr`
| Phase | bd issue      | Goal | Status |
|-------|---------------|------|--------|
| P1.1  | `acct-2ttr.1` | scaffold workspace + schema migrations | ✓ |
| P1.2  | `acct-2ttr.2` | ledger-core (methods + provisional dispatch + unit tests) | ✓ |
| P2    | `acct-2ttr.3` | ledger-direct-c (`ledger_submit_trx_c`) | ✓ |
| P3.1  | `acct-2ttr.4` | ledger-routed-c shmem + `ledger_enqueue_trx_c` | ✓ |
| P3.2  | `acct-2ttr.5` | router BGWorker (window scan + union-find affinity) | |
| P3.3  | `acct-2ttr.6` | committer pool (provisional dispatch, drop-and-continue) | |
| P3.4  | `acct-2ttr.7` | recovery + committer SQL error handling | |
| P4    | `acct-2ttr.8` | harness (3 submission modes + deep-pool seeding + lock-hold metric) | |
| P5    | `acct-2ttr.9` | characterization & PoC report | |

Stream label `stream:ledger-v3.1`; administrative pause gate `acct-1wyk` (`ledger-v3.1-PAUSE`).

## Deliberately omitted (design-v3.1 §13)
Recalc/close (authoritative FIFO/LIFO reconciliation), negative inventory, multi-currency,
effective-dated standard costs, account-balance denormalization, period-close mechanics,
webhook delivery, multi-tenant isolation, routed caller observability beyond harness polling.
