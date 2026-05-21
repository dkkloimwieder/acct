# ledger-v3 — cost-ledger PoC

Greenfield PoC implementing the spec at `poc/design_research/design-v3.md`. Ships two execution paths against the same schema and the same Rust transformation core, to characterize the concurrency × overlap regime where each path wins (design-v3 §10.4).

- **Path A** (`ledger-direct/`): synchronous SPI function `ledger_submit_trx` executes the full ledger work inside the caller's user-tx. One PG transaction per submission, one fsync, caller-visible failures.
- **Path B** (`ledger-routed/`): SPI function `ledger_enqueue_trx` stages submissions in shmem; a router BGWorker groups by pool-overlap; a committer BGWorker pool writes batched commit_groups in their own PG transactions. N submissions per fsync.

Both paths call `ledger-core::plan_apply` (pure Rust, no pgrx) for the per-method cost computation (FIFO, LIFO, WAC, STD, specific-id). The `ledger-harness/` binary drives synthetic workloads across scenarios S1–S6 and emits comparable JSON measurements per design-v3 §8.5.

## Crates

| Crate | Purpose |
|---|---|
| `ledger-core` | Pure-Rust transformation core. `plan_apply(snapshot, lines, posted_at) -> PlanResult` over Snapshot + per-method dispatch. Unit-tested with no DB. |
| `ledger-direct` | pgrx 0.18 extension exposing `ledger_submit_trx` SPI function. Path A. |
| `ledger-routed` | pgrx 0.18 extension exposing `ledger_enqueue_trx` SPI + router/committer BGWorkers + shmem queues. Path B. |
| `ledger-harness` | Binary: multi-session driver, workload generator, scenarios S1–S6, measurement collection, JSON report. |

## How to run

```bash
# One-time
bash scripts/create-poc-v3-db.sh   # creates the poc_v3 database in the dev container
bash scripts/run-migrations.sh     # applies db/migrations/

# Install whichever path you want to measure
bash scripts/install-direct.sh     # builds + loads ledger-direct
bash scripts/install-routed.sh     # builds + loads ledger-routed (sets shared_preload_libraries)

# Regression suite (cluster-per-binary)
bash scripts/run-tests.sh --path direct
bash scripts/run-tests.sh --path routed

# Measurements
cargo run -p ledger-harness -- seed-pools --count 10000 --skus 1000 --locations 10
cargo run -p ledger-harness -- run --scenario s2 --path direct --duration 60s
cargo run -p ledger-harness -- run --scenario s2 --path routed --duration 60s
cargo run -p ledger-harness -- equivalence --scenario s1
```

Scripts, migrations, and per-crate READMEs are added in follow-up beads issues; this is the workspace skeleton only.

## Spec, plan, beads

- Spec: `poc/design_research/design-v3.md`
- Plan: `~/.claude/plans/formulate-a-detailed-plan-dazzling-diffie.md`
- bd Epics: acct-qq6z (P1), acct-963t (P2), acct-xpdq (P3), acct-2lt7 (P4), acct-mz0g (P5), acct-dipt (P6)
