# acct

ERP ledger and inventory system: SKU × location quantity tracking, per-routing-step WIP, document lifecycle (WO/SO/TO/PO), double-entry GL, multi-currency, reservations, commodity provisional pricing, period close.

Postgres-native v0.2 design — see `ledger_design_consolidated_v0.md`.

## Status

**Phase 0 is functionally complete.** The schema, write path (`post_transfers` with `'standard'` and `'wac'` cost dispatchers), reservations, period close, reconciliation, the conformance harness (T5), and the §14.1 perf baseline (`perf_baseline_v0.md`, 13 shapes) all shipped under epic `acct-93b`. The three remaining open issues (`bd ready`) are P3 and explicitly gated on Phase 1 framing or §14.1 follow-up evidence — claim them only if their gates have cleared.

## Quick start

Fresh clone to a passing test suite in three commands:

```bash
./scripts/dev-up.sh           # build + start Postgres 18 (io_uring) in Docker; verifies extensions
./scripts/run-migrations.sh   # apply all migrations under db/migrations/
./scripts/run-tests.sh        # cargo integration tests against an ephemeral acct_test DB
```

If you don't have Docker, `cargo`, or `sqlx-cli`, see [Prerequisites](#prerequisites).

## Stack

Rust + `sqlx` + `sqlx-cli` + Postgres 18 (`io_method=io_uring`, `pg_stat_statements`, `pg_cron`). Tests via `cargo test` (tokio + sqlx integration tests). **No pgTAP, no ORMs, no task runners** — shell scripts in `scripts/` are the canonical entry points.

## Prerequisites

- **Docker** + **Docker Compose** for the dev Postgres container.
- **Rust toolchain** — `rustup default stable`.
- **sqlx-cli** — `cargo install sqlx-cli --no-default-features --features rustls,postgres`.

## Repository layout

| Path | Purpose |
|---|---|
| `ledger_design_consolidated_v0.md` | The design spec — single source of truth. Read first. |
| `ARCHIVE/` | Predecessor design docs (v0.1 + review). Historical, do not edit. |
| `db/migrations/` | sqlx-cli reversible SQL migrations (`NNNN_<name>.{up,down}.sql`). |
| `db/fixtures/small/seed.sql` | Minimal-but-realistic test fixture. |
| `db/Dockerfile` | Postgres 18 + `pg_cron` image build. |
| `db/README.md` | Dev DB details — image, GUC overrides, write path, scheduled jobs. |
| `tests/` | Rust integration tests (`cargo test`). Each `tests/*.rs` is a binary. |
| `tests/common/mod.rs` | Shared test helpers (connect, fixture reset, error assertions, etc.). |
| `tests/data/conformance.json` | T5 conformance fixture — 107 (input, expected_response, expected_state) triples (11 also tagged `also_split` for batch-vs-split equivalence). |
| `scripts/` | Dev helpers — `dev-up.sh`, `run-migrations.sh`, `run-tests.sh`, `ci-check.sh`, `verify-o1-expiry.sh`, `verify-o2-recon.sh`. |
| `Cargo.toml` / `src/lib.rs` | Rust crate root; library only (no binary yet). |
| `docker-compose.yml` | Dev DB service definition. |
| `CLAUDE.md` | Guidance for Claude Code sessions; load-bearing design decisions. |
| `AGENTS.md` | Beads issue-tracker integration block. |
| `.beads/` | Beads (bd) issue store, embedded Dolt. |

## Development workflow

### Dev DB

```bash
./scripts/dev-up.sh        # build + start; verifies io_method=io_uring, pg_cron, pg_stat_statements
./scripts/dev-down.sh      # stop; data volume preserved
./scripts/dev-down.sh --wipe   # stop + remove data volume
psql 'postgres://acct:acct_dev@localhost:5111/acct'   # connect
```

See `db/README.md` for image build, GUC overrides, the dev seccomp note, and scheduled-job inspection.

### Migrations

```bash
./scripts/run-migrations.sh                                          # apply all pending
sqlx migrate info   --source db/migrations                           # what's applied / pending
sqlx migrate revert --source db/migrations                           # roll back the most recent
sqlx migrate add    --source db/migrations -r --sequential <name>    # author a new pair
```

Reversible: every `NNNN_<name>.up.sql` has a matching `.down.sql`. `scripts/ci-check.sh` is the pre-push guard for round-trip cleanliness.

### Tests

```bash
./scripts/run-tests.sh             # full suite against ephemeral acct_test
./scripts/run-tests.sh smoke       # filter by name to one binary
```

The harness drops/recreates `acct_test`, applies all migrations, seeds `db/fixtures/small/seed.sql`, runs `cargo test`, and drops `acct_test` again on exit. The dev `acct` DB is untouched.

Helpers live in `tests/common/mod.rs` (loaded via `mod common;` per binary): `connect_test_db`, `reset_to_fixture`, `expect_sqlstate`, `call_post_transfers`, `try_reserve`, `seed_stock`, `account_id_for_selector`, `snapshot_balances`, plus more.

`run-tests.sh` defaults to `RUST_TEST_THREADS=1` so the conformance binary's two `#[tokio::test]` functions don't race on the shared DB. Override via the env var if explicit parallelism is desired.

### Schema integrity check

```bash
./scripts/ci-check.sh   # clean-DB migrate-revert-redeploy round-trip + schema digest
```

Five steps against an ephemeral `acct_ci`: `sqlx migrate run` → assert all installed → revert all → re-run → `pg_dump -s | sha256sum`. Run this before pushing schema changes; **there is no remote CI** — this script is the check.

### Load test (opt-in)

```bash
./scripts/run-tests.sh --test load_deadlock_freedom -- --ignored --nocapture
```

Defaults to 32 writers × 30 seconds. Spec target (100 writers × 30 min):

```bash
T4_DURATION_SECS=1800 T4_WRITERS=100 \
  ./scripts/run-tests.sh --test load_deadlock_freedom -- --ignored --nocapture
```

### Operations smoke checks

```bash
./scripts/verify-o1-expiry.sh   # confirm pg_cron reservation_expiry is firing on acct
./scripts/verify-o2-recon.sh    # confirm run_daily_reconciliation() detects imbalance on acct
```

## What the tests verify (Phase 0 acceptance)

The Phase 0 acceptance gate (Part IV §13) maps to these tests:

| Test / script | Invariant | Issue |
|---|---|---|
| `scripts/ci-check.sh` | Migrations round-trip cleanly across `0001..NNNN` | S3 |
| `tests/double_entry.rs` | Per-ledger `SUM(debits) - SUM(credits) = 0` per `(ledger_kind, currency)` after valid posts (B3 fix) | T2 |
| `tests/no_negative.rs` | Overdraw of debit-normal account → SQLSTATE `23514` (balance-respects-normal-side CHECK) | T2 |
| `tests/idempotency.rs` | Duplicate `idempotency_key` returns `'exists'`; balance moves exactly once | T2 |
| `tests/period_lock.rs` | Closed period → P0005; `override=TRUE` succeeds | T2 |
| `tests/append_only.rs` | UPDATE/DELETE on `transfers` → P9999 (trigger) | T2 |
| `tests/closed_account.rs` | Post against `is_closed=TRUE` → P0001 | T2 |
| `tests/ledger_mismatch.rs` | qty↔value transfer → P0002 | T2 |
| `tests/currency_mismatch.rs` | Cross-currency value transfer → P0003 | T2 |
| `tests/reserve_concurrency.rs` | 5 concurrent 3-unit reserves vs on-hand=10 → exactly 3 succeed; the FOR UPDATE in `reserve_inventory()` serialises | T3 |
| `tests/reserve_insufficient.rs` | Reserve when `qty_promisable < qty` → 0 rows inserted | T3 |
| `tests/reservation_state_transitions.rs` | allocate / cancel / expire only from `'active'`; otherwise no-op | T3 |
| `tests/reservation_expiry.rs` | Single UPDATE flips 1000 overdue active rows to `'expired'`; future-actives + non-actives untouched | T3 |
| `tests/load_deadlock_freedom.rs` (opt-in, `#[ignore]`) | `pg_stat_database.deadlocks` delta = 0 across 32 writers × 30 s; per-ledger double-entry holds at end | T4 |
| `tests/conformance.rs::conformance_cases` | 107 (input, expected_response, expected_state) triples | T5 |
| `tests/conformance.rs::batch_vs_split_equivalence` | 11 `also_split`-tagged cases prove linked-batch semantics (full rollback) vs independent-call semantics (partial commit) | T5 |
| `tests/reconciliation.rs` | `run_daily_reconciliation()` detects double-entry imbalance + reservation over-promise; clean state produces zero alerts | O2 |

### Reading test failures

- **Each invariant test fails loudly with the specific SQLSTATE / value mismatch.** A panic line like `expected SQLSTATE P0001, got P9999` means the schema raised the wrong error code — start with the migration that owns the rule.
- **Conformance cases name themselves.** A failure like `[B7_amount_zero] expected SQLSTATE 99999, got 23514` pinpoints the case in `tests/data/conformance.json`.
- **Load-test deadlock panics include the writer id and full Postgres error.** A non-zero deadlocks delta means the lock-order proof in `post_transfers` regressed.

## Issue tracking

This project uses **`bd` (beads)** with embedded Dolt under `.beads/`.

```bash
bd ready                # unblocked issues
bd show <id>            # detail
bd update <id> --claim  # take an issue
bd close <id>           # finish
bd create --title="…" --description="…" --type=task|bug|feature --priority=2   # report a bug or new task
bd remember "<insight>" # persistent memory across sessions
```

Issue prefix in this repo: `acct-`. Storage is committed to git via Dolt; sync via `bd dolt push` / `bd dolt pull` once a remote is configured. See `AGENTS.md` for the full integration block.

## Documentation map

- **`ledger_design_consolidated_v0.md`** — the design spec; single source of truth.
- **`db/README.md`** — dev DB internals, write path (`post_transfers`, `reserve_inventory`), scheduled jobs.
- **`CLAUDE.md`** — load-bearing design decisions and Claude-session rules.
- **`AGENTS.md`** — beads workflow integration.
- **`ARCHIVE/`** — predecessor designs (v0.1 + review). Historical only.
