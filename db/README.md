# Database

Postgres 18 dev environment for the acct ledger project.

## Configuration

- **Image:** `acct-postgres:18`, built from `db/Dockerfile` (FROM `postgres:18`, adds `postgresql-18-cron`).
- **GUC overrides** (set via `command:` in `docker-compose.yml`):
  - `io_method=io_uring` — async I/O backend introduced in PG 18. The official `postgres:18` binary is built `--with-liburing`; verified with `ldd $(which postgres)`.
  - `shared_preload_libraries=pg_stat_statements,pg_cron`
  - `cron.database_name=acct`
- **Extensions** (created by `db/init/01-extensions.sql` on first boot):
  - `pg_stat_statements` — query-level perf observability.
  - `pg_cron` — scheduled jobs (used by reservation expiry and daily reconciliation, future Phase 0 work).
- **Database / user:** `acct` / `acct` (password `acct_dev`, dev only).
- **Port:** host `5111` → container `5432` (host port chosen to avoid clashing with other local Postgres containers).
- **Data volume:** named volume `acct-pgdata` mounted at `/var/lib/postgresql` (PG 18+ convention to enable `pg_upgrade --link`). Preserved across `dev-down.sh` unless `--wipe`.
- **seccomp:** `seccomp:unconfined` is set on the container because Docker's default seccomp profile blocks the `io_uring_setup` / `io_uring_enter` / `io_uring_register` syscalls. Acceptable for local dev. Production deploys must use a tightened custom profile (whitelist only the three io_uring syscalls). Tracked separately.

## Usage

```bash
./scripts/dev-up.sh        # build, start, verify io_method and extensions
./scripts/dev-down.sh      # stop (data preserved)
./scripts/dev-down.sh --wipe   # stop and delete data volume
```

Connect:

```
psql 'postgres://acct:acct_dev@localhost:5111/acct'
```

## Verification (run by `dev-up.sh`)

- `pg_isready` succeeds.
- `SHOW io_method` returns `io_uring`.
- `pg_extension` contains `pg_stat_statements` and `pg_cron`.

## Migrations

Plain `.sql` migrations live under `db/migrations/`, applied via `sqlx-cli`. Reversible (`.up.sql` / `.down.sql`), sequential numbering.

Install `sqlx-cli` once:

```bash
cargo install sqlx-cli --no-default-features --features rustls,postgres
```

Run migrations against the dev DB:

```bash
./scripts/run-migrations.sh
```

The script defaults `DATABASE_URL` to `postgres://acct:acct_dev@localhost:5111/acct` if unset. `.env.example` at the repo root documents the dev URL; copy to `.env` if you'd rather have sqlx-cli pick it up automatically.

Direct invocations:

```bash
sqlx migrate run    --source db/migrations
sqlx migrate info   --source db/migrations
sqlx migrate revert --source db/migrations
```

### Adding a migration

```bash
sqlx migrate add --source db/migrations -r --sequential <name>
```

This creates `NNNN_<name>.up.sql` and `NNNN_<name>.down.sql`. Both files are mandatory — `migrate revert` requires the down file.

### Verify (S2 acceptance)

Against a clean DB:

```bash
./scripts/run-migrations.sh                       # 0001 applied
sqlx migrate info   --source db/migrations        # shows installed
sqlx migrate revert --source db/migrations        # rolls back
./scripts/run-migrations.sh                       # redeploys cleanly
```

## Write path (`post_transfers`)

The canonical write entry-point is the PL/pgSQL function `post_transfers(p_events JSONB, p_override_closed_period BOOLEAN DEFAULT FALSE) RETURNS JSONB`. Every document write (PO receipt, SO ship, work-order op_move, scrap, AR/AP, etc.) calls this function with one or more events; it locks the involved accounts in ascending id order, validates each event, posts the resulting balance changes and `transfers` rows, and returns a per-event result array.

Phase 0 calls it **synchronously inside the same Postgres transaction** as document writes — no outbox (per D3, `acct-93b.3`).

### Event input shape

```json
{
  "reason":            "po_receipt",
  "document_kind":     "purchase_order",
  "document_id":       "<uuid>",
  "document_line_id":  "<uuid>",
  "debit_account_id":  12,
  "credit_account_id": 34,
  "amount":            100,
  "routing_op":        10,
  "counterparty_id":   "<uuid>",
  "business_date":     "2026-04-15",
  "idempotency_key":   "<uuid>",
  "posted_by":         "<uuid>"
}
```

`document_line_id`, `routing_op`, `counterparty_id` are optional and may be omitted. All other fields are required — the function does **not** validate field presence upfront; missing required fields surface as Postgres cast or NOT NULL errors.

In Phase 0 the caller pre-computes `amount`. W2 (acct-93b.15) adds a **gating** dispatch on `skus.cost_method` for cost-relevant reasons (`op_move`, `scrap`, `wo_complete`, `so_ship`): if the resolved sku's cost_method is anything other than `'standard'`, the function raises `P0006` (see error table below). Caller still computes `amount` themselves (`qty × skus.standard_cost`). The function-computed-amount path required for non-standard cost methods is tracked as `acct-0ig`, which `acct-8gg` (WAC/FIFO/lot) depends on.

### Per-event return

```json
{ "index": 1, "result": "ok" }     // applied
{ "index": 2, "result": "exists" } // idempotent duplicate, skipped
```

`index` is 1-based. Idempotent duplicates do **not** roll back the batch; they just skip and continue. Any other failure raises an exception with one of the codes below, which rolls back the **entire batch** (linked-batch semantics).

### Error codes

| SQLSTATE | Name | Condition |
|---|---|---|
| `P0001` | `account_closed` | debit or credit account `is_closed = TRUE` |
| `P0002` | `ledger_mismatch` | `debit.ledger_kind ≠ credit.ledger_kind` |
| `P0003` | `currency_mismatch` | both ledger_kind=`'value'`, currencies differ |
| `P0004` | `period_missing` | no period contains `business_date` |
| `P0005` | `period_closed` | period `closed_at IS NOT NULL` and `p_override_closed_period = FALSE` |
| `P0006` | `cost_method_not_implemented` | `reason ∈ {op_move, scrap, wo_complete, so_ship}` and resolved sku's `cost_method ≠ 'standard'` (or sku not resolvable from either account) |

The L3 append-only trigger (`P9999`) and the L2 balance-respects-normal-side CHECK (`23514`) are defenses in depth; neither should fire from inside `post_transfers` under normal use.

## Schema integrity check (`scripts/ci-check.sh`)

Pre-push guard for schema changes. There is no remote CI; this script is the check. Runs entirely against an ephemeral `acct_ci` DB inside the existing dev Postgres instance — the dev `acct` DB is untouched.

```bash
./scripts/ci-check.sh
```

Five steps:

1. `sqlx migrate run` on a freshly-created `acct_ci` — every migration applies cleanly.
2. `sqlx migrate info` — assert every migration shows installed (no `pending` lines).
3. `sqlx migrate revert` in a loop until empty — every migration reverts cleanly.
4. `sqlx migrate run` again — redeploys cleanly on the empty DB.
5. `pg_dump -s | sha256sum` — print the schema digest. Useful for reviewing what changed across branches; identical schemas produce identical digests.

The throwaway DB is dropped on exit via `trap`, even on failure.

## Tests

Cargo integration tests run against an **ephemeral `acct_test` database** inside the existing dev Postgres instance. The dev `acct` DB is untouched.

```bash
./scripts/run-tests.sh                    # full run
./scripts/run-tests.sh smoke              # filter to a single test binary
./scripts/run-tests.sh -- --test-threads=1  # serialize
```

The script `DROP`s `acct_test` if present, `CREATE`s it fresh, runs all migrations against it, seeds `db/fixtures/small/seed.sql`, runs `cargo test`, and drops `acct_test` again on exit (success or failure).

`pg_cron` does not get installed in `acct_test` — its install script enforces a hard `current_database() = cron.database_name` check that `cron.use_background_workers` does not relax. Migration 0001 tolerates this with a `DO/EXCEPTION` block; the test DB simply doesn't get pg_cron. O1 / O2 (which schedule cron jobs) run only against `acct`.

Test helpers live in `tests/common/mod.rs` (loaded via `mod common;` in each integration test file): `connect_test_db`, `reset_to_fixture`, `expect_sqlstate`, `call_post_transfers`. Set `TEST_DATABASE_URL` if you want to run `cargo test` outside of `scripts/run-tests.sh`.

### Load tests (opt-in)

Long-running load tests are gated behind `#[ignore]` so they don't run with the default `cargo test`. Triggered explicitly:

```bash
./scripts/run-tests.sh --test load_deadlock_freedom -- --ignored --nocapture
```

Available env knobs (defaults shown):

- `T4_DURATION_SECS=30` — wall-clock duration of the load phase.
- `T4_WRITERS=32` — number of concurrent tokio writers.

The current single test, `deadlock_freedom_under_concurrent_post_transfers`, posts random valid batches against `post_transfers` and asserts (a) `pg_stat_database.deadlocks` delta is 0, (b) every batch succeeded (no non-deadlock errors), (c) per-ledger double-entry holds at the end. The original Part IV §13 target is 100 writers × 30 minutes; defaults are smaller for tractable sanity runs. Spec-target run:

```bash
T4_DURATION_SECS=1800 T4_WRITERS=100 ./scripts/run-tests.sh --test load_deadlock_freedom -- --ignored --nocapture
```

(Bumping `T4_WRITERS` above the dev container's `max_connections` will fail; scaling work is tracked separately.)

## Scheduled jobs (pg_cron)

`reservation_expiry` runs every 30 seconds in the `acct` database (set via `cron.database_name` GUC) and flips `inventory_reservations` rows from `'active'` to `'expired'` when `expires_at` has passed. Migration `0015_cron_reservation_expiry` registers the job; the migration tolerates non-`acct` databases (acct_test, acct_ci) where pg_cron isn't installed.

Inspect the job:

```sql
SELECT jobid, schedule, command FROM cron.job WHERE jobname = 'reservation_expiry';
SELECT runid, status, return_message, start_time, end_time
  FROM cron.job_run_details
 WHERE jobid = (SELECT jobid FROM cron.job WHERE jobname = 'reservation_expiry')
 ORDER BY runid DESC LIMIT 5;
```

Smoke-check the end-to-end flow (inserts a test row, waits 35s, asserts expiry, cleans up):

```bash
./scripts/verify-o1-expiry.sh
```

## Reference data — Phase 0 scope

Phase 0 ships **stub** reference tables only (`skus`, `locations`, `sales_orders`, `purchase_orders`) — just enough columns to serve as FK targets for the ledger schema and to drive cost dispatch.

Internal document IDs use **`uuidv7()`** (PG 18 builtin) so PKs are time-ordered for B-tree locality. The `idempotency_key` on `transfers` stays random (`gen_random_uuid()` / UUIDv4) because it comes from clients.

**Out of scope until Phase 1:** `customers`, `suppliers`, `users`, `routings`, `boms`, `work_orders`, `facilities`. `sales_orders.customer_id` and `purchase_orders.supplier_id` are nullable UUID columns with no FK — the target tables don't exist yet.
