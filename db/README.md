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

Migrations will be added under `db/migrations/` and run via `sqlx migrate run` (sqlx-cli). See acct-93b.6 (S2) for the migration scaffold.
