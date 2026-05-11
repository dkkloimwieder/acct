# batch-ledger PoC

**Scope**: acct-qdp5 epic, P1 (acct-c0ko) bootstraps this directory. Target: validate **10K transfers/sec** in a clean Postgres before backporting any batch-API design to acct's 70-migration production codebase.

This crate is **throwaway**. It exists to measure the ceiling honestly and to surface the design decisions a batch API forces (HC1–HC12, R1–R7 dissolution). When P8 (acct-qb0q) reaches its synthesis + decision, this directory either becomes ARCHIVE-style historical reference or is deleted.

**Do not** add features beyond what the current phase's exit criteria require. Iteration discipline matters here — every extra dimension confounds the measurement.

## Posture

- **Separate database**: `acct_poc` on the existing dev container (`localhost:5111`).
- **Separate crate**: not in any workspace; standalone `cargo build` / `cargo test`.
- **Separate migrations**: under `db/migrations/`, applied via `sqlx-cli`.
- **Independent of acct**: nothing here imports from acct's working tree, and nothing in acct/ should import from here.

## Setup

The dev container must already be up (`./scripts/dev-up.sh` from the repo root). Then:

```bash
cd poc/batch-ledger
./scripts/setup.sh          # creates acct_poc DB + applies migrations
cargo test                  # runs the P1 smoke tests
```

If the DB URL differs from the default (`postgres://acct:acct_dev@localhost:5111/acct_poc`), set `POC_DATABASE_URL`.

## Phases

| Phase | bd issue   | Status        | Goal                                                     |
|-------|------------|---------------|----------------------------------------------------------|
| P1    | acct-c0ko  | this commit   | Bootstrap subdir, schema, smoke test                      |
| P2    | acct-zdrm  | next          | Match pgledger's 10K TPS baseline on our hardware         |
| P3    | acct-k7c6  |               | `post_batch` for pure double-entry; the ceiling experiment |
| P4    | acct-4dg2  |               | + WAC perpetual with in-batch running balance map         |
| P5    | acct-1hps  |               | + FIFO with pre-pass layer slice allocation               |
| P6    | acct-ha7g  |               | + state machine + GRNI in apply phase                     |
| P7    | acct-yneu  |               | HC1–HC12 hard-cases catalog                               |
| P8    | acct-qb0q  |               | Synthesis + backport decision                             |

See `bd show acct-qdp5` for the full epic; `bd show <phase-id>` for per-phase scope.

## What's deliberately omitted in P1

- `posting_lines` append-only trigger (acct has one; pgledger does not). Re-introduced in P7 / HC11 to quantify the overhead.
- Cost dispatch (P4/P5 add it).
- Period close, recon, subledger tables (out of scope; not load-bearing for the throughput question).
- pg_cron jobs, audit log, RBAC.
- Tests against acct's `_sqlx_migrations` machinery — the PoC has its own.
