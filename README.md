# acct

ERP ledger and inventory system: SKU × location quantity tracking, per-routing-step WIP, document lifecycle (WO/SO/TO/PO), double-entry GL, multi-currency, reservations, commodity provisional pricing, period close.

Postgres-native v0.2 design — see `ledger_design_consolidated_v0.md`.

## Status

**Phase 0 in progress** (initial schema + dev environment). Tracked in beads under epic `acct-93b`.

## Quick start

```bash
./scripts/dev-up.sh        # build + start postgres 18 (io_uring), verify
./scripts/dev-down.sh      # stop (data preserved)

psql 'postgres://acct:acct_dev@localhost:5111/acct'
```

See `db/README.md` for the dev DB details (image, GUC overrides, seccomp note).

## Stack

Rust + `sqlx` + `sqlx-cli` + Postgres 18. Tests via `cargo test`. No pgTAP, no ORMs.

## Repository layout

| Path | Purpose |
|---|---|
| `ledger_design_consolidated_v0.md` | The design spec — single source of truth. Read first. |
| `ARCHIVE/` | Predecessor design docs (v0.1 + review). Historical, do not edit. |
| `db/` | Postgres dev environment + (eventually) migrations. |
| `scripts/` | Dev helpers (`dev-up.sh`, `dev-down.sh`). |
| `docker-compose.yml` | Dev DB service definition. |
| `CLAUDE.md` | Guidance for Claude Code sessions; load-bearing design decisions. |
| `AGENTS.md` | Beads issue-tracker integration block. |
| `.beads/` | Beads (bd) issue store, embedded Dolt. |

## Issue tracking

This project uses **`bd` (beads)**. `bd ready` for unblocked work, `bd show <id>` for detail, `bd update <id> --claim` to take one. See `AGENTS.md`.
