# Custom seccomp profiles

## `postgres-iouring.json`

Docker's default seccomp profile, copied verbatim from upstream moby
**v28.0.0** (`https://raw.githubusercontent.com/moby/moby/v28.0.0/profiles/seccomp/default.json`)
with exactly **three syscalls added** to the default-allow list:

- `io_uring_enter`
- `io_uring_register`
- `io_uring_setup`

Diff vs upstream is +3 lines, alphabetically inserted between `io_submit`
and `ipc` in the first `syscalls[].names` block (the unrestricted default-
allow set). To verify:

```bash
curl -sfL https://raw.githubusercontent.com/moby/moby/v28.0.0/profiles/seccomp/default.json \
  | diff - db/seccomp/postgres-iouring.json
```

Expected output:

```
190a191,193
> 				"io_uring_enter",
> 				"io_uring_register",
> 				"io_uring_setup",
```

## Why we need this

Docker's default seccomp profile pre-dates io_uring's wide adoption and
denies the `io_uring_*` syscalls. Postgres 18 with `io_method=io_uring`
fails to start under the default profile with:

```
could not setup io_uring queue: Operation not permitted
```

The choice in Phase 0 dev was to set `seccomp:unconfined` on the dev
container — adequate for a local rig but unacceptable for production
because it disables ALL of Docker's syscall restrictions, not just the
three we actually need.

This profile is the production answer: take the well-audited default,
add only what's strictly necessary, and lose no other restrictions.

## Wiring it in

Replace the `security_opt` block in your production compose file:

```yaml
security_opt:
  - seccomp:./db/seccomp/postgres-iouring.json
```

(The dev `docker-compose.yml` continues to use `seccomp:unconfined` for
parity with the existing developer workflow; switching dev too is fine
but isn't required, and would couple dev startup to the profile path
which has its own maintenance cost.)

## Verification

The profile was verified by temporarily swapping the dev container to
use it: postgres booted cleanly with `io_method=io_uring`, both
extensions (`pg_stat_statements`, `pg_cron`) loaded, the integration
smoke test (`tests/smoke.rs`) passed.

## Updating

If a newer moby release adds syscalls (e.g., `io_uring_setup2`), or
removes / reshuffles the default-allow set in ways that affect Postgres,
re-derive this file by:

1. Pulling the current upstream `default.json` from a tagged moby
   release.
2. Inserting the three (or however many) `io_uring_*` syscalls into the
   first `syscalls[].names` block, alphabetically.
3. Verifying with `jq -e 'type == "object"'` and
   `jq '.syscalls[0].names | map(select(startswith("io_uring")))'`.
4. Re-running the swap-and-test step against the dev container.
