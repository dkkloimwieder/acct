#!/usr/bin/env bash
# Verify (and optionally apply) the cluster settings ledger-v3.2 depends on.
#
# These were applied by hand during phase 3 and a container rebuild silently
# drops them (acct-1vur.2d) — which is the worst failure mode available, since
# every one of them fails OPEN: the system keeps running and quietly stops
# being correct.
#
#   wal_level = logical         the feed cannot decode without it. Requires a
#                               RESTART; this script cannot apply it live.
#   max_slot_wal_keep_size      bounds WAL pinned by an abandoned slot. The
#                               deliberate trade is that the cluster may
#                               INVALIDATE the feed slot rather than fill the
#                               disk — which is why slot-loss detection and
#                               ledger_feed_reseed() exist.
#   timezone = 'UTC'            belt-and-braces beside 0024. Period boundary
#                               casts are pinned to UTC in SQL precisely so
#                               correctness does NOT depend on this, but a
#                               UTC cluster keeps operator-issued ad-hoc
#                               queries agreeing with the guards (acct-1vur.5).
#
# Usage:
#   bash poc/ledger-v3.2/scripts/check-cluster-prereqs.sh          # verify only
#   APPLY=1 bash poc/ledger-v3.2/scripts/check-cluster-prereqs.sh  # apply what it can
#
# Exit status is nonzero if any prerequisite is unmet after the run, so this
# composes into a startup gate.

set -uo pipefail

CONTAINER="${CONTAINER:-acct-postgres}"
SUPERUSER="${SUPERUSER:-acct}"
DB_NAME="${DB_NAME:-poc_v3_2}"
APPLY="${APPLY:-0}"

# The cluster is shared with other PoC streams, so this script never lowers a
# setting it finds already stricter than required.
WANT_KEEP_SIZE_MB=4096

q() { docker exec "$CONTAINER" psql -U "$SUPERUSER" -d "$DB_NAME" -tAc "$1"; }

fail=0
note() { printf '  %-26s %s\n' "$1" "$2"; }

echo "==> cluster prerequisites for ledger-v3.2 ($CONTAINER/$DB_NAME)"

# ── wal_level ──────────────────────────────────────────────────────────────
wal_level=$(q "SHOW wal_level")
if [ "$wal_level" = "logical" ]; then
    note "wal_level" "logical ✓"
else
    note "wal_level" "$wal_level ✗ — the feed cannot decode"
    echo "     fix: set wal_level=logical in postgresql.conf, then RESTART the container."
    echo "     (ALTER SYSTEM cannot take effect without a restart.)"
    fail=1
fi

# ── max_slot_wal_keep_size ─────────────────────────────────────────────────
keep=$(q "SELECT setting::bigint FROM pg_settings WHERE name = 'max_slot_wal_keep_size'")
if [ "$keep" = "-1" ]; then
    note "max_slot_wal_keep_size" "unlimited ✗ — an abandoned slot can fill the disk"
    if [ "$APPLY" = "1" ]; then
        q "ALTER SYSTEM SET max_slot_wal_keep_size = '${WANT_KEEP_SIZE_MB}MB'" >/dev/null
        q "SELECT pg_reload_conf()" >/dev/null
        note "" "applied ${WANT_KEEP_SIZE_MB}MB"
    else
        echo "     fix: APPLY=1 $0"
        fail=1
    fi
elif [ "$keep" -ge "$WANT_KEEP_SIZE_MB" ] 2>/dev/null; then
    note "max_slot_wal_keep_size" "${keep}MB ✓"
else
    note "max_slot_wal_keep_size" "${keep}MB ✓ (stricter than the ${WANT_KEEP_SIZE_MB}MB baseline)"
fi

# ── timezone ───────────────────────────────────────────────────────────────
tz=$(q "SHOW timezone")
if [ "$tz" = "UTC" ] || [ "$tz" = "GMT" ]; then
    note "timezone" "$tz ✓"
else
    note "timezone" "$tz — advisory only (0024 pins the boundary casts in SQL)"
    if [ "$APPLY" = "1" ]; then
        q "ALTER SYSTEM SET timezone = 'UTC'" >/dev/null
        q "SELECT pg_reload_conf()" >/dev/null
        note "" "applied UTC"
    fi
fi

# ── the feed slot itself ───────────────────────────────────────────────────
# Reported, never auto-created: creating a slot over an existing database
# skips every event decodable before that moment, so it must go through the
# consumer's ensure_slot_with_recovery (which reseeds) rather than a script.
read -r present unhealthy reason <<<"$(q "SELECT present, unhealthy, COALESCE(invalidation_reason, wal_status, 'n/a') FROM feed_slot_health" | tr '|' ' ')"
if [ "$present" = "t" ] && [ "$unhealthy" = "f" ]; then
    note "ledger_feed slot" "healthy ✓"
elif [ "$present" = "t" ]; then
    note "ledger_feed slot" "UNHEALTHY ($reason) ✗"
    echo "     fix: drop the slot, restart the feed (it reseeds), then let recalc drain."
    fail=1
else
    note "ledger_feed slot" "absent — the feed creates and reseeds it on startup"
fi

if [ "$fail" -ne 0 ]; then
    echo "==> prerequisites UNMET"
    exit 1
fi
echo "==> prerequisites OK"
