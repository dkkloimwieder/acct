#!/usr/bin/env bash
# Run the ledger-feed consumer loop against poc_v3_2.
#
# Exactly one instance owns the ledger_feed slot (single-consumer posture; the
# slot has one cursor). The consumer creates the slot on startup if missing.
#
# ORDERING: START THE FEED BEFORE LOADING.
#
# A logical slot only decodes WAL written AFTER it exists. Creating the slot
# puts its cursor at the current WAL position, so events committed before the
# feed first runs are never delivered: their pools are never marked dirty and
# never re-folded, which is a silent mis-valuation rather than a loud one.
#
# Starting late is recoverable but not free — the consumer detects a slot
# created over a database that already holds events and calls
# ledger_feed_reseed(), which enqueues every fifo/lifo pool for a FULL opening
# re-fold. Correct, and much more expensive than having been running.
#
# The same routine is the recovery path for a slot the cluster INVALIDATED
# (max_slot_wal_keep_size is deliberately finite, so this is a real state, not
# a theoretical one). To recover by hand: drop the slot, restart this script,
# let the recalc workers drain. Check state with
#   SELECT * FROM feed_slot_health;
# and verify the cluster settings with scripts/check-cluster-prereqs.sh.
#
# Usage: bash poc/ledger-v3.2/scripts/run-feed.sh
# Env (all optional): FEED_DSN, FEED_BATCH, FEED_POLL_MS
# The slot and publication names are fixed literals — see src/main.rs.

set -euo pipefail

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$WORKSPACE_DIR"
exec cargo run --release -p ledger-feed
