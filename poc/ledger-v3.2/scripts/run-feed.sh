#!/usr/bin/env bash
# Run the ledger-feed consumer loop against poc_v3_2.
#
# Exactly one instance owns the ledger_feed slot (single-consumer posture; the
# slot has one cursor). The consumer creates the slot on startup if missing.
#
# Usage: bash poc/ledger-v3.2/scripts/run-feed.sh
# Env (all optional): FEED_DSN, FEED_SLOT, FEED_PUBLICATION, FEED_BATCH, FEED_POLL_MS

set -euo pipefail

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$WORKSPACE_DIR"
exec cargo run --release -p ledger-feed
