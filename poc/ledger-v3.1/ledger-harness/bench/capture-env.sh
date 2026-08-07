#!/usr/bin/env bash
# acct-0at4.10.2 (B) — capture the measurement environment (FEEDBACK-TESTING #10).
#
# WHY THIS EXISTS
#   Every absolute number in POC-REPORT is only interpretable against the knobs
#   that produced it — shared_buffers, checkpoint cadence, WAL compression,
#   synchronous_commit, io_method, the routed committer GUCs, the fsync latency of
#   the WAL device, the CPU allocation, and which BGWorker pools are preloaded.
#   None of that was recorded anywhere. This tool snapshots the LIVE environment
#   into a tracked markdown baseline (results/env-baseline.md) plus a raw JSON
#   (results/env-<ts>.json), so a run's context travels with its results and so
#   confound-control (acct-0at4.10) has a fixed reference.
#
#   It reads LIVE pg_settings, NOT db/postgresql.conf: the base host-mounted conf
#   is overridden by postgresql.auto.conf in the data volume (e.g. the effective
#   shared_preload_libraries carries the runtime-installed extensions the base
#   file never lists), so only pg_settings reflects what actually ran.
#
# WHAT IT DOES NOT DO (surfaced, not applied — shared infra)
#   • CPU pinning: the acct-postgres container is unpinned (no cpuset/NanoCpus).
#     Pinning needs a docker-compose cpuset edit at the acct REPO ROOT, which is
#     shared across all PoC streams — this tool REPORTS the allocation and the
#     pinning recipe but does not change it.
#   • WAL device isolation: WAL shares the data volume mount; not isolated. Per
#     FEEDBACK #10's "isolate WAL onto its own device OR report its fsync latency",
#     this reports the fsync latency (pg_test_fsync).
#   • autovacuum-off-for-short-runs: reported as an operator recipe; not applied
#     (mutating the shared container is the caller's explicit choice).
#
# USAGE / ENV
#   bash bench/capture-env.sh                 # -> results/env-baseline.md + env-<ts>.json
#   FSYNC_SECS=1 bash bench/capture-env.sh    # faster/looser fsync probe
#   NO_FSYNC=1 bash bench/capture-env.sh      # skip pg_test_fsync (report proxy note)
#
#   CONTAINER(acct-postgres), DIRECT_DSN, RESULTS_DIR (from common.sh),
#   OUTMD(results/env-baseline.md), FSYNC_SECS(2), FSYNC_BIN, PGB_NAME.

set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

PGB_NAME="${PGBOUNCER_NAME:-poc-v3-1-pgbouncer}"
FSYNC_SECS="${FSYNC_SECS:-2}"
FSYNC_BIN="${FSYNC_BIN:-/usr/lib/postgresql/18/bin/pg_test_fsync}"
DBNAME="${DIRECT_DSN##*/}"
TS="$(date -u +%Y-%m-%dT%H-%M-%SZ)"
OUTMD="${OUTMD:-$RESULTS_DIR/env-baseline.md}"
OUTJSON="${OUTJSON:-$RESULTS_DIR/env-${TS}.json}"

command -v jq >/dev/null || { echo "FATAL: jq required" >&2; exit 2; }

# pg_settings subset that governs cross-run comparability. LIVE values.
SETTINGS=(
    server_version shared_buffers effective_cache_size work_mem maintenance_work_mem
    wal_buffers max_wal_size min_wal_size checkpoint_timeout checkpoint_completion_target
    wal_level wal_compression synchronous_commit fsync full_page_writes commit_delay
    commit_siblings io_method effective_io_concurrency io_combine_limit max_connections
    max_locks_per_transaction huge_pages jit backend_flush_after bgwriter_lru_maxpages
    bgwriter_delay track_io_timing autovacuum autovacuum_naptime
    autovacuum_vacuum_scale_factor shared_preload_libraries
)
# Live routed committer GUCs (design-v3.1 §11; keep in sync with common.sh).
ROUTED_GUCS=(
    ledger_routed_c.committer_count ledger_routed_c.affinity_scheme
    ledger_routed_c.batch_size_max ledger_routed_c.batch_window_us
    ledger_routed_c.router_pack_disjoint ledger_routed_c.wake_on_enqueue
)

psqlc() { docker exec "$CONTAINER" psql -U acct -d "$DBNAME" -tAc "$1" 2>/dev/null; }

echo "==> capturing environment @ $TS (host load: $(cat /proc/loadavg))"

# ---- pg_settings (JSON via row_to_json, ordered) --------------------------------
arr="$(printf "'%s'," "${SETTINGS[@]}")"; arr="${arr%,}"
pg_json="$(psqlc "SELECT COALESCE(json_agg(json_build_object('name',name,'setting',setting,'unit',unit,'source',source) ORDER BY name),'[]') FROM pg_settings WHERE name = ANY(ARRAY[$arr]);")"
[ -n "$pg_json" ] || { echo "FATAL: could not read pg_settings (is $CONTAINER up, $DBNAME present?)" >&2; exit 2; }

# ---- routed committer GUCs ------------------------------------------------------
routed_json="{}"
for g in "${ROUTED_GUCS[@]}"; do
    v="$(psqlc "SHOW $g")" || v="(unset)"
    routed_json="$(echo "$routed_json" | jq --arg k "$g" --arg v "${v:-unset}" '. + {($k): $v}')"
done

# ---- host + container -----------------------------------------------------------
host_nproc="$(nproc)"
host_load="$(cat /proc/loadavg)"
host_mem_kb="$(awk '/MemTotal/{print $2}' /proc/meminfo)"
host_kernel="$(uname -sr)"
cset="$(docker inspect -f '{{.HostConfig.CpusetCpus}}' "$CONTAINER" 2>/dev/null)"
nanocpu="$(docker inspect -f '{{.HostConfig.NanoCpus}}' "$CONTAINER" 2>/dev/null)"
memlock="$(docker inspect -f '{{range .HostConfig.Ulimits}}{{if eq .Name "memlock"}}{{.Soft}}/{{.Hard}}{{end}}{{end}}' "$CONTAINER" 2>/dev/null)"
pg_vol="$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/var/lib/postgresql"}}{{.Source}}{{end}}{{end}}' "$CONTAINER" 2>/dev/null)"

# ---- pgbouncer (1000-caller ingress path) ---------------------------------------
pgb_pool="(pgbouncer down)"; pgb_mode="-"
if docker ps --format '{{.Names}}' | grep -qx "$PGB_NAME"; then
    pgb_pool="$(docker exec "$PGB_NAME" sh -c 'grep -iE "^\s*default_pool_size" /etc/pgbouncer/pgbouncer.ini' 2>/dev/null | grep -oE '[0-9]+' | head -1)"
    pgb_mode="$(docker exec "$PGB_NAME" sh -c 'grep -iE "^\s*pool_mode" /etc/pgbouncer/pgbouncer.ini' 2>/dev/null | awk -F= '{gsub(/ /,"",$2);print $2}' | head -1)"
fi

# ---- fsync latency (WAL device; WAL is NOT on a separate volume) -----------------
# pg_test_fsync measures the real fsync cost of the volume the WAL sits on — the
# "report the device's fsync latency" half of FEEDBACK #10. Guarded: if the tool
# is absent or NO_FSYNC=1, fall back to reporting commit_delay + the shipped
# commit-span-ns fsync proxy (ledger-harness report.rs:98).
fsync_raw=""; fsync_note=""
if [ -n "${NO_FSYNC:-}" ]; then
    fsync_note="skipped (NO_FSYNC=1)"
elif docker exec "$CONTAINER" test -x "$FSYNC_BIN" 2>/dev/null; then
    echo "==> pg_test_fsync (-s $FSYNC_SECS/test; measures WAL-volume fsync)"
    fsync_raw="$(docker exec "$CONTAINER" sh -c "cd /tmp && timeout 120 '$FSYNC_BIN' -s $FSYNC_SECS -f /tmp/.pg_test_fsync.\$\$ 2>&1; rm -f /tmp/.pg_test_fsync.*" 2>/dev/null)"
    fsync_note="pg_test_fsync -s $FSYNC_SECS"
else
    fsync_note="pg_test_fsync NOT present; fsync latency proxy = commit-span-ns in run reports (report.rs:98), commit_delay knob captured above"
fi
# Headline fdatasync ops/sec (Linux default WAL sync method), if parseable.
fdatasync_ops="$(printf '%s\n' "$fsync_raw" | awk '/fdatasync/{for(i=1;i<=NF;i++) if($i ~ /^[0-9.]+$/){print $i; exit}}')"

# ---- raw JSON deliverable -------------------------------------------------------
mkdir -p "$RESULTS_DIR"
jq -n \
    --arg ts "$TS" --arg kernel "$host_kernel" --argjson nproc "$host_nproc" \
    --arg load "$host_load" --argjson mem_kb "${host_mem_kb:-0}" \
    --arg cpuset "${cset:-}" --arg nanocpu "${nanocpu:-0}" --arg memlock "${memlock:-}" \
    --arg pg_vol "${pg_vol:-}" --arg pgb_pool "${pgb_pool:-}" --arg pgb_mode "${pgb_mode:-}" \
    --arg fsync_note "$fsync_note" --arg fdatasync_ops "${fdatasync_ops:-}" \
    --argjson pg "$pg_json" --argjson routed "$routed_json" \
    '{measurement:"env_capture", ts:$ts,
      host:{kernel:$kernel, nproc:$nproc, loadavg:$load, mem_kb:$mem_kb},
      container:{cpuset:$cpuset, nanocpus:$nanocpu, memlock:$memlock, pg_volume:$pg_vol},
      pgbouncer:{default_pool_size:$pgb_pool, pool_mode:$pgb_mode},
      fsync:{method:$fsync_note, fdatasync_ops_per_sec:$fdatasync_ops},
      pg_settings:$pg, routed_gucs:$routed}' > "$OUTJSON"

# ---- markdown deliverable (tracked; regenerable) --------------------------------
{
    echo "# ledger-v3.1 measurement environment baseline"
    echo
    echo "_Captured \`$TS\` by \`bench/capture-env.sh\` (\`acct-0at4.10.2\`). LIVE \`pg_settings\`,"
    echo "not \`db/postgresql.conf\` — the base conf is overridden by \`postgresql.auto.conf\`._"
    echo
    echo "## Host & container"
    echo
    echo "| Item | Value |"
    echo "|---|---|"
    echo "| Kernel | \`$host_kernel\` |"
    echo "| CPUs (host) | $host_nproc |"
    echo "| RAM (host) | $(awk -v k="${host_mem_kb:-0}" 'BEGIN{printf "%.1f GiB", k/1048576}') |"
    echo "| Load @ capture | \`$host_load\` |"
    echo "| Container CPU pin | $([ -n "$cset" ] && echo "cpuset=\`$cset\`" || echo "**unpinned** (no cpuset)"), NanoCpus=\`${nanocpu:-0}\` |"
    echo "| Container memlock | \`${memlock:-default}\` |"
    echo "| PG data volume | \`${pg_vol:-?}\` (WAL shares this mount — **not** isolated) |"
    echo "| pgbouncer | pool_mode=\`${pgb_mode:-?}\`, default_pool_size=\`${pgb_pool:-?}\` |"
    echo
    echo "> **Noisy daily-driver host** (\`project_pocv3_bench_host_is_noisy_workstation\`): this is a"
    echo "> workstation, not an isolated bench box; background load (Chrome) swings absolute throughput"
    echo "> ~2×. Trust load-robust structural ratios; gate timed runs on a quiet host (\`common.sh"
    echo "> wait_for_quiet_host\`). The \`Load @ capture\` above stamps this snapshot's host state."
    echo
    echo "## fsync latency — WAL device ($fsync_note)"
    echo
    if [ -n "${fdatasync_ops:-}" ]; then
        echo "**fdatasync ≈ ${fdatasync_ops} ops/sec** (Linux default WAL sync). Full \`pg_test_fsync\`:"
    fi
    if [ -n "$fsync_raw" ]; then
        echo
        echo '```'
        printf '%s\n' "$fsync_raw"
        echo '```'
    else
        echo
        echo "_$fsync_note_"
    fi
    echo
    echo "## PostgreSQL settings (live)"
    echo
    echo "| Setting | Value | Unit |"
    echo "|---|---|---|"
    echo "$pg_json" | jq -r '.[] | "| `\(.name)` | \(.setting) | \(.unit // "") |"'
    echo
    echo "## Routed committer GUCs (live)"
    echo
    echo "| GUC | Value |"
    echo "|---|---|"
    echo "$routed_json" | jq -r 'to_entries[] | "| `\(.key)` | \(.value) |"'
    echo
    echo "## Operator knobs surfaced but NOT applied (shared infra)"
    echo
    echo "These change the **shared** acct-postgres container / acct-root compose, which every PoC"
    echo "stream uses. They are documented here as deliberate operator choices, not silently applied."
    echo
    echo "- **CPU pinning** — the container is unpinned, so the OS scheduler migrates PG backends"
    echo "  across all $host_nproc cores and co-schedules them with host load. To pin, add \`cpuset:\` (or"
    echo "  \`cpus:\`) to the acct-root \`docker-compose\` service and recreate — but recreating"
    echo "  acct-postgres wipes every runtime-installed \`.so\` and boot-blocks on the preloaded routed"
    echo "  extensions (\`recreating-the-shared-acct-postgres-container\`), so coordinate across streams."
    echo "- **autovacuum off for short cells** — \`autovacuum=on\` (naptime 30s) is correct for soak"
    echo "  runs; for short (<1 min) throughput cells where a vacuum cycle would add variance, an"
    echo "  operator may \`ALTER SYSTEM SET autovacuum=off; <restart>\`, run, then \`RESET\` + restart."
    echo "  Not applied by default — it mutates the shared container and hides wraparound pressure that"
    echo "  the \`acct-0at4.10.3\` drift soak specifically wants to observe."
    echo "- **WAL isolation** — WAL is on the data volume, not a separate device; the fsync latency"
    echo "  above is the reported alternative (FEEDBACK #10)."
} > "$OUTMD"

echo "==> markdown: $OUTMD"
echo "==> raw json: $OUTJSON"
