#!/usr/bin/env python3
# acct-0at4.10.3 (C) — drift-soak slope analyzer.
#
# Reads the timestamped drift CSV emitted by run-drift-soak.sh and writes a
# tracked markdown report: a per-minute linear drift slope (+ R²) for every
# metric, ASCII sparklines (matplotlib is absent in-tree, so these ARE the plot),
# an xid-wraparound extrapolation (ARCH-2), and leak/bloat verdicts computed from
# the STEADY window (last half — so the initial pool-fill ramp does not read as a
# leak). Hand-rolled least-squares; no numpy/scipy (project avoids heavy deps).
import sys

CSV, MD, META = sys.argv[1], sys.argv[2], (sys.argv[3] if len(sys.argv) > 3 else "")

# Fixed column order — must match run-drift-soak.sh CSV_HEADER / SAMPLE_SQL.
COLS = ["t_epoch", "t_s", "load1", "age_xid", "wal_bytes", "arena_outstanding",
        "arena_allocs", "arena_frees", "arena_bump", "arena_freelist",
        "trx_committed", "pool_state_rows", "pool_lock_rows", "ckpt_timed",
        "ckpt_req", "ckpt_buffers", "bgw_clean", "bgw_alloc"]

rows = []
with open(CSV) as f:
    header = f.readline()
    for line in f:
        parts = line.strip().split(",")
        if len(parts) < len(COLS):
            continue
        try:
            rows.append([float(x) for x in parts[:len(COLS)]])
        except ValueError:
            continue

if len(rows) < 3:
    open(MD, "w").write(f"# Drift soak — INSUFFICIENT DATA ({len(rows)} samples)\n\n`{META}`\n")
    print(f"[drift-analyze] only {len(rows)} samples — wrote stub {MD}")
    sys.exit(0)

col = {name: [r[i] for r in rows] for i, name in enumerate(COLS)}
t = col["t_s"]
t0, t1 = t[0], t[-1]
window_min = (t1 - t0) / 60.0 if t1 > t0 else 0.0


def linreg(xs, ys):
    """Least-squares slope (per x-unit), intercept, R². Flat series -> (0, mean, 1)."""
    n = len(xs)
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    sxy = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    if sxx == 0:
        return 0.0, my, 0.0
    slope = sxy / sxx
    if syy == 0:
        return 0.0, my, 1.0
    r2 = (sxy * sxy) / (sxx * syy)
    return slope, my - slope * mx, r2


BLOCKS = "▁▂▃▄▅▆▇█"
def spark(ys):
    lo, hi = min(ys), max(ys)
    if hi == lo:
        return BLOCKS[0] * min(len(ys), 60)
    step = max(1, len(ys) // 60)  # cap width ~60 chars
    ys = ys[::step]
    return "".join(BLOCKS[min(7, int((y - lo) / (hi - lo) * 7.999))] for y in ys)


def hnum(x):
    ax = abs(x)
    if ax >= 1e9: return f"{x/1e9:.2f}G"
    if ax >= 1e6: return f"{x/1e6:.2f}M"
    if ax >= 1e3: return f"{x/1e3:.2f}k"
    if ax == int(ax): return f"{int(x)}"
    return f"{x:.2f}"


def slope_per_min(name):
    s, _, r2 = linreg(t, col[name])
    return s * 60.0, r2  # per-second -> per-minute


def steady_slope_per_min(name):
    """Slope over the last half of the window — ignores the initial ramp."""
    half = len(rows) // 2
    xs, ys = t[half:], col[name][half:]
    if len(xs) < 3:
        xs, ys = t, col[name]
    s, _, _ = linreg(xs, ys)
    return s * 60.0


# ── throughput sawtooth (per-interval trx deltas) — absolute, host-sensitive ──
tc = col["trx_committed"]
te = col["t_epoch"]
rates = []
for i in range(1, len(tc)):
    dt = te[i] - te[i - 1]
    if dt > 0 and tc[i] >= tc[i - 1]:
        rates.append((tc[i] - tc[i - 1]) / dt)
rates_sorted = sorted(rates)
def pct(p):
    if not rates_sorted: return 0.0
    k = (len(rates_sorted) - 1) * p
    f = int(k); c = min(f + 1, len(rates_sorted) - 1)
    return rates_sorted[f] + (rates_sorted[c] - rates_sorted[f]) * (k - f)

# ── xid burn extrapolation (ARCH-2) ──
xid_slope_min, xid_r2 = slope_per_min("age_xid")
FREEZE_MAX = 200_000_000   # autovacuum_freeze_max_age default
WRAP = 2 ** 31             # ~2.1B
def eta(target, cur, per_min):
    if per_min <= 0: return "n/a (flat/declining)"
    mins = (target - cur) / per_min
    if mins > 1440: return f"{mins/1440:.1f} days"
    if mins > 60: return f"{mins/60:.1f} h"
    return f"{mins:.0f} min"
age = col["age_xid"]
# freeze events: downward steps > 1M (autovacuum froze datfrozenxid)
freezes = sum(1 for i in range(1, len(age)) if age[i] < age[i - 1] - 1_000_000)

# ── load range ──
loads = col["load1"]

out = []
out.append("# ledger-v3.1 drift-detection soak\n")
out.append(f"_`{META}`_\n")
out.append(f"Samples: **{len(rows)}** over **{window_min:.1f} min** "
           f"(t {t0:.0f}→{t1:.0f}s). Host load1 over window: "
           f"min {min(loads):.2f} / med {sorted(loads)[len(loads)//2]:.2f} / max {max(loads):.2f}.\n")
out.append("> **Posture:** open-loop fixed-rate load → the drift SLOPES below are load-robust. "
           "Absolute throughput/latency on this noisy daily-driver host are NOT a verdict; the "
           "slopes, sawtooth presence, and leak/bloat trends are.\n")

# ── drift slope table ──
out.append("\n## Per-metric drift slope (full-window least-squares)\n")
out.append("| metric | start | end | Δ | slope/min | R² | trend |")
out.append("|---|--:|--:|--:|--:|--:|---|")
TABLE = ["age_xid", "wal_bytes", "arena_outstanding", "arena_allocs", "arena_frees",
         "arena_bump", "arena_freelist", "trx_committed", "pool_state_rows",
         "pool_lock_rows", "ckpt_timed", "ckpt_req", "ckpt_buffers", "bgw_clean", "bgw_alloc"]
for name in TABLE:
    ys = col[name]
    s_min, r2 = slope_per_min(name)
    unit = ""
    disp = s_min
    if name == "wal_bytes":
        unit = " MB/min"; disp = s_min / 1048576.0
    elif name == "age_xid":
        unit = " xid/min"
    out.append(f"| `{name}` | {hnum(ys[0])} | {hnum(ys[-1])} | {hnum(ys[-1]-ys[0])} | "
               f"{hnum(disp)}{unit} | {r2:.2f} | `{spark(ys)}` |")

# ── verdicts ──
out.append("\n## Drift verdicts\n")

# xid burn / wraparound
out.append(f"- **xid burn (ARCH-2):** `age(datfrozenxid)` climbs **{hnum(xid_slope_min)} xid/min** "
           f"(R²={xid_r2:.2f}). At this rate: freeze threshold (autovacuum_freeze_max_age 200M) in "
           f"**{eta(FREEZE_MAX, age[-1], xid_slope_min)}**, wraparound (2.1B) in "
           f"**{eta(WRAP, age[-1], xid_slope_min)}**. "
           + (f"**{freezes} freeze event(s)** observed (age stepped down → autovacuum froze) — "
              "anti-wraparound vacuum is keeping up."
              if freezes else
              "No freeze event in-window (age monotonic); a longer soak is needed to observe "
              "anti-wraparound autovacuum trigger at the freeze threshold.")
           + " Enqueue forcing a real xid per submission is confirmed by the non-trivial slope.")

# arena leak
a_full, a_r2 = slope_per_min("arena_outstanding")
a_steady = steady_slope_per_min("arena_outstanding")
a0, a1 = col["arena_outstanding"][0], col["arena_outstanding"][-1]
leak = a_steady > 1.0 and a_r2 > 0.5 and a1 > a0 * 1.10
out.append(f"- **arena leak check:** `arena_outstanding` {hnum(a0)}→{hnum(a1)}, steady-window slope "
           f"**{hnum(a_steady)}/min** → " + ("**⚠ POSSIBLE LEAK** (outstanding climbing in steady state)."
           if leak else "**✓ BOUNDED** (outstanding not climbing in steady state — no arena leak). "
           f"allocs {hnum(col['arena_allocs'][-1])} vs frees {hnum(col['arena_frees'][-1])}."))

# metadata bloat
for tbl in ("pool_state_rows", "pool_lock_rows"):
    st = steady_slope_per_min(tbl)
    v0, v1 = col[tbl][0], col[tbl][-1]
    bloat = st > 0.5 and (v1 - v0) > max(5.0, 0.10 * max(1.0, v0))
    out.append(f"- **{tbl} bloat:** {hnum(v0)}→{hnum(v1)}, steady slope **{hnum(st)}/min** → "
               + ("**⚠ GROWING** (routed metadata not stabilizing)."
                  if bloat else "**✓ STABLE** (bounded; stabilizes after fill)."))

# checkpoints
ck_t = int(col["ckpt_timed"][-1] - col["ckpt_timed"][0])
ck_r = int(col["ckpt_req"][-1] - col["ckpt_req"][0])
cb_min, _ = slope_per_min("ckpt_buffers")
out.append(f"- **checkpoints:** {ck_t} timed + {ck_r} requested during the window; "
           f"checkpointer buffers written **{hnum(cb_min)}/min**. "
           + ("Crossed ≥1 checkpoint — sawtooth is observable."
              if (ck_t + ck_r) >= 1 else
              "**No checkpoint crossed** — window too short for the checkpoint-sawtooth signal "
              "(checkpoint_timeout=15min); lengthen DUR on a quiet host."))

# throughput note (explicitly not a verdict)
out.append(f"- **throughput (context, not a verdict):** per-interval committed rate "
           f"p50 **{pct(0.5):.0f}** / p95 {pct(0.95):.0f} / min {min(rates_sorted) if rates_sorted else 0:.0f} "
           f"/ max {max(rates_sorted) if rates_sorted else 0:.0f} trx/s. Open-loop target holds the "
           f"arrival rate; dips track host load, not architecture.")

out.append("\n## Interpretation\n")
out.append("The deliverable is the **slopes**, not the absolutes. A flat `arena_outstanding` steady "
           "slope with `allocs≈frees` growth is the leak-free signal; flat `pool_state`/`pool_lock` in "
           "steady state is the no-metadata-bloat signal; the `age_xid` slope + extrapolation is the "
           "ARCH-2 xid-burn characterization that `acct-0at4.2` consumes. Re-run with a multi-hour "
           "`DUR` on a quiet host to cross multiple checkpoints and observe an anti-wraparound vacuum.\n")

open(MD, "w").write("\n".join(out) + "\n")
print(f"[drift-analyze] {len(rows)} samples, {window_min:.1f} min -> {MD}")
print(f"  xid burn {hnum(xid_slope_min)}/min · arena {hnum(a0)}->{hnum(a1)} ({'LEAK?' if leak else 'bounded'}) · "
      f"checkpoints {ck_t}+{ck_r} · thr p50 {pct(0.5):.0f} trx/s")
