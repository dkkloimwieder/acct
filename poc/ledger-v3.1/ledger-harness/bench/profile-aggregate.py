#!/usr/bin/env python3
"""Aggregate committer_profile_sweep.csv into the per-scenario "where does
committer wall-time go" table (acct-0usf STEP 1 deliverable).

Reads the per-(scenario, rep) rows and reports, per scenario, the median and IQR
(p25..p75) of each metric across reps. Median+IQR (not mean) because the host is
a daily-driver workstation: one rep can catch a Chrome spike and skew a mean,
but the median is robust and the IQR width tells you how noisy that cell was.

The headline column is `lock_of_busy` — of non-idle committer time, the share
blocked on the cross-committer pool_lock handoff. That is the ONLY span
committer->pool affinity can shrink. A scenario with a small lock_of_busy is one
where affinity is a priori pointless (the committer is on-CPU, or LWLock-bound on
the shmem ring, or simply idle/upstream-bound). Those get recorded and SKIPPED in
the STEP 3 affinity variants.

Usage: profile-aggregate.py [path/to/committer_profile_sweep.csv]
"""
import csv
import sys
import statistics as st

CSV = sys.argv[1] if len(sys.argv) > 1 else "results/committer_profile_sweep.csv"

# Metric columns to summarize (numeric). Fractions are unitless 0..1.
METRICS = [
    "throughput_trx_s", "commit_group_avg",
    "span_pool_lock", "span_hydrate", "span_apply", "span_commit", "span_prep",
    "committer_busy_frac", "lock_of_busy", "running_of_busy", "lwlock_of_busy",
    "committer_samples", "load1_end",
]


def q(xs, frac):
    """Linear-interpolated quantile (no numpy dependency)."""
    if not xs:
        return float("nan")
    s = sorted(xs)
    if len(s) == 1:
        return s[0]
    pos = frac * (len(s) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (pos - lo)


def fnum(v):
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def main():
    rows = []
    with open(CSV) as f:
        for r in csv.DictReader(f):
            rows.append(r)
    if not rows:
        print(f"no data rows in {CSV}")
        return

    # Preserve first-seen scenario order.
    scenarios = []
    for r in rows:
        if r["scenario"] not in scenarios:
            scenarios.append(r["scenario"])

    # Per-scenario aggregation.
    agg = {}
    for sid in scenarios:
        srows = [r for r in rows if r["scenario"] == sid]
        agg[sid] = {"n": len(srows), "callers": srows[0].get("callers", "")}
        for m in METRICS:
            vals = [fnum(r.get(m)) for r in srows]
            vals = [v for v in vals if v is not None]
            agg[sid][m] = (q(vals, 0.5), q(vals, 0.25), q(vals, 0.75)) if vals else (float("nan"),) * 3

    # ── Headline table: the committer-time decomposition (medians) ──
    print("=" * 110)
    print("PER-SCENARIO COMMITTER WALL-TIME DECOMPOSITION (median across reps; affinity OFF, committer_count=4)")
    print("=" * 110)
    hdr = (f"{'scn':<5}{'cal':>5}{'n':>3}  {'tput':>7} {'cg':>6}  "
           f"{'busy%':>6} | of-busy: {'lock%':>6}{'run%':>6}{'lwlk%':>6}  | "
           f"span: {'lock%':>6}{'appl%':>6}{'cmit%':>6}{'prep%':>6}")
    print(hdr)
    print("-" * 110)
    for sid in scenarios:
        a = agg[sid]
        def md(m):  # median
            return a[m][0]
        print(f"{sid:<5}{a['callers']:>5}{a['n']:>3}  "
              f"{md('throughput_trx_s'):>7.0f} {md('commit_group_avg'):>6.1f}  "
              f"{100*md('committer_busy_frac'):>5.1f} | "
              f"         {100*md('lock_of_busy'):>5.1f}{100*md('running_of_busy'):>5.1f}{100*md('lwlock_of_busy'):>5.1f}  | "
              f"      {100*md('span_pool_lock'):>5.1f}{100*md('span_apply'):>5.1f}{100*md('span_commit'):>5.1f}{100*md('span_prep'):>5.1f}")

    # ── Noise / robustness: IQR width of the headline fraction ──
    print()
    print("NOISE CHECK — lock_of_busy IQR (p25..p75) per scenario; wide IQR = noisy cell, re-run or distrust")
    print("-" * 70)
    for sid in scenarios:
        a = agg[sid]
        med, p25, p75 = a["lock_of_busy"]
        load_med = a["load1_end"][0]
        flag = "  <-- WIDE" if (p75 - p25) > 0.10 else ""
        print(f"{sid:<5} lock_of_busy median={100*med:>5.1f}%  IQR=[{100*p25:>5.1f}..{100*p75:>5.1f}]  load1_med={load_med:>4.2f}{flag}")

    # ── Affinity-relevance verdict (pre-registered threshold) ──
    print()
    print("AFFINITY A-PRIORI RELEVANCE (lock_of_busy is the only span affinity can shrink)")
    print("  rule: busy<30% => upstream/idle-bound, affinity moot; lock_of_busy<25% of busy => not lock-bound, skip")
    print("-" * 70)
    for sid in scenarios:
        a = agg[sid]
        busy = a["committer_busy_frac"][0]
        lob = a["lock_of_busy"][0]
        if busy < 0.30:
            verdict = f"SKIP (committer only {100*busy:.0f}% busy — ceiling is upstream)"
        elif lob < 0.25:
            verdict = f"SKIP (lock only {100*lob:.0f}% of busy — not lock-bound)"
        else:
            verdict = f"CANDIDATE (lock {100*lob:.0f}% of busy, {100*busy:.0f}% busy)"
        print(f"{sid:<5} {verdict}")


if __name__ == "__main__":
    main()
