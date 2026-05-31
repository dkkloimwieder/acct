#!/usr/bin/env python3
"""Aggregate a hardened batch-size-sweep CSV (per-rep rows) into per-cell
median+IQR (acct-czz4 batch-diag STEP 1).

run-batch-size-sweep.sh now writes one row PER REP per (scenario, committers,
pack, batch_size_max) cell. This rolls the reps up: median + IQR (p25..p75) of
each numeric metric, plus the modal named top wait event across reps. Median+IQR
(not mean) because acct-postgres is a daily-driver workstation — one rep can
catch a host spike; the median is robust and the IQR width tells you how noisy
the cell was. A cell whose load1 IQR straddles the 1.5 gate, or whose throughput
IQR is wide, should be distrusted / re-run.

Cell key: (scenario, committers, pack, batch_size_max). Sorted by batch_size_max
within each (scenario, committers, pack) so the larger-is-better curve reads top
to bottom.

Usage: batch-aggregate.py <path/to/batchdiag_*.csv>
"""
import csv
import sys
from collections import Counter

# Numeric metrics to summarize (median + IQR across reps).
METRICS = [
    "throughput_trx_s", "cg_avg", "commits_s", "locks_per_trx",
    "ack_p50_us", "ack_p99_us",
    "busy_frac", "lock_of_busy", "running_of_busy", "lwlock_of_busy", "io_of_busy",
    "load1_end",
]


def q(xs, frac):
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
    if len(sys.argv) < 2:
        print("usage: batch-aggregate.py <batchdiag_*.csv>")
        return
    path = sys.argv[1]
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            rows.append(r)
    if not rows:
        print(f"no data rows in {path}")
        return

    # Group by cell, preserving first-seen order.
    def keyof(r):
        return (r.get("scenario", ""), r.get("committers", ""),
                r.get("pack", ""), r.get("batch_size_max", ""))

    order = []
    cells = {}
    for r in rows:
        k = keyof(r)
        if k not in cells:
            cells[k] = []
            order.append(k)
        cells[k].append(r)

    # Sort cells: (scenario, committers, pack) then numeric batch_size_max asc.
    def sortkey(k):
        scn, cc, pack, bsz = k
        try:
            bszn = int(bsz)
        except ValueError:
            bszn = 0
        return (scn, cc, pack, bszn)

    order.sort(key=sortkey)

    print("=" * 118)
    print("BATCH-SIZE SWEEP — per-cell median (IQR width) across reps  [acct-czz4]")
    print("=" * 118)
    hdr = (f"{'scn':<5}{'cc':>3}{'pack':>5}{'bsz':>5}{'n':>3}  "
           f"{'tput':>7}{'~iqr':>6}  {'cg':>6} {'cmt/s':>7} {'lk/trx':>7} "
           f"{'ackp50':>8} {'ackp99':>9}  "
           f"{'busy%':>6}{'lock%':>6}{'lwlk%':>6}  {'top_wait':>22} {'load1':>6}")
    print(hdr)
    print("-" * 118)

    for k in order:
        scn, cc, pack, bsz = k
        crows = cells[k]
        n = len(crows)

        def med(m):
            vals = [fnum(r.get(m)) for r in crows]
            vals = [v for v in vals if v is not None]
            return q(vals, 0.5) if vals else float("nan")

        def iqr(m):
            vals = [fnum(r.get(m)) for r in crows]
            vals = [v for v in vals if v is not None]
            if not vals:
                return float("nan")
            return q(vals, 0.75) - q(vals, 0.25)

        # Modal named top wait event across reps.
        waits = [(r.get("top_wait_type", "none"), r.get("top_wait_event", "none"))
                 for r in crows]
        waits = [w for w in waits if w != ("none", "none")]
        if waits:
            (tt, te), _ = Counter(waits).most_common(1)[0]
            topwait = f"{tt}/{te}"
        else:
            topwait = "none"

        print(f"{scn:<5}{cc:>3}{pack:>5}{bsz:>5}{n:>3}  "
              f"{med('throughput_trx_s'):>7.0f}{iqr('throughput_trx_s'):>6.0f}  "
              f"{med('cg_avg'):>6.1f} {med('commits_s'):>7.1f} {med('locks_per_trx'):>7.2f} "
              f"{med('ack_p50_us'):>8.0f} {med('ack_p99_us'):>9.0f}  "
              f"{100*med('busy_frac'):>5.1f}{100*med('lock_of_busy'):>5.1f}{100*med('lwlock_of_busy'):>5.1f}  "
              f"{topwait:>22} {med('load1_end'):>6.2f}")

    # Noise flags.
    print()
    print("NOISE CHECK — flag cells with wide throughput IQR or load1 over the 1.5 gate")
    print("-" * 80)
    for k in order:
        scn, cc, pack, bsz = k
        crows = cells[k]

        def med(m):
            vals = [fnum(r.get(m)) for r in crows]
            vals = [v for v in vals if v is not None]
            return q(vals, 0.5) if vals else float("nan")

        def iqr(m):
            vals = [fnum(r.get(m)) for r in crows]
            vals = [v for v in vals if v is not None]
            if not vals:
                return float("nan")
            return q(vals, 0.75) - q(vals, 0.25)

        tmed = med("throughput_trx_s")
        tiqr = iqr("throughput_trx_s")
        lmed = med("load1_end")
        rel = (tiqr / tmed) if tmed else float("inf")
        flags = []
        if rel > 0.10:
            flags.append(f"WIDE tput IQR ({100*rel:.0f}% of median)")
        if lmed >= 1.5:
            flags.append(f"load1 median {lmed:.2f} >= gate")
        if flags:
            print(f"{scn:<5} cc{cc} {pack} bsz{bsz}: " + "; ".join(flags))


if __name__ == "__main__":
    main()
