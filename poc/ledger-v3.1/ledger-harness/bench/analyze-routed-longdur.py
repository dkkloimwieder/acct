#!/usr/bin/env python3
"""Analyze the long-duration routed sweep (acct-0516) and emit POC-REPORT §(g).

Reads, per scenario S1-S21:
  - results/longdur_<sid>_<short>s.json  (1m pretty report, report.rs schema)
  - results/longdur_<sid>_<long>s.json   (10m pretty report)
  - the compact stdout JSON lines from the sweep log (longdur_full.log /
    longdur_sweep captures), which carry trx_materialized + submitted_but_unseen
    — fields the pretty --output JSON does NOT include but AC2 requires.

Emits two markdown blocks to stdout (AC4):
  1. "Long-duration routed cleanliness sweep" — one row per scenario, the AC2
     metrics with 1m | 10m columns.
  2. "Long-duration discoveries" — any cell flagged structural per AC3:
       (a) a counter zero at 1m but non-zero at 10m (EXCEPT P0006 exhaustion,
           which AC3 says to record but NOT flag);
       (b) order-of-magnitude shift in commit_group_size_avg OR
           pool_lock_acquisitions/trx_materialized ratio;
       (c) ack p99 grows superlinearly with duration (10m p99 > 1m p99 *
           (long/short) — i.e. faster than linear in the duration ratio);
       (d) drains_total/duration_s ratio shifts > 50%;
       (e) submitted_but_unseen > 0 at end-of-drain (either column).

Usage:
  analyze-routed-longdur.py [--results DIR] [--log FILE] [--short 60] [--long 600]
"""
import argparse
import glob
import json
import os
import re
import sys

SCENARIOS = [f"s{i}" for i in range(1, 22)]


def load_pretty(results_dir, sid, dur):
    path = os.path.join(results_dir, f"longdur_{sid}_{dur}s.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path) as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError) as e:
        sys.stderr.write(f"warn: cannot parse {path}: {e}\n")
        return None


def load_compact(log_text):
    """Map (scenario, output-basename) -> compact stdout dict.

    The harness prints one compact JSON object per run to stdout; its "output"
    field is the pretty file path, which we key on to disambiguate 1m vs 10m.
    """
    out = {}
    for line in log_text.splitlines():
        line = line.strip()
        if not (line.startswith("{") and '"trx_materialized"' in line):
            continue
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            continue
        opath = d.get("output", "")
        if opath:
            out[os.path.basename(opath)] = d
    return out


def err_text(results_dir, sid, dur):
    path = os.path.join(results_dir, f"longdur_{sid}_{dur}s.err")
    if os.path.exists(path) and os.path.getsize(path) > 0:
        try:
            with open(path) as fh:
                return fh.read()
        except OSError:
            return "(err file unreadable)"
    return ""


def is_p0006(text):
    return bool(text) and ("P0006" in text or "InsufficientInventory" in text)


def cell_metrics(pretty, compact):
    """Merge a pretty report + compact stdout line into the AC2 metric set."""
    m = {}
    r = (pretty or {}).get("routed") or {}
    ack = (pretty or {}).get("ack_latency_us") or {}
    com = (pretty or {}).get("committed_latency_us") or {}
    c = compact or {}

    m["throughput"] = (pretty or {}).get("throughput_trx_per_sec",
                                         c.get("throughput_trx_per_sec", 0.0))
    m["duration_s"] = (pretty or {}).get("duration_secs", 0.0)
    m["attempts"] = (pretty or {}).get("attempts_total", c.get("attempts", 0))
    m["errors"] = (pretty or {}).get("errors_total", c.get("enqueue_errors", 0))
    # trx_materialized + submitted_but_unseen: compact-only (AC2-required).
    m["trx_materialized"] = c.get("trx_materialized", r.get("trx_committed_total", 0))
    m["submitted_but_unseen"] = c.get("submitted_but_unseen", 0)
    m["drains"] = r.get("drains_total", c.get("drains", 0))
    m["commit_group_avg"] = r.get("commit_group_size_avg", c.get("commit_group_avg", 0.0))
    m["pool_lock_acq"] = r.get("pool_lock_acquisitions_total", c.get("pool_lock_acq", 0))
    m["agg_upserts"] = r.get("aggregate_upserts_total", c.get("agg_upserts", 0))
    m["dropped"] = r.get("dropped_submissions_total", c.get("dropped", 0))
    m["poisoned"] = r.get("poisoned_total", c.get("poisoned", 0))
    m["deadlock_retries"] = r.get("deadlock_retries_total", c.get("deadlock_retries", 0))
    m["takeovers"] = r.get("takeover_count", c.get("takeovers", 0))
    m["ack_p50"] = ack.get("p50", 0)
    m["ack_p95"] = ack.get("p95", 0)
    m["ack_p99"] = ack.get("p99", c.get("ack_p99_us", 0))
    m["com_p50"] = com.get("p50", 0)
    m["com_p95"] = com.get("p95", 0)
    m["com_p99"] = com.get("p99", c.get("committed_p99_us", 0))
    tw = (pretty or {}).get("top_wait_events") or []
    m["top_wait"] = ", ".join(f'{e.get("wait_event","?")}({e.get("samples",0)})'
                              for e in tw[:3]) or "—"
    return m


def ratio(a, b):
    return (a / b) if b else 0.0


def classify(sid, short_m, long_m, short_err, long_err, short_dur, long_dur):
    """Return (list_of_structural_findings, list_of_expected_notes)."""
    findings, notes = [], []
    dur_ratio = ratio(long_dur, short_dur) or 1.0

    # P0006 exhaustion is EXPECTED (AC3), not a structural diff.
    if is_p0006(long_err) or is_p0006(short_err):
        notes.append("P0006 pool exhaustion (expected; seed-depth calibration)")

    # (a) counter zero at 1m, non-zero at 10m — excl. exhaustion-driven errors.
    for key, label in [("drained_counters", None)]:
        pass
    for key in ("dropped", "poisoned", "deadlock_retries", "takeovers"):
        if short_m.get(key, 0) == 0 and long_m.get(key, 0) > 0:
            findings.append(f"(a) {key}: 0 → {long_m[key]} (new non-zero at 10m)")
    # errors newly appearing at 10m that are NOT P0006 exhaustion.
    if short_m.get("errors", 0) == 0 and long_m.get("errors", 0) > 0 and not is_p0006(long_err):
        findings.append(f"(a) errors: 0 → {long_m['errors']} at 10m (non-exhaustion)")

    # (b) order-of-magnitude shift in commit_group_avg.
    cga_s, cga_l = short_m.get("commit_group_avg", 0), long_m.get("commit_group_avg", 0)
    if cga_s > 0 and cga_l > 0 and (cga_l / cga_s >= 10 or cga_s / cga_l >= 10):
        findings.append(f"(b) commit_group_avg {cga_s:.2f} → {cga_l:.2f} (≥10×)")
    # (b) pool_lock_acq / trx_materialized ratio shift by ≥10×.
    plr_s = ratio(short_m.get("pool_lock_acq", 0), short_m.get("trx_materialized", 0))
    plr_l = ratio(long_m.get("pool_lock_acq", 0), long_m.get("trx_materialized", 0))
    if plr_s > 0 and plr_l > 0 and (plr_l / plr_s >= 10 or plr_s / plr_l >= 10):
        findings.append(f"(b) pool_lock/trx ratio {plr_s:.3f} → {plr_l:.3f} (≥10×)")

    # (c) ack p99 superlinear: 10m p99 grows faster than the duration ratio.
    a99_s, a99_l = short_m.get("ack_p99", 0), long_m.get("ack_p99", 0)
    if a99_s > 0 and a99_l > a99_s * dur_ratio:
        findings.append(f"(c) ack p99 {a99_s}→{a99_l}µs > {dur_ratio:.0f}× (superlinear)")

    # (d) drains/s ratio shifts > 50%.
    dps_s = ratio(short_m.get("drains", 0), short_dur)
    dps_l = ratio(long_m.get("drains", 0), long_dur)
    if dps_s > 0 and abs(dps_l - dps_s) / dps_s > 0.50:
        findings.append(f"(d) drains/s {dps_s:.0f} → {dps_l:.0f} (>50% shift)")

    # (e) submitted_but_unseen > 0 at end-of-drain.
    for col, m in (("1m", short_m), ("10m", long_m)):
        if m.get("submitted_but_unseen", 0) > 0:
            findings.append(f"(e) submitted_but_unseen={m['submitted_but_unseen']} at {col} end-of-drain")

    return findings, notes


def fmt_int(v):
    try:
        return f"{int(v):,}"
    except (TypeError, ValueError):
        return str(v)


def main():
    ap = argparse.ArgumentParser()
    here = os.path.dirname(os.path.abspath(__file__))
    default_results = os.path.normpath(os.path.join(here, "..", "..", "results"))
    ap.add_argument("--results", default=default_results)
    ap.add_argument("--log", default="/tmp/longdur_full.log")
    ap.add_argument("--short", type=int, default=60)
    ap.add_argument("--long", type=int, default=600)
    args = ap.parse_args()

    log_text = ""
    for cand in (args.log, os.path.join(args.results, "longdur_full.log")):
        if cand and os.path.exists(cand):
            with open(cand) as fh:
                log_text += fh.read() + "\n"
    compact = load_compact(log_text)

    rows, all_findings = [], []
    for sid in SCENARIOS:
        ps = load_pretty(args.results, sid, args.short)
        pl = load_pretty(args.results, sid, args.long)
        cs = compact.get(f"longdur_{sid}_{args.short}s.json")
        cl = compact.get(f"longdur_{sid}_{args.long}s.json")
        if ps is None and pl is None and cs is None and cl is None:
            rows.append((sid, None, None, "MISSING"))
            continue
        sm = cell_metrics(ps, cs)
        lm = cell_metrics(pl, cl)
        se = err_text(args.results, sid, args.short)
        le = err_text(args.results, sid, args.long)
        sdur = sm.get("duration_s") or args.short
        ldur = lm.get("duration_s") or args.long
        findings, notes = classify(sid, sm, lm, se, le, sdur, ldur)
        rows.append((sid, sm, lm, notes))
        if findings:
            all_findings.append((sid, findings))

    # ---- Block 1: the combined per-scenario table (1m | 10m per metric) ----
    print("### Long-duration routed cleanliness sweep\n")
    print(f"Routed-only, two durations per scenario ({args.short}s | {args.long}s), "
          "clean DB (a2 DROP/CREATE + restart) once per scenario before the "
          f"{args.short}s; the {args.long}s extends the same universe.\n")
    cols = ["throughput", "trx_materialized", "submitted_but_unseen", "errors",
            "drains", "commit_group_avg", "pool_lock_acq", "agg_upserts",
            "dropped", "poisoned", "deadlock_retries", "takeovers",
            "ack_p99", "com_p99"]
    hdr = "| scenario | dur | " + " | ".join(cols) + " | top_wait |"
    sep = "|" + "---|" * (len(cols) + 3)
    print(hdr)
    print(sep)
    for sid, sm, lm, notes in rows:
        if sm is None:
            print(f"| {sid} | — | " + " | ".join(["—"] * len(cols)) + " | (missing) |")
            continue
        for label, m in (("1m", sm), ("10m", lm)):
            cells = []
            for c in cols:
                v = m.get(c, 0)
                if c == "throughput":
                    cells.append(f"{v:,.0f}")
                elif c == "commit_group_avg":
                    cells.append(f"{v:.2f}")
                else:
                    cells.append(fmt_int(v))
            note = f" _{'; '.join(notes)}_" if (label == "10m" and notes) else ""
            print(f"| {sid} | {label} | " + " | ".join(cells) + f" | {m.get('top_wait','—')}{note} |")
    print()

    # ---- Block 2: discoveries (AC3) ----
    print("### Long-duration discoveries\n")
    if not all_findings:
        print("No scenario showed a structural 1m→10m difference under the AC3 "
              "criteria (a)–(e). Pool exhaustion (P0006), where it occurred, is "
              "recorded in the table notes as expected seed-depth calibration, "
              "not a structural difference.\n")
    else:
        for sid, findings in all_findings:
            print(f"- **{sid}**:")
            for f in findings:
                print(f"  - {f}")
        print()


if __name__ == "__main__":
    main()
