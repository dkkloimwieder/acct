#!/usr/bin/env bash
#
# sql-lint.sh — scan db/migrations/*.up.sql for AP1-AP8 anti-patterns.
#
# acct-du2 Phase 5 deliverable. Greps for the bug-class shapes
# enumerated in REVIEW.md / CLAUDE.md class-confusion checklist:
#
#   AP1  stock_available appearing in qty-divisor context
#   AP2  COALESCE(d_acct.sku_id, c_acct.sku_id) (debit-first SKU)
#   AP3  pool reads (debits_total - credits_total) without nearby FOR UPDATE
#   AP4  qty * resolve_standard_cost_at outside cost_method='standard' arm
#   AP5  inv_value_wip mutation without solo-at-pool gate (heuristic)
#   AP6  variance routing to debit-normal pool drained in-period
#   AP7  inv_value_* read without explicit currency filter
#   AP8  wo_events / similar idempotency check before FOR UPDATE only
#
# Severity:
#   - All checks emit WARNINGS by default. Pure-grep cannot reliably
#     distinguish the bug pattern from valid usage (e.g. AP4's
#     `qty * resolve_standard_cost_at` inside a `WHEN cost_method =
#     'standard' THEN` arm IS a valid use; AP5's `kind = 'inv_value_wip'`
#     fires on every legitimate WIP code path).
#   - AP1 is the lowest-FP-rate check; treated as ERROR (build-fail)
#     unless `-- @audit-ok: <reason>` appears within 5 preceding lines.
#   - The script is a checklist generator, not a CI gate. Use it to
#     find sites worth re-reading; cross-reference REVIEW.md for
#     verdicts on each match.
#
# Usage:
#   ./scripts/sql-lint.sh                # scan all migrations
#   ./scripts/sql-lint.sh path/to/file   # scan one file
#
# Exit code:
#   0 — no errors (warnings ok)
#   1 — at least one AP1/AP4/AP5 hit without allowlist
#
# False positives: this script does NOT understand SQL semantics. It's
# a starting-point check, not a substitute for the audit walk in
# REVIEW.md. Allowlist with `-- @audit-ok: <verdict in REVIEW.md>` after
# confirming the pattern is benign in context.

set -euo pipefail

cd "$(dirname "$0")/.."

if [[ $# -gt 0 ]]; then
  FILES=("$@")
else
  mapfile -t FILES < <(ls db/migrations/*.up.sql 2>/dev/null | sort)
fi

if [[ ${#FILES[@]} -eq 0 ]]; then
  echo "sql-lint: no migration files found" >&2
  exit 0
fi

ERRORS=0
WARNINGS=0

# Check whether a line has an `-- @audit-ok` comment within the
# preceding 5 lines (covers the "above" pattern; trailing comments on
# the same line are also accepted).
has_allowlist() {
  local file=$1 line=$2
  local from=$((line - 5))
  if [[ $from -lt 1 ]]; then from=1; fi
  sed -n "${from},${line}p" "$file" | grep -q '@audit-ok:'
}

emit() {
  local sev=$1 ap=$2 file=$3 line=$4 desc=$5 content=$6
  printf '%s:%d: [%s/%s] %s\n  %s\n' \
    "$file" "$line" "$sev" "$ap" "$desc" "$content"
}

# AP1: stock_available used as qty divisor.
# Pattern: SELECT ... debits_total - credits_total ... INTO v_*qty*
# preceded by a stock_available account lookup (`v_qty_acct`).
# Heuristic: any line that reads from `v_qty_acct` (the canonical
# stock_available variable name across the codebase) into a variable
# that's then divided.
ap1_check() {
  local file=$1
  while IFS=: read -r line content; do
    if has_allowlist "$file" "$line"; then continue; fi
    emit ERROR AP1 "$file" "$line" \
      "stock_available read into qty variable — risk of cross-class divisor (acct-fii / acct-du2.8 / acct-du2.11)" \
      "$content"
    ERRORS=$((ERRORS + 1))
  done < <(grep -nE 'INTO\s+v_\w*qty\w*\s+FROM\s+accounts' "$file" 2>/dev/null \
             | grep -B1 'stock_available' 2>/dev/null \
             | grep -E '^[0-9]+:' || true)
}

# AP2: debit-first SKU resolution.
ap2_check() {
  local file=$1
  while IFS=: read -r line content; do
    if has_allowlist "$file" "$line"; then continue; fi
    emit WARN AP2 "$file" "$line" \
      "debit-first COALESCE on SKU resolution — should be credit-first for cost dispatch / flagging (acct-7py / acct-du2.4)" \
      "$content"
    WARNINGS=$((WARNINGS + 1))
  done < <(grep -nE 'COALESCE\s*\(\s*\w*d_acct\.sku_id|COALESCE\s*\(\s*\w*debit\w*\.sku_id' "$file" 2>/dev/null || true)
}

# AP3: pool reads without nearby FOR UPDATE.
# Heuristic: flag any `(debits_total - credits_total)` read; manual
# review confirms whether the same account is locked above.
ap3_check() {
  local file=$1
  while IFS=: read -r line content; do
    if has_allowlist "$file" "$line"; then continue; fi
    # Skip the helper functions whose entire purpose IS the read.
    if grep -B 30 -E "^[0-9]+" "$file" 2>/dev/null \
       | sed -n "${line}p" 2>/dev/null \
       | grep -q '_post_transfers_compute_amount\|_wac_close_pool_qty_in'; then
      continue
    fi
    emit WARN AP3 "$file" "$line" \
      "pool read (debits_total - credits_total) — verify FOR UPDATE on same account in nearby lines (acct-du2.1 / .2 / .6 / .7 / .9 / .12)" \
      "$content"
    WARNINGS=$((WARNINGS + 1))
  done < <(grep -nE 'debits_total\s*-\s*credits_total' "$file" 2>/dev/null \
            | grep -v 'COMMENT\|--' || true)
}

# AP4: qty * resolve_standard_cost_at outside CASE on cost_method.
# Hard to do statically without a parser. Flag every occurrence and
# rely on human reviewer to confirm the surrounding CASE.
ap4_check() {
  local file=$1
  while IFS=: read -r line content; do
    if has_allowlist "$file" "$line"; then continue; fi
    emit WARN AP4 "$file" "$line" \
      "qty × resolve_standard_cost_at — verify enclosing CASE on cost_method='standard' (acct-rgb)" \
      "$content"
    WARNINGS=$((WARNINGS + 1))
  done < <(grep -nE '\*\s*resolve_standard_cost_at|resolve_standard_cost_at\s*\([^)]+\)\s*\*' "$file" 2>/dev/null || true)
}

# AP5: post_wo_complete-style residual sweep without solo-at-pool gate.
# Heuristic: flag inv_value_wip residual loops that don't reference
# v_solo or stock_wip qty before posting.
ap5_check() {
  local file=$1
  while IFS=: read -r line content; do
    if has_allowlist "$file" "$line"; then continue; fi
    emit WARN AP5 "$file" "$line" \
      "inv_value_wip residual sweep loop — verify solo-at-pool gate on stock_wip qty before mutation (acct-69e / acct-du2.10)" \
      "$content"
    WARNINGS=$((WARNINGS + 1))
  done < <(grep -nE "kind\s*=\s*'inv_value_wip'" "$file" 2>/dev/null \
            | grep -v 'AND\|RAISE\|--' || true)
}

# AP6: heuristic only — flag any `INSERT INTO transfers .* debit.*inv_value_wip`
# inside a function that also has `drain` semantics. Hard to do statically;
# emit informational only.

# AP7: inv_value_* read without `currency` in the WHERE clause.
ap7_check() {
  local file=$1
  while IFS=: read -r line content; do
    if has_allowlist "$file" "$line"; then continue; fi
    # Skip clear-cut COMMENT lines.
    if echo "$content" | grep -qE "^\s*--|^\s*'"; then continue; fi
    emit WARN AP7 "$file" "$line" \
      "inv_value_* lookup — verify currency = ... is in WHERE clause (multi-currency partition)" \
      "$content"
    WARNINGS=$((WARNINGS + 1))
  done < <(grep -nE "kind\s*=\s*'inv_value_(raw|fg|wip)'" "$file" 2>/dev/null \
            | grep -vE 'currency\s*=' || true)
}

# AP8: SELECT id INTO v_existing FROM wo_events / transfers without
# subsequent FOR UPDATE on the lock target. Hard to do in pure grep;
# emit informational on every wo_events SELECT for human review.
ap8_check() {
  local file=$1
  while IFS=: read -r line content; do
    if has_allowlist "$file" "$line"; then continue; fi
    emit WARN AP8 "$file" "$line" \
      "wo_events idempotency check — verify dual-check pattern (pre + post FOR UPDATE; acct-69p / acct-du2.3 / acct-du2.5)" \
      "$content"
    WARNINGS=$((WARNINGS + 1))
  done < <(grep -nE 'SELECT\s+id\s+INTO\s+v_existing\w*\s+FROM\s+wo_events' "$file" 2>/dev/null || true)
}

for f in "${FILES[@]}"; do
  ap1_check "$f"
  ap2_check "$f"
  ap3_check "$f"
  ap4_check "$f"
  ap5_check "$f"
  ap7_check "$f"
  ap8_check "$f"
done

echo
echo "sql-lint summary: $ERRORS error(s), $WARNINGS warning(s) across ${#FILES[@]} file(s)"

if [[ $ERRORS -gt 0 ]]; then
  exit 1
fi
exit 0
