//! Shared helpers for the `run` drivers (direct per-call, direct batched, routed).

use chrono::{DateTime, Utc};
use serde_json::{json, Value};

use crate::report::MultiTouchReport;
use crate::scenarios::ScenarioSpec;
use crate::workload::{LineParam, OverlapMode, TouchDistribution, Workload};

/// Per-run, per-caller source_id namespace base. `run_prefix + caller_id*1e6 +
/// tick` keeps the trx UNIQUE (trx_type, source_id) constraint from colliding
/// across runs against the same persisted DB (~10^6 ticks/caller, ~10^3
/// callers/run; wraparound ~11.5 days — fine for measurement cadence).
pub fn run_prefix(started_at: DateTime<Utc>) -> i64 {
    (started_at.timestamp() % 1_000_000) * 1_000_000_000_000
}

/// Apply the optional `--max-callers` cap to a resolved scenario, logging when
/// it bites. Capping changes the measured concurrency; prefer a pooler DSN.
pub fn cap_callers(spec: &mut ScenarioSpec, max_callers: Option<usize>) {
    if let Some(cap) = max_callers {
        let capped = spec.callers.min(cap);
        if capped != spec.callers {
            eprintln!(
                "scenario {}: capping callers {} -> {} via --max-callers (concurrency reduced)",
                spec.id, spec.callers, capped
            );
            spec.callers = capped;
            spec.workload.caller_count = capped;
        }
    }
}

/// Overlay the optional `run --multi-touch-pct` / `--touch-dist` knobs onto a
/// resolved scenario's workload (acct-34ce). Either overrides the scenario's
/// own value (the touch distribution is pre-parsed by the caller); logging when
/// an override changes the scenario's setting. With both unset the workload is
/// untouched — the scenario's default (distinct-pool for s1-s8, the preset mix
/// for s9) stands.
pub fn apply_multi_touch(
    spec: &mut ScenarioSpec,
    multi_touch_pct: Option<u8>,
    touch_dist: Option<TouchDistribution>,
) {
    if let Some(pct) = multi_touch_pct {
        if pct != spec.workload.multi_touch_pct {
            eprintln!(
                "scenario {}: multi_touch_pct {} -> {} via --multi-touch-pct",
                spec.id, spec.workload.multi_touch_pct, pct
            );
        }
        spec.workload.multi_touch_pct = pct;
    }
    if let Some(dist) = touch_dist {
        eprintln!("scenario {}: touch_dist -> {} via --touch-dist", spec.id, dist.spec_string());
        spec.workload.touch_dist = dist;
    }
}

/// Build the report's multi-touch block from a resolved workload — Some only
/// when multi-touch is active, so distinct-pool runs record `multi_touch: null`.
pub fn multi_touch_report(w: &Workload) -> Option<MultiTouchReport> {
    (w.multi_touch_pct > 0).then(|| MultiTouchReport {
        pct: w.multi_touch_pct,
        touch_dist: w.touch_dist.spec_string(),
    })
}

/// Overlay the optional `run --pareto-hot-pool-pct` / `--pareto-hot-traffic-pct`
/// knobs onto a resolved scenario's workload (acct-s90k). Either knob being set
/// switches the workload's overlap to `Pareto`; the other defaults to the
/// textbook 80/20 complement (hot pool 20%, hot traffic 80%). Logs the
/// substitution so bench logs make the overlay legible. With both unset the
/// workload is untouched — the scenario's native overlap stands.
pub fn apply_pareto(
    spec: &mut ScenarioSpec,
    hot_pool_pct: Option<u8>,
    hot_traffic_pct: Option<u8>,
) {
    if hot_pool_pct.is_none() && hot_traffic_pct.is_none() {
        return;
    }
    let hp = hot_pool_pct.unwrap_or(20).clamp(0, 100);
    let ht = hot_traffic_pct.unwrap_or(80).clamp(0, 100);
    eprintln!(
        "scenario {}: overlap {:?} -> Pareto hot_pool={}% hot_traffic={}% via --pareto-*",
        spec.id, spec.workload.overlap, hp, ht
    );
    spec.workload.overlap = OverlapMode::Pareto {
        hot_pool_fraction: hp as f64 / 100.0,
        hot_traffic_fraction: ht as f64 / 100.0,
    };
}

/// JSONB array shape expected by `ledger_submit_trx_c` / `ledger_enqueue_trx_c`.
/// Posting accounts are resolved ledger-side from posting_account_map (§3.7), so
/// the line carries only pool/qty/cost, not debit/credit/variance.
pub fn build_lines_json(lines: &[LineParam]) -> Value {
    let arr: Vec<Value> = lines
        .iter()
        .map(|l| {
            json!({
                "pool_id": l.pool_id,
                "line_type": l.line_type,
                "source_id": l.source_id,
                "qty": l.qty,
                "unit_cost": l.unit_cost,
            })
        })
        .collect();
    Value::Array(arr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_lines_json_shape_matches_spi_contract() {
        let lines = vec![LineParam {
            pool_id: 7,
            line_type: "transfer_shipment_line",
            source_id: Some(11),
            qty: -4,
            unit_cost: 50,
        }];
        let v = build_lines_json(&lines);
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["pool_id"], 7);
        assert_eq!(arr[0]["qty"], -4);
        assert_eq!(arr[0]["line_type"], "transfer_shipment_line");
        assert_eq!(arr[0]["unit_cost"], 50);
        // No posting-account fields on the wire (§3.7).
        assert!(arr[0].get("debit_account").is_none());
    }
}
