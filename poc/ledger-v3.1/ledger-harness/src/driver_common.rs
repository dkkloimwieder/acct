//! Shared helpers for the `run` drivers (direct per-call, direct batched, routed).

use chrono::{DateTime, Utc};
use serde_json::{json, Value};

use crate::scenarios::ScenarioSpec;
use crate::workload::LineParam;

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

/// JSONB array shape expected by `ledger_submit_trx_c` / `ledger_enqueue_trx_c`.
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
                "debit_account": l.debit_account,
                "credit_account": l.credit_account,
                "variance_account": l.variance_account,
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
            debit_account: 2000,
            credit_account: 1000,
            variance_account: 3000,
        }];
        let v = build_lines_json(&lines);
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["pool_id"], 7);
        assert_eq!(arr[0]["qty"], -4);
        assert_eq!(arr[0]["line_type"], "transfer_shipment_line");
        assert_eq!(arr[0]["variance_account"], 3000);
    }
}
