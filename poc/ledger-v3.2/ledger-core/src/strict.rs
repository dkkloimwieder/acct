//! Strict layer-walk fold — the recalc engine's per-pool costing core
//! (design-v3.2-recalc-a §2/§7, the persistence-backed counterpart of the
//! acct-0at4.7 oracle's `StrictPool`).
//!
//! The fold is pure: given the pool's opening layer state and its physical
//! event stream, it produces per depletion the authoritative unit_cost + the
//! per-consumed-layer draws (the `cost_layer_consumption` basis, recalc-d D3),
//! and the final open-layer state (the `pool_state.layer_id > 0`
//! materialization, recalc-b §6). FIFO consumes the oldest layer first; LIFO
//! the newest.
//!
//! Ordering contract (R-1): `events` MUST arrive sorted by business chronology
//! `(posted_at, id)` and `opening` by the business chronology of the receipts
//! that created the layers (oldest first). The fold trusts the order — the
//! SQL access path supplies it (`(pool_id, posted_at, id)` index, recalc-d D1).
//! Replaying the same stream in commit order instead is exactly the oracle's
//! §1b falsification case.
//!
//! Ill-defined depletions (alt-C allow-negative): when the walk exhausts every
//! layer, the uncovered remainder is costed at the depletion's own observed
//! `unit_cost` (its recorded provisional basis) — deterministic and replayable,
//! so repeated passes over an unchanged stream converge to zero delta. The
//! close-time sweep (recalc-e §5) reconciles the residue.

use std::collections::VecDeque;

use crate::numeric::banker_div;

/// One open layer: `layer_id` is the trx_line.id of the receipt that created
/// it (the `pool_state.layer_id > 0` convention), `qty` the remaining quantity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrictLayer {
    pub layer_id: i64,
    pub qty: i64,
    pub unit_cost: i64,
}

/// One physical `trx_line` event in R-1 order. `qty` is signed: positive =
/// receipt (opens a layer), negative = depletion (consumes layers). `unit_cost`
/// is the recorded cost — asserted for receipts, the observed provisional for
/// depletions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrictEvent {
    pub trx_line_id: i64,
    pub qty: i64,
    pub unit_cost: i64,
}

/// Quantity drawn from one layer by one depletion — a `cost_layer_consumption`
/// row in the making.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LayerDraw {
    pub layer_trx_line_id: i64,
    pub qty: i64,
    pub unit_cost: i64,
}

/// The fold's per-depletion output: `authoritative_unit_cost =
/// banker_div(Σ draw qty·cost + uncovered_qty·observed, |qty|)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepletionCosting {
    pub depletion_trx_line_id: i64,
    /// Absolute depletion quantity.
    pub qty: i64,
    /// The recorded provisional cost on the depletion line.
    pub observed_unit_cost: i64,
    pub authoritative_unit_cost: i64,
    /// Quantity no layer covered (the flagged-negative case); costed at
    /// `observed_unit_cost`.
    pub uncovered_qty: i64,
    pub draws: Vec<LayerDraw>,
}

/// Everything one pass materializes: the depletion costings in stream order
/// and the final open-layer state (oldest first).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrictFoldOutput {
    pub costings: Vec<DepletionCosting>,
    pub layers: Vec<StrictLayer>,
}

/// Run the strict layer-walk. `newest_first = false` is FIFO, `true` is LIFO.
pub fn strict_fold(
    newest_first: bool,
    opening: Vec<StrictLayer>,
    events: &[StrictEvent],
) -> StrictFoldOutput {
    let mut layers: VecDeque<StrictLayer> = opening.into();
    let mut costings = Vec::new();

    for ev in events {
        if ev.qty > 0 {
            layers.push_back(StrictLayer {
                layer_id: ev.trx_line_id,
                qty: ev.qty,
                unit_cost: ev.unit_cost,
            });
            continue;
        }

        let qty = ev.qty.unsigned_abs() as i64;
        let mut remaining = qty;
        let mut value: i128 = 0;
        let mut draws = Vec::new();

        while remaining > 0 {
            let layer = if newest_first { layers.back_mut() } else { layers.front_mut() };
            let Some(layer) = layer else { break };
            let take = remaining.min(layer.qty);
            value += take as i128 * layer.unit_cost as i128;
            draws.push(LayerDraw {
                layer_trx_line_id: layer.layer_id,
                qty: take,
                unit_cost: layer.unit_cost,
            });
            layer.qty -= take;
            remaining -= take;
            if layer.qty == 0 {
                if newest_first {
                    layers.pop_back();
                } else {
                    layers.pop_front();
                }
            }
        }

        let uncovered_qty = remaining;
        value += uncovered_qty as i128 * ev.unit_cost as i128;

        costings.push(DepletionCosting {
            depletion_trx_line_id: ev.trx_line_id,
            qty,
            observed_unit_cost: ev.unit_cost,
            authoritative_unit_cost: banker_div(value, qty),
            uncovered_qty,
            draws,
        });
    }

    StrictFoldOutput { costings, layers: layers.into_iter().collect() }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt(id: i64, qty: i64, cost: i64) -> StrictEvent {
        StrictEvent { trx_line_id: id, qty, unit_cost: cost }
    }

    fn deplete(id: i64, qty: i64, observed: i64) -> StrictEvent {
        StrictEvent { trx_line_id: id, qty: -qty, unit_cost: observed }
    }

    #[test]
    fn fifo_consumes_oldest_layer_first() {
        let out = strict_fold(
            false,
            vec![],
            &[receipt(1, 10, 100), receipt(2, 10, 200), deplete(3, 10, 0)],
        );
        assert_eq!(out.costings.len(), 1);
        assert_eq!(out.costings[0].authoritative_unit_cost, 100);
        assert_eq!(
            out.costings[0].draws,
            vec![LayerDraw { layer_trx_line_id: 1, qty: 10, unit_cost: 100 }]
        );
        assert_eq!(out.layers, vec![StrictLayer { layer_id: 2, qty: 10, unit_cost: 200 }]);
    }

    #[test]
    fn lifo_consumes_newest_layer_first() {
        let out = strict_fold(
            true,
            vec![],
            &[receipt(1, 10, 100), receipt(2, 10, 200), deplete(3, 10, 0)],
        );
        assert_eq!(out.costings[0].authoritative_unit_cost, 200);
        assert_eq!(
            out.costings[0].draws,
            vec![LayerDraw { layer_trx_line_id: 2, qty: 10, unit_cost: 200 }]
        );
        assert_eq!(out.layers, vec![StrictLayer { layer_id: 1, qty: 10, unit_cost: 100 }]);
    }

    #[test]
    fn multi_layer_draw_weighted_average_uses_banker_rounding() {
        // 1@1 + 1@2, deplete 2: value 3 over qty 2 = 1.5 -> banker rounds to 2.
        let out = strict_fold(
            false,
            vec![],
            &[receipt(1, 1, 1), receipt(2, 1, 2), deplete(3, 2, 0)],
        );
        let c = &out.costings[0];
        assert_eq!(c.authoritative_unit_cost, 2);
        assert_eq!(c.draws.len(), 2);
        assert!(out.layers.is_empty());
    }

    #[test]
    fn partial_layer_decrement_survives_into_final_state() {
        let out = strict_fold(false, vec![], &[receipt(1, 10, 100), deplete(2, 4, 0)]);
        assert_eq!(out.layers, vec![StrictLayer { layer_id: 1, qty: 6, unit_cost: 100 }]);
    }

    #[test]
    fn opening_layers_are_consumed_before_new_receipts_under_fifo() {
        let opening = vec![StrictLayer { layer_id: 7, qty: 5, unit_cost: 50 }];
        let out = strict_fold(false, opening, &[receipt(9, 5, 500), deplete(10, 5, 0)]);
        assert_eq!(out.costings[0].authoritative_unit_cost, 50);
        assert_eq!(out.costings[0].draws[0].layer_trx_line_id, 7);
        assert_eq!(out.layers, vec![StrictLayer { layer_id: 9, qty: 5, unit_cost: 500 }]);
    }

    /// Oracle §1b vignette A: the backdated cheaper receipt. In business order
    /// the cheap lot layers FIRST and the depletion costs 100; replaying the
    /// same events in commit order instead returns 300 — which is why the
    /// engine must feed this fold in (posted_at, id) order (R-1).
    #[test]
    fn oracle_vignette_a_business_order_vs_commit_order() {
        // Business order: R_back(day1, 200@100), R_base(day2, 200@300), D(day3, 100).
        let business = strict_fold(
            false,
            vec![],
            &[receipt(3, 200, 100), receipt(1, 200, 300), deplete(2, 100, 0)],
        );
        assert_eq!(business.costings[0].authoritative_unit_cost, 100);

        // Commit order: R_base, D, R_back — the wrong replay returns 300.
        let commit = strict_fold(
            false,
            vec![],
            &[receipt(1, 200, 300), deplete(2, 100, 0), receipt(3, 200, 100)],
        );
        assert_eq!(commit.costings[0].authoritative_unit_cost, 300);
    }

    #[test]
    fn fully_uncovered_depletion_costs_at_observed() {
        let out = strict_fold(false, vec![], &[deplete(1, 32, 700)]);
        let c = &out.costings[0];
        assert_eq!(c.authoritative_unit_cost, 700);
        assert_eq!(c.uncovered_qty, 32);
        assert!(c.draws.is_empty());
        assert!(out.layers.is_empty());
    }

    #[test]
    fn partially_uncovered_depletion_mixes_layer_and_observed_cost() {
        // 10@50 covered + 5 uncovered@80: (500 + 400) / 15 = 60.
        let out = strict_fold(false, vec![], &[receipt(1, 10, 50), deplete(2, 15, 80)]);
        let c = &out.costings[0];
        assert_eq!(c.authoritative_unit_cost, 60);
        assert_eq!(c.uncovered_qty, 5);
        assert_eq!(c.draws, vec![LayerDraw { layer_trx_line_id: 1, qty: 10, unit_cost: 50 }]);
    }

    #[test]
    fn receipts_only_yields_no_costings_and_ordered_layers() {
        let out = strict_fold(true, vec![], &[receipt(1, 3, 10), receipt(2, 4, 20)]);
        assert!(out.costings.is_empty());
        assert_eq!(out.layers.len(), 2);
        assert_eq!(out.layers[0].layer_id, 1);
        assert_eq!(out.layers[1].layer_id, 2);
    }

    /// Determinism: the same stream folds to the same output — the property
    /// that makes a repeated pass a zero-delta no-op (recalc-d §5).
    #[test]
    fn fold_is_deterministic() {
        let events = [
            receipt(1, 10, 100),
            deplete(2, 3, 90),
            receipt(3, 7, 250),
            deplete(4, 12, 130),
            deplete(5, 8, 60),
        ];
        let a = strict_fold(false, vec![], &events);
        let b = strict_fold(false, vec![], &events);
        assert_eq!(a, b);
    }
}
