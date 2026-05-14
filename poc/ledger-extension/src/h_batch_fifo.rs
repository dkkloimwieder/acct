//! acct-xeee / zm69.h11 — Path 4: native batch FIFO wrapper.
//!
//! Single FFI per batch. Caller passes the full envelope JSONB; this
//! function parses natively, walks layers in receipt order against the
//! `h_layer_arena` shmem cells (no per-issue plpgsql LOOP), and bulk-
//! INSERTs all durable rows at the end.
//!
//! ## Why path 4 exists
//!
//! Paths 1/2/3 (migs 0030/0032/0033/0034) were plpgsql wrappers. Each
//! per-issue / per-layer step crossed the SPI boundary — ~3500-5600 SPI
//! roundtrips per b=1000 batch × ~30-50µs each = ~200ms of pure SPI
//! overhead, before any actual work. The shmem CAS speed of path 2 was
//! invisible behind that wall. See
//! `bench/results-h-ext-paths-2026-05-14.md`.
//!
//! Path 4 collapses the SPI roundtrip count to ~4-5 calls per batch
//! regardless of envelope count:
//!
//!   1. `SELECT nextval(...)` × 2  — pre-allocate layer_ids + cons_ids
//!   2. `INSERT cost_layers_h_ext`  — bulk via `unnest()`
//!   3. `SELECT layer_id, unit_cost FROM cost_layers_h_ext WHERE
//!       layer_group_id = ANY($1) ORDER BY ...`  — one query for ALL
//!       touched groups, partitioned client-side
//!   4. `INSERT cost_consumptions_h_ext`  — bulk via `unnest()`
//!   5. `INSERT cost_layer_depletions_h_ext`  — bulk via `unnest()`
//!
//! Per-issue FIFO walk happens entirely in Rust: a plain `for layer in
//! group_layers { let take = h_layer_decrement(...); ... }`. No SPI per
//! decrement; no plpgsql LOOP overhead.
//!
//! ## Same correctness model as paths 1-3
//!
//! - Per-layer residual tracked in `h_layer_arena` (eager apply, ABORT
//!   reversal — same as path 2 mig 0034).
//! - Per-group invariant tracked in `h_arena` (PRE_COMMIT CAS check).
//! - Durable inserts go to `cost_layers_h_ext` / `cost_consumptions_h_ext`
//!   / `cost_layer_depletions_h_ext` — pure inserts, no predicate reads.
//! - `qty_remaining` on `cost_layers_h_ext` is left STALE (path 2
//!   pattern); shmem residual is truth. Test/recon dispatches on
//!   function name.
//!
//! ## Lock-order discipline
//!
//! Issues are processed in ascending `layer_group_id` order. Two
//! concurrent backends touching overlapping groups acquire the same
//! group's layer cells in the same order, so the per-layer CAS chain is
//! cycle-free.
//!
//! ## Overconsume semantics
//!
//! If a per-issue walk runs out of residual before satisfying its qty,
//! we raise `ERRCODE_T_R_SERIALIZATION_FAILURE` (SQLSTATE 40001) so the
//! bench harness retries. Same convention as path 1 mig 0030's
//! `RAISE EXCEPTION ... USING ERRCODE = '40001'`.

use crate::h_arena;
use crate::h_layer_arena;
use pgrx::prelude::*;
use std::collections::BTreeMap;

#[pg_extern]
pub fn h_apply_batch_fifo(envelopes: pgrx::JsonB) {
    let arr = match envelopes.0.as_array() {
        Some(a) => a,
        None => pgrx::error!("h_apply_batch_fifo: envelopes must be JSONB array"),
    };

    // ─── Phase 1 ── parse + partition ─────────────────────────────────
    #[derive(Clone)]
    struct Receipt {
        gid: i64,
        qty: i64,
        unit_cost: i64,
    }
    #[derive(Clone)]
    struct Issue {
        qty: i64,
    }

    let mut receipts: Vec<Receipt> = Vec::with_capacity(arr.len());
    // BTreeMap so iteration order is ascending group_id (lock-order
    // discipline — see module docstring).
    let mut issues_by_gid: BTreeMap<i64, Vec<Issue>> = BTreeMap::new();
    let mut net_per_gid: BTreeMap<i64, i64> = BTreeMap::new();

    for (idx, env) in arr.iter().enumerate() {
        let obj = match env.as_object() {
            Some(o) => o,
            None => pgrx::error!("h_apply_batch_fifo: envelope[{}] not object", idx),
        };
        let kind = obj
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| {
                pgrx::error!("h_apply_batch_fifo: envelope[{}] missing kind", idx)
            });
        let gid = obj
            .get("layer_group_id")
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| {
                pgrx::error!(
                    "h_apply_batch_fifo: envelope[{}] missing layer_group_id",
                    idx
                )
            });
        let qty = obj
            .get("qty")
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| pgrx::error!("h_apply_batch_fifo: envelope[{}] missing qty", idx));
        if qty <= 0 {
            pgrx::error!(
                "h_apply_batch_fifo: envelope[{}] qty must be > 0; got {}",
                idx,
                qty
            );
        }
        match kind {
            "receipt" => {
                let uc = obj.get("unit_cost").and_then(|v| v.as_i64()).unwrap_or(100);
                receipts.push(Receipt {
                    gid,
                    qty,
                    unit_cost: uc,
                });
                *net_per_gid.entry(gid).or_insert(0) += qty;
            }
            "issue" => {
                issues_by_gid.entry(gid).or_default().push(Issue { qty });
                *net_per_gid.entry(gid).or_insert(0) -= qty;
            }
            other => pgrx::error!(
                "h_apply_batch_fifo: envelope[{}] unsupported kind '{}'",
                idx,
                other
            ),
        }
    }

    // ─── Phase 2 ── pre-allocate layer_ids + bulk INSERT receipts ─────
    if !receipts.is_empty() {
        let n_receipts = receipts.len() as i64;
        let layer_ids: Vec<i64> = Spi::connect(|client| {
            let args: Vec<pgrx::datum::DatumWithOid> = vec![n_receipts.into()];
            let tup = client
                .select(
                    "SELECT nextval('cost_layers_h_ext_layer_id_seq')::BIGINT AS lid \
                     FROM generate_series(1, $1::BIGINT)",
                    None,
                    &args,
                )
                .expect("h_apply_batch_fifo: layer_id nextval");
            let mut ids: Vec<i64> = Vec::with_capacity(n_receipts as usize);
            for row in tup {
                let lid: i64 = row["lid"].value().unwrap().unwrap();
                ids.push(lid);
            }
            ids
        });

        let gids: Vec<i64> = receipts.iter().map(|r| r.gid).collect();
        let qtys: Vec<i64> = receipts.iter().map(|r| r.qty).collect();
        let ucs: Vec<i64> = receipts.iter().map(|r| r.unit_cost).collect();

        Spi::connect_mut(|client| {
            let args: Vec<pgrx::datum::DatumWithOid> = vec![
                layer_ids.clone().into(),
                gids.into(),
                qtys.clone().into(),
                ucs.into(),
            ];
            client
                .update(
                    "INSERT INTO cost_layers_h_ext \
                       (layer_id, layer_group_id, qty, qty_remaining, unit_cost, source_kind) \
                     SELECT lid, g, q, q, c, 'receipt' \
                       FROM unnest($1::BIGINT[], $2::BIGINT[], $3::BIGINT[], $4::BIGINT[]) \
                            AS x(lid, g, q, c)",
                    None,
                    &args,
                )
                .expect("h_apply_batch_fifo: bulk insert layers");
        });

        // Eager-apply shmem cells. Issues in this same batch must see
        // these layers' residual when h_layer_decrement is called below.
        for (i, lid) in layer_ids.iter().enumerate() {
            h_layer_arena::h_layer_create(*lid, qtys[i]);
        }
    }

    // ─── Phase 3 ── pre-fetch layer ordering per touched group ───────
    //
    // One SPI SELECT covers ALL touched groups. We sort client-side into
    // per-group Vec<(layer_id, unit_cost)>. The query orders by born_at
    // then layer_id within each group, so we just preserve fetch order.
    let touched_gids: Vec<i64> = issues_by_gid.keys().copied().collect();
    let mut layers_by_gid: BTreeMap<i64, Vec<(i64, i64)>> = BTreeMap::new();
    if !touched_gids.is_empty() {
        Spi::connect(|client| {
            let args: Vec<pgrx::datum::DatumWithOid> = vec![touched_gids.clone().into()];
            let tup = client
                .select(
                    "SELECT layer_group_id, layer_id, unit_cost \
                       FROM cost_layers_h_ext \
                      WHERE layer_group_id = ANY($1::BIGINT[]) \
                      ORDER BY layer_group_id, born_at, layer_id",
                    None,
                    &args,
                )
                .expect("h_apply_batch_fifo: layer fetch");
            for row in tup {
                let g: i64 = row["layer_group_id"].value().unwrap().unwrap();
                let lid: i64 = row["layer_id"].value().unwrap().unwrap();
                let uc: i64 = row["unit_cost"].value().unwrap().unwrap();
                layers_by_gid.entry(g).or_default().push((lid, uc));
            }
        });
    }

    // ─── Phase 4 ── walk issues per group via shmem CAS ───────────────
    //
    // Accumulate consumption + depletion records in Vec<>. No SPI in the
    // hot loop — h_layer_decrement is a direct Rust call.
    struct ConsumptionRow {
        consumption_id: i64,
        gid: i64,
        qty: i64,
        unit_cost: i64, // weighted
    }
    struct DepletionRow {
        layer_id: i64,
        consumption_id: i64,
        qty_consumed: i64,
        cost_amount: i64,
    }

    let total_issues: usize = issues_by_gid.values().map(|v| v.len()).sum();
    let mut consumption_rows: Vec<ConsumptionRow> = Vec::with_capacity(total_issues);
    // Depletions per issue capped at #layers; estimate generously.
    let mut depletion_rows: Vec<DepletionRow> =
        Vec::with_capacity(total_issues * 4);

    // Pre-allocate consumption_ids in a single nextval call.
    let consumption_ids: Vec<i64> = if total_issues > 0 {
        Spi::connect(|client| {
            let args: Vec<pgrx::datum::DatumWithOid> = vec![(total_issues as i64).into()];
            let tup = client
                .select(
                    "SELECT nextval('cost_consumptions_h_ext_consumption_id_seq')::BIGINT AS cid \
                     FROM generate_series(1, $1::BIGINT)",
                    None,
                    &args,
                )
                .expect("h_apply_batch_fifo: cid nextval");
            let mut ids: Vec<i64> = Vec::with_capacity(total_issues);
            for row in tup {
                let cid: i64 = row["cid"].value().unwrap().unwrap();
                ids.push(cid);
            }
            ids
        })
    } else {
        Vec::new()
    };

    let mut cid_cursor = 0usize;
    for (gid, issues) in &issues_by_gid {
        let layers = layers_by_gid
            .get(gid)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        // Cursor advances across issues in the same group: once a layer
        // hits residual 0 we don't revisit it. h_layer_decrement returns
        // 0 for already-empty cells but the cursor saves the call.
        let mut layer_cursor: usize = 0;

        for iss in issues {
            let cid = consumption_ids[cid_cursor];
            cid_cursor += 1;
            let mut remaining = iss.qty;
            let mut total_cost: i64 = 0;

            while remaining > 0 && layer_cursor < layers.len() {
                let (layer_id, layer_uc) = layers[layer_cursor];
                let requested = remaining;
                let take = h_layer_arena::h_layer_decrement(layer_id, requested);
                if take == 0 {
                    // Layer is exhausted (drained by us in a prior issue,
                    // by another backend, or never seeded). Advance the
                    // cursor permanently for this batch.
                    layer_cursor += 1;
                    continue;
                }
                depletion_rows.push(DepletionRow {
                    layer_id,
                    consumption_id: cid,
                    qty_consumed: take,
                    cost_amount: take * layer_uc,
                });
                remaining -= take;
                total_cost += take * layer_uc;
                if take < requested {
                    // Partial-take ⇒ layer residual hit 0 inside the CAS
                    // (h_layer_decrement takes min(requested, residual);
                    // a sub-requested return implies the layer drained).
                    // Advance cursor.
                    layer_cursor += 1;
                }
                // take == requested ⇒ remaining is now 0, loop exits.
                // Layer may or may not be drained; next issue's call
                // returns 0 if it is and we advance then.
            }

            if remaining > 0 {
                pgrx::ereport!(
                    ERROR,
                    pgrx::PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE,
                    format!(
                        "h_apply_batch_fifo: overconsume group={} short by {} qty",
                        gid, remaining
                    )
                );
            }

            let weighted = total_cost / iss.qty;
            consumption_rows.push(ConsumptionRow {
                consumption_id: cid,
                gid: *gid,
                qty: iss.qty,
                unit_cost: weighted,
            });
        }
    }

    // ─── Phase 5 ── bulk INSERT consumptions ──────────────────────────
    if !consumption_rows.is_empty() {
        let cids: Vec<i64> = consumption_rows.iter().map(|r| r.consumption_id).collect();
        let gids: Vec<i64> = consumption_rows.iter().map(|r| r.gid).collect();
        let qtys: Vec<i64> = consumption_rows.iter().map(|r| r.qty).collect();
        let ucs: Vec<i64> = consumption_rows.iter().map(|r| r.unit_cost).collect();
        Spi::connect_mut(|client| {
            let args: Vec<pgrx::datum::DatumWithOid> =
                vec![cids.into(), gids.into(), qtys.into(), ucs.into()];
            client
                .update(
                    "INSERT INTO cost_consumptions_h_ext \
                       (consumption_id, layer_group_id, qty, unit_cost) \
                     SELECT cid, g, q, uc \
                       FROM unnest($1::BIGINT[], $2::BIGINT[], $3::BIGINT[], $4::BIGINT[]) \
                            AS x(cid, g, q, uc)",
                    None,
                    &args,
                )
                .expect("h_apply_batch_fifo: bulk insert consumptions");
        });
    }

    // ─── Phase 6 ── bulk INSERT depletions ────────────────────────────
    if !depletion_rows.is_empty() {
        let lids: Vec<i64> = depletion_rows.iter().map(|d| d.layer_id).collect();
        let cids: Vec<i64> = depletion_rows.iter().map(|d| d.consumption_id).collect();
        let qcs: Vec<i64> = depletion_rows.iter().map(|d| d.qty_consumed).collect();
        let cas: Vec<i64> = depletion_rows.iter().map(|d| d.cost_amount).collect();
        Spi::connect_mut(|client| {
            let args: Vec<pgrx::datum::DatumWithOid> =
                vec![lids.into(), cids.into(), qcs.into(), cas.into()];
            client
                .update(
                    "INSERT INTO cost_layer_depletions_h_ext \
                       (layer_id, consumption_id, qty_consumed, cost_amount) \
                     SELECT l, c, q, ca \
                       FROM unnest($1::BIGINT[], $2::BIGINT[], $3::BIGINT[], $4::BIGINT[]) \
                            AS x(l, c, q, ca)",
                    None,
                    &args,
                )
                .expect("h_apply_batch_fifo: bulk insert depletions");
        });
    }

    // ─── Phase 7 ── stage per-group net delta into h_arena ─────────────
    //
    // PRE_COMMIT phase will CAS-check + apply. Belt-and-braces against
    // bugs in the FIFO walk: if depletions or shmem cells went out of
    // sync, h_arena's effective_qty check will raise 40001.
    for (gid, net_delta) in net_per_gid {
        h_arena::h_apply_delta(gid, net_delta);
    }
}
