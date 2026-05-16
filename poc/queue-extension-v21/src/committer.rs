//! Committer BGWorker + 5-step pipeline per spec §1.8.
//!
//! M1.3 (acct-wrwf). Single committer, FIFO only. Size-1 SuperBatches
//! (stub router). AVG + STD method dispatch lands at M2.1 + M2.2;
//! committer pool + CAS election at M4.1; per-envelope failure
//! isolation via sub-tx at M6.2.

use crate::cost_method::{
    PocV21CostMethod, PocV21Event, PocV21EventType, PocV21Snapshot, SkuPoolState,
};
use crate::fifo::FIFO_METHOD;
use crate::{
    COMMITTER_QUEUE, POC_V21_COMMITTER_QUEUE_SIZE, SPILLOVER_ARENA, STAGING_QUEUE,
    target_database_str,
};
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::collections::HashMap;
use std::ffi::CString;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::time::Duration;

/// Committer BGWorker entry point.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn poc_v21_committer_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let dbname = target_database_str();
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);

    while BackgroundWorker::wait_latch(Some(Duration::from_millis(50))) {
        loop {
            let claim = claim_next_committer_entry();
            match claim {
                Some(cq_idx) => {
                    BackgroundWorker::transaction(|| {
                        let _ = process_superbatch(cq_idx);
                    });
                }
                None => break,
            }
        }
    }
}

/// CAS-claim the next valid==1 CommitterQueueEntry. Returns its
/// slot index on success.
fn claim_next_committer_entry() -> Option<u32> {
    let queue = COMMITTER_QUEUE.share();
    let head = queue.head.load(Relaxed);
    let capacity = POC_V21_COMMITTER_QUEUE_SIZE as u32;
    let my_pid = unsafe { pg_sys::MyProcPid };
    for i in 0..capacity {
        let idx = ((head + i) % capacity) as usize;
        let slot = &queue.entries[idx];
        if slot.valid.load(Relaxed) == 1
            && slot.committer_pid.compare_exchange(0, my_pid, Acquire, Relaxed).is_ok()
        {
            if slot
                .valid
                .compare_exchange(1, 2, Acquire, Relaxed)
                .is_ok()
            {
                let now_ns = unsafe { pg_sys::GetCurrentTimestamp() as u64 * 1000 };
                slot.committer_acquired_at_ns.store(now_ns, Relaxed);
                queue.head.store((head + 1) % capacity, Relaxed);
                return Some(idx as u32);
            } else {
                // Rare race: another committer flipped valid first; release
                // our pid claim.
                slot.committer_pid.store(0, Relaxed);
            }
        }
    }
    None
}

/// Run the 5-step pipeline for one CommitterQueueEntry.
fn process_superbatch(cq_idx: u32) -> Result<(), String> {
    // Snapshot the CommitterQueueEntry's fields we need.
    let (superbatch_id, envelope_count, staging_offsets_off, sku_keys_off, sku_count, _wip_off, _wip_count) = {
        let queue = COMMITTER_QUEUE.share();
        let slot = &queue.entries[cq_idx as usize];
        (
            slot.superbatch_id,
            slot.envelope_count,
            slot.staging_entry_offsets,
            slot.sku_pool_keys_offset,
            slot.sku_pool_keys_count,
            slot.wip_pool_keys_offset,
            slot.wip_pool_keys_count,
        )
    };

    // Read staging-entry indices from spillover arena.
    let staging_indices: Vec<u32> = {
        let arena = SPILLOVER_ARENA.share();
        let bytes = arena.read_bytes(staging_offsets_off, envelope_count as u32 * 4);
        bytes
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect()
    };

    // Read sku_pool_keys from arena.
    let sku_pool_keys: Vec<(i64, i64)> = if sku_count > 0 {
        let arena = SPILLOVER_ARENA.share();
        let bytes = arena.read_bytes(sku_keys_off, sku_count as u32 * 16);
        bytes
            .chunks_exact(16)
            .map(|c| {
                let a = i64::from_le_bytes(c[0..8].try_into().unwrap());
                let b = i64::from_le_bytes(c[8..16].try_into().unwrap());
                (a, b)
            })
            .collect()
    } else {
        Vec::new()
    };

    // Build events from each staging entry's payload.
    let mut events: Vec<PocV21Event> = Vec::with_capacity(staging_indices.len());
    for &s_idx in &staging_indices {
        let event = read_event_from_staging(s_idx)?;
        events.push(event);
    }

    // Open the sub-tx that owns committer_tx_id + the bulk INSERTs.
    let savepoint_name = CString::new(format!("poc_v21_committer_sb_{superbatch_id}")).unwrap();
    unsafe {
        pg_sys::BeginInternalSubTransaction(savepoint_name.as_ptr());
    }

    let pipeline_result = run_pipeline_inside_subtx(
        cq_idx,
        superbatch_id,
        &staging_indices,
        &sku_pool_keys,
        &events,
    );

    match pipeline_result {
        Ok(_) => {
            unsafe { pg_sys::ReleaseCurrentSubTransaction() };
        }
        Err(err) => {
            unsafe { pg_sys::RollbackAndReleaseCurrentSubTransaction() };
            // Mark all envelopes failed.
            for event in &events {
                let corr = event.correlation_id;
                let detail = pgrx::JsonB(serde_json::json!({ "phase": "pipeline", "detail": err }));
                let _ = Spi::run_with_args(
                    "INSERT INTO poc_v21_submission_status \
                       (correlation_id, state, enqueued_at, processed_at, error_code, error_detail) \
                     VALUES ($1, 'failed', now(), now(), $2, $3) \
                     ON CONFLICT (correlation_id) DO UPDATE SET \
                       state='failed', processed_at=now(), \
                       error_code=EXCLUDED.error_code, error_detail=EXCLUDED.error_detail",
                    &[corr.into(), "pipeline_error".into(), detail.into()],
                );
            }
        }
    }

    // STEP 14: cleanup. Free staging arena blocks, reset staging slots,
    // mark CommitterQueueEntry completed.
    cleanup_after_superbatch(cq_idx, &staging_indices, staging_offsets_off, sku_keys_off);
    Ok(())
}

/// Steps 2–13 inside the active sub-transaction.
fn run_pipeline_inside_subtx(
    cq_idx: u32,
    superbatch_id: u64,
    _staging_indices: &[u32],
    sku_pool_keys: &[(i64, i64)],
    events: &[PocV21Event],
) -> Result<(), String> {
    // STEP 2: locks. SPI INSERT ON CONFLICT DO NOTHING into pool_locks,
    // then SELECT FOR UPDATE in lex order.
    if !sku_pool_keys.is_empty() {
        let sku_ids: Vec<i64> = sku_pool_keys.iter().map(|k| k.0).collect();
        let location_ids: Vec<i64> = sku_pool_keys.iter().map(|k| k.1).collect();
        Spi::run_with_args(
            "INSERT INTO poc_v21_pool_locks (sku_id, location_id) \
             SELECT sku_id, location_id FROM UNNEST($1::bigint[], $2::bigint[]) AS t(sku_id, location_id) \
             ON CONFLICT (sku_id, location_id) DO NOTHING",
            &[sku_ids.clone().into(), location_ids.clone().into()],
        )
        .map_err(|e| format!("pool_locks INSERT: {e}"))?;
        Spi::run_with_args(
            "SELECT 1 FROM poc_v21_pool_locks \
             WHERE (sku_id, location_id) IN (SELECT sku_id, location_id FROM UNNEST($1::bigint[], $2::bigint[]) AS t(sku_id, location_id)) \
             ORDER BY sku_id, location_id \
             FOR UPDATE",
            &[sku_ids.into(), location_ids.into()],
        )
        .map_err(|e| format!("pool_locks SELECT FOR UPDATE: {e}"))?;
    }

    // Force XID allocation via a tiny no-op write (option Q-D(b) — robust
    // across pg_current_xact_id_if_assigned signature ambiguity). The
    // pool_locks INSERT above already triggers XID assignment under
    // ordinary cases (ON CONFLICT DO NOTHING with a non-conflicting row),
    // but the all-conflict case (re-entering a pool) is also possible.
    // Use a dummy LOCAL temp table or a session_lock; simplest is
    // pg_current_xact_id() (always returns; allocates if needed).
    let committer_tx_id: u64 = Spi::get_one("SELECT pg_current_xact_id()::text::bigint")
        .map_err(|e| format!("get committer_tx_id: {e}"))?
        .unwrap_or(0i64) as u64;

    // Store committer_tx_id on the CommitterQueueEntry.
    {
        let queue = COMMITTER_QUEUE.share();
        queue.entries[cq_idx as usize]
            .committer_tx_id
            .store(committer_tx_id, Relaxed);
    }

    // STEP 2.5: dedup. Collect (issue_id, method_used) pairs from events.
    let dedup_pairs: Vec<(i64, String)> = events
        .iter()
        .filter(|e| matches!(
            e.event_type,
            PocV21EventType::InvIssue | PocV21EventType::SoShipment
        ) || (matches!(e.event_type, PocV21EventType::InvAdjust) && e.qty < 0))
        .map(|e| (e.issue_id, "fifo".to_string()))
        .collect();

    let mut replayed_correlation_ids: Vec<pgrx::Uuid> = Vec::new();
    if !dedup_pairs.is_empty() {
        let issue_ids: Vec<i64> = dedup_pairs.iter().map(|p| p.0).collect();
        let methods: Vec<&str> = dedup_pairs.iter().map(|p| p.1.as_str()).collect();
        let methods_owned: Vec<String> = methods.iter().map(|s| s.to_string()).collect();

        // Check both cost_depletions and cost_consumptions for prior writes.
        let existing: Vec<i64> = Spi::connect(|client| {
            let mut out = Vec::new();
            let mut t = client
                .select(
                    "SELECT DISTINCT issue_id FROM poc_v21_cost_depletions \
                     WHERE (issue_id, method_used) IN (\
                       SELECT issue_id, method_used FROM UNNEST($1::bigint[], $2::text[]) AS t(issue_id, method_used)\
                     ) \
                     UNION \
                     SELECT DISTINCT issue_id FROM poc_v21_cost_consumptions \
                     WHERE (issue_id, method_used) IN (\
                       SELECT issue_id, method_used FROM UNNEST($1::bigint[], $2::text[]) AS t(issue_id, method_used)\
                     )",
                    None,
                    &[issue_ids.into(), methods_owned.into()],
                )
                .ok()?;
            while let Some(row) = t.next() {
                if let Ok(Some(id)) = row.get::<i64>(1) {
                    out.push(id);
                }
            }
            Some(out)
        })
        .unwrap_or_default();

        if !existing.is_empty() {
            for event in events {
                if matches!(
                    event.event_type,
                    PocV21EventType::InvIssue | PocV21EventType::SoShipment
                ) || (matches!(event.event_type, PocV21EventType::InvAdjust) && event.qty < 0)
                {
                    if existing.contains(&event.issue_id) {
                        replayed_correlation_ids.push(event.correlation_id);
                    }
                }
            }
        }
    }

    let events_to_plan: Vec<PocV21Event> = events
        .iter()
        .filter(|e| !replayed_correlation_ids.contains(&e.correlation_id))
        .cloned()
        .collect();

    // STEP 3: snapshot hydration. FIFO layers only at M1.3.
    let mut snapshot = PocV21Snapshot::default();
    if !sku_pool_keys.is_empty() {
        let sku_ids: Vec<i64> = sku_pool_keys.iter().map(|k| k.0).collect();
        let location_ids: Vec<i64> = sku_pool_keys.iter().map(|k| k.1).collect();
        hydrate_fifo_layers(&mut snapshot, &sku_ids, &location_ids)?;
    }

    // STEP 4: dispatch. Sort events by chronological key; call FIFO.plan_apply.
    let mut sorted_events = events_to_plan.clone();
    sorted_events.sort_by_key(|e| {
        (
            e.business_date_jdate,
            e.doc_chrono,
            e.document_id,
            e.sub_priority,
        )
    });
    let result = FIFO_METHOD.plan_apply(&sorted_events, &mut snapshot);

    // Drop envelopes whose per_event reported an error.
    let mut failed_correlation_ids: Vec<(pgrx::Uuid, String)> = Vec::new();
    for er in &result.per_event {
        if let Some(code) = &er.error_code {
            failed_correlation_ids.push((er.correlation_id, code.clone()));
        }
    }
    let succeeded: Vec<pgrx::Uuid> = result
        .per_event
        .iter()
        .filter(|er| er.error_code.is_none())
        .map(|er| er.correlation_id)
        .collect();

    // Filter row vectors to succeeded envelopes only.
    let succeeded_set: std::collections::HashSet<pgrx::Uuid> = succeeded.iter().copied().collect();
    let layer_rows: Vec<_> = result
        .layer_inserts
        .iter()
        .filter(|r| succeeded_set.contains(&r.correlation_id))
        .cloned()
        .collect();
    let depletion_rows: Vec<_> = result
        .depletion_inserts
        .iter()
        .filter(|r| succeeded_set.contains(&r.correlation_id))
        .cloned()
        .collect();
    let posting_line_rows: Vec<_> = result
        .posting_line_inserts
        .iter()
        .filter(|r| succeeded_set.contains(&r.correlation_id))
        .cloned()
        .collect();
    let posting_inventory_rows: Vec<_> = result
        .posting_line_inventory_inserts
        .iter()
        .filter_map(|r| {
            let pl = result.posting_line_inserts.get(r.posting_line_ordinal)?;
            if succeeded_set.contains(&pl.correlation_id) {
                Some((r.clone(), pl.correlation_id))
            } else {
                None
            }
        })
        .collect();

    // STEP 5: bulk UNNEST inserts.
    // 5a. posting_lines — INSERT ... RETURNING posting_line_id so we can
    // emit posting_line_inventory rows linked by id. M1.3 size-1 batch
    // has at most a small number of posting lines; we'll loop instead of
    // UNNEST to keep the code simple. M2+ moves to UNNEST.
    let mut posting_line_ids_by_corr: HashMap<pgrx::Uuid, Vec<i64>> = HashMap::new();
    for pl in &posting_line_rows {
        let row: Option<i64> = Spi::get_one_with_args(
            "INSERT INTO poc_v21_posting_lines \
               (business_date, doc_chrono, document_id, sub_priority, event_type, amount, \
                debit_account, credit_account, correlation_id, user_tx_xid, committer_tx_id, superbatch_id) \
             VALUES ('2000-01-01'::date + $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::text::xid8, $11, $12) \
             RETURNING posting_line_id",
            &[
                pl.business_date_jdate.into(),
                pl.doc_chrono.into(),
                pl.document_id.into(),
                pl.sub_priority.into(),
                pl.event_type.into(),
                pl.amount.into(),
                pl.debit_account.into(),
                pl.credit_account.into(),
                pl.correlation_id.into(),
                (pl.user_tx_xid as i64).into(),
                (committer_tx_id as i64).into(),
                (superbatch_id as i64).into(),
            ],
        )
        .map_err(|e| format!("posting_lines INSERT: {e}"))?;
        if let Some(id) = row {
            posting_line_ids_by_corr
                .entry(pl.correlation_id)
                .or_default()
                .push(id);
        }
    }

    // 5b. cost_layers — INSERT ... RETURNING layer_id (so we can stamp
    // depletions that reference them; not used at M1.3 since depletions
    // reference pre-existing layers).
    let mut new_layer_ids_by_corr: HashMap<pgrx::Uuid, Vec<i64>> = HashMap::new();
    for lr in &layer_rows {
        let row: Option<i64> = Spi::get_one_with_args(
            "INSERT INTO poc_v21_cost_layers \
               (sku_id, location_id, qty, unit_cost, born_at, born_seq, source_kind, source_ref, correlation_id, user_tx_xid, committer_tx_id, superbatch_id) \
             VALUES ($1, $2, $3, $4, to_timestamp($5::double precision / 1000000), $6, $7, $8, $9, $10::text::xid8, $11, $12) \
             RETURNING layer_id",
            &[
                lr.sku_id.into(),
                lr.location_id.into(),
                lr.qty.into(),
                lr.unit_cost.into(),
                lr.born_at_micros.into(),
                lr.born_seq.into(),
                lr.source_kind.into(),
                lr.source_ref.into(),
                lr.correlation_id.into(),
                (lr.user_tx_xid as i64).into(),
                (committer_tx_id as i64).into(),
                (superbatch_id as i64).into(),
            ],
        )
        .map_err(|e| format!("cost_layers INSERT: {e}"))?;
        if let Some(id) = row {
            new_layer_ids_by_corr
                .entry(lr.correlation_id)
                .or_default()
                .push(id);
        }
    }

    // 5c. cost_depletions.
    for dr in &depletion_rows {
        Spi::run_with_args(
            "INSERT INTO poc_v21_cost_depletions \
               (layer_id, qty, unit_cost, consumed_at, consumed_seq, issue_id, method_used, correlation_id, user_tx_xid, committer_tx_id, superbatch_id) \
             VALUES ($1, $2, $3, to_timestamp($4::double precision / 1000000), $5, $6, $7, $8, $9::text::xid8, $10, $11)",
            &[
                dr.layer_id.into(),
                dr.qty.into(),
                dr.unit_cost.into(),
                dr.consumed_at_micros.into(),
                dr.consumed_seq.into(),
                dr.issue_id.into(),
                dr.method_used.into(),
                dr.correlation_id.into(),
                (dr.user_tx_xid as i64).into(),
                (committer_tx_id as i64).into(),
                (superbatch_id as i64).into(),
            ],
        )
        .map_err(|e| format!("cost_depletions INSERT: {e}"))?;
    }

    // 5d. posting_line_inventory — link to posting_line_id via the
    // per-correlation pop order.
    let mut pop_cursor: HashMap<pgrx::Uuid, usize> = HashMap::new();
    for (inv_row, corr) in &posting_inventory_rows {
        let cursor = pop_cursor.entry(*corr).or_insert(0);
        let pl_ids = posting_line_ids_by_corr.get(corr).cloned().unwrap_or_default();
        let pl_id = match pl_ids.get(*cursor) {
            Some(&id) => id,
            None => continue,
        };
        *cursor += 1;
        let layer_id = if let Some(real_id) = inv_row.layer_id {
            Some(real_id)
        } else {
            // Receipt-side row references a new layer: pop the next new
            // layer_id for this correlation_id.
            new_layer_ids_by_corr.get_mut(corr).and_then(|v| {
                if v.is_empty() {
                    None
                } else {
                    Some(v.remove(0))
                }
            })
        };
        Spi::run_with_args(
            "INSERT INTO poc_v21_posting_line_inventory \
               (posting_line_id, sku_id, location_id, qty, layer_id) \
             VALUES ($1, $2, $3, $4, $5)",
            &[
                pl_id.into(),
                inv_row.sku_id.into(),
                inv_row.location_id.into(),
                inv_row.qty.into(),
                layer_id.into(),
            ],
        )
        .map_err(|e| format!("posting_line_inventory INSERT: {e}"))?;
    }

    // STEP 12: status updates.
    if !succeeded.is_empty() {
        Spi::run_with_args(
            "UPDATE poc_v21_submission_status \
               SET state='committed', committed_at=now(), processed_at=now(), \
                   committer_tx_id=$1, superbatch_id=$2 \
             WHERE correlation_id = ANY($3::uuid[])",
            &[
                (committer_tx_id as i64).into(),
                (superbatch_id as i64).into(),
                succeeded.clone().into(),
            ],
        )
        .map_err(|e| format!("status committed UPDATE: {e}"))?;
    }
    for (corr, code) in &failed_correlation_ids {
        let detail = pgrx::JsonB(serde_json::json!({ "phase": "plan_apply", "detail": code }));
        Spi::run_with_args(
            "INSERT INTO poc_v21_submission_status \
               (correlation_id, state, enqueued_at, processed_at, error_code, error_detail, committer_tx_id, superbatch_id) \
             VALUES ($1, 'failed', now(), now(), $2, $3, $4, $5) \
             ON CONFLICT (correlation_id) DO UPDATE SET \
               state='failed', processed_at=now(), \
               error_code=EXCLUDED.error_code, error_detail=EXCLUDED.error_detail, \
               committer_tx_id=EXCLUDED.committer_tx_id, superbatch_id=EXCLUDED.superbatch_id",
            &[
                (*corr).into(),
                code.as_str().into(),
                detail.into(),
                (committer_tx_id as i64).into(),
                (superbatch_id as i64).into(),
            ],
        )
        .map_err(|e| format!("status failed UPDATE: {e}"))?;
    }
    if !replayed_correlation_ids.is_empty() {
        Spi::run_with_args(
            "UPDATE poc_v21_submission_status \
               SET state='replayed', processed_at=now(), \
                   committer_tx_id=$1, superbatch_id=$2 \
             WHERE correlation_id = ANY($3::uuid[])",
            &[
                (committer_tx_id as i64).into(),
                (superbatch_id as i64).into(),
                replayed_correlation_ids.clone().into(),
            ],
        )
        .map_err(|e| format!("status replayed UPDATE: {e}"))?;
    }

    Ok(())
}

fn hydrate_fifo_layers(
    snapshot: &mut PocV21Snapshot,
    sku_ids: &[i64],
    location_ids: &[i64],
) -> Result<(), String> {
    use crate::cost_method::LayerView;

    Spi::connect(|client| -> Result<(), String> {
        let mut t = client
            .select(
                "WITH layers AS (\
                   SELECT layer_id, sku_id, location_id, qty, unit_cost, \
                          EXTRACT(EPOCH FROM born_at)::bigint * 1000000 AS born_at_micros, \
                          born_seq, correlation_id, \
                          (qty - COALESCE((SELECT SUM(qty)::bigint FROM poc_v21_cost_depletions d WHERE d.layer_id = poc_v21_cost_layers.layer_id), 0))::bigint AS effective_qty \
                     FROM poc_v21_cost_layers \
                    WHERE (sku_id, location_id) IN (SELECT sku_id, location_id FROM UNNEST($1::bigint[], $2::bigint[]) AS t(sku_id, location_id)) \
                 ) \
                 SELECT layer_id, sku_id, location_id, unit_cost, born_at_micros, born_seq, correlation_id, effective_qty \
                   FROM layers \
                  WHERE effective_qty > 0 \
                  ORDER BY sku_id, location_id, born_at_micros, born_seq",
                None,
                &[sku_ids.to_vec().into(), location_ids.to_vec().into()],
            )
            .map_err(|e| format!("snapshot SELECT cost_layers: {e}"))?;
        while let Some(row) = t.next() {
            let layer_id: i64 = row.get::<i64>(1).map_err(|e| format!("layer_id: {e}"))?.unwrap_or(0);
            let sku_id: i64 = row.get::<i64>(2).map_err(|e| format!("sku_id: {e}"))?.unwrap_or(0);
            let location_id: i64 = row.get::<i64>(3).map_err(|e| format!("location_id: {e}"))?.unwrap_or(0);
            let unit_cost: i64 = row.get::<i64>(4).map_err(|e| format!("unit_cost: {e}"))?.unwrap_or(0);
            let born_at_micros: i64 = row.get::<i64>(5).map_err(|e| format!("born_at_micros: {e}"))?.unwrap_or(0);
            let born_seq: i64 = row.get::<i64>(6).map_err(|e| format!("born_seq: {e}"))?.unwrap_or(0);
            let correlation_id: pgrx::Uuid = row.get::<pgrx::Uuid>(7).map_err(|e| format!("correlation_id: {e}"))?.unwrap();
            let effective_qty: i64 = row.get::<i64>(8).map_err(|e| format!("effective_qty: {e}"))?.unwrap_or(0);

            let pool = snapshot
                .sku_pools
                .entry((sku_id, location_id))
                .or_insert_with(SkuPoolState::default);
            if born_seq > pool.max_born_seq {
                pool.max_born_seq = born_seq;
            }
            pool.layers.push(LayerView {
                layer_id,
                unit_cost,
                effective_qty,
                born_at_micros,
                born_seq,
                correlation_id,
            });
        }
        Ok(())
    })?;

    // Seed max_born_seq for pools with no live layers (rolled-down). The
    // SELECT above filtered effective_qty > 0; ensure max_born_seq still
    // reflects the highest born_seq written. For M1.3 this is conservative
    // — we'll re-fetch separately.
    Spi::connect(|client| -> Result<(), String> {
        let mut t = client
            .select(
                "SELECT sku_id, location_id, MAX(born_seq) AS max_seq \
                   FROM poc_v21_cost_layers \
                  WHERE (sku_id, location_id) IN (SELECT sku_id, location_id FROM UNNEST($1::bigint[], $2::bigint[]) AS t(sku_id, location_id)) \
                  GROUP BY sku_id, location_id",
                None,
                &[sku_ids.to_vec().into(), location_ids.to_vec().into()],
            )
            .map_err(|e| format!("snapshot SELECT max(born_seq): {e}"))?;
        while let Some(row) = t.next() {
            let sku_id: i64 = row.get::<i64>(1).map_err(|e| format!("sku_id: {e}"))?.unwrap_or(0);
            let location_id: i64 = row.get::<i64>(2).map_err(|e| format!("location_id: {e}"))?.unwrap_or(0);
            let max_seq: i64 = row.get::<i64>(3).map_err(|e| format!("max_seq: {e}"))?.unwrap_or(0);
            let pool = snapshot
                .sku_pools
                .entry((sku_id, location_id))
                .or_insert_with(SkuPoolState::default);
            if max_seq > pool.max_born_seq {
                pool.max_born_seq = max_seq;
            }
        }
        Ok(())
    })?;

    Ok(())
}

fn read_event_from_staging(staging_idx: u32) -> Result<PocV21Event, String> {
    let queue = STAGING_QUEUE.share();
    let slot = &queue.entries[staging_idx as usize];
    let payload_offset = slot.payload_offset;
    let payload_length = slot.payload_length;
    let correlation_id = pgrx::Uuid::from_bytes(slot.correlation_id);
    let user_tx_xid = slot.user_tx_xid;
    let event_type_id = slot.event_type_id;
    let enqueued_at = slot.enqueued_at_micros;
    drop(queue);

    let payload_bytes = {
        let arena = SPILLOVER_ARENA.share();
        arena.read_bytes(payload_offset, payload_length)
    };
    let payload: serde_json::Value = serde_json::from_slice(&payload_bytes)
        .map_err(|e| format!("payload parse: {e}"))?;

    let event_type = match event_type_id {
        1 => PocV21EventType::InvAdjust,
        2 => return Err("wo_complete not supported at M1.3".to_string()),
        3 => return Err("wo_start not supported at M1.3".to_string()),
        5 => PocV21EventType::PoReceipt,
        6 => PocV21EventType::SoShipment,
        7 => PocV21EventType::InvIssue,
        _ => return Err(format!("unknown event_type_id={event_type_id}")),
    };

    let sku_id = payload.get("sku_id").and_then(|v| v.as_i64()).unwrap_or(0);
    let location_id = payload
        .get("location_id")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let qty = payload.get("qty").and_then(|v| v.as_i64()).unwrap_or(0);
    let unit_cost = payload.get("unit_cost").and_then(|v| v.as_i64()).unwrap_or(0);
    let issue_id = payload.get("issue_id").and_then(|v| v.as_i64()).unwrap_or(0);
    let business_date_jdate = payload
        .get("business_date_jdate")
        .and_then(|v| v.as_i64())
        .map(|v| v as i32)
        .unwrap_or(9999);
    let doc_chrono = payload
        .get("doc_chrono")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let document_id = payload
        .get("document_id")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let sub_priority = payload
        .get("sub_priority")
        .and_then(|v| v.as_i64())
        .map(|v| v as i32)
        .unwrap_or(0);

    Ok(PocV21Event {
        correlation_id,
        issue_id,
        event_type,
        sku_id,
        location_id,
        qty,
        unit_cost,
        business_date_jdate,
        doc_chrono,
        document_id,
        sub_priority,
        user_tx_xid,
        at_micros: enqueued_at as i64,
    })
}

fn cleanup_after_superbatch(
    cq_idx: u32,
    staging_indices: &[u32],
    staging_offsets_off: u32,
    sku_keys_off: u32,
) {
    // Free staging-entry arena blocks; CAS staging.valid 3→0.
    for &s_idx in staging_indices {
        let (payload_off, sku_off, wip_off, cas_ok) = {
            let queue = STAGING_QUEUE.share();
            let slot = &queue.entries[s_idx as usize];
            let p = slot.payload_offset;
            let s = slot.sku_pool_keys_offset;
            let w = slot.wip_pool_keys_offset;
            let ok = slot
                .valid
                .compare_exchange(3, 0, Release, Relaxed)
                .is_ok();
            (p, s, w, ok)
        };
        if cas_ok {
            let mut arena = SPILLOVER_ARENA.exclusive();
            if payload_off != 0 {
                arena.free(payload_off);
            }
            if sku_off != 0 {
                arena.free(sku_off);
            }
            if wip_off != 0 {
                arena.free(wip_off);
            }
        }
        // CAS failure = ejected; leave arena blocks for the router to re-claim.
    }

    // Free CommitterQueueEntry's own arena blocks (staging_offsets +
    // sku_pool_keys + wip_pool_keys mirror).
    let wip_keys_off = {
        let queue = COMMITTER_QUEUE.share();
        queue.entries[cq_idx as usize].wip_pool_keys_offset
    };
    {
        let mut arena = SPILLOVER_ARENA.exclusive();
        if staging_offsets_off != 0 {
            arena.free(staging_offsets_off);
        }
        if sku_keys_off != 0 {
            arena.free(sku_keys_off);
        }
        if wip_keys_off != 0 {
            arena.free(wip_keys_off);
        }
    }

    // CAS CommitterQueueEntry valid 2→3 (completed), then 3→0.
    {
        let queue = COMMITTER_QUEUE.share();
        let slot = &queue.entries[cq_idx as usize];
        let _ = slot.valid.compare_exchange(2, 3, Release, Relaxed);
        let _ = slot.valid.compare_exchange(3, 0, Release, Relaxed);
        slot.committer_pid.store(0, Relaxed);
        slot.committer_acquired_at_ns.store(0, Relaxed);
        slot.committer_tx_id.store(0, Relaxed);
    }
}

/// Synchronous committer-tick helper for tests: claims one entry and
/// runs the full pipeline. Returns true if work was done.
#[pg_extern]
fn poc_v21_test_committer_tick() -> bool {
    let claim = claim_next_committer_entry();
    match claim {
        Some(cq_idx) => {
            // Run inside an outer transaction so SPI works the same way
            // as in the BGWorker.
            let _ = process_superbatch(cq_idx);
            true
        }
        None => false,
    }
}
