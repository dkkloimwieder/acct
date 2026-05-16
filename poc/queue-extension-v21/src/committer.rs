//! Committer BGWorker + 5-step pipeline per spec §1.8.
//!
//! M1.3 (acct-wrwf). Single committer, FIFO only. Size-1 SuperBatches
//! (stub router). AVG + STD method dispatch lands at M2.1 + M2.2;
//! committer pool + CAS election at M4.1; per-envelope failure
//! isolation via sub-tx at M6.2.

use crate::avg::AVG_METHOD;
use crate::cost_method::{
    PocV21ApplyResult, PocV21CostMethod, PocV21Event, PocV21EventType, PocV21Snapshot,
    SkuPoolState,
};
use crate::fifo::FIFO_METHOD;
use crate::standard::STANDARD_METHOD;
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

    // STEP 2.4 (new at M2.1): hydrate per-SKU method assignments from
    // poc_v21_sku_method_assignments. SKUs without a row default to
    // "fifo" — this preserves M1.3 test convenience and matches the
    // bake-off setup convention (only AVG/STD SKUs need explicit rows).
    let mut method_for_sku: HashMap<i64, &'static str> = HashMap::new();
    if !sku_pool_keys.is_empty() {
        let sku_ids: Vec<i64> = sku_pool_keys.iter().map(|k| k.0).collect();
        let assigned: Vec<(i64, String)> = Spi::connect(|client| {
            let mut out: Vec<(i64, String)> = Vec::new();
            let mut t = client
                .select(
                    "SELECT sku_id, method_id FROM poc_v21_sku_method_assignments \
                       WHERE sku_id = ANY($1::bigint[])",
                    None,
                    &[sku_ids.into()],
                )
                .ok()?;
            while let Some(row) = t.next() {
                let sku_id: i64 = row.get::<i64>(1).ok()??;
                let method: String = row.get::<String>(2).ok()??;
                out.push((sku_id, method));
            }
            Some(out)
        })
        .unwrap_or_default();
        for (sku_id, method) in assigned {
            let m: &'static str = match method.as_str() {
                "avg" => "avg",
                "std" => "std",
                _ => "fifo",
            };
            method_for_sku.insert(sku_id, m);
        }
    }
    let method_of = |sku_id: i64| -> &'static str {
        *method_for_sku.get(&sku_id).unwrap_or(&"fifo")
    };

    // STEP 2.5: dedup. Collect (issue_id, method_used) pairs per event's
    // SKU method assignment (AVG events check cost_consumptions; FIFO
    // checks cost_depletions; UNION below covers both).
    let dedup_pairs: Vec<(i64, &'static str)> = events
        .iter()
        .filter(|e| matches!(
            e.event_type,
            PocV21EventType::InvIssue | PocV21EventType::SoShipment
        ) || (matches!(e.event_type, PocV21EventType::InvAdjust) && e.qty < 0))
        .map(|e| (e.issue_id, method_of(e.sku_id)))
        .collect();

    let mut replayed_correlation_ids: Vec<pgrx::Uuid> = Vec::new();
    if !dedup_pairs.is_empty() {
        let issue_ids: Vec<i64> = dedup_pairs.iter().map(|p| p.0).collect();
        let methods_owned: Vec<String> = dedup_pairs.iter().map(|p| p.1.to_string()).collect();

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

    // STEP 3: snapshot hydration. FIFO loads cost_layers; AVG loads
    // avg_pool_state. Each only hydrates the pools its method covers
    // (the dispatch on event.sku_id at Step 4 reads the right slot).
    let mut snapshot = PocV21Snapshot::default();
    for (sku, &m) in method_for_sku.iter() {
        snapshot.method_assignments.insert(*sku, m);
    }
    if !sku_pool_keys.is_empty() {
        let fifo_pools: Vec<(i64, i64)> = sku_pool_keys
            .iter()
            .filter(|(s, _)| method_of(*s) == "fifo")
            .copied()
            .collect();
        if !fifo_pools.is_empty() {
            let sku_ids: Vec<i64> = fifo_pools.iter().map(|k| k.0).collect();
            let location_ids: Vec<i64> = fifo_pools.iter().map(|k| k.1).collect();
            hydrate_fifo_layers(&mut snapshot, &sku_ids, &location_ids)?;
        }
        let avg_pools: Vec<(i64, i64)> = sku_pool_keys
            .iter()
            .filter(|(s, _)| method_of(*s) == "avg")
            .copied()
            .collect();
        if !avg_pools.is_empty() {
            let sku_ids: Vec<i64> = avg_pools.iter().map(|k| k.0).collect();
            let location_ids: Vec<i64> = avg_pools.iter().map(|k| k.1).collect();
            hydrate_avg_pools(&mut snapshot, &sku_ids, &location_ids)?;
        }
        let std_pools: Vec<(i64, i64)> = sku_pool_keys
            .iter()
            .filter(|(s, _)| method_of(*s) == "std")
            .copied()
            .collect();
        if !std_pools.is_empty() {
            let sku_ids: Vec<i64> = std_pools.iter().map(|k| k.0).collect();
            let location_ids: Vec<i64> = std_pools.iter().map(|k| k.1).collect();
            hydrate_standard_costs(&mut snapshot, &sku_ids, &location_ids)?;
        }
    }

    // STEP 4: per-event dispatch. Sort by chronological key, then
    // route each event to its SKU's method's apply_one. Methods
    // interleave freely within one batch — AVG event then FIFO event
    // then AVG event is supported (each method touches its own
    // snapshot slot).
    let mut sorted_events = events_to_plan.clone();
    sorted_events.sort_by_key(|e| {
        (
            e.business_date_jdate,
            e.doc_chrono,
            e.document_id,
            e.sub_priority,
        )
    });
    let mut result = PocV21ApplyResult::default();
    for event in &sorted_events {
        let m = method_of(event.sku_id);
        let event_result = match m {
            "avg" => AVG_METHOD.apply_one(event, &mut snapshot, &mut result),
            "std" => STANDARD_METHOD.apply_one(event, &mut snapshot, &mut result),
            _ => FIFO_METHOD.apply_one(event, &mut snapshot, &mut result),
        };
        result.per_event.push(event_result);
    }

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

    // STEP 5: bulk UNNEST inserts. Six target tables; this is the
    // load-bearing throughput primitive measured by P5 (spec §4.2):
    // 5a posting_lines, 5b cost_layers, 5c cost_depletions,
    // 5c' cost_consumptions, 5d posting_line_inventory, 5e
    // avg_pool_state UPSERT. Each writes one SuperBatch's rows in
    // a single bulk statement; per-row chatter is gone.
    //
    // posting_lines + cost_layers use RETURNING to surface BIGSERIAL
    // ids for downstream linking (posting_line_id → inventory; new
    // layer_id → inv.layer_id for receipt-side rows). PG returns
    // RETURNING rows in insertion order; the input array order is
    // preserved.
    //
    // posting_line_inventory carries posting_line_ordinal (the index
    // into result.posting_line_inserts where the originating
    // posting line lives). We build an `ordinal → posting_line_id`
    // resolver from the RETURNING output, then UNNEST-INSERT the
    // inventory rows with resolved ids.

    let (posting_line_ords_for_rows, posting_line_input): (
        Vec<usize>,
        Vec<&crate::cost_method::PocV21PostingLineRow>,
    ) = result
        .posting_line_inserts
        .iter()
        .enumerate()
        .filter(|(_, r)| succeeded_set.contains(&r.correlation_id))
        .map(|(i, r)| (i, r))
        .unzip();

    let mut ordinal_to_pl_id: Vec<i64> = vec![0; result.posting_line_inserts.len()];
    if !posting_line_input.is_empty() {
        // Build parallel arrays from the input rows.
        let bd: Vec<i32> = posting_line_input.iter().map(|r| r.business_date_jdate).collect();
        let chrono: Vec<i64> = posting_line_input.iter().map(|r| r.doc_chrono).collect();
        let doc_id: Vec<i64> = posting_line_input.iter().map(|r| r.document_id).collect();
        let sub_pri: Vec<i32> = posting_line_input.iter().map(|r| r.sub_priority).collect();
        let event_type_v: Vec<String> = posting_line_input.iter().map(|r| r.event_type.to_string()).collect();
        let amount: Vec<i64> = posting_line_input.iter().map(|r| r.amount).collect();
        let debit_acct: Vec<Option<i64>> = posting_line_input.iter().map(|r| r.debit_account).collect();
        let credit_acct: Vec<Option<i64>> = posting_line_input.iter().map(|r| r.credit_account).collect();
        let corr: Vec<pgrx::Uuid> = posting_line_input.iter().map(|r| r.correlation_id).collect();
        let user_xid: Vec<i64> = posting_line_input.iter().map(|r| r.user_tx_xid as i64).collect();

        let returned: Vec<i64> = Spi::connect(|client| -> Option<Vec<i64>> {
            let mut out: Vec<i64> = Vec::with_capacity(posting_line_input.len());
            let mut t = client
                .select(
                    "INSERT INTO poc_v21_posting_lines \
                       (business_date, doc_chrono, document_id, sub_priority, event_type, amount, \
                        debit_account, credit_account, correlation_id, user_tx_xid, committer_tx_id, superbatch_id) \
                     SELECT '2000-01-01'::date + bd, chrono, doc_id, sub_pri, et, amt, deb, cred, c, uxid::text::xid8, $11, $12 \
                       FROM UNNEST($1::int[], $2::bigint[], $3::bigint[], $4::int[], $5::text[], $6::bigint[], \
                                   $7::bigint[], $8::bigint[], $9::uuid[], $10::bigint[]) \
                            AS t(bd, chrono, doc_id, sub_pri, et, amt, deb, cred, c, uxid) \
                     RETURNING posting_line_id",
                    None,
                    &[
                        bd.into(),
                        chrono.into(),
                        doc_id.into(),
                        sub_pri.into(),
                        event_type_v.into(),
                        amount.into(),
                        debit_acct.into(),
                        credit_acct.into(),
                        corr.into(),
                        user_xid.into(),
                        (committer_tx_id as i64).into(),
                        (superbatch_id as i64).into(),
                    ],
                )
                .ok()?;
            while let Some(row) = t.next() {
                if let Ok(Some(id)) = row.get::<i64>(1) {
                    out.push(id);
                }
            }
            Some(out)
        })
        .ok_or_else(|| "posting_lines bulk INSERT failed".to_string())?;
        if returned.len() != posting_line_input.len() {
            return Err(format!(
                "posting_lines: expected {} ids, got {}",
                posting_line_input.len(),
                returned.len()
            ));
        }
        for (ordinal, id) in posting_line_ords_for_rows.iter().zip(returned.iter()) {
            ordinal_to_pl_id[*ordinal] = *id;
        }
    }

    // 5b. cost_layers — bulk INSERT RETURNING layer_id. We need
    // per-correlation queues of new layer ids so posting_line_inventory
    // can pop one per receipt-side row.
    let mut new_layer_ids_by_corr: HashMap<pgrx::Uuid, Vec<i64>> = HashMap::new();
    if !layer_rows.is_empty() {
        let sku: Vec<i64> = layer_rows.iter().map(|r| r.sku_id).collect();
        let loc: Vec<i64> = layer_rows.iter().map(|r| r.location_id).collect();
        let qty: Vec<i64> = layer_rows.iter().map(|r| r.qty).collect();
        let unit_cost: Vec<i64> = layer_rows.iter().map(|r| r.unit_cost).collect();
        let born_at: Vec<i64> = layer_rows.iter().map(|r| r.born_at_micros).collect();
        let born_seq: Vec<i64> = layer_rows.iter().map(|r| r.born_seq).collect();
        let source_kind: Vec<String> = layer_rows.iter().map(|r| r.source_kind.to_string()).collect();
        let source_ref: Vec<Option<i64>> = layer_rows.iter().map(|r| r.source_ref).collect();
        let corr: Vec<pgrx::Uuid> = layer_rows.iter().map(|r| r.correlation_id).collect();
        let user_xid: Vec<i64> = layer_rows.iter().map(|r| r.user_tx_xid as i64).collect();

        let returned_pairs: Vec<(i64, pgrx::Uuid)> = Spi::connect(
            |client| -> Option<Vec<(i64, pgrx::Uuid)>> {
                let mut out: Vec<(i64, pgrx::Uuid)> = Vec::with_capacity(layer_rows.len());
                let mut t = client
                    .select(
                        "INSERT INTO poc_v21_cost_layers \
                           (sku_id, location_id, qty, unit_cost, born_at, born_seq, source_kind, source_ref, correlation_id, user_tx_xid, committer_tx_id, superbatch_id) \
                         SELECT sku, loc, q, u, to_timestamp(b::double precision / 1000000), bs, sk, sr, c, uxid::text::xid8, $11, $12 \
                           FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[], $6::bigint[], \
                                       $7::text[], $8::bigint[], $9::uuid[], $10::bigint[]) \
                                AS t(sku, loc, q, u, b, bs, sk, sr, c, uxid) \
                         RETURNING layer_id, correlation_id",
                        None,
                        &[
                            sku.into(),
                            loc.into(),
                            qty.into(),
                            unit_cost.into(),
                            born_at.into(),
                            born_seq.into(),
                            source_kind.into(),
                            source_ref.into(),
                            corr.into(),
                            user_xid.into(),
                            (committer_tx_id as i64).into(),
                            (superbatch_id as i64).into(),
                        ],
                    )
                    .ok()?;
                while let Some(row) = t.next() {
                    let id = row.get::<i64>(1).ok()??;
                    let c = row.get::<pgrx::Uuid>(2).ok()??;
                    out.push((id, c));
                }
                Some(out)
            },
        )
        .ok_or_else(|| "cost_layers bulk INSERT failed".to_string())?;
        for (id, c) in returned_pairs {
            new_layer_ids_by_corr.entry(c).or_default().push(id);
        }
    }

    // 5c. cost_depletions — bulk UNNEST INSERT. No RETURNING needed.
    if !depletion_rows.is_empty() {
        let layer: Vec<i64> = depletion_rows.iter().map(|r| r.layer_id).collect();
        let qty: Vec<i64> = depletion_rows.iter().map(|r| r.qty).collect();
        let unit_cost: Vec<i64> = depletion_rows.iter().map(|r| r.unit_cost).collect();
        let consumed_at: Vec<i64> = depletion_rows.iter().map(|r| r.consumed_at_micros).collect();
        let consumed_seq: Vec<i64> = depletion_rows.iter().map(|r| r.consumed_seq).collect();
        let issue: Vec<i64> = depletion_rows.iter().map(|r| r.issue_id).collect();
        let method: Vec<String> = depletion_rows.iter().map(|r| r.method_used.to_string()).collect();
        let corr: Vec<pgrx::Uuid> = depletion_rows.iter().map(|r| r.correlation_id).collect();
        let user_xid: Vec<i64> = depletion_rows.iter().map(|r| r.user_tx_xid as i64).collect();

        Spi::run_with_args(
            "INSERT INTO poc_v21_cost_depletions \
               (layer_id, qty, unit_cost, consumed_at, consumed_seq, issue_id, method_used, correlation_id, user_tx_xid, committer_tx_id, superbatch_id) \
             SELECT layer, q, u, to_timestamp(ca::double precision / 1000000), cs, i, m, c, uxid::text::xid8, $10, $11 \
               FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[], $6::bigint[], \
                           $7::text[], $8::uuid[], $9::bigint[]) \
                    AS t(layer, q, u, ca, cs, i, m, c, uxid)",
            &[
                layer.into(),
                qty.into(),
                unit_cost.into(),
                consumed_at.into(),
                consumed_seq.into(),
                issue.into(),
                method.into(),
                corr.into(),
                user_xid.into(),
                (committer_tx_id as i64).into(),
                (superbatch_id as i64).into(),
            ],
        )
        .map_err(|e| format!("cost_depletions bulk INSERT: {e}"))?;
    }

    // 5c'. cost_consumptions — bulk UNNEST with ON CONFLICT
    // (issue_id, method_used) DO NOTHING for replay-safety.
    let consumption_rows: Vec<_> = result
        .consumption_inserts
        .iter()
        .filter(|r| succeeded_set.contains(&r.correlation_id))
        .cloned()
        .collect();
    if !consumption_rows.is_empty() {
        let sku: Vec<i64> = consumption_rows.iter().map(|r| r.sku_id).collect();
        let loc: Vec<i64> = consumption_rows.iter().map(|r| r.location_id).collect();
        let qty: Vec<i64> = consumption_rows.iter().map(|r| r.qty).collect();
        let unit_cost: Vec<i64> = consumption_rows.iter().map(|r| r.unit_cost).collect();
        let consumed_at: Vec<i64> = consumption_rows.iter().map(|r| r.consumed_at_micros).collect();
        let consumed_seq: Vec<i64> = consumption_rows.iter().map(|r| r.consumed_seq).collect();
        let issue: Vec<i64> = consumption_rows.iter().map(|r| r.issue_id).collect();
        let method: Vec<String> = consumption_rows.iter().map(|r| r.method_used.to_string()).collect();
        let corr: Vec<pgrx::Uuid> = consumption_rows.iter().map(|r| r.correlation_id).collect();
        let user_xid: Vec<i64> = consumption_rows.iter().map(|r| r.user_tx_xid as i64).collect();

        Spi::run_with_args(
            "INSERT INTO poc_v21_cost_consumptions \
               (sku_id, location_id, qty, applied_unit_cost, consumed_at, consumed_seq, issue_id, method_used, correlation_id, user_tx_xid, committer_tx_id, superbatch_id) \
             SELECT sku, loc, q, u, to_timestamp(ca::double precision / 1000000), cs, i, m, c, uxid::text::xid8, $11, $12 \
               FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[], $6::bigint[], \
                           $7::bigint[], $8::text[], $9::uuid[], $10::bigint[]) \
                    AS t(sku, loc, q, u, ca, cs, i, m, c, uxid) \
             ON CONFLICT (issue_id, method_used) DO NOTHING",
            &[
                sku.into(),
                loc.into(),
                qty.into(),
                unit_cost.into(),
                consumed_at.into(),
                consumed_seq.into(),
                issue.into(),
                method.into(),
                corr.into(),
                user_xid.into(),
                (committer_tx_id as i64).into(),
                (superbatch_id as i64).into(),
            ],
        )
        .map_err(|e| format!("cost_consumptions bulk INSERT: {e}"))?;
    }

    // 5d. posting_line_inventory — resolve posting_line_id via the
    // ordinal map + layer_id via the per-correlation new-layer queue;
    // bulk UNNEST INSERT the resolved rows.
    let mut inv_pl_ids: Vec<i64> = Vec::new();
    let mut inv_sku: Vec<i64> = Vec::new();
    let mut inv_loc: Vec<i64> = Vec::new();
    let mut inv_qty: Vec<i64> = Vec::new();
    let mut inv_layer: Vec<Option<i64>> = Vec::new();
    for (inv_row, corr) in &posting_inventory_rows {
        let pl_id = ordinal_to_pl_id[inv_row.posting_line_ordinal];
        if pl_id == 0 {
            continue;
        }
        let layer_id = if let Some(real_id) = inv_row.layer_id {
            Some(real_id)
        } else {
            new_layer_ids_by_corr.get_mut(corr).and_then(|v| {
                if v.is_empty() {
                    None
                } else {
                    Some(v.remove(0))
                }
            })
        };
        inv_pl_ids.push(pl_id);
        inv_sku.push(inv_row.sku_id);
        inv_loc.push(inv_row.location_id);
        inv_qty.push(inv_row.qty);
        inv_layer.push(layer_id);
    }
    if !inv_pl_ids.is_empty() {
        Spi::run_with_args(
            "INSERT INTO poc_v21_posting_line_inventory \
               (posting_line_id, sku_id, location_id, qty, layer_id) \
             SELECT pl, sku, loc, q, layer \
               FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[]) \
                    AS t(pl, sku, loc, q, layer)",
            &[
                inv_pl_ids.into(),
                inv_sku.into(),
                inv_loc.into(),
                inv_qty.into(),
                inv_layer.into(),
            ],
        )
        .map_err(|e| format!("posting_line_inventory bulk INSERT: {e}"))?;
    }

    // 5e. avg_pool_state UPSERT — persist running average state for
    // every AVG-method pool that this batch touched. Race-free: pool_lock
    // held in Step 2 covers concurrent committer access on the same pool.
    // "NEVER reconstruct AVG from cost_layers history" — this UPSERT is
    // the contract (spec §1.3).
    let mut avg_dirty_pools: Vec<(i64, i64, i64, i64)> = Vec::new(); // (sku, loc, avg_unit_cost, total_qty)
    for ((sku, loc), pool) in snapshot.sku_pools.iter() {
        if pool.avg_dirty {
            avg_dirty_pools.push((*sku, *loc, pool.avg_unit_cost, pool.avg_total_qty));
        }
    }
    if !avg_dirty_pools.is_empty() {
        let sku_ids: Vec<i64> = avg_dirty_pools.iter().map(|t| t.0).collect();
        let loc_ids: Vec<i64> = avg_dirty_pools.iter().map(|t| t.1).collect();
        let units: Vec<i64> = avg_dirty_pools.iter().map(|t| t.2).collect();
        let qtys: Vec<i64> = avg_dirty_pools.iter().map(|t| t.3).collect();
        Spi::run_with_args(
            "INSERT INTO poc_v21_avg_pool_state \
               (sku_id, location_id, avg_unit_cost, total_qty, last_updated_at, last_committer_tx_id) \
             SELECT sku_id, location_id, avg_unit_cost, total_qty, now(), $5::bigint \
               FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[]) \
                    AS t(sku_id, location_id, avg_unit_cost, total_qty) \
             ON CONFLICT (sku_id, location_id) DO UPDATE SET \
               avg_unit_cost=EXCLUDED.avg_unit_cost, \
               total_qty=EXCLUDED.total_qty, \
               last_updated_at=EXCLUDED.last_updated_at, \
               last_committer_tx_id=EXCLUDED.last_committer_tx_id",
            &[
                sku_ids.into(),
                loc_ids.into(),
                units.into(),
                qtys.into(),
                (committer_tx_id as i64).into(),
            ],
        )
        .map_err(|e| format!("avg_pool_state UPSERT: {e}"))?;
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

fn hydrate_standard_costs(
    snapshot: &mut PocV21Snapshot,
    sku_ids: &[i64],
    location_ids: &[i64],
) -> Result<(), String> {
    // DISTINCT ON returns the latest effective row per (sku, location).
    // `effective_from <= now()` filter excludes future-dated rolls.
    Spi::connect(|client| -> Result<(), String> {
        let mut t = client
            .select(
                "SELECT DISTINCT ON (sku_id, location_id) \
                        sku_id, location_id, unit_cost \
                   FROM poc_v21_standard_costs \
                  WHERE (sku_id, location_id) IN \
                        (SELECT sku_id, location_id FROM UNNEST($1::bigint[], $2::bigint[]) AS t(sku_id, location_id)) \
                    AND effective_from <= now() \
                  ORDER BY sku_id, location_id, effective_from DESC",
                None,
                &[sku_ids.to_vec().into(), location_ids.to_vec().into()],
            )
            .map_err(|e| format!("snapshot SELECT standard_costs: {e}"))?;
        while let Some(row) = t.next() {
            let sku_id: i64 = row.get::<i64>(1).map_err(|e| format!("sku_id: {e}"))?.unwrap_or(0);
            let location_id: i64 = row.get::<i64>(2).map_err(|e| format!("location_id: {e}"))?.unwrap_or(0);
            let unit_cost: i64 = row.get::<i64>(3).map_err(|e| format!("unit_cost: {e}"))?.unwrap_or(0);
            snapshot.standard_costs.insert((sku_id, location_id), unit_cost);
        }
        Ok(())
    })?;
    Ok(())
}

fn hydrate_avg_pools(
    snapshot: &mut PocV21Snapshot,
    sku_ids: &[i64],
    location_ids: &[i64],
) -> Result<(), String> {
    // Load avg_unit_cost + total_qty for AVG-method pools. Pools with
    // no row in avg_pool_state get default (0, 0) — first receipt
    // initializes; first consumption hits the insufficient-inventory
    // path.
    Spi::connect(|client| -> Result<(), String> {
        let mut t = client
            .select(
                "SELECT sku_id, location_id, avg_unit_cost, total_qty \
                   FROM poc_v21_avg_pool_state \
                  WHERE (sku_id, location_id) IN \
                        (SELECT sku_id, location_id FROM UNNEST($1::bigint[], $2::bigint[]) AS t(sku_id, location_id))",
                None,
                &[sku_ids.to_vec().into(), location_ids.to_vec().into()],
            )
            .map_err(|e| format!("snapshot SELECT avg_pool_state: {e}"))?;
        while let Some(row) = t.next() {
            let sku_id: i64 = row.get::<i64>(1).map_err(|e| format!("sku_id: {e}"))?.unwrap_or(0);
            let location_id: i64 = row.get::<i64>(2).map_err(|e| format!("location_id: {e}"))?.unwrap_or(0);
            let avg_unit_cost: i64 = row.get::<i64>(3).map_err(|e| format!("avg_unit_cost: {e}"))?.unwrap_or(0);
            let total_qty: i64 = row.get::<i64>(4).map_err(|e| format!("total_qty: {e}"))?.unwrap_or(0);

            let pool = snapshot
                .sku_pools
                .entry((sku_id, location_id))
                .or_insert_with(SkuPoolState::default);
            pool.avg_unit_cost = avg_unit_cost;
            pool.avg_total_qty = total_qty;
        }
        Ok(())
    })?;
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
