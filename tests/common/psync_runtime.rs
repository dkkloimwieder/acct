//! acct-c4p — Shape-L pseudo-sync runtime, extracted from
//! `tests/load_outbox_pseudo_sync.rs` as a reusable module.
//!
//! ## Architecture
//!
//! Writer-side flow per ledger call:
//!   1. Caller invokes a `*_psync` SQL entry point (e.g. post_so_ship_psync)
//!      which INSERTs to `ledger_outbox` (events JSONB) and RETURNs both
//!      the document_id and the outbox row id.
//!   2. Caller awaits `Dispatcher::wait_for(outbox_id, timeout)`.
//!   3. Drainer (single, background task) pulls pending rows, calls
//!      post_posting_lines in its own tx, emits `pg_notify(channel, {id,
//!      status, sqlstate?})` inside the drain tx (delivered on commit).
//!   4. Listener (background task) subscribes to channel, dispatches per-id
//!      outcomes via in-process oneshot channels to waiting writers.
//!
//! ## Consistency model
//!
//! tx 1 (caller's: document inserts + outbox row) commits BEFORE tx 2
//! (drainer's: post_posting_lines). If tx 2 fails (e.g. P0001, 23514),
//! tx 1 documents are orphaned — caller receives Outcome { status: "failed",
//! sqlstate: Some("..."), } and must surface/compensate.
//!
//! ## Why this lives in tests/common, not src/
//!
//! Phase 0/1 has no application tier — the runtime is exercised via the
//! integration-test harness. A production app-tier deployment of c4p
//! (process supervision, restart semantics, multi-region drainer, etc.)
//! is a follow-up task.

use sqlx::PgPool;
use sqlx::postgres::PgListener;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::common::outbox_worker::{DrainConfig, super_batched_drain_loop};

#[derive(Clone, Debug)]
pub struct Outcome {
    pub status: String,
    pub sqlstate: Option<String>,
}

enum SlotState {
    WaitingForNotify(oneshot::Sender<Outcome>),
    NotifyArrived(Outcome),
}

/// In-process rendezvous between the listener task (which receives NOTIFYs
/// from PG) and the writer tasks (which await per-id outcomes after
/// committing their outbox INSERT).
#[derive(Clone)]
pub struct Dispatcher {
    slots: Arc<Mutex<HashMap<i64, SlotState>>>,
}

impl Dispatcher {
    pub fn new() -> Self {
        Self {
            slots: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Called by the listener task per inbound NOTIFY.
    pub fn deliver(&self, id: i64, outcome: Outcome) {
        let mut g = self.slots.lock().unwrap();
        match g.remove(&id) {
            Some(SlotState::WaitingForNotify(tx)) => {
                let _ = tx.send(outcome);
            }
            Some(SlotState::NotifyArrived(_)) | None => {
                g.insert(id, SlotState::NotifyArrived(outcome));
            }
        }
    }

    /// Called by the writer after INSERT-commit. If a NOTIFY already
    /// arrived (race), returns immediately. Otherwise installs a oneshot
    /// and awaits up to `timeout`.
    pub async fn wait_for(&self, id: i64, timeout: Duration) -> Result<Outcome, ()> {
        let rx = {
            let mut g = self.slots.lock().unwrap();
            match g.remove(&id) {
                Some(SlotState::NotifyArrived(o)) => return Ok(o),
                Some(SlotState::WaitingForNotify(_)) => {
                    panic!("dispatcher slot collision for id {id}");
                }
                None => {
                    let (tx, rx) = oneshot::channel();
                    g.insert(id, SlotState::WaitingForNotify(tx));
                    rx
                }
            }
        };
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(o)) => Ok(o),
            Ok(Err(_)) | Err(_) => {
                let mut g = self.slots.lock().unwrap();
                g.remove(&id);
                Err(())
            }
        }
    }
}

/// Spawned runtime: listener task + drainer task + the Dispatcher they
/// share. Hold this for the test's lifetime; on Drop the tasks are
/// signaled to stop.
pub struct PsyncRuntime {
    pub dispatcher: Dispatcher,
    pub channel: String,
    listener_handle: Option<JoinHandle<()>>,
    drainer_handle: Option<JoinHandle<sqlx::Result<crate::common::outbox_worker::DrainStats>>>,
    listener_stop: Arc<AtomicBool>,
    drain_to_empty: Arc<AtomicBool>,
    hard_stop: Arc<AtomicBool>,
}

impl PsyncRuntime {
    /// Spawn the listener + drainer tasks; return a runtime handle. The
    /// caller's pool must have enough connections for both the listener
    /// (1 dedicated) and the drainer (1 dedicated) on top of writer
    /// connections.
    ///
    /// `channel` is the pg_notify channel (must match the drainer's
    /// notify_channel). Default: "ledger_outbox_done".
    pub async fn spawn(pool: PgPool, channel: impl Into<String>) -> Self {
        let channel = channel.into();
        let dispatcher = Dispatcher::new();
        let listener_stop = Arc::new(AtomicBool::new(false));
        let drain_to_empty = Arc::new(AtomicBool::new(false));
        let hard_stop = Arc::new(AtomicBool::new(false));

        // Listener task: subscribe to channel, dispatch outcomes.
        let listener_handle = {
            let pool = pool.clone();
            let dispatcher = dispatcher.clone();
            let stop = listener_stop.clone();
            let channel = channel.clone();
            tokio::spawn(async move {
                let mut listener = match PgListener::connect_with(&pool).await {
                    Ok(l) => l,
                    Err(e) => {
                        eprintln!("psync listener connect error: {e}");
                        return;
                    }
                };
                if let Err(e) = listener.listen(&channel).await {
                    eprintln!("psync listener listen error: {e}");
                    return;
                }
                loop {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let n = match tokio::time::timeout(
                        Duration::from_millis(200),
                        listener.recv(),
                    )
                    .await
                    {
                        Ok(Ok(n)) => n,
                        Ok(Err(e)) => {
                            eprintln!("psync listener recv error: {e}");
                            break;
                        }
                        Err(_) => continue,
                    };
                    let payload = n.payload();
                    let v: serde_json::Value = match serde_json::from_str(payload) {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!("psync parse error: {e} payload={payload}");
                            continue;
                        }
                    };
                    let id = v.get("id").and_then(|x| x.as_i64()).unwrap_or(-1);
                    if id < 0 {
                        continue;
                    }
                    let status = v
                        .get("status")
                        .and_then(|x| x.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let sqlstate = v
                        .get("sqlstate")
                        .and_then(|x| x.as_str())
                        .map(String::from);
                    dispatcher.deliver(id, Outcome { status, sqlstate });
                }
            })
        };

        // Drainer task: super-batched drain with pg_notify per row outcome.
        let drainer_handle = {
            let pool = pool.clone();
            let cfg = DrainConfig {
                batch_size: 1000,
                idle_sleep_ms: 1,
                notify_channel: Some(channel.clone()),
            };
            let stop = drain_to_empty.clone();
            let hs = hard_stop.clone();
            tokio::spawn(async move { super_batched_drain_loop(pool, cfg, stop, hs).await })
        };

        Self {
            dispatcher,
            channel,
            listener_handle: Some(listener_handle),
            drainer_handle: Some(drainer_handle),
            listener_stop,
            drain_to_empty,
            hard_stop,
        }
    }

    /// Cleanly stop both tasks. Waits for the drainer to flush pending
    /// rows (up to `grace`), then forces stop. Listener stops first.
    pub async fn shutdown(mut self, grace: Duration) {
        self.listener_stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.listener_handle.take() {
            let _ = h.await;
        }
        self.drain_to_empty.store(true, Ordering::Relaxed);
        if let Some(h) = self.drainer_handle.take() {
            let _ = tokio::time::timeout(grace, h).await;
        }
        // If grace expired with no completion, force.
        self.hard_stop.store(true, Ordering::Relaxed);
    }
}

impl Drop for PsyncRuntime {
    fn drop(&mut self) {
        self.listener_stop.store(true, Ordering::Relaxed);
        self.drain_to_empty.store(true, Ordering::Relaxed);
        self.hard_stop.store(true, Ordering::Relaxed);
        // JoinHandles abort on drop via tokio's runtime cleanup.
        if let Some(h) = self.listener_handle.take() {
            h.abort();
        }
        if let Some(h) = self.drainer_handle.take() {
            h.abort();
        }
    }
}
