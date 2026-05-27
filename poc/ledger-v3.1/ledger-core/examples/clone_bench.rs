//! Microbench for acct-yojk.12: measure `Snapshot::clone()` cost (the committer's
//! per-submission drop-and-continue trial clone) so the perf-gated decision has a
//! number. Run with: cargo run --release --example clone_bench -p ledger-core
//!
//! Measured (release): ~96 ns (1 pool) / ~121 ns (3) / ~180 ns (8). For comparison
//! a single-row INSERT into `trx` (with its UNIQUE index), in-backend, is ~6.4 µs,
//! and a submission does ~3 such SPI INSERTs (insert_trx / insert_trx_lines /
//! insert_posting_lines) ≈ 19 µs before pool_lock/hydrate amortization. The clone
//! is therefore ~0.5–1% of per-submission committer cost — the SPI write path
//! dominates by ~100×, so the clone-per-trial stays (won't-fix). If recalc/close
//! later materializes layers (growing `pools`), re-run this to re-check the gate.

use std::time::Instant;

use ledger_core::method::{PoolMethod, ProvisionalBasis};
use ledger_core::snapshot::{PoolStateRow, Snapshot};

fn build_snapshot(n_pools: i64) -> Snapshot {
    let mut s = Snapshot::default();
    for p in 1..=n_pools {
        s.pools.insert(p, vec![PoolStateRow { layer_id: 0, qty: 1_000, unit_cost: 50 }]);
        s.method_of.insert(p, PoolMethod::Fifo);
        s.provisional_basis_of.insert(p, ProvisionalBasis::RunningAvg);
        s.sku_location_of.insert(p, (p, p));
        s.standard_cost_of.insert(p, 50);
    }
    s
}

fn bench(n_pools: i64, iters: u64) -> f64 {
    let s = build_snapshot(n_pools);
    // Warm up.
    let mut sink = 0usize;
    for _ in 0..10_000 {
        sink += std::hint::black_box(s.clone()).pools.len();
    }
    let t0 = Instant::now();
    for _ in 0..iters {
        sink += std::hint::black_box(s.clone()).pools.len();
    }
    let elapsed = t0.elapsed();
    std::hint::black_box(sink);
    elapsed.as_nanos() as f64 / iters as f64
}

fn main() {
    let iters: u64 = 5_000_000;
    for n in [1i64, 3, 8] {
        let ns = bench(n, iters);
        println!("Snapshot::clone() with {n} pool(s): {ns:.1} ns/clone  ({iters} iters)");
    }
}
