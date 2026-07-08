//! Offline strict-FIFO/LIFO replay oracle (acct-0at4.7).
//!
//! Path C records FIFO/LIFO depletions at a PROVISIONAL unit_cost (the running
//! average, or the standard cost) and touches only the aggregate row — it never
//! walks layers on the hot path (design-v3.1 §3.5). Its entire premise is that the
//! recorded `trx_line` stream is *sufficient* to reconstruct authoritative
//! strict-FIFO/LIFO cost later (recalc/close, §7). This binary tests that premise
//! and measures the provisional-vs-true variance the business go/no-go consumes.
//!
//! Two arms:
//!  - RECORDED (provisional): the EXACT shipped transform,
//!    `ledger_core::plan_apply_provisional`. No re-implementation — this is the
//!    code the SQL hot path mirrors (SPIKE-B proved byte-identical).
//!  - TRUE (strict): the throwaway layer-walk oracle below. Production ledger-core
//!    has only `MethodMismatch` stubs for strict FIFO/LIFO (§8), which is exactly
//!    why this oracle has to exist to close the loop.
//!
//! Subcommands:
//!   synth            in-memory volatility × basis × method sweep -> variance table
//!   real <tsv-dir>   replay a real harness dump (pool/layers/lines TSV): reproduce
//!                    the recorded provisional from the id-ordered stream, report
//!                    reconstruction fidelity + ordering diagnostics + the native
//!                    variance anchor.

use std::collections::{HashMap, VecDeque};

use chrono::{TimeZone, Utc};
use ledger_core::{
    banker_div, plan_apply_provisional, LineType, PoolMethod, PoolStateRow, PostingAccounts,
    ProvisionalBasis, Snapshot, TrxLineRequest,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

// ─────────────────────────────────────────────────────────────────────────────
// Strict FIFO/LIFO oracle (the throwaway part). Authoritative layer-walk cost.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
struct Layer {
    qty: i64,
    unit_cost: i64,
}

/// A strict layered pool. FIFO consumes the oldest layer first (front); LIFO the
/// newest (back). A depletion's authoritative unit_cost is the banker-rounded
/// weighted average of the layer costs it consumes — the same 1e-6 fixed-precision
/// rounding the ledger uses everywhere (§3.0).
struct StrictPool {
    layers: VecDeque<Layer>,
    lifo: bool,
}

impl StrictPool {
    fn new(lifo: bool) -> Self {
        Self { layers: VecDeque::new(), lifo }
    }

    fn on_hand(&self) -> i64 {
        self.layers.iter().map(|l| l.qty).sum()
    }

    fn receipt(&mut self, qty: i64, unit_cost: i64) {
        if qty > 0 {
            self.layers.push_back(Layer { qty, unit_cost });
        }
    }

    /// Deplete `qty` (positive). Returns the authoritative strict unit_cost, or
    /// `None` if on-hand is insufficient — the "ill-defined" case (a depletion the
    /// stream cannot cost against any layer). Under alt-C allow-negative that case
    /// is exactly where strict reconstruction breaks.
    fn deplete(&mut self, qty: i64) -> Option<i64> {
        if qty <= 0 || self.on_hand() < qty {
            return None;
        }
        let mut remaining = qty;
        let mut value: i128 = 0;
        while remaining > 0 {
            // Peek the consuming end.
            let front = !self.lifo;
            let layer = if front { self.layers.front_mut() } else { self.layers.back_mut() }?;
            let take = remaining.min(layer.qty);
            value += take as i128 * layer.unit_cost as i128;
            layer.qty -= take;
            remaining -= take;
            if layer.qty == 0 {
                if front {
                    self.layers.pop_front();
                } else {
                    self.layers.pop_back();
                }
            }
        }
        Some(banker_div(value, qty))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Recorded (provisional) arm — drives the SHIPPED ledger-core transform.
// ─────────────────────────────────────────────────────────────────────────────

fn dummy_accounts() -> PostingAccounts {
    // The oracle never inspects journal legs; any consistent nonzero set works.
    PostingAccounts {
        receipt: (1, 2),
        transfer: (2, 1),
        build: (1, 2),
        scrap: (2, 1),
        adjustment: (1, 2),
        revaluation: (1, 2),
        variance: Some(3),
    }
}

/// Per-pool provisional driver: holds a `Snapshot` and feeds one line at a time
/// through `plan_apply_provisional`, returning the recorded `trx_line.unit_cost`.
struct ProvisionalPool {
    snap: Snapshot,
    pool_id: i64,
}

impl ProvisionalPool {
    fn new(pool_id: i64, method: PoolMethod, basis: ProvisionalBasis, std_cost: i64) -> Self {
        let mut snap = Snapshot::default();
        snap.method_of.insert(pool_id, method);
        snap.provisional_basis_of.insert(pool_id, basis);
        snap.posting_accounts_of.insert(pool_id, dummy_accounts());
        snap.sku_location_of.insert(pool_id, (pool_id, 0));
        snap.standard_cost_of.insert(pool_id, std_cost);
        Self { snap, pool_id }
    }

    /// Seed the aggregate (opening balance) so both arms start identical.
    fn seed_aggregate(&mut self, qty: i64, unit_cost: i64) {
        let value_sum = (qty as i128 * unit_cost as i128) as i64;
        self.snap.pools.insert(
            self.pool_id,
            vec![PoolStateRow { layer_id: 0, qty, unit_cost, value_sum }],
        );
    }

    /// Feed one signed line; return the recorded unit_cost the ledger writes.
    /// Panics on a ledger error (e.g. InsufficientInventory) — the caller shapes
    /// the workload so the provisional aggregate never underflows.
    fn apply(&mut self, qty: i64, unit_cost: i64) -> i64 {
        let line_type =
            if qty >= 0 { LineType::PoReceiptLine } else { LineType::TransferShipmentLine };
        let line = TrxLineRequest { pool_id: self.pool_id, line_type, source_id: None, qty, unit_cost };
        let posted_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let res = plan_apply_provisional(&mut self.snap, &[line], posted_at)
            .unwrap_or_else(|e| panic!("plan_apply_provisional failed on pool {}: {e:?}", self.pool_id));
        res.trx_lines[0].unit_cost
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Variance statistics.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Default)]
struct Variance {
    /// signed (recorded - true) per depletion, micro-units.
    signed: Vec<i64>,
    /// mean true cost, for relative %.
    true_sum: i128,
}

impl Variance {
    fn push(&mut self, recorded: i64, truth: i64) {
        self.signed.push(recorded - truth);
        self.true_sum += truth as i128;
    }

    fn n(&self) -> usize {
        self.signed.len()
    }

    fn abs_sorted(&self) -> Vec<i64> {
        let mut v: Vec<i64> = self.signed.iter().map(|d| d.abs()).collect();
        v.sort_unstable();
        v
    }

    fn pct(v: &[i64], p: f64) -> i64 {
        if v.is_empty() {
            return 0;
        }
        let idx = ((v.len() as f64 - 1.0) * p).round() as usize;
        v[idx.min(v.len() - 1)]
    }

    fn mean_signed(&self) -> f64 {
        if self.signed.is_empty() {
            return 0.0;
        }
        self.signed.iter().map(|&d| d as i128).sum::<i128>() as f64 / self.signed.len() as f64
    }

    fn mean_abs(&self) -> f64 {
        if self.signed.is_empty() {
            return 0.0;
        }
        self.signed.iter().map(|&d| d.unsigned_abs() as u128).sum::<u128>() as f64
            / self.signed.len() as f64
    }

    fn mean_true(&self) -> f64 {
        if self.signed.is_empty() {
            return 0.0;
        }
        self.true_sum as f64 / self.signed.len() as f64
    }

    fn exact_frac(&self) -> f64 {
        if self.signed.is_empty() {
            return 0.0;
        }
        let exact = self.signed.iter().filter(|&&d| d == 0).count();
        exact as f64 / self.signed.len() as f64
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Synthetic sweep.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
enum Profile {
    Low,
    Med,
    High,
    Trend,
}

impl Profile {
    fn name(self) -> &'static str {
        match self {
            Profile::Low => "low (±2%)",
            Profile::Med => "med (±40%)",
            Profile::High => "high (±90%)",
            Profile::Trend => "trend (rising)",
        }
    }
}

const MEAN_COST: i64 = 500_000; // 0.5 currency units at 1e-6 fixed precision.

/// Draw a receipt unit_cost for `profile`. `receipts_so_far` drives the monotone
/// trend profile (front layers cheap, later layers dear — the worst case for a
/// running average that lags a rising cost front).
fn draw_cost(rng: &mut StdRng, profile: Profile, receipts_so_far: i64) -> i64 {
    match profile {
        Profile::Low => rng.random_range(490_000..=510_000),
        Profile::Med => rng.random_range(300_000..=700_000),
        Profile::High => rng.random_range(50_000..=950_000),
        Profile::Trend => (200_000 + receipts_so_far * 4_000).min(800_000),
    }
}

const N_POOLS: usize = 300;
const LINES_PER_POOL: usize = 300;
const DEPLETE_PCT: u32 = 50;
const BASE_QTY: i64 = 2_000;

/// One sweep cell: N_POOLS independent pools, each a base receipt + LINES_PER_POOL
/// interleaved receipts/depletions, run through BOTH arms in lockstep. Returns the
/// pooled depletion variance.
fn run_cell(method: PoolMethod, basis: ProvisionalBasis, profile: Profile, cell_seed: u64) -> Variance {
    let lifo = matches!(method, PoolMethod::Lifo);
    let mut var = Variance::default();

    for p in 0..N_POOLS {
        let mut rng = StdRng::seed_from_u64(cell_seed.wrapping_add(p as u64));
        let pool_id = (p + 1) as i64;

        // Standard basis prices depletions at a fixed standard set at the mean.
        let std_cost = MEAN_COST;
        let mut prov = ProvisionalPool::new(pool_id, method, basis, std_cost);
        let mut strict = StrictPool::new(lifo);

        // Opening balance: identical base receipt to both arms.
        let base_cost = match profile {
            Profile::Trend => 200_000,
            _ => MEAN_COST,
        };
        prov.seed_aggregate(BASE_QTY, base_cost);
        strict.receipt(BASE_QTY, base_cost);

        let mut receipts_so_far: i64 = 0;
        for _ in 0..LINES_PER_POOL {
            let on_hand = strict.on_hand();
            let deplete = on_hand > 1 && rng.random_range(0..100) < DEPLETE_PCT;
            if deplete {
                let qty = rng.random_range(1..=120.min(on_hand));
                let recorded = prov.apply(-qty, 0);
                let truth = strict.deplete(qty).expect("oracle on-hand checked > qty");
                var.push(recorded, truth);
            } else {
                let cost = draw_cost(&mut rng, profile, receipts_so_far);
                receipts_so_far += 1;
                let qty = rng.random_range(50..=150);
                prov.apply(qty, cost);
                strict.receipt(qty, cost);
            }
        }
    }
    var
}

fn method_name(m: PoolMethod) -> &'static str {
    match m {
        PoolMethod::Fifo => "fifo",
        PoolMethod::Lifo => "lifo",
        _ => "?",
    }
}

fn basis_name(b: ProvisionalBasis) -> &'static str {
    match b {
        ProvisionalBasis::RunningAvg => "running_avg",
        ProvisionalBasis::Standard => "standard",
    }
}

fn synth() {
    println!("## Synthetic variance sweep (provisional recorded vs strict-true)\n");
    println!(
        "N={} pools × {} lines/pool, deplete≈{}%, costs centred on {} µ-units (0.5 units). \
         Variance = recorded − true, in µ-units; rel = mean|Δ| / mean(true).\n",
        N_POOLS, LINES_PER_POOL, DEPLETE_PCT, MEAN_COST
    );
    println!("| method | basis | profile | depl. | exact% | mean Δ | mean\\|Δ\\| | p50\\|Δ\\| | p90\\|Δ\\| | p99\\|Δ\\| | max\\|Δ\\| | rel |");
    println!("|--------|-------|---------|------:|-------:|-------:|--------:|-------:|-------:|-------:|-------:|----:|");

    let methods = [PoolMethod::Fifo, PoolMethod::Lifo];
    let bases = [ProvisionalBasis::RunningAvg, ProvisionalBasis::Standard];
    let profiles = [Profile::Low, Profile::Med, Profile::High, Profile::Trend];

    let mut seed: u64 = 0x5EED_0A7_0000;
    for method in methods {
        for basis in bases {
            for profile in profiles {
                seed = seed.wrapping_add(0x1_0000);
                let var = run_cell(method, basis, profile, seed);
                let abs = var.abs_sorted();
                let rel = if var.mean_true() != 0.0 {
                    var.mean_abs() / var.mean_true() * 100.0
                } else {
                    0.0
                };
                println!(
                    "| {} | {} | {} | {} | {:.1} | {:+.0} | {:.0} | {} | {} | {} | {} | {:.2}% |",
                    method_name(method),
                    basis_name(basis),
                    profile.name(),
                    var.n(),
                    var.exact_frac() * 100.0,
                    var.mean_signed(),
                    var.mean_abs(),
                    Variance::pct(&abs, 0.50),
                    Variance::pct(&abs, 0.90),
                    Variance::pct(&abs, 0.99),
                    abs.last().copied().unwrap_or(0),
                    rel,
                );
            }
        }
    }
    println!();
}

// ─────────────────────────────────────────────────────────────────────────────
// Ordering / backdating falsification (design-v3.1 §14.5, §14.6).
//
// The premise "the trx_line stream reconstructs authoritative FIFO" hinges on
// ONE thing: can recalc recover the intended BUSINESS order of events? The stream
// carries two candidate order keys — trx_line.id (allocation ≈ commit order) and
// trx.posted_at (business date). This mode shows, on a hand-built pool, that when
// business order ≠ commit order (a backdated receipt, §14.5), replaying the
// persisted stream by id yields a DIFFERENT — and wrong — authoritative cost than
// replaying by business date. So id alone is NOT reconstruction-sufficient; the
// stream MUST carry a faithful business-chronology key and recalc MUST sort by it.
// ─────────────────────────────────────────────────────────────────────────────

/// (label, business_day, signed_qty, unit_cost). Vec order = allocation/commit
/// (id) order; business_day gives the intended chronological order.
type Event = (&'static str, u32, i64, i64);

/// Strict-FIFO cost of the single depletion in `events`, replaying in the given
/// order. Returns (true_cost, well_defined).
fn fifo_over(events: &[&Event]) -> (i64, bool) {
    let mut pool = StrictPool::new(false);
    let mut cost = 0;
    let mut ok = true;
    for &&(_, _, qty, uc) in events {
        if qty >= 0 {
            pool.receipt(qty, uc);
        } else {
            match pool.deplete(-qty) {
                Some(c) => cost = c,
                None => ok = false,
            }
        }
    }
    (cost, ok)
}

/// Provisional running-average cost the hot path RECORDS for the depletion,
/// replaying in commit (id) order — this is what gets persisted on trx_line.
fn provisional_over(events: &[&Event]) -> i64 {
    let mut prov = ProvisionalPool::new(1, PoolMethod::Fifo, ProvisionalBasis::RunningAvg, 0);
    // Opening the pool from the first receipt keeps the aggregate well-formed.
    let mut seeded = false;
    let mut recorded = 0;
    for &&(_, _, qty, uc) in events {
        if qty >= 0 && !seeded {
            prov.seed_aggregate(qty, uc);
            seeded = true;
        } else if qty >= 0 {
            prov.apply(qty, uc);
        } else {
            recorded = prov.apply(qty, 0);
        }
    }
    recorded
}

fn ordering() {
    println!("## Ordering / backdating falsification (§14.5, §14.6)\n");
    println!(
        "Each vignette is ONE fifo/running_avg pool. `commit order` = the order the \
         hot path saw (= trx_line.id order in the persisted stream). `business order` = \
         intended chronology (trx.posted_at). A recalc that replays the persisted stream \
         must pick an order key; the question is whether id suffices.\n"
    );

    // Vignette A — backdated receipt: a cheap lot business-dated FIRST but
    // committed LAST (arrived late). Both orders are well-defined; they disagree.
    let a: Vec<Event> = vec![
        ("R_base  200@300 (day2)", 2, 200, 300),
        ("D_1     deplete 100 (day3)", 3, -100, 0),
        ("R_back  200@100 (day1, backdated)", 1, 200, 100),
    ];
    print_vignette("A — backdated cheaper receipt (business≠commit)", &a);

    // Vignette B — same lots, NO backdating: business order == commit order.
    let b: Vec<Event> = vec![
        ("R_back  200@100 (day1)", 1, 200, 100),
        ("R_base  200@300 (day2)", 2, 200, 300),
        ("D_1     deplete 100 (day3)", 3, -100, 0),
    ];
    print_vignette("B — same lots, in business order (business==commit)", &b);
    println!();
}

fn print_vignette(title: &str, events: &[Event]) {
    println!("### Vignette {title}\n");
    let commit: Vec<&Event> = events.iter().collect();
    let mut business: Vec<&Event> = events.iter().collect();
    business.sort_by_key(|e| e.1);

    println!("- commit (id) order:   {}", commit.iter().map(|e| e.0).collect::<Vec<_>>().join("  →  "));
    println!("- business order:      {}", business.iter().map(|e| e.0).collect::<Vec<_>>().join("  →  "));

    let recorded = provisional_over(&commit); // what the hot path persists
    let (true_id, ok_id) = fifo_over(&commit); // naive recalc: replay stream by id
    let (true_biz, ok_biz) = fifo_over(&business); // authoritative: replay by posted_at

    println!("- RECORDED provisional (running_avg, commit order): **{recorded}**");
    println!(
        "- true FIFO, replay by trx_line.id:      **{}**{}",
        true_id,
        if ok_id { "" } else { "  (ILL-DEFINED — depletion uncovered in id order)" }
    );
    println!(
        "- true FIFO, replay by posted_at:        **{}**{}",
        true_biz,
        if ok_biz { "" } else { "  (ILL-DEFINED)" }
    );
    if true_id == true_biz && ok_id && ok_biz {
        println!("- ✅ id-order reconstruction == authoritative. Stream IS sufficient here.");
    } else {
        println!(
            "- ❌ id-order reconstruction ({true_id}) ≠ authoritative ({true_biz}) — off by {}. \
             Reconstruction from id alone is WRONG; posted_at (business chronology) is required.",
            (true_id - true_biz).abs()
        );
    }
    println!();
}

// ─────────────────────────────────────────────────────────────────────────────
// Real-dump mode.
// ─────────────────────────────────────────────────────────────────────────────

struct PoolMeta {
    method: PoolMethod,
    basis: ProvisionalBasis,
}

struct RealLine {
    id: i64,
    qty: i64,
    unit_cost: i64,
    is_run: bool,
}

fn parse_method(s: &str) -> Option<PoolMethod> {
    match s {
        "fifo" => Some(PoolMethod::Fifo),
        "lifo" => Some(PoolMethod::Lifo),
        "wac" => Some(PoolMethod::Wac),
        "std" => Some(PoolMethod::Std),
        "specific" => Some(PoolMethod::Specific),
        _ => None,
    }
}

fn read_tsv(path: &str) -> Vec<Vec<String>> {
    let body = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("cannot read {path}: {e}"));
    body.lines()
        .filter(|l| !l.is_empty())
        .map(|l| l.split('\t').map(|s| s.to_string()).collect())
        .collect()
}

fn real(dir: &str) {
    // pool.tsv: id, method, provisional_basis
    let mut meta: HashMap<i64, PoolMeta> = HashMap::new();
    for r in read_tsv(&format!("{dir}/pool.tsv")) {
        let id: i64 = r[0].parse().unwrap();
        let method = parse_method(&r[1]).unwrap();
        let basis = if r[2] == "standard" { ProvisionalBasis::Standard } else { ProvisionalBasis::RunningAvg };
        meta.insert(id, PoolMeta { method, basis });
    }

    // layers.tsv: pool_id, layer_id, qty, unit_cost  (seed layers, layer_id != 0)
    // Reconstruct each pool's INITIAL aggregate + initial strict layer stack.
    let mut init_qty: HashMap<i64, i64> = HashMap::new();
    let mut init_val: HashMap<i64, i128> = HashMap::new();
    let mut init_layers: HashMap<i64, Vec<(i64, i64, i64)>> = HashMap::new(); // (layer_id, qty, cost)
    for r in read_tsv(&format!("{dir}/layers.tsv")) {
        let pool_id: i64 = r[0].parse().unwrap();
        let layer_id: i64 = r[1].parse().unwrap();
        let qty: i64 = r[2].parse().unwrap();
        let cost: i64 = r[3].parse().unwrap();
        *init_qty.entry(pool_id).or_default() += qty;
        *init_val.entry(pool_id).or_default() += qty as i128 * cost as i128;
        init_layers.entry(pool_id).or_default().push((layer_id, qty, cost));
    }

    // lines.tsv: id, pool_id, line_type, qty, unit_cost, posted_at
    // Already ORDER BY pool_id, id. is_run = posted_at in the run window (not the
    // Jan seed date).
    let rows = read_tsv(&format!("{dir}/lines.tsv"));
    let mut by_pool: HashMap<i64, Vec<RealLine>> = HashMap::new();
    let mut posted_at_set: HashMap<String, u64> = HashMap::new();
    let mut run_lines = 0u64;
    for r in rows {
        let posted_at = r[5].clone();
        let is_run = !posted_at.starts_with("2026-01-01");
        *posted_at_set.entry(posted_at).or_default() += 1;
        if is_run {
            run_lines += 1;
        }
        let id: i64 = r[0].parse().unwrap();
        let pool_id: i64 = r[1].parse().unwrap();
        let qty: i64 = r[3].parse().unwrap();
        let unit_cost: i64 = r[4].parse().unwrap();
        by_pool.entry(pool_id).or_default().push(RealLine { id, qty, unit_cost, is_run });
    }

    // ── Reproduction: replay run-time lines through the SHIPPED provisional
    // transform in trx_line.id order and compare each depletion's computed cost to
    // the recorded trx_line.unit_cost. Match rate == "is id-order a sufficient
    // reconstruction key?" (the §14.6 question) on real concurrent data.
    let mut repro_checked = 0u64;
    let mut repro_match = 0u64;
    let mut anchor = Variance::default();
    let mut ill_defined = 0u64;

    for (pool_id, lines) in by_pool.iter_mut() {
        let m = match meta.get(pool_id) {
            Some(m) => m,
            None => continue,
        };
        // Provisional arm seeded from the reconstructed initial aggregate.
        let iq = init_qty.get(pool_id).copied().unwrap_or(0);
        let iv = init_val.get(pool_id).copied().unwrap_or(0);
        if iq == 0 {
            continue;
        }
        let iuc = banker_div(iv, iq);
        let mut prov = ProvisionalPool::new(*pool_id, m.method, m.basis, iuc);
        prov.seed_aggregate(iq, iuc);

        // Strict oracle seeded from the initial layer stack in layer_id order.
        let lifo = matches!(m.method, PoolMethod::Lifo);
        let mut strict = StrictPool::new(lifo);
        if let Some(ls) = init_layers.get(pool_id) {
            let mut ls = ls.clone();
            ls.sort_by_key(|&(lid, _, _)| lid);
            for (_, q, c) in ls {
                strict.receipt(q, c);
            }
        }

        lines.sort_by_key(|l| l.id);
        for l in lines.iter() {
            if !l.is_run {
                continue; // seed receipts already folded into the initial state
            }
            if l.qty >= 0 {
                prov.apply(l.qty, l.unit_cost);
                strict.receipt(l.qty, l.unit_cost);
            } else {
                let recorded = prov.apply(l.qty, 0);
                repro_checked += 1;
                if recorded == l.unit_cost {
                    repro_match += 1;
                }
                match strict.deplete(-l.qty) {
                    Some(truth) => anchor.push(l.unit_cost, truth),
                    None => ill_defined += 1,
                }
            }
        }
    }

    println!("## Real harness dump — premise check & reproduction\n");
    println!("- pools in dump: {}", meta.len());
    println!("- run-time trx_lines: {run_lines}");
    println!("- distinct posted_at values across the stream: {}", posted_at_set.len());
    let mut pa: Vec<(&String, &u64)> = posted_at_set.iter().collect();
    pa.sort_by(|a, b| a.0.cmp(b.0));
    for (v, c) in pa {
        println!("    - `{v}` × {c}");
    }
    println!();
    println!("### Reconstruction fidelity (id-ordered replay of the shipped transform)\n");
    println!("- depletions checked: {repro_checked}");
    if repro_checked > 0 {
        println!(
            "- recorded == reconstructed(id-order): {repro_match} / {repro_checked}  ({:.4}%)",
            repro_match as f64 / repro_checked as f64 * 100.0
        );
    }
    println!("- ill-defined depletions (no covering layer in strict replay): {ill_defined}");
    println!();
    println!("### Native variance anchor (recorded provisional vs strict-true FIFO/LIFO)\n");
    if anchor.n() > 0 {
        let abs = anchor.abs_sorted();
        println!("- depletions: {}", anchor.n());
        println!("- mean true cost: {:.0} µ-units", anchor.mean_true());
        println!("- exact match: {:.2}%", anchor.exact_frac() * 100.0);
        println!("- mean Δ (signed): {:+.2} µ-units", anchor.mean_signed());
        println!("- mean |Δ|: {:.2} µ-units", anchor.mean_abs());
        println!(
            "- |Δ| p50/p90/p99/max: {} / {} / {} / {} µ-units",
            Variance::pct(&abs, 0.50),
            Variance::pct(&abs, 0.90),
            Variance::pct(&abs, 0.99),
            abs.last().copied().unwrap_or(0),
        );
    } else {
        println!("- no depletions in the dump.");
    }
    println!();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("synth") => synth(),
        Some("ordering") => ordering(),
        Some("real") => {
            let dir = args.get(2).map(|s| s.as_str()).unwrap_or_else(|| {
                eprintln!("usage: replay-oracle real <tsv-dir>");
                std::process::exit(2);
            });
            real(dir);
        }
        _ => {
            eprintln!("usage: replay-oracle <synth|ordering|real <tsv-dir>>");
            std::process::exit(2);
        }
    }
}
