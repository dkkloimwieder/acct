//! ledger-oracle: an independent sequential reference model of design-v3.1 §3
//! (acct-0at4.4).
//!
//! # Why this exists
//!
//! The v3.1 cross-flavor equivalence test (§9.4, `ledger-harness/src/equivalence.rs`)
//! is a *differential* oracle: it drives the same workload through the direct and
//! routed flavors and checks they agree. Both flavors call `ledger-core`, so a bug
//! in `ledger-core` that both inherit is invisible to a direct-vs-routed diff. This
//! crate is the missing *independent reference model*: it re-derives the §3 costing
//! and quantity semantics from the spec, deliberately NOT by calling `ledger-core`.
//!
//! The one primitive most worth re-deriving independently is the rounding. The
//! running-average unit_cost is the exact ratio `value_sum / qty` rounded
//! half-to-even (§3.0). `ledger-core::banker_div` computes that with truncated
//! i128 division plus a sign/remainder adjustment; this model computes it from an
//! exact `BigRational` via `floor` + fractional comparison ([`round_half_even`]).
//! Two different code paths for the same rule: a sign or tie bug in either surfaces
//! as a model-vs-SQL disagreement in the exact-mode differential.
//!
//! # What it is NOT
//!
//! It reuses `ledger-core`'s *data* types (`LineType`, `PoolMethod`,
//! `ProvisionalBasis`, `TrxLineRequest`, `PostingAccounts`) — those are plain
//! classifications and config plumbing, not the cost logic under test. No pgrx, no
//! DB: this crate is the pure-Rust model. The exact-mode differential (drive the
//! real `ledger_submit_trx_c` and diff byte-for-byte) and the envelope-mode check
//! (routed / harness, legal-outcome set) live in the pgrx test binaries that
//! dev-depend on this crate.

use std::collections::HashMap;

use num_bigint::BigInt;
use num_rational::BigRational;
use num_traits::{ToPrimitive, Zero};

use ledger_core::{PoolMethod, PostingAccounts, ProvisionalBasis, TrxLineRequest};

pub mod envelope;

/// Round the exact ratio `numerator / denominator` half-to-even (§3.0), computed
/// independently of `ledger-core::banker_div`.
///
/// `banker_div` truncates toward zero and adjusts by the doubled remainder and a
/// sign term; this walks `BigRational::floor` (toward −∞) and compares the
/// fractional part to ½, breaking ties on the parity of the floor. The two agree
/// on every input — that is the point — but a sign/tie bug in one and not the
/// other shows up as a differential failure.
pub fn round_half_even(numerator: &BigInt, denominator: i64) -> i64 {
    assert!(denominator != 0, "round_half_even: denominator must be non-zero");
    let ratio = BigRational::new(numerator.clone(), BigInt::from(denominator));
    let floor = ratio.floor();
    let floor_int = floor.to_integer();
    let frac = &ratio - &floor; // in [0, 1)
    // Compare frac to 1/2 without floats: 2*frac vs 1.
    let two_frac = &frac + &frac;
    let one = BigRational::from_integer(BigInt::from(1));
    let rounded: BigInt = if two_frac < one {
        floor_int
    } else if two_frac > one {
        floor_int + 1
    } else {
        // Exactly half → nearest even. `%` in num-bigint takes the dividend's
        // sign, so `(&x % 2).is_zero()` is a correct parity test for any sign.
        if (&floor_int % 2i32).is_zero() {
            floor_int
        } else {
            floor_int + 1
        }
    };
    rounded
        .to_i64()
        .expect("round_half_even: unit_cost overflows i64 (pool value out of range)")
}

/// The observable failure modes, mirroring `ledger-core::LedgerError` at the SQL
/// boundary. [`OracleError::message`] returns a substring guaranteed to appear in
/// the `ledger_submit_trx_c` SQL error text, so the exact-mode differential can
/// assert error-for-error (see the SQLSTATE map in `ledger-direct-c`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OracleError {
    InsufficientInventory { pool_id: i64, requested: i64, available: i64 },
    UnknownPool(i64),
    MissingStandardCost { sku_id: i64, location_id: i64 },
    MissingPostingAccounts { sku_id: i64, location_id: i64 },
    MissingVarianceAccount { pool_id: i64 },
    SpecificPoolOccupied { pool_id: i64, existing_qty: i64 },
    Overflow,
}

impl OracleError {
    /// A stable substring of the corresponding `ledger-core` `#[error]` message.
    pub fn message(&self) -> String {
        match self {
            OracleError::InsufficientInventory { pool_id, requested, available } => format!(
                "insufficient inventory: pool={pool_id} requested={requested} available={available}"
            ),
            OracleError::UnknownPool(p) => format!("snapshot missing pool_id={p}"),
            OracleError::MissingStandardCost { sku_id, location_id } => {
                format!("missing standard cost: sku={sku_id} location={location_id}")
            }
            OracleError::MissingPostingAccounts { sku_id, location_id } => {
                format!("missing posting accounts: sku={sku_id} location={location_id}")
            }
            OracleError::MissingVarianceAccount { pool_id } => {
                format!("missing variance account for STD pool={pool_id}")
            }
            OracleError::SpecificPoolOccupied { pool_id, existing_qty } => {
                format!("specific pool {pool_id} already holds a unit (qty={existing_qty})")
            }
            OracleError::Overflow => "arithmetic overflow".to_string(),
        }
    }
}

/// A predicted `trx_line` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedLine {
    /// Signed (positive receipt / negative depletion) — as recorded.
    pub qty: i64,
    /// Recorded cost: actual for receipts; running-average or standard for
    /// aggregate depletions; standard for STD; the layer's cost for specific.
    pub unit_cost: i64,
    /// `source_trx_line_id`: NULL for everything but specific depletions, which
    /// reference the receipt that stocked the layer. The receipt's real
    /// `trx_line.id` is DB-assigned, so the model references it by *flat ordinal*
    /// (its position in the global successful-line stream) and the differential
    /// harness resolves the ordinal to the real id it read back.
    pub source: ExpectedSource,
}

/// How a predicted line's `source_trx_line_id` resolves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedSource {
    /// `source_trx_line_id IS NULL`.
    Null,
    /// `source_trx_line_id` = the real `trx_line.id` at this flat ordinal in the
    /// successful-line stream (the specific receipt that stocked the layer).
    ReceiptFlatIndex(usize),
}

/// A predicted `posting_line` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedPosting {
    /// SQL `posting_event_type` text: `inventory_receipt` / `inventory_depletion`
    /// / `variance`.
    pub event_type: &'static str,
    pub amount: i64,
    pub debit_account: i64,
    pub credit_account: i64,
}

/// A predicted aggregate `pool_state` row (`layer_id = 0`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpectedAggregate {
    pub qty: i64,
    pub unit_cost: i64,
    pub value_sum: i64,
}

/// The predicted effect of one submission: the `trx_line` and `posting_line` rows
/// it writes, in insertion order (= ascending `id` order).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Applied {
    pub lines: Vec<ExpectedLine>,
    pub postings: Vec<ExpectedPosting>,
}

#[derive(Debug, Clone)]
struct Layer {
    /// Flat ordinal of the receipt line that materialized this layer.
    receipt_flat_idx: usize,
    qty: i64,
    unit_cost: i64,
}

#[derive(Debug, Clone)]
struct ModelPool {
    sku_id: i64,
    location_id: i64,
    method: PoolMethod,
    basis: ProvisionalBasis,
    /// `None` models a pool with no `posting_account_map` row (§3.7).
    accounts: Option<PostingAccounts>,
    /// `None` models a pool with no `standard_cost` row.
    standard_cost: Option<i64>,
    qty: i64,
    /// Cumulative book value, kept i64-congruent with the DB (range-checked on
    /// every mutation, so it always fits — a mutation that would exceed i64 is an
    /// `Overflow`, matching the SQL path).
    value_sum: i64,
    unit_cost: i64,
    /// Specific-method layers (`layer_id > 0`). Empty for aggregate methods.
    layers: Vec<Layer>,
}

/// The independent in-memory reference ledger.
#[derive(Debug, Clone, Default)]
pub struct ModelLedger {
    pools: HashMap<i64, ModelPool>,
    /// Count of `trx_line` rows emitted across all *successful* submissions — the
    /// flat ordinal stream the DB's ascending `trx_line.id` mirrors.
    flat_next: usize,
}

impl ModelLedger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a pool, mirroring what the test's `seed_pool` wrote. `accounts`
    /// is `None` to model a missing `posting_account_map` row; `standard_cost` is
    /// `None` to model a missing `standard_cost` row.
    #[allow(clippy::too_many_arguments)]
    pub fn add_pool(
        &mut self,
        pool_id: i64,
        sku_id: i64,
        location_id: i64,
        method: PoolMethod,
        basis: ProvisionalBasis,
        accounts: Option<PostingAccounts>,
        standard_cost: Option<i64>,
    ) {
        self.pools.insert(
            pool_id,
            ModelPool {
                sku_id,
                location_id,
                method,
                basis,
                accounts,
                standard_cost,
                qty: 0,
                value_sum: 0,
                unit_cost: 0,
                layers: Vec::new(),
            },
        );
    }

    /// The predicted aggregate row, or `None` if the pool has no aggregate yet
    /// (no line has ever touched it).
    pub fn aggregate(&self, pool_id: i64) -> Option<ExpectedAggregate> {
        let p = self.pools.get(&pool_id)?;
        if p.qty == 0 && p.value_sum == 0 && p.unit_cost == 0 && p.layers.is_empty() {
            // Never written — the DB has no pool_state row either.
            return None;
        }
        Some(ExpectedAggregate { qty: p.qty, unit_cost: p.unit_cost, value_sum: p.value_sum })
    }

    /// Total `trx_line` rows emitted across all successful submissions — the
    /// length of the flat ordinal stream. The differential harness asserts this
    /// equals the count of real `trx_line.id`s it has read back, keeping the
    /// `ReceiptFlatIndex` resolution honest.
    pub fn lines_emitted(&self) -> usize {
        self.flat_next
    }

    /// Live materialized-layer count (`layer_id > 0`) for a pool. Aggregate
    /// methods keep this at 0 (§3.5 I4); specific tracks its live layers.
    pub fn layer_count(&self, pool_id: i64) -> usize {
        self.pools.get(&pool_id).map(|p| p.layers.len()).unwrap_or(0)
    }

    /// Apply one submission (a `trx` = a slice of lines) transactionally: on the
    /// first failing line the whole submission is rolled back (matching the SQL
    /// path's tx abort — no rows, no consumed ids) and the error returned.
    pub fn apply(&mut self, lines: &[TrxLineRequest]) -> Result<Applied, OracleError> {
        let pools_backup = self.pools.clone();
        let flat_backup = self.flat_next;
        let mut applied = Applied::default();
        for line in lines {
            if let Err(e) = self.apply_line(line, &mut applied) {
                self.pools = pools_backup;
                self.flat_next = flat_backup;
                return Err(e);
            }
        }
        Ok(applied)
    }

    fn apply_line(&mut self, line: &TrxLineRequest, out: &mut Applied) -> Result<(), OracleError> {
        // qty == 0 is a no-op for every method, emitting nothing and consuming no
        // id — checked before any config resolution (matches ledger-core).
        if line.qty == 0 {
            return Ok(());
        }
        let method = self.method_of(line.pool_id)?;
        match method {
            PoolMethod::Wac => self.aggregate_op(line, out, AppliedBasis::RunningAvg),
            PoolMethod::Fifo | PoolMethod::Lifo => {
                let basis = self.pools[&line.pool_id].basis;
                let applied_basis = match basis {
                    ProvisionalBasis::RunningAvg => AppliedBasis::RunningAvg,
                    ProvisionalBasis::Standard => AppliedBasis::Standard,
                };
                self.aggregate_op(line, out, applied_basis)
            }
            PoolMethod::Std => self.std_op(line, out),
            PoolMethod::Specific => self.specific_op(line, out),
        }
    }

    fn method_of(&self, pool_id: i64) -> Result<PoolMethod, OracleError> {
        self.pools.get(&pool_id).map(|p| p.method).ok_or(OracleError::UnknownPool(pool_id))
    }

    fn resolve_accounts(&self, pool_id: i64) -> Result<PostingAccounts, OracleError> {
        let p = &self.pools[&pool_id];
        p.accounts.ok_or(OracleError::MissingPostingAccounts {
            sku_id: p.sku_id,
            location_id: p.location_id,
        })
    }

    fn resolve_standard_cost(&self, pool_id: i64) -> Result<i64, OracleError> {
        let p = &self.pools[&pool_id];
        p.standard_cost.ok_or(OracleError::MissingStandardCost {
            sku_id: p.sku_id,
            location_id: p.location_id,
        })
    }

    fn next_flat(&mut self) -> usize {
        let idx = self.flat_next;
        self.flat_next += 1;
        idx
    }

    /// WAC / provisional FIFO / provisional LIFO. Receipts run the running-average
    /// aggregate update; depletions record at either the running average or the
    /// standard cost, per `applied_basis` (§3.1 / §3.5).
    fn aggregate_op(
        &mut self,
        line: &TrxLineRequest,
        out: &mut Applied,
        applied_basis: AppliedBasis,
    ) -> Result<(), OracleError> {
        if line.qty > 0 {
            self.aggregate_receipt(line, out)
        } else {
            // Standard basis resolves the standard cost BEFORE posting accounts,
            // matching provisional.rs (basis resolution precedes aggregate_deplete).
            let applied = match applied_basis {
                AppliedBasis::RunningAvg => self.pools[&line.pool_id].unit_cost,
                AppliedBasis::Standard => self.resolve_standard_cost(line.pool_id)?,
            };
            self.aggregate_deplete(line, out, applied)
        }
    }

    fn aggregate_receipt(
        &mut self,
        line: &TrxLineRequest,
        out: &mut Applied,
    ) -> Result<(), OracleError> {
        let (debit, credit) = self.resolve_accounts(line.pool_id)?.pair_for(line.line_type);
        let p = self.pools.get(&line.pool_id).unwrap();
        let new_qty = p.qty.checked_add(line.qty).ok_or(OracleError::Overflow)?;
        // Exact accumulation in i128, range-checked back to i64 (the DB stores i64
        // and raises Overflow past its range).
        let new_value_sum_i128 =
            p.value_sum as i128 + (line.qty as i128) * (line.unit_cost as i128);
        let new_value_sum: i64 = new_value_sum_i128.try_into().map_err(|_| OracleError::Overflow)?;
        let new_unit_cost = if new_qty > 0 {
            round_half_even(&BigInt::from(new_value_sum), new_qty)
        } else {
            p.unit_cost
        };
        let amount = mul_i64(line.qty, line.unit_cost)?;

        self.next_flat();
        out.lines.push(ExpectedLine {
            qty: line.qty,
            unit_cost: line.unit_cost, // receipts record the actual asserted cost
            source: ExpectedSource::Null,
        });
        out.postings.push(ExpectedPosting {
            event_type: "inventory_receipt",
            amount,
            debit_account: debit,
            credit_account: credit,
        });

        let p = self.pools.get_mut(&line.pool_id).unwrap();
        p.qty = new_qty;
        p.value_sum = new_value_sum;
        p.unit_cost = new_unit_cost;
        Ok(())
    }

    fn aggregate_deplete(
        &mut self,
        line: &TrxLineRequest,
        out: &mut Applied,
        applied_unit_cost: i64,
    ) -> Result<(), OracleError> {
        let (rcv_debit, rcv_credit) = self.resolve_accounts(line.pool_id)?.pair_for(line.line_type);
        let (debit, credit) = (rcv_credit, rcv_debit); // depletion swaps direction
        let p = self.pools.get(&line.pool_id).unwrap();
        let qty_to_deplete = line.qty.checked_abs().ok_or(OracleError::Overflow)?;
        if p.qty < qty_to_deplete {
            return Err(OracleError::InsufficientInventory {
                pool_id: line.pool_id,
                requested: qty_to_deplete,
                available: p.qty,
            });
        }
        let new_qty = p.qty - qty_to_deplete;
        let amount = mul_i64(qty_to_deplete, applied_unit_cost)?;
        let new_value_sum: i64 = if new_qty == 0 {
            0
        } else {
            (p.value_sum as i128 - amount as i128).try_into().map_err(|_| OracleError::Overflow)?
        };
        let new_unit_cost = if new_qty > 0 {
            round_half_even(&BigInt::from(new_value_sum), new_qty)
        } else {
            p.unit_cost
        };

        self.next_flat();
        out.lines.push(ExpectedLine {
            qty: line.qty,
            unit_cost: applied_unit_cost,
            source: ExpectedSource::Null,
        });
        out.postings.push(ExpectedPosting {
            event_type: "inventory_depletion",
            amount,
            debit_account: debit,
            credit_account: credit,
        });

        let p = self.pools.get_mut(&line.pool_id).unwrap();
        p.qty = new_qty;
        p.value_sum = new_value_sum;
        p.unit_cost = new_unit_cost;
        Ok(())
    }

    /// STD (§3.3). Both directions record at the standard cost; receipts post the
    /// actual-vs-standard purchase-price variance. `resolve_standard_cost` runs
    /// before posting-account resolution (matching apply_std).
    fn std_op(&mut self, line: &TrxLineRequest, out: &mut Applied) -> Result<(), OracleError> {
        let c_std = self.resolve_standard_cost(line.pool_id)?;
        if line.qty > 0 {
            self.std_receipt(line, out, c_std)
        } else {
            self.std_deplete(line, out, c_std)
        }
    }

    fn std_receipt(
        &mut self,
        line: &TrxLineRequest,
        out: &mut Applied,
        c_std: i64,
    ) -> Result<(), OracleError> {
        let accounts = self.resolve_accounts(line.pool_id)?;
        let (debit, credit) = accounts.pair_for(line.line_type);
        let c_actual = line.unit_cost;
        let p = self.pools.get(&line.pool_id).unwrap();
        let new_qty = p.qty.checked_add(line.qty).ok_or(OracleError::Overflow)?;
        let value_sum: i64 =
            (new_qty as i128 * c_std as i128).try_into().map_err(|_| OracleError::Overflow)?;
        let std_value = mul_i64(line.qty, c_std)?;

        self.next_flat();
        out.lines.push(ExpectedLine {
            qty: line.qty,
            unit_cost: c_std, // STD records the standard, not the actual
            source: ExpectedSource::Null,
        });
        out.postings.push(ExpectedPosting {
            event_type: "inventory_receipt",
            amount: std_value,
            debit_account: debit,
            credit_account: credit,
        });

        // Variance leg on actual != standard.
        let delta_unit = c_actual as i128 - c_std as i128;
        if delta_unit != 0 {
            let variance_account = accounts
                .variance
                .ok_or(OracleError::MissingVarianceAccount { pool_id: line.pool_id })?;
            let variance = line.qty as i128 * delta_unit;
            let (var_debit, var_credit, amount) = if variance > 0 {
                (variance_account, credit, variance)
            } else {
                (credit, variance_account, -variance)
            };
            let amount: i64 = amount.try_into().map_err(|_| OracleError::Overflow)?;
            out.postings.push(ExpectedPosting {
                event_type: "variance",
                amount,
                debit_account: var_debit,
                credit_account: var_credit,
            });
        }

        let p = self.pools.get_mut(&line.pool_id).unwrap();
        p.qty = new_qty;
        p.value_sum = value_sum;
        p.unit_cost = c_std;
        Ok(())
    }

    fn std_deplete(
        &mut self,
        line: &TrxLineRequest,
        out: &mut Applied,
        c_std: i64,
    ) -> Result<(), OracleError> {
        let (rcv_debit, rcv_credit) = self.resolve_accounts(line.pool_id)?.pair_for(line.line_type);
        let (debit, credit) = (rcv_credit, rcv_debit);
        let p = self.pools.get(&line.pool_id).unwrap();
        let qty_to_deplete = line.qty.checked_abs().ok_or(OracleError::Overflow)?;
        if p.qty < qty_to_deplete {
            return Err(OracleError::InsufficientInventory {
                pool_id: line.pool_id,
                requested: qty_to_deplete,
                available: p.qty,
            });
        }
        let new_qty = p.qty - qty_to_deplete;
        let value_sum: i64 =
            (new_qty as i128 * c_std as i128).try_into().map_err(|_| OracleError::Overflow)?;
        let amount = mul_i64(qty_to_deplete, c_std)?;

        self.next_flat();
        out.lines.push(ExpectedLine {
            qty: line.qty,
            unit_cost: c_std,
            source: ExpectedSource::Null,
        });
        out.postings.push(ExpectedPosting {
            event_type: "inventory_depletion",
            amount,
            debit_account: debit,
            credit_account: credit,
        });

        let p = self.pools.get_mut(&line.pool_id).unwrap();
        p.qty = new_qty;
        p.value_sum = value_sum;
        p.unit_cost = c_std;
        Ok(())
    }

    /// Specific-id (§3.4). K=1: one layer per pool; a receipt into a stocked pool
    /// is rejected; a depletion consumes the single layer and links it.
    fn specific_op(&mut self, line: &TrxLineRequest, out: &mut Applied) -> Result<(), OracleError> {
        if line.qty > 0 {
            self.specific_receipt(line, out)
        } else {
            self.specific_deplete(line, out)
        }
    }

    fn specific_receipt(
        &mut self,
        line: &TrxLineRequest,
        out: &mut Applied,
    ) -> Result<(), OracleError> {
        let (debit, credit) = self.resolve_accounts(line.pool_id)?.pair_for(line.line_type);
        let p = self.pools.get(&line.pool_id).unwrap();
        if p.qty > 0 {
            return Err(OracleError::SpecificPoolOccupied {
                pool_id: line.pool_id,
                existing_qty: p.qty,
            });
        }
        let new_qty = p.qty.checked_add(line.qty).ok_or(OracleError::Overflow)?;
        let value_sum: i64 = (new_qty as i128 * line.unit_cost as i128)
            .try_into()
            .map_err(|_| OracleError::Overflow)?;
        let amount = mul_i64(line.qty, line.unit_cost)?;

        let flat_idx = self.next_flat();
        out.lines.push(ExpectedLine {
            qty: line.qty,
            unit_cost: line.unit_cost,
            source: ExpectedSource::Null,
        });
        out.postings.push(ExpectedPosting {
            event_type: "inventory_receipt",
            amount,
            debit_account: debit,
            credit_account: credit,
        });

        let p = self.pools.get_mut(&line.pool_id).unwrap();
        p.layers.push(Layer { receipt_flat_idx: flat_idx, qty: line.qty, unit_cost: line.unit_cost });
        p.qty = new_qty;
        p.value_sum = value_sum;
        p.unit_cost = line.unit_cost;
        Ok(())
    }

    fn specific_deplete(
        &mut self,
        line: &TrxLineRequest,
        out: &mut Applied,
    ) -> Result<(), OracleError> {
        let (rcv_debit, rcv_credit) = self.resolve_accounts(line.pool_id)?.pair_for(line.line_type);
        let (debit, credit) = (rcv_credit, rcv_debit);
        let p = self.pools.get(&line.pool_id).unwrap();
        let qty_to_deplete = line.qty.checked_abs().ok_or(OracleError::Overflow)?;
        // Lowest-ordinal live layer (mirrors ledger-core's lowest layer_id).
        let layer = p
            .layers
            .iter()
            .enumerate()
            .min_by_key(|(_, l)| l.receipt_flat_idx)
            .map(|(i, l)| (i, l.clone()));
        let (layer_pos, layer) = match layer {
            Some((i, l)) if l.qty >= qty_to_deplete => (i, l),
            other => {
                let available = other.map(|(_, l)| l.qty).unwrap_or(0);
                return Err(OracleError::InsufficientInventory {
                    pool_id: line.pool_id,
                    requested: qty_to_deplete,
                    available,
                });
            }
        };
        let amount = mul_i64(qty_to_deplete, layer.unit_cost)?;
        let new_agg_qty = (p.qty - qty_to_deplete).max(0);
        let new_value_sum: i64 = (new_agg_qty as i128 * p.unit_cost as i128)
            .try_into()
            .map_err(|_| OracleError::Overflow)?;

        self.next_flat();
        out.lines.push(ExpectedLine {
            qty: line.qty,
            unit_cost: layer.unit_cost,
            source: ExpectedSource::ReceiptFlatIndex(layer.receipt_flat_idx),
        });
        out.postings.push(ExpectedPosting {
            event_type: "inventory_depletion",
            amount,
            debit_account: debit,
            credit_account: credit,
        });

        let p = self.pools.get_mut(&line.pool_id).unwrap();
        p.layers.remove(layer_pos); // K=1 full consumption deletes the layer
        p.qty = new_agg_qty;
        p.value_sum = new_value_sum;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum AppliedBasis {
    RunningAvg,
    Standard,
}

fn mul_i64(a: i64, b: i64) -> Result<i64, OracleError> {
    (a as i128 * b as i128).try_into().map_err(|_| OracleError::Overflow)
}

/// Fully receipt-direction posting accounts where every operation pair is
/// `(debit, credit)` and the variance account is `Some(var)`. Mirrors the
/// `seed_pool` fixture (all pairs share one `(inv, ap)`), so tests can build a
/// matching model without hand-copying `PostingAccounts`.
pub fn uniform_accounts(debit: i64, credit: i64, variance: i64) -> PostingAccounts {
    PostingAccounts {
        receipt: (debit, credit),
        transfer: (debit, credit),
        build: (debit, credit),
        scrap: (debit, credit),
        adjustment: (debit, credit),
        revaluation: (debit, credit),
        variance: Some(variance),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledger_core::LineType;

    fn recv(pool_id: i64, qty: i64, unit_cost: i64) -> TrxLineRequest {
        TrxLineRequest {
            pool_id,
            line_type: LineType::PoReceiptLine,
            source_id: None,
            qty,
            unit_cost,
        }
    }
    fn depl(pool_id: i64, qty: i64) -> TrxLineRequest {
        TrxLineRequest {
            pool_id,
            line_type: LineType::TransferShipmentLine,
            source_id: None,
            qty: -qty,
            unit_cost: 0,
        }
    }

    // ── round_half_even independence: agrees with the design rule (§3.0),
    //    computed via a different code path than banker_div ──────────────
    #[test]
    fn round_half_even_matches_banker_rule() {
        let b = |n: i128, d: i64| round_half_even(&BigInt::from(n), d);
        assert_eq!(b(100, 4), 25);
        assert_eq!(b(9, 4), 2); // 2.25 → 2
        assert_eq!(b(11, 4), 3); // 2.75 → 3
        assert_eq!(b(10, 4), 2); // 2.5, q even → 2
        assert_eq!(b(6, 4), 2); // 1.5, q odd → 2
        assert_eq!(b(14, 4), 4); // 3.5 → 4
        assert_eq!(b(-9, 4), -2); // -2.25 → -2
        assert_eq!(b(-10, 4), -2); // -2.5, even → -2
        assert_eq!(b(-6, 4), -2); // -1.5 → -2
        assert_eq!(b(-7, 2), -4); // -3.5 → -4
        assert_eq!(b(-1, 2), 0); // -0.5 → 0
        assert_eq!(b(-7, -2), 4); // 3.5 → 4
    }

    // Cross-check round_half_even against ledger-core's banker_div on a sweep,
    // proving the two independent implementations of the §3.0 rule agree.
    #[test]
    fn round_half_even_agrees_with_ledger_core_banker_div() {
        for num in -200i128..=200 {
            for den in 1i64..=17 {
                assert_eq!(
                    round_half_even(&BigInt::from(num), den),
                    ledger_core::banker_div(num, den),
                    "disagreement at {num}/{den}"
                );
                assert_eq!(
                    round_half_even(&BigInt::from(num), -den),
                    ledger_core::banker_div(num, -den),
                    "disagreement at {num}/{}",
                    -den
                );
            }
        }
    }

    fn wac_pool() -> ModelLedger {
        let mut m = ModelLedger::new();
        m.add_pool(
            1,
            1,
            1,
            PoolMethod::Wac,
            ProvisionalBasis::RunningAvg,
            Some(uniform_accounts(1000, 2000, 3000)),
            None,
        );
        m
    }

    #[test]
    fn wac_running_average_and_depletion() {
        let mut m = wac_pool();
        // 10 @ 100, 10 @ 200 → avg 150.
        m.apply(&[recv(1, 10, 100)]).unwrap();
        m.apply(&[recv(1, 10, 200)]).unwrap();
        assert_eq!(m.aggregate(1).unwrap(), ExpectedAggregate { qty: 20, unit_cost: 150, value_sum: 3000 });
        // Deplete 5 @ running avg 150 → value_sum 3000-750=2250, qty 15, avg 150.
        let a = m.apply(&[depl(1, 5)]).unwrap();
        assert_eq!(a.lines[0].unit_cost, 150);
        assert_eq!(a.lines[0].source, ExpectedSource::Null);
        assert_eq!(a.postings[0], ExpectedPosting {
            event_type: "inventory_depletion",
            amount: 750,
            debit_account: 2000, // swapped
            credit_account: 1000,
        });
        assert_eq!(m.aggregate(1).unwrap(), ExpectedAggregate { qty: 15, unit_cost: 150, value_sum: 2250 });
    }

    #[test]
    fn depletion_beyond_on_hand_is_insufficient_inventory() {
        let mut m = wac_pool();
        m.apply(&[recv(1, 3, 100)]).unwrap();
        let e = m.apply(&[depl(1, 5)]).unwrap_err();
        assert_eq!(e, OracleError::InsufficientInventory { pool_id: 1, requested: 5, available: 3 });
        // Rolled back: state unchanged, no flat ids consumed.
        assert_eq!(m.aggregate(1).unwrap().qty, 3);
    }

    #[test]
    fn fifo_provisional_matches_wac_running_average() {
        let mut fifo = ModelLedger::new();
        fifo.add_pool(1, 1, 1, PoolMethod::Fifo, ProvisionalBasis::RunningAvg,
            Some(uniform_accounts(1000, 2000, 3000)), None);
        fifo.apply(&[recv(1, 10, 100)]).unwrap();
        fifo.apply(&[recv(1, 10, 200)]).unwrap();
        let a = fifo.apply(&[depl(1, 5)]).unwrap();
        assert_eq!(a.lines[0].unit_cost, 150); // running average, not layer cost
        assert_eq!(fifo.layer_count(1), 0); // provisional: no layers materialized
    }

    #[test]
    fn fifo_standard_basis_depletes_at_standard() {
        let mut m = ModelLedger::new();
        m.add_pool(1, 1, 1, PoolMethod::Fifo, ProvisionalBasis::Standard,
            Some(uniform_accounts(1000, 2000, 3000)), Some(500));
        m.apply(&[recv(1, 10, 100)]).unwrap();
        let a = m.apply(&[depl(1, 5)]).unwrap();
        assert_eq!(a.lines[0].unit_cost, 500); // standard, not the 100 running avg
        // value_sum can go negative: 1000 - 5*500 = -1500 while qty 5 > 0 (§3.5).
        let agg = m.aggregate(1).unwrap();
        assert_eq!(agg.qty, 5);
        assert_eq!(agg.value_sum, -1500);
    }

    #[test]
    fn fifo_standard_basis_missing_standard_cost_raises_on_depletion() {
        let mut m = ModelLedger::new();
        m.add_pool(1, 7, 9, PoolMethod::Fifo, ProvisionalBasis::Standard,
            Some(uniform_accounts(1000, 2000, 3000)), None);
        m.apply(&[recv(1, 10, 100)]).unwrap(); // receipt ok (no std needed)
        let e = m.apply(&[depl(1, 5)]).unwrap_err();
        assert_eq!(e, OracleError::MissingStandardCost { sku_id: 7, location_id: 9 });
    }

    #[test]
    fn std_receipt_emits_variance_and_depletes_at_standard() {
        let mut m = ModelLedger::new();
        m.add_pool(1, 1, 1, PoolMethod::Std, ProvisionalBasis::RunningAvg,
            Some(uniform_accounts(1000, 2000, 3000)), Some(100));
        // Receipt 10 @ actual 120, standard 100 → variance 10*(120-100)=200 unfavorable.
        let a = m.apply(&[recv(1, 10, 120)]).unwrap();
        assert_eq!(a.lines[0].unit_cost, 100);
        assert_eq!(a.postings.len(), 2);
        assert_eq!(a.postings[0], ExpectedPosting {
            event_type: "inventory_receipt", amount: 1000, debit_account: 1000, credit_account: 2000 });
        assert_eq!(a.postings[1], ExpectedPosting {
            event_type: "variance", amount: 200, debit_account: 3000, credit_account: 2000 });
        assert_eq!(m.aggregate(1).unwrap(), ExpectedAggregate { qty: 10, unit_cost: 100, value_sum: 1000 });
    }

    #[test]
    fn std_missing_standard_cost_raises_before_posting_accounts() {
        // Missing BOTH std and accounts → MissingStandardCost wins (resolved first).
        let mut m = ModelLedger::new();
        m.add_pool(1, 4, 5, PoolMethod::Std, ProvisionalBasis::RunningAvg, None, None);
        let e = m.apply(&[recv(1, 10, 120)]).unwrap_err();
        assert_eq!(e, OracleError::MissingStandardCost { sku_id: 4, location_id: 5 });
    }

    #[test]
    fn missing_posting_accounts_raises() {
        let mut m = ModelLedger::new();
        m.add_pool(1, 2, 3, PoolMethod::Wac, ProvisionalBasis::RunningAvg, None, None);
        let e = m.apply(&[recv(1, 10, 100)]).unwrap_err();
        assert_eq!(e, OracleError::MissingPostingAccounts { sku_id: 2, location_id: 3 });
    }

    #[test]
    fn unknown_pool_raises() {
        let mut m = ModelLedger::new();
        let e = m.apply(&[recv(99, 10, 100)]).unwrap_err();
        assert_eq!(e, OracleError::UnknownPool(99));
    }

    #[test]
    fn specific_receipt_then_depletion_links_and_second_receipt_rejected() {
        let mut m = ModelLedger::new();
        m.add_pool(1, 1, 1, PoolMethod::Specific, ProvisionalBasis::RunningAvg,
            Some(uniform_accounts(1000, 2000, 3000)), None);
        // Receipt is flat ordinal 0.
        let a0 = m.apply(&[recv(1, 1, 500)]).unwrap();
        assert_eq!(a0.lines[0].source, ExpectedSource::Null);
        assert_eq!(m.layer_count(1), 1);
        // Second receipt while stocked → K=1 rejection.
        let e = m.apply(&[recv(1, 1, 600)]).unwrap_err();
        assert_eq!(e, OracleError::SpecificPoolOccupied { pool_id: 1, existing_qty: 1 });
        // Depletion links back to the receipt at flat ordinal 0.
        let a1 = m.apply(&[depl(1, 1)]).unwrap();
        assert_eq!(a1.lines[0].unit_cost, 500);
        assert_eq!(a1.lines[0].source, ExpectedSource::ReceiptFlatIndex(0));
        assert_eq!(m.layer_count(1), 0);
        // Now empty → depletion is insufficient.
        let e = m.apply(&[depl(1, 1)]).unwrap_err();
        assert_eq!(e, OracleError::InsufficientInventory { pool_id: 1, requested: 1, available: 0 });
    }

    #[test]
    fn two_lines_same_pool_one_submission_coalesces_final_state() {
        let mut m = wac_pool();
        // One submission, two receipt lines on the same pool.
        let a = m.apply(&[recv(1, 10, 100), recv(1, 10, 300)]).unwrap();
        assert_eq!(a.lines.len(), 2);
        // Final aggregate reflects both (value 4000, qty 20, avg 200).
        assert_eq!(m.aggregate(1).unwrap(), ExpectedAggregate { qty: 20, unit_cost: 200, value_sum: 4000 });
    }

    #[test]
    fn receipts_only_pool_is_order_independent() {
        let mut a = wac_pool();
        a.apply(&[recv(1, 3, 111)]).unwrap();
        a.apply(&[recv(1, 7, 222)]).unwrap();
        a.apply(&[recv(1, 5, 333)]).unwrap();
        let mut b = wac_pool();
        b.apply(&[recv(1, 5, 333)]).unwrap();
        b.apply(&[recv(1, 3, 111)]).unwrap();
        b.apply(&[recv(1, 7, 222)]).unwrap();
        assert_eq!(a.aggregate(1), b.aggregate(1));
    }
}
