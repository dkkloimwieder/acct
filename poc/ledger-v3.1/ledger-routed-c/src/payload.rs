//! Arena payload encoding for ledger-routed (Path B).
//!
//! Each enqueued submission lives in TWO arena allocations:
//!
//!   1. **Submission block** — JSON-serialized [`PocV3Submission`]
//!      descriptor (small fixed-shape fields). Pointed at by
//!      `StagingEntry.payload_offset` + `payload_length`.
//!   2. **Lines block** — `u32` length prefix + JSON-serialized
//!      `Vec<PocV3Line>`. Pointed at by `PocV3Submission.line_offset`
//!      (which the encoder fills in after the lines block is
//!      allocated).
//!
//! The split lets each block be freed independently by the
//! committer's cleanup step. JSON encoding per locked design plan §E:
//! "rev to repr(C) only if measurement shows arena pressure."
//!
//! Length tracking: the submission block's length is carried by
//! `StagingEntry.payload_length` (passed back to `decode_submission`).
//! The lines block embeds its own JSON length as a `u32` prefix —
//! avoids adding a `line_length` field to `PocV3Submission`'s
//! 5-field surface and keeps the lines blob self-describing for
//! cleanup.

// Used by enqueue / committer modules as they land.
#![allow(dead_code)]

use serde::{Deserialize, Serialize};

use crate::shmem::SpilloverArena;

/// One submission descriptor. Allocated as a JSON blob; pointed at
/// by `StagingEntry.payload_offset` + `payload_length`.
///
/// `line_offset` is filled in by [`encode_submission`] after the
/// lines block is written — callers MUST NOT pre-populate it.
/// `line_count` is filled in by the encoder from `lines.len()`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PocV3Submission {
    pub trx_type: String,
    pub source_id: i64,
    pub posted_at_micros: i64,
    pub line_count: u32,
    pub line_offset: u32,
}

/// One line in a submission. Mirrors `TrxLineRequest` on the
/// ledger-core side; the SPI shim does the enum mapping between
/// `line_type` text and the SQL `line_type` enum at decode time.
///
/// `variance_account` carries the STD purchase-price-variance account
/// (design-v3.1 §3.3); `None` for non-STD lines. Defaulted on decode so
/// callers that omit it (the common case) deserialize cleanly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PocV3Line {
    pub line_type: String,
    pub source_id: Option<i64>,
    pub pool_id: i64,
    pub qty: i64,
    pub unit_cost: i64,
    pub debit_account: i64,
    pub credit_account: i64,
    #[serde(default)]
    pub variance_account: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadError {
    ArenaAllocFailed { wanted: u32 },
    LineCountOverflow { count: usize },
    LinesBytesOverflow { bytes: usize },
    PayloadBytesOverflow { bytes: usize },
    OutOfBoundsRead { offset: u32, len: u32, arena_bytes: usize },
    JsonDecode { detail: String },
    LineCountMismatch { declared: u32, actual: usize },
    /// Lines block too small to even hold the length prefix.
    LinesBlockTooSmall { length: u32 },
}

impl std::fmt::Display for PayloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PayloadError::ArenaAllocFailed { wanted } => {
                write!(f, "arena alloc failed for {wanted} bytes")
            }
            PayloadError::LineCountOverflow { count } => {
                write!(f, "line_count {count} > u32::MAX")
            }
            PayloadError::LinesBytesOverflow { bytes } => {
                write!(f, "lines blob {bytes} bytes > u32::MAX")
            }
            PayloadError::PayloadBytesOverflow { bytes } => {
                write!(f, "submission blob {bytes} bytes > u32::MAX")
            }
            PayloadError::OutOfBoundsRead { offset, len, arena_bytes } => {
                write!(
                    f,
                    "arena read out of bounds: offset={offset} len={len} arena_bytes={arena_bytes}"
                )
            }
            PayloadError::JsonDecode { detail } => write!(f, "json decode: {detail}"),
            PayloadError::LineCountMismatch { declared, actual } => {
                write!(f, "line_count mismatch: declared={declared} actual={actual}")
            }
            PayloadError::LinesBlockTooSmall { length } => {
                write!(f, "lines block too small for length prefix: {length}")
            }
        }
    }
}

impl std::error::Error for PayloadError {}

/// Encode one submission into the arena. Allocates two blocks (lines
/// then submission), writes JSON, returns the submission block's
/// `(payload_offset, payload_length)` for the caller to store on
/// `StagingEntry`. `submission`'s `line_count` and `line_offset` are
/// overwritten by the encoder.
///
/// On arena-alloc failure the partial allocations are freed before
/// returning Err, so the arena's outstanding-allocs counter does not
/// drift on the error path.
pub fn encode_submission(
    arena: &mut SpilloverArena,
    submission: &mut PocV3Submission,
    lines: &[PocV3Line],
) -> Result<(u32, u32), PayloadError> {
    let line_count: u32 = u32::try_from(lines.len())
        .map_err(|_| PayloadError::LineCountOverflow { count: lines.len() })?;

    // ── 1. Serialize lines blob (4-byte length prefix + JSON) ──
    let lines_json = serde_json::to_vec(lines).map_err(|e| PayloadError::JsonDecode {
        detail: format!("lines serialize: {e}"),
    })?;
    let lines_bytes_len: u32 = u32::try_from(lines_json.len())
        .map_err(|_| PayloadError::LinesBytesOverflow { bytes: lines_json.len() })?;
    let lines_block_len = lines_bytes_len.saturating_add(4);
    let lines_offset = arena
        .alloc(lines_block_len.max(1))
        .ok_or(PayloadError::ArenaAllocFailed { wanted: lines_block_len })?;
    // Write the length prefix first, then the JSON bytes.
    arena.write_bytes(lines_offset, &lines_bytes_len.to_le_bytes());
    arena.write_bytes(lines_offset + 4, &lines_json);

    // ── 2. Fill in submission's metadata + serialize ──
    submission.line_count = line_count;
    submission.line_offset = lines_offset;

    let sub_json = match serde_json::to_vec(submission) {
        Ok(b) => b,
        Err(e) => {
            arena.free(lines_offset);
            return Err(PayloadError::JsonDecode {
                detail: format!("submission serialize: {e}"),
            });
        }
    };
    let sub_bytes_len: u32 = match u32::try_from(sub_json.len()) {
        Ok(n) => n,
        Err(_) => {
            arena.free(lines_offset);
            return Err(PayloadError::PayloadBytesOverflow { bytes: sub_json.len() });
        }
    };

    let sub_offset = match arena.alloc(sub_bytes_len.max(1)) {
        Some(o) => o,
        None => {
            arena.free(lines_offset);
            return Err(PayloadError::ArenaAllocFailed { wanted: sub_bytes_len });
        }
    };
    arena.write_bytes(sub_offset, &sub_json);

    Ok((sub_offset, sub_bytes_len))
}

/// Decode one submission out of the arena. Reads the submission JSON
/// at `(payload_offset, payload_length)`, then follows
/// `submission.line_offset` to read the lines blob (4-byte length
/// prefix + JSON). Validates `line_count` against the actual
/// decoded line vector length.
pub fn decode_submission(
    arena: &SpilloverArena,
    payload_offset: u32,
    payload_length: u32,
) -> Result<(PocV3Submission, Vec<PocV3Line>), PayloadError> {
    let sub_bytes = read_bytes_bounded(arena, payload_offset, payload_length)?;
    let submission: PocV3Submission = serde_json::from_slice(&sub_bytes)
        .map_err(|e| PayloadError::JsonDecode {
            detail: format!("submission deserialize: {e}"),
        })?;

    // Lines blob: 4-byte length prefix + JSON.
    let prefix = read_bytes_bounded(arena, submission.line_offset, 4)?;
    let lines_bytes_len = u32::from_le_bytes(
        prefix
            .as_slice()
            .try_into()
            .map_err(|_| PayloadError::LinesBlockTooSmall { length: 4 })?,
    );
    let lines_json = read_bytes_bounded(arena, submission.line_offset + 4, lines_bytes_len)?;
    let lines: Vec<PocV3Line> = serde_json::from_slice(&lines_json).map_err(|e| {
        PayloadError::JsonDecode { detail: format!("lines deserialize: {e}") }
    })?;

    if lines.len() as u64 != submission.line_count as u64 {
        return Err(PayloadError::LineCountMismatch {
            declared: submission.line_count,
            actual: lines.len(),
        });
    }
    Ok((submission, lines))
}

/// Free both arena blocks owned by one encoded submission. Caller
/// passes the (offset, length) from `StagingEntry`, plus the decoded
/// submission's `line_offset` (which the cleanup step reads first via
/// `decode_submission`).
pub fn free_submission(
    arena: &mut SpilloverArena,
    payload_offset: u32,
    lines_offset: u32,
) {
    if lines_offset != 0 {
        arena.free(lines_offset);
    }
    if payload_offset != 0 {
        arena.free(payload_offset);
    }
}

fn read_bytes_bounded(
    arena: &SpilloverArena,
    offset: u32,
    len: u32,
) -> Result<Vec<u8>, PayloadError> {
    let arena_bytes = arena.bytes.len();
    let end = (offset as usize).checked_add(len as usize).ok_or(
        PayloadError::OutOfBoundsRead { offset, len, arena_bytes },
    )?;
    if end > arena_bytes {
        return Err(PayloadError::OutOfBoundsRead { offset, len, arena_bytes });
    }
    Ok(arena.read_bytes(offset, len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn fresh_arena() -> Box<SpilloverArena> {
        unsafe { Box::<SpilloverArena>::new_zeroed().assume_init() }
    }

    fn sample_submission() -> (PocV3Submission, Vec<PocV3Line>) {
        let submission = PocV3Submission {
            trx_type: "po_receipt".into(),
            source_id: 42,
            posted_at_micros: 1_700_000_000_000_000,
            line_count: 0,
            line_offset: 0,
        };
        let lines = vec![
            PocV3Line {
                line_type: "po_receipt_line".into(),
                source_id: Some(1001),
                pool_id: 7,
                qty: 10,
                unit_cost: 50,
                debit_account: 1,
                credit_account: 2,
                variance_account: None,
            },
            PocV3Line {
                line_type: "po_receipt_line".into(),
                source_id: None,
                pool_id: 8,
                qty: 5,
                unit_cost: 100,
                debit_account: 1,
                credit_account: 2,
                variance_account: Some(3),
            },
        ];
        (submission, lines)
    }

    #[test]
    fn round_trip_basic_two_lines() {
        let mut arena = fresh_arena();
        let (mut sub, lines) = sample_submission();
        let (off, len) = encode_submission(&mut arena, &mut sub, &lines).unwrap();
        assert_eq!(sub.line_count, 2);
        assert_ne!(sub.line_offset, 0);

        let (decoded_sub, decoded_lines) = decode_submission(&arena, off, len).unwrap();
        assert_eq!(decoded_sub, sub);
        assert_eq!(decoded_lines, lines);
    }

    #[test]
    fn empty_lines_round_trips() {
        let mut arena = fresh_arena();
        let (mut sub, _) = sample_submission();
        let lines: Vec<PocV3Line> = Vec::new();
        let (off, len) = encode_submission(&mut arena, &mut sub, &lines).unwrap();
        assert_eq!(sub.line_count, 0);

        let (decoded_sub, decoded_lines) = decode_submission(&arena, off, len).unwrap();
        assert_eq!(decoded_sub, sub);
        assert_eq!(decoded_lines, lines);
    }

    #[test]
    fn line_count_mismatch_after_tamper_returns_err() {
        let mut arena = fresh_arena();
        let (mut sub, lines) = sample_submission();
        let (off, len) = encode_submission(&mut arena, &mut sub, &lines).unwrap();

        // Tamper: overwrite the lines length prefix to point at a
        // shorter slice that decodes to fewer lines.
        let truncated: Vec<u8> =
            serde_json::to_vec(&vec![lines[0].clone()]).unwrap();
        let truncated_len: u32 = truncated.len() as u32;
        arena.write_bytes(sub.line_offset, &truncated_len.to_le_bytes());
        arena.write_bytes(sub.line_offset + 4, &truncated);

        let err = decode_submission(&arena, off, len).unwrap_err();
        match err {
            PayloadError::LineCountMismatch { declared: 2, actual: 1 } => {}
            other => panic!("expected LineCountMismatch, got {other:?}"),
        }
    }

    #[test]
    fn out_of_bounds_payload_offset_returns_err() {
        let arena = fresh_arena();
        let bad_off = (arena.bytes.len() as u32) - 4;
        let err = decode_submission(&arena, bad_off, 1024).unwrap_err();
        assert!(matches!(err, PayloadError::OutOfBoundsRead { .. }));
    }

    #[test]
    fn invalid_json_in_payload_returns_err() {
        let mut arena = fresh_arena();
        let bytes = b"not valid json";
        let off = arena.alloc(bytes.len() as u32).unwrap();
        arena.write_bytes(off, bytes);
        let err = decode_submission(&arena, off, bytes.len() as u32).unwrap_err();
        assert!(matches!(err, PayloadError::JsonDecode { .. }));
    }

    #[test]
    fn free_submission_returns_arena_outstanding_to_zero() {
        let mut arena = fresh_arena();
        let (mut sub, lines) = sample_submission();
        let (off, _len) = encode_submission(&mut arena, &mut sub, &lines).unwrap();
        assert_eq!(arena.outstanding_allocs(), 2);
        free_submission(&mut arena, off, sub.line_offset);
        assert_eq!(arena.outstanding_allocs(), 0);
    }

    // ── Property test: arbitrary submission + lines round-trip ──

    fn arb_line() -> impl Strategy<Value = PocV3Line> {
        (
            prop_oneof![
                Just("po_receipt_line".to_string()),
                Just("inv_adjustment_line".to_string()),
                Just("transfer_shipment_line".to_string()),
            ],
            prop::option::of(any::<i64>()),
            1i64..=10_000,
            -100_000i64..=100_000,
            1i64..=1_000_000,
            1i64..=64,
            1i64..=64,
            prop::option::of(1i64..=64),
        )
            .prop_map(
                |(line_type, source_id, pool_id, qty, unit_cost, debit, credit, variance)| {
                    PocV3Line {
                        line_type,
                        source_id,
                        pool_id,
                        qty,
                        unit_cost,
                        debit_account: debit,
                        credit_account: credit,
                        variance_account: variance,
                    }
                },
            )
    }

    fn arb_submission_and_lines() -> impl Strategy<Value = (String, i64, i64, Vec<PocV3Line>)>
    {
        (
            prop_oneof![
                Just("po_receipt".to_string()),
                Just("inv_adjustment".to_string()),
                Just("manual_adjustment".to_string()),
            ],
            any::<i64>(),
            any::<i64>(),
            prop::collection::vec(arb_line(), 0..=20),
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 64,
            max_shrink_iters: 32,
            .. ProptestConfig::default()
        })]

        #[test]
        fn round_trip_property(
            (trx_type, source_id, posted_at_micros, lines) in arb_submission_and_lines()
        ) {
            let mut arena = fresh_arena();
            let mut sub = PocV3Submission {
                trx_type: trx_type.clone(),
                source_id,
                posted_at_micros,
                line_count: 0,
                line_offset: 0,
            };
            let (off, len) = encode_submission(&mut arena, &mut sub, &lines).unwrap();
            let (decoded_sub, decoded_lines) =
                decode_submission(&arena, off, len).unwrap();
            prop_assert_eq!(decoded_sub.trx_type, trx_type);
            prop_assert_eq!(decoded_sub.source_id, source_id);
            prop_assert_eq!(decoded_sub.posted_at_micros, posted_at_micros);
            prop_assert_eq!(decoded_sub.line_count as usize, lines.len());
            prop_assert_eq!(decoded_lines, lines);

            free_submission(&mut arena, off, decoded_sub.line_offset);
            prop_assert_eq!(arena.outstanding_allocs(), 0);
        }
    }
}
