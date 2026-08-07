//! `line_type` text → `ledger_core::LineType` decoder.
//!
//! The inverse of `LineType::as_sql()`. Both extension flavors decode the SQL
//! `line_type` enum text the caller supplies (direct from JSONB in
//! `submit::ledger_submit_trx_c`; routed from the staged payload in
//! `committer::decode_lines`) into the ledger-core enum before planning.

use ledger_core::LineType;

/// Map a SQL `line_type` enum text value to [`LineType`]. Returns `None` for any
/// unrecognized text (the caller surfaces it as an invalid-line error).
pub fn decode_line_type(s: &str) -> Option<LineType> {
    match s {
        "po_receipt_line" => Some(LineType::PoReceiptLine),
        "wo_output" => Some(LineType::WoOutput),
        "wo_backflush" => Some(LineType::WoBackflush),
        "wo_scrap" => Some(LineType::WoScrap),
        "inv_adjustment_line" => Some(LineType::InvAdjustmentLine),
        "transfer_shipment_line" => Some(LineType::TransferShipmentLine),
        "transfer_receipt_line" => Some(LineType::TransferReceiptLine),
        "manual_adjustment_line" => Some(LineType::ManualAdjustmentLine),
        "revaluation_line" => Some(LineType::RevaluationLine),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_line_type_maps_all_sql_enum_variants() {
        assert_eq!(decode_line_type("po_receipt_line"), Some(LineType::PoReceiptLine));
        assert_eq!(decode_line_type("wo_output"), Some(LineType::WoOutput));
        assert_eq!(decode_line_type("wo_backflush"), Some(LineType::WoBackflush));
        assert_eq!(decode_line_type("wo_scrap"), Some(LineType::WoScrap));
        assert_eq!(decode_line_type("inv_adjustment_line"), Some(LineType::InvAdjustmentLine));
        assert_eq!(
            decode_line_type("transfer_shipment_line"),
            Some(LineType::TransferShipmentLine)
        );
        assert_eq!(
            decode_line_type("transfer_receipt_line"),
            Some(LineType::TransferReceiptLine)
        );
        assert_eq!(
            decode_line_type("manual_adjustment_line"),
            Some(LineType::ManualAdjustmentLine)
        );
        assert_eq!(decode_line_type("revaluation_line"), Some(LineType::RevaluationLine));
    }

    #[test]
    fn decode_line_type_unknown_returns_none() {
        assert_eq!(decode_line_type(""), None);
        assert_eq!(decode_line_type("not_a_real_type"), None);
        assert_eq!(decode_line_type("PO_RECEIPT_LINE"), None);
    }
}
