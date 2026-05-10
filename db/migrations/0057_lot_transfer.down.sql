-- Best-effort down (project convention; Phase 0/1 has no production data).

DROP FUNCTION IF EXISTS post_lot_transfer(UUID, UUID, JSONB, DATE, UUID, UUID, TEXT);

DROP TABLE IF EXISTS lot_transfer_lines;
DROP TABLE IF EXISTS lot_transfers;

-- Restore _post_posting_lines_apply_event to mig 0046's body
-- (without the v_reason <> 'lot_transfer' gate). Then restore the
-- _inventory_movement_event_type body (without 'lot_transfer'
-- mapping).

CREATE OR REPLACE FUNCTION _inventory_movement_event_type(
  p_reason     posting_line_reason,
  p_signed_qty NUMERIC
) RETURNS SMALLINT
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  RETURN CASE p_reason::TEXT
    WHEN 'po_receipt'              THEN 1
    WHEN 'po_receipt_provisional'  THEN 1
    WHEN 'so_ship'                 THEN 2
    WHEN 'to_release'              THEN 3
    WHEN 'osp_ship'                THEN 3
    WHEN 'to_receipt'              THEN 4
    WHEN 'osp_receive'             THEN 4
    WHEN 'cycle_count_adj'
      THEN CASE WHEN p_signed_qty > 0 THEN 5 ELSE 6 END
    WHEN 'inventory_adjustment'
      THEN CASE WHEN p_signed_qty > 0 THEN 5 ELSE 6 END
    WHEN 'scrap'                   THEN 7
    WHEN 'scrap_v'                 THEN 7
    WHEN 'rm_issue_to_wo'          THEN 8
    WHEN 'wo_complete'             THEN 9
    WHEN 'wo_complete_v'           THEN 9
    WHEN 'op_move'                 THEN 11
    WHEN 'op_move_v'               THEN 11
    WHEN 'customer_return'         THEN 12
    WHEN 'po_return_to_vendor'     THEN 13
    WHEN 'standard_cost_roll'      THEN 14
    WHEN 'cost_adjustment'         THEN 16
    WHEN 'cost_restate'            THEN 16
    WHEN 'wo_close_v'              THEN 16
    ELSE NULL
  END;
END;
$$;

-- _post_posting_lines_apply_event left in current state (mig 0057's
-- gate is benign — the only change is `v_reason <> 'lot_transfer'`
-- which never fires when the enum value isn't used elsewhere).
