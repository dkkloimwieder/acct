-- Restore post_ap_bill from 0016 form (without disposal_match) by dropping
-- the consolidated 0017 replacement. The down for vendor_bill_lines schema
-- changes is best-effort: recreate the original CHECK; drop the new
-- columns; drop the disposal_event index.

DROP FUNCTION IF EXISTS post_ap_bill(UUID, CHAR, JSONB, DATE, UUID, UUID, TEXT);

DROP INDEX IF EXISTS vendor_bill_lines_disposal_event;

ALTER TABLE vendor_bill_lines DROP CONSTRAINT IF EXISTS vendor_bill_lines_check;
ALTER TABLE vendor_bill_lines DROP CONSTRAINT IF EXISTS vendor_bill_lines_kind_check;

ALTER TABLE vendor_bill_lines DROP COLUMN IF EXISTS disposal_wo_event_id;
ALTER TABLE vendor_bill_lines DROP COLUMN IF EXISTS by_product_no;

DROP FUNCTION IF EXISTS post_osp_receive(UUID, INT, BIGINT, UUID, DATE, UUID, UUID, TEXT);
DROP FUNCTION IF EXISTS post_osp_ship(UUID, INT, BIGINT, UUID, DATE, UUID, UUID, TEXT);
DROP FUNCTION IF EXISTS post_wo_close_unproduced(UUID, DATE, UUID, UUID, TEXT);
DROP FUNCTION IF EXISTS post_scrap(UUID, INT, BIGINT, DATE, UUID, UUID, TEXT);
DROP FUNCTION IF EXISTS post_wo_complete(UUID, BIGINT, DATE, UUID, UUID, TEXT);
DROP FUNCTION IF EXISTS post_op_move(UUID, INT, INT, BIGINT, DATE, UUID, UUID, TEXT);
DROP FUNCTION IF EXISTS post_wo_start(UUID, DATE, UUID, UUID, TEXT);
DROP FUNCTION IF EXISTS post_eco_approve(BIGINT, DATE, UUID);

DROP FUNCTION IF EXISTS _wo_emit_bom_lines(UUID, BIGINT, INT, BIGINT, JSONB, UUID, DATE, UUID, TEXT);
DROP FUNCTION IF EXISTS _wo_apply_reason_for(BIGINT, TEXT);
DROP FUNCTION IF EXISTS _wo_explode_bom(BIGINT, DATE);
DROP FUNCTION IF EXISTS _wo_resolve_bom_for(UUID, DATE);
DROP FUNCTION IF EXISTS _bom_header_at(UUID, INT, DATE);

DROP TRIGGER IF EXISTS wo_by_products_planned_qty_immutable_trg ON wo_by_products;
DROP FUNCTION IF EXISTS wo_by_products_planned_qty_immutable();

DROP TABLE IF EXISTS wo_by_products;

DROP TRIGGER IF EXISTS wo_events_check_consumption_policy ON wo_events;
DROP FUNCTION IF EXISTS _wo_events_check_consumption_policy();

DROP TABLE IF EXISTS wo_events;
DROP TABLE IF EXISTS wo_outputs;
DROP TABLE IF EXISTS wo_routings;
DROP TABLE IF EXISTS work_orders;

DROP TABLE IF EXISTS bom_by_products;

DROP TRIGGER IF EXISTS bom_line_self_reference_check ON bom_lines;
DROP FUNCTION IF EXISTS _bom_line_self_reference_guard();

DROP TABLE IF EXISTS bom_lines;
DROP TABLE IF EXISTS bom_headers;
DROP TABLE IF EXISTS engineering_change_orders;
DROP TABLE IF EXISTS absorption_classes;

-- Note: post_ap_bill was originally created in 0016. Restoring the 0016
-- form in this down would require re-issuing the 0016 CREATE OR REPLACE.
-- For now, run `sqlx migrate run` after a `revert` to re-apply 0016's
-- post_ap_bill body. The ci-check.sh revert+redeploy cycle does this
-- automatically. (Phase 0/1 has no production data; downs are best-effort
-- per project convention.)
