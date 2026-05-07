-- acct-3uh — rollback of acct-2yu (BOM2 B11, migration 0057).
--
-- post_osp_ship / post_osp_receive were a premature special case ahead
-- of Slice C (transfer orders / shipments / receipts). OSP fee
-- accounting works without them via bom_line of kind='service'/'charge'
-- with absorption_pool class fired by _wo_emit_bom_lines on first
-- arrival at the OSP op; physical custody tracking is re-evaluated
-- once Slice C produces generic transfer-order primitives.
--
-- See acct-3uh for the trade-off analysis and standard-ERP modelling
-- approaches. Migrations are append-only; this is the supersede.
--
-- Rolled back from this migration:
--   DROP FUNCTION post_osp_ship   (was in 0057)
--   DROP FUNCTION post_osp_receive (was in 0057)
--
-- NOT rolled back (per project convention — ALTER TYPE ADD VALUE
-- down-migration is best-effort; values stay as unused enum labels):
--   transfer_reason: osp_ship, osp_receive, phantom_explode,
--                    lot_charge_apply, burden_apply
--   account_kind:    absorption_pool, stock_consigned_at_vendor
--
-- The unused values are reserved for the eventual acct-3uh
-- re-implementation. They are harmless if no caller references them
-- (no transfer carries those reasons today; no account is of kind
-- stock_consigned_at_vendor today).

DROP FUNCTION IF EXISTS post_osp_receive(UUID, INT, BIGINT, UUID, DATE, UUID, UUID, TEXT);
DROP FUNCTION IF EXISTS post_osp_ship(UUID, INT, BIGINT, UUID, DATE, UUID, UUID, TEXT);
