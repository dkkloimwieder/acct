-- acct-jtt — best-effort revert. Per project convention down is a
-- safety net, not a full restore: we recreate the dropped legacy
-- tables empty so referencing migrations re-apply, but the post_wo_*
-- bodies remain on the BOM2-only path. Reapply 0061 (and earlier
-- BOM2 migrations) from scratch on a fresh DB to recover the dispatch.

CREATE TABLE IF NOT EXISTS boms (
  parent_sku_id    UUID NOT NULL REFERENCES skus(id),
  component_sku_id UUID NOT NULL REFERENCES skus(id),
  component_loc_id UUID NOT NULL REFERENCES locations(id),
  qty_per_parent   BIGINT NOT NULL CHECK (qty_per_parent > 0),
  PRIMARY KEY (parent_sku_id, component_sku_id, component_loc_id)
);

CREATE TABLE IF NOT EXISTS wo_routing_burdens (
  wo_id                 UUID NOT NULL REFERENCES work_orders(id) ON DELETE CASCADE,
  routing_op            INT NOT NULL CHECK (routing_op > 0),
  applied_account_kind  account_kind NOT NULL,
  std_amount            BIGINT NOT NULL CHECK (std_amount > 0),
  PRIMARY KEY (wo_id, routing_op, applied_account_kind)
);
