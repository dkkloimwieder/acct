DROP FUNCTION IF EXISTS post_inventory_adjustment(
  UUID, UUID, BIGINT, BIGINT, TEXT, TEXT, DATE, UUID, UUID, TEXT
);

DROP TABLE IF EXISTS inventory_adjustments;
