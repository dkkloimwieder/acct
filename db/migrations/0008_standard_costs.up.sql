-- Standard costs as a separate transactional entity.
--
-- Consolidates archive mig 0027 (acct-x4t / acct-hlr.1) which dropped
-- skus.standard_cost and replaced it with this append-only stream.
-- Function renamed: resolve_standard_cost_at → _resolve_standard_cost_at
-- (per the helper-discipline pass; was a public name but is internally-
-- used only).

CREATE TABLE standard_costs (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  sku_id          UUID NOT NULL REFERENCES skus(id),
  cost            BIGINT NOT NULL CHECK (cost >= 0),
  effective_at    DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);

-- Hot path for _resolve_standard_cost_at: latest effective_at <=
-- business_date for a given SKU. DESC index lets the LIMIT-1 walk
-- backwards from the requested date.
CREATE INDEX std_cost_sku_eff
  ON standard_costs (sku_id, effective_at DESC);

-- _resolve_standard_cost_at: canonical lookup. Returns the active cost
-- (latest effective_at <= business_date) or raises P0018 if none.
-- Cost-relevant operations on standard SKUs that go through this fn
-- inherit the gate automatically.
CREATE OR REPLACE FUNCTION _resolve_standard_cost_at(
  p_sku_id        UUID,
  p_business_date DATE
) RETURNS BIGINT
LANGUAGE plpgsql STABLE
AS $$
DECLARE
  v_cost BIGINT;
BEGIN
  SELECT cost INTO v_cost
    FROM standard_costs
   WHERE sku_id = p_sku_id
     AND effective_at <= p_business_date
   ORDER BY effective_at DESC
   LIMIT 1;
  IF NOT FOUND THEN
    RAISE EXCEPTION
      'standard_cost_not_established: sku=% has no standard cost in effect as of %',
      p_sku_id, p_business_date
      USING ERRCODE = 'P0018';
  END IF;
  RETURN v_cost;
END;
$$;
