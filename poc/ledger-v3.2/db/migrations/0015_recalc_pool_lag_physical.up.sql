-- G2a/G2b measure PHYSICAL events awaiting authoritative costing, so the
-- per-pool lag view must exclude line_type = 'cost_adjustment_line': those
-- rows are the recalc engine's own output (stamped posted_at = now(), always
-- ahead of the settlement frontier) and would otherwise count as
-- forever-unsettled events, permanently inflating the tip and the
-- unsettled-tail count/value.

CREATE OR REPLACE VIEW recalc_pool_lag AS
SELECT p.id AS pool_id,
       ps.recalc_generation,
       ps.settled_through_posted_at,
       ps.settled_through_id,
       ps.recost_floor_posted_at,
       ps.recost_floor_id,
       q.enqueued_at AS dirty_since,
       tip.posted_at AS tip_posted_at,
       tip.id        AS tip_id,
       lag.unsettled_events,
       lag.unsettled_gross_value
FROM pool p
LEFT JOIN pool_settlement ps ON ps.pool_id = p.id
LEFT JOIN recalc_queue q ON q.pool_id = p.id
LEFT JOIN LATERAL (
    SELECT t.posted_at, t.id
    FROM trx_line t
    WHERE t.pool_id = p.id
      AND t.line_type <> 'cost_adjustment_line'
    ORDER BY t.posted_at DESC, t.id DESC
    LIMIT 1
) tip ON true
LEFT JOIN LATERAL (
    SELECT count(*) AS unsettled_events,
           COALESCE(sum(abs(t.qty) * t.unit_cost), 0)::bigint AS unsettled_gross_value
    FROM trx_line t
    WHERE t.pool_id = p.id
      AND t.line_type <> 'cost_adjustment_line'
      AND (ps.settled_through_posted_at IS NULL
           OR (t.posted_at, t.id)
              > (ps.settled_through_posted_at, ps.settled_through_id))
) lag ON true;
