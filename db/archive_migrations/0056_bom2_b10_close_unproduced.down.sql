-- acct-7he — rollback of B10.

DROP FUNCTION IF EXISTS post_wo_close_unproduced(UUID, DATE, UUID, UUID, TEXT);

ALTER TABLE wo_events DROP CONSTRAINT wo_events_event_kind_check;
ALTER TABLE wo_events ADD CONSTRAINT wo_events_event_kind_check
  CHECK (event_kind IN ('start','op_move','wo_complete','scrap'));

ALTER TABLE wo_events DROP CONSTRAINT wo_events_check;
ALTER TABLE wo_events ADD CONSTRAINT wo_events_check CHECK (
  (event_kind = 'start'
   AND routing_op_from IS NULL AND routing_op_to IS NULL AND qty IS NULL)
  OR
  (event_kind = 'op_move'
   AND routing_op_from IS NOT NULL AND routing_op_to IS NOT NULL
   AND qty IS NOT NULL)
  OR
  (event_kind = 'wo_complete'
   AND routing_op_from IS NOT NULL AND routing_op_to IS NULL
   AND qty IS NOT NULL)
  OR
  (event_kind = 'scrap'
   AND routing_op_from IS NOT NULL AND routing_op_to IS NULL
   AND qty IS NOT NULL)
);
