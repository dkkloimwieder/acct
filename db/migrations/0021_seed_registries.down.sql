DELETE FROM close_hooks
 WHERE hook_fn_name IN (
   'wac_periodic_close_hook',
   'wac_retroactive_close_hook',
   'cost_adjust_retroactive_hook'
 );
