SET search_path = mdver_utils;

BEGIN;

DROP VIEW mdver_utils.gp_mdver_cache_entries;
DROP FUNCTION mdver_utils.__gp_mdver_cache_entries_f(); 

DROP SCHEMA mdver_utils;

COMMIT; 
