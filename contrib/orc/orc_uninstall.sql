-- --------------------------------------------------------------------
--
-- orc_uninstall.sql
--
-- Remove ORC format in pluggable storage framework
--
-- --------------------------------------------------------------------

SET allow_system_table_mods=ddl;
  
DROP FUNCTION IF EXISTS pg_catalog.orc_validate_interfaces();

DROP FUNCTION IF EXISTS pg_catalog.orc_validate_options();

DROP FUNCTION IF EXISTS pg_catalog.orc_validate_encodings();

DROP FUNCTION IF EXISTS pg_catalog.orc_validate_datatypes();

DROP FUNCTION IF EXISTS pg_catalog.orc_beginscan();

DROP FUNCTION IF EXISTS pg_catalog.orc_getnext_init();

DROP FUNCTION IF EXISTS pg_catalog.orc_getnext();

DROP FUNCTION IF EXISTS pg_catalog.orc_rescan();

DROP FUNCTION IF EXISTS pg_catalog.orc_endscan();

DROP FUNCTION IF EXISTS pg_catalog.orc_stopscan();

DROP FUNCTION IF EXISTS pg_catalog.orc_insert_init();

DROP FUNCTION IF EXISTS pg_catalog.orc_insert();

DROP FUNCTION IF EXISTS pg_catalog.orc_insert_finish();

