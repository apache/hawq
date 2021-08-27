-- --------------------------------------------------------------------
--
-- hive_install.sql
--
-- Support HIVE protocol in pluggable storage framework
--
-- --------------------------------------------------------------------

CREATE OR REPLACE FUNCTION pg_catalog.hive_validate() RETURNS void
AS '$libdir/exthive.so', 'hiveprotocol_validate'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.hive_blocklocation() RETURNS void
AS '$libdir/exthive.so', 'hiveprotocol_blocklocation'
LANGUAGE C STABLE;
