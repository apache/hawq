-- --------------------------------------------------------------------
--
-- orc_install.sql
--
-- Support ORC format in pluggable storage framework
--
-- --------------------------------------------------------------------
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
DROP FUNCTION IF EXISTS pg_catalog.hdfs_validate();
DROP FUNCTION IF EXISTS pg_catalog.hdfs_blocklocation();
DROP FUNCTION IF EXISTS pg_catalog.csv_in();
DROP FUNCTION IF EXISTS pg_catalog.csv_out(record);
DROP FUNCTION IF EXISTS pg_catalog.text_in();
DROP FUNCTION IF EXISTS pg_catalog.text_out(record);

SET allow_system_table_mods=ddl;
set gen_new_oid_value to 10915;
CREATE FUNCTION pg_catalog.orc_validate_interfaces() RETURNS void
AS '$libdir/orc.so', 'orc_validate_interfaces'
LANGUAGE C STABLE;

set gen_new_oid_value to 10916;
CREATE FUNCTION pg_catalog.orc_validate_options() RETURNS void
AS '$libdir/orc.so', 'orc_validate_options'
LANGUAGE C STABLE;

set gen_new_oid_value to 10917;
CREATE FUNCTION pg_catalog.orc_validate_encodings() RETURNS void
AS '$libdir/orc.so', 'orc_validate_encodings'
LANGUAGE C STABLE;

set gen_new_oid_value to 10918;
CREATE FUNCTION pg_catalog.orc_validate_datatypes() RETURNS void
AS '$libdir/orc.so', 'orc_validate_datatypes'
LANGUAGE C STABLE;

set gen_new_oid_value to 10919;
CREATE FUNCTION pg_catalog.orc_beginscan() RETURNS bytea
AS '$libdir/orc.so', 'orc_beginscan'
LANGUAGE C STABLE;

set gen_new_oid_value to 10920;
CREATE FUNCTION pg_catalog.orc_getnext_init() RETURNS bytea
AS '$libdir/orc.so', 'orc_getnext_init'
LANGUAGE C STABLE;

set gen_new_oid_value to 10921;
CREATE FUNCTION pg_catalog.orc_getnext() RETURNS bytea
AS '$libdir/orc.so', 'orc_getnext'
LANGUAGE C STABLE;

set gen_new_oid_value to 10922;
CREATE FUNCTION pg_catalog.orc_rescan() RETURNS void
AS '$libdir/orc.so', 'orc_rescan'
LANGUAGE C STABLE;

set gen_new_oid_value to 10923;
CREATE FUNCTION pg_catalog.orc_endscan() RETURNS void
AS '$libdir/orc.so', 'orc_endscan'
LANGUAGE C STABLE;

set gen_new_oid_value to 10924;
CREATE FUNCTION pg_catalog.orc_stopscan() RETURNS void
AS '$libdir/orc.so', 'orc_stopscan'
LANGUAGE C STABLE;

set gen_new_oid_value to 10925;
CREATE FUNCTION pg_catalog.orc_insert_init() RETURNS bytea
AS '$libdir/orc.so', 'orc_insert_init'
LANGUAGE C STABLE;

set gen_new_oid_value to 10926;
CREATE FUNCTION pg_catalog.orc_insert() RETURNS bytea
AS '$libdir/orc.so', 'orc_insert'
LANGUAGE C STABLE;

set gen_new_oid_value to 10927;
CREATE FUNCTION pg_catalog.orc_insert_finish() RETURNS void
AS '$libdir/orc.so', 'orc_insert_finish'
LANGUAGE C STABLE;

set gen_new_oid_value to 10906;
CREATE FUNCTION pg_catalog.hdfs_validate() RETURNS void
AS '$libdir/exthdfs.so', 'hdfsprotocol_validate'
LANGUAGE C STABLE;

set gen_new_oid_value to 10907;
CREATE FUNCTION pg_catalog.hdfs_blocklocation() RETURNS void
AS '$libdir/exthdfs.so', 'hdfsprotocol_blocklocation'
LANGUAGE C STABLE;

set gen_new_oid_value to 10910;
CREATE FUNCTION pg_catalog.csv_in() RETURNS record
AS '$libdir/extfmtcsv.so', 'extfmtcsv_in'
LANGUAGE C STABLE;

set gen_new_oid_value to 10911;
CREATE FUNCTION pg_catalog.csv_out(record) RETURNS bytea
AS '$libdir/extfmtcsv.so', 'extfmtcsv_out'
LANGUAGE C STABLE;

set gen_new_oid_value to 10912;
CREATE FUNCTION pg_catalog.text_in() RETURNS record
AS '$libdir/extfmtcsv.so', 'extfmttext_in'
LANGUAGE C STABLE;

set gen_new_oid_value to 10913;
CREATE FUNCTION pg_catalog.text_out(record) RETURNS bytea
AS '$libdir/extfmtcsv.so', 'extfmttext_out'
LANGUAGE C STABLE;

reset gen_new_oid_value;
