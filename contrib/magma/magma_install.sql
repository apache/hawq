------------------------------------------------------------------
-- magma protocol
------------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.magma_validate();
DROP FUNCTION IF EXISTS pg_catalog.magma_blocklocation();
DROP FUNCTION IF EXISTS pg_catalog.magma_tablesize();
DROP FUNCTION IF EXISTS pg_catalog.magma_databasesize();
DROP FUNCTION IF EXISTS pg_catalog.magma_getstatus();

set gen_new_oid_value to 11000;
CREATE FUNCTION pg_catalog.magma_validate() RETURNS void
AS '$libdir/magma.so', 'magma_protocol_validate'
LANGUAGE C STABLE;

set gen_new_oid_value to 11001;
CREATE FUNCTION pg_catalog.magma_blocklocation() RETURNS void
AS '$libdir/magma.so', 'magma_protocol_blocklocation'
LANGUAGE C STABLE;

set gen_new_oid_value to 11002;
CREATE FUNCTION pg_catalog.magma_tablesize() RETURNS void
AS '$libdir/magma.so', 'magma_protocol_tablesize'
LANGUAGE C STABLE;

set gen_new_oid_value to 11003;
CREATE FUNCTION pg_catalog.magma_databasesize() RETURNS void
AS '$libdir/magma.so', 'magma_protocol_databasesize'
LANGUAGE C STABLE;

-- this function is builtin function which can not be dropped, replace if exsits
set gen_new_oid_value to 5085;
CREATE OR REPLACE FUNCTION pg_catalog.hawq_magma_status() RETURNS
SETOF record LANGUAGE internal VOLATILE STRICT AS 'hawq_magma_status';

------------------------------------------------------------------
-- magma format
------------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.magma_validate_interfaces();
DROP FUNCTION IF EXISTS pg_catalog.magmaap_validate_interfaces();
DROP FUNCTION IF EXISTS pg_catalog.magmatp_validate_interfaces();
DROP FUNCTION IF EXISTS pg_catalog.magma_validate_options();
DROP FUNCTION IF EXISTS pg_catalog.magmaap_validate_options();
DROP FUNCTION IF EXISTS pg_catalog.magmatp_validate_options();
DROP FUNCTION IF EXISTS pg_catalog.magma_validate_encodings();
DROP FUNCTION IF EXISTS pg_catalog.magmaap_validate_encodings();
DROP FUNCTION IF EXISTS pg_catalog.magmatp_validate_encodings();
DROP FUNCTION IF EXISTS pg_catalog.magma_validate_datatypes();
DROP FUNCTION IF EXISTS pg_catalog.magmaap_validate_datatypes();
DROP FUNCTION IF EXISTS pg_catalog.magmatp_validate_datatypes();
DROP FUNCTION IF EXISTS pg_catalog.magma_createtable();
DROP FUNCTION IF EXISTS pg_catalog.magma_droptable();
DROP FUNCTION IF EXISTS pg_catalog.magma_createindex();
DROP FUNCTION IF EXISTS pg_catalog.magma_dropindex();
DROP FUNCTION IF EXISTS pg_catalog.magma_reindex_index();
DROP FUNCTION IF EXISTS pg_catalog.magmatp_beginscan();
DROP FUNCTION IF EXISTS pg_catalog.magmatp_getnext_init();
DROP FUNCTION IF EXISTS pg_catalog.magmatp_getnext();
DROP FUNCTION IF EXISTS pg_catalog.magmatp_rescan();
DROP FUNCTION IF EXISTS pg_catalog.magmatp_endscan();
DROP FUNCTION IF EXISTS pg_catalog.magmatp_stopscan();
DROP FUNCTION IF EXISTS pg_catalog.magmatp_begindelete();
DROP FUNCTION IF EXISTS pg_catalog.magmatp_delete();
DROP FUNCTION IF EXISTS pg_catalog.magmatp_enddelete();
DROP FUNCTION IF EXISTS pg_catalog.magmatp_beginupdate();
DROP FUNCTION IF EXISTS pg_catalog.magmatp_update();
DROP FUNCTION IF EXISTS pg_catalog.magmatp_endupdate();
DROP FUNCTION IF EXISTS pg_catalog.magmatp_insert_init();
DROP FUNCTION IF EXISTS pg_catalog.magmatp_insert();
DROP FUNCTION IF EXISTS pg_catalog.magmatp_insert_finish();
DROP FUNCTION IF EXISTS pg_catalog.magma_transaction();
DROP FUNCTION IF EXISTS pg_catalog.magmaap_beginscan();
DROP FUNCTION IF EXISTS pg_catalog.magmaap_getnext_init();
DROP FUNCTION IF EXISTS pg_catalog.magmaap_getnext();
DROP FUNCTION IF EXISTS pg_catalog.magmaap_rescan();
DROP FUNCTION IF EXISTS pg_catalog.magmaap_endscan();
DROP FUNCTION IF EXISTS pg_catalog.magmaap_stopscan();
DROP FUNCTION IF EXISTS pg_catalog.magmaap_begindelete();
DROP FUNCTION IF EXISTS pg_catalog.magmaap_delete();
DROP FUNCTION IF EXISTS pg_catalog.magmaap_enddelete();
DROP FUNCTION IF EXISTS pg_catalog.magmaap_beginupdate();
DROP FUNCTION IF EXISTS pg_catalog.magmaap_update();
DROP FUNCTION IF EXISTS pg_catalog.magmaap_endupdate();
DROP FUNCTION IF EXISTS pg_catalog.magmaap_insert_init();
DROP FUNCTION IF EXISTS pg_catalog.magmaap_insert();
DROP FUNCTION IF EXISTS pg_catalog.magmaap_insert_finish();

set gen_new_oid_value to 11004;
CREATE FUNCTION pg_catalog.magma_validate_interfaces() RETURNS void
AS '$libdir/magma.so', 'magma_validate_interfaces'
LANGUAGE C STABLE;

set gen_new_oid_value to 11005;
CREATE FUNCTION pg_catalog.magmaap_validate_interfaces() RETURNS void
AS '$libdir/magma.so', 'magma_validate_interfaces'
LANGUAGE C STABLE;

set gen_new_oid_value to 11006;
CREATE FUNCTION pg_catalog.magmatp_validate_interfaces() RETURNS void
AS '$libdir/magma.so', 'magma_validate_interfaces'
LANGUAGE C STABLE;

set gen_new_oid_value to 11007;
CREATE FUNCTION pg_catalog.magma_validate_options() RETURNS void
AS '$libdir/magma.so', 'magma_validate_options'
LANGUAGE C STABLE;

set gen_new_oid_value to 11008;
CREATE FUNCTION pg_catalog.magmaap_validate_options() RETURNS void
AS '$libdir/magma.so', 'magma_validate_options'
LANGUAGE C STABLE;

set gen_new_oid_value to 11009;
CREATE FUNCTION pg_catalog.magmatp_validate_options() RETURNS void
AS '$libdir/magma.so', 'magma_validate_options'
LANGUAGE C STABLE;

set gen_new_oid_value to 11010;
CREATE FUNCTION pg_catalog.magma_validate_encodings() RETURNS void
AS '$libdir/magma.so', 'magma_validate_encodings'
LANGUAGE C STABLE;

set gen_new_oid_value to 11011;
CREATE FUNCTION pg_catalog.magmaap_validate_encodings() RETURNS void
AS '$libdir/magma.so', 'magma_validate_encodings'
LANGUAGE C STABLE;

set gen_new_oid_value to 11012;
CREATE FUNCTION pg_catalog.magmatp_validate_encodings() RETURNS void
AS '$libdir/magma.so', 'magma_validate_encodings'
LANGUAGE C STABLE;

set gen_new_oid_value to 11013;
CREATE FUNCTION pg_catalog.magma_validate_datatypes() RETURNS void
AS '$libdir/magma.so', 'magma_validate_datatypes'
LANGUAGE C STABLE;

set gen_new_oid_value to 11014;
CREATE FUNCTION pg_catalog.magmaap_validate_datatypes() RETURNS void
AS '$libdir/magma.so', 'magma_validate_datatypes'
LANGUAGE C STABLE;

set gen_new_oid_value to 11015;
CREATE FUNCTION pg_catalog.magmatp_validate_datatypes() RETURNS void
AS '$libdir/magma.so', 'magma_validate_datatypes'
LANGUAGE C STABLE;

set gen_new_oid_value to 11016;
CREATE FUNCTION pg_catalog.magma_createtable() RETURNS void
AS '$libdir/magma.so', 'magma_createtable'
LANGUAGE C STABLE;

set gen_new_oid_value to 11017;
CREATE FUNCTION pg_catalog.magma_droptable() RETURNS void
AS '$libdir/magma.so', 'magma_droptable'
LANGUAGE C STABLE;

set gen_new_oid_value to 11018;
CREATE FUNCTION pg_catalog.magma_createindex() RETURNS void
AS '$libdir/magma.so', 'magma_createindex'
LANGUAGE C STABLE;

set gen_new_oid_value to 11019;
CREATE FUNCTION pg_catalog.magma_dropindex() RETURNS void
AS '$libdir/magma.so', 'magma_dropindex'
LANGUAGE C STABLE;

set gen_new_oid_value to 11020;
CREATE FUNCTION pg_catalog.magma_reindex_index() RETURNS void
AS '$libdir/magma.so', 'magma_reindex_index'
LANGUAGE C STABLE;

set gen_new_oid_value to 11021;
CREATE FUNCTION pg_catalog.magmatp_beginscan() RETURNS bytea
AS '$libdir/magma.so', 'magma_beginscan'
LANGUAGE C STABLE;

set gen_new_oid_value to 11022;
CREATE FUNCTION pg_catalog.magmatp_getnext_init() RETURNS bytea
AS '$libdir/magma.so', 'magma_getnext_init'
LANGUAGE C STABLE;

set gen_new_oid_value to 11023;
CREATE FUNCTION pg_catalog.magmatp_getnext() RETURNS bytea
AS '$libdir/magma.so', 'magma_getnext'
LANGUAGE C STABLE;

set gen_new_oid_value to 11024;
CREATE FUNCTION pg_catalog.magmatp_rescan() RETURNS void
AS '$libdir/magma.so', 'magma_rescan'
LANGUAGE C STABLE;

set gen_new_oid_value to 11025;
CREATE FUNCTION pg_catalog.magmatp_endscan() RETURNS void
AS '$libdir/magma.so', 'magma_endscan'
LANGUAGE C STABLE;

set gen_new_oid_value to 11026;
CREATE FUNCTION pg_catalog.magmatp_stopscan() RETURNS void
AS '$libdir/magma.so', 'magma_stopscan'
LANGUAGE C STABLE;

set gen_new_oid_value to 11027;
CREATE FUNCTION pg_catalog.magmatp_begindelete() RETURNS bytea
AS '$libdir/magma.so', 'magma_begindelete'
LANGUAGE C STABLE;

set gen_new_oid_value to 11028;
CREATE FUNCTION pg_catalog.magmatp_delete() RETURNS void
AS '$libdir/magma.so', 'magma_delete'
LANGUAGE C STABLE;

set gen_new_oid_value to 11029;
CREATE FUNCTION pg_catalog.magmatp_enddelete() RETURNS void
AS '$libdir/magma.so', 'magma_enddelete'
LANGUAGE C STABLE;

set gen_new_oid_value to 11030;
CREATE FUNCTION pg_catalog.magmatp_beginupdate() RETURNS bytea
AS '$libdir/magma.so', 'magma_beginupdate'
LANGUAGE C STABLE;

set gen_new_oid_value to 11031;
CREATE FUNCTION pg_catalog.magmatp_update() RETURNS void
AS '$libdir/magma.so', 'magma_update'
LANGUAGE C STABLE;

set gen_new_oid_value to 11032;
CREATE FUNCTION pg_catalog.magmatp_endupdate() RETURNS void
AS '$libdir/magma.so', 'magma_endupdate'
LANGUAGE C STABLE;

set gen_new_oid_value to 11033;
CREATE FUNCTION pg_catalog.magmatp_insert_init() RETURNS bytea
AS '$libdir/magma.so', 'magma_insert_init'
LANGUAGE C STABLE;

set gen_new_oid_value to 11034;
CREATE FUNCTION pg_catalog.magmatp_insert() RETURNS bytea
AS '$libdir/magma.so', 'magma_insert'
LANGUAGE C STABLE;

set gen_new_oid_value to 11035;
CREATE FUNCTION pg_catalog.magmatp_insert_finish() RETURNS void
AS '$libdir/magma.so', 'magma_insert_finish'
LANGUAGE C STABLE;

set gen_new_oid_value to 11036;
CREATE FUNCTION pg_catalog.magma_transaction() RETURNS void
AS '$libdir/magma.so', 'magma_transaction'
LANGUAGE C STABLE;

set gen_new_oid_value to 11037;
CREATE FUNCTION pg_catalog.magmaap_beginscan() RETURNS bytea
AS '$libdir/magma.so', 'magma_beginscan'
LANGUAGE C STABLE;

set gen_new_oid_value to 11038;
CREATE FUNCTION pg_catalog.magmaap_getnext_init() RETURNS bytea
AS '$libdir/magma.so', 'magma_getnext_init'
LANGUAGE C STABLE;

set gen_new_oid_value to 11039;
CREATE FUNCTION pg_catalog.magmaap_getnext() RETURNS bytea
AS '$libdir/magma.so', 'magma_getnext'
LANGUAGE C STABLE;

set gen_new_oid_value to 11040;
CREATE FUNCTION pg_catalog.magmaap_rescan() RETURNS void
AS '$libdir/magma.so', 'magma_rescan'
LANGUAGE C STABLE;

set gen_new_oid_value to 11041;
CREATE FUNCTION pg_catalog.magmaap_endscan() RETURNS void
AS '$libdir/magma.so', 'magma_endscan'
LANGUAGE C STABLE;

set gen_new_oid_value to 11042;
CREATE FUNCTION pg_catalog.magmaap_stopscan() RETURNS void
AS '$libdir/magma.so', 'magma_stopscan'
LANGUAGE C STABLE;

set gen_new_oid_value to 11043;
CREATE FUNCTION pg_catalog.magmaap_begindelete() RETURNS bytea
AS '$libdir/magma.so', 'magma_begindelete'
LANGUAGE C STABLE;

set gen_new_oid_value to 11044;
CREATE FUNCTION pg_catalog.magmaap_delete() RETURNS void
AS '$libdir/magma.so', 'magma_delete'
LANGUAGE C STABLE;

set gen_new_oid_value to 11045;
CREATE FUNCTION pg_catalog.magmaap_enddelete() RETURNS void
AS '$libdir/magma.so', 'magma_enddelete'
LANGUAGE C STABLE;

set gen_new_oid_value to 11046;
CREATE FUNCTION pg_catalog.magmaap_beginupdate() RETURNS bytea
AS '$libdir/magma.so', 'magma_beginupdate'
LANGUAGE C STABLE;

set gen_new_oid_value to 11047;
CREATE FUNCTION pg_catalog.magmaap_update() RETURNS void
AS '$libdir/magma.so', 'magma_update'
LANGUAGE C STABLE;

set gen_new_oid_value to 11048;
CREATE FUNCTION pg_catalog.magmaap_endupdate() RETURNS void
AS '$libdir/magma.so', 'magma_endupdate'
LANGUAGE C STABLE;

set gen_new_oid_value to 11049;
CREATE FUNCTION pg_catalog.magmaap_insert_init() RETURNS bytea
AS '$libdir/magma.so', 'magma_insert_init'
LANGUAGE C STABLE;

set gen_new_oid_value to 11050;
CREATE FUNCTION pg_catalog.magmaap_insert() RETURNS bytea
AS '$libdir/magma.so', 'magma_insert'
LANGUAGE C STABLE;

set gen_new_oid_value to 11051;
CREATE FUNCTION pg_catalog.magmaap_insert_finish() RETURNS void
AS '$libdir/magma.so', 'magma_insert_finish'
LANGUAGE C STABLE;

set gen_new_oid_value to 11052;
CREATE FUNCTION pg_catalog.magma_getstatus() RETURNS void
AS '$libdir/magma.so', 'magma_getstatus'
LANGUAGE C STABLE;

reset gen_new_oid_value;
