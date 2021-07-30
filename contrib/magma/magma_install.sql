------------------------------------------------------------------
-- magma protocol
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION pg_catalog.magma_validate() RETURNS void
AS '$libdir/magma.so', 'magma_protocol_validate'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magma_blocklocation() RETURNS void
AS '$libdir/magma.so', 'magma_protocol_blocklocation'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magma_tablesize() RETURNS void
AS '$libdir/magma.so', 'magma_protocol_tablesize'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magma_databasesize() RETURNS void
AS '$libdir/magma.so', 'magma_protocol_databasesize'
LANGUAGE C STABLE;

------------------------------------------------------------------
-- magma format
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION pg_catalog.magma_validate_interfaces() RETURNS void
AS '$libdir/magma.so', 'magma_validate_interfaces'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmaap_validate_interfaces() RETURNS void
AS '$libdir/magma.so', 'magma_validate_interfaces'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmatp_validate_interfaces() RETURNS void
AS '$libdir/magma.so', 'magma_validate_interfaces'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magma_validate_options() RETURNS void
AS '$libdir/magma.so', 'magma_validate_options'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmaap_validate_options() RETURNS void
AS '$libdir/magma.so', 'magma_validate_options'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmatp_validate_options() RETURNS void
AS '$libdir/magma.so', 'magma_validate_options'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magma_validate_encodings() RETURNS void
AS '$libdir/magma.so', 'magma_validate_encodings'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmaap_validate_encodings() RETURNS void
AS '$libdir/magma.so', 'magma_validate_encodings'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmatp_validate_encodings() RETURNS void
AS '$libdir/magma.so', 'magma_validate_encodings'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magma_validate_datatypes() RETURNS void
AS '$libdir/magma.so', 'magma_validate_datatypes'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmaap_validate_datatypes() RETURNS void
AS '$libdir/magma.so', 'magma_validate_datatypes'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmatp_validate_datatypes() RETURNS void
AS '$libdir/magma.so', 'magma_validate_datatypes'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magma_createtable() RETURNS void
AS '$libdir/magma.so', 'magma_createtable'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magma_droptable() RETURNS void
AS '$libdir/magma.so', 'magma_droptable'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magma_createindex() RETURNS void
AS '$libdir/magma.so', 'magma_createindex'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magma_dropindex() RETURNS void
AS '$libdir/magma.so', 'magma_dropindex'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magma_reindex_index() RETURNS void
AS '$libdir/magma.so', 'magma_reindex_index'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmatp_beginscan() RETURNS bytea
AS '$libdir/magma.so', 'magma_beginscan'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmatp_getnext_init() RETURNS bytea
AS '$libdir/magma.so', 'magma_getnext_init'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmatp_getnext() RETURNS bytea
AS '$libdir/magma.so', 'magma_getnext'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmatp_rescan() RETURNS void
AS '$libdir/magma.so', 'magma_rescan'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmatp_endscan() RETURNS void
AS '$libdir/magma.so', 'magma_endscan'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmatp_stopscan() RETURNS void
AS '$libdir/magma.so', 'magma_stopscan'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmatp_begindelete() RETURNS bytea
AS '$libdir/magma.so', 'magma_begindelete'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmatp_delete() RETURNS void
AS '$libdir/magma.so', 'magma_delete'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmatp_enddelete() RETURNS void
AS '$libdir/magma.so', 'magma_enddelete'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmatp_beginupdate() RETURNS bytea
AS '$libdir/magma.so', 'magma_beginupdate'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmatp_update() RETURNS void
AS '$libdir/magma.so', 'magma_update'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmatp_endupdate() RETURNS void
AS '$libdir/magma.so', 'magma_endupdate'
LANGUAGE C STABLE;
 
CREATE OR REPLACE FUNCTION pg_catalog.magmatp_insert_init() RETURNS bytea
AS '$libdir/magma.so', 'magma_insert_init'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmatp_insert() RETURNS bytea
AS '$libdir/magma.so', 'magma_insert'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmatp_insert_finish() RETURNS void
AS '$libdir/magma.so', 'magma_insert_finish'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magma_transaction() RETURNS void
AS '$libdir/magma.so', 'magma_transaction'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmaap_beginscan() RETURNS bytea
AS '$libdir/magma.so', 'magma_beginscan'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmaap_getnext_init() RETURNS bytea
AS '$libdir/magma.so', 'magma_getnext_init'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmaap_getnext() RETURNS bytea
AS '$libdir/magma.so', 'magma_getnext'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmaap_rescan() RETURNS void
AS '$libdir/magma.so', 'magma_rescan'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmaap_endscan() RETURNS void
AS '$libdir/magma.so', 'magma_endscan'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmaap_stopscan() RETURNS void
AS '$libdir/magma.so', 'magma_stopscan'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmaap_begindelete() RETURNS bytea
AS '$libdir/magma.so', 'magma_begindelete'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmaap_delete() RETURNS void
AS '$libdir/magma.so', 'magma_delete'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmaap_enddelete() RETURNS void
AS '$libdir/magma.so', 'magma_enddelete'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmaap_beginupdate() RETURNS bytea
AS '$libdir/magma.so', 'magma_beginupdate'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmaap_update() RETURNS void
AS '$libdir/magma.so', 'magma_update'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmaap_endupdate() RETURNS void
AS '$libdir/magma.so', 'magma_endupdate'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmaap_insert_init() RETURNS bytea
AS '$libdir/magma.so', 'magma_insert_init'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmaap_insert() RETURNS bytea
AS '$libdir/magma.so', 'magma_insert'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magmaap_insert_finish() RETURNS void
AS '$libdir/magma.so', 'magma_insert_finish'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.magma_getstatus() RETURNS void
AS '$libdir/magma.so', 'magma_getstatus'
LANGUAGE C STABLE;
