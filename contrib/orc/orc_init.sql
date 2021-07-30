-- --------------------------------------------------------------------
--
-- orc_init.sql
--
-- Support ORC format in pluggable storage framework at initialization
--
-- --------------------------------------------------------------------
  
CREATE OR REPLACE FUNCTION pg_catalog.orc_validate_interfaces() RETURNS void
AS '$libdir/orc.so', 'orc_validate_interfaces'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.orc_validate_options() RETURNS void
AS '$libdir/orc.so', 'orc_validate_options'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.orc_validate_encodings() RETURNS void
AS '$libdir/orc.so', 'orc_validate_encodings'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.orc_validate_datatypes() RETURNS void
AS '$libdir/orc.so', 'orc_validate_datatypes'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.orc_beginscan() RETURNS bytea
AS '$libdir/orc.so', 'orc_beginscan'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.orc_getnext_init() RETURNS bytea
AS '$libdir/orc.so', 'orc_getnext_init'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.orc_getnext() RETURNS bytea
AS '$libdir/orc.so', 'orc_getnext'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.orc_rescan() RETURNS void
AS '$libdir/orc.so', 'orc_rescan'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.orc_endscan() RETURNS void
AS '$libdir/orc.so', 'orc_endscan'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.orc_stopscan() RETURNS void
AS '$libdir/orc.so', 'orc_stopscan'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.orc_insert_init() RETURNS bytea
AS '$libdir/orc.so', 'orc_insert_init'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.orc_insert() RETURNS bytea
AS '$libdir/orc.so', 'orc_insert'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.orc_insert_finish() RETURNS void
AS '$libdir/orc.so', 'orc_insert_finish'
LANGUAGE C STABLE;

