-- --------------------------------------------------------------------
--
-- cdb_external_extensions.sql
--
-- External Extensions, including custom formatter, protocol
--
--
-- --------------------------------------------------------------------
  
------------------------------------------------------------------
-- pxf
------------------------------------------------------------------

CREATE OR REPLACE FUNCTION pg_catalog.pxf_write() RETURNS integer
AS '$libdir/pxf.so', 'pxfprotocol_export'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.pxf_read() RETURNS integer
AS '$libdir/pxf.so', 'pxfprotocol_import'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.pxf_validate() RETURNS void
AS '$libdir/pxf.so', 'pxfprotocol_validate_urls'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.pxfwritable_export(record) RETURNS bytea
AS '$libdir/pxf.so', 'gpdbwritableformatter_export'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.pxfwritable_import() RETURNS record
AS '$libdir/pxf.so', 'gpdbwritableformatter_import'
LANGUAGE C STABLE;

CREATE TRUSTED PROTOCOL pxf (
  writefunc		= pxf_write,
  readfunc      = pxf_read,
  validatorfunc = pxf_validate);  
  
------------------------------------------------------------------
-- fixedwidth Formatters
------------------------------------------------------------------
  
CREATE OR REPLACE FUNCTION fixedwidth_in() RETURNS record 
AS '$libdir/fixedwidth.so', 'fixedwidth_in'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION fixedwidth_out(record) RETURNS bytea 
AS '$libdir/fixedwidth.so', 'fixedwidth_out'
LANGUAGE C STABLE;
