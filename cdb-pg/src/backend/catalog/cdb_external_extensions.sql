-- --------------------------------------------------------------------
--
-- cdb_external_extensions.sql
--
-- External Extensions, including custom formatter, protocol
--
--
-- --------------------------------------------------------------------


------------------------------------------------------------------
-- gphdfs Protocol/Formatters
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION pg_catalog.gphdfs_export(record) RETURNS bytea
AS '$libdir/gphdfs.so', 'gphdfsformatter_export'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.gphdfs_import() RETURNS record
AS '$libdir/gphdfs.so', 'gphdfsformatter_import'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.gphdfs_read() RETURNS integer
AS '$libdir/gphdfs.so', 'gphdfsprotocol_import'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.gphdfs_write() RETURNS integer
AS '$libdir/gphdfs.so', 'gphdfsprotocol_export'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.gphdfs_validate() RETURNS void
AS '$libdir/gphdfs.so', 'gphdfsprotocol_validate_urls'
LANGUAGE C STABLE;

CREATE TRUSTED PROTOCOL gphdfs (
  writefunc     = gphdfs_write,
  readfunc      = gphdfs_read,
  validatorfunc = gphdfs_validate);
  
------------------------------------------------------------------
-- gpfusion
------------------------------------------------------------------

CREATE OR REPLACE FUNCTION pg_catalog.gpfusion_write() RETURNS integer
AS '$libdir/gpfusion.so', 'gpfusionprotocol_export'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.gpfusion_read() RETURNS integer
AS '$libdir/gpfusion.so', 'gpfusionprotocol_import'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.gpfusion_validate() RETURNS void
AS '$libdir/gpfusion.so', 'gpfusionprotocol_validate_urls'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.gpfusionwritable_export(record) RETURNS bytea
AS '$libdir/gpfusion.so', 'gpdbwritableformatter_export'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.gpfusionwritable_import() RETURNS record
AS '$libdir/gpfusion.so', 'gpdbwritableformatter_import'
LANGUAGE C STABLE;

CREATE TRUSTED PROTOCOL gpfusion (
  writefunc		= gpfusion_write,
  readfunc      = gpfusion_read,
  validatorfunc = gpfusion_validate);  
  
------------------------------------------------------------------
-- fixedwidth Formatters
------------------------------------------------------------------
  
CREATE OR REPLACE FUNCTION fixedwidth_in() RETURNS record 
AS '$libdir/fixedwidth.so', 'fixedwidth_in'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION fixedwidth_out(record) RETURNS bytea 
AS '$libdir/fixedwidth.so', 'fixedwidth_out'
LANGUAGE C STABLE;
