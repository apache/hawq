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

------------------------------------------------------------------
-- external HDFS
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hdfs_validate() RETURNS void
AS '$libdir/exthdfs.so', 'hdfsprotocol_validate'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION hdfs_blocklocation() RETURNS void
AS '$libdir/exthdfs.so', 'hdfsprotocol_blocklocation'
LANGUAGE C STABLE;

------------------------------------------------------------------
-- external HIVE
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hive_validate() RETURNS void
AS '$libdir/exthive.so', 'hiveprotocol_validate'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION hive_blocklocation() RETURNS void
AS '$libdir/exthive.so', 'hiveprotocol_blocklocation'
LANGUAGE C STABLE;

------------------------------------------------------------------
-- csv Formatters
------------------------------------------------------------------
  
CREATE OR REPLACE FUNCTION csv_in() RETURNS record 
AS '$libdir/extfmtcsv.so', 'extfmtcsv_in'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION csv_out(record) RETURNS bytea 
AS '$libdir/extfmtcsv.so', 'extfmtcsv_out'
LANGUAGE C STABLE;

------------------------------------------------------------------
-- text Formatters
------------------------------------------------------------------
  
CREATE OR REPLACE FUNCTION text_in() RETURNS record 
AS '$libdir/extfmtcsv.so', 'extfmttext_in'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION text_out(record) RETURNS bytea 
AS '$libdir/extfmtcsv.so', 'extfmttext_out'
LANGUAGE C STABLE;