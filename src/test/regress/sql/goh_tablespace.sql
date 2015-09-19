/*
 * Greenplum Restriction:
 *	Appendonly does not support unique index.
 * GOH Restirction:
 *	Database stored on shared storage does not support heap table.
 *	Database stored on local storage can only access local tablespace.
 *	Database stored on shared storage can only access shared tablespace.
 */

/*
 * Prepare the tablespace, assume that there are local and hdfs filespaces.
 */
SET client_min_messages = warning;
DROP DATABASE IF EXISTS goh_tablespace_local;
DROP DATABASE IF EXISTS goh_tablespace_shared;
RESET client_min_messages;

CREATE DATABASE goh_tablespace_local TABLESPACE local;
CREATE DATABASE goh_tablespace_shared TABLESPACE hdfs;

/*
 * Test create objects in a local storaged database.
 * We will test database object, table object, and index object.
 */
\c goh_tablespace_local
-- remove the databases before we create them
SET client_min_messages = warning;
DROP DATABASE IF EXISTS in_local_default;
DROP DATABASE IF EXISTS in_local_ts_local;
DROP DATABASE IF EXISTS in_local_ts_hdfs;
RESET client_min_messages;

-- database
CREATE DATABASE in_local_default;
-- FIXME: extend the psql...
DROP DATABASE in_local_default;

CREATE DATABASE in_local_ts_local TABLESPACE local;
-- FIXME: extend the psql...
DROP DATABASE in_local_ts_local;

CREATE DATABASE in_local_ts_hdfs TABLESPACE hdfs;
-- FIXME: extend the psql...
DROP DATABASE in_local_ts_hdfs;

-- Table stores on the default tablesapce, local in this case.
CREATE TABLE in_local_default(c1 INT PRIMARY KEY USING INDEX TABLESPACE hdfs, c2 INT, c3 TEXT) DISTRIBUTED BY (c1); -- ERROR: GOH restriction
CREATE TABLE in_local_default(c1 INT PRIMARY KEY USING INDEX TABLESPACE local, c2 INT, c3 TEXT) DISTRIBUTED BY (c1);
\d+ in_local_default
DROP TABLE in_local_default;
CREATE TABLE in_local_default(c1 INT PRIMARY KEY, c2 INT, c3 TEXT) DISTRIBUTED BY (c1);
\d+ in_local_default
ALTER TABLE in_local_default SET TABLESPACE local2;
\d+ in_local_default
ALTER TABLE in_local_default SET TABLESPACE local;
\d+ in_local_default
ALTER TABLE in_local_default SET TABLESPACE hdfs; -- ERROR: GOH restriction
\d+ in_local_default
CREATE INDEX i_local_default ON in_local_default(c1);
\d+ in_local_default
CREATE INDEX i_local_ts_local ON in_local_default(c1) TABLESPACE local;
\d+ in_local_default
CREATE INDEX i_local_ts_hdfs ON in_local_default(c1) TABLESPACE hdfs; -- ERROR: GOH restriction
\d+ in_local_default
ALTER INDEX i_local_ts_local SET TABLESPACE local2;
\d+ in_local_default
ALTER INDEX i_local_ts_local SET TABLESPACE local;
\d+ in_local_default
ALTER INDEX i_local_ts_local SET TABLESPACE hdfs; -- ERROR: GOH restriction
\d+ in_local_default
DROP TABLE in_local_default;
-- Table stores on the default tablesapce, local in this case, explictly specify a heap table.
CREATE TABLE in_local_heap_default(c1 INT PRIMARY KEY USING INDEX TABLESPACE hdfs, c2 INT, c3 TEXT) WITH (appendonly=false) DISTRIBUTED BY (c1); -- ERROR: GOH restriction
CREATE TABLE in_local_heap_default(c1 INT PRIMARY KEY USING INDEX TABLESPACE local, c2 INT, c3 TEXT) WITH (appendonly=false) DISTRIBUTED BY (c1);
\d+ in_local_heap_default
DROP TABLE in_local_heap_default;
CREATE TABLE in_local_heap_default(c1 INT PRIMARY KEY, c2 INT, c3 TEXT) WITH (appendonly=false) DISTRIBUTED BY (c1);
\d+ in_local_heap_default
ALTER TABLE in_local_heap_default SET TABLESPACE local2;
\d+ in_local_heap_default
ALTER TABLE in_local_heap_default SET TABLESPACE local;
\d+ in_local_heap_default
ALTER TABLE in_local_heap_default SET TABLESPACE hdfs; -- ERROR: GOH restriction
\d+ in_local_heap_default
CREATE INDEX i_local_heap_default ON in_local_heap_default(c1);
\d+ in_local_heap_default
CREATE INDEX i_local_heap_ts_local ON in_local_heap_default(c1) TABLESPACE local;
\d+ in_local_heap_default
CREATE INDEX i_local_heap_ts_hdfs ON in_local_heap_default(c1) TABLESPACE hdfs; -- ERROR: GOH restriction
\d+ in_local_heap_default
ALTER INDEX i_local_heap_ts_local SET TABLESPACE local2;
\d+ in_local_heap_default
ALTER INDEX i_local_heap_ts_local SET TABLESPACE local;
\d+ in_local_heap_default
ALTER INDEX i_local_heap_ts_local SET TABLESPACE hdfs; -- ERROR: GOH restriction
\d+ i_local_heap_ts_local
DROP TABLE in_local_heap_default;
-- Table stores on the default tablesapce, local in this case, explictly specify a appendonly table.
CREATE TABLE in_local_ao_default(c1 INT, c2 INT, c3 TEXT) WITH (appendonly=true) DISTRIBUTED BY (c1);
\d+ in_local_ao_default
ALTER TABLE in_local_ao_default SET TABLESPACE local2;
\d+ in_local_ao_default
ALTER TABLE in_local_ao_default SET TABLESPACE local;
\d+ in_local_ao_default
ALTER TABLE in_local_ao_default SET TABLESPACE hdfs; -- ERROR: GOH restriction
\d+ in_local_ao_default
CREATE INDEX i_local_ao_default ON in_local_ao_default(c1);
\d+ in_local_ao_default
CREATE INDEX i_local_ao_ts_local ON in_local_ao_default(c1) TABLESPACE local;
\d+ in_local_ao_default
CREATE INDEX i_local_ao_ts_hdfs ON in_local_ao_default(c1) TABLESPACE hdfs; -- ERROR: GOH restriction
\d+ in_local_ao_default
ALTER INDEX i_local_ao_ts_local SET TABLESPACE local2;
\d+ in_local_ao_default
ALTER INDEX i_local_ao_ts_local SET TABLESPACE local;
\d+ in_local_ao_default
ALTER INDEX i_local_ao_ts_local SET TABLESPACE hdfs; -- ERROR: GOH restriction
\d+ in_local_ao_default
DROP TABLE in_local_ao_default;
-- Table stores on the default tablesapce, local in this case, explictly specify a column store table.
CREATE TABLE in_local_co_default(c1 INT, c2 INT, c3 TEXT) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (c1);
\d+ in_local_co_default
ALTER TABLE in_local_co_default SET TABLESPACE local2;
\d+ in_local_co_default
ALTER TABLE in_local_co_default SET TABLESPACE local;
\d+ in_local_co_default
ALTER TABLE in_local_co_default SET TABLESPACE hdfs; -- ERROR: GOH restriction
\d+ in_local_co_default
CREATE INDEX i_local_co_default ON in_local_co_default(c1);
\d+ in_local_co_default
CREATE INDEX i_local_co_ts_local ON in_local_co_default(c1) TABLESPACE local;
\d+ in_local_co_default
CREATE INDEX i_local_co_ts_hdfs ON in_local_co_default(c1) TABLESPACE hdfs; -- ERROR: GOH restriction
\d+ in_local_co_default
ALTER INDEX i_local_co_ts_local SET TABLESPACE local2;
\d+ in_local_co_default
ALTER INDEX i_local_co_ts_local SET TABLESPACE local;
\d+ in_local_co_default
ALTER INDEX i_local_co_ts_local SET TABLESPACE hdfs; -- ERROR: GOH restriction
\d+ in_local_co_default
DROP TABLE in_local_co_default;

-- Table stores on the local tablesapce.
CREATE TABLE in_local_ts_local(c1 INT, c2 INT, c3 TEXT) TABLESPACE local DISTRIBUTED BY (c1);
\d+ in_local_ts_local
-- no need to test all of this
DROP TABLE in_local_ts_local;
-- Table stores on the local tablesapce, explictly specify a heap table.
CREATE TABLE in_local_heap_ts_local(c1 INT, c2 INT, c3 TEXT) WITH (appendonly=false) TABLESPACE local DISTRIBUTED BY (c1);
\d+ in_local_heap_ts_local
-- no need to test all of this
DROP TABLE in_local_heap_ts_local;
-- Table stores on the local tablesapce, explictly specify a appendonly table.
CREATE TABLE in_local_ao_ts_local(c1 INT, c2 INT, c3 TEXT) WITH (appendonly=true) TABLESPACE local DISTRIBUTED BY (c1);
\d+ in_local_ao_ts_local
-- no need to test all of this
DROP TABLE in_local_ao_ts_local;
-- Table stores on the local tablesapce, explictly specify a column store table.
CREATE TABLE in_local_co_ts_local(c1 INT, c2 INT, c3 TEXT) WITH (appendonly=true, orientation=column) TABLESPACE local DISTRIBUTED BY (c1);
\d+ in_local_co_ts_local
-- no need to test all of this
DROP TABLE in_local_co_ts_local;

-- Table stores on the shared tablesapce.
CREATE TABLE in_local_ts_hdfs(c1 INT, c2 INT, c3 TEXT) TABLESPACE hdfs DISTRIBUTED BY (c1); -- ERROR: GOH restriction
-- Table stores on the shared tablesapce, explictly specify a heap table.
CREATE TABLE in_local_heap_ts_hdfs(c1 INT, c2 INT, c3 TEXT) WITH (appendonly=false) TABLESPACE hdfs DISTRIBUTED BY (c1); -- ERROR: GOH restriction
-- Table stores on the shared tablesapce, explictly specify a appendonly table.
CREATE TABLE in_local_ao_ts_hdfs(c1 INT, c2 INT, c3 TEXT) WITH (appendonly=true) TABLESPACE hdfs DISTRIBUTED BY (c1); -- ERROR: GOH restriction
-- Table stores on the shared tablesapce, explictly specify a column store table.
CREATE TABLE in_local_co_ts_hdfs(c1 INT, c2 INT, c3 TEXT) WITH (appendonly=true, orientation=column) TABLESPACE hdfs DISTRIBUTED BY (c1); -- ERROR: GOH restriction

-- external/copy
CREATE EXTERNAL WEB TABLE in_local_ext(c INT) EXECUTE 'echo "no_such_number"' ON ALL FORMAT 'TEXT' LOG ERRORS INTO in_local_ext_error SEGMENT REJECT LIMIT 2;
\d+ in_local_ext_error
SELECT * FROM in_local_ext;
SELECT relname, filename, errmsg FROM in_local_ext_error;
DROP EXTERNAL WEB TABLE in_local_ext;
DROP TABLE in_local_ext_error;

CREATE TABLE copied_table(c INT);
COPY copied_table FROM stdin LOG ERRORS INTO in_local_ext_error SEGMENT REJECT LIMIT 2;
no_such_number
\.

\d+ in_local_ext_error
DROP TABLE copied_table;
DROP TABLE in_local_ext_error;

-- select into
SELECT RANDOM() AS r INTO in_local_random;
\d+ in_local_random
DROP TABLE in_local_random;

/*
 * Test create objects in a shared storaged database.
 * We will test database object, table object, and index object.
 */
\c goh_tablespace_shared
-- remove the databases before we create them
SET client_min_messages = warning;
DROP DATABASE IF EXISTS in_shared_default;
DROP DATABASE IF EXISTS in_shared_ts_local;
DROP DATABASE IF EXISTS in_shared_ts_hdfs;
RESET client_min_messages;

-- database
CREATE DATABASE in_shared_default;
-- FIXME: extend the psql...
DROP DATABASE in_shared_default;

CREATE DATABASE in_shared_ts_local TABLESPACE local;
-- FIXME: extend the psql...
DROP DATABASE in_shared_ts_local;

CREATE DATABASE in_shared_ts_hdfs TABLESPACE hdfs;
-- FIXME: extend the psql...
DROP DATABASE in_shared_ts_hdfs;

-- Table stores on the default tablesapce, shared in this case.
CREATE TABLE in_shared_default(c1 INT PRIMARY KEY USING INDEX TABLESPACE hdfs, c2 INT, c3 TEXT) DISTRIBUTED BY (c1); -- ERROR: Feature not supported || GPDB restriction
CREATE TABLE in_shared_default(c1 INT PRIMARY KEY USING INDEX TABLESPACE local, c2 INT, c3 TEXT) DISTRIBUTED BY (c1); -- ERROR: GOH restriction || GPDB restriction
CREATE TABLE in_shared_default(c1 INT, c2 INT, c3 TEXT) DISTRIBUTED BY (c1);
\d+ in_shared_default
ALTER TABLE in_shared_default SET TABLESPACE local2; -- ERROR: GOH restriction
\d+ in_shared_default
ALTER TABLE in_shared_default SET TABLESPACE local; -- ERROR: GOH restriction
\d+ in_shared_default
ALTER TABLE in_shared_default SET TABLESPACE hdfs2;
\d+ in_shared_default
CREATE INDEX i_shared_default ON in_shared_default(c1);
\d+ in_shared_default
CREATE INDEX i_shared_default ON in_shared_default(c1) TABLESPACE local; -- ERROR: GOH restriction
\d+ in_shared_default
CREATE INDEX i_shared_default ON in_shared_default(c1) TABLESPACE hdfs; -- ERROR: Feature not supported
\d+ in_shared_default
ALTER INDEX i_shared_default SET TABLESPACE local2; -- ERROR: GOH restriction
\d+ in_shared_default
ALTER INDEX i_shared_default SET TABLESPACE local; -- ERROR: GOH restriction
\d+ in_shared_default
ALTER INDEX i_shared_default SET TABLESPACE hdfs; -- ERROR: Feature not supported
\d+ in_shared_default
DROP TABLE in_shared_default;
-- Table stores on the default tablesapce, shared in this case, explictly specify a heap table.
CREATE TABLE in_shared_heap_default(c1 INT PRIMARY KEY USING INDEX TABLESPACE hdfs, c2 INT, c3 TEXT) WITH (appendonly=false) DISTRIBUTED BY (c1); -- ERROR: GOH restriction
CREATE TABLE in_shared_heap_default(c1 INT PRIMARY KEY USING INDEX TABLESPACE local, c2 INT, c3 TEXT) WITH (appendonly=false) DISTRIBUTED BY (c1); -- ERROR: GOH restriction
CREATE TABLE in_shared_heap_default(c1 INT PRIMARY KEY, c2 INT, c3 TEXT) WITH (appendonly=false) DISTRIBUTED BY (c1); -- ERROR: GOH restriction
-- Table stores on the default tablesapce, local in this case, explictly specify a appendonly table.
CREATE TABLE in_shared_ao_default(c1 INT, c2 INT, c3 TEXT) WITH (appendonly=true) DISTRIBUTED BY (c1);
\d+ in_shared_ao_default
ALTER TABLE in_shared_ao_default SET TABLESPACE local2; -- ERROR: GOH restriction
\d+ in_shared_ao_default
ALTER TABLE in_shared_ao_default SET TABLESPACE local; -- ERROR: GOH restriction
\d+ in_shared_ao_default
ALTER TABLE in_shared_ao_default SET TABLESPACE hdfs2;
\d+ in_shared_ao_default
CREATE INDEX i_shared_ao_default ON in_shared_ao_default(c1);
\d+ in_shared_ao_default
CREATE INDEX i_shared_ao_ts_local ON in_shared_ao_default(c1) TABLESPACE local; -- ERROR: GOH restriction
\d+ in_shared_ao_default
CREATE INDEX i_shared_ao_ts_hdfs ON in_shared_ao_default(c1) TABLESPACE hdfs; -- ERROR: Feature not supported
\d+ in_shared_ao_default
ALTER INDEX i_shared_ao_default SET TABLESPACE local2; -- ERROR: GOH restriction
\d+ in_shared_ao_default
ALTER INDEX i_shared_ao_default SET TABLESPACE local; -- ERROR: GOH restriction
\d+ in_shared_ao_default
ALTER INDEX i_shared_ao_default SET TABLESPACE hdfs; -- ERROR: Feature not supported
\d+ in_shared_ao_default
DROP TABLE in_shared_ao_default;
-- Table stores on the default tablesapce, local in this case, explictly specify a column store table.
CREATE TABLE in_shared_co_default(c1 INT, c2 INT, c3 TEXT) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (c1);
\d+ in_shared_co_default
ALTER TABLE in_shared_co_default SET TABLESPACE local2; -- ERROR: GOH restriction
\d+ in_shared_co_default
ALTER TABLE in_shared_co_default SET TABLESPACE local; -- ERROR: GOH restriction
\d+ in_shared_co_default
ALTER TABLE in_shared_co_default SET TABLESPACE hdfs2;
\d+ in_shared_co_default
CREATE INDEX i_shared_co_default ON in_shared_co_default(c1);
\d+ in_shared_co_default
CREATE INDEX i_shared_co_ts_local ON in_shared_co_default(c1) TABLESPACE local; -- ERROR: GOH restriction
\d+ in_shared_co_default
CREATE INDEX i_shared_co_ts_hdfs ON in_shared_co_default(c1) TABLESPACE hdfs; -- ERROR: Feature not supported
\d+ in_shared_co_default
ALTER INDEX i_shared_co_default SET TABLESPACE local2; -- ERROR: GOH restriction
\d+ in_shared_co_default
ALTER INDEX i_shared_co_default SET TABLESPACE local; -- ERROR: GOH restriction
\d+ in_shared_co_default
ALTER INDEX i_shared_co_default SET TABLESPACE hdfs; -- ERROR: Feature not supported
\d+ in_shared_co_default
DROP TABLE in_shared_co_default;

-- Table stores on the local tablesapce.
CREATE TABLE in_shared_ts_local(c1 INT, c2 INT, c3 TEXT) TABLESPACE local DISTRIBUTED BY (c1); -- ERROR: GOH restriction
-- Table stores on the local tablesapce, explictly specify a heap table.
CREATE TABLE in_shared_heap_ts_local(c1 INT, c2 INT, c3 TEXT) WITH (appendonly=false) TABLESPACE local DISTRIBUTED BY (c1); -- ERROR: GOH restriction
-- Table stores on the local tablesapce, explictly specify a appendonly table.
CREATE TABLE in_shared_ao_ts_local(c1 INT, c2 INT, c3 TEXT) WITH (appendonly=true) TABLESPACE local DISTRIBUTED BY (c1); -- ERROR: GOH restriction
-- Table stores on the local tablesapce, explictly specify a column store table.
CREATE TABLE in_shared_co_ts_local(c1 INT, c2 INT, c3 TEXT) WITH (appendonly=true, orientation=column) TABLESPACE local DISTRIBUTED BY (c1); -- ERROR: GOH restriction

-- Table stores on the shared tablesapce.
CREATE TABLE in_shared_ts_hdfs(c1 INT, c2 INT, c3 TEXT) TABLESPACE hdfs DISTRIBUTED BY (c1);
\d+ in_shared_ts_hdfs
-- no need to test all of this
DROP TABLE in_shared_ts_hdfs;
-- Table stores on the shared tablesapce, explictly specify a heap table.
CREATE TABLE in_shared_heap_ts_hdfs(c1 INT, c2 INT, c3 TEXT) WITH (appendonly=false) TABLESPACE hdfs DISTRIBUTED BY (c1); -- ERROR: GOH restriction
-- Table stores on the shared tablesapce, explictly specify a appendonly table.
CREATE TABLE in_shared_ao_ts_hdfs(c1 INT, c2 INT, c3 TEXT) WITH (appendonly=true) TABLESPACE hdfs DISTRIBUTED BY (c1);
\d+ in_shared_ao_ts_hdfs
-- no need to test all of this
DROP TABLE in_shared_ao_ts_hdfs;
-- Table stores on the shared tablesapce, explictly specify a column store table.
CREATE TABLE in_shared_co_ts_hdfs(c1 INT, c2 INT, c3 TEXT) WITH (appendonly=true, orientation=column) TABLESPACE hdfs DISTRIBUTED BY (c1);
\d+ in_shared_co_ts_hdfs
-- no need to test all of this
DROP TABLE in_shared_co_ts_hdfs;

-- external/copy
CREATE EXTERNAL WEB TABLE in_shared_ext(c INT) EXECUTE 'echo "no_such_number"' ON ALL FORMAT 'TEXT' LOG ERRORS INTO in_shared_ext_error SEGMENT REJECT LIMIT 2;
\d+ in_shared_ext_error
SELECT * FROM in_shared_ext;
SELECT relname, filename, errmsg FROM in_shared_ext_error;
DROP EXTERNAL WEB TABLE in_shared_ext;
DROP TABLE in_shared_ext_error;

CREATE TABLE copied_table(c INT);
COPY copied_table FROM stdin LOG ERRORS INTO in_shared_ext_error SEGMENT REJECT LIMIT 2;
no_such_number
\.

\d+ in_shared_ext_error
DROP TABLE copied_table;
DROP TABLE in_shared_ext_error;

-- select into
SELECT RANDOM() AS r INTO in_shared_random;
\d+ in_shared_random
DROP TABLE in_shared_random;
