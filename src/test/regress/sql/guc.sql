-- pg_regress should ensure that this default value applies; however
-- we can't rely on any specific default value of vacuum_cost_delay
SHOW datestyle;

-- SET to some nondefault value
SET vacuum_cost_delay TO 400;
SET datestyle = 'ISO, YMD';
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

-- SET LOCAL has no effect outside of a transaction
SET LOCAL vacuum_cost_delay TO 500;
SHOW vacuum_cost_delay;
SET LOCAL datestyle = 'SQL';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

-- SET LOCAL within a transaction that commits
BEGIN;
SET LOCAL vacuum_cost_delay TO 500;
SHOW vacuum_cost_delay;
SET LOCAL datestyle = 'SQL';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
COMMIT;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

-- SET should be reverted after ROLLBACK
BEGIN;
SET vacuum_cost_delay TO 600;
SHOW vacuum_cost_delay;
SET datestyle = 'German';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
ROLLBACK;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

-- Some tests with subtransactions
BEGIN;
SET vacuum_cost_delay TO 700;
SET datestyle = 'MDY';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
SAVEPOINT first_sp;
SET vacuum_cost_delay TO 800;
SHOW vacuum_cost_delay;
SET datestyle = 'German, DMY';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
ROLLBACK TO first_sp;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
SAVEPOINT second_sp;
SET vacuum_cost_delay TO 900;
SET datestyle = 'SQL, YMD';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
SAVEPOINT third_sp;
SET vacuum_cost_delay TO 1000;
SHOW vacuum_cost_delay;
SET datestyle = 'Postgres, MDY';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
ROLLBACK TO third_sp;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
ROLLBACK TO second_sp;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
ROLLBACK;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

-- SET LOCAL with Savepoints
BEGIN;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
SAVEPOINT sp;
SET LOCAL vacuum_cost_delay TO 300;
SHOW vacuum_cost_delay;
SET LOCAL datestyle = 'Postgres, MDY';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
ROLLBACK TO sp;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
ROLLBACK;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

-- SET followed by SET LOCAL
BEGIN;
SET vacuum_cost_delay TO 400;
SET LOCAL vacuum_cost_delay TO 500;
SHOW vacuum_cost_delay;
SET datestyle = 'ISO, DMY';
SET LOCAL datestyle = 'Postgres, MDY';
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
COMMIT;
SHOW vacuum_cost_delay;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

--
-- Test RESET.  We use datestyle because the reset value is forced by
-- pg_regress, so it doesn't depend on the installation's configuration.
--
SET datestyle = iso, ymd;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;
RESET datestyle;
SHOW datestyle;
SELECT '2006-08-13 12:34:56'::timestamptz;

--
-- Test GUC - gp_disable_catalog_access_on_segment
--
SHOW gp_disable_catalog_access_on_segment;
CREATE TABLE test1(c1 int, c2 int);
INSERT INTO test1 values(1, 10);
SELECT 1 FROM test1;
SELECT * FROM test1;
SELECT c1 + 1 FROM test1;

SET gp_disable_catalog_access_on_segment = true;
SHOW gp_disable_catalog_access_on_segment;
CREATE TABLE test2(c1 int, c2 int);
INSERT INTO test1 values(1, 10);
SELECT 1 FROM test1;
SELECT * FROM test1;
SELECT c1 + 1 FROM test1;

SET gp_disable_catalog_access_on_segment = false;
SHOW gp_disable_catalog_access_on_segment;

-- start_ignore
DROP TABLE test1;
DROP TABLE test2;
-- end_ignore

SELECT min_val, max_val FROM pg_settings WHERE name = 'hawq_metadata_cache_free_block_normal_ratio';
