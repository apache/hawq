CREATE DATABASE goh_database;
DROP DATABASE goh_database;

-- should be a clean databse
CREATE DATABASE goh_database1;
\c goh_database1
CREATE TABLE x(c int);
INSERT INTO x VALUES(generate_series(1, 10));
SELECT * FROM x ORDER BY c;
DROP TABLE x;
\c db_testdatabase_basictest 

-- table should be removed
CREATE DATABASE goh_database2;
\c goh_database2
CREATE TABLE x(c int);
INSERT INTO x VALUES(generate_series(1, 10));
SELECT * FROM x ORDER BY c;
\c db_testdatabase_basictest 

CREATE TABLESPACE goh_regression_tablespace1 FILESPACE dfs_system;
CREATE DATABASE goh_database3 TABLESPACE goh_regression_tablespace1;
\c goh_database3
CREATE TABLE x(c int);
INSERT INTO x VALUES(generate_series(1, 10));
SELECT * FROM x ORDER BY c;
\d x
\c db_testdatabase_basictest 

BEGIN;
CREATE TABLESPACE goh_regression_tablespace2 FILESPACE dfs_system;
CREATE TABLE x(c int) TABLESPACE goh_regression_tablespace2;
INSERT INTO x VALUES(generate_series(1, 10));
SELECT * FROM x ORDER BY c;
\d x
ROLLBACK;

DROP DATABASE goh_database1;
DROP DATABASE goh_database2;
DROP DATABASE goh_database3;
DROP TABLESPACE goh_regression_tablespace1;
