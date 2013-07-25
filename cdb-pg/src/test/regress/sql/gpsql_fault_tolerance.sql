
/* 
 * PREPARE THE TEST DATA
 * id < 100: The data inserts before test
 * id < 200: The data inserts when segment id 1 failed
 * id < 300: The data inserts when segment id 0 failed
 * id < 400: the data inserts after recovery
 */
DROP TABLE IF EXISTS x;
CREATE TABLE x(c INT, d TEXT) DISTRIBUTED BY (c);
INSERT INTO x VALUES(1, 'insert before test');
INSERT INTO x VALUES(2, 'insert before test');
INSERT INTO x VALUES(3, 'insert before test');
INSERT INTO x VALUES(4, 'insert before test');
INSERT INTO x VALUES(5, 'insert before test');
COPY x FROM stdin DELIMITER ',';
6,copy before test
7,copy before test
8,copy before test
9,copy before test
10,copy before test
\.

-- every segments returns
SELECT c, d FROM x;
SELECT count(*) AS data_before_failover FROM x;
-- COPY x TO stdout;
COPY (SELECT c, d FROM x ORDER BY c) TO stdout;

/*
 * FAULT TOLENRANCE TEST
 */
SET gp_test_failed_segmentid_number = 1;
SELECT * FROM x; -- trigger failover

-- SEGMENT ID 1 FAILED
SET gp_test_failed_segmentid_start = 1;
SELECT * FROM x; -- trigger failover
SELECT COUNT(DISTINCT gp_segment_id) > 0 AS has_segment_id_1 FROM x WHERE gp_segment_id = 1; -- must be true
INSERT INTO x VALUES(100, 'insert when segment id 1 failed');
COPY x FROM stdin DELIMITER ',';
101,copy when segment id 1 failed
\.

SELECT COUNT(DISTINCT gp_segment_id) > 0 AS has_segment_id_1 FROM x WHERE gp_segment_id = 1; -- must be true
SELECT c, d FROM x WHERE c BETWEEN 100 AND 200; -- expected insert successful
-- COPY x TO stdout;
COPY (SELECT c, d FROM x WHERE c BETWEEN 100 AND 200 ORDER BY c) TO stdout; -- expected insert successful

-- SEGMENT ID 0 FAILED
SET gp_test_failed_segmentid_start = 0;
SELECT * FROM x; -- trigger failover
SELECT COUNT(DISTINCT gp_segment_id) > 0 AS has_segment_id_0 FROM x WHERE gp_segment_id = 0; -- must be true
INSERT INTO x VALUES(200, 'insert when segment id 0 failed');
COPY x FROM stdin DELIMITER ',';
201,copy when segment id 0 failed
\.

SELECT COUNT(DISTINCT gp_segment_id) > 0 AS has_segment_id_0 FROM x WHERE gp_segment_id = 0; -- must be true
SELECT c, d FROM x WHERE c BETWEEN 200 AND 300; -- expected insert successful
-- COPY x TO stdout;
COPY (SELECT c, d FROM x WHERE c BETWEEN 200 AND 300 ORDER BY c) TO stdout; -- expected insert successful


/*
 * ALL SEGMENTS RECOVERYED
 */
SET gp_test_failed_segmentid_number = 0;
SELECT * FROM x; -- trigger failover

-- All data should be returned
SELECT c, d FROM x;
SELECT count(*) AS data_before_failover FROM x;
-- COPY x TO stdout;
COPY (SELECT c, d FROM x ORDER BY c) TO stdout;

INSERT INTO x VALUES(301, 'insert after recovery');
INSERT INTO x VALUES(302, 'insert after recovery');
INSERT INTO x VALUES(303, 'insert after recovery');
INSERT INTO x VALUES(304, 'insert after recovery');
INSERT INTO x VALUES(305, 'insert after recovery');
COPY x FROM stdin DELIMITER ',';
306,copy after recovery
307,copy after recovery
308,copy after recovery
309,copy after recovery
310,copy after recovery
\.

SELECT c, d FROM x;
SELECT count(*) AS data_before_failover FROM x;
-- COPY x TO stdout;
COPY (SELECT c, d FROM x ORDER BY c) TO stdout;


DROP TABLE x;

-- Temp table tests
CREATE TEMP TABLE y(c int) DISTRIBUTED BY (c);
SELECT * FROM y;
-- start_matchsubs
--
-- m/WARNING:  Any temporary tables for this session/
-- s/\(session id = .*\)//
--
-- end_matchsubs
SET gp_test_failed_segmentid_number = 1;
SELECT * FROM x; -- trigger failover
SET gp_test_failed_segmentid_start = 1;
SELECT * FROM x; -- trigger failover

SELECT * FROM y;
