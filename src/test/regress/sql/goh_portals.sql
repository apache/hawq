--
-- Cursor regression tests
--
-- setup
ALTER RESOURCE QUEUE pg_default WITH (active_statements=30);

CREATE TABLE test1 (a int, b int, c int, d int);
CREATE TABLE test2 (a int, b int, c int, d int);
INSERT INTO test1 SELECT x, 2 * x, 3 * x, 4 * x FROM generate_series(1, 1000) x;
INSERT INTO test2 SELECT x, 2 * x, 3 * x, 4 * x FROM generate_series(1, 1000) x;

BEGIN;

DECLARE foo1 CURSOR FOR SELECT * FROM test1 ORDER BY 1,2,3,4;

DECLARE foo2 CURSOR FOR SELECT * FROM test2 ORDER BY 1,2,3,4;

DECLARE foo3 CURSOR FOR SELECT * FROM test1 ORDER BY 1,2,3,4;

DECLARE foo4 CURSOR FOR SELECT * FROM test2 ORDER BY 1,2,3,4;

DECLARE foo5 CURSOR FOR SELECT * FROM test1 ORDER BY 1,2,3,4;

DECLARE foo6 CURSOR FOR SELECT * FROM test2 ORDER BY 1,2,3,4;

DECLARE foo7 CURSOR FOR SELECT * FROM test1 ORDER BY 1,2,3,4;

DECLARE foo8 CURSOR FOR SELECT * FROM test2 ORDER BY 1,2,3,4;

DECLARE foo9 CURSOR FOR SELECT * FROM test1 ORDER BY 1,2,3,4;

DECLARE foo10 CURSOR FOR SELECT * FROM test2 ORDER BY 1,2,3,4;

DECLARE foo11 CURSOR FOR SELECT * FROM test1 ORDER BY 1,2,3,4;

DECLARE foo12 CURSOR FOR SELECT * FROM test2 ORDER BY 1,2,3,4;

DECLARE foo13 CURSOR FOR SELECT * FROM test1 ORDER BY 1,2,3,4;

DECLARE foo14 CURSOR FOR SELECT * FROM test2 ORDER BY 1,2,3,4;

DECLARE foo15 CURSOR FOR SELECT * FROM test1 ORDER BY 1,2,3,4;

DECLARE foo16 CURSOR FOR SELECT * FROM test2 ORDER BY 1,2,3,4;

DECLARE foo17 CURSOR FOR SELECT * FROM test1 ORDER BY 1,2,3,4;

DECLARE foo18 CURSOR FOR SELECT * FROM test2 ORDER BY 1,2,3,4;

DECLARE foo19 CURSOR FOR SELECT * FROM test1 ORDER BY 1,2,3,4;

DECLARE foo20 CURSOR FOR SELECT * FROM test2 ORDER BY 1,2,3,4;

DECLARE foo21 CURSOR FOR SELECT * FROM test1 ORDER BY 1,2,3,4;

DECLARE foo22 CURSOR FOR SELECT * FROM test2 ORDER BY 1,2,3,4;

DECLARE foo23 CURSOR FOR SELECT * FROM test1 ORDER BY 1,2,3,4;

FETCH 1 in foo1;

FETCH 2 in foo2;

FETCH 3 in foo3;

FETCH 4 in foo4;

FETCH 5 in foo5;

FETCH 6 in foo6;

FETCH 7 in foo7;

FETCH 8 in foo8;

FETCH 9 in foo9;

FETCH 10 in foo10;

FETCH 11 in foo11;

FETCH 12 in foo12;

FETCH 13 in foo13;

FETCH 14 in foo14;

FETCH 15 in foo15;

FETCH 16 in foo16;

FETCH 17 in foo17;

FETCH 18 in foo18;

FETCH 19 in foo19;

FETCH 20 in foo20;

FETCH 21 in foo21;

FETCH 22 in foo22;

FETCH 23 in foo23;


CLOSE foo1;

CLOSE foo2;

CLOSE foo3;

CLOSE foo4;

CLOSE foo5;

CLOSE foo6;

CLOSE foo7;

CLOSE foo8;

CLOSE foo9;

CLOSE foo10;

CLOSE foo11;

CLOSE foo12;

-- leave some cursors open, to test that auto-close works.

-- record this in the system view as well (don't query the time field there
-- however)
SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors ORDER BY name;

END;

SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors ORDER BY name;

--
-- NO SCROLL disallows backward fetching
--

BEGIN;

DECLARE foo24 NO SCROLL CURSOR FOR SELECT * FROM test1 ORDER BY 1,2,3,4;

FETCH 1 FROM foo24;

FETCH BACKWARD 1 FROM foo24; -- should fail

END;

--
-- Cursors outside transaction blocks
--


SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors ORDER BY name;

BEGIN;

DECLARE foo25 CURSOR WITH HOLD FOR SELECT * FROM test2 ORDER BY 1,2,3,4;

FETCH FROM foo25;

FETCH FROM foo25;

COMMIT;

FETCH FROM foo25;

--FETCH BACKWARD FROM foo25;

--FETCH ABSOLUTE -1 FROM foo25;

SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors ORDER BY name;

CLOSE foo25;

--
-- ROLLBACK should close holdable cursors
--

BEGIN;

DECLARE foo26 CURSOR WITH HOLD FOR SELECT * FROM test1 ORDER BY 1,2,3,4;

ROLLBACK;

-- should fail
FETCH FROM foo26;

-- Create a cursor with the BINARY option and check the pg_cursors view
BEGIN;
SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors ORDER BY name;
DECLARE bc BINARY CURSOR FOR SELECT * FROM test1;
SELECT name, statement, is_holdable, is_binary, is_scrollable FROM pg_cursors ORDER BY name;
ROLLBACK;

-- teardown
DROP TABLE test1;
DROP TABLE test2;

ALTER RESOURCE QUEUE pg_default WITH (active_statements=20);
