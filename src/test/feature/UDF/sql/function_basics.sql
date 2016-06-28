-- SETUP
DROP TABLE IF EXISTS foo;
CREATE TABLE foo AS SELECT * FROM generate_series(1, 10) x;
CREATE FUNCTION f(x INT) RETURNS INT AS $$
BEGIN
RETURN x;
END
$$ LANGUAGE PLPGSQL;



-- DDL, CREATE FUNCTION
CREATE FUNCTION g(int) RETURNS INTEGER AS 'SELECT $1' LANGUAGE SQL IMMUTABLE STRICT;
SELECT proname FROM pg_proc WHERE proname = 'g';
SELECT proname FROM gp_dist_random('pg_proc') WHERE proname = 'g';
DROP FUNCTION g(int);



-- DDL, CREATE OR REPLACE FUNCTION
CREATE FUNCTION g(int) RETURNS INTEGER AS 'SELECT $1' LANGUAGE SQL IMMUTABLE STRICT;
SELECT proname FROM pg_proc WHERE proname = 'g';
SELECT proname FROM gp_dist_random('pg_proc') WHERE proname = 'g';
CREATE OR REPLACE FUNCTION g(x INT) RETURNS INT AS $$
BEGIN
RETURN (-1) * x;
END
$$ LANGUAGE PLPGSQL;
SELECT proname, prosrc FROM pg_proc WHERE proname = 'g';
SELECT proname, prosrc FROM gp_dist_random('pg_proc') WHERE proname = 'g';
DROP FUNCTION g(int);



-- DDL, DROP FUNCTION
CREATE FUNCTION g(int) RETURNS INTEGER AS 'SELECT $1' LANGUAGE SQL IMMUTABLE STRICT;
DROP FUNCTION g(int);
SELECT oid, proname FROM pg_proc WHERE proname = 'g';
SELECT oid, proname FROM gp_dist_random('pg_proc') WHERE proname = 'g';



-- DDL, DROP FUNCTION, NEGATIVE
DROP FUNCTION g(int);



-- DDL, CREATE FUNCTION, RECORD
CREATE FUNCTION foo(int) RETURNS record AS 'SELECT * FROM foo WHERE x=$1' LANGUAGE SQL;
SELECT foo(5);
DROP FUNCTION foo(int);
CREATE FUNCTION foo(int) RETURNS foo AS 'SELECT * FROM foo WHERE x=$1' LANGUAGE SQL;
SELECT foo(5);
DROP FUNCTION foo(int);



-- DDL, CREATE FUNCTION, SRF
CREATE FUNCTION g(x setof int) RETURNS INT
    AS $$ SELECT 1 $$ LANGUAGE SQL;
DROP FUNCTION g(setof int);
CREATE FUNCTION g() RETURNS setof INT
    AS $$ SELECT 1 $$ LANGUAGE SQL;
DROP FUNCTION g();



-- DDL, CREATE FUNCTION, TABLE, NEGATIVE
CREATE FUNCTION g() RETURNS TABLE(x int)
    AS $$ SELECT * FROM foo $$ LANGUAGE SQL;
DROP FUNCTION g();
CREATE FUNCTION g(anytable) RETURNS int
    AS 'does_not_exist', 'does_not_exist' LANGUAGE C;



-- DDL, CREATE FUNCTION, SECURITY DEFINER
CREATE FUNCTION g(int) RETURNS INTEGER AS 'SELECT 1' LANGUAGE SQL IMMUTABLE SECURITY DEFINER;
DROP FUNCTION g(int);


-- DDL, ALTER FUNCTION
-- DDL, STRICT
CREATE FUNCTION g(int) RETURNS INTEGER AS 'SELECT 1' LANGUAGE SQL IMMUTABLE;
SELECT g(NULL);
ALTER FUNCTION g(int) STRICT;
SELECT g(NULL);
DROP FUNCTION g(int);



-- DDL, ALTER FUNCTION, OWNER
CREATE ROLE superuser SUPERUSER;
CREATE ROLE u1;
SET ROLE superuser;
CREATE FUNCTION g(int) RETURNS INTEGER AS 'SELECT 1' LANGUAGE SQL IMMUTABLE;
SELECT a.rolname FROM pg_proc p, pg_authid a where p.proowner = a.oid and proname = 'g';
ALTER FUNCTION g(int) OWNER TO u1;
SELECT a.rolname FROM pg_proc p, pg_authid a where p.proowner = a.oid and proname = 'g';
DROP FUNCTION g(int);
RESET ROLE;
DROP ROLE u1;
DROP ROLE superuser;



-- DDL, ALTER FUNCTION, RENAME
CREATE FUNCTION g(int) RETURNS INTEGER AS 'SELECT 1' LANGUAGE SQL IMMUTABLE;
SELECT g(0);
ALTER FUNCTION g(int) RENAME TO h;
SELECT h(0);
DROP FUNCTION h(int);



-- DDL, ALTER FUNCTION, SET SCHEMA
CREATE SCHEMA bar;
CREATE FUNCTION g(int) RETURNS INTEGER AS 'SELECT 1' LANGUAGE SQL IMMUTABLE;
SELECT g(0);
ALTER FUNCTION g(int) SET SCHEMA bar;
SELECT bar.g(0);
DROP SCHEMA bar CASCADE;



-- DDL, ALTER FUNCTION, SECURITY DEFINER
CREATE FUNCTION g(int) RETURNS INTEGER AS 'SELECT 1' LANGUAGE SQL IMMUTABLE;
ALTER FUNCTION g(int) SECURITY DEFINER;
DROP FUNCTION g(int); 



-- DCL, GRANT/REVOKE
-- GRANT { EXECUTE | ALL [ PRIVILEGES ] }
--    ON FUNCTION funcname ( [ [ argmode ] [ argname ] argtype [, ...] ] ) [, ...]
--    TO { username | GROUP groupname | PUBLIC } [, ...] [ WITH GRANT OPTION ]
-- REVOKE [ GRANT OPTION FOR ]
--    { EXECUTE | ALL [ PRIVILEGES ] }
--    ON FUNCTION funcname ( [ [ argmode ] [ argname ] argtype [, ...] ] ) [, ...]
--    FROM { username | GROUP groupname | PUBLIC } [, ...]
--    [ CASCADE | RESTRICT ]

-- DCL, GRANT/REVOKE, EXECUTE
CREATE ROLE superuser SUPERUSER;
SET ROLE superuser;
CREATE ROLE u1;
GRANT SELECT ON TABLE foo TO u1;
CREATE FUNCTION g(int) RETURNS INTEGER AS 'SELECT $1' LANGUAGE SQL IMMUTABLE STRICT;
SELECT proacl FROM pg_proc where proname = 'g';
REVOKE ALL ON FUNCTION g(int) FROM PUBLIC;
SELECT proacl FROM pg_proc where proname = 'g';
SELECT g(1);
SELECT count(g(x)) FROM foo;
SET ROLE u1;
SELECT g(1);
SELECT count(g(x)) FROM foo;
SET ROLE superuser;
GRANT EXECUTE ON FUNCTION g(int) TO u1;
SELECT proacl FROM pg_proc where proname = 'g';
SET ROLE u1;
SELECT g(1);
SELECT count(g(x)) FROM foo;
SET ROLE superuser;
REVOKE EXECUTE ON FUNCTION g(int) FROM u1;
SELECT proacl FROM pg_proc where proname = 'g';
SET ROLE u1;
SELECT g(1);
SELECT count(g(x)) FROM foo;
RESET ROLE;
DROP FUNCTION g(int);
REVOKE SELECT ON TABLE foo FROM u1;
DROP ROLE u1;
DROP ROLE superuser;



-- DCL, GRANT/REVOKE, PUBLIC
CREATE ROLE superuser SUPERUSER;
SET ROLE superuser;
CREATE ROLE u1;
GRANT SELECT ON TABLE foo TO u1;
CREATE FUNCTION g(int) RETURNS INTEGER AS 'SELECT $1' LANGUAGE SQL IMMUTABLE STRICT;
SELECT proacl FROM pg_proc where proname = 'g';
REVOKE ALL ON FUNCTION g(int) FROM PUBLIC;
SELECT proacl FROM pg_proc where proname = 'g';
SELECT g(1);
SELECT count(g(x)) FROM foo;
SET ROLE u1;
SELECT g(1);
SELECT count(g(x)) FROM foo;
SET ROLE superuser;
GRANT EXECUTE ON FUNCTION g(int) TO PUBLIC;
SELECT proacl FROM pg_proc where proname = 'g';
SET ROLE u1;
SELECT g(1);
SELECT count(g(x)) FROM foo;
SET ROLE superuser;
REVOKE EXECUTE ON FUNCTION g(int) FROM PUBLIC;
SELECT proacl FROM pg_proc where proname = 'g';
SET ROLE u1;
SELECT g(1);
SELECT count(g(x)) FROM foo;
RESET ROLE;
DROP FUNCTION g(int);
REVOKE SELECT ON TABLE foo FROM u1;
DROP ROLE u1;
DROP ROLE superuser;



-- DCL, GRANT/REVOKE, Groups
CREATE ROLE superuser SUPERUSER;
SET ROLE superuser;
CREATE ROLE u1;
CREATE ROLE u2 IN GROUP u1;
GRANT SELECT ON TABLE foo TO u1;
CREATE FUNCTION g(int) RETURNS INTEGER AS 'SELECT $1' LANGUAGE SQL IMMUTABLE STRICT;
SELECT proacl FROM pg_proc where proname = 'g';
REVOKE ALL ON FUNCTION g(int) FROM PUBLIC;
SELECT proacl FROM pg_proc where proname = 'g';
SELECT g(1);
SELECT count(g(x)) FROM foo;
SET ROLE u2;
SELECT g(1);
SELECT count(g(x)) FROM foo;
SET ROLE superuser;
GRANT EXECUTE ON FUNCTION g(int) TO u1;
SELECT proacl FROM pg_proc where proname = 'g';
SET ROLE u2;
SELECT g(1);
SELECT count(g(x)) FROM foo;
SET ROLE superuser;
REVOKE EXECUTE ON FUNCTION g(int) FROM u1;
SELECT proacl FROM pg_proc where proname = 'g';
SET ROLE u2;
SELECT g(1);
SELECT count(g(x)) FROM foo;
RESET ROLE;
DROP FUNCTION g(int);
REVOKE SELECT ON TABLE foo FROM u1;
DROP ROLE u1;
DROP ROLE u2;
DROP ROLE superuser;



-- DCL, GRANT/REVOKE, WITH GRANT OPTION
CREATE ROLE superuser SUPERUSER;
SET ROLE superuser;
CREATE ROLE u1;
CREATE ROLE u2;
GRANT SELECT ON TABLE foo TO PUBLIC;
CREATE FUNCTION g(int) RETURNS INTEGER AS 'SELECT $1' LANGUAGE SQL IMMUTABLE STRICT;
SELECT proacl FROM pg_proc where proname = 'g';
REVOKE ALL ON FUNCTION g(int) FROM PUBLIC;
SELECT proacl FROM pg_proc where proname = 'g';
SELECT g(1);
SELECT count(g(x)) FROM foo;
SET ROLE u2;
SELECT g(1);
SELECT count(g(x)) FROM foo;
SET ROLE superuser;
GRANT ALL ON FUNCTION g(int) TO u1 WITH GRANT OPTION;
SET ROLE u1;
GRANT ALL ON FUNCTION g(int) TO u2;
SELECT proacl FROM pg_proc where proname = 'g';
SET ROLE u1;
SELECT g(1);
SELECT count(g(x)) FROM foo;
SET ROLE u2;
SELECT g(1);
SELECT count(g(x)) FROM foo;
SET ROLE superuser;
REVOKE ALL ON FUNCTION g(int) FROM u1 CASCADE;
SELECT proacl FROM pg_proc where proname = 'g';
SET ROLE u1;
SELECT g(1);
SELECT count(g(x)) FROM foo;
SET ROLE u2;
SELECT g(1);
SELECT count(g(x)) FROM foo;
RESET ROLE;
DROP FUNCTION g(int);
REVOKE SELECT ON TABLE foo FROM PUBLIC;
DROP ROLE u1;
DROP ROLE u2;
DROP ROLE superuser;



-- DML, CaseExpr
SELECT CASE WHEN x % 2 = 0 THEN f(x) ELSE 0 END FROM foo ORDER BY x;



-- DML, OpExpr
SELECT f(x) + f(x) FROM foo ORDER BY x;
SELECT f(x) + f(x) + f(x) FROM foo ORDER BY x;
SELECT f(x) + f(x) - f(x) FROM foo ORDER BY x;



-- DML, FuncExpr
CREATE FUNCTION g(x INT) RETURNS INT AS $$
BEGIN
RETURN x;
END
$$ LANGUAGE PLPGSQL;
SELECT g(f(x)) FROM foo ORDER BY x;
DROP FUNCTION g(int);

-- DML, BoolExpr
SELECT x % 2 = 0 AND f(x) % 2 = 1 FROM foo ORDER BY x;



-- DML, DistinctExpr
SELECT x IS DISTINCT FROM f(x) from foo ORDER BY x;



-- DML, PercentileExpr
SELECT MEDIAN(f(x)) FROM foo;



-- DML, Complex Expression
CREATE FUNCTION g(x INT) RETURNS INT AS $$
BEGIN
RETURN x;
END
$$ LANGUAGE PLPGSQL;
SELECT CASE
	WHEN x % 2 = 0 THEN g(g(x)) + g(g(x))
	WHEN f(x) % 2 = 1 THEN g(g(x)) - g(g(x))
	END FROM foo ORDER BY x;
DROP FUNCTION g(int);



-- DML, Qual
SELECT x FROM foo WHERE f(x) % 2 = 0 ORDER BY x;



-- DML, FROM
SELECT * FROM f(5);



-- DML, Grouping
SELECT DISTINCT f(x) FROM foo ORDER BY f(x);
SELECT f(x) FROM foo GROUP BY f(x) ORDER BY f(x);



-- DML, Join
SELECT a.x FROM foo a, foo b WHERE f(a.x) = f(b.x) ORDER BY x;
SELECT a.x FROM foo a JOIN foo b ON f(a.x) = f(b.x) ORDER BY x;



-- DML, Windowing
SELECT avg(x) OVER (PARTITION BY f(x)) FROM foo ORDER BY x;



-- DML, CTE
WITH t AS (SELECT x from foo)
	SELECT f(x) from t ORDER BY x;



-- DML, InitPlan
SELECT UNNEST(ARRAY(SELECT x FROM foo)) ORDER BY 1;
SELECT UNNEST(ARRAY(SELECT f(1)));



-- PROPERTIES, VOLATILITY, IMMUTABLE
CREATE FUNCTION g() RETURNS float AS 'SELECT random();' LANGUAGE SQL IMMUTABLE;
SELECT COUNT(DISTINCT(g())) > 1 FROM foo;
DROP FUNCTION g();



-- PROPERTIES, VOLATILITY, STABLE
CREATE FUNCTION g() RETURNS float AS 'SELECT random();' LANGUAGE SQL STABLE;
SELECT COUNT(DISTINCT(g())) > 1 FROM foo;
DROP FUNCTION g();



-- PROPERTIES, VOLATILITY, VOLATILE
CREATE FUNCTION g() RETURNS float AS 'SELECT random();' LANGUAGE SQL VOLATILE;
SELECT COUNT(DISTINCT(g())) > 1 FROM foo;
DROP FUNCTION g();

-----------------
-- NEGATIVE TESTS
-----------------
SELECT h(1);
-- DML, InitPlan
SELECT UNNEST(ARRAY(SELECT f(x) from foo));

-- LANGUAGES not yet supported
-- CREATE LANGUAGE plr;
-- CREATE LANGUAGE plpython;
-- CREATE LANGUAGE pljava;
-- CREATE LANGUAGE plperl;

-- NESTED FUNCTION
CREATE FUNCTION inner(int) RETURNS INTEGER AS 'SELECT 1' LANGUAGE SQL IMMUTABLE;
CREATE FUNCTION outer(x INT) RETURNS INT AS $$
BEGIN
RETURN inner(x);
END
$$ LANGUAGE PLPGSQL;
SELECT outer(0);
SELECT outer(0) FROM foo;
DROP FUNCTION outer(int);
DROP FUNCTION inner(int);



-- TEARDOWN
DROP TABLE foo;



-- HAWQ-510
drop table if exists testEntryDB;
create table testEntryDB(key int, value int) distributed randomly;
insert into testEntryDB values(1, 0);
select t2.key, t2.value
from   (select key, value from testEntryDB where value = 0) as t1,
       (select generate_series(1,2)::int as key, 0::int as value) as t2
where  t1.value=t2.value;
drop table testEntryDB;
