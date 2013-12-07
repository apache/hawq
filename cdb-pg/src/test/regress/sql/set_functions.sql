-- start_ignore
drop schema set_functions cascade;
-- end_ignore
create schema set_functions;
set search_path=set_functions;

SELECT name, setting FROM pg_settings WHERE name LIKE 'enable%';

CREATE TABLE foo2(fooid int, f2 int);
INSERT INTO foo2 VALUES(1, 11);
INSERT INTO foo2 VALUES(2, 22);
INSERT INTO foo2 VALUES(1, 111);

CREATE FUNCTION foot(int) returns setof foo2 as 'SELECT * FROM foo2 WHERE fooid = $1;' LANGUAGE SQL;


-- function in subselect
select * from foo2 where f2 in (select f2 from foot(1) z where z.fooid = foo2.fooid) ORDER BY 1,2;

-- nested functions
select foot.fooid, foot.f2 from foot(sin(pi()/2)::int) ORDER BY 1,2;

CREATE TABLE foo (fooid int, foosubid int, fooname text, primary key(fooid,foosubid));
INSERT INTO foo VALUES(1,1,'Joe');
INSERT INTO foo VALUES(1,2,'Ed');
INSERT INTO foo VALUES(2,1,'Mary');

CREATE FUNCTION getfoo(int) RETURNS setof int AS 'SELECT fooid FROM foo WHERE fooid = $1;' LANGUAGE SQL;
SELECT * FROM getfoo(1) AS t1;
CREATE VIEW vw_getfoo AS SELECT * FROM getfoo(1);
SELECT * FROM vw_getfoo;

-- sql, proretset = t, prorettype = b
DROP VIEW vw_getfoo;
DROP FUNCTION getfoo(int);
CREATE FUNCTION getfoo(int) RETURNS setof text AS 'SELECT fooname FROM foo WHERE fooid = $1;' LANGUAGE SQL;
SELECT * FROM getfoo(1) AS t1;
CREATE VIEW vw_getfoo AS SELECT * FROM getfoo(1);
SELECT * FROM vw_getfoo;

-- sql, proretset = t, prorettype = c
DROP VIEW vw_getfoo;
DROP FUNCTION getfoo(int);
CREATE FUNCTION getfoo(int) RETURNS setof foo AS 'SELECT * FROM foo WHERE fooid = $1 ORDER BY 1,2,3;' LANGUAGE SQL;
SELECT * FROM getfoo(1) AS t1;
CREATE VIEW vw_getfoo AS SELECT * FROM getfoo(1);
SELECT * FROM vw_getfoo;

-- sql, proretset = t, prorettype = record
DROP VIEW vw_getfoo;
DROP FUNCTION getfoo(int);
CREATE FUNCTION getfoo(int) RETURNS setof record AS 'SELECT * FROM foo WHERE fooid = $1 ORDER BY 1,2,3;' LANGUAGE SQL;
SELECT * FROM getfoo(1) AS t1(fooid int, foosubid int, fooname text);
CREATE VIEW vw_getfoo AS SELECT * FROM getfoo(1) AS
(fooid int, foosubid int, fooname text);
SELECT * FROM vw_getfoo;


DROP VIEW vw_getfoo;
DROP FUNCTION getfoo(int);
DROP FUNCTION foot(int);
DROP TABLE foo2;
DROP TABLE foo;

-- Rescan tests --
CREATE TABLE foorescan (fooid int, foosubid int, fooname text, primary key(fooid,foosubid));
INSERT INTO foorescan values(5000,1,'abc.5000.1');
INSERT INTO foorescan values(5001,1,'abc.5001.1');
INSERT INTO foorescan values(5002,1,'abc.5002.1');
INSERT INTO foorescan values(5003,1,'abc.5003.1');
INSERT INTO foorescan values(5004,1,'abc.5004.1');
INSERT INTO foorescan values(5005,1,'abc.5005.1');
INSERT INTO foorescan values(5006,1,'abc.5006.1');
INSERT INTO foorescan values(5007,1,'abc.5007.1');
INSERT INTO foorescan values(5008,1,'abc.5008.1');
INSERT INTO foorescan values(5009,1,'abc.5009.1');

INSERT INTO foorescan values(5000,2,'abc.5000.2');
INSERT INTO foorescan values(5001,2,'abc.5001.2');
INSERT INTO foorescan values(5002,2,'abc.5002.2');
INSERT INTO foorescan values(5003,2,'abc.5003.2');
INSERT INTO foorescan values(5004,2,'abc.5004.2');
INSERT INTO foorescan values(5005,2,'abc.5005.2');
INSERT INTO foorescan values(5006,2,'abc.5006.2');
INSERT INTO foorescan values(5007,2,'abc.5007.2');
INSERT INTO foorescan values(5008,2,'abc.5008.2');
INSERT INTO foorescan values(5009,2,'abc.5009.2');

INSERT INTO foorescan values(5000,3,'abc.5000.3');
INSERT INTO foorescan values(5001,3,'abc.5001.3');
INSERT INTO foorescan values(5002,3,'abc.5002.3');
INSERT INTO foorescan values(5003,3,'abc.5003.3');
INSERT INTO foorescan values(5004,3,'abc.5004.3');
INSERT INTO foorescan values(5005,3,'abc.5005.3');
INSERT INTO foorescan values(5006,3,'abc.5006.3');
INSERT INTO foorescan values(5007,3,'abc.5007.3');
INSERT INTO foorescan values(5008,3,'abc.5008.3');
INSERT INTO foorescan values(5009,3,'abc.5009.3');

INSERT INTO foorescan values(5000,4,'abc.5000.4');
INSERT INTO foorescan values(5001,4,'abc.5001.4');
INSERT INTO foorescan values(5002,4,'abc.5002.4');
INSERT INTO foorescan values(5003,4,'abc.5003.4');
INSERT INTO foorescan values(5004,4,'abc.5004.4');
INSERT INTO foorescan values(5005,4,'abc.5005.4');
INSERT INTO foorescan values(5006,4,'abc.5006.4');
INSERT INTO foorescan values(5007,4,'abc.5007.4');
INSERT INTO foorescan values(5008,4,'abc.5008.4');
INSERT INTO foorescan values(5009,4,'abc.5009.4');

INSERT INTO foorescan values(5000,5,'abc.5000.5');
INSERT INTO foorescan values(5001,5,'abc.5001.5');
INSERT INTO foorescan values(5002,5,'abc.5002.5');
INSERT INTO foorescan values(5003,5,'abc.5003.5');
INSERT INTO foorescan values(5004,5,'abc.5004.5');
INSERT INTO foorescan values(5005,5,'abc.5005.5');
INSERT INTO foorescan values(5006,5,'abc.5006.5');
INSERT INTO foorescan values(5007,5,'abc.5007.5');
INSERT INTO foorescan values(5008,5,'abc.5008.5');
INSERT INTO foorescan values(5009,5,'abc.5009.5');

CREATE FUNCTION foorescan(int,int) RETURNS setof foorescan AS 'SELECT * FROM foorescan WHERE fooid >= $1 and fooid < $2 ;' LANGUAGE SQL;

--invokes ExecFunctionReScan
SELECT * FROM foorescan f WHERE f.fooid IN (SELECT fooid FROM foorescan(5002,5004)) ORDER BY 1,2;

CREATE VIEW vw_foorescan AS SELECT * FROM foorescan(5002,5004);

--invokes ExecFunctionReScan
SELECT * FROM foorescan f WHERE f.fooid IN (SELECT fooid FROM vw_foorescan) ORDER BY 1,2;

CREATE TABLE barrescan (fooid int primary key);
INSERT INTO barrescan values(5003);
INSERT INTO barrescan values(5004);
INSERT INTO barrescan values(5005);
INSERT INTO barrescan values(5006);
INSERT INTO barrescan values(5007);
INSERT INTO barrescan values(5008);

CREATE FUNCTION foorescan(int) RETURNS setof foorescan AS 'SELECT * FROM foorescan WHERE fooid = $1;' LANGUAGE SQL;

SELECT b.fooid, max(f.foosubid) FROM barrescan b, foorescan f WHERE f.fooid = b.fooid AND b.fooid IN (SELECT fooid FROM foorescan(b.fooid)) GROUP BY b.fooid ORDER BY 1,2;

CREATE VIEW fooview1 AS SELECT f.* FROM barrescan b, foorescan f WHERE f.fooid = b.fooid AND b.fooid IN (SELECT fooid FROM foorescan(b.fooid)) ORDER BY 1,2;

CREATE VIEW fooview2 AS SELECT b.fooid, max(f.foosubid) AS maxsubid FROM barrescan b, foorescan f WHERE f.fooid = b.fooid AND b.fooid IN (SELECT fooid FROM foorescan(b.fooid)) GROUP BY b.fooid ORDER BY 1,2;
SELECT * FROM fooview2 AS fv WHERE fv.maxsubid = 5;

DROP VIEW vw_foorescan;
DROP VIEW fooview1;
DROP VIEW fooview2;
DROP FUNCTION foorescan(int,int);
DROP FUNCTION foorescan(int);
DROP TABLE foorescan;
DROP TABLE barrescan;

--
-- Set functions samples from Madlib
--
create function combination(s text) returns setof text[] as $$
x = s.split(',')

def subset(myset, N):
   left = []
   right = []
   for i in range(0, len(myset)):
      if ((1 << i) & N) > 0:
         left.append(myset[i])
      else:
         right.append(myset[i])
   return (', '.join(left), ', '.join(right))

for i in range(1, (1 << len(x)) - 2):
   yield subset(x, i)
$$ language plpythonu strict;

select x[1] || ' => ' || x[2] from combination('a,b,c,d') x;

CREATE TABLE rules(rule text) distributed by (rule);
insert into rules values('a,b,c');
insert into rules values('d,e');
insert into rules values('f,g,h,i,j');
insert into rules values('k,l,m');

SELECT rule, combination(rule) from rules order by 1,2;

DROP TABLE IF EXISTS foo;
CREATE TABLE foo AS SELECT rule, combination(rule) from rules distributed by (rule);


-- UDT as argument/return type of set returning UDF
CREATE TYPE r_type as (a int, b text);

CREATE FUNCTION f1(x r_type) returns setof text as $$ SELECT $1.b from generate_series(1, $1.a) $$ language sql;
CREATE FUNCTION f2(x int) returns setof r_type as $$ SELECT i, 'hello'::text from generate_series(1, $1) i $$ language sql;
CREATE FUNCTION f3(x r_type) returns setof r_type as $$ SELECT $1 from generate_series(1, $1.a) $$ language sql;

SELECT f1(row(2, 'hello'));
SELECT f2(2);
SELECT f3(row(2,'hello'));

SELECT * FROM f1(row(2,'hello'));
SELECT * FROM f2(2);
SELECT * FROM f3(row(2,'hello'));

CREATE TABLE t1 as SELECT i from generate_series(1,5) i distributed by (i);

SELECT i, f1(row(i, 'hello')) from t1;
SELECT i, f2(i) from t1;
SELECT i, f3(row(i,'hello')) from t1;

CREATE TABLE o1 as SELECT f1(row(i, 'hello')) from t1;
CREATE TABLE o2 as SELECT f2(i) from t1;
CREATE TABLE o3 as SELECT f3(row(i,'hello')) from t1;

-- start_ignore
drop schema set_functions cascade;
-- end_ignore
