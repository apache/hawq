CREATE TABLE foo2(fooid int, f2 int);
INSERT INTO foo2 VALUES(1, 11);
INSERT INTO foo2 VALUES(2, 22);
INSERT INTO foo2 VALUES(1, 111);

CREATE FUNCTION foot(int) returns setof foo2 as 'SELECT * FROM foo2 WHERE fooid = $1;' LANGUAGE SQL;
select foot.fooid, foot.f2 from foot(sin(pi()/2)::int) ORDER BY 1,2;

CREATE TABLE foo (fooid int, foosubid int, fooname text);
INSERT INTO foo VALUES(1,1,'Joe');
INSERT INTO foo VALUES(1,2,'Ed');
INSERT INTO foo VALUES(2,1,'Mary');

CREATE FUNCTION getfoo(int) RETURNS setof int AS 'SELECT fooid FROM foo WHERE fooid = $1;' LANGUAGE SQL;
SELECT * FROM getfoo(1) AS t1;
CREATE VIEW vw_getfoo AS SELECT * FROM getfoo(1);
SELECT * FROM vw_getfoo;

DROP VIEW vw_getfoo;
DROP FUNCTION getfoo(int);
DROP FUNCTION foot(int);
DROP TABLE foo2;
DROP TABLE foo;

-- setof as a paramater --
CREATE TYPE numtype as (i int, j int);

CREATE FUNCTION g_numtype(x setof numtype) RETURNS setof numtype AS $$ select $1; $$ LANGUAGE SQL;

DROP FUNCTION g_numtype(x setof numtype);
DROP TYPE numtype;

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
