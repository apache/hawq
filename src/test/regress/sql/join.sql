--
-- JOIN
-- Test JOIN clauses
--

CREATE TABLE J1_TBL (
  i integer,
  j integer,
  t text
);

CREATE TABLE J2_TBL (
  i integer,
  k integer
);


INSERT INTO J1_TBL VALUES (1, 4, 'one');
INSERT INTO J1_TBL VALUES (2, 3, 'two');
INSERT INTO J1_TBL VALUES (3, 2, 'three');
INSERT INTO J1_TBL VALUES (4, 1, 'four');
INSERT INTO J1_TBL VALUES (5, 0, 'five');
INSERT INTO J1_TBL VALUES (6, 6, 'six');
INSERT INTO J1_TBL VALUES (7, 7, 'seven');
INSERT INTO J1_TBL VALUES (8, 8, 'eight');
INSERT INTO J1_TBL VALUES (0, NULL, 'zero');
INSERT INTO J1_TBL VALUES (NULL, NULL, 'null');
INSERT INTO J1_TBL VALUES (NULL, 0, 'zero');

INSERT INTO J2_TBL VALUES (1, -1);
INSERT INTO J2_TBL VALUES (2, 2);
INSERT INTO J2_TBL VALUES (3, -3);
INSERT INTO J2_TBL VALUES (2, 4);
INSERT INTO J2_TBL VALUES (5, -5);
INSERT INTO J2_TBL VALUES (5, -5);
INSERT INTO J2_TBL VALUES (0, NULL);
INSERT INTO J2_TBL VALUES (NULL, NULL);
INSERT INTO J2_TBL VALUES (NULL, 0);

--
-- CORRELATION NAMES
-- Make sure that table/column aliases are supported
-- before diving into more complex join syntax.
--

SELECT '' AS "xxx", *
  FROM J1_TBL AS tx_join ORDER BY 2,3;

SELECT '' AS "xxx", *
  FROM J1_TBL tx_join ORDER BY 2,3;

SELECT '' AS "xxx", *
  FROM J1_TBL AS t1_join (a, b, c) ORDER BY 2,3;

SELECT '' AS "xxx", *
  FROM J1_TBL t1_join (a, b, c) ORDER BY 2,3;

SELECT '' AS "xxx", *
  FROM J1_TBL t1_join (a, b, c), J2_TBL t2_join (d, e) ORDER BY 2,3,4,5,6;

SELECT '' AS "xxx", t1_join.a, t2_join.e
  FROM J1_TBL t1_join (a, b, c), J2_TBL t2_join (d, e)
  WHERE t1_join.a = t2_join.d ORDER BY 2,3;


--
-- CROSS JOIN
-- Qualifications are not allowed on cross joins,
-- which degenerate into a standard unqualified inner join.
--

SELECT '' AS "xxx", *
  FROM J1_TBL CROSS JOIN J2_TBL ORDER BY 2,3,4,5,6;

-- ambiguous column
SELECT '' AS "xxx", i, k, t
  FROM J1_TBL CROSS JOIN J2_TBL ORDER BY 2,3,4;

-- resolve previous ambiguity by specifying the table name
SELECT '' AS "xxx", t1_join.i, k, t
  FROM J1_TBL t1_join CROSS JOIN J2_TBL t2_join ORDER BY 2,3,4;

SELECT '' AS "xxx", ii, tt, kk
  FROM (J1_TBL CROSS JOIN J2_TBL)
    AS tx_join (ii, jj, tt, ii2, kk) ORDER BY 2,3,4;

SELECT '' AS "xxx", tx_join.ii, tx_join.jj, tx_join.kk
  FROM (J1_TBL t1_join (a, b, c) CROSS JOIN J2_TBL t2_join (d, e))
    AS tx_join (ii, jj, tt, ii2, kk) ORDER BY 2,3,4;

SELECT '' AS "xxx", *
  FROM J1_TBL CROSS JOIN J2_TBL a CROSS JOIN J2_TBL b ORDER BY 2,3,4,5,6,7,8;


--
--
-- Inner joins (equi-joins)
--
--

--
-- Inner joins (equi-joins) with USING clause
-- The USING syntax changes the shape of the resulting table
-- by including a column in the USING clause only once in the result.
--

-- Inner equi-join on specified column
SELECT '' AS "xxx", *
  FROM J1_TBL INNER JOIN J2_TBL USING (i) ORDER BY 2,3,4,5;

-- Same as above, slightly different syntax
SELECT '' AS "xxx", *
  FROM J1_TBL JOIN J2_TBL USING (i) ORDER BY 2,3,4,5;

SELECT '' AS "xxx", *
  FROM J1_TBL t1_join (a, b, c) JOIN J2_TBL t2_join (a, d) USING (a)
  ORDER BY a, d;

SELECT '' AS "xxx", *
  FROM J1_TBL t1_join (a, b, c) JOIN J2_TBL t2_join (a, b) USING (b)
  ORDER BY b, t1_join.a;


--
-- NATURAL JOIN
-- Inner equi-join on all columns with the same name
--

SELECT '' AS "xxx", *
  FROM J1_TBL NATURAL JOIN J2_TBL ORDER BY 2,3,4,5;

SELECT '' AS "xxx", *
  FROM J1_TBL t1_join (a, b, c) NATURAL JOIN J2_TBL t2_join (a, d) ORDER BY 2,3,4,5;

SELECT '' AS "xxx", *
  FROM J1_TBL t1_join (a, b, c) NATURAL JOIN J2_TBL t2_join (d, a) ORDER BY 2,3,4,5;

-- mismatch number of columns
-- currently, Postgres will fill in with underlying names
SELECT '' AS "xxx", *
  FROM J1_TBL t1_join (a, b) NATURAL JOIN J2_TBL t2_join (a) ORDER BY 2,3,4,5;


--
-- Inner joins (equi-joins)
--

SELECT '' AS "xxx", *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.i) ORDER BY 2,3,4,5,6;

SELECT '' AS "xxx", *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.k) ORDER BY 2,3,4,5,6;


--
-- Non-equi-joins
--

SELECT '' AS "xxx", *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i <= J2_TBL.k) ORDER BY 2,3,4,5,6;


--
-- Outer joins
-- Note that OUTER is a noise word
--

SELECT '' AS "xxx", *
  FROM J1_TBL LEFT OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL RIGHT OUTER JOIN J2_TBL USING (i) ORDER BY 2,3,4,5;

SELECT '' AS "xxx", *
  FROM J1_TBL RIGHT JOIN J2_TBL USING (i) ORDER BY 2,3,4,5;

SELECT '' AS "xxx", *
  FROM J1_TBL FULL OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL FULL JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (k = 1) ORDER BY 2,3,4,5;

SELECT '' AS "xxx", *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (i = 1) ORDER BY 2,3,4,5;


--
-- More complicated constructs
--

--
-- Multiway full join
--

CREATE TABLE t1_join (name TEXT, n INTEGER);
CREATE TABLE t2_join (name TEXT, n INTEGER);
CREATE TABLE t3_join (name TEXT, n INTEGER);

INSERT INTO t1_join VALUES ( 'aa', 11 );
INSERT INTO t2_join VALUES ( 'aa', 12 );
INSERT INTO t2_join VALUES ( 'bb', 22 );
INSERT INTO t2_join VALUES ( 'dd', 42 );
INSERT INTO t3_join VALUES ( 'aa', 13 );
INSERT INTO t3_join VALUES ( 'bb', 23 );
INSERT INTO t3_join VALUES ( 'cc', 33 );

SELECT * FROM t1_join FULL JOIN t2_join USING (name) FULL JOIN t3_join USING (name) ORDER BY 1,2,3,4;

--
-- Test interactions of join syntax and subqueries
--

-- Basic cases (we expect planner to pull up the subquery here)
SELECT * FROM
(SELECT * FROM t2_join) as s2
INNER JOIN
(SELECT * FROM t3_join) s3
USING (name) ORDER BY 1,2,3;

SELECT * FROM
(SELECT * FROM t2_join) as s2
LEFT JOIN
(SELECT * FROM t3_join) s3
USING (name) ORDER BY 1,2,3;

SELECT * FROM
(SELECT * FROM t2_join) as s2
FULL JOIN
(SELECT * FROM t3_join) s3
USING (name) ORDER BY 1,2,3;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t2_join) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3_join) s3 ORDER BY 1,2,3;

SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t2_join) as s2
NATURAL LEFT JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3_join) s3 ORDER BY 1,2,3;

SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t2_join) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3_join) s3 ORDER BY 1,2,3;

SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM t1_join) as s1
NATURAL INNER JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM t2_join) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3_join) s3 ORDER BY 1,2,3;

SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM t1_join) as s1
NATURAL FULL JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM t2_join) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3_join) s3 ORDER BY 1,2,3;

SELECT * FROM
(SELECT name, n as s1_n FROM t1_join) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n FROM t2_join) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM t3_join) as s3
  ) ss2 ORDER BY 1,2,3;

SELECT * FROM
(SELECT name, n as s1_n FROM t1_join) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n, 2 as s2_2 FROM t2_join) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM t3_join) as s3
  ) ss2 ORDER BY 1,2,3;


-- Test for propagation of nullability constraints into sub-joins

create temp table x (x1 int, x2 int);
insert into x values (1,11);
insert into x values (2,22);
insert into x values (3,null);
insert into x values (4,44);
insert into x values (5,null);

create temp table y (y1 int, y2 int);
insert into y values (1,111);
insert into y values (2,222);
insert into y values (3,333);
insert into y values (4,null);

select * from x ORDER BY 1,2;
select * from y ORDER BY 1,2;

select * from x left join y on (x1 = y1 and x2 is not null) ORDER BY 1,2,3,4;
select * from x left join y on (x1 = y1 and y2 is not null) ORDER BY 1,2,3,4;

select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) ORDER BY 1,2,3,4;
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and x2 is not null) ORDER BY 1,2,3,4;
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and y2 is not null) ORDER BY 1,2,3,4;
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and xx2 is not null) ORDER BY 1,2,3,4;
-- these should NOT give the same answers as above
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (x2 is not null) ORDER BY 1,2,3,4;
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (y2 is not null) ORDER BY 1,2,3,4;
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (xx2 is not null) ORDER BY 1,2,3,4;

--
-- regression test: check for bug with propagation of implied equality
-- to outside an IN
--
select count(*) from tenk1 a where unique1 in
  (select unique1 from tenk1 b join tenk1 c using (unique1)
   where b.unique2 = 42);

--
-- regression test: check for failure to generate a plan with multiple
-- degenerate IN clauses
--
select count(*) from tenk1 x where
  x.unique1 in (select a.f1 from int4_tbl a,float8_tbl b where a.f1=b.f1) and
  x.unique1 = 0 and
  x.unique1 in (select aa.f1 from int4_tbl aa,float8_tbl bb where aa.f1=bb.f1);


--
-- Clean up
--

DROP TABLE t1_join;
DROP TABLE t2_join;
DROP TABLE t3_join;

DROP TABLE J1_TBL;
DROP TABLE J2_TBL;

-- Both DELETE and UPDATE allow the specification of additional tables
-- to "join" against to determine which rows should be modified.

CREATE TEMP TABLE t1_join (a int, b int);
CREATE TEMP TABLE t2_join (a int, b int);
CREATE TEMP TABLE t3_join (x int, y int);
CREATE TEMP TABLE t4_join (x int, y int);

INSERT INTO t1_join VALUES (5, 10);
INSERT INTO t1_join VALUES (15, 20);
INSERT INTO t1_join VALUES (100, 100);
INSERT INTO t1_join VALUES (200, 1000);
INSERT INTO t2_join VALUES (200, 2000);
INSERT INTO t3_join VALUES (5, 20);
INSERT INTO t3_join VALUES (6, 7);
INSERT INTO t3_join VALUES (7, 8);
INSERT INTO t3_join VALUES (500, 100);
INSERT INTO t4_join SELECT * FROM t3_join;

DELETE FROM t3_join USING t1_join table1 WHERE t3_join.x = table1.a;
SELECT * FROM t3_join ORDER BY 1,2;
DELETE FROM t4_join USING t1_join JOIN t2_join USING (a) WHERE t4_join.x > t1_join.a;
SELECT * FROM t4_join ORDER BY 1,2;
DELETE FROM t3_join USING t3_join t3_other WHERE t3_join.x = t3_other.x AND t3_join.y = t3_other.y;
SELECT * FROM t3_join ORDER BY 1,2;

--
-- regression test for 8.1 merge right join bug
--

CREATE TEMP TABLE tt1 ( tt1_id int4, joincol int4 );
INSERT INTO tt1 VALUES (1, 11);
INSERT INTO tt1 VALUES (2, NULL);

CREATE TEMP TABLE tt2 ( tt2_id int4, joincol int4 );
INSERT INTO tt2 VALUES (21, 11);
INSERT INTO tt2 VALUES (22, 11);

set enable_hashjoin to off;
set enable_nestloop to off;

-- these should give the same results

select tt1.*, tt2.* from tt1 left join tt2 on tt1.joincol = tt2.joincol ORDER BY 1,2,3;

select tt1.*, tt2.* from tt2 right join tt1 on tt1.joincol = tt2.joincol ORDER BY 1,2,3;

reset enable_hashjoin;
reset enable_nestloop;

--
-- regression test for 8.2 bug with improper re-ordering of left joins
--

create temp table tt3(f1 int, f2 text);
insert into tt3 select x, repeat('xyzzy', 100) from generate_series(1,10000) x;
create index tt3i on tt3(f1);
analyze tt3;

create temp table tt4(f1 int);
insert into tt4 values (0),(1),(9999);
analyze tt4;

SELECT a.f1
FROM tt4 a
LEFT JOIN (
        SELECT b.f1
        FROM tt3 b LEFT JOIN tt3 c ON (b.f1 = c.f1)
        WHERE c.f1 IS NULL
) AS d ON (a.f1 = d.f1)
WHERE d.f1 IS NULL;

--
-- regression test for problems of the sort depicted in bug #3494
--

create temp table tt5(f1 int, f2 int);
create temp table tt6(f1 int, f2 int);

insert into tt5 values(1, 10);
insert into tt5 values(1, 11);

insert into tt6 values(1, 9);
insert into tt6 values(1, 2);
insert into tt6 values(2, 9);

select * from tt5,tt6 where tt5.f1 = tt6.f1 and tt5.f1 = tt5.f2 - tt6.f2;

--
-- regression test for problems of the sort depicted in bug #3588
--

create temp table xx (pkxx int);
create temp table yy (pkyy int, pkxx int);

insert into xx values (1);
insert into xx values (2);
insert into xx values (3);

insert into yy values (101, 1);
insert into yy values (201, 2);
insert into yy values (301, NULL);

select yy.pkyy as yy_pkyy, yy.pkxx as yy_pkxx, yya.pkyy as yya_pkyy,
       xxa.pkxx as xxa_pkxx, xxb.pkxx as xxb_pkxx
from yy
     left join (SELECT * FROM yy where pkyy = 101) as yya ON yy.pkyy = yya.pkyy
     left join xx xxa on yya.pkxx = xxa.pkxx
     left join xx xxb on coalesce (xxa.pkxx, 1) = xxb.pkxx;
 
--
-- regression test for improper pushing of constants across outer-join clauses
-- (as seen in early 8.2.x releases)
--

create temp table zt1 (f1 int primary key) distributed by (f1);
create temp table zt2 (f2 int primary key) distributed by (f2);
create temp table zt3 (f3 int primary key) distributed by (f3);
insert into zt1 values(53);
insert into zt2 values(53);

select * from
  zt2 left join zt3 on (f2 = f3)
      left join zt1 on (f3 = f1)
where f2 = 53;

create temp view zv1 as select *,'dummy'::text AS junk from zt1;

select * from
  zt2 left join zt3 on (f2 = f3)
      left join zv1 on (f3 = f1)
where f2 = 53;

-- test numeric hash join

set enable_hashjoin to on;
set enable_mergejoin to off;
set enable_nestloop to off;
create table nhtest (i numeric(10, 2)) distributed by (i);
insert into nhtest values(100000.22);
insert into nhtest values(300000.19);
explain select * from nhtest a join nhtest b using (i);
select * from nhtest a join nhtest b using (i);


--
-- test hash join
--

create table hjtest (i int, j int) distributed by (i,j);
insert into hjtest values(3, 4);

select count(*) from hjtest a1, hjtest a2 where a2.i = least (a1.i,4) and a2.j = 4;

--
-- Predicate propagation over equality conditions
--

drop schema if exists pred;
create schema pred;
set search_path=pred;

create table t1 (x int, y int, z int) distributed by (y);
create table t2 (x int, y int, z int) distributed by (x);
insert into t1 select i, i, i from generate_series(1,100) i;
insert into t2 select * from t1;

analyze t1;
analyze t2;

--
-- infer over equalities
--
explain select count(*) from t1,t2 where t1.x = 100 and t1.x = t2.x;
select count(*) from t1,t2 where t1.x = 100 and t1.x = t2.x;

--
-- infer over >=
--
explain select * from t1,t2 where t1.x = 100 and t2.x >= t1.x;
select * from t1,t2 where t1.x = 100 and t2.x >= t1.x;

--
-- multiple inferences
--

set optimizer_segments=2;
explain select * from t1,t2 where t1.x = 100 and t1.x = t2.y and t1.x <= t2.x;
reset optimizer_segments;
select * from t1,t2 where t1.x = 100 and t1.x = t2.y and t1.x <= t2.x;

--
-- MPP-18537: hash clause references a constant in outer child target list
--

create table hjn_test (i int, j int) distributed by (i,j);
insert into hjn_test values(3, 4);
create table int4_tbl (f1 int);
insert into int4_tbl values(123456), (-2147483647), (0), (-123456), (2147483647);
select count(*) from hjn_test, (select 3 as bar) foo where hjn_test.i = least (foo.bar,4) and hjn_test.j = 4;
select count(*) from hjn_test, (select 3 as bar) foo where hjn_test.i = least (foo.bar,(array[4])[1]) and hjn_test.j = (array[4])[1];
select count(*) from hjn_test, (select 3 as bar) foo where least (foo.bar,(array[4])[1]) = hjn_test.i and hjn_test.j = (array[4])[1];
select count(*) from hjn_test, (select 3 as bar) foo where hjn_test.i = least (foo.bar, least(4,10)) and hjn_test.j = least(4,10);
select * from int4_tbl a join int4_tbl b on (a.f1 = (select f1 from int4_tbl c where c.f1=b.f1));
set enable_hashjoin to off;
select count(*) from hjn_test, (select 3 as bar) foo where hjn_test.i = least (foo.bar,4) and hjn_test.j = 4;
select count(*) from hjn_test, (select 3 as bar) foo where hjn_test.i = least (foo.bar,(array[4])[1]) and hjn_test.j = (array[4])[1];
select count(*) from hjn_test, (select 3 as bar) foo where least (foo.bar,(array[4])[1]) = hjn_test.i and hjn_test.j = (array[4])[1];
select count(*) from hjn_test, (select 3 as bar) foo where hjn_test.i = least (foo.bar, least(4,10)) and hjn_test.j = least(4,10);
select * from int4_tbl a join int4_tbl b on (a.f1 = (select f1 from int4_tbl c where c.f1=b.f1));

reset enable_hashjoin;

drop schema pred cascade;
