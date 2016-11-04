--
-- These tests are intended to cover GPSQL-1260.  Which means queries
-- whose plan contains combinations of InitPlan and SubPlan nodes.
--
-- Derived from //cdbfast/main/subquery/mpp8334/

-- SUITE: hash-vs-nl-not-in
-- start_ignore
drop schema if exists subplan_tests cascade;
-- end_ignore
create schema subplan_tests;
set search_path=subplan_tests;

create table t1(a int, b int) distributed by (a);
insert into t1 select i, i+10 from generate_series(-5,5)i;

create table i3(a int not null, b int not null) distributed by (a);
insert into i3 select i-1, i from generate_series(1,5)i;

create table i4(a int, b int) distributed by (a);
insert into i4 values(null,null);
insert into i4 select i, i-10 from generate_series(-5,0)i;

DROP LANGUAGE IF EXISTS plpythonu CASCADE;
CREATE LANGUAGE plpythonu;

create or replace function twice(int) returns int as $$
       select 2 * $1;
$$ language sql;

create or replace function half(int) returns int as $$
begin
	return $1 / 2;
end;
$$ language plpgsql;

create or replace function thrice(x int) returns int as $$
    if (x is None):
        return 0
    else:
        return x * 3
$$ language plpythonu;

select t1.* from t1 where (t1.a, t1.b) not in
   (select twice(i3.a), i3.b from i3 union select i4.a, thrice(i4.b) from i4);

select t1.* from t1 where (t1.a, half(t1.b)) not in
   (select twice(i3.a), i3.b from i3 union all select i4.a, i4.b from i4);

select t1.a, half(t1.b) from t1 where (t1.a, t1.b) not in
   (select 1, thrice(2) union select 3, 4);

select t1.* from t1 where (half(t1.a), t1.b) not in
   (select thrice(i3.a), i3.b from i3 union select i4.a, i4.b from i4);

select t1.* from t1 where (t1.a, t1.b) not in
   (select i3.a, half(i3.b) from i3 union all
      select i4.a, thrice(i4.b) from i4);

-- Two SubPlan nodes
select t1.* from t1 where (t1.a, t1.b) not in (select i3.a, i3.b from i3) or
   (t1.a, t1.b) not in (select i4.a, i4.b from i4);

-- Two SubPlan nodes
select t1.* from t1 where
   (t1.a, twice(t1.b)) not in (select thrice(i3.a), i3.b from i3) or
      (t1.a, half(t1.b)) not in (select i4.a, i4.b from i4);

-- Two SubPlan nodes
select t1.* from t1 where (t1.a, t1.b) not in (select i3.a,i3.b from i3) or
   (t1.a, half(t1.b)) not in (select thrice(i4.a), i4.b from i4);

-- SUITE: diff-rel-cols-not-in
truncate table t1;
create table t2(a int, b int) distributed by (a);

insert into t1 select i, i-10 from generate_series(-1,3)i;
insert into t2 select i, i-10 from generate_series(2,5)i;

create table i1(a int, b int) distributed by (a);
insert into i1 select i, i-10 from generate_series(3,6)i;

create or replace function twice(int) returns int as $$
       select 2 * $1;
$$ language sql;

create or replace function half(int) returns int as $$
begin
	return $1 / 2;
end;
$$ language plpgsql;

create or replace function thrice(x int) returns int as $$
    if x is not None:
        return x * 3
    return 0
$$ language plpythonu;

select t1.a, twice(t2.b) from t1, t2 where t1.a = half(t2.a) or
   ((t1.a, t2.b) not in (select i1.a, thrice(i1.b) from i1));

select t1.a, t2.b from t1 left join t2 on
   (t1.a = t2.a and ((t1.a, half(t2.b)) not in (select i1.a, i1.b from i1)));

select t1.a, t2.b from t1, t2 where t1.a = t2.a or
   ((t1.a, t2.b) not in (select thrice(i1.a), i1.b from i1));

select t1.a, t2.b from t1 left join t2 on
   (thrice(t1.a) = thrice(t2.a) and
      ((t1.a, t2.b) not in (select i1.a, i1.b from i1)));

select t1.a, t2.b from t1, t2 where t1.a = t2.a or
   ((t1.a, t2.b) not in (select i1.a, half(i1.b) from i1));

select t1.a, t2.b from t1 left join t2 on
   (t1.a = t2.a and
      ((t1.a, twice(t2.b)) not in (select i1.a, thrice(i1.b) from i1)));

-- From MPP-2869
create table bug_data (domain integer, class integer, attr text, value integer)
   distributed by (domain);
insert into bug_data values(1, 1, 'A', 1);
insert into bug_data values(2, 1, 'A', 0);
insert into bug_data values(3, 0, 'B', 1);

-- This one is contains one InitPlan without any SubPlan.
create table foo as 
SELECT attr, class, (select thrice(count(distinct class)::int) from bug_data)
   as dclass FROM bug_data GROUP BY attr, class distributed by (attr);

-- Query from GPSQL-1260, produces InitPlan and a SubPlan.
create or replace function nop(a int) returns int as $$ return a $$
language plpythonu;
create table toy as select generate_series(1, 10) i distributed by (i);
select * from toy; -- only for debugging
select array(select nop(i) from toy order by i);

-- start_ignore
drop schema subplan_tests cascade;
-- end_ignore
