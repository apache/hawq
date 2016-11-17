--
-- ORCA tests
--

-- show version
SELECT count(*) from gp_opt_version();

set optimizer_enable_master_only_queries = on;
-- master only tables

create schema orca;

create table orca.r(a int, b int);
insert into orca.r select i, i/3 from generate_series(1,20) i;

create table orca.s(c int, d int);
insert into orca.s select i, i/2 from generate_series(1,30) i;

set optimizer_log=on;
set optimizer=on;
----------------------------------------------------------------------
-- expected fall back to the planner
select sum(distinct a), count(distinct b) from orca.r;

select * from orca.r;
select * from orca.r, orca.s where r.a=s.c;
select * from orca.r, orca.s where r.a<s.c+1 or r.a>s.c;
select sum(r.a) from orca.r;
select count(*) from orca.r;
select a, b from orca.r, orca.s group by a,b;
select r.a+1 from orca.r;
select * from orca.r, orca.s where r.a<s.c or (r.b<s.d and r.b>s.c);
select case when r.a<s.c then r.a<s.c else r.a<s.c end from orca.r, orca.s;
select case r.b<s.c when true then r.b else s.c end from orca.r, orca.s where r.a = s.d;
select * from orca.r order by a, b limit 100;
select * from orca.r order by a, b limit 10 offset 9;
select * from orca.r order by a, b offset 10;
select sqrt(r.a) from orca.r;
select pow(r.b,r.a) from orca.r;
select b from orca.r group by b having  count(*) > 2;
select b from orca.r group by b having  count(*) <= avg(a) + (select count(*) from orca.s where s.c = r.b);
select sum(a) from orca.r group by b having count(*) > 2 order by b+1;
select sum(a) from orca.r group by b having count(*) > 2 order by b+1;

-- constants

select 0.001::numeric from orca.r;
select NULL::text, NULL::int from orca.r;
select 'helloworld'::text, 'helloworld2'::varchar from orca.r;
select 129::bigint, 5623::int, 45::smallint from orca.r;

select 0.001::numeric from orca.r;
select NULL::text, NULL::int from orca.r;
select 'helloworld'::text, 'helloworld2'::varchar from orca.r;
select 129::bigint, 5623::int, 45::smallint from orca.r;

--  distributed tables

----------------------------------------------------------------------
set optimizer=off;

create table orca.foo (x1 int, x2 int, x3 int);
create table orca.bar1 (x1 int, x2 int, x3 int) distributed randomly;
create table orca.bar2 (x1 int, x2 int, x3 int) distributed randomly;

insert into orca.foo select i,i+1,i+2 from generate_series(1,10) i;

insert into orca.bar1 select i,i+1,i+2 from generate_series(1,20) i;
insert into orca.bar2 select i,i+1,i+2 from generate_series(1,30) i;

set optimizer=on;
----------------------------------------------------------------------

-- produces result node

select x2 from orca.foo where x1 in (select x2 from orca.bar1);
select 1;
SELECT 1 AS one FROM orca.foo having 1 < 2;
SELECT generate_series(1,5) AS one FROM orca.foo having 1 < 2;
SELECT 1 AS one FROM orca.foo group by x1 having 1 < 2;
SELECT x1 AS one FROM orca.foo having 1 < 2;

-- distinct clause
select distinct 1, null;
select distinct 1, null from orca.foo;
select distinct 1, sum(x1) from orca.foo;
select distinct x1, rank() over(order by x1) from (select x1 from orca.foo order by x1) x;
select distinct x1, sum(x3) from orca.foo group by x1,x2;
select distinct s from (select sum(x2) s from orca.foo group by x1) x;
select * from orca.foo a where a.x1 = (select distinct sum(b.x1)+avg(b.x1) sa from orca.bar1 b group by b.x3 order by sa limit 1);
select distinct a.x1 from orca.foo a where a.x1 <= (select distinct sum(b.x1)+avg(b.x1) sa from orca.bar1 b group by b.x3 order by sa limit 1) order by 1;
select * from orca.foo a where a.x1 = (select distinct b.x1 from orca.bar1 b where b.x1=a.x1 limit 1);

-- with clause
with cte1 as (select * from orca.foo) select a.x1+1 from (select * from cte1) a group by a.x1;
select count(*)+1 from orca.bar1 b where b.x1 < any (with cte1 as (select * from orca.foo) select a.x1+1 from (select * from cte1) a group by a.x1);
select count(*)+1 from orca.bar1 b where b.x1 < any (with cte1 as (select * from orca.foo) select a.x1 from (select * from cte1) a group by a.x1);
select count(*)+1 from orca.bar1 b where b.x1 < any (with cte1 as (select * from orca.foo) select a.x1 from cte1 a group by a.x1);
with cte1 as (select * from orca.foo) select count(*)+1 from cte1 a where a.x1 < any (with cte2 as (select * from cte1 b where b.x1 > 10) select c.x1 from (select * from cte2) c group by c.x1);
with cte1 as (select * from orca.foo) select count(*)+1 from cte1 a where a.x1 < any (with cte2 as (select * from cte1 b where b.x1 > 10) select c.x1+1 from (select * from cte2) c group by c.x1);
with x as (select * from orca.foo) select count(*) from (select * from x) y where y.x1 <= (select count(*) from x);
with x as (select * from orca.foo) select count(*)+1 from (select * from x) y where y.x1 <= (select count(*) from x);
with x as (select * from orca.foo) select count(*) from (select * from x) y where y.x1 < (with z as (select * from x) select count(*) from z);

-- outer references
select count(*)+1 from orca.foo x where x.x1 > (select count(*)+1 from orca.bar1 y where y.x1 = x.x2);
select count(*)+1 from orca.foo x where x.x1 > (select count(*) from orca.bar1 y where y.x1 = x.x2);
select count(*) from orca.foo x where x.x1 > (select count(*)+1 from orca.bar1 y where y.x1 = x.x2);

----------------------------------------------------------------------
set optimizer=off;

drop table orca.r cascade;
create table orca.r(a int, b int) distributed by (a);
insert into orca.r select i, i%3 from generate_series(1,20) i;

drop table orca.s;
create table orca.s(c int, d int) distributed by (c);

insert into orca.s select i%7, i%2 from generate_series(1,30) i;

analyze orca.r;
analyze orca.s;

set optimizer=on;
----------------------------------------------------------------------

select * from orca.r, orca.s where r.a=s.c;

-- Materialize node
select * from orca.r, orca.s where r.a<s.c+1 or r.a>s.c;

-- empty target list
select r.* from orca.r, orca.s where s.c=2;

----------------------------------------------------------------------
set optimizer=off;

create table orca.m(a int, b int);
create table orca.m1(a int, b int);

insert into orca.m select i-1, i%2 from generate_series(1,35) i;
insert into orca.m1 select i-2, i%3 from generate_series(1,25) i;

insert into orca.r values (null, 1);

set optimizer=on;
----------------------------------------------------------------------

-- join types
select r.a, s.c from orca.r left outer join orca.s on(r.a=s.c);
select r.a, s.c from orca.r left outer join orca.s on(r.a=s.c and r.a=r.b and s.c=s.d) order by r.a,s.c;
select r.a, s.c from orca.r left outer join orca.s on(r.a=s.c) where s.d > 2 or s.d is null order by r.a;
select r.a, s.c from orca.r right outer join orca.s on(r.a=s.c);
select * from orca.r where exists (select * from orca.s where s.c=r.a + 2);
select * from orca.r where exists (select * from orca.s where s.c=r.b);
select * from orca.m where m.a not in (select a from orca.m1 where a=5);
select * from orca.m where m.a not in (select a from orca.m1);
select * from orca.m where m.a in (select a from orca.m1 where m1.a-1 = m.b);

-- enable_hashjoin=off; enable_mergejoin=on
select 1 from orca.m, orca.m1 where m.a = m1.a and m.b!=m1.b;

-- plan.qual vs hashclauses/join quals:
select * from orca.r left outer join orca.s on (r.a=s.c and r.b<s.d) where s.d is null;
-- select * from orca.r m full outer join orca.r m1 on (m.a=m1.a) where m.a is null;

-- explain Hash Join with 'IS NOT DISTINCT FROM' join condition
-- force_explain
explain  select * from orca.r, orca.s where r.a is not distinct from s.c;

-- explain Hash Join with equality join condition
-- force_explain
explain select * from orca.r, orca.s where r.a = s.c;

-- sort
select * from orca.r join orca.s on(r.a=s.c) order by r.a, s.d;
select * from orca.r join orca.s on(r.a=s.c) order by r.a, s.d limit 10;
select * from orca.r join orca.s on(r.a=s.c) order by r.a + 5, s.d limit 10;

-- group by
select 1 from orca.m group by a+b;

-- join with const table
select * from orca.r where a = (select 1);

-- union with const table
select * from ((select a as x from orca.r) union (select 1 as x )) as foo order by x;

----------------------------------------------------------------------
set optimizer=off;

insert into orca.m values (1,-1), (1,2), (1,1);

set optimizer=on;
----------------------------------------------------------------------

-- computed columns
select a,a,a+b from orca.m;
select a,a+b,a+b from orca.m;

-- func expr
select * from orca.m where a=abs(b);

-- grouping sets

select a,b,count(*) from orca.m group by grouping sets ((a), (a,b));
select b,count(*) from orca.m group by grouping sets ((a), (a,b));
select a,count(*) from orca.m group by grouping sets ((a), (a,b));
select a,count(*) from orca.m group by grouping sets ((a), (b));
select a,b,count(*) from orca.m group by rollup(a, b);
select a,b,count(*) from orca.m group by rollup((a),(a,b)) order by 1,2,3;
select count(*) from orca.m group by ();
select a, count(*) from orca.r group by (), a;
select a, count(*) from orca.r group by grouping sets ((),(a));

select a, b, count(*) c from orca.r group by grouping sets ((),(a), (a,b)) order by b,a,c;
select a, count(*) c from orca.r group by grouping sets ((),(a), (a,b)) order by b,a,c;

select 1 from orca.r group by ();
select a,1 from orca.r group by rollup(a);

-- arrays
select array[array[a,b]], array[b] from orca.r;

-- setops
select a, b from m union select b,a from orca.m;
SELECT a from m UNION ALL select b from orca.m UNION ALL select a+b from orca.m group by 1;

----------------------------------------------------------------------
set optimizer=off;

drop table if exists orca.foo;
create table orca.foo(a int, b int, c int, d int);

drop table if exists orca.bar;
create table orca.bar(a int, b int, c int);

insert into orca.foo select i, i%2, i%4, i-1 from generate_series(1,40)i;
insert into orca.bar select i, i%3, i%2 from generate_series(1,30)i;

set optimizer=on;
----------------------------------------------------------------------

--- distinct operation
SELECT distinct a, b from orca.foo;
SELECT distinct foo.a, bar.b from orca.foo, orca.bar where foo.b = bar.a;
SELECT distinct a, b from orca.foo;
SELECT distinct a, count(*) from orca.foo group by a;
SELECT distinct foo.a, bar.b, sum(bar.c+foo.c) from orca.foo, orca.bar where foo.b = bar.a group by foo.a, bar.b;
SELECT distinct a, count(*) from orca.foo group by a;
SELECT distinct foo.a, bar.b from orca.foo, orca.bar where foo.b = bar.a;
SELECT distinct foo.a, bar.b, sum(bar.c+foo.c) from orca.foo, orca.bar where foo.b = bar.a group by foo.a, bar.b;

SELECT distinct a, b from orca.foo;
SELECT distinct a, count(*) from orca.foo group by a;
SELECT distinct foo.a, bar.b from orca.foo, orca.bar where foo.b = bar.a;
SELECT distinct foo.a, bar.b, sum(bar.c+foo.c) from orca.foo, orca.bar where foo.b = bar.a group by foo.a, bar.b;

--- window operations
select row_number() over() from orca.foo order by 1;
select rank() over(partition by b order by count(*)/sum(a)) from orca.foo group by a, b order by 1;
select row_number() over(order by foo.a) from orca.foo inner join orca.bar using(b) group by foo.a, bar.b, bar.a;
select 1+row_number() over(order by foo.a+bar.a) from orca.foo inner join orca.bar using(b);
select row_number() over(order by foo.a+ bar.a)/count(*) from orca.foo inner join orca.bar using(b) group by foo.a, bar.a, bar.b;
select count(*) over(partition by b order by a range between 1 preceding and (select count(*) from orca.bar) following) from orca.foo;
select a+1, rank() over(partition by b+1 order by a+1) from orca.foo order by 1, 2;
select a , sum(a) over (order by a range '1'::float8 preceding) from orca.r order by 1,2;
select a, b, floor(avg(b) over(order by a desc, b desc rows between unbounded preceding and unbounded following)) as avg, dense_rank() over (order by a) from orca.r order by 1,2,3,4;
select lead(a) over(order by a) from orca.r order by 1;
select lag(c,d) over(order by c,d) from orca.s order by 1;
select lead(c,c+d,1000) over(order by c,d) from orca.s order by 1;

--- cte 
with x as (select a, b from orca.r)
select rank() over(partition by a, case when b = 0 then a+b end order by b asc) as rank_within_parent from x order by a desc ,case when a+b = 0 then a end ,b;

-- alias
select foo.d from orca.foo full join orca.bar on (foo.d = bar.a) group by d;
select 1 as v from orca.foo full join orca.bar on (foo.d = bar.a) group by d;
select * from orca.r where a in (select count(*)+1 as v from orca.foo full join orca.bar on (foo.d = bar.a) group by d+r.b);
select * from orca.r where r.a in (select d+r.b+1 as v from orca.foo full join orca.bar on (foo.d = bar.a) group by d+r.b) order by r.a, r.b;

----------------------------------------------------------------------
set optimizer=off;

drop table if exists orca.rcte;
create table orca.rcte(a int, b int, c int);

insert into orca.rcte select i, i%2, i%3 from generate_series(1,40)i;

-- select disable_xform('CXformInlineCTEConsumer');
set optimizer=on;
----------------------------------------------------------------------

with x as (select * from orca.rcte where a < 10) select * from x x1, x x2;
with x as (select * from orca.rcte where a < 10) select * from x x1, x x2 where x2.a = x1.b;
with x as (select * from orca.rcte where a < 10) select a from x union all select b from x;
with x as (select * from orca.rcte where a < 10) select * from x x1 where x1.b = any (select x2.a from x x2 group by x2.a);
with x as (select * from orca.rcte where a < 10) select * from x x1 where x1.b = all (select x2.a from x x2 group by x2.a);
with x as (select * from orca.rcte where a < 10) select * from x x1, x x2, x x3 where x2.a = x1.b and x3.b = x2.b                                                                                   ;
with x as (select * from orca.rcte where a < 10) select * from x x2 where x2.b < (select avg(b) from x x1);
with x as (select r.a from orca.r, orca.s  where r.a < 10 and s.d < 10 and r.a = s.d) select * from x x1, x x2;
with x as (select r.a from orca.r, orca.s  where r.a < 10 and s.c < 10 and r.a = s.c) select * from x x1, x x2;
with x as (select * from orca.rcte where a < 10) (select a from x x2) union all (select max(a) from x x1);
with x as (select * from orca.r) select * from x order by a;
-- with x as (select * from orca.rcte where a < 10) select * from x x1, x x2 where x2.a = x1.b limit 1;

----------------------------------------------------------------------
-- correlated execution
select (select 1 union select 2);
select (select generate_series(1,5));
select (select a from orca.foo inner1 where inner1.a=outer1.a  union select b from orca.foo inner2 where inner2.b=outer1.b) from orca.foo outer1;
select (select generate_series(1,1)) as series;
select generate_series(1,5);
select a, c from orca.r, orca.s where a = any (select c) order by a, c limit 10;
select a, c from orca.r, orca.s where a = (select c) order by a, c limit 10;
select a, c from orca.r, orca.s where a  not in  (select c) order by a, c limit 10;
select a, c from orca.r, orca.s where a  = any  (select c from orca.r) order by a, c limit 10;
select a, c from orca.r, orca.s where a  <> all (select c) order by a, c limit 10;
select a, (select (select (select c from orca.s where a=c group by c))) as subq from orca.r order by a;
with v as (select a,b from orca.r, orca.s where a=c)  select c from orca.s group by c having count(*) not in (select b from v where a=c) order by c;
----------------------------------------------------------------------
set optimizer=off;

CREATE TABLE orca.onek (
unique1 int4,
unique2 int4,
two int4,
four int4,
ten int4,
twenty int4,
hundred int4,
thousand int4,
twothousand int4,
fivethous int4,
tenthous int4,
odd int4,
even int4,
stringu1 name,
stringu2 name,
string4 name
);

insert into orca.onek values (931,1,1,3,1,11,1,31,131,431,931,2,3,'VJAAAA','BAAAAA','HHHHxx');
insert into orca.onek values (714,2,0,2,4,14,4,14,114,214,714,8,9,'MBAAAA','CAAAAA','OOOOxx');
insert into orca.onek values (711,3,1,3,1,11,1,11,111,211,711,2,3,'JBAAAA','DAAAAA','VVVVxx');
insert into orca.onek values (883,4,1,3,3,3,3,83,83,383,883,6,7,'ZHAAAA','EAAAAA','AAAAxx');
insert into orca.onek values (439,5,1,3,9,19,9,39,39,439,439,18,19,'XQAAAA','FAAAAA','HHHHxx');
insert into orca.onek values (670,6,0,2,0,10,0,70,70,170,670,0,1,'UZAAAA','GAAAAA','OOOOxx');
insert into orca.onek values (543,7,1,3,3,3,3,43,143,43,543,6,7,'XUAAAA','HAAAAA','VVVVxx');

set optimizer=on;
----------------------------------------------------------------------

select ten, sum(distinct four) from orca.onek a
group by ten 
having exists (select 1 from orca.onek b where sum(distinct a.four) = b.four);

-- list partition tests 

-- test homogeneous partitions 
drop table if exists orca.t;

create table orca.t ( a int, b char(2), to_be_drop int, c int, d char(2), e int)
distributed by (a) 
partition by list(d) (partition part1 values('a'), partition part2 values('b'));

insert into orca.t 
	select i, i::char(2), i, i, case when i%2 = 0 then 'a' else 'b' end, i 
	from generate_series(1,100) i;

select * from orca.t order by 1, 2, 3, 4, 5, 6 limit 4;

alter table orca.t drop column to_be_drop;

select * from orca.t order by 1, 2, 3, 4, 5 limit 4;

insert into orca.t (d, a) values('a', 0);
insert into orca.t (a, d) values(0, 'b');

select * from orca.t order by 1, 2, 3, 4, 5 limit 4;

create table orca.multilevel_p (a int, b int)
partition by range(a)
subpartition by range(a)
subpartition template
(
subpartition sp1 start(0) end(10) every (5),
subpartition sp2 start(10) end(200) every(50)
)
(partition aa start(0) end (100) every (50),
partition bb start(100) end(200) every (50) );

insert into orca.multilevel_p values (1,1), (100,200);
select * from orca.multilevel_p;

-- test project elements in TVF

CREATE FUNCTION orca.csq_f(a int) RETURNS int AS $$ select $1 $$ LANGUAGE SQL;

CREATE TABLE orca.csq_r(a int);

INSERT INTO orca.csq_r VALUES (1);

SELECT * FROM orca.csq_r WHERE a IN (SELECT * FROM orca.csq_f(orca.csq_r.a));

-- test algebrization of having clause
drop table if exists orca.tab1;
create table orca.tab1(a int, b int, c int, d int, e int);
insert into orca.tab1 values (1,2,3,4,5);
insert into orca.tab1 values (1,2,3,4,5);
insert into orca.tab1 values (1,2,3,4,5);
select b,d from orca.tab1 group by b,d having min(distinct d)>3;
select b,d from orca.tab1 group by b,d having d>3;
select b,d from orca.tab1 group by b,d having min(distinct d)>b;

create table orca.fooh1 (a int, b int, c int);
create table orca.fooh2 (a int, b int, c int);
insert into orca.fooh1 select i%4, i%3, i from generate_series(1,20) i;
insert into orca.fooh2 select i%3, i%2, i from generate_series(1,20) i;

select sum(f1.b) from orca.fooh1 f1 group by f1.a;
select 1 as one, f1.a from orca.fooh1 f1 group by f1.a having sum(f1.b) > 4;
select f1.a, 1 as one from orca.fooh1 f1 group by f1.a having 10 > (select f2.a from orca.fooh2 f2 group by f2.a having sum(f1.a) > count(*) order by f2.a limit 1) order by f1.a;
select 1 from orca.fooh1 f1 group by f1.a having 10 > (select f2.a from orca.fooh2 f2 group by f2.a having sum(f1.a) > count(*) order by f2.a limit 1) order by f1.a;
select f1.a, 1 as one from orca.fooh1 f1 group by f1.a having 10 > (select 1 from orca.fooh2 f2 group by f2.a having sum(f1.b) > count(*) order by f2.a limit 1) order by f1.a;
select 1 from orca.fooh1 f1 group by f1.a having 10 > (select 1 from orca.fooh2 f2 group by f2.a having sum(f1.b) > count(*) order by f2.a limit 1) order by f1.a;

select f1.a, 1 as one from orca.fooh1 f1 group by f1.a having 0 = (select f2.a from orca.fooh2 f2 group by f2.a having sum(f2.b) > 1 order by f2.a limit 1) order by f1.a;
select 1 as one from orca.fooh1 f1 group by f1.a having 0 = (select f2.a from orca.fooh2 f2 group by f2.a having sum(f2.b) > 1 order by f2.a limit 1) order by f1.a;
select f1.a, 1 as one from orca.fooh1 f1 group by f1.a having 0 = (select f2.a from orca.fooh2 f2 group by f2.a having sum(f2.b) > 1 order by f2.a limit 1) order by f1.a;
select f1.a, 1 as one from orca.fooh1 f1 group by f1.a having 0 = (select f2.a from orca.fooh2 f2 group by f2.a having sum(f2.b + f1.a) > 1 order by f2.a limit 1) order by f1.a;

select f1.a, 1 as one from orca.fooh1 f1 group by f1.a having 0 = (select f2.a from orca.fooh2 f2 group by f2.a having sum(f2.b + sum(f1.b)) > 1 order by f2.a limit 1) order by f1.a;
select f1.a, 1 as one from orca.fooh1 f1 group by f1.a having f1.a < (select f2.a from orca.fooh2 f2 group by f2.a having sum(f2.b + 1) > f1.a order by f2.a desc limit 1) order by f1.a;
select f1.a, 1 as one from orca.fooh1 f1 group by f1.a having f1.a = (select f2.a from orca.fooh2 f2 group by f2.a having sum(f2.b) + 1 > f1.a order by f2.a desc limit 1);
select f1.a, 1 as one from orca.fooh1 f1 group by f1.a having f1.a = (select f2.a from orca.fooh2 f2 group by f2.a having sum(f2.b) > 1 order by f2.a limit 1);
select sum(f1.a+1)+1 from orca.fooh1 f1 group by f1.a+1;
select sum(f1.a+1)+sum(f1.a+1) from orca.fooh1 f1 group by f1.a+1;
select sum(f1.a+1)+avg(f1.a+1), sum(f1.a), sum(f1.a+1) from orca.fooh1 f1 group by f1.a+1;

create table orca.t77(C952 text) WITH (compresstype=zlib,compresslevel=2,appendonly=true,blocksize=393216,checksum=true);
insert into orca.t77 select 'text'::text;
insert into orca.t77 select 'mine'::text;
insert into orca.t77 select 'apple'::text;
insert into orca.t77 select 'orange'::text;

SELECT to_char(AVG( char_length(DT466.C952) ), '9999999.9999999'), MAX( char_length(DT466.C952) ) FROM orca.t77 DT466 GROUP BY char_length(DT466.C952);

create table orca.prod9 (sale integer, prodnm varchar,price integer);
insert into orca.prod9 values (100, 'shirts', 500);
insert into orca.prod9 values (200, 'pants',800);
insert into orca.prod9 values (300, 't-shirts', 300);

-- returning product and price using Having and Group by clause

select prodnm, price from orca.prod9 GROUP BY prodnm, price HAVING price !=300;

-- analyze on tables with dropped attributes
create table orca.toanalyze(a int, b int);
insert into orca.toanalyze values (1,1), (2,2), (3,3);
alter table orca.toanalyze drop column a;
analyze orca.toanalyze;

-- union 

create table orca.ur (a int, b int);
create table orca.us (c int, d int);
create table orca.ut(a int);
create table orca.uu(c int, d bigint);
insert into orca.ur values (1,1);
insert into orca.ur values (1,2);
insert into orca.ur values (2,1);
insert into orca.us values (1,3);
insert into orca.ut values (3);
insert into orca.uu values (1,3);

select * from (select a, a from orca.ur union select c, d from orca.us) x(g,h);
select * from (select a, a from orca.ur union select c, d from orca.us) x(g,h), orca.ut t where t.a = x.h;
select * from (select a, a from orca.ur union select c, d from orca.uu) x(g,h), orca.ut t where t.a = x.h;
select 1 AS two UNION select 2.2;
select 2.2 AS two UNION select 1;
select * from (select 2.2 AS two UNION select 1) x(a), (select 1.0 AS two UNION ALL select 1) y(a) where y.a = x.a;

-- window functions inside inline CTE

CREATE TABLE orca.twf1 AS SELECT i as a, i+1 as b from generate_series(1,10)i;
CREATE TABLE orca.twf2 AS SELECT i as c, i+1 as d from generate_series(1,10)i;

SET optimizer_cte_inlining_bound=1000;
SET optimizer_cte_inlining = on;

WITH CTE(a,b) AS
(SELECT a,d FROM orca.twf1, orca.twf2 WHERE a = d),
CTE1(e,f) AS
( SELECT f1.a, rank() OVER (PARTITION BY f1.b ORDER BY CTE.a) FROM orca.twf1 f1, CTE )
SELECT * FROM CTE1,CTE WHERE CTE.a = CTE1.f and CTE.a = 2 ORDER BY 1;

SET optimizer_cte_inlining = off;

-- catalog queries
select 1 from pg_class c group by c.oid limit 1;

-- CSQs
drop table if exists orca.tab1;
drop table if exists orca.tab2;
create table orca.tab1 (i, j) as select i,i%2 from generate_series(1,10) i;
create table orca.tab2 (a, b) as select 1, 2;
select * from orca.tab1 where 0 < (select count(*) from generate_series(1,i)) order by 1;
select * from orca.tab1 where i > (select b from orca.tab2);

-- subqueries
select NULL in (select 1);
select 1 in (select 1);
select 1 in (select 2);
select NULL in (select 1/0);
select 1 where 22 in (SELECT unnest(array[1,2]));
select 1 where 22 not in (SELECT unnest(array[1,2]));
select 1 where 22 in (SELECT generate_series(1,10));
select 1 where 22 not in (SELECT generate_series(1,10));

-- UDAs
CREATE FUNCTION sum_sfunc(anyelement,anyelement) returns anyelement AS 'select $1+$2' LANGUAGE SQL STRICT;
CREATE FUNCTION sum_prefunc(anyelement,anyelement) returns anyelement AS 'select $1+$2' LANGUAGE SQL STRICT;
CREATE AGGREGATE myagg1(anyelement) (SFUNC = sum_sfunc, PREFUNC = sum_prefunc, STYPE = anyelement, INITCOND = '0');
SELECT myagg1(i) FROM orca.tab1;

CREATE FUNCTION sum_sfunc2(anyelement,anyelement,anyelement) returns anyelement AS 'select $1+$2+$3' LANGUAGE SQL STRICT;
CREATE AGGREGATE myagg2(anyelement,anyelement) (SFUNC = sum_sfunc2, STYPE = anyelement, INITCOND = '0');
SELECT myagg2(i,j) FROM orca.tab1;

CREATE OR REPLACE FUNCTION tfp(anyarray,anyelement) RETURNS anyarray AS 'select $1 || $2' LANGUAGE SQL;
CREATE OR REPLACE FUNCTION ffp(anyarray) RETURNS anyarray AS 'select $1' LANGUAGE SQL;
CREATE AGGREGATE myagg3(BASETYPE = anyelement, SFUNC = tfp, STYPE = anyarray, FINALFUNC = ffp, INITCOND = '{}');
CREATE TABLE array_table(f1 int, f2 int[], f3 text);
INSERT INTO array_table values(1,array[1],'a');
INSERT INTO array_table values(2,array[11],'b');
INSERT INTO array_table values(3,array[111],'c');
INSERT INTO array_table values(4,array[2],'a');
INSERT INTO array_table values(5,array[22],'b');
INSERT INTO array_table values(6,array[222],'c');
INSERT INTO array_table values(7,array[3],'a');
INSERT INTO array_table values(8,array[3],'b');
SELECT f3, myagg3(f1) from (select * from array_table order by f1 limit 10) as foo GROUP BY f3 ORDER BY f3;

-- Functions that are wrongly annotated in the catalog as NOSQL
CREATE TABLE orca.test_part (i int) PARTITION BY RANGE(i) (start(1) exclusive end(2) inclusive);
CREATE TABLE orca.test_distributed(i int);
INSERT INTO orca.test_distributed SELECT i from generate_series(1,2) i;

-- start_ignore
-- wrong results, enable after fixing GPSQL-3035
-- Function pg_get_partition_rule_def(oid, bool)
SELECT pg_get_partition_rule_def(pr1.oid, true) AS partitionboundary ,n.nspname AS schemaname, cl.relname AS tablename 
FROM orca.test_distributed, pg_namespace n, pg_namespace n2, pg_class cl
LEFT JOIN pg_tablespace sp ON cl.reltablespace = sp.oid, pg_class cl2
LEFT JOIN pg_tablespace sp3 ON cl2.reltablespace = sp3.oid, pg_partition pp, pg_partition_rule pr1
WHERE pp.paristemplate = false AND pp.parrelid = cl.oid AND pr1.paroid = pp.oid AND cl2.oid = pr1.parchildrelid
AND cl.relnamespace = n.oid AND cl2.relnamespace = n2.oid and cl.relname ='test_part';

-- Function pg_get_partition_rule_def(oid)
SELECT pg_get_partition_rule_def(pr1.oid) AS partitionboundary ,n.nspname AS schemaname, cl.relname AS tablename 
FROM orca.test_distributed, pg_namespace n, pg_namespace n2, pg_class cl
LEFT JOIN pg_tablespace sp ON cl.reltablespace = sp.oid, pg_class cl2
LEFT JOIN pg_tablespace sp3 ON cl2.reltablespace = sp3.oid, pg_partition pp, pg_partition_rule pr1
WHERE pp.paristemplate = false AND pp.parrelid = cl.oid AND pr1.paroid = pp.oid AND cl2.oid = pr1.parchildrelid
AND cl.relnamespace = n.oid AND cl2.relnamespace = n2.oid and cl.relname ='test_part';
-- end_ignore

-- MPP-22791: SIGSEGV when querying a table with default partition only
create table mpp22791(a int, b int) partition by range(b) (default partition d);
insert into mpp22791 values (1, 1), (2, 2), (3, 3);
select * from mpp22791 where b > 1; 
select * from mpp22791 where b <= 3;

-- MPP-20713, MPP-20714, MPP-20738: Const table get with a filter
select 1 as x where 1 in (2, 3);

-- MPP-23081: keys of partitioned tables
create table orca.p1(a int) partition by range(a)(partition pp1 start(1) end(10), partition pp2 start(10) end(20));
insert into orca.p1 select * from generate_series(2,15);
select count(*) from (select gp_segment_id,ctid,tableoid from orca.p1 group by gp_segment_id,ctid,tableoid) as foo;

--- MPP-25194: histograms on text columns are dropped in ORCA. NDVs of these histograms should be added to NDVRemain
CREATE TABLE orca.tmp_verd_s_pp_provtabs_agt_0015_extract1 (
    uid136 character(16),
    tab_nr smallint,
    prdgrp character(5),
    bg smallint,
    typ142 smallint,
    ad_gsch smallint,
    guelt_ab date,
    guelt_bis date,
    guelt_stat text,
    verarb_ab timestamp(5) without time zone,
    u_erzeugen integer,
    grenze integer,
    erstellt_am_122 timestamp(5) without time zone,
    erstellt_am_135 timestamp(5) without time zone,
    erstellt_am_134 timestamp(5) without time zone
)
WITH (appendonly=true, compresstype=zlib) DISTRIBUTED BY (uid136);

 set allow_system_table_mods="DML";

 UPDATE pg_class                                                                                                                                                                                     
 SET                                                                                                                                                                                                 
         relpages = 30915::int, reltuples = 7.28661e+07::real WHERE relname = 'tmp_verd_s_pp_provtabs_agt_0015_extract1' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'xvclin');

 insert into pg_statistic values ('orca.tmp_verd_s_pp_provtabs_agt_0015_extract1'::regclass,1::smallint,0::real,17::integer,264682::real,1::smallint,2::smallint,0::smallint,0::smallint,1054::oid,1058::oid,0::oid,0::oid,'{0.000161451,0.000107634,0.000107634,0.000107634,0.000107634,0.000107634,0.000107634,0.000107634,0.000107634,0.000107634,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05}'::real[],NULL::real[],NULL::real[],NULL::real[],'{8X8#1F8V92A2025G,YFAXQ§UBF210PA0P,2IIVIE8V92A2025G,9§BP8F8V92A2025G,35A9EE8V92A2025G,§AJ2Z§9MA210PA0P,3NUQ3E8V92A2025G,F7ZD4F8V92A2025G,$WHHEO§§E210PA0P,Z6EATH2BE210PA0P,N7I28E8V92A2025G,YU0K$E$§9210PA0P,3TAI1ANIF210PA0P,P#H8BF8V92A2025G,VTQ$N$D§92A201SC,N7ZD4F8V92A2025G,77BP8F8V92A2025G,39XOXY78H210PA01,#2#OX6NHH210PA01,2DG1J#XZH210PA01,MFEG$E8V92A2025G,M0HKWNGND210PA0P,FSXI67NSA210PA0P,C1L77E8V92A2025G,01#21E8V92A2025G}'::bpchar[],'{§00§1HKZC210PA0P,1D90GE8V92A2025G,2ULZI6L0O210PA01,489G7L8$I210PA01,5RE8FF8V92A2025G,76NIRFNIF210PA0P,8KOMKE8V92A2025G,#9Y#GPSHB210PA0P,BDAJ#D8V92A2025G,CV9Z7IYVK210PA01,#EC5FE8V92A2025G,FQWY§O1XC210PA0P,H8HL4E8V92A2025G,INC5FE8V92A2025G,K4MX0XHCF210PA0P,LKE8FF8V92A2025G,N03G9UM2F210PA0P,OHJ$#GFZ9210PA0P,PXU3T1OTB210PA0P,RCUA45F1H210PA01,SU§FRY#QI210PA01,UABHMLSLK210PA01,VRBP8F8V92A2025G,X65#KZIDC210PA0P,YLFG§#A2G210PA0P,ZZG8H29OC210PA0P,ZZZDBCEVA210PA0P}'::bpchar[],NULL::bpchar[],NULL::bpchar[]);
 insert into pg_statistic values ('orca.tmp_verd_s_pp_provtabs_agt_0015_extract1'::regclass,2::smallint,0::real,2::integer,205.116::real,1::smallint,2::smallint,0::smallint,0::smallint,94::oid,95::oid,0::oid,0::oid,'{0.278637,0.272448,0.0303797,0.0301106,0.0249442,0.0234373,0.0231682,0.0191319,0.0169793,0.0162527,0.0142884,0.0141539,0.0125394,0.0103329,0.0098216,0.00944488,0.00850308,0.00715766,0.0066464,0.00656567,0.00591987,0.0050588,0.00454753,0.00449372,0.0044399}'::real[],NULL::real[],NULL::real[],NULL::real[],'{199,12,14,5,197,11,198,8,152,299,201,153,9,13,74,179,24,202,2,213,17,195,215,16,200}'::int2[],'{1,5,9,12,14,24,58,80,152,195,198,199,207,302,402}'::int2[],NULL::int2[],NULL::int2[]);
 insert into pg_statistic values ('orca.tmp_verd_s_pp_provtabs_agt_0015_extract1'::regclass,3::smallint,0::real,6::integer,201.005::real,1::smallint,2::smallint,0::smallint,0::smallint,1054::oid,1058::oid,0::oid,0::oid,'{0.0478164,0.0439146,0.0406856,0.0239755,0.0154186,0.0149073,0.0148804,0.0143422,0.0141808,0.0139386,0.0138848,0.0138848,0.0137502,0.0134812,0.0134004,0.0133197,0.0133197,0.013239,0.0131852,0.0130775,0.0130775,0.0130237,0.0129699,0.0129699,0.012943}'::real[],NULL::real[],NULL::real[],NULL::real[],'{AG023,AG032,AG999,AG022,AG--B,AG-VB,AG--C,AG-VC,AG014,AG-VA,AG036,AGT4C,AG--A,AG037,AG009,AG015,AG003,AG002,AGT3C,AG025,AG019,AGT2C,AGT1C,AG005,AG031}'::bpchar[],'{AG001,AG004,AG007,AG010,AG013,AG017,AG020,AG022,AG023,AG026,AG030,AG032,AG034,AG037,AG040,AG045,AG122,AG999,AG--B,AGT1C,AGT4C,AG-VB,MA017,MA--A,MK081,MKRKV}'::bpchar[],NULL::bpchar[],NULL::bpchar[]);
 insert into pg_statistic values ('orca.tmp_verd_s_pp_provtabs_agt_0015_extract1'::regclass,4::smallint,0::real,2::integer,34::real,1::smallint,2::smallint,0::smallint,0::smallint,94::oid,95::oid,0::oid,0::oid,'{0.1135,0.112881,0.111778,0.100288,0.0895245,0.0851384,0.0821785,0.0723569,0.0565616,0.0368646,0.0309986,0.0160375,0.00621586,0.00594677,0.0050588,0.00489734,0.00487044,0.00487044,0.00468208,0.00462826,0.00460135,0.00441299,0.00438608,0.00417081,0.00414391}'::real[],NULL::real[],NULL::real[],NULL::real[],'{1,3,2,7,9,4,5,16,10,6,8,77,12,11,14,13,22,23,64,61,24,51,53,15,54}'::int2[],'{1,2,3,4,5,6,7,9,10,14,16,17,53,98}'::int2[],NULL::int2[],NULL::int2[]);
 insert into pg_statistic values ('orca.tmp_verd_s_pp_provtabs_agt_0015_extract1'::regclass,5::smallint,0::real,2::integer,1::real,1::smallint,2::smallint,0::smallint,0::smallint,94::oid,95::oid,0::oid,0::oid,'{1}'::real[],NULL::real[],NULL::real[],NULL::real[],'{1}'::int2[],'{1}'::int2[],NULL::int2[],NULL::int2[]);
 insert into pg_statistic values ('orca.tmp_verd_s_pp_provtabs_agt_0015_extract1'::regclass,6::smallint,0::real,2::integer,2::real,1::smallint,2::smallint,0::smallint,0::smallint,94::oid,95::oid,0::oid,0::oid,'{0.850227,0.149773}'::real[],NULL::real[],NULL::real[],NULL::real[],'{1,2}'::int2[],'{1,2}'::int2[],NULL::int2[],NULL::int2[]);
 insert into pg_statistic values ('orca.tmp_verd_s_pp_provtabs_agt_0015_extract1'::regclass,7::smallint,0::real,4::integer,591.134::real,1::smallint,2::smallint,0::smallint,0::smallint,1093::oid,1095::oid,0::oid,0::oid,'{0.26042,0.0859995,0.0709308,0.0616473,0.0567231,0.0303797,0.0109787,0.0106289,0.00990232,0.00987541,0.00979469,0.00944488,0.00820709,0.00718457,0.00626968,0.00621586,0.00616204,0.00600059,0.00586605,0.00557006,0.00516643,0.00511261,0.0050857,0.0050857,0.0047628}'::real[],NULL::real[],NULL::real[],NULL::real[],'{1994-01-01,1997-01-01,2005-07-01,1999-01-01,2003-09-01,2000-01-01,2001-01-01,1998-10-01,2001-05-01,1999-03-01,2013-01-01,2003-01-01,2008-01-01,2004-01-01,2009-01-01,2003-02-01,1998-09-01,2000-12-01,2007-04-01,1998-08-01,1998-01-01,2003-07-01,1998-11-01,2005-02-01,1999-04-01}'::date[],'{1900-01-01,1994-01-01,1997-01-01,1998-05-07,1999-01-01,1999-05-01,2000-01-01,2001-03-22,2002-10-01,2003-09-01,2004-08-01,2005-07-01,2007-01-01,2008-06-01,2010-04-01,2012-07-01,2014-12-01,2015-01-01}'::date[],NULL::date[],NULL::date[]);
 insert into pg_statistic values ('orca.tmp_verd_s_pp_provtabs_agt_0015_extract1'::regclass,8::smallint,0::real,4::integer,474.232::real,1::smallint,2::smallint,0::smallint,0::smallint,1093::oid,1095::oid,0::oid,0::oid,'{0.52111,0.0709308,0.0591987,0.0587412,0.0148804,0.010279,0.00990232,0.00931034,0.00791109,0.00718457,0.00688857,0.00608132,0.00557006,0.0053817,0.00503189,0.00500498,0.00457444,0.004117,0.00395555,0.00390173,0.00357883,0.00352501,0.00352501,0.00344429,0.00333665}'::real[],NULL::real[],NULL::real[],NULL::real[],'{9999-12-31,2005-07-01,1999-01-01,2003-09-01,2004-02-01,2001-05-01,2005-02-01,1999-03-01,1998-10-01,2000-01-01,2003-01-01,1998-09-01,1998-08-01,2008-01-01,2007-04-01,2000-12-01,1999-04-01,1998-11-01,2003-02-01,1994-01-01,2002-09-01,1999-02-01,2004-01-01,1998-07-01,2003-07-01}'::date[],'{1994-01-01,1998-11-01,1999-01-01,1999-04-01,2001-05-01,2003-07-01,2003-09-01,2004-02-01,2005-07-01,2007-02-01,2010-03-01,9999-12-31}'::date[],NULL::date[],NULL::date[]);
 insert into pg_statistic values ('orca.tmp_verd_s_pp_provtabs_agt_0015_extract1'::regclass,9::smallint,0::real,2::integer,1::real,1::smallint,2::smallint,0::smallint,0::smallint,98::oid,664::oid,0::oid,0::oid,'{1}'::real[],NULL::real[],NULL::real[],NULL::real[],'{H}'::text[],'{H}'::text[],NULL::text[],NULL::text[]);
 insert into pg_statistic values ('orca.tmp_verd_s_pp_provtabs_agt_0015_extract1'::regclass,10::smallint,0::real,8::integer,1::real,1::smallint,2::smallint,0::smallint,0::smallint,2060::oid,2062::oid,0::oid,0::oid,'{1}'::real[],NULL::real[],NULL::real[],NULL::real[],'{1900-01-01 00:00:00}'::timestamp[],'{1900-01-01 00:00:00}'::timestamp[],NULL::timestamp[],NULL::timestamp[]);
 insert into pg_statistic values ('orca.tmp_verd_s_pp_provtabs_agt_0015_extract1'::regclass,11::smallint,1::real,0::integer,-1::real,0::smallint,0::smallint,0::smallint,0::smallint,0::oid,0::oid,0::oid,0::oid,NULL::real[],NULL::real[],NULL::real[],NULL::real[],NULL::int4[],NULL::int4[],NULL::int4[],NULL::int4[]);
 insert into pg_statistic values ('orca.tmp_verd_s_pp_provtabs_agt_0015_extract1'::regclass,12::smallint,0::real,4::integer,1::real,1::smallint,2::smallint,0::smallint,0::smallint,96::oid,97::oid,0::oid,0::oid,'{1}'::real[],NULL::real[],NULL::real[],NULL::real[],'{1}'::int4[],'{1}'::int4[],NULL::int4[],NULL::int4[]);
 insert into pg_statistic values ('orca.tmp_verd_s_pp_provtabs_agt_0015_extract1'::regclass,13::smallint,0.991927::real,8::integer,301.416::real,1::smallint,2::smallint,0::smallint,0::smallint,2060::oid,2062::oid,0::oid,0::oid,'{8.07255e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05,2.69085e-05}'::real[],NULL::real[],NULL::real[],NULL::real[],'{2004-01-05 13:45:06.18894,2007-03-20 12:02:33.73888,2009-12-15 12:33:55.32684,2012-09-21 09:58:09.70321,2012-02-13 14:56:03.11625,2000-10-02 09:56:01.24836,1998-01-14 10:11:29.43055,2014-01-08 14:41:44.85935,2012-06-21 12:46:37.48899,2013-07-23 14:27:52.27322,2011-10-27 16:17:10.01694,2005-07-13 16:55:30.7964,2003-06-05 14:53:13.71932,2002-07-22 10:22:31.0967,2011-12-27 16:12:54.85765,2001-01-12 11:40:09.16207,2005-12-30 08:46:31.30943,2007-03-01 08:29:36.765,2011-06-16 09:09:43.8651,2000-12-15 14:33:29.20083,2006-04-25 13:46:46.09684,2011-06-20 16:26:23.65135,2004-01-23 12:37:06.92535,2002-03-04 10:02:08.92547,2003-08-01 10:33:57.33683}'::timestamp[],'{1997-12-05 10:59:43.94611,2014-11-18 08:48:18.32773}'::timestamp[],NULL::timestamp[],NULL::timestamp[]);
 insert into pg_statistic values ('orca.tmp_verd_s_pp_provtabs_agt_0015_extract1'::regclass,14::smallint,0.329817::real,8::integer,38109.5::real,1::smallint,2::smallint,0::smallint,0::smallint,2060::oid,2062::oid,0::oid,0::oid,'{0.0715766,0.0621317,0.00546242,0.0044399,0.000134542,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05,8.07255e-05}'::real[],NULL::real[],NULL::real[],NULL::real[],'{1997-11-25 19:05:00.83798,1998-12-29 16:19:18.50226,1998-01-28 23:01:18.41289,2000-12-21 18:03:30.34549,2003-08-28 11:14:33.26306,2003-08-28 11:14:33.2622,1999-03-20 13:04:33.24015,2003-08-28 11:14:33.22312,2003-08-28 11:14:33.21933,2003-08-28 11:14:33.22082,2003-08-28 11:14:33.21425,2003-08-28 11:14:33.22336,2003-08-28 11:14:33.2092,2003-08-28 11:14:33.20251,2003-08-28 11:14:33.22145,2003-08-28 11:14:33.26235,2003-08-28 11:14:33.26525,2003-08-28 11:14:33.23714,2003-08-28 11:14:33.26667,2003-08-28 11:14:33.23978,2003-08-28 11:14:33.21527,2003-08-28 11:14:33.2227,1999-04-16 09:01:42.92689,2003-08-28 11:14:33.21846,2003-08-28 11:14:33.24725}'::timestamp[],'{1997-11-25 19:05:00.83798,1998-01-05 11:57:12.19545,1998-10-14 09:06:01.87217,1998-12-29 16:19:18.50226,1999-01-18 15:01:52.77062,1999-12-28 07:57:37.93632,2001-05-16 10:55:44.78317,2003-05-23 10:32:40.1846,2003-08-28 11:14:33.23985,2004-02-04 14:01:57.60942,2005-07-26 17:01:10.98951,2005-07-26 18:41:33.09864,2006-04-25 16:52:03.49003,2008-02-18 14:17:08.58924,2010-04-19 10:16:19.03194,2012-07-23 10:47:40.65789,2014-12-05 10:59:02.25493}'::timestamp[],NULL::timestamp[],NULL::timestamp[]);
 insert into pg_statistic values ('orca.tmp_verd_s_pp_provtabs_agt_0015_extract1'::regclass,15::smallint,0.678255::real,8::integer,7167.15::real,1::smallint,2::smallint,0::smallint,0::smallint,2060::oid,2062::oid,0::oid,0::oid,'{0.000376719,0.00034981,0.00034981,0.00034981,0.00034981,0.00034981,0.00034981,0.000322902,0.000322902,0.000322902,0.000295993,0.000295993,0.000295993,0.000295993,0.000295993,0.000295993,0.000295993,0.000295993,0.000295993,0.000295993,0.000295993,0.000295993,0.000295993,0.000269085,0.000269085}'::real[],NULL::real[],NULL::real[],NULL::real[],'{2012-09-07 14:14:23.95552,2012-09-07 14:14:36.8171,2008-12-02 09:02:19.06415,2012-03-16 15:57:15.10939,1999-06-08 13:59:56.59862,1998-10-29 14:57:47.93588,1997-07-10 09:55:34.68999,2003-03-26 11:18:44.43314,2002-02-27 15:07:42.12004,2002-02-27 15:07:13.65666,2003-03-26 11:22:07.7484,2013-11-29 13:16:25.79261,2007-09-06 09:14:10.7907,1998-10-29 14:54:04.23854,2003-04-11 07:54:56.90542,2006-03-09 15:42:27.40086,2000-05-31 10:27:52.92485,2006-01-23 17:12:44.80256,2003-01-28 10:44:17.44046,2007-11-01 15:11:21.99194,2006-03-09 15:42:56.14013,2004-03-31 10:58:47.12524,1999-06-08 14:02:11.91465,1997-07-11 14:52:47.95918,1999-06-08 13:58:15.07927}'::timestamp[],'{1997-07-09 10:42:54.69421,1997-09-16 10:30:42.71499,1999-06-08 14:32:08.31914,2002-02-27 15:07:13.67355,2003-04-08 16:31:58.80724,2004-05-05 10:18:01.33179,2006-03-13 16:19:59.61215,2007-09-06 09:13:41.71774,2013-11-29 13:16:37.17591,2014-12-03 09:24:25.20945}'::timestamp[],NULL::timestamp[],NULL::timestamp[]);

set optimizer_segments = 256;
select a.*,  b.guelt_ab as guelt_ab_b, b.guelt_bis as guelt_bis_b
from orca.tmp_verd_s_pp_provtabs_agt_0015_extract1 a
left outer join  orca.tmp_verd_s_pp_provtabs_agt_0015_extract1 b
 ON  a.uid136=b.uid136 and a.tab_nr=b.tab_nr and  a.prdgrp=b.prdgrp and a.bg=b.bg and a.typ142=b.typ142 and a.ad_gsch=b.ad_gsch
AND a.guelt_ab <= b.guelt_ab AND a.guelt_bis > b.guelt_ab
;

set optimizer_segments = 3;
set allow_system_table_mods="NONE";
-- Arrayref
drop table if exists orca.arrtest;
create table orca.arrtest (
 a int2[],
 b int4[][][],
 c name[],
 d text[][]
 ) DISTRIBUTED RANDOMLY;

insert into orca.arrtest (a[1:5], b[1:1][1:2][1:2], c, d)
values ('{1,2,3,4,5}', '{{{0,0},{1,2}}}', '{}', '{}');

select a[1:3], b[1][2][1], c[1], d[1][1] FROM orca.arrtest order by 1,2,3,4;

select a[b[1][2][2]] from orca.arrtest;

-- MPP-20713, MPP-20714, MPP-20738: Const table get with a filter
select 1 as x where 1 in (2, 3);

-- MPP-22918: join inner child with universal distribution
SELECT generate_series(1,10) EXCEPT SELECT 1;

-- MPP-23932: SetOp of const table and volatile function
SELECT generate_series(1,10) INTERSECT SELECT 1;

SELECT generate_series(1,10) UNION SELECT 1;
--- warning messages for missing stats
create table foo_missing_stats(a int, b int);
insert into foo_missing_stats select i, i%5 from generate_series(1,20) i;
create table bar_missing_stats(c int, d int);
insert into bar_missing_stats select i, i%8 from generate_series(1,30) i;

analyze foo_missing_stats;
analyze bar_missing_stats;

select count(*) from foo_missing_stats where a = 10;
with x as (select * from foo_missing_stats) select count(*) from x x1, x x2 where x1.a = x2.a;
with x as (select * from foo_missing_stats) select count(*) from x x1, x x2 where x1.a = x2.b;

set allow_system_table_mods="DML";
delete from pg_statistic where starelid='foo_missing_stats'::regclass;
delete from pg_statistic where starelid='bar_missing_stats'::regclass;

select count(*) from foo_missing_stats where a = 10;
with x as (select * from foo_missing_stats) select count(*) from x x1, x x2 where x1.a = x2.a;
with x as (select * from foo_missing_stats) select count(*) from x x1, x x2 where x1.a = x2.b;

set optimizer_disable_missing_stats_collection = on;
-- MPP-25700 Fix to TO_DATE on hybrid datums during constant expression evaluation
drop table if exists orca.t3;
create table orca.t3 (c1 timestamp without time zone);
insert into orca.t3 values  ('2015-07-03 00:00:00'::timestamp without time zone);
select to_char(c1, 'YYYY-MM-DD HH24:MI:SS') from orca.t3 where c1 = TO_DATE('2015-07-03','YYYY-MM-DD');
select to_char(c1, 'YYYY-MM-DD HH24:MI:SS') from orca.t3 where c1 = '2015-07-03'::date;

-- Push components of disjunctive predicates
create table cust(cid integer, firstname text, lastname text) distributed by (cid);
create table datedim(date_sk integer, year integer, moy integer) distributed by (date_sk);
create table sales(item_sk integer, ticket_number integer, cid integer, date_sk integer, type text)
  distributed by (item_sk, ticket_number);

-- create a volatile function which should not be pushed
CREATE FUNCTION plusone(integer) RETURNS integer AS $$
BEGIN
    SELECT $1 + 1;
END;
$$ LANGUAGE plpgsql volatile;

-- force_explain
set optimizer_segments = 3;
explain
select c.cid cid,
       c.firstname firstname,
       c.lastname lastname,
       d.year dyear
from cust c, sales s, datedim d
where c.cid = s.cid and s.date_sk = d.date_sk and
      ((d.year = 2001 and lower(s.type) = 't1' and plusone(d.moy) = 5) or (d.moy = 4 and upper(s.type) = 'T2'));
reset optimizer_segments;

--
-- apply parallelization for subplan MPP-24563
--
create table t1_mpp_24563 (id int, value int) distributed by (id);
insert into t1_mpp_24563 values (1, 3);

create table t2_mpp_24563 (id int, value int, seq int) distributed by (id);
insert into t2_mpp_24563 values (1, 7, 5);

set optimizer = off;
select row_number() over (order by seq asc) as id, foo.cnt
from
(select seq, (select count(*) from t1_mpp_24563 t1 where t1.id = t2.id) cnt from
	t2_mpp_24563 t2 where value = 7) foo;

set optimizer = on;
select row_number() over (order by seq asc) as id, foo.cnt
from
(select seq, (select count(*) from t1_mpp_24563 t1 where t1.id = t2.id) cnt from
	t2_mpp_24563 t2 where value = 7) foo;

drop table t1_mpp_24563;
drop table t2_mpp_24563;

--
-- MPP-20470 update the flow of node after parallelizing subplan.
--
CREATE TABLE t_mpp_20470 (
    col_date timestamp without time zone,
    col_name character varying(6),
    col_expiry date
) DISTRIBUTED BY (col_date) PARTITION BY RANGE(col_date)
(
START ('2013-05-10 00:00:00'::timestamp without time zone) END ('2013-05-11
	00:00:00'::timestamp without time zone) WITH (tablename='t_mpp_20470_ptr1'),
START ('2013-05-24 00:00:00'::timestamp without time zone) END ('2013-05-25
	00:00:00'::timestamp without time zone) WITH (tablename='t_mpp_20470_ptr2')
);

COPY t_mpp_20470 from STDIN delimiter '|' null '';
2013-05-10 00:00:00|OPTCUR|2013-05-29
2013-05-10 04:35:20|OPTCUR|2013-05-29
2013-05-24 03:10:30|FUTCUR|2014-04-28
2013-05-24 05:32:34|OPTCUR|2013-05-29
\.

create view v1_mpp_20470 as
SELECT
CASE
	WHEN  b.col_name::text = 'FUTCUR'::text
	THEN  ( SELECT count(a.col_expiry) AS count FROM t_mpp_20470 a WHERE
		a.col_name::text = b.col_name::text)::text
	ELSE 'Q2'::text END  AS  cc,  1 AS nn
FROM t_mpp_20470 b;

set optimizer = off;
SELECT  cc, sum(nn) over() FROM v1_mpp_20470;
set optimizer = on;
SELECT  cc, sum(nn) over() FROM v1_mpp_20470;

drop view v1_mpp_20470;
drop table t_mpp_20470;

-- Checking if ORCA correctly populates canSetTag in PlannedStmt for multiple statements because of rules
drop table if exists can_set_tag_target;
create table can_set_tag_target
(
	x int,
	y int,
	z char
);

drop table if exists can_set_tag_audit;
create table can_set_tag_audit
(
	t timestamp without time zone,
	x int,
	y int,
	z char
);

create rule can_set_tag_audit_update AS
    ON UPDATE TO can_set_tag_target DO  INSERT INTO can_set_tag_audit (t, x, y, z)
  VALUES (now(), old.x, old.y, old.z);

insert into can_set_tag_target select i, i + 1, i + 2 from generate_series(1,2) as i;

create role unpriv;
grant all on can_set_tag_target to unpriv;
grant all on can_set_tag_audit to unpriv;
set role unpriv;
show optimizer;
update can_set_tag_target set y = y + 1;
select count(1) from can_set_tag_audit;
reset role;

revoke all on can_set_tag_target from unpriv;
revoke all on can_set_tag_audit from unpriv;
drop role unpriv;
drop table can_set_tag_target;
drop table can_set_tag_audit;

reset optimizer_segments;

-- Check if ORCA can handle GPDB's error properly
drop table if exists orca_exc_handle;
create table orca_exc_handle(
	a int primary key,
	b char
);

insert into orca_exc_handle select i, i from generate_Series(1,4) as i; 

-- enable the fault injector
--start_ignore
\! gpfaultinjector -f opt_relcache_translator_catalog_access -y error --seg_dbid 1
--end_ignore

select a from orca_exc_handle;
-- reset the fault injector
--start_ignore
\! gpfaultinjector -f opt_relcache_translator_catalog_access -y reset --seg_dbid 1
--end_ignore

drop table orca_exc_handle;
-- End of Check if ORCA can handle GPDB's error properly

-- Test B-Tree index scan with in list
CREATE TABLE btree_test as SELECT * FROM generate_series(1,100) as a distributed randomly;
CREATE INDEX btree_test_index ON btree_test(a);
EXPLAIN SELECT * FROM btree_test WHERE a in (1, 47);
EXPLAIN SELECT * FROM btree_test WHERE a in ('2', 47);
EXPLAIN SELECT * FROM btree_test WHERE a in ('1', '2');
EXPLAIN SELECT * FROM btree_test WHERE a in ('1', '2', 47);

-- Test Bitmap index scan with in list
CREATE TABLE bitmap_test as SELECT * FROM generate_series(1,100) as a distributed randomly;
CREATE INDEX bitmap_index ON bitmap_test USING BITMAP(a);
EXPLAIN SELECT * FROM bitmap_test WHERE a in (select 1);
EXPLAIN SELECT * FROM bitmap_test WHERE a in (1, 47);
EXPLAIN SELECT * FROM bitmap_test WHERE a in ('2', 47);
EXPLAIN SELECT * FROM bitmap_test WHERE a in ('1', '2');
EXPLAIN SELECT * FROM bitmap_test WHERE a in ('1', '2', 47);

-- Test Logging for unsupported features in ORCA
-- start_ignore
drop table if exists foo;
-- end_ignore

create table foo(a int, b int);
set client_min_messages='log';
select count(*) from foo group by cube(a,b);
reset client_min_messages;

-- start_ignore
drop table foo;
-- end_ignore

-- clean up
drop schema orca cascade;
