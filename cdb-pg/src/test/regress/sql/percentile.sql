create table perct as select a, a / 10 as b from generate_series(1, 100)a;
create table perct2 as select a, a / 10 as b from generate_series(1, 100)a, generate_series(1, 2);
create table perct3 as select a, b from perct, generate_series(1, 10)i where a % 7 < i;
create table perct4 as select case when a % 10 = 5 then null else a end as a,
	b, null::float as c from perct;
create table percts as select '2012-01-01 00:00:00'::timestamp + interval '1day' * i as a,
	i / 10 as b, i as c from generate_series(1, 100)i;
create table perctsz as select '2012-01-01 00:00:00 UTC'::timestamptz + interval '1day' * i as a,
	i / 10 as b, i as c from generate_series(1, 100)i;
create view percv as select percentile_cont(0.4) within group (order by a / 10),
	median(a), percentile_disc(0.51) within group (order by a desc) from perct group by b order by b;
create view percv2 as select median(a) as m1, median(a::float) as m2 from perct;

-- We test the same queries with gp_idf_deduplicate none/force. Make sure
-- both tests have the same when adding.
set gp_idf_deduplicate to none;
select percentile_cont(0.5) within group (order by a),
	median(a), percentile_disc(0.5) within group(order by a) from perct;
select b, percentile_cont(0.5) within group (order by a),
	median(a), percentile_disc(0.5) within group(order by a) from perct group by b order by b;
select percentile_cont(0.2) within group (order by a) from generate_series(1, 100)a;
select a / 10, percentile_cont(0.2) within group (order by a) from generate_series(1, 100)a
	group by a / 10 order by a / 10;
select percentile_cont(0.2) within group (order by a),
	percentile_cont(0.8) within group (order by a desc) from perct group by b order by b;
select percentile_cont(0.1) within group (order by a), count(*), sum(a) from perct
	group by b order by b;
select percentile_cont(0.6) within group (order by a), count(*), sum(a) from perct;
select percentile_cont(0.3) within group (order by a) + count(*) from perct group by b order by b;
select median(a) from perct group by b having median(a) = 5;
select median(a), percentile_cont(0.6) within group (order by a desc) from perct group by b having count(*) > 1 order by 1;
select median(10);
select count(*), median(b+1) from perct group by b+2
	having median(b+1) in (select avg(b+1) from perct group by b+2);
select median(a) from perct2;
select median(a) from perct2 group by b order by b;
select b, count(*), count(distinct a), median(a) from perct3 group by b order by b;
select b+1, count(*), count(distinct a),
	median(a), percentile_cont(0.3) within group (order by a desc)
	from perct group by b+1 order by b+1;
select median(a), median(c) from perct4;
select median(a), median(c) from perct4 group by b;
select count(*) over (partition by b), median(a) from perct group by b order by b;
select sum(median(a)) over (partition by b) from perct group by b order by b;
select percentile_disc(0) within group (order by a) from perct;
prepare p (float) as select percentile_cont($1) within group (order by a)
	from perct group by b order by b;
execute p(0.1);
execute p(0.8);
deallocate p;
select sum((select median(a) from perct)) from perct;
select percentile_cont(null) within group (order by a) from perct;
select percentile_cont(null) within group (order by a),
	percentile_disc(null) within group (order by a desc) from perct group by b;
select median(a), percentile_cont(0.5) within group (order by a),
	percentile_disc(0.5) within group(order by a),
	(select min(a) from percts) - interval '1day' + interval '1day' * median(c),
	(select min(a) from percts) - interval '1day' + interval '1day' *
		percentile_disc(0.5) within group (order by c)
	from percts group by b order by b;
select percentile_cont(1.0/86400) within group (order by a) from percts
	where c between 1 and 2;
select percentile_cont(0.1) within group (order by a),
	percentile_cont(0.9) within group (order by a desc) from percts;
select percentile_cont(0.1) within group (order by a),
	percentile_cont(0.2) within group (order by a) from perctsz;
select median(a - (select min(a) from percts)) from percts;
select median(a), b from perct group by b order by b desc;
select count(*) from(select median(a) from perct group by ())s;
select median(a) from perct group by grouping sets((b)) order by b;
select distinct median(a), count(*) from perct;
-- view
select * from percv;

set gp_idf_deduplicate to force;
select percentile_cont(0.5) within group (order by a),
	median(a), percentile_disc(0.5) within group(order by a) from perct;
select b, percentile_cont(0.5) within group (order by a),
	median(a), percentile_disc(0.5) within group(order by a) from perct group by b order by b;
select percentile_cont(0.2) within group (order by a) from generate_series(1, 100)a;
select a / 10, percentile_cont(0.2) within group (order by a) from generate_series(1, 100)a
	group by a / 10 order by a / 10;
select percentile_cont(0.2) within group (order by a),
	percentile_cont(0.8) within group (order by a desc) from perct group by b order by b;
select percentile_cont(0.1) within group (order by a), count(*), sum(a) from perct
	group by b order by b;
select percentile_cont(0.6) within group (order by a), count(*), sum(a) from perct;
select percentile_cont(0.3) within group (order by a) + count(*) from perct group by b order by b;
select median(a) from perct group by b having median(a) = 5;
select median(a), percentile_cont(0.6) within group (order by a desc) from perct group by b having count(*) > 1 order by 1;
select median(10);
select count(*), median(b+1) from perct group by b+2
	having median(b+1) in (select avg(b+1) from perct group by b+2);
select median(a) from perct2;
select median(a) from perct2 group by b order by b;
select b, count(*), count(distinct a), median(a) from perct3 group by b order by b;
select b+1, count(*), count(distinct a),
	median(a), percentile_cont(0.3) within group (order by a desc)
	from perct group by b+1 order by b+1;
select median(a), median(c) from perct4;
select median(a), median(c) from perct4 group by b;
select count(*) over (partition by b), median(a) from perct group by b order by b;
select sum(median(a)) over (partition by b) from perct group by b order by b;
select percentile_disc(0) within group (order by a) from perct;
prepare p (float) as select percentile_cont($1) within group (order by a)
	from perct group by b order by b;
execute p(0.1);
execute p(0.8);
deallocate p;
select sum((select median(a) from perct)) from perct;
select percentile_cont(null) within group (order by a) from perct;
select percentile_cont(null) within group (order by a),
	percentile_disc(null) within group (order by a desc) from perct group by b;
select median(a), percentile_cont(0.5) within group (order by a),
	percentile_disc(0.5) within group(order by a),
	(select min(a) from percts) - interval '1day' + interval '1day' * median(c),
	(select min(a) from percts) - interval '1day' + interval '1day' *
		percentile_disc(0.5) within group (order by c)
	from percts group by b order by b;
select percentile_cont(1.0/86400) within group (order by a) from percts
	where c between 1 and 2;
select percentile_cont(0.1) within group (order by a),
	percentile_cont(0.9) within group (order by a desc) from percts;
select percentile_cont(0.1) within group (order by a),
	percentile_cont(0.2) within group (order by a) from perctsz;
select median(a - (select min(a) from percts)) from percts;
select median(a), b from perct group by b order by b desc;
select count(*) from(select median(a) from perct group by ())s;
select median(a) from perct group by grouping sets((b)) order by b;
select distinct median(a), count(*) from perct;
-- view
select * from percv;

reset gp_idf_deduplicate;

select pg_get_viewdef('percv');
select pg_get_viewdef('percv2');

-- errors
-- no WITHIN GROUP clause
select percentile_cont(a) from perct;
-- the argument must not contain variable
select percentile_cont(a) within group (order by a) from perct;
-- ungrouped column
select b, percentile_disc(0.1) within group (order by a) from perct;
-- nested aggregate
select percentile_cont(count(*)) within group (order by a) from perct;
select sum(percentile_cont(0.22) within group (order by a)) from perct;
-- OVER clause
select percentile_cont(0.3333) within group (order by a) over (partition by a%2) from perct;
select median(a) over (partition by b) from perct group by b;
-- function scan
select * from median(10);
-- wrong type argument
select percentile_disc('a') within group (order by a) from perct;
-- nested case
select count(median(a)) from perct;
select median(count(*)) from perct;
select percentile_cont(0.2) within group (order by count(*) over()) from perct;
select percentile_disc(0.1) within group (order by group_id()) from perct;
-- subquery is not allowed to the argument
select percentile_cont((select 0.1 from gp_id)) within group (order by a) from perct;
-- the argument must not be volatile expression
select percentile_cont(random()) within group (order by a) from perct;
-- out of range
select percentile_cont(-0.1) within group (order by a) from perct;
select percentile_cont(1.00000001) within group (order by a) from perct;
-- CSQ is not supported currently.  Shame.
select sum((select median(a) from perct where b = t.b)) from perct t;
-- used in LIMIT
select * from perct limit median(a);
-- multiple sort key
select percentile_cont(0.8) within group (order by a, a + 1, a + 2) from perct;
-- set-returning
select generate_series(1, 2), median(a) from perct;
-- GROUPING SETS
select median(a) from perct group by grouping sets((), (b));
-- wrong type in ORDER BY
select median('text') from perct;
select percentile_cont(now()) within group (order by a) from percts;
select percentile_cont(0.5) within group (order by point(0,0)) from perct;
-- outer reference is not allowed for now
select (select a from perct where median(t.a) = 5) from perct t;

drop view percv2;
drop view percv;
drop table perct;
drop table perct2;
drop table perct3;
drop table perct4;
drop table percts;
drop table perctsz;
