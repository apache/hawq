drop table if exists dqa_t1;
drop table if exists dqa_t2;

create table dqa_t1 (d int, i int, c char, dt date);
create table dqa_t2 (d int, i int, c char, dt date);

insert into dqa_t1 select i%23, i%12, (i%10) || '', '2009-06-10'::date + ( (i%34) || ' days')::interval
from generate_series(0, 99) i;
insert into dqa_t2 select i%34, i%45, (i%10) || '', '2009-06-10'::date + ( (i%56) || ' days')::interval
from generate_series(0, 99) i;

set gp_eager_agg_distinct_pruning=on;
set enable_groupagg=off;

-- Distinct keys are distribution keys
select count(distinct d) from dqa_t1;
select count(distinct d) from dqa_t1 group by i;

select count(distinct d), count(distinct dt) from dqa_t1;
select count(distinct d), count(distinct c), count(distinct dt) from dqa_t1;

select count(distinct d), count(distinct dt) from dqa_t1 group by c;
select count(distinct d), count(distinct dt) from dqa_t1 group by d;

select count(distinct dqa_t1.d) from dqa_t1, dqa_t2 where dqa_t1.d = dqa_t2.d;
select count(distinct dqa_t1.d) from dqa_t1, dqa_t2 where dqa_t1.d = dqa_t2.d group by dqa_t2.dt;

-- Distinct keys are not distribution keys
select count(distinct c) from dqa_t1;
select count(distinct c) from dqa_t1 group by dt;
select count(distinct c) from dqa_t1 group by d;

select count(distinct c), count(distinct dt) from dqa_t1;
select count(distinct c), count(distinct dt), i from dqa_t1 group by i;
select count(distinct i), count(distinct c), d from dqa_t1 group by d;

select count(distinct dqa_t1.dt) from dqa_t1, dqa_t2 where dqa_t1.c = dqa_t2.c;
select count(distinct dqa_t1.dt) from dqa_t1, dqa_t2 where dqa_t1.c = dqa_t2.c group by dqa_t2.dt;
