set hawq_hashjoin_bloomfilter=true;

select * from fact, dim where fact.c1 = dim.c1 and dim.c2 < 4 order by dim.c1;

select count(*) from fact, dim where fact.c1 = dim.c1 and dim.c2 < 4;

select fact.c1 from fact, dim where fact.c1 = dim.c1 and dim.c2 < 4 order by dim.c1;

select fact.c1, dim.c1 from fact, dim where fact.c1 = dim.c1 and dim.c2 <4 order by dim.c1;

select fact.c1, dim.c1, dim.c2 from fact, dim where fact.c1 = dim.c1 and dim.c2 <4 order by dim.c1;

select dim.c1, dim.c2 from fact, dim where fact.c1 = dim.c1 and dim.c2<4;
    
with part1 as (
  select c1 from dim where c1 < 4
),
part2 as (
  select c1, 0.2 * avg(c3) as avg_c3
  from fact
  where c1 IN (select c1 from part1)
  group by c1
),
part3 as (
  select 
  * 
  from fact where c1 in (select c1 from part1)
)
select sum(c3) from part2, part3 where part2.c1 = part3.c1;


with part1 as (
  select c1 from dim where c1 < 4
),
part2 as (
  select c1, 0.2 * avg(c3) as avg_c3
  from fact
  where c1 IN (select c1 from part1)
  group by c1
),
part3 as (
  select 
  c1, c3
  from fact where c1 in (select c1 from part1)
)
select sum(c3) from part2, part3 where part2.c1 = part3.c1;

with part1 as (
  select c1 from dim where c1 < 4
),
part2 as (
  select c1, 0.2 * avg(c3) as avg_c3
  from fact
  where c1 IN (select c1 from part1)
  group by c1
),
part3 as (
  select 
  c3, c1
  from fact where c1 in (select c1 from part1)
)
select sum(c3) from part2, part3 where part2.c1 = part3.c1;
