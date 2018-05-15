


select avg(a), count(b), sum(c), count(*) from test_int2;
select avg(a), count(b), sum(c), count(*) from test_int4;
select avg(a), count(b), sum(c), count(*) from test_int8;
select avg(a), count(b), sum(c), count(*) from test_float4;
select avg(a), count(b), sum(c), count(*) from test_float8;


select avg(a), count(b), sum(c), count(*) from test_int2 where a > 65536 and b > 0;
select avg(a), count(b), sum(c), count(*) from test_int4 where a > 65536 and b > 0;
select avg(a), count(b), sum(c), count(*) from test_int8 where a > 65536 and b > 0;
select avg(a), count(b), sum(c), count(*) from test_float4 where a > 65536 and b > 0;
select avg(a), count(b), sum(c), count(*) from test_float8 where a > 65536 and b > 0;

select avg(a), count(b), sum(c), count(*) from test_int2 where a > 5120 and b > 0;
select avg(a), count(b), sum(c), count(*) from test_int4 where a > 5120 and b > 0;
select avg(a), count(b), sum(c), count(*) from test_int8 where a > 5120 and b > 0;
select avg(a), count(b), sum(c), count(*) from test_float4 where a > 5120 and b > 0;
select avg(a), count(b), sum(c), count(*) from test_float8 where a > 5120 and b > 0;



select avg(a), count(b), sum(c), count(*) from test_int2 where a > 5120 and b > 0;
select avg(a), count(b), sum(c), count(*) from test_int4 where a > 5120 and b > 0;
select avg(a), count(b), sum(c), count(*) from test_int8 where a > 5120 and b > 0;
select avg(a), count(b), sum(c), count(*) from test_float4 where a > 5120 and b > 0;
select avg(a), count(b), sum(c), count(*) from test_float8 where a > 5120 and b > 0;


select avg(a), count(b), sum(c), count(*) from test_int2;
select avg(a), count(b), sum(c), count(*) from test_int4;
select avg(a), count(b), sum(c), count(*) from test_int8;
select avg(a), count(b), sum(c), count(*) from test_float4;
select avg(a), count(b), sum(c), count(*) from test_float8;


select avg(a) from test_int2;
select avg(a) from test_int4;
select avg(a) from test_int8;
select avg(a) from test_float4;
select avg(a) from test_float8;


select avg(a) from test_int2 where a > 5120 and b > 0;
select avg(a) from test_int4 where a > 5120 and b > 0;
select avg(a) from test_int8 where a > 5120 and b > 0;
select avg(a) from test_float4 where a > 5120 and b > 0;
select avg(a) from test_float8 where a > 5120 and b > 0;

select sum(a) from test_int2;
select sum(a) from test_int4;
select sum(a) from test_int8;
select sum(a) from test_float4;
select sum(a) from test_float8;


select sum(a) from test_int2 where a > 5120 and b > 0;
select sum(a) from test_int4 where a > 5120 and b > 0;
select sum(a) from test_int8 where a > 5120 and b > 0;
select sum(a) from test_float4 where a > 5120 and b > 0;
select sum(a) from test_float8 where a > 5120 and b > 0;


select count(a) from test_int2;
select count(a) from test_int4;
select count(a) from test_int8;
select count(a) from test_float4;
select count(a) from test_float8;


select count(a) from test_int2 where a > 5120 and b > 0;
select count(a) from test_int4 where a > 5120 and b > 0;
select count(a) from test_int8 where a > 5120 and b > 0;
select count(a) from test_float4 where a > 5120 and b > 0;
select count(a) from test_float8 where a > 5120 and b > 0;

select count(*) from test_int2;
select count(*) from test_int4;
select count(*) from test_int8;
select count(*) from test_float4;
select count(*) from test_float8;


select count(*) from test_int2 where a > 5120 and b > 0;
select count(*) from test_int4 where a > 5120 and b > 0;
select count(*) from test_int8 where a > 5120 and b > 0;
select count(*) from test_float4 where a > 5120 and b > 0;
select count(*) from test_float8 where a > 5120 and b > 0;



--group by a;
select avg(a), count(b), sum(c), count(*) from test_int2 where a > 5120 and b > 0 group by a order by a;
select avg(a), count(b), sum(c), count(*) from test_int4 where a > 5120 and b > 0 group by a order by a;
select avg(a), count(b), sum(c), count(*) from test_int8 where a > 5120 and b > 0 group by a order by a;
select avg(a), count(b), sum(c), count(*) from test_float4 where a > 5120 and b > 0 group by a order by a;
select avg(a), count(b), sum(c), count(*) from test_float8 where a > 5120 and b > 0 group by a order by a;


select avg(a), count(b), sum(c), count(*) from test_int2 group by a order by a;
select avg(a), count(b), sum(c), count(*) from test_int4 group by a order by a;
select avg(a), count(b), sum(c), count(*) from test_int8 group by a order by a;
select avg(a), count(b), sum(c), count(*) from test_float4 group by a order by a;
select avg(a), count(b), sum(c), count(*) from test_float8 group by a order by a;


select avg(a) from test_int2 group by a order by a;
select avg(a) from test_int4 group by a order by a;
select avg(a) from test_int8 group by a order by a;
select avg(a) from test_float4 group by a order by a;
select avg(a) from test_float8 group by a order by a;


select avg(a) from test_int2 where a > 5120 and b > 0 group by a order by a;
select avg(a) from test_int4 where a > 5120 and b > 0 group by a order by a;
select avg(a) from test_int8 where a > 5120 and b > 0 group by a order by a;
select avg(a) from test_float4 where a > 5120 and b > 0 group by a order by a;
select avg(a) from test_float8 where a > 5120 and b > 0 group by a order by a;

select sum(a) from test_int2 group by a order by a;
select sum(a) from test_int4 group by a order by a;
select sum(a) from test_int8 group by a order by a;
select sum(a) from test_float4 group by a order by a;
select sum(a) from test_float8 group by a order by a;


select sum(a) from test_int2 where a > 5120 and b > 0 group by a order by a;
select sum(a) from test_int4 where a > 5120 and b > 0 group by a order by a;
select sum(a) from test_int8 where a > 5120 and b > 0 group by a order by a;
select sum(a) from test_float4 where a > 5120 and b > 0 group by a order by a;
select sum(a) from test_float8 where a > 5120 and b > 0 group by a order by a;


select count(a) from test_int2 group by a;
select count(a) from test_int4 group by a;
select count(a) from test_int8 group by a;
select count(a) from test_float4 group by a;
select count(a) from test_float8 group by a;


select count(a) from test_int2 where a > 5120 and b > 0 group by a;
select count(a) from test_int4 where a > 5120 and b > 0 group by a;
select count(a) from test_int8 where a > 5120 and b > 0 group by a;
select count(a) from test_float4 where a > 5120 and b > 0 group by a;
select count(a) from test_float8 where a > 5120 and b > 0 group by a;

select count(*) from test_int2 group by a;
select count(*) from test_int4 group by a;
select count(*) from test_int8 group by a;
select count(*) from test_float4 group by a;
select count(*) from test_float8 group by a;


select count(*) from test_int2 where a > 5120 and b > 0 group by a;
select count(*) from test_int4 where a > 5120 and b > 0 group by a;
select count(*) from test_int8 where a > 5120 and b > 0 group by a;
select count(*) from test_float4 where a > 5120 and b > 0 group by a;
select count(*) from test_float8 where a > 5120 and b > 0 group by a;


--group by b;
select avg(a), count(b), sum(c), count(*) from test_int2 where a > 5120 and b > 0 group by b;
select avg(a), count(b), sum(c), count(*) from test_int4 where a > 5120 and b > 0 group by b;
select avg(a), count(b), sum(c), count(*) from test_int8 where a > 5120 and b > 0 group by b;
select avg(a), count(b), sum(c), count(*) from test_float4 where a > 5120 and b > 0 group by b;
select avg(a), count(b), sum(c), count(*) from test_float8 where a > 5120 and b > 0 group by b;


select avg(a), count(b), sum(c), count(*) from test_int2 group by b;
select avg(a), count(b), sum(c), count(*) from test_int4 group by b;
select avg(a), count(b), sum(c), count(*) from test_int8 group by b;
select avg(a), count(b), sum(c), count(*) from test_float4 group by b;
select avg(a), count(b), sum(c), count(*) from test_float8 group by b;


select avg(a) from test_int2 group by b;
select avg(a) from test_int4 group by b;
select avg(a) from test_int8 group by b;
select avg(a) from test_float4 group by b;
select avg(a) from test_float8 group by b;


select avg(a) from test_int2 where a > 5120 and b > 0 group by b;
select avg(a) from test_int4 where a > 5120 and b > 0 group by b;
select avg(a) from test_int8 where a > 5120 and b > 0 group by b;
select avg(a) from test_float4 where a > 5120 and b > 0 group by b;
select avg(a) from test_float8 where a > 5120 and b > 0 group by b;

select sum(a) from test_int2 group by b;
select sum(a) from test_int4 group by b;
select sum(a) from test_int8 group by b;
select sum(a) from test_float4 group by b;
select sum(a) from test_float8 group by b;


select sum(a) from test_int2 where a > 5120 and b > 0 group by b;
select sum(a) from test_int4 where a > 5120 and b > 0 group by b;
select sum(a) from test_int8 where a > 5120 and b > 0 group by b;
select sum(a) from test_float4 where a > 5120 and b > 0 group by b;
select sum(a) from test_float8 where a > 5120 and b > 0 group by b;


select count(a) from test_int2 group by b;
select count(a) from test_int4 group by b;
select count(a) from test_int8 group by b;
select count(a) from test_float4 group by b;
select count(a) from test_float8 group by b;


select count(a) from test_int2 where a > 5120 and b > 0 group by b;
select count(a) from test_int4 where a > 5120 and b > 0 group by b;
select count(a) from test_int8 where a > 5120 and b > 0 group by b;
select count(a) from test_float4 where a > 5120 and b > 0 group by b;
select count(a) from test_float8 where a > 5120 and b > 0 group by b;

select count(*) from test_int2 group by b;
select count(*) from test_int4 group by b;
select count(*) from test_int8 group by b;
select count(*) from test_float4 group by b;
select count(*) from test_float8 group by b;


select count(*) from test_int2 where a > 5120 and b > 0 group by b;
select count(*) from test_int4 where a > 5120 and b > 0 group by b;
select count(*) from test_int8 where a > 5120 and b > 0 group by b;
select count(*) from test_float4 where a > 5120 and b > 0 group by b;
select count(*) from test_float8 where a > 5120 and b > 0 group by b;


--group by b,c;
select avg(a), count(b), sum(c), count(*) from test_int2 where a > 5120 and b > 0 group by b,c;
select avg(a), count(b), sum(c), count(*) from test_int4 where a > 5120 and b > 0 group by b,c;
select avg(a), count(b), sum(c), count(*) from test_int8 where a > 5120 and b > 0 group by b,c;
select avg(a), count(b), sum(c), count(*) from test_float4 where a > 5120 and b > 0 group by b,c;
select avg(a), count(b), sum(c), count(*) from test_float8 where a > 5120 and b > 0 group by b,c;


select avg(a), count(b), sum(c), count(*) from test_int2 group by b,c;
select avg(a), count(b), sum(c), count(*) from test_int4 group by b,c;
select avg(a), count(b), sum(c), count(*) from test_int8 group by b,c;
select avg(a), count(b), sum(c), count(*) from test_float4 group by b,c;
select avg(a), count(b), sum(c), count(*) from test_float8 group by b,c;


select avg(a) from test_int2 group by b,c;
select avg(a) from test_int4 group by b,c;
select avg(a) from test_int8 group by b,c;
select avg(a) from test_float4 group by b,c;
select avg(a) from test_float8 group by b,c;


select avg(a) from test_int2 where a > 5120 and b > 0 group by b,c;
select avg(a) from test_int4 where a > 5120 and b > 0 group by b,c;
select avg(a) from test_int8 where a > 5120 and b > 0 group by b,c;
select avg(a) from test_float4 where a > 5120 and b > 0 group by b,c;
select avg(a) from test_float8 where a > 5120 and b > 0 group by b,c;

select sum(a) from test_int2 group by b,c;
select sum(a) from test_int4 group by b,c;
select sum(a) from test_int8 group by b,c;
select sum(a) from test_float4 group by b,c;
select sum(a) from test_float8 group by b,c;


select sum(a) from test_int2 where a > 5120 and b > 0 group by b,c;
select sum(a) from test_int4 where a > 5120 and b > 0 group by b,c;
select sum(a) from test_int8 where a > 5120 and b > 0 group by b,c;
select sum(a) from test_float4 where a > 5120 and b > 0 group by b,c;
select sum(a) from test_float8 where a > 5120 and b > 0 group by b,c;


select count(a) from test_int2 group by b,c;
select count(a) from test_int4 group by b,c;
select count(a) from test_int8 group by b,c;
select count(a) from test_float4 group by b,c;
select count(a) from test_float8 group by b,c;


select count(a) from test_int2 where a > 5120 and b > 0 group by b,c;
select count(a) from test_int4 where a > 5120 and b > 0 group by b,c;
select count(a) from test_int8 where a > 5120 and b > 0 group by b,c;
select count(a) from test_float4 where a > 5120 and b > 0 group by b,c;
select count(a) from test_float8 where a > 5120 and b > 0 group by b,c;

select count(*) from test_int2 group by b,c;
select count(*) from test_int4 group by b,c;
select count(*) from test_int8 group by b,c;
select count(*) from test_float4 group by b,c;
select count(*) from test_float8 group by b,c;


select count(*) from test_int2 where a > 5120 and b > 0 group by b,c;
select count(*) from test_int4 where a > 5120 and b > 0 group by b,c;
select count(*) from test_int8 where a > 5120 and b > 0 group by b,c;
select count(*) from test_float4 where a > 5120 and b > 0 group by b,c;
select count(*) from test_float8 where a > 5120 and b > 0 group by b,c;



