--
-- CREATE_AGGREGATE
--

-- all functions CREATEd
CREATE AGGREGATE newavg (
   sfunc = int4_avg_accum, basetype = int4, stype = bytea, 
   finalfunc = int8_avg,
   initcond1 = '{0}'
);

-- test comments
COMMENT ON AGGREGATE newavg_wrong (int4) IS 'an agg comment';
COMMENT ON AGGREGATE newavg (int4) IS 'an agg comment';
COMMENT ON AGGREGATE newavg (int4) IS NULL;

-- without finalfunc; test obsolete spellings 'sfunc1' etc
CREATE AGGREGATE newsum (
   sfunc1 = int4pl, basetype = int4, stype1 = int4, 
   initcond1 = '0'
);

-- zero-argument aggregate
CREATE AGGREGATE newcnt (*) (
   sfunc = int8inc, stype = int8,
   initcond = '0'
);

-- old-style spelling of same
CREATE AGGREGATE oldcnt (
   sfunc = int8inc, basetype = 'ANY', stype = int8,
   initcond = '0'
);

-- aggregate that only cares about null/nonnull input
CREATE AGGREGATE newcnt ("any") (
   sfunc = int8inc_any, stype = int8,
   initcond = '0'
);

-- multi-argument aggregate
create function sum3(int8,int8,int8) returns int8 as
'select $1 + $2 + $3' language sql strict immutable;

create aggregate sum2(int8,int8) (
   sfunc = sum3, stype = int8,
   initcond = '0'
);


-- multi-argument aggregates sensitive to distinct/order, strict/nonstrict

/*
-- MPP: In Postgres this creates an array type with it, in GP it does not.
create type aggtype as (a integer, b integer, c text);

create function aggf_trans(aggtype[],integer,integer,text) returns aggtype[]
as 'select array_append($1,ROW($2,$3,$4)::aggtype)'
language sql strict immutable;

create function aggfns_trans(aggtype[],integer,integer,text) returns aggtype[]
as 'select array_append($1,ROW($2,$3,$4)::aggtype)'
language sql immutable;

create aggregate aggfstr(integer,integer,text) (
   sfunc = aggf_trans, stype = aggtype[],
   initcond = '{}'
);

create aggregate aggfns(integer,integer,text) (
   sfunc = aggfns_trans, stype = aggtype[],
   initcond = '{}'
);
*/
create function aggf_trans(text[],integer,integer,text) returns text[]
as 'select array_append($1, textin(record_out(ROW($2,$3,$4))))'
language sql strict immutable;

create function aggfns_trans(text[],integer,integer,text) returns text[]
as 'select array_append($1, textin(record_out(ROW($2,$3,$4))))'
language sql immutable;

create ordered aggregate aggfstr(integer,integer,text) (
   stype = text[],
   sfunc = aggf_trans, 
   initcond = '{}'
);

create ordered aggregate aggfns(integer,integer,text) (
   stype = text[],
   sfunc = aggfns_trans, 
   initcond = '{}'
);

-- Negative test: "ordered aggregate prefunc is not supported"
create ordered aggregate should_error(integer,integer,text) (
   stype = text[],
   sfunc = aggfns_trans, 
   prefunc = array_cat,
   initcond = '{}'
);



-- Comments on aggregates
COMMENT ON AGGREGATE nosuchagg (*) IS 'should fail';
COMMENT ON AGGREGATE newcnt (*) IS 'an agg(*) comment';
COMMENT ON AGGREGATE newcnt ("any") IS 'an agg(any) comment';

-- MPP-2863: ensure that aggregate declarations with an initial value == ''
-- do not get converted to an initial value == NULL
create function str_concat(t1 text, t2 text) returns text as
$$
    select $1 || $2;
$$ language sql;

CREATE AGGREGATE string_concat (sfunc = str_concat, prefunc=str_concat, basetype = 'text', stype = text,initcond = '');

create table aggtest2(i int, t text) DISTRIBUTED BY (i);
insert into aggtest2 values(1, 'hello');
insert into aggtest2 values(2, 'hello');
select string_concat(t) from aggtest2;
select string_concat(t) from (select * from aggtest2 limit 2000) tmp;
drop table aggtest2;
drop aggregate string_concat(text);
drop function str_concat(text, text);

-- Test aggregates with prefunc and finalfunc property
CREATE FUNCTION sum(numeric, numeric) RETURNS
numeric
    AS 'select $1 + $2'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE FUNCTION pre_sum(numeric, numeric) RETURNS
numeric
    AS 'select $1 + $2'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE FUNCTION final_sum(numeric) RETURNS
numeric
    AS 'select $1 + $1'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE AGGREGATE agg_sum(numeric) (
    SFUNC = sum,
    STYPE = numeric,
    INITCOND = 0);

CREATE AGGREGATE agg_prefunc(numeric) (
    SFUNC = sum,
    STYPE = numeric,
    PREFUNC =pre_sum,
    INITCOND = 0);

CREATE AGGREGATE agg_finalfunc(numeric) (
    SFUNC = sum,
    STYPE = numeric,
    FINALFUNC =final_sum,
    INITCOND = 0);

CREATE AGGREGATE agg_pre_final(numeric) (
    SFUNC = sum,
    STYPE = numeric,
    PREFUNC =pre_sum,
    FINALFUNC =final_sum,
    INITCOND = 0);

create table aggtest2(a int);
insert into aggtest2 select * from generate_series(1,3);
insert into aggtest2 select * from generate_series(1,3);

select agg_sum(a) from aggtest2;
select agg_prefunc(a) from aggtest2;
select agg_finalfunc(a) from aggtest2;
select agg_pre_final(a) from aggtest2;

-- Test initcond functionality
CREATE FUNCTION simple_func(numeric, numeric) RETURNS
numeric
    AS 'select $1 + $2'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

create aggregate simple_agg(numeric)(sfunc = simple_func, stype = numeric, initcond = 10);
create table foo as select i from generate_series(1, 20)i;
select simple_agg(i) from foo;
drop aggregate simple_agg(numeric);
create aggregate simple_agg(numeric)(sfunc = simple_func, stype = numeric, initcond = -10);
select simple_agg(i) from foo;
drop table foo;
drop aggregate simple_agg(numeric);

-- Test multiple aggregates in the same query
create aggregate simple_agg(numeric)(sfunc = simple_func, stype = numeric, initcond = 0);
create table foo (a int, b int);
insert into foo values (1,2);
insert into foo values (3,4);
insert into foo values (5,6);  
select simple_agg(a), simple_agg(b) from foo;
drop table foo;
drop function simple_func(numeric, numeric) cascade;

-- Test ordered aggregate
create table sch_quantity ( prod_key integer, qty integer, price integer, product character(3));
insert into sch_quantity values (1,100, 50, 'p1');
insert into sch_quantity values (2,200, 100, 'p2');
insert into sch_quantity values (3,300, 200, 'p3');
insert into sch_quantity values (4,400, 35, 'p4');
insert into sch_quantity values (5,500, 40, 'p5');
insert into sch_quantity values (1,150, 50, 'p1');
insert into sch_quantity values (2,50, 100, 'p2');
insert into sch_quantity values (3,150, 200, 'p3');
insert into sch_quantity values (4,200, 35, 'p4');
insert into sch_quantity values (5,300, 40, 'p5');
CREATE ORDERED AGGREGATE sch_array_accum_final (anyelement)
(
      sfunc = array_append,
      stype = anyarray,
      finalfunc = array_out,
      initcond = '{}'
);
select prod_key, sch_array_accum_final(qty order by prod_key,qty) from sch_quantity group by prod_key having prod_key < 5 order by prod_key;
drop table sch_quantity;
drop aggregate sch_array_accum_final(anyelement);


-- Test privileges with aggregates
create role aggregate_user1 with login nosuperuser nocreatedb;
create role aggregate_user2 with login nosuperuser nocreatedb;
create role aggregate_user3 with login nosuperuser nocreatedb;

set session authorization aggregate_user1;

CREATE AGGREGATE user1_aggregate_avg (
   sfunc = int4_avg_accum, basetype = int4, stype = bytea, 
   finalfunc = int8_avg,
   initcond1 = '{0}'
);
create table user1_aggregate_table(i int, j int);
insert into user1_aggregate_table select i , i %3 from generate_series(1, 20) i;
select user1_aggregate_avg(i) from user1_aggregate_table;

GRANT ALL on table user1_aggregate_table to aggregate_user2;
GRANT ALL on function user1_aggregate_avg(integer) to aggregate_user2;

-- only grant permissions on the table to user3
GRANT ALL on table user1_aggregate_table to aggregate_user3;
reset session authorization;


set session authorization aggregate_user2;
-- should work fine since user2 has permissions on aggregate and table
select user1_aggregate_avg(i) from user1_aggregate_table;
--drops should fail
drop table user1_aggregate_table;
drop aggregate user1_aggregate_avg(integer);
reset session authorization;


set session authorization aggregate_user3;
-- user3 is able to execute the average even if no permissions ? 
SELECT user1_aggregate_avg(i) from user1_aggregate_table;
-- drop should fail
drop aggregate user1_aggregate_avg(integer);
reset session authorization;

set session authorization aggregate_user1;
-- drops should work fine 
drop table user1_aggregate_table;
drop aggregate user1_aggregate_avg(integer);
reset session authorization;

set session authorization aggregate_user1;
CREATE AGGREGATE user1_aggregate_avg (
   sfunc = int4_avg_accum, basetype = int4, stype = bytea, 
   finalfunc = int8_avg,
   initcond1 = '{0}'
);
reset session authorization;
ALTER AGGREGATE user1_aggregate_avg(integer) OWNER to aggregate_user2;

set session authorization aggregate_user2;
ALTER AGGREGATE user1_aggregate_avg(integer) RENAME to aggregate_rename;
select proname from pg_proc where proname = 'aggregate_rename';
create table user1_aggregate_table(i int, j int);
insert into user1_aggregate_table select i , i %3 from generate_series(1, 20) i;
SELECT aggregate_rename(i) from user1_aggregate_table;
reset session authorization;

create schema aggschema;
alter aggregate aggregate_rename(integer) set schema aggschema;
SELECT aggschema.aggregate_rename(i) from user1_aggregate_table;
drop aggregate aggschema.aggregate_rename(integer);
drop table user1_aggregate_table;
drop role aggregate_user1;
drop role aggregate_user2;
drop role aggregate_user3;
