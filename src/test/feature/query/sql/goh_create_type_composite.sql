--
-- CREATE_TYPE
--

--
-- Note: widget_in/out were created in create_function_1, without any
-- prior shell-type creation.  These commands therefore complete a test
-- of the "old style" approach of making the functions first.
--
-- start_ignore
drop database hdfs;
-- end_ignore
create database hdfs;
\c hdfs

-- Test stand-alone composite type
create type temp_type_1 as (a int, b int);
create type temp_type_2 as (a int, b int);
create table temp_table (id int, a temp_type_1, b temp_type_2) distributed randomly;

insert into temp_table values (1, (1,2), (3,4));
insert into temp_table values (2, (5,6), (7,8));
insert into temp_table values (3, (9,10), (11,12));

\d temp_table
select * from temp_table order by 1;

drop table temp_table;

create type temp_type_3 as (a temp_type_1, b temp_type_2);
CREATE table temp_table (id int, a temp_type_1, b temp_type_3) distributed randomly;
insert into temp_table values (1, (9,10), ((11,12),(7,8)));
insert into temp_table values (2, (1,2), ((3,4),(5,6)));

select * from temp_table order by 1;

-- check catalog entries for types
select count(typrelid) from pg_type where typname like 'temp_type_%';

comment on type temp_type_1 is 'test composite type';
\dT temp_type_1

select count(reltype) from pg_class where relname like 'temp_type%';

create table test_func (foo temp_type_1);
insert into test_func values((1,2));
insert into test_func values((3,4));
insert into test_func values((5,6));
insert into test_func values((7,8));

-- Functions with UDTs
create function test_temp_func(temp_type_1, temp_type_2) RETURNS temp_type_1 AS '
  select foo from test_func where (foo).a = 3;
' LANGUAGE SQL; 

SELECT * FROM test_temp_func((7,8), (5,6));
drop function test_temp_func(temp_type_1, temp_type_2);

-- UDT and UDA
create or replace function test_temp_func_2(temp_type_1, temp_type_1) RETURNS temp_type_1 AS '
  select ($1.a + $2.a, $1.b + $2.b)::temp_type_1;
' LANGUAGE SQL; 

CREATE AGGREGATE agg_comp_type (temp_type_1) (
   sfunc = test_temp_func_2, stype = temp_type_1,
   initcond = '(0,0)'
);
select agg_comp_type(foo) from test_func;

-- Check alter schema
create schema type_test;
alter type temp_type_1 set schema type_test;

\dT temp_type_1
\dT type_test.temp_type_1
\d test_func

select foo from test_func where (foo).a = 3;

-- type name with truncation
create type abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890 as (a int, b int);

create table huge_type_table (a abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890);
insert into huge_type_table values ((1,2));
insert into huge_type_table values ((3,4));

select * from huge_type_table;
\d huge_type_table;

drop table huge_type_table;
drop type abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890;

-- composite type array tests ..negative test
create table type_array_table (col_one type_test.temp_type_1[]);

\c db_testcreatetypecomposite_basictest 
