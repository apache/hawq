-- @description parquet insert vary pagesize/rowgroupsize 
-- @created 2013-08-09 20:33:16
-- @modified 2013-08-09 20:33:16
-- @tags HAWQ parquet

--start_ignore
drop table if exists pTable1;
drop table if exists pTable3_from;
drop table if exists pTable3_to;
drop table if exists pTable4_from;
drop table if exists pTable4_to;
drop table if exists pTable5_from;
drop table if exists pTable5_to;
--end_ignore

--value/record size equal to pagesize/rowgroupsize
create table pTable1 (a1 char(10485760), a2 char(10485760), a3 char(10485760), a4 char(10485760), a5 char(10485760), a6 char(10485760), a7 char(10485760), a8 char(10485760), a9 char(10485760), a10 char(10485760)) with(appendonly=true, orientation=parquet, pagesize=10485760, rowgroupsize=104857600);

insert into pTable1 values ( ('a'::char(10485760)), ('a'::char(10485760)), ('a'::char(10485760)), ('a'::char(10485760)), ('a'::char(10485760)), ('a'::char(10485760)), ('a'::char(10485760)), ('a'::char(10485760)), ('a'::char(10485760)), ('a'::char(10485760)) );

--single column, one data page contains several values, one rwo group contains several groups
create table pTable3_from ( a1 text ) with(appendonly=true, orientation=parquet);
insert into pTable3_from values(repeat('parquet',100));
insert into pTable3_from values(repeat('parquet',20));
insert into pTable3_from values(repeat('parquet',30));
create table pTable3_to ( a1 text ) with(appendonly=true, orientation=parquet, pagesize=1024, rowgroupsize=1025);
insert into pTable3_to select * from pTable3_from;
select count(*) from pTable3_to;

--multiple columns, multiple rows combination
create table pTable4_from ( a1 text , a2 text) with(appendonly=true, orientation=parquet);
insert into pTable4_from values(repeat('parquet',200), repeat('pq',200));
insert into pTable4_from values(repeat('parquet',50), repeat('pq',200));

create table pTable4_to ( a1 text, a2 text ) with(appendonly=true, orientation=parquet, pagesize=2048, rowgroupsize=4096);
insert into pTable4_to select * from pTable4_from;
select count(*) from pTable4_to;

--large data insert, several column values in one page, several rows in one rowgroup
create table pTable5_from (a1 char(1048576), a2 char(2048576), a3 char(3048576), a4 char(4048576), a5 char(5048576), a6 char(6048576), a7 char(7048576), a8 char(8048576), a9 char(9048576), a10 char(9)) with(appendonly=true, orientation=parquet, pagesize=10485760, rowgroupsize=90874386);
insert into pTable5_from values ( ('a'::char(1048576)), ('a'::char(2048576)), ('a'::char(3048576)), ('a'::char(4048576)), ('a'::char(5048576)), ('a'::char(6048576)), ('a'::char(7048576)), ('a'::char(8048576)), ('a'::char(9048576)), ('a'::char(9)) );

create table pTable5_to (a1 char(1048576), a2 char(2048576), a3 char(3048576), a4 char(4048576), a5 char(5048576), a6 char(6048576), a7 char(7048576), a8 char(8048576), a9 char(9048576), a10 char(9)) with(appendonly=true, orientation=parquet, pagesize=10485760, rowgroupsize=17437200);
insert into pTable5_to select * from pTable5_from;
select count(a10) from pTable5_to;