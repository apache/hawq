--
-- hdfs filesystem
--
--
-- *********************************************************************
-- *********************************************************************
-- test HDFS filesystem.
-- need to create an hdfs filespace named "hdfs_fs" first
--
-- *********************************************************************
-- *********************************************************************

select * from pg_filespace where fsname = 'hdfs_fs';

create tablespace hdfs_ts FILESPACE hdfs_fs;

create database test_hdfs_db TABLESPACE hdfs_ts;
\c test_hdfs_db

create table hdfs_test(a int, b int) with(appendonly=true);

insert into hdfs_test select i, i from generate_series (1,1000) i;

select * from hdfs_test order by a,b limit 10;

-- fault injection
-- err: create table
set gp_filesystem_fault_inject_percent=100;
-- create table hdfs_test_2(a int, b int) with(appendonly=true);

-- good: create table
set gp_filesystem_fault_inject_percent=0;
create table hdfs_test_2(a int, b int) with(appendonly=true);

-- err: insert
-- set gp_filesystem_fault_inject_percent=100;
insert into hdfs_test_2 select i, i from generate_series (1,1000) i;

-- good: insert
set gp_filesystem_fault_inject_percent=0;
insert into hdfs_test_2 select i, i from generate_series (1,1000) i;

-- err: select
set gp_filesystem_fault_inject_percent=100;
select * from hdfs_test_2 order by a,b limit 10;

-- good: select
set gp_filesystem_fault_inject_percent=0;
select * from hdfs_test_2 order by a,b limit 10;

-- end of fault injection
set gp_filesystem_fault_inject_percent=0;

-- err: not empty
drop tablespace hdfs_ts;

-- err: not empty
drop filespace hdfs_fs;

drop table hdfs_test;

\c template1
drop database test_hdfs_db;

drop tablespace hdfs_ts;


