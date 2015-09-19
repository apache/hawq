-- turn off autostats so we don't have to worry about the logging of the autostat queries
set gp_autostats_mode = None;

-- create needed tables
create table direct_test
(
  key int NULL,
  value varchar(50) NULL
)
distributed by (key); 

create table direct_test_two_column
(
  key1 int NULL,
  key2 int NULL,
  value varchar(50) NULL
)
distributed by (key1, key2);

create table direct_test_bitmap  as select '2008-02-01'::DATE AS DT,
        case when j <= 996
                then 0
        when j<= 998 then 2
        when j<=999 then 3
        when i%10000 < 9000 then 4
        when i%10000 < 9800 then 5
        when i % 10000 <= 9998 then 5 else 6
        end as ind,
        (i*1017-j)::bigint as s from generate_series(1,10) i, generate_series(1,10) j distributed by (dt);
create index direct_test_bitmap_idx on direct_test_bitmap using bitmap (ind, dt);

CREATE TABLE direct_test_partition (trans_id int, date date, amount decimal(9,2), region text) DISTRIBUTED BY (trans_id) PARTITION BY RANGE (date) (START (date '2008-01-01') INCLUSIVE END (date '2009-01-01') EXCLUSIVE EVERY (INTERVAL '1month') );

create unique index direct_test_uk on direct_test_partition(trans_id);

create table direct_test_range_partition (a int, b int, c int, d int) distributed by (a) partition by range(d) (start(1) end(10) every(1));
insert into direct_test_range_partition select i, i+1, i+2, i+3 from generate_series(1, 2) i;

create table direct_test_type_real (real1 real, smallint1 smallint, boolean1 boolean, int1 int, double1 double precision, date1 date, numeric1 numeric) distributed by (real1);
create table direct_test_type_smallint (real1 real, smallint1 smallint, boolean1 boolean, int1 int, double1 double precision, date1 date, numeric1 numeric) distributed by (smallint1);
create table direct_test_type_boolean (real1 real, smallint1 smallint, boolean1 boolean, int1 int, double1 double precision, date1 date, numeric1 numeric) distributed by (boolean1);
create table direct_test_type_int (real1 real, smallint1 smallint, boolean1 boolean, int1 int, double1 double precision, date1 date, numeric1 numeric) distributed by (int1);
create table direct_test_type_double (real1 real, smallint1 smallint, boolean1 boolean, int1 int, double1 double precision, date1 date, numeric1 numeric) distributed by (double1);
create table direct_test_type_date (real1 real, smallint1 smallint, boolean1 boolean, int1 int, double1 double precision, date1 date, numeric1 numeric) distributed by (date1);
create table direct_test_type_numeric (real1 real, smallint1 smallint, boolean1 boolean, int1 int, double1 double precision, date1 date, numeric1 numeric) distributed by (numeric1);
create table direct_test_type_abstime (x abstime) distributed by (x);
create table direct_test_type_bit (x bit) distributed by (x);
create table direct_test_type_bpchar (x bpchar) distributed by (x);
create table direct_test_type_bytea (x bytea) distributed by (x);
create table direct_test_type_cidr (x cidr) distributed by (x);
create table direct_test_type_inet (x inet) distributed by (x);
create table direct_test_type_macaddr (x macaddr) distributed by (x);
create table direct_test_type_tinterval (x tinterval) distributed by (x);
create table direct_test_type_varbit (x varbit) distributed by (x);

-- enable printing of printing info
set test_print_direct_dispatch_info=on;

-- Constant single-row insert, one column in distribution
-- DO direct dispatch
insert into direct_test values (100, 'cow');
-- verify
select * from direct_test order by key, value;

-- Constant single-row update, one column in distribution
-- DO direct dispatch
update direct_test set value = 'horse' where key = 100;
-- verify
select * from direct_test order by key, value;

-- Constant single-row delete, one column in distribution
-- DO direct dispatch
delete from direct_test where key = 100;
-- verify
select * from direct_test order by key, value;


-- Constant single-row insert, one column in distribution
-- DO direct dispatch
insert into direct_test values (NULL, 'cow');
-- verify
select * from direct_test order by key, value;

-- Constant single-row insert, two columns in distribution
-- DO direct dispatch
insert into direct_test_two_column values (100, 101, 'cow');
-- verify
select * from direct_test_two_column order by key1, key2, value;

-- Constant single-row update, two columns in distribution
-- DO direct dispatch
update direct_test_two_column set value = 'horse' where key1 = 100 and key2 = 101;
-- verify
select * from direct_test_two_column order by key1, key2, value;

-- Constant single-row delete, two columns in distribution
-- DO direct dispatch
delete from direct_test_two_column where key1 = 100 and key2 = 101;
-- verify
select * from direct_test_two_column order by key1, key2, value;

-- expression single-row insert
-- DO direct dispatch
insert into direct_test (key, value) values ('123',123123);
insert into direct_test (key, value) values (sqrt(100*10*10),123123);
--
-- should get 100 and 123 as the values
--
select * from direct_test where value = 123123 order by key;

delete from direct_test where value = 123123;

--------------------------------------------------------------------------------
-- Multiple row update, where clause lists multiple values which hash differently so no direct dispatch
--
-- note that if the hash function for values changes then certain segment configurations may actually 
--                hash all these values to the same content! (and so test would change)
--
update direct_test set value = 'pig' where key in (1,2,3,4,5);

update direct_test_two_column set value = 'pig' where key1 = 100 and key2 in (1,2,3,4);
update direct_test_two_column set value = 'pig' where key1 in (100,101,102,103,104) and key2 in (1);
update direct_test_two_column set value = 'pig' where key1 in (100,101) and key2 in (1,2);

-- Multiple row update, where clause lists values which all hash to same segment
-- DO direct dispatch
-- CAN'T IMPLEMENT THIS TEST BECAUSE THE # of segments changes again (unless we use a # of segments function, and exploit the simple nature of int4 hashing -- can we do that?)


------------------------------
-- Transaction cases
--
-- note that single-row insert can happen BUT DTM will always go to all contents
--
begin;
insert into direct_test values (1,100);
rollback;

begin;
insert into direct_test values (1,100);
insert into direct_test values (2,100);
insert into direct_test values (3,100);
rollback;

-------------------
-- MPP-7634: bitmap index scan
--
select count(*) from direct_test_bitmap where dt='2008-02-05';
select count(*) from direct_test_bitmap where dt='2008-02-01';
----------------------------------------------------------------------------------
-- MPP-7637: partitioned table
--
insert into direct_test_partition values (1,'2008-01-02',1,'usa');
select * from direct_test_partition where trans_id =1;
----------------------------------------------------------------------------------
-- MPP-7638: range table partition
--
select count(*) from direct_test_range_partition where a =1;
----------------------------------------------------------------------------------
-- MPP-7643: various types
--
set optimizer_enable_constant_expression_evaluation=on;
insert into direct_test_type_real values (8,8,true,8,8,'2008-08-08',8.8);
insert into direct_test_type_smallint values (8,8,true,8,8,'2008-08-08',8.8);
insert into direct_test_type_boolean values (8,8,true,8,8,'2008-08-08',8.8);
insert into direct_test_type_int values (8,8,true,8,8,'2008-08-08',8.8);
insert into direct_test_type_double values (8,8,true,8,8,'2008-08-08',8.8);
insert into direct_test_type_date values (8,8,true,8,8,'2008-08-08',8.8);
insert into direct_test_type_numeric values (8,8,true,8,8,'2008-08-08',8.8);
reset optimizer_enable_constant_expression_evaluation;

select * from direct_test_type_real where real1 = 8::real;
select * from direct_test_type_smallint where smallint1 = 8::smallint;
select * from direct_test_type_int where int1 = 8;
select * from direct_test_type_double where double1 = 8;
select * from direct_test_type_date where date1 = '2008-08-08';
select * from direct_test_type_numeric where numeric1 = 8.8;
----------------------------------------------------------------------------------
-- Prepared statements
--  do same as above ones but using prepared statements, verify data goes to the right spot
prepare test_insert (int) as insert into direct_test values ($1,100);
execute test_insert(1);
execute test_insert(2);

select * from direct_test;

prepare test_update (int) as update direct_test set value = 'boo' where key = $1;
execute test_update(2);

select * from direct_test;

------------------------
-- A subquery
--
set test_print_direct_dispatch_info=off;
CREATE TEMP TABLE direct_dispatch_foo (id integer) DISTRIBUTED BY (id);
CREATE TEMP TABLE direct_dispatch_bar (id1 integer, id2 integer) DISTRIBUTED by (id1);

INSERT INTO direct_dispatch_foo VALUES (1);

INSERT INTO direct_dispatch_bar VALUES (1, 1);
INSERT INTO direct_dispatch_bar VALUES (2, 2);
INSERT INTO direct_dispatch_bar VALUES (3, 1);

set test_print_direct_dispatch_info=on;
SELECT * FROM direct_dispatch_foo WHERE id IN
    (SELECT id2 FROM (SELECT DISTINCT id1, id2 FROM direct_dispatch_bar WHERE direct_dispatch_bar.id1 = 1) AS s) ORDER BY 1;
--
-- this one will NOT do direct dispatch because it is a many slice query and those are disabled right now
SELECT * FROM direct_dispatch_foo WHERE id IN
    (SELECT id2 FROM (SELECT id1, id2 FROM direct_dispatch_bar WHERE direct_dispatch_bar.id1 = 1 UNION
                      SELECT id1, id2 FROM direct_dispatch_bar WHERE direct_dispatch_bar.id1 = 2) AS s) ORDER BY 1;

-- simple one using an expression on the variable
SELECT * from direct_dispatch_foo WHERE id * id = 1;
SELECT * from direct_dispatch_foo WHERE id * id = 1 OR id = 1;
SELECT * from direct_dispatch_foo where id * id = 1 AND id = 1;

-- init plan to see how transaction escalation happens
delete from direct_dispatch_foo where id = (select max(id2) from direct_dispatch_bar where id1 = 5);
delete from direct_dispatch_foo where id * id = (select max(id2) from direct_dispatch_bar where id1 = 5) AND id = 3;
delete from direct_dispatch_foo where id * id = (select max(id2) from direct_dispatch_bar) AND id = 3;

------------------------------------
-- more type tests 
--
-- abstime
insert into direct_test_type_abstime values('2008-08-08');
select 1 from direct_test_type_abstime where x = '2008-08-08';

insert into direct_test_type_bit values('1');
select * from direct_test_type_bit where x = '1';

insert into direct_test_type_bpchar values('abs');
select * from direct_test_type_bpchar where x = 'abs';

insert into direct_test_type_bytea values('greenplum');
select * from direct_test_type_bytea where x = 'greenplum';

insert into direct_test_type_cidr values('68.44.55.111');
select * from direct_test_type_cidr where x = '68.44.55.111';

insert into direct_test_type_inet values('68.44.55.111');
select * from direct_test_type_inet where x = '68.44.55.111';

insert into direct_test_type_macaddr values('12:34:56:78:90:ab');
select * from direct_test_type_macaddr where x = '12:34:56:78:90:ab';

insert into direct_test_type_tinterval values('["2008-08-08" "2010-10-10"]');
select 1 from direct_test_type_tinterval where x = '["2008-08-08" "2010-10-10"]';

insert into direct_test_type_varbit values('0101010');
select * from direct_test_type_varbit where x = '0101010';

------------------------------------
-- int28, int82, etc. checks
set test_print_direct_dispatch_info=off;
CREATE TABLE direct_test_type_int2 (id int2) DISTRIBUTED BY (id);
CREATE TABLE direct_test_type_int4 (id int4) DISTRIBUTED BY (id);
CREATE TABLE direct_test_type_int8 (id int8) DISTRIBUTED BY (id);

INSERT INTO direct_test_type_int2 VALUES (1);
INSERT INTO direct_test_type_int4 VALUES (1);
INSERT INTO direct_test_type_int8 VALUES (1);

set test_print_direct_dispatch_info=on;

SELECT * FROM direct_test_type_int2 WHERE id = 1::int2;
SELECT * FROM direct_test_type_int2 WHERE id = 1::int4;
SELECT * FROM direct_test_type_int2 WHERE id = 1::int8;

SELECT * FROM direct_test_type_int2 WHERE 1::int2 = id;
SELECT * FROM direct_test_type_int2 WHERE 1::int4 = id;
SELECT * FROM direct_test_type_int2 WHERE 1::int8 = id;

SELECT * FROM direct_test_type_int4 WHERE id = 1::int2;
SELECT * FROM direct_test_type_int4 WHERE id = 1::int4;
SELECT * FROM direct_test_type_int4 WHERE id = 1::int8;

SELECT * FROM direct_test_type_int4 WHERE 1::int2 = id;
SELECT * FROM direct_test_type_int4 WHERE 1::int4 = id;
SELECT * FROM direct_test_type_int4 WHERE 1::int8 = id;

SELECT * FROM direct_test_type_int8 WHERE id = 1::int2;
SELECT * FROM direct_test_type_int8 WHERE id = 1::int4;
SELECT * FROM direct_test_type_int8 WHERE id = 1::int8;

SELECT * FROM direct_test_type_int8 WHERE 1::int2 = id;
SELECT * FROM direct_test_type_int8 WHERE 1::int4 = id;
SELECT * FROM direct_test_type_int8 WHERE 1::int8 = id;

-- overflow test
SELECT * FROM direct_test_type_int2 WHERE id = 32768::int4;
SELECT * FROM direct_test_type_int2 WHERE id = -32769::int4;

SELECT * FROM direct_test_type_int2 WHERE 32768::int4 = id;
SELECT * FROM direct_test_type_int2 WHERE -32769::int4 = id;

SELECT * FROM direct_test_type_int2 WHERE id = 2147483648::int8;
SELECT * FROM direct_test_type_int2 WHERE id = -2147483649::int8;
SELECT * FROM direct_test_type_int2 WHERE 2147483648::int8 = id;
SELECT * FROM direct_test_type_int2 WHERE -2147483649::int8 = id;

-- cleanup
set test_print_direct_dispatch_info=off;
drop table direct_test;
drop table direct_test_two_column;
drop table direct_test_bitmap;
drop table direct_test_partition;
drop table direct_test_range_partition;
drop table direct_test_type_real;
drop table direct_test_type_smallint;
drop table direct_test_type_int;
drop table direct_test_type_double;
drop table direct_test_type_date;
drop table direct_test_type_numeric;
drop table direct_dispatch_foo;
drop table direct_dispatch_bar;
drop table direct_test_type_int2;
drop table direct_test_type_int4;
drop table direct_test_type_int8;

drop table direct_test_type_abstime;
drop table direct_test_type_bit;
drop table direct_test_type_bpchar;
drop table direct_test_type_bytea;
drop table direct_test_type_cidr;
drop table direct_test_type_inet;
drop table direct_test_type_macaddr;
drop table direct_test_type_tinterval;
drop table direct_test_type_varbit;



