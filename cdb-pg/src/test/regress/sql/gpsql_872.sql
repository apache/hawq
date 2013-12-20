CREATE OR REPLACE FUNCTION find_num_segno
(
  table_name character varying
)
RETURNS integer AS
$$
DECLARE
  oid integer;
  distinct_count integer;
BEGIN
EXECUTE 'select oid from pg_class where relname = ''' || table_name || '''' into oid;
EXECUTE 'select count(distinct segno) from pg_aoseg.pg_aoseg_' || oid into distinct_count;
return distinct_count;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION find_num_segno_co
(
  table_name character varying
)
RETURNS integer AS
$$
DECLARE
  oid integer;
  distinct_count integer;
BEGIN
EXECUTE 'select oid from pg_class where relname = ''' || table_name || '''' into oid;
EXECUTE 'select count(distinct segno) from pg_aoseg.pg_aocsseg_' || oid into distinct_count;
return distinct_count;
END;
$$ LANGUAGE PLPGSQL;

--Testcase 1: (AO alter table test)

create table t1(c1 int, c2 int);
insert into t1 values(1,1);
insert into t1 values(2,2);
alter table t1 alter column c1 type int;
insert into t1 values(3,3);
insert into t1 values(4,4);
select * from t1 order by c1, c2;
select find_num_segno('t1');
insert into t1 select * from t1;
select * from t1 order by c1, c2;
drop table t1;

--Testcase 2: (AO concurrent insert test)

create table test (a int);
\! echo "insert into test select i from generate_series(1,10)i;" >> out1.sql; gptorment.pl -connect '-p $PGPORT -d regression' -sqlfile out1.sql -parallel 3 &> /dev/null
select find_num_segno('test');
select * from test where a = 1;
insert into test select * from test;
select * from test where a = 1;
drop table test;
\! rm out1.sql

--Testcase 3: (AOCO alter table test)

create table t1(c1 int, c2 int) with (appendonly = true, orientation = column);
insert into t1 values(1,1);
insert into t1 values(2,2);
alter table t1 alter column c1 type int;
insert into t1 values(3,3);
insert into t1 values(4,4);
select * from t1 order by c1, c2;
select find_num_segno_co('t1');
insert into t1 select * from t1;
select * from t1 order by c1, c2;
drop table t1;

--Testcase 4: (AOCO conçurrent insert test)

create table test (a int, b int) with (appendonly = true, orientation = column);
\! echo "insert into test select i,i  from generate_series(1,10)i;" >> out2.sql; gptorment.pl -connect '-p $PGPORT -d regression' -sqlfile out2.sql -parallel 3 &> /dev/null
select find_num_segno_co('test');
select * from test where a = 1 order by a, b;
insert into test select * from test;
select * from test where a = 1 order by a, b;
drop table test;
\! rm out2.sql

--Testcase 5: (copy command test)

create table test2 (a int);
insert into test2 select i from generate_series(1, 10)i;
copy test2 TO '/tmp/test2.csv' CSV;
\! echo "copy test2 from '/tmp/test2.csv';" >> out3.sql; gptorment.pl -connect '-p $PGPORT -d regression' -sqlfile out3.sql -parallel 3 &> /dev/null
select find_num_segno('test2');
select * from test2 where a = 1;
insert into test2 select * from test2;
select * from test2 where a = 1;
drop table test2;
\! rm out3.sql /tmp/test2.csv

--Testcase 6: (partition table test)

create table pt_table(a int, b int) distributed by (a) partition by range(b) (default partition others,start(1) end(100) every(10));
\! echo "insert into pt_table select i,i from generate_series(1,100)i;" >> out4.sql; gptorment.pl -connect '-p $PGPORT -d regression' -sqlfile out4.sql -parallel 3 &> /dev/null 
select * from pt_table where a = 1 order by a, b;
select find_num_segno('pt_table');
insert into pt_table select * from pt_table;
select * from pt_table where a = 1 order by a, b;
drop table pt_table;
\! rm out4.sql

--Testcase 7: (function test)

create table func (a int, b int);
insert into func values(1,1);
insert into func values(2,2);
alter table func alter column a type int;
insert into func values(3,3);
insert into func values(4,4);
select * from func order by a, b;
select find_num_segno('func');
CREATE OR REPLACE FUNCTION table_insert
(
  table_name character varying
)
RETURNS integer AS
$$
DECLARE
  number_of_rows integer;
BEGIN
EXECUTE 'INSERT INTO ' || table_name || ' SELECT * FROM ' || table_name;
EXECUTE 'SELECT count(*) FROM ' || table_name INTO number_of_rows;
return number_of_rows;
END;
$$ LANGUAGE PLPGSQL;
select table_insert('func');
select * from func order by a, b;
drop table func;
