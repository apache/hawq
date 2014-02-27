--start_ignore
drop table if exists parquet_table_tobe_ctas;
drop table if exists parquet_ctas_table1;
DROP TABLE if exists parquet_alter cascade;
DROP TABLE if exists parquet_alter_table cascade;
Drop Table if exists parquet_alter_table_exch;
DROP SCHEMA if exists mytest cascade;
--end_ignore

CREATE TABLE parquet_table_tobe_ctas(text_col text, bigint_col bigint, char_vary_col character varying(30), numeric_col numeric, int_col int4, float_col float4, before_rename_col int4, change_datatype_col numeric, a_ts_without timestamp without time zone, b_ts_with timestamp with time zone, date_column date, col_set_default numeric) with (appendonly=true, orientation=parquet) DISTRIBUTED RANDOMLY;

CREATE TABLE parquet_ctas_table1 with (appendonly=true, orientation=parquet) AS SELECT text_col,bigint_col,char_vary_col,numeric_col FROM parquet_table_tobe_ctas;


CREATE TABLE parquet_alter
    (id SERIAL,a1 int,a2 char(5),a3 numeric,a4 boolean DEFAULT false ,a5 char DEFAULT 'd',a6 text,a7 timestamp,a8 character varying(705),a9 bigint,a10 date)
    WITH (appendonly=true, orientation=parquet,compresstype=gzip,compresslevel=1) distributed randomly Partition by range(a1) (start(1)  end(16) every(8)
    WITH (appendonly=true, orientation=parquet,compresstype=snappy));


INSERT INTO parquet_alter(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) values(generate_series(1,5),'M',2011,'t','a','This is news of today: Deadlock between Republicans and Democrats over how best to reduce the U.S. deficit, and over what period, has blocked an agreement to allow the raising of the $14.3 trillion debt ceiling','2001-12-24 02:26:11','U.S. House of Representatives Speaker John Boehner, the top Republican in Congress who has put forward a deficit reduction plan to be voted on later on Thursday said he had no control over whether his bill would avert a credit downgrade.',generate_series(2490,2495),'2011-10-11');

alter table parquet_alter add partition new_p start(17) end (20) WITH (appendonly=true, orientation=parquet, compresstype=snappy);

alter table parquet_alter add default partition df_p WITH (appendonly=true, orientation=parquet, compresstype=gzip, compresslevel=3);

INSERT INTO parquet_alter(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) values(generate_series(6,25),'F',2010,'f','b','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child','2001-12-25 02:22:11','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child',generate_series(2500,2501),'2011-10-12');

ALTER TABLE ONLY parquet_alter RENAME TO parquet_alter_table;

INSERT INTO parquet_alter_table(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) values(generate_series(26,28),'F',2010,'f','b','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child','2001-12-25 02:22:11','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child',generate_series(2502,2503),'2011-10-12');

select count(*) from parquet_alter_table;

alter table parquet_alter_table rename partition df_p to df_p1;

alter table parquet_alter_table truncate partition df_p1;

alter table parquet_alter_table drop partition new_p;

INSERT INTO parquet_alter_table(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) values(generate_series(1,28),'M',2010,'f','b','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child','2001-12-25 02:22:11','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child',2500,'2011-10-12');

select count(*) from parquet_alter_table;

CREATE TABLE parquet_alter_table_exch(id SERIAL,a1 int,a2 char(5),a3 numeric,a4 boolean DEFAULT false ,a5 char DEFAULT 'd',a6 text,a7 timestamp,a8 character varying(705),a9 bigint,a10 date) WITH (appendonly=true, orientation=parquet, compresstype=snappy)  distributed randomly;

Insert into parquet_alter_table_exch(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) values(generate_series(29,30),'F',2011,'t','a','dfjjjjjj','2001-12-24 02:26:11','hghgh',333,'2011-10-11');

Alter table parquet_alter_table exchange default partition with table parquet_alter_table_exch;

Alter table parquet_alter_table split partition FOR (RANK(2)) at(10) into (partition splitc,partition splitd);

INSERT INTO parquet_alter_table(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) values(generate_series(1,30),'F',2011,'f','b','If the scale of a value to be stored is greater than the declared scale of the column, the system will round the value to the specified number of fractional digits. Then, if the number of digits to the left of the decimal point ','2001-11-24 02:26:11','bbffruikjjjk89kjhjjdsdsjflsdkfjowikfo;lwejkufhekusgfuyewhfkdhsuyfgjsdbfjhkdshgciuvhdskfhiewyerwhkjehriur687687rt3ughjgd67567tcghjzvcnzfTYr7tugfTRE#$#^%*GGHJFTEW#RUYJBJHCFDGJBJGYrythgfT^&^tjhE655ugHD655uVtyr%^uygUYT&^R%^FJYFHGF',2802,'2011-11-12');

select count(*) from parquet_alter_table;

Create schema mytest;
ALTER TABLE parquet_alter_table SET SCHEMA mytest;
\d+ parquet_alter_table
set search_path=mytest;
\d+ parquet_alter_table

Alter table parquet_alter_table SET DISTRIBUTED BY (a9);
INSERT INTO parquet_alter_table(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) values(31,'F',2011,'f','b','If the scale of a value to be stored is greater than the declared scale of the column, the system will round the value to the specified number of fractional digits. Then, if the number of digits to the left of the decimal point ','2001-11-24 02:26:11','bbffruikjjjk89kjhjjdsdsjflsdkfjowikfo;lwejkufhekusgfuyewhfkdhsuyfgjsdbfjhkdshgciuvhdskfhiewyerwhkjehriur687687rt3ughjgd67567tcghjzvcnzfTYr7tugfTRE#$#^%*GGHJFTEW#RUYJBJHCFDGJBJGYrythgfT^&^tjhE655ugHD655uVtyr%^uygUYT&^R%^FJYFHGF',2802,'2011-11-12');

select count(*) from parquet_alter_table;

Alter table parquet_alter_table SET WITH (REORGANIZE=true);

INSERT INTO parquet_alter_table(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) values(32,'F',2011,'f','b','If the scale of a value to be stored is greater than the declared scale of the column, the system will round the value to the specified number of fractional digits. Then, if the number of digits to the left of the decimal point ','2001-11-24 02:26:11','bbffruikjjjk89kjhjjdsdsjflsdkfjowikfo;lwejkufhekusgfuyewhfkdhsuyfgjsdbfjhkdshgciuvhdskfhiewyerwhkjehriur687687rt3ughjgd67567tcghjzvcnzfTYr7tugfTRE#$#^%*GGHJFTEW#RUYJBJHCFDGJBJGYrythgfT^&^tjhE655ugHD655uVtyr%^uygUYT&^R%^FJYFHGF',2802,'2011-11-12');

select count(*) from parquet_alter_table;

ALTER TABLE parquet_alter_table ADD CONSTRAINT mychk CHECK (a1 <= 35);

