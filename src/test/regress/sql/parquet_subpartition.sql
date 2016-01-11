
--
-- Drop table if exists
--
--start_ignore
DROP TABLE if exists parquet_wt_subpartgzip7 cascade;

DROP TABLE if exists parquet_wt_subpartgzip7_uncompr cascade;

--end_ignore
--
-- Create table
--
CREATE TABLE parquet_wt_subpartgzip7 
	(id SERIAL,a1 int,a2 char(5),a3 numeric,a4 boolean DEFAULT false ,a5 char DEFAULT 'd',a6 text,a7 timestamp,a8 character varying(705),a9 bigint,a10 date)
 WITH (appendonly=true, orientation=parquet) distributed randomly  Partition by range(a1) Subpartition by list(a2) subpartition template ( default subpartition df_sp, subpartition sp1 values('M') , subpartition sp2 values('F')  
 WITH (appendonly=true, orientation=parquet,compresstype=gzip,compresslevel=7)) (start(1)  end(5000) every(1000) );

--
-- Insert data to the table
--
 INSERT INTO parquet_wt_subpartgzip7(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) values(generate_series(1,20),'M',2011,'t','a','This is news of today: Deadlock between Republicans and Democrats over how best to reduce the U.S. deficit, and over what period, has blocked an agreement to allow the raising of the $14.3 trillion debt ceiling','2001-12-24 02:26:11','U.S. House of Representatives Speaker John Boehner, the top Republican in Congress who has put forward a deficit reduction plan to be voted on later on Thursday said he had no control over whether his bill would avert a credit downgrade.',generate_series(2490,2505),'2011-10-11'); 

 INSERT INTO parquet_wt_subpartgzip7(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) values(generate_series(500,510),'F',2010,'f','b','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child','2001-12-25 02:22:11','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child',generate_series(2500,2516),'2011-10-12'); 


--Create Uncompressed table of same schema definition

CREATE TABLE parquet_wt_subpartgzip7_uncompr(id SERIAL,a1 int,a2 char(5),a3 numeric,a4 boolean DEFAULT false ,a5 char DEFAULT 'd',a6 text,a7 timestamp,a8 character varying(705),a9 bigint,a10 date) WITH (appendonly=true, orientation=parquet) distributed randomly Partition by range(a1) Subpartition by list(a2) subpartition template ( subpartition sp1 values('M') , subpartition sp2 values('F') ) (start(1)  end(5000) every(1000)) ;

--
-- Insert to uncompressed table
--
 INSERT INTO parquet_wt_subpartgzip7_uncompr(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) values(generate_series(1,20),'M',2011,'t','a','This is news of today: Deadlock between Republicans and Democrats over how best to reduce the U.S. deficit, and over what period, has blocked an agreement to allow the raising of the $14.3 trillion debt ceiling','2001-12-24 02:26:11','U.S. House of Representatives Speaker John Boehner, the top Republican in Congress who has put forward a deficit reduction plan to be voted on later on Thursday said he had no control over whether his bill would avert a credit downgrade.',generate_series(2490,2505),'2011-10-11'); 

 INSERT INTO parquet_wt_subpartgzip7_uncompr(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) values(generate_series(500,510),'F',2010,'f','b','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child','2001-12-25 02:22:11','Some students may need time to adjust to school.For most children, the adjustment is quick. Tears will usually disappear after Mommy and  Daddy leave the classroom. Do not plead with your child',generate_series(2500,2516),'2011-10-12'); 

--
-- ********Validation******* 
--
\d+ parquet_wt_subpartgzip7_1_prt_1_2_prt_sp2

--
-- Compression ratio
--
--select 'compression_ratio', get_ao_compression_ratio('parquet_wt_subpartgzip7_1_prt_1_2_prt_sp2'); 

--Select from pg_attribute_encoding to see the table entry 
select parencattnum, parencattoptions from pg_partition_encoding e, pg_partition p, pg_class c  where c.relname = 'parquet_wt_subpartgzip7' and c.oid = p.parrelid and p.oid = e.parencoid order by parencattnum limit 3; 
--
-- Compare data with uncompressed table
--
--
-- Select number of rows from the uncompressed table 
--
SELECT count(*) as count_uncompressed from  parquet_wt_subpartgzip7_uncompr ;
--
-- Select number of rows from the compressed table 
--
SELECT count(*) as count_compressed from  parquet_wt_subpartgzip7;
--
-- Select number of rows using a FULL outer join on all the columns of the two tables 
-- Count should match with above result if the all the rows uncompressed correctly: 
--
Select count(*) as count_join from parquet_wt_subpartgzip7 t1 full outer join parquet_wt_subpartgzip7_uncompr t2 on t1.id=t2.id and t1.a1=t2.a1 and t1.a2=t2.a2 and t1.a3=t2.a3 and t1.a4=t2.a4 and t1.a5=t2.a5 and t1.a6=t2.a6 and t1.a7=t2.a7 and t1.a8=t2.a8 and t1.a9=t2.a9 and t1.a10=t2.a10;
--
-- Truncate the table 
--
TRUNCATE table parquet_wt_subpartgzip7;
--
-- Insert data again 
--
insert into parquet_wt_subpartgzip7 select * from parquet_wt_subpartgzip7_uncompr order by a1;


--Alter table Add Partition 
alter table parquet_wt_subpartgzip7 add partition new_p start(5050) end (6051) WITH (appendonly=true, orientation=parquet, compresstype=gzip, compresslevel=1);

--Validation with psql utility 
  \d+ parquet_wt_subpartgzip7_1_prt_new_p_2_prt_sp1

alter table parquet_wt_subpartgzip7 add default partition df_p ;

--Validation with psql utility 
  \d+ parquet_wt_subpartgzip7_1_prt_df_p_2_prt_sp2


--
-- Compression ratio
--
--select 'compression_ratio', get_ao_compression_ratio('parquet_wt_subpartgzip7_1_prt_new_p_2_prt_sp1'); 

--Alter table Exchange Partition 
--Create a table to use in exchange partition 
Drop Table if exists parquet_wt_subpartgzip7_exch; 
 CREATE TABLE parquet_wt_subpartgzip7_exch(id SERIAL,a1 int,a2 char(5),a3 numeric,a4 boolean DEFAULT false ,a5 char DEFAULT 'd',a6 text,a7 timestamp,a8 character varying(705),a9 bigint,a10 date) WITH (appendonly=true, orientation=parquet, compresstype=gzip)  distributed randomly;
 
Drop Table if exists parquet_wt_subpartgzip7_defexch; 
 CREATE TABLE parquet_wt_subpartgzip7_defexch(id SERIAL,a1 int,a2 char(5),a3 numeric,a4 boolean DEFAULT false ,a5 char DEFAULT 'd',a6 text,a7 timestamp,a8 character varying(705),a9 bigint,a10 date) WITH (appendonly=true, orientation=parquet, compresstype=gzip)  distributed randomly;
 
Insert into parquet_wt_subpartgzip7_exch(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) select a1,a2,a3,a4,a5,a6,a7,a8,a9,a10 from parquet_wt_subpartgzip7 where  a1=10 and a2!='C';

Insert into parquet_wt_subpartgzip7_defexch(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) select a1,a2,a3,a4,a5,a6,a7,a8,a9,a10 from parquet_wt_subpartgzip7 where a1 =10 and a2!='C';

Alter table parquet_wt_subpartgzip7 alter partition FOR (RANK(1)) exchange partition sp1 with table parquet_wt_subpartgzip7_exch;
\d+ parquet_wt_subpartgzip7_1_prt_1_2_prt_sp1



