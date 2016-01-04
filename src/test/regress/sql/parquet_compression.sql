-- @description parquet compression test
-- @created 2013-08-09 19:57:38
-- @modified 2013-08-09 19:57:38
-- @tags HAWQ parquet

--start_ignore
drop schema parquet cascade;
create schema parquet;
set search_path=parquet,"$user",public;
set default_segment_num = 4;

drop table parquet_gzip;
drop table parquet_gzip_uncompr;
drop table parquet_gzip_part;
drop table parquet_gzip_part_unc;
drop table parquet_snappy;
drop table parquet_snappy_uncompr;
drop table parquet_snappy_part;
drop table parquet_snappy_part_unc;
drop table parquet_gzip_2;

alter resource queue pg_default with ( vseg_resource_quota='mem:2gb');
--set statement_mem='1999MB';
--end_ignore

--Datatypes covered: text,bytea,varchar,bit varying

-- parquet table ,compresstype = gzip

create table parquet_gzip (i int,document text not null, vch1 varchar,bta1 bytea, bitv1 varbit)
      WITH (appendonly=true, orientation=parquet, compresstype=gzip, pagesize=150000000, rowgroupsize= 500000000);

insert into parquet_gzip  values(1001,repeat('large data value for text data type',32732),repeat('large data value for text data type',32732),pg_catalog.decode(repeat('large data value for text data type',32732), 'escape'),1000::int::bit(3244447)::varbit);

-- Uncompressed table and join with compressed to see if select works

create table parquet_gzip_uncompr(i int,document text not null, vch1 varchar,bta1 bytea, bitv1 varbit)
      WITH (appendonly=true, orientation=parquet, pagesize=150000000, rowgroupsize= 500000000);

insert into parquet_gzip_uncompr values(1001,repeat('large data value for text data type',32732),repeat('large data value for text data type',32732),pg_catalog.decode(repeat('large data value for text data type',32732), 'escape'),1000::int::bit(3244447)::varbit);

select count(*) from parquet_gzip c1 full outer join parquet_gzip_uncompr c2 on c1.document=c2.document and c1.vch1=c2.vch1 and c1.bta1=c2.bta1 and c1.bitv1=c2.bitv1;

--select get_ao_compression_ratio('parquet_gzip');

-- parquet partition table with hybrid partitions

create table parquet_gzip_part (p1 int,document text not null, vch1 varchar,bta1 bytea, bitv1 varbit)
      WITH (appendonly=true, orientation=parquet, compresstype=gzip, pagesize=150000000, rowgroupsize= 500000000)
      Partition by range(p1)
      (Partition p1 start(1) end(4) with (appendonly=true),
       Partition p2 start(4) end(7) with (appendonly=true),
       Partition p3 start(7) end(11) with (appendonly=true,orientation=parquet,compresstype=gzip, pagesize=150000000, rowgroupsize= 500000000));

insert into parquet_gzip_part values(generate_series(1,5), repeat('large data value for text data type',32732),repeat('large data value for text data type',32732),pg_catalog.decode(repeat('large data value for text data type',32732), 'escape'),1000::int::bit(3244447)::varbit);

-- Uncompressed table and join with compressed to see if select works

create table parquet_gzip_part_unc (p1 int,document text not null, vch1 varchar,bta1 bytea, bitv1 varbit)
      WITH (appendonly=true, orientation=parquet, pagesize=150000000, rowgroupsize= 500000000)
      Partition by range(p1)
      (Partition p1 start(1) end(4) with (appendonly=true),
       Partition p2 start(4) end(7) with (appendonly=true),
       Partition p3 start(7) end(11) with (appendonly=true,orientation=parquet, pagesize=150000000, rowgroupsize= 500000000));

insert into parquet_gzip_part_unc values(generate_series(1,5), repeat('large data value for text data type',32732),repeat('large data value for text data type',32732),pg_catalog.decode(repeat('large data value for text data type',32732), 'escape'),1000::int::bit(3244447)::varbit);

select count(*) from parquet_gzip_part c1 full outer join parquet_gzip_part_unc c2 on c1.p1=c2.p1 and c1.document=c2.document and c1.vch1=c2.vch1 and c1.bta1=c2.bta1 and c1.bitv1=c2.bitv1;

--select get_ao_compression_ratio('parquet_gzip_part_1_prt_p3');


-- parquet table , compresstype = snappy

create table parquet_snappy (p1 int, document text not null, vch1 varchar,bta1 bytea, bitv1 varbit)
      WITH (appendonly=true, orientation=parquet, compresstype=snappy, pagesize=150000000, rowgroupsize= 500000000);

insert into parquet_snappy values(1001, repeat('large data value for text data type',32732),repeat('large data value for text data type',32732),pg_catalog.decode(repeat('large data value for text data type',32732), 'escape'),1000::int::bit(3244447)::varbit);

-- Uncompressed table and join with compressed to see if select works

create table parquet_snappy_uncompr(p1 int, document text not null, vch1 varchar,bta1 bytea, bitv1 varbit)
      WITH (appendonly=true, orientation=parquet, pagesize=150000000, rowgroupsize= 500000000);

insert into parquet_snappy_uncompr values(1001,repeat('large data value for text data type',32732),repeat('large data value for text data type',32732),pg_catalog.decode(repeat('large data value for text data type',32732), 'escape'),1000::int::bit(3244447)::varbit);

select count(*) from parquet_snappy c1 full outer join parquet_snappy_uncompr c2 on c1.p1=c2.p1 and c1.document=c2.document and c1.vch1=c2.vch1 and c1.bta1=c2.bta1 and c1.bitv1=c2.bitv1;

--select get_ao_compression_ratio('parquet_snappy');

-- parquet partition table with hybrid partitions


create table parquet_snappy_part (p1 int,document text not null, vch1 varchar,bta1 bytea, bitv1 varbit)
      WITH (appendonly=true, orientation=parquet, compresstype=snappy, pagesize=150000000, rowgroupsize= 500000000)
      Partition by range(p1)
      (Partition p1 start(1) end(4) with (appendonly=true),
       Partition p2 start(4) end(7) with (appendonly=true),
       Partition p3 start(7) end(11) with (appendonly=true,orientation=parquet,compresstype=snappy, pagesize=150000000, rowgroupsize= 500000000));

insert into parquet_snappy_part values(generate_series(1,5), repeat('large data value for text data type',32732),repeat('large data value for text data type',32732),pg_catalog.decode(repeat('large data value for text data type',32732), 'escape'),1000::int::bit(3244447)::varbit);

-- Uncompressed table and join with compressed to see if select works

create table parquet_snappy_part_unc (p1 int,document text not null, vch1 varchar,bta1 bytea, bitv1 varbit)
      WITH (appendonly=true, orientation=parquet, pagesize=150000000, rowgroupsize= 500000000)
      Partition by range(p1)
      (Partition p1 start(1) end(4) with (appendonly=true),
       Partition p2 start(4) end(7) with (appendonly=true),
       Partition p3 start(7) end(11) with (appendonly=true,orientation=parquet, pagesize=150000000, rowgroupsize= 500000000));

insert into parquet_snappy_part_unc values(generate_series(1,5), repeat('large data value for text data type',32732),repeat('large data value for text data type',32732),pg_catalog.decode(repeat('large data value for text data type',32732), 'escape'),1000::int::bit(3244447)::varbit);

select count(*) from parquet_snappy_part c1 full outer join parquet_snappy_part_unc c2 on c1.p1=c2.p1 and c1.document=c2.document and c1.vch1=c2.vch1 and c1.bta1=c2.bta1 and c1.bitv1=c2.bitv1;

--select get_ao_compression_ratio('parquet_snappy_part_1_prt_p3');
create table parquet_gzip_2 (i int,document text not null, vch1 varchar,bta1 bytea, bitv1 varbit)
      WITH (appendonly=true, orientation=parquet, compresstype=gzip, pagesize=150000000, rowgroupsize= 500000000);

insert into parquet_gzip_2 values(12,array_to_string(ARRAY(SELECT chr((65 + round(random() * 25)) :: integer) FROM generate_series(1, 32732)), ''),array_to_string(ARRAY(SELECT chr((65 + round(random() * 25)) :: integer) FROM generate_series(1, 32732)), ''),pg_catalog.decode(array_to_string(ARRAY(SELECT chr((65 + round(random() * 25)) :: integer) FROM generate_series(1, 32732)), ''),'escape'),1010110::int::bit(3244447)::varbit);


Select count(*) from parquet_gzip_2;

alter resource queue pg_default with ( vseg_resource_quota='mem:256mb');
