DROP TABLE IF EXISTS test_parquet_empty;
CREATE TABLE test_parquet_empty (c0 int4) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
