DROP TABLE IF EXISTS test_parquet_pagesize_262144;
CREATE TABLE test_parquet_pagesize_262144 (c0 int8) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=262144, compresslevel=0);
INSERT INTO test_parquet_pagesize_262144 SELECT * FROM generate_series(1, 262144);