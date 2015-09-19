DROP TABLE IF EXISTS test_parquet_pagesize_524288;
CREATE TABLE test_parquet_pagesize_524288 (c0 int8) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=524288, compresslevel=0);
INSERT INTO test_parquet_pagesize_524288 SELECT * FROM generate_series(1, 524288);