DROP TABLE IF EXISTS test_parquet_rowgroupsize_1048576;
CREATE TABLE test_parquet_rowgroupsize_1048576 (c0 int8) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=1048576, pagesize=524288, compresslevel=0);
INSERT INTO test_parquet_rowgroupsize_1048576 SELECT * FROM generate_series(1, 524288);