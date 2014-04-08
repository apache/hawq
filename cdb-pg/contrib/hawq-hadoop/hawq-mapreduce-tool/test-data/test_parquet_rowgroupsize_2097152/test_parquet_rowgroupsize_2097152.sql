DROP TABLE IF EXISTS test_parquet_rowgroupsize_2097152;
CREATE TABLE test_parquet_rowgroupsize_2097152 (c0 int8) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=2097152, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_rowgroupsize_2097152 SELECT * FROM generate_series(1, 1048576);