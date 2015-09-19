DROP TABLE IF EXISTS test_parquet_types_int2;
CREATE TABLE test_parquet_types_int2 (c0 int2) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_int2 values (-32768),
(-128),
(0),
(128),
(32767),
(null);