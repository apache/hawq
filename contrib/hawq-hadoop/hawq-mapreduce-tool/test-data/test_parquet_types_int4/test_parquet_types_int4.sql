DROP TABLE IF EXISTS test_parquet_types_int4;
CREATE TABLE test_parquet_types_int4 (c0 int4) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_int4 values (-2147483648),
(-32768),
(-128),
(0),
(128),
(32767),
(2147483647),
(null);