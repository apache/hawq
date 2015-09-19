DROP TABLE IF EXISTS test_parquet_types_numeric;
CREATE TABLE test_parquet_types_numeric (c0 numeric) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_numeric values (6.54),
(0.0001),
(null);