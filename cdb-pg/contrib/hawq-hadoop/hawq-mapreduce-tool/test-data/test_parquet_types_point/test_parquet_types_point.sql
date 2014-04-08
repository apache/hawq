DROP TABLE IF EXISTS test_parquet_types_point;
CREATE TABLE test_parquet_types_point (c0 point) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_point values (POINT(1,2)),
(POINT(100,200)),
(POINT(1000,2000)),
(null);