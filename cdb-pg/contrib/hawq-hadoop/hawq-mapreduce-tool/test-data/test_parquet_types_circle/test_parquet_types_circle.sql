DROP TABLE IF EXISTS test_parquet_types_circle;
CREATE TABLE test_parquet_types_circle (c0 circle) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_circle values ('<(1,2),3>'),
('<(100,200),300>'),
(null);