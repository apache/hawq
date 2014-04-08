DROP TABLE IF EXISTS test_parquet_types_float4;
CREATE TABLE test_parquet_types_float4 (c0 float4) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_float4 values (-12.3456),
(0),
(1.00),
(2.001),
(9999.1234),
(123456),
('Infinity'),
('-Infinity'),
('NaN'),
(null);