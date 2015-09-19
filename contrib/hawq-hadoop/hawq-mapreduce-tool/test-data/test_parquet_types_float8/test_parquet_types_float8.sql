DROP TABLE IF EXISTS test_parquet_types_float8;
CREATE TABLE test_parquet_types_float8 (c0 float8) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_float8 values (-123.456),
(-12.8765),
(0),
(1.00),
(2.001),
(128),
(1298765),
(999999),
(12345),
('Infinity'),
('-Infinity'),
('NaN'),
(null);