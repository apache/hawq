DROP TABLE IF EXISTS test_parquet_types_bit5;
CREATE TABLE test_parquet_types_bit5 (c0 bit(5)) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_bit5 values ('10010'),
('00010'),
(null);