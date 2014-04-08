DROP TABLE IF EXISTS test_parquet_types_bit;
CREATE TABLE test_parquet_types_bit (c0 bit) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_bit values ('1'),
('0'),
(null);