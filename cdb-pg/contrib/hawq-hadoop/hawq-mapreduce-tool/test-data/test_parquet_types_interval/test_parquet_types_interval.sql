DROP TABLE IF EXISTS test_parquet_types_interval;
CREATE TABLE test_parquet_types_interval (c0 interval) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_interval values ('-178000000 years'),
('178000000 years'),
(null);