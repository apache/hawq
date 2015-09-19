DROP TABLE IF EXISTS test_parquet_types_timestamp;
CREATE TABLE test_parquet_types_timestamp (c0 timestamp) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_timestamp values ('4713-01-01 BC'),
('2942-12-31 AD'),
('1999-01-08 04:05:06'),
('2004-10-19 10:23:54'),
(null);