DROP TABLE IF EXISTS test_parquet_types_time;
CREATE TABLE test_parquet_types_time (c0 time) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_time values ('00:00:00'),
('04:05:06'),
('23:59:59'),
(null);