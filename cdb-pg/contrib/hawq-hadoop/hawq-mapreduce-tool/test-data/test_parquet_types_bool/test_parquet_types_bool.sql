DROP TABLE IF EXISTS test_parquet_types_bool;
CREATE TABLE test_parquet_types_bool (c0 bool) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_bool values ('true'),
('false'),
(null);