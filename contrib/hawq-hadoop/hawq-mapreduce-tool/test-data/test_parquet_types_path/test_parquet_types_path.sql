DROP TABLE IF EXISTS test_parquet_types_path;
CREATE TABLE test_parquet_types_path (c0 path) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_path values ('(1,1)'),
('(1,1),(2,3),(4,5)'),
(null);