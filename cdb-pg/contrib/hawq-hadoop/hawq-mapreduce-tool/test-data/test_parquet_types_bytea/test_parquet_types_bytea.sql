DROP TABLE IF EXISTS test_parquet_types_bytea;
CREATE TABLE test_parquet_types_bytea (c0 bytea) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_bytea values ('hello world'),
('aaaaaaaaaaaaaaaaaaa'),
(null);