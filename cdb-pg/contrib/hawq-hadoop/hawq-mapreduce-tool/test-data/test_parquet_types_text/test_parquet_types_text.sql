DROP TABLE IF EXISTS test_parquet_types_text;
CREATE TABLE test_parquet_types_text (c0 text) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_text values (''),
('z'),
('hello world'),
(repeat('b',1000)),
(null);