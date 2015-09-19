DROP TABLE IF EXISTS test_parquet_types_box;
CREATE TABLE test_parquet_types_box (c0 box) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_box values ('((0,1),(2,3))'),
('((100,200),(200,400))'),
(null);