DROP TABLE IF EXISTS test_parquet_types_polygon;
CREATE TABLE test_parquet_types_polygon (c0 polygon) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_polygon values ('((1,1),(3,2),(4,5))'),
('((100,123),(5,10),(7,2),(4,5))'),
(null);