DROP TABLE IF EXISTS test_ao_types_polygon;
CREATE TABLE test_ao_types_polygon (c0 polygon) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_polygon values ('((1,1),(3,2),(4,5))'),
('((100,123),(5,10),(7,2),(4,5))'),
(null);