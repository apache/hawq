DROP TABLE IF EXISTS test_ao_types_box;
CREATE TABLE test_ao_types_box (c0 box) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_box values ('((0,1),(2,3))'),
('((100,200),(200,400))'),
(null);