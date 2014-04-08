DROP TABLE IF EXISTS test_ao_types_point;
CREATE TABLE test_ao_types_point (c0 point) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_point values (POINT(1,2)),
(POINT(100,200)),
(POINT(1000,2000)),
(null);