DROP TABLE IF EXISTS test_ao_types_int2;
CREATE TABLE test_ao_types_int2 (c0 int2) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_int2 values (-32768),
(-128),
(0),
(128),
(32767),
(null);