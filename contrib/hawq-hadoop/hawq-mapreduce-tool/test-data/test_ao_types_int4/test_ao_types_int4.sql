DROP TABLE IF EXISTS test_ao_types_int4;
CREATE TABLE test_ao_types_int4 (c0 int4) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_int4 values (-2147483648),
(-32768),
(-128),
(0),
(128),
(32767),
(2147483647),
(null);