DROP TABLE IF EXISTS test_ao_types_numeric;
CREATE TABLE test_ao_types_numeric (c0 numeric) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_numeric values (6.54),
(0.0001),
(null);