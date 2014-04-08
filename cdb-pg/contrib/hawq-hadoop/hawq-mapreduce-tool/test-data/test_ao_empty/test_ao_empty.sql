DROP TABLE IF EXISTS test_ao_empty;
CREATE TABLE test_ao_empty (c0 int4) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
