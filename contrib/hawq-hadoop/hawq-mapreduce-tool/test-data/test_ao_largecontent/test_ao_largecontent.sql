DROP TABLE IF EXISTS test_ao_largecontent;
CREATE TABLE test_ao_largecontent (c0 text) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_largecontent values (repeat('b', 40000));