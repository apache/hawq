DROP TABLE IF EXISTS test_ao_blocksize_8192;
CREATE TABLE test_ao_blocksize_8192 (c0 int8) with (appendonly=true, orientation=row, compresstype=none, blocksize=8192, checksum=false, compresslevel=0);
INSERT INTO test_ao_blocksize_8192 SELECT * FROM generate_series(1, 8192);