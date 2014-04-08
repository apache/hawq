DROP TABLE IF EXISTS test_ao_blocksize_16384;
CREATE TABLE test_ao_blocksize_16384 (c0 int8) with (appendonly=true, orientation=row, compresstype=none, blocksize=16384, checksum=false, compresslevel=0);
INSERT INTO test_ao_blocksize_16384 SELECT * FROM generate_series(1, 16384);