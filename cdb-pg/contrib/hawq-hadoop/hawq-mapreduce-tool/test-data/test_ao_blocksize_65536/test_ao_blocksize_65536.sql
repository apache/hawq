DROP TABLE IF EXISTS test_ao_blocksize_65536;
CREATE TABLE test_ao_blocksize_65536 (c0 int8) with (appendonly=true, orientation=row, compresstype=none, blocksize=65536, checksum=false, compresslevel=0);
INSERT INTO test_ao_blocksize_65536 SELECT * FROM generate_series(1, 65536);