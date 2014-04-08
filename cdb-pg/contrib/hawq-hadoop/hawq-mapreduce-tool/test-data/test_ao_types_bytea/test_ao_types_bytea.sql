DROP TABLE IF EXISTS test_ao_types_bytea;
CREATE TABLE test_ao_types_bytea (c0 bytea) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_bytea values ('hello world'),
('aaaaaaaaaaaaaaaaaaa'),
(null);