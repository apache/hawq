DROP TABLE IF EXISTS test_ao_types_macaddr;
CREATE TABLE test_ao_types_macaddr (c0 macaddr) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_macaddr values ('FF:89:71:45:AE:01'),
(null);