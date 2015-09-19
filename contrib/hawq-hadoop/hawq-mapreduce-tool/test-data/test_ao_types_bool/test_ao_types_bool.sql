DROP TABLE IF EXISTS test_ao_types_bool;
CREATE TABLE test_ao_types_bool (c0 bool) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_bool values ('true'),
('false'),
(null);