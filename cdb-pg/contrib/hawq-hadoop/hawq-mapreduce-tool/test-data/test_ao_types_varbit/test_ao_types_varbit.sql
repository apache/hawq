DROP TABLE IF EXISTS test_ao_types_varbit;
CREATE TABLE test_ao_types_varbit (c0 varbit) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_varbit values ('1111'),
('000000000011100'),
(null);