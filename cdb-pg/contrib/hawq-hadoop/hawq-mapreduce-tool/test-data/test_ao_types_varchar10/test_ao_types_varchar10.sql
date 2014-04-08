DROP TABLE IF EXISTS test_ao_types_varchar10;
CREATE TABLE test_ao_types_varchar10 (c0 varchar(10)) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_varchar10 values ('123456789a'),
('bbccddeeff'),
('aaaa'),
(null);