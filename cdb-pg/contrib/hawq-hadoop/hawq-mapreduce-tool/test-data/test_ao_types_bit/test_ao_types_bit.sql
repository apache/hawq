DROP TABLE IF EXISTS test_ao_types_bit;
CREATE TABLE test_ao_types_bit (c0 bit) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_bit values ('1'),
('0'),
(null);