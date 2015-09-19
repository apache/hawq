DROP TABLE IF EXISTS test_ao_types_bit5;
CREATE TABLE test_ao_types_bit5 (c0 bit(5)) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_bit5 values ('10010'),
('00010'),
(null);