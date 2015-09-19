DROP TABLE IF EXISTS test_ao_types_text;
CREATE TABLE test_ao_types_text (c0 text) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_text values (''),
('z'),
('hello world'),
(repeat('b',1000)),
(null);