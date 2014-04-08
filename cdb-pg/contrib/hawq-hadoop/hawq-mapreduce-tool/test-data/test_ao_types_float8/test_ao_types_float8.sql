DROP TABLE IF EXISTS test_ao_types_float8;
CREATE TABLE test_ao_types_float8 (c0 float8) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_float8 values (-123.456),
(-12.8765),
(0),
(1.00),
(2.001),
(128),
(1298765),
(999999),
(12345),
('Infinity'),
('-Infinity'),
('NaN'),
(null);