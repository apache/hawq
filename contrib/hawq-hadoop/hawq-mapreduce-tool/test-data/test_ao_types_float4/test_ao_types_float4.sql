DROP TABLE IF EXISTS test_ao_types_float4;
CREATE TABLE test_ao_types_float4 (c0 float4) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_float4 values (-12.3456),
(0),
(1.00),
(2.001),
(9999.1234),
(123456),
('Infinity'),
('-Infinity'),
('NaN'),
(null);