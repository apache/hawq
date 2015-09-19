DROP TABLE IF EXISTS test_ao_types_circle;
CREATE TABLE test_ao_types_circle (c0 circle) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_circle values ('<(1,2),3>'),
('<(100,200),300>'),
(null);