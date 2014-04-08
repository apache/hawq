DROP TABLE IF EXISTS test_ao_types_interval;
CREATE TABLE test_ao_types_interval (c0 interval) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_interval values ('-178000000 years'),
('178000000 years'),
(null);