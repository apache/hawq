DROP TABLE IF EXISTS test_ao_types_timestamp;
CREATE TABLE test_ao_types_timestamp (c0 timestamp) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_timestamp values ('4713-01-01 BC'),
('2942-12-31 AD'),
('1999-01-08 04:05:06'),
('2004-10-19 10:23:54'),
(null);