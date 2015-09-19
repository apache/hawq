DROP TABLE IF EXISTS test_ao_types_time;
CREATE TABLE test_ao_types_time (c0 time) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_time values ('00:00:00'),
('04:05:06'),
('23:59:59'),
(null);