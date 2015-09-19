DROP TABLE IF EXISTS test_ao_types_date;
CREATE TABLE test_ao_types_date (c0 date) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_date values ('4713-01-01 BC'),
('2014-03-02'),
('4277-12-31 AD'),
(null);