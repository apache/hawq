DROP TABLE IF EXISTS test_ao_types_path;
CREATE TABLE test_ao_types_path (c0 path) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_path values ('(1,1)'),
('(1,1),(2,3),(4,5)'),
(null);