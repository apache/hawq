DROP TABLE IF EXISTS test_ao_types_int4_array;
CREATE TABLE test_ao_types_int4_array (c0 int4[]) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_int4_array values ('{1,2,3,4}'),
('{{1,2},{3,4}}'),
('{{{1,2},{3,4}},{{5,6},{7,8}}}'),
(null);