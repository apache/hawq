DROP TABLE IF EXISTS test_ao_types_lseg;
CREATE TABLE test_ao_types_lseg (c0 lseg) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_lseg values ('[(0,0),(6,6)]'),
('[(1,1),(2,2)]'),
(null);