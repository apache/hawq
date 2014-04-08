DROP TABLE IF EXISTS test_ao_types_xml;
CREATE TABLE test_ao_types_xml (c0 xml) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_xml values ('<aa>bb</aa>'),
('<name>wj</name>'),
(null);