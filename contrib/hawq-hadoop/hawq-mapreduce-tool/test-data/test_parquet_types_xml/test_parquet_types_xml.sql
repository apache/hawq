DROP TABLE IF EXISTS test_parquet_types_xml;
CREATE TABLE test_parquet_types_xml (c0 xml) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_xml values ('<aa>bb</aa>'),
('<name>wj</name>'),
(null);