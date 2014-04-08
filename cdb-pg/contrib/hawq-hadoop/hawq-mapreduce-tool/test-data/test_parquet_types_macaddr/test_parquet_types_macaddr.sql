DROP TABLE IF EXISTS test_parquet_types_macaddr;
CREATE TABLE test_parquet_types_macaddr (c0 macaddr) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_macaddr values ('FF:89:71:45:AE:01'),
(null);