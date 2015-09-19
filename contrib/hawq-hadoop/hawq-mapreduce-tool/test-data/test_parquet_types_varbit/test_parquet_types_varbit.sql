DROP TABLE IF EXISTS test_parquet_types_varbit;
CREATE TABLE test_parquet_types_varbit (c0 varbit) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_varbit values ('1111'),
('000000000011100'),
(null);