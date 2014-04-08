DROP TABLE IF EXISTS test_parquet_types_date;
CREATE TABLE test_parquet_types_date (c0 date) with (appendonly=true, orientation=parquet, compresstype=none, rowgroupsize=8388608, pagesize=1048576, compresslevel=0);
INSERT INTO test_parquet_types_date values ('4713-01-01 BC'),
('2014-03-02'),
('4277-12-31 AD'),
(null);