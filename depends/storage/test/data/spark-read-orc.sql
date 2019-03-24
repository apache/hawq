CREATE EXTERNAL TABLE testorc (i2 short, i4 int, i8 long, f4 float, f8 double, text string) stored as orc LOCATION 'hdfs://localhost:9000/testorc' TBLPROPERTIES ("orc.bloom.filter.columns"="i4")
select * from testorc where i4=3;
select * from testorc where i4=4;
