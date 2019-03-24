drop external table if exists testorc;
CREATE WRITABLE EXTERNAL TABLE testorc (i2 int2, i4 int4, i8 int8, f4 float4, f8 float8, text TEXT) LOCATION ('hdfs://localhost:8020/testorc') FORMAT 'orc';
insert INTO testorc select values (1,1,3,5.1,6.1,'aaaaaaa'), (1,2,3,5.1,6.1,'aaaaaaa'), (1,4,3,5.1,6.1,'aaaaaaa'), (1,5,3,5.1,6.1,'aaaaaaa');
