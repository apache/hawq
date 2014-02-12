drop table if exists int4_1column
create table int4_1column (c0 int4)WITH (APPENDONLY=true, ORIENTATION=ROW, COMPRESSTYPE=ZLIB, COMPRESSLEVEL=5);
insert into int4_1column values (128);
insert into int4_1column values (null);
insert into int4_1column values (null);
insert into int4_1column values (32767);
