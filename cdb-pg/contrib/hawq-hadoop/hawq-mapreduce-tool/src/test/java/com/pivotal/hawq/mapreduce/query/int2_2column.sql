drop table if exists int2_2column
create table int2_2column (c0 int2,c1 int2);
insert into int2_2column values (-32768, -32768);
insert into int2_2column values (32767, 0);
insert into int2_2column values (128, -128);
insert into int2_2column values (128, -32768);
