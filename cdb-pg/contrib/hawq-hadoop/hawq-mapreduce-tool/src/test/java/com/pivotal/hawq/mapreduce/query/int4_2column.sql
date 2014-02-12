drop table if exists int4_2column
create table int4_2column (c0 int4,c1 int4);
insert into int4_2column values (-2147483648, 32767);
insert into int4_2column values (0, 128);
insert into int4_2column values (32767, null);
