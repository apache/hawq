drop table if exists int8_1column
create table int8_1column (c0 int8) WITH (APPENDONLY=true, ORIENTATION=ROW, BLOCKSIZE=8192);
insert into int8_1column values (128);
insert into int8_1column values (2147483647);
insert into int8_1column values (null);
insert into int8_1column values (0);
insert into int8_1column values (-2147483648);
insert into int8_1column values (128);
insert into int8_1column values (-9223372036854775808);
insert into int8_1column values (-128);
