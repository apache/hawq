drop table if exists int8_2column
create table int8_2column (c0 int8,c1 int8);
insert into int8_2column values (128, 9223372036854775807);
insert into int8_2column values (-128, -32768);
insert into int8_2column values (128, -9223372036854775808);
insert into int8_2column values (-2147483648, -9223372036854775808);
insert into int8_2column values (-2147483648, 9223372036854775807);
