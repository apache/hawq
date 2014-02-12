drop table if exists int2_1column
create table int2_1column (c0 int2) WITH (APPENDONLY=true, ORIENTATION=ROW, CHECKSUM=TRUE);
insert into int2_1column values (-32768);
insert into int2_1column values (0);
insert into int2_1column values (0);
