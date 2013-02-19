--
-- alter object set schema
--

create schema alter1;
create schema alter2;

create table alter1.t1(f1 serial, f2 int check (f2 > 0));
create view alter1.v1 as select * from alter1.t1;
insert into alter1.t1(f2) values(11);
insert into alter1.t1(f2) values(12);
alter table alter1.t1 set schema alter2;
alter table alter1.v1 set schema alter2;
select * from alter2.t1;
select * from alter2.v1;

create function alter1.plus1(int) returns int as 'select $1+1' language sql;
alter function alter1.plus1(int) set schema alter2;
create domain alter1.posint integer check (value > 0);
alter domain alter1.posint set schema alter2;
create type alter1.ctype as (f1 int, f2 text);
alter type alter1.ctype set schema alter2;

drop schema alter1 cascade;
drop schema alter2 cascade;
