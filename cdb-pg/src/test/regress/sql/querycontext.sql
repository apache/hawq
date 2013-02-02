set Debug_querycontext_print = true;
set Debug_querycontext_print_tuple = true;

--disable dispatching query context
set gp_query_context_mem_limit = 0;

create table a (a int);

select * from a;

insert into a select generate_series(1, 10000);

select count(1), sum(a) from a;

truncate a;

select * from a;

insert into a select generate_series(1, 10000);

select count(1), sum(a) from a;

drop table a;


--set a small value, 2K
set gp_query_context_mem_limit = 2;

create table a (a int);

select * from a;

insert into a select generate_series(1, 10000);

select count(1), sum(a) from a;

truncate a;

select * from a;

insert into a select generate_series(1, 10000);

select count(1), sum(a) from a;

drop table a;


--set a small default value
reset gp_query_context_mem_limit;

create table a (a int);

select * from a;

insert into a select generate_series(1, 10000);

select count(1), sum(a) from a;

truncate a;

select * from a;

insert into a select generate_series(1, 10000);

select count(1), sum(a) from a;

drop table a;