begin;
create table t (a int) with (appendonly=true);
insert into t select * from generate_series(1,10);
abort;