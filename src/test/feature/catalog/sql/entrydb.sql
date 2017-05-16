
-- test case for HAWQ-1455 (Wrong results on CTAS query over catalog)
create temp table entrydb_t1 (entrydb_tta varchar, entrydb_ttb varchar);
create temp table entrydb_t2 (entrydb_tta varchar, entrydb_ttb varchar);
insert into entrydb_t1 values('entrydb_a', '1');
insert into entrydb_t1 values('entrydb_a', '2');
insert into entrydb_t1 values('entrydb_tta', '3');
insert into entrydb_t1 values('entrydb_ttb', '4');

insert into entrydb_t2 select pg_attribute.attname, entrydb_t1.entrydb_ttb from pg_attribute join entrydb_t1 on pg_attribute.attname = entrydb_t1.entrydb_tta;

-- test case for HAWQ-512 (Query hang due to deadlock in entrydb catalog access)
create table entrydb_t3 (key int, value int) distributed randomly;
insert into entrydb_t3 values (1, 0);

begin;

alter table entrydb_t3 set distributed by (key);

select entrydb_t4.key FROM entrydb_t3 AS entrydb_t4, (select generate_series(1, 2)::int as key, 0::int as value) as entrydb_t5, (select generate_series(1, 2)::int as key, 0::int as value) as entrydb_t6 where entrydb_t4.value = entrydb_t5.value and entrydb_t4.value = entrydb_t6.value;

commit;
