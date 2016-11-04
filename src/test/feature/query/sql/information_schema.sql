drop table if exists r;
create table r(a int, b int);
 
SELECT attnum::information_schema.cardinal_number 
from pg_attribute 
where attnum > 0 and attrelid = 'r'::regclass;

-- this one should fail
SELECT attnum::information_schema.cardinal_number 
from pg_attribute 
where attrelid = 'r'::regclass;



SELECT *
from (SELECT attnum::information_schema.cardinal_number 
      from pg_attribute 
      where attnum > 0 and attrelid = 'r'::regclass) q
where attnum=2;

select table_schema, table_name,column_name,ordinal_position
from information_schema.columns
where table_name ='r';


select table_schema, table_name,column_name,ordinal_position
from information_schema.columns
where table_name ='r'
and ordinal_position =1;

select table_schema, table_name,column_name,ordinal_position
from information_schema.columns
where ordinal_position = 20;

-- MPP-25724
select a.column_name
from information_schema.columns a
where a.table_name
in
(select b.table_name from information_schema.tables b where
	a.column_name like 'b') and a.table_name = 'r';

select c.relname
from pg_class c
where c.relname
in
(select b.table_name from information_schema.tables b where
	c.relname like 'r');

select a.table_name
from information_schema.tables a
where a.table_name
in
(select b.relname from pg_class b where
	a.table_name like 'r');

drop table r;
