1
create table a(i int);
0
create language plpythonu;
0
CREATE OR REPLACE FUNCTION f4() RETURNS TEXT AS $$ plpy.execute("select * from a order by i") $$ LANGUAGE plpythonu VOLATILE;
1
select * from f4();
0
drop function f4();
0
drop language plpythonu;
1
CREATE OR REPLACE FUNCTION normalize_si(text) RETURNS text AS $$ BEGIN RETURN substring($1, 9, 2) || substring($1, 7, 2) || substring($1, 5, 2) || substring($1, 1, 4); END; $$LANGUAGE 'plpgsql' IMMUTABLE;
1
CREATE OR REPLACE FUNCTION si_lt(text, text) RETURNS boolean AS $$ BEGIN RETURN normalize_si($1) < normalize_si($2); END; $$ LANGUAGE 'plpgsql' IMMUTABLE;
1
CREATE OPERATOR <# ( PROCEDURE=si_lt,LEFTARG=text, RIGHTARG=text);
1
CREATE OR REPLACE FUNCTION si_same(text, text) RETURNS int AS $$ BEGIN IF normalize_si($1) < normalize_si($2) THEN RETURN -1; END IF; END; $$ LANGUAGE 'plpgsql' IMMUTABLE;
0
CREATE OPERATOR CLASS sva_special_ops FOR TYPE text USING btree AS OPERATOR 1 <#, FUNCTION 1 si_same(text, text);
0
drop OPERATOR CLASS sva_special_ops USING btree;
1
drop OPERATOR <# (text,text) CASCADE;
1
drop FUNCTION si_same(text, text);
1
drop FUNCTION si_lt(text, text);
1
drop FUNCTION normalize_si(text);
0
CREATE RESOURCE QUEUE myqueue WITH (PARENT='pg_root', ACTIVE_STATEMENTS=20, MEMORY_LIMIT_CLUSTER=50%, CORE_LIMIT_CLUSTER=50%);   
0
DROP RESOURCE QUEUE myqueue;
0
CREATE TABLESPACE mytblspace FILESPACE dfs_system;    
1
CREATE TABLE foo(i int) TABLESPACE mytblspace;
1
insert into foo(i) values(1234);
1
drop table foo;
0
drop tablespace mytblspace;
0
COPY a FROM '/tmp/a.txt';
1
COPY a TO STDOUT WITH DELIMITER '|';
1
CREATE EXTERNAL TABLE ext_t ( N_NATIONKEY INTEGER ,N_NAME CHAR(25), N_REGIONKEY  INTEGER ,N_COMMENT    VARCHAR(152))location ('gpfdist://localhost:7070/nation_error50.tbl')FORMAT 'text' (delimiter '|')SEGMENT REJECT LIMIT 51;
1
select * from ext_t order by N_NATIONKEY;   
1
CREATE WRITABLE EXTERNAL TABLE ext_t2 (i int) LOCATION ('gpfdist://localhost:7070/ranger2.out') FORMAT 'TEXT' ( DELIMITER '|' NULL ' ');
1
insert into ext_t2(i) values(234);
1
drop EXTERNAL TABLE ext_t;
1
drop EXTERNAL TABLE ext_t2;
1
create schema sa;
1
create temp table ta(i int);
1
create view av as select * from a order by i;
1
create table aa as select * from a order by i;
1
create table sa.t(a int, b int);
1
CREATE SEQUENCE myseq START 1;
1
insert into a values(1);
1
insert into a values(1);
1
insert into a VALUES (nextval('myseq'));
1
select * from pg_database, a order by oid, i limit 1;
1
select generate_series(1,3);
1
select * from av;
1
SELECT setval('myseq', 1);
1
SELECT * INTO aaa FROM a WHERE i > 0 order by i;
1
PREPARE fooplan (int) AS INSERT INTO a VALUES($1);EXECUTE fooplan(1);DEALLOCATE fooplan;
1
explain select * from a;
1
CREATE FUNCTION scube_accum(numeric, numeric) RETURNS numeric AS 'select $1 + $2 * $2 * $2' LANGUAGE SQL IMMUTABLE RETURNS NULL ON NULL INPUT;
1
CREATE AGGREGATE scube(numeric) ( SFUNC = scube_accum, STYPE = numeric, INITCOND = 0 );
1
ALTER AGGREGATE scube(numeric) RENAME TO scube2;   
1
DROP AGGREGATE scube2(numeric);
1
DROP FUNCTION scube_accum(numeric, numeric);
1
CREATE TYPE mytype AS (f1 int, f2 int);
1
CREATE FUNCTION getfoo() RETURNS SETOF mytype AS $$ SELECT i, i FROM a order by i $$ LANGUAGE SQL;
1
select getfoo();
1
drop type mytype cascade;
1
begin; DECLARE mycursor CURSOR FOR SELECT * FROM a order by i; FETCH FORWARD 2 FROM mycursor; commit;
1
BEGIN; INSERT INTO a VALUES (1); SAVEPOINT my_savepoint; INSERT INTO a VALUES (1); RELEASE SAVEPOINT my_savepoint; COMMIT;
1
\d
1
analyze a;
1
analyze;
1
vacuum aa;
0
vacuum analyze;
1
truncate aa;
1
alter table a rename column i to j;
1
drop SEQUENCE myseq;
1
drop view av;
1
drop table aaa;
1
drop table aa;
1
drop table a;
1
drop schema sa CASCADE;
