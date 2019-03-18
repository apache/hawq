-- Drop object ,clean env
drop OPERATOR CLASS sva_special_ops USING btree;

drop OPERATOR <# (text,text) CASCADE;

drop FUNCTION si_same(text, text);

drop FUNCTION si_lt(text, text);

drop FUNCTION normalize_si(text);

DROP RESOURCE QUEUE myqueue;

drop table foo;

drop tablespace mytblspace;

drop EXTERNAL TABLE ext_t;

drop EXTERNAL TABLE ext_t2;

drop function f4();

DROP AGGREGATE scube2(numeric);

DROP FUNCTION scube_accum(numeric, numeric);

drop type mytype cascade;

drop language plpythonu;

truncate aa;

drop SEQUENCE myseq;

drop view av;

--drop table aaa;

drop table aa;

drop table a;

drop schema sa CASCADE;

-- Create object

create table a(i int);

CREATE OR REPLACE FUNCTION si_same(text, text) RETURNS int AS $$ BEGIN IF normalize_si($1) < normalize_si($2) THEN RETURN -1; END IF; END; $$ LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OPERATOR CLASS sva_special_ops FOR TYPE text USING btree AS OPERATOR 1 <#, FUNCTION 1 si_same(text, text);

CREATE RESOURCE QUEUE myqueue WITH (PARENT='pg_root', ACTIVE_STATEMENTS=20, MEMORY_LIMIT_CLUSTER=50%, CORE_LIMIT_CLUSTER=50%);   

CREATE TABLESPACE mytblspace FILESPACE dfs_system;    

create language plpythonu;

CREATE TABLE foo(i int) TABLESPACE mytblspace;

CREATE EXTERNAL TABLE ext_t ( N_NATIONKEY INTEGER ,N_NAME CHAR(25), N_REGIONKEY  INTEGER ,N_COMMENT    VARCHAR(152))location ('gpfdist://localhost:7070/nation_error50.tbl')FORMAT 'text' (delimiter '|')SEGMENT REJECT LIMIT 51;

CREATE WRITABLE EXTERNAL TABLE ext_t2 (i int) LOCATION ('gpfdist://localhost:7070/ranger2.out') FORMAT 'TEXT' ( DELIMITER '|' NULL ' ');

CREATE OR REPLACE FUNCTION f4() RETURNS TEXT AS $$ plpy.execute("select * from a order by i") $$ LANGUAGE plpythonu VOLATILE;

create schema sa;

create temp table ta(i int);

create view av as select * from a order by i;

create table aa as select * from a order by i;

create table sa.t(a int, b int);

CREATE SEQUENCE myseq START 1;

PREPARE fooplan (int) AS INSERT INTO a VALUES($1);EXECUTE fooplan(1);DEALLOCATE fooplan;

CREATE FUNCTION scube_accum(numeric, numeric) RETURNS numeric AS 'select $1 + $2 * $2 * $2' LANGUAGE SQL IMMUTABLE RETURNS NULL ON NULL INPUT;

CREATE AGGREGATE scube(numeric) ( SFUNC = scube_accum, STYPE = numeric, INITCOND = 0 );

ALTER AGGREGATE scube(numeric) RENAME TO scube2;   

CREATE TYPE mytype AS (f1 int, f2 int);

CREATE FUNCTION getfoo() RETURNS SETOF mytype AS $$ SELECT i, i FROM a order by i $$ LANGUAGE SQL;

--alter table a rename column i to j;

CREATE OR REPLACE FUNCTION normalize_si(text) RETURNS text AS $$ BEGIN RETURN substring($1, 9, 2) || substring($1, 7, 2) || substring($1, 5, 2) || substring($1, 1, 4); END; $$LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OR REPLACE FUNCTION si_lt(text, text) RETURNS boolean AS $$ BEGIN RETURN normalize_si($1) < normalize_si($2); END; $$ LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OPERATOR <# ( PROCEDURE=si_lt,LEFTARG=text, RIGHTARG=text);

