create table a(i int);

create language plpythonu;

CREATE OR REPLACE FUNCTION f4() RETURNS TEXT AS $$ plpy.execute("select * from a order by i") $$ LANGUAGE plpythonu VOLATILE;

select * from f4();

CREATE OR REPLACE FUNCTION normalize_si(text) RETURNS text AS $$ BEGIN RETURN substring($1, 9, 2) || substring($1, 7, 2) || substring($1, 5, 2) || substring($1, 1, 4); END; $$LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OR REPLACE FUNCTION si_lt(text, text) RETURNS boolean AS $$ BEGIN RETURN normalize_si($1) < normalize_si($2); END; $$ LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OPERATOR <# ( PROCEDURE=si_lt,LEFTARG=text, RIGHTARG=text);

CREATE OR REPLACE FUNCTION si_same(text, text) RETURNS int AS $$ BEGIN IF normalize_si($1) < normalize_si($2) THEN RETURN -1; END IF; END; $$ LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OPERATOR CLASS sva_special_ops FOR TYPE text USING btree AS OPERATOR 1 <#, FUNCTION 1 si_same(text, text);

CREATE RESOURCE QUEUE myqueue WITH (PARENT='pg_root', ACTIVE_STATEMENTS=20, MEMORY_LIMIT_CLUSTER=50%, CORE_LIMIT_CLUSTER=50%);   

CREATE TABLESPACE mytblspace FILESPACE dfs_system;    

CREATE TABLE foo(i int) TABLESPACE mytblspace;

insert into foo(i) values(1234);

COPY a TO '/tmp/a.txt';

COPY a FROM '/tmp/a.txt';

COPY a TO STDOUT WITH DELIMITER '|';

CREATE EXTERNAL TABLE ext_t ( N_NATIONKEY INTEGER ,N_NAME CHAR(25), N_REGIONKEY  INTEGER ,N_COMMENT    VARCHAR(152))location ('gpfdist://localhost:7070/nation_error50.tbl')FORMAT 'text' (delimiter '|')SEGMENT REJECT LIMIT 51;

--select * from ext_t order by N_NATIONKEY;   

CREATE WRITABLE EXTERNAL TABLE ext_t2 (i int) LOCATION ('gpfdist://localhost:7070/ranger2.out') FORMAT 'TEXT' ( DELIMITER '|' NULL ' ');

--insert into ext_t2(i) values(234);

create schema sa;

create temp table ta(i int);

create view av as select * from a order by i;

create table aa as select * from a order by i;

create table sa.t(a int, b int);

CREATE SEQUENCE myseq START 1;

insert into a values(1);

insert into a values(1);

insert into a VALUES (nextval('myseq'));

select * from pg_database, a order by oid, i limit 1;

select generate_series(1,3);

select * from av;

SELECT setval('myseq', 1);

SELECT * INTO aaa FROM a WHERE i > 0 order by i;

PREPARE fooplan (int) AS INSERT INTO a VALUES($1);EXECUTE fooplan(1);DEALLOCATE fooplan;

explain select * from a;

CREATE FUNCTION scube_accum(numeric, numeric) RETURNS numeric AS 'select $1 + $2 * $2 * $2' LANGUAGE SQL IMMUTABLE RETURNS NULL ON NULL INPUT;

CREATE AGGREGATE scube(numeric) ( SFUNC = scube_accum, STYPE = numeric, INITCOND = 0 );

ALTER AGGREGATE scube(numeric) RENAME TO scube2;   

CREATE TYPE mytype AS (f1 int, f2 int);

CREATE FUNCTION getfoo() RETURNS SETOF mytype AS $$ SELECT i, i FROM a order by i $$ LANGUAGE SQL;

select getfoo();

begin; DECLARE mycursor CURSOR FOR SELECT * FROM a order by i; FETCH FORWARD 2 FROM mycursor; commit;

BEGIN; INSERT INTO a VALUES (1); SAVEPOINT my_savepoint; INSERT INTO a VALUES (1); RELEASE SAVEPOINT my_savepoint; COMMIT;

\d

analyze a;

analyze;

vacuum aa;

vacuum analyze;

truncate aa;

alter table a rename column i to j;