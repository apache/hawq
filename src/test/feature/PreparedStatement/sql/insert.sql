-- start_ignore
drop function if exists f1();
drop function if exists f2();
drop table if exists t1;
drop table if exists t2;
-- end_ignore

create table t1 (id int);
insert into t1 values (1);

create table t2 (id int);

CREATE OR REPLACE FUNCTION f1()
  RETURNS text
  LANGUAGE plpgsql
AS
$body$
DECLARE
    l_rec record;
    l_item record;
	l_count integer;
BEGIN

	RAISE NOTICE '--- Initial content of t1: begin ---';
	SELECT count(*) INTO l_count FROM t1;
	RAISE NOTICE '--- # tuple: %', l_count;
    RAISE NOTICE 'id';
    FOR l_item IN SELECT * FROM t1 LOOP
        RAISE NOTICE '%', quote_ident(l_item.id);
    END LOOP;
	RAISE NOTICE '--- Initial content of t1: end ---';

    FOR l_rec IN ( SELECT generate_series(1, 3) AS idx )
    LOOP
        INSERT INTO t1 SELECT * FROM t1;

	    RAISE NOTICE '--- Content of t1 after %d insert in loop: begin ---', l_rec.idx;
		SELECT count(*) INTO l_count FROM t1;
		RAISE NOTICE '--- # tuple: %', l_count;
        RAISE NOTICE 'id';
        FOR l_item IN SELECT * FROM t1 LOOP
            RAISE NOTICE '%', quote_ident(l_item.id);
        END LOOP;
	    RAISE NOTICE '--- Content of t1 after %d insert in loop: end ---', l_rec.idx;
    END LOOP;

	RAISE NOTICE '--- Final content of t1: begin ---';
	SELECT count(*) INTO l_count FROM t1;
	RAISE NOTICE '--- # tuple: %', l_count;
    RAISE NOTICE 'id';
    FOR l_item IN SELECT * FROM t1 LOOP
        RAISE NOTICE '%', quote_ident(l_item.id);
    END LOOP;
	RAISE NOTICE '--- Final content of t1: end ---';

    RETURN 'done';
END
$body$
;

CREATE OR REPLACE FUNCTION f2()
  RETURNS text
  LANGUAGE plpgsql
AS
$body$
DECLARE
    l_rec record;
    l_item record;
	l_count integer;
BEGIN
	RAISE NOTICE '--- Initial content of t2: begin ---';
	SELECT count(*) INTO l_count FROM t2;
	RAISE NOTICE '--- # tuple: %', l_count;
    RAISE NOTICE 'id';
    FOR l_item IN SELECT * FROM t2 LOOP
        RAISE NOTICE '%', quote_ident(l_item.id);
    END LOOP;
	RAISE NOTICE '--- Initial content of t2: end ---';

	insert into t2 values (1);

	RAISE NOTICE '--- Content of t2 after seed data inserted: begin ---';
	SELECT count(*) INTO l_count FROM t2;
	RAISE NOTICE '--- # tuple: %', l_count;
    RAISE NOTICE 'id';
    FOR l_item IN SELECT * FROM t2 LOOP
        RAISE NOTICE '%', quote_ident(l_item.id);
    END LOOP;
	RAISE NOTICE '--- Content of t2 after seed data inserted: end ---';

    FOR l_rec IN ( SELECT generate_series(1, 3) AS idx )
    LOOP
        INSERT INTO t2 SELECT * FROM t2;

	    RAISE NOTICE '--- Content of t2 after %d insert in loop: begin ---', l_rec.idx;
		SELECT count(*) INTO l_count FROM t2;
		RAISE NOTICE '--- # tuple: %', l_count;
        RAISE NOTICE 'id';
        FOR l_item IN SELECT * FROM t2 LOOP
            RAISE NOTICE '%', quote_ident(l_item.id);
        END LOOP;
	    RAISE NOTICE '--- Content of t2 after %d insert in loop: end ---', l_rec.idx;
    END LOOP;

	RAISE NOTICE '--- Final content of t2: begin ---';
	SELECT count(*) INTO l_count FROM t2;
	RAISE NOTICE '--- # tuple: %', l_count;
    RAISE NOTICE 'id';
    FOR l_item IN SELECT * FROM t2 LOOP
        RAISE NOTICE '%', quote_ident(l_item.id);
    END LOOP;
	RAISE NOTICE '--- Final content of t2: end ---';

    RETURN 'done';
END
$body$
;

select f1();

select f2();

select * from t1;

select * from t2;

drop function if exists f1();
drop function if exists f2();
drop table if exists t1;
drop table if exists t2;
