-- start_ignore
drop function if exists f_load();
drop table if exists src_table;
drop table if exists main_table;
drop table if exists map_table;
-- end_ignore

create table src_table ( id_natural integer, value varchar, id_file int);
create table main_table (id_auto integer, id_natural integer, value varchar, record_type varchar, id_file integer);
create table map_table (id_auto integer, id_natural integer);

insert into src_table values ( 1, 'sth',      10);
insert into src_table values ( 1, 'sht else', 11);

CREATE OR REPLACE FUNCTION f_load()
  RETURNS text
  LANGUAGE plpgsql
AS
$body$
DECLARE

   l_count integer:=0;
   l_rec record;
   l_tuple integer;
   l_item record;

BEGIN

	RAISE NOTICE '--- Initial content of main_table: begin ---';
    RAISE NOTICE 'id_auto, id_natural, value, record_type, id_file';
    FOR l_item IN SELECT * FROM main_table LOOP
        RAISE NOTICE '%, %, %, %, %', quote_ident(l_item.id_auto), quote_ident(l_item.id_natural), quote_ident(l_item.value), quote_ident(l_item.record_type), quote_ident(l_item.id_file);
    END LOOP;
	RAISE NOTICE '--- Initial content of main_table: end ---';

    INSERT INTO main_table
         ( id_natural
         , value
         , record_type
         , id_file
         )
    SELECT id_natural
         , value
         , 'P'
         , id_file
      FROM src_table;

    GET DIAGNOSTICS l_tuple = ROW_COUNT;
    RAISE NOTICE 'INSERT INTO main_table with seed data from src_table: % tuple inserted', l_tuple;

	RAISE NOTICE '--- Content of main_table after seed data inserted: begin ---';
    RAISE NOTICE 'id_auto, id_natural, value, record_type, id_file';
    FOR l_item IN SELECT * FROM main_table LOOP
        RAISE NOTICE '%, %, %, %, %', quote_ident(l_item.id_auto), quote_ident(l_item.id_natural), quote_ident(l_item.value), quote_ident(l_item.record_type), quote_ident(l_item.id_file);
    END LOOP;
	RAISE NOTICE '--- Content of main_table after seed data inserted: end ---';

FOR l_rec IN ( select id_file from main_table group by id_file order by 1)
  LOOP
      l_count:=l_count+1;

      INSERT INTO main_table
           ( id_natural
           , value
           , record_type
           , id_file
           )
      SELECT id_natural
           , value
           , 'N'
           , l_rec.id_file
        FROM main_table pxf
       WHERE pxf.id_file=l_rec.id_file AND pxf.record_type='P'
           ;

      GET DIAGNOSTICS l_tuple = ROW_COUNT;
      RAISE NOTICE 'Insert into main_table in loop % with first insert statement: % tuple inserted', l_count, l_tuple;

	  RAISE NOTICE '--- Content of main_table after loop % with first insert statement: begin ---', l_count;
      RAISE NOTICE 'id_auto, id_natural, value, record_type, id_file';
      FOR l_item IN SELECT * FROM main_table LOOP
          RAISE NOTICE '%, %, %, %, %', quote_ident(l_item.id_auto), quote_ident(l_item.id_natural), quote_ident(l_item.value), quote_ident(l_item.record_type), quote_ident(l_item.id_file);
      END LOOP;
	  RAISE NOTICE '--- Content of main_table after loop % with first insert statement: end ---', l_count;

      INSERT INTO main_table
           ( id_auto
           , id_natural
           , value
           , record_type
           , id_file
           )
      SELECT l_count
           , ma.id_natural
           , value
           , CASE WHEN mt.id_natural IS NULL THEN 'I' ELSE 'U' END AS record_type
           , id_file
        FROM main_table ma
        LEFT JOIN map_table mt on mt.id_natural=ma.id_natural
       WHERE ma.record_type='N' AND ma.id_file=l_rec.id_file
           ;

      GET DIAGNOSTICS l_tuple = ROW_COUNT;
      RAISE NOTICE 'Insert into main_table in loop % with second insert statement: % tuple inserted', l_count, l_tuple;

	  RAISE NOTICE '--- Content of main_table after loop % with second insert statement: begin ---', l_count;
      RAISE NOTICE 'id_auto, id_natural, value, record_type, id_file';
      FOR l_item IN SELECT * FROM main_table LOOP
          RAISE NOTICE '%, %, %, %, %', quote_ident(l_item.id_auto), quote_ident(l_item.id_natural), quote_ident(l_item.value), quote_ident(l_item.record_type), quote_ident(l_item.id_file);
      END LOOP;
	  RAISE NOTICE '--- Content of main_table after loop % with second insert statement: end ---', l_count;

      execute 'truncate table map_table';

      INSERT INTO map_table
           ( id_auto
           , id_natural
           )
      SELECT ma.id_auto
           , ma.id_natural
        FROM main_table ma
       WHERE record_type NOT IN ('N','P') AND id_file=l_rec.id_file
           ;

     END LOOP;

	 RAISE NOTICE '--- Final content of main_table: begin ---';
     RAISE NOTICE 'id_auto, id_natural, value, record_type, id_file';
     FOR l_item IN SELECT * FROM main_table LOOP
         RAISE NOTICE '%, %, %, %, %', quote_ident(l_item.id_auto), quote_ident(l_item.id_natural), quote_ident(l_item.value), quote_ident(l_item.record_type), quote_ident(l_item.id_file);
     END LOOP;
	 RAISE NOTICE '--- Final content of main_table: end ---';

  RETURN 'done';

END;
$body$
;

select f_load();

select * from main_table;

drop function if exists f_load();
drop table if exists src_table;
drop table if exists main_table;
drop table if exists map_table;
