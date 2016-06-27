drop function if exists f_load_exe();
drop table if exists src_table_exe;
drop table if exists main_table_exe;
drop table if exists map_table_exe;

create table src_table_exe ( id_natural integer, value varchar, id_file int);
create table main_table_exe (id_auto integer, id_natural integer, value varchar, record_type varchar, id_file integer);
create table map_table_exe (id_auto integer, id_natural integer);

insert into src_table_exe values ( 1, 'sth',        10);
insert into src_table_exe values ( 1, 'sht else', 11);

CREATE OR REPLACE FUNCTION f_load_exe()
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

RAISE NOTICE '--- Initial content of main_table_exe: begin ---';
RAISE NOTICE 'id_auto, id_natural, value, record_type, id_file';
FOR l_item IN SELECT * FROM main_table_exe LOOP
    RAISE NOTICE '%, %, %, %, %', quote_ident(l_item.id_auto), quote_ident(l_item.id_natural), quote_ident(l_item.value), quote_ident(l_item.record_type), quote_ident(l_item.id_file);
END LOOP;
RAISE NOTICE '--- Initial content of main_table_exe: end ---';

execute 'INSERT INTO main_table_exe
              ( id_natural
              , value
              , record_type
              , id_file
              )
         SELECT id_natural
              , value
              , ''P''
              , id_file
           FROM src_table_exe';

GET DIAGNOSTICS l_tuple = ROW_COUNT;
RAISE NOTICE 'INSERT INTO main_table_exe with seed data from src_table_exe: % tuple inserted', l_tuple;

RAISE NOTICE '--- Content of main_table_exe after seed data inserted: begin ---';
RAISE NOTICE 'id_auto, id_natural, value, record_type, id_file';
FOR l_item IN SELECT * FROM main_table_exe LOOP
    RAISE NOTICE '%, %, %, %, %', quote_ident(l_item.id_auto), quote_ident(l_item.id_natural), quote_ident(l_item.value), quote_ident(l_item.record_type), quote_ident(l_item.id_file);
END LOOP;
RAISE NOTICE '--- Content of main_table_exe after seed data inserted: end ---';

FOR l_rec IN ( select id_file from main_table_exe group by id_file order by 1)
  LOOP
      l_count:=l_count+1;

execute 'INSERT INTO main_table_exe
              ( id_natural
              , value
              , record_type
              , id_file
              )
         SELECT id_natural
              , value
              , ''N''
              , '||l_rec.id_file||'
           FROM main_table_exe pxf
          WHERE pxf.id_file='||l_rec.id_file||' AND pxf.record_type=''P'''
              ;

GET DIAGNOSTICS l_tuple = ROW_COUNT;
RAISE NOTICE 'Insert into main_table_exe in loop % with first insert statement: % tuple inserted', l_count, l_tuple;

RAISE NOTICE '--- Content of main_table_exe after loop % with first insert statement: begin ---', l_count;
RAISE NOTICE 'id_auto, id_natural, value, record_type, id_file';
FOR l_item IN SELECT * FROM main_table_exe LOOP
    RAISE NOTICE '%, %, %, %, %', quote_ident(l_item.id_auto), quote_ident(l_item.id_natural), quote_ident(l_item.value), quote_ident(l_item.record_type), quote_ident(l_item.id_file);
END LOOP;
RAISE NOTICE '--- Content of main_table_exe after loop % with first insert statement: end ---', l_count;

execute 'INSERT INTO main_table_exe
              ( id_auto
              , id_natural
              , value
              , record_type
              , id_file
              )
         SELECT '||l_count||'
              , ma.id_natural
              , value
              , CASE WHEN mt.id_natural IS NULL THEN ''I'' ELSE ''U'' END AS record_type
              , id_file
           FROM main_table_exe ma
           LEFT JOIN map_table_exe mt on mt.id_natural=ma.id_natural
          WHERE ma.record_type=''N'' AND ma.id_file='||l_rec.id_file
              ;

        execute 'truncate table map_table_exe';

GET DIAGNOSTICS l_tuple = ROW_COUNT;
RAISE NOTICE 'Insert into main_table_exe in loop % with second insert statement: % tuple inserted', l_count, l_tuple;

RAISE NOTICE '--- Content of main_table_exe after loop % with second insert statement: begin ---', l_count;
RAISE NOTICE 'id_auto, id_natural, value, record_type, id_file';
FOR l_item IN SELECT * FROM main_table_exe LOOP
    RAISE NOTICE '%, %, %, %, %', quote_ident(l_item.id_auto), quote_ident(l_item.id_natural), quote_ident(l_item.value), quote_ident(l_item.record_type), quote_ident(l_item.id_file);
END LOOP;
RAISE NOTICE '--- Content of main_table_exe after loop % with second insert statement: end ---', l_count;

execute 'INSERT INTO map_table_exe
              ( id_auto
              , id_natural
              )
         SELECT ma.id_auto
              , ma.id_natural
           FROM main_table_exe ma
          WHERE record_type NOT IN (''N'',''P'') AND id_file='||l_rec.id_file
              ;

     END LOOP;

RAISE NOTICE '--- Final content of main_table_exe: begin ---';
RAISE NOTICE 'id_auto, id_natural, value, record_type, id_file';
FOR l_item IN SELECT * FROM main_table_exe LOOP
    RAISE NOTICE '%, %, %, %, %', quote_ident(l_item.id_auto), quote_ident(l_item.id_natural), quote_ident(l_item.value), quote_ident(l_item.record_type), quote_ident(l_item.id_file);
END LOOP;
RAISE NOTICE '--- Final content of main_table_exe: end ---';

  RETURN 'done';

END;
$body$
;

select f_load_exe();

select * from main_table_exe;

drop function if exists f_load_exe();
drop table if exists src_table_exe;
drop table if exists main_table_exe;
drop table if exists map_table_exe;
