--
-- debug_orc creates views to debug ORC table, leveraging the Apache ORC's tools.
--
-- The relation to debug provides its name as suffix, and the debug_orc action serves as prefix.
-- i.e.
-- Name the debug view in the form of debug_orc_{metadata,contents,statistics}_view_`relname`.
-- Name the intermediate table in the form of debug_orc_{metadata,contents,statistics}_table_`relname`.
--
-- debug_orc_metadata_table_`relname` dump the relation's content as a table.

drop function if exists pg_aoseg(oid);
create or replace function pg_aoseg(t oid)
    returns table
            (
                relname          text,
                orientation      text,
                segno            int,
                tupcount         double precision,
                eof              double precision,
                eof_uncompressed double precision
            )
as
$$
declare
    reloptions text[];
begin
    select pg_class.relname from pg_class where oid = t into relname;
    select pg_class.reloptions from pg_class where oid = t into reloptions;
    select case
               when
                   'orientation=parquet' = any (reloptions) then 'paq'
               when
                   'orientation=orc' = any (reloptions) then 'orc'
               else 'ao'
               end
    into orientation;

    for segno, tupcount, eof, eof_uncompressed in EXECUTE
                        'SELECT segno, tupcount, eof, eofuncompressed from pg_aoseg.pg_' || orientation || 'seg_' ||
                        t::text ||
                        ' ORDER BY segno'
        loop
            return next;
        end loop;
end;
$$ language plpgsql;

drop function if exists ls_hdfs_table_location(oid);
CREATE or replace FUNCTION ls_hdfs_table_location(reloid oid)
    RETURNS setof text
AS
$$
plan = plpy.prepare("select location from pg_exttable where reloid = $1", ["oid"])
rv = plpy.execute(plan, [reloid], 1)

if len(rv) != 0:                 # external table
    loc_list = rv[0]["location"]
else:                            # internal orc table
    plan = plpy.prepare("select 'hdfs://' || pg_settings.setting || '/' || pg_database.dat2tablespace || '/' || pg_database.oid || '/' || pg_class.relfilenode from pg_settings,pg_database,pg_class where pg_settings.name = 'hawq_dfs_url' and datname = current_database() and pg_class.oid = $1;", ["oid"])
    rv= plpy.execute(plan, [reloid], 1)
    loc_list = [rv[0]["?column?"]]

return loc_list
$$ LANGUAGE plpythonu;

drop function if exists ls_hdfs_table_file_list(oid);
CREATE or replace FUNCTION ls_hdfs_table_file_list(reloid oid)
    RETURNS setof text
AS
$$
import subprocess

plan = plpy.prepare("select location from pg_exttable where reloid = $1", ["oid"])
rv = plpy.execute(plan, [reloid], 1)
if len(rv) != 0:   #ext orc table
    loc_list = rv[0]["location"]

    hdfs_ls = lambda loc: subprocess.Popen(['hdfs', 'dfs', '-ls', '-C', loc],
                                        stdout=subprocess.PIPE).communicate()[0].split("\n")
    result = sum(map(hdfs_ls, loc_list), [])
    result = filter(lambda x: len(x) and 'tmp' not in x, result)
    return result
else:    # internal orc table
    plan = plpy.prepare("select 'hdfs://' || pg_settings.setting || '/' || pg_database.dat2tablespace || '/' || pg_database.oid || '/' || pg_class.relfilenode from pg_settings,pg_database,pg_class where pg_settings.name = 'hawq_dfs_url' and datname = current_database() and pg_class.oid = $1;", ["oid"])
    rv= plpy.execute(plan, [reloid], 1)
    loc_list_tmp = rv[0]["?column?"]
    loc_list = []
    loc_list.append(loc_list_tmp)
    hdfs_ls = lambda loc: subprocess.Popen(['hdfs', 'dfs', '-ls', '-C', loc],
                                        stdout=subprocess.PIPE).communicate()[0].split("\n")
    result = sum(map(hdfs_ls, loc_list), [])

    result = filter(lambda x: len(x) and 'tmp' not in x, result)
    return result;

$$ LANGUAGE plpythonu;

DROP FUNCTION IF EXISTS debug_orc(rel regclass);
CREATE OR REPLACE FUNCTION debug_orc(rel regclass) RETURNS text AS
$$
DECLARE
    relName  TEXT;
    segIdx   int;
    segCount int;
    url      TEXT;
    contents_command  text;
    meta_command text;
    st_command text;
BEGIN
    show default_hash_table_bucket_number into segCount;
    select pg_class.relname from pg_class where oid = rel::oid into relName;

    -- drop related views and tables
    EXECUTE 'DROP EXTERNAL TABLE IF EXISTS debug_orc_metadata_table_' ||
            relName || ' CASCADE';
    EXECUTE 'DROP EXTERNAL TABLE IF EXISTS debug_orc_contents_table_' ||
            relName || ' CASCADE';
    EXECUTE 'DROP EXTERNAL TABLE IF EXISTS debug_orc_statistics_table_' ||
            relName || ' CASCADE';
    EXECUTE 'DROP view IF EXISTS debug_orc_metadata_view_' ||
            relName || ' CASCADE';
    EXECUTE 'DROP view IF EXISTS debug_orc_contents_view_' ||
            relName || ' CASCADE';
    EXECUTE 'DROP view IF EXISTS debug_orc_statistics_view_' ||
            relName || ' CASCADE';
    -- Generate command to execute on segment, distributing job according to GP_SEGMENT_ID
    segIdx := 0;
    contents_command := '';
    drop table if exists hdfs_file_path_table;
    create temp table hdfs_file_path_table(path text);
    insert into hdfs_file_path_table select * from ls_hdfs_table_file_list(rel::oid);
    for url in select * from hdfs_file_path_table
        loop
            segIdx := (segIdx + 1) % segCount;
            contents_command := contents_command || E'\n' || 'test $GP_SEGMENT_ID -ne ' ||
                       segIdx || ' || orc-contents ' || url || ';';
        end loop;

    meta_command :='';
    for url in select * from hdfs_file_path_table
        loop
            meta_command := meta_command || ' orc_debug_metadata.py ' ||relName|| ' '||url || ';';
        end loop;

    st_command :='';
    for url in select * from hdfs_file_path_table
        loop
            st_command := st_command || ' orc_debug_statistics.py ' ||relName|| ' '||url || ';';
        end loop;

    EXECUTE '
            CREATE READABLE EXTERNAL WEB TABLE debug_orc_contents_table_' ||
            relName || '(row JSONB) EXECUTE ''' || contents_command || ''' ON ' ||
            segCount::text || ' FORMAT ''TEXT''' || ' (escape ''off'')';
    EXECUTE 'CREATE VIEW debug_orc_contents_view_' || relName || ' as
        SELECT (JSONB_POPULATE_RECORD(NULL::' || relName || ', row, false)).*
        FROM debug_orc_contents_table_' || relName ||';';


   EXECUTE '
            CREATE READABLE EXTERNAL WEB TABLE debug_orc_metadata_table_' ||
            relName || '(table_name text,metadata jsonb) EXECUTE ''' || meta_command || ''' ON ' ||
            'master' || ' FORMAT ''TEXT'''||'(DELIMITER ''|'' escape ''off'')';

   EXECUTE '
            CREATE VIEW debug_orc_metadata_view_' ||relName || 
            '  as select table_name,jsonb_pretty(metadata) as metadata from debug_orc_metadata_table_' ||relName ||';';
   
   EXECUTE '
            CREATE READABLE EXTERNAL WEB TABLE debug_orc_statistics_table_' ||
            relName || '(table_name text, statistics jsonb) EXECUTE ''' || st_command || ''' ON ' ||
            'master' || ' FORMAT ''TEXT'''||'(DELIMITER ''|'' escape ''off'')';
   
   EXECUTE '
           CREATE VIEW  debug_orc_statistics_view_' || relName || 
           '  as select table_name,jsonb_pretty(statistics) as statistics from debug_orc_statistics_table_' ||relName ||';';

    return contents_command;
END;
$$ LANGUAGE PLPGSQL;
