begin;

DROP SCHEMA IF EXISTS hornet_helper CASCADE;
CREATE SCHEMA hornet_helper;
SET SEARCH_PATH = hornet_helper;

DROP FUNCTION IF EXISTS ls_hdfs_dir(TEXT);
CREATE FUNCTION ls_hdfs_dir(location TEXT)
    RETURNS table
            (
                file_replication int,
                file_owner text,
                file_group text,
                file_size bigint,
                file_name text
            )
AS '$libdir/hornet', 'ls_hdfs_dir'
LANGUAGE C IMMUTABLE;

drop type if exists pg_aoseg;
create type pg_aoseg as (relname          text,
    orientation      text,
    segno            int,
    tupcount         double precision,
    eof              double precision,
    eof_uncompressed double precision
    );

drop function if exists pg_aoseg(VARIADIC oid[]);
create or replace function pg_aoseg(VARIADIC reloids oid[])
    returns setof pg_aoseg
as
$$
declare
    ret         hornet_helper.pg_aoseg;
    reloid      oid;
    relname     text;
    reloptions  text[];
    orientation text;
begin
    for reloid in select reloids[idx] from (select generate_series(1, array_upper(reloids, 1))) f(idx)
        loop
            select pg_class.relname from pg_class where oid = reloid into relname;
            select pg_class.reloptions from pg_class where oid = reloid into reloptions;
            select case
                       when
                           'orientation=parquet' = any (reloptions) then 'paq'
                       when
                           'orientation=orc' = any (reloptions) then 'orc'
                       else 'ao'
                       end
            into orientation;

            for ret in EXECUTE
                                    'SELECT ''' || relname::text || ''',''' || orientation::text || ''',' ||
                                    'segno, tupcount, eof, eofuncompressed from pg_aoseg.pg_' || orientation ||
                                    'seg_' ||
                                    reloid::text ||
                                    ' ORDER BY segno'
                loop
                    return next ret;
                end loop;
        end loop;
end;
$$ language plpgsql;

drop function if exists ls_hdfs_table_location(oid);
CREATE or replace FUNCTION ls_hdfs_table_location(reloid oid)
    RETURNS setof text
AS
$$
declare
    locations text[];
    location  text;
begin
    select pg_exttable.location
    from pg_exttable
    where pg_exttable.reloid = reloid
    into locations;
    if locations is not null then
        for location in select locations[idx]
                        from (select generate_series(1,
                                                     array_upper(locations, 1))) f(idx)
            loop
                return next location;
            end loop;
    else
        for location in select 'hdfs://' || pg_settings.setting || '/' ||
                               pg_database.dat2tablespace || '/' ||
                               pg_database.oid || '/' || pg_class.relfilenode
                        from pg_settings,
                             pg_database,
                             pg_class
                        where pg_settings.name = 'hawq_dfs_url'
                          and datname = current_database()
                          and pg_class.oid = reloid
            loop
                return next location;
            end loop;
    end if;
end;
$$ LANGUAGE plpgsql;

drop function if exists ls_hdfs_table_file_list(oid);
CREATE or replace FUNCTION ls_hdfs_table_file_list(reloid oid)
    RETURNS setof text
AS
'select * from (select (hornet_helper.ls_hdfs_dir(ls_hdfs_table_location)).file_name
 from hornet_helper.ls_hdfs_table_location($1)) t where file_name !~ ''.tmp$'';'
    LANGUAGE SQL;


--
-- debug_orc creates views to debug ORC table, leveraging the Apache ORC's tools.
--
-- The relation to debug provides its name as suffix, and the debug_orc action serves as prefix.
-- i.e.
-- Name the debug view in the form of debug_orc_{metadata,contents,statistics}_view_`relname`.
-- Name the intermediate table in the form of debug_orc_{metadata,contents,statistics}_table_`relname`.
--
-- debug_orc_metadata_table_`relname` dump the relation's content as a table.
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
    meta_command :='';
    st_command :='';
    for url in select * from hornet_helper.ls_hdfs_table_file_list(rel::oid)
        loop
            segIdx := (segIdx + 1) % segCount;
            contents_command := contents_command || E'\n' || 'test $GP_SEGMENT_ID -ne ' ||
                       segIdx || ' || orc-contents ' || url || ';';
            meta_command := meta_command || ' orc_debug_metadata.py ' ||relName|| ' '||url || ';';
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


drop function if exists is_supported_proc_in_NewQE(oid);

create function is_supported_proc_in_NewQE(oid) returns boolean as '$libdir/hornet','is_supported_proc_in_NewQE'language c immutable;



drop function if exists orc_tid_scan(anyelement, text, bigint, int[]);
create function orc_tid_scan(anyelement, text, bigint, int[]) returns anyelement
as '$libdir/hornet','orc_url_tid_scan' language c stable;

drop function if exists orc_tid_scan(anyelement, text, bigint);
create function orc_tid_scan(anyelement, text, bigint) returns anyelement
as '$libdir/hornet','orc_url_tid_scan' language c stable;

drop function if exists orc_tid_scan(anyelement, int, bigint, int[]);
create function orc_tid_scan(anyelement, int, bigint, int[]) returns anyelement
as '$libdir/hornet','orc_segno_tid_scan' language c stable;

drop function if exists orc_tid_scan(anyelement, int, bigint);
create function orc_tid_scan(anyelement, int, bigint) returns anyelement
as '$libdir/hornet','orc_segno_tid_scan' language c stable;

commit;
