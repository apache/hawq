set session role=usermanual10000;
select count(*) from information_schema.view_table_usage;
select count(*) from hawq_toolkit.hawq_table_indexes;
select count(*) from pg_catalog.pg_compression;

