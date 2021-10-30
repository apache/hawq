------------------------------------------------------------------
-- hawq status
------------------------------------------------------------------
CREATE VIEW pg_catalog.hawq_magma_status AS
    SELECT * FROM hawq_magma_status() AS s
    (node text,
     compactJobRunning text,
     compactJob text,
     compactActionJobRunning text,
     compactActionJob text,
     dirs text,
     description text);

------------------------------------------------------------------
-- history table
------------------------------------------------------------------
CREATE READABLE EXTERNAL WEB TABLE hawq_toolkit.__hawq_pg_stat_activity_history
(
    datid                int,
    datname              text,
    usesysid             int,
    username             text,
    procpid              int,
    sess_id              int,
    query_start          text,
    query_end            text,
    client_addr          text,
    client_port          int,
    application_name     text,
    cpu                  text,
    memory               text,
    status               text,
    errinfo              text,
    query                text
)
EXECUTE E'cat $GP_SEG_DATADIR/pg_log/*.history' ON MASTER
FORMAT 'CSV' (DELIMITER ',' NULL '' QUOTE '"');

REVOKE ALL ON TABLE hawq_toolkit.__hawq_pg_stat_activity_history FROM public;

------------------------------------------------------------------
-- schema/db previleges
------------------------------------------------------------------

CREATE VIEW information_schema.schema_privileges as
SELECT nspname AS schema_name,
coalesce(nullif(role.name,''), 'PUBLIC') AS grantee,
substring(
CASE WHEN position('U' in split_part(split_part((','||array_to_string(nspacl,',')), ','||role.name||'=',2 ) ,'/',1)) > 0 THEN ',USAGE' ELSE '' END
|| CASE WHEN position('C' in split_part(split_part((','||array_to_string(nspacl,',')), ','||role.name||'=',2 ) ,'/',1)) > 0 THEN ',CREATE' ELSE '' END
, 2,10000) AS privilege_type
FROM pg_namespace pn, (SELECT pg_roles.rolname AS name
FROM pg_roles UNION ALL SELECT '' AS name) AS role
WHERE (','||array_to_string(nspacl,',')) LIKE '%,'||role.name||'=%'
AND nspowner > 1;

GRANT SELECT ON information_schema.schema_privileges TO PUBLIC;

CREATE VIEW information_schema.database_privileges as
SELECT datname AS database_name,
coalesce(nullif(role.name,''), 'PUBLIC') AS grantee,
substring(
CASE WHEN position('C' in split_part(split_part((','||array_to_string(datacl,',')), ','||role.name||'=',2 ) ,'/',1)) > 0 THEN ',CREATE' ELSE '' END
|| CASE WHEN position('T' in split_part(split_part((','||array_to_string(datacl,',')), ','||role.name||'=',2 ) ,'/',1)) > 0 THEN ',TEMPORARY' ELSE '' END
|| CASE WHEN position('c' in split_part(split_part((','||array_to_string(datacl,',')), ','||role.name||'=',2 ) ,'/',1)) > 0 THEN ',CONNECT' ELSE '' END
, 2,10000) AS privilege_type
FROM pg_database pd, (SELECT pg_roles.rolname AS name
FROM pg_roles UNION ALL SELECT '' AS name) AS role
WHERE (','||array_to_string(datacl,',')) LIKE '%,'||role.name||'=%';

GRANT SELECT ON information_schema.database_privileges TO PUBLIC;
