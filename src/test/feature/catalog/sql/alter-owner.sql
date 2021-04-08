create role super superuser;
set session authorization super;
create role u1;
create database u1;
create schema u1;

select 
    nspname, rolname 
  from 
    pg_namespace n, pg_authid a 
  where 
    n.nspowner = a.oid and nspname = 'u1';

select 
    datname, rolname 
  from 
    pg_database d, pg_authid a 
  where 
    d.datdba = a.oid and datname = 'u1';

alter database u1 owner to u1;
alter schema u1 owner to u1;
alter schema u1 rename to u2;

-- test permission
create role u2;
ALTER SCHEMA pg_toast OWNER to u2; -- system schema
GRANT ALL ON SCHEMA u2 to u2;
RESET SESSION AUTHORIZATION;
SET SESSION AUTHORIZATION u2;
CREATE TABLE u2.grant_test(a int) DISTRIBUTED BY (a);
ALTER SCHEMA u2 RENAME to myschema;  -- not the schema owner

RESET SESSION AUTHORIZATION;
drop database u1;
drop schema u2 CASCADE;
drop role u1;
drop role u2;
drop role super;
