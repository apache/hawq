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

reset session authorization;
drop database u1;
drop schema u1;
drop role u1;
drop role super;