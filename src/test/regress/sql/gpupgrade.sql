-- Unit tests for gpupgrade funcitonality

-- Expose interface to 3.0 -> 3.1 serialised strange modifier
create or replace function gp_str302node2str(text) returns text as '$libdir/libgpupgradefuncs.so', 'gp_str30_2node2str' language c strict volatile;
create or replace function gp_str312node2str(text) returns text as '$libdir/libgpupgradefuncs.so', 'gp_str31_2node2str' language c strict volatile;

-- Some sample strings we mustbe able to upgrade
create table nodetest (testname text, oldnode text, newnode text);
copy nodetest from stdin delimiter '|';
int8|{OPEXPR :opno 413 :opfuncid 0 :opresulttype 16 :opretset false :args ({COERCETODOMAINVALUE :typeId 20 :typeMod -1} {CONST :consttype 20 :constlen 8 :constbyval false :constisnull false :constvalue 8 [ 0 16 -91 -44 -24 0 0 0 ]})}|{OPEXPR :opno 413 :opfuncid 0 :opresulttype 16 :opretset false :args ({COERCETODOMAINVALUE :typeId 20 :typeMod -1} {CONST :consttype 20 :constlen 8 :constbyval true :constisnull false :constvalue 8 [ 0 16 -91 -44 -24 0 0 0 ]})}
float4|{OPEXPR :opno 623 :opfuncid 0 :opresulttype 16 :opretset false :args ({COERCETODOMAINVALUE :typeId 700 :typeMod -1} {CONST :consttype 700 :constlen 4 :constbyval false :constisnull false :constvalue 4 [ 0 0 -56 66 ]})}|{OPEXPR :opno 623 :opfuncid 0 :opresulttype 16 :opretset false :args ({COERCETODOMAINVALUE :typeId 700 :typeMod -1} {CONST :consttype 700 :constlen 4 :constbyval true :constisnull false :constvalue 4 [ 0 0 -56 66 0 0 0 0 ]})}
float8|{OPEXPR :opno 674 :opfuncid 0 :opresulttype 16 :opretset false :args ({COERCETODOMAINVALUE :typeId 701 :typeMod -1} {CONST :consttype 701 :constlen 8 :constbyval false :constisnull false :constvalue 8 [ 0 0 0 0 0 -120 -61 64 ]})}|{OPEXPR :opno 674 :opfuncid 0 :opresulttype 16 :opretset false :args ({COERCETODOMAINVALUE :typeId 701 :typeMod -1} {CONST :consttype 701 :constlen 8 :constbyval true :constisnull false :constvalue 8 [ 0 0 0 0 0 -120 -61 64 ]})}
timestamp|{OPEXPR :opno 2064 :opfuncid 0 :opresulttype 16 :opretset false :args ({COERCETODOMAINVALUE :typeId 1114 :typeMod -1} {CONST :consttype 1114 :constlen 8 :constbyval false :constisnull false :constvalue 8 [ -64 -111 -112 112 -9 -56 0 0 ]})}|{OPEXPR :opno 2064 :opfuncid 0 :opresulttype 16 :opretset false :args ({COERCETODOMAINVALUE :typeId 1114 :typeMod -1} {CONST :consttype 1114 :constlen 8 :constbyval true :constisnull false :constvalue 8 [ -64 -111 -112 112 -9 -56 0 0 ]})}
timestamptz|{OPEXPR :opno 1324 :opfuncid 0 :opresulttype 16 :opretset false :args ({COERCETODOMAINVALUE :typeId 1184 :typeMod -1} {CONST :consttype 1184 :constlen 8 :constbyval false :constisnull false :constvalue 8 [ -64 -59 114 -95 -5 -56 0 0 ]})}|{OPEXPR :opno 1324 :opfuncid 0 :opresulttype 16 :opretset false :args ({COERCETODOMAINVALUE :typeId 1184 :typeMod -1} {CONST :consttype 1184 :constlen 8 :constbyval true :constisnull false :constvalue 8 [ -64 -59 114 -95 -5 -56 0 0 ]})}
\.
select testname,
gp_str302node2str(oldnode) = newnode as upgrade,
gp_str312node2str(newnode) = oldnode as downgrade
from nodetest order by testname;
drop table nodetest;
-- test that a variety of nodes can be downgraded and upgraded
create view ugtest1 as select oid, sum(reltuples) from pg_class group by 1
  having(sum(reltuples) > 100) order by 2;

create view ugtest2 as select 1000000000000000 as a,
'2007-01-01 11:11:11'::timestamp as b,
'2007-01-01 11:11:11 PST'::timestamptz as c,
'200000.0000'::float4 as d,
'2000.00000000'::float8 as e,
123 as f;

create view ugtest3 as select * from pg_database limit 5;

create view ugtest4 as select relname, length(relname) from pg_class
where oid in (select distinct oid from pg_attribute);

create view ugtest5 as select array[ '10000000000000000'::int8 ] as test;

select gp_str302node2str(gp_str312node2str(ev_action)) = ev_action from
pg_rewrite where ev_class in('ugtest1'::regclass, 'ugtest2'::regclass,
'ugtest3'::regclass, 'ugtest4'::regclass, 'ugtest5'::regclass);
