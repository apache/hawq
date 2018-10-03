--drop agg functions
--drop the previous funcitons
DROP AGGREGATE IF EXISTS sum(vint2);
DROP AGGREGATE IF EXISTS sum(vint4);
DROP AGGREGATE IF EXISTS sum(vint8);
DROP AGGREGATE IF EXISTS sum(vfloat4);
DROP AGGREGATE IF EXISTS sum(vfloat8);

DROP AGGREGATE IF EXISTS avg(vint2);
DROP AGGREGATE IF EXISTS avg(vint4);
DROP AGGREGATE IF EXISTS avg(vint8);
DROP AGGREGATE IF EXISTS avg(vfloat4);
DROP AGGREGATE IF EXISTS avg(vfloat8);

DROP AGGREGATE IF EXISTS count(vint2);
DROP AGGREGATE IF EXISTS count(vint4);
DROP AGGREGATE IF EXISTS count(vint8);
DROP AGGREGATE IF EXISTS count(vfloat4);
DROP AGGREGATE IF EXISTS count(vfloat8);

DROP AGGREGATE IF EXISTS veccount(*);

DROP FUNCTION IF EXISTS vint2_accum(int8, vint2);
DROP FUNCTION IF EXISTS vint4_accum(int8, vint4);
DROP FUNCTION IF EXISTS vint8_accum(numeric, vint8);
DROP FUNCTION IF EXISTS vfloat4_accum(float4, vfloat4);
DROP FUNCTION IF EXISTS vfloat8_accum(float8, vfloat8);

DROP FUNCTION IF EXISTS vint2_avg_accum(bytea, vint2);
DROP FUNCTION IF EXISTS vint4_avg_accum(bytea, vint4);
DROP FUNCTION IF EXISTS vint8_avg_accum(bytea, vint8);
DROP FUNCTION IF EXISTS vfloat4_avg_accum(bytea, vfloat4);
DROP FUNCTION IF EXISTS vfloat8_avg_accum(bytea, vfloat8);

DROP FUNCTION IF EXISTS vint2_inc(int8, vint2);
DROP FUNCTION IF EXISTS vint4_inc(int8, vint4);
DROP FUNCTION IF EXISTS vint8_inc(int8, vint8);
DROP FUNCTION IF EXISTS vfloat4_inc(int8, vfloat4);
DROP FUNCTION IF EXISTS vfloat8_inc(int8, vfloat8);

DROP FUNCTION IF EXISTS vec_inc_any(int8);

drop type vint2 cascade;
drop type vint4 cascade;
drop type vint8 cascade;
drop type vfloat8 cascade;
drop type vfloat4 cascade;
drop type vbool cascade;
drop type vdateadt cascade;
