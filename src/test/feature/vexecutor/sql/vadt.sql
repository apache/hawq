
-- in/out functions
SELECT '1 1 1'::vint2;
SELECT '1 1 1'::vint4;
SELECT '1 1 1'::vint8;
SELECT '1 1 1'::vfloat4;
SELECT '1 1 1'::vfloat8;
SELECT 'false true false'::vbool;

-- operators +-*/ : v op v
select '1 1 1'::vint2 + '2 2 2'::vint2;
select '1 1 1'::vint2 - '2 2 2'::vint2;
select '1 1 1'::vint2 * '2 2 2'::vint2;
select '1 1 1'::vint2 / '2 2 2'::vint2;


select '1 1 1'::vint4 + '2 2 2'::vint4;
select '1 1 1'::vint4 - '2 2 2'::vint4;
select '1 1 1'::vint4 * '2 2 2'::vint4;
select '1 1 1'::vint4 / '2 2 2'::vint4;


select '1 1 1'::vint8 + '2 2 2'::vint8;
select '1 1 1'::vint8 - '2 2 2'::vint8;
select '1 1 1'::vint8 * '2 2 2'::vint8;
select '1 1 1'::vint8 / '2 2 2'::vint8;



select '1 1 1'::vfloat4 + '2 2 2'::vfloat4;
select '1 1 1'::vfloat4 - '2 2 2'::vfloat4;
select '1 1 1'::vfloat4 * '2 2 2'::vfloat4;
select '1 1 1'::vfloat4 / '2 2 2'::vfloat4;


select '1 1 1'::vfloat8 + '2 2 2'::vfloat8;
select '1 1 1'::vfloat8 - '2 2 2'::vfloat8;
select '1 1 1'::vfloat8 * '2 2 2'::vfloat8;
select '1 1 1'::vfloat8 / '2 2 2'::vfloat8;


--operator +-*/ "v op c
select '1 1 1'::vint2 + 2;
select '1 1 1'::vint2 - 2;
select '1 1 1'::vint2 * 2;
select '1 1 1'::vint2 / 2;


select '1 1 1'::vint4 + 2;
select '1 1 1'::vint4 - 2;
select '1 1 1'::vint4 * 2;
select '1 1 1'::vint4 / 2;

select '1 1 1'::vint8 + 2;
select '1 1 1'::vint8 - 2;
select '1 1 1'::vint8 * 2;
select '1 1 1'::vint8 / 2;


select '1 1 1'::vfloat4 + 2;
select '1 1 1'::vfloat4 - 2;
select '1 1 1'::vfloat4 * 2;
select '1 1 1'::vfloat4 / 2;


select '1 1 1'::vfloat8 + 2;
select '1 1 1'::vfloat8 - 2;
select '1 1 1'::vfloat8 * 2;
select '1 1 1'::vfloat8 / 2;

--operator cmp : v op v
--left vint2
select '1 1 1'::vint2 >  '2 2 2'::vint2;
select '1 1 1'::vint2 >= '2 2 2'::vint2;
select '1 1 1'::vint2 =  '2 2 2'::vint2;
select '1 1 1'::vint2 <  '2 2 2'::vint2;
select '1 1 1'::vint2 <= '2 2 2'::vint2;


select '1 1 1'::vint2 >  '2 2 2'::vint4;
select '1 1 1'::vint2 >= '2 2 2'::vint4;
select '1 1 1'::vint2 =  '2 2 2'::vint4;
select '1 1 1'::vint2 <  '2 2 2'::vint4;
select '1 1 1'::vint2 <= '2 2 2'::vint4;


select '1 1 1'::vint2 >  '2 2 2'::vint8;
select '1 1 1'::vint2 >= '2 2 2'::vint8;
select '1 1 1'::vint2 =  '2 2 2'::vint8;
select '1 1 1'::vint2 <  '2 2 2'::vint8;
select '1 1 1'::vint2 <= '2 2 2'::vint8;


select '1 1 1'::vint2 >  '2 2 2'::vfloat4;
select '1 1 1'::vint2 >= '2 2 2'::vfloat4;
select '1 1 1'::vint2 =  '2 2 2'::vfloat4;
select '1 1 1'::vint2 <  '2 2 2'::vfloat4;
select '1 1 1'::vint2 <= '2 2 2'::vfloat4;


select '1 1 1'::vint2 >  '2 2 2'::vfloat8;
select '1 1 1'::vint2 >= '2 2 2'::vfloat8;
select '1 1 1'::vint2 =  '2 2 2'::vfloat8;
select '1 1 1'::vint2 <  '2 2 2'::vfloat8;
select '1 1 1'::vint2 <= '2 2 2'::vfloat8;

--left vint4
select '1 1 1'::vint4 >  '2 2 2'::vint2;
select '1 1 1'::vint4 >= '2 2 2'::vint2;
select '1 1 1'::vint4 =  '2 2 2'::vint2;
select '1 1 1'::vint4 <  '2 2 2'::vint2;
select '1 1 1'::vint4 <= '2 2 2'::vint2;


select '1 1 1'::vint4 >  '2 2 2'::vint4;
select '1 1 1'::vint4 >= '2 2 2'::vint4;
select '1 1 1'::vint4 =  '2 2 2'::vint4;
select '1 1 1'::vint4 <  '2 2 2'::vint4;
select '1 1 1'::vint4 <= '2 2 2'::vint4;


select '1 1 1'::vint4 >  '2 2 2'::vint8;
select '1 1 1'::vint4 >= '2 2 2'::vint8;
select '1 1 1'::vint4 =  '2 2 2'::vint8;
select '1 1 1'::vint4 <  '2 2 2'::vint8;
select '1 1 1'::vint4 <= '2 2 2'::vint8;


select '1 1 1'::vint4 >  '2 2 2'::vfloat4;
select '1 1 1'::vint4 >= '2 2 2'::vfloat4;
select '1 1 1'::vint4 =  '2 2 2'::vfloat4;
select '1 1 1'::vint4 <  '2 2 2'::vfloat4;
select '1 1 1'::vint4 <= '2 2 2'::vfloat4;


select '1 1 1'::vint4 >  '2 2 2'::vfloat8;
select '1 1 1'::vint4 >= '2 2 2'::vfloat8;
select '1 1 1'::vint4 =  '2 2 2'::vfloat8;
select '1 1 1'::vint4 <  '2 2 2'::vfloat8;
select '1 1 1'::vint4 <= '2 2 2'::vfloat8;


--left vint8
select '1 1 1'::vint8 >  '2 2 2'::vint2;
select '1 1 1'::vint8 >= '2 2 2'::vint2;
select '1 1 1'::vint8 =  '2 2 2'::vint2;
select '1 1 1'::vint8 <  '2 2 2'::vint2;
select '1 1 1'::vint8 <= '2 2 2'::vint2;


select '1 1 1'::vint8 >  '2 2 2'::vint4;
select '1 1 1'::vint8 >= '2 2 2'::vint4;
select '1 1 1'::vint8 =  '2 2 2'::vint4;
select '1 1 1'::vint8 <  '2 2 2'::vint4;
select '1 1 1'::vint8 <= '2 2 2'::vint4;


select '1 1 1'::vint8 >  '2 2 2'::vint8;
select '1 1 1'::vint8 >= '2 2 2'::vint8;
select '1 1 1'::vint8 =  '2 2 2'::vint8;
select '1 1 1'::vint8 <  '2 2 2'::vint8;
select '1 1 1'::vint8 <= '2 2 2'::vint8;


select '1 1 1'::vint8 >  '2 2 2'::vfloat4;
select '1 1 1'::vint8 >= '2 2 2'::vfloat4;
select '1 1 1'::vint8 =  '2 2 2'::vfloat4;
select '1 1 1'::vint8 <  '2 2 2'::vfloat4;
select '1 1 1'::vint8 <= '2 2 2'::vfloat4;


select '1 1 1'::vint8 >  '2 2 2'::vfloat8;
select '1 1 1'::vint8 >= '2 2 2'::vfloat8;
select '1 1 1'::vint8 =  '2 2 2'::vfloat8;
select '1 1 1'::vint8 <  '2 2 2'::vfloat8;
select '1 1 1'::vint8 <= '2 2 2'::vfloat8;

--left vfloat4
select '1 1 1'::vfloat4 >  '2 2 2'::vint2;
select '1 1 1'::vfloat4 >= '2 2 2'::vint2;
select '1 1 1'::vfloat4 =  '2 2 2'::vint2;
select '1 1 1'::vfloat4 <  '2 2 2'::vint2;
select '1 1 1'::vfloat4 <= '2 2 2'::vint2;


select '1 1 1'::vfloat4 >  '2 2 2'::vint4;
select '1 1 1'::vfloat4 >= '2 2 2'::vint4;
select '1 1 1'::vfloat4 =  '2 2 2'::vint4;
select '1 1 1'::vfloat4 <  '2 2 2'::vint4;
select '1 1 1'::vfloat4 <= '2 2 2'::vint4;


select '1 1 1'::vfloat4 >  '2 2 2'::vint8;
select '1 1 1'::vfloat4 >= '2 2 2'::vint8;
select '1 1 1'::vfloat4 =  '2 2 2'::vint8;
select '1 1 1'::vfloat4 <  '2 2 2'::vint8;
select '1 1 1'::vfloat4 <= '2 2 2'::vint8;


select '1 1 1'::vfloat4 >  '2 2 2'::vfloat4;
select '1 1 1'::vfloat4 >= '2 2 2'::vfloat4;
select '1 1 1'::vfloat4 =  '2 2 2'::vfloat4;
select '1 1 1'::vfloat4 <  '2 2 2'::vfloat4;
select '1 1 1'::vfloat4 <= '2 2 2'::vfloat4;


select '1 1 1'::vfloat4 >  '2 2 2'::vfloat8;
select '1 1 1'::vfloat4 >= '2 2 2'::vfloat8;
select '1 1 1'::vfloat4 =  '2 2 2'::vfloat8;
select '1 1 1'::vfloat4 <  '2 2 2'::vfloat8;
select '1 1 1'::vfloat4 <= '2 2 2'::vfloat8;


--left vfloat8
select '1 1 1'::vfloat8 >  '2 2 2'::vint2;
select '1 1 1'::vfloat8 >= '2 2 2'::vint2;
select '1 1 1'::vfloat8 =  '2 2 2'::vint2;
select '1 1 1'::vfloat8 <  '2 2 2'::vint2;
select '1 1 1'::vfloat8 <= '2 2 2'::vint2;


select '1 1 1'::vfloat8 >  '2 2 2'::vint4;
select '1 1 1'::vfloat8 >= '2 2 2'::vint4;
select '1 1 1'::vfloat8 =  '2 2 2'::vint4;
select '1 1 1'::vfloat8 <  '2 2 2'::vint4;
select '1 1 1'::vfloat8 <= '2 2 2'::vint4;


select '1 1 1'::vfloat8 >  '2 2 2'::vint8;
select '1 1 1'::vfloat8 >= '2 2 2'::vint8;
select '1 1 1'::vfloat8 =  '2 2 2'::vint8;
select '1 1 1'::vfloat8 <  '2 2 2'::vint8;
select '1 1 1'::vfloat8 <= '2 2 2'::vint8;


select '1 1 1'::vfloat8 >  '2 2 2'::vfloat4;
select '1 1 1'::vfloat8 >= '2 2 2'::vfloat4;
select '1 1 1'::vfloat8 =  '2 2 2'::vfloat4;
select '1 1 1'::vfloat8 <  '2 2 2'::vfloat4;
select '1 1 1'::vfloat8 <= '2 2 2'::vfloat4;


select '1 1 1'::vfloat8 >  '2 2 2'::vfloat8;
select '1 1 1'::vfloat8 >= '2 2 2'::vfloat8;
select '1 1 1'::vfloat8 =  '2 2 2'::vfloat8;
select '1 1 1'::vfloat8 <  '2 2 2'::vfloat8;
select '1 1 1'::vfloat8 <= '2 2 2'::vfloat8;


--operator cmp : v op c
--left vint2
select '1 1 1'::vint2 >  2::int2;
select '1 1 1'::vint2 >= 2::int2;
select '1 1 1'::vint2 =  2::int2;
select '1 1 1'::vint2 <  2::int2;
select '1 1 1'::vint2 <= 2::int2;


select '1 1 1'::vint2 >  2::int4;
select '1 1 1'::vint2 >= 2::int4;
select '1 1 1'::vint2 =  2::int4;
select '1 1 1'::vint2 <  2::int4;
select '1 1 1'::vint2 <= 2::int4;


select '1 1 1'::vint2 >  2::int8;
select '1 1 1'::vint2 >= 2::int8;
select '1 1 1'::vint2 =  2::int8;
select '1 1 1'::vint2 <  2::int8;
select '1 1 1'::vint2 <= 2::int8;


select '1 1 1'::vint2 >  2::float4;
select '1 1 1'::vint2 >= 2::float4;
select '1 1 1'::vint2 =  2::float4;
select '1 1 1'::vint2 <  2::float4;
select '1 1 1'::vint2 <= 2::float4;


select '1 1 1'::vint2 >  2::float8;
select '1 1 1'::vint2 >= 2::float8;
select '1 1 1'::vint2 =  2::float8;
select '1 1 1'::vint2 <  2::float8;
select '1 1 1'::vint2 <= 2::float8;

--left vint4
select '1 1 1'::vint4 >  2::int2;
select '1 1 1'::vint4 >= 2::int2;
select '1 1 1'::vint4 =  2::int2;
select '1 1 1'::vint4 <  2::int2;
select '1 1 1'::vint4 <= 2::int2;


select '1 1 1'::vint4 >  2::int4;
select '1 1 1'::vint4 >= 2::int4;
select '1 1 1'::vint4 =  2::int4;
select '1 1 1'::vint4 <  2::int4;
select '1 1 1'::vint4 <= 2::int4;


select '1 1 1'::vint4 >  2::int8;
select '1 1 1'::vint4 >= 2::int8;
select '1 1 1'::vint4 =  2::int8;
select '1 1 1'::vint4 <  2::int8;
select '1 1 1'::vint4 <= 2::int8;


select '1 1 1'::vint4 >  2::float4;
select '1 1 1'::vint4 >= 2::float4;
select '1 1 1'::vint4 =  2::float4;
select '1 1 1'::vint4 <  2::float4;
select '1 1 1'::vint4 <= 2::float4;


select '1 1 1'::vint4 >  2::float8;
select '1 1 1'::vint4 >= 2::float8;
select '1 1 1'::vint4 =  2::float8;
select '1 1 1'::vint4 <  2::float8;
select '1 1 1'::vint4 <= 2::float8;


--left vint8
select '1 1 1'::vint8 >  2::int2;
select '1 1 1'::vint8 >= 2::int2;
select '1 1 1'::vint8 =  2::int2;
select '1 1 1'::vint8 <  2::int2;
select '1 1 1'::vint8 <= 2::int2;


select '1 1 1'::vint8 >  2::int4;
select '1 1 1'::vint8 >= 2::int4;
select '1 1 1'::vint8 =  2::int4;
select '1 1 1'::vint8 <  2::int4;
select '1 1 1'::vint8 <= 2::int4;


select '1 1 1'::vint8 >  2::int8;
select '1 1 1'::vint8 >= 2::int8;
select '1 1 1'::vint8 =  2::int8;
select '1 1 1'::vint8 <  2::int8;
select '1 1 1'::vint8 <= 2::int8;


select '1 1 1'::vint8 >  2::float4;
select '1 1 1'::vint8 >= 2::float4;
select '1 1 1'::vint8 =  2::float4;
select '1 1 1'::vint8 <  2::float4;
select '1 1 1'::vint8 <= 2::float4;


select '1 1 1'::vint8 >  2::float8;
select '1 1 1'::vint8 >= 2::float8;
select '1 1 1'::vint8 =  2::float8;
select '1 1 1'::vint8 <  2::float8;
select '1 1 1'::vint8 <= 2::float8;

--left vfloat4
select '1 1 1'::vfloat4 >  2::int2;
select '1 1 1'::vfloat4 >= 2::int2;
select '1 1 1'::vfloat4 =  2::int2;
select '1 1 1'::vfloat4 <  2::int2;
select '1 1 1'::vfloat4 <= 2::int2;


select '1 1 1'::vfloat4 >  2::int4;
select '1 1 1'::vfloat4 >= 2::int4;
select '1 1 1'::vfloat4 =  2::int4;
select '1 1 1'::vfloat4 <  2::int4;
select '1 1 1'::vfloat4 <= 2::int4;


select '1 1 1'::vfloat4 >  2::int8;
select '1 1 1'::vfloat4 >= 2::int8;
select '1 1 1'::vfloat4 =  2::int8;
select '1 1 1'::vfloat4 <  2::int8;
select '1 1 1'::vfloat4 <= 2::int8;


select '1 1 1'::vfloat4 >  2::float4;
select '1 1 1'::vfloat4 >= 2::float4;
select '1 1 1'::vfloat4 =  2::float4;
select '1 1 1'::vfloat4 <  2::float4;
select '1 1 1'::vfloat4 <= 2::float4;


select '1 1 1'::vfloat4 >  2::float8;
select '1 1 1'::vfloat4 >= 2::float8;
select '1 1 1'::vfloat4 =  2::float8;
select '1 1 1'::vfloat4 <  2::float8;
select '1 1 1'::vfloat4 <= 2::float8;


--left vfloat8
select '1 1 1'::vfloat8 >  2::int2;
select '1 1 1'::vfloat8 >= 2::int2;
select '1 1 1'::vfloat8 =  2::int2;
select '1 1 1'::vfloat8 <  2::int2;
select '1 1 1'::vfloat8 <= 2::int2;


select '1 1 1'::vfloat8 >  2::int4;
select '1 1 1'::vfloat8 >= 2::int4;
select '1 1 1'::vfloat8 =  2::int4;
select '1 1 1'::vfloat8 <  2::int4;
select '1 1 1'::vfloat8 <= 2::int4;


select '1 1 1'::vfloat8 >  2::int8;
select '1 1 1'::vfloat8 >= 2::int8;
select '1 1 1'::vfloat8 =  2::int8;
select '1 1 1'::vfloat8 <  2::int8;
select '1 1 1'::vfloat8 <= 2::int8;


select '1 1 1'::vfloat8 >  2::float4;
select '1 1 1'::vfloat8 >= 2::float4;
select '1 1 1'::vfloat8 =  2::float4;
select '1 1 1'::vfloat8 <  2::float4;
select '1 1 1'::vfloat8 <= 2::float4;


select '1 1 1'::vfloat8 >  2::float8;
select '1 1 1'::vfloat8 >= 2::float8;
select '1 1 1'::vfloat8 =  2::float8;
select '1 1 1'::vfloat8 <  2::float8;
select '1 1 1'::vfloat8 <= 2::float8;


