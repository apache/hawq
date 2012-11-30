-- Matrix multiply array[x][m] * array[m][y]
select matrix_multiply(ARRAY[[1,2]]::int2[], ARRAY[[1],[2]]::int2[]);
select matrix_multiply(ARRAY[[1,2]]::int2[], ARRAY[[1],[2]]::int4[]);
select matrix_multiply(ARRAY[[1,2]]::int2[], ARRAY[[1],[2]]::int8[]);
select matrix_multiply(ARRAY[[1,2]]::int2[], ARRAY[[1],[2]]::float4[]);
select matrix_multiply(ARRAY[[1,2]]::int2[], ARRAY[[1],[2]]::float8[]);
select matrix_multiply(ARRAY[[1,2]]::int2[], ARRAY[[1],[2]]::numeric[]);
select matrix_multiply(ARRAY[[1,2]]::int4[], ARRAY[[1],[2]]::int2[]);
select matrix_multiply(ARRAY[[1,2]]::int4[], ARRAY[[1],[2]]::int4[]);
select matrix_multiply(ARRAY[[1,2]]::int4[], ARRAY[[1],[2]]::int8[]);
select matrix_multiply(ARRAY[[1,2]]::int4[], ARRAY[[1],[2]]::float4[]);
select matrix_multiply(ARRAY[[1,2]]::int4[], ARRAY[[1],[2]]::float8[]);
select matrix_multiply(ARRAY[[1,2]]::int4[], ARRAY[[1],[2]]::numeric[]);
select matrix_multiply(ARRAY[[1,2]]::int8[], ARRAY[[1],[2]]::int2[]);
select matrix_multiply(ARRAY[[1,2]]::int8[], ARRAY[[1],[2]]::int4[]);
select matrix_multiply(ARRAY[[1,2]]::int8[], ARRAY[[1],[2]]::int8[]);
select matrix_multiply(ARRAY[[1,2]]::int8[], ARRAY[[1],[2]]::float4[]);
select matrix_multiply(ARRAY[[1,2]]::int8[], ARRAY[[1],[2]]::float8[]);
select matrix_multiply(ARRAY[[1,2]]::int8[], ARRAY[[1],[2]]::numeric[]);
select matrix_multiply(ARRAY[[1,2]]::float4[], ARRAY[[1],[2]]::int2[]);
select matrix_multiply(ARRAY[[1,2]]::float4[], ARRAY[[1],[2]]::int4[]);
select matrix_multiply(ARRAY[[1,2]]::float4[], ARRAY[[1],[2]]::int8[]);
select matrix_multiply(ARRAY[[1,2]]::float4[], ARRAY[[1],[2]]::float4[]);
select matrix_multiply(ARRAY[[1,2]]::float4[], ARRAY[[1],[2]]::float8[]);
select matrix_multiply(ARRAY[[1,2]]::float4[], ARRAY[[1],[2]]::numeric[]);
select matrix_multiply(ARRAY[[1,2]]::float8[], ARRAY[[1],[2]]::int2[]);
select matrix_multiply(ARRAY[[1,2]]::float8[], ARRAY[[1],[2]]::int4[]);
select matrix_multiply(ARRAY[[1,2]]::float8[], ARRAY[[1],[2]]::int8[]);
select matrix_multiply(ARRAY[[1,2]]::float8[], ARRAY[[1],[2]]::float4[]);
select matrix_multiply(ARRAY[[1,2]]::float8[], ARRAY[[1],[2]]::float8[]);
select matrix_multiply(ARRAY[[1,2]]::float8[], ARRAY[[1],[2]]::numeric[]);
select matrix_multiply(ARRAY[[1,2]]::numeric[], ARRAY[[1],[2]]::int2[]);
select matrix_multiply(ARRAY[[1,2]]::numeric[], ARRAY[[1],[2]]::int4[]);
select matrix_multiply(ARRAY[[1,2]]::numeric[], ARRAY[[1],[2]]::int8[]);
select matrix_multiply(ARRAY[[1,2]]::numeric[], ARRAY[[1],[2]]::float4[]);
select matrix_multiply(ARRAY[[1,2]]::numeric[], ARRAY[[1],[2]]::float8[]);
select matrix_multiply(ARRAY[[1,2]]::numeric[], ARRAY[[1],[2]]::numeric[]);


-- Matrix multiply array[x][m] * scalar
select matrix_multiply(ARRAY[[1,2]]::int2[], 5::int2);
select matrix_multiply(ARRAY[[1,2]]::int2[], 5::int4);
select matrix_multiply(ARRAY[[1,2]]::int2[], 5::int8);
select matrix_multiply(ARRAY[[1,2]]::int2[], 5::float4);
select matrix_multiply(ARRAY[[1,2]]::int2[], 5::float8);
select matrix_multiply(ARRAY[[1,2]]::int2[], 5::numeric);
select matrix_multiply(ARRAY[[1,2]]::int4[], 5::int2);
select matrix_multiply(ARRAY[[1,2]]::int4[], 5::int4);
select matrix_multiply(ARRAY[[1,2]]::int4[], 5::int8);
select matrix_multiply(ARRAY[[1,2]]::int4[], 5::float4);
select matrix_multiply(ARRAY[[1,2]]::int4[], 5::float8);
select matrix_multiply(ARRAY[[1,2]]::int4[], 5::numeric);
select matrix_multiply(ARRAY[[1,2]]::int8[], 5::int2);
select matrix_multiply(ARRAY[[1,2]]::int8[], 5::int4);
select matrix_multiply(ARRAY[[1,2]]::int8[], 5::int8);
select matrix_multiply(ARRAY[[1,2]]::int8[], 5::float4);
select matrix_multiply(ARRAY[[1,2]]::int8[], 5::float8);
select matrix_multiply(ARRAY[[1,2]]::int8[], 5::numeric);
select matrix_multiply(ARRAY[[1,2]]::float4[], 5::int2);
select matrix_multiply(ARRAY[[1,2]]::float4[], 5::int4);
select matrix_multiply(ARRAY[[1,2]]::float4[], 5::int8);
select matrix_multiply(ARRAY[[1,2]]::float4[], 5::float4);
select matrix_multiply(ARRAY[[1,2]]::float4[], 5::float8);
select matrix_multiply(ARRAY[[1,2]]::float4[], 5::numeric);
select matrix_multiply(ARRAY[[1,2]]::float8[], 5::int2);
select matrix_multiply(ARRAY[[1,2]]::float8[], 5::int4);
select matrix_multiply(ARRAY[[1,2]]::float8[], 5::int8);
select matrix_multiply(ARRAY[[1,2]]::float8[], 5::float4);
select matrix_multiply(ARRAY[[1,2]]::float8[], 5::float8);
select matrix_multiply(ARRAY[[1,2]]::float8[], 5::numeric);
select matrix_multiply(ARRAY[[1,2]]::numeric[], 5::int2);
select matrix_multiply(ARRAY[[1,2]]::numeric[], 5::int4);
select matrix_multiply(ARRAY[[1,2]]::numeric[], 5::int8);
select matrix_multiply(ARRAY[[1,2]]::numeric[], 5::float4);
select matrix_multiply(ARRAY[[1,2]]::numeric[], 5::float8);
select matrix_multiply(ARRAY[[1,2]]::numeric[], 5::numeric);


-- matrix add array[] + array[]
select matrix_add(ARRAY[[1,2]]::int2[], ARRAY[[1,2]]::int2[]);
select matrix_add(ARRAY[[1,2]]::int2[], ARRAY[[1,2]]::int4[]);
select matrix_add(ARRAY[[1,2]]::int2[], ARRAY[[1,2]]::int8[]);
select matrix_add(ARRAY[[1,2]]::int2[], ARRAY[[1,2]]::float4[]);
select matrix_add(ARRAY[[1,2]]::int2[], ARRAY[[1,2]]::float8[]);
select matrix_add(ARRAY[[1,2]]::int2[], ARRAY[[1,2]]::numeric[]);
select matrix_add(ARRAY[[1,2]]::int4[], ARRAY[[1,2]]::int2[]);
select matrix_add(ARRAY[[1,2]]::int4[], ARRAY[[1,2]]::int4[]);
select matrix_add(ARRAY[[1,2]]::int4[], ARRAY[[1,2]]::int8[]);
select matrix_add(ARRAY[[1,2]]::int4[], ARRAY[[1,2]]::float4[]);
select matrix_add(ARRAY[[1,2]]::int4[], ARRAY[[1,2]]::float8[]);
select matrix_add(ARRAY[[1,2]]::int4[], ARRAY[[1,2]]::numeric[]);
select matrix_add(ARRAY[[1,2]]::int8[], ARRAY[[1,2]]::int2[]);
select matrix_add(ARRAY[[1,2]]::int8[], ARRAY[[1,2]]::int4[]);
select matrix_add(ARRAY[[1,2]]::int8[], ARRAY[[1,2]]::int8[]);
select matrix_add(ARRAY[[1,2]]::int8[], ARRAY[[1,2]]::float4[]);
select matrix_add(ARRAY[[1,2]]::int8[], ARRAY[[1,2]]::float8[]);
select matrix_add(ARRAY[[1,2]]::int8[], ARRAY[[1,2]]::numeric[]);
select matrix_add(ARRAY[[1,2]]::float4[], ARRAY[[1,2]]::int2[]);
select matrix_add(ARRAY[[1,2]]::float4[], ARRAY[[1,2]]::int4[]);
select matrix_add(ARRAY[[1,2]]::float4[], ARRAY[[1,2]]::int8[]);
select matrix_add(ARRAY[[1,2]]::float4[], ARRAY[[1,2]]::float4[]);
select matrix_add(ARRAY[[1,2]]::float4[], ARRAY[[1,2]]::float8[]);
select matrix_add(ARRAY[[1,2]]::float4[], ARRAY[[1,2]]::numeric[]);
select matrix_add(ARRAY[[1,2]]::float8[], ARRAY[[1,2]]::int2[]);
select matrix_add(ARRAY[[1,2]]::float8[], ARRAY[[1,2]]::int4[]);
select matrix_add(ARRAY[[1,2]]::float8[], ARRAY[[1,2]]::int8[]);
select matrix_add(ARRAY[[1,2]]::float8[], ARRAY[[1,2]]::float4[]);
select matrix_add(ARRAY[[1,2]]::float8[], ARRAY[[1,2]]::float8[]);
select matrix_add(ARRAY[[1,2]]::float8[], ARRAY[[1,2]]::numeric[]);
select matrix_add(ARRAY[[1,2]]::numeric[], ARRAY[[1,2]]::int2[]);
select matrix_add(ARRAY[[1,2]]::numeric[], ARRAY[[1,2]]::int4[]);
select matrix_add(ARRAY[[1,2]]::numeric[], ARRAY[[1,2]]::int8[]);
select matrix_add(ARRAY[[1,2]]::numeric[], ARRAY[[1,2]]::float4[]);
select matrix_add(ARRAY[[1,2]]::numeric[], ARRAY[[1,2]]::float8[]);
select matrix_add(ARRAY[[1,2]]::numeric[], ARRAY[[1,2]]::numeric[]);

create table dtype
(tint2     int2[],
 tint4     int4[],
 tint8     int8[],
 tfloat4   float4[],
 tfloat8   float8[],
 tnumeric  numeric[]) 
Distributed randomly;

insert into dtype values(
  array[1,   2,   3],
  array[1,   2,   3],
  array[1,   2,   3],
  array[1.1, 2.2, 3.3],
  array[1.1, 2.2, 3.3],
  array[1.1, 2.2, 3.3]
);
insert into dtype values(
  array[5,   4,   3],
  array[5,   4,   3],
  array[5,   4,   3],
  array[5.1, 4.2, 3.3],
  array[5.1, 4.2, 3.3],
  array[5.1, 4.2, 3.3]
);

select sum(tint2)    from dtype;
select sum(tint4)    from dtype;
select sum(tint8)    from dtype;
select sum(tfloat4)  from dtype;
select sum(tfloat8)  from dtype;
select sum(tnumeric) from dtype;

-- should be able to handle null values during sumation
insert into dtype values(null, null, null, null, null, null);
select sum(tint2)    from dtype;
select sum(tint4)    from dtype;
select sum(tint8)    from dtype;
select sum(tfloat4)  from dtype;
select sum(tfloat8)  from dtype;
select sum(tnumeric) from dtype;

-- these should fail, but tell us what the return type of sum() was
select sum(tint2)::boolean[]     from dtype;  -- bigint[]
select sum(tint4)::boolean[]     from dtype;  -- bigint[]
select sum(tint8)::boolean[]     from dtype;  -- bigint[]
select sum(tfloat4)::boolean[]   from dtype;  -- float8[]
select sum(tfloat8)::boolean[]   from dtype;  -- float8[]
select sum(tnumeric)::boolean[]  from dtype;  -- float8[]

-- What would normal sum do?
select sum(tint2[1])::boolean     from dtype;  -- bigint
select sum(tint4[1])::boolean     from dtype;  -- bigint
select sum(tint8[1])::boolean     from dtype;  -- numeric  (bigint above)
select sum(tfloat4[1])::boolean   from dtype;  -- float4   (float8 above)
select sum(tfloat8[1])::boolean   from dtype;  -- float8
select sum(tnumeric[1])::boolean  from dtype;  -- numeric

-- Matrix inversion 
select pinv(array[[1,-1,3],[2,1,2],[-2,-2,1]])::numeric(10,8)[] as pinv;

-- Ensure that  AA' = A'A
select matrix_multiply(array[[1,-1,3],[2,1,2],[-2,-2,1]], 
                       pinv(array[[1,-1,3],[2,1,2],[-2,-2,1]]))::numeric(10,8)[] as "AA'";
select matrix_multiply(pinv(array[[1,-1,3],[2,1,2],[-2,-2,1]]),
                       array[[1,-1,3],[2,1,2],[-2,-2,1]])::numeric(10,8)[] as "A'A";

drop table dtype;


-- Check overflow
--SMALLINT: [-32768, 32767]
select matrix_add(array[32767]::smallint[], array[1]::smallint[]);      --overflow
select matrix_add(array[32766]::smallint[], array[1]::smallint[]);      --no overflow
select matrix_add(array[-32768]::smallint[], array[-1]::smallint[]);    --overflow
select matrix_add(array[-32767]::smallint[], array[-1]::smallint[]);    --no overflow
   
-- Check overflow
--INT: [-2147483648, 2147483647]
select matrix_add(array[2147483647]::int[], array[1]::int[]);   --overflow
select matrix_add(array[2147483646]::int[], array[1]::int[]);   --no overflow
select matrix_add(array[-2147483648]::int[], array[-1]::int[]); --overflow
select matrix_add(array[-2147483647]::int[], array[-1]::int[]); --no overflow
 
-- Check overflow
--BIGINT: [-9223372036854775808, 9223372036854775807]
select matrix_add(array[9223372036854775807]::bigint[], array[1]::bigint[]);    --overflow
select matrix_add(array[9223372036854775806]::bigint[], array[1]::int[]);       --no overflow
select matrix_add(array[-9223372036854775808]::bigint[], array[-1]::bigint[]);  --overflow
select matrix_add(array[-9223372036854775807]::bigint[], array[-1]::int[]);     --no overflow
 
-- Matrix_multiply will promote result to int64 or float8 automatically
select matrix_multiply(array[array[9223372036854775807/3]]::bigint[], array[array[4]]::bigint[]);       --overflow
select matrix_multiply(array[array[-9223372036854775808]]::bigint[], array[array[-1]]::bigint[]);       --overflow
 
select matrix_multiply(array[array[10e200], array[10e200]]::float8[], array[array[10e200]]::float8[]);  --overflow
 
