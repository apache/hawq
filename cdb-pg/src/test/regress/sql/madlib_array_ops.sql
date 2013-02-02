SET client_min_messages TO ERROR;
DROP SCHEMA IF EXISTS madlib_install_check_gpsql_array_ops CASCADE;
CREATE SCHEMA madlib_install_check_gpsql_array_ops;
SET search_path = madlib_install_check_gpsql_array_ops, madlib;

SELECT
    assert(abs(res1 + res2 + res3 - 17371.7254700712) < 0.01, 'FAIL')
FROM
(
    SELECT
        madlib.array_dot(
            madlib.array_mult(madlib.array_add(an,b),
            madlib.array_sub(an,b)),
        madlib.array_mult(
            madlib.array_div(an,b),
            madlib.normalize(an))) res1 
    FROM (SELECT array[1,2,3]::FLOAT[] an, array[4,5,7]::FLOAT[] b) t
) t1,
(
    SELECT
        (madlib.array_max(b) + 
         madlib.array_min(b) + 
         madlib.array_sum(b) +
         madlib.array_sum_big(b) +
         madlib.array_mean(b) + 
         madlib.array_stddev(b)) res2 
    FROM (SELECT array[4,5,7,NULL]::FLOAT[] b) t
) t2,
(
    SELECT
        madlib.array_sum(
            madlib.array_scalar_mult(
                madlib.array_fill(
                    madlib.array_of_float(20),
                    234.343::FLOAT8),3.7::FLOAT)) res3
) t3;

CREATE table test AS SELECT generate_series(1,100) x;
SELECT array(SELECT unnest(madlib.array_agg(x)) order by 1) FROM test;

DROP SCHEMA madlib_install_check_gpsql_array_ops CASCADE;
