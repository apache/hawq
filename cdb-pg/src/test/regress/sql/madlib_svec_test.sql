SET client_min_messages TO ERROR;
DROP SCHEMA IF EXISTS madlib_install_check_gpsql_svec_test CASCADE;
CREATE SCHEMA madlib_install_check_gpsql_svec_test;
SET search_path = madlib_install_check_gpsql_svec_test, madlib;

create table test_pairs( id int, a madlib.svec, b madlib.svec );
insert into test_pairs values 
       (0, '{1,100,1}:{5,0,5}', '{50,50,2}:{1,2,10}'),
       (1, '{1,100,1}:{-5,0,-5}', '{50,50,2}:{-1,-2,-10}');
insert into test_pairs values 
       (11, '{1}:{0}', '{1}:{1}'),
       (12, '{1}:{5}', '{3}:{-8}'),
       (13, '{1}:{0}', '{1}:{NULL}'),
       (14, '{1,2,1}:{2,4,2}', '{2,1,1}:{0,3,5}'),
       (15, '{1,2,1}:{2,4,2}', '{2,1,1}:{NULL,3,5}');


select id, madlib.svec_plus(a,b) from test_pairs order by id;
select id, madlib.svec_plus(a,b) = madlib.svec_plus(b,a) from test_pairs order by id;
select id, madlib.svec_mult(a,b) from test_pairs order by id;
select id, madlib.svec_mult(a,b) = madlib.svec_mult(b,a) from test_pairs order by id;
select id, madlib.svec_minus(a, b) = madlib.svec_plus(madlib.svec_mult((-1)::svec,b), a) from test_pairs order by id;
select id, madlib.svec_pow(a,2::svec) = madlib.svec_mult(a,a) from test_pairs order by id;

select madlib.svec_plus('{1,2,3}:{4,5,6}', 5::madlib.svec);
select madlib.svec_plus(5::madlib.svec, '{1,2,3}:{4,5,6}');
select madlib.svec_plus(500::madlib.svec, '{1,2,3}:{4,null,6}');
select madlib.svec_div(500::madlib.svec, '{1,2,3}:{4,null,6}');
select madlib.svec_div('{1,2,3}:{4,null,6}', 500::madlib.svec);

-- Test operators between svec and float8[]
select ('{1,2,3,4}:{3,4,5,6}'::madlib.svec)           %*% ('{1,2,3,4}:{3,4,5,6}'::madlib.svec)::float8[];
select ('{1,2,3,4}:{3,4,5,6}'::madlib.svec)::float8[] %*% ('{1,2,3,4}:{3,4,5,6}'::madlib.svec);
select ('{1,2,3,4}:{3,4,5,6}'::madlib.svec)            /  ('{1,2,3,4}:{3,4,5,6}'::madlib.svec)::float8[];
select ('{1,2,3,4}:{3,4,5,6}'::madlib.svec)::float8[]  /  ('{1,2,3,4}:{3,4,5,6}'::madlib.svec);
select ('{1,2,3,4}:{3,4,5,6}'::madlib.svec)            *  ('{1,2,3,4}:{3,4,5,6}'::madlib.svec)::float8[];
select ('{1,2,3,4}:{3,4,5,6}'::madlib.svec)::float8[]  *  ('{1,2,3,4}:{3,4,5,6}'::madlib.svec);
select ('{1,2,3,4}:{3,4,5,6}'::madlib.svec)            +  ('{1,2,3,4}:{3,4,5,6}'::madlib.svec)::float8[];
select ('{1,2,3,4}:{3,4,5,6}'::madlib.svec)::float8[]  +  ('{1,2,3,4}:{3,4,5,6}'::madlib.svec);
select ('{1,2,3,4}:{3,4,5,6}'::madlib.svec)            -  ('{1,2,3,4}:{3,4,5,6}'::madlib.svec)::float8[];
select ('{1,2,3,4}:{3,4,5,6}'::madlib.svec)::float8[]  -  ('{1,2,3,4}:{3,4,5,6}'::madlib.svec);

select id, a, b, madlib.svec_concat(a,b), a, b from test_pairs order by id;

select id, madlib.svec_concat_replicate(0, b), b from test_pairs order by id;
select id, madlib.svec_concat_replicate(1, b) = b from test_pairs order by id;
select id, madlib.svec_concat_replicate(3, b), b from test_pairs order by id;


-- This vfunction test svec creation from position array
select madlib.svec_cast_positions_float8arr('{1,2,4,6,2,5}'::INT8[], '{.2,.3,.4,.5,.3,.1}'::FLOAT8[], 10000, 0.0);

DROP SCHEMA madlib_install_check_gpsql_svec_test CASCADE;

-- sanity check; don't delete madlib schema and its objects.
DROP SCHEMA madlib CASCADE;
