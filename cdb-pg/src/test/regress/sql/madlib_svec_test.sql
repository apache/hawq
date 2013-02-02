SET client_min_messages TO ERROR;
DROP SCHEMA IF EXISTS madlib_install_check_gpsql_svec_test CASCADE;
CREATE SCHEMA madlib_install_check_gpsql_svec_test;
SET search_path = madlib_install_check_gpsql_svec_test, madlib;

select madlib.svec_dmin(1000,1000.1);
select madlib.svec_dmin(1000,NULL);
select madlib.svec_dmin(NULL,1000);
select madlib.svec_dmin(NULL,NULL);
select madlib.svec_dmax(1000,1000.1);
select madlib.svec_dmax(1000,NULL);
select madlib.svec_dmax(NULL,1000);
select madlib.svec_dmax(NULL,NULL);


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


select id, madlib.svec_count(a,b) from test_pairs where madlib.svec_dimension(a) = madlib.svec_dimension(b) order by id;
select id, madlib.svec_plus(a,b) from test_pairs order by id;
select id, madlib.svec_plus(a,b) = madlib.svec_plus(b,a) from test_pairs order by id;
select id, madlib.svec_mult(a,b) from test_pairs order by id;
select id, madlib.svec_mult(a,b) = madlib.svec_mult(b,a) from test_pairs order by id;
--select id, madlib.svec_div(a,b) = madlib.svec_mult(a, madlib.svec_pow(b,(-1)::svec)) from test_pairs order by id;
select id, madlib.svec_minus(a, b) = madlib.svec_plus(madlib.svec_mult((-1)::svec,b), a) from test_pairs order by id;
select id, madlib.svec_pow(a,2::svec) = madlib.svec_mult(a,a) from test_pairs order by id;

select id, madlib.svec_dot(a,b) from test_pairs where madlib.svec_dimension(a) = madlib.svec_dimension(b) order by id;
select id, madlib.svec_dot(a,b) = madlib.svec_dot(b,a) from test_pairs where madlib.svec_dimension(a) = madlib.svec_dimension(b) order by id;
select id, madlib.svec_dot(a,b::float8[]) = madlib.svec_dot(b,a::float8[]) from test_pairs where madlib.svec_dimension(a) = madlib.svec_dimension(b) order by id;
select id, madlib.svec_dot(a::float8[],b) = madlib.svec_dot(b::float8[],a) from test_pairs where madlib.svec_dimension(a) = madlib.svec_dimension(b) order by id;
select id, madlib.svec_dot(a::float8[],b::float8[]) = madlib.svec_dot(b::float8[],a::float8[]) from test_pairs where madlib.svec_dimension(a) = madlib.svec_dimension(b) order by id;

select id, madlib.svec_l2norm(a), madlib.svec_l2norm(a::float[]), madlib.svec_l2norm(b), madlib.svec_l2norm(b::float8[]) from test_pairs order by id;
--select id, madlib.svec_l1norm(a), madlib.svec_l1norm(a::float[]), madlib.svec_l1norm(b), madlib.svec_l1norm(b::float8[]) from test_pairs order by id;

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

-- these should produce error messages 
/*
select '{10000000000000000000}:{1}'::madlib.svec ;
select '{1,null,2}:{2,3,4}'::madlib.svec;
select madlib.svec_count('{1,1,1}:{3,4,5}', '{2,2}:{1,3}');
select madlib.svec_plus('{1,1,1}:{3,4,5}', '{2,2}:{1,3}');
select madlib.svec_minus('{1,1,1}:{3,4,5}', '{2,2}:{1,3}');
select madlib.svec_mult('{1,1,1}:{3,4,5}', '{2,2}:{1,3}');
select madlib.svec_div('{1,1,1}:{3,4,5}', '{2,2}:{1,3}');
*/

select madlib.svec_unnest('{1}:{5}'::madlib.svec);
select madlib.svec_unnest('{1,2,3,4}:{5,6,7,8}'::madlib.svec);
select madlib.svec_unnest('{1,2,3,4}:{5,6,null,8}'::madlib.svec);
--select id, madlib.svec_unnest(a),a from test_pairs where id >= 10 order by id;

select madlib.svec_pivot('{1}:{5}', 2);
select madlib.svec_pivot('{1}:{5}', 5);
select madlib.svec_pivot('{1}:{5}', null);
select madlib.svec_pivot('{1}:{null}', 5);
select madlib.svec_pivot('{1}:{null}', null);
select madlib.svec_pivot('{1,2,3,4}:{5,6,7,8}'::madlib.svec, 2);
select id, madlib.svec_pivot(a, 5), a, madlib.svec_pivot(a,6), a, madlib.svec_pivot(a,2) from test_pairs order by id;
select id, madlib.svec_pivot(b, 5), b, madlib.svec_pivot(b,6), b, madlib.svec_pivot(b,null) from test_pairs order by id;

select madlib.svec_elsum('{1}:{-5}'::madlib.svec);
select id, a, madlib.svec_elsum(a), a, b, madlib.svec_elsum(b), b from test_pairs order by id;
select id, madlib.svec_elsum(a) = madlib.svec_elsum(a::float8[]) from test_pairs order by id;
select id, madlib.svec_elsum(b) = madlib.svec_elsum(b::float8[]) from test_pairs order by id;

select id, a, madlib.svec_median(a), a from test_pairs order by id;
select id, b, madlib.svec_median(b), b from test_pairs order by id;
select id, madlib.svec_median(a) = madlib.svec_median(a::float8[]),
           madlib.svec_median(b) = madlib.svec_median(b::float8[]) from test_pairs order by id;

select id, a, b, madlib.svec_concat(a,b), a, b from test_pairs order by id;

select id, madlib.svec_concat_replicate(0, b), b from test_pairs order by id;
select id, madlib.svec_concat_replicate(1, b) = b from test_pairs order by id;
select id, madlib.svec_concat_replicate(3, b), b from test_pairs order by id;
-- select id, madlib.svec_concat_replicate(-2, b), b from test_pairs order by id; -- this should produce error message

select id, madlib.svec_dimension(a), a, madlib.svec_dimension(b), b from test_pairs order by id;

select madlib.svec_lapply('sqrt', null); 
select id, madlib.svec_lapply('sqrt', madlib.svec_lapply('abs', a)), a from test_pairs order by id;
select id, madlib.svec_lapply('sqrt', madlib.svec_lapply('abs', b)), b from test_pairs order by id;

select madlib.svec_append(null::madlib.svec, 220::float8, 20::int8); 
select id, madlib.svec_append(a, 50, 100), a, madlib.svec_append(b, null, 50), b from test_pairs order by id;

select madlib.svec_proj(a,1), a, madlib.svec_proj(b,1), b from test_pairs order by id;
-- select madlib.svec_proj(a,2), a, madlib.svec_proj(b,2), b from test_pairs order by id; -- this should result in an appropriate error message

select madlib.svec_subvec('{1,20,30,10,600,2}:{1,2,3,4,5,6}', 3,69);
select madlib.svec_subvec('{1,20,30,10,600,2}:{1,2,3,4,5,6}', 69,3);
select madlib.svec_subvec(a,2,4), a from test_pairs where madlib.svec_dimension(a) >= 4 order by id;
select madlib.svec_subvec(a,2,madlib.svec_dimension(a)-1), a from test_pairs where madlib.svec_dimension(a) >= 2 order by id;
-- select madlib.svec_subvec(a,madlib.svec_dimension(a)-1,0), a from test_pairs where madlib.svec_dimension(a) >= 2 order by id;

select madlib.svec_reverse(a), a, madlib.svec_reverse(b), b from test_pairs order by id;
select madlib.svec_subvec('{1,20,30,10,600,2}:{1,2,3,4,5,6}', 3,69) =
       madlib.svec_reverse(madlib.svec_subvec('{1,20,30,10,600,2}:{1,2,3,4,5,6}', 69,3));

select madlib.svec_change('{1,20,30,10,600,2}:{1,2,3,4,5,6}', 3, '{2,3}:{4,null}');
select madlib.svec_change(a,1,'{1}:{-50}'), a from test_pairs order by id;

-- Test the multi-concatenation and show sizes compared with a normal array
create table corpus_proj as (select 10000 *|| ('{45,2,35,4,15,1}:{0,1,0,1,0,2}'::madlib.svec) result );
create table corpus_proj_array as (select result::float8[] from corpus_proj);
-- Calculate on-disk size of sparse vector
--select pg_size_pretty(pg_total_relation_size('corpus_proj'));
-- Calculate on-disk size of normal array
--select pg_size_pretty(pg_total_relation_size('corpus_proj_array'));

-- Calculate L1 norm from sparse vector
select madlib.svec_l1norm(result) from corpus_proj;
-- Calculate L1 norm from float8[]
select madlib.svec_l1norm(result) from corpus_proj_array;
-- Calculate L2 norm from sparse vector
select madlib.svec_l2norm(result) from corpus_proj;
-- Calculate L2 norm from float8[]
select madlib.svec_l2norm(result) from corpus_proj_array;

create table svec_svec as (
select 
    1000 *|| ('{45,2,35,4,15,1}:{0,1,0,1,0,2}'::madlib.svec) result1,
    1000 *|| ('{35,2,45,4,15,1}:{2,0,1,1,0,4}'::madlib.svec) result2 );
-- Calculate L1 norm from two sparse vectors
select madlib.l1norm(result1, result2) from svec_svec;
-- Calculate L2 norm from two sparse vectors
select madlib.l2norm(result1, result2) from svec_svec;
-- Calculate angle between two sparse vectors
select madlib.angle(result1, result2) from svec_svec;
-- Calculate tanimoto distance between two sparse vectors
select madlib.tanimoto_distance(result1, result2) from svec_svec;

-- Calculate normalized vectors
select madlib.normalize(result) from corpus_proj;

-- Test the pivot operator 
create table pivot_test(a float8);
insert into pivot_test values (0),(1),(0),(2),(3);
-- select madlib.svec_agg(a) from pivot_test;
select madlib.svec_l1norm(madlib.svec_agg(a)) from pivot_test;
-- Answer should be 5
select madlib.svec_median(madlib.svec_agg(a)) from (select generate_series(1,9) a) foo;
-- Answer should be a 10-wide vector
-- select madlib.svec_agg(a) from (select trunc(random()*10) a,generate_series(1,100000) order by a) foo;
-- Average is 4.50034, median is 5
select madlib.svec_median('{9960,9926,10053,9993,10080,10050,9938,9941,10030,10029}:{1,9,8,7,6,5,4,3,2,0}'::madlib.svec);
select madlib.svec_median('{9960,9926,10053,9993,10080,10050,9938,9941,10030,10029}:{1,9,8,7,6,5,4,3,2,0}'::madlib.svec::float8[]);

-- This vfunction test svec creation from position array
select madlib.svec_cast_positions_float8arr('{1,2,4,6,2,5}'::INT8[], '{.2,.3,.4,.5,.3,.1}'::FLOAT8[], 10000, 0.0);

-- test of functions returning positions and values of non-base values
select madlib.svec_nonbase_values('{1,2,3,1000,4}:{1,2,3,0,4}'::madlib.SVEC, 0.0::float8);
select madlib.svec_nonbase_positions('{1,2,3,1000,4}:{1,2,3,0,4}'::madlib.SVEC, 0.0::float8);

-- svec conversion to and from string
select madlib.svec_to_string('{2,3}:{4,5}');
select madlib.svec_from_string('{2,3}:{4,5}');

-- UDA: mean(svec) 
create table test_svec (a int, b madlib.svec);
select madlib.mean(b) from test_svec;
insert into test_svec select 1, '{1,2,3}'::float[]::madlib.svec;
insert into test_svec select 2, '{2,2.5,3.1}'::float[]::madlib.svec;
insert into test_svec select 3, '{3,3,3.2}'::float[]::madlib.svec;
select madlib.mean(b) from test_svec;

DROP SCHEMA madlib_install_check_gpsql_svec_test CASCADE;
