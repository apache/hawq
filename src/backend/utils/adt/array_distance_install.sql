-- --------------------------------------------------------------------
--
-- array_distance_install.sql
--
-- Support eculidean metric and cosine distance functions for array
--
-- --------------------------------------------------------------------

-- euclidean_metric_float4array(float4array,float4array) =>,float 
set gen_new_oid_value to 3135;
insert into pg_proc values ('euclidean_metric_float4array',11,10,12,'f','f','f','f' ,'i',2,700,'f','1021 1021',null,null,null,'euclidean_metric_float4array','-',null,'n');

-- euclidean_metric_float8array(float8array,,'f'loat8array) => double
set gen_new_oid_value to 3136;
insert into pg_proc values ('euclidean_metric_float8array',11,10,12,'f','f','f','f' ,'i',2,701,'f','1022 1022',null,null,null,'euclidean_metric_float8array','-',null,'n');

-- cosine_distance_float4array(float4array,,'f'loat4array) => double
set gen_new_oid_value to 3137;
insert into pg_proc values ('cosine_distance_float4array',11,10,12,'f','f','f','f','i',2,701,'f','1021 1021',null,null,null,'cosine_distance_float4array','-',null,'n');

-- cosine_distance_float8array(float8array,,'f'loat8array) => double
set gen_new_oid_value to 3138;
insert into pg_proc values ('cosine_distance_float8array',11,10,12,'f','f','f','f','i',2,701,'f','1022 1022',null,null,null,'cosine_distance_float8array','-',null,'n');
reset gen_new_oid_value;
