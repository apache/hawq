--
-- Catalog Upgrade Script (from 1.1 to 1.2)
--

-- drop and recreate hawq_toolkit schema first
-- if upgrade is successfull gpmigrator will recreate hawq_toolkit objects
DROP SCHEMA IF EXISTS hawq_toolkit CASCADE;
CREATE SCHEMA hawq_toolkit;

ALTER TABLE pg_catalog.pg_appendonly ADD COLUMN pagesize int4;
SELECT catDML('UPDATE pg_catalog.pg_appendonly set pagesize = 0;');

-- The line below is for gpsql-1812 as per BJ 
UPDATE pg_database SET dat2tablespace = (SELECT oid FROM pg_tablespace WHERE spcname = 'dfs_default') WHERE datname = 'template0' AND dat2tablespace = 1663;

-- update definitions of built-in functions

CREATE OR REPLACE FUNCTION pg_catalog.pg_get_constraintdef(oid) RETURNS text LANGUAGE internal STABLE STRICT READS SQL DATA AS 'pg_get_constraintdef' WITH (OID=1387);
SELECT catDML('COMMENT ON FUNCTION pg_catalog.pg_get_constraintdef(oid) IS ''constraint description''');

CREATE OR REPLACE FUNCTION pg_catalog.pg_get_constraintdef(oid, bool) RETURNS text LANGUAGE internal STABLE STRICT READS SQL DATA AS 'pg_get_constraintdef_ext' WITH (OID=2508);
SELECT catDML('COMMENT ON FUNCTION pg_catalog.pg_get_constraintdef(oid, bool) IS ''constraint description with pretty-print option''');

CREATE OR REPLACE FUNCTION pg_catalog.pg_get_partition_rule_def(oid) RETURNS text LANGUAGE internal STABLE STRICT READS SQL DATA AS 'pg_get_partition_rule_def' WITH (OID=5027);

CREATE OR REPLACE FUNCTION pg_catalog.pg_get_partition_rule_def(oid, bool) RETURNS text LANGUAGE internal STABLE STRICT READS SQL DATA AS 'pg_get_partition_rule_def_ext' WITH (OID=5028);
SELECT catDML('COMMENT ON FUNCTION pg_catalog.pg_get_partition_rule_def(oid, bool) IS ''partition configuration for a given rule''');

CREATE OR REPLACE FUNCTION pg_catalog.pg_get_partition_def(oid) RETURNS text LANGUAGE internal STABLE STRICT READS SQL DATA AS 'pg_get_partition_def' WITH (OID=5024);

CREATE OR REPLACE FUNCTION pg_catalog.pg_get_partition_def(oid, bool) RETURNS text LANGUAGE internal STABLE STRICT READS SQL DATA AS 'pg_get_partition_def_ext' WITH (OID=5025);
SELECT catDML('COMMENT ON FUNCTION pg_catalog.pg_get_partition_def(oid, bool) IS ''partition configuration for a given relation''');

CREATE OR REPLACE FUNCTION pg_catalog.pg_get_partition_def(oid, bool, bool) RETURNS text LANGUAGE internal STABLE STRICT READS SQL DATA AS 'pg_get_partition_def_ext2' WITH (OID=5034);
SELECT catDML('COMMENT ON FUNCTION pg_catalog.pg_get_partition_def(oid, bool, bool) IS ''partition configuration for a given relation''');

CREATE OR REPLACE FUNCTION pg_catalog.pg_stat_get_activity(pid integer, OUT datid oid, OUT procpid integer, OUT usesysid oid, OUT application_name text, OUT current_query text, OUT waiting boolean, OUT xact_start timestamp with time zone, OUT query_start timestamp with time zone, OUT backend_start timestamp with time zone, OUT client_addr inet, OUT client_port integer, OUT sess_id integer) RETURNS SETOF pg_catalog.record LANGUAGE internal VOLATILE AS 'pg_stat_get_activity' WITH (OID=6071);
SELECT catDML('COMMENT ON FUNCTION pg_catalog.pg_stat_get_activity(pid integer, OUT datid oid, OUT procpid integer, OUT usesysid oid, OUT application_name text, OUT current_query text, OUT waiting boolean, OUT xact_start timestamp with time zone, OUT query_start timestamp with time zone, OUT backend_start timestamp with time zone, OUT client_addr inet, OUT client_port integer, OUT sess_id integer) IS ''statistics: information about currently active backends''');

-- madlib
SET search_path = madlib, pg_catalog;

DROP AGGREGATE __lda_count_topic_agg(integer[], integer[], integer[], integer, integer);

DROP AGGREGATE array_agg(anyelement);

DROP AGGREGATE avg(double precision[]);

DROP AGGREGATE chi2_gof_test(bigint, double precision, bigint);

DROP AGGREGATE chi2_gof_test(bigint, double precision);

DROP AGGREGATE chi2_gof_test(bigint);

DROP AGGREGATE cox_prop_hazards_step(double precision[], double precision, double precision, double precision[], double precision[], double precision[]);

DROP AGGREGATE f_test(boolean, double precision);

DROP AGGREGATE ks_test(boolean, double precision, bigint, bigint);

DROP AGGREGATE linregr(double precision, double precision[]);

DROP AGGREGATE logregr_cg_step(boolean, double precision[], double precision[]);

DROP AGGREGATE logregr_igd_step(boolean, double precision[], double precision[]);

DROP AGGREGATE logregr_irls_step(boolean, double precision[], double precision[]);

DROP AGGREGATE matrix_agg(double precision[]);

DROP AGGREGATE mlogregr_irls_step(integer, integer, double precision[], double precision[]);

DROP AGGREGATE mw_test(boolean, double precision);

DROP AGGREGATE normalized_avg(double precision[]);

DROP AGGREGATE one_way_anova(integer, double precision);

DROP AGGREGATE t_test_one(double precision);

DROP AGGREGATE t_test_two_pooled(boolean, double precision);

DROP AGGREGATE t_test_two_unpooled(boolean, double precision);

DROP AGGREGATE weighted_sample(bigint, double precision);

DROP AGGREGATE weighted_sample(double precision[], double precision);

DROP AGGREGATE wsr_test(double precision, double precision);

DROP AGGREGATE wsr_test(double precision);

DROP AGGREGATE mean(svec);
DROP AGGREGATE svec_agg(double precision);
DROP AGGREGATE svec_count_nonzero(svec);
DROP AGGREGATE svec_median_inmemory(double precision);
DROP AGGREGATE svec_sum(svec);

DROP FUNCTION __assoc_rules_array_eq(text[], text[]);

DROP FUNCTION __filter_input_relation(character varying, character varying);

DROP FUNCTION __lda_count_topic_prefunc(integer[], integer[]);

DROP FUNCTION __lda_count_topic_sfunc(integer[], integer[], integer[], integer[], integer, integer);

DROP FUNCTION __lda_gibbs_sample(integer[], integer[], integer[], integer[], double precision, double precision, integer, integer, integer);

DROP FUNCTION __lda_random_assign(integer, integer);

DROP FUNCTION __lda_util_conorm_data(text, text, text, text);

DROP FUNCTION __lda_util_index_sort(double precision[]);

DROP FUNCTION __lda_util_norm_dataset(text, text, text);

DROP FUNCTION __lda_util_norm_vocab(text, text);

DROP FUNCTION __lda_util_norm_with_smoothing(double precision[], double precision);

DROP FUNCTION __lda_util_transpose(integer[]);

DROP FUNCTION __lda_util_unnest(integer[]);

DROP FUNCTION array_add(anyarray, anyarray);

DROP FUNCTION array_contains(anyarray, anyarray);

DROP FUNCTION array_div(anyarray, anyarray);

DROP FUNCTION array_dot(anyarray, anyarray);

DROP FUNCTION array_fill(anyarray, anyelement);

DROP FUNCTION array_max(anyarray);

DROP FUNCTION array_mean(anyarray);

DROP FUNCTION array_min(anyarray);

DROP FUNCTION array_mult(anyarray, anyarray);

DROP FUNCTION array_of_bigint(integer);

DROP FUNCTION array_of_float(integer);

DROP FUNCTION array_scalar_mult(anyarray, anyelement);

DROP FUNCTION array_sqrt(anyarray);

DROP FUNCTION array_stddev(anyarray);

DROP FUNCTION array_sub(anyarray, anyarray);

DROP FUNCTION array_sum(anyarray);

DROP FUNCTION array_sum_big(anyarray);

DROP FUNCTION assert(boolean, character varying);

DROP FUNCTION assoc_rules(double precision, double precision, text, text, text, text, boolean);

DROP FUNCTION assoc_rules(double precision, double precision, text, text, text, text);

DROP FUNCTION avg_vector_final(double precision[]);

DROP FUNCTION avg_vector_merge(double precision[], double precision[]);

DROP FUNCTION avg_vector_transition(double precision[], double precision[]);

DROP FUNCTION check_if_raises_error(text);

DROP FUNCTION chi2_gof_test_final(double precision[]);

DROP FUNCTION chi2_gof_test_merge_states(double precision[], double precision[]);

DROP FUNCTION chi2_gof_test_transition(double precision[], bigint, double precision, bigint);

DROP FUNCTION chi2_gof_test_transition(double precision[], bigint, double precision);

DROP FUNCTION chi2_gof_test_transition(double precision[], bigint);

DROP FUNCTION closest_column(double precision[], double precision[], regproc);

DROP FUNCTION closest_column(double precision[], double precision[]);

DROP FUNCTION closest_columns(double precision[], double precision[], integer, regproc);

DROP FUNCTION closest_columns(double precision[], double precision[], integer);

DROP FUNCTION compute_cox_prop_hazards(character varying, character varying, character varying, integer, character varying, double precision);

DROP FUNCTION compute_logregr(character varying, character varying, character varying, integer, character varying, double precision);

DROP FUNCTION compute_mlogregr(character varying, character varying, integer, character varying, integer, character varying, double precision);

DROP FUNCTION cox_prop_hazards(character varying, character varying, character varying, integer, character varying, double precision);

DROP FUNCTION cox_prop_hazards(character varying, character varying, character varying);

DROP FUNCTION cox_prop_hazards(character varying, character varying, character varying, integer);

DROP FUNCTION cox_prop_hazards(character varying, character varying, character varying, integer, character varying);

DROP FUNCTION cox_prop_hazards_step_final(double precision[]);

DROP FUNCTION cox_prop_hazards_step_transition(double precision[], double precision[], double precision, double precision, double precision[], double precision[], double precision[]);

DROP FUNCTION create_schema_pg_temp();

DROP FUNCTION dist_angle(double precision[], double precision[]);

DROP FUNCTION dist_norm1(double precision[], double precision[]);

DROP FUNCTION dist_norm2(double precision[], double precision[]);

DROP FUNCTION dist_tanimoto(double precision[], double precision[]);

DROP FUNCTION f_test_final(double precision[]);

DROP FUNCTION gen_rules_from_cfp(text, integer);

DROP FUNCTION intermediate_cox_prop_hazards(double precision[], double precision[]);

DROP FUNCTION internal_compute_kmeans(character varying, character varying, character varying, character varying, character varying);

DROP FUNCTION internal_compute_kmeans_random_seeding(character varying, character varying, character varying, character varying);

DROP FUNCTION internal_compute_kmeanspp_seeding(character varying, character varying, character varying, character varying);

DROP FUNCTION internal_cox_prop_hazards_result(double precision[]);

DROP FUNCTION internal_cox_prop_hazards_step_distance(double precision[], double precision[]);

DROP FUNCTION internal_execute_using_kmeans_args(character varying, double precision[], regproc, integer, double precision);

DROP FUNCTION internal_execute_using_kmeans_args(character varying, character varying, character varying, character varying, character varying, integer, double precision);

DROP FUNCTION internal_execute_using_kmeans_random_seeding_args(character varying, integer, double precision[]);

DROP FUNCTION internal_execute_using_kmeanspp_seeding_args(character varying, integer, regproc, double precision[]);

DROP FUNCTION internal_execute_using_silhouette_args(character varying, double precision[], regproc);

DROP FUNCTION internal_logregr_cg_result(double precision[]);

DROP FUNCTION internal_logregr_cg_step_distance(double precision[], double precision[]);

DROP FUNCTION internal_logregr_igd_result(double precision[]);

DROP FUNCTION internal_logregr_igd_step_distance(double precision[], double precision[]);

DROP FUNCTION internal_logregr_irls_result(double precision[]);

DROP FUNCTION internal_logregr_irls_step_distance(double precision[], double precision[]);

DROP FUNCTION internal_mlogregr_irls_result(double precision[]);

DROP FUNCTION internal_mlogregr_irls_step_distance(double precision[], double precision[]);

DROP FUNCTION isnan(double precision);

DROP FUNCTION kmeans(character varying, character varying, double precision[], character varying, character varying, integer, double precision);

DROP FUNCTION kmeans(character varying, character varying, double precision[], character varying, character varying, integer);

DROP FUNCTION kmeans(character varying, character varying, double precision[], character varying, character varying);

DROP FUNCTION kmeans(character varying, character varying, double precision[], character varying);

DROP FUNCTION kmeans(character varying, character varying, double precision[]);

DROP FUNCTION kmeans(character varying, character varying, character varying, character varying, character varying, character varying, integer, double precision);

DROP FUNCTION kmeans(character varying, character varying, character varying, character varying, character varying, character varying, integer);

DROP FUNCTION kmeans(character varying, character varying, character varying, character varying, character varying, character varying);

DROP FUNCTION kmeans(character varying, character varying, character varying, character varying, character varying);

DROP FUNCTION kmeans(character varying, character varying, character varying, character varying);

DROP FUNCTION kmeans_random(character varying, character varying, integer, character varying, character varying, integer, double precision);

DROP FUNCTION kmeans_random(character varying, character varying, integer, character varying, character varying, integer);

DROP FUNCTION kmeans_random(character varying, character varying, integer, character varying, character varying);

DROP FUNCTION kmeans_random(character varying, character varying, integer, character varying);

DROP FUNCTION kmeans_random(character varying, character varying, integer);

DROP FUNCTION kmeans_random_seeding(character varying, character varying, integer, double precision[]);

DROP FUNCTION kmeans_random_seeding(character varying, character varying, integer);

DROP FUNCTION kmeanspp(character varying, character varying, integer, character varying, character varying, integer, double precision);

DROP FUNCTION kmeanspp(character varying, character varying, integer, character varying, character varying, integer);

DROP FUNCTION kmeanspp(character varying, character varying, integer, character varying, character varying);

DROP FUNCTION kmeanspp(character varying, character varying, integer, character varying);

DROP FUNCTION kmeanspp(character varying, character varying, integer);

DROP FUNCTION kmeanspp_seeding(character varying, character varying, integer, character varying, double precision[]);

DROP FUNCTION kmeanspp_seeding(character varying, character varying, integer, character varying);

DROP FUNCTION kmeanspp_seeding(character varying, character varying, integer);

DROP FUNCTION ks_test_final(double precision[]);

DROP FUNCTION ks_test_transition(double precision[], boolean, double precision, bigint, bigint);

DROP FUNCTION linregr_final(bytea8);

DROP FUNCTION linregr_merge_states(bytea8, bytea8);

DROP FUNCTION linregr_transition(bytea8, double precision, double precision[]);

DROP FUNCTION logistic(double precision);

DROP FUNCTION logregr(character varying, character varying, character varying, integer, character varying, double precision);

DROP FUNCTION logregr(character varying, character varying, character varying);

DROP FUNCTION logregr(character varying, character varying, character varying, integer);

DROP FUNCTION logregr(character varying, character varying, character varying, integer, character varying);

DROP FUNCTION logregr_cg_step_final(double precision[]);

DROP FUNCTION logregr_cg_step_merge_states(double precision[], double precision[]);

DROP FUNCTION logregr_cg_step_transition(double precision[], boolean, double precision[], double precision[]);

DROP FUNCTION logregr_igd_step_final(double precision[]);

DROP FUNCTION logregr_igd_step_merge_states(double precision[], double precision[]);

DROP FUNCTION logregr_igd_step_transition(double precision[], boolean, double precision[], double precision[]);

DROP FUNCTION logregr_irls_step_final(double precision[]);

DROP FUNCTION logregr_irls_step_merge_states(double precision[], double precision[]);

DROP FUNCTION logregr_irls_step_transition(double precision[], boolean, double precision[], double precision[]);

DROP FUNCTION matrix_agg_final(double precision[]);

DROP FUNCTION matrix_agg_transition(double precision[], double precision[]);

DROP FUNCTION matrix_column(double precision[], integer);

DROP FUNCTION mlogregr(character varying, character varying, integer, character varying, integer, character varying, double precision);

DROP FUNCTION mlogregr(character varying, character varying, integer, character varying);

DROP FUNCTION mlogregr(character varying, character varying, integer, character varying, integer);

DROP FUNCTION mlogregr(character varying, character varying, integer, character varying, integer, character varying);

DROP FUNCTION mlogregr_irls_step_final(double precision[]);

DROP FUNCTION mlogregr_irls_step_merge_states(double precision[], double precision[]);

DROP FUNCTION mlogregr_irls_step_transition(double precision[], integer, integer, double precision[], double precision[]);

DROP FUNCTION mw_test_final(double precision[]);

DROP FUNCTION mw_test_transition(double precision[], boolean, double precision);

DROP FUNCTION lda_get_topic_desc(text, text, text, integer);

DROP FUNCTION lda_get_topic_word_count(text, text);

DROP FUNCTION lda_get_word_topic_count(text, text);

DROP FUNCTION lda_predict(text, text, text);

DROP FUNCTION lda_train(text, text, text, integer, integer, integer, double precision, double precision);

DROP FUNCTION noop();

DROP FUNCTION norm1(double precision[]);

DROP FUNCTION norm2(double precision[]);

DROP FUNCTION normalize(double precision[]);

DROP FUNCTION normalized_avg_vector_final(double precision[]);

DROP FUNCTION normalized_avg_vector_transition(double precision[], double precision[]);

DROP FUNCTION one_way_anova_final(double precision[]);

DROP FUNCTION one_way_anova_merge_states(double precision[], double precision[]);

DROP FUNCTION one_way_anova_transition(double precision[], integer, double precision);

DROP FUNCTION relative_error(double precision, double precision);

DROP FUNCTION relative_error(double precision[], double precision[]);

DROP FUNCTION simple_silhouette(character varying, character varying, double precision[], character varying);

DROP FUNCTION simple_silhouette(character varying, character varying, double precision[]);

DROP FUNCTION squared_dist_norm2(double precision[], double precision[]);

DROP FUNCTION t_test_merge_states(double precision[], double precision[]);

DROP FUNCTION t_test_one_final(double precision[]);

DROP FUNCTION t_test_one_transition(double precision[], double precision);

DROP FUNCTION t_test_two_pooled_final(double precision[]);

DROP FUNCTION t_test_two_transition(double precision[], boolean, double precision);

DROP FUNCTION t_test_two_unpooled_final(double precision[]);

DROP FUNCTION version();

DROP FUNCTION weighted_sample_final_int64(bytea8);

DROP FUNCTION weighted_sample_final_vector(bytea8);

DROP FUNCTION weighted_sample_merge_int64(bytea8, bytea8);

DROP FUNCTION weighted_sample_merge_vector(bytea8, bytea8);

DROP FUNCTION weighted_sample_transition_int64(bytea8, bigint, double precision);

DROP FUNCTION weighted_sample_transition_vector(bytea8, double precision[], double precision);

DROP FUNCTION wsr_test_final(double precision[]);

DROP FUNCTION wsr_test_transition(double precision[], double precision, double precision);

DROP FUNCTION wsr_test_transition(double precision[], double precision);

DROP FUNCTION angle(svec, svec);
DROP FUNCTION l1norm(svec, svec);
DROP FUNCTION l2norm(svec, svec);
DROP FUNCTION normalize(svec);
DROP FUNCTION svec_append(svec, double precision, bigint);
DROP FUNCTION svec_change(svec, integer, svec);
DROP FUNCTION svec_contains(svec, svec);
DROP FUNCTION svec_count(svec, svec);
DROP FUNCTION svec_dimension(svec);
DROP FUNCTION svec_dmax(double precision, double precision);
DROP FUNCTION svec_dmin(double precision, double precision);
DROP FUNCTION svec_elsum(svec);
DROP FUNCTION svec_elsum(double precision[]);
DROP FUNCTION svec_eq_non_zero(svec, svec);
DROP FUNCTION svec_from_string(text);
DROP FUNCTION svec_hash(svec);
DROP FUNCTION svec_l1norm(svec);
DROP FUNCTION svec_l1norm(double precision[]);
DROP FUNCTION svec_l2norm(svec);
DROP FUNCTION svec_l2norm(double precision[]);
DROP FUNCTION svec_lapply(text, svec);
DROP FUNCTION svec_log(svec);
DROP FUNCTION svec_mean_final(double precision[]);
DROP FUNCTION svec_mean_prefunc(double precision[], double precision[]);
DROP FUNCTION svec_mean_transition(double precision[], svec);
DROP FUNCTION svec_median(double precision[]);
DROP FUNCTION svec_median(svec);
DROP FUNCTION svec_nonbase_positions(svec, double precision);
DROP FUNCTION svec_nonbase_values(svec, double precision);
DROP FUNCTION svec_pivot(svec, double precision);
DROP FUNCTION svec_proj(svec, integer);
DROP FUNCTION svec_reverse(svec);
DROP FUNCTION svec_sfv(text[], text[]);
DROP FUNCTION svec_sort(text[]);
DROP FUNCTION svec_subvec(svec, integer, integer);
DROP FUNCTION svec_to_string(svec);
DROP FUNCTION svec_unnest(svec);
DROP FUNCTION tanimoto_distance(svec, svec);

DROP TYPE assoc_rules_results;

DROP TYPE chi2_test_result;

DROP TYPE closest_column_result;

DROP TYPE closest_columns_result;

DROP TYPE cox_prop_hazards_result;

DROP TYPE f_test_result;

DROP TYPE intermediate_cox_prop_hazards_result;

DROP TYPE kmeans_result;

DROP TYPE kmeans_state;

DROP TYPE ks_test_result;

DROP TYPE linregr_result;

DROP TYPE logregr_result;

DROP TYPE mlogregr_result;

DROP TYPE mw_test_result;

DROP TYPE lda_result;

DROP TYPE one_way_anova_result;

DROP TYPE t_test_result;

DROP TYPE wsr_test_result;

CREATE OR REPLACE FUNCTION svec_in(cstring) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_in'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_out(svec) RETURNS cstring
    AS '$libdir/gp_svec.so', 'svec_out'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_recv(internal) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_recv'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_send(svec) RETURNS bytea
    AS '$libdir/gp_svec.so', 'svec_send'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION float8arr_cast_float4(real) RETURNS double precision[]
    AS '$libdir/gp_svec.so', 'float8arr_cast_float4'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION float8arr_cast_float8(double precision) RETURNS double precision[]
    AS '$libdir/gp_svec.so', 'float8arr_cast_float8'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION float8arr_cast_int2(smallint) RETURNS double precision[]
    AS '$libdir/gp_svec.so', 'float8arr_cast_int2'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION float8arr_cast_int4(integer) RETURNS double precision[]
    AS '$libdir/gp_svec.so', 'float8arr_cast_int4'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION float8arr_cast_int8(bigint) RETURNS double precision[]
    AS '$libdir/gp_svec.so', 'float8arr_cast_int8'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION float8arr_cast_numeric(numeric) RETURNS double precision[]
    AS '$libdir/gp_svec.so', 'float8arr_cast_numeric'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION float8arr_div_float8arr(double precision[], double precision[]) RETURNS svec
    AS '$libdir/gp_svec.so', 'float8arr_div_float8arr'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION float8arr_div_svec(double precision[], svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'float8arr_div_svec'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION float8arr_eq(double precision[], double precision[]) RETURNS boolean
    AS '$libdir/gp_svec.so', 'float8arr_equals'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION float8arr_minus_float8arr(double precision[], double precision[]) RETURNS svec
    AS '$libdir/gp_svec.so', 'float8arr_minus_float8arr'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION float8arr_minus_svec(double precision[], svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'float8arr_minus_svec'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION float8arr_mult_float8arr(double precision[], double precision[]) RETURNS svec
    AS '$libdir/gp_svec.so', 'float8arr_mult_float8arr'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION float8arr_mult_svec(double precision[], svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'float8arr_mult_svec'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION float8arr_plus_float8arr(double precision[], double precision[]) RETURNS svec
    AS '$libdir/gp_svec.so', 'float8arr_plus_float8arr'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION float8arr_plus_svec(double precision[], svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'float8arr_plus_svec'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION svec_cast_float4(real) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_cast_float4'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_cast_float8(double precision) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_cast_float8'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_cast_float8arr(double precision[]) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_cast_float8arr'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_cast_int2(smallint) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_cast_int2'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_cast_int4(integer) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_cast_int4'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_cast_int8(bigint) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_cast_int8'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_cast_numeric(numeric) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_cast_numeric'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_cast_positions_float8arr(bigint[], double precision[], bigint, double precision) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_cast_positions_float8arr'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_concat(svec, svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_concat'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION svec_concat_replicate(integer, svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_concat_replicate'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION svec_div(svec, svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_div'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_div_float8arr(svec, double precision[]) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_div_float8arr'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION svec_dot(svec, svec) RETURNS double precision
    AS '$libdir/gp_svec.so', 'svec_dot'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_dot(double precision[], double precision[]) RETURNS double precision
    AS '$libdir/gp_svec.so', 'float8arr_dot'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_dot(svec, double precision[]) RETURNS double precision
    AS '$libdir/gp_svec.so', 'svec_dot_float8arr'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_dot(double precision[], svec) RETURNS double precision
    AS '$libdir/gp_svec.so', 'float8arr_dot_svec'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_eq(svec, svec) RETURNS boolean
    AS '$libdir/gp_svec.so', 'svec_eq'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_l2_cmp(svec, svec) RETURNS integer
    AS '$libdir/gp_svec.so', 'svec_l2_cmp'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION svec_l2_eq(svec, svec) RETURNS boolean
    AS '$libdir/gp_svec.so', 'svec_l2_eq'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION svec_l2_ge(svec, svec) RETURNS boolean
    AS '$libdir/gp_svec.so', 'svec_l2_ge'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION svec_l2_gt(svec, svec) RETURNS boolean
    AS '$libdir/gp_svec.so', 'svec_l2_gt'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION svec_l2_le(svec, svec) RETURNS boolean
    AS '$libdir/gp_svec.so', 'svec_l2_le'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION svec_l2_lt(svec, svec) RETURNS boolean
    AS '$libdir/gp_svec.so', 'svec_l2_lt'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION svec_l2_ne(svec, svec) RETURNS boolean
    AS '$libdir/gp_svec.so', 'svec_l2_ne'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION svec_minus(svec, svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_minus'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_minus_float8arr(svec, double precision[]) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_minus_float8arr'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION svec_mult(svec, svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_mult'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_mult_float8arr(svec, double precision[]) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_mult_float8arr'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION svec_plus(svec, svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_plus'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_plus_float8arr(svec, double precision[]) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_plus_float8arr'
    LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION svec_pow(svec, svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_pow'
    LANGUAGE c IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION svec_return_array(svec) RETURNS double precision[]
    AS '$libdir/gp_svec.so', 'svec_return_array'
    LANGUAGE c IMMUTABLE;

CREATE TABLE pg_catalog.pg_remote_credentials
(
	rcowner				oid not null,
	rcservice			text,
	rcremoteuser		text,
	rcremotepassword	text
)
WITH (relid=7076, reltype_oid=7077, toast_oid=7078, toast_index=7079, toast_reltype=7080, 
	  camelcase=RemoteCredentials, oid=false, shared=false);

CREATE UNIQUE INDEX pg_remote_credentials_owner_service_index ON pg_catalog.pg_remote_credentials(rcowner, rcservice) WITH (indexid=7081, CamelCase=RemoteCredentialsOwnerService);

CREATE OR REPLACE VIEW pg_catalog.pg_remote_logins AS
	SELECT
		A.rolname			AS rolname,
		C.rcservice			AS rcservice,
		C.rcremoteuser		AS rcremoteuser,
        '********'::text	AS rcremotepassword
	FROM pg_remote_credentials C
		 LEFT JOIN pg_authid A ON (A.oid = C.rcowner);

REVOKE ALL ON pg_remote_credentials FROM public;
