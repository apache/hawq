
SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET default_with_oids = false;


CREATE SCHEMA madlib;
CREATE LANGUAGE plpythonu;


SET search_path = madlib, pg_catalog;


CREATE TYPE bytea8;



CREATE FUNCTION bytea8in(cstring) RETURNS bytea8
    AS $$byteain$$
    LANGUAGE internal IMMUTABLE STRICT;



CREATE FUNCTION bytea8out(bytea8) RETURNS cstring
    AS $$byteaout$$
    LANGUAGE internal IMMUTABLE STRICT;



CREATE FUNCTION bytea8recv(internal) RETURNS bytea8
    AS $$bytearecv$$
    LANGUAGE internal IMMUTABLE STRICT;



CREATE FUNCTION bytea8send(bytea8) RETURNS bytea
    AS $$byteasend$$
    LANGUAGE internal IMMUTABLE STRICT;



CREATE TYPE bytea8 (
    INTERNALLENGTH = variable,
    INPUT = bytea8in,
    OUTPUT = bytea8out,
    RECEIVE = bytea8recv,
    SEND = bytea8send,
    ALIGNMENT = double,
    STORAGE = plain
);



CREATE TYPE svec;



CREATE FUNCTION svec_in(cstring) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_in'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_out(svec) RETURNS cstring
    AS '$libdir/libmadlib.so', 'svec_out'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_recv(internal) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_recv'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_send(svec) RETURNS bytea
    AS '$libdir/libmadlib.so', 'svec_send'
    LANGUAGE c IMMUTABLE STRICT;



CREATE TYPE svec (
    INTERNALLENGTH = variable,
    INPUT = svec_in,
    OUTPUT = svec_out,
    RECEIVE = svec_recv,
    SEND = svec_send,
    ALIGNMENT = double,
    STORAGE = extended
);



CREATE TYPE assoc_rules_results AS (
	output_schema text,
	output_table text,
	total_rules integer,
	total_time interval
);



CREATE TYPE chi2_test_result AS (
	statistic double precision,
	p_value double precision,
	df bigint,
	phi double precision,
	contingency_coef double precision
);



CREATE TYPE closest_column_result AS (
	column_id integer,
	distance double precision
);



CREATE TYPE closest_columns_result AS (
	column_ids integer[],
	distances double precision[]
);



CREATE TYPE cox_prop_hazards_result AS (
	coef double precision[],
	loglikelihood double precision,
	std_err double precision[],
	z_stats double precision[],
	p_values double precision[],
	condition_no double precision,
	num_iterations integer
);



CREATE TYPE f_test_result AS (
	statistic double precision,
	df1 double precision,
	df2 double precision,
	p_value_one_sided double precision,
	p_value_two_sided double precision
);



CREATE TYPE intermediate_cox_prop_hazards_result AS (
	x double precision[],
	exp_coef_x double precision,
	x_exp_coef_x double precision[],
	x_xtrans_exp_coef_x double precision[]
);



CREATE TYPE kmeans_result AS (
	centroids double precision[],
	objective_fn double precision,
	frac_reassigned double precision,
	num_iterations integer
);



CREATE TYPE kmeans_state AS (
	centroids double precision[],
	old_centroid_ids integer[],
	objective_fn double precision,
	frac_reassigned double precision
);



CREATE TYPE ks_test_result AS (
	statistic double precision,
	k_statistic double precision,
	p_value double precision
);



CREATE TYPE linregr_result AS (
	coef double precision[],
	r2 double precision,
	std_err double precision[],
	t_stats double precision[],
	p_values double precision[],
	condition_no double precision
);



CREATE TYPE logregr_result AS (
	coef double precision[],
	log_likelihood double precision,
	std_err double precision[],
	z_stats double precision[],
	p_values double precision[],
	odds_ratios double precision[],
	condition_no double precision,
	num_iterations integer
);



CREATE TYPE mlogregr_result AS (
	coef double precision[],
	log_likelihood double precision,
	std_err double precision[],
	z_stats double precision[],
	p_values double precision[],
	odds_ratios double precision[],
	condition_no double precision,
	num_iterations integer
);



CREATE TYPE mw_test_result AS (
	statistic double precision,
	u_statistic double precision,
	p_value_one_sided double precision,
	p_value_two_sided double precision
);



CREATE TYPE lda_result AS (
	output_table text,
	description text
);



CREATE TYPE one_way_anova_result AS (
	sum_squares_between double precision,
	sum_squares_within double precision,
	df_between bigint,
	df_within bigint,
	mean_squares_between double precision,
	mean_squares_within double precision,
	statistic double precision,
	p_value double precision
);



CREATE TYPE t_test_result AS (
	statistic double precision,
	df double precision,
	p_value_one_sided double precision,
	p_value_two_sided double precision
);



CREATE TYPE wsr_test_result AS (
	statistic double precision,
	rank_sum_pos double precision,
	rank_sum_neg double precision,
	num bigint,
	z_statistic double precision,
	p_value_one_sided double precision,
	p_value_two_sided double precision
);



CREATE FUNCTION __assoc_rules_array_eq(arr1 text[], arr2 text[]) RETURNS boolean
    AS $_$
    SELECT COUNT(*) = array_upper($1, 1) AND array_upper($1, 1) = array_upper($2, 1)
    FROM (SELECT unnest($1) id) t1, (SELECT unnest($2) id) t2
    WHERE t1.id = t2.id;

$_$
    LANGUAGE sql IMMUTABLE;



CREATE FUNCTION __filter_input_relation(rel_source character varying, expr_point character varying) RETURNS character varying
    AS $$
DECLARE
    oldClientMinMessages VARCHAR;
    rel_source_filtered VARCHAR;
BEGIN
    IF (SELECT position('.' in rel_source)) > 0 THEN
    	rel_source_filtered := '_madlib_' || split_part(rel_source, '.', 2) || '_filtered';
    ELSE
	rel_source_filtered := '_madlib_' || rel_source || '_filtered';
    END IF;

    oldClientMinMessages :=
        (SELECT setting FROM pg_settings WHERE name = 'client_min_messages');
    EXECUTE 'SET client_min_messages TO warning';
    EXECUTE 'DROP VIEW IF EXISTS _madlib_'||rel_source_filtered||'_filtered';
    EXECUTE 'DROP VIEW IF EXISTS '||rel_source_filtered;
    EXECUTE 'CREATE TEMP VIEW '||rel_source_filtered||'
             AS SELECT * FROM '||rel_source||'
                    WHERE abs(
                              coalesce(
                                 madlib.svec_elsum('||expr_point||'),
                                 ''Infinity''::FLOAT8
                              )
                             ) < ''Infinity''::FLOAT8';
    EXECUTE 'SET client_min_messages TO ' || oldClientMinMessages;
    RETURN rel_source_filtered;
    EXCEPTION
        WHEN undefined_function THEN
	    RAISE EXCEPTION 'Point coordinates (%) are not a valid type
                        (SVEC, FLOAT[], or INTEGER[]).', expr_point;
END
$$
    LANGUAGE plpgsql STRICT;



CREATE FUNCTION __lda_count_topic_prefunc(state1 integer[], state2 integer[]) RETURNS integer[]
    AS '$libdir/libmadlib.so', 'lda_count_topic_prefunc'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION __lda_count_topic_sfunc(state integer[], words integer[], counts integer[], topic_assignment integer[], voc_size integer, topic_num integer) RETURNS integer[]
    AS '$libdir/libmadlib.so', 'lda_count_topic_sfunc'
    LANGUAGE c;



CREATE FUNCTION __lda_gibbs_sample(words integer[], counts integer[], doc_topic integer[], model integer[], alpha double precision, beta double precision, voc_size integer, topic_num integer, iter_num integer) RETURNS integer[]
    AS '$libdir/libmadlib.so', 'lda_gibbs_sample'
    LANGUAGE c;



CREATE FUNCTION __lda_random_assign(word_count integer, topic_num integer) RETURNS integer[]
    AS '$libdir/libmadlib.so', 'lda_random_assign'
    LANGUAGE c STRICT;



CREATE FUNCTION __lda_util_conorm_data(data_table text, vocab_table text, output_data_table text, output_vocab_table text) RETURNS SETOF lda_result
    AS $$
    
    import sys
    from inspect import getframeinfo, currentframe
    try:
        from lda import lda
    except:
        sys.path.append("/usr/local/madlib/ports/greenplum/4.2/modules")
        from lda import lda
    
    # Retrieve the schema name of the current function
    # Make it available as variable: schema_madlib
    fname = getframeinfo(currentframe()).function
    foid  = fname.rsplit('_',1)[1]

    # plpython names its functions "__plpython_procedure_<function name>_<oid>",
    # of which we want the oid
    rv = plpy.execute('SELECT nspname, proname FROM pg_proc p ' \
         'JOIN pg_namespace n ON (p.pronamespace = n.oid) ' \
         'WHERE p.oid = %s' % foid, 1)

    global schema_madlib
    schema_madlib = rv[0]['nspname']    

    lda.conorm_data(
        data_table, vocab_table, output_data_table, output_vocab_table)
    return [[output_data_table,'normalized data table'],
        [output_vocab_table,'normalized vocab table']]
$$
    LANGUAGE plpythonu STRICT;



CREATE FUNCTION __lda_util_index_sort(arr double precision[]) RETURNS integer[]
    AS $$
    
    import sys
    from inspect import getframeinfo, currentframe
    try:
        from lda import lda
    except:
        sys.path.append("/usr/local/madlib/ports/greenplum/4.2/modules")
        from lda import lda
    
    # Retrieve the schema name of the current function
    # Make it available as variable: schema_madlib
    fname = getframeinfo(currentframe()).function
    foid  = fname.rsplit('_',1)[1]

    # plpython names its functions "__plpython_procedure_<function name>_<oid>",
    # of which we want the oid
    rv = plpy.execute('SELECT nspname, proname FROM pg_proc p ' \
         'JOIN pg_namespace n ON (p.pronamespace = n.oid) ' \
         'WHERE p.oid = %s' % foid, 1)

    global schema_madlib
    schema_madlib = rv[0]['nspname']    

    return lda.index_sort(arr)
$$
    LANGUAGE plpythonu STRICT;



CREATE FUNCTION __lda_util_norm_dataset(data_table text, norm_vocab_table text, output_data_table text) RETURNS SETOF lda_result
    AS $$
    
    import sys
    from inspect import getframeinfo, currentframe
    try:
        from lda import lda
    except:
        sys.path.append("/usr/local/madlib/ports/greenplum/4.2/modules")
        from lda import lda
    
    # Retrieve the schema name of the current function
    # Make it available as variable: schema_madlib
    fname = getframeinfo(currentframe()).function
    foid  = fname.rsplit('_',1)[1]

    # plpython names its functions "__plpython_procedure_<function name>_<oid>",
    # of which we want the oid
    rv = plpy.execute('SELECT nspname, proname FROM pg_proc p ' \
         'JOIN pg_namespace n ON (p.pronamespace = n.oid) ' \
         'WHERE p.oid = %s' % foid, 1)

    global schema_madlib
    schema_madlib = rv[0]['nspname']    

    lda.norm_dataset(data_table, norm_vocab_table, output_data_table)
    return [[output_data_table,'normalized data table']]
$$
    LANGUAGE plpythonu STRICT;



CREATE FUNCTION __lda_util_norm_vocab(vocab_table text, output_vocab_table text) RETURNS SETOF lda_result
    AS $$
    
    import sys
    from inspect import getframeinfo, currentframe
    try:
        from lda import lda
    except:
        sys.path.append("/usr/local/madlib/ports/greenplum/4.2/modules")
        from lda import lda
    
    # Retrieve the schema name of the current function
    # Make it available as variable: schema_madlib
    fname = getframeinfo(currentframe()).function
    foid  = fname.rsplit('_',1)[1]

    # plpython names its functions "__plpython_procedure_<function name>_<oid>",
    # of which we want the oid
    rv = plpy.execute('SELECT nspname, proname FROM pg_proc p ' \
         'JOIN pg_namespace n ON (p.pronamespace = n.oid) ' \
         'WHERE p.oid = %s' % foid, 1)

    global schema_madlib
    schema_madlib = rv[0]['nspname']    

    lda.norm_vocab(vocab_table, output_vocab_table)
    return [[output_vocab_table,'normalized vocbulary table']]
$$
    LANGUAGE plpythonu STRICT;



CREATE FUNCTION __lda_util_norm_with_smoothing(arr double precision[], smooth double precision) RETURNS double precision[]
    AS $$
    
    import sys
    from inspect import getframeinfo, currentframe
    try:
        from lda import lda
    except:
        sys.path.append("/usr/local/madlib/ports/greenplum/4.2/modules")
        from lda import lda
    
    # Retrieve the schema name of the current function
    # Make it available as variable: schema_madlib
    fname = getframeinfo(currentframe()).function
    foid  = fname.rsplit('_',1)[1]

    # plpython names its functions "__plpython_procedure_<function name>_<oid>",
    # of which we want the oid
    rv = plpy.execute('SELECT nspname, proname FROM pg_proc p ' \
         'JOIN pg_namespace n ON (p.pronamespace = n.oid) ' \
         'WHERE p.oid = %s' % foid, 1)

    global schema_madlib
    schema_madlib = rv[0]['nspname']    

    return lda.l1_norm_with_smoothing(arr, smooth)
$$
    LANGUAGE plpythonu STRICT;



CREATE FUNCTION __lda_util_transpose(matrix integer[]) RETURNS integer[]
    AS '$libdir/libmadlib.so', 'lda_transpose'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION __lda_util_unnest(arr integer[]) RETURNS SETOF integer[]
    AS '$libdir/libmadlib.so', 'lda_unnest'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION angle(svec, svec) RETURNS double precision
    AS '$libdir/libmadlib.so', 'svec_svec_angle'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION array_add(x anyarray, y anyarray) RETURNS anyarray
    AS '$libdir/libmadlib.so', 'array_add'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION array_contains(x anyarray, y anyarray) RETURNS boolean
    AS '$libdir/libmadlib.so', 'array_contains'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION array_div(x anyarray, y anyarray) RETURNS anyarray
    AS '$libdir/libmadlib.so', 'array_div'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION array_dot(x anyarray, y anyarray) RETURNS double precision
    AS '$libdir/libmadlib.so', 'array_dot'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION array_fill(x anyarray, k anyelement) RETURNS anyarray
    AS '$libdir/libmadlib.so', 'array_fill'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION array_max(x anyarray) RETURNS anyelement
    AS '$libdir/libmadlib.so', 'array_max'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION array_mean(x anyarray) RETURNS double precision
    AS '$libdir/libmadlib.so', 'array_mean'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION array_min(x anyarray) RETURNS anyelement
    AS '$libdir/libmadlib.so', 'array_min'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION array_mult(x anyarray, y anyarray) RETURNS anyarray
    AS '$libdir/libmadlib.so', 'array_mult'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION array_of_bigint(k integer) RETURNS bigint[]
    AS '$libdir/libmadlib.so', 'array_of_bigint'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION array_of_float(k integer) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'array_of_float'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION array_scalar_mult(x anyarray, k anyelement) RETURNS anyarray
    AS '$libdir/libmadlib.so', 'array_scalar_mult'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION array_sqrt(x anyarray) RETURNS anyarray
    AS '$libdir/libmadlib.so', 'array_sqrt'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION array_stddev(x anyarray) RETURNS double precision
    AS '$libdir/libmadlib.so', 'array_stddev'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION array_sub(x anyarray, y anyarray) RETURNS anyarray
    AS '$libdir/libmadlib.so', 'array_sub'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION array_sum(x anyarray) RETURNS anyelement
    AS '$libdir/libmadlib.so', 'array_sum'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION array_sum_big(x anyarray) RETURNS double precision
    AS '$libdir/libmadlib.so', 'array_sum_big'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION assert(condition boolean, msg character varying) RETURNS void
    AS $$
BEGIN
    IF NOT condition THEN
        RAISE EXCEPTION 'Failed assertion: %', msg;
    END IF;
END
$$
    LANGUAGE plpgsql IMMUTABLE;



CREATE FUNCTION assoc_rules(support double precision, confidence double precision, tid_col text, item_col text, input_table text, output_schema text, "verbose" boolean) RETURNS assoc_rules_results
    AS $$

    
    import sys
    from inspect import getframeinfo, currentframe
    try:
        from assoc_rules import assoc_rules
    except:
        sys.path.append("/usr/local/madlib/ports/greenplum/4.2/modules")
        from assoc_rules import assoc_rules
    
    # Retrieve the schema name of the current function
    # Make it available as variable: schema_madlib
    fname = getframeinfo(currentframe()).function
    foid  = fname.rsplit('_',1)[1]

    # plpython names its functions "__plpython_procedure_<function name>_<oid>",
    # of which we want the oid
    rv = plpy.execute('SELECT nspname, proname FROM pg_proc p ' \
         'JOIN pg_namespace n ON (p.pronamespace = n.oid) ' \
         'WHERE p.oid = %s' % foid, 1)

    global schema_madlib
    schema_madlib = rv[0]['nspname']    


    plpy.execute("SET client_min_messages = error;")

    # schema_madlib comes from PythonFunctionBodyOnly
    return assoc_rules.assoc_rules(
        schema_madlib,
        support,
        confidence,
        tid_col,
        item_col,
        input_table,
        output_schema,
        verbose
        );

$$
    LANGUAGE plpythonu;



CREATE FUNCTION assoc_rules(support double precision, confidence double precision, tid_col text, item_col text, input_table text, output_schema text) RETURNS assoc_rules_results
    AS $$

    
    import sys
    from inspect import getframeinfo, currentframe
    try:
        from assoc_rules import assoc_rules
    except:
        sys.path.append("/usr/local/madlib/ports/greenplum/4.2/modules")
        from assoc_rules import assoc_rules
    
    # Retrieve the schema name of the current function
    # Make it available as variable: schema_madlib
    fname = getframeinfo(currentframe()).function
    foid  = fname.rsplit('_',1)[1]

    # plpython names its functions "__plpython_procedure_<function name>_<oid>",
    # of which we want the oid
    rv = plpy.execute('SELECT nspname, proname FROM pg_proc p ' \
         'JOIN pg_namespace n ON (p.pronamespace = n.oid) ' \
         'WHERE p.oid = %s' % foid, 1)

    global schema_madlib
    schema_madlib = rv[0]['nspname']    


    plpy.execute("SET client_min_messages = error;")

    # schema_madlib comes from PythonFunctionBodyOnly
    return assoc_rules.assoc_rules(
        schema_madlib,
        support,
        confidence,
        tid_col,
        item_col,
        input_table,
        output_schema,
        False
        );

$$
    LANGUAGE plpythonu;



CREATE FUNCTION avg_vector_final(state double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'avg_vector_final'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION avg_vector_merge(state_left double precision[], state_right double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'avg_vector_merge'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION avg_vector_transition(state double precision[], x double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'avg_vector_transition'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION check_if_raises_error(sql text) RETURNS boolean
    AS $$
BEGIN
    EXECUTE sql;
    RETURN FALSE;
EXCEPTION
    WHEN OTHERS THEN
        RETURN TRUE;
END;
$$
    LANGUAGE plpgsql;



CREATE FUNCTION chi2_gof_test_final(state double precision[]) RETURNS chi2_test_result
    AS '$libdir/libmadlib.so', 'chi2_gof_test_final'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION chi2_gof_test_merge_states(state1 double precision[], state2 double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'chi2_gof_test_merge_states'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION chi2_gof_test_transition(state double precision[], observed bigint, expected double precision, df bigint) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'chi2_gof_test_transition'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION chi2_gof_test_transition(state double precision[], observed bigint, expected double precision) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'chi2_gof_test_transition'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION chi2_gof_test_transition(state double precision[], observed bigint) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'chi2_gof_test_transition'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION closest_column(m double precision[], x double precision[], dist regproc) RETURNS closest_column_result
    AS '$libdir/libmadlib.so', 'closest_column'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION closest_column(m double precision[], x double precision[]) RETURNS closest_column_result
    AS $_$
    SELECT madlib.closest_column($1, $2,
        'madlib.squared_dist_norm2')
$_$
    LANGUAGE sql IMMUTABLE STRICT;



CREATE FUNCTION closest_columns(m double precision[], x double precision[], num integer, dist regproc) RETURNS closest_columns_result
    AS '$libdir/libmadlib.so', 'closest_columns'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION closest_columns(m double precision[], x double precision[], num integer) RETURNS closest_columns_result
    AS $_$
    SELECT madlib.closest_columns($1, $2, $3,
        'madlib.squared_dist_norm2')
$_$
    LANGUAGE sql IMMUTABLE STRICT;



CREATE FUNCTION compute_cox_prop_hazards(source character varying, "indepColumn" character varying, "depColumn" character varying, "maxNumIterations" integer, optimizer character varying, "precision" double precision) RETURNS integer
    AS $$
    import sys
    from inspect import getframeinfo, currentframe
    try:
        from stats import cox_prop_hazards
    except:
        sys.path.append("/usr/local/madlib/ports/greenplum/4.2/modules")
        from stats import cox_prop_hazards
    
    # Retrieve the schema name of the current function
    # Make it available as variable: schema_madlib
    fname = getframeinfo(currentframe()).function
    foid  = fname.rsplit('_',1)[1]

    # plpython names its functions "__plpython_procedure_<function name>_<oid>",
    # of which we want the oid
    rv = plpy.execute('SELECT nspname, proname FROM pg_proc p ' \
         'JOIN pg_namespace n ON (p.pronamespace = n.oid) ' \
         'WHERE p.oid = %s' % foid, 1)

    global schema_madlib
    schema_madlib = rv[0]['nspname']

    return cox_prop_hazards.compute_cox_prop_hazards(**globals())
$$
    LANGUAGE plpythonu;



CREATE FUNCTION compute_logregr(source character varying, "depColumn" character varying, "indepColumn" character varying, "maxNumIterations" integer, optimizer character varying, "precision" double precision) RETURNS integer
    AS $$
    import sys
    from inspect import getframeinfo, currentframe
    try:
        from regress import logistic
    except:
        sys.path.append("/usr/local/madlib/ports/greenplum/4.2/modules")
        from regress import logistic
    
    # Retrieve the schema name of the current function
    # Make it available as variable: schema_madlib
    fname = getframeinfo(currentframe()).function
    foid  = fname.rsplit('_',1)[1]

    # plpython names its functions "__plpython_procedure_<function name>_<oid>",
    # of which we want the oid
    rv = plpy.execute('SELECT nspname, proname FROM pg_proc p ' \
         'JOIN pg_namespace n ON (p.pronamespace = n.oid) ' \
         'WHERE p.oid = %s' % foid, 1)

    global schema_madlib
    schema_madlib = rv[0]['nspname']

    return logistic.compute_logregr(**globals())
$$
    LANGUAGE plpythonu;



CREATE FUNCTION compute_mlogregr(source character varying, depvar character varying, numcategories integer, indepvar character varying, maxnumiterations integer, optimizer character varying, "precision" double precision) RETURNS integer
    AS $$
    import sys
    from inspect import getframeinfo, currentframe
    try:
        from regress import multilogistic
    except:
        sys.path.append("/usr/local/madlib/ports/greenplum/4.2/modules")
        from regress import multilogistic
    
    # Retrieve the schema name of the current function
    # Make it available as variable: schema_madlib
    fname = getframeinfo(currentframe()).function
    foid  = fname.rsplit('_',1)[1]

    # plpython names its functions "__plpython_procedure_<function name>_<oid>",
    # of which we want the oid
    rv = plpy.execute('SELECT nspname, proname FROM pg_proc p ' \
         'JOIN pg_namespace n ON (p.pronamespace = n.oid) ' \
         'WHERE p.oid = %s' % foid, 1)

    global schema_madlib
    schema_madlib = rv[0]['nspname']

    return multilogistic.compute_mlogregr(**globals())
$$
    LANGUAGE plpythonu;



CREATE FUNCTION cox_prop_hazards(source character varying, "indepColumn" character varying, "depColumn" character varying, "maxNumIterations" integer, optimizer character varying, "precision" double precision) RETURNS cox_prop_hazards_result
    AS $_$
DECLARE
    theIteration INTEGER;
    fnName VARCHAR;
    theResult madlib.cox_prop_hazards_result;
BEGIN
    theIteration := (
        SELECT madlib.compute_cox_prop_hazards($1, $2, $3, $4, $5, $6)
    );
    IF optimizer = 'newton' THEN
        fnName := 'internal_cox_prop_hazards_result';
    ELSE
        RAISE EXCEPTION 'Unknown optimizer (''%'')', optimizer;
    END IF;
    EXECUTE
        $sql$
        SELECT (result).*
        FROM (
            SELECT
                madlib.$sql$ || fnName || $sql$(_madlib_state) AS result
                FROM _madlib_iterative_alg
                WHERE _madlib_iteration = $sql$ || theIteration || $sql$
            ) subq
        $sql$
        INTO theResult;
				
    IF NOT (theResult IS NULL) THEN
        theResult.num_iterations = theIteration;
    END IF;
    RETURN theResult;
END;
$_$
    LANGUAGE plpgsql;



CREATE FUNCTION cox_prop_hazards(source character varying, "indepColumn" character varying, "depColumn" character varying) RETURNS cox_prop_hazards_result
    AS $_$SELECT madlib.cox_prop_hazards($1, $2, $3, 20, 'newton', 0.0001);$_$
    LANGUAGE sql;



CREATE FUNCTION cox_prop_hazards(source character varying, "indepColumn" character varying, "depColumn" character varying, "maxNumIterations" integer) RETURNS cox_prop_hazards_result
    AS $_$SELECT madlib.cox_prop_hazards($1, $2, $3, $4, 'newton', 0.0001);$_$
    LANGUAGE sql;



CREATE FUNCTION cox_prop_hazards(source character varying, "indepColumn" character varying, "depColumn" character varying, "maxNumIterations" integer, optimizer character varying) RETURNS cox_prop_hazards_result
    AS $_$SELECT madlib.cox_prop_hazards($1, $2, $3, $4, $5, 0.0001);$_$
    LANGUAGE sql;



CREATE FUNCTION cox_prop_hazards_step_final(state double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'cox_prop_hazards_step_final'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION cox_prop_hazards_step_transition(double precision[], double precision[], double precision, double precision, double precision[], double precision[], double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'cox_prop_hazards_step_transition'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION create_schema_pg_temp() RETURNS void
    AS $$
BEGIN
    IF pg_my_temp_schema() = 0 THEN
        EXECUTE 'CREATE TEMPORARY TABLE _madlib_temp_table AS SELECT 1;
            DROP TABLE pg_temp._madlib_temp_table CASCADE;';
    END IF;
END;
$$
    LANGUAGE plpgsql;



CREATE FUNCTION dist_angle(x double precision[], y double precision[]) RETURNS double precision
    AS '$libdir/libmadlib.so', 'dist_angle'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION dist_norm1(x double precision[], y double precision[]) RETURNS double precision
    AS '$libdir/libmadlib.so', 'dist_norm1'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION dist_norm2(x double precision[], y double precision[]) RETURNS double precision
    AS '$libdir/libmadlib.so', 'dist_norm2'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION dist_tanimoto(x double precision[], y double precision[]) RETURNS double precision
    AS '$libdir/libmadlib.so', 'dist_tanimoto'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION f_test_final(state double precision[]) RETURNS f_test_result
    AS '$libdir/libmadlib.so', 'f_test_final'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION float8arr_cast_float4(real) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'float8arr_cast_float4'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION float8arr_cast_float8(double precision) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'float8arr_cast_float8'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION float8arr_cast_int2(smallint) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'float8arr_cast_int2'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION float8arr_cast_int4(integer) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'float8arr_cast_int4'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION float8arr_cast_int8(bigint) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'float8arr_cast_int8'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION float8arr_cast_numeric(numeric) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'float8arr_cast_numeric'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION float8arr_div_float8arr(double precision[], double precision[]) RETURNS svec
    AS '$libdir/libmadlib.so', 'float8arr_div_float8arr'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION float8arr_div_svec(double precision[], svec) RETURNS svec
    AS '$libdir/libmadlib.so', 'float8arr_div_svec'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION float8arr_eq(double precision[], double precision[]) RETURNS boolean
    AS '$libdir/libmadlib.so', 'float8arr_equals'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION float8arr_minus_float8arr(double precision[], double precision[]) RETURNS svec
    AS '$libdir/libmadlib.so', 'float8arr_minus_float8arr'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION float8arr_minus_svec(double precision[], svec) RETURNS svec
    AS '$libdir/libmadlib.so', 'float8arr_minus_svec'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION float8arr_mult_float8arr(double precision[], double precision[]) RETURNS svec
    AS '$libdir/libmadlib.so', 'float8arr_mult_float8arr'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION float8arr_mult_svec(double precision[], svec) RETURNS svec
    AS '$libdir/libmadlib.so', 'float8arr_mult_svec'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION float8arr_plus_float8arr(double precision[], double precision[]) RETURNS svec
    AS '$libdir/libmadlib.so', 'float8arr_plus_float8arr'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION float8arr_plus_svec(double precision[], svec) RETURNS svec
    AS '$libdir/libmadlib.so', 'float8arr_plus_svec'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION gen_rules_from_cfp(text, integer) RETURNS SETOF text[]
    AS '$libdir/libmadlib.so', 'gen_rules_from_cfp'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION intermediate_cox_prop_hazards(double precision[], double precision[]) RETURNS intermediate_cox_prop_hazards_result
    AS '$libdir/libmadlib.so', 'intermediate_cox_prop_hazards'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION internal_compute_kmeans(rel_args character varying, rel_state character varying, rel_source character varying, expr_point character varying, agg_centroid character varying) RETURNS integer
    AS $$
    import sys
    from inspect import getframeinfo, currentframe
    try:
        from kmeans import kmeans
    except:
        sys.path.append("/usr/local/madlib/ports/greenplum/4.2/modules")
        from kmeans import kmeans
    
    # Retrieve the schema name of the current function
    # Make it available as variable: schema_madlib
    fname = getframeinfo(currentframe()).function
    foid  = fname.rsplit('_',1)[1]

    # plpython names its functions "__plpython_procedure_<function name>_<oid>",
    # of which we want the oid
    rv = plpy.execute('SELECT nspname, proname FROM pg_proc p ' \
         'JOIN pg_namespace n ON (p.pronamespace = n.oid) ' \
         'WHERE p.oid = %s' % foid, 1)

    global schema_madlib
    schema_madlib = rv[0]['nspname']

    return kmeans.compute_kmeans(**globals())
$$
    LANGUAGE plpythonu;



CREATE FUNCTION internal_compute_kmeans_random_seeding(rel_args character varying, rel_state character varying, rel_source character varying, expr_point character varying) RETURNS integer
    AS $$
    import sys
    from inspect import getframeinfo, currentframe
    try:
        from kmeans import kmeans
    except:
        sys.path.append("/usr/local/madlib/ports/greenplum/4.2/modules")
        from kmeans import kmeans
    
    # Retrieve the schema name of the current function
    # Make it available as variable: schema_madlib
    fname = getframeinfo(currentframe()).function
    foid  = fname.rsplit('_',1)[1]

    # plpython names its functions "__plpython_procedure_<function name>_<oid>",
    # of which we want the oid
    rv = plpy.execute('SELECT nspname, proname FROM pg_proc p ' \
         'JOIN pg_namespace n ON (p.pronamespace = n.oid) ' \
         'WHERE p.oid = %s' % foid, 1)

    global schema_madlib
    schema_madlib = rv[0]['nspname']

    return kmeans.compute_kmeans_random_seeding(**globals())
$$
    LANGUAGE plpythonu;



CREATE FUNCTION internal_compute_kmeanspp_seeding(rel_args character varying, rel_state character varying, rel_source character varying, expr_point character varying) RETURNS integer
    AS $$
    import sys
    from inspect import getframeinfo, currentframe
    try:
        from kmeans import kmeans
    except:
        sys.path.append("/usr/local/madlib/ports/greenplum/4.2/modules")
        from kmeans import kmeans
    
    # Retrieve the schema name of the current function
    # Make it available as variable: schema_madlib
    fname = getframeinfo(currentframe()).function
    foid  = fname.rsplit('_',1)[1]

    # plpython names its functions "__plpython_procedure_<function name>_<oid>",
    # of which we want the oid
    rv = plpy.execute('SELECT nspname, proname FROM pg_proc p ' \
         'JOIN pg_namespace n ON (p.pronamespace = n.oid) ' \
         'WHERE p.oid = %s' % foid, 1)

    global schema_madlib
    schema_madlib = rv[0]['nspname']

    return kmeans.compute_kmeanspp_seeding(**globals())
$$
    LANGUAGE plpythonu;



CREATE FUNCTION internal_cox_prop_hazards_result(double precision[]) RETURNS cox_prop_hazards_result
    AS '$libdir/libmadlib.so', 'internal_cox_prop_hazards_result'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION internal_cox_prop_hazards_step_distance(double precision[], double precision[]) RETURNS double precision
    AS '$libdir/libmadlib.so', 'internal_cox_prop_hazards_step_distance'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION internal_execute_using_kmeans_args(sql character varying, double precision[], regproc, integer, double precision) RETURNS void
    AS '$libdir/libmadlib.so', 'exec_sql_using'
    LANGUAGE c;



CREATE FUNCTION internal_execute_using_kmeans_args(sql character varying, rel_source character varying, expr_point character varying, fn_dist character varying, agg_centroid character varying, max_num_iterations integer, min_frac_reassigned double precision) RETURNS kmeans_result
    AS '$libdir/libmadlib.so', 'exec_sql_using'
    LANGUAGE c;



CREATE FUNCTION internal_execute_using_kmeans_random_seeding_args(sql character varying, integer, double precision[]) RETURNS void
    AS '$libdir/libmadlib.so', 'exec_sql_using'
    LANGUAGE c;



CREATE FUNCTION internal_execute_using_kmeanspp_seeding_args(sql character varying, integer, regproc, double precision[]) RETURNS void
    AS '$libdir/libmadlib.so', 'exec_sql_using'
    LANGUAGE c;



CREATE FUNCTION internal_execute_using_silhouette_args(sql character varying, centroids double precision[], fn_dist regproc) RETURNS double precision
    AS '$libdir/libmadlib.so', 'exec_sql_using'
    LANGUAGE c STABLE;



CREATE FUNCTION internal_logregr_cg_result(double precision[]) RETURNS logregr_result
    AS '$libdir/libmadlib.so', 'internal_logregr_cg_result'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION internal_logregr_cg_step_distance(double precision[], double precision[]) RETURNS double precision
    AS '$libdir/libmadlib.so', 'internal_logregr_cg_step_distance'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION internal_logregr_igd_result(double precision[]) RETURNS logregr_result
    AS '$libdir/libmadlib.so', 'internal_logregr_igd_result'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION internal_logregr_igd_step_distance(double precision[], double precision[]) RETURNS double precision
    AS '$libdir/libmadlib.so', 'internal_logregr_igd_step_distance'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION internal_logregr_irls_result(double precision[]) RETURNS logregr_result
    AS '$libdir/libmadlib.so', 'internal_logregr_irls_result'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION internal_logregr_irls_step_distance(double precision[], double precision[]) RETURNS double precision
    AS '$libdir/libmadlib.so', 'internal_logregr_irls_step_distance'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION internal_mlogregr_irls_result(double precision[]) RETURNS mlogregr_result
    AS '$libdir/libmadlib.so', 'internal_mlogregr_irls_result'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION internal_mlogregr_irls_step_distance(double precision[], double precision[]) RETURNS double precision
    AS '$libdir/libmadlib.so', 'internal_mlogregr_irls_step_distance'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION isnan(number double precision) RETURNS boolean
    AS $_$
    SELECT $1 = 'NaN'::DOUBLE PRECISION;
$_$
    LANGUAGE sql;



CREATE FUNCTION kmeans(rel_source character varying, expr_point character varying, initial_centroids double precision[], fn_dist character varying, agg_centroid character varying, max_num_iterations integer, min_frac_reassigned double precision) RETURNS kmeans_result
    AS $_$
DECLARE
    theIteration INTEGER;
    theResult madlib.kmeans_result;
    oldClientMinMessages VARCHAR;
    class_rel_source REGCLASS;
    proc_fn_dist REGPROCEDURE;
    proc_agg_centroid REGPROCEDURE;
    rel_filtered VARCHAR;
    num_points INTEGER;
    k INTEGER;
    centroids FLOAT8[];
BEGIN
    IF (array_upper(initial_centroids,1) IS NULL) THEN
	RAISE EXCEPTION 'No valid initial centroids given.';
    END IF;

    centroids := ARRAY(SELECT unnest(initial_centroids));
    IF (SELECT madlib.svec_elsum(centroids)) >= 'Infinity'::float THEN
        RAISE EXCEPTION 'At least one initial centroid has non-finite values.';
    END IF;

    rel_filtered = madlib.__filter_input_relation(rel_source, expr_point);
    class_rel_source := rel_filtered;
    proc_fn_dist := fn_dist
        || '(DOUBLE PRECISION[], DOUBLE PRECISION[])';
    IF (SELECT prorettype != 'DOUBLE PRECISION'::regtype OR proisagg = TRUE
        FROM pg_proc WHERE oid = proc_fn_dist) THEN
        RAISE EXCEPTION 'Distance function has wrong signature or is not a simple function.';
    END IF;
    proc_agg_centroid := agg_centroid || '(DOUBLE PRECISION[])';
    IF (SELECT prorettype != 'DOUBLE PRECISION[]'::regtype OR proisagg = FALSE
        FROM pg_proc WHERE oid = proc_agg_centroid) THEN
        RAISE EXCEPTION 'Mean aggregate has wrong signature or is not an aggregate.';
    END IF;
    IF (min_frac_reassigned < 0) OR (min_frac_reassigned > 1) THEN
        RAISE EXCEPTION 'Convergence threshold is not a valid value (must be a fraction between 0 and 1).';
    END IF;
    IF (max_num_iterations < 0) THEN
        RAISE EXCEPTION 'Number of iterations must be a non-negative integer.';
    END IF;

    k := array_upper(initial_centroids,1);
    IF (k <= 0) THEN
        RAISE EXCEPTION 'Number of clusters k must be a positive integer.';
    END IF;
    IF (k > 32767) THEN
	RAISE EXCEPTION 'Number of clusters k must be <= 32767 (for results to be returned in a reasonable amount of time).';
    END IF;
    EXECUTE $sql$ SELECT count(*) FROM $sql$ || textin(regclassout(class_rel_source)) INTO num_points ;
    IF (num_points < k) THEN
	RAISE EXCEPTION 'Number of centroids is greater than number of points.';
    END IF;

    PERFORM madlib.create_schema_pg_temp();
    oldClientMinMessages :=
        (SELECT setting FROM pg_settings WHERE name = 'client_min_messages');
    EXECUTE 'SET client_min_messages TO warning';

    PERFORM madlib.internal_execute_using_kmeans_args($sql$
        DROP TABLE IF EXISTS pg_temp._madlib_kmeans_args;
        CREATE TABLE pg_temp._madlib_kmeans_args AS
        SELECT
            $1 AS initial_centroids, array_upper($1, 1) AS k,
            $2 AS fn_dist, $3 AS max_num_iterations,
            $4 AS min_frac_reassigned;
        $sql$,
        initial_centroids, proc_fn_dist, max_num_iterations,
        min_frac_reassigned);
    EXECUTE 'SET client_min_messages TO ' || oldClientMinMessages;

    theIteration := madlib.internal_compute_kmeans('_madlib_kmeans_args',
            '_madlib_kmeans_state',
            textin(regclassout(class_rel_source)), expr_point,
            textin(regprocout(proc_agg_centroid)));

    EXECUTE
        $sql$
        SELECT (_state).centroids, (_state).objective_fn,
            (_state).frac_reassigned, NULL
        FROM _madlib_kmeans_state
        WHERE _iteration = $sql$ || theIteration || $sql$
        $sql$
        INTO theResult;
    IF NOT (theResult IS NULL) THEN
        theResult.num_iterations = theIteration;
    END IF;
    RETURN theResult;
END;
$_$
    LANGUAGE plpgsql;



CREATE FUNCTION kmeans(rel_source character varying, expr_point character varying, initial_centroids double precision[], fn_dist character varying, agg_centroid character varying, max_num_iterations integer) RETURNS kmeans_result
    AS $_$
    SELECT madlib.kmeans($1, $2, $3, $4, $5, $6, 0.001)
$_$
    LANGUAGE sql STRICT;



CREATE FUNCTION kmeans(rel_source character varying, expr_point character varying, initial_centroids double precision[], fn_dist character varying, agg_centroid character varying) RETURNS kmeans_result
    AS $_$
    SELECT madlib.kmeans($1, $2, $3, $4, $5, 20, 0.001)
$_$
    LANGUAGE sql STRICT;



CREATE FUNCTION kmeans(rel_source character varying, expr_point character varying, initial_centroids double precision[], fn_dist character varying) RETURNS kmeans_result
    AS $_$
    SELECT madlib.kmeans($1, $2, $3, $4, 'madlib.avg', 20,
        0.001)
$_$
    LANGUAGE sql STRICT;



CREATE FUNCTION kmeans(rel_source character varying, expr_point character varying, initial_centroids double precision[]) RETURNS kmeans_result
    AS $_$
    SELECT madlib.kmeans($1, $2, $3,
        'madlib.squared_dist_norm2', 'madlib.avg', 20, 0.001)
$_$
    LANGUAGE sql STRICT;



CREATE FUNCTION kmeans(rel_source character varying, expr_point character varying, rel_initial_centroids character varying, expr_centroid character varying, fn_dist character varying, agg_centroid character varying, max_num_iterations integer, min_frac_reassigned double precision) RETURNS kmeans_result
    AS $_$
DECLARE
    class_rel_initial_centroids REGCLASS;
    theResult madlib.kmeans_result;
BEGIN
    class_rel_initial_centroids := rel_initial_centroids;
    SELECT * FROM madlib.internal_execute_using_kmeans_args($sql$
        SELECT madlib.kmeans(
            $1, $2,
            (
                SELECT madlib.matrix_agg(($sql$ || expr_centroid || $sql$)::FLOAT8[])
                FROM $sql$ || textin(regclassout(class_rel_initial_centroids))
                    || $sql$
            ),
            $3, $4, $5, $6)
            $sql$,
        rel_source, expr_point,
        fn_dist, agg_centroid, max_num_iterations, min_frac_reassigned)
        INTO theResult;
    RETURN theResult;
END;
$_$
    LANGUAGE plpgsql STRICT;



CREATE FUNCTION kmeans(rel_source character varying, expr_point character varying, rel_initial_centroids character varying, expr_centroid character varying, fn_dist character varying, agg_centroid character varying, max_num_iterations integer) RETURNS kmeans_result
    AS $_$
    SELECT madlib.kmeans(
        $1, $2,
        $3, $4, $5, $6, $7, 0.001)
$_$
    LANGUAGE sql STRICT;



CREATE FUNCTION kmeans(rel_source character varying, expr_point character varying, rel_initial_centroids character varying, expr_centroid character varying, fn_dist character varying, agg_centroid character varying) RETURNS kmeans_result
    AS $_$
    SELECT madlib.kmeans(
        $1, $2,
        $3, $4, $5, $6, 20, 0.001)
$_$
    LANGUAGE sql STRICT;



CREATE FUNCTION kmeans(rel_source character varying, expr_point character varying, rel_initial_centroids character varying, expr_centroid character varying, fn_dist character varying) RETURNS kmeans_result
    AS $_$
    SELECT madlib.kmeans(
        $1, $2,
        $3, $4, $5, 'madlib.avg', 20, 0.001)
$_$
    LANGUAGE sql STRICT;



CREATE FUNCTION kmeans(rel_source character varying, expr_point character varying, rel_initial_centroids character varying, expr_centroid character varying) RETURNS kmeans_result
    AS $_$
    SELECT madlib.kmeans(
        $1, $2,
        $3, $4,
        'madlib.squared_dist_norm2', 'madlib.avg', 20, 0.001)
$_$
    LANGUAGE sql STRICT;



CREATE FUNCTION kmeans_random(rel_source character varying, expr_point character varying, k integer, fn_dist character varying, agg_centroid character varying, max_num_iterations integer, min_frac_reassigned double precision) RETURNS kmeans_result
    AS $_$
DECLARE
    ret madlib.kmeans_result;
BEGIN
    ret = madlib.kmeans(
        $1, $2, madlib.kmeans_random_seeding($1, $2, $3),
        $4, $5, $6, $7);
    RETURN ret;
END
$_$
    LANGUAGE plpgsql STRICT;



CREATE FUNCTION kmeans_random(rel_source character varying, expr_point character varying, k integer, fn_dist character varying, agg_centroid character varying, max_num_iterations integer) RETURNS kmeans_result
    AS $_$
DECLARE
    ret madlib.kmeans_result;
BEGIN
    ret = madlib.kmeans(
        $1, $2, madlib.kmeans_random_seeding($1, $2, $3),
        $4, $5, $6, 0.001);
    RETURN ret;
END
$_$
    LANGUAGE plpgsql STRICT;



CREATE FUNCTION kmeans_random(rel_source character varying, expr_point character varying, k integer, fn_dist character varying, agg_centroid character varying) RETURNS kmeans_result
    AS $_$
DECLARE
    ret madlib.kmeans_result;
BEGIN
    ret = madlib.kmeans(
        $1, $2, madlib.kmeans_random_seeding($1, $2, $3),
        $4, $5, 20, 0.001);
    RETURN ret;
END
$_$
    LANGUAGE plpgsql STRICT;



CREATE FUNCTION kmeans_random(rel_source character varying, expr_point character varying, k integer, fn_dist character varying) RETURNS kmeans_result
    AS $_$
DECLARE
    ret madlib.kmeans_result;
BEGIN
    ret = madlib.kmeans(
        $1, $2,
        madlib.kmeans_random_seeding($1, $2, $3),
        $4, 'madlib.avg', 20, 0.001);
    RETURN ret;
END
$_$
    LANGUAGE plpgsql STRICT;



CREATE FUNCTION kmeans_random(rel_source character varying, expr_point character varying, k integer) RETURNS kmeans_result
    AS $_$
DECLARE
    ret madlib.kmeans_result;
BEGIN
    ret = madlib.kmeans(
        $1, $2,
        madlib.kmeans_random_seeding($1, $2, $3),
        'madlib.squared_dist_norm2', 'madlib.avg', 20, 0.001);
    RETURN ret;
END
$_$
    LANGUAGE plpgsql STRICT;



CREATE FUNCTION kmeans_random_seeding(rel_source character varying, expr_point character varying, k integer, initial_centroids double precision[]) RETURNS double precision[]
    AS $_$
DECLARE
    theIteration INTEGER;
    theResult DOUBLE PRECISION[][];
    oldClientMinMessages VARCHAR;
    class_rel_source REGCLASS;
    proc_fn_dist REGPROCEDURE;
    num_points INTEGER;
    num_centroids INTEGER;
    rel_filtered VARCHAR;
BEGIN
    rel_filtered = madlib.__filter_input_relation(rel_source, expr_point);
    class_rel_source := rel_filtered;

    IF (initial_centroids IS NOT NULL) THEN
	num_centroids := array_upper(initial_centroids,1);
    ELSE
	num_centroids := k;
    END IF;

    IF (k <= 0) THEN
        RAISE EXCEPTION 'Number of clusters k must be a positive integer.';
    END IF;
    IF (k > 32767) THEN
	RAISE EXCEPTION 'Number of clusters k must be <= 32767 (for results to be returned in a reasonable amount of time).';
    END IF;
    EXECUTE $sql$ SELECT count(*) FROM $sql$ || textin(regclassout(class_rel_source)) INTO num_points;
    IF (num_points < k  OR num_points < num_centroids) THEN
	RAISE EXCEPTION 'Number of centroids is greater than number of points.';
    END IF;
    IF (k < num_centroids) THEN
	RAISE WARNING 'Number of clusters k is less than number of supplied initial centroids. Number of final clusters will equal number of supplied initial centroids.';
    END IF;

    oldClientMinMessages :=
        (SELECT setting FROM pg_settings WHERE name = 'client_min_messages');
    EXECUTE 'SET client_min_messages TO warning';
    PERFORM madlib.create_schema_pg_temp();
    PERFORM madlib.internal_execute_using_kmeans_random_seeding_args($sql$
        DROP TABLE IF EXISTS pg_temp._madlib_kmeans_random_args;
        CREATE TEMPORARY TABLE _madlib_kmeans_random_args AS
        SELECT $1 AS k, $2 AS initial_centroids;
        $sql$,
        k, initial_centroids);
    EXECUTE 'SET client_min_messages TO ' || oldClientMinMessages;

    theIteration := (
        SELECT madlib.internal_compute_kmeans_random_seeding(
            '_madlib_kmeans_random_args', '_madlib_kmeans_random_state',
            textin(regclassout(class_rel_source)), expr_point)
    );

    EXECUTE
        $sql$
        SELECT _state FROM _madlib_kmeans_random_state
        WHERE _iteration = $sql$ || theIteration || $sql$
        $sql$
        INTO theResult;
    RETURN theResult;
END;
$_$
    LANGUAGE plpgsql;



CREATE FUNCTION kmeans_random_seeding(rel_source character varying, expr_point character varying, k integer) RETURNS double precision[]
    AS $_$
    SELECT madlib.kmeans_random_seeding($1, $2, $3, NULL)
$_$
    LANGUAGE sql;



CREATE FUNCTION kmeanspp(rel_source character varying, expr_point character varying, k integer, fn_dist character varying, agg_centroid character varying, max_num_iterations integer, min_frac_reassigned double precision) RETURNS kmeans_result
    AS $_$
DECLARE
    ret madlib.kmeans_result;
BEGIN
    ret = madlib.kmeans(
        $1, $2, madlib.kmeanspp_seeding($1, $2, $3, $4),
        $4, $5, $6, $7);
    RETURN ret;
END
$_$
    LANGUAGE plpgsql STRICT;



CREATE FUNCTION kmeanspp(rel_source character varying, expr_point character varying, k integer, fn_dist character varying, agg_centroid character varying, max_num_iterations integer) RETURNS kmeans_result
    AS $_$
DECLARE
    ret madlib.kmeans_result;
BEGIN
    ret = madlib.kmeans(
        $1, $2, madlib.kmeanspp_seeding($1, $2, $3, $4),
        $4, $5, $6, 0.001);
    RETURN ret;
END
$_$
    LANGUAGE plpgsql STRICT;



CREATE FUNCTION kmeanspp(rel_source character varying, expr_point character varying, k integer, fn_dist character varying, agg_centroid character varying) RETURNS kmeans_result
    AS $_$
DECLARE
    ret madlib.kmeans_result;
BEGIN
    ret = madlib.kmeans(
        $1, $2, madlib.kmeanspp_seeding($1, $2, $3, $4),
        $4, $5, 20, 0.001);
    RETURN ret;
END
$_$
    LANGUAGE plpgsql STRICT;



CREATE FUNCTION kmeanspp(rel_source character varying, expr_point character varying, k integer, fn_dist character varying) RETURNS kmeans_result
    AS $_$
DECLARE
    ret madlib.kmeans_result;
BEGIN
    ret = madlib.kmeans(
        $1, $2, madlib.kmeanspp_seeding($1, $2, $3, $4),
        $4, 'madlib.avg', 20, 0.001);
    RETURN ret;
END
$_$
    LANGUAGE plpgsql STRICT;



CREATE FUNCTION kmeanspp(rel_source character varying, expr_point character varying, k integer) RETURNS kmeans_result
    AS $_$
DECLARE
    ret madlib.kmeans_result;
BEGIN
    ret = madlib.kmeans(
        $1, $2,
        madlib.kmeanspp_seeding($1, $2, $3,
            'madlib.squared_dist_norm2'),
        'madlib.squared_dist_norm2', 'madlib.avg', 20, 0.001);
    RETURN ret;
END
$_$
    LANGUAGE plpgsql STRICT;



CREATE FUNCTION kmeanspp_seeding(rel_source character varying, expr_point character varying, k integer, fn_dist character varying, initial_centroids double precision[]) RETURNS double precision[]
    AS $_$
DECLARE
    theIteration INTEGER;
    theResult DOUBLE PRECISION[][];
    oldClientMinMessages VARCHAR;
    class_rel_source REGCLASS;
    proc_fn_dist REGPROCEDURE;
    num_points INTEGER;
    num_centroids INTEGER;
    rel_filtered VARCHAR;
BEGIN
    rel_filtered = madlib.__filter_input_relation(rel_source, expr_point);
    class_rel_source := rel_filtered;

    IF (initial_centroids IS NOT NULL) THEN
	num_centroids := array_upper(initial_centroids,1);
    ELSE
	num_centroids := k;
    END IF;

    proc_fn_dist := fn_dist
        || '(DOUBLE PRECISION[], DOUBLE PRECISION[])';
    IF (SELECT prorettype != 'DOUBLE PRECISION'::regtype OR proisagg = TRUE
        FROM pg_proc WHERE oid = proc_fn_dist) THEN
        RAISE EXCEPTION 'Distance function has wrong signature or is not a simple function.';
    END IF;
    IF (k <= 0) THEN
        RAISE EXCEPTION 'Number of clusters k must be a positive integer.';
    END IF;
    IF (k > 32767) THEN
	RAISE EXCEPTION 'Number of clusters k must be <= 32767 (for results to be returned in a reasonable amount of time).';
    END IF;
    EXECUTE $sql$ SELECT count(*) FROM $sql$ || textin(regclassout(class_rel_source)) INTO num_points ;
    IF (num_points < k OR num_points < num_centroids) THEN
	RAISE EXCEPTION 'Number of centroids is greater than number of points.';
    END IF;
    IF (k < num_centroids) THEN
	RAISE WARNING 'Number of clusters k is less than number of supplied initial centroids. Number of final clusters will equal number of supplied initial centroids.';
    END IF;

    oldClientMinMessages :=
        (SELECT setting FROM pg_settings WHERE name = 'client_min_messages');
    EXECUTE 'SET client_min_messages TO warning';
    PERFORM madlib.create_schema_pg_temp();
    PERFORM madlib.internal_execute_using_kmeanspp_seeding_args($sql$
        DROP TABLE IF EXISTS pg_temp._madlib_kmeanspp_args;
        CREATE TEMPORARY TABLE _madlib_kmeanspp_args AS
        SELECT $1 AS k, $2 AS fn_dist, $3 AS initial_centroids;
        $sql$,
        k, proc_fn_dist, initial_centroids);
    EXECUTE 'SET client_min_messages TO ' || oldClientMinMessages;

    theIteration := (
        SELECT madlib.internal_compute_kmeanspp_seeding(
            '_madlib_kmeanspp_args', '_madlib_kmeanspp_state',
            textin(regclassout(class_rel_source)), expr_point)
    );

    EXECUTE
        $sql$
        SELECT _state FROM _madlib_kmeanspp_state
        WHERE _iteration = $sql$ || theIteration || $sql$
        $sql$
        INTO theResult;
    RETURN theResult;
END;
$_$
    LANGUAGE plpgsql;



CREATE FUNCTION kmeanspp_seeding(rel_source character varying, expr_point character varying, k integer, fn_dist character varying) RETURNS double precision[]
    AS $_$
    SELECT madlib.kmeanspp_seeding($1, $2, $3, $4, NULL)
$_$
    LANGUAGE sql;



CREATE FUNCTION kmeanspp_seeding(rel_source character varying, expr_point character varying, k integer) RETURNS double precision[]
    AS $_$
    SELECT madlib.kmeanspp_seeding($1, $2, $3,
        'madlib.squared_dist_norm2', NULL)
$_$
    LANGUAGE sql;



CREATE FUNCTION ks_test_final(state double precision[]) RETURNS ks_test_result
    AS '$libdir/libmadlib.so', 'ks_test_final'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION ks_test_transition(state double precision[], first boolean, value double precision, "numFirst" bigint, "numSecond" bigint) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'ks_test_transition'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION l1norm(svec, svec) RETURNS double precision
    AS '$libdir/libmadlib.so', 'svec_svec_l1norm'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION l2norm(svec, svec) RETURNS double precision
    AS '$libdir/libmadlib.so', 'svec_svec_l2norm'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION linregr_final(state bytea8) RETURNS linregr_result
    AS '$libdir/libmadlib.so', 'linregr_final'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION linregr_merge_states(state1 bytea8, state2 bytea8) RETURNS bytea8
    AS '$libdir/libmadlib.so', 'linregr_merge_states'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION linregr_transition(state bytea8, y double precision, x double precision[]) RETURNS bytea8
    AS '$libdir/libmadlib.so', 'linregr_transition'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION logistic(x double precision) RETURNS double precision
    AS $_$
   SELECT CASE WHEN -$1 < -37 THEN 1
               WHEN -$1 > 709 THEN 0
               ELSE 1 / (1 + exp(-$1))
          END;
$_$
    LANGUAGE sql;



CREATE FUNCTION logregr(source character varying, "depColumn" character varying, "indepColumn" character varying, "maxNumIterations" integer, optimizer character varying, "precision" double precision) RETURNS logregr_result
    AS $_$
DECLARE
    theIteration INTEGER;
    fnName VARCHAR;
    theResult madlib.logregr_result;
BEGIN
    theIteration := (
        SELECT madlib.compute_logregr($1, $2, $3, $4, $5, $6)
    );
    IF optimizer = 'irls' OR optimizer = 'newton' THEN
        fnName := 'internal_logregr_irls_result';
    ELSIF optimizer = 'cg' THEN
        fnName := 'internal_logregr_cg_result';
    ELSEIF optimizer = 'igd' THEN
        fnName := 'internal_logregr_igd_result';
    ELSE
        RAISE EXCEPTION 'Unknown optimizer (''%'')', optimizer;
    END IF;
    EXECUTE
        $sql$
        SELECT (result).*
        FROM (
            SELECT
                madlib.$sql$ || fnName || $sql$(_madlib_state) AS result
                FROM _madlib_iterative_alg
                WHERE _madlib_iteration = $sql$ || theIteration || $sql$
            ) subq
        $sql$
        INTO theResult;
    IF NOT (theResult IS NULL) THEN
        theResult.num_iterations = theIteration;
    END IF;
    RETURN theResult;
END;
$_$
    LANGUAGE plpgsql;



CREATE FUNCTION logregr(source character varying, "depColumn" character varying, "indepColumn" character varying) RETURNS logregr_result
    AS $_$SELECT madlib.logregr($1, $2, $3, 20, 'irls', 0.0001);$_$
    LANGUAGE sql;



CREATE FUNCTION logregr(source character varying, "depColumn" character varying, "indepColumn" character varying, "maxNumIterations" integer) RETURNS logregr_result
    AS $_$SELECT madlib.logregr($1, $2, $3, $4, 'irls', 0.0001);$_$
    LANGUAGE sql;



CREATE FUNCTION logregr(source character varying, "depColumn" character varying, "indepColumn" character varying, "maxNumIterations" integer, optimizer character varying) RETURNS logregr_result
    AS $_$SELECT madlib.logregr($1, $2, $3, $4, $5, 0.0001);$_$
    LANGUAGE sql;



CREATE FUNCTION logregr_cg_step_final(state double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'logregr_cg_step_final'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION logregr_cg_step_merge_states(state1 double precision[], state2 double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'logregr_cg_step_merge_states'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION logregr_cg_step_transition(double precision[], boolean, double precision[], double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'logregr_cg_step_transition'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION logregr_igd_step_final(state double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'logregr_igd_step_final'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION logregr_igd_step_merge_states(state1 double precision[], state2 double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'logregr_igd_step_merge_states'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION logregr_igd_step_transition(double precision[], boolean, double precision[], double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'logregr_igd_step_transition'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION logregr_irls_step_final(state double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'logregr_irls_step_final'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION logregr_irls_step_merge_states(state1 double precision[], state2 double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'logregr_irls_step_merge_states'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION logregr_irls_step_transition(double precision[], boolean, double precision[], double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'logregr_irls_step_transition'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION matrix_agg_final(state double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'matrix_agg_final'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION matrix_agg_transition(state double precision[], x double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'matrix_agg_transition'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION matrix_column(matrix double precision[], col integer) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'matrix_column'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION mlogregr(source character varying, depvar character varying, numcategories integer, indepvar character varying, maxnumiterations integer, optimizer character varying, "precision" double precision) RETURNS mlogregr_result
    AS $_$
DECLARE
    observed_count INTEGER;
    theIteration INTEGER;
    fnName VARCHAR;
    theResult madlib.mlogregr_result;
BEGIN
    IF (SELECT atttypid::regtype <> 'INTEGER'::regtype
        FROM pg_attribute
        WHERE attrelid = source::regclass AND attname = depvar) THEN
        RAISE EXCEPTION 'The dependent variable column should be of type INTEGER';
    END IF;

    EXECUTE $sql$ SELECT count(DISTINCT $sql$ || depvar || $sql$ )
                    FROM $sql$ || textin(regclassout(source))
            INTO observed_count;
    IF observed_count <> numcategories
    THEN
        RAISE WARNING 'Results will be undefined, if ''numcategories'' is not
         same as the number of distinct categories observed in the training data.';
    END IF;

    IF optimizer = 'irls' OR optimizer = 'newton' THEN
        fnName := 'internal_mlogregr_irls_result';
    ELSE
        RAISE EXCEPTION 'Unknown optimizer (''%'')', optimizer;
    END IF;

    theIteration := (
        SELECT madlib.compute_mlogregr($1, $2, $3, $4, $5, $6, $7)
    );
    EXECUTE
        $sql$
        SELECT (result).*
        FROM (
            SELECT
                madlib.$sql$ || fnName || $sql$(_madlib_state) AS result
                FROM _madlib_iterative_alg
                WHERE _madlib_iteration = $sql$ || theIteration || $sql$
            ) subq
        $sql$
        INTO theResult;
    IF NOT (theResult IS NULL) THEN
        theResult.num_iterations = theIteration;
    END IF;
    RETURN theResult;
END;
$_$
    LANGUAGE plpgsql;



CREATE FUNCTION mlogregr(source character varying, depvar character varying, numcategories integer, indepvar character varying) RETURNS mlogregr_result
    AS $_$SELECT madlib.mlogregr($1, $2, $3, $4, 20, 'irls', 0.0001);$_$
    LANGUAGE sql;



CREATE FUNCTION mlogregr(source character varying, depvar character varying, numcategories integer, indepvar character varying, maxnumiterations integer) RETURNS mlogregr_result
    AS $_$SELECT madlib.mlogregr($1, $2, $3, $4, $5, 'irls', 0.0001);$_$
    LANGUAGE sql;



CREATE FUNCTION mlogregr(source character varying, depvar character varying, numcategories integer, indepvar character varying, maxbumiterations integer, optimizer character varying) RETURNS mlogregr_result
    AS $_$SELECT madlib.mlogregr($1, $2, $3, $4, $5, $6, 0.0001);$_$
    LANGUAGE sql;



CREATE FUNCTION mlogregr_irls_step_final(state double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'mlogregr_irls_step_final'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION mlogregr_irls_step_merge_states(state1 double precision[], state2 double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'mlogregr_irls_step_merge_states'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION mlogregr_irls_step_transition(double precision[], integer, integer, double precision[], double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'mlogregr_irls_step_transition'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION mw_test_final(state double precision[]) RETURNS mw_test_result
    AS '$libdir/libmadlib.so', 'mw_test_final'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION mw_test_transition(state double precision[], first boolean, value double precision) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'mw_test_transition'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION lda_get_topic_desc(model_table text, vocab_table text, desc_table text, top_k integer) RETURNS SETOF lda_result
    AS $$
    
    import sys
    from inspect import getframeinfo, currentframe
    try:
        from lda import lda
    except:
        sys.path.append("/usr/local/madlib/ports/greenplum/4.2/modules")
        from lda import lda
    
    # Retrieve the schema name of the current function
    # Make it available as variable: schema_madlib
    fname = getframeinfo(currentframe()).function
    foid  = fname.rsplit('_',1)[1]

    # plpython names its functions "__plpython_procedure_<function name>_<oid>",
    # of which we want the oid
    rv = plpy.execute('SELECT nspname, proname FROM pg_proc p ' \
         'JOIN pg_namespace n ON (p.pronamespace = n.oid) ' \
         'WHERE p.oid = %s' % foid, 1)

    global schema_madlib
    schema_madlib = rv[0]['nspname']    

    lda.get_topic_desc(
        schema_madlib, model_table, vocab_table, desc_table, top_k)
    return [[
        desc_table, 
        """topic description, use "ORDER BY topicid, prob DESC" to check the
        results"""]]
$$
    LANGUAGE plpythonu STRICT;



CREATE FUNCTION lda_get_topic_word_count(model_table text, output_table text) RETURNS SETOF lda_result
    AS $$
    
    import sys
    from inspect import getframeinfo, currentframe
    try:
        from lda import lda
    except:
        sys.path.append("/usr/local/madlib/ports/greenplum/4.2/modules")
        from lda import lda
    
    # Retrieve the schema name of the current function
    # Make it available as variable: schema_madlib
    fname = getframeinfo(currentframe()).function
    foid  = fname.rsplit('_',1)[1]

    # plpython names its functions "__plpython_procedure_<function name>_<oid>",
    # of which we want the oid
    rv = plpy.execute('SELECT nspname, proname FROM pg_proc p ' \
         'JOIN pg_namespace n ON (p.pronamespace = n.oid) ' \
         'WHERE p.oid = %s' % foid, 1)

    global schema_madlib
    schema_madlib = rv[0]['nspname']    

    lda.get_topic_word_count(schema_madlib, model_table, output_table)
    return [[output_table, 'per-topic word counts']]
$$
    LANGUAGE plpythonu STRICT;



CREATE FUNCTION lda_get_word_topic_count(model_table text, output_table text) RETURNS SETOF lda_result
    AS $$
    
    import sys
    from inspect import getframeinfo, currentframe
    try:
        from lda import lda
    except:
        sys.path.append("/usr/local/madlib/ports/greenplum/4.2/modules")
        from lda import lda
    
    # Retrieve the schema name of the current function
    # Make it available as variable: schema_madlib
    fname = getframeinfo(currentframe()).function
    foid  = fname.rsplit('_',1)[1]

    # plpython names its functions "__plpython_procedure_<function name>_<oid>",
    # of which we want the oid
    rv = plpy.execute('SELECT nspname, proname FROM pg_proc p ' \
         'JOIN pg_namespace n ON (p.pronamespace = n.oid) ' \
         'WHERE p.oid = %s' % foid, 1)

    global schema_madlib
    schema_madlib = rv[0]['nspname']    

    lda.get_word_topic_count(schema_madlib, model_table, output_table)
    return [[output_table, 'per-word topic counts']]
$$
    LANGUAGE plpythonu STRICT;



CREATE FUNCTION lda_predict(data_table text, model_table text, output_table text) RETURNS SETOF lda_result
    AS $$
    
    import sys
    from inspect import getframeinfo, currentframe
    try:
        from lda import lda
    except:
        sys.path.append("/usr/local/madlib/ports/greenplum/4.2/modules")
        from lda import lda
    
    # Retrieve the schema name of the current function
    # Make it available as variable: schema_madlib
    fname = getframeinfo(currentframe()).function
    foid  = fname.rsplit('_',1)[1]

    # plpython names its functions "__plpython_procedure_<function name>_<oid>",
    # of which we want the oid
    rv = plpy.execute('SELECT nspname, proname FROM pg_proc p ' \
         'JOIN pg_namespace n ON (p.pronamespace = n.oid) ' \
         'WHERE p.oid = %s' % foid, 1)

    global schema_madlib
    schema_madlib = rv[0]['nspname']    

    lda.lda_predict(schema_madlib, data_table, model_table, output_table)
    return [[
        output_table, 
        'per-doc topic distribution and per-word topic assignments']]
$$
    LANGUAGE plpythonu STRICT;



CREATE FUNCTION lda_train(data_table text, model_table text, output_data_table text, voc_size integer, topic_num integer, iter_num integer, alpha double precision, beta double precision) RETURNS SETOF lda_result
    AS $$
    
    import sys
    from inspect import getframeinfo, currentframe
    try:
        from lda import lda
    except:
        sys.path.append("/usr/local/madlib/ports/greenplum/4.2/modules")
        from lda import lda
    
    # Retrieve the schema name of the current function
    # Make it available as variable: schema_madlib
    fname = getframeinfo(currentframe()).function
    foid  = fname.rsplit('_',1)[1]

    # plpython names its functions "__plpython_procedure_<function name>_<oid>",
    # of which we want the oid
    rv = plpy.execute('SELECT nspname, proname FROM pg_proc p ' \
         'JOIN pg_namespace n ON (p.pronamespace = n.oid) ' \
         'WHERE p.oid = %s' % foid, 1)

    global schema_madlib
    schema_madlib = rv[0]['nspname']    

    lda.lda_train(
        schema_madlib, data_table, model_table, output_data_table, voc_size,
        topic_num, iter_num, alpha, beta
    )
    return [[model_table, 'model table'], 
        [output_data_table, 'output data table']]
$$
    LANGUAGE plpythonu STRICT;



CREATE FUNCTION noop() RETURNS void
    AS '$libdir/libmadlib.so', 'noop'
    LANGUAGE c;



CREATE FUNCTION norm1(x double precision[]) RETURNS double precision
    AS '$libdir/libmadlib.so', 'norm1'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION norm2(x double precision[]) RETURNS double precision
    AS '$libdir/libmadlib.so', 'norm2'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION normalize(x double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'array_normalize'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION normalize(svec) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_normalize'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION normalized_avg_vector_final(state double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'normalized_avg_vector_final'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION normalized_avg_vector_transition(state double precision[], x double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'normalized_avg_vector_transition'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION one_way_anova_final(state double precision[]) RETURNS one_way_anova_result
    AS '$libdir/libmadlib.so', 'one_way_anova_final'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION one_way_anova_merge_states(state1 double precision[], state2 double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'one_way_anova_merge_states'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION one_way_anova_transition(state double precision[], "group" integer, value double precision) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'one_way_anova_transition'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION relative_error(approx double precision, value double precision) RETURNS double precision
    AS $_$
    SELECT abs(($1 - $2)/$2)
$_$
    LANGUAGE sql;



CREATE FUNCTION relative_error(approx double precision[], value double precision[]) RETURNS double precision
    AS $_$
    SELECT madlib.dist_norm2($1, $2) / madlib.norm2($2)
$_$
    LANGUAGE sql;



CREATE FUNCTION simple_silhouette(rel_source character varying, expr_point character varying, centroids double precision[], fn_dist character varying) RETURNS double precision
    AS $_$
DECLARE
    class_rel_source REGCLASS;
    proc_fn_dist REGPROCEDURE;
    rel_filtered VARCHAR;
BEGIN
    IF (array_upper(centroids,1) IS NULL) THEN
	RAISE EXCEPTION 'No valid centroids given.';
    END IF;

    rel_filtered = madlib.__filter_input_relation(rel_source, expr_point);
    class_rel_source := rel_filtered;
    proc_fn_dist := fn_dist
        || '(DOUBLE PRECISION[], DOUBLE PRECISION[])';
    IF (SELECT prorettype != 'DOUBLE PRECISION'::regtype OR proisagg = TRUE
        FROM pg_proc WHERE oid = proc_fn_dist) THEN
        RAISE EXCEPTION 'Distance function has wrong signature or is not a simple function.';
    END IF;

    RETURN madlib.internal_execute_using_silhouette_args($sql$
        SELECT
            avg(CASE
                    WHEN distances[2] = 0 THEN 0
                    ELSE (distances[2] - distances[1]) / distances[2]
                END)
        FROM (
            SELECT
                (madlib.closest_columns(
                    $1,
                    ($sql$ || expr_point || $sql$)::FLOAT8[],
                    2::INTEGER,
                    $2
                )).distances
            FROM
                $sql$ || textin(regclassout(class_rel_source)) || $sql$
        ) AS two_shortest_distances
        $sql$,
        centroids, proc_fn_dist);
END;
$_$
    LANGUAGE plpgsql STABLE STRICT;



CREATE FUNCTION simple_silhouette(rel_source character varying, expr_point character varying, centroids double precision[]) RETURNS double precision
    AS $_$
    SELECT madlib.simple_silhouette($1, $2, $3,
        'madlib.dist_norm2')
$_$
    LANGUAGE sql STABLE STRICT;



CREATE FUNCTION squared_dist_norm2(x double precision[], y double precision[]) RETURNS double precision
    AS '$libdir/libmadlib.so', 'squared_dist_norm2'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_append(svec, double precision, bigint) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_append'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_cast_float4(real) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_cast_float4'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_cast_float8(double precision) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_cast_float8'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_cast_float8arr(double precision[]) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_cast_float8arr'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_cast_int2(smallint) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_cast_int2'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_cast_int4(integer) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_cast_int4'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_cast_int8(bigint) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_cast_int8'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_cast_numeric(numeric) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_cast_numeric'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_cast_positions_float8arr(bigint[], double precision[], bigint, double precision) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_cast_positions_float8arr'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_change(svec, integer, svec) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_change'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_concat(svec, svec) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_concat'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_concat_replicate(integer, svec) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_concat_replicate'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_contains(svec, svec) RETURNS boolean
    AS '$libdir/libmadlib.so', 'svec_contains'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_count(svec, svec) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_count'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_dimension(svec) RETURNS integer
    AS '$libdir/libmadlib.so', 'svec_dimension'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_div(svec, svec) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_div'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_div_float8arr(svec, double precision[]) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_div_float8arr'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_dmax(double precision, double precision) RETURNS double precision
    AS '$libdir/libmadlib.so', 'float8_max'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_dmin(double precision, double precision) RETURNS double precision
    AS '$libdir/libmadlib.so', 'float8_min'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_dot(svec, svec) RETURNS double precision
    AS '$libdir/libmadlib.so', 'svec_dot'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_dot(double precision[], double precision[]) RETURNS double precision
    AS '$libdir/libmadlib.so', 'float8arr_dot'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_dot(svec, double precision[]) RETURNS double precision
    AS '$libdir/libmadlib.so', 'svec_dot_float8arr'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_dot(double precision[], svec) RETURNS double precision
    AS '$libdir/libmadlib.so', 'float8arr_dot_svec'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_elsum(svec) RETURNS double precision
    AS '$libdir/libmadlib.so', 'svec_summate'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_elsum(double precision[]) RETURNS double precision
    AS '$libdir/libmadlib.so', 'float8arr_summate'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_eq(svec, svec) RETURNS boolean
    AS '$libdir/libmadlib.so', 'svec_eq'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_eq_non_zero(svec, svec) RETURNS boolean
    AS '$libdir/libmadlib.so', 'svec_eq_non_zero'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_from_string(text) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_from_string'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_hash(svec) RETURNS integer
    AS '$libdir/libmadlib.so', 'svec_hash'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_l1norm(svec) RETURNS double precision
    AS '$libdir/libmadlib.so', 'svec_l1norm'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_l1norm(double precision[]) RETURNS double precision
    AS '$libdir/libmadlib.so', 'float8arr_l1norm'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_l2_cmp(svec, svec) RETURNS integer
    AS '$libdir/libmadlib.so', 'svec_l2_cmp'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_l2_eq(svec, svec) RETURNS boolean
    AS '$libdir/libmadlib.so', 'svec_l2_eq'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_l2_ge(svec, svec) RETURNS boolean
    AS '$libdir/libmadlib.so', 'svec_l2_ge'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_l2_gt(svec, svec) RETURNS boolean
    AS '$libdir/libmadlib.so', 'svec_l2_gt'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_l2_le(svec, svec) RETURNS boolean
    AS '$libdir/libmadlib.so', 'svec_l2_le'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_l2_lt(svec, svec) RETURNS boolean
    AS '$libdir/libmadlib.so', 'svec_l2_lt'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_l2_ne(svec, svec) RETURNS boolean
    AS '$libdir/libmadlib.so', 'svec_l2_ne'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_l2norm(svec) RETURNS double precision
    AS '$libdir/libmadlib.so', 'svec_l2norm'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_l2norm(double precision[]) RETURNS double precision
    AS '$libdir/libmadlib.so', 'float8arr_l2norm'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_lapply(text, svec) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_lapply'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_log(svec) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_log'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_mean_final(double precision[]) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_mean_final'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_mean_prefunc(double precision[], double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'svec_mean_prefunc'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_mean_transition(double precision[], svec) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'svec_mean_transition'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_median(double precision[]) RETURNS double precision
    AS '$libdir/libmadlib.so', 'float8arr_median'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_median(svec) RETURNS double precision
    AS '$libdir/libmadlib.so', 'svec_median'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_minus(svec, svec) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_minus'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_minus_float8arr(svec, double precision[]) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_minus_float8arr'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_mult(svec, svec) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_mult'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_mult_float8arr(svec, double precision[]) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_mult_float8arr'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_nonbase_positions(svec, double precision) RETURNS bigint[]
    AS '$libdir/libmadlib.so', 'svec_nonbase_positions'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_nonbase_values(svec, double precision) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'svec_nonbase_values'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_pivot(svec, double precision) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_pivot'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_plus(svec, svec) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_plus'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_plus_float8arr(svec, double precision[]) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_plus_float8arr'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_pow(svec, svec) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_pow'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_proj(svec, integer) RETURNS double precision
    AS '$libdir/libmadlib.so', 'svec_proj'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_return_array(svec) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'svec_return_array'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_reverse(svec) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_reverse'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_sfv(text[], text[]) RETURNS svec
    AS '$libdir/libmadlib.so', 'gp_extract_feature_histogram'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_sort(text[]) RETURNS text[]
    AS $_$
       SELECT array(SELECT unnest($1::text[]) ORDER BY 1);
$_$
    LANGUAGE sql;



CREATE FUNCTION svec_subvec(svec, integer, integer) RETURNS svec
    AS '$libdir/libmadlib.so', 'svec_subvec'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION svec_to_string(svec) RETURNS text
    AS '$libdir/libmadlib.so', 'svec_to_string'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION svec_unnest(svec) RETURNS SETOF double precision
    AS '$libdir/libmadlib.so', 'svec_unnest'
    LANGUAGE c IMMUTABLE;



CREATE FUNCTION t_test_merge_states(state1 double precision[], state2 double precision[]) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 't_test_merge_states'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION t_test_one_final(state double precision[]) RETURNS t_test_result
    AS '$libdir/libmadlib.so', 't_test_one_final'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION t_test_one_transition(state double precision[], value double precision) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 't_test_one_transition'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION t_test_two_pooled_final(state double precision[]) RETURNS t_test_result
    AS '$libdir/libmadlib.so', 't_test_two_pooled_final'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION t_test_two_transition(state double precision[], first boolean, value double precision) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 't_test_two_transition'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION t_test_two_unpooled_final(state double precision[]) RETURNS t_test_result
    AS '$libdir/libmadlib.so', 't_test_two_unpooled_final'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION tanimoto_distance(svec, svec) RETURNS double precision
    AS '$libdir/libmadlib.so', 'svec_svec_tanimoto_distance'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION version() RETURNS text
    AS $$
    SELECT (
        'MADlib version: 0.5, '
        'git revision: v0.5.0-2-gaec5541, '
        'cmake configuration time: Fri Jan 18 05:15:27 UTC 2013, '
        'build type: RelWithDebInfo, '
        'build system: Darwin-11.4.2, '
        'C compiler: gcc 4.2.1, '
        'C++ compiler: gcc 4.2.1')::TEXT
$$
    LANGUAGE sql IMMUTABLE;



CREATE FUNCTION weighted_sample_final_int64(state bytea8) RETURNS bigint
    AS '$libdir/libmadlib.so', 'weighted_sample_final_int64'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION weighted_sample_final_vector(state bytea8) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'weighted_sample_final_vector'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION weighted_sample_merge_int64(state_left bytea8, state_right bytea8) RETURNS bytea8
    AS '$libdir/libmadlib.so', 'weighted_sample_merge_int64'
    LANGUAGE c STRICT;



CREATE FUNCTION weighted_sample_merge_vector(state_left bytea8, state_right bytea8) RETURNS bytea8
    AS '$libdir/libmadlib.so', 'weighted_sample_merge_vector'
    LANGUAGE c STRICT;



CREATE FUNCTION weighted_sample_transition_int64(state bytea8, value bigint, weight double precision) RETURNS bytea8
    AS '$libdir/libmadlib.so', 'weighted_sample_transition_int64'
    LANGUAGE c STRICT;



CREATE FUNCTION weighted_sample_transition_vector(state bytea8, value double precision[], weight double precision) RETURNS bytea8
    AS '$libdir/libmadlib.so', 'weighted_sample_transition_vector'
    LANGUAGE c STRICT;



CREATE FUNCTION wsr_test_final(state double precision[]) RETURNS wsr_test_result
    AS '$libdir/libmadlib.so', 'wsr_test_final'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION wsr_test_transition(state double precision[], value double precision, "precision" double precision) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'wsr_test_transition'
    LANGUAGE c IMMUTABLE STRICT;



CREATE FUNCTION wsr_test_transition(state double precision[], value double precision) RETURNS double precision[]
    AS '$libdir/libmadlib.so', 'wsr_test_transition'
    LANGUAGE c IMMUTABLE STRICT;



CREATE AGGREGATE __lda_count_topic_agg(integer[], integer[], integer[], integer, integer) (
    SFUNC = __lda_count_topic_sfunc,
    STYPE = integer[],
    PREFUNC = __lda_count_topic_prefunc
);



CREATE AGGREGATE array_agg(anyelement) (
    SFUNC = array_append,
    STYPE = anyarray,
    PREFUNC = array_cat
);



CREATE AGGREGATE avg(double precision[]) (
    SFUNC = avg_vector_transition,
    STYPE = double precision[],
    INITCOND = '{0,0,0}',
    PREFUNC = avg_vector_merge,
    FINALFUNC = avg_vector_final
);



CREATE AGGREGATE chi2_gof_test(bigint, double precision, bigint) (
    SFUNC = madlib.chi2_gof_test_transition,
    STYPE = double precision[],
    INITCOND = '{0,0,0,0,0,0}',
    PREFUNC = chi2_gof_test_merge_states,
    FINALFUNC = chi2_gof_test_final
);



CREATE AGGREGATE chi2_gof_test(bigint, double precision) (
    SFUNC = madlib.chi2_gof_test_transition,
    STYPE = double precision[],
    INITCOND = '{0,0,0,0,0,0,0}',
    PREFUNC = chi2_gof_test_merge_states,
    FINALFUNC = chi2_gof_test_final
);



CREATE AGGREGATE chi2_gof_test(bigint) (
    SFUNC = madlib.chi2_gof_test_transition,
    STYPE = double precision[],
    INITCOND = '{0,0,0,0,0,0,0}',
    PREFUNC = chi2_gof_test_merge_states,
    FINALFUNC = chi2_gof_test_final
);



CREATE AGGREGATE cox_prop_hazards_step(double precision[], double precision, double precision, double precision[], double precision[], double precision[]) (
    SFUNC = cox_prop_hazards_step_transition,
    STYPE = double precision[],
    INITCOND = '{0,0,0,0,0,0,0}',
    FINALFUNC = cox_prop_hazards_step_final
);



CREATE AGGREGATE f_test(boolean, double precision) (
    SFUNC = t_test_two_transition,
    STYPE = double precision[],
    INITCOND = '{0,0,0,0,0,0,0}',
    PREFUNC = t_test_merge_states,
    FINALFUNC = f_test_final
);



CREATE AGGREGATE ks_test(boolean, double precision, bigint, bigint) (
    SFUNC = ks_test_transition,
    STYPE = double precision[],
    INITCOND = '{0,0,0,0,0,0,0}',
    FINALFUNC = ks_test_final
);



CREATE AGGREGATE linregr(double precision, double precision[]) (
    SFUNC = linregr_transition,
    STYPE = bytea8,
    INITCOND = '',
    PREFUNC = linregr_merge_states,
    FINALFUNC = linregr_final
);



CREATE AGGREGATE logregr_cg_step(boolean, double precision[], double precision[]) (
    SFUNC = logregr_cg_step_transition,
    STYPE = double precision[],
    INITCOND = '{0,0,0,0,0,0}',
    PREFUNC = logregr_cg_step_merge_states,
    FINALFUNC = logregr_cg_step_final
);



CREATE AGGREGATE logregr_igd_step(boolean, double precision[], double precision[]) (
    SFUNC = logregr_igd_step_transition,
    STYPE = double precision[],
    INITCOND = '{0,0,0,0}',
    PREFUNC = logregr_igd_step_merge_states
);



CREATE AGGREGATE logregr_irls_step(boolean, double precision[], double precision[]) (
    SFUNC = logregr_irls_step_transition,
    STYPE = double precision[],
    INITCOND = '{0,0,0}',
    PREFUNC = logregr_irls_step_merge_states,
    FINALFUNC = logregr_irls_step_final
);



CREATE AGGREGATE matrix_agg(double precision[]) (
    SFUNC = matrix_agg_transition,
    STYPE = double precision[],
    INITCOND = '{0,0,0}',
    FINALFUNC = matrix_agg_final
);



CREATE AGGREGATE mean(svec) (
    SFUNC = svec_mean_transition,
    STYPE = double precision[],
    PREFUNC = svec_mean_prefunc,
    FINALFUNC = svec_mean_final
);



CREATE AGGREGATE mlogregr_irls_step(integer, integer, double precision[], double precision[]) (
    SFUNC = mlogregr_irls_step_transition,
    STYPE = double precision[],
    INITCOND = '{0,0,0,0}',
    PREFUNC = mlogregr_irls_step_merge_states,
    FINALFUNC = mlogregr_irls_step_final
);



CREATE AGGREGATE mw_test(boolean, double precision) (
    SFUNC = mw_test_transition,
    STYPE = double precision[],
    INITCOND = '{0,0,0,0,0,0,0}',
    FINALFUNC = mw_test_final
);



CREATE AGGREGATE normalized_avg(double precision[]) (
    SFUNC = normalized_avg_vector_transition,
    STYPE = double precision[],
    INITCOND = '{0,0,0}',
    PREFUNC = avg_vector_merge,
    FINALFUNC = normalized_avg_vector_final
);



CREATE AGGREGATE one_way_anova(integer, double precision) (
    SFUNC = one_way_anova_transition,
    STYPE = double precision[],
    INITCOND = '{0,0}',
    PREFUNC = one_way_anova_merge_states,
    FINALFUNC = one_way_anova_final
);



CREATE AGGREGATE svec_agg(double precision) (
    SFUNC = svec_pivot,
    STYPE = svec
);



CREATE AGGREGATE svec_count_nonzero(svec) (
    SFUNC = svec_count,
    STYPE = svec,
    INITCOND = '{1}:{0.}',
    PREFUNC = svec_plus
);



CREATE AGGREGATE svec_median_inmemory(double precision) (
    SFUNC = svec_pivot,
    STYPE = svec,
    PREFUNC = svec_concat,
    FINALFUNC = madlib.svec_median
);



CREATE AGGREGATE svec_sum(svec) (
    SFUNC = svec_plus,
    STYPE = svec,
    INITCOND = '{1}:{0.}',
    PREFUNC = svec_plus
);



CREATE AGGREGATE t_test_one(double precision) (
    SFUNC = t_test_one_transition,
    STYPE = double precision[],
    INITCOND = '{0,0,0,0,0,0,0}',
    PREFUNC = t_test_merge_states,
    FINALFUNC = t_test_one_final
);



CREATE AGGREGATE t_test_two_pooled(boolean, double precision) (
    SFUNC = t_test_two_transition,
    STYPE = double precision[],
    INITCOND = '{0,0,0,0,0,0,0}',
    PREFUNC = t_test_merge_states,
    FINALFUNC = t_test_two_pooled_final
);



CREATE AGGREGATE t_test_two_unpooled(boolean, double precision) (
    SFUNC = t_test_two_transition,
    STYPE = double precision[],
    INITCOND = '{0,0,0,0,0,0,0}',
    PREFUNC = t_test_merge_states,
    FINALFUNC = t_test_two_unpooled_final
);



CREATE AGGREGATE weighted_sample(bigint, double precision) (
    SFUNC = weighted_sample_transition_int64,
    STYPE = bytea8,
    INITCOND = '',
    PREFUNC = weighted_sample_merge_int64,
    FINALFUNC = weighted_sample_final_int64
);



CREATE AGGREGATE weighted_sample(double precision[], double precision) (
    SFUNC = weighted_sample_transition_vector,
    STYPE = bytea8,
    INITCOND = '',
    PREFUNC = weighted_sample_merge_vector,
    FINALFUNC = weighted_sample_final_vector
);



CREATE AGGREGATE wsr_test(double precision, double precision) (
    SFUNC = madlib.wsr_test_transition,
    STYPE = double precision[],
    INITCOND = '{0,0,0,0,0,0,0,0,0}',
    FINALFUNC = wsr_test_final
);



CREATE AGGREGATE wsr_test(double precision) (
    SFUNC = madlib.wsr_test_transition,
    STYPE = double precision[],
    INITCOND = '{0,0,0,0,0,0,0,0,0}',
    FINALFUNC = wsr_test_final
);



CREATE OPERATOR %*% (
    PROCEDURE = svec_dot,
    LEFTARG = svec,
    RIGHTARG = svec
);



CREATE OPERATOR %*% (
    PROCEDURE = svec_dot,
    LEFTARG = double precision[],
    RIGHTARG = double precision[]
);



CREATE OPERATOR %*% (
    PROCEDURE = svec_dot,
    LEFTARG = double precision[],
    RIGHTARG = svec
);



CREATE OPERATOR %*% (
    PROCEDURE = svec_dot,
    LEFTARG = svec,
    RIGHTARG = double precision[]
);



CREATE OPERATOR * (
    PROCEDURE = svec_mult,
    LEFTARG = svec,
    RIGHTARG = svec
);



CREATE OPERATOR * (
    PROCEDURE = float8arr_mult_float8arr,
    LEFTARG = double precision[],
    RIGHTARG = double precision[]
);



CREATE OPERATOR * (
    PROCEDURE = float8arr_mult_svec,
    LEFTARG = double precision[],
    RIGHTARG = svec
);



CREATE OPERATOR * (
    PROCEDURE = svec_mult_float8arr,
    LEFTARG = svec,
    RIGHTARG = double precision[]
);



CREATE OPERATOR *|| (
    PROCEDURE = svec_concat_replicate,
    LEFTARG = integer,
    RIGHTARG = svec
);



CREATE OPERATOR + (
    PROCEDURE = svec_plus,
    LEFTARG = svec,
    RIGHTARG = svec
);



CREATE OPERATOR + (
    PROCEDURE = float8arr_plus_float8arr,
    LEFTARG = double precision[],
    RIGHTARG = double precision[]
);



CREATE OPERATOR + (
    PROCEDURE = float8arr_plus_svec,
    LEFTARG = double precision[],
    RIGHTARG = svec
);



CREATE OPERATOR + (
    PROCEDURE = svec_plus_float8arr,
    LEFTARG = svec,
    RIGHTARG = double precision[]
);



CREATE OPERATOR - (
    PROCEDURE = svec_minus,
    LEFTARG = svec,
    RIGHTARG = svec
);



CREATE OPERATOR - (
    PROCEDURE = float8arr_minus_float8arr,
    LEFTARG = double precision[],
    RIGHTARG = double precision[]
);



CREATE OPERATOR - (
    PROCEDURE = float8arr_minus_svec,
    LEFTARG = double precision[],
    RIGHTARG = svec
);



CREATE OPERATOR - (
    PROCEDURE = svec_minus_float8arr,
    LEFTARG = svec,
    RIGHTARG = double precision[]
);



CREATE OPERATOR / (
    PROCEDURE = svec_div,
    LEFTARG = svec,
    RIGHTARG = svec
);



CREATE OPERATOR / (
    PROCEDURE = float8arr_div_float8arr,
    LEFTARG = double precision[],
    RIGHTARG = double precision[]
);



CREATE OPERATOR / (
    PROCEDURE = float8arr_div_svec,
    LEFTARG = double precision[],
    RIGHTARG = svec
);



CREATE OPERATOR / (
    PROCEDURE = svec_div_float8arr,
    LEFTARG = svec,
    RIGHTARG = double precision[]
);



CREATE OPERATOR < (
    PROCEDURE = svec_l2_lt,
    LEFTARG = svec,
    RIGHTARG = svec,
    COMMUTATOR = >,
    NEGATOR = >=,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);



CREATE OPERATOR <= (
    PROCEDURE = svec_l2_le,
    LEFTARG = svec,
    RIGHTARG = svec,
    COMMUTATOR = >=,
    NEGATOR = >,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);



CREATE OPERATOR <> (
    PROCEDURE = svec_l2_eq,
    LEFTARG = svec,
    RIGHTARG = svec,
    COMMUTATOR = <>,
    NEGATOR = =,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);



CREATE OPERATOR = (
    PROCEDURE = svec_eq,
    LEFTARG = svec,
    RIGHTARG = svec,
    COMMUTATOR = =,
    NEGATOR = <>,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);



CREATE OPERATOR == (
    PROCEDURE = svec_l2_eq,
    LEFTARG = svec,
    RIGHTARG = svec,
    COMMUTATOR = =,
    NEGATOR = <>,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);



CREATE OPERATOR > (
    PROCEDURE = svec_l2_gt,
    LEFTARG = svec,
    RIGHTARG = svec,
    COMMUTATOR = <,
    NEGATOR = <=,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);



CREATE OPERATOR >= (
    PROCEDURE = svec_l2_ge,
    LEFTARG = svec,
    RIGHTARG = svec,
    COMMUTATOR = <=,
    NEGATOR = <,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);



CREATE OPERATOR ^ (
    PROCEDURE = svec_pow,
    LEFTARG = svec,
    RIGHTARG = svec
);



CREATE OPERATOR || (
    PROCEDURE = svec_concat,
    LEFTARG = svec,
    RIGHTARG = svec
);



CREATE OPERATOR CLASS svec_l2_ops
    DEFAULT FOR TYPE svec USING btree AS
    OPERATOR 1 <(svec,svec) ,
    OPERATOR 2 <=(svec,svec) ,
    OPERATOR 3 ==(svec,svec) ,
    OPERATOR 4 >=(svec,svec) ,
    OPERATOR 5 >(svec,svec) ,
    FUNCTION 1 svec_l2_cmp(svec,svec);


SET search_path = pg_catalog;


CREATE CAST (double precision[] AS madlib.svec) WITH FUNCTION madlib.svec_cast_float8arr(double precision[]);



CREATE CAST (real AS madlib.svec) WITH FUNCTION madlib.svec_cast_float4(real);



CREATE CAST (double precision AS madlib.svec) WITH FUNCTION madlib.svec_cast_float8(double precision);



CREATE CAST (smallint AS madlib.svec) WITH FUNCTION madlib.svec_cast_int2(smallint);



CREATE CAST (integer AS madlib.svec) WITH FUNCTION madlib.svec_cast_int4(integer);



CREATE CAST (bigint AS madlib.svec) WITH FUNCTION madlib.svec_cast_int8(bigint);



CREATE CAST (numeric AS madlib.svec) WITH FUNCTION madlib.svec_cast_numeric(numeric);



CREATE CAST (madlib.svec AS double precision[]) WITH FUNCTION madlib.svec_return_array(madlib.svec);


SET search_path = madlib, pg_catalog;

SET default_tablespace = '';

