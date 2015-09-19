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
    AS '$libdir/gp_svec.so', 'svec_in'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_out(svec) RETURNS cstring
    AS '$libdir/gp_svec.so', 'svec_out'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_recv(internal) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_recv'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_send(svec) RETURNS bytea
    AS '$libdir/gp_svec.so', 'svec_send'
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

CREATE FUNCTION float8arr_cast_float4(real) RETURNS double precision[]
    AS '$libdir/gp_svec.so', 'float8arr_cast_float4'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION float8arr_cast_float8(double precision) RETURNS double precision[]
    AS '$libdir/gp_svec.so', 'float8arr_cast_float8'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION float8arr_cast_int2(smallint) RETURNS double precision[]
    AS '$libdir/gp_svec.so', 'float8arr_cast_int2'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION float8arr_cast_int4(integer) RETURNS double precision[]
    AS '$libdir/gp_svec.so', 'float8arr_cast_int4'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION float8arr_cast_int8(bigint) RETURNS double precision[]
    AS '$libdir/gp_svec.so', 'float8arr_cast_int8'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION float8arr_cast_numeric(numeric) RETURNS double precision[]
    AS '$libdir/gp_svec.so', 'float8arr_cast_numeric'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION float8arr_div_float8arr(double precision[], double precision[]) RETURNS svec
    AS '$libdir/gp_svec.so', 'float8arr_div_float8arr'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION float8arr_div_svec(double precision[], svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'float8arr_div_svec'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION float8arr_eq(double precision[], double precision[]) RETURNS boolean
    AS '$libdir/gp_svec.so', 'float8arr_equals'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION float8arr_minus_float8arr(double precision[], double precision[]) RETURNS svec
    AS '$libdir/gp_svec.so', 'float8arr_minus_float8arr'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION float8arr_minus_svec(double precision[], svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'float8arr_minus_svec'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION float8arr_mult_float8arr(double precision[], double precision[]) RETURNS svec
    AS '$libdir/gp_svec.so', 'float8arr_mult_float8arr'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION float8arr_mult_svec(double precision[], svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'float8arr_mult_svec'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION float8arr_plus_float8arr(double precision[], double precision[]) RETURNS svec
    AS '$libdir/gp_svec.so', 'float8arr_plus_float8arr'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION float8arr_plus_svec(double precision[], svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'float8arr_plus_svec'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION svec_cast_float4(real) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_cast_float4'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_cast_float8(double precision) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_cast_float8'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_cast_float8arr(double precision[]) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_cast_float8arr'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_cast_int2(smallint) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_cast_int2'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_cast_int4(integer) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_cast_int4'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_cast_int8(bigint) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_cast_int8'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_cast_numeric(numeric) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_cast_numeric'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_cast_positions_float8arr(bigint[], double precision[], bigint, double precision) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_cast_positions_float8arr'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_concat(svec, svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_concat'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION svec_concat_replicate(integer, svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_concat_replicate'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION svec_div(svec, svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_div'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_div_float8arr(svec, double precision[]) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_div_float8arr'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION svec_dot(svec, svec) RETURNS double precision
    AS '$libdir/gp_svec.so', 'svec_dot'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_dot(double precision[], double precision[]) RETURNS double precision
    AS '$libdir/gp_svec.so', 'float8arr_dot'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_dot(svec, double precision[]) RETURNS double precision
    AS '$libdir/gp_svec.so', 'svec_dot_float8arr'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_dot(double precision[], svec) RETURNS double precision
    AS '$libdir/gp_svec.so', 'float8arr_dot_svec'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_eq(svec, svec) RETURNS boolean
    AS '$libdir/gp_svec.so', 'svec_eq'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_l2_cmp(svec, svec) RETURNS integer
    AS '$libdir/gp_svec.so', 'svec_l2_cmp'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION svec_l2_eq(svec, svec) RETURNS boolean
    AS '$libdir/gp_svec.so', 'svec_l2_eq'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION svec_l2_ge(svec, svec) RETURNS boolean
    AS '$libdir/gp_svec.so', 'svec_l2_ge'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION svec_l2_gt(svec, svec) RETURNS boolean
    AS '$libdir/gp_svec.so', 'svec_l2_gt'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION svec_l2_le(svec, svec) RETURNS boolean
    AS '$libdir/gp_svec.so', 'svec_l2_le'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION svec_l2_lt(svec, svec) RETURNS boolean
    AS '$libdir/gp_svec.so', 'svec_l2_lt'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION svec_l2_ne(svec, svec) RETURNS boolean
    AS '$libdir/gp_svec.so', 'svec_l2_ne'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION svec_minus(svec, svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_minus'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_minus_float8arr(svec, double precision[]) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_minus_float8arr'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION svec_mult(svec, svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_mult'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_mult_float8arr(svec, double precision[]) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_mult_float8arr'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION svec_plus(svec, svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_plus'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_plus_float8arr(svec, double precision[]) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_plus_float8arr'
    LANGUAGE c IMMUTABLE;

CREATE FUNCTION svec_pow(svec, svec) RETURNS svec
    AS '$libdir/gp_svec.so', 'svec_pow'
    LANGUAGE c IMMUTABLE STRICT;

CREATE FUNCTION svec_return_array(svec) RETURNS double precision[]
    AS '$libdir/gp_svec.so', 'svec_return_array'
    LANGUAGE c IMMUTABLE;

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
