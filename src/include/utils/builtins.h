/*-------------------------------------------------------------------------
 *
 * builtins.h
 *	  Declarations for operations on built-in types.
 *
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/utils/builtins.h,v 1.282.2.2 2007/05/17 23:31:59 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef BUILTINS_H
#define BUILTINS_H

#include "fmgr.h"
#include "nodes/parsenodes.h"
#include "executor/executor.h" /* for AttrMap */

/*
 *		Defined in adt/
 */

/* acl.c */
extern Datum has_table_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_table_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_table_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_table_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_table_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_table_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_database_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_database_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_database_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_database_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_database_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_database_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_foreign_data_wrapper_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_foreign_data_wrapper_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_foreign_data_wrapper_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_foreign_data_wrapper_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_foreign_data_wrapper_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_foreign_data_wrapper_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_function_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_function_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_function_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_function_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_function_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_function_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_language_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_language_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_language_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_language_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_language_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_language_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_schema_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_schema_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_schema_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_schema_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_schema_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_schema_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_server_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_server_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_server_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_server_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_server_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_server_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_tablespace_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_tablespace_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_tablespace_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_tablespace_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_tablespace_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_tablespace_privilege_id(PG_FUNCTION_ARGS);
extern Datum pg_has_role_name_name(PG_FUNCTION_ARGS);
extern Datum pg_has_role_name_id(PG_FUNCTION_ARGS);
extern Datum pg_has_role_id_name(PG_FUNCTION_ARGS);
extern Datum pg_has_role_id_id(PG_FUNCTION_ARGS);
extern Datum pg_has_role_name(PG_FUNCTION_ARGS);
extern Datum pg_has_role_id(PG_FUNCTION_ARGS);

/* bool.c */
extern Datum boolin(PG_FUNCTION_ARGS);
extern Datum boolout(PG_FUNCTION_ARGS);
extern Datum boolrecv(PG_FUNCTION_ARGS);
extern Datum boolsend(PG_FUNCTION_ARGS);
extern Datum booltext(PG_FUNCTION_ARGS);
extern Datum booleq(PG_FUNCTION_ARGS);
extern Datum boolne(PG_FUNCTION_ARGS);
extern Datum boollt(PG_FUNCTION_ARGS);
extern Datum boolgt(PG_FUNCTION_ARGS);
extern Datum boolle(PG_FUNCTION_ARGS);
extern Datum boolge(PG_FUNCTION_ARGS);
extern Datum istrue(PG_FUNCTION_ARGS);
extern Datum isfalse(PG_FUNCTION_ARGS);
extern Datum isnottrue(PG_FUNCTION_ARGS);
extern Datum isnotfalse(PG_FUNCTION_ARGS);
extern Datum booland_statefunc(PG_FUNCTION_ARGS);
extern Datum boolor_statefunc(PG_FUNCTION_ARGS);

/* char.c */
extern Datum charin(PG_FUNCTION_ARGS);
extern Datum charout(PG_FUNCTION_ARGS);
extern Datum charrecv(PG_FUNCTION_ARGS);
extern Datum charsend(PG_FUNCTION_ARGS);
extern Datum chareq(PG_FUNCTION_ARGS);
extern Datum charne(PG_FUNCTION_ARGS);
extern Datum charlt(PG_FUNCTION_ARGS);
extern Datum charle(PG_FUNCTION_ARGS);
extern Datum chargt(PG_FUNCTION_ARGS);
extern Datum charge(PG_FUNCTION_ARGS);
extern Datum chartoi4(PG_FUNCTION_ARGS);
extern Datum i4tochar(PG_FUNCTION_ARGS);
extern Datum text_char(PG_FUNCTION_ARGS);
extern Datum char_text(PG_FUNCTION_ARGS);

/* domains.c */
extern Datum domain_in(PG_FUNCTION_ARGS);
extern Datum domain_recv(PG_FUNCTION_ARGS);

/* int.c */
extern Datum int2in(PG_FUNCTION_ARGS);
extern Datum int2out(PG_FUNCTION_ARGS);
extern Datum int2recv(PG_FUNCTION_ARGS);
extern Datum int2send(PG_FUNCTION_ARGS);
extern Datum int2vectorin(PG_FUNCTION_ARGS);
extern Datum int2vectorout(PG_FUNCTION_ARGS);
extern Datum int2vectorrecv(PG_FUNCTION_ARGS);
extern Datum int2vectorsend(PG_FUNCTION_ARGS);
extern Datum int2vectoreq(PG_FUNCTION_ARGS);
extern Datum int4in(PG_FUNCTION_ARGS);
extern Datum int4out(PG_FUNCTION_ARGS);
extern Datum int4recv(PG_FUNCTION_ARGS);
extern Datum int4send(PG_FUNCTION_ARGS);
extern Datum i2toi4(PG_FUNCTION_ARGS);
extern Datum i4toi2(PG_FUNCTION_ARGS);
extern Datum int2_text(PG_FUNCTION_ARGS);
extern Datum text_int2(PG_FUNCTION_ARGS);
extern Datum int4_bool(PG_FUNCTION_ARGS);
extern Datum bool_int4(PG_FUNCTION_ARGS);
extern Datum int4_text(PG_FUNCTION_ARGS);
extern Datum text_int4(PG_FUNCTION_ARGS);
extern Datum int4eq(PG_FUNCTION_ARGS);
extern Datum int4ne(PG_FUNCTION_ARGS);
extern Datum int4lt(PG_FUNCTION_ARGS);
extern Datum int4le(PG_FUNCTION_ARGS);
extern Datum int4gt(PG_FUNCTION_ARGS);
extern Datum int4ge(PG_FUNCTION_ARGS);
extern Datum int2eq(PG_FUNCTION_ARGS);
extern Datum int2ne(PG_FUNCTION_ARGS);
extern Datum int2lt(PG_FUNCTION_ARGS);
extern Datum int2le(PG_FUNCTION_ARGS);
extern Datum int2gt(PG_FUNCTION_ARGS);
extern Datum int2ge(PG_FUNCTION_ARGS);
extern Datum int24eq(PG_FUNCTION_ARGS);
extern Datum int24ne(PG_FUNCTION_ARGS);
extern Datum int24lt(PG_FUNCTION_ARGS);
extern Datum int24le(PG_FUNCTION_ARGS);
extern Datum int24gt(PG_FUNCTION_ARGS);
extern Datum int24ge(PG_FUNCTION_ARGS);
extern Datum int42eq(PG_FUNCTION_ARGS);
extern Datum int42ne(PG_FUNCTION_ARGS);
extern Datum int42lt(PG_FUNCTION_ARGS);
extern Datum int42le(PG_FUNCTION_ARGS);
extern Datum int42gt(PG_FUNCTION_ARGS);
extern Datum int42ge(PG_FUNCTION_ARGS);
extern Datum int4um(PG_FUNCTION_ARGS);
extern Datum int4up(PG_FUNCTION_ARGS);
extern Datum int4pl(PG_FUNCTION_ARGS);
extern Datum int4mi(PG_FUNCTION_ARGS);
extern Datum int4mul(PG_FUNCTION_ARGS);
extern Datum int4div(PG_FUNCTION_ARGS);
extern Datum int4abs(PG_FUNCTION_ARGS);
extern Datum int4inc(PG_FUNCTION_ARGS);
extern Datum int2um(PG_FUNCTION_ARGS);
extern Datum int2up(PG_FUNCTION_ARGS);
extern Datum int2pl(PG_FUNCTION_ARGS);
extern Datum int2mi(PG_FUNCTION_ARGS);
extern Datum int2mul(PG_FUNCTION_ARGS);
extern Datum int2div(PG_FUNCTION_ARGS);
extern Datum int2abs(PG_FUNCTION_ARGS);
extern Datum int24pl(PG_FUNCTION_ARGS);
extern Datum int24mi(PG_FUNCTION_ARGS);
extern Datum int24mul(PG_FUNCTION_ARGS);
extern Datum int24div(PG_FUNCTION_ARGS);
extern Datum int42pl(PG_FUNCTION_ARGS);
extern Datum int42mi(PG_FUNCTION_ARGS);
extern Datum int42mul(PG_FUNCTION_ARGS);
extern Datum int42div(PG_FUNCTION_ARGS);
extern Datum int4mod(PG_FUNCTION_ARGS);
extern Datum int2mod(PG_FUNCTION_ARGS);
extern Datum int24mod(PG_FUNCTION_ARGS);
extern Datum int42mod(PG_FUNCTION_ARGS);
extern Datum int2larger(PG_FUNCTION_ARGS);
extern Datum int2smaller(PG_FUNCTION_ARGS);
extern Datum int4larger(PG_FUNCTION_ARGS);
extern Datum int4smaller(PG_FUNCTION_ARGS);

extern Datum int4and(PG_FUNCTION_ARGS);
extern Datum int4or(PG_FUNCTION_ARGS);
extern Datum int4xor(PG_FUNCTION_ARGS);
extern Datum int4not(PG_FUNCTION_ARGS);
extern Datum int4shl(PG_FUNCTION_ARGS);
extern Datum int4shr(PG_FUNCTION_ARGS);
extern Datum int2and(PG_FUNCTION_ARGS);
extern Datum int2or(PG_FUNCTION_ARGS);
extern Datum int2xor(PG_FUNCTION_ARGS);
extern Datum int2not(PG_FUNCTION_ARGS);
extern Datum int2shl(PG_FUNCTION_ARGS);
extern Datum int2shr(PG_FUNCTION_ARGS);
extern Datum generate_series_int4(PG_FUNCTION_ARGS);
extern Datum generate_series_step_int4(PG_FUNCTION_ARGS);
extern int2vector *buildint2vector(const int2 *int2s, int n);

/* name.c */
extern Datum namein(PG_FUNCTION_ARGS);
extern Datum nameout(PG_FUNCTION_ARGS);
extern Datum namerecv(PG_FUNCTION_ARGS);
extern Datum namesend(PG_FUNCTION_ARGS);
extern Datum nameeq(PG_FUNCTION_ARGS);
extern Datum namene(PG_FUNCTION_ARGS);
extern Datum namelt(PG_FUNCTION_ARGS);
extern Datum namele(PG_FUNCTION_ARGS);
extern Datum namegt(PG_FUNCTION_ARGS);
extern Datum namege(PG_FUNCTION_ARGS);
extern Datum name_pattern_eq(PG_FUNCTION_ARGS);
extern Datum name_pattern_ne(PG_FUNCTION_ARGS);
extern Datum name_pattern_lt(PG_FUNCTION_ARGS);
extern Datum name_pattern_le(PG_FUNCTION_ARGS);
extern Datum name_pattern_gt(PG_FUNCTION_ARGS);
extern Datum name_pattern_ge(PG_FUNCTION_ARGS);
extern int	namecpy(Name n1, Name n2);
extern int	namestrcpy(Name name, const char *str);
extern int	namestrcmp(Name name, const char *str);
extern Datum current_user(PG_FUNCTION_ARGS);
extern Datum session_user(PG_FUNCTION_ARGS);
extern Datum current_schema(PG_FUNCTION_ARGS);
extern Datum current_schemas(PG_FUNCTION_ARGS);

/* numutils.c */
extern int32 pg_atoi(char *s, int size, int c);
extern void pg_itoa(int16 i, char *a);
extern void pg_ltoa(int32 l, char *a);

/*
 *		Per-opclass comparison functions for new btrees.  These are
 *		stored in pg_amproc and defined in access/nbtree/nbtcompare.c
 */
extern Datum btboolcmp(PG_FUNCTION_ARGS);
extern Datum btint2cmp(PG_FUNCTION_ARGS);
extern Datum btint4cmp(PG_FUNCTION_ARGS);
extern Datum btint8cmp(PG_FUNCTION_ARGS);
extern Datum btfloat4cmp(PG_FUNCTION_ARGS);
extern Datum btfloat8cmp(PG_FUNCTION_ARGS);
extern Datum btint48cmp(PG_FUNCTION_ARGS);
extern Datum btint84cmp(PG_FUNCTION_ARGS);
extern Datum btint24cmp(PG_FUNCTION_ARGS);
extern Datum btint42cmp(PG_FUNCTION_ARGS);
extern Datum btint28cmp(PG_FUNCTION_ARGS);
extern Datum btint82cmp(PG_FUNCTION_ARGS);
extern Datum btfloat48cmp(PG_FUNCTION_ARGS);
extern Datum btfloat84cmp(PG_FUNCTION_ARGS);
extern Datum btoidcmp(PG_FUNCTION_ARGS);
extern Datum btoidvectorcmp(PG_FUNCTION_ARGS);
extern Datum btabstimecmp(PG_FUNCTION_ARGS);
extern Datum btreltimecmp(PG_FUNCTION_ARGS);
extern Datum bttintervalcmp(PG_FUNCTION_ARGS);
extern Datum btcharcmp(PG_FUNCTION_ARGS);
extern Datum btnamecmp(PG_FUNCTION_ARGS);
extern Datum bttextcmp(PG_FUNCTION_ARGS);
extern Datum btname_pattern_cmp(PG_FUNCTION_ARGS);
extern Datum bttext_pattern_cmp(PG_FUNCTION_ARGS);

/* float.c */
extern PGDLLIMPORT int extra_float_digits;

extern double get_float8_infinity(void);
extern float get_float4_infinity(void);
extern double get_float8_nan(void);
extern float get_float4_nan(void);
extern int	is_infinite(double val);

extern Datum float4in(PG_FUNCTION_ARGS);
extern Datum float4out(PG_FUNCTION_ARGS);
extern Datum float4recv(PG_FUNCTION_ARGS);
extern Datum float4send(PG_FUNCTION_ARGS);
extern Datum float8in(PG_FUNCTION_ARGS);
extern Datum float8out(PG_FUNCTION_ARGS);
extern Datum float8recv(PG_FUNCTION_ARGS);
extern Datum float8send(PG_FUNCTION_ARGS);
extern Datum float4abs(PG_FUNCTION_ARGS);
extern Datum float4um(PG_FUNCTION_ARGS);
extern Datum float4up(PG_FUNCTION_ARGS);
extern Datum float4larger(PG_FUNCTION_ARGS);
extern Datum float4smaller(PG_FUNCTION_ARGS);
extern Datum float8abs(PG_FUNCTION_ARGS);
extern Datum float8um(PG_FUNCTION_ARGS);
extern Datum float8up(PG_FUNCTION_ARGS);
extern Datum float8larger(PG_FUNCTION_ARGS);
extern Datum float8smaller(PG_FUNCTION_ARGS);
extern Datum float4pl(PG_FUNCTION_ARGS);
extern Datum float4mi(PG_FUNCTION_ARGS);
extern Datum float4mul(PG_FUNCTION_ARGS);
extern Datum float4div(PG_FUNCTION_ARGS);
extern Datum float8pl(PG_FUNCTION_ARGS);
extern Datum float8mi(PG_FUNCTION_ARGS);
extern Datum float8mul(PG_FUNCTION_ARGS);
extern Datum float8div(PG_FUNCTION_ARGS);
extern Datum float4eq(PG_FUNCTION_ARGS);
extern Datum float4ne(PG_FUNCTION_ARGS);
extern Datum float4lt(PG_FUNCTION_ARGS);
extern Datum float4le(PG_FUNCTION_ARGS);
extern Datum float4gt(PG_FUNCTION_ARGS);
extern Datum float4ge(PG_FUNCTION_ARGS);
extern Datum float8eq(PG_FUNCTION_ARGS);
extern Datum float8ne(PG_FUNCTION_ARGS);
extern Datum float8lt(PG_FUNCTION_ARGS);
extern Datum float8le(PG_FUNCTION_ARGS);
extern Datum float8gt(PG_FUNCTION_ARGS);
extern Datum float8ge(PG_FUNCTION_ARGS);
extern Datum ftod(PG_FUNCTION_ARGS);
extern Datum i4tod(PG_FUNCTION_ARGS);
extern Datum i2tod(PG_FUNCTION_ARGS);
extern Datum dtof(PG_FUNCTION_ARGS);
extern Datum dtoi4(PG_FUNCTION_ARGS);
extern Datum dtoi2(PG_FUNCTION_ARGS);
extern Datum i4tof(PG_FUNCTION_ARGS);
extern Datum i2tof(PG_FUNCTION_ARGS);
extern Datum ftoi4(PG_FUNCTION_ARGS);
extern Datum ftoi2(PG_FUNCTION_ARGS);
extern Datum text_float8(PG_FUNCTION_ARGS);
extern Datum text_float4(PG_FUNCTION_ARGS);
extern Datum float8_text(PG_FUNCTION_ARGS);
extern Datum float4_text(PG_FUNCTION_ARGS);
extern Datum dround(PG_FUNCTION_ARGS);
extern Datum dceil(PG_FUNCTION_ARGS);
extern Datum dfloor(PG_FUNCTION_ARGS);
extern Datum dsign(PG_FUNCTION_ARGS);
extern Datum dtrunc(PG_FUNCTION_ARGS);
extern Datum dsqrt(PG_FUNCTION_ARGS);
extern Datum dcbrt(PG_FUNCTION_ARGS);
extern Datum dpow(PG_FUNCTION_ARGS);
extern Datum dexp(PG_FUNCTION_ARGS);
extern Datum dlog1(PG_FUNCTION_ARGS);
extern Datum dlog10(PG_FUNCTION_ARGS);
extern Datum dacos(PG_FUNCTION_ARGS);
extern Datum dasin(PG_FUNCTION_ARGS);
extern Datum datan(PG_FUNCTION_ARGS);
extern Datum datan2(PG_FUNCTION_ARGS);
extern Datum dcos(PG_FUNCTION_ARGS);
extern Datum dcosh(PG_FUNCTION_ARGS);
extern Datum dcot(PG_FUNCTION_ARGS);
extern Datum dsin(PG_FUNCTION_ARGS);
extern Datum dsinh(PG_FUNCTION_ARGS);
extern Datum dtan(PG_FUNCTION_ARGS);
extern Datum dtanh(PG_FUNCTION_ARGS);
extern Datum degrees(PG_FUNCTION_ARGS);
extern Datum dpi(PG_FUNCTION_ARGS);
extern Datum radians(PG_FUNCTION_ARGS);
extern Datum drandom(PG_FUNCTION_ARGS);
extern Datum setseed(PG_FUNCTION_ARGS);
extern Datum float8_accum(PG_FUNCTION_ARGS);
extern Datum float4_accum(PG_FUNCTION_ARGS);
extern Datum float8_decum(PG_FUNCTION_ARGS);
extern Datum float4_decum(PG_FUNCTION_ARGS);
extern Datum float8_avg(PG_FUNCTION_ARGS);
extern Datum float8_var_pop(PG_FUNCTION_ARGS);
extern Datum float8_var_samp(PG_FUNCTION_ARGS);
extern Datum float8_stddev_pop(PG_FUNCTION_ARGS);
extern Datum float8_stddev_samp(PG_FUNCTION_ARGS);
extern Datum float8_regr_accum(PG_FUNCTION_ARGS);
extern Datum float8_regr_sxx(PG_FUNCTION_ARGS);
extern Datum float8_regr_syy(PG_FUNCTION_ARGS);
extern Datum float8_regr_sxy(PG_FUNCTION_ARGS);
extern Datum float8_regr_avgx(PG_FUNCTION_ARGS);
extern Datum float8_regr_avgy(PG_FUNCTION_ARGS);
extern Datum float8_covar_pop(PG_FUNCTION_ARGS);
extern Datum float8_covar_samp(PG_FUNCTION_ARGS);
extern Datum float8_corr(PG_FUNCTION_ARGS);
extern Datum float8_regr_r2(PG_FUNCTION_ARGS);
extern Datum float8_regr_slope(PG_FUNCTION_ARGS);
extern Datum float8_regr_intercept(PG_FUNCTION_ARGS);
extern Datum float48pl(PG_FUNCTION_ARGS);
extern Datum float48mi(PG_FUNCTION_ARGS);
extern Datum float48mul(PG_FUNCTION_ARGS);
extern Datum float48div(PG_FUNCTION_ARGS);
extern Datum float84pl(PG_FUNCTION_ARGS);
extern Datum float84mi(PG_FUNCTION_ARGS);
extern Datum float84mul(PG_FUNCTION_ARGS);
extern Datum float84div(PG_FUNCTION_ARGS);
extern Datum float48eq(PG_FUNCTION_ARGS);
extern Datum float48ne(PG_FUNCTION_ARGS);
extern Datum float48lt(PG_FUNCTION_ARGS);
extern Datum float48le(PG_FUNCTION_ARGS);
extern Datum float48gt(PG_FUNCTION_ARGS);
extern Datum float48ge(PG_FUNCTION_ARGS);
extern Datum float84eq(PG_FUNCTION_ARGS);
extern Datum float84ne(PG_FUNCTION_ARGS);
extern Datum float84lt(PG_FUNCTION_ARGS);
extern Datum float84le(PG_FUNCTION_ARGS);
extern Datum float84gt(PG_FUNCTION_ARGS);
extern Datum float84ge(PG_FUNCTION_ARGS);
extern Datum width_bucket_float8(PG_FUNCTION_ARGS);
extern Datum float8_amalg(PG_FUNCTION_ARGS); /* MPP */
extern Datum float8_demalg(PG_FUNCTION_ARGS); /* MPP */
extern Datum float8_regr_amalg(PG_FUNCTION_ARGS); /* MPP */
extern Datum pg_highest_oid(PG_FUNCTION_ARGS); /* MPP */
extern Datum gp_max_distributed_xid(PG_FUNCTION_ARGS); /* MPP */
extern Datum gp_distributed_xid(PG_FUNCTION_ARGS); /* MPP */

/* dbsize.c */
extern Datum pg_tablespace_size_oid(PG_FUNCTION_ARGS);
extern Datum pg_tablespace_size_name(PG_FUNCTION_ARGS);
extern Datum pg_database_size_oid(PG_FUNCTION_ARGS);
extern Datum pg_database_size_name(PG_FUNCTION_ARGS);
extern Datum pg_relation_size_oid(PG_FUNCTION_ARGS);
extern Datum pg_relation_size_name(PG_FUNCTION_ARGS);
extern Datum pg_total_relation_size_oid(PG_FUNCTION_ARGS);
extern Datum pg_total_relation_size_name(PG_FUNCTION_ARGS);
extern Datum pg_size_pretty(PG_FUNCTION_ARGS);
/*extern Datum gp_statistics_estimate_reltuples_relpages_oid(PG_FUNCTION_ARGS);*/

/* genfile.c */
extern Datum pg_stat_file(PG_FUNCTION_ARGS);
extern Datum pg_read_file(PG_FUNCTION_ARGS);
extern Datum pg_ls_dir(PG_FUNCTION_ARGS);
extern Datum pg_file_write(PG_FUNCTION_ARGS);
extern Datum pg_file_rename(PG_FUNCTION_ARGS);
extern Datum pg_file_unlink(PG_FUNCTION_ARGS);
extern Datum pg_logdir_ls(PG_FUNCTION_ARGS);
extern Datum pg_file_length(PG_FUNCTION_ARGS);

/* misc.c */
extern Datum nullvalue(PG_FUNCTION_ARGS);
extern Datum nonnullvalue(PG_FUNCTION_ARGS);
extern Datum current_database(PG_FUNCTION_ARGS);
extern Datum current_query(PG_FUNCTION_ARGS);
extern Datum pg_cancel_backend(PG_FUNCTION_ARGS);
extern Datum pg_terminate_backend(PG_FUNCTION_ARGS);
extern Datum gp_cancel_query(PG_FUNCTION_ARGS);
extern Datum pg_reload_conf(PG_FUNCTION_ARGS);
extern Datum pg_tablespace_databases(PG_FUNCTION_ARGS);
extern Datum pg_rotate_logfile(PG_FUNCTION_ARGS);
extern Datum pg_sleep(PG_FUNCTION_ARGS);
extern Datum pg_get_keywords(PG_FUNCTION_ARGS);
extern Datum pg_typeof(PG_FUNCTION_ARGS);

/* not_in.c */
extern Datum int4notin(PG_FUNCTION_ARGS);
extern Datum oidnotin(PG_FUNCTION_ARGS);

/* oid.c */
extern Datum oidin(PG_FUNCTION_ARGS);
extern Datum oidout(PG_FUNCTION_ARGS);
extern Datum oidrecv(PG_FUNCTION_ARGS);
extern Datum oidsend(PG_FUNCTION_ARGS);
extern Datum oideq(PG_FUNCTION_ARGS);
extern Datum oidne(PG_FUNCTION_ARGS);
extern Datum oidlt(PG_FUNCTION_ARGS);
extern Datum oidle(PG_FUNCTION_ARGS);
extern Datum oidge(PG_FUNCTION_ARGS);
extern Datum oidgt(PG_FUNCTION_ARGS);
extern Datum oidlarger(PG_FUNCTION_ARGS);
extern Datum oidsmaller(PG_FUNCTION_ARGS);
extern Datum oid_text(PG_FUNCTION_ARGS);
extern Datum text_oid(PG_FUNCTION_ARGS);
extern Datum oidvectorin(PG_FUNCTION_ARGS);
extern Datum oidvectorout(PG_FUNCTION_ARGS);
extern Datum oidvectorrecv(PG_FUNCTION_ARGS);
extern Datum oidvectorsend(PG_FUNCTION_ARGS);
extern Datum oidvectoreq(PG_FUNCTION_ARGS);
extern Datum oidvectorne(PG_FUNCTION_ARGS);
extern Datum oidvectorlt(PG_FUNCTION_ARGS);
extern Datum oidvectorle(PG_FUNCTION_ARGS);
extern Datum oidvectorge(PG_FUNCTION_ARGS);
extern Datum oidvectorgt(PG_FUNCTION_ARGS);
extern oidvector *buildoidvector(const Oid *oids, int n);

/* pseudotypes.c */
extern Datum cstring_in(PG_FUNCTION_ARGS);
extern Datum cstring_out(PG_FUNCTION_ARGS);
extern Datum cstring_recv(PG_FUNCTION_ARGS);
extern Datum cstring_send(PG_FUNCTION_ARGS);
extern Datum any_in(PG_FUNCTION_ARGS);
extern Datum any_out(PG_FUNCTION_ARGS);
extern Datum anyarray_in(PG_FUNCTION_ARGS);
extern Datum anyarray_out(PG_FUNCTION_ARGS);
extern Datum anyarray_recv(PG_FUNCTION_ARGS);
extern Datum anyarray_send(PG_FUNCTION_ARGS);
extern Datum void_in(PG_FUNCTION_ARGS);
extern Datum void_out(PG_FUNCTION_ARGS);
extern Datum trigger_in(PG_FUNCTION_ARGS);
extern Datum trigger_out(PG_FUNCTION_ARGS);
extern Datum language_handler_in(PG_FUNCTION_ARGS);
extern Datum language_handler_out(PG_FUNCTION_ARGS);
extern Datum internal_in(PG_FUNCTION_ARGS);
extern Datum internal_out(PG_FUNCTION_ARGS);
extern Datum opaque_in(PG_FUNCTION_ARGS);
extern Datum opaque_out(PG_FUNCTION_ARGS);
extern Datum anyelement_in(PG_FUNCTION_ARGS);
extern Datum anyelement_out(PG_FUNCTION_ARGS);
extern Datum shell_in(PG_FUNCTION_ARGS);
extern Datum shell_out(PG_FUNCTION_ARGS);
extern Datum anytable_in(PG_FUNCTION_ARGS);
extern Datum anytable_out(PG_FUNCTION_ARGS);

/* regexp.c */
extern Datum nameregexeq(PG_FUNCTION_ARGS);
extern Datum nameregexne(PG_FUNCTION_ARGS);
extern Datum textregexeq(PG_FUNCTION_ARGS);
extern Datum textregexne(PG_FUNCTION_ARGS);
extern Datum nameicregexeq(PG_FUNCTION_ARGS);
extern Datum nameicregexne(PG_FUNCTION_ARGS);
extern Datum texticregexeq(PG_FUNCTION_ARGS);
extern Datum texticregexne(PG_FUNCTION_ARGS);
extern Datum textregexsubstr(PG_FUNCTION_ARGS);
extern Datum textregexreplace_noopt(PG_FUNCTION_ARGS);
extern Datum textregexreplace(PG_FUNCTION_ARGS);
extern Datum similar_escape(PG_FUNCTION_ARGS);
extern Datum regexp_matches(PG_FUNCTION_ARGS);
extern Datum regexp_matches_no_flags(PG_FUNCTION_ARGS);
extern Datum regexp_split_to_table(PG_FUNCTION_ARGS);
extern Datum regexp_split_to_table_no_flags(PG_FUNCTION_ARGS);
extern Datum regexp_split_to_array(PG_FUNCTION_ARGS);
extern Datum regexp_split_to_array_no_flags(PG_FUNCTION_ARGS);
extern bool regex_flavor_is_basic(void);

/* regproc.c */
extern Datum regprocin(PG_FUNCTION_ARGS);
extern Datum regprocout(PG_FUNCTION_ARGS);
extern Datum regprocrecv(PG_FUNCTION_ARGS);
extern Datum regprocsend(PG_FUNCTION_ARGS);
extern Datum regprocedurein(PG_FUNCTION_ARGS);
extern Datum regprocedureout(PG_FUNCTION_ARGS);
extern Datum regprocedurerecv(PG_FUNCTION_ARGS);
extern Datum regproceduresend(PG_FUNCTION_ARGS);
extern Datum regoperin(PG_FUNCTION_ARGS);
extern Datum regoperout(PG_FUNCTION_ARGS);
extern Datum regoperrecv(PG_FUNCTION_ARGS);
extern Datum regopersend(PG_FUNCTION_ARGS);
extern Datum regoperatorin(PG_FUNCTION_ARGS);
extern Datum regoperatorout(PG_FUNCTION_ARGS);
extern Datum regoperatorrecv(PG_FUNCTION_ARGS);
extern Datum regoperatorsend(PG_FUNCTION_ARGS);
extern Datum regclassin(PG_FUNCTION_ARGS);
extern Datum regclassout(PG_FUNCTION_ARGS);
extern Datum regclassrecv(PG_FUNCTION_ARGS);
extern Datum regclasssend(PG_FUNCTION_ARGS);
extern Datum regtypein(PG_FUNCTION_ARGS);
extern Datum regtypeout(PG_FUNCTION_ARGS);
extern Datum regtyperecv(PG_FUNCTION_ARGS);
extern Datum regtypesend(PG_FUNCTION_ARGS);
extern Datum text_regclass(PG_FUNCTION_ARGS);
extern List *stringToQualifiedNameList(const char *string, const char *caller);
extern char *format_procedure(Oid procedure_oid);
extern char *format_operator(Oid operator_oid);

/* rowtypes.c */
extern Datum record_in(PG_FUNCTION_ARGS);
extern Datum record_out(PG_FUNCTION_ARGS);
extern Datum record_recv(PG_FUNCTION_ARGS);
extern Datum record_send(PG_FUNCTION_ARGS);

/* ruleutils.c */
extern Datum pg_get_ruledef(PG_FUNCTION_ARGS);
extern Datum pg_get_ruledef_ext(PG_FUNCTION_ARGS);
extern Datum pg_get_viewdef(PG_FUNCTION_ARGS);
extern Datum pg_get_viewdef_ext(PG_FUNCTION_ARGS);
extern Datum pg_get_viewdef_name(PG_FUNCTION_ARGS);
extern Datum pg_get_viewdef_name_ext(PG_FUNCTION_ARGS);
extern Datum pg_get_indexdef(PG_FUNCTION_ARGS);
extern Datum pg_get_indexdef_ext(PG_FUNCTION_ARGS);
extern char *pg_get_indexdef_string(Oid indexrelid);
extern Datum pg_get_triggerdef(PG_FUNCTION_ARGS);
extern Datum pg_get_constraintdef(PG_FUNCTION_ARGS);
extern Datum pg_get_constraintdef_ext(PG_FUNCTION_ARGS);
extern char *pg_get_constraintdef_string(Oid constraintId);
extern char *pg_get_constraintexpr_string(Oid constraintId);
extern Datum pg_get_expr(PG_FUNCTION_ARGS);
extern Datum pg_get_expr_ext(PG_FUNCTION_ARGS);
extern Datum pg_get_userbyid(PG_FUNCTION_ARGS);
extern Datum pg_get_serial_sequence(PG_FUNCTION_ARGS);
extern char *deparse_expression(Node *expr, List *dpcontext,
				   bool forceprefix, bool showimplicit);
extern char *deparse_expr_sweet(Node *expr, List *dpcontext,
				   bool forceprefix, bool showimplicit);                /*CDB*/
extern List *deparse_context_for(const char *aliasname, Oid relid);
extern List *deparse_context_for_plan(int outer_varno, Node *outercontext,
						 int inner_varno, Node *innercontext,
						 List *rtable);
extern Node *deparse_context_for_subplan(const char *name, Node *subplan);
extern const char *quote_literal_internal(const char *literal);
extern const char *quote_identifier(const char *ident);
extern char *quote_qualified_identifier(const char *qualifier,
						   const char *ident);
extern Datum pg_get_partition_def(PG_FUNCTION_ARGS);
extern Datum pg_get_partition_def_ext(PG_FUNCTION_ARGS);
extern Datum pg_get_partition_def_ext2(PG_FUNCTION_ARGS);
extern Datum pg_get_partition_rule_def(PG_FUNCTION_ARGS);
extern Datum pg_get_partition_rule_def_ext(PG_FUNCTION_ARGS);
extern Datum pg_get_partition_template_def(PG_FUNCTION_ARGS);

/* tid.c */
extern Datum tidin(PG_FUNCTION_ARGS);
extern Datum tidout(PG_FUNCTION_ARGS);
extern Datum tidrecv(PG_FUNCTION_ARGS);
extern Datum tidsend(PG_FUNCTION_ARGS);
extern Datum tidtoi8(PG_FUNCTION_ARGS);     /*CDB*/
extern Datum tideq(PG_FUNCTION_ARGS);
extern Datum tidne(PG_FUNCTION_ARGS);
extern Datum tidlt(PG_FUNCTION_ARGS);
extern Datum tidle(PG_FUNCTION_ARGS);
extern Datum tidgt(PG_FUNCTION_ARGS);
extern Datum tidge(PG_FUNCTION_ARGS);
extern Datum bttidcmp(PG_FUNCTION_ARGS);
extern Datum tidlarger(PG_FUNCTION_ARGS);
extern Datum tidsmaller(PG_FUNCTION_ARGS);
extern Datum currtid_byreloid(PG_FUNCTION_ARGS);
extern Datum currtid_byrelname(PG_FUNCTION_ARGS);

/* appendonlytid.c */
extern Datum gpaotidin(PG_FUNCTION_ARGS);
extern Datum gpaotidout(PG_FUNCTION_ARGS);
extern Datum gpaotidrecv(PG_FUNCTION_ARGS);
extern Datum gpaotidsend(PG_FUNCTION_ARGS);

/* xlog.c */
extern Datum gpxloglocin(PG_FUNCTION_ARGS);
extern Datum gpxloglocout(PG_FUNCTION_ARGS);
extern Datum gpxloglocrecv(PG_FUNCTION_ARGS);
extern Datum gpxloglocsend(PG_FUNCTION_ARGS);
extern Datum gpxlogloclarger(PG_FUNCTION_ARGS);
extern Datum gpxloglocsmaller(PG_FUNCTION_ARGS);
extern Datum gpxlogloceq(PG_FUNCTION_ARGS);
extern Datum gpxloglocne(PG_FUNCTION_ARGS);
extern Datum gpxlogloclt(PG_FUNCTION_ARGS);
extern Datum gpxloglocle(PG_FUNCTION_ARGS);
extern Datum gpxloglocgt(PG_FUNCTION_ARGS);
extern Datum gpxloglocge(PG_FUNCTION_ARGS);
extern Datum btgpxlogloccmp(PG_FUNCTION_ARGS);

/* varchar.c */
extern Datum bpcharin(PG_FUNCTION_ARGS);
extern Datum bpcharout(PG_FUNCTION_ARGS);
extern Datum bpcharrecv(PG_FUNCTION_ARGS);
extern Datum bpcharsend(PG_FUNCTION_ARGS);
extern Datum bpchartypmodin(PG_FUNCTION_ARGS);
extern Datum bpchartypmodout(PG_FUNCTION_ARGS);
extern Datum bpchar(PG_FUNCTION_ARGS);
extern Datum char_bpchar(PG_FUNCTION_ARGS);
extern Datum name_bpchar(PG_FUNCTION_ARGS);
extern Datum bpchar_name(PG_FUNCTION_ARGS);
extern Datum bpchareq(PG_FUNCTION_ARGS);
extern Datum bpcharne(PG_FUNCTION_ARGS);
extern Datum bpcharlt(PG_FUNCTION_ARGS);
extern Datum bpcharle(PG_FUNCTION_ARGS);
extern Datum bpchargt(PG_FUNCTION_ARGS);
extern Datum bpcharge(PG_FUNCTION_ARGS);
extern Datum bpcharcmp(PG_FUNCTION_ARGS);
extern Datum bpchar_larger(PG_FUNCTION_ARGS);
extern Datum bpchar_smaller(PG_FUNCTION_ARGS);
extern Datum bpcharlen(PG_FUNCTION_ARGS);
extern Datum bpcharoctetlen(PG_FUNCTION_ARGS);
extern Datum hashbpchar(PG_FUNCTION_ARGS);
extern Datum bpchar_pattern_lt(PG_FUNCTION_ARGS);
extern Datum bpchar_pattern_le(PG_FUNCTION_ARGS);
extern Datum bpchar_pattern_gt(PG_FUNCTION_ARGS);
extern Datum bpchar_pattern_ge(PG_FUNCTION_ARGS);
extern Datum btbpchar_pattern_cmp(PG_FUNCTION_ARGS);

extern Datum varcharin(PG_FUNCTION_ARGS);
extern Datum varcharout(PG_FUNCTION_ARGS);
extern Datum varcharrecv(PG_FUNCTION_ARGS);
extern Datum varcharsend(PG_FUNCTION_ARGS);
extern Datum varchartypmodin(PG_FUNCTION_ARGS);
extern Datum varchartypmodout(PG_FUNCTION_ARGS);
extern Datum varchar(PG_FUNCTION_ARGS);

/* varlena.c */
extern text *cstring_to_text(const char *s);
extern text *cstring_to_text_with_len(const char *s, int len);
extern char *text_to_cstring(const text *t);
extern void text_to_cstring_buffer(const text *src, char *dst, size_t dst_len);

#define CStringGetTextDatum(s) PointerGetDatum(cstring_to_text(s))
#define TextDatumGetCString(d) text_to_cstring((text *) DatumGetPointer(d))

extern Datum textin(PG_FUNCTION_ARGS);
extern Datum textout(PG_FUNCTION_ARGS);
extern Datum textrecv(PG_FUNCTION_ARGS);
extern Datum textsend(PG_FUNCTION_ARGS);
extern Datum textcat(PG_FUNCTION_ARGS);
extern Datum texteq(PG_FUNCTION_ARGS);
extern Datum textne(PG_FUNCTION_ARGS);
extern Datum text_lt(PG_FUNCTION_ARGS);
extern Datum text_le(PG_FUNCTION_ARGS);
extern Datum text_gt(PG_FUNCTION_ARGS);
extern Datum text_ge(PG_FUNCTION_ARGS);
extern Datum text_larger(PG_FUNCTION_ARGS);
extern Datum text_smaller(PG_FUNCTION_ARGS);
extern Datum text_pattern_lt(PG_FUNCTION_ARGS);
extern Datum text_pattern_le(PG_FUNCTION_ARGS);
extern Datum text_pattern_gt(PG_FUNCTION_ARGS);
extern Datum text_pattern_ge(PG_FUNCTION_ARGS);
extern Datum bttext_pattern_cmp(PG_FUNCTION_ARGS);
extern Datum textlen(PG_FUNCTION_ARGS);
extern Datum textoctetlen(PG_FUNCTION_ARGS);
extern Datum textpos(PG_FUNCTION_ARGS);
extern Datum text_substr(PG_FUNCTION_ARGS);
extern Datum text_substr_no_len(PG_FUNCTION_ARGS);
extern Datum name_text(PG_FUNCTION_ARGS);
extern Datum text_name(PG_FUNCTION_ARGS);
extern int	varstr_cmp(char *arg1, int len1, char *arg2, int len2);
extern List *textToQualifiedNameList(text *textval);
extern bool SplitIdentifierString(char *rawstring, char separator,
					  List **namelist);
extern Datum replace_text(PG_FUNCTION_ARGS);
extern text *replace_text_regexp(text *src_text, void *regexp,
					text *replace_text, bool glob);
extern Datum split_text(PG_FUNCTION_ARGS);
extern Datum text_to_array(PG_FUNCTION_ARGS);
extern Datum array_to_text(PG_FUNCTION_ARGS);
extern Datum to_hex32(PG_FUNCTION_ARGS);
extern Datum to_hex64(PG_FUNCTION_ARGS);
extern Datum md5_text(PG_FUNCTION_ARGS);
extern Datum md5_bytea(PG_FUNCTION_ARGS);

extern Datum unknownin(PG_FUNCTION_ARGS);
extern Datum unknownout(PG_FUNCTION_ARGS);
extern Datum unknownrecv(PG_FUNCTION_ARGS);
extern Datum unknownsend(PG_FUNCTION_ARGS);

extern Datum byteain(PG_FUNCTION_ARGS);
extern Datum byteaout(PG_FUNCTION_ARGS);
extern Datum bytearecv(PG_FUNCTION_ARGS);
extern Datum byteasend(PG_FUNCTION_ARGS);
extern Datum byteaoctetlen(PG_FUNCTION_ARGS);
extern Datum byteaGetByte(PG_FUNCTION_ARGS);
extern Datum byteaGetBit(PG_FUNCTION_ARGS);
extern Datum byteaSetByte(PG_FUNCTION_ARGS);
extern Datum byteaSetBit(PG_FUNCTION_ARGS);
extern Datum binary_encode(PG_FUNCTION_ARGS);
extern Datum binary_decode(PG_FUNCTION_ARGS);
extern Datum byteaeq(PG_FUNCTION_ARGS);
extern Datum byteane(PG_FUNCTION_ARGS);
extern Datum bytealt(PG_FUNCTION_ARGS);
extern Datum byteale(PG_FUNCTION_ARGS);
extern Datum byteagt(PG_FUNCTION_ARGS);
extern Datum byteage(PG_FUNCTION_ARGS);
extern Datum byteacmp(PG_FUNCTION_ARGS);
extern Datum byteacat(PG_FUNCTION_ARGS);
extern Datum byteapos(PG_FUNCTION_ARGS);
extern Datum bytea_substr(PG_FUNCTION_ARGS);
extern Datum bytea_substr_no_len(PG_FUNCTION_ARGS);
extern Datum pg_column_size(PG_FUNCTION_ARGS);

extern Datum string_agg_transfn(PG_FUNCTION_ARGS);
extern Datum string_agg_delim_transfn(PG_FUNCTION_ARGS);
extern Datum string_agg_finalfn(PG_FUNCTION_ARGS);

/* version.c */
extern Datum pgsql_version(PG_FUNCTION_ARGS);

/* xid.c */
extern Datum xidin(PG_FUNCTION_ARGS);
extern Datum xidout(PG_FUNCTION_ARGS);
extern Datum xidrecv(PG_FUNCTION_ARGS);
extern Datum xidsend(PG_FUNCTION_ARGS);
extern Datum xideq(PG_FUNCTION_ARGS);
extern Datum xidne(PG_FUNCTION_ARGS);
extern Datum xidlt(PG_FUNCTION_ARGS);
extern Datum xidgt(PG_FUNCTION_ARGS);
extern Datum xidle(PG_FUNCTION_ARGS);
extern Datum xidge(PG_FUNCTION_ARGS);
extern Datum btxidcmp(PG_FUNCTION_ARGS);
extern Datum xid_age(PG_FUNCTION_ARGS);
extern Datum cidin(PG_FUNCTION_ARGS);
extern Datum cidout(PG_FUNCTION_ARGS);
extern Datum cidrecv(PG_FUNCTION_ARGS);
extern Datum cidsend(PG_FUNCTION_ARGS);
extern Datum cideq(PG_FUNCTION_ARGS);

/* like.c */
extern Datum namelike(PG_FUNCTION_ARGS);
extern Datum namenlike(PG_FUNCTION_ARGS);
extern Datum nameiclike(PG_FUNCTION_ARGS);
extern Datum nameicnlike(PG_FUNCTION_ARGS);
extern Datum textlike(PG_FUNCTION_ARGS);
extern Datum textnlike(PG_FUNCTION_ARGS);
extern Datum texticlike(PG_FUNCTION_ARGS);
extern Datum texticnlike(PG_FUNCTION_ARGS);
extern Datum bytealike(PG_FUNCTION_ARGS);
extern Datum byteanlike(PG_FUNCTION_ARGS);
extern Datum like_escape(PG_FUNCTION_ARGS);
extern Datum like_escape_bytea(PG_FUNCTION_ARGS);

/* oracle_compat.c */
extern Datum lower(PG_FUNCTION_ARGS);
extern Datum upper(PG_FUNCTION_ARGS);
extern Datum initcap(PG_FUNCTION_ARGS);
extern Datum lpad(PG_FUNCTION_ARGS);
extern Datum rpad(PG_FUNCTION_ARGS);
extern Datum btrim(PG_FUNCTION_ARGS);
extern Datum btrim1(PG_FUNCTION_ARGS);
extern Datum byteatrim(PG_FUNCTION_ARGS);
extern Datum ltrim(PG_FUNCTION_ARGS);
extern Datum ltrim1(PG_FUNCTION_ARGS);
extern Datum rtrim(PG_FUNCTION_ARGS);
extern Datum rtrim1(PG_FUNCTION_ARGS);
extern Datum translate(PG_FUNCTION_ARGS);
extern Datum chr (PG_FUNCTION_ARGS);
extern Datum repeat(PG_FUNCTION_ARGS);
extern Datum ascii(PG_FUNCTION_ARGS);

/* inet_net_ntop.c */
extern char *inet_net_ntop(int af, const void *src, int bits,
			  char *dst, size_t size);
extern char *inet_cidr_ntop(int af, const void *src, int bits,
			   char *dst, size_t size);

/* inet_net_pton.c */
extern int inet_net_pton(int af, const char *src,
			  void *dst, size_t size);

/* network.c */
extern Datum inet_in(PG_FUNCTION_ARGS);
extern Datum inet_out(PG_FUNCTION_ARGS);
extern Datum inet_recv(PG_FUNCTION_ARGS);
extern Datum inet_send(PG_FUNCTION_ARGS);
extern Datum cidr_in(PG_FUNCTION_ARGS);
extern Datum cidr_out(PG_FUNCTION_ARGS);
extern Datum cidr_recv(PG_FUNCTION_ARGS);
extern Datum cidr_send(PG_FUNCTION_ARGS);
extern Datum network_cmp(PG_FUNCTION_ARGS);
extern Datum network_lt(PG_FUNCTION_ARGS);
extern Datum network_le(PG_FUNCTION_ARGS);
extern Datum network_eq(PG_FUNCTION_ARGS);
extern Datum network_ge(PG_FUNCTION_ARGS);
extern Datum network_gt(PG_FUNCTION_ARGS);
extern Datum network_ne(PG_FUNCTION_ARGS);
extern Datum hashinet(PG_FUNCTION_ARGS);
extern Datum network_sub(PG_FUNCTION_ARGS);
extern Datum network_subeq(PG_FUNCTION_ARGS);
extern Datum network_sup(PG_FUNCTION_ARGS);
extern Datum network_supeq(PG_FUNCTION_ARGS);
extern Datum network_network(PG_FUNCTION_ARGS);
extern Datum network_netmask(PG_FUNCTION_ARGS);
extern Datum network_hostmask(PG_FUNCTION_ARGS);
extern Datum network_masklen(PG_FUNCTION_ARGS);
extern Datum network_family(PG_FUNCTION_ARGS);
extern Datum network_broadcast(PG_FUNCTION_ARGS);
extern Datum network_host(PG_FUNCTION_ARGS);
extern Datum network_show(PG_FUNCTION_ARGS);
extern Datum inet_abbrev(PG_FUNCTION_ARGS);
extern Datum cidr_abbrev(PG_FUNCTION_ARGS);
extern double convert_network_to_scalar(Datum value, Oid typid);
extern Datum text_cidr(PG_FUNCTION_ARGS);
extern Datum text_inet(PG_FUNCTION_ARGS);
extern Datum inet_to_cidr(PG_FUNCTION_ARGS);
extern Datum inet_set_masklen(PG_FUNCTION_ARGS);
extern Datum cidr_set_masklen(PG_FUNCTION_ARGS);
extern Datum network_scan_first(Datum in);
extern Datum network_scan_last(Datum in);
extern Datum inet_client_addr(PG_FUNCTION_ARGS);
extern Datum inet_client_port(PG_FUNCTION_ARGS);
extern Datum inet_server_addr(PG_FUNCTION_ARGS);
extern Datum inet_server_port(PG_FUNCTION_ARGS);
extern Datum inetnot(PG_FUNCTION_ARGS);
extern Datum inetand(PG_FUNCTION_ARGS);
extern Datum inetor(PG_FUNCTION_ARGS);
extern Datum inetpl(PG_FUNCTION_ARGS);
extern Datum inetmi_int8(PG_FUNCTION_ARGS);
extern Datum inetmi(PG_FUNCTION_ARGS);
extern void clean_ipv6_addr(int addr_family, char *addr);

/* mac.c */
extern Datum macaddr_in(PG_FUNCTION_ARGS);
extern Datum macaddr_out(PG_FUNCTION_ARGS);
extern Datum macaddr_recv(PG_FUNCTION_ARGS);
extern Datum macaddr_send(PG_FUNCTION_ARGS);
extern Datum macaddr_cmp(PG_FUNCTION_ARGS);
extern Datum macaddr_lt(PG_FUNCTION_ARGS);
extern Datum macaddr_le(PG_FUNCTION_ARGS);
extern Datum macaddr_eq(PG_FUNCTION_ARGS);
extern Datum macaddr_ge(PG_FUNCTION_ARGS);
extern Datum macaddr_gt(PG_FUNCTION_ARGS);
extern Datum macaddr_ne(PG_FUNCTION_ARGS);
extern Datum macaddr_trunc(PG_FUNCTION_ARGS);
extern Datum macaddr_text(PG_FUNCTION_ARGS);
extern Datum text_macaddr(PG_FUNCTION_ARGS);
extern Datum hashmacaddr(PG_FUNCTION_ARGS);

/* numeric.c */
extern Datum numeric_in(PG_FUNCTION_ARGS);
extern Datum numeric_out(PG_FUNCTION_ARGS);
extern Datum numeric_recv(PG_FUNCTION_ARGS);
extern Datum numeric_send(PG_FUNCTION_ARGS);
extern Datum numerictypmodin(PG_FUNCTION_ARGS);
extern Datum numerictypmodout(PG_FUNCTION_ARGS);
extern Datum numeric (PG_FUNCTION_ARGS);
extern Datum numeric_abs(PG_FUNCTION_ARGS);
extern Datum numeric_uminus(PG_FUNCTION_ARGS);
extern Datum numeric_uplus(PG_FUNCTION_ARGS);
extern Datum numeric_sign(PG_FUNCTION_ARGS);
extern Datum numeric_round(PG_FUNCTION_ARGS);
extern Datum numeric_trunc(PG_FUNCTION_ARGS);
extern Datum numeric_ceil(PG_FUNCTION_ARGS);
extern Datum numeric_floor(PG_FUNCTION_ARGS);
extern Datum numeric_cmp(PG_FUNCTION_ARGS);
extern Datum numeric_eq(PG_FUNCTION_ARGS);
extern Datum numeric_ne(PG_FUNCTION_ARGS);
extern Datum numeric_gt(PG_FUNCTION_ARGS);
extern Datum numeric_ge(PG_FUNCTION_ARGS);
extern Datum numeric_lt(PG_FUNCTION_ARGS);
extern Datum numeric_le(PG_FUNCTION_ARGS);
extern Datum numeric_add(PG_FUNCTION_ARGS);
extern Datum numeric_sub(PG_FUNCTION_ARGS);
extern Datum numeric_mul(PG_FUNCTION_ARGS);
extern Datum numeric_div(PG_FUNCTION_ARGS);
extern Datum numeric_mod(PG_FUNCTION_ARGS);
extern Datum numeric_inc(PG_FUNCTION_ARGS);
extern Datum numeric_dec(PG_FUNCTION_ARGS);
extern Datum numeric_smaller(PG_FUNCTION_ARGS);
extern Datum numeric_larger(PG_FUNCTION_ARGS);
extern Datum numeric_fac(PG_FUNCTION_ARGS);
extern Datum numeric_sqrt(PG_FUNCTION_ARGS);
extern Datum numeric_exp(PG_FUNCTION_ARGS);
extern Datum numeric_ln(PG_FUNCTION_ARGS);
extern Datum numeric_log(PG_FUNCTION_ARGS);
extern Datum numeric_power(PG_FUNCTION_ARGS);
extern Datum int4_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_int4(PG_FUNCTION_ARGS);
extern Datum int8_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_int8(PG_FUNCTION_ARGS);
extern Datum int2_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_int2(PG_FUNCTION_ARGS);
extern Datum float8_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_float8(PG_FUNCTION_ARGS);
extern Datum numeric_float8_no_overflow(PG_FUNCTION_ARGS);
extern Datum float4_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_float4(PG_FUNCTION_ARGS);
extern Datum text_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_text(PG_FUNCTION_ARGS);
extern Datum numeric_accum(PG_FUNCTION_ARGS);
extern Datum numeric_decum(PG_FUNCTION_ARGS);
extern Datum int2_accum(PG_FUNCTION_ARGS);
extern Datum int4_accum(PG_FUNCTION_ARGS);
extern Datum int8_accum(PG_FUNCTION_ARGS);
extern Datum int2_decum(PG_FUNCTION_ARGS);
extern Datum int4_decum(PG_FUNCTION_ARGS);
extern Datum int8_decum(PG_FUNCTION_ARGS);
extern Datum numeric_avg(PG_FUNCTION_ARGS);
extern Datum numeric_var_pop(PG_FUNCTION_ARGS);
extern Datum numeric_var_samp(PG_FUNCTION_ARGS);
extern Datum numeric_stddev_pop(PG_FUNCTION_ARGS);
extern Datum numeric_stddev_samp(PG_FUNCTION_ARGS);
extern Datum int2_sum(PG_FUNCTION_ARGS);
extern Datum int4_sum(PG_FUNCTION_ARGS);
extern Datum int8_sum(PG_FUNCTION_ARGS);
extern Datum int2_invsum(PG_FUNCTION_ARGS);
extern Datum int4_invsum(PG_FUNCTION_ARGS);
extern Datum int8_invsum(PG_FUNCTION_ARGS);
extern Datum int2_avg_accum(PG_FUNCTION_ARGS);
extern Datum int4_avg_accum(PG_FUNCTION_ARGS);
extern Datum int8_avg_accum(PG_FUNCTION_ARGS);
extern Datum float4_avg_accum(PG_FUNCTION_ARGS);
extern Datum float8_avg_accum(PG_FUNCTION_ARGS);
extern Datum numeric_avg_accum(PG_FUNCTION_ARGS);
extern Datum int2_avg_decum(PG_FUNCTION_ARGS);
extern Datum int4_avg_decum(PG_FUNCTION_ARGS);
extern Datum int8_avg_decum(PG_FUNCTION_ARGS);
extern Datum float4_avg_decum(PG_FUNCTION_ARGS);
extern Datum float8_avg_decum(PG_FUNCTION_ARGS);
extern Datum numeric_avg_decum(PG_FUNCTION_ARGS);
extern Datum int8_avg(PG_FUNCTION_ARGS);
extern Datum float8_avg(PG_FUNCTION_ARGS);
extern Datum width_bucket_numeric(PG_FUNCTION_ARGS);
extern Datum hash_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_amalg(PG_FUNCTION_ARGS); /* MPP */
extern Datum int8_avg_amalg(PG_FUNCTION_ARGS); /* MPP */
extern Datum float8_avg_amalg(PG_FUNCTION_ARGS); /* MPP */
extern Datum numeric_avg_amalg(PG_FUNCTION_ARGS); /* MPP */
extern Datum numeric_demalg(PG_FUNCTION_ARGS); /* MPP */ 
extern Datum int8_avg_demalg(PG_FUNCTION_ARGS); /* MPP */
extern Datum float8_avg_demalg(PG_FUNCTION_ARGS); /* MPP */
extern Datum numeric_avg_demalg(PG_FUNCTION_ARGS); /* MPP */

/* ri_triggers.c */
extern Datum RI_FKey_check_ins(PG_FUNCTION_ARGS);
extern Datum RI_FKey_check_upd(PG_FUNCTION_ARGS);
extern Datum RI_FKey_noaction_del(PG_FUNCTION_ARGS);
extern Datum RI_FKey_noaction_upd(PG_FUNCTION_ARGS);
extern Datum RI_FKey_cascade_del(PG_FUNCTION_ARGS);
extern Datum RI_FKey_cascade_upd(PG_FUNCTION_ARGS);
extern Datum RI_FKey_restrict_del(PG_FUNCTION_ARGS);
extern Datum RI_FKey_restrict_upd(PG_FUNCTION_ARGS);
extern Datum RI_FKey_setnull_del(PG_FUNCTION_ARGS);
extern Datum RI_FKey_setnull_upd(PG_FUNCTION_ARGS);
extern Datum RI_FKey_setdefault_del(PG_FUNCTION_ARGS);
extern Datum RI_FKey_setdefault_upd(PG_FUNCTION_ARGS);

/* encoding support functions */
extern Datum getdatabaseencoding(PG_FUNCTION_ARGS);
extern Datum database_character_set(PG_FUNCTION_ARGS);
extern Datum pg_client_encoding(PG_FUNCTION_ARGS);
extern Datum PG_encoding_to_char(PG_FUNCTION_ARGS);
extern Datum PG_char_to_encoding(PG_FUNCTION_ARGS);
extern Datum PG_character_set_name(PG_FUNCTION_ARGS);
extern Datum PG_character_set_id(PG_FUNCTION_ARGS);
extern Datum pg_convert(PG_FUNCTION_ARGS);
extern Datum pg_convert2(PG_FUNCTION_ARGS);
extern Datum pg_convert_to(PG_FUNCTION_ARGS);
extern Datum pg_convert_from(PG_FUNCTION_ARGS);
extern Datum length_in_encoding(PG_FUNCTION_ARGS);

/* format_type.c */
extern Datum format_type(PG_FUNCTION_ARGS);
extern char *format_type_be(Oid type_oid);
extern char *format_type_with_typemod(Oid type_oid, int32 typemod);
extern Datum oidvectortypes(PG_FUNCTION_ARGS);
extern int32 type_maximum_size(Oid type_oid, int32 typemod);

/* quote.c */
extern Datum quote_ident(PG_FUNCTION_ARGS);
extern Datum quote_literal(PG_FUNCTION_ARGS);
extern Datum quote_nullable(PG_FUNCTION_ARGS);

/* guc.c */
extern Datum show_config_by_name(PG_FUNCTION_ARGS);
extern Datum set_config_by_name(PG_FUNCTION_ARGS);
extern Datum show_all_settings(PG_FUNCTION_ARGS);

/* lockfuncs.c */
extern Datum pg_lock_status(PG_FUNCTION_ARGS);
extern Datum pg_advisory_lock_int8(PG_FUNCTION_ARGS);
extern Datum pg_advisory_lock_shared_int8(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_lock_int8(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_lock_shared_int8(PG_FUNCTION_ARGS);
extern Datum pg_advisory_unlock_int8(PG_FUNCTION_ARGS);
extern Datum pg_advisory_unlock_shared_int8(PG_FUNCTION_ARGS);
extern Datum pg_advisory_lock_int4(PG_FUNCTION_ARGS);
extern Datum pg_advisory_lock_shared_int4(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_lock_int4(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_lock_shared_int4(PG_FUNCTION_ARGS);
extern Datum pg_advisory_unlock_int4(PG_FUNCTION_ARGS);
extern Datum pg_advisory_unlock_shared_int4(PG_FUNCTION_ARGS);
extern Datum pg_advisory_unlock_all(PG_FUNCTION_ARGS);

/* access/transam/twophase.c */
extern Datum pg_prepared_xact(PG_FUNCTION_ARGS);

/* catalog/pg_conversion.c */
extern Datum pg_convert_using(PG_FUNCTION_ARGS);

/* commands/prepare.c */
extern Datum pg_prepared_statement(PG_FUNCTION_ARGS);

/* utils/mmgr/portalmem.c */
extern Datum pg_cursor(PG_FUNCTION_ARGS);

/* tqual.c */
extern Datum mpp_global_xid_map(PG_FUNCTION_ARGS);

/* utils/resscheduler/resqueue.c */
extern Datum pg_resqueue_status(PG_FUNCTION_ARGS);
extern Datum pg_resqueue_status_kv(PG_FUNCTION_ARGS);
extern Datum pg_explain_resource_distribution(PG_FUNCTION_ARGS);
extern Datum pg_play_resource_action(PG_FUNCTION_ARGS);
extern Datum dump_resource_manager_status(PG_FUNCTION_ARGS);

/* utils/adt/matrix.c */
extern Datum matrix_transpose(PG_FUNCTION_ARGS);
extern Datum matrix_multiply(PG_FUNCTION_ARGS);
extern Datum matrix_add(PG_FUNCTION_ARGS);
extern Datum int8_matrix_smultiply(PG_FUNCTION_ARGS);
extern Datum float8_matrix_smultiply(PG_FUNCTION_ARGS);

/* utils/adt/pivot.c */
Datum int4_pivot_accum(PG_FUNCTION_ARGS);
Datum int8_pivot_accum(PG_FUNCTION_ARGS);
Datum float8_pivot_accum(PG_FUNCTION_ARGS);
Datum text_unpivot(PG_FUNCTION_ARGS);
Datum unnest(PG_FUNCTION_ARGS);

/* cdb/cdbpersistentbuild.c */
Datum gp_persistent_build_db(PG_FUNCTION_ARGS);
Datum gp_persistent_build_all(PG_FUNCTION_ARGS);
Datum gp_persistent_reset_all(PG_FUNCTION_ARGS);
Datum gp_persistent_repair_delete(PG_FUNCTION_ARGS);
Datum gp_persistent_set_relation_bufpool_kind_all(PG_FUNCTION_ARGS);


/* utils/error/elog.c */
extern Datum gp_elog(PG_FUNCTION_ARGS);

/* utils/fmgr/deprecated.c */
extern Datum gp_deprecated(PG_FUNCTION_ARGS);

/* utils/gp/segadmin.c */
extern Datum gp_add_master_standby(PG_FUNCTION_ARGS);
extern Datum gp_remove_master_standby(PG_FUNCTION_ARGS);
extern Datum gp_activate_standby(PG_FUNCTION_ARGS);

extern Datum gp_add_segment_mirror(PG_FUNCTION_ARGS);
extern Datum gp_remove_segment_mirror(PG_FUNCTION_ARGS);
extern Datum gp_add_segment(PG_FUNCTION_ARGS);
extern Datum gp_remove_segment(PG_FUNCTION_ARGS);

extern Datum gp_prep_new_segment(PG_FUNCTION_ARGS);

extern Datum gp_add_segment_persistent_entries(PG_FUNCTION_ARGS);
extern Datum gp_remove_segment_persistent_entries(PG_FUNCTION_ARGS);

/* utils/gp/persistentutil.c */
extern Datum gp_add_persistent_filespace_node_entry(PG_FUNCTION_ARGS);
extern Datum gp_add_persistent_tablespace_node_entry(PG_FUNCTION_ARGS);
extern Datum gp_add_persistent_database_node_entry(PG_FUNCTION_ARGS);
extern Datum gp_add_persistent_relation_node_entry(PG_FUNCTION_ARGS);
extern Datum gp_add_global_sequence_entry(PG_FUNCTION_ARGS);
extern Datum gp_add_relation_node_entry(PG_FUNCTION_ARGS);

extern Datum gp_update_persistent_filespace_node_entry(PG_FUNCTION_ARGS);
extern Datum gp_update_persistent_tablespace_node_entry(PG_FUNCTION_ARGS);
extern Datum gp_update_persistent_database_node_entry(PG_FUNCTION_ARGS);
extern Datum gp_update_persistent_relation_node_entry(PG_FUNCTION_ARGS);
extern Datum gp_update_global_sequence_entry(PG_FUNCTION_ARGS);
extern Datum gp_update_relation_node_entry(PG_FUNCTION_ARGS);

extern Datum gp_delete_persistent_filespace_node_entry(PG_FUNCTION_ARGS);
extern Datum gp_delete_persistent_tablespace_node_entry(PG_FUNCTION_ARGS);
extern Datum gp_delete_persistent_database_node_entry(PG_FUNCTION_ARGS);
extern Datum gp_delete_persistent_relation_node_entry(PG_FUNCTION_ARGS);
extern Datum gp_delete_global_sequence_entry(PG_FUNCTION_ARGS);
extern Datum gp_delete_relation_node_entry(PG_FUNCTION_ARGS);

extern Datum gp_persistent_relation_node_check(PG_FUNCTION_ARGS);

extern Datum pg_partition_oid_transfn(PG_FUNCTION_ARGS);
extern Datum pg_partition_oid_finalfn(PG_FUNCTION_ARGS);

/* storage/compress.c */
extern Datum quicklz_constructor(PG_FUNCTION_ARGS);
extern Datum quicklz_destructor(PG_FUNCTION_ARGS);
extern Datum quicklz_compress(PG_FUNCTION_ARGS);
extern Datum quicklz_decompress(PG_FUNCTION_ARGS);
extern Datum quicklz_validator(PG_FUNCTION_ARGS);

extern Datum zlib_constructor(PG_FUNCTION_ARGS);
extern Datum zlib_destructor(PG_FUNCTION_ARGS);
extern Datum zlib_compress(PG_FUNCTION_ARGS);
extern Datum zlib_decompress(PG_FUNCTION_ARGS);
extern Datum zlib_validator(PG_FUNCTION_ARGS);

extern Datum rle_type_constructor(PG_FUNCTION_ARGS);
extern Datum rle_type_destructor(PG_FUNCTION_ARGS);
extern Datum rle_type_compress(PG_FUNCTION_ARGS);
extern Datum rle_type_decompress(PG_FUNCTION_ARGS);
extern Datum rle_type_validator(PG_FUNCTION_ARGS);

extern Datum delta_constructor(PG_FUNCTION_ARGS);
extern Datum delta_destructor(PG_FUNCTION_ARGS);
extern Datum delta_compress(PG_FUNCTION_ARGS);
extern Datum delta_decompress(PG_FUNCTION_ARGS);
extern Datum delta_validator(PG_FUNCTION_ARGS);

extern Datum dummy_compression_constructor(PG_FUNCTION_ARGS);
extern Datum dummy_compression_destructor(PG_FUNCTION_ARGS);
extern Datum dummy_compression_compress(PG_FUNCTION_ARGS);
extern Datum dummy_compression_decompress(PG_FUNCTION_ARGS);
extern Datum dummy_compression_validator(PG_FUNCTION_ARGS);

extern Datum gp_compressor(PG_FUNCTION_ARGS);
extern Datum gp_decompressor(PG_FUNCTION_ARGS);
extern Datum test_quicklz_compression(PG_FUNCTION_ARGS);

/* percentile.c */
extern Datum percentile_cont_trans(PG_FUNCTION_ARGS);
extern Datum percentile_disc_trans(PG_FUNCTION_ARGS);

/* gp_partition_funtions.c */
extern Datum gp_partition_propagation(PG_FUNCTION_ARGS);
extern void dumpDynamicTableScanPidIndex(int index);
extern Datum gp_partition_selection(PG_FUNCTION_ARGS);
extern Datum gp_partition_expansion(PG_FUNCTION_ARGS);
extern Datum gp_partition_inverse(PG_FUNCTION_ARGS);

/* XForms */
extern Datum disable_xform(PG_FUNCTION_ARGS);
extern Datum enable_xform(PG_FUNCTION_ARGS);

/* Optimizer's version */
extern Datum gp_opt_version(PG_FUNCTION_ARGS);

/* utils/workfile_manager/workfile_mgr_test.c */
extern Datum gp_workfile_mgr_test_harness(PG_FUNCTION_ARGS);

/* cdb/cdbmetadatacache.c */
extern Datum gp_metadata_cache_clear(PG_FUNCTION_ARGS);
extern Datum gp_metadata_cache_current_num(PG_FUNCTION_ARGS);
extern Datum gp_metadata_cache_current_block_num(PG_FUNCTION_ARGS);
extern Datum gp_metadata_cache_exists(PG_FUNCTION_ARGS);
extern Datum gp_metadata_cache_info(PG_FUNCTION_ARGS);
extern Datum gp_metadata_cache_put_entry_for_test(PG_FUNCTION_ARGS);

#endif   /* BUILTINS_H */
