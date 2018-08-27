/*-------------------------------------------------------------------------
 *
 * lsyscache.h
 *	  Convenience routines for common queries in the system catalog cache.
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/utils/lsyscache.h,v 1.133 2010/04/24 16:20:32 sriggs Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef LSYSCACHE_H
#define LSYSCACHE_H

#include "access/attnum.h"
#include "access/htup.h"
#include "catalog/gp_policy.h"
#include "nodes/pg_list.h"
#include "utils/relcache.h"

/* I/O function selector for get_type_io_data */
typedef enum IOFuncSelector
{
	IOFunc_input,
	IOFunc_output,
	IOFunc_receive,
	IOFunc_send
} IOFuncSelector;

/* comparison types */
typedef enum CmpType
{
	CmptEq,		// equality
	CmptNEq,	// inequality
	CmptLT, 	// less than
	CmptLEq,	// less or equal to
	CmptGT,		// greater than
	CmptGEq, 	// greater or equal to
	CmptOther	// other operator
} CmpType;

extern bool op_in_opclass(Oid opno, Oid opclass);
extern int	get_op_opclass_strategy(Oid opno, Oid opclass);
extern void get_op_opclass_properties(Oid opno, Oid opclass,
						  int *strategy, Oid *subtype,
						  bool *recheck);
extern Oid	get_opclass_member(Oid opclass, Oid subtype, int16 strategy);
extern Oid	get_op_hash_function(Oid opno);
extern void get_op_btree_interpretation(Oid opno,
							List **opclasses, List **opstrats);
extern Oid	get_opclass_proc(Oid opclass, Oid subtype, int16 procnum);
extern char *get_attname(Oid relid, AttrNumber attnum);
extern char *get_relid_attribute_name(Oid relid, AttrNumber attnum);
extern AttrNumber get_attnum(Oid relid, const char *attname);
extern Oid	get_atttype(Oid relid, AttrNumber attnum);
extern int32 get_atttypmod(Oid relid, AttrNumber attnum);
extern void get_atttypetypmod(Oid relid, AttrNumber attnum,
				  Oid *typid, int32 *typmod);
extern bool opclass_is_btree(Oid opclass);
extern bool opclass_is_hash(Oid opclass);
extern bool opclass_is_default(Oid opclass);
extern Oid	get_opclass_input_type(Oid opclass);
extern RegProcedure get_opcode(Oid opno);
extern char *get_opname(Oid opno);
extern void op_input_types(Oid opno, Oid *lefttype, Oid *righttype);
extern bool op_mergejoinable(Oid opno, Oid *leftOp, Oid *rightOp);
extern void op_mergejoin_crossops(Oid opno, Oid *ltop, Oid *gtop,
					  RegProcedure *ltproc, RegProcedure *gtproc);
extern bool op_hashjoinable(Oid opno);
extern bool op_strict(Oid opno);
extern char op_volatile(Oid opno);
extern Oid	get_commutator(Oid opno);
extern Oid	get_negator(Oid opno);
extern RegProcedure get_oprrest(Oid opno);
extern RegProcedure get_oprjoin(Oid opno);
extern char *get_trigger_name(Oid triggerid);
extern Oid get_trigger_relid(Oid triggerid);
extern Oid get_trigger_funcid(Oid triggerid);
extern int32 get_trigger_type(Oid triggerid);
extern bool trigger_enabled(Oid triggerid);
extern char *get_func_name(Oid funcid);
extern Oid	get_func_namespace(Oid funcid);
extern Oid	get_func_rettype(Oid funcid);
extern void pfree_ptr_array(char **ptrarray, int nelements);
extern List *get_func_output_arg_types(Oid funcid);
extern List *get_func_arg_types(Oid funcid);
extern int	get_func_nargs(Oid funcid);
extern Oid	get_func_signature(Oid funcid, Oid **argtypes, int *nargs);
extern bool get_func_retset(Oid funcid);
extern bool func_strict(Oid funcid);
extern char func_volatile(Oid funcid);
extern char func_data_access(Oid funcid);
extern Oid get_agg_transtype(Oid aggid);
extern bool is_agg_ordered(Oid aggid);
extern bool has_agg_prelimfunc(Oid aggid);
extern bool agg_has_prelim_or_invprelim_func(Oid aggid);
extern Oid	get_relname_relid(const char *relname, Oid relnamespace);
extern int get_relnatts(Oid relid);
extern char *get_rel_name(Oid relid);
extern char *get_rel_name_partition(Oid relid);
extern Oid	get_rel_namespace(Oid relid);
extern Oid	get_rel_type_id(Oid relid);
extern char get_rel_relkind(Oid relid);
extern float4 get_rel_reltuples(Oid relid);
extern char get_rel_relstorage(Oid relid);
extern Oid	get_rel_tablespace(Oid relid);
extern char *get_type_name(Oid typid);
extern bool get_typisdefined(Oid typid);
extern int16 get_typlen(Oid typid);
extern bool get_typbyval(Oid typid);
extern void get_typlenbyval(Oid typid, int16 *typlen, bool *typbyval);
extern void get_typlenbyvalalign(Oid typid, int16 *typlen, bool *typbyval,
					 char *typalign);
extern Oid	getTypeIOParam(HeapTuple typeTuple);
extern void get_type_io_data(Oid typid,
				 IOFuncSelector which_func,
				 int16 *typlen,
				 bool *typbyval,
				 char *typalign,
				 char *typdelim,
				 Oid *typioparam,
				 Oid *func);
extern char get_typstorage(Oid typid);
extern Node *get_typdefault(Oid typid);
extern char get_typtype(Oid typid);
extern bool type_is_rowtype(Oid typid);
extern bool type_is_basetype(Oid typid);
extern Oid	get_typ_typrelid(Oid typid);
extern Oid	get_element_type(Oid typid);
extern Oid	get_array_type(Oid typid);
extern Oid get_base_element_type(Oid typid);
extern void getTypeInputInfo(Oid type, Oid *typInput, Oid *typIOParam);
extern void getTypeOutputInfo(Oid type, Oid *typOutput, bool *typIsVarlena);
extern void getTypeBinaryInputInfo(Oid type, Oid *typReceive, Oid *typIOParam);
extern void getTypeBinaryOutputInfo(Oid type, Oid *typSend, bool *typIsVarlena);
extern Oid	getBaseType(Oid typid);
extern Oid	getBaseTypeAndTypmod(Oid typid, int32 *typmod);
extern int32 get_typavgwidth(Oid typid, int32 typmod);
extern int32 get_attavgwidth(Oid relid, AttrNumber attnum);
extern float4 get_attdistinct(Oid relid, AttrNumber attnum);
extern HeapTuple get_att_stats(Oid relid, AttrNumber attnum);
extern bool get_attstatsslot(HeapTuple statstuple,
				 Oid atttype, int32 atttypmod,
				 int reqkind, Oid reqop,
				 Datum **values, int *nvalues,
				 float4 **numbers, int *nnumbers);
extern void free_attstatsslot(Oid atttype,
				  Datum *values, int nvalues,
				  float4 *numbers, int nnumbers);
extern char *get_namespace_name(Oid nspid);
extern Oid get_namespace_oid(const char* npname);
extern Oid	get_roleid(const char *rolname);
extern char *get_rolname(Oid roleid);
extern char get_relation_storage_type(Oid relid);
extern Oid	get_roleid_checked(const char *rolname);

extern List *relation_oids(void);
extern List *operator_oids(void);
extern List *function_oids(void);
extern bool relation_exists(Oid oid);
extern bool index_exists(Oid oid);
extern bool type_exists(Oid oid);
extern bool function_exists(Oid oid);
extern bool operator_exists(Oid oid);
extern bool aggregate_exists(Oid oid);
extern Oid get_aggregate(const char *aggname, Oid oidType);
extern List *get_relation_keys(Oid relid);
extern bool attname_exists(Oid relid, const char *attname);
extern bool trigger_exists(Oid oid);

extern bool check_constraint_exists(Oid oidCheckconstraint);
extern List *get_check_constraint_oids(Oid relid);
extern char *get_check_constraint_name(Oid oidCheckconstraint);
extern Node *get_check_constraint_expr_tree(Oid oidCheckconstraint);
Oid get_check_constraint_relid(Oid oidCheckconstraint);

extern bool has_subclass_fast(Oid relationId);
extern bool has_subclass(Oid relationId);
extern bool has_parquet_children(Oid relationId);
extern GpPolicy *relation_policy(Relation rel);
extern bool child_distribution_mismatch(Relation rel);
extern bool child_triggers(Oid relationId, int32 triggerType);

extern bool get_cast_func(Oid oidSrc, Oid oidDest, bool *is_binary_coercible, Oid *oidCastFunc);

extern Oid get_comparison_operator(Oid oidLeft, Oid oidRight, CmpType cmpt);
extern CmpType get_comparison_type(Oid oidOp, Oid oidLeft, Oid oidRight);
extern List *find_all_inheritors(Oid parentrel);

extern List *get_operator_opclasses(Oid opno);
extern List *get_index_opclasses(Oid oidIndex);

#define is_array_type(typid)  (get_element_type(typid) != InvalidOid)

#define TypeIsToastable(typid)	(get_typstorage(typid) != 'p')

#endif   /* LSYSCACHE_H */
