/*-------------------------------------------------------------------------
 *
 * readfuncs.c
 *	  Binary Reader functions for Postgres tree nodes.
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * These routines must be exactly the inverse of the routines in
 * outfast.c.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/filesplit.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/readfuncs.h"
#include "nodes/relation.h"
#include "catalog/pg_class.h"
#include "cdb/cdbgang.h"


/*
 * Macros to simplify reading of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.
 */


#define READ_LOCALS(nodeTypeName) \
	nodeTypeName *local_node = makeNode(nodeTypeName)

/* Allocate non-node variable */
#define ALLOCATE_LOCAL(local, typeName, size) \
	local = (typeName *)palloc0(sizeof(typeName) * size)

/* Read an integer field  */
#define READ_INT_FIELD(fldname) \
	memcpy(&local_node->fldname, *str, sizeof(int));  (*str)+=sizeof(int)

/* Read an unsigned integer field) */
#define READ_UINT_FIELD(fldname) \
	memcpy(&local_node->fldname, *str, sizeof(int)); (*str)+=sizeof(int)

/* Read an uint64 field) */
#define READ_UINT64_FIELD(fldname) \
	memcpy(&local_node->fldname, *str, sizeof(uint64)); (*str)+=sizeof(uint64)

/* Read an int64 field) */
#define READ_INT64_FIELD(fldname) \
	memcpy(&local_node->fldname, *str, sizeof(int64)); (*str)+=sizeof(int64)

/* Read an OID field (don't hard-wire assumption that OID is same as uint) */
#define READ_OID_FIELD(fldname) \
	memcpy(&local_node->fldname, *str, sizeof(Oid)); (*str)+=sizeof(Oid)

/* Read a long-integer field  */
#define READ_LONG_FIELD(fldname) \
	memcpy(&local_node->fldname, *str, sizeof(long)); (*str)+=sizeof(long)

/* Read a char field (ie, one ascii character) */
#define READ_CHAR_FIELD(fldname) \
	memcpy(&local_node->fldname, *str, 1); (*str)++

/* Read an enumerated-type field that was written as a short integer code */
#define READ_ENUM_FIELD(fldname, enumtype) \
	{ int16 ent; memcpy(&ent, *str, sizeof(int16));  (*str)+=sizeof(int16);local_node->fldname = (enumtype) ent; }

/* Read a float field */
#define READ_FLOAT_FIELD(fldname) \
	memcpy(&local_node->fldname, *str, sizeof(double)); (*str)+=sizeof(double)

/* Read a boolean field */
#define READ_BOOL_FIELD(fldname) \
	local_node->fldname = (*str)[0] != 0;  Assert((*str)[0]==1 || (*str)[0]==0); (*str)++

/* Read a character-string field */
#define READ_STRING_FIELD(fldname) \
	{ int slen; char * nn = NULL; \
		memcpy(&slen, *str, sizeof(int)); \
		(*str)+=sizeof(int); \
		if (slen>0) { \
		    nn = palloc(slen+1); \
		    memcpy(nn,*str,slen); \
		    (*str)+=(slen); nn[slen]='\0'; \
		} \
		local_node->fldname = nn;  }

/* Read a Node field */
#define READ_NODE_FIELD(fldname) \
	local_node->fldname = readNodeBinary(str)

/* Read a bitmapset field */
#define READ_BITMAPSET_FIELD(fldname) \
	 local_node->fldname = bitmapsetRead(str)

/* Read in a binary field */
#define READ_BINARY_FIELD(fldname, sz) \
	memcpy(&local_node->fldname, *str, (sz)); (*str) += (sz)

/* Read a bytea field */
#define READ_BYTEA_FIELD(fldname) \
	local_node->fldname = DatumGetPointer(readDatum(str, false))

/* Read a dummy field */
#define READ_DUMMY_FIELD(fldname,fldvalue) \
	{ local_node->fldname = (0); /*(*str) += sizeof(int*);*/ }

/* Routine exit */
#define READ_DONE() \
	return local_node

/* Read an integer array  */
#define READ_INT_ARRAY(fldname, count, Type) \
	if ( local_node->count > 0 ) \
	{ \
		int i; \
		local_node->fldname = (Type *)palloc(local_node->count * sizeof(Type)); \
		for(i=0; i<local_node->count; i++) \
		{ \
			memcpy(&local_node->fldname[i], *str, sizeof(Type)); (*str)+=sizeof(Type); \
		} \
	}



/* Read an Trasnaction ID array  */
#define READ_XID_ARRAY(fldname, count) \
	if ( local_node->count > 0 ) \
	{ \
		int i; \
		local_node->fldname = (TransactionId *)palloc(local_node->count * sizeof(TransactionId)); \
		for(i=0; i<local_node->count; i++) \
		{ \
			memcpy(&local_node->fldname[i], *str, sizeof(TransactionId)); (*str)+=sizeof(TransactionId); \
		} \
	}



/* Read an Oid array  */
#define READ_OID_ARRAY(fldname, count) \
	if ( local_node->count > 0 ) \
	{ \
		int i; \
		local_node->fldname = (Oid *)palloc(local_node->count * sizeof(Oid)); \
		for(i=0; i<local_node->count; i++) \
		{ \
			memcpy(&local_node->fldname[i], *str, sizeof(Oid)); (*str)+=sizeof(Oid); \
		} \
	}

/* Read RelFileNode */
#define READ_RELFILENODE_FIELD(fldname) \
	{ \
		memcpy(&local_node->fldname, *str, sizeof(RelFileNode)); \
		(*str) += sizeof(RelFileNode); \
	}

static void * readNodeBinary(const char ** str);

static Datum readDatum(const char ** str, bool typbyval);

static TupleDesc
_readTupleDesc(const char **str)
{
	int i;
	TupleDesc local_node = (TupleDesc) palloc0(sizeof(struct tupleDesc));
	bool constr_is_notnull = false;

	READ_INT_FIELD(natts);
	local_node->attrs = (Form_pg_attribute *) palloc0(
			local_node->natts * sizeof(Form_pg_attribute));
	for (i = 0; i < local_node->natts; i++)
	{
		local_node->attrs[i] = (Form_pg_attribute) palloc(ATTRIBUTE_FIXED_PART_SIZE);
		memcpy(local_node->attrs[i], *str, ATTRIBUTE_FIXED_PART_SIZE);
		(*str) += ATTRIBUTE_FIXED_PART_SIZE;
	}

	/*
	 * The serializer writes 1 if the pointer was valid, and 0 otherwise.
	 * We need to allocate true memory for constr if we read back the content.
	 */
	constr_is_notnull = (*str)[0] != 0;
	(*str)++;

	if (constr_is_notnull)
	{
		local_node->constr = (TupleConstr *) palloc0(sizeof(TupleConstr));
		READ_INT_FIELD(constr->num_defval);
		if (local_node->constr->num_defval > 0)
		{
			local_node->constr->defval = (AttrDefault *) palloc(
					local_node->constr->num_defval * sizeof(AttrDefault));
		}
		for (i = 0; i < local_node->constr->num_defval; i++)
		{
			READ_INT_FIELD(constr->defval[i].adnum);
			READ_STRING_FIELD(constr->defval[i].adbin);
		}

		READ_INT_FIELD(constr->num_check);
		if (local_node->constr->num_check > 0)
		{
			local_node->constr->check = (ConstrCheck *) palloc(
					local_node->constr->num_check * sizeof(ConstrCheck));
		}

		for (i = 0; i < local_node->constr->num_check; i++)
		{
			READ_STRING_FIELD(constr->check[i].ccname);
			READ_STRING_FIELD(constr->check[i].ccbin);
		}
	}

	READ_OID_FIELD(tdtypeid);
	READ_INT_FIELD(tdtypmod);
	READ_BOOL_FIELD(tdhasoid);
	/* tdrefcount is not effective */

	READ_DONE();
}

/*
 * _readQuery
 */
static Query *
_readQuery(const char ** str)
{
	READ_LOCALS(Query);

	READ_ENUM_FIELD(commandType, CmdType); Assert(local_node->commandType <= CMD_NOTHING);
	READ_ENUM_FIELD(querySource, QuerySource); 	Assert(local_node->querySource <= QSRC_PLANNER);
	READ_BOOL_FIELD(canSetTag);
	READ_NODE_FIELD(utilityStmt);
	READ_INT_FIELD(resultRelation);
	READ_NODE_FIELD(intoClause); 
	READ_BOOL_FIELD(hasAggs);
	READ_BOOL_FIELD(hasWindFuncs);
	READ_BOOL_FIELD(hasSubLinks);
	READ_NODE_FIELD(rtable);
	READ_NODE_FIELD(jointree);
	READ_NODE_FIELD(targetList);
	READ_NODE_FIELD(returningList);
	READ_NODE_FIELD(groupClause);
	READ_NODE_FIELD(havingQual);
	READ_NODE_FIELD(windowClause);
	READ_NODE_FIELD(distinctClause);
	READ_NODE_FIELD(sortClause);
	READ_NODE_FIELD(scatterClause);
	READ_NODE_FIELD(cteList);
	READ_BOOL_FIELD(hasRecursive);
	READ_BOOL_FIELD(hasModifyingCTE);
	READ_NODE_FIELD(limitOffset);
	READ_NODE_FIELD(limitCount);
	READ_NODE_FIELD(rowMarks);
	READ_NODE_FIELD(setOperations);
	READ_NODE_FIELD(resultRelations);
	READ_NODE_FIELD(result_partitions);
	READ_NODE_FIELD(result_aosegnos);
	READ_NODE_FIELD(returningLists);
	READ_NODE_FIELD(contextdisp);
	/* policy not serialized */

	READ_DONE();
}

/*
 * _readNotifyStmt
 */
static NotifyStmt *
_readNotifyStmt(const char ** str)
{
	READ_LOCALS(NotifyStmt);

	READ_NODE_FIELD(relation);

	READ_DONE();
}

/*
 * _readDeclareCursorStmt
 */
static DeclareCursorStmt *
_readDeclareCursorStmt(const char ** str)
{
	READ_LOCALS(DeclareCursorStmt);

	READ_STRING_FIELD(portalname);
	READ_INT_FIELD(options);
	READ_NODE_FIELD(query);
	READ_BOOL_FIELD(is_simply_updatable);

	READ_DONE();
}

/*
 * _readSingleRowErrorDesc
 */
static SingleRowErrorDesc *
_readSingleRowErrorDesc(const char ** str)
{
	READ_LOCALS(SingleRowErrorDesc);

	READ_NODE_FIELD(errtable);
	READ_INT_FIELD(rejectlimit);
	READ_BOOL_FIELD(is_keep);
	READ_BOOL_FIELD(is_limit_in_rows);
	READ_BOOL_FIELD(reusing_existing_errtable);

	READ_DONE();
}

/*
 * _readSortClause
 */
static SortClause *
_readSortClause(const char ** str)
{
	READ_LOCALS(SortClause);

	READ_UINT_FIELD(tleSortGroupRef);
	READ_OID_FIELD(sortop);

	READ_DONE();
}

/*
 * _readGroupClause
 */
static GroupClause *
_readGroupClause(const char ** str)
{
	READ_LOCALS(GroupClause);

	READ_UINT_FIELD(tleSortGroupRef);
	READ_OID_FIELD(sortop);

	READ_DONE();
}

/*
 * _readGroupingClause
 */
static GroupingClause *
_readGroupingClause(const char ** str)
{
	READ_LOCALS(GroupingClause);

	READ_ENUM_FIELD(groupType, GroupingType);
	READ_NODE_FIELD(groupsets);

	READ_DONE();
}

static GroupingFunc *
_readGroupingFunc(const char ** str)
{
	READ_LOCALS(GroupingFunc);

	READ_NODE_FIELD(args);
	READ_INT_FIELD(ngrpcols);

	READ_DONE();
}

static Grouping *
_readGrouping(const char ** str __attribute__((unused)))
{
	READ_LOCALS(Grouping);

	READ_DONE();
}

static GroupId *
_readGroupId(const char ** str __attribute__((unused)))
{
	READ_LOCALS(GroupId);

	READ_DONE();
}

static WindowSpecParse *
_readWindowSpecParse(const char ** str)
{
	READ_LOCALS(WindowSpecParse);

	READ_STRING_FIELD(name);
	READ_NODE_FIELD(elems);

	READ_DONE();
}

static WindowSpec *
_readWindowSpec(const char ** str)
{
	READ_LOCALS(WindowSpec);

	READ_STRING_FIELD(name);
	READ_STRING_FIELD(parent);
	READ_NODE_FIELD(partition);
	READ_NODE_FIELD(order);
	READ_NODE_FIELD(frame);
	READ_INT_FIELD(location);
	READ_DONE();
}

static WindowFrame *
_readWindowFrame(const char ** str)
{
	READ_LOCALS(WindowFrame);

	READ_BOOL_FIELD(is_rows);
	READ_BOOL_FIELD(is_between);
	READ_NODE_FIELD(trail);
	READ_NODE_FIELD(lead);
	READ_ENUM_FIELD(exclude, WindowExclusion);

	READ_DONE();
}

static WindowFrameEdge *
_readWindowFrameEdge(const char ** str)
{
	READ_LOCALS(WindowFrameEdge);

	READ_ENUM_FIELD(kind, WindowBoundingKind);
	READ_NODE_FIELD(val);

	READ_DONE();
}

static PercentileExpr *
_readPercentileExpr(const char ** str)
{
	READ_LOCALS(PercentileExpr);

	READ_OID_FIELD(perctype);
	READ_NODE_FIELD(args);
	READ_ENUM_FIELD(perckind, PercKind);
	READ_NODE_FIELD(sortClause);
	READ_NODE_FIELD(sortTargets);
	READ_NODE_FIELD(pcExpr);
	READ_NODE_FIELD(tcExpr);
	READ_INT_FIELD(location);

	READ_DONE();
}

static DMLActionExpr *
_readDMLActionExpr(const char ** str)
{
	READ_LOCALS(DMLActionExpr);

	READ_DONE();
}

static PartOidExpr *
_readPartOidExpr(const char ** str)
{
	READ_LOCALS(PartOidExpr);

	READ_INT_FIELD(level);

	READ_DONE();
}

static PartDefaultExpr *
_readPartDefaultExpr(const char ** str)
{
	READ_LOCALS(PartDefaultExpr);

	READ_INT_FIELD(level);

	READ_DONE();
}

static PartBoundExpr *
_readPartBoundExpr(const char ** str)
{
	READ_LOCALS(PartBoundExpr);

	READ_INT_FIELD(level);
	READ_OID_FIELD(boundType);
	READ_BOOL_FIELD(isLowerBound);

	READ_DONE();
}

static PartBoundInclusionExpr *
_readPartBoundInclusionExpr(const char ** str)
{
	READ_LOCALS(PartBoundInclusionExpr);

	READ_INT_FIELD(level);
	READ_BOOL_FIELD(isLowerBound);

	READ_DONE();
}

static PartBoundOpenExpr *
_readPartBoundOpenExpr(const char ** str)
{
	READ_LOCALS(PartBoundOpenExpr);

	READ_INT_FIELD(level);
	READ_BOOL_FIELD(isLowerBound);

	READ_DONE();
}

/*
 * _readRowMarkClause
 */
static RowMarkClause *
_readRowMarkClause(const char ** str)
{
	READ_LOCALS(RowMarkClause);

	READ_UINT_FIELD(rti);
	READ_BOOL_FIELD(forUpdate);
	READ_BOOL_FIELD(noWait);

	READ_DONE();
}

static WithClause *
_readWithClause(const char ** str)
{
	READ_LOCALS(WithClause);
	
	READ_NODE_FIELD(ctes);
	READ_BOOL_FIELD(recursive);
	READ_INT_FIELD(location);
	
	READ_DONE();
}

static CommonTableExpr *
_readCommonTableExpr(const char ** str)
{
	READ_LOCALS(CommonTableExpr);
	
	READ_STRING_FIELD(ctename);
	READ_NODE_FIELD(aliascolnames);
	READ_NODE_FIELD(ctequery);
	READ_INT_FIELD(location);
	READ_BOOL_FIELD(cterecursive);
	READ_INT_FIELD(cterefcount);
	READ_NODE_FIELD(ctecolnames);
	READ_NODE_FIELD(ctecoltypes);
	READ_NODE_FIELD(ctecoltypmods);

	READ_DONE();
}

/*
 * _readSetOperationStmt
 */
static SetOperationStmt *
_readSetOperationStmt(const char ** str)
{
	READ_LOCALS(SetOperationStmt);

	READ_ENUM_FIELD(op, SetOperation);
	READ_BOOL_FIELD(all);
	READ_NODE_FIELD(larg);
	READ_NODE_FIELD(rarg);
	READ_NODE_FIELD(colTypes);
	READ_NODE_FIELD(colTypmods);

	READ_DONE();
}

/*
 *	Stuff from primnodes.h.
 */

static Alias *
_readAlias(const char ** str)
{
	READ_LOCALS(Alias);

	READ_STRING_FIELD(aliasname);
	READ_NODE_FIELD(colnames);

	READ_DONE();
}

static RangeVar *
_readRangeVar(const char ** str)
{
	READ_LOCALS(RangeVar);

	local_node->catalogname = NULL;		/* not currently saved in output
										 * format */

	READ_STRING_FIELD(schemaname);
	READ_STRING_FIELD(relname);
	READ_ENUM_FIELD(inhOpt, InhOption); Assert(local_node->inhOpt <= INH_DEFAULT);
	READ_BOOL_FIELD(istemp);
	READ_NODE_FIELD(alias);
    READ_INT_FIELD(location);   /*CDB*/

	READ_DONE();
}

static IntoClause *
_readIntoClause(const char ** str)
{
	READ_LOCALS(IntoClause);
	
	READ_NODE_FIELD(rel);
	READ_NODE_FIELD(colNames);
	READ_NODE_FIELD(options);
	READ_ENUM_FIELD(onCommit, OnCommitAction);
	READ_STRING_FIELD(tableSpaceName);
	READ_OID_FIELD(oidInfo.relOid);
    READ_OID_FIELD(oidInfo.comptypeOid); 
    READ_OID_FIELD(oidInfo.toastOid);
    READ_OID_FIELD(oidInfo.toastIndexOid);
    READ_OID_FIELD(oidInfo.toastComptypeOid);
    READ_OID_FIELD(oidInfo.aosegOid);
    READ_OID_FIELD(oidInfo.aosegIndexOid);
    READ_OID_FIELD(oidInfo.aosegComptypeOid);
    READ_OID_FIELD(oidInfo.aoblkdirOid);
    READ_OID_FIELD(oidInfo.aoblkdirIndexOid);
    READ_OID_FIELD(oidInfo.aoblkdirComptypeOid);

	READ_DONE();
}

/*
 * _readVar
 */
static Var *
_readVar(const char ** str)
{
	READ_LOCALS(Var);

	READ_UINT_FIELD(varno);
	READ_INT_FIELD(varattno);
	READ_OID_FIELD(vartype);
	READ_INT_FIELD(vartypmod);
	READ_UINT_FIELD(varlevelsup);
	READ_UINT_FIELD(varnoold);
	READ_INT_FIELD(varoattno);

	READ_DONE();
}

/*
 * _readConst
 */
static Const *
_readConst(const char ** str)
{
	READ_LOCALS(Const);

	READ_OID_FIELD(consttype);
	READ_INT_FIELD(constlen);
	READ_BOOL_FIELD(constbyval);
	READ_BOOL_FIELD(constisnull);

	if (local_node->constisnull)
		local_node->constvalue = 0;
	else
		local_node->constvalue = readDatum(str,local_node->constbyval);

	READ_DONE();
}

/*
 * _readConstraint
 */
static Constraint *
_readConstraint(const char ** str)
{

	READ_LOCALS(Constraint);

	READ_STRING_FIELD(name);			/* name, or NULL if unnamed */
	READ_OID_FIELD(conoid);

	READ_ENUM_FIELD(contype,ConstrType);
	Assert(local_node->contype <= CONSTR_ATTR_IMMEDIATE);


	switch (local_node->contype)
	{
		case CONSTR_UNIQUE:
		case CONSTR_PRIMARY:
			READ_NODE_FIELD(keys);
			READ_NODE_FIELD(options);
			READ_STRING_FIELD(indexspace);
		break;

		case CONSTR_CHECK:
		case CONSTR_DEFAULT:
			READ_NODE_FIELD(raw_expr);
			READ_STRING_FIELD(cooked_expr);
		break;

		case CONSTR_NULL:
		case CONSTR_NOTNULL:
		case CONSTR_ATTR_DEFERRABLE:
		case CONSTR_ATTR_NOT_DEFERRABLE:
		case CONSTR_ATTR_DEFERRED:
		case CONSTR_ATTR_IMMEDIATE:
		break;

		default:
			elog(WARNING,"Can't deserialize constraint %d",(int)local_node->contype);
			break;
	}


	READ_DONE();
}

static IndexStmt *
_readIndexStmt(const char ** str)
{
	READ_LOCALS(IndexStmt);

	READ_STRING_FIELD(idxname);
	READ_NODE_FIELD(relation);
	READ_STRING_FIELD(accessMethod);
	READ_STRING_FIELD(tableSpace);
	READ_NODE_FIELD(indexParams);
	READ_NODE_FIELD(options);
	READ_NODE_FIELD(whereClause);
	READ_NODE_FIELD(rangetable);
	READ_BOOL_FIELD(is_part_child);
	READ_BOOL_FIELD(unique);
	READ_BOOL_FIELD(primary);
	READ_BOOL_FIELD(isconstraint);
	READ_STRING_FIELD(altconname);
	READ_OID_FIELD(constrOid);
	READ_BOOL_FIELD(concurrent);
	READ_NODE_FIELD(idxOids);

	READ_DONE();
}

static IndexElem *
_readIndexElem(const char ** str)
{
	READ_LOCALS(IndexElem);

	READ_STRING_FIELD(name);
	READ_NODE_FIELD(expr);
	READ_NODE_FIELD(opclass);


	READ_DONE();
}

static ReindexStmt *
_readReindexStmt(const char ** str)
{
	READ_LOCALS(ReindexStmt);

	READ_ENUM_FIELD(kind,ObjectType); Assert(local_node->kind <= OBJECT_VIEW);
	READ_NODE_FIELD(relation);
	READ_STRING_FIELD(name);
	READ_BOOL_FIELD(do_system);
	READ_BOOL_FIELD(do_user);
	READ_NODE_FIELD(new_ind_oids);

	READ_DONE();

}

static ViewStmt *
_readViewStmt(const char ** str)
{
	READ_LOCALS(ViewStmt);

	READ_NODE_FIELD(view);
	READ_NODE_FIELD(aliases);
	READ_NODE_FIELD(query);
	READ_BOOL_FIELD(replace);
	READ_OID_FIELD(relOid);
	READ_OID_FIELD(comptypeOid);
	READ_OID_FIELD(rewriteOid);

	READ_DONE();
}

static RuleStmt *
_readRuleStmt(const char ** str)
{
	READ_LOCALS(RuleStmt);

	READ_NODE_FIELD(relation);
	READ_STRING_FIELD(rulename);
	READ_NODE_FIELD(whereClause);
	READ_ENUM_FIELD(event,CmdType);
	READ_BOOL_FIELD(instead);
	READ_NODE_FIELD(actions);
	READ_BOOL_FIELD(replace);
	READ_OID_FIELD(ruleOid);

	READ_DONE();
}

static DropStmt *
_readDropStmt(const char ** str)
{
	READ_LOCALS(DropStmt);

	READ_NODE_FIELD(objects);
	READ_ENUM_FIELD(removeType,ObjectType);
	READ_ENUM_FIELD(behavior, DropBehavior); Assert(local_node->behavior <= DROP_CASCADE);
	READ_BOOL_FIELD(missing_ok);
	READ_BOOL_FIELD(bAllowPartn);

	READ_DONE();
}

static DropPropertyStmt *
_readDropPropertyStmt(const char ** str)
{
	READ_LOCALS(DropPropertyStmt);

	READ_NODE_FIELD(relation);
	READ_STRING_FIELD(property);
	READ_ENUM_FIELD(removeType,ObjectType);
	READ_ENUM_FIELD(behavior, DropBehavior); Assert(local_node->behavior <= DROP_CASCADE);
	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}

static DropOwnedStmt *
_readDropOwnedStmt(const char ** str)
{
	READ_LOCALS(DropOwnedStmt);

	READ_NODE_FIELD(roles);
	READ_ENUM_FIELD(behavior, DropBehavior);

	READ_DONE();
}

static ReassignOwnedStmt *
_readReassignOwnedStmt(const char ** str)
{
	READ_LOCALS(ReassignOwnedStmt);

	READ_NODE_FIELD(roles);
	READ_STRING_FIELD(newrole);

	READ_DONE();
}

static TruncateStmt *
_readTruncateStmt(const char ** str)
{
	READ_LOCALS(TruncateStmt);

	READ_NODE_FIELD(relations);
	READ_ENUM_FIELD(behavior, DropBehavior);
	Assert(local_node->behavior <= DROP_CASCADE);

	READ_NODE_FIELD(relids);
	READ_NODE_FIELD(new_heap_oids);
	READ_NODE_FIELD(new_toast_oids);
	READ_NODE_FIELD(new_aoseg_oids);
	READ_NODE_FIELD(new_aoblkdir_oids);
	READ_NODE_FIELD(new_ind_oids);

	READ_DONE();
}

static AlterTableStmt *
_readAlterTableStmt(const char ** str)
{
	int m;
	READ_LOCALS(AlterTableStmt);

	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(cmds);
	READ_ENUM_FIELD(relkind, ObjectType); Assert(local_node->relkind <= OBJECT_VIEW);
	READ_NODE_FIELD(oidmap);
	READ_INT_FIELD(oidInfoCount);
	local_node->oidInfo = NULL;
	if (local_node->oidInfoCount > 0)
	{
		ALLOCATE_LOCAL(local_node->oidInfo, TableOidInfo, local_node->oidInfoCount);
		for (m = 0; m < local_node->oidInfoCount; m++)
		{
			READ_OID_FIELD(oidInfo[m].relOid);
			READ_OID_FIELD(oidInfo[m].comptypeOid);
			READ_OID_FIELD(oidInfo[m].toastOid);
			READ_OID_FIELD(oidInfo[m].toastIndexOid);
			READ_OID_FIELD(oidInfo[m].toastComptypeOid);
			READ_OID_FIELD(oidInfo[m].aosegOid);
			READ_OID_FIELD(oidInfo[m].aosegIndexOid);
			READ_OID_FIELD(oidInfo[m].aosegComptypeOid);
			READ_OID_FIELD(oidInfo[m].aoblkdirOid);
			READ_OID_FIELD(oidInfo[m].aoblkdirIndexOid);
			READ_OID_FIELD(oidInfo[m].aoblkdirComptypeOid);
		}
	}

	READ_DONE();
}

static AlterTableCmd *
_readAlterTableCmd(const char ** str)
{
	READ_LOCALS(AlterTableCmd);

	READ_ENUM_FIELD(subtype, AlterTableType);
	READ_STRING_FIELD(name);
	READ_NODE_FIELD(def);
	READ_NODE_FIELD(transform);
	READ_ENUM_FIELD(behavior, DropBehavior); Assert(local_node->behavior <= DROP_CASCADE);
	READ_BOOL_FIELD(part_expanded);
	READ_NODE_FIELD(partoids);
	READ_DONE();
}

static InheritPartitionCmd *
_readInheritPartitionCmd(const char **str)
{
	READ_LOCALS(InheritPartitionCmd);

	READ_NODE_FIELD(parent);

	READ_DONE();
}

static AlterPartitionCmd *
_readAlterPartitionCmd(const char ** str)
{
	READ_LOCALS(AlterPartitionCmd);

	READ_NODE_FIELD(partid);
	READ_NODE_FIELD(arg1);
	READ_NODE_FIELD(arg2);
	READ_NODE_FIELD(scantable_splits);
	READ_NODE_FIELD(newpart_aosegnos);

	READ_DONE();
}

static AlterPartitionId *
_readAlterPartitionId(const char ** str)
{
	READ_LOCALS(AlterPartitionId);

	READ_ENUM_FIELD(idtype, AlterPartitionIdType);
	READ_NODE_FIELD(partiddef);

	READ_DONE();
}

static AlterRewriteTableInfo *
_readAlterRewriteTableInfo(const char **str)
{
	READ_LOCALS(AlterRewriteTableInfo);

	READ_OID_FIELD(relid);
	READ_CHAR_FIELD(relkind);
	local_node->oldDesc = _readTupleDesc(str);
	READ_NODE_FIELD(constraints);
	READ_NODE_FIELD(newvals);
	READ_BOOL_FIELD(new_notnull);
	READ_BOOL_FIELD(new_dropoids);
	READ_OID_FIELD(newTableSpace);
	READ_OID_FIELD(exchange_relid);
	READ_OID_FIELD(newheap_oid);
	READ_NODE_FIELD(scantable_splits);
	READ_NODE_FIELD(ao_segnos);

	READ_DONE();
}

static AlterRewriteNewConstraint *
_readAlterRewriteNewConstraint(const char **str)
{
	READ_LOCALS(AlterRewriteNewConstraint);

	READ_STRING_FIELD(name);
	READ_ENUM_FIELD(contype, ConstrType);
	READ_OID_FIELD(refrelid);
	READ_NODE_FIELD(qual);

	READ_DONE();
}

static AlterRewriteNewColumnValue *
_readAlterRewriteNewColumnValue(const char **str)
{
	READ_LOCALS(AlterRewriteNewColumnValue);

	READ_INT_FIELD(attnum);
	READ_NODE_FIELD(expr);

	READ_DONE();
}

static CreateRoleStmt *
_readCreateRoleStmt(const char ** str)
{
	READ_LOCALS(CreateRoleStmt);

	READ_ENUM_FIELD(stmt_type, RoleStmtType);
	READ_STRING_FIELD(role);
	READ_NODE_FIELD(options);
	READ_OID_FIELD(roleOid);

	READ_DONE();
}

static DenyLoginInterval *
_readDenyLoginInterval(const char ** str)
{
	READ_LOCALS(DenyLoginInterval);

	READ_NODE_FIELD(start);
	READ_NODE_FIELD(end);
	
	READ_DONE();
}

static DenyLoginPoint *
_readDenyLoginPoint(const char ** str)
{
	READ_LOCALS(DenyLoginPoint);

	READ_NODE_FIELD(day);
	READ_NODE_FIELD(time);

	READ_DONE();
}

static DropRoleStmt *
_readDropRoleStmt(const char ** str)
{
	READ_LOCALS(DropRoleStmt);

	READ_NODE_FIELD(roles);
	READ_BOOL_FIELD(missing_ok);

	READ_DONE();

}

static  AlterRoleStmt *
_readAlterRoleStmt(const char ** str)
{
	READ_LOCALS(AlterRoleStmt);

	READ_STRING_FIELD(role);
	READ_NODE_FIELD(options);
	READ_INT_FIELD(action);
	READ_DONE();

}

static  AlterRoleSetStmt *
_readAlterRoleSetStmt(const char ** str)
{
	READ_LOCALS(AlterRoleSetStmt);

	READ_STRING_FIELD(role);
	READ_STRING_FIELD(variable);
	READ_NODE_FIELD(value);

	READ_DONE();

}

static AlterObjectSchemaStmt *
_readAlterObjectSchemaStmt(const char ** str)
{
	READ_LOCALS(AlterObjectSchemaStmt);

	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(object);
	READ_NODE_FIELD(objarg);
	READ_STRING_FIELD(addname);
	READ_STRING_FIELD(newschema);
	READ_ENUM_FIELD(objectType,ObjectType); Assert(local_node->objectType <= OBJECT_VIEW);

	READ_DONE();
}



static AlterOwnerStmt *
_readAlterOwnerStmt(const char ** str)
{
	READ_LOCALS(AlterOwnerStmt);

	READ_ENUM_FIELD(objectType,ObjectType); Assert(local_node->objectType <= OBJECT_VIEW);
	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(object);
	READ_NODE_FIELD(objarg);
	READ_STRING_FIELD(addname);
	READ_STRING_FIELD(newowner);

	READ_DONE();
}


static RenameStmt *
_readRenameStmt(const char ** str)
{
	READ_LOCALS(RenameStmt);

	READ_NODE_FIELD(relation);
	READ_OID_FIELD(objid);
	READ_NODE_FIELD(object);
	READ_NODE_FIELD(objarg);
	READ_STRING_FIELD(subname);
	READ_STRING_FIELD(newname);
	READ_ENUM_FIELD(renameType,ObjectType);
	READ_BOOL_FIELD(bAllowPartn);

	READ_DONE();
}


static FuncCall *
_readFuncCall(const char ** str)
{
	READ_LOCALS(FuncCall);

	READ_NODE_FIELD(funcname);
	READ_NODE_FIELD(args);
    READ_NODE_FIELD(agg_order);
	READ_BOOL_FIELD(agg_star);
	READ_BOOL_FIELD(agg_distinct);
	READ_NODE_FIELD(over);
	READ_INT_FIELD(location);
	READ_NODE_FIELD(agg_filter);
	READ_DONE();
}

static DefElem *
_readDefElem(const char ** str)
{
	READ_LOCALS(DefElem);

	READ_STRING_FIELD(defname);
	READ_NODE_FIELD(arg);
	READ_ENUM_FIELD(defaction, DefElemAction);
	READ_DONE();
}

static A_Const *
_readAConst(const char ** str)
{
	READ_LOCALS(A_Const);

	READ_ENUM_FIELD(val.type, NodeTag);



	switch (local_node->val.type)
	{
		case T_Integer:
			memcpy(&local_node->val.val.ival, *str, sizeof(long)); (*str)+=sizeof(long);
			break;
		case T_Float:
		case T_String:
		case T_BitString:
		{
			int slen; char * nn;
			memcpy(&slen, *str, sizeof(int));
			(*str)+=sizeof(int);
			nn = palloc(slen+1);
			memcpy(nn,*str,slen);
			nn[slen] = '\0';
			local_node->val.val.str = nn; (*str)+=slen;
		}
			break;
	 	case T_Null:
	 	default:
	 		break;
	}


	local_node->typname = NULL;
	READ_NODE_FIELD(typname);
    READ_INT_FIELD(location);   /*CDB*/
	READ_DONE();
}


static A_Indices *
_readA_Indices(const char ** str)
{
	READ_LOCALS(A_Indices);
	READ_NODE_FIELD(lidx);
	READ_NODE_FIELD(uidx);
	READ_DONE();
}

static A_Indirection *
_readA_Indirection(const char ** str)
{
	READ_LOCALS(A_Indirection);
	READ_NODE_FIELD(arg);
	READ_NODE_FIELD(indirection);
	READ_DONE();
}


static A_Expr *
_readAExpr(const char ** str)
{
	READ_LOCALS(A_Expr);

	READ_ENUM_FIELD(kind, A_Expr_Kind);

	Assert(local_node->kind <= AEXPR_IN);

	switch (local_node->kind)
	{
		case AEXPR_OP:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_AND:

			break;
		case AEXPR_OR:

			break;
		case AEXPR_NOT:

			break;
		case AEXPR_OP_ANY:

			READ_NODE_FIELD(name);

			break;
		case AEXPR_OP_ALL:

			READ_NODE_FIELD(name);

			break;
		case AEXPR_DISTINCT:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_NULLIF:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_OF:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_IN:

			READ_NODE_FIELD(name);
			break;
		default:
			elog(ERROR,"Unable to understand A_Expr node ");
			break;
	}




	READ_NODE_FIELD(lexpr);
	READ_NODE_FIELD(rexpr);
	READ_INT_FIELD(location);

	READ_DONE();
}

/*
 * _readParam
 */
static Param *
_readParam(const char ** str)
{
	READ_LOCALS(Param);

	READ_ENUM_FIELD(paramkind, ParamKind);
	READ_INT_FIELD(paramid);
	READ_OID_FIELD(paramtype);

	READ_DONE();
}

/*
 * _readAggref
 */
static Aggref *
_readAggref(const char ** str)
{
	READ_LOCALS(Aggref);

	READ_OID_FIELD(aggfnoid);
	READ_OID_FIELD(aggtype);
	READ_NODE_FIELD(args);
	READ_UINT_FIELD(agglevelsup);
	READ_BOOL_FIELD(aggstar);
	READ_BOOL_FIELD(aggdistinct);
	READ_ENUM_FIELD(aggstage, AggStage);
    READ_NODE_FIELD(aggorder);

	READ_DONE();
}

/*
 * _outAggOrder
 */
static AggOrder *
_readAggOrder(const char ** str)
{
	READ_LOCALS(AggOrder);

    READ_BOOL_FIELD(sortImplicit);
    READ_NODE_FIELD(sortTargets);
    READ_NODE_FIELD(sortClause);

    READ_DONE();
}

/*
 * _readWindowRef
 */
static WindowRef *
_readWindowRef(const char ** str)
{
	READ_LOCALS(WindowRef);

	READ_OID_FIELD(winfnoid);
	READ_OID_FIELD(restype);
	READ_NODE_FIELD(args);
	READ_UINT_FIELD(winlevelsup);
	READ_BOOL_FIELD(windistinct);
	READ_UINT_FIELD(winspec);
	READ_UINT_FIELD(winindex);
	READ_ENUM_FIELD(winstage, WinStage);
	READ_UINT_FIELD(winlevel);

	READ_DONE();
}

/*
 * _readArrayRef
 */
static ArrayRef *
_readArrayRef(const char ** str)
{
	READ_LOCALS(ArrayRef);

	READ_OID_FIELD(refrestype);
	READ_OID_FIELD(refarraytype);
	READ_OID_FIELD(refelemtype);
	READ_NODE_FIELD(refupperindexpr);
	READ_NODE_FIELD(reflowerindexpr);
	READ_NODE_FIELD(refexpr);
	READ_NODE_FIELD(refassgnexpr);

	READ_DONE();
}

/*
 * _readFuncExpr
 */
static FuncExpr *
_readFuncExpr(const char ** str)
{
	READ_LOCALS(FuncExpr);

	READ_OID_FIELD(funcid);
	READ_OID_FIELD(funcresulttype);
	READ_BOOL_FIELD(funcretset);
	READ_ENUM_FIELD(funcformat, CoercionForm);
	READ_NODE_FIELD(args);
	READ_BOOL_FIELD(is_tablefunc);

	READ_DONE();
}

/*
 * _readOpExpr
 */
static OpExpr *
_readOpExpr(const char ** str)
{
	READ_LOCALS(OpExpr);

	READ_OID_FIELD(opno);
	READ_OID_FIELD(opfuncid);

	/*
	 * The opfuncid is stored in the textual format primarily for debugging
	 * and documentation reasons.  We want to always read it as zero to force
	 * it to be re-looked-up in the pg_operator entry.	This ensures that
	 * stored rules don't have hidden dependencies on operators' functions.
	 * (We don't currently support an ALTER OPERATOR command, but might
	 * someday.)
	 */
/*	local_node->opfuncid = InvalidOid; */

	READ_OID_FIELD(opresulttype);
	READ_BOOL_FIELD(opretset);
	READ_NODE_FIELD(args);

	READ_DONE();
}

/*
 * _readDistinctExpr
 */
static DistinctExpr *
_readDistinctExpr(const char ** str)
{
	READ_LOCALS(DistinctExpr);

	READ_OID_FIELD(opno);
	READ_OID_FIELD(opfuncid);

	READ_OID_FIELD(opresulttype);
	READ_BOOL_FIELD(opretset);
	READ_NODE_FIELD(args);

	READ_DONE();
}

/*
 * _readScalarArrayOpExpr
 */
static ScalarArrayOpExpr *
_readScalarArrayOpExpr(const char ** str)
{
	READ_LOCALS(ScalarArrayOpExpr);

	READ_OID_FIELD(opno);
	READ_OID_FIELD(opfuncid);

	READ_BOOL_FIELD(useOr);
	READ_NODE_FIELD(args);

	READ_DONE();
}

/*
 * _readBoolExpr
 */
static BoolExpr *
_readBoolExpr(const char ** str)
{
	READ_LOCALS(BoolExpr);

	READ_ENUM_FIELD(boolop, BoolExprType);

	READ_NODE_FIELD(args);

	READ_DONE();
}

/*
 * _readSubLink
 */
static SubLink *
_readSubLink(const char ** str)
{
	READ_LOCALS(SubLink);

	READ_ENUM_FIELD(subLinkType, SubLinkType);
	READ_NODE_FIELD(testexpr);
	READ_NODE_FIELD(operName);
	READ_INT_FIELD(location);   /*CDB*/
	READ_NODE_FIELD(subselect);

	READ_DONE();
}

/*
 * _readSubPlan
 */
static SubPlan *
_readSubPlan(const char ** str)
{
	READ_LOCALS(SubPlan);

    READ_INT_FIELD(qDispSliceId);   /*CDB*/
	READ_ENUM_FIELD(subLinkType, SubLinkType);
	READ_NODE_FIELD(testexpr);
	READ_NODE_FIELD(paramIds);
	READ_INT_FIELD(plan_id);
	READ_OID_FIELD(firstColType);
	READ_INT_FIELD(firstColTypmod);
	READ_BOOL_FIELD(useHashTable);
	READ_BOOL_FIELD(unknownEqFalse);
	READ_BOOL_FIELD(is_initplan); /*CDB*/
	READ_BOOL_FIELD(is_multirow); /*CDB*/
	READ_NODE_FIELD(setParam);
	READ_NODE_FIELD(parParam);
	READ_NODE_FIELD(args);

	READ_DONE();
}

/*
 * _readFieldSelect
 */
static FieldSelect *
_readFieldSelect(const char ** str)
{
	READ_LOCALS(FieldSelect);

	READ_NODE_FIELD(arg);
	READ_INT_FIELD(fieldnum);
	READ_OID_FIELD(resulttype);
	READ_INT_FIELD(resulttypmod);

	READ_DONE();
}

/*
 * _readFieldStore
 */
static FieldStore *
_readFieldStore(const char ** str)
{
	READ_LOCALS(FieldStore);

	READ_NODE_FIELD(arg);
	READ_NODE_FIELD(newvals);
	READ_NODE_FIELD(fieldnums);
	READ_OID_FIELD(resulttype);

	READ_DONE();
}

/*
 * _readRelabelType
 */
static RelabelType *
_readRelabelType(const char ** str)
{
	READ_LOCALS(RelabelType);

	READ_NODE_FIELD(arg);
	READ_OID_FIELD(resulttype);
	READ_INT_FIELD(resulttypmod);
	READ_ENUM_FIELD(relabelformat, CoercionForm);

	READ_DONE();
}

/*
 * _readConvertRowtypeExpr
 */
static ConvertRowtypeExpr *
_readConvertRowtypeExpr(const char ** str)
{
	READ_LOCALS(ConvertRowtypeExpr);

	READ_NODE_FIELD(arg);
	READ_OID_FIELD(resulttype);
	READ_ENUM_FIELD(convertformat, CoercionForm);

	READ_DONE();
}

/*
 * _readCaseExpr
 */
static CaseExpr *
_readCaseExpr(const char ** str)
{
	READ_LOCALS(CaseExpr);

	READ_OID_FIELD(casetype);
	READ_NODE_FIELD(arg);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(defresult);

	READ_DONE();
}

/*
 * _readCaseWhen
 */
static CaseWhen *
_readCaseWhen(const char ** str)
{
	READ_LOCALS(CaseWhen);

	READ_NODE_FIELD(expr);
	READ_NODE_FIELD(result);

	READ_DONE();
}

/*
 * _readCaseTestExpr
 */
static CaseTestExpr *
_readCaseTestExpr(const char ** str)
{
	READ_LOCALS(CaseTestExpr);

	READ_OID_FIELD(typeId);
	READ_INT_FIELD(typeMod);

	READ_DONE();
}

/*
 * _readArrayExpr
 */
static ArrayExpr *
_readArrayExpr(const char ** str)
{
	READ_LOCALS(ArrayExpr);

	READ_OID_FIELD(array_typeid);
	READ_OID_FIELD(element_typeid);
	READ_NODE_FIELD(elements);
	READ_BOOL_FIELD(multidims);

	READ_DONE();
}

/*
 * _readRowExpr
 */
static RowExpr *
_readRowExpr(const char ** str)
{
	READ_LOCALS(RowExpr);

	READ_NODE_FIELD(args);
	READ_OID_FIELD(row_typeid);
	READ_ENUM_FIELD(row_format, CoercionForm);

	READ_DONE();
}

/*
 * _readRowCompareExpr
 */
static RowCompareExpr *
_readRowCompareExpr(const char ** str)
{
	READ_LOCALS(RowCompareExpr);

	READ_ENUM_FIELD(rctype, RowCompareType);
	READ_NODE_FIELD(opnos);
	READ_NODE_FIELD(opclasses);
	READ_NODE_FIELD(largs);
	READ_NODE_FIELD(rargs);

	READ_DONE();
}

/*
 * _readCoalesceExpr
 */
static CoalesceExpr *
_readCoalesceExpr(const char ** str)
{
	READ_LOCALS(CoalesceExpr);

	READ_OID_FIELD(coalescetype);
	READ_NODE_FIELD(args);

	READ_DONE();
}

/*
 * _readMinMaxExpr
 */
static MinMaxExpr *
_readMinMaxExpr(const char ** str)
{
	READ_LOCALS(MinMaxExpr);

	READ_OID_FIELD(minmaxtype);
	READ_ENUM_FIELD(op, MinMaxOp);
	READ_NODE_FIELD(args);

	READ_DONE();
}

/*
 * _readNullIfExpr
 */
static NullIfExpr *
_readNullIfExpr(const char ** str)
{
	READ_LOCALS(NullIfExpr);

	READ_OID_FIELD(opno);
	READ_OID_FIELD(opfuncid);

	READ_OID_FIELD(opresulttype);
	READ_BOOL_FIELD(opretset);
	READ_NODE_FIELD(args);

	READ_DONE();
}

/*
 * _readNullTest
 */
static NullTest *
_readNullTest(const char ** str)
{
	READ_LOCALS(NullTest);

	READ_NODE_FIELD(arg);
	READ_ENUM_FIELD(nulltesttype, NullTestType);

	READ_DONE();
}

/*
 * _readBooleanTest
 */
static BooleanTest *
_readBooleanTest(const char ** str)
{
	READ_LOCALS(BooleanTest);

	READ_NODE_FIELD(arg);
	READ_ENUM_FIELD(booltesttype, BoolTestType);

	READ_DONE();
}

/*
 * _readCoerceToDomain
 */
static CoerceToDomain *
_readCoerceToDomain(const char ** str)
{
	READ_LOCALS(CoerceToDomain);

	READ_NODE_FIELD(arg);
	READ_OID_FIELD(resulttype);
	READ_INT_FIELD(resulttypmod);
	READ_ENUM_FIELD(coercionformat, CoercionForm);

	READ_DONE();
}

/*
 * _readCoerceToDomainValue
 */
static CoerceToDomainValue *
_readCoerceToDomainValue(const char ** str)
{
	READ_LOCALS(CoerceToDomainValue);

	READ_OID_FIELD(typeId);
	READ_INT_FIELD(typeMod);

	READ_DONE();
}

/*
 * _readSetToDefault
 */
static SetToDefault *
_readSetToDefault(const char ** str)
{
	READ_LOCALS(SetToDefault);

	READ_OID_FIELD(typeId);
	READ_INT_FIELD(typeMod);

	READ_DONE();
}

/*
 * _readCurrentOfExpr
 */
static CurrentOfExpr *
_readCurrentOfExpr(const char ** str)
{
	READ_LOCALS(CurrentOfExpr);

	READ_STRING_FIELD(cursor_name);
	READ_UINT_FIELD(cvarno);
	READ_OID_FIELD(target_relid);
	READ_INT_FIELD(gp_segment_id);
	READ_BINARY_FIELD(ctid, sizeof(ItemPointerData));	
	READ_OID_FIELD(tableoid);

	READ_DONE();
}

/*
 * _readTargetEntry
 */
static TargetEntry *
_readTargetEntry(const char ** str)
{
	READ_LOCALS(TargetEntry);

	READ_NODE_FIELD(expr);
	READ_INT_FIELD(resno);
	READ_STRING_FIELD(resname);
	READ_UINT_FIELD(ressortgroupref);
	READ_OID_FIELD(resorigtbl);
	READ_INT_FIELD(resorigcol);
	READ_BOOL_FIELD(resjunk);

	READ_DONE();
}

/*
 * _readRangeTblRef
 */
static RangeTblRef *
_readRangeTblRef(const char ** str)
{
	READ_LOCALS(RangeTblRef);

	READ_INT_FIELD(rtindex);

	READ_DONE();
}

/*
 * _readJoinExpr
 */
static JoinExpr *
_readJoinExpr(const char ** str)
{
	READ_LOCALS(JoinExpr);

	READ_ENUM_FIELD(jointype, JoinType);
	READ_BOOL_FIELD(isNatural);
	READ_NODE_FIELD(larg);
	READ_NODE_FIELD(rarg);
	READ_NODE_FIELD(usingClause);   /*CDB*/
	READ_NODE_FIELD(quals);
	READ_NODE_FIELD(alias);
	READ_INT_FIELD(rtindex);

	READ_DONE();
}

/*
 * _readFromExpr
 */
static FromExpr *
_readFromExpr(const char ** str)
{
	READ_LOCALS(FromExpr);

	READ_NODE_FIELD(fromlist);
	READ_NODE_FIELD(quals);

	READ_DONE();
}


/*
 *	Stuff from parsenodes.h.
 */

static ColumnDef *
_readColumnDef(const char ** str)
{
	READ_LOCALS(ColumnDef);

	READ_STRING_FIELD(colname);
	READ_NODE_FIELD(typname);
	READ_INT_FIELD(inhcount);
	READ_BOOL_FIELD(is_local);
	READ_BOOL_FIELD(is_not_null);
	READ_NODE_FIELD(raw_default);
	READ_BOOL_FIELD(default_is_null);
	READ_STRING_FIELD(cooked_default);
	READ_NODE_FIELD(constraints);
	READ_NODE_FIELD(encoding);

	READ_DONE();
}

static ColumnRef *
_readColumnRef(const char ** str)
{
	READ_LOCALS(ColumnRef);

	READ_NODE_FIELD(fields);
	READ_INT_FIELD(location);

	READ_DONE();
}

static TypeName *
_readTypeName(const char ** str)
{
	READ_LOCALS(TypeName);

	READ_NODE_FIELD(names);
	READ_OID_FIELD(typid);
	READ_BOOL_FIELD(timezone);
	READ_BOOL_FIELD(setof);
	READ_BOOL_FIELD(pct_type);
	READ_INT_FIELD(typmod);
	READ_NODE_FIELD(arrayBounds);
	READ_INT_FIELD(location);

	READ_DONE();
}

static TypeCast *
_readTypeCast(const char ** str)
{
	READ_LOCALS(TypeCast);

	READ_NODE_FIELD(arg);
	READ_NODE_FIELD(typname);

	READ_DONE();
}


/*
 * _readRangeTblEntry
 */
static RangeTblEntry *
_readRangeTblEntry(const char ** str)
{
	READ_LOCALS(RangeTblEntry);

	/* put alias + eref first to make dump more legible */
	READ_NODE_FIELD(alias);
	READ_NODE_FIELD(eref);
	READ_ENUM_FIELD(rtekind, RTEKind);

	switch (local_node->rtekind)
	{
		case RTE_RELATION:
		case RTE_SPECIAL:
			READ_OID_FIELD(relid);
			break;
		case RTE_SUBQUERY:
			READ_NODE_FIELD(subquery);
			break;
		case RTE_CTE:
			READ_STRING_FIELD(ctename);
			READ_INT_FIELD(ctelevelsup);
			READ_BOOL_FIELD(self_reference);
			READ_NODE_FIELD(ctecoltypes);
			READ_NODE_FIELD(ctecoltypmods);
			break;
		case RTE_FUNCTION:
			READ_NODE_FIELD(funcexpr);
			READ_NODE_FIELD(funccoltypes);
			READ_NODE_FIELD(funccoltypmods);
			break;
		case RTE_TABLEFUNCTION:
			READ_NODE_FIELD(subquery);
			READ_NODE_FIELD(funcexpr);
			READ_NODE_FIELD(funccoltypes);
			READ_NODE_FIELD(funccoltypmods);
			READ_BYTEA_FIELD(funcuserdata);
			break;
		case RTE_VALUES:
			READ_NODE_FIELD(values_lists);
			break;
		case RTE_JOIN:
			READ_ENUM_FIELD(jointype, JoinType);
			READ_NODE_FIELD(joinaliasvars);
			break;
        case RTE_VOID:                                                  /*CDB*/
            break;
		default:
			elog(ERROR, "unrecognized RTE kind: %d",
				 (int) local_node->rtekind);
			break;
	}

	READ_BOOL_FIELD(inh);
	READ_BOOL_FIELD(inFromCl);
	READ_UINT_FIELD(requiredPerms);
	READ_OID_FIELD(checkAsUser);

	READ_BOOL_FIELD(forceDistRandom);

	READ_DONE();
}

/*
 * Greenplum Database additions for serialization support
 */
#include "nodes/plannodes.h"

static void readPlanInfo(const char ** str, Plan *local_node);
static void readScanInfo(const char ** str, Scan *local_node);
static void readJoinInfo(const char ** str, Join *local_node);
static Bitmapset *bitmapsetRead(const char ** str);


static CreateStmt *
_readCreateStmt(const char ** str)
{
	READ_LOCALS(CreateStmt);

	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(tableElts);
	READ_NODE_FIELD(inhRelations);
	READ_NODE_FIELD(constraints);
	READ_NODE_FIELD(options);
	READ_ENUM_FIELD(oncommit,OnCommitAction);
	READ_STRING_FIELD(tablespacename);
	READ_NODE_FIELD(distributedBy);
	READ_OID_FIELD(oidInfo.relOid);
	READ_OID_FIELD(oidInfo.comptypeOid);
	READ_OID_FIELD(oidInfo.toastOid);
	READ_OID_FIELD(oidInfo.toastIndexOid);
	READ_OID_FIELD(oidInfo.toastComptypeOid);
	READ_OID_FIELD(oidInfo.aosegOid);
	READ_OID_FIELD(oidInfo.aosegIndexOid);
	READ_OID_FIELD(oidInfo.aosegComptypeOid);
	READ_OID_FIELD(oidInfo.aoblkdirOid);
	READ_OID_FIELD(oidInfo.aoblkdirIndexOid);
	READ_OID_FIELD(oidInfo.aoblkdirComptypeOid);
	READ_CHAR_FIELD(relKind);
	READ_CHAR_FIELD(relStorage);
	/* policy omitted */
	/* postCreate - for analysis, QD only */
	/* deferredStmts - for analysis, QD only */
	READ_BOOL_FIELD(is_part_child);
	READ_BOOL_FIELD(is_add_part);
	READ_BOOL_FIELD(is_split_part);
	READ_OID_FIELD(ownerid);
	READ_BOOL_FIELD(buildAoBlkdir);
	READ_NODE_FIELD(attr_encodings);

	local_node->policy = NULL;

	/*
	 * Some extra checks to make sure we didn't get lost
	 * during serialization/deserialization
	 */
	Assert(local_node->relKind == RELKIND_INDEX ||
		   local_node->relKind == RELKIND_RELATION ||
		   local_node->relKind == RELKIND_SEQUENCE ||
		   local_node->relKind == RELKIND_UNCATALOGED ||
		   local_node->relKind == RELKIND_TOASTVALUE ||
		   local_node->relKind == RELKIND_VIEW ||
		   local_node->relKind == RELKIND_COMPOSITE_TYPE ||
		   local_node->relKind == RELKIND_AOSEGMENTS ||
		   local_node->relKind == RELKIND_AOBLOCKDIR);
	Assert(local_node->oncommit <= ONCOMMIT_DROP);

	READ_DONE();
}

static ColumnReferenceStorageDirective *
_readColumnReferenceStorageDirective(const char **str)
{
	READ_LOCALS(ColumnReferenceStorageDirective);

	READ_NODE_FIELD(column);
	READ_BOOL_FIELD(deflt);
	READ_NODE_FIELD(encoding);

	READ_DONE();
}


static PartitionBy *
_readPartitionBy(const char **str)
{
	READ_LOCALS(PartitionBy);

	READ_ENUM_FIELD(partType, PartitionByType);
	READ_NODE_FIELD(keys);
	READ_NODE_FIELD(keyopclass);
	READ_NODE_FIELD(partNum);
	READ_NODE_FIELD(subPart);
	READ_NODE_FIELD(partSpec);
	READ_INT_FIELD(partDepth);
	READ_INT_FIELD(partQuiet);
	READ_INT_FIELD(location);

	READ_DONE();
}

static PartitionSpec *
_readPartitionSpec(const char **str)
{
	READ_LOCALS(PartitionSpec);

	READ_NODE_FIELD(partElem);
	READ_NODE_FIELD(subSpec);
	READ_BOOL_FIELD(istemplate);
	READ_INT_FIELD(location);
	READ_NODE_FIELD(enc_clauses);

	READ_DONE();
}

static PartitionElem *
_readPartitionElem(const char **str)
{
	READ_LOCALS(PartitionElem);

	READ_NODE_FIELD(partName);
	READ_NODE_FIELD(boundSpec);
	READ_NODE_FIELD(subSpec);
	READ_BOOL_FIELD(isDefault);
	READ_NODE_FIELD(storeAttr);
	READ_INT_FIELD(partno);
	READ_LONG_FIELD(rrand);
	READ_NODE_FIELD(colencs);
	READ_INT_FIELD(location);

	READ_DONE();
}

static PartitionRangeItem *
_readPartitionRangeItem(const char **str)
{
	READ_LOCALS(PartitionRangeItem);

	READ_NODE_FIELD(partRangeVal);
	READ_ENUM_FIELD(partedge, PartitionEdgeBounding);
	READ_INT_FIELD(location);

	READ_DONE();
}

static PartitionBoundSpec *
_readPartitionBoundSpec(const char **str)
{
	READ_LOCALS(PartitionBoundSpec);

	READ_NODE_FIELD(partStart);
	READ_NODE_FIELD(partEnd);
	READ_NODE_FIELD(partEvery);
	READ_INT_FIELD(location);

	READ_DONE();
}

static PartitionValuesSpec *
_readPartitionValuesSpec(const char **str)
{
	READ_LOCALS(PartitionValuesSpec);

	READ_NODE_FIELD(partValues);
	READ_INT_FIELD(location);

	READ_DONE();
}

static Partition *
_readPartition(const char **str)
{
	READ_LOCALS(Partition);

	READ_OID_FIELD(partid);
	READ_OID_FIELD(parrelid);
	READ_CHAR_FIELD(parkind);
	READ_INT_FIELD(parlevel);
	READ_BOOL_FIELD(paristemplate);
	READ_BINARY_FIELD(parnatts, sizeof(int2));
	READ_INT_ARRAY(paratts, parnatts, int2);
	READ_OID_ARRAY(parclass, parnatts);

	READ_DONE();
}

static PartitionRule *
_readPartitionRule(const char **str)
{
	READ_LOCALS(PartitionRule);

	READ_OID_FIELD(parruleid);
	READ_OID_FIELD(paroid);
	READ_OID_FIELD(parchildrelid);
	READ_OID_FIELD(parparentoid);
	READ_BOOL_FIELD(parisdefault);
	READ_STRING_FIELD(parname);
	READ_NODE_FIELD(parrangestart);
	READ_BOOL_FIELD(parrangestartincl);
	READ_NODE_FIELD(parrangeend);
	READ_BOOL_FIELD(parrangeendincl);
	READ_NODE_FIELD(parrangeevery);
	READ_NODE_FIELD(parlistvalues);
	READ_BINARY_FIELD(parruleord, sizeof(int2));
	READ_NODE_FIELD(parreloptions);
	READ_OID_FIELD(partemplatespaceId);
	READ_NODE_FIELD(children);

	READ_DONE();
}

static PartitionNode *
_readPartitionNode(const char **str)
{
	READ_LOCALS(PartitionNode);

	READ_NODE_FIELD(part);
	READ_NODE_FIELD(default_part);
	READ_NODE_FIELD(rules);

	READ_DONE();
}

static PgPartRule *
_readPgPartRule(const char **str)
{
	READ_LOCALS(PgPartRule);

	READ_NODE_FIELD(pNode);
	READ_NODE_FIELD(topRule);
	READ_STRING_FIELD(partIdStr);
	READ_BOOL_FIELD(isName);
	READ_INT_FIELD(topRuleRank);
	READ_STRING_FIELD(relname);

	READ_DONE();
}

static SegfileMapNode *
_readSegfileMapNode(const char **str)
{
	READ_LOCALS(SegfileMapNode);

	READ_OID_FIELD(relid);
	READ_NODE_FIELD(segnos);

	READ_DONE();
}

static ResultRelSegFileInfoMapNode *
_readResultRelSegFileInfoMapNode(const char **str)
{
	READ_LOCALS(ResultRelSegFileInfoMapNode);

	READ_OID_FIELD(relid);
	READ_NODE_FIELD(segfileinfos);

	READ_DONE();
}

static SegFileSplitMapNode *
_readSegFileSplitMapNode(const char **str)
{
	READ_LOCALS(SegFileSplitMapNode);

	READ_OID_FIELD(relid);
	READ_NODE_FIELD(splits);

	READ_DONE();
}

static FileSplitNode *
_readFileSplitNode(const char **str)
{
	READ_LOCALS(FileSplitNode);

	READ_INT_FIELD(segno);
	READ_INT64_FIELD(logiceof);
	READ_INT64_FIELD(offsets);
	READ_INT64_FIELD(lengths);
	READ_DONE();
}

static ExtTableTypeDesc *
_readExtTableTypeDesc(const char ** str)
{
	READ_LOCALS(ExtTableTypeDesc);

	READ_ENUM_FIELD(exttabletype, ExtTableType);
	READ_NODE_FIELD(location_list);
	READ_NODE_FIELD(on_clause);
	READ_STRING_FIELD(command_string);

	READ_DONE();
}

static CreateExternalStmt *
_readCreateExternalStmt(const char ** str)
{
	READ_LOCALS(CreateExternalStmt);

	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(tableElts);
	READ_NODE_FIELD(exttypedesc);
	READ_STRING_FIELD(format);
	READ_NODE_FIELD(formatOpts);
	READ_BOOL_FIELD(isweb);
	READ_BOOL_FIELD(iswritable);
	READ_NODE_FIELD(sreh);
	READ_NODE_FIELD(encoding);
	READ_NODE_FIELD(distributedBy);

	READ_DONE();
}

static CreateForeignStmt *
_readCreateForeignStmt(const char ** str)
{
	READ_LOCALS(CreateForeignStmt);

	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(tableElts);
	READ_STRING_FIELD(srvname);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static FkConstraint *
_readFkConstraint(const char ** str)
{
	READ_LOCALS(FkConstraint);

	READ_STRING_FIELD(constr_name);
	READ_OID_FIELD(constrOid);
	READ_NODE_FIELD(pktable);
	READ_NODE_FIELD(fk_attrs);
	READ_NODE_FIELD(pk_attrs);
	READ_CHAR_FIELD(fk_matchtype);
	READ_CHAR_FIELD(fk_upd_action);
	READ_CHAR_FIELD(fk_del_action);
	READ_BOOL_FIELD(deferrable);
	READ_BOOL_FIELD(initdeferred);
	READ_BOOL_FIELD(skip_validation);
	READ_OID_FIELD(trig1Oid);
	READ_OID_FIELD(trig2Oid);
	READ_OID_FIELD(trig3Oid);
	READ_OID_FIELD(trig4Oid);

	READ_DONE();
}

static CreateSchemaStmt *
_readCreateSchemaStmt(const char ** str)
{
	READ_LOCALS(CreateSchemaStmt);

	READ_STRING_FIELD(schemaname);
	READ_STRING_FIELD(authid);
	local_node->schemaElts = 0;
	READ_BOOL_FIELD(istemp);
	READ_OID_FIELD(schemaOid);

	READ_DONE();
}


static CreatePLangStmt *
_readCreatePLangStmt(const char ** str)
{
	READ_LOCALS(CreatePLangStmt);

	READ_STRING_FIELD(plname);
	READ_NODE_FIELD(plhandler);
	READ_NODE_FIELD(plvalidator);
	READ_BOOL_FIELD(pltrusted);
	READ_OID_FIELD(plangOid);
	READ_OID_FIELD(plhandlerOid);
	READ_OID_FIELD(plvalidatorOid);

	READ_DONE();

}

static DropPLangStmt *
_readDropPLangStmt(const char ** str)
{
	READ_LOCALS(DropPLangStmt);

	READ_STRING_FIELD(plname);
	READ_ENUM_FIELD(behavior, DropBehavior); Assert(local_node->behavior <= DROP_CASCADE);
	READ_BOOL_FIELD(missing_ok);

	READ_DONE();

}

static CreateSeqStmt *
_readCreateSeqStmt(const char ** str)
{
	READ_LOCALS(CreateSeqStmt);
	READ_NODE_FIELD(sequence);
	READ_NODE_FIELD(options);

	READ_OID_FIELD(relOid);
	READ_OID_FIELD(comptypeOid);

	READ_DONE();
}

static AlterSeqStmt *
_readAlterSeqStmt(const char ** str)
{
	READ_LOCALS(AlterSeqStmt);
	READ_NODE_FIELD(sequence);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static ClusterStmt *
_readClusterStmt(const char ** str)
{
	READ_LOCALS(ClusterStmt);

	READ_NODE_FIELD(relation);
	READ_STRING_FIELD(indexname);
	READ_OID_FIELD(oidInfo.relOid);
	READ_OID_FIELD(oidInfo.comptypeOid);
	READ_OID_FIELD(oidInfo.toastOid);
	READ_OID_FIELD(oidInfo.toastIndexOid);
	READ_OID_FIELD(oidInfo.toastComptypeOid);
	READ_OID_FIELD(oidInfo.aosegOid);
	READ_OID_FIELD(oidInfo.aosegIndexOid);
	READ_OID_FIELD(oidInfo.aosegComptypeOid);
	READ_OID_FIELD(oidInfo.aoblkdirOid);
	READ_OID_FIELD(oidInfo.aoblkdirIndexOid);
	READ_OID_FIELD(oidInfo.aoblkdirComptypeOid);
	READ_NODE_FIELD(new_ind_oids);

	READ_DONE();
}

static CreatedbStmt *
_readCreatedbStmt(const char ** str)
{
	READ_LOCALS(CreatedbStmt);
	READ_STRING_FIELD(dbname);
	READ_NODE_FIELD(options);

	READ_OID_FIELD(dbOid);

	READ_DONE();
}

static DropdbStmt *
_readDropdbStmt(const char ** str)
{
	READ_LOCALS(DropdbStmt);

	READ_STRING_FIELD(dbname);
	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}

static CreateDomainStmt *
_readCreateDomainStmt(const char ** str)
{
	READ_LOCALS(CreateDomainStmt);

	READ_NODE_FIELD(domainname);
	READ_NODE_FIELD(typname);
	READ_NODE_FIELD(constraints);
	READ_OID_FIELD(domainOid);

	READ_DONE();
}

static AlterDomainStmt *
_readAlterDomainStmt(const char ** str)
{
	READ_LOCALS(AlterDomainStmt);

	READ_CHAR_FIELD(subtype);
	READ_NODE_FIELD(typname);
	READ_STRING_FIELD(name);
	READ_NODE_FIELD(def);
	READ_ENUM_FIELD(behavior, DropBehavior); Assert(local_node->behavior <= DROP_CASCADE);

	READ_DONE();
}

static CreateFdwStmt *
_readCreateFdwStmt(const char ** str)
{
	READ_LOCALS(CreateFdwStmt);
	
	READ_STRING_FIELD(fdwname);
	READ_NODE_FIELD(validator);
	READ_NODE_FIELD(options);
	
	READ_DONE();
}

static AlterFdwStmt *
_readAlterFdwStmt(const char ** str)
{
	READ_LOCALS(AlterFdwStmt);
	
	READ_STRING_FIELD(fdwname);
	READ_NODE_FIELD(validator);
	READ_BOOL_FIELD(change_validator);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static DropFdwStmt *
_readDropFdwStmt(const char ** str)
{
	READ_LOCALS(DropFdwStmt);
	
	READ_STRING_FIELD(fdwname);
	READ_BOOL_FIELD(missing_ok);
	READ_ENUM_FIELD(behavior, DropBehavior);

	READ_DONE();
}

static CreateForeignServerStmt *
_readCreateForeignServerStmt(const char ** str)
{
	READ_LOCALS(CreateForeignServerStmt);
	
	READ_STRING_FIELD(servername);
	READ_STRING_FIELD(servertype);
	READ_STRING_FIELD(version);
	READ_STRING_FIELD(fdwname);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static AlterForeignServerStmt *
_readAlterForeignServerStmt(const char ** str)
{
	READ_LOCALS(AlterForeignServerStmt);
	
	READ_STRING_FIELD(servername);
	READ_STRING_FIELD(version);
	READ_NODE_FIELD(options);
	READ_BOOL_FIELD(has_version);

	READ_DONE();
}

static DropForeignServerStmt *
_readDropForeignServerStmt(const char ** str)
{
	READ_LOCALS(DropForeignServerStmt);
	
	READ_STRING_FIELD(servername);
	READ_BOOL_FIELD(missing_ok);
	READ_ENUM_FIELD(behavior, DropBehavior);

	READ_DONE();
}

static CreateUserMappingStmt *
_readCreateUserMappingStmt(const char ** str)
{
	READ_LOCALS(CreateUserMappingStmt);
	
	READ_STRING_FIELD(username);
	READ_STRING_FIELD(servername);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static AlterUserMappingStmt *
_readAlterUserMappingStmt(const char ** str)
{
	READ_LOCALS(AlterUserMappingStmt);
	
	READ_STRING_FIELD(username);
	READ_STRING_FIELD(servername);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static DropUserMappingStmt *
_readDropUserMappingStmt(const char ** str)
{
	READ_LOCALS(DropUserMappingStmt);
	
	READ_STRING_FIELD(username);
	READ_STRING_FIELD(servername);
	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}

static CreateFunctionStmt *
_readCreateFunctionStmt(const char ** str)
{
	READ_LOCALS(CreateFunctionStmt);
	READ_BOOL_FIELD(replace);
	READ_NODE_FIELD(funcname);
	READ_NODE_FIELD(parameters);
	READ_NODE_FIELD(returnType);
	READ_NODE_FIELD(options);
	READ_NODE_FIELD(withClause);
	READ_OID_FIELD(funcOid);
	READ_OID_FIELD(shelltypeOid);

	READ_DONE();
}

static FunctionParameter *
_readFunctionParameter(const char ** str)
{
	READ_LOCALS(FunctionParameter);
	READ_STRING_FIELD(name);
	READ_NODE_FIELD(argType);
	READ_ENUM_FIELD(mode, FunctionParameterMode);

	READ_DONE();
}

static RemoveFuncStmt *
_readRemoveFuncStmt(const char ** str)
{
	READ_LOCALS(RemoveFuncStmt);
	READ_ENUM_FIELD(kind,ObjectType); Assert(local_node->kind <= OBJECT_VIEW);
	READ_NODE_FIELD(name);
	READ_NODE_FIELD(args);
	READ_ENUM_FIELD(behavior, DropBehavior); Assert(local_node->behavior <= DROP_CASCADE);
	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}

static AlterFunctionStmt *
_readAlterFunctionStmt(const char ** str)
{
	READ_LOCALS(AlterFunctionStmt);
	READ_NODE_FIELD(func);
	READ_NODE_FIELD(actions);

	READ_DONE();
}


static DefineStmt *
_readDefineStmt(const char ** str)
{
	READ_LOCALS(DefineStmt);
	READ_ENUM_FIELD(kind, ObjectType); Assert(local_node->kind <= OBJECT_VIEW);
	READ_BOOL_FIELD(oldstyle);
	READ_NODE_FIELD(defnames);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(definition);
	READ_OID_FIELD(newOid);
	READ_OID_FIELD(shadowOid);
	READ_BOOL_FIELD(ordered);  /* CDB */
	READ_BOOL_FIELD(trusted);  /* CDB */

	READ_DONE();

}

static CompositeTypeStmt *
_readCompositeTypeStmt(const char ** str)
{
	READ_LOCALS(CompositeTypeStmt);

	READ_NODE_FIELD(typevar);
	READ_NODE_FIELD(coldeflist);
	READ_OID_FIELD(comptypeOid);

	READ_DONE();

}

static CreateCastStmt *
_readCreateCastStmt(const char ** str)
{
	READ_LOCALS(CreateCastStmt);
	READ_NODE_FIELD(sourcetype);
	READ_NODE_FIELD(targettype);
	READ_NODE_FIELD(func);
	READ_ENUM_FIELD(context, CoercionContext);
	READ_OID_FIELD(castOid);

	READ_DONE();
}

static DropCastStmt *
_readDropCastStmt(const char ** str)
{
	READ_LOCALS(DropCastStmt);
	READ_NODE_FIELD(sourcetype);
	READ_NODE_FIELD(targettype);
	READ_ENUM_FIELD(behavior, DropBehavior); Assert(local_node->behavior <= DROP_CASCADE);
	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}

static CreateOpClassStmt *
_readCreateOpClassStmt(const char ** str)
{
	READ_LOCALS(CreateOpClassStmt);
	READ_NODE_FIELD(opclassname);
	READ_STRING_FIELD(amname);
	READ_NODE_FIELD(datatype);
	READ_NODE_FIELD(items);
	READ_BOOL_FIELD(isDefault);
	READ_OID_FIELD(opclassOid);

	READ_DONE();
}

static CreateOpClassItem *
_readCreateOpClassItem(const char ** str)
{
	READ_LOCALS(CreateOpClassItem);
	READ_INT_FIELD(itemtype);
	READ_NODE_FIELD(name);
	READ_NODE_FIELD(args);
	READ_INT_FIELD(number);
	READ_BOOL_FIELD(recheck);
	READ_NODE_FIELD(storedtype);

	READ_DONE();
}

static RemoveOpClassStmt *
_readRemoveOpClassStmt(const char ** str)
{
	READ_LOCALS(RemoveOpClassStmt);
	READ_NODE_FIELD(opclassname);
	READ_STRING_FIELD(amname);
	READ_ENUM_FIELD(behavior, DropBehavior); Assert(local_node->behavior <= DROP_CASCADE);
	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}

static CreateConversionStmt *
_readCreateConversionStmt(const char ** str)
{
	READ_LOCALS(CreateConversionStmt);
	READ_NODE_FIELD(conversion_name);
	READ_STRING_FIELD(for_encoding_name);
	READ_STRING_FIELD(to_encoding_name);
	READ_NODE_FIELD(func_name);
	READ_BOOL_FIELD(def);
	READ_OID_FIELD(convOid);

	READ_DONE();
}

static CopyStmt *
_readCopyStmt(const char ** str)
{
	READ_LOCALS(CopyStmt);

	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(attlist);
	READ_BOOL_FIELD(is_from);
	READ_STRING_FIELD(filename);
	READ_NODE_FIELD(options);
	READ_NODE_FIELD(sreh);
	READ_NODE_FIELD(partitions);
	READ_NODE_FIELD(ao_segnos);
	READ_NODE_FIELD(ao_segfileinfos);
	READ_NODE_FIELD(err_aosegnos);
	READ_NODE_FIELD(err_aosegfileinfos);
	READ_NODE_FIELD(scantable_splits);

	READ_DONE();

}
static GrantStmt *
_readGrantStmt(const char ** str)
{
	READ_LOCALS(GrantStmt);

	READ_BOOL_FIELD(is_grant);
	READ_ENUM_FIELD(objtype,GrantObjectType);
	READ_NODE_FIELD(objects);
	READ_NODE_FIELD(privileges);
	READ_NODE_FIELD(grantees);
	READ_BOOL_FIELD(grant_option);
	READ_ENUM_FIELD(behavior, DropBehavior); Assert(local_node->behavior <= DROP_CASCADE);
	READ_NODE_FIELD(cooked_privs);

	READ_DONE();
}

static PrivGrantee *
_readPrivGrantee(const char ** str)
{
	READ_LOCALS(PrivGrantee);
	READ_STRING_FIELD(rolname);

	READ_DONE();
}

static FuncWithArgs *
_readFuncWithArgs(const char ** str)
{
	READ_LOCALS(FuncWithArgs);
	READ_NODE_FIELD(funcname);
	READ_NODE_FIELD(funcargs);

	READ_DONE();
}

static GrantRoleStmt *
_readGrantRoleStmt(const char ** str)
{
	READ_LOCALS(GrantRoleStmt);
	READ_NODE_FIELD(granted_roles);
	READ_NODE_FIELD(grantee_roles);
	READ_BOOL_FIELD(is_grant);
	READ_BOOL_FIELD(admin_opt);
	READ_STRING_FIELD(grantor);
	READ_ENUM_FIELD(behavior, DropBehavior); Assert(local_node->behavior <= DROP_CASCADE);

	READ_DONE();
}

static LockStmt *
_readLockStmt(const char ** str)
{
	READ_LOCALS(LockStmt);
	READ_NODE_FIELD(relations);
	READ_INT_FIELD(mode);
	READ_BOOL_FIELD(nowait);

	READ_DONE();
}

static ConstraintsSetStmt *
_readConstraintsSetStmt(const char ** str)
{
	READ_LOCALS(ConstraintsSetStmt);
	READ_NODE_FIELD(constraints);
	READ_BOOL_FIELD(deferred);

	READ_DONE();
}

/*
 * _readPlannedStmt
 */
static PlannedStmt *
_readPlannedStmt(const char ** str)
{
	READ_LOCALS(PlannedStmt);
	READ_ENUM_FIELD(commandType, CmdType);
	READ_ENUM_FIELD(planGen, PlanGenerator);
	READ_BOOL_FIELD(canSetTag);
	READ_BOOL_FIELD(transientPlan);
	READ_NODE_FIELD(planTree);
	READ_NODE_FIELD(rtable);
	READ_NODE_FIELD(resultRelations);
	READ_NODE_FIELD(utilityStmt);
	READ_NODE_FIELD(intoClause);
	READ_NODE_FIELD(subplans);
	READ_NODE_FIELD(rewindPlanIDs);
	READ_NODE_FIELD(returningLists);
	READ_NODE_FIELD(result_partitions);
	READ_NODE_FIELD(result_aosegnos);
	READ_NODE_FIELD(result_segfileinfos);
	READ_NODE_FIELD(scantable_splits);
	READ_NODE_FIELD(into_aosegnos);
	READ_NODE_FIELD(queryPartOids);
	READ_NODE_FIELD(queryPartsMetadata);
	READ_NODE_FIELD(numSelectorsPerScanId);
	READ_NODE_FIELD(rowMarks);
	READ_NODE_FIELD(relationOids);
	READ_NODE_FIELD(invalItems);
	READ_INT_FIELD(nCrossLevelParams);
	READ_INT_FIELD(nMotionNodes);
	READ_INT_FIELD(nInitPlans);
	/* intoPolicy not serialized in outfast.c */
	READ_NODE_FIELD(sliceTable);
	
	READ_INT_FIELD(backoff_weight);
	READ_UINT64_FIELD(query_mem);

	READ_NODE_FIELD(contextdisp);

	READ_NODE_FIELD(resource);

	READ_DONE();
}


/*
 * _readPlan
 */
static Plan *
_readPlan(const char ** str)
{
	READ_LOCALS(Plan);

	readPlanInfo(str, local_node);

	READ_DONE();
}

/*
 * _readResult
 */
static Result *
_readResult(const char ** str)
{
	READ_LOCALS(Result);

	readPlanInfo(str, (Plan *)local_node);

	READ_NODE_FIELD(resconstantqual);

	READ_BOOL_FIELD(hashFilter);
	READ_NODE_FIELD(hashList);

	READ_DONE();
}

/*
 * _readRepeat
 */
static Repeat *
_readRepeat(const char ** str)
{
	READ_LOCALS(Repeat);

	readPlanInfo(str, (Plan *)local_node);

	READ_NODE_FIELD(repeatCountExpr);
	READ_UINT64_FIELD(grouping);

	READ_DONE();
}

/*
 * _readAppend
 */
static Append *
_readAppend(const char ** str)
{
	READ_LOCALS(Append);

	readPlanInfo(str, (Plan *)local_node);

	READ_NODE_FIELD(appendplans);
	READ_BOOL_FIELD(isTarget);
	READ_BOOL_FIELD(isZapped);
	READ_BOOL_FIELD(hasXslice);

	READ_DONE();
}

static Sequence *
_readSequence(const char **str)
{
	READ_LOCALS(Sequence);
	readPlanInfo(str, (Plan *)local_node);
	READ_NODE_FIELD(subplans);
	READ_DONE();
}

/*
 * _readBitmapAnd, _readBitmapOr
 */
static BitmapAnd *
_readBitmapAnd(const char ** str)
{
	READ_LOCALS(BitmapAnd);

	readPlanInfo(str, (Plan *)local_node);

	READ_NODE_FIELD(bitmapplans);

	READ_DONE();
}

static BitmapOr *
_readBitmapOr(const char ** str)
{
    READ_LOCALS(BitmapOr);

    readPlanInfo(str, (Plan *)local_node);

    READ_NODE_FIELD(bitmapplans);

    READ_DONE();
}


/*
 * _readScan
 */
static Scan *
_readScan(const char ** str)
{
	READ_LOCALS(Scan);

	readScanInfo(str, local_node);

	READ_DONE();
}

/*
 * _readSeqScan
 */
static SeqScan *
_readSeqScan(const char ** str)
{
	READ_LOCALS(SeqScan);

	readScanInfo(str, (Scan *)local_node);

	READ_DONE();
}

/*
 * _readAppendOnlyScan
 */
static AppendOnlyScan *
_readAppendOnlyScan(const char ** str)
{
	READ_LOCALS(AppendOnlyScan);

	readScanInfo(str, (Scan *)local_node);

	READ_DONE();
}

static TableScan *
_readTableScan(const char **str)
{
	READ_LOCALS(TableScan);
	readScanInfo(str, (Scan *)local_node);
	READ_DONE();
}
/*
 * _readParquetScan
 */
static ParquetScan *
_readParquetScan(const char ** str)
{
	READ_LOCALS(ParquetScan);

	readScanInfo(str, (Scan *)local_node);

	READ_DONE();
}

static DynamicTableScan *
_readDynamicTableScan(const char **str)
{
	READ_LOCALS(DynamicTableScan);
	readScanInfo(str, (Scan *)local_node);
	READ_INT_FIELD(partIndex);
	READ_INT_FIELD(partIndexPrintable);
	READ_DONE();
}

/*
 * _readExternalScan
 */
static ExternalScan *
_readExternalScan(const char ** str)
{
	READ_LOCALS(ExternalScan);

	readScanInfo(str, (Scan *)local_node);

	READ_NODE_FIELD(uriList);
	READ_NODE_FIELD(fmtOpts);
	READ_CHAR_FIELD(fmtType);
	READ_BOOL_FIELD(isMasterOnly);
	READ_INT_FIELD(rejLimit);
	READ_BOOL_FIELD(rejLimitInRows);
	READ_OID_FIELD(fmterrtbl);
	READ_NODE_FIELD(errAosegnos);
	READ_NODE_FIELD(err_aosegfileinfos);
	READ_INT_FIELD(encoding);
	READ_INT_FIELD(scancounter);

	READ_DONE();
}

static void
readLogicalIndexInfo(const char ** str, LogicalIndexInfo *local_node)
{
	READ_OID_FIELD(logicalIndexOid);
	READ_INT_FIELD(nColumns);
	READ_INT_ARRAY(indexKeys, nColumns, AttrNumber);
	READ_NODE_FIELD(indPred);
	READ_NODE_FIELD(indExprs);
	READ_BOOL_FIELD(indIsUnique);
	READ_ENUM_FIELD(indType, LogicalIndexType);
	READ_NODE_FIELD(partCons);
	READ_NODE_FIELD(defaultLevels);
}

static void
readIndexScanFields(const char ** str, IndexScan *local_node)
{
	readScanInfo(str, (Scan *)local_node);

	READ_OID_FIELD(indexid);
	READ_NODE_FIELD(indexqual);
	READ_NODE_FIELD(indexqualorig);
	READ_NODE_FIELD(indexstrategy);
	READ_NODE_FIELD(indexsubtype);
	READ_ENUM_FIELD(indexorderdir, ScanDirection);

	if (isDynamicScan(&local_node->scan))
	{
		ALLOCATE_LOCAL(local_node->logicalIndexInfo, LogicalIndexInfo, 1 /* single node allocation  */);
		readLogicalIndexInfo(str, local_node->logicalIndexInfo);
	}
}

/*
 * _readIndexScan
 */
static IndexScan *
_readIndexScan(const char ** str)
{
	READ_LOCALS(IndexScan);

	readIndexScanFields(str, local_node);

	READ_DONE();
}

static DynamicIndexScan *
_readDynamicIndexScan(const char **str)
{
	READ_LOCALS(DynamicIndexScan);

	/* DynamicIndexScan has some content from IndexScan. */
	readIndexScanFields(str, (IndexScan *)local_node);

	READ_DONE();
}

static BitmapIndexScan *
_readBitmapIndexScan(const char ** str)
{
	READ_LOCALS(BitmapIndexScan);

	/* BitmapIndexScan has some content from IndexScan. */
	readIndexScanFields(str, (IndexScan *)local_node);

	READ_DONE();
}

static BitmapHeapScan *
_readBitmapHeapScan(const char ** str)
{
	READ_LOCALS(BitmapHeapScan);

	readScanInfo(str, (Scan *)local_node);

	READ_NODE_FIELD(bitmapqualorig);

	READ_DONE();
}

static BitmapAppendOnlyScan *
_readBitmapAppendOnlyScan(const char ** str)
{
	READ_LOCALS(BitmapAppendOnlyScan);

	readScanInfo(str, (Scan *)local_node);

	READ_NODE_FIELD(bitmapqualorig);
	READ_BOOL_FIELD(isAORow);

	READ_DONE();
}

static BitmapTableScan *
_readBitmapTableScan(const char ** str)
{
	READ_LOCALS(BitmapTableScan);

	readScanInfo(str, (Scan *)local_node);

	READ_NODE_FIELD(bitmapqualorig);

	READ_DONE();
}

/*
 * _readTidScan
 */
static TidScan *
_readTidScan(const char ** str)
{
	READ_LOCALS(TidScan);

	readScanInfo(str, (Scan *)local_node);

	READ_NODE_FIELD(tidquals);

	READ_DONE();
}

/*
 * _readSubqueryScan
 */
static SubqueryScan *
_readSubqueryScan(const char ** str)
{
	READ_LOCALS(SubqueryScan);

	readScanInfo(str, (Scan *)local_node);

	READ_NODE_FIELD(subplan);
	/* Planner-only: subrtable -- don't serialize. */

	READ_DONE();
}

/*
 * _readFunctionScan
 */
static FunctionScan *
_readFunctionScan(const char ** str)
{
	READ_LOCALS(FunctionScan);

	readScanInfo(str, (Scan *)local_node);

	READ_DONE();
}

/*
 * _readValuesScan
 */
static ValuesScan *
_readValuesScan(const char ** str)
{
	READ_LOCALS(ValuesScan);

	readScanInfo(str, (Scan *)local_node);

	READ_DONE();
}

/*
 * _readJoin
 */
static Join *
_readJoin(const char ** str)
{
	READ_LOCALS(Join);

	readJoinInfo(str, (Join *)local_node);

	READ_DONE();
}

/*
 * _readNestLoop
 */
static NestLoop *
_readNestLoop(const char ** str)
{
	READ_LOCALS(NestLoop);

	readJoinInfo(str, (Join *)local_node);

    READ_BOOL_FIELD(outernotreferencedbyinner); /*CDB*/
	READ_BOOL_FIELD(shared_outer);
	READ_BOOL_FIELD(singleton_outer); /*CDB-OLAP*/

    READ_DONE();
}

/*
 * _readMergeJoin
 */
static MergeJoin *
_readMergeJoin(const char ** str)
{
	READ_LOCALS(MergeJoin);

	readJoinInfo(str, (Join *)local_node);

	READ_NODE_FIELD(mergeclauses);
	READ_BOOL_FIELD(unique_outer);

	READ_DONE();
}

/*
 * _readHashJoin
 */
static HashJoin *
_readHashJoin(const char ** str)
{
	READ_LOCALS(HashJoin);

	readJoinInfo(str, (Join *)local_node);

	READ_NODE_FIELD(hashclauses);
	READ_NODE_FIELD(hashqualclauses);

	READ_DONE();
}

/*
 * _readAgg
 */
static Agg *
_readAgg(const char ** str)
{
	READ_LOCALS(Agg);

	readPlanInfo(str, (Plan *)local_node);

	READ_ENUM_FIELD(aggstrategy, AggStrategy);
	READ_INT_FIELD(numCols);
	READ_INT_ARRAY(grpColIdx, numCols, AttrNumber);
	READ_LONG_FIELD(numGroups);
	READ_INT_FIELD(transSpace);
	READ_INT_FIELD(numNullCols);
	READ_UINT64_FIELD(inputGrouping);
	READ_UINT64_FIELD(grouping);
	READ_BOOL_FIELD(inputHasGrouping);
	READ_INT_FIELD(rollupGSTimes);
	READ_BOOL_FIELD(lastAgg);
	READ_BOOL_FIELD(streaming);

	READ_DONE();
}

/*
 * _readWindowKey
 */
static WindowKey *
_readWindowKey(const char ** str)
{
	READ_LOCALS(WindowKey);

	READ_INT_FIELD(numSortCols);
	READ_INT_ARRAY(sortColIdx, numSortCols, AttrNumber);
	READ_OID_ARRAY(sortOperators, numSortCols);
	READ_NODE_FIELD(frame);

	READ_DONE();
}

/*
 * _readWindow
 */
static Window *
_readWindow(const char ** str)
{
	READ_LOCALS(Window);

	readPlanInfo(str, (Plan *)local_node);

	READ_INT_FIELD(numPartCols);
	READ_INT_ARRAY(partColIdx, numPartCols, AttrNumber);
	READ_NODE_FIELD(windowKeys);

	READ_DONE();
}

/*
 * _readTableFunctionScan
 */
static TableFunctionScan *
_readTableFunctionScan(const char ** str)
{
	READ_LOCALS(TableFunctionScan);

	readScanInfo(str, (Scan *)local_node);

	READ_DONE();
}

/*
 * _readMaterial
 */
static Material *
_readMaterial(const char ** str)
{
	READ_LOCALS(Material);

    READ_BOOL_FIELD(cdb_strict);

	READ_ENUM_FIELD(share_type, ShareType);
	READ_INT_FIELD(share_id);
	READ_INT_FIELD(driver_slice);
	READ_INT_FIELD(nsharer);
	READ_INT_FIELD(nsharer_xslice);

    readPlanInfo(str, (Plan *)local_node);

	READ_DONE();
}

/*
 * _readShareInputScan
 */
static ShareInputScan *
_readShareInputScan(const char ** str)
{
	READ_LOCALS(ShareInputScan);

	READ_ENUM_FIELD(share_type, ShareType);
	READ_INT_FIELD(share_id);
	READ_INT_FIELD(driver_slice);

	readPlanInfo(str, (Plan *)local_node);

	READ_DONE();
}


/*
 * _readSort
 */
static Sort *
_readSort(const char ** str)
{

	READ_LOCALS(Sort);

	readPlanInfo(str, (Plan *)local_node);

	READ_INT_FIELD(numCols);

	READ_INT_ARRAY(sortColIdx, numCols, AttrNumber);

	READ_OID_ARRAY(sortOperators, numCols);

    /* CDB */
	READ_NODE_FIELD(limitOffset);
	READ_NODE_FIELD(limitCount);
	READ_BOOL_FIELD(noduplicates);

	READ_ENUM_FIELD(share_type, ShareType);
	READ_INT_FIELD(share_id);
	READ_INT_FIELD(driver_slice);
	READ_INT_FIELD(nsharer);
	READ_INT_FIELD(nsharer_xslice);

	READ_DONE();
}

/*
 * _readUnique
 */
static Unique *
_readUnique(const char ** str)
{
	READ_LOCALS(Unique);

	readPlanInfo(str, (Plan *)local_node);

	READ_INT_FIELD(numCols);

	READ_INT_ARRAY(uniqColIdx, numCols, AttrNumber);

	READ_DONE();
}

/*
 * _readSetOp
 */
static SetOp *
_readSetOp(const char ** str)
{
	READ_LOCALS(SetOp);

	readPlanInfo(str, (Plan *)local_node);

	READ_ENUM_FIELD(cmd, SetOpCmd);
	READ_INT_FIELD(numCols);

	READ_INT_ARRAY(dupColIdx, numCols, AttrNumber);

	READ_INT_FIELD(flagColIdx);

	READ_DONE();
}

/*
 * _readLimit
 */
static Limit *
_readLimit(const char ** str)
{
	READ_LOCALS(Limit);

	readPlanInfo(str, (Plan *)local_node);

	READ_NODE_FIELD(limitOffset);
	READ_NODE_FIELD(limitCount);

	READ_DONE();
}


/*
 * _readHash
 */
static Hash *
_readHash(const char ** str)
{
	READ_LOCALS(Hash);

	readPlanInfo(str, (Plan *)local_node);
    READ_BOOL_FIELD(rescannable);           /*CDB*/

	READ_DONE();
}

/*
 * _readFlow
 */
static Flow *
_readFlow(const char ** str)
{
	READ_LOCALS(Flow);

	READ_ENUM_FIELD(flotype, FlowType);
	READ_ENUM_FIELD(req_move, Movement);
	READ_ENUM_FIELD(locustype, CdbLocusType);
	READ_INT_FIELD(segindex);

	READ_INT_FIELD(numSortCols);
	READ_INT_ARRAY(sortColIdx, numSortCols, AttrNumber);
	READ_OID_ARRAY(sortOperators, numSortCols);

	READ_NODE_FIELD(hashExpr);
	READ_NODE_FIELD(flow_before_req_move);

	READ_DONE();
}

/*
 * _readMotion
 */
static Motion *
_readMotion(const char ** str)
{
	READ_LOCALS(Motion);

	READ_INT_FIELD(motionID);
	READ_ENUM_FIELD(motionType, MotionType);

	Assert(local_node->motionType == MOTIONTYPE_FIXED || local_node->motionType == MOTIONTYPE_HASH || local_node->motionType == MOTIONTYPE_EXPLICIT);

	READ_BOOL_FIELD(sendSorted);

	READ_NODE_FIELD(hashExpr);
	READ_NODE_FIELD(hashDataTypes);

	READ_INT_FIELD(numOutputSegs);
	READ_INT_ARRAY(outputSegIdx, numOutputSegs, int);

	READ_INT_FIELD(numSortCols);
	READ_INT_ARRAY(sortColIdx, numSortCols, AttrNumber);
	READ_OID_ARRAY(sortOperators, numSortCols);

	READ_INT_FIELD(segidColIdx);

	readPlanInfo(str, (Plan *)local_node);

	READ_DONE();
}

/*
 * _readDML
 */
static DML *
_readDML(const char ** str)
{
	READ_LOCALS(DML);

	READ_UINT_FIELD(scanrelid);
	READ_INT_FIELD(oidColIdx);
	READ_INT_FIELD(actionColIdx);
	READ_INT_FIELD(ctidColIdx);
	READ_INT_FIELD(tupleoidColIdx);

	readPlanInfo(str, (Plan *)local_node);

	READ_DONE();
}

/*
 * _readSplitUpdate
 */
static SplitUpdate *
_readSplitUpdate(const char ** str)
{
	READ_LOCALS(SplitUpdate);

	READ_INT_FIELD(actionColIdx);
	READ_INT_FIELD(ctidColIdx);
	READ_INT_FIELD(tupleoidColIdx);
	READ_NODE_FIELD(insertColIdx);
	READ_NODE_FIELD(deleteColIdx);

	readPlanInfo(str, (Plan *)local_node);

	READ_DONE();
}

/*
 * _readRowTrigger
 */
static RowTrigger *
_readRowTrigger(const char ** str)
{
	READ_LOCALS(RowTrigger);

	READ_INT_FIELD(relid);
	READ_INT_FIELD(eventFlags);
	READ_NODE_FIELD(oldValuesColIdx);
	READ_NODE_FIELD(newValuesColIdx);

	readPlanInfo(str, (Plan *)local_node);

	READ_DONE();
}

/*
 * _readAssertOp
 */
static AssertOp *
_readAssertOp(const char ** str)
{
	READ_LOCALS(AssertOp);

	READ_NODE_FIELD(errmessage);
	READ_INT_FIELD(errcode);

	readPlanInfo(str, (Plan *)local_node);

	READ_DONE();
}

/*
 * _readPartitionSelector
 */
static PartitionSelector *
_readPartitionSelector(const char ** str)
{
	READ_LOCALS(PartitionSelector);

	READ_INT_FIELD(relid);
	READ_INT_FIELD(nLevels);
	READ_INT_FIELD(scanId);
	READ_INT_FIELD(selectorId);
	READ_NODE_FIELD(levelEqExpressions);
	READ_NODE_FIELD(levelExpressions);
	READ_NODE_FIELD(residualPredicate);
	READ_NODE_FIELD(propagationExpression);
	READ_NODE_FIELD(printablePredicate);
	READ_BOOL_FIELD(staticSelection);
	READ_NODE_FIELD(staticPartOids);
	READ_NODE_FIELD(staticScanIds);

	readPlanInfo(str, (Plan *)local_node);

	READ_DONE();
}


/*
 * _readVacuumStmt
 */
static VacuumStmt *
_readVacuumStmt(const char ** str)
{
	READ_LOCALS(VacuumStmt);

	READ_BOOL_FIELD(vacuum);
	READ_BOOL_FIELD(full);
	READ_BOOL_FIELD(analyze);
	READ_BOOL_FIELD(verbose);
	READ_BOOL_FIELD(rootonly);
	READ_INT_FIELD(freeze_min_age);
	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(va_cols);
	READ_NODE_FIELD(expanded_relids);
	READ_NODE_FIELD(extra_oids);

	READ_DONE();
}


static CdbProcess *
_readCdbProcess(const char ** str)
{
	READ_LOCALS(CdbProcess);

	READ_STRING_FIELD(listenerAddr);
	READ_INT_FIELD(listenerPort);
	READ_INT_FIELD(pid);
	READ_INT_FIELD(contentid);

	READ_DONE();
}

static Slice *
_readSlice(const char ** str)
{
	READ_LOCALS(Slice);

	READ_INT_FIELD(sliceIndex);
	READ_INT_FIELD(rootIndex);
	READ_ENUM_FIELD(gangType, GangType);
	Assert(local_node->gangType <= GANGTYPE_PRIMARY_WRITER);
	READ_INT_FIELD(gangSize);
	READ_INT_FIELD(numGangMembersToBeActive);
	READ_BOOL_FIELD(directDispatch.isDirectDispatch);
	READ_NODE_FIELD(directDispatch.contentIds); /* List of int index */
	READ_INT_FIELD(primary_gang_id);
	READ_INT_FIELD(parentIndex); /* List of int index */
	READ_NODE_FIELD(children); /* List of int index */
	READ_NODE_FIELD(primaryProcesses); /* List of (CDBProcess *) */

	READ_DONE();
}

static SliceTable *
_readSliceTable(const char ** str)
{
	READ_LOCALS(SliceTable);

	READ_INT_FIELD(nMotions);
	READ_INT_FIELD(nInitPlans);
	READ_INT_FIELD(localSlice);
	READ_NODE_FIELD(slices); /* List of Slice* */
    READ_BOOL_FIELD(doInstrument);
	READ_INT_FIELD(ic_instance_id);

	READ_DONE();
}



static VariableResetStmt *
_readVariableResetStmt(const char ** str)
{
	READ_LOCALS(VariableResetStmt);
	READ_STRING_FIELD(name);

	READ_DONE();
}


void readScanInfo(const char ** str, Scan *local_node)
{
	readPlanInfo(str,(Plan *)local_node);

	READ_UINT_FIELD(scanrelid);

	READ_INT_FIELD(partIndex);
	READ_INT_FIELD(partIndexPrintable);
}

void readPlanInfo(const char ** str, Plan *local_node)
{

	READ_INT_FIELD(plan_node_id);
	READ_INT_FIELD(plan_parent_node_id);
	READ_FLOAT_FIELD(startup_cost);
	READ_FLOAT_FIELD(total_cost);
	READ_FLOAT_FIELD(plan_rows);
	READ_INT_FIELD(plan_width);
	READ_NODE_FIELD(targetlist);
	READ_NODE_FIELD(qual);
	READ_BITMAPSET_FIELD(extParam);
	READ_BITMAPSET_FIELD(allParam);
	READ_INT_FIELD(nParamExec);
	
	READ_NODE_FIELD(flow);
	READ_INT_FIELD(dispatch);
	READ_BOOL_FIELD(directDispatch.isDirectDispatch);
	READ_NODE_FIELD(directDispatch.contentIds);
	
	READ_INT_FIELD(nMotionNodes);
	READ_INT_FIELD(nInitPlans);
	READ_NODE_FIELD(sliceTable);

    READ_NODE_FIELD(lefttree);
    READ_NODE_FIELD(righttree);
    READ_NODE_FIELD(initPlan);
    
    READ_UINT64_FIELD(operatorMemKB);
}

void readJoinInfo(const char ** str, Join *local_node)
{


	readPlanInfo(str,(Plan *)local_node);

	READ_BOOL_FIELD(prefetch_inner);

	READ_ENUM_FIELD(jointype, JoinType);
	READ_NODE_FIELD(joinqual);
}

Bitmapset  *
bitmapsetRead(const char ** str)
{


	Bitmapset *bms = NULL;
	int nwords;
	int i;

	memcpy(&nwords, *str, sizeof(int)); (*str)+=sizeof(int);
	if (nwords==0)
		return bms;

	bms = palloc(sizeof(int)+nwords*sizeof(bitmapword));
	bms->nwords = nwords;
	for (i = 0; i < nwords; i++)
	{
		memcpy(&bms->words[i], *str, sizeof(bitmapword)); (*str)+=sizeof(bitmapword);
	}




	return bms;
}

static CreateTrigStmt *
_readCreateTrigStmt(const char ** str)
{
	READ_LOCALS(CreateTrigStmt);

	READ_STRING_FIELD(trigname);
	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(funcname);
	READ_NODE_FIELD(args);
	READ_BOOL_FIELD(before);
	READ_BOOL_FIELD(row);
	{ int slen;
		memcpy(&slen, *str, sizeof(int));
		(*str)+=sizeof(int);
		memcpy(local_node->actions,*str,slen);
		(*str)+=slen; }
	READ_BOOL_FIELD(isconstraint);
	READ_BOOL_FIELD(deferrable);
	READ_BOOL_FIELD(initdeferred);
	READ_NODE_FIELD(constrrel);
	READ_OID_FIELD(trigOid);

	READ_DONE();

}

static CreateFileSpaceStmt *
_readCreateFileSpaceStmt(const char ** str)
{
	READ_LOCALS(CreateFileSpaceStmt);

	READ_STRING_FIELD(filespacename);
	READ_STRING_FIELD(owner);
	READ_STRING_FIELD(fsysname);
	READ_NODE_FIELD(location);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static CreateTableSpaceStmt *
_readCreateTableSpaceStmt(const char ** str)
{
	READ_LOCALS(CreateTableSpaceStmt);

	READ_STRING_FIELD(tablespacename);
	READ_STRING_FIELD(owner);
	READ_STRING_FIELD(filespacename);
	READ_OID_FIELD(tsoid);

	READ_DONE();
}


static CreateQueueStmt *
_readCreateQueueStmt(const char ** str)
{
	READ_LOCALS(CreateQueueStmt);

	READ_STRING_FIELD(queue);
	READ_NODE_FIELD(options);
	READ_OID_FIELD(queueOid);
	READ_NODE_FIELD(optids);

	READ_DONE();
}
static AlterQueueStmt *
_readAlterQueueStmt(const char ** str)
{
	READ_LOCALS(AlterQueueStmt);

	READ_STRING_FIELD(queue);
	READ_NODE_FIELD(options);
	READ_NODE_FIELD(optids);

	READ_DONE();
}
static DropQueueStmt *
_readDropQueueStmt(const char ** str)
{
	READ_LOCALS(DropQueueStmt);

	READ_STRING_FIELD(queue);

	READ_DONE();
}

static CommentStmt *
_readCommentStmt(const char ** str)
{
	READ_LOCALS(CommentStmt);

	READ_ENUM_FIELD(objtype, ObjectType);
	READ_NODE_FIELD(objname);
	READ_NODE_FIELD(objargs);
	READ_STRING_FIELD(comment);

	READ_DONE();
}

static TableValueExpr *
_readTableValueExpr(const char **str)
{
	READ_LOCALS(TableValueExpr);

	READ_NODE_FIELD(subquery);

	READ_DONE();
}

static AlterTypeStmt *
_readAlterTypeStmt(const char **str)
{
	READ_LOCALS(AlterTypeStmt);

	READ_NODE_FIELD(typname);
	READ_NODE_FIELD(encoding);

	READ_DONE();
}

static Node *
_readValue(const char ** str, NodeTag nt)
{
	Node * result = NULL;
	if (nt == T_Integer)
	{
		long ival;
		memcpy(&ival, *str, sizeof(long)); (*str)+=sizeof(long);
		result = (Node *) makeInteger(ival);
	}
	else if (nt == T_Null)
	{
		Value *val = makeNode(Value);
		val->type = T_Null;
		result = (Node *)val;
	}
	else
	{
		int slen;
		char * nn = NULL;
		memcpy(&slen, *str, sizeof(int));
		(*str)+=sizeof(int);

		/*
		 * For the String case we want to create an empty string if slen is
		 * equal to zero, since otherwise we'll set the string to NULL, which
		 * has a different meaning and the NULL case is handed above.
		 */
		if (slen > 0 || nt == T_String)
		{
		    nn = palloc(slen + 1);

			if (slen > 0)
			    memcpy(nn, *str, slen);

		    (*str) += (slen);
			nn[slen] = '\0';
		}

		if (nt == T_Float)
			result = (Node *) makeFloat(nn);
		else if (nt == T_String)
			result = (Node *) makeString(nn);
		else if (nt == T_BitString)
			result = (Node *) makeBitString(nn);
		else
			elog(ERROR, "unknown Value node type %i", nt);
	}

	return result;

}

static QueryContextInfo *
_readQueryContextInfo(const char **str)
{
    READ_LOCALS(QueryContextInfo);

    READ_BOOL_FIELD(useFile);

    if (local_node->useFile)
    {
        READ_STRING_FIELD(sharedPath);
        InitQueryContextInfoFromFile(local_node);
    }
    else
    {
        READ_INT_FIELD(size);
        local_node->buffer = palloc(local_node->size);
        memcpy(local_node->buffer, *str, local_node->size);
        *str += local_node->size;
    }

    READ_BOOL_FIELD(finalized);
    Assert(local_node->finalized);
    READ_DONE();
}

static SharedStorageOpStmt *
_readSharedStorageOpStmt(const char **str)
{
	READ_LOCALS(SharedStorageOpStmt);

	READ_ENUM_FIELD(op, SharedStorageOp);
	READ_INT_FIELD(numTasks);

	local_node->segmentFileNum = palloc(sizeof(int) * local_node->numTasks);
	local_node->relationName = palloc(sizeof(char *) * local_node->numTasks);
//	local_node->contentid = palloc(sizeof(int) * local_node->numTasks);
	local_node->relFileNode =
			palloc(sizeof(RelFileNode) * local_node->numTasks);

	int i;
	for (i = 0; i < local_node->numTasks; ++i)
	{
		READ_INT_FIELD(segmentFileNum[i]);
	}

//	for (i = 0; i < local_node->numTasks; ++i)
//	{
//		READ_INT_FIELD(contentid[i]);
//	}

	for (i = 0; i < local_node->numTasks; ++i)
	{
		READ_RELFILENODE_FIELD(relFileNode[i]);
	}

	for (i = 0; i < local_node->numTasks; ++i)
	{
		READ_STRING_FIELD(relationName[i]);
	}
	READ_DONE();
}

static ResultRelSegFileInfo *
_readResultRelSegFileInfo(const char **str)
{
	READ_LOCALS(ResultRelSegFileInfo);

	READ_INT_FIELD(segno);
	READ_UINT64_FIELD(varblock);
	READ_UINT64_FIELD(tupcount);
	READ_INT_FIELD(numfiles);

	if (local_node->numfiles > 0)
	{
		int i;
		local_node->eof = palloc(sizeof(int64) * local_node->numfiles);
		local_node->uncompressed_eof = palloc(sizeof(int64) * local_node->numfiles);

		for(i = 0; i < local_node->numfiles; i++)
		{
			READ_UINT64_FIELD(eof[i]);
			READ_UINT64_FIELD(uncompressed_eof[i]);
		}
	}
	READ_DONE();
}

static QueryResource *
_readQueryResource(const char **str)
{
	READ_LOCALS(QueryResource);

	// READ_ENUM_FIELD(life, QueryResourceLife);
	READ_INT_FIELD(resource_id);
	READ_UINT_FIELD(segment_memory_mb);
	READ_FLOAT_FIELD(segment_vcore);
	READ_INT_FIELD(numSegments);
	READ_INT_ARRAY(segment_vcore_agg, numSegments, int);
	READ_INT_ARRAY(segment_vcore_writer, numSegments, int);
	READ_INT64_FIELD(master_start_time);

	READ_DONE();
}

void *
readNodeBinary(const char ** str)
{
	void	   *return_value;
	NodeTag 	nt;
	int16       ntt;

	if (str==NULL || *str==NULL)
		return NULL;

	memcpy(&ntt,*str,sizeof(int16));
	(*str)+=sizeof(int16);
	nt = (NodeTag) ntt;

	if (nt==0)
		return NULL;

	if (nt == T_List || nt == T_IntList || nt == T_OidList)
	{
		List	   *l = NIL;
		int listsize = 0;
		int i;

		memcpy(&listsize,*str,sizeof(int));
		(*str)+=sizeof(int);

		if (nt == T_IntList)
		{
			int val;
			for (i = 0; i < listsize; i++)
			{
				memcpy(&val,*str,sizeof(int));
				(*str)+=sizeof(int);
				l = lappend_int(l, val);
			}
		}
		else if (nt == T_OidList)
		{
			Oid val;
			for (i = 0; i < listsize; i++)
			{
				memcpy(&val,*str,sizeof(Oid));
				(*str)+=sizeof(Oid);
				l = lappend_oid(l, val);
			}
		}
		else
		{

			for (i = 0; i < listsize; i++)
			{
				l = lappend(l, readNodeBinary(str));
			}
		}
		Assert(l->length==listsize);

		return l;
	}

	if (nt == T_Integer || nt == T_Float || nt == T_String ||
	   	nt == T_BitString || nt == T_Null)
	{
		return _readValue(str, nt);
	}

	switch(nt)
	{
			case T_PlannedStmt:
				return_value = _readPlannedStmt(str);
				break;
			case T_Plan:
					return_value = _readPlan(str);
					break;
			case T_Result:
				return_value = _readResult(str);
				break;
			case T_Repeat:
				return_value = _readRepeat(str);
				break;
			case T_Append:
				return_value = _readAppend(str);
				break;
			case T_Sequence:
				return_value = _readSequence(str);
				break;
			case T_BitmapAnd:
				return_value = _readBitmapAnd(str);
				break;
			case T_BitmapOr:
				return_value = _readBitmapOr(str);
				break;
			case T_Scan:
				return_value = _readScan(str);
				break;
			case T_SeqScan:
				return_value = _readSeqScan(str);
				break;
			case T_AppendOnlyScan:
				return_value = _readAppendOnlyScan(str);
				break;
			case T_TableScan:
				return_value = _readTableScan(str);
				break;
			case T_DynamicTableScan:
				return_value = _readDynamicTableScan(str);
				break;
			case T_ParquetScan:
				return_value = _readParquetScan(str);
				break;
			case T_ExternalScan:
				return_value = _readExternalScan(str);
				break;
			case T_IndexScan:
				return_value = _readIndexScan(str);
				break;
			case T_DynamicIndexScan:
				return_value = _readDynamicIndexScan(str);
				break;
			case T_BitmapIndexScan:
				return_value = _readBitmapIndexScan(str);
				break;
			case T_BitmapHeapScan:
				return_value = _readBitmapHeapScan(str);
				break;
			case T_BitmapAppendOnlyScan:
				return_value = _readBitmapAppendOnlyScan(str);
				break;
			case T_BitmapTableScan:
				return_value = _readBitmapTableScan(str);
				break;
			case T_TidScan:
				return_value = _readTidScan(str);
				break;
			case T_SubqueryScan:
				return_value = _readSubqueryScan(str);
				break;
			case T_FunctionScan:
				return_value = _readFunctionScan(str);
				break;
			case T_ValuesScan:
				return_value = _readValuesScan(str);
				break;
			case T_Join:
				return_value = _readJoin(str);
				break;
			case T_NestLoop:
				return_value = _readNestLoop(str);
				break;
			case T_MergeJoin:
				return_value = _readMergeJoin(str);
				break;
			case T_HashJoin:
				return_value = _readHashJoin(str);
				break;
			case T_Agg:
				return_value = _readAgg(str);
				break;
			case T_WindowKey:
				return_value = _readWindowKey(str);
				break;
			case T_Window:
				return_value = _readWindow(str);
				break;
			case T_TableFunctionScan:
				return_value = _readTableFunctionScan(str);
				break;
			case T_Material:
				return_value = _readMaterial(str);
				break;
			case T_ShareInputScan:
				return_value = _readShareInputScan(str);
				break;
			case T_Sort:
				return_value = _readSort(str);
				break;
			case T_Unique:
				return_value = _readUnique(str);
				break;
			case T_SetOp:
				return_value = _readSetOp(str);
				break;
			case T_Limit:
				return_value = _readLimit(str);
				break;
			case T_Hash:
				return_value = _readHash(str);
				break;
			case T_Motion:
				return_value = _readMotion(str);
				break;
			case T_DML:
				return_value = _readDML(str);
				break;
			case T_SplitUpdate:
				return_value = _readSplitUpdate(str);
				break;
			case T_RowTrigger:
				return_value = _readRowTrigger(str);
				break;
			case T_AssertOp:
				return_value = _readAssertOp(str);
				break;
			case T_PartitionSelector:
				return_value = _readPartitionSelector(str);
				break;
			case T_Alias:
				return_value = _readAlias(str);
				break;
			case T_RangeVar:
				return_value = _readRangeVar(str);
				break;
			case T_IntoClause:
				return_value = _readIntoClause(str);
				break;
			case T_Var:
				return_value = _readVar(str);
				break;
			case T_Const:
				return_value = _readConst(str);
				break;
			case T_Param:
				return_value = _readParam(str);
				break;
			case T_Aggref:
				return_value = _readAggref(str);
				break;
			case T_AggOrder:
				return_value = _readAggOrder(str);
				break;
			case T_WindowRef:
				return_value = _readWindowRef(str);
				break;
			case T_ArrayRef:
				return_value = _readArrayRef(str);
				break;
			case T_FuncExpr:
				return_value = _readFuncExpr(str);
				break;
			case T_OpExpr:
				return_value = _readOpExpr(str);
				break;
			case T_DistinctExpr:
				return_value = _readDistinctExpr(str);
				break;
			case T_ScalarArrayOpExpr:
				return_value = _readScalarArrayOpExpr(str);
				break;
			case T_BoolExpr:
				return_value = _readBoolExpr(str);
				break;
			case T_SubLink:
				return_value = _readSubLink(str);
				break;
			case T_SubPlan:
				return_value = _readSubPlan(str);
				break;
			case T_FieldSelect:
				return_value = _readFieldSelect(str);
				break;
			case T_FieldStore:
				return_value = _readFieldStore(str);
				break;
			case T_RelabelType:
				return_value = _readRelabelType(str);
				break;
			case T_ConvertRowtypeExpr:
				return_value = _readConvertRowtypeExpr(str);
				break;
			case T_CaseExpr:
				return_value = _readCaseExpr(str);
				break;
			case T_CaseWhen:
				return_value = _readCaseWhen(str);
				break;
			case T_CaseTestExpr:
				return_value = _readCaseTestExpr(str);
				break;
			case T_ArrayExpr:
				return_value = _readArrayExpr(str);
				break;
			case T_RowExpr:
				return_value = _readRowExpr(str);
				break;
			case T_RowCompareExpr:
				return_value = _readRowCompareExpr(str);
				break;
			case T_CoalesceExpr:
				return_value = _readCoalesceExpr(str);
				break;
			case T_MinMaxExpr:
				return_value = _readMinMaxExpr(str);
				break;
			case T_NullIfExpr:
				return_value = _readNullIfExpr(str);
				break;
			case T_NullTest:
				return_value = _readNullTest(str);
				break;
			case T_BooleanTest:
				return_value = _readBooleanTest(str);
				break;
			case T_CoerceToDomain:
				return_value = _readCoerceToDomain(str);
				break;
			case T_CoerceToDomainValue:
				return_value = _readCoerceToDomainValue(str);
				break;
			case T_SetToDefault:
				return_value = _readSetToDefault(str);
				break;
			case T_CurrentOfExpr:
				return_value = _readCurrentOfExpr(str);
				break;
			case T_TargetEntry:
				return_value = _readTargetEntry(str);
				break;
			case T_RangeTblRef:
				return_value = _readRangeTblRef(str);
				break;
			case T_JoinExpr:
				return_value = _readJoinExpr(str);
				break;
			case T_FromExpr:
				return_value = _readFromExpr(str);
				break;
			case T_Flow:
				return_value = _readFlow(str);
				break;
			case T_GrantStmt:
				return_value = _readGrantStmt(str);
				break;
			case T_PrivGrantee:
				return_value = _readPrivGrantee(str);
				break;
			case T_FuncWithArgs:
				return_value = _readFuncWithArgs(str);
				break;
			case T_GrantRoleStmt:
				return_value = _readGrantRoleStmt(str);
				break;
			case T_LockStmt:
				return_value = _readLockStmt(str);
				break;

			case T_CreateStmt:
				return_value = _readCreateStmt(str);
				break;
			case T_ColumnReferenceStorageDirective:
				return_value = _readColumnReferenceStorageDirective(str);
				break;
			case T_PartitionBy:
				return_value = _readPartitionBy(str);
				break;
			case T_PartitionElem:
				return_value = _readPartitionElem(str);
				break;
			case T_PartitionRangeItem:
				return_value = _readPartitionRangeItem(str);
				break;
			case T_PartitionBoundSpec:
				return_value = _readPartitionBoundSpec(str);
				break;
			case T_PartitionSpec:
				return_value = _readPartitionSpec(str);
				break;
			case T_PartitionValuesSpec:
				return_value = _readPartitionValuesSpec(str);
				break;
			case T_Partition:
				return_value = _readPartition(str);
				break;
			case T_PartitionNode:
				return_value = _readPartitionNode(str);
				break;
			case T_PgPartRule:
				return_value = _readPgPartRule(str);
				break;
			case T_PartitionRule:
				return_value = _readPartitionRule(str);
				break;
			case T_SegfileMapNode:
				return_value = _readSegfileMapNode(str);
				break;
			case T_ResultRelSegFileInfoMapNode:
				return_value = _readResultRelSegFileInfoMapNode(str);
				break;
			case T_SegFileSplitMapNode:
				return_value = _readSegFileSplitMapNode(str);
				break;
			case T_FileSplitNode:
				return_value = _readFileSplitNode(str);
				break;

			case T_ExtTableTypeDesc:
				return_value = _readExtTableTypeDesc(str);
				break;
			case T_CreateExternalStmt:
				return_value = _readCreateExternalStmt(str);
				break;
			case T_CreateForeignStmt:
				return_value = _readCreateForeignStmt(str);
				break;				
			case T_IndexStmt:
				return_value = _readIndexStmt(str);
				break;
			case T_ReindexStmt:
				return_value = _readReindexStmt(str);
				break;

			case T_ConstraintsSetStmt:
				return_value = _readConstraintsSetStmt(str);
				break;

			case T_CreateFunctionStmt:
				return_value = _readCreateFunctionStmt(str);
				break;
			case T_FunctionParameter:
				return_value = _readFunctionParameter(str);
				break;
			case T_RemoveFuncStmt:
				return_value = _readRemoveFuncStmt(str);
				break;
			case T_AlterFunctionStmt:
				return_value = _readAlterFunctionStmt(str);
				break;

			case T_DefineStmt:
				return_value = _readDefineStmt(str);
				break;

			case T_CompositeTypeStmt:
				return_value = _readCompositeTypeStmt(str);
				break;
			case T_CreateCastStmt:
				return_value = _readCreateCastStmt(str);
				break;
			case T_DropCastStmt:
				return_value = _readDropCastStmt(str);
				break;
			case T_CreateOpClassStmt:
				return_value = _readCreateOpClassStmt(str);
				break;
			case T_CreateOpClassItem:
				return_value = _readCreateOpClassItem(str);
				break;
			case T_RemoveOpClassStmt:
				return_value = _readRemoveOpClassStmt(str);
				break;
			case T_CreateConversionStmt:
				return_value = _readCreateConversionStmt(str);
				break;


			case T_ViewStmt:
				return_value = _readViewStmt(str);
				break;
			case T_RuleStmt:
				return_value = _readRuleStmt(str);
				break;
			case T_DropStmt:
				return_value = _readDropStmt(str);
				break;
			case T_DropPropertyStmt:
				return_value = _readDropPropertyStmt(str);
				break;

			case T_DropOwnedStmt:
				return_value = _readDropOwnedStmt(str);
				break;
			case T_ReassignOwnedStmt:
				return_value = _readReassignOwnedStmt(str);
				break;

			case T_TruncateStmt:
				return_value = _readTruncateStmt(str);
				break;

			case T_AlterTableStmt:
				return_value = _readAlterTableStmt(str);
				break;
			case T_AlterTableCmd:
				return_value = _readAlterTableCmd(str);
				break;
			case T_InheritPartitionCmd:
				return_value = _readInheritPartitionCmd(str);
				break;
			case T_AlterPartitionCmd:
				return_value = _readAlterPartitionCmd(str);
				break;
			case T_AlterPartitionId:
				return_value = _readAlterPartitionId(str);
				break;
			case T_AlterRewriteTableInfo:
				return_value = _readAlterRewriteTableInfo(str);
				break;
			case T_AlterRewriteNewConstraint:
				return_value = _readAlterRewriteNewConstraint(str);
				break;
			case T_AlterRewriteNewColumnValue:
				return_value = _readAlterRewriteNewColumnValue(str);
				break;

			case T_CreateRoleStmt:
				return_value = _readCreateRoleStmt(str);
				break;
			case T_DropRoleStmt:
				return_value = _readDropRoleStmt(str);
				break;
			case T_AlterRoleStmt:
				return_value = _readAlterRoleStmt(str);
				break;
			case T_AlterRoleSetStmt:
				return_value = _readAlterRoleSetStmt(str);
				break;

			case T_AlterObjectSchemaStmt:
				return_value = _readAlterObjectSchemaStmt(str);
				break;

			case T_AlterOwnerStmt:
				return_value = _readAlterOwnerStmt(str);
				break;

			case T_RenameStmt:
				return_value = _readRenameStmt(str);
				break;

			case T_CreateSeqStmt:
				return_value = _readCreateSeqStmt(str);
				break;
			case T_AlterSeqStmt:
				return_value = _readAlterSeqStmt(str);
				break;
			case T_ClusterStmt:
				return_value = _readClusterStmt(str);
				break;
			case T_CreatedbStmt:
				return_value = _readCreatedbStmt(str);
				break;
			case T_DropdbStmt:
				return_value = _readDropdbStmt(str);
				break;
			case T_CreateDomainStmt:
				return_value = _readCreateDomainStmt(str);
				break;
			case T_AlterDomainStmt:
				return_value = _readAlterDomainStmt(str);
				break;

			case T_CreateFdwStmt:
				return_value = _readCreateFdwStmt(str);
				break;
			case T_AlterFdwStmt:
				return_value = _readAlterFdwStmt(str);
				break;
			case T_DropFdwStmt:
				return_value = _readDropFdwStmt(str);
				break;
			case T_CreateForeignServerStmt:
				return_value = _readCreateForeignServerStmt(str);
				break;
			case T_AlterForeignServerStmt:
				return_value = _readAlterForeignServerStmt(str);
				break;
			case T_DropForeignServerStmt:
				return_value = _readDropForeignServerStmt(str);
				break;
			case T_CreateUserMappingStmt:
				return_value = _readCreateUserMappingStmt(str);
				break;
			case T_AlterUserMappingStmt:
				return_value = _readAlterUserMappingStmt(str);
				break;
			case T_DropUserMappingStmt:
				return_value = _readDropUserMappingStmt(str);
				break;
			case T_NotifyStmt:
				return_value = _readNotifyStmt(str);
				break;
			case T_DeclareCursorStmt:
				return_value = _readDeclareCursorStmt(str);
				break;

			case T_SingleRowErrorDesc:
				return_value = _readSingleRowErrorDesc(str);
				break;
			case T_CopyStmt:
				return_value = _readCopyStmt(str);
				break;
			case T_ColumnDef:
				return_value = _readColumnDef(str);
				break;
			case T_TypeName:
				return_value = _readTypeName(str);
				break;
			case T_TypeCast:
				return_value = _readTypeCast(str);
				break;
			case T_IndexElem:
				return_value = _readIndexElem(str);
				break;
			case T_Query:
				return_value = _readQuery(str);
				break;
			case T_SortClause:
				return_value = _readSortClause(str);
				break;
			case T_GroupClause:
				return_value = _readGroupClause(str);
				break;
			case T_GroupingClause:
				return_value = _readGroupingClause(str);
				break;
			case T_GroupingFunc:
				return_value = _readGroupingFunc(str);
				break;
			case T_Grouping:
				return_value = _readGrouping(str);
				break;
			case T_GroupId:
				return_value = _readGroupId(str);
				break;
			case T_WindowSpecParse:
				return_value = _readWindowSpecParse(str);
				break;
			case T_WindowSpec:
				return_value = _readWindowSpec(str);
				break;
			case T_WindowFrame:
				return_value = _readWindowFrame(str);
				break;
			case T_WindowFrameEdge:
				return_value = _readWindowFrameEdge(str);
				break;
			case T_PercentileExpr:
				return_value = _readPercentileExpr(str);
				break;
			case T_DMLActionExpr:
				return_value = _readDMLActionExpr(str);
				break;
			case T_PartOidExpr:
				return_value = _readPartOidExpr(str);
				break;
			case T_PartDefaultExpr:
				return_value = _readPartDefaultExpr(str);
				break;
			case T_PartBoundExpr:
				return_value = _readPartBoundExpr(str);
				break;
			case T_PartBoundInclusionExpr:
				return_value = _readPartBoundInclusionExpr(str);
				break;
			case T_PartBoundOpenExpr:
				return_value = _readPartBoundOpenExpr(str);
				break;
			case T_RowMarkClause:
				return_value = _readRowMarkClause(str);
				break;
			case T_WithClause:
				return_value = _readWithClause(str);
				break;
			case T_CommonTableExpr:
				return_value = _readCommonTableExpr(str);
				break;
			case T_SetOperationStmt:
				return_value = _readSetOperationStmt(str);
				break;
			case T_RangeTblEntry:
				return_value = _readRangeTblEntry(str);
				break;
			case T_A_Expr:
				return_value = _readAExpr(str);
				break;
			case T_ColumnRef:
				return_value = _readColumnRef(str);
				break;
			case T_A_Const:
				return_value = _readAConst(str);
				break;
			case T_A_Indices:
				return_value = _readA_Indices(str);
				break;
			case T_A_Indirection:
				return_value = _readA_Indirection(str);
				break;
			case T_Constraint:
				return_value = _readConstraint(str);
				break;
			case T_FkConstraint:
				return_value = _readFkConstraint(str);
				break;
			case T_FuncCall:
				return_value = _readFuncCall(str);
				break;
			case T_DefElem:
				return_value = _readDefElem(str);
				break;
			case T_CreateSchemaStmt:
				return_value = _readCreateSchemaStmt(str);
				break;
			case T_CreatePLangStmt:
				return_value = _readCreatePLangStmt(str);
				break;
			case T_DropPLangStmt:
				return_value = _readDropPLangStmt(str);
				break;
			case T_VacuumStmt:
				return_value = _readVacuumStmt(str);
				break;
			case T_CdbProcess:
				return_value = _readCdbProcess(str);
				break;
			case T_Slice:
				return_value = _readSlice(str);
				break;
			case T_SliceTable:
				return_value = _readSliceTable(str);
				break;
			case T_VariableResetStmt:
				return_value = _readVariableResetStmt(str);
				break;
			case T_CreateTrigStmt:
				return_value = _readCreateTrigStmt(str);
				break;

			case T_CreateFileSpaceStmt:
				return_value = _readCreateFileSpaceStmt(str);
				break;

			case T_CreateTableSpaceStmt:
				return_value = _readCreateTableSpaceStmt(str);
				break;

			case T_CreateQueueStmt:
				return_value = _readCreateQueueStmt(str);
				break;
			case T_AlterQueueStmt:
				return_value = _readAlterQueueStmt(str);
				break;
			case T_DropQueueStmt:
				return_value = _readDropQueueStmt(str);
				break;

			case T_CommentStmt:
				return_value = _readCommentStmt(str);
				break;
			case T_DenyLoginInterval:
				return_value = _readDenyLoginInterval(str);
				break;
			case T_DenyLoginPoint:
				return_value = _readDenyLoginPoint(str);
				break;

			case T_TableValueExpr:
				return_value = _readTableValueExpr(str);
				break;

			case T_AlterTypeStmt:
				return_value = _readAlterTypeStmt(str);
				break;

		case T_QueryContextInfo:
		    return_value = _readQueryContextInfo(str);
		    break;

		case T_SharedStorageOpStmt:
			return_value = _readSharedStorageOpStmt(str);
			break;

		case T_ResultRelSegFileInfo:
			return_value = _readResultRelSegFileInfo(str);
			break;

		case T_QueryResource:
			return_value = _readQueryResource(str);
			break;

		default:
			return_value = NULL; /* keep the compiler silent */
			elog(ERROR, "could not deserialize unrecognized node type: %d",
					 (int) nt);

			break;
	}

	return (Node *)return_value;
}

Node *
readNodeFromBinaryString(const char * str, int len __attribute__((unused)))
{
	Node * node;
	int16 tg = 0;

	node = readNodeBinary(&str);

	memcpy(&tg, str, sizeof(int16));
	if (tg != (int16)0xDEAD)
		elog(ERROR,"Deserialization lost sync.");

	return node;

}
/*
 * readDatum
 *
 * Given a binary string representation of a constant, recreate the appropriate
 * Datum.  The string representation embeds length info, but not byValue,
 * so we must be told that.
 */
static Datum
readDatum(const char ** str, bool typbyval)
{
	Size		length;

	Datum		res;
	char	   *s;

	if (typbyval)
	{
		memcpy(&res, *str, sizeof(Datum)); (*str)+=sizeof(Datum);
	}
	else
	{
		memcpy(&length, *str, sizeof(Size)); (*str)+=sizeof(Size);
	  	if (length <= 0)
			res = 0;
		else
		{
			s = (char *) palloc(length+1);
			memcpy(s, *str, length); (*str)+=length;
			s[length]='\0';
			res = PointerGetDatum(s);
		}

	}

	return res;
}
