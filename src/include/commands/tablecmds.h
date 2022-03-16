/*-------------------------------------------------------------------------
 *
 * tablecmds.h
 *	  prototypes for tablecmds.c.
 *
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/commands/tablecmds.h,v 1.31.2.1 2007/05/11 20:18:17 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef TABLECMDS_H
#define TABLECMDS_H

#include <json-c/json.h>
#include "access/attnum.h"
#include "catalog/gp_policy.h"
#include "catalog/pg_exttable.h"
#include "catalog/index.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/relation.h"
#include "parser/parse_node.h"
#include "magma/cwrapper/magma-client-c.h"

/* Struct describing one new constraint to check in ALTER Phase 3 scan.
 *
 * Note: new NOT NULL constraints are handled differently.
 * Also note: This structure is shared only to allow colloboration with
 * partitioning-related functions in cdbpartition.c.  Most items like this
 * are local to tablecmds.c.
 */
typedef struct NewConstraint
{
	char	   *name;			/* Constraint name, or NULL if none */
	ConstrType	contype;		/* CHECK or FOREIGN */
	Oid			refrelid;		/* PK rel, if FOREIGN */
	Node	   *qual;			/* Check expr or FkConstraint struct */
	List	   *qualstate;		/* Execution state for CHECK */
} NewConstraint;

/*
 * During attribute re-mapping for heterogeneous partitions, we use
 * this struct to identify which varno's attributes will be re-mapped.
 * Using this struct as a *context* during expression tree walking, we
 * can skip varattnos that do not belong to a given varno.
 */
typedef struct AttrMapContext{
	const AttrNumber *newattno; /* The mapping table to remap the varattno */
	Index varno; /* Which rte's varattno to re-map */
} AttrMapContext;

extern const char *synthetic_sql;

extern Oid DefineRelation(CreateStmt *stmt,
                          char relkind,
                          char relstorage,
                          const char *formattername);

extern void	DefineExternalRelation(CreateExternalStmt *stmt);

extern void RecognizeExternalRelationFormatterOptions(CreateExternalStmt *createExtStmt);

extern void DefineForeignRelation(CreateForeignStmt *createForeignStmt);

extern void	DefinePartitionedRelation(CreateStmt *stmt, Oid reloid);

extern void EvaluateDeferredStatements(List *deferredStmts);

extern void RemoveRelation(const RangeVar *relation, DropBehavior behavior,
						   DropStmt *stmt /* MPP */, char relkind);

extern void RemoveVlabelByOid(Oid relid);

extern void RemoveVlabel(RangeVar *rel, bool missing_ok);

extern void RemoveElabelByOid(Oid relid);

extern void RemoveElabel(RangeVar *rel, bool missing_ok);

extern void RemoveGraphByOid(Oid relid, bool ifRemoveTable);

extern void RemoveGraph(RangeVar *rel, bool missing_ok, bool ifRemoveTable);

extern bool RelationToRemoveIsTemp(const RangeVar *relation, DropBehavior behavior);

extern void AlterTable(Oid relid, AlterTableStmt *stmt);

extern void AlterRewriteTable(AlterRewriteTableInfo *ar_tab);

extern void ATExecChangeOwner(Oid relationOid, Oid newOwnerId, bool recursing);

extern void ATPExecPartSplit(Relation rel, AlterPartitionCmd *pc);         

extern void AlterTableInternal(Oid relid, List *cmds, bool recurse);

extern void AlterTableNamespace(RangeVar *relation, const char *newschema);

extern void AlterRelationNamespaceInternal(Relation classRel, Oid relOid,
							   Oid oldNspOid, Oid newNspOid,
							   bool hasDependEntry);

extern void ATAddColumn(Relation rel, ColumnDef *colDef);

extern void CheckTableNotInUse(Relation rel, const char *stmt);

extern void ExecuteTruncate(TruncateStmt *stmt);

extern Oid LookupMagmaFunc(char *formatter,
                           char *func);

/* Interfaces for mgma index ddl */
extern void InvokeMagmaCreateIndex(FmgrInfo *func,
                                   char *dbname,
                                   char *schemaname,
                                   char *tablename,
                                   MagmaIndexInfo *index,
                                   MagmaSnapshot *snapshot);

extern void InvokeMagmaDropIndex(FmgrInfo *func,
                                 char *dbname,
                                 char *schemaname,
                                 char *tablename,
                                 char *indexname,
                                 MagmaSnapshot *snapshot);

extern void InvokeMagmaReindexIndex(FmgrInfo *func,
                                    char *dbname,
                                    char *schemaname,
                                    char *tablename,
                                    char *indexname,
                                    MagmaSnapshot *snapshot);

extern void InvokeMagmaDropTable(FmgrInfo *func,
                                 ExtTableEntry *exttable,
                                 char *dbname,
                                 char *schemaname,
                                 char *tablename,
								 MagmaSnapshot *snapshot);

extern char *get_current_schema_name(void);

extern int renameHdfsPath(Oid relid, PartitionNode *pNode, const char *oldrelname, const char *newrelname);
extern void renameatt(Oid myrelid,
		  const char *oldattname,
		  const char *newattname,
		  bool recurse,
		  bool recursing);

extern void renamerel(Oid myrelid,
		  const char *newrelname,
		  RenameStmt *stmt /* MPP */);

extern void find_composite_type_dependencies(Oid typeOid,
											 const char *origTblName,
											 const char *origTypeName);

extern AttrNumber *varattnos_map(TupleDesc old, TupleDesc new);
extern AttrNumber *varattnos_map_schema(TupleDesc old, List *schema);
extern void change_varattnos_of_a_node(Node *node, const AttrNumber *newattno);

extern void change_varattnos_of_a_varno(Node *node, const AttrNumber *newattno, Index varno);


extern void register_on_commit_action(Oid relid, OnCommitAction action);
extern void remove_on_commit_action(Oid relid);

extern void PreCommit_on_commit_actions(void);
extern void AtEOXact_on_commit_actions(bool isCommit);
extern void AtEOSubXact_on_commit_actions(bool isCommit,
							  SubTransactionId mySubid,
							  SubTransactionId parentSubid);
extern PartitionNode *RelationBuildPartitionDesc(Relation rel,
												 bool inctemplate);
extern PartitionNode *RelationBuildPartitionDescByOid(Oid relid,
												 bool inctemplate);

extern bool rel_needs_long_lock(Oid relid);
extern Oid  rel_partition_get_master(Oid relid);

extern void PartitionRangeItemIsValid(ParseState *pstate, PartitionRangeItem *pri);

extern Oid get_settable_tablespace_oid(char *tablespacename);

extern List *
MergeAttributes(List *schema, List *supers, bool istemp, bool isPartitioned,
				List **supOids, List **supconstr, int *supOidCount, GpPolicy *policy);
extern List *make_dist_clause(Relation rel);

extern Oid transformFkeyCheckAttrs(Relation pkrel,
								   int numattrs, int16 *attnums,
								   Oid *opclasses);

extern void buildExternalTableFormatOptionStringInJson(const char *fmtOptsStr,
                                                       char **fmtOptsJson);

extern void buildDefaultFormatterOptionsInJson(
    int encoding, char externalFmtType, struct json_object *optJsonObject);
#endif   /* TABLECMDS_H */
