/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * tablecmds.c
 *	  Commands for creating and altering table structures and settings
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/commands/tablecmds.c,v 1.206.2.5 2008/05/09 22:37:41 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>
#include <fcntl.h>
#include <locale.h>
#include <unistd.h>

#include "access/appendonlywriter.h"
#include "access/bitmap.h"
#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/extprotocol.h"
#include "access/filesplit.h"
#include "access/nbtree.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "access/transam.h"
#include "catalog/catalog.h"
#include "catalog/catquery.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_appendonly.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_compression.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_extprotocol.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_resqueue.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_rule.h"
#include "catalog/toasting.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbparquetam.h"
#include "cdb/cdbpartition.h"
#include "cdb/cdbsharedstorageop.h"
#include "commands/cluster.h"
#include "commands/copy.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/typecmds.h"
#include "executor/executor.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/print.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/plancat.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "parser/analyze.h"
#include "parser/gramparse.h"
#include "parser/parse_agg.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "parser/parser.h"
#include "postmaster/identity.h"
#include "rewrite/rewriteHandler.h"
#include "storage/backendid.h"
#include "storage/smgr.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "utils/uri.h"
#include "mb/pg_wchar.h"

#include "cdb/cdbdisp.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbcat.h"
#include "cdb/cdbrelsize.h"
#include "cdb/cdboidsync.h"
#include "cdb/cdbsreh.h"

#include "cdb/cdbmirroredfilesysobj.h"
#include "cdb/cdbmirroredbufferpool.h"
#include "cdb/cdbmirroredappendonly.h"
#include "cdb/cdbpersistentfilesysobj.h"
#include "cdb/cdbquerycontextdispatching.h"
#include "cdb/dispatcher.h"
#include "cdb/cdbdispatchresult.h"

/*
 * ON COMMIT action list
 */
typedef struct OnCommitItem
{
	Oid			relid;			/* relid of relation */
	OnCommitAction oncommit;	/* what to do at end of xact */

	/*
	 * If this entry was created during the current transaction,
	 * creating_subid is the ID of the creating subxact; if created in a prior
	 * transaction, creating_subid is zero.  If deleted during the current
	 * transaction, deleting_subid is the ID of the deleting subxact; if no
	 * deletion request is pending, deleting_subid is zero.
	 */
	SubTransactionId creating_subid;
	SubTransactionId deleting_subid;
} OnCommitItem;

static List *on_commits = NIL;

/*
 * State information for ALTER TABLE
 *
 * The pending-work queue for an ALTER TABLE is a List of AlteredTableInfo
 * structs, one for each table modified by the operation (the named table
 * plus any child tables that are affected).  We save lists of subcommands
 * to apply to this table (possibly modified by parse transformation steps);
 * these lists will be executed in Phase 2.  If a Phase 3 step is needed,
 * necessary information is stored in the constraints and newvals lists.
 *
 * Phase 2 is divided into multiple passes; subcommands are executed in
 * a pass determined by subcommand type.
 */

#define AT_PASS_DROP			0		/* DROP (all flavors) */
#define AT_PASS_ALTER_TYPE		1		/* ALTER COLUMN TYPE */
#define AT_PASS_OLD_INDEX		2		/* re-add existing indexes */
#define AT_PASS_OLD_CONSTR		3		/* re-add existing constraints */
#define AT_PASS_COL_ATTRS		4		/* set other column attributes */
/* We could support a RENAME COLUMN pass here, but not currently used */
#define AT_PASS_ADD_COL			5		/* ADD COLUMN */
#define AT_PASS_ADD_INDEX		6		/* ADD indexes */
#define AT_PASS_ADD_CONSTR		7		/* ADD constraints, defaults */
#define AT_PASS_MISC			8		/* other stuff */
#define AT_NUM_PASSES			9

typedef struct AlteredTableInfo
{
	/* Information saved before any work commences: */
	Oid			relid;			/* Relation to work on */
	char		relkind;		/* Its relkind */
	TupleDesc	oldDesc;		/* Pre-modification tuple descriptor */
	/* Information saved by Phase 1 for Phase 2: */
	List	   *subcmds[AT_NUM_PASSES]; /* Lists of AlterTableCmd */
	/* Information saved by Phases 1/2 for Phase 3: */
	List	   *constraints;	/* List of NewConstraint */
	List	   *newvals;		/* List of NewColumnValue */
	bool		new_notnull;	/* T if we added new NOT NULL constraints */
	bool		new_dropoids;	/* T if we dropped the OID column */
	Oid			newTableSpace;	/* new tablespace; 0 means no change */
	Oid			exchange_relid;	/* for EXCHANGE, the exchanged in rel */
	/* Objects to rebuild after completing ALTER TYPE operations */
	List	   *changedConstraintOids;	/* OIDs of constraints to rebuild */
	List	   *changedConstraintDefs;	/* string definitions of same */
	List	   *changedIndexOids;		/* OIDs of indexes to rebuild */
	List	   *changedIndexDefs;		/* string definitions of same */
	List *scantable_splits;     /* split information for scan */
	List *ao_segnos;            /* segment file number for the new relation */
} AlteredTableInfo;

/*
 * Struct describing one new column value that needs to be computed during
 * Phase 3 copy (this could be either a new column with a non-null default, or
 * a column that we're changing the type of).  Columns without such an entry
 * are just copied from the old table during ATRewriteTable.  Note that the
 * expr is an expression over *old* table values.
 */
typedef struct NewColumnValue
{
	AttrNumber	attnum;			/* which column */
	Expr	   *expr;			/* expression to compute */
	ExprState  *exprstate;		/* execution state */
} NewColumnValue;

static void truncate_check_rel(Relation rel);
static void MergeConstraintsIntoExisting(Relation child_rel, Relation parent_rel);
static void MergeAttributesIntoExisting(Relation child_rel, Relation parent_rel, List *inhAttrNameList,
										bool is_partition);

static Datum AddDefaultPageRowGroupSize(Datum relOptions, List *options);
static bool add_nonduplicate_cooked_constraint(Constraint *cdef, List *stmtConstraints);
static bool change_varattnos_varno_walker(Node *node, const AttrMapContext *attrMapCxt);

static void StoreCatalogInheritance(Oid relationId, List *supers);
static void StoreCatalogInheritance1(Oid relationId, Oid parentOid,
						 int16 seqNumber, Relation inhRelation,
						 bool is_partition);
static int	findAttrByName(const char *attributeName, List *schema);
static void setRelhassubclassInRelation(Oid relationId, bool relhassubclass);
static void AlterIndexNamespaces(Relation classRel, Relation rel,
					 Oid oldNspOid, Oid newNspOid);
static void AlterSeqNamespaces(Relation classRel, Relation rel,
				   Oid oldNspOid, Oid newNspOid,
				   const char *newNspName);
static int transformColumnNameList(Oid relId, List *colList,
						int16 *attnums, Oid *atttypids);
static int transformFkeyGetPrimaryKey(Relation pkrel, Oid *indexOid,
						   List **attnamelist,
						   int16 *attnums, Oid *atttypids,
						   Oid *opclasses);
static void validateForeignKeyConstraint(FkConstraint *fkconstraint,
							 Relation rel, Relation pkrel);
static void createForeignKeyTriggers(Relation rel, Oid refRelOid,
                        			 FkConstraint *fkconstraint, Oid constraintOid);
static char *fkMatchTypeToString(char match_type);
static void ATController(Relation rel, List *cmds, bool recurse, 
						 int * oidInfoCount, TableOidInfo ** oidInfo, List **poidmap);
static void ATPrepCmd(List **wqueue, Relation rel, AlterTableCmd *cmd,
		  bool recurse, bool recursing);
static void ATRewriteCatalogs(List **wqueue);
static void ATAddToastIfNeeded(List **wqueue, 
							   int oidInfoCount,TableOidInfo * oidInfo, List **poidmap);
static void ATExecCmd(List **wqueue,
					  AlteredTableInfo *tab, Relation rel, AlterTableCmd *cmd);
static void ATRewriteTables(List **wqueue, 
							int oidInfoCount, TableOidInfo * oidInfo, List **poidmap);
static void ATRewriteTable(AlteredTableInfo *tab, Oid OIDNewHeap);
static AlteredTableInfo *ATGetQueueEntry(List **wqueue, Relation rel);
static void ATSimplePermissions(Relation rel, bool allowView);
static void ATSimplePermissionsRelationOrIndex(Relation rel);
static void ATSimpleRecursion(List **wqueue, Relation rel,
				  AlterTableCmd *cmd, bool recurse);
/* static void ATOneLevelRecursion(List **wqueue, Relation rel,
					AlterTableCmd *cmd); */
static void ATPrepAddColumn(Relation rel, bool recurse, AlterTableCmd *cmd);
static void ATExecAddColumn(AlteredTableInfo *tab, Relation rel,
				ColumnDef *colDef);
static void add_column_datatype_dependency(Oid relid, int32 attnum, Oid typid);
static void ATExecDropNotNull(Relation rel, const char *colName);
static void ATExecSetNotNull(AlteredTableInfo *tab, Relation rel,
				 const char *colName);
static void ATExecColumnDefault(Relation rel, const char *colName,
					Node *newDefault);
static void ATPrepSetStatistics(Relation rel, const char *colName,
					Node *flagValue);
static void ATExecSetStatistics(Relation rel, const char *colName,
					Node *newValue);
static void ATExecSetStorage(Relation rel, const char *colName,
				 Node *newValue);
static void ATExecDropColumn(List **wqueue, Relation rel, const char *colName,
				 DropBehavior behavior,
				 bool recurse, bool recursing);
static void ATExecAddIndex(AlteredTableInfo *tab, Relation rel,
			   IndexStmt *stmt, bool is_rebuild, bool part_expanded);
static void ATExecAddConstraint(AlteredTableInfo *tab, Relation rel, Node *newConstraint, bool recurse);
static void ATAddCheckConstraint(AlteredTableInfo *tab, Relation rel, Constraint *constr, bool recurse);

static void ATAddForeignKeyConstraint(AlteredTableInfo *tab, Relation rel,
						  FkConstraint *fkconstraint);
static void ATPrepDropConstraint(List **wqueue, Relation rel,
					 bool recurse, AlterTableCmd *cmd);
static void ATExecDropConstraint(Relation rel, const char *constrName,
					 DropBehavior behavior, bool quiet);
static void ATPrepAlterColumnType(List **wqueue,
					  AlteredTableInfo *tab, Relation rel,
					  bool recurse, bool recursing,
					  AlterTableCmd *cmd);
static void ATExecAlterColumnType(AlteredTableInfo *tab, Relation rel,
					  const char *colName, TypeName *typname);
static void ATPostAlterTypeCleanup(List **wqueue, AlteredTableInfo *tab);
static void ATPostAlterTypeParse(Oid oldRelId, Oid refRelId, char *cmd,
                    			 List **wqueue, Oid constrOid);
static void change_owner_recurse_to_sequences(Oid relationOid,
								  Oid newOwnerId);
static void ATExecClusterOn(Relation rel, const char *indexName);
static void ATExecDropCluster(Relation rel);
static void ATPrepSetTableSpace(AlteredTableInfo *tab, Relation rel,
					char *tablespacename);
static void ATPartsPrepSetTableSpace(List **wqueue, Relation rel, AlterTableCmd *cmd, 
									 List *oids);
static void ATExecSetTableSpace(Oid tableOid, Oid newTableSpace, 
								TableOidInfo *oidInfo);
static void ATExecSetRelOptions(Relation rel, List *defList, bool isReset);
static void ATExecEnableDisableTrigger(Relation rel, char *trigname,
						   bool enable, bool skip_system);
static void ATExecAddInherit(Relation rel, Node *node);
static void ATExecDropInherit(Relation rel, RangeVar *parent, bool is_partition);
static void ATExecSetDistributedBy(Relation rel, Node *node,
								   AlterTableCmd *cmd);
static void ATPrepExchange(Relation rel, AlterPartitionCmd *pc);

const char* synthetic_sql = "(internally generated SQL command)";

/* ALTER TABLE ... PARTITION */


static void ATPExecPartAdd(AlteredTableInfo *tab,
						   Relation rel,
                           AlterPartitionCmd *pc,
						   AlterTableType att);				/* Add */
static void ATPExecPartAlter(List **wqueue, AlteredTableInfo *tab, 
							 Relation rel,
                             AlterPartitionCmd *pc);		/* Alter */
static void ATPExecPartCoalesce(Relation rel,
                                AlterPartitionCmd *pc);		/* Coalesce */
static void ATPExecPartDrop(Relation rel,
                            AlterPartitionCmd *pc);			/* Drop */
static void ATPExecPartExchange(AlteredTableInfo *tab,
								Relation rel,
                                AlterPartitionCmd *pc);		/* Exchange */
static void ATPExecPartMerge(Relation rel,
                             AlterPartitionCmd *pc);		/* Merge */
static void ATPExecPartModify(Relation rel,
                              AlterPartitionCmd *pc);		/* Modify */
static void ATPExecPartRename(Relation rel,
                              AlterPartitionCmd *pc);		/* Rename */

static void ATPExecPartSetTemplate(AlteredTableInfo *tab,   /* Set */
								   Relation rel,            /* Subpartition */
                                   AlterPartitionCmd *pc);	/* Template */
static List *
atpxTruncateList(Relation rel, PartitionNode *pNode);
static void ATPExecPartTruncate(Relation rel,
                                AlterPartitionCmd *pc);		/* Truncate */
static void
copy_buffer_pool_data(
	Relation 	rel,

	SMgrRelation dst,

	ItemPointer persistentTid,

	int64 		persistentSerialNum,

	bool		useWal);

static bool TypeTupleExists(Oid typeId);
static void update_ri_trigger_args(Oid relid,
					   const char *oldname,
					   const char *newname,
					   bool fk_scan,
					   bool update_relname);
static Datum transformLocationUris(List *locs, List* fmtopts, bool isweb, bool iswritable);
static Datum transformExecOnClause(List	*on_clause, int *preferred_segment_num, bool iswritable);
static char transformFormatType(char *formatname);
static Datum transformFormatOpts(char formattype, List *formatOpts, int numcols, bool iswritable);

static Oid DefineRelation_int(CreateStmt *stmt, char relkind, char relstorage);

static void ATExecPartAddInternal(Relation rel, Node *def);

static void
AlterRelationNamespaceInternalTwo(Relation rel,
								  Oid relid,
								  Oid oldNspOid, Oid newNspOid,
								  bool hasDependEntry,
								  const char *newschema);

static RangeVar *make_temp_table_name(Relation rel, BackendId id);
static bool prebuild_temp_table(Relation rel, RangeVar *tmpname, List *distro,
								List *opts, List **hidden_types, bool isTmpTableAo);
static void ATPartitionCheck(AlterTableType subtype, Relation rel, bool rejectroot, bool recursing);
static void InvokeProtocolValidation(Oid procOid, char *procName, bool iswritable, List *locs, List* fmtopts);

static char *alterTableCmdString(AlterTableType subtype);

static bool isPgDefaultTablespace(const char *tablespacename);

bool
isPgDefaultTablespace(const char *tablespacename){
	if(tablespacename == NULL)
		return false;
	else if(strcmp(tablespacename, "pg_default") == 0)
		return true;
	else
		return false;
}


/* ----------------------------------------------------------------
 *		DefineRelation
 *				Creates a new relation.
 *
 * If successful, returns the OID of the new relation.
 * ----------------------------------------------------------------
 */
Oid
DefineRelation(CreateStmt *stmt, char relkind, char relstorage)
{
    Oid reloid = 0;
    Assert(stmt->relation->schemaname == NULL || strlen(stmt->relation->schemaname)>0);

    /* forbid create non-system table on tablespace pg_default */
    if((!IsBootstrapProcessingMode()) && (isPgDefaultTablespace(stmt->tablespacename)))
    {
    	ereport(ERROR,
    					(errcode(ERRCODE_CDB_FEATURE_NOT_YET),
    					errmsg("Creating table on tablespace 'pg_default' is not allowed")));
    }

    reloid = DefineRelation_int(stmt, relkind, relstorage);

    return reloid;
}


Oid
DefineRelation_int(CreateStmt *stmt,
                   char relkind,
                   char relstorage)
{
	char		relname[NAMEDATALEN];
	Oid			namespaceId;
	List	   *schema = stmt->tableElts;
	Oid			relationId = InvalidOid;
	Oid			tablespaceId;
	Relation	rel;
	TupleDesc	descriptor;
	List	   *inheritOids;
	List	   *old_constraints;
	bool		localHasOids;
	int			parentOidCount;
	List	   *rawDefaults;
	Datum		reloptions;
	ListCell   *listptr;
	AttrNumber	attnum;
	bool		isPartitioned;

	ItemPointerData	persistentTid;
	int64			persistentSerialNum;

	TidycatOptions *tidycatOptions = NULL;

	/*
	 * Truncate relname to appropriate length (probably a waste of time, as
	 * parser should have done this already).
	 */
	StrNCpy(relname, stmt->relation->relname, NAMEDATALEN);

	/*
	 * Check consistency of arguments
	 */
	if (stmt->oncommit != ONCOMMIT_NOOP && !stmt->relation->istemp)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("ON COMMIT can only be used on temporary tables"),
						   errOmitLocation(true)));

	/*
	 * Look up the namespace in which we are supposed to create the relation.
	 * Check we have permission to create there. Skip check if bootstrapping,
	 * since permissions machinery may not be working yet.
	 */
	namespaceId = RangeVarGetCreationNamespace(stmt->relation);

	if (!IsBootstrapProcessingMode())
	{
		AclResult	aclresult;

		aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(),
										  ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
						   get_namespace_name(namespaceId));
	}

	/*
	 * Select tablespace to use.  If not specified, use default_tablespace
	 * (which may in turn default to database's default).
	 *
	 * Note: This code duplicates code in indexcmds.c
	 */
	if (relkind == RELKIND_SEQUENCE || 
		relkind == RELKIND_VIEW ||
		relkind == RELKIND_COMPOSITE_TYPE ||
		(relkind == RELKIND_RELATION && (
			relstorage == RELSTORAGE_EXTERNAL ||
			relstorage == RELSTORAGE_FOREIGN  ||
			relstorage == RELSTORAGE_VIRTUAL)))
	{
		/* 
		 * MPP-8262: unable to create sequence with default_tablespace
		 *
		 * These relkinds have no storage, and thus do not support tablespaces.
		 * We shouldn't go through the regular default case for these because we
		 * don't want to pick up the value from the default_tablespace guc.
		 */
		Assert(!stmt->tablespacename);
		tablespaceId = InvalidOid;
	}
	else if (stmt->tablespacename)
	{
		/*
		 * Tablespace specified on the command line, or was passed down by
		 * dispatch.
		 */
		tablespaceId = get_tablespace_oid(stmt->tablespacename);
		if (!OidIsValid(tablespaceId))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("tablespace \"%s\" does not exist",
							stmt->tablespacename),
					 errOmitLocation(true)));
	}
	else
	{
		/*
		 * Get the default tablespace specified via default_tablespace, or fall
		 * back on the database tablespace.
		 */
        
		tablespaceId = (gp_upgrade_mode) ? DEFAULTTABLESPACE_OID : GetDefaultTablespace();

		/* Need the real tablespace id for dispatch */
		if (!OidIsValid(tablespaceId))
			tablespaceId = get_database_dts(MyDatabaseId);
	}

	/* Goh tablespace check. */
	/*
	 * temporaty disable this check for dispatching matedata test.
	 * TODO
	CheckCrossAccessTablespace(tablespaceId);
	*/

	/*
	 * Parse and validate reloptions, if any.
	 */
	reloptions = transformRelOptions((Datum) 0, stmt->options, true, false);

	/*
	 * Accept and only accept tidycat option during upgrade.
	 *
	 * All other storage option will be discarded during upgrade.
	 * During bootstrap, we don't have any storage option. So, during
	 * upgrade, we don't need it as well because we're just creating
	 * catalog objects. Further, we overload the WITH clause to pass-in
	 * the index oid. So, if we don't strip it out, it'll appear in
	 * the pg_class.reloptions, and we don't want that.
	 *
	 */
	if (gp_upgrade_mode)
	{
		tidycatOptions = tidycat_reloptions(reloptions);
		reloptions = 0;
	}

	/* Check permissions except when using database's default */
	if (OidIsValid(tablespaceId) && tablespaceId != MyDatabaseTableSpace &&
			tablespaceId != get_database_dts(MyDatabaseId))
	{
		AclResult	aclresult;

		aclresult = pg_tablespace_aclcheck(tablespaceId, GetUserId(),
										   ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_TABLESPACE,
						   get_tablespace_name(tablespaceId));
	}

	/*
	 * Look up inheritance ancestors and generate relation schema, including
	 * inherited attributes. Update the offsets of the distribution attributes
	 * in GpPolicy if necessary.
	 */
	isPartitioned = stmt->partitionBy ? true : false;
	schema = MergeAttributes(schema, stmt->inhRelations,
							 stmt->relation->istemp, isPartitioned,
							 &inheritOids, &old_constraints, &parentOidCount, stmt->policy);

	/*
	 * If a partition table, and user not specify pagesize and rowgroupsize, specify the default
	 * pagesize to 1MB, rowgroupsize to 8MB.
	 */
	if((stmt->partitionBy) || (stmt->is_part_child)){
		reloptions = AddDefaultPageRowGroupSize(reloptions, stmt->options);
	}

	/*
	 * Create a relation descriptor from the relation schema and create the
	 * relation.  Note that in this stage only inherited (pre-cooked) defaults
	 * and constraints will be included into the new relation.
	 * (BuildDescForRelation takes care of the inherited defaults, but we have
	 * to copy inherited constraints here.)
	 */
	descriptor = BuildDescForRelation(schema);

	localHasOids = interpretOidsOption(stmt->options);
	descriptor->tdhasoid = (localHasOids || parentOidCount > 0);

	/*
	 * old_constraints: pre-cooked constraints from CREATE TABLE ... INHERIT ...
	 * stmt->constraints: might have some pre-cooked constraints passed by analyze.c,
	 * 		due to LIKE tab INCLUDING CONSTRAINTS
	 *
	 * We no longer add these cooked constraints during heap_create_with_catalog.
	 * Instead, we add them along with raw constraints so that we can specify
	 * pg_constraint oid to use on segments.
	 */
	if (old_constraints)
	{
		/* deal with constraints from MergeAttributes */
		foreach(listptr, old_constraints)
		{
			Constraint *cdef = (Constraint *) lfirst(listptr);
			if (cdef->contype == CONSTR_CHECK &&
					add_nonduplicate_cooked_constraint(cdef, stmt->constraints))
				stmt->constraints = lappend(stmt->constraints, cdef);
		}
	}

     stmt->oidInfo.toastOid = InvalidOid;
     stmt->oidInfo.toastIndexOid = InvalidOid;
     stmt->oidInfo.aosegOid = InvalidOid;
     stmt->oidInfo.aosegIndexOid = InvalidOid;
     stmt->oidInfo.aoblkdirOid = InvalidOid;
     stmt->oidInfo.aoblkdirIndexOid = InvalidOid;
     stmt->ownerid = GetUserId();

    if(gp_upgrade_mode && tidycatOptions && (tidycatOptions->relid != InvalidOid))
    {
        stmt->oidInfo.relOid = tidycatOptions->relid;
        stmt->oidInfo.comptypeOid = tidycatOptions->reltype_oid;
        stmt->oidInfo.toastOid = tidycatOptions->toast_oid;
        stmt->oidInfo.toastIndexOid = tidycatOptions->toast_index;
        stmt->oidInfo.toastComptypeOid = tidycatOptions->toast_reltype;
    }

	/* MPP-8405: disallow OIDS on partitioned tables */
	if ((stmt->partitionBy ||
		 stmt->is_part_child) &&
		descriptor->tdhasoid && 
		IsNormalProcessingMode() &&
        (Gp_role == GP_ROLE_DISPATCH))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg(
							 "OIDS=TRUE is not allowed on partitioned tables. "
							 "Use OIDS=FALSE"
							 ),
					 errOmitLocation(true)));

    if (stmt->oidInfo.relOid)
        elog(DEBUG4, "DefineRelation relOid=%d schemaname=%s ",
             stmt->oidInfo.relOid,
             stmt->relation->schemaname ? stmt->relation->schemaname : "");

	relationId = heap_create_with_catalog(relname,
										  namespaceId,
										  tablespaceId,
										  stmt->oidInfo.relOid,
										  stmt->ownerid,
										  descriptor,
										  /* relam */ InvalidOid,
										  relkind,
										  relstorage,
										  tablespaceId==GLOBALTABLESPACE_OID,
										  localHasOids,
										  /* bufferPoolBulkLoad */ false,
										  parentOidCount,
										  stmt->oncommit,
                                          stmt->policy,  /*CDB*/
                                          reloptions,
										  allowSystemTableModsDDL,
										  &stmt->oidInfo.comptypeOid,
										  &persistentTid,
										  &persistentSerialNum);

	StoreCatalogInheritance(relationId, inheritOids);

	/*
	 * We must bump the command counter to make the newly-created relation
	 * tuple visible for opening.
	 */
	CommandCounterIncrement();

	/*
	 * Open the new relation and acquire exclusive lock on it, unless we're
	 * doing partition.
	 *
	 * This isn't even necessary in the case of normal relations since other
	 * backends can't see the new rel anyway until we commit), but it keeps
	 * the lock manager from complaining about deadlock risks.
	 */
	if (stmt->is_part_child)
		rel = relation_open(relationId, NoLock);
	else
		rel = relation_open(relationId, AccessExclusiveLock);

	/*
	 * For some reason, even though we have bumped the command counter above, 
	 * occasionally we are not able to see the persistent info just stored in gp_relation_node
	 * at the XLOG level.  So, we save the values here for debugging purposes.
	 */
	rel->rd_haveCreateDebugInfo = true;
	rel->rd_createDebugIsZeroTid = PersistentStore_IsZeroTid(&persistentTid);
	rel->rd_createDebugPersistentTid = persistentTid;
	rel->rd_createDebugPersistentSerialNum = persistentSerialNum;

	/*
	 * Now add any newly specified column default values and CHECK constraints
	 * to the new relation.  These are passed to us in the form of raw
	 * parsetrees; we need to transform them to executable expression trees
	 * before they can be added. The most convenient way to do that is to
	 * apply the parser's transformExpr routine, but transformExpr doesn't
	 * work unless we have a pre-existing relation. So, the transformation has
	 * to be postponed to this final step of CREATE TABLE.
	 *
	 * First, scan schema to find new column defaults.
	 */
	rawDefaults = NIL;
	attnum = 0;

	foreach(listptr, schema)
	{
		ColumnDef  *colDef = lfirst(listptr);

		attnum++;

		if (colDef->raw_default != NULL)
		{
			RawColumnDefault *rawEnt;

			Assert(colDef->cooked_default == NULL);

			rawEnt = (RawColumnDefault *) palloc(sizeof(RawColumnDefault));
			rawEnt->attnum = attnum;
			rawEnt->raw_default = colDef->raw_default;
			rawDefaults = lappend(rawDefaults, rawEnt);
		}
	}

	/*
	 * Parse and add the defaults/constraints, if any.
	 */
	if (rawDefaults || stmt->constraints)
		AddRelationConstraints(rel, rawDefaults, stmt->constraints);

	if (stmt->attr_encodings)
		AddRelationAttributeEncodings(rel, stmt->attr_encodings);

	/*
	 * Clean up.  We keep lock on new relation (although it shouldn't be
	 * visible to anyone else anyway, until commit).
	 */
	relation_close(rel, NoLock);

	return relationId;
}


/*
 * Add Default page size and rowgroup size to relation options
 */
static Datum AddDefaultPageRowGroupSize(Datum relOptions, List *defList){
	Datum result;
	ListCell   *cell = NULL;
	bool pageSizeSet = false;
	bool rowgroupSizeSet = false;
	bool parquetTable = false;
	bool need_free_arg = false;
	if(defList == NIL)
		return relOptions;

	foreach(cell, defList)
	{
		DefElem    *def = lfirst(cell);

		if(pg_strcasecmp("pagesize", def->defname) == 0)
		{
			pageSizeSet = true;
		}
		if(pg_strcasecmp("rowgroupsize", def->defname) == 0)
		{
			rowgroupSizeSet = true;
		}
		if(pg_strcasecmp("orientation", def->defname) == 0)
		{
			if(def->arg == NULL)
			{
				insist_log(false, "syntax not correct, orientation should has corresponding value");
			}

			if(pg_strcasecmp("parquet", defGetString(def, &need_free_arg)) == 0)
			{
				parquetTable = true;
			}
		}
	}

	if (!parquetTable)
		return relOptions;

	if ((pageSizeSet == true) && (rowgroupSizeSet == true)){
		return relOptions;
	}
	else
	{
		/*set default page size*/
		ArrayBuildState *astate = NULL;
		if(DatumGetPointer(relOptions) != 0){
			ArrayType  *array = DatumGetArrayTypeP(relOptions);
			Datum	   *options;
			int			noptions;
			int			i;

			Assert(ARR_ELEMTYPE(array) == TEXTOID);
			deconstruct_array(array, TEXTOID, -1, false, 'i',
							   &options, NULL, &noptions);

			/* copy the existing rel options*/
			for (i = 0; i < noptions; i++)
			{
				astate = accumArrayResult(astate, options[i],
										  false, TEXTOID,
										  CurrentMemoryContext);
			}
		}

		/* append page size*/
		if (pageSizeSet == false){
			text *t;
			const char *name = "pagesize";
			Size len;
			char value[20];
			pg_ltoa(DEFAULT_PARQUET_PAGE_SIZE_PARTITION, value);

			len = VARHDRSZ + strlen(name) + 1 + strlen(value);
			t = (text *) palloc(len + 1);
			SET_VARSIZE(t, len);
			sprintf(VARDATA(t), "%s=%s", name, value);

			astate = accumArrayResult(astate, PointerGetDatum(t),
									  false, TEXTOID,
									  CurrentMemoryContext);
		}

		if(rowgroupSizeSet == false){
			text *t;
			const char *name = "rowgroupsize";
			char value[20];
			Size len;

			pg_ltoa(DEFAULT_PARQUET_ROWGROUP_SIZE_PARTITION, value);
			len = VARHDRSZ + strlen(name) + 1 + strlen(value);
			t = (text *) palloc(len + 1);
			SET_VARSIZE(t, len);
			sprintf(VARDATA(t), "%s=%s", name, value);

			astate = accumArrayResult(astate, PointerGetDatum(t),
									  false, TEXTOID,
									  CurrentMemoryContext);
		}

		if (astate)
			result = makeArrayResult(astate, CurrentMemoryContext);

		return result;
	}
}

/* ----------------------------------------------------------------
*		DefineExternalRelation
*				Creates a new external relation.
*
* In here we first dispatch a normal DefineRelation() (with relstorage
* external) in order to create the external relation entries in pg_class
* pg_type etc. Then once this is done we dispatch ourselves (DefineExternalRelation)
* in order to create the pg_exttable entry across the gp array and we
* also record a dependency with the error table, if one exists.
*
* Why don't we just do all of this in one dispatch run? because that
* involves duplicating the DefineRelation() code or severely modifying it
* to have special cases for external tables. IMHO it's better and cleaner
* to leave it intact and do another dispatch.
* ----------------------------------------------------------------
*/
extern void
DefineExternalRelation(CreateExternalStmt *createExtStmt)
{
	/*volatile struct CdbDispatcherState ds = {NULL, NULL};*/
	CreateStmt				  *createStmt = makeNode(CreateStmt);
	ExtTableTypeDesc 		  *exttypeDesc = (ExtTableTypeDesc *)createExtStmt->exttypedesc;
	SingleRowErrorDesc 		  *singlerowerrorDesc = NULL;
	DefElem    				  *dencoding = NULL;
	ListCell				  *option;
	Oid						  reloid = 0;
	Oid						  fmtErrTblOid = InvalidOid;
	Datum					  formatOptStr;
	Datum					  locationUris = 0;
	Datum					  locationExec = 0;
	char*					  commandString = NULL;
	char					  rejectlimittype = '\0';
	char					  formattype;
	int						  rejectlimit = -1;
	int						  encoding = -1;
	int preferred_segment_num = -1;
	bool					  issreh = false; /* is single row error handling requested? */
	bool					  iswritable = createExtStmt->iswritable;
	bool					  isweb = createExtStmt->isweb;

	/*
	 * now set the parameters for keys/inheritance etc. Most of these are
	 * uninteresting for external relations...
	 */
	createStmt->relation = createExtStmt->relation;
	createStmt->tableElts = createExtStmt->tableElts;
	createStmt->inhRelations = NIL;
	createStmt->constraints = NIL;
	createStmt->options = NIL;
	createStmt->oncommit = ONCOMMIT_NOOP;
	createStmt->tablespacename = NULL;
	createStmt->policy = createExtStmt->policy; /* policy was set in transform */
	
		
	switch(exttypeDesc->exttabletype)
	{
		case EXTTBL_TYPE_LOCATION:

			/* Parse and validate URI strings (LOCATION clause) */
			locationUris = transformLocationUris(exttypeDesc->location_list,
												 createExtStmt->formatOpts,
												 isweb, iswritable);
			
			break;

		case EXTTBL_TYPE_EXECUTE:
			locationExec = transformExecOnClause(exttypeDesc->on_clause, &preferred_segment_num, iswritable);
			if (createStmt->policy)
			{
			  createStmt->policy->bucketnum = preferred_segment_num;
			}
			commandString = exttypeDesc->command_string;

			if(strlen(commandString) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("Invalid EXECUTE clause. Found an empty command string"),
								   errOmitLocation(true)));

			break;

		default:
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERNAL_ERROR),
					 errmsg("Internal error: unknown external table type"),
							   errOmitLocation(true)));
	}

	/*
	 * check permissions to create this external table.
	 *
	 * - Always allow if superuser.
	 * - Never allow EXECUTE or 'file' exttables if not superuser.
	 * - Allow http, gpfdist or gpfdists tables if pg_auth has the right permissions
	 *   for this role and for this type of table, or if gp_external_grant_privileges 
	 *   is on (gp_external_grant_privileges should be deprecated at some point).
	 */
	if(!superuser() && Gp_role == GP_ROLE_DISPATCH)
	{		
		if(exttypeDesc->exttabletype == EXTTBL_TYPE_EXECUTE)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to create an EXECUTE external web table"),
							   errOmitLocation(true)));
		}
		else
		{
			ListCell   *first_uri = list_head(exttypeDesc->location_list);
			Value		*v = lfirst(first_uri);
			char		*uri_str = pstrdup(v->val.str);
			Uri			*uri = ParseExternalTableUri(uri_str);

			Assert(exttypeDesc->exttabletype == EXTTBL_TYPE_LOCATION);

			if(uri->protocol == URI_FILE)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("must be superuser to create an external table with a file protocol"),
								   errOmitLocation(true)));
			}
			else if(!gp_external_grant_privileges)
			{
				/*
				 * Check if this role has the proper 'gpfdist', 'gpfdists' or 'http'
				 * permissions in pg_auth for creating this table.
				 */

				bool		 isnull;				
				Oid			 userid = GetUserId();
				cqContext	*pcqCtx;
				HeapTuple	 tuple;

				pcqCtx = caql_beginscan(
						NULL,
						cql("SELECT * FROM pg_authid "
							 " WHERE oid = :1 "
							 " FOR UPDATE ",
							 ObjectIdGetDatum(userid)));

				tuple = caql_getnext(pcqCtx);
								
				if (!HeapTupleIsValid(tuple))
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
							 errmsg("role \"%s\" does not exist (in DefineExternalRelation)", 
									 GetUserNameFromId(userid))));

				if ( (uri->protocol == URI_GPFDIST || uri->protocol == URI_GPFDISTS) && iswritable)
				{
					Datum 	d_wextgpfd = caql_getattr(pcqCtx, Anum_pg_authid_rolcreatewextgpfd, 
													  &isnull);
					bool	createwextgpfd = (isnull ? false : DatumGetBool(d_wextgpfd));

					if (!createwextgpfd)
						ereport(ERROR,
								(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
								errmsg("permission denied: no privilege to create a writable gpfdist(s) external table"),
										errOmitLocation(true)));
				}
				else if ( (uri->protocol == URI_GPFDIST || uri->protocol == URI_GPFDISTS) && !iswritable)
				{
					Datum 	d_rextgpfd = caql_getattr(pcqCtx, Anum_pg_authid_rolcreaterextgpfd, 
													  &isnull);
					bool	createrextgpfd = (isnull ? false : DatumGetBool(d_rextgpfd));

					if (!createrextgpfd)
						ereport(ERROR,
								(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
								errmsg("permission denied: no privilege to create a readable gpfdist(s) external table"),
										errOmitLocation(true)));

				}
				else if (uri->protocol == URI_HTTP && !iswritable)
				{
					Datum 	d_exthttp = caql_getattr(pcqCtx, Anum_pg_authid_rolcreaterexthttp, 
													 &isnull);
					bool	createrexthttp = (isnull ? false : DatumGetBool(d_exthttp));

					if (!createrexthttp)
						ereport(ERROR,
								(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
								errmsg("permission denied: no privilege to create an http external table"),
										errOmitLocation(true)));
				}
				else if (uri->protocol == URI_CUSTOM)
				{
					Oid			ownerId = GetUserId();
					char*		protname = uri->customprotocol;
					Oid			ptcId = LookupExtProtocolOid(protname, false);
					AclResult	aclresult;
					
					/* Check we have the right permissions on this protocol */
					if (!pg_extprotocol_ownercheck(ptcId, ownerId))
					{	
						AclMode mode = (iswritable ? ACL_INSERT : ACL_SELECT);
						
						aclresult = pg_extprotocol_aclcheck(ptcId, ownerId, mode);
						
						if (aclresult != ACLCHECK_OK)
							aclcheck_error(aclresult, ACL_KIND_EXTPROTOCOL, protname);
					}
				}
				else
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("internal error in DefineExternalRelation. "
								   "protocol is %d, writable is %d", 
								   uri->protocol, iswritable)));
				}
				
				caql_endscan(pcqCtx);
			}
			FreeExternalTableUri(uri);
			pfree(uri_str);
		}
	}

	/*
	 * Parse and validate FORMAT clause.
	 */
	formattype = transformFormatType(createExtStmt->format);
	
	formatOptStr = transformFormatOpts(formattype,
									   createExtStmt->formatOpts,
									   list_length(createExtStmt->tableElts),
									   iswritable);

	/*
	 * Parse single row error handling info if available
	 */
	singlerowerrorDesc = (SingleRowErrorDesc *)createExtStmt->sreh;

	if(singlerowerrorDesc)
	{
		Assert(!iswritable);
		
		issreh = true;

		/* get reject limit, and reject limit type */
		rejectlimit = singlerowerrorDesc->rejectlimit;
		rejectlimittype = (singlerowerrorDesc->is_limit_in_rows ? 'r' : 'p');
		VerifyRejectLimit(rejectlimittype, rejectlimit);

		/* KEEP only allowed for COPY */
		if(singlerowerrorDesc->is_keep)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("KEEP is not supported in the external table definition. "
							"Error table will be dropped when its external table is "
							"dropped."),
									   errOmitLocation(true)));

		if(singlerowerrorDesc->errtable)
			fmtErrTblOid = RangeVarGetRelid(singlerowerrorDesc->errtable, true, false /*allowHcatalog*/);
		else
			fmtErrTblOid = InvalidOid; /* no err tbl was requested */
	}

	/*
	 * Parse external table data encoding
	 */
	foreach(option, createExtStmt->encoding)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		Assert(strcmp(defel->defname, "encoding") == 0);

		if (dencoding)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("conflicting or redundant ENCODING specification"),
							   errOmitLocation(true)));
		dencoding = defel;
	}

	if (dencoding && dencoding->arg)
	{
		const char *encoding_name;

		if (IsA(dencoding->arg, Integer))
		{
			encoding = intVal(dencoding->arg);
			encoding_name = pg_encoding_to_char(encoding);
			if (strcmp(encoding_name, "") == 0 ||
				pg_valid_client_encoding(encoding_name) < 0)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("%d is not a valid encoding code",
								encoding),
										   errOmitLocation(true)));
		}
		else if (IsA(dencoding->arg, String))
		{
			encoding_name = strVal(dencoding->arg);
			if (pg_valid_client_encoding(encoding_name) < 0)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("%s is not a valid encoding name",
								encoding_name),
										   errOmitLocation(true)));
			encoding = pg_char_to_encoding(encoding_name);
		}
		else
			elog(ERROR, "unrecognized node type: %d",
				 nodeTag(dencoding->arg));
	}

	/* If encoding is defaulted, use database encoding */
	if (encoding < 0)
		encoding = pg_get_client_encoding();


    /*
	 * First, create the pg_class and other regular relation catalog entries.
	 * Under the covers this will dispatch a CREATE TABLE statement to all the
	 * QEs.
	 */
	Assert(Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY);

	reloid = DefineRelation(createStmt, RELKIND_RELATION, RELSTORAGE_EXTERNAL);

	/*
	 * Now we take care of pg_exttable and dependency with error table (if any).
	 */

	/*
	 * get our pg_class external rel OID. If we're the QD we just created
	 * it above. If we're a QE DefineRelation() was already dispatched to
	 * us and therefore we have a local entry in pg_class. get the OID
	 * from cache.
	 */
	if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY)
		Assert(reloid != InvalidOid);
	else
		reloid = RangeVarGetRelid(createExtStmt->relation, true, false /*allowHcatalog*/);

	/*
	 * create a pg_exttable entry for this external table.
	 */
	InsertExtTableEntry(reloid,
						iswritable,
						isweb,
						issreh,
						formattype,
						rejectlimittype,
						commandString,
						rejectlimit,
						fmtErrTblOid,
						encoding,
						formatOptStr,
						locationExec,
						locationUris);

	/*
	 * record a dependency between the external table and its error table (if one exists)
	 */
	if(singlerowerrorDesc && singlerowerrorDesc->errtable)
	{
		ObjectAddress	myself,
		referenced;
		Oid		fmtErrTblOid = RangeVarGetRelid(((SingleRowErrorDesc *)createExtStmt->sreh)->errtable, true, false /*allowHcatalog*/);

		Assert(!createExtStmt->iswritable);
		Assert(fmtErrTblOid != InvalidOid);

		myself.classId = RelationRelationId;
		myself.objectId = reloid;
		myself.objectSubId = 0;
		referenced.classId = RelationRelationId;
		referenced.objectId = fmtErrTblOid;
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}
}

extern void
DefineForeignRelation(CreateForeignStmt *createForeignStmt)
{
	CreateStmt				  *createStmt = makeNode(CreateStmt);
	Oid						  reloid = 0;
	bool					  shouldDispatch = (Gp_role == GP_ROLE_DISPATCH &&
												IsNormalProcessingMode());
	
	/*
	 * now set the parameters for keys/inheritance etc. Most of these are
	 * uninteresting for external relations...
	 */
	createStmt->relation = createForeignStmt->relation;
	createStmt->tableElts = createForeignStmt->tableElts;
	createStmt->inhRelations = NIL;
	createStmt->constraints = NIL;
	createStmt->options = NIL;
	createStmt->oncommit = ONCOMMIT_NOOP;
	createStmt->tablespacename = NULL;
	createStmt->policy = NULL; /* for now, we use "master only" type of distribution */
	
	/* (permissions are checked in foreigncmd.c:InsertForeignTableEntry() ) */

  /*
	 * First, create the pg_class and other regular relation catalog entries.
	 * Under the covers this will dispatch a CREATE TABLE statement to all the
	 * QEs.
	 */
	if(Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY)
		reloid = DefineRelation(createStmt, RELKIND_RELATION, RELSTORAGE_FOREIGN);

	/*
	 * Now we take care of pg_foreign_table
	 */
  ObjectAddress myself;
  ObjectAddress referenced;
  Oid	ownerId = GetUserId();
  ForeignServer *srv = GetForeignServerByName(createForeignStmt->srvname, false);

  /*
   * get our pg_class foreign rel OID. If we're the QD we just created
   * it above. If we're a QE DefineRelation() was already dispatched to
   * us and therefore we have a local entry in pg_class. get the OID
   * from cache.
   */
  if(Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY)
    Assert(reloid != InvalidOid);
  else
    reloid = RangeVarGetRelid(createForeignStmt->relation, true, false);

  /* Add dependency on SERVER and owner */
  myself.classId = RelationRelationId;
  myself.objectId = reloid;
  myself.objectSubId = 0;

  referenced.classId = ForeignServerRelationId;
  referenced.objectId = srv->serverid;
  referenced.objectSubId = 0;
  recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

  recordDependencyOnOwner(RelationRelationId, reloid, ownerId);

  /* create a pg_exttable entry for this foreign table.*/
  InsertForeignTableEntry(reloid,
              createForeignStmt->srvname,
              createForeignStmt->options);


  if (shouldDispatch)
  {

    /* Dispatch the statement tree to all primary and mirror segdbs.
     * Doesn't wait for the QEs to finish execution.
     */
    dispatch_statement_node((Node *) createForeignStmt, NULL, NULL, NULL);
  }
}

/* ----------------------------------------------------------------
*		DefinePartitionedRelation
*				Create the rewrite rule for a partitioned table
*
* parse/analyze.c/transformPartitionBy does the bulk of the work for
* partitioned tables, converting a single CREATE TABLE into a series
* of statements to create the child tables for each partition.  Each
* child table has a check constraint and a rewrite rule to ensure that
* INSERTs to the parent end up in the correct child table (partition).
* However, we cannot add a RuleStmt for a non-existent table to the
* a(fter)list for the Create statement (believe me, I tried, really
* hard).  Thus, we create the "parsed" RuleStmt in analyze, and
* finally parse_analyze it here *after* the relation is created, the
* use process_utility to dispatch.
* ----------------------------------------------------------------
*/
void
DefinePartitionedRelation(CreateStmt *stmt, Oid relOid)
{

	if (stmt->postCreate)
	{
		List *pQry;
		Node *pUtl;
		DestReceiver *dest = None_Receiver;
		List *pL1 = (List *)stmt->postCreate;

		pQry = parse_analyze(linitial(pL1), NULL, NULL, 0);

		if (pQry)
		{
			/* just grab the first guy - "There Can Be Only One"(TM) */
			pUtl = linitial(pQry);

			if (pUtl)
			{
				Assert(IsA(pUtl, Query));
				Assert(((Query *)pUtl)->commandType == CMD_UTILITY);

				ProcessUtility((Node *)(((Query *)pUtl)->utilityStmt),
							   synthetic_sql,
							   NULL, 
							   false, /* not top level */
							   dest,
							   NULL);
			}
		}
	}
}

/*
 * Run deferred statements generated by internal operations around
 * partition addition/split.
 */
void
EvaluateDeferredStatements(List *deferredStmts)
{
	ListCell	   *lc;

	/***
	 *** XXX: Fix MPP-13750, however this fails to address the similar bug
	 ***      with ordinary inheritance and partial indexes.  When that bug,
	 ***      is fixed, this section should become unnecessary!
	 ***/
	
	foreach( lc, deferredStmts )
	{
		ListCell *ulc;
		List *analyzedStmts = NIL;
		DestReceiver *dest = None_Receiver;
		
		Node *dstmt = lfirst(lc);
		
		analyzedStmts = parse_analyze(dstmt, NULL, NULL, 0);
		foreach (ulc, analyzedStmts)
		{
			Query *uquery = NULL;
			Node *utilityStmt = lfirst(ulc);
			
			Insist(IsA(utilityStmt, Query));
			uquery = (Query*)utilityStmt;
			Insist(uquery->commandType == CMD_UTILITY);
			
			ereport(DEBUG1, 
					(errmsg("processing deferred utility statement")));
			
			ProcessUtility((Node*)uquery->utilityStmt,
						   synthetic_sql,
						   NULL,
						   false,
						   dest,
						   NULL);
		}
	}
}

/* Don't track internal namespaces for toast, bitmap, aoseg */
#define METATRACK_VALIDNAMESPACE(namespaceId) \
	(namespaceId != PG_TOAST_NAMESPACE &&	\
	 namespaceId != PG_BITMAPINDEX_NAMESPACE && \
	 namespaceId != PG_AOSEGMENT_NAMESPACE )

/* check for valid namespace and valid relkind */
static 
bool
MetaTrackValidKindNsp(Form_pg_class rd_rel)
{
	Oid nsp = rd_rel->relnamespace;

	if (PG_CATALOG_NAMESPACE == nsp)
	{
		/* MPP-7773: don't track objects in system namespace
		 * if modifying system tables (eg during upgrade)  
		 */
		if (allowSystemTableModsDDL)
			return (false);
	}

	/* MPP-7599: watch out for toast indexes */
	return (METATRACK_VALIDNAMESPACE(nsp)
			&& MetaTrackValidRelkind(rd_rel->relkind)
			/* MPP-7572: not valid if in any temporary namespace */
			&& (!(isAnyTempNamespace(nsp))));
}

/*
 * RemoveRelation
 *		Deletes a relation.
 */
void
RemoveRelation(const RangeVar *relation, DropBehavior behavior,
			   DropStmt *stmt)
{
	Oid			relOid;
	ObjectAddress object;
	HeapTuple tuple;
	cqContext  *pcqCtx;

	AcceptInvalidationMessages();

	relOid = RangeVarGetRelid(relation, false, false /*allowHcatalog*/);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		LockRelationOid(RelationRelationId, RowExclusiveLock);
		LockRelationOid(TypeRelationId, RowExclusiveLock);
		LockRelationOid(DependRelationId, RowExclusiveLock);
	}

	LockRelationOid(relOid, AccessExclusiveLock);

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(relOid)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "relation \"%s\" does not exist", relation->relname);

	/* MPP-3260: disallow direct DROP TABLE of a partition */
	if (stmt && rel_is_child_partition(relOid) && !stmt->bAllowPartn)
	{
		Oid		 master = rel_partition_get_master(relOid);
		char	*pretty	= rel_get_part_path_pretty(relOid,
												   " ALTER PARTITION ",
												   " DROP PARTITION ");

		ereport(ERROR,
				(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
				errmsg("cannot drop partition \"%s\" directly",
					   get_rel_name(relOid)),
				errhint("Table \"%s\" is a child partition of table "
						"\"%s\".  To drop it, use ALTER TABLE \"%s\"%s...",
						get_rel_name(relOid), get_rel_name(master),
						get_rel_name(master), pretty ? pretty : "" ),
								   errOmitLocation(true)));

	}

	object.classId = RelationRelationId;
	object.objectId = relOid;
	object.objectSubId = 0;

	caql_endscan(pcqCtx);

	/* if we got here then we should proceed. */
	performDeletion(&object, behavior);
}

/*
 * RelationToRemoveIsTemp
 *		Checks if an object being targeted for drop is a temporary table.
 */
bool
RelationToRemoveIsTemp(const RangeVar *relation, DropBehavior behavior)
{
	Oid			relOid;
	Oid			recheckoid;
	ObjectAddress object;
	HeapTuple	relTup;
	Form_pg_class relForm;
	char	   *nspname;
	char	   *relname;
	bool		isTemp;
	cqContext  *pcqCtx;

	elog(DEBUG5, "Relation to remove catalogname %s, schemaname %s, relname %s",
		 (relation->catalogname == NULL ? "<empty>" : relation->catalogname),
		 (relation->schemaname == NULL ? "<empty>" : relation->schemaname),
		 (relation->relname == NULL ? "<empty>" : relation->relname));

	// UNDONE: Not sure how to interpret 'behavior'...

	relOid = RangeVarGetRelid(relation, false, false /*allowHcatalog*/);

	object.classId = RelationRelationId;
	object.objectId = relOid;
	object.objectSubId = 0;

	/*
	 * Lock down the object to stablize it before we examine its
	 * charactistics.
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		LockRelationOid(RelationRelationId, RowExclusiveLock);
		LockRelationOid(TypeRelationId, RowExclusiveLock);
		LockRelationOid(DependRelationId, RowExclusiveLock);
	}

	/* Lock the relation to be dropped */
	LockRelationOid(relOid, AccessExclusiveLock);

	/*
	 * When we got the relOid lock, it is possible that the relation has gone away.
	 * this will throw Error if the relation is already deleted.
	 */
	recheckoid = RangeVarGetRelid(relation, false, false /*allowHcatalog*/);

	/* if we got here then we should proceed. */

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(relOid)));

	relTup = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(relTup))
		elog(ERROR, "cache lookup failed for relation %u", relOid);
	relForm = (Form_pg_class) GETSTRUCT(relTup);

	/* Qualify the name if not visible in search path */
	if (RelationIsVisible(relOid))
		nspname = NULL;
	else
		nspname = get_namespace_name(relForm->relnamespace);

	/* XXX XXX: is this all just for debugging?  could just be simplified to:
	   SELECT relnamespace from pg_class 
	*/

	relname = quote_qualified_identifier(nspname, NameStr(relForm->relname));

	isTemp = isTempNamespace(relForm->relnamespace);

	elog(DEBUG5, "Relation name is %s, namespace %s, isTemp = %s",
	     relname,
	     (nspname == NULL ? "<null>" : nspname),
	     (isTemp ? "true" : "false"));

	caql_endscan(pcqCtx);

	return isTemp;
}


/*
 * ExecuteTruncate
 *		Executes a TRUNCATE command.
 *
 * This is a multi-relation truncate.  We first open and grab exclusive
 * lock on all relations involved, checking permissions and otherwise
 * verifying that the relation is OK for truncation.  In CASCADE mode,
 * relations having FK references to the targeted relations are automatically
 * added to the group; in RESTRICT mode, we check that all FK references are
 * internal to the group that's being truncated.  Finally all the relations
 * are truncated and reindexed.
 */
void
ExecuteTruncate(TruncateStmt *stmt)
{
	List	   *rels = NIL;
	List	   *relids = NIL;
	List	   *meta_relids = NIL;
	ListCell   *cell;
    int partcheck = 2;
	List *partList = NIL;

	/*
	 * Open, exclusive-lock, and check all the explicitly-specified relations
	 *
	 * Check if table has partitions and add them too
	 */
	while (partcheck)
	{
		foreach(cell, stmt->relations)
		{
			RangeVar   *rv = lfirst(cell);
			Relation	rel;
			PartitionNode *pNode;

			PG_TRY();
			{
				rel = heap_openrv(rv, AccessExclusiveLock);
			}
			
			PG_CATCH();
			{
				/* 
				 * In the case of the table being dropped concurrently, 
				 * throw a friendlier error than:
				 * 
				 * "could not open relation with relid 1234"
				 */
				if (rv->schemaname)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_TABLE),
							 errmsg("relation \"%s.%s\" does not exist",
									rv->schemaname, rv->relname),
							 errOmitLocation(true)));
				else
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_TABLE),
							 errmsg("relation \"%s\" does not exist",
									rv->relname),
							 errOmitLocation(true)));
				PG_RE_THROW();
			}
			PG_END_TRY();
			
			truncate_check_rel(rel);

			if (partcheck == 2)
			{
				pNode = RelationBuildPartitionDesc(rel, false);

				if (pNode)
				{
					List *plist = atpxTruncateList(rel, pNode);

					if (plist)
					{
						if (partList)
							partList = list_concat(partList, plist);
						else
							partList = plist;
					}
				}
			}
			heap_close(rel, NoLock);
		}

		partcheck--;

		if (partList)
		{
			/* add the partitions to the relation list and try again */
			if (partcheck == 1)
				stmt->relations = list_concat(partList, stmt->relations);
		}
		else
			/* no partitions - no need to try again */
			partcheck = 0;
	} /* end while partcheck */

	/*
	 * Open, exclusive-lock, and check all the explicitly-specified relations
	 */
	foreach(cell, stmt->relations)
	{
		RangeVar   *rv = lfirst(cell);
		Relation	rel;

		rel = heap_openrv(rv, AccessExclusiveLock);
		truncate_check_rel(rel);
		rels = lappend(rels, rel);
		relids = lappend_oid(relids, RelationGetRelid(rel));

		if (MetaTrackValidKindNsp(rel->rd_rel))
			meta_relids = lappend_oid(meta_relids, RelationGetRelid(rel));
	}


	/*
	 * In CASCADE mode, suck in all referencing relations as well.	This
	 * requires multiple iterations to find indirectly-dependent relations. At
	 * each phase, we need to exclusive-lock new rels before looking for their
	 * dependencies, else we might miss something.	Also, we check each rel as
	 * soon as we open it, to avoid a faux pas such as holding lock for a long
	 * time on a rel we have no permissions for.
	 */
	if (stmt->behavior == DROP_CASCADE)
	{
		for (;;)
		{
			List	   *newrelids;

			newrelids = heap_truncate_find_FKs(relids);
			if (newrelids == NIL)
				break;			/* nothing else to add */

			foreach(cell, newrelids)
			{
				Oid			relid = lfirst_oid(cell);
				Relation	rel;

				rel = heap_open(relid, AccessExclusiveLock);
				ereport(NOTICE,
						(errmsg("truncate cascades to table \"%s\"",
								RelationGetRelationName(rel)),
										   errOmitLocation(true)));
				truncate_check_rel(rel);
				rels = lappend(rels, rel);
				relids = lappend_oid(relids, relid);

				if (MetaTrackValidKindNsp(rel->rd_rel))
					meta_relids = lappend_oid(meta_relids, 
											  RelationGetRelid(rel));
			}
		}
	}


	/*
	 * Check foreign key references.  In CASCADE mode, this should be
	 * unnecessary since we just pulled in all the references; but as a
	 * cross-check, do it anyway if in an Assert-enabled build.
	 */
#ifdef USE_ASSERT_CHECKING
	heap_truncate_check_FKs(rels, false);
#else
	if (stmt->behavior == DROP_RESTRICT)
		heap_truncate_check_FKs(rels, false);
#endif

	/*
	 * OK, truncate each table.
	 */
	foreach(cell, rels)
	{
		Relation	rel = (Relation) lfirst(cell);
		Oid			heap_relid;
		Oid			toast_relid;
		Oid			aoseg_relid = InvalidOid;
		Oid			aoblkdir_relid = InvalidOid;
		Oid			new_heap_oid = InvalidOid;
		Oid			new_toast_oid = InvalidOid;
		Oid			new_aoseg_oid = InvalidOid;
		Oid			new_aoblkdir_oid = InvalidOid;
		List	   *indoids = NIL;

		/*
		 * Create a new empty storage file for the relation, and assign it
		 * as the relfilenode value. The old storage file is scheduled for
		 * deletion at commit.
		 */
		new_heap_oid = setNewRelfilenode(rel);

		heap_relid = RelationGetRelid(rel);
		toast_relid = rel->rd_rel->reltoastrelid;
		heap_close(rel, NoLock);

		if (RelationIsAoRows(rel) || RelationIsParquet(rel)){
			GetAppendOnlyEntryAuxOids(heap_relid, SnapshotNow,
											  &aoseg_relid, NULL,
											  &aoblkdir_relid, NULL);
			AORelRemoveHashEntryOnCommit(RelationGetRelid(rel));
		}
		else{
			ereport(ERROR,
								( errcode(ERRCODE_GP_COMMAND_ERROR),
										errmsg("TRUNCATE on heap table is not supported.")));
		}

		/*
		 * The same for the toast table, if any.
		 */
		if (OidIsValid(toast_relid))
		{
			rel = relation_open(toast_relid, AccessExclusiveLock);
			new_toast_oid = setNewRelfilenode(rel);
			heap_close(rel, NoLock);
		}

		/*
		 * The same for the aoseg table, if any.
		 */
		if (OidIsValid(aoseg_relid))
		{
			rel = relation_open(aoseg_relid, AccessExclusiveLock);
			new_aoseg_oid = setNewRelfilenode(rel);
			heap_close(rel, NoLock);
		}

		if (OidIsValid(aoblkdir_relid))
		{
			rel = relation_open(aoblkdir_relid, AccessExclusiveLock);
			new_aoblkdir_oid = setNewRelfilenode(rel);
			heap_close(rel, NoLock);
		}

		/*
		 * Reconstruct the indexes to match, and we're done.
		 */
		reindex_relation(heap_relid, true, true, true, &indoids, true);
	}
}

/*
 * Check that a given rel is safe to truncate.	Subroutine for ExecuteTruncate
 */
static void
truncate_check_rel(Relation rel)
{
	/* Only allow truncate on regular or append-only tables */
	if (rel->rd_rel->relkind != RELKIND_RELATION)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is not a table",
							RelationGetRelationName(rel)),
									   errOmitLocation(true)));

	if (RelationIsExternal(rel))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is an external relation and can't be truncated",
						RelationGetRelationName(rel)),
								   errOmitLocation(true)));


	/* Permissions checks */
	if (!pg_class_ownercheck(RelationGetRelid(rel), GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
					   RelationGetRelationName(rel));

	if (!allowSystemTableModsDDL && IsSystemRelation(rel))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied: \"%s\" is a system catalog",
						RelationGetRelationName(rel)),
								   errOmitLocation(true)));

	/*
	 * We can never allow truncation of shared or nailed-in-cache relations,
	 * because we can't support changing their relfilenode values.
	 */
	if (rel->rd_rel->relisshared || rel->rd_isnailed)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot truncate system relation \"%s\"",
						RelationGetRelationName(rel)),
								   errOmitLocation(true)));

	/*
	 * Don't allow truncate on temp tables of other backends ... their local
	 * buffer manager is not going to cope.
	 */
	if (isOtherTempNamespace(RelationGetNamespace(rel)))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			  errmsg("cannot truncate temporary tables of other sessions"),
					   errOmitLocation(true)));

	/*
	 * Also check for active uses of the relation in the current transaction,
	 * including open scans and pending AFTER trigger events.
	 */
	CheckTableNotInUse(rel, "TRUNCATE");
}

/*----------
 * MergeAttributes
 *		Returns new schema given initial schema and superclasses.
 *
 * Input arguments:
 * 'schema' is the column/attribute definition for the table. (It's a list
 *		of ColumnDef's.) It is destructively changed.
 * 'supers' is a list of names (as RangeVar nodes) of parent relations.
 * 'istemp' is TRUE if we are creating a temp relation.
 * 'GpPolicy *' is NULL if the distribution policy is not to be updated
 *
 * Output arguments:
 * 'supOids' receives a list of the OIDs of the parent relations.
 * 'supconstr' receives a list of constraints belonging to the parents,
 *		updated as necessary to be valid for the child.
 * 'supOidCount' is set to the number of parents that have OID columns.
 * 'GpPolicy' is updated with the offsets of the distribution
 *      attributes in the new schema
 *
 * Return value:
 * Completed schema list.
 *
 * Notes:
 *	  The order in which the attributes are inherited is very important.
 *	  Intuitively, the inherited attributes should come first. If a table
 *	  inherits from multiple parents, the order of those attributes are
 *	  according to the order of the parents specified in CREATE TABLE.
 *
 *	  Here's an example:
 *
 *		create table person (name text, age int4, location point);
 *		create table emp (salary int4, manager text) inherits(person);
 *		create table student (gpa float8) inherits (person);
 *		create table stud_emp (percent int4) inherits (emp, student);
 *
 *	  The order of the attributes of stud_emp is:
 *
 *							person {1:name, 2:age, 3:location}
 *							/	 \
 *			   {6:gpa}	student   emp {4:salary, 5:manager}
 *							\	 /
 *						   stud_emp {7:percent}
 *
 *	   If the same attribute name appears multiple times, then it appears
 *	   in the result table in the proper location for its first appearance.
 *
 *	   Constraints (including NOT NULL constraints) for the child table
 *	   are the union of all relevant constraints, from both the child schema
 *	   and parent tables.
 *
 *	   The default value for a child column is defined as:
 *		(1) If the child schema specifies a default, that value is used.
 *		(2) If neither the child nor any parent specifies a default, then
 *			the column will not have a default.
 *		(3) If conflicting defaults are inherited from different parents
 *			(and not overridden by the child), an error is raised.
 *		(4) Otherwise the inherited default is used.
 *		Rule (3) is new in Postgres 7.1; in earlier releases you got a
 *		rather arbitrary choice of which parent default to use.
 *----------
 */
List *
MergeAttributes(List *schema, List *supers, bool istemp, bool isPartitioned,
				List **supOids, List **supconstr, int *supOidCount, GpPolicy *policy)
{
	ListCell   *entry;
	List	   *inhSchema = NIL;
	List	   *parentOids = NIL;
	List	   *constraints = NIL;
	int			parentsWithOids = 0;
	bool		have_bogus_defaults = false;
	char	   *bogus_marker = "Bogus!";		/* marks conflicting defaults */
	int			child_attno;

	/*
	 * Check for and reject tables with too many columns. We perform this
	 * check relatively early for two reasons: (a) we don't run the risk of
	 * overflowing an AttrNumber in subsequent code (b) an O(n^2) algorithm is
	 * okay if we're processing <= 1600 columns, but could take minutes to
	 * execute if the user attempts to create a table with hundreds of
	 * thousands of columns.
	 *
	 * Note that we also need to check that any we do not exceed this figure
	 * after including columns from inherited relations.
	 */
	if (list_length(schema) > MaxHeapAttributeNumber)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("tables can have at most %d columns",
						MaxHeapAttributeNumber)));

	/*
	 * Check for duplicate names in the explicit list of attributes.
	 *
	 * Although we might consider merging such entries in the same way that we
	 * handle name conflicts for inherited attributes, it seems to make more
	 * sense to assume such conflicts are errors.
	 */
	foreach(entry, schema)
	{
		ColumnDef  *coldef = lfirst(entry);
		ListCell   *rest;

		for_each_cell(rest, lnext(entry))
		{
			ColumnDef  *restdef = lfirst(rest);

			if (strcmp(coldef->colname, restdef->colname) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column \"%s\" duplicated",
								coldef->colname),
										   errOmitLocation(true)));
		}
	}

	/*
	 * Scan the parents left-to-right, and merge their attributes to form a
	 * list of inherited attributes (inhSchema).  Also check to see if we need
	 * to inherit an OID column.
	 */
	child_attno = 0;
	foreach(entry, supers)
	{
		RangeVar   *parent = (RangeVar *) lfirst(entry);
		Relation	relation;
		TupleDesc	tupleDesc;
		TupleConstr *constr;
		AttrNumber *newattno;
		AttrNumber	parent_attno;

		relation = heap_openrv(parent, AccessShareLock);

		if (relation->rd_rel->relkind != RELKIND_RELATION)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("inherited relation \"%s\" is not a table",
							parent->relname),
									   errOmitLocation(true)));
		/* Permanent rels cannot inherit from temporary ones */
		if (!istemp && isTempNamespace(RelationGetNamespace(relation)))
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot inherit from temporary relation \"%s\"",
							parent->relname),
									   errOmitLocation(true)));

		/*
		 * We should have an UNDER permission flag for this, but for now,
		 * demand that creator of a child table own the parent.
		 */
		if (!pg_class_ownercheck(RelationGetRelid(relation), GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
						   RelationGetRelationName(relation));

		/*
		 * Reject duplications in the list of parents.
		 */
		if (list_member_oid(parentOids, RelationGetRelid(relation)))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("inherited relation \"%s\" duplicated",
							parent->relname),
									   errOmitLocation(true)));

		parentOids = lappend_oid(parentOids, RelationGetRelid(relation));

		if (relation->rd_rel->relhasoids)
			parentsWithOids++;

		tupleDesc = RelationGetDescr(relation);
		constr = tupleDesc->constr;

		/*
		 * newattno[] will contain the child-table attribute numbers for the
		 * attributes of this parent table.  (They are not the same for
		 * parents after the first one, nor if we have dropped columns.)
		 */
		newattno = (AttrNumber *)
			palloc(tupleDesc->natts * sizeof(AttrNumber));

		for (parent_attno = 1; parent_attno <= tupleDesc->natts;
			 parent_attno++)
		{
			Form_pg_attribute attribute = tupleDesc->attrs[parent_attno - 1];
			char	   *attributeName = NameStr(attribute->attname);
			int			exist_attno;
			ColumnDef  *def;

			/*
			 * Ignore dropped columns in the parent.
			 */
			if (attribute->attisdropped)
			{
				/*
				 * change_varattnos_of_a_node asserts that this is greater
				 * than zero, so if anything tries to use it, we should find
				 * out.
				 */
				newattno[parent_attno - 1] = 0;
				continue;
			}

			/*
			 * Does it conflict with some previously inherited column?
			 */
			exist_attno = findAttrByName(attributeName, inhSchema);
			if (exist_attno > 0)
			{
				/*
				 * Yes, try to merge the two column definitions. They must
				 * have the same type and typmod.
				 */
				if (Gp_role == GP_ROLE_EXECUTE)
				{
					ereport(DEBUG1,
						(errmsg("merging multiple inherited definitions of column \"%s\"",
								attributeName)));
				}
				else
				ereport(NOTICE,
						(errmsg("merging multiple inherited definitions of column \"%s\"",
								attributeName)));
				def = (ColumnDef *) list_nth(inhSchema, exist_attno - 1);
				if (typenameTypeId(NULL, def->typname) != attribute->atttypid ||
					def->typname->typmod != attribute->atttypmod)
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
						errmsg("inherited column \"%s\" has a type conflict",
							   attributeName),
							 errdetail("%s versus %s",
									   TypeNameToString(def->typname),
									   format_type_be(attribute->atttypid)),
											   errOmitLocation(true)));
				def->inhcount++;
				/* Merge of NOT NULL constraints = OR 'em together */
				def->is_not_null |= attribute->attnotnull;
				/* Default and other constraints are handled below */
				newattno[parent_attno - 1] = exist_attno;

				/*
				 * Update GpPolicy
				 */
				if (policy != NULL)
				{
					int attr_ofst = 0;

					Assert(policy->nattrs >= 0 && "the number of distribution attributes is not negative");

					/* Iterate over all distribution attribute offsets */
					for (attr_ofst = 0; attr_ofst < policy->nattrs; attr_ofst++)
					{
						/* Check if any distribution attribute has higher offset than the current */
						if (policy->attrs[attr_ofst] > child_attno)
						{
							Assert(policy->attrs[attr_ofst] > 0 && "index should not become negative");
							policy->attrs[attr_ofst]--;
						}
					}
				}

			}
			else
			{
				/*
				 * No, create a new inherited column
				 */
				def = makeNode(ColumnDef);
				def->colname = pstrdup(attributeName);
				def->typname = makeTypeNameFromOid(attribute->atttypid,
													attribute->atttypmod);
				def->inhcount = 1;
				def->is_local = false;
				def->is_not_null = attribute->attnotnull;
				def->raw_default = NULL;
				def->cooked_default = NULL;
				def->constraints = NIL;
				inhSchema = lappend(inhSchema, def);
				newattno[parent_attno - 1] = ++child_attno;
			}

			/*
			 * Copy default if any
			 */
			if (attribute->atthasdef)
			{
				char	   *this_default = NULL;
				AttrDefault *attrdef;
				int			i;

				/* Find default in constraint structure */
				Assert(constr != NULL);
				attrdef = constr->defval;
				for (i = 0; i < constr->num_defval; i++)
				{
					if (attrdef[i].adnum == parent_attno)
					{
						this_default = attrdef[i].adbin;
						break;
					}
				}
				Assert(this_default != NULL);

				/*
				 * If default expr could contain any vars, we'd need to fix
				 * 'em, but it can't; so default is ready to apply to child.
				 *
				 * If we already had a default from some prior parent, check
				 * to see if they are the same.  If so, no problem; if not,
				 * mark the column as having a bogus default. Below, we will
				 * complain if the bogus default isn't overridden by the child
				 * schema.
				 */
				Assert(def->raw_default == NULL);
				if (def->cooked_default == NULL)
					def->cooked_default = pstrdup(this_default);
				else if (strcmp(def->cooked_default, this_default) != 0)
				{
					def->cooked_default = bogus_marker;
					have_bogus_defaults = true;
				}
			}
		}

		/*
		 * Now copy the constraints of this parent, adjusting attnos using the
		 * completed newattno[] map
		 */
		if (constr && constr->num_check > 0)
		{
			ConstrCheck *check = constr->check;
			int			i;

			for (i = 0; i < constr->num_check; i++)
			{
				Constraint *cdef = makeNode(Constraint);
				Node	   *expr;

				cdef->contype = CONSTR_CHECK;
				cdef->name = pstrdup(check[i].ccname);
				cdef->raw_expr = NULL;
				/* adjust varattnos of ccbin here */
				expr = stringToNode(check[i].ccbin);
				change_varattnos_of_a_node(expr, newattno);
				cdef->cooked_expr = nodeToString(expr);
				constraints = lappend(constraints, cdef);
			}
		}

		pfree(newattno);

		/*
		 * Close the parent rel, but keep our AccessShareLock on it until xact
		 * commit.	That will prevent someone else from deleting or ALTERing
		 * the parent before the child is committed.
		 */
		heap_close(relation, NoLock);
	}

	/*
	 * If we had no inherited attributes, the result schema is just the
	 * explicitly declared columns.  Otherwise, we need to merge the declared
	 * columns into the inherited schema list.
	 */
	if (inhSchema != NIL)
	{
		foreach(entry, schema)
		{
			ColumnDef  *newdef = lfirst(entry);
			char	   *attributeName = newdef->colname;
			int			exist_attno;

			/*
			 * Does it conflict with some previously inherited column?
			 */
			exist_attno = findAttrByName(attributeName, inhSchema);
			if (exist_attno > 0)
			{
				ColumnDef  *def;

				/*
				 * Yes, try to merge the two column definitions. They must
				 * have the same type and typmod.
				 */
				if (Gp_role == GP_ROLE_EXECUTE)
				{
					ereport(DEBUG1,
					   (errmsg("merging column \"%s\" with inherited definition",
							   attributeName)));
				}
				else
				ereport(NOTICE,
				   (errmsg("merging column \"%s\" with inherited definition",
						   attributeName)));
				def = (ColumnDef *) list_nth(inhSchema, exist_attno - 1);
				if (typenameTypeId(NULL, def->typname) != typenameTypeId(NULL, newdef->typname) ||
					def->typname->typmod != newdef->typname->typmod)
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("column \"%s\" has a type conflict",
									attributeName),
							 errdetail("%s versus %s",
									   TypeNameToString(def->typname),
									   TypeNameToString(newdef->typname))));
				/* Mark the column as locally defined */
				def->is_local = true;
				/* Merge of NOT NULL constraints = OR 'em together */
				def->is_not_null |= newdef->is_not_null;
				/* If new def has a default, override previous default */
				if (newdef->raw_default != NULL)
				{
					def->raw_default = newdef->raw_default;
					def->cooked_default = newdef->cooked_default;
				}
			}
			else
			{
				/*
				 * No, attach new column to result schema
				 */
				inhSchema = lappend(inhSchema, newdef);
			}
		}

		schema = inhSchema;

		/*
		 * Check that we haven't exceeded the legal # of columns after merging
		 * in inherited columns.
		 */
		if (list_length(schema) > MaxHeapAttributeNumber)
			ereport(ERROR,
					(errcode(ERRCODE_TOO_MANY_COLUMNS),
					 errmsg("tables can have at most %d columns",
							MaxHeapAttributeNumber)));
	}

	/*
	 * If we found any conflicting parent default values, check to make sure
	 * they were overridden by the child.
	 */
	if (have_bogus_defaults)
	{
		foreach(entry, schema)
		{
			ColumnDef  *def = lfirst(entry);

			if (def->cooked_default == bogus_marker)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
				  errmsg("column \"%s\" inherits conflicting default values",
						 def->colname),
						 errhint("To resolve the conflict, specify a default explicitly.")));
		}
	}

	*supOids = parentOids;
	*supconstr = constraints;
	*supOidCount = parentsWithOids;
	return schema;
}


/*
 * In multiple-inheritance situations, it's possible to inherit
 * the same grandparent constraint through multiple parents.
 * Hence, we want to discard inherited constraints that match as to
 * both name and expression.  Otherwise, gripe if there are conflicting
 * names. Nonconflicting constraints are added back to the CreateStmt->constraints
 */
static bool
add_nonduplicate_cooked_constraint(Constraint *cdef, List *stmtConstraints)
{
	/* Should only see precooked constraints here */
	Assert(cdef->contype == CONSTR_CHECK);
	Assert(cdef->name != NULL);
	Assert(cdef->raw_expr == NULL && cdef->cooked_expr != NULL);

	ListCell 	*listptr;

	foreach(listptr, stmtConstraints)
	{
		Constraint *stmtCdef = (Constraint *) lfirst(listptr);

		/* Constraint in CreateStmt may be raw and maynot be a check constraint */
		if (stmtCdef->cooked_expr == NULL || stmtCdef->contype != CONSTR_CHECK)
			continue;
		if (strcmp(stmtCdef->name, cdef->name) != 0)
			continue;
		if (strcmp(stmtCdef->cooked_expr, cdef->cooked_expr) == 0)
			return false;
	}
	return true;
}


/*
 * Replace varattno values in an expression tree according to the given
 * map array, that is, varattno N is replaced by newattno[N-1].  It is
 * caller's responsibility to ensure that the array is long enough to
 * define values for all user varattnos present in the tree.  System column
 * attnos remain unchanged. For historical reason, we only map varattno of the first
 * range table entry from this method. So, we call the more general
 * change_varattnos_of_a_varno() with varno set to 1
 *
 * Note that the passed node tree is modified in-place!
 */
void
change_varattnos_of_a_node(Node *node, const AttrNumber *newattno)
{
	/* Only attempt re-mapping if re-mapping is necessary (i.e., non-null newattno map) */
	if (newattno)
	{
		change_varattnos_of_a_varno(node, newattno, 1 /* varno is hard-coded to 1 (i.e., only first RTE) */);
	}
}

/*
 * Replace varattno values for a given varno RTE index in an expression
 * tree according to the given map array, that is, varattno N is replaced
 * by newattno[N-1].  It is caller's responsibility to ensure that the array
 * is long enough to define values for all user varattnos present in the tree.
 * System column attnos remain unchanged.
 *
 * Note that the passed node tree is modified in-place!
 */
void
change_varattnos_of_a_varno(Node *node, const AttrNumber *newattno, Index varno)
{
	AttrMapContext attrMapCxt;

	attrMapCxt.newattno = newattno;
	attrMapCxt.varno = varno;

	(void) change_varattnos_varno_walker(node, &attrMapCxt);
}

/*
 * Remaps the varattno of a varattno in a Var node using an attribute map.
 */
static bool
change_varattnos_varno_walker(Node *node, const AttrMapContext *attrMapCxt)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varlevelsup == 0 && (var->varno == attrMapCxt->varno) &&
			var->varattno > 0)
		{
			/*
			 * ??? the following may be a problem when the node is multiply
			 * referenced though stringToNode() doesn't create such a node
			 * currently.
			 */
			Assert(attrMapCxt->newattno[var->varattno - 1] > 0);
			var->varattno = var->varoattno = attrMapCxt->newattno[var->varattno - 1];
		}
		return false;
	}
	return expression_tree_walker(node, change_varattnos_varno_walker,
								  (void *) attrMapCxt);
}

/*
 * Generate a map for change_varattnos_of_a_node from old and new TupleDesc's,
 * matching according to column name. This function returns a NULL pointer (i.e.
 * null map) if no mapping is necessary (i.e., old and new TupleDesc are already
 * aligned).
 */
AttrNumber *
varattnos_map(TupleDesc old, TupleDesc new)
{
	AttrNumber *attmap;
	int			i,
				j;

	bool mapRequired = false;

	attmap = (AttrNumber *) palloc0(sizeof(AttrNumber) * old->natts);
	for (i = 1; i <= old->natts; i++)
	{
		if (old->attrs[i - 1]->attisdropped)
			continue;			/* leave the entry as zero */

		for (j = 1; j <= new->natts; j++)
		{
			if (strcmp(NameStr(old->attrs[i - 1]->attname),
					   NameStr(new->attrs[j - 1]->attname)) == 0)
			{
				attmap[i - 1] = j;

				if (i != j)
				{
					mapRequired = true;
				}
				break;
			}
		}
	}

	if (!mapRequired)
	{
		pfree(attmap);

		/* No mapping required, so return NULL */
		attmap = NULL;
	}

	return attmap;
}

/*
 * Generate a map for change_varattnos_of_a_node from a TupleDesc and a list
 * of ColumnDefs
 */
AttrNumber *
varattnos_map_schema(TupleDesc old, List *schema)
{
	AttrNumber *attmap;
	int			i;

	attmap = (AttrNumber *) palloc0(sizeof(AttrNumber) * old->natts);
	for (i = 1; i <= old->natts; i++)
	{
		if (old->attrs[i - 1]->attisdropped)
			continue;			/* leave the entry as zero */

		attmap[i - 1] = findAttrByName(NameStr(old->attrs[i - 1]->attname),
									   schema);
	}
	return attmap;
}


/*
 * StoreCatalogInheritance
 *		Updates the system catalogs with proper inheritance information.
 *
 * supers is a list of the OIDs of the new relation's direct ancestors.
 */
static void
StoreCatalogInheritance(Oid relationId, List *supers)
{
	Relation	relation;
	int16		seqNumber;
	ListCell   *entry;

	/*
	 * sanity checks
	 */
	AssertArg(OidIsValid(relationId));

	if (supers == NIL)
		return;

	/*
	 * Store INHERITS information in pg_inherits using direct ancestors only.
	 * Also enter dependencies on the direct ancestors, and make sure they are
	 * marked with relhassubclass = true.
	 *
	 * (Once upon a time, both direct and indirect ancestors were found here
	 * and then entered into pg_ipl.  Since that catalog doesn't exist
	 * anymore, there's no need to look for indirect ancestors.)
	 */
	relation = heap_open(InheritsRelationId, RowExclusiveLock);

	seqNumber = 1;
	foreach(entry, supers)
	{
		Oid			parentOid = lfirst_oid(entry);

		StoreCatalogInheritance1(relationId, parentOid, seqNumber, relation,
								 false);
		seqNumber++;
	}

	heap_close(relation, RowExclusiveLock);
}

/*
 * Make catalog entries showing relationId as being an inheritance child
 * of parentOid.  inhRelation is the already-opened pg_inherits catalog.
 */
static void
StoreCatalogInheritance1(Oid relationId, Oid parentOid,
						 int16 seqNumber, Relation inhRelation,
						 bool is_partition)
{
	Datum		datum[Natts_pg_inherits];
	bool		nullarr[Natts_pg_inherits];
	ObjectAddress childobject,
				parentobject;
	HeapTuple	tuple;
	cqContext	cqc;
	cqContext  *pcqCtx;

	Assert(RelationGetRelid(inhRelation) == InheritsRelationId);

	/*
	 * Make the pg_inherits entry
	 */
	datum[0] = ObjectIdGetDatum(relationId);	/* inhrelid */
	datum[1] = ObjectIdGetDatum(parentOid);		/* inhparent */
	datum[2] = Int16GetDatum(seqNumber);		/* inhseqno */

	nullarr[0] = false;
	nullarr[1] = false;
	nullarr[2] = false;

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), inhRelation),
			cql("INSERT INTO pg_inherits",
				NULL));

	tuple = caql_form_tuple(pcqCtx, datum, nullarr);

	caql_insert(pcqCtx, tuple); /* implicit update of index as well */

	heap_freetuple(tuple);
	caql_endscan(pcqCtx);

	/*
	 * Store a dependency too
	 */
	parentobject.classId = RelationRelationId;
	parentobject.objectId = parentOid;
	parentobject.objectSubId = 0;
	childobject.classId = RelationRelationId;
	childobject.objectId = relationId;
	childobject.objectSubId = 0;

	recordDependencyOn(&childobject, &parentobject,
					   is_partition ? DEPENDENCY_AUTO : DEPENDENCY_NORMAL);

	/*
	 * Mark the parent as having subclasses.
	 */
	setRelhassubclassInRelation(parentOid, true);
}

/*
 * Look for an existing schema entry with the given name.
 *
 * Returns the index (starting with 1) if attribute already exists in schema,
 * 0 if it doesn't.
 */
static int
findAttrByName(const char *attributeName, List *schema)
{
	ListCell   *s;
	int			i = 1;

	foreach(s, schema)
	{
		ColumnDef  *def = lfirst(s);

		if (strcmp(attributeName, def->colname) == 0)
			return i;

		i++;
	}
	return 0;
}

/*
 * Update a relation's pg_class.relhassubclass entry to the given value
 */
static void
setRelhassubclassInRelation(Oid relationId, bool relhassubclass)
{
	Relation	relationRelation;
	HeapTuple	tuple;
	Form_pg_class classtuple;
	cqContext	cqc;
	cqContext  *pcqCtx;

	/*
	 * Fetch a modifiable copy of the tuple, modify it, update pg_class.
	 *
	 * If the tuple already has the right relhassubclass setting, we don't
	 * need to update it, but we still need to issue an SI inval message.
	 */
	relationRelation = heap_open(RelationRelationId, RowExclusiveLock);
	
	pcqCtx = caql_addrel(cqclr(&cqc), relationRelation);

	tuple = caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(relationId)));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relationId);
	classtuple = (Form_pg_class) GETSTRUCT(tuple);

	if (classtuple->relhassubclass != relhassubclass)
	{
		classtuple->relhassubclass = relhassubclass;
		caql_update_current(pcqCtx, tuple);
		/* and Update indexes (implicit) */

	}
	else
	{
		/* no need to change tuple, but force relcache rebuild anyway */
		CacheInvalidateRelcacheByTuple(tuple);
	}

	heap_freetuple(tuple);
	heap_close(relationRelation, NoLock);
}


/*
 *		renameatt		- changes the name of a attribute in a relation
 *
 *		Attname attribute is changed in attribute catalog.
 *		No record of the previous attname is kept (correct?).
 *
 *		get proper relrelation from relation catalog (if not arg)
 *		scan attribute catalog
 *				for name conflict (within rel)
 *				for original attribute (if not arg)
 *		modify attname in attribute tuple
 *		insert modified attribute in attribute catalog
 *		delete original attribute from attribute catalog
 */
void
renameatt(Oid myrelid,
		  const char *oldattname,
		  const char *newattname,
		  bool recurse,
		  bool recursing)
{
	Relation	targetrelation;
	Relation	attrelation;
	HeapTuple	atttup;
	Form_pg_attribute attform;
	int			attnum;
	List	   *indexoidlist;
	ListCell   *indexoidscan;
	cqContext	cqc;
	cqContext	cqc2;
	cqContext  *pcqCtx;

	/*
	 * Grab an exclusive lock on the target table, which we will NOT release
	 * until end of transaction.
	 */
	targetrelation = relation_open(myrelid, AccessExclusiveLock);

	/*
	 * permissions checking.  this would normally be done in utility.c, but
	 * this particular routine is recursive.
	 *
	 * normally, only the owner of a class can change its schema.
	 */
	if (!pg_class_ownercheck(myrelid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
					   RelationGetRelationName(targetrelation));
	if (!allowSystemTableModsDDL && IsSystemRelation(targetrelation))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied: \"%s\" is a system catalog",
						RelationGetRelationName(targetrelation))));

	if (RelationIsParquet(targetrelation))
		ereport(ERROR,
				(errcode(ERRCODE_CDB_FEATURE_NOT_SUPPORTED),
				 errmsg("Unsupported Rename column command for table type parquet"),
				 errOmitLocation(true)));

	/*
	 * if the 'recurse' flag is set then we are supposed to rename this
	 * attribute in all classes that inherit from 'relname' (as well as in
	 * 'relname').
	 *
	 * any permissions or problems with duplicate attributes will cause the
	 * whole transaction to abort, which is what we want -- all or nothing.
	 */
	if (recurse)
	{
		ListCell   *child;
		List	   *children;

		/* this routine is actually in the planner */
		children = find_all_inheritors(myrelid);

		/*
		 * find_all_inheritors does the recursive search of the inheritance
		 * hierarchy, so all we have to do is process all of the relids in the
		 * list that it returns.
		 */
		foreach(child, children)
		{
			Oid			childrelid = lfirst_oid(child);

			if (childrelid == myrelid)
				continue;
			/* note we need not recurse again */
			renameatt(childrelid, oldattname, newattname, false, true);
		}
	}
	else
	{
		/*
		 * If we are told not to recurse, there had better not be any child
		 * tables; else the rename would put them out of step.
		 */
		if (!recursing &&
			find_inheritance_children(myrelid) != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("inherited column \"%s\" must be renamed in child tables too",
							oldattname)));
	}

	attrelation = heap_open(AttributeRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), attrelation);

	atttup = caql_getattname(pcqCtx, myrelid, oldattname);
	if (!HeapTupleIsValid(atttup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" does not exist",
						oldattname),
				 errOmitLocation(true)));
	attform = (Form_pg_attribute) GETSTRUCT(atttup);

	attnum = attform->attnum;
	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot rename system column \"%s\"",
						oldattname)));

	/*
	 * if the attribute is inherited, forbid the renaming, unless we are
	 * already inside a recursive rename.
	 */
	if (attform->attinhcount > 0 && !recursing)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot rename inherited column \"%s\"",
						oldattname)));

	/* should not already exist */
	/* this test is deliberately not attisdropped-aware */
	if (caql_getcount(
				caql_addrel(cqclr(&cqc2), attrelation),
				cql("SELECT COUNT(*) FROM pg_attribute "
					" WHERE attrelid = :1 "
					" AND attname = :2 ",
					ObjectIdGetDatum(myrelid),
					PointerGetDatum((char *) newattname))))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" already exists",
					  newattname, RelationGetRelationName(targetrelation)),
				 errOmitLocation(true)));
	}
	namestrcpy(&(attform->attname), newattname);

	caql_update_current(pcqCtx, atttup); /* implicit update of index as well */

	heap_freetuple(atttup);

	/*
	 * Update column names of indexes that refer to the column being renamed.
	 */
	indexoidlist = RelationGetIndexList(targetrelation);

	foreach(indexoidscan, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(indexoidscan);
		HeapTuple	indextup;
		Form_pg_index indexform;
		int			i;
		cqContext  *pidxCtx;

		/*
		 * Scan through index columns to see if there's any simple index
		 * entries for this attribute.	We ignore expressional entries.
		 */

		pidxCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_index "
					" WHERE indexrelid = :1 ",
					ObjectIdGetDatum(indexoid)));

		indextup = caql_getnext(pidxCtx);

		if (!HeapTupleIsValid(indextup))
			elog(ERROR, "cache lookup failed for index %u", indexoid);
		indexform = (Form_pg_index) GETSTRUCT(indextup);

		for (i = 0; i < indexform->indnatts; i++)
		{
			if (attnum != indexform->indkey.values[i])
				continue;

			/*
			 * Found one, rename it.
			 */
			pcqCtx = caql_addrel(cqclr(&cqc), attrelation);
			
			atttup = caql_getfirst(
					pcqCtx,
					cql("SELECT * FROM pg_attribute "
						" WHERE attrelid = :1 "
						" AND attnum = :2 "
						" FOR UPDATE ",
						ObjectIdGetDatum(indexoid),
						Int16GetDatum(i + 1)));

			if (!HeapTupleIsValid(atttup))
				continue;		/* should we raise an error? */

			/*
			 * Update the (copied) attribute tuple.
			 */
			namestrcpy(&(((Form_pg_attribute) GETSTRUCT(atttup))->attname),
					   newattname);

			caql_update_current(pcqCtx, atttup);
			/* and Update indexes (implicit) */

			heap_freetuple(atttup);
		}

		caql_endscan(pidxCtx);
	}

	list_free(indexoidlist);

	heap_close(attrelation, RowExclusiveLock);

	/*
	 * Update att name in any RI triggers associated with the relation.
	 */
	if (targetrelation->rd_rel->reltriggers > 0)
	{
		/* update tgargs column reference where att is primary key */
		update_ri_trigger_args(RelationGetRelid(targetrelation),
							   oldattname, newattname,
							   false, false);
		/* update tgargs column reference where att is foreign key */
		update_ri_trigger_args(RelationGetRelid(targetrelation),
							   oldattname, newattname,
							   true, false);
	}

	/* MPP-6929, MPP-7600: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(targetrelation->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(targetrelation),
						   GetUserId(),
						   "ALTER", "RENAME COLUMN"
				);


	relation_close(targetrelation, NoLock);		/* close rel but keep lock */
}

/*
 *		renamerel		- change the name of a relation
 *
 *		XXX - When renaming sequences, we don't bother to modify the
 *			  sequence name that is stored within the sequence itself
 *			  (this would cause problems with MVCC). In the future,
 *			  the sequence name should probably be removed from the
 *			  sequence, AFAIK there's no need for it to be there.
 */
void
renamerel(Oid myrelid, const char *newrelname, RenameStmt *stmt)
{
	Relation		pg_class_desc;
	HeapTuple		tuple;
	Oid				typeId;
	Oid				namespaceId;
	char			relkind;
	char			oldrelname[NAMEDATALEN];
	bool			relhastriggers;
	Form_pg_class	form;
	bool			isSystemRelation;
	cqContext		cqc;
	cqContext	   *pcqCtx;

	/* if this is a child table of a partitioning configuration, complain */
	if (stmt && rel_is_child_partition(myrelid) && !stmt->bAllowPartn)
	{
		Oid		 master = rel_partition_get_master(myrelid);
		char	*pretty	= rel_get_part_path_pretty(myrelid,
												   " ALTER PARTITION ",
												   " RENAME PARTITION ");
		ereport(ERROR,
				(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
				errmsg("cannot rename partition \"%s\" directly",
					   get_rel_name(myrelid)),
				errhint("Table \"%s\" is a child partition of table "
						"\"%s\".  To rename it, use ALTER TABLE \"%s\"%s...",
						get_rel_name(myrelid), get_rel_name(master),
						get_rel_name(master), pretty ? pretty : "" ),
				errOmitLocation(true)));
	}

	pg_class_desc = heap_open(RelationRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), pg_class_desc);

	tuple = caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(myrelid)));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("relation \"%d\" does not exist", myrelid)));

	form = (Form_pg_class) GETSTRUCT(tuple);
	relkind = form->relkind;
	namespaceId = form->relnamespace;
	relhastriggers = form->reltriggers;
	typeId = form->reltype;
	strncpy(&oldrelname[0], (form->relname).data, NAMEDATALEN);

	isSystemRelation = IsSystemNamespace(namespaceId) ||
					   IsToastNamespace(namespaceId) ||
					   IsAoSegmentNamespace(namespaceId);

	Assert (allowSystemTableModsDDL || !isSystemRelation);

	/*
	 * Find relation's pg_class tuple, and make sure newrelname isn't in use.
	 */
	if (get_relname_relid(newrelname, namespaceId) != InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" already exists",
						newrelname),
				 errOmitLocation(true)));

	/*
	 * Update pg_class tuple with new relname.  (Scribbling on tuple is OK
	 * because it's a copy...)
	 */
	namestrcpy(&(form->relname), newrelname);

	caql_update_current(pcqCtx, tuple); /* implicit update of index as well */

	heap_freetuple(tuple);
	heap_close(pg_class_desc, NoLock);

	/*
	 * Also rename the associated type, if any.
	 */
	if (relkind != RELKIND_INDEX)
	{
		if (!TypeTupleExists(typeId))
			ereport(WARNING,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("type \"%s\" does not exist", oldrelname)));
		else
			TypeRename(typeId, newrelname);
	}

	/*
	 * Update rel name in any RI triggers associated with the relation.
	 */
	if (relhastriggers)
	{
		/* update tgargs where relname is primary key */
		update_ri_trigger_args(myrelid,
							   oldrelname,
							   newrelname,
							   false, true);

		/* update tgargs where relname is foreign key */
		update_ri_trigger_args(myrelid,
							   oldrelname,
							   newrelname,
							   true, true);
	}


	/* MPP-3059: recursive rename of partitioned table */
	/* Note: the top-level RENAME has bAllowPartn=FALSE, while the
	 * generated statements from this block set it to TRUE.  That way,
	 * the rename of each partition is allowed, but this block doesn't
	 * get invoked recursively.
	 */
	if (stmt && !rel_is_child_partition(myrelid) && !stmt->bAllowPartn &&
		(Gp_role == GP_ROLE_DISPATCH))
	{
		PartitionNode *pNode;
		pNode = RelationBuildPartitionDescByOid(myrelid, false);

		if (pNode)
		{
			RenameStmt 			   	*renStmt 	 = makeNode(RenameStmt);
			DestReceiver 		   	*dest  		 = None_Receiver;
			List 					*renList 	 = NIL;
			int 					 skipped 	 = 0;
			int 					 renamed 	 = 0;

			renStmt->renameType = OBJECT_TABLE;
			renStmt->subname = NULL;
			renStmt->bAllowPartn = true; /* allow rename of partitions */

			/* rename the children as well */
			renList = atpxRenameList(pNode, oldrelname, newrelname, &skipped);

			/* process children if there are any */
			if (renList)
			{
				ListCell 		*lc;

				foreach(lc, renList)
				{
					ListCell 		*lc2;
					List  			*lpair = lfirst(lc);

					lc2 = list_head(lpair);

					renStmt->relation = (RangeVar *)lfirst(lc2);
					lc2 = lnext(lc2);
					renStmt->newname = (char *)lfirst(lc2);

					ProcessUtility((Node *) renStmt,
								   synthetic_sql,
								   NULL,
								   false, /* not top level */
								   dest,
								   NULL);
					renamed++;
				}

				/* MPP-3542: warn when skip child partitions */
				if (skipped)
				{
					ereport(WARNING,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("renamed %d partitions, "
									"skipped %d child partitions "
									"due to name truncation",
									renamed, skipped),
							 errOmitLocation(true)));
				}
			}
		}
	}

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&&
		/* MPP-7773: don't track objects in system namespace
		 * if modifying system tables (eg during upgrade)
		 */
		( ! ( (PG_CATALOG_NAMESPACE == namespaceId) && (allowSystemTableModsDDL)))
		&& (   MetaTrackValidRelkind(relkind)
			&& METATRACK_VALIDNAMESPACE(namespaceId)
			   && (!(isAnyTempNamespace(namespaceId)))
				))
		MetaTrackUpdObject(RelationRelationId,
						   myrelid,
						   GetUserId(),
						   "ALTER", "RENAME"
				);
}

static bool
TypeTupleExists(Oid typeId)
{
	Relation	pg_type_desc;
	cqContext	cqc;
	bool		bExists;

	pg_type_desc = heap_open(TypeRelationId, AccessShareLock);

	bExists = (0 < 
			   caql_getcount(
					   caql_addrel(cqclr(&cqc), pg_type_desc),
					   cql("SELECT COUNT(*) FROM pg_type "
						   " WHERE oid = :1 ",
						   ObjectIdGetDatum(typeId))));

	heap_close(pg_type_desc, AccessShareLock);

	return (bExists);
}

/*
 * Scan pg_trigger for RI triggers that are on the specified relation
 * (if fk_scan is false) or have it as the tgconstrrel (if fk_scan
 * is true).  Update RI trigger args fields matching oldname to contain
 * newname instead.  If update_relname is true, examine the relname
 * fields; otherwise examine the attname fields.
 */
static void
update_ri_trigger_args(Oid relid,
					   const char *oldname,
					   const char *newname,
					   bool fk_scan,
					   bool update_relname)
{
	cqContext  *pcqCtx;
	HeapTuple	tuple;
	Datum		values[Natts_pg_trigger];
	bool		nulls[Natts_pg_trigger];
	bool		replaces[Natts_pg_trigger];

	if (fk_scan)
	{
		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_trigger "
					" WHERE tgconstrrelid = :1 "
					" FOR UPDATE ",
					ObjectIdGetDatum(relid)));
	}
	else
	{
		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_trigger "
					" WHERE tgrelid = :1 "
					" FOR UPDATE ",
					ObjectIdGetDatum(relid)));
	}

	while (HeapTupleIsValid(tuple = caql_getnext(pcqCtx)))
	{
		Form_pg_trigger pg_trigger = (Form_pg_trigger) GETSTRUCT(tuple);
		bytea	   *val;
		bytea	   *newtgargs;
		bool		isnull;
		int			tg_type;
		bool		examine_pk;
		bool		changed;
		int			tgnargs;
		int			i;
		int			newlen;
		const char *arga[RI_MAX_ARGUMENTS];
		const char *argp;

		tg_type = RI_FKey_trigger_type(pg_trigger->tgfoid);
		if (tg_type == RI_TRIGGER_NONE)
		{
			/* Not an RI trigger, forget it */
			continue;
		}

		/*
		 * It is an RI trigger, so parse the tgargs bytea.
		 *
		 * NB: we assume the field will never be compressed or moved out of
		 * line; so does trigger.c ...
		 */
		tgnargs = pg_trigger->tgnargs;
		val = DatumGetByteaP(caql_getattr(pcqCtx,
										  Anum_pg_trigger_tgargs,
										  &isnull));
		if (isnull || tgnargs < RI_FIRST_ATTNAME_ARGNO ||
			tgnargs > RI_MAX_ARGUMENTS)
		{
			/* This probably shouldn't happen, but ignore busted triggers */
			continue;
		}
		argp = (const char *) VARDATA(val);
		for (i = 0; i < tgnargs; i++)
		{
			arga[i] = argp;
			argp += strlen(argp) + 1;
		}

		/*
		 * Figure out which item(s) to look at.  If the trigger is primary-key
		 * type and attached to my rel, I should look at the PK fields; if it
		 * is foreign-key type and attached to my rel, I should look at the FK
		 * fields.	But the opposite rule holds when examining triggers found
		 * by tgconstrrel search.
		 */
		examine_pk = (tg_type == RI_TRIGGER_PK) == (!fk_scan);

		changed = false;
		if (update_relname)
		{
			/* Change the relname if needed */
			i = examine_pk ? RI_PK_RELNAME_ARGNO : RI_FK_RELNAME_ARGNO;
			if (strcmp(arga[i], oldname) == 0)
			{
				arga[i] = newname;
				changed = true;
			}
		}
		else
		{
			/* Change attname(s) if needed */
			i = examine_pk ? RI_FIRST_ATTNAME_ARGNO + RI_KEYPAIR_PK_IDX :
				RI_FIRST_ATTNAME_ARGNO + RI_KEYPAIR_FK_IDX;
			for (; i < tgnargs; i += 2)
			{
				if (strcmp(arga[i], oldname) == 0)
				{
					arga[i] = newname;
					changed = true;
				}
			}
		}

		if (!changed)
		{
			/* Don't need to update this tuple */
			continue;
		}

		/*
		 * Construct modified tgargs bytea.
		 */
		newlen = VARHDRSZ;
		for (i = 0; i < tgnargs; i++)
			newlen += strlen(arga[i]) + 1;
		newtgargs = (bytea *) palloc(newlen);
		SET_VARSIZE(newtgargs, newlen);
		newlen = VARHDRSZ;
		for (i = 0; i < tgnargs; i++)
		{
			strcpy(((char *) newtgargs) + newlen, arga[i]);
			newlen += strlen(arga[i]) + 1;
		}

		/*
		 * Build modified tuple.
		 */
		for (i = 0; i < Natts_pg_trigger; i++)
		{
			values[i] = (Datum) 0;
			replaces[i] = false;
			nulls[i] = false;
		}
		values[Anum_pg_trigger_tgargs - 1] = PointerGetDatum(newtgargs);
		replaces[Anum_pg_trigger_tgargs - 1] = true;

		tuple = caql_modify_current(pcqCtx, values, nulls, replaces);

		/*
		 * Update pg_trigger and its indexes
		 */
		caql_update_current(pcqCtx, tuple);

		/*
		 * Invalidate trigger's relation's relcache entry so that other
		 * backends (and this one too!) are sent SI message to make them
		 * rebuild relcache entries.  (Ideally this should happen
		 * automatically...)
		 *
		 * We can skip this for triggers on relid itself, since that relcache
		 * flush will happen anyway due to the table or column rename.	We
		 * just need to catch the far ends of RI relationships.
		 */
		pg_trigger = (Form_pg_trigger) GETSTRUCT(tuple);
		if (pg_trigger->tgrelid != relid)
			CacheInvalidateRelcacheByRelid(pg_trigger->tgrelid);

		/* free up our scratch memory */
		pfree(newtgargs);
		heap_freetuple(tuple);
	}
	
	caql_endscan(pcqCtx);

	/*
	 * Increment cmd counter to make updates visible; this is needed in case
	 * the same tuple has to be updated again by next pass (can happen in case
	 * of a self-referential FK relationship).
	 */
	CommandCounterIncrement();
}

/*
 * Disallow ALTER TABLE (and similar commands) when the current backend has
 * any open reference to the target table besides the one just acquired by
 * the calling command; this implies there's an open cursor or active plan.
 * We need this check because our AccessExclusiveLock doesn't protect us
 * against stomping on our own foot, only other people's feet!
 *
 * For ALTER TABLE, the only case known to cause serious trouble is ALTER
 * COLUMN TYPE, and some changes are obviously pretty benign, so this could
 * possibly be relaxed to only error out for certain types of alterations.
 * But the use-case for allowing any of these things is not obvious, so we
 * won't work hard at it for now.
 *
 * We also reject these commands if there are any pending AFTER trigger events
 * for the rel.  This is certainly necessary for the rewriting variants of
 * ALTER TABLE, because they don't preserve tuple TIDs and so the pending
 * events would try to fetch the wrong tuples.  It might be overly cautious
 * in other cases, but again it seems better to err on the side of paranoia.
 *
 * REINDEX calls this with "rel" referencing the index to be rebuilt; here
 * we are worried about active indexscans on the index.  The trigger-event
 * check can be skipped, since we are doing no damage to the parent table.
 *
 * The statement name (eg, "ALTER TABLE") is passed for use in error messages.
 */
void
CheckTableNotInUse(Relation rel, const char *stmt)
{
	int			expected_refcnt;

	expected_refcnt = rel->rd_isnailed ? 2 : 1;

	/*
	 * XXX For a bitmap index, since vacuum (or vacuum full) is currently done through
	 * reindex_index, the reference count could be 2 (or 3). We set it
	 * here until vacuum is done properly.
	 */
	if (expected_refcnt == 1 &&
		RelationIsBitmapIndex(rel) &&
		(rel->rd_refcnt == 2 || rel->rd_refcnt == 3))
		expected_refcnt = rel->rd_refcnt;

	if (rel->rd_refcnt != expected_refcnt)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 /* translator: first %s is a SQL command, eg ALTER TABLE */
				 errmsg("cannot %s \"%s\" because "
						"it is being used by active queries in this session",
						stmt, RelationGetRelationName(rel))));

	if (rel->rd_rel->relkind != RELKIND_INDEX &&
		AfterTriggerPendingOnRel(RelationGetRelid(rel)))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 /* translator: first %s is a SQL command, eg ALTER TABLE */
				 errmsg("cannot %s \"%s\" because "
						"it has pending trigger events",
						stmt, RelationGetRelationName(rel))));
}

static
void ATVerifyObject(AlterTableStmt *stmt, Relation rel)
{

	/* Nothing to check for ALTER INDEX */
	if(stmt->relkind == OBJECT_INDEX)
		return;
	

	/* 
	 * Verify the object specified against relstorage in the catalog.
	 * Enforce correct syntax usage. 
	 */
	if (RelationIsForeign(rel) && stmt->relkind != OBJECT_FOREIGNTABLE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is a foreign table", RelationGetRelationName(rel)),
				 errhint("Use ALTER FOREIGN TABLE instead"),
				 errOmitLocation(true)));
	}
	else if (RelationIsExternal(rel) && stmt->relkind != OBJECT_EXTTABLE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is an external table", RelationGetRelationName(rel)),
				 errhint("Use ALTER EXTERNAL TABLE instead"),
				 errOmitLocation(true)));
	}
	else if (!RelationIsExternal(rel) && !RelationIsForeign(rel) && stmt->relkind != OBJECT_TABLE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is a base table", RelationGetRelationName(rel)),
				 errhint("Use ALTER TABLE instead"),
				 errOmitLocation(true)));
	}
	
	/*
	 * Check the ALTER command type is supported for this object
	 */
	if (RelationIsForeign(rel) || RelationIsExternal(rel))
	{
		ListCell *lcmd;
		
		foreach(lcmd, stmt->cmds)
		{
			AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lcmd);

			switch(cmd->subtype)
			{
				/* FOREIGN and EXTERNAL tables doesn't support the following AT */
				case AT_ColumnDefault:		
				case AT_DropNotNull:			
				case AT_SetNotNull:			
				case AT_SetStatistics:		
				case AT_SetStorage:			
				case AT_AddIndex:			
				case AT_ReAddIndex:			
				case AT_AddConstraint:		
				case AT_AddConstraintRecurse:
				case AT_ProcessedConstraint:	
				case AT_DropConstraint:		
				case AT_DropConstraintQuietly:			
				case AT_ClusterOn:			
				case AT_DropCluster:			
				case AT_DropOids:			
				case AT_SetTableSpace:		
				case AT_SetRelOptions:		
				case AT_ResetRelOptions:		
				case AT_EnableTrig:			
				case AT_DisableTrig:			
				case AT_EnableTrigAll:		
				case AT_DisableTrigAll:		
				case AT_EnableTrigUser:		
				case AT_DisableTrigUser:		
				case AT_AddInherit:			
				case AT_DropInherit:			
				case AT_SetDistributedBy:	
				case AT_PartAdd:				
				case AT_PartAlter:			
				case AT_PartCoalesce:		
				case AT_PartDrop:			
				case AT_PartExchange:		
				case AT_PartMerge:			
				case AT_PartModify:			
				case AT_PartRename:			
				case AT_PartSetTemplate:		
				case AT_PartSplit:			
				case AT_PartTruncate:		
				case AT_PartAddInternal:		
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
							 errmsg("Unsupported ALTER command for table type %s", 
									 (RelationIsExternal(rel) ? "external" : "foreign")),
							 errOmitLocation(true)));
					break;
				
				case AT_AddColumn: /* check no constraint is added too */
					if(((ColumnDef *) cmd->def)->constraints != NIL ||
					   ((ColumnDef *) cmd->def)->is_not_null ||
					   ((ColumnDef *) cmd->def)->raw_default)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
								 errmsg("Unsupported ALTER command for table type %s. No constraints allowed.", 
										 (RelationIsExternal(rel) ? "external" : "foreign")),
								 errOmitLocation(true)));
					break;
					
				default:
					/* ALTER type supported */
					break;
			}
		}
	}

	/*
	 * Check the ALTER command type is supported for this object
	 */
	/*
	 * Check whether the relation or its sub partition table is parquet table, if yes,
	 * check whether the command is supported
	 */
	List *children = find_all_inheritors(RelationGetRelid(rel));
	ListCell *lc;
	foreach(lc, children)
	{
		Oid relid = lfirst_oid(lc);
		Relation crel;
		if (relid == RelationGetRelid(rel))
			crel = rel;
		else
			crel = heap_open(relid, AccessShareLock);

		if (RelationIsParquet(crel)) {
			ListCell *lcmd;

			foreach(lcmd, stmt->cmds)
			{
				AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lcmd);

				switch (cmd->subtype) {
				/* FOREIGN and EXTERNAL tables doesn't support the following AT */
				case AT_AddColumn:
				case AT_AddColumnRecurse:
				case AT_ColumnDefault:
				case AT_DropNotNull:
				case AT_SetNotNull:
				case AT_SetStorage:
				case AT_DropColumn:
				case AT_DropColumnRecurse:
				case AT_AddIndex:
				case AT_ReAddIndex:
				case AT_AlterColumnType:
				case AT_ClusterOn:
				case AT_DropCluster:
				case AT_DropOids:
				case AT_EnableTrig:
				case AT_DisableTrig:
				case AT_EnableTrigAll:
				case AT_DisableTrigAll:
				case AT_EnableTrigUser:
				case AT_DisableTrigUser:
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
									errmsg("Unsupported ALTER command for parquet table \"%s\"",
									NameStr(crel->rd_rel->relname)),
									errOmitLocation(true)));
					break;
				default:
					/* ALTER type supported */
					break;
				}
			}
		}

		/*close sub partition table*/
		if (relid != RelationGetRelid(rel))
			heap_close(crel, AccessShareLock);
	}
	list_free(children);
}

/*
 * Copy the AlteredTableInfo to a serializeable AlterRewriteTableInfo, to dispatch
 * ATRewriteTable work to segments.
 */
static AlterRewriteTableInfo *
prepareAlteredTableInfo(AlteredTableInfo *tab)
{
	AlterRewriteTableInfo  *ar_tab = makeNode(AlterRewriteTableInfo);
	ListCell			   *l;

	ar_tab->relid = tab->relid;
	ar_tab->relkind = tab->relkind;
	ar_tab->oldDesc = tab->oldDesc;
	ar_tab->new_notnull = tab->new_notnull;
	ar_tab->new_dropoids = tab->new_dropoids;
	ar_tab->newTableSpace = tab->newTableSpace;
	ar_tab->exchange_relid = tab->exchange_relid;

	/* Copy constraints */
	foreach (l, tab->constraints)
	{
		NewConstraint	   *con = lfirst(l);
		AlterRewriteNewConstraint	   *ar_con =
					makeNode(AlterRewriteNewConstraint);

		ar_con->name = pstrdup(con->name);
		ar_con->contype = con->contype;
		ar_con->refrelid = con->refrelid;
		ar_con->qual = copyObject(con->qual);

		ar_tab->constraints = lappend(ar_tab->constraints, ar_con);
	}

	/* Copy newvals */
	foreach (l, tab->newvals)
	{
		NewColumnValue	   *ex = lfirst(l);
		AlterRewriteNewColumnValue *ar_ex =
					makeNode(AlterRewriteNewColumnValue);

		ar_ex->attnum = ex->attnum;
		ar_ex->expr = copyObject(ex->expr);

		ar_tab->newvals = lappend(ar_tab->newvals, ar_ex);
	}

	return ar_tab;
}

/*
 * Copy back the AlteredTableInfo from a serializeable AlterRewriteTableInfo.
 */
static AlteredTableInfo *
rebuildAlteredTableInfo(AlterRewriteTableInfo *ar_tab)
{
	AlteredTableInfo	   *tab =
			(AlteredTableInfo *) palloc0(sizeof(AlteredTableInfo));
	ListCell			   *l;

	tab->relid = ar_tab->relid;
	tab->relkind = ar_tab->relkind;
	tab->oldDesc = ar_tab->oldDesc;
	tab->new_notnull = ar_tab->new_notnull;
	tab->new_dropoids = ar_tab->new_dropoids;
	tab->newTableSpace = ar_tab->newTableSpace;
	tab->exchange_relid = ar_tab->exchange_relid;
	tab->scantable_splits = ar_tab->scantable_splits;
	tab->ao_segnos = ar_tab->ao_segnos;

	/* Copy constraints */
	foreach (l, ar_tab->constraints)
	{
		AlterRewriteNewConstraint	   *ar_con = lfirst(l);
		NewConstraint	   *con =
				(NewConstraint *) palloc0(sizeof(NewConstraint));

		con->name = pstrdup(ar_con->name);
		con->contype = ar_con->contype;
		con->refrelid = ar_con->refrelid;
		con->qual = copyObject(ar_con->qual);

		tab->constraints = lappend(tab->constraints, con);
	}

	/* Copy newvals */
	foreach (l, ar_tab->newvals)
	{
		AlterRewriteNewColumnValue *ar_ex = lfirst(l);
		NewColumnValue	   *ex =
				(NewColumnValue *) palloc0(sizeof(NewColumnValue));

		ex->attnum = ar_ex->attnum;
		ex->expr = copyObject(ar_ex->expr);

		tab->newvals = lappend(tab->newvals, ex);
	}

	return tab;
}

/*
 * AlterTable
 *		Execute ALTER TABLE, which can be a list of subcommands
 *
 * ALTER TABLE is performed in three phases:
 *		1. Examine subcommands and perform pre-transformation checking.
 *		2. Update system catalogs.
 *		3. Scan table(s) to check new constraints, and optionally recopy
 *		   the data into new table(s).
 * Phase 3 is not performed unless one or more of the subcommands requires
 * it.	The intention of this design is to allow multiple independent
 * updates of the table schema to be performed with only one pass over the
 * data.
 *
 * ATPrepCmd performs phase 1.	A "work queue" entry is created for
 * each table to be affected (there may be multiple affected tables if the
 * commands traverse a table inheritance hierarchy).  Also we do preliminary
 * validation of the subcommands, including parse transformation of those
 * expressions that need to be evaluated with respect to the old table
 * schema.
 *
 * ATRewriteCatalogs performs phase 2 for each affected table (note that
 * phases 2 and 3 do no explicit recursion, since phase 1 already did it).
 * Certain subcommands need to be performed before others to avoid
 * unnecessary conflicts; for example, DROP COLUMN should come before
 * ADD COLUMN.	Therefore phase 1 divides the subcommands into multiple
 * lists, one for each logical "pass" of phase 2.
 *
 * ATRewriteTables performs phase 3 for those tables that need it.
 *
 * Thanks to the magic of MVCC, an error anywhere along the way rolls back
 * the whole operation; we don't have to do anything special to clean up.
 */
void
AlterTable(Oid relid, AlterTableStmt *stmt)
{
	GpRoleValue oldrole = GP_ROLE_UNDEFINED;

    if(gp_upgrade_mode)
    {
    	oldrole = Gp_role;
    	Gp_role = GP_ROLE_UTILITY;
    }

	if (stmt->relkind == OBJECT_INDEX)
	{
		if (!gp_called_by_pgdump)
			ereport(ERROR,
				(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support alter index statement yet") ));
	}
	else if (stmt->relkind == OBJECT_FOREIGNTABLE)
	{
		if (!gp_called_by_pgdump)
			ereport(ERROR,
				(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support alter foreign table statement yet") ));
	}
	else if (stmt->relkind == OBJECT_EXTTABLE)
	{
		if (!gp_called_by_pgdump)
			ereport(ERROR,
				(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support alter external table statement yet") ));
	}

	/* check if this is an hcatalog table */
#ifdef USE_ASSERT_CHECKING
	Oid oidRel = 
#endif //USE_ASSERT_CHECKING
	RangeVarGetRelid(stmt->relation, false /*failOk*/, false /*allowHcatalog*/);
	Assert(OidIsValid(oidRel));
	Assert(OidIsValid(relid));

	Relation rel;

	/* Caller is required to provide an adequate lock. */
	rel = relation_open(relid, NoLock);
	int			expected_refcnt;

	ATVerifyObject(stmt, rel);
	
	/*
	 * Disallow ALTER TABLE when the current backend has any open reference
	 * to it besides the one we just got (such as an open cursor or active
	 * plan); our AccessExclusiveLock doesn't protect us against stomping on
	 * our own foot, only other people's feet!
	 *
	 * Note: the only case known to cause serious trouble is ALTER COLUMN TYPE,
	 * and some changes are obviously pretty benign, so this could possibly
	 * be relaxed to only error out for certain types of alterations.  But
	 * the use-case for allowing any of these things is not obvious, so we
	 * won't work hard at it for now.
	 */
	expected_refcnt = rel->rd_isnailed ? 2 : 1;
	if (rel->rd_refcnt != expected_refcnt)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("relation \"%s\" is being used by active queries in this session",
						RelationGetRelationName(rel))));

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		LockRelationOid(RelationRelationId, RowExclusiveLock);
		LockRelationOid(TypeRelationId, RowExclusiveLock);
		LockRelationOid(DependRelationId, RowExclusiveLock);
	}

	ATController(rel,
				 stmt->cmds,
				 interpretInhOption(stmt->relation->inhOpt),
				 &stmt->oidInfoCount,
				 &stmt->oidInfo,
				 &stmt->oidmap);

    if(gp_upgrade_mode)
    	Gp_role = oldrole;
}

/*
 * AlterTableInternal
 *
 * ALTER TABLE with target specified by OID
 *
 * We do not reject if the relation is already open, because it's quite likely
 * that one or more layers of caller have it open.  That means it is unsafe to
 * use this entry point for alterations that could break existing query plans.
 * On the assumption it's not used for such, we don't have to reject pending
 * AFTER triggers, either.
 *
 * It is also unsafe to use this function for any Alter Table subcommand that
 * requires rewriting the table or creating toast tables, because that requires
 * creating relfilenodes outside of a context that understands dispatch.
 * Commands that rewrite the table include: adding or altering columns, changing
 * the tablespace, etc.
 */
void
AlterTableInternal(Oid relid, List *cmds, bool recurse)
{
	ATController(relation_open(relid, AccessExclusiveLock),
				 cmds,
				 recurse,
				 0,
				 NULL,
				 NULL);
}

static void
ATController(Relation rel, List *cmds, bool recurse, 
			 int * oidInfoCount, TableOidInfo ** oidInfo, List **poidmap)
{
	List	   *wqueue = NIL;
	ListCell   *lcmd;
	int			ocount = 0;
	TableOidInfo * oids = NULL;
	bool is_partition = false;
	bool is_data_remote = (RelationIsExternal(rel) || RelationIsForeign(rel));
#ifdef USE_ASSERT_CHECKING
	Oid			relid = RelationGetRelid(rel);
#endif

	/*
	 * In HAWQ, only the master has the catalog information.
	 * So, no need to sync oid to segments.
	 */
	/* cdb_sync_oid_to_segments(); */

	/* Phase 1: preliminary examination of commands, create work queue */
	foreach(lcmd, cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lcmd);

		if (cmd->subtype == AT_PartAddInternal)
			is_partition = true;

		ATPrepCmd(&wqueue, rel, cmd, recurse, false);
	}

	if (is_partition)
		relation_close(rel, AccessExclusiveLock);
	else
		/* Close the relation, but keep lock until commit */
		relation_close(rel, NoLock);

	/* Phase 2: update system catalogs */
	ATRewriteCatalogs(&wqueue);

	if (Gp_role == GP_ROLE_DISPATCH && oidInfoCount != NULL)
	{
		*oidInfoCount = list_length(wqueue);
		*oidInfo = palloc0(sizeof(TableOidInfo)*(*oidInfoCount));
	}

	if (oidInfoCount != NULL)
		ocount = *oidInfoCount;
	if (oidInfo != NULL)
		oids = *oidInfo;

	/*
	 * Build OID map info.
	 *
	 * If we've recursed (i.e., expanded) an ALTER TABLE SET DISTRIBUTED
	 * command from a master table to its children, then we need to extract
	 * the OID map (used to ensure that all segments map old index OIDs to
	 * new index OIDs) for each of the sub commands and put them in the
	 * master command. This is because we only dispatch the command on the
	 * master table to the segments, not each expanded command. We expect
	 * the segments to do the same expansion.
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		List *umap = NIL; /* the full OID map for all relations */
		ListCell *lc;
		AlterTableCmd *mastercmd = NULL; /* the command which was expanded */

		/* 
		 * Iterate over the work queue looking for SET WITH DISTRIBUTED
		 * statements.
		 */
		foreach(lc, wqueue)
		{
			ListCell *lc2;
			AlteredTableInfo *tab = (AlteredTableInfo *) lfirst(lc);

			/* AT_PASS_MISC is used for SET DISTRIBUTED BY */
			List *subcmds = (List *)tab->subcmds[AT_PASS_MISC];

			foreach(lc2, subcmds)
			{
				AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc2);

				if (cmd->subtype == AT_SetDistributedBy)
				{
					if (!mastercmd)
					{
						Assert(tab->relid == relid);
						mastercmd = cmd;
					}

					/*
					 * cmd->def stores the data we're sending to the QEs.
					 * The second element is where we stash QE data to drive
					 * the command. The second element of qe_data is our
					 * OID map. See ATExecSetDistributedBy().
					 *
					 * Yes, this is begging to be improved.
					 */
					List *data = (List *)cmd->def;
					List *qe_data = lsecond(data);

					if (qe_data && list_length(qe_data) >= 2)
					{
						List *oidmap = lsecond(qe_data);

						umap = list_concat(umap, oidmap);
					}
				}
			}
		}

		/* Finally, push it all back into the master command */
		if (mastercmd && umap)
		{
			List *qe_data = lsecond((List *)mastercmd->def);

			if (list_length(qe_data) >= 2)
				lsecond(qe_data) = umap;
			else if (list_length(qe_data) == 1)
			{
				qe_data = lappend(qe_data, umap);

				/* 
				 * We've changed the pointer, save it back to the master
				 * definition.
				 */
				lsecond((List *)mastercmd->def) = qe_data;
			}
		}
	}

	/* 
	 * Phase 3: scan/rewrite tables as needed. If the data is in an external
	 * table, no need to rewrite it or to add toast.
	 */
	if (!is_data_remote)
	{
		ATRewriteTables(&wqueue, ocount, oids, poidmap);

		ATAddToastIfNeeded(&wqueue, ocount, oids, poidmap);		
	}

}

/*
 * This is the execution work of the phase 3 of ALTER TABLE.  The piece of
 * information should be dispatched from master.
 */
void
AlterRewriteTable(AlterRewriteTableInfo *ar_tab)
{
	AlteredTableInfo	   *tab;

	Assert(Gp_role == GP_ROLE_EXECUTE);

	tab = rebuildAlteredTableInfo(ar_tab);
	ATRewriteTable(tab, ar_tab->newheap_oid);
}

/*
 * ATPrepCmd
 *
 * Traffic cop for ALTER TABLE Phase 1 operations, including simple
 * recursion and permission checks.
 *
 * Caller must have acquired AccessExclusiveLock on relation already.
 * This lock should be held until commit.
 */
static void
ATPrepCmd(List **wqueue, Relation rel, AlterTableCmd *cmd,
		  bool recurse, bool recursing)
{
	AlteredTableInfo *tab;
	int			pass = 0;

	/* Find or create work queue entry for this table */
	tab = ATGetQueueEntry(wqueue, rel);

	/*
	 * Copy the original subcommand for each table.  This avoids conflicts
	 * when different child tables need to make different parse
	 * transformations (for example, the same column may have different column
	 * numbers in different children).
	 */
	if (recursing)
		cmd = copyObject(cmd);

	/*
	 * Do permissions checking, recursion to child tables if needed, and any
	 * additional phase-1 processing needed.
	 */
	switch (cmd->subtype)
	{
		case AT_AddColumn:		/* ADD COLUMN */
			ATSimplePermissions(rel, false);
			/*
			 * test that this is allowed for partitioning, but only if we aren't
			 * recursing.
			 */
			ATPartitionCheck(cmd->subtype, rel, false, recursing);
			/* Performs own recursion */
			ATPrepAddColumn(rel, recurse, cmd);
			pass = AT_PASS_ADD_COL;
			break;
		case AT_AddColumnRecurse:		/* ADD COLUMN internal */
			ATSimplePermissions(rel, false);
			/* No need to do ATPartitionCheck */
			/* Performs own recursion */
			ATPrepAddColumn(rel, recurse, cmd);
			pass = AT_PASS_ADD_COL;
			break;
		case AT_ColumnDefault:	/* ALTER COLUMN DEFAULT */
			/*
			 * We allow defaults on views so that INSERT into a view can have
			 * default-ish behavior.  This works because the rewriter
			 * substitutes default values into INSERTs before it expands
			 * rules.
			 */
			ATSimplePermissions(rel, true);
			ATPartitionCheck(cmd->subtype, rel, false, recursing);
			ATSimpleRecursion(wqueue, rel, cmd, recurse);
			/* No command-specific prep needed */
			pass = cmd->def ? AT_PASS_ADD_CONSTR : AT_PASS_DROP;
			break;
		case AT_DropNotNull:	/* ALTER COLUMN DROP NOT NULL */
			ATSimplePermissions(rel, false);
			ATPartitionCheck(cmd->subtype, rel, false, recursing);
			ATSimpleRecursion(wqueue, rel, cmd, recurse);
			/* No command-specific prep needed */
			pass = AT_PASS_DROP;
			break;
		case AT_SetNotNull:		/* ALTER COLUMN SET NOT NULL */
			ATSimplePermissions(rel, false);
			ATSimpleRecursion(wqueue, rel, cmd, recurse);
			if (!cmd->part_expanded)
				ATPartitionCheck(cmd->subtype, rel, false, recursing);
			/* No command-specific prep needed */
			pass = AT_PASS_ADD_CONSTR;
			break;
		case AT_SetStatistics:	/* ALTER COLUMN STATISTICS */
			ATSimpleRecursion(wqueue, rel, cmd, recurse);
			/* Performs own permission checks */
			ATPrepSetStatistics(rel, cmd->name, cmd->def);
			pass = AT_PASS_COL_ATTRS;
			break;
		case AT_SetStorage:		/* ALTER COLUMN STORAGE */
			ATSimplePermissions(rel, false);
			ATSimpleRecursion(wqueue, rel, cmd, recurse);
			/* No command-specific prep needed */
			pass = AT_PASS_COL_ATTRS;
			break;
		case AT_DropColumn:		/* DROP COLUMN */
		case AT_DropColumnRecurse:
			ATSimplePermissions(rel, false);
			ATPartitionCheck(cmd->subtype, rel, false, recursing);
			/* Recursion occurs during execution phase */
			/* No command-specific prep needed except saving recurse flag */
			if (recurse)
				cmd->subtype = AT_DropColumnRecurse;
			pass = AT_PASS_DROP;
			break;
		case AT_AddIndex:		/* ADD INDEX */
			ereport(ERROR,
					(errcode(ERRCODE_CDB_FEATURE_NOT_YET),
					 errmsg("ALTER TABLE ... ADD INDEX is not supported")));
			ATSimplePermissions(rel, false);
			/* Any recursion for partitioning is done in ATExecAddIndex() itself */
			/* However, if the index supports a PK or UNIQUE constraint and the
			 * relation is partitioned, we need to assure the constraint is named
			 * in a reasonable way. */
			{
				ConstrType contype = CONSTR_NULL; /* name picker will reject this */
				IndexStmt *topindexstmt = (IndexStmt*)cmd->def;
				Insist(IsA(topindexstmt, IndexStmt));
				if ( topindexstmt->isconstraint )
				{
					if ( !topindexstmt->altconname )
 					{
 						if ( topindexstmt->primary )
							contype = CONSTR_PRIMARY;
						else if ( topindexstmt->unique )
							contype = CONSTR_UNIQUE;
							 						 						
						/* Pick and install a constraint name. */
						topindexstmt->altconname =
							ChooseConstraintNameForPartitionEarly(rel, 
																  contype,
																  (Node*)topindexstmt->indexParams);
					}

					switch ( rel_part_status(RelationGetRelid(rel)) )
					{
						case PART_STATUS_ROOT:
 							break;
 						case PART_STATUS_INTERIOR:
 						case PART_STATUS_LEAF:
							/* Don't do this check for child parts (= internal requests). */
 							if ( ! topindexstmt->is_part_child )
 							{
 								char *what = "UNIQUE";
 								if ( contype == CONSTR_PRIMARY )
 									what = "PRIMARY KEY";
 								ereport(ERROR,
 										(errcode(ERRCODE_WRONG_OBJECT_TYPE),
 										 errmsg("can't place a %s constraint on just part of "
 												"partitioned table \"%s\"", 
 												what,RelationGetRelationName(rel)),
 										 errhint("Constrain the whole table or create a "
 												 "part-wise UNIQUE index instead.")));
 							}
 							break;
 						default:
 							break;
 					}
				}
			}
			pass = AT_PASS_ADD_INDEX;
			break;
		case AT_AddConstraint:	/* ADD CONSTRAINT */
			ATSimplePermissions(rel, false);
			/* (!recurse &&  !recursing) is supposed to detect the ONLY clause.
			 * We allow operations on the root of a partitioning hierarchy, but
			 * not ONLY the root.
			 */
			ATPartitionCheck(cmd->subtype, rel, (!recurse && !recursing), recursing);
			/*
			 * Currently we recurse only for CHECK constraints, never for
			 * foreign-key constraints.  UNIQUE/PKEY constraints won't be seen
			 * here.
			 */
			/* Any recursion for partitioning/inheritance is done in ATExecAddConstraint() itself */
			if (recurse)
				cmd->subtype = AT_AddConstraintRecurse;
			pass = AT_PASS_ADD_CONSTR;
			break;
		case AT_AddConstraintRecurse: /* ADD check CONSTRAINT internal */
			/* Parent/Base CHECK constraints apply to child/part tables here.
			 * No need for ATPartitionCheck
			 */
			ATSimplePermissions(rel, false);
			pass = AT_PASS_ADD_CONSTR;
			break;
		case AT_DropConstraint:	/* DROP CONSTRAINT */
			ATSimplePermissions(rel, false);
			/* (!recurse &&  !recursing) is supposed to detect the ONLY clause.
			 * We allow operations on the root of a partitioning hierarchy, but
			 * not ONLY the root.
			 */
			ATPartitionCheck(cmd->subtype, rel, (!recurse && !recursing), recursing);
			/* Performs own recursion */
			ATPrepDropConstraint(wqueue, rel, recurse, cmd);
			pass = AT_PASS_DROP;
			break;
		case AT_DropConstraintQuietly:	/* DROP CONSTRAINT for child */
			ATSimplePermissions(rel, false);
			ATSimpleRecursion(wqueue, rel, cmd, recurse);
			/* No command-specific prep needed */
			pass = AT_PASS_DROP;
			break;
		case AT_AlterColumnType:		/* ALTER COLUMN TYPE */
			ATSimplePermissions(rel, false);
			ATPartitionCheck(cmd->subtype, rel, false, recursing);
			/* Performs own recursion */
			ATPrepAlterColumnType(wqueue, tab, rel, recurse, recursing, cmd);
			pass = AT_PASS_ALTER_TYPE;
			break;
		case AT_ChangeOwner:	/* ALTER OWNER */
			{
				bool do_recurse = false;

				if (Gp_role == GP_ROLE_DISPATCH)
				{
					ATPartitionCheck(cmd->subtype, rel, false, recursing);
					if (rel_is_partitioned(RelationGetRelid(rel)))
					{
						do_recurse = true;
						/* tell QE to recurse */
						cmd->def = (Node *)makeInteger(1);
					}
				}
				else if (Gp_role == GP_ROLE_EXECUTE)
				{
					if (cmd->def && intVal(cmd->def))
						do_recurse = true;
				}

				if (do_recurse)
					ATSimpleRecursion(wqueue, rel, cmd, recurse);

				pass = AT_PASS_MISC;
			}
			break;
		case AT_ClusterOn:		/* CLUSTER ON */
			ereport(ERROR,
					(errcode(ERRCODE_CDB_FEATURE_NOT_YET),
					 errmsg("ALTER TABLE ... CLUSTER is not supported")));
		case AT_DropCluster:	/* SET WITHOUT CLUSTER */
			ereport(ERROR,
					(errcode(ERRCODE_CDB_FEATURE_NOT_YET),
					 errmsg("ALTER TABLE ... SET WITHOUT CLUSTER is not supported")));
			ATSimplePermissions(rel, false);
			/* These commands never recurse */
			/* No command-specific prep needed */
			pass = AT_PASS_MISC;
			break;
		case AT_DropOids:		/* SET WITHOUT OIDS */
			ATSimplePermissions(rel, false);
			ATPartitionCheck(cmd->subtype, rel, false, recursing);
			/* Performs own recursion */
			if (rel->rd_rel->relhasoids)
			{
				AlterTableCmd *dropCmd = makeNode(AlterTableCmd);

				dropCmd->subtype = AT_DropColumn;
				dropCmd->name = pstrdup("oid");
				dropCmd->behavior = cmd->behavior;
				ATPrepCmd(wqueue, rel, dropCmd, recurse, false);
			}
			pass = AT_PASS_DROP;
			break;
		case AT_SetTableSpace:	/* SET TABLESPACE */
			ereport(ERROR,
					(errcode(ERRCODE_CDB_FEATURE_NOT_YET),
					 errmsg("ALTER TABLE ... SET TABLESPACE is not supported")));
			ATSimplePermissionsRelationOrIndex(rel);
			/* This command never recurses, but the offered relation may be partitioned, 
			 * in which case, we need to act as if the command specified the top-level
			 * list of parts.
			 */
			if (Gp_role == GP_ROLE_DISPATCH && rel_is_partitioned(RelationGetRelid(rel)) )
			{
				PartitionNode *pnode = NULL;
				List *all_oids = NIL;
				
				pnode = get_parts(RelationGetRelid(rel), 
								  0, /**/
								  InvalidOid, /**/
								  false, /**/
								  CurrentMemoryContext,
								  true /*includesubparts*/);
				
				all_oids = lcons_oid(RelationGetRelid(rel), all_partition_relids(pnode));
				
				Assert( cmd->partoids == NIL );
				cmd->partoids = all_oids;
				
				ATPartsPrepSetTableSpace(wqueue, rel, cmd, all_oids);
			}
			else if (Gp_role == GP_ROLE_EXECUTE && cmd->partoids)
			{
				ATPartsPrepSetTableSpace(wqueue, rel, cmd, cmd->partoids);
			}
			else if (Gp_role == GP_ROLE_UTILITY)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
 						 errmsg("SET TABLESPACE is not supported in utility mode")));
			}
			else
				ATPrepSetTableSpace(tab, rel, cmd->name);
			pass = AT_PASS_MISC;	/* doesn't actually matter */
			break;
		case AT_SetRelOptions:	/* SET (...) */
		case AT_ResetRelOptions:		/* RESET (...) */
			ATSimplePermissionsRelationOrIndex(rel);
			/* This command never recurses */
			/* No command-specific prep needed */
			pass = AT_PASS_MISC;
			break;
		case AT_SetDistributedBy:	/* SET DISTRIBUTED BY */
			if ( !recursing ) /* MPP-5772, MPP-5784 */
			{
				Oid relid = RelationGetRelid(rel);
				PartStatus ps = rel_part_status(relid);

				if ( recurse ) /* Normal ALTER TABLE */
				{
					switch (ps)
					{
						case PART_STATUS_NONE:
						case PART_STATUS_ROOT:
						case PART_STATUS_LEAF:
							break;

						case PART_STATUS_INTERIOR:
							/*Reject interior branches of partitioned tables.*/
							ereport(ERROR,
									(errcode(ERRCODE_WRONG_OBJECT_TYPE),
									 errmsg("can't set the distribution"
											" policy of \"%s\"",
											RelationGetRelationName(rel)),
									 errhint("Distribution policy may be set"
											 " for an entire partitioned table"
											 " or one of its leaf parts;"
											 " not for an interior branch."
											 ),
									 errOmitLocation(true)));
							break; /* tidy */
					}
				}
				else /* ALTER TABLE ONLY */
				{
					switch (ps)
					{
						case PART_STATUS_NONE:
						case PART_STATUS_LEAF:
							break;

						case PART_STATUS_ROOT:
						case PART_STATUS_INTERIOR:
							ereport(ERROR,
									(errcode(ERRCODE_WRONG_OBJECT_TYPE),
									 errmsg("can't set the distribution policy "
											"of ONLY \"%s\"",
											RelationGetRelationName(rel)),
									 errhint("Distribution policy may be set "
											 "for an entire partitioned table"
											 " or one of its leaf parts."
											 ),
									 errOmitLocation(true)));
							break; /* tidy */
					}
				}

				if ( ps == PART_STATUS_LEAF )
				{
					Oid ptrelid = rel_partition_get_master(relid);
					List *dist_cnames = lsecond((List*)cmd->def);

					/*	might be null if no policy set, e.g. just
					 *  a change of storage options...
					 */
					if (dist_cnames)
					{
						Relation ptrel = heap_open(ptrelid,
												   AccessShareLock);
						Assert(IsA(dist_cnames, List));

						if (! can_implement_dist_on_part(ptrel,
														 dist_cnames) )
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("cannot SET DISTRIBUTED BY for %s",
											RelationGetRelationName(rel)),
									 errhint("Leaf distribution policy must be"
											 " random or match that of the"
											 " entire partitioned table."),
									 errOmitLocation(true)
									 ));
						heap_close(ptrel, AccessShareLock);
					}
				}
			}
			ATSimplePermissions(rel, false);
			ATSimpleRecursion(wqueue, rel, cmd, recurse);
			pass = AT_PASS_MISC;
			break;
		case AT_EnableTrig:		/* ENABLE TRIGGER variants */
		case AT_EnableTrigAll:
		case AT_EnableTrigUser:
			ereport(ERROR,
					(errcode(ERRCODE_CDB_FEATURE_NOT_YET),
					 errmsg("ALTER TABLE ... ENABLE TRIGGER is not supported")));
		case AT_DisableTrig:	/* DISABLE TRIGGER variants */
		case AT_DisableTrigAll:
		case AT_DisableTrigUser:
			ereport(ERROR,
					(errcode(ERRCODE_CDB_FEATURE_NOT_YET),
					 errmsg("ALTER TABLE ... DISABLE TRIGGER is not supported")));
			ATSimplePermissions(rel, false);
			ATPartitionCheck(cmd->subtype, rel, false, recursing);
			/* These commands never recurse */
			/* No command-specific prep needed */
			pass = AT_PASS_MISC;
			break;
		case AT_AddInherit:		/* INHERIT / NO INHERIT */
		case AT_DropInherit:
			ATSimplePermissions(rel, false);
			ATPartitionCheck(cmd->subtype, rel, true, recursing);
			/* These commands never recurse */
			/* No command-specific prep needed */
			pass = AT_PASS_MISC;
			break;
			/* CDB: Partitioned Table commands */
		case AT_PartExchange:			/* Exchange */
			ATPartitionCheck(cmd->subtype, rel, false, recursing);
			
			if (Gp_role == GP_ROLE_UTILITY)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
 						 errmsg("EXCHANGE is not supported in utility mode")));
			
			pass = AT_PASS_MISC;
			
			ATPrepExchange(rel, (AlterPartitionCmd *)cmd->def);
			
			break;
		case AT_PartMerge:
			ereport(ERROR,
					(errcode(ERRCODE_CDB_FEATURE_NOT_YET),
					 errmsg("ALTER TABLE ... MERGE PARTITION is not supported")));
		{
			AlterPartitionCmd  *pc    	= (AlterPartitionCmd *) cmd->def;
			PgPartRule   	   *prule1	= NULL;
			PgPartRule		   *prule2	= NULL;
			PgPartRule		   *prule3	= NULL;
			ListCell		   *lc;
			List			   *source_rels = NIL;
			Relation			target = NULL;

			ATPartitionCheck(cmd->subtype, rel, false, recursing);

			if (Gp_role == GP_ROLE_UTILITY)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
 						 errmsg("MERGE is not supported in utility mode")));
			else if (Gp_role == GP_ROLE_DISPATCH)
			{
				Relation srel;
				AlterPartitionId *pid = (AlterPartitionId *) pc->partid;

				prule1 = get_part_rule(rel, pid, true, true,
									   CurrentMemoryContext, NULL, false);

				prule2 = get_part_rule(rel, (AlterPartitionId *)pc->arg1,
									   true, true,
									   CurrentMemoryContext, NULL, false);

				if (pc->arg2)
				{
					AlterPartitionCmd *pc2 = (AlterPartitionCmd *)pc->arg2;

					prule3 = get_part_rule(rel,
										   (AlterPartitionId *)pc2->partid,
										   true, true,
										   CurrentMemoryContext, NULL, false);
				}

				if (prule1)
				{
					/* cannot exchange a hash partition */
					if ('h' == prule1->pNode->part->parkind)
						ereport(ERROR,
							(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
								 errmsg("cannot merge HASH partition%s "
									    "of relation \"%s\"",
										prule1->partIdStr,
										RelationGetRelationName(rel))));
				}

				srel = heap_open(prule1->topRule->parchildrelid,
								 AccessExclusiveLock);
				source_rels = lappend(source_rels, srel);

				srel = heap_open(prule2->topRule->parchildrelid,
								 AccessExclusiveLock);
				if (prule3)
				{
					source_rels = lappend(source_rels, srel);
					target = heap_open(prule3->topRule->parchildrelid,
									   AccessExclusiveLock);
				}
				else
					target = srel;
			}
			else
			{
				Relation srel;
				srel = heap_openrv((RangeVar *)pc->partid,
								   AccessExclusiveLock);
				source_rels = lappend(source_rels, srel);

				srel = heap_openrv((RangeVar *)pc->arg1, AccessExclusiveLock);

				if (pc->arg2)
				{
					source_rels = lappend(source_rels, srel);
					target = heap_openrv((RangeVar *)pc->arg2,
										 AccessExclusiveLock);
				}
				else
					target = heap_openrv((RangeVar *)pc->arg1,
										 AccessExclusiveLock);
			}

			ATSimplePermissions(rel, false);

			foreach(lc, source_rels)
				ATSimplePermissions(lfirst(lc), false);

			ATSimplePermissions(target, false);

			foreach(lc, source_rels)
				heap_close(lfirst(lc), NoLock);

			heap_close(target, NoLock);

			pass = AT_PASS_MISC;
			break;
		}

		case AT_PartSplit:				/* Split */
		{
			AlterPartitionCmd	*pc    	= (AlterPartitionCmd *) cmd->def;

			ATPartitionCheck(cmd->subtype, rel, false, recursing);

			if (Gp_role == GP_ROLE_UTILITY)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
 						 errmsg("SPLIT is not supported in utility mode")));
			else if (Gp_role == GP_ROLE_DISPATCH)
			{
				PgPartRule			*prule1	= NULL;
				PgPartRule			*prule2	= NULL;
				PgPartRule			*prule3	= NULL;
				PgPartRule			*prule_specified	= NULL;
				Relation			 target = NULL;
				
				AlterPartitionId *pid = (AlterPartitionId *) pc->partid;
				AlterPartitionCmd *cmd2;
				Node *at;
				bool is_at = true;
				List *todo;
				ListCell *l;
				bool rollup_vals = false;

				/*
				 * Need to do a bunch of validation:
				 * 0) Target exists
				 * 0.5) Shouldn't have children
				 * 1) The usual permissions checks
				 * 2) Not called on HASH
				 * 3) AT () parameter falls into constraint specified
				 * 4) INTO partitions don't exist except for the one being split
				 */

				/* We'll error out if it doesn't exist */
				prule1 = get_part_rule(rel, pid, true, true,
									   CurrentMemoryContext, NULL, false);

				if (prule1->topRule->children)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot split partition with child "
									"partitions"),
							 errhint("Try splitting the child partitions."),
							 errOmitLocation(true)));

				target = heap_open(prule1->topRule->parchildrelid,
								   AccessExclusiveLock);

				if (linitial((List *)pc->arg1))
					is_at = false;

				if (prule1->topRule->parisdefault &&
					prule1->pNode->part->parkind == 'r' &&
					is_at)
				{
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("AT clause cannot be used when splitting "
									"a default RANGE partition"),
							 errOmitLocation(true)));
				}
				else if (prule1->pNode->part->parkind == 'l' && !is_at)
				{
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("cannot SPLIT DEFAULT PARTITION "
									"with LIST"),
							errhint("Use SPLIT with the AT clause instead."),
							errOmitLocation(true)));

				}

				if (prule1->pNode->part->parkind == 'h')
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("SPLIT is not supported for "
									"HASH partitions"),
							 errOmitLocation(true)));

				if (linitial((List *)pc->arg1))
					is_at = false;

				todo = (List *)pc->arg1;

				pc->arg1 = (Node *)NIL;
				foreach(l, todo)
				{
					List *l1;
					ListCell *lc;
					Node *t = lfirst(l);

					if (!t)
					{
						pc->arg1 = (Node *)lappend((List *)pc->arg1, NULL);
						continue;
					}
					else if (is_at)
						l1 = (List *)t;
					else if (!is_at)
					{
						Node *n = t;
						PartitionRangeItem *ri = (PartitionRangeItem *)n;

						Assert(IsA(n, PartitionRangeItem));

						l1 = ri->partRangeVal;
					}

					if (IsA(linitial(l1), A_Const) ||
						IsA(linitial(l1), TypeCast) ||
						IsA(linitial(l1), FuncExpr) ||
						IsA(linitial(l1), OpExpr))
					{
						List *new = NIL;
						ListCell *lc;

						foreach(lc, l1)
							new = lappend(new, list_make1(lfirst(lc)));

						l1 = new;
						rollup_vals = true;
					}
					else
						Insist(IsA(linitial(l1), List));

					foreach(lc, l1)
					{
						List *vals = lfirst(lc);
						ListCell *lc2;
						int i = 0;
						ParseState *pstate = make_parsestate(NULL);

						if (prule1->pNode->part->parnatts != list_length(vals))
						{
							AttrNumber parnatts = prule1->pNode->part->parnatts;
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("partition being split has "
											"%i column%s but parameter "
											"has %i column%s",
											parnatts,
											parnatts > 1 ? "s" : "",
											list_length(vals),
										   list_length(vals) > 1 ? "s" : ""),
									  errOmitLocation(true)));
						}

						vals = (List *)transformExpressionList(pstate, vals);

						free_parsestate(&pstate);

						foreach(lc2, vals)
						{
							Node *n = lfirst(lc2);
							AttrNumber attnum =
									prule1->pNode->part->paratts[i];
							Oid typid =
									rel->rd_att->attrs[attnum - 1]->atttypid;
							int32 typmod =
									rel->rd_att->attrs[attnum - 1]->atttypmod;
							char parkind = prule1->pNode->part->parkind;
							PartitionByType t = (parkind == 'r') ?
												 PARTTYP_RANGE : PARTTYP_LIST;

							n = coerce_partition_value(n, typid, typmod, t);

							lfirst(lc2) = n;

							i++;
						}
						lfirst(lc) = vals;
					}

					if (rollup_vals)
					{
						/* now, unroll */
						if (prule1->pNode->part->parkind == 'r')
						{
							List *new = NIL;
							ListCell *lc;
							Node *n;

							foreach(lc, l1)
								new = lappend(new, linitial(lfirst(lc)));

							if (is_at)
								n = (Node *)new;
							else
							{
								PartitionRangeItem *ri = lfirst(l);

								ri->partRangeVal = new;
								n = (Node *)ri;
							}
							pc->arg1 = (Node *)lappend((List *)pc->arg1, n);
						}
						else
						{
							/* for LIST, leave it as is */
							pc->arg1 = (Node *)lappend((List *)pc->arg1,
														l1);
						}
					}
					else
						pc->arg1 = (Node *)lappend((List *)pc->arg1, l1);
				}

				at = lsecond((List *)pc->arg1);
				if (prule1->pNode->part->parkind == 'l')
				{

					if (prule1->topRule->parisdefault)
					{
						/*
						 * Check that AT() value doesn't exist in any existing
						 * definition.
						 */

					}
					else
					{
						/* all elements in AT() must match */
						ListCell *lcat;
						List *lv = (List *)prule1->topRule->parlistvalues;
						int16 nkeys = prule1->pNode->part->parnatts;
						List *atlist = (List *)at;
						int nmatches = 0;

						foreach(lcat, atlist)
						{
							List *cols = lfirst(lcat);
							ListCell *parvals = list_head(lv);
							bool found = false;

							Assert(list_length(cols) == nkeys);

							while (parvals)
							{
								List *vals = lfirst(parvals);
								ListCell *lcv = list_head(vals);
								ListCell *lcc = list_head(cols);
								int parcol;
								bool matched = true;

								for (parcol = 0; parcol < nkeys; parcol++)
								{
									Oid opclass =
										prule1->pNode->part->parclass[parcol];
									Oid funcid = get_opclass_proc(opclass, 0,
																 BTORDER_PROC);
									Const *v = lfirst(lcv);
									Const *c = lfirst(lcc);
									Datum d;

									if (v->constisnull && c->constisnull)
										continue;
									else if (v->constisnull || c->constisnull)
									{
										matched = false;
										break;
									}

									d = OidFunctionCall2(funcid, c->constvalue,
														 v->constvalue);

									if (DatumGetInt32(d) != 0)
									{
										matched = false;
										break;
									}

									lcv = lnext(lcv);
									lcc = lnext(lcc);
								}

								if (!matched)
								{
									parvals = lnext(parvals);
									continue;
								}
								else
								{
									found = true;
									nmatches++;
									break;
								}
							}
							if (!found)
								ereport(ERROR,
										(errcode(ERRCODE_WRONG_OBJECT_TYPE),
										errmsg("AT clause parameter is not "
											   "a member of the target "
											   "partition specification"),
										errOmitLocation(true)));


						}

						if (nmatches >= list_length(lv))
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("AT clause cannot contain all "
											"values in partition%s",
											prule1->partIdStr),
									 errOmitLocation(true)));

					}
				}
				else
				{
					/* XXX: handle split of default */
					if (prule1->topRule->parisdefault)
					{
					}
					else
					{
						/* at must be in range */
						Const	*start	  = NULL;
						Const	*end	  = NULL;
						Const	*atval	  = NULL;
						Oid		 opclass  = prule1->pNode->part->parclass[0];
						Oid		 funcid	  = get_opclass_proc(opclass, 0,
															 BTORDER_PROC);
						Node	*n;
						Datum	 res;
						int32	 ret;
						bool	 in_range = true;

						n = (Node *)linitial((List *)at);

						Assert(IsA(n, Const));

						atval = (Const *)n;

						/* XXX: ignore composite key range for now */
						if (prule1->topRule->parrangestart)
							start = linitial((List *)prule1->topRule->parrangestart);

						/* MPP-6589: allow "open" start or end */
						if (start)
						{
							Insist(exprType((Node *)n) == exprType((Node *)start));
							res = OidFunctionCall2(funcid, start->constvalue,
												   atval->constvalue);

							ret = DatumGetInt32(res);
						}
						else
						{
							/* MPP-6589: no start - check end */
							Assert(prule1->topRule->parrangeend);
							ret = -1;
						}

						if (ret > 0)
							in_range = false;
						else if (ret == 0)
						{
							if (!prule1->topRule->parrangestartincl)
								in_range = false;
						}
						else
						{
							/* in the range, test end */
							if (prule1->topRule->parrangeend)
							{
								end = linitial((List *)prule1->topRule->parrangeend);
								res = OidFunctionCall2(funcid,
													   atval->constvalue,
											   		   end->constvalue);

								ret = DatumGetInt32(res);

								if (ret > 0)
									in_range = false;
								else if (ret == 0)
									if (!prule1->topRule->parrangeendincl)
										in_range = false;
							}
							/* if no end, remains "in range" */
						}

						if (!in_range)
						{
							ereport(ERROR,
									(errcode(ERRCODE_WRONG_OBJECT_TYPE),
									errmsg("AT clause parameter is not "
										   "a member of the target "
										   "partition specification"),
									errOmitLocation(true)));
						}
					}
				}

				/*
				 * pc->partid == target
				 * pc->arg1 == AT
				 * pc->arg2 == AlterPartitionCmd (partid == 1, arg1 = 2)
				 */
				if (PointerIsValid(pc->arg2))
				{
					cmd2 = (AlterPartitionCmd *)pc->arg2;

					prule2 = get_part_rule(rel,
										   (AlterPartitionId *)cmd2->partid,
										   false, false, CurrentMemoryContext,
										   NULL, false);

					prule3 = get_part_rule(rel, (AlterPartitionId *)cmd2->arg1,
										   false, false, CurrentMemoryContext,
										   NULL, false);
					/* both can't exist */
					if (prule2 && prule3)
					{
						ereport(ERROR,
								(errcode(ERRCODE_DUPLICATE_OBJECT),
								 errmsg("both INTO partitions "
										"already exist"),
								 errOmitLocation(true)));
					}
					else if (prule1->topRule->parisdefault &&
							 !prule2 && !prule3)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("default partition name missing "
										"from INTO clause"),
								 errOmitLocation(true)));

					}
				}
				
				/* MPP-18395 begs for a last minute check that we aren't about to
				 * split into an existing partition.  The case that prompts the check
				 * has to do with specifying an existing partition by name, but it
				 * may (someday) be possible to specify one by rank or constraint,
				 * so lets just refuse to use an existing partition that isn't the
				 * one we're splitting.  (We're going to drop this one, so it's okay
				 * recycle it.)
				 *
				 * Examples: 
				 *    t is target part to split, 
				 *    n, m are new parts,
				 *    x, y are existing parts != t
				 *
				 * t -> t, n -- ok
				 * t -> n, t -- ok
				 * t -> n, m -- ok
				 * t -> n, x -- no
				 * t -> x, n -- no
				 * t -> x, y -- no (unexpected)
				 *
				 * By the way, at this point
				 *   prule1 refers to the part to split
				 *   prule2 refers to the first INTO part
				 *   prule3 refers to the seconde INTO part
				 */
				Insist( prule2 == NULL || prule3 == NULL );
				
				if ( prule2 != NULL )
				    prule_specified = prule2;
				else if ( prule3 != NULL )
					prule_specified = prule3;
				else
					prule_specified = NULL;
					
				if ( prule_specified &&
					 prule_specified->topRule->parruleid != prule1->topRule->parruleid 
					 )
				{
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_OBJECT),
							 errmsg("cannot split into an existing partition"),
							 errOmitLocation(true)));
				}
				
				ATSimplePermissions(target, false);
				heap_close(target, NoLock);
			}
			else
			{
				Insist(IsA(pc->partid, Integer));
				Insist(IsA(pc->arg1, RangeVar));
				Insist(IsA(pc->arg2, RangeVar));
			}
			pass = AT_PASS_MISC;
			break;
		}

		case AT_PartAlter:				/* Alter */
			if ( Gp_role == GP_ROLE_DISPATCH && recurse && ! recursing )
			{
				AlterTableCmd *basecmd = basic_AT_cmd(cmd);
				
				switch ( basecmd->subtype )
				{
					case AT_SetTableSpace:
						cmd->partoids = basic_AT_oids(rel,cmd);
						ATPartsPrepSetTableSpace(wqueue, rel, basecmd, cmd->partoids);
						break;
					
					default:
						/* Not a for-each-subsumed-part-type command. */
						break;
				}
			}
			else if (Gp_role == GP_ROLE_EXECUTE && cmd->partoids)
			{
				AlterTableCmd *basecmd = basic_AT_cmd(cmd);

				switch ( basecmd->subtype )
				{
					case AT_SetTableSpace:
						ATPartsPrepSetTableSpace(wqueue, rel, basecmd, cmd->partoids);
						break;
						
					default:
						/* Not a for-each-subsumed-part-type command. */
						break;
				}
			}
			else if (Gp_role == GP_ROLE_UTILITY)
			{
				if (basic_AT_cmd(cmd)->subtype == AT_SetTableSpace)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("SET TABLESPACE is not supported in utility mode")));
			}
			pass = AT_PASS_MISC;
			break;				

		case AT_PartSetTemplate:		/* Set Subpartition Template */
			if (!gp_allow_non_uniform_partitioning_ddl)
			{
				ereport(ERROR,
					   (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Cannot modify subpartition template for partitioned table")));
			}
		case AT_PartAdd:				/* Add */
		case AT_PartCoalesce:			/* Coalesce */
		case AT_PartDrop:				/* Drop */
		case AT_PartRename:				/* Rename */

		case AT_PartTruncate:			/* Truncate */
		case AT_PartAddInternal:		/* internal partition creation */
			ATSimplePermissions(rel, false);
			ATPartitionCheck(cmd->subtype, rel, false, recursing);
				/* XXX XXX XXX */
			/* This command never recurses */
			/* No command-specific prep needed */
			/* XXX: check permissions */
			pass = AT_PASS_MISC;
			break;

		case AT_PartModify:             /* Modify */
			ereport(ERROR,
					(errcode(ERRCODE_CDB_FEATURE_NOT_YET),
					 errmsg("ALTER TABLE ... MODIFY PARTITION is not supported")));
			break;

		default:				/* oops */
			elog(ERROR, "unrecognized alter table type: %d",
				 (int) cmd->subtype);
			pass = 0;			/* keep compiler quiet */
			break;
	}

	/* Add the subcommand to the appropriate list for phase 2 */
	tab->subcmds[pass] = lappend(tab->subcmds[pass], cmd);
}

/*
 * ATRewriteCatalogs
 *
 * Traffic cop for ALTER TABLE Phase 2 operations.	Subcommands are
 * dispatched in a "safe" execution order (designed to avoid unnecessary
 * conflicts).
 */
static void
ATRewriteCatalogs(List **wqueue)
{
	int			pass;
	ListCell   *ltab;

	/*
	 * We process all the tables "in parallel", one pass at a time.  This is
	 * needed because we may have to propagate work from one table to another
	 * (specifically, ALTER TYPE on a foreign key's PK has to dispatch the
	 * re-adding of the foreign key constraint to the other table).  Work can
	 * only be propagated into later passes, however.
	 */
	for (pass = 0; pass < AT_NUM_PASSES; pass++)
	{
		/* Go through each table that needs to be processed */
		foreach(ltab, *wqueue)
		{
			AlteredTableInfo *tab = (AlteredTableInfo *) lfirst(ltab);
			List	   *subcmds = tab->subcmds[pass];
			Relation	rel;
			ListCell   *lcmd;

			if (subcmds == NIL)
				continue;

			/*
			 * Exclusive lock was obtained by phase 1, needn't get it again
			 */
			rel = relation_open(tab->relid, NoLock);

			foreach(lcmd, subcmds)
			{
				AlterTableCmd *atc = lfirst(lcmd);

				ATExecCmd(wqueue, tab, rel, atc);

				/*
				 * SET DISTRIBUTED BY() calls RelationForgetRelation(),
				 * which will scribble on rel, so we must re-open it.
				 */
				if (atc->subtype == AT_SetDistributedBy)
					rel = relation_open(tab->relid, NoLock);
			}
			/*
			 * After the ALTER TYPE pass, do cleanup work (this is not done in
			 * ATExecAlterColumnType since it should be done only once if
			 * multiple columns of a table are altered).
			 */
			if (pass == AT_PASS_ALTER_TYPE)
				ATPostAlterTypeCleanup(wqueue, tab);

			relation_close(rel, NoLock);
		}
	}

}

static void
ATAddToastIfNeeded(List **wqueue, 
				   int oidInfoCount, TableOidInfo * oidInfo, List **poidmap)
{

	ListCell   *ltab;
	int			m = 0;
	
	/*
	 * Check to see if a toast table must be added, if we executed any
	 * subcommands that might have added a column or changed column storage.
	 */
	foreach(ltab, *wqueue)
	{
		Oid tOid = InvalidOid;
		Oid tiOid = InvalidOid;
		Oid	*toastComptypeOid = NULL;
		AlteredTableInfo *tab = (AlteredTableInfo *) lfirst(ltab);
		bool is_part = !rel_needs_long_lock(tab->relid);

		if (tab->relkind == RELKIND_RELATION &&
			(tab->subcmds[AT_PASS_ADD_COL] ||
			 tab->subcmds[AT_PASS_ALTER_TYPE] ||
			 tab->subcmds[AT_PASS_COL_ATTRS]))
		{
			Relation rel;
			Oid reltablespace;

			/*
			 * For upgrade, don't create a toast table.
			 */
			if (gp_upgrade_mode)
				continue;

			/* 
			 * Determine if we need to create a toast table.
			 */
			rel = heap_open(tab->relid, NoLock);
			if (rel->rd_rel->reltoastrelid != InvalidOid)
			{  /* Already have one */
				heap_close(rel, NoLock);
				continue;
			}
			if (!RelationNeedsToastTable(rel))
			{  /* Don't need one */
				heap_close(rel, NoLock);
				continue;
			}
			reltablespace = rel->rd_rel->reltablespace;
			heap_close(rel, NoLock);
			
			/* 
			 * We do not currently have a toast table, but we need one.  If we
			 * have not already allocated oids in the oidinfo then we need to do
			 * so now.
			 */
			if (Gp_role == GP_ROLE_DISPATCH)
			{
				Relation	pg_class_desc = NULL;
				Relation    pg_type_desc = NULL;

				Assert(oidInfo);
				Assert(m < oidInfoCount);

				if (oidInfo[m].toastOid == InvalidOid)
				{
					pg_class_desc = heap_open(RelationRelationId, RowExclusiveLock);
					oidInfo[m].toastOid = GetNewRelFileNode(reltablespace, false, pg_class_desc, false);
				}
				if (oidInfo[m].toastIndexOid == InvalidOid)
				{
					if (!pg_class_desc)
						pg_class_desc = heap_open(RelationRelationId, RowExclusiveLock);
					oidInfo[m].toastIndexOid = GetNewRelFileNode(reltablespace, false, pg_class_desc, false);
				}
				if (oidInfo[m].toastComptypeOid == InvalidOid)
				{
					pg_type_desc = heap_open(RelationRelationId, RowExclusiveLock);
					oidInfo[m].toastIndexOid = GetNewRelFileNode(reltablespace, false, pg_type_desc, false);
				}
				if (pg_class_desc)
					heap_close(pg_class_desc, NoLock);
				if (pg_type_desc)
					heap_close(pg_type_desc, NoLock);
			}

			/*
			 * Normal dispatch/execute mode will always have an oidInfo, but if
			 * we are in utility mode or bootstrap then we might not.  In this
			 * event we fall back to the InvalidOid as initialized above.
			 */
			if (oidInfo != NULL)
			{
				Assert(m < oidInfoCount);
				tOid = oidInfo[m].toastOid;
				tiOid = oidInfo[m].toastIndexOid;
				toastComptypeOid = &oidInfo[m].toastComptypeOid;
			}

			/* Finally create the toast table */
			elog(DEBUG2,"Create Toast %d with Oid %u", m, tOid);
			AlterTableCreateToastTableWithOid(tab->relid, tOid, tiOid, 
											  toastComptypeOid, is_part);
			m++;

		}
	}
}

/*
 * ATExecCmd: dispatch a subcommand to appropriate execution routine
 */
static void
ATExecCmd(List **wqueue, AlteredTableInfo *tab, Relation rel, AlterTableCmd *cmd)
{
	switch (cmd->subtype)
	{
		case AT_AddColumn:		/* ADD COLUMN */
		case AT_AddColumnRecurse:
			ATExecAddColumn(tab, rel, (ColumnDef *) cmd->def);
			break;
		case AT_ColumnDefault:	/* ALTER COLUMN DEFAULT */
			ATExecColumnDefault(rel, cmd->name, cmd->def);
			break;
		case AT_DropNotNull:	/* ALTER COLUMN DROP NOT NULL */
			ATExecDropNotNull(rel, cmd->name);
			break;
		case AT_SetNotNull:		/* ALTER COLUMN SET NOT NULL */
			ATExecSetNotNull(tab, rel, cmd->name);
			break;
		case AT_SetStatistics:	/* ALTER COLUMN STATISTICS */
			ATExecSetStatistics(rel, cmd->name, cmd->def);
			break;
		case AT_SetStorage:		/* ALTER COLUMN STORAGE */
			ATExecSetStorage(rel, cmd->name, cmd->def);
			break;
		case AT_DropColumn:		/* DROP COLUMN */
			ATExecDropColumn(wqueue, rel, cmd->name, cmd->behavior, false, false);
			break;
		case AT_DropColumnRecurse:		/* DROP COLUMN with recursion */
			ATExecDropColumn(wqueue, rel, cmd->name, cmd->behavior, true, false);
			break;
		case AT_AddIndex:		/* ADD INDEX */
			ATExecAddIndex(tab, rel, (IndexStmt *) cmd->def, false,
						   cmd->part_expanded);
			break;
		case AT_ReAddIndex:		/* ADD INDEX */
			ATExecAddIndex(tab, rel, (IndexStmt *) cmd->def, true,
						   cmd->part_expanded);
			break;
		case AT_AddConstraint:	/* ADD CONSTRAINT */
			ATExecAddConstraint(tab, rel, cmd->def, false);
			break;
		case AT_AddConstraintRecurse: /* ADD CONSTRAINT with recursion: internal */
			ATExecAddConstraint(tab, rel, cmd->def, true);
			break;
		case AT_DropConstraint:	/* DROP CONSTRAINT */
			ATExecDropConstraint(rel, cmd->name, cmd->behavior, false);
			break;
		case AT_DropConstraintQuietly:	/* DROP CONSTRAINT for child */
			ATExecDropConstraint(rel, cmd->name, cmd->behavior, true);
			break;
		case AT_AlterColumnType:		/* ALTER COLUMN TYPE */
			ATExecAlterColumnType(tab, rel, cmd->name, (TypeName *) cmd->def);
			break;
		case AT_ChangeOwner:	/* ALTER OWNER */
			ATExecChangeOwner(RelationGetRelid(rel),
							  get_roleid_checked(cmd->name),
							  false);
			break;
		case AT_ClusterOn:		/* CLUSTER ON */
			ATExecClusterOn(rel, cmd->name);
			break;
		case AT_DropCluster:	/* SET WITHOUT CLUSTER */
			ATExecDropCluster(rel);
			break;
		case AT_DropOids:		/* SET WITHOUT OIDS */

			/*
			 * Nothing to do here; we'll have generated a DropColumn
			 * subcommand to do the real work
			 */
			break;
		case AT_SetTableSpace:	/* SET TABLESPACE */

			/*
			 * Nothing to do here; Phase 3 does the work
			 */
			break;
		case AT_SetRelOptions:	/* SET (...) */
			ATExecSetRelOptions(rel, (List *) cmd->def, false);
			break;
		case AT_ResetRelOptions:		/* RESET (...) */
			ATExecSetRelOptions(rel, (List *) cmd->def, true);
			break;
		case AT_EnableTrig:		/* ENABLE TRIGGER name */
			ATExecEnableDisableTrigger(rel, cmd->name, true, false);
			break;
		case AT_DisableTrig:	/* DISABLE TRIGGER name */
			ATExecEnableDisableTrigger(rel, cmd->name, false, false);
			break;
		case AT_EnableTrigAll:	/* ENABLE TRIGGER ALL */
			ATExecEnableDisableTrigger(rel, NULL, true, false);
			break;
		case AT_DisableTrigAll:	/* DISABLE TRIGGER ALL */
			ATExecEnableDisableTrigger(rel, NULL, false, false);
			break;
		case AT_EnableTrigUser:	/* ENABLE TRIGGER USER */
			ATExecEnableDisableTrigger(rel, NULL, true, true);
			break;
		case AT_DisableTrigUser:		/* DISABLE TRIGGER USER */
			ATExecEnableDisableTrigger(rel, NULL, false, true);
			break;
		case AT_AddInherit:
			ATExecAddInherit(rel, (Node *) cmd->def);
			break;
		case AT_DropInherit:
			ATExecDropInherit(rel, (RangeVar *) cmd->def, false);
			break;
		case AT_SetDistributedBy:	/* SET DISTRIBUTED BY */
			ATExecSetDistributedBy(rel, (Node *) cmd->def, cmd);
			break;
			/* CDB: Partitioned Table commands */
		case AT_PartAdd:				/* Add */
			ATPExecPartAdd(tab, rel, (AlterPartitionCmd *) cmd->def,
						   cmd->subtype);
            break;
		case AT_PartAlter:				/* Alter */
            ATPExecPartAlter(wqueue, tab, rel, (AlterPartitionCmd *) cmd->def);
            break;
		case AT_PartCoalesce:			/* Coalesce */
            ATPExecPartCoalesce(rel, (AlterPartitionCmd *) cmd->def);
            break;
		case AT_PartDrop:				/* Drop */
            ATPExecPartDrop(rel, (AlterPartitionCmd *) cmd->def);
            break;
		case AT_PartExchange:			/* Exchange */
            ATPExecPartExchange(tab, rel, (AlterPartitionCmd *) cmd->def);
            break;
		case AT_PartMerge:				/* Merge */
            ATPExecPartMerge(rel, (AlterPartitionCmd *) cmd->def);
            break;
		case AT_PartModify:				/* Modify */
            ATPExecPartModify(rel, (AlterPartitionCmd *) cmd->def);
            break;
		case AT_PartRename:				/* Rename */
            ATPExecPartRename(rel, (AlterPartitionCmd *) cmd->def);
            break;
		case AT_PartSetTemplate:		/* Set Subpartition Template */
			ATPExecPartSetTemplate(tab, rel, (AlterPartitionCmd *) cmd->def);
            break;
		case AT_PartSplit:				/* Split */
            ATPExecPartSplit(rel, (AlterPartitionCmd *) cmd->def);
            break;
		case AT_PartTruncate:			/* Truncate */
			ATPExecPartTruncate(rel, (AlterPartitionCmd *) cmd->def);
            break;
		case AT_PartAddInternal:
			ATExecPartAddInternal(rel, cmd->def);
			break;
		default:				/* oops */
			elog(ERROR, "unrecognized alter table type: %d",
				 (int) cmd->subtype);
			break;
	}

	/*
	 * Bump the command counter to ensure the next subcommand in the sequence
	 * can see the changes so far
	 */
	CommandCounterIncrement();
}

/*
 * ATRewriteTables: ALTER TABLE phase 3
 */
static void
ATRewriteTables(List **wqueue, 
				int oidInfoCount, TableOidInfo * oidInfo, List **poidmap)
{
	ListCell   *ltab;
	int			m = 0;

	/* Go through each table that needs to be checked or rewritten */
	foreach(ltab, *wqueue)
	{
		AlteredTableInfo *tab = (AlteredTableInfo *) lfirst(ltab);
		Relation rel;
		bool relisshared;
		bool relisnailed;
		Oid  newTableSpace;
		Oid  oldTableSpace;
		Oid  relNamespace;

		/* We will lock the table iff we decide to actually rewrite it */
		rel = relation_open(tab->relid, NoLock);
		relisshared   = rel->rd_rel->relisshared;
		relisnailed   = rel->rd_isnailed;
		relNamespace  = RelationGetNamespace(rel);
		oldTableSpace = rel->rd_rel->reltablespace;
		newTableSpace = tab->newTableSpace ? tab->newTableSpace : oldTableSpace;

		if (tab->newTableSpace) {
			RejectAccessTablespace(oldTableSpace, "cannot move from shared tablespace %s");
			RejectAccessTablespace(newTableSpace, "cannot move to shared tablespace %s");
			/* If this is an index and we are in a shared storage database, reject change index tablespace */
			if (rel->rd_rel->relkind == RELKIND_INDEX && is_tablespace_shared_master(get_database_dts(MyDatabaseId)))
				RejectAccessTablespace(get_database_dts(MyDatabaseId), "cannot change tablespace on shared storage");
		}
		/*
		 * There are two cases where we will rewrite the table, for these cases
		 * run the necessary sanity checks.
		 */
		if (tab->newvals != NIL || tab->newTableSpace)
		{
			/* 
			 * AlterTableInternal is not allowed to execute alter table
			 * subcommands that require allocating new relfilenodes since it is
			 * executed in a context that doesn't dispatch the Alter Table.
			 *
			 * This should never occur.
			 */
			if (!oidInfo)
				elog(ERROR, 
					 "internal alter table cannot allocate new relfilenodes");
			Assert(m < oidInfoCount);

			/*
			 * We can never allow rewriting of shared or nailed-in-cache
			 * relations, because we can't support changing their relfilenode
			 * values.
			 */
			if (relisshared || relisnailed)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot rewrite system relation \"%s\"",
								RelationGetRelationName(rel))));

			/*
			 * Don't allow rewrite on temp tables of other backends ... their
			 * local buffer manager is not going to cope.
			 */
			if (isOtherTempNamespace(relNamespace))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("cannot rewrite temporary tables of other sessions")));

		}
		heap_close(rel, NoLock);

		/*
		 * We only need to rewrite the table if at least one column needs to be
		 * recomputed, or we are removing the OID column.
		 */
		if (tab->newvals != NIL || tab->new_dropoids)
		{
			/* Sanity check of oidInfoCount */
			Insist(oidInfoCount==list_length(*wqueue));

			/* Build a temporary relation and copy data */
			char		NewHeapName[NAMEDATALEN];
			Oid         OIDNewHeap;
			ObjectAddress object;

			/*
			 * Create the new heap, using a temporary name in the same
			 * namespace as the existing table.  NOTE: there is some risk of
			 * collision with user relnames.  Working around this seems more
			 * trouble than it's worth; in particular, we can't create the new
			 * heap in a different namespace from the old, or we will have
			 * problems with the TEMP status of temp tables.
			 */
			snprintf(NewHeapName, sizeof(NewHeapName),
					 "pg_temp_%u", tab->relid);

            List *indexIds = RelationGetIndexList(rel);
            OIDNewHeap = make_new_heap(tab->relid, NewHeapName, newTableSpace,
                                       &oidInfo[m], list_length(indexIds) > 0);
            list_free(indexIds);

			/*
			 * Copy the heap data into the new table with the desired
			 * modifications, and test the current data within the table
			 * against new constraints generated by ALTER TABLE commands.
			 */
			ATRewriteTable(tab, OIDNewHeap);

			/* 
			 * Swap the physical files of the old and new heaps. 
			 *
			 * MPP-17516 - The third argument to swap_relation_files is 
			 * "swap_stats", and it dictates whether the relpages and 
			 * reltuples of the fake relfile should be copied over to our 
			 * original pg_class tuple. We do not want to do this in the case 
			 * of ALTER TABLE rewrites as the temp relfile will not have correct 
			 * stats.
			 */
			swap_relation_files(tab->relid, OIDNewHeap, false);

			CommandCounterIncrement();

			/* Destroy new heap with old filenode */
			object.classId = RelationRelationId;
			object.objectId = OIDNewHeap;
			object.objectSubId = 0;

			/*
			 * The new relation is local to our transaction and we know
			 * nothing depends on it, so DROP_RESTRICT should be OK.
			 */
			performDeletion(&object, DROP_RESTRICT);
			/* performDeletion does CommandCounterIncrement at end */

			/*
			 * Rebuild each index on the relation (but not the toast table,
			 * which is all-new anyway).  We do not need
			 * CommandCounterIncrement() because reindex_relation does it.
			 * reindex_relation also checks for non-NULL poidmap, so we don't
			 * need to.
			 *	
			 * It is not legal to reindex without dispatch of the associated
			 * oids!
			 */
			reindex_relation(tab->relid, false, false, false, 
							 poidmap, poidmap && Gp_role == GP_ROLE_DISPATCH);
		}
		else
		{
			/*
			 * Test the current data within the table against new constraints
			 * generated by ALTER TABLE commands, but don't rebuild data.
			 */
			if (tab->constraints != NIL || tab->new_notnull)
				ATRewriteTable(tab, InvalidOid);

			/*
			 * If we had SET TABLESPACE but no reason to reconstruct tuples,
			 * just do a block-by-block copy.
			 */
			if (tab->newTableSpace)
			{
				populate_oidInfo(&oidInfo[m], newTableSpace, relisshared, 
								 false);
				
				ATExecSetTableSpace(tab->relid, tab->newTableSpace, 
									&oidInfo[m]);
			}
		}
		m++;

	}

	/*
	 * Foreign key constraints are checked in a final pass, since (a) it's
	 * generally best to examine each one separately, and (b) it's at least
	 * theoretically possible that we have changed both relations of the
	 * foreign key, and we'd better have finished both rewrites before we try
	 * to read the tables.
	 */
	foreach(ltab, *wqueue)
	{
		AlteredTableInfo *tab = (AlteredTableInfo *) lfirst(ltab);
		Relation	rel = NULL;
		ListCell   *lcon;

		foreach(lcon, tab->constraints)
		{
			NewConstraint *con = lfirst(lcon);

			if (con->contype == CONSTR_FOREIGN)
			{
				FkConstraint *fkconstraint = (FkConstraint *) con->qual;
				Relation	refrel;

				if (rel == NULL)
				{
					/* Long since locked, no need for another */
					rel = heap_open(tab->relid, NoLock);
				}

				refrel = heap_open(con->refrelid, RowShareLock);

				validateForeignKeyConstraint(fkconstraint, rel, refrel);

				heap_close(refrel, NoLock);
			}
		}

		if (rel)
			heap_close(rel, NoLock);
	}
}

/*
 * ATRewriteTable: scan or rewrite one table
 *
 * OIDNewHeap is InvalidOid if we don't need to rewrite
 */
static void
ATRewriteTable(AlteredTableInfo *tab, Oid OIDNewHeap)
{
	Relation	oldrel;
	Relation	newrel;
	TupleDesc	oldTupDesc;
	TupleDesc	newTupDesc;
	bool		needscan = false;
	List	   *notnull_attrs;
	int			i;
	ListCell   *l;
	EState	   *estate;

	/*
	 * Open the relation(s).  We have surely already locked the existing table.
	 * In EXCHANGE of partition case, we only need to validate the content
	 * based on new constraints.  oldTupDesc should point to the oldrel's tuple
	 * descriptor since tab->oldDesc comes from the parent partition.
	 */
	if (OidIsValid(tab->exchange_relid))
	{
		oldrel = heap_open(tab->exchange_relid, NoLock);
		oldTupDesc = RelationGetDescr(oldrel);
	}
	else
	{
		oldrel = heap_open(tab->relid, NoLock);
		oldTupDesc = tab->oldDesc;
	}

	newTupDesc = RelationGetDescr(oldrel);		/* includes all mods */

	if (OidIsValid(OIDNewHeap))
		newrel = heap_open(OIDNewHeap, AccessExclusiveLock);
	else
		newrel = NULL;

	/*
	 * If we need to rewrite the table, the operation has to be propagated to
	 * tables that use this table's rowtype as a column type.
	 *
	 * (Eventually this will probably become true for scans as well, but at
	 * the moment a composite type does not enforce any constraints, so it's
	 * not necessary/appropriate to enforce them just during ALTER.)
	 */
	if (newrel)
		find_composite_type_dependencies(oldrel->rd_rel->reltype,
										 RelationGetRelationName(oldrel),
										 NULL);

	/*
	 * Generate the constraint and default execution states
	 */

	estate = CreateExecutorState();

	/* Build the needed expression execution states */
	foreach(l, tab->constraints)
	{
		NewConstraint *con = lfirst(l);

		switch (con->contype)
		{
			case CONSTR_CHECK:
				needscan = true;
				con->qualstate = (List *)
					ExecPrepareExpr((Expr *) con->qual, estate);
				break;
			case CONSTR_FOREIGN:
				/* Nothing to do here */
				break;
			default:
				elog(ERROR, "unrecognized constraint type: %d",
					 (int) con->contype);
		}
	}

	foreach(l, tab->newvals)
	{
		NewColumnValue *ex = lfirst(l);

		needscan = true;

		ex->exprstate = ExecPrepareExpr((Expr *) ex->expr, estate);
	}

	notnull_attrs = NIL;
	if (newrel || tab->new_notnull)
	{
		/*
		 * If we are rebuilding the tuples OR if we added any new NOT NULL
		 * constraints, check all not-null constraints.  This is a bit of
		 * overkill but it minimizes risk of bugs, and heap_attisnull is a
		 * pretty cheap test anyway.
		 */
		for (i = 0; i < newTupDesc->natts; i++)
		{
			if (newTupDesc->attrs[i]->attnotnull &&
				!newTupDesc->attrs[i]->attisdropped)
				notnull_attrs = lappend_int(notnull_attrs, i);
		}
		if (notnull_attrs)
			needscan = true;
	}

	if (newrel || needscan)
	{
		ExprContext *econtext;
		Datum	   *values;
		bool	   *isnull;
		TupleTableSlot *oldslot;
		TupleTableSlot *newslot;
		HeapScanDesc heapscan = NULL;
		AppendOnlyScanDesc aoscan = NULL;
		HeapTuple	htuple = NULL;
		MemTuple	mtuple = NULL;
		MemoryContext oldCxt;
		List	   *dropped_attrs = NIL;
		ListCell   *lc;
		char		relstorage = oldrel->rd_rel->relstorage;

		econtext = GetPerTupleExprContext(estate);

		/*
		 * Make tuple slots for old and new tuples.  Note that even when the
		 * tuples are the same, the tupDescs might not be (consider ADD COLUMN
		 * without a default).
		 */
		oldslot = MakeSingleTupleTableSlot(oldTupDesc);
		newslot = MakeSingleTupleTableSlot(newTupDesc);

		/* Preallocate values/isnull arrays */
		i = Max(newTupDesc->natts, oldTupDesc->natts);
		values = (Datum *) palloc0(i * sizeof(Datum));
		isnull = (bool *) palloc(i * sizeof(bool));
		memset(isnull, true, i * sizeof(bool));

		/*
		 * Any attributes that are dropped according to the new tuple
		 * descriptor can be set to NULL. We precompute the list of dropped
		 * attributes to avoid needing to do so in the per-tuple loop.
		 */
		for (i = 0; i < newTupDesc->natts; i++)
		{
			if (newTupDesc->attrs[i]->attisdropped)
				dropped_attrs = lappend_int(dropped_attrs, i);
		}


		/*
		 * Scan through the rows, generating a new row if needed and then
		 * checking all the constraints.
		 */
		if(relstorage == RELSTORAGE_HEAP)
		{
			heapscan = heap_beginscan(oldrel, SnapshotNow, 0, NULL);

			/*
			 * Switch to per-tuple memory context and reset it for each tuple
			 * produced, so we don't leak memory.
			 */
			oldCxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

			while ((htuple = heap_getnext(heapscan, ForwardScanDirection)) != NULL)
			{
				if (newrel)
				{
					Oid			tupOid = InvalidOid;

					/* Extract data from old tuple */
					heap_deform_tuple(htuple, oldTupDesc, values, isnull);
					if (oldTupDesc->tdhasoid)
						tupOid = HeapTupleGetOid(htuple);

					/* Set dropped attributes to null in new tuple */
					foreach(lc, dropped_attrs)
						isnull[lfirst_int(lc)] = true;

					/*
					 * Process supplied expressions to replace selected columns.
					 * Expression inputs come from the old tuple.
					 */
					ExecStoreHeapTuple(htuple, oldslot, InvalidBuffer, false);
					econtext->ecxt_scantuple = oldslot;

					foreach(l, tab->newvals)
					{
						NewColumnValue *ex = lfirst(l);

						values[ex->attnum - 1] = ExecEvalExpr(ex->exprstate,
															  econtext,
															  &isnull[ex->attnum - 1],
															  NULL);
					}

					/*
					 * Form the new tuple. Note that we don't explicitly pfree it,
					 * since the per-tuple memory context will be reset shortly.
					 */
					htuple = heap_form_tuple(newTupDesc, values, isnull);

					/* Preserve OID, if any */
					if (newTupDesc->tdhasoid)
						HeapTupleSetOid(htuple, tupOid);
				}

				/* Now check any constraints on the possibly-changed tuple */
				ExecStoreHeapTuple(htuple, newslot, InvalidBuffer, false);
				econtext->ecxt_scantuple = newslot;

				foreach(l, notnull_attrs)
				{
					int			attn = lfirst_int(l);

					if (heap_attisnull(htuple, attn + 1))
						ereport(ERROR,
								(errcode(ERRCODE_NOT_NULL_VIOLATION),
								 errmsg("column \"%s\" contains null values",
										NameStr(newTupDesc->attrs[attn]->attname)),
								 errOmitLocation(true)));
				}

				foreach(l, tab->constraints)
				{
					NewConstraint *con = lfirst(l);

					switch (con->contype)
					{
						case CONSTR_CHECK:
							if (!ExecQual(con->qualstate, econtext, true))
							{
								if (OidIsValid(tab->exchange_relid))
									ereport(ERROR,
											(errcode(ERRCODE_CHECK_VIOLATION),
											 errmsg("exchange table contains "
													"a row which violates the "
													"partitioning "
													"specification of \"%s\"",
													get_rel_name(tab->relid)),
											 errOmitLocation(true)));
								else
									ereport(ERROR,
											(errcode(ERRCODE_CHECK_VIOLATION),
											 errmsg("check constraint \"%s\" is violated by some row",
												con->name),
											 errOmitLocation(true)));
							}
							break;
						case CONSTR_FOREIGN:
							/* Nothing to do here */
							break;
						default:
							elog(ERROR, "unrecognized constraint type: %d",
								 (int) con->contype);
					}
				}

				/* Write the tuple out to the new relation */
				if (newrel)
					simple_heap_insert(newrel, htuple);

				ResetExprContext(econtext);

				CHECK_FOR_INTERRUPTS();
			}

			MemoryContextSwitchTo(oldCxt);
			heap_endscan(heapscan);

		}
		else if(relstorage == RELSTORAGE_AOROWS && Gp_role != GP_ROLE_DISPATCH)
		{
			/*
			 * When rewriting an appendonly table we choose to always insert
			 * into the segfile reserved for special purposes (segno #0).
			 */
			int 					segno = list_nth_int(tab->ao_segnos, GetQEIndex());;
			AppendOnlyInsertDesc 	aoInsertDesc = NULL;
			MemTupleBinding*		mt_bind;

			if(newrel)
			{
			  ResultRelSegFileInfo *segfileinfo = InitResultRelSegFileInfo(segno, RELSTORAGE_AOROWS, 1);
				aoInsertDesc = appendonly_insert_init(newrel, segfileinfo);
			}

			mt_bind = (newrel ? aoInsertDesc->mt_bind : create_memtuple_binding(newTupDesc));

			aoscan = appendonly_beginscan(oldrel, SnapshotNow, 0, NULL);
		  aoscan->splits = GetFileSplitsOfSegment(tab->scantable_splits,
		          oldrel->rd_id, GetQEIndex());
			/*
			 * Switch to per-tuple memory context and reset it for each tuple
			 * produced, so we don't leak memory.
			 */
			oldCxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

			while ((mtuple = appendonly_getnext(aoscan, ForwardScanDirection, oldslot)) != NULL)
			{
				if (newrel)
				{
					Oid			tupOid = InvalidOid;

					TupClearShouldFree(oldslot);
					econtext->ecxt_scantuple = oldslot;

					/* Extract data from old tuple */
					for(i = 0; i < oldslot->tts_tupleDescriptor->natts ; i++)
						values[i] = slot_getattr(oldslot, i+1, &isnull[i]);

					if (oldTupDesc->tdhasoid)
						tupOid = MemTupleGetOid(mtuple, mt_bind);

					/* Set dropped attributes to null in new tuple */
					foreach(lc, dropped_attrs)
						isnull[lfirst_int(lc)] = true;

					/*
					 * Process supplied expressions to replace selected columns.
					 * Expression inputs come from the old tuple.
					 */
					foreach(l, tab->newvals)
					{
						NewColumnValue *ex = lfirst(l);

						values[ex->attnum - 1] = ExecEvalExpr(ex->exprstate,
															  econtext,
															  &isnull[ex->attnum - 1],
															  NULL);
					}

					/*
					 * Form the new tuple. Note that we don't explicitly pfree it,
					 * since the per-tuple memory context will be reset shortly.
					 */
					mtuple = memtuple_form_to(mt_bind,
											  values, isnull,
											  NULL, NULL, false);


					/* Preserve OID, if any */
					if (newTupDesc->tdhasoid)
						MemTupleSetOid(mtuple, mt_bind, tupOid);
				}

				/* Now check any constraints on the possibly-changed tuple */
				ExecStoreMemTuple(mtuple, newslot, false);
				econtext->ecxt_scantuple = newslot;

				foreach(l, notnull_attrs)
				{
					int					attn = lfirst_int(l);
					
					if (memtuple_attisnull(mtuple, mt_bind, attn + 1))
						ereport(ERROR,
								(errcode(ERRCODE_NOT_NULL_VIOLATION),
								 errmsg("column \"%s\" contains null values",
										NameStr(newTupDesc->attrs[attn]->attname)),
								 errOmitLocation(true)));
				}

				foreach(l, tab->constraints)
				{
					NewConstraint *con = lfirst(l);

					switch (con->contype)
					{
						case CONSTR_CHECK:
							if (!ExecQual(con->qualstate, econtext, true))
								ereport(ERROR,
										(errcode(ERRCODE_CHECK_VIOLATION),
										 errmsg("check constraint \"%s\" is violated by some row",
												con->name),
										 errOmitLocation(true)));
							break;
						case CONSTR_FOREIGN:
							/* Nothing to do here */
							break;
						default:
							elog(ERROR, "unrecognized constraint type: %d",
								 (int) con->contype);
					}
				}

				/* Write the tuple out to the new relation */
				if (newrel)
				{
					Oid 		tupleOid;
					AOTupleId	aoTupleId;
					
					appendonly_insert(aoInsertDesc, mtuple, &tupleOid, &aoTupleId);
				}

				ResetExprContext(econtext);

				CHECK_FOR_INTERRUPTS();

			}

			MemoryContextSwitchTo(oldCxt);
			appendonly_endscan(aoscan);

			if(newrel)
			{
				StringInfo buf = NULL;
				QueryContextDispatchingSendBack sendback = NULL;

				if (Gp_role == GP_ROLE_EXECUTE)
					buf = PreSendbackChangedCatalog(1);

				sendback = CreateQueryContextDispatchingSendBack(1);
				aoInsertDesc->sendback = sendback;
				sendback->relid = RelationGetRelid(aoInsertDesc->aoi_rel);

				appendonly_insert_finish(aoInsertDesc);

				if (Gp_role == GP_ROLE_EXECUTE)
					AddSendbackChangedCatalogContent(buf, sendback);

				DropQueryContextDispatchingSendBack(sendback);

				if (Gp_role == GP_ROLE_EXECUTE)
					FinishSendbackChangedCatalog(buf);
			}
		}
		else if(relstorage == RELSTORAGE_PARQUET && Gp_role != GP_ROLE_DISPATCH)
		{
			int segno = list_nth_int(tab->ao_segnos, GetQEIndex());
			ParquetInsertDesc idesc = NULL;
			ParquetScanDesc sdesc = NULL;
			int nvp = oldrel->rd_att->natts;
			bool *proj = palloc0(sizeof(bool) * nvp);

			/*
			 * We use the old tuple descriptor instead of oldrel's tuple descriptor,
			 * which may already contain altered column.
			 */
			if (oldTupDesc)
			{
				Assert(oldTupDesc->natts <= nvp);
				memset(proj, true, oldTupDesc->natts);
			}
			else
				memset(proj, true, nvp);

			if(newrel)
			{
			  ResultRelSegFileInfo *segfileinfo = InitResultRelSegFileInfo(segno, RELSTORAGE_PARQUET, 1);
				idesc = parquet_insert_init(newrel, segfileinfo);
			}

			sdesc = parquet_beginscan(oldrel, SnapshotNow, oldTupDesc, proj);
			sdesc->splits = GetFileSplitsOfSegment(tab->scantable_splits,
			                oldrel->rd_id, GetQEIndex());
			parquet_getnext(sdesc, ForwardScanDirection, oldslot);


			while(!TupIsNull(oldslot))
			{
				oldCxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
				econtext->ecxt_scantuple = oldslot;

				if(newrel)
				{
					Datum *slotvalues = slot_get_values(oldslot);
					bool  *slotnulls = slot_get_isnull(oldslot);
					Datum *newslotvalues = slot_get_values(newslot);
					bool *newslotnulls = slot_get_isnull(newslot);

					int nv = Min(oldslot->tts_tupleDescriptor->natts, newslot->tts_tupleDescriptor->natts);

					/* aocs does not do oid yet */
					Assert(!oldTupDesc->tdhasoid);
					Assert(!newTupDesc->tdhasoid);

					/* Dropped att should be set correctly by aocs_getnext */
					/* transfer values from old to new slot */

					memcpy(newslotvalues, slotvalues, sizeof(Datum) * nv);
					memcpy(newslotnulls, slotnulls, sizeof(bool) * nv);
					TupSetVirtualTupleNValid(newslot, nv);

					/* Process supplied expressions */
					foreach(l, tab->newvals)
					{
						NewColumnValue *ex = lfirst(l);

						newslotvalues[ex->attnum-1] =
							ExecEvalExpr(ex->exprstate,
										 econtext,
										 &newslotnulls[ex->attnum-1],
										 NULL
								);

						if (ex->attnum > nv)
							TupSetVirtualTupleNValid(newslot, ex->attnum);
					}

					/* Use the modified tuple to check constraints below */
					econtext->ecxt_scantuple = newslot;
				}

				/* Check constraints */
				foreach (l, tab->constraints)
				{
					NewConstraint *con = lfirst(l);
					switch(con->contype)
					{
						case CONSTR_CHECK:
							if(!ExecQual(con->qualstate, econtext, true))
								ereport(ERROR,
										(errcode(ERRCODE_CHECK_VIOLATION),
										 errmsg("check constraint \"%s\" is violated",
												con->name
											 ),
										 errOmitLocation(true)));
							break;
						case CONSTR_FOREIGN:
							/* Nothing to do */
							break;
						default:
							elog(ERROR, "Unrecognized constraint type: %d",
								 (int) con->contype);
					}
				}

				if(newrel)
				{
					MemoryContextSwitchTo(oldCxt);
					parquet_insert(idesc, newslot);
				}

				ResetExprContext(econtext);
				CHECK_FOR_INTERRUPTS();

				MemoryContextSwitchTo(oldCxt);
				parquet_getnext(sdesc, ForwardScanDirection, oldslot);
			}

			parquet_endscan(sdesc);

			if(newrel)
			{
				StringInfo buf = NULL;
				QueryContextDispatchingSendBack sendback = NULL;

				if (Gp_role == GP_ROLE_EXECUTE)
					buf = PreSendbackChangedCatalog(1);

				sendback = CreateQueryContextDispatchingSendBack(
						idesc->parquet_rel->rd_att->natts);

				idesc->sendback = sendback;
				sendback->relid = RelationGetRelid(idesc->parquet_rel);

				parquet_insert_finish(idesc);

				if (Gp_role == GP_ROLE_EXECUTE)
					AddSendbackChangedCatalogContent(buf, sendback);

				DropQueryContextDispatchingSendBack(sendback);

				if (Gp_role == GP_ROLE_EXECUTE)
					FinishSendbackChangedCatalog(buf);
			}
			pfree(proj);
		}
		else if(relstorage_is_ao(relstorage) && Gp_role == GP_ROLE_DISPATCH)
		{
			/*
			 * All we want to do on the QD during an AO table rewrite
			 * is to drop the shared memory hash table entry for this
			 * table if it exists. We must do so since before the
			 * rewrite we probably have few non-zero segfile entries
			 * for this table while after the rewrite only segno zero
			 * will be full and the others will be empty. By dropping
			 * the hash entry we force refreshing the entry from the
			 * catalog the next time a write into this AO table comes
			 * along.
			 *
			 * Note that ALTER already took an exclusive lock on the
			 * old relation so we are guaranteed to not drop the hash
			 * entry from under any concurrent operation.
			 */
			LWLockAcquire(AOSegFileLock, LW_EXCLUSIVE);
			AORelRemoveHashEntry(RelationGetRelid(oldrel), false);
			LWLockRelease(AOSegFileLock);
		}
		else
		{
			Assert(!"Invalid relstorage type");
		}

		ExecDropSingleTupleTableSlot(oldslot);
		ExecDropSingleTupleTableSlot(newslot);
	}

	FreeExecutorState(estate);

	/*
	 * In hawq, here we dispatch the process to segments as opposed to what
	 * we did in GPDB which is to dispatch the whole statement from the top-level.
	 * The reason is we need a new relation created, but not yet swapped, so
	 * the catalog status is somewhat transient.  This is the only timing we
	 * can construct metadata dispatch and delegate to segments.  We don't
	 * need to dispatch the process if ATRewriteTable has no work to do.
	 */
	if (Gp_role == GP_ROLE_DISPATCH && (newrel || needscan))
	{
		AlterRewriteTableInfo *ar_tab;
		QueryContextInfo *contextdisp;
		DispatchDataResult result;
		QueryResource *resource = NULL;
		int target_segment_num = -1;
		List *segment_segnos = NIL;
		QueryResource *savedResource = NULL;

		{
		  /*
		   * calculate the target segment number based on
		   * the target relation.
		   */
		  GpPolicy *targetPolicy = NULL;

		  targetPolicy = GpPolicyFetch(CurrentMemoryContext, tab->relid);
		  Assert(targetPolicy);
		  target_segment_num = targetPolicy->bucketnum;
		  pfree(targetPolicy);

      if (newrel)
      {
        targetPolicy = GpPolicyFetch(CurrentMemoryContext, newrel->rd_id);
        Assert(targetPolicy);
        if (target_segment_num != targetPolicy->bucketnum)
        {
          pfree(targetPolicy);
          elog(ERROR, "target segment num %d IS NOT equal to the bucket number of this relation %d", target_segment_num, targetPolicy->bucketnum);
        }
        pfree(targetPolicy);
      }
		}

	  resource = AllocateResource(QRL_ONCE, 1, 1, target_segment_num, target_segment_num,NULL,0);
	  savedResource = GetActiveQueryResource();
	  SetActiveQueryResource(resource);

	  segment_segnos = SetSegnoForWrite(NIL, 0, target_segment_num, true, false);

    /*
     * We create seg files for the new relation here.
     */
    if (newrel)
    {
      CreateAppendOnlyParquetSegFileForRelationOnMaster(newrel, segment_segnos);
    }

		/* prepare for the metadata dispatch */
		contextdisp = CreateQueryContextInfo();
		ar_tab = prepareAlteredTableInfo(tab);
		ar_tab->scantable_splits = NIL;
		ar_tab->ao_segnos = segment_segnos;

		/*
		 * For the original table, there could be a case to scan
		 * partition table, while the new rel is always a new single
		 * relation.
		 */
		prepareDispatchedCatalogRelation(contextdisp,
										 tab->relid,
										 false,
										 NULL);
		ar_tab->scantable_splits = AssignAOSegFileSplitToSegment(tab->relid, NIL, true,
		                    target_segment_num, ar_tab->scantable_splits);
		/*
		 * Specify the segno directly as we don't have segno mapping here.
		 */
		if (OidIsValid(OIDNewHeap))
			prepareDispatchedCatalogSingleRelation(contextdisp,
												   OIDNewHeap,
												   true,
												   segment_segnos);
		FinalizeQueryContextInfo(contextdisp);

		ar_tab->newheap_oid = OIDNewHeap;

    dispatch_statement_node((Node *) ar_tab, contextdisp, resource, &result);
    cdbdisp_iterate_results_sendback(result.result, result.numresults,
                UpdateCatalogModifiedOnSegments);
    dispatch_free_result(&result);
    DropQueryContextInfo(contextdisp);
    FreeResource(resource);
    SetActiveQueryResource(savedResource);
	}

	heap_close(oldrel, NoLock);
	if (newrel)
		heap_close(newrel, NoLock);

}

/*
 * ATGetQueueEntry: find or create an entry in the ALTER TABLE work queue
 */
static AlteredTableInfo *
ATGetQueueEntry(List **wqueue, Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	AlteredTableInfo *tab;
	ListCell   *ltab;

	foreach(ltab, *wqueue)
	{
		tab = (AlteredTableInfo *) lfirst(ltab);
		if (tab->relid == relid)
			return tab;
	}

	/*
	 * Not there, so add it.  Note that we make a copy of the relation's
	 * existing descriptor before anything interesting can happen to it.
	 */
	tab = (AlteredTableInfo *) palloc0(sizeof(AlteredTableInfo));
	tab->relid = relid;
	tab->relkind = rel->rd_rel->relkind;
	tab->oldDesc = CreateTupleDescCopy(RelationGetDescr(rel));

	*wqueue = lappend(*wqueue, tab);

	return tab;
}

/*
 * Issue an error, if this is a part of a partitioned table or, in case 
 * rejectroot is true, if this is a partitioned table itself.
 *
 * Usually called from ATPrepCmd to validate a subcommand of an ALTER
 * TABLE command.  Many commands may be invoked on an entire partitioned
 * table, but not on just a part.  These specify rejectroot as false.
 */
static void
ATPartitionCheck(AlterTableType subtype, Relation rel, bool rejectroot, bool recursing)
{
	if (recursing)
		return;

	if (rejectroot &&
		(rel_is_child_partition(RelationGetRelid(rel)) ||
		 rel_is_partitioned(RelationGetRelid(rel))))
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("can't %s \"%s\"; it is a partitioned table or part thereof",
						alterTableCmdString(subtype),
						RelationGetRelationName(rel)),
				 errOmitLocation(true)));
	}
	else if (rel_is_child_partition(RelationGetRelid(rel)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("can't %s \"%s\"; it is part of a partitioned table",
						alterTableCmdString(subtype),
						RelationGetRelationName(rel)),
				 errhint("You may be able to perform the operation on the partitioned table as a whole."),
				 errOmitLocation(true)));
	}
}

/*
 * ATSimplePermissions
 *
 * - Ensure that it is a relation (or possibly a view)
 * - Ensure this user is the owner
 * - Ensure that it is not a system table
 */
static void
ATSimplePermissions(Relation rel, bool allowView)
{
	if (rel->rd_rel->relkind != RELKIND_RELATION)
	{
		if (allowView)
		{
			if (rel->rd_rel->relkind != RELKIND_VIEW)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is not a table or view",
								RelationGetRelationName(rel)),
						 errOmitLocation(true)));
		}
		else if (!IsUnderPostmaster &&
				 (rel->rd_rel->relkind == RELKIND_AOSEGMENTS ||
				  rel->rd_rel->relkind == RELKIND_AOBLOCKDIR))
		{
			/*
			 * Allow ALTER TABLE operations in standard alone mode on
			 * AO segment tables.
			 */
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is not a table",
							RelationGetRelationName(rel)),
					 errOmitLocation(true)));
	}

	/* Permissions checks */
	if (!pg_class_ownercheck(RelationGetRelid(rel), GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
					   RelationGetRelationName(rel));

	if (!allowSystemTableModsDDL && IsSystemRelation(rel))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied: \"%s\" is a system catalog",
						RelationGetRelationName(rel)),
								   errOmitLocation(true)));
}

/*
 * ATSimplePermissionsRelationOrIndex
 *
 * - Ensure that it is a relation or an index
 * - Ensure this user is the owner
 * - Ensure that it is not a system table
 */
static void
ATSimplePermissionsRelationOrIndex(Relation rel)
{
	if (rel->rd_rel->relkind != RELKIND_RELATION &&
		rel->rd_rel->relkind != RELKIND_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table or index",
						RelationGetRelationName(rel)),
				 errOmitLocation(true)));

	/* Permissions checks */
	if (!pg_class_ownercheck(RelationGetRelid(rel), GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
					   RelationGetRelationName(rel));

	if (!allowSystemTableModsDDL && IsSystemRelation(rel))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied: \"%s\" is a system catalog",
						RelationGetRelationName(rel))));
}

/*
 * ATSimpleRecursion
 *
 * Simple table recursion sufficient for most ALTER TABLE operations.
 * All direct and indirect children are processed in an unspecified order.
 * Note that if a child inherits from the original table via multiple
 * inheritance paths, it will be visited just once.
 */
static void
ATSimpleRecursion(List **wqueue, Relation rel,
				  AlterTableCmd *cmd, bool recurse)
{
	/*
	 * Propagate to children if desired.  Non-table relations never have
	 * children, so no need to search in that case.
	 */
	if (recurse && rel->rd_rel->relkind == RELKIND_RELATION)
	{
		Oid			relid = RelationGetRelid(rel);
		ListCell   *child;
		List	   *children;

		/* this routine is actually in the planner */
		children = find_all_inheritors(relid);

		/*
		 * find_all_inheritors does the recursive search of the inheritance
		 * hierarchy, so all we have to do is process all of the relids in the
		 * list that it returns.
		 */
		foreach(child, children)
		{
			Oid			childrelid = lfirst_oid(child);
			Relation	childrel;

			if (childrelid == relid)
				continue;
			childrel = relation_open(childrelid, AccessExclusiveLock);
			CheckTableNotInUse(childrel, "ALTER TABLE");
			ATPrepCmd(wqueue, childrel, cmd, false, true);
			relation_close(childrel, NoLock);
		}
	}
}

/*
 * ATOneLevelRecursion
 *
 * Here, we visit only direct inheritance children.  It is expected that
 * the command's prep routine will recurse again to find indirect children.
 * When using this technique, a multiply-inheriting child will be visited
 * multiple times.
 */
/*
static void
ATOneLevelRecursion(List **wqueue, Relation rel,
					AlterTableCmd *cmd)
{
	Oid			relid = RelationGetRelid(rel);
	ListCell   *child;
	List	   *children;

	* this routine is actually in the planner *
	children = find_inheritance_children(relid);

	foreach(child, children)
	{
		Oid			childrelid = lfirst_oid(child);
		Relation	childrel;

		childrel = relation_open(childrelid, AccessExclusiveLock);
		CheckTableNotInUse(childrel, "ALTER TABLE");
		ATPrepCmd(wqueue, childrel, cmd, true, true);
		relation_close(childrel, NoLock);
	}
}
*/

/*
 * find_composite_type_dependencies
 *
 * Check to see if a composite type is being used as a column in some
 * other table (possibly nested several levels deep in composite types!).
 * Eventually, we'd like to propagate the check or rewrite operation
 * into other such tables, but for now, just error out if we find any.
 *
 * Caller should provide either a table name or a type name (not both) to
 * report in the error message, if any.
 *
 * We assume that functions and views depending on the type are not reasons
 * to reject the ALTER.  (How safe is this really?)
 */
void
find_composite_type_dependencies(Oid typeOid,
								 const char *origTblName,
								 const char *origTypeName)
{
	cqContext  *pcqCtx;
	HeapTuple	depTup;

	/*
	 * We scan pg_depend to find those things that depend on the rowtype. (We
	 * assume we can ignore refobjsubid for a rowtype.)
	 */

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_depend "
				" WHERE refclassid = :1 "
				" AND refobjid = :2 ",
				ObjectIdGetDatum(TypeRelationId),
				ObjectIdGetDatum(typeOid)));

	while (HeapTupleIsValid(depTup = caql_getnext(pcqCtx)))
	{
		Form_pg_depend pg_depend = (Form_pg_depend) GETSTRUCT(depTup);
		Relation	rel;
		Form_pg_attribute att;

		/* Ignore dependees that aren't user columns of relations */
		/* (we assume system columns are never of rowtypes) */
		if (pg_depend->classid != RelationRelationId ||
			pg_depend->objsubid <= 0)
			continue;

		rel = relation_open(pg_depend->objid, AccessShareLock);
		att = rel->rd_att->attrs[pg_depend->objsubid - 1];

		if (rel->rd_rel->relkind == RELKIND_RELATION)
		{
			if (origTblName)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot alter table \"%s\" because column \"%s\".\"%s\" uses its rowtype",
								origTblName,
								RelationGetRelationName(rel),
								NameStr(att->attname)),
										   errOmitLocation(true)));
			else
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot alter type \"%s\" because column \"%s\".\"%s\" uses it",
								origTypeName,
								RelationGetRelationName(rel),
								NameStr(att->attname)),
										   errOmitLocation(true)));
		}
		else if (OidIsValid(rel->rd_rel->reltype))
		{
			/*
			 * A view or composite type itself isn't a problem, but we must
			 * recursively check for indirect dependencies via its rowtype.
			 */
			find_composite_type_dependencies(rel->rd_rel->reltype,
											 origTblName, origTypeName);
		}

		relation_close(rel, AccessShareLock);
	}
	caql_endscan(pcqCtx);
}


/*
 * ALTER TABLE ADD COLUMN
 *
 * Adds an additional attribute to a relation making the assumption that
 * CHECK, NOT NULL, and FOREIGN KEY constraints will be removed from the
 * AT_AddColumn AlterTableCmd by analyze.c and added as independent
 * AlterTableCmd's.
 */
static void
ATPrepAddColumn(Relation rel, bool recurse, AlterTableCmd *cmd)
{
	/* 
	 * If there's an encoding clause, this better be an append only
	 * column oriented table.
	 */
	ColumnDef *def = (ColumnDef *)cmd->def;
	if (def->encoding)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ENCODING clause supplied for table that is not supported")));

	if (def->encoding)
		def->encoding = transformStorageEncodingClause(def->encoding);

	/*
	 * If we are told not to recurse, there had better not be any child
	 * tables; else the addition would put them out of step.
	 */
	if (!recurse && find_inheritance_children(RelationGetRelid(rel)) != NIL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("column must be added to child tables too")));
	}
	/*
	 * We are the master and the table has child(ren):
	 * 		internally create and execute new AlterTableStmt(s) on child(ren)
	 * 		before dispatching the original AlterTableStmt
	 * This is to ensure that pg_constraint oid is consistent across segments for
	 * 		ALTER TABLE ... ADD COLUMN ... CHECK ...
	 */
	else if (Gp_role == GP_ROLE_DISPATCH)
	{
		/*
		 * Recurse to add the column to child classes.
		 *
		 * We must recurse one level at a time, so that multiply-inheriting
		 * children are visited the right number of times and end up with the
		 * right attinhcount.
		 */
		List		*children;
		ListCell	*lchild;

		children = find_inheritance_children(RelationGetRelid(rel));
		DestReceiver *dest = None_Receiver;
		foreach(lchild, children)
		{
			Oid 			childrelid = lfirst_oid(lchild);
			Relation 		childrel;

			RangeVar 		*rv;
			AlterTableCmd 	*atc;
			AlterTableStmt 	*ats;

			if (childrelid == RelationGetRelid(rel))
				continue;

			childrel = heap_open(childrelid, AccessShareLock);
			CheckTableNotInUse(childrel, "ALTER TABLE");

			/* Recurse to child */
			atc = copyObject(cmd);
			atc->subtype = AT_AddColumnRecurse;

			/* Child should see column as singly inherited */
			((ColumnDef *) atc->def)->inhcount = 1;
			((ColumnDef *) atc->def)->is_local = false;

			rv = makeRangeVar(NULL /*catalogname*/, get_namespace_name(RelationGetNamespace(childrel)),
							  get_rel_name(childrelid), -1);

			ats = makeNode(AlterTableStmt);
			ats->relation = rv;
			ats->cmds = list_make1(atc);
			ats->relkind = OBJECT_TABLE;

			heap_close(childrel, AccessShareLock);

			ProcessUtility((Node *)ats,
							synthetic_sql,
							NULL,
							false, /* not top level */
							dest,
							NULL);
		}
	}
}

void
ATAddColumn(Relation rel, ColumnDef *colDef)
{
	ATExecAddColumn((AlteredTableInfo*)NULL, rel, colDef);
}

static void
ATExecAddColumn(AlteredTableInfo *tab, Relation rel,
				ColumnDef *colDef)
{
	Oid			myrelid = RelationGetRelid(rel);
	Relation	pgclass,
				attrdesc;
	HeapTuple	reltup;
	HeapTuple	attributeTuple;
	Form_pg_attribute attribute;
	FormData_pg_attribute attributeD;
	int			i;
	int			minattnum,
				maxatts;
	HeapTuple	typeTuple;
	Oid			typeOid;
	Form_pg_type tform;
	Expr	   *defval;
	cqContext	cqc;
	cqContext	cqc2;
	cqContext  *pclaCtx;
	cqContext  *patCtx;

	/*
	 * Append Only tables don't support ADD COLUMN when no default value is
	 * specified. This is because this scenario makes a schema change that
	 * memtuples can't deal with yet. If a default value is specified than it
	 * it ok because the whole table will get re-written in phase 3. Let's
	 * catch this now before doing any real work. 
	 *
	 * coldDef->raw_default is null in 2 cases - when there is no DEFAULT 
	 * specified and when DEFAULT NULL is specified. We only want to block
	 * the case when there is no DEFAULT specified. Hence we also check
	 * colDef->default_is_null as it records the fact that DEFAULT NULL was 
	 * specified. 
 	*/
	 
	if ((RelationIsAoRows(rel) || RelationIsParquet(rel)) &&
		(!colDef->default_is_null && !colDef->raw_default))
	{
		ereport(ERROR,
				(errcode(ERRCODE_GP_FEATURE_NOT_YET),
				 errmsg("ADD COLUMN with no default value in append-only tables"
						" is not yet supported."),
								   errOmitLocation(true)
                ));
	}

	attrdesc = heap_open(AttributeRelationId, RowExclusiveLock);

	patCtx = caql_beginscan( 
			caql_addrel(cqclr(&cqc2), attrdesc),
			cql("INSERT INTO pg_attribute ",
				NULL));

	/*
	 * Are we adding the column to a recursion child?  If so, check whether to
	 * merge with an existing definition for the column.
	 */
	if (colDef->inhcount > 0)
	{
		HeapTuple	tuple;
		cqContext	cqc3;
		cqContext  *patCtx2;

		patCtx2 = caql_addrel(cqclr(&cqc3), attrdesc);

		/* Does child already have a column by this name? */
		tuple = caql_getattname(patCtx2, myrelid, colDef->colname);
		if (HeapTupleIsValid(tuple))
		{
			Form_pg_attribute childatt = (Form_pg_attribute) GETSTRUCT(tuple);

			/* Okay if child matches by type */
			if (typenameTypeId(NULL, colDef->typname) != childatt->atttypid ||
				colDef->typname->typmod != childatt->atttypmod)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("child table \"%s\" has different type for column \"%s\"",
							RelationGetRelationName(rel), colDef->colname),
									   errOmitLocation(true)));

			/* Bump the existing child att's inhcount */
			childatt->attinhcount++;
			caql_update_current(patCtx2, tuple);
			/* and Update indexes (implicit) */

			heap_freetuple(tuple);

			/* Inform the user about the merge */
			if (Gp_role == GP_ROLE_EXECUTE)
			{
				ereport(DEBUG1,
			  (errmsg("merging definition of column \"%s\" for child \"%s\"",
					  colDef->colname, RelationGetRelationName(rel)),
							   errOmitLocation(true)));

			}
			else
			ereport(NOTICE,
			  (errmsg("merging definition of column \"%s\" for child \"%s\"",
					  colDef->colname, RelationGetRelationName(rel)),
							   errOmitLocation(true)));

			heap_close(attrdesc, RowExclusiveLock);
			return;
		}
	}

	pgclass = heap_open(RelationRelationId, RowExclusiveLock);

	pclaCtx = caql_addrel(cqclr(&cqc), pgclass);

	reltup = caql_getfirst(
			pclaCtx,
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(myrelid)));

	if (!HeapTupleIsValid(reltup))
		elog(ERROR, "cache lookup failed for relation %u", myrelid);

	/*
	 * this test is deliberately not attisdropped-aware, since if one tries to
	 * add a column matching a dropped column name, it's gonna fail anyway.
	 *
	 * For GPDB upgrade, we know that some columns don't exist, so don't go to
	 * the waste of looking them up. This could be a major cost if we're adding
	 * 20,000 columns for AO, for example.
	 */
	if (!(!IsUnderPostmaster && (rel->rd_rel->relkind == RELKIND_AOSEGMENTS ||
								 rel->rd_rel->relkind == RELKIND_AOBLOCKDIR)))
	{
		cqContext	cqc3;

		if (caql_getcount(
					caql_addrel(cqclr(&cqc3), attrdesc),
					cql("SELECT COUNT(*) FROM pg_attribute "
						" WHERE attrelid = :1 "
						" AND attname = :2 ",
						ObjectIdGetDatum(myrelid),
						PointerGetDatum(colDef->colname))))
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_COLUMN),
					 errmsg("column \"%s\" of relation \"%s\" already exists",
							colDef->colname, RelationGetRelationName(rel)),
					 errOmitLocation(true)));
		}
	}
	minattnum = ((Form_pg_class) GETSTRUCT(reltup))->relnatts;
	maxatts = minattnum + 1;
	if (maxatts > MaxHeapAttributeNumber)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("tables can have at most %d columns",
						MaxHeapAttributeNumber)));
	i = minattnum + 1;

	typeTuple = typenameType(NULL, colDef->typname);
	tform = (Form_pg_type) GETSTRUCT(typeTuple);
	typeOid = HeapTupleGetOid(typeTuple);

	/* make sure datatype is legal for a column */
	CheckAttributeType(colDef->colname, typeOid);

	attributeTuple = heap_addheader(Natts_pg_attribute,
									false,
									ATTRIBUTE_TUPLE_SIZE,
									(void *) &attributeD);

	attribute = (Form_pg_attribute) GETSTRUCT(attributeTuple);

	attribute->attrelid = myrelid;
	namestrcpy(&(attribute->attname), colDef->colname);
	attribute->atttypid = typeOid;
	attribute->attstattarget = -1;
	attribute->attlen = tform->typlen;
	attribute->attcacheoff = -1;
	attribute->atttypmod = colDef->typname->typmod;
	attribute->attnum = i;
	attribute->attbyval = tform->typbyval;
	attribute->attndims = list_length(colDef->typname->arrayBounds);
	attribute->attstorage = tform->typstorage;
	attribute->attalign = tform->typalign;
	attribute->attnotnull = colDef->is_not_null;
	attribute->atthasdef = false;
	attribute->attisdropped = false;
	attribute->attislocal = colDef->is_local;
	attribute->attinhcount = colDef->inhcount;

	ReleaseType(typeTuple);

	caql_insert(patCtx, attributeTuple); /* implicit update of index as well */

	caql_endscan(patCtx);
	heap_close(attrdesc, RowExclusiveLock);

	/*
	 * Update number of attributes in pg_class tuple
	 */
	((Form_pg_class) GETSTRUCT(reltup))->relnatts = maxatts;

	caql_update_current(pclaCtx, reltup);/* implicit update of index as well */

	heap_freetuple(reltup);

	heap_close(pgclass, RowExclusiveLock);

	/* Make the attribute's catalog entry visible */
	CommandCounterIncrement();

	/*
	 * Store the DEFAULT, if any, in the catalogs
	 */
	if (colDef->raw_default)
	{
		RawColumnDefault *rawEnt;

		rawEnt = (RawColumnDefault *) palloc(sizeof(RawColumnDefault));
		rawEnt->attnum = attribute->attnum;
		rawEnt->raw_default = copyObject(colDef->raw_default);

		/*
		 * This function is intended for CREATE TABLE, so it processes a
		 * _list_ of defaults, but we just do one.
		 */
		AddRelationConstraints(rel, list_make1(rawEnt), NIL);

		/* Make the additional catalog changes visible */
		CommandCounterIncrement();
	}

	/*
	 * Tell Phase 3 to fill in the default expression, if there is one.
	 *
	 * If there is no default, Phase 3 doesn't have to do anything, because
	 * that effectively means that the default is NULL.  The heap tuple access
	 * routines always check for attnum > # of attributes in tuple, and return
	 * NULL if so, so without any modification of the tuple data we will get
	 * the effect of NULL values in the new column.
	 *
	 * An exception occurs when the new column is of a domain type: the domain
	 * might have a NOT NULL constraint, or a check constraint that indirectly
	 * rejects nulls.  If there are any domain constraints then we construct
	 * an explicit NULL default value that will be passed through
	 * CoerceToDomain processing.  (This is a tad inefficient, since it causes
	 * rewriting the table which we really don't have to do, but the present
	 * design of domain processing doesn't offer any simple way of checking
	 * the constraints more directly.)
	 *
	 * Note: we use build_column_default, and not just the cooked default
	 * returned by AddRelationConstraints, so that the right thing happens
	 * when a datatype's default applies.
	 */
	defval = (Expr *) build_column_default(rel, attribute->attnum);

	if (!defval && GetDomainConstraints(typeOid) != NIL)
	{
		Oid			basetype = getBaseType(typeOid);

		defval = (Expr *) makeNullConst(basetype, -1);
		defval = (Expr *) coerce_to_target_type(NULL, 
												(Node *) defval,
												basetype,
												typeOid,
												colDef->typname->typmod,
												COERCION_ASSIGNMENT,
												COERCE_IMPLICIT_CAST,
												-1);
		if (defval == NULL)		/* should not happen */
			elog(ERROR, "failed to coerce base type to domain");
	}

	/*
	 * MPP-14367/MPP-19664 - Handling of default NULL for AO/CO tables. Currently
	 * memtuples cannot deal with the scenario where the number of
  	 * attributes in the tuple data don't match the attnum. We will generate
	 * an explicit NULL default value and force a rewrite of the table below.
	 * Note: This is inefficient and there is already another JIRA MPP-5419 open
	 * to track adding columns without default values to AO tables efficiently.
	 * When that JIRA is fixed, the following piece of code may be removed
	 */
	if (!defval && (RelationIsAoRows(rel))
			&& colDef->default_is_null)
	{
		defval = (Expr *) makeNullConst(typeOid, -1);
	}

	if (defval)
	{
		NewColumnValue *newval;

		newval = (NewColumnValue *) palloc0(sizeof(NewColumnValue));
		newval->attnum = attribute->attnum;
		newval->expr = defval;

		/* tab is null if this is called by "create or replace view"
		 * which can't have any default value.
		 */
		Assert(tab);
		tab->newvals = lappend(tab->newvals, newval);
	}

	/*
	 * If the new column is NOT NULL, tell Phase 3 it needs to test that.
	 * Also, "create or replace view" won't have constraint on the column.
	 */
	Assert(!colDef->is_not_null || tab);
	if (tab)
		tab->new_notnull |= colDef->is_not_null;

	/*
	 * Add needed dependency entries for the new column.
	 */
	add_column_datatype_dependency(myrelid, i, attribute->atttypid);

	if (colDef->encoding)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ENCODING clause not supported")));
						
	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "ADD COLUMN"
				);

}

/*
 * Install a column's dependency on its datatype.
 */
static void
add_column_datatype_dependency(Oid relid, int32 attnum, Oid typid)
{
	ObjectAddress myself,
				referenced;

	myself.classId = RelationRelationId;
	myself.objectId = relid;
	myself.objectSubId = attnum;
	referenced.classId = TypeRelationId;
	referenced.objectId = typid;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
}

/*
 * ALTER TABLE ALTER COLUMN DROP NOT NULL
 */
static void
ATExecDropNotNull(Relation rel, const char *colName)
{
	HeapTuple	tuple;
	AttrNumber	attnum;
	Relation	attr_rel;
	List	   *indexoidlist;
	ListCell   *indexoidscan;
	cqContext	cqc;
	cqContext  *pcqCtx;

	/*
	 * lookup the attribute
	 */
	attr_rel = heap_open(AttributeRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), attr_rel);

	tuple = caql_getattname(pcqCtx, RelationGetRelid(rel), colName);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel)),
				 errOmitLocation(true)));

	attnum = ((Form_pg_attribute) GETSTRUCT(tuple))->attnum;

	/* Prevent them from altering a system attribute */
	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system column \"%s\"",
						colName)));

	/*
	 * Check that the attribute is not in a primary key
	 */

	/* Loop over all indexes on the relation */
	indexoidlist = RelationGetIndexList(rel);

	foreach(indexoidscan, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(indexoidscan);
		HeapTuple	indexTuple;
		Form_pg_index indexStruct;
		int			i;
		cqContext  *pidxCtx;

		pidxCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_index "
					" WHERE indexrelid = :1 ",
					ObjectIdGetDatum(indexoid)));

		indexTuple = caql_getnext(pidxCtx);

		if (!HeapTupleIsValid(indexTuple))
			elog(ERROR, "cache lookup failed for index %u", indexoid);
		indexStruct = (Form_pg_index) GETSTRUCT(indexTuple);

		/* If the index is not a primary key, skip the check */
		if (indexStruct->indisprimary)
		{
			/*
			 * Loop over each attribute in the primary key and see if it
			 * matches the to-be-altered attribute
			 */
			for (i = 0; i < indexStruct->indnatts; i++)
			{
				if (indexStruct->indkey.values[i] == attnum)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							 errmsg("column \"%s\" is in a primary key",
									colName),
							 errOmitLocation(true)));
			}
		}

		caql_endscan(pidxCtx);
	}

	list_free(indexoidlist);

	/*
	 * Okay, actually perform the catalog change ... if needed
	 */
	if (((Form_pg_attribute) GETSTRUCT(tuple))->attnotnull)
	{
		((Form_pg_attribute) GETSTRUCT(tuple))->attnotnull = FALSE;

		caql_update_current(pcqCtx, tuple);
		/* and Update indexes (implicit) */
	}

	heap_close(attr_rel, RowExclusiveLock);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "ALTER COLUMN DROP NOT NULL"
				);

}

/*
 * ALTER TABLE ALTER COLUMN SET NOT NULL
 */
static void
ATExecSetNotNull(AlteredTableInfo *tab, Relation rel,
				 const char *colName)
{
	HeapTuple	tuple;
	AttrNumber	attnum;
	Relation	attr_rel;
	cqContext	cqc;
	cqContext  *pcqCtx;

	/*
	 * lookup the attribute
	 */
	attr_rel = heap_open(AttributeRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), attr_rel);

	tuple = caql_getattname(pcqCtx, RelationGetRelid(rel), colName);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel)),
			     errOmitLocation(true)));

	attnum = ((Form_pg_attribute) GETSTRUCT(tuple))->attnum;

	/* Prevent them from altering a system attribute */
	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system column \"%s\"",
						colName)));

	/*
	 * Okay, actually perform the catalog change ... if needed
	 */
	if (!((Form_pg_attribute) GETSTRUCT(tuple))->attnotnull)
	{
		((Form_pg_attribute) GETSTRUCT(tuple))->attnotnull = TRUE;

		caql_update_current(pcqCtx, tuple);
		/* and Update indexes (implicit) */

		/* Tell Phase 3 it needs to test the constraint */
		tab->new_notnull = true;
	}

	heap_close(attr_rel, RowExclusiveLock);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "ALTER COLUMN SET NOT NULL"
				);

}

/*
 * ALTER TABLE ALTER COLUMN SET/DROP DEFAULT
 */
static void
ATExecColumnDefault(Relation rel, const char *colName,
					Node *newDefault)
{
	AttrNumber	attnum;

	/*
	 * get the number of the attribute
	 */
	attnum = get_attnum(RelationGetRelid(rel), colName);
	if (attnum == InvalidAttrNumber)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel)),
				 errOmitLocation(true)));

	/* Prevent them from altering a system attribute */
	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system column \"%s\"",
						colName)));

	/*
	 * Remove any old default for the column.  We use RESTRICT here for
	 * safety, but at present we do not expect anything to depend on the
	 * default.
	 */
	RemoveAttrDefault(RelationGetRelid(rel), attnum, DROP_RESTRICT, false);

	if (newDefault)
	{
		/* SET DEFAULT */
		RawColumnDefault *rawEnt;

		rawEnt = (RawColumnDefault *) palloc(sizeof(RawColumnDefault));
		rawEnt->attnum = attnum;
		rawEnt->raw_default = newDefault;

		/*
		 * This function is intended for CREATE TABLE, so it processes a
		 * _list_ of defaults, but we just do one.
		 */
		AddRelationConstraints(rel, list_make1(rawEnt), NIL);
	}

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "ALTER COLUMN DEFAULT"
				);


}

/*
 * ALTER TABLE ALTER COLUMN SET STATISTICS
 */
static void
ATPrepSetStatistics(Relation rel, const char *colName, Node *flagValue)
{
	/*
	 * We do our own permission checking because (a) we want to allow SET
	 * STATISTICS on indexes (for expressional index columns), and (b) we want
	 * to allow SET STATISTICS on system catalogs without requiring
	 * allowSystemTableModsDDL to be turned on.
	 */
	if (rel->rd_rel->relkind != RELKIND_RELATION &&
		rel->rd_rel->relkind != RELKIND_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table or index",
						RelationGetRelationName(rel)),
				 errOmitLocation(true)));

	/* Permissions checks */
	if (!pg_class_ownercheck(RelationGetRelid(rel), GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
					   RelationGetRelationName(rel));
}

static void
ATExecSetStatistics(Relation rel, const char *colName, Node *newValue)
{
	int			newtarget;
	Relation	attrelation;
	HeapTuple	tuple;
	Form_pg_attribute attrtuple;
	cqContext	cqc;
	cqContext  *pcqCtx;

	Assert(IsA(newValue, Integer));
	newtarget = intVal(newValue);

	/*
	 * Limit target to a sane range
	 */
	if (newtarget < -1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("statistics target %d is too low",
						newtarget)));
	}
	else if (newtarget > 1000)
	{
		newtarget = 1000;
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("lowering statistics target to %d",
						newtarget)));
	}

	attrelation = heap_open(AttributeRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), attrelation);

	tuple = caql_getattname(pcqCtx, RelationGetRelid(rel), colName);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel))));
	attrtuple = (Form_pg_attribute) GETSTRUCT(tuple);

	if (attrtuple->attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system column \"%s\"",
						colName)));

	attrtuple->attstattarget = newtarget;

	caql_update_current(pcqCtx, tuple); /* implicit update of index as well */

	heap_freetuple(tuple);

	heap_close(attrelation, RowExclusiveLock);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "ALTER COLUMN SET STATISTICS"
				);


}

/*
 * ALTER TABLE ALTER COLUMN SET STORAGE
 */
static void
ATExecSetStorage(Relation rel, const char *colName, Node *newValue)
{
	char	   *storagemode;
	char		newstorage;
	Relation	attrelation;
	HeapTuple	tuple;
	Form_pg_attribute attrtuple;
	cqContext	cqc;
	cqContext  *pcqCtx;

	Assert(IsA(newValue, String));
	storagemode = strVal(newValue);

	if (pg_strcasecmp(storagemode, "plain") == 0)
		newstorage = 'p';
	else if (pg_strcasecmp(storagemode, "external") == 0)
		newstorage = 'e';
	else if (pg_strcasecmp(storagemode, "extended") == 0)
		newstorage = 'x';
	else if (pg_strcasecmp(storagemode, "main") == 0)
		newstorage = 'm';
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid storage type \"%s\"",
						storagemode)));
		newstorage = 0;			/* keep compiler quiet */
	}

	attrelation = heap_open(AttributeRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), attrelation);

	tuple = caql_getattname(pcqCtx, RelationGetRelid(rel), colName);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel)),
				 errOmitLocation(true)));
	attrtuple = (Form_pg_attribute) GETSTRUCT(tuple);

	if (attrtuple->attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system column \"%s\"",
						colName)));

	/*
	 * safety check: do not allow toasted storage modes unless column datatype
	 * is TOAST-aware.
	 */
	if (newstorage == 'p' || TypeIsToastable(attrtuple->atttypid))
		attrtuple->attstorage = newstorage;
	else
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("column data type %s can only have storage PLAIN",
						format_type_be(attrtuple->atttypid))));

	caql_update_current(pcqCtx, tuple); /* implicit update of index as well */

	heap_freetuple(tuple);

	heap_close(attrelation, RowExclusiveLock);
	
	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "ALTER COLUMN SET STORAGE"
				);
	
}


/*
 * ALTER TABLE DROP COLUMN
 *
 * DROP COLUMN cannot use the normal ALTER TABLE recursion mechanism,
 * because we have to decide at runtime whether to recurse or not depending
 * on whether attinhcount goes to zero or not.	(We can't check this in a
 * static pre-pass because it won't handle multiple inheritance situations
 * correctly.)
 */
static void
ATExecDropColumn(List **wqueue, Relation rel, const char *colName,
				 DropBehavior behavior,
				 bool recurse, bool recursing)
{
	HeapTuple	tuple;
	Form_pg_attribute targetatt;
	AttrNumber	attnum;
	List	   *children;
	ObjectAddress object;
	GpPolicy  *policy;
	PartitionNode *pn;
	cqContext	*pcqCtx;

	/* At top level, permission check was done in ATPrepCmd, else do it */
	if (recursing)
		ATSimplePermissions(rel, false);

	/*
	 * get the number of the attribute
	 */
	pcqCtx = caql_getattname_scan(NULL, RelationGetRelid(rel), colName);

	tuple = caql_get_current(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel))));
	targetatt = (Form_pg_attribute) GETSTRUCT(tuple);

	attnum = targetatt->attnum;

	/* Can't drop a system attribute, except OID */
	if (attnum <= 0 && attnum != ObjectIdAttributeNumber)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot drop system column \"%s\"",
						colName),
								   errOmitLocation(true)));

	/* Don't drop inherited columns */
	if (targetatt->attinhcount > 0 && !recursing)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot drop inherited column \"%s\"",
						colName),
								   errOmitLocation(true)));

	/* better not be a column we partition on */
	pn = RelationBuildPartitionDesc(rel, false);
	if (pn)
	{
		List *patts = get_partition_attrs(pn);

		if (list_member_int(patts, attnum))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot drop partitioning column \"%s\"",
							colName),
									   errOmitLocation(true)));

		/*
		 * Remove any partition encoding entry
		 */
		RemovePartitionEncodingByRelidAttribute(RelationGetRelid(rel), attnum);
	}

	caql_endscan(pcqCtx);

	policy = rel->rd_cdbpolicy;
	if (policy != NULL && policy->ptype == POLICYTYPE_PARTITIONED)
	{
		int			ia = 0;

		for (ia = 0; ia < policy->nattrs; ia++)
		{
			if (attnum == policy->attrs[ia])
			{
				policy->nattrs = 0;
				rel->rd_cdbpolicy = GpPolicyCopy(GetMemoryChunkContext(rel), policy);
				GpPolicyReplace(RelationGetRelid(rel), policy);
				if (Gp_role != GP_ROLE_EXECUTE)
				    ereport(NOTICE,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("Dropping a column that is part of the "
								"distribution policy forces a "
								"NULL distribution policy"),
										   errOmitLocation(true)));
			}
		}
	}

	/*
	 * Propagate to children as appropriate.  Unlike most other ALTER
	 * routines, we have to do this one level of recursion at a time; we can't
	 * use find_all_inheritors to do it in one pass.
	 */
	children = find_inheritance_children(RelationGetRelid(rel));

	if (children)
	{
		Relation	attr_rel;
		ListCell   *child;

		attr_rel = heap_open(AttributeRelationId, RowExclusiveLock);
		foreach(child, children)
		{
			Oid			childrelid = lfirst_oid(child);
			Relation	childrel;
			Form_pg_attribute childatt;
			cqContext	cqc;
			cqContext  *attcqCtx;

			childrel = heap_open(childrelid, AccessExclusiveLock);
			CheckTableNotInUse(childrel, "ALTER TABLE");

			attcqCtx = caql_addrel(cqclr(&cqc), attr_rel);

			tuple = caql_getattname(attcqCtx, childrelid, colName);

			if (!HeapTupleIsValid(tuple))		/* shouldn't happen */
				elog(ERROR, "cache lookup failed for attribute \"%s\" of relation %u",
					 colName, childrelid);
			childatt = (Form_pg_attribute) GETSTRUCT(tuple);

			if (childatt->attinhcount <= 0)		/* shouldn't happen */
				elog(ERROR, "relation %u has non-inherited attribute \"%s\"",
					 childrelid, colName);

			if (recurse)
			{
				/*
				 * If the child column has other definition sources,
				 * just decrement its inheritance count;
				 * if not or if this is part of a partition
				 * configuration, recurse to delete it.
				 */
				if ((childatt->attinhcount == 1 && !childatt->attislocal) ||
					pn)
				{
					/* Time to delete this child column, too */
					ATExecDropColumn(wqueue, childrel, colName, behavior, true, true);
				}
				else
				{
					/* Child column must survive my deletion */
					childatt->attinhcount--;

					caql_update_current(attcqCtx, tuple);
					/* and Update indexes (implicit) */

					/* Make update visible */
					CommandCounterIncrement();
				}
			}
			else
			{
				/*
				 * If we were told to drop ONLY in this table (no recursion),
				 * we need to mark the inheritors' attribute as locally
				 * defined rather than inherited.
				 */
				childatt->attinhcount--;
				childatt->attislocal = true;

				caql_update_current(attcqCtx, tuple);
				/* and Update indexes (implicit) */

				/* Make update visible */
				CommandCounterIncrement();
			}

			heap_freetuple(tuple);

			heap_close(childrel, NoLock);
		}
		heap_close(attr_rel, RowExclusiveLock);
	}

	/*
	 * Perform the actual column deletion
	 */
	object.classId = RelationRelationId;
	object.objectId = RelationGetRelid(rel);
	object.objectSubId = attnum;

	performDeletion(&object, behavior);

	/*
	 * If we dropped the OID column, must adjust pg_class.relhasoids and
	 * tell Phase 3 to physically get rid of the column.
	 */
	if (attnum == ObjectIdAttributeNumber)
	{
		Form_pg_class tuple_class;
		AlteredTableInfo *tab;
		cqContext  *relcqCtx;

		relcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_class "
					" WHERE oid = :1 "
					" FOR UPDATE ",
					ObjectIdGetDatum(RelationGetRelid(rel))));

		tuple = caql_getnext(relcqCtx);

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for relation %u",
				 RelationGetRelid(rel));

		/* make a copy to update */
		tuple = heap_copytuple(tuple);

		tuple_class = (Form_pg_class) GETSTRUCT(tuple);

		tuple_class->relhasoids = false;

		caql_update_current(relcqCtx, tuple);
		/* and Update indexes (implicit) */

		caql_endscan(relcqCtx);
		
		/* Find or create work queue entry for this table */
		tab = ATGetQueueEntry(wqueue, rel);
		
		/* Tell Phase 3 to physically remove the OID column */
		tab->new_dropoids = true;
	}

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "DROP COLUMN"
				);

}

/*
 * ALTER TABLE ADD INDEX
 *
 * There is no such command in the grammar, but the parser converts UNIQUE
 * and PRIMARY KEY constraints into AT_AddIndex subcommands.  This lets us
 * schedule creation of the index at the appropriate time during ALTER.
 */
static void
ATExecAddIndex(AlteredTableInfo *tab, Relation rel,
			   IndexStmt *stmt, bool is_rebuild, bool part_expanded)
{
	bool		check_rights;
	bool		skip_build;
	bool		quiet;

	Assert(IsA(stmt, IndexStmt));

	/* The index should already be built if we are a QE */
	if (Gp_role == GP_ROLE_EXECUTE)
		return;

	/* suppress schema rights check when rebuilding existing index */
	check_rights = !is_rebuild;
	/* skip index build if phase 3 will have to rewrite table anyway */
	skip_build = (tab->newvals != NIL);
	/* suppress notices when rebuilding existing index */
	quiet = is_rebuild;

	DefineIndex(RelationGetRelid(rel), /* relation */
				stmt->idxname,	/* index name */
				InvalidOid,		/* no predefined OID */
				stmt->accessMethod,		/* am name */
				stmt->tableSpace,
				stmt->indexParams,		/* parameters */
				(Expr *) stmt->whereClause,
				stmt->rangetable,
				stmt->options,
				stmt->unique,
				stmt->primary,
				stmt->isconstraint,
				true,			/* is_alter_table */
				check_rights,
				skip_build,
				quiet,
				false,
				part_expanded,
				stmt);

	/* 
	 * If we're the master and this is a partitioned table, cascade index
	 * creation for all children.
	 */
	if (Gp_role == GP_ROLE_DISPATCH &&
		rel_is_partitioned(RelationGetRelid(rel)))
	{
		List *children = find_all_inheritors(RelationGetRelid(rel));
		ListCell *lc;
		bool prefix_match = false;
		char *pname = RelationGetRelationName(rel);
		char *iname = stmt->idxname ? stmt->idxname : "idx";
		DestReceiver *dest = None_Receiver;

		/* is the parent relation name a prefix of the index name? */
		if (strlen(iname) > strlen(pname) &&
			strncmp(pname, iname, strlen(pname)) == 0)
			prefix_match = true;

		foreach(lc, children)
		{
			Oid relid = lfirst_oid(lc);
			Relation crel;
			IndexStmt *istmt;
			AlterTableStmt *ats;
			AlterTableCmd *atc;
			RangeVar *rv;
			char *idxname;

			if (relid == RelationGetRelid(rel))
				continue;

 			crel = heap_open(relid, AccessShareLock);
			istmt = copyObject(stmt);
			istmt->idxOids = NIL;
			istmt->is_part_child = true;
			istmt->constrOid = InvalidOid;

			ats = makeNode(AlterTableStmt);
			atc = makeNode(AlterTableCmd);

			rv = makeRangeVar(NULL /*catalogname*/, get_namespace_name(RelationGetNamespace(rel)),
							  get_rel_name(relid), -1);
			istmt->relation = rv;
			if (prefix_match)
			{
				/* 
				 * If the next character in the index name is '_', absorb
				 * it, as ChooseRelationName() will add another.
				 */
				int off = 0;
				if (iname[strlen(pname)] == '_')
					off = 1;
				idxname = ChooseRelationName(RelationGetRelationName(crel),
											 NULL,
											 (iname + strlen(pname) + off),
											 RelationGetNamespace(crel),
											 NULL);
			}
			else
				idxname = ChooseRelationName(RelationGetRelationName(crel),
											 NULL,
											 iname,
											 RelationGetNamespace(crel),
											 NULL);
			istmt->idxname = idxname;
			atc->subtype = AT_AddIndex;
			atc->def = (Node *)istmt;
			atc->part_expanded = true;

			ats->relation = copyObject(istmt->relation);
			ats->cmds = list_make1(atc);
			ats->relkind = OBJECT_TABLE;

			heap_close(crel, AccessShareLock); /* already locked master */

			ProcessUtility((Node *)ats, 
						   synthetic_sql,
						   NULL, 
						   false, /* not top level */
						   dest, 
						   NULL);
		}
	}

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "ADD INDEX"
				);

}


static void
ATExecAddConstraint(AlteredTableInfo *tab, Relation rel, Node *newConstraint, bool recurse)
{
	switch (nodeTag(newConstraint))
	{
		case T_Constraint:
			{
				Constraint *constr = (Constraint *) newConstraint;
				/*
				 * Currently, we only expect to see CONSTR_CHECK nodes
				 * arriving here (see the preprocessing done in
				 * parser/analyze.c).  Use a switch anyway to make it easier
				 * to add more code later.
				 */
				switch (constr->contype)
				{
					case CONSTR_CHECK:
						{
							ATAddCheckConstraint(tab, rel, constr, recurse);
							break;
						}
					default:
						elog(ERROR, "unrecognized constraint type: %d",
							 (int) constr->contype);
				}
				break;
			}
		case T_FkConstraint:
			{
				FkConstraint *fkconstraint = (FkConstraint *) newConstraint;
				/*
				 * Assign or validate constraint name
				 */
				if (fkconstraint->constr_name)
				{
					if (ConstraintNameIsUsed(CONSTRAINT_RELATION,
											 RelationGetRelid(rel),
											 RelationGetNamespace(rel),
											 fkconstraint->constr_name))
						ereport(ERROR,
								(errcode(ERRCODE_DUPLICATE_OBJECT),
								 errmsg("constraint \"%s\" for relation \"%s\" already exists",
										fkconstraint->constr_name,
										RelationGetRelationName(rel)),
												   errOmitLocation(true)));
				}
				else
					fkconstraint->constr_name =
						ChooseConstraintName(RelationGetRelationName(rel),
									strVal(linitial(fkconstraint->fk_attrs)),
											 "fkey",
											 RelationGetNamespace(rel),
											 NIL);

				ATAddForeignKeyConstraint(tab, rel, fkconstraint);

				break;
			}
		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(newConstraint));
	}

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "ADD CONSTRAINT"
				);

}

static void
ATAddCheckConstraint(AlteredTableInfo *tab, Relation rel, Constraint *constr, bool recurse)
{
	List		*newcons;
	ListCell	*lcon;
	List 		*children;
	ListCell	*lchild;

	/*
	 * Call AddRelationNewConstraints to do the work.
	 * It returns a list of cooked constraints.
	 */
	newcons = AddRelationConstraints(rel, NIL, list_make1(constr));

	/* Add each constraint to Phase 3's queue */
	foreach(lcon, newcons)
	{
		CookedConstraint *ccon = (CookedConstraint *) lfirst(lcon);
		NewConstraint *newcon;

		newcon = (NewConstraint *) palloc0(sizeof(NewConstraint));
		newcon->name = ccon->name;
		newcon->contype = ccon->contype;
		/* ExecQual wants implicit-AND format */
		newcon->qual = (Node *) make_ands_implicit((Expr *) ccon->expr);

		tab->constraints = lappend(tab->constraints, newcon);

		/* Save the actually assigned name if it was defaulted on partitioned table */
		if (rel_is_partitioned(RelationGetRelid(rel)) && constr->name == NULL)
			constr->name = ccon->name;
	}

	/*
	 * If this constraint needs to be recursed, this is a base/parent table,
	 * and we are the master, then cascade check constraint creation for all children.
	 * We do it here to synchronize pg_constraint oid.
	 */
	if (Gp_role == GP_ROLE_DISPATCH && recurse)
	{
		/* Propagate to children as appropriate. */
		children = find_inheritance_children(RelationGetRelid(rel));
		if (children == NIL)
			return;

		DestReceiver *dest = None_Receiver;
		foreach(lchild, children)
		{
			Oid 			childrelid = lfirst_oid(lchild);
			Relation 		childrel;
			Constraint 		*childconstr;

			RangeVar 		*rv;
			AlterTableCmd 	*atc;
			AlterTableStmt 	*ats;

			if (childrelid == RelationGetRelid(rel))
				continue;

 			childrel = heap_open(childrelid, AccessShareLock);
 			CheckTableNotInUse(childrel, "ALTER TABLE");

 			/* Recurse to child */
 			childconstr = copyObject(constr);
 			childconstr->conoid = InvalidOid;
 			//Insist(childconstr->name != NULL);

			rv = makeRangeVar(NULL /*catalogname*/, get_namespace_name(RelationGetNamespace(childrel)),
							  get_rel_name(childrelid), -1);

			atc = makeNode(AlterTableCmd);
			atc->subtype = AT_AddConstraintRecurse;
			atc->def = (Node *) childconstr;

			ats = makeNode(AlterTableStmt);
			ats->relation = rv;
			ats->cmds = list_make1(atc);
			ats->relkind = OBJECT_TABLE;

			heap_close(childrel, AccessShareLock);

			ProcessUtility((Node *)ats,
						   synthetic_sql,
						   NULL,
						   false, /* not top level */
						   dest,
						   NULL);
		}
	}
}


/*
 * Add a foreign-key constraint to a single table
 *
 * Subroutine for ATExecAddConstraint.	Must already hold exclusive
 * lock on the rel, and have done appropriate validity/permissions checks
 * for it.
 */
static void
ATAddForeignKeyConstraint(AlteredTableInfo *tab, Relation rel,
						  FkConstraint *fkconstraint)
{
	Relation	pkrel;
	AclResult	aclresult;
	int16		pkattnum[INDEX_MAX_KEYS];
	int16		fkattnum[INDEX_MAX_KEYS];
	Oid			pktypoid[INDEX_MAX_KEYS];
	Oid			fktypoid[INDEX_MAX_KEYS];
	Oid			opclasses[INDEX_MAX_KEYS];
	int			i;
	int			numfks,
				numpks;
	Oid			indexOid;
	Oid			constrOid;

	/*
	 * Grab an exclusive lock on the pk table, so that someone doesn't delete
	 * rows out from under us. (Although a lesser lock would do for that
	 * purpose, we'll need exclusive lock anyway to add triggers to the pk
	 * table; trying to start with a lesser lock will just create a risk of
	 * deadlock.)
	 */
	if (OidIsValid(fkconstraint->old_pktable_oid))
	{
		pkrel = heap_open(fkconstraint->old_pktable_oid, AccessExclusiveLock);
	}
	else
	{
		pkrel = heap_openrv(fkconstraint->pktable, AccessExclusiveLock);
	}

	/*
	 * Validity and permissions checks
	 *
	 * Note: REFERENCES permissions checks are redundant with CREATE TRIGGER,
	 * but we may as well error out sooner instead of later.
	 */
	if (pkrel->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("referenced relation \"%s\" is not a table",
						RelationGetRelationName(pkrel)),
								   errOmitLocation(true)));

	aclresult = pg_class_aclcheck(RelationGetRelid(pkrel), GetUserId(),
								  ACL_REFERENCES);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_CLASS,
					   RelationGetRelationName(pkrel));

	if (!allowSystemTableModsDDL && IsSystemRelation(pkrel))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied: \"%s\" is a system catalog",
						RelationGetRelationName(pkrel)),
								   errOmitLocation(true)));

	aclresult = pg_class_aclcheck(RelationGetRelid(rel), GetUserId(),
								  ACL_REFERENCES);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_CLASS,
					   RelationGetRelationName(rel));

	/*
	 * Disallow reference from permanent table to temp table or vice versa.
	 * (The ban on perm->temp is for fairly obvious reasons.  The ban on
	 * temp->perm is because other backends might need to run the RI triggers
	 * on the perm table, but they can't reliably see tuples the owning
	 * backend has created in the temp table, because non-shared buffers are
	 * used for temp tables.)
	 */
	if (isTempNamespace(RelationGetNamespace(pkrel)))
	{
		if (!isTempNamespace(RelationGetNamespace(rel)))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("cannot reference temporary table from permanent table constraint"),
							   errOmitLocation(true)));
	}
	else
	{
		if (isTempNamespace(RelationGetNamespace(rel)))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("cannot reference permanent table from temporary table constraint"),
							   errOmitLocation(true)));
	}
	
	/*
	 * Disallow reference to a part of a partitioned table.  A foreign key 
	 * must reference the whole partitioned table or none of it.
	 */
	if ( rel_is_child_partition(RelationGetRelid(pkrel)) )
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot reference just part of a partitioned table"),
				 errOmitLocation(true)));
	}

	/*
	 * Look up the referencing attributes to make sure they exist, and record
	 * their attnums and type OIDs.
	 */
	MemSet(pkattnum, 0, sizeof(pkattnum));
	MemSet(fkattnum, 0, sizeof(fkattnum));
	MemSet(pktypoid, 0, sizeof(pktypoid));
	MemSet(fktypoid, 0, sizeof(fktypoid));
	MemSet(opclasses, 0, sizeof(opclasses));

	numfks = transformColumnNameList(RelationGetRelid(rel),
									 fkconstraint->fk_attrs,
									 fkattnum, fktypoid);

	/*
	 * If the attribute list for the referenced table was omitted, lookup the
	 * definition of the primary key and use it.  Otherwise, validate the
	 * supplied attribute list.  In either case, discover the index OID and
	 * index opclasses, and the attnums and type OIDs of the attributes.
	 */
	if (fkconstraint->pk_attrs == NIL)
	{
		numpks = transformFkeyGetPrimaryKey(pkrel, &indexOid,
											&fkconstraint->pk_attrs,
											pkattnum, pktypoid,
											opclasses);
	}
	else
	{
		numpks = transformColumnNameList(RelationGetRelid(pkrel),
										 fkconstraint->pk_attrs,
										 pkattnum, pktypoid);
		/* Look for an index matching the column list */
		indexOid = transformFkeyCheckAttrs(pkrel, numpks, pkattnum,
										   opclasses);
	}

	/* Be sure referencing and referenced column types are comparable */
	if (numfks != numpks)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FOREIGN_KEY),
				 errmsg("number of referencing and referenced columns for foreign key disagree"),
						   errOmitLocation(true)));

	for (i = 0; i < numpks; i++)
	{
		/*
		 * pktypoid[i] is the primary key table's i'th key's type fktypoid[i]
		 * is the foreign key table's i'th key's type
		 *
		 * Note that we look for an operator with the PK type on the left;
		 * when the types are different this is critical because the PK index
		 * will need operators with the indexkey on the left. (Ordinarily both
		 * commutator operators will exist if either does, but we won't get
		 * the right answer from the test below on opclass membership unless
		 * we select the proper operator.)
		 */
		Operator	o = oper(NULL, list_make1(makeString("=")),
							 pktypoid[i], fktypoid[i],
							 true, -1);

		if (o == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("foreign key constraint \"%s\" "
							"cannot be implemented",
							fkconstraint->constr_name),
					 errdetail("Key columns \"%s\" and \"%s\" "
							   "are of incompatible types: %s and %s.",
							   strVal(list_nth(fkconstraint->fk_attrs, i)),
							   strVal(list_nth(fkconstraint->pk_attrs, i)),
							   format_type_be(fktypoid[i]),
							   format_type_be(pktypoid[i])),
									   errOmitLocation(true)));

		/*
		 * Check that the found operator is compatible with the PK index, and
		 * generate a warning if not, since otherwise costly seqscans will be
		 * incurred to check FK validity.
		 */
		if (Gp_role != GP_ROLE_EXECUTE && !op_in_opclass(oprid(o), opclasses[i]))
			ereport(WARNING,
					(errmsg("foreign key constraint \"%s\" "
							"will require costly sequential scans",
							fkconstraint->constr_name),
					 errdetail("Key columns \"%s\" and \"%s\" "
							   "are of different types: %s and %s.",
							   strVal(list_nth(fkconstraint->fk_attrs, i)),
							   strVal(list_nth(fkconstraint->pk_attrs, i)),
							   format_type_be(fktypoid[i]),
							   format_type_be(pktypoid[i])),
									   errOmitLocation(true)));

		ReleaseOperator(o);
	}

	/*
	 * Tell Phase 3 to check that the constraint is satisfied by existing rows
	 * (we can skip this during table creation).
	 */
	if (!fkconstraint->skip_validation)
	{
		NewConstraint *newcon;

		newcon = (NewConstraint *) palloc0(sizeof(NewConstraint));
		newcon->name = fkconstraint->constr_name;
		newcon->contype = CONSTR_FOREIGN;
		newcon->refrelid = RelationGetRelid(pkrel);
		newcon->qual = (Node *) fkconstraint;

		tab->constraints = lappend(tab->constraints, newcon);
	}

	/*
	 * Record the FK constraint in pg_constraint.
	 */
	constrOid = CreateConstraintEntry(fkconstraint->constr_name,
									  fkconstraint->constrOid,
									  RelationGetNamespace(rel),
									  CONSTRAINT_FOREIGN,
									  fkconstraint->deferrable,
									  fkconstraint->initdeferred,
									  RelationGetRelid(rel),
									  fkattnum,
									  numfks,
									  InvalidOid,		/* not a domain
														 * constraint */
									  RelationGetRelid(pkrel),
									  pkattnum,
									  numpks,
									  fkconstraint->fk_upd_action,
									  fkconstraint->fk_del_action,
									  fkconstraint->fk_matchtype,
									  indexOid,
									  NULL,		/* no check constraint */
									  NULL,
									  NULL);
	fkconstraint->constrOid = constrOid;

	/*
	 * Create the triggers that will enforce the constraint.
	 */
	createForeignKeyTriggers(rel, RelationGetRelid(pkrel), fkconstraint,
	                         constrOid);

	/*
	 * Close pk table, but keep lock until we've committed.
	 */
	heap_close(pkrel, NoLock);
}


/*
 * transformColumnNameList - transform list of column names
 *
 * Lookup each name and return its attnum and type OID
 */
static int
transformColumnNameList(Oid relId, List *colList,
						int16 *attnums, Oid *atttypids)
{
	ListCell   *l;
	int			attnum;

	attnum = 0;
	foreach(l, colList)
	{
		char	   *attname = strVal(lfirst(l));
		HeapTuple	atttuple;
		cqContext  *pcqCtx;

		pcqCtx = caql_getattname_scan(NULL, relId, attname);

		atttuple = caql_get_current(pcqCtx);

		if (!HeapTupleIsValid(atttuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" referenced in foreign key constraint does not exist",
							attname),
									   errOmitLocation(true)));
		if (attnum >= INDEX_MAX_KEYS)
			ereport(ERROR,
					(errcode(ERRCODE_TOO_MANY_COLUMNS),
					 errmsg("cannot have more than %d keys in a foreign key",
							INDEX_MAX_KEYS)));
		attnums[attnum] = ((Form_pg_attribute) GETSTRUCT(atttuple))->attnum;
		atttypids[attnum] = ((Form_pg_attribute) GETSTRUCT(atttuple))->atttypid;

		caql_endscan(pcqCtx);

		attnum++;
	}

	return attnum;
}

/*
 * transformFkeyGetPrimaryKey -
 *
 *	Look up the names, attnums, and types of the primary key attributes
 *	for the pkrel.	Also return the index OID and index opclasses of the
 *	index supporting the primary key.
 *
 *	All parameters except pkrel are output parameters.	Also, the function
 *	return value is the number of attributes in the primary key.
 *
 *	Used when the column list in the REFERENCES specification is omitted.
 */
static int
transformFkeyGetPrimaryKey(Relation pkrel, Oid *indexOid,
						   List **attnamelist,
						   int16 *attnums, Oid *atttypids,
						   Oid *opclasses)
{
	List	   *indexoidlist;
	ListCell   *indexoidscan;
	HeapTuple	indexTuple = NULL;
	Form_pg_index indexStruct = NULL;
	Datum		indclassDatum;
	bool		isnull;
	oidvector  *indclass;
	int			i;
	cqContext  *pcqCtx;

	/*
	 * Get the list of index OIDs for the table from the relcache, and look up
	 * each one in the pg_index syscache until we find one marked primary key
	 * (hopefully there isn't more than one such).
	 */
	*indexOid = InvalidOid;

	indexoidlist = RelationGetIndexList(pkrel);

	foreach(indexoidscan, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(indexoidscan);

		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_index "
					" WHERE indexrelid = :1 ",
					ObjectIdGetDatum(indexoid)));

		indexTuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(indexTuple))
			elog(ERROR, "cache lookup failed for index %u", indexoid);
		indexStruct = (Form_pg_index) GETSTRUCT(indexTuple);
		if (indexStruct->indisprimary)
		{
			*indexOid = indexoid;
			break;
		}
		caql_endscan(pcqCtx);
	}

	list_free(indexoidlist);

	/*
	 * Check that we found it
	 */
	if (!OidIsValid(*indexOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("there is no primary key for referenced table \"%s\"",
						RelationGetRelationName(pkrel)),
								   errOmitLocation(true)));

	/* Must get indclass the hard way */
	indclassDatum = caql_getattr(pcqCtx,
								 Anum_pg_index_indclass, &isnull);
	Assert(!isnull);
	indclass = (oidvector *) DatumGetPointer(indclassDatum);

	/*
	 * Now build the list of PK attributes from the indkey definition (we
	 * assume a primary key cannot have expressional elements)
	 */
	*attnamelist = NIL;
	for (i = 0; i < indexStruct->indnatts; i++)
	{
		int			pkattno = indexStruct->indkey.values[i];

		attnums[i] = pkattno;
		atttypids[i] = attnumTypeId(pkrel, pkattno);
		opclasses[i] = indclass->values[i];
		*attnamelist = lappend(*attnamelist,
			   makeString(pstrdup(NameStr(*attnumAttName(pkrel, pkattno)))));
	}

	caql_endscan(pcqCtx);

	return i;
}

/*
 * transformFkeyCheckAttrs -
 *
 *	Make sure that the attributes of a referenced table belong to a unique
 *	(or primary key) constraint.  Return the OID of the index supporting
 *	the constraint, as well as the opclasses associated with the index
 *	columns.
 */
Oid
transformFkeyCheckAttrs(Relation pkrel,
						int numattrs, int16 *attnums,
						Oid *opclasses) /* output parameter */
{
	Oid			indexoid = InvalidOid;
	bool		found = false;
	List	   *indexoidlist;
	ListCell   *indexoidscan;

	/*
	 * Get the list of index OIDs for the table from the relcache, and look up
	 * each one in the pg_index syscache, and match unique indexes to the list
	 * of attnums we are given.
	 */
	indexoidlist = RelationGetIndexList(pkrel);

	foreach(indexoidscan, indexoidlist)
	{
		HeapTuple	indexTuple;
		cqContext  *pcqCtx;
		Form_pg_index indexStruct;
		int			i,
					j;

		indexoid = lfirst_oid(indexoidscan);
		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_index "
					" WHERE indexrelid = :1 ",
					ObjectIdGetDatum(indexoid)));

		indexTuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(indexTuple))
			elog(ERROR, "cache lookup failed for index %u", indexoid);
		indexStruct = (Form_pg_index) GETSTRUCT(indexTuple);

		/*
		 * Must have the right number of columns; must be unique and not a
		 * partial index; forget it if there are any expressions, too
		 */
		if (indexStruct->indnatts == numattrs &&
			indexStruct->indisunique &&
			heap_attisnull(indexTuple, Anum_pg_index_indpred) &&
			heap_attisnull(indexTuple, Anum_pg_index_indexprs))
		{
			/* Must get indclass the hard way */
			Datum		indclassDatum;
			bool		isnull;
			oidvector  *indclass;

			indclassDatum = caql_getattr(pcqCtx,
										 Anum_pg_index_indclass, &isnull);
			Assert(!isnull);
			indclass = (oidvector *) DatumGetPointer(indclassDatum);

			/*
			 * The given attnum list may match the index columns in any order.
			 * Check that each list is a subset of the other.
			 */
			for (i = 0; i < numattrs; i++)
			{
				found = false;
				for (j = 0; j < numattrs; j++)
				{
					if (attnums[i] == indexStruct->indkey.values[j])
					{
						found = true;
						break;
					}
				}
				if (!found)
					break;
			}
			if (found)
			{
				for (i = 0; i < numattrs; i++)
				{
					found = false;
					for (j = 0; j < numattrs; j++)
					{
						if (attnums[j] == indexStruct->indkey.values[i])
						{
							opclasses[j] = indclass->values[i];
							found = true;
							break;
						}
					}
					if (!found)
						break;
				}
			}
		}
		caql_endscan(pcqCtx);
		if (found)
			break;
	} /* end foreach(indexoidscan, indexoidlist) */

	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FOREIGN_KEY),
				 errmsg("there is no unique constraint matching given keys for referenced table \"%s\"",
						RelationGetRelationName(pkrel)),
								   errOmitLocation(true)));

	list_free(indexoidlist);

	return indexoid;
}

/*
 * Scan the existing rows in a table to verify they meet a proposed FK
 * constraint.
 *
 * Caller must have opened and locked both relations.
 */
static void
validateForeignKeyConstraint(FkConstraint *fkconstraint,
							 Relation rel,
							 Relation pkrel)
{
	HeapScanDesc scan;
	HeapTuple	tuple;
	Trigger		trig;
	ListCell   *list;
	int			count;

	/*
	 * See if we can do it with a single LEFT JOIN query.  A FALSE result
	 * indicates we must proceed with the fire-the-trigger method.
	 */
	if (RI_Initial_Check(fkconstraint, rel, pkrel))
		return;

	/*
	 * Scan through each tuple, calling RI_FKey_check_ins (insert trigger) as
	 * if that tuple had just been inserted.  If any of those fail, it should
	 * ereport(ERROR) and that's that.
	 */
	MemSet(&trig, 0, sizeof(trig));
	trig.tgoid = InvalidOid;
	trig.tgname = fkconstraint->constr_name;
	trig.tgenabled = TRUE;
	trig.tgisconstraint = TRUE;
	trig.tgconstrrelid = RelationGetRelid(pkrel);
	trig.tgdeferrable = FALSE;
	trig.tginitdeferred = FALSE;

	trig.tgargs = (char **) palloc(sizeof(char *) *
								   (4 + list_length(fkconstraint->fk_attrs)
									+ list_length(fkconstraint->pk_attrs)));

	trig.tgargs[0] = trig.tgname;
	trig.tgargs[1] = RelationGetRelationName(rel);
	trig.tgargs[2] = RelationGetRelationName(pkrel);
	trig.tgargs[3] = fkMatchTypeToString(fkconstraint->fk_matchtype);
	count = 4;
	foreach(list, fkconstraint->fk_attrs)
	{
		char	   *fk_at = strVal(lfirst(list));

		trig.tgargs[count] = fk_at;
		count += 2;
	}
	count = 5;
	foreach(list, fkconstraint->pk_attrs)
	{
		char	   *pk_at = strVal(lfirst(list));

		trig.tgargs[count] = pk_at;
		count += 2;
	}
	trig.tgnargs = count - 1;

	scan = heap_beginscan(rel, SnapshotNow, 0, NULL);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		FunctionCallInfoData fcinfo;
		TriggerData trigdata;

		/*
		 * Make a call to the trigger function
		 *
		 * No parameters are passed, but we do set a context
		 */
		MemSet(&fcinfo, 0, sizeof(fcinfo));

		/*
		 * We assume RI_FKey_check_ins won't look at flinfo...
		 */
		trigdata.type = T_TriggerData;
		trigdata.tg_event = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_ROW;
		trigdata.tg_relation = rel;
		trigdata.tg_trigtuple = tuple;
		trigdata.tg_newtuple = NULL;
		trigdata.tg_trigger = &trig;
		trigdata.tg_trigtuplebuf = scan->rs_cbuf;
		trigdata.tg_newtuplebuf = InvalidBuffer;

		fcinfo.context = (Node *) &trigdata;

		RI_FKey_check_ins(&fcinfo);
	}

	heap_endscan(scan);

	pfree(trig.tgargs);
}

static void
CreateFKCheckTrigger(Oid myRelOid, Oid refRelOid,
					 FkConstraint *fkconstraint,
					 ObjectAddress *constrobj, ObjectAddress *trigobj,
					 bool on_insert)
{
	CreateTrigStmt *fk_trigger;
	ListCell   *fk_attr;
	ListCell   *pk_attr;

	fk_trigger = makeNode(CreateTrigStmt);
	fk_trigger->trigname = fkconstraint->constr_name;
	fk_trigger->relation = NULL;
	fk_trigger->before = false;
	fk_trigger->row = true;

	/* Either ON INSERT or ON UPDATE */
	if (on_insert)
	{
		fk_trigger->funcname = SystemFuncName("RI_FKey_check_ins");
		fk_trigger->actions[0] = 'i';
		fk_trigger->trigOid = fkconstraint->trig1Oid;
	}
	else
	{
		fk_trigger->funcname = SystemFuncName("RI_FKey_check_upd");
		fk_trigger->actions[0] = 'u';
		fk_trigger->trigOid = fkconstraint->trig2Oid;
	}
	fk_trigger->actions[1] = '\0';

	fk_trigger->isconstraint = true;
	fk_trigger->deferrable = fkconstraint->deferrable;
	fk_trigger->initdeferred = fkconstraint->initdeferred;
	fk_trigger->constrrel = NULL;

	fk_trigger->args = NIL;
	fk_trigger->args = lappend(fk_trigger->args,
							   makeString(fkconstraint->constr_name));
	fk_trigger->args = lappend(fk_trigger->args,
							   makeString(get_rel_name(myRelOid)));
	fk_trigger->args = lappend(fk_trigger->args,
							   makeString(fkconstraint->pktable->relname));
	fk_trigger->args = lappend(fk_trigger->args,
				makeString(fkMatchTypeToString(fkconstraint->fk_matchtype)));
	if (list_length(fkconstraint->fk_attrs) != list_length(fkconstraint->pk_attrs))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FOREIGN_KEY),
				 errmsg("number of referencing and referenced columns for foreign key disagree"),
						   errOmitLocation(true)));

	forboth(fk_attr, fkconstraint->fk_attrs,
			pk_attr, fkconstraint->pk_attrs)
	{
		fk_trigger->args = lappend(fk_trigger->args, lfirst(fk_attr));
		fk_trigger->args = lappend(fk_trigger->args, lfirst(pk_attr));
	}

	trigobj->objectId = CreateTrigger(fk_trigger, myRelOid, refRelOid, true);

	if (on_insert)
		fkconstraint->trig1Oid = trigobj->objectId;
	else
		fkconstraint->trig2Oid = trigobj->objectId;

	/* Register dependency from trigger to constraint */
	recordDependencyOn(trigobj, constrobj, DEPENDENCY_INTERNAL);

	/* Make changes-so-far visible */
	CommandCounterIncrement();
}

/*
 * Create the triggers that implement an FK constraint.
 */
static void
createForeignKeyTriggers(Relation rel, Oid refRelOid,
						 FkConstraint *fkconstraint, Oid constrOid)
{
	Oid         myRelOid;
	CreateTrigStmt *fk_trigger;
	ListCell   *fk_attr;
	ListCell   *pk_attr;
	ObjectAddress trigobj,
				constrobj;

	/*
	 * Special for Greenplum Database: Ignore foreign keys for now, with warning
	 */
	if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_EXECUTE)
	{
		if (Gp_role == GP_ROLE_DISPATCH)
			ereport(WARNING,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("Referential integrity (FOREIGN KEY) constraints are not supported in Greenplum Database, will not be enforced."),
						   errOmitLocation(true)));
	}

	myRelOid = RelationGetRelid(rel);

	/*
	 * Preset objectAddress fields
	 */
	constrobj.classId = ConstraintRelationId;
	constrobj.objectId = constrOid;
	constrobj.objectSubId = 0;
	trigobj.classId = TriggerRelationId;
	trigobj.objectSubId = 0;

	/* Make changes-so-far visible */
	CommandCounterIncrement();

	/*
	 * Build and execute a CREATE CONSTRAINT TRIGGER statement for the CHECK
	 * action for both INSERTs and UPDATEs on the referencing table.
	 */
	CreateFKCheckTrigger(myRelOid, refRelOid, fkconstraint,
						 &constrobj, &trigobj, true);
	CreateFKCheckTrigger(myRelOid, refRelOid, fkconstraint,
						 &constrobj, &trigobj, false);

	/*
	 * Build and execute a CREATE CONSTRAINT TRIGGER statement for the ON
	 * DELETE action on the referenced table.
	 */
	fk_trigger = makeNode(CreateTrigStmt);
	fk_trigger->trigname = fkconstraint->constr_name;
	fk_trigger->relation = NULL;
	fk_trigger->before = false;
	fk_trigger->row = true;
	fk_trigger->actions[0] = 'd';
	fk_trigger->actions[1] = '\0';

	fk_trigger->isconstraint = true;
	fk_trigger->constrrel = NULL;
	switch (fkconstraint->fk_del_action)
	{
		case FKCONSTR_ACTION_NOACTION:
			fk_trigger->deferrable = fkconstraint->deferrable;
			fk_trigger->initdeferred = fkconstraint->initdeferred;
			fk_trigger->funcname = SystemFuncName("RI_FKey_noaction_del");
			break;
		case FKCONSTR_ACTION_RESTRICT:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_restrict_del");
			break;
		case FKCONSTR_ACTION_CASCADE:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_cascade_del");
			break;
		case FKCONSTR_ACTION_SETNULL:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_setnull_del");
			break;
		case FKCONSTR_ACTION_SETDEFAULT:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_setdefault_del");
			break;
		default:
			elog(ERROR, "unrecognized FK action type: %d",
				 (int) fkconstraint->fk_del_action);
			break;
	}

	fk_trigger->args = NIL;
	fk_trigger->args = lappend(fk_trigger->args,
							   makeString(fkconstraint->constr_name));
	fk_trigger->args = lappend(fk_trigger->args,
							   makeString(get_rel_name(myRelOid)));
	fk_trigger->args = lappend(fk_trigger->args,
							   makeString(fkconstraint->pktable->relname));
	fk_trigger->args = lappend(fk_trigger->args,
				makeString(fkMatchTypeToString(fkconstraint->fk_matchtype)));
	forboth(fk_attr, fkconstraint->fk_attrs,
			pk_attr, fkconstraint->pk_attrs)
	{
		fk_trigger->args = lappend(fk_trigger->args, lfirst(fk_attr));
		fk_trigger->args = lappend(fk_trigger->args, lfirst(pk_attr));
	}

	fk_trigger->trigOid = fkconstraint->trig3Oid;

	trigobj.objectId = CreateTrigger(fk_trigger, refRelOid, myRelOid, true);

	fkconstraint->trig3Oid = trigobj.objectId;

	/* Register dependency from trigger to constraint */
	recordDependencyOn(&trigobj, &constrobj, DEPENDENCY_INTERNAL);

	/* Make changes-so-far visible */
	CommandCounterIncrement();

	/*
	 * Build and execute a CREATE CONSTRAINT TRIGGER statement for the ON
	 * UPDATE action on the referenced table.
	 */
	fk_trigger = makeNode(CreateTrigStmt);
	fk_trigger->trigname = fkconstraint->constr_name;
	fk_trigger->relation = NULL;
	fk_trigger->before = false;
	fk_trigger->row = true;
	fk_trigger->actions[0] = 'u';
	fk_trigger->actions[1] = '\0';
	fk_trigger->isconstraint = true;
	fk_trigger->constrrel = NULL;
	switch (fkconstraint->fk_upd_action)
	{
		case FKCONSTR_ACTION_NOACTION:
			fk_trigger->deferrable = fkconstraint->deferrable;
			fk_trigger->initdeferred = fkconstraint->initdeferred;
			fk_trigger->funcname = SystemFuncName("RI_FKey_noaction_upd");
			break;
		case FKCONSTR_ACTION_RESTRICT:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_restrict_upd");
			break;
		case FKCONSTR_ACTION_CASCADE:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_cascade_upd");
			break;
		case FKCONSTR_ACTION_SETNULL:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_setnull_upd");
			break;
		case FKCONSTR_ACTION_SETDEFAULT:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_setdefault_upd");
			break;
		default:
			elog(ERROR, "unrecognized FK action type: %d",
				 (int) fkconstraint->fk_upd_action);
			break;
	}

	fk_trigger->args = NIL;
	fk_trigger->args = lappend(fk_trigger->args,
							   makeString(fkconstraint->constr_name));
	fk_trigger->args = lappend(fk_trigger->args,
							   makeString(get_rel_name(myRelOid)));
	fk_trigger->args = lappend(fk_trigger->args,
							   makeString(fkconstraint->pktable->relname));
	fk_trigger->args = lappend(fk_trigger->args,
				makeString(fkMatchTypeToString(fkconstraint->fk_matchtype)));
	forboth(fk_attr, fkconstraint->fk_attrs,
			pk_attr, fkconstraint->pk_attrs)
	{
		fk_trigger->args = lappend(fk_trigger->args, lfirst(fk_attr));
		fk_trigger->args = lappend(fk_trigger->args, lfirst(pk_attr));
	}

	fk_trigger->trigOid = fkconstraint->trig4Oid;

	trigobj.objectId = CreateTrigger(fk_trigger, refRelOid, myRelOid, true);

	fkconstraint->trig4Oid = trigobj.objectId;

	/* Register dependency from trigger to constraint */
	recordDependencyOn(&trigobj, &constrobj, DEPENDENCY_INTERNAL);
}

/*
 * fkMatchTypeToString -
 *	  convert FKCONSTR_MATCH_xxx code to string to use in trigger args
 */
static char *
fkMatchTypeToString(char match_type)
{
	switch (match_type)
	{
		case FKCONSTR_MATCH_FULL:
			return pstrdup("FULL");
		case FKCONSTR_MATCH_PARTIAL:
			return pstrdup("PARTIAL");
		case FKCONSTR_MATCH_UNSPECIFIED:
			return pstrdup("UNSPECIFIED");
		default:
			elog(ERROR, "unrecognized match type: %d",
				 (int) match_type);
	}
	return NULL;				/* can't get here */
}

/*
 * ALTER TABLE DROP CONSTRAINT
 */
static void
ATPrepDropConstraint(List **wqueue, Relation rel,
					 bool recurse, AlterTableCmd *cmd)
{
	/*
	 * We don't want errors or noise from child tables, so we have to pass
	 * down a modified command.
	 *
	 * This can't result in dropping the partitioning rule because the
	 * partitioning rule exists only on the part, and ATPrepCmd guards
	 * against dropping directly from parts.
	 */
	if (recurse)
	{
		AlterTableCmd *childCmd = copyObject(cmd);

		childCmd->subtype = AT_DropConstraintQuietly;
		ATSimpleRecursion(wqueue, rel, childCmd, recurse);
	}
}

static void
ATExecDropConstraint(Relation rel, const char *constrName,
					 DropBehavior behavior, bool quiet)
{
	int			deleted;

	deleted = RemoveRelConstraints(rel, constrName, behavior);

	if (!quiet)
	{
		/* If zero constraints deleted, complain */
		if (deleted == 0)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("constraint \"%s\" does not exist",
							constrName),
									   errOmitLocation(true)));
		/* Otherwise if more than one constraint deleted, notify */
		else if (deleted > 1 && Gp_role != GP_ROLE_EXECUTE)
			ereport(NOTICE,
					(errmsg("multiple constraints named \"%s\" were dropped",
							constrName),
									   errOmitLocation(true)));
	}

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "DROP CONSTRAINT"
				);

}

/*
 * ALTER COLUMN TYPE
 */
static void
ATPrepAlterColumnType(List **wqueue,
					  AlteredTableInfo *tab, Relation rel,
					  bool recurse, bool recursing,
					  AlterTableCmd *cmd)
{
	char	   *colName = cmd->name;
	TypeName   *typname = (TypeName *) cmd->def;
	HeapTuple	tuple;
	Form_pg_attribute attTup;
	AttrNumber	attnum;
	Oid			targettype;
	Node	   *transform;
	NewColumnValue *newval;
	ParseState *pstate = make_parsestate(NULL);
	cqContext  *pcqCtx;

	/* lookup the attribute so we can check inheritance status */
	pcqCtx = caql_getattname_scan(NULL, RelationGetRelid(rel), colName);
	
	tuple = caql_get_current(pcqCtx);
	
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel)),
								   errOmitLocation(true)));
	attTup = (Form_pg_attribute) GETSTRUCT(tuple);
	attnum = attTup->attnum;

	/* Can't alter a system attribute */
	if (attnum <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter system column \"%s\"",
						colName),
								   errOmitLocation(true)));

	/* Don't alter inherited columns */
	if (attTup->attinhcount > 0 && !recursing)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot alter inherited column \"%s\"",
						colName),
								   errOmitLocation(true)));

	/* Look up the target type */
	targettype = typenameTypeId(NULL, typname);

	/* make sure datatype is legal for a column */
	CheckAttributeType(colName, targettype);

	/*
	 * Set up an expression to transform the old data value to the new type.
	 * If a USING option was given, transform and use that expression, else
	 * just take the old value and try to coerce it.  We do this first so that
	 * type incompatibility can be detected before we waste effort, and
	 * because we need the expression to be parsed against the original table
	 * rowtype.
	 */

	/* GPDB: we always need the RTE */
	{
		RangeTblEntry *rte;

		/* Expression must be able to access vars of old table */
		rte = addRangeTableEntryForRelation(pstate,
											rel,
											NULL,
											false,
											true);
		addRTEtoQuery(pstate, rte, false, true, true);
	}

	if (cmd->transform)
	{
		transform = transformExpr(pstate, cmd->transform);
		/* It can't return a set */
		if (expression_returns_set(transform))
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("transform expression must not return a set"),
							   errOmitLocation(true)));

		/* No subplans or aggregates, either... */
		if (pstate->p_hasSubLinks)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot use subquery in transform expression"),
							   errOmitLocation(true)));
		if (pstate->p_hasAggs)
			ereport(ERROR,
					(errcode(ERRCODE_GROUPING_ERROR),
			errmsg("cannot use aggregate function in transform expression"),
					   errOmitLocation(true)));
		if (pstate->p_hasWindFuncs)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("cannot use window function in transform expression"),
					   errOmitLocation(true)));

	}
	else
	{
		transform = (Node *) makeVar(1, attnum,
									 attTup->atttypid, attTup->atttypmod,
									 0);
	}

	transform = coerce_to_target_type(pstate,
									  transform, exprType(transform),
									  targettype, typname->typmod,
									  COERCION_ASSIGNMENT,
									  COERCE_IMPLICIT_CAST,
									  -1);
	if (transform == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("column \"%s\" cannot be cast to type \"%s\"",
						colName, TypeNameToString(typname)),
								   errOmitLocation(true)));

	free_parsestate(&pstate);

	/*
	 * Add a work queue item to make ATRewriteTable update the column
	 * contents.
	 */
	newval = (NewColumnValue *) palloc0(sizeof(NewColumnValue));
	newval->attnum = attnum;
	newval->expr = (Expr *) transform;

	tab->newvals = lappend(tab->newvals, newval);

	caql_endscan(pcqCtx);

	/*
	 * The recursion case is handled by ATSimpleRecursion.	However, if we are
	 * told not to recurse, there had better not be any child tables; else the
	 * alter would put them out of step.
	 */
	if (recurse)
		ATSimpleRecursion(wqueue, rel, cmd, recurse);
	else if (!recursing &&
			 find_inheritance_children(RelationGetRelid(rel)) != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("type of inherited column \"%s\" must be changed in child tables too",
						colName),
								   errOmitLocation(true)));
}

static void
ATExecAlterColumnType(AlteredTableInfo *tab, Relation rel,
					  const char *colName, TypeName *typname)
{
	HeapTuple	heapTup;
	Form_pg_attribute attTup;
	AttrNumber	attnum;
	HeapTuple	typeTuple;
	Form_pg_type tform;
	Oid			targettype;
	Node	   *defaultexpr;
	Relation	attrelation;
	Relation	depRel;
	HeapTuple	depTup;
	GpPolicy * policy = NULL;
	bool		sourceIsInt = false;
	bool		targetIsInt = false;
	bool		sourceIsVarlenA = false;
	bool		targetIsVarlenA = false;
	bool		hashCompatible = false;
	cqContext	cqc;
	cqContext	cqc2;
	cqContext  *pcqCtx;
	cqContext  *patCtx;

	attrelation = heap_open(AttributeRelationId, RowExclusiveLock);

	patCtx = caql_addrel(cqclr(&cqc2), attrelation);

	/* Look up the target column */
	heapTup = caql_getattname(patCtx, RelationGetRelid(rel), colName);

	if (!HeapTupleIsValid(heapTup))		/* shouldn't happen */
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colName, RelationGetRelationName(rel)),
								   errOmitLocation(true)));
	attTup = (Form_pg_attribute) GETSTRUCT(heapTup);
	attnum = attTup->attnum;

	/* Check for multiple ALTER TYPE on same column --- can't cope */
	if (attTup->atttypid != tab->oldDesc->attrs[attnum - 1]->atttypid ||
		attTup->atttypmod != tab->oldDesc->attrs[attnum - 1]->atttypmod)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter type of column \"%s\" twice",
						colName),
								   errOmitLocation(true)));

	/* Look up the target type (should not fail, since prep found it) */
	typeTuple = typenameType(NULL, typname);
	tform = (Form_pg_type) GETSTRUCT(typeTuple);
	targettype = HeapTupleGetOid(typeTuple);

	if (targettype == INT4OID ||
		targettype == INT2OID ||
		targettype == INT8OID)
		sourceIsInt = true;

	if (attTup->atttypid == INT4OID ||
		attTup->atttypid == INT2OID ||
		attTup->atttypid == INT8OID)
		targetIsInt = true;

	if (targettype == VARCHAROID ||
		targettype == CHAROID ||
		targettype == TEXTOID)
		targetIsVarlenA = true;

	if (attTup->atttypid == VARCHAROID ||
		attTup->atttypid == CHAROID ||
		attTup->atttypid == TEXTOID)
		sourceIsVarlenA = true;

	if (sourceIsInt && targetIsInt)
		hashCompatible = true;
	else if (sourceIsVarlenA && targetIsVarlenA)
		hashCompatible = true;

	/*
	 * If there is a default expression for the column, get it and ensure we
	 * can coerce it to the new datatype.  (We must do this before changing
	 * the column type, because build_column_default itself will try to
	 * coerce, and will not issue the error message we want if it fails.)
	 *
	 * We remove any implicit coercion steps at the top level of the old
	 * default expression; this has been agreed to satisfy the principle of
	 * least surprise.	(The conversion to the new column type should act like
	 * it started from what the user sees as the stored expression, and the
	 * implicit coercions aren't going to be shown.)
	 */
	if (attTup->atthasdef)
	{
		defaultexpr = build_column_default(rel, attnum);
		Assert(defaultexpr);
		defaultexpr = strip_implicit_coercions(defaultexpr);
		defaultexpr = coerce_to_target_type(NULL,		/* no UNKNOWN params */
										  defaultexpr, exprType(defaultexpr),
											targettype, typname->typmod,
											COERCION_ASSIGNMENT,
											COERCE_IMPLICIT_CAST,
											-1);
		if (defaultexpr == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
			errmsg("default for column \"%s\" cannot be cast to type \"%s\"",
				   colName, TypeNameToString(typname)),
						   errOmitLocation(true)));
	}
	else
		defaultexpr = NULL;

	/*
	 * Find everything that depends on the column (constraints, indexes, etc),
	 * and record enough information to let us recreate the objects.
	 *
	 * The actual recreation does not happen here, but only after we have
	 * performed all the individual ALTER TYPE operations.	We have to save
	 * the info before executing ALTER TYPE, though, else the deparser will
	 * get confused.
	 *
	 * There could be multiple entries for the same object, so we must check
	 * to ensure we process each one only once.  Note: we assume that an index
	 * that implements a constraint will not show a direct dependency on the
	 * column.
	 */
	depRel = heap_open(DependRelationId, RowExclusiveLock);

	/* FOR UPDATE due to DELETE later... */
	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), depRel),
			cql("SELECT * FROM pg_depend "
				" WHERE refclassid = :1 "
				" AND refobjid = :2 "
				" AND refobjsubid = :3 "
				" FOR UPDATE ",
				ObjectIdGetDatum(RelationRelationId),
				ObjectIdGetDatum(RelationGetRelid(rel)),
				Int32GetDatum((int32) attnum)));

	while (HeapTupleIsValid(depTup = caql_getnext(pcqCtx)))
	{
		Form_pg_depend foundDep = (Form_pg_depend) GETSTRUCT(depTup);
		ObjectAddress foundObject;

		/* We don't expect any PIN dependencies on columns */
		if (foundDep->deptype == DEPENDENCY_PIN)
			elog(ERROR, "cannot alter type of a pinned column");

		foundObject.classId = foundDep->classid;
		foundObject.objectId = foundDep->objid;
		foundObject.objectSubId = foundDep->objsubid;

		switch (getObjectClass(&foundObject))
		{
			case OCLASS_CLASS:
				{
					char		relKind = get_rel_relkind(foundObject.objectId);

					if (relKind == RELKIND_INDEX)
					{
						Assert(foundObject.objectSubId == 0);
						if (!list_member_oid(tab->changedIndexOids, foundObject.objectId))
						{
							char * indexdefstring = pg_get_indexdef_string(foundObject.objectId);
							tab->changedIndexOids = lappend_oid(tab->changedIndexOids,
													   foundObject.objectId);
							tab->changedIndexDefs = lappend(tab->changedIndexDefs,indexdefstring);

							if (Gp_role == GP_ROLE_DISPATCH && indexdefstring &&strstr(indexdefstring," UNIQUE ")!=0 && !hashCompatible)
							{
								policy = rel->rd_cdbpolicy;
								if (policy != NULL && policy->ptype == POLICYTYPE_PARTITIONED)
								{
									int			ia = 0;

									if (cdbRelSize(rel) != 0)

									for (ia = 0; ia < policy->nattrs; ia++)
									{
										if (attnum == policy->attrs[ia])
										{
											ereport(ERROR,
													(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
													 errmsg("Changing the type of a column that is part of the distribution policy and used in a unique index is not allowed"),
															   errOmitLocation(true)));
										}
									}
								}
							}
						}
					}
					else if (relKind == RELKIND_SEQUENCE)
					{
						/*
						 * This must be a SERIAL column's sequence.  We need
						 * not do anything to it.
						 */
						Assert(foundObject.objectSubId == 0);
					}
					else
					{
						/* Not expecting any other direct dependencies... */
						elog(ERROR, "unexpected object depending on column: %s",
							 getObjectDescription(&foundObject));
					}
					break;
				}

			case OCLASS_CONSTRAINT:
				Assert(foundObject.objectSubId == 0);
				if (!list_member_oid(tab->changedConstraintOids,
									 foundObject.objectId))
				{
					char	   *defstring = pg_get_constraintdef_string(foundObject.objectId);

					if (Gp_role == GP_ROLE_DISPATCH && !hashCompatible)
					if (strstr(defstring," UNIQUE")!=0 ||
						strstr(defstring,"PRIMARY KEY")!=0 )
					{
						policy = rel->rd_cdbpolicy;
						if (policy != NULL && policy->ptype == POLICYTYPE_PARTITIONED)
						{
							int			ia = 0;

							if (cdbRelSize(rel) != 0)

							for (ia = 0; ia < policy->nattrs; ia++)
							{
								if (attnum == policy->attrs[ia])
								{
									ereport(ERROR,
											(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
											 errmsg("Changing the type of a column that is used in a UNIQUE or PRIMARY KEY constraint is not allowed"),
													   errOmitLocation(true)));
								}
							}
						}
					}

					/*
					 * Put NORMAL dependencies at the front of the list and
					 * AUTO dependencies at the back.  This makes sure that
					 * foreign-key constraints depending on this column will
					 * be dropped before unique or primary-key constraints of
					 * the column; which we must have because the FK
					 * constraints depend on the indexes belonging to the
					 * unique constraints.
					 */
					if (foundDep->deptype == DEPENDENCY_NORMAL)
					{
						tab->changedConstraintOids =
							lcons_oid(foundObject.objectId,
									  tab->changedConstraintOids);
						tab->changedConstraintDefs =
							lcons(defstring,
								  tab->changedConstraintDefs);
					}
					else
					{
						tab->changedConstraintOids =
							lappend_oid(tab->changedConstraintOids,
													   foundObject.objectId);
						tab->changedConstraintDefs =
							lappend(tab->changedConstraintDefs,
									defstring);
				}
				}
				break;

			case OCLASS_REWRITE:
				/* XXX someday see if we can cope with revising views */
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot alter type of a column used by a view or rule"),
						 errdetail("%s depends on column \"%s\"",
								   getObjectDescription(&foundObject),
								   colName),
										   errOmitLocation(true)));
				break;

			case OCLASS_DEFAULT:

				/*
				 * Ignore the column's default expression, since we will fix
				 * it below.
				 */
				Assert(defaultexpr);
				break;

			case OCLASS_PROC:
			case OCLASS_TYPE:
			case OCLASS_CAST:
			case OCLASS_CONVERSION:
			case OCLASS_LANGUAGE:
			case OCLASS_OPERATOR:
			case OCLASS_OPCLASS:
			case OCLASS_TRIGGER:
			case OCLASS_SCHEMA:

				/*
				 * We don't expect any of these sorts of objects to depend on
				 * a column.
				 */
				elog(ERROR, "unexpected object depending on column: %s",
					 getObjectDescription(&foundObject));
				break;

			default:
				elog(ERROR, "unrecognized object class: %u",
					 foundObject.classId);
		}
	}

	caql_endscan(pcqCtx);

	/*
	 * Now scan for dependencies of this column on other things.  The only
	 * thing we should find is the dependency on the column datatype, which we
	 * want to remove.
	 */

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), depRel),
			cql("SELECT * FROM pg_depend "
				" WHERE classid = :1 "
				" AND objid = :2 "
				" AND objsubid = :3 "
				" FOR UPDATE ",
				ObjectIdGetDatum(RelationRelationId),
				ObjectIdGetDatum(RelationGetRelid(rel)),
				Int32GetDatum((int32) attnum)));

	while (HeapTupleIsValid(depTup = caql_getnext(pcqCtx)))
	{
		Form_pg_depend foundDep = (Form_pg_depend) GETSTRUCT(depTup);

		if (foundDep->deptype != DEPENDENCY_NORMAL)
			elog(ERROR, "found unexpected dependency type '%c'",
				 foundDep->deptype);
		if (foundDep->refclassid != TypeRelationId ||
			foundDep->refobjid != attTup->atttypid)
			elog(ERROR, "found unexpected dependency for column");

		caql_delete_current(pcqCtx);
	}

	caql_endscan(pcqCtx);
	heap_close(depRel, RowExclusiveLock);

	policy = rel->rd_cdbpolicy;
	if (policy != NULL && policy->ptype == POLICYTYPE_PARTITIONED &&
		!hashCompatible)
	{
		if (Gp_role != GP_ROLE_EXECUTE)
		{
			int			ia = 0;
			ListCell   *lc;
			List	   *partkeys;
			
			partkeys = rel_partition_key_attrs(rel->rd_id);
			foreach (lc, partkeys)
			{
				if ( attnum == lfirst_int(lc) )
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot alter type of a column used in "
									"a partitioning key"),
							 errOmitLocation(true)));
			}

			if (cdbRelSize(rel) != 0)
			{
				for (ia = 0; ia < policy->nattrs; ia++)
				{
					if (attnum == policy->attrs[ia])
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("cannot alter type of a column used in "
										"a distribution policy"),
								 errOmitLocation(true)));
				}
			}
		}
	}

	/*
	 * Here we go --- change the recorded column type.	(Note heapTup is a
	 * copy of the syscache entry, so okay to scribble on.)
	 */
	attTup->atttypid = targettype;
	attTup->atttypmod = typname->typmod;
	attTup->attndims = list_length(typname->arrayBounds);
	attTup->attlen = tform->typlen;
	attTup->attbyval = tform->typbyval;
	attTup->attalign = tform->typalign;
	attTup->attstorage = tform->typstorage;

	ReleaseType(typeTuple);

	caql_update_current(patCtx, heapTup);/* implicit update of index as well */

	heap_close(attrelation, RowExclusiveLock);

	/* Install dependency on new datatype */
	add_column_datatype_dependency(RelationGetRelid(rel), attnum, targettype);

	/*
	 * Drop any pg_statistic entry for the column, since it's now wrong type
	 */
	RemoveStatistics(RelationGetRelid(rel), attnum);

	/*
	 * Update the default, if present, by brute force --- remove and re-add
	 * the default.  Probably unsafe to take shortcuts, since the new version
	 * may well have additional dependencies.  (It's okay to do this now,
	 * rather than after other ALTER TYPE commands, since the default won't
	 * depend on other column types.)
	 */
	if (defaultexpr)
	{
		/* Must make new row visible since it will be updated again */
		CommandCounterIncrement();

		/*
		 * We use RESTRICT here for safety, but at present we do not expect
		 * anything to depend on the default.
		 */
		RemoveAttrDefault(RelationGetRelid(rel), attnum, DROP_RESTRICT, true);

		StoreAttrDefault(rel, attnum, nodeToString(defaultexpr));
	}

	/* Cleanup */
	heap_freetuple(heapTup);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "ALTER COLUMN TYPE"
				);


}

/*
 * Cleanup after we've finished all the ALTER TYPE operations for a
 * particular relation.  We have to drop and recreate all the indexes
 * and constraints that depend on the altered columns.
 */
static void
ATPostAlterTypeCleanup(List **wqueue, AlteredTableInfo *tab)
{
	ObjectAddress obj;
	ListCell   *def_item;
	ListCell   *oid_item;

	/*
	 * Re-parse the index and constraint definitions, and attach them to the
	 * appropriate work queue entries.	We do this before dropping because in
	 * the case of a FOREIGN KEY constraint, we might not yet have exclusive
	 * lock on the table the constraint is attached to, and we need to get
	 * that before dropping.  It's safe because the parser won't actually look
	 * at the catalogs to detect the existing entry.
	 *
	 * We can't rely on the output of deparsing to tell us which relation
	 * to operate on, because concurrent activity might have made the name
	 * resolve differently.  Instead, we've got to use the OID of the
	 * constraint or index we're processing to figure out which relation
	 * to operate on.
	 */
	forboth(oid_item, tab->changedIndexOids,
			def_item, tab->changedIndexDefs)
	{
		/*
		 * Temporary workaround for MPP-1318. INDEX CREATE is dispatched
		 * immediately, which unfortunately breaks the ALTER work queue.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot alter indexed column"),
						errhint("DROP the index first, and recreate it after the ALTER"),
						errOmitLocation(true)));
		/*Oid     oldId = lfirst_oid(oid_item);
		Oid     relid;

		relid = IndexGetRelation(oldId);
		ATPostAlterTypeParse(relid, InvalidOid,
				(char *) lfirst(def_item),
				wqueue, oldOid);*/
	}

	/* Reuse old constraint OID for new constraint */
	forboth(oid_item, tab->changedConstraintOids,
			def_item, tab->changedConstraintDefs)
	{
		Oid     oldId = lfirst_oid(oid_item);
		Oid     relid;
		Oid     confrelid;

		get_constraint_relation_oids(oldId, &relid, &confrelid);
		ATPostAlterTypeParse(relid, confrelid,
							 (char *) lfirst(def_item),
							 wqueue, oldId);
	}

	/*
	 * Now we can drop the existing constraints and indexes --- constraints
	 * first, since some of them might depend on the indexes.  In fact, we
	 * have to delete FOREIGN KEY constraints before UNIQUE constraints, but
	 * we already ordered the constraint list to ensure that would happen. It
	 * should be okay to use DROP_RESTRICT here, since nothing else should be
	 * depending on these objects.
	 */
	foreach(oid_item, tab->changedConstraintOids)
	{
		obj.classId = ConstraintRelationId;
		obj.objectId = lfirst_oid(oid_item);
		obj.objectSubId = 0;
		performDeletion(&obj, DROP_RESTRICT);
	}

	foreach(oid_item, tab->changedIndexOids)
	{
		obj.classId = RelationRelationId;
		obj.objectId = lfirst_oid(oid_item);
		obj.objectSubId = 0;
		performDeletion(&obj, DROP_RESTRICT);
	}

	/*
	 * The objects will get recreated during subsequent passes over the work
	 * queue.
	 */
}

static void
ATPostAlterTypeParse(Oid oldRelId, Oid refRelId, char *cmd, List **wqueue, Oid constrOid)
{
	List	   *raw_parsetree_list;
	List	   *querytree_list;
	ListCell   *list_item;
	Relation    rel;

	/*
	 * We expect that we only have to do raw parsing and parse analysis, not
	 * any rule rewriting, since these will all be utility statements.
	 */
	raw_parsetree_list = raw_parser(cmd);
	querytree_list = NIL;
	foreach(list_item, raw_parsetree_list)
	{
		Node	   *parsetree = (Node *) lfirst(list_item);

		querytree_list = list_concat(querytree_list,
									 parse_analyze(parsetree, cmd, NULL, 0));
	}

	/* Caller should already have acquired whatever lock we need. */
	rel = relation_open(oldRelId, NoLock);

	/*
	 * Attach each generated command to the proper place in the work queue.
	 * Note this could result in creation of entirely new work-queue entries.
	 */
	foreach(list_item, querytree_list)
	{
		Query	   *query = (Query *) lfirst(list_item);
		AlteredTableInfo *tab;

		Assert(IsA(query, Query));
		Assert(query->commandType == CMD_UTILITY);
		switch (nodeTag(query->utilityStmt))
		{
			case T_IndexStmt:
				{
					IndexStmt  *stmt = (IndexStmt *) query->utilityStmt;
					AlterTableCmd *newcmd;

					tab = ATGetQueueEntry(wqueue, rel);
					newcmd = makeNode(AlterTableCmd);
					newcmd->subtype = AT_ReAddIndex;
					newcmd->def = (Node *) stmt;
					tab->subcmds[AT_PASS_OLD_INDEX] =
						lappend(tab->subcmds[AT_PASS_OLD_INDEX], newcmd);
					break;
				}
			case T_AlterTableStmt:
				{
					AlterTableStmt *stmt = (AlterTableStmt *) query->utilityStmt;
					ListCell   *lcmd;

					tab = ATGetQueueEntry(wqueue, rel);
					foreach(lcmd, stmt->cmds)
					{
						AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lcmd);

						switch (cmd->subtype)
						{
							case AT_AddIndex:
								cmd->subtype = AT_ReAddIndex;
								tab->subcmds[AT_PASS_OLD_INDEX] =
									lappend(tab->subcmds[AT_PASS_OLD_INDEX], cmd);
								break;
							case AT_AddConstraint:
								/* Reuse old constraint OID for new constraint */
								if (nodeTag(cmd->def) == T_Constraint)
									((Constraint *)cmd->def)->conoid = constrOid;
								else if (nodeTag(cmd->def) == T_FkConstraint)
									((FkConstraint *)cmd->def)->constrOid = constrOid;
								tab->subcmds[AT_PASS_OLD_CONSTR] =
									lappend(tab->subcmds[AT_PASS_OLD_CONSTR], cmd);
								break;
							default:
								elog(ERROR, "unexpected statement type: %d",
									 (int) cmd->subtype);
						}
					}
					break;
				}
			default:
				elog(ERROR, "unexpected statement type: %d",
					 (int) nodeTag(query->utilityStmt));
		}
	}

	relation_close(rel, NoLock);
}

/*
 * ALTER TABLE OWNER
 *
 * recursing is true if we are recursing from a table to its indexes,
 * sequences, or toast table.  We don't allow the ownership of those things to
 * be changed separately from the parent table.  Also, we can skip permission
 * checks (this is necessary not just an optimization, else we'd fail to
 * handle toast tables properly).
 *
 * recursing is also true if ALTER TYPE OWNER is calling us to fix up a
 * free-standing composite type.
 */
void
ATExecChangeOwner(Oid relationOid, Oid newOwnerId, bool recursing)
{
	Relation	target_rel;
	HeapTuple	tuple;
	Form_pg_class tuple_class;
	cqContext  *pcqCtx;

	/*
	 * Get exclusive lock till end of transaction on the target table. Use
	 * relation_open so that we can work on indexes and sequences.
	 */
	target_rel = relation_open(relationOid, AccessExclusiveLock);

	/* Get its pg_class tuple, too */
	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(relationOid)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relationOid);
	tuple_class = (Form_pg_class) GETSTRUCT(tuple);

	/* Can we change the ownership of this tuple? */
	switch (tuple_class->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_VIEW:
			/* ok to change owner */
			break;
		case RELKIND_INDEX:
			if (!recursing)
			{
				/*
				 * Because ALTER INDEX OWNER used to be allowed, and in fact
				 * is generated by old versions of pg_dump, we give a warning
				 * and do nothing rather than erroring out.  Also, to avoid
				 * unnecessary chatter while restoring those old dumps, say
				 * nothing at all if the command would be a no-op anyway.
				 */
				if (tuple_class->relowner != newOwnerId)
					ereport(WARNING,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("cannot change owner of index \"%s\"",
									NameStr(tuple_class->relname)),
							 errhint("Change the ownership of the index's table, instead."),
									   errOmitLocation(true)));
				/* quick hack to exit via the no-op path */
				newOwnerId = tuple_class->relowner;
			}
			break;
		case RELKIND_SEQUENCE:
			if (!recursing &&
				tuple_class->relowner != newOwnerId)
			{
				/* if it's an owned sequence, disallow changing it by itself */
				Oid			tableId;
				int32		colId;

				if (sequenceIsOwned(relationOid, &tableId, &colId))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot change owner of sequence \"%s\"",
									NameStr(tuple_class->relname)),
					  errdetail("Sequence \"%s\" is linked to table \"%s\".",
								NameStr(tuple_class->relname),
								get_rel_name(tableId)),
										   errOmitLocation(true)));
			}
			break;
		case RELKIND_TOASTVALUE:
		case RELKIND_AOSEGMENTS:
		case RELKIND_AOBLOCKDIR:
		case RELKIND_COMPOSITE_TYPE:
			if (recursing)
				break;
			/* FALL THRU */
		default:
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is not a table, view, or sequence",
							NameStr(tuple_class->relname)),
									   errOmitLocation(true)));
	}

	/*
	 * If the new owner is the same as the existing owner, consider the
	 * command to have succeeded.  This is for dump restoration purposes.
	 */
	if (tuple_class->relowner != newOwnerId)
	{
		Datum		repl_val[Natts_pg_class];
		bool		repl_null[Natts_pg_class];
		bool		repl_repl[Natts_pg_class];
		Acl		   *newAcl;
		Datum		aclDatum;
		bool		isNull;
		HeapTuple	newtuple;

		/* skip permission checks when recursing to index or toast table */
		if (!recursing)
		{
			/* Superusers can always do it */
			if (!superuser())
			{
				Oid			namespaceOid = tuple_class->relnamespace;
				AclResult	aclresult;

				/* Otherwise, must be owner of the existing object */
				if (!pg_class_ownercheck(relationOid, GetUserId()))
					aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
								   RelationGetRelationName(target_rel));

				/* Must be able to become new owner */
				check_is_member_of_role(GetUserId(), newOwnerId);

				/* New owner must have CREATE privilege on namespace */
				aclresult = pg_namespace_aclcheck(namespaceOid, newOwnerId,
												  ACL_CREATE);
				if (aclresult != ACLCHECK_OK)
					aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
								   get_namespace_name(namespaceOid));
			}
		}

		memset(repl_null, false, sizeof(repl_null));
		memset(repl_repl, false, sizeof(repl_repl));

		repl_repl[Anum_pg_class_relowner - 1] = true;
		repl_val[Anum_pg_class_relowner - 1] = ObjectIdGetDatum(newOwnerId);

		/*
		 * Determine the modified ACL for the new owner.  This is only
		 * necessary when the ACL is non-null.
		 */
		aclDatum = caql_getattr(pcqCtx,
								Anum_pg_class_relacl,
								&isNull);
		if (!isNull)
		{
			newAcl = aclnewowner(DatumGetAclP(aclDatum),
								 tuple_class->relowner, newOwnerId);
			repl_repl[Anum_pg_class_relacl - 1] = true;
			repl_val[Anum_pg_class_relacl - 1] = PointerGetDatum(newAcl);
		}

		newtuple = caql_modify_current(pcqCtx, repl_val, repl_null, repl_repl);

		caql_update_current(pcqCtx, newtuple);
		/* and Update indexes (implicit) */

		heap_freetuple(newtuple);

		/*
		 * Update owner dependency reference, if any.  A composite type has
		 * none, because it's tracked for the pg_type entry instead of here;
		 * indexes don't have their own entries either.
		 */
		if (tuple_class->relkind != RELKIND_COMPOSITE_TYPE &&
			tuple_class->relkind != RELKIND_INDEX &&
			tuple_class->relkind != RELKIND_TOASTVALUE)
			changeDependencyOnOwner(RelationRelationId, relationOid,
									newOwnerId);

		/*
		 * Also change the ownership of the table's rowtype, if it has one
		 */
		if (tuple_class->relkind != RELKIND_INDEX)
			AlterTypeOwnerInternal(tuple_class->reltype, newOwnerId,
							tuple_class->relkind == RELKIND_COMPOSITE_TYPE);

		/*
		 * If we are operating on a table, also change the ownership of any
		 * indexes and sequences that belong to the table, as well as the
		 * table's toast table (if it has one)
		 */
		if (tuple_class->relkind == RELKIND_RELATION ||
			tuple_class->relkind == RELKIND_TOASTVALUE ||
			tuple_class->relkind == RELKIND_AOSEGMENTS ||
			tuple_class->relkind == RELKIND_AOBLOCKDIR)
		{
			List	   *index_oid_list;
			ListCell   *i;

			/* Find all the indexes belonging to this relation */
			index_oid_list = RelationGetIndexList(target_rel);

			/* For each index, recursively change its ownership */
			foreach(i, index_oid_list)
				ATExecChangeOwner(lfirst_oid(i), newOwnerId, true);

			list_free(index_oid_list);
		}

		if (tuple_class->relkind == RELKIND_RELATION)
		{
			/* If it has a toast table, recurse to change its ownership */
			if (tuple_class->reltoastrelid != InvalidOid)
				ATExecChangeOwner(tuple_class->reltoastrelid, newOwnerId,
								  true);

			if(RelationIsAoRows(target_rel) || RelationIsParquet(target_rel))
			{
				Oid segrelid, blkdirrelid;
				GetAppendOnlyEntryAuxOids(relationOid, SnapshotNow,
									  &segrelid, NULL,
									  &blkdirrelid, NULL);
				
				/* If it has an AO segment table, recurse to change its
				 * ownership */
				if (segrelid != InvalidOid)
					ATExecChangeOwner(segrelid, newOwnerId, true);

				/* If it has an AO block directory table, recurse to change its
				 * ownership */
				if (blkdirrelid != InvalidOid)
					ATExecChangeOwner(blkdirrelid, newOwnerId, true);
			}
			
			/* If it has dependent sequences, recurse to change them too */
			change_owner_recurse_to_sequences(relationOid, newOwnerId);
		}
	}

	caql_endscan(pcqCtx);

	relation_close(target_rel, NoLock);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(tuple_class)
			)
		MetaTrackUpdObject(RelationRelationId,
						   relationOid,
						   GetUserId(),
						   "ALTER", "OWNER"
				);

}

/*
 * change_owner_recurse_to_sequences
 *
 * Helper function for ATExecChangeOwner.  Examines pg_depend searching
 * for sequences that are dependent on serial columns, and changes their
 * ownership.
 */
static void
change_owner_recurse_to_sequences(Oid relationOid, Oid newOwnerId)
{
	cqContext  *pcqCtx;
	HeapTuple	tup;

	/*
	 * SERIAL sequences are those having an auto dependency on one of the
	 * table's columns (we don't care *which* column, exactly).
	 */
	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_depend "
				" WHERE refclassid = :1 "
				" AND refobjid = :2 ",
				ObjectIdGetDatum(RelationRelationId),
				ObjectIdGetDatum(relationOid)));
	/* we leave refobjsubid unspecified */

	while (HeapTupleIsValid(tup = caql_getnext(pcqCtx)))
	{
		Form_pg_depend depForm = (Form_pg_depend) GETSTRUCT(tup);
		Relation	seqRel;

		/* skip dependencies other than auto dependencies on columns */
		if (depForm->refobjsubid == 0 ||
			depForm->classid != RelationRelationId ||
			depForm->objsubid != 0 ||
			depForm->deptype != DEPENDENCY_AUTO)
			continue;

		/* Use relation_open just in case it's an index */
		seqRel = relation_open(depForm->objid, AccessExclusiveLock);

		/* skip non-sequence relations */
		if (RelationGetForm(seqRel)->relkind != RELKIND_SEQUENCE)
		{
			/* No need to keep the lock */
			relation_close(seqRel, AccessExclusiveLock);
			continue;
		}

		/* We don't need to close the sequence while we alter it. */
		ATExecChangeOwner(depForm->objid, newOwnerId, true);

		/* Now we can close it.  Keep the lock till end of transaction. */
		relation_close(seqRel, NoLock);
	}

	caql_endscan(pcqCtx);

}

/*
 * ALTER TABLE CLUSTER ON
 *
 * The only thing we have to do is to change the indisclustered bits.
 */
static void
ATExecClusterOn(Relation rel, const char *indexName)
{
	Oid			indexOid;

	indexOid = get_relname_relid(indexName, rel->rd_rel->relnamespace);

	if (!OidIsValid(indexOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("index \"%s\" for table \"%s\" does not exist",
						indexName, RelationGetRelationName(rel)),
								   errOmitLocation(true)));

	/* Check index is valid to cluster on */
	check_index_is_clusterable(rel, indexOid, false);

	/* And do the work */
	mark_index_clustered(rel, indexOid);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "CLUSTER ON"
				);
}

/*
 * ALTER TABLE SET WITHOUT CLUSTER
 *
 * We have to find any indexes on the table that have indisclustered bit
 * set and turn it off.
 */
static void
ATExecDropCluster(Relation rel)
{
	mark_index_clustered(rel, InvalidOid);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "SET WITHOUT CLUSTER"
				);

}

/*
 * ALTER TABLE SET TABLESPACE
 */

/* 
 * Convert tablespace name to pg_tablespace Oid.  Error out if not valid and
 * settable by the current user.
 */
Oid
get_settable_tablespace_oid(char *tablespacename)
{
	Oid			tablespaceId;
	AclResult	aclresult;
	
	/* Check that the tablespace exists */
	tablespaceId = get_tablespace_oid(tablespacename);
	if (!OidIsValid(tablespaceId))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("tablespace \"%s\" does not exist", tablespacename)));
	
	/* Check its permissions */
	aclresult = pg_tablespace_aclcheck(tablespaceId, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_TABLESPACE, tablespacename);
	
	return tablespaceId;
}

static void
ATPrepSetTableSpace(AlteredTableInfo *tab, Relation rel, char *tablespacename)
{
	Oid			tablespaceId;

	tablespaceId = get_settable_tablespace_oid(tablespacename);

	/* Save info for Phase 3 to do the real work */
	if (OidIsValid(tab->newTableSpace))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cannot have multiple SET TABLESPACE subcommands"),
						   errOmitLocation(true)));
	tab->newTableSpace = tablespaceId;
}

/* 
 * ATPartsPrepSetTableSpace is like ATPrepSetTableSpace except that it generates work queue
 * entries for the command (an ALTER TABLE ... SET TABLESPACE ...)  for each part within the 
 * sub-hierarchy indicated by the oid list. 
 *
 * Designed to be called from the AT_PartAlter case of ATPrepCmd.
 */
static void
ATPartsPrepSetTableSpace(List **wqueue, Relation rel, AlterTableCmd *cmd, List *oids)
{
	ListCell *lc;
	Oid tablespaceId;
	int pass = AT_PASS_MISC;
	
	Assert( cmd && cmd->subtype == AT_SetTableSpace );
	Assert( oids );
	
	tablespaceId = get_settable_tablespace_oid(cmd->name);
	Assert(tablespaceId);
	
	ereport(DEBUG1,
			(errmsg("Expanding ALTER TABLE %s SET TABLESPACE...", 
					RelationGetRelationName(rel))
			 ));
	
	foreach(lc, oids)
	{
		Oid partrelid = lfirst_oid(lc);
		Relation partrel = relation_open(partrelid, NoLock); 
		/* NoLock because we should be holding AccessExclusiveLock on parent */
		AlterTableCmd *partcmd = copyObject(cmd);
		AlteredTableInfo *parttab = ATGetQueueEntry(wqueue, partrel);
		if( OidIsValid(parttab->newTableSpace) )
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("conflicting SET TABLESPACE subcommands for \"%s\"",
							RelationGetRelationName(partrel)),
					 errOmitLocation(true)));
		
		parttab->newTableSpace = tablespaceId;
		parttab->subcmds[pass] = lappend(parttab->subcmds[pass], partcmd);
		
		ereport(DEBUG1,
				(errmsg("Will SET TABLESPACE on \"%s\"", 
						RelationGetRelationName(partrel))));
		
		relation_close(partrel, NoLock);
	}
}

/*
 * ALTER TABLE/INDEX SET (...) or RESET (...)
 */
static void
ATExecSetRelOptions(Relation rel, List *defList, bool isReset)
{
	Oid			relid;
	HeapTuple	tuple;
	HeapTuple	newtuple;
	Datum		datum;
	bool		isnull;
	Datum		newOptions;
	Datum		repl_val[Natts_pg_class];
	bool		repl_null[Natts_pg_class];
	bool		repl_repl[Natts_pg_class];
	cqContext  *pcqCtx;

	if (defList == NIL)
		return;					/* nothing to do */

	/* Get the old reloptions */
	relid = RelationGetRelid(rel);

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(relid)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	datum = caql_getattr(pcqCtx, Anum_pg_class_reloptions, &isnull);

	/* MPP-5777: disallow all options except fillfactor.
	 * Future work: could convert from SET to SET WITH codepath which
	 * can support additional reloption types
	 */
	if ((defList != NIL)
/*		&& ((rel->rd_rel->relkind == RELKIND_RELATION)
		|| (rel->rd_rel->relkind == RELKIND_TOASTVALUE)) */
			)
	{
		ListCell   *cell;

		foreach(cell, defList)
		{
			DefElem    *def = lfirst(cell);
			int			kw_len = strlen(def->defname);
			char	   *text_str = "fillfactor";
			int			text_len = strlen(text_str);

			if ((text_len != kw_len) ||
				(pg_strncasecmp(text_str, def->defname, kw_len) != 0))
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("cannot SET reloption \"%s\"",
								def->defname)));

		}
	}


	/* Generate new proposed reloptions (text array) */
	newOptions = transformRelOptions(isnull ? (Datum) 0 : datum,
									 defList, false, isReset);

	/* Validate */
	switch (rel->rd_rel->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_TOASTVALUE:
		case RELKIND_AOSEGMENTS:
		case RELKIND_AOBLOCKDIR:
			if(RelationIsAoRows(rel) || RelationIsParquet(rel))
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("altering reloptions for append only tables"
								" is not permitted"),
										   errOmitLocation(true)));

			(void) heap_reloptions(rel->rd_rel->relkind, newOptions, true);
			break;
		case RELKIND_INDEX:
			(void) index_reloptions(rel->rd_am->amoptions, newOptions, true);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is not a table, index, or TOAST table",
							RelationGetRelationName(rel)),
									   errOmitLocation(true)));
			break;
	}

	/*
	 * All we need do here is update the pg_class row; the new options will be
	 * propagated into relcaches during post-commit cache inval.
	 */
	memset(repl_val, 0, sizeof(repl_val));
	memset(repl_null, false, sizeof(repl_null));
	memset(repl_repl, false, sizeof(repl_repl));

	if (newOptions != (Datum) 0)
		repl_val[Anum_pg_class_reloptions - 1] = newOptions;
	else
		repl_null[Anum_pg_class_reloptions - 1] = true;

	repl_repl[Anum_pg_class_reloptions - 1] = true;

	newtuple = caql_modify_current(pcqCtx,
								   repl_val, repl_null, repl_repl);

	caql_update_current(pcqCtx, newtuple); 
	/* and Update indexes (implicit) */

	heap_freetuple(newtuple);

	caql_endscan(pcqCtx);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", isReset ? "RESET" : "SET" 
				);


}

static void 
copy_append_only_data(
	RelFileNode		*oldRelFileNode,
	
	RelFileNode		*newRelFileNode,
	
	int32			segmentFileNum,

	char			*relationName,

	int64			eof,

	ItemPointer		persistentTid,
	
	int64			persistentSerialNum,
	
	char			*buffer)
{
	char srcFileName[MAXPGPATH];
	char dstFileName[MAXPGPATH];
	char extension[12];

	File		srcFile;

	MirroredAppendOnlyOpen mirroredDstOpen;

	int64	endOffset;
	int64	readOffset;
	int32	bufferLen;
	int 	retval;

	int 		primaryError;

	/*bool mirrorDataLossOccurred;*/

	/*bool mirrorCatchupRequired;

	MirrorDataLossTrackingState 	originalMirrorDataLossTrackingState;
	int64 							originalMirrorDataLossTrackingSessionNum;*/

	if (segmentFileNum > 0)
	{
		sprintf(extension, ".%u", segmentFileNum);
	}
	else
		extension[0] = '\0';

	CopyRelPath(srcFileName, MAXPGPATH, *oldRelFileNode);
	if (segmentFileNum > 0)
	{
		strcat(srcFileName, extension);
	}

	/*
	 * Open the files
	 */
	srcFile = PathNameOpenFile(srcFileName, O_RDONLY | PG_BINARY, 0);
	if (srcFile < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", srcFileName),
				 errdetail("%s", HdfsGetLastError())));


	CopyRelPath(dstFileName, MAXPGPATH, *newRelFileNode);
	if (segmentFileNum > 0)
	{
		strcat(dstFileName, extension);
	}

	MirroredAppendOnly_OpenReadWrite(
								&mirroredDstOpen,
								newRelFileNode,
								segmentFileNum,
								relationName,
								/* logicalEof */ 0, // NOTE: This is the START EOF.  Since we are copying, we start at 0.
								true,
								&primaryError);
	if (primaryError != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\" for relation '%s': %s", dstFileName, relationName, strerror(primaryError)),
				 errdetail("%s", HdfsGetLastError())));
	
	/*
	 * Do the data copying.
	 */
	endOffset = eof;
	readOffset = 0;
	bufferLen = (Size) Min(2*BLCKSZ, endOffset);
	while (readOffset < endOffset)
	{
		CHECK_FOR_INTERRUPTS();
		
		retval = FileRead(srcFile, buffer, bufferLen);
		if (retval != bufferLen) 
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from position: " INT64_FORMAT " in file '%s' : %m", readOffset, srcFileName),
					 errdetail("%s", HdfsGetLastError())));
			
			break;
		}						
		
		MirroredAppendOnly_Append(
							  &mirroredDstOpen,
							  buffer,
							  bufferLen,
							  &primaryError/*,
							  &mirrorDataLossOccurred*/);
		if (primaryError != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %s", dstFileName, strerror(primaryError)),
					 errdetail("%s", HdfsGetLastError())));
		
		readOffset += bufferLen;
		
		bufferLen = (Size) Min(2*BLCKSZ, endOffset - readOffset); 					
	}
	
	MirroredAppendOnly_FlushAndClose(
							&mirroredDstOpen,
							&primaryError
							/*&mirrorDataLossOccurred,
							&mirrorCatchupRequired,
							&originalMirrorDataLossTrackingState,
							&originalMirrorDataLossTrackingSessionNum*/);
	if (primaryError != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not flush (fsync) file \"%s\" for relation '%s': %s"
						 , dstFileName, relationName, strerror(primaryError)),
				 errdetail("%s", HdfsGetLastError())));

	FileClose(srcFile);

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
			 "ALTER TABLE SET TABLESPACE: Copied Append-Only segment file #%d EOF " INT64_FORMAT,
			 segmentFileNum,
			 eof);
}

static void
ATExecSetTableSpace_AppendOnly(
	Oid				tableOid,
	Relation		rel,
	Relation		gp_relfile_node,
	RelFileNode		*newRelFileNode)
{
	Oid			oldTablespace;

	char *buffer;

	GpRelfileNodeScan 	gpRelfileNodeScan;

	HeapTuple tuple;

	int32 segmentFileNum;

	ItemPointerData oldPersistentTid;
	int64 oldPersistentSerialNum;

	ItemPointerData newPersistentTid;
	int64 newPersistentSerialNum;

	HeapTuple tupleCopy;

	AppendOnlyEntry *aoEntry;

	Snapshot	appendOnlyMetaDataSnapshot = SnapshotNow;
				/*
				 * We can use SnapshotNow since we have an exclusive lock on the source.
				 */
					
	int segmentCount;

	oldTablespace = rel->rd_rel->reltablespace ? rel->rd_rel->reltablespace : MyDatabaseTableSpace;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
			 "ALTER TABLE SET TABLESPACE: Append-Only "
			 "relation id %u, old path %u/%u/%u, new path %u/%u/%u",
			 RelationGetRelid(rel),
			 rel->rd_node.spcNode,
			 rel->rd_node.dbNode,
			 rel->rd_node.relNode,
			 newRelFileNode->spcNode,
			 newRelFileNode->dbNode,
			 newRelFileNode->relNode);

	/* Use palloc to ensure we get a maxaligned buffer */		
	buffer = palloc(2*BLCKSZ);

	aoEntry = GetAppendOnlyEntry(RelationGetRelid(rel), appendOnlyMetaDataSnapshot);

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
			 "ALTER TABLE SET TABLESPACE: pg_appendonly entry for Append-Only %u/%u/%u, segment file #%d "
			 "segrelid %u, segidxid %u, "
			 "persistent TID %s and serial number " INT64_FORMAT,
			 rel->rd_node.spcNode,
			 rel->rd_node.dbNode,
			 rel->rd_node.relNode,
			 segmentFileNum,
			 aoEntry->segrelid,
			 aoEntry->segidxid,
			 ItemPointerToString(&oldPersistentTid),
			 oldPersistentSerialNum);	

	/*
	 * Loop through segment files
	 *    Create segment file in new tablespace under transaction,
	 *    Copy Append-Only segment file data and fsync,
	 *    Update old gp_relation_node with new relfilenode and persistent information,
	 *    Schedule old segment file for drop under transaction,
	 */
	GpRelfileNodeBeginScan(
					gp_relfile_node,
					tableOid,
					rel->rd_rel->relfilenode,
					&gpRelfileNodeScan);
	segmentCount = 0;
	while ((tuple = GpRelfileNodeGetNext(
							&gpRelfileNodeScan,
							&segmentFileNum,
							&oldPersistentTid,
							&oldPersistentSerialNum)))
	{
		int64 eof = 0;
		/*
		 * Create segment file in new tablespace under transaction.
		 */

		MirroredFileSysObj_TransactionCreateAppendOnlyFile(
											newRelFileNode,
											segmentFileNum,
											rel->rd_rel->relname.data,
											/* doJustInTimeDirCreate */ true,
											&newPersistentTid,
											&newPersistentSerialNum);

		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
				 "ALTER TABLE SET TABLESPACE: Create for Append-Only %u/%u/%u, segment file #%d "
				 "persistent TID %s and serial number " INT64_FORMAT,
				 newRelFileNode->spcNode,
				 newRelFileNode->dbNode,
				 newRelFileNode->relNode,
				 segmentFileNum,
				 ItemPointerToString(&newPersistentTid),
				 newPersistentSerialNum);


		/*
		 * Update gp_relation_node with new persistent information.
		 */
		tupleCopy = heap_copytuple(tuple);

		UpdateGpRelfileNodeTuple(
							gp_relfile_node,
							tupleCopy,
							newRelFileNode->relNode,
							segmentFileNum,
							&newPersistentTid,
							newPersistentSerialNum);

		/*
		 * Schedule old segment file for drop under transaction.
		 */
		MirroredFileSysObj_ScheduleDropAppendOnlyFile(
											&rel->rd_node,
											segmentFileNum,
											rel->rd_rel->relname.data,
											&oldPersistentTid,
											oldPersistentSerialNum);

		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
				 "ALTER TABLE SET TABLESPACE: Drop for Append-Only %u/%u/%u, segment file #%d "
				 "persistent TID %s and serial number " INT64_FORMAT,
				 rel->rd_node.spcNode,
				 rel->rd_node.dbNode,
				 rel->rd_node.relNode,
				 segmentFileNum,
				 ItemPointerToString(&oldPersistentTid),
				 oldPersistentSerialNum);

		/*
		 * Copy Append-Only segment file data and fsync.
		 */
		if (RelationIsAoRows(rel))
		{
			FileSegInfo *fileSegInfo;

			fileSegInfo = GetFileSegInfo(rel, aoEntry, appendOnlyMetaDataSnapshot, segmentFileNum);
			if (fileSegInfo == NULL)
			{
				/*
				 * Segment file #0 is special and can exist without an entry.
				 */
				if (segmentFileNum == 0)
					eof = 0;
				else
					ereport(ERROR,
							(errmsg("Append-only %u/%u/%u row segment file #%d information missing",
									rel->rd_node.spcNode,
									rel->rd_node.dbNode,
									rel->rd_node.relNode,
									segmentFileNum)));

			}
			else
			{
				eof = fileSegInfo->eof;

				pfree(fileSegInfo);
			}
		}

		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
				 "ALTER TABLE SET TABLESPACE: Going to copy Append-Only %u/%u/%u to %u/%u/%u, segment file #%d, EOF " INT64_FORMAT ", "
				 "persistent TID %s and serial number " INT64_FORMAT,
				 rel->rd_node.spcNode,
				 rel->rd_node.dbNode,
				 rel->rd_node.relNode,
				 newRelFileNode->spcNode,
				 newRelFileNode->dbNode,
				 newRelFileNode->relNode,
				 segmentFileNum,
				 eof,
				 ItemPointerToString(&newPersistentTid),
				 newPersistentSerialNum);
		
		copy_append_only_data(
						&rel->rd_node,
						newRelFileNode,
						segmentFileNum,
						rel->rd_rel->relname.data,
						eof,
						&newPersistentTid,
						newPersistentSerialNum,
						buffer);

		segmentCount++;
	}

	GpRelfileNodeEndScan(&gpRelfileNodeScan);

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
			 "ALTER TABLE SET TABLESPACE: Copied %d Append-Only segments from %u/%u/%u to %u/%u/%u, "
			 "persistent TID %s and serial number " INT64_FORMAT,
			 segmentCount,
			 rel->rd_node.spcNode,
			 rel->rd_node.dbNode,
			 rel->rd_node.relNode,
			 newRelFileNode->spcNode,
			 newRelFileNode->dbNode,
			 newRelFileNode->relNode,
			 ItemPointerToString(&newPersistentTid),
			 newPersistentSerialNum);

	pfree(aoEntry);

	pfree(buffer);
}

static void
ATExecSetTableSpace_BufferPool(
	Oid				tableOid,
	Relation		rel,
	Relation		gp_relfile_node,
	RelFileNode		*newRelFileNode)
{
	Oid			oldTablespace;
	HeapTuple	nodeTuple;
	ItemPointerData newPersistentTid;
	int64 newPersistentSerialNum;
	SMgrRelation dstrel;
	bool useWal;
	
	PersistentFileSysRelStorageMgr localRelStorageMgr;
	PersistentFileSysRelBufpoolKind relBufpoolKind;
	
	oldTablespace = rel->rd_rel->reltablespace ? rel->rd_rel->reltablespace : MyDatabaseTableSpace;

	/*
	 * We need to log the copied data in WAL enabled AND
	 * it's not a temp rel.
	 */
	useWal = !XLog_CanBypassWal() && !rel->rd_istemp;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
			 "ALTER TABLE SET TABLESPACE: Buffer Pool managed "
			 "relation id %u, old path '%s', old relfilenode %u, old reltablespace %u, "
			 "new path '%s', new relfilenode %u, new reltablespace %u, "
			 "bulk load %s",
			 RelationGetRelid(rel),
			 relpath(rel->rd_node),
			 rel->rd_rel->relfilenode,
			 oldTablespace,
			 relpath(*newRelFileNode),
			 newRelFileNode->relNode,
			 newRelFileNode->spcNode,
			 (!useWal ? "true" : "false"));
	
	/* Fetch relation's gp_relfile_node row */
	/* TODO in hawq */
	Assert(!"need contentid");
	nodeTuple = ScanGpRelfileNodeTuple(
							gp_relfile_node,
							rel->rd_rel->relfilenode,
							/* segmentFileNum */ 0);
	if (!HeapTupleIsValid(nodeTuple))
		elog(ERROR, "cache lookup failed for relation %u, tablespace %u, relation file node %u", 
		     tableOid,
		     oldTablespace,
		     rel->rd_rel->relfilenode);

	GpPersistentRelfileNode_GetRelfileInfo(
										rel->rd_rel->relkind,
										rel->rd_rel->relstorage,
										rel->rd_rel->relam,
										&localRelStorageMgr,
										&relBufpoolKind);
	Assert(localRelStorageMgr == PersistentFileSysRelStorageMgr_BufferPool);
	
	dstrel = smgropen(*newRelFileNode);
	MirroredFileSysObj_TransactionCreateBufferPoolFile(
											dstrel,
											relBufpoolKind,
											rel->rd_isLocalBuf,
											rel->rd_rel->relname.data,
											/* doJustInTimeDirCreate */ true,
											/* bufferPoolBulkLoad */ !useWal,
											&newPersistentTid,
											&newPersistentSerialNum);

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
		     "ALTER TABLE SET TABLESPACE: Create for Buffer Pool managed '%s' "
			 "persistent TID %s and serial number " INT64_FORMAT,
			 relpath(*newRelFileNode),
			 ItemPointerToString(&newPersistentTid),
			 newPersistentSerialNum);

	/* copy relation data to the new physical file */
	copy_buffer_pool_data(
					rel, 
					dstrel,
					&newPersistentTid,
					newPersistentSerialNum,
					useWal);

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
			 "ALTER TABLE SET TABLESPACE: Scheduling drop for '%s' "
			 "persistent TID %s and serial number " INT64_FORMAT,
			 relpath(*newRelFileNode),
			 ItemPointerToString(&rel->rd_relationnodeinfo.persistentTid),
			 rel->rd_relationnodeinfo.persistentSerialNum);


	/* schedule unlinking old physical file */
	MirroredFileSysObj_ScheduleDropBufferPoolRel(rel);

	/*
	 * Now drop smgr references.  The source was already dropped by
	 * smgrscheduleunlink.
	 */
	smgrclose(dstrel);

	/* Update gp_relfile_node row. */
	UpdateGpRelfileNodeTuple(
						gp_relfile_node,
						nodeTuple,
						newRelFileNode->relNode,
						/* segmentFileNum */ 0,
						&newPersistentTid,
						newPersistentSerialNum);

}

static bool
ATExecSetTableSpace_Relation(Oid tableOid, Oid newTableSpace, Oid newrelfilenode)
{
	Relation    rel;
	Relation	pg_class;
	Oid			oldTableSpace;
	HeapTuple	tuple;
	Form_pg_class rd_rel;
	Relation	gp_relfile_node;
	RelFileNode newrnode;
	cqContext	cqc;
	cqContext  *pcqCtx;

	rel = relation_open(tableOid, AccessExclusiveLock);

	/*
	 * We can never allow moving of shared or nailed-in-cache relations,
	 * because we can't support changing their reltablespace values.
	 */
	if (rel->rd_rel->relisshared || rel->rd_isnailed)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot move system relation \"%s\"",
						RelationGetRelationName(rel)),
								   errOmitLocation(true)));

	/*
	 * Don't allow moving temp tables of other backends ... their local buffer
	 * manager is not going to cope.
	 */
	if (isOtherTempNamespace(RelationGetNamespace(rel)))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot move temporary tables of other sessions"),
						   errOmitLocation(true)));

	/*
	 * No work if no change in tablespace.
	 */
	oldTableSpace = rel->rd_rel->reltablespace;
	if (newTableSpace == oldTableSpace ||
		(newTableSpace == MyDatabaseTableSpace && oldTableSpace == 0))
	{
		/* XXX - Why do we hold the lock if we aren't changing anything? */
		relation_close(rel, NoLock);
		return false;
	}

	/* Fetch relation's pg_class row */
	pg_class = heap_open(RelationRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), pg_class);

	tuple = caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(tableOid)));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", tableOid);
	rd_rel = (Form_pg_class) GETSTRUCT(tuple);

	gp_relfile_node = heap_open(GpRelfileNodeRelationId, RowExclusiveLock);

	/* create another storage file. Is it a little ugly ? */
	/* NOTE: any conflict in relfilenode value will be caught here */
	newrnode = rel->rd_node;
	newrnode.relNode = newrelfilenode;
	newrnode.spcNode = newTableSpace;

	/* update the pg_class row */
	rd_rel->reltablespace = (newTableSpace == MyDatabaseTableSpace) ? InvalidOid : newTableSpace;
	rd_rel->relfilenode = newrelfilenode;
	
	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
			 "ALTER TABLE SET TABLESPACE: Update pg_class for relation id %u --> relfilenode to %u, reltablespace to %u",
			 tableOid,
			 rd_rel->relfilenode,
			 rd_rel->reltablespace);

	caql_update_current(pcqCtx, tuple); /* implicit update of index as well */

	heap_freetuple(tuple);

	if (RelationIsAoRows(rel) || RelationIsParquet(rel))
		ATExecSetTableSpace_AppendOnly(
								tableOid,
								rel, 
								gp_relfile_node,
								&newrnode);
	else
		ATExecSetTableSpace_BufferPool(
								tableOid,
								rel, 
								gp_relfile_node,
								&newrnode);


	heap_close(pg_class, RowExclusiveLock);

	heap_close(gp_relfile_node, RowExclusiveLock);

	/* Make sure the reltablespace change is visible */
	CommandCounterIncrement();

	/* Hold the lock until commit */
	relation_close(rel, NoLock);

	return true;
}

/*
 * Execute ALTER TABLE SET TABLESPACE for cases where there is no tuple
 * rewriting to be done, so we just want to copy the data as fast as possible.
 */
static void
ATExecSetTableSpace(Oid tableOid, Oid newTableSpace, TableOidInfo *oidInfo)
{
	Relation	rel;
	Oid         newrelfilenode;
	Oid			reltoastrelid = InvalidOid;
	Oid			reltoastidxid = InvalidOid;
	Oid			relaosegrelid = InvalidOid;
	Oid			relaosegidxid = InvalidOid;
	Oid			relaoblkdirrelid = InvalidOid;
	Oid			relaoblkdiridxid = InvalidOid;
	Oid			relbmrelid = InvalidOid;
	Oid			relbmidxid = InvalidOid;


	/* Ensure valid input */
	Assert(OidIsValid(tableOid));
	Assert(OidIsValid(newTableSpace));
	Assert(oidInfo);

	newrelfilenode = oidInfo->relOid;

	/*
	 * Need lock here in case we are recursing to toast table or index
	 */
	rel = relation_open(tableOid, AccessExclusiveLock);

	reltoastrelid = rel->rd_rel->reltoastrelid;

	/* Find the toast relation index */
	if (reltoastrelid)
	{
		Relation toastrel;

		toastrel = relation_open(reltoastrelid, NoLock);
		reltoastidxid = toastrel->rd_rel->reltoastidxid;
		relation_close(toastrel, NoLock);
	}

	/* Get the ao sub objects */
	if (RelationIsAoRows(rel))
		GetAppendOnlyEntryAuxOids(tableOid, SnapshotNow,
								  &relaosegrelid, &relaosegidxid,
								  &relaoblkdirrelid, &relaoblkdiridxid);

	/* Get the bitmap sub objects */
	if (RelationIsBitmapIndex(rel))
		GetBitmapIndexAuxOids(rel, &relbmrelid, &relbmidxid);

	/* Move the main table */
	if (!ATExecSetTableSpace_Relation(tableOid, newTableSpace, newrelfilenode))
	{
		/* XXX - Why do we hold the lock if we aren't changing anything? */
		relation_close(rel, NoLock);
		return;
	}

	/* Move associated toast/toast_index/ao subobjects */
	if (OidIsValid(reltoastrelid))
		ATExecSetTableSpace_Relation(reltoastrelid, newTableSpace, 
									 oidInfo->toastOid);
	if (OidIsValid(reltoastidxid))
		ATExecSetTableSpace_Relation(reltoastidxid, newTableSpace,
									 oidInfo->toastIndexOid);
	if (OidIsValid(relaosegrelid))
		ATExecSetTableSpace_Relation(relaosegrelid, newTableSpace,
									 oidInfo->aosegOid);
	if (OidIsValid(relaosegidxid))
		ATExecSetTableSpace_Relation(relaosegidxid, newTableSpace,
									 oidInfo->aosegIndexOid);
	if (OidIsValid(relaoblkdirrelid))
		ATExecSetTableSpace_Relation(relaoblkdirrelid, newTableSpace,
									 oidInfo->aoblkdirOid);
	if (OidIsValid(relaoblkdiridxid))
		ATExecSetTableSpace_Relation(relaoblkdiridxid, newTableSpace,
									 oidInfo->aoblkdirIndexOid);

	/* 
	 * MPP-7996 - bitmap index subobjects w/Alter Table Set tablespace
	 *
	 * Minor hack: Bitmap indexes are never AO tables.  Rather than extending
	 * OidInfo with yet more oids we simply overload aosegOid/aosegIndexOid
	 * to serve double purpose as oids for the bitmap subobjects.
	 */
	if (OidIsValid(relbmrelid))
	{
		Assert(!relaosegrelid);
		ATExecSetTableSpace_Relation(relbmrelid, newTableSpace,
									 oidInfo->aosegOid);
	}
	if (OidIsValid(relbmidxid))
	{
		Assert(!relaosegidxid);
		ATExecSetTableSpace_Relation(relbmidxid, newTableSpace,
									 oidInfo->aosegIndexOid);
	}

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH) && MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "SET TABLESPACE"
			);

	/* Hold the lock until commit */
	relation_close(rel, NoLock);
}

/*
 * Copy data, block by block
 */
static void
copy_buffer_pool_data(
	Relation 	rel,

	SMgrRelation dst,

	ItemPointer persistentTid,

	int64 		persistentSerialNum,

	bool		useWal)
{
	SMgrRelation src;
	BlockNumber nblocks;
	BlockNumber blkno;
	char		buf[BLCKSZ];
	Page		page = (Page) buf;


	/*
	 * Since we copy the file directly without looking at the shared buffers,
	 * we'd better first flush out any pages of the source relation that are
	 * in shared buffers.  We assume no new changes will be made while we are
	 * holding exclusive lock on the rel.
	 */
	FlushRelationBuffers(rel);

	nblocks = RelationGetNumberOfBlocks(rel);

	/* RelationGetNumberOfBlocks will certainly have opened rd_smgr */
	src = rel->rd_smgr;

	if (useWal)
	{
		if (Debug_persistent_print)
		{
			elog(Persistent_DebugPrintLevel(),
				 "copy_buffer_pool_data %u/%u/%u: not bypassing the WAL -- not using bulk load, persistent serial num " INT64_FORMAT ", TID %s",
				 rel->rd_node.spcNode,
				 rel->rd_node.dbNode,
				 rel->rd_node.relNode,
				 persistentSerialNum,
				 ItemPointerToString(persistentTid));
		}
	}
	
	for (blkno = 0; blkno < nblocks; blkno++)
	{
		smgrread(src, blkno, buf);

		/* XLOG stuff */
		if (useWal)
		{
			xl_heap_newpage xlrec;
			XLogRecPtr	recptr;
			XLogRecData rdata[2];

			/* NO ELOG(ERROR) from here till newpage op is logged */
			START_CRIT_SECTION();

			xlrec.heapnode.node = dst->smgr_rnode;
			xlrec.heapnode.persistentTid = *persistentTid;
			xlrec.heapnode.persistentSerialNum = persistentSerialNum;
			xlrec.blkno = blkno;

			rdata[0].data = (char *) &xlrec;
			rdata[0].len = SizeOfHeapNewpage;
			rdata[0].buffer = InvalidBuffer;
			rdata[0].next = &(rdata[1]);

			rdata[1].data = (char *) page;
			rdata[1].len = BLCKSZ;
			rdata[1].buffer = InvalidBuffer;
			rdata[1].next = NULL;

			recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_NEWPAGE, rdata);

			PageSetLSN(page, recptr);
			PageSetTLI(page, ThisTimeLineID);

			END_CRIT_SECTION();
		}

		
		// -------- MirroredLock ----------
		LWLockAcquire(MirroredLock, LW_SHARED);
		
		/*
		 * Now write the page.	We say isTemp = true even if it's not a temp
		 * rel, because there's no need for smgr to schedule an fsync for this
		 * write; we'll do it ourselves below.
		 */
		smgrwrite(dst, blkno, buf, true);
		
		LWLockRelease(MirroredLock);
		// -------- MirroredLock ----------
	}

	/*
	 * If the rel isn't temp, we must fsync it down to disk before it's safe
	 * to commit the transaction.  (For a temp rel we don't care since the rel
	 * will be uninteresting after a crash anyway.)
	 *
	 * It's obvious that we must do this when not WAL-logging the copy. It's
	 * less obvious that we have to do it even if we did WAL-log the copied
	 * pages. The reason is that since we're copying outside shared buffers, a
	 * CHECKPOINT occurring during the copy has no way to flush the previously
	 * written data to disk (indeed it won't know the new rel even exists).  A
	 * crash later on would replay WAL from the checkpoint, therefore it
	 * wouldn't replay our earlier WAL entries. If we do not fsync those pages
	 * here, they might still not be on disk when the crash occurs.
	 */
	
	// -------- MirroredLock ----------
	LWLockAcquire(MirroredLock, LW_SHARED);
	
	if (!rel->rd_istemp)
		smgrimmedsync(dst);
	
	LWLockRelease(MirroredLock);
	// -------- MirroredLock ----------

	if (useWal)
	{
		if (Debug_persistent_print)
		{
			elog(Persistent_DebugPrintLevel(),
				 "copy_buffer_pool_data %u/%u/%u: did not bypass the WAL -- did not use bulk load, persistent serial num " INT64_FORMAT ", TID %s",
				 rel->rd_node.spcNode,
				 rel->rd_node.dbNode,
				 rel->rd_node.relNode,
				 persistentSerialNum,
				 ItemPointerToString(persistentTid));
		}
	}
}

/*
 * ALTER TABLE ENABLE/DISABLE TRIGGER
 *
 * We just pass this off to trigger.c.
 */
static void
ATExecEnableDisableTrigger(Relation rel, char *trigname,
						   bool enable, bool skip_system)
{
	EnableDisableTrigger(rel, trigname, enable, skip_system);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", 
						   enable ? "ENABLE TRIGGER" : 
						   "DISABLE TRIGGER"
				);

}

static void
inherit_parent(Relation parent_rel, Relation child_rel, bool is_partition, List *inhAttrNameList)
{
	cqContext  *pcqCtx;
	cqContext	cqc;
	HeapTuple	inheritsTuple;
	int32		inhseqno;
	List	   *children;
	Relation	catalogRelation;

	/*
	 * Check for duplicates in the list of parents, and determine the highest
	 * inhseqno already present; we'll use the next one for the new parent.
	 * (Note: get RowExclusiveLock because we will write pg_inherits below.)
	 *
	 * Note: we do not reject the case where the child already inherits from
	 * the parent indirectly; CREATE TABLE doesn't reject comparable cases.
	 */
	catalogRelation = heap_open(InheritsRelationId, RowExclusiveLock);
	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), catalogRelation),
			cql("SELECT * FROM pg_inherits "
				" WHERE inhrelid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(RelationGetRelid(child_rel))));

	/* inhseqno sequences start at 1 */
	inhseqno = 0;
	while (HeapTupleIsValid(inheritsTuple = caql_getnext(pcqCtx)))
	{
		Form_pg_inherits inh = (Form_pg_inherits) GETSTRUCT(inheritsTuple);

		if (inh->inhparent == RelationGetRelid(parent_rel))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("inherited relation \"%s\" duplicated",
							RelationGetRelationName(parent_rel)),
									   errOmitLocation(true)));
		if (inh->inhseqno > inhseqno)
			inhseqno = inh->inhseqno;
	}
	caql_endscan(pcqCtx);

	/*
	 * Prevent circularity by seeing if proposed parent inherits from child.
	 * (In particular, this disallows making a rel inherit from itself.)
	 *
	 * This is not completely bulletproof because of race conditions: in
	 * multi-level inheritance trees, someone else could concurrently
	 * be making another inheritance link that closes the loop but does
	 * not join either of the rels we have locked.  Preventing that seems
	 * to require exclusive locks on the entire inheritance tree, which is
	 * a cure worse than the disease.  find_all_inheritors() will cope with
	 * circularity anyway, so don't sweat it too much.
	 */
	children = find_all_inheritors(RelationGetRelid(child_rel));

	if (list_member_oid(children, RelationGetRelid(parent_rel)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("circular inheritance not allowed"),
				 errdetail("\"%s\" is already a child of \"%s\".",
						   RelationGetRelationName(parent_rel),
						   RelationGetRelationName(child_rel)),
								   errOmitLocation(true)));

	/* If parent has OIDs then child must have OIDs */
	if (parent_rel->rd_rel->relhasoids && !child_rel->rd_rel->relhasoids)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("table \"%s\" without OIDs cannot inherit from table \"%s\" with OIDs",
						RelationGetRelationName(child_rel),
						RelationGetRelationName(parent_rel)),
								   errOmitLocation(true)));

	/* Match up the columns and bump attinhcount and attislocal */
	MergeAttributesIntoExisting(child_rel, parent_rel, inhAttrNameList, is_partition);

	/* Match up the constraints and make sure they're present in child */
	MergeConstraintsIntoExisting(child_rel, parent_rel);

	/*
	 * OK, it looks valid.  Make the catalog entries that show inheritance.
	 */
	StoreCatalogInheritance1(RelationGetRelid(child_rel),
							 RelationGetRelid(parent_rel),
							 inhseqno + 1,
							 catalogRelation, is_partition);

	/* Now we're done with pg_inherits */
	heap_close(catalogRelation, RowExclusiveLock);

}

/*
 * ALTER TABLE INHERIT
 *
 * Add a parent to the child's parents. This verifies that all the columns and
 * check constraints of the parent appear in the child and that they have the
 * same data types and expressions.
 */
static void
ATExecAddInherit(Relation child_rel, Node *node)
{
	Relation	parent_rel;
	bool		is_partition;
	RangeVar   *parent;
	List	   *inhAttrNameList = NIL;

	Assert(PointerIsValid(node));

	if (IsA(node, InheritPartitionCmd))
	{
		parent = ((InheritPartitionCmd *)node)->parent;
		is_partition = true;
	}
	else
	{
		List *inhParms;
		Assert(IsA(node, List));
		inhParms = (List *)node;

		Insist(list_length(inhParms) == 2);
		Assert(IsA(linitial(inhParms), RangeVar));
		Assert(lsecond(inhParms) == NIL || IsA(lsecond(inhParms), List));

		parent = (RangeVar *)linitial(inhParms);
		inhAttrNameList = (List *)lsecond(inhParms);
		is_partition = false;
	}

	/*
	 * AccessShareLock on the parent is what's obtained during normal CREATE
	 * TABLE ... INHERITS ..., so should be enough here.
	 */
	parent_rel = heap_openrv(parent, AccessShareLock);

	/*
	 * Must be owner of both parent and child -- child was checked by
	 * ATSimplePermissions call in ATPrepCmd
	 */
	ATSimplePermissions(parent_rel, false);

	/* Permanent rels cannot inherit from temporary ones */
	if (!isTempNamespace(RelationGetNamespace(child_rel)) &&
		isTempNamespace(RelationGetNamespace(parent_rel)))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot inherit from temporary relation \"%s\"",
						RelationGetRelationName(parent_rel)),
								   errOmitLocation(true)));

	if (is_partition)
	{
		/* lookup all attrs */
		int attno;

		for (attno = 0; attno < parent_rel->rd_att->natts; attno++)
		{
			Form_pg_attribute	 attribute = parent_rel->rd_att->attrs[attno];
			char				*attributeName = NameStr(attribute->attname);

			/* MPP-5397: ignore dropped cols */
			if (!attribute->attisdropped)
				inhAttrNameList =
						lappend(inhAttrNameList,
								makeString(attributeName));
		}

	}
	inherit_parent(parent_rel, child_rel, is_partition, inhAttrNameList);

	/*
	 * Keep our lock on the parent relation until commit, unless we're
	 * doing partitioning, in which case the parent is sufficiently locked.
	 * We want to unlock here in case we're doing deep sub partitioning. We do
	 * not want to acquire too many locks since we're overflow the lock buffer.
	 * An exclusive lock on the parent table is sufficient to guard against
	 * concurrency issues.
	 */
	if (is_partition)
		heap_close(parent_rel, AccessShareLock);
	else
		heap_close(parent_rel, NoLock);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(child_rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(child_rel),
						   GetUserId(),
						   "ALTER", "INHERIT"
				);
}

/*
 * Obtain the source-text form of the constraint expression for a check
 * constraint, given its pg_constraint tuple
 */
static char *
decompile_conbin(HeapTuple contup, TupleDesc tupdesc)
{
	Form_pg_constraint con;
	bool		isnull;
	Datum		attr;
	Datum		expr;

	con = (Form_pg_constraint) GETSTRUCT(contup);
	attr = heap_getattr(contup, Anum_pg_constraint_conbin, tupdesc, &isnull);
	if (isnull)
		elog(ERROR, "null conbin for constraint %u", HeapTupleGetOid(contup));

	expr = DirectFunctionCall2(pg_get_expr, attr,
							   ObjectIdGetDatum(con->conrelid));
	return DatumGetCString(DirectFunctionCall1(textout, expr));
}
/*
	toast_idxid = index_create(toast_relid, toast_idxname, newIndexOid,
							   indexInfo,
							   BTREE_AM_OID,
							   rel->rd_rel->reltablespace,
							   classObjectId,
							   true, false, true, false);*/

/*
 * Check columns in child table match up with columns in parent, and increment
 * their attinhcount.
 *
 * Called by ATExecAddInherit
 *
 * Currently all parent columns must be found in child. Missing columns are an
 * error.  One day we might consider creating new columns like CREATE TABLE
 * does.  However, that is widely unpopular --- in the common use case of
 * partitioned tables it's a foot-gun.
 *
 * The data type must match exactly. If the parent column is NOT NULL then
 * the child must be as well. Defaults are not compared, however.
 */
static void
MergeAttributesIntoExisting(Relation child_rel, Relation parent_rel, List *inhAttrNameList,
							bool is_partition)
{
	Relation	attrrel;
	AttrNumber	parent_attno;
	int			parent_natts;
	TupleDesc	tupleDesc;
	TupleConstr *constr;
	HeapTuple	tuple;
	ListCell	*attNameCell;
	cqContext	cqc;
	cqContext  *pcqCtx;

	attrrel = heap_open(AttributeRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), attrrel);

	tupleDesc = RelationGetDescr(parent_rel);
	parent_natts = tupleDesc->natts;
	constr = tupleDesc->constr;

	/*
	 * If we have an inherited column list, ensure all named columns exist in parent
	 * and that the list excludes system columns.
	 */
	foreach( attNameCell, inhAttrNameList )
	{
		bool columnDefined = false;
		char *inhAttrName = strVal((Value *)lfirst(attNameCell));

		for (parent_attno = 1; parent_attno <= parent_natts && !columnDefined; parent_attno++)
		{
			char *attributeName;
			Form_pg_attribute attribute = tupleDesc->attrs[parent_attno - 1];
			if (attribute->attisdropped)
				continue;
			attributeName = NameStr(attribute->attname);
			columnDefined = (strcmp(inhAttrName, attributeName) == 0);
		}

		if (!columnDefined)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					errmsg("column \"%s\" does not exist in parent table \"%s\"",
							inhAttrName,
							RelationGetRelationName(parent_rel)),
									   errOmitLocation(true)));
	}

	for (parent_attno = 1; parent_attno <= parent_natts; parent_attno++)
	{
		Form_pg_attribute attribute = tupleDesc->attrs[parent_attno - 1];
		char	   *attributeName = NameStr(attribute->attname);

		/* Ignore dropped columns in the parent. */
		if (attribute->attisdropped)
			continue;

		/* Find same column in child (matching on column name). */
		tuple = caql_getattname(pcqCtx, RelationGetRelid(child_rel),
								attributeName);

		if (HeapTupleIsValid(tuple))
		{
			/* Check they are same type and typmod */
			Form_pg_attribute childatt = (Form_pg_attribute) GETSTRUCT(tuple);

			if (attribute->atttypid != childatt->atttypid ||
				attribute->atttypmod != childatt->atttypmod)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("child table \"%s\" has different type for column \"%s\"",
								RelationGetRelationName(child_rel),
								attributeName),
										   errOmitLocation(true)));

			if (attribute->attnotnull && !childatt->attnotnull)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
					  errmsg("column \"%s\" in child table must be marked NOT NULL",
							 attributeName),
									   errOmitLocation(true)));

			/*
			 * OK, bump the child column's inheritance count.  (If we fail
			 * later on, this change will just roll back.)
			 */
			childatt->attinhcount++;

			/*
			 * For locally-defined attributes, check to see if the attribute is named
			 * in the "fully-inherited" list.  If so, mark the child attribute as
			 * not locally defined.  (Default/standard behaviour is to leave the
			 * attribute locally defined.)
			 */
			if (childatt->attislocal)
			{
				/* never local when we're doing partitioning */
				if (is_partition)
					childatt->attislocal = false;
				else
				{
					foreach( attNameCell, inhAttrNameList )
					{
						char *inhAttrName = strVal((Value *)lfirst(attNameCell));
						if (strcmp(attributeName, inhAttrName) == 0)
						{
							childatt->attislocal = false;
							break;
						}
					}
				}
			}

			caql_update_current(pcqCtx, tuple);
			/* and Update indexes (implicit) */

			heap_freetuple(tuple);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("child table is missing column \"%s\"",
							attributeName),
									   errOmitLocation(true)));
		}
	}

	heap_close(attrrel, RowExclusiveLock);
}

/*
 * Check constraints in child table match up with constraints in parent
 *
 * Called by ATExecAddInherit and exchange_part_inheritance
 *
 * Currently all constraints in parent must be present in the child. One day we
 * may consider adding new constraints like CREATE TABLE does. We may also want
 * to allow an optional flag on parent table constraints indicating they are
 * intended to ONLY apply to the master table, not to the children. That would
 * make it possible to ensure no records are mistakenly inserted into the
 * master in partitioned tables rather than the appropriate child.
 *
 * XXX This is O(N^2) which may be an issue with tables with hundreds of
 * constraints. As long as tables have more like 10 constraints it shouldn't be
 * a problem though. Even 100 constraints ought not be the end of the world.
 */
static void
MergeConstraintsIntoExisting(Relation child_rel, Relation parent_rel)
{
	Relation	catalogRelation;
	TupleDesc	tupleDesc;
	cqContext  *pcqCtx;
	cqContext	cqc;
	HeapTuple	constraintTuple;
	ListCell   *elem;
	List	   *constraints;

	/* First gather up the child's constraint definitions */
	catalogRelation = heap_open(ConstraintRelationId, AccessShareLock);
	tupleDesc = RelationGetDescr(catalogRelation);

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), catalogRelation),
			cql("SELECT * FROM pg_constraint "
				" WHERE conrelid = :1 ",
				ObjectIdGetDatum(RelationGetRelid(child_rel))));

	constraints = NIL;
	while (HeapTupleIsValid(constraintTuple = caql_getnext(pcqCtx)))
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(constraintTuple);

		if (con->contype != CONSTRAINT_CHECK)
			continue;

		constraints = lappend(constraints, heap_copytuple(constraintTuple));
	}

	caql_endscan(pcqCtx);

	/* Then scan through the parent's constraints looking for matches */

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), catalogRelation),
			cql("SELECT * FROM pg_constraint "
				" WHERE conrelid = :1 ",
				ObjectIdGetDatum(RelationGetRelid(parent_rel))));

	while (HeapTupleIsValid(constraintTuple = caql_getnext(pcqCtx)))
	{
		Form_pg_constraint parent_con = (Form_pg_constraint) GETSTRUCT(constraintTuple);
		bool		found = false;
		Form_pg_constraint child_con = NULL;
		HeapTuple	child_contuple = NULL;

		if (parent_con->contype != CONSTRAINT_CHECK)
			continue;

		foreach(elem, constraints)
		{
			child_contuple = (HeapTuple) lfirst(elem);
			child_con = (Form_pg_constraint) GETSTRUCT(child_contuple);
			if (strcmp(NameStr(parent_con->conname),
					   NameStr(child_con->conname)) == 0)
			{
				found = true;
				break;
			}
		}

		if (!found)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("child table is missing constraint \"%s\"",
							NameStr(parent_con->conname)),
									   errOmitLocation(true)));

		if (parent_con->condeferrable != child_con->condeferrable ||
			parent_con->condeferred != child_con->condeferred ||
			strcmp(decompile_conbin(constraintTuple, tupleDesc),
				   decompile_conbin(child_contuple, tupleDesc)) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("constraint definition for check constraint \"%s\" does not match",
							NameStr(parent_con->conname)),
									   errOmitLocation(true)));

		/*
		 * TODO: add conislocal,coninhcount to constraints. This is where we
		 * would have to bump them just like attributes
		 */
	}

	caql_endscan(pcqCtx);
	heap_close(catalogRelation, AccessShareLock);
}

/*
 * ALTER TABLE NO INHERIT
 *
 * Drop a parent from the child's parents. This just adjusts the attinhcount
 * and attislocal of the columns and removes the pg_inherit and pg_depend
 * entries.
 *
 * If attinhcount goes to 0 then attislocal gets set to true. If it goes back
 * up attislocal stays true, which means if a child is ever removed from a
 * parent then its columns will never be automatically dropped which may
 * surprise. But at least we'll never surprise by dropping columns someone
 * isn't expecting to be dropped which would actually mean data loss.
 */
static void
ATExecDropInherit(Relation rel, RangeVar *parent, bool is_partition)
{
	Relation	parent_rel;
	Relation	catalogRelation;
	cqContext  *pcqCtx;
	cqContext	cqc;
	HeapTuple	inheritsTuple,
				attributeTuple,
				depTuple;
	bool		found = false;

	/*
	 * AccessShareLock on the parent is probably enough, seeing that DROP TABLE
	 * doesn't lock parent tables at all.  We need some lock since we'll be
	 * inspecting the parent's schema.
	 */
	parent_rel = heap_openrv(parent, AccessShareLock);

	/*
	 * We don't bother to check ownership of the parent table --- ownership
	 * of the child is presumed enough rights.
	 */

	/*
	 * Find and destroy the pg_inherits entry linking the two, or error out
	 * if there is none.
	 */

	/* XXX XXX: could do DELETE FROM ... WHERE ... AND inhparent = :2,
	   ObjectIdGetDatum(RelationGetRelid(parent_rel)) 
	*/
	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_inherits "
				" WHERE inhrelid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(RelationGetRelid(rel))));

	while (HeapTupleIsValid(inheritsTuple = caql_getnext(pcqCtx)))
	{
		Oid			inhparent;

		inhparent = ((Form_pg_inherits) GETSTRUCT(inheritsTuple))->inhparent;
		if (inhparent == RelationGetRelid(parent_rel))
		{
			caql_delete_current(pcqCtx);
			found = true;
			break;
		}
	}
	caql_endscan(pcqCtx);

	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation \"%s\" is not a parent of relation \"%s\"",
						RelationGetRelationName(parent_rel),
						RelationGetRelationName(rel)),
								   errOmitLocation(true)));

	/*
	 * Search through child columns looking for ones matching parent rel
	 */
	catalogRelation = heap_open(AttributeRelationId, RowExclusiveLock);

	/* XXX XXX: AND attisdrop = FALSE, etc */
	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), catalogRelation),
			cql("SELECT * FROM pg_attribute "
				" WHERE attrelid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(RelationGetRelid(rel))));

	while (HeapTupleIsValid(attributeTuple = caql_getnext(pcqCtx)))
	{
		Form_pg_attribute att = (Form_pg_attribute) GETSTRUCT(attributeTuple);
		HeapTuple	attnameTuple;

		/* Ignore if dropped or not inherited */
		if (att->attisdropped)
			continue;
		if (att->attinhcount <= 0)
			continue;

		/* XXX: would have been attname_exists() */
		attnameTuple = caql_getattname(NULL, RelationGetRelid(parent_rel),
									   NameStr(att->attname));

		if (HeapTupleIsValid(attnameTuple))
		{
			/* Decrement inhcount and possibly set islocal to true */
			HeapTuple	copyTuple = heap_copytuple(attributeTuple);
			Form_pg_attribute copy_att = (Form_pg_attribute) GETSTRUCT(copyTuple);

			copy_att->attinhcount--;
			if (copy_att->attinhcount == 0)
				copy_att->attislocal = true;

			caql_update_current(pcqCtx, copyTuple);
			heap_freetuple(copyTuple);
			heap_freetuple(attnameTuple);
		}
	}
	caql_endscan(pcqCtx);
	heap_close(catalogRelation, RowExclusiveLock);

	/*
	 * Drop the dependency
	 *
	 * There's no convenient way to do this, so go trawling through pg_depend
	 */

	/* XXX XXX: AND refclassid, refobjid, etc */
	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_depend "
				" WHERE classid = :1 "
				" AND objid = :2 "
				" AND objsubid = :3 "
				" FOR UPDATE ",
				ObjectIdGetDatum(RelationRelationId),
				ObjectIdGetDatum(RelationGetRelid(rel)),
				Int32GetDatum(0)));

	while (HeapTupleIsValid(depTuple = caql_getnext(pcqCtx)))
	{
		Form_pg_depend dep = (Form_pg_depend) GETSTRUCT(depTuple);

		if (dep->refclassid == RelationRelationId &&
			dep->refobjid == RelationGetRelid(parent_rel) &&
			dep->refobjsubid == 0 &&
			((dep->deptype == DEPENDENCY_NORMAL && !is_partition) ||
			 (dep->deptype == DEPENDENCY_AUTO && is_partition)))
				caql_delete_current(pcqCtx);
	}

	caql_endscan(pcqCtx);

	/* keep our lock on the parent relation until commit */
	heap_close(parent_rel, NoLock);

	/* MPP-6929: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "NO INHERIT"
				);


}

/*
 * deparse pg_class.reloptions into a list.
 */
static List *
reloptions_list(Oid relid)
{
	Datum reloptions;
    HeapTuple tuple;
	bool isNull = true;
	List *opts = NIL;
	cqContext	*pcqCtx;

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(relid)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for relation %u", relid);

    reloptions = caql_getattr(pcqCtx,
							  Anum_pg_class_reloptions,
							  &isNull);
    if (!isNull)
		opts = untransformRelOptions(reloptions);

	caql_endscan(pcqCtx);

	return opts;
}

/*
 * Build:
 *
 * CREATE TABLE pg_temp_<NNNN> AS SELECT * FROM rel
 *   DISTRIBUTED BY dist_clause
 */
static QueryDesc *
build_ctas_with_dist(Relation rel, List *dist_clause,
					 List *storage_opts, RangeVar **tmprv, List **hidden_types)
{
	Query *q;
	SelectStmt *s = makeNode(SelectStmt);
	Node *n;
	List	*parsetrees;
	RangeVar *from_tbl;
	List *rewritten;
	PlannedStmt *stmt;
	DestReceiver *dest;
	QueryDesc *queryDesc;
	RangeVar *tmprel = make_temp_table_name(rel, MyBackendId);
	bool pre_built;

	{
		ResTarget *t = makeNode(ResTarget);
	    ColumnRef *c = makeNode(ColumnRef);
        c->fields = list_make1(makeString("*"));
        c->location = -1;
        t->val = (Node *)c;
        t->location = -1;
		s->targetList = list_make1(t);
	}

	from_tbl = makeRangeVar(NULL /*catalogname*/, get_namespace_name(RelationGetNamespace(rel)),
							pstrdup(RelationGetRelationName(rel)), -1);
	from_tbl->inhOpt = INH_NO; /* MPP-5300: turn off inheritance -
								* Otherwise, the data from the child
								* tables is added to the parent!
								*/
	s->fromClause = list_make1(from_tbl);

	/* Handle tables with OIDS */
	if (rel->rd_rel->relhasoids)
		storage_opts = lappend(storage_opts, defWithOids(true));

	pre_built = prebuild_temp_table(rel, tmprel, dist_clause,
									storage_opts, hidden_types,
									(RelationIsAoRows(rel) ||
									 RelationIsParquet(rel)));
	if (pre_built)
	{
		InsertStmt *i = makeNode(InsertStmt);

		i->relation = tmprel;
		i->selectStmt = (Node *)s;
		n = (Node *)i;
	}
	else
	{
		Oid tblspc = rel->rd_rel->reltablespace;

		s->intoClause = makeNode(IntoClause);
		s->intoClause->rel = tmprel;
		s->intoClause->options = storage_opts;
		s->intoClause->tableSpaceName = get_tablespace_name(tblspc);
		s->distributedBy = dist_clause;
		n = (Node *)s;
	}
	*tmprv = tmprel;

	parsetrees = parse_analyze((Node *)n, NULL, NULL, 0);
	Assert(list_length(parsetrees) == 1);
	q = (Query *)linitial(parsetrees);

	AcquireRewriteLocks(q);

	/* Rewrite through rule system */
	rewritten = QueryRewrite(q);

	/* We don't expect more or less than one result query */
	Assert(list_length(rewritten) == 1);

	q = (Query *) linitial(rewritten);
	Assert(q->commandType == CMD_SELECT || q->commandType == CMD_INSERT);

	/* plan the query */
	stmt = planner(q, 0, NULL, QRL_ONCE);

	/*
	 * Update snapshot command ID to ensure this query sees results of any
	 * previously executed queries.
	 */
	//ActiveSnapshot->curcid = GetCurrentCommandId();

	/* Create dest receiver for COPY OUT */
	dest = CreateDestReceiver(DestIntoRel, NULL);

	/* Create a QueryDesc requesting no output */
	queryDesc = CreateQueryDesc(stmt,  pstrdup("(internal SELECT INTO query)"),
								ActiveSnapshot, InvalidSnapshot,
								dest, NULL, false);
	return queryDesc;
}

static Datum
new_rel_opts(Relation rel, List *lwith)
{
	Datum newOptions = PointerGetDatum(NULL);
	bool make_heap = false;
	bool need_free_value = false;
	if (lwith && list_length(lwith))
	{
		ListCell *lc;

		/*
		 * See if user has specified appendonly = false. If so, we
		 * have no use for the existing reloptions
		 */
		foreach(lc, lwith)
		{
			DefElem *e = lfirst(lc);
			if (pg_strcasecmp(e->defname, "appendonly") == 0 &&
				pg_strcasecmp(defGetString(e, &need_free_value), "false") == 0)
			{
				make_heap = true;
				break;
			}
		}
	}

	if (!make_heap)
	{
		/* Get the old reloptions */
		bool isnull;
		Oid relid = RelationGetRelid(rel);
		cqContext	*pcqCtx;
		HeapTuple optsTuple;

		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_class "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(relid)));

		optsTuple  = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(optsTuple))
				elog(ERROR, "cache lookup failed for relation %u", relid);

		newOptions = caql_getattr(pcqCtx,
								  Anum_pg_class_reloptions, &isnull);

		/* take a copy since we're using it after ReleaseSysCache() */
		if (!isnull)
			newOptions = datumCopy(newOptions, false, -1);

		caql_endscan(pcqCtx);
	}

	/* Generate new proposed reloptions (text array) */
	newOptions = transformRelOptions(newOptions, lwith, false, false);

	return newOptions;
}

static RangeVar *
make_temp_table_name(Relation rel, BackendId id)
{
	char *nspname,
		 tmpname[NAMEDATALEN];
	/* temporary enough */
	snprintf(tmpname, NAMEDATALEN, "pg_temp_%u_%i", RelationGetRelid(rel), id);
	nspname = get_namespace_name(RelationGetNamespace(rel));

	return makeRangeVar(NULL /*catalogname*/, nspname, pstrdup(tmpname), -1);
}

/* Fill-in the names field of the given TypeName node with the name
 * of an existing type that matches the given internal length, and alignment.
 *
 * If none can be found, return NULL, else return the given TypeName
 * pointer.
 *
 * It may seem as if we should include by-value in the test, but built-in
 * types cover the allowable internal lengths for these and we don't want
 * to second guess.
 *
 * Specifically for use by make_typename.
 */
static TypeName *
pick_name_of_similar_type(TypeName *tname, int2 typlen, char typalign)
{
	HeapTuple	tuple;

	/* XXX XXX: not sure why this is RowExclusiveLock */
	tuple = caql_getfirst(
			NULL,
			cql("SELECT * FROM pg_type "
				" WHERE typlen = :1 "
				" AND typalign = :2 "
				" FOR UPDATE ",
				Int16GetDatum(typlen),
				CharGetDatum(typalign)));

	if (HeapTupleIsValid(tuple))
	{
		Form_pg_type typtuple = (Form_pg_type)GETSTRUCT(tuple);
		tname->names =
			list_make2(makeString(get_namespace_name(typtuple->typnamespace)),
					   makeString(pstrdup(NameStr(typtuple->typname))));
	}
	else
		tname = NULL;


	return tname;
}

/*
 * Build the basic CreateFunctionStmt statement for dispatch as part of
 * build_hidden_type().
 */
static CreateFunctionStmt *
build_iofunc(char *newname, char *iotype, char *baseio, char *returns)
{
	CreateFunctionStmt *iofunc;
	char ioname[NAMEDATALEN];
	FunctionParameter *param;
	List *args;

	if (strcmp("in", iotype) == 0)
	   args = list_make2(makeString("pg_catalog"), makeString("cstring"));
	else
		args = list_make1(makeString(newname));

	/*
	 * CREATE FUNCTION F(cstring) RETURNS shelltype AS 'int4(cstring)' language
	 * internal;
	 */
	iofunc = makeNode(CreateFunctionStmt);
	snprintf(ioname, NAMEDATALEN, "%s_%s", newname, iotype);
	iofunc->funcname = list_make1(makeString(pstrdup(ioname)));
	param = makeNode(FunctionParameter);
	param->argType = makeTypeNameFromNameList(args);
	param->mode = FUNC_PARAM_IN;
	iofunc->parameters = list_make1(param);
	iofunc->returnType = makeTypeName(returns);
	iofunc->options = list_make2(makeDefElem("as", (Node *)list_make1(makeString(baseio))),
								 makeDefElem("language", (Node *)makeString("internal")));

	return iofunc;
}

/*
 * Build a temporary hidden type for dropped columns for which there is no
 * suitable pg_type entry. We find these "ghost types" when a user has created a
 * type, dropped a column using it and THEN dropped the type. There is a kind of
 * "homeopathic echo" here, where we have the structural details of the type in
 * pg_attribute but no way of pin pointing the type configuration by name.
 *
 * Procedure:
 * a) Build a shell type
 * b) Create IO functions
 * c) Fill out the shell type with IO functions and correct length and alignment
 * data
 *
 * The IO functions do not have to do anything because all values in the column
 * will be NULL, the functions will never be called.
 */
static TypeName *
build_hidden_type(Form_pg_attribute att)
{
	DefineStmt *newtype;
	CreateFunctionStmt *iofunc;
	Value *inputname;
	Value *outputname;
	List *parsetrees;
	DestReceiver *dest = None_Receiver;
	Query *q;
	char *alignname = NULL;
	TypeName *typname;
	char newname[NAMEDATALEN];

	Assert(Gp_role == GP_ROLE_DISPATCH);

	snprintf(newname, NAMEDATALEN, "pg_atsdb_%u_%i_%u", att->attrelid,
			 att->attnum, MyBackendId);

	/* shell type */
	newtype = makeNode(DefineStmt);
	newtype->kind = OBJECT_TYPE;
	newtype->defnames = list_make1(makeString(newname));

	/* that's all that's needed for the shell! */
	parsetrees = parse_analyze((Node *)newtype, NULL, NULL, 0);
	Assert(list_length(parsetrees) == 1);
	q = (Query *)linitial(parsetrees);
	ProcessUtility((Node *)q->utilityStmt, 
				   synthetic_sql,
				   NULL, 
				   false, /* not top level */
				   dest, 
				   NULL);
	CommandCounterIncrement();

	/*
	 * Now for the input function.
	 */
	iofunc = build_iofunc(newname, "in", "int4in", newname);
	inputname = copyObject(linitial(iofunc->funcname));
	parsetrees = parse_analyze((Node *)iofunc, NULL, NULL, 0);
	q = (Query *)linitial(parsetrees);
	ProcessUtility((Node *)q->utilityStmt, 
				   synthetic_sql,
				   NULL, 
				   false, /* not top level */
				   dest, 
				   NULL);
	CommandCounterIncrement();

	/* and then output */
	/* output function accepts shellname as its argument */
	iofunc = build_iofunc(newname, "out", "int4out", "cstring");
	outputname = copyObject(linitial(iofunc->funcname));
	parsetrees = parse_analyze((Node *)iofunc, NULL, NULL, 0);
	q = (Query *)linitial(parsetrees);
	ProcessUtility((Node *)q->utilityStmt,
				   synthetic_sql,
				   NULL, 
				   false, /* not top level */
				   dest, 
				   NULL);
	CommandCounterIncrement();

	/* now build full type */
	newtype = makeNode(DefineStmt);
	newtype->kind = OBJECT_TYPE;
	newtype->defnames = list_make1(makeString(newname));

	newtype->definition = lappend(newtype->definition, makeDefElem("input", (Node *)inputname));
	newtype->definition = lappend(newtype->definition, makeDefElem("output", (Node *)outputname));

	if (att->attbyval)
		newtype->definition = lappend(newtype->definition, makeDefElem("passedbyvalue",
																	   (Node *)makeInteger(1)));

	if (att->attlen == -1)
		newtype->definition = lappend(newtype->definition, makeDefElem("internallength",
																	   (Node *)makeString("variable")));
	else
		newtype->definition = lappend(newtype->definition, makeDefElem("internallength",
																	   (Node *)makeInteger(att->attlen)));

	if (att->attalign == 'd')
		alignname = "double";
	else if (att->attalign == 'i')
		alignname = "int4";
	else if (att->attalign == 's')
		alignname = "int2";
	else if (att->attalign == 'c')
		alignname = "char";
	else
		Assert(false); /* shouldn't get here */

	newtype->definition = lappend(newtype->definition, makeDefElem("alignment",
																   (Node *)makeString(alignname)));

	parsetrees = parse_analyze((Node *)newtype, NULL, NULL, 0);
	q = (Query *)linitial(parsetrees);
	ProcessUtility((Node *)q->utilityStmt,
				   synthetic_sql,
				   NULL, 
				   false, /* not top level */
				   dest, 
				   NULL);
	CommandCounterIncrement();

	typname = makeNode(TypeName);
	typname->names = list_make1(makeString(pstrdup(newname)));

	return typname;
}

/* Allocate and return a pointer to a TypeName node specifying the
 * qualified name of an existing type that matches the internal length
 * and alignment of the given pg_attribute tuple.
 *
 * First we try a few hard-coded builtin types. Internal types exist for
 *    select distinct typlen, typalign
 *    from pg_type where typlen > 0;
 * on a new catalog.  Names are not unique.
 *
 * MPP-5483: If that fails, we look in pg_type for a match.  If even that
 * fails, we issue an error.
 *
 * Specifically for use by prebuilt_temp_table.
 */
static TypeName *
make_typname(Form_pg_attribute att, bool *built)
{
	TypeName *tname = makeNode(TypeName);
	int2 attlen = att->attlen;
	Value *typname = NULL;
	bool dig_in_catalog = false;

	Assert(att->attisdropped); /* better not be here unless */

	tname->typmod = att->atttypmod;

	if (attlen == -1)
	{
		if (att->attalign == 'i')
		{
			if (att->atttypmod < MaxAttrSize)
				typname = makeString(pstrdup("varchar"));
			else
				typname = makeString(pstrdup("numeric"));

			/* 
			 * We're building dropped columns. Dropped columns should never
			 * create a TOAST table because they only ever contain NULL. So,
			 * even if the original typmod was -1 (variable length), set it to a
			 * real value so that we avoid TOAST creation. To do this, we must
			 * be one byte longer than the maximum variable length header size.
			 * See needs_toast_table().
			 *
			 * XXX: there may be occassions where tables with dropped TOASTable
			 * columns do still have TOAST tables. Need to explore those.
			 */
			tname->typmod = 1 + Max(NUMERIC_HDRSZ, VARHDRSZ);
		}
		else if (att->attalign == 'd')
			typname = makeString(pstrdup("path"));
		else
			dig_in_catalog = true;
	}
	else if (attlen == 1 && att->attalign == 'c')
    {
		typname = makeString(pstrdup("char"));
    }
	else if (attlen == 2 && att->attalign == 's')
    {
		typname = makeString(pstrdup("int2"));
    }
	else if (attlen == 4 && att->attalign == 'i')
    {
		typname = makeString(pstrdup("int4"));
    }
	else if (attlen == 6 && att->attalign == 's')
    {
            typname = makeString(pstrdup("tid"));
	}
    else if (attlen == 6 && att->attalign == 'i')
	{
            typname = makeString(pstrdup("macaddr"));
    }
	else if (attlen == 8 && att->attalign == 'd')
    {
		typname = makeString(pstrdup("int8"));
    }
	else if (attlen == 12 && att->attalign == 'd')
    {
			typname = makeString(pstrdup("timetz"));
	}
	else if (attlen == 12 && att->attalign == 'i')
	{
		typname = makeString(pstrdup("tinterval"));
    }
	else if (attlen == 16 && att->attalign == 'd')
    {
		typname = makeString(pstrdup("interval"));
    }
	else if (attlen == 24 && att->attalign == 'd')
    {
		typname = makeString(pstrdup("circle"));
    }
	else if (attlen == 32 && att->attalign == 'd')
    {
		typname = makeString(pstrdup("box"));
    }
	else if (attlen == 64 && att->attalign == 'i')
    {
		typname = makeString(pstrdup("name"));
    }
	else
	{
		dig_in_catalog = true;
    }

	*built = false; /* assume we found an existing type */
	if ( dig_in_catalog )
	{
		tname = pick_name_of_similar_type(tname, attlen, att->attalign);
		if (!tname)
		{
			*built = true;
			tname = build_hidden_type(att);
		}
	}
	else
	{
		tname->names = list_make2(makeString(pstrdup("pg_catalog")), typname);
	}

	tname->location = -1;
	return tname;
}

/*
 * If the table has dropped columns, we must create the table and
 * drop the columns before we can dispatch the select statement.
 * Return true if we do it, false if we do not. If we return false,
 * there are no dropped columns and we can do a SELECT INTO later.
 * If we need to do it, but fail, issue an error. (See make_type.)
 *
 * Specifically for build_ctas_with_dist.
 *
 * Note that the caller should guarantee that isTmpTableAo has
 * a value that matches 'opts'.
 */
static bool
prebuild_temp_table(Relation rel, RangeVar *tmpname, List *distro, List *opts,
					List **hidden_types, bool isTmpTableAo)
{
	bool need_rebuild = false;
	int attno = 0;
	TupleDesc tupdesc = RelationGetDescr(rel);
	List *drop_atts = NIL; /* attnums where we want an attribute to drop */

	if (!need_rebuild)
	{
		for (attno = 0; attno < tupdesc->natts; attno++)
		{
			if (tupdesc->attrs[attno]->attisdropped)
			{
				need_rebuild = true;
				break;
			}
		}
	}

	/*
	 * If the new table is an AO table with indexes, always use
	 * Create Table + Insert Into. During Create Table phase,
	 * we determine whether to create the block directory
	 * depending on whether the original table has indexes. It is
	 * important to create the block directory to support the reindex
	 * later. See MPP-9545 for more info.
	 */
	if (isTmpTableAo &&
		rel->rd_rel->relhasindex)
		need_rebuild = true;

	if (need_rebuild)
	{
		CreateStmt *cs = makeNode(CreateStmt);
		List *parsetrees;
		Query *q;
		char *dstr = "__gp_atsdb_droppedcol";
		DestReceiver *dest = None_Receiver;

		cs->relKind = RELKIND_RELATION;
		cs->distributedBy = distro;
		cs->relation = tmpname;
		cs->ownerid = rel->rd_rel->relowner;
		cs->tablespacename = get_tablespace_name(rel->rd_rel->reltablespace);
		cs->buildAoBlkdir = false;

		if (isTmpTableAo &&
			rel->rd_rel->relhasindex)
			cs->buildAoBlkdir = true;

		cs->options = opts;

		for (attno = 0; attno < tupdesc->natts; attno++)
		{
			ColumnDef *cd = makeNode(ColumnDef);
			TypeName *tname = NULL;
			Form_pg_attribute att = tupdesc->attrs[attno];

			cd->is_local = true;

			if (att->attisdropped)
			{
				NameData alias;
				bool built;

				tname = make_typname(att, &built);
				snprintf(NameStr(alias), sizeof(alias), "%s%d", dstr, attno);
				cd->colname = pstrdup(NameStr(alias));
				drop_atts = lappend_int(drop_atts, attno);

				if (built)
					*hidden_types = lappend(*hidden_types, tname->names);
			}
			else
			{
				Type typ = typeidType(att->atttypid);
				Oid typnamespace = ((Form_pg_type) GETSTRUCT(typ))->typnamespace;
				char *nspname = get_namespace_name(typnamespace);
				int arno;
				char *typstr;
				int4 ndims = att->attndims;

				tname = makeNode(TypeName);

				if (!PointerIsValid(nspname))
					elog(ERROR, "could not lookup namespace %d", typnamespace);
				cd->colname = pstrdup(NameStr(att->attname));
				typstr = typeTypeName(typ);
				tname->names = list_make2(makeString(nspname),
										  makeString(typstr));
				ReleaseType(typ);
				tname->typmod = att->atttypmod;

				/*
				 * If this is a built in array type, like _int4, then reduce
				 * the array dimensions by 1. This is an annoying postgres
				 * hack which I wish would go away.
				 */
				if (typstr && typstr[0] == '_' && ndims > 0)
					ndims--;

				for (arno = 0; arno < ndims; arno++)
					/* bound of -1 are fine because this has no effect on data */
					tname->arrayBounds = lappend(tname->arrayBounds,
												 makeInteger(-1));

			}

			tname->location = -1;
			cd->typname = tname;
			cs->tableElts = lappend(cs->tableElts, cd);
		}
		parsetrees = parse_analyze((Node *)cs, NULL, NULL, 0);
		Assert(list_length(parsetrees) == 1);
		q = (Query *)linitial(parsetrees);
		ProcessUtility((Node *)q->utilityStmt, 
					   synthetic_sql,
					   NULL, 
					   false, /* not top level */
					   dest, 
					   NULL);
		CommandCounterIncrement();

		/* should always be true */
		if (drop_atts)
		{
			/* Build an ALTER TABLE DROP COLUMN statement */
			AlterTableStmt *ats = makeNode(AlterTableStmt);
			ListCell *lc;

			ats->relkind = OBJECT_TABLE;
			ats->relation = tmpname;

			foreach(lc, drop_atts)
			{
				AlterTableCmd *atc = makeNode(AlterTableCmd);
				NameData colname;

				attno = lfirst_int(lc);
				atc->subtype = AT_DropColumn;
				snprintf(NameStr(colname), NAMEDATALEN, "%s%d", dstr, attno);
				atc->name = pstrdup(NameStr(colname));
				ats->cmds = lappend(ats->cmds, atc);
			}
#ifdef NOT_USED
			elog_node_display(NOTICE, "DROP STATEMENT", ats, true);
#endif

			parsetrees = parse_analyze((Node *)ats, NULL, NULL, 0);
			Assert(list_length(parsetrees) == 1);
			q = (Query *)linitial(parsetrees);
			ProcessUtility((Node *)q->utilityStmt, 
						   synthetic_sql,
						   NULL, 
						   false, /* not top level */
						   dest, 
						   NULL);
			CommandCounterIncrement();
		}
	}
	return need_rebuild;
}

/* Build a human readable tag for what we're doing */
static char *
make_distro_str(List *lwith, List *ldistro)
{
    char       *distro_str = "SET WITH DISTRIBUTED BY";

	if (lwith && ldistro)
		distro_str = "SET WITH DISTRIBUTED BY";
	else
	{
		if (lwith)
			distro_str = "SET WITH";
		else if (ldistro)
			distro_str = "SET DISTRIBUTED BY";
	}
	return pstrdup(distro_str); /* don't return a stack address */
}

/*
 * ALTER TABLE SET DISTRIBUTED BY
 *
 * set distribution policy for rel
 *
 * GPSQL: Despite the GP_ROLE_DISPATCH references, note that this entire
 * function executes only on the master.
 */
static void
ATExecSetDistributedBy(Relation rel, Node *node, AlterTableCmd *cmd)
{
	Assert(Gp_role != GP_ROLE_EXECUTE);		/* GPSQL */

	List 	   *lprime;
	List 	   *lwith, *ldistro;
	List	   *cols = NIL;
	ListCell   *lc;
	GpPolicy   *policy = NULL;
	QueryDesc  *queryDesc;
	RangeVar   *tmprv = NULL;
	Oid			tmprelid;
	Oid			tarrelid = RelationGetRelid(rel);
	List	   *oid_map = NIL;
	List	   *qe_data = NIL; /* data to pass to QEs */
	bool        rand_pol = false;
	bool        force_reorg = false;
	Datum		newOptions = PointerGetDatum(NULL);
	bool		change_policy = false;
	bool		is_ao = false;
    bool        is_aocs = false;
    char        relstorage = RELSTORAGE_HEAP;
	List	   *hidden_types = NIL; /* types we need to build for dropped columns */

	/* Permissions checks */
	if (!pg_class_ownercheck(RelationGetRelid(rel), GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
					   RelationGetRelationName(rel));

	/* Can't ALTER TABLE SET system catalogs */
	if (IsSystemRelation(rel))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied: \"%s\" is a system catalog",
						RelationGetRelationName(rel)),
								   errOmitLocation(true)));

	Assert(PointerIsValid(node));
	Assert(IsA(node, List));

	lprime = (List *)node;

	/* 
	 * First element is the WITH clause, second element is the actual
	 * distribution clause.
	 */
	lwith   = (List *)linitial(lprime);
	ldistro = (List *)lsecond(lprime);

	if (Gp_role == GP_ROLE_UTILITY)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("SET DISTRIBUTED BY not supported in utility mode")));

	/* we only support fully distributed tables */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		if (rel->rd_cdbpolicy->ptype != POLICYTYPE_PARTITIONED)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("%s not supported on non-distributed tables",
							ldistro ? "SET DISTRIBUTED BY" : "SET WITH"),
									   errOmitLocation(true)));
	}

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		if (lwith)
		{
			bool		 seen_reorg = false;
			ListCell	*lc;
			char		*reorg_str = "reorganize";
			List		*nlist = NIL;

			/* remove the "REORGANIZE=true/false" from the WITH clause */
			foreach(lc, lwith)
			{
        DefElem *def = lfirst(lc);

        if (pg_strcasecmp(reorg_str, def->defname) != 0)
        {
          /* MPP-7770: disable changing storage options for now */
          if (!gp_setwith_alter_storage)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("option \"%s\" not supported", def->defname),
                    errOmitLocation(true)));

          if (pg_strcasecmp(def->defname, "appendonly") == 0)
          {
            if (IsA(def->arg, String)
                && pg_strcasecmp(strVal(def->arg), "true") == 0)
            {
              is_ao = true;
              relstorage = RELSTORAGE_AOROWS;
            }
            else
            {
              is_ao = false;
            }
          }

          if (pg_strcasecmp(def->defname, "orientation") == 0)
          {
            if (IsA(def->arg, String)
                && pg_strcasecmp(strVal(def->arg), "column") == 0)
            {
              ereport(ERROR,
                  (errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
                      errmsg("orientation option \"column\" is deprecated. Not support it any more."),
                      errhint("valid orientation option is \"row\" or \"parquet\""),
                      errOmitLocation(true)));
            }
            else if (IsA(def->arg, String)
                && pg_strcasecmp(strVal(def->arg), "parquet") == 0)
            {
              is_aocs = true;
              relstorage = RELSTORAGE_PARQUET;
            }
            else
            {
              if (!IsA(def->arg, String)
                  || pg_strcasecmp(strVal(def->arg), "row") != 0)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid orientation option"),
                        errhint("valid orientation option is \"row\" or \"parquet\""),
                        errOmitLocation(true)));
            }
          }

          nlist = lappend(nlist, def);
				}
				else
				{
          /* have we been here before ? */
          if (seen_reorg)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("\"%s\" specified more than once", reorg_str),
                    errOmitLocation(true)));

          seen_reorg = true;
          if (!def->arg)
          {
            force_reorg = true;
          }
          else
          {
            if (IsA(def->arg, String)
                && pg_strcasecmp("TRUE", strVal(def->arg)) == 0)
            {
              force_reorg = true;
            }
            else if (IsA(def->arg, String)
                && pg_strcasecmp("FALSE", strVal(def->arg)) == 0)
            {
              force_reorg = false;
            }
            else
            {
              ereport(ERROR,
                  (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                      errmsg("Invalid REORGANIZE option"),
                      errhint("valid REORGANIZE option is \"true\" or \"false\""),
                      errOmitLocation(true)));
            }
          }
				}
			}

      if (is_aocs && !is_ao)
      {
          ereport(ERROR,
                  (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                   errmsg("specify orientation requires appendonly"),
                     errOmitLocation(true)));
      }

			lwith = nlist;

			/*
			 * If there are other storage options, but REORGANIZE is not
			 * specified, then the storage must be re-org'd.  But if
			 * REORGANIZE was specified use that setting.
			 *
			 * If the user specified we not force a reorg but there are other
			 * WITH clause items, then we cannot honour what the user has
			 * requested.
			 */
			if (!seen_reorg && list_length(lwith))
				force_reorg = true;
			else if (seen_reorg && force_reorg == false && list_length(lwith))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("%s must be set to true when changing storage "
								 "type", reorg_str),
										   errOmitLocation(true)));

			newOptions = new_rel_opts(rel, lwith);
			
			/* ensure that the options parse */
			if (newOptions)
				(void)heap_reloptions(rel->rd_rel->relkind, newOptions, true);
		}
		else
			newOptions = new_rel_opts(rel, NIL);

		if (ldistro)
			change_policy = true;

		if (ldistro && linitial(ldistro) == NULL)
		{
			Insist(list_length(ldistro) == 1);

			rand_pol = true;

			if (!force_reorg)
			{
				if (rel->rd_cdbpolicy->nattrs == 0)
					ereport(WARNING,
							(errcode(ERRCODE_DUPLICATE_OBJECT),
							 errmsg("distribution policy of relation \"%s\" "
									"already set to DISTRIBUTED RANDOMLY",
									RelationGetRelationName(rel)),
							 errhint("Use ALTER TABLE \"%s\" "
									 "SET WITH (REORGANIZE=TRUE) "
									 "DISTRIBUTED RANDOMLY "
									 "to force a random redistribution",
									 RelationGetRelationName(rel)),
											   errOmitLocation(true)));
			}

			policy = (GpPolicy *) palloc(sizeof(GpPolicy));
			policy->ptype = POLICYTYPE_PARTITIONED;
			policy->nattrs = 0;

			rel->rd_cdbpolicy = GpPolicyCopy(GetMemoryChunkContext(rel),
										 policy);
			GpPolicyReplace(RelationGetRelid(rel), policy);

			/* only need to rebuild if have new storage options */
			if (!(DatumGetPointer(newOptions) || force_reorg))
			{
				/*
				 * caller expects ATExecSetDistributedBy() to close rel
				 * (see the non-random distribution case below for why.
				 */
				heap_close(rel, NoLock);
				goto l_distro_fini;
			}
		}
	}

	/*
	 * Changing a table from random distribution to a specific distribution
	 * policy is the hard bit. For that, we must do the following:
	 *
	 * a) Ensure that the proposed policy is sensible
	 * b) Create a temporary table and reorganise data according to
	 * our desired distribution policy. To do this, we build a Query
	 * node which expresses the query CREATE TABLE tmp_table_name AS
	 * SELECT * FROM cur_table DISTRIBUTED BY (policy)
	 * c) Execute the query across all nodes
	 * d) Update our parse tree to include the details of the newly created
	 * table
	 * e) Update the ownership of the temporary table
	 * f) Swap the relfilenodes of the existing table and the temporary table
	 * g) Update the policy on the QD to reflect the underlying data
	 * h) Drop the temporary table -- and with it, the old copy of the data
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		volatile Snapshot saveSnapshot = NULL;
		if (change_policy)
		{
			policy = palloc(sizeof(GpPolicy) +
							sizeof(policy->attrs[0]) * list_length(ldistro));
			policy->ptype = POLICYTYPE_PARTITIONED;
			policy->nattrs = 0;

			/* Step (a) */
			if (!rand_pol)
			{
				foreach(lc, ldistro)
				{
					char *colName = strVal((Value *)lfirst(lc));
					cqContext	*attcqCtx;
					HeapTuple tuple;

					attcqCtx = caql_getattname_scan(NULL, 
													RelationGetRelid(rel), 
													colName);
	
					tuple = caql_get_current(attcqCtx);

					AttrNumber	attnum;

					if (list_member(cols, lfirst(lc)))
							ereport(ERROR,
									(errcode(ERRCODE_DUPLICATE_OBJECT),
									 errmsg("distribution policy must be a "
											"unique set of columns"),
									 errhint("Column \"%s\" appears "
											 "more than once", colName),
													   errOmitLocation(true)));
					if (!HeapTupleIsValid(tuple))
							ereport(ERROR,
									(errcode(ERRCODE_UNDEFINED_COLUMN),
									 errmsg("column \"%s\" of "
											"relation \"%s\" does not exist",
											colName,
											RelationGetRelationName(rel)),
													   errOmitLocation(true)));

					attnum = ((Form_pg_attribute) GETSTRUCT(tuple))->attnum;

					/* Prevent them from altering a system attribute */
					if (attnum <= 0)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						    errmsg("cannot distribute by system column \"%s\"",
									colName),
											   errOmitLocation(true)));

					policy->attrs[policy->nattrs++] = attnum;

					caql_endscan(attcqCtx);
					cols = lappend(cols, lfirst(lc));
				} /* end foreach */

				Assert(policy->nattrs > 0);

				/*
				 * See if the the old policy is the same as the new one but
				 * remember, we still might have to rebuild if there are new
				 * storage options.
				 */
				if (!DatumGetPointer(newOptions) && !force_reorg &&
					(policy->nattrs == rel->rd_cdbpolicy->nattrs))
				{
					int i;
					bool diff = false;

					for (i = 0; i < policy->nattrs; i++)
					{
						if (policy->attrs[i] != rel->rd_cdbpolicy->attrs[i])
						{
							diff = true;
							break;
						}
					}
					if (!diff)
					{
						char *dist =
							palloc(list_length(ldistro) * (NAMEDATALEN + 1));

						dist[0] = '\0';

						foreach(lc, ldistro)
						{
							if (lc != list_head(ldistro))
								strcat(dist, ",");
							strcat(dist, strVal(lfirst(lc)));
						}
						ereport(WARNING,
							(errcode(ERRCODE_DUPLICATE_OBJECT),
							 errmsg("distribution policy of relation \"%s\" "
									"already set to (%s)",
									RelationGetRelationName(rel),
									dist),
							 errhint("Use ALTER TABLE \"%s\" "
									 "SET WITH (REORGANIZE=TRUE) "
									 "DISTRIBUTED BY (%s) "
									 "to force redistribution",
									 RelationGetRelationName(rel),
									 dist),
											   errOmitLocation(true)));
						heap_close(rel, NoLock);
						/* Tell QEs to do nothing */
						linitial(lprime) = NULL;
						lsecond(lprime) = list_make1(NULL);

						return;
						/* don't goto l_distro_fini -- didn't do anything! */
					}
				}
			}
		}

		if (!ldistro)
			ldistro = make_dist_clause(rel);

		/* force the use of legacy query optimizer, since PQO will not redistribute the tuples if the current and required
		   distributions are both RANDOM even when reorganize is set to "true"*/
		bool saveOptimizerGucValue = optimizer;
		optimizer = false;

		if (saveOptimizerGucValue)
		{
			elog(LOG, "ALTER SET DISTRIBUTED BY: falling back to legacy query optimizer to ensure re-distribution of tuples.");
		}

		PG_TRY();
		{
			/* 
			 * We need to update our snapshot here to make sure we see all
			 * committed work. We have an exclusive lock on the table so no one
			 * will be able to access the table now.
			 */
			saveSnapshot = ActiveSnapshot;
			ActiveSnapshot = CopySnapshot(GetLatestSnapshot());

	    /* Step (b) - build CTAS */
	    queryDesc = build_ctas_with_dist(rel, ldistro,
	                     untransformRelOptions(newOptions),
	                     &tmprv,
	                     &hidden_types);

			/* Step (c) - run on all nodes */
			ExecutorStart(queryDesc, 0);
			ExecutorRun(queryDesc, ForwardScanDirection, 0L);
			ExecutorEnd(queryDesc);
			FreeQueryDesc(queryDesc);

			/* Restore the old snapshot */
			ActiveSnapshot = saveSnapshot;
			optimizer = saveOptimizerGucValue;

		}
		PG_CATCH();
		{
			ActiveSnapshot = saveSnapshot;
			optimizer = saveOptimizerGucValue;

			PG_RE_THROW();
		}
		PG_END_TRY();

		CommandCounterIncrement(); /* see the effects of the command */

		/*
		 * Step (d) - tell the seg nodes about the temporary relation. This
		 * involves stomping on the node we've been given
		 */
		qe_data = lappend(qe_data, makeInteger(MyBackendId));
	}
	/*
	GPSQL: ALTER SET DISTRIBUTED BY doesn't need to propagate the tmprv and relopts to
	the QEs, because the QEs won't be needing to revise their own catalogs.

	else
	{
		int backend_id;
		bool reorg = true;

		Assert(list_length(lprime) >= 2);

		lwith = linitial(lprime);
		qe_data = lsecond(lprime);

		if (lwith)
		{
			if (list_length(lwith))
			{
				DefElem *e = linitial(lwith);
				if (pg_strcasecmp(e->defname, "reorganize") == 0 &&
					pg_strcasecmp(strVal(e->arg), "false") == 0)
					reorg = false;
			}
		}
		else if (!lwith)
			reorg = false;

		if (!reorg && list_length(qe_data) == 1 && linitial(qe_data) == NULL)
		{
			heap_close(rel, NoLock);
			goto l_distro_fini;			
		}

		backend_id = intVal(linitial(qe_data));
		tmprv = make_temp_table_name(rel, backend_id);
		oid_map = lsecond(qe_data);
		if (list_length(qe_data) == 3)
			hidden_types = lthird(qe_data);

		if (list_length(lprime) == 3)
		{
			Value *v = lthird(lprime);
			if (intVal(v) == 1)
      {
			  is_ao = true;
        relstorage = RELSTORAGE_AOROWS;
      }
			else
      {
				is_ao = false;
        relstorage = RELSTORAGE_HEAP;
      }
		}

		newOptions = new_rel_opts(rel, lwith);
	}
	*/

	/*
	 * Step (e) - Correct ownership on temporary table:
	 *   necessary so that the toast tables/indices have the correct
	 *   owner after we swap them.
	 *
	 * Note: ATExecChangeOwner does NOT dispatch, so this does not
	 * belong in the dispatch block above (MPP-9663).
	 */
	ATExecChangeOwner(RangeVarGetRelid(tmprv, false, false /*allowHcatalog*/),
					  rel->rd_rel->relowner, true);
	CommandCounterIncrement(); /* see the effects of the command */

	/*
	 * Step (f) - swap relfilenodes and MORE !!!
	 *
	 * Just lookup the Oid and pass it to swap_relation_files(). To do
	 * this we must close the rel, since it needs to be forgotten by
	 * the cache, we keep the lock though. ATRewriteCatalogs() knows
	 * that we've closed the relation here.
	 */
	heap_close(rel, NoLock);
   	tmprelid = RangeVarGetRelid(tmprv, false, false /*allowHcatalog*/);
	swap_relation_files(tarrelid, tmprelid, false);

	if (DatumGetPointer(newOptions))
	{
		Datum		repl_val[Natts_pg_class];
		bool		repl_null[Natts_pg_class];
		bool		repl_repl[Natts_pg_class];
		HeapTuple	newOptsTuple;
		HeapTuple	tuple;
		cqContext	*relcqCtx;

		/*
		 * All we need do here is update the pg_class row; the new
		 * options will be propagated into relcaches during
		 * post-commit cache inval.
		 */
		MemSet(repl_val, 0, sizeof(repl_val));
		MemSet(repl_null, false, sizeof(repl_null));
		MemSet(repl_repl, false, sizeof(repl_repl));

		if (newOptions != (Datum) 0)
			repl_val[Anum_pg_class_reloptions - 1] = newOptions;
		else
			repl_null[Anum_pg_class_reloptions - 1] = true;

		repl_repl[Anum_pg_class_reloptions - 1] = true;

		relcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_class "
					" WHERE oid = :1 "
					" FOR UPDATE ",
					ObjectIdGetDatum(tarrelid)));

		tuple = caql_getnext(relcqCtx);

		Insist(HeapTupleIsValid(tuple));
		newOptsTuple = caql_modify_current(relcqCtx,
										   repl_val, repl_null, repl_repl);

		caql_update_current(relcqCtx, newOptsTuple);
		/* and Update indexes (implicit) */

		heap_freetuple(newOptsTuple);

		caql_endscan(relcqCtx);

		/*
		 * Increment cmd counter to make updates visible; this is
		 * needed because the same tuple has to be updated again
		 */
		CommandCounterIncrement();
	}

	/* now, reindex */
	/* GPSQL: This will run only on the master, and that's ok. At this
	 * stage of GPSQL, the only indexes we're concerned with are those
	 * that reside on the master, which index master metadata. */
	reindex_relation(tarrelid, false, false /* ao_segs ? */, false,
					 &oid_map, Gp_role == GP_ROLE_DISPATCH);

	/* Step (g) */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		if (change_policy)
			GpPolicyReplace(tarrelid, policy);

		qe_data = lappend(qe_data, oid_map);

		if (hidden_types)
			qe_data = lappend(qe_data, hidden_types);
		linitial(lprime) = lwith;
		lsecond(lprime) = qe_data;
		lprime = lappend(lprime, makeInteger(is_ao ? (is_aocs ? 2 : 1) : 0));
	}

	/* Step (h) Drop the table */
	{
		ObjectAddress object;
		object.classId = RelationRelationId;
		object.objectId = tmprelid;
		object.objectSubId = 0;

		performDeletion(&object, DROP_RESTRICT);

		if (hidden_types)
		{
			object.classId = TypeRelationId;

			foreach(lc, hidden_types)
			{
				TypeName *tname = makeTypeNameFromNameList(lfirst(lc));
				Oid typid = typenameTypeId(NULL, tname);

				object.objectId = typid;

				/* cascade to get rid of functions */
				performDeletion(&object, DROP_CASCADE);
			}
		}
	}

	AORelRemoveHashEntryOnCommit(tarrelid);

l_distro_fini:

	/* MPP-6929: metadata tracking */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char *distro_str = make_distro_str(lwith, ldistro);
		/* don't check relkind - must be a table */
		MetaTrackUpdObject(RelationRelationId,
						   tarrelid,
						   GetUserId(),
						   "ALTER", distro_str
				);
	}

}


/*
 * rel could be a toast table, toast table index or index on a
 * table. Get that table's OID.
 */
static Oid
rel_get_table_oid(Relation rel)
{
	Oid toid = RelationGetRelid(rel);
	Relation thisrel = NULL;

	if (rel->rd_rel->relkind == RELKIND_INDEX)
	{
		Oid indrelid;
		int fetchCount;

		indrelid = caql_getoid_plus(
				NULL,
				&fetchCount,
				NULL,
				cql("SELECT indrelid FROM pg_index "
					" WHERE indexrelid = :1 ",
					ObjectIdGetDatum(toid)));

		if (!fetchCount)
			elog(ERROR, "cache lookup failure: cannot find pg_index entry for OID %u",
				 toid);
		toid = indrelid;

		thisrel = relation_open(toid, NoLock);
		toid = rel_get_table_oid(thisrel); /* **RECURSIVE** */
		relation_close(thisrel, NoLock);
		return toid;
	}
	else if (rel->rd_rel->relkind == RELKIND_AOSEGMENTS ||
			 rel->rd_rel->relkind == RELKIND_AOBLOCKDIR ||
			 rel->rd_rel->relkind == RELKIND_TOASTVALUE)
	{
		/* use pg_depend to find parent */
		cqContext  *pcqCtx;
		cqContext	cqc;
		HeapTuple tup;

		/* XXX XXX: SnapShotAny */
		pcqCtx = caql_beginscan(
				caql_snapshot(cqclr(&cqc), SnapshotAny),
				cql("SELECT * FROM pg_depend "
					" WHERE classid = :1 "
					" AND objid = :2 ",
					ObjectIdGetDatum(RelationRelationId),
					ObjectIdGetDatum(toid)));
		/*
		 * We use SnapshotAny because the ordering of the dependency code means
		 * that some times we've already deleted the pg_depend tuple. So, we do
		 * an extra test below to see that, if this tuple is deleted, it was
		 * done so by our xid, otherwise we overlook it.
		 */

		while (HeapTupleIsValid(tup = caql_getnext(pcqCtx)))
		{
			Form_pg_depend foundDep = (Form_pg_depend) GETSTRUCT(tup);
			HeapTupleHeader htup = tup->t_data;

			if (!TransactionIdIsNormal(HeapTupleHeaderGetXmax(htup)) ||
				TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(htup)))
			{
				if (foundDep->deptype == DEPENDENCY_INTERNAL)
				{
					toid = foundDep->refobjid;
					break;
				}
			}
		}
		caql_endscan(pcqCtx);
	}
	return toid;
}

/*
 * partition children, toast tables and indexes, and indexes on partition
 * children do not long lived locks because the lock on the partition master
 * protects us.
 */
bool
rel_needs_long_lock(Oid relid)
{
	bool needs_lock = true;
	Relation rel = relation_open(relid, NoLock);

	relid = rel_get_table_oid(rel);

	relation_close(rel, NoLock);

	if (Gp_role == GP_ROLE_DISPATCH)
		needs_lock = !rel_is_child_partition(relid);
	else
	{
		int fetchCount;

		fetchCount  = caql_getcount(
				NULL,
				cql("SELECT COUNT(*) FROM pg_inherits "
					" WHERE inhrelid = :1 "
					" AND inhseqno = :2 ",
					 ObjectIdGetDatum(relid),
					Int32GetDatum(1)));

		if (fetchCount)
			needs_lock = false;
	}
	return needs_lock;
}


/*
 * ALTER TABLE ... ADD PARTITION
 *
 */

static 	AlterPartitionId *
wack_pid_relname(AlterPartitionId 		 *pid,
				 PartitionNode  		**ppNode,
				 Relation 				  rel,
				 PgPartRule 			**ppar_prule,
				 char 					**plrelname,
				 char 					 *lRelNameBuf)
{
	AlterPartitionId 	*locPid = pid;	/* local pid if IDRule */

	if (!pid)
		return NULL;

	if (AT_AP_IDRule == locPid->idtype)
	{
		List 		*l1		   = (List *)pid->partiddef;
		ListCell 	*lc;
		PgPartRule 	*par_prule = NULL;

		lc = list_head(l1);
		*ppar_prule = (PgPartRule*) lfirst(lc);

		par_prule = *ppar_prule;

		*plrelname = par_prule->relname;

		if (par_prule && par_prule->topRule && par_prule->topRule->children)
			*ppNode = par_prule->topRule->children;

		lc = lnext(lc);

		locPid = (AlterPartitionId *)lfirst(lc);

		Assert(locPid);
	}
	else
	{
		*ppNode = RelationBuildPartitionDesc(rel, false);

		snprintf(lRelNameBuf, (NAMEDATALEN*2),
					 "relation \"%s\"",
					 RelationGetRelationName(rel));
		*plrelname = lRelNameBuf;
	}

	return locPid;
}

static void
ATPExecPartAdd(AlteredTableInfo *tab,
			   Relation rel,
               AlterPartitionCmd *pc,
			   AlterTableType att)
{
	AlterPartitionId 	*pid 		= (AlterPartitionId *)pc->partid;
	PgPartRule   		*prule 		= NULL;
	PartitionNode  		*pNode 		= NULL;
	char           		*parTypName = NULL;
	char			 	 namBuf[NAMEDATALEN];
	AlterPartitionId 	*locPid 	= NULL;	/* local pid if IDRule */
	PgPartRule* 		 par_prule 	= NULL;	/* prule for parent if IDRule */
	char 		 		 lRelNameBuf[(NAMEDATALEN*2)];
	char 				*lrelname   = NULL;
	Node 				*pSubSpec 	= NULL;
	AlterPartitionCmd   *pc2		= NULL;
	bool				 is_split = false;
	bool				 bSetTemplate = (att == AT_PartSetTemplate);

	/* This whole function is QD only. */
	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	pc2 = (AlterPartitionCmd *)pc->arg2;

	if (pc2->partid)
		is_split = true;

	locPid =
			wack_pid_relname(pid,
							 &pNode,
							 rel,
							 &par_prule,
							 &lrelname,
							 lRelNameBuf);

	if (!pNode)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("%s is not partitioned", lrelname),
				 errOmitLocation(true)));

	switch (pNode->part->parkind)
	{
		case 'h': /* hash */
			parTypName = "HASH";
			break;
		case 'r': /* range */
			parTypName = "RANGE";
			break;
		case 'l': /* list */
			parTypName = "LIST";
			break;
		default:
			elog(ERROR, "unrecognized partitioning kind '%c'",
				 pNode->part->parkind);
			Assert(false);
			break;
	} /* end switch */


	if (locPid->idtype == AT_AP_IDName)
			snprintf(namBuf, sizeof(namBuf), " \"%s\"",
					 strVal(locPid->partiddef));
	else
			namBuf[0] = '\0';

	if ('h' == pNode->part->parkind)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot add HASH partition%s to %s",
						namBuf,
						lrelname),
				 errhint("recreate the table to increase the "
						 "number of partitions"),
								   errOmitLocation(true)));


	/* partition must have a valid name */
	if ((locPid->idtype != AT_AP_IDName)
		&& (locPid->idtype != AT_AP_IDNone))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot ADD partition%s to %s by rank or value",
						namBuf,
						lrelname),
					 errhint("use a named partition"),
							   errOmitLocation(true)));

	/* don't check if splitting or setting a subpartition template */
	if (!is_split && !bSetTemplate)
		/* We complain if partition already exists, so prule should be NULL */
		prule = get_part_rule(rel, pid, true, false,
							  CurrentMemoryContext, NULL,
							  false);

	if (!prule)
	{
		bool isDefault = intVal(pc->arg1);

		/* DEFAULT checks */
		if (!isDefault && (pNode->default_part) &&
			!is_split && !bSetTemplate) /* MPP-6093: ok to reset template */
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot add %s partition%s to "
						"%s with DEFAULT partition \"%s\"",
						parTypName,
						namBuf,
						lrelname,
						pNode->default_part->parname),
				 errhint("need to SPLIT partition \"%s\"",
						 pNode->default_part->parname),
								   errOmitLocation(true)));

		if (isDefault && !is_split)
		{
			if ('h' == pNode->part->parkind)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("cannot add a DEFAULT partition%s "
								"to %s of type HASH",
								namBuf,
								lrelname),
										   errOmitLocation(true)));

			/* MPP-6093: ok to reset template */
			if (pNode->default_part && !bSetTemplate)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_OBJECT),
						 errmsg("DEFAULT partition \"%s\" for "
								"%s already exists",
								pNode->default_part->parname,
								lrelname),
										   errOmitLocation(true)));

			/* XXX XXX: move this check to gram.y ? */
			if (pc2->arg1)
			{
				PartitionElem *pelem = (PartitionElem *) pc2->arg1;

				if (pelem->boundSpec)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							 errmsg("invalid use of boundary specification "
									"for DEFAULT partition%s of %s",
									namBuf,
									lrelname),
											   errOmitLocation(true)));
			}
		}

		/* Do the real work for add ... */
		

		if ('r' == pNode->part->parkind)
		{
			pSubSpec =
			atpxPartAddList(rel, pc, pNode,
							pc2->arg2, /* utl statement */
							(locPid->idtype == AT_AP_IDName) ?
							locPid->partiddef : NULL, /* partition name */
							isDefault, (PartitionElem *) pc2->arg1,
							PARTTYP_RANGE,
							par_prule,
							lrelname,
							bSetTemplate,
							rel->rd_rel->relowner);

		}
		else if ('l' == pNode->part->parkind)
		{
			pSubSpec =
			atpxPartAddList(rel, pc, pNode,
							pc2->arg2, /* utl statement */
							(locPid->idtype == AT_AP_IDName) ?
							locPid->partiddef : NULL, /* partition name */
							isDefault, (PartitionElem *) pc2->arg1,
							PARTTYP_LIST,
							par_prule,
							lrelname,
							bSetTemplate,
							rel->rd_rel->relowner);
		}

	}

	/* MPP-6929: metadata tracking */
	if (!is_split && !bSetTemplate)
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "PARTITION", "ADD"
				);

} /* end ATPExecPartAdd */


/*
 * Add partition hierarchy to catalogs.
 *
 * parts is a list, with a partitioning rule and potentially sub-partitions.
 */
static void
ATExecPartAddInternal(Relation rel, Node *def)
{
	PartitionBy *part = (PartitionBy *)def;
	add_part_to_catalog(RelationGetRelid(rel), part, false);
}


/* ALTER TABLE ... ALTER PARTITION */
static void
ATPExecPartAlter(List **wqueue, AlteredTableInfo *tab, Relation rel,
                 AlterPartitionCmd *pc)
{
	AlterPartitionId 	*pid		   = (AlterPartitionId *)pc->partid;
	AlterTableCmd 		*atc		   = (AlterTableCmd *)pc->arg1;
	PgPartRule   		*prule		   = NULL;
	List 				*pidlst		   = NIL;
	AlterPartitionId 	*pid2		   = makeNode(AlterPartitionId);
	AlterPartitionCmd 	*pc2		   = NULL;
	bool				 bPartitionCmd = true;	/* true if a "partition" cmd */
	Relation			 rel2		   = rel;

	while (1)
	{
		pidlst = lappend(pidlst, pid);

		if (atc->subtype != AT_PartAlter)
			break;
		pc2 = (AlterPartitionCmd *)atc->def;
		pid = (AlterPartitionId *)pc2->partid;
		atc = (AlterTableCmd *)pc2->arg1;
	}

	/* let split, exchange through */
	if (!(atc->subtype == AT_PartExchange ||
		  atc->subtype == AT_PartSplit ||
		  atc->subtype == AT_SetDistributedBy) &&
		Gp_role != GP_ROLE_DISPATCH)
		return;

	switch (atc->subtype)
	{
		case AT_PartAdd:				/* Add */
		case AT_PartCoalesce:			/* Coalesce */
		case AT_PartDrop:				/* Drop */
		case AT_PartSplit:				/* Split */
		case AT_PartMerge:				/* Merge */
		case AT_PartModify:				/* Modify */
		case AT_PartSetTemplate:		/* Set Subpartition Template */
				if (!gp_allow_non_uniform_partitioning_ddl)
				{
					ereport(ERROR,
						   (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("Cannot modify multi-level partitioned table to have non-uniform partitioning hierarchy.")));
				}
				break;
				/* XXX XXX: treat set subpartition template special:

				need to pass the pNode to ATPExecPartSetTemplate and bypass
				ATExecCmd ...

				*/
		case AT_PartRename:	 			/* Rename */
		case AT_PartExchange:			/* Exchange */
		case AT_PartTruncate:			/* Truncate */
				break;

		/* Next, list of ALTER TABLE commands applicable to a child table */
		case AT_SetTableSpace:			/* Set Tablespace */
		case AT_SetDistributedBy:	/* SET DISTRIBUTED BY */
				bPartitionCmd = false;
				break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot ALTER PARTITION for relation \"%s\"",
							RelationGetRelationName(rel)),
									   errOmitLocation(true)));
	}

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		pid2->idtype = AT_AP_IDList;
		pid2->partiddef = (Node *)pidlst;
		pid2->location  = -1;

		prule = get_part_rule(rel, pid2, true, true,
							  CurrentMemoryContext, NULL,
							  false);

		if (!prule)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot ALTER PARTITION for relation \"%s\"",
							RelationGetRelationName(rel)),
									   errOmitLocation(true)));
		if (bPartitionCmd)
		{
			/* build the IDRule for the nested ALTER PARTITION cmd ... */
			Assert(IsA(atc->def, AlterPartitionCmd));

			pc2 = (AlterPartitionCmd *)atc->def;
			pid = (AlterPartitionId *)pc2->partid;

			pid2->idtype = AT_AP_IDRule;
			pid2->partiddef = (Node *)list_make2((Node *)prule, pid);
			pid2->location  = -1;

			pc2->partid = (Node *)pid2;
		}
		else /* treat as a table */
		{
			/* Update PID for use on QEs */
			pid2->idtype = AT_AP_IDRule;
			pid2->partiddef = (Node *)list_make2((Node *)prule, pid);
			pid2->location  = -1;
			pc->partid = (Node *)pid2;

			/* get the child table relid */
			rel2 = heap_open(prule->topRule->parchildrelid,
							 AccessExclusiveLock);

			/* MPP-5524: check if can change distribution policy */
			if (atc->subtype == AT_SetDistributedBy)
			{
				List *dist_cnames = NIL;
				Assert(IsA(atc->def, List));

				dist_cnames = lsecond((List*)atc->def);

				/*	might be null if no policy set, e.g. just a change
				 *	of storage options...
				 */
				if (dist_cnames)
				{
					Assert(IsA(dist_cnames, List));

					if (! can_implement_dist_on_part(rel, dist_cnames) )
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("cannot ALTER PARTITION ... SET "
										"DISTRIBUTED BY for %s",
										prule->relname),
										errhint("distribution policy of "
												"partition must match parent"),
														   errOmitLocation(true)
										));
				}

			}

			/*
			 * Give the notice first, because it looks weird if it
			 * comes after a failure message
			 */
			ereport(NOTICE,
					(errmsg("altering table \"%s\" "
							"(%s)",
							RelationGetRelationName(rel2),
							prule->relname)));
		}
	}
	else if (Gp_role == GP_ROLE_EXECUTE && atc->subtype == AT_SetDistributedBy)
	{
		pid = (AlterPartitionId *)pc->partid;
		Assert(IsA(pid->partiddef, List));
		prule = (PgPartRule *)linitial((List *)pid->partiddef);
		/* get the child table relid */
		rel2 = heap_open(prule->topRule->parchildrelid,
						 AccessExclusiveLock);
		bPartitionCmd = false;
	}
	/* execute the command */
	ATExecCmd(wqueue, tab, rel2, atc);

	if (!bPartitionCmd)
	{
		/* NOTE: for the case of Set Distro,
		 * ATExecSetDistributedBy rebuilds the relation, so rel2
		 * is already gone!
		 */
		if (atc->subtype != AT_SetDistributedBy)
			heap_close(rel2, NoLock);
	}

	/* MPP-6929: metadata tracking - don't track this! */

} /* end ATPExecPartAlter */


/* ALTER TABLE ... COALESCE PARTITION */
static void
ATPExecPartCoalesce(Relation rel,
                    AlterPartitionCmd *pc)
{
	AlterPartitionId *pid = (AlterPartitionId *)pc->partid;
	PgPartRule   *prule = NULL;

	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	prule = get_part_rule(rel, pid, true, true, CurrentMemoryContext, NULL,
						  false);

	if (0)
	{

		parruleord_open_gap(
				prule->pNode->part->partid,
				prule->pNode->part->parlevel,
				prule->topRule->parparentoid,
				prule->topRule->parruleord,
				0,
				CurrentMemoryContext);

	}

    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("cannot COALESCE PARTITION for relation \"%s\"",
                    RelationGetRelationName(rel)),
             			   errOmitLocation(true)));

}

/* ALTER TABLE ... DROP PARTITION */

static void
ATPExecPartDrop(Relation rel,
                AlterPartitionCmd *pc)
{
	AlterPartitionId 	*pid 		 = (AlterPartitionId *)pc->partid;
	PgPartRule   		*prule 		 = NULL;
	PartitionNode  		*pNode  	 = NULL;
	DropStmt 			*ds 		 = (DropStmt *)pc->arg1;
	bool 				 bCheckMaybe = !(ds->missing_ok);
	AlterPartitionId 	*locPid 	 = pid;  /* local pid if IDRule */
	PgPartRule* 		 par_prule 	 = NULL; /* prule for parent if IDRule */
	char 		 		 lRelNameBuf[(NAMEDATALEN*2)];
	char 				*lrelname	= NULL;
	bool 				 bForceDrop	= false;

	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	if (pc->arg2 &&
		IsA(pc->arg2, AlterPartitionCmd))
	{
		/* NOTE: Ugh, I hate this hack.  Normally, PartDrop has a null
		 * pc->arg2 (the DropStmt is on arg1).  However, for SPLIT, we
		 * have the case where we may need to DROP that last partition
		 * of a table, which in only ok because we will re-ADD two
		 * partitions to replace it.  So allow bForceDrop only for
		 * this case.  We need a better way to decorate the ALTER cmd
		 * structs to annotate these special cases.
		 */
		bForceDrop = true;
	}

	/* missing partition id only ok for range partitions -- just get
	 * first one */

	locPid =
			wack_pid_relname(pid,
							 &pNode,
							 rel,
							 &par_prule,
							 &lrelname,
							 lRelNameBuf);

	if (AT_AP_IDNone == locPid->idtype)
	{
		if (pNode && pNode->part && (pNode->part->parkind != 'r'))
		{
			if (strlen(lrelname))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("missing name or value for DROP for %s",
								lrelname),
										   errOmitLocation(true)));
			else
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("missing name or value for DROP"),
								   errOmitLocation(true)));
		}

		/* if a range partition, and not specified, just get the first one */
		locPid->idtype = AT_AP_IDRank;
		locPid->partiddef = (Node *)makeInteger(1);
	}

	prule = get_part_rule(rel, pid, bCheckMaybe, true,
						  CurrentMemoryContext, NULL, false);

	/* MPP-3722: complain if for(value) matches the default partition */
	if ((locPid->idtype == AT_AP_IDValue)
		&& prule &&
		(prule->topRule == prule->pNode->default_part))
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("FOR expression matches "
							"DEFAULT partition%s of %s",
							prule->partIdStr,
							prule->relname),
					 errhint("FOR expression may only specify "
							 "a non-default partition in this context."),
									   errOmitLocation(true)));

	if (!prule)
	{
		Assert(ds->missing_ok);
		switch (locPid->idtype)
		{
			case AT_AP_IDNone:				/* no ID */
				/* should never happen */
				Assert(false);
				break;
			case AT_AP_IDName:				/* IDentify by Name */
				ereport(NOTICE,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("partition \"%s\" of %s does not "
							"exist, skipping",
							strVal(locPid->partiddef),
							lrelname
							),
									   errOmitLocation(true)));

				break;
			case AT_AP_IDValue:				/* IDentifier FOR Value */
				ereport(NOTICE,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("partition for specified value of "
							"%s does not exist, skipping",
							lrelname
							 ),
									   errOmitLocation(true)));

				break;
			case AT_AP_IDRank:				/* IDentifier FOR Rank */
				ereport(NOTICE,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("partition for specified rank of "
							"%s does not exist, skipping",
							lrelname
							 ),
									   errOmitLocation(true)));

				break;
			case AT_AP_ID_oid:				/* IDentifier by oid */
				ereport(NOTICE,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("partition for specified oid of "
							"%s does not exist, skipping",
							lrelname
							 ),
									   errOmitLocation(true)));
				break;
			case AT_AP_IDDefault:			/* IDentify DEFAULT partition */
				ereport(NOTICE,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("DEFAULT partition for "
							"%s does not exist, skipping",
							lrelname
							 ),
									   errOmitLocation(true)));
				break;
			default: /* XXX XXX */
				Assert(false);
		}
		return;
	}
	else
	{
		char* prelname;
		int   numParts = list_length(prule->pNode->rules);
		DestReceiver 	*dest = None_Receiver;
		Relation rel2;
		RangeVar *relation;
		char *namespace_name;

		/* add the default partition to the count of partitions */
		if (prule->pNode->default_part)
			numParts++;

		/* maybe ERRCODE_INVALID_TABLE_DEFINITION ? */

		/* cannot drop a hash partition */
		if ('h' == prule->pNode->part->parkind)
			ereport(ERROR,
					(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
					 errmsg("cannot drop HASH partition%s of %s",
							prule->partIdStr,
							prule->relname),
					 errhint("recreate the table to reduce the "
							 "number of partitions"),
									   errOmitLocation(true)));

		/* cannot drop last partition of table */
		if (!bForceDrop && (numParts <= 1))
		{
			if (AT_AP_IDRule != pid->idtype)
				ereport(ERROR,
						(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
						 errmsg("cannot drop partition%s of "
								"%s -- only one remains",
								prule->partIdStr,
								prule->relname),
						 errhint("Use DROP TABLE \"%s\" to remove the "
								 "table and the final partition ",
								 RelationGetRelationName(rel)),
										   errOmitLocation(true)));
			else
				ereport(ERROR,
						(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
						 errmsg("cannot drop partition%s of "
								"%s -- only one remains",
								prule->partIdStr,
								prule->relname),
						 errhint("DROP the parent partition to remove the "
								 "final partition "),
										   errOmitLocation(true)));

		}
		rel2 = heap_open(prule->topRule->parchildrelid, NoLock);

		elog(DEBUG5, "dropping partition oid %u", prule->topRule->parchildrelid);
		prelname = pstrdup(RelationGetRelationName(rel2));
		namespace_name = get_namespace_name(rel2->rd_rel->relnamespace);

		/* XXX XXX : don't need "relation" unless fix to use removerelation */
		relation = makeRangeVar(NULL /*catalogname*/, namespace_name, prelname, -1);
		relation->location = pc->location;

		heap_close(rel2, NoLock);

		ds->removeType = OBJECT_TABLE;
		ds->bAllowPartn = true; /* allow drop of partitions */

		if (prule->topRule->children)
		{
			List *l1 = atpxDropList(rel2, prule->topRule->children);

			ds->objects = lappend(l1,
								  list_make2(makeString(namespace_name),
											 makeString(prelname)));
		}
		else
			ds->objects = list_make1(list_make2(makeString(namespace_name),
												makeString(prelname)));

		ProcessUtility((Node *) ds,
					   synthetic_sql,
					   NULL, 
					   false, /* not top level */
					   dest, 
					   NULL);

		/* Notify of name if did not use name for partition id spec */
		if (prule && prule->topRule && prule->topRule->children
			&& (ds->behavior != DROP_CASCADE ))
		{
			ereport(NOTICE,
					(errmsg("dropped partition%s for %s and its children",
							prule->partIdStr,
							prule->relname)));
		}
		else if ((pid->idtype != AT_AP_IDName)
				 && prule->isName)
				ereport(NOTICE,
						(errmsg("dropped partition%s for %s",
								prule->partIdStr,
								prule->relname)));


		/* MPP-6929: metadata tracking */
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "PARTITION", "DROP"
				);
	}

} /* end ATPExecPartDrop */

static void
exchange_part_inheritance(Oid oldrelid, Oid newrelid)
{
	int fetchCount;
	Oid parentid;
	Relation oldrel;
	Relation newrel;
	Relation parent;

	oldrel = heap_open(oldrelid, AccessExclusiveLock);
	newrel = heap_open(newrelid, AccessExclusiveLock);

	parentid = caql_getoid_plus(
			NULL,
			&fetchCount,
			NULL,
			cql("SELECT inhparent FROM pg_inherits "
				" WHERE inhrelid = :1 ",
				ObjectIdGetDatum(oldrelid)));

	/* should be one and only one parent when it comes to inheritance */
	Assert(1 == fetchCount); 

	parent = heap_open(parentid, AccessShareLock); /* should be enough */
	ATExecDropInherit(oldrel,
			makeRangeVar(NULL /*catalogname*/, get_namespace_name(parent->rd_rel->relnamespace),
					     RelationGetRelationName(parent), -1),
			true);

	inherit_parent(parent, newrel, true /* it's a partition */, NIL);
	heap_close(parent, NoLock);
	heap_close(oldrel, NoLock);
	heap_close(newrel, NoLock);
}

/* ALTER TABLE ... EXCHANGE PARTITION 
 * 
 * Do the exchange that was validated earlier (in ATPrepExchange).
 */
static void
ATPExecPartExchange(AlteredTableInfo *tab, Relation rel, AlterPartitionCmd *pc)
{
	Oid					 oldrelid 	= InvalidOid;
	Oid					 newrelid 	= InvalidOid;
	PgPartRule   	    *orig_prule = NULL;
	AlterPartitionCmd	*pc2 = NULL;
	bool is_split = false;
	List				*pcols = NIL; /* partitioned attributes of rel */
	AlterPartitionIdType orig_pid_type = AT_AP_IDNone;	/* save for NOTICE msg at end... */

	if (Gp_role == GP_ROLE_UTILITY)
		return;
	
	/* Exchange for SPLIT is different from user-requested EXCHANGE.  The special
	 * coding to indicate SPLIT is obscure. */
	is_split = ((AlterPartitionCmd *)pc->arg2)->arg2 != NULL;

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		AlterPartitionId   *pid   = (AlterPartitionId *)pc->partid;
		PgPartRule   	   *prule = NULL;
		RangeVar       	   *newrelrv  = (RangeVar *)pc->arg1;
		RangeVar		   *oldrelrv;
		PartitionNode 	   *pn;
		Relation			oldrel;

		pn = RelationBuildPartitionDesc(rel, false);
		pcols = get_partition_attrs(pn);

		prule = get_part_rule(rel, pid, true, true,
							  CurrentMemoryContext, NULL, false);

		if (!prule)
			return;

		if (prule && prule->topRule && prule->topRule->children)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot EXCHANGE PARTITION for "
							"%s -- partition has children",
							prule->relname
							 ),
									   errOmitLocation(true)));

		orig_pid_type = pid->idtype;
		orig_prule = prule;

		oldrelid = prule->topRule->parchildrelid;
		newrelid = RangeVarGetRelid(newrelrv, false, false /*allowHcatalog*/);

		pfree(pc->partid);

		oldrel = heap_open(oldrelid, NoLock);
		oldrelrv =
			makeRangeVar(NULL /*catalogname*/, get_namespace_name(RelationGetNamespace(oldrel)),
						 get_rel_name(oldrelid), -1);
		heap_close(oldrel, NoLock);

		pc->partid = (Node *)oldrelrv;
		pc2 = (AlterPartitionCmd *)pc->arg2;
		pc2->arg2 = (Node *)pcols; /* for execute nodes */

		/* MPP-6929: metadata tracking */
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "PARTITION", "EXCHANGE"
				);
	}
	else if (Gp_role == GP_ROLE_EXECUTE)
	{
		RangeVar *oldrelrv;
		RangeVar *newrelrv;

		Assert(IsA(pc->partid, RangeVar));
		Assert(IsA(pc->arg1, RangeVar));
		oldrelrv = (RangeVar *)pc->partid;
		newrelrv = (RangeVar *)pc->arg1;
		oldrelid = RangeVarGetRelid(oldrelrv, false, false /*allowHcatalog*/);
		newrelid = RangeVarGetRelid(newrelrv, false, false /*allowHcatalog*/);
		pc2 = (AlterPartitionCmd *)pc->arg2;
		pcols = (List *)pc2->arg2;
	}

	Assert(OidIsValid(oldrelid));
	Assert(OidIsValid(newrelid));

#if IF_ONLY_IT_WAS_THAT_SIMPLE
	swap_relation_files(oldrelid, newrelid, false);
	CommandCounterIncrement();
#else
	/*
	 * It would be nice to just swap the relfilenodes. In fact, we could
	 * do that in most cases, the exceptions being tables with dropped 
	 * columns and append only tables.
	 *
	 * So instead, we swap the names of the tables, the type names, the 
	 * constraints, inheritance. We do not swap indexes, ao information
	 * or statistics.
	 *
	 * Note that the state, whether QD or QE, at this point is
	 * - rel -- relid of partitioned table
	 * - oldrelid -- relid of part currently in place
	 * - newrelid -- relid of candidate part to exchange in
	 * - orig_pid_type -- what kind of AlterTablePartitionId id'd the part
	 * - orig_prule -- Used in issuing notice in occasional case.
	 * - pc2->arg1 -- integer Value node: 1 validate, else 0.
	 * - pcols -- integer list of master partitioning columns
	 */
	{
		char			 tmpname1[NAMEDATALEN];
		char			 tmpname2[NAMEDATALEN];
		char			*newname;
		char			*oldname;
		Relation		 newrel;
		Relation		 oldrel;
		AttrMap			*newmap;
		AttrMap			*oldmap;
		List			*newcons;
		bool			 ok;
		bool			 validate	= intVal(pc2->arg1) ? true : false;
		Oid				 oldnspid	= InvalidOid;
		Oid				 newnspid	= InvalidOid;
		char			*newNspName = NULL;
		char			*oldNspName = NULL;

		newrel = heap_open(newrelid, AccessExclusiveLock);
		oldrel = heap_open(oldrelid, AccessExclusiveLock);

		oldnspid = RelationGetNamespace(oldrel);
		newnspid = RelationGetNamespace(newrel);

		if (oldnspid != newnspid)
		{
			newNspName = pstrdup(get_namespace_name(newnspid));
			oldNspName = pstrdup(get_namespace_name(oldnspid));
		}

		newname = pstrdup(RelationGetRelationName(newrel));
		oldname = pstrdup(RelationGetRelationName(oldrel));
		
		ok = map_part_attrs(rel, newrel, &newmap, FALSE);
		Assert(ok);
		ok = map_part_attrs(rel, oldrel, &oldmap, FALSE);
		Assert(ok);

		newcons = cdb_exchange_part_constraints(rel, oldrel, newrel, validate, is_split);
		tab->constraints = list_concat(tab->constraints, newcons);
		CommandCounterIncrement();

		exchange_part_inheritance(RelationGetRelid(oldrel), RelationGetRelid(newrel));
		CommandCounterIncrement();

		snprintf(tmpname1, sizeof(tmpname1), "pg_temp_%u", oldrelid);
		snprintf(tmpname2, sizeof(tmpname2), "pg_temp_%u", newrelid);

		exchange_permissions(RelationGetRelid(oldrel),
							 RelationGetRelid(newrel));
		CommandCounterIncrement();

		heap_close(newrel, NoLock);
		heap_close(oldrel, NoLock);

		/* rename rel renames the type too */
		renamerel(oldrelid, tmpname1, NULL);
		CommandCounterIncrement();
		RelationForgetRelation(oldrelid);

		/* MPP-6979: if the namespaces are different, switch them */
		if (newNspName)
		{
			/* move the old partition (which has a temporary name) to
			 * the new namespace 
			 */
			oldrel = heap_open(oldrelid, AccessExclusiveLock);
			AlterRelationNamespaceInternalTwo(oldrel,
											  oldrelid,
											  oldnspid, newnspid,
											  true,
											  newNspName);
			heap_close(oldrel, NoLock);
			CommandCounterIncrement();
			RelationForgetRelation(oldrelid);

			/* before we move the new table to the old namespace,
			 * rename it to a temporary name to avoid a name
			 * collision.  It would be nice to have an atomic
			 * operation to rename and renamespace a relation... 
			 */
			renamerel(newrelid, tmpname2, NULL);
			CommandCounterIncrement();
			RelationForgetRelation(newrelid);

			newrel = heap_open(newrelid, AccessExclusiveLock);
			AlterRelationNamespaceInternalTwo(newrel,
											  newrelid,
											  newnspid, oldnspid,
											  true,
											  oldNspName);
			heap_close(newrel, NoLock);
			CommandCounterIncrement();
			RelationForgetRelation(newrelid);
		}

		renamerel(newrelid, oldname, NULL);
		CommandCounterIncrement();
		RelationForgetRelation(newrelid);

		renamerel(oldrelid, newname, NULL);
		CommandCounterIncrement();
		RelationForgetRelation(oldrelid);

		CommandCounterIncrement();

		/* fix up partitioning rule if we're on the QD*/
		if (Gp_role == GP_ROLE_DISPATCH)
		{
			exchange_part_rule(oldrelid, newrelid);
			CommandCounterIncrement();

			/* Notify of name if did not use name for partition id spec */
			if ( orig_pid_type != AT_AP_IDName && orig_prule->isName )
				ereport(NOTICE,
						(errmsg("exchanged partition%s of "
								"%s with relation \"%s\"",
								orig_prule->partIdStr,
								orig_prule->relname,
								newname),
						 errOmitLocation(true)));
		}
	}
	tab->exchange_relid = newrelid;
#endif
}

/* ALTER TABLE ... MERGE PARTITION */
static void
ATPExecPartMerge(Relation rel,
                 AlterPartitionCmd *pc)
{
	Relation source = NULL;
	Relation target = NULL;

	if (Gp_role == GP_ROLE_UTILITY)
		return;
	else if (Gp_role == GP_ROLE_DISPATCH)
	{
		AlterPartitionId   *pid   = (AlterPartitionId *)pc->partid;
		PgPartRule   	   *prule1 = NULL;
		PgPartRule   	   *prule2 = NULL;
		PgPartRule   	   *prule3 = NULL;
		RangeVar		   *relrv1, *relrv2;
		Relation			r1, r2;

		prule1 = get_part_rule(rel, pid, true, true,
							   CurrentMemoryContext, NULL, false);

		/*
		 * XXX: get_namespace_name(prule1->topRule->parchildrelid) is wrong
		 * need the relnamespace not relid
		 */
		relrv1 = makeRangeVar(
					NULL /*catalogname*/, 
					get_namespace_name(prule1->topRule->parchildrelid),
					get_rel_name(prule1->topRule->parchildrelid), -1);

		r1 = heap_openrv(relrv1, AccessExclusiveLock);

		prule2 = get_part_rule(rel, (AlterPartitionId *)pc->arg1,
							   true, true,
							   CurrentMemoryContext, NULL, false);

		relrv2 = makeRangeVar(
					NULL /*catalogname*/, 	
					get_namespace_name(prule2->topRule->parchildrelid),
					get_rel_name(prule2->topRule->parchildrelid), -1);

		r2 = heap_openrv(relrv2, AccessExclusiveLock);

		if (pc->arg2)
		{
			AlterPartitionCmd *pc2 = (AlterPartitionCmd *)pc->arg2;

			prule3 = get_part_rule(rel, (AlterPartitionId *)pc2->partid,
								   true, true, CurrentMemoryContext,
								   NULL, false);

			Assert(PointerIsValid(prule3));

			if (prule3->topRule->parchildrelid == prule1->topRule->parchildrelid)
			{
				source = r1;
				pc->partid = (Node *)relrv1;
				target = r2;
				pc->arg1 = (Node *)relrv2;
			}
			else if (prule3->topRule->parchildrelid == prule2->topRule->parchildrelid)
			{
				source = r2;
				pc->partid = (Node *)relrv2;
				target = r1;
				pc->arg1 = (Node *)relrv1;
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("target of MERGE must be either \"%s\" or "
								"\"%s\"",
								get_rel_name(prule1->topRule->parchildrelid),
								get_rel_name(prule2->topRule->parchildrelid)),
										   errOmitLocation(true)));

			}
		}
		else
		{
			source = r1;
			target = r2;
			pc->partid = (Node *)relrv1;
			pc->arg1 = (Node *)relrv2;
		}

		/* MPP-6929: metadata tracking */
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "PARTITION", "MERGE"
				);
	}
	else if (Gp_role == GP_ROLE_EXECUTE)
	{
		RangeVar *relrv;

		Assert(IsA(pc->partid, RangeVar));
		Assert(IsA(pc->arg1, RangeVar));

		relrv = (RangeVar *)pc->partid;
		source = heap_openrv(relrv, AccessExclusiveLock);

		relrv = (RangeVar *)pc->arg1;
		target = heap_openrv(relrv, AccessExclusiveLock);
	}

	elog(NOTICE, "merging");
	elog(NOTICE, "%s", RelationGetRelationName(source));

	elog(NOTICE, "with %s", RelationGetRelationName(target));

    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("cannot MERGE PARTITION for relation \"%s\"",
                    RelationGetRelationName(rel)),
             			   errOmitLocation(true)));

}


/* ALTER TABLE ... MODIFY PARTITION */
static void
ATPExecPartModify(Relation rel,
                  AlterPartitionCmd *pc)
{
	AlterPartitionId *pid = (AlterPartitionId *)pc->partid;
	PgPartRule   *prule = NULL;

	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	prule = get_part_rule(rel, pid, true, true, CurrentMemoryContext, NULL,
						  false);

	if (prule)
	{
		AlterPartitionCmd 		*pc2 	= (AlterPartitionCmd *)pc->arg1;
		AlterPartitionCmd 		*pc3 	= (AlterPartitionCmd *)pc2;
		DefElem    				*def 	= (DefElem *)pc3->arg1;
		bool 					 bStart = false;
		bool 					 bEnd 	= false;
		bool 					 bAdd 	= false;
		bool 					 bDrop 	= false;
		char           *parTypName = NULL;

		if (0 == strcmp(def->defname, "START"))
			bStart = true;
		else if (0 == strcmp(def->defname, "END"))
			bEnd = true;
		else if (0 == strcmp(def->defname, "ADD"))
			bAdd = true;
		else if (0 == strcmp(def->defname, "DROP"))
			bDrop = true;
		else Assert(false);

		switch (prule->pNode->part->parkind)
		{
			case 'h': /* hash */
				parTypName = "HASH";
				break;
			case 'r': /* range */
				parTypName = "RANGE";
				break;
			case 'l': /* list */
				parTypName = "LIST";
				break;
			default:
				elog(ERROR, "unrecognized partitioning kind '%c'",
					 prule->pNode->part->parkind);
				Assert(false);
				break;
		} /* end switch */

		/* cannot modify default partitions */
		if (prule->pNode->default_part == prule->topRule)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("cannot MODIFY DEFAULT %s partition%s "
							"for relation \"%s\"",
							parTypName,
							((prule && prule->isName) ? prule->partIdStr : ""),
							RelationGetRelationName(rel)),
									   errOmitLocation(true)));

		if ((bStart || bEnd) && ('r' != prule->pNode->part->parkind))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("invalid use of %s boundary specification "
							"in partition%s of type %s",
							"RANGE",
							((prule && prule->isName) ? prule->partIdStr : ""),
							parTypName
							 ),
									   errOmitLocation(true)));

		if ((bAdd || bDrop) && ('l' != prule->pNode->part->parkind))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("invalid use of %s boundary specification "
							"in partition%s of type %s",
							"LIST",
							((prule && prule->isName) ? prule->partIdStr : ""),
							parTypName
							 ),
									   errOmitLocation(true)));

		if (bAdd || bDrop)
		{
			bool stat = atpxModifyListOverlap(rel, pid, prule,
											  (PartitionElem *)pc3->arg2,
											  bAdd);
			stat = false;
		}
		if (bStart || bEnd)
		{
			bool stat = atpxModifyRangeOverlap(rel, pid, prule,
											   (PartitionElem *)pc3->arg2);
			stat = false;
		}


		if (0)
		parruleord_reset_rank(
				prule->pNode->part->partid,
				prule->pNode->part->parlevel,
				prule->topRule->parparentoid,
				prule->topRule->parruleord,
				CurrentMemoryContext);

		/* MPP-6929: metadata tracking */
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "PARTITION", "MODIFY"
				);
	}

	if (0)
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("cannot MODIFY PARTITION for relation \"%s\"",
                    RelationGetRelationName(rel)),
             			   errOmitLocation(true)));

}

/* ALTER TABLE ... RENAME PARTITION */

static void
ATPExecPartRename(Relation rel,
                  AlterPartitionCmd *pc)
{
	AlterPartitionId 	*pid 	   = (AlterPartitionId *)pc->partid;
	PgPartRule   		*prule 	   = NULL;
	PartitionNode  		*pNode     = NULL;
	AlterPartitionId 	*locPid    = pid;	/* local pid if IDRule */
	PgPartRule* 		 par_prule = NULL;	/* prule for parent if IDRule */
	char 		 		 lRelNameBuf[(NAMEDATALEN*2)];
	char 				*lrelname=NULL;

	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	locPid =
			wack_pid_relname(pid,
							 &pNode,
							 rel,
							 &par_prule,
							 &lrelname,
							 lRelNameBuf);

	prule = get_part_rule(rel, pid, true, true, CurrentMemoryContext, NULL,
						  false);

	if (prule)
	{
		AlterPartitionId		 newpid;
		PgPartRule   		   	*prule2 	 = NULL;
		Relation			 	 targetrelation;
		char        		 	 targetrelname[NAMEDATALEN];
		Relation			 	 parentrelation;
		Oid						 namespaceId;
		char	   			   	*newpartname = strVal(pc->arg1);
		char	   			   	*relname;
		char	  			 	 parentname[NAMEDATALEN];
		int 				 	 partDepth 	 = prule->pNode->part->parlevel;
		RenameStmt 			   	*renStmt 	 = makeNode(RenameStmt);
		DestReceiver 		   	*dest  		 = None_Receiver;
		Relation				 part_rel;
		HeapTuple				 tuple;
		Form_pg_partition_rule	 pgrule;
		List 					*renList 	 = NIL;
		int 					 skipped 	 = 0;
		int 					 renamed 	 = 0;
		cqContext				 cqc;
		cqContext				*pcqCtx;

		newpid.idtype = AT_AP_IDName;
		newpid.partiddef = pc->arg1;
		newpid.location = -1;

		/* ERROR if exists */
		prule2 = get_part_rule1(rel, &newpid, true, false,
								CurrentMemoryContext, NULL,
								pNode,
								lrelname,
								NULL);

		targetrelation = relation_open(prule->topRule->parchildrelid,
									   AccessExclusiveLock);

		StrNCpy(targetrelname, RelationGetRelationName(targetrelation),
				NAMEDATALEN);

		namespaceId = RelationGetNamespace(targetrelation);

		relation_close(targetrelation, AccessExclusiveLock);

		if (0 == prule->topRule->parparentoid)
		{
			StrNCpy(parentname,
					RelationGetRelationName(rel), NAMEDATALEN);
		}
		else
		{
			Assert(par_prule);
			if (par_prule)
			{
				/* look in the parent prule */
				parentrelation =
					RelationIdGetRelation(par_prule->topRule->parchildrelid);
				StrNCpy(parentname,
						RelationGetRelationName(parentrelation), NAMEDATALEN);
				RelationClose(parentrelation);
			}
		}

		/* MPP-3523: the "label" portion of the new relation is
		 * prt_`newpartname', and makeObjectName won't truncate this
		 * portion of the partition name -- it will assert instead.
		 */
		if (strlen(newpartname) > (NAMEDATALEN - 8))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("name \"%s\" for child partition "
							"is too long",
							newpartname)));

		relname = ChoosePartitionName(parentname,
									  partDepth,
									  newpartname,
									  namespaceId);
		/* does CommandCounterIncrement */

		renStmt->renameType = OBJECT_TABLE;
		renStmt->relation = makeRangeVar(NULL /*catalogname*/, get_namespace_name(namespaceId),
										 pstrdup(targetrelname), -1);

		renStmt->subname = NULL;
		renStmt->newname = relname;
		renStmt->bAllowPartn = true; /* allow rename of partitions */

		if (prule && prule->topRule && prule->topRule->children)
				pNode = prule->topRule->children;
		else
				pNode = NULL;

		/* rename the children as well */
		renList = atpxRenameList(pNode, targetrelname, relname, &skipped);

		part_rel = heap_open(PartitionRuleRelationId, RowExclusiveLock);

		pcqCtx = caql_addrel(cqclr(&cqc), part_rel);

		tuple = caql_getfirst(
				pcqCtx,
				cql("SELECT * FROM pg_partition_rule "
					" WHERE oid = :1 "
					" FOR UPDATE ",
					ObjectIdGetDatum(prule->topRule->parruleid)));
		Insist(HeapTupleIsValid(tuple));

		pgrule = (Form_pg_partition_rule)GETSTRUCT(tuple);
		namestrcpy(&(pgrule->parname), newpartname);

		caql_update_current(pcqCtx, tuple);
		/* and Update indexes (implicit) */

		heap_freetuple(tuple);
		heap_close(part_rel, NoLock);

		CommandCounterIncrement();

		ProcessUtility((Node *) renStmt,
					   synthetic_sql,
					   NULL,
					   false, /* not top level */
					   dest, 
					   NULL);

		/* process children if there are any */
		if (renList)
		{
			ListCell 		*lc;

			foreach(lc, renList)
			{
				ListCell 		*lc2;
				List  			*lpair = lfirst(lc);

				lc2 = list_head(lpair);

				renStmt->relation = (RangeVar *)lfirst(lc2);
				lc2 = lnext(lc2);
				renStmt->newname = (char *)lfirst(lc2);

				ProcessUtility((Node *) renStmt,
							   synthetic_sql,
							   NULL, 
							   false, /* not top level */
							   dest, 
							   NULL);
				renamed++;
			}
		}

		/* MPP-6929: metadata tracking */
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "PARTITION", "RENAME"
				);

		/* Notify of name if did not use name for partition id spec */
		if ((pid->idtype != AT_AP_IDName)
			&& prule->isName)
			ereport(NOTICE,
					(errmsg("renamed partition%s to \"%s\" for %s",
							prule->partIdStr,
							newpartname,
							prule->relname)));

		/* MPP-3542: warn when skip child partitions */
		if (skipped)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("renamed %d partitions, skipped %d child partitions due to name truncation",
							renamed, skipped),
									   errOmitLocation(true)));
		}

	}

} /* end ATPExecPartRename */


/* ALTER TABLE ... SET SUBPARTITION TEMPLATE  */
static void
ATPExecPartSetTemplate(AlteredTableInfo *tab,
					   Relation rel,
                       AlterPartitionCmd *pc)
{
	AlterPartitionId	*pid   = (AlterPartitionId *)pc->partid;
	PgPartRule			*prule = NULL;
	int					 lvl   = 1;

	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	/* set template for top level table */
    if (pid && (pid->idtype != AT_AP_IDName))
    {
		Assert((pid->idtype == AT_AP_IDRule) && IsA(pid->partiddef, List));

		/* MPP-5941: work correctly with many levels of templates */
		/* wah! the idrule is invalid, so can't use get_part_rule.
		 * So pull the pgpartrule directly from the idrule (yuck!)
		 */
		prule = (PgPartRule *)linitial((List*)pid->partiddef);

		Assert(prule);

		/* current pnode level is one below current (our parent), and
		 * we want the one above us (our subpartition), so add 2
		 */
		lvl = prule->pNode->part->parlevel + 2;

		Assert (lvl > 1);
	}

	{
		/* relid, level, no parent */
		switch (del_part_template(RelationGetRelid(rel), lvl, 0))
		{
			case 0:
				if (pc->arg1)
				{
					/* no prior template - just add new one */
					ereport(NOTICE,
							(errmsg("%s level %d "
									"subpartition template specification "
									"for relation \"%s\"",
									"adding",
									lvl,
									RelationGetRelationName(rel))));
				}
				else
				{
					/* tried to drop non-existent template */
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
							 errmsg("relation \"%s\" does not have a "
									"level %d "
									"subpartition template specification",
									RelationGetRelationName(rel),
									lvl),
											   errOmitLocation(true)));
				}
				break;
			case 1:
					/* if have new spec,
					 * note old spec is being replaced,
					 * else just note it is dropped
					 */
					ereport(NOTICE,
							(errmsg("%s level %d "
									"subpartition template specification "
									"for relation \"%s\"",
									(pc->arg1) ? "replacing" : "dropped",
									lvl,
									RelationGetRelationName(rel))));
				break;
			default:
					elog(ERROR,
						 "could not drop "
						 "level %d "
						 "subpartition template specification "
						 "for relation \"%s\"",
						 lvl,
						 RelationGetRelationName(rel));
					break;
		}

	}

	if (pc->arg1)
		ATPExecPartAdd(tab, rel, pc, AT_PartSetTemplate);

	/* MPP-6929: metadata tracking */
	MetaTrackUpdObject(RelationRelationId,
					   RelationGetRelid(rel),
					   GetUserId(),
					   "ALTER", "SET SUBPARTITION TEMPLATE"
			);


} /* end ATPExecPartSetTemplate */


static bool
partrule_walker(Node *node, void *context)
{
	part_rule_cxt *p = (part_rule_cxt *)context;

	if (node == NULL)
		return false;
	if (IsA(node, PgPartRule))
	{
		PgPartRule *pg = (PgPartRule *)node;

		partrule_walker((Node *)pg->topRule, p);
		partrule_walker((Node *)pg->pNode, p);
		return false;
	}
	else if (IsA(node, Partition))
	{
		return false;
	}
	else if (IsA(node, PartitionRule))
	{
		PartitionRule *pr = (PartitionRule *)node;

		if (pr->parchildrelid == p->old_oid)
			pr->parchildrelid = p->new_oid;

		return partrule_walker((Node *)pr->children, p);

	}
	else if (IsA(node, PartitionNode))
	{
		PartitionNode *pn = (PartitionNode *)node;
		ListCell *lc;

		partrule_walker((Node *)pn->default_part, p);
		foreach(lc, pn->rules)
		{
			PartitionRule *r = lfirst(lc);
			partrule_walker((Node *)r, p);
		}
		return false;
	}

	return expression_tree_walker(node, partrule_walker, p);
}

/* 
 * Build a basic ResultRelInfo for executing split. We only need
 * the relation descriptor and index information.
 */
static ResultRelInfo *
make_split_resultrel(Relation rel)
{
	ResultRelInfo *rri;

	rri = palloc0(sizeof(ResultRelInfo));
	rri->type = T_ResultRelInfo;
	rri->ri_RelationDesc = rel;
	rri->ri_NumIndices = 0;

	ExecOpenIndices(rri);
	return rri;
}

/*
 * Close indexes and free memory
 */
static void
destroy_split_resultrel(ResultRelInfo *rri)
{
	ExecCloseIndices(rri);

	/* 
	 * Don't do anything with the relation descriptor, that's our caller's job
	 */
	pfree(rri);

}

/*
 * Scan tuples from the temprel (origin, T) and route them to split parts (A, B)
 * based on the constraints.  It is important that the origin may have dropped
 * columns while the new relations will not.
 *
 * This also covers index tuple population.  Note this doesn't handle row OID
 * as it's not allowed in partition.
 */
static void
split_rows(Relation intoa, Relation intob, Relation temprel, List *splits, int segno)
{
	ResultRelInfo *rria = make_split_resultrel(intoa);
	ResultRelInfo *rrib = make_split_resultrel(intob);
	EState *estate = CreateExecutorState();
	TupleDesc		tupdescT = temprel->rd_att;
	TupleTableSlot *slotT = MakeSingleTupleTableSlot(tupdescT);
	HeapScanDesc heapscan = NULL;
	AppendOnlyScanDesc aoscan = NULL;
  ParquetScanDesc parquetscan = NULL;
  bool *aocsproj = NULL;
	MemoryContext oldCxt;
	AppendOnlyInsertDesc aoinsertdesc_a = NULL;
	AppendOnlyInsertDesc aoinsertdesc_b = NULL;
  ParquetInsertDesc parquetinsertdesc_a = NULL;
  ParquetInsertDesc parquetinsertdesc_b = NULL;
	ExprState *achk = NULL;
	ExprState *bchk = NULL;

	/*
	 * Set up for reconstructMatchingTupleSlot.  In split operation,
	 * slot/tupdesc should look same between A and B, but here we don't
	 * assume so just in case, to be safe.
	 */
	rria->ri_partSlot = MakeSingleTupleTableSlot(RelationGetDescr(intoa));
	rrib->ri_partSlot = MakeSingleTupleTableSlot(RelationGetDescr(intob));
	map_part_attrs(temprel, intoa, &rria->ri_partInsertMap, true);
	map_part_attrs(temprel, intob, &rrib->ri_partInsertMap, true);

	/* constr might not be defined if this is a default partition */
	if (intoa->rd_att->constr && intoa->rd_att->constr->num_check)
	{
		List * all_part_constraints = NIL;
		for (int i = 0; i < intoa->rd_att->constr->num_check; i++) 
			all_part_constraints = lappend(all_part_constraints, 
											stringToNode(intoa->rd_att->constr->check[i].ccbin));
		achk = ExecPrepareExpr((Expr *)all_part_constraints, estate);
	}

	if (intob->rd_att->constr && intob->rd_att->constr->num_check)
	{
		List * all_part_constraints = NIL;
		for (int i = 0; i < intob->rd_att->constr->num_check; i++) 
			all_part_constraints = lappend(all_part_constraints, 
											stringToNode(intob->rd_att->constr->check[i].ccbin));
		bchk = ExecPrepareExpr((Expr *)all_part_constraints, estate);
	}

	/* be careful about AO vs. normal heap tables */
	if (RelationIsHeap(temprel))
		heapscan = heap_beginscan(temprel, SnapshotNow, 0, NULL);
	else if (RelationIsAoRows(temprel))
	{
		aoscan = appendonly_beginscan(temprel, SnapshotNow, 0, NULL);
		aoscan->splits = splits;
	}
	else if (RelationIsParquet(temprel))
	{
		int nvp = temprel->rd_att->natts;
		int i;

		aocsproj = (bool *) palloc(sizeof(bool) * nvp);
		for(i=0; i<nvp; ++i)
			aocsproj[i] = true;


		parquetscan = parquet_beginscan(temprel, SnapshotNow, NULL /* relationTupleDesc */, aocsproj);
		parquetscan->splits = splits;
	}
	else
	{
		Assert(false);
	}

	oldCxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

	while (true)
	{
		ExprContext *econtext = GetPerTupleExprContext(estate);
		bool targetIsA;
		Relation targetRelation;
		AppendOnlyInsertDesc *targetAODescPtr;
		ParquetInsertDesc *targetParquetDescPtr;
		TupleTableSlot	   *targetSlot;
		ItemPointer			tid;
		ResultRelInfo	   *targetRelInfo = NULL;
		AOTupleId			aoTupleId;

		/* read next tuple from temprel */
		if (RelationIsHeap(temprel))
		{
			HeapTuple tuple;

			tuple = heap_getnext(heapscan, ForwardScanDirection);
			if (!HeapTupleIsValid(tuple))
				break;

			tuple = heap_copytuple(tuple);
			ExecStoreHeapTuple(tuple, slotT, InvalidBuffer, false);
		}
		else if (RelationIsAoRows(temprel))
		{
			MemTuple mtuple;

			mtuple = appendonly_getnext(aoscan, ForwardScanDirection, slotT);
			if (!PointerIsValid(mtuple))
				break;

			TupClearShouldFree(slotT);
		}
		else if (RelationIsParquet(temprel)){
			parquet_getnext(parquetscan, ForwardScanDirection, slotT);
			if (TupIsNull(slotT))
				break;
		}

		/* prepare for ExecQual */
		econtext->ecxt_scantuple = slotT;

		/* determine if we are inserting into a or b */
		if (achk)
		{
			targetIsA = ExecQual((List *)achk, econtext, false);
		}
		else
		{
			Assert(PointerIsValid(bchk));

			targetIsA = !ExecQual((List *)bchk, econtext, false);
		}

		/* load variables for the specific target */
		if (targetIsA)
		{
			targetRelation = intoa;
			targetAODescPtr = &aoinsertdesc_a;
			targetParquetDescPtr = &parquetinsertdesc_a;
			targetRelInfo = rria;
		}
		else
		{
			targetRelation = intob;
			targetAODescPtr = &aoinsertdesc_b;
			targetParquetDescPtr = &parquetinsertdesc_b;
			targetRelInfo = rrib;
		}

		/*
		 * Map attributes from origin to target.  We should consider dropped
		 * columns in the origin.
		 */
		targetSlot = reconstructMatchingTupleSlot(slotT, targetRelInfo);

		/* insert into the target table */
		if (RelationIsHeap(targetRelation))
		{
			HeapTuple tuple;

			tuple = ExecFetchSlotHeapTuple(targetSlot);
			simple_heap_insert(targetRelation, tuple);

			/* cache TID for later updating of indexes */
			tid = &(((HeapTuple) tuple)->t_self);
		}
		else if (RelationIsAoRows(targetRelation))
		{
			MemTuple	mtuple;
			Oid			tupleOid;

			if (!(*targetAODescPtr))
			{
			  ResultRelSegFileInfo *segfileinfo = NULL;
				MemoryContextSwitchTo(oldCxt);
			  segfileinfo = InitResultRelSegFileInfo(segno, RELSTORAGE_AOROWS, 1);
				*targetAODescPtr = appendonly_insert_init(targetRelation,
														  segfileinfo);
				MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
			}

			mtuple = ExecFetchSlotMemTuple(targetSlot, false);
			appendonly_insert(*targetAODescPtr, mtuple, &tupleOid, &aoTupleId);

			/* cache TID for later updating of indexes */
			tid = (ItemPointer) &aoTupleId;
		}
		else if (RelationIsParquet(targetRelation)){
			if (!*targetParquetDescPtr)
			{
			  ResultRelSegFileInfo *segfileinfo = NULL;
				MemoryContextSwitchTo(oldCxt);
				segfileinfo = InitResultRelSegFileInfo(segno, RELSTORAGE_PARQUET, 1);
				*targetParquetDescPtr = parquet_insert_init(targetRelation,
													  segfileinfo);
				MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
			}

			parquet_insert(*targetParquetDescPtr, targetSlot);

			/* cache TID for later updating of indexes */
			tid = slot_get_ctid(targetSlot);
		}
		else
		{
			Assert(false);
		}

		/*
		 * Insert index for this tuple.
		 * TODO: for performance reason, should we call index_build() instead?
		 */
		if (targetRelInfo->ri_NumIndices > 0)
		{
			estate->es_result_relation_info = targetRelInfo;
			ExecInsertIndexTuples(targetSlot, tid, estate, false);
			estate->es_result_relation_info = NULL;
		}

		/* done, clean up context for this pass */
		ResetExprContext(econtext);
	}

	StringInfo buf = NULL;
	QueryContextDispatchingSendBack sendback = NULL;

	int aocount = 0;
	if (aoinsertdesc_a)
		++aocount;
	if (aoinsertdesc_b)
		++aocount;
	if (parquetinsertdesc_a)
		++aocount;
	if (parquetinsertdesc_b)
		++aocount;

	if (Gp_role == GP_ROLE_EXECUTE && aocount > 0)
		buf = PreSendbackChangedCatalog(aocount);

	if (aoinsertdesc_a)
	{
		sendback = CreateQueryContextDispatchingSendBack(1);
		aoinsertdesc_a->sendback = sendback;
		sendback->relid = RelationGetRelid(aoinsertdesc_a->aoi_rel);

		appendonly_insert_finish(aoinsertdesc_a);

		if (Gp_role == GP_ROLE_EXECUTE)
			AddSendbackChangedCatalogContent(buf, sendback);

		DropQueryContextDispatchingSendBack(sendback);
	}
	if (aoinsertdesc_b)
	{
		sendback = CreateQueryContextDispatchingSendBack(1);
		aoinsertdesc_b->sendback = sendback;
		sendback->relid = RelationGetRelid(aoinsertdesc_b->aoi_rel);

		appendonly_insert_finish(aoinsertdesc_b);

		if (Gp_role == GP_ROLE_EXECUTE)
			AddSendbackChangedCatalogContent(buf, sendback);

		DropQueryContextDispatchingSendBack(sendback);
	}
	if (parquetinsertdesc_a)
	{
		sendback = CreateQueryContextDispatchingSendBack(1);
		parquetinsertdesc_a->sendback = sendback;
		sendback->relid = RelationGetRelid(parquetinsertdesc_a->parquet_rel);

		parquet_insert_finish(parquetinsertdesc_a);

		if (Gp_role == GP_ROLE_EXECUTE)
			AddSendbackChangedCatalogContent(buf, sendback);

		DropQueryContextDispatchingSendBack(sendback);
	}
	if (parquetinsertdesc_b)
	{
		sendback = CreateQueryContextDispatchingSendBack(1);
		parquetinsertdesc_b->sendback = sendback;
		sendback->relid = RelationGetRelid(parquetinsertdesc_b->parquet_rel);

		parquet_insert_finish(parquetinsertdesc_b);

		if (Gp_role == GP_ROLE_EXECUTE)
			AddSendbackChangedCatalogContent(buf, sendback);

		DropQueryContextDispatchingSendBack(sendback);
	}

	if (Gp_role == GP_ROLE_EXECUTE && aocount > 0)
		FinishSendbackChangedCatalog(buf);

	MemoryContextSwitchTo(oldCxt);
	ExecDropSingleTupleTableSlot(slotT);
	ExecDropSingleTupleTableSlot(rria->ri_partSlot);
	ExecDropSingleTupleTableSlot(rrib->ri_partSlot);

	if (rria->ri_partInsertMap)
		pfree(rria->ri_partInsertMap);
	if (rrib->ri_partInsertMap)
		pfree(rrib->ri_partInsertMap);

	if (RelationIsHeap(temprel))
		heap_endscan(heapscan);
	else if (RelationIsAoRows(temprel))
		appendonly_endscan(aoscan);
	else if(RelationIsParquet(temprel)){
		pfree(aocsproj);
		parquet_endscan(parquetscan);
	}

	destroy_split_resultrel(rria);
	destroy_split_resultrel(rrib);
}

/* ALTER TABLE ... SPLIT PARTITION */

/* 
   atpxSplitDropRule: walk the partition rules, eliminating rules that
   match parruleid and paroid.  If topRule is supplied, walk his
   subtree as well (it may differ from pnode if the PgPartRule was
   copied).
*/
static void
atpxSplitDropRule(PartitionNode *pNode, PartitionRule *topRule,
				  Oid parruleid, Oid paroid)
{
	ListCell *lc;
	ListCell *lcprev = NULL, *lcdel = NULL;

	if (!pNode)
		return;

	foreach(lc, pNode->rules)
	{
		PartitionRule *rule = lfirst(lc);

		if (rule->children)
			atpxSplitDropRule(rule->children, NULL, 
							  parruleid, paroid);

		if ((parruleid == rule->parruleid) &&
			(paroid == rule->paroid))
		{
			lcdel = lc; /* should only be one match */
			break;
		}
		lcprev = lc;
	}
	if (lcdel)
		pNode->rules = list_delete_cell(pNode->rules, lcdel, lcprev);

	/* and the default partition */
	if (pNode->default_part)
	{
		PartitionRule *rule = pNode->default_part;

		if (rule->children)
				atpxSplitDropRule(rule->children, NULL,
								  parruleid, paroid);

		if ((parruleid == rule->parruleid) &&
			(paroid == rule->paroid))
			pNode->default_part = NULL;
	}

	/* check optional topRule */
	if (topRule && topRule->children)
		atpxSplitDropRule(topRule->children, NULL,
						  parruleid, paroid);

} /* end atpxSplitDropRule */

/* Given a Relation, make a distributed by () clause for parser consumption. */
List *
make_dist_clause(Relation rel)
{
	int i;
	List *distro = NIL;

	for (i = 0; i < rel->rd_cdbpolicy->nattrs; i++)
	{
		AttrNumber attno = rel->rd_cdbpolicy->attrs[i];
		TupleDesc tupdesc = RelationGetDescr(rel);
		Value *attstr;
		NameData attname;

		attname = tupdesc->attrs[attno - 1]->attname;
		attstr = makeString(pstrdup(NameStr(attname)));

		distro = lappend(distro, attstr);
	}

	if (!distro)
	{
		/* must be random distribution */
		distro = list_make1(NULL);
	}
	return distro;
}

/*
 * Given a relation, get all column encodings for that relation as a list of
 * ColumnReferenceStorageDirective structures.
 */
static List *
rel_get_column_encodings(Relation rel)
{
	List **colencs = RelationGetUntransformedAttributeOptions(rel);
	List *out = NIL;

	if (colencs)
	{
		AttrNumber attno;

		for (attno = 0; attno < RelationGetNumberOfAttributes(rel); attno++)
		{
			if (colencs[attno] && !rel->rd_att->attrs[attno]->attisdropped)
			{
				ColumnReferenceStorageDirective *d =
					makeNode(ColumnReferenceStorageDirective);
				char *colname = pstrdup(NameStr(rel->rd_att->attrs[attno]->attname));
				d->column = makeString(colname);
				d->encoding = colencs[attno];
		
				out = lappend(out, d);
			}
		}
	}
	return out;
}

/*
 * Depending on whether a table is heap, append only or append only column
 * oriented, return NIL, (appendonly=true) or (appendonly=true,
 * orientation=parquet) respectively.
 */
static List *
make_orientation_options(Relation rel)
{
	List *l = NIL;

	if (RelationIsAoRows(rel) ||
		RelationIsParquet(rel) )
	{
		l = lappend(l, makeDefElem("appendonly", (Node *)makeString("true")));

		if (RelationIsParquet(rel))
		{
			l = lappend(l, makeDefElem("orientation",
									   (Node *)makeString("parquet")));
		}
	}
	return l;
}

void
ATPExecPartSplit(Relation rel,
                 AlterPartitionCmd *pc)
{
	Relation temprel = NULL;
	Relation intoa = NULL;
	Relation intob = NULL;
	Oid temprelid;

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CreateStmt *ct = makeNode(CreateStmt);
		char tmpname[NAMEDATALEN];
		InhRelation *inh = makeNode(InhRelation);
		DestReceiver *dest = None_Receiver;
		Node *at = lsecond((List *)pc->arg1);
		AlterPartitionCmd *pc2 = (AlterPartitionCmd *)pc->arg2;
		AlterPartitionId *pid = (AlterPartitionId *)pc->partid;
		AlterTableStmt *ats = makeNode(AlterTableStmt);
		RangeVar *rv;
		AlterTableCmd *cmd = makeNode(AlterTableCmd);
		AlterPartitionCmd *mypc = makeNode(AlterPartitionCmd);
		AlterPartitionCmd *mypc2 = makeNode(AlterPartitionCmd);
		AlterPartitionId *idpid;
		RangeVar *tmprv;
		PgPartRule *prule;
		DropStmt *ds = makeNode(DropStmt);
		List *parsetrees;
		Query *q;
		char *nspname = get_namespace_name(RelationGetNamespace(rel));
		char *relname = get_rel_name(RelationGetRelid(rel));
		Oid relid = RelationGetRelid(rel);
		RangeVar *rva = NULL;
		RangeVar *rvb = NULL;
		int into_exists = 0; /* which into partition exists? */
		int i;
		AlterPartitionId *intopid1 = NULL;
		AlterPartitionId *intopid2 = NULL;
		int default_pos = 0;
		Oid rel_to_drop = InvalidOid;
		AlterPartitionId *aapid = NULL; /* just for alter partition pids */
		Relation existrel;
		List *existstorage_opts;
		ListCell *lc;
		char *defparname = NULL; /* name of default partition (if specified) */
		List *distro = NIL;
		List *colencs = NIL;
		List *orient = NIL;

		/* Get target meta data */
		prule = get_part_rule(rel, pid, true, true,
							  CurrentMemoryContext, NULL, false);
		/*
		 * In order to implement SPLIT, we do the following:
		 *
		 * 1) Build a temporary table T on all nodes.
		 * 2) Exchange that table with the target partition P
		 *    Now, T has all the data or P
		 * 3) Drop partition P
		 * 4) Create two new partitions in the place of the old one
		 */

		/* look up INTO clause info, if the user supplied it */
		if (!pc2) /* no INTO */
		{
			if (prule->topRule->parisdefault)
			{
				defparname = pstrdup(prule->topRule->parname);
				into_exists = 2;
			}
		}
		else /* has INTO clause */
		{
			bool isdef = false;
			bool exists = false;
			char *parname = NULL;

			/*
			 * If we're working on a subpartition, the INTO partition is
			 * actually a child partition of the parent identified by
			 * prule. So, we cannot just use get_part_rule() to determine
			 * if one of them is a default.
			 */

			/* first item */
			if (pid->idtype == AT_AP_IDRule)
			{
				if (prule->pNode->default_part &&
					((AlterPartitionId *)pc2->partid)->idtype == AT_AP_IDDefault)
				{
					isdef = true;
					exists = true;
					parname = prule->pNode->default_part->parname;
					if (!defparname && isdef) 
						defparname = pstrdup(parname);
				}
				else
				{
					ListCell *rc;
					AlterPartitionId *id = (AlterPartitionId *)pc2->partid;

					if (id->idtype == AT_AP_IDDefault)
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_OBJECT),
								 errmsg("relation \"%s\" does not have a "
										"default partition",
										RelationGetRelationName(rel)),
												   errOmitLocation(true)));

					foreach(rc, prule->pNode->rules)
					{
						PartitionRule *r = lfirst(rc);

						if (strcmp(r->parname, strVal((Value *)id->partiddef)) == 0)
						{
							isdef = false;
							exists = true;
							parname = r->parname;
						}
					}

					if (prule->pNode->default_part &&
						strcmp(prule->pNode->default_part->parname,
							   strVal((Value *)id->partiddef)) == 0)
					{
						isdef = true;
						exists = true;
						parname = prule->pNode->default_part->parname;
						if (!defparname && isdef) 
							defparname = pstrdup(parname);
					}
				}
			}
			else /* not a AT_AP_IDRule */
			{
				PgPartRule *tmprule;
				tmprule = get_part_rule(rel, (AlterPartitionId *)pc2->partid,
										false, false, CurrentMemoryContext,
										NULL, false);
				if (tmprule)
				{
					isdef = tmprule->topRule->parisdefault;
					exists = true;
					parname = tmprule->topRule->parname;
					if (!defparname && isdef) 
						defparname = pstrdup(parname);
				}
			}

			if (exists && isdef)
			{
				default_pos = 1;
				intopid2 = (AlterPartitionId *)pc2->partid;
				intopid1 = (AlterPartitionId *)pc2->arg1;
				into_exists = 2;

				if (intopid2->idtype == AT_AP_IDDefault)
					 intopid2->partiddef = (Node *)makeString(pstrdup(parname));
			}
			else
			{
				if (exists)
					into_exists = 1;

				intopid1 = (AlterPartitionId *)pc2->partid;
				intopid2 = (AlterPartitionId *)pc2->arg1;
			}

			/* second item */
			exists = false;
			isdef = false;
			parname = NULL;
			if (pid->idtype == AT_AP_IDRule)
			{
				if (prule->pNode->default_part &&
					((AlterPartitionId *)pc2->arg1)->idtype == AT_AP_IDDefault)
				{
					isdef = true;
					exists = true;
					parname = prule->pNode->default_part->parname;
					if (!defparname && isdef) 
						defparname = pstrdup(parname);
				}
				else
				{
					ListCell *rc;
					AlterPartitionId *id = (AlterPartitionId *)pc2->arg1;

					if (id->idtype == AT_AP_IDDefault)
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_OBJECT),
								 errmsg("relation \"%s\" does not have a "
										"default partition",
										RelationGetRelationName(rel)),
												   errOmitLocation(true)));

					foreach(rc, prule->pNode->rules)
					{
						PartitionRule *r = lfirst(rc);

						if (strcmp(r->parname, strVal((Value *)id->partiddef)) == 0)
						{
							isdef = false;
							exists = true;
							parname = r->parname;
						}
					}

					if (prule->pNode->default_part &&
						strcmp(prule->pNode->default_part->parname,
							   strVal((Value *)id->partiddef)) == 0)
					{
						isdef = true;
						exists = true;
						parname = prule->pNode->default_part->parname;
						if (!defparname && isdef) 
							defparname = pstrdup(parname);
					}

				}
			}
			else
			{
				PgPartRule *tmprule;
				tmprule = get_part_rule(rel, (AlterPartitionId *)pc2->arg1,
										false, false, CurrentMemoryContext,
										NULL, false);
				if (tmprule)
				{
					isdef = tmprule->topRule->parisdefault;
					exists = true;
					parname = tmprule->topRule->parname;
					if (!defparname && isdef) 
						defparname = pstrdup(parname);
				}
			}

			if (exists)
			{
				if (into_exists != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_OBJECT),
							 errmsg("both INTO partitions "
									"already exist"),
							 errOmitLocation(true)));

				into_exists = 2;
				intopid1 = (AlterPartitionId *)pc2->partid;
				intopid2 = (AlterPartitionId *)pc2->arg1;

				if (isdef)
				{
					default_pos = 2;

					if (intopid2->idtype == AT_AP_IDDefault)
						 intopid2->partiddef = (Node *)makeString(parname);
				}
			}
		}

		existrel = heap_open(prule->topRule->parchildrelid, NoLock);
		existstorage_opts = reloptions_list(RelationGetRelid(existrel));
		distro = make_dist_clause(existrel);
		colencs = rel_get_column_encodings(existrel);
		orient = make_orientation_options(existrel);

		heap_close(existrel, NoLock);

		/* 1) Create temp table */
		rv = makeRangeVar(NULL /*catalogname*/, nspname, relname, -1);
		inh->relation = copyObject(rv);
        inh->options = list_make3_int(CREATE_TABLE_LIKE_INCLUDING_DEFAULTS,
									  CREATE_TABLE_LIKE_INCLUDING_CONSTRAINTS,
									  CREATE_TABLE_LIKE_INCLUDING_INDEXES);
		ct->tableElts = list_make1(inh);
		ct->distributedBy = list_copy(distro); /* must preserve the list for later */

		/* should be unique enough */
		snprintf(tmpname, NAMEDATALEN, "pg_temp_%u", relid);
		tmprv = makeRangeVar(NULL /*catalogname*/, nspname, tmpname, -1);
		ct->relation = tmprv;
		ct->relKind = RELKIND_RELATION;
		ct->ownerid = rel->rd_rel->relowner;
		ct->is_split_part = true;
		parsetrees = parse_analyze((Node *)ct, NULL, NULL, 0);

		q = (Query *)linitial(parsetrees);
		ProcessUtility((Node *)q->utilityStmt,
					   synthetic_sql,
					   NULL,
					   false, /* not top level */
					   dest, 
					   NULL);
		CommandCounterIncrement();

		/* get the oid of the temporary table */
		temprelid = get_relname_relid(tmpname,
									  RelationGetNamespace(rel));

		if (pid->idtype == AT_AP_IDRule)
		{
			idpid = copyObject(pid);
		}
		else
		{
			idpid = makeNode(AlterPartitionId);
			idpid->idtype = AT_AP_IDRule;
			idpid->partiddef = (Node *)list_make2((Node *)prule, pid);
			idpid->location  = -1;
		}

		/* 2) EXCHANGE temp with target */
		rel_to_drop = prule->topRule->parchildrelid;

		ats->relation = copyObject(rv);
		ats->relkind = OBJECT_TABLE;

		cmd->subtype = AT_PartExchange;

		mypc->partid = (Node *)idpid;
		mypc->arg1 = (Node *)tmprv;
		mypc->arg2 = (Node *)mypc2;

		mypc2->arg1 = (Node *)makeInteger(0);
		mypc2->arg2 = (Node *)makeInteger(1); /* tell them we're SPLIT */
		cmd->def = (Node *)mypc;
		ats->cmds = list_make1(cmd);
		parsetrees = parse_analyze((Node *)ats, NULL, NULL, 0);
		Assert(list_length(parsetrees) == 1);
		q = (Query *)linitial(parsetrees);

		heap_close(rel, NoLock);
		ProcessUtility((Node *)q->utilityStmt,
					   synthetic_sql,
					   NULL, 
					   false, /* not top level */
					   dest, 
					   NULL);
		rel = heap_open(relid, AccessExclusiveLock);
		CommandCounterIncrement();

		/* 3) drop the old partition */
		if (pid->idtype == AT_AP_IDRule)
		{
			PgPartRule *pr;
			part_rule_cxt cxt;

			idpid = copyObject(pid);


			/* need to update the OID reference */
			pr = linitial((List *)idpid->partiddef);
			cxt.old_oid = rel_to_drop;
			cxt.new_oid = temprelid;
			partrule_walker((Node *)pr, (void *)&cxt);
			elog(DEBUG5, "dropping OID %u", temprelid);
		}
		else
		{
			/* refresh prule, out of date due to EXCHANGE */
			prule = get_part_rule(rel, pid, true, true,
								  CurrentMemoryContext, NULL, false);
			idpid = makeNode(AlterPartitionId);
			idpid->idtype = AT_AP_IDRule;
			idpid->partiddef = (Node *)list_make2((Node *)prule, pid);
			idpid->location  = -1;
		}

		cmd->subtype = AT_PartDrop;

		ds->missing_ok = false;
		ds->behavior = DROP_RESTRICT;
		ds->removeType = OBJECT_TABLE;

		mypc->partid = (Node *)idpid;
		mypc->arg1 = (Node *)ds;
		/* MPP-6589: make DROP work, even if last one
		 * NOTE: hateful hackery.  Normally, the arg2 for the PartDrop
		 * cmd is null.  But since SPLIT may need to DROP the last
		 * partition before it re-ADDs two new ones, we pass a non-null
		 * arg2 as a flag to enable ForceDrop in ATPExecPartDrop().  
		*/
		mypc->arg2 = (Node *)makeNode(AlterPartitionCmd);

		cmd->def = (Node *)mypc;
		parsetrees = parse_analyze((Node *)ats, NULL, NULL, 0);
		heap_close(rel, NoLock);

		/* 
		 * Might have expanded to multiple statements if, for example, the
		 * master table has indexes on it.
		 */
		foreach(lc, parsetrees)
		{
			q = (Query *)lfirst(lc);

			ProcessUtility((Node *)q->utilityStmt, 
						   synthetic_sql,
						   NULL, 
						   false, /* not top level */
						   dest, 
						   NULL);
		}

		rel = heap_open(relid, AccessExclusiveLock);
		CommandCounterIncrement();

		/*
		 * Now that we've dropped the partition, we need to handle updating
		 * the PgPartRule pid for the case where it is a pid representing
		 * ALTER PARTITION ... ALTER PARTITION ...
		 *
		 * For the normal case, we'll just run get_part_rule() again deeper
		 * in the code.
		 */
		if (pid->idtype == AT_AP_IDRule)
		{
			AlterPartitionId *tmppid = copyObject(pid);

			/* MPP-10223: pid contains a "stale" pNode with a
			 * partition rule for the partition we just dropped.
			 * Delve deep into pNode to eliminate the topRule.
			 */
			if (1)
			{
				AlterPartitionId		*newpid	 = tmppid;
				PgPartRule				*tmprule = NULL;

				while (tmppid->idtype == AT_AP_IDRule)
				{
					List *l = (List *)tmppid->partiddef;

					/* wipe out the topRule because the partition was
					 * dropped, or PartAdd will find it and complain!!
					 * Note that because the pid was copied,
					 * tmprule->topRule has a separate copy of the
					 * prule subtree, so we need to fix this one, too.
					 */
					tmprule = (PgPartRule *)linitial(l);

					atpxSplitDropRule(tmprule->pNode, 
									  tmprule->topRule,
									  prule->topRule->parruleid, 
									  prule->topRule->paroid);
					tmppid = lsecond(l);
				}
				
				tmppid = newpid;
			}

			aapid = tmppid;
		}

		/* Add two new partitions */
		elog(DEBUG5, "Split partition: adding two new partitions");

		/* 4) add two new partitions, via a loop to reduce code duplication */
		for (i = 1; i <= 2; i++)
		{
			/* build up commands for adding two. */
			AlterPartitionId *mypid = makeNode(AlterPartitionId);
			CreateStmt *mycs = makeNode(CreateStmt);
			char *parname = NULL;
			AlterPartitionId *intopid = NULL;
			PartitionElem *pelem = makeNode(PartitionElem);
			Oid newchildrelid = InvalidOid;
			AlterPartitionCmd *storenode = NULL;

			/* use storage options for existing rel */
			if (existstorage_opts)
			{
				storenode = makeNode(AlterPartitionCmd);
				storenode->arg1 = (Node *)existstorage_opts;
				storenode->location = -1;
			}
			pelem->storeAttr = (Node *)storenode;

			if (pc2)
			{
				if (i == 1)
					intopid = intopid1;
				else
					intopid = intopid2;
			}
			else
			{
				if (into_exists == i && prule->topRule->parname)
					parname = pstrdup(prule->topRule->parname);
			}

			mycs->relation = makeRangeVar(NULL /*catalogname*/, NULL, "fake_partition_name", -1);
			mycs->relKind = RELKIND_RELATION;

			/*
			 * If the new partition is column oriented, initialize the column
			 * entity list to the set of column encodings. Latter in parse
			 * analysis of this statement, we will append to this list a LIKE
			 * clause to clone the other column attributes.
			 *
			 * Be careful to copy the list since it is modified by parse
			 * analysis.
			 */
			if (colencs)
				mycs->tableElts = list_copy(colencs);

			mycs->options = orient;

			mypid->idtype = AT_AP_IDNone;
			mypid->location = -1;
			mypid->partiddef = NULL;

			mypc->partid = (Node *)mypid;

			if (intopid)
				parname = strVal(intopid->partiddef);

			if (prule->topRule->parisdefault && i == into_exists)
			{
				/* nothing to do */
			}
			else
			{
				if (prule->pNode->part->parkind == 'r')
				{
					PartitionBoundSpec *a = makeNode(PartitionBoundSpec);

					if (prule->topRule->parisdefault)
					{
						a->partStart = linitial((List *)pc->arg1);
						a->partEnd = lsecond((List *)pc->arg1);
					}
					else if (i == 1)
					{
						PartitionRangeItem *ri;

						/* MPP-6589: if the partition has an "open"
						 * START, pass a NULL partStart 
						 */
						if (prule->topRule &&
							prule->topRule->parrangestart)
						{
							ri = makeNode(PartitionRangeItem);
							ri->location = -1;
							ri->everycount = 0;
							ri->partRangeVal = 
									copyObject(prule->topRule->parrangestart);
							ri->partedge =
									prule->topRule->parrangestartincl ? 
									PART_EDGE_INCLUSIVE : PART_EDGE_EXCLUSIVE;

							a->partStart = (Node *)ri;
						}
						else
							a->partStart = NULL;

						ri = makeNode(PartitionRangeItem);
						ri->location = -1;
						ri->everycount = 0;
						ri->partRangeVal = copyObject(at);
						ri->partedge = PART_EDGE_EXCLUSIVE;

						a->partEnd = (Node *)ri;
					}
					else if (i == 2)
					{
						PartitionRangeItem *ri;
						ri = makeNode(PartitionRangeItem);
						ri->location = -1;
						ri->everycount = 0;
						ri->partRangeVal = copyObject(at);
						ri->partedge = PART_EDGE_INCLUSIVE;

						a->partStart = (Node *)ri;

						/* MPP-6589: if the partition has an "open"
						 * END, pass a NULL partEnd 
						 */
						if (prule->topRule &&
							prule->topRule->parrangeend)
						{
							ri = makeNode(PartitionRangeItem);
							ri->location = -1;
							ri->everycount = 0;
							ri->partRangeVal =
									copyObject(prule->topRule->parrangeend);
							ri->partedge = prule->topRule->parrangeendincl ? 
									PART_EDGE_INCLUSIVE : PART_EDGE_EXCLUSIVE;

							a->partEnd = (Node *)ri;
						}
						else
							a->partEnd = NULL;

					}
					pelem->boundSpec = (Node *)a;
				}
				else
				{
					if ((into_exists && into_exists != i &&
						 prule->topRule->parisdefault) ||
						(i == 2 && !prule->topRule->parisdefault))
					{
						PartitionValuesSpec *valuesspec =
							makeNode(PartitionValuesSpec);

						valuesspec->partValues = copyObject(at);
						valuesspec->location = -1;
						pelem->boundSpec = (Node *)valuesspec;
					}
					else
					{
						PartitionValuesSpec *valuesspec =
							makeNode(PartitionValuesSpec);
						List *lv = prule->topRule->parlistvalues;
						List *newvals = NIL;
						ListCell *lc;
						List *atlist = (List *)at;

						/* must be LIST */
						Assert(prule->pNode->part->parkind == 'l');

						/*
						 * Iterate through list of existing constraints and
						 * pick out those not in AT() clause
						 */
						foreach(lc, lv)
						{
							List *cols = lfirst(lc);
							ListCell *parvals = list_head(atlist);
							int16 nkeys = prule->pNode->part->parnatts;
							bool found = false;

							while (parvals)
							{
								List *vals = lfirst(parvals);
								ListCell *lcv = list_head(vals);
								ListCell *lcc = list_head(cols);
								int parcol;
								bool matched = true;

								for (parcol = 0; parcol < nkeys; parcol++)
								{
									Oid opclass =
										prule->pNode->part->parclass[parcol];
									Oid funcid = get_opclass_proc(opclass, 0,
																  BTORDER_PROC);
									Const *v = lfirst(lcv);
									Const *c = lfirst(lcc);
									Datum d;

									if (v->constisnull && c->constisnull)
										continue;
									else if (v->constisnull || c->constisnull)
									{
										matched = false;
										break;
									}

									d = OidFunctionCall2(funcid, c->constvalue,
														 v->constvalue);

									if (DatumGetInt32(d) != 0)
									{
										matched = false;
										break;
									}

									lcv = lnext(lcv);
									lcc = lnext(lcc);
								}

								if (matched)
								{
									found = true;
									break;
								}

								parvals = lnext(parvals);
							}
							if (!found)
							{
								/* Not in AT() clause, so keep it */
								newvals = lappend(newvals,
										copyObject((Node *)cols));
							}
						}
						valuesspec->partValues = newvals;
						valuesspec->location = -1;
						pelem->boundSpec = (Node *)valuesspec;
					}
				}
			}

			/*
			 * We always want to create a partition name because we
			 * need to recall the partition details below.
			 */
			if (!parname)
			{
				char n[NAMEDATALEN];

				snprintf(n, NAMEDATALEN, "r%lu", random());
				parname = pstrdup(n);

			}

			pelem->partName = (Node *)makeString(parname);
			mypid->partiddef = pelem->partName;
			mypid->idtype = AT_AP_IDName;

			pelem->location  = -1;
			/* MPP-10421: determine if new partition, re-use of old
			 * partition, and/or is default partition 
			 */
			pelem->isDefault = (defparname && 
								parname &&
								(0 == strcmp(parname, defparname)));

			/* tell ADD PARTITION we're doing a SPLIT */
			mypc2->partid = (Node *)makeInteger(1);

			mypc2->arg1 = (Node *)pelem;
			mypc2->arg2 = (Node *)list_make1(mycs);
			mypc2->location = -1;

			mypc->arg1 = (Node *)makeInteger(pelem->isDefault ? 1 : 0);
			mypc->arg2 = (Node *)mypc2;
			mypc->location = -1;

			cmd->subtype = AT_PartAdd;
			cmd->def = (Node *)mypc;

			/* turn this into ALTER PARTITION if need be */
			if (pid->idtype == AT_AP_IDRule)
			{
				AlterTableCmd *ac = makeNode(AlterTableCmd);
				AlterPartitionCmd *ap = makeNode(AlterPartitionCmd);

				ac->subtype = AT_PartAlter;
				ac->def = (Node *)ap;
				ap->partid = (Node *)aapid;
				ap->arg1 = (Node *)cmd; /* embed the real command */
				ap->location = -1;

				cmd = ac;
			}

			ats->cmds = list_make1(cmd);
			parsetrees = parse_analyze((Node *)ats, NULL, NULL, 0);
			Assert(list_length(parsetrees) == 1);
			q = (Query *)linitial(parsetrees);

			heap_close(rel, NoLock);
			ProcessUtility((Node *)q->utilityStmt, 
						   synthetic_sql,
						   NULL, 
						   false, /* not top level */
						   dest, 
						   NULL);
			rel = heap_open(relid, AccessExclusiveLock);

			/* make our change visible */
			CommandCounterIncrement();

			/* get the rule back which ADD PARTITION just created */
			if (pid->idtype == AT_AP_IDRule)
			{
				cqContext cqc;
				int fetchCount;
				Relation rulerel = heap_open(PartitionRuleRelationId,
											 AccessShareLock);

				Datum d = DirectFunctionCall1(namein,
							CStringGetDatum(strVal(pelem->partName)));

				/* XXX XXX: SnapshotSelf - but we just did a
				 * CommandCounterIncrement()
				 */
				newchildrelid = caql_getoid_plus(
						caql_snapshot(caql_addrel(cqclr(&cqc), rulerel), 
								  SnapshotSelf), 
						&fetchCount,
						NULL,
						cql("SELECT parchildrelid FROM pg_partition_rule "
							" WHERE paroid = :1 "
							" AND parparentrule = :2 "
							" AND parname = :3 ",
							ObjectIdGetDatum(prule->topRule->paroid),
							ObjectIdGetDatum(prule->topRule->parparentoid),
							d));

				Insist(fetchCount);

				heap_close(rulerel, NoLock);
			}
			else
			{
				PgPartRule *tmprule;
				tmprule = get_part_rule(rel, mypid, true, true,
									    CurrentMemoryContext, NULL, false);
				newchildrelid = tmprule->topRule->parchildrelid;
			}

			Assert(OidIsValid(newchildrelid));

			if (i == 1)
			{
				intoa = heap_open(newchildrelid,
								  AccessExclusiveLock);

				rva = makeRangeVar(
							NULL /*catalogname*/, 
							get_namespace_name(RelationGetNamespace(intoa)),
						    get_rel_name(RelationGetRelid(intoa)), -1);
			}
			else
			{
				intob = heap_open(newchildrelid,
								  AccessExclusiveLock);

				rvb = makeRangeVar(
							NULL /*catalogname*/, 	
							get_namespace_name(RelationGetNamespace(intob)),
						    get_rel_name(RelationGetRelid(intob)), -1);

			}
		}

		temprel = heap_open(rel_to_drop, AccessExclusiveLock);

		/* update parse tree with info for the QEs */
		Assert(PointerIsValid(rva));
		Assert(PointerIsValid(rvb));

		/* update for consumption by QEs */
		pc->partid = (Node *)makeInteger(RelationGetRelid(temprel));
		pc->arg1 = (Node *)copyObject(rva);
		pc->arg2 = (Node *)copyObject(rvb);

		/* MPP-6929: metadata tracking */
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "PARTITION", "SPLIT"
				);

	} /* end if dispatch */
	else if (Gp_role == GP_ROLE_EXECUTE)
	{
		Oid temprelid;
		RangeVar *rva;
		RangeVar *rvb;

		temprelid = (Oid)intVal((Value *)pc->partid);
		temprel = heap_open(temprelid, AccessExclusiveLock);
		rva = (RangeVar *)pc->arg1;
		intoa = heap_openrv(rva, AccessExclusiveLock);
		rvb = (RangeVar *)pc->arg2;
		intob = heap_openrv(rvb, AccessExclusiveLock);
	}
	else
		return;

	/*
	 * Now, on every node, scan the exchanged out table, splitting data
	 * between two new partitions.
	 *
	 * To do this, we use the CHECK constraints we just created on the
	 * tables via ADD PARTITION.
	 */
	if (Gp_role == GP_ROLE_EXECUTE)
	{
    List *split = NIL;
    int segno = 0;
    if (RelationIsAoRows(temprel) || RelationIsParquet(temprel))
    {
      split = GetFileSplitsOfSegment(pc->scantable_splits,
            temprel->rd_id, GetQEIndex());
      segno = list_nth_int(pc->newpart_aosegnos, GetQEIndex());
    }
	  split_rows(intoa, intob, temprel, split, segno);
	}

	/* 
	 * In HAWQ, we need to dispatch the splitting of rows to segments. Most other
	 * ALTER operations, dispatch the ALTER phase 3 work to segments. ALTER .. SPLIT PARTITION
	 * is different as the actual working of splitting happens in phase 2. Hence we dispatch
	 * at this point 
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		
		QueryContextInfo *contextdisp;
    QueryResource *resource = NULL;
    DispatchDataResult    result;
    List *segment_segnos;
    List *scantable_splits;
    QueryResource *savedResource = NULL;

    int target_segment_num = -1;

    /*
     * make sure all three relations have the same
     * bucket number.
     */
    {
      GpPolicy *targetPolicy = NULL;

      targetPolicy = GpPolicyFetch(CurrentMemoryContext, (Oid)intVal((Value *)pc->partid));
      Assert(targetPolicy);
      target_segment_num = targetPolicy->bucketnum;
      pfree(targetPolicy);

      targetPolicy = GpPolicyFetch(CurrentMemoryContext, intoa->rd_id);
      Assert(targetPolicy);
      if (target_segment_num != targetPolicy->bucketnum)
      {
        pfree(targetPolicy);
        elog(ERROR, "target segment num %d IS NOT equal to the bucket number of this relation %d", target_segment_num, targetPolicy->bucketnum);
      }
      pfree(targetPolicy);

      targetPolicy = GpPolicyFetch(CurrentMemoryContext, intob->rd_id);
      Assert(targetPolicy);
      if (target_segment_num != targetPolicy->bucketnum)
      {
        pfree(targetPolicy);
        elog(ERROR, "target segment num %d IS NOT equal to the bucket number of this relation %d", target_segment_num, targetPolicy->bucketnum);
      }
      pfree(targetPolicy);

    }

    resource = AllocateResource(QRL_ONCE, 1, 1, target_segment_num, target_segment_num, NULL, 0);
    savedResource = GetActiveQueryResource();
    SetActiveQueryResource(resource);
    segment_segnos = SetSegnoForWrite(NIL, 0, target_segment_num, true, false);
    scantable_splits = NIL;

		/* create the segfiles for the new relations here */
		CreateAppendOnlyParquetSegFileForRelationOnMaster(intoa, segment_segnos);

		CreateAppendOnlyParquetSegFileForRelationOnMaster(intob, segment_segnos);

		/* prepare for the metadata dispatch */	
		contextdisp = CreateQueryContextInfo();
		
		prepareDispatchedCatalogSingleRelation(contextdisp, 
							intoa->rd_id,
							true, 
							segment_segnos);

		prepareDispatchedCatalogSingleRelation(contextdisp, 
							intob->rd_id,
							true, 
							segment_segnos);

		prepareDispatchedCatalogRelation(contextdisp,
							(Oid)intVal((Value *)pc->partid), 
							false,
							NULL);

    /*
     * Dispatch split-related metadata.
     */
    scantable_splits = AssignAOSegFileSplitToSegment((Oid)intVal((Value *)pc->partid),
              NIL, true, target_segment_num, scantable_splits);

    pc->scantable_splits = scantable_splits;
    pc->newpart_aosegnos = segment_segnos;

    FinalizeQueryContextInfo(contextdisp);
    dispatch_statement_node((Node *) pc, contextdisp, resource, &result);
    cdbdisp_iterate_results_sendback(result.result, result.numresults,
                UpdateCatalogModifiedOnSegments);
    dispatch_free_result(&result);
    DropQueryContextInfo(contextdisp);
    FreeResource(resource);
    SetActiveQueryResource(savedResource);
	}

	elog(DEBUG5, "dropping temp rel %s", RelationGetRelationName(temprel));
	temprelid = RelationGetRelid(temprel);
	heap_close(temprel, NoLock);

	/* 
	 * In HAWQ, we only need to drop the temp relation on the master which will ensure
	 * the relation is dropped on segments as well. 
 	 */
	if (Gp_role != GP_ROLE_EXECUTE)
	{
		ObjectAddress addr;
		addr.classId = RelationRelationId;
		addr.objectId = temprelid;
		addr.objectSubId = 0;

		performDeletion(&addr, DROP_RESTRICT);
	}

	heap_close(intoa, NoLock);
	heap_close(intob, NoLock);
}

/* ALTER TABLE ... TRUNCATE PARTITION */
static List *
atpxTruncateList(Relation rel, PartitionNode *pNode)
{
	List *l1 = NIL;
	ListCell *lc;

	if (!pNode)
		return l1;

	/* add the child lists first */
	foreach(lc, pNode->rules)
	{
		PartitionRule *rule = lfirst(lc);
		List *l2 = NIL;

		if (rule->children)
			l2 = atpxTruncateList(rel, rule->children);
		else
			l2 = NIL;

		if (l2)
		{
			if (l1)
				l1 = list_concat(l1, l2);
			else
				l1 = l2;
		}
	}

	/* and the default partition */
	if (pNode->default_part)
	{
		PartitionRule *rule = pNode->default_part;
		List *l2 = NIL;

		if (rule->children)
			l2 = atpxTruncateList(rel, rule->children);
		else
			l2 = NIL;

		if (l2)
		{
			if (l1)
				l1 = list_concat(l1, l2);
			else
				l1 = l2;
		}
	}

	/* add entries for rules at current level */
	foreach(lc, pNode->rules)
	{
		PartitionRule 	*rule = lfirst(lc);
		RangeVar 		*rv;
		Relation		 rel;

		rel = heap_open(rule->parchildrelid, AccessShareLock);

		rv = makeRangeVar(NULL /*catalogname*/, get_namespace_name(RelationGetNamespace(rel)),
						  pstrdup(RelationGetRelationName(rel)), -1);

		heap_close(rel, NoLock);

		if (l1)
			l1 = lappend(l1, rv);
		else
			l1 = list_make1(rv);
	}

	/* and the default partition */
	if (pNode->default_part)
	{
		PartitionRule 	*rule = pNode->default_part;
		RangeVar 		*rv;
		Relation		 rel;

		rel = heap_open(rule->parchildrelid, AccessShareLock);
		rv = makeRangeVar(NULL /*catalogname*/, get_namespace_name(RelationGetNamespace(rel)),
						  pstrdup(RelationGetRelationName(rel)), -1);

		heap_close(rel, NoLock);

		if (l1)
			l1 = lappend(l1, rv);
		else
			l1 = list_make1(rv);
	}

	return l1;
} /* end atpxTruncateList */

static void
ATPExecPartTruncate(Relation rel,
                    AlterPartitionCmd *pc)
{
	AlterPartitionId *pid = (AlterPartitionId *)pc->partid;
	PgPartRule   *prule = NULL;

	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	prule = get_part_rule(rel, pid, true, true, CurrentMemoryContext, NULL,
						  false);

	if (prule)
	{
		RangeVar 		*rv;
		TruncateStmt 	*ts   = (TruncateStmt *)pc->arg1;
		DestReceiver 	*dest = None_Receiver;
		Relation	  	 rel2;

		rel2 = heap_open(prule->topRule->parchildrelid, AccessShareLock);
		rv = makeRangeVar(NULL /*catalogname*/, get_namespace_name(RelationGetNamespace(rel2)),
						  pstrdup(RelationGetRelationName(rel2)), -1);

		rv->location = pc->location;

		if (prule->topRule->children)
		{
			List *l1 = atpxTruncateList(rel2, prule->topRule->children);

			ts->relations = lappend(l1, rv);
		}
		else
			ts->relations = list_make1(rv);

		heap_close(rel2, NoLock);

		ProcessUtility( (Node *) ts,
					   synthetic_sql,
					   NULL, 
					   false, /* not top level */
					   dest, 
					   NULL);

		/* Notify of name if did not use name for partition id spec */
		if (prule && prule->topRule && prule->topRule->children
			&& (ts->behavior != DROP_CASCADE ))
		{
			ereport(NOTICE,
					(errmsg("truncated partition%s for %s and its children",
							prule->partIdStr,
							prule->relname),
									   errOmitLocation(true)));
		}
		else if ((pid->idtype != AT_AP_IDName)
				 && prule->isName)
				ereport(NOTICE,
						(errmsg("truncated partition%s for %s",
								prule->partIdStr,
								prule->relname),
										   errOmitLocation(true)));
	}

} /* end ATPExecPartTruncate */

/*
 * Execute ALTER TABLE SET SCHEMA
 *
 * WARNING WARNING WARNING: In previous *minor* releases the caller was
 * responsible for checking ownership of the relation, but now we do it here.
 */
void
AlterTableNamespace(RangeVar *relation, const char *newschema)
{
	Relation	rel;
	Oid			relid;
	Oid			oldNspOid;
	Oid			nspOid;

	/* make sure this is not an hcatalog table */
	relid = RangeVarGetRelid(relation, false /*failOk*/, false /*allowHcatalog*/);
	rel = heap_openrv(relation, AccessExclusiveLock);
	CheckRelationOwnership(relid, true);

	oldNspOid = RelationGetNamespace(rel);

	/* heap_openrv allows TOAST, but we don't want to */
	if (rel->rd_rel->relkind == RELKIND_TOASTVALUE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is a TOAST relation",
						RelationGetRelationName(rel)),
								   errOmitLocation(true)));

	if (rel->rd_rel->relkind == RELKIND_AOSEGMENTS)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is an Append Only segment listing relation",
						RelationGetRelationName(rel)),
								   errOmitLocation(true)));

	if (rel->rd_rel->relkind == RELKIND_AOBLOCKDIR)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is an Append Only block directory relation",
						RelationGetRelationName(rel)),
								   errOmitLocation(true)));

	/* if it's an owned sequence, disallow moving it by itself */
	if (rel->rd_rel->relkind == RELKIND_SEQUENCE)
	{
		Oid			tableId;
		int32		colId;

		if (sequenceIsOwned(relid, &tableId, &colId))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot move an owned sequence into another schema"),
					 errdetail("Sequence \"%s\" is linked to table \"%s\".",
							   RelationGetRelationName(rel),
							   get_rel_name(tableId)),
									   errOmitLocation(true)));
	}

	/* get schema OID and check its permissions */
	nspOid = LookupCreationNamespace(newschema);

	if (oldNspOid == nspOid)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" is already in schema \"%s\"",
						RelationGetRelationName(rel),
						newschema),
								   errOmitLocation(true)));

	/* disallow renaming into or out of temp schemas */
	if (isAnyTempNamespace(nspOid) || isAnyTempNamespace(oldNspOid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("cannot move objects into or out of temporary schemas"),
					   errOmitLocation(true)));

	/* same for TOAST schema */
	if (nspOid == PG_TOAST_NAMESPACE || oldNspOid == PG_TOAST_NAMESPACE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot move objects into or out of TOAST schema"),
						   errOmitLocation(true)));

	/* same for AO SEGMENT schema */
	if (nspOid == PG_AOSEGMENT_NAMESPACE || oldNspOid == PG_AOSEGMENT_NAMESPACE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot move objects into or out of AO SEGMENT schema"),
						   errOmitLocation(true)));


	/* OK, modify the pg_class row and pg_depend entry */
	AlterRelationNamespaceInternalTwo(rel, relid,
									  oldNspOid, nspOid,
									  true, newschema);
	
	/* MPP-7825, MPP-6929, MPP-7600: metadata tracking */
	if ((Gp_role == GP_ROLE_DISPATCH)
		&& MetaTrackValidKindNsp(rel->rd_rel))
		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(rel),
						   GetUserId(),
						   "ALTER", "SET SCHEMA"
				);

	/* close rel, but keep lock until commit */
	relation_close(rel, NoLock);
}

static void
AlterRelationNamespaceInternalTwo(Relation rel,
								  Oid relid,
								  Oid oldNspOid, Oid newNspOid,
								  bool hasDependEntry,
								  const char *newschema)
{
	Relation classRel;

	/* OK, modify the pg_class row and pg_depend entry */
	classRel = heap_open(RelationRelationId, RowExclusiveLock);

	AlterRelationNamespaceInternal(classRel, relid, oldNspOid, newNspOid, 
								   hasDependEntry);

	/* Fix the table's rowtype too */
	AlterTypeNamespaceInternal(rel->rd_rel->reltype, newNspOid, false);

	/* Fix other dependent stuff */
	if (rel->rd_rel->relkind == RELKIND_RELATION)
	{
		AlterIndexNamespaces(classRel, rel, oldNspOid, newNspOid);
		AlterSeqNamespaces(classRel, rel, oldNspOid, newNspOid, newschema);
		AlterConstraintNamespaces(relid, oldNspOid, newNspOid, false);
	}
	heap_close(classRel, RowExclusiveLock);
}

/*
 * The guts of relocating a relation to another namespace: fix the pg_class
 * entry, and the pg_depend entry if any.  Caller must already have
 * opened and write-locked pg_class.
 */
void
AlterRelationNamespaceInternal(Relation classRel, Oid relOid,
							   Oid oldNspOid, Oid newNspOid,
							   bool hasDependEntry)
{
	HeapTuple	classTup;
	Form_pg_class classForm;
	cqContext	cqc;
	cqContext  *pcqCtx;

	Assert(RelationGetRelid(classRel) == RelationRelationId);

	pcqCtx = caql_addrel(cqclr(&cqc), classRel);

	classTup = caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(relOid)));

	if (!HeapTupleIsValid(classTup))
		elog(ERROR, "cache lookup failed for relation %u", relOid);
	classForm = (Form_pg_class) GETSTRUCT(classTup);

	Assert(classForm->relnamespace == oldNspOid);

	/* check for duplicate name (more friendly than unique-index failure) */
	if (get_relname_relid(NameStr(classForm->relname),
						  newNspOid) != InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" already exists in schema \"%s\"",
						NameStr(classForm->relname),
						get_namespace_name(newNspOid)),
								   errOmitLocation(true)));

	/* classTup is a copy, so OK to scribble on */
	classForm->relnamespace = newNspOid;

	caql_update_current(pcqCtx, classTup);
	/* and Update indexes (implicit) */

	/* Update dependency on schema if caller said so */
	if (hasDependEntry &&
		changeDependencyFor(RelationRelationId, relOid,
							NamespaceRelationId, oldNspOid, newNspOid) != 1)
		elog(ERROR, "failed to change schema dependency for relation \"%s\"",
			 NameStr(classForm->relname));

	heap_freetuple(classTup);
}

/*
 * Move all indexes for the specified relation to another namespace.
 *
 * Note: we assume adequate permission checking was done by the caller,
 * and that the caller has a suitable lock on the owning relation.
 */
static void
AlterIndexNamespaces(Relation classRel, Relation rel,
					 Oid oldNspOid, Oid newNspOid)
{
	List	   *indexList;
	ListCell   *l;

	indexList = RelationGetIndexList(rel);

	foreach(l, indexList)
	{
		Oid			indexOid = lfirst_oid(l);

		/*
		 * Note: currently, the index will not have its own dependency on the
		 * namespace, so we don't need to do changeDependencyFor(). There's no
		 * rowtype in pg_type, either.
		 */
		AlterRelationNamespaceInternal(classRel, indexOid,
									   oldNspOid, newNspOid,
									   false);
	}

	list_free(indexList);
}

/*
 * Move all SERIAL-column sequences of the specified relation to another
 * namespace.
 *
 * Note: we assume adequate permission checking was done by the caller,
 * and that the caller has a suitable lock on the owning relation.
 */
static void
AlterSeqNamespaces(Relation classRel, Relation rel,
				   Oid oldNspOid, Oid newNspOid, const char *newNspName)
{
	cqContext  *pcqCtx;
	HeapTuple	tup;

	/*
	 * SERIAL sequences are those having an auto dependency on one of the
	 * table's columns (we don't care *which* column, exactly).
	 */

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_depend "
				" WHERE refclassid = :1 "
				" AND refobjid = :2 ",
				ObjectIdGetDatum(RelationRelationId),
				ObjectIdGetDatum(RelationGetRelid(rel))));

	while (HeapTupleIsValid(tup = caql_getnext(pcqCtx)))
	{
		Form_pg_depend depForm = (Form_pg_depend) GETSTRUCT(tup);
		Relation	seqRel;

		/* skip dependencies other than auto dependencies on columns */
		if (depForm->refobjsubid == 0 ||
			depForm->classid != RelationRelationId ||
			depForm->objsubid != 0 ||
			depForm->deptype != DEPENDENCY_AUTO)
			continue;

		/* Use relation_open just in case it's an index */
		seqRel = relation_open(depForm->objid, AccessExclusiveLock);

		/* skip non-sequence relations */
		if (RelationGetForm(seqRel)->relkind != RELKIND_SEQUENCE)
		{
			/* No need to keep the lock */
			relation_close(seqRel, AccessExclusiveLock);
			continue;
		}

		/* Fix the pg_class and pg_depend entries */
		AlterRelationNamespaceInternal(classRel, depForm->objid,
									   oldNspOid, newNspOid,
									   true);

		/*
		 * Sequences have entries in pg_type. We need to be careful to move
		 * them to the new namespace, too.
		 */
		AlterTypeNamespaceInternal(RelationGetForm(seqRel)->reltype,
								   newNspOid, false);

		/* Now we can close it.  Keep the lock till end of transaction. */
		relation_close(seqRel, NoLock);
	}

	caql_endscan(pcqCtx);

}


/*
 * This code supports
 *	CREATE TEMP TABLE ... ON COMMIT { DROP | PRESERVE ROWS | DELETE ROWS }
 *
 * Because we only support this for TEMP tables, it's sufficient to remember
 * the state in a backend-local data structure.
 */

/*
 * Register a newly-created relation's ON COMMIT action.
 */
void
register_on_commit_action(Oid relid, OnCommitAction action)
{
	OnCommitItem *oc;
	MemoryContext oldcxt;

	/*
	 * We needn't bother registering the relation unless there is an ON COMMIT
	 * action we need to take.
	 */
	if (action == ONCOMMIT_NOOP || action == ONCOMMIT_PRESERVE_ROWS)
		return;

	oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

	oc = (OnCommitItem *) palloc(sizeof(OnCommitItem));
	oc->relid = relid;
	oc->oncommit = action;
	oc->creating_subid = GetCurrentSubTransactionId();
	oc->deleting_subid = InvalidSubTransactionId;

	on_commits = lcons(oc, on_commits);

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Unregister any ON COMMIT action when a relation is deleted.
 *
 * Actually, we only mark the OnCommitItem entry as to be deleted after commit.
 */
void
remove_on_commit_action(Oid relid)
{
	ListCell   *l;

	foreach(l, on_commits)
	{
		OnCommitItem *oc = (OnCommitItem *) lfirst(l);

		if (oc->relid == relid)
		{
			oc->deleting_subid = GetCurrentSubTransactionId();
			break;
		}
	}
}

/*
 * Perform ON COMMIT actions.
 *
 * This is invoked just before actually committing, since it's possible
 * to encounter errors.
 */
void
PreCommit_on_commit_actions(void)
{
	ListCell   *l;
	List	   *oids_to_truncate = NIL;

	foreach(l, on_commits)
	{
		OnCommitItem *oc = (OnCommitItem *) lfirst(l);

		/* Ignore entry if already dropped in this xact */
		if (oc->deleting_subid != InvalidSubTransactionId)
			continue;

		switch (oc->oncommit)
		{
			case ONCOMMIT_NOOP:
			case ONCOMMIT_PRESERVE_ROWS:
				/* Do nothing (there shouldn't be such entries, actually) */
				break;
			case ONCOMMIT_DELETE_ROWS:
				oids_to_truncate = lappend_oid(oids_to_truncate, oc->relid);
				break;
			case ONCOMMIT_DROP:
				{
					ObjectAddress object;

					object.classId = RelationRelationId;
					object.objectId = oc->relid;
					object.objectSubId = 0;
					performDeletion(&object, DROP_CASCADE);

					/*
					 * Note that table deletion will call
					 * remove_on_commit_action, so the entry should get marked
					 * as deleted.
					 */
					Assert(oc->deleting_subid != InvalidSubTransactionId);
					break;
				}
		}
	}
	if (oids_to_truncate != NIL)
	{
		heap_truncate(oids_to_truncate);
		CommandCounterIncrement();		/* XXX needed? */
	}
}

/*
 * Post-commit or post-abort cleanup for ON COMMIT management.
 *
 * All we do here is remove no-longer-needed OnCommitItem entries.
 *
 * During commit, remove entries that were deleted during this transaction;
 * during abort, remove those created during this transaction.
 */
void
AtEOXact_on_commit_actions(bool isCommit)
{
	ListCell   *cur_item;
	ListCell   *prev_item;

	prev_item = NULL;
	cur_item = list_head(on_commits);

	while (cur_item != NULL)
	{
		OnCommitItem *oc = (OnCommitItem *) lfirst(cur_item);

		if (isCommit ? oc->deleting_subid != InvalidSubTransactionId :
			oc->creating_subid != InvalidSubTransactionId)
		{
			/* cur_item must be removed */
			on_commits = list_delete_cell(on_commits, cur_item, prev_item);
			pfree(oc);
			if (prev_item)
				cur_item = lnext(prev_item);
			else
				cur_item = list_head(on_commits);
		}
		else
		{
			/* cur_item must be preserved */
			oc->creating_subid = InvalidSubTransactionId;
			oc->deleting_subid = InvalidSubTransactionId;
			prev_item = cur_item;
			cur_item = lnext(prev_item);
		}
	}
}

/*
 * Post-subcommit or post-subabort cleanup for ON COMMIT management.
 *
 * During subabort, we can immediately remove entries created during this
 * subtransaction.	During subcommit, just relabel entries marked during
 * this subtransaction as being the parent's responsibility.
 */
void
AtEOSubXact_on_commit_actions(bool isCommit, SubTransactionId mySubid,
							  SubTransactionId parentSubid)
{
	ListCell   *cur_item;
	ListCell   *prev_item;

	prev_item = NULL;
	cur_item = list_head(on_commits);

	while (cur_item != NULL)
	{
		OnCommitItem *oc = (OnCommitItem *) lfirst(cur_item);

		if (!isCommit && oc->creating_subid == mySubid)
		{
			/* cur_item must be removed */
			on_commits = list_delete_cell(on_commits, cur_item, prev_item);
			pfree(oc);
			if (prev_item)
				cur_item = lnext(prev_item);
			else
				cur_item = list_head(on_commits);
		}
		else
		{
			/* cur_item must be preserved */
			if (oc->creating_subid == mySubid)
				oc->creating_subid = parentSubid;
			if (oc->deleting_subid == mySubid)
				oc->deleting_subid = isCommit ? parentSubid : InvalidSubTransactionId;
			prev_item = cur_item;
			cur_item = lnext(prev_item);
		}
	}
}


/*
 * Transform the URI string list into a text array (the form that is
 * used in the catalog table pg_exttable). While at it we validate
 * the URI strings.
 *
 * The result is a text array but we declare it as Datum to avoid
 * including array.h in analyze.h.
 */
static Datum transformLocationUris(List *locs, List* fmtopts, bool isweb, bool iswritable)
{
	ListCell   *cell;
	ArrayBuildState *astate;
	Datum		result;
	UriProtocol first_protocol = URI_FILE; /* initialize to keep gcc quiet */
	bool		first_uri = true;
	
#define FDIST_DEF_PORT 8080

	/* Parser should not let this happen */
	Assert(locs != NIL);

	/* We build new array using accumArrayResult */
	astate = NULL;

	/*
	 * first, check for duplicate URI entries
	 */
	foreach(cell, locs)
	{
		Value		*v1 = lfirst(cell);
		const char	*uri1 = v1->val.str;
		ListCell   *rest;

		for_each_cell(rest, lnext(cell))
		{
			Value		*v2 = lfirst(rest);
			const char	*uri2 = v2->val.str;

			if (strcmp(uri1, uri2) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("location uri \"%s\" appears more than once",
								uri1),
										   errOmitLocation(true)));
		}
	}

	/*
	 * iterate through the user supplied URI list from LOCATION clause.
	 */
	foreach(cell, locs)
	{
		Uri			*uri;
		text		*t;
		char		*uri_str_orig;
		char		*uri_str_final;
		Size		len;
		Value		*v = lfirst(cell);
		
		/* get the current URI string from the command */
		uri_str_orig = pstrdup(v->val.str);

		/* parse it to its components */
		uri = ParseExternalTableUri(uri_str_orig);

		/* allocate memory for a modified URI string (if needs modification) */
		uri_str_final = (char *) palloc(strlen(uri_str_orig) *
						  sizeof(char) +
						  1 + 4 + 1 /* default port if added */);

		/*
		 * in here edit the uri string if needed
		 */

		/* in HAWQ 2.0, the file protocol is not supported any more. */
		if (uri->protocol == URI_FILE)
		{
	    ereport(ERROR,
	        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
	        errmsg("the file protocol for external tables is deprecated"),
	        errhint("use the gpfdist protocol or COPY FROM instead"),
	        errOmitLocation(true)));
		}

		/* no port was specified for gpfdist, gpfdists or hdfs. add the default */
		if ((uri->protocol == URI_GPFDIST || uri->protocol == URI_GPFDISTS) && uri->port == -1)
		{
			char *at_hostname = (char *) uri_str_orig
					+ strlen(uri->protocol == URI_GPFDIST ? "gpfdist://" : "gpfdists://");
			char *after_hostname = strchr(at_hostname, '/');
			int  len = after_hostname - at_hostname;
			char *hostname = pstrdup(at_hostname);

			hostname[len] = '\0';

			/* add the default port number to the uri string */
			sprintf(uri_str_final, "%s%s:%d%s",
					(uri->protocol == URI_GPFDIST ? PROTOCOL_GPFDIST : PROTOCOL_GPFDISTS),
					hostname,
					FDIST_DEF_PORT, after_hostname);

			pfree(hostname);
		}
		else
		{
			/* no changes to original uri string */
			uri_str_final = (char *) uri_str_orig;
		}

		/* 
		 * If a custom protocol is used, validate its existence.
		 * If it exists, and a custom protocol url validator exists
		 * as well, invoke it now.
		 */
		if (first_uri && uri->protocol == URI_CUSTOM)
		{
			Oid		procOid = InvalidOid;
			
			procOid = LookupExtProtocolFunction(uri->customprotocol, 
												EXTPTC_FUNC_VALIDATOR, 
												false);

			if (OidIsValid(procOid) && Gp_role == GP_ROLE_DISPATCH)
				InvokeProtocolValidation(procOid, 
										 uri->customprotocol, 
										 iswritable, 
										 locs, fmtopts);
		}
		
		if(first_uri)
		{
		    first_protocol = uri->protocol;
		    first_uri = false;
		} 
		    	

		if(uri->protocol != first_protocol)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("URI protocols must be the same for all data sources"),
					 errhint("Available protocols are 'http', 'file', 'pxf', 'gpfdist' and 'gpfdists'"),
							   errOmitLocation(true)));

		}
		
		if(uri->protocol != URI_HTTP && isweb)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("an EXTERNAL WEB TABLE may only use http URI\'s, problem in: \'%s\'", uri_str_final),
					 errhint("Use CREATE EXTERNAL TABLE instead."),
							   errOmitLocation(true)));

		if(uri->protocol == URI_HTTP && !isweb)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("http URI\'s can only be used in an external web table"),
					errhint("Use CREATE EXTERNAL WEB TABLE instead."),
							   errOmitLocation(true)));

		if(iswritable && (uri->protocol == URI_HTTP || uri->protocol == URI_FILE))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported URI protocol \'%s\' for writable external table", 
							(uri->protocol == URI_HTTP ? "http" : "file")),
					 errhint("Writable external tables may use \'gpfdist(s)\' URIs only."),
							   errOmitLocation(true)));

		if(uri->protocol != URI_CUSTOM && iswritable && strchr(uri->path, '*'))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("Unsupported use of wildcard in a writable external web table definition: "
							"\'%s\'", uri_str_final),
					errhint("Specify the explicit path and file name to write into.")));
		
		if ((uri->protocol == URI_GPFDIST || uri->protocol == URI_GPFDISTS) && iswritable && uri->path[strlen(uri->path) - 1] == '/')
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("Unsupported use of a directory name in a writable gpfdist(s) external table : "
							"\'%s\'", uri_str_final),
					errhint("Specify the explicit path and file name to write into."),
							   errOmitLocation(true)));

		len = VARHDRSZ + strlen(uri_str_final);

		/* +1 leaves room for sprintf's trailing null */
		t = (text *) palloc(len + 1);
		SET_VARSIZE(t, len);
		sprintf((char *) VARDATA(t), "%s", uri_str_final);


		astate = accumArrayResult(astate, PointerGetDatum(t),
								  false, TEXTOID,
								  CurrentMemoryContext);

		FreeExternalTableUri(uri);
		pfree(uri_str_final);
	}

	if (astate)
		result = makeArrayResult(astate, CurrentMemoryContext);
	else
		result = (Datum) 0;

	return result;

}

static Datum transformExecOnClause(List	*on_clause, int *preferred_segment_num, bool iswritable)
{
	ArrayBuildState *astate;
	Datum		result;

	ListCell   *exec_location_opt;
	char	   *exec_location_str = NULL;
	char	   *value_str = NULL;
	int			value_int;
	Size		len;
	text		*t;

	/*
	 * Extract options from the statement node tree
	 * NOTE: as of now we only support one option in the ON clause
	 * and therefore more than one is an error (check here in case
	 * the sql parser isn't strict enough).
	 */
	foreach(exec_location_opt, on_clause)
	{
		DefElem    *defel = (DefElem *) lfirst(exec_location_opt);

		/* only one element is allowed! */
		if(exec_location_str)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("ON clause must not have more than one element."),
							   errOmitLocation(true)));

		if (strcmp(defel->defname, "all") == 0)
		{
		  /* in HAWQ 2.0, we don't support ON ALL any more. */
      ereport(ERROR,
          (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
          errmsg("the ON ALL syntax for external tables is deprecated"),
          errOmitLocation(true)));

			/* result: "ALL_SEGMENTS" */
			exec_location_str = (char *) palloc(12 + 1);
			exec_location_str = "ALL_SEGMENTS";
			*preferred_segment_num = GetAllWorkerHostNum();
		}
		else if (strcmp(defel->defname, "hostname") == 0)
		{
		  /* in HAWQ 2.0, we don't support ON HOST any more. */
      ereport(ERROR,
          (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
          errmsg("the ON HOST syntax for external tables is deprecated"),
          errOmitLocation(true)));

			/* result: "HOST:<hostname>" */
			value_str = strVal(defel->arg);
			exec_location_str = (char *) palloc(5 + 1 + strlen(value_str) + 1);
			sprintf((char *) exec_location_str, "HOST:%s", value_str);
			*preferred_segment_num = GetAllWorkerHostNum();
		}
		else if (strcmp(defel->defname, "eachhost") == 0)
		{
		  /* in HAWQ 2.0, we don't support EACHHOST any more. */
      ereport(ERROR,
          (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
          errmsg("the ON EACHHOST syntax for external tables is deprecated"),
          errOmitLocation(true)));

			/* result: "PER_HOST" */
			exec_location_str = (char *) palloc(8 + 1);
			exec_location_str = "PER_HOST";
			*preferred_segment_num = GetAllWorkerHostNum();
		}
		else if (strcmp(defel->defname, "master") == 0)
		{
			if(iswritable){
			  ereport(ERROR,
	 	      (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			    errmsg("the ON master syntax for writable external tables is deprecated"),
		      errOmitLocation(true)));
			}
			/* result: "MASTER_ONLY" */
			exec_location_str = (char *) palloc(11 + 1);
			exec_location_str = "MASTER_ONLY";
		  *preferred_segment_num = -1;
		}
		else if (strcmp(defel->defname, "segment") == 0)
		{
			if(iswritable){
			  ereport(ERROR,
				  (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			    errmsg("the ON segment syntax for writable external tables is deprecated"),
	        errOmitLocation(true)));
			}
			/* result: "SEGMENT_ID:<segid>" */
			value_int = intVal(defel->arg);
			exec_location_str = (char *) palloc(10 + 1 + 8 + 1);
			sprintf((char *) exec_location_str, "SEGMENT_ID:%d", value_int);
			*preferred_segment_num = value_int + 1;
		}
		else if (strcmp(defel->defname, "random") == 0)
		{
			/* result: "TOTAL_SEGS:<number>" */
			value_int = intVal(defel->arg);
			exec_location_str = (char *) palloc(10 + 1 + 8 + 1);
			sprintf((char *) exec_location_str, "TOTAL_SEGS:%d", value_int);
			*preferred_segment_num = value_int;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERNAL_ERROR),
					 errmsg("Unknown location code for EXECUTE in tablecmds."),
							   errOmitLocation(true)));
		}
	}

	/* convert to text[] */
	astate = NULL;
	len = VARHDRSZ + strlen(exec_location_str);
	t = (text *) palloc(len + 1);
	SET_VARSIZE(t, len);
	sprintf((char *) VARDATA(t), "%s", exec_location_str);


	astate = accumArrayResult(astate, PointerGetDatum(t),
							  false, TEXTOID,
							  CurrentMemoryContext);

	if (astate)
		result = makeArrayResult(astate, CurrentMemoryContext);
	else
		result = (Datum) 0;

	return result;
}

/*
 * transform format name to format code and validate that
 * the format is supported. Currently the only supported formats
 * are "text" (type 't') ,"csv" (type 'c') and "custom" (type 'b')
 */
static char transformFormatType(char *formatname)
{
	char	result = '\0';

	if(pg_strcasecmp(formatname, "text") == 0)
		result = 't';
	else if(pg_strcasecmp(formatname, "csv") == 0)
		result = 'c';
	else if(pg_strcasecmp(formatname, "custom") == 0)
		result = 'b';
	else
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("unsupported format '%s'", formatname),
				 errhint("available formats are \"text\", \"csv\", or \"custom\""),
						   errOmitLocation(true)));

	return result;
}


/*
 * Transform the FORMAT options into a text field. Parse the
 * options and validate them for their respective format type.
 *
 * The result is a text field that includes the format string.
 */
static Datum transformFormatOpts(char formattype, List *formatOpts, int numcols, bool iswritable)
{
	ListCell   *option;
	Datum		result;
	char *format_str = NULL;
	char *delim = NULL;
	char *null_print = NULL;
	char *quote = NULL;
	char *escape = NULL;
	char *eol_str = NULL;
	char *formatter = NULL;
	bool header_line = false;
	bool fill_missing = false;
	List *force_notnull = NIL;
	List *force_quote = NIL;
	Size len;
	StringInfoData fnn, fq, nl;

	Assert(fmttype_is_custom(formattype) || 
		   fmttype_is_text(formattype) ||
		   fmttype_is_csv(formattype));
	
	/* Extract options from the statement node tree */
	if (fmttype_is_text(formattype) || fmttype_is_csv(formattype))
	{
		foreach(option, formatOpts)
		{
			DefElem    *defel = (DefElem *) lfirst(option);

			if (strcmp(defel->defname, "delimiter") == 0)
			{
				if (delim)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("conflicting or redundant options"),
									   errOmitLocation(true)));
				delim = strVal(defel->arg);
			}
			else if (strcmp(defel->defname, "null") == 0)
			{
				if (null_print)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("conflicting or redundant options"),
									   errOmitLocation(true)));
				null_print = strVal(defel->arg);
			}
			else if (strcmp(defel->defname, "header") == 0)
			{
				if (header_line)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("conflicting or redundant options"),
									   errOmitLocation(true)));
				header_line = intVal(defel->arg);
			}
			else if (strcmp(defel->defname, "quote") == 0)
			{
				if (quote)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("conflicting or redundant options"),
									   errOmitLocation(true)));
				quote = strVal(defel->arg);
			}
			else if (strcmp(defel->defname, "escape") == 0)
			{
				if (escape)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("conflicting or redundant options"),
									   errOmitLocation(true)));
				escape = strVal(defel->arg);
			}
			else if (strcmp(defel->defname, "force_notnull") == 0)
			{
				if (force_notnull)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("conflicting or redundant options"),
									   errOmitLocation(true)));
					
				force_notnull = (List *) defel->arg;
			}
			else if (strcmp(defel->defname, "force_quote") == 0)
			{
				if (force_quote)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("conflicting or redundant options"),
									   errOmitLocation(true)));
					
				force_quote = (List *) defel->arg;
			}
			else if (strcmp(defel->defname, "fill_missing_fields") == 0)
			{
				if (fill_missing)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("conflicting or redundant options"),
									   errOmitLocation(true)));
				fill_missing = intVal(defel->arg);
			}
			else if (strcmp(defel->defname, "newline") == 0)
			{
				if (eol_str)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("conflicting or redundant options"),
									   errOmitLocation(true)));
				eol_str = strVal(defel->arg);
			}
			else if (strcmp(defel->defname, "formatter") == 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("formatter option only valid for custom formatters"),
						 errOmitLocation(true)));
			}
			else
				elog(ERROR, "option \"%s\" not recognized",
					 defel->defname);
		}

		/*
		 * Set defaults
		 */
		if (!delim)
			delim = fmttype_is_csv(formattype) ? "," : "\t";

		if (!null_print)
			null_print = fmttype_is_csv(formattype) ? "" : "\\N";

		if (fmttype_is_csv(formattype))
		{
			if (!quote)
				quote = "\"";
			if (!escape)
				escape = quote;
		}

		if (!fmttype_is_csv(formattype) && !escape)
			escape = "\\";			/* default escape for text mode */

		/*
		 * re-construct the FORCE NOT NULL list string.
		 * TODO: is there no existing util function that does this? can't find.
		 */
		if(force_notnull)
		{
			ListCell   *l;
			bool 		is_first_col = true;

			initStringInfo(&fnn);
			appendStringInfo(&fnn, " force not null");

			foreach(l, force_notnull)
			{
				const char	   *col_name = strVal(lfirst(l));

				appendStringInfo(&fnn, (is_first_col ? " %s" : ",%s"),
								 quote_identifier(col_name));
				is_first_col = false;
			}
		}

		/*
		 * re-construct the FORCE QUOTE list string.
		 */
		if(force_quote)
		{
			ListCell   *l;
			bool 		is_first_col = true;

			initStringInfo(&fq);
			appendStringInfo(&fq, " force quote");

			foreach(l, force_quote)
			{
				const char	   *col_name = strVal(lfirst(l));

				appendStringInfo(&fq, (is_first_col ? " %s" : ",%s"),
								 quote_identifier(col_name));
				is_first_col = false;
			}
		}

		if(eol_str)
		{
			initStringInfo(&nl);
			appendStringInfo(&nl, " newline '%s'", eol_str);
		}
		
		/* verify all user supplied control char combinations are legal */
		ValidateControlChars(false,
							 !iswritable,
							 fmttype_is_csv(formattype),
							 delim,
							 null_print,
							 quote,
							 escape,
							 force_quote,
							 force_notnull,
							 header_line,
							 fill_missing,
							 eol_str,
							 numcols);
	
		/*
		 * build the format option string that will get stored in the catalog.
		 */
	
		len = 9 + 1 + 1 + strlen(delim) + 1 +		/* "delimiter 'x' of 'off'" */
			  1 +									/* space					*/
			  4 + 1 + 1 + strlen(null_print) + 1 +	/* "null 'str'"				*/
			  1 +									/* space					*/
			  6 + 1 + 1 + strlen(escape) + 1;		/* "escape 'c' or 'off' 	*/
	
		if (fmttype_is_csv(formattype))
			len +=	1 +								/* space					*/
					5 + 1 + 3;						/* "quote 'x'"				*/
	
		len += header_line ? strlen(" header") : 0;
		len += fill_missing ? strlen(" fill missing fields") : 0;
		len += force_notnull ? strlen(fnn.data) : 0;
		len += force_quote ? strlen(fq.data) : 0;
		len += (eol_str ? (1 + 7 + 1 + 1 + strlen(eol_str) + 1) : 0); /* space x 2, newline 'xx/xxxx' */
	
		/* +1 leaves room for sprintf's trailing null */
		format_str = (char *) palloc(len + 1);
	
		if(fmttype_is_text(formattype))
		{
			sprintf((char *) format_str, "delimiter '%s' null '%s' escape '%s'%s%s%s",
					delim, null_print, escape, (header_line ? " header":""),
					(fill_missing ? " fill missing fields":""), (eol_str ?  nl.data : ""));
		}
		else if (fmttype_is_csv(formattype))
		{
			
			sprintf((char *) format_str, "delimiter '%s' null '%s' escape '%s' quote '%s'%s%s%s%s%s",
					delim, null_print, escape, quote, (header_line ? " header" : ""),
					(fill_missing ? " fill missing fields":""),
					(force_notnull ? fnn.data :""), (force_quote ? fq.data :""),
					(eol_str ?  nl.data : ""));
		}
		else
		{
			/* should never happen */
			Assert(false);
		}

	}
	else
	{
		/* custom format */
		
		int 		len = 0;
		const int	maxlen = 8 * 1024 - 1;
		StringInfoData key_modified;
		
		initStringInfo(&key_modified);
		
		format_str = (char *) palloc0(maxlen + 1);
		
		foreach(option, formatOpts)
		{
			DefElem    *defel = (DefElem *) lfirst(option);
			char	   *key = defel->defname;
			bool need_free_value = false;
			char	   *val = defGetString(defel, &need_free_value);
			
			if (strcmp(key, "formatter") == 0)
			{
				if (formatter)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("conflicting or redundant options"),
									   errOmitLocation(true)));
					
				formatter = strVal(defel->arg);
			}
			
			/* MPP-14467 - replace any space chars with meta char */
			resetStringInfo(&key_modified);
			appendStringInfoString(&key_modified, key);
			replaceStringInfoString(&key_modified, " ", "<gpx20>");
			
			sprintf((char *) format_str + len, "%s '%s' ", key_modified.data, val);
			len += strlen(key_modified.data) + strlen(val) + 4;
			
			if (need_free_value)
			{
				pfree(val);
				val = NULL;
			}

			AssertImply(need_free_value, NULL == val);

			if (len > maxlen)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("format options must be less than %d bytes in size", maxlen),
								   errOmitLocation(true)));			
		}
		
		if(!formatter)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("no formatter function specified"),
							   errOmitLocation(true)));
	}
	

	/* convert c string to text datum */
	result = DirectFunctionCall1(textin, CStringGetDatum(format_str));

	/* clean up */
	if (format_str) pfree(format_str);
	if (force_notnull) pfree(fnn.data);
	if (force_quote) pfree(fq.data);
	if (eol_str) pfree(nl.data);
	
	return result;

}

/* ALTER TABLE EXCHANGE
 *
 * NB different signature from non-partitioned table Prep functions.
 */
static void
ATPrepExchange(Relation rel, AlterPartitionCmd *pc)
{
	PgPartRule   	   	*prule 	= NULL;
	Relation			 oldrel = NULL;
	Relation			 newrel = NULL;
	
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		AlterPartitionId *pid = (AlterPartitionId *) pc->partid;
		bool				is_split;
		
		is_split = ((AlterPartitionCmd *)pc->arg2)->arg2 != NULL;
		
		if (is_split)
			return;
		
		prule = get_part_rule(rel, pid, true, true,
							  CurrentMemoryContext, NULL, false);
		if (prule)
		{
			/* cannot exchange a hash partition */
			if ('h' == prule->pNode->part->parkind)
				ereport(ERROR,
						(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
						 errmsg("cannot exchange HASH partition%s "
								"of relation \"%s\"", prule->partIdStr,
								RelationGetRelationName(rel))));
		}
		oldrel = heap_open(prule->topRule->parchildrelid,
						   AccessExclusiveLock);
		newrel = heap_openrv((RangeVar *)pc->arg1,
							 AccessExclusiveLock);
	}
	else
	{
		oldrel = heap_openrv((RangeVar *)pc->partid,
							 AccessExclusiveLock);
		newrel = heap_openrv((RangeVar *)pc->arg1,
							 AccessExclusiveLock);
		
	}
	ATSimplePermissions(rel, false);
	ATSimplePermissions(oldrel, false);
	ATSimplePermissions(newrel, false);
	
	/* Check that old and new look the same, error if not. */
	is_exchangeable(rel, oldrel, newrel, true);
	
	heap_close(oldrel, NoLock);
	heap_close(newrel, NoLock);
}

/*
 * Return a palloc'd string representing an AlterTableCmd type for use
 * in a message pattern like "can't %s a partitioned table".  The default
 * return string is "alter".
 *
 * This may pose a challenge for localization.  
 */
static
char *alterTableCmdString(AlterTableType subtype)
{
	char *cmdstring = NULL;
	
	switch (subtype)
	{
		case AT_AddColumn: /* add column */
		case AT_AddColumnRecurse: /* internal to command/tablecmds.c*/
			cmdstring = pstrdup("add a column to");
			break;
			
		case AT_ColumnDefault: /* alter column default */
			cmdstring = pstrdup("alter a column default of");
			break;
			
		case AT_DropNotNull: /* alter column drop not null */
		case AT_SetNotNull: /* alter column set not null */
			cmdstring = pstrdup("alter a column null setting of");
			break;
			
		case AT_SetStatistics: /* alter column statistics */
		case AT_SetStorage: /* alter column storage */
			break;
			
		case AT_DropColumn: /* drop column */
		case AT_DropColumnRecurse: /* internal to commands/tablecmds.c */
			cmdstring = pstrdup("drop a column from");
			break;
			
		case AT_AddIndex: /* add index */
		case AT_ReAddIndex: /* internal to commands/tablecmds.c */
			break;
			
		case AT_AddConstraint: /* add constraint */
		case AT_AddConstraintRecurse: /* internal to commands/tablecmds.c */
			cmdstring = pstrdup("add a constraint to");
			break;
			
		case AT_ProcessedConstraint: /* pre-processed add constraint (local in parser/analyze.c) */
			break;
			
		case AT_DropConstraint: /* drop constraint */
			cmdstring = pstrdup("drop a constraint from");
			break;
			
		case AT_DropConstraintQuietly: /* drop constraint, no error/warning (local in commands/tablecmds.c) */
			break;
			
		case AT_AlterColumnType: /* alter column type */
			cmdstring = pstrdup("alter a column datatype of");
			break;
			
		case AT_ChangeOwner: /* change owner */
			cmdstring = pstrdup("alter the owner of");
			break;
			
		case AT_ClusterOn: /* CLUSTER ON */
		case AT_DropCluster: /* SET WITHOUT CLUSTER */
			break;
			
		case AT_DropOids: /* SET WITHOUT OIDS */
			cmdstring = pstrdup("alter the oid setting of");
			break;
			
		case AT_SetTableSpace: /* SET TABLESPACE */
		case AT_SetRelOptions: /* SET (...) -- AM specific parameters */
		case AT_ResetRelOptions: /* RESET (...) -- AM specific parameters */
			break;
			
		case AT_EnableTrig: /* ENABLE TRIGGER name */
		case AT_DisableTrig: /* DISABLE TRIGGER name */
		case AT_EnableTrigAll: /* ENABLE TRIGGER ALL */
		case AT_DisableTrigAll: /* DISABLE TRIGGER ALL */
		case AT_EnableTrigUser: /* ENABLE TRIGGER USER */
		case AT_DisableTrigUser: /* DISABLE TRIGGER USER */
			cmdstring = pstrdup("enable or disable triggers on");
			break;
			
		case AT_AddInherit: /* INHERIT parent */
		case AT_DropInherit: /* NO INHERIT parent */
			cmdstring = pstrdup("alter inheritance on");
			break;
			
		case AT_SetDistributedBy: /* SET DISTRIBUTED BY */
			break;
			
		case AT_PartAdd: /* Add */
		case AT_PartAlter: /* Alter */
		case AT_PartCoalesce: /* Coalesce */
		case AT_PartDrop: /* Drop */
			break;
			
		case AT_PartExchange: /* Exchange */
			cmdstring = pstrdup("exchange a part into");
			break;
			
		case AT_PartMerge: /* Merge */
			cmdstring = pstrdup("merge parts of");
			break;
			
		case AT_PartModify: /* Modify */
		case AT_PartRename: /* Rename */
		case AT_PartSetTemplate: /* Set Subpartition Template */
			break;
			
		case AT_PartSplit: /* Split */
			cmdstring = pstrdup("split parts of");
			break;
			
		case AT_PartTruncate: /* Truncate */
		case AT_PartAddInternal: /* CREATE TABLE time partition addition */
			break;
	}
	
	if ( cmdstring == NULL )
	{
		cmdstring = pstrdup("alter");
	}
	
	return cmdstring;
}

static void 
InvokeProtocolValidation(Oid procOid, char *procName, bool iswritable, List *locs, List* fmtopts)
{
	
	ExtProtocolValidatorData   *validator_data;
	FmgrInfo				   *validator_udf;
	FunctionCallInfoData		fcinfo;
	
	validator_data = (ExtProtocolValidatorData *) palloc0 (sizeof(ExtProtocolValidatorData));
	validator_udf = palloc(sizeof(FmgrInfo));
	fmgr_info(procOid, validator_udf);
	
	validator_data->type 		= T_ExtProtocolValidatorData;
	validator_data->url_list 	= locs;
	validator_data->format_opts = fmtopts;
	validator_data->errmsg		= NULL;
	validator_data->direction 	= (iswritable ? EXT_VALIDATE_WRITE :
											    EXT_VALIDATE_READ);
	
	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo, 
							 /* FmgrInfo */ validator_udf, 
							 /* nArgs */ 0, 
							 /* Call Context */ (Node *) validator_data, 
							 /* ResultSetInfo */ NULL);
	
	/* invoke validator. if this function returns - validation passed */
	FunctionCallInvoke(&fcinfo);

	/* We do not expect a null result */
	if (fcinfo.isnull)
		elog(ERROR, "validator function %u returned NULL", 
					fcinfo.flinfo->fn_oid);

	pfree(validator_data);
	pfree(validator_udf);
}
