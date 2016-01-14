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
 * heap.c
 *	  code to create and destroy POSTGRES heap relations
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/catalog/heap.c,v 1.314 2006/11/05 22:42:08 tgl Exp $
 *
 *
 * INTERFACE ROUTINES
 *		heap_create()			- Create an uncataloged heap relation
 *		heap_create_with_catalog() - Create a cataloged relation
 *		heap_drop_with_catalog() - Removes named relation from catalogs
 *
 * NOTES
 *	  this code taken from access/heap/create.c, which contains
 *	  the old heap_create_with_catalog, amcreate, and amdestroy.
 *	  those routines will soon call these routines using the function
 *	  manager,
 *	  just like the poorly named "NewXXX" routines do.	The
 *	  "New" routines are all going to die soon, once and for all!
 *		-cim 1/13/91
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/catquery.h"
#include "catalog/dependency.h"
#include "catalog/gp_policy.h"
#include "catalog/gp_persistent.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_appendonly.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_rule.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbpartition.h"
#include "cdb/cdbanalyze.h"
#include "cdb/cdbparquetfooterprocessor.h"
#include "commands/dbcommands.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"             /* CDB: GetMemoryChunkContext */
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/guc.h"
#include "cdb/cdbvars.h"

#include "cdb/cdbsharedstorageop.h"
#include "cdb/cdbmirroredfilesysobj.h"
#include "cdb/cdbpersistentfilesysobj.h"
#include "cdb/cdbparquetstoragewrite.h"
#include "catalog/gp_persistent.h"

#include "utils/guc.h"

typedef struct pg_result PGresult;
extern void PQclear(PGresult *res);

static void MetaTrackAddUpdInternal(cqContext  *pcqCtx,
									Oid			classid,
									Oid			objoid, 
									Oid			relowner,
									char*		actionname,
									char*		subtype,
									Relation	rel,
									HeapTuple	old_tuple);



static void AddNewRelationTuple(Relation pg_class_desc,
					Relation new_rel_desc,
					Oid new_rel_oid, Oid new_type_oid,
					Oid relowner,
					char relkind,
					char relstorage,
					Datum reloptions);
static Oid AddNewRelationType(const char *typeName,
				   Oid typeNamespace,
				   Oid new_rel_oid,
				   char new_rel_kind,
				   Oid ownerid);
static void RelationRemoveInheritance(Oid relid);
static Oid StoreRelCheck(Relation rel, char *ccname, char *ccbin, Oid conoid);
static Node* cookConstraint (ParseState *pstate,
							 Node 		*raw_constraint,
							 char		*relname);
static void StoreConstraints(Relation rel, TupleDesc tupdesc);
static List *insert_ordered_unique_oid(List *list, Oid datum);

/* ----------------------------------------------------------------
 *				XXX UGLY HARD CODED BADNESS FOLLOWS XXX
 *
 *		these should all be moved to someplace in the lib/catalog
 *		module, if not obliterated first.
 * ----------------------------------------------------------------
 */


/*
 * Note:
 *		Should the system special case these attributes in the future?
 *		Advantage:	consume much less space in the ATTRIBUTE relation.
 *		Disadvantage:  special cases will be all over the place.
 */

static FormData_pg_attribute a1 = {
	0, {"ctid"}, TIDOID, 0, sizeof(ItemPointerData),
	SelfItemPointerAttributeNumber, 0, -1, -1,
	false, 'p', 's', true, false, false, true, 0
};

static FormData_pg_attribute a2 = {
	0, {"oid"}, OIDOID, 0, sizeof(Oid),
	ObjectIdAttributeNumber, 0, -1, -1,
	true, 'p', 'i', true, false, false, true, 0
};

static FormData_pg_attribute a3 = {
	0, {"xmin"}, XIDOID, 0, sizeof(TransactionId),
	MinTransactionIdAttributeNumber, 0, -1, -1,
	true, 'p', 'i', true, false, false, true, 0
};

static FormData_pg_attribute a4 = {
	0, {"cmin"}, CIDOID, 0, sizeof(CommandId),
	MinCommandIdAttributeNumber, 0, -1, -1,
	true, 'p', 'i', true, false, false, true, 0
};

static FormData_pg_attribute a5 = {
	0, {"xmax"}, XIDOID, 0, sizeof(TransactionId),
	MaxTransactionIdAttributeNumber, 0, -1, -1,
	true, 'p', 'i', true, false, false, true, 0
};

static FormData_pg_attribute a6 = {
	0, {"cmax"}, CIDOID, 0, sizeof(CommandId),
	MaxCommandIdAttributeNumber, 0, -1, -1,
	true, 'p', 'i', true, false, false, true, 0
};

/*
 * We decided to call this attribute "tableoid" rather than say
 * "classoid" on the basis that in the future there may be more than one
 * table of a particular class/type. In any case table is still the word
 * used in SQL.
 */
static FormData_pg_attribute a7 = {
	0, {"tableoid"}, OIDOID, 0, sizeof(Oid),
	TableOidAttributeNumber, 0, -1, -1,
	true, 'p', 'i', true, false, false, true, 0
};

/*CDB*/
static FormData_pg_attribute a8 = {
	0, {"gp_segment_id"}, INT4OID, 0, sizeof(gpsegmentId),
	GpSegmentIdAttributeNumber, 0, -1, -1,
	true, 'p', 'i', true, false, false, true, 0
};

static const Form_pg_attribute SysAtt[] = {&a1, &a2, &a3, &a4, &a5, &a6, &a7, &a8};

/*
 * This function returns a Form_pg_attribute pointer for a system attribute.
 * Note that we elog if the presented attno is invalid, which would only
 * happen if there's a problem upstream.
 */
Form_pg_attribute
SystemAttributeDefinition(AttrNumber attno, bool relhasoids)
{
	if (attno >= 0 || attno < -(int) lengthof(SysAtt))
		elog(ERROR, "invalid system attribute number %d", attno);
	if (attno == ObjectIdAttributeNumber && !relhasoids)
		elog(ERROR, "invalid system attribute number %d", attno);
	return SysAtt[-attno - 1];
}

/*
 * If the given name is a system attribute name, return a Form_pg_attribute
 * pointer for a prototype definition.	If not, return NULL.
 */
Form_pg_attribute
SystemAttributeByName(const char *attname, bool relhasoids)
{
	int			j;

	for (j = 0; j < (int) lengthof(SysAtt); j++)
	{
		Form_pg_attribute att = SysAtt[j];

		if (relhasoids || att->attnum != ObjectIdAttributeNumber)
		{
			if (strcmp(NameStr(att->attname), attname) == 0)
				return att;
		}
	}

	return NULL;
}


/* ----------------------------------------------------------------
 *				XXX END OF UGLY HARD CODED BADNESS XXX
 * ---------------------------------------------------------------- */


/* ----------------------------------------------------------------
 *		heap_create		- Create an uncataloged heap relation
 *
 *		Note API change: the caller must now always provide the OID
 *		to use for the relation.
 *
 *		rel->rd_rel is initialized by RelationBuildLocalRelation,
 *		and is mostly zeroes at return.
 * ----------------------------------------------------------------
 */
Relation
heap_create(const char *relname,
			Oid relnamespace,
			Oid reltablespace,
			Oid relid,
			TupleDesc tupDesc,
			Oid relam,
			char relkind,
			char relstorage,
			bool shared_relation,
			bool allow_system_table_mods,
			bool bufferPoolBulkLoad)
{
	bool		create_storage;
	Relation	rel;
	bool		isAppendOnly = (relstorage == RELSTORAGE_AOROWS || relstorage == RELSTORAGE_PARQUET);
	bool		skipCreatingSharedTable = false;

	/* The caller must have provided an OID for the relation. */
	Assert(OidIsValid(relid));

	/*
	 * sanity checks
	 */
	if (!allow_system_table_mods &&
		(IsSystemNamespace(relnamespace) || IsToastNamespace(relnamespace) ||
		 IsAoSegmentNamespace(relnamespace)) && IsNormalProcessingMode())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to create \"%s.%s\"",
						get_namespace_name(relnamespace), relname),
		errdetail("System catalog modifications are currently disallowed."),
				   errOmitLocation(true)));

	/*
	 * Decide if we need storage or not, and handle a couple other special
	 * cases for particular relkinds.
	 */
	switch (relkind)
	{
		case RELKIND_VIEW:
		case RELKIND_COMPOSITE_TYPE:
			create_storage = false;

			/*
			 * Force reltablespace to zero if the relation has no physical
			 * storage.  This is mainly just for cleanliness' sake.
			 */
			reltablespace = InvalidOid;
			break;
		case RELKIND_SEQUENCE:
			create_storage = true;

			/*
			 * Force reltablespace to zero for sequences, since we don't
			 * support moving them around into different tablespaces.
			 */
			reltablespace = InvalidOid;
			break;
		default:
			if(relstorage_is_external(relstorage))
				create_storage = false;
			else
				create_storage = true;
			break;
	}

	/*
	 * Never allow a pg_class entry to explicitly specify the database's
	 * default tablespace in reltablespace; force it to zero instead. This
	 * ensures that if the database is cloned with a different default
	 * tablespace, the pg_class entry will still match where CREATE DATABASE
	 * will put the physically copied relation.
	 *
	 * Yes, this is a bit of a hack.
	 */
	if (reltablespace == MyDatabaseTableSpace || reltablespace == get_database_dts(MyDatabaseId))
		reltablespace = InvalidOid;

	/*
	 * build the relcache entry.
	 */
	rel = RelationBuildLocalRelation(relname,
									 relnamespace,
									 tupDesc,
									 relid,
									 reltablespace,
                                     relkind,           /*CDB*/
                                     relstorage,
									 shared_relation);

	if (!create_storage)
		return rel;

	/*
	 * We save the persistent TID and serial number in pg_class so we
	 * can supply them to the Storage Manager if the object is subsequently
	 * dropped.
	 *
	 * For shared table (that we created during upgrade), we create it once in every
	 * database, but they will all point to the same file. So, the file might have already
	 * been created.
	 * So, if we're in upgrade and we're creating shared table, we scan for existences.
	 * (We don't need to scan when we're creating shared table during initdb.)
	 * If it exists, we reuse the Tid and SerialNum and mark the isPresent to true.
	 *
	 * Note that we have not tried creating shared AO table.
	 *
	 * For non-shared table, we should always need to create a file.
	 */
	// WARNING: Do not use the rel structure -- it doesn't have relstorage set...

	if (shared_relation && gp_upgrade_mode)
	{
        if(GpPersistent_IsPersistentRelation(rel->rd_node.relNode))
        {
            // persistent systable is created with gp_before_persistence_work set to true,
            // which means all persistent systables aren't initiated at this time, so we
            // can't all API related to persistent table, here directly check file exists on disk
            char	   *rpath;
            int			fd;

            rpath = relpath(rel->rd_node);
            fd = PathNameOpenFile(rpath, O_RDONLY | PG_BINARY, 0);

            if (fd >= 0)
            {
                FileClose(fd);
                skipCreatingSharedTable = true;
            }
            pfree(rpath);
        }
        else
        {
            skipCreatingSharedTable = PersistentFileSysObj_ScanForRelation(
                    &rel->rd_node,
                    /* segmentFileNum */ 0,
                    &rel->rd_relationnodeinfo.persistentTid,
                    &rel->rd_relationnodeinfo.persistentSerialNum);
        }
        if (Debug_persistent_print && skipCreatingSharedTable)
            elog(Persistent_DebugPrintLevel(),
                    "heap_create: file for shared relation '%s' already exists", relname);
	}

	if (skipCreatingSharedTable)
		return rel;

	if (!isAppendOnly)
	{
		PersistentFileSysRelStorageMgr	localRelStorageMgr;
		PersistentFileSysRelBufpoolKind	relBufpoolKind;

		GpPersistentRelfileNode_GetRelfileInfo(
											relkind,
											relstorage,
											relam,
											&localRelStorageMgr,
											&relBufpoolKind);
		Assert(localRelStorageMgr == PersistentFileSysRelStorageMgr_BufferPool);

		Assert(rel->rd_smgr == NULL);
		RelationOpenSmgr(rel);

		MirroredFileSysObj_TransactionCreateBufferPoolFile(
											rel->rd_smgr,
											relBufpoolKind,
											rel->rd_isLocalBuf,
											rel->rd_rel->relname.data,
											/* doJustInTimeDirCreate */true, bufferPoolBulkLoad,
											&rel->rd_relationnodeinfo.persistentTid,
											&rel->rd_relationnodeinfo.persistentSerialNum);

		rel->rd_relationnodeinfo.isPresent = true;
	}
	else
	{
		Assert(Gp_role == GP_ROLE_DISPATCH);

		TransactionCreateDatabaseDir(rel->rd_node.dbNode, rel->rd_node.spcNode);

		MirroredFileSysObj_TransactionCreateRelationDir(
											&rel->rd_node,
											false,
											&rel->rd_relationnodeinfo.persistentTid,
											&rel->rd_relationnodeinfo.persistentSerialNum);
		rel->rd_relationnodeinfo.isPresent = true;

#if 0
		SharedStorageOpTasks *tasks = CreateSharedStorageOpTasks();

		int i;
		for (i = 0 ; i < rel->rd_segfile0_count; ++i)
		{
			int32	id = i - 1;
			SharedStorageOpPreAddTask(&rel->rd_node, 0, id, relname,
							&rel->rd_segfile0_relationnodeinfos[i].persistentTid,
							&rel->rd_segfile0_relationnodeinfos[i].persistentSerialNum);

			rel->rd_segfile0_relationnodeinfos[i].isPresent = TRUE;

			SharedStorageOpAddTask(relname, &rel->rd_node, 0, id,
					&rel->rd_segfile0_relationnodeinfos[i].persistentTid,
					rel->rd_segfile0_relationnodeinfos[i].persistentSerialNum,
					tasks);
		}

		PerformSharedStorageOpTasks(tasks, Op_CreateSegFile);
		PostPerformSharedStorageOpTasks(tasks);
		DropSharedStorageOpTasks(tasks);


		int i;
		Relation gp_relation_node;

		//rel->rd_segfile0_count;
		for (i = 0 ; i < 1; ++i)
		{
			int	error;

			smgrcreatepending(&rel->rd_node,
						0,
						PersistentFileSysRelStorageMgr_AppendOnly,
						PersistentFileSysRelBufpoolKind_None,
						(char *) relname,
						&rel->rd_segfile0_relationnodeinfo.persistentTid,
						&rel->rd_segfile0_relationnodeinfo.persistentSerialNum,
						/*isLocalBuf*/ false,
						/*bufferPoolBulkLoad*/ false,
						/*flushToXLog*/ true);

			rel->rd_segfile0_relationnodeinfo.isPresent = true;

			MirroredAppendOnly_Create(&rel->rd_node, 0,
					(char *) relname, &error);
			
			if (error != 0)
			{
				ereport(ERROR,
					(errcode_for_file_access(),
						errmsg("could not create relation file '%s', relation name '%s', contentid: %d: %s",
							relpath(rel->rd_node),
							relname, i - 1,
							strerror(error))));
			}

			gp_relation_node = heap_open(GpRelfileNodeRelationId, RowExclusiveLock);
			
			InsertGpRelationNodeTuple(
							gp_relation_node,
							/* relationId */ 0,
							(char *) relname,
							rel->rd_node.relNode,
							0,
							/* updateIndex */ true,
							&rel->rd_segfile0_relationnodeinfo.persistentTid,
							rel->rd_segfile0_relationnodeinfo.persistentSerialNum);
			heap_close(gp_relation_node, RowExclusiveLock);
		}
#endif	
	}

	if (Debug_check_for_invalid_persistent_tid &&
		!Persistent_BeforePersistenceWork() &&
		PersistentStore_IsZeroTid(&rel->rd_relationnodeinfo.persistentTid))
	{	
		elog(ERROR, 
			 "setNewRelfilenodeCommon has invalid TID (0,0) into relation %u/%u/%u '%s', serial number " INT64_FORMAT,
			 rel->rd_node.spcNode,
			 rel->rd_node.dbNode,
			 rel->rd_node.relNode,
			 NameStr(rel->rd_rel->relname),
			 rel->rd_relationnodeinfo.persistentSerialNum);
	}

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
		     "heap_create: '%s', Append-Only '%s', persistent TID %s and serial number " INT64_FORMAT " for CREATE",
			 relpath(rel->rd_node),
			 (isAppendOnly ? "true" : "false"),
			 ItemPointerToString(&rel->rd_relationnodeinfo.persistentTid),
			 rel->rd_relationnodeinfo.persistentSerialNum);

	return rel;
}

/* ----------------------------------------------------------------
 *		heap_create_with_catalog		- Create a cataloged relation
 *
 *		this is done in multiple steps:
 *
 *		1) CheckAttributeNamesTypes() is used to make certain the tuple
 *		   descriptor contains a valid set of attribute names and types
 *
 *		2) pg_class is opened and get_relname_relid()
 *		   performs a scan to ensure that no relation with the
 *		   same name already exists.
 *
 *		3) heap_create() is called to create the new relation on disk.
 *
 *		4) TypeCreate() is called to define a new type corresponding
 *		   to the new relation.
 *
 *		5) AddNewRelationTuple() is called to register the
 *		   relation in pg_class.
 *
 *		6) AddNewAttributeTuples() is called to register the
 *		   new relation's schema in pg_attribute.
 *
 *		7) StoreConstraints is called ()		- vadim 08/22/97
 *
 *		8) the relations are closed and the new relation's oid
 *		   is returned.
 *
 * ----------------------------------------------------------------
 */

/* --------------------------------
 *		CheckAttributeNamesTypes
 *
 *		this is used to make certain the tuple descriptor contains a
 *		valid set of attribute names and datatypes.  a problem simply
 *		generates ereport(ERROR) which aborts the current transaction.
 * --------------------------------
 */
void
CheckAttributeNamesTypes(TupleDesc tupdesc, char relkind)
{
	int			i;
	int			j;
	int			natts = tupdesc->natts;

	/* Sanity check on column count */
	if (natts < 0 || natts > MaxHeapAttributeNumber)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("tables can have at most %d columns",
						MaxHeapAttributeNumber)));

	/*
	 * first check for collision with system attribute names
	 *
	 * Skip this for a view or type relation, since those don't have system
	 * attributes.
	 */
	if (relkind != RELKIND_VIEW && relkind != RELKIND_COMPOSITE_TYPE)
	{
		for (i = 0; i < natts; i++)
		{
			if (SystemAttributeByName(NameStr(tupdesc->attrs[i]->attname),
									  tupdesc->tdhasoid) != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column name \"%s\" conflicts with a system column name",
								NameStr(tupdesc->attrs[i]->attname))));
		}
	}

	/*
	 * next check for repeated attribute names
	 */
	for (i = 1; i < natts; i++)
	{
		for (j = 0; j < i; j++)
		{
			if (strcmp(NameStr(tupdesc->attrs[j]->attname),
					   NameStr(tupdesc->attrs[i]->attname)) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column name \"%s\" is duplicated",
								NameStr(tupdesc->attrs[j]->attname)),
										   errOmitLocation(true)));
		}
	}

	/*
	 * next check the attribute types
	 */
	for (i = 0; i < natts; i++)
	{
		CheckAttributeType(NameStr(tupdesc->attrs[i]->attname),
						   tupdesc->attrs[i]->atttypid);
	}
}


/* --------------------------------
 *		CheckAttributeType
 *
 *		Verify that the proposed datatype of an attribute is legal.
 *		This is needed because there are types (and pseudo-types)
 *		in the catalogs that we do not support as elements of real tuples.
 * --------------------------------
 */
void
CheckAttributeType(const char *attname, Oid atttypid)
{
	char		att_typtype = get_typtype(atttypid);

	/*
	 * Warn user, but don't fail, if column to be created has UNKNOWN type
	 * (usually as a result of a 'retrieve into' - jolly)
	 *
	 * Refuse any attempt to create a pseudo-type column.
	 */
	if (Gp_role != GP_ROLE_EXECUTE)
	{
		if (atttypid == UNKNOWNOID)
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("column \"%s\" has type \"unknown\"", attname),
					 errdetail("Proceeding with relation creation anyway."),
							   errOmitLocation(true)));
		else if (att_typtype == 'p')
		{
			/* Special hack for pg_statistic: allow ANYARRAY during initdb */
			if (atttypid != ANYARRAYOID || IsUnderPostmaster)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("column \"%s\" has pseudo-type %s",
								attname, format_type_be(atttypid)),
										   errOmitLocation(true)));
		}
	}
}


/* --------------------------------
 *		CheckAttributeArray
 *
 *		Verify that whether the attribute is of type array,
 *		parquet table doesn't support array type
 * --------------------------------
 */
void CheckAttributeForParquet(TupleDesc tupdesc, char relstorage){

	if(relstorage != RELSTORAGE_PARQUET){
		return;
	}
	int	natts = tupdesc->natts;
	for(int i = 0; i < natts; i++){
		switch(tupdesc->attrs[i]->atttypid){
			case HAWQ_TYPE_BOOL:
			case HAWQ_TYPE_CHAR:
			case HAWQ_TYPE_NAME:
			case HAWQ_TYPE_INT8:
			case HAWQ_TYPE_INT2:
			case HAWQ_TYPE_INT4:
			case HAWQ_TYPE_FLOAT4:
			case HAWQ_TYPE_FLOAT8:
			case HAWQ_TYPE_MONEY:
			case HAWQ_TYPE_NUMERIC:
			case HAWQ_TYPE_BYTE:
			case HAWQ_TYPE_TEXT:
			case HAWQ_TYPE_XML:
			case HAWQ_TYPE_MACADDR:
			case HAWQ_TYPE_INET:
			case HAWQ_TYPE_CIDR:
			case HAWQ_TYPE_BPCHAR:
			case HAWQ_TYPE_VARCHAR:
			case HAWQ_TYPE_DATE:
			case HAWQ_TYPE_TIME:
			case HAWQ_TYPE_TIMESTAMP:
			case HAWQ_TYPE_TIMETZ:
			case HAWQ_TYPE_TIMESTAMPTZ:
			case HAWQ_TYPE_INTERVAL:
			case HAWQ_TYPE_BIT:
			case HAWQ_TYPE_VARBIT:
			case HAWQ_TYPE_POINT:
			case HAWQ_TYPE_LSEG:
			case HAWQ_TYPE_PATH:
			case HAWQ_TYPE_BOX:
			case HAWQ_TYPE_POLYGON:
			case HAWQ_TYPE_CIRCLE:
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
						 errmsg("data type for column %s is not supported for parquet table yet",
								 NameStr(tupdesc->attrs[i]->attname)),
						 errOmitLocation(true)));
		}

		if(tupdesc->attrs[i]->attndims != 0){
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("array type of column \"%s\" is not supported for parquet table yet",
						 NameStr(tupdesc->attrs[i]->attname))));
		}
	}
}


/* MPP-6929: metadata tracking */
/* --------------------------------
 *		MetaTrackAddObject
 *
 *		Track creation of object in pg_stat_last_operation. The
 *		arguments are:
 *
 *		classid		- the oid of the table containing the object, eg
 *					  "pg_class" for a relation
 *		objoid		- the oid of the object itself in the specified table
 *		relowner	- role ? user ?
 *		actionname	- generally CREATE for this case
 *		subtype		- some generic descriptive, eg TABLE for a "CREATE TABLE"
 *
 *
 * --------------------------------
 */

static void MetaTrackAddUpdInternal(cqContext  *pcqCtx,
									Oid			classid,
									Oid			objoid,
									Oid			relowner,
									char*		actionname,
									char*		subtype,
									Relation	rel,
									HeapTuple	old_tuple)
{
	HeapTuple	new_tuple;
	Datum		values[Natts_pg_statlastop];
	bool		isnull[Natts_pg_statlastop];
	bool		new_record_repl[Natts_pg_statlastop];
	NameData	uname;
	NameData	aname;

	MemSet(isnull, 0, sizeof(bool) * Natts_pg_statlastop);
	MemSet(new_record_repl, 0, sizeof(bool) * Natts_pg_statlastop);

	values[Anum_pg_statlastop_classid - 1] = ObjectIdGetDatum(classid);
	values[Anum_pg_statlastop_objid - 1] = ObjectIdGetDatum(objoid);

	aname.data[0] = '\0';
	namestrcpy(&aname, actionname);
	values[Anum_pg_statlastop_staactionname - 1] = NameGetDatum(&aname);

	values[Anum_pg_statlastop_stasysid - 1] = ObjectIdGetDatum(relowner);
	/* set this column to update */
	new_record_repl[Anum_pg_statlastop_stasysid - 1] = true;

	uname.data[0] = '\0';

	{
		char	*rolnamestr;
		int		 fetchCount;

		rolnamestr = caql_getcstring_plus(
				NULL,
				&fetchCount,
				NULL,
				cql("SELECT rolname FROM pg_authid "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(relowner)));

		if (fetchCount)
		{
			namestrcpy(&uname, rolnamestr);
			pfree(rolnamestr);
		}
		else
		{
			/* Generate numeric OID if we don't find an entry */
			sprintf(NameStr(uname), "%u", relowner);
		}
	}

	values[Anum_pg_statlastop_stausename - 1] = NameGetDatum(&uname);
	/* set this column to update */
	new_record_repl[Anum_pg_statlastop_stausename - 1] = true;

	values[Anum_pg_statlastop_stasubtype - 1] = CStringGetTextDatum(subtype);
	/* set this column to update */
	new_record_repl[Anum_pg_statlastop_stasubtype - 1] = true;

	values[Anum_pg_statlastop_statime - 1] = GetCurrentTimestamp();
	/* set this column to update */
	new_record_repl[Anum_pg_statlastop_statime - 1] = true;

	if (HeapTupleIsValid(old_tuple))
	{
		new_tuple = caql_modify_current(pcqCtx,
										values,
										isnull, new_record_repl);

		caql_update_current(pcqCtx, new_tuple);
		/* and Update indexes (implicit) */
	}
	else
	{
		new_tuple = caql_form_tuple(pcqCtx, values, isnull);

		(void) caql_insert(pcqCtx, new_tuple);
		/* and Update indexes (implicit) */
	}

	if (HeapTupleIsValid(old_tuple))
		heap_freetuple(new_tuple);

} /* end MetaTrackAddUpdInternal */


void MetaTrackAddObject(Oid		classid, 
						Oid		objoid, 
						Oid		relowner,
						char*	actionname,
						char*	subtype)
{
	Relation	rel;
	cqContext	cqc;
	cqContext  *pcqCtx;

	if (IsBootstrapProcessingMode())
		return;

	if (IsSharedRelation(classid))
	{
		rel = heap_open(StatLastShOpRelationId, RowExclusiveLock);
		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), rel),
				cql("INSERT INTO pg_stat_last_shoperation ",
					NULL));
	}
	else
	{
		rel = heap_open(StatLastOpRelationId, RowExclusiveLock);
		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), rel),
				cql("INSERT INTO pg_stat_last_operation ",
					NULL));
	}

	MetaTrackAddUpdInternal(pcqCtx,
							classid, objoid, relowner,
							actionname, subtype,
							rel, InvalidOid);

	caql_endscan(pcqCtx);
	heap_close(rel, RowExclusiveLock);

/*	CommandCounterIncrement(); */

} /* end MetaTrackAddObject */

void MetaTrackUpdObject(Oid		classid, 
						Oid		objoid, 
						Oid		relowner,
						char*	actionname,
						char*	subtype)
{
	HeapTuple	tuple;
	cqContext  *pcqCtx;
	cqContext	cqc;
	Relation	rel;
	int			ii = 0;

	if (IsBootstrapProcessingMode())
		return;

	if (IsSharedRelation(classid))
		rel = heap_open(StatLastShOpRelationId, RowExclusiveLock);
	else
		rel = heap_open(StatLastOpRelationId, RowExclusiveLock);

	if (IsSharedRelation(classid))
	{
		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), rel),
				cql("SELECT * FROM pg_stat_last_shoperation "
					" WHERE classid = :1 "
					" AND objid = :2 "
					" AND staactionname = :3 "
					" FOR UPDATE ",
					ObjectIdGetDatum(classid),
					ObjectIdGetDatum(objoid),
					CStringGetDatum(actionname)));
	}
	else
	{
		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), rel),
				cql("SELECT * FROM pg_stat_last_operation "
					" WHERE classid = :1 "
					" AND objid = :2 "
					" AND staactionname = :3 "
					" FOR UPDATE ",
					ObjectIdGetDatum(classid),
					ObjectIdGetDatum(objoid),
					CStringGetDatum(actionname)));
	}

	/* should be a unique index - only 1 answer... */
	while (HeapTupleIsValid(tuple = caql_getnext(pcqCtx)))
	{
		if (HeapTupleIsValid(tuple))
		{
			MetaTrackAddUpdInternal(pcqCtx,
									classid, objoid, relowner,
									actionname, subtype,
									rel, tuple);
			
/*			CommandCounterIncrement(); */

			ii++;
		}
	}
	caql_endscan(pcqCtx);
	heap_close(rel, RowExclusiveLock);

	/* add it if it didn't already exist */
	if (!ii)
		MetaTrackAddObject(classid, 
						   objoid, 
						   relowner,
						   actionname,
						   subtype);

} /* end MetaTrackUpdObject */
void MetaTrackDropObject(Oid		classid, 
						 Oid		objoid)
{
	int ii = 0;

	if (IsSharedRelation(classid))
	{
		ii = caql_getcount(
				NULL,
				cql("DELETE FROM pg_stat_last_shoperation "
					" WHERE classid = :1 "
					" AND objid = :2 ",
					ObjectIdGetDatum(classid),
					ObjectIdGetDatum(objoid)));
	}
	else
	{
		ii = caql_getcount(
				NULL,
				cql("DELETE FROM pg_stat_last_operation "
					" WHERE classid = :1 "
					" AND objid = :2 ",
					ObjectIdGetDatum(classid),
					ObjectIdGetDatum(objoid)));
	}

} /* end MetaTrackDropObject */



/* --------------------------------
 *		AddNewAttributeTuples
 *
 *		this registers the new relation's schema by adding
 *		tuples to pg_attribute.
 * --------------------------------
 */
static void
AddNewAttributeTuples(Oid new_rel_oid,
					  TupleDesc tupdesc,
					  char relkind,
					  bool oidislocal,
					  int oidinhcount)
{
	const Form_pg_attribute *dpp;
	int			i;
	HeapTuple	tup;
	int			natts = tupdesc->natts;
	ObjectAddress myself,
				referenced;
	cqContext  *pcqCtx;

	/*
	 * open pg_attribute and its indexes.
	 */
	pcqCtx = caql_beginscan(
			NULL,
			cql("INSERT INTO pg_attribute ",
				NULL));

	/*
	 * First we add the user attributes.  This is also a convenient place to
	 * add dependencies on their datatypes.
	 */
	dpp = tupdesc->attrs;
	for (i = 0; i < natts; i++)
	{
		/* Fill in the correct relation OID */
		(*dpp)->attrelid = new_rel_oid;
		/* Make sure these are OK, too */
		(*dpp)->attstattarget = -1;
		(*dpp)->attcacheoff = -1;

		tup = heap_addheader(Natts_pg_attribute,
							 false,
							 ATTRIBUTE_TUPLE_SIZE,
							 (void *) *dpp);

		caql_insert(pcqCtx, tup);
		/* and Update indexes (implicit) */

		heap_freetuple(tup);

		myself.classId = RelationRelationId;
		myself.objectId = new_rel_oid;
		myself.objectSubId = i + 1;
		referenced.classId = TypeRelationId;
		referenced.objectId = (*dpp)->atttypid;
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

		dpp++;
	}

	/*
	 * Next we add the system attributes.  Skip OID if rel has no OIDs. Skip
	 * all for a view or type relation.  We don't bother with making datatype
	 * dependencies here, since presumably all these types are pinned.
	 */
	if (relkind != RELKIND_VIEW && relkind != RELKIND_COMPOSITE_TYPE)
	{
		dpp = SysAtt;
		for (i = 0; i < -1 - FirstLowInvalidHeapAttributeNumber; i++)
		{
			if (tupdesc->tdhasoid ||
				(*dpp)->attnum != ObjectIdAttributeNumber)
			{
				Form_pg_attribute attStruct;

				tup = heap_addheader(Natts_pg_attribute,
									 false,
									 ATTRIBUTE_TUPLE_SIZE,
									 (void *) *dpp);
				attStruct = (Form_pg_attribute) GETSTRUCT(tup);

				/* Fill in the correct relation OID in the copied tuple */
				attStruct->attrelid = new_rel_oid;

				/* Fill in correct inheritance info for the OID column */
				if (attStruct->attnum == ObjectIdAttributeNumber)
				{
					attStruct->attislocal = oidislocal;
					attStruct->attinhcount = oidinhcount;
				}

				/*
				 * Unneeded since they should be OK in the constant data
				 * anyway
				 */
				/* attStruct->attstattarget = 0; */
				/* attStruct->attcacheoff = -1; */

				caql_insert(pcqCtx, tup);
				/* and Update indexes (implicit) */

				heap_freetuple(tup);
			}
			dpp++;
		}
	}

	/*
	 * clean up
	 */
	caql_endscan(pcqCtx); /* close rel, indexes */
}

/* --------------------------------
 *		InsertPgClassTuple
 *
 *		Construct and insert a new tuple in pg_class.
 *
 * Caller has already opened and locked pg_class.
 * Tuple data is taken from new_rel_desc->rd_rel, except for the
 * variable-width fields which are not present in a cached reldesc.
 * We always initialize relacl to NULL (i.e., default permissions),
 * and reloptions is set to the passed-in text array (if any).
 * --------------------------------
 */
void
InsertPgClassTuple(Relation pg_class_desc,
				   Relation new_rel_desc,
				   Oid new_rel_oid,
				   Datum reloptions)
{
	Form_pg_class rd_rel = new_rel_desc->rd_rel;
	Datum		values[Natts_pg_class];
	bool		nulls[Natts_pg_class];
	HeapTuple	tup;
	cqContext	cqc;
	cqContext  *pcqCtx;

	/* This is a tad tedious, but way cleaner than what we used to do... */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_class_relname - 1] = NameGetDatum(&rd_rel->relname);
	values[Anum_pg_class_relnamespace - 1] = ObjectIdGetDatum(rd_rel->relnamespace);
	values[Anum_pg_class_reltype - 1] = ObjectIdGetDatum(rd_rel->reltype);
	values[Anum_pg_class_relowner - 1] = ObjectIdGetDatum(rd_rel->relowner);
	values[Anum_pg_class_relam - 1] = ObjectIdGetDatum(rd_rel->relam);
	values[Anum_pg_class_relfilenode - 1] = ObjectIdGetDatum(rd_rel->relfilenode);
	values[Anum_pg_class_reltablespace - 1] = ObjectIdGetDatum(rd_rel->reltablespace);
	values[Anum_pg_class_relpages - 1] = Int32GetDatum(rd_rel->relpages);
	values[Anum_pg_class_reltuples - 1] = Float4GetDatum(rd_rel->reltuples);
	values[Anum_pg_class_reltoastrelid - 1] = ObjectIdGetDatum(rd_rel->reltoastrelid);
	values[Anum_pg_class_reltoastidxid - 1] = ObjectIdGetDatum(rd_rel->reltoastidxid);
	values[Anum_pg_class_relaosegrelid - 1] = ObjectIdGetDatum(rd_rel->relaosegrelid);
	values[Anum_pg_class_relaosegidxid - 1] = ObjectIdGetDatum(rd_rel->relaosegidxid);
	values[Anum_pg_class_relhasindex - 1] = BoolGetDatum(rd_rel->relhasindex);
	values[Anum_pg_class_relisshared - 1] = BoolGetDatum(rd_rel->relisshared);
	values[Anum_pg_class_relkind - 1] = CharGetDatum(rd_rel->relkind);
	values[Anum_pg_class_relstorage - 1] = CharGetDatum(rd_rel->relstorage);
	values[Anum_pg_class_relnatts - 1] = Int16GetDatum(rd_rel->relnatts);
	values[Anum_pg_class_relchecks - 1] = Int16GetDatum(rd_rel->relchecks);
	values[Anum_pg_class_reltriggers - 1] = Int16GetDatum(rd_rel->reltriggers);
	values[Anum_pg_class_relukeys - 1] = Int16GetDatum(rd_rel->relukeys);
	values[Anum_pg_class_relfkeys - 1] = Int16GetDatum(rd_rel->relfkeys);
	values[Anum_pg_class_relrefs - 1] = Int16GetDatum(rd_rel->relrefs);
	values[Anum_pg_class_relhasoids - 1] = BoolGetDatum(rd_rel->relhasoids);
	values[Anum_pg_class_relhaspkey - 1] = BoolGetDatum(rd_rel->relhaspkey);
	values[Anum_pg_class_relhasrules - 1] = BoolGetDatum(rd_rel->relhasrules);
	values[Anum_pg_class_relhassubclass - 1] = BoolGetDatum(rd_rel->relhassubclass);
	values[Anum_pg_class_relfrozenxid - 1] = TransactionIdGetDatum(rd_rel->relfrozenxid);
	/* start out with empty permissions */
	nulls[Anum_pg_class_relacl - 1] = true;
	if (reloptions != (Datum) 0)
		values[Anum_pg_class_reloptions - 1] = reloptions;
	else
		nulls[Anum_pg_class_reloptions - 1] = true;

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), pg_class_desc),
			cql("INSERT INTO pg_class ",
				NULL));

	tup = caql_form_tuple(pcqCtx, values, nulls);

	/*
	 * The new tuple must have the oid already chosen for the rel.	Sure would
	 * be embarrassing to do this sort of thing in polite company.
	 */
	HeapTupleSetOid(tup, new_rel_oid);

	/* finally insert the new tuple, update the indexes, and clean up */
	caql_insert(pcqCtx, tup);
	/* and Update indexes (implicit) */

	caql_endscan(pcqCtx);

	heap_freetuple(tup);
}

/* --------------------------------
 *		AddNewRelationTuple
 *
 *		this registers the new relation in the catalogs by
 *		adding a tuple to pg_class.
 * --------------------------------
 */
static void
AddNewRelationTuple(Relation pg_class_desc,
					Relation new_rel_desc,
					Oid new_rel_oid,
					Oid new_type_oid,
					Oid relowner,
					char relkind,
					char relstorage,
					Datum reloptions)
{
	Form_pg_class new_rel_reltup;

	/*
	 * first we update some of the information in our uncataloged relation's
	 * relation descriptor.
	 */
	new_rel_reltup = new_rel_desc->rd_rel;

	switch (relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_INDEX:
		case RELKIND_TOASTVALUE:
		case RELKIND_AOSEGMENTS:
		case RELKIND_AOBLOCKDIR:
			/* The relation is real, but as yet empty */
			new_rel_reltup->relpages = 0;
			new_rel_reltup->reltuples = 0;

			/* estimated stats for external tables */
			/* NOTE: look at cdb_estimate_rel_size() if changing these values */
			if(relstorage_is_external(relstorage))
			{
				new_rel_reltup->relpages = gp_external_table_default_number_of_pages; 
				new_rel_reltup->reltuples = gp_external_table_default_number_of_tuples;
			}
			break;
		case RELKIND_SEQUENCE:
			/* Sequences always have a known size */
			new_rel_reltup->relpages = 1;
			new_rel_reltup->reltuples = 1;
			break;
		default:
			/* Views, etc, have no disk storage */
			new_rel_reltup->relpages = 0;
			new_rel_reltup->reltuples = 0;
			break;
	}

	/* Initialize relfrozenxid */
	if (relkind == RELKIND_RELATION ||
		relkind == RELKIND_TOASTVALUE ||
		relkind == RELKIND_AOSEGMENTS ||
		relkind == RELKIND_AOBLOCKDIR)
	{
		/*
		 * Initialize to the minimum XID that could put tuples in the table.
		 * We know that no xacts older than RecentXmin are still running,
		 * so that will do.
		 */
		if (!IsBootstrapProcessingMode())
			new_rel_reltup->relfrozenxid = RecentXmin;
		else
			new_rel_reltup->relfrozenxid = FirstNormalTransactionId;
	}
	else
	{
		/*
		 * Other relation types will not contain XIDs, so set relfrozenxid to
		 * InvalidTransactionId.  (Note: a sequence does contain a tuple, but
		 * we force its xmin to be FrozenTransactionId always; see
		 * commands/sequence.c.)
		 */
		new_rel_reltup->relfrozenxid = InvalidTransactionId;
	}

	new_rel_reltup->relowner = relowner;
	new_rel_reltup->reltype = new_type_oid;
	new_rel_reltup->relkind = relkind;
	new_rel_reltup->relstorage = relstorage;

	new_rel_desc->rd_att->tdtypeid = new_type_oid;

	/* Now build and insert the tuple */
	InsertPgClassTuple(pg_class_desc, new_rel_desc, new_rel_oid, reloptions);
}


/* --------------------------------
 *		AddNewRelationType -
 *
 *		define a composite type corresponding to the new relation
 * --------------------------------
 */
static Oid
AddNewRelationType(const char *typeName,
				   Oid typeNamespace,
				   Oid new_rel_oid,
				   char new_rel_kind,
				   Oid ownerid)
{
	return
		TypeCreate(typeName,	/* type name */
				   typeNamespace,		/* type namespace */
				   new_rel_oid, /* relation oid */
				   new_rel_kind,	/* relation kind */
				   ownerid,             /* owner's ID */
				   -1,			/* internal size (varlena) */
				   'c',			/* type-type (complex) */
				   ',',			/* default array delimiter */
				   F_RECORD_IN, /* input procedure */
				   F_RECORD_OUT,	/* output procedure */
				   F_RECORD_RECV,		/* receive procedure */
				   F_RECORD_SEND,		/* send procedure */
				   InvalidOid,	/* analyze procedure - default */
				   InvalidOid,	/* array element type - irrelevant */
				   InvalidOid,	/* domain base type - irrelevant */
				   NULL,		/* default value - none */
				   NULL,		/* default binary representation */
				   false,		/* passed by reference */
				   'd',			/* alignment - must be the largest! */
				   'x',			/* fully TOASTable */
				   -1,			/* typmod */
				   0,			/* array dimensions for typBaseType */
				   false);		/* Type NOT NULL */
}

void
InsertGpRelfileNodeTuple(
	Relation 		gp_relfile_node,
	Oid				relationId,
	char			*relname,
	Oid				relfilenode,
	int32			segmentFileNum,
	bool			updateIndex,
	ItemPointer		persistentTid,
	int64			persistentSerialNum)
{
	Datum		values[Natts_gp_relfile_node];
	bool		nulls[Natts_gp_relfile_node];
	HeapTuple	tuple;
//	cqContext	cqc;
//	cqContext  *pcqCtx;

	if (Debug_check_for_invalid_persistent_tid &&
		!Persistent_BeforePersistenceWork() &&
		PersistentStore_IsZeroTid(persistentTid))
	{	
		elog(ERROR, 
			 "Inserting with invalid TID (0,0) into relation id %u '%s', relfilenode %u, segment file #%d, serial number " INT64_FORMAT,
			 relationId,
			 relname,
			 relfilenode,
			 segmentFileNum,
			 persistentSerialNum);
	}

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
			 "InsertGpRelationNodeTuple: Inserting into relation id %u '%s', relfilenode %u, segment file #%d, serial number " INT64_FORMAT ", TID %s",
			 relationId,
			 relname,
			 relfilenode,
			 segmentFileNum,
			 persistentSerialNum,
			 ItemPointerToString(persistentTid));

//	pcqCtx = caql_beginscan(
//			caql_addrel(cqclr(&cqc), pg_class_desc),
//			cqlXXX("INSERT INTO gp_relation_node ",
//				NULL));

	GpRelfileNode_SetDatumValues(
								values,
								relfilenode,
								segmentFileNum,
								persistentTid,
								persistentSerialNum);

//	tuple = caql_form_tuple(pcqCtx, values, nulls);
	/* XXX XXX: note optional index update */
	tuple = heap_form_tuple(RelationGetDescr(gp_relfile_node), values, nulls);

	/* finally insert the new tuple, update the indexes, and clean up */
	simple_heap_insert(gp_relfile_node, tuple);

	if (updateIndex)
	{
		CatalogUpdateIndexes(gp_relfile_node, tuple);
	}

//	caql_endscan(pcqCtx);

	heap_freetuple(tuple);
}

void
UpdateGpRelfileNodeTuple(
	Relation 	gp_relfile_node,
	HeapTuple 	tuple,
	Oid			relfilenode,
	int32		segmentFileNum,
	ItemPointer persistentTid,
	int64 		persistentSerialNum)
{
	Datum		repl_val[Natts_gp_relfile_node];
	bool		repl_null[Natts_gp_relfile_node];
	bool		repl_repl[Natts_gp_relfile_node];
	HeapTuple	newtuple;

	if (Debug_check_for_invalid_persistent_tid &&
		!Persistent_BeforePersistenceWork() &&
		PersistentStore_IsZeroTid(persistentTid))
	{	
		elog(ERROR, 
			 "Updating with invalid TID (0,0) in relfilenode %u, segment file #%d, serial number " INT64_FORMAT,
			 relfilenode,
			 segmentFileNum,
			 persistentSerialNum);
	}

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
			 "UpdateGpRelationNodeTuple: Updating relfilenode %u, segment file #%d, serial number " INT64_FORMAT " at TID %s",
			 relfilenode,
			 segmentFileNum,
			 persistentSerialNum,
			 ItemPointerToString(persistentTid));

	memset(repl_val, 0, sizeof(repl_val));
	memset(repl_null, false, sizeof(repl_null));
	memset(repl_repl, false, sizeof(repl_null));

	repl_repl[Anum_gp_relfile_node_relfilenode_oid - 1] = true;
	repl_val[Anum_gp_relfile_node_relfilenode_oid - 1] = ObjectIdGetDatum(relfilenode);
	
	repl_repl[Anum_gp_relfile_node_segment_file_num - 1] = true;
	repl_val[Anum_gp_relfile_node_segment_file_num - 1] = Int32GetDatum(segmentFileNum);

	// UNDONE: createMirrorDataLossTrackingSessionNum

	repl_repl[Anum_gp_relfile_node_persistent_tid- 1] = true;
	repl_val[Anum_gp_relfile_node_persistent_tid- 1] = PointerGetDatum(persistentTid);
	
	repl_repl[Anum_gp_relfile_node_persistent_serial_num - 1] = true;
	repl_val[Anum_gp_relfile_node_persistent_serial_num - 1] = Int64GetDatum(persistentSerialNum);

	newtuple = heap_modify_tuple(tuple, RelationGetDescr(gp_relfile_node), repl_val, repl_null, repl_repl);
	
	simple_heap_update(gp_relfile_node, &newtuple->t_self, newtuple);

	CatalogUpdateIndexes(gp_relfile_node, newtuple);

	heap_freetuple(newtuple);
}


static void
AddNewRelfileNodeTuple(
						Relation gp_relfile_node,
						Relation new_rel)
{
	if (new_rel->rd_relationnodeinfo.isPresent)
	{
		InsertGpRelfileNodeTuple(
							gp_relfile_node,
							new_rel->rd_id,
							new_rel->rd_rel->relname.data,
							new_rel->rd_rel->relfilenode,
							/* segmentFileNum */ 0,
							/* updateIndex */ true,
							&new_rel->rd_relationnodeinfo.persistentTid,
							new_rel->rd_relationnodeinfo.persistentSerialNum);
							
	}
}

/* --------------------------------
 *		heap_create_with_catalog
 *
 *		creates a new cataloged relation.  see comments above.
 * --------------------------------
 */
Oid
heap_create_with_catalog(const char *relname,
						 Oid relnamespace,
						 Oid reltablespace,
						 Oid relid,
						 Oid ownerid,
						 TupleDesc tupdesc,
						 Oid relam,
						 char relkind,
						 char relstorage,
						 bool shared_relation,
						 bool oidislocal,
						 bool bufferPoolBulkLoad,
						 int oidinhcount,
						 OnCommitAction oncommit,
                         const struct GpPolicy *policy,
                         Datum reloptions,
						 bool allow_system_table_mods,
						 Oid *comptypeOid,
						 ItemPointer persistentTid,
						 int64 *persistentSerialNum)
{
	Relation	pg_class_desc;
	Relation	gp_relfile_node_desc;
	Relation	new_rel_desc;
	Oid			new_type_oid;
	bool		appendOnlyRel;
	StdRdOptions *stdRdOptions;
	int			safefswritesize = gp_safefswritesize;
	bool		override = false;

	pg_class_desc = heap_open(RelationRelationId, RowExclusiveLock);

    // When creating gp_persistent_relfile_node, we can't directly insert meta info into gp_relfile_node 
    // for this table is renamed from gp_relation_node, also it's schema changed.
	if (IsBootstrapProcessingMode()|| (gp_upgrade_mode && GpPersistent_IsPersistentRelation(relid)))
		gp_relfile_node_desc = NULL;
	else
		gp_relfile_node_desc = heap_open(GpRelfileNodeRelationId, RowExclusiveLock);

	/*
	 * sanity checks
	 */
	Assert(IsNormalProcessingMode() || IsBootstrapProcessingMode());

	/*
	 * Was "appendonly" specified in the relopts? If yes, fix our relstorage.
	 * Also, check for override (debug) GUCs.
	 * During upgrade, do not validate because we accept tidycat options as well.
	 */
	stdRdOptions = (StdRdOptions*) heap_reloptions(relkind, reloptions, !gp_upgrade_mode);
	heap_test_override_reloptions(relkind, stdRdOptions, &safefswritesize);
	appendOnlyRel = stdRdOptions->appendonly;
	if(appendOnlyRel)
	{
		if(stdRdOptions->columnstore == RELSTORAGE_PARQUET){
			DetectHostEndian();
		}
		relstorage = stdRdOptions->columnstore;
	}
	
	reltablespace = GetSuitableTablespace(relkind, relstorage,
									  reltablespace, &override);
	if (override)
	{
		if (stdRdOptions->forceHeap)
			ereport(ERROR,
					(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
					 errmsg("tablespace \"%s\" does not support heap relation",
							get_tablespace_name(reltablespace)),
					 errOmitLocation(true)));
		appendOnlyRel = stdRdOptions->appendonly = true;
		relstorage = stdRdOptions->columnstore;

		reloptions = transformRelOptions(reloptions,
										list_make1(makeDefElem("appendonly", (Node *) makeString("true"))),
										true,
										false);
	}

	validateAppendOnlyRelOptions(appendOnlyRel,
								 stdRdOptions->blocksize,
								 stdRdOptions->pagesize,
								 stdRdOptions->rowgroupsize,
								 safefswritesize,
								 stdRdOptions->compresslevel,
								 stdRdOptions->compresstype,
								 stdRdOptions->checksum,
								 relkind,
								 stdRdOptions->columnstore);

	/* MPP-8058: disallow OIDS on column-oriented tables */
	if (tupdesc->tdhasoid && 
		IsNormalProcessingMode() &&
        (Gp_role == GP_ROLE_DISPATCH))
	{
		if ((relstorage == RELSTORAGE_PARQUET))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg(
							 "OIDS=TRUE is not allowed on tables that "
							 "use column-oriented storage. Use OIDS=FALSE"
							 ),
					 errOmitLocation(true)));
		else
			ereport(NOTICE,
					(errmsg(
							 "OIDS=TRUE is not recommended for user-created "
							 "tables. Use OIDS=FALSE to prevent wrap-around "
							 "of the OID counter"
							 ),
					 errOmitLocation(true)));
	}

	CheckAttributeNamesTypes(tupdesc, relkind);

	CheckAttributeForParquet(tupdesc, relstorage);

	if (get_relname_relid(relname, relnamespace))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" already exists", relname),
						   errOmitLocation(true)));

	/*
	 * Allocate an OID for the relation, unless we were told what to use.
	 *
	 * The OID will be the relfilenode as well, so make sure it doesn't
	 * collide with either pg_class OIDs or existing physical files.
	 */
	if (!OidIsValid(relid))
		relid = GetNewRelFileNode(reltablespace, shared_relation,
								  pg_class_desc,
								  relstorage_is_ao(pg_class_desc->rd_rel->relstorage));
	else
		if (IsUnderPostmaster)
		{
			CheckNewRelFileNodeIsOk(relid, reltablespace, shared_relation,
									pg_class_desc,
									relstorage_is_ao(pg_class_desc->rd_rel->relstorage));
		}

	/*
	 * Create the relcache entry (mostly dummy at this point) and the physical
	 * disk file.  (If we fail further down, it's the smgr's responsibility to
	 * remove the disk file again.)
	 */
	new_rel_desc = heap_create(relname,
							   relnamespace,
							   reltablespace,
							   relid,
							   tupdesc,
							   relam,
							   relkind,
							   relstorage,
							   shared_relation,
							   allow_system_table_mods,
							   bufferPoolBulkLoad);

	Assert(relid == RelationGetRelid(new_rel_desc));

	if (persistentTid != NULL)
	{
		*persistentTid =
				new_rel_desc->rd_relationnodeinfo.persistentTid;
		*persistentSerialNum =
				new_rel_desc->rd_relationnodeinfo.persistentSerialNum;
	}

	/*
	 * since defining a relation also defines a complex type, we add a new
	 * system type corresponding to the new relation.
	 *
	 * NOTE: we could get a unique-index failure here, in case the same name
	 * has already been used for a type.
	 *
	 * Don't create the shell type if the bootstrapper tells us it already
	 * knows what it is. Importing for upgrading.
	 */
	if (IsBootstrapProcessingMode() &&
		(PointerIsValid(comptypeOid) && OidIsValid(*comptypeOid)))
	{
		new_type_oid = *comptypeOid;
	}
	else
	{
		if (comptypeOid == NULL || *comptypeOid == InvalidOid)
			new_type_oid = AddNewRelationType(relname,
											  relnamespace,
											  relid,
											  relkind,
											  ownerid);
		else
		{
			new_type_oid = TypeCreateWithOid(relname,	/* type name */
					   relnamespace,		/* type namespace */
					   relid, /* relation oid */
					   relkind,	/* relation kind */
					   ownerid,
					   -1,			/* internal size (varlena) */
					   'c',			/* type-type (complex) */
					   ',',			/* default array delimiter */
					   F_RECORD_IN, /* input procedure */
					   F_RECORD_OUT,	/* output procedure */
					   F_RECORD_RECV,		/* receive procedure */
					   F_RECORD_SEND,		/* send procedure */
					   InvalidOid,	/* analyze procedure - default */
					   InvalidOid,	/* array element type - irrelevant */
					   InvalidOid,	/* domain base type - irrelevant */
					   NULL,		/* default value - none */
					   NULL,		/* default binary representation */
					   false,		/* passed by reference */
					   'd',			/* alignment - must be the largest! */
					   'x',			/* fully TOASTable */
					   -1,			/* typmod */
					   0,			/* array dimensions for typBaseType */
					   false,		/* Type NOT NULL */
					   *comptypeOid,
					   0);
		}
	}

	if (comptypeOid)
		*comptypeOid = new_type_oid;
	/*
	 * now create an entry in pg_class for the relation.
	 *
	 * NOTE: we could get a unique-index failure here, in case someone else is
	 * creating the same relation name in parallel but hadn't committed yet
	 * when we checked for a duplicate name above.
	 */
	AddNewRelationTuple(pg_class_desc,
						new_rel_desc,
						relid,
						new_type_oid,
						ownerid,
						relkind,
						relstorage,
						reloptions);

	if (gp_relfile_node_desc != NULL)
	{
		if (!appendOnlyRel)
		{
			/*
			 * for heap table and index, use master's contentid always
			 */
			AddNewRelfileNodeTuple(
						gp_relfile_node_desc,
						new_rel_desc);
		}
		heap_close(gp_relfile_node_desc, RowExclusiveLock);
	}

	/* Add an entry in pg_appendonly. */
	if(appendOnlyRel)
	{
		if(relstorage == RELSTORAGE_PARQUET)
		{
			InsertAppendOnlyEntry(relid,
							  stdRdOptions->rowgroupsize,
							  stdRdOptions->pagesize,
							  0,
							  safefswritesize,
							  stdRdOptions->compresslevel,
							  1,
							  0,
							  stdRdOptions->checksum,
							  (stdRdOptions->columnstore == RELSTORAGE_AOROWS) ? false : true,
							  stdRdOptions->compresstype,
							  InvalidOid,
							  InvalidOid,
							  InvalidOid,
							  InvalidOid);
		}
		else if (relstorage == RELSTORAGE_AOROWS)
		{
			InsertAppendOnlyEntry(relid,
								  stdRdOptions->blocksize,
								  0,
								  DEFAULT_SPLIT_WRITE_SIZE,
								  safefswritesize,
								  stdRdOptions->compresslevel,
								  2,
								  0,
								  stdRdOptions->checksum,
								  (stdRdOptions->columnstore == RELSTORAGE_AOROWS) ? false : true,
								  stdRdOptions->compresstype,
								  InvalidOid,
								  InvalidOid,
								  InvalidOid,
								  InvalidOid);
		}
	}


	/*
	 * now add tuples to pg_attribute for the attributes in our new relation.
	 */
	AddNewAttributeTuples(relid, new_rel_desc->rd_att, relkind,
						  oidislocal, oidinhcount);

	/*
	 * Make a dependency link to force the relation to be deleted if its
	 * namespace is.  Also make a dependency link to its owner.
	 *
	 * For composite types, these dependencies are tracked for the pg_type
	 * entry, so we needn't record them here.  Likewise, TOAST tables don't
	 * need a namespace dependency (they live in a pinned namespace) nor an
	 * owner dependency (they depend indirectly through the parent table).
	 * Also, skip this in bootstrap mode, since we don't make dependencies
	 * while bootstrapping.
	 */
	if (relkind != RELKIND_COMPOSITE_TYPE &&
		relkind != RELKIND_TOASTVALUE &&
		!IsBootstrapProcessingMode())
	{
		ObjectAddress myself,
					referenced;

		myself.classId = RelationRelationId;
		myself.objectId = relid;
		myself.objectSubId = 0;
		referenced.classId = NamespaceRelationId;
		referenced.objectId = relnamespace;
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

		recordDependencyOnOwner(RelationRelationId, relid, ownerid);
	}

	/*
	 * Store any supplied constraints and defaults.
	 *
	 * NB: this may do a CommandCounterIncrement and rebuild the relcache
	 * entry, so the relation must be valid and self-consistent at this point.
	 * In particular, there are not yet constraints and defaults anywhere.
	 */
	StoreConstraints(new_rel_desc, tupdesc);

	/*
	 * If there's a special on-commit action, remember it
	 */
	if (oncommit != ONCOMMIT_NOOP)
		register_on_commit_action(relid, oncommit);

	/*
     * CDB: If caller gave us a distribution policy, store the distribution
     * key column list in the gp_distribution_policy catalog and attach a
     * copy to the relcache entry.
     */
    if (!gp_upgrade_mode && policy &&
        Gp_role == GP_ROLE_DISPATCH)
    {
        Assert(relkind == RELKIND_RELATION);
        new_rel_desc->rd_cdbpolicy = GpPolicyCopy(GetMemoryChunkContext(new_rel_desc), policy);
        GpPolicyStore(relid, policy);
    }

	if (Gp_role == GP_ROLE_DISPATCH) /* MPP-11313: */
	{
		bool doIt = true;
		char *subtyp = "TABLE";

		switch (relkind)
		{
			case RELKIND_RELATION:
				break;
			case RELKIND_INDEX:
				subtyp = "INDEX";
				break;
			case RELKIND_SEQUENCE:
				subtyp = "SEQUENCE";
				break;
			case RELKIND_VIEW:
				subtyp = "VIEW";
				break;
			default:
				doIt = false;
		}

		/* MPP-7576: don't track internal namespace tables */
		switch (relnamespace) 
		{
			case PG_CATALOG_NAMESPACE:
				/* MPP-7773: don't track objects in system namespace
				 * if modifying system tables (eg during upgrade)  
				 */
				if (allowSystemTableModsDDL)
					doIt = false;
				break;

			case PG_TOAST_NAMESPACE:
			case PG_BITMAPINDEX_NAMESPACE:
			case PG_AOSEGMENT_NAMESPACE:
				doIt = false;
				break;
			default:
				break;
		}

		/* MPP-7572: not valid if in any temporary namespace */
		if (doIt)
			doIt = (!(isAnyTempNamespace(relnamespace)));

		/* MPP-6929: metadata tracking */
		if (doIt)
			MetaTrackAddObject(RelationRelationId,
							   relid, GetUserId(), /* not ownerid */
							   "CREATE", subtyp
					);
	}

	/*
	 * ok, the relation has been cataloged, so close our relations and return
	 * the OID of the newly created relation.
	 */
	heap_close(new_rel_desc, NoLock);	/* do not unlock till end of xact */

	heap_close(pg_class_desc, RowExclusiveLock);

	return relid;
} /* end heap_create_with_catalog */


/*
 *		RelationRemoveInheritance
 *
 * Formerly, this routine checked for child relations and aborted the
 * deletion if any were found.	Now we rely on the dependency mechanism
 * to check for or delete child relations.	By the time we get here,
 * there are no children and we need only remove any pg_inherits rows
 * linking this relation to its parent(s).
 */
static void
RelationRemoveInheritance(Oid relid)
{
	int numDel = 0;

	numDel = caql_getcount(
			NULL,
			cql("DELETE FROM pg_inherits "
				" WHERE inhrelid = :1 ",
				ObjectIdGetDatum(relid)));
}

/* del_part_entry_by_key is superfluous - removed */

static void
RemovePartitioning(Oid relid)
{
	Relation rel;
	HeapTuple tuple;
	Relation pgrule;
	cqContext	*pcqCtx;
	cqContext	 cqc, cqcrul;
	int			 numDel = 0;

	if (Gp_role == GP_ROLE_EXECUTE)
		return;

	RemovePartitionEncodingByRelid(relid);

	/* loop through all matches in pg_partition */
	rel = heap_open(PartitionRelationId, RowExclusiveLock);
	pgrule = heap_open(PartitionRuleRelationId,
					   RowExclusiveLock);

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), rel),
			cql("SELECT * FROM pg_partition "
				" WHERE parrelid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(relid)));

	while (HeapTupleIsValid(tuple = caql_getnext(pcqCtx)))
	{
		Oid paroid = HeapTupleGetOid(tuple);

		numDel = caql_getcount(
				caql_addrel(cqclr(&cqcrul), pgrule),
				cql("DELETE FROM pg_partition_rule "
					" WHERE paroid = :1 ",
					ObjectIdGetDatum(paroid)));

		/* remove ourself */
		caql_delete_current(pcqCtx);
	}
	caql_endscan(pcqCtx);
	heap_close(rel, NoLock);

	/* we might be a leaf partition: delete any records */

	numDel = caql_getcount(
			caql_addrel(cqclr(&cqcrul), pgrule),
			cql("DELETE FROM pg_partition_rule "
				" WHERE parchildrelid = :1 ",
				ObjectIdGetDatum(relid)));

	heap_close(pgrule, NoLock);

	CommandCounterIncrement();
}

/*
 *		DeleteRelationTuple
 *
 * Remove pg_class row for the given relid.
 *
 * Note: this is shared by relation deletion and index deletion.  It's
 * not intended for use anyplace else.
 */
void
DeleteRelationTuple(Oid relid)
{
	HeapTuple	tup;
	cqContext  *pcqCtx;

	/* Grab an appropriate lock on the pg_class relation */

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(relid)));

	tup = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	/* delete the relation tuple from pg_class, and finish up */
	caql_delete_current(pcqCtx);

	caql_endscan(pcqCtx);
}

/*
 *		DeleteAttributeTuples
 *
 * Remove pg_attribute rows for the given relid.
 *
 * Note: this is shared by relation deletion and index deletion.  It's
 * not intended for use anyplace else.
 */
void
DeleteAttributeTuples(Oid relid)
{
	int numDel = 0;

	numDel = caql_getcount(
			NULL,
			cql("DELETE FROM pg_attribute "
				" WHERE attrelid = :1 ",
				ObjectIdGetDatum(relid)));
}

void
DeleteGpRelfileNodeTuple(
	Relation 	relation,
	int32		segmentFileNum)
{
	Relation	gp_relfile_node;
	HeapTuple	tuple;
/*
	Assert(NULL != relation->rd_segfile0_relationnodeinfos
			&& contentid + 1 < relation->rd_segfile0_count);
*/
	Assert(relation->rd_relationnodeinfo.isPresent);

	/* Grab an appropriate lock on the pg_class relation */
	gp_relfile_node = heap_open(GpRelfileNodeRelationId, RowExclusiveLock);

	tuple = ScanGpRelfileNodeTuple(
						gp_relfile_node,
						relation->rd_rel->relfilenode,
						segmentFileNum);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find node tuple for relation %u, relation file node %u, segment file #%d",
			 RelationGetRelid(relation),
			 relation->rd_rel->relfilenode,
			 segmentFileNum);

	/* delete the relation tuple from pg_class, and finish up */
	simple_heap_delete(gp_relfile_node, &tuple->t_self);

	heap_close(gp_relfile_node, RowExclusiveLock);
}


/*
 *		RemoveAttributeById
 *
 * This is the guts of ALTER TABLE DROP COLUMN: actually mark the attribute
 * deleted in pg_attribute.  We also remove pg_statistic entries for it.
 * (Everything else needed, such as getting rid of any pg_attrdef entry,
 * is handled by dependency.c.)
 */
void
RemoveAttributeById(Oid relid, AttrNumber attnum)
{
	Relation	rel;
	Relation	attr_rel;
	HeapTuple	tuple;
	Form_pg_attribute attStruct;
	char		newattname[NAMEDATALEN];
	cqContext	cqc;
	cqContext  *pcqCtx;

	/*
	 * Grab an exclusive lock on the target table, which we will NOT release
	 * until end of transaction.  (In the simple case where we are directly
	 * dropping this column, AlterTableDropColumn already did this ... but
	 * when cascading from a drop of some other object, we may not have any
	 * lock.)
	 */
	rel = relation_open(relid, AccessExclusiveLock);

	attr_rel = heap_open(AttributeRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), attr_rel);

	tuple = caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_attribute "
				" WHERE attrelid = :1 "
				" AND attnum = :2 "
				" FOR UPDATE ",
				ObjectIdGetDatum(relid),
				Int16GetDatum(attnum)));

	if (!HeapTupleIsValid(tuple))		/* shouldn't happen */
		elog(ERROR, "cache lookup failed for attribute %d of relation %u",
			 attnum, relid);
	attStruct = (Form_pg_attribute) GETSTRUCT(tuple);

	if (attnum < 0)
	{
		/* System attribute (probably OID) ... just delete the row */

		caql_delete_current(pcqCtx);
	}
	else
	{
		/* Dropping user attributes is lots harder */

		/* Mark the attribute as dropped */
		attStruct->attisdropped = true;

		/*
		 * Set the type OID to invalid.  A dropped attribute's type link
		 * cannot be relied on (once the attribute is dropped, the type might
		 * be too). Fortunately we do not need the type row --- the only
		 * really essential information is the type's typlen and typalign,
		 * which are preserved in the attribute's attlen and attalign.  We set
		 * atttypid to zero here as a means of catching code that incorrectly
		 * expects it to be valid.
		 */
		attStruct->atttypid = InvalidOid;

		/* Remove any NOT NULL constraint the column may have */
		attStruct->attnotnull = false;

		/* We don't want to keep stats for it anymore */
		attStruct->attstattarget = 0;

		/*
		 * Change the column name to something that isn't likely to conflict
		 */
		snprintf(newattname, sizeof(newattname),
				 "........pg.dropped.%d........", attnum);
		namestrcpy(&(attStruct->attname), newattname);

		caql_update_current(pcqCtx, tuple);

	}

	/*
	 * Because updating the pg_attribute row will trigger a relcache flush for
	 * the target relation, we need not do anything else to notify other
	 * backends of the change.
	 */

	heap_close(attr_rel, RowExclusiveLock);

	if (attnum > 0)
		RemoveStatistics(relid, attnum);

	relation_close(rel, NoLock);
}

/*
 *		RemoveAttrDefault
 *
 * If the specified relation/attribute has a default, remove it.
 * (If no default, raise error if complain is true, else return quietly.)
 */
void
RemoveAttrDefault(Oid relid, AttrNumber attnum,
				  DropBehavior behavior, bool complain)
{
	Relation	attrdef_rel;
	cqContext  *pcqCtx;
	cqContext	cqc;
	HeapTuple	tuple;
	bool		found = false;

	attrdef_rel = heap_open(AttrDefaultRelationId, RowExclusiveLock);

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), attrdef_rel),
			cql("SELECT * FROM pg_attrdef "
				" WHERE adrelid = :1 "
				" AND adnum = :2 "
				" FOR UPDATE ",
				ObjectIdGetDatum(relid),
				Int16GetDatum(attnum)));

	/* XXX XXX: SELECT oid ... due to unique index ? */
	/* There should be at most one matching tuple, but we loop anyway */
	while (HeapTupleIsValid(tuple = caql_getnext(pcqCtx)))
	{
		ObjectAddress object;

		object.classId = AttrDefaultRelationId;
		object.objectId = HeapTupleGetOid(tuple);
		object.objectSubId = 0;

		performDeletion(&object, behavior);
		
		found = true;
	}

	caql_endscan(pcqCtx);
	heap_close(attrdef_rel, RowExclusiveLock);

	if (complain && !found)
		elog(ERROR, "could not find attrdef tuple for relation %u attnum %d",
			 relid, attnum);
}

/*
 *		RemoveAttrDefaultById
 *
 * Remove a pg_attrdef entry specified by OID.	This is the guts of
 * attribute-default removal.  Note it should be called via performDeletion,
 * not directly.
 */
void
RemoveAttrDefaultById(Oid attrdefId)
{
	Relation	attrdef_rel;
	Relation	attr_rel;
	Relation	myrel;
	cqContext  *adrcqCtx;
	cqContext  *attcqCtx;
	cqContext	cqc;
	cqContext	cqc2;
	HeapTuple	tuple;
	Oid			myrelid;
	AttrNumber	myattnum;

	/* Grab an appropriate lock on the pg_attrdef relation */
	attrdef_rel = heap_open(AttrDefaultRelationId, RowExclusiveLock);

	/* Find the pg_attrdef tuple */

	adrcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), attrdef_rel),
			cql("SELECT * FROM pg_attrdef "
				" WHERE oid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(attrdefId)));

	tuple = caql_getnext(adrcqCtx);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find tuple for attrdef %u", attrdefId);

	myrelid = ((Form_pg_attrdef) GETSTRUCT(tuple))->adrelid;
	myattnum = ((Form_pg_attrdef) GETSTRUCT(tuple))->adnum;

	/* Get an exclusive lock on the relation owning the attribute */
	myrel = relation_open(myrelid, AccessExclusiveLock);

	/* Now we can delete the pg_attrdef row */
	caql_delete_current(adrcqCtx);

	caql_endscan(adrcqCtx);
	heap_close(attrdef_rel, RowExclusiveLock);

	/* Fix the pg_attribute row */
	attr_rel = heap_open(AttributeRelationId, RowExclusiveLock);

	attcqCtx = caql_addrel(cqclr(&cqc2), attr_rel);

	tuple = caql_getfirst(
			attcqCtx,
			cql("SELECT * FROM pg_attribute "
				" WHERE attrelid = :1 "
				" AND attnum = :2 "
				" FOR UPDATE ",
				ObjectIdGetDatum(myrelid),
				Int16GetDatum(myattnum)));

	if (!HeapTupleIsValid(tuple))		/* shouldn't happen */
		elog(ERROR, "cache lookup failed for attribute %d of relation %u",
			 myattnum, myrelid);

	((Form_pg_attribute) GETSTRUCT(tuple))->atthasdef = false;

	caql_update_current(attcqCtx, tuple);
	/* and Update indexes (implicit) */

	/*
	 * Our update of the pg_attribute row will force a relcache rebuild, so
	 * there's nothing else to do here.
	 */
	heap_close(attr_rel, RowExclusiveLock);

	/* Keep lock on attribute's rel until end of xact */
	relation_close(myrel, NoLock);
}

static void
remove_gp_relation_node_and_schedule_drop(
	Relation	rel)
{
	PersistentFileSysRelStorageMgr relStorageMgr;
	
	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
			 "remove_gp_relation_node_and_schedule_drop: dropping relation '%s', relation id %u '%s', relfilenode %u",
			 rel->rd_rel->relname.data,
			 rel->rd_id,
			 relpath(rel->rd_node),
			 rel->rd_rel->relfilenode);

	relStorageMgr = ((RelationIsAoRows(rel) || RelationIsParquet(rel)) ?
													PersistentFileSysRelStorageMgr_AppendOnly:
													PersistentFileSysRelStorageMgr_BufferPool);

	if (relStorageMgr == PersistentFileSysRelStorageMgr_BufferPool)
	{
		MirroredFileSysObj_ScheduleDropBufferPoolRel(rel);

		DeleteGpRelfileNodeTuple(
								rel,
								/* segmentFileNum */ 0);
		
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
				 "remove_gp_relation_node_and_schedule_drop: For Buffer Pool managed relation '%s' persistent TID %s and serial number " INT64_FORMAT " for DROP",
				 relpath(rel->rd_node),
				 ItemPointerToString(&rel->rd_relationnodeinfo.persistentTid),
				 rel->rd_relationnodeinfo.persistentSerialNum);
	}
	else
	{
		Relation relNodeRelation;
		GpRelfileNodeScan	gpRelfileNodeScan;
		HeapTuple tuple;
		int32 segmentFileNum;
		ItemPointerData persistentTid;
		int64 persistentSerialNum;
		
		relNodeRelation = heap_open(GpRelfileNodeRelationId, RowExclusiveLock);

		GpRelfileNodeBeginScan(
						relNodeRelation,
						rel->rd_id,
						rel->rd_rel->relfilenode,
						&gpRelfileNodeScan);
		
		while ((tuple = GpRelfileNodeGetNext(
								&gpRelfileNodeScan,
								&segmentFileNum,
								&persistentTid,
								&persistentSerialNum)))
		{
			if (Debug_persistent_print)
				elog(Persistent_DebugPrintLevel(), 
					 "remove_gp_relation_node_and_schedule_drop: For Append-Only relation %u relfilenode %u scanned segment file #%d, serial number " INT64_FORMAT " at TID %s for DROP",
					 rel->rd_id,
					 rel->rd_rel->relfilenode,
					 segmentFileNum,
					 persistentSerialNum,
					 ItemPointerToString(&persistentTid));
			
			simple_heap_delete(relNodeRelation, &tuple->t_self);
			
			MirroredFileSysObj_ScheduleDropAppendOnlyFile(
											&rel->rd_node,
											segmentFileNum,
											rel->rd_rel->relname.data,
											&persistentTid,
											persistentSerialNum);
		}
		
		GpRelfileNodeEndScan(&gpRelfileNodeScan);
		
		heap_close(relNodeRelation, RowExclusiveLock);

		/*
		 * Now schedule the relation directory removal.
		 *
		 * TODO: Add some logic to search the TID and serial number
		 * of the tuple associated with the relation in the persistent
		 * table.
		 */

		MirroredFileSysObj_ScheduleDropRelationDir(&rel->rd_node,
												is_tablespace_shared(rel->rd_node.spcNode));
	}
}

/*
 * heap_drop_with_catalog	- removes specified relation from catalogs
 *
 * Note that this routine is not responsible for dropping objects that are
 * linked to the pg_class entry via dependencies (for example, indexes and
 * constraints).  Those are deleted by the dependency-tracing logic in
 * dependency.c before control gets here.  In general, therefore, this routine
 * should never be called directly; go through performDeletion() instead.
 */
void
heap_drop_with_catalog(Oid relid)
{
	Relation	rel;
	const struct GpPolicy *policy;
	bool		removePolicy = false;
	bool		is_part_child = false;
	bool		is_appendonly_rel;
	bool		is_external_rel;
	bool		is_foreign_rel;
	char		relkind;

	/*
	 * Open and lock the relation.
	 */
	rel = relation_open(relid, AccessExclusiveLock);

	relkind = rel->rd_rel->relkind;

	is_appendonly_rel = (RelationIsAoRows(rel) || RelationIsParquet(rel));
	is_external_rel = RelationIsExternal(rel);
	is_foreign_rel = RelationIsForeign(rel);

	/*
 	 * Get the distribution policy and figure out if it is to be removed.
 	 */
	policy = rel->rd_cdbpolicy;
	if (policy &&
		policy->ptype == POLICYTYPE_PARTITIONED &&
		Gp_role == GP_ROLE_DISPATCH &&
		relkind == RELKIND_RELATION)
		removePolicy = true;

	/*
	 * Schedule unlinking of the relation's physical file at commit.
	 */
	if (relkind != RELKIND_VIEW &&
		relkind != RELKIND_COMPOSITE_TYPE &&
		!RelationIsExternal(rel))
	{
		remove_gp_relation_node_and_schedule_drop(rel);
	}

	/*
	 * Close relcache entry, but *keep* AccessExclusiveLock (unless this is
	 * a child partition) on the relation until transaction commit.  This
	 * ensures no one else will try to do something with the doomed relation.
	 */
	is_part_child = !rel_needs_long_lock(RelationGetRelid(rel));
	if (is_part_child)
		relation_close(rel, AccessExclusiveLock);
	else
		relation_close(rel, NoLock);

	/*
	 * Forget any ON COMMIT action for the rel
	 */
	remove_on_commit_action(relid);

	/*
	 * Flush the relation from the relcache.  We want to do this before
	 * starting to remove catalog entries, just to be certain that no relcache
	 * entry rebuild will happen partway through.  (That should not really
	 * matter, since we don't do CommandCounterIncrement here, but let's be
	 * safe.)
	 */
	RelationForgetRelation(relid);

	/*
	 * remove inheritance information
	 */
	RelationRemoveInheritance(relid);

	/*
	 * remove partitioning configuration
	 */
	RemovePartitioning(relid);

	/*
	 * delete statistics
	 */
	RemoveStatistics(relid, 0);

	/*
	 * delete attribute tuples
	 */
	DeleteAttributeTuples(relid);

	/*
	 * delete relation tuple
	 */
	DeleteRelationTuple(relid);

	/*
	 * append-only table? delete the corresponding pg_appendonly tuple
	 */
	if(is_appendonly_rel)
		RemoveAppendonlyEntry(relid);

	/*
	 * External table? If so, delete the pg_exttable tuple.
	 */
	if (is_external_rel)
		RemoveExtTableEntry(relid);

	if (is_foreign_rel)
		RemoveForeignTableEntry(relid);
	
	/*
 	 * delete distribution policy if present
 	 */
	if (removePolicy)
		GpPolicyRemove(relid);

	/*
	 * Attribute encoding
	 */
	if (relkind == RELKIND_RELATION)
		RemoveAttributeEncodingsByRelid(relid);

	/* MPP-6929: metadata tracking */
	MetaTrackDropObject(RelationRelationId,
						relid);

}


/*
 * Store a default expression for column attnum of relation rel.
 * The expression must be presented as a nodeToString() string.
 */
void
StoreAttrDefault(Relation rel, AttrNumber attnum, char *adbin)
{
	Node	   *expr;
	char	   *adsrc;
	Relation	adrel;
	HeapTuple	tuple;
	Datum		values[4];
	static bool nulls[4] = {false, false, false, false};
	Relation	attrrel;
	HeapTuple	atttup;
	Form_pg_attribute attStruct;
	Oid			attrdefOid;
	ObjectAddress colobject,
				defobject;
	cqContext  *adrcqCtx;
	cqContext  *attcqCtx;
	cqContext	cqc;
	cqContext	cqc2;

	/*
	 * Need to construct source equivalent of given node-string.
	 */
	expr = stringToNode(adbin);

	/*
	 * Also deparse it to form the mostly-obsolete adsrc field.
	 */
	adsrc = deparse_expression(expr,
							deparse_context_for(RelationGetRelationName(rel),
												RelationGetRelid(rel)),
							   false, false);

	/*
	 * Make the pg_attrdef entry.
	 */
	values[Anum_pg_attrdef_adrelid - 1] = RelationGetRelid(rel);
	values[Anum_pg_attrdef_adnum - 1] = attnum;
	values[Anum_pg_attrdef_adbin - 1] = CStringGetTextDatum(adbin);
	values[Anum_pg_attrdef_adsrc - 1] = CStringGetTextDatum(adsrc);

	adrel = heap_open(AttrDefaultRelationId, RowExclusiveLock);

	adrcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), adrel),
			cql("INSERT INTO pg_attrdef ", 
				NULL));

	if (Debug_check_for_invalid_persistent_tid)
	{	
		elog(LOG, 
			 "StoreAttrDefault[1] relation %u/%u/%u '%s', isPresent %s, serial number " INT64_FORMAT ", TID %s",
			 adrel->rd_node.spcNode,
			 adrel->rd_node.dbNode,
			 adrel->rd_node.relNode,
			 NameStr(adrel->rd_rel->relname),
			 (adrel->rd_relationnodeinfo.isPresent ? "true" : "false"),
			 adrel->rd_relationnodeinfo.persistentSerialNum,
			 ItemPointerToString(&adrel->rd_relationnodeinfo.persistentTid));
	}

	// Fetch gp_persistent_relation_node information that will be added to XLOG record.
	RelationFetchGpRelationNodeForXLog(adrel);

	if (Debug_check_for_invalid_persistent_tid)
	{	
		elog(LOG, 
			 "StoreAttrDefault[2] relation %u/%u/%u '%s', isPresent %s, serial number " INT64_FORMAT ", TID %s",
			 adrel->rd_node.spcNode,
			 adrel->rd_node.dbNode,
			 adrel->rd_node.relNode,
			 NameStr(adrel->rd_rel->relname),
			 (adrel->rd_relationnodeinfo.isPresent ? "true" : "false"),
			 adrel->rd_relationnodeinfo.persistentSerialNum,
			 ItemPointerToString(&adrel->rd_relationnodeinfo.persistentTid));
	}

	tuple = caql_form_tuple(adrcqCtx, values, nulls);

	attrdefOid = caql_insert(adrcqCtx, tuple);
	/* and Update indexes (implicit) */

	defobject.classId = AttrDefaultRelationId;
	defobject.objectId = attrdefOid;
	defobject.objectSubId = 0;

	caql_endscan(adrcqCtx);
	heap_close(adrel, RowExclusiveLock);

	/* now can free some of the stuff allocated above */
	pfree(DatumGetPointer(values[Anum_pg_attrdef_adbin - 1]));
	pfree(DatumGetPointer(values[Anum_pg_attrdef_adsrc - 1]));
	heap_freetuple(tuple);
	pfree(adsrc);

	/*
	 * Update the pg_attribute entry for the column to show that a default
	 * exists.
	 */
	attrrel = heap_open(AttributeRelationId, RowExclusiveLock);

	attcqCtx = caql_addrel(cqclr(&cqc2), attrrel);

	atttup = caql_getfirst(
			attcqCtx,
			cql("SELECT * FROM pg_attribute "
				" WHERE attrelid = :1 "
				" AND attnum = :2 "
				" FOR UPDATE ",
				ObjectIdGetDatum(RelationGetRelid(rel)),
				Int16GetDatum(attnum)));

	if (!HeapTupleIsValid(atttup))
		elog(ERROR, "cache lookup failed for attribute %d of relation %u",
			 attnum, RelationGetRelid(rel));
	attStruct = (Form_pg_attribute) GETSTRUCT(atttup);
	if (!attStruct->atthasdef)
	{
		attStruct->atthasdef = true;
		caql_update_current(attcqCtx, atttup);
		/* and Update indexes (implicit) */
	}
	heap_close(attrrel, RowExclusiveLock);
	heap_freetuple(atttup);

	/*
	 * Make a dependency so that the pg_attrdef entry goes away if the column
	 * (or whole table) is deleted.
	 */
	colobject.classId = RelationRelationId;
	colobject.objectId = RelationGetRelid(rel);
	colobject.objectSubId = attnum;

	recordDependencyOn(&defobject, &colobject, DEPENDENCY_AUTO);

	/*
	 * Record dependencies on objects used in the expression, too.
	 */
	recordDependencyOnExpr(&defobject, expr, NIL, DEPENDENCY_NORMAL);
}

/*
 * Store a check-constraint expression for the given relation.
 * The expression must be presented as a nodeToString() string.
 *
 * Caller is responsible for updating the count of constraints
 * in the pg_class entry for the relation.
 *
 * Return OID of the newly created constraint entry.
 */
static Oid
StoreRelCheck(Relation rel, char *ccname, char *ccbin, Oid conOid)
{
	Node	   *expr;
	char	   *ccsrc;
	List	   *varList;
	int			keycount;
	int16	   *attNos;

	/*
	 * Convert condition to an expression tree.
	 */
	expr = stringToNode(ccbin);

	/*
	 * deparse it
	 */
	ccsrc = deparse_expression(expr,
							deparse_context_for(RelationGetRelationName(rel),
												RelationGetRelid(rel)),
							   false, false);

	/*
	 * Find columns of rel that are used in ccbin
	 *
	 * NB: pull_var_clause is okay here only because we don't allow subselects
	 * in check constraints; it would fail to examine the contents of
	 * subselects.
	 */
	varList = pull_var_clause(expr, false);
	keycount = list_length(varList);

	if (keycount > 0)
	{
		ListCell   *vl;
		int			i = 0;

		attNos = (int16 *) palloc(keycount * sizeof(int16));
		foreach(vl, varList)
		{
			Var		   *var = (Var *) lfirst(vl);
			int			j;

			for (j = 0; j < i; j++)
				if (attNos[j] == var->varattno)
					break;
			if (j == i)
				attNos[i++] = var->varattno;
		}
		keycount = i;
	}
	else
		attNos = NULL;

	/*
	 * Create the Check Constraint
	 */
	conOid = CreateConstraintEntry(ccname,		/* Constraint Name */
								   conOid,		/* Constraint Oid */
								   RelationGetNamespace(rel),	/* namespace */
								   CONSTRAINT_CHECK,		/* Constraint Type */
								   false,	/* Is Deferrable */
								   false,	/* Is Deferred */
								   RelationGetRelid(rel),		/* relation */
								   attNos,		/* attrs in the constraint */
								   keycount,		/* # attrs in the constraint */
								   InvalidOid,	/* not a domain constraint */
								   InvalidOid,	/* Foreign key fields */
								   NULL,
								   0,
								   ' ',
								   ' ',
								   ' ',
								   InvalidOid,	/* no associated index */
								   expr, /* Tree form check constraint */
								   ccbin,	/* Binary form check constraint */
								   ccsrc);		/* Source form check constraint */

	pfree(ccsrc);
	return conOid;
}

/*
 * Store defaults and constraints passed in via the tuple constraint struct.
 *
 * NOTE: only pre-cooked expressions will be passed this way, which is to
 * say expressions inherited from an existing relation.  Newly parsed
 * expressions can be added later, by direct calls to StoreAttrDefault
 * and StoreRelCheck (see AddRelationRawConstraints()).
 */
static void
StoreConstraints(Relation rel, TupleDesc tupdesc)
{
	TupleConstr *constr = tupdesc->constr;
	int			i;

	if (!constr)
		return;					/* nothing to do */

	/*
	 * Deparsing of constraint expressions will fail unless the just-created
	 * pg_attribute tuples for this relation are made visible.	So, bump the
	 * command counter.  CAUTION: this will cause a relcache entry rebuild.
	 */
	CommandCounterIncrement();

	for (i = 0; i < constr->num_defval; i++)
		StoreAttrDefault(rel, constr->defval[i].adnum,
						 constr->defval[i].adbin);
}


/*
 * AddRelationConstraints
 *
 * Add both raw (not-yet-transformed) and cooked column default expressions and/or
 * constraint check expressions to an existing relation.  This is defined to do both
 * for efficiency in DefineRelation, but of course you can do just one or
 * the other by passing empty lists.
 *
 * rel: relation to be modified
 * rawColDefaults: list of RawColumnDefault structures
 * constraints: list of Constraint nodes
 *
 * All entries in rawColDefaults will be processed.  Entries in rawConstraints
 * will be processed only if they are CONSTR_CHECK type and contain a "raw"
 * expression.
 *
 * Returns a list of CookedConstraint nodes that shows the cooked form of
 * the default and constraint expressions added to the relation.
 *
 * NB: caller should have opened rel with AccessExclusiveLock, and should
 * hold that lock till end of transaction.	Also, we assume the caller has
 * done a CommandCounterIncrement if necessary to make the relation's catalog
 * tuples visible.
 */
List *
AddRelationConstraints(Relation rel,
						  List *rawColDefaults,
						  List *constraints)
{
	List	   *cookedConstraints = NIL;
	TupleDesc	tupleDesc;
	TupleConstr *oldconstr;
	int			numoldchecks;
	ParseState *pstate;
	RangeTblEntry *rte;
	int			numchecks;
	List	   *checknames;
	ListCell   *cell;
	Node	   *expr;
	CookedConstraint *cooked;

	/*
	 * Get info about existing constraints.
	 */
	tupleDesc = RelationGetDescr(rel);
	oldconstr = tupleDesc->constr;
	if (oldconstr)
		numoldchecks = oldconstr->num_check;
	else
		numoldchecks = 0;

	/*
	 * Create a dummy ParseState and insert the target relation as its sole
	 * rangetable entry.  We need a ParseState for transformExpr.
	 */
	pstate = make_parsestate(NULL);
	rte = addRangeTableEntryForRelation(pstate,
										rel,
										NULL,
										false,
										true);
	addRTEtoQuery(pstate, rte, true, true, true);

	/*
	 * Process column default expressions.
	 */
	foreach(cell, rawColDefaults)
	{
		RawColumnDefault *colDef = (RawColumnDefault *) lfirst(cell);
		Form_pg_attribute atp = rel->rd_att->attrs[colDef->attnum - 1];

		expr = cookDefault(pstate, colDef->raw_default,
						   atp->atttypid, atp->atttypmod,
						   NameStr(atp->attname));

		StoreAttrDefault(rel, colDef->attnum, nodeToString(expr));

		cooked = (CookedConstraint *) palloc(sizeof(CookedConstraint));
		cooked->contype = CONSTR_DEFAULT;
		cooked->name = NULL;
		cooked->attnum = colDef->attnum;
		cooked->expr = expr;
		cookedConstraints = lappend(cookedConstraints, cooked);
	}

	/*
	 * Process constraint expressions.
	 */
	numchecks = numoldchecks;
	checknames = NIL;
	foreach(cell, constraints)
	{
		Constraint *cdef = (Constraint *) lfirst(cell);
		char	   *ccname;

		if (cdef->contype != CONSTR_CHECK)
			continue;

		/*
		 * Transform raw parsetree to executable expression, and verify
		 * it's valid as a CHECK constraint
		 */
		if (cdef->raw_expr != NULL)
		{
			Insist(cdef->cooked_expr == NULL);
			expr = cookConstraint(pstate, cdef->raw_expr,
								  RelationGetRelationName(rel));
		}
		/*
		 * Here, we assume the parser will only pass us valid CHECK
		 * expressions, so we do no particular checking.
		 */
		else
		{
			Insist(cdef->cooked_expr != NULL);
			expr = stringToNode(cdef->cooked_expr);
		}

		/*
		 * Check name uniqueness, or generate a name if none was given.
		 */
		if (cdef->name != NULL)
		{
			ListCell   *cell2;

			ccname = cdef->name;
			/* Check against pre-existing constraints */
			if (ConstraintNameIsUsed(CONSTRAINT_RELATION,
									 RelationGetRelid(rel),
									 RelationGetNamespace(rel),
									 ccname))
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_OBJECT),
						 errmsg("constraint \"%s\" for relation \"%s\" already exists",
								ccname, RelationGetRelationName(rel)),
								errOmitLocation(true)));

			/* Check against other new constraints */
			/* Needed because we don't do CommandCounterIncrement in loop */
			foreach(cell2, checknames)
			{
				if (strcmp((char *) lfirst(cell2), ccname) == 0)
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_OBJECT),
							 errmsg("check constraint \"%s\" already exists",
									ccname),
									errOmitLocation(true)));
			}
		}
		else
		{
			/*
			 * When generating a name, we want to create "tab_col_check" for a
			 * column constraint and "tab_check" for a table constraint.  We
			 * no longer have any info about the syntactic positioning of the
			 * constraint phrase, so we approximate this by seeing whether the
			 * expression references more than one column.	(If the user
			 * played by the rules, the result is the same...)
			 *
			 * Note: pull_var_clause() doesn't descend into sublinks, but we
			 * eliminated those above; and anyway this only needs to be an
			 * approximate answer.
			 */
			List	   *vars;
			char	   *colname;

			vars = pull_var_clause(expr, false);

			/* eliminate duplicates */
			vars = list_union(NIL, vars);

			if (list_length(vars) == 1)
				colname = get_attname(RelationGetRelid(rel),
									  ((Var *) linitial(vars))->varattno);
			else
				colname = NULL;

			ccname = ChooseConstraintName(RelationGetRelationName(rel),
										  colname,
										  "check",
										  RelationGetNamespace(rel),
										  checknames);
		}

		/* save name for future checks */
		checknames = lappend(checknames, ccname);

		/*
		 * OK, store it.
		 */
		cdef->conoid = StoreRelCheck(rel, ccname, nodeToString(expr), cdef->conoid);

		numchecks++;

		cooked = (CookedConstraint *) palloc(sizeof(CookedConstraint));
		cooked->contype = CONSTR_CHECK;
		cooked->name = ccname;
		cooked->attnum = 0;
		cooked->expr = expr;
		cookedConstraints = lappend(cookedConstraints, cooked);
	}

	/* Cleanup the parse state */
	free_parsestate(&pstate);

	/*
	 * Update the count of constraints in the relation's pg_class tuple. We do
	 * this even if there was no change, in order to ensure that an SI update
	 * message is sent out for the pg_class tuple, which will force other
	 * backends to rebuild their relcache entries for the rel. (This is
	 * critical if we added defaults but not constraints.)
	 */
	SetRelationNumChecks(rel, numchecks);

	return cookedConstraints;
}

/*
 * Transform raw parsetree to executable expression.
 */
static Node*
cookConstraint (ParseState 	*pstate,
				Node 		*raw_constraint,
				char		*relname)
{
	Node	*expr;

	/* Transform raw parsetree to executable expression. */
	expr = transformExpr(pstate, raw_constraint);

	/* Make sure it yields a boolean result. */
	expr = coerce_to_boolean(pstate, expr, "CHECK");

	/* Make sure no outside relations are referred to. */
	if (list_length(pstate->p_rtable) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
		errmsg("only table \"%s\" can be referenced in check constraint",
			   relname), errOmitLocation(true)));
	/*
	 * No subplans or aggregates, either...
	 */
	if (pstate->p_hasSubLinks)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot use subquery in check constraint"),
						   errOmitLocation(true)));
	if (pstate->p_hasAggs)
		ereport(ERROR,
				(errcode(ERRCODE_GROUPING_ERROR),
		   errmsg("cannot use aggregate function in check constraint"),
				   errOmitLocation(true)));
	if (pstate->p_hasWindFuncs)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
		   errmsg("cannot use window function in check constraint"),
		   errOmitLocation(true)));

	return expr;
}


/*
 * Update the count of constraints in the relation's pg_class tuple.
 *
 * Caller had better hold exclusive lock on the relation.
 *
 * An important side effect is that a SI update message will be sent out for
 * the pg_class tuple, which will force other backends to rebuild their
 * relcache entries for the rel.  Also, this backend will rebuild its
 * own relcache entry at the next CommandCounterIncrement.
 */
void
SetRelationNumChecks(Relation rel, int numchecks)
{
	Relation	relrel;
	HeapTuple	reltup;
	Form_pg_class relStruct;
	cqContext	cqc;
	cqContext  *pcqCtx;

	relrel = heap_open(RelationRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), relrel);

	reltup = caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(RelationGetRelid(rel))));

	if (!HeapTupleIsValid(reltup))
		elog(ERROR, "cache lookup failed for relation %u",
			 RelationGetRelid(rel));
	relStruct = (Form_pg_class) GETSTRUCT(reltup);

	if (relStruct->relchecks != numchecks)
	{
		relStruct->relchecks = numchecks;

		caql_update_current(pcqCtx, reltup);
		/* and Update indexes (implicit) */
	}
	else
	{
		/* Skip the disk update, but force relcache inval anyway */
		CacheInvalidateRelcache(rel);
	}

	heap_freetuple(reltup);
	heap_close(relrel, RowExclusiveLock);
}

/*
 * Take a raw default and convert it to a cooked format ready for
 * storage.
 *
 * Parse state should be set up to recognize any vars that might appear
 * in the expression.  (Even though we plan to reject vars, it's more
 * user-friendly to give the correct error message than "unknown var".)
 *
 * If atttypid is not InvalidOid, coerce the expression to the specified
 * type (and typmod atttypmod).   attname is only needed in this case:
 * it is used in the error message, if any.
 */
Node *
cookDefault(ParseState *pstate,
			Node *raw_default,
			Oid atttypid,
			int32 atttypmod,
			char *attname)
{
	Node	   *expr;

	Assert(raw_default != NULL);

	/*
	 * Transform raw parsetree to executable expression.
	 */
	expr = transformExpr(pstate, raw_default);

	/*
	 * Make sure default expr does not refer to any vars.
	 */
	if (contain_var_clause(expr))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
			  errmsg("cannot use column references in default expression")));

	/*
	 * It can't return a set either.
	 */
	if (expression_returns_set(expr))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("default expression must not return a set"),
						   errOmitLocation(true)));

	/*
	 * No subplans or aggregates, either...
	 */
	if (pstate->p_hasSubLinks)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot use subquery in default expression"),
						   errOmitLocation(true)));
	if (pstate->p_hasAggs)
		ereport(ERROR,
				(errcode(ERRCODE_GROUPING_ERROR),
			 errmsg("cannot use aggregate function in default expression"),
					   errOmitLocation(true)));
	if (pstate->p_hasWindFuncs)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("cannot use window function in default expression"),
					   errOmitLocation(true)));
	/*
	 * Coerce the expression to the correct type and typmod, if given. This
	 * should match the parser's processing of non-defaulted expressions ---
	 * see transformAssignedExpr().
	 */
	if (OidIsValid(atttypid))
	{
		Oid			type_id = exprType(expr);

		expr = coerce_to_target_type(pstate, expr, type_id,
									 atttypid, atttypmod,
									 COERCION_ASSIGNMENT,
									 COERCE_IMPLICIT_CAST,
									 -1);
		if (expr == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("column \"%s\" is of type %s"
							" but default expression is of type %s",
							attname,
							format_type_be(atttypid),
							format_type_be(type_id)),
			   errhint("You will need to rewrite or cast the expression."),
					   errOmitLocation(true)));
	}

	return expr;
}


/*
 * Removes all constraints on a relation that match the given name.
 *
 * It is the responsibility of the calling function to acquire a suitable
 * lock on the relation.
 *
 * Returns: The number of constraints removed.
 */
int
RemoveRelConstraints(Relation rel, const char *constrName,
					 DropBehavior behavior)
{
	int			ndeleted = 0;
	cqContext  *pcqCtx;
	HeapTuple	contup;

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_constraint "
				" WHERE conrelid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(RelationGetRelid(rel))));

	/*
	 * Scan over the result set, removing any matching entries.
	 */
	while (HeapTupleIsValid(contup = caql_getnext(pcqCtx)))
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(contup);

		if (strcmp(NameStr(con->conname), constrName) == 0)
		{
			ObjectAddress conobj;

			conobj.classId = ConstraintRelationId;
			conobj.objectId = HeapTupleGetOid(contup);
			conobj.objectSubId = 0;

			performDeletion(&conobj, behavior);

			ndeleted++;
		}
	}

	/* Clean up after the scan */
	caql_endscan(pcqCtx);

	return ndeleted;
}


/*
 * RemoveStatistics --- remove entries in pg_statistic for a rel or column
 *
 * If attnum is zero, remove all entries for rel; else remove only the one
 * for that column.
 */
void
RemoveStatistics(Oid relid, AttrNumber attnum)
{
	int numDel = 0;

	if (attnum == 0)
	{
		numDel = caql_getcount(
				NULL,
				cql("DELETE FROM pg_statistic "
					" WHERE starelid = :1 ",
					ObjectIdGetDatum(relid)));
	}
	else
	{
		numDel = caql_getcount(
				NULL,
				cql("DELETE FROM pg_statistic "
					" WHERE starelid = :1 "
					" AND staattnum = :2 ",
					ObjectIdGetDatum(relid),
					Int16GetDatum(attnum)));		 
	}
}


/*
 * RelationTruncateIndexes - truncate all indexes associated
 * with the heap relation to zero tuples.
 *
 * The routine will truncate and then reconstruct the indexes on
 * the specified relation.	Caller must hold exclusive lock on rel.
 */
static void
RelationTruncateIndexes(Relation heapRelation)
{
	ListCell   *indlist;

	/* Ask the relcache to produce a list of the indexes of the rel */
	foreach(indlist, RelationGetIndexList(heapRelation))
	{
		Oid			indexId = lfirst_oid(indlist);
		Relation	currentIndex;
		IndexInfo  *indexInfo;

		/* Open the index relation; use exclusive lock, just to be sure */
		currentIndex = index_open(indexId, AccessExclusiveLock);

		/* Fetch info needed for index_build */
		indexInfo = BuildIndexInfo(currentIndex);

		/* Now truncate the actual file (and discard buffers) */
		RelationTruncate(
					currentIndex, 
					0,
					/* markPersistentAsPhysicallyTruncated */ true);

		/* Initialize the index and rebuild */
		/* Note: we do not need to re-establish pkey setting */
		index_build(heapRelation, currentIndex, indexInfo, false);

		/* We're done with this index */
		index_close(currentIndex, NoLock);
	}
}

/*
 *	 heap_truncate
 *
 *	 This routine deletes all data within all the specified relations.
 *
 * This is not transaction-safe!  There is another, transaction-safe
 * implementation in commands/tablecmds.c.	We now use this only for
 * ON COMMIT truncation of temporary tables, where it doesn't matter.
 */
void
heap_truncate(List *relids)
{
	List	   *relations = NIL;
	ListCell   *cell;

	/* Open relations for processing, and grab exclusive access on each */
	foreach(cell, relids)
	{
		Oid			rid = lfirst_oid(cell);
		Relation	rel, trel;
		Oid			toastrelid;
		Oid			aosegrelid;
		Oid         aoblkdirrelid;
		AppendOnlyEntry *aoEntry = NULL;

		rel = heap_open(rid, AccessExclusiveLock);
		relations = lappend(relations, rel);

		/* If there is a toast table, add it to the list too */
		toastrelid = rel->rd_rel->reltoastrelid;
		if (OidIsValid(toastrelid))
		{
			trel = heap_open(toastrelid, AccessExclusiveLock);
			relations = lappend(relations, trel);
		}

		/*
		 * CONCERN: Not clear this EVER makes sense for Append-Only.
		 */
		if (RelationIsAoRows(rel) || RelationIsParquet(rel))
		{
			aoEntry = GetAppendOnlyEntry(rid, SnapshotNow);

			/* If there is an aoseg table, add it to the list too */
			aosegrelid = aoEntry->segrelid;
			if (OidIsValid(aosegrelid))
			{
				rel = heap_open(aosegrelid, AccessExclusiveLock);
				relations = lappend(relations, rel);
			}

			/* If there is an aoblkdir table, add it to the list too */
			aoblkdirrelid = aoEntry->blkdirrelid;
			if (OidIsValid(aoblkdirrelid))
			{
				rel = heap_open(aoblkdirrelid, AccessExclusiveLock);
				relations = lappend(relations, rel);
			}

			pfree(aoEntry);
		}
	}

	/* Don't allow truncate on tables that are referenced by foreign keys */
	heap_truncate_check_FKs(relations, true);

	/* OK to do it */
	foreach(cell, relations)
	{
		Relation	rel = lfirst(cell);

		/* Truncate the actual file (and discard buffers) */
		RelationTruncate(
					rel, 
					0,
					/* markPersistentAsPhysicallyTruncated */ false);

		/* If this relation has indexes, truncate the indexes too */
		RelationTruncateIndexes(rel);

		/*
		 * Close the relation, but keep exclusive lock on it until commit.
		 */
		heap_close(rel, NoLock);
	}
}

/*
 * heap_truncate_check_FKs
 *		Check for foreign keys referencing a list of relations that
 *		are to be truncated, and raise error if there are any
 *
 * We disallow such FKs (except self-referential ones) since the whole point
 * of TRUNCATE is to not scan the individual rows to be thrown away.
 *
 * This is split out so it can be shared by both implementations of truncate.
 * Caller should already hold a suitable lock on the relations.
 *
 * tempTables is only used to select an appropriate error message.
 */
void
heap_truncate_check_FKs(List *relations, bool tempTables)
{
	List	   *oids = NIL;
	List	   *dependents;
	ListCell   *cell;

	/*
	 * Build a list of OIDs of the interesting relations.
	 *
	 * If a relation has no triggers, then it can neither have FKs nor be
	 * referenced by a FK from another table, so we can ignore it.
	 */
	foreach(cell, relations)
	{
		Relation	rel = lfirst(cell);

		if (rel->rd_rel->reltriggers != 0)
			oids = lappend_oid(oids, RelationGetRelid(rel));
	}

	/*
	 * Fast path: if no relation has triggers, none has FKs either.
	 */
	if (oids == NIL)
		return;

	/*
	 * Otherwise, must scan pg_constraint.	We make one pass with all the
	 * relations considered; if this finds nothing, then all is well.
	 */
	dependents = heap_truncate_find_FKs(oids);
	if (dependents == NIL)
		return;

	/*
	 * Otherwise we repeat the scan once per relation to identify a particular
	 * pair of relations to complain about.  This is pretty slow, but
	 * performance shouldn't matter much in a failure path.  The reason for
	 * doing things this way is to ensure that the message produced is not
	 * dependent on chance row locations within pg_constraint.
	 */
	foreach(cell, oids)
	{
		Oid			relid = lfirst_oid(cell);
		ListCell   *cell2;

		dependents = heap_truncate_find_FKs(list_make1_oid(relid));

		foreach(cell2, dependents)
		{
			Oid			relid2 = lfirst_oid(cell2);

			if (!list_member_oid(oids, relid2))
			{
				char	   *relname = get_rel_name(relid);
				char	   *relname2 = get_rel_name(relid2);

				if (tempTables)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("unsupported ON COMMIT and foreign key combination"),
							 errdetail("Table \"%s\" references \"%s\", but they do not have the same ON COMMIT setting.",
									   relname2, relname),
											   errOmitLocation(true)));
				else
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot truncate a table referenced in a foreign key constraint"),
							 errdetail("Table \"%s\" references \"%s\".",
									   relname2, relname),
						   errhint("Truncate table \"%s\" at the same time, "
								   "or use TRUNCATE ... CASCADE.",
								   relname2),
										   errOmitLocation(true)));
			}
		}
	}
}

/*
 * heap_truncate_find_FKs
 *		Find relations having foreign keys referencing any of the given rels
 *
 * Input and result are both lists of relation OIDs.  The result contains
 * no duplicates, does *not* include any rels that were already in the input
 * list, and is sorted in OID order.  (The last property is enforced mainly
 * to guarantee consistent behavior in the regression tests; we don't want
 * behavior to change depending on chance locations of rows in pg_constraint.)
 *
 * Note: caller should already have appropriate lock on all rels mentioned
 * in relationIds.	Since adding or dropping an FK requires exclusive lock
 * on both rels, this ensures that the answer will be stable.
 */
List *
heap_truncate_find_FKs(List *relationIds)
{
	List	   *result = NIL;
	Relation	fkeyRel;
	SysScanDesc fkeyScan;
	HeapTuple	tuple;

	/*
	 * Must scan pg_constraint.  Right now, it is a seqscan because there is
	 * no available index on confrelid.
	 */
	fkeyRel = heap_open(ConstraintRelationId, AccessShareLock);

	fkeyScan = systable_beginscan(fkeyRel, InvalidOid, false,
								  SnapshotNow, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(fkeyScan)))
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(tuple);

		/* Not a foreign key */
		if (con->contype != CONSTRAINT_FOREIGN)
			continue;

		/* Not referencing one of our list of tables */
		if (!list_member_oid(relationIds, con->confrelid))
			continue;

		/* Add referencer unless already in input or result list */
		if (!list_member_oid(relationIds, con->conrelid))
			result = insert_ordered_unique_oid(result, con->conrelid);
	}

	systable_endscan(fkeyScan);
	heap_close(fkeyRel, AccessShareLock);

	return result;
}

/*
 * insert_ordered_unique_oid
 *		Insert a new Oid into a sorted list of Oids, preserving ordering,
 *		and eliminating duplicates
 *
 * Building the ordered list this way is O(N^2), but with a pretty small
 * constant, so for the number of entries we expect it will probably be
 * faster than trying to apply qsort().  It seems unlikely someone would be
 * trying to truncate a table with thousands of dependent tables ...
 */
static List *
insert_ordered_unique_oid(List *list, Oid datum)
{
	ListCell   *prev;

	/* Does the datum belong at the front? */
	if (list == NIL || datum < linitial_oid(list))
		return lcons_oid(datum, list);
	/* Does it match the first entry? */
	if (datum == linitial_oid(list))
		return list;			/* duplicate, so don't insert */
	/* No, so find the entry it belongs after */
	prev = list_head(list);
	for (;;)
	{
		ListCell   *curr = lnext(prev);

		if (curr == NULL || datum < lfirst_oid(curr))
			break;				/* it belongs after 'prev', before 'curr' */

		if (datum == lfirst_oid(curr))
			return list;		/* duplicate, so don't insert */

		prev = curr;
	}
	/* Insert datum into list after 'prev' */
	lappend_cell_oid(list, prev, datum);
	return list;
}

/*
 * setNewRelfilenodeCommon
 *
 * Replaces relfilenode and updates pg_class / gp_relation_node.
 * If the updating relation is gp_relation_node's index, the caller
 * should rebuild the index by index_build().
 */
static void
setNewRelfilenodeCommon(Relation relation, Oid newrelfilenode)
{
	RelFileNode newrnode;
	Relation	pg_class;
	HeapTuple	tuple;
	Form_pg_class rd_rel;
	bool		isAppendOnly;
	Relation	gp_relation_node;
	bool		is_gp_relation_node_index;
	cqContext	cqc;
	cqContext  *pcqCtx;
	char * relname;

	/*
	 * Find the pg_class tuple for the given relation.	This is not used
	 * during bootstrap, so okay to use heap_update always.
	 */
	pg_class = heap_open(RelationRelationId, RowExclusiveLock);
	gp_relation_node = heap_open(GpRelfileNodeRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), pg_class);

	tuple = caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(RelationGetRelid(relation))));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find tuple for relation %u",
			 RelationGetRelid(relation));
	rd_rel = (Form_pg_class) GETSTRUCT(tuple);

	/* schedule unlinking old relfilenode */
	remove_gp_relation_node_and_schedule_drop(relation);

	/* create another storage file. Is it a little ugly ? */
	/* NOTE: any conflict in relfilenode value will be caught here */
	newrnode = relation->rd_node;
	newrnode.relNode = newrelfilenode;

	isAppendOnly = (relation->rd_rel->relstorage == RELSTORAGE_AOROWS || 
					relation->rd_rel->relstorage == RELSTORAGE_PARQUET);
	
	relname = RelationGetRelationName(relation);

	if (!isAppendOnly)
	{
		SMgrRelation srel;

		PersistentFileSysRelStorageMgr localRelStorageMgr;
		PersistentFileSysRelBufpoolKind relBufpoolKind;
		
		GpPersistentRelfileNode_GetRelfileInfo(
											relation->rd_rel->relkind,
											relation->rd_rel->relstorage,
											relation->rd_rel->relam,
											&localRelStorageMgr,
											&relBufpoolKind);
		Assert(localRelStorageMgr == PersistentFileSysRelStorageMgr_BufferPool);
		
		srel = smgropen(newrnode);
	
		MirroredFileSysObj_TransactionCreateBufferPoolFile(
											srel,
											relBufpoolKind,
											relation->rd_isLocalBuf,
											relname,
											/* doJustInTimeDirCreate */ true,
											/* bufferPoolBulkLoad */ false,
											&relation->rd_relationnodeinfo.persistentTid,
											&relation->rd_relationnodeinfo.persistentSerialNum);
		smgrclose(srel);
	}
	else
	{
		MirroredFileSysObj_TransactionCreateRelationDir(
											&newrnode,
											false,
											&relation->rd_relationnodeinfo.persistentTid,
											&relation->rd_relationnodeinfo.persistentSerialNum);
	}

	if (Debug_check_for_invalid_persistent_tid &&
		!Persistent_BeforePersistenceWork() &&
		PersistentStore_IsZeroTid(&relation->rd_relationnodeinfo.persistentTid))
	{	
		elog(ERROR, 
			 "setNewRelfilenodeCommon has invalid TID (0,0) for relation %u/%u/%u '%s', serial number " INT64_FORMAT,
			 newrnode.spcNode,
			 newrnode.dbNode,
			 newrnode.relNode,
			 NameStr(relation->rd_rel->relname),
			 relation->rd_relationnodeinfo.persistentSerialNum);
	}

	relation->rd_relationnodeinfo.isPresent = true;
	
	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
			 "setNewRelfilenodeCommon: NEW '%s', Append-Only '%s', persistent TID %s and serial number " INT64_FORMAT,
			 relpath(newrnode),
			 (isAppendOnly ? "true" : "false"),
			 ItemPointerToString(&relation->rd_relationnodeinfo.persistentTid),
			 relation->rd_relationnodeinfo.persistentSerialNum);

	/* Update GETSTRUCT fields of the pg_class row */
	rd_rel->relfilenode = newrelfilenode;
	rd_rel->relpages = 0;		/* it's empty until further notice */
	rd_rel->reltuples = 0;

	/*
	 * If the swapping relation is an index of gp_relation_node,
	 * updating itself is bogus; if gp_relation_node has old indexlist,
	 * CatalogUpdateIndexes updates old index file, and is crash-unsafe.
	 * Hence, here we skip it and count on later index_build.
	 * (Or should we add index_build() call after CCI beflow in this case?)
	 */
	is_gp_relation_node_index = relation->rd_index &&
								relation->rd_index->indrelid == GpRelfileNodeRelationId;

	if (!isAppendOnly)
	{
		InsertGpRelfileNodeTuple(
								gp_relation_node,
								relation->rd_id,
								NameStr(relation->rd_rel->relname),
								newrelfilenode,
								/* segmentFileNum */ 0,
								/* updateIndex */ !is_gp_relation_node_index,
								&relation->rd_relationnodeinfo.persistentTid,
								relation->rd_relationnodeinfo.persistentSerialNum);
	}


	caql_update_current(pcqCtx, tuple);
	/* and Update indexes (implicit) */

	heap_freetuple(tuple);

	heap_close(pg_class, RowExclusiveLock);

	heap_close(gp_relation_node, RowExclusiveLock);

	/* Make sure the relfilenode change is visible */
	CommandCounterIncrement();
}

/*
 * setNewRelfilenode		- assign a new relfilenode value to the relation
 *
 * Caller must already hold exclusive lock on the relation.
 */
Oid
setNewRelfilenode(Relation relation)
{
	Oid			newrelfilenode;

	/* Can't change relfilenode for nailed tables (indexes ok though) */
	Assert(!relation->rd_isnailed ||
		   relation->rd_rel->relkind == RELKIND_INDEX);
	/* Can't change for shared tables or indexes */
	Assert(!relation->rd_rel->relisshared);

	/* Allocate a new relfilenode */
	newrelfilenode = GetNewRelFileNode(relation->rd_rel->reltablespace,
									   relation->rd_rel->relisshared,
									   NULL,
									   relstorage_is_ao(relation->rd_rel->relstorage));

		elog(DEBUG1, "setNewRelfilenode called in EXECUTE mode, "
			 "newrelfilenode=%d", newrelfilenode);

	setNewRelfilenodeCommon(relation, newrelfilenode);

	return newrelfilenode;
}

/*
 * Greenplum specific routine.
 *
 * setNewRelfilenodeToOid		- assign a new relfilenode value to the relation
 *
 * Caller must already hold exclusive lock on the relation.
 */
Oid
setNewRelfilenodeToOid(Relation relation, Oid newrelfilenode)
{
	/* Can't change relfilenode for nailed tables (indexes ok though) */
	Assert(!relation->rd_isnailed ||
		   relation->rd_rel->relkind == RELKIND_INDEX);
	/* Can't change for shared tables or indexes */
	Assert(!relation->rd_rel->relisshared);

	elog(DEBUG3, "setNewRelfilenodeToOid called.  newrelfilenode = %d",
		 newrelfilenode);

	CheckNewRelFileNodeIsOk(newrelfilenode, relation->rd_rel->reltablespace,
							relation->rd_rel->relisshared, NULL, relstorage_is_ao(relation->rd_rel->relstorage));

	setNewRelfilenodeCommon(relation, newrelfilenode);

	return newrelfilenode;
}
