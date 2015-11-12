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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*-------------------------------------------------------------------------
 *
 * cdbdirectopen.c
 *
 *-------------------------------------------------------------------------
 */

#include "cdb/cdbdirectopen.h"
#include "utils/guc.h"
#include "storage/smgr.h"
#include "utils/memutils.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_am.h"
#include "catalog/pg_index.h"
#include "catalog/pg_opclass.h"
#include "utils/builtins.h"
#include "cdb/cdbvars.h"

/*
 * pg_class.
 */
static FormData_pg_class 
			DatabaseInfo_PgClassPgClass = 
						{Class_pg_class};

static FormData_pg_attribute 
			DatabaseInfo_PgClassAttrArray[Natts_pg_class] =
							{Schema_pg_class};

DirectOpenDefineStatic(DirectOpen_PgClass,\
DatabaseInfo_PgClassPgClass,\
DatabaseInfo_PgClassAttrArray,\
true);

/*
 * gp_relation_node.
 */
	
static FormData_pg_class 
			DatabaseInfo_GpRelfileNodePgClass =
						{Class_gp_relfile_node};

static FormData_pg_attribute 
			DatabaseInfo_GpRelfileNodeAttrArray[Natts_gp_relfile_node] =
							{Schema_gp_relfile_node};

DirectOpenDefineStatic(DirectOpen_GpRelfileNode,\
DatabaseInfo_GpRelfileNodePgClass,\
DatabaseInfo_GpRelfileNodeAttrArray,\
false);

	
/*
 * pg_appendonly.
 */
static FormData_pg_class 
			DatabaseInfo_PgAppendOnlyPgClass = 
						{Class_pg_appendonly};

static FormData_pg_attribute 
			DatabaseInfo_PgAppendOnlyAttrArray[Natts_pg_appendonly] =
							{Schema_pg_appendonly};

DirectOpenDefineStatic(DirectOpen_PgAppendOnly,\
DatabaseInfo_PgAppendOnlyPgClass,\
DatabaseInfo_PgAppendOnlyAttrArray,\
false);

/*
 * pg_aogseg.
 */
static FormData_pg_class 
			DatabaseInfo_PgAoSegPgClass = 
						{Class_pg_aoseg};

static FormData_pg_attribute 
			DatabaseInfo_PgAoSegAttrArray[Natts_pg_aoseg] =
							{Schema_pg_aoseg};

DirectOpenDefineStatic(DirectOpen_PgAoSeg,\
DatabaseInfo_PgAoSegPgClass,\
DatabaseInfo_PgAoSegAttrArray,\
false);

/*
 * gp_global_sequence.
 */
static FormData_pg_class 
			GlobalSequence_PgClass = 
							{Class_gp_global_sequence};

static FormData_pg_attribute 
			GlobalSequence_AttrArray[Natts_gp_global_sequence] =
							{Schema_gp_global_sequence};

DirectOpenDefineStatic(DirectOpen_GpGlobalSequence,\
		GlobalSequence_PgClass,\
		GlobalSequence_AttrArray,\
		false);

/*
 * gp_persistent_relfile_node.
 */
static FormData_pg_class 
			PersistentFileSysObj_RelfilePgClass =
							{Class_gp_persistent_relfile_node};

static FormData_pg_attribute 
			PersistentFileSysObj_RelfileAttrArray[Natts_gp_persistent_relfile_node] =
							{Schema_gp_persistent_relfile_node};

DirectOpenDefineStatic(DirectOpen_GpPersistentRelfileNode,\
PersistentFileSysObj_RelfilePgClass,\
PersistentFileSysObj_RelfileAttrArray,\
false);

/*
 * gp_persistent_relation_node.
 */
static FormData_pg_class
			PersistentFileSysObj_RelationPgClass =
							{Class_gp_persistent_relation_node};

static FormData_pg_attribute
			PersistentFileSysObj_RelationAttrArray[Natts_gp_persistent_relation_node] =
							{Schema_gp_persistent_relation_node};

DirectOpenDefineStatic(DirectOpen_GpPersistentRelationNode,\
PersistentFileSysObj_RelationPgClass,\
PersistentFileSysObj_RelationAttrArray,\
false);

/*
 * gp_persistent_database_node.
 */
static FormData_pg_class 
			PersistentFileSysObj_DatabasePgClass = 
							{Class_gp_persistent_database_node};

static FormData_pg_attribute 
			PersistentFileSysObj_DatabaseAttrArray[Natts_gp_persistent_database_node] =
							{Schema_gp_persistent_database_node};

DirectOpenDefineStatic(DirectOpen_GpPersistentDatabaseNode,\
PersistentFileSysObj_DatabasePgClass,\
PersistentFileSysObj_DatabaseAttrArray,\
false);

/*
 * This HUGE MAGIC DEFINE expands into module globals and two routines:
 *     PersistentFileSysObj_TablespaceOpenShared
 *     PersistentFileSysObj_TablespaceClose
 * It allows for opening the relation without going through pg_class, etc.
 */
static FormData_pg_class 
			PersistentFileSysObj_TablespacePgClass = 
							{Class_gp_persistent_tablespace_node};

static FormData_pg_attribute 
			PersistentFileSysObj_TablespaceAttrArray[Natts_gp_persistent_tablespace_node] =
							{Schema_gp_persistent_tablespace_node};

DirectOpenDefineStatic(DirectOpen_GpPersistentTableSpaceNode,\
PersistentFileSysObj_TablespacePgClass,\
PersistentFileSysObj_TablespaceAttrArray,\
false);


/*
 * This HUGE MAGIC DEFINE expands into module globals and two routines:
 *     PersistentFileSysObj_FilespaceOpenShared
 *     PersistentFileSysObj_FilespaceClose
 * It allows for opening the relation without going through pg_class, etc.
 */
static FormData_pg_class 
			PersistentFileSysObj_FilespacePgClass = 
							{Class_gp_persistent_filespace_node};

static FormData_pg_attribute 
			PersistentFileSysObj_FilespaceAttrArray[Natts_gp_persistent_filespace_node] =
							{Schema_gp_persistent_filespace_node};

DirectOpenDefineStatic(DirectOpen_GpPersistentFileSpaceNode,\
PersistentFileSysObj_FilespacePgClass,\
PersistentFileSysObj_FilespaceAttrArray,\
false);

// INDEX Variants


/*
 * gp_relation_node_index.
 */
	
static FormData_pg_class 
			DatabaseInfo_GpRelfileNodeIndexPgClass =
						{Class_gp_relfile_node_index};

static FormData_pg_attribute 
			DatabaseInfo_GpRelfileNodeIndexAttrArray[Natts_gp_relfile_node_index] =
							{Schema_gp_relfile_node_index};

static FormData_pg_am 
			DatabaseInfo_GpRelfileNodeIndexPgAm =
						{Am_gp_relfile_node_index};

static FormData_pg_index 
			DatabaseInfo_GpRelfileNodeIndexPgIndex =
						{Index_gp_relfile_node_index};

static int2 DatabaseInfo_GpRelfileNodeIndexIndKeyArray[Natts_gp_relfile_node_index] =
			{IndKey_gp_relfile_node_index};

static Oid DatabaseInfo_GpRelfileNodeIndexIndClassArray[Natts_gp_relfile_node_index] =
			{IndClass_gp_relfile_node_index};

DirectOpenIndexDefineStatic(DirectOpen_GpRelfileNodeIndex,\
DatabaseInfo_GpRelfileNodeIndexPgClass,\
DatabaseInfo_GpRelfileNodeIndexAttrArray,\
DatabaseInfo_GpRelfileNodeIndexPgAm,\
DatabaseInfo_GpRelfileNodeIndexPgIndex,\
DatabaseInfo_GpRelfileNodeIndexIndKeyArray,\
DatabaseInfo_GpRelfileNodeIndexIndClassArray,\
false);


Relation DirectOpen_Open(
	DirectOpen *direct,

	Oid relationId,

	Oid tablespace,

	Oid database,

	Oid relfilenode,

	FormData_pg_class *pgClass,

	FormData_pg_attribute *attrArray,

	FormData_pg_am *pgAm,

	FormData_pg_index *pgIndex,

	int2 *indKeyArray,

	Oid *indClassArray,

	bool relHasOid)
{
	int natts;
	int i;

	Assert(pgClass != NULL);
	natts = pgClass->relnatts;

	if (relationId == -1)
		relationId = pgClass->relfilenode;		// Assume it is ok to use the relfilenode as the relationId in our limited usage.

	if (relfilenode == -1)
		relfilenode = pgClass->relfilenode;

	if (!direct->isInit)
	{
		/*
		 * Lots of Hard-coded construction of the gp_persistent* RelationS and
		 * dependent objects like tuple descriptors, etc.
		 */

		direct->relationData.rd_refcnt = 0;
		direct->relationData.rd_isvalid = true;

		direct->relationData.rd_id = relationId;

		direct->relationData.rd_rel = pgClass;

		if (pgIndex != NULL)
		{
			int pgIndexFixedLen = offsetof(FormData_pg_index, indkey);
			int indKeyVectorLen = Int2VectorSize(natts);
			int2vector *indKeyVector;

			uint16		amstrategies;
			uint16		amsupport;

			Oid 	   *operator;
			RegProcedure *support;
			FmgrInfo   *supportinfo;

			Assert(pgAm != NULL);
			Assert(indKeyArray != NULL);
			Assert(indClassArray != NULL);

			/*
			 * Allocate Formdata_pg_index with fields through indkey
			 * where indkey is a variable length int2vector with indKeyArray values.
			 */
			direct->relationData.rd_index = 
						(FormData_pg_index*)palloc(
								pgIndexFixedLen + indKeyVectorLen);
			memcpy(direct->relationData.rd_index, pgIndex, pgIndexFixedLen);

			indKeyVector = buildint2vector(
									indKeyArray,
									natts);
			memcpy(
				&direct->relationData.rd_index->indkey, 
				indKeyVector, 
				indKeyVectorLen);

			pfree(indKeyVector);

			/*
			 * Create oidvector in rd_indclass with values from indClassArray.
			 */
			direct->relationData.rd_indclass = 
									buildoidvector(
											indClassArray, 
											natts);

			direct->relationData.rd_am = pgAm;

			amstrategies = pgAm->amstrategies;
			amsupport = pgAm->amsupport;

			direct->relationData.rd_indexcxt = TopMemoryContext;

			/*
			 * Allocate arrays to hold data
			 */
			direct->relationData.rd_aminfo = (RelationAmInfo *)
				MemoryContextAllocZero(TopMemoryContext, sizeof(RelationAmInfo));

			if (amstrategies > 0)
				operator = (Oid *)
					MemoryContextAllocZero(TopMemoryContext,
										   natts * amstrategies * sizeof(Oid));
			else
				operator = NULL;

			if (amsupport > 0)
			{
				int			nsupport = natts * amsupport;

				support = (RegProcedure *)
					MemoryContextAllocZero(TopMemoryContext, nsupport * sizeof(RegProcedure));
				supportinfo = (FmgrInfo *)
					MemoryContextAllocZero(TopMemoryContext, nsupport * sizeof(FmgrInfo));
			}
			else
			{
				support = NULL;
				supportinfo = NULL;
			}

			direct->relationData.rd_operator = operator;
			direct->relationData.rd_support = support;
			direct->relationData.rd_supportinfo = supportinfo;

			/*
			 * Fill the operator and support procedure OID arrays.	(aminfo and
			 * supportinfo are left as zeroes, and are filled on-the-fly when used)
			 */
			IndexSupportInitialize(direct->relationData.rd_indclass,
								   operator, support,
								   amstrategies, amsupport, natts);

			/*
			 * expressions and predicate cache will be filled later.
			 */
			direct->relationData.rd_indexprs = NIL;
			direct->relationData.rd_indpred = NIL;
			direct->relationData.rd_amcache = NULL;		
		}

		// Not much in terms of contraints.
		direct->constrData.has_not_null = true;

		/*
		 * Setup tuple descriptor for columns.
		 */
		direct->descData.natts = pgClass->relnatts;

		// Make the array of pointers.
		direct->descData.attrs = 
				(Form_pg_attribute*)
						MemoryContextAllocZero(
									TopMemoryContext, 
									sizeof(Form_pg_attribute*) * pgClass->relnatts);

		for (i = 0; i < pgClass->relnatts; i++)
		{
			direct->descData.attrs[i] = 
						(Form_pg_attribute)
								MemoryContextAllocZero(
											TopMemoryContext, 
											sizeof(FormData_pg_attribute));

			memcpy(direct->descData.attrs[i], &(attrArray[i]), sizeof(FormData_pg_attribute));

			// Patch up relation id.
			direct->descData.attrs[i]->attrelid = relationId;
		}

		direct->descData.constr = &direct->constrData;
		direct->descData.tdtypeid = pgClass->reltype;
		direct->descData.tdtypmod = -1;
		direct->descData.tdhasoid = relHasOid;
		direct->descData.tdrefcount = 1;

		direct->relationData.rd_att = &direct->descData;

		direct->pgStat.t_id = relationId;
		direct->pgStat.t_shared = 1;

		direct->relationData.pgstat_info = &direct->pgStat;

		direct->relationData.rd_relationnodeinfo.isPresent = FALSE;
		direct->relationData.rd_relationnodeinfo.tidAllowedToBeZero = FALSE;

		direct->isInit = true;
	}
	
	// UNDONE: Should verify for NON-SHARED relations we don't open relations in different databases / or
	// UNDONE: open different relations in same database at same time !!!
	direct->relationData.rd_node.spcNode = tablespace;
	direct->relationData.rd_node.dbNode = database;
	direct->relationData.rd_node.relNode = relfilenode;

	for (i = 0; i < direct->relationData.rd_rel->relnatts; i++)
	{
		Assert(direct->descData.attrs[i] != NULL);
		
		// Patch up relation id.
		direct->descData.attrs[i]->attrelid = direct->relationData.rd_id;
	}

	direct->relationData.rd_refcnt++;

	RelationOpenSmgr(&direct->relationData);

	return &direct->relationData;
}

void DirectOpen_Close(
	DirectOpen 	*direct,
	Relation	rel)
{
	Assert(rel == &direct->relationData);
	Assert (direct->isInit);

	RelationCloseSmgr(&direct->relationData);

	Assert(direct->relationData.rd_refcnt > 0);
	direct->relationData.rd_refcnt--;
}
