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
 * relcache.h
 *	  Relation descriptor cache definitions.
 *
 *
 * Portions Copyright (c) 2005-2009, Greenplum inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/utils/relcache.h,v 1.56 2006/11/05 23:40:31 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELCACHE_H
#define RELCACHE_H

#include "access/htup.h"
#include "access/skey.h"
#include "access/tupdesc.h"
#include "nodes/bitmapset.h"
#include "nodes/pg_list.h"
#include "catalog/pg_class.h"


struct SysScanDescData; /* defined in access/genam.h */

typedef struct RelationData *Relation;

/* ----------------
 *		RelationPtr is used in the executor to support index scans
 *		where we have to keep track of several index relations in an
 *		array.	-cim 9/10/89
 * ----------------
 */
typedef Relation *RelationPtr;

/*
 * Routines to open (lookup) and close a relcache entry
 */
extern Relation RelationIdGetRelation(Oid relationId);
extern void RelationClose(Relation relation);

/*
 * Routines to compute/retrieve additional cached information
 */
struct GpPolicy *RelationGetPartitioningKey(Relation relation);
extern List *RelationGetIndexList(Relation relation);
extern Oid	RelationGetOidIndex(Relation relation);
extern List *RelationGetIndexExpressions(Relation relation);
extern List *RelationGetIndexPredicate(Relation relation);

extern void RelationSetIndexList(Relation relation,
					 List *indexIds, Oid oidIndex);

extern void RelationInitIndexAccessInfo(Relation relation);

typedef struct GpRelfileNodeScan
{
	Relation 		gp_relfile_node;

	Oid				relationId;

	Oid				relfilenode;

	ScanKeyData     scankey[1];
	
	struct SysScanDescData  *scan;
} GpRelfileNodeScan;

extern void
GpRelfileNodeBeginScan(
	Relation 			gp_relfile_node,

	Oid					relationId,

	Oid 				relfilenode,

	GpRelfileNodeScan 	*gpRelfileNodeScan);

extern HeapTuple
GpRelfileNodeGetNext(
	GpRelfileNodeScan 	*gpRelfileNodeScan,

	int32				*segmentFileNum,

	ItemPointer			persistentTid,

	int64				*persistentSerialNum);

extern void
GpRelfileNodeEndScan(
	GpRelfileNodeScan 	*gpRelfileNodeScan);

extern HeapTuple ScanGpRelfileNodeTuple(
	Relation 	gp_relfile_node,
	Oid 		relationNode,
	int32		segmentFileNum);
extern HeapTuple FetchGpRelfileNodeTuple(
	Relation 		gp_relfile_node,
	Oid 			relationNode,
	int32			segmentFileNum,
	ItemPointer		persistentTid,
	int64			*persistentSerialNum);
extern bool ReadGpRelfileNode(
	Oid 			relfilenode,
	int32			segmentFileNum,
	ItemPointer		persistentTid,
	int64			*persistentSerialNum);
extern void RelationFetchGpRelationNode(Relation relation);
extern void RelationFetchGpRelationNodeForXLog_Index(Relation relation);

/*
 * Routines for backend startup
 */
extern void RelationCacheInitialize(void);
extern void RelationCacheInitializePhase2(void);
	
/*
 * Routine to create a relcache entry for an about-to-be-created relation
 */
extern Relation RelationBuildLocalRelation(const char *relname,
						   Oid relnamespace,
						   TupleDesc tupDesc,
						   Oid relid,
						   Oid reltablespace,
			               char relkind,            /*CDB*/
			               char relstorage,
						   bool shared_relation);

/*
 * Routines for flushing/rebuilding relcache entries in various scenarios
 */
extern void RelationForgetRelation(Oid rid);

extern void RelationCacheInvalidateEntry(Oid relationId);

extern void RelationCacheInvalidate(void);

extern void AtEOXact_RelationCache(bool isCommit);
extern void AtEOSubXact_RelationCache(bool isCommit, SubTransactionId mySubid,
						  SubTransactionId parentSubid);

/*
 * Routines to help manage rebuilding of relcache init file
 */
extern bool RelationIdIsInInitFile(Oid relationId);
extern void RelationCacheInitFileInvalidate(bool beforeSend);
extern void RelationCacheInitFileRemove(const char *dbPath);

extern void IndexSupportInitialize(oidvector *indclass,
					   Oid *indexOperator,
					   RegProcedure *indexSupport,
					   StrategyNumber maxStrategyNumber,
					   StrategyNumber maxSupportNumber,
					   AttrNumber maxAttributeNumber);

/* should be used only by relcache.c and catcache.c */
extern bool criticalRelcachesBuilt;

/* Enum for system cache invalidation action types */
typedef enum SysCacheInvalidateAction
{
	/* Deleting a tuple from a catalog table */
	SysCacheInvalidate_Delete = 0,
	/* An in-place update to a tuple in a catalog table */
	SysCacheInvalidate_Update_InPlace,
	/* The "delete" part of an update to a catalog table */
	SysCacheInvalidate_Update_OldTup,
	/* The "insert" part of an update to a catalog table */
	SysCacheInvalidate_Update_NewTup,
	/* Inserting a tuple to a catalog table */
	SysCacheInvalidate_Insert,
	/* Vacuuming a tuple in a catalog table */
	SysCacheInvalidate_VacuumMove,

	/* Should be last */
	SysCacheInvalidate_InvalidAction
} SysCacheInvalidateAction;

#endif   /* RELCACHE_H */
