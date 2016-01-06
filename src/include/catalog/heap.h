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
 * heap.h
 *	  prototypes for functions in backend/catalog/heap.c
 *
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/catalog/heap.h,v 1.85 2006/07/13 16:49:19 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef HEAP_H
#define HEAP_H

#include "parser/parse_node.h"
#include "catalog/gp_persistent.h"


typedef struct RawColumnDefault
{
	AttrNumber	attnum;			/* attribute to attach default to */
	Node	   *raw_default;	/* default value (untransformed parse tree) */
} RawColumnDefault;

typedef struct CookedConstraint
{
	ConstrType	contype;		/* CONSTR_DEFAULT or CONSTR_CHECK */
	char	   *name;			/* name, or NULL if none */
	AttrNumber	attnum;			/* which attr (only for DEFAULT) */
	Node	   *expr;			/* transformed default or check expr */
} CookedConstraint;

extern Relation heap_create(const char *relname,
			Oid relnamespace,
			Oid reltablespace,
			Oid relid,
			TupleDesc tupDesc,
			Oid relam,
			char relkind,
			char relstorage,
			bool shared_relation,
			bool allow_system_table_mods,
			bool bufferPoolBulkLoad);

extern Oid heap_create_with_catalog(const char *relname,
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
                         const struct GpPolicy *policy,    /* MPP */
						 Datum reloptions,
						 bool allow_system_table_mods,
						 Oid *comptypeOid, /* MPP */
						 ItemPointer persistentTid,
						 int64 *persistentSerialNum);

extern void heap_drop_with_catalog(Oid relid);

extern void heap_truncate(List *relids);

extern void heap_truncate_check_FKs(List *relations, bool tempTables);

extern List *heap_truncate_find_FKs(List *relationIds);

extern void InsertPgClassTuple(Relation pg_class_desc,
				   Relation new_rel_desc,
				   Oid new_rel_oid,
				   Datum reloptions);

extern void InsertGpRelfileNodeTuple(
	Relation 		gp_relfile_node,
	Oid				relationId,
	char			*relname,
	Oid				relation,
	int32			segmentFileNum,
	bool			updateIndex,
	ItemPointer		persistentTid,
	int64			persistentSerialNum);
extern void UpdateGpRelfileNodeTuple(
		Relation	gp_relfile_node,
		HeapTuple	tuple,
		Oid 		relation,
		int32		segmentFileNum,
		ItemPointer persistentTid,
		int64		persistentSerialNum);
extern void DeleteGpRelfileNodeTuple(
		Relation 	relation,
		int32		segmentFileNum);

extern List *AddRelationRawConstraints(Relation rel,
						  List *rawColDefaults,
						  List *rawConstraints);
extern List *AddRelationConstraints(Relation rel,
						  List *rawColDefaults,
						  List *constraints);

extern void StoreAttrDefault(Relation rel, AttrNumber attnum, char *adbin);

extern Node *cookDefault(ParseState *pstate,
			Node *raw_default,
			Oid atttypid,
			int32 atttypmod,
			char *attname);

extern int RemoveRelConstraints(Relation rel, const char *constrName,
					 DropBehavior behavior);

extern void DeleteRelationTuple(Oid relid);
extern void DeleteAttributeTuples(Oid relid);
extern void RemoveAttributeById(Oid relid, AttrNumber attnum);
extern void RemoveAttrDefault(Oid relid, AttrNumber attnum,
				  DropBehavior behavior, bool complain);
extern void RemoveAttrDefaultById(Oid attrdefId);
extern void RemoveStatistics(Oid relid, AttrNumber attnum);

extern Form_pg_attribute SystemAttributeDefinition(AttrNumber attno,
						  bool relhasoids);

extern Form_pg_attribute SystemAttributeByName(const char *attname,
					  bool relhasoids);

extern void CheckAttributeNamesTypes(TupleDesc tupdesc, char relkind);

extern void CheckAttributeType(const char *attname, Oid atttypid);
extern void CheckAttributeForParquet(TupleDesc tupdesc, char relstorage);

extern void SetRelationNumChecks(Relation rel, int numchecks);

extern Oid setNewRelfilenode(Relation relation);

extern Oid setNewRelfilenodeToOid(Relation relation, Oid newrelfilenode);

/* MPP-6929: metadata tracking */
extern void MetaTrackAddObject(Oid		classid, 
							   Oid		objoid, 
							   Oid		relowner,
							   char*	actionname,
							   char*	subtype);
extern void MetaTrackUpdObject(Oid		classid, 
							   Oid		objoid, 
							   Oid		relowner,
							   char*	actionname,
							   char*	subtype);
extern void MetaTrackDropObject(Oid		classid, 
								Oid		objoid);

#define MetaTrackValidRelkind(relkind) \
		(((relkind) == RELKIND_RELATION) \
		|| ((relkind) == RELKIND_INDEX) \
		|| ((relkind) == RELKIND_SEQUENCE) \
		|| ((relkind) == RELKIND_VIEW)) 

#endif   /* HEAP_H */
