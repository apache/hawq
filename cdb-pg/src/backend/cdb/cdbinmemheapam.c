/*-------------------------------------------------------------------------
 *
 * cdbinmemheapam.c
 *	  goh in-memory heap table access method
 *
 * Copyright (c) 2007-2013, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "cdb/cdbinmemheapam.h"
#include "utils/hsearch.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "nodes/memnodes.h"
#include "storage/lock.h"
#include "access/valid.h"
#include "access/genam.h"
#include "cdb/cdbvars.h"
#include "nodes/parsenodes.h"
#include "catalog/gp_fastsequence.h"
#include "catalog/aoseg.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_tablespace.h"
#include "catalog/catalog.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_constraint.h"
#include "optimizer/prep.h"

HTAB *OidInMemHeapMapping = NULL;

static MemoryContext InMemHeapTableCxt = NULL;

static int InMemHeap_Find(InMemHeapRelation relation, ItemPointer otid);
static void InMemHeap_Insert_Internal(InMemHeapRelation relation,
		HeapTuple tup, uint8 flag, int contentid);
static void prepareDispatchedCatalogFastSequence(Relation rel, Oid objid, int64 objmod);

/*
 * init relid to in-memory table mapping
 */
void
InitOidInMemHeapMapping(long initSize, MemoryContext memcxt)
{
	HASHCTL info;

	Assert(!OidInMemHeapMapping);

	info.hcxt = memcxt;
	info.hash = oid_hash;
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(struct OidInMemHeapMappingEntry);

	OidInMemHeapMapping = hash_create("OidInMemHeapMapping", initSize, &info,
			HASH_CONTEXT | HASH_FUNCTION | HASH_ELEM);

	Assert(NULL != OidInMemHeapMapping);

	InMemHeapTableCxt = memcxt;
}

/*
 * cleanup relid to in-memory table mapping
 */
void
CleanupOidInMemHeapMapping(void)
{
	if(!OidInMemHeapMapping)
		return;

	hash_destroy(OidInMemHeapMapping);
	OidInMemHeapMapping = NULL;
	InMemHeapTableCxt = NULL;
}

/*
 * get a in-memory table by relid,
 */
InMemHeapRelation
OidGetInMemHeapRelation(Oid relid)
{
	bool found = FALSE;
	struct OidInMemHeapMappingEntry *retval;

	if (OidInMemHeapMapping)
	{
		retval = hash_search(OidInMemHeapMapping, &relid, HASH_FIND, &found);
		if (NULL != retval)
			return retval->rel;
	}

	return NULL;
}

/*
 * create a in-memory heap table with Oid.
 * the in-memory table and all its tuples are in memcxt memory context.
 * at first, initSize tuples space will be alloced in the tuple,
 * and will re-alloc at runtime if inserting more tuples.
 */
InMemHeapRelation
InMemHeap_Create(Oid relid, Relation rel, bool ownrel, int32 initSize,
		LOCKMODE lock, const char *relname)
{
	bool found = FALSE;
	struct OidInMemHeapMappingEntry *entry;
	InMemHeapRelation memheap = NULL;
	MemoryContext oldcxt;

	Assert (NULL != OidInMemHeapMapping);

	entry = hash_search(OidInMemHeapMapping, &relid, HASH_FIND, &found);

	if (found)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("in-memory table with Oid = %d already exist.", relid)));

	Assert(MemoryContextIsValid(InMemHeapTableCxt));

	oldcxt = CurrentMemoryContext;
	CurrentMemoryContext = InMemHeapTableCxt;

	memheap = palloc(sizeof(InMemHeapRelationData));

	memheap->memcxt = InMemHeapTableCxt;
	memheap->relid = relid;
	memheap->tupsize = 0;
	memheap->tupmaxsize = initSize;
	memheap->tuples = NULL;
	memheap->rellock = lock;
	memheap->ownrel = ownrel;
	memheap->rel = rel;
	strncpy(memheap->relname, relname, NAMEDATALEN);

	initSize = initSize > 1 ? initSize : 1;
	memheap->tuples = palloc(sizeof(InMemHeapTupleData) * initSize);

	entry = hash_search(OidInMemHeapMapping, &relid, HASH_ENTER, &found);

	entry->relid = relid;
	entry->rel = memheap;

	CurrentMemoryContext = oldcxt;

	return memheap;
}

/*
 * drop a in-memory heap table.
 */
void
InMemHeap_Drop(Oid relid)
{
	bool found = FALSE;
	struct OidInMemHeapMappingEntry *entry;

	Assert(NULL != OidInMemHeapMapping);

	entry = hash_search(OidInMemHeapMapping, &relid, HASH_FIND, &found);

	if (!entry)
		return;

	Assert(entry->rel);

	if (entry->rel->tuples)
	{
		int i;
		HeapTuple tup;
		for (i = 0; i < entry->rel->tupsize; ++i)
		{
			tup = entry->rel->tuples[i].tuple;
			if (tup)
				pfree(tup);
		}
		pfree(entry->rel->tuples);
	}

	if (entry->rel->ownrel && entry->rel->rel)
		heap_close(entry->rel->rel, entry->rel->rellock);

	pfree(entry->rel);

	hash_search(OidInMemHeapMapping, &relid, HASH_REMOVE, &found);
}

/*
 * trop all in-memory table.
 */
void
InMemHeap_DropAll(void)
{
	HASH_SEQ_STATUS scan;
	struct OidInMemHeapMappingEntry *entry;

	if (!OidInMemHeapMapping)
		return;

	hash_seq_init(&scan, OidInMemHeapMapping);

	while (!!(entry = (struct OidInMemHeapMappingEntry *) hash_seq_search(&scan)))
	{
		InMemHeap_Drop(entry->relid);
	}
}

/*
 * begin a in-memory heap table scan.
 */
InMemHeapScanDesc
InMemHeap_BeginScan(InMemHeapRelation memheap, int nkeys,
		ScanKey key, bool inmemonly)
{
	InMemHeapScanDesc scan = palloc0(sizeof (InMemHeapScanDescData));
	Assert(scan);

	scan->rs_rd = memheap;
	scan->rs_nkeys = nkeys;
	scan->rs_index = -1;

	if (nkeys > 0)
		scan->rs_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
	else
		scan->rs_key = NULL;

	if (key != NULL )
		memcpy(scan->rs_key, key, scan->rs_nkeys * sizeof(ScanKeyData));

	if (!inmemonly && scan->rs_rd->rel)
		scan->hscan = heap_beginscan(scan->rs_rd->rel, SnapshotNow,
				scan->rs_nkeys, scan->rs_key);

	return scan;
}

/*
 * end a in-memory heap table scan.
 */
void
InMemHeap_EndScan(InMemHeapScanDesc scan)
{
	Assert(scan);

	if (scan->rs_key)
		pfree(scan->rs_key);

	if (scan->hscan)
		heap_endscan(scan->hscan);

	pfree(scan);
}

/*
 * get next tuple in in-memory heap table.
 */
HeapTuple
InMemHeap_GetNext(InMemHeapScanDesc scan, ScanDirection direction)
{

	bool valid;

	InMemHeapTuple pmemtup = NULL;

	Assert(NULL != scan);

	if (ScanDirectionIsForward(direction))
	{
		for (scan->rs_index++; scan->rs_index < scan->rs_rd->tupsize;
				scan->rs_index++)
		{
			pmemtup = &scan->rs_rd->tuples[scan->rs_index];

			if (pmemtup->flags == INMEM_HEAP_TUPLE_DELETED)
				continue;

			Assert(NULL != pmemtup->tuple);

			if (scan->rs_key != NULL )
			{
				Assert(NULL != scan->rs_rd->rel);
				HeapKeyTest(pmemtup->tuple, RelationGetDescr(scan->rs_rd->rel),
						scan->rs_nkeys, scan->rs_key, &valid);
			}

			if (!valid)
				continue;

			scan->rs_ctup = pmemtup->tuple;
			return scan->rs_ctup;
		}
	}
	else if (ScanDirectionIsBackward(direction))
	{
		for (scan->rs_index--; scan->rs_index >= 0; scan->rs_index--)
		{
			pmemtup = &scan->rs_rd->tuples[scan->rs_index];

			if (pmemtup->flags == INMEM_HEAP_TUPLE_DELETED)
				continue;

			Assert(NULL != pmemtup->tuple);

			if (scan->rs_key != NULL )
			{
				Assert(NULL == scan->rs_rd->rel);
				HeapKeyTest(pmemtup->tuple, RelationGetDescr(scan->rs_rd->rel),
						scan->rs_nkeys, scan->rs_key, &valid);
			}

			if (!valid)
				continue;

			scan->rs_ctup = pmemtup->tuple;
			return scan->rs_ctup;
		}
	}
	else
	{
		Assert(direction == NoMovementScanDirection);

		if (scan->rs_index >= 0 && scan->rs_index < scan->rs_rd->tupsize)
		{
			pmemtup = &scan->rs_rd->tuples[scan->rs_index];
			if (pmemtup->flags == INMEM_HEAP_TUPLE_DELETED)
				return scan->rs_ctup = pmemtup->tuple;;
		}
	}
	/*
	 * read from local read only heap table.
	 */
	if (scan->hscan)
		return heap_getnext(scan->hscan, direction);

	return NULL ;
}

/*
 * find a tuple which is only in memory, do not find it in heap table.
 */
static int
InMemHeap_Find(InMemHeapRelation relation, ItemPointer otid)
{
	int rc = 0;
	Assert(NULL != relation);
	for (rc = 0; rc < relation->tupsize; ++rc)
		if (!memcmp(&relation->tuples[rc].tuple->t_self, otid, sizeof(ItemPointerData))
				&& relation->tuples[rc].flags != INMEM_HEAP_TUPLE_IGNORE)
			break;
	return rc;
}

/*
 * insert a tuple into in-memory heap table with flag.
 *
 * caller need to make sure that no dirty entry in the table.
 * And we also do not check the constrains on the table.
 *
 * Eg, first delete tuple A from in-memory table and then insert it into,
 * that will add TWO tuple of A into in-memory table, one with flag of INMEM_HEAP_TUPLE_DELETED
 * and another with flag of INMEM_HEAP_TUPLE_ADDED.
 */
static void
InMemHeap_Insert_Internal(InMemHeapRelation relation,
		HeapTuple tup, uint8 flag, int contentid)
{
	InMemHeapTuple inmemtup;

	MemoryContext oldmem = CurrentMemoryContext;

	Assert(NULL != relation && NULL != tup);

	CurrentMemoryContext = relation->memcxt;

	if (relation->tupsize >= relation->tupmaxsize)
	{
		Assert(NULL != relation->tuples);
		relation->tuples = repalloc(relation->tuples,
				sizeof(InMemHeapTupleData) * relation->tupmaxsize * 2);
		relation->tupmaxsize *= 2;
	}

	inmemtup = &relation->tuples[relation->tupsize];

	inmemtup->contentid = contentid;
	inmemtup->flags = flag;
	inmemtup->tuple = heaptuple_copy_to(tup, NULL, NULL);
	Assert(inmemtup->tuple != NULL);

	++ relation->tupsize;

	CurrentMemoryContext = oldmem;

}


void
InMemHeap_InsertTablespaceInfo(InMemHeapRelation relation, DispatchedFilespaceDirEntry entry)
{
	InMemHeapTuple inmemtup;

	MemoryContext oldmem = CurrentMemoryContext;

	Assert(NULL != relation && NULL != entry);

	CurrentMemoryContext = relation->memcxt;

	if (relation->tupsize >= relation->tupmaxsize)
	{
		Assert(NULL != relation->tuples);
		relation->tuples = repalloc(relation->tuples,
				sizeof(InMemHeapTupleData) * relation->tupmaxsize * 2);
		relation->tupmaxsize *= 2;
	}

	inmemtup = &relation->tuples[relation->tupsize];

	inmemtup->contentid = MASTER_CONTENT_ID;
	inmemtup->flags = INMEM_HEAP_TUPLE_ADDED;
	inmemtup->entry = palloc(sizeof(DispatchedFilespaceDirEntryData));
	memcpy(inmemtup->entry, entry, sizeof(DispatchedFilespaceDirEntryData));

	++relation->tupsize;

	CurrentMemoryContext = oldmem;
}


/*
 * insert a tuple into in-memory heap table.
 * if updateFlag is true, insert tuple with flag INMEM_HEAP_TUPLE_ADDED
 */
void
InMemHeap_Insert(InMemHeapRelation relation, HeapTuple tup,
		bool updateFlag, int contentid)
{
	Assert(NULL != relation && NULL != tup);
	if (Gp_role != GP_ROLE_EXECUTE)
		Assert(ItemPointerIsValid(&tup->t_self));

	if (updateFlag)
		InMemHeap_Insert_Internal(relation, tup, INMEM_HEAP_TUPLE_ADDED, contentid);
	else
		InMemHeap_Insert_Internal(relation, tup, 0, contentid);
}

/*
 * update a tuple in in-memory heap table.
 *
 * if the target tuple already in the memory,
 * update it in-place with flag INMEM_HEAP_TUPLE_UPDATED.
 * else report an error.
 *
 * update should not change the otid of the old tuple,
 * since updated tuple should write back to the master and update there.
 */
void
InMemHeap_Update(InMemHeapRelation relation, ItemPointer otid,
		HeapTuple tup)
{
	int pos;
	HeapTuple target;
	MemoryContext oldmem = CurrentMemoryContext;

	Assert(ItemPointerIsValid(otid));

	pos = InMemHeap_Find(relation, otid);

	CurrentMemoryContext = relation->memcxt;

	/*
	 * not found, add a new one
	 */
	if (pos >= relation->tupsize)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("update a tuple which does not exist, relname = %s, relid = %u",
								relation->rel->rd_rel->relname.data, relation->relid)));
	}

	/*
	 * already in table
	 */
	/*
	 * cannot update a deleted tuple
	 */
	if (relation->tuples[pos].flags == INMEM_HEAP_TUPLE_DELETED)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("update an deleted in-memory heap tuple")));
	}
	else if (relation->tuples[pos].flags == INMEM_HEAP_TUPLE_ADDED)
	{
		/* keep the flag as INMEM_HEAP_TUPLE_ADDED but update the content */
	}
	else
	{
		/* Duplicate update */
		Assert(relation->tuples[pos].flags == INMEM_HEAP_TUPLE_DISPATCHED
				|| relation->tuples[pos].flags == INMEM_HEAP_TUPLE_UPDATED);
		relation->tuples[pos].flags = INMEM_HEAP_TUPLE_UPDATED;
	}

	target = heaptuple_copy_to(tup, NULL, NULL);

	/*
	 * do not modify original tuple header
	 */
	ItemPointerCopy(&target->t_self, &relation->tuples[pos].tuple->t_self);

	Assert(ItemPointerEquals(&target->t_self, otid));

	memcpy(target->t_data, relation->tuples[pos].tuple->t_data,
			sizeof(HeapTupleHeaderData));

	CurrentMemoryContext = oldmem;

	pfree(relation->tuples[pos].tuple);
	relation->tuples[pos].tuple = target;
}

/*
 * mark an in-memory tuple deleted.
 */
void
InMemHeap_Delete(InMemHeapRelation relation, ItemPointer tid)
{
	int pos;
	pos = InMemHeap_Find(relation, tid);

	if (pos == relation->tupsize)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("to be deleted in-memory heap tuple cannot be found.")));

	if (relation->tuples[pos].flags == INMEM_HEAP_TUPLE_ADDED)
	{
		/* delete the tuple which was add by QE, it does not need to be deleted on QD, ignore it*/
		relation->tuples[pos].flags = INMEM_HEAP_TUPLE_IGNORE;
	}
	else
	{
		/* drop the update and delete the tuple */
		relation->tuples[pos].flags = INMEM_HEAP_TUPLE_DELETED;
	}
}

/*
 * collect pg_namespace tuples for oid.
 * add them to in-memory heap table for dispatcher
 */
void
prepareDispatchedCatalogNamespace(Oid namespace)
{
	HeapTuple tuple;
	InMemHeapRelation inmemrel;

	if (namespace == PG_CATALOG_NAMESPACE || namespace == PG_TOAST_NAMESPACE
			|| namespace == PG_BITMAPINDEX_NAMESPACE
			|| namespace == PG_PUBLIC_NAMESPACE
			|| namespace == PG_AOSEGMENT_NAMESPACE)
		return;

	tuple = SearchSysCache(NAMESPACEOID, ObjectIdGetDatum(namespace), 0, 0, 0);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for namespace %u", namespace);

	inmemrel = OidGetInMemHeapRelation(NamespaceRelationId);
	if (NULL == inmemrel)
	{
		inmemrel = InMemHeap_Create(NamespaceRelationId, NULL, FALSE, 10,
				NoLock, "pg_namespace");
		Assert(NULL != inmemrel);
	}
	InMemHeap_Insert(inmemrel, tuple, TRUE, MASTER_CONTENT_ID);

	ReleaseSysCache(tuple);
}

/*
 * collect pg_tablespace tuples for oid.
 * add them to in-memory heap table for dispatcher
 */
void
prepareDispatchedCatalogTablespace(Oid tablespace)
{

	char *path = NULL, *pos;

	InMemHeapRelation inmemrel;
	DispatchedFilespaceDirEntryData entry;

	if (IsBuiltinTablespace(tablespace) || tablespace == InvalidOid )
		return;

	inmemrel = OidGetInMemHeapRelation(TableSpaceRelationId);
	if (NULL == inmemrel)
	{
		inmemrel = InMemHeap_Create(TableSpaceRelationId, NULL, FALSE, 10, NoLock, "pg_tablespace");
		Assert(NULL != inmemrel);
	}

	entry.tablespace = tablespace;

	/*
	 * here we get the seg0' s tablespace location since
	 * master's tablespace location is special in gpsql.
	 */
	Assert(GpIdentity.numsegments >=1 );
	GetFilespacePathForTablespace(tablespace, 0, &path);

	Assert(NULL != path);
	Assert(strlen(path) < FilespaceLocationBlankPaddedWithNullTermLen);

	pos = path + strlen(path) - 1;
	Assert('0' == *pos);

	*pos = '\0';
	snprintf(entry.location, FilespaceLocationBlankPaddedWithNullTermLen,"%s%s", path, "%d");

	InMemHeap_InsertTablespaceInfo(inmemrel, &entry);
}

/*
 * collect pg_class/pg_type/pg_attribute tuples for oid.
 * add them to in-memory heap table for dispatcher.
 */
void
prepareDispatchedCatalogObject(Oid oid)
{
	HeapTuple classtuple;
	HeapTuple typetuple;
	HeapTuple attrtuple;
	HeapTuple attrdeftuple;
	HeapTuple constuple;

	Datum typeid, namespace, tablespace, toastrelid;

	Relation rel;
	ScanKeyData skey;
	SysScanDesc scandesc;

	Assert(oid != InvalidOid);
	InMemHeapRelation inmemrel;

	/* find oid in pg_class */
	classtuple = SearchSysCache(RELOID, ObjectIdGetDatum(oid), 0, 0, 0);

	if (!HeapTupleIsValid(classtuple))
		elog(ERROR, "cache lookup failed for relation %u", oid);

	inmemrel = OidGetInMemHeapRelation(RelationRelationId);
	if (NULL == inmemrel)
	{
		inmemrel = InMemHeap_Create(RelationRelationId, NULL, FALSE, 10, NoLock, "pg_class");
		Assert(NULL != inmemrel);
	}
	InMemHeap_Insert(inmemrel, classtuple, TRUE, MASTER_CONTENT_ID);

	/* collect pg_namespace info */
	namespace = SysCacheGetAttr(RELOID, classtuple, Anum_pg_class_relnamespace,
			NULL );

	if (InvalidOid == DatumGetObjectId(namespace))
		elog(ERROR, "relnamespace field in pg_class of %u is invalid", oid);

	prepareDispatchedCatalogNamespace(DatumGetObjectId(namespace));

	/* collect pg_tablespace info */
	tablespace = SysCacheGetAttr(RELOID, classtuple, Anum_pg_class_reltablespace,
			NULL );

	prepareDispatchedCatalogTablespace(DatumGetObjectId(tablespace));

	/* collect toast info */
	toastrelid = SysCacheGetAttr(RELOID, classtuple,
			Anum_pg_class_reltoastrelid, NULL );

	if (InvalidOid != DatumGetObjectId(toastrelid))
	{
		/*TODO*/
		Insist(!"cannot handle toast table right now!");
	}

	/* collect pg_type info */
	typeid = SysCacheGetAttr(RELOID, classtuple, Anum_pg_class_reltype, NULL );

	if (InvalidOid == DatumGetObjectId(typeid))
		elog(ERROR, "reltype in pg_class for %u is invalid", oid);

	typetuple = SearchSysCache(TYPEOID, typeid, 0, 0, 0);
	inmemrel = OidGetInMemHeapRelation(TypeRelationId);
	if (NULL == inmemrel)
	{
		inmemrel = InMemHeap_Create(TypeRelationId, NULL, FALSE, 10, NoLock, "pg_type");
		Assert(NULL != inmemrel);
	}
	InMemHeap_Insert(inmemrel, typetuple, TRUE, MASTER_CONTENT_ID);
	/*
	 * TODO:
	 * 1) to check typelem for array type
	 * 2) to check referenced tuple in pg_proc
	 * 3) to check text type attributes
	 */
	ReleaseSysCache(typetuple);


	/* collect pg_attribute info */

	inmemrel = OidGetInMemHeapRelation(AttributeRelationId);
	if (NULL == inmemrel)
	{
		inmemrel = InMemHeap_Create(AttributeRelationId, NULL, FALSE, 10,
				NoLock, "pg_attribute");
		Assert(NULL != inmemrel);
	}

	rel = heap_open(AttributeRelationId, AccessShareLock);
	Assert(NULL != rel);

	ScanKeyInit(&skey, Anum_pg_attribute_attrelid, BTEqualStrategyNumber,
			F_OIDEQ, ObjectIdGetDatum(oid));
	scandesc = systable_beginscan(rel, InvalidOid, FALSE, SnapshotNow, 1,
			&skey);
	attrtuple = systable_getnext(scandesc);
	while (attrtuple)
	{
		InMemHeap_Insert(inmemrel, attrtuple, TRUE, -1);
		attrtuple = systable_getnext(scandesc);
	}
	systable_endscan(scandesc);

	heap_close(rel, AccessShareLock);

	/* collect pg_attrdef info */
	inmemrel = OidGetInMemHeapRelation(AttrDefaultRelationId);
	if (NULL == inmemrel)
	{
		inmemrel = InMemHeap_Create(AttrDefaultRelationId, NULL, FALSE, 10,
				NoLock, "pg_attrdef");
		Assert(NULL != inmemrel);
	}

	rel = heap_open(AttrDefaultRelationId, AccessShareLock);
	Assert(NULL != rel);

	ScanKeyInit(&skey, Anum_pg_attrdef_adrelid, BTEqualStrategyNumber,
			F_OIDEQ, ObjectIdGetDatum(oid));
	scandesc = systable_beginscan(rel, InvalidOid, FALSE, SnapshotNow, 1,
			&skey);
	attrdeftuple = systable_getnext(scandesc);
	while (attrdeftuple)
	{
		InMemHeap_Insert(inmemrel, attrdeftuple, TRUE, MASTER_CONTENT_ID);
		attrdeftuple = systable_getnext(scandesc);
	}
	systable_endscan(scandesc);

	heap_close(rel, AccessShareLock);

	/* collect pg_constraint info */
	inmemrel = OidGetInMemHeapRelation(ConstraintRelationId);
	if (NULL == inmemrel)
	{
		inmemrel = InMemHeap_Create(ConstraintRelationId, NULL, FALSE, 10,
				NoLock, "pg_constraint");
		Assert(NULL != inmemrel);
	}

	rel = heap_open(ConstraintRelationId, AccessShareLock);
	Assert(NULL != rel);

	ScanKeyInit(&skey, Anum_pg_constraint_conrelid, BTEqualStrategyNumber,
			F_OIDEQ, ObjectIdGetDatum(oid));
	scandesc = systable_beginscan(rel, InvalidOid, FALSE, SnapshotNow, 1,
			&skey);
	constuple = systable_getnext(scandesc);
	while (constuple)
	{
		InMemHeap_Insert(inmemrel, constuple, TRUE, MASTER_CONTENT_ID);
		constuple = systable_getnext(scandesc);
	}
	systable_endscan(scandesc);

	heap_close(rel, AccessShareLock);

	ReleaseSysCache(classtuple);

}

/*
 * parse fast_sequence for dispatch
 */
static void
prepareDispatchedCatalogFastSequence(Relation rel, Oid objid,
		int64 objmod)
{

	InMemHeapRelation inmemrel;
	SysScanDesc scanDesc;
	HeapTuple tuple;
	Datum contentid;

	ScanKeyData scanKeys[2];

	ScanKeyInit(&scanKeys[0], Anum_gp_fastsequence_objid, BTEqualStrategyNumber,
			F_OIDEQ, ObjectIdGetDatum(objid));

	ScanKeyInit(&scanKeys[1], Anum_gp_fastsequence_objmod,
			BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(objmod));

	scanDesc = systable_beginscan(rel, InvalidOid, FALSE, SnapshotNow, 2,
			scanKeys);

	inmemrel = OidGetInMemHeapRelation(rel->rd_id);
	if (NULL == inmemrel)
	{
		inmemrel = InMemHeap_Create(rel->rd_id, NULL, FALSE, 10, NoLock, "gp_fastsequence");
		Assert(NULL != inmemrel);
	}

	while (NULL != (tuple = systable_getnext(scanDesc)))
	{
		bool isNull;

		contentid = heap_getattr(tuple,
				Anum_gp_fastsequence_contentid, rel->rd_att,
				&isNull);

		if (isNull)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("got an invalid contentid: NULL")));

		InMemHeap_Insert(inmemrel, tuple, TRUE, DatumGetInt32(contentid));
	}

	systable_endscan(scanDesc);
}

/*
 * parse pg_appendonly for dispatch
 */
static void
prepareDispatchedCatalogGpAppendOnly(Oid relid, Oid *segrelid, Oid *segidxid)
{
	Relation pg_appendonly_rel;
	TupleDesc pg_appendonly_dsc;
	HeapTuple tuple;
	ScanKeyData key;
	SysScanDesc aoscan;
	InMemHeapRelation inmemrel;

	bool isNull;
	Datum temp;

	/*
	 * Check the pg_appendonly relation to be certain the ao table
	 * is there.
	 */
	pg_appendonly_rel = heap_open(AppendOnlyRelationId, AccessShareLock);
	pg_appendonly_dsc = RelationGetDescr(pg_appendonly_rel);

	ScanKeyInit(&key, Anum_pg_appendonly_relid, BTEqualStrategyNumber, F_OIDEQ,
			ObjectIdGetDatum(relid));

	aoscan = systable_beginscan(pg_appendonly_rel, AppendOnlyRelidIndexId, TRUE,
			SnapshotNow, 1, &key);

	tuple = systable_getnext(aoscan);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("missing pg_appendonly entry for relation \"%s\"", get_rel_name(relid))));


	inmemrel = OidGetInMemHeapRelation(AppendOnlyRelationId);
	if (NULL == inmemrel)
	{
		inmemrel = InMemHeap_Create(AppendOnlyRelationId, NULL, FALSE, 10,
				NoLock, "pg_appendonly");
		Assert(NULL != inmemrel);
	}
	InMemHeap_Insert(inmemrel, tuple, TRUE, MASTER_CONTENT_ID);


	temp = heap_getattr(tuple, Anum_pg_appendonly_segrelid, pg_appendonly_dsc,
			&isNull);

	if (isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("got an invalid segrelid: NULL")));

	*segrelid = DatumGetObjectId(temp);

	temp = heap_getattr(tuple, Anum_pg_appendonly_segidxid, pg_appendonly_dsc,
			&isNull);

	if (isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("got an invalid segidxid: NULL")));

	*segidxid = DatumGetObjectId(temp);


	/* Finish up scan and close pg_appendonly catalog. */
	systable_endscan(aoscan);

	heap_close(pg_appendonly_rel, AccessShareLock);

}

/*
 * parse AO/CO relation range table and collect metadata used for QE
 * add them to in-memory heap table for dispatcher.
 */
void
prepareDispatchedCatalogForAoCo(Relation rel)
{
	Relation ao_seg_rel;
	Relation fast_seq_rel;
	HeapTuple pg_aoseg_tuple;
	InMemHeapRelation inmemrel;

	SysScanDesc aosegScanDesc;

	Oid segrelid, segidxid;

	Assert((RelationIsAoRows(rel) || RelationIsAoCols(rel)));

	/*
	 * 1, add tuple in pg_appendonly
	 */
	prepareDispatchedCatalogGpAppendOnly(rel->rd_id, &segrelid, &segidxid);

	/*
	 * 2, add pg_aoseg_XXX
	 */
	if (segrelid != InvalidOid)
	{
		/*
		 * add pg_aoseg_XXX's metdata
		 */
		prepareDispatchedCatalogObject(segrelid);

		/*
		 * add pg_aoseg_XXX's content
		 */
		ao_seg_rel = heap_open(segrelid, AccessShareLock);

		inmemrel = OidGetInMemHeapRelation(segrelid);
		if (NULL == inmemrel)
		{
			inmemrel = InMemHeap_Create(segrelid, NULL, FALSE, 10, NoLock, RelationGetRelationName(ao_seg_rel));
			Assert(NULL != inmemrel);
		}

		aosegScanDesc = systable_beginscan(ao_seg_rel, InvalidOid, FALSE,
				SnapshotNow, 0, NULL);

		fast_seq_rel = heap_open(FastSequenceRelationId, AccessShareLock);

		while (NULL != (pg_aoseg_tuple = systable_getnext(aosegScanDesc)))
		{
			bool isNull;

			Datum segno, contentid;

			segno = heap_getattr(pg_aoseg_tuple, 1, ao_seg_rel->rd_att,
					&isNull);

			if (isNull)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("got an invalid segno: NULL")));

			if (rel->rd_rel->relstorage == RELSTORAGE_AOROWS)
			{
				contentid = heap_getattr(pg_aoseg_tuple,
						Anum_pg_aoseg_XXX_content, ao_seg_rel->rd_att, &isNull);
			}
			else
			{
				Assert(rel->rd_rel->relstorage == RELSTORAGE_AOCOLS);
				contentid = heap_getattr(pg_aoseg_tuple,
						Anum_pg_aocsseg_XXX_content, ao_seg_rel->rd_att, &isNull);
			}

			if (isNull)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("got an invalid contentid: NULL")));

			InMemHeap_Insert(inmemrel, pg_aoseg_tuple, TRUE, DatumGetInt32(contentid));

			/*
			 * 3, add gp_fastsequence
			 */
			prepareDispatchedCatalogFastSequence(fast_seq_rel, segrelid,
					DatumGetObjectId(segno));
		}

		systable_endscan(aosegScanDesc);

		heap_close(fast_seq_rel, AccessShareLock);
		heap_close(ao_seg_rel, AccessShareLock);
	}
	/*TODO index for ao/cs*/
}


/*
 * parse AO/CO relation range table and collect metadata used for QE
 * add them to in-memory heap table for dispatcher.
 */
static void
prepareDispatchedCatalogExternalTable(Relation rel)
{
	Relation pg_exttable_rel;
	TupleDesc pg_exttable_dsc;
	HeapTuple tuple;
	ScanKeyData key;
	SysScanDesc extscan;
	InMemHeapRelation inmemrel;

	pg_exttable_rel = heap_open(ExtTableRelationId, RowExclusiveLock);
	pg_exttable_dsc = RelationGetDescr(pg_exttable_rel);

	ScanKeyInit(&key, Anum_pg_exttable_reloid, BTEqualStrategyNumber, F_OIDEQ,
			ObjectIdGetDatum(rel->rd_id));

	extscan = systable_beginscan(pg_exttable_rel, ExtTableReloidIndexId, true,
			SnapshotNow, 1, &key);

	tuple = systable_getnext(extscan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("missing pg_exttable entry for relation \"%s\"", RelationGetRelationName(rel))));

	inmemrel = OidGetInMemHeapRelation(ExtTableRelationId);
	if (NULL == inmemrel)
	{
		inmemrel = InMemHeap_Create(ExtTableRelationId, NULL, FALSE, 10, NoLock, "pg_exttable");
		Assert(NULL != inmemrel);
	}

	InMemHeap_Insert(inmemrel, tuple, TRUE, DatumGetInt32(MASTER_CONTENT_ID));

	systable_endscan(extscan);

	heap_close(pg_exttable_rel, RowExclusiveLock);
}


/*
 * parse Relation range table and collect metadata used for QE
 * add them to in-memory heap table for dispatcher.
 */
static void
prepareDispatchedCatalogRelation(RangeTblEntry *rte, HTAB *rels)
{
	Relation rel = heap_open(rte->relid, AccessShareLock);

	Assert(NULL != rte && rte->rtekind == RTE_RELATION);
	switch (rel->rd_rel->relstorage)
	{
	case RELSTORAGE_HEAP:
		/*
		 * We need to support this because gp_dist_random with catalog
		 * table comes here.
		 */
		prepareDispatchedCatalogObject(rte->relid);
		break;
	case RELSTORAGE_AOROWS:
	case RELSTORAGE_AOCOLS:
		{
			List		*children = NIL;
			ListCell	*child;

			/* Dispatch all of inherits children: SELECT/DML should be processed differently. */
			children = find_all_inheritors(rte->relid);
			foreach(child, children)
			{
				Relation crel = heap_open(lfirst_oid(child), AccessShareLock);
				bool	found = false;

				hash_search(rels, &lfirst_oid(child), HASH_ENTER, &found);

				if (!found)
				{
					prepareDispatchedCatalogObject(lfirst_oid(child));
					prepareDispatchedCatalogForAoCo(crel);
				}

				heap_close(crel, AccessShareLock);
			}
			list_free(children);
		}	
		break;
	case RELSTORAGE_EXTERNAL:
		prepareDispatchedCatalogObject(rte->relid);
		prepareDispatchedCatalogExternalTable(rel);
		break;
	case RELSTORAGE_VIRTUAL:
		prepareDispatchedCatalogObject(rte->relid);
		break;

	case RELSTORAGE_FOREIGN:
		/* TODO */
		elog(ERROR, "not implemented relstorage: %c", rel->rd_rel->relstorage);
		break;
	default:
		Assert(!"never get here");
		break;
	}
	heap_close(rel, AccessShareLock);
}

/*
 * parse range table and collect metadata used for QE,
 * add them to in-memory heap table for dispatcher.
 */
void
prepareDispatchedCatalog(List * rtable)
{
	ListCell *lc;

	HASHCTL info;
	HTAB *rels = NULL;

	Assert(NULL != rtable);

	MemSet(&info, 0, sizeof(info));

	info.hash = oid_hash;
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(Oid);

	rels = hash_create("all relations", 10, &info,
			HASH_FUNCTION | HASH_ELEM);

	foreach(lc, rtable)
	{
		RangeTblEntry *rte = lfirst(lc);
		switch (rte->rtekind)
		{
		case RTE_RELATION: 		/* ordinary relation reference */
			prepareDispatchedCatalogRelation(rte, rels);
			break;

		case RTE_JOIN: 			/* join */
		case RTE_SUBQUERY: 		/* subquery in FROM */
		case RTE_VOID: 			/* CDB: deleted RTE */
		case RTE_VALUES: 		/* VALUES (<exprlist>), (<exprlist>): ... */
		case RTE_FUNCTION: 		/* function in FROM */
			break;

		case RTE_SPECIAL: 		/* special rule relation (NEW or OLD) */
		case RTE_CTE: 			/* CommonTableExpr in FROM */
		case RTE_TABLEFUNCTION: /* CDB: Functions over multiset input */
			/* TODO */
			elog(ERROR, "not implemented");
			break;
		default:
			Assert(!"never get here");
			break;
		}
	}

	hash_destroy(rels);
}

