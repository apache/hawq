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
 * cdbinmemheapam.c
 * goh in-memory heap table access method
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/valid.h"
#include "catalog/catalog.h"
#include "catalog/gp_policy.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "cdb/cdbinmemheapam.h"
#include "cdb/cdbvars.h"
#include "nodes/memnodes.h"
#include "nodes/parsenodes.h"
#include "storage/lock.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/inval.h"

HTAB* OidInMemMappings[INMEM_MAPPINGS_SIZE] = { NULL };
static MemoryContext InMemMappingCxt[INMEM_MAPPINGS_SIZE] = { NULL };
static const char* InMemMappingNames[INMEM_MAPPINGS_SIZE] = { "OidInMemHeapMapping", "OidInMemOnlyMapping"};

static int
InMemHeap_Find(InMemHeapRelation relation, ItemPointer otid);

static bool
InMemHeap_GetNextIndex(InMemHeapScanDesc scan, ScanDirection direction);

static void 
CheckInMemConstraintsPgNamespace(InMemHeapRelation relation, HeapTuple newTuple);

static void 
CheckInMemConstraintsPgClass(InMemHeapRelation relation, HeapTuple newTuple);

static void
CheckInMemConstraintsPgType(InMemHeapRelation relation, HeapTuple newTuple);

static void 
CheckInMemConstraintsPgAttribute(InMemHeapRelation relation, HeapTuple newTuple);

static void
CheckInMemConstraintsPgExttable(InMemHeapRelation relation, HeapTuple newTuple);

static void
CheckInMemConstraintsGpDistributionPolicy(InMemHeapRelation relation, HeapTuple newTuple);

typedef void (*CheckConstraintsFn) (InMemHeapRelation relation, HeapTuple newTuple);

/*
 * init relid to in-memory table mapping
 */
void
InitOidInMemHeapMapping(long initSize, MemoryContext memcxt, InMemMappingType mappingType)
{
    HASHCTL info;

    Assert(mappingType < INMEM_MAPPINGS_SIZE);
    Assert(NULL == OidInMemMappings[mappingType]);

    info.hcxt = memcxt;
    info.hash = oid_hash;
    info.keysize = sizeof(Oid);
    info.entrysize = sizeof(struct OidInMemHeapMappingEntry);

    OidInMemMappings[mappingType] = hash_create(InMemMappingNames[mappingType], initSize, &info,
            HASH_CONTEXT | HASH_FUNCTION | HASH_ELEM);

    Assert(NULL != OidInMemMappings[mappingType]);

    InMemMappingCxt[mappingType] = memcxt;
}

/*
 * cleanup relid to in-memory table mapping
 */
void
CleanupOidInMemHeapMapping(InMemMappingType mappingType)
{
	Assert(mappingType < INMEM_MAPPINGS_SIZE);

    if (NULL == OidInMemMappings[mappingType])
    {
        return;
    }

    hash_destroy(OidInMemMappings[mappingType]);
    OidInMemMappings[mappingType] = NULL;

    InMemMappingCxt[mappingType] = NULL;
}

/*
 * get a in-memory table by relid,
 */
InMemHeapRelation
OidGetInMemHeapRelation(Oid relid, InMemMappingType mappingType)
{
	bool found = FALSE;
	struct OidInMemHeapMappingEntry *retval;

	Assert(mappingType < INMEM_MAPPINGS_SIZE);

	if (NULL != OidInMemMappings[mappingType])
	{
		retval = hash_search(OidInMemMappings[mappingType], &relid, HASH_FIND, &found);
		if (NULL != retval)
		{
			return retval->rel;
		}
	}

	return NULL ;
}

struct MemHeapHashIndexEntry {
    Oid    key;
    List  *values;
};

typedef struct MemHeapHashIndexEntry MemHeapHashIndexEntry;

/*
 * create a in-memory heap table with Oid.
 * the in-memory table and all its tuples are in memcxt memory context.
 * at first, initSize tuples space will be alloced in the tuple,
 * and will re-alloc at runtime if inserting more tuples.
 */
InMemHeapRelation
InMemHeap_Create(Oid relid, Relation rel, bool ownrel,
        int32 initSize, LOCKMODE lock, const char *relname, bool createIndex, int keyAttrno,
		InMemMappingType mappingType)
{
    bool found = FALSE;
    struct OidInMemHeapMappingEntry *entry;
    InMemHeapRelation memheap = NULL;
    MemoryContext oldcxt;

    Assert(mappingType < INMEM_MAPPINGS_SIZE);
    Assert(NULL != OidInMemMappings[mappingType]);

    hash_search(OidInMemMappings[mappingType], &relid, HASH_FIND, &found);

    if (found)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("in-memory table with Oid = %d already exist.", relid)));
    }

    Assert(MemoryContextIsValid(InMemMappingCxt[mappingType]));

    oldcxt = CurrentMemoryContext;
    CurrentMemoryContext = InMemMappingCxt[mappingType];

    memheap = palloc(sizeof(InMemHeapRelationData));

    memheap->memcxt = InMemMappingCxt[mappingType];
    memheap->relid = relid;
    memheap->tupsize = 0;
    memheap->tupmaxsize = initSize;
    memheap->tuples = NULL;
    memheap->rellock = lock;
    memheap->ownrel = ownrel;
    memheap->rel = rel;
    memheap->hashIndex = NULL;
    memheap->keyAttrno = 0;
    StrNCpy(memheap->relname, relname, NAMEDATALEN);

    if (createIndex)
    {
        HASHCTL         info;
        memheap->keyAttrno = keyAttrno;

        /* Set key and entry sizes. */
        MemSet(&info, 0, sizeof(info));
        info.keysize = sizeof(Oid);
        info.entrysize = sizeof(MemHeapHashIndexEntry);
        info.hash = oid_hash;
        info.hcxt = memheap->memcxt;

        memheap->hashIndex = hash_create("InMemHeap hash index",
                    10, &info,
                    HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    }

    initSize = initSize > 1 ? initSize : 1;
    memheap->tuples = palloc(sizeof(InMemHeapTupleData) * initSize);

    entry = hash_search(OidInMemMappings[mappingType], &relid, HASH_ENTER, &found);

    entry->relid = relid;
    entry->rel = memheap;

    CurrentMemoryContext = oldcxt;

    return memheap;
}

/*
 * drop a in-memory heap table.
 */
void
InMemHeap_Drop(Oid relid, InMemMappingType mappingType)
{
    bool found = FALSE;
    struct OidInMemHeapMappingEntry *entry = NULL;

    Assert(mappingType < INMEM_MAPPINGS_SIZE);
    Assert(NULL != OidInMemMappings[mappingType]);

    entry = hash_search(OidInMemMappings[mappingType], &relid, HASH_FIND, &found);

    if (NULL == entry)
    {
        return;
    }

    Assert(NULL != entry->rel);

    if (entry->rel->hashIndex)
    {
        hash_destroy(entry->rel->hashIndex);
        entry->rel->hashIndex = NULL;
    }

    if (entry->rel->tuples)
    {
        int i;
        HeapTuple tup;
        for (i = 0; i < entry->rel->tupsize; ++i)
        {
            tup = entry->rel->tuples[i].tuple;
            if (tup)
            {
                pfree(tup);
            }
        }
        pfree(entry->rel->tuples);
    }

    if (entry->rel->ownrel && entry->rel->rel)
    {
        heap_close(entry->rel->rel, entry->rel->rellock);
    }

    pfree(entry->rel);

    hash_search(OidInMemMappings[mappingType], &relid, HASH_REMOVE, &found);
}

/*
 * drop all in-memory tables of given mapping
 */
void
InMemHeap_DropAll(InMemMappingType mappingType)
{
	HASH_SEQ_STATUS scan;
	struct OidInMemHeapMappingEntry *entry;
	Assert(mappingType < INMEM_MAPPINGS_SIZE);

	if (NULL == OidInMemMappings[mappingType])
	{
		return;
	}

	elog(DEBUG1, "Dropping in memory mapping %s", InMemMappingNames[mappingType]);

	hash_seq_init(&scan, OidInMemMappings[mappingType]);

	while (!!(entry = (struct OidInMemHeapMappingEntry *) hash_seq_search(&scan)))
	{
		InMemHeap_Drop(entry->relid, mappingType);
	}
}

/*
 * begin a in-memory heap table scan.
 */
InMemHeapScanDesc
InMemHeap_BeginScan(InMemHeapRelation memheap, int nkeys,
        ScanKey key, AttrNumber *orig_attnos, bool inmemonly)
{
    InMemHeapScanDesc scan = palloc0(sizeof (InMemHeapScanDescData));
    Assert(NULL != scan);

    scan->rs_rd = memheap;
    scan->rs_nkeys = nkeys;
    scan->rs_index = -1;
    
    if (nkeys > 0)
    {
        scan->rs_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
    }
    else
    {
        scan->rs_key = NULL;
    }
 
    if (key != NULL)
    {
        memcpy(scan->rs_key, key, scan->rs_nkeys * sizeof(ScanKeyData));
        if (NULL != orig_attnos)
        {
        	/* restore original key attribute numbers as the they are invalid in the passed array of keys */
        	/* note: the scankey struct contains the attnos of the keys in the index scan, and here we need to
        	 * refer to the original ones from the heap relation
        	 */
        	int i = 0;
        	for (i = 0; i < nkeys; i++)
        	{
        		scan->rs_key[i].sk_attno = orig_attnos[i];
        	}
        }
        /*
         * test if we can use hash index
         */
        if (memheap->hashIndex)
        {
            int i;
            for (i = 0 ; i < nkeys; ++i)
            {
                if(scan->rs_key[i].sk_attno == memheap->keyAttrno
                        && scan->rs_key[i].sk_strategy == BTEqualStrategyNumber)
                {
                    /*
                     * we have a hash index on this attribute
                     */
                    scan->hashIndexOk = TRUE;
                    scan->hashKeyIndexInScanKey = i;
                    break;
                }
            }
        }
    }

    if (!inmemonly && (NULL != scan->rs_rd->rel))
    {
         /*
         * GPSQL-483, GPSQL-486
         *
         * When a QE exists on the master, we still want to
         * leverage metadata that was extracted for query execution via metadata dispatch.
         * (Otherwise, we'd have to reintroduce snapshot propagation for some sort of
         * bastardized DTM that exists to coordinate the dispatcher with a master QE.) 
         * In leveraging dispatched metadata on a master QE, we also need to ensure that
         * we can't read duplicate metadata from the heap itself. To accomplish this,
         * we constrain the fallback heap scan to only metadata which could not have
         * been dispatched, namely the builtin catalog data. Thus, we add
         * OID < FirstNormalOid to the scan key. 
         */

        int heap_nkeys = nkeys + 1;
        ScanKey heap_key = (ScanKey) palloc0(sizeof(ScanKeyData) * heap_nkeys);
        /* Copy the given input keys */
        if (NULL != key)
        {
           memcpy(heap_key, scan->rs_key, nkeys * sizeof(ScanKeyData));
        }

        ScanKeyInit(&heap_key[heap_nkeys -1],
                    ObjectIdAttributeNumber,
                    BTLessStrategyNumber, F_OIDLT,
                    ObjectIdGetDatum(FirstNormalObjectId));

        scan->hscan = heap_beginscan(scan->rs_rd->rel, SnapshotNow,
                heap_nkeys, heap_key);

        if (NULL != heap_key)
        {
        	pfree(heap_key);
        }
    }
    return scan;
}

/*
 * end a in-memory heap table scan.
 */
void
InMemHeap_EndScan(InMemHeapScanDesc scan)
{
    Assert(NULL != scan);

    if (NULL != scan->rs_key)
    {
        pfree(scan->rs_key);
    }

    if (NULL != scan->hscan)
    {
        heap_endscan(scan->hscan);
    }

    if (NIL != scan->indexReverseList)
    {
    	list_free(scan->indexReverseList);
    }

    pfree(scan);
}

/*
 * Increment scan->rs_index based on scan direction.
 * Returns false when scan reaches its end.
 */
static bool
InMemHeap_GetNextIndex(InMemHeapScanDesc scan, ScanDirection direction)
{
	if (BackwardScanDirection == direction)
	{
		if (-1 == scan->rs_index) /* scan beginning */
		{
			scan->rs_index = scan->rs_rd->tupsize;
		}
		scan->rs_index--;
		return (scan->rs_index > -1);
	}
	else
	{
		scan->rs_index++;
		return (scan->rs_index < scan->rs_rd->tupsize);
	}
}

/*
 * get next tuple in in-memory heap table.
 */
HeapTuple
InMemHeap_GetNext(InMemHeapScanDesc scan, ScanDirection direction)
{
    bool valid = true;

    InMemHeapTuple pmemtup = NULL;

    Assert(NULL != scan);

    if (scan->hashIndexOk)
    {
        if (FALSE == scan->indexScanInitialized)
        {
            Oid key;
            bool found;

            key = DatumGetObjectId(scan->rs_key[scan->hashKeyIndexInScanKey].sk_argument);

            MemHeapHashIndexEntry *entry;
            entry = (MemHeapHashIndexEntry *) hash_search(scan->rs_rd->hashIndex, &key,
                    HASH_FIND, &found);

            if (found)
            {
            	if (BackwardScanDirection == direction)
            	{
            		/* if direction is backward, reverse list */
            		scan->indexReverseList = list_reverse_ints(entry->values);
            		entry->values = scan->indexReverseList;
            	}
                scan->indexNext = list_head(entry->values);
            }
            else
            	scan->indexNext = NULL;

            scan->indexScanInitialized = TRUE;
            scan->indexScanKey = key;
        }

        for (; scan->indexNext != NULL;
                scan->indexNext = lnext(scan->indexNext))
        {
            int32 index = lfirst_int(scan->indexNext);

            elog(DEBUG1, "read index %d key %d for relation %s", index, scan->indexScanKey, scan->rs_rd->relname);

            pmemtup = &scan->rs_rd->tuples[index];
            HeapKeyTest(pmemtup->tuple, RelationGetDescr(scan->rs_rd->rel),
                    scan->rs_nkeys, scan->rs_key, &valid);

            if (!valid)
            {
                continue;
            }

            scan->rs_ctup = pmemtup->tuple;
            scan->indexNext = lnext(scan->indexNext);
            return scan->rs_ctup;
        }
    }
    else
    {
    	/* for backward scan, change direction of iterator */
    	while (InMemHeap_GetNextIndex(scan, direction))
        {
            pmemtup = &scan->rs_rd->tuples[scan->rs_index];

            Assert(NULL != pmemtup->tuple);

            if (scan->rs_key != NULL)
            {
                Assert(NULL != scan->rs_rd->rel);
                HeapKeyTest(pmemtup->tuple, RelationGetDescr(scan->rs_rd->rel),
                        scan->rs_nkeys, scan->rs_key, &valid);
            }

            if (!valid)
            {
                continue;
            }

            scan->rs_ctup = pmemtup->tuple;
            return scan->rs_ctup;
        }
    }

    /*
     * read from local read only heap table.
     */
    if (NULL != scan->hscan)
    {
        return heap_getnext(scan->hscan, direction);
    }

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
    {
        if (!memcmp(&relation->tuples[rc].tuple->t_self, otid,
                sizeof(ItemPointerData)))
        {
            break;
        }
    }
    return rc;
}

/*
 * insert a tuple into in-memory heap table.
 */
void
InMemHeap_Insert(InMemHeapRelation relation, HeapTuple tup, int contentid)
{
    InMemHeapTuple inmemtup;

    MemoryContext oldmem = CurrentMemoryContext;
    Assert(NULL != relation && NULL != tup);
    Assert(GP_ROLE_EXECUTE == Gp_role || -1 == contentid);
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
    inmemtup->flags = INMEM_HEAP_TUPLE_DISPATCHED;
    inmemtup->tuple = heaptuple_copy_to(tup, NULL, NULL);
    Assert(inmemtup->tuple != NULL);

    if (relation->hashIndex)
    {
        Oid key;
        bool isNull, found;

        key = DatumGetObjectId(
                heap_getattr(tup, relation->keyAttrno,
                        RelationGetDescr(relation->rel), &isNull));

        Insist(!isNull && "index key cannot be null");

        MemHeapHashIndexEntry *entry;
        entry = (MemHeapHashIndexEntry *) hash_search(relation->hashIndex, &key,
                HASH_ENTER, &found);

        if (!found)
        {
            entry->key = key;
            entry->values = NIL;
        }

        entry->values = lappend_int(entry->values, relation->tupsize);

        elog(DEBUG1, "add index %d key %d relation %s", relation->tupsize, key, relation->relname);
    }

    ++relation->tupsize;

    CurrentMemoryContext = oldmem;
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
     * not found, report error
     */
    if (pos >= relation->tupsize)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("update a tuple which does not exist,"
                                " relname = %s, relid = %u", relation->rel->rd_rel->relname.data,
                                relation->relid)));
    }

    Insist(relation->hashIndex == NULL && "cannot handle index in in-memory heap when update");

    /*
     * already in table
     */
    Assert(relation->tuples[pos].flags == INMEM_HEAP_TUPLE_DISPATCHED
            || relation->tuples[pos].flags == INMEM_HEAP_TUPLE_UPDATED);
    relation->tuples[pos].flags = INMEM_HEAP_TUPLE_UPDATED;

    target = heaptuple_copy_to(tup, NULL, NULL );

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
 * InMemHeap_CheckConstraints
 * 		Check uniqueness constraints for in-memory tuples upon insert
 */
void
InMemHeap_CheckConstraints(InMemHeapRelation relation, HeapTuple newTuple)
{
	Assert(NULL != relation); 
	Assert(NULL != newTuple);

	Oid relid = relation->relid;

	CheckConstraintsFn fn = NULL;

	switch (relid)
	{
		case NamespaceRelationId:
			fn = CheckInMemConstraintsPgNamespace;
			break;
		case RelationRelationId:
			fn = CheckInMemConstraintsPgClass;
			break;
		case TypeRelationId:
			fn = CheckInMemConstraintsPgType;
			break;
		case AttributeRelationId:
			fn = CheckInMemConstraintsPgAttribute;
			break;
		case ExtTableRelationId:
			fn = CheckInMemConstraintsPgExttable;
			break;
		case GpPolicyRelationId:
			fn = CheckInMemConstraintsGpDistributionPolicy;
			break;
		default:
			/* no constraint checking for relations other than:
			 * pg_namespace, pg_class, pg_type, pg_attribute,
			 * pg_exttable, gp_distribution_policy */
			return;
	}

	fn(relation, newTuple);
}

/*
 * CheckInMemConstraintsPgNamespace
 * 		Check uniqueness constraints for pg_namespace in-memory tuples upon insert
 */
static void
CheckInMemConstraintsPgNamespace(InMemHeapRelation relation, HeapTuple newTuple)
{
	Assert(NULL != newTuple);
	Assert(NULL != relation); 
	Assert(NULL != relation->rel);

	TupleDesc tupleDesc = relation->rel->rd_att;
	Oid nspdboidNew     = DatumGetObjectId(tuple_getattr(newTuple, tupleDesc, Anum_pg_namespace_nspdboid));
	char *nspnameNew    = DatumGetCString(tuple_getattr(newTuple, tupleDesc, Anum_pg_namespace_nspname));

	for (int i = 0; i < relation->tupsize; i++)
	{
		HeapTuple tuple = relation->tuples[i].tuple;
		Assert(NULL != tuple);

		insist_log(HeapTupleGetOid(tuple) != HeapTupleGetOid(newTuple), 
			"in-memory tuple with Oid = %d already exists in pg_namespace.", HeapTupleGetOid(tuple));

		Oid nspdboid  = DatumGetObjectId(tuple_getattr(tuple, tupleDesc, Anum_pg_namespace_nspdboid));
		char *nspname = DatumGetCString(tuple_getattr(tuple, tupleDesc, Anum_pg_namespace_nspname));
		size_t nspnameLen = strlen(nspname);

		insist_log(nspdboid != nspdboidNew ||
				   nspnameLen != strlen(nspnameNew) ||
				   0 != strncmp(nspname, nspnameNew, nspnameLen),
			"in-memory tuple with nspname = %s and nspdboid = %d already exists in pg_namespace.", nspname, nspdboid);
	}
}

/*
 * CheckInMemConstraintsPgClass
 * 		Check uniqueness constraints for pg_class in-memory tuples upon insert
 */
static void
CheckInMemConstraintsPgClass(InMemHeapRelation relation, HeapTuple newTuple)
{
	Assert(NULL != newTuple);
	Assert(NULL != relation); 
	Assert(NULL != relation->rel);

	TupleDesc tupleDesc = relation->rel->rd_att;
	Oid relnamespaceNew = DatumGetObjectId(tuple_getattr(newTuple, tupleDesc, Anum_pg_class_relnamespace));
	char *relnameNew    = DatumGetCString(tuple_getattr(newTuple, tupleDesc, Anum_pg_class_relname));

	for (int i = 0; i < relation->tupsize; i++)
	{
		HeapTuple tuple = relation->tuples[i].tuple;
		Assert(NULL != tuple);

		insist_log(HeapTupleGetOid(tuple) != HeapTupleGetOid(newTuple), 
			"in-memory tuple with Oid = %d already exists in pg_class.", HeapTupleGetOid(tuple));

		Oid relnamespace = DatumGetObjectId(tuple_getattr(tuple, tupleDesc, Anum_pg_class_relnamespace));
		char *relname    = DatumGetCString(tuple_getattr(tuple, tupleDesc, Anum_pg_class_relname));
		size_t relnameLen = strlen(relname);
		
		insist_log(relnamespace != relnamespaceNew ||
				   relnameLen != strlen(relnameNew) ||
				   0 != strncmp(relname, relnameNew, relnameLen),
			"in-memory tuple with relname = %s and relnamespace = %d already exists in pg_class.", relname, relnamespace);
	}
}

/*
 * CheckInMemConstraintsPgType
 * 		Check uniqueness constraints for pg_type in-memory tuples upon insert
 */
static void
CheckInMemConstraintsPgType(InMemHeapRelation relation, HeapTuple newTuple)
{
	Assert(NULL != newTuple);
	Assert(NULL != relation);
	Assert(NULL != relation->rel);

	TupleDesc tupleDesc = relation->rel->rd_att;
	Oid relnamespaceNew = DatumGetObjectId(tuple_getattr(newTuple, tupleDesc, Anum_pg_type_typnamespace));
	char *typnameNew    = DatumGetCString(tuple_getattr(newTuple, tupleDesc, Anum_pg_type_typname));

	for (int i = 0; i < relation->tupsize; i++)
	{
		HeapTuple tuple = relation->tuples[i].tuple;
		Assert(NULL != tuple);

		insist_log(HeapTupleGetOid(tuple) != HeapTupleGetOid(newTuple),
					"in-memory tuple with Oid = %d already exists in pg_type.", HeapTupleGetOid(tuple));

		Oid relnamespace = DatumGetObjectId(tuple_getattr(tuple, tupleDesc, Anum_pg_type_typnamespace));
		char *typname    = DatumGetCString(tuple_getattr(tuple, tupleDesc, Anum_pg_type_typname));
		size_t typnameLen = strlen(typname);

		insist_log(relnamespace != relnamespaceNew ||
				   typnameLen != strlen(typnameNew) ||
				   0 != strncmp(typname, typnameNew, typnameLen),
				"in-memory tuple with typname = %s and typnamespace = %d already exists in pg_type.", typname, relnamespace);
	}
}

/*
 * CheckInMemConstraintsPgAttribute
 * 		Check uniqueness constraints for pg_attribute in-memory tuples upon insert
 */
static void
CheckInMemConstraintsPgAttribute(InMemHeapRelation relation, HeapTuple newTuple)
{
	Assert(NULL != newTuple);
	Assert(NULL != relation); 
	Assert(NULL != relation->rel);

	TupleDesc tupleDesc = relation->rel->rd_att;
	Oid attrelidNew     = DatumGetObjectId(tuple_getattr(newTuple, tupleDesc, Anum_pg_attribute_attrelid));
	char *attnameNew    = DatumGetCString(tuple_getattr(newTuple, tupleDesc, Anum_pg_attribute_attname));
	AttrNumber attnoNew = DatumGetInt16((tuple_getattr(newTuple, tupleDesc, Anum_pg_attribute_attnum)));

	for (int i = 0; i < relation->tupsize; i++)
	{
		HeapTuple tuple = relation->tuples[i].tuple;
		Assert(NULL != tuple);

		Oid attrelid     = DatumGetObjectId(tuple_getattr(tuple, tupleDesc, Anum_pg_attribute_attrelid));
		char *attname    = DatumGetCString(tuple_getattr(tuple, tupleDesc, Anum_pg_attribute_attname));
		AttrNumber attno = DatumGetInt16((tuple_getattr(tuple, tupleDesc, Anum_pg_attribute_attnum)));
		size_t attnameLen = strlen(attname);

		if (attrelid != attrelidNew)
		{
			/* attributes belong to different relations */
			continue;
		}

		insist_log(attno != attnoNew,
			"in-memory tuple with attrelid = %d and attno = %d already exists in pg_attribute.", attrelid, attno);

		insist_log((attnameLen != strlen(attnameNew)) ||
				   (0 != strncmp(attname, attnameNew, attnameLen)),
			"in-memory tuple with attrelid = %d and attname = %s already exists in pg_attribute.", attrelid, attname);
	}
}

/*
 * CheckInMemConstraintsGpDistributionPolicy
 * 		Check uniqueness constraints for gp_distribution_policy in-memory tuples upon insert
 */
static void
CheckInMemConstraintsGpDistributionPolicy(InMemHeapRelation relation, HeapTuple newTuple)
{
	Assert(NULL != newTuple);
	Assert(NULL != relation);
	Assert(NULL != relation->rel);

	TupleDesc tupleDesc = relation->rel->rd_att;
	Oid reloidNew = DatumGetObjectId(tuple_getattr(newTuple, tupleDesc, Anum_gp_policy_localoid));

	for (int i = 0; i < relation->tupsize; i++)
	{
		HeapTuple tuple = relation->tuples[i].tuple;
		Assert(NULL != tuple);

		Oid reloid = DatumGetObjectId(tuple_getattr(tuple, tupleDesc, Anum_gp_policy_localoid));

		insist_log(reloidNew != reloid,
				   "in-memory tuple with localoid = %d already exists in gp_distribution_policy.", reloid);
	}
}

/*
 * CheckInMemConstraintsPgExttable
 * 		Check uniqueness constraints for pg_exttable in-memory tuples upon insert
 */
static void
CheckInMemConstraintsPgExttable(InMemHeapRelation relation, HeapTuple newTuple)
{
	Assert(NULL != newTuple);
	Assert(NULL != relation);
	Assert(NULL != relation->rel);

	TupleDesc tupleDesc = relation->rel->rd_att;
	Oid reloidNew = DatumGetObjectId(tuple_getattr(newTuple, tupleDesc, Anum_pg_exttable_reloid));

	for (int i = 0; i < relation->tupsize; i++)
	{
		HeapTuple tuple = relation->tuples[i].tuple;
		Assert(NULL != tuple);

		Oid reloid = DatumGetObjectId(tuple_getattr(tuple, tupleDesc, Anum_pg_exttable_reloid));

		insist_log(reloidNew != reloid,
				   "in-memory tuple with reloid = %d already exists in pg_exttable.", reloid);
	}
}

/* ----------------
 *      tuple_getattr
 *
 *      Extracts an attribute from a HeapTuple given its attnum and
 *      returns it as a Datum.
 *
 *      <tuple> is the pointer to the heap tuple.  <attnum> is the attribute
 *      number of the column (field) caller wants.  <tupleDesc> is a
 *      pointer to the structure describing the row and all its fields.
 *
 * ----------------
 */
Datum
tuple_getattr(HeapTuple tuple, TupleDesc tupleDesc, int attnum)
{
	Assert(NULL != tupleDesc);
	Assert(NULL != tuple);
	bool isnull;
	Datum attr = heap_getattr(tuple, attnum, tupleDesc, &isnull);
	insist_log(!isnull, "attribute cannot be null");
	return attr;
}
