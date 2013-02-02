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

#include "access/genam.h"
#include "access/valid.h"
#include "catalog/catalog.h"
#include "cdb/cdbinmemheapam.h"
#include "cdb/cdbvars.h"
#include "nodes/memnodes.h"
#include "nodes/parsenodes.h"
#include "storage/lock.h"
#include "utils/hsearch.h"


HTAB *OidInMemHeapMapping = NULL;

static MemoryContext InMemHeapTableCxt = NULL;

static int
InMemHeap_Find(InMemHeapRelation relation, ItemPointer otid);

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
    if (!OidInMemHeapMapping)
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

    return NULL ;
}

/*
 * create a in-memory heap table with Oid.
 * the in-memory table and all its tuples are in memcxt memory context.
 * at first, initSize tuples space will be alloced in the tuple,
 * and will re-alloc at runtime if inserting more tuples.
 */
InMemHeapRelation
InMemHeap_Create(Oid relid, Relation rel, bool ownrel,
        int32 initSize, LOCKMODE lock, const char *relname)
{
    bool found = FALSE;
    struct OidInMemHeapMappingEntry *entry;
    InMemHeapRelation memheap = NULL;
    MemoryContext oldcxt;

    Assert(NULL != OidInMemHeapMapping);

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
InMemHeap_GetNext(InMemHeapScanDesc scan)
{

    bool valid;

    InMemHeapTuple pmemtup = NULL;

    Assert(NULL != scan);

    for (scan->rs_index++; scan->rs_index < scan->rs_rd->tupsize;
            scan->rs_index++)
    {
        pmemtup = &scan->rs_rd->tuples[scan->rs_index];

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

    /*
     * read from local read only heap table.
     */
    if (scan->hscan)
        return heap_getnext(scan->hscan, ForwardScanDirection);

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
        if (!memcmp(&relation->tuples[rc].tuple->t_self, otid,
                sizeof(ItemPointerData)))
            break;
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
    Assert(Gp_role == GP_ROLE_EXECUTE);
    Assert(NULL != relation && NULL != tup);
    Assert(ItemPointerIsValid(&tup->t_self));

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
    inmemtup->tuple = heaptuple_copy_to(tup, NULL, NULL );
    Assert(inmemtup->tuple != NULL);

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

