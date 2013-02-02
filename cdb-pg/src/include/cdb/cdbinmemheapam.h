/*-------------------------------------------------------------------------
 *
 * cdbinmemheapam.h
 *	 goh in-memory heap table access method
 *
 * Copyright (c) 2007-2013, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */

#ifndef _INMEMHEAP_H_
#define _INMEMHEAP_H_

#include "access/htup.h"
#include "access/relscan.h"

#include "utils/relcache.h"
#include "access/sdir.h"
#include "access/heapam.h"
#include "nodes/primnodes.h"

enum InMemHeapTupleFlag
{
    INMEM_HEAP_TUPLE_DISPATCHED = 0, INMEM_HEAP_TUPLE_UPDATED, /* the tuple was dispatched and updated by QE */
};
typedef enum InMemHeapTupleFlag InMemHeapTupleFlag;

struct InMemHeapTupleData
{
    HeapTuple tuple; /* heap tuple */
    int32 contentid; /* contend id for this tuple, -1 means valid for all segments */
    uint8 flags; /* tuple flag such as INMEM_HEAP_TUPLE_DELETED */
};
typedef struct InMemHeapTupleData InMemHeapTupleData;

typedef struct InMemHeapTupleData * InMemHeapTuple;

struct InMemHeapRelationData
{
    MemoryContext memcxt;
    InMemHeapTuple tuples; /* a vector of InMemHeapTuple */
    Relation rel;
    LOCKMODE rellock;
    int32 tupsize;
    int32 tupmaxsize;
    Oid relid;
    char relname[NAMEDATALEN];
    bool ownrel;
};
typedef struct InMemHeapRelationData InMemHeapRelationData;
typedef struct InMemHeapRelationData * InMemHeapRelation;

extern HTAB * OidInMemHeapMapping;

struct OidInMemHeapMappingEntry
{
    Oid relid;
    InMemHeapRelation rel;
};

typedef struct InMemHeapScanDescData
{
    InMemHeapRelation rs_rd; /* heap relation descriptor */
    int rs_nkeys; /* number of scan keys */
    ScanKey rs_key; /* array of scan key descriptors */

    /* scan current state */
    HeapTuple rs_ctup; /* current tuple in scan, if any */
    int32 rs_index; /* current tuple position in in-memory heap table */
    HeapScanDesc hscan; /* if there is a heap table with the same Oid, this a heap scan descriptor */
} InMemHeapScanDescData;

typedef InMemHeapScanDescData * InMemHeapScanDesc;

extern void InitOidInMemHeapMapping(long initSize, MemoryContext memcxt);

extern void CleanupOidInMemHeapMapping(void);

extern InMemHeapRelation OidGetInMemHeapRelation(Oid relid);

extern InMemHeapRelation InMemHeap_Create(Oid relid, Relation rel, bool ownrel,
        int32 initSize, LOCKMODE lock, const char * relname);

extern void InMemHeap_Drop(Oid relid);

extern void InMemHeap_DropAll(void);

extern InMemHeapScanDesc InMemHeap_BeginScan(InMemHeapRelation memheap,
        int nkeys, ScanKey key, bool inmemonly);

extern void InMemHeap_EndScan(InMemHeapScanDesc scan);

extern HeapTuple InMemHeap_GetNext(InMemHeapScanDesc scan);

extern void
InMemHeap_Insert(InMemHeapRelation relation, HeapTuple tup, int contentid);

extern void
InMemHeap_Update(InMemHeapRelation relation, ItemPointer otid, HeapTuple tup);

#endif /* _INMEMHEAP_H_ */
