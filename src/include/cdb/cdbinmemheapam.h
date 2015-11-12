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
 * cdbinmemheapam.h
 *	 goh in-memory heap table access method
 *
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

/*
 * In memory storage types. When creating/accessing/dropping tables,
 * the mapping type needs to be specified.
 */
enum InMemMappingType
{
	INMEM_HEAP_MAPPING = 0, /* Tuples from heap tables saved in memory (e.g. when passed to segs) */
	INMEM_ONLY_MAPPING, /* Tuples that are only kept in memory and do not have a copy on disk. (e.g. HCatalog) */
	INMEM_MAPPINGS_SIZE /* Number of mappings - keep last. */
};
typedef enum InMemMappingType InMemMappingType;

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
    HTAB *hashIndex;    /* build a hash index for fast lookup */
    int  keyAttrno;     /* attribute no of hash index key, key must be Oid type */
};
typedef struct InMemHeapRelationData InMemHeapRelationData;
typedef struct InMemHeapRelationData * InMemHeapRelation;

extern HTAB* OidInMemMappings[INMEM_MAPPINGS_SIZE];

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

    Oid indexScanKey; /* hash key searched in hash table */
    bool hashIndexOk; /* hash index is ok to use */
    bool indexScanInitialized; /* hash index scan has initialized */
    int hashKeyIndexInScanKey; /* the index of hash key in scan key array */
    ListCell *indexNext; /* cursor in hash index */
    List* indexReverseList; /* reverse list of the scan key for backward scan */
} InMemHeapScanDescData;

typedef InMemHeapScanDescData * InMemHeapScanDesc;

extern void InitOidInMemHeapMapping(long initSize, MemoryContext memcxt, InMemMappingType mappingType);

extern void CleanupOidInMemHeapMapping(InMemMappingType mappingType);

extern InMemHeapRelation OidGetInMemHeapRelation(Oid relid, InMemMappingType mappingType);

extern InMemHeapRelation InMemHeap_Create(Oid relid, Relation rel, bool ownrel,
        int32 initSize, LOCKMODE lock, const char * relname, bool createIndex, int keyAttrno,
		InMemMappingType mappingType);

extern void InMemHeap_Drop(Oid relid, InMemMappingType mappingType);

extern void InMemHeap_DropAll(InMemMappingType mappingType);

extern InMemHeapScanDesc InMemHeap_BeginScan(InMemHeapRelation memheap,
        int nkeys, ScanKey key, AttrNumber *orig_attnos, bool inmemonly);

extern void InMemHeap_EndScan(InMemHeapScanDesc scan);

extern HeapTuple InMemHeap_GetNext(InMemHeapScanDesc scan, ScanDirection direction);

extern void
InMemHeap_Insert(InMemHeapRelation relation, HeapTuple tup, int contentid);

extern void
InMemHeap_Update(InMemHeapRelation relation, ItemPointer otid, HeapTuple tup);

extern void 
InMemHeap_CheckConstraints(InMemHeapRelation relation, HeapTuple newTuple);

extern Datum
tuple_getattr(HeapTuple tuple, TupleDesc tupleDesc, int attnum);

#endif /* _INMEMHEAP_H_ */
