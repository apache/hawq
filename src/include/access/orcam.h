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

// orc access manager

#ifndef ORCAM_H_
#define ORCAM_H_

#include "access/orcsegfiles.h"
#include "cdb/cdbquerycontextdispatching.h"
#include "nodes/relation.h"

#define DEFAULT_ORC_ROW_GROUP_SIZE 65536
#define MIN_ORC_ROW_GROUP_SIZE 1000
#define MAX_ORC_ROW_GROUP_SIZE 1024 * 1024 * 1024
// here we use orc block size in Bytes
#define DEFAULT_ORC_COMPRESS_BLOCK_SIZE 256 * 1024
#define MIN_ORC_COMPRESS_BLOCK_SIZE 1
#define MAX_ORC_COMPRESS_BLOCK_SIZE 1024 * 1024 * 1024
// here we use orc stripe size in MBytes
#define DEFAULT_ORC_STRIPE_SIZE 64
#define MIN_ORC_STRIPE_SIZE 1
#define MAX_ORC_STRIPE_SIZE 1024

struct ScanState;

typedef struct OrcFormatData OrcFormatData;
typedef struct OrcInsertDescData {
  Relation rel;
  int32 segno;
  int64 insertCount;
  OrcFormatData *orcFormatData;
  QueryContextDispatchingSendBack sendback;
  MemoryContext memCxt;
} OrcInsertDescData;

typedef struct OrcScanDescData {
  Relation rel;
  OrcFormatData *orcFormatData;
  ItemPointerData cdb_fake_ctid;
} OrcScanDescData;

typedef struct OrcDeleteDescData {
  Relation rel;
  int32 newSegno;
  Datum rowId;
  OrcFormatData *orcFormatData;
  QueryContextDispatchingSendBack sendback;
  bool directDispatch;
} OrcDeleteDescData;

typedef struct OrcUpdateDescData {
  Relation rel;
  int32 newSegno;
  int64 updateCount;
  Datum rowId;
  TupleTableSlot *slot;
  OrcFormatData *orcFormatData;
  QueryContextDispatchingSendBack sendback;
  bool directDispatch;
  MemoryContext memCxt;
} OrcUpdateDescData;

// insert
extern OrcInsertDescData *orcBeginInsert(Relation rel,
                                         ResultRelSegFileInfo *segfileinfo);
extern Oid orcInsert(OrcInsertDescData *insertDesc, TupleTableSlot *slot);
extern Oid orcInsertValues(OrcInsertDescData *insertDesc, Datum *values,
                           bool *nulls, TupleDesc tupleDesc);
extern void orcEndInsert(OrcInsertDescData *insertDesc);

// scan
extern void orcBeginScan(struct ScanState* scanState);
extern TupleTableSlot* orcScanNext(struct ScanState* scanState);
extern void orcEndScan(struct ScanState* scanState);
extern void orcReScan(struct ScanState* scanState);

extern OrcScanDescData* orcBeginReadWithOptionsStr(
    Relation rel, Snapshot snapshot, TupleDesc desc, List* fileSplits,
    bool* colToReads, void* pushDown, const char*);
extern OrcScanDescData* orcBeginRead(Relation rel, Snapshot snapshot,
                                     TupleDesc desc, List* fileSplits,
                                     bool* colToReads, void* pushDown);
extern void orcReadNext(OrcScanDescData* scanData, TupleTableSlot* slot);
extern void orcEndRead(OrcScanDescData* scanData);
extern void orcResetRead(OrcScanDescData* scanData);

// delete
extern OrcDeleteDescData *orcBeginDelete(Relation rel, List *fileSplits,
                                         List *relFileNodeInfo,
                                         bool orderedRowId,
                                         bool directDispatch);
extern void orcDelete(OrcDeleteDescData *deleteDesc);
extern uint64 orcEndDelete(OrcDeleteDescData *deleteDesc);

// update
extern OrcUpdateDescData *orcBeginUpdate(Relation rel, List *fileSplits,
                                         List *relFileNodeInfo,
                                         bool orderedRowId,
                                         bool directDispatch);
extern void orcUpdate(OrcUpdateDescData *updateDesc);
extern uint64 orcEndUpdate(OrcUpdateDescData *updateDesc);

// utils
extern bool isDirectDispatch(Plan *plan);

void checkOrcError(OrcFormatData* orcFormatData);

// index
extern int64_t* orcCreateIndex(Relation rel, Oid idxId, List* segno, int64* eof,
                               List* columnsToRead, int sortIdx);
extern void orcBeginIndexOnlyScan(struct ScanState* scanState, Oid idxId,
                                  List* columnsInIndex);
extern void orcIndexReadNext(OrcScanDescData* scanData, TupleTableSlot* slot,
                             List* columnsInIndex);

extern TupleTableSlot* orcIndexOnlyScanNext(struct ScanState* scanState);

extern void orcEndIndexOnlyScan(struct ScanState* scanState);

extern void orcIndexOnlyReScan(struct ScanState* scanState);

extern OrcScanDescData* orcBeginIndexOnlyRead(Relation rel, Oid idxId,
                                              List* columnsInIndex,
                                              Snapshot snapshot, TupleDesc desc,
                                              List* fileSplits,
                                              bool* colToReads, void* pushDown);

extern void orcBeginIndexOnlyScan(struct ScanState* scanState, Oid idxId,
                                  List* columnsInIndex);

extern TupleTableSlot* orcIndexOnlyScanNext(struct ScanState* scanState);

extern void orcEndIndexOnlyScan(struct ScanState* scanState);

extern void orcIndexOnlyReScan(struct ScanState* scanState);

extern OrcScanDescData* orcBeginIndexOnlyRead(Relation rel, Oid idxId,
                                              List* columnsInIndex,
                                              Snapshot snapshot, TupleDesc desc,
                                              List* fileSplits,
                                              bool* colToReads, void* pushDown);

#endif /* ORCAM_H_ */
