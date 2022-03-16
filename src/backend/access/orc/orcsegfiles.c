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

#include "postgres.h"

#include "access/orcsegfiles.h"

#include "access/aomd.h"
#include "access/filesplit.h"
#include "access/genam.h"
#include "catalog/catalog.h"
#include "nodes/relation.h"
#include "cdb/cdbmetadatacache.h"
#include "cdb/cdbvars.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"

void insertInitialOrcSegnoEntry(AppendOnlyEntry *aoEntry, int segNo) {
  if (segNo == 0) return;

  Relation segRel = heap_open(aoEntry->segrelid, RowExclusiveLock);
  TupleDesc desc = RelationGetDescr(segRel);
  int natts = desc->natts;
  bool *nulls = palloc(sizeof(bool) * natts);
  Datum *values = palloc0(sizeof(Datum) * natts);
  MemSet(nulls, 0, sizeof(char) * natts);

  values[Anum_pg_orcseg_segno - 1] = Int32GetDatum(segNo);
  values[Anum_pg_orcseg_eof - 1] = Float8GetDatum(0);
  values[Anum_pg_orcseg_tupcount - 1] = Float8GetDatum(0);
  values[Anum_pg_orcseg_eofuncompressed - 1] = Float8GetDatum(0);
  HeapTuple tuple = heap_form_tuple(desc, values, nulls);
  if (!HeapTupleIsValid(tuple))
    elog(ERROR, "failed to build orc file segment tuple");

  frozen_heap_insert(segRel, tuple);

  if (Gp_role == GP_ROLE_DISPATCH) CatalogUpdateIndexes(segRel, tuple);

  heap_freetuple(tuple);

  heap_close(segRel, RowExclusiveLock);
}

void insertInitialOrcIndexEntry(AppendOnlyEntry *aoEntry, int idxOid, int segNo)
{
  if (idxOid == 0 || segNo == 0) return;

  Relation segRel = heap_open(aoEntry->blkdirrelid, RowExclusiveLock);
  TupleDesc desc = RelationGetDescr(segRel);
  int natts = desc->natts;
  bool *nulls = palloc(sizeof(bool) * natts);
  Datum *values = palloc0(sizeof(Datum) * natts);
  MemSet(nulls, 0, sizeof(char) * natts);

  values[Anum_pg_orcseg_idx_idxoid - 1] = Int32GetDatum(idxOid);
  values[Anum_pg_orcseg_idx_segno - 1] = Int32GetDatum(segNo);
  values[Anum_pg_orcseg_idx_eof - 1] = Float8GetDatum(0);
  HeapTuple tuple = heap_form_tuple(desc, values, nulls);
  if (!HeapTupleIsValid(tuple))
    elog(ERROR, "failed to build orc index file segment tuple");

  frozen_heap_insert(segRel, tuple);

  if (Gp_role == GP_ROLE_DISPATCH) CatalogUpdateIndexes(segRel, tuple);

  heap_freetuple(tuple);
  heap_close(segRel, RowExclusiveLock);
}

void insertOrcSegnoEntry(AppendOnlyEntry *aoEntry, int segNo, float8 tupleCount,
                         float8 eof, float8 uncompressedEof) {
  Relation segRel = heap_open(aoEntry->segrelid, RowExclusiveLock);
  TupleDesc desc = RelationGetDescr(segRel);
  int natts = desc->natts;
  bool *nulls = palloc(sizeof(bool) * natts);
  Datum *values = palloc0(sizeof(Datum) * natts);
  MemSet(nulls, 0, sizeof(char) * natts);

  values[Anum_pg_orcseg_segno - 1] = Int32GetDatum(segNo);
  values[Anum_pg_orcseg_eof - 1] = Float8GetDatum(eof);
  values[Anum_pg_orcseg_tupcount - 1] = Float8GetDatum(tupleCount);
  values[Anum_pg_orcseg_eofuncompressed - 1] = Float8GetDatum(uncompressedEof);
  HeapTuple tuple = heap_form_tuple(desc, values, nulls);
  if (!HeapTupleIsValid(tuple))
    elog(ERROR, "failed to build orc file segment tuple");

  simple_heap_insert(segRel, tuple);

  heap_freetuple(tuple);

  heap_close(segRel, RowExclusiveLock);
}

void deleteOrcIndexHdfsFiles(Relation rel, int32 segmentFileNum, int32 idx)
{
  RelFileNode rd_node = rel->rd_node;
  char *basepath = relpath(rel->rd_node);
  HdfsFileInfo *file_info;
  char *path = (char*)palloc(MAXPGPATH + 1);

  FormatAOSegmentIndexFileName(basepath, segmentFileNum, idx,  -1, 0, &segmentFileNum, path);

  RemovePath(path, 0);

  if (!IsLocalPath(path) && Gp_role == GP_ROLE_DISPATCH)
  {
    // Remove Hdfs block locations info in Metadata Cache
    file_info = CreateHdfsFileInfo(rd_node, segmentFileNum);
    LWLockAcquire(MetadataCacheLock, LW_EXCLUSIVE);
    RemoveHdfsFileBlockLocations(file_info);
    LWLockRelease(MetadataCacheLock);
    DestroyHdfsFileInfo(file_info);
  }

  pfree(path);
}

void deleteOrcIndexFileInfo(Relation rel, AppendOnlyEntry *aoEntry, int idxOid)
{
  if (aoEntry->blkdirrelid == 0) return;
  Relation segRel = heap_open(aoEntry->blkdirrelid, RowExclusiveLock);
  TupleDesc desc = RelationGetDescr(segRel);
  ScanKeyData key[1];
  ScanKeyInit(&key[0], (AttrNumber)Anum_pg_orcseg_idx_idxoid, BTEqualStrategyNumber,
              F_INT4EQ, Int32GetDatum(idxOid));
  SysScanDesc scan = systable_beginscan(segRel, aoEntry->blkdiridxid, TRUE,
                                        SnapshotNow, 1, &key[0]);
  HeapTuple tuple;
  while ((tuple = systable_getnext(scan)))
  {
    int segno = DatumGetInt32(fastgetattr(tuple, Anum_pg_orcseg_idx_segno, desc, NULL));
    /* delete hdfs index files */
    deleteOrcIndexHdfsFiles(rel, segno, idxOid);
    /* delete catalog info */
    simple_heap_delete(segRel, &tuple->t_self);
  }

  systable_endscan(scan);
  heap_close(segRel, RowExclusiveLock);
}

void updateOrcIndexFileInfo(AppendOnlyEntry *aoEntry, int idxOid, int segNo, int64 eof)
{
  Relation segRel = heap_open(aoEntry->blkdirrelid, RowExclusiveLock);
  TupleDesc desc = RelationGetDescr(segRel);
  /* both idxoid and segno needed to scan tuple */
  ScanKeyData key[2];
  ScanKeyInit(&key[0], (AttrNumber)Anum_pg_orcseg_idx_idxoid, BTEqualStrategyNumber,
              F_INT4EQ, Int32GetDatum(idxOid));
  ScanKeyInit(&key[1], (AttrNumber)Anum_pg_orcseg_idx_segno, BTEqualStrategyNumber,
              F_INT4EQ, Int32GetDatum(segNo));
  SysScanDesc scan = systable_beginscan(segRel, aoEntry->blkdiridxid, TRUE,
                                        SnapshotNow, 2, &key[0]);
  HeapTuple tuple = systable_getnext(scan);

  Datum *record = palloc0(sizeof(Datum) * desc->natts);
  bool *nulls = palloc0(sizeof(bool) * desc->natts);
  bool *repl = palloc0(sizeof(bool) * desc->natts);

  record[Anum_pg_orcseg_idx_eof - 1] = Float8GetDatum((float8)eof);
  repl[Anum_pg_orcseg_idx_eof - 1] = true;

  HeapTuple newTuple = heap_modify_tuple(tuple, desc, record, nulls, repl);
  simple_heap_update(segRel, &tuple->t_self, newTuple);
  CatalogUpdateIndexes(segRel, newTuple);
  heap_freetuple(newTuple);

  systable_endscan(scan);
  heap_close(segRel, RowExclusiveLock);
  pfree(record);
  pfree(nulls);
  pfree(repl);
}

void updateOrcFileSegInfo(Relation rel, AppendOnlyEntry *aoEntry, int segNo,
                          int64 eof, int64 uncompressedEof, int64 tupCountAdded,
                          bool forInsert) {
  Relation segRel = heap_open(aoEntry->segrelid, RowExclusiveLock);
  TupleDesc desc = RelationGetDescr(segRel);
  ScanKeyData key[1];
  ScanKeyInit(&key[0], (AttrNumber)Anum_pg_orcseg_segno, BTEqualStrategyNumber,
              F_INT4EQ, Int32GetDatum(segNo));
  SysScanDesc scan = systable_beginscan(segRel, aoEntry->segidxid, TRUE,
                                        SnapshotNow, 1, &key[0]);
  HeapTuple tuple = systable_getnext(scan);
  if (!HeapTupleIsValid(tuple)) {
    if (forInsert)
      ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                      errmsg("orc table \"%s\" file segment \"%d\" entry "
                             "does not exist",
                             RelationGetRelationName(rel), segNo)));
    else {
      systable_endscan(scan);
      heap_close(segRel, RowExclusiveLock);
      return;
    }
  }

  Datum *record = palloc0(sizeof(Datum) * desc->natts);
  bool *nulls = palloc0(sizeof(bool) * desc->natts);
  bool *repl = palloc0(sizeof(bool) * desc->natts);
  Datum tupleCount =
      forInsert ? DirectFunctionCall2(
                      float8pl,
                      fastgetattr(tuple, Anum_pg_orcseg_tupcount, desc, NULL),
                      Float8GetDatum((float8)tupCountAdded))
                : Float8GetDatum((float8)tupCountAdded);
  record[Anum_pg_orcseg_eof - 1] = Float8GetDatum((float8)eof);
  repl[Anum_pg_orcseg_eof - 1] = true;
  record[Anum_pg_orcseg_tupcount - 1] = tupleCount;
  repl[Anum_pg_orcseg_tupcount - 1] = true;
  record[Anum_pg_orcseg_eofuncompressed - 1] =
      Float8GetDatum((float8)uncompressedEof);
  repl[Anum_pg_orcseg_eofuncompressed - 1] = true;

  HeapTuple newTuple = heap_modify_tuple(tuple, desc, record, nulls, repl);
  simple_heap_update(segRel, &tuple->t_self, newTuple);
  CatalogUpdateIndexes(segRel, newTuple);
  heap_freetuple(newTuple);

  systable_endscan(scan);
  heap_close(segRel, RowExclusiveLock);
  pfree(record);
  pfree(nulls);
  pfree(repl);
}

List *orcGetAllSegFileSplits(AppendOnlyEntry *aoEntry, Snapshot snapshot) {
  Relation segRel = heap_open(aoEntry->segrelid, AccessShareLock);
  TupleDesc segDesc = RelationGetDescr(segRel);
  HeapTuple tuple = NULL;
  List *splits = NIL;

  SysScanDesc segScan =
      systable_beginscan(segRel, InvalidOid, FALSE, snapshot, 0, NULL);
  while (HeapTupleIsValid(tuple = systable_getnext(segScan))) {
    FileSplit split = makeNode(FileSplitNode);
    split->segno =
        DatumGetInt32(fastgetattr(tuple, Anum_pg_orcseg_segno, segDesc, NULL));
    split->offsets = 0;
    split->lengths = (int64)DatumGetFloat8(
        fastgetattr(tuple, Anum_pg_orcseg_eof, segDesc, NULL));
    split->logiceof = split->lengths;
    split->ext_file_uri_string = NULL;
    splits = lappend(splits, split);
  }
  systable_endscan(segScan);

  heap_close(segRel, AccessShareLock);
  return splits;
}

static int fileSegInfoCmp(const void *left, const void *right) {
  FileSegInfo *leftSegInfo = *((FileSegInfo **)left);
  FileSegInfo *rightSegInfo = *((FileSegInfo **)right);

  if (leftSegInfo->segno < rightSegInfo->segno) return -1;

  if (leftSegInfo->segno > rightSegInfo->segno) return 1;

  return 0;
}

static FileSegInfo **getAllOrcFileSegInfoInternal(Relation segRel,
                                                  AppendOnlyEntry *aoEntry,
                                                  Snapshot snapshot,
                                                  int *totalSegs,
                                                  int expectedSegno) {
  int segInfoSlotNo = AO_FILESEGINFO_ARRAY_SIZE;
  FileSegInfo **allSegInfo =
      (FileSegInfo **)palloc0(sizeof(FileSegInfo *) * segInfoSlotNo);

  ScanKeyData key[1];
  int numOfKey = 0;
  bool indexOK = FALSE;
  Oid indexId = InvalidOid;
  if (expectedSegno >= 0) {
    ScanKeyInit(&key[numOfKey++], (AttrNumber)Anum_pg_orcseg_segno,
                BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(expectedSegno));
    if (Gp_role == GP_ROLE_DISPATCH) {
      indexOK = TRUE;
      indexId = aoEntry->segidxid;
    }
  }
  SysScanDesc scan =
      systable_beginscan(segRel, indexId, indexOK, snapshot, numOfKey, &key[0]);
  int segInfoNo = 0;
  HeapTuple tuple = NULL;
  TupleDesc desc = RelationGetDescr(segRel);
  while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
    if (segInfoNo >= segInfoSlotNo) {
      segInfoSlotNo *= 2;
      allSegInfo = (FileSegInfo **)repalloc(
          allSegInfo, sizeof(FileSegInfo *) * segInfoSlotNo);
    }

    FileSegInfo *segInfo = allSegInfo[segInfoNo++] =
        (FileSegInfo *)palloc0(sizeof(FileSegInfo));

    if (Gp_role == GP_ROLE_DISPATCH)
      GetTupleVisibilitySummary(tuple, &segInfo->tupleVisibilitySummary);

    segInfo->segno =
        DatumGetInt32(fastgetattr(tuple, Anum_pg_orcseg_segno, desc, NULL));
    segInfo->eof = (int64)DatumGetFloat8(
        fastgetattr(tuple, Anum_pg_orcseg_eof, desc, NULL));
    segInfo->tupcount = (int64)DatumGetFloat8(
        fastgetattr(tuple, Anum_pg_orcseg_tupcount, desc, NULL));
    segInfo->eof_uncompressed = (int64)DatumGetFloat8(
        fastgetattr(tuple, Anum_pg_orcseg_eofuncompressed, desc, NULL));
    ItemPointerSetInvalid(&segInfo->sequence_tid);
  }

  systable_endscan(scan);

  *totalSegs = segInfoNo;
  if (*totalSegs == 0) {
    pfree(allSegInfo);
    return NULL;
  }
  qsort((char *)allSegInfo, *totalSegs, sizeof(FileSegInfo *), fileSegInfoCmp);

  return allSegInfo;
}

FileSegInfo **getAllOrcFileSegInfo(AppendOnlyEntry *aoEntry, Snapshot snapshot,
                                   int *totalSegs) {
  Relation segRel = heap_open(aoEntry->segrelid, AccessShareLock);
  FileSegInfo **result =
      getAllOrcFileSegInfoInternal(segRel, aoEntry, snapshot, totalSegs, -1);
  heap_close(segRel, AccessShareLock);
  return result;
}

FileSegInfo **getAllOrcFileSegInfoWithSegNo(AppendOnlyEntry *aoEntry,
                                            Snapshot snapshot, int segNo,
                                            int *totalSegs) {
  Relation segRel = heap_open(aoEntry->segrelid, AccessShareLock);
  FileSegInfo **result =
      getAllOrcFileSegInfoInternal(segRel, aoEntry, snapshot, totalSegs, segNo);
  heap_close(segRel, AccessShareLock);
  return result;
}

FileSegInfo *getOrcFileSegInfo(Relation rel, AppendOnlyEntry *aoEntry,
                               Snapshot snapshot, int segNo) {
  Relation segRel = heap_open(aoEntry->segrelid, AccessShareLock);
  bool indexOK = FALSE;
  Oid indexId = InvalidOid;
  if (Gp_role == GP_ROLE_DISPATCH) {
    indexOK = TRUE;
    indexId = aoEntry->segidxid;
  }

  ScanKeyData key[1];
  ScanKeyInit(&key[0], (AttrNumber)Anum_pg_orcseg_segno, BTEqualStrategyNumber,
              F_INT4EQ, Int32GetDatum(segNo));
  SysScanDesc scan =
      systable_beginscan(segRel, indexId, indexOK, snapshot, 1, &key[0]);
  HeapTuple tuple = systable_getnext(scan);
  if (!HeapTupleIsValid(tuple)) {
    systable_endscan(scan);
    heap_close(segRel, AccessShareLock);
    return NULL;
  }

  tuple = heap_copytuple(tuple);
  systable_endscan(scan);

  Assert(HeapTupleIsValid(tuple));

  FileSegInfo *segInfo = (FileSegInfo *)palloc0(sizeof(FileSegInfo));
  TupleDesc desc = RelationGetDescr(segRel);
  segInfo->segno =
      DatumGetInt32(fastgetattr(tuple, Anum_pg_orcseg_segno, desc, NULL));
  segInfo->eof =
      (int64)DatumGetFloat8(fastgetattr(tuple, Anum_pg_orcseg_eof, desc, NULL));
  segInfo->tupcount = (int64)DatumGetFloat8(
      fastgetattr(tuple, Anum_pg_orcseg_tupcount, desc, NULL));
  segInfo->eof_uncompressed = (int64)DatumGetFloat8(
      fastgetattr(tuple, Anum_pg_orcseg_eofuncompressed, desc, NULL));
  ItemPointerSetInvalid(&segInfo->sequence_tid);

  heap_close(segRel, AccessShareLock);
  return segInfo;
}

void fetchOrcSegFileInfo(AppendOnlyEntry *aoEntry, List *segFileInfo,
                         Snapshot snapshot) {
  bool indexOK = FALSE;
  Oid indexId = InvalidOid;
  if (Gp_role == GP_ROLE_DISPATCH) {
    indexOK = TRUE;
    indexId = aoEntry->segidxid;
  }
  Relation segRel = heap_open(aoEntry->segrelid, AccessShareLock);
  TupleDesc desc = RelationGetDescr(segRel);
  ListCell *lc;
  foreach (lc, segFileInfo) {
    ResultRelSegFileInfo *info = (ResultRelSegFileInfo *)lfirst(lc);
    Assert(info != NULL);
    ScanKeyData key[1];
    ScanKeyInit(&key[0], (AttrNumber)Anum_pg_orcseg_segno,
                BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(info->segno));
    SysScanDesc scan =
        systable_beginscan(segRel, indexId, indexOK, snapshot, 1, &key[0]);
    HeapTuple tuple = systable_getnext(scan);
    if (HeapTupleIsValid(tuple)) {
      info->eof = (int64 *)palloc(sizeof(int64));
      info->eof[0] = (int64)DatumGetFloat8(
          fastgetattr(tuple, Anum_pg_orcseg_eof, desc, NULL));
      info->numfiles = 1;
      info->tupcount = (int64)DatumGetFloat8(
          fastgetattr(tuple, Anum_pg_orcseg_tupcount, desc, NULL));
      info->uncompressed_eof = (int64 *)palloc(sizeof(int64));
      info->uncompressed_eof[0] = (int64)DatumGetFloat8(
          fastgetattr(tuple, Anum_pg_orcseg_eofuncompressed, desc, NULL));
    }
    systable_endscan(scan);
  }
  heap_close(segRel, AccessShareLock);
}

FileSegTotals *getOrcSegFileStats(Relation rel, Snapshot snapshot) {
  FileSegTotals *result = (FileSegTotals *)palloc0(sizeof(FileSegTotals));
  AppendOnlyEntry *aoEntry =
      GetAppendOnlyEntry(RelationGetRelid(rel), snapshot);
  Relation segRel = heap_open(aoEntry->segrelid, AccessShareLock);
  TupleDesc desc = RelationGetDescr(segRel);
  SysScanDesc segScan =
      systable_beginscan(segRel, InvalidOid, FALSE, snapshot, 0, NULL);
  HeapTuple tuple = NULL;
  while (HeapTupleIsValid(tuple = systable_getnext(segScan))) {
    result->totalbytes += (int64)DatumGetFloat8(
        fastgetattr(tuple, Anum_pg_orcseg_eof, desc, NULL));
    result->totaltuples += (int64)DatumGetFloat8(
        fastgetattr(tuple, Anum_pg_orcseg_tupcount, desc, NULL));
    result->totalbytesuncompressed += (int64)DatumGetFloat8(
        fastgetattr(tuple, Anum_pg_orcseg_eofuncompressed, desc, NULL));
    ++result->totalfilesegs;
  }
  systable_endscan(segScan);
  heap_close(segRel, AccessShareLock);
  pfree(aoEntry);
  return result;
}

int64 getOrcTotalBytes(Relation rel, Snapshot snapshot) {
  int64 result = 0;
  FileSegTotals *fstotal = getOrcSegFileStats(rel, SnapshotNow);
  result = fstotal->totalbytes;
  pfree(fstotal);
  return result;
}

Datum getOrcCompressionRatio(Relation rel) {
  float8 ratio = -1;
  FileSegTotals *fstotal = getOrcSegFileStats(rel, SnapshotNow);
  if (fstotal->totalbytes > 0) {
    char buf[8];

    /* calculate the compression ratio */
    float8 rawRatio = DatumGetFloat8(DirectFunctionCall2(
        float8div, Float8GetDatum(fstotal->totalbytesuncompressed),
        Float8GetDatum(fstotal->totalbytes)));

    /* format to 2 digits past the decimal point */
    snprintf(buf, 8, "%.2f", rawRatio);

    /* format to 2 digit decimal precision */
    ratio = DatumGetFloat8(DirectFunctionCall1(float8in, CStringGetDatum(buf)));
  }
  pfree(fstotal);
  PG_RETURN_FLOAT8(ratio);
}
