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

#include <inttypes.h>

#include "postgres.h"

#include "access/orcam.h"

#include "access/aomd.h"
#include "access/filesplit.h"
#include "catalog/catalog.h"
#include "cdb/cdbfilesystemcredential.h"
#include "cdb/cdbvars.h"
#include "executor/cwrapper/executor-c.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "optimizer/newPlanner.h"
#include "storage/cwrapper/hdfs-file-system-c.h"
#include "storage/cwrapper/orc-format-c.h"
#include "storage/fd.h"
#include "storage/lmgr.h"
#include "utils/datetime.h"
#include "utils/guc.h"
#include "utils/hawq_type_mapping.h"
#include "utils/memutils.h"
#include "utils/numeric.h"

static char dummyPlaceholder[16];
static const int32 MAX_BUFFER_LEN = 2 * 32768;

typedef struct OrcCopyContext {
  const char *destDir;
  char *destFile;
  const char *srcDir;
  char *srcFile;
  List *segNoList;
  char *buffer;
} OrcCopyContext;

typedef struct {
  int64 second;
  int64 nanosecond;
} TimestampType;

typedef struct OrcFormatData {
  ORCFormatC *fmt;
  StorageFormatC *updateDeleteFmt;

  // schema
  int32 numberOfColumns;
  char **colNames;
  int *colDatatypes;
  uint64 *colDatatypeMods;

  // output values buffer
  char **colRawValues;
  uint64 *colValLength;
  TimestampType *colTimestamp;
  struct varlena **colFixedLenUDT;
} OrcFormatData;

static void initOrcFormatIndexUserData(TupleDesc tup_desc,
                                       OrcFormatData *orcFormatData,
                                       bool *colToReads, List *columnsInIndex,
                                       bool *colToReadInIndex) {
  int natts = list_length(columnsInIndex) + 1;
  orcFormatData->numberOfColumns = natts;
  orcFormatData->colNames = palloc0(sizeof(char *) * natts);
  orcFormatData->colDatatypes = palloc0(sizeof(int) * natts);
  orcFormatData->colDatatypeMods = palloc0(sizeof(uint64) * natts);
  orcFormatData->colRawValues = palloc0(sizeof(char *) * natts);
  orcFormatData->colValLength = palloc0(sizeof(uint64) * natts);
  orcFormatData->colTimestamp = palloc0(sizeof(TimestampType) * natts);
  orcFormatData->colFixedLenUDT = palloc0(sizeof(struct varlena *) * natts);
  int count = 0;
  for (int i = 0; i < tup_desc->natts; ++i) {
    for (int j = 0; j < list_length(columnsInIndex); j++) {
      if ((int)list_nth_oid(columnsInIndex, j) - 1 == i) {
        // allocate memory for colFixedLenUDT[i] of fixed-length type in advance
        bool isFixedLengthType = tup_desc->attrs[i]->attlen > 0 ? true : false;
        if (isFixedLengthType) {
          orcFormatData->colFixedLenUDT[count] = (struct valena *)palloc0(
              tup_desc->attrs[i]->attlen + sizeof(uint32_t));
        }
        orcFormatData->colNames[count] = palloc0(NAMEDATALEN);
        strcpy(orcFormatData->colNames[count],
               tup_desc->attrs[i]->attname.data);
        orcFormatData->colDatatypes[count] =
            map_hawq_type_to_common_plan((int)(tup_desc->attrs[i]->atttypid));
        orcFormatData->colDatatypeMods[count] = tup_desc->attrs[i]->atttypmod;
        if (orcFormatData->colDatatypes[count] == CHARID &&
            tup_desc->attrs[i]->atttypmod == -1) {
          // XXX(chiyang): From orc.c to determine BPCHAR's typemod
          orcFormatData->colDatatypeMods[count] =
              strlen(tup_desc->attrs[i]->attname.data) + VARHDRSZ;
        }
        if (colToReads[i]) {
          colToReadInIndex[count] = true;
        }
        count++;
      }
    }
  }
  orcFormatData->colNames[count] = palloc0(NAMEDATALEN);
  strcpy(orcFormatData->colNames[count], "tid");
  orcFormatData->colDatatypes[count] = BIGINTID;
  orcFormatData->colDatatypeMods[count] = -1;
}

static void initOrcFormatUserData(TupleDesc tup_desc,
                                  OrcFormatData *orcFormatData) {
  int natts = tup_desc->natts;
  orcFormatData->numberOfColumns = tup_desc->natts;
  orcFormatData->colNames = palloc0(sizeof(char *) * natts);
  orcFormatData->colDatatypes = palloc0(sizeof(int) * natts);
  orcFormatData->colDatatypeMods = palloc0(sizeof(uint64) * natts);
  orcFormatData->colRawValues = palloc0(sizeof(char *) * natts);
  orcFormatData->colValLength = palloc0(sizeof(uint64) * natts);
  orcFormatData->colTimestamp = palloc0(sizeof(TimestampType) * natts);
  orcFormatData->colFixedLenUDT = palloc0(sizeof(struct varlena *) * natts);

  for (int i = 0; i < orcFormatData->numberOfColumns; ++i) {
    // allocate memory for colFixedLenUDT[i] of fixed-length type in advance
    bool isFixedLengthType = tup_desc->attrs[i]->attlen > 0 ? true : false;
    if (isFixedLengthType) {
      orcFormatData->colFixedLenUDT[i] = (struct valena *)palloc0(
          tup_desc->attrs[i]->attlen + sizeof(uint32_t));
    }

    orcFormatData->colNames[i] = palloc0(NAMEDATALEN);
    strcpy(orcFormatData->colNames[i], tup_desc->attrs[i]->attname.data);

    orcFormatData->colDatatypes[i] =
        map_hawq_type_to_common_plan((int)(tup_desc->attrs[i]->atttypid));
    orcFormatData->colDatatypeMods[i] = tup_desc->attrs[i]->atttypmod;

    if (orcFormatData->colDatatypes[i] == CHARID &&
        tup_desc->attrs[i]->atttypmod == -1) {
      // XXX(chiyang): From orc.c to determine BPCHAR's typemod
      orcFormatData->colDatatypeMods[i] =
          strlen(tup_desc->attrs[i]->attname.data) + VARHDRSZ;
    }
  }
}

static freeOrcFormatUserData(OrcFormatData *orcFormatData) {
  for (int i = 0; i < orcFormatData->numberOfColumns; ++i) {
    pfree(orcFormatData->colNames[i]);
    if (orcFormatData->colFixedLenUDT[i])
      pfree(orcFormatData->colFixedLenUDT[i]);
  }

  pfree(orcFormatData->colTimestamp);
  pfree(orcFormatData->colValLength);
  pfree(orcFormatData->colRawValues);
  pfree(orcFormatData->colDatatypeMods);
  pfree(orcFormatData->colDatatypes);
  pfree(orcFormatData->colNames);
}

void checkOrcError(OrcFormatData *orcFormatData) {
  ORCFormatCatchedError *e = ORCFormatGetErrorORCFormatC(orcFormatData->fmt);
  if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    ORCFormatCatchedError errBuf = *e;
    ORCFormatFreeORCFormatC(&orcFormatData->fmt);
    ORCFormatFreeStorageFormatC(&orcFormatData->updateDeleteFmt);
    ereport(ERROR, (errcode(errBuf.errCode), errmsg("%s", errBuf.errMessage)));
  }
}

static void addFilesystemCredential(const char *uri) {
  if (enable_secure_filesystem) {
    if (Gp_role != GP_ROLE_EXECUTE) {
      if (!login())
        ereport(ERROR,
                (EACCES, errmsg("kerberos login failed for path %s", uri)));
      SetCcname(krb5_ccname);
    } else {
      char *token = find_filesystem_credential_with_uri(uri);
      SetToken(uri, token);
    }
  }
}

void constructOrcFormatOptionString(StringInfoData *option, Relation rel,
                                    ResultRelSegFileInfo *segfileinfo,
                                    AppendOnlyEntry *aoentry) {
  initStringInfo(option);
  appendStringInfoChar(option, '{');

  // neglect for UPDATE/DELETE
  if (segfileinfo) {
    appendStringInfo(option, "\"logicEof\": %" PRId64, segfileinfo->eof[0]);
    appendStringInfo(option, ", \"uncompressedEof\": %lld, ",
                     segfileinfo->uncompressed_eof[0]);
  }

  appendStringInfo(
      option, "\"stripeSize\": %" PRId64,
      ((StdRdOptions *)(rel->rd_options))->stripesize * 1024 * 1024);
  appendStringInfo(option, ", \"rowIndexStride\": %" PRId64,
                   ((StdRdOptions *)(rel->rd_options))->rowindexstride);
  appendStringInfo(option, ", \"blockSize\": %" PRId64,
                   ((StdRdOptions *)(rel->rd_options))->compressblocksize);
  if (aoentry->compresstype)
    appendStringInfo(option, ", %s", aoentry->compresstype);

  // transform bloomfilter option from column names to column indexes.
  if (((StdRdOptions *)(rel->rd_options))->bloomfilter) {
    TupleDesc tupDesc = rel->rd_att;
    int attrNum = tupDesc->natts;
    char **attrNames = palloc0(attrNum * sizeof(char *));
    for (int i = 0; i < attrNum; ++i) {
      int nameLen =
          strlen(((Form_pg_attribute)(tupDesc->attrs[i]))->attname.data);
      char *attribute = palloc0(nameLen + 1);
      strncpy(attribute, ((Form_pg_attribute)(tupDesc->attrs[i]))->attname.data,
              nameLen);
      attrNames[i] = attribute;
    }

    StringInfoData bloomFilterOptsStr;
    initStringInfo(&bloomFilterOptsStr);
    char *token = strtok(((StdRdOptions *)(rel->rd_options))->bloomfilter, ",");
    while (token) {
      for (int attrIdx = 0; attrIdx < attrNum; ++attrIdx) {
        if (strncasecmp(token, attrNames[attrIdx],
                        strlen(attrNames[attrIdx])) == 0) {
          appendStringInfo(&bloomFilterOptsStr, "%d", attrIdx + 1);
          appendStringInfoChar(&bloomFilterOptsStr, ',');
          break;
        }
      }
      token = strtok(NULL, ",");
    }
    if (bloomFilterOptsStr.data[bloomFilterOptsStr.len - 1] == ',')
      bloomFilterOptsStr.data[bloomFilterOptsStr.len - 1] = '\0';
    appendStringInfo(option, ", \"bloomfilter\": [%s]",
                     pstrdup(bloomFilterOptsStr.data));
  }

  appendStringInfoChar(option, '}');
}

// FIXME(sxwang): In fact, upstream logic should ensure that FileSplit are all
// valid.
static bool IsValidFileSplit(FileSplit split, Oid idxId) {
  return split->replicaGroup_id == idxId;
}

// Filter invalid fileSplit.
static int32 GetSplitCount(List *fileSplits, Oid idxId) {
  int32 splitCount = list_length(fileSplits);
  int32 ret = 0;
  for (int32 i = 0; i < splitCount; ++i) {
    if (IsValidFileSplit(list_nth(fileSplits, i), idxId)) {
      ++ret;
    }
  }
  return ret;
}

OrcInsertDescData *orcBeginInsert(Relation rel,
                                  ResultRelSegFileInfo *segfileinfo) {
  OrcInsertDescData *insertDesc =
      (OrcInsertDescData *)palloc0(sizeof(OrcInsertDescData));
  insertDesc->memCxt = AllocSetContextCreate(
      CurrentMemoryContext, "NativeOrcInsertMemCxt", ALLOCSET_DEFAULT_MINSIZE,
      ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
  insertDesc->rel = rel;
  int hdfsPathMaxLen = AOSegmentFilePathNameLen(rel) + 1;
  char *hdfsPath = (char *)palloc0(hdfsPathMaxLen);
  MakeAOSegmentFileName(rel, segfileinfo->segno, -1, &insertDesc->segno,
                        hdfsPath);
  LockRelationAppendOnlySegmentFile(&rel->rd_node, segfileinfo->segno,
                                    AccessExclusiveLock,
                                    /* dontWait */ false);

  AppendOnlyEntry *aoentry =
      GetAppendOnlyEntry(RelationGetRelid(rel), SnapshotNow);
  StringInfoData option;
  constructOrcFormatOptionString(&option, rel, segfileinfo, aoentry);

  insertDesc->orcFormatData = palloc0(sizeof(OrcFormatData));
  insertDesc->orcFormatData->fmt =
      ORCFormatNewORCFormatC(option.data, segfileinfo->segno);
  initOrcFormatUserData(rel->rd_att, insertDesc->orcFormatData);

  addFilesystemCredential(hdfsPath);

  ORCFormatBeginInsertORCFormatFileC(
      insertDesc->orcFormatData->fmt, hdfsPath,
      insertDesc->orcFormatData->colNames,
      insertDesc->orcFormatData->colDatatypes,
      insertDesc->orcFormatData->colDatatypeMods,
      insertDesc->orcFormatData->numberOfColumns);

  pfree(hdfsPath);
  pfree(aoentry);

  checkOrcError(insertDesc->orcFormatData);

  return insertDesc;
}

Oid orcInsert(OrcInsertDescData *insertDesc, TupleTableSlot *tts) {
  slot_getallattrs(tts);
  bool *nulls = slot_get_isnull(tts);
  Datum *values = slot_get_values(tts);
  return orcInsertValues(insertDesc, values, nulls, tts->tts_tupleDescriptor);
}

static void convertAndFillIntoOrcFormatData(OrcFormatData *orcFormatData,
                                            Datum *values, bool *nulls,
                                            TupleDesc tupleDesc) {
  // Convert input slot and fill in write buffer
  for (int i = 0; i < orcFormatData->numberOfColumns; ++i) {
    int dataType = (int)(tupleDesc->attrs[i]->atttypid);

    orcFormatData->colRawValues[i] = NULL;

    if (nulls[i]) {
      orcFormatData->colRawValues[i] = dummyPlaceholder;
      continue;
    }

    if (dataType == HAWQ_TYPE_CHAR || dataType == HAWQ_TYPE_INT2 ||
        dataType == HAWQ_TYPE_INT4 || dataType == HAWQ_TYPE_INT8 ||
        dataType == HAWQ_TYPE_FLOAT4 || dataType == HAWQ_TYPE_FLOAT8 ||
        dataType == HAWQ_TYPE_BOOL || dataType == HAWQ_TYPE_TIME) {
      orcFormatData->colRawValues[i] = (char *)(&(values[i]));
    } else if (dataType == HAWQ_TYPE_TIMESTAMP ||
               dataType == HAWQ_TYPE_TIMESTAMPTZ) {
      int64_t *timestamp = (int64_t *)(&(values[i]));
      orcFormatData->colTimestamp[i].second =
          *timestamp / 1000000 +
          (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * 60 * 60 * 24;
      orcFormatData->colTimestamp[i].nanosecond = *timestamp % 1000000 * 1000;
      int64_t days = orcFormatData->colTimestamp[i].second / 60 / 60 / 24;
      if (orcFormatData->colTimestamp[i].nanosecond < 0 &&
          (days > POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE || days < 0))
        orcFormatData->colTimestamp[i].nanosecond += 1000000000;
      if (orcFormatData->colTimestamp[i].second < 0 &&
          orcFormatData->colTimestamp[i].nanosecond)
        orcFormatData->colTimestamp[i].second -= 1;
      orcFormatData->colRawValues[i] =
          (char *)(&(orcFormatData->colTimestamp[i]));
    } else if (dataType == HAWQ_TYPE_DATE) {
      int *date = (int *)(&(values[i]));
      *date += POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE;
      orcFormatData->colRawValues[i] = (char *)(&(values[i]));
    } else if (dataType == HAWQ_TYPE_NUMERIC) {
      Numeric num = DatumGetNumeric(values[i]);
      orcFormatData->colRawValues[i] = (char *)num;
      if (NUMERIC_IS_NAN(num)) nulls[i] = true;
    } else {
      // Check whether values[i] is fixed length udt.
      bool isFixedLengthType = tupleDesc->attrs[i]->attlen > 0 ? true : false;
      bool isPassByVal = tupleDesc->attrs[i]->attbyval;
      if (isFixedLengthType) {
        uint32_t dataLen = tupleDesc->attrs[i]->attlen;
        uint32_t totalLen = dataLen + sizeof(uint32_t);

        uint32_t tmpLen = __builtin_bswap32(totalLen);
        char *lenArr = (char *)(&tmpLen);
        memcpy(orcFormatData->colFixedLenUDT[i]->vl_len_, lenArr,
               sizeof(uint32_t));

        if (isPassByVal) {  // pass by val
          char *data = (char *)(&values[i]);
          memcpy(orcFormatData->colFixedLenUDT[i]->vl_dat, data, dataLen);
          orcFormatData->colRawValues[i] =
              (char *)(orcFormatData->colFixedLenUDT[i]);
        } else {  // pass by pointer
          char *data = (char *)(values[i]);
          memcpy(orcFormatData->colFixedLenUDT[i]->vl_dat, data, dataLen);
          orcFormatData->colRawValues[i] =
              (char *)(orcFormatData->colFixedLenUDT[i]);
        }
      } else {
        orcFormatData->colRawValues[i] = (char *)PG_DETOAST_DATUM(values[i]);
      }
    }
  }
}

Oid orcInsertValues(OrcInsertDescData *insertDesc, Datum *values, bool *nulls,
                    TupleDesc tupleDesc) {
  if (++insertDesc->insertCount % 2048 == 0)
    MemoryContextReset(insertDesc->memCxt);

  MemoryContext oldContext = MemoryContextSwitchTo(insertDesc->memCxt);

  OrcFormatData *orcFormatData = (OrcFormatData *)(insertDesc->orcFormatData);

  convertAndFillIntoOrcFormatData(orcFormatData, values, nulls, tupleDesc);

  MemoryContextSwitchTo(oldContext);

  ORCFormatInsertORCFormatC(orcFormatData->fmt, orcFormatData->colDatatypes,
                            orcFormatData->colRawValues, NULL, NULL, NULL,
                            nulls);
  checkOrcError(orcFormatData);

  PG_RETURN_OID(InvalidOid);
}

void orcEndInsert(OrcInsertDescData *insertDesc) {
  if (insertDesc->orcFormatData->fmt) {
    ORCFormatEndInsertORCFormatFileC(
        insertDesc->orcFormatData->fmt, &insertDesc->sendback->eof[0],
        &insertDesc->sendback->uncompressed_eof[0]);
    checkOrcError(insertDesc->orcFormatData);
    ORCFormatFreeORCFormatC(&insertDesc->orcFormatData->fmt);
  }

  insertDesc->sendback->segno = insertDesc->segno;
  insertDesc->sendback->insertCount = insertDesc->insertCount;
  insertDesc->sendback->numfiles = 1;

  MemoryContextResetAndDeleteChildren(insertDesc->memCxt);
  freeOrcFormatUserData(insertDesc->orcFormatData);
  pfree(insertDesc->orcFormatData);
  pfree(insertDesc);
}

void orcBeginScan(ScanState *scanState) {
  Assert(scanState->scan_state == SCAN_INIT ||
         scanState->scan_state == SCAN_DONE);

  Relation rel = scanState->ss_currentRelation;
  int natts = rel->rd_att->natts;
  bool *colToReads = palloc0(sizeof(bool) * natts);
  GetNeededColumnsForScan((Node *)scanState->ps.plan->targetlist, colToReads,
                          natts);
  GetNeededColumnsForScan((Node *)scanState->ps.plan->qual, colToReads, natts);

  ((OrcScanState *)scanState)->scandesc =
      orcBeginRead(rel, scanState->ps.state->es_snapshot, NULL,
                   scanState->splits, colToReads, scanState->ps.plan);

  pfree(colToReads);
  scanState->scan_state = SCAN_SCAN;
}

TupleTableSlot *orcScanNext(ScanState *scanState) {
  orcReadNext(((OrcScanState *)scanState)->scandesc,
              scanState->ss_ScanTupleSlot);
  return scanState->ss_ScanTupleSlot;
}

void orcEndScan(ScanState *scanState) {
  OrcScanDescData *scanDesc = ((OrcScanState *)scanState)->scandesc;

  orcEndRead(scanDesc);

  pfree(scanDesc);
  scanState->scan_state = SCAN_INIT;
}

void orcReScan(ScanState *scanState) {
  orcResetRead(((OrcScanState *)scanState)->scandesc);
}

OrcScanDescData *orcBeginRead(Relation rel, Snapshot snapshot, TupleDesc desc,
                              List *fileSplits, bool *colToReads,
                              void *pushDown) {
  return orcBeginReadWithOptionsStr(rel, snapshot, desc, fileSplits, colToReads,
                                    pushDown, "{}");
}

OrcScanDescData *orcBeginReadWithOptionsStr(Relation rel, Snapshot snapshot,
                                            TupleDesc desc, List *fileSplits,
                                            bool *colToReads, void *pushDown,
                                            const char *optsStr) {
  OrcScanDescData *scanDesc = palloc0(sizeof(OrcScanDescData));
  OrcFormatData *orcFormatData = scanDesc->orcFormatData =
      palloc0(sizeof(OrcFormatData));

  RelationIncrementReferenceCount(rel);

  if (desc == NULL)
    desc = RelationGetDescr(rel);

  scanDesc->rel = rel;
  orcFormatData->fmt = ORCFormatNewORCFormatC(optsStr, 0);
  initOrcFormatUserData(desc, orcFormatData);

  int32 splitCount = GetSplitCount(fileSplits, InvalidOid);
  ORCFormatFileSplit *splits = palloc0(sizeof(ORCFormatFileSplit) * splitCount);
  int32 filePathMaxLen = AOSegmentFilePathNameLen(rel) + 1;
  int32 orgSplitCount = list_length(fileSplits);
  for (int32 i = 0, j = 0; i < orgSplitCount; ++i) {
    FileSplit split = (FileSplitNode *)list_nth(fileSplits, i);
    if (IsValidFileSplit(split, InvalidOid)) {
      splits[j].start = split->offsets;
      splits[j].len = split->lengths;
      splits[j].eof = split->logiceof;

      if (split->ext_file_uri_string) {
        // XXX(chiyang): hack way to manage split info manually
        splits[j].fileName = split->ext_file_uri_string;
      } else {
        splits[j].fileName = palloc0(filePathMaxLen);
        MakeAOSegmentFileName(rel, split->segno, -1, dummyPlaceholder,
                              splits[j].fileName);
      }
      ++j;
    }
  }

  if (splitCount > 0)
    addFilesystemCredential(splits[0].fileName);

  void *qualList = NULL;
  CommonPlanContext ctx;
  ctx.univplan = NULL;
  Plan *plan = (Plan *)pushDown;
  if (strcasecmp(orc_enable_filter_pushdown, "ON") == 0 && plan &&
      list_length(plan->qual) > 0)
    qualList = convert_orcscan_qual_to_common_plan(plan, &ctx);

  ORCFormatBeginORCFormatC(orcFormatData->fmt, splits, splitCount, colToReads,
                           orcFormatData->colNames, orcFormatData->colDatatypes,
                           orcFormatData->colDatatypeMods,
                           orcFormatData->numberOfColumns, qualList);
  checkOrcError(orcFormatData);

  ItemPointerSetInvalid(&scanDesc->cdb_fake_ctid);

  for (int32 i = 0; i < splitCount; ++i)
    pfree(splits[i].fileName);
  pfree(splits);

  return scanDesc;
}

void orcIndexReadNext(OrcScanDescData *scanData, TupleTableSlot *slot,
                      List *columnsInIndex) {
  OrcFormatData *orcFormatData = scanData->orcFormatData;
  bool *nulls = slot_get_isnull(slot);
  bool *idxnulls = palloc0(sizeof(bool) * orcFormatData->numberOfColumns);
  memset(idxnulls, true, orcFormatData->numberOfColumns);
  Datum *values = slot_get_values(slot);
  TupleDesc tupleDesc = slot->tts_tupleDescriptor;
  int natts = tupleDesc->natts;
  memset(nulls, true, natts);

  uint64_t rowId;
  bool res = ORCFormatNextORCFormatWithRowIdC(
      orcFormatData->fmt, orcFormatData->colRawValues,
      orcFormatData->colValLength, idxnulls, &rowId);

  checkOrcError(orcFormatData);
  int idx = 0;
  if (res) {
    for (int32_t i = 0; i < natts; ++i) {
      // can't find the column in columnsInIndex
      if (!list_member_oid(columnsInIndex, i + 1)) continue;
      // index data is null
      if (idxnulls[idx]) {
        idx++;
        continue;
      }
      nulls[i] = false;
      switch (tupleDesc->attrs[i]->atttypid) {
        case HAWQ_TYPE_BOOL: {
          values[i] = BoolGetDatum(*(bool *)(orcFormatData->colRawValues[idx]));
          break;
        }
        case HAWQ_TYPE_INT2: {
          values[i] =
              Int16GetDatum(*(int16_t *)(orcFormatData->colRawValues[idx]));
          break;
        }
        case HAWQ_TYPE_INT4: {
          values[i] =
              Int32GetDatum(*(int32_t *)(orcFormatData->colRawValues[idx]));
          break;
        }
        case HAWQ_TYPE_INT8:
        case HAWQ_TYPE_TIME:
        case HAWQ_TYPE_TIMESTAMP:
        case HAWQ_TYPE_TIMESTAMPTZ: {
          values[i] =
              Int64GetDatum(*(int64_t *)(orcFormatData->colRawValues[idx]));
          break;
        }
        case HAWQ_TYPE_FLOAT4: {
          values[i] =
              Float4GetDatum(*(float *)(orcFormatData->colRawValues[idx]));
          break;
        }
        case HAWQ_TYPE_FLOAT8: {
          values[i] =
              Float8GetDatum(*(double *)(orcFormatData->colRawValues[idx]));
          break;
        }
        case HAWQ_TYPE_DATE: {
          values[i] =
              Int32GetDatum(*(int32_t *)(orcFormatData->colRawValues[idx]) -
                            POSTGRES_EPOCH_JDATE + UNIX_EPOCH_JDATE);
          break;
        }
        default: {
          // Check whether value[i] is fixed length udt.
          bool isFixedLengthType =
              tupleDesc->attrs[i]->attlen > 0 ? true : false;
          bool isPassByVal = tupleDesc->attrs[i]->attbyval;
          if (isFixedLengthType) {
            if (isPassByVal) {  // pass by val
              struct varlena *var =
                  (struct varlena *)(orcFormatData->colRawValues[idx]);
              uint32 valLen = *(uint32 *)(var->vl_len_);
              memcpy((void *)&values[i], var->vl_dat, valLen);
            } else {  // pass by pointer
              SET_VARSIZE((struct varlena *)(orcFormatData->colRawValues[idx]),
                          orcFormatData->colValLength[idx]);
              values[i] = PointerGetDatum(orcFormatData->colRawValues[idx] +
                                          sizeof(uint32_t));
            }
          } else {
            SET_VARSIZE((struct varlena *)(orcFormatData->colRawValues[idx]),
                        orcFormatData->colValLength[idx]);
            values[i] = PointerGetDatum(orcFormatData->colRawValues[idx]);
          }
          break;
        }
      }
      idx++;
    }
    TupSetVirtualTupleNValid(slot, slot->tts_tupleDescriptor->natts);
    ItemPointerSetRowIdToFakeCtid(&scanData->cdb_fake_ctid, rowId);
    slot_set_ctid(slot, &scanData->cdb_fake_ctid);
  } else {
    ExecClearTuple(slot);
  }
}

void orcReadNext(OrcScanDescData *scanData, TupleTableSlot *slot) {
  OrcFormatData *orcFormatData = scanData->orcFormatData;
  bool *nulls = slot_get_isnull(slot);
  Datum *values = slot_get_values(slot);
  memset(nulls, true, orcFormatData->numberOfColumns);
  TupleDesc tupleDesc = slot->tts_tupleDescriptor;

  uint64_t rowId;
  bool res = ORCFormatNextORCFormatWithRowIdC(
      orcFormatData->fmt, orcFormatData->colRawValues,
      orcFormatData->colValLength, nulls, &rowId);

  checkOrcError(orcFormatData);
  if (res) {
    for (int32_t i = 0; i < orcFormatData->numberOfColumns; ++i) {
      if (nulls[i])
        continue;

      switch (tupleDesc->attrs[i]->atttypid) {
        case HAWQ_TYPE_BOOL: {
          values[i] = BoolGetDatum(*(bool *)(orcFormatData->colRawValues[i]));
          break;
        }
        case HAWQ_TYPE_INT2: {
          values[i] =
              Int16GetDatum(*(int16_t *)(orcFormatData->colRawValues[i]));
          break;
        }
        case HAWQ_TYPE_INT4: {
          values[i] =
              Int32GetDatum(*(int32_t *)(orcFormatData->colRawValues[i]));
          break;
        }
        case HAWQ_TYPE_INT8:
        case HAWQ_TYPE_TIME:
        case HAWQ_TYPE_TIMESTAMP:
        case HAWQ_TYPE_TIMESTAMPTZ: {
          values[i] =
              Int64GetDatum(*(int64_t *)(orcFormatData->colRawValues[i]));
          break;
        }
        case HAWQ_TYPE_FLOAT4: {
          values[i] =
              Float4GetDatum(*(float *)(orcFormatData->colRawValues[i]));
          break;
        }
        case HAWQ_TYPE_FLOAT8: {
          values[i] =
              Float8GetDatum(*(double *)(orcFormatData->colRawValues[i]));
          break;
        }
        case HAWQ_TYPE_DATE: {
          values[i] =
              Int32GetDatum(*(int32_t *)(orcFormatData->colRawValues[i]) -
                            POSTGRES_EPOCH_JDATE + UNIX_EPOCH_JDATE);
          break;
        }
        default: {
          // Check whether value[i] is fixed length udt.
          bool isFixedLengthType =
              tupleDesc->attrs[i]->attlen > 0 ? true : false;
          bool isPassByVal = tupleDesc->attrs[i]->attbyval;
          if (isFixedLengthType) {
            if (isPassByVal) {  // pass by val
              struct varlena *var =
                  (struct varlena *)(orcFormatData->colRawValues[i]);
              uint32 valLen = *(uint32 *)(var->vl_len_);
              memcpy((void *)&values[i], var->vl_dat, valLen);
            } else {  // pass by pointer
              SET_VARSIZE((struct varlena *)(orcFormatData->colRawValues[i]),
                          orcFormatData->colValLength[i]);
              values[i] = PointerGetDatum(orcFormatData->colRawValues[i] +
                                          sizeof(uint32_t));
            }
          } else {
            SET_VARSIZE((struct varlena *)(orcFormatData->colRawValues[i]),
                        orcFormatData->colValLength[i]);
            values[i] = PointerGetDatum(orcFormatData->colRawValues[i]);
          }
          break;
        }
      }
    }
    TupSetVirtualTupleNValid(slot, slot->tts_tupleDescriptor->natts);
    ItemPointerSetRowIdToFakeCtid(&scanData->cdb_fake_ctid, rowId);
    slot_set_ctid(slot, &scanData->cdb_fake_ctid);
  } else {
    ExecClearTuple(slot);
  }
}

void orcEndRead(OrcScanDescData *scanData) {
  RelationDecrementReferenceCount(scanData->rel);

  if (scanData->orcFormatData->fmt) {
    ORCFormatEndORCFormatC(scanData->orcFormatData->fmt);
    checkOrcError(scanData->orcFormatData);
    ORCFormatFreeORCFormatC(&scanData->orcFormatData->fmt);
  }

  freeOrcFormatUserData(scanData->orcFormatData);
  pfree(scanData->orcFormatData);
}

void orcResetRead(OrcScanDescData *scanData) {
  ORCFormatRescanORCFormatC(scanData->orcFormatData->fmt);
  checkOrcError(scanData->orcFormatData);

  ItemPointerSetInvalid(&scanData->cdb_fake_ctid);
}

static int orcReadFully(const char *path, File *file, char *buf, int amount) {
  int nRead = 0;
  int ret = -1;
retry:
  ret = FileRead(file, buf + nRead, amount - nRead);
  if (ret > 0) {
    nRead += ret;
    if (nRead < amount)
      goto retry;
  } else if (ret < 0) {
    if (errno == EINTR)
      goto retry;
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not read file \"%s\": %m", path),
                    errdetail("%s", HdfsGetLastError())));
  } else { // EOF
  }
  return nRead;
}

static void orcWriteFully(const char *path, File *file, char *buf, int amount) {
  int nWrite = 0;
  int ret = -1;
retry:
  ret = FileWrite(file, buf + nWrite, amount - nWrite);
  if (ret >= 0) {
    nWrite += ret;
    if (nWrite < amount)
      goto retry;
  } else {
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not read file \"%s\": %m", path),
                    errdetail("%s", HdfsGetLastError())));
  }
}

static void orcCopyInternal(const char *srcPath, int64 eof,
                            const char *destPath, char *buffer) {
  File srcFile = PathNameOpenFile(srcPath, O_RDONLY, 0);
  if (srcFile < 0)
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not open file \"%s\": %m", srcPath),
                    errdetail("%s", HdfsGetLastError())));
  File destFile =
      PathNameOpenFile(destPath, O_WRONLY | O_APPEND | O_SYNC, 0600);
  if (destFile < 0)
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not open file \"%s\": %m", destPath),
                    errdetail("%s", HdfsGetLastError())));

  int64 start = 0;
  while (start < eof) {
    CHECK_FOR_INTERRUPTS();

    int32 bufferLen = (int32)Min(MAX_BUFFER_LEN, eof - start);
    orcReadFully(srcPath, srcFile, buffer, bufferLen);

    orcWriteFully(destPath, destFile, buffer, bufferLen);

    start += bufferLen;
  }

  if (FileSync(destFile) != 0)
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not sync file \"%s\": %m", destPath),
                    errdetail("%s", HdfsGetLastError())));

  FileClose(destFile);
  FileClose(srcFile);
}

static bool orcCopy(const char *name, tOffset size, void *arg) {
  if (size == 0)
    return false;

  OrcCopyContext *ctx = (OrcCopyContext *)arg;
  char *ptr = strrchr(name, '/');
  int32 segno = pg_atoi(ptr + 1, sizeof(int), 0);
  if (list_member_int(ctx->segNoList, segno))
    return false;

  sprintf(ctx->srcFile, "%s/%u", ctx->srcDir, segno);
  sprintf(ctx->destFile, "%s/%u", ctx->destDir, segno);
  orcCopyInternal(ctx->srcFile, size, ctx->destFile, ctx->buffer);

  return false;
}

static void copyFileForDirectDispatch(Oid relId, int32 targetSegNo,
                                      List *fileSplits, const char *oldPath,
                                      const char *newPath, int hdfsPathMaxLen) {
  List *segNoList = NIL;
  int32 splitCount = list_length(fileSplits);
  int32 prevSegNo = -1;
  for (int32 i = 0; i < splitCount; ++i) {
    int32 curSegNo = ((FileSplitNode *)list_nth(fileSplits, i))->segno;
    if (curSegNo != prevSegNo) {
      segNoList = lappend_int(segNoList, curSegNo);
      prevSegNo = curSegNo;
    }
  }

  ListCell *cell = NULL;
  foreach (cell, segNoList) {
    if (lfirst_int(cell) == targetSegNo)
      continue;

    QueryContextDispatchingSendBack sendback =
        CreateQueryContextDispatchingSendBack(1);
    sendback->relid = relId;
    sendback->segno = lfirst_int(cell);
    sendback->insertCount = 0;
    sendback->eof[0] = 0;
    sendback->uncompressed_eof[0] = 0;
    sendback->numfiles = 1;
    sendback->varblock = ORC_DIRECT_DISPATCH;
    StringInfo buf = PreSendbackChangedCatalog(1);
    AddSendbackChangedCatalogContent(buf, sendback);
    FinishSendbackChangedCatalog(buf);
  }

  OrcCopyContext ctx;
  ctx.buffer = palloc(MAX_BUFFER_LEN);
  ctx.destDir = newPath;
  ctx.srcDir = oldPath;
  ctx.segNoList = segNoList;
  ctx.destFile = (char *)palloc0(hdfsPathMaxLen);
  ctx.srcFile = (char *)palloc0(hdfsPathMaxLen);
  HdfsIterateFilesInDir(oldPath, orcCopy, (void *)&ctx);
  pfree(ctx.destFile);
  pfree(ctx.srcFile);
  pfree(ctx.buffer);
  if (segNoList)
    pfree(segNoList);
}

OrcDeleteDescData *orcBeginDelete(Relation rel, List *fileSplits,
                                  List *relFileNodeInfo, bool orderedRowId,
                                  bool directDispatch) {
  checkOushuDbExtensiveFeatureSupport("ORC DELETE");
  OrcDeleteDescData *deleteDesc =
      (OrcDeleteDescData *)palloc0(sizeof(OrcDeleteDescData));

  deleteDesc->rel = rel;
  deleteDesc->newSegno = GetQEIndex() + 1;
  deleteDesc->directDispatch = directDispatch;

  TupleDesc desc = RelationGetDescr(rel);
  AppendOnlyEntry *aoentry =
      GetAppendOnlyEntry(RelationGetRelid(rel), SnapshotNow);
  StringInfoData option;
  constructOrcFormatOptionString(&option, rel, NULL, aoentry);

  int hdfsPathMaxLen = AOSegmentFilePathNameLen(rel) + 1;
  char *hdfsPath = (char *)palloc0(hdfsPathMaxLen);
  RelFileNode newrnode = rel->rd_node;
  newrnode.relNode = InvalidOid;
  // find relfilenode for insert
  for (int32 i = 0; i < list_length(relFileNodeInfo); i += 2) {
    if (list_nth_oid(relFileNodeInfo, i) == RelationGetRelid(rel)) {
      newrnode.relNode = list_nth_oid(relFileNodeInfo, i + 1);
      break;
    }
  }
  char *basePath = relpath(newrnode);
  sprintf(hdfsPath, "%s/%u", basePath, deleteDesc->newSegno);

  int32 splitCount = list_length(fileSplits);
  ORCFormatFileSplit *splits = palloc0(sizeof(ORCFormatFileSplit) * splitCount);
  for (int32 i = 0; i < splitCount; ++i) {
    FileSplit split = (FileSplitNode *)list_nth(fileSplits, i);
    splits[i].start = split->offsets;
    splits[i].len = split->lengths;
    splits[i].eof = split->logiceof;
    splits[i].fileName = palloc0(hdfsPathMaxLen);
    MakeAOSegmentFileName(rel, split->segno, -1, dummyPlaceholder,
                          splits[i].fileName);
  }

  deleteDesc->orcFormatData = palloc0(sizeof(OrcFormatData));
  OrcFormatData *orcFormatData = deleteDesc->orcFormatData;
  initOrcFormatUserData(desc, orcFormatData);
  orcFormatData->fmt =
      ORCFormatNewORCFormatC(option.data, deleteDesc->newSegno);

  addFilesystemCredential(hdfsPath);

  deleteDesc->orcFormatData->updateDeleteFmt = OrcFormatBeginDeleteC(
      orcFormatData->fmt, splits, splitCount, orcFormatData->colNames,
      orcFormatData->colDatatypes, orcFormatData->colDatatypeMods,
      orcFormatData->numberOfColumns, hdfsPath, orderedRowId, gp_session_id,
      gp_command_count, deleteDesc->rel->rd_id, deleteDesc->newSegno,
      rm_seg_tmp_dirs, orc_update_delete_work_mem);
  checkOrcError(orcFormatData);

  if (directDispatch) {
    char *oldPath = relpath(rel->rd_node);
    copyFileForDirectDispatch(RelationGetRelid(rel), deleteDesc->newSegno,
                              fileSplits, oldPath, basePath, hdfsPathMaxLen);
    pfree(oldPath);
  }

  for (int32 i = 0; i < splitCount; ++i)
    pfree(splits[i].fileName);
  pfree(splits);
  pfree(basePath);
  pfree(hdfsPath);
  pfree(aoentry);

  return deleteDesc;
}

void orcDelete(OrcDeleteDescData *deleteDesc) {
  OrcFormatData *orcFormatData = deleteDesc->orcFormatData;

  ORCFormatDoDeleteC(orcFormatData->updateDeleteFmt,
                     DatumGetUInt64(deleteDesc->rowId));
  checkOrcError(orcFormatData);
}

uint64 orcEndDelete(OrcDeleteDescData *deleteDesc) {
  StorageFormatCallback callback;
  OrcFormatData *orcFormatData = deleteDesc->orcFormatData;

  if (orcFormatData->fmt) {
    ORCFormatEndDeleteC(orcFormatData->updateDeleteFmt);
    checkOrcError(orcFormatData);

    callback = StorageFormatDumpCallbackC(orcFormatData->updateDeleteFmt);
    deleteDesc->sendback->segno = deleteDesc->newSegno;
    deleteDesc->sendback->insertCount = callback.tupcount;
    deleteDesc->sendback->eof[0] = callback.eof;
    deleteDesc->sendback->uncompressed_eof[0] = callback.uncompressed_eof;
    deleteDesc->sendback->numfiles = 1;
    if (deleteDesc->directDispatch)
      deleteDesc->sendback->varblock = ORC_DIRECT_DISPATCH;

    ORCFormatFreeStorageFormatC(&orcFormatData->updateDeleteFmt);
    ORCFormatFreeORCFormatC(&orcFormatData->fmt);
  }

  freeOrcFormatUserData(orcFormatData);
  pfree(orcFormatData);
  pfree(deleteDesc);
  return callback.processedTupleCount;
}

OrcUpdateDescData *orcBeginUpdate(Relation rel, List *fileSplits,
                                  List *relFileNodeInfo, bool orderedRowId,
                                  bool directDispatch) {
  checkOushuDbExtensiveFeatureSupport("ORC UPDATE");
  OrcUpdateDescData *updateDesc =
      (OrcUpdateDescData *)palloc0(sizeof(OrcUpdateDescData));
  updateDesc->memCxt = AllocSetContextCreate(
      CurrentMemoryContext, "NativeOrcUpdateMemCxt", ALLOCSET_DEFAULT_MINSIZE,
      ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

  updateDesc->rel = rel;
  updateDesc->newSegno = GetQEIndex() + 1;
  updateDesc->directDispatch = directDispatch;

  TupleDesc desc = RelationGetDescr(rel);
  AppendOnlyEntry *aoentry =
      GetAppendOnlyEntry(RelationGetRelid(rel), SnapshotNow);
  StringInfoData option;
  constructOrcFormatOptionString(&option, rel, NULL, aoentry);

  int hdfsPathMaxLen = AOSegmentFilePathNameLen(rel) + 1;
  char *hdfsPath = (char *)palloc0(hdfsPathMaxLen);
  RelFileNode newrnode = rel->rd_node;
  newrnode.relNode = InvalidOid;
  // find relfilenode for insert
  for (int32 i = 0; i < list_length(relFileNodeInfo); i += 2) {
    if (list_nth_oid(relFileNodeInfo, i) == RelationGetRelid(rel)) {
      newrnode.relNode = list_nth_oid(relFileNodeInfo, i + 1);
      break;
    }
  }
  char *basePath = relpath(newrnode);
  sprintf(hdfsPath, "%s/%u", basePath, updateDesc->newSegno);

  int32 splitCount = list_length(fileSplits);
  ORCFormatFileSplit *splits = palloc0(sizeof(ORCFormatFileSplit) * splitCount);
  int32 filePathMaxLen = AOSegmentFilePathNameLen(rel) + 1;
  for (int32 i = 0; i < splitCount; ++i) {
    FileSplit split = (FileSplitNode *)list_nth(fileSplits, i);
    splits[i].start = split->offsets;
    splits[i].len = split->lengths;
    splits[i].eof = split->logiceof;
    splits[i].fileName = palloc0(filePathMaxLen);
    MakeAOSegmentFileName(rel, split->segno, -1, dummyPlaceholder,
                          splits[i].fileName);
  }

  updateDesc->orcFormatData = palloc0(sizeof(OrcFormatData));
  OrcFormatData *orcFormatData = updateDesc->orcFormatData;
  initOrcFormatUserData(desc, orcFormatData);
  orcFormatData->fmt =
      ORCFormatNewORCFormatC(option.data, updateDesc->newSegno);

  addFilesystemCredential(hdfsPath);

  updateDesc->orcFormatData->updateDeleteFmt = OrcFormatBeginUpdateC(
      orcFormatData->fmt, splits, splitCount, orcFormatData->colNames,
      orcFormatData->colDatatypes, orcFormatData->colDatatypeMods,
      orcFormatData->numberOfColumns, hdfsPath, orderedRowId, gp_session_id,
      gp_command_count, updateDesc->rel->rd_id, updateDesc->newSegno,
      rm_seg_tmp_dirs, orc_update_delete_work_mem);
  checkOrcError(orcFormatData);

  if (directDispatch) {
    char *oldPath = relpath(rel->rd_node);
    copyFileForDirectDispatch(RelationGetRelid(rel), updateDesc->newSegno,
                              fileSplits, oldPath, basePath, hdfsPathMaxLen);
    pfree(oldPath);
  }

  for (int32 i = 0; i < splitCount; ++i)
    pfree(splits[i].fileName);
  pfree(splits);
  pfree(basePath);
  pfree(hdfsPath);
  pfree(aoentry);

  return updateDesc;
}

void orcUpdate(OrcUpdateDescData *updateDesc) {
  OrcFormatData *orcFormatData = updateDesc->orcFormatData;

  if (++updateDesc->updateCount % 2048 == 0)
    MemoryContextReset(updateDesc->memCxt);
  MemoryContext oldContext = MemoryContextSwitchTo(updateDesc->memCxt);

  slot_getallattrs(updateDesc->slot);
  bool *nulls = slot_get_isnull(updateDesc->slot);
  Datum *values = slot_get_values(updateDesc->slot);
  convertAndFillIntoOrcFormatData(orcFormatData, values, nulls,
                                  updateDesc->slot->tts_tupleDescriptor);

  MemoryContextSwitchTo(oldContext);

  ORCFormatDoUpdateC(orcFormatData->updateDeleteFmt,
                     orcFormatData->colDatatypes, orcFormatData->colRawValues,
                     nulls, updateDesc->rowId);
  checkOrcError(orcFormatData);
}

uint64 orcEndUpdate(OrcUpdateDescData *updateDesc) {
  StorageFormatCallback callback;
  OrcFormatData *orcFormatData = updateDesc->orcFormatData;

  if (orcFormatData->fmt) {
    ORCFormatEndUpdateC(orcFormatData->updateDeleteFmt);
    checkOrcError(orcFormatData);

    callback = StorageFormatDumpCallbackC(orcFormatData->updateDeleteFmt);
    updateDesc->sendback->segno = updateDesc->newSegno;
    updateDesc->sendback->insertCount = callback.tupcount;
    updateDesc->sendback->eof[0] = callback.eof;
    updateDesc->sendback->uncompressed_eof[0] = callback.uncompressed_eof;
    updateDesc->sendback->numfiles = 1;
    if (updateDesc->directDispatch)
      updateDesc->sendback->varblock = ORC_DIRECT_DISPATCH;

    ORCFormatFreeStorageFormatC(&orcFormatData->updateDeleteFmt);
    ORCFormatFreeORCFormatC(&orcFormatData->fmt);
  }

  MemoryContextResetAndDeleteChildren(updateDesc->memCxt);
  freeOrcFormatUserData(orcFormatData);
  pfree(orcFormatData);
  pfree(updateDesc);
  return callback.processedTupleCount;
}

int64_t *orcCreateIndex(Relation rel, Oid idxId, List *segno, int64 *eof,
                        List *columnsToRead, int sortIdx) {
  checkOushuDbExtensiveFeatureSupport("ORC INDEX");
  OrcScanDescData *scanDesc = palloc0(sizeof(OrcScanDescData));
  OrcFormatData *orcFormatData = scanDesc->orcFormatData =
      palloc0(sizeof(OrcFormatData));

  RelationIncrementReferenceCount(rel);

  TupleDesc desc = RelationGetDescr(rel);

  scanDesc->rel = rel;
  orcFormatData->fmt = ORCFormatNewORCFormatC("{}", 0);
  initOrcFormatUserData(desc, orcFormatData);

  int32 splitCount = list_length(segno);
  int *columnsToReadList =
      palloc0(sizeof(int) * orcFormatData->numberOfColumns);
  for (int i = 0; i < list_length(columnsToRead); i++) {
    columnsToReadList[list_nth_int(columnsToRead, i) - 1] = 1;
  }
  int *sortIdxList = palloc0(sizeof(int) * orcFormatData->numberOfColumns);
  for (int i = 0; i < sortIdx; i++) {
    sortIdxList[i] = list_nth_int(columnsToRead, i) - 1;
  }
  int *segnoList = palloc0(sizeof(int) * splitCount);
  for (int i = 0; i < splitCount; i++) {
    segnoList[i] = list_nth_int(segno, i);
  }

  ORCFormatFileSplit *splits = palloc0(sizeof(ORCFormatFileSplit) * splitCount);
  int32 filePathMaxLen = AOSegmentFilePathNameLen(rel) + 1;
  for (int32 i = 0; i < splitCount; ++i) {
    splits[i].fileName = palloc0(filePathMaxLen);
    MakeAOSegmentFileName(rel, list_nth_int(segno, i), -1, dummyPlaceholder,
                          splits[i].fileName);
  }

  if (splitCount > 0) addFilesystemCredential(splits[0].fileName);
  RelationDecrementReferenceCount(rel);
  return ORCFormatCreateIndex(
      idxId, splits, splitCount, eof, columnsToReadList, sortIdxList, sortIdx,
      orcFormatData->colNames, orcFormatData->colDatatypes,
      orcFormatData->colDatatypeMods, orcFormatData->numberOfColumns,
      gp_session_id, rm_seg_tmp_dirs, segnoList);
}

static int OrcoidComparator(const void *arg1, const void *arg2) {
  Oid oid1 = *(const Oid *)arg1;
  Oid oid2 = *(const Oid *)arg2;

  if (oid1 > oid2) return 1;
  if (oid1 < oid2) return -1;
  return 0;
}

void orcBeginIndexOnlyScan(ScanState *scanState, Oid idxId,
                           List *columnsInIndex) {
  Assert(scanState->scan_state == SCAN_INIT ||
         scanState->scan_state == SCAN_DONE);
  Assert(length(columnsInIndex) > 0);

  Relation rel = scanState->ss_currentRelation;
  int natts = rel->rd_att->natts;
  bool *colToReads = palloc0(sizeof(bool) * natts);
  GetNeededColumnsForScan((Node *)scanState->ps.plan->targetlist, colToReads,
                          natts);
  OrcIndexOnlyScan *plan =
      (OrcIndexOnlyScan *)(((IndexScanState *)scanState)->ss.ps.plan);
  GetNeededColumnsForScan((Node *)(plan->indexqualorig), colToReads, natts);

  GetNeededColumnsForScan((Node *)scanState->ps.plan->qual, colToReads, natts);

  ((IndexScanState *)scanState)->scandesc = orcBeginIndexOnlyRead(
      rel, idxId, columnsInIndex, scanState->ps.state->es_snapshot, NULL,
      scanState->splits, colToReads, scanState->ps.plan);

  pfree(colToReads);
  scanState->scan_state = SCAN_SCAN;
}

TupleTableSlot *orcIndexOnlyScanNext(ScanState *scanState) {
  orcIndexReadNext(
      ((IndexScanState *)scanState)->scandesc, scanState->ss_ScanTupleSlot,
      ((IndexScan *)(((IndexScanState *)scanState)->ss.ps.plan))->idxColummns);
  return scanState->ss_ScanTupleSlot;
}

void orcEndIndexOnlyScan(ScanState *scanState) {
  if ((((IndexScanState *)scanState)->ss.scan_state & SCAN_SCAN) != 0) {
    OrcScanDescData *scanDesc = ((IndexScanState *)scanState)->scandesc;

    orcEndRead(scanDesc);

    pfree(scanDesc);
    scanState->scan_state = SCAN_INIT;
  }
}

void orcIndexOnlyReScan(ScanState *scanState) {
  orcResetRead(((IndexScanState *)scanState)->scandesc);
}

OrcScanDescData *orcBeginIndexOnlyRead(Relation rel, Oid idxId,
                                       List *columnsInIndex, Snapshot snapshot,
                                       TupleDesc desc, List *fileSplits,
                                       bool *colToReads, void *pushDown) {
  OrcScanDescData *scanDesc = palloc0(sizeof(OrcScanDescData));
  OrcFormatData *orcFormatData = scanDesc->orcFormatData =
      palloc0(sizeof(OrcFormatData));

  RelationIncrementReferenceCount(rel);

  if (desc == NULL) desc = RelationGetDescr(rel);

  scanDesc->rel = rel;
  orcFormatData->fmt = ORCFormatNewORCFormatC("{}", 0);
  ORCFormatSetIndexFlag(orcFormatData->fmt);
  bool *colToReadInIndex =
      palloc0(sizeof(bool) * (list_length(columnsInIndex) + 1));
  initOrcFormatIndexUserData(desc, orcFormatData, colToReads, columnsInIndex,
                             colToReadInIndex);

  int32 splitCount = GetSplitCount(fileSplits, idxId);
  ORCFormatFileSplit *splits = palloc0(sizeof(ORCFormatFileSplit) * splitCount);
  int lenOfIdxId = 0;
  Oid idxCount = idxId;
  while (idxCount) {
    lenOfIdxId++;
    idxCount /= 10;
  }
  if (lenOfIdxId == 0) lenOfIdxId = 1;
  int32 filePathMaxLen = AOSegmentFilePathNameLen(rel) + lenOfIdxId + 2;
  int32 orgSplitCount = list_length(fileSplits);
  for (int32 i = 0, j = 0; i < orgSplitCount; ++i) {
    FileSplit split = (FileSplitNode *)list_nth(fileSplits, i);
    if (IsValidFileSplit(split, idxId)) {
      splits[j].start = split->offsets;
      splits[j].len = split->lengths;
      splits[j].eof = split->logiceof;
      splits[j].fileName = palloc0(filePathMaxLen);
      MakeAOSegmentIndexFileName(rel, idxId, split->segno, -1, dummyPlaceholder,
                                 splits[j].fileName);
      ++j;
    }
  }

  if (splitCount > 0) addFilesystemCredential(splits[0].fileName);

  void *qualList = NULL;
  CommonPlanContext ctx;
  ctx.univplan = NULL;
  Plan *plan = (Plan *)pushDown;

  /*
   * 1. the varattno of indexqualorig consistent with table columns info
   * 2. the varattno of indexqualorig does not match the index file
   * 3. adjust the "columnsInIndex" order
   * 4. fix indexqualorig varattno utilization "columnsInIndex" in
   * do_convert_expr_to_common_plan to be consistent with index file
   */
  int len = length(columnsInIndex);
  Oid *value = palloc(len * sizeof(Oid));
  for (int i = 0; i < len; ++i) {
    value[i] = list_nth_oid(columnsInIndex, i);
  }
  qsort(value, len, sizeof(Oid), OrcoidComparator);
  List *colIdxs = NIL;
  for (int i = 0; i < len; ++i) {
    colIdxs = lappend_oid(colIdxs, value[i]);
  }
  pfree(value);
  qualList = convert_orcscan_indexqualorig_to_common_plan(plan, &ctx, colIdxs);

  ORCFormatBeginORCFormatC(
      orcFormatData->fmt, splits, splitCount, colToReadInIndex,
      orcFormatData->colNames, orcFormatData->colDatatypes,
      orcFormatData->colDatatypeMods, orcFormatData->numberOfColumns, qualList);
  checkOrcError(orcFormatData);

  ItemPointerSetInvalid(&scanDesc->cdb_fake_ctid);

  for (int32 i = 0; i < splitCount; ++i) pfree(splits[i].fileName);
  pfree(splits);

  return scanDesc;
}

bool isDirectDispatch(Plan *plan) {
  return plan->directDispatch.isDirectDispatch;
}
