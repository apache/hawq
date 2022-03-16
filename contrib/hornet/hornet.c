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

#include <inttypes.h>

#include "funcapi.h"

#include "access/fileam.h"
#include "access/filesplit.h"
#include "access/orcam.h"
#include "catalog/pg_exttable.h"
#include "hdfs/hdfs.h"
#include "storage/cwrapper/orc-format-c.h"
#include "storage/fd.h"
#include "storage/filesystem.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/hawq_funcoid_mapping.h"
#include "utils/lsyscache.h"

Datum ls_hdfs_dir(PG_FUNCTION_ARGS);

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(ls_hdfs_dir);

struct LsHdfsDirInfo {
  hdfsFS fs;
  hdfsFileInfo *fileInfos;
  char hdfsPrefix[HOSTNAME_MAX_LENGTH];
};

#define NUM_LS_HDFS_DIR 5
Datum ls_hdfs_dir(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx;

  if (SRF_IS_FIRSTCALL()) {
    funcctx = SRF_FIRSTCALL_INIT();
    TupleDesc tupdesc =
        CreateTemplateTupleDesc(NUM_LS_HDFS_DIR, false /* hasoid */);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "file_replication", INT4OID, -1,
                       0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "file_owner", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "file_group", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "file_size", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "file_name", TEXTOID, -1, 0);

    funcctx->tuple_desc = BlessTupleDesc(tupdesc);

    MemoryContext oldcontext =
        MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    const char *url = DatumGetCString(
        DirectFunctionCall1(textout, PointerGetDatum(PG_GETARG_TEXT_P(0))));
    char *host = NULL, *protocol = NULL;
    int port;

    if (HdfsParsePath(url, &protocol, &host, &port, NULL) != 0) {
      ereport(ERROR,
              (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
               errmsg("invalid HDFS URL: \"%s\"", url), errOmitLocation(true)));
      funcctx = SRF_PERCALL_SETUP();
      SRF_RETURN_DONE(funcctx);
    }
    struct LsHdfsDirInfo *info = palloc0(sizeof(struct LsHdfsDirInfo));
    const char *path = strstr(strstr(url, "://") + 3, "/");
    strncpy(info->hdfsPrefix, url, path - url);

    struct hdfsBuilder *builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, host);
    if (port != 0)
      hdfsBuilderSetNameNodePort(builder, port);
    info->fs = hdfsBuilderConnect(builder);
    hdfsFreeBuilder(builder);

    if (info->fs == NULL) {
      ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                      errmsg("fail to connect HDFS URL: \"%s\"", url),
                      errOmitLocation(true)));
      funcctx = SRF_PERCALL_SETUP();
      SRF_RETURN_DONE(funcctx);
    }

    info->fileInfos =
        hdfsListDirectory(info->fs, path, (int *)&funcctx->max_calls);
    funcctx->user_fctx = info;

    MemoryContextSwitchTo(oldcontext);
  }

  funcctx = SRF_PERCALL_SETUP();
  struct LsHdfsDirInfo *info = funcctx->user_fctx;

  if (funcctx->call_cntr < funcctx->max_calls) {
    Datum values[NUM_LS_HDFS_DIR];
    bool nulls[NUM_LS_HDFS_DIR];
    MemSet(nulls, 0, sizeof(nulls));

    char *fileName =
        palloc(strlen(info->hdfsPrefix) +
               strlen(info->fileInfos[funcctx->call_cntr].mName) + 1);
    fileName[0] = '\0';
    strcat(fileName, info->hdfsPrefix);
    strcat(fileName, info->fileInfos[funcctx->call_cntr].mName);
    values[0] =
        UInt64GetDatum(info->fileInfos[funcctx->call_cntr].mReplication);
    values[1] = PointerGetDatum(
        cstring_to_text(info->fileInfos[funcctx->call_cntr].mOwner));
    values[2] = PointerGetDatum(
        cstring_to_text(info->fileInfos[funcctx->call_cntr].mGroup));
    values[3] = UInt64GetDatum(info->fileInfos[funcctx->call_cntr].mSize);
    values[4] = PointerGetDatum(cstring_to_text(fileName));

    HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
    Datum result = HeapTupleGetDatum(tuple);

    pfree(fileName);

    SRF_RETURN_NEXT(funcctx, result);
  } else {
    hdfsFreeFileInfo(info->fileInfos, funcctx->max_calls);
    hdfsDisconnect(info->fs);
    pfree(info);
    SRF_RETURN_DONE(funcctx);
  }
}

Datum is_supported_proc_in_NewQE(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(is_supported_proc_in_NewQE);
Datum is_supported_proc_in_NewQE(PG_FUNCTION_ARGS) {
  Oid a = PG_GETARG_OID(0);
  int32_t mappingFuncId = HAWQ_FUNCOID_MAPPING(a);
  PG_RETURN_BOOL(!(IS_HAWQ_MAPPING_FUNCID_INVALID(mappingFuncId)));
}

Datum orc_tid_scan(FunctionCallInfo fcinfo, int segno, const char *url,
                   uint64_t tid) {
  Assert(segno == 0 || url == NULL);

  // Argument checking
  Oid argtype = get_fn_expr_argtype(fcinfo->flinfo, 0);
  if (!type_is_rowtype(argtype))
    ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
             errmsg("first argument of %s must be a row type", __func__)));
  Oid relId = get_typ_typrelid(argtype);
  char relStorage = get_rel_relstorage(relId);
  if (relstorage_is_external(relStorage)) {
    ExtTableEntry *extEntry = GetExtTableEntry(relId);
    const char *fmtName = getExtTblFormatterTypeInFmtOptsStr(extEntry->fmtopts);
    if (strcasecmp("orc", fmtName) != 0) {
      ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("Invalid external table type of %s for ORC Table.",
                             fmtName)));
    }
    if (segno > 0) {
      ereport(ERROR,
              (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
               errmsg("Expecting URL for external ORC Table.", fmtName)));
    }
  } else if (RELSTORAGE_ORC != relStorage) {
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Invalid table type of '%c' for ORC Table.",
                           get_rel_relstorage(relId))));
  }

  // Retrieve output tuple description
  TupleDesc tupdesc;
  if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("function returning record called in context "
                           "that cannot accept type record")));
  TupleTableSlot *slot = TupleDescGetSlot(tupdesc);

  // Setup projection
  bool *proj = (bool *)palloc(tupdesc->natts * sizeof(bool));
  if (fcinfo->nargs == 4) {  // specify the attribute to project
    ArrayType *arr = PG_GETARG_ARRAYTYPE_P(3);
    size_t num = (ARR_SIZE(arr) - ARR_DATA_OFFSET(arr)) / sizeof(int32_t);
    int32_t *attrNums = (int32_t *)ARR_DATA_PTR(arr);
    memset(proj, 0, tupdesc->natts);
    for (size_t i = 0; i < num; i++) {
      if (attrNums[i] <= 0 || attrNums[i] > tupdesc->natts)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Invalid attribute number of %" PRId32 ".",
                               attrNums[i])));
      proj[attrNums[i] - 1] = 1;
    }
  } else {  // scan the whole tuple
    memset(proj, 1, tupdesc->natts);
  }

  // Construct file splits
  FileSplit split = makeNode(FileSplitNode);
  split->segno = segno;
  if (segno == 0) split->ext_file_uri_string = (char *)url;
  split->offsets = 0;
  split->lengths = INT64_MAX;
  split->logiceof = INT64_MAX;
  List *fileSplits = list_make1(split);

  Relation rel = RelationIdGetRelation(relId);
  OrcScanDescData *scanDesc =
      orcBeginReadWithOptionsStr(rel, ActiveSnapshot, NULL, fileSplits, proj,
                                 NULL, "{\"format\": \"APACHE_ORC_FORMAT\"}");
  RelationClose(rel);

  // XXX(chiyang): hack way to directly get `ORCFormatC *fmt;`, which is defined
  // inside orcam.c.
  bool scanSucceed =
      ORCFormatTidScanC(*(ORCFormatC **)scanDesc->orcFormatData, tid);
  checkOrcError(scanDesc->orcFormatData);
  if (scanSucceed) {
    orcReadNext(scanDesc, slot);

    // Materialize the tuple
    Datum *values = slot_get_values(slot);
    bool *nulls = slot_get_isnull(slot);
    for (size_t idx = 0; idx < tupdesc->natts; idx++) {
      if (!nulls[idx]) {
        values[idx] = datumCopy(values[idx], tupdesc->attrs[idx]->attbyval,
                                tupdesc->attrs[idx]->attlen);
      }
    }
  } else {
    orcEndRead(scanDesc);
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("TID %" PRIu64 " exceeds file tuple count.", tid)));
  }
  orcEndRead(scanDesc);

  HeapTuple retTuple =
      heap_form_tuple(tupdesc, slot_get_values(slot), slot_get_isnull(slot));

  PG_RETURN_DATUM(HeapTupleGetDatum(retTuple));
}

Datum orc_segno_tid_scan(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(orc_segno_tid_scan);
Datum orc_segno_tid_scan(PG_FUNCTION_ARGS) {
  int segno = PG_GETARG_INT32(1);
  uint64_t tid = PG_GETARG_INT64(2);

  return orc_tid_scan(fcinfo, segno, NULL, tid);
}

Datum orc_url_tid_scan(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(orc_url_tid_scan);
Datum orc_url_tid_scan(PG_FUNCTION_ARGS) {
  const char *url = DatumGetCString(
      DirectFunctionCall1(textout, PointerGetDatum(PG_GETARG_TEXT_P(1))));
  uint64_t tid = PG_GETARG_INT64(2);

  return orc_tid_scan(fcinfo, 0, url, tid);
}
