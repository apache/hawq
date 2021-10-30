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

#include "funcapi.h"

#include "hdfs/hdfs.h"
#include "storage/fd.h"
#include "storage/filesystem.h"
#include "utils/builtins.h"

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
