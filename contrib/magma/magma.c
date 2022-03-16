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

#include <json-c/json.h>

#include "c.h"
#include "port.h"
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "access/extprotocol.h"
#include "access/filesplit.h"
#include "access/fileam.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/plugstorage.h"
#include "access/tupdesc.h"
#include "access/transam.h"
#include "catalog/namespace.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_attribute.h"
#include "cdb/cdbdatalocality.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbvars.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/dbcommands.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "optimizer/newPlanner.h"
#include "parser/parse_type.h"
#include "postmaster/identity.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/hawq_type_mapping.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/uri.h"

#include "storage/cwrapper/magma-format-c.h"
#include "magma/cwrapper/magma-client-c.h"
#include "univplan/cwrapper/univplan-c.h"

/*
 * Do the module magic dance
 */
PG_MODULE_MAGIC;

/*
 * Validators for magma protocol in pluggable storage
 */
PG_FUNCTION_INFO_V1(magma_protocol_blocklocation);
PG_FUNCTION_INFO_V1(magma_protocol_tablesize);
PG_FUNCTION_INFO_V1(magma_protocol_databasesize);
PG_FUNCTION_INFO_V1(magma_protocol_validate);

/*
 * Validators for magma format in pluggable storage
 */
PG_FUNCTION_INFO_V1(magma_validate_interfaces);
PG_FUNCTION_INFO_V1(magma_validate_options);
PG_FUNCTION_INFO_V1(magma_validate_encodings);
PG_FUNCTION_INFO_V1(magma_validate_datatypes);

/*
 * Accessors for magma format in pluggable storage
 */
PG_FUNCTION_INFO_V1(magma_createtable);
PG_FUNCTION_INFO_V1(magma_droptable);
PG_FUNCTION_INFO_V1(magma_beginscan);
PG_FUNCTION_INFO_V1(magma_getnext_init);
PG_FUNCTION_INFO_V1(magma_getnext);
PG_FUNCTION_INFO_V1(magma_rescan);
PG_FUNCTION_INFO_V1(magma_endscan);
PG_FUNCTION_INFO_V1(magma_stopscan);
PG_FUNCTION_INFO_V1(magma_begindelete);
PG_FUNCTION_INFO_V1(magma_delete);
PG_FUNCTION_INFO_V1(magma_enddelete);
PG_FUNCTION_INFO_V1(magma_beginupdate);
PG_FUNCTION_INFO_V1(magma_update);
PG_FUNCTION_INFO_V1(magma_endupdate);
PG_FUNCTION_INFO_V1(magma_insert_init);
PG_FUNCTION_INFO_V1(magma_insert);
PG_FUNCTION_INFO_V1(magma_insert_finish);

/*
 * Transaction for magma format
 */
PG_FUNCTION_INFO_V1(magma_transaction);

/*
 * Definitions of validators for magma protocol in pluggable storage
 */
Datum magma_protocol_blocklocation(PG_FUNCTION_ARGS);
Datum magma_protocol_validate(PG_FUNCTION_ARGS);
Datum magma_getstatus(PG_FUNCTION_ARGS);
/*
 * Definitions of validators for magma format in pluggable storage
 */
Datum magma_validate_interfaces(PG_FUNCTION_ARGS);
Datum magma_validate_options(PG_FUNCTION_ARGS);
Datum magma_validate_encodings(PG_FUNCTION_ARGS);
Datum magma_validate_datatypes(PG_FUNCTION_ARGS);

/*
 * Definitions of accessors for magma format in pluggable storage
 */
Datum magma_createtable(PG_FUNCTION_ARGS);
Datum magma_droptable(PG_FUNCTION_ARGS);
Datum magma_beginscan(PG_FUNCTION_ARGS);
Datum magma_getnext_init(PG_FUNCTION_ARGS);
Datum magma_getnext(PG_FUNCTION_ARGS);
Datum magma_rescan(PG_FUNCTION_ARGS);
Datum magma_endscan(PG_FUNCTION_ARGS);
Datum magma_stopscan(PG_FUNCTION_ARGS);
Datum magma_begindelete(PG_FUNCTION_ARGS);
Datum magma_delete(PG_FUNCTION_ARGS);
Datum magma_enddelete(PG_FUNCTION_ARGS);
Datum magma_beginupdate(PG_FUNCTION_ARGS);
Datum magma_update(PG_FUNCTION_ARGS);
Datum magma_endupdate(PG_FUNCTION_ARGS);
Datum magma_insert_init(PG_FUNCTION_ARGS);
Datum magma_insert(PG_FUNCTION_ARGS);
Datum magma_insert_finish(PG_FUNCTION_ARGS);

/*
 * Definitions of accessors for magma format index in pluggable storage
 */
Datum magma_createindex(PG_FUNCTION_ARGS);
Datum magma_dropindex(PG_FUNCTION_ARGS);
Datum magma_reindex_index(PG_FUNCTION_ARGS);

/*
 * Definition of transaction for magma format
 */
Datum magma_transaction(PG_FUNCTION_ARGS);

typedef struct {
  int64_t second;
  int64_t nanosecond;
} TimestampType;

typedef struct MagmaTidC {
  uint64_t rowid;
  uint16_t rangeid;
} MagmaTidC;

typedef struct MagmaFormatUserData {
  MagmaFormatC *fmt;
  char *dbname;
  char *schemaname;
  char *tablename;
  bool isMagmatp;
  int *colIndexes;
  bool *colIsNulls;

  char **colNames;
  int *colDatatypes;
  int64_t *colDatatypeMods;
  int32_t numberOfColumns;
  char **colRawValues;
  Datum *colValues;
  uint64_t *colValLength;
  bool *colToReads;
  char *colRawTid;
  MagmaTidC colTid;

  // for insert/update/delete
  TimestampType *colTimestamp;

  bool isFirstRescan;
} MagmaFormatUserData;

static MagmaClientC *magma_client_instance;

/*
 * Utility functions for magma in pluggable storage
 */
static void init_common_plan_context(CommonPlanContext *ctx);
static void free_common_plan_context(CommonPlanContext *ctx);
static FmgrInfo *get_magma_function(char *formatter_name, char *function_name);
static void get_magma_category_info(char *fmtoptstr, bool *isexternal);
static void get_magma_scan_functions(char *formatter_name,
                                     FileScanDesc file_scan_desc);
static void get_magma_insert_functions(char *formatter_name,
                                       ExternalInsertDesc ext_insert_desc);
static void get_magma_delete_functions(char *formatter_name,
                                       ExternalInsertDesc ext_delete_desc);
static void get_magma_update_functions(char *formatter_name,
                                       ExternalInsertDesc ext_update_desc);

static MagmaFormatC *create_magma_formatter_instance(List *fmt_opts_defelem,
                                                     char *serializeSchema,
                                                     int serializeSchemaLen,
                                                     int fmt_encoding,
                                                     char *formatterName,
                                                     int rangeNum);

static MagmaClientC *create_magma_client_instance();
static void init_magma_format_user_data_for_read(
    TupleDesc tup_desc, MagmaFormatUserData *user_data);
static void init_magma_format_user_data_for_write(
    TupleDesc tup_desc, MagmaFormatUserData *user_data, Relation relation);

static void build_options_in_json(char *serializeSchema, int serializeSchemaLen,
                                  List *fmt_opts_defelem, int encoding, int rangeNum,
                                  char *formatterName, char **json_str);
static void build_magma_tuple_descrition_for_read(
    Plan *plan, Relation relation, MagmaFormatUserData *user_data, bool skipTid);

static void magma_scan_error_callback(void *arg);

static List *magma_parse_format_string(char *fmtname, char **fmtstr);
static char *magma_strtokx2(const char *s, const char *whitespace,
                            const char *delim, const char *quote, char escape,
                            bool e_strings, bool del_quotes, int encoding);
static void magma_strip_quotes(char *source, char quote, char escape,
                               int encoding);

static void magma_check_result(MagmaClientC **client);

static bool checkUnsupportedDataTypeMagma(int32_t hawqTypeID);

int32_t map_hawq_type_to_magma_type(int32_t hawqTypeID, bool isMagmatp);

char *search_hostname_by_ipaddr(const char *ipaddr);

static void getHostNameByIp(const char *ipaddr, char *hostname);

static void magma_clear(PlugStorage ps, bool clearSlot) {
  FileScanDesc fsd = ps->ps_file_scan_desc;
  MagmaFormatUserData *user_data = (MagmaFormatUserData *)(fsd->fs_ps_user_data);
  TupleTableSlot *slot = ps->ps_tuple_table_slot;

  if (user_data->fmt) {
    MagmaFormatCatchedError *e = MagmaFormatGetErrorMagmaFormatC(user_data->fmt);
    if (e->errCode == ERRCODE_SUCCESSFUL_COMPLETION) {
      MagmaFormatEndScanMagmaFormatC(user_data->fmt);
      e = MagmaFormatGetErrorMagmaFormatC(user_data->fmt);
      if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
        ereport(ERROR, (errcode(e->errCode), errmsg("MAGMA:%s", e->errMessage)));
      }

      MagmaFormatFreeMagmaFormatC(&(user_data->fmt));

      // call getnext should clear data resource
      if (clearSlot) {
        pfree(user_data->colRawValues);
        pfree(user_data->colValues);
        pfree(user_data->colToReads);
        pfree(user_data->colValLength);
        for (int i = 0; i < user_data->numberOfColumns; ++i)
          pfree(user_data->colNames[i]);
        pfree(user_data->colNames);
        pfree(user_data->colDatatypes);
        pfree(user_data->colDatatypeMods);
        pfree(user_data->colIsNulls);
        pfree(user_data);
        fsd->fs_ps_user_data = NULL;

        ps->ps_has_tuple = false;
        slot->PRIVATE_tts_values = NULL;
        ExecClearTuple(slot);
      }
    } else {
      ereport(ERROR, (errcode(e->errCode), errmsg("MAGMA:%s", e->errMessage)));
    }
  }
}

static inline void ConvertTidToCtidAndRangeid(const MagmaTidC tid,
                                                ItemPointerData *ctid,
                                                uint32_t *tts_rangeid) {
  // MagmaTidC tidVal = *(MagmaTidC *)DatumGetPointer(tid);
  /* put low 48 bits rowid in ctid and high 16 bits rowid in tts_rangeid. */
  ctid->ip_blkid.bi_hi = (uint16) (tid.rowid >> 32);
  ctid->ip_blkid.bi_lo = (uint16) (tid.rowid >> 16);
  ctid->ip_posid = tid.rowid;
  *tts_rangeid = ((uint32)(tid.rowid >> 32) & 0xFFFF0000) | (uint32)tid.rangeid;
  return;
}

static inline List *SortMagmaFilesByRangeId(List *files, int32_t length) {
  List *sortedFiles = list_copy(files);
  ListCell *cell;
  blocklocation_file *blf;
  uint16_t rangeId;
  for (int i = 0; i < length; i++) {
    cell = list_nth_cell(files, i);
    blf = (blocklocation_file *)(cell->data.ptr_value);
    Assert(blf->block_num > 0 && blf->locations);
    rangeId = blf->locations[0].rangeId;
    list_nth_replace(sortedFiles, rangeId, blf);
  }
  list_free(files);
  return sortedFiles;
}

/*
 * Get magma node status
 */
Datum magma_getstatus(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  elog(DEBUG1, "magma_getstatus begin");
  ExtProtocolMagmaInfo magmadata =
      palloc0(sizeof(ExtProtocolMagmaStatusData));
  if (magmadata == NULL) {
    elog(ERROR, "magma_getstatus: failed to allocate new space");
  }
  magmadata->type = T_ExtProtocolMagmaStatusData;
  fcinfo->resultinfo = magmadata;

  MagmaClientC *client = create_magma_client_instance();
  if (client == NULL) {
    elog(ERROR, "magma_getstatus failed to connect to magma service");
  }
  magmadata->magmaNodes = MagmaClientC_GetMagmaStatus(client, &(magmadata->size));
  magma_check_result(&client);

  elog(DEBUG1, "magma_getstatus end");
  PG_RETURN_VOID();
}

/*
 * Implementation of blocklocation for magma protocol in pluggable storage
 */
Datum magma_protocol_blocklocation(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  elog(DEBUG3, "magma_protocol_blocklocation begin");
  /*
   * Step 1. prepare instances
   */
  /* Build the result instance and basic properties */
  ExtProtocolBlockLocation bldata =
      palloc0(sizeof(ExtProtocolBlockLocationData));

  if (bldata == NULL) {
    elog(ERROR,
         "magma_protocol_blocklocation: failed to allocate new space");
  }
  bldata->type = T_ExtProtocolBlockLocationData;
  fcinfo->resultinfo = bldata;

  /* Build validator data */
  ExtProtocolValidator pvalidator_data =
      (ExtProtocolValidator)(fcinfo->context);
  List *fmt_opts = pvalidator_data->format_opts;
  char *dbname = pvalidator_data->dbname;
  char *schemaname = pvalidator_data->schemaname;
  char *tablename = pvalidator_data->tablename;
  bool useClientCacheDirectly = pvalidator_data->useClientCacheDirectly;

  MagmaSnapshot *snapshot = &(pvalidator_data->snapshot);

  char *format_str = pstrdup((char *)strVal(linitial(fmt_opts)));
  /*
   * Step 2. get table schema and range distribution
   */
  char *fmt_name = NULL;
  List *l = magma_parse_format_string(format_str, &fmt_name);
  pfree(format_str);

  MagmaClientC *client = create_magma_client_instance();
  if (client == NULL) {
    elog(ERROR, "failed to connect to magma service");
  }

  int16_t tableType = 0;
  if (pg_strncasecmp(
      fmt_name, MAGMA_STORAGE_TYPE_TP, MAGMA_STORAGE_TYPE_TP_LEN) == 0) {
    tableType = MAGMACLIENTC_TABLETYPE_TP;
  } else if (pg_strncasecmp(fmt_name, MAGMA_STORAGE_TYPE_AP,
                            MAGMA_STORAGE_TYPE_AP_LEN) == 0) {
    tableType = MAGMACLIENTC_TABLETYPE_AP;
  } else {
    elog(ERROR,
         "magma_get_blocklocation: failed to recognize table format type: [%s]",
         fmt_name);
  }
  MagmaClientC_SetupTableInfo(client, dbname, schemaname, tablename, tableType);
  MagmaClientC_SetupSnapshot(client, snapshot);
  MagmaTablePtr table = MagmaClientC_FetchTable(client, useClientCacheDirectly);
  magma_check_result(&client);

  elog(DEBUG3, "magma_protocol_blocklocation pass fetch table");

  /*
   * Step 3. map ranges to block locations
   */
  bldata->serializeSchemaLen = MagmaClientC_MTGetSerializeSchemaLen(table);
  bldata->serializeSchema = palloc0(bldata->serializeSchemaLen);
  memcpy(bldata->serializeSchema, MagmaClientC_MTGetSerializeSchema(table),
         bldata->serializeSchemaLen);

  bldata->files = NIL;
  blocklocation_file *blf = NULL;

  // build block location files which reference cached range location
  MagmaRangeDistPtr rangeDist = MagmaClientC_FetchRangeDist(client);
  magma_check_result(&client);

  uint32_t rgNum = MagmaClientC_RDGetNumOfRgs(rangeDist);
  int32_t totalGroupNum = 0;
  elog(DEBUG3, "rg num %d", rgNum);
  for ( int rgIndex = 0 ; rgIndex < rgNum ; ++rgIndex ) {
    uint32_t rangeNum = MagmaClientC_RDGetNumOfRangesByRg(rangeDist, rgIndex);
    elog(DEBUG3, "rangeNum num %d", rangeNum);
    for ( int rangeIndex = 0 ; rangeIndex < rangeNum ; ++rangeIndex ) {
      // create block location file instance
      blocklocation_file *blf = palloc0(sizeof(blocklocation_file));
      blf->block_num = 1;
      blf->file_uri = NULL;  // not used field, set NULL to make it tidy
      blf->locations = palloc0(sizeof(BlockLocation));
      BlockLocation *bl = &(blf->locations[0]);
      MagmaRangePtr rangePtr = MagmaClientC_RDGetRangeByRg(rangeDist,
                                                           rgIndex,
                                                           rangeIndex);
      bl->replicaGroupId = MagmaClientC_RangeGetLeaderRgId(rangePtr);
      bl->rangeId = MagmaClientC_RangeGetRangeId(rangePtr);
      bl->length = 1;      // always one range as one block
      bl->offset = 0;      // no offet
      bl->corrupt = 0;     // no corrupt setting
      bl->numOfNodes = 1;  // we save leader node only
      bl->hosts = palloc0(sizeof(char *) * bl->numOfNodes);
      bl->names = palloc0(sizeof(char *) * bl->numOfNodes);
      bl->topologyPaths = palloc0(sizeof(char *) * bl->numOfNodes);
      bl->hosts[0] = search_hostname_by_ipaddr(
          MagmaClientC_RangeGetLeaderRgAddress(rangePtr));
      bl->names[0] = pstrdup(MagmaClientC_RangeGetLeaderRgFullAddress(rangePtr));
      bl->topologyPaths[0] = bl->names[0];

      // connect block location file instance to the list
      bldata->files = lappend(bldata->files, (void *)blf);
      totalGroupNum++;
    }
  }

  bldata->files = SortMagmaFilesByRangeId(bldata->files, totalGroupNum);

  /*
   * 4. return range locations
   */
  elog(DEBUG3, "magma_protocol_blocklocation pass");

  PG_RETURN_VOID();
}

/*
 * Implementation of tablesize caculation for magma protocol in pluggable storage
 */
Datum magma_protocol_tablesize(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  elog(DEBUG3, "magma_protocol_tablesize begin");
  /*
   * Step 1. prepare instances
   */
  /* Build the result instance and basic properties */
  ExtProtocolTableSize tsdata =
      palloc0(sizeof(ExtProtocolTableSizeData));

  if (tsdata == NULL) {
    elog(ERROR,
         "magma_protocol_blocklocation: failed to allocate new space");
  }
  tsdata->type = T_ExtProtocolTableSizeData;
  fcinfo->resultinfo = tsdata;

  /* Build validator data */
  ExtProtocolValidator pvalidator_data =
      (ExtProtocolValidator)(fcinfo->context);
  List *fmt_opts = pvalidator_data->format_opts;
  char *dbname = pvalidator_data->dbname;
  char *schemaname = pvalidator_data->schemaname;
  char *tablename = pvalidator_data->tablename;

  MagmaSnapshot *snapshot = &(pvalidator_data->snapshot);

  char *format_str = pstrdup((char *)strVal(linitial(fmt_opts)));
  /*
   * Step 2. get table size
   */
  char *fmt_name = NULL;
  List *l = magma_parse_format_string(format_str, &fmt_name);
  pfree(format_str);

  MagmaClientC *client = create_magma_client_instance();
  if (client == NULL) {
    elog(ERROR, "failed to connect to magma service");
  }

  int16_t tableType = 0;
  if (pg_strncasecmp(
      fmt_name, MAGMA_STORAGE_TYPE_TP, MAGMA_STORAGE_TYPE_TP_LEN) == 0) {
    tableType = MAGMACLIENTC_TABLETYPE_TP;
  } else if (pg_strncasecmp(fmt_name, MAGMA_STORAGE_TYPE_AP,
                            MAGMA_STORAGE_TYPE_AP_LEN) == 0) {
    tableType = MAGMACLIENTC_TABLETYPE_AP;
  } else {
    elog(ERROR,
         "magma_get_tablesize: failed to recognize table format type: [%s]",
         fmt_name);
  }
  MagmaClientC_SetupTableInfo(client, dbname, schemaname, tablename, tableType);
  MagmaClientC_SetupSnapshot(client, snapshot);

  // set size of table in tp type to zero.
  if (tableType == MAGMACLIENTC_TABLETYPE_AP) {
    tsdata->tablesize = MagmaClientC_GetTableSize(client);
  } else {
    tsdata->tablesize = 0;
  }

  elog(LOG,"table size in magma.c is %llu", tsdata->tablesize);
  magma_check_result(&client);

  elog(LOG, "magma_protocol_tablesize psss get tablesize.");

  elog(DEBUG3, "magma_protocol_tablesize pass");

  PG_RETURN_VOID();
}

/*
 * Implementation of database calculation for magma protocol in pluggable storage
 */

Datum magma_protocol_databasesize(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  elog(DEBUG3, "magma_protocol_databasesize begin");
  /*
   * Step 1. prepare instances
   */
  /* Build the result instance and basic properties */
  ExtProtocolDatabaseSize dbsdata =
      palloc0(sizeof(ExtProtocolDatabaseSizeData));

  if (dbsdata == NULL) {
    elog(ERROR,
         "magma_protocol_databasesize: failed to allocate new space");
  }
  dbsdata->type = T_ExtProtocolDatabaseSizeData;
  fcinfo->resultinfo = dbsdata;

  /* Build validator data */
  ExtProtocolValidator pvalidator_data =
      (ExtProtocolValidator)(fcinfo->context);
  char *dbname = pvalidator_data->dbname;

  MagmaSnapshot *snapshot = &(pvalidator_data->snapshot);

  /*
   * Step 2. get database size
   */

  MagmaClientC *client = create_magma_client_instance();
  if (client == NULL) {
    elog(ERROR, "failed to connect to magma service");
  }

  MagmaClientC_SetupDatabaseInfo(client, dbname);
  MagmaClientC_SetupSnapshot(client, snapshot);
  dbsdata->dbsize = MagmaClientC_GetDatabaseSize(client);
  elog(LOG,"dbsize in magma.c is %llu", dbsdata->dbsize);
  magma_check_result(&client);

  elog(LOG, "magma_protocol_databasesize psss get databasesize.");

  elog(DEBUG3, "magma_protocol_tablesize pass");

  PG_RETURN_VOID();
}

/*
 * Implementation of validators for magma protocol in pluggable storage
 */

Datum magma_protocol_validate(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  elog(DEBUG3, "magma_protocol_validate begin");

  /* Check action to be performed */
  ExtProtocolValidatorData *pvalidator_data =
      (ExtProtocolValidatorData *)(fcinfo->context);
  /* Validate formatter options, url, and create directory in magma */

  List *locs = pvalidator_data->url_list;

  ListCell *cell;
  foreach (cell, locs) {
    char *url = (char *)strVal(lfirst(cell));
    Uri *uri = ParseExternalTableUri(url);
    if (uri == NULL) {
      elog(ERROR,
           "magma_protocol_validate :"
           "invalid URI encountered %s",
           url);
    }
    if (uri->protocol != URI_MAGMA) {
      elog(ERROR,
           "magma_protocol_validate :"
           "invalid URI protocol encountered in %s, "
           "magma:// protocol is required",
           url);
    }
    FreeExternalTableUri(uri);
  }

  elog(DEBUG3, "magma_protocol_validate pass");

  PG_RETURN_VOID();
}

/*
 * Implementation of validators for magma format in pluggable storage
 */

/*
 * void
 * magma_validate_interfaces(char *formatName)
 */
Datum magma_validate_interfaces(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorageValidator psv_interface = (PlugStorageValidator)(fcinfo->context);

  if (pg_strncasecmp(psv_interface->format_name, "magma",
                     sizeof("magma") - 1) != 0) {
    ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg("magma_validate_interfaces : incorrect format name \'%s\'",
                    psv_interface->format_name)));
  }

  PG_RETURN_VOID();
}

/*
 * void
 * magma_validate_options(List *formatOptions,
 *                     char *formatStr,
 *                     bool isWritable)
 */
Datum magma_validate_options(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorageValidator psv = (PlugStorageValidator)(fcinfo->context);

  List *format_opts = psv->format_opts;
  char *format_str = psv->format_str;
  // bool is_writable  = psv->is_writable;

  char *formatter = NULL;
  char *category = NULL;
  // char *bucketnum = NULL;

  ListCell *opt;

  const int maxlen = 8 * 1024 - 1;
  int len = 0;

  foreach (opt, format_opts) {
    DefElem *defel = (DefElem *)lfirst(opt);
    char *key = defel->defname;
    bool need_free_value = false;
    char *val = (char *)defGetString(defel, &need_free_value);

    /* check formatter */
    if (strncasecmp(key, "formatter", strlen("formatter")) == 0) {
      char *formatter_values[] = {"magmaap", "magmatp"};
      checkPlugStorageFormatOption(&formatter, key, val, true, 2,
                                   formatter_values);
    }

    /* check category */
    if (strncasecmp(key, "category", strlen("category")) == 0) {
      char *category_values[] = {"internal", "external"};
      checkPlugStorageFormatOption(&category, key, val, true, 2,
                                   category_values);
    }

    if (strncasecmp(key, "bucketnum", strlen("bucketnum")) == 0) {
      ereport(ERROR,
             (errcode(ERRCODE_SYNTAX_ERROR),
              errmsg("bucketnum of magmatp/magmaap table are not supported by "
                     "user defined yet"),
              errOmitLocation(true)));
    }

    if (strncasecmp(key, "formatter", strlen("formatter")) &&
        strncasecmp(key, "category", strlen("category")) &&
        strncasecmp(key, "bucketnum", strlen("bucketnum"))) {
      ereport(ERROR,
              (errcode(ERRCODE_SYNTAX_ERROR),
               errmsg("format options for magma table must be formatter"),
               errOmitLocation(true)));
    }

    sprintf((char *)format_str + len, "%s '%s' ", key, val);
    len += strlen(key) + strlen(val) + 4;

    if (need_free_value) {
      pfree(val);
      val = NULL;
    }

    AssertImply(need_free_value, NULL == val);

    if (len > maxlen) {
      ereport(
          ERROR,
          (errcode(ERRCODE_SYNTAX_ERROR),
           errmsg("format options must be less than %d bytes in size", maxlen),
           errOmitLocation(true)));
    }
  }

  if (!formatter) {
    ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg("no formatter function specified"), errOmitLocation(true)));
  }

  PG_RETURN_VOID();
}

/*
 * void
 * magma_validate_encodings(char *encodingName)
 */
Datum magma_validate_encodings(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorageValidator psv = (PlugStorageValidator)(fcinfo->context);
  char *encoding_name = psv->encoding_name;

  if (strncasecmp(encoding_name, "SQL_ASCII", strlen("SQL_ASCII"))) {
    ereport(
        ERROR,
        (errcode(ERRCODE_SYNTAX_ERROR),
         errmsg("\"%s\" is not a valid encoding for external table with magma. "
                "Encoding for external table with magma must be SQL_ASCII.",
                encoding_name),
         errOmitLocation(true)));
  }

  PG_RETURN_VOID();
}

/*
 * void
 * magma_validate_datatypes(TupleDesc tupDesc)
 */
Datum magma_validate_datatypes(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorageValidator psv = (PlugStorageValidator)(fcinfo->context);
  TupleDesc tup_desc = psv->tuple_desc;

  for (int i = 0; i < tup_desc->natts; ++i) {
    int32_t datatype =
        (int32_t)(((Form_pg_attribute)(tup_desc->attrs[i]))->atttypid);

    if (checkUnsupportedDataTypeMagma(datatype)) {
      ereport(
          ERROR,
          (errcode(ERRCODE_SYNTAX_ERROR),
           errmsg("unsupported data types %s for columns of table with magma "
                  "format is specified.",
                  TypeNameToString(makeTypeNameFromOid(datatype, -1))),
           errOmitLocation(true)));
    }

    // for numeric, it must set precisions when create table
    if (HAWQ_TYPE_NUMERIC == datatype)
    {
      // get type modifier
      int4 tmp_typmod =
        ((Form_pg_attribute) (tup_desc->attrs[i]))->atttypmod - VARHDRSZ;

      // get precision and scale values
      int precision = (tmp_typmod >> 16) & 0xffff;
      int scale = tmp_typmod & 0xffff;
      if (precision < 1 || 38 < precision){
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("For Magma Format, DECIMAL precision must be between 1 and 38")));
      }
      if (scale == 0){
        ereport(NOTICE,
            (errmsg("Using a scale of zero for DECIMAL in Magma Format")));
      }
    }
  }

  PG_RETURN_VOID();
}

Datum magma_createindex(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  char *dbname = ps->ps_db_name;
  char *schemaname = ps->ps_schema_name;
  char *tablename = ps->ps_table_name;
  MagmaIndex *magmaidx = &(ps->magma_idx);
  MagmaSnapshot *snapshot = &(ps->ps_snapshot);

  elog(DEBUG1, "create index use index name:%s, index type:%s,"
      " columns counts:%d, key counts:%d, unique:%d, primary:%d",
      magmaidx->indexName, magmaidx->indexType, magmaidx->colCount,
      magmaidx->keynums, magmaidx->unique, magmaidx->primary);

  /* create index in magma */
  MagmaClientC *client = create_magma_client_instance();
  if (client == NULL) {
    elog(ERROR, "failed to create to magma service when create index.");
  }

  int16_t tableType = 0;
  MagmaClientC_SetupTableInfo(client, dbname, schemaname, tablename, tableType);
  MagmaClientC_SetupSnapshot(client, snapshot);
  MagmaClientC_CreateIndex(client, magmaidx);
  magma_check_result(&client);
  PG_RETURN_VOID();
}

Datum magma_dropindex(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  char *dbname = ps->ps_db_name;
  char *schemaname = ps->ps_schema_name;
  char *tablename = ps->ps_table_name;
  char *indexname = ps->magma_idx.indexName;
  MagmaSnapshot *snapshot = &(ps->ps_snapshot);

  elog(DEBUG1, "drop index use index name:%s,", indexname);

  /* drop index in magma */
  MagmaClientC *client = create_magma_client_instance();
  if (client == NULL) {
    elog(ERROR, "failed to create to magma service when drop index.");
  }

  int16_t tableType = 0;
  MagmaClientC_SetupTableInfo(client, dbname, schemaname, tablename, tableType);
  MagmaClientC_SetupSnapshot(client, snapshot);
  MagmaClientC_DropIndex(client, indexname);
  magma_check_result(&client);
  PG_RETURN_VOID();
}

Datum magma_reindex_index(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  char *dbname = ps->ps_db_name;
  char *schemaname = ps->ps_schema_name;
  char *tablename = ps->ps_table_name;
  char *indexname = ps->magma_idx.indexName;
  MagmaSnapshot *snapshot = &(ps->ps_snapshot);

  elog(DEBUG1, "reindex index use index name:%s,", indexname);

  /* reindex index in magma */
  MagmaClientC *client = create_magma_client_instance();
  if (client == NULL) {
    elog(ERROR, "failed to create to magma service when reindex index.");
  }

  int16_t tableType = 0;
  MagmaClientC_SetupTableInfo(client, dbname, schemaname, tablename, tableType);
  MagmaClientC_SetupSnapshot(client, snapshot);
  MagmaClientC_Reindex(client, indexname);
  magma_check_result(&client);
  PG_RETURN_VOID();
}

/*
 * Implementations of accessors for magma format in pluggable storage
 */

/*
 * void
 * magma_createtable(char *dbname,
 *                char *schemaname,
 *                char *tablename,
 *                List *tableelements,
 *                IndexStmt *primarykey)
 */
Datum magma_createtable(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);

  char *dbname = ps->ps_db_name;
  char *schemaname = ps->ps_schema_name;
  char *tablename = ps->ps_table_name;
  char *fmtName = ps->ps_formatter_name;
  MagmaSnapshot *snapshot = &(ps->ps_snapshot);


  List *tableelements = ps->ps_table_elements;
  IndexStmt *primarykey = ps->ps_primary_key;
  List *distributedkey = ps->ps_distributed_key;
  // bool isexternal = ps->ps_is_external;
  // List *locations = ps->ps_ext_locations;

  /* get primary key */
  List *pk_names = NIL;
  // process 1 or multi primary keys.
  if (primarykey != NULL) {
    ListCell *lc;
    foreach (lc, primarykey->indexParams) {
      IndexElem *idx = (IndexElem *)lfirst(lc);
      Assert(IsA(idx, IndexElem));

      pk_names = lappend(pk_names, makeString(idx->name));
    }
  }
  /* count number of keys and values of table */
  MagmaColumn *cols = NULL;
  int ncols = 0;

  int nkeys = primarykey == NULL ? 0 : list_length(primarykey->indexParams);

  Assert(nkeys == list_length(pk_names));

  /* prepare keys and values for table creation */
  cols =
      (MagmaColumn *)palloc0(sizeof(MagmaColumn) * list_length(tableelements));
  ListCell *element;

  int16_t tableType = 0;
  if (pg_strncasecmp(fmtName, "magmatp", strlen("magmatp")) == 0) {
    tableType = 0;
  } else if (pg_strncasecmp(fmtName, "magmaap", strlen("magmaap")) == 0) {
    tableType = 1;
  } else {
    elog(ERROR, "magma_createtable: failed to get table format type: [%s]",
         fmtName);
  }

  foreach (element, tableelements) {
    ColumnDef *col = (ColumnDef *)(lfirst(element));
    MagmaColumn *dcol = NULL;
    int pkpos = list_find(pk_names, makeString(col->colname));
    int dkpos = list_find(distributedkey, makeString(col->colname));
    dcol = &(cols[ncols]);
    // TODO(xsheng): get default value from col->raw_default
    dcol->defaultValue = "";
    dcol->dropped = false;
    dcol->primaryKeyIndex = pkpos;
    dcol->distKeyIndex = dkpos;
    // TODO(xsheng): leave unimplemented sort key index, add it later
    dcol->sortKeyIndex = -1;
    dcol->id = ncols;
    dcol->name = pstrdup(col->colname);
    dcol->datatype = LookupTypeName(NULL, col->typname);
    dcol->rawTypeMod = col->typname->typmod;
    Oid tmpOidVal = dcol->datatype;
    dcol->datatype = map_hawq_type_to_magma_type(dcol->datatype, !((bool)tableType));
    switch (dcol->datatype) {
      case BOOLEANID:
      case TINYINTID:
      case SMALLINTID:
      case INTID:
      case BIGINTID:
      case FLOATID:
      case DOUBLEID:
      case TIMESTAMPID:
      case DATEID:
      case TIMEID: {
        dcol->scale1 = 0;
        dcol->scale2 = 0;
        dcol->isnull = false;
      } break;

      case JSONBID:
      case JSONID:
      case BINARYID:
      case CHARID:
      case VARCHARID:
      case STRINGID:
      case DECIMALID:
      case DECIMALNEWID: {
        dcol->scale1 = col->typname->typmod - VARHDRSZ;
        dcol->scale2 = 0;
        dcol->isnull = false;
      } break;

      case STRUCTEXID:
      case IOBASETYPEID: {
        dcol->scale1 = col->typname->typmod - VARHDRSZ;
        dcol->scale2 = tmpOidVal;  // original oid
        dcol->isnull = false;
      } break;

      case INVALIDTYPEID: {
        elog(ERROR, "data type %s is invalid", TypeNameToString(makeTypeNameFromOid(dcol->datatype, -1)));
      } break;

      default: {
        elog(ERROR, "data type %s is not supported yet",
             TypeNameToString(makeTypeNameFromOid(dcol->datatype, -1)));
      } break;
    }
    ncols++;
  }

  assert(ncols == list_length(tableelements));
  /* create table in magma */
  MagmaClientC *client = create_magma_client_instance();
  if (client == NULL) {
    elog(ERROR, "failed to create to magma service");
  }

  MagmaClientC_SetupTableInfo(client, dbname, schemaname, tablename, tableType);
  MagmaClientC_SetupSnapshot(client, snapshot);
  MagmaClientC_CreateTable(client, ncols, cols);
  magma_check_result(&client);
  pfree(cols);
  list_free(pk_names);
  PG_RETURN_VOID();
}

/*
 * void
 * magma_droptable(char *dbname,
 *              char *schemaname,
 *              char *tablename)
 */
Datum magma_droptable(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);

  // ExtTableEntry *ete = ps->ps_exttable;
  char *dbname = ps->ps_db_name;
  char *schemaname = ps->ps_schema_name;
  char *tablename = ps->ps_table_name;
  MagmaSnapshot *snapshot = &(ps->ps_snapshot);


  /* drop table in magma */
  MagmaClientC *client = create_magma_client_instance();
  if (client == NULL) {
    elog(ERROR, "failed to connect to magma service");
  }
  int16_t tableType = 0;
  // for drop table, tableType won't be used in the process, set it as default
  MagmaClientC_SetupTableInfo(client, dbname, schemaname, tablename, tableType);
  MagmaClientC_SetupSnapshot(client, snapshot);
  MagmaClientC_DropTable(client);
  magma_check_result(&client);

  PG_RETURN_VOID();
}

Datum magma_beginscan(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  ExternalScan *ext_scan = ps->ps_ext_scan;
  ScanState *scan_state = ps->ps_scan_state;
  Relation relation = ps->ps_relation;
  int formatterType = ps->ps_formatter_type;
  char *formatterName = ps->ps_formatter_name;
  char *serializeSchema = ps->ps_magma_serializeSchema;
  int serializeSchemaLen = ps->ps_magma_serializeSchemaLen;
  MagmaSnapshot *snapshot = &(ps->ps_snapshot);

  Index scan_rel_id = ext_scan->scan.scanrelid;
  uint32 scan_counter = ext_scan->scancounter;
  List *uri_list = ext_scan->uriList;
  List *fmt_opts = ext_scan->fmtOpts;
  int fmt_encoding = ext_scan->encoding;
  Plan *scan_plan = &(ext_scan->scan.plan);

  /* Increment relation reference count while scanning relation */
  /*
   * This is just to make really sure the relcache entry won't go away while
   * the scan has a pointer to it.  Caller should be holding the rel open
   * anyway, so this is redundant in all normal scenarios...
   */
  RelationIncrementReferenceCount(relation);

  /* Allocate and initialize the select descriptor */
  FileScanDesc file_scan_desc = palloc0(sizeof(FileScanDescData));
  file_scan_desc->fs_inited = false;
  file_scan_desc->fs_ctup.t_data = NULL;
  ItemPointerSetInvalid(&file_scan_desc->fs_ctup.t_self);
  file_scan_desc->fs_cbuf = InvalidBuffer;
  file_scan_desc->fs_rd = relation;
  file_scan_desc->fs_scanrelid = scan_rel_id;
  file_scan_desc->fs_scancounter = scan_counter;
  file_scan_desc->fs_scanquals = scan_plan->qual;
  file_scan_desc->fs_noop = false;
  file_scan_desc->fs_file = NULL;
  file_scan_desc->fs_formatter = NULL;
  file_scan_desc->fs_formatter_type = formatterType;
  file_scan_desc->fs_formatter_name = formatterName;
  file_scan_desc->fs_serializeSchema =
      pnstrdup(serializeSchema, serializeSchemaLen);
  file_scan_desc->fs_serializeSchemaLen = serializeSchemaLen;
  file_scan_desc->fs_ps_magma_splits = ps->ps_magma_splits;
  file_scan_desc->fs_ps_magma_skip_tid = ps->ps_magma_skip_tid;

  /* Setup scan functions */
  get_magma_scan_functions(formatterName, file_scan_desc);

  /* Get URI for the scan */
  /*
   * get the external URI assigned to us.
   *
   * The URI assigned for this segment is normally in the uriList list
   * at the index of this segment id. However, if we are executing on
   * MASTER ONLY the (one and only) entry which is destined for the master
   * will be at the first entry of the uriList list.
   */
  char *uri_str = NULL;
  int segindex = GetQEIndex();

  Value *v = NULL;

  v = (Value *)list_nth(uri_list, 0);
  uri_str = (char *)strVal(v);
  if (v->type == T_Null)
    uri_str = NULL;
  else
    uri_str = (char *)strVal(v);

  /*
   * If a uri is assigned to us - get a reference to it. Some executors
   * don't have a uri to scan (if # of uri's < # of primary segdbs).
   * in which case uri will be NULL. If that's the case for this
   * segdb set to no-op.
   */
  if (uri_str) {
    /* set external source (uri) */
    file_scan_desc->fs_uri = uri_str;
    elog(DEBUG3, "fs_uri (%d) is set as %s", segindex, uri_str);
    /* NOTE: we delay actually opening the data source until external_getnext()
     */
  } else {
    /* segdb has no work to do. set to no-op */
    file_scan_desc->fs_noop = true;
    file_scan_desc->fs_uri = NULL;
  }

  /* Allocate values and nulls structure */
  TupleDesc tup_desc = RelationGetDescr(relation);
  file_scan_desc->fs_tupDesc = tup_desc;
  file_scan_desc->attr = tup_desc->attrs;
  file_scan_desc->num_phys_attrs = tup_desc->natts;

  file_scan_desc->values =
      (Datum *)palloc0(file_scan_desc->num_phys_attrs * sizeof(Datum));
  file_scan_desc->nulls =
      (bool *)palloc0(file_scan_desc->num_phys_attrs * sizeof(bool));

  /* Setup user data */
  /* sliceId is no use in there, executor could ensure this */
  /* currentSliceId == ps->ps_scan_state->ps.state->currentSliceIdInPlan */
  if (AmISegment()) {
    /* Initialize user data */
    MagmaFormatUserData *user_data = palloc0(sizeof(MagmaFormatUserData));
    user_data->isFirstRescan = true;
    if (formatterName != NULL &&
        (strncasecmp(formatterName, "magmatp", sizeof("magmatp") - 1) == 0)) {
      user_data->isMagmatp = true;
    } else {
      user_data->isMagmatp = false;
    }

    // special handling for magmatp decimal
    if (user_data->isMagmatp) {
      file_scan_desc->in_functions = (FmgrInfo *)palloc0(
          file_scan_desc->num_phys_attrs * sizeof(FmgrInfo));
      file_scan_desc->typioparams =
          (Oid *)palloc0(file_scan_desc->num_phys_attrs * sizeof(Oid));
      bool hasNumeric = false;
      for (int i = 0; i < file_scan_desc->num_phys_attrs; ++i) {
        if (file_scan_desc->attr[i]->atttypid != HAWQ_TYPE_NUMERIC) continue;
        hasNumeric = true;
        getTypeInputInfo(file_scan_desc->attr[i]->atttypid,
                         &file_scan_desc->in_func_oid,
                         &file_scan_desc->typioparams[i]);
        fmgr_info(file_scan_desc->in_func_oid,
                  &file_scan_desc->in_functions[i]);
      }
      /*
       * magmatp table support numeric type with old decimal. Numeric related
       * function will be called to read numeric column in magmatp table. To
       * prevent OutOfMemory, InputFunctionCall() should be wrapped by pre_row_context.
       * magmaap table support numeric type with new decimal. So it's unnecessary
       * to add the MemoryContext.
       */
      if (hasNumeric) {
        file_scan_desc->fs_pstate = (CopyStateData *)palloc0(sizeof(CopyStateData));
        CopyState pstate = file_scan_desc->fs_pstate;
        pstate->fe_eof = false;
        pstate->eol_type = EOL_UNKNOWN;
        pstate->eol_str = NULL;
        pstate->cur_relname = RelationGetRelationName(relation);
        pstate->cur_lineno = 0;
        pstate->err_loc_type = ROWNUM_ORIGINAL;
        pstate->cur_attname = NULL;
        pstate->raw_buf_done = true; /* true so we will read data in first run */
        pstate->line_done = true;
        pstate->bytesread = 0;
        pstate->custom = false;
        pstate->header_line = false;
        pstate->fill_missing = false;
        pstate->line_buf_converted = false;
        pstate->raw_buf_index = 0;
        pstate->processed = 0;
        pstate->filename = uri_str;
        pstate->copy_dest = COPY_EXTERNAL_SOURCE;
        pstate->missing_bytes = 0;
        pstate->csv_mode = false;
        pstate->custom = true;
        pstate->custom_formatter_func = NULL;
        pstate->custom_formatter_name = NULL;
        pstate->custom_formatter_params = NIL;
        pstate->rel = relation;
        pstate->client_encoding = PG_UTF8;
        pstate->enc_conversion_proc = NULL;
        pstate->need_transcoding = false;
        pstate->encoding_embeds_ascii =
            PG_ENCODING_IS_CLIENT_ONLY(pstate->client_encoding);
        pstate->attr_offsets = NULL;
        pstate->attnumlist = NULL;
        pstate->force_quote = NIL;
        pstate->force_quote_flags = NULL;
        pstate->force_notnull = NIL;
        pstate->force_notnull_flags = NULL;
        initStringInfo(&pstate->attribute_buf);
        initStringInfo(&pstate->line_buf);
        MemSet(pstate->raw_buf, ' ', RAW_BUF_SIZE * sizeof(char));
        pstate->raw_buf[RAW_BUF_SIZE] = '\0';
        /*
         * Create a temporary memory context that we can reset once per row to
         * recover palloc'd memory. This avoids any problems with leaks inside
         * datatype input or output routines, and should be faster than retail
         * pfree's anyway.
         */
        pstate->rowcontext = AllocSetContextCreate(
            CurrentMemoryContext, "ExtTableMemCxt", ALLOCSET_DEFAULT_MINSIZE,
                ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
      }
    } else {
      file_scan_desc->in_functions = NULL;
      file_scan_desc->typioparams = NULL;
    }

    /* the number of ranges is dynamic for magma table */
    int32_t nRanges = 0;
    ListCell *lc_split = NULL;
    foreach (lc_split, ps->ps_magma_splits) {
      List *split = (List *)lfirst(lc_split);
      nRanges += list_length(split);
    }

    init_magma_format_user_data_for_read(tup_desc, user_data);

    /* Create formatter instance */
    // ExtTableEntry *ete = GetExtTableEntry(RelationGetRelid(relation));
    user_data->fmt = create_magma_formatter_instance(
        NIL, serializeSchema, serializeSchemaLen, PG_UTF8, formatterName,
        nRanges);

    /* Prepare database, schema, and table information */
    char *dbname = database;
    char *schemaname = getNamespaceNameByOid(RelationGetNamespace(relation));
    Assert(schemaname != NULL);
    char *tablename = RelationGetRelationName(relation);

    MagmaFormatC_SetupTarget(user_data->fmt, dbname, schemaname, tablename);
    MagmaFormatC_SetupTupDesc(user_data->fmt, user_data->numberOfColumns,
                              user_data->colNames, user_data->colDatatypes,
                              user_data->colDatatypeMods,
                              user_data->colIsNulls);

    /* Build tuple description */
    Plan *plan = &(ext_scan->scan.plan);
    file_scan_desc->fs_ps_plan = plan;
    build_magma_tuple_descrition_for_read(plan, relation, user_data, ps->ps_magma_skip_tid);

    /* prepare plan */
    CommonPlanContext ctx;
    init_common_plan_context(&ctx);
    scan_plan->plan_parent_node_id = -1;
    convert_extscan_to_common_plan(scan_plan, scan_state->splits, relation,
                                   &ctx);
    // elog(DEBUG1, "common plan: %s",
    // univPlanGetJsonFormatedPlan(ctx.univplan));

    int32_t size = 0;
    char *planstr = univPlanSerialize(ctx.univplan, &size, false);
    /* Save user data */
    file_scan_desc->fs_ps_user_data = (void *)user_data;

    /* Begin scan with the formatter */
    bool enableShm = (strcasecmp(magma_enable_shm, "ON") == 0);
    MagmaFormatBeginScanMagmaFormatC(
        user_data->fmt, user_data->colToReads, snapshot, planstr, size,
        enableShm, ps->ps_magma_skip_tid, magma_shm_limit_per_block * 1024);
    MagmaFormatCatchedError *e =
        MagmaFormatGetErrorMagmaFormatC(user_data->fmt);
    if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
      elog(ERROR, "magma_scan: failed to scan: %s(%d)", e->errMessage,
           e->errCode);
    }

    free_common_plan_context(&ctx);
  }

  /* Save file_scan_desc */
  ps->ps_file_scan_desc = file_scan_desc;

  PG_RETURN_POINTER(file_scan_desc);
}

void init_common_plan_context(CommonPlanContext *ctx) {
  ctx->univplan = univPlanNewInstance();
  ctx->convertible = true;
  ctx->base.node = NULL;
  ctx->querySelect = false;
  ctx->isMagma = true;
  ctx->stmt = NULL;
  ctx->setDummyTListRef = false;
  ctx->scanReadStatsOnly = false;
  ctx->parent = NULL;
  ctx->exprBufStack = NIL;
  ctx->isConvertingIndexQual = false;
  ctx->idxColumns = NIL;
}

void free_common_plan_context(CommonPlanContext *ctx) {
  univPlanFreeInstance(&ctx->univplan);
}
/*
 * ExternalSelectDesc
 * magma_getnext_init(PlanState *planState,
 *                 ExternalScanState *extScanState)
 */
Datum magma_getnext_init(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);

  ExternalSelectDesc ext_select_desc = NULL;
  /*
  ExternalSelectDesc ext_select_desc = (ExternalSelectDesc)palloc0(
                  sizeof(ExternalSelectDescData));

  Plan *rootPlan = NULL;

  if (plan_state != NULL)
  {
          ext_select_desc->projInfo = plan_state->ps_ProjInfo;

          // If we have an agg type then our parent is an Agg node
          rootPlan = plan_state->state->es_plannedstmt->planTree;
          if (IsA(rootPlan, Agg) && ext_scan_state->parent_agg_type)
          {
                  ext_select_desc->agg_type = ext_scan_state->parent_agg_type;
          }
  }
  */

  ps->ps_ext_select_desc = ext_select_desc;

  PG_RETURN_POINTER(ext_select_desc);
}

Datum magma_getnext(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  FileScanDesc fsd = ps->ps_file_scan_desc;
  MagmaFormatUserData *user_data =
      (MagmaFormatUserData *)(fsd->fs_ps_user_data);
  TupleTableSlot *slot = ps->ps_tuple_table_slot;
  bool *nulls = slot_get_isnull(slot);
  memset(nulls, true, user_data->numberOfColumns);

  bool res = MagmaFormatNextMagmaFormatC(
      user_data->fmt, user_data->colRawValues, user_data->colValLength, nulls,
      &(user_data->colRawTid));
  if (res) {
    MemoryContext old_context = NULL;
    if (user_data->isMagmatp && fsd->fs_pstate != NULL &&
        fsd->fs_pstate->rowcontext != NULL) {
      /* Free memory for previous tuple if necessary */
      MemoryContextReset(fsd->fs_pstate->rowcontext);
      old_context = MemoryContextSwitchTo(fsd->fs_pstate->rowcontext);
    }

    for (int32_t i = 0; i < user_data->numberOfColumns; ++i) {
      // Column not to read or column is null
      if (nulls[i]) continue;

      switch (fsd->attr[i]->atttypid) {
        case HAWQ_TYPE_BOOL: {
          user_data->colValues[i] =
              BoolGetDatum(*(bool *)(user_data->colRawValues[i]));
          break;
        }
        case HAWQ_TYPE_INT2: {
          user_data->colValues[i] =
              Int16GetDatum(*(int16_t *)(user_data->colRawValues[i]));
          break;
        }
        case HAWQ_TYPE_INT4: {
          user_data->colValues[i] =
              Int32GetDatum(*(int32_t *)(user_data->colRawValues[i]));
          break;
        }
        case HAWQ_TYPE_INT8:
        case HAWQ_TYPE_TIME:
        case HAWQ_TYPE_TIMESTAMP:
        case HAWQ_TYPE_TIMESTAMPTZ: {
          user_data->colValues[i] =
              Int64GetDatum(*(int64_t *)(user_data->colRawValues[i]));
          break;
        }
        case HAWQ_TYPE_FLOAT4: {
          user_data->colValues[i] =
              Float4GetDatum(*(float *)(user_data->colRawValues[i]));
          break;
        }
        case HAWQ_TYPE_FLOAT8: {
          user_data->colValues[i] =
              Float8GetDatum(*(double *)(user_data->colRawValues[i]));
          break;
        }
        case HAWQ_TYPE_JSONB:
        case HAWQ_TYPE_JSON:
        case HAWQ_TYPE_VARCHAR:
        case HAWQ_TYPE_TEXT:
        case HAWQ_TYPE_BPCHAR:
        case HAWQ_TYPE_BYTE: {
          SET_VARSIZE((struct varlena *)(user_data->colRawValues[i]),
                      user_data->colValLength[i]);
          user_data->colValues[i] = PointerGetDatum(user_data->colRawValues[i]);
          break;
        }
        case HAWQ_TYPE_NUMERIC: {
          SET_VARSIZE((struct varlena *)(user_data->colRawValues[i]),
                      user_data->colValLength[i]);
          user_data->colValues[i] =
              PointerGetDatum(user_data->colRawValues[i]);
          break;
        }
        case HAWQ_TYPE_DATE: {
          user_data->colValues[i] =
              Int32GetDatum(*(int32_t *)(user_data->colRawValues[i]) -
                            POSTGRES_EPOCH_JDATE + UNIX_EPOCH_JDATE);
          break;
        }
        default: {
          ereport(ERROR, (errmsg_internal("MAGMA:%d", fsd->attr[i]->atttypid)));
          break;
        }
      }
    }
    if (user_data->isMagmatp && fsd->fs_pstate != NULL &&
        fsd->fs_pstate->rowcontext != NULL) {
      MemoryContextSwitchTo(old_context);
    }

    if (user_data->colRawTid != NULL) {
      user_data->colTid = *(MagmaTidC *)(user_data->colRawTid);
      ConvertTidToCtidAndRangeid(user_data->colTid,
                                 &(slot->PRIVATE_tts_synthetic_ctid),
                                 &(slot->tts_rangeid));
    }

    ps->ps_has_tuple = true;
    slot->PRIVATE_tts_values = user_data->colValues;
    TupSetVirtualTupleNValid(slot, user_data->numberOfColumns);
    PG_RETURN_BOOL(true);
  }

  magma_clear(ps, true);

  PG_RETURN_BOOL(false);
}

/*
 * void
 * magma_rescan(FileScanDesc scan)
 */
Datum magma_rescan(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  ScanState *scan_state = ps->ps_scan_state;
  FileScanDesc fsd = ps->ps_file_scan_desc;
  MagmaSnapshot *snapshot = &(ps->ps_snapshot);

  MagmaRuntimeKeys runtimeKeys;
  Assert(ps->num_run_time_keys >= 0);
  if (ps->num_run_time_keys == 0) {
    runtimeKeys.num = 0;
    runtimeKeys.keys = NULL;
  } else {
    Assert(ps->runtime_key_info != NULL);
    runtimeKeys.num = ps->num_run_time_keys;
    runtimeKeys.keys = palloc0(ps->num_run_time_keys * sizeof(MagmaRuntimeKey));
    for (int i = 0; i < ps->num_run_time_keys; ++i) {
      ScanKey scan_key = ps->runtime_key_info[i].scan_key;
      runtimeKeys.keys[i].flag = scan_key->sk_flags;
      runtimeKeys.keys[i].attnoold = scan_key->sk_attnoold;
      runtimeKeys.keys[i].value =
          OutputFunctionCall(&scan_key->sk_out_func, scan_key->sk_argument);
    }
  }

  MagmaFormatUserData *user_data =
      (MagmaFormatUserData *)(fsd->fs_ps_user_data);
  if (user_data != NULL) {
    // There are 2 cases that user_data is not null:
    // 1. If this is the first rescan, at this point, we have done
    // magma_beginscan() and haven't done magma_getnext() yet.
    // We don't need to create user_data from scratch, just use it.
    if (user_data->isFirstRescan) {
      user_data->isFirstRescan = false;
      MagmaFormatReScanMagmaFormatC(user_data->fmt, &runtimeKeys);
      if (runtimeKeys.keys) {
        pfree(runtimeKeys.keys);
      }
      PG_RETURN_VOID();
    }

    // 2. Otherwise is not the first rescan, we should do magma_clear() here.
    // This case happens with the Nested Loop Exists Join. In that case, as long
    // as we can get a piece of data in magma_getnext(), we will start a new
    // rescan. Therefore, we didn't do mamga_clear() in magma_getnext(), which
    // resulted in the dirty user_data not being cleared.
    // We don't reuse the user_data since that would make the code complex, just
    // clear it and create a new one below.
    magma_clear(ps, true);
  }

  /* Initialize user data */
  user_data = palloc0(sizeof(MagmaFormatUserData));
  if (fsd->fs_formatter_name != NULL &&
      (strncasecmp(fsd->fs_formatter_name, "magmatp", sizeof("magmatp") - 1) == 0)) {
    user_data->isMagmatp = true;
  } else {
    user_data->isMagmatp = false;
  }

  /* the number of ranges is dynamic for magma table */
  int32_t nRanges = 0;
  ListCell *lc_split = NULL;
  foreach (lc_split, fsd->fs_ps_magma_splits) {
    List *split = (List *)lfirst(lc_split);
    nRanges += list_length(split);
  }

  init_magma_format_user_data_for_read(fsd->fs_tupDesc, user_data);

  /* Create formatter instance */
  user_data->fmt = create_magma_formatter_instance(
      NIL, fsd->fs_serializeSchema, fsd->fs_serializeSchemaLen, PG_UTF8, fsd->fs_formatter_name, nRanges);

  /* Prepare database, schema, and table information */
  char *dbname = database;
  char *schemaname = getNamespaceNameByOid(RelationGetNamespace(fsd->fs_rd));
  Assert(schemaname != NULL);
  char *tablename = RelationGetRelationName(fsd->fs_rd);

  MagmaFormatC_SetupTarget(user_data->fmt, dbname, schemaname, tablename);
  MagmaFormatC_SetupTupDesc(user_data->fmt, user_data->numberOfColumns,
                            user_data->colNames, user_data->colDatatypes,
                            user_data->colDatatypeMods,
                            user_data->colIsNulls);

  /* Build tuple description */
  Plan *plan = fsd->fs_ps_plan;
  build_magma_tuple_descrition_for_read(plan, fsd->fs_rd, user_data, fsd->fs_ps_magma_skip_tid);

  /* Build plan */
  CommonPlanContext ctx;
  init_common_plan_context(&ctx);
  plan->plan_parent_node_id = -1;
  convert_extscan_to_common_plan(plan, scan_state->splits,
                                 fsd->fs_rd, &ctx);
  int32_t size = 0;
  char *planstr = univPlanSerialize(ctx.univplan, &size, false);

  /* Save user data */
  fsd->fs_ps_user_data = (void *)user_data;

  /* Begin scan with the formatter */
  bool enableShm = (strcasecmp(magma_enable_shm, "ON") == 0);
  MagmaFormatBeginScanMagmaFormatC(user_data->fmt, user_data->colToReads,
                                   snapshot, planstr, size,
                                   enableShm, fsd->fs_ps_magma_skip_tid,
                                   magma_shm_limit_per_block * 1024);
  MagmaFormatCatchedError *e = MagmaFormatGetErrorMagmaFormatC(user_data->fmt);
  if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    elog(ERROR, "magma_scan: failed to beginscan: %s(%d)", e->errMessage,
         e->errCode);
  }

  MagmaFormatReScanMagmaFormatC(user_data->fmt, &runtimeKeys);
  if (runtimeKeys.keys) {
    pfree(runtimeKeys.keys);
  }

  free_common_plan_context(&ctx);

  PG_RETURN_VOID();
}

/*
 * void
 * magma_endscan(FileScanDesc scan)
 */
Datum magma_endscan(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  FileScanDesc fsd = ps->ps_file_scan_desc;

  MagmaFormatUserData *user_data = (MagmaFormatUserData *)(fsd->fs_ps_user_data);

  // free memory in endscan, for some subquery scenarios "getnext" might not be called
  if (user_data != NULL) {
    magma_clear(ps, false);
  }

  if (fsd->values) {
    // decrement relation reference count and free scan descriptor storage
    RelationDecrementReferenceCount(fsd->fs_rd);

    pfree(fsd->values);
    fsd->values = NULL;
  }

  if (fsd->nulls) {
    pfree(fsd->nulls);
    fsd->nulls = NULL;
  }

  // free formatter information
  if (fsd->fs_formatter_name) {
    pfree(fsd->fs_formatter_name);
    fsd->fs_formatter_name = NULL;
  }

  if (fsd->in_functions) {
    pfree(fsd->in_functions);
    fsd->in_functions = NULL;
  }

  if (fsd->typioparams) {
    pfree(fsd->typioparams);
    fsd->typioparams = NULL;
  }

  if (fsd->fs_pstate != NULL && fsd->fs_pstate->rowcontext != NULL) {
    /*
     * delete the row context
     */
    MemoryContextDelete(fsd->fs_pstate->rowcontext);
    fsd->fs_pstate->rowcontext = NULL;
  }

  /*
   * free parse state memory
   */
  if (fsd->fs_pstate != NULL) {
    if (fsd->fs_pstate->attribute_buf.data)
      pfree(fsd->fs_pstate->attribute_buf.data);
    if (fsd->fs_pstate->line_buf.data) pfree(fsd->fs_pstate->line_buf.data);

    pfree(fsd->fs_pstate);
    fsd->fs_pstate = NULL;
  }

  PG_RETURN_VOID();
}

/*
 * void
 * magma_stopscan(FileScanDesc scan)
 */
Datum magma_stopscan(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  FileScanDesc fsd = ps->ps_file_scan_desc;
  MagmaFormatUserData *user_data =
      (MagmaFormatUserData *)(fsd->fs_ps_user_data);
  TupleTableSlot *tts = ps->ps_tuple_table_slot;

  if (!user_data) PG_RETURN_VOID();

  MagmaFormatStopScanMagmaFormatC(user_data->fmt);
  MagmaFormatCatchedError *e = MagmaFormatGetErrorMagmaFormatC(user_data->fmt);
  if (e->errCode == ERRCODE_SUCCESSFUL_COMPLETION) {
    MagmaFormatEndScanMagmaFormatC(user_data->fmt);
    e = MagmaFormatGetErrorMagmaFormatC(user_data->fmt);
    if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
      elog(ERROR, "Magma: failed to finish scan: %s (%d)", e->errMessage,
           e->errCode);
    }

    MagmaFormatFreeMagmaFormatC(&(user_data->fmt));

    pfree(user_data->colRawValues);
    pfree(user_data->colValues);
    pfree(user_data->colToReads);
    pfree(user_data->colValLength);
    for (int i = 0; i < user_data->numberOfColumns; ++i)
      pfree(user_data->colNames[i]);
    pfree(user_data->colNames);
    pfree(user_data->colDatatypes);
    pfree(user_data->colDatatypeMods);
    pfree(user_data->colIsNulls);
    pfree(user_data);
    fsd->fs_ps_user_data = NULL;

    /* form empty tuple */
    ps->ps_has_tuple = false;

    tts->PRIVATE_tts_values = NULL;
    tts->PRIVATE_tts_isnull = NULL;
    ExecClearTuple(tts);
  } else {
    elog(ERROR, "magma_stopscan: failed to stop scan: %s(%d)", e->errMessage,
         e->errCode);
  }

  PG_RETURN_VOID();
}

/* ExternalInsertDesc
 * magma_begindelete(Relation relation)
 */
Datum magma_begindelete(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  Relation relation = ps->ps_relation;
  char *serializeSchema = ps->ps_magma_serializeSchema;
  int serializeSchemaLen = ps->ps_magma_serializeSchemaLen;
  MagmaSnapshot *snapshot = &(ps->ps_snapshot);

  /* 1. Allocate and initialize the delete descriptor */
  ExternalInsertDesc edd = palloc0(sizeof(ExternalInsertDescData));

  ExtTableEntry *ete = GetExtTableEntry(RelationGetRelid(relation));

  int formatterType = ExternalTableType_Invalid;

  char *formatterName = NULL;
  getExternalTableTypeStr(ete->fmtcode, ete->fmtopts, &formatterType,
                          &formatterName);

  /* 1.1 Setup delete functions */
  get_magma_delete_functions(formatterName, edd);

  List *fmt_opts = NIL;
  fmt_opts = lappend(fmt_opts, makeString(pstrdup(ete->fmtopts)));

  /* 1.2 Allocate and initialize structure which track data parsing state */
  edd->ext_pstate = (CopyStateData *)palloc0(sizeof(CopyStateData));
  edd->ext_tupDesc = RelationGetDescr(relation);

  /* 1.3 Initialize parse state */
  /* 1.3.1 Initialize basic information for pstate */
  CopyState pstate = edd->ext_pstate;

  /* 1.3.2 Setup encoding information */
  /*
   * Set up encoding conversion info.  Even if the client and server
   * encodings are the same, we must apply pg_client_to_server() to validate
   * data in multibyte encodings.
   *
   * Each external table specifies the encoding of its external data. We will
   * therefore set a client encoding and client-to-server conversion procedure
   * in here (server-to-client in WET) and these will be used in the data
   * conversion routines (in copy.c CopyReadLineXXX(), etc).
   */
  int fmt_encoding = ete->encoding;
  Insist(PG_VALID_ENCODING(fmt_encoding));
  pstate->client_encoding = fmt_encoding;
  Oid conversion_proc =
      FindDefaultConversionProc(GetDatabaseEncoding(), fmt_encoding);

  if (OidIsValid(conversion_proc)) {
    /* conversion proc found */
    pstate->enc_conversion_proc = palloc0(sizeof(FmgrInfo));
    fmgr_info(conversion_proc, pstate->enc_conversion_proc);
  } else {
    /* no conversion function (both encodings are probably the same) */
    pstate->enc_conversion_proc = NULL;
  }

  pstate->need_transcoding = pstate->client_encoding != GetDatabaseEncoding();
  pstate->encoding_embeds_ascii =
      PG_ENCODING_IS_CLIENT_ONLY(pstate->client_encoding);

  /* 1.3.3 Setup tuple description */
  TupleDesc tup_desc = edd->ext_tupDesc;
  pstate->attr_offsets = (int *)palloc0(tup_desc->natts * sizeof(int));

  /* 1.3.4 Generate or convert list of attributes to process */
  pstate->attnumlist = CopyGetAttnums(tup_desc, relation, NIL);

  /* 1.3.5 Convert FORCE NOT NULL name list to per-column flags, check validity
   */
  pstate->force_notnull_flags = (bool *)palloc0(tup_desc->natts * sizeof(bool));
  if (pstate->force_notnull) {
    List *attnums;
    ListCell *cur;

    attnums = CopyGetAttnums(tup_desc, relation, pstate->force_notnull);

    foreach (cur, attnums) {
      int attnum = lfirst_int(cur);
      pstate->force_notnull_flags[attnum - 1] = true;
    }
  }

  /* 1.3.6 Take care of state that is WET specific */
  Form_pg_attribute *attr = tup_desc->attrs;
  ListCell *cur;

  pstate->null_print_client = pstate->null_print; /* default */
  pstate->fe_msgbuf = makeStringInfo(); /* use fe_msgbuf as a per-row buffer */
  pstate->out_functions =
      (FmgrInfo *)palloc0(tup_desc->natts * sizeof(FmgrInfo));

  foreach (cur,
           pstate->attnumlist) /* Get info about the columns need to process */
  {
    int attnum = lfirst_int(cur);
    Oid out_func_oid;
    bool isvarlena;

    getTypeOutputInfo(attr[attnum - 1]->atttypid, &out_func_oid, &isvarlena);
    fmgr_info(out_func_oid, &pstate->out_functions[attnum - 1]);
  }

  /*
   * We need to convert null_print to client encoding, because it
   * will be sent directly with CopySendString.
   */
  if (pstate->need_transcoding) {
    pstate->null_print_client = pg_server_to_custom(
        pstate->null_print, pstate->null_print_len, pstate->client_encoding,
        pstate->enc_conversion_proc);
  }

  /* 1.3.7 Create temporary memory context for per row process */
  /*
   * Create a temporary memory context that we can reset once per row to
   * recover palloc'd memory.  This avoids any problems with leaks inside
   * datatype input or output routines, and should be faster than retail
   * pfree's anyway.
   */
  pstate->rowcontext = AllocSetContextCreate(
      CurrentMemoryContext, "ExtTableMemCxt", ALLOCSET_DEFAULT_MINSIZE,
      ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

  /* 1.3.8 Parse format options */
  char *format_str = pstrdup((char *)strVal(linitial(fmt_opts)));
  char *fmt_name = NULL;
  List *l = magma_parse_format_string(format_str, &fmt_name);
  pstate->custom_formatter_name = fmt_name;
  pstate->custom_formatter_params = l;
  pfree(format_str);

  /* 1.4 Initialize formatter data */
  edd->ext_formatter_data = (FormatterData *)palloc0(sizeof(FormatterData));
  edd->ext_formatter_data->fmt_perrow_ctx = edd->ext_pstate->rowcontext;

  /* 2. Setup user data */
  /* 2.1 Get database, schema, table name for the delete */
  Assert(database != NULL);
  Oid namespaceOid = RelationGetNamespace(relation);
  char *schema = getNamespaceNameByOid(namespaceOid);
  char *table = RelationGetRelationName(relation);

  MagmaFormatUserData *user_data =
      (MagmaFormatUserData *)palloc0(sizeof(MagmaFormatUserData));

  if (formatterName != NULL &&
      (strncasecmp(formatterName, "magmatp", sizeof("magmatp") - 1) == 0)) {
    user_data->isMagmatp = true;
  } else {
    user_data->isMagmatp = false;
  }

  init_magma_format_user_data_for_write(tup_desc, user_data, relation);

  /* the number of ranges is dynamic for magma table */
  int32_t nRanges = 0;
  ListCell *lc_split = NULL;
  foreach (lc_split, ps->ps_magma_splits) {
      List *split = (List *)lfirst(lc_split);
      nRanges += list_length(split);
  }

  /* 2.2 Create formatter instance */
  List *fmt_opts_defelem = pstate->custom_formatter_params;
  user_data->fmt = create_magma_formatter_instance(
      fmt_opts_defelem, serializeSchema, serializeSchemaLen, fmt_encoding,
      formatterName, nRanges);
  /*prepare hash info */
  int32_t nDistKeyIndex = 0;
  int16_t *distKeyIndex = NULL;
  fetchDistributionPolicy(relation->rd_id, &nDistKeyIndex, &distKeyIndex);

  uint32 range_to_rg_map[nRanges];
  List *rg = magma_build_range_to_rg_map(ps->ps_magma_splits, range_to_rg_map);
  int nRg = list_length(rg);
  uint16 *rgId = palloc0(sizeof(uint16) * nRg);
  char **rgUrl = palloc0(sizeof(char *) * nRg);
  magma_build_rg_to_url_map(ps->ps_magma_splits, rg, rgId, rgUrl);

  /* 2.3 Prepare database, schema, and table information */
  MagmaFormatC_SetupTarget(user_data->fmt, database, schema, table);
  MagmaFormatC_SetupTupDesc(user_data->fmt, user_data->numberOfColumns,
                            user_data->colNames, user_data->colDatatypes,
                            user_data->colDatatypeMods, user_data->colIsNulls);

  int *jumpHashMap = get_jump_hash_map(nRanges);
  MagmaFormatC_SetupHasher(user_data->fmt, nDistKeyIndex, distKeyIndex, nRanges,
                           range_to_rg_map, nRg, rgId, rgUrl, jumpHashMap,
                           JUMP_HASH_MAP_LENGTH);
  MagmaFormatCatchedError *e = MagmaFormatGetErrorMagmaFormatC(user_data->fmt);
  if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    elog(ERROR, "magma_delete: failed to begindelete: %s(%d)", e->errMessage,
         e->errCode);
  }

  /* 2.4 Save user data */
  edd->ext_ps_user_data = (void *)user_data;

  /* 3. Begin insert with the formatter */
  MagmaFormatBeginDeleteMagmaFormatC(user_data->fmt, snapshot);
  MagmaFormatCatchedError *e1 = MagmaFormatGetErrorMagmaFormatC(user_data->fmt);
  if (e1->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    elog(ERROR, "magma_delete: failed to begindelete: %s(%d)", e1->errMessage,
         e1->errCode);
  }

  /* 4. Save the result */
  ps->ps_ext_delete_desc = edd;

  PG_RETURN_POINTER(edd);
}

/* void
 * magma_delete(ExternalInsertDesc extDeleteDesc,
 *           TupleTableSlot *tupTableSlot)
 */
Datum magma_delete(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  ExternalInsertDesc edd = ps->ps_ext_delete_desc;
  TupleTableSlot *tts = ps->ps_tuple_table_slot;

  /* It may be memtuple, we need to transfer it to virtual tuple */
  slot_getallattrs(tts);

  MagmaFormatUserData *user_data =
      (MagmaFormatUserData *)(edd->ext_ps_user_data);

  user_data->colTid.rangeid = DatumGetUInt16(edd->ext_rangeId);
  user_data->colTid.rowid = DatumGetUInt64(edd->ext_rowId);
  user_data->colValues = slot_get_values(tts);
  user_data->colIsNulls = slot_get_isnull(tts);

  static bool DUMMY_BOOL = true;
  static int8_t DUMMY_INT8 = 0;
  static int16_t DUMMY_INT16 = 0;
  static int32_t DUMMY_INT32 = 0;
  static int64_t DUMMY_INT64 = 0;
  static float DUMMY_FLOAT = 0.0;
  static double DUMMY_DOUBLE = 0.0;
  static char DUMMY_TEXT[1] = "";
  static TimestampType DUMMY_TIMESTAMP = {0, 0};

  MemoryContext per_row_context = edd->ext_pstate->rowcontext;
  MemoryContext old_context = MemoryContextSwitchTo(per_row_context);

  /* Get column values */
  user_data->colRawTid = (char *)(&(user_data->colTid));
  for (int i = 0; i < user_data->numberOfColumns; ++i) {
    int dataType = (int)(tts->tts_tupleDescriptor->attrs[i]->atttypid);

    user_data->colRawValues[i] = NULL;

    if (user_data->colIsNulls[i]) {
      if (dataType == HAWQ_TYPE_CHAR) {
        user_data->colRawValues[i] = (char *)(&DUMMY_INT8);
      } else if (dataType == HAWQ_TYPE_INT2) {
        user_data->colRawValues[i] = (char *)(&DUMMY_INT16);
      } else if (dataType == HAWQ_TYPE_INT4) {
        user_data->colRawValues[i] = (char *)(&DUMMY_INT32);
      } else if (dataType == HAWQ_TYPE_INT8) {
        user_data->colRawValues[i] = (char *)(&DUMMY_INT64);
      } else if (dataType == HAWQ_TYPE_TEXT || dataType == HAWQ_TYPE_BYTE ||
                 dataType == HAWQ_TYPE_BPCHAR ||
                 dataType == HAWQ_TYPE_VARCHAR ||
                 dataType == HAWQ_TYPE_NUMERIC) {
        user_data->colRawValues[i] = (char *)(DUMMY_TEXT);
      } else if (dataType == HAWQ_TYPE_FLOAT4) {
        user_data->colRawValues[i] = (char *)(&DUMMY_FLOAT);
      } else if (dataType == HAWQ_TYPE_FLOAT8) {
        user_data->colRawValues[i] = (char *)(&DUMMY_DOUBLE);
      } else if (dataType == HAWQ_TYPE_BOOL) {
        user_data->colRawValues[i] = (char *)(&DUMMY_BOOL);
      } else if (dataType == HAWQ_TYPE_DATE) {
        user_data->colRawValues[i] = (char *)(&DUMMY_INT32);
      } else if (dataType == HAWQ_TYPE_TIME) {
        user_data->colRawValues[i] = (char *)(&DUMMY_INT64);
      } else if (dataType == HAWQ_TYPE_TIMESTAMP) {
        user_data->colRawValues[i] = (char *)(&DUMMY_TIMESTAMP);
      } else if (STRUCTEXID == user_data->colDatatypes[i] ||
                 IOBASETYPEID == user_data->colDatatypes[i]) {
        user_data->colRawValues[i] = (char *)(DUMMY_TEXT);
      } else if (dataType == HAWQ_TYPE_INVALID) {
        elog(ERROR, "HAWQ data type %s is invalid", TypeNameToString(makeTypeNameFromOid(dataType, -1)));
      } else {
        elog(ERROR, "HAWQ data type %s is not supported yet", TypeNameToString(makeTypeNameFromOid(dataType, -1)));
      }

      continue;
    }

    if (dataType == HAWQ_TYPE_INT4 || dataType == HAWQ_TYPE_INT8 ||
        dataType == HAWQ_TYPE_FLOAT4 || dataType == HAWQ_TYPE_FLOAT8 ||
        dataType == HAWQ_TYPE_INT2 || dataType == HAWQ_TYPE_CHAR ||
        dataType == HAWQ_TYPE_BOOL || dataType == HAWQ_TYPE_TIME) {
      user_data->colRawValues[i] = (char *)(&(user_data->colValues[i]));
    } else if (dataType == HAWQ_TYPE_DATE) {
      int *date = (int *)(&(user_data->colValues[i]));
      *date += POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE;
      user_data->colRawValues[i] = (char *)(&(user_data->colValues[i]));
    } else if (dataType == HAWQ_TYPE_TIMESTAMP) {
      int64_t *timestamp = (int64_t *) (&(user_data->colValues[i]));
      user_data->colTimestamp[i].second = *timestamp / 1000000
          + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * 60 * 60 * 24;
      user_data->colTimestamp[i].nanosecond = *timestamp % 1000000 * 1000;
      int64_t days = user_data->colTimestamp[i].second / 60 / 60 / 24;
      if (user_data->colTimestamp[i].nanosecond < 0 &&
          (days > POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE || days < 0))
        user_data->colTimestamp[i].nanosecond += 1000000000;
      if(user_data->colTimestamp[i].second < 0 && user_data->colTimestamp[i].nanosecond)
        user_data->colTimestamp[i].second -= 1;
      user_data->colRawValues[i] = (char *) (&(user_data->colTimestamp[i]));
    } else if (dataType == HAWQ_TYPE_TEXT || dataType == HAWQ_TYPE_BYTE ||
               dataType == HAWQ_TYPE_BPCHAR || dataType == HAWQ_TYPE_VARCHAR ||
               dataType == HAWQ_TYPE_NUMERIC) {
      user_data->colRawValues[i] = OutputFunctionCall(
          &(edd->ext_pstate->out_functions[i]), user_data->colValues[i]);
    } else if (STRUCTEXID == user_data->colDatatypes[i]) {
      int32_t len = VARSIZE(user_data->colValues[i]);
      if (len <= 0) {
        elog(ERROR, "HAWQ base type(udt) %s should not be less than 0",
             TypeNameToString(makeTypeNameFromOid(dataType, -1)));
      }

      char *pVal = DatumGetPointer(user_data->colValues[i]);
      user_data->colRawValues[i] = palloc0(VARHDRSZ + len);

      //  set value : the first 4 byte is length, than the raw value
      // SET_VARSIZE(  (struct varlena * )user_data->colRawValues[i], len);
      *((int32 *)(user_data->colRawValues[i])) = len;
      memcpy(user_data->colRawValues[i] + VARHDRSZ, pVal, len);
    } else if (IOBASETYPEID == user_data->colDatatypes[i]) {
      //  get the length of basetype
      bool passbyval = tts->tts_tupleDescriptor->attrs[i]->attbyval;
      int32_t orilen = (int32_t)(tts->tts_tupleDescriptor->attrs[i]->attlen);
      int32_t len =
          get_typlen_fast(dataType, passbyval, orilen, user_data->colValues[i]);

      if (1 > len) {  //  invalid length
        elog(ERROR,
             "HAWQ composite type(udt) %s got an invalid length:%d",
             TypeNameToString(makeTypeNameFromOid(dataType, -1)), len);
      }

      if (passbyval) {
        //  value store in Datum directly
        char *val = (char *)(user_data->colValues[i]);
        user_data->colRawValues[i] = palloc0(len);
        memcpy(user_data->colRawValues[i], val, len);
      } else {
        //  value stored by pointer in Datum
        char *val = DatumGetPointer(user_data->colValues[i]);
        user_data->colRawValues[i] = palloc0(VARHDRSZ + len);

        //  set value : the first 4 byte is length, than the raw value
        // SET_VARSIZE(  (struct varlena * )user_data->colRawValues[i], len);
        *((int32 *)(user_data->colRawValues[i])) = len;
        memcpy(user_data->colRawValues[i] + VARHDRSZ, val, len);
      }
    } else if (dataType == HAWQ_TYPE_INVALID) {
      elog(ERROR, "HAWQ data type %s is invalid", TypeNameToString(makeTypeNameFromOid(dataType, -1)));
    } else {
      elog(ERROR, "HAWQ data type %s is not supported yet", TypeNameToString(makeTypeNameFromOid(dataType, -1)));
    }
  }

  /* Pass to formatter to output */
  MagmaFormatDeleteMagmaFormatC(user_data->fmt, user_data->colRawTid,
                                user_data->colRawValues, user_data->colIsNulls);

  MagmaFormatCatchedError *e = MagmaFormatGetErrorMagmaFormatC(user_data->fmt);
  if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    elog(ERROR, "magma_delete: failed to delete: %s(%d)", e->errMessage,
         e->errCode);
  }

  MemoryContextReset(per_row_context);
  MemoryContextSwitchTo(old_context);

  ps->ps_tuple_oid = InvalidOid;

  PG_RETURN_VOID();
}

/* void
 * magma_enddelete(ExternalInsertDesc extDeleteDesc)
 */
Datum magma_enddelete(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  ExternalInsertDesc edd = ps->ps_ext_delete_desc;

  MagmaFormatUserData *user_data =
      (MagmaFormatUserData *)(edd->ext_ps_user_data);

  MagmaFormatEndDeleteMagmaFormatC(user_data->fmt);

  MagmaFormatCatchedError *e = MagmaFormatGetErrorMagmaFormatC(user_data->fmt);
  if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    elog(ERROR, "magma_delete: failed to end delete: %s(%d)", e->errMessage,
         e->errCode);
  }
  MagmaFormatFreeMagmaFormatC(&(user_data->fmt));

  for (int i = 0; i < user_data->numberOfColumns; ++i) {
    pfree(user_data->colNames[i]);
  }
  pfree(user_data->colNames);
  /*
   * DO NOT pfree colValues and colIsNulls here since ExecutorEnd will call
   * cleanup_slot to pfree slot->PRIVATE_tts_values and
   * slot->PRIVATE_tts_isnull. Otherwise it will be freed 2 times.
   *
   * pfree(user_data->colValues);
   * pfree(user_data->colIsNulls);
   */
  pfree(user_data->colDatatypes);
  pfree(user_data->colRawValues);
  pfree(user_data);

  if (edd->ext_formatter_data) pfree(edd->ext_formatter_data);

  if (edd->ext_pstate != NULL && edd->ext_pstate->rowcontext != NULL) {
    /*
     * delete the row context
     */
    MemoryContextDelete(edd->ext_pstate->rowcontext);
    edd->ext_pstate->rowcontext = NULL;
  }

  pfree(edd);

  PG_RETURN_VOID();
}

/* ExternalInsertDesc
 * magma_beginupdate(Relation relation)
 */
Datum magma_beginupdate(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  Relation relation = ps->ps_relation;
  char *serializeSchema = ps->ps_magma_serializeSchema;
  int serializeSchemaLen = ps->ps_magma_serializeSchemaLen;
  MagmaSnapshot *snapshot = &(ps->ps_snapshot);

  /* 1. Allocate and initialize the update descriptor */
  ExternalInsertDesc eud = palloc0(sizeof(ExternalInsertDescData));

  ExtTableEntry *ete = GetExtTableEntry(RelationGetRelid(relation));

  int formatterType = ExternalTableType_Invalid;

  char *formatterName = NULL;
  getExternalTableTypeStr(ete->fmtcode, ete->fmtopts, &formatterType,
                          &formatterName);

  /* 1.1 Setup update functions */
  get_magma_update_functions(formatterName, eud);

  List *fmt_opts = NIL;
  fmt_opts = lappend(fmt_opts, makeString(pstrdup(ete->fmtopts)));

  /* 1.2 Allocate and initialize structure which track data parsing state */
  eud->ext_pstate = (CopyStateData *)palloc0(sizeof(CopyStateData));
  eud->ext_tupDesc = RelationGetDescr(relation);

  /* 1.3 Initialize parse state */
  /* 1.3.1 Initialize basic information for pstate */
  CopyState pstate = eud->ext_pstate;

  /* 1.3.2 Setup encoding information */
  /*
   * Set up encoding conversion info.  Even if the client and server
   * encodings are the same, we must apply pg_client_to_server() to validate
   * data in multibyte encodings.
   *
   * Each external table specifies the encoding of its external data. We will
   * therefore set a client encoding and client-to-server conversion procedure
   * in here (server-to-client in WET) and these will be used in the data
   * conversion routines (in copy.c CopyReadLineXXX(), etc).
   */
  int fmt_encoding = ete->encoding;
  Insist(PG_VALID_ENCODING(fmt_encoding));
  pstate->client_encoding = fmt_encoding;
  Oid conversion_proc =
      FindDefaultConversionProc(GetDatabaseEncoding(), fmt_encoding);

  if (OidIsValid(conversion_proc)) {
    /* conversion proc found */
    pstate->enc_conversion_proc = palloc0(sizeof(FmgrInfo));
    fmgr_info(conversion_proc, pstate->enc_conversion_proc);
  } else {
    /* no conversion function (both encodings are probably the same) */
    pstate->enc_conversion_proc = NULL;
  }

  pstate->need_transcoding = pstate->client_encoding != GetDatabaseEncoding();
  pstate->encoding_embeds_ascii =
      PG_ENCODING_IS_CLIENT_ONLY(pstate->client_encoding);

  /* 1.3.3 Setup tuple description */
  TupleDesc tup_desc = eud->ext_tupDesc;
  pstate->attr_offsets = (int *)palloc0(tup_desc->natts * sizeof(int));

  /* 1.3.4 Generate or convert list of attributes to process */
  pstate->attnumlist = CopyGetAttnums(tup_desc, relation, NIL);

  /* 1.3.5 Convert FORCE NOT NULL name list to per-column flags, check validity
   */
  pstate->force_notnull_flags = (bool *)palloc0(tup_desc->natts * sizeof(bool));
  if (pstate->force_notnull) {
    List *attnums;
    ListCell *cur;

    attnums = CopyGetAttnums(tup_desc, relation, pstate->force_notnull);

    foreach (cur, attnums) {
      int attnum = lfirst_int(cur);
      pstate->force_notnull_flags[attnum - 1] = true;
    }
  }

  /* 1.3.6 Take care of state that is WET specific */
  Form_pg_attribute *attr = tup_desc->attrs;
  ListCell *cur;

  pstate->null_print_client = pstate->null_print; /* default */
  pstate->fe_msgbuf = makeStringInfo(); /* use fe_msgbuf as a per-row buffer */
  pstate->out_functions =
      (FmgrInfo *)palloc0(tup_desc->natts * sizeof(FmgrInfo));

  foreach (cur,
           pstate->attnumlist) /* Get info about the columns need to process */
  {
    int attnum = lfirst_int(cur);
    Oid out_func_oid;
    bool isvarlena;

    getTypeOutputInfo(attr[attnum - 1]->atttypid, &out_func_oid, &isvarlena);
    fmgr_info(out_func_oid, &pstate->out_functions[attnum - 1]);
  }

  /*
   * We need to convert null_print to client encoding, because it
   * will be sent directly with CopySendString.
   */
  if (pstate->need_transcoding) {
    pstate->null_print_client = pg_server_to_custom(
        pstate->null_print, pstate->null_print_len, pstate->client_encoding,
        pstate->enc_conversion_proc);
  }

  /* 1.3.7 Create temporary memory context for per row process */
  /*
   * Create a temporary memory context that we can reset once per row to
   * recover palloc'd memory.  This avoids any problems with leaks inside
   * datatype input or output routines, and should be faster than retail
   * pfree's anyway.
   */
  pstate->rowcontext = AllocSetContextCreate(
      CurrentMemoryContext, "ExtTableMemCxt", ALLOCSET_DEFAULT_MINSIZE,
      ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

  /* 1.3.8 Parse format options */
  char *format_str = pstrdup((char *)strVal(linitial(fmt_opts)));
  char *fmt_name = NULL;
  List *l = magma_parse_format_string(format_str, &fmt_name);
  pstate->custom_formatter_name = fmt_name;
  pstate->custom_formatter_params = l;
  pfree(format_str);

  /* 1.4 Initialize formatter data */
  eud->ext_formatter_data = (FormatterData *)palloc0(sizeof(FormatterData));
  eud->ext_formatter_data->fmt_perrow_ctx = eud->ext_pstate->rowcontext;

  /* 2. Setup user data */

  /* 2.1 Get database, schema, table name for the update */
  Assert(database != NULL);
  Oid namespaceOid = RelationGetNamespace(relation);
  char *schema = getNamespaceNameByOid(namespaceOid);
  char *table = RelationGetRelationName(relation);

  MagmaFormatUserData *user_data =
      (MagmaFormatUserData *)palloc0(sizeof(MagmaFormatUserData));

  if (formatterName != NULL &&
      (strncasecmp(formatterName, "magmatp", sizeof("magmatp") - 1) == 0)) {
    user_data->isMagmatp = true;
  } else {
    user_data->isMagmatp = false;
  }

  init_magma_format_user_data_for_write(tup_desc, user_data, relation);

  /* the number of ranges is dynamic for magma table */
  int32_t nRanges = 0;
  ListCell *lc_split = NULL;
  foreach (lc_split, ps->ps_magma_splits) {
      List *split = (List *)lfirst(lc_split);
      nRanges += list_length(split);
  }

  /* 2.2 Create formatter instance */
  bool isexternal = false;
  get_magma_category_info(ete->fmtopts, &isexternal);

  List *fmt_opts_defelem = pstate->custom_formatter_params;
  user_data->fmt = create_magma_formatter_instance(
      fmt_opts_defelem, serializeSchema, serializeSchemaLen, fmt_encoding,
      formatterName, nRanges);

  /*prepare hash info */
  int32_t nDistKeyIndex = 0;
  int16_t *distKeyIndex = NULL;
  fetchDistributionPolicy(relation->rd_id, &nDistKeyIndex, &distKeyIndex);


  int32_t range_to_rg_map[nRanges];
  List *rg = magma_build_range_to_rg_map(ps->ps_magma_splits, range_to_rg_map);
  int nRg = list_length(rg);
  int16_t *rgId = palloc0(sizeof(int16_t) * nRg);
  char **rgUrl = palloc0(sizeof(char *) * nRg);
  magma_build_rg_to_url_map(ps->ps_magma_splits, rg, rgId, rgUrl);

  /* 2.3 Prepare database, schema, and table information */
  MagmaFormatC_SetupTarget(user_data->fmt, database, schema, table);
  MagmaFormatC_SetupTupDesc(user_data->fmt, user_data->numberOfColumns,
                            user_data->colNames, user_data->colDatatypes,
                            user_data->colDatatypeMods, user_data->colIsNulls);

  int *jumpHashMap = get_jump_hash_map(nRanges);
  MagmaFormatC_SetupHasher(user_data->fmt, nDistKeyIndex, distKeyIndex, nRanges,
                           range_to_rg_map, nRg, rgId, rgUrl, jumpHashMap,
                           JUMP_HASH_MAP_LENGTH);
  MagmaFormatCatchedError *e = MagmaFormatGetErrorMagmaFormatC(user_data->fmt);
  if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    elog(ERROR, "magma_update: failed to begin update: %s(%d)", e->errMessage,
         e->errCode);
  }
  /* 2.4 Save user data */
  eud->ext_ps_user_data = (void *)user_data;

  /* 3. Begin insert with the formatter */
  MagmaFormatBeginUpdateMagmaFormatC(user_data->fmt, snapshot);
  MagmaFormatCatchedError *e1 = MagmaFormatGetErrorMagmaFormatC(user_data->fmt);
  if (e1->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    elog(ERROR, "magma_update: failed to begin update: %s(%d)", e1->errMessage,
         e1->errCode);
  }
  /* 4. Save the result */
  ps->ps_ext_update_desc = eud;

  PG_RETURN_POINTER(eud);
}

/* void
 * magma_delete(ExternalInsertDesc extUpdDesc,
 *           TupleTableSlot *tupTableSlot)
 */
Datum magma_update(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  ExternalInsertDesc eud = ps->ps_ext_update_desc;
  TupleTableSlot *tts = ps->ps_tuple_table_slot;

  /* It may be memtuple, we need to transfer it to virtual tuple */
  slot_getallattrs(tts);

  MagmaFormatUserData *user_data =
      (MagmaFormatUserData *)(eud->ext_ps_user_data);

  user_data->colTid.rangeid = DatumGetUInt16(eud->ext_rangeId);
  user_data->colTid.rowid = DatumGetUInt64(eud->ext_rowId);
  user_data->colValues = slot_get_values(tts);
  user_data->colIsNulls = slot_get_isnull(tts);

  static bool DUMMY_BOOL = true;
  static int8_t DUMMY_INT8 = 0;
  static int16_t DUMMY_INT16 = 0;
  static int32_t DUMMY_INT32 = 0;
  static int64_t DUMMY_INT64 = 0;
  static float DUMMY_FLOAT = 0.0;
  static double DUMMY_DOUBLE = 0.0;
  static char DUMMY_TEXT[1] = "";
  static TimestampType DUMMY_TIMESTAMP = {0, 0};

  MemoryContext per_row_context = eud->ext_pstate->rowcontext;
  MemoryContext old_context = MemoryContextSwitchTo(per_row_context);

  /* Get column values */
  user_data->colRawTid = (char *)(&(user_data->colTid));
  for (int i = 0; i < user_data->numberOfColumns; ++i) {
    int dataType = (int)(tts->tts_tupleDescriptor->attrs[i]->atttypid);

    user_data->colRawValues[i] = NULL;

    if (user_data->colIsNulls[i]) {
      if (dataType == HAWQ_TYPE_CHAR) {
        user_data->colRawValues[i] = (char *)(&DUMMY_INT8);
      } else if (dataType == HAWQ_TYPE_INT4) {
        user_data->colRawValues[i] = (char *)(&DUMMY_INT32);
      } else if (dataType == HAWQ_TYPE_INT8) {
        user_data->colRawValues[i] = (char *)(&DUMMY_INT64);
      } else if (dataType == HAWQ_TYPE_TEXT || dataType == HAWQ_TYPE_BYTE ||
                 dataType == HAWQ_TYPE_BPCHAR ||
                 dataType == HAWQ_TYPE_VARCHAR ||
                 dataType == HAWQ_TYPE_NUMERIC) {
        user_data->colRawValues[i] = (char *)(DUMMY_TEXT);
      } else if (dataType == HAWQ_TYPE_FLOAT4) {
        user_data->colRawValues[i] = (char *)(&DUMMY_FLOAT);
      } else if (dataType == HAWQ_TYPE_FLOAT8) {
        user_data->colRawValues[i] = (char *)(&DUMMY_DOUBLE);
      } else if (dataType == HAWQ_TYPE_INT2) {
        user_data->colRawValues[i] = (char *)(&DUMMY_INT16);
      } else if (dataType == HAWQ_TYPE_BOOL) {
        user_data->colRawValues[i] = (char *)(&DUMMY_BOOL);
      } else if (dataType == HAWQ_TYPE_DATE) {
        user_data->colRawValues[i] = (char *)(&DUMMY_INT32);
      } else if (dataType == HAWQ_TYPE_TIME) {
        user_data->colRawValues[i] = (char *)(&DUMMY_INT64);
      } else if (dataType == HAWQ_TYPE_TIMESTAMP) {
        user_data->colRawValues[i] = (char *)(&DUMMY_TIMESTAMP);
      } else if (STRUCTEXID == user_data->colDatatypes[i] ||
                 IOBASETYPEID == user_data->colDatatypes[i]) {
        user_data->colRawValues[i] = (char *)(DUMMY_TEXT);
      } else if (dataType == HAWQ_TYPE_INVALID) {
        elog(ERROR, "HAWQ data type %s is invalid", TypeNameToString(makeTypeNameFromOid(dataType, -1)));
      } else {
        elog(ERROR, "HAWQ data type %s is not supported yet", TypeNameToString(makeTypeNameFromOid(dataType, -1)));
      }

      continue;
    }

    if (dataType == HAWQ_TYPE_INT4 || dataType == HAWQ_TYPE_INT8 ||
        dataType == HAWQ_TYPE_FLOAT4 || dataType == HAWQ_TYPE_FLOAT8 ||
        dataType == HAWQ_TYPE_INT2 || dataType == HAWQ_TYPE_CHAR ||
        dataType == HAWQ_TYPE_BOOL || dataType == HAWQ_TYPE_TIME) {
      user_data->colRawValues[i] = (char *)(&(user_data->colValues[i]));
    } else if (dataType == HAWQ_TYPE_DATE) {
      int *date = (int *)(&(user_data->colValues[i]));
      *date += POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE;
      user_data->colRawValues[i] = (char *)(&(user_data->colValues[i]));
    } else if (dataType == HAWQ_TYPE_TIMESTAMP) {
      int64_t *timestamp = (int64_t *) (&(user_data->colValues[i]));
      user_data->colTimestamp[i].second = *timestamp / 1000000
          + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * 60 * 60 * 24;
      user_data->colTimestamp[i].nanosecond = *timestamp % 1000000 * 1000;
      int64_t days = user_data->colTimestamp[i].second / 60 / 60 / 24;
      if (user_data->colTimestamp[i].nanosecond < 0 &&
          (days > POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE || days < 0))
        user_data->colTimestamp[i].nanosecond += 1000000000;
      if(user_data->colTimestamp[i].second < 0 && user_data->colTimestamp[i].nanosecond)
        user_data->colTimestamp[i].second -= 1;
      user_data->colRawValues[i] = (char *) (&(user_data->colTimestamp[i]));
    } else if (dataType == HAWQ_TYPE_TEXT || dataType == HAWQ_TYPE_BYTE ||
               dataType == HAWQ_TYPE_BPCHAR || dataType == HAWQ_TYPE_VARCHAR) {
      user_data->colRawValues[i] = OutputFunctionCall(
          &(eud->ext_pstate->out_functions[i]), user_data->colValues[i]);
    } else if (dataType == HAWQ_TYPE_NUMERIC) {
      user_data->colRawValues[i] = OutputFunctionCall(
          &(eud->ext_pstate->out_functions[i]), user_data->colValues[i]);
      Numeric num = DatumGetNumeric(user_data->colValues[i]);
      if (NUMERIC_IS_NAN(num)) {
        user_data->colIsNulls[i] = true;
      }
    } else if (STRUCTEXID == user_data->colDatatypes[i]) {
      int32_t len = VARSIZE(user_data->colValues[i]);
      if (len <= 0) {
        elog(ERROR, "HAWQ base type(udt) %s should not be less than 0",
             TypeNameToString(makeTypeNameFromOid(dataType, -1)));
      }

      char *pVal = DatumGetPointer(user_data->colValues[i]);
      user_data->colRawValues[i] = palloc0(VARHDRSZ + len);

      //  set value : the first 4 byte is length, than the raw value
      // SET_VARSIZE(  (struct varlena * )user_data->colRawValues[i], len);
      *((int32 *)(user_data->colRawValues[i])) = len;
      memcpy(user_data->colRawValues[i] + VARHDRSZ, pVal, len);
    } else if (IOBASETYPEID == user_data->colDatatypes[i]) {
      //  get the length of basetype
      bool passbyval = tts->tts_tupleDescriptor->attrs[i]->attbyval;
      int32_t orilen = (int32_t)(tts->tts_tupleDescriptor->attrs[i]->attlen);
      int32_t len =
          get_typlen_fast(dataType, passbyval, orilen, user_data->colValues[i]);

      if (1 > len) {  //  invalid length
        elog(ERROR,
             "HAWQ composite type(udt) %s got an invalid length:%d",
             TypeNameToString(makeTypeNameFromOid(dataType, -1)), len);
      }

      if (passbyval) {
        //  value store in Datum directly
        char *val = (char *)(user_data->colValues[i]);
        user_data->colRawValues[i] = palloc0(len);
        memcpy(user_data->colRawValues[i], val, len);
      } else {
        //  value stored by pointer in Datum
        char *val = DatumGetPointer(user_data->colValues[i]);
        user_data->colRawValues[i] = palloc0(VARHDRSZ + len);

        //  set value : the first 4 byte is length, than the raw value
        // SET_VARSIZE(  (struct varlena * )user_data->colRawValues[i], len);
        *((int32 *)(user_data->colRawValues[i])) = len;
        memcpy(user_data->colRawValues[i] + VARHDRSZ, val, len);
      }
    } else if (dataType == HAWQ_TYPE_INVALID) {
      elog(ERROR, "HAWQ data type %s is invalid", TypeNameToString(makeTypeNameFromOid(dataType, -1)));
    } else {
      elog(ERROR, "HAWQ data type %s is not supported yet", TypeNameToString(makeTypeNameFromOid(dataType, -1)));
    }
  }

  /* Pass to formatter to output */
  int updateCount = MagmaFormatUpdateMagmaFormatC(user_data->fmt, user_data->colRawTid,
                                user_data->colRawValues, user_data->colIsNulls);

  ps->ps_update_count = updateCount;

  MagmaFormatCatchedError *e = MagmaFormatGetErrorMagmaFormatC(user_data->fmt);
  if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    elog(ERROR, "magma_update: failed to update: %s(%d)", e->errMessage,
         e->errCode);
  }

  MemoryContextReset(per_row_context);
  MemoryContextSwitchTo(old_context);

  ps->ps_tuple_oid = InvalidOid;

  // PG_RETURN_VOID();
  PG_RETURN_UINT32(updateCount);
}

/* void
 * magma_endupdate(ExternalInsertDesc extUpdDesc)
 */
Datum magma_endupdate(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  ExternalInsertDesc eud = ps->ps_ext_update_desc;

  MagmaFormatUserData *user_data =
      (MagmaFormatUserData *)(eud->ext_ps_user_data);

  int updateCount = MagmaFormatEndUpdateMagmaFormatC(user_data->fmt);
  ps->ps_update_count = updateCount;

  MagmaFormatCatchedError *e = MagmaFormatGetErrorMagmaFormatC(user_data->fmt);
  if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    elog(ERROR, "magma_update: failed to end update: %s(%d)", e->errMessage,
         e->errCode);
  }
  MagmaFormatFreeMagmaFormatC(&(user_data->fmt));

  for (int i = 0; i < user_data->numberOfColumns; ++i) {
    pfree(user_data->colNames[i]);
  }
  pfree(user_data->colNames);
  pfree(user_data->colDatatypes);
  pfree(user_data->colRawValues);

  if (eud->ext_formatter_data) {
    pfree(eud->ext_formatter_data);
  }

  if (eud->ext_pstate != NULL && eud->ext_pstate->rowcontext != NULL) {
    /*
     * delete the row context
     */
    MemoryContextDelete(eud->ext_pstate->rowcontext);
    eud->ext_pstate->rowcontext = NULL;
  }

  pfree(eud);

  // PG_RETURN_VOID();
  PG_RETURN_UINT32(updateCount);
}

/*
 * ExternalInsertDesc
 * magma_insert_init(Relation relation,
 *                int formatterType,
 *                char *formatterName)
 */
Datum magma_insert_init(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  Relation relation = ps->ps_relation;
  int formatterType = ps->ps_formatter_type;
  char *formatterName = ps->ps_formatter_name;
  char *serializeSchema = ps->ps_magma_serializeSchema;
  int serializeSchemaLen = ps->ps_magma_serializeSchemaLen;
  MagmaSnapshot *snapshot = &(ps->ps_snapshot);

  /* 1. Allocate and initialize the insert descriptor */
  ExternalInsertDesc eid = palloc0(sizeof(ExternalInsertDescData));
  eid->ext_formatter_type = formatterType;
  eid->ext_formatter_name = formatterName;

  /* 1.1 Setup insert functions */
  get_magma_insert_functions(formatterName, eid);

  /* 1.2 Initialize basic information */
  eid->ext_rel = relation;
  eid->ext_noop = (Gp_role == GP_ROLE_DISPATCH);
  eid->ext_formatter_data = NULL;

  /* 1.3 Get URI string */
  ExtTableEntry *ete = GetExtTableEntry(RelationGetRelid(relation));

  Value *v = linitial(ete->locations);
  char *uri_str = pstrdup(v->val.str);
  eid->ext_uri = uri_str;

  /* 1.4 Allocate and initialize structure which track data parsing state */
  eid->ext_pstate = (CopyStateData *)palloc0(sizeof(CopyStateData));
  eid->ext_tupDesc = RelationGetDescr(relation);
  eid->ext_values = (Datum *)palloc0(eid->ext_tupDesc->natts * sizeof(Datum));
  eid->ext_nulls = (bool *)palloc0(eid->ext_tupDesc->natts * sizeof(bool));

  /* 1.5 Get format options */
  List *fmt_opts = NIL;
  fmt_opts = lappend(fmt_opts, makeString(pstrdup(ete->fmtopts)));

  /* 1.6 Initialize parse state */
  /* 1.6.1 Initialize basic information for pstate */
  CopyState pstate = eid->ext_pstate;
  pstate->fe_eof = false;
  pstate->eol_type = EOL_UNKNOWN;
  pstate->eol_str = NULL;
  pstate->cur_relname = RelationGetRelationName(relation);
  pstate->cur_lineno = 0;
  pstate->err_loc_type = ROWNUM_ORIGINAL;
  pstate->cur_attname = NULL;
  pstate->raw_buf_done = true; /* true so we will read data in first run */
  pstate->line_done = true;
  pstate->bytesread = 0;
  pstate->custom = false;
  pstate->header_line = false;
  pstate->fill_missing = false;
  pstate->line_buf_converted = false;
  pstate->raw_buf_index = 0;
  pstate->processed = 0;
  pstate->filename = uri_str;
  pstate->copy_dest = COPY_EXTERNAL_SOURCE;
  pstate->missing_bytes = 0;
  pstate->rel = relation;

  /* 1.6.2 Setup encoding information */
  /*
   * Set up encoding conversion info.  Even if the client and server
   * encodings are the same, we must apply pg_client_to_server() to validate
   * data in multibyte encodings.
   *
   * Each external table specifies the encoding of its external data. We will
   * therefore set a client encoding and client-to-server conversion procedure
   * in here (server-to-client in WET) and these will be used in the data
   * conversion routines (in copy.c CopyReadLineXXX(), etc).
   */
  int fmt_encoding = ete->encoding;
  Insist(PG_VALID_ENCODING(fmt_encoding));
  pstate->client_encoding = fmt_encoding;
  Oid conversion_proc =
      FindDefaultConversionProc(GetDatabaseEncoding(), fmt_encoding);

  if (OidIsValid(conversion_proc)) {
    /* conversion proc found */
    pstate->enc_conversion_proc = palloc0(sizeof(FmgrInfo));
    fmgr_info(conversion_proc, pstate->enc_conversion_proc);
  } else {
    /* no conversion function (both encodings are probably the same) */
    pstate->enc_conversion_proc = NULL;
  }

  pstate->need_transcoding = pstate->client_encoding != GetDatabaseEncoding();
  pstate->encoding_embeds_ascii =
      PG_ENCODING_IS_CLIENT_ONLY(pstate->client_encoding);

  /* 1.6.3 Parse format options */
  char *format_str = pstrdup((char *)strVal(linitial(fmt_opts)));
  char *fmt_name = NULL;
  List *l = magma_parse_format_string(format_str, &fmt_name);
  pstate->custom_formatter_name = fmt_name;
  pstate->custom_formatter_params = l;
  pfree(format_str);

  /* 1.6.4 Setup tuple description */
  TupleDesc tup_desc = eid->ext_tupDesc;
  pstate->attr_offsets = (int *)palloc0(tup_desc->natts * sizeof(int));

  /* 1.6.5 Generate or convert list of attributes to process */
  pstate->attnumlist = CopyGetAttnums(tup_desc, relation, NIL);

  /* 1.6.6 Convert FORCE NOT NULL name list to per-column flags, check validity
   */
  pstate->force_notnull_flags = (bool *)palloc0(tup_desc->natts * sizeof(bool));
  if (pstate->force_notnull) {
    List *attnums;
    ListCell *cur;

    attnums = CopyGetAttnums(tup_desc, relation, pstate->force_notnull);

    foreach (cur, attnums) {
      int attnum = lfirst_int(cur);
      pstate->force_notnull_flags[attnum - 1] = true;
    }
  }

  /* 1.6.7 Take care of state that is WET specific */
  Form_pg_attribute *attr = tup_desc->attrs;
  ListCell *cur;

  pstate->null_print_client = pstate->null_print; /* default */
  pstate->fe_msgbuf = makeStringInfo(); /* use fe_msgbuf as a per-row buffer */
  pstate->out_functions =
      (FmgrInfo *)palloc0(tup_desc->natts * sizeof(FmgrInfo));

  foreach (cur,
           pstate->attnumlist) /* Get info about the columns need to process */
  {
    int attnum = lfirst_int(cur);
    Oid out_func_oid;
    bool isvarlena;

    getTypeOutputInfo(attr[attnum - 1]->atttypid, &out_func_oid, &isvarlena);
    fmgr_info(out_func_oid, &pstate->out_functions[attnum - 1]);
  }

  /*
   * We need to convert null_print to client encoding, because it
   * will be sent directly with CopySendString.
   */
  if (pstate->need_transcoding) {
    pstate->null_print_client = pg_server_to_custom(
        pstate->null_print, pstate->null_print_len, pstate->client_encoding,
        pstate->enc_conversion_proc);
  }

  /* 1.6.8 Create temporary memory context for per row process */
  /*
   * Create a temporary memory context that we can reset once per row to
   * recover palloc'd memory.  This avoids any problems with leaks inside
   * datatype input or output routines, and should be faster than retail
   * pfree's anyway.
   */
  pstate->rowcontext = AllocSetContextCreate(
      CurrentMemoryContext, "ExtTableMemCxt", ALLOCSET_DEFAULT_MINSIZE,
      ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

  /* 1.7 Initialize formatter data */
  eid->ext_formatter_data = (FormatterData *)palloc0(sizeof(FormatterData));
  eid->ext_formatter_data->fmt_perrow_ctx = eid->ext_pstate->rowcontext;

  /* 2. Setup user data */

  /* 2.1 Create formatter instance */
  Assert(database != NULL);
  Oid namespaceOid = RelationGetNamespace(relation);
  char *schema = getNamespaceNameByOid(namespaceOid);
  char *table = RelationGetRelationName(relation);

  MagmaFormatUserData *user_data =
      (MagmaFormatUserData *)palloc0(sizeof(MagmaFormatUserData));

  if (formatterName != NULL &&
      (strncasecmp(formatterName, "magmatp", sizeof("magmatp") - 1) == 0)) {
    user_data->isMagmatp = true;
  } else {
    user_data->isMagmatp = false;
  }

  /* the number of ranges is dynamic for magma table */
  int32_t nRanges = 0;
  ListCell *lc_split = NULL;
  foreach (lc_split, ps->ps_magma_splits) {
      List *split = (List *)lfirst(lc_split);
      nRanges += list_length(split);
  }

  init_magma_format_user_data_for_write(tup_desc, user_data, relation);

  /*2.2 Create formatter instance*/
  bool isexternal = false;
  get_magma_category_info(ete->fmtopts, &isexternal);

  List *fmt_opts_defelem = pstate->custom_formatter_params;
  user_data->fmt = create_magma_formatter_instance(
      fmt_opts_defelem, serializeSchema, serializeSchemaLen, fmt_encoding,
      formatterName, nRanges);

  /* prepare hash info */
  int32_t nDistKeyIndex = 0;
  int16_t *distKeyIndex = NULL;
  fetchDistributionPolicy(relation->rd_id, &nDistKeyIndex, &distKeyIndex);

  uint32 range_to_rg_map[nRanges];
  List *rg = magma_build_range_to_rg_map(ps->ps_magma_splits, range_to_rg_map);
  int nRg = list_length(rg);
  uint16 *rgId = palloc0(sizeof(uint16) * nRg);
  char **rgUrl = palloc0(sizeof(char *) * nRg);
  magma_build_rg_to_url_map(ps->ps_magma_splits, rg, rgId, rgUrl);

  /* 2.3 Prepare database, schema, and table information */
  MagmaFormatC_SetupTarget(user_data->fmt, database, schema, table);
  MagmaFormatC_SetupTupDesc(user_data->fmt, user_data->numberOfColumns,
                            user_data->colNames, user_data->colDatatypes,
                            user_data->colDatatypeMods, user_data->colIsNulls);

  int *jumpHashMap = get_jump_hash_map(nRanges);
  MagmaFormatC_SetupHasher(user_data->fmt, nDistKeyIndex, distKeyIndex, nRanges,
                           range_to_rg_map, nRg, rgId, rgUrl, jumpHashMap,
                           JUMP_HASH_MAP_LENGTH);
  MagmaFormatCatchedError *e = MagmaFormatGetErrorMagmaFormatC(user_data->fmt);
  if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    elog(ERROR, "magma_insert: failed to begin insert: %s (%d)", e->errMessage,
         e->errCode);
  }

  eid->ext_ps_user_data = (void *)user_data;

  /* 3. Begin insert with the formatter */
  MagmaFormatBeginInsertMagmaFormatC(user_data->fmt, snapshot);

  MagmaFormatCatchedError *e1 = MagmaFormatGetErrorMagmaFormatC(user_data->fmt);
  if (e1->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    elog(ERROR, "magma_insert: failed to begin insert: %s (%d)", e1->errMessage,
         e1->errCode);
  }

  /* 4. Save the result */
  ps->ps_ext_insert_desc = eid;

  PG_RETURN_POINTER(eid);
}

/*
 * Oid
 * magma_insert(ExternalInsertDesc extInsertDesc,
 *           TupleTableSlot *tupTableSlot)
 */
Datum magma_insert(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  ExternalInsertDesc eid = ps->ps_ext_insert_desc;
  TupleTableSlot *tts = ps->ps_tuple_table_slot;

  MagmaFormatUserData *user_data =
      (MagmaFormatUserData *)(eid->ext_ps_user_data);

  user_data->colValues = slot_get_values(tts);
  user_data->colIsNulls = slot_get_isnull(tts);

  static bool DUMMY_BOOL = true;
  static int8_t DUMMY_INT8 = 0;
  static int16_t DUMMY_INT16 = 0;
  static int32_t DUMMY_INT32 = 0;
  static int64_t DUMMY_INT64 = 0;
  static float DUMMY_FLOAT = 0.0;
  static double DUMMY_DOUBLE = 0.0;
  static char DUMMY_TEXT[1] = "";
  static int32_t DUMMY_DATE = 0;
  static int64_t DUMMY_TIME = 0;
  static TimestampType DUMMY_TIMESTAMP = {0, 0};

  TupleDesc tupdesc = tts->tts_tupleDescriptor;
  user_data->numberOfColumns = tupdesc->natts;

  MemoryContext per_row_context = eid->ext_pstate->rowcontext;
  MemoryContext old_context = MemoryContextSwitchTo(per_row_context);

  /* Get column values */
  for (int i = 0; i < user_data->numberOfColumns; ++i) {
    int dataType = (int)(tupdesc->attrs[i]->atttypid);

    user_data->colRawValues[i] = NULL;

    if (user_data->colIsNulls[i]) {
      if (dataType == HAWQ_TYPE_CHAR) {
        user_data->colRawValues[i] = (char *)(&DUMMY_INT8);
      } else if (dataType == HAWQ_TYPE_INT2) {
        user_data->colRawValues[i] = (char *)(&DUMMY_INT16);
      } else if (dataType == HAWQ_TYPE_INT4) {
        user_data->colRawValues[i] = (char *)(&DUMMY_INT32);
      } else if (dataType == HAWQ_TYPE_INT8) {
        user_data->colRawValues[i] = (char *)(&DUMMY_INT64);
      } else if (dataType == HAWQ_TYPE_TEXT || dataType == HAWQ_TYPE_BYTE ||
                 dataType == HAWQ_TYPE_BPCHAR ||
                 dataType == HAWQ_TYPE_VARCHAR ||
                 dataType == HAWQ_TYPE_NUMERIC ||
                 dataType == HAWQ_TYPE_JSON ||
                 dataType == HAWQ_TYPE_JSONB) {
        user_data->colRawValues[i] = (char *)(DUMMY_TEXT);
      } else if (dataType == HAWQ_TYPE_FLOAT4) {
        user_data->colRawValues[i] = (char *)(&DUMMY_FLOAT);
      } else if (dataType == HAWQ_TYPE_FLOAT8) {
        user_data->colRawValues[i] = (char *)(&DUMMY_DOUBLE);
      } else if (dataType == HAWQ_TYPE_BOOL) {
        user_data->colRawValues[i] = (char *)(&DUMMY_BOOL);
      } else if (dataType == HAWQ_TYPE_DATE) {
        user_data->colRawValues[i] = (char *)(&DUMMY_DATE);
      } else if (dataType == HAWQ_TYPE_TIME) {
        user_data->colRawValues[i] = (char *)(&DUMMY_TIME);
      } else if (dataType == HAWQ_TYPE_TIMESTAMP) {
        user_data->colRawValues[i] = (char *)(&DUMMY_TIMESTAMP);
      }
      // do not adjust the rowtype/basetype to any other location
      else if (STRUCTEXID == user_data->colDatatypes[i] ||
               IOBASETYPEID == user_data->colDatatypes[i]) {
        user_data->colRawValues[i] = (char *)(DUMMY_TEXT);
      } else if (dataType == HAWQ_TYPE_INVALID) {
        elog(ERROR, "HAWQ data type %s is invalid", TypeNameToString(makeTypeNameFromOid(dataType, -1)));
      } else {
        elog(ERROR, "HAWQ data type %s is not supported yet", TypeNameToString(makeTypeNameFromOid(dataType, -1)));
      }

      continue;
    }

    if (dataType == HAWQ_TYPE_CHAR || dataType == HAWQ_TYPE_INT2 ||
        dataType == HAWQ_TYPE_INT4 || dataType == HAWQ_TYPE_INT8 ||
        dataType == HAWQ_TYPE_FLOAT4 || dataType == HAWQ_TYPE_FLOAT8 ||
        dataType == HAWQ_TYPE_BOOL || dataType == HAWQ_TYPE_TIME) {
      user_data->colRawValues[i] = (char *)(&(user_data->colValues[i]));
    } else if (dataType == HAWQ_TYPE_DATE) {
      int *date = (int *)(&(user_data->colValues[i]));
      *date += POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE;
      user_data->colRawValues[i] = (char *)(&(user_data->colValues[i]));
    } else if (dataType == HAWQ_TYPE_TIMESTAMP) {
      int64_t *timestamp = (int64_t *) (&(user_data->colValues[i]));
      user_data->colTimestamp[i].second = *timestamp / 1000000
          + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * 60 * 60 * 24;
      user_data->colTimestamp[i].nanosecond = *timestamp % 1000000 * 1000;
      int64_t days = user_data->colTimestamp[i].second / 60 / 60 / 24;
      if (user_data->colTimestamp[i].nanosecond < 0 &&
          (days > POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE || days < 0))
        user_data->colTimestamp[i].nanosecond += 1000000000;
      if(user_data->colTimestamp[i].second < 0 && user_data->colTimestamp[i].nanosecond)
        user_data->colTimestamp[i].second -= 1;
      user_data->colRawValues[i] = (char *) (&(user_data->colTimestamp[i]));
    } else if (dataType == HAWQ_TYPE_NUMERIC) {
      Numeric num = DatumGetNumeric(user_data->colValues[i]);
      user_data->colRawValues[i] = num;
      if (NUMERIC_IS_NAN(num)) // XXX(chiyang): problematic legacy NaN
      {
        user_data->colIsNulls[i] = true;
      }
    } else if (dataType == HAWQ_TYPE_TEXT || dataType == HAWQ_TYPE_BYTE ||
               dataType == HAWQ_TYPE_BPCHAR || dataType == HAWQ_TYPE_VARCHAR
  || dataType == HAWQ_TYPE_JSON || dataType == HAWQ_TYPE_JSONB) {
      struct varlena *varlen =
          (struct varlena *)DatumGetPointer(user_data->colValues[i]);
      user_data->colValLength[i] = VARSIZE_ANY_EXHDR(varlen);
      user_data->colRawValues[i] = VARDATA_ANY(varlen);
    } else if (STRUCTEXID == user_data->colDatatypes[i]) {
      int32_t len = VARSIZE(user_data->colValues[i]);
      if (len <= 0) {
        elog(ERROR, "HAWQ base type(udt) %s should not be less than 0",
             TypeNameToString(makeTypeNameFromOid(dataType, -1)));
      }

      char *pVal = DatumGetPointer(user_data->colValues[i]);
      user_data->colRawValues[i] = palloc0(VARHDRSZ + len);

      //  set value : the first 4 byte is length, than the raw value
      // SET_VARSIZE(  (struct varlena * )user_data->colRawValues[i], len);
      *((int32 *)(user_data->colRawValues[i])) = len;
      memcpy(user_data->colRawValues[i] + VARHDRSZ, pVal, len);

    } else if (IOBASETYPEID == user_data->colDatatypes[i]) {
      //  get the length of basetype
      bool passbyval = tupdesc->attrs[i]->attbyval;
      int32_t orilen = (int32_t)(tupdesc->attrs[i]->attlen);
      int32_t len =
          get_typlen_fast(dataType, passbyval, orilen, user_data->colValues[i]);

      if (1 > len) {  //  invalid length
        elog(ERROR,
             "HAWQ composite type(udt) %s got an invalid length:%d",
             TypeNameToString(makeTypeNameFromOid(dataType, -1)), len);
      }

      if (passbyval) {
        //  value store in Datum directly
        char *val = &(user_data->colValues[i]);
        user_data->colRawValues[i] = palloc0(VARHDRSZ + len);
        *((int32 *)(user_data->colRawValues[i])) = len;
        memcpy(user_data->colRawValues[i] + VARHDRSZ, val, len);
      } else {
        //  value stored by pointer in Datum
        char *val = DatumGetPointer(user_data->colValues[i]);
        user_data->colRawValues[i] = palloc0(VARHDRSZ + len);

        //  set value : the first 4 byte is length, than the raw value
        // SET_VARSIZE(  (struct varlena * )user_data->colRawValues[i], len);
        *((int32 *)(user_data->colRawValues[i])) = len;
        memcpy(user_data->colRawValues[i] + VARHDRSZ, val, len);
      }
    } else if (dataType == HAWQ_TYPE_INVALID) {
      elog(ERROR, "HAWQ data type %s is invalid", TypeNameToString(makeTypeNameFromOid(dataType, -1)));
    } else {
      elog(ERROR, "HAWQ data type %s is not supported yet", TypeNameToString(makeTypeNameFromOid(dataType, -1)));
    }
  }

  /* Pass to formatter to output */
  MagmaFormatInsertMagmaFormatC(user_data->fmt, user_data->colRawValues,
                                user_data->colValLength, user_data->colIsNulls);

  MagmaFormatCatchedError *e = MagmaFormatGetErrorMagmaFormatC(user_data->fmt);
  if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    elog(ERROR, "magma_insert: failed to insert: %s(%d)", e->errMessage,
         e->errCode);
  }

  ps->ps_tuple_oid = InvalidOid;

  MemoryContextReset(per_row_context);
  MemoryContextSwitchTo(old_context);

  PG_RETURN_OID(InvalidOid);
}

/*
 * void
 * magma_insert_finish(ExternalInsertDesc extInsertDesc)
 */
Datum magma_insert_finish(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  ExternalInsertDesc eid = ps->ps_ext_insert_desc;

  MagmaFormatUserData *user_data =
      (MagmaFormatUserData *)(eid->ext_ps_user_data);

  MagmaFormatEndInsertMagmaFormatC(user_data->fmt);

  MagmaFormatCatchedError *e = MagmaFormatGetErrorMagmaFormatC(user_data->fmt);
  if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    elog(ERROR, "magma_insert: failed to end insert: %s(%d)", e->errMessage,
         e->errCode);
  }

  MagmaFormatFreeMagmaFormatC(&(user_data->fmt));

  for (int i = 0; i < user_data->numberOfColumns; ++i) {
    pfree(user_data->colNames[i]);
  }
  pfree(user_data->colNames);
  pfree(user_data->colDatatypes);
  pfree(user_data->colRawValues);
  pfree(user_data->colDatatypeMods);
  pfree(user_data->colTimestamp);
  pfree(user_data);

  if (eid->ext_formatter_data) pfree(eid->ext_formatter_data);

  if (eid->ext_formatter_name) pfree(eid->ext_formatter_name);

  if (eid->ext_pstate != NULL && eid->ext_pstate->rowcontext != NULL) {
    /*
     * delete the row context
     */
    MemoryContextDelete(eid->ext_pstate->rowcontext);
    eid->ext_pstate->rowcontext = NULL;
  }

  pfree(eid);

  PG_RETURN_VOID();
}

/*
 * void
 * magma_transaction(PlugStorageTransaction transaction)
 */
Datum magma_transaction(PG_FUNCTION_ARGS) {
  checkOushuDbExtensiveFunctionSupport(__func__);
  elog(DEBUG3, "magma_transaction begin");
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  PlugStorageTransaction pst = ps->ps_transaction;

  PlugStorageTransactionCommand txn_command = pst->pst_transaction_command;

  MagmaClientC *client = create_magma_client_instance();
  if (client == NULL) {
    elog(ERROR, "failed to connect to magma service");
  }

  MagmaClientC_SetupSnapshot(client, pst->pst_transaction_snapshot);

  switch (txn_command) {
    case PS_TXN_CMD_START_TRANSACTION: {
      pst->pst_transaction_state = MagmaClientC_StartTransaction(client);
      pst->pst_transaction_snapshot = NULL;
      pst->pst_transaction_status = PS_TXN_STS_DEFAULT;
      pst->pst_transaction_id = InvalidTransactionId;
      pst->pst_transaction_snapshot = NULL;
      elog(DEBUG1, "magma_transaction: start transaction");
      magma_check_result(&client);
      break;
    }
    case PS_TXN_CMD_COMMIT_TRANSACTION:
      if (pst->pst_transaction_snapshot == NULL) {
        elog(DEBUG1, "magma_transaction: commit snapshot: NULL");
      } else {
        elog(DEBUG1,
             "magma_transaction: commit snapshot: (%llu, %u, %llu, %u)",
             pst->pst_transaction_snapshot->currentTransaction.txnId,
             pst->pst_transaction_snapshot->currentTransaction.txnStatus,
             pst->pst_transaction_snapshot->txnActions.txnActionStartOffset,
             pst->pst_transaction_snapshot->txnActions.txnActionSize);
      }

      MagmaClientC_CommitTransaction(client);
      magma_check_result(&client);
      break;
    case PS_TXN_CMD_ABORT_TRANSACTION:
      if (pst->pst_transaction_snapshot == NULL) {
        elog(DEBUG1, "magma_transaction: abort snapshot: NULL");
      } else {
        elog(DEBUG1,
             "magma_transaction: abort snapshot: (%llu, %u, %llu, %u)",
             pst->pst_transaction_snapshot->currentTransaction.txnId,
             pst->pst_transaction_snapshot->currentTransaction.txnStatus,
             pst->pst_transaction_snapshot->txnActions.txnActionStartOffset,
             pst->pst_transaction_snapshot->txnActions.txnActionSize);
      }

      if (pst->pst_transaction_status != PS_TXN_STS_DEFAULT &&
          pst->pst_transaction_id != InvalidTransactionId &&
          pst->pst_transaction_snapshot != NULL) {
        MagmaClientC_AbortTransaction(client, PlugStorageGetIsCleanupAbort());
        pst->pst_transaction_snapshot = NULL;
        pst->pst_transaction_id = InvalidTransactionId;
        pst->pst_transaction_status = PS_TXN_STS_DEFAULT;
        magma_check_result(&client);
      }
      break;
    case PS_TXN_CMD_GET_SNAPSHOT: {
      MagmaClientC_CleanupTableInfo(client);
      int i = 0;
      ListCell *lc;
      foreach (lc, ps->magma_talbe_full_names) {
        MagmaTableFullName* mtfn = lfirst(lc);
        MagmaClientC_AddTableInfo(client, mtfn->databaseName, mtfn->schemaName,
                                  mtfn->tableName, 0);
        ++i;
      }
      pst->pst_transaction_snapshot = MagmaClientC_GetSnapshot(client);
      if (pst->pst_transaction_snapshot == NULL) {
        pst->pst_transaction_status = PS_TXN_STS_DEFAULT;
        pst->pst_transaction_id = InvalidTransactionId;
        pst->pst_transaction_snapshot = NULL;
        elog(DEBUG1, "magma_transaction: get snapshot: NULL");
      } else {
        elog(DEBUG1, "magma_transaction: get snapshot: (%llu, %u, %llu, %u)",
             pst->pst_transaction_snapshot->currentTransaction.txnId,
             pst->pst_transaction_snapshot->currentTransaction.txnStatus,
             pst->pst_transaction_snapshot->txnActions.txnActionStartOffset,
             pst->pst_transaction_snapshot->txnActions.txnActionSize);
      }
      magma_check_result(&client);
      break;
    }
    case PS_TXN_CMD_GET_TRANSACTIONID: {
      MagmaClientC_CleanupTableInfo(client);
      int i = 0;
      ListCell *lc;
      foreach (lc, ps->magma_talbe_full_names) {
        MagmaTableFullName* mtfn = lfirst(lc);
        MagmaClientC_AddTableInfo(client, mtfn->databaseName, mtfn->schemaName,
                                  mtfn->tableName, 0);
        ++i;
      }
      pst->pst_transaction_state = MagmaClientC_GetTransctionId(client);
      pst->pst_transaction_snapshot = MagmaClientC_GetSnapshot(client);
      if (pst->pst_transaction_snapshot == NULL) {
        pst->pst_transaction_status = PS_TXN_STS_DEFAULT;
        pst->pst_transaction_id = InvalidTransactionId;
        pst->pst_transaction_snapshot = NULL;
        elog(DEBUG1, "magma_transaction: get transaction state: NULL");
      } else {
        elog(DEBUG1, "magma_transaction: get transaction state: (%llu, %u, %llu, %u)",
             pst->pst_transaction_snapshot->currentTransaction.txnId,
             pst->pst_transaction_snapshot->currentTransaction.txnStatus,
             pst->pst_transaction_snapshot->txnActions.txnActionStartOffset,
             pst->pst_transaction_snapshot->txnActions.txnActionSize);
      }
      magma_check_result(&client);
      break;
    }
    default:
      elog(ERROR, "Transaction command for magma is invalid %d", txn_command);
      break;
  }


  PG_RETURN_VOID();
}

static void get_magma_category_info(char *fmtoptstr, bool *isexternal) {
  // do nothing now.
  char *fmt_name = NULL;
  List *l = magma_parse_format_string(fmtoptstr, &fmt_name);

  ListCell *opt;
  foreach (opt, l) {
    DefElem *defel = (DefElem *)lfirst(opt);
    char *key = defel->defname;
    bool need_free_value = false;
    char *val = (char *)defGetString(defel, &need_free_value);

    /* check category */
    if (strncasecmp(key, "category", strlen("category")) == 0) {
      if (strncasecmp(val, "internal", strlen("internal")) == 0) {
        isexternal = false;
      }
      if (strncasecmp(val, "external", strlen("external")) == 0) {
        isexternal = true;
      }
    }
  }
}

static FmgrInfo *get_magma_function(char *formatter_name, char *function_name) {
  Assert(formatter_name);
  Assert(function_name);

  Oid procOid = InvalidOid;
  FmgrInfo *procInfo = NULL;

  procOid = LookupPlugStorageValidatorFunc(formatter_name, function_name);

  if (OidIsValid(procOid)) {
    procInfo = (FmgrInfo *)palloc0(sizeof(FmgrInfo));
    fmgr_info(procOid, procInfo);
  } else {
    elog(ERROR, "%s_%s function was not found for pluggable storage",
         formatter_name, function_name);
  }

  return procInfo;
}

static void get_magma_scan_functions(char *formatter_name,
                                     FileScanDesc file_scan_desc) {
  file_scan_desc->fs_ps_scan_funcs.beginscan =
      get_magma_function(formatter_name, "beginscan");

  file_scan_desc->fs_ps_scan_funcs.getnext_init =
      get_magma_function(formatter_name, "getnext_init");

  file_scan_desc->fs_ps_scan_funcs.getnext =
      get_magma_function(formatter_name, "getnext");

  file_scan_desc->fs_ps_scan_funcs.rescan =
      get_magma_function(formatter_name, "rescan");

  file_scan_desc->fs_ps_scan_funcs.endscan =
      get_magma_function(formatter_name, "endscan");

  file_scan_desc->fs_ps_scan_funcs.stopscan =
      get_magma_function(formatter_name, "stopscan");
}

static void get_magma_insert_functions(char *formatter_name,
                                       ExternalInsertDesc ext_insert_desc) {
  ext_insert_desc->ext_ps_insert_funcs.insert_init =
      get_magma_function(formatter_name, "insert_init");

  ext_insert_desc->ext_ps_insert_funcs.insert =
      get_magma_function(formatter_name, "insert");

  ext_insert_desc->ext_ps_insert_funcs.insert_finish =
      get_magma_function(formatter_name, "insert_finish");
}

static void get_magma_delete_functions(char *formatter_name,
                                       ExternalInsertDesc ext_delete_desc) {
  ext_delete_desc->ext_ps_delete_funcs.begindeletes =
      get_magma_function(formatter_name, "begindelete");

  ext_delete_desc->ext_ps_delete_funcs.deletes =
      get_magma_function(formatter_name, "delete");

  ext_delete_desc->ext_ps_delete_funcs.enddeletes =
      get_magma_function(formatter_name, "enddelete");
}

static void get_magma_update_functions(char *formatter_name,
                                       ExternalInsertDesc ext_update_desc) {
  ext_update_desc->ext_ps_update_funcs.beginupdates =
      get_magma_function(formatter_name, "beginupdate");

  ext_update_desc->ext_ps_update_funcs.updates =
      get_magma_function(formatter_name, "update");

  ext_update_desc->ext_ps_update_funcs.endupdates =
      get_magma_function(formatter_name, "endupdate");
}

static void build_options_in_json(char *serializeSchema, int serializeSchemaLen,
                                  List *fmt_opts_defelem, int encoding, int rangeNum,
                                  char *formatterName, char **json_str) {
  struct json_object *opt_json_object = json_object_new_object();

  /* add format options for the formatter */
  char *key_str = NULL;
  char *val_str = NULL;
  // const char *whitespace = " \t\n\r";

  int nargs = list_length(fmt_opts_defelem);
  for (int i = 0; i < nargs; ++i) {
    key_str = ((DefElem *)(list_nth(fmt_opts_defelem, i)))->defname;
    val_str =
        ((Value *)((DefElem *)(list_nth(fmt_opts_defelem, i)))->arg)->val.str;

    json_object_object_add(opt_json_object, key_str,
                           json_object_new_string(val_str));
  }

  /* add encoding option for orc */
  if (json_object_object_get(opt_json_object, "encoding") == NULL) {
    const char *encodingStr = pg_encoding_to_char(encoding);
    char *encodingStrLower = str_tolower(encodingStr, strlen(encodingStr));

    json_object_object_add(opt_json_object, "encoding",
                           json_object_new_string(encodingStrLower));
    if (encodingStrLower) pfree(encodingStrLower);
  }

  /* add magma_range_num option for magma */
  if (json_object_object_get(opt_json_object, "magma_range_num") == NULL) {

    json_object_object_add(opt_json_object, "magma_range_num",
                           json_object_new_int64(rangeNum));
  }

  /* add magma_serialized_schema option for magma */
  if (json_object_object_get(opt_json_object, "serialized_schema") == NULL) {
    json_object_object_add(
        opt_json_object, "serialized_schema",
        json_object_new_string_len(serializeSchema, serializeSchemaLen));
  }

  /* add magma_format_type option for magma */
  if (json_object_object_get(opt_json_object, "magma_format_type") == NULL) {
    char *magma_type = NULL;
    if (formatterName != NULL &&
        (strncasecmp(formatterName, "magmatp", strlen("magmatp")) == 0)) {
      magma_type = "0";
    } else if (strncasecmp(formatterName, "magmaap", strlen("magmaap")) ==
               0) {
      magma_type = "1";
    }
    json_object_object_add(opt_json_object, "magma_format_type",
                           json_object_new_string(magma_type));
  }

  *json_str = NULL;
  if (opt_json_object != NULL) {
    const char *str = json_object_to_json_string(opt_json_object);
    *json_str = (char *)palloc0(strlen(str) + 1);
    strcpy(*json_str, str);
    json_object_put(opt_json_object);

    elog(DEBUG3, "formatter options are %s", *json_str);
  }
}

static MagmaFormatC *create_magma_formatter_instance(List *fmt_opts_defelem,
                                                     char *serializeSchema,
                                                     int serializeSchemaLen,
                                                     int fmt_encoding,
                                                     char *formatterName,
                                                     int rangeNum) {
  char *fmt_opts_str = NULL;

  build_options_in_json(serializeSchema, serializeSchemaLen, fmt_opts_defelem,
                        fmt_encoding, rangeNum, formatterName, &fmt_opts_str);

  MagmaFormatC *magma_format_c = MagmaFormatNewMagmaFormatC(fmt_opts_str);
  if (fmt_opts_str != NULL) {
    pfree(fmt_opts_str);
  }
  return magma_format_c;
}

static MagmaClientC *create_magma_client_instance() {
  if (magma_client_instance != NULL) {
    MagmaClientC_ResetMagmaClient4Reuse(&magma_client_instance);
    return magma_client_instance;
  }

  magma_client_instance = MagmaClientC_NewMagmaClient(magma_nodes_url);
  MagmaResult *result = MagmaClientC_GetResult(magma_client_instance);
  if (result->level == MAGMA_ERROR) {
    MagmaClientC_FreeMagmaClient(&magma_client_instance);
    elog(ERROR, "%s", result->message);
  }
  return magma_client_instance;
}

static void init_magma_format_user_data_for_read(
    TupleDesc tup_desc, MagmaFormatUserData *user_data) {
  user_data->numberOfColumns = tup_desc->natts;
  user_data->colNames = palloc0(sizeof(char *) * user_data->numberOfColumns);
  user_data->colDatatypes = palloc0(sizeof(int) * user_data->numberOfColumns);
  user_data->colDatatypeMods = palloc0(
      sizeof(int64_t) * user_data->numberOfColumns);
  user_data->colValues = palloc0(sizeof(Datum) * user_data->numberOfColumns);
  user_data->colRawValues = palloc0(
      sizeof(char *) * user_data->numberOfColumns);
  user_data->colValLength = palloc0(
      sizeof(uint64_t) * user_data->numberOfColumns);
  user_data->colIsNulls = palloc0(sizeof(bool) * user_data->numberOfColumns);

  for (int i = 0; i < user_data->numberOfColumns; i++) {
      Form_pg_attribute attr = tup_desc->attrs[i];
      user_data->colNames[i] = pstrdup(attr->attname.data);
      user_data->colDatatypes[i] = map_hawq_type_to_magma_type(attr->atttypid, user_data->isMagmatp);
      user_data->colDatatypeMods[i] = attr->atttypmod;
      user_data->colRawTid = NULL;
      user_data->colValues[i] = NULL;
      user_data->colRawValues[i] = NULL;
      user_data->colValLength[i] = 0;
      user_data->colIsNulls[i] = false;
    }
}

static void init_magma_format_user_data_for_write(
    TupleDesc tup_desc, MagmaFormatUserData *user_data, Relation relation) {
  user_data->numberOfColumns = tup_desc->natts;
  user_data->colNames = palloc0(sizeof(char *) * user_data->numberOfColumns);
  user_data->colDatatypes = palloc0(sizeof(int) * user_data->numberOfColumns);
  user_data->colRawValues =
      palloc0(sizeof(char *) * user_data->numberOfColumns);
  user_data->colValLength =
      palloc0(sizeof(uint64_t) * user_data->numberOfColumns);
  user_data->colDatatypeMods =
      palloc0(sizeof(int64_t) * user_data->numberOfColumns);
  user_data->colIsNulls = palloc0(sizeof(bool) * user_data->numberOfColumns);
  user_data->colTimestamp =
      palloc0(sizeof(TimestampType) * user_data->numberOfColumns);
  for (int i = 0; i < user_data->numberOfColumns; ++i) {
    Form_pg_attribute attr = tup_desc->attrs[i];
    user_data->colNames[i] = pstrdup(attr->attname.data);
    user_data->colDatatypes[i] = map_hawq_type_to_magma_type(attr->atttypid, user_data->isMagmatp);
    user_data->colDatatypeMods[i] = relation->rd_att->attrs[i]->atttypmod;
    user_data->colIsNulls[i] = !(relation->rd_att->attrs[i]->attnotnull);
  }
}

static void build_magma_tuple_descrition_for_read(
    Plan *plan, Relation relation, MagmaFormatUserData *user_data, bool skipTid) {
  user_data->colToReads = palloc0(sizeof(bool) * user_data->numberOfColumns);

  for (int i = 0; i < user_data->numberOfColumns; ++i)
  {
    user_data->colToReads[i] = plan ? false : true;

    /* 64 is the name type length */
    user_data->colNames[i] = palloc(sizeof(char) * 64);

    strcpy(user_data->colNames[i],
        relation->rd_att->attrs[i]->attname.data);

    int data_type = (int) (relation->rd_att->attrs[i]->atttypid);
    user_data->colDatatypes[i] = map_hawq_type_to_common_plan(data_type);
    user_data->colDatatypeMods[i] = relation->rd_att->attrs[i]->atttypmod;
  }

  if (plan)
  {
    /* calculate columns to read for seqscan */
    GetNeededColumnsForScan((Node *) plan->targetlist,
        user_data->colToReads, user_data->numberOfColumns);

    GetNeededColumnsForScan((Node *) plan->qual, user_data->colToReads,
        user_data->numberOfColumns);

//    if (skipTid) {
//      int32_t i = 0;
//      for (; i < user_data->numberOfColumns; ++i) {
//        if (user_data->colToReads[i]) break;
//      }
//      if (i == user_data->numberOfColumns) user_data->colToReads[0] = true;
//    }
  }
}

static void magma_scan_error_callback(void *arg) {
  CopyState cstate = (CopyState)arg;

  errcontext("External table %s", cstate->cur_relname);
}

static List *magma_parse_format_string(char *fmtstr, char **fmtname) {
  char *token;
  const char *whitespace = " \t\n\r";
  char nonstd_backslash = 0;
  int encoding = GetDatabaseEncoding();

  token =
      magma_strtokx2(fmtstr, whitespace, NULL, NULL, 0, false, true, encoding);
  /* parse user custom options. take it as is. no validation needed */

  List *l = NIL;
  bool formatter_found = false;

  if (token) {
    char *key = token;
    char *val = NULL;
    StringInfoData key_modified;

    initStringInfo(&key_modified);

    while (key) {
      /* MPP-14467 - replace meta chars back to original */
      resetStringInfo(&key_modified);
      appendStringInfoString(&key_modified, key);
      replaceStringInfoString(&key_modified, "<gpx20>", " ");

      val = magma_strtokx2(NULL, whitespace, NULL, "'", nonstd_backslash, true,
                           true, encoding);
      if (val) {
        if (pg_strcasecmp(key, "formatter") == 0) {
          *fmtname = pstrdup(val);
          formatter_found = true;
        } else

          l = lappend(l, makeDefElem(pstrdup(key_modified.data),
                                     (Node *)makeString(pstrdup(val))));
      } else
        goto error;

      key = magma_strtokx2(NULL, whitespace, NULL, NULL, 0, false, false,
                           encoding);
    }
  }

  if (!formatter_found) {
    /*
     * If there is no formatter option specified, use format name. So
     * we don't report error here.
     */
  }

  return l;

error:
  if (token)
    ereport(ERROR,
            (errcode(ERRCODE_GP_INTERNAL_ERROR),
             errmsg("external table internal parse error at \"%s\"", token)));
  else
    ereport(ERROR, (errcode(ERRCODE_GP_INTERNAL_ERROR),
                    errmsg("external table internal parse error at end of "
                           "line")));
  return NIL;
}

static char *magma_strtokx2(const char *s, const char *whitespace,
                            const char *delim, const char *quote, char escape,
                            bool e_strings, bool del_quotes, int encoding) {
  static char *storage = NULL; /* store the local copy of the users string
                                * here */
  static char *string = NULL;  /* pointer into storage where to continue on
                                * next call */

  /* variously abused variables: */
  unsigned int offset;
  char *start;
  char *p;

  if (s) {
    // pfree(storage);

    /*
     * We may need extra space to insert delimiter nulls for adjacent
     * tokens.	2X the space is a gross overestimate, but it's unlikely
     * that this code will be used on huge strings anyway.
     */
    storage = palloc0(2 * strlen(s) + 1);
    strcpy(storage, s);
    string = storage;
  }

  if (!storage) return NULL;

  /* skip leading whitespace */
  offset = strspn(string, whitespace);
  start = &string[offset];

  /* end of string reached? */
  if (*start == '\0') {
    /* technically we don't need to free here, but we're nice */
    pfree(storage);
    storage = NULL;
    string = NULL;
    return NULL;
  }

  /* test if delimiter character */
  if (delim && strchr(delim, *start)) {
    /*
     * If not at end of string, we need to insert a null to terminate the
     * returned token.	We can just overwrite the next character if it
     * happens to be in the whitespace set ... otherwise move over the
     * rest of the string to make room.  (This is why we allocated extra
     * space above).
     */
    p = start + 1;
    if (*p != '\0') {
      if (!strchr(whitespace, *p)) memmove(p + 1, p, strlen(p) + 1);
      *p = '\0';
      string = p + 1;
    } else {
      /* at end of string, so no extra work */
      string = p;
    }

    return start;
  }

  /* check for E string */
  p = start;
  if (e_strings && (*p == 'E' || *p == 'e') && p[1] == '\'') {
    quote = "'";
    escape = '\\'; /* if std strings before, not any more */
    p++;
  }

  /* test if quoting character */
  if (quote && strchr(quote, *p)) {
    /* okay, we have a quoted token, now scan for the closer */
    char thisquote = *p++;

    /* MPP-6698 START
     * unfortunately, it is possible for an external table format
     * string to be represented in the catalog in a way which is
     * problematic to parse: when using a single quote as a QUOTE
     * or ESCAPE character the format string will show [quote '''].
     * since we do not want to change how this is stored at this point
     * (as it will affect previous versions of the software already
     * in production) the following code block will detect this scenario
     * where 3 quote characters follow each other, with no forth one.
     * in that case, we will skip the second one (the first is skipped
     * just above) and the last trailing quote will be skipped below.
     * the result will be the actual token (''') and after stripping
     * it due to del_quotes we'll end up with (').
     * very ugly, but will do the job...
     */
    char qt = quote[0];

    if (strlen(p) >= 3 && p[0] == qt && p[1] == qt && p[2] != qt) p++;
    /* MPP-6698 END */

    for (; *p; p += pg_encoding_mblen(encoding, p)) {
      if (*p == escape && p[1] != '\0')
        p++; /* process escaped anything */
      else if (*p == thisquote && p[1] == thisquote)
        p++; /* process doubled quote */
      else if (*p == thisquote) {
        p++; /* skip trailing quote */
        break;
      }
    }

    /*
     * If not at end of string, we need to insert a null to terminate the
     * returned token.	See notes above.
     */
    if (*p != '\0') {
      if (!strchr(whitespace, *p)) memmove(p + 1, p, strlen(p) + 1);
      *p = '\0';
      string = p + 1;
    } else {
      /* at end of string, so no extra work */
      string = p;
    }

    /* Clean up the token if caller wants that */
    if (del_quotes) magma_strip_quotes(start, thisquote, escape, encoding);

    return start;
  }

  /*
   * Otherwise no quoting character.	Scan till next whitespace, delimiter
   * or quote.  NB: at this point, *start is known not to be '\0',
   * whitespace, delim, or quote, so we will consume at least one character.
   */
  offset = strcspn(start, whitespace);

  if (delim) {
    unsigned int offset2 = strcspn(start, delim);

    if (offset > offset2) offset = offset2;
  }

  if (quote) {
    unsigned int offset2 = strcspn(start, quote);

    if (offset > offset2) offset = offset2;
  }

  p = start + offset;

  /*
   * If not at end of string, we need to insert a null to terminate the
   * returned token.	See notes above.
   */
  if (*p != '\0') {
    if (!strchr(whitespace, *p)) memmove(p + 1, p, strlen(p) + 1);
    *p = '\0';
    string = p + 1;
  } else {
    /* at end of string, so no extra work */
    string = p;
  }

  return start;
}

static void magma_strip_quotes(char *source, char quote, char escape,
                               int encoding) {
  char *src;
  char *dst;

  Assert(source);
  Assert(quote);

  src = dst = source;

  if (*src && *src == quote) src++; /* skip leading quote */

  while (*src) {
    char c = *src;
    int i;

    if (c == quote && src[1] == '\0')
      break; /* skip trailing quote */
    else if (c == quote && src[1] == quote)
      src++; /* process doubled quote */
    else if (c == escape && src[1] != '\0')
      src++; /* process escaped character */

    i = pg_encoding_mblen(encoding, src);
    while (i--) *dst++ = *src++;
  }

  *dst = '\0';
}

static void magma_check_result(MagmaClientC **client) {
  Assert(client != NULL && *client != NULL);

  MagmaResult *result = MagmaClientC_GetResult(*client);
  Assert(result != NULL);

  switch (result->level) {
    case 0:  // DEBUG
      elog(DEBUG3, "%s", result->message);
      break;

    case 1:  // LOG
      elog(LOG, "%s", result->message);
      break;

    case 2:  // INFO
      elog(INFO, "%s", result->message);
      break;

    case 3:  // NOTICE
      elog(NOTICE, "%s", result->message);
      break;

    case 4:  // WARNING
      elog(WARNING, "%s", result->message);
      break;

    case 5:  // ERROR
      elog(ERROR, "%s", result->message);
      break;

    default:
      elog(ERROR, "invalid error level %d", result->level);
      break;
  }
}

bool checkUnsupportedDataTypeMagma(int32_t hawqTypeID) {
  switch (hawqTypeID) {
    case HAWQ_TYPE_BOOL:
    case HAWQ_TYPE_INT2:
    case HAWQ_TYPE_INT4:
    case HAWQ_TYPE_INT8:
    case HAWQ_TYPE_FLOAT4:
    case HAWQ_TYPE_FLOAT8:
    case HAWQ_TYPE_CHAR:
    case HAWQ_TYPE_TEXT:
    case HAWQ_TYPE_BYTE:
    case HAWQ_TYPE_BPCHAR:
    case HAWQ_TYPE_VARCHAR:
    case HAWQ_TYPE_DATE:
    case HAWQ_TYPE_TIME:
    case HAWQ_TYPE_TIMESTAMP:
    case HAWQ_TYPE_INT2_ARRAY:
    case HAWQ_TYPE_INT4_ARRAY:
    case HAWQ_TYPE_INT8_ARRAY:
    case HAWQ_TYPE_FLOAT4_ARRAY:
    case HAWQ_TYPE_FLOAT8_ARRAY:
    case HAWQ_TYPE_NUMERIC:
    case HAWQ_TYPE_JSON:
    case HAWQ_TYPE_JSONB:
      return false;
    default:
      return true;
  }
}

/*
static int rangeCmp(const void *p1, const void *p2) {
  MagmaRange *r1 = (MagmaRange *)p1;
  MagmaRange *r2 = (MagmaRange *)p2;

  // replicaGroupid frist
  if (r1->replicaGroups[0].id < r2->replicaGroups[0].id) {
    return -1;
  }

  if (r1->replicaGroups[0].id > r2->replicaGroups[0].id) {
    return 1;
  }

  // inner Group second by the first three bits in rangeid
  if (r1->groupId < r2->groupId) {
    return -1;
  }

  if (r1->groupId > r2->groupId) {
    return 1;
  }

  return 0;
}
*/

int32_t map_hawq_type_to_magma_type(int32_t hawqTypeID, bool isMagmatp) {
  switch (hawqTypeID) {
    case HAWQ_TYPE_BOOL:
      return BOOLEANID;

    case HAWQ_TYPE_CHAR:
      return TINYINTID;

    case HAWQ_TYPE_INT2:
      return SMALLINTID;

    case HAWQ_TYPE_INT4:
      return INTID;

    case HAWQ_TYPE_INT8:
    case HAWQ_TYPE_TID:
      return BIGINTID;

    case HAWQ_TYPE_FLOAT4:
      return FLOATID;

    case HAWQ_TYPE_FLOAT8:
      return DOUBLEID;

    case HAWQ_TYPE_NUMERIC:
      return DECIMALNEWID;

    case HAWQ_TYPE_DATE:
      return DATEID;

    case HAWQ_TYPE_BPCHAR:
      return CHARID;

    case HAWQ_TYPE_VARCHAR:
      return VARCHARID;

    case HAWQ_TYPE_NAME:
    case HAWQ_TYPE_TEXT:
      return STRINGID;

    case HAWQ_TYPE_JSON:
      return JSONID;

    case HAWQ_TYPE_JSONB:
      return JSONBID;

    case HAWQ_TYPE_TIME:
      return TIMEID;

    case HAWQ_TYPE_TIMESTAMPTZ:
    case HAWQ_TYPE_TIMESTAMP:
    case HAWQ_TYPE_TIMETZ:
      return TIMESTAMPID;

    case HAWQ_TYPE_INTERVAL:
      return INTERVALID;

    case HAWQ_TYPE_MONEY:
    case HAWQ_TYPE_BIT:
    case HAWQ_TYPE_VARBIT:
    case HAWQ_TYPE_BYTE:
    case HAWQ_TYPE_XML:
    case HAWQ_TYPE_MACADDR:
    case HAWQ_TYPE_INET:
    case HAWQ_TYPE_CIDR:
      return BINARYID;

    case HAWQ_TYPE_INT2_ARRAY:
      return SMALLINTARRAYID;

    case HAWQ_TYPE_INT4_ARRAY:
      return INTARRAYID;

    case HAWQ_TYPE_INT8_ARRAY:
      return BIGINTARRAYID;

    case HAWQ_TYPE_FLOAT4_ARRAY:
      return FLOATARRAYID;

    case HAWQ_TYPE_FLOAT8_ARRAY:
      return DOUBLEARRAYID;

    case HAWQ_TYPE_TEXT_ARRAY:
      return STRINGARRAYID;

    case HAWQ_TYPE_BPCHAR_ARRAY:
      return BPCHARARRAYID;

    case HAWQ_TYPE_POINT:
    case HAWQ_TYPE_LSEG:
    case HAWQ_TYPE_PATH:
    case HAWQ_TYPE_BOX:
    case HAWQ_TYPE_POLYGON:
    case HAWQ_TYPE_CIRCLE:
    default:
      return type_is_rowtype(hawqTypeID)
                 ? (STRUCTEXID)
                 : (type_is_basetype(hawqTypeID) ? IOBASETYPEID
                                                 : INVALIDTYPEID);
  }
}
