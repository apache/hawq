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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "optimizer/newPlanner.h"
#include "catalog/catalog.h"

#include "access/aomd.h"
#include "access/fileam.h"
#include "access/filesplit.h"
#include "access/plugstorage.h"
#include "catalog/catquery.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_proc.h"
#include "cdb/cdbdatalocality.h"
#include "cdb/cdbfilesystemcredential.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbvars.h"
#include "commands/tablecmds.h"
#include "magma/cwrapper/magma-client-c.h"
#include "miscadmin.h"
#include "optimizer/planmain.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "storage/cwrapper/orc-format-c.h"
#include "storage/cwrapper/text-format-c.h"
#include "univplan/cwrapper/univplan-c.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/hawq_funcoid_mapping.h"
#include "utils/hawq_type_mapping.h"
#include "utils/lsyscache.h"
#include "utils/uri.h"

const char *new_executor_mode_on = "on";
const char *new_executor_mode_auto = "auto";
const char *new_executor_mode_off = "off";
char *new_executor_mode;
char *new_executor_enable_partitioned_hashagg_mode;
char *new_executor_enable_partitioned_hashjoin_mode;
char *new_executor_enable_external_sort_mode;
int new_executor_partitioned_hash_recursive_depth_limit;
int new_executor_ic_tcp_client_limit_per_query_per_segment;
int new_executor_external_sort_memory_limit_size_mb;

const char *new_executor_runtime_filter_mode;
const char *new_executor_runtime_filter_mode_local = "local";
const char *new_executor_runtime_filter_mode_global = "global";

int new_interconnect_type;
const char *show_new_interconnect_type() {
  switch (new_interconnect_type) {
    case INTERCONNECT_TYPE_UDP:
      return "UDP";
    case INTERCONNECT_TYPE_TCP:
    default:
      return "TCP";
  }
}

#define MAGMA_MAX_FILESPLIT_NUM 2048
#define MAGMA_MAX_FILESPLIT_NAME 128

static void do_convert_plantree_to_common_plan(Plan *node, int32_t pid,
                                               bool isLeft, bool isSubPlan,
                                               List *splits, Relation rel,
                                               bool insist,
                                               CommonPlanContext *ctx);
static bool do_convert_targetlist_to_common_plan(Plan *node,
                                                 CommonPlanContext *ctx);
static bool do_convert_quallist_to_common_plan(Plan *node,
                                               CommonPlanContext *ctx,
                                               bool isInsist);
static bool do_convert_indexqualorig_to_common_plan(Plan *node,
                                                    CommonPlanContext *ctx,
                                                    bool isInsist);
static bool do_convert_indexqual_to_common_plan(Plan *node,
                                                CommonPlanContext *ctx,
                                                bool isInsist);
static bool do_convert_initplan_to_common_plan(Plan *node,
                                               CommonPlanContext *ctx);
static bool do_convert_hashExpr_to_common_plan(Motion *node,
                                               CommonPlanContext *ctx);
static void do_convert_onetbl_to_common_plan(Oid relid, CommonPlanContext *ctx);
static void do_convert_magma_rangevseg_map_to_common_plan(
    CommonPlanContext *ctx);
static void do_convert_rangetbl_to_common_plan(List *rtable,
                                               CommonPlanContext *ctx);
static void do_convert_result_partitions_to_common_plan(
    PartitionNode *partitionNode, CommonPlanContext *ctx);
static void do_convert_token_map_to_common_plan(CommonPlanContext *ctx);
static void do_convert_snapshot_to_common_plan(CommonPlanContext *ctx);
static void do_convert_splits_list_to_common_plan(List *splits, Oid relOid,
                                                  CommonPlanContext *ctx);
static void do_convert_splits_to_common_plan(Scan *scan, Oid relOid,
                                             CommonPlanContext *ctx);
static bool do_convert_expr_to_common_plan(int32_t pid, Expr *expr,
                                           CommonPlanContext *ctx);
static bool do_convert_limit_to_common_plan(Limit *node,
                                            CommonPlanContext *ctx);
static bool do_convert_sort_limit_to_common_plan(Sort *node,
                                                 CommonPlanContext *ctx);
static bool do_convert_nestloop_joinqual_to_common_plan(NestLoop *node,
                                                        CommonPlanContext *ctx);
static bool do_convert_hashjoin_clause_to_common_plan(HashJoin *node,
                                                      CommonPlanContext *ctx);
static bool do_convert_mergejoin_clause_to_common_plan(MergeJoin *node,
                                                       CommonPlanContext *ctx);
static bool do_convert_result_qual_to_common_plan(Result *node,
                                                  CommonPlanContext *ctx);
static bool do_convert_subqueryscan_subplan_to_common_plan(
    SubqueryScan *node, CommonPlanContext *ctx);
static Expr *parentExprSwitchTo(Expr *parent, CommonPlanContext *ctx);
static void setDummyTListRef(CommonPlanContext *ctx);
static void unsetDummyTListRef(CommonPlanContext *ctx);
static void getFmtName(char *fmtOptsJson, char **fmtName);
static bool checkSupportedTableFormat(Node *node, CommonPlanContext *ctx);
static void checkUnsupportedStmt(PlannedStmt *stmt, CommonPlanContext *ctx);
static void checkReadStatsOnlyForAgg(Agg *node, CommonPlanContext *ctx);
static bool checkSupportedSubLinkType(SubLinkType sublinkType);
static bool checkInsertSupportTable(PlannedStmt *stmt);
static bool checkIsPrepareQuery(QueryDesc *queryDesc);

// @return format string whose life time goes along with current MemoryContext
static const char *buildInternalTableFormatOptionStringInJson(Relation rel) {
  AppendOnlyEntry *aoentry =
      GetAppendOnlyEntry(RelationGetRelid(rel), SnapshotNow);
  StringInfoData option;
  initStringInfo(&option);
  appendStringInfoChar(&option, '{');
  if (aoentry->compresstype)
    appendStringInfo(&option, "%s", aoentry->compresstype);
  appendStringInfoChar(&option, '}');
  return option.data;
}

#define DIRECT_LEFT_CHILD_VAR 0
#define INT64_MAX_LENGTH 20

static bool checkSupportedTableFormat(Node *node, CommonPlanContext *cxt) {
  if (NULL == node) return false;

  switch (nodeTag(node)) {
    case T_MagmaIndexScan:
    case T_MagmaIndexOnlyScan:
    case T_ExternalScan: {
      ExternalScan *n = (ExternalScan *)node;
      char fmtType = n->fmtType;
      char *fmtName = NULL;
      fmtName = getExtTblFormatterTypeInFmtOptsList(n->fmtOpts);

      if (fmtType == 'b') {
        if (!pg_strcasecmp(fmtName, ORCTYPE) ||
            !pg_strcasecmp(fmtName, TEXTTYPE) ||
            !pg_strcasecmp(fmtName, CSVTYPE)) {
          cxt->querySelect = true;
        }
        if (!pg_strncasecmp(fmtName, MAGMATYPE, sizeof(MAGMATYPE) - 1)) {
          cxt->querySelect = true;
          cxt->isMagma = true;
          cxt->magmaRelIndex = n->scan.scanrelid;
        }
      }

      if (fmtName) pfree(fmtName);
      break;
    }
    case T_AppendOnlyScan: {
      AppendOnlyScan *n = (AppendOnlyScan *)node;
      RangeTblEntry *rte = rt_fetch(n->scan.scanrelid, cxt->stmt->rtable);
      if (RELSTORAGE_ORC == get_rel_relstorage(rte->relid)) {
        cxt->querySelect = true;
        break;
      } else {
        cxt->convertible = false;
        return true;
      }
    }

    case T_ParquetScan: {
      cxt->convertible = false;
      return true;
    }
    default:
      break;
  }

  return plan_tree_walker(node, checkSupportedTableFormat, cxt);
}

void buildDefaultFormatterOptionsInJson(int encoding, char externalFmtType,
                                        struct json_object *optJsonObject) {
  if (json_object_object_get(optJsonObject, "delimiter") == NULL) {
    json_object_object_add(
        optJsonObject, "delimiter",
        json_object_new_string((externalFmtType == TextFormatTypeTXT) ? "\t"
                                                                      : ","));
  }

  if (json_object_object_get(optJsonObject, "null") == NULL) {
    json_object_object_add(
        optJsonObject, "null",
        json_object_new_string((externalFmtType == TextFormatTypeTXT) ? "\\N"
                                                                      : ""));
  }

  if (json_object_object_get(optJsonObject, "fill_missing_fields") == NULL) {
    json_object_object_add(optJsonObject, "fill_missing_fields",
                           json_object_new_boolean(0));
  } else {
    json_object_object_del(optJsonObject, "fill_missing_fields");
    json_object_object_add(optJsonObject, "fill_missing_fields",
                           json_object_new_boolean(1));
  }

  if (json_object_object_get(optJsonObject, "header") == NULL) {
    json_object_object_add(optJsonObject, "header", json_object_new_boolean(0));
  } else {
    json_object_object_del(optJsonObject, "header");
    json_object_object_add(optJsonObject, "header", json_object_new_boolean(1));
  }

  if (json_object_object_get(optJsonObject, "reject_limit") == NULL) {
    json_object_object_add(optJsonObject, "reject_limit",
                           json_object_new_int(0));
  } else {
    struct json_object *val =
        json_object_object_get(optJsonObject, "reject_limit");
    if (!json_object_is_type(val, json_type_int)) {
      const char *str = json_object_get_string(val);
      char *endPtr = NULL;
      int reject_limit = strtol(str, &endPtr, 10);
      if (*endPtr != '\0') {
        reject_limit = 0;
      }
      json_object_object_del(optJsonObject, "reject_limit");
      json_object_object_add(optJsonObject, "reject_limit",
                             json_object_new_int(reject_limit));
    }
  }

  if (json_object_object_get(optJsonObject, "err_table") == NULL) {
    json_object_object_add(optJsonObject, "err_table",
                           json_object_new_string(""));
  }

  if (json_object_object_get(optJsonObject, "newline") == NULL) {
    json_object_object_add(optJsonObject, "newline",
                           json_object_new_string("lf"));
  }

  if (json_object_object_get(optJsonObject, "encoding") == NULL) {
    const char *encodingStr = pg_encoding_to_char(encoding);
    char lowerCaseEncodingStr[64];
    strcpy(lowerCaseEncodingStr, encodingStr);
    for (char *p = lowerCaseEncodingStr; *p != '\0'; ++p) {
      *p = tolower(*p);
    }

    json_object_object_add(optJsonObject, "encoding",
                           json_object_new_string(lowerCaseEncodingStr));
  }

  if (externalFmtType == TextFormatTypeCSV &&
      json_object_object_get(optJsonObject, "quote") == NULL) {
    json_object_object_add(optJsonObject, "quote",
                           json_object_new_string("\""));
  }

  if (json_object_object_get(optJsonObject, "escape") == NULL) {
    if (externalFmtType == TextFormatTypeCSV) {
      /* Let escape follow quote's setting */
      struct json_object *val = json_object_object_get(optJsonObject, "quote");
      json_object_object_add(
          optJsonObject, "escape",
          json_object_new_string(json_object_get_string(val)));
    } else {
      json_object_object_add(optJsonObject, "escape",
                             json_object_new_string("\\"));
    }
  }

  if (json_object_object_get(optJsonObject, "force_quote") == NULL) {
    json_object_object_add(optJsonObject, "force_quote",
                           json_object_new_string(""));
  }

  /* This is for csv formatter only */
  if (externalFmtType == TextFormatTypeCSV &&
      json_object_object_get(optJsonObject, "force_notnull") == NULL) {
    json_object_object_add(optJsonObject, "force_notnull",
                           json_object_new_string(""));
  }
}

bool can_convert_common_plan(QueryDesc *queryDesc, CommonPlanContext *ctx) {
  PlannedStmt *stmt = queryDesc ? queryDesc->plannedstmt : NULL;
  // disable for cursor and bind message
  if (!queryDesc || queryDesc->extended_query) return false;

  // Disable new executor when too many TCP connection.
  // Here it considers only the TCP client number of the root plan, regardless
  // of its subplan and initplan. And GATHER MOTION are also considered as fully
  // connected layer, as that in REDISTRIBUTE and BROADCAST MOTION.
  int ic_tcp_client_num_per_segment = queryDesc->planner_segments *
                                      queryDesc->planner_segments *
                                      stmt->nMotionNodes / slaveHostNumber;
  if (ic_tcp_client_num_per_segment >
          new_executor_ic_tcp_client_limit_per_query_per_segment &&
      new_interconnect_type == INTERCONNECT_TYPE_TCP) {
    if (!queryDesc->newPlanForceAuto &&
        pg_strcasecmp(new_executor_mode, new_executor_mode_on) == 0)
      elog(
          ERROR,
          "Exceeding new executor TCP client limit per query per segment, %d > "
          "%d, please set new_executor=auto/off to fall back to old executor",
          ic_tcp_client_num_per_segment,
          new_executor_ic_tcp_client_limit_per_query_per_segment);
    return false;
  }

  Assert(stmt->planTree);
  planner_init_common_plan_context(stmt, ctx);

  // Fix issue 817
  if (checkIsPrepareQuery(queryDesc)) goto end;

  stmt->planner_segments = queryDesc->planner_segments;
  stmt->originNodeType = queryDesc->originNodeType;
  convert_to_common_plan(stmt, ctx);

  if (!ctx->convertible) goto end;

  convert_querydesc_to_common_plan(queryDesc, ctx);

  if (!ctx->convertible) goto end;

  return true;

end:
  planner_destroy_common_plan_context(ctx, !queryDesc->newPlanForceAuto);
  return false;
}

void convert_extscan_to_common_plan(Plan *node, List *splits, Relation rel,
                                    CommonPlanContext *ctx) {
  // only covert the plan node of extscan
  if (ctx->convertible)
    do_convert_plantree_to_common_plan(node, -1, true, false, splits, rel,
                                       false, ctx);
  if (ctx->convertible) {
    do_convert_onetbl_to_common_plan(rel->rd_id, ctx);
  }
}

void *convert_orcscan_indexqualorig_to_common_plan(Plan *node,
                                                   CommonPlanContext *ctx,
                                                   List *idxColumns) {
  planner_init_common_plan_context(NULL, ctx);
  ctx->idxColumns = idxColumns;
  univPlanSeqScanNewInstance(ctx->univplan, -1);
  do_convert_indexqualorig_to_common_plan(node, ctx, false);
  return univPlanGetQualList(ctx->univplan);
}

void *convert_orcscan_qual_to_common_plan(Plan *node, CommonPlanContext *ctx) {
  planner_init_common_plan_context(NULL, ctx);
  univPlanSeqScanNewInstance(ctx->univplan, -1);
  do_convert_quallist_to_common_plan(node, ctx, false);
  return univPlanGetQualList(ctx->univplan);
}

// cal the range num
void convert_rangenum_to_common_plan(PlannedStmt *stmt,
                                     CommonPlanContext *ctx) {
  ListCell *lc_split = NULL;
  foreach (lc_split, stmt->scantable_splits) {
    List *split = (List *)lfirst(lc_split);
    ctx->rangeNum += list_length(split);
  }
}

void convert_to_common_plan(PlannedStmt *stmt, CommonPlanContext *ctx) {
  checkUnsupportedStmt(stmt, ctx);

  if (ctx->convertible) checkSupportedTableFormat((Node *)stmt->planTree, ctx);

  if (ctx->convertible) {
    int32_t pid = -1;
    if (stmt->commandType == CMD_INSERT) {
      pid = univPlanInsertNewInstance(ctx->univplan, pid);
      univPlanInsertSetRelId(ctx->univplan,
                             list_nth_int(stmt->resultRelations, 0));
      /* deal with magma insert info */
      /* 1. get the reloid */
      int32_t index = list_nth_int(stmt->resultRelations, 0);
      RangeTblEntry *rte = (RangeTblEntry *)list_nth(stmt->rtable, index - 1);
      Oid oid = rte->relid;
      if (dataStoredInMagmaByOid(oid)) {
        ctx->isMagma = true;
        ctx->magmaRelIndex = index;
        /* 2. get magma splits */
        List *magma_splits =
            GetFileSplitsOfSegmentMagma(stmt->scantable_splits, oid);
        /* 3. prepare hash info */
        int32_t nDistKeyIndex = 0;
        int16_t *distKeyIndex = NULL;
        fetchDistributionPolicy(oid, &nDistKeyIndex, &distKeyIndex);
        /* the number of ranges is dynamic for magma table */
        int32_t nRanges = 0;
        ListCell *lc_split = NULL;
        foreach (lc_split, magma_splits) {
          List *split = (List *)lfirst(lc_split);
          nRanges += list_length(split);
        }
        uint32 range_to_rg_map[nRanges];
        List *rg = magma_build_range_to_rg_map(magma_splits, range_to_rg_map);
        int nRg = list_length(rg);
        uint16 *rgId = palloc0(sizeof(uint16) * nRg);
        char **rgUrl = palloc0(sizeof(char *) * nRg);
        magma_build_rg_to_url_map(magma_splits, rg, rgId, rgUrl);
        univPlanInsertSetHasher(ctx->univplan, nDistKeyIndex, distKeyIndex,
                                nRanges, range_to_rg_map, nRg, rgId, rgUrl);
        pfree(rgId);
        pfree(rgUrl);
      }
      // For append-only internal table
      if (get_relation_storage_type(oid) == RELSTORAGE_ORC) {
        ListCell *lc;
        foreach (lc, stmt->result_segfileinfos) {
          ResultRelSegFileInfoMapNode *pRelSegFileInfoMapNode =
              (ResultRelSegFileInfoMapNode *)lfirst(lc);
          ListCell *lc;
          foreach (lc, pRelSegFileInfoMapNode->segfileinfos) {
            ResultRelSegFileInfo *pSegFileInfo = lfirst(lc);
            if (pSegFileInfo->numfiles == 0) {
              // detect mixed-up partition of external table
              ctx->convertible = false;
              return;
            }
            univPlanAddResultRelSegFileInfo(
                ctx->univplan, pRelSegFileInfoMapNode->relid,
                pSegFileInfo->segno, pSegFileInfo->eof[0],
                pSegFileInfo->uncompressed_eof[0]);
          }
        }
      }
      univPlanAddToPlanNode(ctx->univplan, true);
    }
    do_convert_plantree_to_common_plan(stmt->planTree, pid, true, false, NIL,
                                       NULL, true, ctx);
    do_convert_magma_rangevseg_map_to_common_plan(ctx);
  }

  ListCell *lc;
  foreach (lc, stmt->subplans) {
    Plan *subplan = (Plan *)lfirst(lc);
    if (ctx->convertible)
      do_convert_plantree_to_common_plan(subplan, -1, true, true, NIL, NULL,
                                         true, ctx);
  }
  if (ctx->convertible) do_convert_rangetbl_to_common_plan(stmt->rtable, ctx);
  if (ctx->convertible && stmt->result_partitions)
    do_convert_result_partitions_to_common_plan(stmt->result_partitions, ctx);
  if (ctx->convertible && enable_secure_filesystem)
    do_convert_token_map_to_common_plan(ctx);
  if (ctx->convertible && ctx->isMagma) do_convert_snapshot_to_common_plan(ctx);
}

void planner_init_common_plan_context(PlannedStmt *stmt,
                                      CommonPlanContext *ctx) {
  ctx->univplan = univPlanNewInstance();
  ctx->convertible =
      pg_strcasecmp(new_executor_mode, new_executor_mode_off) != 0 ? true
                                                                   : false;
  ctx->base.node = (Node *)stmt;
  ctx->querySelect = false;
  ctx->isMagma = false;
  ctx->stmt = stmt;
  ctx->setDummyTListRef = false;
  ctx->scanReadStatsOnly = false;
  ctx->parent = NULL;
  ctx->exprBufStack = NIL;
  ctx->rangeNum = 0;
  ctx->isConvertingIndexQual = false;
  ctx->idxColumns = NIL;
}

void planner_destroy_common_plan_context(CommonPlanContext *ctx, bool enforce) {
  bool succeed = ctx->convertible;
  univPlanFreeInstance(&ctx->univplan);
  // only enforce query statement
  if (enforce && ctx->querySelect && !succeed &&
      pg_strcasecmp(new_executor_mode, new_executor_mode_on) == 0)
    elog(ERROR,
         "New executor not supported yet, please set new_executor=auto/off to "
         "fall back to old executor");
}

void get_all_stageno_from_plantree(Plan *node, int32_t *stageNo,
                                   int32_t *stageNum, bool *isInitPlan) {
  if (node == NULL) return;

  switch (nodeTag(node)) {
    case T_Motion: {
      Motion *m = (Motion *)node;
      stageNo[*stageNum] = m->motionID;
      (*stageNum)++;
      break;
    }
    case T_SubqueryScan: {
      SubqueryScan *subqueryscan = (SubqueryScan *)node;
      get_all_stageno_from_plantree(subqueryscan->subplan, stageNo, stageNum,
                                    isInitPlan);
    }
  }
  if (node->initPlan) {
    ListCell *lc;
    foreach (lc, node->initPlan) {
      SubPlan *initplan = (SubPlan *)lfirst(lc);
      isInitPlan[initplan->plan_id - 1] = true;
    }
  }
  get_all_stageno_from_plantree(node->lefttree, stageNo, stageNum, isInitPlan);
  get_all_stageno_from_plantree(node->righttree, stageNo, stageNum, isInitPlan);
}

void do_convert_plantree_to_common_plan(Plan *node, int32_t pid, bool isLeft,
                                        bool isSubPlan, List *splits,
                                        Relation rel, bool insist,
                                        CommonPlanContext *ctx) {
  if (node == NULL || !ctx->convertible) return;
  int32_t uid;

  switch (nodeTag(node)) {
    case T_Motion: {
      Motion *m = (Motion *)node;
      ConnectorType connType;
      if (m->motionType == MOTIONTYPE_HASH) {
        connType = UnivPlanShuffle;
      } else if (m->motionType == MOTIONTYPE_FIXED) {
        if (m->numOutputSegs == 0)
          connType = UnivPlanBroadcast;
        else
          connType = UnivPlanConverge;
      } else {
        goto end;
      }
      uid = univPlanConnectorNewInstance(ctx->univplan, pid);
      univPlanConnectorSetType(ctx->univplan, connType);
      univPlanConnectorSetStageNo(ctx->univplan, m->motionID);
      if (m->numSortCols > 0) {
        int32_t *mappingSortFuncId = palloc(m->numSortCols * sizeof(int32_t));
        int32_t *colIdx = palloc(m->numSortCols * sizeof(int32_t));
        for (int i = 0; i < m->numSortCols; i++) {
          mappingSortFuncId[i] =
              HAWQ_FUNCOID_MAPPING(get_opcode(m->sortOperators[i]));
          if (IS_HAWQ_MAPPING_FUNCID_INVALID(mappingSortFuncId[i])) goto end;
          colIdx[i] = m->sortColIdx[i];
        }
        univPlanConnectorSetColIdx(ctx->univplan, m->numSortCols, colIdx);
        univPlanConnectorSetSortFuncId(ctx->univplan, m->numSortCols,
                                       mappingSortFuncId);
        pfree(mappingSortFuncId);
        pfree(colIdx);
      }
      if (m->plan.directDispatch.isDirectDispatch) {
        List *contentIds = m->plan.directDispatch.contentIds;
        Assert(list_length(contentIds) == 1);
        univPlanConnectorSetDirectDispatchId(ctx->univplan,
                                             linitial_int(contentIds));
      }
      if (connType == UnivPlanShuffle) {
        if (!do_convert_hashExpr_to_common_plan(node, ctx)) goto end;
      }
      setDummyTListRef(ctx);
      if (!do_convert_targetlist_to_common_plan(node, ctx)) goto end;
      unsetDummyTListRef(ctx);
      if (!do_convert_quallist_to_common_plan(node, ctx, true)) goto end;
      if (!do_convert_initplan_to_common_plan(node, ctx)) goto end;
      break;
    }
    case T_MagmaIndexScan:
    case T_MagmaIndexOnlyScan:
    case T_ExternalScan: {
      ExternalScan *n = (ExternalScan *)node;
      // currently we support orc, text, csv and magma format
      char fmtType = n->fmtType;
      char *fmtName = NULL;
      bool magmaTable = false;
      fmtName = getExtTblFormatterTypeInFmtOptsList(n->fmtOpts);
      // For orc and magma table have different infos in scan node
      if (fmtName) {
        if (!pg_strncasecmp(fmtName, MAGMATYPE, sizeof(MAGMATYPE) - 1)) {
          magmaTable = true;
        }
      }

      if (fmtType != 'b' ||
          (pg_strncasecmp(fmtName, ORCTYPE, sizeof(ORCTYPE) - 1) &&
           pg_strncasecmp(fmtName, MAGMATYPE, sizeof(MAGMATYPE) - 1) &&
           pg_strncasecmp(fmtName, TEXTTYPE, sizeof(TEXTTYPE) - 1) &&
           pg_strncasecmp(fmtName, CSVTYPE, sizeof(CSVTYPE) - 1))) {
        if (fmtName) pfree(fmtName);
        goto end;
      }

      if (fmtName) pfree(fmtName);

      ListCell *lc;
      foreach (lc, n->uriList) {
        char *url = (char *)strVal(lfirst(lc));
        Uri *uri = ParseExternalTableUri(url);
        if (uri == NULL ||
            (uri->protocol != URI_HDFS && uri->protocol != URI_MAGMA)) {
          goto end;
        }
      }
      // calculate columns to read for seqscan
      int32_t numColsToRead = 0;
      Plan *plan = (Plan *)&((Scan *)node)->plan;
      Oid relOid;
      // scan magma table in old executor
      if (magmaTable && rel != NULL) {
        relOid = rel->rd_id;
      } else {
        // scan magma table in new executor
        relOid = getrelid(((Scan *)node)->scanrelid, ctx->stmt->rtable);
      }
      int32_t numCols = get_relnatts(relOid);
      bool *proj = (bool *)palloc0(sizeof(bool) * numCols);
      GetNeededColumnsForScan((Node *)plan->targetlist, proj, numCols);
      GetNeededColumnsForScan((Node *)plan->qual, proj, numCols);

      //      if (magmaTable) {
      //        int32_t i = 0;
      //        for (; i < numCols; ++i) {
      //          if (proj[i]) break;
      //        }
      //        if (i == numCols) proj[0] = true;
      //      }

      for (int32_t i = 0; i < numCols; i++) {
        if (proj[i]) numColsToRead++;
      }

      int32_t *columnsToRead = palloc(numColsToRead * sizeof(int32_t));
      int32_t index = 0;
      for (int32_t i = 0; i < numCols; i++) {
        if (proj[i]) columnsToRead[index++] = i + 1;
      }
      // This branch deal with magma table
      if (magmaTable) {
        uid = univPlanExtScanNewInstance(ctx->univplan, pid);
        if (node->type != T_ExternalScan) {
          univPlanExtScanSetIndex(ctx->univplan, true);
          switch (node->type) {
            case T_MagmaIndexScan:
              univPlanExtScanSetScanType(ctx->univplan, ExternalIndexScan);
              break;
            case T_MagmaIndexOnlyScan:
              univPlanExtScanSetScanType(ctx->univplan, ExternalIndexOnlyScan);
              break;
            default:
              elog(ERROR, "unknown external scan type.");
              break;
          }
          univPlanExtScanDirection(ctx->univplan,
                                   ((ExternalScan *)node)->indexorderdir);
          univPlanExtScanSetIndexName(ctx->univplan,
                                      ((ExternalScan *)node)->indexname);
          if (!do_convert_indexqual_to_common_plan(node, ctx, insist)) goto end;
        } else {
          univPlanExtScanSetScanType(ctx->univplan, NormalExternalScan);
        }
        univPlanExtScanSetRelId(ctx->univplan, ((Scan *)node)->scanrelid);
        univPlanExtScanSetReadStatsOnly(ctx->univplan, ctx->scanReadStatsOnly);
        if (columnsToRead)
          univPlanExtScanSetColumnsToRead(ctx->univplan, numColsToRead,
                                          columnsToRead);
        // TODO(xsheng) cannot convert some TARGETENTRY to univplan because
        // some expression types are not supported by universal plan.
        // e.g. (composite type field)
        //      update t_boxes set tp.len = (tp).len+1 where id = 2;
        // currently we can support convert composite type(e.g. tp) but we
        // cannot convert composite type field(e.g. tp.len)
        // we won't use target list in the plan post-processing, comment it now
        if (splits == NIL && ctx->stmt != NULL) {  // do it for new executor
          if (!do_convert_targetlist_to_common_plan(node, ctx)) goto end;
        }
        // if (!do_convert_targetlist_to_common_plan(node, ctx)) goto end;
        if (!do_convert_quallist_to_common_plan(node, ctx, insist)) goto end;
        if (!do_convert_initplan_to_common_plan(node, ctx)) goto end;
        if (splits != NULL && ctx->stmt == NULL) {
          // old executor, only convert magma external scan plan
          do_convert_splits_list_to_common_plan(splits, relOid, ctx);
        } else if (splits == NIL && ctx->stmt != NULL) {
          // new executor, convert whole plan
          do_convert_splits_to_common_plan((Scan *)node, relOid, ctx);
        }
      } else if (!magmaTable) {  // This branch deal with orc table
        uid = univPlanSeqScanNewInstance(ctx->univplan, pid);
        univPlanSeqScanSetRelId(ctx->univplan, ((Scan *)node)->scanrelid);
        univPlanSeqScanSetReadStatsOnly(ctx->univplan, ctx->scanReadStatsOnly);
        if (columnsToRead)
          univPlanSeqScanSetColumnsToRead(ctx->univplan, numColsToRead,
                                          columnsToRead);
        if (!do_convert_targetlist_to_common_plan(node, ctx)) goto end;
        if (!do_convert_quallist_to_common_plan(node, ctx, true)) goto end;
        if (!do_convert_initplan_to_common_plan(node, ctx)) goto end;
        do_convert_splits_to_common_plan((Scan *)node, relOid, ctx);
      } else {
        goto end;
      }
      break;
    }
    case T_AppendOnlyScan: {
      int32_t numColsToRead = 0;
      Plan *plan = (Plan *)&((Scan *)node)->plan;
      Oid relOid = getrelid(((Scan *)node)->scanrelid, ctx->stmt->rtable);
      int32_t numCols = get_relnatts(relOid);
      bool *proj = (bool *)palloc0(sizeof(bool) * numCols);
      GetNeededColumnsForScan((Node *)plan->targetlist, proj, numCols);
      GetNeededColumnsForScan((Node *)plan->qual, proj, numCols);

      for (int32_t i = 0; i < numCols; i++) {
        if (proj[i]) numColsToRead++;
      }

      int32_t *columnsToRead = palloc(numColsToRead * sizeof(int32_t));
      int32_t index = 0;
      for (int32_t i = 0; i < numCols; i++) {
        if (proj[i]) columnsToRead[index++] = i + 1;
      }
      uid = univPlanSeqScanNewInstance(ctx->univplan, pid);
      univPlanSeqScanSetRelId(ctx->univplan, ((Scan *)node)->scanrelid);
      univPlanSeqScanSetReadStatsOnly(ctx->univplan, ctx->scanReadStatsOnly);
      if (columnsToRead)
        univPlanSeqScanSetColumnsToRead(ctx->univplan, numColsToRead,
                                        columnsToRead);
      if (!do_convert_targetlist_to_common_plan(node, ctx)) goto end;
      if (!do_convert_quallist_to_common_plan(node, ctx, true)) goto end;
      if (!do_convert_initplan_to_common_plan(node, ctx)) goto end;
      do_convert_splits_to_common_plan((Scan *)node, relOid, ctx);
      break;
    }
    case T_Agg: {
      Agg *agg = (Agg *)node;

      uid = univPlanAggNewInstance(ctx->univplan, pid);
      int64_t numCols = agg->numCols;
      int32_t *grpColIdx = palloc(numCols * sizeof(int32_t));
      for (int i = 0; i < numCols; ++i) grpColIdx[i] = agg->grpColIdx[i];
      univPlanAggSetNumGroupsAndGroupColIndexes(ctx->univplan, agg->numGroups,
                                                numCols, grpColIdx);
      univPlanAggSetAggstrategy(ctx->univplan, agg->aggstrategy);
      univPlanAggSetRollup(ctx->univplan, agg->numNullCols, agg->inputGrouping,
                           agg->grouping, agg->rollupGSTimes,
                           agg->inputHasGrouping, agg->lastAgg, agg->streaming);
      pfree(grpColIdx);
      if (!do_convert_targetlist_to_common_plan(node, ctx)) goto end;
      if (!do_convert_quallist_to_common_plan(node, ctx, true)) goto end;
      if (!do_convert_initplan_to_common_plan(node, ctx)) goto end;
      if (!isSubPlan) checkReadStatsOnlyForAgg(agg, ctx);
      break;
    }
    case T_Sort: {
      Sort *sort = (Sort *)node;
      uid = univPlanSortNewInstance(ctx->univplan, pid);
      int32_t *mappingSortFuncId = palloc(sort->numCols * sizeof(int32_t));
      int32_t *colIdx = palloc(sort->numCols * sizeof(int32_t));
      for (int i = 0; i < sort->numCols; i++) {
        mappingSortFuncId[i] =
            HAWQ_FUNCOID_MAPPING(get_opcode(sort->sortOperators[i]));
        if (IS_HAWQ_MAPPING_FUNCID_INVALID(mappingSortFuncId[i])) goto end;
        colIdx[i] = sort->sortColIdx[i];
      }
      univPlanSortSetColIdx(ctx->univplan, sort->numCols, colIdx);
      univPlanSortSetSortFuncId(ctx->univplan, sort->numCols,
                                mappingSortFuncId);
      univPlanSortSetNoDuplicates(ctx->univplan, sort->noduplicates);
      pfree(mappingSortFuncId);
      pfree(colIdx);
      if (!do_convert_sort_limit_to_common_plan(sort, ctx)) goto end;
      if (!do_convert_targetlist_to_common_plan(node, ctx)) goto end;
      if (!do_convert_quallist_to_common_plan(node, ctx, true)) goto end;
      if (!do_convert_initplan_to_common_plan(node, ctx)) goto end;
      break;
    }
    case T_Limit: {
      Limit *limit = (Limit *)node;
      uid = univPlanLimitNewInstance(ctx->univplan, pid);
      if (!do_convert_limit_to_common_plan(limit, ctx)) goto end;
      if (!do_convert_targetlist_to_common_plan(node, ctx)) goto end;
      if (!do_convert_quallist_to_common_plan(node, ctx, true)) goto end;
      if (!do_convert_initplan_to_common_plan(node, ctx)) goto end;
      break;
    }
    case T_Append: {
      Append *append = (Append *)node;
      if (append->isTarget || append->plan.qual) goto end;
      uid = univPlanAppendNewInstance(ctx->univplan, pid);
      if (!do_convert_targetlist_to_common_plan(node, ctx)) goto end;
      if (!do_convert_quallist_to_common_plan(node, ctx, true)) goto end;
      if (!do_convert_initplan_to_common_plan(node, ctx)) goto end;
      break;
    }
    case T_NestLoop: {
      if (pg_strcasecmp(enable_alpha_newqe_str, "OFF") == 0) goto end;
      NestLoop *nl = (NestLoop *)node;
      if (nl->outernotreferencedbyinner || nl->shared_outer ||
          nl->singleton_outer)
        goto end;
      uid = univPlanNestLoopNewInstance(ctx->univplan, pid);
      if (!univPlanNestLoopSetType(ctx->univplan,
                                   (UnivPlanCJoinType)nl->join.jointype))
        goto end;
      if (!do_convert_targetlist_to_common_plan(node, ctx)) goto end;
      if (!do_convert_quallist_to_common_plan(node, ctx, true)) goto end;
      if (!do_convert_initplan_to_common_plan(node, ctx)) goto end;
      if (!do_convert_nestloop_joinqual_to_common_plan(nl, ctx)) goto end;
      break;
    }
    case T_HashJoin: {
      if (pg_strcasecmp(enable_alpha_newqe_str, "OFF") == 0) goto end;
      HashJoin *hj = (HashJoin *)node;
      uid = univPlanHashJoinNewInstance(ctx->univplan, pid);
      if (!univPlanHashJoinSetType(ctx->univplan,
                                   (UnivPlanCJoinType)hj->join.jointype))
        goto end;
      if (!do_convert_targetlist_to_common_plan(node, ctx)) goto end;
      if (!do_convert_quallist_to_common_plan(node, ctx, true)) goto end;
      if (!do_convert_initplan_to_common_plan(node, ctx)) goto end;
      if (!do_convert_hashjoin_clause_to_common_plan(hj, ctx)) goto end;
      break;
    }
    case T_Hash: {
      uid = univPlanHashNewInstance(ctx->univplan, pid);
      if (!do_convert_targetlist_to_common_plan(node, ctx)) goto end;
      if (!do_convert_quallist_to_common_plan(node, ctx, true)) goto end;
      if (!do_convert_initplan_to_common_plan(node, ctx)) goto end;
      break;
    }
    case T_MergeJoin: {
      MergeJoin *mj = (MergeJoin *)node;
      uid = univPlanMergeJoinNewInstance(ctx->univplan, pid);
      univPlanMergeJoinSetUniqueOuter(ctx->univplan, mj->unique_outer);
      if (!univPlanMergeJoinSetType(ctx->univplan,
                                    (UnivPlanCJoinType)mj->join.jointype))
        goto end;
      if (!do_convert_targetlist_to_common_plan(node, ctx)) goto end;
      if (!do_convert_quallist_to_common_plan(node, ctx, true)) goto end;
      if (!do_convert_initplan_to_common_plan(node, ctx)) goto end;
      if (!do_convert_mergejoin_clause_to_common_plan(mj, ctx)) goto end;
      break;
    }
    case T_Material: {
      Material *material = (Material *)node;
      uid = univPlanMaterialNewInstance(ctx->univplan, pid);
      if (!univPlanMaterialSetAttr(
              ctx->univplan, (UnivPlanCShareType)material->share_type,
              material->cdb_strict, material->share_id, material->driver_slice,
              material->nsharer, material->nsharer_xslice))
        goto end;
      if (!do_convert_targetlist_to_common_plan(node, ctx)) goto end;
      if (!do_convert_quallist_to_common_plan(node, ctx, true)) goto end;
      if (!do_convert_initplan_to_common_plan(node, ctx)) goto end;
      break;
    }
    case T_ShareInputScan: {
      ShareInputScan *shareInputScan = (ShareInputScan *)node;
      uid = univPlanShareInputScanNewInstance(ctx->univplan, pid);
      if (!univPlanShareInputScanSetAttr(
              ctx->univplan, (UnivPlanCShareType)shareInputScan->share_type,
              shareInputScan->share_id, shareInputScan->driver_slice))
        goto end;
      if (!do_convert_targetlist_to_common_plan(node, ctx)) goto end;
      if (!do_convert_quallist_to_common_plan(node, ctx, true)) goto end;
      if (!do_convert_initplan_to_common_plan(node, ctx)) goto end;
      break;
    }
    case T_Result: {
      Result *result = (Result *)node;
      if (result->hashFilter) goto end;
      uid = univPlanResultNewInstance(ctx->univplan, pid);
      if (!do_convert_targetlist_to_common_plan(node, ctx)) goto end;
      if (!do_convert_quallist_to_common_plan(node, ctx, true)) goto end;
      if (!do_convert_initplan_to_common_plan(node, ctx)) goto end;
      if (!do_convert_result_qual_to_common_plan(result, ctx)) goto end;
      break;
    }
    case T_SubqueryScan: {
      SubqueryScan *subqueryscan = (SubqueryScan *)node;
      uid = univPlanSubqueryScanNewInstance(ctx->univplan, pid);
      if (!do_convert_targetlist_to_common_plan(node, ctx)) goto end;
      if (!do_convert_quallist_to_common_plan(node, ctx, true)) goto end;
      if (!do_convert_initplan_to_common_plan(node, ctx)) goto end;
      if (!do_convert_subqueryscan_subplan_to_common_plan(subqueryscan, ctx))
        goto end;
      break;
    }
    case T_Unique: {
      Unique *uniq = (Unique *)node;
      uid = univPlanUniqueNewInstance(ctx->univplan, pid);
      int64_t numCols = uniq->numCols;
      int32_t *uniqColIdx = palloc(numCols * sizeof(int32_t));
      for (int i = 0; i < numCols; ++i) uniqColIdx[i] = uniq->uniqColIdx[i];
      univPlanUniqueSetNumGroupsAndUniqColIdxs(ctx->univplan, uniq->numCols,
                                               uniqColIdx);
      pfree(uniqColIdx);
      if (!do_convert_targetlist_to_common_plan(node, ctx)) goto end;
      if (!do_convert_quallist_to_common_plan(node, ctx, true)) goto end;
      if (!do_convert_initplan_to_common_plan(node, ctx)) goto end;
      break;
    }
    case T_SetOp: {
      SetOp *setop = (SetOp *)node;
      uid = univPlanSetOpNewInstance(ctx->univplan, pid);
      if (!univPlanSetOpSetAttr(ctx->univplan, setop->cmd, setop->numCols,
                                setop->dupColIdx, setop->flagColIdx))
        goto end;
      if (!do_convert_targetlist_to_common_plan(node, ctx)) goto end;
      if (!do_convert_quallist_to_common_plan(node, ctx, true)) goto end;
      if (!do_convert_initplan_to_common_plan(node, ctx)) goto end;
      break;
    }
    default:  // plannode not supported yet
      goto end;
  }

  univPlanSetPlanNodeInfo(ctx->univplan, node->plan_rows, node->plan_width,
                          node->operatorMemKB);
  univPlanAddToPlanNode(ctx->univplan, isLeft);
  do_convert_plantree_to_common_plan(node->lefttree, uid, true, isSubPlan,
                                     splits, rel, insist, ctx);
  do_convert_plantree_to_common_plan(node->righttree, uid, false, isSubPlan,
                                     splits, rel, insist, ctx);
  if (node->type == T_Append) {
    Append *append = (Append *)node;
    ListCell *lc;
    foreach (lc, append->appendplans) {
      Plan *planNode = (Plan *)lfirst(lc);
      do_convert_plantree_to_common_plan(planNode, uid, true, isSubPlan, splits,
                                         rel, insist, ctx);
    }
  }

  return;

end:
  ctx->convertible = false;
  elog(DEBUG1, "New Executor not support plan node of %s", nodeToString(node));
  return;
}

bool do_convert_targetlist_to_common_plan(Plan *node, CommonPlanContext *ctx) {
  ListCell *lc;
  foreach (lc, node->targetlist) {
    TargetEntry *te = (TargetEntry *)lfirst(lc);
    univPlanNewExpr(ctx->univplan);
    if (!do_convert_expr_to_common_plan(-1, (Expr *)te, ctx)) return false;
    univPlanTargetListAddTargetEntry(ctx->univplan, te->resjunk);
  }
  return true;
}

/*
 * param isInsist indicates whether insist convert quallist if there are
 * unsupported expr type. if true then return false for unsuppported type
 * otherwise convert "all" quallists that supported
 * return true if all the quallist node is converted
 */
bool do_convert_quallist_to_common_plan(Plan *node, CommonPlanContext *ctx,
                                        bool isInsist) {
  ListCell *lc;
  foreach (lc, node->qual) {
    Expr *expr = (Expr *)lfirst(lc);
    univPlanNewExpr(ctx->univplan);
    bool convert_ret = do_convert_expr_to_common_plan(-1, expr, ctx);
    if (!convert_ret && isInsist)
      return false;
    else if (!convert_ret && !isInsist)
      continue;
    else if (convert_ret)
      univPlanQualListAddExpr(ctx->univplan);
  }
  return true;
}

static bool do_convert_indexqualorig_to_common_plan(Plan *node,
                                                    CommonPlanContext *ctx,
                                                    bool isInsist) {
  ListCell *lc;
  foreach (lc, ((IndexScan *)node)->indexqualorig) {
    Expr *expr = (Expr *)lfirst(lc);
    univPlanNewExpr(ctx->univplan);
    bool convert_ret = do_convert_expr_to_common_plan(-1, expr, ctx);
    if (!convert_ret && isInsist)
      return false;
    else if (!convert_ret && !isInsist)
      continue;
    else if (convert_ret)
      univPlanQualListAddExpr(ctx->univplan);
  }
  return true;
}

static bool do_convert_indexqual_to_common_plan(Plan *node,
                                                CommonPlanContext *ctx,
                                                bool isInsist) {
  /* Must set to false when this function exit. */
  ctx->isConvertingIndexQual = true;

  ListCell *lc;
  foreach (lc, ((ExternalScan *)node)->indexqualorig) {
    Expr *expr = (Expr *)lfirst(lc);
    univPlanNewExpr(ctx->univplan);
    bool convert_ret = do_convert_expr_to_common_plan(-1, expr, ctx);
    if (!convert_ret && isInsist) {
      ctx->isConvertingIndexQual = false;
      return false;
    } else if (!convert_ret && !isInsist)
      continue;
    else if (convert_ret)
      univPlanIndexQualListAddExpr(ctx->univplan);
  }

  ctx->isConvertingIndexQual = false;
  return true;
}

bool do_convert_initplan_to_common_plan(Plan *node, CommonPlanContext *ctx) {
  ListCell *lc;
  foreach (lc, node->initPlan) {
    Expr *expr = (Expr *)lfirst(lc);
    univPlanNewExpr(ctx->univplan);
    bool convert_ret = do_convert_expr_to_common_plan(-1, expr, ctx);
    if (!convert_ret) return false;
    univPlanInitplanAddExpr(ctx->univplan);
  }
  return true;
}

bool do_convert_hashExpr_to_common_plan(Motion *node, CommonPlanContext *ctx) {
  ListCell *lc;
  foreach (lc, node->hashExpr) {
    Expr *expr = (Expr *)lfirst(lc);
    univPlanNewExpr(ctx->univplan);
    if (!do_convert_expr_to_common_plan(-1, expr, ctx)) return false;
    univPlanConnectorAddHashExpr(ctx->univplan);
  }
  return true;
}

void do_convert_magma_rangevseg_map_to_common_plan(CommonPlanContext *ctx) {
  int *map = NULL;
  int nmap = 0;
  get_magma_range_vseg_map(&map, &nmap, ctx->stmt->planner_segments);
  univPlanSetRangeVsegMap(ctx->univplan, map, nmap);
}

bool do_convert_limit_to_common_plan(Limit *node, CommonPlanContext *ctx) {
  if (node->limitOffset) {
    univPlanNewExpr(ctx->univplan);
    if (!do_convert_expr_to_common_plan(-1, (Expr *)node->limitOffset, ctx))
      return false;
    univPlanLimitAddLimitOffset(ctx->univplan);
  }
  if (node->limitCount) {
    univPlanNewExpr(ctx->univplan);
    if (!do_convert_expr_to_common_plan(-1, (Expr *)node->limitCount, ctx))
      return false;
    univPlanLimitAddLimitCount(ctx->univplan);
  }
  return true;
}

bool do_convert_sort_limit_to_common_plan(Sort *node, CommonPlanContext *ctx) {
  if (node->limitOffset) {
    univPlanNewExpr(ctx->univplan);
    if (!do_convert_expr_to_common_plan(-1, (Expr *)node->limitOffset, ctx))
      return false;
    univPlanSortAddLimitOffset(ctx->univplan);
  }
  if (node->limitCount) {
    univPlanNewExpr(ctx->univplan);
    if (!do_convert_expr_to_common_plan(-1, (Expr *)node->limitCount, ctx))
      return false;
    univPlanSortAddLimitCount(ctx->univplan);
  }
  return true;
}

bool do_convert_nestloop_joinqual_to_common_plan(NestLoop *node,
                                                 CommonPlanContext *ctx) {
  ListCell *lc;
  foreach (lc, node->join.joinqual) {
    Expr *expr = (Expr *)lfirst(lc);
    univPlanNewExpr(ctx->univplan);
    if (!do_convert_expr_to_common_plan(-1, expr, ctx)) return false;
    univPlanNestLoopAddJoinQual(ctx->univplan);
  }
  return true;
}

bool do_convert_hashjoin_clause_to_common_plan(HashJoin *node,
                                               CommonPlanContext *ctx) {
  ListCell *lc;

  foreach (lc, node->join.joinqual) {
    Expr *expr = (Expr *)lfirst(lc);
    univPlanNewExpr(ctx->univplan);
    if (!do_convert_expr_to_common_plan(-1, expr, ctx)) return false;
    univPlanHashJoinAddJoinQual(ctx->univplan);
  }

  foreach (lc, node->hashclauses) {
    Expr *expr = (Expr *)lfirst(lc);
    univPlanNewExpr(ctx->univplan);
    if (!do_convert_expr_to_common_plan(-1, expr, ctx)) return false;
    univPlanHashJoinAddHashClause(ctx->univplan);
  }

  foreach (lc, node->hashqualclauses) {
    Expr *expr = (Expr *)lfirst(lc);
    univPlanNewExpr(ctx->univplan);
    if (!do_convert_expr_to_common_plan(-1, expr, ctx)) return false;
    univPlanHashJoinAddHashQualClause(ctx->univplan);
  }
  return true;
}

bool do_convert_mergejoin_clause_to_common_plan(MergeJoin *node,
                                                CommonPlanContext *ctx) {
  ListCell *lc;
  foreach (lc, node->join.joinqual) {
    Expr *expr = (Expr *)lfirst(lc);
    univPlanNewExpr(ctx->univplan);
    if (!do_convert_expr_to_common_plan(-1, expr, ctx)) return false;
    univPlanMergeJoinAddJoinQual(ctx->univplan);
  }
  foreach (lc, node->mergeclauses) {
    Expr *expr = (Expr *)lfirst(lc);
    univPlanNewExpr(ctx->univplan);
    if (!do_convert_expr_to_common_plan(-1, expr, ctx)) return false;
    univPlanMergeJoinAddMergeClause(ctx->univplan);
  }
  return true;
}

bool do_convert_result_qual_to_common_plan(Result *node,
                                           CommonPlanContext *ctx) {
  ListCell *lc;
  foreach (lc, (List *)node->resconstantqual) {
    Expr *expr = (Expr *)lfirst(lc);
    univPlanNewExpr(ctx->univplan);
    if (!do_convert_expr_to_common_plan(-1, expr, ctx)) return false;
    univPlanResultAddResConstantQual(ctx->univplan);
  }
  return true;
}

bool do_convert_subqueryscan_subplan_to_common_plan(SubqueryScan *node,
                                                    CommonPlanContext *ctx) {
  univPlanNewSubPlanNode(ctx->univplan);
  do_convert_plantree_to_common_plan(node->subplan, -1, true, false, NIL, NULL,
                                     true, ctx);
  univPlanSubqueryScanAddSubPlan(ctx->univplan);
  univPlanFreeSubPlanNode(ctx->univplan);
  return ctx->convertible;
}

void do_convert_splits_to_common_plan(Scan *scan, Oid relOid,
                                      CommonPlanContext *ctx) {
  ListCell *lc1;
  foreach (lc1, ctx->stmt->scantable_splits) {
    SegFileSplitMap map = (SegFileSplitMapNode *)lfirst(lc1);
    if (map->relid == relOid) {
      List *relSplits = map->splits;
      ListCell *lc2;
      foreach (lc2, relSplits) {
        List *taskSplits = (List *)lfirst(lc2);
        do_convert_splits_list_to_common_plan(taskSplits, relOid, ctx);
      }
      return;
    }
  }
}

void do_convert_splits_list_to_common_plan(List *splits, Oid relOid,
                                           CommonPlanContext *ctx) {
  uint32_t fileSplitNum = (splits == NIL) ? 0 : splits->length;
  elog(DEBUG1, "SplitNum: %u", fileSplitNum);
  char relStorage = get_rel_relstorage(relOid);
  if (relStorage == RELSTORAGE_ORC) {
    if (splits == NIL) {
      univPlanSeqScanAddTaskWithFileSplits(false, ctx->univplan, 0, NULL, NULL,
                                           NULL, NULL, NULL, NULL);
      return;
    }

    int64_t *start = palloc0(fileSplitNum * sizeof(int64_t));
    int64_t *len = palloc0(fileSplitNum * sizeof(int64_t));
    int64_t *logicEof = palloc0(fileSplitNum * sizeof(int64_t));
    Relation rel = heap_open(relOid, NoLock);
    int32 filePathMaxLen = AOSegmentFilePathNameLen(rel) + 1;
    char **fileName = palloc0(fileSplitNum * sizeof(char *));
    int index = 0;
    int dummySegno;
    ListCell *lc = NULL;
    foreach (lc, splits) {
      FileSplitNode *filesplit = (FileSplitNode *)lfirst(lc);
      start[index] = filesplit->offsets;
      len[index] = filesplit->lengths;
      logicEof[index] = filesplit->logiceof;
      fileName[index] = palloc0(filePathMaxLen);
      MakeAOSegmentFileName(rel, filesplit->segno, -1, &dummySegno,
                            fileName[index]);
      ++index;
      elog(DEBUG1, "segno: %d, offset %d, length %d, logic eof %d",
           filesplit->segno, filesplit->offsets, filesplit->lengths,
           filesplit->logiceof);
    }
    heap_close(rel, NoLock);
    univPlanSeqScanAddTaskWithFileSplits(false, ctx->univplan, fileSplitNum,
                                         (const char **)fileName, start, len,
                                         logicEof, NULL, NULL);

    for (index = 0; index < fileSplitNum; ++index)
      if (fileName[index]) pfree(fileName[index]);
    pfree(fileName);
    pfree(start);
    pfree(len);
    pfree(logicEof);
  } else {
    ExtTableEntry *extEntry = GetExtTableEntry(relOid);
    Uri *uri = ParseExternalTableUri(strVal(linitial(extEntry->locations)));

    ListCell *lc = NULL;
    int index = 0;
    int fileNameLen;
    if (is_hdfs_protocol(uri)) {
      if (splits == NIL) {
        univPlanSeqScanAddTaskWithFileSplits(false, ctx->univplan, 0, NULL,
                                             NULL, NULL, NULL, NULL, NULL);
        FreeExternalTableUri(uri);
        return;
      }
      char **fileName = palloc0(fileSplitNum * sizeof(char *));
      int64_t *start = palloc0(fileSplitNum * sizeof(int64_t));
      int64_t *len = palloc0(fileSplitNum * sizeof(int64_t));

      foreach (lc, splits) {
        FileSplitNode *filesplit = (FileSplitNode *)lfirst(lc);

        start[index] = filesplit->offsets;
        len[index] = filesplit->lengths;

        fileNameLen =
            strlen(uri->hostname) + strlen(filesplit->ext_file_uri_string) + 15;
        fileName[index] = palloc(sizeof(char) * fileNameLen);
        sprintf(fileName[index], "hdfs://%s:%d%s", uri->hostname, uri->port,
                filesplit->ext_file_uri_string);

        ++index;
        elog(DEBUG1, "SplitInfo: %s, offset %d, length %d",
             filesplit->ext_file_uri_string, filesplit->offsets,
             filesplit->lengths);
      }

      univPlanSeqScanAddTaskWithFileSplits(false, ctx->univplan, fileSplitNum,
                                           (const char **)fileName, start, len,
                                           NULL, NULL, NULL);

      for (index = 0; index < fileSplitNum; ++index) {
        if (fileName[index]) {
          pfree(fileName[index]);
        }
      }
      pfree(fileName);
      pfree(start);
      pfree(len);
    } else if (is_magma_protocol(uri)) {
      if (splits == NIL) {
        univPlanSeqScanAddTaskWithFileSplits(true, ctx->univplan, 0, NULL, NULL,
                                             NULL, NULL, NULL, NULL);
        FreeExternalTableUri(uri);
        return;
      }
      if (fileSplitNum > MAGMA_MAX_FILESPLIT_NUM) {
        elog(ERROR, "File split number for table is %u, which exceeds %u",
             fileSplitNum, MAGMA_MAX_FILESPLIT_NUM);
      }

      char *fileName[MAGMA_MAX_FILESPLIT_NUM];
      int64_t start[MAGMA_MAX_FILESPLIT_NUM];
      int64_t len[MAGMA_MAX_FILESPLIT_NUM];
      int32_t rangeId[MAGMA_MAX_FILESPLIT_NUM];
      int32_t replicaGroupId[MAGMA_MAX_FILESPLIT_NUM];

      foreach (lc, splits) {
        FileSplitNode *filesplit = (FileSplitNode *)lfirst(lc);

        start[index] = filesplit->offsets;
        len[index] = filesplit->lengths;
        rangeId[index] = filesplit->range_id;
        replicaGroupId[index] = filesplit->replicaGroup_id;
        fileName[index] = filesplit->ext_file_uri_string;

        ++index;
      }

      univPlanSeqScanAddTaskWithFileSplits(true, ctx->univplan, fileSplitNum,
                                           (const char **)fileName, start, len,
                                           NULL, rangeId, replicaGroupId);
    } else {
      elog(ERROR, "external table with %s protocol is not supported",
           uri->customprotocol);
    }

    FreeExternalTableUri(uri);
  }
}

void do_convert_rangetbl_to_common_plan(List *rtable, CommonPlanContext *ctx) {
  ListCell *lc;
  foreach (lc, rtable) {
    RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);
    if (rte->rtekind != RTE_RELATION || rte->inh) {
      univPlanRangeTblEntryAddDummy(ctx->univplan);
      continue;
    }
    do_convert_onetbl_to_common_plan(rte->relid, ctx);
  }
}

void do_convert_onetbl_to_common_plan(Oid relid, CommonPlanContext *ctx) {
  Relation rel = heap_open(relid, NoLock);
  int attNum = 0;
  char **columnName = NULL;
  int32_t *columnDataType = NULL;
  int64_t *columnDataTypeMod = NULL;

  if (RelationIsOrc(rel)) {
    TupleDesc tableAttrs = rel->rd_att;
    attNum = tableAttrs->natts;
    columnName = palloc(attNum * sizeof(char *));
    columnDataType = palloc(attNum * sizeof(int32_t));
    columnDataTypeMod = palloc(attNum * sizeof(int64_t));
    for (int i = 0; i < attNum; ++i) {
      Form_pg_attribute att = tableAttrs->attrs[i];
      columnName[i] = pstrdup(att->attname.data);
      columnDataType[i] = map_hawq_type_to_common_plan(att->atttypid);
      if (columnDataType[i] == INVALIDTYPEID) {
        if (ctx->isMagma) {
          columnDataType[i] = IOBASETYPEID;
        } else {
          ctx->convertible = false;
          goto end;
        }
      }
      columnDataTypeMod[i] = att->atttypmod;
    }
    FormatType fmttype = UnivPlanOrcFormat;
    univPlanRangeTblEntryAddTable(
        ctx->univplan, relid, fmttype, relpath(rel->rd_node),
        buildInternalTableFormatOptionStringInJson(rel), attNum,
        (const char **)columnName, columnDataType, columnDataTypeMod, NULL,
        rel->rd_rel->relname.data);
  } else if (RelationIsExternal(rel)) {
    TupleDesc tableAttrs = rel->rd_att;
    attNum = tableAttrs->natts;
    columnName = palloc(attNum * sizeof(char *));
    columnDataType = palloc(attNum * sizeof(int32_t));
    columnDataTypeMod = palloc(attNum * sizeof(int64_t));
    for (int i = 0; i < attNum; ++i) {
      Form_pg_attribute att = tableAttrs->attrs[i];
      columnName[i] = pstrdup(att->attname.data);
      columnDataType[i] = map_hawq_type_to_common_plan(att->atttypid);
      if (columnDataType[i] == INVALIDTYPEID) {
        if (ctx->isMagma) {
          columnDataType[i] = IOBASETYPEID;
        } else {
          ctx->convertible = false;
          goto end;
        }
      }
      columnDataTypeMod[i] = att->atttypmod;
    }
    ExtTableEntry *extEntry = GetExtTableEntry(relid);
    char *fmtOptsJson = NULL;
    buildExternalTableFormatOptionStringInJson(extEntry->fmtopts, &fmtOptsJson);

    FormatType fmttype;
    char *location = NULL;
    char *fmtName = NULL;
    char *targetName = NULL;
    getFmtName(fmtOptsJson, &fmtName);
    int16_t magmaType = -1;
    if (pg_strcasecmp(fmtName, "magmatp") == 0) {
      magmaType = 0;
    } else if (pg_strcasecmp(fmtName, "magmaap") == 0) {
      magmaType = 1;
    }
    // indicate magma format table
    if (magmaType >= 0) {
      fmttype = UnivPlanMagmaFormat;
      Oid namespaceOid = RelationGetNamespace(rel);
      char *schema = getNamespaceNameByOid(namespaceOid);
      Assert(schema != NULL);
      char *table = RelationGetRelationName(rel);
      int locationLen = sizeof("magma:///") - 1 + strlen(database) +
                        sizeof("/") + strlen(schema) + sizeof("/") +
                        strlen(table) + sizeof('\0');
      location = palloc(locationLen * sizeof(char));
      sprintf(location, "magma:///%s/%s/%s", database, schema, table);

      /* get the magma target str */
      int tarLen = strlen(database) + sizeof(".") + strlen(schema) +
                   sizeof(".") + strlen(table) + sizeof('\0');
      targetName = palloc(tarLen * sizeof(char));
      sprintf(targetName, "%s.%s.%s", database, schema, table);

      if (ctx->stmt != NULL) {
        // for magma table, we should set magma_format_type, serialized_schema
        // only for new executor
        struct json_object *opt_json_object = json_tokener_parse(fmtOptsJson);
        if (json_object_object_get(opt_json_object, "magma_format_type") ==
            NULL) {
          char tmp[2];
          pg_itoa(magmaType, tmp);
          json_object_object_add(opt_json_object, "magma_format_type",
                                 json_object_new_string(tmp));
        }
        char *serializeSchema = NULL;
        int serializeSchemaLen = 0;
        GetMagmaSchemaByRelid(ctx->stmt->scantable_splits, relid,
                              &serializeSchema, &serializeSchemaLen);
        if (json_object_object_get(opt_json_object, "serialized_schema") ==
            NULL) {
          json_object_object_add(
              opt_json_object, "serialized_schema",
              json_object_new_string_len(serializeSchema, serializeSchemaLen));
        }
        if (opt_json_object != NULL) {
          const char *str = json_object_to_json_string(opt_json_object);
          pfree(fmtOptsJson);
          fmtOptsJson = (char *)palloc0(strlen(str) + 1);
          strcpy(fmtOptsJson, str);
          json_object_put(opt_json_object);
        }
      }
    } else if (pg_strcasecmp(fmtName, ORCTYPE) == 0) {
      fmttype = UnivPlanOrcFormat;
      location = pstrdup(strVal(linitial(extEntry->locations)));
    } else if (pg_strcasecmp(fmtName, TEXTTYPE) == 0 ||
               pg_strcasecmp(fmtName, CSVTYPE) == 0) {
      for (int i = 0; i < attNum; ++i) {
        // newQE doesn't support date/timestamp in text/csv format yet
        if (columnDataType[i] == DATEID || columnDataType[i] == TIMESTAMPID ||
            columnDataType[i] == TIMESTAMPTZID) {
          ctx->convertible = false;
          goto end;
        }
      }
      struct json_object *opt_json_object = json_tokener_parse(fmtOptsJson);
      if (pg_strcasecmp(fmtName, TEXTTYPE) == 0) {
        buildDefaultFormatterOptionsInJson(extEntry->encoding,
                                           TextFormatTypeTXT, opt_json_object);
        fmttype = UnivPlanTextFormat;
      } else {
        buildDefaultFormatterOptionsInJson(extEntry->encoding,
                                           TextFormatTypeCSV, opt_json_object);
        fmttype = UnivPlanCsvFormat;
      }
      const char *optsJsonStr = json_object_to_json_string(opt_json_object);
      pfree(fmtOptsJson);
      fmtOptsJson = (char *)palloc0(strlen(optsJsonStr) + 1);
      strcpy(fmtOptsJson, optsJsonStr);
      json_object_put(opt_json_object);
      location = pstrdup(strVal(linitial(extEntry->locations)));
    } else {
      elog(ERROR, "Cannot get external table format.");
    }

    univPlanRangeTblEntryAddTable(ctx->univplan, relid, fmttype, location,
                                  fmtOptsJson, attNum,
                                  (const char **)columnName, columnDataType,
                                  columnDataTypeMod, targetName, NULL);

    if (fmtOptsJson != NULL) pfree(fmtOptsJson);
    if (fmtName != NULL) pfree(fmtName);
    if (targetName != NULL) pfree(targetName);
    pfree(location);
  } else {
    univPlanRangeTblEntryAddDummy(ctx->univplan);
    heap_close(rel, NoLock);
    return;
  }

end:
  heap_close(rel, NoLock);
  for (int i = 0; i < attNum; ++i) {
    pfree(columnName[i]);
  }
  pfree(columnName);
  pfree(columnDataType);
  pfree(columnDataTypeMod);
}

static void do_convert_result_partition_rule_to_common_plan(
    CommonPlanContext *ctx, PartitionRule *partitionRule,
    bool isDefaultPartition) {
  if (partitionRule->children) {
    // TODO(chiyang): sub-partition
    ctx->convertible = false;
    return;
  }
  univPlanResultPartitionsAddPartitionRule(
      ctx->univplan, partitionRule->parchildrelid, partitionRule->parname,
      isDefaultPartition);

  ListCell *lc;
  foreach (lc, partitionRule->parlistvalues) {
    univPlanPartitionRuleAddPartitionValue(ctx->univplan, isDefaultPartition);
    List *partitionListValues = (List *)lfirst(lc);
    ListCell *lc;
    foreach (lc, partitionListValues) {
      Const *val = (List *)lfirst(lc);
      do_convert_expr_to_common_plan(-1, val, ctx);
      univPlanPartitionValueAddConst(ctx->univplan, isDefaultPartition);
    }
  }
}

static void do_convert_result_partitions_to_common_plan(
    PartitionNode *partitionNode, CommonPlanContext *ctx) {
  if (partitionNode->part->parkind != 'l') {
    // TODO(chiyang): range partition
    ctx->convertible = false;
    return;
  }

  Relation rel = RelationIdGetRelation(partitionNode->part->parrelid);
  if (rel->rd_node.relNode != partitionNode->part->parrelid) {
    // TODO(chiyang): support INSERT INTO partition table after TRUNCATE
    ctx->convertible = false;
    RelationClose(rel);
    return;
  }
  RelationClose(rel);

  univPlanAddResultPartitions(ctx->univplan, partitionNode->part->parrelid,
                              partitionNode->part->parkind,
                              partitionNode->part->paratts,
                              partitionNode->part->parnatts);
  ListCell *lc;
  foreach (lc, partitionNode->rules) {
    PartitionRule *partitionRule = (PartitionRule *)lfirst(lc);
    do_convert_result_partition_rule_to_common_plan(ctx, partitionRule, false);
  }
  if (partitionNode->default_part) {
    do_convert_result_partition_rule_to_common_plan(
        ctx, partitionNode->default_part, true);
  }
}

void do_convert_token_map_to_common_plan(CommonPlanContext *ctx) {
  HASH_SEQ_STATUS status;
  struct FileSystemCredential *entry;
  StringInfoData buffer;
  FileSystemCredentialC entryC;
  MemSet(&entryC, 0, sizeof(FileSystemCredentialC));
  FileSystemCredentialCPtr entryCPtr = &entryC;

  HTAB *currentFilesystemCredentials;
  MemoryContext currentFilesystemCredentialsMemoryContext;

  get_current_credential_cache_and_memcxt(
      &currentFilesystemCredentials,
      &currentFilesystemCredentialsMemoryContext);

  Insist(NULL != currentFilesystemCredentials);
  Insist(NULL != currentFilesystemCredentialsMemoryContext);

  initStringInfo(&buffer);
  hash_seq_init(&status, currentFilesystemCredentials);

  while (NULL != (entry = hash_seq_search(&status))) {
    entryCPtr->credential = entry->credential;
    entryCPtr->key.host = entry->key.host;
    entryCPtr->key.port = entry->key.port;
    entryCPtr->key.protocol = entry->key.protocol;
    univPlanAddTokenEntry(ctx->univplan, entryCPtr);
  }
  return;
}

// it's convertible and it's a magma scan
void do_convert_snapshot_to_common_plan(CommonPlanContext *ctx) {
  // start transaction in magma for SELECT in new executor
  // if (PlugStorageGetTransactionStatus() == PS_TXN_STS_DEFAULT) {
  //   PlugStorageStartTransaction(NULL);
  // }
  Assert(PlugStorageGetTransactionStatus() == PS_TXN_STS_STARTED);
  int32_t size = 0;
  char *snapshot = NULL;
  MagmaClientC_SerializeSnapshot(PlugStorageGetTransactionSnapshot(NULL),
                                 &snapshot, &size);
  if (snapshot && size != 0) {
    univPlanAddSnapshot(ctx->univplan, snapshot, size);
  }
  free(snapshot);
}
/*
 * return true if all the node expr is converted
 */
bool do_convert_expr_to_common_plan(int32_t pid, Expr *expr,
                                    CommonPlanContext *ctx) {
  int32_t mappingFuncId;
  int32_t uid;
  ListCell *lc;
  Expr *old;
  switch (expr->type) {
    case T_TargetEntry: {
      TargetEntry *te = (TargetEntry *)expr;
      old = parentExprSwitchTo(expr, ctx);
      if (!do_convert_expr_to_common_plan(pid, te->expr, ctx)) goto end;
      parentExprSwitchTo(old, ctx);
      break;
    }

    case T_RelabelType: {
      RelabelType *te = (RelabelType *)expr;
      old = parentExprSwitchTo(expr, ctx);
      if (!do_convert_expr_to_common_plan(pid, te->arg, ctx)) goto end;
      parentExprSwitchTo(old, ctx);
      break;
    }

    case T_Var: {
      Var *var = (Var *)expr;
      // deal with orc index
      if (ctx->idxColumns != NIL) {
        var->varattno = list_find_oid(ctx->idxColumns, var->varattno) + 1;
      }
      // TODO(chiyang): support system attribute
      if (var->varattno < 0 &&
          !(var->varattno == SelfItemPointerAttributeNumber ||
            var->varattno == GpSegmentIdAttributeNumber))
        goto end;
      if (ctx->parent && ctx->parent->type == T_Aggref) {
        Aggref *aggref = (Aggref *)ctx->parent;
        univPlanAggrefAddProxyVar(ctx->univplan, pid, var->varattno,
                                  HAWQ_FUNCOID_MAPPING(aggref->aggfnoid),
                                  var->vartypmod, var->varnoold,
                                  var->varoattno);
      } else {
        Oid varType = var->vartype;
        if (checkUnsupportedDataType(varType, DateStyle)) goto end;
        univPlanExprAddVar(
            ctx->univplan, pid,
            var->varno == DIRECT_LEFT_CHILD_VAR ? OUTER : var->varno,
            var->varattno, map_hawq_type_to_common_plan(varType),
            var->vartypmod, var->varnoold, var->varoattno);
      }
      break;
    }

    case T_Const: {
      Const *constval = (Const *)expr;
      int32_t consttype = map_hawq_type_to_common_plan(constval->consttype);
      if ((!constval->constisnull) &&
          (checkUnsupportedDataType(constval->consttype, DateStyle)))
        goto end;
      if (ctx->setDummyTListRef && ctx->parent &&
          ctx->parent->type == T_TargetEntry) {
        univPlanExprAddVar(ctx->univplan, pid, OUTER,
                           ((TargetEntry *)ctx->parent)->resno, consttype,
                           constval->consttypmod, 0, 0);
      } else {
        Oid typoutput;
        bool typIsVarlena;
        getTypeOutputInfo(constval->consttype, &typoutput, &typIsVarlena);
        char *extval = NULL;
        if (!constval->constisnull) {
          int savedDateStyle = DateStyle;
          int savedDateOrder = DateOrder;
          DateStyle = USE_ISO_DATES;
          DateOrder = DATEORDER_MDY;
          extval = OidOutputFunctionCall(typoutput, constval->constvalue);
          DateStyle = savedDateStyle;
          DateOrder = savedDateOrder;
          if (constval->consttype == INTERVALOID) {
            Interval *ival = (Interval *)DatumGetPointer(constval->constvalue);
            extval = palloc(sizeof(char) * INT64_MAX_LENGTH * 2);
            sprintf(extval, "%d:%d:%lld", ival->month, ival->day, ival->time);
          }
        }
        univPlanExprAddConst(ctx->univplan, pid, consttype,
                             constval->constisnull, extval,
                             constval->consttypmod);
      }
      break;
    }

    case T_OpExpr: {
      OpExpr *opExpr = (OpExpr *)expr;

      // Disable parameterized index qual, not supported yet.
      // The reason we do not disable it on Segment is because the old executor
      // will also run here(called by convert_extscan_to_common_plan() in
      // magma_beginscan()/magma_rescan()), which supports parameterized index
      // qual.
      if (AmIMaster() && ctx->isMagma && ctx->isConvertingIndexQual) {
        Expr *leftop = (Expr *)get_leftop(opExpr);
        if (leftop && IsA(leftop, RelabelType))
          leftop = ((RelabelType *)leftop)->arg;
        Expr *rightop = (Expr *)get_rightop(opExpr);
        if (rightop && IsA(rightop, RelabelType))
          rightop = ((RelabelType *)rightop)->arg;

        if (IsA(leftop, Var) && IsA(rightop, Var)) goto end;
      }

      old = parentExprSwitchTo(expr, ctx);

      mappingFuncId = HAWQ_FUNCOID_MAPPING(opExpr->opfuncid);
      if (IS_HAWQ_MAPPING_FUNCID_INVALID(mappingFuncId)) goto end;
      uid = univPlanExprAddOpExpr(ctx->univplan, pid, mappingFuncId);
      foreach (lc, opExpr->args) {
        if (!do_convert_expr_to_common_plan(uid, lfirst(lc), ctx)) goto end;
      }

      parentExprSwitchTo(old, ctx);
      break;
    }

    case T_FuncExpr: {
      FuncExpr *funcExpr = (FuncExpr *)expr;

      old = parentExprSwitchTo(expr, ctx);

      mappingFuncId = HAWQ_FUNCOID_MAPPING(funcExpr->funcid);
      if (IS_HAWQ_MAPPING_FUNCID_INVALID(mappingFuncId)) goto end;
      if (IS_HAWQ_MAPPING_DO_NOTHING(mappingFuncId)) {
        if (funcExpr->args->length != 1) goto end;
        foreach (lc, funcExpr->args) {
          if (!do_convert_expr_to_common_plan(pid, lfirst(lc), ctx)) goto end;
        }
      } else {
        uid = univPlanExprAddFuncExpr(ctx->univplan, pid, mappingFuncId);
        foreach (lc, funcExpr->args) {
          if (!do_convert_expr_to_common_plan(uid, lfirst(lc), ctx)) goto end;
        }
      }

      parentExprSwitchTo(old, ctx);
      break;
    }

    case T_Aggref: {
      Aggref *aggref = (Aggref *)expr;

      // disable count distinct case
      if (aggref->aggdistinct || aggref->aggorder) goto end;

      old = parentExprSwitchTo(expr, ctx);

      mappingFuncId = HAWQ_FUNCOID_MAPPING(aggref->aggfnoid);
      if (IS_HAWQ_MAPPING_FUNCID_INVALID(mappingFuncId)) goto end;
      switch (aggref->aggstage) {
        case AGGSTAGE_NORMAL:
          uid = univPlanAggrefAddOneStage(ctx->univplan, pid, mappingFuncId);
          break;
        case AGGSTAGE_PARTIAL:
          uid =
              univPlanAggrefAddPartialStage(ctx->univplan, pid, mappingFuncId);
          break;
        case AGGSTAGE_INTERMEDIATE:
          uid = univPlanAggrefAddIntermediateStage(ctx->univplan, pid,
                                                   mappingFuncId);
          break;
        case AGGSTAGE_FINAL:
          uid = univPlanAggrefAddFinalStage(ctx->univplan, pid, mappingFuncId);
          break;
        default:
          goto end;
      }

      foreach (lc, aggref->args) {
        if (!do_convert_expr_to_common_plan(uid, lfirst(lc), ctx)) goto end;
      }

      parentExprSwitchTo(old, ctx);
      break;
    }
    case T_BoolExpr: {
      BoolExpr *boolExpr = (BoolExpr *)expr;

      old = parentExprSwitchTo(expr, ctx);

      uid = univPlanExprAddBoolExpr(ctx->univplan, pid,
                                    (UnivplanBoolExprType)boolExpr->boolop);
      foreach (lc, boolExpr->args) {
        if (!do_convert_expr_to_common_plan(uid, lfirst(lc), ctx)) goto end;
      }

      parentExprSwitchTo(old, ctx);
      break;
    }
    case T_NullTest: {
      NullTest *nullTest = (NullTest *)expr;

      old = parentExprSwitchTo(expr, ctx);

      uid = univPlanExprAddNullTestExpr(
          ctx->univplan, pid, (UnivplanNullTestType)nullTest->nulltesttype);
      if (!do_convert_expr_to_common_plan(uid, nullTest->arg, ctx)) goto end;

      parentExprSwitchTo(old, ctx);
      break;
    }
    case T_BooleanTest: {
      BooleanTest *boolTest = (BooleanTest *)expr;

      old = parentExprSwitchTo(expr, ctx);

      uid = univPlanExprAddBoolTestExpr(
          ctx->univplan, pid, (UnivplanBooleanTestType)boolTest->booltesttype);
      if (!do_convert_expr_to_common_plan(uid, boolTest->arg, ctx)) goto end;

      parentExprSwitchTo(old, ctx);
      break;
    }

    case T_CaseExpr: {
      CaseExpr *caseexpr = (CaseExpr *)expr;

      old = parentExprSwitchTo(expr, ctx);

      ctx->exprBufStack = lcons(caseexpr->arg, ctx->exprBufStack);

      int32_t casetype = map_hawq_type_to_common_plan(caseexpr->casetype);
      if (checkUnsupportedDataType(caseexpr->casetype, DateStyle)) {
        goto end;
      }
      uid = univPlanExprAddCaseExpr(ctx->univplan, pid, casetype);
      foreach (lc, caseexpr->args) {
        if (!do_convert_expr_to_common_plan(uid, lfirst(lc), ctx)) goto end;
      }

      univPlanExprAddCaseExprDefresult(ctx->univplan, uid);
      if (!do_convert_expr_to_common_plan(uid, caseexpr->defresult, ctx))
        goto end;

      parentExprSwitchTo(old, ctx);
      ctx->exprBufStack = list_delete_first(ctx->exprBufStack);
      break;
    }

    case T_CaseWhen: {
      CaseWhen *casewhen = (CaseWhen *)expr;

      old = parentExprSwitchTo(expr, ctx);

      uid = univPlanExprAddCaseWhen(ctx->univplan, pid);

      univPlanExprAddCaseWhenExpr(ctx->univplan, uid);
      if (!do_convert_expr_to_common_plan(uid, casewhen->expr, ctx)) goto end;

      univPlanExprAddCaseWhenResult(ctx->univplan, uid);
      if (!do_convert_expr_to_common_plan(uid, casewhen->result, ctx)) goto end;

      parentExprSwitchTo(old, ctx);
      break;
    }

    case T_CaseTestExpr: {
      if (!do_convert_expr_to_common_plan(pid, linitial(ctx->exprBufStack),
                                          ctx))
        goto end;
      break;
    }

    case T_Param: {
      Param *param = (Param *)expr;
      if (param->paramkind != PARAM_EXEC) goto end;
      univPlanExprAddParam(ctx->univplan, pid,
                           (UnivplanParamKind)param->paramkind, param->paramid,
                           map_hawq_type_to_common_plan(param->paramtype),
                           param->paramtypmod);
      break;
    }

    case T_SubPlan: {
      SubPlan *subplan = (SubPlan *)expr;
      // TODO(chiyang): support ExecHashSubPlan
      if (subplan->useHashTable) goto end;
      if (!checkSupportedSubLinkType(subplan->subLinkType)) goto end;
      uid = univPlanExprAddSubPlan(
          ctx->univplan, pid, (UnivplanSubLinkType)subplan->subLinkType,
          subplan->plan_id, subplan->qDispSliceId,
          map_hawq_type_to_common_plan(subplan->firstColType),
          subplan->firstColTypmod, subplan->useHashTable, subplan->is_initplan);
      int num = 0;
      if ((num = list_length(subplan->setParam)) > 0) {
        int32_t *setParam = palloc(num * sizeof(int32_t));
        int idx = 0;
        foreach (lc, subplan->setParam)
          setParam[idx++] = lfirst_int(lc);
        univPlanSubPlanAddSetParam(ctx->univplan, uid, num, setParam);
        pfree(setParam);
      }
      if ((num = list_length(subplan->parParam)) > 0) {
        int32_t *parParam = palloc(num * sizeof(int32_t));
        int idx = 0;
        foreach (lc, subplan->parParam)
          parParam[idx++] = lfirst_int(lc);
        univPlanSubPlanAddParParam(ctx->univplan, uid, num, parParam);
        pfree(parParam);
      }
      if ((num = list_length(subplan->paramIds)) > 0) {
        int32_t *testexprParam = palloc(num * sizeof(int32_t));
        int idx = 0;
        foreach (lc, subplan->paramIds)
          testexprParam[idx++] = lfirst_int(lc);
        univPlanSubPlanAddTestexprParam(ctx->univplan, uid, num, testexprParam);
        pfree(testexprParam);
      }
      foreach (lc, subplan->args) {
        if (!do_convert_expr_to_common_plan(uid, lfirst(lc), ctx)) goto end;
      }
      univPlanExprAddSubPlanTestexpr(ctx->univplan, uid);
      if (subplan->testexpr &&
          !do_convert_expr_to_common_plan(uid, (Expr *)subplan->testexpr, ctx))
        goto end;
      break;
    }

    case T_ScalarArrayOpExpr: {
      ScalarArrayOpExpr *scalarArrayOpExpr = (ScalarArrayOpExpr *)expr;

      old = parentExprSwitchTo(expr, ctx);

      mappingFuncId = HAWQ_FUNCOID_MAPPING(scalarArrayOpExpr->opfuncid);
      if (IS_HAWQ_MAPPING_FUNCID_INVALID(mappingFuncId)) goto end;
      uid = univPlanExprAddScalarArrayOpExpr(ctx->univplan, pid, mappingFuncId,
                                             scalarArrayOpExpr->useOr);
      foreach (lc, scalarArrayOpExpr->args) {
        if (!do_convert_expr_to_common_plan(uid, lfirst(lc), ctx)) goto end;
      }

      parentExprSwitchTo(old, ctx);
      break;
    }

    case T_CoalesceExpr: {
      CoalesceExpr *coalesceExpr = (CoalesceExpr *)expr;

      old = parentExprSwitchTo(expr, ctx);

      int32_t coalesceType =
          map_hawq_type_to_common_plan(coalesceExpr->coalescetype);
      if (checkUnsupportedDataType(coalesceExpr->coalescetype, DateStyle)) {
        goto end;
      }
      uid = univPlanExprAddCoalesceExpr(ctx->univplan, pid, coalesceType,
                                        exprTypmod(coalesceExpr));
      foreach (lc, coalesceExpr->args) {
        if (!do_convert_expr_to_common_plan(uid, lfirst(lc), ctx)) goto end;
      }

      parentExprSwitchTo(old, ctx);
      break;
    }

    case T_NullIfExpr: {
      NullIfExpr *nullIfExpr = (NullIfExpr *)expr;

      old = parentExprSwitchTo(expr, ctx);

      mappingFuncId = HAWQ_FUNCOID_MAPPING(nullIfExpr->opfuncid);
      if (IS_HAWQ_MAPPING_FUNCID_INVALID(mappingFuncId)) goto end;
      int32_t nullIfType = map_hawq_type_to_common_plan(exprType(nullIfExpr));
      if (checkUnsupportedDataType(exprType(nullIfExpr), DateStyle)) {
        goto end;
      }
      uid = univPlanExprAddNullIfExpr(ctx->univplan, pid, mappingFuncId,
                                      nullIfType, exprTypmod(nullIfExpr));
      foreach (lc, nullIfExpr->args) {
        if (!do_convert_expr_to_common_plan(uid, lfirst(lc), ctx)) goto end;
      }

      parentExprSwitchTo(old, ctx);
      break;
    }

    case T_DistinctExpr: {
      DistinctExpr *distExpr = (DistinctExpr *)expr;

      old = parentExprSwitchTo(expr, ctx);

      mappingFuncId = HAWQ_FUNCOID_MAPPING(distExpr->opfuncid);
      if (IS_HAWQ_MAPPING_FUNCID_INVALID(mappingFuncId)) goto end;
      uid = univPlanExprAddDistinctExpr(ctx->univplan, pid, mappingFuncId);
      foreach (lc, distExpr->args) {
        if (!do_convert_expr_to_common_plan(uid, lfirst(lc), ctx)) goto end;
      }

      parentExprSwitchTo(old, ctx);
      break;
    }

    case T_Grouping: {
      old = parentExprSwitchTo(expr, ctx);
      uid = univPlanExprAddGrouping(ctx->univplan, pid);
      parentExprSwitchTo(old, ctx);
      break;
    }

    case T_GroupId: {
      old = parentExprSwitchTo(expr, ctx);
      uid = univPlanExprAddGroupId(ctx->univplan, pid);
      parentExprSwitchTo(old, ctx);
      break;
    }

    case T_GroupingFunc: {
      GroupingFunc *groupingFunc = (GroupId *)expr;
      old = parentExprSwitchTo(expr, ctx);
      int32_t *args = palloc(list_length(groupingFunc->args) * sizeof(int32_t));

      ListCell *lc;
      int idx = 0;
      foreach (lc, groupingFunc->args) {
        args[idx++] = (int)intVal(lfirst(lc));
      }
      uid = univPlanExprAddGroupingFunc(ctx->univplan, pid, args,
                                        list_length(groupingFunc->args),
                                        groupingFunc->ngrpcols);
      pfree(args);
      parentExprSwitchTo(old, ctx);
      break;
    }

    default:
      goto end;
  }

  return true;
end:
  elog(DEBUG1, "New Executor not support expression of %s", nodeToString(expr));
  return false;
}

Expr *parentExprSwitchTo(Expr *parent, CommonPlanContext *ctx) {
  Expr *old = ctx->parent;
  ctx->parent = parent;
  return old;
}

void setDummyTListRef(CommonPlanContext *ctx) { ctx->setDummyTListRef = true; }

void unsetDummyTListRef(CommonPlanContext *ctx) {
  ctx->setDummyTListRef = false;
}

void getFmtName(char *fmtOptsJson, char **fmtName) {
  *fmtName = NULL;
  struct json_object *jobj = json_tokener_parse(fmtOptsJson);

  struct json_object *returnObj;
  if (jobj != NULL &&
      json_object_object_get_ex(jobj, "formatter", &returnObj)) {
    if (returnObj != NULL) {
      const char *str = json_object_get_string(returnObj);
      *fmtName = (char *)palloc0(strlen(str) + 1);
      strcpy(*fmtName, str);
    }
    json_object_put(jobj);
  }
}

void checkUnsupportedStmt(PlannedStmt *stmt, CommonPlanContext *ctx) {
  if (stmt->commandType == CMD_UPDATE || stmt->commandType == CMD_DELETE)
    goto end;

  if (stmt->commandType == CMD_INSERT && !checkInsertSupportTable(stmt))
    goto end;

  if (stmt->originNodeType == T_CopyStmt) goto end;

  // disable insert into for common plan currently
  if (stmt->intoClause) goto end;

  return;

end:
  ctx->convertible = false;
}

bool checkInsertSupportTable(PlannedStmt *stmt) {
  if (list_length(stmt->resultRelations) > 1) return false;
  int32_t index = list_nth_int(stmt->resultRelations, 0);
  RangeTblEntry *rte = (RangeTblEntry *)list_nth(stmt->rtable, index - 1);

  if (RELSTORAGE_ORC == get_rel_relstorage(rte->relid)) return true;

  // disable partition table insert for external table
  if (stmt->result_partitions) return false;

  Relation pgExtTableRel = heap_open(ExtTableRelationId, RowExclusiveLock);
  cqContext cqc;
  HeapTuple tuple = caql_getfirst(caql_addrel(cqclr(&cqc), pgExtTableRel),
                                  cql("SELECT * FROM pg_exttable "
                                      " WHERE reloid = :1 "
                                      " FOR UPDATE ",
                                      ObjectIdGetDatum(rte->relid)));
  if (!HeapTupleIsValid(tuple)) goto end;

  bool isNull;
  char fmtCode =
      DatumGetChar(heap_getattr(tuple, Anum_pg_exttable_fmttype,
                                RelationGetDescr(pgExtTableRel), &isNull));
  if (!fmttype_is_custom(fmtCode)) goto end;

  Datum fmtOptDatum = heap_getattr(tuple, Anum_pg_exttable_fmtopts,
                                   RelationGetDescr(pgExtTableRel), &isNull);
  char *fmtOptString =
      DatumGetCString(DirectFunctionCall1(textout, fmtOptDatum));
  char *fmtName = getExtTblFormatterTypeInFmtOptsStr(fmtOptString);
  // format name must be orc/magmaap
  const int FORMAT_MAGMAAP_LEN = 7;
  const int FORMAT_ORC_LEN = 3;
  bool isSupported =
      fmtName && (!pg_strncasecmp(fmtName, "magmaap", FORMAT_MAGMAAP_LEN) ||
                  !pg_strncasecmp(fmtName, "orc", FORMAT_ORC_LEN));
  if (!isSupported) goto end;
  heap_close(pgExtTableRel, RowExclusiveLock);
  return true;

end:
  heap_close(pgExtTableRel, RowExclusiveLock);
  return false;
}

void checkReadStatsOnlyForAgg(Agg *node, CommonPlanContext *ctx) {
  ctx->scanReadStatsOnly = false;

  if (((Plan *)node)->lefttree->type == T_ExternalScan ||
      ((Plan *)node)->lefttree->type == T_Append ||
      ((Plan *)node)->lefttree->type == T_AppendOnlyScan) {
    // not work for group by statements
    if (node->numCols - node->numNullCols > 0) return;

    // not work for scan with filter
    if (((Plan *)node)->lefttree->qual) return;

    // for append node
    if (((Plan *)node)->lefttree->type == T_Append) {
      Append *appendNode = (Append *)((Plan *)node)->lefttree;
      ListCell *lc;
      foreach (lc, appendNode->appendplans) {
        Plan *appendPlan = (Plan *)lfirst(lc);
        if (!appendPlan->type == T_ExternalScan) return;
        if (appendPlan->qual) return;
        ListCell *lstcell;
        foreach (lstcell, appendPlan->targetlist) {
          TargetEntry *te = (TargetEntry *)lfirst(lstcell);
          if (te->expr->type != T_Var) return;
        }
      }
    }

    bool readStatsOnly = false;
    ListCell *lc1;
    foreach (lc1, ((Plan *)node)->targetlist) {
      TargetEntry *te = (TargetEntry *)lfirst(lc1);
      assert(te->expr->type == T_Aggref);
      Aggref *aggref = (Aggref *)te->expr;
      assert(aggref->aggstage == AGGSTAGE_PARTIAL);
      // filter functions that cannot use stats
      // currently only avg(),sum(),count(),min(),max() could use stats
      if (!((aggref->aggfnoid >= 2100 && aggref->aggfnoid <= 2147) ||
            (aggref->aggfnoid >= 2244 && aggref->aggfnoid <= 2245) ||
            (aggref->aggfnoid == 2803))) {
        return;
      }
      // special case for count(*)
      if (list_length(aggref->args) == 0) return;
      ListCell *lc2;
      foreach (lc2, aggref->args) {
        Expr *expr = lfirst(lc2);
        if (expr->type == T_Var) {
          // disable read stats only for timestamp
          if (aggref->aggfnoid != COUNT_ANY_OID) {
            if (((Var *)expr)->vartype == HAWQ_TYPE_TIMESTAMP ||
                ((Var *)expr)->vartype == HAWQ_TYPE_TIMESTAMPTZ)
              return;
          }
          readStatsOnly = true;
        } else if (expr->type == T_RelabelType &&
                   ((RelabelType *)expr)->arg->type == T_Var) {
          // disable read stats only for timestamp
          if (aggref->aggfnoid != COUNT_ANY_OID) {
            if (((Var *)((RelabelType *)expr)->arg)->vartype ==
                HAWQ_TYPE_TIMESTAMP)
              return;
          }
          readStatsOnly = true;
        } else {
          return;
        }
      }
    }
    ctx->scanReadStatsOnly = readStatsOnly;
  }
}

bool checkSupportedSubLinkType(SubLinkType sublinkType) {
  switch (sublinkType) {
    case EXISTS_SUBLINK:
    case ALL_SUBLINK:
    case ANY_SUBLINK:
    case EXPR_SUBLINK:
    case NOT_EXISTS_SUBLINK:
      return true;
    default:
      return false;
  }
}

bool checkIsPrepareQuery(QueryDesc *queryDesc) {
  if (queryDesc->sourceText) {
    if (pg_strncasecmp((queryDesc->sourceText), "PREPARE", 7) == 0) {
      if (pg_strcasecmp(new_executor_mode, new_executor_mode_on) == 0) {
        elog(ERROR,
             "New executor not supported yet, please set new_executor=auto/off "
             "to "
             "fall back to old executor");
      }
      return true;
    }
  }
  return false;
}

void convert_querydesc_to_common_plan(QueryDesc *queryDesc,
                                      CommonPlanContext *ctx) {
  if (queryDesc->params != NULL && queryDesc->params->numParams > 0) {
    int savedDateStyle = DateStyle;
    int savedDateOrder = DateOrder;
    for (int32_t iParam = 0; iParam < queryDesc->params->numParams; ++iParam) {
      ParamExternData *pxd = &queryDesc->params->params[iParam];
      Oid typoutput;
      bool typIsVarlena;
      getTypeOutputInfo(pxd->ptype, &typoutput, &typIsVarlena);
      char *extval = NULL;
      if (!pxd->isnull) {
        switch (pxd->ptype) {
          case BOOLOID:
          case INT8OID:
          case INT4OID:
          case INT2OID:
          case FLOAT8OID:
          case FLOAT4OID:
          case TIMEOID:
          case TIMETZOID:
            extval = OidOutputFunctionCall(typoutput, pxd->value);
            break;
          case DATEOID:
          case TIMESTAMPOID:
          case TIMESTAMPTZOID:
            DateStyle = USE_ISO_DATES;
            DateOrder = DATEORDER_MDY;
            extval = OidOutputFunctionCall(typoutput, pxd->value);
            DateStyle = savedDateStyle;
            DateOrder = savedDateOrder;
            break;
          case INTERVALOID: {
            Interval *ival = (Interval *)DatumGetPointer(pxd->value);
            extval = palloc(sizeof(char) * INT64_MAX_LENGTH * 2);
            sprintf(extval, "%d-%d-%lld", ival->month, ival->day, ival->time);
          } break;
          default:
            if (pxd->value)
              extval = OidOutputFunctionCall(typoutput, pxd->value);
        }
      }
      univPlanAddParamInfo(ctx->univplan,
                           map_hawq_type_to_common_plan(pxd->ptype),
                           pxd->isnull, extval);
    }
  }
}
