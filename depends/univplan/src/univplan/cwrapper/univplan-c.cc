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

#include "univplan/cwrapper/univplan-c.h"

#include <cassert>
#include <map>
#include <stack>
#include <string>
#include <utility>
#include <vector>

#include "dbcommon/function/func-kind.cg.h"
#include "dbcommon/function/func.h"
#include "dbcommon/log/logger.h"
#include "dbcommon/utils/comp/lz4-compressor.h"
#include "dbcommon/utils/macro.h"

#include "univplan/common/plannode-util.h"
#include "univplan/common/stagize.h"
#include "univplan/common/var-util.h"
#include "univplan/univplanbuilder/univplanbuilder-connector.h"
#include "univplan/univplanbuilder/univplanbuilder-expr-tree.h"
#include "univplan/univplanbuilder/univplanbuilder-plan.h"
#include "univplan/univplanbuilder/univplanbuilder-table.h"
#include "univplan/univplanbuilder/univplanbuilder.h"

#ifdef __cplusplus
extern "C" {
#endif

static void univPlanSetError(UnivPlanCatchedError *ce, int errCode,
                             const char *errMsg);

struct UnivPlanC {
  univplan::UnivPlanBuilder::uptr upb;
  std::stack<univplan::UnivPlanBuilder::uptr> upbBak;
  univplan::UnivPlanBuilderNode::uptr curNode;
  std::stack<univplan::UnivPlanBuilderNode::uptr> curNodeBak;
  univplan::UnivPlanBuilderExprTree::uptr curExpr;
  std::string serializedPlan;
  UnivPlanCatchedError error;
  std::string debugString;
  uint16_t totalStageNo;
  std::map<int32_t, int32_t> subplanStageNo;
};

UnivPlanC *univPlanNewInstance() {
  UnivPlanC *instance = new UnivPlanC();
  instance->upb.reset(new univplan::UnivPlanBuilder);
  instance->error.errCode = ERRCODE_SUCCESSFUL_COMPLETION;
  instance->totalStageNo = 0;
  return instance;
}

void univPlanFreeInstance(UnivPlanC **up) {
  assert(up != nullptr);
  if (*up == nullptr) return;
  delete *up;
  *up = nullptr;
}

void univPlanNewSubPlanNode(UnivPlanC *up) {
  up->upbBak.push(std::move(up->upb));
  up->curNodeBak.push(std::move(up->curNode));
  up->upb.reset(new univplan::UnivPlanBuilder);
}

void univPlanFreeSubPlanNode(UnivPlanC *up) {
  up->upb = std::move(up->upbBak.top());
  up->upbBak.pop();
  up->curNode = std::move(up->curNodeBak.top());
  up->curNodeBak.pop();
}

void univPlanRangeTblEntryAddTable(UnivPlanC *up, uint64_t tid,
                                   FormatType format, const char *location,
                                   const char *optStrInJson, uint32_t columnNum,
                                   const char **columnName,
                                   int32_t *columnDataType,
                                   int64_t *columnDataTypeMod,
                                   const char *targetName) {
  univplan::UnivPlanBuilderPlan *bld = up->upb->getPlanBuilderPlan();
  univplan::UnivPlanBuilderRangeTblEntry::uptr rte =
      bld->addRangeTblEntryAndGetBuilder();
  univplan::UnivPlanBuilderTable::uptr table(
      new univplan::UnivPlanBuilderTable);
  table->setTableId(tid);
  univplan::UNIVPLANFORMATTYPE fmtType;
  switch (format) {
    case FormatType::UnivPlanCsvFormat:
      fmtType = univplan::CSV_FORMAT;
      break;
    case FormatType::UnivPlanTextFormat:
      fmtType = univplan::TEXT_FORMAT;
      break;
    case FormatType::UnivPlanOrcFormat:
      fmtType = univplan::ORC_FORMAT;
      break;
    case FormatType::UnivPlanMagmaFormat:
      fmtType = univplan::MAGMA_FORMAT;
      table->setTargetName(targetName);
      break;
    default:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "univPlanRangeTblEntryAddTable can't handle FormatType %d",
                format);
  }
  table->setTableFormat(fmtType);
  table->setTableLocation(location);

  std::string cvKey = "TableOptionsInJson_" + std::to_string(tid);
  std::string nCvKey;
  bld->addCommonValue(cvKey, optStrInJson, &nCvKey);
  table->setTableOptionsInJson(nCvKey);
  for (int i = 0; i < columnNum; ++i) {
    univplan::UnivPlanBuilderColumn::uptr column =
        table->addPlanColumnAndGetBuilder();
    column->setColumnName(columnName[i]);
    column->setTypeId(columnDataType[i]);
    column->setTypeMod(columnDataTypeMod[i]);
  }

  rte->setTable(std::move(table->ownTable()));
}

void univPlanRangeTblEntryAddDummy(UnivPlanC *up) {
  univplan::UnivPlanBuilderPlan *bld = up->upb->getPlanBuilderPlan();
  univplan::UnivPlanBuilderRangeTblEntry::uptr rte =
      bld->addRangeTblEntryAndGetBuilder();
  univplan::UnivPlanBuilderTable::uptr table(
      new univplan::UnivPlanBuilderTable);
  table->setTableId(0);
  univplan::UNIVPLANFORMATTYPE fmtType = univplan::INVALID_FORMAT;
  table->setTableFormat(fmtType);
  table->setTableLocation("");
  table->setTableOptionsInJson("");
  rte->setTable(std::move(table->ownTable()));
}

void univPlanReceiverAddListeners(UnivPlanC *up, uint32_t listenerNum,
                                  const char **addr, int32_t *port) {
  univplan::UnivPlanBuilderPlan *bld = up->upb->getPlanBuilderPlan();
  univplan::UnivPlanBuilderReceiver::uptr rev = bld->addReceiverAndGetBuilder();
  for (int i = 0; i < listenerNum; ++i) {
    univplan::UnivPlanBuilderListener::uptr listener =
        rev->addPlanListenerAndGetBuilder();
    listener->setAddress(addr[i]);
    listener->setPort(port[i]);
  }
}

void univPlanAddParamInfo(UnivPlanC *up, int32_t type, bool isNull,
                          const char *buffer) {
  univplan::UnivPlanBuilderPlan *bld = up->upb->getPlanBuilderPlan();
  univplan::UnivPlanBuilderParamInfo::uptr paramInfo =
      bld->addParamInfoAndGetBuilder();
  paramInfo->setType(type).setIsNull(isNull);
  if (!isNull && buffer) paramInfo->setValue(buffer);
}

void univPlanSetDoInstrument(UnivPlanC *up, bool doInstrument) {
  univplan::UnivPlanBuilderPlan *bld = up->upb->getPlanBuilderPlan();
  bld->setDoInstrument(doInstrument);
}

void univPlanSetNCrossLevelParams(UnivPlanC *up, int32_t nCrossLevelParams) {
  univplan::UnivPlanBuilderPlan *bld = up->upb->getPlanBuilderPlan();
  bld->setNCrossLevelParams(nCrossLevelParams);
}

void univPlanSetCmdType(UnivPlanC *up, UnivPlanCCmdType type) {
  univplan::UnivPlanBuilderPlan *bld = up->upb->getPlanBuilderPlan();
  univplan::UNIVPLANCMDTYPE cmdType;
  switch (type) {
    case UnivPlanCCmdType::UNIVPLAN_CMD_SELECT:
      cmdType = univplan::UNIVPLANCMDTYPE::CMD_SELECT;
      break;
    case UnivPlanCCmdType::UNIVPLAN_CMD_INSERT:
      cmdType = univplan::UNIVPLANCMDTYPE::CMD_INSERT;
      break;
    default:
      LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "CmdType(%d) not supported yet",
                type);
  }

  bld->setCmdType(cmdType);
}

void univPlanSetPlanNodeInfo(UnivPlanC *up, double planRows,
                             int32_t planRowWidth, uint64_t operatorMemKB) {
  up->curNode->setNodePlanRows(planRows);
  up->curNode->setNodePlanRowWidth(planRowWidth);
  up->curNode->setNodePlanOperatorMemKB(operatorMemKB);
}

int32_t univPlanConnectorNewInstance(UnivPlanC *up, int32_t pid) {
  up->curNode = univplan::PlanNodeUtil::createPlanBuilderNode(
      univplan::UNIVPLAN_CONNECTOR);
  up->curNode->pid = pid;
  up->curNode->uid = up->upb->getUid();
  up->totalStageNo += 1;
  return up->curNode->uid;
}

void univPlanConnectorSetType(UnivPlanC *up, ConnectorType type) {
  switch (type) {
    case ConnectorType::UnivPlanShuffle:
      dynamic_cast<univplan::UnivPlanBuilderConnector *>(up->curNode.get())
          ->setConnectorType(univplan::CONNECTORTYPE_SHUFFLE);
      break;
    case ConnectorType::UnivPlanBroadcast:
      dynamic_cast<univplan::UnivPlanBuilderConnector *>(up->curNode.get())
          ->setConnectorType(univplan::CONNECTORTYPE_BROADCAST);
      break;
    case ConnectorType::UnivPlanConverge:
      dynamic_cast<univplan::UnivPlanBuilderConnector *>(up->curNode.get())
          ->setConnectorType(univplan::CONNECTORTYPE_CONVERGE);
      break;
    default:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "univPlanConnectorSetType can't handle ConnectorType %d", type);
  }
}

void univPlanConnectorSetStageNo(UnivPlanC *up, int32_t stageNo) {
  dynamic_cast<univplan::UnivPlanBuilderConnector *>(up->curNode.get())
      ->setStageNo(stageNo);
}

void univPlanConnectorSetRangeVsegMap(UnivPlanC *up, int *map,
                                      bool magmaTable) {
  dynamic_cast<univplan::UnivPlanBuilderConnector *>(up->curNode.get())
      ->setMagmaMap(map);
  dynamic_cast<univplan::UnivPlanBuilderConnector *>(up->curNode.get())
      ->setMagmaTable(magmaTable);
}

void univPlanSetRangeNum(UnivPlanC *up, int rangeNum) {
  dynamic_cast<univplan::UnivPlanBuilderConnector *>(up->curNode.get())
      ->setRangeNum(rangeNum);
}

void univPlanConnectorSetColIdx(UnivPlanC *up, int64_t numCols,
                                const int32_t *colIdx) {
  univplan::UnivPlanBuilderConnector *conn =
      dynamic_cast<univplan::UnivPlanBuilderConnector *>(up->curNode.get());
  std::vector<int32_t> nArray(colIdx, colIdx + numCols);
  conn->setColIdx(nArray);
}

void univPlanConnectorSetSortFuncId(UnivPlanC *up, int64_t numCols,
                                    const int32_t *sortFuncId) {
  univplan::UnivPlanBuilderConnector *conn =
      dynamic_cast<univplan::UnivPlanBuilderConnector *>(up->curNode.get());
  std::vector<int32_t> nArray(sortFuncId, sortFuncId + numCols);
  conn->setSortFuncId(nArray);
}

int32_t univPlanExtScanNewInstance(UnivPlanC *up, int32_t pid) {
  up->curNode = univplan::PlanNodeUtil::createPlanBuilderNode(
      univplan::UNIVPLAN_EXT_GS_SCAN);
  up->curNode->pid = pid;
  up->curNode->uid = up->upb->getUid();
  return up->curNode->uid;
}

void univPlanExtScanSetRelId(UnivPlanC *up, uint32_t relId) {
  dynamic_cast<univplan::UnivPlanBuilderExtGSScan *>(up->curNode.get())
      ->setScanRelId(relId);
}

void univPlanExtScanSetIndex(UnivPlanC *up, bool index) {
  dynamic_cast<univplan::UnivPlanBuilderExtGSScan *>(up->curNode.get())
      ->setScanIndex(index);
}

void univPlanExtScanSetScanType(UnivPlanC *up, int type) {
  dynamic_cast<univplan::UnivPlanBuilderExtGSScan *>(up->curNode.get())
      ->setIndexScanType(univplan::ExternalScanType(type));
}

void univPlanExtScanDirection(UnivPlanC *up, int direction) {
  dynamic_cast<univplan::UnivPlanBuilderExtGSScan *>(up->curNode.get())
      ->setDirectionType(univplan::ExternalScanDirection(direction));
}

void univPlanExtScanSetIndexName(UnivPlanC *up, const char *indexName) {
  dynamic_cast<univplan::UnivPlanBuilderExtGSScan *>(up->curNode.get())
      ->setIndexName(indexName);
}

void univPlanIndexQualListAddExpr(UnivPlanC *up) {
  dynamic_cast<univplan::UnivPlanBuilderExtGSScan *>(up->curNode.get())
      ->addIndexQual(std::move(up->curExpr));
}

void univPlanExtScanSetColumnsToRead(UnivPlanC *up, int64_t numCols,
                                     const int32_t *columnsToRead) {
  std::vector<int32_t> nArray(columnsToRead, columnsToRead + numCols);
  dynamic_cast<univplan::UnivPlanBuilderExtGSScan *>(up->curNode.get())
      ->setColumnsToRead(nArray);
}

int32_t univPlanSeqScanNewInstance(UnivPlanC *up, int32_t pid) {
  up->curNode = univplan::PlanNodeUtil::createPlanBuilderNode(
      univplan::UNIVPLAN_SCAN_SEQ);
  up->curNode->pid = pid;
  up->curNode->uid = up->upb->getUid();
  return up->curNode->uid;
}

void univPlanSeqScanSetRelId(UnivPlanC *up, uint32_t relId) {
  dynamic_cast<univplan::UnivPlanBuilderScanSeq *>(up->curNode.get())
      ->setScanRelId(relId);
}

void univPlanSeqScanSetColumnsToRead(UnivPlanC *up, int64_t numCols,
                                     const int32_t *columnsToRead) {
  std::vector<int32_t> nArray(columnsToRead, columnsToRead + numCols);
  dynamic_cast<univplan::UnivPlanBuilderScanSeq *>(up->curNode.get())
      ->setColumnsToRead(nArray);
}

void univPlanSeqScanSetReadStatsOnly(UnivPlanC *up, bool readStatsOnly) {
  dynamic_cast<univplan::UnivPlanBuilderScanSeq *>(up->curNode.get())
      ->setReadStatsOnly(readStatsOnly);
}

void univPlanSeqScanAddTaskWithFileSplits(bool isMagma, UnivPlanC *up,
                                          uint32_t fileSplitNum,
                                          const char **fileName, int64_t *start,
                                          int64_t *len, int32_t *rangeid,
                                          int32_t *rgid) {
  univplan::UnivPlanBuilderScanTask::uptr task;
  if (isMagma) {
    task = dynamic_cast<univplan::UnivPlanBuilderExtGSScan *>(up->curNode.get())
               ->addScanTaskAndGetBuilder();
  } else {
    task = dynamic_cast<univplan::UnivPlanBuilderScanSeq *>(up->curNode.get())
               ->addScanTaskAndGetBuilder();
  }
  for (int i = 0; i < fileSplitNum; ++i) {
    task->addScanFileSplit(fileName[i], start[i], len[i],
                           (rangeid == nullptr ? -1 : rangeid[i]),
                           (rgid == nullptr ? -1 : rgid[i]));
  }
  task->generate();
}

void univPlanAggSetAggstrategy(UnivPlanC *up, UnivPlanAggStrategy aggStrategy) {
  univplan::UnivPlanBuilderAgg *agg =
      reinterpret_cast<univplan::UnivPlanBuilderAgg *>(up->curNode.get());
  agg->setAggStrategy(static_cast<univplan::AggStrategy>(aggStrategy));
}

int32_t univPlanAggNewInstance(UnivPlanC *up, int32_t pid) {
  up->curNode =
      univplan::PlanNodeUtil::createPlanBuilderNode(univplan::UNIVPLAN_AGG);
  up->curNode->pid = pid;
  up->curNode->uid = up->upb->getUid();
  return up->curNode->uid;
}

void univPlanAggSetNumGroupsAndGroupColIndexes(UnivPlanC *up, int64_t numGroups,
                                               int64_t numCols,
                                               const int32_t *grpColIdx) {
  univplan::UnivPlanBuilderAgg *agg =
      dynamic_cast<univplan::UnivPlanBuilderAgg *>(up->curNode.get());
  agg->setNumGroups(numGroups);
  std::vector<int32_t> nArray(grpColIdx, grpColIdx + numCols);
  agg->setGroupColIndexes(nArray);
}

int32_t univPlanSortNewInstance(UnivPlanC *up, int32_t pid) {
  up->curNode =
      univplan::PlanNodeUtil::createPlanBuilderNode(univplan::UNIVPLAN_SORT);
  up->curNode->pid = pid;
  up->curNode->uid = up->upb->getUid();
  return up->curNode->uid;
}

void univPlanSortSetColIdx(UnivPlanC *up, int64_t numCols,
                           const int32_t *colIdx) {
  univplan::UnivPlanBuilderSort *sort =
      dynamic_cast<univplan::UnivPlanBuilderSort *>(up->curNode.get());
  std::vector<int32_t> nArray(colIdx, colIdx + numCols);
  sort->setColIdx(nArray);
}

void univPlanSortSetSortFuncId(UnivPlanC *up, int64_t numCols,
                               const int32_t *sortFuncId) {
  univplan::UnivPlanBuilderSort *sort =
      dynamic_cast<univplan::UnivPlanBuilderSort *>(up->curNode.get());
  std::vector<int32_t> nArray(sortFuncId, sortFuncId + numCols);
  sort->setSortFuncId(nArray);
}

void univPlanSortAddLimitOffset(UnivPlanC *up) {
  assert(up->curExpr);
  dynamic_cast<univplan::UnivPlanBuilderSort *>(up->curNode.get())
      ->setLimitOffset(std::move(up->curExpr));
}

void univPlanSortAddLimitCount(UnivPlanC *up) {
  assert(up->curExpr);
  dynamic_cast<univplan::UnivPlanBuilderSort *>(up->curNode.get())
      ->setLimitCount(std::move(up->curExpr));
}

int32_t univPlanLimitNewInstance(UnivPlanC *up, int32_t pid) {
  up->curNode =
      univplan::PlanNodeUtil::createPlanBuilderNode(univplan::UNIVPLAN_LIMIT);
  up->curNode->pid = pid;
  up->curNode->uid = up->upb->getUid();
  return up->curNode->uid;
}

int32_t univPlanAppendNewInstance(UnivPlanC *up, int32_t pid) {
  up->curNode =
      univplan::PlanNodeUtil::createPlanBuilderNode(univplan::UNIVPLAN_APPEND);
  up->curNode->pid = pid;
  up->curNode->uid = up->upb->getUid();
  return up->curNode->uid;
}

void univPlanAppendAddAppendPlan(UnivPlanC *up) {
  const univplan::UnivPlanPlanNodePoly &poly =
      up->upb->getPlanBuilderPlan()->getPlan()->plan();
  dynamic_cast<univplan::UnivPlanBuilderAppend *>(up->curNodeBak.top().get())
      ->addAppendPlan(poly);
}

static univplan::UNIVPLANJOINTYPE joinTypeMapping(UnivPlanCJoinType type) {
  switch (type) {
    case UnivPlanCJoinType::UNIVPLAN_JOIN_INNER:
      return univplan::UNIVPLANJOINTYPE::JOIN_INNER;
    case UnivPlanCJoinType::UNIVPLAN_JOIN_LEFT:
      return univplan::UNIVPLANJOINTYPE::JOIN_LEFT;
    case UnivPlanCJoinType::UNIVPLAN_JOIN_RIGHT:
      return univplan::UNIVPLANJOINTYPE::JOIN_RIGHT;
    case UnivPlanCJoinType::UNIVPLAN_JOIN_FULL:
      return univplan::UNIVPLANJOINTYPE::JOIN_FULL;
    default:
      LOG_INFO("JoinType(%d) not supported yet", type);
      return univplan::UNIVPLANJOINTYPE::JOIN_NOT_SUPPORTED;
  }
}

int32_t univPlanNestLoopNewInstance(UnivPlanC *up, int32_t pid) {
  up->curNode = univplan::PlanNodeUtil::createPlanBuilderNode(
      univplan::UNIVPLAN_NESTLOOP);
  up->curNode->pid = pid;
  up->curNode->uid = up->upb->getUid();
  return up->curNode->uid;
}

bool univPlanNestLoopSetType(UnivPlanC *up, UnivPlanCJoinType type) {
  univplan::UNIVPLANJOINTYPE univplanType = joinTypeMapping(type);
  if (univplanType == univplan::UNIVPLANJOINTYPE::JOIN_NOT_SUPPORTED)
    return false;
  dynamic_cast<univplan::UnivPlanBuilderNestLoop *>(up->curNode.get())
      ->setJoinType(univplanType);
  return true;
}

void univPlanNestLoopAddJoinQual(UnivPlanC *up) {
  assert(up->curExpr);
  dynamic_cast<univplan::UnivPlanBuilderNestLoop *>(up->curNode.get())
      ->addJoinQual(std::move(up->curExpr));
}

int32_t univPlanHashJoinNewInstance(UnivPlanC *up, int32_t pid) {
  up->curNode = univplan::PlanNodeUtil::createPlanBuilderNode(
      univplan::UNIVPLAN_HASHJOIN);
  up->curNode->pid = pid;
  up->curNode->uid = up->upb->getUid();
  return up->curNode->uid;
}

bool univPlanHashJoinSetType(UnivPlanC *up, UnivPlanCJoinType type) {
  switch (type) {
    case UnivPlanCJoinType::UNIVPLAN_JOIN_INNER:
    case UnivPlanCJoinType::UNIVPLAN_JOIN_LEFT:
    case UnivPlanCJoinType::UNIVPLAN_JOIN_IN:
    case UnivPlanCJoinType::UNIVPLAN_JOIN_LASJ:
    case UnivPlanCJoinType::UNIVPLAN_JOIN_LASJ_NOTIN:
      break;
    default:
      return false;
  }
  dynamic_cast<univplan::UnivPlanBuilderHashJoin *>(up->curNode.get())
      ->setJoinType(static_cast<univplan::UNIVPLANJOINTYPE>(type));
  return true;
}

void univPlanHashJoinAddJoinQual(UnivPlanC *up) {
  assert(up->curExpr);
  dynamic_cast<univplan::UnivPlanBuilderHashJoin *>(up->curNode.get())
      ->addJoinQual(std::move(up->curExpr));
}

void univPlanHashJoinAddHashClause(UnivPlanC *up) {
  assert(up->curExpr);
  dynamic_cast<univplan::UnivPlanBuilderHashJoin *>(up->curNode.get())
      ->addHashClause(std::move(up->curExpr));
}

void univPlanHashJoinAddHashQualClause(UnivPlanC *up) {
  assert(up->curExpr);
  dynamic_cast<univplan::UnivPlanBuilderHashJoin *>(up->curNode.get())
      ->addHashQualClause(std::move(up->curExpr));
}

int32_t univPlanMergeJoinNewInstance(UnivPlanC *up, int32_t pid) {
  up->curNode = univplan::PlanNodeUtil::createPlanBuilderNode(
      univplan::UNIVPLAN_MERGEJOIN);
  up->curNode->pid = pid;
  up->curNode->uid = up->upb->getUid();
  return up->curNode->uid;
}

bool univPlanMergeJoinSetType(UnivPlanC *up, UnivPlanCJoinType type) {
  univplan::UNIVPLANJOINTYPE univplanType = joinTypeMapping(type);
  if (univplanType == univplan::UNIVPLANJOINTYPE::JOIN_NOT_SUPPORTED)
    return false;
  dynamic_cast<univplan::UnivPlanBuilderMergeJoin *>(up->curNode.get())
      ->setJoinType(joinTypeMapping(type));
  return true;
}

void univPlanMergeJoinAddJoinQual(UnivPlanC *up) {
  assert(up->curExpr);
  dynamic_cast<univplan::UnivPlanBuilderMergeJoin *>(up->curNode.get())
      ->addJoinQual(std::move(up->curExpr));
}

void univPlanMergeJoinAddMergeClause(UnivPlanC *up) {
  assert(up->curExpr);
  dynamic_cast<univplan::UnivPlanBuilderMergeJoin *>(up->curNode.get())
      ->addMergeClause(std::move(up->curExpr));
}

int32_t univPlanHashNewInstance(UnivPlanC *up, int32_t pid) {
  up->curNode =
      univplan::PlanNodeUtil::createPlanBuilderNode(univplan::UNIVPLAN_HASH);
  up->curNode->pid = pid;
  up->curNode->uid = up->upb->getUid();
  return up->curNode->uid;
}

static univplan::UNIVPLANSHARETYPE shareTypeMapping(UnivPlanCShareType type) {
  switch (type) {
    case UnivPlanCShareType::UNIVPLAN_SHARE_NOTSHARED:
      return univplan::UNIVPLANSHARETYPE::SHARE_NOTSHARED;
    case UnivPlanCShareType::UNIVPLAN_SHARE_MATERIAL_XSLICE:
      return univplan::UNIVPLANSHARETYPE::SHARE_MATERIAL_XSLICE;
    default:
      LOG_INFO("ShareType(%d) not supported yet", type);
      return univplan::UNIVPLANSHARETYPE::SHARE_NOT_SUPPORTED;
  }
}

int32_t univPlanMaterialNewInstance(UnivPlanC *up, int32_t pid) {
  up->curNode = univplan::PlanNodeUtil::createPlanBuilderNode(
      univplan::UNIVPLAN_MATERIAL);
  up->curNode->pid = pid;
  up->curNode->uid = up->upb->getUid();
  return up->curNode->uid;
}

bool univPlanMaterialSetAttr(UnivPlanC *up, UnivPlanCShareType type,
                             bool cdbStrict, int32_t shareId,
                             int32_t driverSlice, int32_t nsharer,
                             int32_t xslice) {
  univplan::UNIVPLANSHARETYPE univplanType = shareTypeMapping(type);
  if (univplanType == univplan::UNIVPLANSHARETYPE::SHARE_NOT_SUPPORTED)
    return false;
  dynamic_cast<univplan::UnivPlanBuilderMaterial *>(up->curNode.get())
      ->setShareType(univplanType)
      .setCdbStrict(cdbStrict)
      .setShareId(shareId)
      .setDriverSlice(driverSlice)
      .setNSharer(nsharer)
      .setNSharerXSlice(xslice);
  return true;
}

int32_t univPlanShareInputScanNewInstance(UnivPlanC *up, int32_t pid) {
  up->curNode = univplan::PlanNodeUtil::createPlanBuilderNode(
      univplan::UNIVPLAN_SHAREINPUTSCAN);
  up->curNode->pid = pid;
  up->curNode->uid = up->upb->getUid();
  return up->curNode->uid;
}

bool univPlanShareInputScanSetAttr(UnivPlanC *up, UnivPlanCShareType type,
                                   int32_t shareId, int32_t driverSlice) {
  univplan::UNIVPLANSHARETYPE univplanType = shareTypeMapping(type);
  if (univplanType == univplan::UNIVPLANSHARETYPE::SHARE_NOT_SUPPORTED)
    return false;
  univplan::UnivPlanBuilderShareInputScan *bld =
      dynamic_cast<univplan::UnivPlanBuilderShareInputScan *>(
          up->curNode.get());
  bld->setShareType(univplanType);
  bld->setShareId(shareId);
  bld->setDriverSlice(driverSlice);
  return true;
}

int32_t univPlanResultNewInstance(UnivPlanC *up, int32_t pid) {
  up->curNode =
      univplan::PlanNodeUtil::createPlanBuilderNode(univplan::UNIVPLAN_RESULT);
  up->curNode->pid = pid;
  up->curNode->uid = up->upb->getUid();
  return up->curNode->uid;
}

void univPlanResultAddResConstantQual(UnivPlanC *up) {
  assert(up->curExpr);
  dynamic_cast<univplan::UnivPlanBuilderResult *>(up->curNode.get())
      ->addResConstantQual(std::move(up->curExpr));
}

int32_t univPlanSubqueryScanNewInstance(UnivPlanC *up, int32_t pid) {
  up->curNode = univplan::PlanNodeUtil::createPlanBuilderNode(
      univplan::UNIVPLAN_SUBQUERYSCAN);
  up->curNode->pid = pid;
  up->curNode->uid = up->upb->getUid();
  return up->curNode->uid;
}

void univPlanSubqueryScanAddSubPlan(UnivPlanC *up) {
  const univplan::UnivPlanPlanNodePoly &poly =
      up->upb->getPlanBuilderPlan()->getPlan()->plan();
  dynamic_cast<univplan::UnivPlanBuilderSubqueryScan *>(
      up->curNodeBak.top().get())
      ->setSubPlan(poly);
}

int32_t univPlanUniqueNewInstance(UnivPlanC *up, int32_t pid) {
  up->curNode =
      univplan::PlanNodeUtil::createPlanBuilderNode(univplan::UNIVPLAN_UNIQUE);
  up->curNode->pid = pid;
  up->curNode->uid = up->upb->getUid();
  return up->curNode->uid;
}
void univPlanUniqueSetNumGroupsAndUniqColIdxs(UnivPlanC *up, int64_t numCols,
                                              const int32_t *uniqColIdxs) {
  auto uniq =
      dynamic_cast<univplan::UnivPlanBuilderUnique *>(up->curNode.get());
  uniq->setUniqColIdxs(numCols, uniqColIdxs);
}

int32_t univPlanInsertNewInstance(UnivPlanC *up, int32_t pid) {
  up->curNode =
      univplan::PlanNodeUtil::createPlanBuilderNode(univplan::UNIVPLAN_INSERT);
  up->curNode->pid = pid;
  up->curNode->uid = up->upb->getUid();
  up->totalStageNo += 1;
  return up->curNode->uid;
}

void univPlanInsertSetRelId(UnivPlanC *up, uint32_t relId) {
  dynamic_cast<univplan::UnivPlanBuilderInsert *>(up->curNode.get())
      ->setInsertRelId(relId);
}

void univPlanInsertSetHasher(UnivPlanC *up, int32_t nDistKeyIndex,
                             int16_t *distKeyIndex, int32_t nRanges,
                             uint32_t *rangeToRgMap, int16_t nRg,
                             uint16_t *rgIds, const char **rgUrls) {
  dynamic_cast<univplan::UnivPlanBuilderInsert *>(up->curNode.get())
      ->setInsertHasher(nDistKeyIndex, distKeyIndex, nRanges, rangeToRgMap, nRg,
                        rgIds, rgUrls);
}

void univPlanAddToPlanNode(UnivPlanC *up, bool isLeft) {
  up->upb->addPlanNode(isLeft, std::move(up->curNode));
}

UnivPlanCatchedError *univPlanGetLastError(UnivPlanC *up) {
  return &(up->error);
}

void univPlanFixVarType(UnivPlanC *up) {
  univplan::VarUtil varUtil;
  varUtil.fixVarType(up->upb->getPlanBuilderPlan()->getPlan()->mutable_plan());
}

void univPlanStagize(UnivPlanC *up) {
  univplan::PlanStagizer stagizer;
  univplan::StagizerContext ctx;
  ctx.totalStageNo = up->totalStageNo;
  ctx.subplanStageNo = up->subplanStageNo;
  stagizer.stagize(up->upb->getPlanBuilderPlan()->getPlan(), &ctx);
  up->upb = std::move(ctx.upb);
}

void univPlanAddGuc(UnivPlanC *up, const char *name, const char *value) {
  up->upb->getPlanBuilderPlan()->addGuc(name, value);
}

const char *univPlanSerialize(UnivPlanC *up, int32_t *size, bool compress) {
  // compress
  if (compress) {
    std::string serializedPlan = up->upb->serialize();
    dbcommon::LZ4Compressor comp;
    comp.compress(serializedPlan.c_str(), serializedPlan.size(),
                  &up->serializedPlan);

  } else {
    up->serializedPlan = up->upb->serialize();
  }
  *size = up->serializedPlan.size();
  return up->serializedPlan.data();
}

const char *univPlanGetJsonFormatedPlan(UnivPlanC *up) {
  up->debugString = up->upb->getJsonFormatedPlan();
  return up->debugString.c_str();
}

void univPlanSetError(UnivPlanCatchedError *ce, int errCode,
                      const char *errMsg) {
  assert(ce != nullptr);
  ce->errCode = errCode;
  snprintf(ce->errMessage, strlen(errMsg) + 1, "%s", errMsg);
}

void univPlanNewExpr(UnivPlanC *up) {
  up->curExpr.reset(new univplan::UnivPlanBuilderExprTree);
}

void univPlanQualListAddExpr(UnivPlanC *up) {
  assert(up->curExpr);
  up->curNode->addQualList(std::move(up->curExpr));
}

void univPlanInitplanAddExpr(UnivPlanC *up) {
  assert(up->curExpr);
  up->curNode->addInitplan(std::move(up->curExpr));
}

void univPlanTargetListAddTargetEntry(UnivPlanC *up, bool resJunk) {
  assert(up->curExpr);
  univplan::UnivPlanBuilderTargetEntry::uptr te =
      up->curNode->addTargetEntryAndGetBuilder();
  te->setResJunk(resJunk);
  te->setExpr(std::move(up->curExpr));
}

void univPlanConnectorAddHashExpr(UnivPlanC *up) {
  assert(up->curExpr);
  // todo assert curNode is connector
  dynamic_cast<univplan::UnivPlanBuilderConnector *>(up->curNode.get())
      ->addHashExpr(std::move(up->curExpr));
}

void univPlanLimitAddLimitOffset(UnivPlanC *up) {
  assert(up->curExpr);
  dynamic_cast<univplan::UnivPlanBuilderLimit *>(up->curNode.get())
      ->setLimitOffset(std::move(up->curExpr));
}

void univPlanLimitAddLimitCount(UnivPlanC *up) {
  assert(up->curExpr);
  dynamic_cast<univplan::UnivPlanBuilderLimit *>(up->curNode.get())
      ->setLimitCount(std::move(up->curExpr));
}

int32_t univPlanExprAddConst(UnivPlanC *up, int32_t pid, int32_t type,
                             bool isNull, const char *buffer, int64_t typeMod) {
  assert(up->curExpr);
  auto constVal =
      up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderConst>(pid);
  constVal->setType(type).setTypeMod(typeMod).setIsNull(isNull);
  if (!isNull) constVal->setValue(buffer);

  int32_t uid = constVal->uid;
  up->curExpr->addExprNode(std::move(constVal));
  return uid;
}

int32_t univPlanExprAddVar(UnivPlanC *up, int32_t pid, uint32_t varNo,
                           int32_t varAttNo, int32_t typeId, int64_t typeMod) {
  assert(up->curExpr);
  auto var = up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderVar>(pid);
  var->setVarNo(varNo).setVarAttNo(varAttNo).setTypeId(typeId).setTypeMod(
      typeMod);

  int32_t uid = var->uid;
  up->curExpr->addExprNode(std::move(var));
  return uid;
}

int32_t univPlanExprAddOpExpr(UnivPlanC *up, int32_t pid, int32_t funcId) {
  assert(up->curExpr);
  auto opExpr =
      up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderOpExpr>(pid);
  opExpr->setFuncId(funcId);
  opExpr->setRetType(
      dbcommon::Func::instance()
          ->getFuncEntryById(static_cast<dbcommon::FuncKind>(funcId))
          ->retType);

  int32_t uid = opExpr->uid;
  up->curExpr->addExprNode(std::move(opExpr));
  return uid;
}

int32_t univPlanExprAddFuncExpr(UnivPlanC *up, int32_t pid, int32_t funcId) {
  assert(up->curExpr);
  auto funcExpr =
      up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderFuncExpr>(pid);
  funcExpr->setFuncId(funcId);
  funcExpr->setRetType(
      dbcommon::Func::instance()
          ->getFuncEntryById(static_cast<dbcommon::FuncKind>(funcId))
          ->retType);

  int32_t uid = funcExpr->uid;
  up->curExpr->addExprNode(std::move(funcExpr));
  return uid;
}

// Add type cast for final stage AGG function, in order to keep compatible with
// HAWQ's optimizer and executor
static int32_t univPlanAggrefAddFinalStageTypeCast(
    UnivPlanC *up, int32_t pid, const dbcommon::AggEntry *aggEnt) {
  if (aggEnt->aggFnId == dbcommon::FuncKind::AVG_SMALLINT ||
      aggEnt->aggFnId == dbcommon::FuncKind::AVG_INT ||
      aggEnt->aggFnId == dbcommon::FuncKind::AVG_BIGINT)
    return univPlanExprAddFuncExpr(up, pid,
                                   dbcommon::FuncKind::DOUBLE_TO_DECIMAL);
  if (aggEnt->aggFnId == dbcommon::FuncKind::SUM_BIGINT)
    return univPlanExprAddFuncExpr(up, pid,
                                   dbcommon::FuncKind::BIGINT_TO_DECIMAL);
  if (aggEnt->aggFnId == dbcommon::FuncKind::SUM_FLOAT)
    return univPlanExprAddFuncExpr(up, pid,
                                   dbcommon::FuncKind::DOUBLE_TO_FLOAT);
  return pid;
}

int32_t univPlanAggrefAddPartialStage(UnivPlanC *up, int32_t pid,
                                      int32_t funcId) {
  assert(up->curExpr);
  auto aggref =
      up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderAggref>(pid);
  const dbcommon::AggEntry *aggEntry =
      dbcommon::Func::instance()->getAggEntryById(
          static_cast<dbcommon::FuncKind>(funcId));
  dbcommon::TypeKind retType = dbcommon::Func::instance()
                                   ->getFuncEntryById(aggEntry->aggTransFnId)
                                   ->retType;
  aggref->setFuncId(funcId)
      .setTransFuncId(aggEntry->aggTransFnId)
      .setRetType(retType)
      .setTransInitVal(aggEntry->aggInitVal);

  int32_t uid = aggref->uid;
  up->curExpr->addExprNode(std::move(aggref));
  return uid;
}

int32_t univPlanAggrefAddIntermediateStage(UnivPlanC *up, int32_t pid,
                                           int32_t funcId) {
  assert(up->curExpr);
  auto aggref =
      up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderAggref>(pid);
  const dbcommon::AggEntry *aggEntry =
      dbcommon::Func::instance()->getAggEntryById(
          static_cast<dbcommon::FuncKind>(funcId));
  dbcommon::TypeKind retType = dbcommon::Func::instance()
                                   ->getFuncEntryById(aggEntry->aggPrelimFnId)
                                   ->retType;
  aggref->setFuncId(funcId)
      .setTransFuncId(aggEntry->aggPrelimFnId)
      .setFinalFuncId(dbcommon::FuncKind::FUNCINVALID)
      .setRetType(retType)
      .setTransInitVal(aggEntry->aggInitVal);

  int32_t uid = aggref->uid;
  up->curExpr->addExprNode(std::move(aggref));
  return uid;
}

int32_t univPlanAggrefAddFinalStage(UnivPlanC *up, int32_t pid,
                                    int32_t funcId) {
  assert(up->curExpr);
  auto aggref =
      up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderAggref>(pid);
  const dbcommon::AggEntry *aggEntry =
      dbcommon::Func::instance()->getAggEntryById(
          static_cast<dbcommon::FuncKind>(funcId));
  dbcommon::TypeKind retType =
      dbcommon::Func::instance()
          ->getFuncEntryById(aggEntry->aggFinalFn ==
                                     dbcommon::FuncKind::FUNCINVALID
                                 ? aggEntry->aggTransFnId
                                 : aggEntry->aggFinalFn)
          ->retType;
  aggref->setFuncId(funcId)
      .setTransFuncId(aggEntry->aggPrelimFnId)
      .setFinalFuncId(aggEntry->aggFinalFn)
      .setRetType(retType)
      .setTransInitVal(aggEntry->aggInitVal);
  if (aggEntry->aggFinalFn != dbcommon::FuncKind::FUNCINVALID)
    aggref->setRetType(dbcommon::Func::instance()
                           ->getFuncEntryById(aggEntry->aggFinalFn)
                           ->retType);

  int32_t uid = aggref->uid;
  aggref->pid = univPlanAggrefAddFinalStageTypeCast(up, pid, aggEntry);
  up->curExpr->addExprNode(std::move(aggref));
  return uid;
}

int32_t univPlanAggrefAddOneStage(UnivPlanC *up, int32_t pid, int32_t funcId) {
  assert(up->curExpr);
  auto aggref =
      up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderAggref>(pid);
  const dbcommon::AggEntry *aggEntry =
      dbcommon::Func::instance()->getAggEntryById(
          static_cast<dbcommon::FuncKind>(funcId));
  dbcommon::TypeKind retType =
      dbcommon::Func::instance()
          ->getFuncEntryById(aggEntry->aggFinalFn ==
                                     dbcommon::FuncKind::FUNCINVALID
                                 ? aggEntry->aggTransFnId
                                 : aggEntry->aggFinalFn)
          ->retType;
  aggref->setFuncId(funcId)
      .setTransFuncId(aggEntry->aggTransFnId)
      .setFinalFuncId(aggEntry->aggFinalFn)
      .setRetType(retType)
      .setTransInitVal(aggEntry->aggInitVal);

  int32_t uid = aggref->uid;
  aggref->pid = univPlanAggrefAddFinalStageTypeCast(up, pid, aggEntry);
  up->curExpr->addExprNode(std::move(aggref));
  return uid;
}

int32_t univPlanAggrefAddProxyVar(UnivPlanC *up, int32_t pid, int32_t varAttNo,
                                  int32_t funcId, int64_t typeMod) {
  assert(up->curExpr);
  auto var = up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderVar>(pid);
  const dbcommon::AggEntry *aggEntry =
      dbcommon::Func::instance()->getAggEntryById(
          static_cast<dbcommon::FuncKind>(funcId));
  dbcommon::TypeKind retType = dbcommon::Func::instance()
                                   ->getFuncEntryById(aggEntry->aggTransFnId)
                                   ->retType;
  var->setVarNo(OUTER_VAR).setVarAttNo(varAttNo).setTypeId(retType).setTypeMod(
      typeMod);

  int32_t uid = var->uid;
  up->curExpr->addExprNode(std::move(var));
  return uid;
}

int32_t univPlanExprAddParam(UnivPlanC *up, int32_t pid,
                             UnivplanParamKind paramKind, int32_t paramId,
                             int32_t typeId, int64_t typeMod) {
  assert(up->curExpr);
  auto param =
      up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderParam>(pid);
  param->setParamKind(static_cast<univplan::PARAMKIND>(paramKind))
      .setParamId(paramId)
      .setTypeId(typeId)
      .setTypeMod(typeMod);

  int32_t uid = param->uid;
  up->curExpr->addExprNode(std::move(param));
  return uid;
}

int32_t univPlanExprAddSubPlan(UnivPlanC *up, int32_t pid,
                               UnivplanSubLinkType sublinkType, int32_t planId,
                               int32_t stageNo, int32_t typeId, int64_t typeMod,
                               bool useHashTable, bool initPlan) {
  assert(up->curExpr);
  auto subplan =
      up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderSubPlan>(pid);
  subplan->setSubLinkType(static_cast<univplan::SUBLINKTYPE>(sublinkType))
      .setPlanId(planId)
      .setTypeId(typeId)
      .setTypeMod(typeMod)
      .setUseHashTable(useHashTable)
      .setInitPlan(initPlan);

  int32_t uid = subplan->uid;
  up->curExpr->addExprNode(std::move(subplan));
  if (initPlan) up->subplanStageNo[planId] = stageNo;
  return uid;
}
void univPlanExprAddSubPlanTestexpr(UnivPlanC *up, int32_t subplanId) {
  reinterpret_cast<univplan::UnivPlanBuilderSubPlan *>(
      up->curExpr->getExprNode(subplanId))
      ->setAddingTestexpr();
}

void univPlanAddTokenEntry(UnivPlanC *up, FileSystemCredentialCPtr tokenEntry) {
  std::string protocol(tokenEntry->key.protocol);
  std::string host(tokenEntry->key.host);
  int port = tokenEntry->key.port;
  std::string token(tokenEntry->credential);
  up->upb->getPlanBuilderPlan()->setTokenEntry(protocol, host, port, token);
}

void univPlanAddSnapshot(UnivPlanC *up, char *snapshot, int32_t snapshot_len) {
  std::string Snapshot(snapshot, snapshot_len);
  up->upb->getPlanBuilderPlan()->setSnapshot(Snapshot);
  // LOG_DEBUG("univplan c add snapshot: %s, size: %d", Snapshot.c_str(),
  //          snapshot_len);
}

void univPlanSubPlanAddSetParam(UnivPlanC *up, int32_t subplanId, int32_t num,
                                int32_t *setParam) {
  univplan::UnivPlanBuilderSubPlan *builder =
      reinterpret_cast<univplan::UnivPlanBuilderSubPlan *>(
          up->curExpr->getExprNode(subplanId));
  for (int32_t i = 0; i < num; ++i) {
    builder->addSetParam(setParam[i]);
  }
}

void univPlanSubPlanAddParParam(UnivPlanC *up, int32_t subplanId, int32_t num,
                                int32_t *parParam) {
  univplan::UnivPlanBuilderSubPlan *builder =
      reinterpret_cast<univplan::UnivPlanBuilderSubPlan *>(
          up->curExpr->getExprNode(subplanId));
  for (int32_t i = 0; i < num; ++i) {
    builder->addParParam(parParam[i]);
  }
}

void univPlanSubPlanAddTestexprParam(UnivPlanC *up, int32_t subplanId,
                                     int32_t num, int32_t *testexprParam) {
  univplan::UnivPlanBuilderSubPlan *builder =
      reinterpret_cast<univplan::UnivPlanBuilderSubPlan *>(
          up->curExpr->getExprNode(subplanId));
  for (int32_t i = 0; i < num; ++i) {
    builder->addTestexprParam(testexprParam[i]);
  }
}

int32_t univPlanExprAddBoolExpr(UnivPlanC *up, int32_t pid,
                                UnivplanBoolExprType boolExprType) {
  assert(up->curExpr);
  auto boolExpr =
      up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderBoolExpr>(pid);
  boolExpr->setType(static_cast<univplan::BOOLEXPRTYPE>(boolExprType));

  int32_t uid = boolExpr->uid;
  up->curExpr->addExprNode(std::move(boolExpr));
  return uid;
}
int32_t univPlanExprAddNullTestExpr(UnivPlanC *up, int32_t pid,
                                    UnivplanNullTestType nullTestType) {
  assert(up->curExpr);
  auto nullTest =
      up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderNullTest>(pid);
  nullTest->setType(static_cast<univplan::NULLTESTTYPE>(nullTestType));

  int32_t uid = nullTest->uid;
  up->curExpr->addExprNode(std::move(nullTest));
  return uid;
}

int32_t univPlanExprAddBoolTestExpr(UnivPlanC *up, int32_t pid,
                                    UnivplanBooleanTestType boolTestType) {
  assert(up->curExpr);
  auto boolTest =
      up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderBooleanTest>(pid);
  boolTest->setType(static_cast<univplan::BOOLTESTTYPE>(boolTestType));

  int32_t uid = boolTest->uid;
  up->curExpr->addExprNode(std::move(boolTest));
  return uid;
}

int32_t univPlanExprAddCaseExpr(UnivPlanC *up, int32_t pid, int32_t casetype) {
  auto caseexpr =
      up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderCaseExpr>(pid);
  caseexpr->setCasetype(casetype);

  int32_t uid = caseexpr->uid;
  up->curExpr->addExprNode(std::move(caseexpr));
  return uid;
}

void univPlanExprAddCaseExprDefresult(UnivPlanC *up, int32_t caseexpr_id) {
  reinterpret_cast<univplan::UnivPlanBuilderCaseExpr *>(
      up->curExpr->getExprNode(caseexpr_id))
      ->setAddingDefresult();
}

int32_t univPlanExprAddCaseWhen(UnivPlanC *up, int32_t pid) {
  auto casewhen =
      up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderCaseWhen>(pid);

  int32_t uid = casewhen->uid;
  up->curExpr->addExprNode(std::move(casewhen));
  return uid;
}

void univPlanExprAddCaseWhenExpr(UnivPlanC *up, int32_t casewhen_id) {}

void univPlanExprAddCaseWhenResult(UnivPlanC *up, int32_t casewhen_id) {
  reinterpret_cast<univplan::UnivPlanBuilderCaseWhen *>(
      up->curExpr->getExprNode(casewhen_id))
      ->setAddingResult();
}

int32_t univPlanExprAddScalarArrayOpExpr(UnivPlanC *up, int32_t pid,
                                         int32_t funcId, bool useOr) {
  assert(up->curExpr);
  auto opExpr =
      up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderScalarArrayOpExpr>(
          pid);
  opExpr->setFuncId(funcId).setUseOr(useOr);

  int32_t uid = opExpr->uid;
  up->curExpr->addExprNode(std::move(opExpr));
  return uid;
}

int32_t univPlanExprAddCoalesceExpr(UnivPlanC *up, int32_t pid,
                                    int32_t coalesceType,
                                    int32_t coalesceTypeMod) {
  auto coalesceExpr =
      up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderCoalesceExpr>(pid);
  coalesceExpr->setCoalesceType(coalesceType)
      .setCoalesceTypeMod(coalesceTypeMod);
  int32_t uid = coalesceExpr->uid;
  up->curExpr->addExprNode(std::move(coalesceExpr));
  return uid;
}

int32_t univPlanExprAddNullIfExpr(UnivPlanC *up, int32_t pid, int32_t funcId,
                                  int32_t retType, int32_t typeMod) {
  assert(up->curExpr);
  auto nullIfExpr =
      up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderNullIfExpr>(pid);
  nullIfExpr->setFuncId(funcId);
  nullIfExpr->setRetType(retType);
  nullIfExpr->setTypeMod(typeMod);

  int32_t uid = nullIfExpr->uid;
  up->curExpr->addExprNode(std::move(nullIfExpr));
  return uid;
}

int32_t univPlanExprAddDistinctExpr(UnivPlanC *up, int32_t pid,
                                    int32_t funcId) {
  assert(up->curExpr);
  auto distinctExpr =
      up->curExpr->ExprNodeFactory<univplan::UnivPlanBuilderDistinctExpr>(pid);
  distinctExpr->setFuncId(funcId);
  distinctExpr->setRetType(
      dbcommon::Func::instance()
          ->getFuncEntryById(static_cast<dbcommon::FuncKind>(funcId))
          ->retType);

  int32_t uid = distinctExpr->uid;
  up->curExpr->addExprNode(std::move(distinctExpr));
  return uid;
}

#ifdef __cplusplus
}
#endif
