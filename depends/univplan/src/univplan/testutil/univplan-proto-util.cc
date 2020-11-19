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

#include "univplan/testutil/univplan-proto-util.h"

#include <memory>
#include <string>

#include "dbcommon/function/func-kind.cg.h"

namespace univplan {

UnivPlanProtoUtility::UnivPlanProtoUtility() {
  this->univplan = univPlanNewInstance();
}

UnivPlanProtoUtility::~UnivPlanProtoUtility() {
  univPlanFreeInstance(&univplan);
}

void UnivPlanProtoUtility::constructVarOpConstExpr(
    int32_t pid, int32_t opFuncId, int32_t varNo, int32_t varAttNo,
    dbcommon::TypeKind varType, dbcommon::TypeKind constType,
    const char *buffer) {
  univPlanNewExpr(univplan);
  int32_t root = univPlanExprAddOpExpr(univplan, pid, opFuncId);
  univPlanExprAddVar(univplan, root, varNo, varAttNo, varType, -1);
  univPlanExprAddConst(univplan, root, constType, false, buffer, -1);
}

void UnivPlanProtoUtility::constructConstOpVarExpr(
    int32_t pid, int32_t opFuncId, int32_t varNo, int32_t varAttNo,
    dbcommon::TypeKind varType, dbcommon::TypeKind constType,
    const char *buffer) {
  univPlanNewExpr(univplan);
  int32_t root = univPlanExprAddOpExpr(univplan, pid, opFuncId);
  univPlanExprAddConst(univplan, root, constType, false, buffer, -1);
  univPlanExprAddVar(univplan, root, varNo, varAttNo, varType, -1);
}

void UnivPlanProtoUtility::constructVarOpVarExpr(
    int32_t pid, int32_t opFuncId, int32_t varNo1, int32_t varAttNo1,
    dbcommon::TypeKind varType1, int32_t varNo2, int32_t varAttNo2,
    dbcommon::TypeKind varType2) {
  univPlanNewExpr(univplan);
  int32_t root = univPlanExprAddOpExpr(univplan, pid, opFuncId);
  univPlanExprAddVar(univplan, root, varNo1, varAttNo1, varType1, -1);
  univPlanExprAddVar(univplan, root, varNo2, varAttNo2, varType2, -1);
}

void UnivPlanProtoUtility::constructConstOpConstExpr(
    int32_t pid, int32_t opFuncId, dbcommon::TypeKind constType1,
    const char *buffer1, dbcommon::TypeKind constType2, const char *buffer2) {
  univPlanNewExpr(univplan);
  int32_t root = univPlanExprAddOpExpr(univplan, pid, opFuncId);
  univPlanExprAddConst(univplan, root, constType1, false, buffer1, -1);
  univPlanExprAddConst(univplan, root, constType2, false, buffer2, -1);
}

void UnivPlanProtoUtility::constructFuncOpVarExpr(int32_t pid, int32_t opFuncId,
                                                  int32_t varNo,
                                                  int32_t varAttNo,
                                                  dbcommon::TypeKind varType,
                                                  int32_t mappingFuncId) {
  univPlanNewExpr(univplan);
  int32_t root = univPlanExprAddOpExpr(univplan, pid, opFuncId);
  univPlanExprAddFuncExpr(univplan, root, mappingFuncId);
  univPlanExprAddVar(univplan, root, varNo, varAttNo, varType, -1);
}

void UnivPlanProtoUtility::constructBoolExpr(
    int32_t pid, int32_t opFuncId1, int32_t varNo1, int32_t varAttNo1,
    dbcommon::TypeKind varType1, dbcommon::TypeKind constType1,
    const char *buffer1, int32_t opFuncId2, int32_t varNo2, int32_t varAttNo2,
    dbcommon::TypeKind varType2, dbcommon::TypeKind constType2,
    const char *buffer2, int8_t boolType) {
  univPlanNewExpr(univplan);
  int32_t root =
      univPlanExprAddBoolExpr(univplan, pid, UnivplanBoolExprType(boolType));

  int32_t root1 = univPlanExprAddOpExpr(univplan, root, opFuncId1);
  univPlanExprAddVar(univplan, root1, varNo1, varAttNo1, varType1, -1);
  univPlanExprAddConst(univplan, root1, constType1, false, buffer1, -1);

  int32_t root2 = univPlanExprAddOpExpr(univplan, root, opFuncId2);
  univPlanExprAddVar(univplan, root2, varNo2, varAttNo2, varType2, -1);
  univPlanExprAddConst(univplan, root2, constType2, false, buffer2, -1);
}

void UnivPlanProtoUtility::constructNullTestExpr(int32_t pid,
                                                 int8_t nulltesttype,
                                                 int32_t varNo,
                                                 int32_t varAttNo,
                                                 dbcommon::TypeKind varType) {
  univPlanNewExpr(univplan);
  int32_t root = univPlanExprAddNullTestExpr(
      univplan, pid, UnivplanNullTestType(nulltesttype));
  univPlanExprAddVar(univplan, root, varNo, varAttNo, varType, -1);
}

void UnivPlanProtoUtility::constructVarOpConstThenOpVarOpConstExpr(
    int32_t pid, int32_t opFuncId, int32_t opFuncId1, int32_t varNo1,
    int32_t varAttNo1, dbcommon::TypeKind varType1,
    dbcommon::TypeKind constType1, const char *buffer1, int32_t opFuncId2,
    int32_t varNo2, int32_t varAttNo2, dbcommon::TypeKind varType2,
    dbcommon::TypeKind constType2, const char *buffer2) {
  univPlanNewExpr(univplan);
  int32_t root = univPlanExprAddOpExpr(univplan, pid, opFuncId);

  int32_t root1 = univPlanExprAddOpExpr(univplan, root, opFuncId1);
  univPlanExprAddVar(univplan, root1, varNo1, varAttNo1, varType1, -1);
  univPlanExprAddConst(univplan, root1, constType1, false, buffer1, -1);

  int32_t root2 = univPlanExprAddOpExpr(univplan, root, opFuncId2);
  univPlanExprAddVar(univplan, root2, varNo2, varAttNo2, varType2, -1);
  univPlanExprAddConst(univplan, root2, constType2, false, buffer2, -1);
}

void UnivPlanProtoUtility::constructVarTargetEntry(int32_t pid, int32_t varNo,
                                                   int32_t varAttNo,
                                                   dbcommon::TypeKind type) {
  univPlanNewExpr(univplan);
  univPlanExprAddVar(univplan, pid, varNo, varAttNo, type, -1);
  univPlanTargetListAddTargetEntry(univplan, false);
}

void UnivPlanProtoUtility::constructConstTargetEntry(
    int32_t pid, dbcommon::TypeKind constType, const char *buffer) {
  univPlanNewExpr(univplan);
  if (constType == dbcommon::TypeKind::ANYID)
    univPlanExprAddConst(univplan, -1, dbcommon::TypeKind::TINYINTID, true,
                         buffer, -1);
  else
    univPlanExprAddConst(univplan, -1, constType, false, buffer, -1);
  univPlanTargetListAddTargetEntry(univplan, false);
}

void UnivPlanProtoUtility::constructConstOpConstTargetEntry(
    int32_t pid, int32_t opFuncId, dbcommon::TypeKind constType1,
    const char *buffer1, dbcommon::TypeKind constType2, const char *buffer2) {
  univPlanNewExpr(univplan);
  constructConstOpConstExpr(pid, opFuncId, constType1, buffer1, constType2,
                            buffer2);
  univPlanTargetListAddTargetEntry(univplan, false);
}

void UnivPlanProtoUtility::constructAggRefTargetEntry(
    int32_t pid, int8_t aggstage, int32_t mappingFuncId, int32_t varNo,
    int32_t varAttNo, dbcommon::TypeKind type) {
  univPlanNewExpr(univplan);
  int32_t uid;
  if (aggstage == 1)  // AGGSTAGE_PARTIAL
    uid = univPlanAggrefAddPartialStage(univplan, -1, mappingFuncId);
  else if (aggstage == 3)  // AGGSTAGE_FINAL
    uid = univPlanAggrefAddFinalStage(univplan, -1, mappingFuncId);
  univPlanAggrefAddProxyVar(univplan, uid, varAttNo, mappingFuncId, -1);
  univPlanTargetListAddTargetEntry(univplan, false);
}

void UnivPlanProtoUtility::constructNullTestTargetEntry(
    int32_t pid, int8_t nulltesttype, int32_t varNo, int32_t varAttNo,
    dbcommon::TypeKind varType) {
  constructNullTestExpr(pid, nulltesttype, varNo, varAttNo, varType);
  univPlanTargetListAddTargetEntry(univplan, false);
}

void UnivPlanProtoUtility::constructVarOpConstTargetEntry(
    int32_t pid, int32_t opFuncId, int32_t varNo, int32_t varAttNo,
    dbcommon::TypeKind varType, dbcommon::TypeKind constType,
    const char *buffer) {
  constructVarOpConstExpr(pid, opFuncId, varNo, varAttNo, varType, constType,
                          buffer);
  univPlanTargetListAddTargetEntry(univplan, false);
}

void UnivPlanProtoUtility::constructVarOpConstQualList(
    int32_t pid, int32_t opFuncId, int32_t varNo, int32_t varAttNo,
    dbcommon::TypeKind varType, dbcommon::TypeKind constType,
    const char *buffer) {
  constructVarOpConstExpr(pid, opFuncId, varNo, varAttNo, varType, constType,
                          buffer);
  univPlanQualListAddExpr(univplan);
}

void UnivPlanProtoUtility::constructConstOpVarQualList(
    int32_t pid, int32_t opFuncId, int32_t varNo, int32_t varAttNo,
    dbcommon::TypeKind varType, dbcommon::TypeKind constType,
    const char *buffer) {
  constructConstOpVarExpr(pid, opFuncId, varNo, varAttNo, varType, constType,
                          buffer);
  univPlanQualListAddExpr(univplan);
}

void UnivPlanProtoUtility::constructVarOpVarQualList(
    int32_t pid, int32_t opFuncId, int32_t varNo1, int32_t varAttNo1,
    dbcommon::TypeKind varType1, int32_t varNo2, int32_t varAttNo2,
    dbcommon::TypeKind varType2) {
  constructVarOpVarExpr(pid, opFuncId, varNo1, varAttNo1, varType1, varNo2,
                        varAttNo2, varType2);
  univPlanQualListAddExpr(univplan);
}

void UnivPlanProtoUtility::constructFuncOpVarQualList(
    int32_t pid, int32_t opFuncId, int32_t varNo, int32_t varAttNo,
    dbcommon::TypeKind varType, int32_t mappingFuncId) {
  constructFuncOpVarExpr(pid, opFuncId, varNo, varAttNo, varType,
                         mappingFuncId);
  univPlanQualListAddExpr(univplan);
}

void UnivPlanProtoUtility::constructBoolQualList(
    int32_t pid, int32_t opFuncId1, int32_t varNo1, int32_t varAttNo1,
    dbcommon::TypeKind varType1, dbcommon::TypeKind constType1,
    const char *buffer1, int32_t opFuncId2, int32_t varNo2, int32_t varAttNo2,
    dbcommon::TypeKind varType2, dbcommon::TypeKind constType2,
    const char *buffer2, int8_t boolType) {
  constructBoolExpr(pid, opFuncId1, varNo1, varAttNo1, varType1, constType1,
                    buffer1, opFuncId2, varNo2, varAttNo2, varType2, constType2,
                    buffer2, boolType);
  univPlanQualListAddExpr(univplan);
}

void UnivPlanProtoUtility::constructNullTestQualList(
    int32_t pid, int8_t nulltesttype, int32_t varNo, int32_t varAttNo,
    dbcommon::TypeKind varType) {
  constructNullTestExpr(pid, nulltesttype, varNo, varAttNo, varType);
  univPlanQualListAddExpr(univplan);
}

void UnivPlanProtoUtility::constructVarOpConstThenOpVarOpConstQualList(
    int32_t pid, int32_t opFuncId, int32_t opFuncId1, int32_t varNo1,
    int32_t varAttNo1, dbcommon::TypeKind varType1,
    dbcommon::TypeKind constType1, const char *buffer1, int32_t opFuncId2,
    int32_t varNo2, int32_t varAttNo2, dbcommon::TypeKind varType2,
    dbcommon::TypeKind constType2, const char *buffer2) {
  constructVarOpConstThenOpVarOpConstExpr(
      pid, opFuncId, opFuncId1, varNo1, varAttNo1, varType1, constType1,
      buffer1, opFuncId2, varNo2, varAttNo2, varType2, constType2, buffer2);
  univPlanQualListAddExpr(univplan);
}

int32_t UnivPlanProtoUtility::constructSeqScan(int32_t pid, bool withQualList,
                                               int8_t targetEntryType) {
  int32_t uid = ::univPlanSeqScanNewInstance(univplan, pid);

  constructVarTargetEntry(-1, 1, 1, dbcommon::TypeKind::INTID);
  if (targetEntryType == 0) {
    constructVarTargetEntry(-1, 1, 3, dbcommon::TypeKind::STRINGID);
  } else if (targetEntryType == 1) {
    constructVarTargetEntry(-1, 1, 2, dbcommon::TypeKind::INTID);
    constructVarTargetEntry(-1, 1, 3, dbcommon::TypeKind::STRINGID);
  } else if (targetEntryType == 2) {
    constructNullTestTargetEntry(-1, 1, 1, 3, dbcommon::TypeKind::STRINGID);
  } else if (targetEntryType == 3) {
    constructVarOpConstTargetEntry(-1, dbcommon::INT_ADD_INT, 1, 1,
                                   dbcommon::TypeKind::INTID,
                                   dbcommon::TypeKind::INTID, "1");
  } else if (targetEntryType == 4) {
    constructVarOpConstTargetEntry(-1, dbcommon::STRING_EQUAL_STRING, 1, 3,
                                   dbcommon::TypeKind::STRINGID,
                                   dbcommon::TypeKind::STRINGID, "1");
  } else if (targetEntryType == 5) {
    constructConstTargetEntry(-1, dbcommon::TypeKind::BIGINTID, "1");
  } else if (targetEntryType == 6) {
    constructConstTargetEntry(-1, dbcommon::TypeKind::ANYID, "");
  } else if (targetEntryType == 7) {
    constructVarOpConstTargetEntry(-1, dbcommon::INT_ADD_INT, 1, 1,
                                   dbcommon::TypeKind::INTID,
                                   dbcommon::TypeKind::INTID, "1");
    constructVarOpConstTargetEntry(-1, dbcommon::STRING_EQUAL_STRING, 1, 3,
                                   dbcommon::TypeKind::STRINGID,
                                   dbcommon::TypeKind::STRINGID, "1");
  } else if (targetEntryType == 8) {
    constructVarTargetEntry(-1, 1, 3, dbcommon::TypeKind::STRINGID);
    constructVarTargetEntry(-1, 1, 2, dbcommon::TypeKind::INTID);
  }

  if (withQualList) {
    constructVarOpConstQualList(-1, dbcommon::INT_GREATER_THAN_INT, 1, 2,
                                dbcommon::TypeKind::INTID,
                                dbcommon::TypeKind::INTID, "1");
    constructVarOpVarQualList(-1, dbcommon::BIGINT_EQUAL_INT, 1, 1,
                              dbcommon::TypeKind::INTID, 1, 2,
                              dbcommon::TypeKind::INTID);
    constructFuncOpVarQualList(-1, dbcommon::DOUBLE_LESS_THAN_DOUBLE, 1, 1,
                               dbcommon::TypeKind::INTID,
                               dbcommon::TINYINT_LESS_THAN_TINYINT);
    constructBoolQualList(
        -1, dbcommon::BIGINT_GREATER_THAN_INT, 1, 1, dbcommon::TypeKind::INTID,
        dbcommon::TypeKind::INTID, "1", dbcommon::INT_LESS_THAN_INT, 1, 2,
        dbcommon::TypeKind::INTID, dbcommon::TypeKind::INTID, "10");
  }

  univPlanSeqScanSetRelId(univplan, 1);

  int32_t numCols = 2;
  int32_t *columnsToRead =
      reinterpret_cast<int32_t *>(malloc(numCols * sizeof(int32_t)));
  columnsToRead[0] = 0;
  columnsToRead[1] = 1;
  univPlanSeqScanSetColumnsToRead(univplan, numCols, columnsToRead);
  free(columnsToRead);

  int64_t start[2] = {1, 2};
  int64_t len[2] = {200, 100};
  std::string fileName_buf[2] = {"/a", "/b"};
  std::unique_ptr<const char *[]> fileName(new const char *[2]);
  fileName[0] = fileName_buf[0].c_str();
  fileName[1] = fileName_buf[1].c_str();
  univPlanSeqScanAddTaskWithFileSplits(false, univplan, 1, fileName.get(),
                                       start, len, NULL, NULL);
  univPlanSeqScanAddTaskWithFileSplits(false, univplan, 2, fileName.get(),
                                       start, len, NULL, NULL);

  univPlanAddToPlanNode(univplan, true);
  return uid;
}

int32_t UnivPlanProtoUtility::constructConnector(int32_t pid, int8_t ctype,
                                                 bool allTargetEntry) {
  int32_t uid = univPlanConnectorNewInstance(univplan, pid);
  univPlanConnectorSetType(univplan, ConnectorType(ctype));

  constructVarTargetEntry(-1, 1, 1, dbcommon::TypeKind::INTID);
  if (allTargetEntry)
    constructVarTargetEntry(-1, 1, 2, dbcommon::TypeKind::INTID);
  constructVarTargetEntry(-1, 1, 3, dbcommon::TypeKind::STRINGID);

  if (ctype == 0) {
    univPlanNewExpr(univplan);
    univPlanExprAddVar(univplan, -1, 1, 0, dbcommon::TypeKind::INTID, -1);
    univPlanConnectorAddHashExpr(univplan);
  }

  univPlanAddToPlanNode(univplan, true);
  return uid;
}

int32_t UnivPlanProtoUtility::constructSortConnector(int32_t pid,
                                                     int8_t ctype) {
  int32_t uid = univPlanConnectorNewInstance(univplan, pid);
  univPlanConnectorSetType(univplan, ConnectorType(ctype));

  constructVarTargetEntry(-1, 1, 1, dbcommon::TypeKind::INTID);
  constructVarTargetEntry(-1, 1, 3, dbcommon::TypeKind::STRINGID);

  int64_t numCols = 2;
  int32_t *colIdx =
      reinterpret_cast<int32_t *>(malloc(numCols * sizeof(int32_t)));
  int32_t *mappingSortFuncId =
      reinterpret_cast<int32_t *>(malloc(numCols * sizeof(int32_t)));
  colIdx[0] = 1;
  colIdx[1] = 2;
  mappingSortFuncId[0] = dbcommon::BIGINT_LESS_THAN_BIGINT;
  mappingSortFuncId[1] = dbcommon::INT_LESS_THAN_INT;
  univPlanConnectorSetColIdx(univplan, numCols, colIdx);
  univPlanConnectorSetSortFuncId(univplan, numCols, mappingSortFuncId);
  free(colIdx);
  free(mappingSortFuncId);

  univPlanAddToPlanNode(univplan, true);
  return uid;
}

int32_t UnivPlanProtoUtility::constructLimitBelow(int32_t pid, int64_t count,
                                                  int64_t offset) {
  int32_t uid = univPlanLimitNewInstance(univplan, pid);

  constructVarTargetEntry(-1, 1, 1, dbcommon::TypeKind::INTID);
  constructVarTargetEntry(-1, 1, 2, dbcommon::TypeKind::INTID);
  constructVarTargetEntry(-1, 1, 3, dbcommon::TypeKind::STRINGID);

  univPlanSetPlanNodeInfo(univplan, static_cast<double>(count + offset), 0, 0);

  std::string countTmp = std::to_string(count);
  char const *countBuf = countTmp.c_str();
  std::string offsetTmp = std::to_string(offset);
  char const *offsetBuf = offsetTmp.c_str();
  if (count != -1) {
    constructConstOpConstExpr(-1, dbcommon::BIGINT_ADD_BIGINT,
                              dbcommon::BIGINTID, countBuf, dbcommon::BIGINTID,
                              offsetBuf);
    univPlanLimitAddLimitCount(univplan);
  }

  if (offset != -1) {
    univPlanNewExpr(univplan);
    univPlanExprAddConst(univplan, -1, dbcommon::BIGINTID, false, "0", -1);
    univPlanLimitAddLimitOffset(univplan);
  }

  univPlanAddToPlanNode(univplan, true);
  return uid;
}

int32_t UnivPlanProtoUtility::constructLimitTop(int32_t pid, int64_t count,
                                                int64_t offset) {
  int32_t uid = univPlanLimitNewInstance(univplan, pid);

  constructVarTargetEntry(-1, 1, 1, dbcommon::TypeKind::INTID);
  constructVarTargetEntry(-1, 1, 2, dbcommon::TypeKind::INTID);
  constructVarTargetEntry(-1, 1, 3, dbcommon::TypeKind::STRINGID);

  univPlanSetPlanNodeInfo(univplan, static_cast<double>(count + offset), 0, 0);

  if (count != -1) {
    univPlanNewExpr(univplan);
    std::string countTmp = std::to_string(count);
    char const *countBuf = countTmp.c_str();
    univPlanExprAddConst(univplan, -1, dbcommon::BIGINTID, false, countBuf, -1);
    univPlanLimitAddLimitCount(univplan);
  }

  if (offset != -1) {
    univPlanNewExpr(univplan);
    std::string offsetTmp = std::to_string(offset);
    char const *offsetBuf = offsetTmp.c_str();
    univPlanExprAddConst(univplan, -1, dbcommon::BIGINTID, false, offsetBuf,
                         -1);
    univPlanLimitAddLimitOffset(univplan);
  }

  univPlanAddToPlanNode(univplan, true);
  return uid;
}

int32_t UnivPlanProtoUtility::constructAgg(int32_t pid, int8_t aggstage) {
  int32_t uid = univPlanAggNewInstance(univplan, pid);

  constructAggRefTargetEntry(-1, aggstage, dbcommon::COUNT, 1, 1,
                             dbcommon::TypeKind::INTID);
  constructVarTargetEntry(-1, 1, 2, dbcommon::TypeKind::INTID);
  constructVarTargetEntry(-1, 1, 3, dbcommon::TypeKind::STRINGID);

  int64_t numCols = 2;
  int32_t *grpColIdx =
      reinterpret_cast<int32_t *>(malloc(numCols * sizeof(int32_t)));
  grpColIdx[0] = 1;
  grpColIdx[1] = 3;
  univPlanAggSetNumGroupsAndGroupColIndexes(univplan, 1000, numCols, grpColIdx);
  univPlanAggSetAggstrategy(univplan, UNIVPLAN_AGG_HASHED);
  free(grpColIdx);

  univPlanAddToPlanNode(univplan, true);
  return uid;
}

int32_t UnivPlanProtoUtility::constructSort(int32_t pid) {
  int32_t uid = univPlanSortNewInstance(univplan, pid);

  int64_t numCols = 2;
  int32_t *colIdx =
      reinterpret_cast<int32_t *>(malloc(numCols * sizeof(int32_t)));
  int32_t *mappingSortFuncId =
      reinterpret_cast<int32_t *>(malloc(numCols * sizeof(int32_t)));
  colIdx[0] = 1;
  colIdx[1] = 2;
  mappingSortFuncId[0] = dbcommon::BIGINT_LESS_THAN_BIGINT;
  mappingSortFuncId[1] = dbcommon::INT_LESS_THAN_INT;
  univPlanSortSetColIdx(univplan, numCols, colIdx);
  univPlanSortSetSortFuncId(univplan, numCols, mappingSortFuncId);
  free(colIdx);
  free(mappingSortFuncId);

  univPlanAddToPlanNode(univplan, true);
  return uid;
}

void UnivPlanProtoUtility::constructRangeTable() {
  univPlanRangeTblEntryAddDummy(univplan);

  char location[] = "/tmp";
  char fmtOptsJson[] = "option1 string in json";
  int32_t columnNum = 2;
  int32_t columnDataType[2] = {dbcommon::TypeKind::INTID,
                               dbcommon::TypeKind::STRINGID};
  int64_t columnDataTypeMod[2] = {-1, -1};
  std::string columnName_buf[2] = {"p1", "p2"};
  std::unique_ptr<const char *[]> columnName(new const char *[2]);
  columnName[0] = columnName_buf[0].c_str();
  columnName[1] = columnName_buf[1].c_str();

  univPlanRangeTblEntryAddTable(univplan, 2, UnivPlanOrcFormat, location,
                                fmtOptsJson, columnNum, columnName.get(),
                                columnDataType, columnDataTypeMod, nullptr);
}

void UnivPlanProtoUtility::constructReceiver() {
  std::string addr_buf1[1] = {"mdw"};
  std::unique_ptr<const char *[]> addr1(new const char *[1]);
  addr1[0] = addr_buf1[0].c_str();
  int32_t port1[2] = {101};

  std::string addr_buf2[2] = {"smdw", "smdw"};
  std::unique_ptr<const char *[]> addr2(new const char *[2]);
  addr2[0] = addr_buf2[0].c_str();
  addr2[1] = addr_buf2[1].c_str();
  int32_t port2[2] = {201, 203};

  univPlanReceiverAddListeners(univplan, 1, addr1.get(), port1);
  univPlanReceiverAddListeners(univplan, 2, addr2.get(), port2);
}

}  // namespace univplan
