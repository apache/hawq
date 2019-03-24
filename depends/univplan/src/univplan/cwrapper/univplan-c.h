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

#ifndef UNIVPLAN_SRC_UNIVPLAN_CWRAPPER_UNIVPLAN_C_H_
#define UNIVPLAN_SRC_UNIVPLAN_CWRAPPER_UNIVPLAN_C_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef ERROR_MESSAGE_BUFFER_SIZE
#define ERROR_MESSAGE_BUFFER_SIZE 4096
#endif

struct UnivPlanC;

typedef struct UnivPlanC UnivPlanC;

typedef struct FileSystemCredentialKeyC {
  char *protocol;
  char *host;
  int port;
} FileSystemCredentialKeyC;

typedef struct FileSystemCredentialC {
  struct FileSystemCredentialKeyC key;
  char *credential;
} FileSystemCredentialC;

typedef struct FileSystemCredentialC *FileSystemCredentialCPtr;

typedef struct UnivPlanCatchedError {
  int errCode;
  char errMessage[ERROR_MESSAGE_BUFFER_SIZE];
} UnivPlanCatchedError;

UnivPlanC *univPlanNewInstance();
void univPlanFreeInstance(UnivPlanC **up);

void univPlanNewSubPlanNode(UnivPlanC *up);
void univPlanFreeSubPlanNode(UnivPlanC *up);

// fill RangeTblEntry
typedef enum FormatType {
  UnivPlanTextFormat,
  UnivPlanCsvFormat,
  UnivPlanOrcFormat,
  UnivPlanMagmaFormat
} FormatType;
void univPlanRangeTblEntryAddTable(UnivPlanC *up, uint64_t tid,
                                   FormatType format, const char *location,
                                   const char *optStrInJson, uint32_t columnNum,
                                   const char **columnName,
                                   int32_t *columnDataType,
                                   int64_t *columnDataTypeMod);
void univPlanRangeTblEntryAddDummy(UnivPlanC *up);

// construct interconnect info
void univPlanReceiverAddListeners(UnivPlanC *up, uint32_t listenerNum,
                                  const char **addr, int32_t *port);

// add param info
void univPlanAddParamInfo(UnivPlanC *up, int32_t type, bool isNull,
                          const char *buffer);

void univPlanSetDoInstrument(UnivPlanC *up, bool doInstrument);

void univPlanSetNCrossLevelParams(UnivPlanC *up, int32_t nCrossLevelParams);

typedef enum UnivPlanCCmdType {
  UNIVPLAN_CMD_UNKNOWN,
  UNIVPLAN_CMD_SELECT,
  UNIVPLAN_CMD_UPDATE,
  UNIVPLAN_CMD_INSERT,
  UNIVPLAN_CMD_DELETE,
  UNIVPLAN_CMD_UTILITY,
  UNIVPLAN_CMD_NOTHING
} UnivPlanCCmdType;
void univPlanSetCmdType(UnivPlanC *up, UnivPlanCCmdType type);

void univPlanSetPlanNodeInfo(UnivPlanC *up, double planRows,
                             int32_t planRowWidth, uint64_t operatorMemKB);

/*
 * the expr tree must be built before adding targetlist/qualist
 */
void univPlanQualListAddExpr(UnivPlanC *up);
void univPlanInitplanAddExpr(UnivPlanC *up);
void univPlanTargetListAddTargetEntry(UnivPlanC *up, bool resJunk);
void univPlanConnectorAddHashExpr(UnivPlanC *up);
void univPlanConnectorSetRangeVsegMap(UnivPlanC *up, int *map, bool magmaTable);
// set magma range num
void univPlanSetRangeNum(UnivPlanC *up, int rangeNum);

// construct Connector
typedef enum ConnectorType {
  UnivPlanShuffle,
  UnivPlanBroadcast,
  UnivPlanConverge
} ConnectorType;
int32_t univPlanConnectorNewInstance(UnivPlanC *up, int32_t);
void univPlanConnectorSetType(UnivPlanC *up, ConnectorType type);
void univPlanConnectorSetStageNo(UnivPlanC *up, int32_t stageNo);
void univPlanConnectorSetColIdx(UnivPlanC *up, int64_t numCols,
                                const int32_t *colIdx);
void univPlanConnectorSetSortFuncId(UnivPlanC *up, int64_t numCols,
                                    const int32_t *sortFuncId);

// construct ExtScan
int32_t univPlanExtScanNewInstance(UnivPlanC *up, int32_t pid);
void univPlanExtScanSetRelId(UnivPlanC *up, uint32_t relId);
void univPlanExtScanSetColumnsToRead(UnivPlanC *up, int64_t numCols,
                                     const int32_t *columnsToRead);
// construct magma index info
void univPlanExtScanSetIndex(UnivPlanC *up, bool index);
void univPlanExtScanSetScanType(UnivPlanC *up, int type);
void univPlanExtScanDirection(UnivPlanC *up, int direction);
void univPlanExtScanSetIndexName(UnivPlanC *up, const char *indexName);
void univPlanIndexQualListAddExpr(UnivPlanC *up);

/*void univPlanExtScanAddTaskWithFileSplits(UnivPlanC *up, uint32_t
   fileSplitNum, int64_t *lbLen, int64_t *ubLen, const char **lowerBound, const
   char **upperBound);
*/
// construct SeqScan
int32_t univPlanSeqScanNewInstance(UnivPlanC *up, int32_t pid);
void univPlanSeqScanSetRelId(UnivPlanC *up, uint32_t relId);
void univPlanSeqScanSetReadStatsOnly(UnivPlanC *up, bool readStatsOnly);
void univPlanSeqScanSetColumnsToRead(UnivPlanC *up, int64_t numCols,
                                     const int32_t *columnsToRead);
void univPlanSeqScanAddTaskWithFileSplits(bool isMagma, UnivPlanC *up,
                                          uint32_t fileSplitNum,
                                          const char **fileName, int64_t *start,
                                          int64_t *len, int32_t *rangeid,
                                          int32_t *rgid);

// construct Agg
int32_t univPlanAggNewInstance(UnivPlanC *up, int32_t pid);
void univPlanAggSetNumGroupsAndGroupColIndexes(UnivPlanC *up, int64_t numGroups,
                                               int64_t numCols,
                                               const int32_t *grpColIdx);

// construct Sort
int32_t univPlanSortNewInstance(UnivPlanC *up, int32_t pid);
void univPlanSortSetColIdx(UnivPlanC *up, int64_t numCols,
                           const int32_t *colIdx);
void univPlanSortSetSortFuncId(UnivPlanC *up, int64_t numCols,
                               const int32_t *sortFuncId);
void univPlanSortAddLimitOffset(UnivPlanC *up);
void univPlanSortAddLimitCount(UnivPlanC *up);

// construct Limit
int32_t univPlanLimitNewInstance(UnivPlanC *up, int32_t pid);
void univPlanLimitAddLimitOffset(UnivPlanC *up);
void univPlanLimitAddLimitCount(UnivPlanC *up);

// construct append
int32_t univPlanAppendNewInstance(UnivPlanC *up, int32_t pid);
void univPlanAppendAddAppendPlan(UnivPlanC *up);

typedef enum UnivPlanCJoinType {
  UNIVPLAN_JOIN_INNER,
  UNIVPLAN_JOIN_LEFT,
  UNIVPLAN_JOIN_RIGHT,
  UNIVPLAN_JOIN_FULL,
  UNIVPLAN_JOIN_IN,
  UNIVPLAN_JOIN_LASJ = 8,
  UNIVPLAN_JOIN_LASJ_NOTIN = 9
} UnivPlanCJoinType;
// construct nestloop
int32_t univPlanNestLoopNewInstance(UnivPlanC *up, int32_t pid);
bool univPlanNestLoopSetType(UnivPlanC *up, UnivPlanCJoinType type);
void univPlanNestLoopAddJoinQual(UnivPlanC *up);
// construct hashjoin
int32_t univPlanHashJoinNewInstance(UnivPlanC *up, int32_t pid);
bool univPlanHashJoinSetType(UnivPlanC *up, UnivPlanCJoinType type);
void univPlanHashJoinAddJoinQual(UnivPlanC *up);
void univPlanHashJoinAddHashClause(UnivPlanC *up);
void univPlanHashJoinAddHashQualClause(UnivPlanC *up);
// construct mergejoin
int32_t univPlanMergeJoinNewInstance(UnivPlanC *up, int32_t pid);
bool univPlanMergeJoinSetType(UnivPlanC *up, UnivPlanCJoinType type);
void univPlanMergeJoinAddJoinQual(UnivPlanC *up);
void univPlanMergeJoinAddMergeClause(UnivPlanC *up);

// construct hash
int32_t univPlanHashNewInstance(UnivPlanC *up, int32_t pid);

typedef enum UnivPlanCShareType {
  UNIVPLAN_SHARE_NOTSHARED,
  UNIVPLAN_SHARE_MATERIAL,
  UNIVPLAN_SHARE_MATERIAL_XSLICE,
  UNIVPLAN_SHARE_SORT,
  UNIVPLAN_SHARE_SORT_XSLICE
} UnivPlanCShareType;
// construct material
int32_t univPlanMaterialNewInstance(UnivPlanC *up, int32_t pid);
bool univPlanMaterialSetAttr(UnivPlanC *up, UnivPlanCShareType type,
                             bool cdbStrict, int32_t shareId,
                             int32_t driverSlice, int32_t nsharer,
                             int32_t xslice);

// construct shareinputscan
int32_t univPlanShareInputScanNewInstance(UnivPlanC *up, int32_t pid);
bool univPlanShareInputScanSetAttr(UnivPlanC *up, UnivPlanCShareType type,
                                   int32_t shareId, int32_t driverSlice);

// construct result
int32_t univPlanResultNewInstance(UnivPlanC *up, int32_t pid);
void univPlanResultAddResConstantQual(UnivPlanC *up);

// construct subqueryscan
int32_t univPlanSubqueryScanNewInstance(UnivPlanC *up, int32_t pid);
void univPlanSubqueryScanAddSubPlan(UnivPlanC *up);

// construct Unique
int32_t univPlanUniqueNewInstance(UnivPlanC *up, int32_t pid);
void univPlanUniqueSetNumGroupsAndUniqColIdxs(UnivPlanC *up, int64_t numCols,
                                              const int32_t *uniqColIdxs);

// construct Insert
int32_t univPlanInsertNewInstance(UnivPlanC *up, int32_t pid);
void univPlanInsertSetRelId(UnivPlanC *up, uint32_t relId);

void univPlanAddToPlanNode(UnivPlanC *up, bool isLeft);

void univPlanFixVarType(UnivPlanC *up);

void univPlanStagize(UnivPlanC *up);

void univPlanAddGuc(UnivPlanC *up, const char *name, const char *value);

const char *univPlanSerialize(UnivPlanC *up, int32_t *size, bool compress);

UnivPlanCatchedError *univPlanGetLastError(UnivPlanC *up);

// call before add expr node
void univPlanNewExpr(UnivPlanC *up);

// following functions return the id of the new expr node in the expr tree
// which will be used as the pid for adding its argument in the expr tree
int32_t univPlanExprAddConst(UnivPlanC *up, int32_t pid, int32_t type,
                             bool isNull, const char *buffer, int64_t typeMod);
int32_t univPlanExprAddVar(UnivPlanC *up, int32_t pid, uint32_t varNo,
                           int32_t varAttNo, int32_t typeId, int64_t typeMod);
int32_t univPlanExprAddOpExpr(UnivPlanC *up, int32_t pid, int32_t funcId);
int32_t univPlanExprAddFuncExpr(UnivPlanC *up, int32_t pid, int32_t funcId);
int32_t univPlanAggrefAddPartialStage(UnivPlanC *up, int32_t pid,
                                      int32_t funcId);
int32_t univPlanAggrefAddIntermediateStage(UnivPlanC *up, int32_t pid,
                                           int32_t funcId);
int32_t univPlanAggrefAddFinalStage(UnivPlanC *up, int32_t pid, int32_t funcId);
int32_t univPlanAggrefAddOneStage(UnivPlanC *up, int32_t pid, int32_t funcId);
int32_t univPlanAggrefAddProxyVar(UnivPlanC *up, int32_t pid, int32_t varAttNo,
                                  int32_t funcId, int64_t typeMod);
typedef enum { UNIVPLAN_PARAM_EXTERN, UNIVPLAN_PARAM_EXEC } UnivplanParamKind;
int32_t univPlanExprAddParam(UnivPlanC *up, int32_t pid,
                             UnivplanParamKind paramKind, int32_t paramId,
                             int32_t typeId, int64_t typeMod);
typedef enum {
  UNIVPLAN_EXISTS_SUBLINK = 0,
  UNIVPLAN_ALL_SUBLINK = 1,
  UNIVPLAN_ANY_SUBLINK = 2,
  UNIVPLAN_ROWCOMPARE_SUBLINK = 3,
  UNIVPLAN_EXPR_SUBLINK = 4,
  UNIVPLAN_ARRAY_SUBLINK = 5,
  UNIVPLAN_NOT_EXISTS_SUBLINK = 6
} UnivplanSubLinkType;
int32_t univPlanExprAddSubPlan(UnivPlanC *up, int32_t pid,
                               UnivplanSubLinkType sublinkType, int32_t planId,
                               int32_t stageNo, int32_t typeId, int64_t typeMod,
                               bool useHashTable, bool initPlan);
void univPlanExprAddSubPlanTestexpr(UnivPlanC *up, int32_t subplanId);

void univPlanAddTokenEntry(UnivPlanC *up, FileSystemCredentialCPtr tokenEntry);
void univPlanAddSnapshot(UnivPlanC *up, char *snapshot, int32_t snapshot_len);
void univPlanSubPlanAddSetParam(UnivPlanC *up, int32_t subplanId, int32_t num,
                                int32_t *setParam);
void univPlanSubPlanAddParParam(UnivPlanC *up, int32_t subplanId, int32_t num,
                                int32_t *parParam);
void univPlanSubPlanAddTestexprParam(UnivPlanC *up, int32_t subplanId,
                                     int32_t num, int32_t *testexprParam);

typedef enum {
  UNIVPLAN_BOOLEXPRTYPE_AND_EXPR,
  UNIVPLAN_BOOLEXPRTYPE_OR_EXPR,
  UNIVPLAN_BOOLEXPRTYPE_NOT_EXPR
} UnivplanBoolExprType;
int32_t univPlanExprAddBoolExpr(UnivPlanC *up, int32_t pid,
                                UnivplanBoolExprType boolExprType);
typedef enum {
  UNIVPLAN_NULLTESTTYPE_IS_NULL,
  UNIVPLAN_NULLTESTTYPE_IS_NOT_NULL
} UnivplanNullTestType;
int32_t univPlanExprAddNullTestExpr(UnivPlanC *up, int32_t pid,
                                    UnivplanNullTestType nullTestType);

typedef enum {
  UNIVPLAN_BOOLEANTESTTYPE_IS_TRUE,
  UNIVPLAN_BOOLEANTESTTYPE_IS_NOT_TRUE,
  UNIVPLAN_BOOLEANTESTTYPE_IS_FALSE,
  UNIVPLAN_BOOLEANTESTTYPE_IS_NOT_FALSE,
  UNIVPLAN_BOOLEANTESTTYPE_IS_UNKNOWN,
  UNIVPLAN_BOOLEANTESTTYPE_IS_NOT_UNKNOWN
} UnivplanBooleanTestType;
int32_t univPlanExprAddBoolTestExpr(UnivPlanC *up, int32_t pid,
                                    UnivplanBooleanTestType boolTestType);

int32_t univPlanExprAddCaseExpr(UnivPlanC *up, int32_t pid, int32_t casetype);
void univPlanExprAddCaseExprDefresult(UnivPlanC *up, int32_t caseexpr_id);
int32_t univPlanExprAddCaseWhen(UnivPlanC *up, int32_t pid);
void univPlanExprAddCaseWhenExpr(UnivPlanC *up, int32_t casewhen_id);
void univPlanExprAddCaseWhenResult(UnivPlanC *up, int32_t casewhen_id);

int32_t univPlanExprAddScalarArrayOpExpr(UnivPlanC *up, int32_t pid,
                                         int32_t funcId, bool useOr);

int32_t univPlanExprAddCoalesceExpr(UnivPlanC *up, int32_t pid,
                                    int32_t coalesceType,
                                    int32_t coalesceTypeMod);

int32_t univPlanExprAddNullIfExpr(UnivPlanC *up, int32_t pid, int32_t funcId,
                                  int32_t retType, int32_t typeMod);

int32_t univPlanExprAddDistinctExpr(UnivPlanC *up, int32_t pid, int32_t funcId);

// debug
const char *univPlanGetJsonFormatedPlan(UnivPlanC *up);

#ifdef __cplusplus
}
#endif

#endif  // UNIVPLAN_SRC_UNIVPLAN_CWRAPPER_UNIVPLAN_C_H_
