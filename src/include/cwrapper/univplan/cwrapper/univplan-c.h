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
///////////////////////////////////////////////////////////////////////////////

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

__attribute__((weak)) UnivPlanC *univPlanNewInstance() {}
__attribute__((weak)) void univPlanFreeInstance(UnivPlanC **up) {}

__attribute__((weak)) void univPlanNewSubPlanNode(UnivPlanC *up) {}
__attribute__((weak)) void univPlanFreeSubPlanNode(UnivPlanC *up) {}

// fill RangeTblEntry
typedef enum FormatType {
  UnivPlanTextFormat,
  UnivPlanCsvFormat,
  UnivPlanOrcFormat,
  UnivPlanMagmaFormat
} FormatType;
__attribute__((weak)) void univPlanRangeTblEntryAddTable(UnivPlanC *up, uint64_t tid,
                                   FormatType format, const char *location,
                                   const char *optStrInJson, uint32_t columnNum,
                                   const char **columnName,
                                   int32_t *columnDataType,
                                   int64_t *columnDataTypeMod,
                                   const char *targetName,
                                   const char *tableName) {}
__attribute__((weak)) void univPlanRangeTblEntryAddDummy(UnivPlanC *up) {}

// construct interconnect info
__attribute__((weak)) void univPlanReceiverAddListeners(UnivPlanC *up, uint32_t listenerNum,
                                  int32_t recId, const char **addr,
                                  int32_t *port) {}

// add param info
__attribute__((weak)) void univPlanAddParamInfo(UnivPlanC *up, int32_t type, bool isNull,
                          const char *buffer) {}

__attribute__((weak)) void univPlanSetDoInstrument(UnivPlanC *up, bool doInstrument) {}

__attribute__((weak)) void univPlanSetNCrossLevelParams(UnivPlanC *up, int32_t nCrossLevelParams) {}
__attribute__((weak)) void univPlanSetRangeVsegMap(UnivPlanC *up, const int32_t *rangeVsegMap,
                             int32_t num) {}

typedef enum UnivPlanCCmdType {
  UNIVPLAN_CMD_UNKNOWN,
  UNIVPLAN_CMD_SELECT,
  UNIVPLAN_CMD_UPDATE,
  UNIVPLAN_CMD_INSERT,
  UNIVPLAN_CMD_DELETE,
  UNIVPLAN_CMD_UTILITY,
  UNIVPLAN_CMD_NOTHING
} UnivPlanCCmdType;
__attribute__((weak)) void univPlanSetCmdType(UnivPlanC *up, UnivPlanCCmdType type) {}

__attribute__((weak)) void univPlanSetPlanNodeInfo(UnivPlanC *up, double planRows,
                             int32_t planRowWidth, uint64_t operatorMemKB) {}

/*
 * the expr tree must be built before adding targetlist/qualist
 */
__attribute__((weak)) void univPlanQualListAddExpr(UnivPlanC *up) {}
__attribute__((weak)) void univPlanInitplanAddExpr(UnivPlanC *up) {}
__attribute__((weak)) void univPlanTargetListAddTargetEntry(UnivPlanC *up, bool resJunk) {}
__attribute__((weak)) void univPlanConnectorAddHashExpr(UnivPlanC *up) {}

// construct Connector
typedef enum ConnectorType {
  UnivPlanShuffle,
  UnivPlanBroadcast,
  UnivPlanConverge
} ConnectorType;
__attribute__((weak)) int32_t univPlanConnectorNewInstance(UnivPlanC *up, int32_t id) {}
__attribute__((weak)) void univPlanConnectorSetType(UnivPlanC *up, ConnectorType type) {}
__attribute__((weak)) void univPlanConnectorSetStageNo(UnivPlanC *up, int32_t stageNo) {}
__attribute__((weak)) void univPlanConnectorSetColIdx(UnivPlanC *up, int64_t numCols,
                                const int32_t *colIdx) {}
__attribute__((weak)) void univPlanConnectorSetSortFuncId(UnivPlanC *up, int64_t numCols,
                                    const int32_t *sortFuncId) {}
__attribute__((weak)) void univPlanConnectorSetDirectDispatchId(UnivPlanC *up, int32_t id) {}

// construct ExtScan
__attribute__((weak)) int32_t univPlanExtScanNewInstance(UnivPlanC *up, int32_t pid) {}
__attribute__((weak)) void univPlanExtScanSetRelId(UnivPlanC *up, uint32_t relId) {}
__attribute__((weak)) void univPlanExtScanSetColumnsToRead(UnivPlanC *up, int64_t numCols,
                                     const int32_t *columnsToRead) {}
// construct magma index info
__attribute__((weak)) void univPlanExtScanSetIndex(UnivPlanC *up, bool index) {}
__attribute__((weak)) void univPlanExtScanSetScanType(UnivPlanC *up, int type) {}
__attribute__((weak)) void univPlanExtScanDirection(UnivPlanC *up, int direction) {}
__attribute__((weak)) void univPlanExtScanSetIndexName(UnivPlanC *up, const char *indexName) {}
__attribute__((weak)) void univPlanIndexQualListAddExpr(UnivPlanC *up) {}
__attribute__((weak)) void univPlanExtScanSetReadStatsOnly(UnivPlanC *up, bool readStatsOnly) {}

/*__attribute__((weak)) void univPlanExtScanAddTaskWithFileSplits(UnivPlanC *up, uint32_t
   fileSplitNum, int64_t *lbLen, int64_t *ubLen, const char **lowerBound, const
   char **upperBound) {}
*/
// construct SeqScan
__attribute__((weak)) int32_t univPlanSeqScanNewInstance(UnivPlanC *up, int32_t pid) {}
__attribute__((weak)) void univPlanSeqScanSetRelId(UnivPlanC *up, uint32_t relId) {}
__attribute__((weak)) void univPlanSeqScanSetReadStatsOnly(UnivPlanC *up, bool readStatsOnly) {}
__attribute__((weak)) void univPlanSeqScanSetColumnsToRead(UnivPlanC *up, int64_t numCols,
                                     const int32_t *columnsToRead) {}
__attribute__((weak)) void univPlanSeqScanAddTaskWithFileSplits(bool isMagma, UnivPlanC *up,
                                          uint32_t fileSplitNum,
                                          const char **fileName, int64_t *start,
                                          int64_t *len, int64_t *logicEof,
                                          int32_t *rangeid, int32_t *rgid) {}

// construct Agg
__attribute__((weak)) int32_t univPlanAggNewInstance(UnivPlanC *up, int32_t pid) {}
__attribute__((weak)) void univPlanAggSetNumGroupsAndGroupColIndexes(UnivPlanC *up, int64_t numGroups,
                                               int64_t numCols,
                                               const int32_t *grpColIdx) {}
typedef enum UnivPlanAggStrategy {
  UNIVPLAN_AGG_PLAIN,  /* simple agg across all input rows */
  UNIVPLAN_AGG_SORTED, /* grouped agg, input must be sorted */
  UNIVPLAN_AGG_HASHED  /* grouped agg, use internal hashtable */
} UnivPlanAggStrategy;
__attribute__((weak)) void univPlanAggSetAggstrategy(UnivPlanC *up, UnivPlanAggStrategy aggStrategy) {}
__attribute__((weak)) void univPlanAggSetRollup(UnivPlanC *up, int32_t numNullCols,
                          int64_t inputGrouping, int64_t grouping,
                          int32_t rollupGStimes, bool inputHasGrouping,
                          bool lastAgg, bool streaming) {}

// construct Sort
__attribute__((weak)) int32_t univPlanSortNewInstance(UnivPlanC *up, int32_t pid) {}
__attribute__((weak)) void univPlanSortSetColIdx(UnivPlanC *up, int64_t numCols,
                           const int32_t *colIdx) {}
__attribute__((weak)) void univPlanSortSetSortFuncId(UnivPlanC *up, int64_t numCols,
                               const int32_t *sortFuncId) {}
__attribute__((weak)) void univPlanSortAddLimitOffset(UnivPlanC *up) {}
__attribute__((weak)) void univPlanSortAddLimitCount(UnivPlanC *up) {}
__attribute__((weak)) void univPlanSortSetNoDuplicates(UnivPlanC *up, bool noDuplicates) {}

// construct Limit
__attribute__((weak)) int32_t univPlanLimitNewInstance(UnivPlanC *up, int32_t pid) {}
__attribute__((weak)) void univPlanLimitAddLimitOffset(UnivPlanC *up) {}
__attribute__((weak)) void univPlanLimitAddLimitCount(UnivPlanC *up) {}

// construct append
__attribute__((weak)) int32_t univPlanAppendNewInstance(UnivPlanC *up, int32_t pid) {}

typedef enum UnivPlanCJoinType {
  UNIVPLAN_JOIN_INNER,
  UNIVPLAN_JOIN_LEFT,
  UNIVPLAN_JOIN_FULL,
  UNIVPLAN_JOIN_RIGHT,
  UNIVPLAN_JOIN_IN,
  UNIVPLAN_JOIN_LASJ = 8,
  UNIVPLAN_JOIN_LASJ_NOTIN = 9
} UnivPlanCJoinType;
// construct nestloop
__attribute__((weak)) int32_t univPlanNestLoopNewInstance(UnivPlanC *up, int32_t pid) {}
__attribute__((weak)) bool univPlanNestLoopSetType(UnivPlanC *up, UnivPlanCJoinType type) {}
__attribute__((weak)) void univPlanNestLoopAddJoinQual(UnivPlanC *up) {}
// construct hashjoin
__attribute__((weak)) int32_t univPlanHashJoinNewInstance(UnivPlanC *up, int32_t pid) {}
__attribute__((weak)) bool univPlanHashJoinSetType(UnivPlanC *up, UnivPlanCJoinType type) {}
__attribute__((weak)) void univPlanHashJoinAddJoinQual(UnivPlanC *up) {}
__attribute__((weak)) void univPlanHashJoinAddHashClause(UnivPlanC *up) {}
__attribute__((weak)) void univPlanHashJoinAddHashQualClause(UnivPlanC *up) {}
// construct mergejoin
__attribute__((weak)) int32_t univPlanMergeJoinNewInstance(UnivPlanC *up, int32_t pid) {}
__attribute__((weak)) bool univPlanMergeJoinSetType(UnivPlanC *up, UnivPlanCJoinType type) {}
__attribute__((weak)) void univPlanMergeJoinAddJoinQual(UnivPlanC *up) {}
__attribute__((weak)) void univPlanMergeJoinAddMergeClause(UnivPlanC *up) {}
__attribute__((weak)) void univPlanMergeJoinSetUniqueOuter(UnivPlanC *up, bool uniqueOuter) {}

// construct hash
__attribute__((weak)) int32_t univPlanHashNewInstance(UnivPlanC *up, int32_t pid) {}

typedef enum UnivPlanCShareType {
  UNIVPLAN_SHARE_NOTSHARED,
  UNIVPLAN_SHARE_MATERIAL,
  UNIVPLAN_SHARE_MATERIAL_XSLICE,
  UNIVPLAN_SHARE_SORT,
  UNIVPLAN_SHARE_SORT_XSLICE
} UnivPlanCShareType;
// construct material
__attribute__((weak)) int32_t univPlanMaterialNewInstance(UnivPlanC *up, int32_t pid) {}
__attribute__((weak)) bool univPlanMaterialSetAttr(UnivPlanC *up, UnivPlanCShareType type,
                             bool cdbStrict, int32_t shareId,
                             int32_t driverSlice, int32_t nsharer,
                             int32_t xslice) {}

// construct shareinputscan
__attribute__((weak)) int32_t univPlanShareInputScanNewInstance(UnivPlanC *up, int32_t pid) {}
__attribute__((weak)) bool univPlanShareInputScanSetAttr(UnivPlanC *up, UnivPlanCShareType type,
                                   int32_t shareId, int32_t driverSlice) {}

// construct result
__attribute__((weak)) int32_t univPlanResultNewInstance(UnivPlanC *up, int32_t pid) {}
__attribute__((weak)) void univPlanResultAddResConstantQual(UnivPlanC *up) {}

// construct subqueryscan
__attribute__((weak)) int32_t univPlanSubqueryScanNewInstance(UnivPlanC *up, int32_t pid) {}
__attribute__((weak)) void univPlanSubqueryScanAddSubPlan(UnivPlanC *up) {}

// construct Unique
__attribute__((weak)) int32_t univPlanUniqueNewInstance(UnivPlanC *up, int32_t pid) {}
__attribute__((weak)) void univPlanUniqueSetNumGroupsAndUniqColIdxs(UnivPlanC *up, int64_t numCols,
                                              const int32_t *uniqColIdxs) {}

// construct Insert
__attribute__((weak)) int32_t univPlanInsertNewInstance(UnivPlanC *up, int32_t pid) {}
__attribute__((weak)) void univPlanInsertSetRelId(UnivPlanC *up, uint32_t relId) {}
// set magma table hasher when insert
__attribute__((weak)) void univPlanInsertSetHasher(UnivPlanC *up, int32_t nDistKeyIndex,
                             int16_t *distKeyIndex, int32_t nRanges,
                             uint32_t *rangeToRgMap, int16_t nRg,
                             uint16_t *rgIds, const char **rgUrls) {}

// construct SetOp
__attribute__((weak)) int32_t univPlanSetOpNewInstance(UnivPlanC *up, int32_t pid) {}
__attribute__((weak)) bool univPlanSetOpSetAttr(UnivPlanC *up, int cmd, int nduplicateColIdxs,
                          int16_t *duplicateColIdxs, int16_t flagColIdx) {}

__attribute__((weak)) void univPlanAddToPlanNode(UnivPlanC *up, bool isLeft) {}

__attribute__((weak)) void univPlanFixVarType(UnivPlanC *up) {}

// Generate the NewQE adapted univplan, which comes along with some extra
// mutation.
__attribute__((weak)) void univPlanStagize(UnivPlanC *up) {}

__attribute__((weak)) void univPlanAddGuc(UnivPlanC *up, const char *name, const char *value) {}

__attribute__((weak)) const char *univPlanSerialize(UnivPlanC *up, int32_t *size, bool compress) {}

__attribute__((weak)) UnivPlanCatchedError *univPlanGetLastError(UnivPlanC *up) {}

// call before add expr node
__attribute__((weak)) void univPlanNewExpr(UnivPlanC *up) {}

// following functions return the id of the new expr node in the expr tree
// which will be used as the pid for adding its argument in the expr tree
__attribute__((weak)) int32_t univPlanExprAddConst(UnivPlanC *up, int32_t pid, int32_t type,
                             bool isNull, const char *buffer, int64_t typeMod) {}
__attribute__((weak)) int32_t univPlanExprAddVar(UnivPlanC *up, int32_t pid, uint32_t varNo,
                           int32_t varAttNo, int32_t typeId, int64_t typeMod,
                           uint32_t varNoOld, int32_t varAttNoOld) {}
__attribute__((weak)) int32_t univPlanExprAddOpExpr(UnivPlanC *up, int32_t pid, int32_t funcId) {}
__attribute__((weak)) int32_t univPlanExprAddFuncExpr(UnivPlanC *up, int32_t pid, int32_t funcId) {}
__attribute__((weak)) int32_t univPlanAggrefAddPartialStage(UnivPlanC *up, int32_t pid,
                                      int32_t funcId) {}
__attribute__((weak)) int32_t univPlanAggrefAddIntermediateStage(UnivPlanC *up, int32_t pid,
                                           int32_t funcId) {}
__attribute__((weak)) int32_t univPlanAggrefAddFinalStage(UnivPlanC *up, int32_t pid, int32_t funcId) {}
__attribute__((weak)) int32_t univPlanAggrefAddOneStage(UnivPlanC *up, int32_t pid, int32_t funcId) {}
__attribute__((weak)) int32_t univPlanAggrefAddProxyVar(UnivPlanC *up, int32_t pid, int32_t varAttNo,
                                  int32_t funcId, int64_t typeMod,
                                  uint32_t varNoOld, int32_t varAttNoOld) {}
typedef enum { UNIVPLAN_PARAM_EXTERN, UNIVPLAN_PARAM_EXEC } UnivplanParamKind;
__attribute__((weak)) int32_t univPlanExprAddParam(UnivPlanC *up, int32_t pid,
                             UnivplanParamKind paramKind, int32_t paramId,
                             int32_t typeId, int64_t typeMod) {}
typedef enum {
  UNIVPLAN_EXISTS_SUBLINK = 0,
  UNIVPLAN_ALL_SUBLINK = 1,
  UNIVPLAN_ANY_SUBLINK = 2,
  UNIVPLAN_ROWCOMPARE_SUBLINK = 3,
  UNIVPLAN_EXPR_SUBLINK = 4,
  UNIVPLAN_ARRAY_SUBLINK = 5,
  UNIVPLAN_NOT_EXISTS_SUBLINK = 6
} UnivplanSubLinkType;
__attribute__((weak)) int32_t univPlanExprAddSubPlan(UnivPlanC *up, int32_t pid,
                               UnivplanSubLinkType sublinkType, int32_t planId,
                               int32_t stageNo, int32_t typeId, int64_t typeMod,
                               bool useHashTable, bool initPlan) {}
__attribute__((weak)) void univPlanExprAddSubPlanTestexpr(UnivPlanC *up, int32_t subplanId) {}

__attribute__((weak)) void univPlanAddTokenEntry(UnivPlanC *up, FileSystemCredentialCPtr tokenEntry) {}
__attribute__((weak)) void univPlanAddSnapshot(UnivPlanC *up, char *snapshot, int32_t snapshot_len) {}
__attribute__((weak)) void univPlanSubPlanAddSetParam(UnivPlanC *up, int32_t subplanId, int32_t num,
                                int32_t *setParam) {}
__attribute__((weak)) void univPlanSubPlanAddParParam(UnivPlanC *up, int32_t subplanId, int32_t num,
                                int32_t *parParam) {}
__attribute__((weak)) void univPlanSubPlanAddTestexprParam(UnivPlanC *up, int32_t subplanId,
                                     int32_t num, int32_t *testexprParam) {}

typedef enum {
  UNIVPLAN_BOOLEXPRTYPE_AND_EXPR,
  UNIVPLAN_BOOLEXPRTYPE_OR_EXPR,
  UNIVPLAN_BOOLEXPRTYPE_NOT_EXPR
} UnivplanBoolExprType;
__attribute__((weak)) int32_t univPlanExprAddBoolExpr(UnivPlanC *up, int32_t pid,
                                UnivplanBoolExprType boolExprType) {}
typedef enum {
  UNIVPLAN_NULLTESTTYPE_IS_NULL,
  UNIVPLAN_NULLTESTTYPE_IS_NOT_NULL
} UnivplanNullTestType;
__attribute__((weak)) int32_t univPlanExprAddNullTestExpr(UnivPlanC *up, int32_t pid,
                                    UnivplanNullTestType nullTestType) {}

typedef enum {
  UNIVPLAN_BOOLEANTESTTYPE_IS_TRUE,
  UNIVPLAN_BOOLEANTESTTYPE_IS_NOT_TRUE,
  UNIVPLAN_BOOLEANTESTTYPE_IS_FALSE,
  UNIVPLAN_BOOLEANTESTTYPE_IS_NOT_FALSE,
  UNIVPLAN_BOOLEANTESTTYPE_IS_UNKNOWN,
  UNIVPLAN_BOOLEANTESTTYPE_IS_NOT_UNKNOWN
} UnivplanBooleanTestType;
__attribute__((weak)) int32_t univPlanExprAddBoolTestExpr(UnivPlanC *up, int32_t pid,
                                    UnivplanBooleanTestType boolTestType) {}

__attribute__((weak)) int32_t univPlanExprAddCaseExpr(UnivPlanC *up, int32_t pid, int32_t casetype) {}
__attribute__((weak)) void univPlanExprAddCaseExprDefresult(UnivPlanC *up, int32_t caseexpr_id) {}
__attribute__((weak)) int32_t univPlanExprAddCaseWhen(UnivPlanC *up, int32_t pid) {}
__attribute__((weak)) void univPlanExprAddCaseWhenExpr(UnivPlanC *up, int32_t casewhen_id) {}
__attribute__((weak)) void univPlanExprAddCaseWhenResult(UnivPlanC *up, int32_t casewhen_id) {}

__attribute__((weak)) int32_t univPlanExprAddScalarArrayOpExpr(UnivPlanC *up, int32_t pid,
                                         int32_t funcId, bool useOr) {}

__attribute__((weak)) int32_t univPlanExprAddCoalesceExpr(UnivPlanC *up, int32_t pid,
                                    int32_t coalesceType,
                                    int32_t coalesceTypeMod) {}

__attribute__((weak)) int32_t univPlanExprAddNullIfExpr(UnivPlanC *up, int32_t pid, int32_t funcId,
                                  int32_t retType, int32_t typeMod) {}

__attribute__((weak)) int32_t univPlanExprAddDistinctExpr(UnivPlanC *up, int32_t pid, int32_t funcId) {}

__attribute__((weak)) int32_t univPlanExprAddGrouping(UnivPlanC *up, int32_t pid) {}

__attribute__((weak)) int32_t univPlanExprAddGroupId(UnivPlanC *up, int32_t pid) {}

__attribute__((weak)) int32_t univPlanExprAddGroupingFunc(UnivPlanC *up, int32_t pid, int32_t *args,
                                    int32_t nargs, int32_t numGrouping) {}

__attribute__((weak)) const void *univPlanGetQualList(UnivPlanC *up) {}

// debug
__attribute__((weak)) const char *univPlanGetJsonFormatedPlan(UnivPlanC *up) {}

#ifdef __cplusplus
}
#endif

#ifdef __cplusplus
#include <map>
#include <stack>
#include <string>

#include "univplan/univplanbuilder/univplanbuilder.h"
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
#endif

#endif  // UNIVPLAN_SRC_UNIVPLAN_CWRAPPER_UNIVPLAN_C_H_
