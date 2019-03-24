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

#ifndef UNIVPLAN_SRC_UNIVPLAN_TESTUTIL_UNIVPLAN_PROTO_UTIL_H_
#define UNIVPLAN_SRC_UNIVPLAN_TESTUTIL_UNIVPLAN_PROTO_UTIL_H_

#include <string>
#include <utility>

#include "univplan/cwrapper/univplan-c.h"

#include "dbcommon/log/logger.h"
#include "dbcommon/type/type-kind.h"

#include "univplan/common/stagize.h"
#include "univplan/common/var-util.h"
#include "univplan/univplanbuilder/univplanbuilder-expr-tree.h"
#include "univplan/univplanbuilder/univplanbuilder-plan.h"
#include "univplan/univplanbuilder/univplanbuilder-table.h"
#include "univplan/univplanbuilder/univplanbuilder.h"

struct UnivPlanC {
  univplan::UnivPlanBuilder::uptr upb;
  univplan::UnivPlanBuilderNode::uptr curNode;
  univplan::UnivPlanBuilderExprTree::uptr curExpr;
  std::string seriaizedPlan;
  UnivPlanCatchedError error;
  std::string debugString;
  uint16_t totalStageNo;
};

typedef enum NodeTag {
  T_Invalid = 0,
  T_BoolExpr = 313,
} NodeTag;

typedef struct Expr {
  NodeTag type;
} Expr;

typedef enum BoolExprType { AND_EXPR, OR_EXPR, NOT_EXPR } BoolExprType;

struct ListCell {
  union {
    void *ptr_value;
    int int_value;
    unsigned int oid_value;
  } data;
  ListCell *next;
};

typedef struct List {
  NodeTag type; /* T_List, T_IntList, or T_OidList */
  int length;
  ListCell *head;
  ListCell *tail;
} List;

typedef struct BoolExpr {
  Expr xpr;
  BoolExprType boolop;
  List *args;   /* arguments to this expression */
  int location; /* token location, or -1 if unknown */
} BoolExpr;

typedef enum NullTestType { IS_NULL, IS_NOT_NULL } NullTestType;

typedef struct NullTest {
  Expr xpr;
  Expr *arg;                 /* input expression */
  NullTestType nulltesttype; /* IS NULL, IS NOT NULL */
} NullTest;

namespace univplan {
class UnivPlanProtoUtility {
 public:
  UnivPlanProtoUtility();
  ~UnivPlanProtoUtility();

  // like p op const
  void constructVarOpConstExpr(int32_t pid, int32_t opFuncId, int32_t varNo,
                               int32_t varAttNo, dbcommon::TypeKind varType,
                               dbcommon::TypeKind constType,
                               const char *buffer);

  // like const op p
  void constructConstOpVarExpr(int32_t pid, int32_t opFuncId, int32_t varNo,
                               int32_t varAttNo, dbcommon::TypeKind varType,
                               dbcommon::TypeKind constType,
                               const char *buffer);
  // like p op q
  void constructVarOpVarExpr(int32_t pid, int32_t opFuncId, int32_t varNo1,
                             int32_t varAttNo1, dbcommon::TypeKind varType1,
                             int32_t varNo2, int32_t varAttNo2,
                             dbcommon::TypeKind varType2);

  // like const op const
  void constructConstOpConstExpr(int32_t pid, int32_t opFuncId,
                                 dbcommon::TypeKind constType1,
                                 const char *buffer1,
                                 dbcommon::TypeKind constType2,
                                 const char *buffer2);

  // like func op p
  void constructFuncOpVarExpr(int32_t pid, int32_t opFuncId, int32_t varNo,
                              int32_t varAttNo, dbcommon::TypeKind varType,
                              int32_t mappingFuncId);

  // like p op const or q op const
  void constructBoolExpr(int32_t pid, int32_t opFuncId1, int32_t varNo1,
                         int32_t varAttNo1, dbcommon::TypeKind varType1,
                         dbcommon::TypeKind constType1, const char *buffer1,
                         int32_t opFuncId2, int32_t varNo2, int32_t varAttNo2,
                         dbcommon::TypeKind varType2,
                         dbcommon::TypeKind constType2, const char *buffer2,
                         int8_t boolType = 1);

  // like p is (not) null
  void constructNullTestExpr(int32_t pid, int8_t nulltesttype, int32_t varNo,
                             int32_t varAttNo, dbcommon::TypeKind varType);

  // like (p op const) op (q op const)
  void constructVarOpConstThenOpVarOpConstExpr(
      int32_t pid, int32_t opFuncId, int32_t opFuncId1, int32_t varNo1,
      int32_t varAttNo1, dbcommon::TypeKind varType1,
      dbcommon::TypeKind constType1, const char *buffer1, int32_t opFuncId2,
      int32_t varNo2, int32_t varAttNo2, dbcommon::TypeKind varType2,
      dbcommon::TypeKind constType2, const char *buffer2);

  // TargetEntry with var
  void constructVarTargetEntry(int32_t pid, int32_t varNo, int32_t varAttNo,
                               dbcommon::TypeKind type);

  // TargetEntry with const
  void constructConstTargetEntry(int32_t pid, dbcommon::TypeKind constType,
                                 const char *buffer);

  // TargetEntry with const op const
  void constructConstOpConstTargetEntry(int32_t pid, int32_t opFuncId,
                                        dbcommon::TypeKind constType1,
                                        const char *buffer1,
                                        dbcommon::TypeKind constType2,
                                        const char *buffer2);

  // TargetEntry with Aggref
  void constructAggRefTargetEntry(int32_t pid, int8_t aggstage, int32_t varNo,
                                  int32_t mappingFuncId, int32_t varAttNo,
                                  dbcommon::TypeKind type);

  // TargetEntry with NullTest
  void constructNullTestTargetEntry(int32_t pid, int8_t nulltesttype,
                                    int32_t varNo, int32_t varAttNo,
                                    dbcommon::TypeKind varType);

  // TargetEntry with p op const
  void constructVarOpConstTargetEntry(int32_t pid, int32_t opFuncId,
                                      int32_t varNo, int32_t varAttNo,
                                      dbcommon::TypeKind varType,
                                      dbcommon::TypeKind constType,
                                      const char *buffer);

  // QualList like p op const
  void constructVarOpConstQualList(int32_t pid, int32_t opFuncId, int32_t varNo,
                                   int32_t varAttNo, dbcommon::TypeKind varType,
                                   dbcommon::TypeKind constType,
                                   const char *buffer);

  // QualList like const op p
  void constructConstOpVarQualList(int32_t pid, int32_t opFuncId, int32_t varNo,
                                   int32_t varAttNo, dbcommon::TypeKind varType,
                                   dbcommon::TypeKind constType,
                                   const char *buffer);

  // QualList like p op q
  void constructVarOpVarQualList(int32_t pid, int32_t opFuncId, int32_t varNo1,
                                 int32_t varAttNo1, dbcommon::TypeKind varType1,
                                 int32_t varNo2, int32_t varAttNo2,
                                 dbcommon::TypeKind varType2);

  // QualList like func op p
  void constructFuncOpVarQualList(int32_t pid, int32_t opFuncId, int32_t varNo,
                                  int32_t varAttNo, dbcommon::TypeKind varType,
                                  int32_t mappingFuncId);

  // QualList like p op const or q op const
  void constructBoolQualList(int32_t pid, int32_t opFuncId1, int32_t varNo1,
                             int32_t varAttNo1, dbcommon::TypeKind varType1,
                             dbcommon::TypeKind constType1, const char *buffer1,
                             int32_t opFuncId2, int32_t varNo2,
                             int32_t varAttNo2, dbcommon::TypeKind varType2,
                             dbcommon::TypeKind constType2, const char *buffer2,
                             int8_t boolType = 1);

  // QualList with NullTest
  void constructNullTestQualList(int32_t pid, int8_t nulltesttype,
                                 int32_t varNo, int32_t varAttNo,
                                 dbcommon::TypeKind varType);

  // QualList like p op const or q op const
  void constructVarOpConstThenOpVarOpConstQualList(
      int32_t pid, int32_t opFuncId, int32_t opFuncId1, int32_t varNo1,
      int32_t varAttNo1, dbcommon::TypeKind varType1,
      dbcommon::TypeKind constType1, const char *buffer1, int32_t opFuncId2,
      int32_t varNo2, int32_t varAttNo2, dbcommon::TypeKind varType2,
      dbcommon::TypeKind constType2, const char *buffer2);

  int32_t constructSeqScan(int32_t pid, bool wihQualList,
                           int8_t targetEntryType);

  int32_t constructConnector(int32_t pid, int8_t ctype, bool allTargetEntry);

  int32_t constructSortConnector(int32_t pid, int8_t ctype);

  void constructRangeTable();

  void constructReceiver();

  // when count/limit = -1, means there is no count/limit
  int32_t constructLimitBelow(int32_t pid, int64_t count, int64_t offset);

  // when count/limit = -1, means there is no count/limit
  int32_t constructLimitTop(int32_t pid, int64_t count, int64_t offset);

  int32_t constructAgg(int32_t pid, int8_t aggstage);

  int32_t constructSort(int32_t pid);

  void univPlanFixVarType() { ::univPlanFixVarType(univplan); }

  void univPlanStagize() { ::univPlanStagize(univplan); }

  const char *univPlanSerialize(int32_t *size) {
    return ::univPlanSerialize(univplan, size, true);
  }

  const char *univPlanGetJsonFormatedPlan() {
    return ::univPlanGetJsonFormatedPlan(univplan);
  }

  void univPlanSetDoInstrument(bool doInstrument) {
    ::univPlanSetDoInstrument(univplan, doInstrument);
  }

  int32_t univPlanSeqScanNewInstance(int32_t pid) {
    return ::univPlanSeqScanNewInstance(univplan, pid);
  }

  void univPlanAddToPlanNodeTest(bool isleft) {
    ::univPlanAddToPlanNode(univplan, isleft);
  }

  UnivPlanC *getUnivPlan() { return std::move(univplan); }

 private:
  UnivPlanC *univplan = nullptr;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_TESTUTIL_UNIVPLAN_PROTO_UTIL_H_
