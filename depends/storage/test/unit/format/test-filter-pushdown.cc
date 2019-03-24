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

#include "dbcommon/common/tuple-batch.h"
#include "dbcommon/common/tuple-desc.h"
#include "dbcommon/filesystem/file-system-manager.h"
#include "dbcommon/filesystem/file-system.h"
#include "dbcommon/filesystem/local/local-file-system.h"
#include "dbcommon/function/func-kind.cg.h"
#include "dbcommon/log/logger.h"
#include "dbcommon/testutil/tuple-batch-utils.h"
#include "dbcommon/utils/parameters.h"
#include "gtest/gtest.h"
#include "storage/format/orc/orc-format.h"
#include "storage/testutil/format-util.h"

using namespace testing;  // NOLINT

namespace storage {

void generateTest(const std::string &pattern,
                  const univplan::UnivPlanExprPolyList *predicateExprs,
                  const char *casename, bool shouldSkip1 = false,
                  bool shouldSkip2 = true, bool testNe = false) {
  dbcommon::TupleBatchUtility tbu;
  std::string path = "/tmp/";
  path.append(casename);
  FormatUtility fmtu;
  {
    dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc(pattern);
    dbcommon::TupleBatch::uptr tb;
    if (testNe)
      tb = tbu.generateTupleBatch(*desc, 0, 1, false);
    else
      tb = tbu.generateTupleBatch(*desc, 0, 32, false);
    fmtu.writeThenReadCompare("orc", desc.get(), std::move(tb), path,
                              predicateExprs, shouldSkip1);
    LOG_INFO("OK without nulls");
  }
  {
    dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc(pattern);
    dbcommon::TupleBatch::uptr tb;
    if (testNe)
      tb = tbu.generateTupleBatch(*desc, 1, 2, true);
    else
      tb = tbu.generateTupleBatch(*desc, 1, 30, true);
    fmtu.writeThenReadCompare("orc", desc.get(), std::move(tb), path,
                              predicateExprs, shouldSkip2);
    LOG_INFO("OK with nulls");
  }
}

void testVarOpConst(const std::string &pattern, const char *casename,
                    int32_t opFuncId, dbcommon::TypeKind varType,
                    const char *buffer, bool shouldSkip1 = false,
                    bool shouldSkip2 = true, bool testNe = false) {
  univplan::UnivPlanProtoUtility upu;
  int32_t uid = upu.univPlanSeqScanNewInstance(-1);
  upu.constructVarOpConstQualList(-1, opFuncId, 1, 1, varType, varType, buffer);
  upu.univPlanAddToPlanNodeTest(true);
  const univplan::UnivPlanPlanNodePoly *planNode =
      &upu.getUnivPlan()->upb.get()->getPlanBuilderPlan()->getPlan()->plan();
  const univplan::UnivPlanScanSeq &ss = planNode->scanseq();
  const univplan::UnivPlanExprPolyList *predicateExprs = &ss.super().quallist();
  LOG_INFO("plan=%s", upu.univPlanGetJsonFormatedPlan());
  generateTest(pattern, predicateExprs, casename, shouldSkip1, shouldSkip2,
               testNe);
}

TEST(TestORCFormat, TestFilterPushDown_ConstEqVar) {
  univplan::UnivPlanProtoUtility upu;
  int32_t uid = upu.univPlanSeqScanNewInstance(-1);
  upu.constructConstOpVarQualList(-1, dbcommon::SMALLINT_EQUAL_INT, 1, 1,
                                  dbcommon::SMALLINTID, dbcommon::SMALLINTID,
                                  "0");
  upu.univPlanAddToPlanNodeTest(true);
  const univplan::UnivPlanPlanNodePoly *planNode =
      &upu.getUnivPlan()->upb.get()->getPlanBuilderPlan()->getPlan()->plan();
  const univplan::UnivPlanScanSeq &ss = planNode->scanseq();
  const univplan::UnivPlanExprPolyList *predicateExprs = &ss.super().quallist();
  LOG_INFO("plan=%s", upu.univPlanGetJsonFormatedPlan());
  generateTest("h", predicateExprs, "TestFilterPushDown_ConstEqVar");
}

TEST(TestORCFormat, TestFilterPushDown_VarEqConstSmallint) {
  testVarOpConst("h", "TestFilterPushDown_VarEqConstSmallint",
                 dbcommon::SMALLINT_EQUAL_INT, dbcommon::SMALLINTID, "0");
}

TEST(TestORCFormat, TestFilterPushDown_VarEqConstInt) {
  testVarOpConst("i", "TestFilterPushDown_VarEqConstInt",
                 dbcommon::INT_EQUAL_INT, dbcommon::INTID, "0");
}

TEST(TestORCFormat, TestFilterPushDown_VarEqConstBigint) {
  testVarOpConst("l", "TestFilterPushDown_VarEqConstBigint",
                 dbcommon::BIGINT_EQUAL_INT, dbcommon::BIGINTID, "0");
}

TEST(TestORCFormat, TestFilterPushDown_VarEqConstDouble) {
  testVarOpConst("d", "TestFilterPushDown_VarEqConstDouble",
                 dbcommon::DOUBLE_EQUAL_DOUBLE, dbcommon::DOUBLEID, "0");
}

TEST(TestORCFormat, TestFilterPushDown_VarEqConstFloat) {
  testVarOpConst("f", "TestFilterPushDown_VarEqConstFloat",
                 dbcommon::FLOAT_EQUAL_DOUBLE, dbcommon::FLOATID, "0");
}

TEST(TestORCFormat, TestFilterPushDown_VarEqConstString) {
  testVarOpConst("s", "TestFilterPushDown_VarEqConstString",
                 dbcommon::STRING_EQUAL_STRING, dbcommon::STRINGID, "0");
}

TEST(TestORCFormat, TestFilterPushDown_VarEqConstDate) {
  testVarOpConst("D", "TestFilterPushDown_VarEqConstDate",
                 dbcommon::INT_EQUAL_INT, dbcommon::DATEID, "2018-01-10");
}

TEST(TestORCFormat, TestFilterPushDown_VarEqConstTime) {
  testVarOpConst("T", "TestFilterPushDown_VarEqConstTime",
                 dbcommon::BIGINT_EQUAL_BIGINT, dbcommon::TIMEID, "00:00:00");
}

TEST(TestORCFormat, TestFilterPushDown_VarEqConstTimestamp) {
  testVarOpConst("S", "TestFilterPushDown_VarEqConstTimestamp",
                 dbcommon::TIMESTAMP_EQUAL_TIMESTAMP, dbcommon::TIMESTAMPID,
                 "2018-01-10 15:16:01.123");
}

TEST(TestORCFormat, TestFilterPushDown_VarNeConstSmallint) {
  testVarOpConst("h", "TestFilterPushDown_VarNeConstSmallint",
                 dbcommon::SMALLINT_NOT_EQUAL_INT, dbcommon::SMALLINTID, "0",
                 true, false, true);
}

TEST(TestORCFormat, TestFilterPushDown_VarNeConstInt) {
  testVarOpConst("i", "TestFilterPushDown_VarNeConstInt",
                 dbcommon::INT_NOT_EQUAL_INT, dbcommon::INTID, "0", true, false,
                 true);
}

TEST(TestORCFormat, TestFilterPushDown_VarNeConstBigint) {
  testVarOpConst("l", "TestFilterPushDown_VarNeConstBigint",
                 dbcommon::BIGINT_NOT_EQUAL_INT, dbcommon::BIGINTID, "0", true,
                 false, true);
}

TEST(TestORCFormat, TestFilterPushDown_VarNeConstDouble) {
  testVarOpConst("d", "TestFilterPushDown_VarNeConstDouble",
                 dbcommon::DOUBLE_NOT_EQUAL_DOUBLE, dbcommon::DOUBLEID, "0",
                 true, false, true);
}

TEST(TestORCFormat, TestFilterPushDown_VarNeConstFloat) {
  testVarOpConst("f", "TestFilterPushDown_VarNeConstFloat",
                 dbcommon::FLOAT_NOT_EQUAL_DOUBLE, dbcommon::FLOATID, "0", true,
                 false, true);
}

TEST(TestORCFormat, TestFilterPushDown_VarNeConstString) {
  testVarOpConst("s", "TestFilterPushDown_VarNeConstString",
                 dbcommon::STRING_NOT_EQUAL_STRING, dbcommon::STRINGID, "0",
                 true, false, true);
}

TEST(TestORCFormat, TestFilterPushDown_VarNeConstDate) {
  testVarOpConst("D", "TestFilterPushDown_VarNeConstDate",
                 dbcommon::INT_NOT_EQUAL_INT, dbcommon::DATEID, "2018-01-10",
                 true, false, true);
}

TEST(TestORCFormat, TestFilterPushDown_VarNeConstTime) {
  testVarOpConst("T", "TestFilterPushDown_VarNeConstTime",
                 dbcommon::BIGINT_NOT_EQUAL_BIGINT, dbcommon::TIMEID,
                 "00:00:00", true, false, true);
}

TEST(TestORCFormat, TestFilterPushDown_VarNeConstTimestamp) {
  testVarOpConst("S", "TestFilterPushDown_VarNeConstTimestamp",
                 dbcommon::TIMESTAMP_NOT_EQUAL_TIMESTAMP, dbcommon::TIMESTAMPID,
                 "2018-01-10 15:16:01.123", false, false, true);
}

TEST(TestORCFormat, TestFilterPushDown_VarLqConstSmallint) {
  testVarOpConst("h", "TestFilterPushDown_VarLqConstSmallint",
                 dbcommon::SMALLINT_LESS_THAN_INT, dbcommon::SMALLINTID, "1");
}

TEST(TestORCFormat, TestFilterPushDown_VarLqConstInt) {
  testVarOpConst("i", "TestFilterPushDown_VarLqConstInt",
                 dbcommon::INT_LESS_THAN_INT, dbcommon::INTID, "1");
}

TEST(TestORCFormat, TestFilterPushDown_VarLqConstBigint) {
  testVarOpConst("l", "TestFilterPushDown_VarLqConstBigint",
                 dbcommon::BIGINT_LESS_THAN_INT, dbcommon::BIGINTID, "1");
}

TEST(TestORCFormat, TestFilterPushDown_VarLqConstDouble) {
  testVarOpConst("d", "TestFilterPushDown_VarLqConstDouble",
                 dbcommon::DOUBLE_LESS_THAN_DOUBLE, dbcommon::DOUBLEID, "1");
}

TEST(TestORCFormat, TestFilterPushDown_VarLqConstFloat) {
  testVarOpConst("f", "TestFilterPushDown_VarLqConstFloat",
                 dbcommon::DOUBLE_LESS_THAN_DOUBLE, dbcommon::FLOATID, "1");
}

TEST(TestORCFormat, TestFilterPushDown_VarLqConstString) {
  testVarOpConst("s", "TestFilterPushDown_VarLqConstString",
                 dbcommon::STRING_LESS_THAN_STRING, dbcommon::STRINGID, "1");
}

TEST(TestORCFormat, TestFilterPushDown_VarLqConstDate) {
  testVarOpConst("D", "TestFilterPushDown_VarLqConstDate",
                 dbcommon::INT_LESS_THAN_INT, dbcommon::DATEID, "2018-01-11");
}

TEST(TestORCFormat, TestFilterPushDown_VarLqConstTime) {
  testVarOpConst("T", "TestFilterPushDown_VarLqConstTime",
                 dbcommon::BIGINT_LESS_THAN_BIGINT, dbcommon::TIMEID,
                 "00:00:00.1");
}

TEST(TestORCFormat, TestFilterPushDown_VarLqConstTimestamp) {
  testVarOpConst("S", "TestFilterPushDown_VarLqConstTimestamp",
                 dbcommon::TIMESTAMP_LESS_THAN_TIMESTAMP, dbcommon::TIMESTAMPID,
                 "2018-01-11 15:16:01.123");
}

TEST(TestORCFormat, TestFilterPushDown_VarGqConstSmallint) {
  testVarOpConst("h", "TestFilterPushDown_VarGqConstSmallint",
                 dbcommon::SMALLINT_GREATER_THAN_INT, dbcommon::SMALLINTID,
                 "30");
}

TEST(TestORCFormat, TestFilterPushDown_VarGqConstInt) {
  testVarOpConst("i", "TestFilterPushDown_VarGqConstInt",
                 dbcommon::INT_GREATER_THAN_INT, dbcommon::INTID, "30");
}

TEST(TestORCFormat, TestFilterPushDown_VarGqConstBigint) {
  testVarOpConst("l", "TestFilterPushDown_VarGqConstBigint",
                 dbcommon::BIGINT_GREATER_THAN_INT, dbcommon::BIGINTID, "30");
}

TEST(TestORCFormat, TestFilterPushDown_VarGqConstDouble) {
  testVarOpConst("d", "TestFilterPushDown_VarGqConstDouble",
                 dbcommon::DOUBLE_GREATER_THAN_DOUBLE, dbcommon::DOUBLEID,
                 "30");
}

TEST(TestORCFormat, TestFilterPushDown_VarGqConstFloat) {
  testVarOpConst("f", "TestFilterPushDown_VarGqConstFloat",
                 dbcommon::FLOAT_GREATER_THAN_DOUBLE, dbcommon::FLOATID, "30");
}

TEST(TestORCFormat, TestFilterPushDown_VarGqConstString) {
  testVarOpConst("s", "TestFilterPushDown_VarGqConstString",
                 dbcommon::STRING_GREATER_THAN_STRING, dbcommon::STRINGID, "90",
                 true, true);
}

TEST(TestORCFormat, TestFilterPushDown_VarGqConstDate) {
  testVarOpConst("D", "TestFilterPushDown_VarGqConstDate",
                 dbcommon::INT_GREATER_THAN_INT, dbcommon::DATEID,
                 "2018-01-16");
}

TEST(TestORCFormat, TestFilterPushDown_VarGqConstTime) {
  testVarOpConst("T", "TestFilterPushDown_VarGqConstTime",
                 dbcommon::BIGINT_GREATER_THAN_BIGINT, dbcommon::TIMEID,
                 "00:00:00.9", true, true);
}

TEST(TestORCFormat, TestFilterPushDown_VarGqConstTimestamp) {
  testVarOpConst("S", "TestFilterPushDown_VarGqConstTimestamp",
                 dbcommon::TIMESTAMP_GREATER_THAN_TIMESTAMP,
                 dbcommon::TIMESTAMPID, "2018-01-16 15:16:01.123999999");
}

TEST(TestORCFormat, TestFilterPushDown_VarLeConstSmallint) {
  testVarOpConst("h", "TestFilterPushDown_VarLeConstSmallint",
                 dbcommon::SMALLINT_LESS_EQ_INT, dbcommon::SMALLINTID, "0");
}

TEST(TestORCFormat, TestFilterPushDown_VarLeConstInt) {
  testVarOpConst("i", "TestFilterPushDown_VarLeConstInt",
                 dbcommon::INT_LESS_EQ_INT, dbcommon::INTID, "0");
}

TEST(TestORCFormat, TestFilterPushDown_VarLeConstBigint) {
  testVarOpConst("l", "TestFilterPushDown_VarLeConstBigint",
                 dbcommon::BIGINT_LESS_EQ_INT, dbcommon::BIGINTID, "0");
}

TEST(TestORCFormat, TestFilterPushDown_VarLeConstDouble) {
  testVarOpConst("d", "TestFilterPushDown_VarLeConstDouble",
                 dbcommon::DOUBLE_LESS_EQ_DOUBLE, dbcommon::DOUBLEID, "0");
}

TEST(TestORCFormat, TestFilterPushDown_VarLeConstFloat) {
  testVarOpConst("f", "TestFilterPushDown_VarLeConstFloat",
                 dbcommon::FLOAT_LESS_EQ_DOUBLE, dbcommon::FLOATID, "0");
}

TEST(TestORCFormat, TestFilterPushDown_VarLeConstString) {
  testVarOpConst("s", "TestFilterPushDown_VarLeConstString",
                 dbcommon::STRING_LESS_EQ_STRING, dbcommon::STRINGID, "0");
}

TEST(TestORCFormat, TestFilterPushDown_VarLeConstDate) {
  testVarOpConst("D", "TestFilterPushDown_VarLeConstDate",
                 dbcommon::INT_LESS_EQ_INT, dbcommon::DATEID, "2018-01-10");
}

TEST(TestORCFormat, TestFilterPushDown_VarLeConstTime) {
  testVarOpConst("T", "TestFilterPushDown_VarLeConstTime",
                 dbcommon::BIGINT_LESS_EQ_BIGINT, dbcommon::TIMEID, "00:00:00");
}

TEST(TestORCFormat, TestFilterPushDown_VarLeConstTimestamp) {
  testVarOpConst("S", "TestFilterPushDown_VarLeConstTimestamp",
                 dbcommon::TIMESTAMP_LESS_EQ_TIMESTAMP, dbcommon::TIMESTAMPID,
                 "2018-01-10 15:16:01.123");
}

TEST(TestORCFormat, TestFilterPushDown_VarGeConstSmallint) {
  testVarOpConst("h", "TestFilterPushDown_VarGeConstSmallint",
                 dbcommon::SMALLINT_GREATER_EQ_INT, dbcommon::SMALLINTID, "31");
}

TEST(TestORCFormat, TestFilterPushDown_VarGeConstInt) {
  testVarOpConst("i", "TestFilterPushDown_VarGeConstInt",
                 dbcommon::INT_GREATER_EQ_INT, dbcommon::INTID, "31");
}

TEST(TestORCFormat, TestFilterPushDown_VarGeConstBigint) {
  testVarOpConst("l", "TestFilterPushDown_VarGeConstBigint",
                 dbcommon::BIGINT_GREATER_EQ_INT, dbcommon::BIGINTID, "31");
}

TEST(TestORCFormat, TestFilterPushDown_VarGeConstDouble) {
  testVarOpConst("d", "TestFilterPushDown_VarGeConstDouble",
                 dbcommon::DOUBLE_GREATER_EQ_DOUBLE, dbcommon::DOUBLEID, "31");
}

TEST(TestORCFormat, TestFilterPushDown_VarGeConstFloat) {
  testVarOpConst("f", "TestFilterPushDown_VarGeConstFloat",
                 dbcommon::FLOAT_GREATER_EQ_DOUBLE, dbcommon::FLOATID, "31");
}

TEST(TestORCFormat, TestFilterPushDown_VarGeConstString) {
  testVarOpConst("s", "TestFilterPushDown_VarGeConstString",
                 dbcommon::STRING_GREATER_EQ_STRING, dbcommon::STRINGID, "90",
                 true, true);
}

TEST(TestORCFormat, TestFilterPushDown_VarGeConstDate) {
  testVarOpConst("D", "TestFilterPushDown_VarGeConstDate",
                 dbcommon::INT_GREATER_EQ_INT, dbcommon::DATEID, "2018-01-17");
}

TEST(TestORCFormat, TestFilterPushDown_VarGeConstTime) {
  testVarOpConst("T", "TestFilterPushDown_VarGeConstTime",
                 dbcommon::BIGINT_GREATER_EQ_BIGINT, dbcommon::TIMEID,
                 "00:00:00.91", true, true);
}

TEST(TestORCFormat, TestFilterPushDown_VarGeConstTimestamp) {
  testVarOpConst("S", "TestFilterPushDown_VarGeConstTimestamp",
                 dbcommon::TIMESTAMP_GREATER_EQ_TIMESTAMP,
                 dbcommon::TIMESTAMPID, "2018-01-17 15:16:01.123");
}

TEST(TestORCFormat, DISABLED_TestFilterPushDown_FuncLqVar) {
  // TEST(TestORCFormat, TestFilterPushDown_FuncLqVar) {
  univplan::UnivPlanProtoUtility upu;
  int32_t uid = upu.univPlanSeqScanNewInstance(-1);
  upu.constructFuncOpVarQualList(-1, dbcommon::DOUBLE_LESS_THAN_DOUBLE, 1, 4,
                                 dbcommon::DOUBLEID, dbcommon::RANDOMF);
  upu.univPlanAddToPlanNodeTest(true);
  const univplan::UnivPlanPlanNodePoly *planNode =
      &upu.getUnivPlan()->upb.get()->getPlanBuilderPlan()->getPlan()->plan();
  const univplan::UnivPlanScanSeq &ss = planNode->scanseq();
  const univplan::UnivPlanExprPolyList *predicateExprs = &ss.super().quallist();
  LOG_INFO("plan=%s", upu.univPlanGetJsonFormatedPlan());
  generateTest("hildfs", predicateExprs, "TestFilterPushDown_FuncEqVar");
}

TEST(TestORCFormat, TestFilterPushDown_TestOr1) {
  univplan::UnivPlanProtoUtility upu;
  int32_t uid = upu.univPlanSeqScanNewInstance(-1);
  upu.constructBoolQualList(-1, dbcommon::SMALLINT_EQUAL_INT, 1, 1,
                            dbcommon::SMALLINTID, dbcommon::SMALLINTID, "0",
                            dbcommon::INT_EQUAL_INT, 1, 2, dbcommon::INTID,
                            dbcommon::INTID, "0");
  upu.constructBoolQualList(-1, dbcommon::BIGINT_EQUAL_INT, 1, 3,
                            dbcommon::BIGINTID, dbcommon::BIGINTID, "0",
                            dbcommon::DOUBLE_EQUAL_DOUBLE, 1, 4,
                            dbcommon::DOUBLEID, dbcommon::DOUBLEID, "0");
  upu.constructBoolQualList(-1, dbcommon::FLOAT_EQUAL_DOUBLE, 1, 5,
                            dbcommon::FLOATID, dbcommon::FLOATID, "0",
                            dbcommon::BIGINT_EQUAL_INT, 1, 6,
                            dbcommon::STRINGID, dbcommon::STRINGID, "0");
  upu.univPlanAddToPlanNodeTest(true);
  const univplan::UnivPlanPlanNodePoly *planNode =
      &upu.getUnivPlan()->upb.get()->getPlanBuilderPlan()->getPlan()->plan();
  const univplan::UnivPlanScanSeq &ss = planNode->scanseq();
  const univplan::UnivPlanExprPolyList *predicateExprs = &ss.super().quallist();
  LOG_INFO("plan=%s", upu.univPlanGetJsonFormatedPlan());
  generateTest("hildfs", predicateExprs, "TestFilterPushDown_TestOr1");
}

TEST(TestORCFormat, TestFilterPushDown_TestOr2) {
  univplan::UnivPlanProtoUtility upu;
  int32_t uid = upu.univPlanSeqScanNewInstance(-1);
  upu.constructBoolQualList(-1, dbcommon::SMALLINT_EQUAL_INT, 1, 1,
                            dbcommon::SMALLINTID, dbcommon::SMALLINTID, "1",
                            dbcommon::INT_EQUAL_INT, 1, 2, dbcommon::INTID,
                            dbcommon::INTID, "0");
  upu.constructBoolQualList(-1, dbcommon::BIGINT_EQUAL_INT, 1, 3,
                            dbcommon::BIGINTID, dbcommon::BIGINTID, "1",
                            dbcommon::DOUBLE_EQUAL_DOUBLE, 1, 4,
                            dbcommon::DOUBLEID, dbcommon::DOUBLEID, "0");
  upu.constructBoolQualList(-1, dbcommon::FLOAT_EQUAL_DOUBLE, 1, 5,
                            dbcommon::FLOATID, dbcommon::FLOATID, "1",
                            dbcommon::BIGINT_EQUAL_INT, 1, 6,
                            dbcommon::STRINGID, dbcommon::STRINGID, "0");
  upu.univPlanAddToPlanNodeTest(true);
  const univplan::UnivPlanPlanNodePoly *planNode =
      &upu.getUnivPlan()->upb.get()->getPlanBuilderPlan()->getPlan()->plan();
  const univplan::UnivPlanScanSeq &ss = planNode->scanseq();
  const univplan::UnivPlanExprPolyList *predicateExprs = &ss.super().quallist();
  LOG_INFO("plan=%s", upu.univPlanGetJsonFormatedPlan());
  generateTest("hildfs", predicateExprs, "TestFilterPushDown_TestOr2", false,
               false);
}

TEST(TestORCFormat, TestFilterPushDown_TestAnd) {
  univplan::UnivPlanProtoUtility upu;
  int32_t uid = upu.univPlanSeqScanNewInstance(-1);
  upu.constructBoolQualList(-1, dbcommon::SMALLINT_EQUAL_INT, 1, 1,
                            dbcommon::SMALLINTID, dbcommon::SMALLINTID, "1",
                            dbcommon::INT_EQUAL_INT, 1, 2, dbcommon::INTID,
                            dbcommon::INTID, "0", 0);
  upu.constructBoolQualList(-1, dbcommon::BIGINT_EQUAL_INT, 1, 3,
                            dbcommon::BIGINTID, dbcommon::BIGINTID, "1",
                            dbcommon::DOUBLE_EQUAL_DOUBLE, 1, 4,
                            dbcommon::DOUBLEID, dbcommon::DOUBLEID, "0", 0);
  upu.constructBoolQualList(-1, dbcommon::FLOAT_EQUAL_DOUBLE, 1, 5,
                            dbcommon::FLOATID, dbcommon::FLOATID, "0",
                            dbcommon::BIGINT_EQUAL_INT, 1, 6,
                            dbcommon::STRINGID, dbcommon::STRINGID, "0", 0);
  upu.univPlanAddToPlanNodeTest(true);
  const univplan::UnivPlanPlanNodePoly *planNode =
      &upu.getUnivPlan()->upb.get()->getPlanBuilderPlan()->getPlan()->plan();
  const univplan::UnivPlanScanSeq &ss = planNode->scanseq();
  const univplan::UnivPlanExprPolyList *predicateExprs = &ss.super().quallist();
  LOG_INFO("plan=%s", upu.univPlanGetJsonFormatedPlan());
  generateTest("hildfs", predicateExprs, "TestFilterPushDown_TestAnd");
}

TEST(TestORCFormat, TestFilterPushDown_TestNot) {
  univplan::UnivPlanProtoUtility upu;
  int32_t uid = upu.univPlanSeqScanNewInstance(-1);
  upu.constructBoolQualList(-1, dbcommon::SMALLINT_EQUAL_INT, 1, 1,
                            dbcommon::SMALLINTID, dbcommon::SMALLINTID, "1",
                            dbcommon::INT_EQUAL_INT, 1, 2, dbcommon::INTID,
                            dbcommon::INTID, "0", 2);
  upu.constructBoolQualList(-1, dbcommon::BIGINT_EQUAL_INT, 1, 3,
                            dbcommon::BIGINTID, dbcommon::BIGINTID, "1",
                            dbcommon::DOUBLE_EQUAL_DOUBLE, 1, 4,
                            dbcommon::DOUBLEID, dbcommon::DOUBLEID, "0", 2);
  upu.constructBoolQualList(-1, dbcommon::FLOAT_EQUAL_DOUBLE, 1, 5,
                            dbcommon::FLOATID, dbcommon::FLOATID, "0",
                            dbcommon::BIGINT_EQUAL_INT, 1, 6,
                            dbcommon::STRINGID, dbcommon::STRINGID, "0", 2);
  upu.univPlanAddToPlanNodeTest(true);
  const univplan::UnivPlanPlanNodePoly *planNode =
      &upu.getUnivPlan()->upb.get()->getPlanBuilderPlan()->getPlan()->plan();
  const univplan::UnivPlanScanSeq &ss = planNode->scanseq();
  const univplan::UnivPlanExprPolyList *predicateExprs = &ss.super().quallist();
  LOG_INFO("plan=%s", upu.univPlanGetJsonFormatedPlan());
  generateTest("hildfs", predicateExprs, "TestFilterPushDown_TestNot", false,
               false);
}

TEST(TestORCFormat, TestFilterPushDown_IsNull) {
  univplan::UnivPlanProtoUtility upu;
  int32_t uid = upu.univPlanSeqScanNewInstance(-1);
  upu.constructNullTestQualList(-1, 0, 1, 1,
                                dbcommon::SMALLINTID);  // NULLTESTTYPE_IS_NULL
  upu.constructNullTestQualList(-1, 0, 1, 2,
                                dbcommon::INTID);  // NULLTESTTYPE_IS_NULL
  upu.constructNullTestQualList(-1, 0, 1, 3,
                                dbcommon::BIGINTID);  // NULLTESTTYPE_IS_NULL
  upu.constructNullTestQualList(-1, 0, 1, 4,
                                dbcommon::DOUBLEID);  // NULLTESTTYPE_IS_NULL
  upu.constructNullTestQualList(-1, 0, 1, 5,
                                dbcommon::FLOATID);  // NULLTESTTYPE_IS_NULL
  upu.constructNullTestQualList(-1, 0, 1, 6,
                                dbcommon::STRINGID);  // NULLTESTTYPE_IS_NULL
  upu.univPlanAddToPlanNodeTest(true);
  const univplan::UnivPlanPlanNodePoly *planNode =
      &upu.getUnivPlan()->upb.get()->getPlanBuilderPlan()->getPlan()->plan();
  const univplan::UnivPlanScanSeq &ss = planNode->scanseq();
  const univplan::UnivPlanExprPolyList *predicateExprs = &ss.super().quallist();
  LOG_INFO("plan=%s", upu.univPlanGetJsonFormatedPlan());
  generateTest("hildfs", predicateExprs, "TestFilterPushDown_IsNull", true,
               false);
}

TEST(TestORCFormat, TestFilterPushDown_IsNotNull) {
  univplan::UnivPlanProtoUtility upu;
  int32_t uid = upu.univPlanSeqScanNewInstance(-1);
  upu.constructNullTestQualList(
      -1, 1, 1, 1, dbcommon::SMALLINTID);  // NULLTESTTYPE_IS_NOT_NULL
  upu.constructNullTestQualList(-1, 1, 1, 2,
                                dbcommon::INTID);  // NULLTESTTYPE_IS_NOT_NULL
  upu.constructNullTestQualList(
      -1, 1, 1, 3, dbcommon::BIGINTID);  // NULLTESTTYPE_IS_NOT_NULL
  upu.constructNullTestQualList(
      -1, 1, 1, 4, dbcommon::DOUBLEID);  // NULLTESTTYPE_IS_NOT_NULL
  upu.constructNullTestQualList(-1, 1, 1, 5,
                                dbcommon::FLOATID);  // NULLTESTTYPE_IS_NOT_NULL
  upu.constructNullTestQualList(
      -1, 1, 1, 6, dbcommon::STRINGID);  // NULLTESTTYPE_IS_NOT_NULL
  upu.univPlanAddToPlanNodeTest(true);
  const univplan::UnivPlanPlanNodePoly *planNode =
      &upu.getUnivPlan()->upb.get()->getPlanBuilderPlan()->getPlan()->plan();
  const univplan::UnivPlanScanSeq &ss = planNode->scanseq();
  const univplan::UnivPlanExprPolyList *predicateExprs = &ss.super().quallist();
  LOG_INFO("plan=%s", upu.univPlanGetJsonFormatedPlan());
  generateTest("hildfs", predicateExprs, "TestFilterPushDown_IsNotNull", false,
               false);
}

void testVarOpConstThenOpVarOpConst(
    const std::string &pattern, const char *casename, int32_t opFuncId,
    int32_t opFuncId1, dbcommon::TypeKind varType1, const char *buffer1,
    int32_t opFuncId2, dbcommon::TypeKind varType2, const char *buffer2,
    bool shouldSkip1 = false, bool shouldSkip2 = true) {
  univplan::UnivPlanProtoUtility upu;
  int32_t uid = upu.univPlanSeqScanNewInstance(-1);
  upu.constructVarOpConstThenOpVarOpConstQualList(
      -1, opFuncId, opFuncId1, 1, 1, varType1, varType1, buffer1, opFuncId2, 1,
      2, varType2, varType2, buffer2);
  upu.univPlanAddToPlanNodeTest(true);
  const univplan::UnivPlanPlanNodePoly *planNode =
      &upu.getUnivPlan()->upb.get()->getPlanBuilderPlan()->getPlan()->plan();
  const univplan::UnivPlanScanSeq &ss = planNode->scanseq();
  const univplan::UnivPlanExprPolyList *predicateExprs = &ss.super().quallist();
  LOG_INFO("plan=%s", upu.univPlanGetJsonFormatedPlan());
  generateTest(pattern, predicateExprs, casename);
}

TEST(TestORCFormat, TestFilterPushDown_VarOpConstThenOpVarOpConst1) {
  testVarOpConstThenOpVarOpConst(
      "hh", "TestFilterPushDown_VarOpConstThenOpVarOpConst1",
      dbcommon::SMALLINT_LESS_THAN_SMALLINT, dbcommon::SMALLINT_ADD_SMALLINT,
      dbcommon::SMALLINTID, "29", dbcommon::SMALLINT_SUB_SMALLINT,
      dbcommon::SMALLINTID, "1");
}

TEST(TestORCFormat, TestFilterPushDown_VarOpConstThenOpVarOpConst2) {
  testVarOpConstThenOpVarOpConst(
      "ii", "TestFilterPushDown_VarOpConstThenOpVarOpConst2",
      dbcommon::INT_LESS_THAN_INT, dbcommon::INT_MUL_INT, dbcommon::INTID,
      "100", dbcommon::INT_ADD_INT, dbcommon::INTID, "22");
}

TEST(TestORCFormat, TestFilterPushDown_VarOpConstThenOpVarOpConst3) {
  testVarOpConstThenOpVarOpConst(
      "ll", "TestFilterPushDown_VarOpConstThenOpVarOpConst3",
      dbcommon::BIGINT_GREATER_THAN_BIGINT, dbcommon::BIGINT_SUB_BIGINT,
      dbcommon::BIGINTID, "29", dbcommon::BIGINT_MUL_BIGINT, dbcommon::BIGINTID,
      "1");
}

TEST(TestORCFormat, TestFilterPushDown_VarOpConstThenOpVarOpConst4) {
  testVarOpConstThenOpVarOpConst(
      "dd", "TestFilterPushDown_VarOpConstThenOpVarOpConst4",
      dbcommon::DOUBLE_GREATER_THAN_DOUBLE, dbcommon::DOUBLE_DIV_DOUBLE,
      dbcommon::DOUBLEID, "6", dbcommon::DOUBLE_MUL_DOUBLE, dbcommon::DOUBLEID,
      "5");
}

TEST(TestORCFormat, TestFilterPushDown_VarOpConstThenOpVarOpConst5) {
  testVarOpConstThenOpVarOpConst(
      "ff", "TestFilterPushDown_VarOpConstThenOpVarOpConst5",
      dbcommon::FLOAT_EQUAL_FLOAT, dbcommon::FLOAT_DIV_FLOAT, dbcommon::FLOATID,
      "3", dbcommon::FLOAT_ADD_FLOAT, dbcommon::FLOATID, "10");
}

}  // namespace storage
