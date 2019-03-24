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

#include "./test-univplan.h"

#include <errno.h>

#include "gtest/gtest.h"

#include "dbcommon/log/exception.h"
#include "dbcommon/log/logger.h"
#include "dbcommon/type/type-kind.h"

#include "univplan/cwrapper/univplan-c.h"
#include "univplan/testutil/univplan-proto-util.h"
#include "univplan/univplanbuilder/univplanbuilder.h"

namespace univplan {

TEST(TestUnivPlanCWrapper, TestUnivPlanVarutil) {
  UnivPlanProtoUtility upu;
  int32_t uid1 = upu.constructConnector(-1, 2, false);
  int32_t uid2 = upu.constructSeqScan(uid1, false, 0);
  upu.constructRangeTable();
  upu.constructReceiver();
  upu.univPlanFixVarType();
}

TEST(TestUnivPlanCWrapper, TestStagize) {
  UnivPlanProtoUtility upu;
  int32_t uid1 = upu.constructConnector(-1, 1, false);
  int32_t uid2 = upu.constructSeqScan(uid1, false, 0);
  upu.constructReceiver();

  const char *jsonFormatPlan1 = upu.univPlanGetJsonFormatedPlan();
  UnivPlanPlanString plan;
  // plan.writeIntoFile("TestStagizeBefore", jsonFormatPlan1);
  plan.readFromFile("TestStagizeBefore");
  //  puts("--TestStagizeBefore--");
  //  puts(jsonFormatPlan1);
  const char *expecedPlan1 = plan.getPlanString();
  EXPECT_STREQ(expecedPlan1, jsonFormatPlan1);

  upu.univPlanStagize();

  const char *jsonFormatPlan2 = upu.univPlanGetJsonFormatedPlan();
  // plan.writeIntoFile("TestStagizeAfter", jsonFormatPlan2);
  plan.readFromFile("TestStagizeAfter");
  //  puts("--TestStagizeAfter--");
  //  puts(jsonFormatPlan2);
  const char *expecedPlan2 = plan.getPlanString();
  EXPECT_STREQ(expecedPlan2, jsonFormatPlan2);
}

TEST(TestUnivPlanCWrapper, TestSerialize) {
  UnivPlanProtoUtility upu;
  int32_t uid1 = upu.constructConnector(-1, 2, false);
  int32_t uid2 = upu.constructSeqScan(uid1, false, 0);
  upu.constructRangeTable();
  upu.constructReceiver();
  int32_t len;
  upu.univPlanSerialize(&len);
  EXPECT_EQ(406, len);
}

TEST(TestUnivPlanCWrapper, TestSetDoInstrument) {
  UnivPlanProtoUtility upu;
  int32_t uid1 = upu.constructConnector(-1, 2, false);
  int32_t uid2 = upu.constructSeqScan(uid1, false, 0);
  upu.constructRangeTable();
  upu.univPlanSetDoInstrument(true);
}

}  // namespace univplan
