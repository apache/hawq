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

#include "univplan/common/plannode-util.h"
#include "univplan/common/stagize.h"
#include "univplan/common/var-util.h"
#include "univplan/cwrapper/univplan-c.h"
#include "univplan/testutil/univplan-proto-util.h"
#include "univplan/univplanbuilder/univplanbuilder-expr-tree.h"
#include "univplan/univplanbuilder/univplanbuilder-plan.h"
#include "univplan/univplanbuilder/univplanbuilder-table.h"
#include "univplan/univplanbuilder/univplanbuilder.h"

namespace univplan {

TEST(TestBasicUnivPlan, TestBasicUnivPlan) {
  univplan::UnivPlanBuilder upb;
  univplan::UnivPlanBuilderAgg *aggb;

  univplan::UnivPlanBuilderNode::uptr aggb1 =
      univplan::PlanNodeUtil::createPlanBuilderNode(univplan::UNIVPLAN_AGG);
  aggb = dynamic_cast<univplan::UnivPlanBuilderAgg *>(aggb1.get());

  aggb->setNumGroups(0);
  aggb->uid = 1;
  aggb->pid = -1;

  univplan::UnivPlanBuilderNode::uptr aggb2 =
      univplan::PlanNodeUtil::createPlanBuilderNode(univplan::UNIVPLAN_AGG);
  aggb = dynamic_cast<univplan::UnivPlanBuilderAgg *>(aggb2.get());
  aggb->setNumGroups(0);
  aggb->uid = 2;
  aggb->pid = 1;

  univplan::UnivPlanBuilderNode::uptr aggb3 =
      univplan::PlanNodeUtil::createPlanBuilderNode(univplan::UNIVPLAN_AGG);
  aggb = dynamic_cast<univplan::UnivPlanBuilderAgg *>(aggb3.get());
  aggb->setNumGroups(0);
  aggb->uid = 3;
  aggb->pid = 1;

  upb.addPlanNode(true, std::move(aggb1));
  upb.addPlanNode(true, std::move(aggb2));
  upb.addPlanNode(false, std::move(aggb3));

  UnivPlanPlan plan;
  plan.ParseFromString(upb.serialize());
  EXPECT_STREQ(upb.getJsonFormatedPlan().c_str(), plan.DebugString().c_str());
}

TEST(TestBasicUnivPlan, TestUnivPlanProtoGenerate) {
  UnivPlanProtoUtility upu;
  int32_t uid1 = upu.constructConnector(-1, 2, false);
  int32_t uid2 = upu.constructSeqScan(uid1, false, 0);
  upu.constructRangeTable();
  upu.constructReceiver();
  const char *jsonFormatPlan = upu.univPlanGetJsonFormatedPlan();
  UnivPlanPlanString plan;
  // plan.writeIntoFile("TestUnivPlanProtoGenerate", jsonFormatPlan);
  plan.readFromFile("TestUnivPlanProtoGenerate");
  const char *expecedPlan = plan.getPlanString();
  EXPECT_STREQ(expecedPlan, jsonFormatPlan);
}

TEST(TestBasicUnivPlan, TestLimitCountOffset) {
  UnivPlanProtoUtility upu;
  int32_t uid1 = upu.constructLimitTop(-1, 10, 100);
  int32_t uid2 = upu.constructConnector(uid1, 2, false);
  int32_t uid3 = upu.constructLimitBelow(uid2, 10, 100);
  int32_t uid4 = upu.constructSeqScan(uid3, false, 0);
  const char *jsonFormatPlan = upu.univPlanGetJsonFormatedPlan();
  UnivPlanPlanString plan;
  // plan.writeIntoFile("TestLimitCountOffset", jsonFormatPlan);
  plan.readFromFile("TestLimitCountOffset");
  const char *expecedPlan = plan.getPlanString();
  EXPECT_STREQ(expecedPlan, jsonFormatPlan);
}

TEST(TestBasicUnivPlan, TestLimitCount) {
  UnivPlanProtoUtility upu;
  int32_t uid1 = upu.constructLimitTop(-1, 10, -1);
  int32_t uid2 = upu.constructConnector(uid1, 2, false);
  int32_t uid3 = upu.constructLimitBelow(uid2, 10, -1);
  int32_t uid4 = upu.constructSeqScan(uid3, false, 0);
  const char *jsonFormatPlan = upu.univPlanGetJsonFormatedPlan();
  UnivPlanPlanString plan;
  // plan.writeIntoFile("TestLimitCount", jsonFormatPlan);
  plan.readFromFile("TestLimitCount");
  const char *expecedPlan = plan.getPlanString();
  EXPECT_STREQ(expecedPlan, jsonFormatPlan);
}

TEST(TestBasicUnivPlan, TestLimitOffset) {
  UnivPlanProtoUtility upu;
  int32_t uid1 = upu.constructLimitTop(-1, -1, 100);
  int32_t uid2 = upu.constructConnector(uid1, 2, false);
  int32_t uid3 = upu.constructLimitBelow(uid2, -1, 100);
  int32_t uid4 = upu.constructSeqScan(uid3, false, 0);
  const char *jsonFormatPlan = upu.univPlanGetJsonFormatedPlan();
  UnivPlanPlanString plan;
  // plan.writeIntoFile("TestLimitOffset", jsonFormatPlan);
  plan.readFromFile("TestLimitOffset");
  const char *expecedPlan = plan.getPlanString();
  EXPECT_STREQ(expecedPlan, jsonFormatPlan);
}

TEST(TestBasicUnivPlan, TestAgg) {
  UnivPlanProtoUtility upu;
  int32_t uid1 = upu.constructConnector(-1, 2, false);
  int32_t uid2 = upu.constructAgg(uid1, 3);
  int32_t uid3 = upu.constructConnector(uid2, 0, false);
  int32_t uid4 = upu.constructAgg(uid3, 1);
  int32_t uid5 = upu.constructSeqScan(uid4, false, 0);
  const char *jsonFormatPlan = upu.univPlanGetJsonFormatedPlan();
  UnivPlanPlanString plan;
  // plan.writeIntoFile("TestAgg", jsonFormatPlan);
  plan.readFromFile("TestAgg");
  const char *expecedPlan = plan.getPlanString();
  EXPECT_STREQ(expecedPlan, jsonFormatPlan);
}

TEST(TestBasicUnivPlan, TestSort) {
  UnivPlanProtoUtility upu;
  int32_t uid1 = upu.constructSortConnector(-1, 2);
  int32_t uid2 = upu.constructSort(uid1);
  int32_t uid3 = upu.constructSeqScan(uid2, false, 0);
  const char *jsonFormatPlan = upu.univPlanGetJsonFormatedPlan();
  UnivPlanPlanString plan;
  // plan.writeIntoFile("TestSort", jsonFormatPlan);
  plan.readFromFile("TestSort");
  const char *expecedPlan = plan.getPlanString();
  EXPECT_STREQ(expecedPlan, jsonFormatPlan);
}

TEST(TestBasicUnivPlan, TestQualListAndExpr) {
  UnivPlanProtoUtility upu;
  int32_t uid1 = upu.constructConnector(-1, 2, false);
  int32_t uid2 = upu.constructSeqScan(uid1, true, 0);
  upu.constructRangeTable();
  upu.constructReceiver();
  const char *jsonFormatPlan = upu.univPlanGetJsonFormatedPlan();
  UnivPlanPlanString plan;
  // plan.writeIntoFile("TestQualListAndExpr", jsonFormatPlan);
  plan.readFromFile("TestQualListAndExpr");
  const char *expecedPlan = plan.getPlanString();
  EXPECT_STREQ(expecedPlan, jsonFormatPlan);
}

TEST(TestBasicUnivPlan, TestNullTest) {
  UnivPlanProtoUtility upu;
  int32_t uid1 = upu.constructConnector(-1, 2, false);
  int32_t uid2 = upu.constructSeqScan(uid1, false, 2);
  upu.constructRangeTable();
  upu.constructReceiver();
  const char *jsonFormatPlan = upu.univPlanGetJsonFormatedPlan();
  UnivPlanPlanString plan;
  // plan.writeIntoFile("TestNullTest", jsonFormatPlan);
  plan.readFromFile("TestNullTest");
  const char *expecedPlan = plan.getPlanString();
  EXPECT_STREQ(expecedPlan, jsonFormatPlan);
}

TEST(TestUnivPlan, TestCommonValue) {
  univplan::UnivPlanBuilder upb;
  upb.getPlanBuilderPlan()->addCommonValue("abc", "value", nullptr);
  EXPECT_EQ(1, upb.getPlanBuilderPlan()->getPlan()->commonvalue().size());

  for (auto i = upb.getPlanBuilderPlan()->getPlan()->commonvalue().begin();
       i != upb.getPlanBuilderPlan()->getPlan()->commonvalue().end(); ++i) {
    LOG_INFO("key %s value %s", i->first.c_str(), i->second.c_str());
  }

  std::string fetch1 =
      upb.getPlanBuilderPlan()->getPlan()->commonvalue().find("abc")->second;
  LOG_INFO("pass 1");

  EXPECT_STREQ("value", fetch1.c_str());
  // add duplicate key when having no new key referenced, in this case, should
  // have old value overwrited
  upb.getPlanBuilderPlan()->addCommonValue("abc", "value2", nullptr);
  fetch1 = upb.getPlanBuilderPlan()->getPlan()->commonvalue().at("abc");
  EXPECT_STREQ("value2", fetch1.c_str());
  LOG_INFO("pass 2");
  // pass new key reference, duplicate key will cause new key generated
  std::string nkey1, nkey2;
  upb.getPlanBuilderPlan()->addCommonValue("dupkey1", "value1", &nkey1);
  fetch1 = upb.getPlanBuilderPlan()->getPlan()->commonvalue().at(nkey1);
  EXPECT_STREQ("value1", fetch1.c_str());
  LOG_INFO("pass 3");
  upb.getPlanBuilderPlan()->addCommonValue("dupkey1", "value2", &nkey2);
  fetch1 = upb.getPlanBuilderPlan()->getPlan()->commonvalue().at(nkey2);
  EXPECT_STREQ("value2", fetch1.c_str());
  EXPECT_STRNE(nkey1.c_str(), nkey2.c_str());
  LOG_INFO("pass 4");
  // passing duplicate key and its value, there should cause nothing updated
  upb.getPlanBuilderPlan()->addCommonValue("dupkey1", "value1", &nkey1);
  EXPECT_STREQ("dupkey10", nkey1.c_str());
  LOG_INFO("pass 5");
}

/*
 * SELECT count(int1), text from test2 where int1 > 1 or int2 < 10 and random()
 * < int1 group by int1, text limit 10 offset 100;
 */
TEST(TestUnivPlanCWrapper, TestCompletedPlan) {
  UnivPlanProtoUtility upu;
  int32_t uid1 = upu.constructLimitTop(-1, 10, 100);
  int32_t uid2 = upu.constructConnector(uid1, 2, true);
  int32_t uid3 = upu.constructLimitBelow(uid2, 10, 100);
  int32_t uid4 = upu.constructAgg(uid3, 3);
  int32_t uid5 = upu.constructConnector(uid4, 0, true);
  int32_t uid6 = upu.constructAgg(uid5, 1);
  int32_t uid7 = upu.constructSeqScan(uid6, true, 0);
  upu.constructRangeTable();
  upu.univPlanSetDoInstrument(true);
  upu.constructReceiver();

  const char *jsonFormatPlan1 = upu.univPlanGetJsonFormatedPlan();
  UnivPlanPlanString plan;
  // plan.writeIntoFile("TestCompletedPlanBefore", jsonFormatPlan1);
  plan.readFromFile("TestCompletedPlanBefore");
  const char *expecedPlan1 = plan.getPlanString();
  EXPECT_STREQ(expecedPlan1, jsonFormatPlan1);

  upu.univPlanStagize();

  const char *jsonFormatPlan2 = upu.univPlanGetJsonFormatedPlan();
  // plan.writeIntoFile("TestCompletedPlanAfter", jsonFormatPlan2);
  plan.readFromFile("TestCompletedPlanAfter");
  const char *expecedPlan2 = plan.getPlanString();
  EXPECT_STREQ(expecedPlan2, jsonFormatPlan2);
}

}  // namespace univplan
