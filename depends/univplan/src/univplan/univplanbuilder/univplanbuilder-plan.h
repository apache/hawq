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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_PLAN_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_PLAN_H_

#include <memory>
#include <string>
#include <vector>

#include "univplan/proto/universal-plan.pb.h"
#include "univplan/univplanbuilder/univplanbuilder-node.h"
#include "univplan/univplanbuilder/univplanbuilder-paraminfo.h"
#include "univplan/univplanbuilder/univplanbuilder-range-tbl-entry.h"
#include "univplan/univplanbuilder/univplanbuilder-receiver.h"

namespace univplan {

class UnivPlanBuilderPlan {
 public:
  UnivPlanBuilderPlan() {
    plan.reset(new UnivPlanPlan());
    ref = plan.get();
  }

  explicit UnivPlanBuilderPlan(UnivPlanPlan *plan) : ref(plan) {}

  virtual ~UnivPlanBuilderPlan() {}

  typedef std::unique_ptr<UnivPlanBuilderPlan> uptr;

  UnivPlanPlan *getPlan() { return ref; }
  UnivPlanPlan *ownPlan() { return plan.release(); }

  // set root node of the plan
  void setPlanNode(UnivPlanBuilderPlanNodePoly::uptr pn);
  void setSubplanNode(UnivPlanBuilderPlanNodePoly::uptr pn);
  bool isSettingSubplan() { return ref->has_plan(); }

  void setStageNo(int32_t stageNo);
  void setDoInstrument(bool doInstrument);
  void setNCrossLevelParams(int32_t nCrossLevelParams);
  void setCmdType(UNIVPLANCMDTYPE cmdType);

  void from(const UnivPlanPlan &from);
  void setTokenEntry(std::string protocol, std::string host, int port,
                     std::string token);
  void setSnapshot(const std::string &snapshot);

  void addGuc(const std::string &name, const std::string &value);

  void addCommonValue(const std::string &key, const std::string &value,
                      std::string *newKey);

  UnivPlanBuilderRangeTblEntry::uptr addRangeTblEntryAndGetBuilder();

  UnivPlanBuilderReceiver::uptr addReceiverAndGetBuilder();

  UnivPlanBuilderParamInfo::uptr addParamInfoAndGetBuilder();

  UnivPlanBuilderPlan::uptr addChildStageAndGetBuilder();

 private:
  UnivPlanPlan *ref;
  std::unique_ptr<UnivPlanPlan> plan;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_PLAN_H_
