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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_H_

#include <map>
#include <string>

#include "univplan/proto/universal-plan.pb.h"
#include "univplan/univplanbuilder/univplanbuilder-node.h"
#include "univplan/univplanbuilder/univplanbuilder-plan.h"

namespace univplan {

class UnivPlanBuilder {
 public:
  UnivPlanBuilder() {
    builder.reset(new UnivPlanBuilderPlan());
    idToPlanNodes.clear();
  }
  virtual ~UnivPlanBuilder() {}

  typedef std::unique_ptr<UnivPlanBuilder> uptr;

  int32_t getUid() { return nodeCounter++; }
  UnivPlanBuilderPlan *getPlanBuilderPlan() { return builder.get(); }
  UnivPlanBuilderPlan::uptr swapBuilder(UnivPlanBuilderPlan::uptr newBuilder);

  //----------------------------
  // Add plan node to this plan
  //----------------------------
  void addPlanNode(bool isleft,  // as its parent's left child
                   UnivPlanBuilderNode::uptr bld);

  //-----------------------------
  // Utility
  //-----------------------------
  std::string getJsonFormatedPlan();

  std::string serialize();

 private:
  UnivPlanBuilderPlan::uptr builder;

  int32_t nodeCounter = 0;
  std::map<int32_t, UnivPlanBuilderNode::uptr> idToPlanNodes;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_H_
