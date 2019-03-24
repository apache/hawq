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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_EXT_GS_PROJ_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_EXT_GS_PROJ_H_

#include <memory>
#include <vector>

#include "univplan/proto/universal-plan.pb.h"
#include "univplan/univplanbuilder/univplanbuilder-node.h"

namespace univplan {

class UnivPlanBuilderExtGSProject : public UnivPlanBuilderNode {
 public:
  UnivPlanBuilderExtGSProject();
  virtual ~UnivPlanBuilderExtGSProject();

  typedef std::unique_ptr<UnivPlanBuilderExtGSProject> uptr;

  std::unique_ptr<UnivPlanBuilderPlanNodePoly> ownPlanNode() override;

  void from(const UnivPlanPlanNodePoly &node) override;

  void setColumnIndexes(const std::vector<int32_t> &colIndexes);

 private:
  UnivPlanExtGSProj *ref;
  std::unique_ptr<UnivPlanExtGSProj> planNode;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_EXT_GS_PROJ_H_
