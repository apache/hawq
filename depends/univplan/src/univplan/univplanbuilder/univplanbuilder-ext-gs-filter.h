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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_EXT_GS_FILTER_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_EXT_GS_FILTER_H_

#include <memory>
#include <vector>

#include "univplan/proto/universal-plan.pb.h"
#include "univplan/univplanbuilder/univplanbuilder-node.h"

namespace univplan {

class UnivPlanBuilderExtGSFilter : public UnivPlanBuilderNode {
 public:
  UnivPlanBuilderExtGSFilter();
  virtual ~UnivPlanBuilderExtGSFilter();

  typedef std::unique_ptr<UnivPlanBuilderExtGSFilter> uptr;

  std::unique_ptr<UnivPlanBuilderPlanNodePoly> ownPlanNode() override;

  void from(const UnivPlanPlanNodePoly &node) override;

  void setExpr(UnivPlanBuilderExprPoly::uptr exprPoly);

 private:
  UnivPlanExtGSFilter *ref;
  std::unique_ptr<UnivPlanExtGSFilter> planNode;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_EXT_GS_FILTER_H_
