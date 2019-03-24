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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_SORT_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_SORT_H_

#include <vector>

#include "univplan/proto/universal-plan.pb.h"
#include "univplan/univplanbuilder/univplanbuilder-node.h"

namespace univplan {

class UnivPlanBuilderSort : public UnivPlanBuilderNode {
 public:
  UnivPlanBuilderSort();
  virtual ~UnivPlanBuilderSort() {}

  typedef std::unique_ptr<UnivPlanBuilderSort> uptr;

  UnivPlanBuilderPlanNodePoly::uptr ownPlanNode() override;

  void from(const UnivPlanPlanNodePoly &node) override;

  void setColIdx(const std::vector<int32_t> &nArray);
  void setSortFuncId(const std::vector<int32_t> &nArray);

  void setLimitOffset(UnivPlanBuilderExprTree::uptr limitOffset);
  void setLimitCount(UnivPlanBuilderExprTree::uptr limitCount);

 private:
  UnivPlanSort *ref;
  std::unique_ptr<UnivPlanSort> planNode;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_SORT_H_
