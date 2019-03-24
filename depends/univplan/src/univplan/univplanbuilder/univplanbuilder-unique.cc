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

#include "univplan/univplanbuilder/univplanbuilder-unique.h"

#include <utility>

namespace univplan {

UnivPlanBuilderUnique::UnivPlanBuilderUnique() : UnivPlanBuilderNode() {
  planNode.reset(new UnivPlanUnique());
  ref = planNode.get();

  ref->set_allocated_super(UnivPlanBuilderNode::basePlanNode.release());
}

UnivPlanBuilderUnique::~UnivPlanBuilderUnique() {}

UnivPlanBuilderPlanNodePoly::uptr UnivPlanBuilderUnique::ownPlanNode() {
  UnivPlanBuilderPlanNodePoly::uptr pn(
      new UnivPlanBuilderPlanNodePoly(UNIVPLAN_UNIQUE, planNode->super()));
  pn->getPlanNodePoly()->set_allocated_unique(planNode.release());
  return std::move(pn);
}

void UnivPlanBuilderUnique::from(const UnivPlanPlanNodePoly &node) {
  assert(node.type() == UNIVPLAN_UNIQUE);
  ref->CopyFrom(node.unique());
  baseRef = ref->mutable_super();
  baseRef->clear_leftplan();
  baseRef->clear_rightplan();
}

void UnivPlanBuilderUnique::setUniqColIdxs(size_t numCols,
                                           const int32_t *uniqColIdxs) {
  for (int i = 0; i < numCols; ++i) {
    ref->add_uniqcolidxs(uniqColIdxs[i]);
  }
}

}  // namespace univplan
