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

#include "univplan/univplanbuilder/univplanbuilder-sort.h"

namespace univplan {

UnivPlanBuilderSort::UnivPlanBuilderSort() : UnivPlanBuilderNode() {
  planNode.reset(new UnivPlanSort());
  ref = planNode.get();

  ref->set_allocated_super(UnivPlanBuilderNode::basePlanNode.release());
}

UnivPlanBuilderPlanNodePoly::uptr UnivPlanBuilderSort::ownPlanNode() {
  UnivPlanBuilderPlanNodePoly::uptr pn(
      new UnivPlanBuilderPlanNodePoly(UNIVPLAN_SORT, planNode->super()));
  pn->getPlanNodePoly()->set_allocated_sort(planNode.release());
  return std::move(pn);
}

void UnivPlanBuilderSort::from(const UnivPlanPlanNodePoly &node) {
  assert(node.type() == UNIVPLAN_SORT);
  ref->CopyFrom(node.sort());
  baseRef = ref->mutable_super();
  baseRef->clear_leftplan();
  baseRef->clear_rightplan();
}

void UnivPlanBuilderSort::setColIdx(const std::vector<int32_t> &nArray) {
  for (int i = 0; i < nArray.size(); ++i) {
    ref->add_colidx(nArray[i]);
  }
}
void UnivPlanBuilderSort::setSortFuncId(const std::vector<int32_t> &nArray) {
  for (int i = 0; i < nArray.size(); ++i) {
    ref->add_sortfuncid(nArray[i]);
  }
}

void UnivPlanBuilderSort::setLimitOffset(UnivPlanBuilderExprTree::uptr offset) {
  ref->set_allocated_limitoffset(offset->ownExprPoly().release());
}

void UnivPlanBuilderSort::setLimitCount(UnivPlanBuilderExprTree::uptr count) {
  ref->set_allocated_limitcount(count->ownExprPoly().release());
}

}  // namespace univplan
