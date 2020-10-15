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

#include "univplan/univplanbuilder/univplanbuilder-insert.h"

namespace univplan {

UnivPlanBuilderInsert::UnivPlanBuilderInsert() : UnivPlanBuilderNode() {
  planNode.reset(new UnivPlanInsert());
  ref = planNode.get();

  ref->set_allocated_super(UnivPlanBuilderNode::basePlanNode.release());
}

UnivPlanBuilderInsert::~UnivPlanBuilderInsert() {}

UnivPlanBuilderPlanNodePoly::uptr UnivPlanBuilderInsert::ownPlanNode() {
  UnivPlanBuilderPlanNodePoly::uptr pn(
      new UnivPlanBuilderPlanNodePoly(UNIVPLAN_INSERT, planNode->super()));
  pn->getPlanNodePoly()->set_allocated_insert(planNode.release());
  return std::move(pn);
}

void UnivPlanBuilderInsert::from(const UnivPlanPlanNodePoly &node) {
  assert(node.type() == UNIVPLAN_INSERT);
  ref->CopyFrom(node.insert());
  baseRef = ref->mutable_super();
  baseRef->clear_leftplan();
  baseRef->clear_rightplan();
}

void UnivPlanBuilderInsert::setInsertRelId(uint32_t id) { ref->set_relid(id); }

void UnivPlanBuilderInsert::setInsertHasher(
    int32_t nDistKeyIndex, int16_t *distKeyIndex, int32_t nRanges,
    uint32_t *rangeToRgMap, int16_t nRg, uint16_t *rgIds, const char **rgUrls) {
  for (int32_t i = 0; i < nDistKeyIndex; i++) {
    if (distKeyIndex[i] > 0) {
      ref->mutable_hasher()->mutable_hashkeys()->Add(distKeyIndex[i]);
    }
  }
  if (ref->mutable_hasher()->mutable_hashkeys()->size() == 0) {
    ref->mutable_hasher()->mutable_hashkeys()->Add(1);
  }
  for (int i = 0; i < nRanges; i++) {
    (*ref->mutable_hasher()->mutable_range2rg())[i] = rangeToRgMap[i];
  }
  for (int16_t i = 0; i < nRg; i++) {
    (*ref->mutable_hasher()->mutable_r2u())[rgIds[i]] = rgUrls[i];
  }
}

}  // namespace univplan
