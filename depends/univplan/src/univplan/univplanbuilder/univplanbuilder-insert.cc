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

}  // namespace univplan
