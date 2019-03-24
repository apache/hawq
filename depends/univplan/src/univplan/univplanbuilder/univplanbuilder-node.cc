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

#include "univplan/univplanbuilder/univplanbuilder-node.h"

#include <utility>

namespace univplan {

void UnivPlanBuilderNode::setNodeLeftPlan(
    UnivPlanBuilderPlanNodePoly::uptr pn) {
  assert(baseRef != nullptr);
  baseRef->set_allocated_leftplan(pn->ownPlanNodePoly().release());
}

void UnivPlanBuilderNode::setNodeRightPlan(
    UnivPlanBuilderPlanNodePoly::uptr pn) {
  assert(baseRef != nullptr);
  baseRef->set_allocated_rightplan(pn->ownPlanNodePoly().release());
}

void UnivPlanBuilderNode::setNodePlanRows(double planRows) {
  assert(baseRef != nullptr);
  baseRef->set_planrows(planRows);
}

void UnivPlanBuilderNode::setNodePlanRowWidth(int32_t planRowWidth) {
  assert(baseRef != nullptr);
  baseRef->set_planrowwidth(planRowWidth);
}

void UnivPlanBuilderNode::setNodePlanOperatorMemKB(uint64_t operatorMemKB) {
  baseRef->set_operatormemkb(operatorMemKB);
}

UnivPlanBuilderTargetEntry::uptr
UnivPlanBuilderNode::addTargetEntryAndGetBuilder() {
  assert(baseRef != nullptr);
  UnivPlanExprPoly *exprTree = baseRef->add_targetlist();
  exprTree->set_type(UNIVPLAN_EXPR_TARGETENTRY);
  UnivPlanTargetEntry *targetEntry = exprTree->mutable_targetentry();
  UnivPlanBuilderTargetEntry::uptr bld(
      new UnivPlanBuilderTargetEntry(targetEntry));
  return std::move(bld);
}

void UnivPlanBuilderNode::addQualList(UnivPlanBuilderExprTree::uptr exprTree) {
  assert(baseRef != nullptr);
  baseRef->mutable_quallist()->AddAllocated(exprTree->ownExprPoly().release());
}

void UnivPlanBuilderNode::addInitplan(UnivPlanBuilderExprTree::uptr exprTree) {
  assert(baseRef != nullptr);
  baseRef->mutable_initplan()->AddAllocated(exprTree->ownExprPoly().release());
}

}  // namespace univplan
