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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_HASHJOIN_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_HASHJOIN_H_

#include "univplan/proto/universal-plan.pb.h"
#include "univplan/univplanbuilder/univplanbuilder-node.h"

namespace univplan {

class UnivPlanBuilderHashJoin : public UnivPlanBuilderNode {
 public:
  UnivPlanBuilderHashJoin() : UnivPlanBuilderNode() {
    planNode.reset(new UnivPlanHashJoin());
    ref = planNode.get();
    ref->set_allocated_super(UnivPlanBuilderNode::basePlanNode.release());
  }
  virtual ~UnivPlanBuilderHashJoin() {}

  typedef std::unique_ptr<UnivPlanBuilderHashJoin> uptr;

  UnivPlanBuilderPlanNodePoly::uptr ownPlanNode() override {
    UnivPlanBuilderPlanNodePoly::uptr pn(
        new UnivPlanBuilderPlanNodePoly(UNIVPLAN_HASHJOIN, planNode->super()));
    pn->getPlanNodePoly()->set_allocated_hashjoin(planNode.release());
    return std::move(pn);
  }

  void from(const UnivPlanPlanNodePoly &node) override {
    assert(node.type() == UNIVPLAN_HASHJOIN);
    ref->CopyFrom(node.hashjoin());
    baseRef = ref->mutable_super();
    baseRef->clear_leftplan();
    baseRef->clear_rightplan();
  }

  void addJoinQual(UnivPlanBuilderExprTree::uptr exprTree) {
    assert(ref != nullptr);
    ref->mutable_joinqual()->AddAllocated(exprTree->ownExprPoly().release());
  }

  void addHashClause(UnivPlanBuilderExprTree::uptr exprTree) {
    assert(ref != nullptr);
    ref->mutable_hashclauses()->AddAllocated(exprTree->ownExprPoly().release());
  }

  void addHashQualClause(UnivPlanBuilderExprTree::uptr exprTree) {
    assert(ref != nullptr);
    ref->mutable_hashqualclauses()->AddAllocated(
        exprTree->ownExprPoly().release());
  }

  void setJoinType(UNIVPLANJOINTYPE type) { ref->set_type(type); }

 private:
  UnivPlanHashJoin *ref;
  std::unique_ptr<UnivPlanHashJoin> planNode;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_HASHJOIN_H_
