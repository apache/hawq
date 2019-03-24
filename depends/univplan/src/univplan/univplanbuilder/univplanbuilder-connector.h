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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_CONNECTOR_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_CONNECTOR_H_

#include <memory>
#include <utility>
#include <vector>

#include "univplan/proto/universal-plan.pb.h"
#include "univplan/univplanbuilder/univplanbuilder-node.h"

namespace univplan {

class UnivPlanBuilderConnector : public UnivPlanBuilderNode {
 public:
  UnivPlanBuilderConnector() : UnivPlanBuilderNode() {
    planNode.reset(new UnivPlanConnector);
    ref = planNode.get();

    ref->set_allocated_super(UnivPlanBuilderNode::basePlanNode.release());
  }
  virtual ~UnivPlanBuilderConnector() {}

  typedef std::unique_ptr<UnivPlanBuilderConnector> uptr;

  UnivPlanBuilderPlanNodePoly::uptr ownPlanNode() override {
    UnivPlanBuilderPlanNodePoly::uptr pn(
        new UnivPlanBuilderPlanNodePoly(UNIVPLAN_CONNECTOR, planNode->super()));
    pn->getPlanNodePoly()->set_allocated_connector(planNode.release());
    return std::move(pn);
  }

  void from(const UnivPlanPlanNodePoly &node) override {
    assert(node.type() == UNIVPLAN_CONNECTOR);
    ref->CopyFrom(node.connector());
    baseRef = ref->mutable_super();
    baseRef->clear_leftplan();
    baseRef->clear_rightplan();
  }

  void setConnectorType(UNIVPLANCONNECTORTYPE type) { ref->set_type(type); }

  void setStageNo(int stageNo) { ref->set_stageno(stageNo); }

  void setMagmaTable(bool stageNo) { ref->set_magmatable(stageNo); }

  void setMagmaMap(int *map) {
    int rangeNum = ref->rangenum();
    for (int i = 0; i < rangeNum; ++i) {
      ref->add_magmamap(map[i]);
    }
  }

  void setRangeNum(int rangeNum) { ref->set_rangenum(rangeNum); }

  void setColIdx(const std::vector<int32_t> &nArray) {
    for (int i = 0; i < nArray.size(); ++i) {
      ref->add_colidx(nArray[i]);
    }
  }

  void setSortFuncId(const std::vector<int32_t> &nArray) {
    for (int i = 0; i < nArray.size(); ++i) {
      ref->add_sortfuncid(nArray[i]);
    }
  }

  void addHashExpr(UnivPlanBuilderExprTree::uptr exprTree) {
    assert(ref != nullptr);
    ref->mutable_hashexpr()->AddAllocated(exprTree->ownExprPoly().release());
  }

 private:
  UnivPlanConnector *ref;
  std::unique_ptr<UnivPlanConnector> planNode;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_CONNECTOR_H_
