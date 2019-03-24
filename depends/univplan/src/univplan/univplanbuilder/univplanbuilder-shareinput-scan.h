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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_SHAREINPUT_SCAN_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_SHAREINPUT_SCAN_H_

#include <memory>
#include <utility>

#include "univplan/proto/universal-plan.pb.h"
#include "univplan/univplanbuilder/univplanbuilder-node.h"

namespace univplan {

class UnivPlanBuilderShareInputScan : public UnivPlanBuilderNode {
 public:
  UnivPlanBuilderShareInputScan() : UnivPlanBuilderNode() {
    planNode_.reset(new UnivPlanShareInputScan());
    ref_ = planNode_.get();
    ref_->set_allocated_super(UnivPlanBuilderNode::basePlanNode.release());
  }
  virtual ~UnivPlanBuilderShareInputScan() {}

  UnivPlanBuilderPlanNodePoly::uptr ownPlanNode() override {
    UnivPlanBuilderPlanNodePoly::uptr pn(new UnivPlanBuilderPlanNodePoly(
        UNIVPLAN_SHAREINPUTSCAN, planNode_->super()));
    pn->getPlanNodePoly()->set_allocated_shareinputscan(planNode_.release());
    return std::move(pn);
  }

  void from(const UnivPlanPlanNodePoly &node) override {
    assert(node.type() == UNIVPLAN_SHAREINPUTSCAN);
    ref_->CopyFrom(node.shareinputscan());
    baseRef = ref_->mutable_super();
    baseRef->clear_leftplan();
    baseRef->clear_rightplan();
  }

  void setShareType(UNIVPLANSHARETYPE type) { ref_->set_sharetype(type); }

  void setShareId(int32_t shareId) { ref_->set_sharedid(shareId); }

  void setDriverSlice(int32_t driverSlice) {
    ref_->set_driverslice(driverSlice);
  }

 private:
  UnivPlanShareInputScan *ref_;
  std::unique_ptr<UnivPlanShareInputScan> planNode_;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_SHAREINPUT_SCAN_H_
