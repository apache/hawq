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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_MATERIAL_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_MATERIAL_H_

#include <memory>
#include <utility>

#include "univplan/proto/universal-plan.pb.h"
#include "univplan/univplanbuilder/univplanbuilder-node.h"

namespace univplan {

class UnivPlanBuilderMaterial : public UnivPlanBuilderNode {
 public:
  UnivPlanBuilderMaterial() : UnivPlanBuilderNode() {
    planNode.reset(new UnivPlanMaterial());
    ref = planNode.get();
    ref->set_allocated_super(UnivPlanBuilderNode::basePlanNode.release());
  }
  virtual ~UnivPlanBuilderMaterial() {}

  typedef std::unique_ptr<UnivPlanBuilderMaterial> uptr;

  UnivPlanBuilderPlanNodePoly::uptr ownPlanNode() override {
    UnivPlanBuilderPlanNodePoly::uptr pn(
        new UnivPlanBuilderPlanNodePoly(UNIVPLAN_MATERIAL, planNode->super()));
    pn->getPlanNodePoly()->set_allocated_material(planNode.release());
    return std::move(pn);
  }

  void from(const UnivPlanPlanNodePoly& node) override {
    assert(node.type() == UNIVPLAN_MATERIAL);
    ref->CopyFrom(node.material());
    baseRef = ref->mutable_super();
    baseRef->clear_leftplan();
    baseRef->clear_rightplan();
  }

  UnivPlanBuilderMaterial& setShareType(UNIVPLANSHARETYPE type) {
    ref->set_share_type(type);
    return *this;
  }

  UnivPlanBuilderMaterial& setCdbStrict(bool cdbStrict) {
    ref->set_cdbstrict(cdbStrict);
    return *this;
  }

  UnivPlanBuilderMaterial& setShareId(int32_t shareId) {
    ref->set_shared_id(shareId);
    return *this;
  }

  UnivPlanBuilderMaterial& setDriverSlice(int32_t driverSlice) {
    ref->set_driver_slice(driverSlice);
    return *this;
  }

  UnivPlanBuilderMaterial& setNSharer(int32_t nsharer) {
    ref->set_nsharer(nsharer);
    return *this;
  }

  UnivPlanBuilderMaterial& setNSharerXSlice(int32_t xslice) {
    ref->set_nsharer_xslice(xslice);
    return *this;
  }

 private:
  UnivPlanMaterial* ref;
  std::unique_ptr<UnivPlanMaterial> planNode;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_MATERIAL_H_
