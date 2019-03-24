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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_NODE_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_NODE_H_

#include <memory>

#include "univplan/proto/universal-plan.pb.h"
#include "univplan/univplanbuilder/univplanbuilder-expr-tree.h"
#include "univplan/univplanbuilder/univplanbuilder-plan-node-poly.h"
#include "univplan/univplanbuilder/univplanbuilder-target-entry.h"

namespace univplan {

class UnivPlanBuilderNode {
 public:
  UnivPlanBuilderNode() {
    basePlanNode.reset(new UnivPlanPlanNode());
    baseRef = basePlanNode.get();
  }
  virtual ~UnivPlanBuilderNode() {}

  typedef std::unique_ptr<UnivPlanBuilderNode> uptr;

  void setNodeLeftPlan(UnivPlanBuilderPlanNodePoly::uptr pn);
  void setNodeRightPlan(UnivPlanBuilderPlanNodePoly::uptr pn);

  void setNodePlanRows(double planRows);

  void setNodePlanRowWidth(int32_t planRowWidth);

  void setNodePlanOperatorMemKB(uint64_t operatorMemKB);

  virtual std::unique_ptr<UnivPlanBuilderPlanNodePoly> ownPlanNode() = 0;

  // copy from the plannode content without left and right child
  virtual void from(const UnivPlanPlanNodePoly &node) = 0;

  UnivPlanBuilderTargetEntry::uptr addTargetEntryAndGetBuilder();
  void addQualList(UnivPlanBuilderExprTree::uptr exprTree);
  void addInitplan(UnivPlanBuilderExprTree::uptr exprTree);

 public:
  int32_t uid = -1;
  int32_t pid = -1;

 protected:
  std::unique_ptr<UnivPlanPlanNode> basePlanNode;
  UnivPlanPlanNode *baseRef = nullptr;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_NODE_H_
