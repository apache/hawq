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

#include "univplan/common/var-util.h"

#include <utility>

#include "dbcommon/log/logger.h"
#include "dbcommon/utils/macro.h"

#include "univplan/common/plannode-util.h"
#include "univplan/common/plannode-walker.h"

namespace univplan {

class FixVarTypeContext : public PlanNodeWalkerContext {
 public:
  FixVarTypeContext() {}
  virtual ~FixVarTypeContext() {}

 public:
  UnivPlanPlanNode* planNode = nullptr;
};

class FixVarTypeWalker : public PlanNodeWalker {
 public:
  void walk(UnivPlanExprPoly* expr, PlanNodeWalkerContext* ctx) override {
    if (expr->has_var()) {
      FixVarTypeContext* context = reinterpret_cast<FixVarTypeContext*>(ctx);
      UnivPlanPlanNode* planNode = context->planNode;
      UnivPlanVar* var = expr->mutable_var();
      if (var->varno() == OUTER_VAR && planNode->has_leftplan()) {
        UnivPlanPlanNode* leftPlanNode =
            PlanNodeUtil::getMutablePlanNode(planNode->mutable_leftplan());
        UnivPlanExprPoly* refTargetEntry =
            leftPlanNode->mutable_targetlist(var->varattno() - 1);
        var->set_typeid_(
            static_cast<int32_t>(PlanNodeUtil::exprType(*refTargetEntry)));
      } else if (var->varno() == INNER_VAR && planNode->has_rightplan()) {
        UnivPlanPlanNode* rightPlanNode =
            PlanNodeUtil::getMutablePlanNode(planNode->mutable_rightplan());
        UnivPlanExprPoly* refTargetEntry =
            rightPlanNode->mutable_targetlist(var->varattno() - 1);
        var->set_typeid_(
            static_cast<int32_t>(PlanNodeUtil::exprType(*refTargetEntry)));
      }
    }
  }
};

void VarUtil::fixVarType(UnivPlanPlanNodePoly* plantree) {
  UnivPlanPlanNode* planNode = PlanNodeUtil::getMutablePlanNode(plantree);

  // workaround here
  if (plantree->type() == UNIVPLAN_APPEND) return;

  if (planNode->has_leftplan()) fixVarType(planNode->mutable_leftplan());
  if (planNode->has_rightplan()) fixVarType(planNode->mutable_rightplan());
  FixVarTypeContext walkerContext;
  walkerContext.planNode = planNode;
  FixVarTypeWalker walker;

  PlanNodeWalker::walkExpressionTree(planNode->mutable_targetlist(), &walker,
                                     &walkerContext);
}

class CollectVarTypeContext : public PlanNodeWalkerContext {
 public:
  CollectVarTypeContext() {}
  virtual ~CollectVarTypeContext() {}

 public:
  std::vector<int32_t> varAttNoVec;
};

class CollectVarTypeWalker : public PlanNodeWalker {
 public:
  void walk(UnivPlanExprPoly* expr, PlanNodeWalkerContext* ctx) override {
    if (expr->has_var()) {
      CollectVarTypeContext* context =
          reinterpret_cast<CollectVarTypeContext*>(ctx);
      context->varAttNoVec.push_back(expr->var().varattno());
    }
  }
};

std::vector<int32_t> VarUtil::collectVarAttNo(UnivPlanExprPoly* expr) {
  CollectVarTypeContext walkerContext;
  CollectVarTypeWalker walker;

  PlanNodeWalker::walkExpressionTree(expr, &walker, &walkerContext);
  return std::move(walkerContext.varAttNoVec);
}

}  // namespace univplan
