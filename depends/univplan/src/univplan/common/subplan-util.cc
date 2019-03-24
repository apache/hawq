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

#include "univplan/common/subplan-util.h"
#include "univplan/common/plannode-util.h"
#include "univplan/common/plannode-walker.h"

namespace univplan {

class SubplanWalker : public PlanNodeWalker {
 public:
  void walk(UnivPlanPlanNodePoly* planNodePoly, PlanNodeWalker* walker,
            PlanNodeWalkerContext* ctx) override {
    auto planNode = PlanNodeUtil::getMutablePlanNode(planNodePoly);

    // Basic check
    PlanNodeWalker::walkExpressionTree(planNode->mutable_targetlist(), walker,
                                       ctx);
    PlanNodeWalker::walkExpressionTree(planNode->mutable_quallist(), walker,
                                       ctx);
    PlanNodeWalker::walkExpressionTree(planNode->mutable_initplan(), walker,
                                       ctx);

    // Special check for shuffle
    if (planNodePoly->has_shuffle()) {
      auto sf = planNodePoly->mutable_shuffle();
      PlanNodeWalker::walkExpressionTree(sf->mutable_hashexpr(), walker, ctx);
    }

    // Special check for joinQual
    if (planNodePoly->has_nestloop()) {
      auto nl = planNodePoly->mutable_nestloop();
      PlanNodeWalker::walkExpressionTree(nl->mutable_joinqual(), walker, ctx);
    }
    if (planNodePoly->has_hashjoin()) {
      auto hj = planNodePoly->mutable_hashjoin();
      PlanNodeWalker::walkExpressionTree(hj->mutable_joinqual(), walker, ctx);
      PlanNodeWalker::walkExpressionTree(hj->mutable_hashclauses(), walker,
                                         ctx);
      PlanNodeWalker::walkExpressionTree(hj->mutable_hashqualclauses(), walker,
                                         ctx);
    }
    if (planNodePoly->has_mergejoin()) {
      auto mj = planNodePoly->mutable_mergejoin();
      PlanNodeWalker::walkExpressionTree(mj->mutable_joinqual(), walker, ctx);
      PlanNodeWalker::walkExpressionTree(mj->mutable_mergeclauses(), walker,
                                         ctx);
    }

    // Special check for limit
    if (planNodePoly->has_limit()) {
      auto limit = planNodePoly->mutable_limit();
      if (limit->has_limitoffset())
        PlanNodeWalker::walkExpressionTree(limit->mutable_limitoffset(), walker,
                                           ctx);
      if (limit->has_limitcount())
        PlanNodeWalker::walkExpressionTree(limit->mutable_limitcount(), walker,
                                           ctx);
    }
    if (planNodePoly->has_sort()) {
      auto sort = planNodePoly->mutable_sort();
      if (sort->has_limitoffset())
        PlanNodeWalker::walkExpressionTree(sort->mutable_limitoffset(), walker,
                                           ctx);
      if (sort->has_limitcount())
        PlanNodeWalker::walkExpressionTree(sort->mutable_limitcount(), walker,
                                           ctx);
    }

    // Special check from result
    if (planNodePoly->has_result()) {
      auto result = planNodePoly->mutable_result();
      PlanNodeWalker::walkExpressionTree(result->mutable_resconstantqual(),
                                         walker, ctx);
    }
  }
};

class SubplanPlanidWalkerContext : public PlanNodeWalkerContext {
 public:
  std::vector<int32_t> planids;
};

class SubplanPlanidWalker : public SubplanWalker {
 public:
  void walk(UnivPlanExprPoly* expr, PlanNodeWalkerContext* ctx) override {
    if (expr->has_subplan() && !expr->subplan().initplan()) {
      SubplanPlanidWalkerContext* context =
          reinterpret_cast<SubplanPlanidWalkerContext*>(ctx);
      context->planids.push_back(expr->subplan().planid());
    }
  }
};

std::vector<int32_t> SubplanUtil::getRelatedSubplanIds(
    const UnivPlanPlanNodePoly& planNodePolyInput) {
  SubplanPlanidWalkerContext ctx;
  SubplanPlanidWalker walker;
  walker.SubplanWalker::walk(
      &(const_cast<UnivPlanPlanNodePoly&>(planNodePolyInput)), &walker, &ctx);
  return ctx.planids;
}

class SubplanAttachWalkerContext : public PlanNodeWalkerContext {
 public:
  const UnivPlanPlan* rootPlan;
  UnivPlanPlan* currStage;
};

class SubplanAttachWalker : public SubplanWalker {
 public:
  void walk(UnivPlanExprPoly* expr, PlanNodeWalkerContext* ctx) override {
    if (expr->has_subplan() && !expr->subplan().initplan()) {
      auto context = reinterpret_cast<SubplanAttachWalkerContext*>(ctx);
      // count plan ID from 1
      auto oldPlanId = expr->subplan().planid();
      context->currStage->add_subplans()->CopyFrom(
          context->rootPlan->subplans(oldPlanId - 1));
      expr->mutable_subplan()->set_planid(context->currStage->subplans_size());
    }
  }
};

void SubplanUtil::linkSubplan(UnivPlanPlan* rootPlan, UnivPlanPlan* currStage) {
  SubplanAttachWalker walker;
  SubplanAttachWalkerContext ctx;
  ctx.rootPlan = rootPlan;
  ctx.currStage = currStage;
  PlanNodeWalker::walkPlanTree(currStage->mutable_plan(), &walker, &ctx);
  for (auto i = 0; i < currStage->childstages_size(); i++) {
    linkSubplan(rootPlan, currStage->mutable_childstages(i));
  }
}

}  // namespace univplan
