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

#ifndef UNIVPLAN_SRC_UNIVPLAN_COMMON_PLANNODE_WALKER_H_
#define UNIVPLAN_SRC_UNIVPLAN_COMMON_PLANNODE_WALKER_H_

#include "univplan/proto/universal-plan.pb.h"

#include "dbcommon/log/logger.h"

namespace univplan {

class PlanNodeWalkerContext {
 public:
  PlanNodeWalkerContext() {}
  virtual ~PlanNodeWalkerContext() {}
};

class PlanNodeWalker {
 public:
  PlanNodeWalker() {}
  virtual ~PlanNodeWalker() {}

  virtual void walk(UnivPlanExprPoly *expr, PlanNodeWalkerContext *ctx) = 0;

  // Walk all dependent expressions.
  // @param expr Root expr
  // @param walker Apply walker->walk() for checking each expression
  // @param ctx Context for checking list
  static void walkExpressionTree(UnivPlanExprPoly *expr, PlanNodeWalker *walker,
                                 PlanNodeWalkerContext *ctx) {
    // Call walkExpressionTree to check all containing "UnivPlanExprPoly".
    // Pay attention to check the "optional UnivPlanExprPoly" exist before
    // calling walkExpressionTree.
    if (expr->has_var() || expr->has_val() || expr->has_param()) {
      // do nothing
    } else if (expr->has_aggref()) {
      UnivPlanAggref *aggref = expr->mutable_aggref();
      walkExpressionTree(aggref->mutable_args(), walker, ctx);
    } else if (expr->has_funcexpr()) {
      UnivPlanFuncExpr *funcExpr = expr->mutable_funcexpr();
      walkExpressionTree(funcExpr->mutable_args(), walker, ctx);
    } else if (expr->has_opexpr()) {
      UnivPlanOpExpr *opExpr = expr->mutable_opexpr();
      walkExpressionTree(opExpr->mutable_args(), walker, ctx);
    } else if (expr->has_boolexpr()) {
      UnivPlanBoolExpr *boolExpr = expr->mutable_boolexpr();
      walkExpressionTree(boolExpr->mutable_args(), walker, ctx);
    } else if (expr->has_nulltest()) {
      UnivPlanNullTest *nullTest = expr->mutable_nulltest();
      walkExpressionTree(nullTest->mutable_arg(), walker, ctx);
    } else if (expr->has_booltest()) {
      UnivPlanBooleanTest *boolTest = expr->mutable_booltest();
      walkExpressionTree(boolTest->mutable_arg(), walker, ctx);
    } else if (expr->has_targetentry()) {
      UnivPlanTargetEntry *targetEntry = expr->mutable_targetentry();
      walkExpressionTree(targetEntry->mutable_expression(), walker, ctx);
    } else if (expr->has_caseexpr()) {
      UnivPlanCaseExpr *caseexpr = expr->mutable_caseexpr();
      walkExpressionTree(caseexpr->mutable_args(), walker, ctx);
      walkExpressionTree(caseexpr->mutable_defresult(), walker, ctx);
    } else if (expr->has_casewhen()) {
      UnivPlanCaseWhen *casewhen = expr->mutable_casewhen();
      walkExpressionTree(casewhen->mutable_expr(), walker, ctx);
      walkExpressionTree(casewhen->mutable_result(), walker, ctx);
    } else if (expr->has_subplan()) {
      UnivPlanSubPlan *subplan = expr->mutable_subplan();
      walkExpressionTree(subplan->mutable_args(), walker, ctx);
      if (subplan->has_testexpr())
        walkExpressionTree(subplan->mutable_testexpr(), walker, ctx);
    } else if (expr->has_scalararrayopexpr()) {
      UnivPlanScalarArrayOpExpr *scalarArrayOpExpr =
          expr->mutable_scalararrayopexpr();
      walkExpressionTree(scalarArrayOpExpr->mutable_args(), walker, ctx);
    } else if (expr->has_coalesceexpr()) {
      UnivPlanCoalesceExpr *coalesceExpr = expr->mutable_coalesceexpr();
      walkExpressionTree(coalesceExpr->mutable_args(), walker, ctx);
    } else if (expr->has_nullifexpr()) {
      UnivPlanNullIfExpr *nullIfExpr = expr->mutable_nullifexpr();
      walkExpressionTree(nullIfExpr->mutable_args(), walker, ctx);
    } else if (expr->has_distinctexpr()) {
      UnivPlanDistinctExpr *distinctExpr = expr->mutable_distinctexpr();
      walkExpressionTree(distinctExpr->mutable_args(), walker, ctx);
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "PlanNodeUtil::expressionTreeWalker expr %d not supported",
                expr->type());
    }
    walker->walk(expr, ctx);
  }

  static void walkExpressionTree(UnivPlanExprPolyList *exprs,
                                 PlanNodeWalker *walker,
                                 PlanNodeWalkerContext *ctx) {
    for (int i = 0; i < exprs->size(); ++i) {
      walkExpressionTree(exprs->Mutable(i), walker, ctx);
    }
  }

  virtual void walk(UnivPlanPlanNodePoly *planNodePoly, PlanNodeWalker *walker,
                    PlanNodeWalkerContext *ctx) {}

  static void walkPlanTree(UnivPlanPlanNodePoly *planNodePoly,
                           PlanNodeWalker *walker, PlanNodeWalkerContext *ctx) {
    auto planNode = PlanNodeUtil::getMutablePlanNode(planNodePoly);
    if (planNode->has_leftplan())
      walkPlanTree(planNode->mutable_leftplan(), walker, ctx);
    if (planNode->has_rightplan())
      walkPlanTree(planNode->mutable_rightplan(), walker, ctx);
    if (planNodePoly->has_append()) {
      auto append = planNodePoly->mutable_append();
      for (auto i = 0; i < append->appendplans_size(); i++)
        walkPlanTree(append->mutable_appendplans(i), walker, ctx);
    }

    walker->walk(planNodePoly, walker, ctx);
  }
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_COMMON_PLANNODE_WALKER_H_
