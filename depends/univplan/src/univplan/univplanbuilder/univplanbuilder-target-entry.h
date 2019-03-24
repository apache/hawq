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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_TARGET_ENTRY_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_TARGET_ENTRY_H_

#include "univplan/univplanbuilder/univplanbuilder-expr-node.h"
#include "univplan/univplanbuilder/univplanbuilder-expr-tree.h"

namespace univplan {

class UnivPlanBuilderTargetEntry : public UnivPlanBuilderExprNode {
 public:
  UnivPlanBuilderTargetEntry() {
    node.reset(new UnivPlanTargetEntry());
    ref = node.get();
  }

  explicit UnivPlanBuilderTargetEntry(UnivPlanTargetEntry *targetEntry)
      : ref(targetEntry) {}

  virtual ~UnivPlanBuilderTargetEntry() {}

  typedef std::unique_ptr<UnivPlanBuilderTargetEntry> uptr;

  UnivPlanBuilderExprPoly::uptr getExprPolyBuilder() {
    assert(node != nullptr);
    UnivPlanBuilderExprPoly::uptr exprPoly(
        new UnivPlanBuilderExprPoly(UNIVPLAN_EXPR_TARGETENTRY));
    exprPoly->getExprPoly()->set_allocated_targetentry(node.release());
    return std::move(exprPoly);
  }

  void setResJunk(bool resJunk) { ref->set_resjunk(resJunk); }

  void setExpr(UnivPlanBuilderExprPoly::uptr exprPoly) {
    ref->set_allocated_expression(exprPoly->ownExprPoly().release());
  }

  void setExpr(UnivPlanBuilderExprTree::uptr expr) {
    ref->set_allocated_expression(expr->ownExprPoly().release());
  }

 private:
  UnivPlanTargetEntry *ref = nullptr;
  std::unique_ptr<UnivPlanTargetEntry> node;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_TARGET_ENTRY_H_
