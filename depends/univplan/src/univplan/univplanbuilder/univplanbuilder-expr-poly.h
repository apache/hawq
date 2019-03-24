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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_EXPR_POLY_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_EXPR_POLY_H_

#include "dbcommon/log/logger.h"

#include "univplan/proto/universal-plan.pb.h"

namespace univplan {

class UnivPlanBuilderExprPoly {
 public:
  explicit UnivPlanBuilderExprPoly(UNIVPLANEXPRTYPE type) {
    exprPoly.reset(new UnivPlanExprPoly());
    ref = exprPoly.get();
    ref->set_type(type);
  }

  virtual ~UnivPlanBuilderExprPoly() {}

  typedef std::unique_ptr<UnivPlanBuilderExprPoly> uptr;

  UnivPlanExprPoly *getExprPoly() { return ref; }

  std::unique_ptr<UnivPlanExprPoly> ownExprPoly() {
    return std::move(exprPoly);
  }

 private:
  UnivPlanExprPoly *ref;
  std::unique_ptr<UnivPlanExprPoly> exprPoly;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_EXPR_POLY_H_
