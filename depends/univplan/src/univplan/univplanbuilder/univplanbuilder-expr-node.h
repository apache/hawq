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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_EXPR_NODE_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_EXPR_NODE_H_

#include <memory>
#include <string>
#include <utility>

#include "univplan/proto/universal-plan.pb.h"
#include "univplan/univplanbuilder/univplanbuilder-expr-poly.h"

namespace univplan {

class UnivPlanBuilderExprNode {
 public:
  typedef std::unique_ptr<UnivPlanBuilderExprNode> uptr;

  UnivPlanBuilderExprNode() {}

  UnivPlanBuilderExprNode(int32_t pid, int32_t uid) : pid(pid), uid(uid) {}

  virtual ~UnivPlanBuilderExprNode() {}

  // Create a UnivPlanBuilderExprPoly instance and transfer out the ownership.
  virtual std::unique_ptr<UnivPlanBuilderExprPoly> getExprPolyBuilder() = 0;

  virtual UnivPlanBuilderExprNode &addArgs(UnivPlanBuilderExprPoly::uptr arg) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "add args not implemented");
  }

  int32_t pid = -1;
  int32_t uid = -1;
};

class UnivPlanBuilderVar final : public UnivPlanBuilderExprNode {
 public:
  typedef std::unique_ptr<UnivPlanBuilderVar> uptr;

  UnivPlanBuilderVar() : node_(new UnivPlanVar), ref_(node_.get()) {}

  UnivPlanBuilderVar(int32_t pid, int32_t uid)
      : UnivPlanBuilderExprNode(pid, uid),
        node_(new UnivPlanVar),
        ref_(node_.get()) {}

  UnivPlanBuilderExprPoly::uptr getExprPolyBuilder() override {
    UnivPlanBuilderExprPoly::uptr exprPoly(
        new UnivPlanBuilderExprPoly(UNIVPLAN_EXPR_VAR));
    exprPoly->getExprPoly()->set_allocated_var(node_.release());
    return exprPoly;
  }

  UnivPlanBuilderVar &setVarNo(uint32_t varNo) {
    ref_->set_varno(varNo);
    return *this;
  }

  UnivPlanBuilderVar &setVarAttNo(int32_t varAttNo) {
    ref_->set_varattno(varAttNo);
    return *this;
  }

  UnivPlanBuilderVar &setTypeId(int32_t typeId) {
    ref_->set_typeid_(typeId);
    return *this;
  }

  UnivPlanBuilderVar &setTypeMod(int64_t typeMod) {
    ref_->set_typemod(typeMod);
    return *this;
  }

 private:
  std::unique_ptr<UnivPlanVar> node_;
  UnivPlanVar *ref_;
};

class UnivPlanBuilderConst final : public UnivPlanBuilderExprNode {
 public:
  typedef std::unique_ptr<UnivPlanBuilderConst> uptr;

  UnivPlanBuilderConst() : node_(new UnivPlanConst), ref_(node_.get()) {}

  UnivPlanBuilderConst(int32_t pid, int32_t uid)
      : UnivPlanBuilderExprNode(pid, uid),
        node_(new UnivPlanConst),
        ref_(node_.get()) {}

  UnivPlanBuilderExprPoly::uptr getExprPolyBuilder() override {
    UnivPlanBuilderExprPoly::uptr exprPoly(
        new UnivPlanBuilderExprPoly(UNIVPLAN_EXPR_CONST));
    exprPoly->getExprPoly()->set_allocated_val(node_.release());
    return exprPoly;
  }

  UnivPlanBuilderConst &setType(int32_t type) {
    ref_->set_type(type);
    return *this;
  }

  UnivPlanBuilderConst &setTypeMod(int64_t typeMod) {
    ref_->set_typemod(typeMod);
    return *this;
  }

  UnivPlanBuilderConst &setIsNull(bool isNull) {
    ref_->set_isnull(isNull);
    return *this;
  }

  UnivPlanBuilderConst &setValue(const std::string &value) {
    ref_->set_value(value);
    return *this;
  }

 private:
  std::unique_ptr<UnivPlanConst> node_;
  UnivPlanConst *ref_;
};

class UnivPlanBuilderOpExpr final : public UnivPlanBuilderExprNode {
 public:
  typedef std::unique_ptr<UnivPlanBuilderOpExpr> uptr;

  UnivPlanBuilderOpExpr() : node_(new UnivPlanOpExpr), ref_(node_.get()) {}

  UnivPlanBuilderOpExpr(int32_t pid, int32_t uid)
      : UnivPlanBuilderExprNode(pid, uid),
        node_(new UnivPlanOpExpr),
        ref_(node_.get()) {}

  UnivPlanBuilderExprPoly::uptr getExprPolyBuilder() override {
    UnivPlanBuilderExprPoly::uptr exprPoly(
        new UnivPlanBuilderExprPoly(UNIVPLAN_EXPR_OPEXPR));
    exprPoly->getExprPoly()->set_allocated_opexpr(node_.release());
    return exprPoly;
  }

  UnivPlanBuilderExprNode &addArgs(UnivPlanBuilderExprPoly::uptr arg) override {
    ref_->mutable_args()->AddAllocated(arg->ownExprPoly().release());
    return *this;
  }

  UnivPlanBuilderOpExpr &setRetType(int32_t retType) {
    ref_->set_rettype(retType);
    return *this;
  }

  UnivPlanBuilderOpExpr &setFuncId(int32_t funcId) {
    ref_->set_funcid(funcId);
    return *this;
  }

 private:
  std::unique_ptr<UnivPlanOpExpr> node_;
  UnivPlanOpExpr *ref_;
};

class UnivPlanBuilderFuncExpr final : public UnivPlanBuilderExprNode {
 public:
  typedef std::unique_ptr<UnivPlanBuilderFuncExpr> uptr;

  UnivPlanBuilderFuncExpr() : node_(new UnivPlanFuncExpr), ref_(node_.get()) {}

  UnivPlanBuilderFuncExpr(int32_t pid, int32_t uid)
      : UnivPlanBuilderExprNode(pid, uid),
        node_(new UnivPlanFuncExpr),
        ref_(node_.get()) {}

  UnivPlanBuilderExprPoly::uptr getExprPolyBuilder() override {
    UnivPlanBuilderExprPoly::uptr exprPoly(
        new UnivPlanBuilderExprPoly(UNIVPLAN_EXPR_FUNCEXPR));
    exprPoly->getExprPoly()->set_allocated_funcexpr(node_.release());
    return exprPoly;
  }

  UnivPlanBuilderFuncExpr &setFuncId(int32_t funcId) {
    ref_->set_funcid(funcId);
    return *this;
  }

  UnivPlanBuilderFuncExpr &setRetType(int32_t retType) {
    ref_->set_rettype(retType);
    return *this;
  }

  UnivPlanBuilderExprNode &addArgs(UnivPlanBuilderExprPoly::uptr arg) override {
    ref_->mutable_args()->AddAllocated(arg->ownExprPoly().release());
    return *this;
  }

 private:
  std::unique_ptr<UnivPlanFuncExpr> node_;
  UnivPlanFuncExpr *ref_;
};

class UnivPlanBuilderAggref final : public UnivPlanBuilderExprNode {
 public:
  typedef std::unique_ptr<UnivPlanBuilderAggref> uptr;

  UnivPlanBuilderAggref() : node_(new UnivPlanAggref), ref_(node_.get()) {}

  UnivPlanBuilderAggref(int32_t pid, int32_t uid)
      : UnivPlanBuilderExprNode(pid, uid),
        node_(new UnivPlanAggref),
        ref_(node_.get()) {}

  UnivPlanBuilderExprPoly::uptr getExprPolyBuilder() override {
    UnivPlanBuilderExprPoly::uptr exprPoly(
        new UnivPlanBuilderExprPoly(UNIVPLAN_EXPR_AGGREF));
    exprPoly->getExprPoly()->set_allocated_aggref(node_.release());
    return exprPoly;
  }

  UnivPlanBuilderAggref &setFuncId(int32_t funcId) {
    ref_->set_funcid(funcId);

    return *this;
  }

  UnivPlanBuilderAggref &setTransFuncId(int32_t funcId) {
    ref_->set_transfuncid(funcId);
    return *this;
  }

  UnivPlanBuilderAggref &setFinalFuncId(int32_t funcId) {
    ref_->set_finalfuncid(funcId);
    return *this;
  }

  UnivPlanBuilderAggref &setRetType(int32_t retType) {
    ref_->set_rettype(retType);
    return *this;
  }

  UnivPlanBuilderAggref &setTransInitVal(bool transInitVal) {
    ref_->set_transinitval(transInitVal);
    return *this;
  }

  UnivPlanBuilderExprNode &addArgs(UnivPlanBuilderExprPoly::uptr arg) override {
    ref_->mutable_args()->AddAllocated(arg->ownExprPoly().release());
    return *this;
  }

 private:
  std::unique_ptr<UnivPlanAggref> node_;
  UnivPlanAggref *ref_;
};

class UnivPlanBuilderBoolExpr final : public UnivPlanBuilderExprNode {
 public:
  typedef std::unique_ptr<UnivPlanBuilderBoolExpr> uptr;

  UnivPlanBuilderBoolExpr() : node_(new UnivPlanBoolExpr), ref_(node_.get()) {}

  UnivPlanBuilderBoolExpr(int32_t pid, int32_t uid)
      : UnivPlanBuilderExprNode(pid, uid),
        node_(new UnivPlanBoolExpr),
        ref_(node_.get()) {}

  UnivPlanBuilderExprPoly::uptr getExprPolyBuilder() override {
    UnivPlanBuilderExprPoly::uptr exprPoly(
        new UnivPlanBuilderExprPoly(UNIVPLAN_EXPR_BOOLEXPR));
    exprPoly->getExprPoly()->set_allocated_boolexpr(node_.release());
    return exprPoly;
  }

  UnivPlanBuilderBoolExpr &setType(BOOLEXPRTYPE type) {
    ref_->set_type(type);
    return *this;
  }

  UnivPlanBuilderExprNode &addArgs(UnivPlanBuilderExprPoly::uptr arg) override {
    ref_->mutable_args()->AddAllocated(arg->ownExprPoly().release());
    return *this;
  }

 private:
  std::unique_ptr<UnivPlanBoolExpr> node_;
  UnivPlanBoolExpr *ref_;
};

class UnivPlanBuilderNullTest final : public UnivPlanBuilderExprNode {
 public:
  typedef std::unique_ptr<UnivPlanBuilderNullTest> uptr;

  UnivPlanBuilderNullTest() : node_(new UnivPlanNullTest), ref_(node_.get()) {}

  UnivPlanBuilderNullTest(int32_t pid, int32_t uid)
      : UnivPlanBuilderExprNode(pid, uid),
        node_(new UnivPlanNullTest),
        ref_(node_.get()) {}

  UnivPlanBuilderExprPoly::uptr getExprPolyBuilder() override {
    UnivPlanBuilderExprPoly::uptr exprPoly(
        new UnivPlanBuilderExprPoly(UNIVPLAN_EXPR_NULLTEST));
    exprPoly->getExprPoly()->set_allocated_nulltest(node_.release());
    return exprPoly;
  }

  UnivPlanBuilderNullTest &setType(NULLTESTTYPE type) {
    ref_->set_type(type);
    return *this;
  }

  UnivPlanBuilderExprNode &addArgs(UnivPlanBuilderExprPoly::uptr arg) override {
    ref_->set_allocated_arg(arg->ownExprPoly().release());
    return *this;
  }

 private:
  std::unique_ptr<UnivPlanNullTest> node_;
  UnivPlanNullTest *ref_;
};

class UnivPlanBuilderBooleanTest final : public UnivPlanBuilderExprNode {
 public:
  typedef std::unique_ptr<UnivPlanBuilderBooleanTest> uptr;

  UnivPlanBuilderBooleanTest()
      : node_(new UnivPlanBooleanTest), ref_(node_.get()) {}

  UnivPlanBuilderBooleanTest(int32_t pid, int32_t uid)
      : UnivPlanBuilderExprNode(pid, uid),
        node_(new UnivPlanBooleanTest),
        ref_(node_.get()) {}

  UnivPlanBuilderExprPoly::uptr getExprPolyBuilder() override {
    UnivPlanBuilderExprPoly::uptr exprPoly(
        new UnivPlanBuilderExprPoly(UNIVPLAN_EXPR_BOOLEANTEST));
    exprPoly->getExprPoly()->set_allocated_booltest(node_.release());
    return exprPoly;
  }

  UnivPlanBuilderBooleanTest &setType(BOOLTESTTYPE type) {
    ref_->set_type(type);
    return *this;
  }

  UnivPlanBuilderExprNode &addArgs(UnivPlanBuilderExprPoly::uptr arg) override {
    ref_->set_allocated_arg(arg->ownExprPoly().release());
    return *this;
  }

 private:
  std::unique_ptr<UnivPlanBooleanTest> node_;
  UnivPlanBooleanTest *ref_;
};

class UnivPlanBuilderCaseExpr final : public UnivPlanBuilderExprNode {
 public:
  typedef std::unique_ptr<UnivPlanBuilderCaseExpr> uptr;

  UnivPlanBuilderCaseExpr() : node_(new UnivPlanCaseExpr), ref_(node_.get()) {}

  UnivPlanBuilderCaseExpr(int32_t pid, int32_t uid)
      : UnivPlanBuilderExprNode(pid, uid),
        node_(new UnivPlanCaseExpr),
        ref_(node_.get()) {}

  UnivPlanBuilderExprPoly::uptr getExprPolyBuilder() override {
    UnivPlanBuilderExprPoly::uptr exprPoly(
        new UnivPlanBuilderExprPoly(UNIVPLAN_EXPR_CASEEXPR));
    exprPoly->getExprPoly()->set_allocated_caseexpr(node_.release());
    return exprPoly;
  }

  UnivPlanBuilderCaseExpr &addArgs(UnivPlanBuilderExprPoly::uptr arg) override {
    if (addingDefresult_) return setDefresult(std::move(arg));
    ref_->mutable_args()->AddAllocated(arg->ownExprPoly().release());
    return *this;
  }

  UnivPlanBuilderCaseExpr &setCasetype(int32_t casetype) {
    ref_->set_casetype(casetype);
    return *this;
  }

  void setAddingDefresult() { addingDefresult_ = true; }

  UnivPlanBuilderCaseExpr &setDefresult(UnivPlanBuilderExprPoly::uptr arg) {
    ref_->set_allocated_defresult(arg->ownExprPoly().release());
    return *this;
  }

 private:
  std::unique_ptr<UnivPlanCaseExpr> node_;
  UnivPlanCaseExpr *ref_;
  bool addingDefresult_ = false;
};

class UnivPlanBuilderCaseWhen final : public UnivPlanBuilderExprNode {
 public:
  typedef std::unique_ptr<UnivPlanBuilderCaseWhen> uptr;

  UnivPlanBuilderCaseWhen() : node_(new UnivPlanCaseWhen), ref_(node_.get()) {}

  UnivPlanBuilderCaseWhen(int32_t pid, int32_t uid)
      : UnivPlanBuilderExprNode(pid, uid),
        node_(new UnivPlanCaseWhen),
        ref_(node_.get()) {}

  UnivPlanBuilderExprPoly::uptr getExprPolyBuilder() override {
    UnivPlanBuilderExprPoly::uptr exprPoly(
        new UnivPlanBuilderExprPoly(UNIVPLAN_EXPR_CASEWHEN));
    exprPoly->getExprPoly()->set_allocated_casewhen(node_.release());
    return exprPoly;
  }

  UnivPlanBuilderCaseWhen &addArgs(UnivPlanBuilderExprPoly::uptr arg) override {
    if (addingResult_) return setResult(std::move(arg));
    ref_->set_allocated_expr(arg->ownExprPoly().release());
    return *this;
  }

  void setAddingResult() { addingResult_ = true; }

  UnivPlanBuilderCaseWhen &setResult(UnivPlanBuilderExprPoly::uptr arg) {
    ref_->set_allocated_result(arg->ownExprPoly().release());
    return *this;
  }

 private:
  std::unique_ptr<UnivPlanCaseWhen> node_;
  UnivPlanCaseWhen *ref_;
  bool addingResult_ = false;
};

class UnivPlanBuilderSubPlan final : public UnivPlanBuilderExprNode {
 public:
  typedef std::unique_ptr<UnivPlanBuilderSubPlan> uptr;

  UnivPlanBuilderSubPlan() : node_(new UnivPlanSubPlan), ref_(node_.get()) {}

  UnivPlanBuilderSubPlan(int32_t pid, int32_t uid)
      : UnivPlanBuilderExprNode(pid, uid),
        node_(new UnivPlanSubPlan),
        ref_(node_.get()) {}

  UnivPlanBuilderExprPoly::uptr getExprPolyBuilder() override {
    UnivPlanBuilderExprPoly::uptr exprPoly(
        new UnivPlanBuilderExprPoly(UNIVPLAN_EXPR_SUBPLAN));
    exprPoly->getExprPoly()->set_allocated_subplan(node_.release());
    return exprPoly;
  }

  UnivPlanBuilderSubPlan &setSubLinkType(SUBLINKTYPE subLinkType) {
    ref_->set_sublinktype(subLinkType);
    return *this;
  }

  UnivPlanBuilderSubPlan &setPlanId(int32_t planId) {
    ref_->set_planid(planId);
    return *this;
  }

  UnivPlanBuilderSubPlan &setTypeId(int32_t typeId) {
    ref_->set_typeid_(typeId);
    return *this;
  }

  UnivPlanBuilderSubPlan &setTypeMod(int64_t typeMod) {
    ref_->set_typemod(typeMod);
    return *this;
  }

  UnivPlanBuilderSubPlan &setUseHashTable(bool useHashTable) {
    ref_->set_usehashtable(useHashTable);
    return *this;
  }

  UnivPlanBuilderSubPlan &setInitPlan(bool initPlan) {
    ref_->set_initplan(initPlan);
    return *this;
  }

  UnivPlanBuilderSubPlan &addSetParam(int32_t setParam) {
    ref_->add_setparam(setParam);
    return *this;
  }

  UnivPlanBuilderSubPlan &addParParam(int32_t parParam) {
    ref_->add_parentparam(parParam);
    return *this;
  }

  UnivPlanBuilderSubPlan &addArgs(UnivPlanBuilderExprPoly::uptr arg) override {
    if (addingTestexpr_) {
      ref_->set_allocated_testexpr(arg->ownExprPoly().release());
    } else {
      ref_->mutable_args()->AddAllocated(arg->ownExprPoly().release());
    }
    return *this;
  }

  void setAddingTestexpr() { addingTestexpr_ = true; }

  UnivPlanBuilderSubPlan &addTestexprParam(int32_t testexprParam) {
    ref_->add_testexprparam(testexprParam);
    return *this;
  }

 private:
  std::unique_ptr<UnivPlanSubPlan> node_;
  UnivPlanSubPlan *ref_;
  bool addingTestexpr_ = false;
};

class UnivPlanBuilderParam final : public UnivPlanBuilderExprNode {
 public:
  typedef std::unique_ptr<UnivPlanBuilderParam> uptr;

  UnivPlanBuilderParam() : node_(new UnivPlanParam), ref_(node_.get()) {}

  UnivPlanBuilderParam(int32_t pid, int32_t uid)
      : UnivPlanBuilderExprNode(pid, uid),
        node_(new UnivPlanParam),
        ref_(node_.get()) {}

  UnivPlanBuilderExprPoly::uptr getExprPolyBuilder() override {
    UnivPlanBuilderExprPoly::uptr exprPoly(
        new UnivPlanBuilderExprPoly(UNIVPLAN_EXPR_PARAM));
    exprPoly->getExprPoly()->set_allocated_param(node_.release());
    return exprPoly;
  }

  UnivPlanBuilderParam &setParamKind(PARAMKIND paramKind) {
    ref_->set_paramkind(paramKind);
    return *this;
  }

  UnivPlanBuilderParam &setParamId(int32_t paramId) {
    ref_->set_paramid(paramId);
    return *this;
  }

  UnivPlanBuilderParam &setTypeId(int32_t typeId) {
    ref_->set_typeid_(typeId);
    return *this;
  }

  UnivPlanBuilderParam &setTypeMod(int64_t typeMod) {
    ref_->set_typemod(typeMod);
    return *this;
  }

 private:
  std::unique_ptr<UnivPlanParam> node_;
  UnivPlanParam *ref_;
};

class UnivPlanBuilderScalarArrayOpExpr final : public UnivPlanBuilderExprNode {
 public:
  typedef std::unique_ptr<UnivPlanBuilderOpExpr> uptr;

  UnivPlanBuilderScalarArrayOpExpr()
      : node_(new UnivPlanScalarArrayOpExpr), ref_(node_.get()) {}

  UnivPlanBuilderScalarArrayOpExpr(int32_t pid, int32_t uid)
      : UnivPlanBuilderExprNode(pid, uid),
        node_(new UnivPlanScalarArrayOpExpr),
        ref_(node_.get()) {}

  UnivPlanBuilderExprPoly::uptr getExprPolyBuilder() override {
    UnivPlanBuilderExprPoly::uptr exprPoly(
        new UnivPlanBuilderExprPoly(UNIVPLAN_EXPR_SCALARARRAYOPEXPR));
    exprPoly->getExprPoly()->set_allocated_scalararrayopexpr(node_.release());
    return exprPoly;
  }

  UnivPlanBuilderExprNode &addArgs(UnivPlanBuilderExprPoly::uptr arg) override {
    ref_->mutable_args()->AddAllocated(arg->ownExprPoly().release());
    return *this;
  }

  UnivPlanBuilderScalarArrayOpExpr &setUseOr(bool useOr) {
    ref_->set_useor(useOr);
    return *this;
  }

  UnivPlanBuilderScalarArrayOpExpr &setFuncId(int32_t funcId) {
    ref_->set_funcid(funcId);
    return *this;
  }

 private:
  std::unique_ptr<UnivPlanScalarArrayOpExpr> node_;
  UnivPlanScalarArrayOpExpr *ref_;
};

class UnivPlanBuilderCoalesceExpr final : public UnivPlanBuilderExprNode {
 public:
  typedef std::unique_ptr<UnivPlanBuilderCoalesceExpr> uptr;

  UnivPlanBuilderCoalesceExpr()
      : node_(new UnivPlanCoalesceExpr), ref_(node_.get()) {}

  UnivPlanBuilderCoalesceExpr(int32_t pid, int32_t uid)
      : UnivPlanBuilderExprNode(pid, uid),
        node_(new UnivPlanCoalesceExpr),
        ref_(node_.get()) {}

  UnivPlanBuilderExprPoly::uptr getExprPolyBuilder() override {
    UnivPlanBuilderExprPoly::uptr exprPoly(
        new UnivPlanBuilderExprPoly(UNIVPLAN_EXPR_COALESCEEXPR));
    exprPoly->getExprPoly()->set_allocated_coalesceexpr(node_.release());
    return exprPoly;
  }

  UnivPlanBuilderCoalesceExpr &addArgs(
      UnivPlanBuilderExprPoly::uptr arg) override {
    ref_->mutable_args()->AddAllocated(arg->ownExprPoly().release());
    return *this;
  }

  UnivPlanBuilderCoalesceExpr &setCoalesceType(int32_t coalesceType) {
    ref_->set_coalescetype(coalesceType);
    return *this;
  }

  UnivPlanBuilderCoalesceExpr &setCoalesceTypeMod(int32_t coalesceTypeMod) {
    ref_->set_coalescetypemod(coalesceTypeMod);
    return *this;
  }

 private:
  std::unique_ptr<UnivPlanCoalesceExpr> node_;
  UnivPlanCoalesceExpr *ref_;
};

class UnivPlanBuilderNullIfExpr final : public UnivPlanBuilderExprNode {
 public:
  typedef std::unique_ptr<UnivPlanBuilderNullIfExpr> uptr;

  UnivPlanBuilderNullIfExpr()
      : node_(new UnivPlanNullIfExpr), ref_(node_.get()) {}

  UnivPlanBuilderNullIfExpr(int32_t pid, int32_t uid)
      : UnivPlanBuilderExprNode(pid, uid),
        node_(new UnivPlanNullIfExpr),
        ref_(node_.get()) {}

  UnivPlanBuilderExprPoly::uptr getExprPolyBuilder() override {
    UnivPlanBuilderExprPoly::uptr exprPoly(
        new UnivPlanBuilderExprPoly(UNIVPLAN_EXPR_NULLIFEXPR));
    exprPoly->getExprPoly()->set_allocated_nullifexpr(node_.release());
    return exprPoly;
  }

  UnivPlanBuilderNullIfExpr &addArgs(
      UnivPlanBuilderExprPoly::uptr arg) override {
    ref_->mutable_args()->AddAllocated(arg->ownExprPoly().release());
    return *this;
  }

  UnivPlanBuilderNullIfExpr &setRetType(int32_t retType) {
    ref_->set_rettype(retType);
    return *this;
  }

  UnivPlanBuilderNullIfExpr &setTypeMod(int32_t typeMod) {
    ref_->set_typemod(typeMod);
    return *this;
  }

  UnivPlanBuilderNullIfExpr &setFuncId(int32_t funcId) {
    ref_->set_funcid(funcId);
    return *this;
  }

 private:
  std::unique_ptr<UnivPlanNullIfExpr> node_;
  UnivPlanNullIfExpr *ref_;
};

class UnivPlanBuilderDistinctExpr final : public UnivPlanBuilderExprNode {
 public:
  typedef std::unique_ptr<UnivPlanBuilderDistinctExpr> uptr;

  UnivPlanBuilderDistinctExpr()
      : node_(new UnivPlanDistinctExpr), ref_(node_.get()) {}

  UnivPlanBuilderDistinctExpr(int32_t pid, int32_t uid)
      : UnivPlanBuilderExprNode(pid, uid),
        node_(new UnivPlanDistinctExpr),
        ref_(node_.get()) {}

  UnivPlanBuilderExprPoly::uptr getExprPolyBuilder() override {
    UnivPlanBuilderExprPoly::uptr exprPoly(
        new UnivPlanBuilderExprPoly(UNIVPLAN_EXPR_DISTINCTEXPR));
    exprPoly->getExprPoly()->set_allocated_distinctexpr(node_.release());
    return exprPoly;
  }

  UnivPlanBuilderExprNode &addArgs(UnivPlanBuilderExprPoly::uptr arg) override {
    ref_->mutable_args()->AddAllocated(arg->ownExprPoly().release());
    return *this;
  }

  UnivPlanBuilderDistinctExpr &setRetType(int32_t retType) {
    ref_->set_rettype(retType);
    return *this;
  }

  UnivPlanBuilderDistinctExpr &setFuncId(int32_t funcId) {
    ref_->set_funcid(funcId);
    return *this;
  }

 private:
  std::unique_ptr<UnivPlanDistinctExpr> node_;
  UnivPlanDistinctExpr *ref_;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_EXPR_NODE_H_
