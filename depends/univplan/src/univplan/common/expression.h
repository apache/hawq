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

#ifndef UNIVPLAN_SRC_UNIVPLAN_COMMON_EXPRESSION_H_
#define UNIVPLAN_SRC_UNIVPLAN_COMMON_EXPRESSION_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "dbcommon/function/invoker.h"
#include "dbcommon/type/decimal.h"
#include "dbcommon/type/interval.h"
#include "dbcommon/utils/string-util.h"

#include "univplan/common/plannode-util.h"

namespace dbcommon {
class TupleBatch;
}

namespace univplan {

class ExprContext {
 public:
  ExprContext()
      : innerBatch(nullptr), outerBatch(nullptr), scanBatch(nullptr) {}
  ExprContext(dbcommon::TupleBatch *outerBatch,
              dbcommon::TupleBatch *innerBatch, dbcommon::TupleBatch *scanBatch)
      : outerBatch(outerBatch), innerBatch(innerBatch), scanBatch(scanBatch) {
    assert((scanBatch && !innerBatch && !outerBatch) ||
           (outerBatch && !innerBatch && !scanBatch) ||
           (innerBatch && outerBatch && !scanBatch));
  }

  dbcommon::TupleBatch *innerBatch;
  dbcommon::TupleBatch *outerBatch;
  dbcommon::TupleBatch *scanBatch;

  // Iterator of plain index for processing input tuple by tuple rather than TB
  // by TB
  uint64_t tupleIdx = -1;
  bool tupleIterValid() { return tupleIdx != -1; }
  void setTupleIter(uint64_t tupleIdx) { this->tupleIdx = tupleIdx; }
  void disableTupleIter() { tupleIdx = -1; }

 public:
  void setSelected(dbcommon::SelectList *selected) {
    if (innerBatch) innerBatch->setSelected(selected);
    if (outerBatch) outerBatch->setSelected(selected);
    if (scanBatch) scanBatch->setSelected(selected);
  }

  std::unique_ptr<dbcommon::SelectList> releaseSelected() {
    if (innerBatch) return innerBatch->releaseSelected();
    if (outerBatch) return outerBatch->releaseSelected();
    if (scanBatch) return scanBatch->releaseSelected();
    return nullptr;
  }

  size_t getNumOfRows() {
    if (innerBatch) return innerBatch->getNumOfRows();
    if (outerBatch) return outerBatch->getNumOfRows();
    if (scanBatch) return scanBatch->getNumOfRows();

    // tupleCount=1 special for constant select
    return 1;
  }

  size_t getNumOfRowsPlain() {
    if (innerBatch) return innerBatch->getNumOfRowsPlain();
    if (outerBatch) return outerBatch->getNumOfRowsPlain();
    if (scanBatch) return scanBatch->getNumOfRowsPlain();

    // tupleCount=1 special for constant select
    return 1;
  }

  dbcommon::SelectList *getSelectList() {
    if (innerBatch && innerBatch->getSelected())
      return innerBatch->getSelected();
    if (outerBatch && outerBatch->getSelected())
      return outerBatch->getSelected();
    if (scanBatch && scanBatch->getSelected()) return scanBatch->getSelected();
    return nullptr;
  }
};

class ExprState {
 public:
  ExprState() {}
  virtual ~ExprState() {}

  typedef std::unique_ptr<ExprState> uptr;

  virtual dbcommon::Datum calc(ExprContext *context) = 0;

  dbcommon::SelectList *convertSelectList(const dbcommon::Datum &d,
                                          ExprContext *context);

  dbcommon::TypeKind getRetType() { return retType; }

  int64_t getRetTypeModifier() { return retTypeMod; }

  void addArg(ExprState::uptr arg) { args.push_back(std::move(arg)); }

  ExprState *getArg(size_t idx) { return args[idx].get(); }

 protected:
  dbcommon::TypeKind retType = dbcommon::TypeKind::UNKNOWNID;
  int64_t retTypeMod = -1;
  std::vector<ExprState::uptr> args;

  // When relying on current ExprContext's SelectList content, ExprState should
  // backup the SelectList.
  std::unique_ptr<dbcommon::SelectList> backupSel_;

 private:
  std::unique_ptr<dbcommon::SelectList> convertedSelectlist;
};

class ListExprState : public ExprState {
 public:
  explicit ListExprState(const univplan::UnivPlanExprPolyList *exprs);
  ~ListExprState() {}

  typedef std::unique_ptr<ListExprState> uptr;

  dbcommon::Datum calc(ExprContext *context) override;

 private:
  dbcommon::SelectList retval;
};

class VarExprState : public ExprState {
 public:
  explicit VarExprState(const univplan::UnivPlanVar *var) : var(var) {
    retType = univplan::PlanNodeUtil::typeKindMapping(var->typeid_());
    retTypeMod = var->typemod();
  }
  ~VarExprState() {}

  typedef std::unique_ptr<VarExprState> uptr;

  dbcommon::Datum calc(ExprContext *context) override;

  int32_t getAttributeNumber() const { return var->varattno(); }

  uint32_t getVarNo() const { return var->varno(); }

 private:
  const univplan::UnivPlanVar *var;
  // iterator for processing input tuple by tuple
  dbcommon::Scalar scalar;
};

class ConstExprState : public ExprState {
 public:
  explicit ConstExprState(const univplan::UnivPlanConst *val);
  ~ConstExprState() {}

  typedef std::unique_ptr<ConstExprState> uptr;

  dbcommon::Datum calc(ExprContext *context) override;

 private:
  dbcommon::Scalar scalar;
  dbcommon::Timestamp ts;
  dbcommon::DecimalVar decimalReg;
  dbcommon::IntervalVar intervalReg;
  std::vector<char> binary;
  dbcommon::Vector::uptr array;
};

class OpExprState : public ExprState {
 public:
  explicit OpExprState(const univplan::UnivPlanOpExpr *op);
  explicit OpExprState(dbcommon::FuncKind funcId);
  ~OpExprState() {}

  typedef std::unique_ptr<OpExprState> uptr;

  dbcommon::Datum calc(ExprContext *context) override;

 private:
  dbcommon::Invoker invoker;
  std::unique_ptr<dbcommon::Object> retval;
};

class FuncExprState : public ExprState {
 public:
  explicit FuncExprState(const univplan::UnivPlanFuncExpr *func)
      : func(func),
        invoker(univplan::PlanNodeUtil::funcKindMapping(func->funcid())) {}
  ~FuncExprState() {}

  typedef std::unique_ptr<FuncExprState> uptr;

  dbcommon::Datum calc(ExprContext *context) override;

 private:
  const univplan::UnivPlanFuncExpr *func;
  dbcommon::Invoker invoker;
  std::unique_ptr<dbcommon::Object> retval;
};

class NullTestState : public ExprState {
 public:
  explicit NullTestState(const univplan::UnivPlanNullTest *op)
      : op(op),
        isNull(op->type() == univplan::NULLTESTTYPE::NULLTESTTYPE_IS_NULL) {}

  explicit NullTestState(bool isNull) : op(nullptr), isNull(isNull) {}

  ~NullTestState() {}

  typedef std::unique_ptr<NullTestState> uptr;

  dbcommon::Datum calc(ExprContext *context) override;

 private:
  const univplan::UnivPlanNullTest *op;
  bool isNull;
  dbcommon::SelectList result;
  dbcommon::Scalar scalarResult_;
};

class BooleanTestState : public ExprState {
 public:
  explicit BooleanTestState(const univplan::UnivPlanBooleanTest *op) : op(op) {}
  ~BooleanTestState() {}

  typedef std::unique_ptr<BooleanTestState> uptr;

  dbcommon::Datum calc(ExprContext *context) override;

 private:
  const univplan::UnivPlanBooleanTest *op;
  std::unique_ptr<dbcommon::SelectList> result;
};

class BoolExprState : public ExprState {
 public:
  explicit BoolExprState(const univplan::UnivPlanBoolExpr *expr)
      : op(expr->type()) {}
  ~BoolExprState() {}

  typedef std::unique_ptr<BoolExprState> uptr;

  dbcommon::Datum calc(ExprContext *context) override;
  dbcommon::Datum calcAnd(ExprContext *context);
  dbcommon::Datum calcOr(ExprContext *context);
  dbcommon::Datum calcNot(ExprContext *context);

 private:
  const univplan::BOOLEXPRTYPE op;
  std::unique_ptr<dbcommon::Object> retval;
};

ExprState::uptr InitExpr(const univplan::UnivPlanExprPoly *expr);
ExprState::uptr InitExpr(const univplan::UnivPlanExprPolyList *exprs);

/* those interface only for magma index */
struct IndexExpr {
  IndexExpr() : colidx(-1), typeId(-1), typeModify(-1) {}

  IndexExpr(const IndexExpr &idx) {
    colidx = idx.colidx;
    typeId = idx.typeId;
    oper = idx.oper;
    constVal = idx.constVal;
    typeModify = idx.typeModify;
  }

  IndexExpr &operator=(const IndexExpr &idx) {
    colidx = idx.colidx;
    typeId = idx.typeId;
    oper = idx.oper;
    constVal = idx.constVal;
    typeModify = idx.typeModify;
    return *this;
  }

  bool isEqual() {
    return dbcommon::StringUtil::countReplicates(oper, "equal");
  }

  bool isGreater() {
    return dbcommon::StringUtil::countReplicates(oper, "greater");
  }

  bool isLessthan() {
    return dbcommon::StringUtil::countReplicates(oper, "less");
  }

  ~IndexExpr() {}
  int colidx;
  int typeId;
  int typeModify;
  std::string oper;
  std::string constVal;
};

bool do_opexpr(const univplan::UnivPlanOpExpr &op, IndexExpr *index, int pk,
               int *equal);

bool do_boolexpr(const univplan::UnivPlanBoolExpr &expr, IndexExpr *index,
                 int pk, int *equal);

std::vector<std::unique_ptr<IndexExpr> > getIndexExpr(
    const std::vector<int> &index,
    const univplan::UnivPlanExprPolyList *filterExprs);

bool do_opexpr2(int pk, const univplan::UnivPlanOpExpr &op,
                std::vector<std::unique_ptr<IndexExpr> > &allExpr,    // NOLINT
                std::vector<std::unique_ptr<IndexExpr> > &equalExpr,  // NOLINT
                int *equal);
bool do_boolexpr2(
    int pk, const univplan::UnivPlanBoolExpr &expr,
    std::vector<std::unique_ptr<IndexExpr> > &allExpr,    // NOLINT
    std::vector<std::unique_ptr<IndexExpr> > &equalExpr,  // NOLINT
    int *equal);
bool loopContinue(
    int pkCol, const univplan::UnivPlanExprPolyList *filterExprs,
    std::vector<std::unique_ptr<IndexExpr> > &allExpr,     // NOLINT
    std::vector<std::unique_ptr<IndexExpr> > &equalExpr);  // NOLINT

std::vector<std::vector<std::unique_ptr<IndexExpr> > > getIndexExpr2(
    const std::vector<int> &index,
    const univplan::UnivPlanExprPolyList *filterExprs);
}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_COMMON_EXPRESSION_H_
