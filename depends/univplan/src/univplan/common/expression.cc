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

#include "univplan/common/expression.h"

#include <algorithm>

#include "dbcommon/common/tuple-batch.h"
#include "dbcommon/common/vector/fixed-length-vector.h"
#include "dbcommon/common/vector/list-vector.h"
#include "dbcommon/common/vector/struct-vector.h"
#include "dbcommon/common/vector/variable-length-vector.h"
#include "dbcommon/function/func-kind.cg.h"
#include "dbcommon/log/logger.h"
#include "dbcommon/type/array.h"
#include "dbcommon/type/type-util.h"

namespace univplan {

dbcommon::SelectList *ExprState::convertSelectList(const dbcommon::Datum &d,
                                                   ExprContext *context) {
  dbcommon::Object *obj = dbcommon::DatumGetValue<dbcommon::Object *>(d);
  dbcommon::SelectList *sel = nullptr;
  dbcommon::BooleanVector *vec = nullptr;
  dbcommon::Scalar *val = nullptr;

  if ((sel = dynamic_cast<dbcommon::SelectList *>(obj)) == nullptr) {
    if (convertedSelectlist == nullptr)
      convertedSelectlist.reset(new dbcommon::SelectList);
    sel = convertedSelectlist.get();
    if ((vec = dynamic_cast<dbcommon::BooleanVector *>(obj)) != nullptr) {
      sel->fromVector(vec);
    } else if ((val = dynamic_cast<dbcommon::Scalar *>(obj)) != nullptr) {
      sel->clear();
      if (val->getBoolValue()) {
        auto size = context->getNumOfRows();
        sel->resize(size);
        for (auto i = 0; i < size; i++) (*sel)[i] = i;
      }
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "Supposed to be SelectList or BooleanVector or Const");
    }
  }
  return sel;
}

ListExprState::ListExprState(const univplan::UnivPlanExprPolyList *exprs) {
  for (int i = 0; i < exprs->size(); ++i) {
    args.push_back(InitExpr(&exprs->Get(i)));
  }
}

dbcommon::Datum ListExprState::calc(ExprContext *context) {
  dbcommon::SelectList *result = nullptr;

  for (ExprState::uptr &e : args) {
    dbcommon::Datum d = e->calc(context);
    dbcommon::SelectList *sel = convertSelectList(d, context);

    if (result) {
      retval.resize(result->size() + sel->size());
      auto end =
          std::set_intersection(result->begin(), result->end(), sel->begin(),
                                sel->end(), retval.begin());
      retval.resize(end - retval.begin());
      result->swap(retval);
    } else {
      result = sel;
    }
  }

  return dbcommon::CreateDatum(result);
}

dbcommon::Datum VarExprState::calc(ExprContext *context) {
  int32_t columnIndex = var->varattno() - 1;

  dbcommon::TupleBatch *batch = nullptr;
  if (var->varno() == INNER_VAR) {
    batch = context->innerBatch;
  } else if (var->varno() == OUTER_VAR) {
    batch = context->outerBatch;
  } else {
    batch = context->scanBatch;
  }

  if (context->tupleIterValid() && !batch->isScalarTupleBatch()) {
    batch->getColumn(columnIndex)->readPlainScalar(context->tupleIdx, &scalar);
    return dbcommon::CreateDatum<dbcommon::Scalar *>(&scalar);
  }

  dbcommon::Vector *ret;

  if (columnIndex >= 0) {
    // Case 1. Relation attribute
    ret = batch->getColumn(columnIndex);
  } else {
    // Case 2. System attribute
    LOG_ERROR(
        ERRCODE_FEATURE_NOT_SUPPORTED,
        "System attribute is not supported in univplan::VarExprState::calc");
  }

  return CreateDatum(ret);
}

ConstExprState::ConstExprState(const univplan::UnivPlanConst *val)
: scalar(dbcommon::Datum(static_cast<int64_t>(0))) {
  retType = univplan::PlanNodeUtil::typeKindMapping(val->type());
  retTypeMod = val->typemod();
  if (val->isnull()) {
    scalar.isnull = true;
  } else if (univplan::PlanNodeUtil::typeKindMapping(val->type()) ==
                 dbcommon::TypeKind::TIMESTAMPID ||
             univplan::PlanNodeUtil::typeKindMapping(val->type()) ==
                 dbcommon::TypeKind::TIMESTAMPTZID) {
    scalar.value = dbcommon::CreateDatum(
        val->value().c_str(), &ts,
        univplan::PlanNodeUtil::typeKindMapping(val->type()));
  } else if (univplan::PlanNodeUtil::typeKindMapping(val->type()) ==
                 dbcommon::TypeKind::DECIMALID ||
             univplan::PlanNodeUtil::typeKindMapping(val->type()) ==
                 dbcommon::TypeKind::DECIMALNEWID) {
    dbcommon::DecimalType t;
    decimalReg = t.fromString(val->value().c_str());
    scalar.value = dbcommon::CreateDatum(&decimalReg);
  } else if (univplan::PlanNodeUtil::typeKindMapping(val->type()) ==
             dbcommon::TypeKind::INTERVALID) {
    intervalReg = dbcommon::IntervalType::fromString(val->value().c_str());
    scalar.value = dbcommon::CreateDatum(&intervalReg);
  } else if (univplan::PlanNodeUtil::typeKindMapping(val->type()) ==
                 dbcommon::TypeKind::BINARYID ||
             univplan::PlanNodeUtil::typeKindMapping(val->type()) ==
                 dbcommon::TypeKind::IOBASETYPEID ||
             univplan::PlanNodeUtil::typeKindMapping(val->type()) ==
                 dbcommon::TypeKind::STRUCTEXID) {
    auto srcStr = val->value().c_str();
    auto srcLen = val->value().size();
    uint32_t dstLen = 0;
    binary.resize(srcLen + sizeof(dstLen));
    for (auto i = 0; i < srcLen;) {
      if (srcStr[i] != '\\') {
        binary[dstLen++] = srcStr[i];
        i += 1;
      } else if (srcStr[i + 1] == '\\') {
        binary[dstLen++] = '\\';
        i += 2;
      } else {
        binary[dstLen++] = (srcStr[i + 1] - '0') * 64 +
                           (srcStr[i + 2] - '0') * 8 + (srcStr[i + 3] - '0');
        i += 4;
      }
    }
    scalar.value = dbcommon::CreateDatum<char *>(&binary[0]);
    scalar.length = dstLen;
  } else if (univplan::PlanNodeUtil::typeKindMapping(val->type()) ==
                 dbcommon::TypeKind::SMALLINTARRAYID ||
             univplan::PlanNodeUtil::typeKindMapping(val->type()) ==
                 dbcommon::TypeKind::INTARRAYID ||
             univplan::PlanNodeUtil::typeKindMapping(val->type()) ==
                 dbcommon::TypeKind::BIGINTARRAYID ||
             univplan::PlanNodeUtil::typeKindMapping(val->type()) ==
                 dbcommon::TypeKind::FLOATARRAYID ||
             univplan::PlanNodeUtil::typeKindMapping(val->type()) ==
                 dbcommon::TypeKind::DOUBLEARRAYID ||
             univplan::PlanNodeUtil::typeKindMapping(val->type()) ==
                 dbcommon::TypeKind::STRINGARRAYID ||
             univplan::PlanNodeUtil::typeKindMapping(val->type()) ==
                 dbcommon::TypeKind::BPCHARARRAYID) {
    auto typeEnt = reinterpret_cast<dbcommon::ArrayType *>(
        dbcommon::TypeUtil::instance()
            ->getTypeEntryById(
                univplan::PlanNodeUtil::typeKindMapping(val->type()))
            ->type.get());
    array = std::move(typeEnt->getScalarFromString(val->value()));
    scalar.value = dbcommon::CreateDatum(array.get());
  } else {
    scalar.value = dbcommon::CreateDatum(
        val->value().c_str(),
        univplan::PlanNodeUtil::typeKindMapping(val->type()));
  }

  if (univplan::PlanNodeUtil::typeKindMapping(val->type()) ==
          dbcommon::TypeKind::STRINGID ||
      univplan::PlanNodeUtil::typeKindMapping(val->type()) ==
          dbcommon::TypeKind::VARCHARID ||
      univplan::PlanNodeUtil::typeKindMapping(val->type()) ==
          dbcommon::TypeKind::CHARID ||
      univplan::PlanNodeUtil::typeKindMapping(val->type()) ==
          dbcommon::TypeKind::DECIMALNEWID) {
    scalar.length = val->value().length();
  }
}

dbcommon::Datum ConstExprState::calc(ExprContext *context) {
  return dbcommon::CreateDatum(&scalar);
}

OpExprState::OpExprState(const univplan::UnivPlanOpExpr *op)
    : invoker(univplan::PlanNodeUtil::funcKindMapping(op->funcid())) {
  const dbcommon::FuncEntry *entry =
      dbcommon::Func::instance()->getFuncEntryById(
          univplan::PlanNodeUtil::funcKindMapping(op->funcid()));
  retType = entry->retType;
}

OpExprState::OpExprState(dbcommon::FuncKind funcId) : invoker(funcId) {
  const dbcommon::FuncEntry *entry =
      dbcommon::Func::instance()->getFuncEntryById(
          univplan::PlanNodeUtil::funcKindMapping(funcId));
  retType = entry->retType;
}

dbcommon::Datum OpExprState::calc(ExprContext *context) {
  invoker.resetPrarmeter();
  invoker.addParam(dbcommon::CreateDatum(0));

  assert(args.size() == 2 || args.size() == 1);
  for (ExprState::uptr &arg : args) {
    invoker.addParam(arg->calc(context));
  }
  if (retval) {
    retval->clear();
  } else {
    bool isRetvalVector = true;
    if (args.size() == 2) {
      dbcommon::Object *paraL =
          dbcommon::DatumGetValue<dbcommon::Object *>(invoker.getParam(1));
      dbcommon::Vector *vecL = dynamic_cast<dbcommon::Vector *>(paraL);
      dbcommon::Object *paraR =
          dbcommon::DatumGetValue<dbcommon::Object *>(invoker.getParam(2));
      dbcommon::Vector *vecR = dynamic_cast<dbcommon::Vector *>(paraR);
      isRetvalVector = vecL || vecR;
    } else if (args.size() == 1) {
      dbcommon::Object *para =
          dbcommon::DatumGetValue<dbcommon::Object *>(invoker.getParam(1));
      isRetvalVector = dynamic_cast<dbcommon::Vector *>(para);
    }

    if (isRetvalVector) {
      if (retType == dbcommon::TypeKind::BOOLEANID)
        retval = std::unique_ptr<dbcommon::Object>(new dbcommon::SelectList);
      else
        retval = dbcommon::Vector::BuildVector(retType, true, retTypeMod);
    } else {
      retval = std::unique_ptr<dbcommon::Object>(new dbcommon::Scalar);
    }
  }
  invoker.setParam(0, dbcommon::CreateDatum<dbcommon::Object *>(retval.get()));

  return invoker.invoke();
}

dbcommon::Datum FuncExprState::calc(ExprContext *context) {
  invoker.resetPrarmeter();
  invoker.addParam(dbcommon::CreateDatum(0));

  for (ExprState::uptr &arg : args) {
    invoker.addParam(arg->calc(context));
  }

  if (retval) {
    retval->clear();
  } else {
    bool isVecFunc = false;
    for (size_t i = 1; i <= args.size(); i++) {
      dbcommon::Object *para =
          dbcommon::DatumGetValue<dbcommon::Object *>(invoker.getParam(i));
      dbcommon::Vector *vec = dynamic_cast<dbcommon::Vector *>(para);
      if (vec) {
        isVecFunc = true;
        break;
      }
    }
    if (func->funcid() == dbcommon::FuncKind::RANDOMF) isVecFunc = true;
    if (!isVecFunc) {
      retval = std::unique_ptr<dbcommon::Object>(new dbcommon::Scalar);
    } else {
      dbcommon::TypeKind rettype =
          univplan::PlanNodeUtil::typeKindMapping(func->rettype());
      if (rettype == dbcommon::TypeKind::BOOLEANID)
        retval = std::unique_ptr<dbcommon::Object>(new dbcommon::SelectList);
      else
        retval = dbcommon::Vector::BuildVector(rettype, true);
    }
  }
  invoker.setParam(0, dbcommon::CreateDatum<dbcommon::Object *>(retval.get()));
  if (func->funcid() == dbcommon::FuncKind::RANDOMF) {
    invoker.addParam(dbcommon::CreateDatum(context->outerBatch != nullptr
                                               ? context->outerBatch
                                               : context->scanBatch));
  }

  return invoker.invoke();
}

dbcommon::Datum NullTestState::calc(ExprContext *context) {
  assert(args.size() == 1);

  auto obj =
      dbcommon::DatumGetValue<dbcommon::Object *>(args[0]->calc(context));

  if (auto scalar = dynamic_cast<dbcommon::Scalar *>(obj)) {
    scalarResult_.value = dbcommon::CreateDatum<bool>(scalar->isnull == isNull);
    return dbcommon::CreateDatum(&scalarResult_);
  } else {
    auto vec = reinterpret_cast<dbcommon::Vector *>(obj);
    size_t size = vec->getNumOfRows();
    result.resize(0);

    for (size_t i = 0; i < size; ++i) {
      bool null = vec->isNull(i);

      if (null == isNull) {
        result.push_back(vec->getPlainIndex(i));
      }
    }
  }

  return dbcommon::CreateDatum(&result);
}

dbcommon::Datum BooleanTestState::calc(ExprContext *context) {
  assert(args.size() == 1);
  for (ExprState::uptr &arg : args) {
    dbcommon::Vector *vec =
        dbcommon::DatumGetValue<dbcommon::Vector *>(arg->calc(context));

    size_t size = vec->getNumOfRows();
    result.reset(new dbcommon::SelectList);
    result->reserve(size);

    const bool *val = reinterpret_cast<const bool *>(vec->getValue());
    for (size_t i = 0; i < size; ++i) {
      uint64_t index = vec->getPlainIndex(i);
      bool null = vec->isNullPlain(index);
      if (op->type() == univplan::BOOLTESTTYPE::BOOLTESTTYPE_IS_TRUE) {
        if (!null && val[index]) result->push_back(index);
      } else if (op->type() ==
                 univplan::BOOLTESTTYPE::BOOLTESTTYPE_IS_NOT_TRUE) {
        if (null || !val[index]) result->push_back(index);
      } else if (op->type() == univplan::BOOLTESTTYPE::BOOLTESTTYPE_IS_FALSE) {
        if (!null && !val[index]) result->push_back(index);
      } else if (op->type() ==
                 univplan::BOOLTESTTYPE::BOOLTESTTYPE_IS_NOT_FALSE) {
        if (null || val[index]) result->push_back(index);
      } else if (op->type() ==
                 univplan::BOOLTESTTYPE::BOOLTESTTYPE_IS_UNKNOWN) {
        if (null) result->push_back(index);
      } else {
        assert(op->type() ==
               univplan::BOOLTESTTYPE::BOOLTESTTYPE_IS_NOT_UNKNOWN);
        if (!null) result->push_back(index);
      }
    }
  }

  return dbcommon::CreateDatum(result.get());
}

dbcommon::Datum BoolExprState::calc(ExprContext *context) {
  if (op == univplan::BOOLEXPRTYPE::BOOLEXPRTYPE_OR_EXPR) {
    return calcOr(context);
  } else if (op == univplan::BOOLEXPRTYPE::BOOLEXPRTYPE_NOT_EXPR) {
    return calcNot(context);
  } else {
    assert(op == univplan::BOOLEXPRTYPE::BOOLEXPRTYPE_AND_EXPR);
    return calcAnd(context);
  }
}

dbcommon::Datum BoolExprState::calcAnd(ExprContext *context) {
  assert(args.size() >= 2);
  backupSel_ = context->releaseSelected();
  dbcommon::SelectList *backupSel = context->getSelectList();

  dbcommon::Object *para0 = args[0]->calc(context);
  if (dynamic_cast<dbcommon::Scalar *>(para0))
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "Feature not supported");

  if (dynamic_cast<dbcommon::SelectList *>(para0)) {
    if (retval == nullptr)
      retval =
          dbcommon::Vector::BuildVector(dbcommon::TypeKind::BOOLEANID, true);
    reinterpret_cast<dbcommon::SelectList *>(para0)->toVector(
        reinterpret_cast<dbcommon::Vector *>(retval.get()));
  } else {
    retval =
        reinterpret_cast<dbcommon::Vector *>(para0)->cloneSelected(nullptr);
  }

  dbcommon::Vector *retVector =
      reinterpret_cast<dbcommon::Vector *>(retval.get());

  std::unique_ptr<dbcommon::Vector> tmpVectorBackup;

  for (auto i = 1; i < args.size(); i++) {
    auto &it = args[i];
    dbcommon::Object *para = it->calc(context);

    auto lhsVector = retVector->cloneSelected(nullptr);
    dbcommon::Vector *rhsVector = reinterpret_cast<dbcommon::Vector *>(para);

    if (dynamic_cast<dbcommon::Scalar *>(para))
      LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "Feature not supported");

    if (auto sel = dynamic_cast<dbcommon::SelectList *>(para)) {
      if (tmpVectorBackup == nullptr)
        tmpVectorBackup =
            dbcommon::Vector::BuildVector(dbcommon::TypeKind::BOOLEANID, true);
      rhsVector = tmpVectorBackup.get();
      sel->toVector(rhsVector);
    }

    dbcommon::FixedSizeTypeVectorRawData<bool> lhs(lhsVector.get());
    dbcommon::FixedSizeTypeVectorRawData<bool> rhs(rhsVector);
    retVector->resize(context->getNumOfRowsPlain(), nullptr, lhs.nulls,
                      rhs.nulls);
    dbcommon::FixedSizeTypeVectorRawData<bool> ret(retVector);
    auto setBoolean = [&](uint64_t plainIdx) {
      ret.values[plainIdx] = lhs.values[plainIdx] & rhs.values[plainIdx];
    };
    dbcommon::transformVector(ret.plainSize, nullptr, nullptr, setBoolean);

    if (ret.nulls) {
      auto setNullFromLhs = [&](uint64_t plainIdx) {
        if (lhs.values[plainIdx] == false)
          const_cast<bool *>(ret.nulls)[plainIdx] = false;
      };
      dbcommon::transformVector(ret.plainSize, nullptr, lhs.nulls,
                                setNullFromLhs);

      auto setNullFromRhs = [&](uint64_t plainIdx) {
        if (rhs.values[plainIdx] == false)
          const_cast<bool *>(ret.nulls)[plainIdx] = false;
      };
      dbcommon::transformVector(ret.plainSize, nullptr, rhs.nulls,
                                setNullFromRhs);
    }
  }

  context->setSelected(backupSel);
  auto retsel = convertSelectList(dbcommon::CreateDatum(retVector), context);
  return dbcommon::CreateDatum(retsel);
}

dbcommon::Datum BoolExprState::calcOr(ExprContext *context) {
  assert(args.size() >= 2);
  backupSel_ = context->releaseSelected();
  dbcommon::SelectList *backupSel = context->getSelectList();
  if (retval == nullptr) retval.reset(new dbcommon::SelectList);
  retval->clear();
  dbcommon::SelectList *retsel =
      static_cast<dbcommon::SelectList *>(retval.get());
  retsel->setPlainSize(0);

  dbcommon::Datum d = args[0]->calc(context);
  dbcommon::SelectList *sel = convertSelectList(d, context);
  *retsel = *sel;

  for (auto i = 1; i < args.size(); i++) {
    auto &it = args[i];
    dbcommon::Datum d = it->calc(context);
    dbcommon::SelectList *sel = convertSelectList(d, context);

    dbcommon::SelectList tmp = *retsel;

    retsel->resize(tmp.size() + sel->size());
    auto end = std::set_union(tmp.begin(), tmp.end(), sel->begin(), sel->end(),
                              retsel->begin());
    retsel->resize(end - retsel->begin());
    retsel->setNulls(context->getNumOfRowsPlain(), nullptr, tmp.getNulls(),
                     sel->getNulls());
  }
  retsel->setNulls(context->getNumOfRowsPlain(), retsel, false);

  context->setSelected(backupSel);
  return dbcommon::CreateDatum(retval.get());
}

dbcommon::Datum BoolExprState::calcNot(ExprContext *context) {
  assert(args.size() == 1);
  backupSel_ = context->releaseSelected();
  dbcommon::SelectList *backupSel = context->getSelectList();
  dbcommon::Datum d = args.front()->calc(context);

  if (auto sel = dynamic_cast<dbcommon::SelectList *>(
          dbcommon::DatumGetValue<dbcommon::Object *>(d))) {
    if (retval == nullptr) retval.reset(new dbcommon::SelectList);
    auto retsel = reinterpret_cast<dbcommon::SelectList *>(retval.get());
    *retsel = sel->getComplement(context->getNumOfRowsPlain());
    retsel->setNulls(context->getNumOfRowsPlain(), nullptr, sel->getNulls());

    if (backupSel) {
      auto end = std::set_intersection(retsel->begin(), retsel->end(),
                                       backupSel->begin(), backupSel->end(),
                                       retsel->begin());
      retsel->resize(end - retsel->begin());
    }
  } else if (dbcommon::BooleanVector *vec =
                 dynamic_cast<dbcommon::BooleanVector *>(
                     dbcommon::DatumGetValue<dbcommon::Object *>(d))) {
    if (retval == nullptr) retval.reset(new dbcommon::SelectList);
    auto retsel = reinterpret_cast<dbcommon::SelectList *>(retval.get());
    dbcommon::SelectList tmp;
    tmp.fromVector(vec);
    *retsel = tmp.getComplement(context->getNumOfRowsPlain());
    retsel->setNulls(context->getNumOfRowsPlain(), nullptr, tmp.getNulls());

    if (backupSel) {
      auto end = std::set_intersection(retsel->begin(), retsel->end(),
                                       backupSel->begin(), backupSel->end(),
                                       retsel->begin());
      retsel->resize(end - retsel->begin());
    }
  } else {
    dbcommon::Scalar *scalar = static_cast<dbcommon::Scalar *>(
        dbcommon::DatumGetValue<dbcommon::Object *>(d));
    if (scalar->value.value.i8)
      scalar->value.value.i8 = 0;
    else
      scalar->value.value.i8 = 1;
    retval.reset(new dbcommon::Scalar(*scalar));
  }

  context->setSelected(backupSel);
  return dbcommon::CreateDatum(retval.get());
}

ExprState::uptr InitExpr(const univplan::UnivPlanExprPolyList *exprs) {
  return ExprState::uptr(new ListExprState(exprs));
}

ExprState::uptr InitExpr(const univplan::UnivPlanExprPoly *expr) {
  switch (expr->type()) {
    case univplan::UNIVPLAN_EXPR_VAR:
      return ExprState::uptr(new VarExprState(&expr->var()));
    case univplan::UNIVPLAN_EXPR_CONST:
      return ExprState::uptr(new ConstExprState(&expr->val()));
    case univplan::UNIVPLAN_EXPR_TARGETENTRY:
      return InitExpr(&expr->targetentry().expression());
    case univplan::UNIVPLAN_EXPR_OPEXPR: {
      const univplan::UnivPlanOpExpr &op = expr->opexpr();
      bool predictable =
          dbcommon::Func::instance()
              ->getFuncEntryById(static_cast<dbcommon::FuncKind>(op.funcid()))
              ->predictable;
      if (!predictable)
        LOG_ERROR(ERRCODE_INTERNAL_ERROR, "FuncExpr %d is not predictable",
                  op.funcid());
      OpExprState::uptr retval(new OpExprState(&op));
      for (int i = 0; i < op.args_size(); ++i)
        retval->addArg(InitExpr(&op.args(i)));
      return std::move(retval);
    }
    case univplan::UNIVPLAN_EXPR_FUNCEXPR: {
      const univplan::UnivPlanFuncExpr &func = expr->funcexpr();
      bool predictable =
          dbcommon::Func::instance()
              ->getFuncEntryById(static_cast<dbcommon::FuncKind>(func.funcid()))
              ->predictable;
      if (!predictable)
        LOG_ERROR(ERRCODE_INTERNAL_ERROR, "FuncExpr %d is not predictable",
                  func.funcid());
      FuncExprState::uptr retval(new FuncExprState(&func));
      for (int i = 0; i < func.args_size(); ++i)
        retval->addArg(InitExpr(&func.args(i)));
      return std::move(retval);
    }
    case univplan::UNIVPLAN_EXPR_NULLTEST: {
      const univplan::UnivPlanNullTest &op = expr->nulltest();
      NullTestState::uptr retval(new NullTestState(&op));
      retval->addArg(InitExpr(&op.arg()));
      return std::move(retval);
    }
    case univplan::UNIVPLAN_EXPR_BOOLEANTEST: {
      const univplan::UnivPlanBooleanTest &op = expr->booltest();
      BooleanTestState::uptr retval(new BooleanTestState(&op));
      retval->addArg(InitExpr(&op.arg()));
      return std::move(retval);
    }
    case univplan::UNIVPLAN_EXPR_BOOLEXPR: {
      const univplan::UnivPlanBoolExpr &op = expr->boolexpr();
      BoolExprState::uptr retval(new BoolExprState(&op));
      for (int i = 0; i < op.args_size(); ++i)
        retval->addArg(InitExpr(&op.args(i)));
      return std::move(retval);
    }
    default:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "expr type %d is not predictable",
                expr->type());
  }
  return nullptr;
}

bool do_opexpr2(int col, const univplan::UnivPlanOpExpr &op,
                std::vector<std::unique_ptr<IndexExpr> > &allExpr,    // NOLINT
                std::vector<std::unique_ptr<IndexExpr> > &equalExpr,  // NOLINT
                int *equal) {
  std::unique_ptr<IndexExpr> idx(new IndexExpr());
  const univplan::UnivPlanVar &var = op.args(0).var();
  int att = var.varattno();
  int typeId = var.typeid_();
  if (typeId == dbcommon::TypeKind::DECIMALID ||
      typeId == dbcommon::TypeKind::BOOLEANID) {
    return false;
  }
  int modify = var.typemod();
  std::string funcName =
      dbcommon::Func::instance()
          ->getFuncEntryById(static_cast<dbcommon::FuncKind>(op.funcid()))
          ->funcName;
  if (att - 1 == col &&
      (!(dbcommon::StringUtil::countReplicates(funcName, "not_equal") == 1))) {
    const univplan::UnivPlanConst &con = op.args(1).val();
    idx->colidx = att;
    idx->typeId = typeId;
    idx->oper = funcName;
    idx->constVal = con.value();
    idx->typeModify = modify;
    if (!(dbcommon::StringUtil::countReplicates(funcName, "equal") == 1)) {
      allExpr.push_back(std::move(idx));
      *equal = 0;
    } else {
      equalExpr.push_back(std::move(idx));
      *equal = 1;
    }
    return true;
  }
  return false;
}

bool do_boolexpr2(
    int pk, const univplan::UnivPlanBoolExpr &expr,
    std::vector<std::unique_ptr<IndexExpr> > &allExpr,    // NOLINT
    std::vector<std::unique_ptr<IndexExpr> > &equalExpr,  // NOLINT
    int *equal) {
  int args_size = expr.args_size();
  bool hasExpr = false;
  for (int i = 0; i < args_size; ++i) {
    const univplan::UnivPlanExprPoly &op = expr.args(i);
    switch (op.type()) {
      case univplan::UNIVPLAN_EXPR_OPEXPR: {
        const univplan::UnivPlanOpExpr &opex = op.opexpr();
        bool has = do_opexpr2(pk, opex, allExpr, equalExpr, equal);
        if (*equal == 1) return true;
        if (has) hasExpr = true;
        break;
      }
      case univplan::UNIVPLAN_EXPR_BOOLEXPR: {
        const univplan::UnivPlanBoolExpr &opex = op.boolexpr();
        bool has = do_boolexpr2(pk, opex, allExpr, equalExpr, equal);
        if (*equal == 1) return true;
        if (has) hasExpr = true;
        break;
      }
      default:
        break;
    }
  }
  return hasExpr;
}

bool loopContinue(
    int pkCol, const univplan::UnivPlanExprPolyList *filterExprs,
    std::vector<std::unique_ptr<IndexExpr> > &allExpr,      // NOLINT
    std::vector<std::unique_ptr<IndexExpr> > &equalExpr) {  // NOLINT
  int filterSize = filterExprs->size();
  assert(filterSize > 0);
  bool hasExpr = false;
  for (int i = 0; i < filterSize; ++i) {
    const univplan::UnivPlanExprPoly &ep = filterExprs->Get(i);
    switch (ep.type()) {
      case univplan::UNIVPLAN_EXPR_OPEXPR: {
        const univplan::UnivPlanOpExpr &op = ep.opexpr();
        int equal = 0;
        bool has = do_opexpr2(pkCol, op, allExpr, equalExpr, &equal);
        if (equal == 1) return true;
        if (has) hasExpr = true;
        break;
      }
      case univplan::UNIVPLAN_EXPR_BOOLEXPR: {
        const univplan::UnivPlanBoolExpr &op = ep.boolexpr();
        if (op.type() == univplan::BOOLEXPRTYPE::BOOLEXPRTYPE_AND_EXPR) {
          int equal = 0;
          bool has = do_boolexpr2(pkCol, op, allExpr, equalExpr, &equal);
          if (equal == 1) return true;
          if (has) hasExpr = true;
        }
        break;
      }
      default:
        break;
    }
  }
  return hasExpr;
}

std::vector<std::vector<std::unique_ptr<IndexExpr> > > getIndexExpr2(
    const std::vector<int> &index,
    const univplan::UnivPlanExprPolyList *filterExprs) {
  std::vector<std::vector<std::unique_ptr<IndexExpr> > > indexExprVec;
  int indexSize = index.size();
  assert(indexSize > 0);
  for (int i = 0; i < indexSize; ++i) {
    int col = index[i];
    std::vector<std::unique_ptr<IndexExpr> > allExpr;
    std::vector<std::unique_ptr<IndexExpr> > equalExpr;
    bool loop = loopContinue(col, filterExprs, allExpr, equalExpr);
    if (!loop) return indexExprVec;
    int equalSize = equalExpr.size();
    int allSize = allExpr.size();
    if (equalSize > 0) {
      indexExprVec.push_back(std::move(equalExpr));
      continue;
    } else {
      if (allSize > 0) {
        indexExprVec.push_back(std::move(allExpr));
        return indexExprVec;
      }
    }
  }
  return indexExprVec;
}
}  // namespace univplan
