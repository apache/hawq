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

#include "univplan/minmax/minmax-predicates.h"

#include <memory>
#include <sstream>
#include <utility>

#include "dbcommon/log/logger.h"
#include "dbcommon/type/type-util.h"
#include "dbcommon/utils/string-util.h"

#include "univplan/common/plannode-walker.h"

namespace univplan {

ListPredicate::ListPredicate(const univplan::UnivPlanExprPolyList* exprs,
                             const MinMaxPredicatesAbstract* pred)
    : PredicateOper(pred) {
  for (int i = 0; i < exprs->size(); ++i) {
    children.push_back(
        MinMaxPredicatesAbstract::buildPredicateOper(&exprs->Get(i), pred));
  }
}

bool ListPredicate::canDrop() {
  // behave as And
  for (PredicateOper::uptr& child : children) {
    if (child->canDrop()) return true;
  }
  return false;
}

BooleanPredicate::BooleanPredicate(const MinMaxPredicatesAbstract* pred,
                                   const univplan::UnivPlanBoolExpr* op)
    : PredicateOper(pred) {
  for (int i = 0; i < op->args_size(); ++i) {
    children.push_back(
        MinMaxPredicatesAbstract::buildPredicateOper(&op->args(i), pred));
  }
}

bool VarPredicate::canDrop() {
  if (PREDPAGE(pred)->hasAllNull(var->varattno())) return true;
  PredicateStats s = PREDPAGE(pred)->getMinMax(var->varattno());
  if (s.hasMinMax && !dbcommon::DatumGetValue<bool>(s.maxValue.value))
    return true;
  return false;
}

bool IsTrueBooleanTestPredicate::canDrop() {
  if (PREDPAGE(pred)->hasAllNull(var->varattno())) return true;
  PredicateStats s = PREDPAGE(pred)->getMinMax(var->varattno());
  if (s.hasMinMax && !dbcommon::DatumGetValue<bool>(s.maxValue.value))
    return true;
  return false;
}

bool IsNotTrueBooleanTestPredicate::canDrop() {
  if (!PREDPAGE(pred)->hasNull(var->varattno())) {
    PredicateStats s = PREDPAGE(pred)->getMinMax(var->varattno());
    if (s.hasMinMax && dbcommon::DatumGetValue<bool>(s.minValue.value))
      return true;
  }
  return false;
}

bool IsFalseBooleanTestPredicate::canDrop() {
  if (PREDPAGE(pred)->hasAllNull(var->varattno())) return true;
  PredicateStats s = PREDPAGE(pred)->getMinMax(var->varattno());
  if (s.hasMinMax && dbcommon::DatumGetValue<bool>(s.minValue.value))
    return true;
  return false;
}

bool IsNotFalseBooleanTestPredicate::canDrop() {
  if (!PREDPAGE(pred)->hasNull(var->varattno())) {
    PredicateStats s = PREDPAGE(pred)->getMinMax(var->varattno());
    if (s.hasMinMax && !dbcommon::DatumGetValue<bool>(s.maxValue.value))
      return true;
  }
  return false;
}

bool IsUnknownBooleanTestPredicate::canDrop() {
  if (!PREDPAGE(pred)->hasNull(var->varattno())) return true;
  return false;
}

bool IsNotUnknownBooleanTestPredicate::canDrop() {
  if (PREDPAGE(pred)->hasAllNull(var->varattno())) return true;
  return false;
}

bool AndPredicate::canDrop() {
  // as long as one branch is OK to drop, we can drop it.
  for (PredicateOper::uptr& child : children) {
    if (child->canDrop()) return true;
  }
  return false;
}

bool OrPredicate::canDrop() {
  // as long as one branch is NOT ok to drop, we can NOT drop it.
  for (PredicateOper::uptr& child : children) {
    if (!child->canDrop()) return false;
  }
  return true;
}

bool IsNullPredicate::canDrop() {
  // as long as one var has null, we can't drop it
  for (auto index : varAttrNo) {
    if (PREDPAGE(pred)->hasNull(index)) return false;
  }
  return true;
}

bool IsNotNullPredicate::canDrop() {
  // if one var has all null, we can drop it
  for (auto index : varAttrNo) {
    if (PREDPAGE(pred)->hasAllNull(index)) return true;
  }
  return false;
}

dbcommon::TupleBatch::uptr CompPredicate::buildTupleBatch(int32_t argIndex) {
  dbcommon::TupleBatch::uptr batch(
      new dbcommon::TupleBatch(*PREDPAGE(pred)->getTupleDesc(), true));
  dbcommon::TupleBatchWriter& writer = batch->getTupleBatchWriter();
  for (uint32_t i = 0; i < batch->getNumOfColumns(); ++i) {
    if (varAttNo[argIndex].size() == 1 && i == varAttNo[argIndex][0] - 1) {
      PredicateStats s =
          PREDPAGE(pred)->getMinMax(i + 1, &minTimestamp, &maxTimestamp);
      if (!s.hasMinMax) return nullptr;
      writer[i]->append(&s.minValue);
      writer[i]->append(&s.maxValue);
    }
  }
  batch->incNumOfRows(2);
  return std::move(batch);
}

PredicateStats::uptr CompPredicate::calcLeft() { return doCalc(0); }

PredicateStats::uptr CompPredicate::calcRight() { return doCalc(1); }

PredicateStats::uptr CompPredicate::doCalc(int32_t argIndex) {
  PredicateStats::uptr stats(new PredicateStats);

  if (varAttNo[argIndex].size() == 1 &&
      PREDPAGE(pred)->hasAllNull(varAttNo[argIndex][0])) {
    stats->hasAllNull = true;
    return std::move(stats);
  }

  tbVec_.push_back(buildTupleBatch(argIndex));
  if (!tbVec_[argIndex]) return nullptr;
  ExprContext context;
  context.scanBatch = tbVec_[argIndex].get();
  dbcommon::Object* obj = dbcommon::DatumGetValue<dbcommon::Object*>(
      args[argIndex]->calc(&context));
  dbcommon::Vector* vec = dynamic_cast<dbcommon::Vector*>(obj);
  if (!vec) {
    dbcommon::Scalar* scalar = dynamic_cast<dbcommon::Scalar*>(obj);
    stats->maxValue = *scalar;
    stats->minValue = *scalar;
  } else {
    dbcommon::Scalar s1;
    dbcommon::Scalar s2;
    vec->readPlainScalar(0, &s1);
    vec->readPlainScalar(1, &s2);
    int32_t comRes =
        compareTo(s1, typeKinds[argIndex], s2, typeKinds[argIndex]);
    stats->maxValue = comRes >= 0 ? s1 : s2;
    stats->minValue = comRes <= 0 ? s1 : s2;
  }
  return std::move(stats);
}

bool CompPredicate::isValidToPredicate() const {
  // for arg1 op arg2, currently we only allow at most one variable
  // exists in each side, and the two vars can't be the same one
  if (varAttNo[0].size() > 1 || varAttNo[1].size() > 1) return false;

  if (varAttNo[0].size() == 1 && varAttNo[1].size() == 1 &&
      varAttNo[0][0] == varAttNo[1][0])
    return false;

  return true;
}

std::string CompPredicate::covertPredicateTypeStr(std::string typeName) {
  if (typeName == "date")
    return "int32";
  else if (typeName == "time")
    return "int64";
  else if (typeName == "bpchar" || typeName == "varchar")
    return "string";
  return typeName;
}

int32_t CompPredicate::compareTo(const dbcommon::Scalar& s1,
                                 dbcommon::TypeKind t1,
                                 const dbcommon::Scalar& s2,
                                 dbcommon::TypeKind t2) {
  std::string type1 =
      covertPredicateTypeStr(dbcommon::TypeUtil::getTypeNameById(t1));
  std::string type2 =
      covertPredicateTypeStr(dbcommon::TypeUtil::getTypeNameById(t2));
  std::string funcName = type1 + "_less_than_" + type2;
  const dbcommon::FuncEntry* funcEntry =
      dbcommon::Func::instance()->getFuncEntryByName(funcName);
  if (!funcEntry)
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "func name %s not found in func system",
              funcName.c_str());
  std::unique_ptr<dbcommon::Object> retval(new dbcommon::Scalar);
  std::unique_ptr<dbcommon::Invoker> invoker(
      new dbcommon::Invoker(funcEntry->funcId));
  invoker->resetPrarmeter();
  invoker->addParam(dbcommon::CreateDatum<dbcommon::Object*>(retval.get()));
  invoker->addParam(dbcommon::CreateDatum<const dbcommon::Object*>(&s1));
  invoker->addParam(dbcommon::CreateDatum<const dbcommon::Object*>(&s2));
  invoker->invoke();
  if (dbcommon::DatumGetValue<bool>(
          dynamic_cast<dbcommon::Scalar*>(retval.get())->value))
    return -1;

  funcName = type1 + "_equal_" + type2;
  funcEntry = dbcommon::Func::instance()->getFuncEntryByName(funcName);
  if (!funcEntry)
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "func name %s not found in func system",
              funcName.c_str());
  invoker.reset(new dbcommon::Invoker(funcEntry->funcId));
  invoker->resetPrarmeter();
  invoker->addParam(dbcommon::CreateDatum<dbcommon::Object*>(retval.get()));
  invoker->addParam(dbcommon::CreateDatum<const dbcommon::Object*>(&s1));
  invoker->addParam(dbcommon::CreateDatum<const dbcommon::Object*>(&s2));
  invoker->invoke();
  if (dbcommon::DatumGetValue<bool>(
          dynamic_cast<dbcommon::Scalar*>(retval.get())->value))
    return 0;

  return 1;
}

bool EqualPredicate::canDrop() {
  if (!isValidToPredicate()) return false;

  PredicateStats::uptr leftStat = this->calcLeft();
  PredicateStats::uptr rightStat = this->calcRight();

  // left or right has no statistics
  if (leftStat == nullptr || rightStat == nullptr) return false;

  // if either side is ALL null, can drop
  if (leftStat->hasAllNull || rightStat->hasAllNull) return true;

  // can drop when left's max < right's min, or right's max < left's min
  if (compareTo(leftStat->maxValue, typeKinds[0], rightStat->minValue,
                typeKinds[1]) < 0 ||
      compareTo(rightStat->maxValue, typeKinds[1], leftStat->minValue,
                typeKinds[0]) < 0)
    return true;

  // now check bloom filter
  int32_t columnId;
  PredicateStats* stat;
  dbcommon::TypeKind type;
  if (expr->args(0).has_var() && expr->args(1).has_val()) {
    columnId = expr->args(0).var().varattno();
    stat = rightStat.get();
    type = typeKinds[1];
  } else if (expr->args(1).has_var() && expr->args(0).has_val()) {
    columnId = expr->args(1).var().varattno();
    stat = leftStat.get();
    type = typeKinds[0];
  } else {
    return false;
  }

  if (PREDPAGE(pred)->canDropByBloomFilter(columnId, stat, type)) return true;

  return false;
}

bool NEPredicate::canDrop() {
  if (!isValidToPredicate()) return false;

  PredicateStats::uptr leftStat = this->calcLeft();
  PredicateStats::uptr rightStat = this->calcRight();

  // left or right has no statistics
  if (leftStat == nullptr || rightStat == nullptr) return false;

  // if either side is ALL null, can drop
  if (leftStat->hasAllNull || rightStat->hasAllNull) return true;

  // can drop when there is only one unique value.
  if (compareTo(leftStat->minValue, typeKinds[0], leftStat->maxValue,
                typeKinds[0]) == 0 &&
      compareTo(rightStat->minValue, typeKinds[1], rightStat->maxValue,
                typeKinds[1]) == 0 &&
      compareTo(leftStat->maxValue, typeKinds[0], rightStat->maxValue,
                typeKinds[1]) == 0)
    return true;
  else
    return false;
}

bool GTPredicate::canDrop() {
  if (!isValidToPredicate()) return false;

  PredicateStats::uptr leftStat = this->calcLeft();
  PredicateStats::uptr rightStat = this->calcRight();

  // left or right has no statistics
  if (leftStat == nullptr || rightStat == nullptr) return false;

  // if either side is ALL null, can drop
  if (leftStat->hasAllNull || rightStat->hasAllNull) return true;

  // can drop when left's max <= right's min.
  if (compareTo(leftStat->maxValue, typeKinds[0], rightStat->minValue,
                typeKinds[1]) <= 0)
    return true;
  else
    return false;
}

bool GEPredicate::canDrop() {
  if (!isValidToPredicate()) return false;

  PredicateStats::uptr leftStat = this->calcLeft();
  PredicateStats::uptr rightStat = this->calcRight();

  // left or right has no statistics
  if (leftStat == nullptr || rightStat == nullptr) return false;

  // if either side is ALL null, can drop
  if (leftStat->hasAllNull || rightStat->hasAllNull) return true;

  // can drop when left's max < right's min.
  if (compareTo(leftStat->maxValue, typeKinds[0], rightStat->minValue,
                typeKinds[1]) < 0) {
    return true;
  } else {
    return false;
  }
}

bool LTPredicate::canDrop() {
  if (!isValidToPredicate()) return false;

  PredicateStats::uptr leftStat = this->calcLeft();
  PredicateStats::uptr rightStat = this->calcRight();

  // left or right has no statistics
  if (leftStat == nullptr || rightStat == nullptr) return false;

  // if either side is ALL null, can drop
  if (leftStat->hasAllNull || rightStat->hasAllNull) return true;

  // can drop when right's max <= left's min.
  if (compareTo(rightStat->maxValue, typeKinds[1], leftStat->minValue,
                typeKinds[0]) <= 0)
    return true;
  else
    return false;
}

bool LEPredicate::canDrop() {
  if (!isValidToPredicate()) return false;

  PredicateStats::uptr leftStat = this->calcLeft();
  PredicateStats::uptr rightStat = this->calcRight();

  // left or right has no statistics
  if (leftStat == nullptr || rightStat == nullptr) return false;

  // if either side is ALL null, can drop
  if (leftStat->hasAllNull || rightStat->hasAllNull) return true;

  // can drop when right's max < left's min.
  if (compareTo(rightStat->maxValue, typeKinds[1], leftStat->minValue,
                typeKinds[0]) < 0)
    return true;
  else
    return false;
}

PredicateOper::uptr MinMaxPredicatesAbstract::buildPredicateOper(
    const univplan::UnivPlanExprPolyList* exprs,
    const MinMaxPredicatesAbstract* owner) {
  return PredicateOper::uptr(new ListPredicate(exprs, owner));
}

PredicateOper::uptr MinMaxPredicatesAbstract::buildPredicateOper(
    const univplan::UnivPlanExprPoly* expr,
    const MinMaxPredicatesAbstract* owner) {
  switch (expr->type()) {
    case univplan::UNIVPLAN_EXPR_OPEXPR: {
      const univplan::UnivPlanOpExpr& op = expr->opexpr();
      CompPredicate::uptr ret;
      std::string funcName =
          dbcommon::Func::instance()
              ->getFuncEntryById(static_cast<dbcommon::FuncKind>(op.funcid()))
              ->funcName;
      if (dbcommon::StringUtil::countReplicates(funcName, "less_than") == 1)
        ret.reset(new LTPredicate(owner, &op));
      else if (dbcommon::StringUtil::countReplicates(funcName, "less_eq") == 1)
        ret.reset(new LEPredicate(owner, &op));
      else if (dbcommon::StringUtil::countReplicates(funcName, "not_equal") ==
               1)
        ret.reset(new NEPredicate(owner, &op));
      else if (dbcommon::StringUtil::countReplicates(funcName, "equal") == 1)
        ret.reset(new EqualPredicate(owner, &op));
      else if (dbcommon::StringUtil::countReplicates(funcName,
                                                     "greater_than") == 1)
        ret.reset(new GTPredicate(owner, &op));
      else if (dbcommon::StringUtil::countReplicates(funcName, "greater_eq") ==
               1)
        ret.reset(new GEPredicate(owner, &op));
      else
        goto end;
      for (int i = 0; i < op.args_size(); ++i)
        ret->addArg(InitExpr(&op.args(i)));
      return std::move(ret);
    }
    case univplan::UNIVPLAN_EXPR_BOOLEXPR: {
      const univplan::UnivPlanBoolExpr& op = expr->boolexpr();
      BooleanPredicate::uptr ret;
      if (op.type() == univplan::BOOLEXPRTYPE::BOOLEXPRTYPE_AND_EXPR)
        ret.reset(new AndPredicate(owner, &op));
      else if (op.type() == univplan::BOOLEXPRTYPE::BOOLEXPRTYPE_OR_EXPR)
        ret.reset(new OrPredicate(owner, &op));
      else
        goto end;
      return std::move(ret);
    }
    case univplan::UNIVPLAN_EXPR_NULLTEST: {
      const univplan::UnivPlanNullTest& op = expr->nulltest();
      NullTestPredicate::uptr ret;
      if (op.type() == univplan::NULLTESTTYPE::NULLTESTTYPE_IS_NULL) {
        ret.reset(new IsNullPredicate(owner, &op));
      } else {
        assert(op.type() == univplan::NULLTESTTYPE::NULLTESTTYPE_IS_NOT_NULL);
        ret.reset(new IsNotNullPredicate(owner, &op));
      }
      return std::move(ret);
    }
    case univplan::UNIVPLAN_EXPR_VAR: {
      const univplan::UnivPlanVar& var = expr->var();
      VarPredicate::uptr ret(new VarPredicate(owner, &var));
      return std::move(ret);
    }
    case univplan::UNIVPLAN_EXPR_BOOLEANTEST: {
      const univplan::UnivPlanBooleanTest& op = expr->booltest();
      BooleanTestPredicate::uptr ret;
      if (op.type() == univplan::BOOLTESTTYPE::BOOLTESTTYPE_IS_TRUE) {
        ret.reset(new IsTrueBooleanTestPredicate(owner, &op));
      } else if (op.type() ==
                 univplan::BOOLTESTTYPE::BOOLTESTTYPE_IS_NOT_TRUE) {
        ret.reset(new IsNotTrueBooleanTestPredicate(owner, &op));
      } else if (op.type() == univplan::BOOLTESTTYPE::BOOLTESTTYPE_IS_FALSE) {
        ret.reset(new IsFalseBooleanTestPredicate(owner, &op));
      } else if (op.type() ==
                 univplan::BOOLTESTTYPE::BOOLTESTTYPE_IS_NOT_FALSE) {
        ret.reset(new IsNotFalseBooleanTestPredicate(owner, &op));
      } else if (op.type() == univplan::BOOLTESTTYPE::BOOLTESTTYPE_IS_UNKNOWN) {
        ret.reset(new IsUnknownBooleanTestPredicate(owner, &op));
      } else {
        assert(op.type() ==
               univplan::BOOLTESTTYPE::BOOLTESTTYPE_IS_NOT_UNKNOWN);
        ret.reset(new IsNotUnknownBooleanTestPredicate(owner, &op));
      }
      return std::move(ret);
    }
    default: {
      // do nothing
      break;
    }
  }
end:
  LOG_INFO("Predicate warning: expr is not predicated: %s",
           expr->DebugString().c_str());
  PredicateOper::uptr ret(new PredicateOper(owner));
  return std::move(ret);
}

bool MinMaxPredicatesPage::canDrop() {
  if (nullptr == exprs) {
    return false;
  }

  try {
    predicate = MinMaxPredicatesAbstract::buildPredicateOper(exprs, this);
    return predicate->canDrop();
  } catch (...) {
    return false;
  }
}

COBlockTaskList::COBlockTaskList() {}

void COBlockTaskList::intersect(const COBlockTaskList& source) {
  auto selfIter = taskList.begin();
  auto srcIter = source.taskList.begin();
  if (selfIter != taskList.end()) {
    selfIter->second->resetIntersection();
    return;  // no task to process
  }
  while (srcIter != source.taskList.end() && selfIter != taskList.end()) {
    COBlockTask* selfTask = selfIter->second.get();
    COBlockTask* srcTask = srcIter->second.get();
    if (selfTask->taskBeginRowId > srcTask->getTaskEndRowId()) {
      srcIter++;  // try next source task
    } else if (srcTask->taskBeginRowId > selfTask->getTaskEndRowId()) {
      selfIter++;
      if (selfIter != taskList.end()) {
        selfIter->second->resetIntersection();
      }
    } else {
      selfIter->second->intersect(*srcTask);
      srcIter++;  // try next source task
    }
  }
}

void COBlockTaskList::unionAll(const COBlockTaskList& source) {
  auto selfIter = taskList.begin();
  auto srcIter = source.taskList.begin();
  if (selfIter != taskList.end()) {
    selfIter->second->resetUnionAll();
    return;  // no task to process
  }
  while (srcIter != source.taskList.end() && selfIter != taskList.end()) {
    COBlockTask* selfTask = selfIter->second.get();
    COBlockTask* srcTask = srcIter->second.get();
    if (selfTask->beginRowId > srcTask->getTaskEndRowId()) {
      srcIter++;  // try next source task
    } else if (srcTask->taskBeginRowId > selfTask->getEndRowId()) {
      selfIter++;
      if (selfIter != taskList.end()) {
        selfIter->second->resetUnionAll();
      }
    } else {
      selfIter->second->unionAll(*srcTask);
      srcIter++;  // try next source task
    }
  }
}

void MinMaxPredicatesCO::preparePredicates() {
  predicate.reset(new ListPredicate(exprs, this));
}

void MinMaxPredicatesCO::calculate(
    std::unordered_map<int, COBlockTaskList::uptr>* res) {
  // prepare res
  for (int i = 0; i < this->td->getNumOfColumns(); ++i) {
    (*res)[i + 1].reset(new COBlockTaskList());
  }

  while (true) {
    predicatesForPages.reset();
    preparePredicatesForStrip();
    if (predicatesForPages == nullptr) {
      break;  // no more to work with
    }
    if (!predicatesForPages->canDrop()) {
      for (auto iter = res->begin(); iter != res->end(); ++iter) {
        iter->second->add(std::move(buildCOTaskFromCurrentStrip(iter->first)));
      }
    }
  }
}

}  // namespace univplan
