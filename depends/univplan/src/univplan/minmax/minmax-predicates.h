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

#ifndef UNIVPLAN_SRC_UNIVPLAN_MINMAX_MINMAX_PREDICATES_H_
#define UNIVPLAN_SRC_UNIVPLAN_MINMAX_MINMAX_PREDICATES_H_

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "dbcommon/common/tuple-batch.h"
#include "dbcommon/common/tuple-desc.h"
#include "dbcommon/nodes/datum.h"

#include "univplan/common/expression.h"
#include "univplan/common/statistics.h"
#include "univplan/common/univplan-type.h"
#include "univplan/common/var-util.h"

namespace univplan {

class PredicateStats {
 public:
  PredicateStats() {}
  virtual ~PredicateStats() {}
  typedef std::unique_ptr<PredicateStats> uptr;

  dbcommon::Scalar maxValue;
  dbcommon::Scalar minValue;
  bool hasAllNull = false;
  bool hasMinMax = false;
};

class MinMaxPredicatesAbstract;
class MinMaxPredicatesPage;
class MinMaxPredicatesCO;

#define PREDPAGE(pred) (dynamic_cast<const MinMaxPredicatesPage*>(pred))
#define PREDCO(pred) (dynamic_cast<const MinMaxPredicatesCO*>(pred))

class PredicateOper {
 public:
  explicit PredicateOper(const MinMaxPredicatesAbstract* pred) : pred(pred) {}
  virtual ~PredicateOper() {}

  typedef std::unique_ptr<PredicateOper> uptr;

  virtual bool canDrop() { return false; }

 protected:
  const MinMaxPredicatesAbstract* pred = nullptr;
};

class ListPredicate : public PredicateOper {
 public:
  ListPredicate(const univplan::UnivPlanExprPolyList* exprs,
                const MinMaxPredicatesAbstract* pred);
  ~ListPredicate() {}

  typedef std::unique_ptr<ListPredicate> uptr;

  bool canDrop() override;

 private:
  std::vector<PredicateOper::uptr> children;
};

class BooleanPredicate : public PredicateOper {
 public:
  BooleanPredicate(const MinMaxPredicatesAbstract* pred,
                   const univplan::UnivPlanBoolExpr* op);
  ~BooleanPredicate() {}

  typedef std::unique_ptr<BooleanPredicate> uptr;

 protected:
  std::vector<PredicateOper::uptr> children;
};

class VarPredicate : public PredicateOper {
 public:
  VarPredicate(const MinMaxPredicatesAbstract* pred,
               const univplan::UnivPlanVar* var)
      : PredicateOper(pred), var(var) {}
  ~VarPredicate() {}

  typedef std::unique_ptr<VarPredicate> uptr;

  bool canDrop() override;

 private:
  const univplan::UnivPlanVar* var;
};

class BooleanTestPredicate : public PredicateOper {
 public:
  BooleanTestPredicate(const MinMaxPredicatesAbstract* pred,
                       const univplan::UnivPlanBooleanTest* op)
      : PredicateOper(pred), var(&op->arg().var()) {}
  ~BooleanTestPredicate() {}

  typedef std::unique_ptr<BooleanTestPredicate> uptr;

 protected:
  const univplan::UnivPlanVar* var;
};

class IsTrueBooleanTestPredicate : public BooleanTestPredicate {
 public:
  IsTrueBooleanTestPredicate(const MinMaxPredicatesAbstract* pred,
                             const univplan::UnivPlanBooleanTest* op)
      : BooleanTestPredicate(pred, op) {}
  ~IsTrueBooleanTestPredicate() {}

  typedef std::unique_ptr<IsTrueBooleanTestPredicate> uptr;

  bool canDrop() override;
};

class IsNotTrueBooleanTestPredicate : public BooleanTestPredicate {
 public:
  IsNotTrueBooleanTestPredicate(const MinMaxPredicatesAbstract* pred,
                                const univplan::UnivPlanBooleanTest* op)
      : BooleanTestPredicate(pred, op) {}
  ~IsNotTrueBooleanTestPredicate() {}

  typedef std::unique_ptr<IsNotTrueBooleanTestPredicate> uptr;

  bool canDrop() override;
};

class IsFalseBooleanTestPredicate : public BooleanTestPredicate {
 public:
  IsFalseBooleanTestPredicate(const MinMaxPredicatesAbstract* pred,
                              const univplan::UnivPlanBooleanTest* op)
      : BooleanTestPredicate(pred, op) {}
  ~IsFalseBooleanTestPredicate() {}

  typedef std::unique_ptr<IsFalseBooleanTestPredicate> uptr;

  bool canDrop() override;
};

class IsNotFalseBooleanTestPredicate : public BooleanTestPredicate {
 public:
  IsNotFalseBooleanTestPredicate(const MinMaxPredicatesAbstract* pred,
                                 const univplan::UnivPlanBooleanTest* op)
      : BooleanTestPredicate(pred, op) {}
  ~IsNotFalseBooleanTestPredicate() {}

  typedef std::unique_ptr<IsNotFalseBooleanTestPredicate> uptr;

  bool canDrop() override;
};

class IsUnknownBooleanTestPredicate : public BooleanTestPredicate {
 public:
  IsUnknownBooleanTestPredicate(const MinMaxPredicatesAbstract* pred,
                                const univplan::UnivPlanBooleanTest* op)
      : BooleanTestPredicate(pred, op) {}
  ~IsUnknownBooleanTestPredicate() {}

  typedef std::unique_ptr<IsUnknownBooleanTestPredicate> uptr;

  bool canDrop() override;
};

class IsNotUnknownBooleanTestPredicate : public BooleanTestPredicate {
 public:
  IsNotUnknownBooleanTestPredicate(const MinMaxPredicatesAbstract* pred,
                                   const univplan::UnivPlanBooleanTest* op)
      : BooleanTestPredicate(pred, op) {}
  ~IsNotUnknownBooleanTestPredicate() {}

  typedef std::unique_ptr<IsNotUnknownBooleanTestPredicate> uptr;

  bool canDrop() override;
};

class AndPredicate : public BooleanPredicate {
 public:
  AndPredicate(const MinMaxPredicatesAbstract* pred,
               const univplan::UnivPlanBoolExpr* op)
      : BooleanPredicate(pred, op) {}
  ~AndPredicate() {}

  typedef std::unique_ptr<AndPredicate> uptr;

  bool canDrop() override;
};

class OrPredicate : public BooleanPredicate {
 public:
  OrPredicate(const MinMaxPredicatesAbstract* pred,
              const univplan::UnivPlanBoolExpr* op)
      : BooleanPredicate(pred, op) {}
  ~OrPredicate() {}

  typedef std::unique_ptr<OrPredicate> uptr;

  bool canDrop() override;
};

class NullTestPredicate : public PredicateOper {
 public:
  NullTestPredicate(const MinMaxPredicatesAbstract* pred,
                    const univplan::UnivPlanNullTest* op)
      : PredicateOper(pred) {
    if (!op->arg().has_var())
      LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                "NullTestPredicate only work for simple expression");
    univplan::VarUtil varUtil;
    varAttrNo = varUtil.collectVarAttNo(
        const_cast<univplan::UnivPlanExprPoly*>(&op->arg()));
  }
  ~NullTestPredicate() {}

  typedef std::unique_ptr<NullTestPredicate> uptr;

 protected:
  std::vector<int32_t> varAttrNo;
};

class IsNullPredicate : public NullTestPredicate {
 public:
  IsNullPredicate(const MinMaxPredicatesAbstract* pred,
                  const univplan::UnivPlanNullTest* op)
      : NullTestPredicate(pred, op) {}
  ~IsNullPredicate() {}

  typedef std::unique_ptr<IsNullPredicate> uptr;

  bool canDrop() override;
};

class IsNotNullPredicate : public NullTestPredicate {
 public:
  IsNotNullPredicate(const MinMaxPredicatesAbstract* pred,
                     const univplan::UnivPlanNullTest* op)
      : NullTestPredicate(pred, op) {}
  ~IsNotNullPredicate() {}

  typedef std::unique_ptr<IsNotNullPredicate> uptr;

  bool canDrop() override;
};

class CompPredicate : public PredicateOper {
 public:
  CompPredicate(const MinMaxPredicatesAbstract* pred,
                const univplan::UnivPlanOpExpr* op)
      : PredicateOper(pred), expr(op) {
    assert(op->args_size() == 2);
    typeKinds.push_back(univplan::PlanNodeUtil::exprType(op->args(0)));
    typeKinds.push_back(univplan::PlanNodeUtil::exprType(op->args(1)));
    univplan::VarUtil varUtil;
    varAttNo.push_back(varUtil.collectVarAttNo(
        const_cast<univplan::UnivPlanExprPoly*>(&op->args(0))));
    varAttNo.push_back(varUtil.collectVarAttNo(
        const_cast<univplan::UnivPlanExprPoly*>(&op->args(1))));
  }
  ~CompPredicate() {}

  typedef std::unique_ptr<CompPredicate> uptr;

  void addArg(ExprState::uptr arg) { args.push_back(std::move(arg)); }

  bool isValidToPredicate() const;

 protected:
  PredicateStats::uptr calcLeft();
  PredicateStats::uptr calcRight();
  int32_t compareTo(const dbcommon::Scalar& s1, dbcommon::TypeKind t1,
                    const dbcommon::Scalar& s2, dbcommon::TypeKind t2);

 private:
  dbcommon::TupleBatch::uptr buildTupleBatch(int32_t argIndex);
  PredicateStats::uptr doCalc(int32_t argIndex);
  std::string covertPredicateTypeStr(std::string typeName);

 protected:
  std::vector<dbcommon::TypeKind> typeKinds;
  const univplan::UnivPlanOpExpr* expr;

 private:
  std::vector<ExprState::uptr> args;
  std::vector<std::vector<int32_t>> varAttNo;
  dbcommon::Timestamp minTimestamp;
  dbcommon::Timestamp maxTimestamp;
  std::vector<dbcommon::TupleBatch::uptr> tbVec_;
};

class EqualPredicate : public CompPredicate {
 public:
  EqualPredicate(const MinMaxPredicatesAbstract* pred,
                 const univplan::UnivPlanOpExpr* op)
      : CompPredicate(pred, op) {}
  ~EqualPredicate() {}

  typedef std::unique_ptr<EqualPredicate> uptr;

  bool canDrop() override;
};

class NEPredicate : public CompPredicate {
 public:
  NEPredicate(const MinMaxPredicatesAbstract* pred,
              const univplan::UnivPlanOpExpr* op)
      : CompPredicate(pred, op) {}
  ~NEPredicate() {}

  typedef std::unique_ptr<NEPredicate> uptr;

  bool canDrop() override;
};

class GTPredicate : public CompPredicate {
 public:
  GTPredicate(const MinMaxPredicatesAbstract* pred,
              const univplan::UnivPlanOpExpr* op)
      : CompPredicate(pred, op) {}
  ~GTPredicate() {}

  typedef std::unique_ptr<GTPredicate> uptr;

  bool canDrop() override;
};

class GEPredicate : public CompPredicate {
 public:
  GEPredicate(const MinMaxPredicatesAbstract* pred,
              const univplan::UnivPlanOpExpr* op)
      : CompPredicate(pred, op) {}
  ~GEPredicate() {}

  typedef std::unique_ptr<GEPredicate> uptr;

  bool canDrop() override;
};

class LTPredicate : public CompPredicate {
 public:
  LTPredicate(const MinMaxPredicatesAbstract* pred,
              const univplan::UnivPlanOpExpr* op)
      : CompPredicate(pred, op) {}
  ~LTPredicate() {}

  typedef std::unique_ptr<LTPredicate> uptr;

  bool canDrop() override;
};

class LEPredicate : public CompPredicate {
 public:
  LEPredicate(const MinMaxPredicatesAbstract* pred,
              const univplan::UnivPlanOpExpr* op)
      : CompPredicate(pred, op) {}
  ~LEPredicate() {}

  typedef std::unique_ptr<LEPredicate> uptr;

  bool canDrop() override;
};

class MinMaxPredicatesAbstract {
 public:
  MinMaxPredicatesAbstract(const univplan::UnivPlanExprPolyList* predicateExprs,
                           const dbcommon::TupleDesc* tupleDesc)
      : exprs(predicateExprs), td(tupleDesc) {}
  virtual ~MinMaxPredicatesAbstract() {}

  // facility for building general purpose predicate tree
  static PredicateOper::uptr buildPredicateOper(
      const univplan::UnivPlanExprPolyList* exprs,
      const MinMaxPredicatesAbstract* owner);

  static PredicateOper::uptr buildPredicateOper(
      const univplan::UnivPlanExprPoly* expr,
      const MinMaxPredicatesAbstract* owner);

  const dbcommon::TupleDesc* getTupleDesc() const { return td; }

 protected:
  const univplan::UnivPlanExprPolyList* exprs;  // original pushed filter
  const dbcommon::TupleDesc* td;                // full tuple desc
  PredicateOper::uptr predicate;                // generated predicate
};

class MinMaxPredicatesPage : public MinMaxPredicatesAbstract {
 public:
  MinMaxPredicatesPage(const Statistics* s,
                       const univplan::UnivPlanExprPolyList* predicateExprs,
                       const dbcommon::TupleDesc* tupleDesc)
      : MinMaxPredicatesAbstract(predicateExprs, tupleDesc), stripeStats(s) {}
  virtual ~MinMaxPredicatesPage() {}

  typedef std::unique_ptr<MinMaxPredicatesPage> uptr;

  virtual bool canDrop();  // page level min-max calculation interface

 public:
  virtual bool hasNull(int32_t colId) const = 0;
  virtual bool hasAllNull(int32_t colId) const = 0;
  virtual bool canDropByBloomFilter(int32_t colId, PredicateStats* stat,
                                    dbcommon::TypeKind type) const = 0;
  virtual PredicateStats getMinMax(int32_t colId) const = 0;
  virtual PredicateStats getMinMax(int32_t colId,
                                   dbcommon::Timestamp* minTimestamp,
                                   dbcommon::Timestamp* maxTimestamp) const = 0;

 protected:
  const Statistics* stripeStats;
};

class COBlockTask {
 public:
  COBlockTask(uint64_t offset, uint64_t beginRowId, uint64_t rowCount)
      : offset(offset),
        beginRowId(beginRowId),
        rowCount(rowCount),
        taskBeginRowId(beginRowId),
        taskRowCount(rowCount),
        lastTaskBeginRowId(0),
        lastTaskRowCount(0),
        intersected(false) {}

  COBlockTask(uint64_t offset, uint64_t beginRowId, uint64_t rowCount,
              uint64_t taskBeginRowId, uint64_t taskRowCount)
      : offset(offset),
        beginRowId(beginRowId),
        rowCount(rowCount),
        taskBeginRowId(taskBeginRowId),
        taskRowCount(taskRowCount),
        lastTaskBeginRowId(0),
        lastTaskRowCount(0),
        intersected(false) {
    LOG_DEBUG("CO block task %llu %llu %llu %llu %llu", offset, beginRowId,
              rowCount, taskBeginRowId, taskRowCount);
  }

  typedef std::unique_ptr<COBlockTask> uptr;

 public:
  inline uint64_t getEndRowId() const { return beginRowId + rowCount - 1; }
  inline uint64_t getTaskEndRowId() const {
    return taskBeginRowId + taskRowCount - 1;
  }
  inline uint64_t getLastTaskEndRowId() const {
    return lastTaskBeginRowId + lastTaskRowCount - 1;
  }

  inline void resetIntersection() {
    intersected = false;
    lastTaskBeginRowId = taskBeginRowId;
    lastTaskRowCount = taskRowCount;
    taskRowCount = 0;
  }

  inline void resetUnionAll() {
    lastTaskBeginRowId = taskBeginRowId;
    lastTaskRowCount = taskRowCount;
  }

  inline void intersect(const COBlockTask& srcTask) {
    if (srcTask.getTaskEndRowId() < lastTaskBeginRowId ||
        srcTask.taskBeginRowId > this->getLastTaskEndRowId()) {
      if (!intersected) {
        if (srcTask.getEndRowId() < this->lastTaskBeginRowId) {
          // does not touch existing job, so restore original version
          taskBeginRowId = lastTaskBeginRowId;
          taskRowCount = lastTaskRowCount;
          intersected = true;
        } else if (srcTask.getEndRowId() <= this->getLastTaskEndRowId()) {
          // restore partial job
          taskBeginRowId = srcTask.getEndRowId() + 1;
          taskRowCount = this->getLastTaskEndRowId() - taskBeginRowId + 1;
          intersected = true;
        }
        return;
      }
    }

    if (!intersected) {
      // fix task begin row id, the source task may have smaller task begin row
      // id, thus check to make sure this task has valid task begin row id. end
      // row id has the same problem as well.
      taskBeginRowId = srcTask.taskBeginRowId < taskBeginRowId
                           ? taskBeginRowId
                           : srcTask.taskBeginRowId;
      uint64_t tmpTaskEndRowId = srcTask.getTaskEndRowId();
      tmpTaskEndRowId = tmpTaskEndRowId > this->getLastTaskEndRowId()
                            ? this->getLastTaskEndRowId()
                            : tmpTaskEndRowId;
      taskRowCount = tmpTaskEndRowId - taskBeginRowId + 1;
      intersected = true;
    } else {
      // dont check begin row id again, as we expect srce tasks are processed
      // in ascending order, and we use last task row id range to perform
      // intersection
      uint64_t tmpTaskEndRowId = srcTask.getTaskEndRowId();
      tmpTaskEndRowId = tmpTaskEndRowId > this->getLastTaskEndRowId()
                            ? this->getLastTaskEndRowId()
                            : tmpTaskEndRowId;
      taskRowCount = tmpTaskEndRowId - taskBeginRowId + 1;
    }
  }

  inline void unionAll(const COBlockTask& srcTask) {
    if (srcTask.getTaskEndRowId() < beginRowId ||
        srcTask.taskBeginRowId > this->getEndRowId()) {
      // skip this part, as no overlap in row id range
      return;
    }
    // we always check its possible new begin rowid and it should not go out of
    // current task range
    taskBeginRowId = taskBeginRowId > srcTask.taskBeginRowId
                         ? srcTask.taskBeginRowId
                         : taskBeginRowId;
    taskBeginRowId = taskBeginRowId < beginRowId ? beginRowId : taskBeginRowId;
    // then is its end rowid
    uint64_t tmpTaskEndRowId = srcTask.getTaskEndRowId();
    tmpTaskEndRowId = tmpTaskEndRowId > this->getTaskEndRowId()
                          ? tmpTaskEndRowId
                          : this->getTaskEndRowId();
    tmpTaskEndRowId = tmpTaskEndRowId > this->getEndRowId()
                          ? this->getEndRowId()
                          : tmpTaskEndRowId;
    taskRowCount = tmpTaskEndRowId - taskBeginRowId + 1;
  }

 public:
  uint64_t offset;
  uint64_t beginRowId;
  uint64_t rowCount;
  uint64_t taskBeginRowId;
  uint64_t taskRowCount;
  uint64_t lastTaskBeginRowId;
  uint64_t lastTaskRowCount;

 private:
  bool intersected;
};

class COBlockTaskList {
 public:
  COBlockTaskList();

  void add(COBlockTask::uptr task) {
    uint64_t taskBeginRowId = task->taskBeginRowId;
    taskList[taskBeginRowId] = std::move(task);
  }
  void intersect(const COBlockTaskList& source);
  void unionAll(const COBlockTaskList& source);

  typedef std::unique_ptr<COBlockTaskList> uptr;

 public:
  std::map<uint64_t, COBlockTask::uptr> taskList;
};

class MinMaxPredicatesCO : public MinMaxPredicatesAbstract {
 public:
  MinMaxPredicatesCO(const univplan::UnivPlanExprPolyList* predicateExprs,
                     const dbcommon::TupleDesc* tupleDesc)
      : MinMaxPredicatesAbstract(predicateExprs, tupleDesc) {}
  virtual ~MinMaxPredicatesCO() {}

  // methods implemented by sub-class to provide customized data preparation
  virtual void preparePredicatesForStrip() = 0;
  virtual COBlockTask::uptr buildCOTaskFromCurrentStrip(int colId) = 0;

  // methods of whole calculation framework
  virtual void preparePredicates();
  virtual void calculate(std::unordered_map<int, COBlockTaskList::uptr>* res);

 protected:
  // holding one strip's statistics
  std::unique_ptr<univplan::Statistics> stripStatistics;
  // holding one page level predicates to do actual calculation
  std::unique_ptr<MinMaxPredicatesPage> predicatesForPages;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_MINMAX_MINMAX_PREDICATES_H_
