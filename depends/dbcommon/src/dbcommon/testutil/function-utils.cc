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

#include "dbcommon/testutil/function-utils.h"

#include <algorithm>
#include <cmath>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

#include "dbcommon/type/decimal.h"
#include "dbcommon/type/type-util.h"

namespace dbcommon {

FunctionUtility::FunctionUtility(FuncKind funcid) {
  funcEnt_ = Func::instance()->getFuncEntryById(funcid);
  funcToTest_ = funcEnt_->func;
  LOG_TESTING("%s", funcEnt_->funcName.c_str());
}

FunctionUtility::~FunctionUtility() {
  auto &counter = functionUtilityMonitor_.fus[funcEnt_->funcId];
  counter.hasScalarInput |= hasScalarInput_;
  counter.hasNullScalarInput |= hasNullScalarInput_;
  counter.hasVectorInput |= hasVectorInput_;
  counter.hasVectorInputWithNull |= hasVectorInputWithNull_;
}

void FunctionUtility::test(Datum *params, uint64_t paramsSize, Datum expect,
                           int expectErrcode) {
  // Calculate number of input case and type of input param
  size_t numOfCase = 0;
  bool hasVectorInput = false;
  if (ERRCODE_SUCCESSFUL_COMPLETION != expectErrcode)
    ASSERT_EQ(Datum(0), expect);

  for (auto i = 1; i < paramsSize; i++) {
    Object *para = DatumGetValue<Object *>(params[i]);
    if (auto vec = dynamic_cast<Vector *>(para)) {
      ASSERT_TRUE(vec->getSelected() == nullptr);
      ASSERT_EQ(funcEnt_->argTypes[i - 1], vec->getTypeKind());
      hasVectorInput = true;
      hasVectorInput_ = true;
      hasVectorInputWithNull_ |= vec->hasNullValue();
      if (numOfCase == 0)
        numOfCase = vec->getNumOfRows();
      else
        ASSERT_EQ(numOfCase, vec->getNumOfRows());
    }
  }

  // Decompose Vector input into several Scalar input to generate more test on
  // scalar param input
  if (hasVectorInput) {
    LOG_TESTING("Decomposed Scalar input");
    hasScalarInput_ = true;
    std::vector<Scalar> scalars(paramsSize + 1);
    std::vector<Datum> scalarParmas(paramsSize);
    for (auto i = 0; i < paramsSize; i++)
      scalarParmas[i] = CreateDatum(&scalars[i]);

    for (auto inputIdx = 0; inputIdx < numOfCase; inputIdx++) {
      bool hasNullScalarInput = false;
      // Prepare param input
      for (auto paramIdx = 1; paramIdx < paramsSize; paramIdx++) {
        Object *para = DatumGetValue<Object *>(params[paramIdx]);
        if (auto vec = dynamic_cast<Vector *>(para)) {
          vec->readPlainScalar(inputIdx, &scalars[paramIdx]);
        } else if (auto scalar = dynamic_cast<Scalar *>(para)) {
          scalars[paramIdx] = *scalar;
        } else if (auto sel = dynamic_cast<SelectList *>(para)) {
          scalars[paramIdx].value = CreateDatum<bool>(
              std::find(sel->begin(), sel->end(), inputIdx) != sel->end());
        }
        hasNullScalarInput |= scalars[paramIdx].isnull;
      }

      // Prepare expect result
      Datum scalarExpect = CreateDatum(&scalars.back());
      Object *para = DatumGetValue<Object *>(expect);
      if (auto vec = dynamic_cast<Vector *>(para)) {
        vec->readPlainScalar(inputIdx, &scalars.back());
      } else if (auto scalar = dynamic_cast<Scalar *>(para)) {
        scalars.back() = *scalar;
      } else if (auto sel = dynamic_cast<SelectList *>(para)) {
        scalars.back().isnull = hasNullScalarInput;
        scalars.back().value = CreateDatum<bool>(
            std::find(sel->begin(), sel->end(), inputIdx) != sel->end());
      }
      hasNullScalarInput_ |= hasNullScalarInput;

      // Test
      testScalarInput(scalarParmas.data(), scalarParmas.size(), scalarExpect,
                      expectErrcode);
    }  // Loop for each decomposed scalar input
  }

  if (hasVectorInput) {
    LOG_TESTING("Vector input");
    {
      LOG_TESTING("Vector without SelectList");
      cloneInput(params, paramsSize, expect);
      testVectorInput(params_.data(), params_.size(), expect_, expectErrcode);
    }

    {
      LOG_TESTING("Vector with filter SelectList");
      applyFilterSelectList(numOfCase_ / 2);
      if (numOfCase_ / 2 > 0)
        testVectorInput(params_.data(), params_.size(), expect_, expectErrcode);
    }

    {
      LOG_TESTING("Vector with point lookup SelectList");

      for (auto selIdx = 0; selIdx < numOfCase_; selIdx++) {
        applyPointLookupSelectList(selIdx);
        testVectorInput(params_.data(), params_.size(), expect_, expectErrcode);
      }
    }

    {
      LOG_TESTING("Test Vector without null value and SelectList");
      cloneInput(params, paramsSize, expect);
      stripNulls();
      testVectorInput(params_.data(), params_.size(), expect_, expectErrcode);
    }

    {
      LOG_TESTING("Test Vector without null value but with SelectList");
      applyFilterSelectList(numOfCase_ / 2);
      if (numOfCase_ / 2 > 0)
        testVectorInput(params_.data(), params_.size(), expect_, expectErrcode);

      for (auto selIdx = 0; selIdx < numOfCase_; selIdx++) {
        applyPointLookupSelectList(selIdx);
        testVectorInput(params_.data(), params_.size(), expect_, expectErrcode);
      }
    }
  } else {
    LOG_TESTING("Scalar input");
    testScalarInput(params, paramsSize, expect, expectErrcode);
  }
}

void FunctionUtility::cloneInput(Datum *params, uint64_t paramsSize,
                                 Datum expect) {
  selsBackup_.resize(paramsSize + 1);
  vecs_.resize(paramsSize + 1);
  scalars_.resize(paramsSize + 1);
  sels_.resize(paramsSize + 1);
  params_.resize(paramsSize);

  for (auto &vec : vecs_) vec = nullptr;

  // Prepare param input
  for (auto i = 0; i < paramsSize; i++) {
    Object *para = DatumGetValue<Object *>(params[i]);
    if (auto vec = dynamic_cast<Vector *>(para)) {
      vecs_[i] = vec->clone();
      params_[i] = CreateDatum(vecs_[i].get());
    } else if (auto scalar = dynamic_cast<Scalar *>(para)) {
      scalars_[i] = *scalar;
      params_[i] = CreateDatum(&scalars_[i]);
    } else if (auto sel = dynamic_cast<SelectList *>(para)) {
      sels_[i] = *sel;
      selsBackup_[i] = *sel;
      params_[i] = CreateDatum(&sels_[i]);
    }
  }

  // Prepare expect result
  expect_ = Datum(0);
  Object *para = DatumGetValue<Object *>(expect);
  if (auto vec = dynamic_cast<Vector *>(para)) {
    vecs_.back() = vec->clone();
    expect_ = CreateDatum(vecs_.back().get());
  } else if (auto scalar = dynamic_cast<Scalar *>(para)) {
    scalars_.back() = *scalar;
    expect_ = CreateDatum(&scalars_.back());
  } else if (auto sel = dynamic_cast<SelectList *>(para)) {
    sels_.back() = *sel;
    selsBackup_.back() = *sel;
    expect_ = CreateDatum(&sels_.back());
  }

  // Record number of case
  for (auto &vec : vecs_)
    if (vec) numOfCase_ = vec->getNumOfRows();
}

// Select a number of elements
void FunctionUtility::applyFilterSelectList(size_t selCount) {
  assert(selCount <= numOfCase_);
  applySel_.resize(0);

  // Generate a random list
  std::default_random_engine generator;
  std::vector<uint16_t> tmpIdxs(numOfCase_);
  std::iota(tmpIdxs.begin(), tmpIdxs.end(), 0);
  for (auto i = 0; i < selCount; i++) {
    std::uniform_int_distribution<int> distribution(0, numOfCase_ - i - 1);
    int selIdx = distribution(generator);
    applySel_.push_back(tmpIdxs[selIdx]);
    std::swap(tmpIdxs[selIdx], tmpIdxs[numOfCase_ - i - 1]);
  }
  std::sort(applySel_.begin(), applySel_.end());

  // Update Vector
  for (auto i = 1; i < vecs_.size(); i++) {
    auto &vec = vecs_[i];
    if (vec) vec->setSelected(&applySel_, true);
  }

  // Update SelectList
  for (auto i = 0; i < sels_.size(); i++) {
    sels_[i].resize(0);
    for (auto selIdx : applySel_)
      if (std::find(selsBackup_[i].begin(), selsBackup_[i].end(), selIdx) !=
          selsBackup_[i].end()) {
        sels_[i].push_back(selIdx);
      }
  }
}

// Select a specified element
void FunctionUtility::applyPointLookupSelectList(size_t selIdx) {
  applySel_.resize(0);
  applySel_.push_back(selIdx);

  // Update Vector
  for (auto i = 1; i < vecs_.size(); i++) {
    auto &vec = vecs_[i];
    if (vec) vec->setSelected(&applySel_, true);
  }

  // Update SelectList
  for (auto i = 0; i < sels_.size(); i++) {
    if (std::find(selsBackup_[i].begin(), selsBackup_[i].end(), selIdx) !=
        selsBackup_[i].end()) {
      sels_[i].resize(0);
      sels_[i].push_back(selIdx);
    } else {
      sels_[i].resize(0);
    }
  }
}

void FunctionUtility::stripNulls() {
  SelectList notNullSel;

  // Find not null value
  for (auto inputIdx = 0; inputIdx < numOfCase_; inputIdx++) {
    bool nonull = true;
    for (auto i = 1; i < vecs_.size(); i++) {
      auto &vec = vecs_[i];
      if (vec && vec->hasNullValue()) {
        auto nulls = vec->getNulls();
        nonull &= !nulls[inputIdx];
      }
    }
    if (nonull) notNullSel.push_back(inputIdx);
  }

  // Update Vector
  for (auto i = 1; i < vecs_.size(); i++) {
    auto &vec = vecs_[i];
    if (vec) {
      vec->setSelected(&notNullSel, true);
      vec->materialize();
      vec->setHasNull(false);
    }
  }
  numOfCase_ = notNullSel.size();

  // Update SelectList
  for (auto &sel : sels_) {
    SelectList tmp;
    for (auto i = 0; i < notNullSel.size(); i++)
      if (std::find(sel.begin(), sel.end(), notNullSel[i]) != sel.end()) {
        tmp.push_back(i);
      }
    sel = tmp;
  }
  for (auto &sel : selsBackup_) {
    SelectList tmp;
    for (auto i = 0; i < notNullSel.size(); i++)
      if (std::find(sel.begin(), sel.end(), notNullSel[i]) != sel.end()) {
        tmp.push_back(i);
      }
    sel = tmp;
  }
}

void FunctionUtility::testVectorInput(Datum *params, uint64_t paramsSize,
                                      Datum expect, int expectErrcode) {
  if (dynamic_cast<Vector *>(DatumGetValue<Object *>(expect))) {
    // Case1. Test Vector result
    Vector *vecRet = dynamic_cast<Vector *>(DatumGetValue<Object *>(params[0]));
    Vector *vecExpect = dynamic_cast<Vector *>(DatumGetValue<Object *>(expect));
    ASSERT_TRUE(vecRet != nullptr);
    ASSERT_TRUE(vecExpect != nullptr);

    funcToTest_(params, paramsSize);
    EXPECT_EQ(vecExpect->toString(), vecRet->toString());
  } else if (dynamic_cast<SelectList *>(DatumGetValue<Object *>(expect))) {
    // Case2. Test SelectList result
    SelectList *selRet =
        dynamic_cast<SelectList *>(DatumGetValue<Object *>(params[0]));
    SelectList *selExpect =
        dynamic_cast<SelectList *>(DatumGetValue<Object *>(expect));
    ASSERT_TRUE(selRet != nullptr);
    ASSERT_TRUE(selExpect != nullptr);

    funcToTest_(params, paramsSize);
    ASSERT_EQ(selExpect->size(), selRet->size());
    ASSERT_EQ(0, memcmp(reinterpret_cast<char *>(selExpect->begin()),
                        reinterpret_cast<char *>(selRet->begin()),
                        selExpect->size() * sizeof(SelectList::value_type)));
  } else {
    // Case3. Test TransactionAbortException
    try {
      funcToTest_(params, paramsSize);
    } catch (TransactionAbortException &e) {
      ASSERT_EQ(expectErrcode, e._errCode);
      return;
    }
    if (ERRCODE_SUCCESSFUL_COMPLETION != expectErrcode) ASSERT_TRUE(false);
  }
}

void FunctionUtility::testScalarInput(Datum *params, uint64_t paramsSize,
                                      Datum expect, int expectErrcode) {
  try {
    funcToTest_(params, paramsSize);
  } catch (TransactionAbortException &e) {
    ASSERT_EQ(expectErrcode, e._errCode);
    return;
  }
  if (ERRCODE_SUCCESSFUL_COMPLETION != expectErrcode) ASSERT_TRUE(false);

  Scalar *scalarRet =
      dynamic_cast<Scalar *>(DatumGetValue<Object *>(params[0]));
  Scalar *scalarExpect =
      dynamic_cast<Scalar *>(DatumGetValue<Object *>(expect));
  ASSERT_TRUE(scalarRet != nullptr);
  ASSERT_TRUE(scalarExpect != nullptr);

  const TypeEntry *typeEnt =
      TypeUtil::instance()->getTypeEntryById(funcEnt_->retType);
  if (scalarExpect->isnull) {  // null value
    ASSERT_TRUE(scalarRet->isnull);
  } else if (TypeKind::TIMESTAMPID == typeEnt->id ||
             VAR_TYPE_LENGTH ==
                 typeEnt->typeLength) {  // value with extra memory
    ASSERT_EQ(scalarExpect->isnull, scalarRet->isnull);
    ASSERT_EQ(scalarExpect->length, scalarRet->length);
    ASSERT_EQ(0, memcmp(DatumGetValue<char *>(scalarExpect->value),
                        DatumGetValue<char *>(scalarRet->value),
                        scalarExpect->length));
  } else if (TypeKind::DECIMALNEWID == typeEnt->id) {
    DecimalType t;
    ASSERT_LT(
        abs(stod(t.toString(*static_cast<DecimalVar *>(scalarExpect->value))) -
            stod(t.toString(*static_cast<DecimalVar *>(scalarRet->value)))),
        0.01);
  } else if (TypeKind::DOUBLEID == typeEnt->id) {
    ASSERT_EQ(scalarExpect->isnull, scalarRet->isnull);
    ASSERT_EQ(scalarExpect->length, scalarRet->length);
    ASSERT_DOUBLE_EQ(scalarExpect->value.value.i64, scalarRet->value.value.i64);
  } else if (TypeKind::FLOATID == typeEnt->id) {
    ASSERT_EQ(scalarExpect->isnull, scalarRet->isnull);
    ASSERT_EQ(scalarExpect->length, scalarRet->length);
    ASSERT_FLOAT_EQ(scalarExpect->value.value.i64, scalarRet->value.value.i64);
  } else {  // value without extra memory
    ASSERT_EQ(scalarExpect->isnull, scalarRet->isnull);
    ASSERT_EQ(scalarExpect->length, scalarRet->length);
    switch (typeEnt->typeLength) {
      case 8:
        ASSERT_EQ(scalarExpect->value.value.i64, scalarRet->value.value.i64);
        break;
      case 4:
        ASSERT_EQ(scalarExpect->value.value.i32, scalarRet->value.value.i32);
        break;
      case 2:
        ASSERT_EQ(scalarExpect->value.value.i16, scalarRet->value.value.i16);
        break;
      case 1:
        ASSERT_EQ(scalarExpect->value.value.i8, scalarRet->value.value.i8);
        break;
    }
  }
}

FunctionUtilityMonitor FunctionUtility::functionUtilityMonitor_;

FunctionUtilityMonitor::~FunctionUtilityMonitor() {
  for (auto &ent : fus) {
    auto funcEnt = Func::instance()->getFuncEntryById(ent.first);
    auto status = ent.second;
    EXPECT_TRUE(status.hasScalarInput
                    ? testing::AssertionSuccess()
                    : testing::AssertionFailure()
                          << "Did not check scalar input param for "
                          << funcEnt->funcName);
    EXPECT_TRUE(status.hasVectorInput
                    ? testing::AssertionSuccess()
                    : testing::AssertionFailure()
                          << "Did not check vector input param for "
                          << funcEnt->funcName);
    EXPECT_TRUE(status.hasNullScalarInput
                    ? testing::AssertionSuccess()
                    : testing::AssertionFailure()
                          << "Did not check NULL scalar input param for "
                          << funcEnt->funcName);
    EXPECT_TRUE(status.hasVectorInputWithNull
                    ? testing::AssertionSuccess()
                    : testing::AssertionFailure()
                          << "Did not check vector input param with NULL for "
                          << funcEnt->funcName);
  }
}

TEST_P(TestFunction, Test) {
  TestFunctionEntry ent = GetParam();
  FunctionUtility fu(ent.funcid);

  auto getDatum = [&](std::string s, int argth) -> Datum {
    if (s.find("Vector: ") != std::string::npos && s.find("Vector: ") == 0) {
      // input as "Vector: val1 val2"
      return fu.generatePara<Vector>(argth, s.substr(8));
    } else if (s.find("Scalar: ") != std::string::npos &&
               s.find("Scalar: ") == 0) {
      // input as "Scalar: val"
      return fu.generatePara<Scalar>(argth, s.substr(8));
    } else if (s.find("Vector{delimiter=") != std::string::npos &&
               s.find("Vector{delimiter=") == 0 && s[18] == '}' &&
               s[19] == ':' && s[20] == ' ') {
      // input as "Vector{delimiter=,}: val1,val2"
      return fu.generatePara<Vector>(argth, s.substr(21), s[17]);
    } else if (s == "Error") {
      // test the errcode
      return CreateDatum(0);
    } else {
      assert(false &&
             "May not support invalid input form for TestFunctionEntry");
    }
  };

  Datum expect = getDatum(ent.expectRet, 0);

  auto ret = fu.generatePara<0, Vector>("NULL");
  std::vector<Datum> params{ret};
  for (int i = 0; i < ent.args.size(); i++) {
    params.push_back(getDatum(ent.args[i], i + 1));
  }

  fu.test(params.data(), params.size(), expect, ent.expectErrcode);
}

}  // namespace dbcommon
