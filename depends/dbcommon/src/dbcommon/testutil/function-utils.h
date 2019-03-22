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

#ifndef DBCOMMON_SRC_DBCOMMON_TESTUTIL_FUNCTION_UTILS_H_
#define DBCOMMON_SRC_DBCOMMON_TESTUTIL_FUNCTION_UTILS_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "dbcommon/common/vector.h"
#include "dbcommon/function/func-kind.cg.h"
#include "dbcommon/function/func.h"
#include "dbcommon/log/debug-logger.h"
#include "dbcommon/nodes/datum.h"
#include "dbcommon/nodes/select-list.h"
#include "dbcommon/testutil/scalar-utils.h"
#include "dbcommon/testutil/vector-utils.h"
#include "gtest/gtest.h"

namespace dbcommon {

// FunctionUtility intends to setup a testing framework for dbcommon::func_type.
//
// To complete a new dbcommon::func_type, whether input param is Scalar or
// Vector, whether Vector param is with SelectList and whether Vector param
// contains null value should be taken into account, which is really a tedious
// work.
//
// Therefore we introduce FunctionUtility, which checks the common control
// logic of dbcommon::func_type, to free your hand from preparing Scalar input
// and applying SelectList/null values to Vector input.
// The common user case is to generate parameters and expect using ScalarUtility
// and VectorUtiliy. And then call FunctionUtility::test to check the function.
// Refer test-string-function.cc as a good example.

struct FunctionUtilityMonitor;
class FunctionUtility {
  explicit FunctionUtility(FuncKind funcid);

  ~FunctionUtility();

  // Generate parameter for FunctionUtility::test
  //
  // @param parameterIdx stands for the exact parameter position in
  // dbcommon::Function' params, as a result of which, 0 stands for the return
  // value of the SQL output.
  // @param Type
  template <int parameterIdx, typename Type>
  Datum generatePara(const std::string &str, char delimiter = ' ') {
    assert(parameterIdx >= 0 && parameterIdx <= funcEnt_->argTypes.size());
    if (std::is_same<Type, dbcommon::Vector>::value) {
      if (parameterIdx == 0)
        paraVus_.emplace_back(funcEnt_->retType);
      else
        paraVus_.emplace_back(funcEnt_->argTypes[parameterIdx - 1]);
      paraVectors_.emplace_back(paraVus_.back().generateVector(str, delimiter));
      if (paraVus_.back().getTypeKind() == TypeKind::BOOLEANID &&
          parameterIdx == 0) {
        std::unique_ptr<SelectList> sel(new SelectList);
        sel->fromVector(paraVectors_.back().get());
        paraSelectList_.push_back(std::move(sel));
        return CreateDatum(paraSelectList_.back().get());
      }
      return CreateDatum(paraVectors_.back().get());
    }
    if (std::is_same<Type, dbcommon::Scalar>::value) {
      if (parameterIdx == 0)
        paraSus_.emplace_back(funcEnt_->retType);
      else
        paraSus_.emplace_back(funcEnt_->argTypes[parameterIdx - 1]);
      paraScalars_.emplace_back(
          new Scalar(paraSus_.back().generateScalar(str)));
      return CreateDatum(paraScalars_.back().get());
    }
    assert("May not generate parameter other than Vector or Scalar" && false);
    return CreateDatum(0);
  }

  template <typename Type>
  Datum generatePara(int parameterIdx, const std::string &str,
                     char delimiter = ' ') {
    assert(parameterIdx < 5);
    switch (parameterIdx) {
      case 0:
        return generatePara<0, Type>(str, delimiter);
      case 1:
        return generatePara<1, Type>(str, delimiter);
      case 2:
        return generatePara<2, Type>(str, delimiter);
      case 3:
        return generatePara<3, Type>(str, delimiter);
    }
    return CreateDatum(0);
  }

  // FunctionUtility::test automatically generate several kinds of test case:
  // 1. Decompose Vector input into Scalar input
  // 2. Convert Vector input to
  //    2.1 Vector input without both SelectList and NullValue
  //    2.2 Vector input without SelectList but with NullValue
  //    2.3 Vector input with SelectList but without NullValue
  //    2.4 Vector input with both SelectList and NullValue
  //
  // When testing TransactionAbortException, all the test cases should be
  // exception cases.
  //
  // @param[in] params
  // @param[in] paramsSize
  // @param[in] expect
  // @param[in] expectErrcode
  void test(Datum *params, uint64_t paramsSize, Datum expect,
            int expectErrcode = ERRCODE_SUCCESSFUL_COMPLETION);

 private:
  void cloneInput(Datum *params, uint64_t paramsSize, Datum expect);

  // Select a number of elements
  void applyFilterSelectList(size_t selCount);

  // Select a specified element
  void applyPointLookupSelectList(size_t selIdx);

  void stripNulls();

  void testVectorInput(Datum *params, uint64_t paramsSize, Datum expect,
                       int expectErrcode);

  void testScalarInput(Datum *params, uint64_t paramsSize, Datum expect,
                       int expectErrcode);

  const dbcommon::FuncEntry *funcEnt_;
  dbcommon::func_type funcToTest_;

  bool hasVectorInput_ = false;
  bool hasVectorInputWithNull_ = false;
  bool hasNullScalarInput_ = false;
  bool hasScalarInput_ = false;

  // buffer for clone input
  size_t numOfCase_ = 0;  // number of function test case
  std::vector<SelectList> selsBackup_;
  std::vector<std::unique_ptr<Vector>> vecs_;
  std::vector<SelectList> sels_;
  std::vector<Scalar> scalars_;
  SelectList applySel_;
  std::vector<Datum> params_;
  Datum expect_;

  // buffer for generated parameter
  std::vector<std::unique_ptr<Scalar>> paraScalars_;
  std::vector<std::unique_ptr<Vector>> paraVectors_;
  std::vector<std::unique_ptr<SelectList>> paraSelectList_;
  std::vector<ScalarUtility> paraSus_;
  std::vector<VectorUtility> paraVus_;

  static FunctionUtilityMonitor functionUtilityMonitor_;
};

struct FunctionUtilityMonitorUnit {
  bool hasVectorInput = false;
  bool hasVectorInputWithNull = false;
  bool hasNullScalarInput = false;
  bool hasScalarInput = false;
};
struct FunctionUtilityMonitor {
  ~FunctionUtilityMonitor();
  std::map<FuncKind, FunctionUtilityMonitorUnit> fus;
};

// TestFunctionEntry works as a test case constructor for dbcommon::Function.
//
// It has 3 attributes(i.e. funcid, expectRet and args) corresponding to
// dbcommon::FuncEntry's .funcId, .retType and .argTypes. Its .expectErrcode
// field somehow serves as a extra specification of expected return values.
//
// When run as a test case, TestFunctionEntry converts its std::string
// representation into real raw data according to the type info that retrieved
// from dbcommon::FuncEntry. It classified the std::string representation input
// into three categories.
// 1. Vector
//     "Vector: val1 val2", using space character as default value separator
//     "Vector{delimiter=X}: val1Xval2", using character 'X' as value separator
// 2. Scalar
//     "Scalar: val", scalar value
// 3. Error
//     "Error", used in .expectRet only when testing exception case, which comes
//              along with extra .expectErrcode
//
struct TestFunctionEntry {
  TestFunctionEntry(FuncKind funcid, std::string expectRet,
                    std::vector<std::string> args,
                    int expectErrcode = ERRCODE_SUCCESSFUL_COMPLETION)
      : funcid(funcid),
        expectRet(expectRet),
        args(args),
        expectErrcode(expectErrcode) {}
  FuncKind funcid = FuncKind::FUNCINVALID;
  std::string expectRet;
  std::vector<std::string> args;
  int expectErrcode = ERRCODE_SUCCESSFUL_COMPLETION;
};

class TestFunction : public ::testing::TestWithParam<TestFunctionEntry> {};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_TESTUTIL_FUNCTION_UTILS_H_
