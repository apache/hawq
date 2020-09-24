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

#include "gtest/gtest.h"

#include "dbcommon/function/agg-func.h"
#include "dbcommon/function/arith-cmp-func.cg.h"
#include "dbcommon/testutil/agg-func-utils.h"
#include "dbcommon/testutil/tuple-batch-utils.h"
#include "dbcommon/testutil/vector-utils.h"

namespace dbcommon {

TEST_F(AggFuncTest, count_star) {
  std::vector<Datum> params(4);
  {
    std::vector<int64_t> initAggGrpVals = {2, 3, 2, 3};
    std::unique_ptr<AggGroupValues> grpVals =
        generateAggGroupValues<int64_t>(initAggGrpVals, false, false);
    std::vector<uint64_t> hashGroups = {0, 1, 0, 0};

    params[0] = CreateDatum(grpVals.get());
    params[2] = CreateDatum<const std::vector<uint64_t> *>(&hashGroups);
    params[3] = CreateDatum<bool>(true);

    count_star(params.data(), 4);
    checkAcc<int64_t>({5, 4, 2, 3}, grpVals.get());
  }
}

TEST_F(AggFuncTest, count_inc) {
  func_type testFunc = count_inc;
  std::vector<int64_t> initAggGrpVals = {10, 10, 233, 233};
  std::unique_ptr<AggGroupValues> grpVals;

  std::vector<int64_t> vals = {1, 1, 2, 1};
  std::vector<bool> nulls = {false, false, false, true};
  SelectList sel = {0, 2};

  {
    LOG_INFO("Testing without seleletList");
    std::unique_ptr<Vector> vec = VectorUtility::generateSelectVector<int64_t>(
        BIGINTID, vals, &nulls, nullptr);
    grpVals = generateAggGroupValues<int64_t>(initAggGrpVals, false, true);
    std::vector<uint64_t> hashGroups = {0, 0, 1, 0};

    callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
    checkAcc<int64_t>({12, 11, 233, 233}, grpVals.get());

    LOG_INFO("Testing has no nulls");
    vec->setHasNull(false);
    callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
    checkAcc<int64_t>({15, 12, 233, 233}, grpVals.get());
  }

  {
    LOG_INFO("Testing with seleletList");
    std::unique_ptr<Vector> vec = VectorUtility::generateSelectVector<int64_t>(
        BIGINTID, vals, &nulls, nullptr);
    vec->setSelected(&sel, false);
    grpVals = generateAggGroupValues<int64_t>(initAggGrpVals, false, true);
    std::vector<uint64_t> hashGroups = {0, 1};

    callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
    checkAcc<int64_t>({11, 11, 233, 233}, grpVals.get());

    LOG_INFO("Testing has no nulls");
    vec->setHasNull(false);
    callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
    checkAcc<int64_t>({12, 12, 233, 233}, grpVals.get());
  }
}

TEST_F(AggFuncTest, count_add) {
  func_type testFunc = count_add;
  std::vector<int64_t> initAggGrpVals = {0, 0, 233, 233};

  std::vector<int64_t> vals = {9, 1, 2, 4};
  SelectList sel = {0, 2, 3};

  {
    LOG_INFO("Testing with seleletList");
    std::unique_ptr<AggGroupValues> grpVals =
        generateAggGroupValues<int64_t>(initAggGrpVals, false, true);
    std::vector<uint64_t> hashGroups = {0, 1, 1};
    std::unique_ptr<Vector> vec = VectorUtility::generateSelectVector<int64_t>(
        BIGINTID, vals, nullptr, &sel);

    callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
    checkAcc<int64_t>({9, 6, 233, 233}, grpVals.get());
  }
  {
    LOG_INFO("Testing without seleletList");
    std::vector<uint64_t> hashGroups = {0, 0, 1, 0};
    std::unique_ptr<AggGroupValues> grpVals =
        generateAggGroupValues<int64_t>(initAggGrpVals, false, true);
    std::unique_ptr<Vector> vec = VectorUtility::generateSelectVector<int64_t>(
        BIGINTID, vals, nullptr, nullptr);

    callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
    checkAcc<int64_t>({14, 2, 233, 233}, grpVals.get());
  }
}

template <class T, TypeKind TK, class RT>
void AggFuncTest::testSum(func_type testFunc) {
  std::vector<RT> initAggGrpVals = {0, 0, 233, 233};
  std::unique_ptr<AggGroupValues> grpVals;

  std::vector<T> vals = {9, 1, 2, 4};
  std::vector<bool> nulls = {true, false, true, false};
  SelectList sel = {0, 2, 3};

  std::unique_ptr<Vector> vec;

  {
    LOG_INFO("Testing with seleletList");
    grpVals = generateAggGroupValues<RT>(initAggGrpVals, false, true);
    std::vector<uint64_t> hashGroups = {0, 1, 1};

    LOG_INFO("Testing has no nulls");
    vec = VectorUtility::generateSelectVector<T>(TK, vals, nullptr, &sel);
    callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
    checkAcc<RT>({9, 6, 233, 233}, grpVals.get());

    LOG_INFO("Testing has nulls");
    vec = VectorUtility::generateSelectVector<T>(TK, vals, &nulls, &sel);
    callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
    checkAcc<RT>({9, 10, 233, 233}, grpVals.get());
  }

  {
    LOG_INFO("Testing without seleletList");
    std::vector<uint64_t> hashGroups = {0, 0, 1, 0};
    grpVals = generateAggGroupValues<RT>(initAggGrpVals, false, true);
    vec = VectorUtility::generateSelectVector<T>(TK, vals, nullptr, nullptr);

    LOG_INFO("Testing has no nulls");
    callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
    checkAcc<RT>({14, 2, 233, 233}, grpVals.get());

    LOG_INFO("Testing has nulls");
    vec = VectorUtility::generateSelectVector<T>(TK, vals, &nulls, nullptr);
    callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
    checkAcc<RT>({19, 2, 233, 233}, grpVals.get());
  }
}

TEST_F(AggFuncTest, Sum) {
  testSum<int8_t, TINYINTID, int64_t>(sum_int8_add);
  testSum<int64_t, BIGINTID, int64_t>(sum_int8_sum);
  testSum<int16_t, SMALLINTID, int64_t>(sum_int16_add);
  testSum<int64_t, BIGINTID, int64_t>(sum_int16_sum);
  testSum<int32_t, INTID, int64_t>(sum_int32_add);
  testSum<int64_t, BIGINTID, int64_t>(sum_int32_sum);
  testSum<int64_t, BIGINTID, int64_t>(sum_int64_add);
  testSum<int64_t, BIGINTID, int64_t>(sum_int64_sum);
  testSum<float, FLOATID, double>(sum_float_add);
  testSum<double, DOUBLEID, double>(sum_float_sum);
  testSum<double, DOUBLEID, double>(sum_double_add);
  testSum<double, DOUBLEID, double>(sum_double_sum);
}

template <class T, TypeKind TK>
void AggFuncTest::testAvgAccu(func_type testFunc) {
  std::vector<T> initGrpVals = {0, 0, 0, 0};
  std::unique_ptr<AggGroupValues> grpVals;

  std::vector<T> vals = {6, 2, 4, 5, 7};
  std::vector<bool> nulls = {false, true, false, false, true};
  {
    LOG_INFO("Testing first stage avg");
    std::unique_ptr<Vector> vec;
    {
      LOG_INFO("Testing with seleletList");
      std::vector<uint64_t> hashGroups = {0, 1, 1};
      SelectList sel = {0, 3, 4};
      grpVals = generateAggGroupValues<T>(initGrpVals, true, true);
      LOG_INFO("Testing has no nulls");
      vec = VectorUtility::generateSelectVector<T>(TK, vals, nullptr, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      checkAvg({{6, 1}, {12, 2}, {0, 0}, {0, 0}}, grpVals.get());
      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectVector<T>(TK, vals, &nulls, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      checkAvg({{12, 2}, {17, 3}, {0, 0}, {0, 0}}, grpVals.get());
    }
    {
      LOG_INFO("Testing without seleletList");
      grpVals = generateAggGroupValues<T>(initGrpVals, true, true);
      LOG_INFO("Testing has no nulls");
      std::vector<uint64_t> hashGroups = {0, 0, 1, 1, 1};
      vec = VectorUtility::generateSelectVector<T>(TK, vals, nullptr, nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      checkAvg({{8, 2}, {16, 3}, {0, 0}, {0, 0}}, grpVals.get());

      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectVector<T>(TK, vals, &nulls, nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      checkAvg({{14, 3}, {25, 5}, {0, 0}, {0, 0}}, grpVals.get());
    }
  }
}
TEST_F(AggFuncTest, AvgAccumulate) {
  testAvgAccu<int8_t, TINYINTID>(avg_int8_accu);
  testAvgAccu<int16_t, SMALLINTID>(avg_int16_accu);
  testAvgAccu<int32_t, INTID>(avg_int32_accu);
  testAvgAccu<int64_t, BIGINTID>(avg_int64_accu);
  testAvgAccu<float, FLOATID>(avg_float_accu);
  testAvgAccu<double, DOUBLEID>(avg_double_accu);
}

template <class T, TypeKind TK>
void AggFuncTest::testAvgAmalg(func_type testFunc) {
  std::vector<T> initGrpVals = {0, 0, 0, 0};
  std::unique_ptr<AggGroupValues> grpVals;

  {
    LOG_INFO("Testing second stage avg");
    std::vector<double> vals = {6, 2, 4, 5, 7};
    std::vector<uint64_t> counts = {1, 1, 1, 1, 1};
    std::vector<bool> nulls = {false, true, false, false, true};
    {
      LOG_INFO("Testing without seleletList");
      std::unique_ptr<Vector> vecSum, vecCount;
      vecSum = VectorUtility::generateSelectVector<double>(
          TypeKind::DECIMALNEWID == TK ? DECIMALNEWID : DOUBLEID, vals, &nulls,
          nullptr);
      vecCount = VectorUtility::generateSelectVector<uint64_t>(BIGINTID, counts,
                                                               &nulls, nullptr);
      std::vector<std::unique_ptr<Vector>> vecs;
      vecs.push_back(std::move(vecSum));
      vecs.push_back(std::move(vecCount));
      std::unique_ptr<dbcommon::Vector> vec =
          VectorUtility::generateSelectStructVector(
              vecs, nullptr, nullptr,
              (TypeKind::DECIMALNEWID == TK ? AVG_DECIMAL_TRANS_DATA_ID
                                            : AVG_DOUBLE_TRANS_DATA_ID));

      grpVals = generateAggGroupValues<T>(initGrpVals, true, true);
      std::vector<uint64_t> hashGroups = {0, 0, 1, 1, 1};

      LOG_INFO("Testing has nulls");
      vec->getChildVector(0)->setHasNull(true);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      checkAvg({{6, 1}, {9, 2}, {0, 0}, {0, 0}}, grpVals.get());

      LOG_INFO("Testing has no nulls");
      std::vector<bool> nonulls(nulls.size(), false);
      vecSum = VectorUtility::generateSelectVector<double>(
          TypeKind::DECIMALNEWID == TK ? DECIMALNEWID : DOUBLEID, vals,
          &nonulls, nullptr);
      vecCount = VectorUtility::generateSelectVector<uint64_t>(
          BIGINTID, counts, &nonulls, nullptr);
      vec->childs[0] = std::move(vecSum);
      vec->childs[1] = std::move(vecCount);
      vec->getChildVector(0)->setHasNull(false);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      checkAvg({{14, 3}, {25, 5}, {0, 0}, {0, 0}}, grpVals.get());
    }
    {
      LOG_INFO("Testing with seleletList");
      std::unique_ptr<Vector> vecSum, vecCount;
      vecSum = VectorUtility::generateSelectVector<double>(
          TypeKind::DECIMALNEWID == TK ? DECIMALNEWID : DOUBLEID, vals, &nulls,
          nullptr);
      vecCount = VectorUtility::generateSelectVector<uint64_t>(BIGINTID, counts,
                                                               &nulls, nullptr);
      std::vector<std::unique_ptr<Vector>> vecs;
      vecs.push_back(std::move(vecSum));
      vecs.push_back(std::move(vecCount));
      std::unique_ptr<dbcommon::Vector> vec =
          VectorUtility::generateSelectStructVector(
              vecs, nullptr, nullptr,
              (TypeKind::DECIMALNEWID == TK ? AVG_DECIMAL_TRANS_DATA_ID
                                            : AVG_DOUBLE_TRANS_DATA_ID));

      std::vector<uint64_t> hashGroups = {0, 1, 1};
      SelectList sel = {0, 3, 4};
      vec->setSelected(&sel, false);
      grpVals = generateAggGroupValues<T>(initGrpVals, true, true);

      LOG_INFO("Testing has nulls");
      vec->getChildVector(0)->setHasNull(true);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      checkAvg({{6, 1}, {5, 1}, {0, 0}, {0, 0}}, grpVals.get());

      LOG_INFO("Testing has no nulls");
      std::vector<bool> nonulls(nulls.size(), false);
      vecSum = VectorUtility::generateSelectVector<double>(
          TypeKind::DECIMALNEWID == TK ? DECIMALNEWID : DOUBLEID, vals,
          &nonulls, nullptr);
      vecCount = VectorUtility::generateSelectVector<uint64_t>(
          BIGINTID, counts, &nonulls, nullptr);
      vec->childs[0] = std::move(vecSum);
      vec->childs[1] = std::move(vecCount);
      vec->setSelected(&sel, false);
      vec->getChildVector(0)->setHasNull(false);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      checkAvg({{12, 2}, {17, 3}, {0, 0}, {0, 0}}, grpVals.get());
    }
  }
}
TEST_F(AggFuncTest, AvgAmalgamate) {
  testAvgAmalg<int8_t, TINYINTID>(avg_int8_amalg);
  testAvgAmalg<int16_t, SMALLINTID>(avg_int16_amalg);
  testAvgAmalg<int32_t, INTID>(avg_int32_amalg);
  testAvgAmalg<int64_t, BIGINTID>(avg_int64_amalg);
  testAvgAmalg<float, FLOATID>(avg_float_amalg);
  testAvgAmalg<double, DOUBLEID>(avg_double_amalg);
}

TEST_F(AggFuncTest, Scalar) {
  std::vector<Datum> params(5);
  Scalar scalar(CreateDatum<int64_t>(3));
  std::vector<int64_t> initGrpVals = {0, 0};
  std::unique_ptr<AggGroupValues> grpVals;
  std::vector<uint64_t> hashGroups = {0, 1, 0};
  params[2] = CreateDatum<const std::vector<uint64_t> *>(&hashGroups);
  params[3] = CreateDatum<bool>(true);
  params[4] = CreateDatum<Scalar *>(&scalar);

  {
    grpVals = generateAggGroupValues<int64_t>(initGrpVals, false, true);
    params[0] = CreateDatum(grpVals.get());

    LOG_INFO("Testing COUNT(SCALAR)");
    count_inc(params.data(), 5);
    checkAcc<int64_t>({2, 1}, grpVals.get());

    LOG_INFO("Testing SUM(SCALAR)");
    sum_int64_add(params.data(), 5);
    checkAcc<int64_t>({8, 4}, grpVals.get());
  }

  {
    LOG_INFO("Testing AVG(SCALAR)");
    grpVals = generateAggGroupValues<int64_t>(initGrpVals, true, true);
    params[0] = CreateDatum(grpVals.get());

    avg_int64_accu(params.data(), 5);
    checkAvg({{6, 2}, {3, 1}}, grpVals.get());
  }
}
TEST_F(AggFuncTest, Null) {
  std::vector<uint64_t> hashGroups = {0, 1, 2, 3, 4};
  std::unique_ptr<AggGroupValues> grpVals;
  std::vector<int64_t> vals = {6, 2, 4, 5, 7};
  std::vector<bool> nulls = {false, true, false, false, true};
  std::unique_ptr<Vector> vec = VectorUtility::generateSelectVector<int64_t>(
      BIGINTID, vals, &nulls, nullptr);
  {  // !isAvg
    grpVals = generateAggGroupValues<uint64_t>(hashGroups, false, true);
    callFunc(sum_int64_add, grpVals.get(), &hashGroups, true, vec.get());
    checkNull(nulls, grpVals.get(), false);

    grpVals = generateAggGroupValues<uint64_t>(hashGroups, false, true);
    callFunc(sum_int64_sum, grpVals.get(), &hashGroups, true, vec.get());
    checkNull(nulls, grpVals.get(), false);
  }
}
template <class T, TypeKind TK>
void AggFuncTest::testMinMax(func_type testFunc, bool testMin) {
  std::vector<T> initGrpVals = {0, 0, 23, 23};
  std::unique_ptr<AggGroupValues> grpVals;
  std::vector<T> vals = {6, 2, 4, 5, 7};
  std::vector<bool> nulls = {false, true, false, true, false};

  {  // small scale
    std::unique_ptr<Vector> vec;
    {  // without vec but with scalar
      std::unique_ptr<Scalar> scalar = generateScalar<T>(TK, 1);
      std::vector<uint64_t> hashGroups = {0, 0, 1, 1, 1};
      grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
      callFuncScalar(testFunc, grpVals.get(), &hashGroups, true, scalar.get());
      checkAcc<T>({1, 1, 23, 23}, grpVals.get());
    }
    {
      LOG_INFO("Testing with seleletList");
      std::vector<uint64_t> hashGroups = {0, 0, 1, 1};
      SelectList sel = {0, 2, 3, 4};
      grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectVector<T>(TK, vals, &nulls, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      if (testMin)
        checkAcc<T>({4, 7, 23, 23}, grpVals.get());
      else
        checkAcc<T>({6, 7, 23, 23}, grpVals.get());

      grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
      LOG_INFO("Testing has no nulls");
      vec = VectorUtility::generateSelectVector<T>(TK, vals, nullptr, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      if (testMin)
        checkAcc<T>({4, 5, 23, 23}, grpVals.get());
      else
        checkAcc<T>({6, 7, 23, 23}, grpVals.get());

      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectVector<T>(TK, vals, &nulls, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      if (testMin)
        checkAcc<T>({4, 5, 23, 23}, grpVals.get());
      else
        checkAcc<T>({6, 7, 23, 23}, grpVals.get());
    }
    {
      LOG_INFO("Testing without seleletList");
      grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
      std::vector<uint64_t> hashGroups = {0, 0, 1, 1, 1};

      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectVector<T>(TK, vals, &nulls, nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      if (testMin)
        checkAcc<T>({6, 4, 23, 23}, grpVals.get());
      else
        checkAcc<T>({6, 7, 23, 23}, grpVals.get());

      grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
      LOG_INFO("Testing has no nulls");
      vec = VectorUtility::generateSelectVector<T>(TK, vals, nullptr, nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      if (testMin)
        checkAcc<T>({2, 4, 23, 23}, grpVals.get());
      else
        checkAcc<T>({6, 7, 23, 23}, grpVals.get());

      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectVector<T>(TK, vals, &nulls, nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      if (testMin)
        checkAcc<T>({2, 4, 23, 23}, grpVals.get());
      else
        checkAcc<T>({6, 7, 23, 23}, grpVals.get());
    }
  }
}
template <>
void AggFuncTest::testMinMax<std::string, STRINGID>(func_type testFunc,
                                                    bool testMin) {
  typedef std::string T;
  std::vector<T> initGrpVals = {"0", "0", "233", "233"};
  std::unique_ptr<AggGroupValues> grpVals;
  std::vector<T> vals = {"26", "2", "1", "25", "27"};
  std::vector<bool> nulls = {false, true, false, true, false};

  {  // small scale
    std::unique_ptr<Vector> vec;
    {  // without vec but with scalar
      LOG_INFO("Testing scalar");
      std::unique_ptr<Scalar> scalar = generateScalar<T>("1");
      std::vector<uint64_t> hashGroups = {0, 0, 1, 1, 1};
      grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
      callFuncScalar(testFunc, grpVals.get(), &hashGroups, true, scalar.get());
      checkAccOnString({"1", "1", "233", "233"}, grpVals.get());
    }
    {
      LOG_INFO("Testing with seleletList");
      LOG_INFO("Testing select list");
      std::vector<uint64_t> hashGroups = {0, 0, 1, 1};
      SelectList sel = {0, 2, 3, 4};
      grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
      LOG_INFO("Testing has nulls");
      vec =
          VectorUtility::generateSelectVector<T>(STRINGID, vals, &nulls, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      if (testMin)
        checkAccOnString({"1", "27", "233", "233"}, grpVals.get());
      else
        checkAccOnString({"26", "27", "233", "233"}, grpVals.get());

      grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
      LOG_INFO("Testing has no nulls");
      vec =
          VectorUtility::generateSelectVector<T>(STRINGID, vals, nullptr, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      if (testMin)
        checkAccOnString({"1", "25", "233", "233"}, grpVals.get());
      else
        checkAccOnString({"26", "27", "233", "233"}, grpVals.get());

      LOG_INFO("Testing has nulls");
      vec =
          VectorUtility::generateSelectVector<T>(STRINGID, vals, &nulls, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      if (testMin)
        checkAccOnString({"1", "25", "233", "233"}, grpVals.get());
      else
        checkAccOnString({"26", "27", "233", "233"}, grpVals.get());
    }
    {
      LOG_INFO("Testing without seleletList");
      grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
      std::vector<uint64_t> hashGroups = {0, 0, 1, 1, 1};

      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectVector<T>(STRINGID, vals, &nulls,
                                                   nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      if (testMin)
        checkAccOnString({"26", "1", "233", "233"}, grpVals.get());
      else
        checkAccOnString({"26", "27", "233", "233"}, grpVals.get());

      grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
      LOG_INFO("Testing has no nulls");
      vec = VectorUtility::generateSelectVector<T>(STRINGID, vals, nullptr,
                                                   nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      if (testMin)
        checkAccOnString({"2", "1", "233", "233"}, grpVals.get());
      else
        checkAccOnString({"26", "27", "233", "233"}, grpVals.get());

      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectVector<T>(STRINGID, vals, &nulls,
                                                   nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      if (testMin)
        checkAccOnString({"2", "1", "233", "233"}, grpVals.get());
      else
        checkAccOnString({"26", "27", "233", "233"}, grpVals.get());
    }
  }
}
template <>
void AggFuncTest::testMinMax<Timestamp, TIMESTAMPID>(func_type testFunc,
                                                     bool testMin) {
  typedef Timestamp T;
  std::vector<std::string> initGrpValStrs = {
      "1970-01-01 00:00:00", "1970-01-01 00:00:00", "2018-01-20 11:04:32",
      "2018-01-20 11:04:32.1"};
  std::vector<T> initGrpVals = {{0, 0}, {0, 0}, {0, 0}, {0, 0}};
  std::unique_ptr<AggGroupValues> grpVals;
  std::vector<std::string> valStrs = {
      "2018-01-19 17:25:10", "2018-01-19 17:25:10.1123",
      "2018-01-19 17:25:10 BC", "2018-01-19 17:25:10.1123 BC",
      "2018-01-19 17:25:10.22"};
  std::vector<T> vals = {{0, 0}, {0, 0}, {0, 0}, {0, 0}, {0, 0}};
  std::vector<bool> nulls = {false, true, false, true, false};

  {  // small scale
    std::unique_ptr<Vector> vec;
    {  // without vec but with scalar
      Timestamp tsScalar;
      std::unique_ptr<Scalar> scalar =
          generateScalar<T>("2018-01-19 19:52:00", &tsScalar);
      std::vector<uint64_t> hashGroups = {0, 0, 1, 1, 1};
      grpVals = generateAggGroupValues<T>(initGrpValStrs, initGrpVals, true);
      callFuncScalar(testFunc, grpVals.get(), &hashGroups, true, scalar.get());
      checkAccOnTimestamp({{1516391520, 0},
                           {1516391520, 0},
                           {1516446272, 0},
                           {1516446272, 100000000}},
                          grpVals.get());
    }
    {
      LOG_INFO("Testing with seleletList");
      std::vector<uint64_t> hashGroups = {0, 0, 1, 1};
      SelectList sel = {0, 2, 3, 4};
      grpVals = generateAggGroupValues<T>(initGrpValStrs, initGrpVals, true);
      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectTimestampVector(TIMESTAMPID, valStrs,
                                                         &vals, &nulls, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      if (testMin)
        checkAccOnTimestamp({{-125815962890, 0},
                             {1516382710, 220000000},
                             {1516446272, 0},
                             {1516446272, 100000000}},
                            grpVals.get());
      else
        checkAccOnTimestamp({{1516382710, 0},
                             {1516382710, 220000000},
                             {1516446272, 0},
                             {1516446272, 100000000}},
                            grpVals.get());

      grpVals = generateAggGroupValues<T>(initGrpValStrs, initGrpVals, true);
      LOG_INFO("Testing has no nulls");
      vec = VectorUtility::generateSelectTimestampVector(TIMESTAMPID, valStrs,
                                                         &vals, nullptr, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      if (testMin)
        checkAccOnTimestamp({{-125815962890, 0},
                             {-125815962890, 112300000},
                             {1516446272, 0},
                             {1516446272, 100000000}},
                            grpVals.get());
      else
        checkAccOnTimestamp({{1516382710, 0},
                             {1516382710, 220000000},
                             {1516446272, 0},
                             {1516446272, 100000000}},
                            grpVals.get());

      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectTimestampVector(TIMESTAMPID, valStrs,
                                                         &vals, &nulls, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      if (testMin)
        checkAccOnTimestamp({{-125815962890, 0},
                             {-125815962890, 112300000},
                             {1516446272, 0},
                             {1516446272, 100000000}},
                            grpVals.get());
      else
        checkAccOnTimestamp({{1516382710, 0},
                             {1516382710, 220000000},
                             {1516446272, 0},
                             {1516446272, 100000000}},
                            grpVals.get());
    }
    {
      LOG_INFO("Testing without seleletList");
      grpVals = generateAggGroupValues<T>(initGrpValStrs, initGrpVals, true);
      std::vector<uint64_t> hashGroups = {0, 0, 1, 1, 1};

      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectTimestampVector(
          TIMESTAMPID, valStrs, &vals, &nulls, nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      if (testMin)
        checkAccOnTimestamp({{1516382710, 0},
                             {-125815962890, 0},
                             {1516446272, 0},
                             {1516446272, 100000000}},
                            grpVals.get());
      else
        checkAccOnTimestamp({{1516382710, 0},
                             {1516382710, 220000000},
                             {1516446272, 0},
                             {1516446272, 100000000}},
                            grpVals.get());

      grpVals = generateAggGroupValues<T>(initGrpValStrs, initGrpVals, true);
      LOG_INFO("Testing has no nulls");
      vec = VectorUtility::generateSelectTimestampVector(
          TIMESTAMPID, valStrs, &vals, nullptr, nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      if (testMin)
        checkAccOnTimestamp({{1516382710, 0},
                             {-125815962890, 0},
                             {1516446272, 0},
                             {1516446272, 100000000}},
                            grpVals.get());
      else
        checkAccOnTimestamp({{1516382710, 112300000},
                             {1516382710, 220000000},
                             {1516446272, 0},
                             {1516446272, 100000000}},
                            grpVals.get());

      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectTimestampVector(
          TIMESTAMPID, valStrs, &vals, &nulls, nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, true, vec.get());
      if (testMin)
        checkAccOnTimestamp({{1516382710, 0},
                             {-125815962890, 0},
                             {1516446272, 0},
                             {1516446272, 100000000}},
                            grpVals.get());
      else
        checkAccOnTimestamp({{1516382710, 112300000},
                             {1516382710, 220000000},
                             {1516446272, 0},
                             {1516446272, 100000000}},
                            grpVals.get());
    }
  }
}
TEST_F(AggFuncTest, DISABLED_MinMax) {
  testMinMax<int8_t, TINYINTID>(min_int8_smaller, true);
  testMinMax<int16_t, SMALLINTID>(min_int16_smaller, true);
  testMinMax<int32_t, INTID>(min_int32_smaller, true);
  testMinMax<int64_t, BIGINTID>(min_int64_smaller, true);
  testMinMax<float, FLOATID>(min_float_smaller, true);
  testMinMax<double, DOUBLEID>(min_double_smaller, true);
  testMinMax<std::string, STRINGID>(min_string_smaller, true);
  testMinMax<Timestamp, TIMESTAMPID>(min_timestamp_smaller, true);
  testMinMax<int32_t, DATEID>(min_date_smaller, true);
  testMinMax<int64_t, TIMEID>(min_time_smaller, true);

  testMinMax<int8_t, TINYINTID>(max_int8_larger, false);
  testMinMax<int16_t, SMALLINTID>(max_int16_larger, false);
  testMinMax<int32_t, INTID>(max_int32_larger, false);
  testMinMax<int64_t, BIGINTID>(max_int64_larger, false);
  testMinMax<float, FLOATID>(max_float_larger, false);
  testMinMax<double, DOUBLEID>(max_double_larger, false);
  testMinMax<std::string, STRINGID>(max_string_larger, false);
  testMinMax<Timestamp, TIMESTAMPID>(max_timestamp_larger, false);
  testMinMax<int32_t, DATEID>(max_date_larger, false);
  testMinMax<int64_t, TIMEID>(max_time_larger, false);
}

TEST_F(AggFuncTest, TestDecimalScalar) {
  this->isDecimal_ = true;
  std::vector<Datum> params(5);
  std::unique_ptr<Scalar> scalar = generateScalar(DECIMALNEWID, 3);
  std::vector<double> initGrpVals = {0, 0, 233, 233};
  std::unique_ptr<AggGroupValues> grpVals;
  std::vector<uint64_t> hashGroups = {0, 1, 0};
  params[2] = CreateDatum<const std::vector<uint64_t> *>(&hashGroups);
  params[3] = CreateDatum<bool>(true);
  params[4] = CreateDatum<Scalar *>(scalar.get());

  {
    LOG_INFO("Testing SUM(SCALAR)");
    grpVals = generateAggGroupValues<double>(initGrpVals, false, true);
    params[0] = CreateDatum(grpVals.get());

    sum_decimal_add(params.data(), 5);
    checkAcc<double>({6, 3, 233, 233}, grpVals.get());
  }

  {
    LOG_INFO("Testing AVG(SCALAR)");
    grpVals = generateAggGroupValues<double>(initGrpVals, true, true);
    params[0] = CreateDatum(grpVals.get());

    avg_decimal_accu(params.data(), 5);
    checkAvg({{6, 2}, {3, 1}, {233, 0}, {233, 0}}, grpVals.get());
  }
}

TEST_F(AggFuncTest, TestDecimal) {
  this->isDecimal_ = true;
  testMinMax<double, DECIMALNEWID>(min_decimal_smaller, true);
  testMinMax<double, DECIMALNEWID>(max_decimal_larger, false);
  testSum<double, DECIMALNEWID, double>(sum_decimal_add);
  testSum<double, DECIMALNEWID, double>(sum_decimal_sum);
  testAvgAccu<double, DECIMALNEWID>(avg_decimal_accu);
  testAvgAmalg<double, DECIMALNEWID>(avg_decimal_amalg);
}

}  // namespace dbcommon
