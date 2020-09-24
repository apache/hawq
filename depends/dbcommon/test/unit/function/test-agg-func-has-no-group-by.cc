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
#include "dbcommon/testutil/agg-func-utils.h"
#include "dbcommon/testutil/tuple-batch-utils.h"
#include "dbcommon/testutil/vector-utils.h"

namespace dbcommon {

TEST_F(AggFuncTest, has_no_group_by_count_star) {
  std::vector<Datum> params(4);
  {
    std::vector<int64_t> initAggGrpVals = {3};
    std::unique_ptr<AggGroupValues> grpValsBase =
        generateAggGroupValues<int64_t>(initAggGrpVals, false, false);
    std::vector<uint64_t> hashGroups = {0, 1};

    params[0] = CreateDatum<AggGroupValues *>(grpValsBase.get());
    params[2] = CreateDatum<const std::vector<uint64_t> *>(&hashGroups);
    params[3] = CreateDatum<bool>(false);

    count_star(params.data(), 4);
    auto grpVals =
        reinterpret_cast<AggPrimitiveGroupValues *>(grpValsBase.get());
    auto accessor = grpVals->getAccessor<AggPrimitiveGroupValues::Accessor>();
    EXPECT_EQ(5, DatumGetValue<int64_t>(accessor.at(0)->accVal.value));
  }
}

TEST_F(AggFuncTest, has_no_group_by_count_inc) {
  func_type testFunc = count_inc;
  std::vector<int64_t> initAggGrpVals = {20};
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

    LOG_INFO("Testing has GroupBy");
    callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
    checkAcc<int64_t>({23}, grpVals.get());

    LOG_INFO("Testing has no GroupBy");
    callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
    checkAcc<int64_t>({26}, grpVals.get());

    LOG_INFO("Testing has no nulls");
    vec->setHasNull(false);
    callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
    checkAcc<int64_t>({30}, grpVals.get());
  }

  {
    LOG_INFO("Testing with seleletList");
    std::unique_ptr<Vector> vec = VectorUtility::generateSelectVector<int64_t>(
        BIGINTID, vals, &nulls, nullptr);
    vec->setSelected(&sel, false);
    grpVals = generateAggGroupValues<int64_t>(initAggGrpVals, false, true);
    std::vector<uint64_t> hashGroups = {0, 1};

    LOG_INFO("Testing has GroupBy");
    callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
    checkAcc<int64_t>({22}, grpVals.get());

    LOG_INFO("Testing has no GroupBy");
    callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
    checkAcc<int64_t>({24}, grpVals.get());

    LOG_INFO("Testing has no nulls");
    vec->setHasNull(false);
    callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
    checkAcc<int64_t>({26}, grpVals.get());
  }
}

TEST_F(AggFuncTest, has_no_group_by_count_add) {
  func_type testFunc = count_add;
  std::vector<int64_t> initAggGrpVals = {0};

  std::vector<int64_t> vals = {9, 1, 2, 4};
  SelectList sel = {0, 2, 3};

  std::unique_ptr<AggGroupValues> grpVals;
  {
    LOG_INFO("Testing with seleletList");
    grpVals = generateAggGroupValues<int64_t>(initAggGrpVals, false, true);
    std::vector<uint64_t> hashGroups = {0, 1, 1};
    std::unique_ptr<Vector> vec = VectorUtility::generateSelectVector<int64_t>(
        BIGINTID, vals, nullptr, &sel);

    callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
    checkAcc<int64_t>({15}, grpVals.get());
  }
  {
    LOG_INFO("Testing without seleletList");
    std::vector<uint64_t> hashGroups = {0, 0, 1, 0};
    grpVals = generateAggGroupValues<int64_t>(initAggGrpVals, false, true);
    std::unique_ptr<Vector> vec = VectorUtility::generateSelectVector<int64_t>(
        BIGINTID, vals, nullptr, nullptr);

    callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
    checkAcc<int64_t>({16}, grpVals.get());
  }
}

template <class T, TypeKind TK, class RT>
void AggFuncTest::testSumHasNoGroupBy(func_type testFunc) {
  std::vector<RT> initAggGrpVals = {0};
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
    callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
    checkAcc<RT>({15}, grpVals.get());

    LOG_INFO("Testing has nulls");
    vec = VectorUtility::generateSelectVector<T>(TK, vals, &nulls, &sel);
    callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
    checkAcc<RT>({19}, grpVals.get());
  }

  {
    LOG_INFO("Testing without seleletList");
    std::vector<uint64_t> hashGroups = {0, 0, 1, 0};
    grpVals = generateAggGroupValues<RT>(initAggGrpVals, false, true);
    vec = VectorUtility::generateSelectVector<T>(TK, vals, nullptr, nullptr);

    LOG_INFO("Testing has no nulls");
    callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
    checkAcc<RT>({16}, grpVals.get());

    LOG_INFO("Testing has nulls");
    vec = VectorUtility::generateSelectVector<T>(TK, vals, &nulls, nullptr);
    callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
    checkAcc<RT>({21}, grpVals.get());
  }
}

TEST_F(AggFuncTest, has_no_group_by_Sum) {
  testSumHasNoGroupBy<int8_t, TINYINTID, int64_t>(sum_int8_add);
  testSumHasNoGroupBy<int64_t, BIGINTID, int64_t>(sum_int8_sum);
  testSumHasNoGroupBy<int16_t, SMALLINTID, int64_t>(sum_int16_add);
  testSumHasNoGroupBy<int64_t, BIGINTID, int64_t>(sum_int16_sum);
  testSumHasNoGroupBy<int32_t, INTID, int64_t>(sum_int32_add);
  testSumHasNoGroupBy<int64_t, BIGINTID, int64_t>(sum_int32_sum);
  testSumHasNoGroupBy<int64_t, BIGINTID, int64_t>(sum_int64_add);
  testSumHasNoGroupBy<int64_t, BIGINTID, int64_t>(sum_int64_sum);
  testSumHasNoGroupBy<float, FLOATID, double>(sum_float_add);
  testSumHasNoGroupBy<double, DOUBLEID, double>(sum_float_sum);
  testSumHasNoGroupBy<double, DOUBLEID, double>(sum_double_add);
  testSumHasNoGroupBy<double, DOUBLEID, double>(sum_double_sum);
}

template <class T, TypeKind TK>
void AggFuncTest::testAvgAccuHasNoGroupBy(func_type testFunc) {
  std::vector<T> initGrpVals = {0};
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
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      checkAvg({{18, 3}}, grpVals.get());
      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectVector<T>(TK, vals, &nulls, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      checkAvg({{29, 5}}, grpVals.get());
    }
    {
      LOG_INFO("Testing without seleletList");
      grpVals = generateAggGroupValues<T>(initGrpVals, true, true);
      LOG_INFO("Testing has no nulls");
      std::vector<uint64_t> hashGroups = {0, 0, 1, 1, 1};
      vec = VectorUtility::generateSelectVector<T>(TK, vals, nullptr, nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      checkAvg({{24, 5}}, grpVals.get());

      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectVector<T>(TK, vals, &nulls, nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      checkAvg({{39, 8}}, grpVals.get());
    }
  }
}
TEST_F(AggFuncTest, has_no_group_by_AvgAccumulate) {
  testAvgAccuHasNoGroupBy<int8_t, TINYINTID>(avg_int8_accu);
  testAvgAccuHasNoGroupBy<int16_t, SMALLINTID>(avg_int16_accu);
  testAvgAccuHasNoGroupBy<int32_t, INTID>(avg_int32_accu);
  testAvgAccuHasNoGroupBy<int64_t, BIGINTID>(avg_int64_accu);
  testAvgAccuHasNoGroupBy<float, FLOATID>(avg_float_accu);
  testAvgAccuHasNoGroupBy<double, DOUBLEID>(avg_double_accu);
}

template <class T, TypeKind TK>
void AggFuncTest::testAvgAmalgHasNoGroupBy(func_type testFunc) {
  std::vector<T> initGrpVals = {0};
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
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      checkAvg({{15, 3}}, grpVals.get());

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
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      checkAvg({{39, 8}}, grpVals.get());
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
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      checkAvg({{11, 2}}, grpVals.get());

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
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      checkAvg({{29, 5}}, grpVals.get());
    }
  }
}
TEST_F(AggFuncTest, has_no_group_by_AvgAmalgamate) {
  testAvgAmalgHasNoGroupBy<int8_t, TINYINTID>(avg_int8_amalg);
  testAvgAmalgHasNoGroupBy<int16_t, SMALLINTID>(avg_int16_amalg);
  testAvgAmalgHasNoGroupBy<int32_t, INTID>(avg_int32_amalg);
  testAvgAmalgHasNoGroupBy<int64_t, BIGINTID>(avg_int64_amalg);
  testAvgAmalgHasNoGroupBy<float, FLOATID>(avg_float_amalg);
  testAvgAmalgHasNoGroupBy<double, DOUBLEID>(avg_double_amalg);
}

TEST_F(AggFuncTest, has_no_group_by_AvgAverage) {
  std::vector<Datum> params(1);
  params[0] =
      CreateDatum<AvgPrimitiveTransData *>(new AvgPrimitiveTransData({11, 2}));
  Datum avgDouble = avg_double_avg(params.data(), 1);
  EXPECT_EQ(5.5, DatumGetValue<double>(avgDouble));

  params[0] = CreateDatum(new AvgDecimalTransData({{0, 11, 0}, 2}));
  Datum avgDecimal = avg_decimal_avg(params.data(), 1);
  std::string avgStr = dbcommon::DecimalType::toString(
      (DatumGetValue<DecimalVar *>(avgDecimal))->highbits,
      (DatumGetValue<DecimalVar *>(avgDecimal))->lowbits,
      (DatumGetValue<DecimalVar *>(avgDecimal))->scale);
  EXPECT_EQ(5.5, std::stod(avgStr));
}

TEST_F(AggFuncTest, has_no_group_by_Scalar) {
  std::vector<Datum> params(5);
  Scalar scalar(CreateDatum<int64_t>(3));
  std::vector<int64_t> initGrpVals = {0};
  std::unique_ptr<AggGroupValues> grpVals;
  std::vector<uint64_t> hashGroups = {0, 0, 0};
  params[2] = CreateDatum<const std::vector<uint64_t> *>(&hashGroups);
  params[3] = CreateDatum<bool>(false);
  params[4] = CreateDatum<Scalar *>(&scalar);

  {
    LOG_INFO("Testing COUNT(SCALAR)");
    grpVals = generateAggGroupValues<int64_t>(initGrpVals, false, true);
    params[0] = CreateDatum(grpVals.get());

    count_inc(params.data(), 5);
    checkAcc<int64_t>({3}, grpVals.get());
  }

  {
    LOG_INFO("Testing SUM(SCALAR)");
    grpVals = generateAggGroupValues<int64_t>(initGrpVals, false, true);
    params[0] = CreateDatum(grpVals.get());

    sum_int64_add(params.data(), 5);
    checkAcc<int64_t>({9}, grpVals.get());
  }

  {
    LOG_INFO("Testing AVG(SCALAR)");
    grpVals = generateAggGroupValues<int64_t>(initGrpVals, true, true);
    params[0] = CreateDatum(grpVals.get());

    avg_int64_accu(params.data(), 5);
    checkAvg({{9, 3}}, grpVals.get());
  }
}

TEST_F(AggFuncTest, has_no_group_by_Statictis) {
  std::vector<Datum> params(5);
  Vector::uptr vec = Vector::BuildVector(BIGINTID, true);
  std::vector<int64_t> initGrpVals = {0};
  std::unique_ptr<AggGroupValues> grpVals;
  std::vector<uint64_t> hashGroups = {0, 0, 0};
  params[2] = CreateDatum<const std::vector<uint64_t> *>(&hashGroups);
  params[3] = CreateDatum<bool>(false);
  params[4] = CreateDatum<Vector *>(vec.get());

  VectorStatistics stat;
  stat.hasMinMaxStats = true;
  stat.minimum = CreateDatum(uint64_t(199));
  stat.maximum = CreateDatum(uint64_t(998));
  stat.sum = CreateDatum(uint64_t(6666));
  stat.valueCount = 233;
  vec->setVectorStatistics(stat);

  {
    LOG_INFO("Testing COUNT(STATISTICS)");
    grpVals = generateAggGroupValues<int64_t>(initGrpVals, false, true);
    params[0] = CreateDatum(grpVals.get());

    count_inc(params.data(), 5);
    checkAcc<int64_t>({233}, grpVals.get());
    count_inc(params.data(), 5);
    checkAcc<int64_t>({233 * 2}, grpVals.get());
  }

  {
    LOG_INFO("Testing SUM(STATISTICS)");
    grpVals = generateAggGroupValues<int64_t>(initGrpVals, false, true);
    params[0] = CreateDatum(grpVals.get());

    sum_int64_add(params.data(), 5);
    checkAcc<int64_t>({6666}, grpVals.get());
    sum_int64_add(params.data(), 5);
    checkAcc<int64_t>({6666 * 2}, grpVals.get());
  }

  {
    LOG_INFO("Testing AVG(STATISTICS)");
    grpVals = generateAggGroupValues<int64_t>(initGrpVals, true, true);
    params[0] = CreateDatum(grpVals.get());

    avg_int64_accu(params.data(), 5);
    checkAvg({{6666, 233}}, grpVals.get());
    avg_int64_accu(params.data(), 5);
    checkAvg({{6666 * 2, 233 * 2}}, grpVals.get());
  }

  {
    LOG_INFO("Testing MIN/MAX(STATISTICS)");
    grpVals = generateAggGroupValues<int64_t>(initGrpVals, false, true);
    params[0] = CreateDatum(grpVals.get());

    min_int64_smaller(params.data(), 5);
    checkAcc<int64_t>({199}, grpVals.get());
    min_int64_smaller(params.data(), 5);
    checkAcc<int64_t>({199}, grpVals.get());
  }
}

TEST_F(AggFuncTest, has_no_group_by_Null) {
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
void AggFuncTest::testMinMaxHasNoGroupBy(func_type testFunc, bool testMin) {
  std::vector<T> initGrpVals = {0};
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
      checkAcc<T>({1}, grpVals.get());
    }
    {
      LOG_INFO("Testing with seleletList");
      std::vector<uint64_t> hashGroups = {0, 1, 1};
      SelectList sel = {0, 3, 4};
      grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectVector<T>(TK, vals, &nulls, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      if (testMin)
        checkAcc<T>({6}, grpVals.get());
      else
        checkAcc<T>({7}, grpVals.get());

      grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
      LOG_INFO("Testing has no nulls");
      vec = VectorUtility::generateSelectVector<T>(TK, vals, nullptr, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      if (testMin)
        checkAcc<T>({5}, grpVals.get());
      else
        checkAcc<T>({7}, grpVals.get());

      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectVector<T>(TK, vals, &nulls, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      if (testMin)
        checkAcc<T>({5}, grpVals.get());
      else
        checkAcc<T>({7}, grpVals.get());
    }
    {
      LOG_INFO("Testing without seleletList");
      grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
      std::vector<uint64_t> hashGroups = {0, 0, 1, 1, 1};

      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectVector<T>(TK, vals, &nulls, nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      if (testMin)
        checkAcc<T>({4}, grpVals.get());
      else
        checkAcc<T>({7}, grpVals.get());

      grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
      LOG_INFO("Testing has no nulls");
      vec = VectorUtility::generateSelectVector<T>(TK, vals, nullptr, nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      if (testMin)
        checkAcc<T>({2}, grpVals.get());
      else
        checkAcc<T>({7}, grpVals.get());

      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectVector<T>(TK, vals, &nulls, nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      if (testMin)
        checkAcc<T>({2}, grpVals.get());
      else
        checkAcc<T>({7}, grpVals.get());
    }
  }
}
template <>
void AggFuncTest::testMinMaxHasNoGroupBy<std::string, STRINGID>(
    func_type testFunc, bool testMin) {
  typedef std::string T;
  std::vector<T> initGrpVals = {"0"};
  std::unique_ptr<AggGroupValues> grpVals;
  std::vector<T> vals = {"6", "2", "4", "5", "7"};
  std::vector<bool> nulls = {false, true, false, true, false};

  {
    LOG_INFO("Testing MIN/MAX(STATISTICS)");
    Vector::uptr vec = Vector::BuildVector(STRINGID, true);

    VectorStatistics stat;
    stat.hasMinMaxStats = true;
    stat.minimum = CreateDatum<const char *>("hello");
    stat.maximum = CreateDatum<const char *>("hi");
    stat.valueCount = 233;
    vec->setVectorStatistics(stat);

    std::vector<uint64_t> hashGroups = {0};
    grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
    callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
    if (testMin)
      checkAccOnString({"hello"}, grpVals.get());
    else
      checkAccOnString({"hi"}, grpVals.get());

    stat.minimum = CreateDatum<const char *>("heihei");
    stat.maximum = CreateDatum<const char *>("xixi");
    vec->setVectorStatistics(stat);
    callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
    if (testMin)
      checkAccOnString({"heihei"}, grpVals.get());
    else
      checkAccOnString({"xixi"}, grpVals.get());
  }

  {  // small scale
    std::unique_ptr<Vector> vec;
    {  // without vec but with scalar
      LOG_INFO("Testing scalar");
      std::unique_ptr<Scalar> scalar = generateScalar<T>("1");
      std::vector<uint64_t> hashGroups = {0, 0, 1, 1, 1};
      grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
      callFuncScalar(testFunc, grpVals.get(), &hashGroups, true, scalar.get());
      checkAccOnString({"1"}, grpVals.get());
    }
    {
      LOG_INFO("Testing with seleletList");
      std::vector<uint64_t> hashGroups = {0, 1, 1};
      SelectList sel = {0, 3, 4};
      grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
      LOG_INFO("Testing has nulls");
      vec =
          VectorUtility::generateSelectVector<T>(STRINGID, vals, &nulls, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      if (testMin)
        checkAccOnString({"6"}, grpVals.get());
      else
        checkAccOnString({"7"}, grpVals.get());

      grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
      LOG_INFO("Testing has no nulls");
      vec =
          VectorUtility::generateSelectVector<T>(STRINGID, vals, nullptr, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      if (testMin)
        checkAccOnString({"5"}, grpVals.get());
      else
        checkAccOnString({"7"}, grpVals.get());

      LOG_INFO("Testing has nulls");
      vec =
          VectorUtility::generateSelectVector<T>(STRINGID, vals, &nulls, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      if (testMin)
        checkAccOnString({"5"}, grpVals.get());
      else
        checkAccOnString({"7"}, grpVals.get());
    }
    {
      LOG_INFO("Testing without seleletList");
      grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
      std::vector<uint64_t> hashGroups = {0, 0, 1, 1, 1};

      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectVector<T>(STRINGID, vals, &nulls,
                                                   nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      if (testMin)
        checkAccOnString({"4"}, grpVals.get());
      else
        checkAccOnString({"7"}, grpVals.get());

      grpVals = generateAggGroupValues<T>(initGrpVals, false, true);
      LOG_INFO("Testing has no nulls");
      vec = VectorUtility::generateSelectVector<T>(STRINGID, vals, nullptr,
                                                   nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      if (testMin)
        checkAccOnString({"2"}, grpVals.get());
      else
        checkAccOnString({"7"}, grpVals.get());

      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectVector<T>(STRINGID, vals, &nulls,
                                                   nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      if (testMin)
        checkAccOnString({"2"}, grpVals.get());
      else
        checkAccOnString({"7"}, grpVals.get());
    }
  }
}
template <>
void AggFuncTest::testMinMaxHasNoGroupBy<Timestamp, TIMESTAMPID>(
    func_type testFunc, bool testMin) {
  typedef Timestamp T;
  std::vector<std::string> initGrpValStrs = {"1970-01-01 00:00:00"};
  std::vector<T> initGrpVals = {{0, 0}};
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
      checkAccOnTimestamp({{1516391520, 0}}, grpVals.get());
    }
    {
      LOG_INFO("Testing with selectList");
      std::vector<uint64_t> hashGroups = {0, 1, 1};
      SelectList sel = {0, 3, 4};
      grpVals = generateAggGroupValues<T>(initGrpValStrs, initGrpVals, true);
      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectTimestampVector(TIMESTAMPID, valStrs,
                                                         &vals, &nulls, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      if (testMin)
        checkAccOnTimestamp({{1516382710, 0}}, grpVals.get());
      else
        checkAccOnTimestamp({{1516382710, 220000000}}, grpVals.get());

      grpVals = generateAggGroupValues<T>(initGrpValStrs, initGrpVals, true);
      LOG_INFO("Testing has no nulls");
      vec = VectorUtility::generateSelectTimestampVector(TIMESTAMPID, valStrs,
                                                         &vals, nullptr, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      if (testMin)
        checkAccOnTimestamp({{-125815962890, 112300000}}, grpVals.get());
      else
        checkAccOnTimestamp({{1516382710, 220000000}}, grpVals.get());

      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectTimestampVector(TIMESTAMPID, valStrs,
                                                         &vals, &nulls, &sel);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      if (testMin)
        checkAccOnTimestamp({{-125815962890, 112300000}}, grpVals.get());
      else
        checkAccOnTimestamp({{1516382710, 220000000}}, grpVals.get());
    }
    {
      LOG_INFO("Testing without selectList");
      grpVals = generateAggGroupValues<T>(initGrpValStrs, initGrpVals, true);
      std::vector<uint64_t> hashGroups = {0, 0, 1, 1, 1};

      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectTimestampVector(
          TIMESTAMPID, valStrs, &vals, &nulls, nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      if (testMin)
        checkAccOnTimestamp({{-125815962890, 0}}, grpVals.get());
      else
        checkAccOnTimestamp({{1516382710, 220000000}}, grpVals.get());

      grpVals = generateAggGroupValues<T>(initGrpValStrs, initGrpVals, true);
      LOG_INFO("Testing has no nulls");
      vec = VectorUtility::generateSelectTimestampVector(
          TIMESTAMPID, valStrs, &vals, nullptr, nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      if (testMin)
        checkAccOnTimestamp({{-125815962890, 0}}, grpVals.get());
      else
        checkAccOnTimestamp({{1516382710, 220000000}}, grpVals.get());

      LOG_INFO("Testing has nulls");
      vec = VectorUtility::generateSelectTimestampVector(
          TIMESTAMPID, valStrs, &vals, &nulls, nullptr);
      callFunc(testFunc, grpVals.get(), &hashGroups, false, vec.get());
      if (testMin)
        checkAccOnTimestamp({{-125815962890, 0}}, grpVals.get());
      else
        checkAccOnTimestamp({{1516382710, 220000000}}, grpVals.get());
    }
  }
}
TEST_F(AggFuncTest, DISABLED_has_no_group_by_MinMax) {
  testMinMaxHasNoGroupBy<int8_t, TINYINTID>(min_int8_smaller, true);
  testMinMaxHasNoGroupBy<int16_t, SMALLINTID>(min_int16_smaller, true);
  testMinMaxHasNoGroupBy<int32_t, INTID>(min_int32_smaller, true);
  testMinMaxHasNoGroupBy<int64_t, BIGINTID>(min_int64_smaller, true);
  testMinMaxHasNoGroupBy<float, FLOATID>(min_float_smaller, true);
  testMinMaxHasNoGroupBy<double, DOUBLEID>(min_double_smaller, true);
  testMinMaxHasNoGroupBy<std::string, STRINGID>(min_string_smaller, true);
  testMinMaxHasNoGroupBy<Timestamp, TIMESTAMPID>(min_timestamp_smaller, true);

  testMinMaxHasNoGroupBy<int8_t, TINYINTID>(max_int8_larger, false);
  testMinMaxHasNoGroupBy<int16_t, SMALLINTID>(max_int16_larger, false);
  testMinMaxHasNoGroupBy<int32_t, INTID>(max_int32_larger, false);
  testMinMaxHasNoGroupBy<int64_t, BIGINTID>(max_int64_larger, false);
  testMinMaxHasNoGroupBy<float, FLOATID>(max_float_larger, false);
  testMinMaxHasNoGroupBy<double, DOUBLEID>(max_double_larger, false);
  testMinMaxHasNoGroupBy<std::string, STRINGID>(max_string_larger, false);
  testMinMaxHasNoGroupBy<Timestamp, TIMESTAMPID>(max_timestamp_larger, false);
}

TEST_F(AggFuncTest, TestDecimalScalar_has_no_group_by) {
  this->isDecimal_ = true;
  std::vector<Datum> params(5);
  std::unique_ptr<Scalar> scalar = generateScalar(DECIMALNEWID, 3);
  std::vector<double> initGrpVals = {0};
  std::unique_ptr<AggGroupValues> grpVals;
  std::vector<uint64_t> hashGroups = {0, 0, 0};
  params[2] = CreateDatum<const std::vector<uint64_t> *>(&hashGroups);
  params[3] = CreateDatum<bool>(false);
  params[4] = CreateDatum<Scalar *>(scalar.get());

  {
    LOG_INFO("Testing SUM(SCALAR)");
    grpVals = generateAggGroupValues<double>(initGrpVals, false, true);
    params[0] = CreateDatum(grpVals.get());

    sum_decimal_add(params.data(), 5);
    checkAcc<double>({9}, grpVals.get());
    sum_decimal_add(params.data(), 5);
    checkAcc<double>({18}, grpVals.get());
  }

  {
    LOG_INFO("Testing AVG(SCALAR)");
    grpVals = generateAggGroupValues<double>(initGrpVals, true, true);
    params[0] = CreateDatum(grpVals.get());

    avg_decimal_accu(params.data(), 5);
    checkAvg({{9, 3}}, grpVals.get());
    avg_decimal_accu(params.data(), 5);
    checkAvg({{18, 6}}, grpVals.get());
  }
}

TEST_F(AggFuncTest, TestDecimal_has_no_group_by_Statictis) {
  this->isDecimal_ = true;
  std::vector<Datum> params(5);
  Vector::uptr vec = Vector::BuildVector(DECIMALNEWID, true);
  std::vector<double> initGrpVals = {0};
  std::unique_ptr<AggGroupValues> grpVals;
  std::vector<uint64_t> hashGroups = {0, 0, 0};
  params[2] = CreateDatum<const std::vector<uint64_t> *>(&hashGroups);
  params[3] = CreateDatum<bool>(false);
  params[4] = CreateDatum<Vector *>(vec.get());

  DecimalVar min{0, 199, 0}, max{0, 998, 0}, sum{0, 6666, 0};
  VectorStatistics stat;
  stat.hasMinMaxStats = true;
  stat.minimum = CreateDatum<const char *>("199");
  stat.maximum = CreateDatum<const char *>("998");
  stat.sum = CreateDatum<const char *>("6666");
  stat.valueCount = 233;
  vec->setVectorStatistics(stat);

  {
    LOG_INFO("Testing SUM(STATISTICS)");
    grpVals = generateAggGroupValues<double>(initGrpVals, false, true);
    params[0] = CreateDatum(grpVals.get());

    sum_decimal_add(params.data(), 5);
    checkAcc<double>({6666}, grpVals.get());
    sum_decimal_add(params.data(), 5);
    checkAcc<double>({6666 * 2}, grpVals.get());
  }

  {
    LOG_INFO("Testing AVG(STATISTICS)");
    grpVals = generateAggGroupValues<double>(initGrpVals, true, true);
    params[0] = CreateDatum(grpVals.get());

    avg_decimal_accu(params.data(), 5);
    checkAvg({{6666, 233}}, grpVals.get());
    avg_decimal_accu(params.data(), 5);
    checkAvg({{6666 * 2, 233 * 2}}, grpVals.get());
  }

  {
    LOG_INFO("Testing MIN/MAX(STATISTICS)");
    grpVals = generateAggGroupValues<double>(initGrpVals, false, true);
    params[0] = CreateDatum(grpVals.get());

    min_decimal_smaller(params.data(), 5);
    checkAcc<double>({199}, grpVals.get());

    stat.minimum = CreateDatum<const char *>("6");
    vec->setVectorStatistics(stat);
    min_decimal_smaller(params.data(), 5);
    checkAcc<double>({6}, grpVals.get());
  }
}

TEST_F(AggFuncTest, TestDecimal_has_no_group_by) {
  this->isDecimal_ = true;
  testMinMaxHasNoGroupBy<double, DECIMALNEWID>(min_decimal_smaller, true);
  testMinMaxHasNoGroupBy<double, DECIMALNEWID>(max_decimal_larger, false);
  testSumHasNoGroupBy<double, DECIMALNEWID, double>(sum_decimal_add);
  testSumHasNoGroupBy<double, DECIMALNEWID, double>(sum_decimal_sum);
  testAvgAccuHasNoGroupBy<double, DECIMALNEWID>(avg_decimal_accu);
  testAvgAmalgHasNoGroupBy<double, DECIMALNEWID>(avg_decimal_amalg);
}

}  // namespace dbcommon
