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

#ifndef DBCOMMON_SRC_DBCOMMON_TESTUTIL_AGG_FUNC_UTILS_H_
#define DBCOMMON_SRC_DBCOMMON_TESTUTIL_AGG_FUNC_UTILS_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "dbcommon/function/agg-func.h"
#include "dbcommon/testutil/scalar-utils.h"
#include "dbcommon/testutil/vector-utils.h"

namespace dbcommon {

class AggFuncTest : public ::testing::Test {
 public:
  AggFuncTest() {}
  virtual ~AggFuncTest() {}

  template <class T>
  std::unique_ptr<Scalar> generateScalar(TypeKind typekind, T val) {
    if (scalarUtilitys_[typekind] == nullptr)
      scalarUtilitys_[typekind].reset(new ScalarUtility(typekind));
    std::unique_ptr<Scalar> sc(new Scalar);
    *sc = scalarUtilitys_[typekind]->generateScalar(std::to_string(val));
    return std::move(sc);
  }
  template <class T>
  std::unique_ptr<Scalar> generateScalar(std::string val) {
    std::unique_ptr<Scalar> sc(new Scalar);
    sc->value = CreateDatum(val.c_str(), TypeMapping<T>::type);
    return std::move(sc);
  }
  template <class T>
  std::unique_ptr<Scalar> generateScalar(std::string val,
                                         Timestamp *timestamp) {
    std::unique_ptr<Scalar> sc(new Scalar);
    sc->value = CreateDatum(val.c_str(), timestamp, TypeMapping<T>::type);
    return std::move(sc);
  }

  // @param vect The initial value of AggGroupValues
  template <class T>
  std::unique_ptr<AggGroupValues> generateAggGroupValues(
      const std::vector<T> &vect, bool isAvg, bool isNotCountStar) {
    std::unique_ptr<AggGroupValues> ret;
    if (isDecimal_) {
      using AggGroupValues = AggDecimalGroupValues;
      ret.reset(new AggDecimalGroupValues);
      ret->resize(vect.size());
      auto grps = reinterpret_cast<AggGroupValues *>(ret.get());
      auto accessor = grps->getAccessor<AggGroupValues::Accessor>();

      for (int i = 0; i < vect.size(); i++) {
        DecimalVar decimal = DecimalType::fromString(std::to_string(vect[i]));
        auto val = accessor.at(i);
        val->accVal.value = decimal;
        val->accVal.isNotNull = !isNotCountStar;
        if (isAvg) {
          val->avgVal.sum = DecimalVar(0, 0, 0);
          val->avgVal.count = 0;
        }
      }
      return std::move(ret);
    }

    ret.reset(new AggPrimitiveGroupValues);
    ret->resize(vect.size());
    auto grps = reinterpret_cast<AggPrimitiveGroupValues *>(ret.get());
    auto accessor = grps->getAccessor<AggPrimitiveGroupValues::Accessor>();
    for (int i = 0; i < vect.size(); i++) {
      Datum d = CreateDatum<T>(vect[i]);
      auto val = accessor.at(i);
      val->accVal.value = d;
      val->accVal.isNotNull = !isNotCountStar;
      if (isAvg) {
        val->avgVal.sum = 0;
        val->avgVal.count = 0;
      }
    }
    return std::move(ret);
  }

  template <class T>
  std::unique_ptr<AggGroupValues> generateAggGroupValues(
      const std::vector<std::string> &vect, bool isAvg, bool isNotCountStar) {
    std::unique_ptr<AggGroupValues> ret(new AggStringGroupValues);
    ret->resize(vect.size());
    auto grps = reinterpret_cast<AggStringGroupValues *>(ret.get());
    auto accessor = grps->getAccessor<AggStringGroupValues ::Accessor>();
    for (int i = 0; i < vect.size(); i++) {
      auto val = accessor.at(i);
      if (!isNotCountStar)
        val->accVal.value =
            (ret->newStrTransData(vect[i].c_str(), vect[i].size()));
    }
    return std::move(ret);
  }

  template <class T>
  std::unique_ptr<AggGroupValues> generateAggGroupValues(
      const std::vector<std::string> &vectStr,  // NOLINT
      std::vector<Timestamp> &vect,             // NOLINT
      bool isNotCountStar) {
    std::unique_ptr<AggGroupValues> ret(new AggTimestampGroupValues);
    ret->resize(vect.size());
    auto grps = reinterpret_cast<AggTimestampGroupValues *>(ret.get());
    auto accessor = grps->getAccessor<AggTimestampGroupValues::Accessor>();
    for (int i = 0; i < vect.size(); i++) {
      Datum d = CreateDatum(vectStr[i].c_str(), &vect[i], TIMESTAMPID);
      auto val = accessor.at(i);
      val->accVal.value = vect[i];
      val->accVal.isNotNull = !isNotCountStar;
    }
    return std::move(ret);
  }

  void callFunc(func_type func, AggGroupValues *grpVals,
                const std::vector<uint64_t> *hashGroups, bool hasGroupBy,
                Vector *vec) {
    std::vector<Datum> params(5);
    params[0] = CreateDatum(grpVals);
    params[2] = CreateDatum<const std::vector<uint64_t> *>(hashGroups);
    params[3] = CreateDatum<bool>(hasGroupBy);
    params[4] = CreateDatum<Vector *>(vec);
    func(params.data(), 5);
  }
  void callFuncScalar(func_type func, AggGroupValues *grpVals,
                      const std::vector<uint64_t> *hashGroups, bool hasGroupBy,
                      Scalar *scalar) {
    std::vector<Datum> params(5);
    params[0] = CreateDatum(grpVals);
    params[2] = CreateDatum<const std::vector<uint64_t> *>(hashGroups);
    params[3] = CreateDatum<bool>(hasGroupBy);
    params[4] = CreateDatum<Scalar *>(scalar);
    func(params.data(), 5);
  }
  template <class T>
  void checkAcc(std::vector<T> expected, AggGroupValues *grpValsBase) {
    EXPECT_EQ(expected.size(), grpValsBase->size());
    if (grpValsBase->isDecimal()) {
      using AggGroupValues = AggDecimalGroupValues;
      auto grpVals = reinterpret_cast<AggGroupValues *>(grpValsBase);
      auto accessor = grpVals->getAccessor<AggGroupValues::Accessor>();
      DecimalType t;
      EXPECT_EQ(expected.size(), grpVals->size());
      for (int64_t i = 0; i < expected.size(); i++) {
        EXPECT_EQ(expected[i],
                  std::stod(t.toString(accessor.at(i)->accVal.value)));
      }
      return;
    }

    auto grpVals = reinterpret_cast<AggPrimitiveGroupValues *>(grpValsBase);
    auto accessor = grpVals->getAccessor<AggPrimitiveGroupValues::Accessor>();
    EXPECT_EQ(expected.size(), grpVals->size());
    for (int64_t i = 0; i < expected.size(); i++) {
      EXPECT_EQ(expected[i], DatumGetValue<T>(accessor.at(i)->accVal.value));
    }
  }
  void checkAccOnString(std::vector<std::string> expected,
                        AggGroupValues *grpValsBase) {
    auto grpVals = reinterpret_cast<AggStringGroupValues *>(grpValsBase);
    auto accessor = grpVals->getAccessor<AggStringGroupValues::Accessor>();
    EXPECT_EQ(expected.size(), grpVals->size());
    for (int64_t i = 0; i < expected.size(); i++) {
      auto strTransData = (accessor.at(i)->accVal.value);
      if (accessor.at(i)->accVal.isNotNull)
        EXPECT_EQ(expected[i],
                  std::string(strTransData->str, strTransData->length));
    }
  }
  void checkAccOnTimestamp(std::vector<Timestamp> expected,
                           AggGroupValues *grpValsBase) {
    auto grpVals = reinterpret_cast<AggTimestampGroupValues *>(grpValsBase);
    auto accessor = grpVals->getAccessor<AggTimestampGroupValues::Accessor>();
    EXPECT_EQ(expected.size(), grpVals->size());
    for (int64_t i = 0; i < expected.size(); i++) {
      Timestamp *ts = &(accessor.at(i)->accVal.value);
      EXPECT_EQ(expected[i], *ts);
    }
  }
  void checkAvg(std::vector<AvgPrimitiveTransData> expected,
                AggGroupValues *grpValsBase) {
    EXPECT_EQ(expected.size(), grpValsBase->size());
    if (grpValsBase->isDecimal()) {
      DecimalType t;
      using AggGroupValues = AggDecimalGroupValues;
      auto grpVals = reinterpret_cast<AggGroupValues *>(grpValsBase);
      auto accessor = grpVals->getAccessor<AggGroupValues::Accessor>();
      EXPECT_EQ(expected.size(), grpVals->size());
      for (int64_t i = 0; i < expected.size(); i++) {
        if (expected[i].count)
          EXPECT_EQ(expected[i].sum,
                    std::stod(t.toString(accessor.at(i)->avgVal.sum)));
        EXPECT_EQ(expected[i].count, accessor.at(i)->avgVal.count);
      }
      return;
    }
    auto grpVals = reinterpret_cast<AggPrimitiveGroupValues *>(grpValsBase);
    auto accessor = grpVals->getAccessor<AggPrimitiveGroupValues::Accessor>();
    EXPECT_EQ(expected.size(), grpVals->size());
    for (int64_t i = 0; i < expected.size(); i++) {
      EXPECT_EQ(expected[i].sum, accessor.at(i)->avgVal.sum);
      EXPECT_EQ(expected[i].count, accessor.at(i)->avgVal.count);
    }
  }
  void checkNull(std::vector<bool> expected, AggGroupValues *grpValsBase,
                 bool isAvg) {
    auto grpVals = reinterpret_cast<AggPrimitiveGroupValues *>(grpValsBase);
    auto accessor = grpVals->getAccessor<AggPrimitiveGroupValues::Accessor>();
    EXPECT_EQ(expected.size(), grpVals->size());
    for (int64_t i = 0; i < expected.size(); i++) {
      if (isAvg)
        EXPECT_EQ(expected[i], accessor.at(i)->avgVal.count == 0);
      else
        EXPECT_EQ(!expected[i], accessor.at(i)->accVal.isNotNull);
    }
  }
  template <class T, TypeKind TK, class RT>
  void testSum(func_type testFunc);
  template <class T, TypeKind TK>
  void testAvgAccu(func_type testFunc);
  template <class T, TypeKind TK>
  void testAvgAmalg(func_type testFunc);
  template <class T, TypeKind TK>
  void testMinMax(func_type testFunc, bool testMin);

  template <class T, TypeKind TK, class RT>
  void testSumSmallScale(func_type testFunc);
  template <class T, TypeKind TK>
  void testAvgAccuSmallScale(func_type testFunc);
  template <class T, TypeKind TK>
  void testAvgAmalgSmallScale(func_type testFunc);
  template <class T, TypeKind TK>
  void testMinMaxSmallScale(func_type testFunc, bool testMin);

  template <class T, TypeKind TK, class RT>
  void testSumHasNoGroupBy(func_type testFunc);
  template <class T, TypeKind TK>
  void testAvgAccuHasNoGroupBy(func_type testFunc);
  template <class T, TypeKind TK>
  void testAvgAmalgHasNoGroupBy(func_type testFunc);
  template <class T, TypeKind TK>
  void testMinMaxHasNoGroupBy(func_type testFunc, bool testMin);

 private:
  bool isDecimal_ = false;
  std::map<TypeKind, std::unique_ptr<ScalarUtility>> scalarUtilitys_;
};
}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_TESTUTIL_AGG_FUNC_UTILS_H_
