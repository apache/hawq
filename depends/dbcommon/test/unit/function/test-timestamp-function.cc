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

#include "dbcommon/common/vector.h"
#include "dbcommon/common/vector/fixed-length-vector.h"
#include "dbcommon/common/vector/timestamp-vector.h"
#include "dbcommon/function/arith-cmp-func.cg.h"
#include "dbcommon/function/date-function.h"
#include "dbcommon/function/func-kind.cg.h"
#include "dbcommon/function/func.h"
#include "dbcommon/nodes/select-list.h"

namespace dbcommon {
template <class T>
struct TypeMapping;
template <>
struct TypeMapping<int8_t> {
  static const TypeKind type = TINYINTID;
};
template <>
struct TypeMapping<int16_t> {
  static const TypeKind type = SMALLINTID;
};
template <>
struct TypeMapping<int32_t> {
  static const TypeKind type = INTID;
};
template <>
struct TypeMapping<int64_t> {
  static const TypeKind type = BIGINTID;
};
template <>
struct TypeMapping<float> {
  static const TypeKind type = FLOATID;
};
template <>
struct TypeMapping<double> {
  static const TypeKind type = DOUBLEID;
};
template <>
struct TypeMapping<std::string> {
  static const TypeKind type = STRINGID;
};
template <>
struct TypeMapping<Timestamp> {
  static const TypeKind type = TIMESTAMPID;
};

class TimestampFunctionTest {
 public:
  TimestampFunctionTest() {}

  virtual ~TimestampFunctionTest() {}

  std::unique_ptr<Scalar> generateTimestampScalar(const char *valStr,
                                                  Timestamp *val) {
    std::unique_ptr<Scalar> sc(new Scalar);
    sc->value = CreateDatum(valStr, val, TIMESTAMPID);
    return std::move(sc);
  }

  template <class T>
  std::unique_ptr<Scalar> generateScalar(T val) {
    std::unique_ptr<Scalar> sc(new Scalar);
    sc->value = CreateDatum<T>(val);
    return std::move(sc);
  }

  template <class T>
  std::unique_ptr<Vector> generateVector(const std::vector<T> &vect,
                                         const std::vector<bool> &nulls,
                                         SelectList *lst) {
    std::unique_ptr<Vector> vec =
        Vector::BuildVector(TypeMapping<T>::type, true);

    for (size_t s = 0; s < vect.size(); s++) {
      Datum d = CreateDatum<T>(vect[s]);
      vec->append(d, nulls[s]);
    }
    if (lst) vec->setSelected(lst, false);
    return std::move(vec);
  }

  std::unique_ptr<Vector> generateVector(
      TypeKind type, const std::vector<std::string> &vectStr,
      std::vector<Timestamp> &vect, const std::vector<bool> &nulls) {  // NOLINT
    std::unique_ptr<Vector> vec = Vector::BuildVector(type, true);

    for (size_t s = 0; s < vect.size(); s++) {
      Datum d = CreateDatum(vectStr[s].c_str(), &vect[s], TIMESTAMPID);
      vec->append(d, nulls[s]);
    }
    return std::move(vec);
  }

  std::unique_ptr<Vector> generateSelectVector(
      TypeKind type, const std::vector<std::string> &vectStr,
      std::vector<Timestamp> &vect, const std::vector<bool> &nulls,  // NOLINT
      SelectList *sel) {
    std::unique_ptr<Vector> result = generateVector(type, vectStr, vect, nulls);

    if (sel) result->setSelected(sel, false);

    return std::move(result);
  }

  std::vector<Datum> generateCMPScalarScalar(const char *vStr1, Timestamp *v1,
                                             const char *vStr2, Timestamp *v2) {
    std::vector<Datum> params(3);

    std::unique_ptr<Scalar> s1 = generateScalar<bool>(false);
    params[0] = CreateDatum<Scalar *>(s1.release());

    std::unique_ptr<Scalar> s2 = generateTimestampScalar(vStr1, v1);
    params[1] = CreateDatum<Scalar *>(s2.release());

    std::unique_ptr<Scalar> s3 = generateTimestampScalar(vStr2, v2);
    params[2] = CreateDatum<Scalar *>(s3.release());

    return std::move(params);
  }

  std::vector<Datum> generateCMPScalarVector(
      const char *vStr1, Timestamp *v1, TypeKind t2,
      const std::vector<std::string> &vectStr,
      std::vector<Timestamp> &vect,  // NOLINT
      const std::vector<bool> &nulls, SelectList *lst) {
    std::vector<Datum> params(3);

    std::unique_ptr<SelectList> s1(new SelectList);
    params[0] = CreateDatum<SelectList *>(s1.release());

    std::unique_ptr<Scalar> s2 = generateTimestampScalar(vStr1, v1);
    params[1] = CreateDatum<Scalar *>(s2.release());

    std::unique_ptr<Vector> s3 =
        generateSelectVector(t2, vectStr, vect, nulls, lst);
    params[2] = CreateDatum<Vector *>(s3.release());

    return std::move(params);
  }

  std::vector<Datum> generateCMPVectorScalar(
      TypeKind t1, const std::vector<std::string> &vectStr,
      std::vector<Timestamp> &vect, const std::vector<bool> &nulls,  // NOLINT
      SelectList *lst, const char *vStr2, Timestamp *v2) {
    std::vector<Datum> params(3);

    std::unique_ptr<SelectList> s1(new SelectList);
    params[0] = CreateDatum<SelectList *>(s1.release());

    std::unique_ptr<Vector> s2 =
        generateSelectVector(t1, vectStr, vect, nulls, lst);
    params[1] = CreateDatum<Vector *>(s2.release());

    std::unique_ptr<Scalar> s3 = generateTimestampScalar(vStr2, v2);
    params[2] = CreateDatum<Scalar *>(s3.release());
    return std::move(params);
  }

  std::vector<Datum> generateCMPVectorVector(
      TypeKind t1, const std::vector<std::string> &vectStr1,
      std::vector<Timestamp> &vect1, const std::vector<bool> &nulls1,  // NOLINT
      SelectList *lst1, TypeKind t2, const std::vector<std::string> &vectStr2,
      std::vector<Timestamp> &vect2, const std::vector<bool> &nulls2,  // NOLINT
      SelectList *lst2) {
    std::vector<Datum> params(3);

    std::unique_ptr<SelectList> s1(new SelectList);
    params[0] = CreateDatum<SelectList *>(s1.release());

    std::unique_ptr<Vector> s2 =
        generateSelectVector(t1, vectStr1, vect1, nulls1, lst1);
    params[1] = CreateDatum<Vector *>(s2.release());

    std::unique_ptr<Vector> s3 =
        generateSelectVector(t2, vectStr2, vect2, nulls2, lst2);
    params[2] = CreateDatum<Vector *>(s3.release());

    return std::move(params);
  }

  std::vector<Datum> generateExtractVector(
      const std::string &field, TypeKind t,
      const std::vector<std::string> &vectStr,
      std::vector<Timestamp> &vect,    // NOLINT
      const std::vector<bool> &nulls,  // NOLINT
      SelectList *lst) {
    std::vector<Datum> params(3);

    std::unique_ptr<DoubleVector> s1(new DoubleVector(true));
    params[0] = CreateDatum<DoubleVector *>(s1.release());

    std::unique_ptr<Scalar> s2(new Scalar);
    s2->value = CreateDatum(field.c_str(), STRINGID);
    params[1] = CreateDatum<Scalar *>(s2.release());

    std::unique_ptr<Vector> s3 =
        generateSelectVector(t, vectStr, vect, nulls, lst);
    params[2] = CreateDatum<Vector *>(s3.release());

    return std::move(params);
  }

 protected:
  const char *valStr0_ = "2018-01-21 15:28:45 BC",
             *valStr1_ = "2018-01-21 15:28:45.01 BC",
             *valStr2_ = "2018-01-21 15:28:44.009 BC";
  Timestamp val0_ = {0, 0}, val1_ = {0, 0}, val2_ = {0, 0};
  std::vector<std::string> vectStr0_ = {
      "2018-01-21 15:28:45 BC", "2018-01-21 15:28:45.01 BC",
      "2018-01-21 15:28:45.01 BC", "2018-01-21 15:28:45",
      "2018-01-21 15:28:45.01"};
  std::vector<Timestamp> vect0_ = {{0, 0}, {0, 0}, {0, 0}, {0, 0}, {0, 0}};
  std::vector<bool> nulls0_ = {false, true, true, false, false};
  SelectList sel0_ = {0, 1, 3};
  std::vector<std::string> vectStr1_ = {
      "2018-01-21 15:28:45 BC", "2018-01-21 15:28:45.01 BC",
      "2018-01-21 15:28:45.01 BC", "2018-01-21 15:28:44.999999999",
      "2018-01-21 15:28:45.001"};
  std::vector<Timestamp> vect1_ = {{0, 0}, {0, 0}, {0, 0}, {0, 0}, {0, 0}};
  std::vector<bool> nulls1_ = {false, false, false, false, false};
  SelectList sel1_ = {0, 1, 2};
};

struct TimestampValCmpValFuncEntry {
  FuncKind funcEntry;
  bool valCmpValEq, valCmpValLt, valCmpValGt;
};
class TimestampFunctionTestValCmpVal
    : public TimestampFunctionTest,
      public ::testing::TestWithParam<TimestampValCmpValFuncEntry> {
 public:
  static const TimestampValCmpValFuncEntry testEntries[];
};
const TimestampValCmpValFuncEntry
    TimestampFunctionTestValCmpVal::testEntries[] = {
        TimestampValCmpValFuncEntry({.funcEntry = TIMESTAMP_EQUAL_TIMESTAMP,
                                     .valCmpValEq = true,
                                     .valCmpValLt = false,
                                     .valCmpValGt = false}),
        TimestampValCmpValFuncEntry({.funcEntry = TIMESTAMP_NOT_EQUAL_TIMESTAMP,
                                     .valCmpValEq = false,
                                     .valCmpValLt = true,
                                     .valCmpValGt = true}),
        TimestampValCmpValFuncEntry({.funcEntry = TIMESTAMP_LESS_THAN_TIMESTAMP,
                                     .valCmpValEq = false,
                                     .valCmpValLt = true,
                                     .valCmpValGt = false}),
        TimestampValCmpValFuncEntry(
            {.funcEntry = TIMESTAMP_GREATER_THAN_TIMESTAMP,
             .valCmpValEq = false,
             .valCmpValLt = false,
             .valCmpValGt = true}),
        TimestampValCmpValFuncEntry({.funcEntry = TIMESTAMP_LESS_EQ_TIMESTAMP,
                                     .valCmpValEq = true,
                                     .valCmpValLt = true,
                                     .valCmpValGt = false}),
        TimestampValCmpValFuncEntry(
            {.funcEntry = TIMESTAMP_GREATER_EQ_TIMESTAMP,
             .valCmpValEq = true,
             .valCmpValLt = false,
             .valCmpValGt = true})};

TEST_P(TimestampFunctionTestValCmpVal, ) {
  auto cmpEntry = GetParam();
  const FuncEntry *funcEntry =
      Func::instance()->getFuncEntryById(cmpEntry.funcEntry);
  {
    std::vector<Datum> params =
        generateCMPScalarScalar(valStr0_, &val0_, valStr0_, &val0_);
    Datum ret = funcEntry->func(params.data(), 3);
    Scalar *scalar = DatumGetValue<Scalar *>(ret);
    EXPECT_EQ(cmpEntry.valCmpValEq, DatumGetValue<bool>(scalar->value));
  }

  {
    std::vector<Datum> params =
        generateCMPScalarScalar(valStr0_, &val0_, valStr1_, &val1_);
    Datum ret = funcEntry->func(params.data(), 3);
    Scalar *scalar = DatumGetValue<Scalar *>(ret);
    EXPECT_EQ(cmpEntry.valCmpValLt, DatumGetValue<bool>(scalar->value));
  }

  {
    std::vector<Datum> params =
        generateCMPScalarScalar(valStr1_, &val1_, valStr2_, &val2_);
    Datum ret = funcEntry->func(params.data(), 3);
    Scalar *scalar = DatumGetValue<Scalar *>(ret);
    EXPECT_EQ(cmpEntry.valCmpValGt, DatumGetValue<bool>(scalar->value));
  }
}

INSTANTIATE_TEST_CASE_P(
    ValCmpVal, TimestampFunctionTestValCmpVal,
    ::testing::ValuesIn(TimestampFunctionTestValCmpVal::testEntries));

struct TimestampVecCmpValFuncEntry {
  FuncKind funcEntry;
  int64_t sizeOfRet, sizeOfRetHasSel, sizeOfRetHasNull, sizeOfRetHasNullAndSel;
};
class TimestampFunctionTestVecCmpVal
    : public TimestampFunctionTest,
      public ::testing::TestWithParam<TimestampVecCmpValFuncEntry> {
 public:
  static const TimestampVecCmpValFuncEntry testEntries[];
};
const TimestampVecCmpValFuncEntry
    TimestampFunctionTestVecCmpVal::testEntries[] = {
        TimestampVecCmpValFuncEntry({.funcEntry = TIMESTAMP_EQUAL_TIMESTAMP,
                                     .sizeOfRet = 2,
                                     .sizeOfRetHasSel = 2,
                                     .sizeOfRetHasNull = 1,
                                     .sizeOfRetHasNullAndSel = 1}),
        TimestampVecCmpValFuncEntry(
            {.funcEntry = TIMESTAMP_NOT_EQUAL_TIMESTAMP,
             .sizeOfRet = 3,
             .sizeOfRetHasSel = 1,
             .sizeOfRetHasNull = 2,  // xxx assume null values is not equal
             .sizeOfRetHasNullAndSel = 1}),
        TimestampVecCmpValFuncEntry({.funcEntry = TIMESTAMP_LESS_THAN_TIMESTAMP,
                                     .sizeOfRet = 1,
                                     .sizeOfRetHasSel = 1,
                                     .sizeOfRetHasNull = 0,
                                     .sizeOfRetHasNullAndSel = 0}),
        TimestampVecCmpValFuncEntry({.funcEntry = TIMESTAMP_LESS_EQ_TIMESTAMP,
                                     .sizeOfRet = 3,
                                     .sizeOfRetHasSel = 3,
                                     .sizeOfRetHasNull = 1,
                                     .sizeOfRetHasNullAndSel = 1}),
        TimestampVecCmpValFuncEntry(
            {.funcEntry = TIMESTAMP_GREATER_THAN_TIMESTAMP,
             .sizeOfRet = 2,
             .sizeOfRetHasSel = 0,
             .sizeOfRetHasNull = 2,
             .sizeOfRetHasNullAndSel = 1}),
        TimestampVecCmpValFuncEntry(
            {.funcEntry = TIMESTAMP_GREATER_EQ_TIMESTAMP,
             .sizeOfRet = 4,
             .sizeOfRetHasSel = 2,
             .sizeOfRetHasNull = 3,
             .sizeOfRetHasNullAndSel = 2}),
};
TEST_P(TimestampFunctionTestVecCmpVal, ) {
  auto cmpEntry = GetParam();
  const FuncEntry *funcEntry =
      Func::instance()->getFuncEntryById(cmpEntry.funcEntry);
  {
    std::vector<Datum> params = generateCMPVectorScalar(
        TIMESTAMPID, vectStr0_, vect0_, nulls0_, &sel0_, valStr0_, &val0_);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRetHasNullAndSel,
              DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }

  {
    std::vector<Datum> params = generateCMPVectorScalar(
        TIMESTAMPID, vectStr0_, vect0_, nulls0_, nullptr, valStr0_, &val0_);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRetHasNull,
              DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }
  // Does not has null
  {
    std::vector<Datum> params = generateCMPVectorScalar(
        TIMESTAMPID, vectStr1_, vect1_, nulls1_, &sel1_, valStr1_, &val1_);
    TimestampVector *vec = DatumGetValue<TimestampVector *>(params[1]);
    vec->setHasNull(false);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRetHasSel,
              DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }
  {
    std::vector<Datum> params = generateCMPVectorScalar(
        TIMESTAMPID, vectStr1_, vect1_, nulls1_, nullptr, valStr1_, &val1_);
    TimestampVector *vec = DatumGetValue<TimestampVector *>(params[1]);
    vec->setHasNull(false);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRet, DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }
}
INSTANTIATE_TEST_CASE_P(
    VecCmpVal, TimestampFunctionTestVecCmpVal,
    ::testing::ValuesIn(TimestampFunctionTestVecCmpVal::testEntries));

struct TimestampValCmpVecFuncEntry {
  FuncKind funcEntry;
  int64_t sizeOfRet, sizeOfRetHasSel, sizeOfRetHasNull, sizeOfRetHasNullAndSel;
};
class TimestampFunctionTestValCmpVec
    : public TimestampFunctionTest,
      public ::testing::TestWithParam<TimestampValCmpVecFuncEntry> {
 public:
  static const TimestampValCmpVecFuncEntry testEntries[];
};
const TimestampValCmpVecFuncEntry
    TimestampFunctionTestValCmpVec::testEntries[] = {
        TimestampValCmpVecFuncEntry({.funcEntry = TIMESTAMP_EQUAL_TIMESTAMP,
                                     .sizeOfRet = 2,
                                     .sizeOfRetHasSel = 2,
                                     .sizeOfRetHasNull = 1,
                                     .sizeOfRetHasNullAndSel = 1}),
        TimestampValCmpVecFuncEntry(
            {.funcEntry = TIMESTAMP_NOT_EQUAL_TIMESTAMP,
             .sizeOfRet = 3,
             .sizeOfRetHasSel = 1,
             .sizeOfRetHasNull = 2,
             // xxx null values is not available to be compared
             .sizeOfRetHasNullAndSel = 1}),
        TimestampValCmpVecFuncEntry(
            {.funcEntry = TIMESTAMP_GREATER_THAN_TIMESTAMP,
             .sizeOfRet = 1,
             .sizeOfRetHasSel = 1,
             .sizeOfRetHasNull = 0,
             .sizeOfRetHasNullAndSel = 0}),
        TimestampValCmpVecFuncEntry(
            {.funcEntry = TIMESTAMP_GREATER_EQ_TIMESTAMP,
             .sizeOfRet = 3,
             .sizeOfRetHasSel = 3,
             .sizeOfRetHasNull = 1,
             .sizeOfRetHasNullAndSel = 1}),
        TimestampValCmpVecFuncEntry({.funcEntry = TIMESTAMP_LESS_THAN_TIMESTAMP,
                                     .sizeOfRet = 2,
                                     .sizeOfRetHasSel = 0,
                                     .sizeOfRetHasNull = 2,
                                     .sizeOfRetHasNullAndSel = 1}),
        TimestampValCmpVecFuncEntry({.funcEntry = TIMESTAMP_LESS_EQ_TIMESTAMP,
                                     .sizeOfRet = 4,
                                     .sizeOfRetHasSel = 2,
                                     .sizeOfRetHasNull = 3,
                                     .sizeOfRetHasNullAndSel = 2}),
};
TEST_P(TimestampFunctionTestValCmpVec, ) {
  auto cmpEntry = GetParam();
  const FuncEntry *funcEntry =
      Func::instance()->getFuncEntryById(cmpEntry.funcEntry);
  {
    std::vector<Datum> params = generateCMPScalarVector(
        valStr0_, &val0_, TIMESTAMPID, vectStr0_, vect0_, nulls0_, &sel0_);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRetHasNullAndSel,
              DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }

  {
    std::vector<Datum> params = generateCMPScalarVector(
        valStr0_, &val0_, TIMESTAMPID, vectStr0_, vect0_, nulls0_, nullptr);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRetHasNull,
              DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }
  // Does not has null
  {
    std::vector<Datum> params = generateCMPScalarVector(
        valStr1_, &val1_, TIMESTAMPID, vectStr1_, vect1_, nulls1_, &sel1_);
    TimestampVector *vec = DatumGetValue<TimestampVector *>(params[2]);
    vec->setHasNull(false);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRetHasSel,
              DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }
  {
    std::vector<Datum> params = generateCMPScalarVector(
        valStr1_, &val1_, TIMESTAMPID, vectStr1_, vect1_, nulls1_, nullptr);
    TimestampVector *vec = DatumGetValue<TimestampVector *>(params[2]);
    vec->setHasNull(false);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRet, DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }
}

INSTANTIATE_TEST_CASE_P(
    ValCmpVec, TimestampFunctionTestValCmpVec,
    ::testing::ValuesIn(TimestampFunctionTestValCmpVec::testEntries));

struct TimestampVecCmpVecFuncEntry {
  FuncKind funcEntry;
  int64_t sizeOfRet;
};
class TimestampFunctionTestVecCmpVec
    : public TimestampFunctionTest,
      public ::testing::TestWithParam<TimestampVecCmpVecFuncEntry> {
 public:
  static const TimestampVecCmpVecFuncEntry testEntries[];
};
const TimestampVecCmpVecFuncEntry
    TimestampFunctionTestVecCmpVec::testEntries[] = {
        TimestampVecCmpVecFuncEntry(
            {.funcEntry = TIMESTAMP_EQUAL_TIMESTAMP, .sizeOfRet = 1}),
        TimestampVecCmpVecFuncEntry(
            {.funcEntry = TIMESTAMP_NOT_EQUAL_TIMESTAMP, .sizeOfRet = 2}),
        TimestampVecCmpVecFuncEntry(
            {.funcEntry = TIMESTAMP_LESS_THAN_TIMESTAMP, .sizeOfRet = 0}),
        TimestampVecCmpVecFuncEntry(
            {.funcEntry = TIMESTAMP_LESS_EQ_TIMESTAMP, .sizeOfRet = 1}),
        TimestampVecCmpVecFuncEntry(
            {.funcEntry = TIMESTAMP_GREATER_THAN_TIMESTAMP, .sizeOfRet = 2}),
        TimestampVecCmpVecFuncEntry(
            {.funcEntry = TIMESTAMP_GREATER_EQ_TIMESTAMP, .sizeOfRet = 3}),
};
TEST_P(TimestampFunctionTestVecCmpVec, ) {
  auto cmpEntry = GetParam();
  const FuncEntry *funcEntry =
      Func::instance()->getFuncEntryById(cmpEntry.funcEntry);
  std::vector<Datum> params =
      generateCMPVectorVector(TIMESTAMPID, vectStr0_, vect0_, nulls0_, nullptr,
                              TIMESTAMPID, vectStr1_, vect1_, nulls1_, nullptr);
  Datum ret = funcEntry->func(params.data(), 3);
  EXPECT_EQ(cmpEntry.sizeOfRet, DatumGetValue<SelectList *>(ret)->size())
      << funcEntry->funcName;
}

INSTANTIATE_TEST_CASE_P(
    VecCmpVec, TimestampFunctionTestVecCmpVec,
    ::testing::ValuesIn(TimestampFunctionTestVecCmpVec::testEntries));

}  // namespace dbcommon
