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

#include "dbcommon/common/vector/variable-length-vector.h"
#include "dbcommon/function/arith-cmp-func.cg.h"
#include "dbcommon/function/func-kind.cg.h"
#include "dbcommon/function/func.h"
#include "dbcommon/nodes/select-list.h"

namespace dbcommon {

class StringFunctionTest {
 public:
  StringFunctionTest() {}

  virtual ~StringFunctionTest() {}

  std::unique_ptr<Scalar> generateStringScalar(const char *val) {
    std::unique_ptr<Scalar> sc(new Scalar);
    sc->value = CreateDatum(val, STRINGID);
    sc->length = strlen(val);
    return std::move(sc);
  }

  template <class T>
  std::unique_ptr<Scalar> generateScalar(T val) {
    std::unique_ptr<Scalar> sc(new Scalar);
    sc->value = CreateDatum<T>(val);
    return std::move(sc);
  }

  std::unique_ptr<Vector> generateVector(TypeKind type,
                                         const std::vector<std::string> &vect,
                                         const std::vector<bool> &nulls) {
    std::unique_ptr<Vector> vec = Vector::BuildVector(type, true);

    for (size_t s = 0; s < vect.size(); s++) {
      Datum d = CreateDatum(vect[s].c_str(), STRINGID);
      vec->append(d, nulls[s]);
    }
    return std::move(vec);
  }

  std::unique_ptr<Vector> generateSelectVector(
      TypeKind type, const std::vector<std::string> &vect,
      const std::vector<bool> &nulls, SelectList *sel) {
    std::unique_ptr<Vector> result = generateVector(type, vect, nulls);

    if (sel) result->setSelected(sel, false);

    return std::move(result);
  }

  std::vector<Datum> generateCMPScalarScalar(const char *v1, const char *v2) {
    std::vector<Datum> params(3);

    std::unique_ptr<Scalar> s1 = generateScalar<bool>(false);
    params[0] = CreateDatum<Scalar *>(s1.release());

    std::unique_ptr<Scalar> s2 = generateStringScalar(v1);
    params[1] = CreateDatum<Scalar *>(s2.release());

    std::unique_ptr<Scalar> s3 = generateStringScalar(v2);
    params[2] = CreateDatum<Scalar *>(s3.release());

    return std::move(params);
  }

  std::vector<Datum> generateCMPScalarVector(
      const char *v1, TypeKind t2, const std::vector<std::string> &vect,
      const std::vector<bool> &nulls, SelectList *lst) {
    std::vector<Datum> params(3);

    std::unique_ptr<SelectList> s1(new SelectList);
    params[0] = CreateDatum<SelectList *>(s1.release());

    std::unique_ptr<Scalar> s2 = generateStringScalar(v1);
    params[1] = CreateDatum<Scalar *>(s2.release());

    std::unique_ptr<Vector> s3 = generateSelectVector(t2, vect, nulls, lst);
    params[2] = CreateDatum<Vector *>(s3.release());

    return std::move(params);
  }

  std::vector<Datum> generateCMPVectorScalar(
      TypeKind t1, const std::vector<std::string> &vect,
      const std::vector<bool> &nulls, SelectList *lst, const char *v2) {
    std::vector<Datum> params(3);

    std::unique_ptr<SelectList> s1(new SelectList);
    params[0] = CreateDatum<SelectList *>(s1.release());

    std::unique_ptr<Vector> s2 = generateSelectVector(t1, vect, nulls, lst);
    params[1] = CreateDatum<Vector *>(s2.release());

    std::unique_ptr<Scalar> s3 = generateStringScalar(v2);
    params[2] = CreateDatum<Scalar *>(s3.release());
    return std::move(params);
  }

  std::vector<Datum> generateCMPVectorVector(
      TypeKind t1, const std::vector<std::string> &vect1,
      const std::vector<bool> &nulls1, SelectList *lst1, TypeKind t2,
      const std::vector<std::string> &vect2, const std::vector<bool> &nulls2,
      SelectList *lst2) {
    std::vector<Datum> params(3);

    std::unique_ptr<SelectList> s1(new SelectList);
    params[0] = CreateDatum<SelectList *>(s1.release());

    std::unique_ptr<Vector> s2 = generateSelectVector(t1, vect1, nulls1, lst1);
    params[1] = CreateDatum<Vector *>(s2.release());

    std::unique_ptr<Vector> s3 = generateSelectVector(t2, vect2, nulls2, lst2);
    params[2] = CreateDatum<Vector *>(s3.release());

    return std::move(params);
  }

 protected:
  const char *val0_ = "aa", *val1_ = "cc", *val2_ = "bs";
  std::vector<std::string> vect0_ = {"aa", "cc", "cc", "dd", "ee"};
  std::vector<bool> nulls0_ = {false, true, true, false, false};
  SelectList sel0_ = {0, 1, 3};
  std::vector<std::string> vect1_ = {"aa", "cc", "cc", "da", "eb"};
  std::vector<bool> nulls1_ = {false, false, false, false, false};
  SelectList sel1_ = {0, 1, 2};
};

struct StrValCmpValEntry {
  FuncKind funcEntry;
  bool valCmpValEq, valCmpValLt, valCmpValGt;
};
class StringFunctionTestValCmpVal
    : public StringFunctionTest,
      public ::testing::TestWithParam<StrValCmpValEntry> {
 public:
  static const StrValCmpValEntry testEntries[];
};
const StrValCmpValEntry StringFunctionTestValCmpVal::testEntries[] = {
    StrValCmpValEntry({.funcEntry = STRING_EQUAL_STRING,
                       .valCmpValEq = true,
                       .valCmpValLt = false,
                       .valCmpValGt = false}),
    StrValCmpValEntry({.funcEntry = STRING_NOT_EQUAL_STRING,
                       .valCmpValEq = false,
                       .valCmpValLt = true,
                       .valCmpValGt = true}),
    StrValCmpValEntry({.funcEntry = STRING_LESS_THAN_STRING,
                       .valCmpValEq = false,
                       .valCmpValLt = true,
                       .valCmpValGt = false}),
    StrValCmpValEntry({.funcEntry = STRING_GREATER_THAN_STRING,
                       .valCmpValEq = false,
                       .valCmpValLt = false,
                       .valCmpValGt = true}),
    StrValCmpValEntry({.funcEntry = STRING_LESS_EQ_STRING,
                       .valCmpValEq = true,
                       .valCmpValLt = true,
                       .valCmpValGt = false}),
    StrValCmpValEntry({.funcEntry = STRING_GREATER_EQ_STRING,
                       .valCmpValEq = true,
                       .valCmpValLt = false,
                       .valCmpValGt = true})};

TEST_P(StringFunctionTestValCmpVal, ) {
  auto cmpEntry = GetParam();
  const FuncEntry *funcEntry =
      Func::instance()->getFuncEntryById(cmpEntry.funcEntry);
  {
    std::vector<Datum> params = generateCMPScalarScalar(val0_, val0_);
    Datum ret = funcEntry->func(params.data(), 3);
    Scalar *scalar = DatumGetValue<Scalar *>(ret);
    EXPECT_EQ(cmpEntry.valCmpValEq, DatumGetValue<bool>(scalar->value));
  }

  {
    std::vector<Datum> params = generateCMPScalarScalar(val0_, val1_);
    Datum ret = funcEntry->func(params.data(), 3);
    Scalar *scalar = DatumGetValue<Scalar *>(ret);
    EXPECT_EQ(cmpEntry.valCmpValLt, DatumGetValue<bool>(scalar->value));
  }

  {
    std::vector<Datum> params = generateCMPScalarScalar(val1_, val2_);
    Datum ret = funcEntry->func(params.data(), 3);
    Scalar *scalar = DatumGetValue<Scalar *>(ret);
    EXPECT_EQ(cmpEntry.valCmpValGt, DatumGetValue<bool>(scalar->value));
  }
}

INSTANTIATE_TEST_CASE_P(
    ValCmpVal, StringFunctionTestValCmpVal,
    ::testing::ValuesIn(StringFunctionTestValCmpVal::testEntries));

struct StrVecCmpValEntry {
  FuncKind funcEntry;
  int64_t sizeOfRet, sizeOfRetHasSel, sizeOfRetHasNull, sizeOfRetHasNullAndSel;
};
class StringFunctionTestVecCmpVal
    : public StringFunctionTest,
      public ::testing::TestWithParam<StrVecCmpValEntry> {
 public:
  static const StrVecCmpValEntry testEntries[];
};
const StrVecCmpValEntry StringFunctionTestVecCmpVal::testEntries[] = {
    StrVecCmpValEntry({.funcEntry = STRING_EQUAL_STRING,
                       .sizeOfRet = 2,
                       .sizeOfRetHasSel = 2,
                       .sizeOfRetHasNull = 1,
                       .sizeOfRetHasNullAndSel = 1}),
    StrVecCmpValEntry(
        {.funcEntry = STRING_NOT_EQUAL_STRING,
         .sizeOfRet = 3,
         .sizeOfRetHasSel = 1,
         .sizeOfRetHasNull = 2,  // xxx assume null values is not equal
         .sizeOfRetHasNullAndSel = 1}),
    StrVecCmpValEntry({.funcEntry = STRING_LESS_THAN_STRING,
                       .sizeOfRet = 1,
                       .sizeOfRetHasSel = 1,
                       .sizeOfRetHasNull = 0,
                       .sizeOfRetHasNullAndSel = 0}),
    StrVecCmpValEntry({.funcEntry = STRING_LESS_EQ_STRING,
                       .sizeOfRet = 3,
                       .sizeOfRetHasSel = 3,
                       .sizeOfRetHasNull = 1,
                       .sizeOfRetHasNullAndSel = 1}),
    StrVecCmpValEntry({.funcEntry = STRING_GREATER_THAN_STRING,
                       .sizeOfRet = 2,
                       .sizeOfRetHasSel = 0,
                       .sizeOfRetHasNull = 2,
                       .sizeOfRetHasNullAndSel = 1}),
    StrVecCmpValEntry({.funcEntry = STRING_GREATER_EQ_STRING,
                       .sizeOfRet = 4,
                       .sizeOfRetHasSel = 2,
                       .sizeOfRetHasNull = 3,
                       .sizeOfRetHasNullAndSel = 2}),
};
TEST_P(StringFunctionTestVecCmpVal, ) {
  auto cmpEntry = GetParam();
  const FuncEntry *funcEntry =
      Func::instance()->getFuncEntryById(cmpEntry.funcEntry);
  {
    std::vector<Datum> params =
        generateCMPVectorScalar(STRINGID, vect0_, nulls0_, &sel0_, val0_);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRetHasNullAndSel,
              DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }

  {
    std::vector<Datum> params =
        generateCMPVectorScalar(STRINGID, vect0_, nulls0_, nullptr, val0_);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRetHasNull,
              DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }
  // Does not has null
  {
    std::vector<Datum> params =
        generateCMPVectorScalar(STRINGID, vect1_, nulls1_, &sel1_, val1_);
    StringVector *vec = DatumGetValue<StringVector *>(params[1]);
    vec->setHasNull(false);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRetHasSel,
              DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }
  {
    std::vector<Datum> params =
        generateCMPVectorScalar(STRINGID, vect1_, nulls1_, nullptr, val1_);
    StringVector *vec = DatumGetValue<StringVector *>(params[1]);
    vec->setHasNull(false);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRet, DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }
}
INSTANTIATE_TEST_CASE_P(
    VecCmpVal, StringFunctionTestVecCmpVal,
    ::testing::ValuesIn(StringFunctionTestVecCmpVal::testEntries));

struct StrValCmpVecEntry {
  FuncKind funcEntry;
  int64_t sizeOfRet, sizeOfRetHasSel, sizeOfRetHasNull, sizeOfRetHasNullAndSel;
};
class StringFunctionTestValCmpVec
    : public StringFunctionTest,
      public ::testing::TestWithParam<StrValCmpVecEntry> {
 public:
  static const StrValCmpVecEntry testEntries[];
};
const StrValCmpVecEntry StringFunctionTestValCmpVec::testEntries[] = {
    StrValCmpVecEntry({.funcEntry = STRING_EQUAL_STRING,
                       .sizeOfRet = 2,
                       .sizeOfRetHasSel = 2,
                       .sizeOfRetHasNull = 1,
                       .sizeOfRetHasNullAndSel = 1}),
    StrValCmpVecEntry({.funcEntry = STRING_NOT_EQUAL_STRING,
                       .sizeOfRet = 3,
                       .sizeOfRetHasSel = 1,
                       .sizeOfRetHasNull = 2,
                       // xxx null values is not available to be compared
                       .sizeOfRetHasNullAndSel = 1}),
    StrValCmpVecEntry({.funcEntry = STRING_GREATER_THAN_STRING,
                       .sizeOfRet = 1,
                       .sizeOfRetHasSel = 1,
                       .sizeOfRetHasNull = 0,
                       .sizeOfRetHasNullAndSel = 0}),
    StrValCmpVecEntry({.funcEntry = STRING_GREATER_EQ_STRING,
                       .sizeOfRet = 3,
                       .sizeOfRetHasSel = 3,
                       .sizeOfRetHasNull = 1,
                       .sizeOfRetHasNullAndSel = 1}),
    StrValCmpVecEntry({.funcEntry = STRING_LESS_THAN_STRING,
                       .sizeOfRet = 2,
                       .sizeOfRetHasSel = 0,
                       .sizeOfRetHasNull = 2,
                       .sizeOfRetHasNullAndSel = 1}),
    StrValCmpVecEntry({.funcEntry = STRING_LESS_EQ_STRING,
                       .sizeOfRet = 4,
                       .sizeOfRetHasSel = 2,
                       .sizeOfRetHasNull = 3,
                       .sizeOfRetHasNullAndSel = 2}),
};
TEST_P(StringFunctionTestValCmpVec, ) {
  auto cmpEntry = GetParam();
  const FuncEntry *funcEntry =
      Func::instance()->getFuncEntryById(cmpEntry.funcEntry);
  {
    std::vector<Datum> params =
        generateCMPScalarVector(val0_, STRINGID, vect0_, nulls0_, &sel0_);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRetHasNullAndSel,
              DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }

  {
    std::vector<Datum> params =
        generateCMPScalarVector(val0_, STRINGID, vect0_, nulls0_, nullptr);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRetHasNull,
              DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }
  // Does not has null
  {
    std::vector<Datum> params =
        generateCMPScalarVector(val1_, STRINGID, vect1_, nulls1_, &sel1_);
    StringVector *vec = DatumGetValue<StringVector *>(params[2]);
    vec->setHasNull(false);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRetHasSel,
              DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }
  {
    std::vector<Datum> params =
        generateCMPScalarVector(val1_, STRINGID, vect1_, nulls1_, nullptr);
    StringVector *vec = DatumGetValue<StringVector *>(params[2]);
    vec->setHasNull(false);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRet, DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }
}

INSTANTIATE_TEST_CASE_P(
    ValCmpVec, StringFunctionTestValCmpVec,
    ::testing::ValuesIn(StringFunctionTestValCmpVec::testEntries));

struct StrVecCmpVecEntry {
  FuncKind funcEntry;
  int64_t sizeOfRet;
};
class StringFunctionTestVecCmpVec
    : public StringFunctionTest,
      public ::testing::TestWithParam<StrVecCmpVecEntry> {
 public:
  static const StrVecCmpVecEntry testEntries[];
};
const StrVecCmpVecEntry StringFunctionTestVecCmpVec::testEntries[] = {
    StrVecCmpVecEntry({.funcEntry = STRING_EQUAL_STRING, .sizeOfRet = 1}),
    StrVecCmpVecEntry({.funcEntry = STRING_NOT_EQUAL_STRING, .sizeOfRet = 2}),
    StrVecCmpVecEntry({.funcEntry = STRING_LESS_THAN_STRING, .sizeOfRet = 0}),
    StrVecCmpVecEntry({.funcEntry = STRING_LESS_EQ_STRING, .sizeOfRet = 1}),
    StrVecCmpVecEntry(
        {.funcEntry = STRING_GREATER_THAN_STRING, .sizeOfRet = 2}),
    StrVecCmpVecEntry({.funcEntry = STRING_GREATER_EQ_STRING, .sizeOfRet = 3}),
};
TEST_P(StringFunctionTestVecCmpVec, ) {
  auto cmpEntry = GetParam();
  const FuncEntry *funcEntry =
      Func::instance()->getFuncEntryById(cmpEntry.funcEntry);
  std::vector<Datum> params = generateCMPVectorVector(
      STRINGID, vect0_, nulls0_, nullptr, STRINGID, vect1_, nulls1_, nullptr);
  Datum ret = funcEntry->func(params.data(), 3);
  EXPECT_EQ(cmpEntry.sizeOfRet, DatumGetValue<SelectList *>(ret)->size())
      << funcEntry->funcName;
}

INSTANTIATE_TEST_CASE_P(
    VecCmpVec, StringFunctionTestVecCmpVec,
    ::testing::ValuesIn(StringFunctionTestVecCmpVec::testEntries));
}  // namespace dbcommon
