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
#include "dbcommon/testutil/scalar-utils.h"

namespace dbcommon {

class BinaryFunctionTest {
 public:
  BinaryFunctionTest() : su_(TypeKind::BINARYID) {}

  virtual ~BinaryFunctionTest() {}

  std::unique_ptr<Scalar> generateBinaryScalar(const char *srcStr) {
    std::unique_ptr<Scalar> sc(new Scalar(su_.generateScalar(srcStr)));
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
      vec->append(vect[s], nulls[s]);
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

    std::unique_ptr<Scalar> s2 = generateBinaryScalar(v1);
    params[1] = CreateDatum<Scalar *>(s2.release());

    std::unique_ptr<Scalar> s3 = generateBinaryScalar(v2);
    params[2] = CreateDatum<Scalar *>(s3.release());

    return std::move(params);
  }

  std::vector<Datum> generateCMPScalarVector(
      const char *v1, TypeKind t2, const std::vector<std::string> &vect,
      const std::vector<bool> &nulls, SelectList *lst) {
    std::vector<Datum> params(3);

    std::unique_ptr<SelectList> s1(new SelectList);
    params[0] = CreateDatum<SelectList *>(s1.release());

    std::unique_ptr<Scalar> s2 = generateBinaryScalar(v1);
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

    std::unique_ptr<Scalar> s3 = generateBinaryScalar(v2);
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
  const char *val0_ = "\\000a", *val1_ = "cc", *val2_ = "b\\\173";
  std::vector<std::string> vect0_ = {"\\000a", "cc", "cc", "dd", "ee"};
  std::vector<bool> nulls0_ = {false, true, true, false, false};
  SelectList sel0_ = {0, 1, 3};
  std::vector<std::string> vect1_ = {"\\000a", "cc", "cc", "da", "eb"};
  std::vector<bool> nulls1_ = {false, false, false, false, false};
  SelectList sel1_ = {0, 1, 2};

 private:
  std::vector<std::vector<char>> binaryBuffer;
  ScalarUtility su_;
};

struct BinValCmpValEntry {
  FuncKind funcEntry;
  bool valCmpValEq, valCmpValLt, valCmpValGt;
};
class BinaryFunctionTestValCmpVal
    : public BinaryFunctionTest,
      public ::testing::TestWithParam<BinValCmpValEntry> {
 public:
  static const BinValCmpValEntry testEntries[];
};
const BinValCmpValEntry BinaryFunctionTestValCmpVal::testEntries[] = {
    BinValCmpValEntry({.funcEntry = BINARY_EQUAL_BINARY,
                       .valCmpValEq = true,
                       .valCmpValLt = false,
                       .valCmpValGt = false}),
    BinValCmpValEntry({.funcEntry = BINARY_NOT_EQUAL_BINARY,
                       .valCmpValEq = false,
                       .valCmpValLt = true,
                       .valCmpValGt = true}),
    BinValCmpValEntry({.funcEntry = BINARY_LESS_THAN_BINARY,
                       .valCmpValEq = false,
                       .valCmpValLt = true,
                       .valCmpValGt = false}),
    BinValCmpValEntry({.funcEntry = BINARY_GREATER_THAN_BINARY,
                       .valCmpValEq = false,
                       .valCmpValLt = false,
                       .valCmpValGt = true}),
    BinValCmpValEntry({.funcEntry = BINARY_LESS_EQ_BINARY,
                       .valCmpValEq = true,
                       .valCmpValLt = true,
                       .valCmpValGt = false}),
    BinValCmpValEntry({.funcEntry = BINARY_GREATER_EQ_BINARY,
                       .valCmpValEq = true,
                       .valCmpValLt = false,
                       .valCmpValGt = true})};

TEST_P(BinaryFunctionTestValCmpVal, ) {
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
    ValCmpVal, BinaryFunctionTestValCmpVal,
    ::testing::ValuesIn(BinaryFunctionTestValCmpVal::testEntries));

struct BinVecCmpValEntry {
  FuncKind funcEntry;
  int64_t sizeOfRet, sizeOfRetHasSel, sizeOfRetHasNull, sizeOfRetHasNullAndSel;
};
class BinaryFunctionTestVecCmpVal
    : public BinaryFunctionTest,
      public ::testing::TestWithParam<BinVecCmpValEntry> {
 public:
  static const BinVecCmpValEntry testEntries[];
};
const BinVecCmpValEntry BinaryFunctionTestVecCmpVal::testEntries[] = {
    BinVecCmpValEntry({.funcEntry = BINARY_EQUAL_BINARY,
                       .sizeOfRet = 2,
                       .sizeOfRetHasSel = 2,
                       .sizeOfRetHasNull = 1,
                       .sizeOfRetHasNullAndSel = 1}),
    BinVecCmpValEntry(
        {.funcEntry = BINARY_NOT_EQUAL_BINARY,
         .sizeOfRet = 3,
         .sizeOfRetHasSel = 1,
         .sizeOfRetHasNull = 2,  // xxx assume null values is not equal
         .sizeOfRetHasNullAndSel = 1}),
    BinVecCmpValEntry({.funcEntry = BINARY_LESS_THAN_BINARY,
                       .sizeOfRet = 1,
                       .sizeOfRetHasSel = 1,
                       .sizeOfRetHasNull = 0,
                       .sizeOfRetHasNullAndSel = 0}),
    BinVecCmpValEntry({.funcEntry = BINARY_LESS_EQ_BINARY,
                       .sizeOfRet = 3,
                       .sizeOfRetHasSel = 3,
                       .sizeOfRetHasNull = 1,
                       .sizeOfRetHasNullAndSel = 1}),
    BinVecCmpValEntry({.funcEntry = BINARY_GREATER_THAN_BINARY,
                       .sizeOfRet = 2,
                       .sizeOfRetHasSel = 0,
                       .sizeOfRetHasNull = 2,
                       .sizeOfRetHasNullAndSel = 1}),
    BinVecCmpValEntry({.funcEntry = BINARY_GREATER_EQ_BINARY,
                       .sizeOfRet = 4,
                       .sizeOfRetHasSel = 2,
                       .sizeOfRetHasNull = 3,
                       .sizeOfRetHasNullAndSel = 2}),
};
TEST_P(BinaryFunctionTestVecCmpVal, ) {
  auto cmpEntry = GetParam();
  const FuncEntry *funcEntry =
      Func::instance()->getFuncEntryById(cmpEntry.funcEntry);
  {
    std::vector<Datum> params =
        generateCMPVectorScalar(BINARYID, vect0_, nulls0_, &sel0_, val0_);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRetHasNullAndSel,
              DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }

  {
    std::vector<Datum> params =
        generateCMPVectorScalar(BINARYID, vect0_, nulls0_, nullptr, val0_);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRetHasNull,
              DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }
  // Does not has null
  {
    std::vector<Datum> params =
        generateCMPVectorScalar(BINARYID, vect1_, nulls1_, &sel1_, val1_);
    BinaryVector *vec = DatumGetValue<BinaryVector *>(params[1]);
    vec->setHasNull(false);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRetHasSel,
              DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }
  {
    std::vector<Datum> params =
        generateCMPVectorScalar(BINARYID, vect1_, nulls1_, nullptr, val1_);
    BinaryVector *vec = DatumGetValue<BinaryVector *>(params[1]);
    vec->setHasNull(false);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRet, DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }
}
INSTANTIATE_TEST_CASE_P(
    VecCmpVal, BinaryFunctionTestVecCmpVal,
    ::testing::ValuesIn(BinaryFunctionTestVecCmpVal::testEntries));

struct BinValCmpVecEntry {
  FuncKind funcEntry;
  int64_t sizeOfRet, sizeOfRetHasSel, sizeOfRetHasNull, sizeOfRetHasNullAndSel;
};
class BinaryFunctionTestValCmpVec
    : public BinaryFunctionTest,
      public ::testing::TestWithParam<BinValCmpVecEntry> {
 public:
  static const BinValCmpVecEntry testEntries[];
};
const BinValCmpVecEntry BinaryFunctionTestValCmpVec::testEntries[] = {
    BinValCmpVecEntry({.funcEntry = BINARY_EQUAL_BINARY,
                       .sizeOfRet = 2,
                       .sizeOfRetHasSel = 2,
                       .sizeOfRetHasNull = 1,
                       .sizeOfRetHasNullAndSel = 1}),
    BinValCmpVecEntry({.funcEntry = BINARY_NOT_EQUAL_BINARY,
                       .sizeOfRet = 3,
                       .sizeOfRetHasSel = 1,
                       .sizeOfRetHasNull = 2,
                       // xxx null values is not available to be compared
                       .sizeOfRetHasNullAndSel = 1}),
    BinValCmpVecEntry({.funcEntry = BINARY_GREATER_THAN_BINARY,
                       .sizeOfRet = 1,
                       .sizeOfRetHasSel = 1,
                       .sizeOfRetHasNull = 0,
                       .sizeOfRetHasNullAndSel = 0}),
    BinValCmpVecEntry({.funcEntry = BINARY_GREATER_EQ_BINARY,
                       .sizeOfRet = 3,
                       .sizeOfRetHasSel = 3,
                       .sizeOfRetHasNull = 1,
                       .sizeOfRetHasNullAndSel = 1}),
    BinValCmpVecEntry({.funcEntry = BINARY_LESS_THAN_BINARY,
                       .sizeOfRet = 2,
                       .sizeOfRetHasSel = 0,
                       .sizeOfRetHasNull = 2,
                       .sizeOfRetHasNullAndSel = 1}),
    BinValCmpVecEntry({.funcEntry = BINARY_LESS_EQ_BINARY,
                       .sizeOfRet = 4,
                       .sizeOfRetHasSel = 2,
                       .sizeOfRetHasNull = 3,
                       .sizeOfRetHasNullAndSel = 2}),
};
TEST_P(BinaryFunctionTestValCmpVec, ) {
  auto cmpEntry = GetParam();
  const FuncEntry *funcEntry =
      Func::instance()->getFuncEntryById(cmpEntry.funcEntry);
  {
    std::vector<Datum> params =
        generateCMPScalarVector(val0_, BINARYID, vect0_, nulls0_, &sel0_);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRetHasNullAndSel,
              DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }

  {
    std::vector<Datum> params =
        generateCMPScalarVector(val0_, BINARYID, vect0_, nulls0_, nullptr);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRetHasNull,
              DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }
  // Does not has null
  {
    std::vector<Datum> params =
        generateCMPScalarVector(val1_, BINARYID, vect1_, nulls1_, &sel1_);
    BinaryVector *vec = DatumGetValue<BinaryVector *>(params[2]);
    vec->setHasNull(false);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRetHasSel,
              DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }
  {
    std::vector<Datum> params =
        generateCMPScalarVector(val1_, BINARYID, vect1_, nulls1_, nullptr);
    BinaryVector *vec = DatumGetValue<BinaryVector *>(params[2]);
    vec->setHasNull(false);
    Datum ret = funcEntry->func(params.data(), 3);
    EXPECT_EQ(cmpEntry.sizeOfRet, DatumGetValue<SelectList *>(ret)->size())
        << funcEntry->funcName;
  }
}

INSTANTIATE_TEST_CASE_P(
    ValCmpVec, BinaryFunctionTestValCmpVec,
    ::testing::ValuesIn(BinaryFunctionTestValCmpVec::testEntries));

struct BinVecCmpVecEntry {
  FuncKind funcEntry;
  int64_t sizeOfRet;
};
class BinaryFunctionTestVecCmpVecCmpEntry
    : public BinaryFunctionTest,
      public ::testing::TestWithParam<BinVecCmpVecEntry> {
 public:
  static const BinVecCmpVecEntry testEntries[];
};
const BinVecCmpVecEntry BinaryFunctionTestVecCmpVecCmpEntry::testEntries[] = {
    BinVecCmpVecEntry({.funcEntry = BINARY_EQUAL_BINARY, .sizeOfRet = 1}),
    BinVecCmpVecEntry({.funcEntry = BINARY_NOT_EQUAL_BINARY, .sizeOfRet = 2}),
    BinVecCmpVecEntry({.funcEntry = BINARY_LESS_THAN_BINARY, .sizeOfRet = 0}),
    BinVecCmpVecEntry({.funcEntry = BINARY_LESS_EQ_BINARY, .sizeOfRet = 1}),
    BinVecCmpVecEntry(
        {.funcEntry = BINARY_GREATER_THAN_BINARY, .sizeOfRet = 2}),
    BinVecCmpVecEntry({.funcEntry = BINARY_GREATER_EQ_BINARY, .sizeOfRet = 3}),
};
TEST_P(BinaryFunctionTestVecCmpVecCmpEntry, ) {
  auto cmpEntry = GetParam();
  const FuncEntry *funcEntry =
      Func::instance()->getFuncEntryById(cmpEntry.funcEntry);
  std::vector<Datum> params = generateCMPVectorVector(
      BINARYID, vect0_, nulls0_, nullptr, BINARYID, vect1_, nulls1_, nullptr);
  Datum ret = funcEntry->func(params.data(), 3);
  EXPECT_EQ(cmpEntry.sizeOfRet, DatumGetValue<SelectList *>(ret)->size())
      << funcEntry->funcName;
}

INSTANTIATE_TEST_CASE_P(
    VecCmpVec, BinaryFunctionTestVecCmpVecCmpEntry,
    ::testing::ValuesIn(BinaryFunctionTestVecCmpVecCmpEntry::testEntries));
}  // namespace dbcommon
