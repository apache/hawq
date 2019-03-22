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

#include "dbcommon/type/decimal.h"

#include "dbcommon/common/tuple-desc.h"
#include "dbcommon/common/vector.h"
#include "dbcommon/function/date-function.h"
#include "dbcommon/function/typecast-func.cg.h"
#include "dbcommon/testutil/vector-utils.h"

// [[[cog
#if false
from cog import out, outl
import sys, os
print(cog.inFile)
python_path = os.path.dirname(cog.inFile) + "/../../../src/dbcommon/python"
python_path = os.path.abspath(python_path)
sys.path.append(python_path)
from code_generator import *
cog.outl("""
/*
 * DO NOT EDIT!"
 * This file is generated from : %s
 */
""" % cog.inFile)
#endif
// ]]]
// [[[end]]]

namespace dbcommon {

class CodeGenTypecastFunctionTest : public ::testing::Test {
 public:
  CodeGenTypecastFunctionTest() {}
  virtual ~CodeGenTypecastFunctionTest() {}

  template <class T>
  std::unique_ptr<Scalar> generateScalar(T val) {
    std::unique_ptr<Scalar> sc(new Scalar);
    sc->value = CreateDatum<T>(val);
    return std::move(sc);
  }
  template <class T>
  std::unique_ptr<Vector> generateVector(const std::vector<T> &vect,
                                         const std::vector<bool> &nulls) {
    std::unique_ptr<Vector> vec =
        Vector::BuildVector(TypeMapping<T>::type, true);

    for (size_t s = 0; s < vect.size(); s++) {
      Datum d = CreateDatum<T>(vect[s]);
      vec->append(d, nulls[s]);
    }
    return std::move(vec);
  }

  std::unique_ptr<Vector> generateTimestampSelectVector(
      const std::vector<std::string> &vectStr,
      std::vector<Timestamp> &vect,  // NOLINT
      const std::vector<bool> &nulls, SelectList *sel) {
    std::unique_ptr<Vector> result = Vector::BuildVector(TIMESTAMPID, true);

    for (size_t s = 0; s < vect.size(); s++) {
      Datum d = CreateDatum(vectStr[s].c_str(), &vect[s], TIMESTAMPID);
      result->append(d, nulls[s]);
    }

    if (sel) result->setSelected(sel, false);

    return std::move(result);
  }
};

// [[[cog
#if false
def generate_typecast_with_rettype_def(optype, typemap):
  typeList = sorted(typemap.keys())
  for i in range(len(typeList)):
    for j in xrange(len(typeList)):
      rettype = typemap[typeList[j]].symbol
      paramtype = typemap[typeList[i]].symbol
      typekind = typemap[typeList[j]].typekind
      funcImpl = "%s_to_%s" % (typemap[typeList[i]].name, typemap[typeList[j]].name)
      if i != j and not(i == 5 and j == 4):
        cog.outl("""
TEST_F(CodeGenTypecastFunctionTest, %(funcImpl)s) {
  std::vector<Datum> params(2);

  std::vector<%(paramtype)s> src = {-1, 1, 2, 3, 1};
  std::vector<%(rettype)s> dst = {-1, 1, 2, 3, 1};
  std::vector<%(rettype)s> dummy = {0, 0, 0, 0, 0};
  std::vector<bool> nulls = {false, true, true, false, false};

  {
    auto vec = generateVector<%(paramtype)s>(src, nulls);
    auto expectedVect = generateVector<%(rettype)s>(dst, nulls);
    auto rvec = generateVector<%(rettype)s>(dummy, nulls);
    params[0] = CreateDatum<Vector *>(rvec.get());
    params[1] = CreateDatum<Vector *>(vec.get());
    Datum ret = %(funcImpl)s(params.data(), 2);
    Vector *resultVect = DatumGetValue<Vector *>(ret);
    EXPECT_EQ(Vector::equal(*resultVect, *expectedVect), true);
  }

  {
    auto vec = generateVector<%(paramtype)s>(src, nulls);
    vec->setHasNull(false);
    auto expectedVect = generateVector<%(rettype)s>(dst, nulls);
    expectedVect->setHasNull(false);
    auto rvec = generateVector<%(rettype)s>(dummy, nulls);
    params[0] = CreateDatum<Vector *>(rvec.get());
    params[1] = CreateDatum<Vector *>(vec.get());
    Datum ret = %(funcImpl)s(params.data(), 2);
    Vector *resultVect = DatumGetValue<Vector *>(ret);
    EXPECT_EQ(Vector::equal(*resultVect, *expectedVect), true);
  }

  SelectList sel = {0, 1, 3};
  {
    auto vec = generateVector<%(paramtype)s>(src, nulls);
    vec->setSelected(&sel, false);
    auto expectedVect = generateVector<%(rettype)s>(dst, nulls);
    expectedVect->setSelected(&sel, false);
    auto rvec = generateVector<%(rettype)s>(dummy, nulls);
    params[0] = CreateDatum<Vector *>(rvec.get());
    params[1] = CreateDatum<Vector *>(vec.get());
    Datum ret = %(funcImpl)s(params.data(), 2);
    Vector *resultVect = DatumGetValue<Vector *>(ret);
    EXPECT_EQ(Vector::equal(*resultVect, *expectedVect), true);
  }

  {
    auto vec = generateVector<%(paramtype)s>(src, nulls);
    vec->setHasNull(false);
    vec->setSelected(&sel, false);
    auto expectedVect = generateVector<%(rettype)s>(dst, nulls);
    expectedVect->setHasNull(false);
    expectedVect->setSelected(&sel, false);
    auto rvec = generateVector<%(rettype)s>(dummy, nulls);
    params[0] = CreateDatum<Vector *>(rvec.get());
    params[1] = CreateDatum<Vector *>(vec.get());
    Datum ret = %(funcImpl)s(params.data(), 2);
    Vector *resultVect = DatumGetValue<Vector *>(ret);
    EXPECT_EQ(Vector::equal(*resultVect, *expectedVect), true);
  }

  std::unique_ptr<Scalar> scalar = generateScalar<%(paramtype)s>(0);
  params[0] = CreateDatum<Scalar *>(scalar.get());
  params[1] = CreateDatum<Scalar *>(scalar.get());
  %(funcImpl)s(params.data(), 2);
}
""" % {'funcImpl': funcImpl, 'optype': optype, 'paramtype': paramtype, 
    'rettype': rettype, 'typekind': typekind})

#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
generate_typecast_with_rettype_def("TypeCast", NUMERIC_TYPES)
#endif
// ]]]
// [[[end]]]

TEST_F(CodeGenTypecastFunctionTest, DateToTimestamp) {
  std::vector<Datum> params(2);

  std::vector<int32_t> src = {-719528, -719162, 0, 17765, -1455989};
  std::vector<Timestamp> dst = {{0, 0}, {0, 0}, {0, 0}, {0, 0}, {0, 0}};
  std::vector<std::string> dstStr = {
      "0001-01-01 00:00:00 BC", "0001-01-01 00:00:00", "1970-01-01 00:00:00",
      "2018-08-22 00:00:00", "2018-08-22 00:00:00 BC"};
  std::vector<Timestamp> dummy = {{0, 0}, {0, 0}, {0, 0}, {0, 0}, {0, 0}};
  std::vector<std::string> dummyStr = {
      "0000-00-00 00:00:00", "0000-00-00 00:00:00", "0000-00-00 00:00:00",
      "0000-00-00 00:00:00", "0000-00-00 00:00:00"};
  std::vector<bool> nulls = {false, true, true, false, false};

  {
    auto vec = generateVector<int32_t>(src, nulls);
    std::unique_ptr<Vector> expectedVect =
        generateTimestampSelectVector(dstStr, dst, nulls, nullptr);
    std::unique_ptr<Vector> rvec =
        generateTimestampSelectVector(dummyStr, dummy, nulls, nullptr);
    params[0] = CreateDatum<Vector *>(rvec.get());
    params[1] = CreateDatum<Vector *>(vec.get());
    Datum ret = date_to_timestamp(params.data(), 2);
    Vector *resultVect = DatumGetValue<Vector *>(ret);
    EXPECT_EQ(Vector::equal(*resultVect, *expectedVect), true);
  }

  {
    auto vec = generateVector<int32_t>(src, nulls);
    vec->setHasNull(false);
    std::unique_ptr<Vector> expectedVect =
        generateTimestampSelectVector(dstStr, dst, nulls, nullptr);
    expectedVect->setHasNull(false);
    std::unique_ptr<Vector> rvec =
        generateTimestampSelectVector(dummyStr, dummy, nulls, nullptr);
    params[0] = CreateDatum<Vector *>(rvec.get());
    params[1] = CreateDatum<Vector *>(vec.get());
    Datum ret = date_to_timestamp(params.data(), 2);
    Vector *resultVect = DatumGetValue<Vector *>(ret);
    EXPECT_EQ(Vector::equal(*resultVect, *expectedVect), true);
  }

  SelectList sel = {0, 1, 3};
  {
    auto vec = generateVector<int32_t>(src, nulls);
    vec->setSelected(&sel, false);
    std::unique_ptr<Vector> expectedVect =
        generateTimestampSelectVector(dstStr, dst, nulls, nullptr);
    expectedVect->setSelected(&sel, false);
    std::unique_ptr<Vector> rvec =
        generateTimestampSelectVector(dummyStr, dummy, nulls, nullptr);
    params[0] = CreateDatum<Vector *>(rvec.get());
    params[1] = CreateDatum<Vector *>(vec.get());
    Datum ret = date_to_timestamp(params.data(), 2);
    Vector *resultVect = DatumGetValue<Vector *>(ret);
    EXPECT_EQ(Vector::equal(*resultVect, *expectedVect), true);
  }

  {
    auto vec = generateVector<int32_t>(src, nulls);
    vec->setHasNull(false);
    vec->setSelected(&sel, false);
    std::unique_ptr<Vector> expectedVect =
        generateTimestampSelectVector(dstStr, dst, nulls, nullptr);
    expectedVect->setHasNull(false);
    expectedVect->setSelected(&sel, false);
    std::unique_ptr<Vector> rvec =
        generateTimestampSelectVector(dummyStr, dummy, nulls, nullptr);
    params[0] = CreateDatum<Vector *>(rvec.get());
    params[1] = CreateDatum<Vector *>(vec.get());
    Datum ret = date_to_timestamp(params.data(), 2);
    Vector *resultVect = DatumGetValue<Vector *>(ret);
    EXPECT_EQ(Vector::equal(*resultVect, *expectedVect), true);
  }
}

}  // namespace dbcommon
