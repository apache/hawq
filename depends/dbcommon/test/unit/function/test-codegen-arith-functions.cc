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

#include <map>

#include "dbcommon/common/tuple-desc.h"
#include "dbcommon/common/vector.h"
#include "dbcommon/function/arith-cmp-func.cg.h"
#include "dbcommon/testutil/scalar-utils.h"
#include "dbcommon/testutil/vector-utils.h"
#include "gtest/gtest.h"

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

class CodeGenArithFunctionTest : public ::testing::Test {
 public:
  CodeGenArithFunctionTest() {}
  virtual ~CodeGenArithFunctionTest() {}
  template <class T>
  std::unique_ptr<Scalar> generateScalar(TypeKind typekind, T val) {
    if (scalarUtilitys_[typekind] == nullptr)
      scalarUtilitys_[typekind].reset(new ScalarUtility(typekind));
    std::unique_ptr<Scalar> sc(new Scalar);
    *sc = scalarUtilitys_[typekind]->generateScalar(std::to_string(val));
    return std::move(sc);
  }

  template <class T>
  std::unique_ptr<Vector> generateVector(TypeKind typekind,
                                         const std::vector<T> &vect,
                                         const std::vector<bool> &nulls) {
    std::unique_ptr<Vector> vec = Vector::BuildVector(typekind, true);

    if (FLOATID != typekind && DOUBLEID != typekind) {
      for (size_t s = 0; s < vect.size(); s++) {
        vec->append(std::to_string(vect[s]), nulls[s]);
      }
    } else {
      for (size_t s = 0; s < vect.size(); s++) {
        Datum d = CreateDatum<T>(vect[s]);
        vec->append(d, nulls[s]);
      }
    }
    return std::move(vec);
  }

  template <class T>
  std::unique_ptr<Vector> generateSelectVector(TypeKind type,
                                               const std::vector<T> &vect,
                                               const std::vector<bool> &nulls,
                                               SelectList *sel) {
    std::unique_ptr<Vector> result = generateVector(type, vect, nulls);

    if (sel) result->setSelected(sel, false);

    return std::move(result);
  }

  template <class T1, class T2>
  std::vector<Datum> generateArthScalarScalar(TypeKind t1, T1 v1, TypeKind t2,
                                              T2 v2) {
    std::vector<Datum> params(3);

    std::unique_ptr<Scalar> s1(new Scalar);
    params[0] = CreateDatum<Scalar *>(s1.release());

    std::unique_ptr<Scalar> s2 = generateScalar<T1>(t1, v1);
    params[1] = CreateDatum<Scalar *>(s2.release());

    std::unique_ptr<Scalar> s3 = generateScalar<T2>(t2, v2);
    params[2] = CreateDatum<Scalar *>(s3.release());

    return std::move(params);
  }

  template <class T1, class T2, class RT>
  std::vector<Datum> generateArthScalarVector(TypeKind t1, T1 v1, TypeKind t2,
                                              const std::vector<T2> &vect,
                                              const std::vector<bool> &nulls,
                                              SelectList *lst, TypeKind trt) {
    std::vector<Datum> params(3);

    std::unique_ptr<Vector> s1(Vector::BuildVector(trt, true));
    params[0] = CreateDatum<Vector *>(s1.release());

    std::unique_ptr<Scalar> s2 = generateScalar<T1>(t1, v1);
    params[1] = CreateDatum<Scalar *>(s2.release());

    std::unique_ptr<Vector> s3 = generateSelectVector<T2>(t2, vect, nulls, lst);
    params[2] = CreateDatum<Vector *>(s3.release());

    return std::move(params);
  }

  template <class T1, class T2, class RT>
  std::vector<Datum> generateArthVectorScalar(TypeKind t1,
                                              const std::vector<T1> &vect,
                                              const std::vector<bool> &nulls,
                                              SelectList *lst, TypeKind t2,
                                              T2 v2, TypeKind trt) {
    std::vector<Datum> params(3);

    std::unique_ptr<Vector> s1(Vector::BuildVector(trt, true));
    params[0] = CreateDatum<Vector *>(s1.release());

    std::unique_ptr<Vector> s2 = generateSelectVector<T1>(t1, vect, nulls, lst);
    params[1] = CreateDatum<Vector *>(s2.release());

    std::unique_ptr<Scalar> s3 = generateScalar<T2>(t2, v2);
    params[2] = CreateDatum<Scalar *>(s3.release());

    return std::move(params);
  }

  template <class T1, class T2, class RT>
  std::vector<Datum> generateArthVectorVector(TypeKind t1,
                                              const std::vector<T1> &vect1,
                                              const std::vector<bool> &nulls1,
                                              SelectList *lst1, TypeKind t2,
                                              const std::vector<T2> &vect2,
                                              const std::vector<bool> &nulls2,
                                              SelectList *lst2, TypeKind trt) {
    std::vector<Datum> params(3);

    std::unique_ptr<Vector> s1(Vector::BuildVector(trt, true));
    params[0] = CreateDatum<Vector *>(s1.release());

    std::unique_ptr<Vector> s2 =
        generateSelectVector<T1>(t1, vect1, nulls1, lst1);
    params[1] = CreateDatum<Vector *>(s2.release());

    std::unique_ptr<Vector> s3 =
        generateSelectVector<T2>(t2, vect2, nulls2, lst2);
    params[2] = CreateDatum<Vector *>(s3.release());

    return std::move(params);
  }

 private:
  std::map<TypeKind, std::unique_ptr<ScalarUtility>> scalarUtilitys_;
};

// [[[cog test on val arith val
#if false
def generate_test_function_on_arith(optype, opmap, ltypemap, rtypemap) :
  for op_key in opmap:
    for ta in ltypemap:
      for tb in rtypemap:
        rettype = get_func_return_type(ta, tb, op_key)
        cog.outl("""
TEST_F(CodeGenArithFunctionTest, %(t1)s_val_%(op)s_%(t2)s_val) {
  {
    std::vector<Datum> params = generateArthScalarScalar<%(t1_type)s, %(t2_type)s>(%(t1_typekind)s, 2, %(t2_typekind)s, 1);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    auto expected = generateScalar<%(rt_type)s>(%(trt_typekind)s, (%(rt_type)s)(2 %(opsym)s 1));
    if (DECIMALNEWID == %(t1_typekind)s) {
      // FIXME(chiyang): Determine the Scalar form for decimal type
      // EXPECT_EQ(DatumGetValue<Scalar *>(ret)->length, expected->length);
    } else {
      EXPECT_EQ(DatumGetValue<Scalar *>(ret)->value.value.i64, expected->value.value.i64);
    }
  }
}

TEST_F(CodeGenArithFunctionTest, %(t1)s_val_%(op)s_%(t2)s_vec) {
  %(t1_type)s val = 4;
  std::vector<%(t2_type)s> vect = {-1, 1, 2, 3, 1};
  std::vector<bool> nulls = {false, true, true, false, false};
  SelectList sel = {0, 1, 3};
  {
    std::vector<Datum> params =
        generateArthScalarVector<%(t1_type)s, %(t2_type)s, %(rt_type)s>(%(t1_typekind)s, val, %(t2_typekind)s, vect, nulls, nullptr, %(trt_typekind)s);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    Vector *resultVect = DatumGetValue<Vector *>(ret);

    std::vector<%(rt_type)s> expected(vect.size());
    std::vector<bool> expectedNulls(expected.size());
    for (int i = 0; i < vect.size(); i++) {
      expected[i] = val %(opsym)s vect[i];
      expectedNulls[i] = nulls[i];
    }

    std::unique_ptr<Vector> expectedVect =
        generateVector<%(rt_type)s>(%(trt_typekind)s, expected, nulls);
    EXPECT_EQ(expectedVect->toString(), resultVect->toString());
  }

   {
     std::vector<Datum> params =
         generateArthScalarVector<%(t1_type)s, %(t2_type)s, %(rt_type)s>(%(t1_typekind)s, val, %(t2_typekind)s, vect, nulls, &sel, %(trt_typekind)s);
     Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
     Vector *resultVect = DatumGetValue<Vector *>(ret);

     std::vector<%(rt_type)s> expected(sel.size());
     std::vector<bool> expectedNulls(expected.size());
     for (int i = 0; i < sel.size(); i++) {
       expected[i] = val %(opsym)s vect[sel[i]];
       expectedNulls[i] = nulls[sel[i]];
     }

     std::unique_ptr<Vector> expectedVect =
         generateVector<%(rt_type)s>(%(trt_typekind)s, expected, expectedNulls);
     EXPECT_EQ(expectedVect->toString(), resultVect->toString());
   }

  std::fill(nulls.begin(), nulls.end(), false);
  {
    std::vector<Datum> params =
        generateArthScalarVector<%(t1_type)s, %(t2_type)s, %(rt_type)s>(%(t1_typekind)s, val, %(t2_typekind)s, vect, nulls, nullptr, %(trt_typekind)s);

    DatumGetValue<Vector *>(params[2])->setHasNull(false);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    Vector *resultVect = DatumGetValue<Vector *>(ret);

    std::vector<%(rt_type)s> expected(vect.size());
    std::vector<bool> expectedNulls(expected.size());
    for (int i = 0; i < vect.size(); i++) {
      expected[i] = val %(opsym)s vect[i];
      expectedNulls[i] = nulls[i];
    }

    std::unique_ptr<Vector> expectedVect =
        generateVector<%(rt_type)s>(%(trt_typekind)s, expected, expectedNulls);
    expectedVect->setHasNull(false);
    EXPECT_EQ(expectedVect->toString(), resultVect->toString());
  }

  {
    std::vector<Datum> params =
        generateArthScalarVector<%(t1_type)s, %(t2_type)s, %(rt_type)s>(%(t1_typekind)s, val, %(t2_typekind)s, vect, nulls, &sel, %(trt_typekind)s);
    DatumGetValue<Vector *>(params[2])->setHasNull(false);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    Vector *resultVect = DatumGetValue<Vector *>(ret);

    std::vector<%(rt_type)s> expected(sel.size());
    std::vector<bool> expectedNulls(expected.size());
    for (int i = 0; i < sel.size(); i++) {
      expected[i] = val %(opsym)s vect[sel[i]];
      expectedNulls[i] = nulls[sel[i]];
    }

    std::unique_ptr<Vector> expectedVect =
        generateVector<%(rt_type)s>(%(trt_typekind)s, expected, expectedNulls);
    expectedVect->setHasNull(false);
    EXPECT_EQ(expectedVect->toString(), resultVect->toString());
  }
}

TEST_F(CodeGenArithFunctionTest, %(t1)s_vec_%(op)s_%(t2)s_val) {
  std::vector<%(t1_type)s> vect = {-1, 1, 2, 3, 1};
  %(t2_type)s val = 11;
  std::vector<bool> nulls = {false, true, true, false, false};
  SelectList sel = {0, 1, 3};
  {
    std::vector<Datum> params =
        generateArthVectorScalar<%(t1_type)s, %(t2_type)s, %(rt_type)s>(%(t1_typekind)s, vect, nulls, nullptr, %(t2_typekind)s, val, %(trt_typekind)s);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    Vector *resultVect = DatumGetValue<Vector *>(ret);

    std::vector<%(rt_type)s> expected(vect.size());
    std::vector<bool> expectedNulls(expected.size());
    for (int i = 0; i < vect.size(); i++) {
      expected[i] = vect[i] %(opsym)s val;
      expectedNulls[i] = nulls[i];
    }

    std::unique_ptr<Vector> expectedVect =
        generateVector<%(rt_type)s>(%(trt_typekind)s, expected, nulls);
    EXPECT_EQ(expectedVect->toString(), resultVect->toString());
  }

  {
    std::vector<Datum> params =
        generateArthVectorScalar<%(t1_type)s, %(t2_type)s, %(rt_type)s>(%(t1_typekind)s, vect, nulls, &sel, %(t2_typekind)s, val, %(trt_typekind)s);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    Vector *resultVect = DatumGetValue<Vector *>(ret);

    std::vector<%(rt_type)s> expected(sel.size());
    std::vector<bool> expectedNulls(expected.size());
    for (int i = 0; i < sel.size(); i++) {
      expected[i] = vect[sel[i]] %(opsym)s val;
      expectedNulls[i] = nulls[sel[i]];
    }
    std::unique_ptr<Vector> expectedVect =
        generateVector<%(rt_type)s>(%(trt_typekind)s, expected, expectedNulls);
    EXPECT_EQ(expectedVect->toString(), resultVect->toString());
  }

  std::fill(nulls.begin(), nulls.end(), false);
  {
    std::vector<Datum> params =
        generateArthVectorScalar<%(t1_type)s, %(t2_type)s, %(rt_type)s>(%(t1_typekind)s, vect, nulls, nullptr, %(t2_typekind)s, val, %(trt_typekind)s);
    DatumGetValue<Vector *>(params[1])->setHasNull(false);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    Vector *resultVect = DatumGetValue<Vector *>(ret);

    std::vector<%(rt_type)s> expected(vect.size());
    std::vector<bool> expectedNulls(expected.size());
    for (int i = 0; i < vect.size(); i++) {
      expected[i] = vect[i] %(opsym)s val;
      expectedNulls[i] = nulls[i];
    }

    std::unique_ptr<Vector> expectedVect =
        generateVector<%(rt_type)s>(%(trt_typekind)s, expected, nulls);
    expectedVect->setHasNull(false);
    EXPECT_EQ(expectedVect->toString(), resultVect->toString());
  }

  {
    std::vector<Datum> params =
        generateArthVectorScalar<%(t1_type)s, %(t2_type)s, %(rt_type)s>(%(t1_typekind)s, vect, nulls, &sel, %(t2_typekind)s, val, %(trt_typekind)s);
    DatumGetValue<Vector *>(params[1])->setHasNull(false);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    Vector *resultVect = DatumGetValue<Vector *>(ret);

    std::vector<%(rt_type)s> expected(sel.size());
    std::vector<bool> expectedNulls(expected.size());
    for (int i = 0; i < sel.size(); i++) {
      expected[i] = vect[sel[i]] %(opsym)s val;
      expectedNulls[i] = nulls[sel[i]];
    }
    std::unique_ptr<Vector> expectedVect =
        generateVector<%(rt_type)s>(%(trt_typekind)s, expected, expectedNulls);
    expectedVect->setHasNull(false);
    EXPECT_EQ(expectedVect->toString(), resultVect->toString());
  }

}

TEST_F(CodeGenArithFunctionTest, %(t1)s_vec_%(op)s_%(t2)s_vec) {
  std::vector<%(t1_type)s> vect1 = {0, 5, 2, 3, 1};
  std::vector<bool> nulls1 = {false, true, true, false, false};

  std::vector<%(t2_type)s> vect2 = {1, 1, 2, 3, 1};
  std::vector<bool> nulls2 = {false, false, false, true, false};
  { // both without select list
    std::vector<Datum> params = generateArthVectorVector<%(t1_type)s, %(t2_type)s, %(rt_type)s>(
         %(t1_typekind)s, vect1, nulls1, nullptr, %(t2_typekind)s, vect2, nulls2, nullptr, %(trt_typekind)s);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    Vector *resultVect = DatumGetValue<Vector *>(ret);

    std::vector<%(rt_type)s> expected(vect1.size());
    std::vector<bool> expectedNulls(expected.size());
    for (int i = 0; i < vect1.size(); i++) {
      expected[i] = vect1[i] %(opsym)s vect2[i];
      expectedNulls[i] = nulls1[i] | nulls2[i];
    }

    std::unique_ptr<Vector> expectedVect =
        generateVector<%(rt_type)s>(%(trt_typekind)s, expected, expectedNulls);
    EXPECT_EQ(expectedVect->toString(), resultVect->toString());
  }

  { // both with same select list
    SelectList sel1 = {0, 1, 3};  // -1/false, 1/true, 3/false
    SelectList sel2 = {0, 1, 3};

    std::vector<Datum> params = generateArthVectorVector<%(t1_type)s, %(t2_type)s, %(rt_type)s>(
        %(t1_typekind)s, vect1, nulls1, nullptr, %(t2_typekind)s, vect2, nulls2, nullptr, %(trt_typekind)s);
    auto sel = &sel1;
    DatumGetValue<Vector*>(params[1])->setSelected(sel,true);
    DatumGetValue<Vector*>(params[2])->setSelected(sel,true);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    Vector *resultVect = DatumGetValue<Vector *>(ret);
    std::vector<%(rt_type)s> expected(sel1.size());
    std::vector<bool> expectedNulls(expected.size());

    for (int i = 0; i < sel1.size(); i++) {
      expected[i] = vect1[sel1[i]] %(opsym)s vect2[sel2[i]];
      expectedNulls[i] = nulls1[sel1[i]] | nulls2[sel2[i]];
    }
    std::unique_ptr<Vector> expectedVect =
        generateVector<%(rt_type)s>(%(trt_typekind)s, expected, expectedNulls);
    EXPECT_EQ(expectedVect->toString(), resultVect->toString());
  }

  // one has nulls but one does not
  std::fill(nulls2.begin(), nulls2.end(), false);
  { // both without select list
    std::vector<Datum> params = generateArthVectorVector<%(t1_type)s, %(t2_type)s, %(rt_type)s>(
         %(t1_typekind)s, vect1, nulls1, nullptr, %(t2_typekind)s, vect2, nulls2, nullptr, %(trt_typekind)s);
    DatumGetValue<Vector *>(params[2])->setHasNull(false);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    Vector *resultVect = DatumGetValue<Vector *>(ret);

    std::vector<%(rt_type)s> expected(vect1.size());
    std::vector<bool> expectedNulls(expected.size());
    for (int i = 0; i < vect1.size(); i++) {
      expected[i] = vect1[i] %(opsym)s vect2[i];
      expectedNulls[i] = nulls1[i];
    }

    std::unique_ptr<Vector> expectedVect =
        generateVector<%(rt_type)s>(%(trt_typekind)s, expected, expectedNulls);
    EXPECT_EQ(expectedVect->toString(), resultVect->toString());
  }

  // both has no null value
  std::fill(nulls1.begin(), nulls1.end(), false);
  std::fill(nulls2.begin(), nulls2.end(), false);
  {
    std::vector<Datum> params = generateArthVectorVector<%(t1_type)s, %(t2_type)s, %(rt_type)s>(
         %(t1_typekind)s, vect1, nulls1, nullptr, %(t2_typekind)s, vect2, nulls2, nullptr, %(trt_typekind)s);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    Vector *resultVect = DatumGetValue<Vector *>(ret);

    std::vector<%(rt_type)s> expected(vect1.size());
    std::vector<bool> expectedNulls(expected.size());
    for (int i = 0; i < vect1.size(); i++) {
      expected[i] = vect1[i] %(opsym)s vect2[i];
      expectedNulls[i] = nulls1[i] | nulls2[i];
    }

    std::unique_ptr<Vector> expectedVect =
        generateVector<%(rt_type)s>(%(trt_typekind)s, expected, expectedNulls);

    {
      DatumGetValue<Vector *>(params[1])->setHasNull(false);
      DatumGetValue<Vector *>(params[2])->setHasNull(false);
      Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
      Vector *resultVect = DatumGetValue<Vector *>(ret);
      expectedVect->setHasNull(false);
      EXPECT_EQ(expectedVect->toString(), resultVect->toString());
    }
  }


  {
    SelectList sel1 = {0, 1, 3};  // -1/false, 1/true, 3/false
     SelectList sel2 = {0, 1, 3};

     std::vector<Datum> params = generateArthVectorVector<%(t1_type)s, %(t2_type)s, %(rt_type)s>(
         %(t1_typekind)s, vect1, nulls1, nullptr, %(t2_typekind)s, vect2, nulls2, nullptr, %(trt_typekind)s);
     auto sel = &sel1;
     DatumGetValue<Vector*>(params[1])->setSelected(sel,true);
     DatumGetValue<Vector*>(params[2])->setSelected(sel,true);
     Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
     Vector *resultVect = DatumGetValue<Vector *>(ret);
     std::vector<%(rt_type)s> expected(sel1.size());
     std::vector<bool> expectedNulls(expected.size());

     for (int i = 0; i < sel1.size(); i++) {
       expected[i] = vect1[sel1[i]] %(opsym)s vect2[sel2[i]];
       expectedNulls[i] = nulls1[sel1[i]] | nulls2[sel2[i]];
     }
     std::unique_ptr<Vector> expectedVect =
         generateVector<%(rt_type)s>(%(trt_typekind)s, expected, expectedNulls);
    {
      DatumGetValue<Vector *>(params[1])->setHasNull(false);
      DatumGetValue<Vector *>(params[2])->setHasNull(false);
      Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
      Vector *resultVect = DatumGetValue<Vector *>(ret);
      expectedVect->setHasNull(false);
      EXPECT_EQ(expectedVect->toString(), resultVect->toString());
    }
  }
}
""" % {'t1': ltypemap[ta].name,'t1_type':ltypemap[ta].symbol,
    'op': opmap[op_key].name, 'opsym': opmap[op_key].symbol,
    't2': rtypemap[tb].name, 't2_type': rtypemap[tb].symbol,
    'rt_type': rettype.symbol,
    't1_typekind': ltypemap[ta].typekind,
    't2_typekind': rtypemap[tb].typekind,
    'trt_typekind': rettype.typekind
})

cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
generate_test_function_on_arith("ArithOp", ARITH_OP, NUMERIC_TYPES, NUMERIC_TYPES)
generate_test_function_on_arith("CmpOp", ARITH_OP, DECIMAL_TYPE, DECIMAL_TYPE)
#endif
// ]]]
// [[[end]]]

// [[[cog test on vec div zero
#if false
def generate_test_function_on_div_zero(optype, opmap, ltypemap, rtypemap) :
    op_key = DIV
    for ta in ltypemap:
      for tb in rtypemap:
        rettype = get_func_return_type(ta, tb, op_key)
        cog.outl("""
TEST_F(CodeGenArithFunctionTest, %(t1)s_val_%(op)s_%(t2)s_val_divzero) {
  std::vector<Datum> params = generateArthScalarScalar<%(t1_type)s, %(t2_type)s>(%(t1_typekind)s, 2, %(t1_typekind)s, 0);
  EXPECT_THROW(%(t1)s_div_%(t2)s(params.data(), 3), dbcommon::TransactionAbortException);
}
TEST_F(CodeGenArithFunctionTest, %(t1)s_vec_%(op)s_%(t2)s_val_divzero) {
  std::vector<%(t1_type)s> vect = {-1, 1, 2, 3, 1};
  %(t2_type)s val = 0;
  std::vector<bool> nulls = {false, true, true, false, false};
  std::vector<Datum> params =
      generateArthVectorScalar<%(t1_type)s, %(t2_type)s, %(rt_type)s>(%(t1_typekind)s, vect, nulls, nullptr, %(t2_typekind)s, val, %(trt_typekind)s);
    EXPECT_THROW(%(t1)s_div_%(t2)s(params.data(), 3), dbcommon::TransactionAbortException);
}
TEST_F(CodeGenArithFunctionTest, %(t1)s_val_%(op)s_%(t2)s_vec_divzero) {
  %(t1_type)s val = 4;
  std::vector<%(t2_type)s> vect = {-1, 1, 2, 0, 1};
  std::vector<bool> nulls = {false, true, true, false, false};
  SelectList sel = {0, 1, 3};
  {
    std::vector<Datum> params =
        generateArthScalarVector<%(t1_type)s, %(t2_type)s, %(rt_type)s>(%(t1_typekind)s, val, %(t2_typekind)s, vect, nulls, nullptr, %(trt_typekind)s);
    EXPECT_THROW(%(t1)s_div_%(t2)s(params.data(), 3), dbcommon::TransactionAbortException);
  }

  {
    std::vector<Datum> params =
        generateArthScalarVector<%(t1_type)s, %(t2_type)s, %(rt_type)s>(%(t1_typekind)s, val, %(t2_typekind)s, vect, nulls, &sel, %(trt_typekind)s);
    EXPECT_THROW(%(t1)s_div_%(t2)s(params.data(), 3), dbcommon::TransactionAbortException);
  }

  std::fill(nulls.begin(), nulls.end(), false);
  {
    std::vector<Datum> params =
        generateArthScalarVector<%(t1_type)s, %(t2_type)s, %(rt_type)s>(%(t1_typekind)s, val, %(t2_typekind)s, vect, nulls, nullptr, %(trt_typekind)s);

    DatumGetValue<Vector *>(params[2])->setHasNull(false);
    EXPECT_THROW(%(t1)s_div_%(t2)s(params.data(), 3), dbcommon::TransactionAbortException);
  }

  {
    std::vector<Datum> params =
        generateArthScalarVector<%(t1_type)s, %(t2_type)s, %(rt_type)s>(%(t1_typekind)s, val, %(t2_typekind)s, vect, nulls, &sel, %(trt_typekind)s);
    DatumGetValue<Vector *>(params[2])->setHasNull(false);
    EXPECT_THROW(%(t1)s_div_%(t2)s(params.data(), 3), dbcommon::TransactionAbortException);
  }
}
TEST_F(CodeGenArithFunctionTest, %(t1)s_vec_%(op)s_%(t2)s_vec_divzero) {
  std::vector<%(t1_type)s> vect1 = {-1, 1, 2, 0, 1};
  std::vector<bool> nulls1 = {false, true, true, false, false};
  std::vector<%(t2_type)s> vect2 = {-1, 1, 2, 0, 1};
  std::vector<bool> nulls2 = {false, true, true, false, false};
  SelectList sel = {0, 1, 3};
  {
    std::vector<Datum> params =
        generateArthVectorVector<%(t1_type)s, %(t2_type)s, %(rt_type)s>(%(t1_typekind)s, vect1, nulls1, nullptr, %(t2_typekind)s, vect2, nulls2, nullptr, %(trt_typekind)s);
    EXPECT_THROW(%(t1)s_div_%(t2)s(params.data(), 3), dbcommon::TransactionAbortException);
  }

  {
    std::vector<Datum> params =
        generateArthVectorVector<%(t1_type)s, %(t2_type)s, %(rt_type)s>(%(t1_typekind)s, vect1, nulls1, nullptr, %(t2_typekind)s, vect2, nulls2, nullptr, %(trt_typekind)s);
    DatumGetValue<Vector*>(params[1])->setSelected(&sel, true);
    DatumGetValue<Vector*>(params[2])->setSelected(&sel, true);
    EXPECT_THROW(%(t1)s_div_%(t2)s(params.data(), 3), dbcommon::TransactionAbortException);
  }

  std::fill(nulls2.begin(), nulls2.end(), false);
  {
    std::vector<Datum> params =
        generateArthVectorVector<%(t1_type)s, %(t2_type)s, %(rt_type)s>(%(t1_typekind)s, vect1, nulls1, nullptr, %(t2_typekind)s, vect2, nulls2, nullptr, %(trt_typekind)s);

    DatumGetValue<Vector *>(params[2])->setHasNull(false);
    EXPECT_THROW(%(t1)s_div_%(t2)s(params.data(), 3), dbcommon::TransactionAbortException);
  }

  {
    std::vector<Datum> params =
        generateArthVectorVector<%(t1_type)s, %(t2_type)s, %(rt_type)s>(%(t1_typekind)s, vect1, nulls1, nullptr, %(t2_typekind)s, vect2, nulls2, nullptr, %(trt_typekind)s);
    DatumGetValue<Vector*>(params[1])->setSelected(&sel, true);
    DatumGetValue<Vector*>(params[2])->setSelected(&sel, true);
    DatumGetValue<Vector *>(params[2])->setHasNull(false);
    EXPECT_THROW(%(t1)s_div_%(t2)s(params.data(), 3), dbcommon::TransactionAbortException);
  }
}
""" % {'t1': ltypemap[ta].name, 't1_type':ltypemap[ta].symbol,
    'op': opmap[op_key].name, 'opsym': opmap[op_key].symbol,
    't2': rtypemap[tb].name, 't2_type':rtypemap[tb].symbol,
    't1_typekind': ltypemap[ta].typekind,
    't2_typekind': rtypemap[tb].typekind,
    'rt_type': rettype.symbol,
    'trt_typekind': rettype.typekind
})

cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
generate_test_function_on_div_zero("ArithOp", ARITH_OP, NUMERIC_TYPES, NUMERIC_TYPES)
generate_test_function_on_div_zero("ArithOp", ARITH_OP, DECIMAL_TYPE, DECIMAL_TYPE)
#endif
// ]]]
// [[[end]]]

}  // namespace dbcommon
