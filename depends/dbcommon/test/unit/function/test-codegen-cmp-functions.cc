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

class CodeGenCmpFunctionTest : public ::testing::Test {
 public:
  CodeGenCmpFunctionTest() {}
  virtual ~CodeGenCmpFunctionTest() {}

  template <class T>
  std::unique_ptr<Scalar> generateScalar(TypeKind typekind, T val) {
    if (scalarUtilitys_[typekind] == nullptr)
      scalarUtilitys_[typekind].reset(new ScalarUtility(typekind));
    std::unique_ptr<Scalar> sc(new Scalar);
    *sc = scalarUtilitys_[typekind]->generateScalar(
        BOOLEANID == typekind ? (val != 0 ? "t" : "f") : std::to_string(val));
    return std::move(sc);
  }
  template <class T>
  std::unique_ptr<Vector> generateVector(TypeKind typekind,
                                         const std::vector<T> &vect,
                                         const std::vector<bool> &nulls) {
    std::unique_ptr<Vector> vec = Vector::BuildVector(typekind, true);

    for (size_t s = 0; s < vect.size(); s++) {
      vec->append((BOOLEANID == typekind ? (vect[s] != 0 ? "t" : "f")
                                         : std::to_string(vect[s])),
                  nulls[s]);
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
  std::vector<Datum> generateCMPScalarScalar(TypeKind t1, T1 v1, TypeKind t2,
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

  template <class T1, class T2>
  std::vector<Datum> generateCMPScalarVector(TypeKind t1, T1 v1, TypeKind t2,
                                             const std::vector<T2> &vect,
                                             const std::vector<bool> &nulls,
                                             SelectList *lst) {
    std::vector<Datum> params(3);

    std::unique_ptr<SelectList> s1(new SelectList);
    params[0] = CreateDatum<SelectList *>(s1.release());

    std::unique_ptr<Scalar> s2 = generateScalar<T1>(t1, v1);
    params[1] = CreateDatum<Scalar *>(s2.release());

    std::unique_ptr<Vector> s3 = generateSelectVector<T2>(t2, vect, nulls, lst);
    params[2] = CreateDatum<Vector *>(s3.release());

    return std::move(params);
  }

  template <class T1, class T2>
  std::vector<Datum> generateCMPVectorScalar(TypeKind t1,
                                             const std::vector<T1> &vect,
                                             const std::vector<bool> &nulls,
                                             SelectList *lst, TypeKind t2,
                                             T2 v2) {
    std::vector<Datum> params(3);

    std::unique_ptr<SelectList> s1(new SelectList);
    params[0] = CreateDatum<SelectList *>(s1.release());

    std::unique_ptr<Vector> s2 = generateSelectVector<T1>(t1, vect, nulls, lst);
    params[1] = CreateDatum<Vector *>(s2.release());

    std::unique_ptr<Scalar> s3 = generateScalar<T2>(t2, v2);
    params[2] = CreateDatum<Scalar *>(s3.release());

    return std::move(params);
  }

  template <class T1, class T2>
  std::vector<Datum> generateCMPVectorVector(TypeKind t1,
                                             const std::vector<T1> &vect1,
                                             const std::vector<bool> &nulls1,
                                             SelectList *lst1, TypeKind t2,
                                             const std::vector<T2> &vect2,
                                             const std::vector<bool> &nulls2,
                                             SelectList *lst2) {
    std::vector<Datum> params(3);

    std::unique_ptr<SelectList> s1(new SelectList);
    params[0] = CreateDatum<SelectList *>(s1.release());

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
def generate_test_function_on_cmp(optype, opmap, ltypemap, rtypemap) :
  for op_key in opmap:
    for ta in ltypemap:
      for tb in rtypemap:
        cog.outl("""
TEST_F(CodeGenCmpFunctionTest, %(t1)s_val_%(op)s_%(t2)s_val) {
  {
    %(t1_type)s val1 = 1;
    %(t2_type)s val2 = 4;
    std::vector<Datum> params =
        generateCMPScalarScalar<%(t1_type)s, %(t2_type)s>(%(t1_typekind)s, val1, %(t2_typekind)s, val2);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    EXPECT_EQ(DatumGetValue<Scalar *>(ret)->value.value.i8, val1 %(opsym)s val2);
  }

  {
    %(t1_type)s val1 = 100;
    %(t2_type)s val2 = 13;
    std::vector<Datum> params =
        generateCMPScalarScalar<%(t1_type)s, %(t2_type)s>(%(t1_typekind)s, val1, %(t2_typekind)s, val2);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    EXPECT_EQ(DatumGetValue<Scalar *>(ret)->value.value.i8, val1 %(opsym)s val2);
  }

  {
    %(t1_type)s val1 = 2;
    %(t2_type)s val2 = 2;
    std::vector<Datum> params =
        generateCMPScalarScalar<%(t1_type)s, %(t2_type)s>(%(t1_typekind)s, val1, %(t2_typekind)s, val2);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    EXPECT_EQ(DatumGetValue<Scalar *>(ret)->value.value.i8, val1 %(opsym)s val2);
  }

}

TEST_F(CodeGenCmpFunctionTest, %(t1)s_val_%(op)s_%(t2)s_vec) {
  for (int i = 0; i < 2; i++) {
  %(t1_type)s val = i;
  std::vector<%(t2_type)s> vect = {-1, 1, 2, 3, 1, 1, 0};
  std::vector<bool> nulls = {false, true, true, false, false, false, false};
  {
    std::vector<Datum> params = generateCMPScalarVector<%(t1_type)s, %(t2_type)s>(
        %(t1_typekind)s, val, %(t2_typekind)s, vect, nulls, nullptr);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    int ans = 0;
    for (int i = 0; i < vect.size(); i++) {
      if (!nulls[i] && val %(opsym)s vect[i])
        ans += 1;
    }
    EXPECT_EQ(DatumGetValue<SelectList *>(ret)->size(), ans);
  }

  {
    SelectList sel = {0, 1, 3, 5, 6};
    std::vector<Datum> params = generateCMPScalarVector<%(t1_type)s, %(t2_type)s>(
         %(t1_typekind)s, val, %(t2_typekind)s, vect, nulls, &sel);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    int ans = 0;
    for (int i = 0; i < sel.size(); i++) {
      if (!nulls[sel[i]] && val %(opsym)s vect[sel[i]])
        ans += 1;
    }
    EXPECT_EQ(DatumGetValue<SelectList *>(ret)->size(), ans);
  }

  std::fill(nulls.begin(), nulls.end(), false);
  {
    std::vector<Datum> params = generateCMPScalarVector<%(t1_type)s, %(t2_type)s>(
         %(t1_typekind)s, val, %(t2_typekind)s, vect, nulls, nullptr);
    DatumGetValue<Vector *>(params[2])->setHasNull(false);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    int ans = 0;
    for (int i = 0; i < vect.size(); i++) {
      if (val %(opsym)s vect[i])
        ans += 1;
    }
    EXPECT_EQ(DatumGetValue<SelectList *>(ret)->size(), ans);
  }

  {
    SelectList sel = {0, 1, 3, 6};
    std::vector<Datum> params = generateCMPScalarVector<%(t1_type)s, %(t2_type)s>(
         %(t1_typekind)s, val, %(t2_typekind)s, vect, nulls, &sel);
    DatumGetValue<Vector *>(params[2])->setHasNull(false);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    int ans = 0;
    for (int i = 0; i < sel.size(); i++) {
      if (val %(opsym)s vect[sel[i]])
        ans += 1;
    }
    EXPECT_EQ(DatumGetValue<SelectList *>(ret)->size(), ans);
  }
  }
}

TEST_F(CodeGenCmpFunctionTest, %(t1)s_vec_%(op)s_%(t2)s_val) {
  for (int i = 0; i < 2; i++) {
  %(t1_type)s val = i;
  std::vector<%(t1_type)s> vect = {-1, 1, 2, 3, 1, 1, 0};
  std::vector<bool> nulls = {false, true, true, false, false, false, false};
  {
    SelectList sel = {0, 1, 3, 5, 6};
    std::vector<Datum> params = generateCMPVectorScalar<%(t1_type)s, %(t2_type)s>(
        %(t1_typekind)s, vect, nulls, &sel, %(t2_typekind)s, val);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    int ans = 0;
    for (int i = 0; i < sel.size(); i++) {
      if (!nulls[sel[i]] && vect[sel[i]] %(opsym)s val)
        ans += 1;
    }
    EXPECT_EQ(DatumGetValue<SelectList *>(ret)->size(), ans);
  }

  {
    std::vector<Datum> params = generateCMPVectorScalar<%(t1_type)s, %(t2_type)s>(
        %(t1_typekind)s, vect, nulls, nullptr, %(t2_typekind)s, val);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    int ans = 0;
    for (int i = 0; i < vect.size(); i++) {
      if (!nulls[i] && vect[i] %(opsym)s val)
        ans += 1;
    }
    EXPECT_EQ(DatumGetValue<SelectList *>(ret)->size(), ans);
  }

  std::fill(nulls.begin(), nulls.end(), false);
  {
    SelectList sel = {0, 1, 3, 5, 6};
    std::vector<Datum> params = generateCMPVectorScalar<%(t1_type)s, %(t2_type)s>(
        %(t1_typekind)s, vect, nulls, &sel, %(t2_typekind)s, val);
    DatumGetValue<Vector *>(params[1])->setHasNull(false);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    int ans = 0;
    for (int i = 0; i < sel.size(); i++) {
      if (vect[sel[i]] %(opsym)s val)
        ans += 1;
    }
    EXPECT_EQ(DatumGetValue<SelectList *>(ret)->size(), ans);
  }

  {
    std::vector<Datum> params = generateCMPVectorScalar<%(t1_type)s, %(t2_type)s>(
        %(t1_typekind)s, vect, nulls, nullptr, %(t2_typekind)s, val);
    DatumGetValue<Vector *>(params[1])->setHasNull(false);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    int ans = 0;
    for (int i = 0; i < vect.size(); i++) {
      if (vect[i] %(opsym)s val)
        ans += 1;
    }
    EXPECT_EQ(DatumGetValue<SelectList *>(ret)->size(), ans);
  }
  }
}

TEST_F(CodeGenCmpFunctionTest, %(t1)s_vec_%(op)s_%(t2)s_vec) {
  std::vector<%(t1_type)s> vect1 = {-1, 1, 2, 3, 1, 6, 0, 7};
  std::vector<bool> nulls1 = {false, true, false, false, false, false, false, false};

  std::vector<%(t2_type)s> vect2 = {2, 1, 2, 3, 1, 5, 7, 0};
  std::vector<bool> nulls2 = {false, false, true, false, false, false, false, false};

  {
    std::vector<Datum> params = generateCMPVectorVector<%(t1_type)s, %(t2_type)s>(
        %(t1_typekind)s, vect1, nulls1, nullptr, %(t2_typekind)s, vect2, nulls2, nullptr);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    int ans = 0;
    for (int i = 0; i < vect1.size(); i++) {
      if (!nulls1[i] && !nulls2[i] && vect1[i] %(opsym)s vect2[i])
        ans += 1;
    }
    EXPECT_EQ(DatumGetValue<SelectList *>(ret)->size(), ans);
  }

  SelectList sel = {0, 1, 3, 5, 6, 7};
  {
    std::vector<Datum> params = generateCMPVectorVector<%(t1_type)s, %(t2_type)s>(
        %(t1_typekind)s, vect1, nulls1, &sel, %(t2_typekind)s, vect2, nulls2, &sel);
    DatumGetValue<Vector *>(params[2])->selected = nullptr;
    DatumGetValue<Vector *>(params[2])->selectListOwner = nullptr;
    DatumGetValue<Vector *>(params[2])->setSelected(DatumGetValue<Vector *>(params[1])->getSelected(), true);
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    int ans = 0;
    for (int i = 0; i < sel.size(); i++) {
      if (!nulls1[sel[i]] && !nulls2[sel[i]] && vect1[sel[i]] %(opsym)s vect2[sel[i]])
        ans += 1;
    }
    EXPECT_EQ(DatumGetValue<SelectList *>(ret)->size(), ans);
  }

  std::fill(nulls1.begin(), nulls1.end(), false);
  std::fill(nulls2.begin(), nulls2.end(), false);
  {
    std::vector<Datum> params = generateCMPVectorVector<%(t1_type)s, %(t2_type)s>(
        %(t1_typekind)s, vect1, nulls1, nullptr, %(t2_typekind)s, vect2, nulls2, nullptr);
    DatumGetValue<Vector *>(params[1])->setHasNull(false);
    DatumGetValue<Vector *>(params[2])->setHasNull(false);
    int ans = 0;
    for (int i = 0; i < vect1.size(); i++) {
      if (vect1[i] %(opsym)s vect2[i])
        ans += 1;
    }
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    EXPECT_EQ(DatumGetValue<SelectList *>(ret)->size(), ans);
  }

  {
    std::vector<Datum> params = generateCMPVectorVector<%(t1_type)s, %(t2_type)s>(
        %(t1_typekind)s, vect1, nulls1, &sel, %(t2_typekind)s, vect2, nulls2, &sel);
    DatumGetValue<Vector *>(params[1])->setHasNull(false);
    DatumGetValue<Vector *>(params[2])->setHasNull(false);
    DatumGetValue<Vector *>(params[2])->selected = nullptr;
    DatumGetValue<Vector *>(params[2])->selectListOwner = nullptr;
    DatumGetValue<Vector *>(params[2])->setSelected(DatumGetValue<Vector *>(params[1])->getSelected(), true);
    int ans = 0;
    for (int i = 0; i < sel.size(); i++) {
      if (vect1[sel[i]] %(opsym)s vect2[sel[i]])
        ans += 1;
    }
    Datum ret = %(t1)s_%(op)s_%(t2)s(params.data(), 3);
    EXPECT_EQ(DatumGetValue<SelectList *>(ret)->size(), ans);
  }
}
""" % {'t1': ltypemap[ta].name,'t1_type':ltypemap[ta].symbol,
    'op': opmap[op_key].name, 'opsym': opmap[op_key].symbol,
    't2': rtypemap[tb].name, 't2_type': rtypemap[tb].symbol,
    't1_typekind': ltypemap[ta].typekind, 't2_typekind': rtypemap[tb].typekind,
})

cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
generate_test_function_on_cmp("CmpOp", CMP_OP, NUMERIC_TYPES, NUMERIC_TYPES)
generate_test_function_on_cmp("CmpOp", CMP_OP, BOOL_TYPE, BOOL_TYPE)
generate_test_function_on_cmp("CmpOp", CMP_OP, DECIMAL_TYPE, DECIMAL_TYPE)
#endif
// ]]]
// [[[end]]]

}  // namespace dbcommon
