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

#include <cmath>

#include "dbcommon/function/array-function.h"
#include "dbcommon/testutil/vector-utils.h"
#include "dbcommon/type/array.h"
#include "gtest/gtest.h"

namespace dbcommon {

class ArrayFunctionTest : public ::testing::Test {
 public:
  ArrayFunctionTest() {}

  virtual ~ArrayFunctionTest() {}

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

  template <class T>
  std::vector<Datum> generateOPScalarScalar(const char *v1, const char *v2,
                                            bool retBool, TypeKind t) {
    std::vector<Datum> params(3);

    if (retBool) {
      std::unique_ptr<Scalar> s1 = generateScalar<bool>(false);
      params[0] = CreateDatum<Scalar *>(s1.release());
    } else {
      std::unique_ptr<Scalar> s1 = generateScalar<T>(0);
      params[0] = CreateDatum<Scalar *>(s1.release());
    }

    {
      auto typeEnt = reinterpret_cast<dbcommon::ArrayType *>(
          dbcommon::TypeUtil::instance()->getTypeEntryById(t)->type.get());
      std::unique_ptr<Vector> array = typeEnt->getScalarFromString(v1);
      std::unique_ptr<Scalar> s2 = generateScalar<Vector *>(array.release());
      params[1] = CreateDatum<Scalar *>(s2.release());
    }

    {
      auto typeEnt = reinterpret_cast<dbcommon::ArrayType *>(
          dbcommon::TypeUtil::instance()->getTypeEntryById(t)->type.get());
      std::unique_ptr<Vector> array = typeEnt->getScalarFromString(v2);
      std::unique_ptr<Scalar> s3 = generateScalar<Vector *>(array.release());
      params[2] = CreateDatum<Scalar *>(s3.release());
    }

    return std::move(params);
  }

  template <class T, class RetT>
  std::vector<Datum> generateOPVectorScalar(
      TypeKind t, TypeKind ct, const std::vector<T> &vect,
      const std::vector<uint64_t> &offsets, const std::vector<bool> &nulls,
      const std::vector<bool> &valueNulls, SelectList *lst, const char *v2,
      bool retSelectList) {
    std::vector<Datum> params(3);

    if (retSelectList) {
      std::unique_ptr<SelectList> s1(new SelectList);
      params[0] = CreateDatum<SelectList *>(s1.release());
    } else {
      std::unique_ptr<Vector> s1(
          Vector::BuildVector(TypeMapping<RetT>::type, true));
      params[0] = CreateDatum<Vector *>(s1.release());
    }

    std::unique_ptr<Vector> s2 = VectorUtility::generateSelectListVector<T>(
        t, ct, vect, offsets, &nulls, &valueNulls, lst);
    params[1] = CreateDatum<Vector *>(s2.release());

    auto typeEnt = reinterpret_cast<dbcommon::ArrayType *>(
        dbcommon::TypeUtil::instance()->getTypeEntryById(t)->type.get());
    std::unique_ptr<Vector> array = typeEnt->getScalarFromString(v2);
    std::unique_ptr<Scalar> s3 = generateScalar<Vector *>(array.release());
    params[2] = CreateDatum<Scalar *>(s3.release());

    return std::move(params);
  }

  template <class T, TypeKind AT, class RetT>
  void arrayDistanceTest(Datum(arrayDistance)(Datum *, uint64_t),
                         int distanceType) {
    // NULL, {3,4,3}, {3, 4}, {0, 0}
    std::vector<T> vect = {3, 4, 3, -1, 1, 1, 1};
    std::vector<uint64_t> offsets = {0, 0, 3, 5, 7};
    std::vector<bool> nulls = {true, false, false, false};
    std::vector<bool> valueNulls = {false, false, false, false,
                                    false, false, false};
    std::string val = "{3,4}";
    {
      SelectList sel = {0, 1, 3};
      std::vector<RetT> expected(sel.size());
      switch (distanceType) {
        case 1:  // euclidean metric
          for (size_t i = 0; i < sel.size(); i++) {
            size_t j = offsets[sel[i]];
            expected[i] = (vect[j] - 3) * (vect[j] - 3) +
                          (vect[j + 1] - 4) * (vect[j + 1] - 4);
            for (j = offsets[sel[i]] + 2; j < offsets[sel[i] + 1]; j++) {
              expected[i] += vect[j] * vect[j];
            }
            expected[i] = sqrt(expected[i]);
          }
          break;
        case 2:  // cosine distance
          for (size_t i = 0; i < sel.size(); i++) {
            size_t j = offsets[sel[i]];
            expected[i] = vect[j] * 3 + vect[j + 1] * 4;
            T length1 = 0;
            T length2 = 25;
            for (j = offsets[sel[i]]; j < offsets[sel[i] + 1]; j++) {
              length1 += vect[j] * vect[j];
            }
            expected[i] = expected[i] / sqrt(length1) / sqrt(length2);
          }
          break;
        default:
          LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "invalid distance type");
      }

      std::unique_ptr<Vector> expectedVect = generateVector(expected, nulls);

      std::vector<Datum> params = generateOPVectorScalar<T, RetT>(
          AT, TypeMapping<T>::type, vect, offsets, nulls, valueNulls, &sel,
          val.c_str(), false);
      Datum ret = arrayDistance(params.data(), 3);
      Vector *resultVect = DatumGetValue<Vector *>(ret);
      expectedVect->setHasNull(true);
      EXPECT_EQ(Vector::equal(*resultVect, *expectedVect), true);
    }

    {
      std::vector<RetT> expected(nulls.size());
      switch (distanceType) {
        case 1:  // euclidean metric
          for (size_t i = 0; i < nulls.size(); i++) {
            size_t j = offsets[i];
            expected[i] = (vect[j] - 3) * (vect[j] - 3) +
                          (vect[j + 1] - 4) * (vect[j + 1] - 4);
            for (j = offsets[i] + 2; j < offsets[i + 1]; j++) {
              expected[i] += vect[j] * vect[j];
            }
            expected[i] = sqrt(expected[i]);
          }
          break;
        case 2:  // cosine distance
          for (size_t i = 0; i < nulls.size(); i++) {
            size_t j = offsets[i];
            expected[i] = vect[j] * 3 + vect[j + 1] * 4;
            T length1 = 0;
            T length2 = 25;
            for (j = offsets[i]; j < offsets[i + 1]; j++) {
              length1 += vect[j] * vect[j];
            }
            expected[i] = expected[i] / sqrt(length1) / sqrt(length2);
          }
          break;
        default:
          LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "invalid distance type");
      }

      std::unique_ptr<Vector> expectedVect = generateVector(expected, nulls);

      std::vector<Datum> params = generateOPVectorScalar<T, RetT>(
          AT, TypeMapping<T>::type, vect, offsets, nulls, valueNulls, nullptr,
          val.c_str(), false);
      Datum ret = arrayDistance(params.data(), 3);
      Vector *resultVect = DatumGetValue<Vector *>(ret);
      expectedVect->setHasNull(true);
      EXPECT_EQ(Vector::equal(*resultVect, *expectedVect), true);
    }

    {
      std::string val2 = "{3,4}";
      std::vector<Datum> params =
          generateOPScalarScalar<T>(val.c_str(), val2.c_str(), false, AT);
      Datum ret = arrayDistance(params.data(), 3);
      Scalar *retScalar = DatumGetValue<Scalar *>(ret);
      T result = DatumGetValue<T>(retScalar->value);
      switch (distanceType) {
        case 1:  // euclidean metric
          EXPECT_EQ(result, 0);
          break;
        case 2:
          EXPECT_EQ(result, 1);
          break;
        default:
          LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "invalid distance type");
      }
    }
  }
};

TEST_F(ArrayFunctionTest, FloatArrayEuclideanMetricTest) {
  arrayDistanceTest<float, FLOATARRAYID, float>(float_array_euclidean_metric,
                                                1);
}

TEST_F(ArrayFunctionTest, DoubleArrayEuclideanMetricTest) {
  arrayDistanceTest<double, DOUBLEARRAYID, double>(
      double_array_euclidean_metric, 1);
}

// Disabled for precise problem
TEST_F(ArrayFunctionTest, DISABLED_FloatArrayCosineDistanceTest) {
  arrayDistanceTest<float, FLOATARRAYID, double>(float_array_cosine_distance,
                                                 2);
}

TEST_F(ArrayFunctionTest, DoubleArrayCosineDistanceTest) {
  arrayDistanceTest<double, DOUBLEARRAYID, double>(double_array_cosine_distance,
                                                   2);
}

TEST_F(ArrayFunctionTest, BigintArrayOverlapTest) {
  // NULL, {1,4,3}, {NULL, 2, 2, 2, 2}, {NULL, NULL}
  std::vector<int64_t> vect = {1, 4, 3, 0, 2, 2, 2, 2, 0, 0};
  std::vector<uint64_t> offsets = {0, 0, 3, 8, 10};
  std::vector<bool> nulls = {true, false, false, false};
  std::vector<bool> valueNulls = {false, false, false, true, true,
                                  true,  true,  false, true, true};
  std::string val = "{2,NULL,1}";
  {
    SelectList sel = {0, 1, 3};
    std::vector<Datum> params = generateOPVectorScalar<int64_t, int64_t>(
        BIGINTARRAYID, BIGINTID, vect, offsets, nulls, valueNulls, &sel,
        val.c_str(), true);
    Datum ret = bigint_array_overlap(params.data(), 3);
    EXPECT_EQ(DatumGetValue<SelectList *>(ret)->size(), 1);
  }

  {
    std::vector<Datum> params = generateOPVectorScalar<int64_t, int64_t>(
        BIGINTARRAYID, BIGINTID, vect, offsets, nulls, valueNulls, nullptr,
        val.c_str(), true);
    Datum ret = bigint_array_overlap(params.data(), 3);
    EXPECT_EQ(DatumGetValue<SelectList *>(ret)->size(), 2);
  }

  {
    std::string val2 = "{NULL,3}";
    std::vector<Datum> params = generateOPScalarScalar<int64_t>(
        val.c_str(), val2.c_str(), true, BIGINTARRAYID);
    Datum ret = bigint_array_overlap(params.data(), 3);
    EXPECT_EQ(DatumGetValue<Scalar *>(ret)->value.value.i8, false);
  }

  {
    std::string val2 = "{3,1}";
    std::vector<Datum> params = generateOPScalarScalar<int64_t>(
        val.c_str(), val2.c_str(), true, BIGINTARRAYID);
    Datum ret = bigint_array_overlap(params.data(), 3);
    EXPECT_EQ(DatumGetValue<Scalar *>(ret)->value.value.i8, true);
  }
}

TEST_F(ArrayFunctionTest, BigintArrayContainsTest) {
  // NULL, {1,4,3}, {NULL, 2, 2, 2, 2}, {NULL, NULL}
  std::vector<int64_t> vect = {1, 2, 3, 0, 2, 1, 2, 2, 0, 0};
  std::vector<uint64_t> offsets = {0, 0, 3, 8, 10};
  std::vector<bool> nulls = {true, false, false, false};
  std::vector<bool> valueNulls = {false, false, false, true, true,
                                  true,  true,  false, true, true};
  std::string val = "{2,1}";
  {
    SelectList sel = {0, 1, 3};
    std::vector<Datum> params = generateOPVectorScalar<int64_t, int64_t>(
        BIGINTARRAYID, BIGINTID, vect, offsets, nulls, valueNulls, &sel,
        val.c_str(), true);
    Datum ret = bigint_array_contains(params.data(), 3);
    EXPECT_EQ(DatumGetValue<SelectList *>(ret)->size(), 1);
  }

  {
    std::vector<Datum> params = generateOPVectorScalar<int64_t, int64_t>(
        BIGINTARRAYID, BIGINTID, vect, offsets, nulls, valueNulls, nullptr,
        val.c_str(), true);
    Datum ret = bigint_array_contains(params.data(), 3);
    EXPECT_EQ(DatumGetValue<SelectList *>(ret)->size(), 2);
  }

  {
    std::string val2 = "{NULL,2}";
    std::vector<Datum> params = generateOPScalarScalar<int64_t>(
        val.c_str(), val2.c_str(), true, BIGINTARRAYID);
    Datum ret = bigint_array_contains(params.data(), 3);
    EXPECT_EQ(DatumGetValue<Scalar *>(ret)->value.value.i8, false);
  }

  {
    std::string val2 = "{2}";
    std::vector<Datum> params = generateOPScalarScalar<int64_t>(
        val.c_str(), val2.c_str(), true, BIGINTARRAYID);
    Datum ret = bigint_array_contains(params.data(), 3);
    EXPECT_EQ(DatumGetValue<Scalar *>(ret)->value.value.i8, true);
  }
}

TEST_F(ArrayFunctionTest, BigintArrayContainedTest) {
  // NULL, {1,4,3}, {NULL, 2, 2, 2, 2}, {NULL, NULL}
  std::vector<int64_t> vect = {1, 4, 3, 0, 2, 2, 2, 2, 0, 0};
  std::vector<uint64_t> offsets = {0, 0, 3, 8, 10};
  std::vector<bool> nulls = {true, false, false, false};
  std::vector<bool> valueNulls = {false, false, false, true, true,
                                  true,  true,  false, true, true};
  std::string val = "{2,NULL,1,4,3}";
  {
    SelectList sel = {0, 1, 3};
    std::vector<Datum> params = generateOPVectorScalar<int64_t, int64_t>(
        BIGINTARRAYID, BIGINTID, vect, offsets, nulls, valueNulls, &sel,
        val.c_str(), true);
    Datum ret = bigint_array_contained(params.data(), 3);
    EXPECT_EQ(DatumGetValue<SelectList *>(ret)->size(), 1);
  }

  {
    std::vector<Datum> params = generateOPVectorScalar<int64_t, int64_t>(
        BIGINTARRAYID, BIGINTID, vect, offsets, nulls, valueNulls, nullptr,
        val.c_str(), true);
    Datum ret = bigint_array_contained(params.data(), 3);
    EXPECT_EQ(DatumGetValue<SelectList *>(ret)->size(), 1);
  }

  {
    std::string val2 = "{NULL,3,2,1}";
    std::vector<Datum> params = generateOPScalarScalar<int64_t>(
        val.c_str(), val2.c_str(), true, BIGINTARRAYID);
    Datum ret = bigint_array_contained(params.data(), 3);
    EXPECT_EQ(DatumGetValue<Scalar *>(ret)->value.value.i8, false);
  }

  {
    std::string val2 = "{3,1}";
    std::vector<Datum> params = generateOPScalarScalar<int64_t>(
        val.c_str(), val2.c_str(), true, BIGINTARRAYID);
    Datum ret = bigint_array_contained(params.data(), 3);
    EXPECT_EQ(DatumGetValue<Scalar *>(ret)->value.value.i8, false);
  }
}
}  // namespace dbcommon
