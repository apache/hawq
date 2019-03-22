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

#include "dbcommon/function/array-function.h"

#include <x86intrin.h>

#include <cmath>
#include <memory>
#include <set>
#include <utility>

#include "dbcommon/common/vector.h"
#include "dbcommon/common/vector/list-vector.h"
#include "dbcommon/function/invoker.h"
#include "dbcommon/type/array.h"
#include "dbcommon/utils/macro.h"

namespace dbcommon {

template <typename T>
T euclidean_metric_intrinsic(uint64_t num, const T *array1, const T *array2) {}

template <typename T>
T euclidean_metric_intrinsic(uint64_t num, const T *array) {}

template <>
double euclidean_metric_intrinsic(uint64_t num,
                                  const double *__restrict__ array1_,
                                  const double *__restrict__ array2_) {
  double distance = 0;
  uint64_t i;
  /* Do not remove code below! This is for compiler to save these vars in
   * register*/
  const double *__restrict__ array1 = array1_;
  const double *__restrict__ array2 = array2_;
#ifdef AVX_OPT
  uint64_t num_align = num / 4 * 4;
  __m256d euclidean = _mm256_setzero_pd();
  if (num > 3) {
    for (i = 0; i < num_align; i += 4) {
      const __m256d x = _mm256_loadu_pd(array1);
      const __m256d y = _mm256_loadu_pd(array2);
      const __m256d x_minus_y = _mm256_sub_pd(x, y);
      const __m256d x_minus_y_sq = _mm256_mul_pd(x_minus_y, x_minus_y);
      euclidean = _mm256_add_pd(euclidean, x_minus_y_sq);
      array1 += 4;
      array2 += 4;
    }
    distance = euclidean[0] + euclidean[1] + euclidean[2] + euclidean[3];
  }
#else
  uint64_t num_align = 0;
#endif

  for (i = 0; i < num - num_align; i++) {
    distance += (array1[i] - array2[i]) * (array1[i] - array2[i]);
  }

  return distance;
}

template <>
float euclidean_metric_intrinsic(uint64_t num,
                                 const float *__restrict__ array1_,
                                 const float *__restrict__ array2_) {
  float distance = 0;
  uint64_t i;
  /* Do not remove code below! This is for compiler to save these vars in
   * register*/
  const float *__restrict__ array1 = array1_;
  const float *__restrict__ array2 = array2_;
#ifdef AVX_OPT
  uint64_t num_align = num / 8 * 8;
  __m256 euclidean = _mm256_setzero_ps();
  if (num > 7) {
    for (i = 0; i < num_align; i += 8) {
      const __m256 x = _mm256_loadu_ps(array1);
      const __m256 y = _mm256_loadu_ps(array2);
      const __m256 x_minus_y = _mm256_sub_ps(x, y);
      const __m256 x_minus_y_sq = _mm256_mul_ps(x_minus_y, x_minus_y);
      euclidean = _mm256_add_ps(euclidean, x_minus_y_sq);
      array1 += 8;
      array2 += 8;
    }
    distance = euclidean[0] + euclidean[1] + euclidean[2] + euclidean[3] +
               euclidean[4] + euclidean[5] + euclidean[6] + euclidean[7];
  }
#else
  uint64_t num_align = 0;
#endif

  for (i = 0; i < num - num_align; i++) {
    distance += (array1[i] - array2[i]) * (array1[i] - array2[i]);
  }

  return distance;
}

template <typename T>
void cosine_distance_intrinsic(uint64_t num, const T *array1, const T *array2,
                               double *length1, double *length2,
                               double *length) {}

template <typename T>
void cosine_distance_intrinsic(uint64_t num, const T *array, double *length) {}

template <>
void cosine_distance_intrinsic(uint64_t num, const double *__restrict__ array1_,
                               const double *__restrict__ array2_,
                               double *length1, double *length2,
                               double *length) {
  double distance = 0;
  double distance1 = 0;
  double distance2 = 0;
  uint64_t i;
  /* Do not remove code below! This is for compiler to save these vars in
   * register*/
  const double *__restrict__ array1 = array1_;
  const double *__restrict__ array2 = array2_;
#ifdef AVX_OPT
  uint64_t num_align = num / 4 * 4;
  __m256d xx = _mm256_setzero_pd();
  __m256d yy = _mm256_setzero_pd();
  __m256d xy = _mm256_setzero_pd();
  if (num > 3) {
    for (i = 0; i < num_align; i += 4) {
      const __m256d x = _mm256_loadu_pd(array1);
      const __m256d y = _mm256_loadu_pd(array2);
      const __m256d x_sq = _mm256_mul_pd(x, x);
      const __m256d y_sq = _mm256_mul_pd(y, y);
      const __m256d x_mul_y = _mm256_mul_pd(x, y);
      xx = _mm256_add_pd(xx, x_sq);
      yy = _mm256_add_pd(yy, y_sq);
      xy = _mm256_add_pd(xy, x_mul_y);
      array1 += 4;
      array2 += 4;
    }
    distance1 = xx[0] + xx[1] + xx[2] + xx[3];
    distance2 = yy[0] + yy[1] + yy[2] + yy[3];
    distance = xy[0] + xy[1] + xy[2] + xy[3];
  }
#else
  uint64_t num_align = 0;
#endif

  for (i = 0; i < num - num_align; i++) {
    distance1 += array1[i] * array1[i];
    distance2 += array2[i] * array2[i];
    distance += array1[i] * array2[i];
  }
  *length1 += distance1;
  *length2 += distance2;
  *length += distance;
}

template <>
void cosine_distance_intrinsic(uint64_t num, const float *__restrict__ array1_,
                               const float *__restrict__ array2_,
                               double *length1, double *length2,
                               double *length) {
  double distance = 0;
  double distance1 = 0;
  double distance2 = 0;
  uint64_t i;
  /* Do not remove code below! This is for compiler to save these vars in
   * register*/
  const float *__restrict__ array1 = array1_;
  const float *__restrict__ array2 = array2_;
#ifdef AVX_OPT
  uint64_t num_align = num / 8 * 8;
  __m256 xx = _mm256_setzero_ps();
  __m256 yy = _mm256_setzero_ps();
  __m256 xy = _mm256_setzero_ps();
  if (num > 3) {
    for (i = 0; i < num_align; i += 8) {
      const __m256 x = _mm256_loadu_ps(array1);
      const __m256 y = _mm256_loadu_ps(array2);
      const __m256 x_sq = _mm256_mul_ps(x, x);
      const __m256 y_sq = _mm256_mul_ps(y, y);
      const __m256 x_mul_y = _mm256_mul_ps(x, y);
      xx = _mm256_add_ps(xx, x_sq);
      yy = _mm256_add_ps(yy, y_sq);
      xy = _mm256_add_ps(xy, x_mul_y);
      array1 += 8;
      array2 += 8;
    }
    distance1 = xx[0] + xx[1] + xx[2] + xx[3] + xx[4] + xx[5] + xx[6] + xx[7];
    distance2 = yy[0] + yy[1] + yy[2] + yy[3] + yy[4] + yy[5] + yy[6] + yy[7];
    distance = xy[0] + xy[1] + xy[2] + xy[3] + xy[4] + xy[5] + xy[6] + xy[7];
  }
#else
  uint64_t num_align = 0;
#endif

  for (i = 0; i < num - num_align; i++) {
    distance1 += array1[i] * array1[i];
    distance2 += array2[i] * array2[i];
    distance += array1[i] * array2[i];
  }
  *length1 += distance1;
  *length2 += distance2;
  *length += distance;
}

template <>
void cosine_distance_intrinsic(uint64_t num, const double *__restrict__ array_,
                               double *length) {
  double distance = 0;
  uint64_t i;
  /* Do not remove code below! This is for compiler to save these vars in
   * register*/
  const double *__restrict__ array = array_;
#ifdef AVX_OPT
  uint64_t num_align = num / 4 * 4;
  __m256d xx = _mm256_setzero_pd();
  if (num > 3) {
    for (i = 0; i < num_align; i += 4) {
      const __m256d x = _mm256_loadu_pd(array);
      const __m256d x_sq = _mm256_mul_pd(x, x);
      xx = _mm256_add_pd(xx, x_sq);
      array += 4;
    }
    distance = xx[0] + xx[1] + xx[2] + xx[3];
  }
#else
  uint64_t num_align = 0;
#endif

  for (i = 0; i < num - num_align; i++) {
    distance += array[i] * array[i];
  }
  *length += distance;
}

template <>
void cosine_distance_intrinsic(uint64_t num, const float *__restrict__ array_,
                               double *length) {
  double distance = 0;
  uint64_t i;
  /* Do not remove code below! This is for compiler to save these vars in
   * register*/
  const float *__restrict__ array = array_;
#ifdef AVX_OPT
  uint64_t num_align = num / 8 * 8;
  __m256 xx = _mm256_setzero_ps();
  if (num > 3) {
    for (i = 0; i < num_align; i += 8) {
      const __m256 x = _mm256_loadu_ps(array);
      const __m256 x_sq = _mm256_mul_ps(x, x);
      xx = _mm256_add_ps(xx, x_sq);
      array += 8;
    }
    distance = xx[0] + xx[1] + xx[2] + xx[3] + xx[4] + xx[5] + xx[6] + xx[7];
  }
#else
  uint64_t num_align = 0;
#endif

  for (i = 0; i < num - num_align; i++) {
    distance += array[i] * array[i];
  }
  *length += distance;
}

template <>
double euclidean_metric_intrinsic(uint64_t num,
                                  const double *__restrict__ array_) {
  double distance = 0;
  uint64_t i;
  /* Do not remove code below! This is for compiler to save these vars in
   * register*/
  const double *__restrict__ array = array_;
#ifdef AVX_OPT
  uint64_t num_align = num / 4 * 4;
  __m256d euclidean = _mm256_setzero_pd();
  if (num > 3) {
    for (i = 0; i < num_align; i += 4) {
      const __m256d x = _mm256_loadu_pd(array);
      const __m256d x_sq = _mm256_mul_pd(x, x);
      euclidean = _mm256_add_pd(euclidean, x_sq);
      array += 4;
    }
    distance = euclidean[0] + euclidean[1] + euclidean[2] + euclidean[3];
  }
#else
  uint64_t num_align = 0;
#endif

  for (i = 0; i < num - num_align; i++) {
    distance += array[i] * array[i];
  }

  return distance;
}

template <>
float euclidean_metric_intrinsic(uint64_t num,
                                 const float *__restrict__ array_) {
  float distance = 0;
  uint64_t i;
  /* Do not remove code below! This is for compiler to save these vars in
   * register*/
  const float *__restrict__ array = array_;
#ifdef AVX_OPT
  uint64_t num_align = num / 8 * 8;
  __m256 euclidean = _mm256_setzero_ps();
  if (num > 7) {
    for (i = 0; i < num_align; i += 8) {
      const __m256 x = _mm256_loadu_ps(array);
      const __m256 x_sq = _mm256_mul_ps(x, x);
      euclidean = _mm256_add_ps(euclidean, x_sq);
      array += 8;
    }
    distance = euclidean[0] + euclidean[1] + euclidean[2] + euclidean[3] +
               euclidean[4] + euclidean[5] + euclidean[6] + euclidean[7];
  }
#else
  uint64_t num_align = 0;
#endif

  for (i = 0; i < num - num_align; i++) {
    distance += array[i] * array[i];
  }

  return distance;
}

template <typename POD>
Datum array_euclidean_metric_internal(Datum *params, uint64_t size) {
  assert(size == 3);

  Object *para2 = DatumGetValue<Object *>(params[2]);
  Scalar *scalar2 = dynamic_cast<Scalar *>(para2);
  if (!scalar2)
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "Second parameter should be const value");
  auto arrayRight = DatumGetValue<Vector *>(scalar2->value);
  assert(arrayRight->getSelected() == nullptr && arrayRight->hasNullValue());
  uint64_t nums2 = arrayRight->getNumOfRowsPlain();
  const POD *__restrict__ data2 =
      reinterpret_cast<const POD *>(arrayRight->getValue());
  const bool *__restrict__ nulls2 = arrayRight->getNulls();

  Object *para1 = DatumGetValue<Object *>(params[1]);
  Scalar *scalar1 = dynamic_cast<Scalar *>(para1);
  POD distance = 0;

  if (scalar1) {
    Scalar *ret = DatumGetValue<Scalar *>(params[0]);
    auto arrayLeft = DatumGetValue<Vector *>(scalar1->value);
    assert(arrayLeft->getSelected() == nullptr && arrayLeft->hasNullValue());
    const POD *__restrict__ data1 =
        reinterpret_cast<const POD *>(arrayLeft->getValue());
    const bool *__restrict__ nulls1 = arrayLeft->getNulls();
    uint64_t nums1 = arrayLeft->getNumOfRowsPlain();
    uint64_t i;
    if (nums1 < nums2) {
      for (i = 0; i < nums1; i++) {
        auto value1 = data1[i];
        auto isNull1 = nulls1[i];
        auto value2 = data2[i];
        auto isNull2 = nulls2[i];
        if (isNull1 || isNull2)
          LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                    "array should not contain NULL");
        distance += (value1 - value2) * (value1 - value2);
      }
      for (; i < nums2; i++) {
        auto value2 = data2[i];
        auto isNull2 = nulls2[i];
        if (isNull2)
          LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                    "array should not contain NULL");
        distance += value2 * value2;
      }
    } else {
      for (i = 0; i < nums2; i++) {
        auto value1 = data1[i];
        auto isNull1 = nulls1[i];
        auto value2 = data2[i];
        auto isNull2 = nulls2[i];
        if (isNull1 || isNull2)
          LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                    "array should not contain NULL");
        distance += (value1 - value2) * (value1 - value2);
      }
      for (; i < nums1; i++) {
        auto value1 = data1[i];
        auto isNull1 = nulls1[i];
        if (isNull1)
          LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                    "array should not contain NULL");
        distance += value1 * value1;
      }
    }
    ret->value = CreateDatum(static_cast<POD>(sqrt(distance)));
    return CreateDatum(ret);
  } else {
    ListVector *vec1 = reinterpret_cast<ListVector *>(para1);
    Vector *retVec = DatumGetValue<Vector *>(params[0]);
    SelectList *sel = vec1->getSelected();
    for (uint64_t i = 0; i < nums2; i++) {
      if (nulls2[i])
        LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                  "array should not contain NULL");
    }
    uint64_t rowNum = vec1->getNumOfRows();
    const uint64_t plainRowNum = vec1->getNumOfRowsPlain();
    dbcommon::ByteBuffer &retDataBuf = *retVec->getValueBuffer();
    retDataBuf.resize(plainRowNum * sizeof(POD));
    POD *__restrict__ retData = reinterpret_cast<POD *>(retDataBuf.data());
    const uint64_t *__restrict__ offsets = vec1->getOffsets();
    const POD *__restrict__ array1all =
        reinterpret_cast<const POD *>(vec1->getValue());

    retVec->setHasNull(vec1->hasNullValue());
    if (vec1->hasNullValue()) {
      dbcommon::BoolBuffer *__restrict__ nulls = vec1->getNullBuffer();
      dbcommon::BoolBuffer &retNullBuf = *retVec->getNullBuffer();
      retNullBuf.resize(plainRowNum);
      char *__restrict__ retNull = retNullBuf.getChars();
      if (vec1->getSelected()) {
        retVec->setSelected(vec1->getSelected(), false);
#pragma clang loop vectorize(enable)
        for (uint64_t i = 0; i < rowNum; i++) {
          auto idx = (*sel)[i];
          uint64_t nums1 = offsets[idx + 1] - offsets[idx];
          bool null = nulls->getChar(idx);
          auto array1 = array1all + offsets[idx];
          if (!null) {
            if (nums1 < nums2) {
              distance = euclidean_metric_intrinsic<POD>(nums1, array1, data2);
              distance +=
                  euclidean_metric_intrinsic<POD>(nums2 - nums1, data2 + nums1);
            } else {
              distance = euclidean_metric_intrinsic<POD>(nums2, array1, data2);
              distance += euclidean_metric_intrinsic<POD>(nums1 - nums2,
                                                          array1 + nums2);
            }
          }
          retData[idx] = static_cast<POD>(sqrt(distance));
          retNull[idx] = null;
        }
      } else {
#pragma clang loop vectorize(enable)
        for (uint64_t i = 0; i < rowNum; i++) {
          auto idx = i;
          uint64_t nums1 = offsets[idx + 1] - offsets[idx];
          bool null = nulls->getChar(idx);
          auto array1 = array1all + offsets[idx];
          if (!null) {
            if (nums1 < nums2) {
              distance = euclidean_metric_intrinsic<POD>(nums1, array1, data2);
              distance +=
                  euclidean_metric_intrinsic<POD>(nums2 - nums1, data2 + nums1);
            } else {
              distance = euclidean_metric_intrinsic<POD>(nums2, array1, data2);
              distance += euclidean_metric_intrinsic<POD>(nums1 - nums2,
                                                          array1 + nums2);
            }
          }
          retData[idx] = static_cast<POD>(sqrt(distance));
          retNull[idx] = null;
        }
      }
    } else {
      if (vec1->getSelected()) {
        retVec->setSelected(vec1->getSelected(), false);
#pragma clang loop vectorize(enable)
        for (uint64_t i = 0; i < rowNum; i++) {
          auto idx = (*sel)[i];
          uint64_t nums1 = offsets[idx + 1] - offsets[idx];
          auto array1 = array1all + offsets[idx];
          if (nums1 < nums2) {
            distance = euclidean_metric_intrinsic<POD>(nums1, array1, data2);
            distance +=
                euclidean_metric_intrinsic<POD>(nums2 - nums1, data2 + nums1);
          } else {
            distance = euclidean_metric_intrinsic<POD>(nums2, array1, data2);
            distance +=
                euclidean_metric_intrinsic<POD>(nums1 - nums2, array1 + nums2);
          }
          retData[idx] = static_cast<POD>(sqrt(distance));
        }
      } else {
#pragma clang loop vectorize(enable)
        for (uint64_t i = 0; i < rowNum; i++) {
          auto idx = i;
          uint64_t nums1 = offsets[idx + 1] - offsets[idx];
          auto array1 = array1all + offsets[idx];
          if (nums1 < nums2) {
            distance = euclidean_metric_intrinsic<POD>(nums1, array1, data2);
            distance +=
                euclidean_metric_intrinsic<POD>(nums2 - nums1, data2 + nums1);
          } else {
            distance = euclidean_metric_intrinsic<POD>(nums2, array1, data2);
            distance +=
                euclidean_metric_intrinsic<POD>(nums1 - nums2, array1 + nums2);
          }
          retData[idx] = static_cast<POD>(sqrt(distance));
        }
      }
    }

    return CreateDatum(retVec);
  }
}

template <typename POD>
Datum array_cosine_distance_internal(Datum *params, uint64_t size) {
  assert(size == 3);

  Object *para2 = DatumGetValue<Object *>(params[2]);
  Scalar *scalar2 = dynamic_cast<Scalar *>(para2);
  if (!scalar2)
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "Second parameter should be const value");
  auto arrayRight = DatumGetValue<Vector *>(scalar2->value);
  assert(arrayRight->getSelected() == nullptr && arrayRight->hasNullValue());
  uint64_t nums2 = arrayRight->getNumOfRowsPlain();
  const POD *__restrict__ data2 =
      reinterpret_cast<const POD *>(arrayRight->getValue());
  const bool *__restrict__ nulls2 = arrayRight->getNulls();

  Object *para1 = DatumGetValue<Object *>(params[1]);
  Scalar *scalar1 = dynamic_cast<Scalar *>(para1);
  double distance = 0;

  if (scalar1) {
    Scalar *ret = DatumGetValue<Scalar *>(params[0]);
    double length1 = 0.0, length2 = 0.0;
    auto arrayLeft = DatumGetValue<Vector *>(scalar1->value);
    assert(arrayLeft->getSelected() == nullptr && arrayLeft->hasNullValue());
    const POD *__restrict__ data1 =
        reinterpret_cast<const POD *>(arrayLeft->getValue());
    const bool *__restrict__ nulls1 = arrayLeft->getNulls();
    uint64_t nums1 = arrayLeft->getNumOfRowsPlain();
    uint64_t i;
    if (nums1 < nums2) {
      for (i = 0; i < nums1; i++) {
        auto value1 = data1[i];
        auto isNull1 = nulls1[i];
        auto value2 = data2[i];
        auto isNull2 = nulls2[i];
        if (isNull1 || isNull2)
          LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                    "array should not contain NULL");
        length1 += value1 * value1;
        length2 += value2 * value2;
        distance += value1 * value2;
      }
      for (; i < nums2; i++) {
        auto value2 = data2[i];
        auto isNull2 = nulls2[i];
        if (isNull2)
          LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                    "array should not contain NULL");
        length2 += value2 * value2;
      }
    } else {
      for (i = 0; i < nums2; i++) {
        auto value1 = data1[i];
        auto isNull1 = nulls1[i];
        auto value2 = data2[i];
        auto isNull2 = nulls2[i];
        if (isNull1 || isNull2)
          LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                    "array should not contain NULL");
        length1 += value1 * value1;
        length2 += value2 * value2;
        distance += value1 * value2;
      }
      for (; i < nums1; i++) {
        auto value1 = data1[i];
        auto isNull1 = nulls1[i];
        if (isNull1)
          LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                    "array should not contain NULL");
        length2 += value1 * value1;
      }
    }
    distance = distance / sqrt(length1) / sqrt(length2);
    ret->value = CreateDatum(distance);
    return CreateDatum(ret);
  } else {
    ListVector *vec1 = reinterpret_cast<ListVector *>(para1);
    Vector *retVec = DatumGetValue<Vector *>(params[0]);
    SelectList *sel = vec1->getSelected();
    for (uint64_t i = 0; i < nums2; i++) {
      if (nulls2[i])
        LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                  "array should not contain NULL");
    }
    uint64_t rowNum = vec1->getNumOfRows();
    const uint64_t plainRowNum = vec1->getNumOfRowsPlain();
    dbcommon::ByteBuffer &retDataBuf = *retVec->getValueBuffer();
    retDataBuf.resize(plainRowNum * sizeof(double));
    double *__restrict__ retData =
        reinterpret_cast<double *>(retDataBuf.data());
    const uint64_t *__restrict__ offsets = vec1->getOffsets();
    const POD *__restrict__ array1all =
        reinterpret_cast<const POD *>(vec1->getValue());

    retVec->setHasNull(vec1->hasNullValue());
    if (vec1->hasNullValue()) {
      dbcommon::BoolBuffer *__restrict__ nulls = vec1->getNullBuffer();
      dbcommon::BoolBuffer &retNullBuf = *retVec->getNullBuffer();
      retNullBuf.resize(plainRowNum);
      char *__restrict__ retNull = retNullBuf.getChars();

      if (vec1->getSelected()) {
        retVec->setSelected(vec1->getSelected(), false);
#pragma clang loop vectorize(enable)
        for (uint64_t i = 0; i < rowNum; i++) {
          auto idx = (*sel)[i];
          uint64_t nums1, width;
          bool null;
          auto array1 = reinterpret_cast<const POD *>(
              vec1->read(idx, &nums1, &width, &null));
          if (!null) {
            double length1 = 0.0, length2 = 0.0;
            distance = 0.0;
            if (nums1 < nums2) {
              cosine_distance_intrinsic<POD>(nums1, array1, data2, &length1,
                                             &length2, &distance);
              cosine_distance_intrinsic<POD>(nums2 - nums1, data2 + nums1,
                                             &length2);
            } else {
              cosine_distance_intrinsic<POD>(nums2, array1, data2, &length1,
                                             &length2, &distance);
              cosine_distance_intrinsic<POD>(nums1 - nums2, array1 + nums2,
                                             &length1);
            }
            distance = distance / sqrt(length1) / sqrt(length2);
          }
          retData[idx] = distance;
          retNull[idx] = null;
        }
      } else {
#pragma clang loop vectorize(enable)
        for (uint64_t i = 0; i < rowNum; i++) {
          auto idx = i;
          uint64_t nums1, width;
          bool null;
          auto array1 = reinterpret_cast<const POD *>(
              vec1->read(idx, &nums1, &width, &null));
          if (!null) {
            double length1 = 0.0, length2 = 0.0;
            distance = 0.0;
            if (nums1 < nums2) {
              cosine_distance_intrinsic<POD>(nums1, array1, data2, &length1,
                                             &length2, &distance);
              cosine_distance_intrinsic<POD>(nums2 - nums1, data2 + nums1,
                                             &length2);
            } else {
              cosine_distance_intrinsic<POD>(nums2, array1, data2, &length1,
                                             &length2, &distance);
              cosine_distance_intrinsic<POD>(nums1 - nums2, array1 + nums2,
                                             &length1);
            }
            distance = distance / sqrt(length1) / sqrt(length2);
          }
          retData[idx] = distance;
          retNull[idx] = null;
        }
      }
    } else {
      if (vec1->getSelected()) {
        retVec->setSelected(vec1->getSelected(), false);
#pragma clang loop vectorize(enable)
        for (uint64_t i = 0; i < rowNum; i++) {
          auto idx = (*sel)[i];
          uint64_t nums1, width;
          bool null;
          auto array1 = reinterpret_cast<const POD *>(
              vec1->read(idx, &nums1, &width, &null));
          double length1 = 0.0, length2 = 0.0;
          distance = 0.0;
          if (nums1 < nums2) {
            cosine_distance_intrinsic<POD>(nums1, array1, data2, &length1,
                                           &length2, &distance);
            cosine_distance_intrinsic<POD>(nums2 - nums1, data2 + nums1,
                                           &length2);
          } else {
            cosine_distance_intrinsic<POD>(nums2, array1, data2, &length1,
                                           &length2, &distance);
            cosine_distance_intrinsic<POD>(nums1 - nums2, array1 + nums2,
                                           &length1);
          }
          distance = distance / sqrt(length1) / sqrt(length2);
          retData[idx] = distance;
        }
      } else {
#pragma clang loop vectorize(enable)
        for (uint64_t i = 0; i < rowNum; i++) {
          auto idx = i;
          uint64_t nums1, width;
          bool null;
          auto array1 = reinterpret_cast<const POD *>(
              vec1->read(idx, &nums1, &width, &null));
          double length1 = 0.0, length2 = 0.0;
          distance = 0.0;
          if (nums1 < nums2) {
            cosine_distance_intrinsic<POD>(nums1, array1, data2, &length1,
                                           &length2, &distance);
            cosine_distance_intrinsic<POD>(nums2 - nums1, data2 + nums1,
                                           &length2);
          } else {
            cosine_distance_intrinsic<POD>(nums2, array1, data2, &length1,
                                           &length2, &distance);
            cosine_distance_intrinsic<POD>(nums1 - nums2, array1 + nums2,
                                           &length1);
          }
          distance = distance / sqrt(length1) / sqrt(length2);
          retData[idx] = distance;
        }
      }
    }

    return CreateDatum(retVec);
  }
}

template <typename T>
bool isOverlap(const T *array, bool containNull1, const uint64_t nums,
               std::set<T> *values, bool containNull2,
               OverlapType overlapType) {
  bool ret;
  uint64_t containNum = 0;
  uint64_t i = 0;
  std::set<T> containValues;
  switch (overlapType) {
    case OVERLAP:
      ret = false;
      for (i = 0; i < nums; i++) {
        if (values->find(array[i]) != values->end()) {
          ret = true;
          break;
        }
      }
      break;
    case CONTAINS:
      if (containNull2 || nums < values->size()) {
        ret = false;
        break;
      }
      containNum = 0;
      for (i = 0; i < nums; i++) {
        if (values->find(array[i]) != values->end()) {
          if (containValues.find(array[i]) == containValues.end()) {
            containValues.insert(array[i]);
            containNum++;
          }
        }
      }
      if (containNum == values->size())
        ret = true;
      else
        ret = false;
      break;
    case CONTAINED:
      if (containNull1) {
        ret = false;
        break;
      }
      ret = true;
      for (i = 0; i < nums; i++) {
        if (values->find(array[i]) == values->end()) {
          ret = false;
          break;
        }
      }
      break;
    default:
      LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                "Invalid array function type %d, only support "
                "1(overlap)/2(contains)/3(contained) now",
                overlapType);
  }
  return ret;
}

template <typename POD>
Datum array_overlap_internal(Datum *params, uint64_t size,
                             OverlapType overlapType) {
  assert(size == 3);

  Object *para2 = DatumGetValue<Object *>(params[2]);
  Scalar *scalar2 = dynamic_cast<Scalar *>(para2);
  if (!scalar2)
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "Second parameter should be const value");
  auto arrayRight = DatumGetValue<Vector *>(scalar2->value);
  assert(arrayRight->getSelected() == nullptr && arrayRight->hasNullValue());
  uint64_t nums2 = arrayRight->getNumOfRowsPlain();
  const POD *__restrict__ data2 =
      reinterpret_cast<const POD *>(arrayRight->getValue());
  const bool *__restrict__ nulls2 = arrayRight->getNulls();

  Object *para1 = DatumGetValue<Object *>(params[1]);
  Scalar *scalar1 = dynamic_cast<Scalar *>(para1);
  bool overlap = false;

  std::set<POD> values;
  bool containNull2 = false;
  for (uint64_t i = 0; i < nums2; i++) {
    auto value2 = data2[i];
    auto isNull2 = nulls2[i];
    if (!isNull2)
      values.insert(value2);
    else
      containNull2 = true;
  }

  if (scalar1) {
    Scalar *ret = DatumGetValue<Scalar *>(params[0]);
    auto arrayLeft = DatumGetValue<Vector *>(scalar1->value);
    assert(arrayLeft->getSelected() == nullptr && arrayLeft->hasNullValue());
    const POD *__restrict__ data1 =
        reinterpret_cast<const POD *>(arrayLeft->getValue());
    const bool *__restrict__ nulls1 = arrayLeft->getNulls();
    uint64_t nums1 = arrayLeft->getNumOfRowsPlain();
    bool containNull1;
    for (uint64_t i = 0; i < nums1; i++) {
      if (nulls1[i]) containNull1 = true;
    }
    if (isOverlap<POD>(data1, containNull1, nums1, &values, containNull2,
                       overlapType))
      overlap = true;
    ret->value = CreateDatum(overlap);
    return CreateDatum(ret);
  } else {
    ListVector *vec1 = reinterpret_cast<ListVector *>(para1);
    SelectList *ret = DatumGetValue<SelectList *>(params[0]);
    SelectList *sel = vec1->getSelected();
    uint64_t rowNum = vec1->getNumOfRows();
    const uint64_t *__restrict__ offsets = vec1->getOffsets();
    const POD *__restrict__ array1all =
        reinterpret_cast<const POD *>(vec1->getValue());

    if (vec1->hasNullValue()) {
      dbcommon::BoolBuffer *__restrict__ nulls = vec1->getNullBuffer();
      if (vec1->getChildVector(0)->hasNullValue()) {
        dbcommon::BoolBuffer *__restrict__ valueNulls =
            vec1->getChildVector(0)->getNullBuffer();
        if (vec1->getSelected()) {
          for (uint64_t i = 0; i < rowNum; i++) {
            auto idx = (*sel)[i];
            uint64_t nums1 = offsets[idx + 1] - offsets[idx];
            bool null = nulls->getChar(idx);
            auto array1 = array1all + offsets[idx];
            if (null) continue;

            bool containNull1 = false;
            for (size_t j = 0; j < nums1; j++) {
              if (valueNulls->get(offsets[idx] + j)) {
                containNull1 = true;
                break;
              }
            }
            if (isOverlap<POD>(array1, containNull1, nums1, &values,
                               containNull2, overlapType))
              ret->push_back(idx);
          }
        } else {
          for (uint64_t i = 0; i < rowNum; i++) {
            auto idx = i;
            uint64_t nums1 = offsets[idx + 1] - offsets[idx];
            bool null = nulls->getChar(idx);
            auto array1 = array1all + offsets[idx];
            if (null) continue;

            bool containNull1 = false;
            for (size_t j = 0; j < nums1; j++) {
              if (valueNulls->get(offsets[idx] + j)) {
                containNull1 = true;
                break;
              }
            }
            if (isOverlap<POD>(array1, containNull1, nums1, &values,
                               containNull2, overlapType))
              ret->push_back(idx);
          }
        }
      } else {
        if (vec1->getSelected()) {
          for (uint64_t i = 0; i < rowNum; i++) {
            auto idx = (*sel)[i];
            uint64_t nums1 = offsets[idx + 1] - offsets[idx];
            bool null = nulls->getChar(idx);
            auto array1 = array1all + offsets[idx];
            if (null) continue;

            if (isOverlap<POD>(array1, false, nums1, &values, containNull2,
                               overlapType))
              ret->push_back(idx);
          }
        } else {
          for (uint64_t i = 0; i < rowNum; i++) {
            auto idx = i;
            uint64_t nums1 = offsets[idx + 1] - offsets[idx];
            bool null = nulls->getChar(idx);
            auto array1 = array1all + offsets[idx];
            if (null) continue;

            if (isOverlap<POD>(array1, false, nums1, &values, containNull2,
                               overlapType))
              ret->push_back(idx);
          }
        }
      }
    } else {
      if (vec1->getChildVector(0)->hasNullValue()) {
        dbcommon::BoolBuffer *__restrict__ valueNulls =
            vec1->getChildVector(0)->getNullBuffer();
        if (vec1->getSelected()) {
          for (uint64_t i = 0; i < rowNum; i++) {
            auto idx = (*sel)[i];
            uint64_t nums1 = offsets[idx + 1] - offsets[idx];
            auto array1 = array1all + offsets[idx];
            bool containNull1 = false;
            for (size_t j = 0; j < nums1; j++) {
              if (valueNulls->get(offsets[idx] + j)) {
                containNull1 = true;
                break;
              }
            }
            if (isOverlap<POD>(array1, containNull1, nums1, &values,
                               containNull2, overlapType))
              ret->push_back(idx);
          }
        } else {
          for (uint64_t i = 0; i < rowNum; i++) {
            auto idx = i;
            uint64_t nums1 = offsets[idx + 1] - offsets[idx];
            auto array1 = array1all + offsets[idx];
            bool containNull1 = false;
            for (size_t j = 0; j < nums1; j++) {
              if (valueNulls->get(offsets[idx] + j)) {
                containNull1 = true;
                break;
              }
            }
            if (isOverlap<POD>(array1, containNull1, nums1, &values,
                               containNull2, overlapType))
              ret->push_back(idx);
          }
        }
      } else {
        if (vec1->getSelected()) {
          for (uint64_t i = 0; i < rowNum; i++) {
            auto idx = (*sel)[i];
            uint64_t nums1 = offsets[idx + 1] - offsets[idx];
            auto array1 = array1all + offsets[idx];
            if (isOverlap<POD>(array1, false, nums1, &values, containNull2,
                               overlapType))
              ret->push_back(idx);
          }
        } else {
          for (uint64_t i = 0; i < rowNum; i++) {
            auto idx = i;
            uint64_t nums1 = offsets[idx + 1] - offsets[idx];
            auto array1 = array1all + offsets[idx];
            if (isOverlap<POD>(array1, false, nums1, &values, containNull2,
                               overlapType))
              ret->push_back(idx);
          }
        }
      }
    }

    return CreateDatum(ret);
  }
}

Datum float_array_euclidean_metric(Datum *params, uint64_t size) {
  return array_euclidean_metric_internal<float>(params, size);
}

Datum double_array_euclidean_metric(Datum *params, uint64_t size) {
  return array_euclidean_metric_internal<double>(params, size);
}

Datum float_array_cosine_distance(Datum *params, uint64_t size) {
  return array_cosine_distance_internal<float>(params, size);
}

Datum double_array_cosine_distance(Datum *params, uint64_t size) {
  return array_cosine_distance_internal<double>(params, size);
}

Datum bigint_array_overlap(Datum *params, uint64_t size) {
  return array_overlap_internal<int64_t>(params, size, OVERLAP);
}

Datum bigint_array_contains(Datum *params, uint64_t size) {
  return array_overlap_internal<int64_t>(params, size, CONTAINS);
}

Datum bigint_array_contained(Datum *params, uint64_t size) {
  return array_overlap_internal<int64_t>(params, size, CONTAINED);
}

}  // namespace dbcommon
