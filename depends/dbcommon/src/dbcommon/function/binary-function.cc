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

#include "dbcommon/function/string-binary-function.h"

#include "dbcommon/common/vector.h"
#include "dbcommon/common/vector/variable-length-vector.h"
#include "dbcommon/function/invoker.h"
#include "dbcommon/utils/macro.h"

namespace dbcommon {

typedef bool (*cmpWithZero)(int64_t x);
static inline bool LTZero(int64_t x) { return x < 0; }
static inline bool LEZero(int64_t x) { return x <= 0; }
static inline bool EQZero(int64_t x) { return x == 0; }
static inline bool NEZero(int64_t x) { return x != 0; }
static inline bool GEZero(int64_t x) { return x >= 0; }
static inline bool GTZero(int64_t x) { return x > 0; }

template <cmpWithZero cmpCondition>
static inline force_inline bool strCmp(const char *arg1Src, size_t len1,
                                       const char *arg2Src, size_t len2) {
  const unsigned char *__restrict__ arg1 =
      reinterpret_cast<const unsigned char *>(arg1Src);
  const unsigned char *__restrict__ arg2 =
      reinterpret_cast<const unsigned char *>(arg2Src);
  size_t len = std::min(len1, len2);
  for (int i = 0; i < len; ++i) {
    int result = arg1[i] - arg2[i];
    if (result) {
      return cmpCondition(result);
    }
  }
  return cmpCondition(len1 - len2);
}

template <>
inline force_inline bool strCmp<EQZero>(const char *s1, size_t len1,
                                        const char *s2, size_t len2) {
  if (len1 != len2) return false;
  auto len = len1;
  uint64_t idx = 0;
  while (idx + 8 <= len) {
    if (*reinterpret_cast<const uint64_t *>(s1 + idx) !=
        *reinterpret_cast<const uint64_t *>(s2 + idx))
      return false;
    idx += 8;
  }
  if (idx + 4 <= len) {
    if (*reinterpret_cast<const uint32_t *>(s1 + idx) !=
        *reinterpret_cast<const uint32_t *>(s2 + idx))
      return false;
    idx += 4;
  }
  if (idx + 2 <= len) {
    if (*reinterpret_cast<const uint16_t *>(s1 + idx) !=
        *reinterpret_cast<const uint16_t *>(s2 + idx))
      return false;
    idx += 2;
  }
  if (idx < len) {
    if (*reinterpret_cast<const uint8_t *>(s1 + idx) !=
        *reinterpret_cast<const uint8_t *>(s2 + idx))
      return false;
  }
  return true;
}

template <cmpWithZero condition>
Datum BINARY_VAL_VAL_CMP(Datum *params, uint64_t size) {
  assert(size == 3 && "invalid input");
  Scalar *ret = DatumGetValue<Scalar *>(params[0]);
  Scalar *left = DatumGetValue<Scalar *>(params[1]);
  Scalar *right = DatumGetValue<Scalar *>(params[2]);
  char *val1 = DatumGetValue<char *>(left->value);
  char *val2 = DatumGetValue<char *>(right->value);
  int res = strCmp<condition>(val1, left->length, val2, right->length);
  ret->value = CreateDatum(static_cast<bool>(res));
  return CreateDatum(ret);
}

template <cmpWithZero condition>
Datum BINARY_VAL_VEC_CMP(Datum *params, uint64_t size) {
  SelectList *retSelectList = params[0];
  Scalar *lhsScalar = params[1];
  Vector *rhsVector = params[2];

  rhsVector->trim();

  VariableSizeTypeVectorRawData rhs(rhsVector);
  retSelectList->setNulls(rhs.plainSize, rhs.sel, retSelectList->getNulls(),
                          rhs.nulls);
  SelectList::value_type *__restrict__ ret = retSelectList->begin();
  uint64_t counter = 0;
  auto cmp = [&](uint64_t plainIdx) {
    if (strCmp<condition>(lhsScalar->value, lhsScalar->length,
                          rhs.valptrs[plainIdx], rhs.lengths[plainIdx]))
      ret[counter++] = plainIdx;
  };
  transformVector(rhs.plainSize, rhs.sel, rhs.nulls, cmp);
  retSelectList->resize(counter);

  return params[0];
}

template <cmpWithZero condition>
Datum BINARY_VEC_VAL_CMP(Datum *params, uint64_t size) {
  SelectList *retSelectList = params[0];
  Vector *lhsVector = params[1];
  Scalar *rhsScalar = params[2];

  lhsVector->trim();

  VariableSizeTypeVectorRawData lhs(lhsVector);
  retSelectList->setNulls(lhs.plainSize, lhs.sel, retSelectList->getNulls(),
                          lhs.nulls);
  SelectList::value_type *__restrict__ ret = retSelectList->begin();
  uint64_t counter = 0;
  auto cmp = [&](uint64_t plainIdx) {
    if (strCmp<condition>(lhs.valptrs[plainIdx], lhs.lengths[plainIdx],
                          rhsScalar->value, rhsScalar->length))
      ret[counter++] = plainIdx;
  };
  transformVector(lhs.plainSize, lhs.sel, lhs.nulls, cmp);
  retSelectList->resize(counter);

  return params[0];
}

template <cmpWithZero condition>
Datum BINARY_VEC_VEC_CMP(Datum *params, uint64_t size) {
  SelectList *retSelectList = params[0];
  Vector *lhsVector = params[1];
  Vector *rhsVector = params[2];

  lhsVector->trim();
  rhsVector->trim();

  VariableSizeTypeVectorRawData lhs(lhsVector);
  VariableSizeTypeVectorRawData rhs(rhsVector);
  retSelectList->setNulls(lhs.plainSize, lhs.sel, retSelectList->getNulls(),
                          lhs.nulls, rhs.nulls);
  SelectList::value_type *__restrict__ ret = retSelectList->begin();
  uint64_t counter = 0;
  auto cmp = [&](uint64_t plainIdx) {
    if (strCmp<condition>(lhs.valptrs[plainIdx], lhs.lengths[plainIdx],
                          rhs.valptrs[plainIdx], rhs.lengths[plainIdx]))
      ret[counter++] = plainIdx;
  };
  transformVector(lhs.plainSize, lhs.sel, lhs.nulls, rhs.nulls, cmp);
  retSelectList->resize(counter);

  return params[0];
}

Datum binary_val_less_than_binary_val(Datum *params, uint64_t size) {
  return BINARY_VAL_VAL_CMP<LTZero>(params, size);
}

Datum binary_val_less_than_binary_vec(Datum *params, uint64_t size) {
  return BINARY_VAL_VEC_CMP<LTZero>(params, size);
}

Datum binary_vec_less_than_binary_val(Datum *params, uint64_t size) {
  return BINARY_VEC_VAL_CMP<LTZero>(params, size);
}

Datum binary_vec_less_than_binary_vec(Datum *params, uint64_t size) {
  return BINARY_VEC_VEC_CMP<LTZero>(params, size);
}

Datum binary_val_less_eq_binary_val(Datum *params, uint64_t size) {
  return BINARY_VAL_VAL_CMP<LEZero>(params, size);
}

Datum binary_val_less_eq_binary_vec(Datum *params, uint64_t size) {
  return BINARY_VAL_VEC_CMP<LEZero>(params, size);
}

Datum binary_vec_less_eq_binary_val(Datum *params, uint64_t size) {
  return BINARY_VEC_VAL_CMP<LEZero>(params, size);
}

Datum binary_vec_less_eq_binary_vec(Datum *params, uint64_t size) {
  return BINARY_VEC_VEC_CMP<LEZero>(params, size);
}

Datum binary_val_equal_binary_val(Datum *params, uint64_t size) {
  return BINARY_VAL_VAL_CMP<EQZero>(params, size);
}

Datum binary_val_equal_binary_vec(Datum *params, uint64_t size) {
  return BINARY_VAL_VEC_CMP<EQZero>(params, size);
}

Datum binary_vec_equal_binary_val(Datum *params, uint64_t size) {
  return BINARY_VEC_VAL_CMP<EQZero>(params, size);
}

Datum binary_vec_equal_binary_vec(Datum *params, uint64_t size) {
  return BINARY_VEC_VEC_CMP<EQZero>(params, size);
}

Datum binary_val_not_equal_binary_val(Datum *params, uint64_t size) {
  return BINARY_VAL_VAL_CMP<NEZero>(params, size);
}

Datum binary_val_not_equal_binary_vec(Datum *params, uint64_t size) {
  return BINARY_VAL_VEC_CMP<NEZero>(params, size);
}

Datum binary_vec_not_equal_binary_val(Datum *params, uint64_t size) {
  return BINARY_VEC_VAL_CMP<NEZero>(params, size);
}

Datum binary_vec_not_equal_binary_vec(Datum *params, uint64_t size) {
  return BINARY_VEC_VEC_CMP<NEZero>(params, size);
}

Datum binary_val_greater_than_binary_val(Datum *params, uint64_t size) {
  return BINARY_VAL_VAL_CMP<GTZero>(params, size);
}

Datum binary_val_greater_than_binary_vec(Datum *params, uint64_t size) {
  return BINARY_VAL_VEC_CMP<GTZero>(params, size);
}

Datum binary_vec_greater_than_binary_val(Datum *params, uint64_t size) {
  return BINARY_VEC_VAL_CMP<GTZero>(params, size);
}

Datum binary_vec_greater_than_binary_vec(Datum *params, uint64_t size) {
  return BINARY_VEC_VEC_CMP<GTZero>(params, size);
}

Datum binary_val_greater_eq_binary_val(Datum *params, uint64_t size) {
  return BINARY_VAL_VAL_CMP<GEZero>(params, size);
}

Datum binary_val_greater_eq_binary_vec(Datum *params, uint64_t size) {
  return BINARY_VAL_VEC_CMP<GEZero>(params, size);
}

Datum binary_vec_greater_eq_binary_val(Datum *params, uint64_t size) {
  return BINARY_VEC_VAL_CMP<GEZero>(params, size);
}

Datum binary_vec_greater_eq_binary_vec(Datum *params, uint64_t size) {
  return BINARY_VEC_VEC_CMP<GEZero>(params, size);
}

Datum binary_octet_length(Datum *params, uint64_t size) {
  assert(size == 2);
  Object *para = params[1];
  if (dynamic_cast<Vector *>(para)) {
    Vector *retVector = params[0];
    Vector *srcVector = params[1];

    VariableSizeTypeVectorRawData src(srcVector);
    retVector->resize(src.plainSize, src.sel, src.nulls);
    FixedSizeTypeVectorRawData<int32_t> ret(retVector);

    auto copyLength = [&](uint64_t plainIdx) {
      ret.values[plainIdx] = src.lengths[plainIdx];
    };
    dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, copyLength);

  } else {
    Scalar *retScalar = params[0];
    Scalar *srcScalar = params[1];

    if (srcScalar->isnull) {
      retScalar->isnull = true;
    } else {
      retScalar->isnull = false;
      retScalar->value = CreateDatum<int32_t>(srcScalar->length);
    }
  }
  return params[0];
}

}  // namespace dbcommon
