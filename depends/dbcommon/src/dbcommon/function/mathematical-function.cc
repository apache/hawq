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

#include "dbcommon/function/mathematical-function.h"

#include <vector>

#include "dbcommon/common/vector-transformer.h"
#include "dbcommon/function/arithmetic-function.h"
#include "dbcommon/function/decimal-function.h"

namespace dbcommon {

Datum double_abs(Datum *params, uint64_t size) {
  return op_type<abs<double>, double>(params, size);
}

Datum float_abs(Datum *params, uint64_t size) {
  return op_type<abs<float>, float>(params, size);
}

Datum int64_abs(Datum *params, uint64_t size) {
  return op_type<abs<int64_t>, int64_t>(params, size);
}

Datum int32_abs(Datum *params, uint64_t size) {
  return op_type<abs<int32_t>, int32_t>(params, size);
}

Datum int16_abs(Datum *params, uint64_t size) {
  return op_type<abs<int16_t>, int16_t>(params, size);
}

Datum double_cbrt(Datum *params, uint64_t size) {
  return op_type<cbrt, double>(params, size);
}

Datum double_sqrt(Datum *params, uint64_t size) {
  return op_type<sqrt, double, false, false, true>(params, size);
}

Datum decimal_sqrt(Datum *params, uint64_t size) {
  return decimal_op<sqrt>(params, size);
}

Datum int16_binary_not(Datum *params, uint64_t size) {
  return op_type<binary_not<int16_t>, int16_t>(params, size);
}

Datum int32_binary_not(Datum *params, uint64_t size) {
  return op_type<binary_not<int32_t>, int32_t>(params, size);
}

Datum int64_binary_not(Datum *params, uint64_t size) {
  return op_type<binary_not<int64_t>, int64_t>(params, size);
}

Datum int16_binary_shift_left(Datum *params, uint64_t size) {
  return type1_op_type2<int16_t, binary_shift_left<int16_t>, int32_t, int16_t,
                        false>(params, size);
}

Datum int32_binary_shift_left(Datum *params, uint64_t size) {
  return type1_op_type2<int32_t, binary_shift_left<int32_t>, int32_t, int32_t,
                        false>(params, size);
}

Datum int64_binary_shift_left(Datum *params, uint64_t size) {
  return type1_op_type2<int64_t, binary_shift_left<int64_t>, int32_t, int64_t,
                        false>(params, size);
}

Datum int16_binary_shift_right(Datum *params, uint64_t size) {
  return type1_op_type2<int16_t, binary_shift_right<int16_t>, int32_t, int16_t,
                        false>(params, size);
}

Datum int32_binary_shift_right(Datum *params, uint64_t size) {
  return type1_op_type2<int32_t, binary_shift_right<int32_t>, int32_t, int32_t,
                        false>(params, size);
}

Datum int64_binary_shift_right(Datum *params, uint64_t size) {
  return type1_op_type2<int64_t, binary_shift_right<int64_t>, int32_t, int64_t,
                        false>(params, size);
}

Datum int16_binary_and(Datum *params, uint64_t size) {
  return type1_op_type2<int16_t, std::bit_and<int16_t>, int16_t, int16_t,
                        false>(params, size);
}

Datum int32_binary_and(Datum *params, uint64_t size) {
  return type1_op_type2<int32_t, std::bit_and<int32_t>, int32_t, int32_t,
                        false>(params, size);
}

Datum int64_binary_and(Datum *params, uint64_t size) {
  return type1_op_type2<int64_t, std::bit_and<int64_t>, int64_t, int64_t,
                        false>(params, size);
}

Datum int16_binary_or(Datum *params, uint64_t size) {
  return type1_op_type2<int16_t, std::bit_or<int16_t>, int16_t, int16_t, false>(
      params, size);
}

Datum int32_binary_or(Datum *params, uint64_t size) {
  return type1_op_type2<int32_t, std::bit_or<int32_t>, int32_t, int32_t, false>(
      params, size);
}

Datum int64_binary_or(Datum *params, uint64_t size) {
  return type1_op_type2<int64_t, std::bit_or<int64_t>, int64_t, int64_t, false>(
      params, size);
}

Datum int16_binary_xor(Datum *params, uint64_t size) {
  return type1_op_type2<int16_t, std::bit_xor<int16_t>, int16_t, int16_t,
                        false>(params, size);
}

Datum int32_binary_xor(Datum *params, uint64_t size) {
  return type1_op_type2<int32_t, std::bit_xor<int32_t>, int32_t, int32_t,
                        false>(params, size);
}

Datum int64_binary_xor(Datum *params, uint64_t size) {
  return type1_op_type2<int64_t, std::bit_xor<int64_t>, int64_t, int64_t,
                        false>(params, size);
}

Datum int16_mod(Datum *params, uint64_t size) {
  return type1_op_type2<int16_t, mod<int16_t, int16_t, int16_t>, int16_t,
                        int16_t, true>(params, size);
}

Datum int32_mod(Datum *params, uint64_t size) {
  return type1_op_type2<int32_t, mod<int32_t, int32_t, int32_t>, int32_t,
                        int32_t, true>(params, size);
}

Datum int16_32_mod(Datum *params, uint64_t size) {
  return type1_op_type2<int16_t, mod<int32_t, int16_t, int32_t>, int32_t,
                        int32_t, true>(params, size);
}

Datum int32_16_mod(Datum *params, uint64_t size) {
  return type1_op_type2<int32_t, mod<int32_t, int32_t, int16_t>, int16_t,
                        int32_t, true>(params, size);
}

Datum int64_mod(Datum *params, uint64_t size) {
  return type1_op_type2<int64_t, mod<int64_t, int64_t, int64_t>, int64_t,
                        int64_t, true>(params, size);
}

Datum double_pow(Datum *params, uint64_t size) {
  return type1_op_type2<double, pow<double>, double, double, false>(params,
                                                                    size);
}

Datum decimal_pow(Datum *params, uint64_t size) {
  return decimal_op_two_decimal<pow<DecimalVar>>(params, size);
}

Datum double_ceil(Datum *params, uint64_t size) {
  return op_type<ceil<double>, double>(params, size);
}

Datum double_floor(Datum *params, uint64_t size) {
  return op_type<floor<double>, double>(params, size);
}

Datum double_round(Datum *params, uint64_t size) {
  return op_type<round<double>, double>(params, size);
}

Datum double_trunc(Datum *params, uint64_t size) {
  return op_type<trunc<double>, double>(params, size);
}

Datum double_sign(Datum *params, uint64_t size) {
  return op_type<sign<double>, double>(params, size);
}

Datum double_exp(Datum *params, uint64_t size) {
  return op_type<exp<double>, double, false, true>(params, size);
}

Datum decimal_exp(Datum *params, uint64_t size) {
  return decimal_op<exp<DecimalVar>>(params, size);
}

Datum double_ln(Datum *params, uint64_t size) {
  return op_type<ln<double>, double, true>(params, size);
}

Datum decimal_ln(Datum *params, uint64_t size) {
  return decimal_op<ln<DecimalVar>>(params, size);
}

Datum double_lg(Datum *params, uint64_t size) {
  return op_type<lg<double>, double, true>(params, size);
}

Datum double_log(Datum *params, uint64_t size) {
  return type1_op_type2<double, logx<double>, double, double, false>(params,
                                                                     size);
}

Datum decimal_log(Datum *params, uint64_t size) {
  return decimal_op_two_decimal<logx<DecimalVar>>(params, size);
}

Datum double_acos(Datum *params, uint64_t size) {
  return op_type<acos<double>, double>(params, size);
}

Datum double_asin(Datum *params, uint64_t size) {
  return op_type<asin<double>, double>(params, size);
}

Datum double_atan(Datum *params, uint64_t size) {
  return op_type<atan<double>, double>(params, size);
}

Datum double_atan2(Datum *params, uint64_t size) {
  return type1_op_type2<double, atan2<double>, double, double, false>(params,
                                                                      size);
}

Datum double_cos(Datum *params, uint64_t size) {
  return op_type<cos<double>, double>(params, size);
}

Datum double_cot(Datum *params, uint64_t size) {
  return op_type<cot<double>, double>(params, size);
}

Datum double_sin(Datum *params, uint64_t size) {
  return op_type<sin<double>, double>(params, size);
}

Datum double_tan(Datum *params, uint64_t size) {
  return op_type<tan<double>, double>(params, size);
}

Datum decimal_fac(Datum *params, uint64_t size) {
  Object *para = params[1];
  bool isVector = dynamic_cast<Vector *>(para);
  bool isInValid = false;

  // Check if this argument will cause an overflow
  if (isVector) {
    Vector *srcVector = params[1];
    FixedSizeTypeVectorRawData<int64_t> vec(srcVector);
    auto checkIfValid = [&](SelectList::value_type plainIdx) {
      isInValid |= (vec.values[plainIdx] < -34 || vec.values[plainIdx] > 34);
    };
    transformVector(vec.plainSize, vec.sel, vec.nulls, checkIfValid);
  } else {
    Scalar *srcScalar = params[1];
    int64_t srcVal = DatumGetValue<int64_t>(srcScalar->value);
    isInValid |= (srcVal < -34 || srcVal > 34);
  }

  if (isInValid)
    LOG_ERROR(ERRCODE_INTERVAL_FIELD_OVERFLOW, "value out of range: overflow");

  // convert int64_t type to decimal type
  std::vector<Datum> newParams{CreateDatum(0), params[1]};
  Vector::uptr rltVector = Vector::BuildVector(TypeKind::DECIMALNEWID, true);
  DecimalVar zero = DecimalVar(0, 0, 0);
  Scalar rltScalar(CreateDatum(&zero));

  if (isVector) {
    newParams[0] = CreateDatum(rltVector.get());
  } else {
    newParams[0] = CreateDatum(&rltScalar);
  }

  Datum rlt = int64_to_decimal(newParams.data(), newParams.size());

  std::vector<Datum> tmpParams{params[0], rlt};

  return op_decimal<fac>(tmpParams.data(), tmpParams.size());
}

}  // namespace dbcommon
