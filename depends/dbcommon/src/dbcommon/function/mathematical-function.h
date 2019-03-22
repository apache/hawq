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

#ifndef DBCOMMON_SRC_DBCOMMON_FUNCTION_MATHEMATICAL_FUNCTION_H_
#define DBCOMMON_SRC_DBCOMMON_FUNCTION_MATHEMATICAL_FUNCTION_H_

#include <algorithm>
#include <cmath>
#include <vector>

#include "dbcommon/common/vector-transformer.h"
#include "dbcommon/common/vector/fixed-length-vector.h"
#include "dbcommon/function/decimal-function.h"
#include "dbcommon/function/typecast-func.cg.h"

namespace dbcommon {

Datum double_abs(Datum *params, uint64_t size);
Datum float_abs(Datum *params, uint64_t size);
Datum int64_abs(Datum *params, uint64_t size);
Datum int32_abs(Datum *params, uint64_t size);
Datum int16_abs(Datum *params, uint64_t size);

// cbrt function
Datum double_cbrt(Datum *params, uint64_t size);

// sqrt function
Datum double_sqrt(Datum *params, uint64_t size);

// binary_not function
Datum int16_binary_not(Datum *params, uint64_t size);
Datum int32_binary_not(Datum *params, uint64_t size);
Datum int64_binary_not(Datum *params, uint64_t size);

// binary_shift function
Datum int16_binary_shift_left(Datum *params, uint64_t size);
Datum int32_binary_shift_left(Datum *params, uint64_t size);
Datum int64_binary_shift_left(Datum *params, uint64_t size);

// binary_right function
Datum int16_binary_shift_right(Datum *params, uint64_t size);
Datum int32_binary_shift_right(Datum *params, uint64_t size);
Datum int64_binary_shift_right(Datum *params, uint64_t size);

// binary_and function
Datum int16_binary_and(Datum *params, uint64_t size);
Datum int32_binary_and(Datum *params, uint64_t size);
Datum int64_binary_and(Datum *params, uint64_t size);

// binary_or function
Datum int16_binary_or(Datum *params, uint64_t size);
Datum int32_binary_or(Datum *params, uint64_t size);
Datum int64_binary_or(Datum *params, uint64_t size);

// binary_xor function
Datum int16_binary_xor(Datum *params, uint64_t size);
Datum int32_binary_xor(Datum *params, uint64_t size);
Datum int64_binary_xor(Datum *params, uint64_t size);
// mod function
Datum int16_mod(Datum *params, uint64_t size);
Datum int32_mod(Datum *params, uint64_t size);
Datum int16_32_mod(Datum *params, uint64_t size);
Datum int32_16_mod(Datum *params, uint64_t size);
Datum int64_mod(Datum *params, uint64_t size);

// pow function
Datum double_pow(Datum *params, uint64_t size);

// rounding function
Datum double_ceil(Datum *params, uint64_t size);
Datum double_floor(Datum *params, uint64_t size);
Datum double_round(Datum *params, uint64_t size);
Datum double_trunc(Datum *params, uint64_t size);

// sign function
Datum double_sign(Datum *params, uint64_t size);

// exponential function
Datum double_exp(Datum *params, uint64_t size);

// logarithm function
Datum double_ln(Datum *params, uint64_t size);
Datum double_lg(Datum *params, uint64_t size);
Datum double_log(Datum *params, uint64_t size);

// trigonometric function
Datum double_acos(Datum *params, uint64_t size);
Datum double_asin(Datum *params, uint64_t size);
Datum double_atan(Datum *params, uint64_t size);
Datum double_atan2(Datum *params, uint64_t size);
Datum double_cos(Datum *params, uint64_t size);
Datum double_cot(Datum *params, uint64_t size);
Datum double_sin(Datum *params, uint64_t size);
Datum double_tan(Datum *params, uint64_t size);

Datum decimal_sqrt(Datum *params, uint64_t size);
Datum decimal_exp(Datum *params, uint64_t size);
Datum decimal_ln(Datum *params, uint64_t size);
Datum decimal_log(Datum *params, uint64_t size);
Datum decimal_lg(Datum *params, uint64_t size);
Datum decimal_fac(Datum *params, uint64_t size);
Datum decimal_pow(Datum *params, uint64_t size);

template <typename Type>
struct abs {
  Type operator()(Type x) { return x >= 0 ? x : -x; }
};

template <>
struct abs<DecimalVar> {
  DecimalVar operator()(DecimalVar x) {
    Int128 tmp(x.highbits, x.lowbits);
    tmp = tmp.abs();
    return DecimalVar(tmp.getHighBits(), tmp.getLowBits(), x.scale);
  }
};

struct cbrt {
  double operator()(double x) { return std::cbrt(x); }
};

struct sqrt {
  double operator()(double x) { return std::sqrt(x); }
  Datum operator()(Datum *x, uint64_t y) { return double_sqrt(x, y); }
};

template <typename Type>
struct binary_not {
  Type operator()(Type x) { return ~x; }
};

template <typename Type>
struct binary_shift_left {
  Type operator()(Type x, int32_t y) { return x << y; }
};

template <typename Type>
struct binary_shift_right {
  Type operator()(Type x, int32_t y) { return x >> y; }
};

template <typename Typeret, typename Typelhs, typename Typerhs>
struct mod {
  Typeret operator()(Typelhs x, Typerhs y) { return x % y; }
};

template <>
struct mod<DecimalVar, DecimalVar, DecimalVar> {
  DecimalVar operator()(DecimalVar x, DecimalVar y) {
    Int128 remainder;
    Int128 dividend(x.highbits, x.lowbits), divisor(y.highbits, y.lowbits);
    int64_t diffScale = x.scale - y.scale;
    if (diffScale > 0) {
      divisor = scaleUpInt128ByPowerOfTen(divisor, diffScale);
    } else {
      dividend = scaleUpInt128ByPowerOfTen(dividend, std::abs(diffScale));
    }
    dividend.divide(divisor, remainder);
    return DecimalVar(remainder.getHighBits(), remainder.getLowBits(),
                      std::max(x.scale, y.scale));
  }
};

template <typename Type>
struct pow {
  Type operator()(Type x, Type y) {
    if (x == 0 && y < 0)
      LOG_ERROR(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION,
                "zero raised to a negative power is undefined");
    if (x < 0 && (std::floor(y) != y))
      LOG_ERROR(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION,
                "invalid argument for power function");
    Type ret = std::pow(x, y);
    if (isinf(ret))
      LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                "value out of range: overflow");
    return ret;
  }
  Datum operator()(Datum *x, uint64_t y) { return double_pow(x, y); }
};

template <typename Type>
struct ceil {
  Type operator()(Type x) { return std::ceil(x); }
};

template <>
struct ceil<DecimalVar> {
  DecimalVar operator()(DecimalVar x) { return x.ceil(); }
};

template <typename Type>
struct floor {
  Type operator()(Type x) { return std::floor(x); }
};

template <>
struct floor<DecimalVar> {
  DecimalVar operator()(DecimalVar x) { return x.floor(); }
};

template <typename Type>
struct round {
  Type operator()(Type x) { return std::round(x); }
};

template <>
struct round<DecimalVar> {
  DecimalVar operator()(DecimalVar x, int32_t y = 0) { return x.round(y); }
};

template <typename Type>
struct trunc {
  Type operator()(Type x) { return std::trunc(x); }
};

template <>
struct trunc<DecimalVar> {
  DecimalVar operator()(DecimalVar x, int32_t y = 0) { return x.trunc(y); }
};

template <typename Type>
struct sign {
  Type operator()(Type x) { return x != 0 ? (x > 0 ? 1 : -1) : 0; }
};

template <>
struct sign<DecimalVar> {
  DecimalVar operator()(DecimalVar x) {
    return (x.highbits == 0 && x.lowbits == 0)
               ? DecimalVar(0, 0, 0)
               : (x.highbits < 0 ? DecimalVar(-1, -1, 0) : DecimalVar(0, 1, 0));
  }
};

template <typename Type>
struct exp {
  Type operator()(Type x) { return std::exp(x); }
  Datum operator()(Datum *x, uint64_t y) { return double_exp(x, y); }
};

template <typename Type>
struct ln {
  Type operator()(Type x) { return std::log(x); }
  Datum operator()(Datum *x, uint64_t y) { return double_ln(x, y); }
};

template <typename Type>
struct lg {
  Type operator()(Type x) { return std::log(x) / std::log(10); }
};

template <typename Type>
struct logx {
  Type operator()(Type x, Type y) {
    if (x < 0 || y < 0)
      LOG_ERROR(ERRCODE_INVALID_ARGUMENT_FOR_LOG,
                "cannot take logarithm of a negative number");
    if (x == 0 || y == 0)
      LOG_ERROR(ERRCODE_INVALID_ARGUMENT_FOR_LOG,
                "cannot take logarithm of zero");
    if (x == 1) LOG_ERROR(ERRCODE_DIVISION_BY_ZERO, "divison by zero");
    return std::log(y) / std::log(x);
  }
  Datum operator()(Datum *x, uint64_t y) { return double_log(x, y); }
};

template <typename Type>
struct acos {
  Type operator()(Type x) { return std::acos(x); }
};

template <typename Type>
struct asin {
  Type operator()(Type x) { return std::asin(x); }
};

template <typename Type>
struct atan {
  Type operator()(Type x) { return std::atan(x); }
};

template <typename Type>
struct atan2 {
  Type operator()(Type x, Type y) { return std::atan2(x, y); }
};

template <typename Type>
struct cos {
  Type operator()(Type x) { return std::cos(x); }
};

template <typename Type>
struct cot {
  Type operator()(Type x) { return 1 / std::tan(x); }
};

template <typename Type>
struct sin {
  Type operator()(Type x) { return std::sin(x); }
};

template <typename Type>
struct tan {
  Type operator()(Type x) { return std::tan(x); }
};

struct fac {
  DecimalVar operator()(DecimalVar x) {
    DecimalVar one(0, 1, 0), ret(0, 1, 0);
    ret.cast(x.scale);
    if ((x.highbits == 0 && x.lowbits == 0) || x < DecimalVar(0, 0, 0))
      return ret;
    while (!(x.highbits == 0 && x.lowbits == 0)) {
      ret *= x;
      x -= one;
    }
    return ret;
  }
};

template <typename UnaryOperator, typename Type, bool isLogarithm = false,
          bool isExponential = false, bool isSqrt = false>
Datum op_type(Datum *params, uint64_t size) {
  assert(size == 2);
  Object *para = params[1];
  if (dynamic_cast<Vector *>(para)) {
    Vector *retVector = params[0];
    Vector *srcVector = params[1];

    if (isLogarithm) srcVector->checkLogarithmAgrumentError();

    if (isExponential) srcVector->checkExponentialArgumentError();

    if (isSqrt && srcVector->checkLessZero())
      LOG_ERROR(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION,
                "cannot take square root of a negative number");

    FixedSizeTypeVectorRawData<Type> src(srcVector);
    retVector->resize(src.plainSize, src.sel, src.nulls);
    FixedSizeTypeVectorRawData<Type> ret(retVector);

    auto abs = [&](uint64_t plainIdx) {
      ret.values[plainIdx] = UnaryOperator()(src.values[plainIdx]);
    };

    dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, abs);
  } else {
    Scalar *retScalar = params[0];
    Scalar *srcScalar = params[1];

    if (srcScalar->isnull) {
      retScalar->isnull = true;
    } else {
      retScalar->isnull = false;
      Type srcVal = DatumGetValue<Type>(srcScalar->value);

      if (isLogarithm) {
        if (srcVal < 0) {
          LOG_ERROR(ERRCODE_INVALID_ARGUMENT_FOR_LOG,
                    "cannot take logarithm of a negative number");
        } else if (srcVal == 0) {
          LOG_ERROR(ERRCODE_INVALID_ARGUMENT_FOR_LOG,
                    "cannot take logarithm of zero");
        }
      }

      if (isExponential) {
        if (srcVal < -708.4 || srcVal > 709.8)
          LOG_ERROR(ERRCODE_INTERVAL_FIELD_OVERFLOW,
                    "value out of range: overflow");
      }

      if (isSqrt) {
        if (srcVal < 0)
          LOG_ERROR(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION,
                    "cannot take square root of a negative number");
      }

      retScalar->value = CreateDatum<Type>(UnaryOperator()(srcVal));
    }
  }
  return params[0];
}

template <typename UnaryOperator>
Datum decimal_op(Datum *params, uint64_t size) {
  Object *para = params[1];
  bool isVector = dynamic_cast<Vector *>(para) ? true : false;

  Vector::uptr retVector = Vector::BuildVector(TypeKind::DOUBLEID, true);
  Scalar retScalar(CreateDatum<double>(0));
  std::vector<Datum> fisrtTmpParams{CreateDatum(0), params[1]};
  if (isVector) {
    fisrtTmpParams[0] = CreateDatum(retVector.get());
  } else {
    fisrtTmpParams[0] = CreateDatum(&retScalar);
  }

  Datum doubleRet =
      decimal_to_double(fisrtTmpParams.data(), fisrtTmpParams.size());

  std::vector<Datum> secondTmpParams{CreateDatum(0), doubleRet};
  if (isVector) {
    secondTmpParams[0] = CreateDatum(retVector.get());
  } else {
    secondTmpParams[0] = CreateDatum(&retScalar);
  }

  Datum sqrtVal =
      UnaryOperator()(secondTmpParams.data(), secondTmpParams.size());

  if (isVector) {
    bool overFlow = false;
    Vector *vecRet = sqrtVal;
    FixedSizeTypeVectorRawData<double> vec(vecRet);
    auto checkValid = [&](uint16_t plainIdx) {
      overFlow |=
          (vec.values[plainIdx] < -1e+38 || vec.values[plainIdx] > 1e+38);
      overFlow |= (vec.values[plainIdx] > -1e-38 &&
                   vec.values[plainIdx] < 1e-38 && vec.values[plainIdx] != 0);
    };
    transformVector(vec.plainSize, vec.sel, vec.nulls, checkValid);
    if (overFlow)
      LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                "value out of range: overflow");
  } else {
    Scalar *scalarRet = sqrtVal;
    bool overFlow = false;
    double retVal = DatumGetValue<double>(scalarRet->value);
    overFlow |= (retVal < -1e+38 || retVal > 1e+38);
    overFlow |= (retVal > -1e-38 && retVal < 1e-38 && retVal != 0);
    if (overFlow)
      LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                "value out of range: overflow");
  }

  Datum originPtr = params[1];
  params[1] = sqrtVal;
  Datum result = double_to_decimal(params, size);
  params[1] = originPtr;

  return result;
}

template <typename UnaryOperator>
Datum decimal_op_two_decimal(Datum *params, uint64_t size) {
  Object *paraL = params[1];
  Object *paraR = params[2];
  Vector *vecL = dynamic_cast<Vector *>(paraL);
  Vector *vecR = dynamic_cast<Vector *>(paraR);

  std::vector<Datum> lhsInParams{CreateDatum(0), params[1]};
  std::vector<Datum> rhsInParams{CreateDatum(0), params[2]};
  Vector::uptr lhsInResultVector =
      Vector::BuildVector(TypeKind::DOUBLEID, true);
  Vector::uptr rhsInResultVector =
      Vector::BuildVector(TypeKind::DOUBLEID, true);
  Scalar lhsInResultScalar(CreateDatum<double>(0));
  Scalar rhsInResultScalar(CreateDatum<double>(0));

  if (vecL) {
    if (vecR) {
      lhsInParams[0] = CreateDatum(lhsInResultVector.get());
      rhsInParams[0] = CreateDatum(rhsInResultVector.get());
    } else {
      lhsInParams[0] = CreateDatum(lhsInResultVector.get());
      rhsInParams[0] = CreateDatum(&lhsInResultScalar);
    }
  } else {
    if (vecR) {
      lhsInParams[0] = CreateDatum(&lhsInResultScalar);
      rhsInParams[0] = CreateDatum(rhsInResultVector.get());
    } else {
      lhsInParams[0] = CreateDatum(&lhsInResultScalar);
      rhsInParams[0] = CreateDatum(&rhsInResultScalar);
    }
  }

  Datum lhsFisrtRlt = decimal_to_double(lhsInParams.data(), lhsInParams.size());
  Datum rhsFisrtRlt = decimal_to_double(rhsInParams.data(), lhsInParams.size());

  std::vector<Datum> srcOutParams{CreateDatum(0), lhsFisrtRlt, rhsFisrtRlt};
  Vector::uptr srcOutVector = Vector::BuildVector(TypeKind::DOUBLEID, true);
  Scalar srcOutScalar(CreateDatum<double>(0));

  if (vecL || vecR) {
    srcOutParams[0] = CreateDatum(srcOutVector.get());
  } else {
    srcOutParams[0] = CreateDatum(&srcOutScalar);
  }

  Datum ret = UnaryOperator()(srcOutParams.data(), srcOutParams.size());

  if (vecL || vecR) {
    bool overFlow = false;
    Vector *vecRet = ret;
    FixedSizeTypeVectorRawData<double> vec(vecRet);
    auto checkValid = [&](uint16_t plainIdx) {
      overFlow |=
          (vec.values[plainIdx] < -1e+38 || vec.values[plainIdx] > 1e+38);
      overFlow |= (vec.values[plainIdx] > -1e-38 &&
                   vec.values[plainIdx] < 1e-38 && vec.values[plainIdx] != 0);
    };
    transformVector(vec.plainSize, vec.sel, vec.nulls, checkValid);
    if (overFlow)
      LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                "value out of range: overflow");
  } else {
    Scalar *scalarRet = ret;
    bool overFlow = false;
    double retVal = DatumGetValue<double>(scalarRet->value);
    overFlow |= (retVal < -1e+38 || retVal > 1e+38);
    overFlow |= (retVal > -1e-38 && retVal < 1e-38 && retVal != 0);
    if (overFlow)
      LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                "value out of range: overflow");
  }

  std::vector<Datum> finalParams{params[0], ret};

  return double_to_decimal(finalParams.data(), finalParams.size());
}

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_FUNCTION_MATHEMATICAL_FUNCTION_H_
