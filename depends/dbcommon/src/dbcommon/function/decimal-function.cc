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

#include "dbcommon/function/decimal-function.h"

#include <algorithm>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>

#include "boost/multiprecision/cpp_dec_float.hpp"
#include "boost/multiprecision/cpp_int.hpp"

#include "dbcommon/common/vector.h"
#include "dbcommon/common/vector/fixed-length-vector.h"
#include "dbcommon/common/vector/variable-length-vector.h"
#include "dbcommon/function/arithmetic-function.h"
#include "dbcommon/function/invoker.h"
#include "dbcommon/function/mathematical-function.h"
#include "dbcommon/type/type-util.h"
#include "dbcommon/utils/macro.h"

namespace dbcommon {

typedef bool (*cmpWithZero)(Int128 x);
static inline bool LTZero(Int128 x) { return x < 0; }
static inline bool LEZero(Int128 x) { return x <= 0; }
static inline bool EQZero(Int128 x) { return x == 0; }
static inline bool NEZero(Int128 x) { return x != 0; }
static inline bool GEZero(Int128 x) { return x >= 0; }
static inline bool GTZero(Int128 x) { return x > 0; }

template <cmpWithZero cmpCondition>
static inline force_inline bool decimalCmp(Int128 val1, int64_t scale1,
                                           Int128 val2, int64_t scale2) {
  if (scale1 < scale2) {
    val1 =
        scaleUpInt128ByPowerOfTen(val1, static_cast<int32_t>(scale2 - scale1));
  }
  if (scale1 > scale2) {
    val2 =
        scaleUpInt128ByPowerOfTen(val2, static_cast<int32_t>(scale1 - scale2));
  }
  val1 -= val2;
  return cmpCondition(val1);
}

template <cmpWithZero cmpCondition>
Datum DECIMAL_VAL_VAL_CMP(Datum *params, uint64_t size) {
  assert(size == 3 && "invalid input");
  Scalar *ret = DatumGetValue<Scalar *>(params[0]);
  Scalar *left = DatumGetValue<Scalar *>(params[1]);
  Scalar *right = DatumGetValue<Scalar *>(params[2]);
  DecimalVar *value1 = DatumGetValue<DecimalVar *>(left->value);
  DecimalVar *value2 = DatumGetValue<DecimalVar *>(right->value);

  Int128 ival1 = Int128(value1->highbits, value1->lowbits);
  Int128 ival2 = Int128(value2->highbits, value2->lowbits);

  int r = decimalCmp<cmpCondition>(ival1, value1->scale, ival2, value2->scale);
  ret->value = CreateDatum(static_cast<bool>(r));
  return CreateDatum(ret);
}

template <cmpWithZero cmpCondition>
Datum DECIMAL_VAL_VEC_CMP(Datum *params, uint64_t size) {
  assert(size == 3 && "invalid input");
  SelectList *selected = DatumGetValue<SelectList *>(params[0]);
  Scalar *para = DatumGetValue<Scalar *>(params[1]);
  DecimalVar *val = DatumGetValue<DecimalVar *>(para->value);
  Int128 srcVal = Int128(val->highbits, val->lowbits);
  int64_t scale1 = val->scale;
  DecimalVector *vec = DatumGetValue<DecimalVector *>(params[2]);

  const int64_t *highbitVal = (const int64_t *)(vec->getAuxiliaryValue());
  const uint64_t *lowbitVal = (const uint64_t *)(vec->getValue());
  const int64_t *scaleVal = (const int64_t *)(vec->getScaleValue());
  bool hasNull = vec->hasNullValue();
  SelectList *sel = vec->getSelected();
  uint64_t sz = vec->getNumOfRows();

  uint64_t counter = 0;
  selected->setNulls(vec->getNumOfRowsPlain(), sel, selected->getNulls(),
                     vec->getNulls());
  SelectList::value_type *__restrict__ res = selected->begin();
  if (sel) {
    if (hasNull) {
      const bool *nulls = vec->getNullBuffer()->getBools();
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel)[i];
        if (!nulls[index]) {
          Int128 vecVal = Int128(highbitVal[index], lowbitVal[index]);
          if (decimalCmp<cmpCondition>(srcVal, scale1, vecVal,
                                       scaleVal[index])) {
            res[counter++] = index;
          }
        }
      }
    } else {
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel)[i];
        Int128 vecVal = Int128(highbitVal[index], lowbitVal[index]);
        if (decimalCmp<cmpCondition>(srcVal, scale1, vecVal, scaleVal[index])) {
          res[counter++] = index;
        }
      }
    }
  } else {
    if (hasNull) {
      const bool *nulls = vec->getNullBuffer()->getBools();
      for (uint64_t i = 0; i < sz; ++i) {
        if (!nulls[i]) {
          Int128 vecVal = Int128(highbitVal[i], lowbitVal[i]);
          if (decimalCmp<cmpCondition>(srcVal, scale1, vecVal, scaleVal[i])) {
            res[counter++] = i;
          }
        }
      }
    } else {
      for (uint64_t i = 0; i < sz; ++i) {
        Int128 vecVal = Int128(highbitVal[i], lowbitVal[i]);
        if (decimalCmp<cmpCondition>(srcVal, scale1, vecVal, scaleVal[i])) {
          res[counter++] = i;
        }
      }
    }
  }
  selected->resize(counter);
  return CreateDatum(selected);
}

template <cmpWithZero cmpCondition>
Datum DECIMAL_VEC_VAL_CMP(Datum *params, uint64_t size) {
  assert(size == 3 && "invalid input");
  SelectList *selected = DatumGetValue<SelectList *>(params[0]);
  Scalar *para = DatumGetValue<Scalar *>(params[2]);
  DecimalVar *val = DatumGetValue<DecimalVar *>(para->value);
  int64_t scale2 = val->scale;
  Int128 srcVal = Int128(val->highbits, val->lowbits);
  DecimalVector *vec = DatumGetValue<DecimalVector *>(params[1]);

  const int64_t *highbitVal = (const int64_t *)(vec->getAuxiliaryValue());
  const uint64_t *lowbitVal = (const uint64_t *)(vec->getValue());
  const int64_t *scaleVal = (const int64_t *)(vec->getScaleValue());
  bool hasNull = vec->hasNullValue();
  SelectList *sel = vec->getSelected();
  uint64_t sz = vec->getNumOfRows();

  uint64_t counter = 0;
  selected->setNulls(vec->getNumOfRowsPlain(), sel, selected->getNulls(),
                     vec->getNulls());
  SelectList::value_type *__restrict__ res = selected->begin();
  if (sel) {
    if (hasNull) {
      const bool *nulls = vec->getNullBuffer()->getBools();
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel)[i];
        if (!nulls[index]) {
          Int128 vecVal = Int128(highbitVal[index], lowbitVal[index]);
          if (decimalCmp<cmpCondition>(vecVal, scaleVal[index], srcVal,
                                       scale2)) {
            res[counter++] = index;
          }
        }
      }
    } else {
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel)[i];
        Int128 vecVal = Int128(highbitVal[index], lowbitVal[index]);
        if (decimalCmp<cmpCondition>(vecVal, scaleVal[index], srcVal, scale2)) {
          res[counter++] = index;
        }
      }
    }
  } else {
    if (hasNull) {
      const bool *nulls = vec->getNullBuffer()->getBools();
      for (uint64_t i = 0; i < sz; ++i) {
        if (!nulls[i]) {
          Int128 vecVal = Int128(highbitVal[i], lowbitVal[i]);
          if (decimalCmp<cmpCondition>(vecVal, scaleVal[i], srcVal, scale2)) {
            res[counter++] = i;
          }
        }
      }
    } else {
      for (uint64_t i = 0; i < sz; ++i) {
        Int128 vecVal = Int128(highbitVal[i], lowbitVal[i]);
        if (decimalCmp<cmpCondition>(vecVal, scaleVal[i], srcVal, scale2)) {
          res[counter++] = i;
        }
      }
    }
  }
  selected->resize(counter);
  return CreateDatum(selected);
}

template <cmpWithZero cmpCondition>
Datum DECIMAL_VEC_VEC_CMP(Datum *params, uint64_t size) {
  assert(size == 3 && "invalid input");
  SelectList *selected = DatumGetValue<SelectList *>(params[0]);

  DecimalVector *vec1 = DatumGetValue<DecimalVector *>(params[1]);
  const int64_t *highbitVal1 = (const int64_t *)(vec1->getAuxiliaryValue());
  const uint64_t *lowbitVal1 = (const uint64_t *)(vec1->getValue());
  const int64_t *scaleVal1 = (const int64_t *)(vec1->getScaleValue());
  bool hasNull1 = vec1->hasNullValue();
  SelectList *sel1 = vec1->getSelected();

  DecimalVector *vec2 = DatumGetValue<DecimalVector *>(params[2]);
  const int64_t *highbitVal2 = (const int64_t *)(vec2->getAuxiliaryValue());
  const uint64_t *lowbitVal2 = (const uint64_t *)(vec2->getValue());
  const int64_t *scaleVal2 = (const int64_t *)(vec2->getScaleValue());
  bool hasNull2 = vec2->hasNullValue();
  SelectList *sel2 = vec2->getSelected();

  assert(vec1->getNumOfRows() == vec2->getNumOfRows() && "invalid parameter");
  uint64_t sz = vec1->getNumOfRows();
  uint64_t counter = 0;
  selected->setNulls(vec1->getNumOfRowsPlain(), sel1, selected->getNulls(),
                     vec1->getNulls(), vec2->getNulls());
  SelectList::value_type *__restrict__ res = selected->begin();
  if (sel1) {
    assert(*sel1 == *sel2);
    if (!hasNull1 && !hasNull2) {
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel1)[i];
        Int128 vecVal1 = Int128(highbitVal1[index], lowbitVal1[index]);
        Int128 vecVal2 = Int128(highbitVal2[index], lowbitVal2[index]);
        if (decimalCmp<cmpCondition>(vecVal1, scaleVal1[index], vecVal2,
                                     scaleVal2[index])) {
          res[counter++] = index;
        }
      }
    } else {
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel1)[i];
        Int128 vecVal1 = Int128(highbitVal1[index], lowbitVal1[index]);
        Int128 vecVal2 = Int128(highbitVal2[index], lowbitVal2[index]);
        if (!vec1->isNullPlain(index) && !vec2->isNullPlain(index) &&
            decimalCmp<cmpCondition>(vecVal1, scaleVal1[index], vecVal2,
                                     scaleVal2[index])) {
          res[counter++] = index;
        }
      }
    }
  } else {
    if (!hasNull1 && !hasNull2) {
      for (uint64_t i = 0; i < sz; ++i) {
        Int128 vecVal1 = Int128(highbitVal1[i], lowbitVal1[i]);
        Int128 vecVal2 = Int128(highbitVal2[i], lowbitVal2[i]);
        if (decimalCmp<cmpCondition>(vecVal1, scaleVal1[i], vecVal2,
                                     scaleVal2[i])) {
          res[counter++] = i;
        }
      }
    } else {
      for (uint64_t i = 0; i < sz; ++i) {
        Int128 vecVal1 = Int128(highbitVal1[i], lowbitVal1[i]);
        Int128 vecVal2 = Int128(highbitVal2[i], lowbitVal2[i]);
        if (!vec1->isNullPlain(i) && !vec2->isNullPlain(i) &&
            decimalCmp<cmpCondition>(vecVal1, scaleVal1[i], vecVal2,
                                     scaleVal2[i])) {
          res[counter++] = i;
        }
      }
    }
  }

  selected->resize(counter);
  return CreateDatum(selected);
}

Datum decimal_val_less_than_decimal_val(Datum *params, uint64_t size) {
  return DECIMAL_VAL_VAL_CMP<LTZero>(params, size);
}
Datum decimal_val_less_than_decimal_vec(Datum *params, uint64_t size) {
  return DECIMAL_VAL_VEC_CMP<LTZero>(params, size);
}
Datum decimal_vec_less_than_decimal_val(Datum *params, uint64_t size) {
  return DECIMAL_VEC_VAL_CMP<LTZero>(params, size);
}
Datum decimal_vec_less_than_decimal_vec(Datum *params, uint64_t size) {
  return DECIMAL_VEC_VEC_CMP<LTZero>(params, size);
}

Datum decimal_val_less_eq_decimal_val(Datum *params, uint64_t size) {
  return DECIMAL_VAL_VAL_CMP<LEZero>(params, size);
}
Datum decimal_val_less_eq_decimal_vec(Datum *params, uint64_t size) {
  return DECIMAL_VAL_VEC_CMP<LEZero>(params, size);
}
Datum decimal_vec_less_eq_decimal_val(Datum *params, uint64_t size) {
  return DECIMAL_VEC_VAL_CMP<LEZero>(params, size);
}
Datum decimal_vec_less_eq_decimal_vec(Datum *params, uint64_t size) {
  return DECIMAL_VEC_VEC_CMP<LEZero>(params, size);
}

Datum decimal_val_equal_decimal_val(Datum *params, uint64_t size) {
  return DECIMAL_VAL_VAL_CMP<EQZero>(params, size);
}
Datum decimal_val_equal_decimal_vec(Datum *params, uint64_t size) {
  return DECIMAL_VAL_VEC_CMP<EQZero>(params, size);
}
Datum decimal_vec_equal_decimal_val(Datum *params, uint64_t size) {
  return DECIMAL_VEC_VAL_CMP<EQZero>(params, size);
}
Datum decimal_vec_equal_decimal_vec(Datum *params, uint64_t size) {
  return DECIMAL_VEC_VEC_CMP<EQZero>(params, size);
}

Datum decimal_val_not_equal_decimal_val(Datum *params, uint64_t size) {
  return DECIMAL_VAL_VAL_CMP<NEZero>(params, size);
}
Datum decimal_val_not_equal_decimal_vec(Datum *params, uint64_t size) {
  return DECIMAL_VAL_VEC_CMP<NEZero>(params, size);
}
Datum decimal_vec_not_equal_decimal_val(Datum *params, uint64_t size) {
  return DECIMAL_VEC_VAL_CMP<NEZero>(params, size);
}
Datum decimal_vec_not_equal_decimal_vec(Datum *params, uint64_t size) {
  return DECIMAL_VEC_VEC_CMP<NEZero>(params, size);
}

Datum decimal_val_greater_than_decimal_val(Datum *params, uint64_t size) {
  return DECIMAL_VAL_VAL_CMP<GTZero>(params, size);
}

Datum decimal_val_greater_than_decimal_vec(Datum *params, uint64_t size) {
  return DECIMAL_VAL_VEC_CMP<GTZero>(params, size);
}

Datum decimal_vec_greater_than_decimal_val(Datum *params, uint64_t size) {
  return DECIMAL_VEC_VAL_CMP<GTZero>(params, size);
}

Datum decimal_vec_greater_than_decimal_vec(Datum *params, uint64_t size) {
  return DECIMAL_VEC_VEC_CMP<GTZero>(params, size);
}

Datum decimal_val_greater_eq_decimal_val(Datum *params, uint64_t size) {
  return DECIMAL_VAL_VAL_CMP<GEZero>(params, size);
}

Datum decimal_val_greater_eq_decimal_vec(Datum *params, uint64_t size) {
  return DECIMAL_VAL_VEC_CMP<GEZero>(params, size);
}
Datum decimal_vec_greater_eq_decimal_val(Datum *params, uint64_t size) {
  return DECIMAL_VEC_VAL_CMP<GEZero>(params, size);
}

Datum decimal_vec_greater_eq_decimal_vec(Datum *params, uint64_t size) {
  return DECIMAL_VEC_VEC_CMP<GEZero>(params, size);
}

std::string decimal_round_internal(std::string srcStr, const int64_t rscale) {
  bool positive = true;
  if (srcStr.length() > 0 && srcStr[0] == '-') {
    positive = false;
    srcStr = srcStr.substr(1);
  }

  std::string rstr = srcStr, intDigits, decDigits;
  uint64_t cscale = 0;
  size_t pointIdx = srcStr.find(".");
  if (pointIdx == std::string::npos) {
    intDigits = srcStr;
    cscale = 0;
  } else {
    cscale = srcStr.length() - pointIdx - 1;
    intDigits = srcStr.substr(0, pointIdx);
    decDigits = srcStr.substr(pointIdx + 1);
  }

  if (rscale == 0) {
    boost::multiprecision::cpp_int intNum(intDigits);
    if (cscale > 0) {
      intNum = (decDigits[rscale] >= '5' ? intNum + 1 : intNum);
    }
    rstr = intNum.str();
  } else if (rscale < 0) {
    int carrierIndex = intDigits.length() + rscale;
    if (carrierIndex < 0) {
      rstr = "0";
    } else {
      char cdigit = intDigits[carrierIndex];
      Int128 intNum = Int128(intDigits);
      Int128 divisor = Int128(POWERS_OF_TEN[-rscale]);
      Int128 remainder;
      intNum.divide(divisor, remainder);
      intNum -= remainder;
      intNum = cdigit >= '5' ? intNum += divisor : intNum;
      rstr = intNum.toString();
    }
  } else {
    if (cscale > rscale) {
      char cdigit = decDigits[rscale];
      decDigits = decDigits.substr(0, rscale);
      if (cdigit >= '5') {
        int nzero = 0;
        while (decDigits[nzero] == '0') nzero++;
        std::string newDecDigits = decDigits.substr(nzero);
        boost::multiprecision::cpp_int decNum(newDecDigits);
        decNum += 1;
        newDecDigits = decNum.str();
        nzero = decDigits.length() - newDecDigits.length();
        std::string zeroStr(std::max(nzero, 0), '0');
        decDigits = zeroStr + newDecDigits;
        if (decDigits.length() > rscale) {
          boost::multiprecision::cpp_int intNum(intDigits);
          intNum += 1;
          intDigits = intNum.str();
          decDigits = decDigits.substr(1, rscale);
        }
      }
    } else {
      int zeros2add = rscale - cscale;
      while (zeros2add-- > 0) decDigits += "0";
    }
    rstr = intDigits + "." + decDigits;
  }

  if (!positive && !decimal_equals_zero(rstr)) rstr = "-" + rstr;
  return rstr;
}

Int128 get_decimal_sign(Int128 val) {
  Int128 result;
  if (val > 0) {
    result = 1;
  } else if (val < 0) {
    result = -1;
  } else {
    result = 0;
  }
  return result;
}

bool decimal_equals_zero(std::string str) {
  assert(str.length() != 0);
  for (char c : str) {
    if (c != '-' && c != '.' && c != '0') return false;
  }
  return true;
}

bool decimal_is_nan(std::string str) {
  std::string cp(str);
  std::transform(cp.begin(), cp.end(), cp.begin(), tolower);
  if (str == "nan") return true;
  return false;
}

template <typename Operator, bool isDivide = false>
Datum decimal_val_op_decimal_val(Datum *params, uint64_t size) {
  Scalar *retScalar = params[0];
  Scalar *lhsScalar = params[1];
  Scalar *rhsScalar = params[2];

  if (lhsScalar->isnull || rhsScalar->isnull) {
    retScalar->isnull = true;
  } else {
    retScalar->isnull = false;

    DecimalVar *retVal = retScalar->allocateValue<DecimalVar>();

    DecimalVar *lhsVal = DatumGetValue<DecimalVar *>(lhsScalar->value);
    DecimalVar *rhsVal = DatumGetValue<DecimalVar *>(rhsScalar->value);
    if (isDivide && Int128(rhsVal->highbits, rhsVal->lowbits) == Int128(0, 0))
      LOG_ERROR(ERRCODE_DIVISION_BY_ZERO, "division by zero");

    *retVal = Operator()(*lhsVal, *rhsVal);
  }

  return params[0];
}

template <typename Operator, bool isDivides = false>
Datum decimal_val_op_decimal_vec(Datum *params, uint64_t size) {
  // Retrieve Scalar and Vector from params
  DecimalVector *retVector = params[0];
  Scalar *lhsScalar = params[1];
  DecimalVector *rhsVector = params[2];

  DecimalVar lhsVal = *static_cast<DecimalVar *>(lhsScalar->value);

  if (isDivides && rhsVector->checkZeroValue())
    LOG_ERROR(ERRCODE_DIVISION_BY_ZERO, "division by zero");

  // Setup return Vector
  DecimalVectorRawData rhs(rhsVector);
  retVector->resize(rhs.plainSize, rhs.sel, rhs.nulls);
  DecimalVectorRawData ret(retVector);

  auto operation = [&](int plainIdx) {
    DecimalVar rhsVal(rhs.hightbits[plainIdx], rhs.lowbits[plainIdx],
                      rhs.scales[plainIdx]);
    DecimalVar retVal = Operator()(lhsVal, rhsVal);
    std::tie(ret.hightbits[plainIdx], ret.lowbits[plainIdx],
             ret.scales[plainIdx]) =
        std::make_tuple(retVal.highbits, retVal.lowbits, retVal.scale);
  };
  transformVector(ret.plainSize, ret.sel, ret.nulls, operation);

  return params[0];
}

template <typename Operator, bool isDivides = false>
Datum decimal_vec_op_decimal_val(Datum *params, uint64_t size) {
  // Retrieve Scalar and Vector from params
  DecimalVector *retVector = params[0];
  DecimalVector *lhsVector = params[1];
  Scalar *rhsScalar = params[2];

  DecimalVar rhsVal = *static_cast<DecimalVar *>(rhsScalar->value);

  if (isDivides && Int128(rhsVal.highbits, rhsVal.lowbits) == Int128(0, 0))
    LOG_ERROR(ERRCODE_DIVISION_BY_ZERO, "division by zero");

  // Setup return Vector
  DecimalVectorRawData lhs(lhsVector);
  retVector->resize(lhs.plainSize, lhs.sel, lhs.nulls);
  DecimalVectorRawData ret(retVector);

  auto operation = [&](int plainIdx) {
    DecimalVar lhsVal(lhs.hightbits[plainIdx], lhs.lowbits[plainIdx],
                      lhs.scales[plainIdx]);
    DecimalVar retVal = Operator()(lhsVal, rhsVal);
    std::tie(ret.hightbits[plainIdx], ret.lowbits[plainIdx],
             ret.scales[plainIdx]) =
        std::make_tuple(retVal.highbits, retVal.lowbits, retVal.scale);
  };
  transformVector(ret.plainSize, ret.sel, ret.nulls, operation);

  return params[0];
}

template <typename Operator, bool isDivides = false>
Datum decimal_vec_op_decimal_vec(Datum *params, uint64_t size) {
  // Retrieve Vector from params
  DecimalVector *retVector = params[0];
  DecimalVector *lhsVector = params[1];
  DecimalVector *rhsVector = params[2];

  if (isDivides && rhsVector->checkZeroValue())
    LOG_ERROR(ERRCODE_DIVISION_BY_ZERO, "division by zero");

  // Setup return Vector
  DecimalVectorRawData lhs(lhsVector), rhs(rhsVector);
  retVector->resize(lhs.plainSize, lhs.sel, lhs.nulls, rhs.nulls);
  DecimalVectorRawData ret(retVector);
  assert(lhs.sel == rhs.sel && lhs.plainSize == rhs.plainSize);

  auto operation = [&](int plainIdx) {
    DecimalVar lhsVal(lhs.hightbits[plainIdx], lhs.lowbits[plainIdx],
                      lhs.scales[plainIdx]);
    DecimalVar rhsVal(rhs.hightbits[plainIdx], rhs.lowbits[plainIdx],
                      rhs.scales[plainIdx]);
    DecimalVar retVal = Operator()(lhsVal, rhsVal);
    std::tie(ret.hightbits[plainIdx], ret.lowbits[plainIdx],
             ret.scales[plainIdx]) =
        std::make_tuple(retVal.highbits, retVal.lowbits, retVal.scale);
  };
  transformVector(ret.plainSize, ret.sel, ret.nulls, operation);

  return params[0];
}

template <typename Operator, bool isDivide = false>
Datum decimal_op_decimal(Datum *params, uint64_t size) {
  return type1_op_type2_bind<decimal_vec_op_decimal_vec<Operator, isDivide>,
                             decimal_vec_op_decimal_val<Operator, isDivide>,
                             decimal_val_op_decimal_vec<Operator, isDivide>,
                             decimal_val_op_decimal_val<Operator, isDivide>>(
      params, size);
}

Datum decimal_add_decimal(Datum *params, uint64_t size) {
  return decimal_op_decimal<std::plus<DecimalVar>>(params, size);
}

Datum decimal_sub_decimal(Datum *params, uint64_t size) {
  return decimal_op_decimal<std::minus<DecimalVar>>(params, size);
}

Datum decimal_mul_decimal(Datum *params, uint64_t size) {
  return decimal_op_decimal<std::multiplies<DecimalVar>>(params, size);
}

Datum decimal_div_decimal(Datum *params, uint64_t size) {
  return decimal_op_decimal<std::divides<DecimalVar>>(params, size);
}

Datum decimal_to_decimal(Datum *params, uint64_t size) {
  assert(size == 3 && "invalid input");
  assert(dynamic_cast<Scalar *>(DatumGetValue<Object *>(params[2])));

  Object *para = DatumGetValue<Object *>(params[1]);
  int64_t typeMod = DatumGetValue<Scalar *>(params[2])->value;
  int64_t scale = TypeModifierUtil::getScale(typeMod);

  if (dynamic_cast<Vector *>(para)) {  // Vector input
    assert(dynamic_cast<DecimalVector *>(DatumGetValue<Object *>(params[0])));
    DecimalVector *retVector = params[0];
    DecimalVector *srcVector = params[1];

    DecimalVectorRawData src(srcVector);
    retVector->resize(src.plainSize, src.sel, src.nulls);
    DecimalVectorRawData ret(retVector);

    auto cast = [&](size_t plainIdx) {
      DecimalVar val(src.hightbits[plainIdx], src.lowbits[plainIdx],
                     src.scales[plainIdx]);
      val = val.cast(scale);
      std::tie(ret.hightbits[plainIdx], ret.lowbits[plainIdx],
               ret.scales[plainIdx]) =
          std::make_tuple(val.highbits, val.lowbits, val.scale);
    };
    transformVector(ret.plainSize, ret.sel, ret.nulls, cast);

  } else {  // Scalar input
    Scalar *retScalar = params[0];
    Scalar *srcScalar = params[1];
    if (srcScalar->isnull) {
      retScalar->isnull = true;
    } else {
      retScalar->isnull = false;
      DecimalVar *retVal = retScalar->allocateValue<DecimalVar>();
      *retVal = DatumGetValue<DecimalVar *>(srcScalar->value)->cast(scale);
    }
  }
  return params[0];
}

template <typename Integer>
Datum integer_to_decimal(Datum *params, uint64_t size) {
  assert(size == 2 && "invalid input");

  Object *para = DatumGetValue<Object *>(params[1]);

  if (dynamic_cast<Vector *>(para)) {  // Case1. Vector input
    assert(dynamic_cast<DecimalVector *>(DatumGetValue<Object *>(params[0])));
    DecimalVector *retVector = params[0];
    DecimalVector *srcVector = params[1];

    FixedSizeTypeVectorRawData<Integer> src(srcVector);
    retVector->resize(src.plainSize, src.sel, src.nulls);
    DecimalVectorRawData ret(retVector);

    // fill high bits
    auto fillHighbit = [&](size_t plainIdx) {
      ret.hightbits[plainIdx] = (src.values[plainIdx] >= 0) ? 0 : uint64_t(-1);
    };
    transformVector(ret.plainSize, ret.sel, ret.nulls, fillHighbit);

    // fill low bits
    auto fillLowbit = [&](size_t plainIdx) {
      ret.lowbits[plainIdx] = static_cast<int64_t>(src.values[plainIdx]);
    };
    transformVector(ret.plainSize, ret.sel, ret.nulls, fillLowbit);

    // fill scale
    memset(ret.scales, 0, ret.plainSize * sizeof(ret.scales[0]));

  } else {  // Case2. Scalar input
    Scalar *retScalar = params[0];
    Scalar *srcScalar = params[1];
    if (srcScalar->isnull) {
      retScalar->isnull = true;
    } else {
      retScalar->isnull = false;
      DecimalVar *retVal = retScalar->allocateValue<DecimalVar>();
      *retVal = DecimalVar(
          static_cast<int64_t>(DatumGetValue<Integer>(srcScalar->value)));
    }
  }
  return params[0];
}

Datum int8_to_decimal(Datum *params, uint64_t size) {
  return integer_to_decimal<int8_t>(params, size);
}
Datum int16_to_decimal(Datum *params, uint64_t size) {
  return integer_to_decimal<int16_t>(params, size);
}
Datum int32_to_decimal(Datum *params, uint64_t size) {
  return integer_to_decimal<int32_t>(params, size);
}
Datum int64_to_decimal(Datum *params, uint64_t size) {
  return integer_to_decimal<int64_t>(params, size);
}

template <typename Integer>
Datum decimal_to_integer(Datum *params, uint64_t size) {
  assert(size == 2 && "invalid input");

  uint64_t upperBound = static_cast<uint64_t>(
      static_cast<typename std::make_unsigned<Integer>::type>(
          std::numeric_limits<Integer>::max()));
  uint64_t lowerBound = static_cast<uint64_t>(
      static_cast<int64_t>(std::numeric_limits<Integer>::min()));

  Object *para = DatumGetValue<Object *>(params[1]);

  if (dynamic_cast<Vector *>(para)) {  // Case1. Vector input
    Vector *retVector = params[0];
    DecimalVector *srcVector = params[1];

    DecimalVectorRawData src(srcVector);
    retVector->resize(src.plainSize, src.sel, src.nulls);
    FixedSizeTypeVectorRawData<Integer> ret(retVector);

    // cast to int128
    std::unique_ptr<Int128[]> scaleDownInt128s(new Int128[src.plainSize]);
    auto castToInt128 = [&](size_t plainIdx) {
      scaleDownInt128s[plainIdx] = scaleDownInt128ByPowerOfTen(
          Int128(src.hightbits[plainIdx], src.lowbits[plainIdx]),
          src.scales[plainIdx]);
    };
    transformVector(ret.plainSize, ret.sel, ret.nulls, castToInt128);

    // check overflow
    bool castOk = true;
    auto checkIntegerOverflow = [&](size_t plainIdx) {
      uint64_t highbits = scaleDownInt128s[plainIdx].getHighBits();
      uint64_t lowbits = scaleDownInt128s[plainIdx].getLowBits();
      castOk &= ((highbits == -1) & (lowbits >= lowerBound)) |
                ((highbits == 0) & (lowbits <= upperBound));
    };
    transformVector(ret.plainSize, ret.sel, ret.nulls, checkIntegerOverflow);
    if (!castOk)
      LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, "%s out of range",
                TypeUtil::instance()
                    ->getTypeEntryById(retVector->getTypeKind())
                    ->name.c_str());

    // cast to Integer
    auto castToInteger = [&](size_t plainIdx) {
      ret.values[plainIdx] = static_cast<Integer>(
          static_cast<int64_t>(scaleDownInt128s[plainIdx].getLowBits()));
    };
    transformVector(ret.plainSize, ret.sel, ret.nulls, castToInteger);

  } else {  // Case2. Scalar input
    Scalar *retScalar = params[0];
    Scalar *srcScalar = params[1];
    if (srcScalar->isnull) {
      retScalar->isnull = true;
    } else {
      retScalar->isnull = false;
      DecimalVar x = *DatumGetValue<DecimalVar *>(srcScalar->value);
      Int128 scaleDownInt128 =
          scaleDownInt128ByPowerOfTen(Int128(x.highbits, x.lowbits), x.scale);

      uint64_t highbits = scaleDownInt128.getHighBits();
      uint64_t lowbits = scaleDownInt128.getLowBits();
      if ((highbits == -1 && lowbits >= lowerBound) ||
          (highbits == 0 && lowbits <= upperBound)) {
        retScalar->value = CreateDatum(static_cast<Integer>(lowbits));
      } else {
        LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, "%s out of range",
                  TypeUtil::instance()
                      ->getTypeEntryById(TypeMapping<Integer>::type)
                      ->name.c_str());
      }
    }
  }
  return params[0];
}

Datum decimal_to_int8(Datum *params, uint64_t size) {
  return decimal_to_integer<int8_t>(params, size);
}
Datum decimal_to_int16(Datum *params, uint64_t size) {
  return decimal_to_integer<int16_t>(params, size);
}
Datum decimal_to_int32(Datum *params, uint64_t size) {
  return decimal_to_integer<int32_t>(params, size);
}
Datum decimal_to_int64(Datum *params, uint64_t size) {
  return decimal_to_integer<int64_t>(params, size);
}

template <typename Operator, typename rhsType>
Datum decimal_val_op_integer_val(Datum *params, uint64_t size) {
  Scalar *retScalar = params[0];
  Scalar *lhsScalar = params[1];
  Scalar *rhsScalar = params[2];

  if (lhsScalar->isnull || rhsScalar->isnull) {
    retScalar->isnull = true;
  } else {
    retScalar->isnull = false;
    DecimalVar *retVal = retScalar->allocateValue<DecimalVar>();

    DecimalVar *lhsVal = DatumGetValue<DecimalVar *>(lhsScalar->value);
    rhsType rhsVal = DatumGetValue<rhsType>(rhsScalar->value);

    *retVal = Operator()(*lhsVal, rhsVal);
  }

  return params[0];
}

template <typename Operator, typename rhsType, bool isDivides = false>
Datum decimal_val_op_integer_vec(Datum *params, uint64_t size) {
  DecimalVector *retVector = params[0];
  Scalar *lhsScalar = params[1];
  Vector *rhsVector = params[2];

  DecimalVar lhsVal = *static_cast<DecimalVar *>(lhsScalar->value);

  if (isDivides) LOG_ERROR(ERRCODE_DIVISION_BY_ZERO, "division by zero");

  FixedSizeTypeVectorRawData<rhsType> rhs(rhsVector);
  retVector->resize(rhs.plainSize, rhs.sel, rhs.nulls);
  DecimalVectorRawData ret(retVector);

  auto operation = [&](int plainIdx) {
    rhsType rhsVal = rhs.values[plainIdx];
    DecimalVar retVal = Operator()(lhsVal, rhsVal);
    std::tie(ret.hightbits[plainIdx], ret.lowbits[plainIdx],
             ret.scales[plainIdx]) =
        std::make_tuple(retVal.highbits, retVal.lowbits, retVal.scale);
  };
  transformVector(ret.plainSize, ret.sel, ret.nulls, operation);

  return params[0];
}

template <typename Operator, typename rhsType, bool isDivides = false>
Datum decimal_vec_op_integer_val(Datum *params, uint64_t size) {
  DecimalVector *retVector = params[0];
  DecimalVector *lhsVector = params[1];
  Scalar *rhsScalar = params[2];

  rhsType rhsVal = DatumGetValue<rhsType>(rhsScalar->value);

  if (isDivides) LOG_ERROR(ERRCODE_DIVISION_BY_ZERO, "division by zero");

  DecimalVectorRawData lhs(lhsVector);
  retVector->resize(lhs.plainSize, lhs.sel, lhs.nulls);
  DecimalVectorRawData ret(retVector);

  auto operation = [&](int plainIdx) {
    DecimalVar lhsVal(lhs.hightbits[plainIdx], lhs.lowbits[plainIdx],
                      lhs.scales[plainIdx]);
    DecimalVar retVal = Operator()(lhsVal, rhsVal);
    std::tie(ret.hightbits[plainIdx], ret.lowbits[plainIdx],
             ret.scales[plainIdx]) =
        std::make_tuple(retVal.highbits, retVal.lowbits, retVal.scale);
  };
  transformVector(ret.plainSize, ret.sel, ret.nulls, operation);

  return params[0];
}

template <typename Operator, typename rhsType, bool isDivides = false>
Datum decimal_vec_op_integer_vec(Datum *params, uint64_t size) {
  DecimalVector *retVector = params[0];
  DecimalVector *lhsVector = params[1];
  Vector *rhsVector = params[2];

  if (isDivides) LOG_ERROR(ERRCODE_DIVISION_BY_ZERO, "division by zero");

  DecimalVectorRawData lhs(lhsVector);
  FixedSizeTypeVectorRawData<rhsType> rhs(rhsVector);
  retVector->resize(lhs.plainSize, lhs.sel, lhs.nulls, rhs.nulls);
  DecimalVectorRawData ret(retVector);

  assert(lhs.sel == rhs.sel && lhs.plainSize == rhs.plainSize);

  auto operation = [&](int plainIdx) {
    DecimalVar lhsVal(lhs.hightbits[plainIdx], lhs.lowbits[plainIdx],
                      lhs.scales[plainIdx]);
    rhsType rhsVal = rhs.values[plainIdx];
    DecimalVar retVal = Operator()(lhsVal, rhsVal);
    std::tie(ret.hightbits[plainIdx], ret.lowbits[plainIdx],
             ret.scales[plainIdx]) =
        std::make_tuple(retVal.highbits, retVal.lowbits, retVal.scale);
  };
  transformVector(ret.plainSize, ret.sel, ret.nulls, operation);

  return params[0];
}

template <typename Operator, typename rhsType>
Datum decimal_op_integer(Datum *params, uint64_t size) {
  return type1_op_type2_bind<decimal_vec_op_integer_vec<Operator, rhsType>,
                             decimal_vec_op_integer_val<Operator, rhsType>,
                             decimal_val_op_integer_vec<Operator, rhsType>,
                             decimal_val_op_integer_val<Operator, rhsType>>(
      params, size);
}

Datum decimal_abs(Datum *params, uint64_t size) {
  return op_decimal<abs<DecimalVar>>(params, size);
}

Datum decimal_sign(Datum *params, uint64_t size) {
  return op_decimal<sign<DecimalVar>>(params, size);
}

Datum decimal_ceil(Datum *params, uint64_t size) {
  return op_decimal<ceil<DecimalVar>>(params, size);
}

Datum decimal_floor(Datum *params, uint64_t size) {
  return op_decimal<floor<DecimalVar>>(params, size);
}

Datum decimal_round(Datum *params, uint64_t size) {
  return decimal_op_integer<round<DecimalVar>, int32_t>(params, size);
}

Datum decimal_round_without_scale(Datum *params, uint64_t size) {
  return op_decimal<round<DecimalVar>>(params, size);
}

Datum decimal_trunc(Datum *params, uint64_t size) {
  return decimal_op_integer<trunc<DecimalVar>, int32_t>(params, size);
}

Datum decimal_trunc_without_scale(Datum *params, uint64_t size) {
  return op_decimal<trunc<DecimalVar>>(params, size);
}

Datum decimal_mod(Datum *params, uint64_t size) {
  return decimal_op_decimal<mod<DecimalVar, DecimalVar, DecimalVar>, true>(
      params, size);
}
}  // namespace dbcommon
