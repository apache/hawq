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

#ifndef DBCOMMON_SRC_DBCOMMON_FUNCTION_ARITHMETIC_FUNCTION_H_
#define DBCOMMON_SRC_DBCOMMON_FUNCTION_ARITHMETIC_FUNCTION_H_

#include <math.h>

#include <cfloat>
#include <cmath>
#include <functional>
#include <limits>

#include "dbcommon/common/vector-transformer.h"
#include "dbcommon/function/func.h"
#include "dbcommon/type/type-util.h"

namespace dbcommon {

template <typename Typelhs, typename Typerhs, typename Typeret>
inline int32_t addOk(Typelhs x, Typerhs y) {
  Typeret sum = static_cast<Typeret>(x) + static_cast<Typeret>(y);
  return !(x < 0 && y < 0 && sum >= 0) && !(x >= 0 && y >= 0 && sum < 0);
}

template <typename Typelhs, typename Typerhs, typename Typeret>
inline int32_t subOk(Typelhs x, Typerhs y) {
  if (static_cast<Typeret>(y) == std::numeric_limits<Typeret>::min()) {
    return x >= 0 ? 0 : 1;
  }
  return addOk<Typelhs, Typerhs, Typeret>(x, -y);
}

template <typename Typelhs, typename Typerhs, typename Typeret>
inline int32_t mulOk(Typelhs x, Typerhs y) {
  if (x == 0 || y == 0) {
    return 1;
  }
  volatile Typeret mul = static_cast<Typeret>(x) * static_cast<Typeret>(y);
  return mul / static_cast<Typeret>(x) == static_cast<Typeret>(y);
}

template <typename Typelhs, typename Typerhs, typename Typeret,
          typename Operator>
struct CheckOk {
  int32_t operator()(Typelhs lhsVal, Typerhs rhsVal) const { return 1; }
};

template <typename Typelhs, typename Typerhs, typename Typeret>
struct CheckOk<Typelhs, Typerhs, Typeret, std::plus<Typeret>> {
  int32_t operator()(Typelhs lhsVal, Typerhs rhsVal) const {
    if (std::is_same<float, Typeret>::value ||
        std::is_same<double, Typeret>::value) {
      Typeret res = std::plus<Typeret>()(lhsVal, rhsVal);
      if (isinf(res)) {
        LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                  "value out of range: overflow");
      } else {
        return 1;
      }
    } else {
      return addOk<Typelhs, Typerhs, Typeret>(lhsVal, rhsVal);
    }
  }
};

template <typename Typelhs, typename Typerhs, typename Typeret>
struct CheckOk<Typelhs, Typerhs, Typeret, std::minus<Typeret>> {
  int32_t operator()(Typelhs lhsVal, Typerhs rhsVal) const {
    if (std::is_same<float, Typeret>::value ||
        std::is_same<double, Typeret>::value) {
      Typeret res = std::minus<Typeret>()(lhsVal, rhsVal);
      if (isinf(res)) {
        LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                  "value out of range: overflow");
      } else {
        return 1;
      }
    } else {
      return subOk<Typelhs, Typerhs, Typeret>(lhsVal, rhsVal);
    }
  }
};

template <typename Typelhs, typename Typerhs, typename Typeret>
struct CheckOk<Typelhs, Typerhs, Typeret, std::multiplies<Typeret>> {
  int32_t operator()(Typelhs lhsVal, Typerhs rhsVal) const {
    if (std::is_same<float, Typeret>::value ||
        std::is_same<double, Typeret>::value) {
      Typeret res = std::multiplies<Typeret>()(lhsVal, rhsVal);
      if (isinf(res)) {
        LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                  "value out of range: overflow");
      } else {
        return 1;
      }
    } else {
      return mulOk<Typelhs, Typerhs, Typeret>(lhsVal, rhsVal);
    }
  }
};

template <typename Typeret, int32_t code>
struct SendErroLog {
  void operator()() const { return; }
};

template <typename Typeret>
struct SendErroLog<Typeret, ERRCODE_DIVISION_BY_ZERO> {
  void operator()() const {
    LOG_ERROR(ERRCODE_DIVISION_BY_ZERO, "division by zero");
  }
};

template <typename Typeret>
struct SendErroLog<Typeret, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE> {
  void operator()() const {
    if (std::is_same<float, Typeret>::value ||
        std::is_same<double, Typeret>::value) {
      LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                "value out of range: overflow");
    } else {
      LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, "%s out of range",
                pgTypeName<Typeret>());
    }
  }
};

template <typename Typelhs, typename Operator, typename Typerhs,
          typename Typeret, bool isDivides = false, bool isNeedOverFlow = false>
Datum type1_val_op_type2_val(Datum *params, uint64_t size) {
  Scalar *retScalar = params[0];
  Scalar *lhsScalar = params[1];
  Scalar *rhsScalar = params[2];

  if (lhsScalar->isnull || rhsScalar->isnull) {
    retScalar->isnull = true;
  } else {
    Typelhs lhsVal = DatumGetValue<Typelhs>(lhsScalar->value);
    Typerhs rhsVal = DatumGetValue<Typerhs>(rhsScalar->value);

    if (isDivides && rhsVal == 0)
      SendErroLog<Typeret, ERRCODE_DIVISION_BY_ZERO>()();
    if (isNeedOverFlow) {
      if (!CheckOk<Typelhs, Typerhs, Typeret, Operator>()(lhsVal, rhsVal))
        SendErroLog<Typeret, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE>()();
    }

    Typeret retVal = Operator()(lhsVal, rhsVal);
    retScalar->isnull = false;
    retScalar->value = CreateDatum(retVal);
  }

  return params[0];
}

template <typename Typelhs, typename Operator, typename Typerhs,
          typename Typeret, bool isDivides = false, bool isNeedOverFlow = false>
Datum type1_val_op_type2_vec(Datum *params, uint64_t size) {
  // Retrieve Scalar and Vector from params
  Vector *retVector = params[0];
  Scalar *lhsScalar = params[1];
  Vector *rhsVector = params[2];

  Typelhs lhsVal = DatumGetValue<Typelhs>(lhsScalar->value);

  if (isDivides && rhsVector->checkZeroValue())
    SendErroLog<Typeret, ERRCODE_DIVISION_BY_ZERO>()();

  // Setup return Vector
  FixedSizeTypeVectorRawData<Typerhs> rhs(rhsVector);
  retVector->resize(rhs.plainSize, rhs.sel, rhs.nulls);
  FixedSizeTypeVectorRawData<Typeret> ret(retVector);

  auto operation = [&](uint64_t plainIdx) {
    Typerhs rhsVal = rhs.values[plainIdx];
    if (isNeedOverFlow) {
      if (!CheckOk<Typelhs, Typerhs, Typeret, Operator>()(lhsVal, rhsVal))
        SendErroLog<Typeret, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE>()();
    }
    ret.values[plainIdx] = Operator()(lhsVal, rhsVal);
  };
  transformVector(ret.plainSize, ret.sel, ret.nulls, operation);

  return params[0];
}

template <typename Typelhs, typename Operator, typename Typerhs,
          typename Typeret, bool isDivides = false, bool isNeedOverFlow = false>
Datum type1_vec_op_type2_val(Datum *params, uint64_t size) {
  // Retrieve Scalar and Vector from params
  Vector *retVector = params[0];
  Vector *lhsVector = params[1];
  Scalar *rhsScalar = params[2];

  Typerhs rhsVal = DatumGetValue<Typerhs>(rhsScalar->value);

  if (isDivides && rhsVal == 0)
    SendErroLog<Typeret, ERRCODE_DIVISION_BY_ZERO>()();

  // Setup return Vector
  FixedSizeTypeVectorRawData<Typelhs> lhs(lhsVector);
  retVector->resize(lhs.plainSize, lhs.sel, lhs.nulls);
  FixedSizeTypeVectorRawData<Typeret> ret(retVector);

  auto operation = [&](uint64_t plainIdx) {
    Typelhs lhsVal = lhs.values[plainIdx];
    if (isNeedOverFlow) {
      if (!CheckOk<Typelhs, Typerhs, Typeret, Operator>()(lhsVal, rhsVal))
        SendErroLog<Typeret, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE>()();
    }
    ret.values[plainIdx] = Operator()(lhsVal, rhsVal);
  };
  transformVector(ret.plainSize, ret.sel, ret.nulls, operation);

  return params[0];
}

template <typename Typelhs, typename Operator, typename Typerhs,
          typename Typeret, bool isDivides = false, bool isNeedOverFlow = false>
Datum type1_vec_op_type2_vec(Datum *params, uint64_t size) {
  // Retrieve Vector from params
  Vector *retVector = params[0];
  Vector *lhsVector = params[1];
  Vector *rhsVector = params[2];

  if (isDivides && rhsVector->checkZeroValue())
    SendErroLog<Typeret, ERRCODE_DIVISION_BY_ZERO>()();

  // Setup return Vector
  FixedSizeTypeVectorRawData<Typelhs> lhs(lhsVector);
  FixedSizeTypeVectorRawData<Typerhs> rhs(rhsVector);
  retVector->resize(lhs.plainSize, lhs.sel, lhs.nulls, rhs.nulls);
  FixedSizeTypeVectorRawData<Typeret> ret(retVector);
  assert(lhs.sel == rhs.sel && lhs.plainSize == rhs.plainSize);

  auto operation = [&](uint64_t plainIdx) {
    Typelhs lhsVal = lhs.values[plainIdx];
    Typerhs rhsVal = rhs.values[plainIdx];
    if (isNeedOverFlow) {
      if (!CheckOk<Typelhs, Typerhs, Typeret, Operator>()(lhsVal, rhsVal))
        SendErroLog<Typeret, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE>()();
    }
    ret.values[plainIdx] = Operator()(lhsVal, rhsVal);
  };
  transformVector(ret.plainSize, ret.sel, ret.nulls, operation);

  return params[0];
}

template <dbcommon::Function vec_op_vec, dbcommon::Function vec_op_val,
          dbcommon::Function val_op_vec, dbcommon::Function val_op_val>
Datum type1_op_type2_bind(Datum *params, uint64_t size) {
  assert(size == 3 && "invalid input");

  Object *paraL = DatumGetValue<Object *>(params[1]);
  Vector *vecL = dynamic_cast<Vector *>(paraL);
  Object *paraR = DatumGetValue<Object *>(params[2]);
  Vector *vecR = dynamic_cast<Vector *>(paraR);

  if (vecL) {
    if (vecR) {
      return vec_op_vec(params, size);
    } else {
      return vec_op_val(params, size);
    }
  } else {
    if (vecR) {
      return val_op_vec(params, size);
    } else {
      return val_op_val(params, size);
    }
  }
}

template <dbcommon::Function vec_vec_vec_op, dbcommon::Function vec_vec_val_op,
          dbcommon::Function vec_val_vec_op, dbcommon::Function vec_val_val_op,
          dbcommon::Function val_vec_vec_op, dbcommon::Function val_vec_val_op,
          dbcommon::Function val_val_vec_op, dbcommon::Function val_val_val_op>
Datum type1_type2_type3_bind(Datum *params, uint64_t size) {
  assert(size == 4 && "invalid input");

  Object *para1 = DatumGetValue<Object *>(params[1]);
  Vector *vec1 = dynamic_cast<Vector *>(para1);
  Object *para2 = DatumGetValue<Object *>(params[2]);
  Vector *vec2 = dynamic_cast<Vector *>(para2);
  Object *para3 = DatumGetValue<Object *>(params[3]);
  Vector *vec3 = dynamic_cast<Vector *>(para3);

  if (vec1) {
    if (vec2) {
      if (vec3) {
        return vec_vec_vec_op(params, size);
      } else {
        return vec_vec_val_op(params, size);
      }
    } else {
      if (vec3) {
        return vec_val_vec_op(params, size);
      } else {
        return vec_val_val_op(params, size);
      }
    }
  } else {
    if (vec2) {
      if (vec3) {
        return val_vec_vec_op(params, size);
      } else {
        return val_vec_val_op(params, size);
      }
    } else {
      if (vec3) {
        return val_val_vec_op(params, size);
      } else {
        return val_val_val_op(params, size);
      }
    }
  }
}

template <typename Typelhs, typename Operator, typename Typerhs,
          typename Typeret, bool isDivides, bool isNeedOverFlow = false>
Datum type1_op_type2(Datum *params, uint64_t size) {
  return type1_op_type2_bind<
      type1_vec_op_type2_vec<Typelhs, Operator, Typerhs, Typeret, isDivides,
                             isNeedOverFlow>,
      type1_vec_op_type2_val<Typelhs, Operator, Typerhs, Typeret, isDivides,
                             isNeedOverFlow>,
      type1_val_op_type2_vec<Typelhs, Operator, Typerhs, Typeret, isDivides,
                             isNeedOverFlow>,
      type1_val_op_type2_val<Typelhs, Operator, Typerhs, Typeret, isDivides,
                             isNeedOverFlow>>(params, size);
}

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_FUNCTION_ARITHMETIC_FUNCTION_H_
