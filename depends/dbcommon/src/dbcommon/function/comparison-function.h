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

#ifndef DBCOMMON_SRC_DBCOMMON_FUNCTION_COMPARISON_FUNCTION_H_
#define DBCOMMON_SRC_DBCOMMON_FUNCTION_COMPARISON_FUNCTION_H_

#include "dbcommon/common/vector-transformer.h"
#include "dbcommon/function/func.h"

namespace dbcommon {

template <typename Typelhs, typename Operator, typename Typerhs>
Datum type1_val_cmp_type2_val(Datum *params, uint64_t size) {
  Scalar *retScalar = params[0];
  Scalar *lhsScalar = params[1];
  Scalar *rhsScalar = params[2];

  Typelhs lhsVal = DatumGetValue<Typelhs>(lhsScalar->value);
  Typerhs rhsVal = DatumGetValue<Typerhs>(rhsScalar->value);

  bool retVal = Operator()(lhsVal, rhsVal);
  retScalar->value = CreateDatum(retVal);

  return params[0];
}

template <typename Typelhs, typename Operator, typename Typerhs>
Datum type1_val_cmp_type2_vec(Datum *params, uint64_t size) {
  // Retrieve Scalar and Vector from params
  SelectList *retSelectList = params[0];
  Scalar *lhsScalar = params[1];
  Vector *rhsVector = params[2];

  Typelhs lhsVal = DatumGetValue<Typelhs>(lhsScalar->value);

  // Setup return Vector
  FixedSizeTypeVectorRawData<Typerhs> rhs(rhsVector);

  retSelectList->setNulls(rhs.plainSize, rhs.sel, retSelectList->getNulls(),
                          rhs.nulls);
  SelectList::size_type counter = 0;
  SelectList::value_type *__restrict__ ret = retSelectList->begin();
  auto operation = [&](uint64_t plainIdx) {
    Typerhs rhsVal = rhs.values[plainIdx];
    if (Operator()(lhsVal, rhsVal)) ret[counter++] = plainIdx;
  };
  transformVector(rhs.plainSize, rhs.sel, rhs.nulls, operation);
  retSelectList->resize(counter);

  return params[0];
}

template <typename Typelhs, typename Operator, typename Typerhs>
Datum type1_vec_cmp_type2_val(Datum *params, uint64_t size) {
  // Retrieve Scalar and Vector from params
  SelectList *retSelectList = params[0];
  Vector *lhsVector = params[1];
  Scalar *rhsScalar = params[2];

  Typerhs rhsVal = DatumGetValue<Typerhs>(rhsScalar->value);

  // Setup return Vector
  FixedSizeTypeVectorRawData<Typelhs> lhs(lhsVector);

  retSelectList->setNulls(lhs.plainSize, lhs.sel, retSelectList->getNulls(),
                          lhs.nulls);
  SelectList::size_type counter = 0;
  SelectList::value_type *__restrict__ ret = retSelectList->begin();
  auto operation = [&](uint64_t plainIdx) {
    Typelhs lhsVal = lhs.values[plainIdx];
    if (Operator()(lhsVal, rhsVal)) ret[counter++] = plainIdx;
  };
  transformVector(lhs.plainSize, lhs.sel, lhs.nulls, operation);
  retSelectList->resize(counter);

  return params[0];
}

template <typename Typelhs, typename Operator, typename Typerhs>
Datum type1_vec_cmp_type2_vec(Datum *params, uint64_t size) {
  // Retrieve Scalar and Vector from params
  SelectList *retSelectList = params[0];
  Vector *lhsVector = params[1];
  Vector *rhsVector = params[2];

  // Setup return Vector
  FixedSizeTypeVectorRawData<Typelhs> lhs(lhsVector);
  FixedSizeTypeVectorRawData<Typerhs> rhs(rhsVector);
  assert(lhs.sel == rhs.sel && lhs.plainSize == rhs.plainSize);

  retSelectList->setNulls(lhs.plainSize, lhs.sel, retSelectList->getNulls(),
                          lhs.nulls, rhs.nulls);
  SelectList::size_type counter = 0;
  SelectList::value_type *__restrict__ ret = retSelectList->begin();
  auto operation = [&](uint64_t plainIdx) {
    Typelhs lhsVal = lhs.values[plainIdx];
    Typerhs rhsVal = rhs.values[plainIdx];
    if (Operator()(lhsVal, rhsVal)) ret[counter++] = plainIdx;
  };
  transformVector(lhs.plainSize, lhs.sel, lhs.nulls, rhs.nulls, operation);
  retSelectList->resize(counter);

  return params[0];
}

template <dbcommon::Function vec_op_vec, dbcommon::Function vec_op_val,
          dbcommon::Function val_op_vec, dbcommon::Function val_op_val>
Datum type1_cmp_type2_bind(Datum *params, uint64_t size) {
  assert(size == 3 && "invalid input");

  Object *paraL = DatumGetValue<Object *>(params[1]);
  Vector *vecL = dynamic_cast<Vector *>(paraL);
  Object *paraR = DatumGetValue<Object *>(params[2]);
  Vector *vecR = dynamic_cast<Vector *>(paraR);

  if (vecL) {
    if (vecR) {
      return vec_op_vec(params, size);
    } else {
      auto scalarR = DatumGetValue<Scalar *>(params[2]);
      if (scalarR->isnull) {
        auto *selected = DatumGetValue<SelectList *>(params[0]);
        selected->resize(0);
        return CreateDatum(selected);
      }
      return vec_op_val(params, size);
    }
  } else {
    if (vecR) {
      auto scalarL = DatumGetValue<Scalar *>(params[1]);
      if (scalarL->isnull) {
        auto *selected = DatumGetValue<SelectList *>(params[0]);
        selected->resize(0);
        return CreateDatum(selected);
      }
      return val_op_vec(params, size);
    } else {
      return val_op_val(params, size);
    }
  }
}

template <typename Typelhs, typename Operator, typename Typerhs>
Datum type1_cmp_type2(Datum *params, uint64_t size) {
  return type1_cmp_type2_bind<
      type1_vec_cmp_type2_vec<Typelhs, Operator, Typerhs>,
      type1_vec_cmp_type2_val<Typelhs, Operator, Typerhs>,
      type1_val_cmp_type2_vec<Typelhs, Operator, Typerhs>,
      type1_val_cmp_type2_val<Typelhs, Operator, Typerhs> >(params, size);
}

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_FUNCTION_COMPARISON_FUNCTION_H_
