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

#ifndef DBCOMMON_SRC_DBCOMMON_FUNCTION_DECIMAL_FUNCTION_H_
#define DBCOMMON_SRC_DBCOMMON_FUNCTION_DECIMAL_FUNCTION_H_

#include <string>

#include "dbcommon/common/vector-transformer.h"
#include "dbcommon/common/vector/decimal-vector.h"
#include "dbcommon/nodes/datum.h"
#include "dbcommon/type/decimal.h"

namespace dbcommon {

Int128 decimal_add(Int128 val1, int64_t scale1, Int128 val2, int64_t scale2,
                   int64_t *rscale);

Int128 decimal_sub(Int128 val1, int64_t scale1, Int128 val2, int64_t scale2,
                   int64_t *rscale);

Int128 decimal_mul(Int128 val1, int64_t scale1, Int128 val2, int64_t scale2,
                   int64_t *rscale);

std::string decimal_div(const char *str1, uint64_t len1, const char *str2,
                        uint64_t len2, int64_t *rscale);

int64_t compute_div_scale(std::string str1, std::string str2);

std::string decimal_round_internal(std::string srcStr, const int64_t rscale);

Int128 get_decimal_sign(Int128 val);

bool decimal_equals_zero(std::string str);

bool decimal_is_nan(std::string str);

Datum decimal_abs(Datum *params, uint64_t size);

Datum decimal_sign(Datum *params, uint64_t size);

Datum decimal_ceil(Datum *params, uint64_t size);

Datum decimal_floor(Datum *params, uint64_t size);

Datum decimal_round(Datum *params, uint64_t size);

Datum decimal_round_without_scale(Datum *params, uint64_t size);

Datum decimal_trunc(Datum *params, uint64_t size);

Datum decimal_trunc_without_scale(Datum *params, uint64_t size);

Datum decimal_mod(Datum *params, uint64_t size);

template <typename Operator>
Datum op_decimal(Datum *params, uint64_t size) {
  assert(size == 2 && "invaild input");
  Object *para = DatumGetValue<Object *>(params[1]);
  if (dynamic_cast<Vector *>(para)) {
    DecimalVector *retVector = params[0];
    DecimalVector *srcVector = params[1];

    DecimalVectorRawData src(srcVector);
    retVector->resize(src.plainSize, src.sel, src.nulls);
    DecimalVectorRawData ret(retVector);

    auto operation = [&](int plainIdx) {
      DecimalVar srcVal(src.hightbits[plainIdx], src.lowbits[plainIdx],
                        src.scales[plainIdx]);
      auto retVal = Operator()(srcVal);
      std::tie(ret.hightbits[plainIdx], ret.lowbits[plainIdx],
               ret.scales[plainIdx]) =
          std::make_tuple(retVal.highbits, retVal.lowbits, retVal.scale);
    };
    transformVector(ret.plainSize, ret.sel, ret.nulls, operation);
  } else {
    Scalar *retScalar = params[0];
    Scalar *srcScalar = params[1];
    if (srcScalar->isnull) {
      retScalar->isnull = true;
    } else {
      retScalar->isnull = false;
      DecimalVar *retVal = retScalar->allocateValue<DecimalVar>();
      DecimalVar *srcVal = DatumGetValue<DecimalVar *>(srcScalar->value);
      *retVal = Operator()(*srcVal);
    }
  }
  return params[0];
}

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_FUNCTION_DECIMAL_FUNCTION_H_ */
