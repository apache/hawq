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

#include "dbcommon/function/typecast-texttonum-func.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stack>
#include <string>
#include <typeinfo>

#include "dbcommon/common/vector-transformer.h"
#include "dbcommon/common/vector.h"
#include "dbcommon/common/vector/decimal-vector.h"
#include "dbcommon/common/vector/fixed-length-vector.h"
#include "dbcommon/common/vector/variable-length-vector.h"
#include "dbcommon/function/arithmetic-function.h"
#include "dbcommon/function/decimal-function.h"
#include "dbcommon/function/function.h"
#include "dbcommon/type/decimal.h"
#include "dbcommon/type/type-util.h"
#include "dbcommon/utils/macro.h"
#include "dbcommon/utils/string-util.h"

namespace dbcommon {

template <typename TP>
Datum text_to_Float(Datum *params, uint64_t size) {
  assert(size == 2 && "invalid input");
  Object *para = params[1];

  auto sendErroLog = [](const char *&srcbufferFrontPtrtemp,
                        int64_t &strLength) {
    LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
              "invalid input syntax for type %s: \"%*.*s\"", pgTypeName<TP>(),
              strLength, strLength, srcbufferFrontPtrtemp);
  };
  auto textTypeTonumeric = [&sendErroLog](int64_t &strLength,
                                          const char *srcbufferPtr,
                                          TP &resval) {
    const char *srcbufferBackPtrtemp = srcbufferPtr + strLength;
    const char *srcbufferFrontPtrtemp = srcbufferPtr;
    if ((*srcbufferPtr < '0' || *srcbufferPtr > '9') && *srcbufferPtr != '-') {
      sendErroLog(srcbufferFrontPtrtemp, strLength);
    }
    uint64_t isNegative = (*srcbufferPtr == '-');
    if (isNegative) {
      ++srcbufferPtr;
    }

    resval = 0;
    const char *pdot = nullptr;
    const char *pe = nullptr;
    for (const char *ptemp = srcbufferPtr; ptemp < srcbufferBackPtrtemp;
         ++ptemp) {
      if (*ptemp == '.') {
        pdot = ptemp;
      }
      if (*ptemp == 'e' || *ptemp == 'E') {
        pe = ptemp;
      }
    }

    if (pe == srcbufferPtr || (pe < pdot && pe && pdot) ||
        (pe && *(pe + 1) != '+' && *(pe + 1) != '-' &&
         (*(pe + 1) < '0' || *(pe + 1) > '9'))) {
      sendErroLog(srcbufferFrontPtrtemp, strLength);
    }
    for (const char *ptemp = srcbufferPtr; ptemp < srcbufferBackPtrtemp;
         ++ptemp) {
      if ((*ptemp < '0' || *ptemp > '9') && ptemp != pdot && ptemp != pe &&
          ptemp != pe + 1) {
        sendErroLog(srcbufferFrontPtrtemp, strLength);
      }
    }

    const char *ptempend = pdot ? pdot : (pe ? pe : srcbufferBackPtrtemp);
    if (isNegative) {
      for (const char *ptemp = srcbufferPtr; ptemp < ptempend; ++ptemp) {
        resval = resval * 10 - (*ptemp - '0');
      }
    } else {
      for (const char *ptemp = srcbufferPtr; ptemp < ptempend; ++ptemp) {
        resval = resval * 10 + (*ptemp - '0');
      }
    }

    if (pdot) {
      TP tempval = 0;
      if (isNegative) {
        for (const char *ptemp = pe ? pe - 1 : srcbufferBackPtrtemp - 1;
             ptemp > pdot; --ptemp) {
          tempval = tempval / 10 - ((TP)(*ptemp - '0')) / 10;
        }
      } else {
        for (const char *ptemp = pe ? pe - 1 : srcbufferBackPtrtemp - 1;
             ptemp > pdot; --ptemp) {
          tempval = tempval / 10 + ((TP)(*ptemp - '0')) / 10;
        }
      }
      resval += tempval;
    }

    if (pe) {
      uint64_t isExNegative = (*(++pe) == '-');
      if (*pe < '0' || *pe > '9') {
        ++pe;
      }
      uint64_t exval = 0;
      for (; pe < srcbufferBackPtrtemp; ++pe) {
        exval = exval * 10 + (*pe - '0');
      }
      if (!isExNegative) {
        for (; exval > 0; --exval) {
          resval *= 10;
        }
      } else {
        for (; exval > 0; --exval) {
          resval /= 10;
        }
      }
    }
  };

  auto textToFloat = [&textTypeTonumeric](ByteBuffer &buf, text in) -> TP {
    TP resval;
    textTypeTonumeric(in.length, in.val, resval);
    return resval;
  };

  return one_param_bind<TP, text>(params, size, textToFloat);
}

template <typename TP>
Datum text_to_Integer(Datum *params, uint64_t size) {
  assert(size == 2 && "invalid input");
  Object *para = params[1];

  auto textTypeToNumeric = [](uint32_t &strLength, const char *srcBufferPtr,
                              TP &tempval1) {
    const char *srcbufferBackPtrtemp = srcBufferPtr + strLength;
    const char *srcbufferFrontPtrtemp = srcBufferPtr;
    uint64_t isNegative = (*srcBufferPtr == '-');
    if (isNegative) {
      ++srcBufferPtr;
    }
    for (const char *ptemp = srcBufferPtr; ptemp < srcbufferBackPtrtemp;
         ++ptemp) {
      if (*ptemp < '0' || *ptemp > '9') {
        LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
                  "invalid input syntax for integer: \"%*.*s\"",
                  isNegative ? strLength - 1 : strLength,
                  isNegative ? strLength - 1 : strLength, srcBufferPtr);
      }
    }

    uint64_t longVal = 0;
    const char *ptemp = srcBufferPtr;
    for (; ptemp < srcbufferBackPtrtemp; ++ptemp) {
      longVal = longVal * 10 + (*ptemp - '0');
    }

    if ((isNegative && longVal - 1 > std::numeric_limits<TP>::max()) ||
        (!isNegative && longVal > std::numeric_limits<TP>::max())) {
      LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                "value \"%*.*s\" is out of range for type %s", strLength,
                strLength, srcbufferFrontPtrtemp, pgTypeName<TP>());
    }

    tempval1 =
        isNegative ? -static_cast<TP>(longVal) : static_cast<TP>(longVal);
  };

  if (dynamic_cast<Vector *>(para)) {
    Vector *retVector = params[0];
    Vector *srcVector = params[1];
    VariableSizeTypeVectorRawData src(srcVector);
    retVector->resize(src.plainSize, src.sel, src.nulls);
    FixedSizeTypeVectorRawData<TP> ret(retVector);
    auto srcBuffer = srcVector->getValueBuffer();
    char *srcbufferPtr = srcBuffer->data();

    auto textToNumeric = [&](uint64_t plainIdx) {
      uint32_t strlen = src.lengths[plainIdx];
      TP &tempval1 = ret.values[plainIdx];
      textTypeToNumeric(strlen, src.valptrs[plainIdx], tempval1);
    };
    dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, textToNumeric);
    retVector->computeValPtrs();
  } else {
    Scalar *retScalar = params[0];
    Scalar *srcScalar = params[1];
    if (srcScalar->isnull) {
      retScalar->isnull = true;
    } else {
      retScalar->isnull = false;
      char *srcbufferPtr = srcScalar->value;
      uint32_t strlen = srcScalar->length;
      TP val = 0;
      textTypeToNumeric(strlen, srcbufferPtr, val);
      retScalar->value = CreateDatum<TP>(val);
    }
  }
  return params[0];
}

Datum text_to_int2(Datum *params, uint64_t size) {
  return text_to_Integer<int16_t>(params, size);
}
Datum text_to_int4(Datum *params, uint64_t size) {
  return text_to_Integer<int32_t>(params, size);
}
Datum text_to_int8(Datum *params, uint64_t size) {
  return text_to_Integer<int64_t>(params, size);
}
Datum text_to_float4(Datum *params, uint64_t size) {
  return text_to_Float<float>(params, size);
}
Datum text_to_float8(Datum *params, uint64_t size) {
  return text_to_Float<double>(params, size);
}

}  // namespace dbcommon
