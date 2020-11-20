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

DecimalVar stringToDecimal(const char *srcbufferPtr, int64_t strLength) {
  auto sendErroLog = [](const char *&srcbufferFrontPtrtemp,
                        int64_t &strLength) {
    LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
              "invalid input syntax for type %s: \"%*.*s\"",
              pgTypeName<DecimalVar>(), static_cast<int32_t>(strLength),
              static_cast<int32_t>(strLength), srcbufferFrontPtrtemp);
  };

  const char *srcbufferBackPtrtemp = srcbufferPtr + strLength;
  const char *srcbufferFrontPtrtemp = srcbufferPtr;
  if ((*srcbufferPtr < '0' || *srcbufferPtr > '9') && *srcbufferPtr != '-' &&
      *srcbufferPtr != '.') {
    sendErroLog(srcbufferFrontPtrtemp, strLength);
  }
  bool isNegative = (*srcbufferPtr == '-');
  if (isNegative) {
    ++srcbufferPtr;
  }

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
      (pe && *(pe + 1) != '+' && *(pe + 1) != '-')) {
    sendErroLog(srcbufferFrontPtrtemp, strLength);
  }
  for (const char *ptemp = srcbufferPtr; ptemp < srcbufferBackPtrtemp;
       ++ptemp) {
    if ((*ptemp < '0' || *ptemp > '9') && ptemp != pdot && ptemp != pe &&
        ptemp != pe + 1) {
      sendErroLog(srcbufferFrontPtrtemp, strLength);
    }
  }
  Int128 intVal(0);
  const char *ptempend = pdot ? pdot : (pe ? pe : srcbufferBackPtrtemp);
  const char *ptempend1 =
      ptempend - srcbufferPtr < 20 ? ptempend : srcbufferPtr + 19;
  uint64_t int64val = 0;
  for (const char *ptemp = srcbufferPtr; ptemp < ptempend1; ++ptemp) {
    int64val = int64val * 10 + *ptemp - '0';
  }
  intVal = Int128(0, int64val);
  if (ptempend1 < ptempend) {
    for (const char *ptemp = ptempend1; ptemp < ptempend; ++ptemp) {
      intVal *= Int128(10);
      intVal += Int128(*ptemp - '0');
    }
  }
  int64_t dotNum = 0;
  if (pdot) {
    const char *pend = pe ? pe : srcbufferBackPtrtemp;
    if (ptempend - srcbufferPtr < 20) {
      int64_t rest = 20 - (ptempend - srcbufferPtr);
      const char *ptempend2 =
          pend - (pdot + 1) < rest ? pend : pdot + 1 + rest - 1;
      for (const char *ptemp = pdot + 1; ptemp < ptempend2; ++ptemp) {
        int64val = int64val * 10 + *ptemp - '0';
      }
      intVal = Int128(0, int64val);
      if (ptempend2 < pend) {
        for (const char *ptemp = ptempend2; ptemp < pend; ++ptemp) {
          intVal *= Int128(10);
          intVal += Int128(*ptemp - '0');
        }
      }
    } else {
      for (const char *ptemp = pdot + 1; ptemp < pend; ++ptemp) {
        intVal *= Int128(10);
        intVal += Int128(*ptemp - '0');
      }
    }
    dotNum = (pend - 1) - pdot;
  }
  if (isNegative) {
    intVal.negate();
  }
  int64_t exval = 0;
  if (pe) {
    uint64_t isExNegative = *(pe + 1) == '-';
    for (const char *ptemp = pe + 2; ptemp < srcbufferBackPtrtemp; ++ptemp) {
      exval = exval * 10 + (*ptemp - '0');
    }
    exval = isExNegative ? -exval : exval;
  }
  int64_t scale = dotNum - exval;
  DecimalVar resVal =
      DecimalVar(intVal.getHighBits(), intVal.getLowBits(), scale);
  return scale < 0 ? resVal.cast(0) : resVal;
}

Datum textToDecimal(Datum *params, uint64_t size) {
  assert(size == 2 && "invalid input");
  Object *para = params[1];

  auto strToDecimal = [](ByteBuffer &buf, Text in) -> DecimalVar {
    int64_t strLength = in.length;
    const char *srcbufferPtr = in.val;
    return stringToDecimal(srcbufferPtr, strLength);
  };
  return one_param_bind<DecimalVar, Text>(params, size, strToDecimal);
}

Datum toNumber(Datum *params, uint64_t size) {
  assert(size == 3 && "invalid input");

  auto strToDecimal = [](ByteBuffer &buf, Text inStr,
                         Text inMod) -> DecimalVar {
// In all cases, Text 's length is determined by its length rather than '\0'.
#define NEXTCHAR(ptr, end)              \
  while (ptr < end && (++ptr) != end) { \
    if (*ptr != ' ') break;             \
  }
#define LASTCHAR(ptr)       \
  while (*(--ptr) == ' ') { \
  }
    int64_t strLength = inStr.length;
    const char *strFrontPtr = inStr.val;
    const char *strBackPtr = strFrontPtr + strLength;
    int64_t modLength = inMod.length;
    const char *modFrontPtr = inMod.val;
    const char *modBackPtr = modFrontPtr + modLength;

    int32_t numOfS = 0;
    int32_t numOfMI = 0;
    int32_t numOfDot = 0;
    int32_t numOfPR = 0;
    int32_t numOfPL = 0;
    int32_t dotNeg = 0;
    int32_t prNeg = 0;
    int32_t precision = 0;
    bool afterDot = false;
    Int128 intVal = 0;
    uint64_t int64Val = 0;
    int64_t scale = 0;
    int64_t num = 0;
    const char *ptempMod = modFrontPtr;
    const char *ptempNum = strFrontPtr;
    while (ptempMod < modBackPtr) {
      if (*ptempMod == 'E' || *ptempMod == 'e') {
        LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
                  "\"E\" is not supported for function \"to_char\"");
      }
      if (*ptempMod == 's' || *ptempMod == 'S') {
        ++numOfS;
        if (numOfS > 1) {
          LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
                    "cannot use \"S\" twice for function \"to_char\"");
        }
        if (numOfS + numOfMI > 1) {
          LOG_ERROR(
              ERRCODE_INVALID_TEXT_REPRESENTATION,
              "cannot use \"S\" and \"MI\" together for function \"to_char\"");
        }
        if (numOfPL) {
          LOG_ERROR(
              ERRCODE_INVALID_TEXT_REPRESENTATION,
              "cannot use \"S\" and \"PL\" together for function \"to_char\"");
        }
        if (numOfPR) {
          LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
                    "cannot use \"S\" and \"PR\"/\"PL\"/\"MI\"/\"SG\" together "
                    "for function \"to_char\"");
        }
        NEXTCHAR(ptempMod, modBackPtr);
        continue;
      }
      if (*ptempMod == 'f' || *ptempMod == 'F') {
        NEXTCHAR(ptempMod, modBackPtr);
        if (ptempMod < modBackPtr && (*ptempMod == 'm' || *ptempMod == 'M')) {
          NEXTCHAR(ptempMod, modBackPtr);
          continue;
        }
        NEXTCHAR(ptempNum, strBackPtr);
        continue;
      } else if (*ptempMod == 't' || *ptempMod == 'T') {
        NEXTCHAR(ptempMod, modBackPtr);
        if (ptempMod < modBackPtr && (*ptempMod == 'h' || *ptempMod == 'H')) {
          NEXTCHAR(ptempMod, modBackPtr);
          continue;
        }
        NEXTCHAR(ptempNum, strBackPtr);
        continue;
      }

      if (*ptempMod == 'm' || *ptempMod == 'M') {
        NEXTCHAR(ptempMod, modBackPtr);
        if (ptempMod < modBackPtr) {
          if (*ptempMod == 'i' || *ptempMod == 'I') {
            ++numOfMI;
            if (numOfS && numOfMI) {
              LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
                        "cannot use \"S\" and \"MI\" together for function "
                        "\"to_char\"");
            }
            NEXTCHAR(ptempMod, modBackPtr);
            if (numOfMI > 1) NEXTCHAR(ptempNum, strBackPtr);
            continue;
          }
        }
        NEXTCHAR(ptempNum, strBackPtr);
        continue;
      }
      if (*ptempMod == 'd' || *ptempMod == 'D' || *ptempMod == '.') {
        ++numOfDot;
        if (numOfDot > 1) {
          LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
                    "multiple decimal points for function \"to_char\"");
        }
        if (ptempNum < strBackPtr && *ptempNum != '.') {
          std::string tmpNum;
          for (const char *tmp = strFrontPtr; tmp < strBackPtr; ++tmp) {
            if (*tmp == '.') break;
            if (*tmp > '0' && *tmp < '9') tmpNum.push_back(*tmp);
          }
          LOG_ERROR(
              ERRCODE_INVALID_TEXT_REPRESENTATION,
              "A field with precision %d, scale 0 must round to an absolute "
              "value less than 10^%d. Rounded overflowing value: %s",
              precision, precision, tmpNum.c_str());
        } else {
          afterDot = true;
          NEXTCHAR(ptempNum, strBackPtr);
          NEXTCHAR(ptempMod, modBackPtr);
          continue;
        }
      }
      if (*ptempMod == 'r' || *ptempMod == 'R') {
        NEXTCHAR(ptempMod, modBackPtr);
        if (ptempMod < modBackPtr) {
          if (*ptempMod == 'n' || *ptempMod == 'N') {
            LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
                      "\"RN\" not supported with function \"to_number\"");
          }
        }
        NEXTCHAR(ptempNum, strBackPtr);
        continue;
      }
      if (*ptempMod == 'p' || *ptempMod == 'P') {
        NEXTCHAR(ptempMod, modBackPtr);
        if (ptempMod < modBackPtr) {
          if (*ptempMod == 'l' || *ptempMod == 'L') {
            ++numOfPL;
            if (numOfS)
              LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
                        "cannot use \"S\" and \"PL\" together for function "
                        "\"to_char\"");
            NEXTCHAR(ptempMod, modBackPtr);
            NEXTCHAR(ptempNum, strBackPtr);
            continue;
          }
          if (*ptempMod == 'r' || *ptempMod == 'R') {
            ++numOfPR;
            if (numOfS)
              LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
                        "cannot use \"PR\" and \"S\"/\"PL\"/\"MI\"/\"SG\" "
                        "together for function \"to_char\"");
            NEXTCHAR(ptempMod, modBackPtr);
            if (ptempMod < modBackPtr && (*ptempMod == '0' || *ptempMod == '9'))
              LOG_ERROR(
                  ERRCODE_INVALID_TEXT_REPRESENTATION,
                  "\"%d\" must be ahead of \"PR\" for function \"to_char\"",
                  *ptempMod - '0');
            NEXTCHAR(ptempNum, strBackPtr);
            continue;
          }
        }
        NEXTCHAR(ptempMod, modBackPtr);
        NEXTCHAR(ptempNum, strBackPtr);
        continue;
      }

      if (*ptempMod != '0' && *ptempMod != '9') {
        NEXTCHAR(ptempMod, modBackPtr);
        NEXTCHAR(ptempNum, strBackPtr);
      } else {
        if (ptempNum >= strBackPtr) {
          NEXTCHAR(ptempMod, modBackPtr);
          continue;
        }
        if (*ptempNum == '.') {
          afterDot = true;
          for (; ptempMod < modBackPtr; ++ptempMod) {
            if (*ptempMod == 'D' || *ptempMod == 'd' || *ptempMod == '.') {
              NEXTCHAR(ptempMod, modBackPtr);
              break;
            }
          }
          NEXTCHAR(ptempNum, strBackPtr);
          if (ptempNum < strBackPtr && *ptempNum == '-') dotNeg = 1;
        } else if (*ptempNum < '0' || *ptempNum > '9') {
          NEXTCHAR(ptempNum, strBackPtr);
        } else {
          if (afterDot) ++scale;
          num++;
          if (num < 20) {
            int64Val = int64Val * 10 + *ptempNum - '0';
            intVal = Int128(0, int64Val);
          } else {
            intVal *= Int128(10);
            intVal += Int128(*ptempNum - '0');
          }
          if (!afterDot) ++precision;
          NEXTCHAR(ptempNum, strBackPtr);
          NEXTCHAR(ptempMod, modBackPtr);
        }
      }
    }

    if (((dotNeg || *(strBackPtr - 1) == '-') && (numOfS || numOfMI)) ||
        *strFrontPtr == '-' || (*strFrontPtr == '<' && numOfPR))
      intVal.negate();
    return DecimalVar(intVal.getHighBits(), intVal.getLowBits(), scale);
  };
  return two_params_bind<DecimalVar, Text, Text>(params, size, strToDecimal);
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
