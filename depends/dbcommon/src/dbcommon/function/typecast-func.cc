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

#include <cmath>
#include <tuple>
#include <fstream>

//#include <boost/lexical_cast.hpp>
#include <boost/multiprecision/cpp_int.hpp>
#include "dbcommon/common/vector-transformer.h"
#include "dbcommon/common/vector.h"
#include "dbcommon/common/vector/decimal-vector.h"
#include "dbcommon/common/vector/fixed-length-vector.h"
#include "dbcommon/common/vector/timestamp-vector.h"
#include "dbcommon/function/decimal-function.h"
#include "dbcommon/function/function.h"
#include "dbcommon/function/typecast-func.cg.h"
#include "dbcommon/type/decimal.h"
#include "dbcommon/type/type-util.h"
#include "dbcommon/utils/macro.h"

// clang-format off

// [[[cog
#if false
from cog import out, outl
import sys, os
python_path = os.path.dirname(cog.inFile) + "/../python"
python_path = os.path.abspath(python_path)
sys.path.append(python_path)
from code_generator import *
cog.outl("""
/*
 * DO NOT EDIT!"
 * This file is generated from : %s
 */
""" % cog.inFile)
#endif
// ]]]
// [[[end]]]

// clang-format on

namespace dbcommon {

class TypeCast {
 public:
  template <typename TP, typename TR>
  static Datum castType(Datum *params, uint64_t size, bool isExplicit = false);

  template <typename TP, typename TR, bool isExplicit = false>
  static Datum castTypeToInteger(Datum *params, uint64_t size);

  template <typename TR>
  static Datum castDecimalToFloatType(Datum *params, uint64_t size);

  template <typename TP>
  static Datum castFloatTypeToDecimal(Datum *params, uint64_t size);

 private:
  template <typename TP, typename TR>
  static Datum valCastType(TP a, bool isExplicit);

  template <typename TP, typename TR>
  static Datum vecCastType(Vector *rvec, const Vector *vec, bool isExplicit);

  template <typename TR>
  static Datum valCastDecimalToIntegerType(DecimalVar *param);

  template <typename TR>
  static Datum vecCastDecimalToIntegerType(Vector *rvec, Vector *vec);

  template <typename TR>
  static Datum valCastDecimalToFloatType(DecimalVar *param);

  template <typename TR>
  static Datum vecCastDecimalToFloatType(Vector *rvec, Vector *vec);

  template <typename TP>
  static Datum vecCastFloatTypeToDecimal(Vector *rvec, Vector *vec);
};

// function for operator cast type
template <typename TP, typename TR>
Datum TypeCast::castType(Datum *params, uint64_t size, bool isExplicit) {
  assert(size == 2 && "invalid input");
  Object *para = DatumGetValue<Object *>(params[1]);
  Vector *vec = dynamic_cast<Vector *>(para);
  if (!vec) {
    auto rval = DatumGetValue<Scalar *>(params[0]);
    TP val = DatumGetValue<TP>(DatumGetValue<Scalar *>(params[1])->value);
    rval->value = valCastType<TP, TR>(val, isExplicit);
    return CreateDatum(rval);
  } else {
    auto *rvec = DatumGetValue<Vector *>(params[0]);
    return vecCastType<TP, TR>(rvec, vec, isExplicit);
  }
}

template <typename TP, typename TR>
bool isOverflow(TP val) {
  if (val > std::numeric_limits<TR>::max() ||
      val < std::numeric_limits<TR>::min()) {
    return true;
  }
  return false;
}

template <typename TP, typename TR, bool isExplicit>
Datum TypeCast::castTypeToInteger(Datum *params, uint64_t size) {
  assert(size == 2 && "invalid input");
  Object *para = params[1];

  if (!dynamic_cast<Vector *>(para)) {
    Scalar *retScalar = params[0];
    Scalar *srcScalar = params[1];

    if (srcScalar->isnull) {
      retScalar->isnull = true;
    } else {
      TP src = srcScalar->value;

      if (isExplicit) {
        if (isOverflow<TP, TR>(src)) {
          LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, "%s out of range",
                    TypeUtil::instance()
                        ->getTypeEntryById(TypeMapping<TR>::type)
                        ->name.c_str());
        }
      }

      retScalar->isnull = false;
      retScalar->value = CreateDatum(static_cast<TR>(rint(src)));
    }

  } else {
    Vector *retVector = params[0];
    Vector *srcVector = params[1];

    FixedSizeTypeVectorRawData<TP> src(srcVector);
    retVector->resize(src.plainSize, src.sel, src.nulls);
    FixedSizeTypeVectorRawData<TR> ret(retVector);

    if (isExplicit) {
      bool castError = false;
      auto checkOverflow = [&](uint64_t plainIdx) {
        castError |= isOverflow<TP, TR>(src.values[plainIdx]);
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls,
                                checkOverflow);
      if (castError)
        LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, "%s out of range",
                  TypeUtil::instance()
                      ->getTypeEntryById(retVector->getTypeKind())
                      ->name.c_str());
    }

    auto castToInteger = [&](uint64_t plainIdx) {
      ret.values[plainIdx] = static_cast<TR>(rint(src.values[plainIdx]));
    };
    dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, castToInteger);
  }

  return params[0];
}

template <typename TP, typename TR>
Datum TypeCast::valCastType(TP val, bool isExplicit) {
  if (isExplicit) {
    if (!isOverflow<TP, TR>(val)) {
      TR res = static_cast<TR>(val);
      return CreateDatum(res);
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "typecast value out of range!");
    }
  } else {
    return CreateDatum(static_cast<TR>(val));
  }
}

template <typename TP, typename TR>
Datum TypeCast::vecCastType(Vector *rvec, const Vector *vec, bool isExplicit) {
  const TP *v = reinterpret_cast<const TP *>(vec->getValue());
  auto sel = vec->getSelected();

  bool hasNull = vec->hasNullValue();
  rvec->setHasNull(hasNull);

  uint64_t sz = vec->getNumOfRows();
  const uint64_t plainSize = vec->getNumOfRowsPlain();
  dbcommon::ByteBuffer &rDataBuf = *rvec->getValueBuffer();
  rDataBuf.resize(plainSize * sizeof(TR));
  TR *__restrict__ rdata = reinterpret_cast<TR *>(rDataBuf.data());
  char *__restrict__ rnull;
  if (hasNull) {
    dbcommon::BoolBuffer &rNullBuf = *rvec->getNullBuffer();
    rNullBuf.resize(plainSize);
    rnull = rNullBuf.getChars();
  }

  if (sel) {
    rvec->setSelected(sel, true);
    if (hasNull) {
      const char *__restrict__ nulls = vec->getNullBuffer()->getChars();
      if (isExplicit) {
#pragma clang loop unroll(full)
        for (uint64_t i = 0; i < sz; ++i) {
          auto index = (*sel)[i];
          if (!isOverflow<TP, TR>(v[index])) {
            rdata[index] = static_cast<TR>(v[index]);
            rnull[index] = nulls[index];
          } else {
            LOG_ERROR(ERRCODE_INTERNAL_ERROR, "typecast value out of range!");
          }
        }
      } else {
#pragma clang loop unroll(full)
        for (uint64_t i = 0; i < sz; ++i) {
          auto index = (*sel)[i];
          rdata[index] = static_cast<TR>(v[index]);
          rnull[index] = nulls[index];
        }
      }
    } else {
      if (isExplicit) {
#pragma clang loop unroll(full)
        for (uint64_t i = 0; i < sz; ++i) {
          auto index = (*sel)[i];
          if (!isOverflow<TP, TR>(v[index])) {
            rdata[index] = static_cast<TR>(v[index]);
          } else {
            LOG_ERROR(ERRCODE_INTERNAL_ERROR, "typecast value out of range!");
          }
        }
      } else {
#pragma clang loop unroll(full)
        for (uint64_t i = 0; i < sz; ++i) {
          auto index = (*sel)[i];
          rdata[index] = static_cast<TR>(v[index]);
        }
      }
    }
  } else {
    if (hasNull) {
      const char *__restrict__ nulls = vec->getNullBuffer()->getChars();
      if (isExplicit) {
#pragma clang loop unroll(full)
        for (uint64_t i = 0; i < sz; ++i) {
          if (!isOverflow<TP, TR>(v[i])) {
            rdata[i] = static_cast<TR>(v[i]);
            rnull[i] = nulls[i];
          } else {
            LOG_ERROR(ERRCODE_INTERNAL_ERROR, "typecast value out of range!");
          }
        }
      } else {
#pragma clang loop unroll(full)
        for (uint64_t i = 0; i < sz; ++i) {
          rdata[i] = static_cast<TR>(v[i]);
          rnull[i] = nulls[i];
        }
      }
    } else {
      if (isExplicit) {
#pragma clang loop unroll(full)
        for (uint64_t i = 0; i < sz; ++i) {
          if (!isOverflow<TP, TR>(v[i])) {
            rdata[i] = static_cast<TR>(v[i]);
          } else {
            LOG_ERROR(ERRCODE_INTERNAL_ERROR, "typecast value out of range!");
          }
        }
      } else {
#pragma clang loop unroll(full)
        for (uint64_t i = 0; i < sz; ++i) {
          rdata[i] = static_cast<TR>(v[i]);
        }
      }
    }
  }

  return CreateDatum(rvec);
}

template <typename TR>
Datum TypeCast::castDecimalToFloatType(Datum *params, uint64_t size) {
  assert(size == 2 && "invalid input");
  Object *para = DatumGetValue<Object *>(params[1]);
  Vector *vec = dynamic_cast<Vector *>(para);
  if (!vec) {
    Scalar *ret = params[0];
    Scalar *scalar = params[1];
    ret->isnull = scalar->isnull;
    if (!scalar->isnull) {
      DecimalVar *val = DatumGetValue<DecimalVar *>(scalar->value);
      ret->value = valCastDecimalToFloatType<TR>(val);
    }
    return CreateDatum(ret);
  } else {
    auto *rvec = DatumGetValue<Vector *>(params[0]);
    return vecCastDecimalToFloatType<TR>(rvec, vec);
  }
}

template <typename TP>
inline DecimalVar floatToDecimal(TP in) {
  int64_t scale = 0;
  while (true) {
    if (scale >= 13) {
      break;
    }
    if (abs(in - static_cast<int64_t>(in)) > 1e-8) {
      in *= 10.0;
      ++scale;
    } else {
      break;
    }
  }
  Int128 int128 = static_cast<int64_t>(in);
  return DecimalVar(int128.getHighBits(), int128.getLowBits(), scale);
}

template <typename TP>
Datum TypeCast::castFloatTypeToDecimal(Datum *params, uint64_t size) {
  assert(size == 2 && "invalid input");
  Object *para = DatumGetValue<Object *>(params[1]);
  Vector *vec = dynamic_cast<Vector *>(para);
  if (!vec) {
    Scalar *retScalar = params[0];
    Scalar *srcScalar = params[1];

    if (srcScalar->isnull) {
      retScalar->isnull = true;
    } else {
      retScalar->isnull = false;
      DecimalVar *ret = retScalar->allocateValue<DecimalVar>();

      TP val = DatumGetValue<TP>(srcScalar->value);
      *ret = floatToDecimal(val);
    }
    return CreateDatum(retScalar);
  } else {
    auto *rvec = DatumGetValue<Vector *>(params[0]);
    return vecCastFloatTypeToDecimal<TP>(rvec, vec);
  }
}

template <typename TR>
bool isDecimalOverflow(std::string val) {
  boost::multiprecision::cpp_int mval(val);
  if (mval > std::numeric_limits<TR>::max() ||
      mval < std::numeric_limits<TR>::min()) {
    return true;
  }
  return false;
}

template <int64_t n>
constexpr uint64_t res2() {
  return 2 * res2<n - 1>();
}

template <>
constexpr uint64_t res2<1>() {
  return 2;
}

template <typename TR>
inline TR decimalToFloat(int64_t highbits, uint64_t lowbits, int64_t scale) {
  TR exp[] = {
      1.0,
      10.0,
      100.0,
      1000.0,
      10000.0,
      100000.0,
      1000000.0,
      10000000.0,
      100000000.0,
      1000000000.0,
      10000000000.0,
      100000000000.0,
      1000000000000.0,
      10000000000000.0,
      100000000000000.0,
      1000000000000000.0,
      10000000000000000.0,
      100000000000000000.0,
      1000000000000000000.0,
      10000000000000000000.0,
      100000000000000000000.0,
  };
  Int128 int128(highbits, lowbits);
  int128.abs();
  TR fval =
      static_cast<TR>(int128.getHighBits()) * static_cast<TR>(res2<64>()) +
      static_cast<TR>(int128.getLowBits());
  if (scale <= 20)
    fval /= exp[scale];
  else
    for (int32_t i = 0; i < scale; ++i) fval /= 10.0;
  return highbits < 0 ? -fval : fval;
}

template <typename TR>
Datum TypeCast::valCastDecimalToFloatType(DecimalVar *param) {
  return CreateDatum(static_cast<TR>(
      decimalToFloat<TR>(param->highbits, param->lowbits, param->scale)));
}

template <typename TR>
Datum TypeCast::vecCastDecimalToFloatType(Vector *rvec, Vector *vec) {
  auto sel = vec->getSelected();
  DecimalVector *dvec = dynamic_cast<DecimalVector *>(vec);
  const int64_t *hval =
      reinterpret_cast<const int64_t *>(dvec->getAuxiliaryValue());
  const uint64_t *lval = reinterpret_cast<const uint64_t *>(dvec->getValue());
  const int64_t *sval =
      reinterpret_cast<const int64_t *>(dvec->getScaleValue());
  bool hasNull = vec->hasNullValue();
  rvec->setHasNull(hasNull);
  uint64_t sz = vec->getNumOfRows();
  const uint64_t plainSize = vec->getNumOfRowsPlain();
  dbcommon::ByteBuffer &rDataBuf = *rvec->getValueBuffer();
  rDataBuf.resize(plainSize * sizeof(TR));
  TR *__restrict__ rdata = reinterpret_cast<TR *>(rDataBuf.data());

  char *__restrict__ rnull;
  if (hasNull) {
    dbcommon::BoolBuffer *rNullBuf = rvec->getNullBuffer();
    rvec->getNullBuffer()->resize(plainSize);
    rnull = rNullBuf->getChars();
  }

  if (sel) {
    rvec->setSelected(sel, true);
    if (hasNull) {
      const char *__restrict__ nulls = vec->getNullBuffer()->getChars();
      for (uint64_t i = 0; i < sz; ++i) {
        auto index = (*sel)[i];
        rnull[index] = nulls[index];
        if (rnull[index]) {
          rdata[index] = 0;
        } else {
          rdata[index] =
              decimalToFloat<TR>(hval[index], lval[index], sval[index]);
        }
      }
    } else {
      for (uint64_t i = 0; i < sz; ++i) {
        auto index = (*sel)[i];
        rdata[index] =
            decimalToFloat<TR>(hval[index], lval[index], sval[index]);
      }
    }
  } else {
    if (hasNull) {
      const char *__restrict__ nulls = vec->getNullBuffer()->getChars();
      for (uint64_t i = 0; i < sz; ++i) {
        rnull[i] = nulls[i];
        if (rnull[i]) {
          rdata[i] = 0;
        } else {
          rdata[i] = decimalToFloat<TR>(hval[i], lval[i], sval[i]);
        }
      }
    } else {
      for (uint64_t i = 0; i < sz; ++i) {
        rdata[i] = decimalToFloat<TR>(hval[i], lval[i], sval[i]);
      }
    }
  }

  return CreateDatum(rvec);
}

template <typename TP>
Datum TypeCast::vecCastFloatTypeToDecimal(Vector *rvec, Vector *vec) {
  const TP *v = reinterpret_cast<const TP *>(vec->getValue());
  auto sel = vec->getSelected();

  bool hasNull = vec->hasNullValue();
  rvec->setHasNull(hasNull);
  uint64_t sz = vec->getNumOfRows();
  const uint64_t plainSize = vec->getNumOfRowsPlain();

  DecimalVector *dvec = dynamic_cast<DecimalVector *>(rvec);
  dbcommon::ByteBuffer *rLowbitDataBuf = dvec->getValueBuffer();
  rLowbitDataBuf->resize(plainSize * sizeof(uint64_t));
  uint64_t *rLowbit = reinterpret_cast<uint64_t *>(rLowbitDataBuf->data());
  dbcommon::ByteBuffer *rHighbitDataBuf = dvec->getAuxiliaryValueBuffer();
  rHighbitDataBuf->resize(plainSize * sizeof(int64_t));
  int64_t *rHighbit = reinterpret_cast<int64_t *>(rHighbitDataBuf->data());
  dbcommon::ByteBuffer *rScaleDataBuf = dvec->getScaleValueBuffer();
  rScaleDataBuf->resize(plainSize * sizeof(int64_t));
  int64_t *rScales = reinterpret_cast<int64_t *>(rScaleDataBuf->data());

  char *__restrict__ rnull;
  if (hasNull) {
    dbcommon::BoolBuffer &rNullBuf = *rvec->getNullBuffer();
    rNullBuf.resize(plainSize);
    rnull = rNullBuf.getChars();
  }

  char buffer[DBL_DIG + 100];

  if (sel) {
    if (hasNull) {
      const char *__restrict__ nulls = vec->getNullBuffer()->getChars();
      for (uint64_t i = 0; i < sz; ++i) {
        auto index = (*sel)[i];
        rnull[index] = nulls[index];
        if (!rnull[index]) {
          DecimalVar decValtmp = floatToDecimal(v[index]);
          rLowbit[index] = decValtmp.lowbits;
          rHighbit[index] = decValtmp.highbits;
          rScales[index] = decValtmp.scale;
        }
      }
    } else {
      for (uint64_t i = 0; i < sz; ++i) {
        auto index = (*sel)[i];
        DecimalVar decValtmp = floatToDecimal(v[index]);
        rLowbit[index] = decValtmp.lowbits;
        rHighbit[index] = decValtmp.highbits;
        rScales[index] = decValtmp.scale;
      }
    }
  } else {
    if (hasNull) {
      const char *__restrict__ nulls = vec->getNullBuffer()->getChars();
      for (uint64_t i = 0; i < sz; ++i) {
        rnull[i] = nulls[i];
        if (!rnull[i]) {
          DecimalVar decValtmp = floatToDecimal(v[i]);
          rLowbit[i] = decValtmp.lowbits;
          rHighbit[i] = decValtmp.highbits;
          rScales[i] = decValtmp.scale;
        }
      }
    } else {
      for (uint64_t i = 0; i < sz; ++i) {
        DecimalVar decValtmp = floatToDecimal(v[i]);
        rLowbit[i] = decValtmp.lowbits;
        rHighbit[i] = decValtmp.highbits;
        rScales[i] = decValtmp.scale;
      }
    }
  }

  if (auto sel = vec->getSelected()) {
    rvec->setSelected(sel, true);
  }
  return CreateDatum(rvec);
}

template <typename TR>
Datum boolToInt(Datum *params, uint64_t size) {
  auto cast = [](ByteBuffer &buf, bool val) -> TR {
    return static_cast<TR>(val);
  };
  return one_param_bind<TR, bool>(params, size, cast);
}

template <typename TP>
Datum intToBool(Datum *params, uint64_t size) {
  assert(size == 2);
  Object *para = DatumGetValue<Object *>(params[1]);
  if (dynamic_cast<Vector *>(para)) {
    SelectList *retSelectlist = params[0];
    Vector *srcVector = params[1];

    FixedSizeTypeVectorRawData<TP> src(srcVector);

    uint64_t counter = 0;
    retSelectlist->setNulls(src.plainSize, src.sel, retSelectlist->getNulls(),
                            src.nulls);
    SelectList::value_type *ret = retSelectlist->begin();
    auto cast = [&](uint64_t plainIdx) {
      if (src.values[plainIdx]) {
        ret[counter++] = plainIdx;
      }
    };
    dbcommon::transformVector(src.plainSize, src.sel, src.nulls, cast);
    retSelectlist->resize(counter);
  } else {
    Scalar *retScalar = params[0];
    Scalar *srcScalar = params[1];
    retScalar->clear();
    if (srcScalar->isnull) {
      retScalar->isnull = true;
    } else {
      retScalar->isnull = false;
      TP val = srcScalar->value;
      retScalar->value = CreateDatum<bool>(val ? true : false);
    }
  }
  return params[0];
}

Datum bool_to_int2(Datum *params, uint64_t size) {
  return boolToInt<int16_t>(params, size);
}

Datum bool_to_int4(Datum *params, uint64_t size) {
  return boolToInt<int32_t>(params, size);
}

Datum bool_to_int8(Datum *params, uint64_t size) {
  return boolToInt<int64_t>(params, size);
}

Datum int2_to_bool(Datum *params, uint64_t size) {
  return intToBool<int16_t>(params, size);
}

Datum int4_to_bool(Datum *params, uint64_t size) {
  return intToBool<int32_t>(params, size);
}

Datum int8_to_bool(Datum *params, uint64_t size) {
  return intToBool<int64_t>(params, size);
}

// [[[cog
#if false
def generate_typecast_with_rettype_def(optype, typemap):
  typeList = sorted(typemap.keys())
  for i in range(len(typeList)):
    for j in xrange(len(typeList)):
      if typeList[i] in DECIMAL_TYPE.keys():
        rettype = typemap[typeList[j]].symbol
        paramtype = typemap[typeList[i]].symbol
        funcImpl = "decimal_to_%s" % (typemap[typeList[j]].name)
        if typeList[j] in FLOAT_TYPES.keys():
          cog.outl("""
Datum %s (Datum *params, uint64_t size) {
  return %s::castDecimalToFloatType<%s>(params, size);
}
""" % (funcImpl, optype, rettype))
      elif typeList[j] in DECIMAL_TYPE.keys():
        rettype = typemap[typeList[j]].symbol
        paramtype = typemap[typeList[i]].symbol
        funcImpl = "%s_to_decimal" % (typemap[typeList[i]].name)
        if typeList[i] in FLOAT_TYPES.keys():
            cog.outl("""
Datum %s (Datum *params, uint64_t size) {
  return %s::castFloatTypeToDecimal<%s>(params, size);
}
""" % (funcImpl, optype, paramtype))
      elif i < j:
        rettype = typemap[typeList[j]].symbol
        paramtype = typemap[typeList[i]].symbol
        funcImpl = "%s_to_%s" % (typemap[typeList[i]].name, typemap[typeList[j]].name)
        if typeList[j] in INT_TYPES.keys():
          cog.outl("""
Datum %s (Datum *params, uint64_t size) {
  return %s::castTypeToInteger<%s, %s>(params, size);
}
""" % (funcImpl, optype, paramtype, rettype))
        else:
          cog.outl("""
Datum %s (Datum *params, uint64_t size) {
	return %s::castType<%s, %s>(params, size);
}
""" % (funcImpl, optype, paramtype, rettype))
      elif i > j:
        rettype = typemap[typeList[j]].symbol
        paramtype = typemap[typeList[i]].symbol
        funcImpl = "%s_to_%s" % (typemap[typeList[i]].name, typemap[typeList[j]].name)
        if typeList[j] in INT_TYPES.keys():
          cog.outl("""
Datum %s (Datum *params, uint64_t size) {
  return %s::castTypeToInteger<%s, %s, true>(params, size);
}
""" % (funcImpl, optype, paramtype, rettype))
        else:
          cog.outl("""
Datum %s (Datum *params, uint64_t size) {
	return %s::castType<%s, %s>(params, size);
}
""" % (funcImpl, optype, paramtype, rettype))
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
generate_typecast_with_rettype_def("TypeCast", NUMERIC_AND_DECIMAL_TYPES)
#endif
// ]]]
// [[[end]]]

Datum char_to_string(Datum *params, uint64_t size) { return params[1]; }

}  // namespace dbcommon
