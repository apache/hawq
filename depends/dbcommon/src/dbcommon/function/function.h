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

#ifndef DBCOMMON_SRC_DBCOMMON_FUNCTION_FUNCTION_H_
#define DBCOMMON_SRC_DBCOMMON_FUNCTION_FUNCTION_H_

#include "dbcommon/common/vector-transformer.h"
#include "dbcommon/common/vector/decimal-vector.h"

namespace dbcommon {

// Here, I am going to draft the blueprint on how these template function and
// callable object work together to construct a dbcommon::Function.
//
// There are six basic elements:
//     1. C++ POD and ByteBuffer
//            actual physical unit that specify how to retrieve raw data, which
//            could be int32_t, int64_t, text, Timestamp, DecimalVar and etc.
//
//     2. callable object
//            operate on C++ POD and ByteBuffer, which works like that in
//            Postgres
//
//     3. n-ary_param_bind
//            delegate specific function according to input type's combination
//            of n-ary Cartesian power of {Vector, Scalar}
//
//     4. Vector/Scalar and their corresponding VectorRawData/ScalarRawData
//            used by delegated function to retrieve specific raw data according
//            to the deduced {Vector, Scalar} combination and POD parameterized
//            type
//
//     5. func_bind
//            use VectorRawData/ScalarRawData's setter and getter to bind
//            callable object over the raw data iterator
//
//     6. transformVector
//            apply callable object when iterate over the raw data
//
// Typically, the elements that needed to be taken into count for users is 1, 2,
// and 3.
//
// Take DEMO_FUNCTION as an example, which capitalizes these basic elements.
//
// User specifies 2 parameterized type, calls one_param_bind and provides a
// callable object, which is a lambda function that takes matching argument type
// and return matching return type according the the specified parameterized
// type.
//
// Datum DEME_FUNCTION(Datum *params, uint64_t size) {
//   auto CALLABLE_OBJECT = [](ByteBuffer &buf, PARA_TYPE1 str) -> RETURN_TYPE {
//     buf.resize(buf.size() + str.length);
//     memcpy(buf.tail() - str.length, str.val, str.length);
//     return RETURN_TYPE(nullptr, str.length);
//   };
//   return ONE_PARAM_BIND<RETURN_TYPE, PARA_TYPE1>(params, size,
//   CALLABLE_OBJECT);
// }
//
// The most tricky thing among these elements is ByteBuffer, which is a special
// use for supporting memory allocation for variable length data type, i.e.
// text.

template <class Type>
struct VectorRawData {
  VectorRawData<Type>(Vector *in) : vec(in) {}

  Type get(uint64_t plainIdx) { return vec.values[plainIdx]; }
  void set(uint64_t plainIdx, Type val) { vec.values[plainIdx] = val; }

  FixedSizeTypeVectorRawData<Type> vec;
};

template <>
struct VectorRawData<text> {
  VectorRawData<text>(Vector *in) : vec(in) {}

  text get(uint64_t plainIdx) {
    return text(vec.valptrs[plainIdx], vec.lengths[plainIdx]);
  }
  void set(uint64_t plainIdx, text val) { vec.lengths[plainIdx] = val.length; }

  VariableSizeTypeVectorRawData vec;
};

template <>
struct VectorRawData<DecimalVar> {
  VectorRawData<DecimalVar>(Vector *in) : vec(in) {}

  DecimalVar get(uint64_t plainIdx) {
    return DecimalVar(vec.hightbits[plainIdx], vec.lowbits[plainIdx],
                      vec.scales[plainIdx]);
  }
  void set(uint64_t plainIdx, DecimalVar val) {
    vec.hightbits[plainIdx] = val.highbits;
    vec.lowbits[plainIdx] = val.lowbits;
    vec.scales[plainIdx] = val.scale;
  }

  DecimalVectorRawData vec;
};

template <>
struct VectorRawData<Timestamp> {
  VectorRawData<Timestamp>(Vector *in) : vec(in) {}

  Timestamp get(uint64_t plainIdx) {
    return Timestamp(vec.seconds[plainIdx], vec.nanoseconds[plainIdx]);
  }
  void set(uint64_t plainIdx, Timestamp val) {
    vec.seconds[plainIdx] = val.second;
    vec.nanoseconds[plainIdx] = val.nanosecond;
  }
  TimestampVectorRawData vec;
};

template <>
struct VectorRawData<IntervalVar> {
  VectorRawData<IntervalVar>(Vector *in) : vec(in) {}

  IntervalVar get(uint64_t plainIdx) {
    return IntervalVar(vec.timeOffsets[plainIdx], vec.days[plainIdx],
                       vec.months[plainIdx]);
  }
  void set(uint64_t plainIdx, IntervalVar val) {
    vec.timeOffsets[plainIdx] = val.timeOffset;
    vec.days[plainIdx] = val.day;
    vec.months[plainIdx] = val.month;
  }
  IntervalVectorRawData vec;
};

template <class Type>
struct ScalarRawData {
  ScalarRawData<Type>(Scalar *in) : scalar(in) {}

  Type get() { return scalar->value; }
  void set(Type val) { scalar->value = CreateDatum<Type>(val); }

 private:
  Scalar *scalar;
};

template <>
struct ScalarRawData<text> {
  ScalarRawData<text>(Scalar *in) : scalar(in) {}

  text get() { return text(scalar->value, scalar->length); }
  void set(text val) {
    scalar->value = CreateDatum(scalar->getValueBuffer()->data());
    scalar->length = val.length;
  }

 private:
  Scalar *scalar;
};

template <>
struct ScalarRawData<DecimalVar> {
  ScalarRawData<DecimalVar>(Scalar *in) : scalar(in) {}

  DecimalVar get() { return *DatumGetValue<DecimalVar *>(scalar->value); }
  void set(DecimalVar val) { scalar->length = sizeof(DecimalVar); }

 private:
  Scalar *scalar;
};

template <>
struct ScalarRawData<Timestamp> {
  ScalarRawData<Timestamp>(Scalar *in) : scalar(in) {}

  Timestamp get() {
    Timestamp *val = DatumGetValue<Timestamp *>(scalar->value);
    return *val;
  }

  void set(Timestamp ts) {
    Timestamp *val = scalar->allocateValue<Timestamp>();
    val->second = ts.second;
    val->nanosecond = ts.nanosecond;
    scalar->value = CreateDatum<Timestamp *>(val);
    scalar->length = sizeof(Timestamp);
  }

 private:
  Scalar *scalar;
};

template <>
struct ScalarRawData<IntervalVar> {
  ScalarRawData<IntervalVar>(Scalar *in) : scalar(in) {}

  IntervalVar get() {
    IntervalVar *val = DatumGetValue<IntervalVar *>(scalar->value);
    return *val;
  }

  void set(IntervalVar interval) {
    IntervalVar *val = scalar->allocateValue<IntervalVar>();
    val->timeOffset = interval.timeOffset;
    val->day = interval.day;
    val->month = interval.month;
    scalar->value = CreateDatum<IntervalVar *>(val);
    scalar->length = sizeof(IntervalVar);
  }

 private:
  Scalar *scalar;
};

template <typename RetType, typename ParaType1, typename ParaType2,
          typename ParaType3, class FunctionProto>
inline Datum transform_VectorVectorVector(Datum *params, uint64_t size,
                                          FunctionProto func) {
  Vector *retVector = params[0];
  Vector *para1Vector = params[1];
  Vector *para2Vector = params[2];
  Vector *para3Vector = params[3];

  para1Vector->trim();
  para2Vector->trim();
  para3Vector->trim();
  VectorRawData<ParaType1> para1(para1Vector);
  VectorRawData<ParaType2> para2(para2Vector);
  VectorRawData<ParaType3> para3(para3Vector);
  retVector->resize(para1.vec.plainSize, para1.vec.sel, para1.vec.nulls,
                    para2.vec.nulls, para3.vec.nulls);
  VectorRawData<RetType> ret(retVector);

  auto func_bind = [&](uint64_t plainIdx) {
    ret.set(plainIdx, func(*retVector->getValueBuffer(), para1.get(plainIdx),
                           para2.get(plainIdx), para3.get(plainIdx)));
  };

  transformVector(ret.vec.plainSize, ret.vec.sel, ret.vec.nulls, func_bind);
  retVector->computeValPtrs();

  return params[0];
}

template <typename RetType, typename ParaType1, typename ParaType2,
          typename ParaType3, class FunctionProto>
inline Datum transform_VectorVectorScalar(Datum *params, uint64_t size,
                                          FunctionProto func) {
  Vector *retVector = params[0];
  Vector *para1Vector = params[1];
  Vector *para2Vector = params[2];
  Scalar *para3Scalar = params[3];

  para1Vector->trim();
  para2Vector->trim();
  VectorRawData<ParaType1> para1(para1Vector);
  VectorRawData<ParaType2> para2(para2Vector);
  ScalarRawData<ParaType3> para3(para3Scalar);

  if (para3Scalar->isnull) {
    retVector->resize(para1.vec.plainSize, para1.vec.sel, true);
  } else {
    retVector->resize(para1.vec.plainSize, para1.vec.sel, para1.vec.nulls,
                      para2.vec.nulls);
    VectorRawData<RetType> ret(retVector);

    auto func_bind = [&](uint64_t plainIdx) {
      ret.set(plainIdx, func(*retVector->getValueBuffer(), para1.get(plainIdx),
                             para2.get(plainIdx), para3.get()));
    };

    transformVector(ret.vec.plainSize, ret.vec.sel, ret.vec.nulls, func_bind);
    retVector->computeValPtrs();
  }

  return params[0];
}

template <typename RetType, typename ParaType1, typename ParaType2,
          typename ParaType3, class FunctionProto>
inline Datum transform_VectorScalarVector(Datum *params, uint64_t size,
                                          FunctionProto func) {
  Vector *retVector = params[0];
  Vector *para1Vector = params[1];
  Scalar *para2Scalar = params[2];
  Vector *para3Vector = params[3];

  para1Vector->trim();
  para3Vector->trim();
  VectorRawData<ParaType1> para1(para1Vector);
  ScalarRawData<ParaType2> para2(para2Scalar);
  VectorRawData<ParaType3> para3(para3Vector);

  if (para2Scalar->isnull) {
    retVector->resize(para1.vec.plainSize, para1.vec.sel, true);
  } else {
    retVector->resize(para1.vec.plainSize, para1.vec.sel, para1.vec.nulls,
                      para3.vec.nulls);
    VectorRawData<RetType> ret(retVector);

    auto func_bind = [&](uint64_t plainIdx) {
      ret.set(plainIdx, func(*retVector->getValueBuffer(), para1.get(plainIdx),
                             para2.get(), para3.get(plainIdx)));
    };

    transformVector(ret.vec.plainSize, ret.vec.sel, ret.vec.nulls, func_bind);
    retVector->computeValPtrs();
  }

  return params[0];
}

template <typename RetType, typename ParaType1, typename ParaType2,
          typename ParaType3, class FunctionProto>
inline Datum transform_VectorScalarScalar(Datum *params, uint64_t size,
                                          FunctionProto func) {
  Vector *retVector = params[0];
  Vector *para1Vector = params[1];
  Scalar *para2Scalar = params[2];
  Scalar *para3Scalar = params[3];

  para1Vector->trim();
  VectorRawData<ParaType1> para1(para1Vector);
  ScalarRawData<ParaType2> para2(para2Scalar);
  ScalarRawData<ParaType3> para3(para3Scalar);

  if (para2Scalar->isnull || para3Scalar->isnull) {
    retVector->resize(para1.vec.plainSize, para1.vec.sel, true);
  } else {
    retVector->resize(para1.vec.plainSize, para1.vec.sel, para1.vec.nulls);
    VectorRawData<RetType> ret(retVector);

    auto func_bind = [&](uint64_t plainIdx) {
      ret.set(plainIdx, func(*retVector->getValueBuffer(), para1.get(plainIdx),
                             para2.get(), para3.get()));
    };

    transformVector(ret.vec.plainSize, ret.vec.sel, ret.vec.nulls, func_bind);
    retVector->computeValPtrs();
  }

  return params[0];
}

template <typename RetType, typename ParaType1, typename ParaType2,
          typename ParaType3, class FunctionProto>
inline Datum transform_ScalarVectorVector(Datum *params, uint64_t size,
                                          FunctionProto func) {
  Vector *retVector = params[0];
  Scalar *para1Scalar = params[1];
  Vector *para2Vector = params[2];
  Vector *para3Vector = params[3];

  para2Vector->trim();
  para3Vector->trim();
  ScalarRawData<ParaType1> para1(para1Scalar);
  VectorRawData<ParaType2> para2(para2Vector);
  VectorRawData<ParaType3> para3(para3Vector);

  if (para1Scalar->isnull) {
    retVector->resize(para2.vec.plainSize, para2.vec.sel, true);
  } else {
    retVector->resize(para2.vec.plainSize, para2.vec.sel, para2.vec.nulls,
                      para3.vec.nulls);
    VectorRawData<RetType> ret(retVector);

    auto func_bind = [&](uint64_t plainIdx) {
      ret.set(plainIdx, func(*retVector->getValueBuffer(), para1.get(),
                             para2.get(plainIdx), para3.get(plainIdx)));
    };

    transformVector(ret.vec.plainSize, ret.vec.sel, ret.vec.nulls, func_bind);
    retVector->computeValPtrs();
  }

  return params[0];
}

template <typename RetType, typename ParaType1, typename ParaType2,
          typename ParaType3, class FunctionProto>
inline Datum transform_ScalarVectorScalar(Datum *params, uint64_t size,
                                          FunctionProto func) {
  Vector *retVector = params[0];
  Scalar *para1Scalar = params[1];
  Vector *para2Vector = params[2];
  Scalar *para3Scalar = params[3];

  para2Vector->trim();
  ScalarRawData<ParaType1> para1(para1Scalar);
  VectorRawData<ParaType2> para2(para2Vector);
  ScalarRawData<ParaType3> para3(para3Scalar);

  if (para1Scalar->isnull || para3Scalar->isnull) {
    retVector->resize(para2.vec.plainSize, para2.vec.sel, true);
  } else {
    retVector->resize(para2.vec.plainSize, para2.vec.sel, para2.vec.nulls);
    VectorRawData<RetType> ret(retVector);

    auto func_bind = [&](uint64_t plainIdx) {
      ret.set(plainIdx, func(*retVector->getValueBuffer(), para1.get(),
                             para2.get(plainIdx), para3.get()));
    };

    transformVector(ret.vec.plainSize, ret.vec.sel, ret.vec.nulls, func_bind);
    retVector->computeValPtrs();
  }

  return params[0];
}

template <typename RetType, typename ParaType1, typename ParaType2,
          typename ParaType3, class FunctionProto>
inline Datum transform_ScalarScalarVector(Datum *params, uint64_t size,
                                          FunctionProto func) {
  Vector *retVector = params[0];
  Scalar *para1Scalar = params[1];
  Scalar *para2Scalar = params[2];
  Vector *para3Vector = params[3];

  para3Vector->trim();
  ScalarRawData<ParaType1> para1(para1Scalar);
  ScalarRawData<ParaType2> para2(para2Scalar);
  VectorRawData<ParaType3> para3(para3Vector);

  if (para1Scalar->isnull || para2Scalar->isnull) {
    retVector->resize(para3.vec.plainSize, para3.vec.sel, true);
  } else {
    retVector->resize(para3.vec.plainSize, para3.vec.sel, para3.vec.nulls);
    VectorRawData<RetType> ret(retVector);

    auto func_bind = [&](uint64_t plainIdx) {
      ret.set(plainIdx, func(*retVector->getValueBuffer(), para1.get(),
                             para2.get(), para3.get(plainIdx)));
    };

    transformVector(ret.vec.plainSize, ret.vec.sel, ret.vec.nulls, func_bind);
    retVector->computeValPtrs();
  }

  return params[0];
}

template <typename RetType, typename ParaType1, typename ParaType2,
          typename ParaType3, class FunctionProto>
inline Datum transform_ScalarScalarScalar(Datum *params, uint64_t size,
                                          FunctionProto func) {
  Scalar *retScalar = params[0];
  Scalar *para1Scalar = params[1];
  Scalar *para2Scalar = params[2];
  Scalar *para3Scalar = params[3];

  if (para1Scalar->isnull || para2Scalar->isnull || para3Scalar->isnull) {
    retScalar->isnull = true;
  } else {
    retScalar->isnull = false;
    ScalarRawData<RetType> ret(retScalar);
    ScalarRawData<ParaType1> para1(para1Scalar);
    ScalarRawData<ParaType2> para2(para2Scalar);
    ScalarRawData<ParaType3> para3(para3Scalar);

    retScalar->getValueBuffer()->clear();
    ret.set(func(*retScalar->getValueBuffer(), para1.get(), para2.get(),
                 para3.get()));
  }

  return params[0];
}

template <typename RetType, typename ParaType1, typename ParaType2,
          typename ParaType3, class FunctionProto>
Datum three_params_bind(Datum *params, uint64_t size, FunctionProto func) {
  Object *para1 = DatumGetValue<Object *>(params[1]);
  Object *para2 = DatumGetValue<Object *>(params[2]);
  Object *para3 = DatumGetValue<Object *>(params[3]);
  Vector *vec1 = dynamic_cast<Vector *>(para1);
  Vector *vec2 = dynamic_cast<Vector *>(para2);
  Vector *vec3 = dynamic_cast<Vector *>(para3);

  if (vec1) {
    if (vec2) {
      if (vec3) {
        return transform_VectorVectorVector<RetType, ParaType1, ParaType2,
                                            ParaType3>(params, size, func);
      } else {
        return transform_VectorVectorScalar<RetType, ParaType1, ParaType2,
                                            ParaType3>(params, size, func);
      }
    } else {
      if (vec3) {
        return transform_VectorScalarVector<RetType, ParaType1, ParaType2,
                                            ParaType3>(params, size, func);
      } else {
        return transform_VectorScalarScalar<RetType, ParaType1, ParaType2,
                                            ParaType3>(params, size, func);
      }
    }
  } else {
    if (vec2) {
      if (vec3) {
        return transform_ScalarVectorVector<RetType, ParaType1, ParaType2,
                                            ParaType3>(params, size, func);
      } else {
        return transform_ScalarVectorScalar<RetType, ParaType1, ParaType2,
                                            ParaType3>(params, size, func);
      }
    } else {
      if (vec3) {
        return transform_ScalarScalarVector<RetType, ParaType1, ParaType2,
                                            ParaType3>(params, size, func);
      } else {
        return transform_ScalarScalarScalar<RetType, ParaType1, ParaType2,
                                            ParaType3>(params, size, func);
      }
    }
  }
}

template <typename RetType, typename ParaType1, typename ParaType2,
          class FunctionProto>
inline Datum transform_VectorVector(Datum *params, uint64_t size,
                                    FunctionProto func) {
  Vector *retVector = params[0];
  Vector *para1Vector = params[1];
  Vector *para2Vector = params[2];

  para1Vector->trim();
  para2Vector->trim();
  VectorRawData<ParaType1> para1(para1Vector);
  VectorRawData<ParaType2> para2(para2Vector);
  retVector->resize(para1.vec.plainSize, para1.vec.sel, para1.vec.nulls,
                    para2.vec.nulls);
  VectorRawData<RetType> ret(retVector);

  auto func_bind = [&](uint64_t plainIdx) {
    ret.set(plainIdx, func(*retVector->getValueBuffer(), para1.get(plainIdx),
                           para2.get(plainIdx)));
  };

  transformVector(ret.vec.plainSize, ret.vec.sel, ret.vec.nulls, func_bind);
  retVector->computeValPtrs();

  return params[0];
}

template <typename RetType, typename ParaType1, typename ParaType2,
          class FunctionProto>
inline Datum transform_VectorScalar(Datum *params, uint64_t size,
                                    FunctionProto func) {
  Vector *retVector = params[0];
  Vector *para1Vector = params[1];
  Scalar *para2Scalar = params[2];

  para1Vector->trim();
  VectorRawData<ParaType1> para1(para1Vector);
  ScalarRawData<ParaType2> para2(para2Scalar);

  if (para2Scalar->isnull) {
    retVector->resize(para1.vec.plainSize, para1.vec.sel, true);
  } else {
    retVector->resize(para1.vec.plainSize, para1.vec.sel, para1.vec.nulls);
    VectorRawData<RetType> ret(retVector);

    auto func_bind = [&](uint64_t plainIdx) {
      ret.set(plainIdx, func(*retVector->getValueBuffer(), para1.get(plainIdx),
                             para2.get()));
    };

    transformVector(ret.vec.plainSize, ret.vec.sel, ret.vec.nulls, func_bind);
    retVector->computeValPtrs();
  }
  return params[0];
}

template <typename RetType, typename ParaType1, typename ParaType2,
          class FunctionProto>
inline Datum transform_ScalarVector(Datum *params, uint64_t size,
                                    FunctionProto func) {
  Vector *retVector = params[0];
  Scalar *para1Scalar = params[1];
  Vector *para2Vector = params[2];

  para2Vector->trim();
  ScalarRawData<ParaType1> para1(para1Scalar);
  VectorRawData<ParaType2> para2(para2Vector);

  if (para1Scalar->isnull) {
    retVector->resize(para2.vec.plainSize, para2.vec.sel, true);
  } else {
    retVector->resize(para2.vec.plainSize, para2.vec.sel, para2.vec.nulls);
    VectorRawData<RetType> ret(retVector);

    auto func_bind = [&](uint64_t plainIdx) {
      ret.set(plainIdx, func(*retVector->getValueBuffer(), para1.get(),
                             para2.get(plainIdx)));
    };

    transformVector(ret.vec.plainSize, ret.vec.sel, ret.vec.nulls, func_bind);
    retVector->computeValPtrs();
  }
  return params[0];
}

template <typename RetType, typename ParaType1, typename ParaType2,
          class FunctionProto>
inline Datum transform_ScalarScalar(Datum *params, uint64_t size,
                                    FunctionProto func) {
  Scalar *retScalar = params[0];
  Scalar *para1Scalar = params[1];
  Scalar *para2Scalar = params[2];

  if (para1Scalar->isnull || para2Scalar->isnull) {
    retScalar->isnull = true;
  } else {
    retScalar->isnull = false;

    ScalarRawData<RetType> ret(retScalar);
    ScalarRawData<ParaType1> para1(para1Scalar);
    ScalarRawData<ParaType2> para2(para2Scalar);

    retScalar->getValueBuffer()->clear();
    ret.set(func(*retScalar->getValueBuffer(), para1.get(), para2.get()));
  }
  return params[0];
}

template <typename RetType, typename ParaType1, typename ParaType2,
          class FunctionProto>
Datum two_params_bind(Datum *params, uint64_t size, FunctionProto func) {
  Object *paraL = DatumGetValue<Object *>(params[1]);
  Vector *vecL = dynamic_cast<Vector *>(paraL);
  Object *paraR = DatumGetValue<Object *>(params[2]);
  Vector *vecR = dynamic_cast<Vector *>(paraR);
  if (vecL) {
    if (vecR) {
      return transform_VectorVector<RetType, ParaType1, ParaType2>(params, size,
                                                                   func);
    } else {
      return transform_VectorScalar<RetType, ParaType1, ParaType2>(params, size,
                                                                   func);
    }
  } else {
    if (vecR) {
      return transform_ScalarVector<RetType, ParaType1, ParaType2>(params, size,
                                                                   func);
    } else {
      return transform_ScalarScalar<RetType, ParaType1, ParaType2>(params, size,
                                                                   func);
    }
  }
}

template <typename RetType, typename ParaType, class FunctionProto>
inline Datum transform_Vector(Datum *params, uint64_t size,
                              FunctionProto func) {
  Vector *retVector = params[0];
  Vector *paraVector = params[1];

  paraVector->trim();
  VectorRawData<ParaType> para(paraVector);
  retVector->resize(para.vec.plainSize, para.vec.sel, para.vec.nulls);
  VectorRawData<RetType> ret(retVector);

  auto func_bind = [&](uint64_t plainIdx) {
    ret.set(plainIdx, func(*retVector->getValueBuffer(), para.get(plainIdx)));
  };

  transformVector(ret.vec.plainSize, ret.vec.sel, ret.vec.nulls, func_bind);
  retVector->computeValPtrs();

  return params[0];
}

template <typename RetType, typename ParaType, class FunctionProto>
inline Datum transform_Scalar(Datum *params, uint64_t size,
                              FunctionProto func) {
  Scalar *retScalar = params[0];
  Scalar *paraScalar = params[1];

  if (paraScalar->isnull) {
    retScalar->isnull = true;
  } else {
    retScalar->isnull = false;

    ScalarRawData<ParaType> para(paraScalar);
    ScalarRawData<RetType> ret(retScalar);

    retScalar->getValueBuffer()->clear();
    ret.set(func(*retScalar->getValueBuffer(), para.get()));
  }

  return params[0];
}

template <typename RetType, typename ParaType, class FunctionProto>
Datum one_param_bind(Datum *params, uint64_t size, FunctionProto func) {
  Object *para = DatumGetValue<Object *>(params[1]);
  if (dynamic_cast<Vector *>(para)) {
    return transform_Vector<RetType, ParaType>(params, size, func);
  } else {
    return transform_Scalar<RetType, ParaType>(params, size, func);
  }
}

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_FUNCTION_FUNCTION_H_
