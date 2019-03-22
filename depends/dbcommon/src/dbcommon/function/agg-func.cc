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

#include "dbcommon/function/agg-func.h"

#include <float.h>
#include <algorithm>
#include <cassert>
#include <string>
#include "dbcommon/common/tuple-batch.h"
#include "dbcommon/common/vector/decimal-vector.h"
#include "dbcommon/function/decimal-function.h"
#include "dbcommon/function/invoker.h"
#include "dbcommon/log/debug-logger.h"
#include "dbcommon/utils/macro.h"

namespace dbcommon {
/*
 * 1. Handle agg(Scalar)
 * 2. Handle optimization for statistics
 * 3. Handle !hasGroupBy. When there is !hasGroupBy, the size of grpVals is 1
 *    and hasGroups[i] is 0
 * 4. Handle small scale and large scale while getAccessor is always correct but
 *    getQuickAccessor is faster and correct only for small scale
 * 5. Handle hasNull and !hasNull
 * 6. Handle select list and no select list
 * 7. It has been experimented that select with bit arithmetic performs better
 *    than hand written CMOV instruction.
 * 8. The second stage SUM() is identical to the first stage.
 * 9. The second stage AVG() differs from the first stage.
 */

AggStringGroupValues::~AggStringGroupValues() {
  if (isSmallScale()) {
    auto accessor = getAccessor<QuickAccessor>();
    auto size = data_.size();
    for (uint64_t i = 0; i < size; i++) {
      auto tmp = (accessor.at(i)->accVal.value);
      if (accessor.at(i)->accVal.isNotNull) cnfree(tmp);
    }
    return;
  }
  auto accessor = getAccessor<Accessor>();
  auto size = data_.size();
  for (uint64_t i = 0; i < size; i++) {
    auto tmp = (accessor.at(i)->accVal.value);
    if (accessor.at(i)->accVal.isNotNull) cnfree(tmp);
  }
}

template <typename T, std::size_t = sizeof(T)>
struct Selector;
template <typename T>
struct Selector<T, sizeof(uint64_t)> {
  typedef uint64_t type;
};
template <typename T>
struct Selector<T, sizeof(uint32_t)> {
  typedef uint32_t type;
};
template <typename T>
struct Selector<T, sizeof(uint16_t)> {
  typedef uint16_t type;
};
template <typename T>
struct Selector<T, sizeof(uint8_t)> {
  typedef uint8_t type;
};

template <typename Accessor>
Datum count_star_impl(Datum *params, uint64_t size) {
  assert(size == 4);
  AggPrimitiveGroupValues &grpVals =
      *DatumGetValue<AggPrimitiveGroupValues *>(params[0]);
  uint64_t sz = DatumGetValue<const std::vector<uint64_t> *>(params[2])->size();
  const uint64_t *__restrict__ hashGroups =
      DatumGetValue<const std::vector<uint64_t> *>(params[2])->data();
  bool hasGroupBy = DatumGetValue<bool>(params[3]);
  Vector *vec = DatumGetValue<Vector *>(params[4]);

  Accessor accessor = grpVals.getAccessor<Accessor>();

  if (!hasGroupBy) {
    assert(grpVals.size() == 1);
    Datum &val = accessor.at(0)->accVal.value;
    DatumPlus(val, sz);

    return CreateDatum(0);
  }

#pragma clang loop unroll_count(4)
  for (uint64_t i = 0; i < sz; i++) {
    Datum &val = accessor.at(hashGroups[i])->accVal.value;
    DatumPlus(val, (uint64_t)1);
  }
  return CreateDatum(0);
}
Datum count_star(Datum *params, uint64_t size) {
  AggPrimitiveGroupValues *grpVals =
      DatumGetValue<AggPrimitiveGroupValues *>(params[0]);
  return grpVals->isSmallScale()
             ? count_star_impl<AggPrimitiveGroupValues::QuickAccessor>(params,
                                                                       size)
             : count_star_impl<AggPrimitiveGroupValues::Accessor>(params, size);
}

template <typename Accessor>
Datum count_inc_impl(Datum *params, uint64_t size) {
  assert(size == 5);

  Object *para = DatumGetValue<Object *>(params[4]);
  if (dynamic_cast<Scalar *>(para)) {
    return count_star(params, 4);
  }
  AggPrimitiveGroupValues &grpVals =
      *DatumGetValue<AggPrimitiveGroupValues *>(params[0]);
  Vector *vec = DatumGetValue<Vector *>(params[4]);

  Accessor accessor = grpVals.getAccessor<Accessor>();

  // optimize based on vector statistics
  const VectorStatistics *stats = vec->getVectorStatistics();
  if (stats) {
    Datum &val = accessor.at(0)->accVal.value;
    DatumPlus(val, stats->valueCount);
    return CreateDatum(0);
  }

  uint64_t sz = vec->getNumOfRows();
  const uint64_t *__restrict__ hashGroups =
      DatumGetValue<const std::vector<uint64_t> *>(params[2])->data();
  bool hasGroupBy = DatumGetValue<bool>(params[3]);

  bool hasNull = vec->hasNullValue();
  SelectList *sel = vec->getSelected();

  assert(sz == DatumGetValue<const std::vector<uint64_t> *>(params[2])->size());
  assert(vec->isValid() && "bug");

  if (!hasGroupBy) {
    assert(grpVals.size() == 1);
    Datum &val = accessor.at(0)->accVal.value;
    uint64_t inc = sz;
    if (hasNull) {
      dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
      if (sel) {
#pragma clang loop unroll_count(4)
        for (int64_t i = 0; i < sz; ++i) {
          uint64_t isNull = nulls->getChar((*sel)[i]);  // 0x1 or 0x0
          inc -= isNull;
        }
      } else {
#pragma clang loop unroll_count(4)
        for (int64_t i = 0; i < sz; ++i) {
          uint64_t isNull = nulls->getChar(i);  // 0x1 or 0x0
          inc -= isNull;
        }
      }
    }
    DatumPlus(val, inc);

    return CreateDatum(0);
  }

  if (sel) {
    if (hasNull) {
      dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        Datum &val = accessor.at(hashGroups[i])->accVal.value;
        uint64_t index = (*sel)[i];
        uint64_t isNotNull = nulls->getChar(index) ^ 0x1;  // 0x1 or 0x0
        DatumPlus(val, isNotNull);
      }
    } else {
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        Datum &val = accessor.at(hashGroups[i])->accVal.value;
        DatumPlus(val, (uint64_t)1);
      }
    }
  } else {
    if (hasNull) {
      dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        Datum &val = accessor.at(hashGroups[i])->accVal.value;
        uint64_t isNotNull = nulls->getChar(i) ^ 0x1;  // 0x1 or 0x0
        DatumPlus(val, isNotNull);
      }
    } else {
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        Datum &val = accessor.at(hashGroups[i])->accVal.value;
        DatumPlus(val, (uint64_t)1);
      }
    }
  }
  return CreateDatum(0);
}
Datum count_inc(Datum *params, uint64_t size) {
  AggPrimitiveGroupValues *grpVals =
      DatumGetValue<AggPrimitiveGroupValues *>(params[0]);
  return grpVals->isSmallScale()
             ? count_inc_impl<AggPrimitiveGroupValues::QuickAccessor>(params,
                                                                      size)
             : count_inc_impl<AggPrimitiveGroupValues::Accessor>(params, size);
}

// todo: check the count_add with null
template <typename Accessor>
Datum count_add_impl(Datum *params, uint64_t size) {
  assert(size == 5);
  AggPrimitiveGroupValues &grpVals =
      *DatumGetValue<AggPrimitiveGroupValues *>(params[0]);
  uint64_t sz = DatumGetValue<const std::vector<uint64_t> *>(params[2])->size();
  const uint64_t *__restrict__ hashGroups =
      DatumGetValue<const std::vector<uint64_t> *>(params[2])->data();
  bool hasGroupBy = DatumGetValue<bool>(params[3]);
  Vector *vec = DatumGetValue<Vector *>(params[4]);
  SelectList *sel = vec->getSelected();
  const int64_t *v = reinterpret_cast<const int64_t *>(vec->getValue());

  assert(vec && "invalid input");
  assert(sz == DatumGetValue<const std::vector<uint64_t> *>(params[2])->size());

  Accessor accessor = grpVals.getAccessor<Accessor>();

  if (!hasGroupBy) {
    assert(grpVals.size() == 1);
    Datum &val = accessor.at(0)->accVal.value;

    if (sel) {
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel)[i];
        DatumPlus(val, v[index]);
      }
    } else {
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        DatumPlus(val, v[i]);
      }
    }

    return CreateDatum(0);
  }

  if (sel) {
#pragma clang loop unroll_count(4)
    for (uint64_t i = 0; i < sz; ++i) {
      Datum &val = accessor.at(hashGroups[i])->accVal.value;
      uint64_t index = (*sel)[i];
      DatumPlus(val, v[index]);
    }
  } else {
#pragma clang loop unroll_count(4)
    for (uint64_t i = 0; i < sz; ++i) {
      Datum &val = accessor.at(hashGroups[i])->accVal.value;
      DatumPlus(val, v[i]);
    }
  }
  return CreateDatum(0);
}
Datum count_add(Datum *params, uint64_t size) {
  AggPrimitiveGroupValues *grpVals =
      DatumGetValue<AggPrimitiveGroupValues *>(params[0]);
  return grpVals->isSmallScale()
             ? count_add_impl<AggPrimitiveGroupValues::QuickAccessor>(params,
                                                                      size)
             : count_add_impl<AggPrimitiveGroupValues::Accessor>(params, size);
}

template <typename T, typename R, typename Accessor>
Datum sum_type_sum_impl(Datum *params, uint64_t size) {
  assert(size == 5);

  AggPrimitiveGroupValues &grpVals =
      *DatumGetValue<AggPrimitiveGroupValues *>(params[0]);
  uint64_t sz = DatumGetValue<const std::vector<uint64_t> *>(params[2])->size();
  const uint64_t *__restrict__ hashGroups =
      DatumGetValue<const std::vector<uint64_t> *>(params[2])->data();
  bool hasGroupBy = DatumGetValue<bool>(params[3]);
  Object *para = DatumGetValue<Object *>(params[4]);
  Vector *vec = dynamic_cast<Vector *>(para);

  Accessor accessor = grpVals.getAccessor<Accessor>();

  if (!vec) {
    Scalar *scalar = dynamic_cast<Scalar *>(para);

    if (!hasGroupBy) {
      auto aggVal = accessor.at(0);
      Datum &val = aggVal->accVal.value;
      aggVal->accVal.isNotNull = true;
      R value = DatumGetValue<R>(val);
      value += DatumGetValue<T>(scalar->value) * sz;
      val = CreateDatum(value);

      return CreateDatum(0);
    }

#pragma clang loop unroll_count(4)
    for (uint64_t i = 0; i < sz; ++i) {
      auto aggVal = accessor.at(hashGroups[i]);
      Datum &val = aggVal->accVal.value;
      aggVal->accVal.isNotNull = true;
      R value = DatumGetValue<R>(val);
      value += DatumGetValue<T>(scalar->value);
      val = CreateDatum(value);
    }

    return CreateDatum(0);
  }

  // optimize based on vector statistics
  const VectorStatistics *stats = vec->getVectorStatistics();
  if (stats) {
    if (!stats->hasMinMaxStats) return CreateDatum(0);
    auto aggVal = accessor.at(0);
    Datum &val = aggVal->accVal.value;
    if (!aggVal->accVal.isNotNull) {
      aggVal->accVal.isNotNull = true;
      val = stats->sum;
    } else {
      DatumPlus<R>(val, DatumGetValue<R>(stats->sum));
    }
    return CreateDatum(0);
  }

  bool hasNull = vec->hasNullValue();
  SelectList *sel = vec->getSelected();
  assert(sz == DatumGetValue<const std::vector<uint64_t> *>(params[2])->size());
  const T *v = reinterpret_cast<const T *>(vec->getValue());

  if (!hasGroupBy) {
    assert(grpVals.size() == 1);
    auto aggVal = accessor.at(0);

    if (sel) {
      if (hasNull) {
        dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          uint64_t index = (*sel)[i];
          Datum &val = aggVal->accVal.value;

          char isNull = nulls->getChar(index);
          char isNotNull = isNull ^ 0x1;  // 0x1 or 0x0
          aggVal->accVal.isNotNull |= isNotNull;
          R acc = v[index];
          typename Selector<R>::type accSrc =
              *reinterpret_cast<typename Selector<R>::type *>(&acc);
          accSrc &= (~(uint64_t)isNotNull + 1);
          acc = *reinterpret_cast<R *>(&accSrc);

          DatumPlus<R>(val, acc);
        }
      } else {
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          uint64_t index = (*sel)[i];
          Datum &val = aggVal->accVal.value;
          aggVal->accVal.isNotNull = true;
          DatumPlus<R>(val, v[index]);
        }
      }
    } else {
      if (hasNull) {
        dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          Datum &val = aggVal->accVal.value;

          char isNull = nulls->getChar(i);
          char isNotNull = isNull ^ 0x1;  // 0x1 or 0x0
          aggVal->accVal.isNotNull |= isNotNull;
          R acc = v[i];
          typename Selector<R>::type accSrc =
              *reinterpret_cast<typename Selector<R>::type *>(&acc);
          accSrc &= (~(uint64_t)isNotNull + 1);
          acc = *reinterpret_cast<R *>(&accSrc);

          DatumPlus<R>(val, acc);
        }
      } else {
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          Datum &val = aggVal->accVal.value;
          aggVal->accVal.isNotNull = true;
          DatumPlus<R>(val, v[i]);
        }
      }
    }

    return CreateDatum(0);
  }

  if (sel) {
    if (hasNull) {
      dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel)[i];
        auto aggVal = accessor.at(hashGroups[i]);
        Datum &val = aggVal->accVal.value;

        char isNull = nulls->getChar(index);
        char isNotNull = isNull ^ 0x1;  // 0x1 or 0x0
        aggVal->accVal.isNotNull |= isNotNull;
        R acc = v[index];
        typename Selector<R>::type accSrc =
            *reinterpret_cast<typename Selector<R>::type *>(&acc);
        accSrc &= (~(uint64_t)isNotNull + 1);
        acc = *reinterpret_cast<R *>(&accSrc);

        DatumPlus<R>(val, acc);
      }
    } else {
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel)[i];
        auto aggVal = accessor.at(hashGroups[i]);
        Datum &val = aggVal->accVal.value;
        aggVal->accVal.isNotNull = true;
        DatumPlus<R>(val, v[index]);
      }
    }
  } else {
    if (hasNull) {
      dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        auto aggVal = accessor.at(hashGroups[i]);
        Datum &val = aggVal->accVal.value;

        char isNull = nulls->getChar(i);
        char isNotNull = isNull ^ 0x1;  // 0x1 or 0x0
        aggVal->accVal.isNotNull |= isNotNull;
        R acc = v[i];
        typename Selector<R>::type accSrc =
            *reinterpret_cast<typename Selector<R>::type *>(&acc);
        accSrc &= (~(uint64_t)isNotNull + 1);
        acc = *reinterpret_cast<R *>(&accSrc);

        DatumPlus<R>(val, acc);
      }
    } else {
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        auto aggVal = accessor.at(hashGroups[i]);
        Datum &val = aggVal->accVal.value;
        aggVal->accVal.isNotNull = true;
        DatumPlus<R>(val, v[i]);
      }
    }
  }

  return CreateDatum(0);
}
template <typename T, typename R>
Datum sum_type_sum(Datum *params, uint64_t size) {
  AggPrimitiveGroupValues *grpVals =
      DatumGetValue<AggPrimitiveGroupValues *>(params[0]);
  return grpVals->isSmallScale()
             ? sum_type_sum_impl<T, R, AggPrimitiveGroupValues::QuickAccessor>(
                   params, size)
             : sum_type_sum_impl<T, R, AggPrimitiveGroupValues::Accessor>(
                   params, size);
}

template <typename Accessor>
Datum sum_decimal_sum_impl(Datum *params, uint64_t size) {
  assert(size == 5);

  AggDecimalGroupValues &grpVals =
      *DatumGetValue<AggDecimalGroupValues *>(params[0]);
  uint64_t sz = DatumGetValue<const std::vector<uint64_t> *>(params[2])->size();
  const uint64_t *__restrict__ hashGroups =
      DatumGetValue<const std::vector<uint64_t> *>(params[2])->data();
  bool hasGroupBy = DatumGetValue<bool>(params[3]);
  Object *para = DatumGetValue<Object *>(params[4]);
  Vector *vec = dynamic_cast<Vector *>(para);

  Accessor accessor = grpVals.getAccessor<Accessor>();

  // Aggregate scalar
  if (!vec) {
    Scalar *scalar = dynamic_cast<Scalar *>(para);
    DecimalVar val = *DatumGetValue<DecimalVar *>(scalar->value);
    if (!hasGroupBy) {
      auto aggVal = accessor.at(0);
      val *= sz;
      aggVal->accVal.value += val;
      aggVal->accVal.isNotNull = true;
      return CreateDatum(0);
    }

    for (uint64_t i = 0; i < sz; ++i) {
      auto aggVal = accessor.at(hashGroups[i]);
      aggVal->accVal.value += val;
      aggVal->accVal.isNotNull = true;
    }
    return CreateDatum(0);
  }

  // optimize based on vector statistics
  const VectorStatistics *stats = vec->getVectorStatistics();
  if (stats && !hasGroupBy) {
    if (!stats->hasMinMaxStats) return CreateDatum(0);
    auto aggVal = accessor.at(0);
    char *sum = DatumGetValue<char *>(stats->sum);
    DecimalVar decVal = DecimalType::fromString(std::string(sum, strlen(sum)));
    aggVal->accVal.value += decVal;
    aggVal->accVal.isNotNull = true;
    return CreateDatum(0);
  }

  bool hasNull = vec->hasNullValue();
  SelectList *sel = vec->getSelected();
  assert(sz == DatumGetValue<const std::vector<uint64_t> *>(params[2])->size());
  DecimalVector *dvec = dynamic_cast<DecimalVector *>(vec);
  const int64_t *hval =
      reinterpret_cast<const int64_t *>(dvec->getAuxiliaryValue());
  const uint64_t *lval = reinterpret_cast<const uint64_t *>(dvec->getValue());
  const int64_t *sval =
      reinterpret_cast<const int64_t *>(dvec->getScaleValue());

  if (!hasGroupBy) {
    assert(grpVals.size() == 1);
    auto aggVal = accessor.at(0);
    if (sel) {
      if (hasNull) {
        auto nulls = vec->getNulls();
        for (uint64_t actualIdx = 0; actualIdx < sz; ++actualIdx) {
          uint64_t plainIdx = (*sel)[actualIdx];
          if (!nulls[plainIdx]) {
            aggVal->accVal.value +=
                DecimalVar(hval[plainIdx], lval[plainIdx], sval[plainIdx]);
            aggVal->accVal.isNotNull = true;
          }
        }
      } else {
        for (uint64_t actualIdx = 0; actualIdx < sz; ++actualIdx) {
          uint64_t plainIdx = (*sel)[actualIdx];
          aggVal->accVal.value +=
              DecimalVar(hval[plainIdx], lval[plainIdx], sval[plainIdx]);
          aggVal->accVal.isNotNull = true;
        }
      }
    } else {
      if (hasNull) {
        auto nulls = vec->getNulls();
        for (uint64_t plainIdx = 0; plainIdx < sz; ++plainIdx) {
          if (!nulls[plainIdx]) {
            aggVal->accVal.value +=
                DecimalVar(hval[plainIdx], lval[plainIdx], sval[plainIdx]);
            aggVal->accVal.isNotNull = true;
          }
        }
      } else {
        for (uint64_t plainIdx = 0; plainIdx < sz; ++plainIdx) {
          aggVal->accVal.value +=
              DecimalVar(hval[plainIdx], lval[plainIdx], sval[plainIdx]);
          aggVal->accVal.isNotNull = true;
        }
      }
    }

    return CreateDatum(0);
  }

  // Aggregate with group by
  if (sel) {
    if (hasNull) {
      auto nulls = vec->getNulls();
      for (uint64_t actualIdx = 0; actualIdx < sz; ++actualIdx) {
        uint64_t plainIdx = (*sel)[actualIdx];
        if (!nulls[plainIdx]) {
          auto aggVal = accessor.at(hashGroups[actualIdx]);
          aggVal->accVal.value +=
              DecimalVar(hval[plainIdx], lval[plainIdx], sval[plainIdx]);
          aggVal->accVal.isNotNull = true;
        }
      }
    } else {
      for (uint64_t actualIdx = 0; actualIdx < sz; ++actualIdx) {
        uint64_t plainIdx = (*sel)[actualIdx];
        auto aggVal = accessor.at(hashGroups[actualIdx]);
        aggVal->accVal.value +=
            DecimalVar(hval[plainIdx], lval[plainIdx], sval[plainIdx]);
        aggVal->accVal.isNotNull = true;
      }
    }
  } else {
    if (hasNull) {
      auto nulls = vec->getNulls();
      for (uint64_t plainIdx = 0; plainIdx < sz; ++plainIdx) {
        if (!nulls[plainIdx]) {
          auto aggVal = accessor.at(hashGroups[plainIdx]);
          aggVal->accVal.value +=
              DecimalVar(hval[plainIdx], lval[plainIdx], sval[plainIdx]);
          aggVal->accVal.isNotNull = true;
        }
      }
    } else {
      for (uint64_t plainIdx = 0; plainIdx < sz; ++plainIdx) {
        auto aggVal = accessor.at(hashGroups[plainIdx]);
        aggVal->accVal.value +=
            DecimalVar(hval[plainIdx], lval[plainIdx], sval[plainIdx]);
        aggVal->accVal.isNotNull = true;
      }
    }
  }

  return CreateDatum(0);
}
Datum sum_decimal_sum(Datum *params, uint64_t size) {
  auto grpVals = DatumGetValue<AggPrimitiveGroupValues *>(params[0]);
  return grpVals->isSmallScale()
             ? sum_decimal_sum_impl<AggDecimalGroupValues::QuickAccessor>(
                   params, size)
             : sum_decimal_sum_impl<AggDecimalGroupValues::Accessor>(params,
                                                                     size);
}

template <typename T, typename R, typename Accessor>
Datum avg_type_accu_impl(Datum *params, uint64_t size) {
  assert(size == 5);

  AggPrimitiveGroupValues &grpVals =
      *DatumGetValue<AggPrimitiveGroupValues *>(params[0]);
  uint64_t sz = DatumGetValue<const std::vector<uint64_t> *>(params[2])->size();
  const uint64_t *__restrict__ hashGroups =
      DatumGetValue<const std::vector<uint64_t> *>(params[2])->data();
  bool hasGroupBy = DatumGetValue<bool>(params[3]);
  Object *para = DatumGetValue<Object *>(params[4]);
  Vector *vec = dynamic_cast<Vector *>(para);

  Accessor accessor = grpVals.getAccessor<Accessor>();

  if (!vec) {
    Scalar *scalar = reinterpret_cast<Scalar *>(para);
    if (!hasGroupBy) {
      auto aggVal = accessor.at(0);
      aggVal->avgVal.sum +=
          static_cast<double>(DatumGetValue<T>(scalar->value)) * sz;
      aggVal->avgVal.count += sz;

      return CreateDatum(0);
    }

#pragma clang loop unroll_count(4)
    for (uint64_t i = 0; i < sz; ++i) {
      auto aggVal = accessor.at(hashGroups[i]);
      aggVal->avgVal.sum +=
          static_cast<double>(DatumGetValue<T>(scalar->value));
      aggVal->avgVal.count += 1;
    }

    return CreateDatum(0);
  }

  // optimize based on vector statistics
  const VectorStatistics *stats = vec->getVectorStatistics();
  if (stats) {
    if (!stats->hasMinMaxStats) return CreateDatum(0);
    auto aggVal = accessor.at(0);
    aggVal->avgVal.sum += static_cast<double>(DatumGetValue<R>(stats->sum));
    aggVal->avgVal.count += stats->valueCount;
    return CreateDatum(0);
  }

  bool hasNull = vec->hasNullValue();
  SelectList *sel = vec->getSelected();
  assert(sz == DatumGetValue<const std::vector<uint64_t> *>(params[2])->size());

  Vector *valVec = vec;

  if (!hasGroupBy) {
    assert(grpVals.size() == 1);
    auto aggVal = accessor.at(0);

    const T *v = reinterpret_cast<const T *>(valVec->getValue());
    if (sel) {
      if (hasNull) {
        dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          uint64_t index = (*sel)[i];
          auto val = &(aggVal->avgVal);

          char isNotNull = nulls->getChar(index) ^ 0x1;  // 0x1 or 0x0
          double acc = v[index];
          uint64_t accSrc = *reinterpret_cast<uint64_t *>(&acc);
          accSrc &= (~(uint64_t)isNotNull + 1);
          acc = *reinterpret_cast<double *>(&accSrc);

          val->sum += acc;
          val->count += isNotNull;
        }
      } else {
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          uint64_t index = (*sel)[i];
          auto val = &(aggVal->avgVal);
          val->sum += v[index];
          ++val->count;
        }
      }
    } else {
      if (hasNull) {
        dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          auto val = &(aggVal->avgVal);

          char isNotNull = nulls->getChar(i) ^ 0x1;  // 0x1 or 0x0
          double acc = v[i];
          uint64_t accSrc = *reinterpret_cast<uint64_t *>(&acc);
          accSrc &= (~(uint64_t)isNotNull + 1);
          acc = *reinterpret_cast<double *>(&accSrc);

          val->sum += acc;
          val->count += isNotNull;
        }
      } else {
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          auto val = &(aggVal->avgVal);
          val->sum += v[i];
          ++val->count;
        }
      }
    }

    return CreateDatum(0);
  }

  const T *v = reinterpret_cast<const T *>(valVec->getValue());
  if (sel) {
    if (hasNull) {
      dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel)[i];
        auto aggVal = accessor.at(hashGroups[i]);
        auto val = &(aggVal->avgVal);

        char isNotNull = nulls->getChar(index) ^ 0x1;  // 0x1 or 0x0
        double acc = v[index];
        uint64_t accSrc = *reinterpret_cast<uint64_t *>(&acc);
        accSrc &= (~(uint64_t)isNotNull + 1);
        acc = *reinterpret_cast<double *>(&accSrc);

        val->sum += acc;
        val->count += isNotNull;
      }
    } else {
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel)[i];
        auto aggVal = accessor.at(hashGroups[i]);
        auto val = &(aggVal->avgVal);
        val->sum += v[index];
        ++val->count;
      }
    }
  } else {
    if (hasNull) {
      dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        auto aggVal = accessor.at(hashGroups[i]);
        auto val = &(aggVal->avgVal);

        char isNotNull = nulls->getChar(i) ^ 0x1;  // 0x1 or 0x0
        double acc = v[i];
        uint64_t accSrc = *reinterpret_cast<uint64_t *>(&acc);
        accSrc &= (~(uint64_t)isNotNull + 1);
        acc = *reinterpret_cast<double *>(&accSrc);

        val->sum += acc;
        val->count += isNotNull;
      }
    } else {
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        auto aggVal = accessor.at(hashGroups[i]);
        auto val = &(aggVal->avgVal);
        val->sum += v[i];
        ++val->count;
      }
    }
  }

  return CreateDatum(0);
}
template <typename T, typename R>
Datum avg_type_accu(Datum *params, uint64_t size) {
  AggPrimitiveGroupValues *grpVals =
      DatumGetValue<AggPrimitiveGroupValues *>(params[0]);
  return grpVals->isSmallScale()
             ? avg_type_accu_impl<T, R, AggPrimitiveGroupValues::QuickAccessor>(
                   params, size)
             : avg_type_accu_impl<T, R, AggPrimitiveGroupValues::Accessor>(
                   params, size);
}

template <typename Accessor>
Datum avg_type_amalg_impl(Datum *params, uint64_t size) {
  assert(size == 5);

  AggPrimitiveGroupValues &grpVals =
      *DatumGetValue<AggPrimitiveGroupValues *>(params[0]);
  uint64_t sz = DatumGetValue<const std::vector<uint64_t> *>(params[2])->size();
  const uint64_t *__restrict__ hashGroups =
      DatumGetValue<const std::vector<uint64_t> *>(params[2])->data();
  bool hasGroupBy = DatumGetValue<bool>(params[3]);
  Object *para = DatumGetValue<Object *>(params[4]);

  Vector *vec = reinterpret_cast<Vector *>(para);
  assert(dynamic_cast<Vector *>(para)->getTypeKind() == TypeKind::STRUCTID);

  Accessor accessor = grpVals.getAccessor<Accessor>();

  bool hasNull = vec->hasNullValue();
  SelectList *sel = vec->getSelected();
  assert(sz == DatumGetValue<const std::vector<uint64_t> *>(params[2])->size());

  Vector *valVec = vec->getChildVector(0);
  Vector *countVec = vec->getChildVector(1);

  if (!hasGroupBy) {
    assert(grpVals.size() == 1);
    auto aggVal = accessor.at(0);

    const double *v = reinterpret_cast<const double *>(valVec->getValue());
    const int64_t *countv =
        reinterpret_cast<const int64_t *>(countVec->getValue());
    if (sel) {
      if (valVec->hasNullValue()) {
        dbcommon::BoolBuffer *nulls = valVec->getNullBuffer();
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          uint64_t index = (*sel)[i];
          if (!nulls->get(index)) {
            auto val = &(aggVal->avgVal);
            val->count += countv[index];
            val->sum += v[index];
          }
        }
      } else {
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          uint64_t index = (*sel)[i];
          auto val = &(aggVal->avgVal);
          val->count += countv[index];
          val->sum += v[index];
        }
      }
    } else {
      if (valVec->hasNullValue()) {
        dbcommon::BoolBuffer *nulls = valVec->getNullBuffer();
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          if (!nulls->get(i)) {
            auto val = &(aggVal->avgVal);
            val->count += countv[i];
            val->sum += v[i];
          }
        }
      } else {
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          auto val = &(aggVal->avgVal);
          val->count += countv[i];
          val->sum += v[i];
        }
      }
    }

    return CreateDatum(0);
  }

  const double *v = reinterpret_cast<const double *>(valVec->getValue());
  const int64_t *countv =
      reinterpret_cast<const int64_t *>(countVec->getValue());
  if (sel) {
    if (valVec->hasNullValue()) {
      dbcommon::BoolBuffer *nulls = valVec->getNullBuffer();
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel)[i];
        if (!nulls->get(index)) {
          auto aggVal = accessor.at(hashGroups[i]);
          auto val = &(aggVal->avgVal);
          val->count += countv[index];
          val->sum += v[index];
        }
      }
    } else {
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel)[i];
        auto aggVal = accessor.at(hashGroups[i]);
        auto val = &(aggVal->avgVal);
        val->count += countv[index];
        val->sum += v[index];
      }
    }
  } else {
    if (valVec->hasNullValue()) {
      dbcommon::BoolBuffer *nulls = valVec->getNullBuffer();
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        if (!nulls->get(i)) {
          auto aggVal = accessor.at(hashGroups[i]);
          auto val = &(aggVal->avgVal);
          val->count += countv[i];
          val->sum += v[i];
        }
      }
    } else {
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        auto aggVal = accessor.at(hashGroups[i]);
        auto val = &(aggVal->avgVal);
        val->count += countv[i];
        val->sum += v[i];
      }
    }
  }

  return CreateDatum(0);
}
Datum avg_type_amalg(Datum *params, uint64_t size) {
  AggPrimitiveGroupValues *grpVals =
      DatumGetValue<AggPrimitiveGroupValues *>(params[0]);
  return grpVals->isSmallScale()
             ? avg_type_amalg_impl<AggPrimitiveGroupValues::QuickAccessor>(
                   params, size)
             : avg_type_amalg_impl<AggPrimitiveGroupValues::Accessor>(params,
                                                                      size);
}

template <typename Accessor>
Datum avg_decimal_accu_impl(Datum *params, uint64_t size) {
  assert(size == 5);

  AggDecimalGroupValues &grpVals =
      *DatumGetValue<AggDecimalGroupValues *>(params[0]);
  uint64_t sz = DatumGetValue<const std::vector<uint64_t> *>(params[2])->size();
  const uint64_t *__restrict__ hashGroups =
      DatumGetValue<const std::vector<uint64_t> *>(params[2])->data();
  bool hasGroupBy = DatumGetValue<bool>(params[3]);
  Object *para = DatumGetValue<Object *>(params[4]);
  Vector *vec = dynamic_cast<Vector *>(para);

  Accessor accessor = grpVals.getAccessor<Accessor>();

  if (!vec) {
    Scalar *scalar = reinterpret_cast<Scalar *>(para);
    DecimalVar val = *DatumGetValue<DecimalVar *>(scalar->value);
    if (!hasGroupBy) {
      auto aggVal = accessor.at(0);
      val *= sz;
      aggVal->avgVal.sum += val;
      aggVal->avgVal.count += sz;

      return CreateDatum(0);
    }

#pragma clang loop unroll_count(4)
    for (uint64_t i = 0; i < sz; ++i) {
      auto aggVal = accessor.at(hashGroups[i]);
      aggVal->avgVal.sum += val;
      aggVal->avgVal.count += 1;
    }

    return CreateDatum(0);
  }

  // optimize based on vector statistics
  const VectorStatistics *stats = vec->getVectorStatistics();
  if (stats) {
    if (!stats->hasMinMaxStats) return CreateDatum(0);
    auto sum = DatumGetValue<const char *>(stats->sum);
    auto sumDecVal = DecimalType::fromString(std::string(sum, strlen(sum)));

    auto aggVal = accessor.at(0);
    aggVal->avgVal.sum += sumDecVal;
    aggVal->avgVal.count += stats->valueCount;
    return CreateDatum(0);
  }

  bool hasNull = vec->hasNullValue();
  SelectList *sel = vec->getSelected();
  Vector *valVec = vec;

  DecimalVector *dvec = reinterpret_cast<DecimalVector *>(valVec);
  const int64_t *hval =
      reinterpret_cast<const int64_t *>(dvec->getAuxiliaryValue());
  const uint64_t *lval = reinterpret_cast<const uint64_t *>(dvec->getValue());
  const int64_t *sval =
      reinterpret_cast<const int64_t *>(dvec->getScaleValue());

  if (!hasGroupBy) {
    auto aggVal = accessor.at(0);
    if (sel) {
      if (hasNull) {
        dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          uint64_t index = (*sel)[i];

          char isNotNull = nulls->getChar(index) ^ 0x1;  // 0x1 or 0x0
          if (isNotNull) {
            auto val = &(aggVal->avgVal);
            val->sum += DecimalVar(hval[index], lval[index], sval[index]);
            val->count += 1;
          }
        }
      } else {
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          uint64_t index = (*sel)[i];
          auto val = &(aggVal->avgVal);
          val->sum += DecimalVar(hval[index], lval[index], sval[index]);
          val->count += 1;
        }
      }
    } else {
      if (hasNull) {
        dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          char isNotNull = nulls->getChar(i) ^ 0x1;  // 0x1 or 0x0
          if (isNotNull) {
            auto val = &(aggVal->avgVal);
            val->sum += DecimalVar(hval[i], lval[i], sval[i]);
            val->count += 1;
          }
        }
      } else {
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          auto val = &(aggVal->avgVal);
          val->sum += DecimalVar(hval[i], lval[i], sval[i]);
          val->count += 1;
        }
      }
    }
    return CreateDatum(0);
  }

  if (sel) {
    if (hasNull) {
      dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel)[i];

        char isNotNull = nulls->getChar(index) ^ 0x1;  // 0x1 or 0x0
        if (isNotNull) {
          auto aggVal = accessor.at(hashGroups[i]);
          auto val = &(aggVal->avgVal);
          val->sum += DecimalVar(hval[index], lval[index], sval[index]);
          val->count += 1;
        }
      }
    } else {
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel)[i];
        auto aggVal = accessor.at(hashGroups[i]);
        auto val = &(aggVal->avgVal);
        val->sum += DecimalVar(hval[index], lval[index], sval[index]);
        val->count += 1;
      }
    }
  } else {
    if (hasNull) {
      dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        char isNotNull = nulls->getChar(i) ^ 0x1;  // 0x1 or 0x0
        if (isNotNull) {
          auto aggVal = accessor.at(hashGroups[i]);
          auto val = &(aggVal->avgVal);
          val->sum += DecimalVar(hval[i], lval[i], sval[i]);
          val->count += 1;
        }
      }
    } else {
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        auto aggVal = accessor.at(hashGroups[i]);
        auto val = &(aggVal->avgVal);
        val->sum += DecimalVar(hval[i], lval[i], sval[i]);
        val->count += 1;
      }
    }
  }

  return CreateDatum(0);
}
Datum avg_decimal_accu(Datum *params, uint64_t size) {
  AggPrimitiveGroupValues *grpVals =
      DatumGetValue<AggPrimitiveGroupValues *>(params[0]);
  return grpVals->isSmallScale()
             ? avg_decimal_accu_impl<AggDecimalGroupValues::QuickAccessor>(
                   params, size)
             : avg_decimal_accu_impl<AggDecimalGroupValues::Accessor>(params,
                                                                      size);
}

template <typename Accessor>
Datum avg_decimal_amalg_impl(Datum *params, uint64_t size) {
  assert(size == 5);

  AggDecimalGroupValues &grpVals =
      *DatumGetValue<AggDecimalGroupValues *>(params[0]);
  uint64_t sz = DatumGetValue<const std::vector<uint64_t> *>(params[2])->size();
  const uint64_t *__restrict__ hashGroups =
      DatumGetValue<const std::vector<uint64_t> *>(params[2])->data();
  bool hasGroupBy = DatumGetValue<bool>(params[3]);
  Object *para = DatumGetValue<Object *>(params[4]);

  Vector *vec = reinterpret_cast<Vector *>(para);
  assert(dynamic_cast<Vector *>(para)->getTypeKind() == TypeKind::STRUCTID);

  Accessor accessor = grpVals.getAccessor<Accessor>();

  bool hasNull = vec->hasNullValue();
  SelectList *sel = vec->getSelected();
  Vector *valVec = vec->getChildVector(0);
  Vector *countVec = vec->getChildVector(1);

  DecimalVector *dvec = dynamic_cast<DecimalVector *>(valVec);
  const int64_t *hval =
      reinterpret_cast<const int64_t *>(dvec->getAuxiliaryValue());
  const uint64_t *lval = reinterpret_cast<const uint64_t *>(dvec->getValue());
  const int64_t *sval =
      reinterpret_cast<const int64_t *>(dvec->getScaleValue());

  if (!hasGroupBy) {
    auto aggVal = accessor.at(0);
    const int64_t *countv =
        reinterpret_cast<const int64_t *>(countVec->getValue());
    if (sel) {
      if (valVec->hasNullValue()) {
        dbcommon::BoolBuffer *nulls = valVec->getNullBuffer();
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          uint64_t index = (*sel)[i];
          if (!nulls->get(index)) {
            auto val = &(aggVal->avgVal);
            val->count += countv[index];
            val->sum += DecimalVar(hval[index], lval[index], sval[index]);
          }
        }
      } else {
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          uint64_t index = (*sel)[i];
          auto val = &(aggVal->avgVal);
          val->count += countv[index];
          val->sum += DecimalVar(hval[index], lval[index], sval[index]);
        }
      }
    } else {
      if (valVec->hasNullValue()) {
        dbcommon::BoolBuffer *nulls = valVec->getNullBuffer();
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          if (!nulls->get(i)) {
            auto val = &(aggVal->avgVal);
            val->count += countv[i];
            val->sum += DecimalVar(hval[i], lval[i], sval[i]);
          }
        }
      } else {
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          auto val = &(aggVal->avgVal);
          val->count += countv[i];
          val->sum += DecimalVar(hval[i], lval[i], sval[i]);
        }
      }
    }
    return CreateDatum(0);
  }

  const int64_t *countv =
      reinterpret_cast<const int64_t *>(countVec->getValue());
  if (sel) {
    if (valVec->hasNullValue()) {
      dbcommon::BoolBuffer *nulls = valVec->getNullBuffer();
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel)[i];
        if (!nulls->get(index)) {
          auto aggVal = accessor.at(hashGroups[i]);
          auto val = &(aggVal->avgVal);
          val->count += countv[index];
          val->sum += DecimalVar(hval[index], lval[index], sval[index]);
        }
      }
    } else {
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel)[i];
        auto aggVal = accessor.at(hashGroups[i]);
        auto val = &(aggVal->avgVal);
        val->count += countv[index];
        val->sum += DecimalVar(hval[index], lval[index], sval[index]);
      }
    }
  } else {
    if (valVec->hasNullValue()) {
      dbcommon::BoolBuffer *nulls = valVec->getNullBuffer();
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        if (!nulls->get(i)) {
          auto aggVal = accessor.at(hashGroups[i]);
          auto val = &(aggVal->avgVal);
          val->count += countv[i];
          val->sum += DecimalVar(hval[i], lval[i], sval[i]);
        }
      }
    } else {
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        auto aggVal = accessor.at(hashGroups[i]);
        auto val = &(aggVal->avgVal);
        val->count += countv[i];
        val->sum += DecimalVar(hval[i], lval[i], sval[i]);
      }
    }
  }

  return CreateDatum(0);
}

Datum avg_decimal_amalg(Datum *params, uint64_t size) {
  AggPrimitiveGroupValues *grpVals =
      DatumGetValue<AggPrimitiveGroupValues *>(params[0]);
  return grpVals->isSmallScale()
             ? avg_decimal_amalg_impl<AggDecimalGroupValues::QuickAccessor>(
                   params, size)
             : avg_decimal_amalg_impl<AggDecimalGroupValues::Accessor>(params,
                                                                       size);
}

Datum avg_decimal_avg(Datum *params, uint64_t size) {
  assert(size == 1);
  AvgDecimalTransData *val = DatumGetValue<AvgDecimalTransData *>(params[0]);
  if (val->count != 0) {
    val->sum /= DecimalVar(val->count);
  }
  return CreateDatum(&val->sum);
}

Datum sum_int8_sum(Datum *params, uint64_t size) {
  return sum_type_sum<int64_t, int64_t>(params, size);
}

Datum sum_int16_sum(Datum *params, uint64_t size) {
  return sum_type_sum<int64_t, int64_t>(params, size);
}

Datum sum_int32_sum(Datum *params, uint64_t size) {
  return sum_type_sum<int64_t, int64_t>(params, size);
}

Datum sum_int64_sum(Datum *params, uint64_t size) {
  return sum_type_sum<int64_t, int64_t>(params, size);
}

Datum sum_float_sum(Datum *params, uint64_t size) {
  return sum_type_sum<double, double>(params, size);
}

Datum sum_double_sum(Datum *params, uint64_t size) {
  return sum_type_sum<double, double>(params, size);
}

Datum sum_int8_add(Datum *params, uint64_t size) {
  return sum_type_sum<int8_t, int64_t>(params, size);
}

Datum sum_int16_add(Datum *params, uint64_t size) {
  return sum_type_sum<int16_t, int64_t>(params, size);
}

Datum sum_int32_add(Datum *params, uint64_t size) {
  return sum_type_sum<int32_t, int64_t>(params, size);
}

Datum sum_int64_add(Datum *params, uint64_t size) {
  return sum_type_sum<int64_t, int64_t>(params, size);
}

Datum sum_float_add(Datum *params, uint64_t size) {
  return sum_type_sum<float, double>(params, size);
}

Datum sum_double_add(Datum *params, uint64_t size) {
  return sum_type_sum<double, double>(params, size);
}

Datum sum_decimal_add(Datum *params, uint64_t size) {
  return sum_decimal_sum(params, size);
}

Datum avg_int8_accu(Datum *params, uint64_t size) {
  return avg_type_accu<int8_t, int64_t>(params, size);
}

Datum avg_int16_accu(Datum *params, uint64_t size) {
  return avg_type_accu<int16_t, int64_t>(params, size);
}

Datum avg_int32_accu(Datum *params, uint64_t size) {
  return avg_type_accu<int32_t, int64_t>(params, size);
}

Datum avg_int64_accu(Datum *params, uint64_t size) {
  return avg_type_accu<int64_t, int64_t>(params, size);
}

Datum avg_float_accu(Datum *params, uint64_t size) {
  return avg_type_accu<float, double>(params, size);
}

Datum avg_double_accu(Datum *params, uint64_t size) {
  return avg_type_accu<double, double>(params, size);
}

Datum avg_int8_amalg(Datum *params, uint64_t size) {
  return avg_type_amalg(params, size);
}

Datum avg_int16_amalg(Datum *params, uint64_t size) {
  return avg_type_amalg(params, size);
}

Datum avg_int32_amalg(Datum *params, uint64_t size) {
  return avg_type_amalg(params, size);
}

Datum avg_int64_amalg(Datum *params, uint64_t size) {
  return avg_type_amalg(params, size);
}

Datum avg_float_amalg(Datum *params, uint64_t size) {
  return avg_type_amalg(params, size);
}

Datum avg_double_amalg(Datum *params, uint64_t size) {
  return avg_type_amalg(params, size);
}

Datum avg_double_avg(Datum *params, uint64_t size) {
  assert(size == 1);
  auto val = DatumGetValue<AvgPrimitiveTransData *>(params[0]);
  double value = val->sum / val->count;
  return CreateDatum<double>(value);
}

template <typename T, typename C, typename Accessor>
Datum min_max_impl(Datum *params, uint64_t size) {
  assert(size == 5);

  AggPrimitiveGroupValues &grpVals =
      *DatumGetValue<AggPrimitiveGroupValues *>(params[0]);
  TupleBatch *batch = DatumGetValue<TupleBatch *>(params[1]);
  uint64_t sz = DatumGetValue<const std::vector<uint64_t> *>(params[2])->size();
  const uint64_t *__restrict__ hashGroups =
      DatumGetValue<const std::vector<uint64_t> *>(params[2])->data();
  Object *para = DatumGetValue<Object *>(params[4]);
  Vector *vec = dynamic_cast<Vector *>(para);

  Accessor accessor = grpVals.getAccessor<Accessor>();

  if (!vec) {
    Scalar *scalar = dynamic_cast<Scalar *>(para);

#pragma clang loop unroll_count(4)
    for (uint64_t i = 0; i < sz; ++i) {
      auto aggVal = accessor.at(hashGroups[i]);
      aggVal->accVal.isNotNull = true;
      aggVal->accVal.value = scalar->value;
    }
    return CreateDatum(0);
  }

  // optimize based on vector statistics
  const VectorStatistics *stats = vec->getVectorStatistics();
  if (stats) {
    if (!stats->hasMinMaxStats) return CreateDatum(0);
    auto aggVal = accessor.at(0);
    Datum &val = aggVal->accVal.value;
    Datum statsVal = CreateDatum<T>(C()(DatumGetValue<T>(stats->minimum),
                                        DatumGetValue<T>(stats->maximum)));
    if (!aggVal->accVal.isNotNull) {
      aggVal->accVal.isNotNull = true;
      val = statsVal;
    } else {
      val = CreateDatum<T>(
          C()(DatumGetValue<T>(statsVal), DatumGetValue<T>(val)));
    }
    return CreateDatum(0);
  }

  bool hasNull = vec->hasNullValue();
  SelectList *sel = vec->getSelected();
  assert(sz == DatumGetValue<const std::vector<uint64_t> *>(params[2])->size());
  bool hasGroupBy = DatumGetValue<bool>(params[3]);
  const T *v = reinterpret_cast<const T *>(vec->getValue());

  if (!hasGroupBy) {
    assert(grpVals.size() == 1);
    auto aggVal = accessor.at(0);
    if (sel) {
      if (hasNull) {
        dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          uint64_t index = (*sel)[i];
          Datum &val = aggVal->accVal.value;
          if (!nulls->get(index)) {
            if (!aggVal->accVal.isNotNull) {
              aggVal->accVal.isNotNull = true;
              val = CreateDatum<T>(v[index]);
            } else {
              val = CreateDatum<T>(C()(v[index], DatumGetValue<T>(val)));
            }
          }
        }
      } else {
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          uint64_t index = (*sel)[i];
          Datum &val = aggVal->accVal.value;
          if (!aggVal->accVal.isNotNull) {
            aggVal->accVal.isNotNull = true;
            val = CreateDatum<T>(v[index]);
          } else {
            val = CreateDatum<T>(C()(v[index], DatumGetValue<T>(val)));
          }
        }
      }
    } else {
      if (hasNull) {
        dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          Datum &val = aggVal->accVal.value;
          if (!nulls->get(i)) {
            if (!aggVal->accVal.isNotNull) {
              aggVal->accVal.isNotNull = true;
              val = CreateDatum<T>(v[i]);
            } else {
              val = CreateDatum<T>(C()(v[i], DatumGetValue<T>(val)));
            }
          }
        }
      } else {
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          Datum &val = aggVal->accVal.value;
          if (!aggVal->accVal.isNotNull) {
            aggVal->accVal.isNotNull = true;
            val = CreateDatum<T>(v[i]);
          } else {
            val = CreateDatum<T>(C()(v[i], DatumGetValue<T>(val)));
          }
        }
      }
    }

    return CreateDatum(0);
  }

  if (sel) {
    if (hasNull) {
      dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel)[i];
        auto aggVal = accessor.at(hashGroups[i]);
        Datum &val = aggVal->accVal.value;
        if (!nulls->get(index)) {
          if (!aggVal->accVal.isNotNull) {
            aggVal->accVal.isNotNull = true;
            val = CreateDatum<T>(v[index]);
          } else {
            val = CreateDatum<T>(C()(v[index], DatumGetValue<T>(val)));
          }
        }
      }
    } else {
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel)[i];
        auto aggVal = accessor.at(hashGroups[i]);
        Datum &val = aggVal->accVal.value;
        if (!aggVal->accVal.isNotNull) {
          aggVal->accVal.isNotNull = true;
          val = CreateDatum<T>(v[index]);
        } else {
          val = CreateDatum<T>(C()(v[index], DatumGetValue<T>(val)));
        }
      }
    }
  } else {
    if (hasNull) {
      dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        auto aggVal = accessor.at(hashGroups[i]);
        Datum &val = aggVal->accVal.value;
        if (!nulls->get(i)) {
          if (!aggVal->accVal.isNotNull) {
            aggVal->accVal.isNotNull = true;
            val = CreateDatum<T>(v[i]);
          } else {
            val = CreateDatum<T>(C()(v[i], DatumGetValue<T>(val)));
          }
        }
      }
    } else {
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        auto aggVal = accessor.at(hashGroups[i]);
        Datum &val = aggVal->accVal.value;
        if (!aggVal->accVal.isNotNull) {
          aggVal->accVal.isNotNull = true;
          val = CreateDatum<T>(v[i]);
        } else {
          val = CreateDatum<T>(C()(v[i], DatumGetValue<T>(val)));
        }
      }
    }
  }
  return CreateDatum(0);
}
template <typename T, typename C>
Datum min_max_internal(Datum *params, uint64_t size) {
  AggPrimitiveGroupValues *grpVals =
      DatumGetValue<AggPrimitiveGroupValues *>(params[0]);
  return grpVals->isSmallScale()
             ? min_max_impl<T, C, AggPrimitiveGroupValues::QuickAccessor>(
                   params, size)
             : min_max_impl<T, C, AggPrimitiveGroupValues::Accessor>(params,
                                                                     size);
}

template <typename C, typename Accessor>
Datum min_max_string_impl(Datum *params, uint64_t size) {
  assert(size == 5);

  auto &grpVals = *DatumGetValue<AggStringGroupValues *>(params[0]);
  TupleBatch *batch = DatumGetValue<TupleBatch *>(params[1]);
  uint64_t sz = DatumGetValue<const std::vector<uint64_t> *>(params[2])->size();
  const uint64_t *__restrict__ hashGroups =
      DatumGetValue<const std::vector<uint64_t> *>(params[2])->data();
  Object *para = DatumGetValue<Object *>(params[4]);
  Vector *vec = dynamic_cast<Vector *>(para);

  Accessor accessor = grpVals.getAccessor<Accessor>();

  if (!vec) {
    Scalar *scalar = dynamic_cast<Scalar *>(para);

#pragma clang loop unroll_count(4)
    for (uint64_t i = 0; i < sz; ++i) {
      auto aggVal = accessor.at(hashGroups[i]);
      if (!aggVal->accVal.isNotNull) {
        auto str = DatumGetValue<char *>(scalar->value);
        aggVal->accVal.value = grpVals.newStrTransData(str, strlen(str));
      }
    }

    return CreateDatum(0);
  }

  // optimize based on vector statistics
  const VectorStatistics *stats = vec->getVectorStatistics();
  if (stats) {
    if (!stats->hasMinMaxStats) return CreateDatum(0);
    auto aggVal = accessor.at(0);
    char *minStr = DatumGetValue<char *>(stats->minimum);
    char *maxStr = DatumGetValue<char *>(stats->maximum);
    char *statsStr = maxStr;
    if (C::strCmp(minStr, strlen(minStr), maxStr, strlen(maxStr))) {
      statsStr = minStr;
    }
    if (aggVal->accVal.isNotNull) {
      auto valStr = aggVal->accVal.value;
      if (C::strCmp(statsStr, strlen(statsStr), valStr->str, valStr->length)) {
        cnfree(valStr);
        aggVal->accVal.value =
            grpVals.newStrTransData(statsStr, strlen(statsStr));
      }
    } else {
      aggVal->accVal.value =
          grpVals.newStrTransData(statsStr, strlen(statsStr));
    }
    return CreateDatum(0);
  }

  SelectList *sel = vec->getSelected();
  const bool *__restrict__ nulls = vec->getNulls();
  assert(sz == DatumGetValue<const std::vector<uint64_t> *>(params[2])->size());
  bool hasGroupBy = DatumGetValue<bool>(params[3]);

  const char **v = vec->getValPtrs();
  const uint64_t *lens = vec->getLengths();

  if (!hasGroupBy) {
    assert(grpVals.size() == 1);
    auto aggVal = accessor.at(0);
    if (sel) {
      if (nulls) {
#pragma clang loop unroll_count(4)
        for (auto plainIdx : *sel) {
          if (!nulls[plainIdx]) {
            if (aggVal->accVal.isNotNull) {
              auto oldStr = aggVal->accVal.value;
              if (C::strCmp(v[plainIdx], lens[plainIdx], oldStr->str,
                            oldStr->length)) {
                cnfree(oldStr);
                aggVal->accVal.value =
                    grpVals.newStrTransData(v[plainIdx], lens[plainIdx]);
              }
            } else {
              aggVal->accVal.value =
                  grpVals.newStrTransData(v[plainIdx], lens[plainIdx]);
            }
          }
        }
      } else {
#pragma clang loop unroll_count(4)
        for (auto plainIdx : *sel) {
          if (aggVal->accVal.isNotNull) {
            auto oldStr = aggVal->accVal.value;
            if (C::strCmp(v[plainIdx], lens[plainIdx], oldStr->str,
                          oldStr->length)) {
              cnfree(oldStr);
              aggVal->accVal.value =
                  grpVals.newStrTransData(v[plainIdx], lens[plainIdx]);
            }
          } else {
            aggVal->accVal.value =
                grpVals.newStrTransData(v[plainIdx], lens[plainIdx]);
          }
        }
      }
    } else {
      if (nulls) {
#pragma clang loop unroll_count(4)
        for (auto plainIdx = 0; plainIdx < sz; plainIdx++) {
          if (!nulls[plainIdx]) {
            if (aggVal->accVal.isNotNull) {
              auto oldStr = aggVal->accVal.value;
              if (C::strCmp(v[plainIdx], lens[plainIdx], oldStr->str,
                            oldStr->length)) {
                cnfree(oldStr);
                aggVal->accVal.value =
                    grpVals.newStrTransData(v[plainIdx], lens[plainIdx]);
              }
            } else {
              aggVal->accVal.value =
                  grpVals.newStrTransData(v[plainIdx], lens[plainIdx]);
            }
          }
        }
      } else {
#pragma clang loop unroll_count(4)
        for (auto plainIdx = 0; plainIdx < sz; plainIdx++) {
          if (aggVal->accVal.isNotNull) {
            auto oldStr = aggVal->accVal.value;
            if (C::strCmp(v[plainIdx], lens[plainIdx], oldStr->str,
                          oldStr->length)) {
              cnfree(oldStr);
              aggVal->accVal.value =
                  grpVals.newStrTransData(v[plainIdx], lens[plainIdx]);
            }
          } else {
            aggVal->accVal.value =
                grpVals.newStrTransData(v[plainIdx], lens[plainIdx]);
          }
        }
      }
    }

    return CreateDatum(0);
  }

  if (sel) {
    if (nulls) {
#pragma clang loop unroll_count(4)
      for (auto actualIdx = 0; actualIdx < sz; actualIdx++) {
        auto plainIdx = (*sel)[actualIdx];
        if (!nulls[plainIdx]) {
          auto aggVal = accessor.at(hashGroups[actualIdx]);
          if (aggVal->accVal.isNotNull) {
            auto oldStr = aggVal->accVal.value;
            if (C::strCmp(v[plainIdx], lens[plainIdx], oldStr->str,
                          oldStr->length)) {
              cnfree(oldStr);
              aggVal->accVal.value =
                  grpVals.newStrTransData(v[plainIdx], lens[plainIdx]);
            }
          } else {
            aggVal->accVal.value =
                grpVals.newStrTransData(v[plainIdx], lens[plainIdx]);
          }
        }
      }
    } else {
#pragma clang loop unroll_count(4)
      for (auto actualIdx = 0; actualIdx < sz; actualIdx++) {
        auto plainIdx = (*sel)[actualIdx];
        auto aggVal = accessor.at(hashGroups[actualIdx]);
        if (aggVal->accVal.isNotNull) {
          auto oldStr = aggVal->accVal.value;
          if (C::strCmp(v[plainIdx], lens[plainIdx], oldStr->str,
                        oldStr->length)) {
            cnfree(oldStr);
            aggVal->accVal.value =
                grpVals.newStrTransData(v[plainIdx], lens[plainIdx]);
          }
        } else {
          aggVal->accVal.value =
              grpVals.newStrTransData(v[plainIdx], lens[plainIdx]);
        }
      }
    }
  } else {
    if (nulls) {
#pragma clang loop unroll_count(4)
      for (auto plainIdx = 0; plainIdx < sz; plainIdx++) {
        if (!nulls[plainIdx]) {
          auto actualIdx = plainIdx;
          auto aggVal = accessor.at(hashGroups[actualIdx]);
          if (aggVal->accVal.isNotNull) {
            auto oldStr = aggVal->accVal.value;
            if (C::strCmp(v[plainIdx], lens[plainIdx], oldStr->str,
                          oldStr->length)) {
              cnfree(oldStr);
              aggVal->accVal.value =
                  grpVals.newStrTransData(v[plainIdx], lens[plainIdx]);
            }
          } else {
            aggVal->accVal.value =
                grpVals.newStrTransData(v[plainIdx], lens[plainIdx]);
          }
        }
      }
    } else {
#pragma clang loop unroll_count(4)
      for (auto plainIdx = 0; plainIdx < sz; plainIdx++) {
        auto actualIdx = plainIdx;
        auto aggVal = accessor.at(hashGroups[actualIdx]);
        if (aggVal->accVal.isNotNull) {
          auto oldStr = aggVal->accVal.value;
          if (C::strCmp(v[plainIdx], lens[plainIdx], oldStr->str,
                        oldStr->length)) {
            cnfree(oldStr);
            aggVal->accVal.value =
                grpVals.newStrTransData(v[plainIdx], lens[plainIdx]);
          }
        } else {
          aggVal->accVal.value =
              grpVals.newStrTransData(v[plainIdx], lens[plainIdx]);
        }
      }
    }
  }

  return CreateDatum(0);
}
template <typename C>
Datum min_max_string(Datum *params, uint64_t size) {
  using AggGroupValues = AggStringGroupValues;
  AggGroupValues *grpVals = DatumGetValue<AggGroupValues *>(params[0]);
  return grpVals->isSmallScale()
             ? min_max_string_impl<C, AggGroupValues::QuickAccessor>(params,
                                                                     size)
             : min_max_string_impl<C, AggGroupValues::Accessor>(params, size);
}

template <typename C, typename Accessor>
Datum min_max_timestamp_impl(Datum *params, uint64_t size) {
  assert(size == 5);

  auto &grpVals = *DatumGetValue<AggTimestampGroupValues *>(params[0]);
  uint64_t sz = DatumGetValue<const std::vector<uint64_t> *>(params[2])->size();
  const uint64_t *__restrict__ hashGroups =
      DatumGetValue<const std::vector<uint64_t> *>(params[2])->data();
  Object *para = DatumGetValue<Object *>(params[4]);
  Vector *vec = dynamic_cast<Vector *>(para);

  Accessor accessor = grpVals.getAccessor<Accessor>();

  if (!vec) {
    Scalar *scalar = dynamic_cast<Scalar *>(para);

#pragma clang loop unroll_count(4)
    for (uint64_t i = 0; i < sz; ++i) {
      auto aggVal = accessor.at(hashGroups[i]);
      aggVal->accVal.value = *DatumGetValue<Timestamp *>(scalar->value);
      aggVal->accVal.isNotNull = true;
    }

    return CreateDatum(0);
  }

  // optimize based on vector statistics, but now it is disabled for statistics
  // for timestamp is only millisecond
  const VectorStatistics *stats = vec->getVectorStatistics();
  if (false && stats) {
    if (!stats->hasMinMaxStats) return CreateDatum(0);
    auto aggVal = accessor.at(0);
    auto &val = aggVal->accVal.value;
    Timestamp *minTs = DatumGetValue<Timestamp *>(stats->minimum);
    Timestamp *maxTs = DatumGetValue<Timestamp *>(stats->minimum);
    Timestamp *statsTs = maxTs;
    if (!aggVal->accVal.isNotNull) {
      aggVal->accVal.isNotNull = true;
      val = C()(*minTs, *maxTs);
    } else {
      val = C()(val, C()(*minTs, *maxTs));
    }
    return CreateDatum(0);
  }

  bool hasNull = vec->hasNullValue();
  SelectList *sel = vec->getSelected();
  assert(sz == DatumGetValue<const std::vector<uint64_t> *>(params[2])->size());
  bool hasGroupBy = DatumGetValue<bool>(params[3]);
  const int64_t *v = reinterpret_cast<const int64_t *>(vec->getValue());
  const int64_t *nano =
      reinterpret_cast<const int64_t *>(vec->getNanoseconds());

  if (!hasGroupBy) {
    assert(grpVals.size() == 1);
    auto aggVal = accessor.at(0);
    if (sel) {
      if (hasNull) {
        dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          uint64_t index = (*sel)[i];
          if (!nulls->get(index)) {
            if (!aggVal->accVal.isNotNull) {
              aggVal->accVal.isNotNull = true;
              aggVal->accVal.value = Timestamp(v[index], nano[index]);
            } else {
              auto val = aggVal->accVal.value;
              aggVal->accVal.value = C()(val, Timestamp(v[index], nano[index]));
            }
          }
        }
      } else {
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          uint64_t index = (*sel)[i];
          if (!aggVal->accVal.isNotNull) {
            aggVal->accVal.isNotNull = true;
            aggVal->accVal.value = Timestamp(v[index], nano[index]);
          } else {
            auto val = aggVal->accVal.value;
            aggVal->accVal.value = C()(val, Timestamp(v[index], nano[index]));
          }
        }
      }
    } else {
      if (hasNull) {
        dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          if (!nulls->get(i)) {
            if (!aggVal->accVal.isNotNull) {
              aggVal->accVal.isNotNull = true;
              aggVal->accVal.value = Timestamp(v[i], nano[i]);
            } else {
              auto val = aggVal->accVal.value;
              aggVal->accVal.value = C()(val, Timestamp(v[i], nano[i]));
            }
          }
        }
      } else {
#pragma clang loop unroll_count(4)
        for (uint64_t i = 0; i < sz; ++i) {
          if (!aggVal->accVal.isNotNull) {
            aggVal->accVal.isNotNull = true;
            aggVal->accVal.value = Timestamp(v[i], nano[i]);
          } else {
            auto val = aggVal->accVal.value;
            aggVal->accVal.value = C()(val, Timestamp(v[i], nano[i]));
          }
        }
      }
    }

    return CreateDatum(0);
  }

  if (sel) {
    if (hasNull) {
      dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel)[i];
        auto aggVal = accessor.at(hashGroups[i]);
        if (!nulls->get(index)) {
          if (!aggVal->accVal.isNotNull) {
            aggVal->accVal.isNotNull = true;
            aggVal->accVal.value = Timestamp(v[index], nano[index]);
          } else {
            auto val = aggVal->accVal.value;
            aggVal->accVal.value = C()(val, Timestamp(v[index], nano[index]));
          }
        }
      }
    } else {
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        uint64_t index = (*sel)[i];
        auto aggVal = accessor.at(hashGroups[i]);
        if (!aggVal->accVal.isNotNull) {
          aggVal->accVal.isNotNull = true;
          aggVal->accVal.value = Timestamp(v[index], nano[index]);
        } else {
          auto val = aggVal->accVal.value;
          aggVal->accVal.value = C()(val, Timestamp(v[index], nano[index]));
        }
      }
    }
  } else {
    if (hasNull) {
      dbcommon::BoolBuffer *nulls = vec->getNullBuffer();
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        auto aggVal = accessor.at(hashGroups[i]);
        if (!nulls->get(i)) {
          if (!aggVal->accVal.isNotNull) {
            aggVal->accVal.isNotNull = true;
            aggVal->accVal.value = Timestamp(v[i], nano[i]);
          } else {
            auto val = aggVal->accVal.value;
            aggVal->accVal.value = C()(val, Timestamp(v[i], nano[i]));
          }
        }
      }
    } else {
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < sz; ++i) {
        auto aggVal = accessor.at(hashGroups[i]);
        if (!aggVal->accVal.isNotNull) {
          aggVal->accVal.isNotNull = true;
          aggVal->accVal.value = Timestamp(v[i], nano[i]);
        } else {
          auto val = aggVal->accVal.value;
          aggVal->accVal.value = C()(val, Timestamp(v[i], nano[i]));
        }
      }
    }
  }
  return CreateDatum(0);
}
template <typename C>
Datum min_max_timestamp(Datum *params, uint64_t size) {
  using AggGroupValues = AggTimestampGroupValues;
  auto grpVals = DatumGetValue<AggGroupValues *>(params[0]);
  return grpVals->isSmallScale()
             ? min_max_timestamp_impl<C, AggGroupValues::QuickAccessor>(params,
                                                                        size)
             : min_max_timestamp_impl<C, AggGroupValues::Accessor>(params,
                                                                   size);
}

template <typename C, typename Accessor>
Datum min_max_decimal_impl(Datum *params, uint64_t size) {
  assert(size == 5);

  AggDecimalGroupValues &grpVals =
      *DatumGetValue<AggDecimalGroupValues *>(params[0]);
  TupleBatch *batch = DatumGetValue<TupleBatch *>(params[1]);
  uint64_t sz = DatumGetValue<const std::vector<uint64_t> *>(params[2])->size();
  const uint64_t *__restrict__ hashGroups =
      DatumGetValue<const std::vector<uint64_t> *>(params[2])->data();
  bool hasGroupBy = DatumGetValue<bool>(params[3]);
  Object *para = DatumGetValue<Object *>(params[4]);
  Vector *vec = dynamic_cast<Vector *>(para);

  Accessor accessor = grpVals.getAccessor<Accessor>();

  if (!vec) {
    Scalar *scalar = dynamic_cast<Scalar *>(para);
    DecimalVar val = *DatumGetValue<DecimalVar *>(scalar->value);

#pragma clang loop unroll_count(4)
    for (uint64_t i = 0; i < sz; ++i) {
      auto aggVal = accessor.at(hashGroups[i]);
      aggVal->accVal.isNotNull = true;
      aggVal->accVal.value = val;
    }
    return CreateDatum(0);
  }

  // optimize based on vector statistics
  const VectorStatistics *stats = vec->getVectorStatistics();
  if (stats) {
    if (!stats->hasMinMaxStats) return CreateDatum(0);
    auto min = DatumGetValue<const char *>(stats->minimum);
    auto max = DatumGetValue<const char *>(stats->maximum);

    DecimalVar minDecVal =
        DecimalType::fromString(std::string(min, strlen(min)));
    DecimalVar maxDecVal =
        DecimalType::fromString(std::string(max, strlen(max)));
    auto statsVal = (C()(minDecVal, maxDecVal));

    auto aggVal = accessor.at(0);
    auto &val = aggVal->accVal.value;
    if (!aggVal->accVal.isNotNull) {
      aggVal->accVal.value = statsVal;
      aggVal->accVal.isNotNull = true;
    } else {
      aggVal->accVal.value = C()(aggVal->accVal.value, statsVal);
    }
    return CreateDatum(0);
  }

  bool hasNull = vec->hasNullValue();
  SelectList *sel = vec->getSelected();
  assert(sz == DatumGetValue<const std::vector<uint64_t> *>(params[2])->size());

  DecimalVector *dvec = dynamic_cast<DecimalVector *>(vec);
  const int64_t *hval =
      reinterpret_cast<const int64_t *>(dvec->getAuxiliaryValue());
  const uint64_t *lval = reinterpret_cast<const uint64_t *>(dvec->getValue());
  const int64_t *sval =
      reinterpret_cast<const int64_t *>(dvec->getScaleValue());

  if (!hasGroupBy) {
    assert(grpVals.size() == 1);
    auto aggVal = accessor.at(0);
    if (sel) {
      if (hasNull) {
        auto nulls = vec->getNulls();
#pragma clang loop unroll_count(4)
        for (uint64_t actualIdx = 0; actualIdx < sz; ++actualIdx) {
          uint64_t plainIdx = (*sel)[actualIdx];
          auto &val = aggVal->accVal.value;
          if (!nulls[plainIdx]) {
            if (!aggVal->accVal.isNotNull) {
              aggVal->accVal.isNotNull = true;
              val = DecimalVar(hval[plainIdx], lval[plainIdx], sval[plainIdx]);
            } else {
              val = C()(val, DecimalVar(hval[plainIdx], lval[plainIdx],
                                        sval[plainIdx]));
            }
          }
        }
      } else {
#pragma clang loop unroll_count(4)
        for (uint64_t actualIdx = 0; actualIdx < sz; ++actualIdx) {
          uint64_t plainIdx = (*sel)[actualIdx];
          auto &val = aggVal->accVal.value;
          if (!aggVal->accVal.isNotNull) {
            aggVal->accVal.isNotNull = true;
            val = DecimalVar(hval[plainIdx], lval[plainIdx], sval[plainIdx]);
          } else {
            val = C()(val, DecimalVar(hval[plainIdx], lval[plainIdx],
                                      sval[plainIdx]));
          }
        }
      }
    } else {
      if (hasNull) {
        auto nulls = vec->getNulls();
#pragma clang loop unroll_count(4)
        for (uint64_t actualIdx = 0; actualIdx < sz; ++actualIdx) {
          uint64_t plainIdx = actualIdx;
          auto &val = aggVal->accVal.value;
          if (!nulls[plainIdx]) {
            if (!aggVal->accVal.isNotNull) {
              aggVal->accVal.isNotNull = true;
              val = DecimalVar(hval[plainIdx], lval[plainIdx], sval[plainIdx]);
            } else {
              val = C()(val, DecimalVar(hval[plainIdx], lval[plainIdx],
                                        sval[plainIdx]));
            }
          }
        }
      } else {
#pragma clang loop unroll_count(4)
        for (uint64_t actualIdx = 0; actualIdx < sz; ++actualIdx) {
          uint64_t plainIdx = actualIdx;
          auto &val = aggVal->accVal.value;
          if (!aggVal->accVal.isNotNull) {
            aggVal->accVal.isNotNull = true;
            val = DecimalVar(hval[plainIdx], lval[plainIdx], sval[plainIdx]);
          } else {
            val = C()(val, DecimalVar(hval[plainIdx], lval[plainIdx],
                                      sval[plainIdx]));
          }
        }
      }
    }

    return CreateDatum(0);
  }

  if (sel) {
    if (hasNull) {
      auto nulls = vec->getNulls();
#pragma clang loop unroll_count(4)
      for (uint64_t actualIdx = 0; actualIdx < sz; ++actualIdx) {
        uint64_t plainIdx = (*sel)[actualIdx];
        auto aggVal = accessor.at(hashGroups[actualIdx]);
        auto &val = aggVal->accVal.value;
        if (!nulls[plainIdx]) {
          if (!aggVal->accVal.isNotNull) {
            aggVal->accVal.isNotNull = true;
            val = DecimalVar(hval[plainIdx], lval[plainIdx], sval[plainIdx]);
          } else {
            val = C()(val, DecimalVar(hval[plainIdx], lval[plainIdx],
                                      sval[plainIdx]));
          }
        }
      }
    } else {
#pragma clang loop unroll_count(4)
      for (uint64_t actualIdx = 0; actualIdx < sz; ++actualIdx) {
        uint64_t plainIdx = (*sel)[actualIdx];
        auto aggVal = accessor.at(hashGroups[actualIdx]);
        auto &val = aggVal->accVal.value;
        if (!aggVal->accVal.isNotNull) {
          aggVal->accVal.isNotNull = true;
          val = DecimalVar(hval[plainIdx], lval[plainIdx], sval[plainIdx]);
        } else {
          val = C()(val,
                    DecimalVar(hval[plainIdx], lval[plainIdx], sval[plainIdx]));
        }
      }
    }
  } else {
    if (hasNull) {
      auto nulls = vec->getNulls();
#pragma clang loop unroll_count(4)
      for (uint64_t actualIdx = 0; actualIdx < sz; ++actualIdx) {
        uint64_t plainIdx = actualIdx;
        auto aggVal = accessor.at(hashGroups[actualIdx]);
        auto &val = aggVal->accVal.value;
        if (!nulls[plainIdx]) {
          if (!aggVal->accVal.isNotNull) {
            aggVal->accVal.isNotNull = true;
            val = DecimalVar(hval[plainIdx], lval[plainIdx], sval[plainIdx]);
          } else {
            val = C()(val, DecimalVar(hval[plainIdx], lval[plainIdx],
                                      sval[plainIdx]));
          }
        }
      }
    } else {
#pragma clang loop unroll_count(4)
      for (uint64_t actualIdx = 0; actualIdx < sz; ++actualIdx) {
        uint64_t plainIdx = actualIdx;
        auto aggVal = accessor.at(hashGroups[actualIdx]);
        auto &val = aggVal->accVal.value;
        if (!aggVal->accVal.isNotNull) {
          aggVal->accVal.isNotNull = true;
          val = DecimalVar(hval[plainIdx], lval[plainIdx], sval[plainIdx]);
        } else {
          val = C()(val,
                    DecimalVar(hval[plainIdx], lval[plainIdx], sval[plainIdx]));
        }
      }
    }
  }
  return CreateDatum(0);
}
template <typename C>
Datum min_max_decimal(Datum *params, uint64_t size) {
  AggPrimitiveGroupValues *grpVals =
      DatumGetValue<AggPrimitiveGroupValues *>(params[0]);
  return grpVals->isSmallScale()
             ? min_max_decimal_impl<C, AggDecimalGroupValues::QuickAccessor>(
                   params, size)
             : min_max_decimal_impl<C, AggDecimalGroupValues::Accessor>(params,
                                                                        size);
}

template <typename T>
class MinCmp {
 public:
  T operator()(T x, T y) { return std::min<T>(x, y); }
  static bool strCmp(const char *__restrict__ xSrc, uint64_t lenx,
                     const char *__restrict__ ySrc, uint64_t leny) {
    const unsigned char *__restrict__ x =
        reinterpret_cast<const unsigned char *>(xSrc);
    const unsigned char *__restrict__ y =
        reinterpret_cast<const unsigned char *>(ySrc);
    uint64_t len = lenx < leny ? lenx : leny;
    if (lenx < leny) {
      auto xEnd = x + lenx;
      while (x < xEnd && (*x == *y)) {
        x++;
        y++;
      }
      if (x == xEnd || *x < *y) return true;
    } else {
      auto yEnd = y + leny;
      while (y < yEnd && (*x == *y)) {
        x++;
        y++;
      }
      if (y != yEnd && *x < *y) return true;
    }
    return false;
  }
};

template <typename T>
class MaxCmp {
 public:
  T operator()(T x, T y) { return std::max<T>(x, y); }
  static bool strCmp(const char *__restrict__ xSrc, uint64_t lenx,
                     const char *__restrict__ ySrc, uint64_t leny) {
    const unsigned char *__restrict__ x =
        reinterpret_cast<const unsigned char *>(xSrc);
    const unsigned char *__restrict__ y =
        reinterpret_cast<const unsigned char *>(ySrc);
    uint64_t len = lenx < leny ? lenx : leny;
    if (lenx > leny) {
      auto yEnd = y + leny;
      while (y < yEnd && (*x == *y)) {
        x++;
        y++;
      }
      if (y == yEnd || *x > *y) return true;
    } else {
      auto xEnd = x + lenx;
      while (x < xEnd && (*x == *y)) {
        x++;
        y++;
      }
      if (x != xEnd && *x > *y) return true;
    }
    return false;
  }
};

Datum min_int8_smaller(Datum *params, uint64_t size) {
  return min_max_internal<int8_t, MinCmp<int8_t>>(params, size);
}

Datum min_int16_smaller(Datum *params, uint64_t size) {
  return min_max_internal<int16_t, MinCmp<int16_t>>(params, size);
}

Datum min_int32_smaller(Datum *params, uint64_t size) {
  return min_max_internal<int32_t, MinCmp<int32_t>>(params, size);
}

Datum min_int64_smaller(Datum *params, uint64_t size) {
  return min_max_internal<int64_t, MinCmp<int64_t>>(params, size);
}

Datum min_float_smaller(Datum *params, uint64_t size) {
  return min_max_internal<float, MinCmp<float>>(params, size);
}

Datum min_double_smaller(Datum *params, uint64_t size) {
  return min_max_internal<double, MinCmp<double>>(params, size);
}

Datum min_string_smaller(Datum *params, uint64_t size) {
  return min_max_string<MinCmp<std::string>>(params, size);
}

Datum min_date_smaller(Datum *params, uint64_t size) {
  return min_max_internal<int32_t, MinCmp<int32_t>>(params, size);
}

Datum min_time_smaller(Datum *params, uint64_t size) {
  return min_max_internal<int64_t, MinCmp<int64_t>>(params, size);
}

Datum min_timestamp_smaller(Datum *params, uint64_t size) {
  return min_max_timestamp<MinCmp<Timestamp>>(params, size);
}

Datum min_decimal_smaller(Datum *params, uint64_t size) {
  return min_max_decimal<MinCmp<DecimalVar>>(params, size);
}

Datum max_int8_larger(Datum *params, uint64_t size) {
  return min_max_internal<int8_t, MaxCmp<int8_t>>(params, size);
}

Datum max_int16_larger(Datum *params, uint64_t size) {
  return min_max_internal<int16_t, MaxCmp<int16_t>>(params, size);
}

Datum max_int32_larger(Datum *params, uint64_t size) {
  return min_max_internal<int32_t, MaxCmp<int32_t>>(params, size);
}

Datum max_int64_larger(Datum *params, uint64_t size) {
  return min_max_internal<int64_t, MaxCmp<int64_t>>(params, size);
}

Datum max_float_larger(Datum *params, uint64_t size) {
  return min_max_internal<float, MaxCmp<float>>(params, size);
}

Datum max_double_larger(Datum *params, uint64_t size) {
  return min_max_internal<double, MaxCmp<double>>(params, size);
}

Datum max_string_larger(Datum *params, uint64_t size) {
  return min_max_string<MaxCmp<std::string>>(params, size);
}

Datum max_date_larger(Datum *params, uint64_t size) {
  return min_max_internal<int32_t, MaxCmp<int32_t>>(params, size);
}

Datum max_time_larger(Datum *params, uint64_t size) {
  return min_max_internal<int64_t, MaxCmp<int64_t>>(params, size);
}

Datum max_timestamp_larger(Datum *params, uint64_t size) {
  return min_max_timestamp<MaxCmp<Timestamp>>(params, size);
}

Datum max_decimal_larger(Datum *params, uint64_t size) {
  return min_max_decimal<MaxCmp<DecimalVar>>(params, size);
}

}  // namespace dbcommon
