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

#ifndef DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_TRANSFORMER_H_
#define DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_TRANSFORMER_H_

#include "dbcommon/common/vector.h"
#include "dbcommon/nodes/select-list.h"

namespace dbcommon {

template <class Operation>
inline void transformVector(const size_t plainSize, const SelectList *sel,
                            const bool *__restrict__ nulls,
                            Operation operation) {
  if (sel) {
    if (nulls) {
      for (auto plainIdx : *sel) {
        if (!nulls[plainIdx]) operation(plainIdx);
      }
    } else {
      for (auto plainIdx : *sel) {
        operation(plainIdx);
      }
    }
  } else {
    if (nulls) {
      for (uint64_t plainIdx = 0; plainIdx < plainSize; ++plainIdx) {
        if (!nulls[plainIdx]) operation(plainIdx);
      }
    } else {
      for (uint64_t plainIdx = 0; plainIdx < plainSize; ++plainIdx) {
        operation(plainIdx);
      }
    }
  }
}

template <class Operation>
inline void transformVector(const size_t plainSize, const SelectList *sel,
                            const bool *__restrict__ nulls1,
                            const bool *__restrict__ nulls2,
                            Operation operation) {
  if (sel) {
    if (nulls1) {
      if (nulls2) {
        for (auto plainIdx : *sel)
          if (!nulls1[plainIdx] && !nulls2[plainIdx]) operation(plainIdx);
      } else {
        for (auto plainIdx : *sel)
          if (!nulls1[plainIdx]) operation(plainIdx);
      }
    } else {
      if (nulls2) {
        for (auto plainIdx : *sel)
          if (!nulls2[plainIdx]) operation(plainIdx);
      } else {
        for (auto plainIdx : *sel) operation(plainIdx);
      }
    }
  } else {
    if (nulls1) {
      if (nulls2) {
        for (uint64_t plainIdx = 0; plainIdx < plainSize; ++plainIdx)
          if (!nulls1[plainIdx] && !nulls2[plainIdx]) operation(plainIdx);
      } else {
        for (uint64_t plainIdx = 0; plainIdx < plainSize; ++plainIdx)
          if (!nulls1[plainIdx]) operation(plainIdx);
      }
    } else {
      if (nulls2) {
        for (uint64_t plainIdx = 0; plainIdx < plainSize; ++plainIdx)
          if (!nulls2[plainIdx]) operation(plainIdx);
      } else {
        for (uint64_t plainIdx = 0; plainIdx < plainSize; ++plainIdx)
          operation(plainIdx);
      }
    }
  }
}

template <class Operation>
inline void transformDistinct(const size_t plainSize, const SelectList *sel,
                              const bool *__restrict__ nulls,
                              SelectList *selected, bool isnull,
                              Operation operation) {
  SelectList::value_type *__restrict__ res = selected->begin();
  uint64_t counter = 0;
  if (isnull) {
    if (sel) {
      if (!nulls) {
        for (const auto index : *sel) {
          res[counter++] = index;
        }
      } else {
        for (const auto index : *sel) {
          if (!nulls[index]) res[counter++] = index;
        }
      }
    } else {
      if (!nulls) {
        for (uint64_t i = 0; i < plainSize; ++i) {
          res[counter++] = i;
        }
      } else {
        for (uint64_t i = 0; i < plainSize; ++i) {
          if (!nulls[i]) res[counter++] = i;
        }
      }
    }
  } else {
    if (sel) {
      if (!nulls) {
        for (const auto index : *sel) {
          if (operation(index)) res[counter++] = index;
        }
      } else {
        for (const auto index : *sel) {
          if (nulls[index] || operation(index)) res[counter++] = index;
        }
      }
    } else {
      if (!nulls) {
        for (uint64_t i = 0; i < plainSize; ++i) {
          if (operation(i)) res[counter++] = i;
        }
      } else {
        for (uint64_t i = 0; i < plainSize; ++i) {
          if (nulls[i] || operation(i)) res[counter++] = i;
        }
      }
    }
  }
  selected->resize(counter);
}

template <class Operation>
inline void transformDistinct(const size_t plainSize, const SelectList *sel,
                              const bool *__restrict__ nulls1,
                              const bool *__restrict__ nulls2,
                              SelectList *selected, Operation operation) {
  SelectList::value_type *__restrict__ res = selected->begin();
  uint64_t counter = 0;
  if (sel) {
    if (!nulls1) {
      if (!nulls2) {
        for (const auto index : *sel) {
          if (operation(index)) {
            res[counter++] = index;
          }
        }
      } else {
        for (const auto index : *sel) {
          if (nulls2[index] || operation(index)) {
            res[counter++] = index;
          }
        }
      }
    } else {
      if (!nulls2) {
        for (const auto index : *sel) {
          if (nulls1[index] || operation(index)) {
            res[counter++] = index;
          }
        }
      } else {
        for (const auto index : *sel) {
          if ((nulls1[index] && !nulls2[index]) ||
              (!nulls1[index] && nulls2[index]) ||
              (!nulls1[index] && !nulls2[index] && operation(index))) {
            res[counter++] = index;
          }
        }
      }
    }
  } else {
    if (!nulls1) {
      if (!nulls2) {
        for (uint64_t i = 0; i < plainSize; ++i) {
          if (operation(i)) {
            res[counter++] = i;
          }
        }
      } else {
        for (uint64_t i = 0; i < plainSize; ++i) {
          if (nulls2[i] || operation(i)) {
            res[counter++] = i;
          }
        }
      }
    } else {
      if (!nulls2) {
        for (uint64_t i = 0; i < plainSize; ++i) {
          if (nulls1[i] || operation(i)) {
            res[counter++] = i;
          }
        }
      } else {
        for (uint64_t i = 0; i < plainSize; ++i) {
          if ((!nulls1[i] && nulls2[i]) || (nulls1[i] && !nulls2[i]) ||
              (!nulls1[i] && !nulls2[i] && operation(i))) {
            res[counter++] = i;
          }
        }
      }
    }
  }
  selected->resize(counter);
}

struct DecimalVectorRawData {
  explicit DecimalVectorRawData(Vector *vec)
      : plainSize(vec->getNumOfRowsPlain()),
        sel(vec->getSelected()),
        nulls((vec->getNulls())),
        hightbits(reinterpret_cast<int64_t *>(
            vec->getAuxiliaryValueBuffer()->data())),
        lowbits(reinterpret_cast<uint64_t *>(vec->getValueBuffer()->data())),
        scales(
            reinterpret_cast<int64_t *>(vec->getScaleValueBuffer()->data())) {}

  size_t plainSize;
  const SelectList *sel;
  const bool *__restrict__ nulls;
  int64_t *__restrict__ hightbits;
  uint64_t *__restrict__ lowbits;
  int64_t *__restrict__ scales;
};

template <typename FixedSizeType>
struct FixedSizeTypeVectorRawData {
  explicit FixedSizeTypeVectorRawData(Vector *vec)
      : plainSize(vec->getNumOfRowsPlain()),
        sel(vec->getSelected()),
        nulls((vec->getNulls())),
        values(
            reinterpret_cast<FixedSizeType *>(vec->getValueBuffer()->data())) {}

  size_t plainSize;
  const SelectList *sel;
  const bool *__restrict__ nulls;
  FixedSizeType *__restrict__ values;
};

struct VariableSizeTypeVectorRawData {
  explicit VariableSizeTypeVectorRawData(Vector *vec)
      : plainSize(vec->getNumOfRowsPlain()),
        sel(vec->getSelected()),
        nulls(vec->getNulls()),
        valptrs(vec->getValPtrs()),
        lengths(const_cast<uint64_t *>(vec->getLengths())) {}

  size_t plainSize;
  const SelectList *sel;
  const bool *__restrict__ nulls;
  const char **__restrict__ valptrs;
  uint64_t *__restrict__ lengths;
};

struct TimestampVectorRawData {
  explicit TimestampVectorRawData(Vector *vec)
      : plainSize(vec->getNumOfRowsPlain()),
        sel(vec->getSelected()),
        nulls(vec->getNulls()),
        seconds(reinterpret_cast<int64_t *>(vec->getValueBuffer()->data())),
        nanoseconds(
            reinterpret_cast<int64_t *>(vec->getNanosecondsBuffer()->data())) {}

  size_t plainSize;
  const SelectList *sel;
  const bool *__restrict__ nulls;
  int64_t *__restrict__ seconds;
  int64_t *__restrict__ nanoseconds;
};

struct IntervalVectorRawData {
  explicit IntervalVectorRawData(Vector *vec)
      : plainSize(vec->getNumOfRowsPlain()),
        sel(vec->getSelected()),
        nulls(vec->getNulls()),
        timeOffsets(reinterpret_cast<int64_t *>(vec->getValueBuffer()->data())),
        days(reinterpret_cast<int32_t *>(vec->getDayBuffer()->data())),
        months(reinterpret_cast<int32_t *>(vec->getMonthBuffer()->data())) {}

  size_t plainSize;
  const SelectList *sel;
  const bool *__restrict__ nulls;
  int64_t *__restrict__ timeOffsets;
  int32_t *__restrict__ days;
  int32_t *__restrict__ months;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_TRANSFORMER_H_
