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

#ifndef DBCOMMON_SRC_DBCOMMON_FUNCTION_AGG_FUNC_H_
#define DBCOMMON_SRC_DBCOMMON_FUNCTION_AGG_FUNC_H_

#include <memory>
#include <string>
#include <vector>

#include "dbcommon/function/invoker.h"
#include "dbcommon/type/decimal.h"
#include "dbcommon/utils/flat-memory-buffer.h"
#include "dbcommon/utils/macro.h"

namespace dbcommon {

struct AccPrimitiveTransData {
  Datum value;
  bool isNotNull;
};
struct AvgPrimitiveTransData {
  double sum;
  uint64_t count;
  // the order of sum and count affect the final function call
};
union AggPrimitiveGroupValue {
  AccPrimitiveTransData accVal;
  AvgPrimitiveTransData avgVal;
};

struct AccDecimalTransData {
  DecimalVar value;
  bool isNotNull;
};
struct AvgDecimalTransData {
  DecimalVar sum;
  uint64_t count;
};
union AggDecimalGroupValue {
  AccDecimalTransData accVal;
  AvgDecimalTransData avgVal;
};

struct AccTimestampTransData {
  Timestamp value;
  bool isNotNull;
  uint64_t padding;
};
union AggTimestampGroupValue {
  AccTimestampTransData accVal;
};

struct StrTransData {
  uint64_t length;
  char str[];
};
union AccStringTransData {
  StrTransData *value;
  void *isNotNull;  // a dummy to keep consistent with other AccTransData
};
union AggStringGroupValue {
  AccStringTransData accVal;
};

class AggGroupValues {
 public:
  AggGroupValues() {}
  virtual ~AggGroupValues() {}

  virtual bool isSmallScale() = 0;

  virtual uint64_t size() = 0;

  virtual void resize(uint64_t newSize) = 0;

  virtual double getMemUsed() = 0;

  // @return true when used as average data but not accumulate data
  bool isAvgData() { return isAvgData_; }
  void setisAvgData(bool isAvgData) { isAvgData_ = isAvgData; }

  virtual bool isString() { return false; }

  virtual bool isDecimal() { return false; }

  virtual bool isTimestamp() { return false; }

  virtual bool isPrimitive() { return false; }

  static force_inline StrTransData *newStrTransData(
      const char *__restrict__ str, uint64_t length) {
    char *buf = cnmalloc(sizeof(length) + length);
    StrTransData *ret = reinterpret_cast<StrTransData *>(buf);
    ret->length = length;
    char *__restrict__ ptr = ret->str;
    char *ptrEnd = ptr + length;
    while (ptr < ptrEnd) {
      *ptr++ = *str++;
    }
    return ret;
  }

 private:
  bool isAvgData_ = false;
};

template <typename AggGroupValue>
class AggGroupValuesTemplate : public AggGroupValues {
 public:
  bool isSmallScale() override {
#ifdef NDEBUG
    return size() <= MaxSingleBlkGroupNum;
#else
    // Here is a trick for testing in develop environment and code coverage
    return size() <= 2;
#endif
  }

  double getMemUsed() override { return data_.getMemUsed(); }

  uint64_t size() override { return data_.size(); }

  // Initialize avgVal.count and avgVal.sum to zero
  // Initialize accVal.isNotNull to false
  void resize(uint64_t newSize) override {
    uint64_t oldSize = data_.size();
    data_.resize(newSize);

    if (newSize > oldSize) data_.memZero(oldSize, newSize);
  }

  template <typename Accessor>
  Accessor getAccessor() {
    if (std::is_same<QuickAccessor, Accessor>::value) assert(isSmallScale());
    return Accessor(data_);
  }

 protected:
  typedef FlatMemBuf<AggGroupValue, DEFAULT_SIZE_PER_AGG_COUNTER_BLK>
      AggGroupValueBuf;
  typedef typename AggGroupValueBuf::NormalAccessor Accessor;
  typedef typename AggGroupValueBuf::QuickAccessor QuickAccessor;

  AggGroupValueBuf data_;
  static const uint64_t MaxSingleBlkGroupNum = AggGroupValueBuf::BlkSize;
};

class AggPrimitiveGroupValues
    : public AggGroupValuesTemplate<AggPrimitiveGroupValue> {
 public:
  typedef typename AggGroupValuesTemplate<AggPrimitiveGroupValue>::QuickAccessor
      QuickAccessor;
  typedef typename AggGroupValuesTemplate<AggPrimitiveGroupValue>::Accessor
      Accessor;

  virtual bool isPrimitive() { return true; }
};

class AggStringGroupValues
    : public AggGroupValuesTemplate<AggStringGroupValue> {
 public:
  typedef
      typename AggGroupValuesTemplate<AggStringGroupValue>::Accessor Accessor;
  typedef typename AggGroupValuesTemplate<AggStringGroupValue>::QuickAccessor
      QuickAccessor;

  ~AggStringGroupValues();

  bool isString() override { return true; }
};

class AggTimestampGroupValues
    : public AggGroupValuesTemplate<AggTimestampGroupValue> {
 public:
  typedef typename AggGroupValuesTemplate<AggTimestampGroupValue>::Accessor
      Accessor;
  typedef typename AggGroupValuesTemplate<AggTimestampGroupValue>::QuickAccessor
      QuickAccessor;

  bool isTimestamp() override { return true; }
};

class AggDecimalGroupValues
    : public AggGroupValuesTemplate<AggDecimalGroupValue> {
 public:
  typedef
      typename AggGroupValuesTemplate<AggDecimalGroupValue>::Accessor Accessor;
  typedef typename AggGroupValuesTemplate<AggDecimalGroupValue>::QuickAccessor
      QuickAccessor;

  bool isDecimal() override { return true; }
};

Datum count_star(Datum *params, uint64_t size);

Datum count_inc(Datum *params, uint64_t size);

Datum count_add(Datum *params, uint64_t size);

Datum sum_int8_sum(Datum *params, uint64_t size);

Datum sum_int16_sum(Datum *params, uint64_t size);

Datum sum_int32_sum(Datum *params, uint64_t size);

Datum sum_int64_sum(Datum *params, uint64_t size);

Datum sum_float_sum(Datum *params, uint64_t size);

Datum sum_double_sum(Datum *params, uint64_t size);

Datum sum_decimal_sum(Datum *params, uint64_t size);

Datum sum_int8_add(Datum *params, uint64_t size);

Datum sum_int16_add(Datum *params, uint64_t size);

Datum sum_int32_add(Datum *params, uint64_t size);

Datum sum_int64_add(Datum *params, uint64_t size);

Datum sum_float_add(Datum *params, uint64_t size);

Datum sum_double_add(Datum *params, uint64_t size);

Datum sum_decimal_add(Datum *params, uint64_t size);

Datum avg_int8_accu(Datum *params, uint64_t size);

Datum avg_int16_accu(Datum *params, uint64_t size);

Datum avg_int32_accu(Datum *params, uint64_t size);

Datum avg_int64_accu(Datum *params, uint64_t size);

Datum avg_float_accu(Datum *params, uint64_t size);

Datum avg_double_accu(Datum *params, uint64_t size);

Datum avg_decimal_accu(Datum *params, uint64_t size);

Datum avg_int8_amalg(Datum *params, uint64_t size);

Datum avg_int16_amalg(Datum *params, uint64_t size);

Datum avg_int32_amalg(Datum *params, uint64_t size);

Datum avg_int64_amalg(Datum *params, uint64_t size);

Datum avg_float_amalg(Datum *params, uint64_t size);

Datum avg_double_amalg(Datum *params, uint64_t size);

Datum avg_decimal_amalg(Datum *params, uint64_t size);

Datum avg_double_avg(Datum *params, uint64_t size);

Datum avg_decimal_avg(Datum *params, uint64_t size);

Datum min_int8_smaller(Datum *params, uint64_t size);

Datum min_int16_smaller(Datum *params, uint64_t size);

Datum min_int32_smaller(Datum *params, uint64_t size);

Datum min_int64_smaller(Datum *params, uint64_t size);

Datum min_float_smaller(Datum *params, uint64_t size);

Datum min_double_smaller(Datum *params, uint64_t size);

Datum min_string_smaller(Datum *params, uint64_t size);

Datum min_date_smaller(Datum *params, uint64_t size);

Datum min_time_smaller(Datum *params, uint64_t size);

Datum min_timestamp_smaller(Datum *params, uint64_t size);

Datum min_decimal_smaller(Datum *params, uint64_t size);

Datum max_int8_larger(Datum *params, uint64_t size);

Datum max_int16_larger(Datum *params, uint64_t size);

Datum max_int32_larger(Datum *params, uint64_t size);

Datum max_int64_larger(Datum *params, uint64_t size);

Datum max_float_larger(Datum *params, uint64_t size);

Datum max_double_larger(Datum *params, uint64_t size);

Datum max_string_larger(Datum *params, uint64_t size);

Datum max_date_larger(Datum *params, uint64_t size);

Datum max_time_larger(Datum *params, uint64_t size);

Datum max_timestamp_larger(Datum *params, uint64_t size);

Datum max_decimal_larger(Datum *params, uint64_t size);

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_FUNCTION_AGG_FUNC_H_
