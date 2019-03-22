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

#ifndef DBCOMMON_SRC_DBCOMMON_TYPE_FLOAT_H_
#define DBCOMMON_SRC_DBCOMMON_TYPE_FLOAT_H_

#include <cerrno>
#include <climits>
#include <cstdint>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string>

#include "dbcommon/log/logger.h"
#include "dbcommon/type/typebase.h"

namespace dbcommon {

template <typename T>
class FloatBaseType : public FixedSizeTypeBase {
 public:
  typedef T base_type;

  static inline base_type fromString(const std::string &str) {
    char *end = NULL;
    errno = 0;
    long double val = std::strtold(str.c_str(), &end);  // NOLINT

    if (end == str.c_str()) {
      LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
                "invalid input syntax for float: \"%s\"", str.c_str());
    } else if ((errno == ERANGE) ||
               val > std::numeric_limits<base_type>::max() ||
               val < std::numeric_limits<base_type>::lowest()) {
      LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                "value \"%s\" is out of range", str.c_str());
    } else if ((val > 0 && val < std::numeric_limits<base_type>::min()) ||
               (val < 0 && -val < std::numeric_limits<base_type>::min())) {
      LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                "value \"%s\" is out of range", str.c_str());
    }

    return val;
  }

  static inline std::string toString(base_type val) {
    int ndig;
    if (std::is_same<T, float>::value)
      ndig = 6;
    else if (std::is_same<T, double>::value)
      ndig = 15;
    else
      assert(false && "should not reach here");
    std::ostringstream out;
    out << std::setprecision(ndig) << val;
    return out.str();
  }

  uint64_t getTypeWidth() const override { return kWidth; }

  std::string DatumToString(const Datum &d) const override {
    auto v = DatumGetValue<base_type>(d);
    return std::to_string(v);
  }

  std::string DatumToBinary(const Datum &d) const override {
    auto v = DatumGetValue<base_type>(d);
    return std::string(reinterpret_cast<char *>(&v), kWidth);
  }

  int compare(const Datum &a, const Datum &b) const override {
    auto v1 = DatumGetValue<base_type>(a);
    auto v2 = DatumGetValue<base_type>(b);

    if (v1 == v2) return 0;
    return v1 < v2 ? -1 : 1;
  }

  int compare(const char *str1, uint64_t len1, const char *str2,
              uint64_t len2) const override {
    assert(len1 == kWidth);
    assert(len2 == kWidth);
    assert(str1 != nullptr && str2 != nullptr);

    auto v1 = *reinterpret_cast<const base_type *>(str1);
    auto v2 = *reinterpret_cast<const base_type *>(str2);

    if (v1 == v2) return 0;
    return v1 < v2 ? -1 : 1;
  }

  static const uint64_t kWidth;
};

template <typename T>
const uint64_t FloatBaseType<T>::kWidth = sizeof(FloatBaseType<T>::base_type);

class FloatType : public FloatBaseType<float> {
 public:
  FloatType() { this->typeKind = FLOATID; }

  Datum getDatum(const char *str) const override {
    auto ret = CreateDatum<float>(fromString(str));
    return ret;
  }
};

class DoubleType : public FloatBaseType<double> {
 public:
  DoubleType() { this->typeKind = DOUBLEID; }

  Datum getDatum(const char *str) const override {
    auto ret = CreateDatum<double>(fromString(str));
    return ret;
  }
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_TYPE_FLOAT_H_
