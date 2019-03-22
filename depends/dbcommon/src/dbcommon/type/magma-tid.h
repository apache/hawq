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

#ifndef DBCOMMON_SRC_DBCOMMON_TYPE_MAGMA_TID_H_
#define DBCOMMON_SRC_DBCOMMON_TYPE_MAGMA_TID_H_

#include <climits>
#include <limits>
#include <string>

#include "dbcommon/nodes/datum.h"
#include "dbcommon/type/typebase.h"

namespace dbcommon {

class MagmaTidType : public FixedSizeTypeBase {
 public:
  MagmaTidType() { this->typeKind = MAGMATID; }

  static MagmaTid fromString(const std::string &str) {
    LOG_WARNING(
        "Should not come here! MagmaTid type not supported yet, only "
        "workaround for unittest");
    char *end = NULL;
    errno = 0;
    int64_t rowId = std::strtoll(str.c_str(), &end, 0);  // NOLINT

    if (end == str.c_str()) {
      LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
                "invalid input syntax for integer: \"%s\"", str.c_str());
    } else if (((rowId == LLONG_MAX || rowId == LLONG_MIN) &&
                errno == ERANGE) ||
               rowId > std::numeric_limits<int64_t>::max() ||
               rowId < std::numeric_limits<int64_t>::min()) {
      LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                "value \"%s\" is out of range", str.c_str());
    }

    MagmaTid tid(0, rowId);
    return tid;
  }

  static std::string toString(MagmaTid val) {
    LOG_WARNING(
        "Should not come here! MagmaTid type not supported yet, only "
        "workaround for unittest");
    return std::to_string(val.rowId);
  }

  uint64_t getTypeWidth() const override { return kWidth; }

  std::string DatumToString(const Datum &d) const override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "MagmaTid type not supported yet");
  }

  std::string DatumToBinary(const Datum &d) const override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "MagmaTid type not supported yet");
  }

  int compare(const Datum &a, const Datum &b) const override {
    auto v1 = *DatumGetValue<MagmaTid *>(a);
    auto v2 = *DatumGetValue<MagmaTid *>(b);

    if (v1 == v2) return 0;
    return v1 < v2 ? -1 : 1;
  }

  int compare(const char *str1, uint64_t len1, const char *str2,
              uint64_t len2) const override {
    assert(len1 == kWidth);
    assert(len2 == kWidth);
    assert(str1 != nullptr && str2 != nullptr);

    auto v1 = *reinterpret_cast<const MagmaTid *>(str1);
    auto v2 = *reinterpret_cast<const MagmaTid *>(str2);

    if (v1 == v2) return 0;
    return v1 < v2 ? -1 : 1;
  }

  Datum getDatum(const char *str) const override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "MagmaTid type not supported yet");
  }

  static const uint64_t kWidth = sizeof(MagmaTid);
};
}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_TYPE_MAGMA_TID_H_
