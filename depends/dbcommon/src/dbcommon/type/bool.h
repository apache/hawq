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

#ifndef DBCOMMON_SRC_DBCOMMON_TYPE_BOOL_H_
#define DBCOMMON_SRC_DBCOMMON_TYPE_BOOL_H_

#include <climits>
#include <cstdint>
#include <limits>
#include <string>

#include "dbcommon/log/logger.h"
#include "dbcommon/type/typebase.h"

namespace dbcommon {

class BooleanType : public FixedSizeTypeBase {
 public:
  BooleanType() { typeKind = BOOLEANID; }

  static inline bool fromString(const std::string &str) {
    assert(str == "t" || str == "f");
    return (str == "t") ? true : false;
  }

  static inline std::string toString(bool val) {
    if (val)
      return "t";
    else
      return "f";
  }

  uint64_t getTypeWidth() const override { return kWidth; }

  Datum getDatum(const char *str) const override;

  std::string DatumToString(const Datum &d) const override;
  std::string DatumToBinary(const Datum &d) const override;
  int compare(const Datum &a, const Datum &b) const override;
  int compare(const char *str1, uint64_t len1, const char *str2,
              uint64_t len2) const override;
  static const uint64_t kWidth = sizeof(bool);
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_TYPE_BOOL_H_
