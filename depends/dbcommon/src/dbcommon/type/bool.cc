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

#include "dbcommon/type/bool.h"

#include <memory>
#include <utility>

#include "dbcommon/common/vector/fixed-length-vector.h"

namespace dbcommon {

Datum BooleanType::getDatum(const char *str) const {
  auto ret = CreateDatum<bool>(fromString(str));
  return ret;
}

std::string BooleanType::DatumToString(const Datum &d) const {
  bool c = DatumGetValue<bool>(d);
  return toString(c);
}

std::string BooleanType::DatumToBinary(const Datum &d) const {
  bool c = DatumGetValue<bool>(d);
  return std::string(reinterpret_cast<char *>(&c), kWidth);
}

int BooleanType::compare(const Datum &a, const Datum &b) const {
  bool v1 = DatumGetValue<bool>(a);
  bool v2 = DatumGetValue<bool>(b);

  return static_cast<int>(v1) - static_cast<int>(v2);
}

int BooleanType::compare(const char *str1, uint64_t len1, const char *str2,
                         uint64_t len2) const {
  assert(len1 == kWidth);
  assert(len2 == kWidth);
  assert(str1 != nullptr && str2 != nullptr);
  bool v1 = *reinterpret_cast<const bool *>(str1);
  bool v2 = *reinterpret_cast<const bool *>(str2);

  return static_cast<int>(v1) - static_cast<int>(v2);
}

}  // namespace dbcommon
