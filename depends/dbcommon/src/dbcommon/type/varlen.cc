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

#include "dbcommon/type/varlen.h"

#include <algorithm>
#include <cstring>
#include "dbcommon/common/vector/variable-length-vector.h"
#include "dbcommon/type/decimal.h"

namespace dbcommon {

Datum BinaryType::getDatum(const char *srcStr) const {
  LOG_ERROR(ERRCODE_INTERNAL_ERROR,
            "Should not use this function for binary type");
  // Generate a new datum from string will lead to new memory allocate.
  // Refer to ConstExprState::ConstExprState see how to do this
}

std::string BinaryType::DatumToString(const Datum &d) const {
  char *p = DatumGetValue<char *>(d);
  auto srcLen = *reinterpret_cast<uint32_t *>(p);
  auto srcBin = p + sizeof(uint32_t);
  std::string output;
  for (auto i = 0; i < srcLen; i++) {
    unsigned char byte = srcBin[i];
    if (byte == '\\') {
      output.append("\\\\");
    } else if (byte < 0x20 || byte > 0x7e) {
      output.append(1, '\\');
      output.append(1, '0' + byte / 64);
      output.append(1, '0' + byte / 8 % 8);
      output.append(1, '0' + byte % 8);
    } else {
      output.append(1, byte);
    }
  }
  return std::move(output);
}

Datum BytesType::getDatum(const char *str) const {
  return CreateDatum<const char *>(str);
}

std::string BytesType::DatumToString(const Datum &d) const {
  char *p = DatumGetValue<char *>(d);
  return std::string(p);
}

std::string BytesType::DatumToBinary(const Datum &d) const {
  char *p = DatumGetValue<char *>(d);
  return std::string(p);
}

int BytesType::compare(const Datum &a, const Datum &b) const {
  auto v1 = DatumGetValue<char *>(a);
  auto v2 = DatumGetValue<char *>(b);

  return strcmp(v1, v2);
}

int BytesType::compare(const char *str1, uint64_t len1, const char *str2,
                       uint64_t len2) const {
  assert((str1 != nullptr || len1 == 0) && (str2 != nullptr || len2 == 0));
  const unsigned char *__restrict__ arg1 =
      reinterpret_cast<const unsigned char *>(str1);
  const unsigned char *__restrict__ arg2 =
      reinterpret_cast<const unsigned char *>(str2);
  size_t len = std::min(len1, len2);
  for (int i = 0; i < len; ++i) {
    int result = arg1[i] - arg2[i];
    if (result) {
      return result;
    }
  }
  return len1 - len2;
}

//  IOBaseType
std::string IOBaseType::DatumToString(const Datum &d) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "iobase type not supported yet");
}

int IOBaseType::compare(const Datum &a, const Datum &b) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "iobase type not supported yet");
}

int IOBaseType::compare(const char *str1, uint64_t len1, const char *str2,
                        uint64_t len2) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
            "iobase type type not supported yet");
}

//  StructExType
Datum StructExType::getDatum(const char *srcStr) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "structEx type not supported yet");
}

std::string StructExType::DatumToString(const Datum &d) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "structEx type not supported yet");
}

std::string StructExType::DatumToBinary(const Datum &d) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "structEx type not supported yet");
}

int StructExType::compare(const Datum &a, const Datum &b) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "structEx type not supported yet");
}

int StructExType::compare(const char *str1, uint64_t len1, const char *str2,
                          uint64_t len2) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "structEx type not supported yet");
}

Datum NumericType::getDatum(const char *str) const {
  return CreateDatum<const char *>(str);
}

std::string NumericType::DatumToString(const Datum &d) const {
  char *p = DatumGetValue<char *>(d);
  return std::string(p);
}

std::string NumericType::DatumToBinary(const Datum &d) const {
  char *p = DatumGetValue<char *>(d);
  return std::string(p);
}

int NumericType::compare(const Datum &a, const Datum &b) const {
  char *v1 = DatumGetValue<char *>(a);
  char *v2 = DatumGetValue<char *>(b);
  uint64_t len1 = strlen(v1);
  uint64_t len2 = strlen(v2);

  return NumericType::compare(v1, len1, v2, len2);
}

int NumericType::compare(const char *str1, uint64_t len1, const char *str2,
                         uint64_t len2) const {
  std::string srcStr1(str1, len1);
  std::string srcStr2(str2, len2);
  int32_t scale1 = 0, scale2 = 0;
  bool overflow = false;
  int32_t idx1 = srcStr1.find(".");
  int32_t idx2 = srcStr2.find(".");
  if (idx1 != std::string::npos) {
    srcStr1 = srcStr1.substr(0, idx1) + srcStr1.substr(idx1 + 1);
    scale1 = len1 - idx1 - 1;
  }
  if (idx2 != std::string::npos) {
    srcStr2 = srcStr2.substr(0, idx2) + srcStr2.substr(idx2 + 1);
    scale2 = len2 - idx2 - 1;
  }
  Int128 srcValue1 = Int128(srcStr1);
  Int128 srcValue2 = Int128(srcStr2);
  if (scale1 < scale2) {
    srcValue1 = scaleUpInt128ByPowerOfTen(srcValue1, scale2 - scale1);
  } else {
    srcValue2 = scaleUpInt128ByPowerOfTen(srcValue2, scale1 - scale2);
  }
  if (srcValue1 == srcValue2) return 0;
  return srcValue1 < srcValue2 ? -1 : 1;
}

}  // namespace dbcommon
