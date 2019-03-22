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

#include "dbcommon/type/array.h"

#include <utility>

#include "dbcommon/common/vector/list-vector.h"

namespace dbcommon {

template <typename POD>
std::unique_ptr<Vector> getIntegerArrayScalarFromString(
    const std::string &str) {
  auto ret = dbcommon::Vector::BuildVector(PODTypeKindMapping<POD>::typeKind,
                                           true, -1);
  int offset = 1;  // offset for '{'
  while (offset < str.size() - 1) {
    Scalar val;
    if (offset + 4 < str.size() - 1 &&
        strncmp(str.data() + offset, "NULL", 4) == 0) {
      offset = offset + 5;
      val.isnull = true;
      val.value = CreateDatum(0);
    } else {
      POD value = 0;
      bool isNegative = false;
      if (str[offset] == '-') {
        offset++;
        isNegative = true;
      }
      while ((offset < str.size() - 1) && (str[offset] != ',')) {
        value = value * 10 + str[offset] - '0';
        offset++;
      }
      offset = offset + 1;
      val.isnull = false;
      val.value = isNegative ? CreateDatum(-value) : CreateDatum(value);
    }
    ret->append(&val);
  }
  return ret;
}

template <typename POD>
std::unique_ptr<Vector> getFloatpointArrayScalarFromString(
    const std::string &str) {
  auto ret = dbcommon::Vector::BuildVector(PODTypeKindMapping<POD>::typeKind,
                                           true, -1);
  int offset = 1;  // offset for '{'
  while (offset < str.size() - 1) {
    Scalar val;
    if (offset + 4 < str.size() - 1 &&
        strncmp(str.data() + offset, "NULL", 4) == 0) {
      offset = offset + 5;
      val.isnull = true;
      val.value = CreateDatum(0);
    } else {
      POD value = 0;
      bool isNegative = false;
      if (str[offset] == '-') {
        offset++;
        isNegative = true;
      }
      POD decimal = 10;
      while ((offset < str.size() - 1) && (str[offset] != ',') &&
             (str[offset] != 'e')) {
        if (str[offset] != '.') {
          if (decimal > 1) {
            value = value * 10 + str[offset] - '0';
          } else {
            decimal /= 10;
            value += (str[offset] - '0') * decimal;
          }
        } else {
          decimal /= 10;
        }
        offset++;
      }

      offset = offset + 1;
      val.isnull = false;
      val.value = isNegative ? CreateDatum(-value) : CreateDatum(value);
    }
    ret->append(&val);
  }
  return ret;
}

std::unique_ptr<Vector> SmallIntArrayType::getScalarFromString(
    const std::string &input) {
  return getIntegerArrayScalarFromString<int16_t>(input);
}

std::unique_ptr<Vector> IntArrayType::getScalarFromString(
    const std::string &input) {
  return getIntegerArrayScalarFromString<int32_t>(input);
}

std::unique_ptr<Vector> BigIntArrayType::getScalarFromString(
    const std::string &str) {
  return getIntegerArrayScalarFromString<int64_t>(str);
}

std::unique_ptr<Vector> FloatArrayType::getScalarFromString(
    const std::string &input) {
  return getFloatpointArrayScalarFromString<float>(input);
}

std::unique_ptr<Vector> DoubleArrayType::getScalarFromString(
    const std::string &input) {
  return getFloatpointArrayScalarFromString<double>(input);
}

Datum ArrayType::getDatum(const char *str) const {
  return CreateDatum<const char *>(str);
}

std::string ArrayType::DatumToString(const Datum &d) const {
  char *p = DatumGetValue<char *>(d);
  return std::string(p);
}

std::string ArrayType::DatumToBinary(const Datum &d) const {
  char *p = DatumGetValue<char *>(d);
  return std::string(p);
}

int ArrayType::compare(const Datum &a, const Datum &b) const {
  auto v1 = DatumGetValue<char *>(a);
  auto v2 = DatumGetValue<char *>(b);

  return strcmp(v1, v2);
}

int ArrayType::compare(const char *str1, uint64_t len1, const char *str2,
                       uint64_t len2) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "array type not supported yet");
}

std::unique_ptr<Vector> StringArrayType::getScalarFromString(
    const std::string &str) {
  auto ret = dbcommon::Vector::BuildVector(getBaseTypeKind(), true, -1);
  int offset = 1;
  while (offset < str.size() - 1) {
    Scalar val;
    std::string value;
    if (offset + 4 < str.size() - 1 &&
        strncmp(str.data() + offset, "NULL", 4) == 0) {
      offset = offset + 5;
      val.isnull = true;
    } else {
      if (str[offset] == '"') {
        // escape
        offset++;  // for leftmost double-quote
        while ((offset < str.size() - 1) && (str[offset] != '"')) {
          if (str[offset] == '\\') {
            offset++;
            // FIXME(chiyang): C-like backslash escape sequence
          }
          value.push_back(str[offset]);
          offset++;
        }
        offset++;  // for rightmost double-quote
      } else {
        // no escape
        while ((offset < str.size() - 1) && (str[offset] != ',')) {
          value.push_back(str[offset]);
          offset++;
        }
      }
      offset = offset + 1;
      val.isnull = false;
      val.length = value.length();
      val.value = CreateDatum(value.c_str());
    }
    ret->append(&val);
  }
  return ret;
}

}  // namespace dbcommon
