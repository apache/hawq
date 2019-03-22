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

#ifndef DBCOMMON_SRC_DBCOMMON_TYPE_VARLEN_H_
#define DBCOMMON_SRC_DBCOMMON_TYPE_VARLEN_H_

#include <memory>
#include <string>
#include <utility>

#include "dbcommon/type/typebase.h"

namespace dbcommon {

class Vector;

class VariableLengthArrayTypeBase : public TypeBase {
 public:
  VariableLengthArrayTypeBase() : TypeBase(false) {}
  virtual ~VariableLengthArrayTypeBase() {}

  bool isFixedSizeType() const override { return false; }

  uint64_t getTypeWidth() const override { return maxLenModifier_; }

  static inline const char *fromString(const std::string &str, uint64_t *len) {
    assert(len != nullptr);

    // length do not include the trailing '\0',
    // compatible with external format like orc
    *len = str.length();
    return reinterpret_cast<const char *>(str.c_str());
  }

  static inline std::string toString(const char *val, uint64_t size) {
    return std::move(std::string(val, size));
  }

 protected:
  uint64_t maxLenModifier_ = -1;  // -1 for unlimited
};

class BytesType : public VariableLengthArrayTypeBase {
 public:
  Datum getDatum(const char *str) const override;

  std::string DatumToString(const Datum &d) const override;
  std::string DatumToBinary(const Datum &d) const override;
  int compare(const Datum &a, const Datum &b) const override;
  int compare(const char *str1, uint64_t len1, const char *str2,
              uint64_t len2) const override;
};

class BlankPaddedCharType : public BytesType {
 public:
  explicit BlankPaddedCharType(uint64_t maxLenModifier = 1) {
    typeKind = CHARID;
    maxLenModifier_ = maxLenModifier;
  }
  virtual ~BlankPaddedCharType() {}
};
class VaryingCharType : public BytesType {
 public:
  explicit VaryingCharType(uint64_t maxLenModifier = -1) {
    typeKind = VARCHARID;
    maxLenModifier_ = maxLenModifier;
  }
  virtual ~VaryingCharType() {}
};
class StringType : public BytesType {
 public:
  StringType() { typeKind = STRINGID; }
  virtual ~StringType() {}
};
class BinaryType : public BytesType {
 public:
  BinaryType() { typeKind = BINARYID; }
  virtual ~BinaryType() {}

  Datum getDatum(const char *str) const override;
  std::string DatumToString(const Datum &d) const override;
};
class NumericType : public VariableLengthArrayTypeBase {
 public:
  NumericType() { typeKind = DECIMALID; }
  virtual ~NumericType() {}
  Datum getDatum(const char *str) const override;
  std::string DatumToString(const Datum &d) const override;
  std::string DatumToBinary(const Datum &d) const override;
  int compare(const Datum &a, const Datum &b) const override;
  int compare(const char *str1, uint64_t len1, const char *str2,
              uint64_t len2) const override;
};

class IOBaseType : public BinaryType {
 public:
  IOBaseType() { typeKind = IOBASETYPEID; }
  virtual ~IOBaseType() {}

  std::string DatumToString(const Datum &d) const override;

  int compare(const Datum &a, const Datum &b) const override;

  int compare(const char *str1, uint64_t len1, const char *str2,
              uint64_t len2) const override;
};

class StructExType : public StructType {
 public:
  StructExType() { typeKind = STRUCTEXID; }
  virtual ~StructExType() {}

  bool isFixedSizeType() const override { return false; }

  uint64_t getTypeWidth() const override { return maxLenModifier_; }

  Datum getDatum(const char *str) const override;

  std::string DatumToString(const Datum &d) const override;

  std::string DatumToBinary(const Datum &d) const override;

  int compare(const Datum &a, const Datum &b) const override;

  int compare(const char *str1, uint64_t len1, const char *str2,
              uint64_t len2) const override;

 protected:
  uint64_t maxLenModifier_ = -1;  // -1 for unlimited
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_TYPE_VARLEN_H_
