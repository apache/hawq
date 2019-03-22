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

#ifndef DBCOMMON_SRC_DBCOMMON_TYPE_ARRAY_H_
#define DBCOMMON_SRC_DBCOMMON_TYPE_ARRAY_H_

#include <climits>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>

#include "dbcommon/log/logger.h"
#include "dbcommon/type/typebase.h"
#include "dbcommon/type/varlen.h"

namespace dbcommon {

class ArrayType : public VariableLengthArrayTypeBase {
 public:
  Datum getDatum(const char *str) const override;

  std::string DatumToString(const Datum &d) const override;
  std::string DatumToBinary(const Datum &d) const override;
  int compare(const Datum &a, const Datum &b) const override;
  int compare(const char *str1, uint64_t len1, const char *str2,
              uint64_t len2) const override;

  static uint64_t getNumsFromString(const char *str) {
    uint64_t nums = 0;
    for (uint64_t i = 0; i < strlen(str) - 1; i++) {
      if (str[i] == ',') nums++;
    }

    return nums + 1;
  }

  virtual TypeKind getBaseTypeKind() = 0;
  virtual std::unique_ptr<Vector> getScalarFromString(
      const std::string &input) = 0;
};

class SmallIntArrayType : public ArrayType {
 public:
  SmallIntArrayType() { typeKind = SMALLINTARRAYID; }
  virtual ~SmallIntArrayType() {}

  TypeKind getBaseTypeKind() override { return SMALLINTID; }

  std::unique_ptr<Vector> getScalarFromString(
      const std::string &input) override;
};

class IntArrayType : public ArrayType {
 public:
  IntArrayType() { typeKind = INTARRAYID; }
  virtual ~IntArrayType() {}

  TypeKind getBaseTypeKind() override { return INTID; }

  std::unique_ptr<Vector> getScalarFromString(
      const std::string &input) override;
};

class BigIntArrayType : public ArrayType {
 public:
  BigIntArrayType() { typeKind = BIGINTARRAYID; }
  virtual ~BigIntArrayType() {}

  TypeKind getBaseTypeKind() override { return BIGINTID; }

  std::unique_ptr<Vector> getScalarFromString(
      const std::string &input) override;
};

class FloatArrayType : public ArrayType {
 public:
  FloatArrayType() { typeKind = FLOATARRAYID; }
  virtual ~FloatArrayType() {}

  TypeKind getBaseTypeKind() override { return FLOATID; }

  std::unique_ptr<Vector> getScalarFromString(
      const std::string &input) override;
};

class DoubleArrayType : public ArrayType {
 public:
  DoubleArrayType() { typeKind = DOUBLEARRAYID; }
  virtual ~DoubleArrayType() {}

  TypeKind getBaseTypeKind() override { return DOUBLEID; }

  std::unique_ptr<Vector> getScalarFromString(
      const std::string &input) override;
};

class StringArrayType : public ArrayType {
 public:
  StringArrayType() { typeKind = STRINGARRAYID; }
  virtual ~StringArrayType() {}

  TypeKind getBaseTypeKind() override { return STRINGID; }

  std::unique_ptr<Vector> getScalarFromString(
      const std::string &input) override;
};

class BpcharArrayType : public StringArrayType {
 public:
  BpcharArrayType() { typeKind = BPCHARARRAYID; }
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_TYPE_ARRAY_H_
