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

#ifndef DBCOMMON_SRC_DBCOMMON_TYPE_TYPEBASE_H_
#define DBCOMMON_SRC_DBCOMMON_TYPE_TYPEBASE_H_

#include <stddef.h>

#include <cassert>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "dbcommon/log/logger.h"
#include "dbcommon/nodes/datum.h"
#include "dbcommon/type/type-kind.h"

#define VAR_TYPE_LENGTH (-1)
namespace dbcommon {
class Vector;

// TypeBase is a base class for all type
class TypeBase {
 public:
  explicit TypeBase(bool passByValue) : passByValue_(passByValue) {}

  virtual ~TypeBase() {}

  bool passByValue() { return this->passByValue_; }

  virtual bool isFixedSizeType() const = 0;
  virtual uint64_t getTypeWidth() const = 0;
  virtual TypeKind getTypeKind() const {
    assert(typeKind != INVALIDTYPEID);
    return typeKind;
  }

  std::unique_ptr<Vector> getVector(bool ownData);

  virtual Datum getDatum(const char* str) const = 0;
  virtual std::string DatumToString(const Datum& d) const = 0;
  virtual std::string DatumToBinary(const Datum& d) const = 0;

  // = return 0, < return negative, > return positive;
  virtual int compare(const Datum& a, const Datum& b) const = 0;
  virtual int compare(const char* str1, uint64_t len1, const char* str2,
                      uint64_t len2) const = 0;

  virtual uint32_t getSubtypeCount() const {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "getSubtypeCount not supported");
  }

  virtual const TypeBase* getSubtype(uint32_t childId) const {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "getSubtype not supported");
  }

  virtual const std::string& getFieldName(uint32_t childId) const {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "getFieldName not supported");
  }

  virtual TypeBase* addStructField(const std::string& fieldName,
                                   TypeBase* fieldType) {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "addStructField not supported");
  }

 protected:
  TypeKind typeKind = INVALIDTYPEID;
  bool passByValue_ = true;
};

// FixedSizeType is the base class for all fixed-size scalar simple types.
class FixedSizeTypeBase : public TypeBase {
 public:
  FixedSizeTypeBase() : TypeBase(true) {}

  bool isFixedSizeType() const override { return true; }

  ~FixedSizeTypeBase() {}
};

// VariableSizeType is the base class for all variable-size scalar simple types
class VariableSizeTypeBase : public TypeBase {
 public:
  VariableSizeTypeBase() : TypeBase(false) {}

  virtual ~VariableSizeTypeBase() {}

  bool isFixedSizeType() const override { return false; }

  uint64_t getTypeWidth() const override { return VARIABLE_TYPE_WIDTH; }

  // here we return 0 for VARIABLE_TYPE_WIDTH
  static const uint64_t VARIABLE_TYPE_WIDTH = 0;
};

class StructType : public TypeBase {
 public:
  StructType() : TypeBase(false) { typeKind = STRUCTID; }
  virtual ~StructType() {}

  bool isFixedSizeType() const override;

  uint64_t getTypeWidth() const override;

  Datum getDatum(const char* str) const override;

  std::string DatumToString(const Datum& d) const override;

  std::string DatumToBinary(const Datum& d) const override;

  int compare(const Datum& a, const Datum& b) const override;

  int compare(const char* str1, uint64_t len1, const char* str2,
              uint64_t len2) const override;

  uint32_t getSubtypeCount() const override { return subtypes.size(); }

  const TypeBase* getSubtype(uint32_t childId) const override {
    return subtypes[childId];
  }

  const std::string& getFieldName(uint32_t childId) const override {
    return fieldNames[childId];
  }

  TypeBase* addStructField(const std::string& fieldName,
                           TypeBase* fieldType) override {
    fieldNames.push_back(fieldName);
    subtypes.push_back(fieldType);
    return subtypes.back();
  }

 private:
  std::vector<TypeBase*> subtypes;
  std::vector<std::string> fieldNames;
};

class MapType : public TypeBase {
 public:
  MapType() : TypeBase(false) { typeKind = MAPID; }

  virtual ~MapType() {}

  bool isFixedSizeType() const override;

  uint64_t getTypeWidth() const override;

  Datum getDatum(const char* str) const override;

  std::string DatumToString(const Datum& d) const override;

  std::string DatumToBinary(const Datum& d) const override;

  int compare(const Datum& a, const Datum& b) const override;

  int compare(const char* str1, uint64_t len1, const char* str2,
              uint64_t len2) const override;
};

class UnionType : public TypeBase {
 public:
  UnionType() : TypeBase(false) { typeKind = UNIONID; }

  virtual ~UnionType() {}

  bool isFixedSizeType() const override;

  uint64_t getTypeWidth() const override;

  Datum getDatum(const char* str) const override;

  std::string DatumToString(const Datum& d) const override;

  std::string DatumToBinary(const Datum& d) const override;

  int compare(const Datum& a, const Datum& b) const override;

  int compare(const char* str1, uint64_t len1, const char* str2,
              uint64_t len2) const override;
};

class AnyType : public TypeBase {
 public:
  AnyType() : TypeBase(false) { typeKind = ANYID; }

  virtual ~AnyType() {}

  bool isFixedSizeType() const override;

  uint64_t getTypeWidth() const override;

  Datum getDatum(const char* str) const override;

  std::string DatumToString(const Datum& d) const override;

  std::string DatumToBinary(const Datum& d) const override;

  int compare(const Datum& a, const Datum& b) const override;

  int compare(const char* str1, uint64_t len1, const char* str2,
              uint64_t len2) const override;
};

class UnknownType : public TypeBase {
 public:
  UnknownType() : TypeBase(false) { typeKind = UNKNOWNID; }

  virtual ~UnknownType() {}

  bool isFixedSizeType() const override;

  uint64_t getTypeWidth() const override;

  Datum getDatum(const char* str) const override;

  std::string DatumToString(const Datum& d) const override;

  std::string DatumToBinary(const Datum& d) const override;

  int compare(const Datum& a, const Datum& b) const override;

  int compare(const char* str1, uint64_t len1, const char* str2,
              uint64_t len2) const override;
};

template <class T>
struct PODTypeKindMapping;
template <>
struct PODTypeKindMapping<int8_t> {
  static const TypeKind typeKind = TINYINTID;
};
template <>
struct PODTypeKindMapping<int16_t> {
  static const TypeKind typeKind = SMALLINTID;
};
template <>
struct PODTypeKindMapping<int32_t> {
  static const TypeKind typeKind = INTID;
};
template <>
struct PODTypeKindMapping<int64_t> {
  static const TypeKind typeKind = BIGINTID;
};
template <>
struct PODTypeKindMapping<float> {
  static const TypeKind typeKind = FLOATID;
};
template <>
struct PODTypeKindMapping<double> {
  static const TypeKind typeKind = DOUBLEID;
};
template <>
struct PODTypeKindMapping<std::string> {
  static const TypeKind typeKind = STRINGID;
};
template <>
struct PODTypeKindMapping<Timestamp> {
  static const TypeKind typeKind = TIMESTAMPID;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_TYPE_TYPEBASE_H_
