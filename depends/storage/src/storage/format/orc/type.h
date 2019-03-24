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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_TYPE_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_TYPE_H_

#include <string>

#include "dbcommon/utils/memory-pool.h"

namespace orc {
enum ORCTypeKind {
  BOOLEAN = 0,
  BYTE = 1,
  SHORT = 2,
  INT = 3,
  LONG = 4,
  FLOAT = 5,
  DOUBLE = 6,
  STRING = 7,
  BINARY = 8,
  TIMESTAMP = 9,
  LIST = 10,
  MAP = 11,
  STRUCT = 12,
  UNION = 13,
  DECIMAL = 14,
  DATE = 15,
  VARCHAR = 16,
  CHAR = 17,
  TIME = 18,
  TYPE_INVALID = -1
};

struct ColumnVectorBatch;

class Type {
 public:
  virtual ~Type();
  virtual uint64_t getColumnId() const = 0;
  virtual uint64_t getMaximumColumnId() const = 0;
  virtual ORCTypeKind getKind() const = 0;
  virtual uint64_t getSubtypeCount() const = 0;
  virtual const Type* getSubtype(uint64_t childId) const = 0;
  virtual const std::string& getFieldName(uint64_t childId) const = 0;
  virtual uint64_t getMaximumLength() const = 0;
  virtual uint64_t getPrecision() const = 0;
  virtual uint64_t getScale() const = 0;
  virtual std::string toString() const = 0;

  // Create a row batch for this type.
  virtual std::unique_ptr<ColumnVectorBatch> createRowBatch(
      uint64_t size, dbcommon::MemoryPool& pool) const = 0;  // NOLINT

  // Add a new field to a struct type.
  // @param fieldName the name of the new field
  // @param fieldType the type of the new field
  // @return a reference to the struct type
  virtual Type* addStructField(const std::string& fieldName,
                               std::unique_ptr<Type> fieldType) = 0;

  // Add a new child to a union type.
  // @param fieldType the type of the new field
  // @return a reference to the union type
  virtual Type* addUnionChild(std::unique_ptr<Type> fieldType) = 0;

  // Assign ids to this node and its children giving this
  // node rootId.
  // @param rootId the column id that should be assigned to this node.
  virtual uint64_t assignIds(uint64_t rootId) const = 0;
};

const int64_t DEFAULT_DECIMAL_SCALE = 18;
const int64_t DEFAULT_DECIMAL_PRECISION = 38;

std::unique_ptr<Type> createPrimitiveType(ORCTypeKind kind);
std::unique_ptr<Type> createCharType(ORCTypeKind kind, uint64_t maxLength);
std::unique_ptr<Type> createDecimalType(
    uint64_t precision = DEFAULT_DECIMAL_PRECISION,
    uint64_t scale = DEFAULT_DECIMAL_SCALE);

std::unique_ptr<Type> createStructType();
std::unique_ptr<Type> createListType(std::unique_ptr<Type> elements);
std::unique_ptr<Type> createMapType(std::unique_ptr<Type> key,
                                    std::unique_ptr<Type> value);
std::unique_ptr<Type> createUnionType();

}  // namespace orc
#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_TYPE_H_
