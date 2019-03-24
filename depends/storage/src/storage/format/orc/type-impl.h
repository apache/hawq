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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_TYPE_IMPL_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_TYPE_IMPL_H_

#include <string>
#include <vector>

#include "storage/format/orc/orc_proto.pb.h"
#include "storage/format/orc/type.h"

namespace orc {

class TypeImpl : public Type {
 private:
  TypeImpl* parent;
  mutable int64_t columnId;
  mutable int64_t maximumColumnId;
  ORCTypeKind kind;
  std::vector<Type*> subTypes;
  std::vector<std::string> fieldNames;
  uint64_t subtypeCount;
  uint64_t maxLength;
  uint64_t precision;
  uint64_t scale;

 public:
  // Create most of the primitive types.
  explicit TypeImpl(ORCTypeKind kind);

  // Create char and varchar type.
  TypeImpl(ORCTypeKind kind, uint64_t maxLength);

  // Create decimal type.
  TypeImpl(ORCTypeKind kind, uint64_t precision, uint64_t scale);

  virtual ~TypeImpl();

  uint64_t getColumnId() const override;

  uint64_t getMaximumColumnId() const override;

  ORCTypeKind getKind() const override;

  uint64_t getSubtypeCount() const override;

  const Type* getSubtype(uint64_t i) const override;

  const std::string& getFieldName(uint64_t i) const override;

  uint64_t getMaximumLength() const override;

  uint64_t getPrecision() const override;

  uint64_t getScale() const override;

  std::string toString() const override;

  Type* addStructField(const std::string& fieldName,
                       std::unique_ptr<Type> fieldType) override;
  Type* addUnionChild(std::unique_ptr<Type> fieldType) override;

  std::unique_ptr<ColumnVectorBatch> createRowBatch(
      uint64_t size, dbcommon::MemoryPool& pool) const override;

  // Explicitly set the column ids. Only for internal usage.
  void setIds(uint64_t columnId, uint64_t maxColumnId);

  // Add a child type.
  void addChildType(std::unique_ptr<Type> childType);

  uint64_t assignIds(uint64_t rootId) const override;

 private:
  // Ensure that ids are assigned to all of the nodes.
  void ensureIdAssigned() const;
};

std::unique_ptr<Type> convertType(const proto::Type& type,
                                  const proto::Footer& footer);

// Build a clone of the file type, projecting columns from the selected
// vector. This routine assumes that the parent of any selected column
// is also selected.
// @param fileType the type in the file
// @param selected is each column by id selected
// @return a clone of the fileType filtered by the selection array
std::unique_ptr<Type> buildSelectedType(const Type* fileType,
                                        const std::vector<bool>& selected);
}  // namespace orc

#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_TYPE_IMPL_H_
