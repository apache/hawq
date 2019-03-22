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

#ifndef DBCOMMON_SRC_DBCOMMON_COMMON_TUPLE_DESC_H_
#define DBCOMMON_SRC_DBCOMMON_COMMON_TUPLE_DESC_H_

#include <memory>
#include <string>
#include <vector>

#include "dbcommon/type/type-kind.h"

namespace dbcommon {

class TupleDesc {
 public:
  TupleDesc();

  ~TupleDesc();

  size_t getNumOfColumns() const;

  const std::string &getColumnName(size_t idx) const;

  dbcommon::TypeKind getColumnType(size_t idx) const;

  uint64_t getColumnTypeModifier(size_t idx) const;

  char getColumnNullable(size_t idx) const;

  void add(const std::string &field, dbcommon::TypeKind type,
           int64_t typeMod = -1, bool nullable = true);

  std::vector<std::string> &getColumnNames() { return columnNames; }

  std::vector<dbcommon::TypeKind> &getColumnTypes() { return columnTypes; }

  std::vector<int64_t> &getColumnTypeModifiers() { return columnTypeModifiers; }

  std::vector<char> &getColumnNullables() { return columnNullables; }

  std::vector<uint32_t> getFixedLengths() const;

  typedef std::unique_ptr<TupleDesc> uptr;

 private:
  std::vector<std::string> columnNames;
  std::vector<dbcommon::TypeKind> columnTypes;
  std::vector<int64_t> columnTypeModifiers;
  std::vector<char> columnNullables;
};
}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_COMMON_TUPLE_DESC_H_
