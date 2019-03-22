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

#ifndef DBCOMMON_SRC_DBCOMMON_TYPE_TYPE_UTIL_H_
#define DBCOMMON_SRC_DBCOMMON_TYPE_TYPE_UTIL_H_

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "dbcommon/nodes/datum.h"
#include "dbcommon/type/type-kind.h"

namespace dbcommon {

template <typename T>
inline const char *pgTypeName() {
  if (std::is_same<T, int16_t>::value) return "smallint";

  if (std::is_same<T, int32_t>::value) return "integer";

  if (std::is_same<T, int64_t>::value) return "bigint";

  if (std::is_same<T, float>::value) return "real";

  if (std::is_same<T, double>::value) return "double precision";

  return "invalid type";
}

template <class T>
struct TypeMapping;
template <>
struct TypeMapping<int8_t> {
  static const TypeKind type = TINYINTID;
};
template <>
struct TypeMapping<int16_t> {
  static const TypeKind type = SMALLINTID;
};
template <>
struct TypeMapping<int32_t> {
  static const TypeKind type = INTID;
};
template <>
struct TypeMapping<int64_t> {
  static const TypeKind type = BIGINTID;
};
template <>
struct TypeMapping<float> {
  static const TypeKind type = FLOATID;
};
template <>
struct TypeMapping<double> {
  static const TypeKind type = DOUBLEID;
};
template <>
struct TypeMapping<std::string> {
  static const TypeKind type = STRINGID;
};
template <>
struct TypeMapping<Timestamp> {
  static const TypeKind type = TIMESTAMPID;
};

class TypeBase;

// PAType is only used in parser & analyzer
// For other components, Type Entry is used
struct TypeEntry {
  TypeKind id;                     // typeid
  std::string name;                // typename
  int64_t typeLength;              // typelength
  bool ifTypMods;                  // identify if there exist typMods
  std::shared_ptr<TypeBase> type;  // internal type representation
};

typedef std::unordered_map<std::string, TypeEntry> TypeMap;
typedef std::map<dbcommon::TypeKind, TypeEntry> TypeKindMap;

class TypeUtil {
 public:
  TypeUtil();
  virtual ~TypeUtil() {}

  typedef std::unique_ptr<TypeUtil> uptr;

 public:
  // check whether the given info for type exists in type system.
  bool exist(TypeKind id);
  bool exist(const std::string &name);

  // type system access for non PA components
  const TypeEntry *getTypeEntryById(TypeKind id);
  const TypeEntry *getTypeEntryByTypeName(const std::string &typeName);

  static const char *getTypeNameById(TypeKind id) {
    return instance()->getTypeEntryById(id)->name.c_str();
  }

  static TypeUtil *instance();

 private:
  std::string getTypeBaseName(const std::string &name, std::vector<int> *dest);
  void getTypeModName(const std::string &src, const std::string &separator,
                      std::vector<int> *dest);

  void setupTypeTable();

  TypeKindMap TypeIdEntryMap;
  TypeMap TypeEntryMap;

  static TypeUtil *inst;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_TYPE_TYPE_UTIL_H_
