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

#include "dbcommon/type/type-util.h"

#include <cassert>

#include "dbcommon/type/array.h"
#include "dbcommon/type/bool.h"
#include "dbcommon/type/date.h"
#include "dbcommon/type/decimal.h"
#include "dbcommon/type/float.h"
#include "dbcommon/type/integer.h"
#include "dbcommon/type/interval.h"
#include "dbcommon/type/magma-tid.h"
#include "dbcommon/type/varlen.h"
#include "dbcommon/utils/string-util.h"

// [[[cog
#if false
from cog import out, outl
import sys, os
python_path = os.path.dirname(cog.inFile) + "/../python"
python_path = os.path.abspath(python_path)
sys.path.append(python_path)
from code_generator import *
cog.outl("""
    /*
     * DO NOT EDIT!"
     * This file is generated from : %s
     */
    """ % cog.inFile)
#endif
// ]]]
// [[[end]]]

namespace dbcommon {

TypeUtil *TypeUtil::inst = new TypeUtil();

TypeUtil::TypeUtil() {
  inst = this;
  setupTypeTable();
}

void TypeUtil::setupTypeTable() {
  // Refer to https://www.postgresql.org/docs/current/static/datatype.html
  // nullptr for type field means not implemented yet
  std::vector<TypeEntry> TypeEntryArray = {
      {TINYINTID, "int8", 1, false,
       std::shared_ptr<TypeBase>(new TinyIntType())},
      {SMALLINTID, "int16", 2, false,
       std::shared_ptr<TypeBase>(new SmallIntType())},
      {INTID, "int32", 4, false, std::shared_ptr<TypeBase>(new IntType())},
      {BIGINTID, "int64", 8, false,
       std::shared_ptr<TypeBase>(new BigIntType())},
      {FLOATID, "float", 4, false, std::shared_ptr<TypeBase>(new FloatType())},
      {DOUBLEID, "double", 8, false,
       std::shared_ptr<TypeBase>(new DoubleType())},
      {DECIMALID, "decimal", VAR_TYPE_LENGTH, false,
       std::shared_ptr<TypeBase>(new NumericType())},
      {DECIMALNEWID, "decimal_new", 24, false,
       std::shared_ptr<TypeBase>(new DecimalType())},
      {TIMESTAMPID, "timestamp", 16, false,
       std::shared_ptr<TypeBase>(new TimestampType())},
      {TIMESTAMPTZID, "timestamptz", 16, false,
       std::shared_ptr<TypeBase>(new TimestamptzType())},
      {DATEID, "date", 4, false, std::shared_ptr<TypeBase>(new DateType())},
      {TIMEID, "time", 8, false, std::shared_ptr<TypeBase>(new TimeType())},
      {TIMETZID, "timetz", 8, false, nullptr},
      {INTERVALID, "interval", 8, false,
       std::shared_ptr<TypeBase>(new IntervalType())},
      {STRINGID, "string", VAR_TYPE_LENGTH, false,
       std::shared_ptr<TypeBase>(new StringType())},
      {VARCHARID, "varchar", VAR_TYPE_LENGTH, true,
       std::shared_ptr<TypeBase>(new VaryingCharType())},
      {CHARID, "bpchar", VAR_TYPE_LENGTH, true,
       std::shared_ptr<TypeBase>(new BlankPaddedCharType())},
      {BOOLEANID, "boolean", 1, false,
       std::shared_ptr<TypeBase>(new BooleanType())},
      {BINARYID, "binary", VAR_TYPE_LENGTH, false,
       std::shared_ptr<TypeBase>(new BinaryType())},
      {ARRAYID, "array", VAR_TYPE_LENGTH, true, nullptr},
      {MAPID, "map", VAR_TYPE_LENGTH, false, nullptr},
      {STRUCTID, "struct", VAR_TYPE_LENGTH, false,
       std::shared_ptr<TypeBase>(new StructType())},
      {UNIONID, "union", VAR_TYPE_LENGTH, false, nullptr},
      {SMALLINTARRAYID, "smallintarray", VAR_TYPE_LENGTH, false,
       std::shared_ptr<TypeBase>(new SmallIntArrayType())},
      {INTARRAYID, "intarray", VAR_TYPE_LENGTH, false,
       std::shared_ptr<TypeBase>(new IntArrayType())},
      {BIGINTARRAYID, "bigintarray", VAR_TYPE_LENGTH, false,
       std::shared_ptr<TypeBase>(new BigIntArrayType())},
      {FLOATARRAYID, "floatarray", VAR_TYPE_LENGTH, false,
       std::shared_ptr<TypeBase>(new FloatArrayType())},
      {DOUBLEARRAYID, "doublearray", VAR_TYPE_LENGTH, false,
       std::shared_ptr<TypeBase>(new DoubleArrayType())},
      {STRINGARRAYID, "textarray", VAR_TYPE_LENGTH, false,
       std::shared_ptr<TypeBase>(new StringArrayType())},
      {BPCHARARRAYID, "bpchararray", VAR_TYPE_LENGTH, false,
       std::shared_ptr<TypeBase>(new BpcharArrayType())},
      {DECIMAL128ARRAYID, "decimal128array", VAR_TYPE_LENGTH, false,
       std::shared_ptr<TypeBase>(new Decimal128ArrayType())},
      {ANYID, "any", VAR_TYPE_LENGTH, false, nullptr},
      {UNKNOWNID, "unknown", VAR_TYPE_LENGTH, false,
       std::shared_ptr<TypeBase>(new UnknownType())},
      {STRUCTEXID, "structex", VAR_TYPE_LENGTH, false,
       std::shared_ptr<TypeBase>(new StructExType())},
      {IOBASETYPEID, "iobasetype", VAR_TYPE_LENGTH, false,
       std::shared_ptr<TypeBase>(new IOBaseType())},
      {MAGMATID, "magmatid", 16, false,
       std::shared_ptr<TypeBase>(new MagmaTidType())},
  };

  for (uint64_t i = 0; i < TypeEntryArray.size(); ++i) {
    TypeIdEntryMap.insert(
        std::make_pair(TypeEntryArray[i].id, TypeEntryArray[i]));
    TypeEntryMap.insert(
        std::make_pair(TypeEntryArray[i].name, TypeEntryArray[i]));
  }
}

std::string TypeUtil::getTypeBaseName(const std::string &name,
                                      std::vector<int> *dest) {
  if (name.empty()) {
    return name;
  }
  std::string str = name;
  std::string baseStr = name;
  std::string::size_type start = 0, index;
  index = str.find("(");
  if (index != std::string::npos) {
    baseStr = str.substr(start, index);
    getTypeModName(str.substr(index + 1, str.size() - 1), ",", dest);
  }
  return dbcommon::StringUtil::trim(baseStr);
}

void TypeUtil::getTypeModName(const std::string &src,
                              const std::string &separator,
                              std::vector<int> *dest) {
  if (src.empty()) {
    return;
  }
  std::string str = src;
  std::string subString;
  std::string::size_type start = 0, index;

  do {
    index = str.find_first_of(separator, start);
    if (index != std::string::npos) {
      subString = str.substr(start, index - start);
      dest->push_back(atoi(subString.c_str()));
      start = str.find_first_not_of(separator, index);
      if (start == std::string::npos) return;
    }
  } while (index != std::string::npos);

  // the last token
  subString = str.substr(start);
  dest->push_back(atoi(subString.c_str()));
}

const TypeEntry *TypeUtil::getTypeEntryById(TypeKind id) {
  TypeKindMap::const_iterator iter = TypeIdEntryMap.find(id);
  if (iter != TypeIdEntryMap.end()) {
    if (nullptr == iter->second.type)
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "type %d not supported yet", id);
    return &(iter->second);
  }
  return nullptr;
}

const TypeEntry *TypeUtil::getTypeEntryByTypeName(const std::string &typeName) {
  TypeMap::const_iterator iter =
      TypeEntryMap.find(dbcommon::StringUtil::lower(typeName));
  if (iter != TypeEntryMap.end()) {
    if (nullptr == iter->second.type)
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "type %s not supported yet",
                typeName.c_str());
    return &(iter->second);
  }
  return nullptr;
}

bool TypeUtil::exist(TypeKind id) {
  auto it = TypeIdEntryMap.find(id);
  if (TypeIdEntryMap.end() != it) {
    return true;
  }
  return false;
}

bool TypeUtil::exist(const std::string &name) {
  std::unique_ptr<std::vector<int>> typMods(new std::vector<int>());
  std::string baseName = getTypeBaseName(name, typMods.get());
  const TypeMap::const_iterator iter =
      TypeEntryMap.find(dbcommon::StringUtil::lower(baseName));
  if (iter != TypeEntryMap.end()) {
    if (iter->second.id == BOOLEANID || iter->second.id == UNKNOWNID ||
        iter->second.type == nullptr)
      return false;
    return true;
  } else {
    return false;
  }
}

TypeUtil *TypeUtil::instance() { return inst; }

}  // namespace dbcommon
