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

#include <cassert>

#include "dbcommon/common/tuple-desc.h"
#include "dbcommon/type/type-modifier.h"

namespace dbcommon {

TupleDesc::TupleDesc() {}

TupleDesc::~TupleDesc() {}

size_t TupleDesc::getNumOfColumns() const { return columnNames.size(); }

const std::string &TupleDesc::getColumnName(size_t idx) const {
  return columnNames.at(idx);
}

TypeKind TupleDesc::getColumnType(size_t idx) const {
  return columnTypes.at(idx);
}

uint64_t TupleDesc::getColumnTypeModifier(size_t idx) const {
  return columnTypeModifiers.at(idx);
}

char TupleDesc::getColumnNullable(size_t idx) const {
  return columnNullables.at(idx);
}

void TupleDesc::add(const std::string &field, dbcommon::TypeKind type,
                    int64_t typeMod, bool nullable) {
  if (type == TypeKind::VARCHARID && typeMod == -1) type = TypeKind::STRINGID;
  columnNames.push_back(field);
  columnTypes.push_back(type);
  columnNullables.push_back(nullable);
  // todo: a trim workaround
  if (field == "Dummy" && type == CHARID && typeMod == -1)
    typeMod = TypeModifierUtil::getTypeModifierFromMaxLength(0);
  columnTypeModifiers.push_back(typeMod);
}

void TupleDesc::removeColumn(size_t idx) {
  assert(idx < columnTypes.size());
  columnNames.erase(columnNames.begin() + idx);
  columnTypes.erase(columnTypes.begin() + idx);
  columnNullables.erase(columnNullables.begin() + idx);
  columnTypeModifiers.erase(columnTypeModifiers.begin() + idx);
}

std::vector<uint32_t> TupleDesc::getFixedLengths() const {
  std::vector<uint32_t> ret;
  for (auto i = 0; i < columnTypes.size(); i++) {
    uint32_t fixedLength;
    switch (columnTypes[i]) {
      case dbcommon::TypeKind::BOOLEANID:
        fixedLength = sizeof(char);
        break;

      case dbcommon::TypeKind::TINYINTID:
        fixedLength = sizeof(int8_t);
        break;

      case dbcommon::TypeKind::SMALLINTID:
        fixedLength = sizeof(int16_t);
        break;

      case dbcommon::TypeKind::INTID:
      case dbcommon::TypeKind::DATEID:
        fixedLength = sizeof(int32_t);
        break;

      case dbcommon::TypeKind::BIGINTID:
      case dbcommon::TypeKind::TIMEID:
        fixedLength = sizeof(int64_t);
        break;

      case dbcommon::TypeKind::TIMESTAMPID:
      case dbcommon::TypeKind::TIMESTAMPTZID:
        fixedLength = sizeof(int64_t) * 2;
        break;

      case dbcommon::TypeKind::FLOATID:
        fixedLength = sizeof(float);
        break;
      case dbcommon::TypeKind::DOUBLEID:
        fixedLength = sizeof(double);
        break;
      case dbcommon::TypeKind::DECIMALNEWID:
        fixedLength = sizeof(uint64_t) * 3;
        break;

      case dbcommon::TypeKind::CHARID:
      case dbcommon::TypeKind::VARCHARID:
      case dbcommon::TypeKind::STRINGID:
      case dbcommon::TypeKind::BINARYID:
      case dbcommon::TypeKind::IOBASETYPEID:
      case dbcommon::TypeKind::STRUCTEXID:
      case dbcommon::TypeKind::DECIMALID:
        fixedLength = 0;
        break;

      default:
        assert(false);  // not supported types
    }
    ret.push_back(fixedLength);
  }
  return ret;
}

}  // namespace dbcommon
