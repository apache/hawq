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

#ifndef DBCOMMON_SRC_DBCOMMON_TESTUTIL_TUPLE_BATCH_UTILS_H_
#define DBCOMMON_SRC_DBCOMMON_TESTUTIL_TUPLE_BATCH_UTILS_H_

#include <string>
#include <utility>
#include <vector>

#include "dbcommon/common/tuple-batch.h"
#include "dbcommon/common/tuple-desc.h"
#include "dbcommon/testutil/vector-utils.h"
#include "dbcommon/type/type-util.h"
#include "dbcommon/utils/string-util.h"

namespace dbcommon {

class TupleBatchUtility {
 public:
  TupleBatchUtility() {}
  ~TupleBatchUtility() {}

  // Due to historical reason, we keep two form of TupleDesc pattern.
  // You are recommend to use the former one, which is more human-readable.
  // 1. "schema: int8 int16"
  //    refer the type name in TypeUtil::setupTypeTable()
  // 2. "tl"
  static TupleDesc::uptr generateTupleDesc(const std::string& pattern) {
    TupleDesc::uptr desc(new TupleDesc());

    if (pattern.find("schema: ") != std::string::npos) {
      auto dataTypes = StringUtil::split(pattern, ' ');
      for (auto i = 1; i < dataTypes.size(); i++) {
        std::string dataTypeName = dataTypes[i];
        int64_t typeMod = -1;
        if (dataTypeName == "bpchar")
          typeMod = TypeModifierUtil::getTypeModifierFromMaxLength(1);
        if (dataTypeName.find('(') != std::string::npos) {
          int maxLen = std::stoi(dataTypeName.substr(
              dataTypeName.find('(') + 1,
              (dataTypeName.find(')') - dataTypeName.find('(') - 1)));
          typeMod = TypeModifierUtil::getTypeModifierFromMaxLength(maxLen);
          dataTypeName = dataTypeName.substr(0, dataTypeName.find('('));
        }
        auto typeEntry =
            TypeUtil::instance()->getTypeEntryByTypeName(dataTypeName);
        desc->add(dataTypeName + "_" + std::to_string(i), typeEntry->id,
                  typeMod, true);
      }
      return desc;
    }
    for (int charIdx = 0, colIdx = 1; charIdx < pattern.length();
         charIdx++, colIdx++) {
      uint64_t maxLen;
      switch (pattern.at(charIdx)) {
        case 'b':
          desc->add("byte_" + std::to_string(colIdx), TypeKind::BINARYID);
          break;
        case 't':
          desc->add("tinyint_" + std::to_string(colIdx), TypeKind::TINYINTID);
          break;
        case 'h':  // 's' is used for string, so we use 'h' here
          desc->add("short_" + std::to_string(colIdx), TypeKind::SMALLINTID);
          break;
        case 'i':
          desc->add("int32_" + std::to_string(colIdx), TypeKind::INTID);
          break;
        case 'l':
          desc->add("bigint_" + std::to_string(colIdx), TypeKind::BIGINTID);
          break;
        case 'd':
          desc->add("double_" + std::to_string(colIdx), TypeKind::DOUBLEID);
          break;
        case 'f':
          desc->add("float_" + std::to_string(colIdx), TypeKind::FLOATID);
          break;
        case 's':
          desc->add("string_" + std::to_string(colIdx), TypeKind::STRINGID);
          break;
        case 'v':
        case 'c':
          maxLen = 10;
          if (charIdx + 1 < pattern.length()) {
            if (pattern[charIdx + 1] == '(') {
              charIdx += 2;
              std::string str;
              while (pattern[charIdx] != ')') {
                str.append(1, pattern[charIdx]);
                charIdx++;
              }
              maxLen = std::stoi(str);
            }
          }
          desc->add((pattern.at(charIdx) == 'v' ? "varchar_" : "char_") +
                        std::to_string(colIdx),
                    (pattern.at(charIdx) == 'v' ? TypeKind::VARCHARID
                                                : TypeKind::CHARID),
                    TypeModifierUtil::getTypeModifierFromMaxLength(maxLen));
          break;
        case 'B':
          desc->add("bool_" + std::to_string(colIdx), TypeKind::BOOLEANID);
          break;
        case 'D':
          desc->add("date_" + std::to_string(colIdx), TypeKind::DATEID);
          break;
        case 'T':
          desc->add("time_" + std::to_string(colIdx), TypeKind::TIMEID);
          break;
        case 'S':
          desc->add("timestamp_" + std::to_string(colIdx),
                    TypeKind::TIMESTAMPID);
          break;
        default:
          assert(0 == 1 && "wrong pattern");
          break;
      }
    }

    return std::move(desc);
  }

  // generate a tuple batch whose vector is sequential numbers
  static TupleBatch::uptr generateTupleBatch(const TupleDesc& desc,
                                             uint32_t startRowNo,
                                             uint32_t numRows,
                                             bool hasNull = false) {
    TupleBatch::uptr batch(new TupleBatch(desc, true));
    TupleBatchWriter& writer = batch->getTupleBatchWriter();

    for (int colIdx = 0; colIdx < batch->getNumOfColumns(); colIdx++) {
      for (int tupleIdx = startRowNo; tupleIdx < startRowNo + numRows;
           tupleIdx++) {
        writer[colIdx]->append(VectorUtility::convertIntToString(
                                   desc.getColumnType(colIdx), tupleIdx, false),
                               false);
      }
      writer[colIdx]->setHasNull(hasNull);
    }

    batch->incNumOfRows(numRows);

    if (hasNull) {
      batch->appendNull(1);
    }

    assert(batch->isValid());

    return std::move(batch);
  }

  static TupleBatch::uptr generateTupleBatchDuplicate(const TupleDesc& desc,
                                                      uint32_t repeatedNumber,
                                                      uint32_t numRows,
                                                      bool hasNull) {
    TupleBatch::uptr batch(new TupleBatch(desc, true));
    TupleBatchWriter& writer = batch->getTupleBatchWriter();

    for (int colIdx = 0; colIdx < batch->getNumOfColumns(); colIdx++) {
      for (int tupleIdx = 0; tupleIdx < numRows; tupleIdx++) {
        bool isNull = hasNull && std::rand() % 2 == 0;
        writer[colIdx]->append(
            VectorUtility::convertIntToString(desc.getColumnType(colIdx),
                                              repeatedNumber, isNull),
            isNull);
      }
      writer[colIdx]->setHasNull(hasNull);
    }

    batch->incNumOfRows(numRows);
    return std::move(batch);
  }

  static TupleBatch::uptr generateTupleBatchRandom(const TupleDesc& desc,
                                                   uint32_t startRowNo,
                                                   uint32_t numRows,
                                                   bool hasNull,
                                                   bool hasSeleteList = false) {
    TupleBatch::uptr batch(new TupleBatch(desc, true));
    TupleBatchWriter& writer = batch->getTupleBatchWriter();
    SelectList sel;

    for (int colIdx = 0; colIdx < batch->getNumOfColumns(); colIdx++) {
      for (int tupleIdx = startRowNo; tupleIdx < startRowNo + numRows;
           tupleIdx++) {
        bool isNull = hasNull && std::rand() % 2 == 0;
        writer[colIdx]->append(
            VectorUtility::convertIntToString(desc.getColumnType(colIdx),
                                              std::rand() % 32767, isNull),
            isNull);
      }
      writer[colIdx]->setHasNull(hasNull);
    }

    batch->incNumOfRows(numRows);
    if (hasSeleteList) {
      for (int tupleIdx = 0; tupleIdx < numRows; tupleIdx++) {
        if (std::rand() % 2 == 0) sel.push_back(tupleIdx);
      }
      batch->setSelected(sel);
    }

    return std::move(batch);
  }

  static TupleBatch::uptr generateTupleBatchPatch(const TupleDesc& desc,
                                                  uint32_t startRowNo,
                                                  uint32_t numRows,
                                                  bool hasNull) {
    return generateTupleBatchRandom(desc, startRowNo, numRows, hasNull);
  }

  // Generate TupleBatch from std::string representation.
  //
  // (space character) as DELIMITER
  // (newline character) as NEWLINE
  // ("NULL") as NULL value
  static TupleBatch::uptr generateTupleBatch(const TupleDesc& desc,
                                             const std::string& tbStr,
                                             const char delimiter = ' ') {
    TupleBatch::uptr tupleBatch(new TupleBatch(desc, true));
    TupleBatchWriter& writers = tupleBatch->getTupleBatchWriter();
    std::vector<bool> hasnulls(desc.getNumOfColumns(), false);
    auto rows = StringUtil::split(tbStr, '\n');
    for (auto& row : rows) {
      auto fields = StringUtil::split(row, delimiter);
      int colIdx = 0;
      for (auto i = 0; i < fields.size(); i++) {
        std::string field = fields[i];
        if (writers[colIdx]->getTypeKind() == TypeKind::TIMESTAMPID &&
            field != "NULL" && delimiter == ' ') {
          i++;
          field = field + " " + fields[i];
        }
        hasnulls[colIdx] = hasnulls[colIdx] | (field == "NULL");
        writers[colIdx]->append(field == "NULL" ? "" : field, field == "NULL");
        colIdx++;
      }
      assert(colIdx == tupleBatch->getNumOfColumns());
    }
    for (auto colidx = 0; colidx < tupleBatch->getNumOfColumns(); colidx++)
      writers[colidx]->setHasNull(hasnulls[colidx]);
    tupleBatch->setNumOfRows(rows.size());
    return std::move(tupleBatch);
  }

  // Generate TupleBatch from std::string representation.
  //
  // (number) before ':' indicate ctid
  // (space character) as DELIMITER
  // (newline character) as NEWLINE
  // ("NULL") as NULL value
  static TupleBatch::uptr generateTupleBatchWithCtid(const TupleDesc& desc,
                                                     const std::string& tbStr) {
    TupleBatch::uptr tupleBatch(new TupleBatch(desc, true));
    tupleBatch->addSysColumn(dbcommon::TypeKind::MAGMATID, -1, true);
    auto& ctidWriter = tupleBatch->getTupleBatchSysWriter()[0];
    TupleBatchWriter& writers = tupleBatch->getTupleBatchWriter();
    std::vector<bool> hasnulls(desc.getNumOfColumns(), false);
    auto rows = StringUtil::split(tbStr, '\n');
    for (auto& row : rows) {
      auto fields = StringUtil::split(row, ' ');
      int colIdx = 0;
      assert(fields[0].back() == ':');
      ctidWriter->append(fields[0].substr(0, fields[0].size() - 1), false);
      for (auto i = 1; i < fields.size(); i++) {
        std::string field = fields[i];
        if (writers[colIdx]->getTypeKind() == TypeKind::TIMESTAMPID) {
          i++;
          field = field + " " + fields[i];
        }
        hasnulls[colIdx] = hasnulls[colIdx] | (field == "NULL");
        writers[colIdx]->append(field == "NULL" ? "" : field, field == "NULL");
        colIdx++;
      }
      assert(colIdx == tupleBatch->getNumOfColumns());
    }
    for (auto colidx = 0; colidx < tupleBatch->getNumOfColumns(); colidx++)
      writers[colidx]->setHasNull(hasnulls[colidx]);
    tupleBatch->setNumOfRows(rows.size());
    return std::move(tupleBatch);
  }
};
}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_TESTUTIL_TUPLE_BATCH_UTILS_H_
