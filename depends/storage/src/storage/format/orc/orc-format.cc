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

#include "storage/format/orc/orc-format.h"

#include <memory>
#include <utility>

#include "dbcommon/log/logger.h"

namespace storage {

void ORCFormat::beginInsert(const std::string &targetName,
                            const dbcommon::TupleDesc &tupleDesc) {
  assert(!targetName.empty());
  assert(params != nullptr);

  writer.reset(new ORCFormatWriter(
      this->fsManager, const_cast<dbcommon::TupleDesc *>(&tupleDesc),
      targetName.c_str(), this->blockAlignSize, params));
  writer->beginWrite();
}

void ORCFormat::doInsert(std::unique_ptr<dbcommon::TupleBatch> tb) {
  writer->write(tb.get());
}

void ORCFormat::endInsert() { writer->endWrite(); }

void ORCFormat::beginUpdate(const std::string &targetName,
                            const dbcommon::TupleDesc &td) {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "UPDATE is not implemented yet");
}

int ORCFormat::doUpdate(std::unique_ptr<dbcommon::TupleBatch> tb) {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "UPDATE is not implemented yet");
}

void ORCFormat::endUpdate() {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "UPDATE is not implemented yet");
}

void ORCFormat::beginDelete(const std::string &targetName,
                            const dbcommon::TupleDesc &td) {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "DELETE is not implemented yet");
}

void ORCFormat::doDelete(std::unique_ptr<dbcommon::TupleBatch> tb) {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "DELETE is not implemented yet");
}

void ORCFormat::endDelete() {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "DELETE is not implemented yet");
}

void ORCFormat::beginScan(const univplan::UnivPlanScanFileSplitListList *splits,
                          const dbcommon::TupleDesc *tupleDesc,
                          const std::vector<bool> *projectionCols,
                          const univplan::UnivPlanExprPolyList *filterExprs,
                          const FormatContext *formatContext,
                          bool readStatsOnly) {
  this->splits = splits;
  if (this->splits != nullptr) {
    assert(tupleDesc != nullptr);
    assert(params != nullptr);
    std::string tableOptionStr = params->get("table.options", "");
    assert(!tableOptionStr.empty());
    Json::Reader jreader;
    Json::Value root;
    if (!jreader.parse(tableOptionStr, root))
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "jsoncpp failed to parse \'%s\'",
                tableOptionStr.c_str());

    reader.reset(new ORCFormatReader());

    assert(fsManager != nullptr);

    reader->beginRead(fsManager, splits,
                      const_cast<std::vector<bool> *>(projectionCols),
                      this->nTuplesPerBatch,
                      const_cast<univplan::UnivPlanExprPolyList *>(filterExprs),
                      tupleDesc, readStatsOnly);
  }
}

dbcommon::TupleBatch::uptr ORCFormat::next() {
  if (splits != nullptr) {
    dbcommon::TupleBatch::uptr result = reader->read();
    assert(!result || result->isValid());
    return std::move(result);
  }

  return dbcommon::TupleBatch::uptr();
}

void ORCFormat::endScan() {
  if (splits != nullptr) {
    reader->endRead();
  }
}

void ORCFormat::reScan() {
  if (this->splits != nullptr) {
    assert(reader != nullptr);
    reader->reset();
  }
}

void ORCFormat::stopScan() {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "stopScan is not implemented yet");
}

}  // namespace storage
