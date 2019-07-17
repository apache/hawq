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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_ORC_FORMAT_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_ORC_FORMAT_H_

#include <memory>
#include <string>
#include <vector>

#include "jsoncpp/json/json.h"

#include "dbcommon/utils/int-util.h"
#include "storage/format/format.h"
#include "storage/format/orc/orc-format-reader.h"
#include "storage/format/orc/orc-format-writer.h"

namespace storage {

//
// ORCFormat
//
class ORCFormat : public Format {
 public:
  ORCFormat() {}

  // constructor with input parameters map
  explicit ORCFormat(dbcommon::Parameters *params) : params(params) {
    if (params != nullptr) {
      this->blockAlignSize =
          params->getAsInt32("format.block.align.size", Format::kBlockSize);

      if (!dbcommon::isPowerOfTwo(this->blockAlignSize)) {
        LOG_ERROR(ERRCODE_INVALID_PARAMETER_VALUE,
                  "for ORCFormat, "
                  "format.block.align.size can only be power of 2, input is %d",
                  this->blockAlignSize);
      }

      this->nTuplesPerBatch = params->getAsInt32("number.tuples.per.batch",
                                                 Format::kTuplesPerBatch);
      if (this->nTuplesPerBatch % 8 != 0)
        LOG_ERROR(
            ERRCODE_INVALID_PARAMETER_VALUE,
            "for ORCFormat, "
            "number.tuples.per.batch can only be multiples of 8, input is %d",
            this->nTuplesPerBatch);
    }
  }

  virtual ~ORCFormat() {}

  ORCFormat(ORCFormat &&format) = delete;
  ORCFormat(const ORCFormat &format) = delete;
  ORCFormat &operator=(const ORCFormat &format) = delete;
  ORCFormat &operator=(ORCFormat &&format) = delete;

  void beginScan(const univplan::UnivPlanScanFileSplitListList *splits,
                 const dbcommon::TupleDesc *tupleDesc,
                 const std::vector<bool> *projectionCols,
                 const univplan::UnivPlanExprPolyList *filterExpr,
                 const FormatContext *formatContext,
                 bool readStatsOnly) override;

  dbcommon::TupleBatch::uptr next() override;
  void endScan() override;
  void reScan() override;
  void stopScan() override;

  void beginInsert(const std::string &targetName,
                   const dbcommon::TupleDesc &td) override;
  void doInsert(std::unique_ptr<dbcommon::TupleBatch> tb) override;
  void endInsert() override;

  void beginUpdate(const std::string &targetName,
                   const dbcommon::TupleDesc &td) override;
  void doUpdate(std::unique_ptr<dbcommon::TupleBatch> tb) override;
  void endUpdate() override;

  void beginDelete(const std::string &targetName,
                   const dbcommon::TupleDesc &td) override;
  void doDelete(std::unique_ptr<dbcommon::TupleBatch> tb) override;
  void endDelete() override;

 private:
  std::unique_ptr<ORCFormatWriter> writer;
  std::unique_ptr<ORCFormatReader> reader;

  uint32_t blockAlignSize = Format::kBlockSize;
  uint32_t nTuplesPerBatch = Format::kTuplesPerBatch;

  dbcommon::Parameters *params = nullptr;
};

}  // namespace storage

#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_ORC_FORMAT_H_
