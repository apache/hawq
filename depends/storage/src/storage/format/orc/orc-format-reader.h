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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_ORC_FORMAT_READER_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_ORC_FORMAT_READER_H_

#include <string>
#include <vector>

#include "dbcommon/common/tuple-batch.h"
#include "dbcommon/filesystem/file-system-manager.h"
#include "dbcommon/filesystem/file-system.h"
#include "dbcommon/utils/byte-buffer.h"

#include "storage/format/format.h"
#include "storage/format/orc/column-printer.h"
#include "storage/format/orc/reader.h"
#include "storage/format/orc/seekable-input-stream.h"
#include "storage/format/orc/vector.h"

namespace storage {

class ORCFormatReader {
 public:
  ORCFormatReader() {}
  virtual ~ORCFormatReader() {}

  void beginRead(dbcommon::FileSystemManagerInterface *fsManager,
                 const univplan::UnivPlanScanFileSplitListList *splits,
                 std::vector<bool> *columnsToRead, uint32_t nTuplesPerBatch,
                 const univplan::UnivPlanExprPolyList *predicateExprs,
                 const dbcommon::TupleDesc *td, bool readStatsOnly);
  dbcommon::TupleBatch::uptr read();
  void endRead();
  void reset();

 private:
  void startNewSplit();
  dbcommon::TupleBatch::uptr createTupleBatch(orc::ColumnVectorBatch *batch);
  bool hasSomethingToRead();
  void typeCheck(dbcommon::TypeKind colType, const std::string &colName,
                 orc::ColumnVectorBatch *b);

 private:
  const univplan::UnivPlanScanFileSplitListList *splits = nullptr;
  std::vector<bool> *columnsToRead = nullptr;
  dbcommon::FileSystemManagerInterface *fsManager = nullptr;
  uint32_t nTuplesPerBatch = Format::kTuplesPerBatch;

  std::unique_ptr<orc::Reader> orcReader;
  orc::ReaderOptions opts;
  std::unique_ptr<orc::ColumnVectorBatch> batch;
  bool startAnotherSplit = true;
  int32_t currentSplitIdx = -1;

  // count for filter push down
  uint32_t skippedStripe = 0;
  uint32_t scannedStripe = 0;
};

}  // namespace storage

#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_ORC_FORMAT_READER_H_
