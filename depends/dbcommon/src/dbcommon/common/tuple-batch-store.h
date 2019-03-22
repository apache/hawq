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

#ifndef DBCOMMON_SRC_DBCOMMON_COMMON_TUPLE_BATCH_STORE_H_
#define DBCOMMON_SRC_DBCOMMON_COMMON_TUPLE_BATCH_STORE_H_

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "dbcommon/common/tuple-batch.h"
#include "dbcommon/filesystem/file-system.h"
#include "dbcommon/utils/byte-buffer.h"

namespace dbcommon {

class NTupleBatchStore {
 public:
  enum Mode { IN_MEMEORY, INPUT, OUTPUT };

  NTupleBatchStore() : mode_(Mode::IN_MEMEORY), buffer_(false) {}

  NTupleBatchStore(FileSystem *filesystem, const std::string &filename,
                   Mode mode)
      : mode_(mode), buffer_(true), filesystem_(filesystem) {
    if (mode == Mode::INPUT) {
      file_ = filesystem_->open(filename, O_RDONLY);
    } else if (mode == Mode::OUTPUT) {
      filesystem_->removeIfExists(filename);
      file_ = filesystem_->open(filename, O_WRONLY);
    }
  }

  ~NTupleBatchStore();

  void writeEOF();

  void reset();

  int64_t getTotalRows() const { return totalRows_; }

  NTupleBatchStore(const NTupleBatchStore &other) = delete;
  NTupleBatchStore &operator=(const NTupleBatchStore &other) = delete;
  NTupleBatchStore(NTupleBatchStore &&other) = delete;
  NTupleBatchStore &operator=(NTupleBatchStore &&other) = delete;

  typedef std::unique_ptr<NTupleBatchStore> uptr;

  void PutIntoNTupleBatchStore(dbcommon::TupleBatch::uptr tb);
  void PutIntoNTupleBatchStore(dbcommon::TupleBatch *tb);

  dbcommon::TupleBatch::uptr GetFromNTupleBatchStore();

 private:
  Mode mode_;
  ByteBuffer buffer_;
  FileSystem *filesystem_ = nullptr;
  std::unique_ptr<File> file_ = nullptr;
  std::vector<std::unique_ptr<TupleBatch>> tbs_;
  int32_t tbsOffset_ = 0;
  int64_t totalRows_ = 0;
};
}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_COMMON_TUPLE_BATCH_STORE_H_
