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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_ORC_FORMAT_WRITER_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_ORC_FORMAT_WRITER_H_

#include <string>

#include "dbcommon/common/tuple-batch.h"
#include "dbcommon/common/tuple-desc.h"
#include "dbcommon/filesystem/file-system-manager.h"
#include "dbcommon/filesystem/file-system.h"
#include "storage/format/orc/writer.h"

namespace storage {

class Parameters;
class TupleBatch;
class ORCFormatWriter {
 public:
  ORCFormatWriter(dbcommon::FileSystemManagerInterface* fsManager,
                  dbcommon::TupleDesc* td, const char* fileName,
                  uint32_t blockAlignSize, dbcommon::Parameters* p);

  virtual ~ORCFormatWriter() {}

  void beginWrite();

  void write(dbcommon::TupleBatch* tb);

  void endWrite();

 private:
  std::unique_ptr<orc::Type> buildSchema(dbcommon::TupleDesc* td);

 private:
  dbcommon::FileSystemManagerInterface* fsManager = nullptr;
  dbcommon::FileSystem* fileSystem = nullptr;
  std::string fileName;
  dbcommon::TupleDesc* desc = nullptr;

  orc::WriterOptions opts;
  std::unique_ptr<orc::Writer> writer;
};

}  // namespace storage

#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_ORC_FORMAT_WRITER_H_
