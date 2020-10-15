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

#ifndef STORAGE_SRC_STORAGE_FORMAT_FORMAT_H_
#define STORAGE_SRC_STORAGE_FORMAT_FORMAT_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "dbcommon/common/tuple-batch.h"
#include "dbcommon/common/tuple-desc.h"
#include "dbcommon/filesystem/file-system-manager.h"
#include "dbcommon/filesystem/file-system.h"
#include "dbcommon/utils/macro.h"
#include "dbcommon/utils/parameters.h"
#include "univplan/common/univplan-type.h"

namespace storage {

class FileSystemManagerInterface;

class Input {
 public:
  Input() {}
  virtual ~Input() {}

  // Get input name
  // @return The input name
  virtual const std::string &getName() const = 0;

  // Get input size
  // @return The input size
  virtual int64_t getSize() const = 0;
};

class FileInput : public Input {
 public:
  FileInput(const char *fileName, int64_t length) {
    this->fileName = fileName;
    this->length = length;
  }
  FileInput(const std::string &fileName, int64_t length) {
    this->fileName = fileName;
    this->length = length;
  }
  FileInput(FileInput &&file) {  // NOLINT
    this->fileName = std::move(file.fileName);
    this->length = file.length;
  }
  FileInput(const FileInput &file) {
    this->fileName = file.fileName;
    this->length = file.length;
  }
  FileInput &operator=(const FileInput &file) {
    this->fileName = file.fileName;
    this->length = file.length;
    return *this;
  }

  virtual ~FileInput() {}

  const std::string &getName() const override { return fileName; }

  int64_t getSize() const override { return length; }

 private:
  std::string fileName;
  int64_t length;
};

// This struct was added to make the foramt interface extensible
typedef struct FormatContext {
  univplan::UnivPlanExprPolyList indexExpr;
} FormatContext;

// Format "read" accepts a list of splits, and return TupleBatches one by one.
// And Format "write" accept TupleBatches, and write them to storage. It is
// quite like InputFormat/OutputFormat of MR.
//
// It is a general concept, not only about concrete file format.
// It can be FAST format files on HDFS, Text files on HDFS,
// even HBase format. So it is extensible.
// Users should be able to write their only format.
// For example, users can write a PostgresqlFormat to read and write data
// to a postgresql server.

class Format {
 public:
  Format() {}

  virtual ~Format() {}

  void setFileSystemManager(dbcommon::FileSystemManagerInterface *fsManager) {
    this->fsManager = fsManager;
  }

  // Begin scan of the splits
  // @param splits The file splits need to be scanned
  // @param tupleDesc The tuple description for the target table
  // @param projectionCols The project columns list
  // @param filterExpr The filter expression
  // @param readStatsOnly To indicate if read only statistics
  // @return void
  virtual void beginScan(const univplan::UnivPlanScanFileSplitListList *splits,
                         const dbcommon::TupleDesc *tupleDesc,
                         const std::vector<bool> *projectionCols,
                         const univplan::UnivPlanExprPolyList *filterExpr,
                         const FormatContext *formatContext,
                         bool readStatsOnly) = 0;

  // Get next TupleBatch
  // @return unique_ptr of dbcommon::TupleBatch
  virtual std::unique_ptr<dbcommon::TupleBatch> next() = 0;

  // End the scan
  // @return void
  virtual void endScan() = 0;

  // Restart the scan
  // @return void
  virtual void reScan() = 0;

  // Stop the scan
  // @return void
  virtual void stopScan() = 0;

  // Begin insert
  // @param targetName The target name. For 'fast' and 'text' format, it
  // is the target file name. For 'hbase' format, it is the target table.
  // @param td The tuple description
  // @return void
  virtual void beginInsert(const std::string &targetName,
                           const dbcommon::TupleDesc &tupleDesc) = 0;

  // Insert a tuple batch
  // @param tb The tuple batch to be inserted
  virtual void doInsert(std::unique_ptr<dbcommon::TupleBatch> tb) = 0;

  // End insert
  virtual void endInsert() = 0;

  // Begin update
  // @param targetName The target name. For 'fast' and 'text' format, it
  // is the target file name. For 'hbase' format, it is the target table.
  // @param td The tuple description
  // @return void
  virtual void beginUpdate(const std::string &targetName,
                           const dbcommon::TupleDesc &tupleDesc) = 0;

  // Update a tuple batch
  // @param tb The tuple batch to be updated
  virtual void doUpdate(std::unique_ptr<dbcommon::TupleBatch> tb) = 0;

  // End update
  virtual void endUpdate() = 0;

  // Begin delete
  // @param targetName The target name. For 'fast' and 'text' format, it
  // is the target file name. For 'hbase' format, it is the target table.
  // @param td The tuple description
  // @return void
  virtual void beginDelete(const std::string &targetName,
                           const dbcommon::TupleDesc &tupleDesc) = 0;

  // Delete a tuple batch
  // @param tb The tuple batch to be deleted
  virtual void doDelete(std::unique_ptr<dbcommon::TupleBatch> tb) = 0;

  // End delete
  virtual void endDelete() = 0;

  // Create tasks given input and the number of workers
  // @param files The input files
  // @param nWorker The number of workers
  // @return The list of tasks, each worker has one task assigned.
  // it is possible if there is no splits in a task
  // when there is no enough splits (each task has a split list)
  virtual std::unique_ptr<univplan::UnivPlanScanFileSplitListList> createTasks(
      const std::vector<std::unique_ptr<Input> > &inputs, int nWorker);

  // set & get user command for external table
  std::string getUserCommand() const { return userCommand; }
  void setUserCommand(std::string command) { userCommand = command; }
  virtual void setCancelled() {}

  virtual void setupHasher(
      const dbcommon::TupleDesc &td, const std::vector<uint64_t> &hashKeys,
      const std::unordered_map<uint16_t, uint32_t> &r2rg,
      const std::unordered_map<uint16_t, std::string> &r2u) {}

  static std::unique_ptr<Format> createFormat(
      univplan::UNIVPLANFORMATTYPE type);
  static std::unique_ptr<Format> createFormat(univplan::UNIVPLANFORMATTYPE type,
                                              dbcommon::Parameters *p);

  static const int kTuplesPerBatch = DEFAULT_NUMBER_TUPLES_PER_BATCH;
  static const int kBlockSize = DEFAULT_BLOCK_SIZE;

 protected:
  // Format does not own splits, so it does not delete it in destructor.
  const univplan::UnivPlanScanFileSplitListList *splits = nullptr;
  // The file system manager used to get the file system
  dbcommon::FileSystemManagerInterface *fsManager = nullptr;
  // user command for external table such as DBGEN
  std::string userCommand = "";
};

}  // namespace storage

#endif  // STORAGE_SRC_STORAGE_FORMAT_FORMAT_H_
