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

#ifndef STORAGE_SRC_STORAGE_TESTUTIL_FORMAT_UTIL_H_
#define STORAGE_SRC_STORAGE_TESTUTIL_FORMAT_UTIL_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "dbcommon/common/tuple-desc.h"
#include "dbcommon/filesystem/file-system.h"
#include "dbcommon/filesystem/local/local-file-system.h"
#include "dbcommon/log/logger.h"
#include "dbcommon/testutil/tuple-batch-utils.h"
#include "dbcommon/utils/global.h"

#include "storage/format/format.h"
#include "univplan/testutil/univplan-proto-util.h"

namespace storage {

class FormatUtility {
 public:
  FormatUtility() {}
  ~FormatUtility() {}

  std::unique_ptr<storage::Format> createFormat(
      const std::string &fmt, dbcommon::Parameters *params = nullptr) {
    std::unique_ptr<storage::Format> format;

    if (dbcommon::StringUtil::iequals(fmt, "fast")) {
      // format.reset(new FastFormat(params));
      LOG_ERROR(ERRCODE_INVALID_PARAMETER_VALUE, "invalid format: %s",
                fmt.c_str());
    } else if (dbcommon::StringUtil::iequals(fmt, "orc")) {
      format.reset(new ORCFormat(params));
    } else {
      LOG_ERROR(ERRCODE_INVALID_PARAMETER_VALUE, "invalid format: %s",
                fmt.c_str());
    }

    return std::move(format);
  }

  void writeThenReadCompare(
      const std::string &fmt, dbcommon::TupleDesc *desc,
      dbcommon::TupleBatch::uptr tb, const std::string &localPath,
      std::string paramsStr,
      const univplan::UnivPlanExprPolyList *predicateExprs, bool shouldSkip) {
    EXPECT_EQ(tb->isValid(), true);
    dbcommon::Parameters params;
    if (paramsStr.size() > 0)
      params.set("table.options", paramsStr);
    else
      params.set("table.options",
                 "{\"compresstype\":\"none\",\"rlecoder\":\"v2\", "
                 "\"dicthreshold\":\"0.5\"}");
    std::unique_ptr<storage::Format> format = createFormat(fmt, &params);

    format->setFileSystemManager(&FSManager);

    std::string beforeInsert = tb->toString();

    std::string fullFileName = "file://" + localPath;
    std::string fileName(localPath);
    dbcommon::FileSystem *fs = FSManager.get(fullFileName.c_str());

    if (fs->exists(fileName.c_str())) {
      fs->remove(fileName.c_str());
    }
    int totalWrite = tb->getNumOfRows();
    format->beginInsert(fullFileName, *desc);
    format->doInsert(std::move(tb));
    format->endInsert();

    std::vector<std::unique_ptr<Input> > files;
    std::unique_ptr<dbcommon::FileInfo> info =
        fs->getFileInfo(fileName.c_str());

    std::unique_ptr<Input> file(
        new FileInput(fullFileName.c_str(), info->size));
    files.push_back(std::move(file));
    std::unique_ptr<univplan::UnivPlanScanFileSplitListList> tasks =
        format->createTasks(files, 1);
    format->beginScan(tasks.get(), desc, nullptr, predicateExprs, nullptr,
                      false);

    int totalRead = 0;
    dbcommon::TupleBatch::uptr result = format->next();
    if (shouldSkip) {
      EXPECT_EQ(result.get(), nullptr);
    } else {
      if (result.get() == nullptr)
        throw dbcommon::TransactionAbortException("got zero row",
                                                  ERRCODE_DATA_EXCEPTION);
      EXPECT_EQ(result->isValid(), true);

      format->endScan();

      std::string afterInsert = result->toString();

      EXPECT_EQ(beforeInsert, afterInsert);
    }
    if (fs->exists(fileName.c_str())) {
      fs->remove(fileName.c_str());
    }
  }
  void writeThenReadCompare(const std::string &fmt, dbcommon::TupleDesc *desc,
                            dbcommon::TupleBatch::uptr tb,
                            const std::string &localPath) {
    writeThenReadCompare(fmt, desc, std::move(tb), localPath, "", nullptr,
                         false);
  }

  void writeThenReadCompare(const std::string &fmt, dbcommon::TupleDesc *desc,
                            dbcommon::TupleBatch::uptr tb,
                            const std::string &localPath,
                            std::string paramsStr) {
    writeThenReadCompare(fmt, desc, std::move(tb), localPath, paramsStr,
                         nullptr, false);
  }

  void writeThenReadCompare(
      const std::string &fmt, dbcommon::TupleDesc *desc,
      dbcommon::TupleBatch::uptr tb, const std::string &localPath,
      const univplan::UnivPlanExprPolyList *predicateExprs, bool shouldSkip) {
    writeThenReadCompare(fmt, desc, std::move(tb), localPath, "",
                         predicateExprs, shouldSkip);
  }

  void multiBlockTest(const std::string &fmt, const std::string &pattern,
                      uint64_t start, uint64_t step, uint64_t number,
                      uint64_t nTupleReadPerBatch, uint64_t blockAlignSize,
                      const std::string &localPath) {
    dbcommon::Parameters params;
    params.set("number.tuples.per.batch", std::to_string(nTupleReadPerBatch));
    params.set("format.block.align.size", std::to_string(blockAlignSize));
    params.set("table.options",
               "{\"compresstype\":\"snappy\",\"rlecoder\":\"v2\","
               "\"bloomfilter\":\"0\"}");
    std::unique_ptr<storage::Format> format = createFormat(fmt, &params);

    format->setFileSystemManager(&FSManager);

    dbcommon::TupleBatchUtility tbu;
    dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc(pattern);

    std::string fullFileName = "file://" + localPath;
    std::string fileName(localPath);
    dbcommon::FileSystem *fs = FSManager.get(fullFileName.c_str());

    if (fs->exists(fileName.c_str())) {
      fs->remove(fileName.c_str());
    }

    format->beginInsert(fullFileName, *desc);

    uint64_t writeEnd = start + number;
    for (uint64_t i = start; i < writeEnd; i += step) {
      uint64_t numToAdd = (i + step > writeEnd ? writeEnd - i : step);
      dbcommon::TupleBatch::uptr tb =
          tbu.generateTupleBatch(*desc, i, numToAdd);
      EXPECT_EQ(tb->isValid(), true);
      format->doInsert(std::move(tb));
    }
    format->endInsert();

    std::vector<std::unique_ptr<storage::Input> > files;
    std::unique_ptr<dbcommon::FileInfo> info =
        fs->getFileInfo(fileName.c_str());

    std::unique_ptr<storage::Input> file(
        new storage::FileInput(fullFileName.c_str(), info->size));
    files.push_back(std::move(file));
    std::unique_ptr<univplan::UnivPlanScanFileSplitListList> tasks =
        format->createTasks(files, 1);
    format->beginScan(tasks.get(), desc.get(), nullptr, nullptr, nullptr,
                      false);

    uint64_t totalRead = 0;
    uint64_t startFrom = start;
    dbcommon::TupleBatch::uptr result;
    while (totalRead < number) {
      result = format->next();
      EXPECT_EQ(result->isValid(), true);
      dbcommon::TupleBatch::uptr tb =
          tbu.generateTupleBatch(*desc, startFrom, result->getNumOfRows());
      ASSERT_EQ(result->toString(), tb->toString());
      totalRead += result->getNumOfRows();
      startFrom += result->getNumOfRows();
      result.reset(nullptr);
    }

    EXPECT_EQ(totalRead, number);

    format->endScan();
  }

  void multiBlockTest(const std::string &fmt, const std::string &pattern,
                      uint64_t start, uint64_t step, uint64_t number,
                      uint64_t nTupleReadPerBatch, uint64_t blockAlignSize,
                      const std::string &localPath, bool hasNull) {
    dbcommon::Parameters params;
    params.set("number.tuples.per.batch", std::to_string(nTupleReadPerBatch));
    params.set("format.block.align.size", std::to_string(blockAlignSize));
    params.set("table.options",
               "{\"compresstype\":\"snappy\",\"rlecoder\":\"v2\","
               "\"bloomfilter\":\"0\"}");
    std::unique_ptr<storage::Format> format = createFormat(fmt, &params);

    format->setFileSystemManager(&FSManager);

    dbcommon::TupleBatchUtility tbu;
    dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc(pattern);

    std::string fullFileName = "file://" + localPath;
    std::string fileName(localPath);
    dbcommon::FileSystem *fs = FSManager.get(fullFileName.c_str());

    if (fs->exists(fileName.c_str())) {
      fs->remove(fileName.c_str());
    }

    format->beginInsert(fullFileName, *desc);

    uint64_t writeEnd = start + number;
    for (uint64_t i = start; i < writeEnd; i += step) {
      uint64_t numToAdd = (i + step > writeEnd ? writeEnd - i : step);
      dbcommon::TupleBatch::uptr tb =
          tbu.generateTupleBatch(*desc, i, numToAdd, hasNull);
      EXPECT_EQ(tb->isValid(), true);
      format->doInsert(std::move(tb));
    }
    format->endInsert();

    std::vector<std::unique_ptr<storage::Input> > files;
    std::unique_ptr<dbcommon::FileInfo> info =
        fs->getFileInfo(fileName.c_str());

    std::unique_ptr<storage::Input> file(
        new storage::FileInput(fullFileName.c_str(), info->size));
    files.push_back(std::move(file));
    std::unique_ptr<univplan::UnivPlanScanFileSplitListList> tasks =
        format->createTasks(files, 1);
    format->beginScan(tasks.get(), desc.get(), nullptr, nullptr, nullptr,
                      false);

    uint64_t totalRead = 0;
    uint64_t startFrom = start;
    dbcommon::TupleBatch::uptr result;
    while (totalRead < number) {
      result = format->next();
      EXPECT_EQ(result->isValid(), true);
      dbcommon::TupleBatch::uptr tb = tbu.generateTupleBatch(
          *desc, startFrom, result->getNumOfRows(), hasNull);
      ASSERT_EQ(result->toString(), tb->toString());
      totalRead += result->getNumOfRows();
      startFrom += result->getNumOfRows();
      result.reset(nullptr);
    }

    EXPECT_EQ(totalRead, number);

    format->endScan();
  }
};

}  // end of namespace storage

#endif  // STORAGE_SRC_STORAGE_TESTUTIL_FORMAT_UTIL_H_
