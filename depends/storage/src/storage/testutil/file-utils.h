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

#ifndef STORAGE_SRC_STORAGE_TESTUTIL_FILE_UTILS_H_
#define STORAGE_SRC_STORAGE_TESTUTIL_FILE_UTILS_H_

#include <fstream>
#include <iostream>
#include <string>

#include "dbcommon/filesystem/file-system-manager.h"
#include "dbcommon/filesystem/file-system.h"
#include "dbcommon/filesystem/local/local-file-system.h"
#include "dbcommon/testutil/tuple-batch-utils.h"
#include "dbcommon/utils/string-util.h"

namespace storage {

class FileUtility {
 public:
  explicit FileUtility(dbcommon::FileSystem *fs) : fs(fs) {}
  ~FileUtility() {}

 public:
  void createFile(const std::string &fileName, const std::string &content) {
    std::unique_ptr<dbcommon::File> file = fs->open(fileName.c_str(), O_WRONLY);
    fs->write(file.get(), content.c_str(), content.length());
  }

  void dropFile(const std::string &fileName) { fs->remove(fileName.c_str()); }

 private:
  dbcommon::FileSystem *fs = nullptr;
};

}  // namespace storage

#endif  // STORAGE_SRC_STORAGE_TESTUTIL_FILE_UTILS_H_
