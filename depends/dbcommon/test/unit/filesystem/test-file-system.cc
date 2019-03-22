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

#include <errno.h>

#include "dbcommon/filesystem/hdfs/hdfs-file-system.h"
#include "dbcommon/filesystem/local/local-file-system.h"
#include "dbcommon/log/exception.h"
#include "dbcommon/log/logger.h"

#include "gtest/gtest.h"
#include "hdfs/hdfs.h"


namespace dbcommon {

TEST(TestFileSystem, TestHDFS_Blocks) {
  std::string name = "localhost";
  int port = 5000;
  dbcommon::HdfsFileSystem fs(name.c_str(), port);
  int blocksize = fs.getBlockSize();
  fs.setBlockSize(blocksize);
}

TEST(TestFileSystem, TestHDFS_FailCondition) {
  std::string name = "";
  int port = 5000;
  dbcommon::HdfsFileSystem fs(name.c_str(), port);
  EXPECT_THROW(fs.connect(), dbcommon::TransactionAbortException);

  EXPECT_THROW(fs.open(name.c_str(), O_SYNC),
               dbcommon::TransactionAbortException);
  dbcommon::HdfsFile file(nullptr, nullptr);
  EXPECT_THROW(fs.seek(&file, 0), dbcommon::TransactionAbortException);
  EXPECT_THROW(fs.remove(name.c_str()), dbcommon::TransactionAbortException);
  EXPECT_THROW(fs.getFileInfo(name.c_str()),
               dbcommon::TransactionAbortException);
  EXPECT_THROW(fs.getFileLength(name.c_str()),
               dbcommon::TransactionAbortException);
  EXPECT_THROW(fs.read(&file, nullptr, 0), dbcommon::TransactionAbortException);
  EXPECT_THROW(fs.write(&file, nullptr, 0),
               dbcommon::TransactionAbortException);
  EXPECT_THROW(fs.createDir(name.c_str()), dbcommon::TransactionAbortException);
  EXPECT_THROW(fs.chmod(name.c_str(), 0), dbcommon::TransactionAbortException);
  EXPECT_THROW(fs.tell(&file), dbcommon::TransactionAbortException);
  EXPECT_THROW(fs.getFileBlockLocation(name.c_str(), 0, 0),
               dbcommon::TransactionAbortException);
}

TEST(TestFileSystem, TestLocal_FailCondition) {
  std::string name = "";
  int port = 5000;
  dbcommon::LocalFileSystem fs;
  EXPECT_THROW(fs.open(name, O_SYNC),
               dbcommon::TransactionAbortException);
  EXPECT_THROW(fs.open("/fake", O_SYNC), dbcommon::TransactionAbortException);
  dbcommon::LocalFile file(-1, 0);
  char buf[10];
  EXPECT_THROW(fs.seek(&file, 0), dbcommon::TransactionAbortException);
  EXPECT_THROW(fs.remove(name), dbcommon::TransactionAbortException);
  EXPECT_THROW(fs.getFileInfo(name),
               dbcommon::TransactionAbortException);
  EXPECT_THROW(fs.getFileLength(name),
               dbcommon::TransactionAbortException);
  EXPECT_THROW(fs.read(&file, buf, 1), dbcommon::TransactionAbortException);
  EXPECT_THROW(fs.write(&file, buf, 11), dbcommon::TransactionAbortException);
  EXPECT_THROW(fs.chmod(name, 0), dbcommon::TransactionAbortException);
  EXPECT_THROW(fs.tell(&file), dbcommon::TransactionAbortException);
  EXPECT_THROW(fs.getFileBlockLocation(name, 0, 0),
               dbcommon::TransactionAbortException);
  EXPECT_THROW(fs.dir("fakedir"), dbcommon::TransactionAbortException);
  dbcommon::LocalFile filetoClose(-2, 0);
  EXPECT_THROW(filetoClose.close(), dbcommon::TransactionAbortException);
  filetoClose.setHandle(-1);
}

}  // namespace dbcommon
