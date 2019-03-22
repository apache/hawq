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

#ifndef DBCOMMON_SRC_DBCOMMON_FILESYSTEM_HDFS_HDFS_FILE_SYSTEM_H_
#define DBCOMMON_SRC_DBCOMMON_FILESYSTEM_HDFS_HDFS_FILE_SYSTEM_H_

#include <cassert>
#include <cerrno>
#include <string>
#include <vector>

#include "hdfs/hdfs.h"

#include "dbcommon/filesystem/file-system.h"
#include "dbcommon/log/logger.h"

namespace dbcommon {

#define INVALID_HDFS_FILE_HANDLE (nullptr)

class HdfsFile : public dbcommon::File {
 public:
  explicit HdfsFile(hdfsFS fs, hdfsFile handle)
      : File(FileType_HDFS), fsHandle(fs), fileHandle(handle) {}
  virtual ~HdfsFile() { close(); }
  void setFsHandle(hdfsFS fs) { fsHandle = fs; }
  hdfsFS getFsHandle() { return fsHandle; }
  void setFileHandle(hdfsFile handle) { fileHandle = handle; }
  hdfsFile getFileHandle() { return fileHandle; }
  void close() {
    if (fileHandle != INVALID_HDFS_FILE_HANDLE) {
      int ret = hdfsCloseFile(fsHandle, fileHandle);
      if (ret < 0) {
        LOG_ERROR(ERRCODE_IO_ERROR, "hdfsCloseFile failed, path:%s, errno: %d",
                  this->fileName().c_str(), errno);
      }
      fileHandle = INVALID_HDFS_FILE_HANDLE;
    }
  }

 private:
  hdfsFS fsHandle;
  hdfsFile fileHandle;
};

//
// wrapper of Hdfs Dir to avoid possible memory leak.
//
class HdfsDir {
 public:
  explicit HdfsDir(hdfsFS fs, const std::string &path) {
    array = hdfsListDirectory(fs, path.c_str(), &arraylen);
    if (array == nullptr) {
      LOG_ERROR(ERRCODE_IO_ERROR, "cannot open directory: %s, errno: %d",
                path.c_str(), errno);
    }
  }

  ~HdfsDir() {
    if (array != nullptr) hdfsFreeFileInfo(array, arraylen);
  }

  hdfsFileInfo *getDir() {
    assert(array != nullptr);

    return array;
  }

  int getNumEntries() {
    assert(array != nullptr);
    return arraylen;
  }

 private:
  int arraylen = 0;
  hdfsFileInfo *array = nullptr;
};

#define INVALID_FILESYSTEM_HANDLE (nullptr)
#define INVALID_FILESYSTEM_PORT (0)

class HdfsFileSystem : public dbcommon::FileSystem {
 public:
  HdfsFileSystem(const char *namenode, tPort port) : fsPort(port) {
    fsNameNode = namenode;
  }
  HdfsFileSystem(const char *namenode, tPort port, const char *ccname,
                 const char *token)
      : fsPort(port) {
    fsNameNode = namenode;
    fsCcname = ccname;
    fsToken = token;
  }

  virtual ~HdfsFileSystem() { disconnect(); }

  void connect() override;
  void disconnect() override;

  std::unique_ptr<File> open(const std::string &path, int flag) override;
  void seek(File *file, uint64_t offset) override;
  void remove(const std::string &path) override;
  std::unique_ptr<dbcommon::FileInfo> getFileInfo(
      const std::string &fileName) override;
  bool exists(const std::string &path) override;
  int64_t getFileLength(const std::string &fileName) override;
  char getFileKind(const std::string &fileName) override;
  int read(File *file, void *buf, int size) override;
  void write(File *file, const void *buf, int size) override;
  void createDir(const std::string &path) override;
  std::vector<std::unique_ptr<dbcommon::FileInfo> > dir(
      const std::string &path) override;
  void chmod(const std::string &path, int mode) override;
  int64_t tell(File *file) override;
  void rename(const std::string &oldPath, const std::string &newPath) override;
  std::vector<std::unique_ptr<FileBlockLocation> >  // NOLINT
  getFileBlockLocation(const std::string &path, int64_t start,
                       int64_t length) override;
  void setBlockSize(int size) override;
  int getBlockSize() override;
  std::string &getFileSystemNameNodeAddr() { return fsNameNode; }
  tPort getFileSystemPort() { return fsPort; }
  std::string getFileSystemToken() { return fsToken; }

 private:
  hdfsFS getFileSystemHandle() { return fsHandle; }
  int getBufferSize() { return fsBufferSize; }
  int getBlockReplication() { return fsBlockReplication; }
  void setBufferSize(int size) { fsBufferSize = size; }
  void setBlockReplication(int rep) { fsBlockReplication = rep; }

  hdfsFS fsHandle = nullptr;
  std::string fsNameNode;
  tPort fsPort = INVALID_FILESYSTEM_PORT;
  int fsBlockSize = 0;
  int fsBlockReplication = 0;
  int fsBufferSize = 0;
  std::string fsCcname;
  std::string fsToken;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_FILESYSTEM_HDFS_HDFS_FILE_SYSTEM_H_
