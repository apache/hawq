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

#ifndef DBCOMMON_SRC_DBCOMMON_FILESYSTEM_LOCAL_LOCAL_FILE_SYSTEM_H_
#define DBCOMMON_SRC_DBCOMMON_FILESYSTEM_LOCAL_LOCAL_FILE_SYSTEM_H_

#include <dirent.h>
#include <fcntl.h>

#include <cerrno>
#include <list>
#include <string>
#include <vector>

#include "dbcommon/filesystem/file-system.h"
#include "dbcommon/log/logger.h"
#include "dbcommon/utils/lock.h"

namespace dbcommon {

#define INVALID_LOCAL_FILE_HANDLE (-1)

class LocalFile : public dbcommon::File {
 public:
  LocalFile(int handle, int flag)
      : File(FileType_Local), fileHandle(handle), flag(flag) {}

  virtual ~LocalFile() { close(); }

  void setHandle(int handle) { fileHandle = handle; }

  int getHandle() { return fileHandle; }

  int getFlag() { return flag; }

  bool openForWrite() { return flag & O_WRONLY; }

  void close() {
    if (fileHandle != INVALID_LOCAL_FILE_HANDLE) {
      int ret = ::close(fileHandle);
      if (ret < 0) {
        LOG_ERROR(ERRCODE_IO_ERROR,
                  "open OS API call [close] failed, path:%s, errno: %d",
                  this->fileName().c_str(), errno);
      }
    }
    fileHandle = INVALID_LOCAL_FILE_HANDLE;
  }

 private:
  int fileHandle;
  int flag;
};

class LocalFileInCache {
 public:
  LocalFileInCache() {
    lastAccessTime = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
  }
  LocalFileInCache(std::unique_ptr<File> file_, Lock *lock_) {
    file = std::move(file_);
    lock = lock_;
    lastAccessTime = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
  }

  void access() {
    lastAccessTime = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
  }

  File *getFile() { return file.get(); }

  Lock *getLock() { return lock; }

  uint64_t getLastAccessTime() { return lastAccessTime; }

  LocalFileInCache &operator=(LocalFileInCache &&fileCache) {  // NOLINT
    if (this != &fileCache) {
      file = std::move(fileCache.file);
      lock = fileCache.lock;
      lastAccessTime = fileCache.lastAccessTime;
    }
    return *this;
  }

 private:
  std::unique_ptr<File> file;
  Lock *lock = nullptr;
  uint64_t lastAccessTime;
};

#define MAXFILENUM 20
// 10s
#define OPENFILETIMEOUT 10000

class LocalFilesCache {
 public:
  explicit LocalFilesCache(dbcommon::LockManager *lockMgr_)
      : lockMgr(lockMgr_) {
    cacheLock = lockMgr->create("local files cache");
    filesQueue.clear();
    filesCache.clear();
  }

  Lock *getFilesCacheLock();

  LocalFileInCache *findFile(uint64_t fileNum, bool refresh);

  void putFile(uint64_t fileNum, std::unique_ptr<File> file,
               dbcommon::Lock *lock);

  void cleanUpFile();

 private:
  dbcommon::LockManager *lockMgr;
  std::unique_ptr<Lock> cacheLock;
  std::list<uint64_t> filesQueue;
  std::unordered_map<uint64_t, dbcommon::LocalFileInCache> filesCache;
};

//
// wrapper of OS Dir to avoid possible memory leak.
//
class OSDir {
 public:
  explicit OSDir(const std::string &path);
  ~OSDir();
  DIR *getDir() { return dir; }

 private:
  DIR *dir = nullptr;
};

class LocalFileSystem : public dbcommon::FileSystem {
 public:
  LocalFileSystem() {}
  virtual ~LocalFileSystem() {}

  void connect() override {}
  void disconnect() override {}

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
  std::vector<std::unique_ptr<FileBlockLocation> > getFileBlockLocation(
      const std::string &path, int64_t start, int64_t length) override;

  void truncate(File *file, uint64_t offset);
  void preallocate(File *file, uint64_t size);
  void fdatasync(File *file);
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_FILESYSTEM_LOCAL_LOCAL_FILE_SYSTEM_H_
