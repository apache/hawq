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

#ifndef DBCOMMON_SRC_DBCOMMON_FILESYSTEM_FILE_SYSTEM_H_
#define DBCOMMON_SRC_DBCOMMON_FILESYSTEM_FILE_SYSTEM_H_

#include <fcntl.h>

#include <memory>
#include <string>
#include <vector>

#include "dbcommon/utils/file-info.h"

namespace dbcommon {

typedef enum FileType {
  FileType_Local,
  FileType_HDFS,
  FileType_MEMORY,
  FileType_JBOD,
} FileType;

class File {
 public:
  explicit File(FileType t) : ft(t) {}
  virtual ~File() {}

  FileType type() { return ft; }
  virtual void close() = 0;
  virtual std::string &fileName() { return fName; }
  void setFileName(const std::string &name) { fName = name; }
  void setFileName(const char *name) { fName = name; }

 private:
  FileType ft;
  std::string fName;
};

class FileBlockLocation {
 public:
  FileBlockLocation() {}
  ~FileBlockLocation() {}

  std::vector<std::string> hosts;
  std::vector<std::string> names;
  std::vector<std::string> topoPaths;
  std::vector<int> ports;
  int64_t length = -1;
  int64_t offset = -1;
  int corrupt = 0;
};

// A general NOTE: if an operation failed on a "File",
// for example "seek beyond EOF",
// any following operation except "close" on the
// file is undefined behavior. So users should "close"
// the file first, then open it again.
class FileSystem {
 public:
  FileSystem() {}
  virtual ~FileSystem() {}

  // Connect to the file system
  // @return void
  virtual void connect() = 0;

  // Disconnect from the file system
  // @return void
  virtual void disconnect() = 0;

  // Open and possibly create the file. No close function is needed,
  // "File" destructor should close the file when it is destroyed
  // @param path The give file with absolute path
  // @param flags Supported flag combinations are
  //        1) O_RDONLY: read only open.
  //        2) O_WRONLY: create or overwrite i.e., implies O_TRUNCAT)
  //        3) O_WRONLY | O_SYNC: write and sync to disk when file close
  //        4) O_CREAT: create file if file not exists, otherwise, report error
  // @return The file opened.
  virtual std::unique_ptr<File> open(const std::string &path, int flags) = 0;

  // Seek to offset. Open for "write" (O_WRITE) file cannot seek,
  // since some filesystem does not support random write. So,
  // seek on "open for write" file will report exception
  // NOTE: file status after "seek beyond EOF on file write" is undefined.
  // And for "open for read" files, "seek beyond EOF" behavior is undefined.
  // for example, for local filesystem, it works, but for HDFS, it throws
  // an exception.
  // @param file The input file
  // @param offset The file offset to seek
  // @return void
  virtual void seek(File *file, uint64_t offset) = 0;

  // Remove the give path or directory recursively. If the file does
  // not exist, exception is thrown.
  // @param path The input path
  // @return void
  virtual void remove(const std::string &path) = 0;

  // Remove the give path or directory recursively. If the file does
  // not exist, return succussfully.
  // @param path The input path
  // @return void
  void removeIfExists(const std::string &path) {
    if (exists(path)) {
      remove(path);
    }
  }

  // Return file information for the given file (absolute path).
  // If the file does not exist, or other error happens, exception is thrown.
  // @param fileName The given file name
  // @return The file info.
  virtual std::unique_ptr<dbcommon::FileInfo> getFileInfo(
      const std::string &fileName) = 0;

  // Test whether the given path exist
  // @param fileName The given path
  // @return True if it exists, otherwise false.
  virtual bool exists(const std::string &path) = 0;

  // Get file length. If the file does
  // not exist, or other error happens, exception is thrown.
  // @param path The given file name
  // @return The file info.
  virtual int64_t getFileLength(const std::string &path) = 0;

  // Get file kind. If the file does
  // not exist, or other error happens, exception is thrown.
  // @param path The given file name
  // @return The file info.
  virtual char getFileKind(const std::string &path) = 0;

  // Read 'size' data to the given buffer.
  // This API guarantees that we will read 'size' data
  // if the file have at least 'size' data, except that some
  // IO errors happen. When IO errors happen, exception is thrown.
  // @param file The given file
  // @param buf The output buffer
  // @param size The read size
  // @return The real size we read.
  virtual int read(File *file, void *buf, int size) = 0;

  // Write 'size' data in the buffer to the given file.
  // This API guarantees that we will write 'size' data
  // except that some IO errors happen. When IO errors happen,
  // exception is thrown.
  // @param file The given file
  // @param buf The output buffer
  // @param size The read size
  // @return The real size we read.
  virtual void write(File *file, const void *buf, int size) = 0;

  // Create directory recursively if parent does not exists.
  // @param path The given path to create. The default permission is 0755
  // @return void
  virtual void createDir(const std::string &path) = 0;

  // List the directory content.
  // @param path The given path.
  // @return The FileInfo vector
  virtual std::vector<std::unique_ptr<dbcommon::FileInfo> > dir(
      const std::string &path) = 0;

  // Change the permissions of files or directories
  // @param path The given path.
  // @param mode The mode to set, for example 0777
  // @return The FileInfo vector
  virtual void chmod(const std::string &path, int mode) = 0;

  // Get the current offset in the file, in bytes.
  // @param file The input file.
  // @return The current offset
  virtual int64_t tell(File *file) = 0;

  // Rename file
  // @param oldPath The path of the source file.
  // @param newPath The path of the destination file.
  virtual void rename(const std::string &oldPath,
                      const std::string &newPath) = 0;

  // Test whether the given file of a table exist
  // @param tableName The given table name
  // @param fileName The given file name of table
  // @return True if it exists, otherwise false.
  virtual bool exists(const std::string &tableName,
                      const std::string &fileName) {
    return false;
  }

  // Get the BlockLocation of the file
  // @param path The path to the file
  // @param start The start offset of the file
  // @param length The length of the file
  // @return An array of FileBlockLocation
  virtual std::vector<std::unique_ptr<FileBlockLocation> > getFileBlockLocation(
      const std::string &path, int64_t start, int64_t length) = 0;

  // set and get block size for file system.
  virtual void setBlockSize(int size) { blockSize = size; }
  virtual int getBlockSize() { return blockSize; }

 protected:
  double blockSize = 128 * 1024 * 1024;
};

}  // end of namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_FILESYSTEM_FILE_SYSTEM_H_
