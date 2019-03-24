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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_INPUT_STREAM_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_INPUT_STREAM_H_

#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <memory>
#include <string>

#include "dbcommon/filesystem/file-system-manager.h"
#include "dbcommon/filesystem/file-system.h"
#include "storage/format/orc/exceptions.h"

// The top level interface to ORC.
namespace orc {

// An abstract interface for providing ORC readers a stream of bytes.
class InputStream {
 public:
  InputStream() {}
  virtual ~InputStream() {}

  // Get the total length of the file in bytes.
  virtual uint64_t getLength() const = 0;

  // Get the natural size for reads.
  // @return the number of bytes that should be read at once
  virtual uint64_t getNaturalReadSize() const = 0;

  // Read length bytes from the file starting at offset into
  // the buffer starting at buf.
  // @param buf the starting position of a buffer.
  // @param length the number of bytes to read.
  // @param offset the position in the stream to read from.
  virtual void read(void* buf, uint64_t length, uint64_t offset) = 0;

  // Get the name of the stream for error messages.
  virtual const std::string& getName() const = 0;

  virtual void readBloomFilter(void* buf, uint64_t length, uint64_t offset) = 0;
};

class GeneralFileInputStream : public InputStream {
 public:
  GeneralFileInputStream(dbcommon::FileSystem* fs, std::string fileName)
      : fs(fs), fileName(fileName) {
    file = fs->open(fileName.c_str(), O_RDONLY);
    totalLength = fs->getFileLength(fileName.c_str());
  }

  virtual ~GeneralFileInputStream() {}

  uint64_t getLength() const override { return totalLength; }

  uint64_t getNaturalReadSize() const override { return 128 * 1024; }

  void read(void* buf, uint64_t length, uint64_t offset) override {
    assert(buf != nullptr);

    fs->seek(file.get(), offset);
    int bytesRead = fs->read(file.get(), buf, length);
  }

  void readBloomFilter(void* buf, uint64_t length, uint64_t offset) override {
    assert(buf != nullptr);
    if (!bloomFilterHandler)
      bloomFilterHandler = fs->open(fileName.c_str(), O_RDONLY);
    fs->seek(bloomFilterHandler.get(), offset);
    int bytesRead = fs->read(bloomFilterHandler.get(), buf, length);
  }

  const std::string& getName() const override { return fileName; }

 private:
  std::string fileName;
  std::unique_ptr<dbcommon::File> file = nullptr;
  std::unique_ptr<dbcommon::File> bloomFilterHandler = nullptr;
  uint64_t totalLength = 0;
  dbcommon::FileSystem* fs = nullptr;
};

std::unique_ptr<InputStream> readFile(dbcommon::FileSystem* fs,
                                      const std::string& path);

}  // end of namespace orc

#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_INPUT_STREAM_H_
