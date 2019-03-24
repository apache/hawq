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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_OUTPUT_STREAM_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_OUTPUT_STREAM_H_

#include <cassert>
#include <string>

#include "dbcommon/filesystem/file-system.h"

namespace orc {

class OutputStream {
 public:
  OutputStream() {}
  virtual ~OutputStream() {}

  // Write length bytes from the buffer to the file
  // @param buf The output buffer
  // @param length The number of bytes in the buffer to write
  // @return Void
  virtual void write(void* buf, uint64_t length) = 0;

  // Get the name of the stream for error messages.
  // @return The stream name
  virtual const std::string& getName() const = 0;

  // Get the natural size for reads.
  // @return the number of bytes that should be write at once
  virtual uint64_t getNaturalWriteSize() const = 0;

  // Get the total length of the file in bytes.
  // @return The length
  virtual uint64_t getLength() const = 0;

  // Get current file position
  // @return Current file position
  virtual uint64_t getPosition() const = 0;

  // Padding given bytes to the file
  // @param size The bytes to pad
  // @return Void
  virtual void padding(uint64_t size) = 0;

  // Close the stream
  // @return Void
  virtual void close() = 0;
};

class GeneralFileOutputStream : public OutputStream {
 public:
  GeneralFileOutputStream(dbcommon::FileSystem* fs, std::string fileName)
      : fs(fs), fileName(fileName) {
    file = NULL;
    totalLength = -1;
  }

  virtual ~GeneralFileOutputStream() {}

  uint64_t getLength() const override { return totalLength; }

  uint64_t getNaturalWriteSize() const override { return 128 * 1024; }

  void write(void* buf, uint64_t length) override {
    assert(buf != nullptr);

    if (!file) {
      file = fs->open(fileName.c_str(), O_WRONLY);
      totalLength = fs->getFileLength(fileName.c_str());
    }
    fs->write(file.get(), buf, length);
  }

  const std::string& getName() const override { return fileName; }

  uint64_t getPosition() const override { return fs->tell(file.get()); }

  void padding(uint64_t size) override {
    static char buffer[1024] = {0};

    if (size > 0) {
      // we use the first byte of the padding area as FASTBlock type.
      // so we set the padding area to 0
      int times = size / sizeof(buffer);
      int left = size % sizeof(buffer);

      for (int i = 0; i < times; i++)
        fs->write(file.get(), buffer, sizeof(buffer));

      if (left > 0) fs->write(file.get(), buffer, left);
    }
  }

  void close() override {
    if (file) {
      file->close();
    }
  }

  bool fileopen() {
    if (file)
      return true;
    else
      return false;
  }

 private:
  std::string fileName;
  std::unique_ptr<dbcommon::File> file;
  uint64_t totalLength = 0;
  dbcommon::FileSystem* fs = nullptr;
};

std::unique_ptr<OutputStream> writeFile(dbcommon::FileSystem* fs,
                                        const std::string& path);

}  //  end of namespace orc
#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_OUTPUT_STREAM_H_
