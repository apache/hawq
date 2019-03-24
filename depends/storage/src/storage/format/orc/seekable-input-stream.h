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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_SEEKABLE_INPUT_STREAM_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_SEEKABLE_INPUT_STREAM_H_

#include <google/protobuf/io/zero_copy_stream.h>
#include <snappy.h>

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <list>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "storage/format/orc/input-stream.h"
#include "storage/format/orc/orc-proto-definition.h"
#include "zlib.h"  //NOLINT

namespace orc {

void printBuffer(std::ostream& out, const char* buffer,  // NOLINT
                 uint64_t length);

class PositionProvider {
 private:
  std::list<uint64_t>::const_iterator position;

 public:
  explicit PositionProvider(const std::list<uint64_t>& positions);
  uint64_t next();
};

enum DecompressState {
  DECOMPRESS_HEADER,
  DECOMPRESS_START,
  DECOMPRESS_CONTINUE,
  DECOMPRESS_ORIGINAL,
  DECOMPRESS_EOF
};

// A subclass of Google's ZeroCopyInputStream that supports seek.
// By extending Google's class, we get the ability to pass it directly
// to the protobuf readers.
class SeekableInputStream : public google::protobuf::io::ZeroCopyInputStream {
 public:
  virtual ~SeekableInputStream();
  virtual void seek(PositionProvider& position) = 0;  // NOLINT
  virtual std::string getName() const = 0;
};

// Create a seekable input stream based on a memory range.
class SeekableArrayInputStream : public SeekableInputStream {
 private:
  const char* data;
  uint64_t length;
  uint64_t position;
  uint64_t blockSize;

 public:
  SeekableArrayInputStream(const unsigned char* list, uint64_t length,
                           uint64_t block_size = 0);
  SeekableArrayInputStream(const char* list, uint64_t length,
                           uint64_t block_size = 0);
  virtual ~SeekableArrayInputStream();
  bool Next(const void** data, int* size) override;
  void BackUp(int count) override;
  bool Skip(int count) override;
  google::protobuf::int64 ByteCount() const override;
  void seek(PositionProvider& position) override;
  std::string getName() const override;
};

// Create a seekable input stream based on an input stream.
class SeekableFileInputStream : public SeekableInputStream {
 protected:
  dbcommon::MemoryPool& memoryPool;
  InputStream* const input;
  const uint64_t start;
  const uint64_t length;
  const uint64_t blockSize;
  std::unique_ptr<DataBuffer<char> > buffer;
  uint64_t position;
  uint64_t pushBack;

 public:
  SeekableFileInputStream(InputStream* input, uint64_t offset,
                          uint64_t byteCount,
                          dbcommon::MemoryPool& pool,  // NOLINT
                          uint64_t blockSize = 0);
  explicit SeekableFileInputStream(InputStream* input,
                                   dbcommon::MemoryPool& pool);  // NOLINT
  virtual ~SeekableFileInputStream();

  bool Next(const void** data, int* size) override;
  void BackUp(int count) override;
  bool Skip(int count) override;
  int64_t ByteCount() const override;
  void seek(PositionProvider& position) override;
  std::string getName() const override;
};

class SeekableFileBloomFilterInputStream : public SeekableFileInputStream {
 public:
  SeekableFileBloomFilterInputStream(InputStream* input, uint64_t offset,
                                     uint64_t byteCount,
                                     dbcommon::MemoryPool& pool,  // NOLINT
                                     uint64_t blockSize = 0);
  virtual ~SeekableFileBloomFilterInputStream();

  bool Next(const void** data, int* size) override;
};

class ZlibDecompressionStream : public SeekableInputStream {
 public:
  ZlibDecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                          size_t blockSize,
                          dbcommon::MemoryPool& pool);  // NOLINT
  virtual ~ZlibDecompressionStream();
  bool Next(const void** data, int* size) override;
  void BackUp(int count) override;
  bool Skip(int count) override;
  int64_t ByteCount() const override;
  void seek(PositionProvider& position) override;
  std::string getName() const override;

 private:
  void readBuffer(bool failOnEof) {
    int length;
    if (!input->Next(reinterpret_cast<const void**>(&inputBuffer), &length)) {
      if (failOnEof) {
        LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                  "Read past EOF in "
                  "ZlibDecompressionStream::readBuffer");
      }
      state = DECOMPRESS_EOF;
      inputBuffer = nullptr;
      inputBufferEnd = nullptr;
    } else {
      inputBufferEnd = inputBuffer + length;
    }
  }

  uint32_t readByte(bool failOnEof) {
    if (inputBuffer == inputBufferEnd) {
      readBuffer(failOnEof);
      if (state == DECOMPRESS_EOF) {
        return 0;
      }
    }
    return static_cast<unsigned char>(*(inputBuffer++));
  }

  void readHeader() {
    uint32_t header = readByte(false);
    if (state != DECOMPRESS_EOF) {
      header |= readByte(true) << 8;
      header |= readByte(true) << 16;
      if (header & 1) {
        state = DECOMPRESS_ORIGINAL;
      } else {
        state = DECOMPRESS_START;
      }
      remainingLength = header >> 1;
    } else {
      remainingLength = 0;
    }
  }

  dbcommon::MemoryPool& memoryPool;
  const size_t blockSize;
  std::unique_ptr<SeekableInputStream> input;
  z_stream zstream;
  DataBuffer<char> buffer;

  // the current state
  DecompressState state;

  // the start of the current buffer
  // This pointer is not owned by us. It is either owned by zstream or
  // the underlying stream.
  const char* outputBuffer;
  // the size of the current buffer
  size_t outputBufferLength;
  // the size of the current chunk
  size_t remainingLength;

  // the last buffer returned from the input
  const char* inputBuffer;
  const char* inputBufferEnd;

  // roughly the number of bytes returned
  off_t bytesReturned;
};

class BlockDecompressionStream : public SeekableInputStream {
 public:
  BlockDecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                           size_t blockSize,
                           dbcommon::MemoryPool& pool);  // NOLINT

  virtual ~BlockDecompressionStream() {}
  bool Next(const void** data, int* size) override;
  void BackUp(int count) override;
  bool Skip(int count) override;
  int64_t ByteCount() const override;
  void seek(PositionProvider& position) override;
  std::string getName() const override = 0;

 protected:
  virtual uint64_t decompress(const char* input, uint64_t length, char* output,
                              size_t maxOutputLength) = 0;

  std::string getStreamName() const { return input->getName(); }

 private:
  void readBuffer(bool failOnEof) {
    int length;
    if (!input->Next(reinterpret_cast<const void**>(&inputBufferPtr),
                     &length)) {
      if (failOnEof) {
        LOG_ERROR(ERRCODE_INTERNAL_ERROR, "%s getName() read past EOF",
                  getName().c_str());
      }
      state = DECOMPRESS_EOF;
      inputBufferPtr = nullptr;
      inputBufferPtrEnd = nullptr;
    } else {
      inputBufferPtrEnd = inputBufferPtr + length;
    }
  }

  uint32_t readByte(bool failOnEof) {
    if (inputBufferPtr == inputBufferPtrEnd) {
      readBuffer(failOnEof);
      if (state == DECOMPRESS_EOF) {
        return 0;
      }
    }
    return static_cast<unsigned char>(*(inputBufferPtr++));
  }

  void readHeader() {
    uint32_t header = readByte(false);
    if (state != DECOMPRESS_EOF) {
      header |= readByte(true) << 8;
      header |= readByte(true) << 16;
      if (header & 1) {
        state = DECOMPRESS_ORIGINAL;
      } else {
        state = DECOMPRESS_START;
      }
      remainingLength = header >> 1;
    } else {
      remainingLength = 0;
    }
  }

  std::unique_ptr<SeekableInputStream> input;
  dbcommon::MemoryPool& memoryPool;

  // may need to stitch together multiple input buffers;
  // to give snappy a contiguous block
  DataBuffer<char> inputBuffer;

  // uncompressed output
  DataBuffer<char> outputBuffer;

  // the current state
  DecompressState state;

  // the start of the current output buffer
  const char* outputBufferPtr;
  // the size of the current output buffer
  size_t outputBufferLength;

  // the size of the current chunk
  size_t remainingLength;

  // the last buffer returned from the input
  const char* inputBufferPtr;
  const char* inputBufferPtrEnd;

  // bytes returned by this stream
  off_t bytesReturned;
};

class SnappyDecompressionStream : public BlockDecompressionStream {
 public:
  SnappyDecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                            size_t blockSize,
                            dbcommon::MemoryPool& pool)  // NOLINT
      : BlockDecompressionStream(std::move(inStream), blockSize, pool) {
    // PASS
  }

  std::string getName() const override {
    std::ostringstream result;
    result << "snappy(" << getStreamName() << ")";
    return result.str();
  }

 protected:
  uint64_t decompress(const char* input, uint64_t length, char* output,
                      size_t maxOutputLength) override;
};

class LzoDecompressionStream : public BlockDecompressionStream {
 public:
  LzoDecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                         size_t blockSize,
                         dbcommon::MemoryPool& pool)  // NOLINT
      : BlockDecompressionStream(std::move(inStream), blockSize, pool) {
    // PASS
  }

  std::string getName() const override {
    std::ostringstream result;
    result << "lzo(" << getStreamName() << ")";
    return result.str();
  }

 protected:
  uint64_t decompress(const char* input, uint64_t length, char* output,
                      size_t maxOutputLength) override;
};

class Lz4DecompressionStream : public BlockDecompressionStream {
 public:
  Lz4DecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                         size_t blockSize,
                         dbcommon::MemoryPool& pool)  // NOLINT
      : BlockDecompressionStream(std::move(inStream), blockSize, pool) {
    // PASS
  }

  std::string getName() const override {
    std::ostringstream result;
    result << "lz4(" << getStreamName() << ")";
    return result.str();
  }

 protected:
  uint64_t decompress(const char* input, uint64_t length, char* output,
                      size_t maxOutputLength) override;
};

// Create a decompressor for the given compression kind.
// @param kind the compression type to implement
// @param input the input stream that is the underlying source
// @param bufferSize the maximum size of the buffer
// @param pool the memory pool
std::unique_ptr<SeekableInputStream> createDecompressor(
    CompressionKind kind, std::unique_ptr<SeekableInputStream> input,
    uint64_t bufferSize, dbcommon::MemoryPool& pool);  // NOLINT
}  // namespace orc

#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_SEEKABLE_INPUT_STREAM_H_
