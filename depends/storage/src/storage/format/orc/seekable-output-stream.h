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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_SEEKABLE_OUTPUT_STREAM_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_SEEKABLE_OUTPUT_STREAM_H_

#include <google/protobuf/io/zero_copy_stream.h>

#include <snappy.h>
#include <vector>

#include "dbcommon/log/logger.h"
#include "dbcommon/utils/byte-buffer.h"
#include "dbcommon/utils/comp/lz4-compressor.h"
#include "dbcommon/utils/comp/snappy-compressor.h"
#include "storage/format/orc/orc-proto-definition.h"
#include "storage/format/orc/output-stream.h"

namespace orc {

class PositionRecorder {
 public:
  void addPosition(uint64_t offset) { positions.push_back(offset); }

  const std::vector<uint64_t> *getPositions() const { return &positions; }

 private:
  std::vector<uint64_t> positions;
};

// corresponding to PositionedOutputStream in JAVA code.
// This is the base class for all memory based stream.
class SeekableOutputStream {
 public:
  SeekableOutputStream() : plainBuffer(true) {}

  virtual ~SeekableOutputStream() {}

  // Record the current position to the recorder.
  // @param recorder the object that receives the position
  // @throws IOException
  void recordPosition(PositionRecorder *recorder) {
    recorder->addPosition(plainBuffer.size());
  }

  // Get the stream size. This is the real size used by this stream.
  // For uncompressed stream, return the plain size.
  // And for compressed stream, return the size after compression.
  // @return void
  virtual uint64_t getStreamSize() { return plainBuffer.size(); }

  virtual uint64_t getEstimatedSpaceNeeded() { return plainBuffer.size(); }

  // Write the buffer of length "len" to the stream.
  // @param buffer The input buffer
  // @param len The length of the buffer
  // @return Void
  virtual void write(const char *buffer, uint64_t len) {
    plainBuffer.append(buffer, len);
  }

  // Write the input value
  // @param value The input value
  // @return Void
  template <class T>
  void write(const T value) {
    plainBuffer.append<T>(value);
  }

  // Write the give byte
  // @param value The input value
  // @return Void
  virtual void writeByte(int8_t value) {
    plainBuffer.append<int8_t>((int8_t)(value));
  }

  // Flush to the given output stream.
  // @param os The output stream
  // @return Void
  virtual void flushToStream(OutputStream *os) {
    // when the input is all null values, the plainBuffer size is 0
    // so we do not need to write anything here.
    if (plainBuffer.size() > 0) {
      os->write(plainBuffer.data(), plainBuffer.size());
    }
  }

  // Clear the internal buffer
  // @return Void
  virtual void reset() { plainBuffer.resize(0); }

 public:
  static uint64_t COMPRESS_BLOCK_SIZE;

 protected:
  dbcommon::ByteBuffer plainBuffer;
};

class BufferedStream : public SeekableOutputStream {
 public:
  BufferedStream() {}
  virtual ~BufferedStream() {}
};

class BlockCompressionStream : public SeekableOutputStream {
 public:
  BlockCompressionStream() : compressedBuffer(true) {}

  virtual ~BlockCompressionStream() {}

  uint64_t getStreamSize() override { return compressedBuffer.size(); }

  uint64_t getEstimatedSpaceNeeded() override {
    return plainBuffer.size() + compressedBuffer.size();
  }

  void write(const char *buffer, uint64_t len) override {
    plainBuffer.append(buffer, len);
    if (plainBuffer.size() >= COMPRESS_BLOCK_SIZE) {
      compress(false);
    }
  }

  template <class T>
  void write(const T value) {
    plainBuffer.append<T>(value);
    if (plainBuffer.size() >= COMPRESS_BLOCK_SIZE) {
      compress(false);
    }
  }

  void writeByte(int8_t value) override {
    plainBuffer.append<int8_t>((int8_t)(value));
    if (plainBuffer.size() >= COMPRESS_BLOCK_SIZE) {
      compress(false);
    }
  }

  void flushToStream(OutputStream *os) override {
    compress(true);
    if (compressedBuffer.size() > 0) {
      os->write(compressedBuffer.data(), compressedBuffer.size());
    }
  }

  void reset() override {
    plainBuffer.resize(0);
    compressedBuffer.resize(0);
  }

 private:
  void compress(bool compressLastBlock) {
    uint64_t bufSize = plainBuffer.size();
    uint64_t oldBufSize = bufSize;
    char *oldData = plainBuffer.data();
    char *data = plainBuffer.data();

    uint64_t stopSize = compressLastBlock ? 0 : (bufSize % COMPRESS_BLOCK_SIZE);
    while (bufSize > stopSize) {
      // reserve some space
      size_t clen = 0;
      uint64_t olen =
          (bufSize >= COMPRESS_BLOCK_SIZE ? COMPRESS_BLOCK_SIZE : bufSize);
      uint64_t maxSz = compressor->maxCompressedLength(olen);
      compressedBuffer.reserve(compressedBuffer.size() + sizeof(char) * 3 +
                               maxSz);

      // allocate header, after compressedBuffer.reserve() function,
      // lenPtr can change, so we can only put lenPtr recording after reserve()
      char *lenPtr = compressedBuffer.tail();
      compressedBuffer.append<char>(0);
      compressedBuffer.append<char>(0);
      compressedBuffer.append<char>(0);

      // compress
      // snappy::RawCompress(data, olen, compressedBuffer.tail(), &clen);
      clen = compressor->compress(data, olen, compressedBuffer.tail(), maxSz);

      uint64_t ulen = clen << 1;
      if (clen < olen) {
        compressedBuffer.resize(compressedBuffer.size() + clen);
        // LOG_INFO("Has compression: ulen %llu clen %zu olen %llu", ulen, clen,
        //    olen);
      } else {
        memcpy(compressedBuffer.tail(), data, olen);
        compressedBuffer.resize(compressedBuffer.size() + olen);

        ulen = olen << 1;
        ulen |= 1;  // set the last bit to 1
        // LOG_INFO("NO compression: ulen %llu clen %zu olen %llu", ulen, clen,
        //    olen);
      }

      // set the header
      lenPtr[0] = ulen & 0xFF;
      lenPtr[1] = (ulen >> 8) & 0xFF;
      lenPtr[2] = (ulen >> 16) & 0xFF;

      // LOG_INFO("lenPtr %d %d %d", lenPtr[0], lenPtr[1], lenPtr[2]);

      bufSize -= olen;
      data += olen;
    }

    // move the last block to the beginning of plainBuffer;
    if (bufSize < oldBufSize && bufSize > 0) {
      assert(bufSize == (plainBuffer.tail() - data));
      memcpy(oldData, data, bufSize);
    }

    plainBuffer.resize(bufSize);
  }

 protected:
  std::unique_ptr<dbcommon::Compressor> compressor;

 private:
  dbcommon::ByteBuffer compressedBuffer;
};

class SnappyCompressionStream : public BlockCompressionStream {
 public:
  SnappyCompressionStream() {
    compressor.reset(new dbcommon::SnappyCompressor());
  }
  ~SnappyCompressionStream() {}
};

class LZ4CompressionStream : public BlockCompressionStream {
 public:
  LZ4CompressionStream() { compressor.reset(new dbcommon::LZ4Compressor()); }
  ~LZ4CompressionStream() {}
};

class ZlibCompressionStream : public SeekableOutputStream {
 public:
  ZlibCompressionStream() {}
  ~ZlibCompressionStream() {}
};

std::unique_ptr<SeekableOutputStream> createBlockCompressor(
    orc::CompressionKind kind);
}  // end of namespace orc

#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_SEEKABLE_OUTPUT_STREAM_H_
