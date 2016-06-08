/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ORC_COMPRESSION_HH
#define ORC_COMPRESSION_HH

#include "orc/OrcFile.hh"

#include "Adaptor.hh"
#include "wrap/zero-copy-stream-wrapper.h"

#include <list>
#include <vector>
#include <fstream>
#include <iostream>
#include <sstream>
#include <memory>

namespace orc {

  void printBuffer(std::ostream& out,
                   const char *buffer,
                   uint64_t length);

  class PositionProvider {
  private:
    std::list<uint64_t>::const_iterator position;
  public:
    PositionProvider(const std::list<uint64_t>& positions);
    uint64_t next();
  };

  /**
   * A subclass of Google's ZeroCopyInputStream that supports seek.
   * By extending Google's class, we get the ability to pass it directly
   * to the protobuf readers.
   */
  class SeekableInputStream: public google::protobuf::io::ZeroCopyInputStream {
  public:
    virtual ~SeekableInputStream();
    virtual void seek(PositionProvider& position) = 0;
    virtual std::string getName() const = 0;
  };

  /**
   * Create a seekable input stream based on a memory range.
   */
  class SeekableArrayInputStream: public SeekableInputStream {
  private:
    const char* data;
    uint64_t length;
    uint64_t position;
    uint64_t blockSize;

  public:
    SeekableArrayInputStream(const unsigned char* list,
                             uint64_t length,
                             uint64_t block_size = 0);
    SeekableArrayInputStream(const char* list,
                             uint64_t length,
                             uint64_t block_size = 0);
    virtual ~SeekableArrayInputStream();
    virtual bool Next(const void** data, int*size) override;
    virtual void BackUp(int count) override;
    virtual bool Skip(int count) override;
    virtual google::protobuf::int64 ByteCount() const override;
    virtual void seek(PositionProvider& position) override;
    virtual std::string getName() const override;
  };

  /**
   * Create a seekable input stream based on an input stream.
   */
  class SeekableFileInputStream: public SeekableInputStream {
  private:
    MemoryPool& pool;
    InputStream* const input;
    const uint64_t start;
    const uint64_t length;
    const uint64_t blockSize;
    std::unique_ptr<DataBuffer<char> > buffer;
    uint64_t position;
    uint64_t pushBack;

  public:
    SeekableFileInputStream(InputStream* input,
                            uint64_t offset,
                            uint64_t byteCount,
                            MemoryPool& pool,
                            uint64_t blockSize = 0);
    virtual ~SeekableFileInputStream();

    virtual bool Next(const void** data, int*size) override;
    virtual void BackUp(int count) override;
    virtual bool Skip(int count) override;
    virtual int64_t ByteCount() const override;
    virtual void seek(PositionProvider& position) override;
    virtual std::string getName() const override;
  };

  /**
   * Create a decompressor for the given compression kind.
   * @param kind the compression type to implement
   * @param input the input stream that is the underlying source
   * @param bufferSize the maximum size of the buffer
   * @param pool the memory pool
   */
  std::unique_ptr<SeekableInputStream>
     createDecompressor(CompressionKind kind,
                        std::unique_ptr<SeekableInputStream> input,
                        uint64_t bufferSize,
                        MemoryPool& pool);
}

#endif
