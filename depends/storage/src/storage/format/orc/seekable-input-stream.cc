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

#include <lz4.h>

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "storage/format/orc/exceptions.h"
#include "storage/format/orc/lzo-decompressor.h"
#include "storage/format/orc/seekable-input-stream.h"

namespace orc {

void printBuffer(std::ostream& out, const char* buffer,  // NOLINT
                 uint64_t length) {
  const uint64_t width = 24;
  out << std::hex;
  for (uint64_t line = 0; line < (length + width - 1) / width; ++line) {
    out << std::setfill('0') << std::setw(7) << (line * width);
    for (uint64_t byte = 0; byte < width && line * width + byte < length;
         ++byte) {
      out << " " << std::setfill('0') << std::setw(2)
          << static_cast<uint64_t>(0xff & buffer[line * width + byte]);
    }
    out << "\n";
  }
  out << std::dec;
}

PositionProvider::PositionProvider(const std::list<uint64_t>& posns) {
  position = posns.begin();
}

uint64_t PositionProvider::next() {
  uint64_t result = *position;
  ++position;
  return result;
}

SeekableInputStream::~SeekableInputStream() {
  // PASS
}

SeekableArrayInputStream::~SeekableArrayInputStream() {
  // PASS
}

SeekableArrayInputStream::SeekableArrayInputStream(const unsigned char* values,
                                                   uint64_t size,
                                                   uint64_t blkSize)
    : data(reinterpret_cast<const char*>(values)) {
  length = size;
  position = 0;
  blockSize = blkSize == 0 ? length : static_cast<uint64_t>(blkSize);
}

SeekableArrayInputStream::SeekableArrayInputStream(const char* values,
                                                   uint64_t size,
                                                   uint64_t blkSize)
    : data(values) {
  length = size;
  position = 0;
  blockSize = blkSize == 0 ? length : static_cast<uint64_t>(blkSize);
}

bool SeekableArrayInputStream::Next(const void** buffer, int* size) {
  uint64_t currentSize = std::min(length - position, blockSize);
  if (currentSize > 0) {
    *buffer = data + position;
    *size = static_cast<int>(currentSize);
    position += currentSize;
    return true;
  }
  *size = 0;
  return false;
}

void SeekableArrayInputStream::BackUp(int count) {
  if (count >= 0) {
    uint64_t unsignedCount = static_cast<uint64_t>(count);
    if (unsignedCount <= blockSize && unsignedCount <= position) {
      position -= unsignedCount;
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Can't backup that much!");
    }
  }
}

bool SeekableArrayInputStream::Skip(int count) {
  if (count >= 0) {
    uint64_t unsignedCount = static_cast<uint64_t>(count);
    if (unsignedCount + position <= length) {
      position += unsignedCount;
      return true;
    } else {
      position = length;
    }
  }
  return false;
}

google::protobuf::int64 SeekableArrayInputStream::ByteCount() const {
  return static_cast<google::protobuf::int64>(position);
}

void SeekableArrayInputStream::seek(PositionProvider& seekPosition) {
  position = seekPosition.next();
}

std::string SeekableArrayInputStream::getName() const {
  std::ostringstream result;
  result << "SeekableArrayInputStream " << position << " of " << length;
  return result.str();
}

static uint64_t computeBlock(uint64_t request, uint64_t length) {
  return std::min(length, request == 0 ? 256 * 1024 : request);
}

SeekableFileInputStream::SeekableFileInputStream(InputStream* stream,
                                                 uint64_t offset,
                                                 uint64_t byteCount,
                                                 dbcommon::MemoryPool& pool,
                                                 uint64_t _blockSize)
    : memoryPool(pool),
      input(stream),
      start(offset),
      length(byteCount),
      blockSize(computeBlock(length, length)) {
  position = 0;
  buffer.reset(new DataBuffer<char>(pool, length));
  pushBack = 0;
}

SeekableFileInputStream::SeekableFileInputStream(InputStream* input,
                                                 dbcommon::MemoryPool& pool)
    : memoryPool(pool),
      input(input),
      start(0),
      length(input->getLength()),
      blockSize(computeBlock(0, length)) {
  position = 0;
  buffer.reset(new DataBuffer<char>(pool, length));
  pushBack = 0;
}

SeekableFileInputStream::~SeekableFileInputStream() {
  // PASS
}

bool SeekableFileInputStream::Next(const void** data, int* size) {
  uint64_t bytesRead;
  if (pushBack != 0) {
    *data = buffer->data() + (buffer->size() - pushBack);
    bytesRead = pushBack;
  } else {
    bytesRead = std::min(length - position, blockSize);
    buffer->resize(bytesRead);
    if (bytesRead > 0) {
      input->read(buffer->data(), bytesRead, start + position);
      *data = static_cast<void*>(buffer->data());
    }
  }
  position += bytesRead;
  pushBack = 0;
  *size = static_cast<int>(bytesRead);
  return bytesRead != 0;
}

void SeekableFileInputStream::BackUp(int signedCount) {
  if (signedCount < 0) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "can't backup negative distances");
  }
  uint64_t count = static_cast<uint64_t>(signedCount);
  if (pushBack > 0) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "can't backup unless we just called Next");
  }
  if (count > blockSize || count > position) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "can't backup that far");
  }
  pushBack = static_cast<uint64_t>(count);
  position -= pushBack;
}

bool SeekableFileInputStream::Skip(int signedCount) {
  if (signedCount < 0) {
    return false;
  }
  uint64_t count = static_cast<uint64_t>(signedCount);
  position = std::min(position + count, length);
  pushBack = 0;
  return position < length;
}

int64_t SeekableFileInputStream::ByteCount() const {
  return static_cast<int64_t>(position);
}

void SeekableFileInputStream::seek(PositionProvider& location) {
  position = location.next();
  if (position > length) {
    position = length;
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "seek too far");
  }
  pushBack = 0;
}

std::string SeekableFileInputStream::getName() const {
  std::ostringstream result;
  result << input->getName() << " from " << start << " for " << length;
  return result.str();
}

SeekableFileBloomFilterInputStream::SeekableFileBloomFilterInputStream(
    InputStream* stream, uint64_t offset, uint64_t byteCount,
    dbcommon::MemoryPool& pool, uint64_t _blockSize)
    : SeekableFileInputStream(stream, offset, byteCount, pool, _blockSize) {}

SeekableFileBloomFilterInputStream::~SeekableFileBloomFilterInputStream() {}

bool SeekableFileBloomFilterInputStream::Next(const void** data, int* size) {
  uint64_t bytesRead;
  if (pushBack != 0) {
    *data = buffer->data() + (buffer->size() - pushBack);
    bytesRead = pushBack;
  } else {
    bytesRead = std::min(length - position, blockSize);
    buffer->resize(bytesRead);
    if (bytesRead > 0) {
      input->readBloomFilter(buffer->data(), bytesRead, start + position);
      *data = static_cast<void*>(buffer->data());
    }
  }
  position += bytesRead;
  pushBack = 0;
  *size = static_cast<int>(bytesRead);
  return bytesRead != 0;
}

ZlibDecompressionStream::ZlibDecompressionStream(
    std::unique_ptr<SeekableInputStream> inStream, size_t _blockSize,
    dbcommon::MemoryPool& pool)
    : memoryPool(pool), blockSize(_blockSize), buffer(pool, _blockSize) {
  input.reset(inStream.release());
  zstream.next_in = Z_NULL;
  zstream.avail_in = 0;
  zstream.zalloc = Z_NULL;
  zstream.zfree = Z_NULL;
  zstream.opaque = Z_NULL;
  zstream.next_out = reinterpret_cast<Bytef*>(buffer.data());
  zstream.avail_out = static_cast<uInt>(blockSize);
  int64_t result = inflateInit2(&zstream, -15);
  switch (result) {
    case Z_OK:
      break;
    case Z_MEM_ERROR:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Memory error from inflateInit2");
    case Z_VERSION_ERROR:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Version error from inflateInit2");
    case Z_STREAM_ERROR:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Stream error from inflateInit2");
    default:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Unknown error from inflateInit2");
  }
  outputBuffer = nullptr;
  outputBufferLength = 0;
  remainingLength = 0;
  state = DECOMPRESS_HEADER;
  inputBuffer = nullptr;
  inputBufferEnd = nullptr;
  bytesReturned = 0;
}

ZlibDecompressionStream::~ZlibDecompressionStream() {
  int64_t result = inflateEnd(&zstream);
  if (result != Z_OK) {
    // really can't throw in destructors
    std::cout << "Error in ~ZlibDecompressionStream() " << result << "\n";
  }
}

bool ZlibDecompressionStream::Next(const void** data, int* size) {
  // if the user pushed back, return them the partial buffer
  if (outputBufferLength) {
    *data = outputBuffer;
    *size = static_cast<int>(outputBufferLength);
    outputBuffer += outputBufferLength;
    outputBufferLength = 0;
    return true;
  }
  if (state == DECOMPRESS_HEADER || remainingLength == 0) {
    readHeader();
  }
  if (state == DECOMPRESS_EOF) {
    return false;
  }
  if (inputBuffer == inputBufferEnd) {
    readBuffer(true);
  }
  size_t availSize = std::min(static_cast<size_t>(inputBufferEnd - inputBuffer),
                              remainingLength);
  if (state == DECOMPRESS_ORIGINAL) {
    *data = inputBuffer;
    *size = static_cast<int>(availSize);
    outputBuffer = inputBuffer + availSize;
    outputBufferLength = 0;
  } else if (state == DECOMPRESS_START) {
    zstream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(inputBuffer));
    zstream.avail_in = static_cast<uInt>(availSize);
    outputBuffer = buffer.data();
    zstream.next_out =
        reinterpret_cast<Bytef*>(const_cast<char*>(outputBuffer));
    zstream.avail_out = static_cast<uInt>(blockSize);
    if (inflateReset(&zstream) != Z_OK) {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "Bad inflateReset in "
                "ZlibDecompressionStream::Next");
    }
    int64_t result;
    do {
      result = inflate(&zstream,
                       availSize == remainingLength ? Z_FINISH : Z_SYNC_FLUSH);
      switch (result) {
        case Z_OK:
          remainingLength -= availSize;
          inputBuffer += availSize;
          readBuffer(true);
          availSize =
              std::min(static_cast<size_t>(inputBufferEnd - inputBuffer),
                       remainingLength);
          zstream.next_in =
              reinterpret_cast<Bytef*>(const_cast<char*>(inputBuffer));
          zstream.avail_in = static_cast<uInt>(availSize);
          break;
        case Z_STREAM_END:
          break;
        case Z_BUF_ERROR:
          LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                    "Buffer error in "
                    "ZlibDecompressionStream::Next");
        case Z_DATA_ERROR:
          LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                    "Data error in "
                    "ZlibDecompressionStream::Next");
        case Z_STREAM_ERROR:
          LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                    "Stream error in "
                    "ZlibDecompressionStream::Next");
        default:
          LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                    "Unknown error in "
                    "ZlibDecompressionStream::Next");
      }
    } while (result != Z_STREAM_END);
    *size = static_cast<int>(blockSize - zstream.avail_out);
    *data = outputBuffer;
    outputBufferLength = 0;
    outputBuffer += *size;
  } else {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "Unknown compression state in "
              "ZlibDecompressionStream::Next");
  }
  inputBuffer += availSize;
  remainingLength -= availSize;
  bytesReturned += *size;
  return true;
}

void ZlibDecompressionStream::BackUp(int count) {
  if (outputBuffer == nullptr || outputBufferLength != 0) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "Backup without previous Next in "
              "ZlibDecompressionStream");
  }
  outputBuffer -= static_cast<size_t>(count);
  outputBufferLength = static_cast<size_t>(count);
  bytesReturned -= count;
}

bool ZlibDecompressionStream::Skip(int count) {
  bytesReturned += count;
  // this is a stupid implementation for now.
  // should skip entire blocks without decompressing
  while (count > 0) {
    const void* ptr;
    int len;
    if (!Next(&ptr, &len)) {
      return false;
    }
    if (len > count) {
      BackUp(len - count);
      count = 0;
    } else {
      count -= len;
    }
  }
  return true;
}

int64_t ZlibDecompressionStream::ByteCount() const { return bytesReturned; }

void ZlibDecompressionStream::seek(PositionProvider& position) {
  input->seek(position);
  bytesReturned = input->ByteCount();
  if (!Skip(static_cast<int>(position.next()))) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "Bad skip in ZlibDecompressionStream::seek");
  }
}

std::string ZlibDecompressionStream::getName() const {
  std::ostringstream result;
  result << "zlib(" << input->getName() << ")";
  return result.str();
}

BlockDecompressionStream::BlockDecompressionStream(
    std::unique_ptr<SeekableInputStream> inStream, size_t bufferSize,
    dbcommon::MemoryPool& pool)
    : memoryPool(pool),
      inputBuffer(pool, bufferSize),
      outputBuffer(pool, bufferSize),
      state(DECOMPRESS_HEADER),
      outputBufferPtr(0),
      outputBufferLength(0),
      remainingLength(0),
      inputBufferPtr(0),
      inputBufferPtrEnd(0),
      bytesReturned(0) {
  input.reset(inStream.release());
}

bool BlockDecompressionStream::Next(const void** data, int* size) {
  // if the user pushed back, return them the partial buffer
  if (outputBufferLength) {
    *data = outputBufferPtr;
    *size = static_cast<int>(outputBufferLength);
    outputBufferPtr += outputBufferLength;
    bytesReturned += outputBufferLength;
    outputBufferLength = 0;
    return true;
  }
  if (state == DECOMPRESS_HEADER || remainingLength == 0) {
    readHeader();
  }
  if (state == DECOMPRESS_EOF) {
    return false;
  }
  if (inputBufferPtr == inputBufferPtrEnd) {
    readBuffer(true);
  }

  size_t availSize = std::min(
      static_cast<size_t>(inputBufferPtrEnd - inputBufferPtr), remainingLength);
  if (state == DECOMPRESS_ORIGINAL) {
    *data = inputBufferPtr;
    *size = static_cast<int>(availSize);
    outputBufferPtr = inputBufferPtr + availSize;
    outputBufferLength = 0;
    inputBufferPtr += availSize;
    remainingLength -= availSize;
  } else if (state == DECOMPRESS_START) {
    // Get contiguous bytes of compressed block.
    const char* compressed = inputBufferPtr;
    if (remainingLength == availSize) {
      inputBufferPtr += availSize;
    } else {
      // Did not read enough from input.
      if (inputBuffer.capacity() < remainingLength) {
        inputBuffer.resize(remainingLength);
      }
      ::memcpy(inputBuffer.data(), inputBufferPtr, availSize);
      inputBufferPtr += availSize;
      compressed = inputBuffer.data();

      for (size_t pos = availSize; pos < remainingLength;) {
        readBuffer(true);
        size_t avail =
            std::min(static_cast<size_t>(inputBufferPtrEnd - inputBufferPtr),
                     remainingLength - pos);
        ::memcpy(inputBuffer.data() + pos, inputBufferPtr, avail);
        pos += avail;
        inputBufferPtr += avail;
      }
    }

    outputBufferLength =
        decompress(compressed, remainingLength, outputBuffer.data(),
                   outputBuffer.capacity());

    remainingLength = 0;
    state = DECOMPRESS_HEADER;
    *data = outputBuffer.data();
    *size = static_cast<int>(outputBufferLength);
    outputBufferPtr = outputBuffer.data() + outputBufferLength;
    outputBufferLength = 0;
  }

  bytesReturned += *size;
  return true;
}

void BlockDecompressionStream::BackUp(int count) {
  if (outputBufferPtr == nullptr || outputBufferLength != 0) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Backup without previous Next in %s",
              getName().c_str());
  }
  outputBufferPtr -= static_cast<size_t>(count);
  outputBufferLength = static_cast<size_t>(count);
  bytesReturned -= count;
}

bool BlockDecompressionStream::Skip(int count) {
  bytesReturned += count;
  // this is a stupid implementation for now.
  // should skip entire blocks without decompressing
  while (count > 0) {
    const void* ptr;
    int len;
    if (!Next(&ptr, &len)) {
      return false;
    }
    if (len > count) {
      BackUp(len - count);
      count = 0;
    } else {
      count -= len;
    }
  }
  return true;
}

int64_t BlockDecompressionStream::ByteCount() const { return bytesReturned; }

void BlockDecompressionStream::seek(PositionProvider& position) {
  input->seek(position);
  if (!Skip(static_cast<int>(position.next()))) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Bad skip in %s", getName().c_str());
  }
}

uint64_t SnappyDecompressionStream::decompress(const char* input,
                                               uint64_t length, char* output,
                                               size_t maxOutputLength) {
  size_t outLength;
  if (!snappy::GetUncompressedLength(input, length, &outLength)) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "SnappyDecompressionStream choked on corrupt input");
  }

  if (outLength > maxOutputLength) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Snappy length exceeds block size");
  }

  if (!snappy::RawUncompress(input, length, output)) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "SnappyDecompressionStream choked on corrupt input");
  }
  return outLength;
}

uint64_t LzoDecompressionStream::decompress(const char* input, uint64_t length,
                                            char* output,
                                            size_t maxOutputLength) {
  return lzoDecompress(input, input + length, output, output + maxOutputLength);
}

uint64_t Lz4DecompressionStream::decompress(const char* input, uint64_t length,
                                            char* output,
                                            size_t maxOutputLength) {
  int result = LZ4_decompress_safe(input, output, static_cast<int>(length),
                                   static_cast<int>(maxOutputLength));
  if (result < 0) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "%s - failed to decompress",
              getName().c_str());
  }
  return static_cast<uint64_t>(result);
}

std::unique_ptr<SeekableInputStream> createDecompressor(
    CompressionKind kind, std::unique_ptr<SeekableInputStream> input,
    uint64_t blockSize, dbcommon::MemoryPool& pool) {  // NOLINT
  switch (static_cast<int64_t>(kind)) {
    case CompressionKind_NONE:
      return std::move(input);
    case CompressionKind_ZLIB:
      return std::unique_ptr<SeekableInputStream>(
          new ZlibDecompressionStream(std::move(input), blockSize, pool));
    case CompressionKind_SNAPPY:
      return std::unique_ptr<SeekableInputStream>(
          new SnappyDecompressionStream(std::move(input), blockSize, pool));
    case CompressionKind_LZO:
      return std::unique_ptr<SeekableInputStream>(
          new LzoDecompressionStream(std::move(input), blockSize, pool));
    case CompressionKind_LZ4:
      return std::unique_ptr<SeekableInputStream>(
          new Lz4DecompressionStream(std::move(input), blockSize, pool));
    default:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Unknown compression codec %lu", kind);
  }
}
}  // namespace orc
