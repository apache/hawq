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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_RLE_V0_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_RLE_V0_H_

#include <algorithm>

#include "storage/format/orc/rle.h"

namespace orc {

template <class IntType>
class RleDecoderV0 : public RleDecoder {
 public:
  explicit RleDecoderV0(std::unique_ptr<SeekableInputStream> input)
      : inputStream(std::move(input)) {}

  void seek(PositionProvider &location) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "skip not supported yet");
  }

  void skip(uint64_t numValues) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "skip not supported yet");
  }

  void next(void *data, uint64_t numValues, const char *notNull) override {
    if (notNull) {
      uint64_t notNullValues = 0;
      for (uint64_t i = 0; i < numValues; ++i) {
        if (notNull[i]) {
          ++notNullValues;
        }
      }
      IntType *dat = reinterpret_cast<IntType *>(data);
      std::unique_ptr<IntType> dataNotNull(new IntType(notNullValues));
      readData(dataNotNull.get(), notNullValues);
      IntType *datNotNull = dataNotNull.get();
      for (uint64_t j = 0, k = 0; j < numValues; ++j) {
        if (notNull[j]) {
          dat[j] = datNotNull[k++];
        }
      }
    } else {
      readData(data, numValues);
    }
  }

 private:
  void nextBuffer() {
    int bufferLength = 0;
    const void *bufferPointer = nullptr;
    bool result = inputStream->Next(&bufferPointer, &bufferLength);
    if (!result) {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "bad read in nextBuffer");
    }
    bufferStart = static_cast<const char *>(bufferPointer);
    bufferEnd = bufferStart + bufferLength;
  }

  void readData(void *data, uint64_t numValues) {
    uint64_t i = 0;
    uint64_t count = numValues * sizeof(IntType);
    while (i < count) {
      if (bufferStart == bufferEnd) {
        nextBuffer();
      }
      uint64_t copyBytes =
          std::min(count - i, static_cast<uint64_t>(bufferEnd - bufferStart));
      memcpy(data, bufferStart, copyBytes);
      bufferStart += copyBytes;
      i += copyBytes;
    }
  }

  const std::unique_ptr<SeekableInputStream> inputStream;

  const char *bufferStart = nullptr;
  const char *bufferEnd = nullptr;
};

template <class IntType>
class RleCoderV0 : public RleCoder {
 public:
  explicit RleCoderV0(std::unique_ptr<SeekableOutputStream> stream)
      : output(std::move(stream)) {}

  void write(void *data, uint64_t numValues, const char *notNull) override {
    IntType *d = reinterpret_cast<IntType *>(data);
    if (notNull) {
      for (uint64_t i = 0; i < numValues; i++) {
        if (notNull[i]) {
          output->write<IntType>(d[i]);
        }
      }
    } else {
      for (uint64_t i = 0; i < numValues; i++) {
        output->write<IntType>(d[i]);
      }
    }
  }

  void flushToStream(OutputStream *stream) override {
    output->flushToStream(stream);
  }

  uint64_t getStreamSize() override { return output->getStreamSize(); }

  void reset() override { output->reset(); }

  uint64_t getEstimatedSpaceNeeded() override {
    return output->getEstimatedSpaceNeeded();
  }

 private:
  std::unique_ptr<SeekableOutputStream> output;
};

}  // namespace orc

#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_RLE_V0_H_
