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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_BYTE_RLE_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_BYTE_RLE_H_

#include <memory>
#include <vector>

#include "storage/format/orc/rle.h"
#include "storage/format/orc/seekable-input-stream.h"
#include "storage/format/orc/seekable-output-stream.h"

namespace orc {

class ByteRleDecoder {
 public:
  virtual ~ByteRleDecoder();

  // Seek to a particular spot.
  // @param pos The position to seek
  // @return void
  virtual void seek(PositionProvider& pos) = 0;  // NOLINT

  // Seek over a given number of values.
  // @param numValues Skip the number of values
  // @return void
  virtual void skip(uint64_t numValues) = 0;

  // Read a number of values into the batch.
  // @param data the array to read into
  // @param numValues the number of values to read
  // @param notNull If the pointer is null, all values are read. If the
  //    pointer is not null, positions that are false are skipped.
  virtual void next(char* data, uint64_t numValues, const char* notNull) = 0;
};

// Create a byte RLE decoder.
// @param input the input stream to read from
// @return The decoder
std::unique_ptr<ByteRleDecoder> createByteRleDecoder(
    std::unique_ptr<SeekableInputStream> input);

// Create a boolean RLE decoder.
// Unlike the other RLE decoders, the boolean decoder sets the data to 0
// if the value is masked by notNull. This is required for the notNull stream
// processing to properly apply multiple masks from nested types.
// @param input the input stream to read from
// @return The boolean RLE decoder
std::unique_ptr<ByteRleDecoder> createBooleanRleDecoder(
    std::unique_ptr<SeekableInputStream> input);

class ByteRleDecoderImpl : public ByteRleDecoder {
 public:
  explicit ByteRleDecoderImpl(std::unique_ptr<SeekableInputStream> input);

  virtual ~ByteRleDecoderImpl();

  void seek(PositionProvider&) override;

  void skip(uint64_t numValues) override;

  void next(char* data, uint64_t numValues, const char* notNull) override;

 protected:
  inline void nextBuffer();
  inline signed char readByte();
  inline void readHeader();

  std::unique_ptr<SeekableInputStream> inputStream;
  size_t remainingValues;
  char value;
  const char* bufferStart;
  const char* bufferEnd;
  bool repeating;
};

// Run length byte encoder. A control byte is written before
// each run with positive values 0 to 127 meaning 2 to 129 repetitions. If the
// bytes is -1 to -128, 1 to 128 literal byte values follow.
class ByteRleCoder : public RleCoder {
 public:
  explicit ByteRleCoder(std::unique_ptr<SeekableOutputStream> stream)
      : output(std::move(stream)), literals(MAX_LITERAL_SIZE) {}
  ~ByteRleCoder() {}

  void flushToStream(OutputStream* os) override {
    writeValues();
    output->flushToStream(os);
  }

  void flush() { writeValues(); }

  uint64_t getStreamSize() override { return output->getStreamSize(); }

  void reset() override {
    output->reset();
    repeat = false;
    tailRunLength = 0;
    numLiterals = 0;
  }

  uint64_t getEstimatedSpaceNeeded() override {
    // This is the maximal space used.
    // It might not be accurate.
    return output->getEstimatedSpaceNeeded() + sizeof(int8_t) * numLiterals +
           sizeof(int8_t) /* control byte*/;
  }

  void write(void* data, uint64_t numValues, const char* notNull) override {
    int8_t* d = reinterpret_cast<int8_t*>(data);

    if (notNull) {
      for (uint64_t i = 0; i < numValues; i++) {
        if (notNull[i]) {
          write(d[i]);
        }
      }
    } else {
      for (uint64_t i = 0; i < numValues; i++) {
        write(d[i]);
      }
    }
  }

  void write(int8_t value) {
    if (numLiterals == 0) {
      literals[numLiterals++] = value;
      tailRunLength = 1;
    } else if (repeat) {
      if (value == literals[0]) {
        numLiterals += 1;
        if (numLiterals == MAX_REPEAT_SIZE) {
          writeValues();
        }
      } else {
        writeValues();
        literals[numLiterals++] = value;
        tailRunLength = 1;
      }
    } else {
      if (value == literals[numLiterals - 1]) {
        tailRunLength += 1;
      } else {
        tailRunLength = 1;
      }
      if (tailRunLength == MIN_REPEAT_SIZE) {
        if (numLiterals + 1 == MIN_REPEAT_SIZE) {
          repeat = true;
          numLiterals += 1;
        } else {
          numLiterals -= MIN_REPEAT_SIZE - 1;
          writeValues();
          literals[0] = value;
          repeat = true;
          numLiterals = MIN_REPEAT_SIZE;
        }
      } else {
        literals[numLiterals++] = value;
        if (numLiterals == MAX_LITERAL_SIZE) {
          writeValues();
        }
      }
    }
  }

 private:
  void writeValues() {
    if (numLiterals != 0) {
      if (repeat) {
        assert(numLiterals - MIN_REPEAT_SIZE >= 0);
        output->write<int8_t>(numLiterals - MIN_REPEAT_SIZE);
        output->write(reinterpret_cast<const char*>(literals.data()),
                      sizeof(int8_t) * 1);
      } else {
        output->write<int8_t>(-numLiterals);
        output->write(reinterpret_cast<const char*>(literals.data()),
                      numLiterals * sizeof(int8_t));
      }
      repeat = false;
      tailRunLength = 0;
      numLiterals = 0;
    }
  }

 private:
  const int32_t MIN_REPEAT_SIZE = 3;
  const int32_t MAX_LITERAL_SIZE = 128;
  const int32_t MAX_REPEAT_SIZE = 127 + MIN_REPEAT_SIZE;

  std::unique_ptr<SeekableOutputStream> output;
  std::vector<int8_t> literals;
  int32_t numLiterals = 0;
  bool repeat = false;
  int32_t tailRunLength = 0;
};

// Create a byte RLE coder.
// @param output  The output stream to write to
// @return The coder
std::unique_ptr<ByteRleCoder> createByteRleCoder(CompressionKind kind);

class BooleanRleEncoderImpl : public ByteRleCoder {
 public:
  BooleanRleEncoderImpl(std::unique_ptr<SeekableOutputStream> output);
  virtual ~BooleanRleEncoderImpl() override;

  virtual void write(const char* data, uint64_t numValues, const char* notNull);

  virtual void flush();
  virtual void flushToStream(OutputStream* stream) override;

 private:
  int bitsRemained;
  char current;
};
std::unique_ptr<BooleanRleEncoderImpl> createBooleanRleEncoderImpl(
    CompressionKind kind);

}  // end of namespace orc
#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_BYTE_RLE_H_
