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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_RLE_V1_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_RLE_V1_H_

#include <algorithm>
#include <memory>
#include <vector>

#include "storage/format/orc/rle.h"

namespace orc {

const uint64_t MINIMUM_REPEAT = 3;
const uint64_t BASE_128_MASK = 0x7f;

template <class IntType, class UIntType>
class RleDecoderV1 : public RleDecoder {
 public:
  RleDecoderV1(std::unique_ptr<SeekableInputStream> input, bool hasSigned)
      : inputStream(std::move(input)), isSigned(hasSigned) {}

  // Seek to a particular spot.
  void seek(PositionProvider& location) override {
    // move the input stream
    inputStream->seek(location);
    // force a re-read from the stream
    bufferEnd = bufferStart;
    // read a new header
    readHeader();
    // skip ahead the given number of records
    skip(location.next());
  }

  // Seek over a given number of values.
  void skip(uint64_t numValues) override {
    while (numValues > 0) {
      if (remainingValues == 0) {
        readHeader();
      }
      uint64_t count = std::min(numValues, remainingValues);
      remainingValues -= count;
      numValues -= count;
      if (repeating) {
        value += delta * static_cast<int64_t>(count);
      } else {
        skipLongs(count);
      }
    }
  }

  // Read a number of values into the batch.
  void next(void* data, uint64_t numValues, const char* notNull) override {
    uint64_t position = 0;
    IntType* dat = reinterpret_cast<IntType*>(data);

    // skipNulls()
    if (notNull) {
      // Skip over null values.
      while (position < numValues && !notNull[position]) {
        ++position;
      }
    }
    while (position < numValues) {
      // If we are out of values, read more.
      if (remainingValues == 0) {
        readHeader();
      }
      // How many do we read out of this block?
      uint64_t count = std::min(numValues - position, remainingValues);
      uint64_t consumed = 0;
      if (repeating) {
        if (notNull) {
          for (uint64_t i = 0; i < count; ++i) {
            if (notNull[position + i]) {
              dat[position + i] = static_cast<IntType>(
                  value + static_cast<int64_t>(consumed) * delta);
              consumed += 1;
            }
          }
        } else {
          for (uint64_t i = 0; i < count; ++i) {
            dat[position + i] =
                static_cast<IntType>(value + static_cast<int64_t>(i) * delta);
          }
          consumed = count;
        }
        value += static_cast<int64_t>(consumed) * delta;
      } else {
        if (notNull) {
          for (uint64_t i = 0; i < count; ++i) {
            if (notNull[position + i]) {
              dat[position + i] =
                  isSigned ? static_cast<IntType>(unZigZag(readLong()))
                           : static_cast<IntType>(readLong());
              ++consumed;
            }
          }
        } else {
          if (isSigned) {
            for (uint64_t i = 0; i < count; ++i) {
              dat[position + i] = static_cast<IntType>(unZigZag(readLong()));
            }
          } else {
            for (uint64_t i = 0; i < count; ++i) {
              dat[position + i] = static_cast<IntType>(readLong());
            }
          }
          consumed = count;
        }
      }
      remainingValues -= consumed;
      position += count;

      // skipNulls()
      if (notNull) {
        // Skip over null values.
        while (position < numValues && !notNull[position]) {
          ++position;
        }
      }
    }
  }

 private:
  signed char readByte() {
    if (bufferStart == bufferEnd) {
      int32_t bufferLength;
      const void* bufferPointer;
      if (!inputStream->Next(&bufferPointer, &bufferLength)) {
        LOG_ERROR(ERRCODE_INTERNAL_ERROR, "bad read in readByte");
      }
      bufferStart = static_cast<const char*>(bufferPointer);
      bufferEnd = bufferStart + bufferLength;
    }
    return *(bufferStart++);
  }

  void readHeader() {
    signed char ch = readByte();
    if (ch < 0) {
      remainingValues = static_cast<uint64_t>(-ch);
      repeating = false;
    } else {
      remainingValues = static_cast<uint64_t>(ch) + MINIMUM_REPEAT;
      repeating = true;
      delta = readByte();
      value =
          isSigned ? unZigZag(readLong()) : static_cast<int64_t>(readLong());
    }
  }

  uint64_t readLong() {
    uint64_t result = 0;
    int64_t offset = 0;
    signed char ch = readByte();
    if (ch >= 0) {
      result = static_cast<uint64_t>(ch);
    } else {
      result = static_cast<uint64_t>(ch) & BASE_128_MASK;
      while ((ch = readByte()) < 0) {
        offset += 7;
        result |= (static_cast<uint64_t>(ch) & BASE_128_MASK) << offset;
      }
      result |= static_cast<uint64_t>(ch) << (offset + 7);
    }
    return result;
  }

  void skipLongs(uint64_t numValues) {
    while (numValues > 0) {
      if (readByte() >= 0) {
        --numValues;
      }
    }
  }

  const std::unique_ptr<SeekableInputStream> inputStream;
  const bool isSigned;
  uint64_t remainingValues = 0;
  int64_t value = 0;
  const char* bufferStart = nullptr;
  const char* bufferEnd = nullptr;
  int64_t delta = 0;
  bool repeating = false;
};

// Integer Run Length Encoding version 1
// Source link:
// https://cwiki.apache.org/confluence/display/Hive/LanguageManual+ORC#LanguageManualORC-IntegerRunLengthEncodingversion1
// //NOLINT
//
// In Hive 0.11 ORC files used Run Length Encoding version 1 (RLEv1),
// which provides a lightweight compression of signed or unsigned integer
// sequences. RLEv1 has two sub-encodings:
// 1) Run - a sequence of values that differ by a small fixed delta
// 2) Literals - a sequence of varint encoded values
//
// Runs start with an initial byte of 0x00 to 0x7f, which encodes the
// length of the run - 3. A second byte provides the fixed delta in the
// range of -128 to 127. Finally, the first value of the run is encoded
// as a base 128 varint.
// For example, if the sequence is 100 instances of 7 the encoding would
// start with 100 - 3, followed by a delta of 0, and a varint of 7 for
// an encoding of [0x61, 0x00, 0x07]. To encode the sequence of numbers
// running from 100 to 1, the first byte is 100 - 3, the delta is -1,
// and the varint is 100 for an encoding of [0x61, 0xff, 0x64].
//
// Literals start with an initial byte of 0x80 to 0xff, which corresponds
// to the negative of number of literals in the sequence. Following the
// header byte, the list of N varints is encoded. Thus, if there are
// no runs, the overhead is 1 byte for each 128 integers. The first 5
// prime numbers [2, 3, 4, 7, 11] would encoded as [0xfb, 0x02, 0x03,
// 0x04, 0x07, 0xb].

template <class IntType>
class RleCoderV1 : public RleCoder {
 public:
  explicit RleCoderV1(std::unique_ptr<SeekableOutputStream> stream,
                      bool hasSigned)
      : output(std::move(stream)),
        isSigned(hasSigned),
        literals(MAX_LITERAL_SIZE) {}

  void write(void* data, uint64_t numValues, const char* notNull) override {
    IntType* d = reinterpret_cast<IntType*>(data);
    for (uint64_t i = 0; i < numValues; i++) {
      if ((notNull == nullptr) || notNull[i]) {
        // LOG_INFO("write value %lld", static_cast<int64_t>(d[i]));
        write(static_cast<int64_t>(d[i]));
      }
    }
  }

  void flushToStream(OutputStream* stream) override {
    writeValues();
    output->flushToStream(stream);
  }

  uint64_t getStreamSize() override { return output->getStreamSize(); }

  void reset() override { output->reset(); }

  uint64_t getEstimatedSpaceNeeded() override {
    return output->getEstimatedSpaceNeeded() + numLiterals * sizeof(int64_t) +
           sizeof(uint8_t)     // delta
           + sizeof(uint8_t);  // control bytes
  }

 private:
  // Write the input value
  // @param value The input value
  // @return Void
  void write(int64_t value) {
    if (numLiterals == 0) {
      literals[numLiterals++] = value;
      tailRunLength = 1;
    } else if (repeat) {
      if (value == literals[0] + delta * numLiterals) {
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
      if (tailRunLength == 1) {
        delta = value - literals[numLiterals - 1];
        if (delta < MIN_DELTA || delta > MAX_DELTA) {
          tailRunLength = 1;
        } else {
          tailRunLength = 2;
        }
      } else if (value == literals[numLiterals - 1] + delta) {
        tailRunLength += 1;
      } else {
        delta = value - literals[numLiterals - 1];
        if (delta < MIN_DELTA || delta > MAX_DELTA) {
          tailRunLength = 1;
        } else {
          tailRunLength = 2;
        }
      }
      if (tailRunLength == MIN_REPEAT_SIZE) {
        if (numLiterals + 1 == MIN_REPEAT_SIZE) {
          repeat = true;
          numLiterals += 1;
        } else {
          numLiterals -= MIN_REPEAT_SIZE - 1;
          int64_t base = literals[numLiterals];
          writeValues();
          literals[0] = base;
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

  void writeValues() {
    if (numLiterals != 0) {
      if (repeat) {
        output->writeByte(numLiterals - MIN_REPEAT_SIZE);
        output->writeByte((int8_t)delta);

        if (isSigned) {
          writeInt64(output.get(), literals[0]);
        } else {
          writeUInt64(output.get(), literals[0]);
        }
      } else {
        output->writeByte(-numLiterals);
        for (uint32_t i = 0; i < numLiterals; ++i) {
          if (isSigned) {
            writeInt64(output.get(), literals[i]);
          } else {
            writeUInt64(output.get(), literals[i]);
          }
        }
      }
      repeat = false;
      numLiterals = 0;
      tailRunLength = 0;
    }
  }

 private:
  std::unique_ptr<SeekableOutputStream> output;
  const bool isSigned = false;

  const int32_t MIN_REPEAT_SIZE = 3;
  const int32_t MAX_DELTA = 127;
  const int32_t MIN_DELTA = -128;
  const int32_t MAX_LITERAL_SIZE = 128;
  const int32_t MAX_REPEAT_SIZE = 127 + MIN_REPEAT_SIZE;

  std::vector<int64_t> literals;
  int32_t numLiterals = 0;
  int64_t delta = 0;
  bool repeat = false;
  int32_t tailRunLength = 0;
};

}  // namespace orc

#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_RLE_V1_H_
