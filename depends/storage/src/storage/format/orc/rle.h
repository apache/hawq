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
#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_RLE_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_RLE_H_

#include <memory>
#include <vector>

#include "storage/format/orc/seekable-input-stream.h"
#include "storage/format/orc/seekable-output-stream.h"

namespace orc {

inline int64_t unZigZag(uint64_t value) { return value >> 1 ^ -(value & 1); }

class RleDecoder {
 public:
  // must be non-inline!
  virtual ~RleDecoder();

  // Seek to a particular spot.
  virtual void seek(PositionProvider &) = 0;

  // Seek over a given number of values.
  virtual void skip(uint64_t numValues) = 0;

  // Read a number of values into the batch.
  // @param data the array to read into
  // @param numValues the number of values to read
  // @param notNull If the pointer is null, all values are read. If the
  //    pointer is not null, positions that are false are skipped.
  virtual void next(void *data, uint64_t numValues, const char *notNull) = 0;
};

enum RleVersion { RleVersion_1, RleVersion_2, RleVersion_0 };

// Create an RLE decoder.
// @param input The input stream to read from
// @param isSigned True if the number sequence is signed
// @param version Version of RLE decoding to do
// @param pool Memory pool to use for allocation
// @return The RLE decoder
std::unique_ptr<RleDecoder> createRleDecoder(
    std::unique_ptr<SeekableInputStream> input, bool isSigned,
    RleVersion version, dbcommon::MemoryPool &pool,  // NOLINT
    ORCTypeKind type = LONG);

class RleCoder {
 public:
  RleCoder() { this->writeBuffer.resize(BUFFER_SIZE); }
  virtual ~RleCoder() {}

  // Write a number of values out.
  // @param data The array to write
  // @param numValues The number of values to write
  // @param notNull If the pointer is null, all values are read. If the
  //   pointer is not null, positions that are false are skipped.
  virtual void write(void *data, uint64_t numValues, const char *notNull) = 0;

  // Flush the buffer to the given output stream
  // @param os The output stream
  // @return Void
  virtual void flushToStream(OutputStream *os) = 0;

  // Get stream size. This function just calls the
  // getStreamSize() function of the underlying stream.
  // So this size should be obtained after flushToStream.
  // Otherwise there might be some buffers in RleCoders that are not
  // been flushed to underlying stream.
  // @return The stream size.
  virtual uint64_t getStreamSize() = 0;

  // Get the estimated space for the data that have been written to
  // this coder.
  // @return The estimated space
  virtual uint64_t getEstimatedSpaceNeeded() = 0;

  // Rest this RleCoder, and everything is reset
  // @return Void
  virtual void reset() = 0;

 protected:
  // Here varint encoding is used:
  // https://developers.google.com/protocol-buffers/docs/encoding#varints
  // ">>" is arithmetic shift
  // @param os The output stream.
  // @param value The signed 64-bit integer to write out
  // @return Void
  void writeInt64(orc::SeekableOutputStream *os, int64_t value) {
    writeUInt64(os, zigzagEncode(value));
  }

  // Write out the value in Varint (ittle endian format)
  // https://developers.google.com/protocol-buffers/docs/encoding#varints
  // @param os The output stream
  // @param value The unsigned 64-bit integer to write out
  // @return Void
  void writeUInt64(orc::SeekableOutputStream *os, uint64_t value) {
    while (true) {
      if ((value & ~0x7f) == 0) {
        os->writeByte((int8_t)value);
        return;
      } else {
        os->writeByte((int8_t)(0x80 | (value & 0x7f)));
        value >>= 7;
      }
    }
  }

  // Bitpack and write the input values to underlying output stream
  // @param input - values to write
  // @param offset - offset
  // @param len - length
  // @param bitSize - bit width
  // @param output - output stream
  // @return Void
  void writeInts(int64_t *input, int32_t offset, int32_t len, int32_t bitSize,
                 SeekableOutputStream *output) {
    if (input == nullptr || offset < 0 || len < 1 || bitSize < 1) {
      LOG_ERROR(ERRCODE_INVALID_PARAMETER_VALUE, "invalid parameter value");
    }

    switch (bitSize) {
      case 1:
        unrolledBitPack1(input, offset, len, output);
        return;
      case 2:
        unrolledBitPack2(input, offset, len, output);
        return;
      case 4:
        unrolledBitPack4(input, offset, len, output);
        return;
      case 8:
        unrolledBitPack8(input, offset, len, output);
        return;
      case 16:
        unrolledBitPack16(input, offset, len, output);
        return;
      case 24:
        unrolledBitPack24(input, offset, len, output);
        return;
      case 32:
        unrolledBitPack32(input, offset, len, output);
        return;
      case 40:
        unrolledBitPack40(input, offset, len, output);
        return;
      case 48:
        unrolledBitPack48(input, offset, len, output);
        return;
      case 56:
        unrolledBitPack56(input, offset, len, output);
        return;
      case 64:
        unrolledBitPack64(input, offset, len, output);
        return;
      default:
        break;
    }

    int32_t bitsLeft = 8;
    int8_t current = 0;
    for (int32_t i = offset; i < (offset + len); i++) {
      int64_t value = input[i];
      int32_t bitsToWrite = bitSize;
      while (bitsToWrite > bitsLeft) {
        // add the bits to the bottom of the current word
        current |= ((uint64_t)value) >> (bitsToWrite - bitsLeft);
        // subtract out the bits we just added
        bitsToWrite -= bitsLeft;
        // zero out the bits above bitsToWrite
        value &= (1LL << bitsToWrite) - 1;
        output->write(current);
        current = 0;
        bitsLeft = 8;
      }
      bitsLeft -= bitsToWrite;
      current |= value << bitsLeft;
      if (bitsLeft == 0) {
        output->writeByte(current);
        current = 0;
        bitsLeft = 8;
      }
    }

    // flush
    if (bitsLeft != 8) {
      output->writeByte(current);
      current = 0;
      bitsLeft = 8;
    }
  }

  // Bitpack and write the input values (only 1 bit per input value)
  // to underlying output stream
  // @param input - values to write
  // @param offset - offset
  // @param len - length
  // @param bitSize - bit width
  // @param output - output stream
  // @return Void
  void unrolledBitPack1(int64_t *input, int32_t offset, int32_t len,
                        SeekableOutputStream *output) {
    int32_t numHops = 8;
    int32_t remainder = len % numHops;
    int32_t endOffset = offset + len;
    int32_t endUnroll = endOffset - remainder;
    int32_t val = 0;
    for (int32_t i = offset; i < endUnroll; i = i + numHops) {
      val = (int32_t)(val | ((input[i] & 1) << 7) | ((input[i + 1] & 1) << 6) |
                      ((input[i + 2] & 1) << 5) | ((input[i + 3] & 1) << 4) |
                      ((input[i + 4] & 1) << 3) | ((input[i + 5] & 1) << 2) |
                      ((input[i + 6] & 1) << 1) | ((input[i + 7]) & 1));
      output->writeByte(val);
      val = 0;
    }

    if (remainder > 0) {
      int32_t startShift = 7;
      for (int32_t i = endUnroll; i < endOffset; i++) {
        val = (int32_t)(val | (input[i] & 1) << startShift);
        startShift -= 1;
      }
      output->writeByte(val);
    }
  }

  void unrolledBitPack2(int64_t *input, int32_t offset, int32_t len,
                        SeekableOutputStream *output) {
    int32_t numHops = 4;
    int32_t remainder = len % numHops;
    int32_t endOffset = offset + len;
    int32_t endUnroll = endOffset - remainder;
    int32_t val = 0;
    for (int32_t i = offset; i < endUnroll; i = i + numHops) {
      val = static_cast<int32_t>(
          val | ((input[i] & 3) << 6) | ((input[i + 1] & 3) << 4) |
          ((input[i + 2] & 3) << 2) | ((input[i + 3]) & 3));
      output->writeByte(val);
      val = 0;
    }

    if (remainder > 0) {
      int32_t startShift = 6;
      for (int32_t i = endUnroll; i < endOffset; i++) {
        val = static_cast<int32_t>(val | (input[i] & 3) << startShift);
        startShift -= 2;
      }
      output->writeByte(val);
    }
  }

  void unrolledBitPack4(int64_t *input, int32_t offset, int32_t len,
                        SeekableOutputStream *output) {
    int32_t numHops = 2;
    int32_t remainder = len % numHops;
    int32_t endOffset = offset + len;
    int32_t endUnroll = endOffset - remainder;
    int val = 0;
    for (int32_t i = offset; i < endUnroll; i = i + numHops) {
      val = (int32_t)(val | ((input[i] & 15) << 4) | ((input[i + 1]) & 15));
      output->writeByte(val);
      val = 0;
    }

    if (remainder > 0) {
      int32_t startShift = 4;
      for (int32_t i = endUnroll; i < endOffset; i++) {
        val = (int32_t)(val | (input[i] & 15) << startShift);
        startShift -= 4;
      }
      output->writeByte(val);
    }
  }

  void unrolledBitPack8(int64_t *input, int32_t offset, int32_t len,
                        SeekableOutputStream *output) {
    unrolledBitPackBytes(input, offset, len, output, 1);
  }

  void unrolledBitPack16(int64_t *input, int32_t offset, int32_t len,
                         SeekableOutputStream *output) {
    unrolledBitPackBytes(input, offset, len, output, 2);
  }

  void unrolledBitPack24(int64_t *input, int32_t offset, int32_t len,
                         SeekableOutputStream *output) {
    unrolledBitPackBytes(input, offset, len, output, 3);
  }

  void unrolledBitPack32(int64_t *input, int32_t offset, int32_t len,
                         SeekableOutputStream *output) {
    unrolledBitPackBytes(input, offset, len, output, 4);
  }

  void unrolledBitPack40(int64_t *input, int32_t offset, int32_t len,
                         SeekableOutputStream *output) {
    unrolledBitPackBytes(input, offset, len, output, 5);
  }

  void unrolledBitPack48(int64_t *input, int32_t offset, int32_t len,
                         SeekableOutputStream *output) {
    unrolledBitPackBytes(input, offset, len, output, 6);
  }

  void unrolledBitPack56(int64_t *input, int32_t offset, int32_t len,
                         SeekableOutputStream *output) {
    unrolledBitPackBytes(input, offset, len, output, 7);
  }

  void unrolledBitPack64(int64_t *input, int32_t offset, int32_t len,
                         SeekableOutputStream *output) {
    unrolledBitPackBytes(input, offset, len, output, 8);
  }

  void unrolledBitPackBytes(int64_t *input, int32_t offset, int32_t len,
                            SeekableOutputStream *output, int32_t numBytes) {
    int32_t numHops = 8;
    int32_t remainder = len % numHops;
    int32_t endOffset = offset + len;
    int32_t endUnroll = endOffset - remainder;
    int32_t i = offset;
    for (; i < endUnroll; i = i + numHops) {
      writeLongBE(output, input, i, numHops, numBytes);
    }

    if (remainder > 0) {
      writeRemainingLongs(output, i, input, remainder, numBytes);
    }
  }

  void writeRemainingLongs(SeekableOutputStream *output, int32_t offset,
                           int64_t *input, int32_t remainder,
                           int32_t numBytes) {
    int32_t numHops = remainder;

    int idx = 0;
    switch (numBytes) {
      case 1:
        while (remainder > 0) {
          writeBuffer[idx] = (int8_t)(input[offset + idx] & 255);
          remainder--;
          idx++;
        }
        break;
      case 2:
        while (remainder > 0) {
          writeLongBE2(output, input[offset + idx], idx * 2);
          remainder--;
          idx++;
        }
        break;
      case 3:
        while (remainder > 0) {
          writeLongBE3(output, input[offset + idx], idx * 3);
          remainder--;
          idx++;
        }
        break;
      case 4:
        while (remainder > 0) {
          writeLongBE4(output, input[offset + idx], idx * 4);
          remainder--;
          idx++;
        }
        break;
      case 5:
        while (remainder > 0) {
          writeLongBE5(output, input[offset + idx], idx * 5);
          remainder--;
          idx++;
        }
        break;
      case 6:
        while (remainder > 0) {
          writeLongBE6(output, input[offset + idx], idx * 6);
          remainder--;
          idx++;
        }
        break;
      case 7:
        while (remainder > 0) {
          writeLongBE7(output, input[offset + idx], idx * 7);
          remainder--;
          idx++;
        }
        break;
      case 8:
        while (remainder > 0) {
          writeLongBE8(output, input[offset + idx], idx * 8);
          remainder--;
          idx++;
        }
        break;
      default:
        break;
    }

    int32_t toWrite = numHops * numBytes;
    output->write(reinterpret_cast<const char *>(writeBuffer.data()), toWrite);
  }

  void writeLongBE(SeekableOutputStream *output, int64_t *input, int32_t offset,
                   int32_t numHops, int32_t numBytes) {
    switch (numBytes) {
      case 1:
        writeBuffer[0] = (uint8_t)(input[offset + 0] & 255);
        writeBuffer[1] = (uint8_t)(input[offset + 1] & 255);
        writeBuffer[2] = (uint8_t)(input[offset + 2] & 255);
        writeBuffer[3] = (uint8_t)(input[offset + 3] & 255);
        writeBuffer[4] = (uint8_t)(input[offset + 4] & 255);
        writeBuffer[5] = (uint8_t)(input[offset + 5] & 255);
        writeBuffer[6] = (uint8_t)(input[offset + 6] & 255);
        writeBuffer[7] = (uint8_t)(input[offset + 7] & 255);
        break;
      case 2:
        writeLongBE2(output, input[offset + 0], 0);
        writeLongBE2(output, input[offset + 1], 2);
        writeLongBE2(output, input[offset + 2], 4);
        writeLongBE2(output, input[offset + 3], 6);
        writeLongBE2(output, input[offset + 4], 8);
        writeLongBE2(output, input[offset + 5], 10);
        writeLongBE2(output, input[offset + 6], 12);
        writeLongBE2(output, input[offset + 7], 14);
        break;
      case 3:
        writeLongBE3(output, input[offset + 0], 0);
        writeLongBE3(output, input[offset + 1], 3);
        writeLongBE3(output, input[offset + 2], 6);
        writeLongBE3(output, input[offset + 3], 9);
        writeLongBE3(output, input[offset + 4], 12);
        writeLongBE3(output, input[offset + 5], 15);
        writeLongBE3(output, input[offset + 6], 18);
        writeLongBE3(output, input[offset + 7], 21);
        break;
      case 4:
        writeLongBE4(output, input[offset + 0], 0);
        writeLongBE4(output, input[offset + 1], 4);
        writeLongBE4(output, input[offset + 2], 8);
        writeLongBE4(output, input[offset + 3], 12);
        writeLongBE4(output, input[offset + 4], 16);
        writeLongBE4(output, input[offset + 5], 20);
        writeLongBE4(output, input[offset + 6], 24);
        writeLongBE4(output, input[offset + 7], 28);
        break;
      case 5:
        writeLongBE5(output, input[offset + 0], 0);
        writeLongBE5(output, input[offset + 1], 5);
        writeLongBE5(output, input[offset + 2], 10);
        writeLongBE5(output, input[offset + 3], 15);
        writeLongBE5(output, input[offset + 4], 20);
        writeLongBE5(output, input[offset + 5], 25);
        writeLongBE5(output, input[offset + 6], 30);
        writeLongBE5(output, input[offset + 7], 35);
        break;
      case 6:
        writeLongBE6(output, input[offset + 0], 0);
        writeLongBE6(output, input[offset + 1], 6);
        writeLongBE6(output, input[offset + 2], 12);
        writeLongBE6(output, input[offset + 3], 18);
        writeLongBE6(output, input[offset + 4], 24);
        writeLongBE6(output, input[offset + 5], 30);
        writeLongBE6(output, input[offset + 6], 36);
        writeLongBE6(output, input[offset + 7], 42);
        break;
      case 7:
        writeLongBE7(output, input[offset + 0], 0);
        writeLongBE7(output, input[offset + 1], 7);
        writeLongBE7(output, input[offset + 2], 14);
        writeLongBE7(output, input[offset + 3], 21);
        writeLongBE7(output, input[offset + 4], 28);
        writeLongBE7(output, input[offset + 5], 35);
        writeLongBE7(output, input[offset + 6], 42);
        writeLongBE7(output, input[offset + 7], 49);
        break;
      case 8:
        writeLongBE8(output, input[offset + 0], 0);
        writeLongBE8(output, input[offset + 1], 8);
        writeLongBE8(output, input[offset + 2], 16);
        writeLongBE8(output, input[offset + 3], 24);
        writeLongBE8(output, input[offset + 4], 32);
        writeLongBE8(output, input[offset + 5], 40);
        writeLongBE8(output, input[offset + 6], 48);
        writeLongBE8(output, input[offset + 7], 56);
        break;
      default:
        break;
    }

    int32_t toWrite = numHops * numBytes;
    output->write(reinterpret_cast<const char *>(writeBuffer.data()), toWrite);
  }

  void writeLongBE2(SeekableOutputStream *output, int64_t val,
                    int32_t wbOffset) {
    writeBuffer[wbOffset + 0] = (uint8_t)((uint64_t)val >> 8);
    writeBuffer[wbOffset + 1] = (uint8_t)((uint64_t)val >> 0);
  }

  void writeLongBE3(SeekableOutputStream *output, int64_t val,
                    int32_t wbOffset) {
    writeBuffer[wbOffset + 0] = (uint8_t)((uint64_t)val >> 16);
    writeBuffer[wbOffset + 1] = (uint8_t)((uint64_t)val >> 8);
    writeBuffer[wbOffset + 2] = (uint8_t)((uint64_t)val >> 0);
  }

  void writeLongBE4(SeekableOutputStream *output, int64_t val,
                    int32_t wbOffset) {
    writeBuffer[wbOffset + 0] = (uint8_t)((uint64_t)val >> 24);
    writeBuffer[wbOffset + 1] = (uint8_t)((uint64_t)val >> 16);
    writeBuffer[wbOffset + 2] = (uint8_t)((uint64_t)val >> 8);
    writeBuffer[wbOffset + 3] = (uint8_t)((uint64_t)val >> 0);
  }

  void writeLongBE5(SeekableOutputStream *output, int64_t val,
                    int32_t wbOffset) {
    writeBuffer[wbOffset + 0] = (uint8_t)((uint64_t)val >> 32);
    writeBuffer[wbOffset + 1] = (uint8_t)((uint64_t)val >> 24);
    writeBuffer[wbOffset + 2] = (uint8_t)((uint64_t)val >> 16);
    writeBuffer[wbOffset + 3] = (uint8_t)((uint64_t)val >> 8);
    writeBuffer[wbOffset + 4] = (uint8_t)((uint64_t)val >> 0);
  }

  void writeLongBE6(SeekableOutputStream *output, int64_t val,
                    int32_t wbOffset) {
    writeBuffer[wbOffset + 0] = (uint8_t)((uint64_t)val >> 40);
    writeBuffer[wbOffset + 1] = (uint8_t)((uint64_t)val >> 32);
    writeBuffer[wbOffset + 2] = (uint8_t)((uint64_t)val >> 24);
    writeBuffer[wbOffset + 3] = (uint8_t)((uint64_t)val >> 16);
    writeBuffer[wbOffset + 4] = (uint8_t)((uint64_t)val >> 8);
    writeBuffer[wbOffset + 5] = (uint8_t)((uint64_t)val >> 0);
  }

  void writeLongBE7(SeekableOutputStream *output, int64_t val,
                    int32_t wbOffset) {
    writeBuffer[wbOffset + 0] = (uint8_t)((uint64_t)val >> 48);
    writeBuffer[wbOffset + 1] = (uint8_t)((uint64_t)val >> 40);
    writeBuffer[wbOffset + 2] = (uint8_t)((uint64_t)val >> 32);
    writeBuffer[wbOffset + 3] = (uint8_t)((uint64_t)val >> 24);
    writeBuffer[wbOffset + 4] = (uint8_t)((uint64_t)val >> 16);
    writeBuffer[wbOffset + 5] = (uint8_t)((uint64_t)val >> 8);
    writeBuffer[wbOffset + 6] = (uint8_t)((uint64_t)val >> 0);
  }

  void writeLongBE8(SeekableOutputStream *output, int64_t val,
                    int32_t wbOffset) {
    writeBuffer[wbOffset + 0] = (uint8_t)((uint64_t)val >> 56);
    writeBuffer[wbOffset + 1] = (uint8_t)((uint64_t)val >> 48);
    writeBuffer[wbOffset + 2] = (uint8_t)((uint64_t)val >> 40);
    writeBuffer[wbOffset + 3] = (uint8_t)((uint64_t)val >> 32);
    writeBuffer[wbOffset + 4] = (uint8_t)((uint64_t)val >> 24);
    writeBuffer[wbOffset + 5] = (uint8_t)((uint64_t)val >> 16);
    writeBuffer[wbOffset + 6] = (uint8_t)((uint64_t)val >> 8);
    writeBuffer[wbOffset + 7] = (uint8_t)((uint64_t)val >> 0);
  }

  // zigzag encode the given value
  // @param val
  // @return zigzag encoded value
  uint64_t zigzagEncode(int64_t val) { return (val << 1) ^ (val >> 63); }

 private:
  const int32_t BUFFER_SIZE = 64;
  std::vector<uint8_t> writeBuffer;
};

// Create an RLE coder.
// @param isSigned true if the number sequence is signed
// @param version version of RLE decoding to do
// @param type The type
// @param kind The compression method
// @param alignedBitpacking Whether to use aligned bitpacking
// @return The RLE coder
std::unique_ptr<RleCoder> createRleCoder(
    bool isSigned, RleVersion version, ORCTypeKind type, CompressionKind kind,
    bool alignedBitpacking = false);  // NOLINT

}  // namespace orc

#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_RLE_H_
