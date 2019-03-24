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

#include <immintrin.h>
#include <string.h>

#include <algorithm>
#include <iostream>
#include <utility>

#include "storage/format/orc/byte-rle.h"
#include "storage/format/orc/exceptions.h"

namespace orc {

const size_t MINIMUM_REPEAT = 3;

ByteRleDecoder::~ByteRleDecoder() {
  // PASS
}

void ByteRleDecoderImpl::nextBuffer() {
  int bufferLength = 0;
  const void *bufferPointer = nullptr;
  bool result = inputStream->Next(&bufferPointer, &bufferLength);
  if (!result) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "bad read in nextBuffer");
  }
  bufferStart = static_cast<const char *>(bufferPointer);
  bufferEnd = bufferStart + bufferLength;
}

signed char ByteRleDecoderImpl::readByte() {
  if (bufferStart == bufferEnd) {
    nextBuffer();
  }
  return *(bufferStart++);
}

void ByteRleDecoderImpl::readHeader() {
  signed char ch = readByte();
  if (ch < 0) {
    remainingValues = static_cast<size_t>(-ch);
    repeating = false;
  } else {
    remainingValues = static_cast<size_t>(ch) + MINIMUM_REPEAT;
    repeating = true;
    value = readByte();
  }
}

ByteRleDecoderImpl::ByteRleDecoderImpl(
    std::unique_ptr<SeekableInputStream> input) {
  inputStream = std::move(input);
  repeating = false;
  remainingValues = 0;
  value = 0;
  bufferStart = 0;
  bufferEnd = 0;
}

ByteRleDecoderImpl::~ByteRleDecoderImpl() {
  // PASS
}

void ByteRleDecoderImpl::seek(PositionProvider &location) {
  // move the input stream
  inputStream->seek(location);
  // force a re-read from the stream
  bufferEnd = bufferStart;
  // read a new header
  readHeader();
  // skip ahead the given number of records
  ByteRleDecoderImpl::skip(location.next());
}

void ByteRleDecoderImpl::skip(uint64_t numValues) {
  while (numValues > 0) {
    if (remainingValues == 0) {
      readHeader();
    }
    size_t count = std::min(static_cast<size_t>(numValues), remainingValues);
    remainingValues -= count;
    numValues -= count;
    // for literals we need to skip over count bytes, which may involve
    // reading from the underlying stream
    if (!repeating) {
      size_t consumedBytes = count;
      while (consumedBytes > 0) {
        if (bufferStart == bufferEnd) {
          nextBuffer();
        }
        size_t skipSize =
            std::min(static_cast<size_t>(consumedBytes),
                     static_cast<size_t>(bufferEnd - bufferStart));
        bufferStart += skipSize;
        consumedBytes -= skipSize;
      }
    }
  }
}

void ByteRleDecoderImpl::next(char *data, uint64_t numValues,
                              const char *notNull) {
  uint64_t position = 0;
  // skip over null values
  while (notNull && position < numValues && !notNull[position]) {
    position += 1;
  }
  while (position < numValues) {
    // if we are out of values, read more
    if (remainingValues == 0) {
      readHeader();
    }
    // how many do we read out of this block?
    size_t count =
        std::min(static_cast<size_t>(numValues - position), remainingValues);
    uint64_t consumed = 0;
    if (repeating) {
      if (notNull) {
        for (uint64_t i = 0; i < count; ++i) {
          if (notNull[position + i]) {
            data[position + i] = value;
            consumed += 1;
          }
        }
      } else {
        memset(data + position, value, count);
        consumed = count;
      }
    } else {
      if (notNull) {
        for (uint64_t i = 0; i < count; ++i) {
          if (notNull[position + i]) {
            data[position + i] = readByte();
            consumed += 1;
          }
        }
      } else {
        uint64_t i = 0;
        while (i < count) {
          if (bufferStart == bufferEnd) {
            nextBuffer();
          }
          uint64_t copyBytes =
              std::min(static_cast<uint64_t>(count - i),
                       static_cast<uint64_t>(bufferEnd - bufferStart));
          memcpy(data + position + i, bufferStart, copyBytes);
          bufferStart += copyBytes;
          i += copyBytes;
        }
        consumed = count;
      }
    }
    remainingValues -= consumed;
    position += count;
    // skip over any null values
    while (notNull && position < numValues && !notNull[position]) {
      position += 1;
    }
  }
}

std::unique_ptr<ByteRleDecoder> createByteRleDecoder(
    std::unique_ptr<SeekableInputStream> input) {
  return std::unique_ptr<ByteRleDecoder>(
      new ByteRleDecoderImpl(std::move(input)));
}

class BooleanRleDecoderImpl : public ByteRleDecoderImpl {
 public:
  explicit BooleanRleDecoderImpl(std::unique_ptr<SeekableInputStream> input);

  virtual ~BooleanRleDecoderImpl();

  void seek(PositionProvider &) override;

  void skip(uint64_t numValues) override;

  void next(char *data, uint64_t numValues, const char *notNull) override;

 protected:
  size_t remainingBits = 0;
  char lastByte = 0;
};

BooleanRleDecoderImpl::BooleanRleDecoderImpl(
    std::unique_ptr<SeekableInputStream> input)
    : ByteRleDecoderImpl(std::move(input)) {}

BooleanRleDecoderImpl::~BooleanRleDecoderImpl() {
  // PASS
}

void BooleanRleDecoderImpl::seek(PositionProvider &location) {
  ByteRleDecoderImpl::seek(location);
  uint64_t consumed = location.next();
  if (consumed > 8) {
    throw ParseError("bad position");
  }
  if (consumed != 0) {
    remainingBits = 8 - consumed;
    ByteRleDecoderImpl::next(&lastByte, 1, nullptr);
  }
}

void BooleanRleDecoderImpl::skip(uint64_t numValues) {
  if (numValues <= remainingBits) {
    remainingBits -= numValues;
  } else {
    numValues -= remainingBits;
    uint64_t bytesSkipped = numValues / 8;
    ByteRleDecoderImpl::skip(bytesSkipped);
    ByteRleDecoderImpl::next(&lastByte, 1, nullptr);
    remainingBits = 8 - (numValues % 8);
  }
}

void BooleanRleDecoderImpl::next(char *__restrict__ data, uint64_t numValues,
                                 const char *__restrict__ notNull) {
  // next spot to fill in
  uint64_t position = 0;

  // use up any remaining bits
  if (notNull) {
    while (remainingBits > 0 && position < numValues) {
      if (notNull[position]) {
        remainingBits -= 1;
        data[position] =
            (static_cast<unsigned char>(lastByte) >> remainingBits) & 0x1;
      } else {
        data[position] = 0;
      }
      position += 1;
    }
  } else {
    while (remainingBits > 0 && position < numValues) {
      remainingBits -= 1;
      data[position++] =
          (static_cast<unsigned char>(lastByte) >> remainingBits) & 0x1;
    }
  }

  // count the number of nonNulls remaining
  uint64_t nonNulls = numValues - position;
  if (notNull) {
    for (uint64_t i = position; i < numValues; ++i) {
      if (!notNull[i]) {
        nonNulls -= 1;
      }
    }
  }

  // fill in the remaining values
  if (nonNulls == 0) {
    while (position < numValues) {
      data[position++] = 0;
    }
  } else if (position < numValues) {
    // read the new bytes into the array
    uint64_t bytesRead = (nonNulls + 7) / 8;
    ByteRleDecoderImpl::next(data + position, bytesRead, nullptr);
    lastByte = data[position + bytesRead - 1];
    remainingBits = bytesRead * 8 - nonNulls;
    // expand the array backwards so that we don't clobber the data
    uint64_t bitsLeft = bytesRead * 8 - remainingBits;
    if (notNull) {
      for (int64_t i = static_cast<int64_t>(numValues) - 1;
           i >= static_cast<int64_t>(position); --i) {
        if (notNull[i]) {
          uint64_t shiftPosn = (-bitsLeft) % 8;
          data[i] = (data[position + (bitsLeft - 1) / 8] >> shiftPosn) & 0x1;
          bitsLeft -= 1;
        } else {
          data[i] = 0;
        }
      }
    } else {
      // performance: edit the code below carefully
      const char *__restrict__ dataSrc = data;
      int64_t i = static_cast<int64_t>(numValues) - 1;
#ifdef AVX_OPT
      int64_t positionEnd = i - (i - position + 1) % 16;
      assert((positionEnd - position + 1) % 16 == 0);
#else
      int64_t positionEnd = static_cast<int64_t>(position) - 1;
#endif
      // step 1: remove the back element to align to 16 byte e.g. 128 bit
      for (; i > positionEnd;) {
        uint8_t shiftPosn = (-bitsLeft) % 8;
        data[i] = (dataSrc[position + (bitsLeft - 1) / 8] >> shiftPosn) & 0x1;
        --i, --bitsLeft;
        if (shiftPosn == 7) break;
      }
      for (; i - 7 > positionEnd; i -= 8, bitsLeft -= 8) {
        char tmpDataSrc = dataSrc[position + (bitsLeft - 1) / 8];
        uint64_t tmpBuf;
#ifdef IS_BIG_ENDIAN
#pragma clang loop unroll(full)
        for (int8_t shiftPosn = 7; shiftPosn >= 0; shiftPosn--) {
          tmpBuf <<= 8;
          tmpBuf |= (char)(tmpDataSrc >> shiftPosn) & 0x1;
        }
#else
#pragma clang loop unroll(full)
        for (int8_t shiftPosn = 0; shiftPosn <= 7; shiftPosn++) {
          tmpBuf <<= 8;
          tmpBuf |= (char)(tmpDataSrc >> shiftPosn) & 0x1;
        }
#endif
        uint64_t *tmpPtr = (uint64_t *)&data[i - 7];
        *tmpPtr = tmpBuf;
      }
// end of step 1
#ifdef AVX_OPT
      // step 2: simd
      // intel cpus are all little endian
      // 2 bytes src e.g. 16 bits expand to 16 bytes e.g. 128 bits
      // todo: there could be more specific version for avx2, avx512
      __m128i *tmpPtr = (__m128i *)&data[i - 15];
      if ((uint64_t)tmpPtr % 16 == 0) {
        // _mm128_store_si128 require aligned, otherwise exception
        __m128i mask = _mm_set1_epi8(0x1);
        for (; i - 15 >= static_cast<int64_t>(position);
             i -= 16, bitsLeft -= 16) {
          const char *tds = &dataSrc[position + (bitsLeft - 1) / 8 - 1];
          __m128i src = _mm_set_epi8(0, 0, 0, 0, 0, 0, 0, tds[1], 0, 0, 0, 0, 0,
                                     0, 0, tds[0]);
          // high to low in register
          __m128i res = _mm_set1_epi8(0x0);
          {
            __m128i tmp;
            // pay attention to shift right logically
            tmp = _mm_slli_si128(src, 0);  // shift the byte
            tmp = _mm_srli_epi64(tmp, 7);  // shift the bit
            res = _mm_or_si128(res, tmp);

            tmp = _mm_slli_si128(src, 1);  // shift the byte
            tmp = _mm_srli_epi64(tmp, 6);  // shift the bit
            res = _mm_or_si128(res, tmp);

            tmp = _mm_slli_si128(src, 2);  // shift the byte
            tmp = _mm_srli_epi64(tmp, 5);  // shift the bit
            res = _mm_or_si128(res, tmp);

            tmp = _mm_slli_si128(src, 3);  // shift the byte
            tmp = _mm_srli_epi64(tmp, 4);  // shift the bit
            res = _mm_or_si128(res, tmp);

            tmp = _mm_slli_si128(src, 4);  // shift the byte
            tmp = _mm_srli_epi64(tmp, 3);  // shift the bit
            res = _mm_or_si128(res, tmp);

            tmp = _mm_slli_si128(src, 5);  // shift the byte
            tmp = _mm_srli_epi64(tmp, 2);  // shift the bit
            res = _mm_or_si128(res, tmp);

            tmp = _mm_slli_si128(src, 6);  // shift the byte
            tmp = _mm_srli_epi64(tmp, 1);  // shift the bit
            res = _mm_or_si128(res, tmp);

            tmp = _mm_slli_si128(src, 7);  // shift the byte
            tmp = _mm_srli_epi64(tmp, 0);  // shift the bit
            res = _mm_or_si128(res, tmp);
          }
          res = _mm_and_si128(res, mask);
          __m128i *tmpPtr = (__m128i *)&data[i - 15];
          _mm_storeu_si128(tmpPtr, res);
        }
      } else {  // address not aligned
        int64_t positionEnd = static_cast<int64_t>(position) - 1;
        for (; i > positionEnd;) {
          uint8_t shiftPosn = (-bitsLeft) % 8;
          data[i] = (dataSrc[position + (bitsLeft - 1) / 8] >> shiftPosn) & 0x1;
          --i, --bitsLeft;
          if (shiftPosn == 7) break;
        }
        for (; i - 7 > positionEnd; i -= 8, bitsLeft -= 8) {
          char tmpDataSrc = dataSrc[position + (bitsLeft - 1) / 8];
          uint64_t tmpBuf;
#pragma clang loop unroll(full)
          for (int8_t shiftPosn = 0; shiftPosn <= 7; shiftPosn++) {
            tmpBuf <<= 8;
            tmpBuf |= (char)(tmpDataSrc >> shiftPosn) & 0x1;
          }
          uint64_t *tmpPtr = (uint64_t *)&data[i - 7];
          *tmpPtr = tmpBuf;
        }
      }
#endif
      assert(bitsLeft == 0);
    }
  }
}

std::unique_ptr<ByteRleDecoder> createBooleanRleDecoder(
    std::unique_ptr<SeekableInputStream> input) {
  BooleanRleDecoderImpl *decoder = new BooleanRleDecoderImpl(std::move(input));
  return std::unique_ptr<ByteRleDecoder>(
      reinterpret_cast<ByteRleDecoder *>(decoder));
}

std::unique_ptr<ByteRleCoder> createByteRleCoder(CompressionKind kind) {
  std::unique_ptr<ByteRleCoder> coder(
      new ByteRleCoder(createBlockCompressor(kind)));
  return std::move(coder);
}

BooleanRleEncoderImpl::BooleanRleEncoderImpl(
    std::unique_ptr<SeekableOutputStream> output)
    : ByteRleCoder(std::move(output)) {
  bitsRemained = 8;
  current = static_cast<char>(0);
}

BooleanRleEncoderImpl::~BooleanRleEncoderImpl() {}

void BooleanRleEncoderImpl::write(const char *data, uint64_t numValues,
                                  const char *notNull) {
  for (uint64_t i = 0; i < numValues; ++i) {
    if (bitsRemained == 0) {
      ByteRleCoder::write(current);
      current = static_cast<char>(0);
      bitsRemained = 8;
    }
    if (!notNull || notNull[i]) {
      if (!data || data[i]) {
        current = static_cast<char>(current | (0x80 >> (8 - bitsRemained)));
      }
      --bitsRemained;
    }
  }
  if (bitsRemained == 0) {
    ByteRleCoder::write(current);
    current = static_cast<char>(0);
    bitsRemained = 8;
  }
}

void BooleanRleEncoderImpl::flush() {
  if (bitsRemained != 8) {
    ByteRleCoder::write(current);
  }
  bitsRemained = 8;
  current = static_cast<char>(0);
  ByteRleCoder::flush();
}

void BooleanRleEncoderImpl::flushToStream(OutputStream *stream) {
  flush();
  ByteRleCoder::flushToStream(stream);
}

std::unique_ptr<BooleanRleEncoderImpl> createBooleanRleEncoderImpl(
    CompressionKind kind) {
  std::unique_ptr<BooleanRleEncoderImpl> coder(
      new BooleanRleEncoderImpl(createBlockCompressor(kind)));
  return std::move(coder);
}
}  // namespace orc
