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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_RLE_V2_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_RLE_V2_H_

#include <algorithm>
#include <cmath>
#include <memory>
#include <utility>
#include <vector>

#include "storage/format/orc/exceptions.h"
#include "storage/format/orc/rle.h"

namespace orc {

#define MIN_REPEAT 3

struct FixedBitSizes {
  enum FBS {
    ONE = 0,
    TWO,
    THREE,
    FOUR,
    FIVE,
    SIX,
    SEVEN,
    EIGHT,
    NINE,
    TEN,
    ELEVEN,
    TWELVE,
    THIRTEEN,
    FOURTEEN,
    FIFTEEN,
    SIXTEEN,
    SEVENTEEN,
    EIGHTEEN,
    NINETEEN,
    TWENTY,
    TWENTYONE,
    TWENTYTWO,
    TWENTYTHREE,
    TWENTYFOUR,
    TWENTYSIX,
    TWENTYEIGHT,
    THIRTY,
    THIRTYTWO,
    FORTY,
    FORTYEIGHT,
    FIFTYSIX,
    SIXTYFOUR
  };
};

enum EncodingType {
  SHORT_REPEAT = 0,
  DIRECT = 1,
  PATCHED_BASE = 2,
  DELTA = 3,
  UNKNOWN = 4
};

// Decodes the ordinal fixed bit value to actual fixed bit width value
// @param n - encoded fixed bit width
// @return decoded fixed bit width
inline uint32_t decodeBitWidth(uint32_t n) {
  if (n <= FixedBitSizes::TWENTYFOUR) {
    return n + 1;
  } else if (n == FixedBitSizes::TWENTYSIX) {
    return 26;
  } else if (n == FixedBitSizes::TWENTYEIGHT) {
    return 28;
  } else if (n == FixedBitSizes::THIRTY) {
    return 30;
  } else if (n == FixedBitSizes::THIRTYTWO) {
    return 32;
  } else if (n == FixedBitSizes::FORTY) {
    return 40;
  } else if (n == FixedBitSizes::FORTYEIGHT) {
    return 48;
  } else if (n == FixedBitSizes::FIFTYSIX) {
    return 56;
  } else {
    return 64;
  }
}

template <class IntType, class UIntType>
class RleDecoderV2 : public RleDecoder {
 public:
  RleDecoderV2(std::unique_ptr<SeekableInputStream> input, bool isSigned,
               dbcommon::MemoryPool &pool)  // NOLINT
      : inputStream(std::move(input)),
        isSigned(isSigned),
        unpacked(pool, 0),
        unpackedPatch(pool, 0) {
    // PASS
  }

  // Seek to a particular spot.
  void seek(PositionProvider &location) override {
    // move the input stream
    inputStream->seek(location);
    // clear state
    bufferEnd = bufferStart = 0;
    runRead = runLength = 0;
    // skip ahead the given number of records
    skip(location.next());
  }

  // Seek over a given number of values.
  void skip(uint64_t numValues) override {
    // simple for now, until perf tests indicate something
    // encoding specific is needed
    const uint64_t N = 64;
    int64_t dummy[N];

    while (numValues) {
      uint64_t nRead = std::min(N, numValues);
      next(dummy, nRead, nullptr);
      numValues -= nRead;
    }
  }

  // Read a number of values into the batch.
  void next(void *data, uint64_t numValues, const char *notNull) override {
    uint64_t nRead = 0;
    IntType *dat = reinterpret_cast<IntType *>(data);

    while (nRead < numValues) {
      // Skip any nulls before attempting to read first byte.
      if (notNull) {
        while (!notNull[nRead]) {
          if (++nRead == numValues) {
            return;  // ended with null values
          }
        }
      }

      if (runRead == runLength) {
        resetRun();
        firstByte = readByte();
      }

      uint64_t offset = nRead, length = numValues - nRead;

      EncodingType enc = static_cast<EncodingType>((firstByte >> 6) & 0x03);
      switch (static_cast<int64_t>(enc)) {
        case SHORT_REPEAT:
          nRead += nextShortRepeats(dat, offset, length, notNull);
          break;
        case DIRECT:
          nRead += nextDirect(dat, offset, length, notNull);
          break;
        case PATCHED_BASE:
          nRead += nextPatched(dat, offset, length, notNull);
          break;
        case DELTA:
          nRead += nextDelta(dat, offset, length, notNull);
          break;
        default:
          LOG_ERROR(ERRCODE_INTERNAL_ERROR, "unknown encoding");
      }
    }
  }

 private:
  // Used by PATCHED_BASE
  void adjustGapAndPatch() {
    curGap = static_cast<uint64_t>(unpackedPatch[patchIdx]) >> patchBitSize;
    curPatch = unpackedPatch[patchIdx] & patchMask;
    actualGap = 0;

    // special case: gap is >255 then patch value will be 0.
    // if gap is <=255 then patch value cannot be 0
    while (curGap == 255 && curPatch == 0) {
      actualGap += 255;
      ++patchIdx;
      curGap = static_cast<uint64_t>(unpackedPatch[patchIdx]) >> patchBitSize;
      curPatch = unpackedPatch[patchIdx] & patchMask;
    }
    // add the left over gap
    actualGap += curGap;
  }

  void resetReadLongs() {
    bitsLeft = 0;
    curByte = 0;
  }

  void resetRun() {
    resetReadLongs();
    bitSize = 0;
  }

  unsigned char readByte() {
    assert(bufferStart <= bufferEnd);
    if (bufferStart == bufferEnd) {
      int32_t bufferLength;
      const void *bufferPointer;
      if (!inputStream->Next(&bufferPointer, &bufferLength)) {
        // fixme: LOG_ERROR should be alarmed
        // LOG_ERROR(ERRCODE_INTERNAL_ERROR, "bad read in
        // RleDecoderV2::readByte");
      }
      bufferStart = static_cast<const char *>(bufferPointer);
      bufferEnd = bufferStart + bufferLength;
    }

    unsigned char result = static_cast<unsigned char>(*bufferStart++);
    return result;
  }

  int64_t readLongBE(uint64_t bsz) {
    int64_t ret = 0;
    uint64_t n = bsz;
    while (n > 0) {
      n--;
      auto val = readByte();
      ret <<= 8;
      ret |= val;
    }
    return ret;
  }

  template <std::size_t N>
  int64_t readLongBEQuickInternal(const char *bufferStart) {
    // todo(xxx) more optimization on a big endian machine
    int64_t ret = 0;
#pragma clang loop unroll(full)
    for (uint64_t n = N; n > 0; n--) {
      auto val = static_cast<unsigned char>(*bufferStart++);
      ret <<= 8;
      ret |= val;
    }
    return ret;
  }
  inline int64_t readLongBEQuick(uint64_t bsz) {
    assert(bufferStart + bsz - 1 < bufferEnd);

    int64_t ret;
    switch (bsz) {
      case 8:
        ret = readLongBEQuickInternal<8>(bufferStart);
        break;
      case 7:
        ret = readLongBEQuickInternal<7>(bufferStart);
        break;
      case 6:
        ret = readLongBEQuickInternal<6>(bufferStart);
        break;
      case 5:
        ret = readLongBEQuickInternal<5>(bufferStart);
        break;
      case 4:
        ret = readLongBEQuickInternal<4>(bufferStart);
        break;
      case 3:
        ret = readLongBEQuickInternal<3>(bufferStart);
        break;
      case 2:
        ret = readLongBEQuickInternal<2>(bufferStart);
        break;
      case 1:
        ret = readLongBEQuickInternal<1>(bufferStart);
        break;
    }
    bufferStart += bsz;
    return ret;
  }

  int64_t readVslong() { return unZigZag(readVulong()); }

  uint64_t readVulong() {
    uint64_t ret = 0, b;
    uint64_t offset = 0;
    do {
      b = readByte();
      ret |= (0x7f & b) << offset;
      offset += 7;
    } while (b >= 0x80);
    return ret;
  }

  template <class ReadIntType>
  uint64_t readLongs(ReadIntType *data, uint64_t offset, uint64_t len,
                     uint64_t fb, const char *notNull = nullptr) {
    switch (fb) {
      case 1:
        return readLongs<ReadIntType, 1>(data, offset, len, notNull);
      case 2:
        return readLongs<ReadIntType, 2>(data, offset, len, notNull);
      case 3:
        return readLongs<ReadIntType, 3>(data, offset, len, notNull);
      case 4:
        return readLongs<ReadIntType, 4>(data, offset, len, notNull);
      case 5:
        return readLongs<ReadIntType, 5>(data, offset, len, notNull);
      case 6:
        return readLongs<ReadIntType, 6>(data, offset, len, notNull);
      case 7:
        return readLongs<ReadIntType, 7>(data, offset, len, notNull);
      case 8:
        return readLongs<ReadIntType, 8>(data, offset, len, notNull);
      case 9:
        return readLongs<ReadIntType, 9>(data, offset, len, notNull);
      case 10:
        return readLongs<ReadIntType, 10>(data, offset, len, notNull);
      case 11:
        return readLongs<ReadIntType, 11>(data, offset, len, notNull);
      case 12:
        return readLongs<ReadIntType, 12>(data, offset, len, notNull);
      case 13:
        return readLongs<ReadIntType, 13>(data, offset, len, notNull);
      case 14:
        return readLongs<ReadIntType, 14>(data, offset, len, notNull);
      case 15:
        return readLongs<ReadIntType, 15>(data, offset, len, notNull);
      case 16:
        return readLongs<ReadIntType, 16>(data, offset, len, notNull);
      case 17:
        return readLongs<ReadIntType, 17>(data, offset, len, notNull);
      case 18:
        return readLongs<ReadIntType, 18>(data, offset, len, notNull);
      case 19:
        return readLongs<ReadIntType, 19>(data, offset, len, notNull);
      case 20:
        return readLongs<ReadIntType, 20>(data, offset, len, notNull);
      case 21:
        return readLongs<ReadIntType, 21>(data, offset, len, notNull);
      case 22:
        return readLongs<ReadIntType, 22>(data, offset, len, notNull);
      case 23:
        return readLongs<ReadIntType, 23>(data, offset, len, notNull);
      case 24:
        return readLongs<ReadIntType, 24>(data, offset, len, notNull);
      case 25:
        return readLongs<ReadIntType, 25>(data, offset, len, notNull);
      case 26:
        return readLongs<ReadIntType, 26>(data, offset, len, notNull);
      case 27:
        return readLongs<ReadIntType, 27>(data, offset, len, notNull);
      case 28:
        return readLongs<ReadIntType, 28>(data, offset, len, notNull);
      case 29:
        return readLongs<ReadIntType, 29>(data, offset, len, notNull);
      case 30:
        return readLongs<ReadIntType, 30>(data, offset, len, notNull);
      case 31:
        return readLongs<ReadIntType, 31>(data, offset, len, notNull);
      case 32:
        return readLongs<ReadIntType, 32>(data, offset, len, notNull);
      case 33:
        return readLongs<ReadIntType, 33>(data, offset, len, notNull);
      case 34:
        return readLongs<ReadIntType, 34>(data, offset, len, notNull);
      case 35:
        return readLongs<ReadIntType, 35>(data, offset, len, notNull);
      case 36:
        return readLongs<ReadIntType, 36>(data, offset, len, notNull);
      case 37:
        return readLongs<ReadIntType, 37>(data, offset, len, notNull);
      case 38:
        return readLongs<ReadIntType, 38>(data, offset, len, notNull);
      case 39:
        return readLongs<ReadIntType, 39>(data, offset, len, notNull);
      case 40:
        return readLongs<ReadIntType, 40>(data, offset, len, notNull);
      case 41:
        return readLongs<ReadIntType, 41>(data, offset, len, notNull);
      case 42:
        return readLongs<ReadIntType, 42>(data, offset, len, notNull);
      case 43:
        return readLongs<ReadIntType, 43>(data, offset, len, notNull);
      case 44:
        return readLongs<ReadIntType, 44>(data, offset, len, notNull);
      case 45:
        return readLongs<ReadIntType, 45>(data, offset, len, notNull);
      case 46:
        return readLongs<ReadIntType, 46>(data, offset, len, notNull);
      case 47:
        return readLongs<ReadIntType, 47>(data, offset, len, notNull);
      case 48:
        return readLongs<ReadIntType, 48>(data, offset, len, notNull);
      case 49:
        return readLongs<ReadIntType, 49>(data, offset, len, notNull);
      case 50:
        return readLongs<ReadIntType, 50>(data, offset, len, notNull);
      case 51:
        return readLongs<ReadIntType, 51>(data, offset, len, notNull);
      case 52:
        return readLongs<ReadIntType, 52>(data, offset, len, notNull);
      case 53:
        return readLongs<ReadIntType, 53>(data, offset, len, notNull);
      case 54:
        return readLongs<ReadIntType, 54>(data, offset, len, notNull);
      case 55:
        return readLongs<ReadIntType, 55>(data, offset, len, notNull);
      case 56:
        return readLongs<ReadIntType, 56>(data, offset, len, notNull);
      case 57:
        return readLongs<ReadIntType, 57>(data, offset, len, notNull);
      case 58:
        return readLongs<ReadIntType, 58>(data, offset, len, notNull);
      case 59:
        return readLongs<ReadIntType, 59>(data, offset, len, notNull);
      case 60:
        return readLongs<ReadIntType, 60>(data, offset, len, notNull);
      case 61:
        return readLongs<ReadIntType, 61>(data, offset, len, notNull);
      case 62:
        return readLongs<ReadIntType, 62>(data, offset, len, notNull);
      case 63:
        return readLongs<ReadIntType, 63>(data, offset, len, notNull);
      case 64:
        return readLongs<ReadIntType, 64>(data, offset, len, notNull);
    }
    return 0;
  }
  template <class ReadIntType, uint64_t FixedBitLength,
            uint64_t Mask = (FixedBitLength == 64)
                                ? -1
                                : ((uint64_t)1 << FixedBitLength) - 1>
  uint64_t readLongs(ReadIntType *data, uint64_t offset, uint64_t len,
                     const char *notNull = nullptr) {
    auto curByte = this->curByte;
    auto bitsLeft = this->bitsLeft;
    // TODO(xxx): unroll to improve performance
    if (notNull) {
      uint64_t ret = 0;
      for (uint64_t i = offset; i < (offset + len); i++) {
        // skip null positions
        if (!notNull[i]) {
          continue;
        }
        uint64_t result = 0;
        uint64_t bitsLeftToRead = FixedBitLength;
        while (bitsLeftToRead > bitsLeft) {
          result <<= bitsLeft;
          result |= curByte & ((1 << bitsLeft) - 1);
          bitsLeftToRead -= bitsLeft;
          curByte = readByte();
          bitsLeft = 8;
        }

        // handle the left over bits
        if (bitsLeftToRead > 0) {
          result <<= bitsLeftToRead;
          bitsLeft -= static_cast<uint32_t>(bitsLeftToRead);
          result |= (curByte >> bitsLeft) & ((1 << bitsLeftToRead) - 1);
        }
        data[i] = static_cast<ReadIntType>(result);
        ++ret;
      }
      this->curByte = curByte;
      this->bitsLeft = bitsLeft;
      return ret;
    } else {
      auto bufferStart = this->bufferStart;
      auto bufferEnd = this->bufferEnd;
      for (uint64_t i = offset; i < (offset + len); i++) {
        // 1. Fast decode
        {  // performance: reduce the cost on the checking the buffer and
           // reassigning the bitsLeft

          uint64_t curLong = curByte;
          while (bufferStart + 8 < bufferEnd && i < (offset + len)) {
            uint64_t result;
            if (bitsLeft >= FixedBitLength) {
              result = (curLong >> (bitsLeft - FixedBitLength));
              result &= Mask;
            } else {
              uint64_t lastLong = curLong;
              curLong = (*reinterpret_cast<const uint64_t *>(bufferStart));
              curLong = __builtin_bswap64(curLong);
              bufferStart += 8;
              if (FixedBitLength != 64) {
                result = (lastLong << (FixedBitLength - bitsLeft));
                result |= (curLong >> (64 - (FixedBitLength - bitsLeft)));
              } else {
                result = curLong;
              }
              result &= Mask;
              bitsLeft += 64;
            }
            bitsLeft -= FixedBitLength;

            data[i] = static_cast<ReadIntType>(result);
            i++;
          }
          uint8_t trimBits = 0;
          while (bitsLeft >= 8) {
            bufferStart -= 1;
            bitsLeft -= 8;
            trimBits += 8;
          }
          curByte = static_cast<uint8_t>(curLong >> trimBits);
          assert(bufferStart <= bufferEnd);
        }
        if (i >= (offset + len)) break;

        // 2. Normal decode

        // update buffer info
        this->bufferStart = bufferStart;
        this->bufferEnd = bufferEnd;

        uint64_t result = 0;
        uint64_t bitsLeftToRead = FixedBitLength;
        while (bitsLeftToRead > bitsLeft) {
          result <<= bitsLeft;
          result |= curByte & ((1 << bitsLeft) - 1);
          bitsLeftToRead -= bitsLeft;
          curByte = readByte();
          bitsLeft = 8;
        }

        // handle the left over bits
        if (bitsLeftToRead > 0) {
          result <<= bitsLeftToRead;
          bitsLeft -= static_cast<uint32_t>(bitsLeftToRead);
          result |= (curByte >> bitsLeft) & ((1 << bitsLeftToRead) - 1);
        }
        data[i] = static_cast<ReadIntType>(result);

        // update buffer info
        bufferStart = this->bufferStart;
        bufferEnd = this->bufferEnd;
      }
      assert(bufferStart <= bufferEnd);
      this->bufferStart = bufferStart;
      this->bufferEnd = bufferEnd;
    }
    this->curByte = curByte;
    this->bitsLeft = bitsLeft;

    return len;
  }

  inline uint32_t getClosestFixedBits(uint32_t n) {
    if (n == 0) {
      return 1;
    }

    if (n >= 1 && n <= 24) {
      return n;
    } else if (n > 24 && n <= 26) {
      return 26;
    } else if (n > 26 && n <= 28) {
      return 28;
    } else if (n > 28 && n <= 30) {
      return 30;
    } else if (n > 30 && n <= 32) {
      return 32;
    } else if (n > 32 && n <= 40) {
      return 40;
    } else if (n > 40 && n <= 48) {
      return 48;
    } else if (n > 48 && n <= 56) {
      return 56;
    } else {
      return 64;
    }
  }

  uint64_t nextShortRepeats(IntType *data, uint64_t offset, uint64_t numValues,
                            const char *notNull) {
    if (runRead == runLength) {
      // extract the number of fixed bytes
      byteSize = (firstByte >> 3) & 0x07;
      byteSize += 1;

      runLength = firstByte & 0x07;
      // run lengths values are stored only after MIN_REPEAT value is met
      runLength += MIN_REPEAT;
      runRead = 0;

      // read the repeated value which is store using fixed bytes
      if (bufferStart + byteSize - 1 < bufferEnd)
        firstValue = readLongBEQuick(byteSize);
      else
        firstValue = readLongBE(byteSize);

      if (isSigned) {
        firstValue = unZigZag(static_cast<uint64_t>(firstValue));
      }
    }

    uint64_t nRead = std::min(runLength - runRead, numValues);

    int64_t firstValue = this->firstValue;
    if (notNull) {
      // performance: reduce mem acces on this->runRead
      auto runRead = this->runRead;
      for (auto pos = offset; pos < offset + nRead; ++pos) {
        if (notNull[pos]) {
          data[pos] = firstValue;
          runRead++;
        }
      }
      this->runRead = runRead;
    } else {
#pragma clang vectorize(enable)
      auto pos = offset;
      for (pos = offset; pos < offset + nRead; ++pos) {
        data[pos] = firstValue;
      }
      runRead += pos - offset;
    }

    return nRead;
  }

  uint64_t nextDirect(IntType *data, uint64_t offset, uint64_t numValues,
                      const char *notNull) {
    if (runRead == runLength) {
      // extract the number of fixed bits
      unsigned char fbo = (firstByte >> 1) & 0x1f;
      bitSize = decodeBitWidth(fbo);

      // extract the run length
      runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
      runLength |= readByte();
      // runs are one off
      runLength += 1;
      runRead = 0;
    }

    uint64_t nRead = std::min(runLength - runRead, numValues);

    runRead += readLongs<IntType>(data, offset, nRead, bitSize, notNull);

    if (isSigned) {
      if (notNull) {
        for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
          if (notNull[pos]) {
            // note here, we must cast to UIntType first, instead of
            // casting to uint64_t directly. since if data[pos] is negative
            // casting it to uint64_t will become a very big number.
            // This is why we add UIntType template class to this class.
            data[pos] = unZigZag(static_cast<UIntType>(data[pos]));
          }
        }
      } else {
        for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
          data[pos] = unZigZag(static_cast<UIntType>(data[pos]));
        }
      }
    }

    return nRead;
  }

  uint64_t nextPatched(IntType *data, uint64_t offset, uint64_t numValues,
                       const char *notNull) {
    if (runRead == runLength) {
      // extract the number of fixed bits
      unsigned char fbo = (firstByte >> 1) & 0x1f;
      bitSize = decodeBitWidth(fbo);

      // extract the run length
      runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
      runLength |= readByte();
      // runs are one off
      runLength += 1;
      runRead = 0;

      // extract the number of bytes occupied by base
      uint64_t thirdByte = readByte();
      byteSize = (thirdByte >> 5) & 0x07;
      // base width is one off
      byteSize += 1;

      // extract patch width
      uint32_t pwo = thirdByte & 0x1f;
      patchBitSize = decodeBitWidth(pwo);

      // read fourth byte and extract patch gap width
      uint64_t fourthByte = readByte();
      uint32_t pgw = (fourthByte >> 5) & 0x07;
      // patch gap width is one off
      pgw += 1;

      // extract the length of the patch list
      size_t pl = fourthByte & 0x1f;
      if (pl == 0) {
        LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                  "Corrupt PATCHED_BASE encoded data (pl==0)!");
      }

      // read the next base width number of bytes to extract base value
      base = readLongBE(byteSize);
      int64_t mask = (static_cast<int64_t>(1) << ((byteSize * 8) - 1));
      // if mask of base value is 1 then base is negative value else positive
      if ((base & mask) != 0) {
        base = base & ~mask;
        base = -base;
      }

      // TODO(xxx): something more efficient than resize
      unpacked.resize(runLength);
      unpackedIdx = 0;
      readLongs<int64_t>(unpacked.data(), 0, runLength, bitSize);
      // any remaining bits are thrown out
      resetReadLongs();

      // TODO(xxx): something more efficient than resize
      unpackedPatch.resize(pl);
      patchIdx = 0;
      // TODO(xxx): Skip corrupt?
      //    if ((patchBitSize + pgw) > 64 && !skipCorrupt) {
      if ((patchBitSize + pgw) > 64) {
        LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                  "Corrupt PATCHED_BASE encoded data "
                  "(patchBitSize + pgw > 64)!");
      }
      uint32_t cfb = getClosestFixedBits(patchBitSize + pgw);
      readLongs<int64_t>(unpackedPatch.data(), 0, pl, cfb);
      // any remaining bits are thrown out
      resetReadLongs();

      // apply the patch directly when decoding the packed data
      patchMask = ((static_cast<int64_t>(1) << patchBitSize) - 1);

      adjustGapAndPatch();
    }

    uint64_t nRead = std::min(runLength - runRead, numValues);

    for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
      // skip null positions
      if (notNull && !notNull[pos]) {
        continue;
      }
      if (static_cast<int64_t>(unpackedIdx) != actualGap) {
        // no patching required. add base to unpacked value to get final value
        data[pos] = base + unpacked[unpackedIdx];
      } else {
        // extract the patch value
        int64_t patchedVal = unpacked[unpackedIdx] | (curPatch << bitSize);

        // add base to patched value
        data[pos] = base + patchedVal;

        // increment the patch to point to next entry in patch list
        ++patchIdx;

        if (patchIdx < unpackedPatch.size()) {
          adjustGapAndPatch();

          // next gap is relative to the current gap
          actualGap += unpackedIdx;
        }
      }

      ++runRead;
      ++unpackedIdx;
    }

    return nRead;
  }

  uint64_t nextDelta(IntType *data, uint64_t offset, uint64_t numValues,
                     const char *notNull) {
    auto runRead = this->runRead;
    auto prevValue = this->prevValue;
    auto deltaBase = this->deltaBase;
    if (runRead == runLength) {
      // extract the number of fixed bits
      unsigned char fbo = (firstByte >> 1) & 0x1f;
      if (fbo != 0) {
        bitSize = decodeBitWidth(fbo);
      } else {
        bitSize = 0;
      }

      // extract the run length
      runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
      runLength |= readByte();
      ++runLength;  // account for first value
      runRead = deltaBase = 0;

      // read the first value stored as vint
      if (isSigned) {
        firstValue = static_cast<int64_t>(readVslong());
      } else {
        firstValue = static_cast<int64_t>(readVulong());
      }

      prevValue = firstValue;

      // read the fixed delta value stored as vint (deltas can be negative even
      // if all number are positive)
      deltaBase = static_cast<int64_t>(readVslong());
    }

    uint64_t nRead = std::min(runLength - runRead, numValues);

    uint64_t pos = offset;
    for (; pos < offset + nRead; ++pos) {
      // skip null positions
      if (!notNull || notNull[pos]) break;
    }
    if (runRead == 0 && pos < offset + nRead) {
      data[pos++] = firstValue;
      ++runRead;
    }

    if (bitSize == 0) {
      // add fixed deltas to adjacent values
      if (notNull) {
        for (; pos < offset + nRead; ++pos) {
          // skip null positions
          if (!notNull[pos]) {
            continue;
          }
          prevValue = data[pos] = prevValue + deltaBase;
          ++runRead;
        }
      } else {
        for (; pos < offset + nRead; ++pos) {
          prevValue = data[pos] = prevValue + deltaBase;
          ++runRead;
        }
      }
    } else {
      for (; pos < offset + nRead; ++pos) {
        // skip null positions
        if (!notNull || notNull[pos]) break;
      }
      if (runRead < 2 && pos < offset + nRead) {
        // add delta base and first value
        prevValue = data[pos++] = firstValue + deltaBase;
        ++runRead;
      }

      // write the unpacked values, add it to previous value and store final
      // value to result buffer. if the delta base value is negative then it
      // is a decreasing sequence else an increasing sequence
      uint64_t remaining = (offset + nRead) - pos;
      runRead += readLongs<IntType>(data, pos, remaining, bitSize, notNull);

      if (deltaBase < 0) {
        for (; pos < offset + nRead; ++pos) {
          // skip null positions
          if (notNull && !notNull[pos]) {
            continue;
          }
          prevValue = data[pos] = prevValue - data[pos];
        }
      } else {
        for (; pos < offset + nRead; ++pos) {
          // skip null positions
          if (notNull && !notNull[pos]) {
            continue;
          }
          prevValue = data[pos] = prevValue + data[pos];
        }
      }
    }
    this->runRead = runRead;
    this->prevValue = prevValue;
    this->deltaBase = deltaBase;
    return nRead;
  }

  const std::unique_ptr<SeekableInputStream> inputStream;
  const bool isSigned = false;

  unsigned char firstByte = 0;
  uint64_t runLength = 0;
  uint64_t runRead = 0;
  const char *bufferStart = nullptr;
  const char *bufferEnd = nullptr;
  int64_t deltaBase = 0;              // Used by DELTA
  uint64_t byteSize = 0;              // Used by SHORT_REPEAT and PATCHED_BASE
  int64_t firstValue = 0;             // Used by SHORT_REPEAT and DELTA
  int64_t prevValue = 0;              // Used by DELTA
  uint32_t bitSize = 0;               // Used by DIRECT, PATCHED_BASE and DELTA
  uint8_t bitsLeft = 0;               // Used by anything that uses readLongs
  uint8_t curByte = 0;                // Used by anything that uses readLongs
  uint32_t patchBitSize = 0;          // Used by PATCHED_BASE
  uint64_t unpackedIdx = 0;           // Used by PATCHED_BASE
  uint64_t patchIdx = 0;              // Used by PATCHED_BASE
  int64_t base = 0;                   // Used by PATCHED_BASE
  uint64_t curGap = 0;                // Used by PATCHED_BASE
  int64_t curPatch = 0;               // Used by PATCHED_BASE
  int64_t patchMask = 0;              // Used by PATCHED_BASE
  int64_t actualGap = 0;              // Used by PATCHED_BASE
  DataBuffer<int64_t> unpacked;       // Used by PATCHED_BASE
  DataBuffer<int64_t> unpackedPatch;  // Used by PATCHED_BASE
};

template <class IntType>
class RleCoderV2 : public RleCoder {
 public:
  explicit RleCoderV2(std::unique_ptr<SeekableOutputStream> stream,
                      bool isSigned)
      : output(std::move(stream)),
        isSigned(isSigned),
        literals(MAX_SCOPE),
        zigzagLiterals(MAX_SCOPE),
        baseRedLiterals(MAX_SCOPE),
        adjDeltas(MAX_SCOPE) {
    clear();
  }

  RleCoderV2(std::unique_ptr<SeekableOutputStream> stream, bool isSigned,
             bool alignedBitpacking)
      : output(std::move(stream)),
        isSigned(isSigned),
        literals(MAX_SCOPE),
        zigzagLiterals(MAX_SCOPE),
        baseRedLiterals(MAX_SCOPE),
        adjDeltas(MAX_SCOPE),
        alignedBitpacking(alignedBitpacking) {
    clear();
  }

  void write(void *data, uint64_t numValues, const char *notNull) override {
    IntType *d = reinterpret_cast<IntType *>(data);
    for (uint64_t i = 0; i < numValues; i++) {
      if ((notNull == nullptr) || notNull[i]) {
        write(static_cast<int64_t>(d[i]));
      }
    }
  }

  uint64_t getStreamSize() override { return output->getStreamSize(); }

  uint64_t getEstimatedSpaceNeeded() override {
    return output->getEstimatedSpaceNeeded() + numLiterals * sizeof(int64_t) +
           sizeof(uint8_t) * 10;  // maximal value: header + base + delta
  }

  void flushToStream(OutputStream *os) override {
    if (numLiterals != 0) {
      if (variableRunLength != 0) {
        determineEncoding();
        writeValues();
      } else if (fixedRunLength != 0) {
        if (fixedRunLength < MIN_REPEAT) {
          variableRunLength = fixedRunLength;
          fixedRunLength = 0;
          determineEncoding();
          writeValues();
        } else if (fixedRunLength >= MIN_REPEAT &&
                   fixedRunLength <= MAX_SHORT_REPEAT_LENGTH) {
          encoding = EncodingType::SHORT_REPEAT;
          writeValues();
        } else {
          encoding = EncodingType::DELTA;
          isFixedDelta = true;
          writeValues();
        }
      }
    }
    output->flushToStream(os);
  }

  void reset() override { output->reset(); }

 private:
  void write(int64_t val) {
    if (numLiterals == 0) {
      initializeLiterals(val);
    } else {
      if (numLiterals == 1) {
        prevDelta = val - literals[0];
        literals[numLiterals++] = val;
        // if both values are same count as fixed run else variable run
        if (val == literals[0]) {
          fixedRunLength = 2;
          variableRunLength = 0;
        } else {
          fixedRunLength = 0;
          variableRunLength = 2;
        }
      } else {
        int64_t currentDelta = val - literals[numLiterals - 1];
        if (prevDelta == 0 && currentDelta == 0) {
          // fixed delta run

          literals[numLiterals++] = val;

          // if variable run is non-zero then we are seeing repeating
          // values at the end of variable run in which case keep
          // updating variable and fixed runs
          if (variableRunLength > 0) {
            fixedRunLength = 2;
          }
          fixedRunLength += 1;

          // if fixed run met the minimum condition and if variable
          // run is non-zero then flush the variable run and shift the
          // tail fixed runs to start of the buffer
          if (fixedRunLength >= MIN_REPEAT && variableRunLength > 0) {
            numLiterals -= MIN_REPEAT;
            variableRunLength -= MIN_REPEAT - 1;
            // copy the tail fixed runs
            std::vector<int64_t> tailVals(MIN_REPEAT);
            assert(literals.size() > numLiterals);

            memcpy(tailVals.data(), &literals[numLiterals],
                   MIN_REPEAT * sizeof(int64_t));

            // System.arraycopy(literals, numLiterals, tailVals, 0, MIN_REPEAT);

            // determine variable encoding and flush values
            determineEncoding();
            writeValues();

            // shift tail fixed runs to beginning of the buffer
            for (int64_t l : tailVals) {
              literals[numLiterals++] = l;
            }
          }

          // if fixed runs reached max repeat length then write values
          if (fixedRunLength == MAX_SCOPE) {
            determineEncoding();
            writeValues();
          }
        } else {
          // variable delta run

          // if fixed run length is non-zero and if it satisfies the
          // short repeat conditions then write the values as short repeats
          // else use delta encoding
          if (fixedRunLength >= MIN_REPEAT) {
            if (fixedRunLength <= MAX_SHORT_REPEAT_LENGTH) {
              encoding = EncodingType::SHORT_REPEAT;
              writeValues();
            } else {
              encoding = EncodingType::DELTA;
              isFixedDelta = true;
              writeValues();
            }
          }

          // if fixed run length is <MIN_REPEAT and current value is
          // different from previous then treat it as variable run
          if (fixedRunLength > 0 && fixedRunLength < MIN_REPEAT) {
            if (val != literals[numLiterals - 1]) {
              variableRunLength = fixedRunLength;
              fixedRunLength = 0;
            }
          }

          // after writing values re-initialize the variables
          if (numLiterals == 0) {
            initializeLiterals(val);
          } else {
            // keep updating variable run lengths
            prevDelta = val - literals[numLiterals - 1];
            literals[numLiterals++] = val;
            variableRunLength += 1;

            // if variable run length reach the max scope, write it
            if (variableRunLength == MAX_SCOPE) {
              determineEncoding();
              writeValues();
            }
          }
        }
      }
    }
  }

  // Compute the bits required to represent pth percentile value
  // @param data - array
  // @param p - percentile value (>=0.0 to <=1.0)
  // @return pth percentile bits
  int32_t percentileBits(int64_t *data, int32_t offset, int32_t length,
                         double p) {
    if ((p > 1.0) || (p <= 0.0)) {
      return -1;
    }

    // histogram that store the encoded bit requirement for each values.
    // maximum number of bits that can encoded is 32 (refer FixedBitSizes)
    // int[] hist = new int[32];
    std::vector<int32_t> hist(32);

    // compute the histogram
    for (int32_t i = offset; i < (offset + length); i++) {
      int32_t idx = encodeBitWidth(findClosestNumBits(data[i]));
      hist[idx] += 1;
    }

    int32_t perLen = (int32_t)(length * (1.0 - p));

    // return the bits required by pth percentile length
    for (int32_t i = hist.size() - 1; i >= 0; i--) {
      perLen -= hist[i];
      if (perLen < 0) {
        return decodeBitWidth(i);
      }
    }

    return 0;
  }

  // Do not want to use Guava LongMath.checkedSubtract() here as it will throw
  // ArithmeticException in case of overflow
  bool isSafeSubtract(int64_t left, int64_t right) {
    return (left ^ right) >= 0 | (left ^ (left - right)) >= 0;
  }

  void determineEncoding() {
    // we need to compute zigzag values for DIRECT encoding if we decide to
    // break early for delta overflows or for shorter runs
    computeZigZagLiterals();

    zzBits100p = percentileBits(zigzagLiterals.data(), 0, numLiterals, 1.0);

    // not a big win for shorter runs to determine encoding
    if (numLiterals <= MIN_REPEAT) {
      encoding = EncodingType::DIRECT;
      return;
    }

    // DELTA encoding check

    // for identifying monotonic sequences
    bool isIncreasing = true;
    bool isDecreasing = true;
    this->isFixedDelta = true;

    this->min = literals[0];
    int64_t max = literals[0];
    int64_t initialDelta = literals[1] - literals[0];
    int64_t currDelta = initialDelta;
    int64_t deltaMax = initialDelta;
    this->adjDeltas[0] = initialDelta;

    for (int32_t i = 1; i < numLiterals; i++) {
      int64_t l1 = literals[i];
      int64_t l0 = literals[i - 1];
      currDelta = l1 - l0;
      min = std::min(min, l1);
      max = std::max(max, l1);

      isIncreasing &= (l0 <= l1);
      isDecreasing &= (l0 >= l1);

      isFixedDelta &= (currDelta == initialDelta);
      if (i > 1) {
        adjDeltas[i - 1] = std::abs(currDelta);
        deltaMax = std::max(deltaMax, adjDeltas[i - 1]);
      }
    }

    // its faster to exit under delta overflow condition without checking for
    // PATCHED_BASE condition as encoding using DIRECT is faster and has less
    // overhead than PATCHED_BASE
    if (!isSafeSubtract(max, min)) {
      encoding = EncodingType::DIRECT;
      return;
    }

    // invariant - subtracting any number from any other in the literals after
    // this point won't overflow

    // if min is equal to max then the delta is 0, this condition happens for
    // fixed values run >10 which cannot be encoded with SHORT_REPEAT
    if (min == max) {
      assert(isFixedDelta == true);
      assert(currDelta == 0);
      fixedDelta = 0;
      encoding = EncodingType::DELTA;
      return;
    }

    if (isFixedDelta) {
      assert(currDelta == initialDelta);
      encoding = EncodingType::DELTA;
      fixedDelta = currDelta;
      return;
    }

    // if initialDelta is 0 then we cannot delta encode as we cannot identify
    // the sign of deltas (increasing or decreasing)
    if (initialDelta != 0) {
      // stores the number of bits required for packing delta blob in
      // delta encoding
      bitsDeltaMax = findClosestNumBits(deltaMax);

      // monotonic condition
      if (isIncreasing || isDecreasing) {
        encoding = EncodingType::DELTA;
        return;
      }
    }

    // PATCHED_BASE encoding check

    // percentile values are computed for the zigzag encoded values. if the
    // number of bit requirement between 90th and 100th percentile varies
    // beyond a threshold then we need to patch the values. if the variation
    // is not significant then we can use direct encoding

    zzBits90p = percentileBits(zigzagLiterals.data(), 0, numLiterals, 0.9);
    int32_t diffBitsLH = zzBits100p - zzBits90p;

    // if the difference between 90th percentile and 100th percentile fixed
    // bits is > 1 then we need patch the values
    if (diffBitsLH > 1) {
      // patching is done only on base reduced values.
      // remove base from literals
      for (int32_t i = 0; i < numLiterals; i++) {
        baseRedLiterals[i] = literals[i] - min;
      }

      // 95th percentile width is used to determine max allowed value
      // after which patching will be done
      brBits95p = percentileBits(baseRedLiterals.data(), 0, numLiterals, 0.95);

      // 100th percentile is used to compute the max patch width
      brBits100p = percentileBits(baseRedLiterals.data(), 0, numLiterals, 1.0);

      // after base reducing the values, if the difference in bits between
      // 95th percentile and 100th percentile value is zero then there
      // is no point in patching the values, in which case we will
      // fallback to DIRECT encoding.
      // The decision to use patched base was based on zigzag values, but the
      // actual patching is done on base reduced literals.
      if ((brBits100p - brBits95p) != 0) {
        encoding = EncodingType::PATCHED_BASE;
        preparePatchedBlob();
        return;
      } else {
        encoding = EncodingType::DIRECT;
        return;
      }
    } else {
      // if difference in bits between 90th percentile and 100th percentile is
      // 0, then patch length will become 0. Hence we will fallback to direct
      encoding = EncodingType::DIRECT;
      return;
    }
  }

  void computeZigZagLiterals() {
    // populate zigzag encoded literals
    int64_t zzEncVal = 0;
    for (int32_t i = 0; i < numLiterals; i++) {
      if (isSigned) {
        zzEncVal = zigzagEncode(literals[i]);
      } else {
        zzEncVal = literals[i];
      }
      zigzagLiterals[i] = zzEncVal;
    }
  }

  void preparePatchedBlob() {
    // mask will be max value beyond which patch will be generated
    int64_t mask = (1LL << brBits95p) - 1;

    // since we are considering only 95 percentile, the size of gap and
    // patch array can contain only be 5% values
    patchLength = (int32_t)std::ceil((numLiterals * 0.05));

    std::vector<int32_t> gapList(patchLength);
    std::vector<int64_t> patchList(patchLength);

    // #bit for patch
    patchWidth = brBits100p - brBits95p;
    patchWidth = getClosestFixedBits(patchWidth);

    // if patch bit requirement is 64 then it will not possible to pack
    // gap and patch together in a long. To make sure gap and patch can be
    // packed together adjust the patch width
    if (patchWidth == 64) {
      patchWidth = 56;
      brBits95p = 8;
      mask = (1LL << brBits95p) - 1;
    }

    int32_t gapIdx = 0;
    int32_t patchIdx = 0;
    int32_t prev = 0;
    int32_t gap = 0;
    int32_t maxGap = 0;

    for (int32_t i = 0; i < numLiterals; i++) {
      // if value is above mask then create the patch and record the gap
      if (baseRedLiterals[i] > mask) {
        gap = i - prev;
        if (gap > maxGap) {
          maxGap = gap;
        }

        // gaps are relative, so store the previous patched value index
        prev = i;
        gapList[gapIdx++] = gap;

        // extract the most significant bits that are over mask bits
        int64_t patch = ((uint64_t)baseRedLiterals[i] >> brBits95p);
        patchList[patchIdx++] = patch;

        // strip off the MSB to enable safe bit packing
        baseRedLiterals[i] &= mask;
      }
    }

    // adjust the patch length to number of entries in gap list
    patchLength = gapIdx;

    // if the element to be patched is the first and only element then
    // max gap will be 0, but to store the gap as 0 we need atleast 1 bit
    if (maxGap == 0 && patchLength != 0) {
      patchGapWidth = 1;
    } else {
      patchGapWidth = findClosestNumBits(maxGap);
    }

    // special case: if the patch gap width is greater than 256, then
    // we need 9 bits to encode the gap width. But we only have 3 bits in
    // header to record the gap width. To deal with this case, we will save
    // two entries in patch list in the following way
    // 256 gap width => 0 for patch value
    // actual gap - 256 => actual patch value
    // We will do the same for gap width = 511. If the element to be patched is
    // the last element in the scope then gap width will be 511. In this case we
    // will have 3 entries in the patch list in the following way
    // 255 gap width => 0 for patch value
    // 255 gap width => 0 for patch value
    // 1 gap width => actual patch value
    if (patchGapWidth > 8) {
      patchGapWidth = 8;
      // for gap = 511, we need two additional entries in patch list
      if (maxGap == 511) {
        patchLength += 2;
      } else {
        patchLength += 1;
      }
    }

    // create gap vs patch list
    gapIdx = 0;
    patchIdx = 0;
    gapVsPatchList.resize(patchLength);
    for (int32_t i = 0; i < patchLength; i++) {
      int64_t g = gapList[gapIdx++];
      int64_t p = patchList[patchIdx++];
      while (g > 255) {
        gapVsPatchList[i++] = (255L << patchWidth);
        g -= 255;
      }

      // store patch value in LSBs and gap in MSBs
      gapVsPatchList[i] = (g << patchWidth) | p;
    }
  }

  void writeValues() {
    if (numLiterals != 0) {
      if (encoding == EncodingType::SHORT_REPEAT) {
        writeShortRepeatValues();
      } else if (encoding == EncodingType::DIRECT) {
        writeDirectValues();
      } else if (encoding == EncodingType::PATCHED_BASE) {
        writePatchedBaseValues();
      } else {
        writeDeltaValues();
      }

      // clear all the variables
      clear();
    }
  }

  int32_t getClosestAlignedFixedBits(int32_t n) {
    if (n == 0 || n == 1) {
      return 1;
    } else if (n > 1 && n <= 2) {
      return 2;
    } else if (n > 2 && n <= 4) {
      return 4;
    } else if (n > 4 && n <= 8) {
      return 8;
    } else if (n > 8 && n <= 16) {
      return 16;
    } else if (n > 16 && n <= 24) {
      return 24;
    } else if (n > 24 && n <= 32) {
      return 32;
    } else if (n > 32 && n <= 40) {
      return 40;
    } else if (n > 40 && n <= 48) {
      return 48;
    } else if (n > 48 && n <= 56) {
      return 56;
    } else {
      return 64;
    }
  }

  // For a given fixed bit this function will return the closest available fixed
  // bit
  // @param n
  // @return closest valid fixed bit
  int32_t getClosestFixedBits(int32_t n) {
    if (n == 0) {
      return 1;
    }

    if (n >= 1 && n <= 24) {
      return n;
    } else if (n > 24 && n <= 26) {
      return 26;
    } else if (n > 26 && n <= 28) {
      return 28;
    } else if (n > 28 && n <= 30) {
      return 30;
    } else if (n > 30 && n <= 32) {
      return 32;
    } else if (n > 32 && n <= 40) {
      return 40;
    } else if (n > 40 && n <= 48) {
      return 48;
    } else if (n > 48 && n <= 56) {
      return 56;
    } else {
      return 64;
    }
  }

  // Finds the closest available fixed bit width match and returns its encoded
  // value (ordinal)
  // @param n  Fixed bit width to encode
  // @return  Encoded fixed bit width
  int32_t encodeBitWidth(int32_t n) {
    n = getClosestFixedBits(n);

    if (n >= 1 && n <= 24) {
      return n - 1;
    } else if (n > 24 && n <= 26) {
      return FixedBitSizes::FBS::TWENTYSIX;
    } else if (n > 26 && n <= 28) {
      return FixedBitSizes::FBS::TWENTYEIGHT;
    } else if (n > 28 && n <= 30) {
      return FixedBitSizes::FBS::THIRTY;
    } else if (n > 30 && n <= 32) {
      return FixedBitSizes::FBS::THIRTYTWO;
    } else if (n > 32 && n <= 40) {
      return FixedBitSizes::FBS::FORTY;
    } else if (n > 40 && n <= 48) {
      return FixedBitSizes::FBS::FORTYEIGHT;
    } else if (n > 48 && n <= 56) {
      return FixedBitSizes::FBS::FIFTYSIX;
    } else {
      return FixedBitSizes::FBS::SIXTYFOUR;
    }
  }

  // Store the opcode in 2 MSB bits
  // @return opcode
  int32_t getOpcode() { return encoding << 6; }

  void writeDeltaValues() {
    int32_t len = 0;
    int32_t fb = bitsDeltaMax;
    int32_t efb = 0;

    if (alignedBitpacking) {
      fb = getClosestAlignedFixedBits(fb);
    }

    if (isFixedDelta) {
      // if fixed run length is greater than threshold then it will be fixed
      // delta sequence with delta value 0 else fixed delta sequence with
      // non-zero delta value
      if (fixedRunLength > MIN_REPEAT) {
        // ex. sequence: 2 2 2 2 2 2 2 2
        len = fixedRunLength - 1;
        fixedRunLength = 0;
      } else {
        // ex. sequence: 4 6 8 10 12 14 16
        len = variableRunLength - 1;
        variableRunLength = 0;
      }
    } else {
      // fixed width 0 is used for long repeating values.
      // sequences that require only 1 bit to encode will have an additional bit
      if (fb == 1) {
        fb = 2;
      }
      efb = encodeBitWidth(fb);
      efb = efb << 1;
      len = variableRunLength - 1;
      variableRunLength = 0;
    }

    // extract the 9th bit of run length
    int32_t tailBits = (uint32_t)(len & 0x100) >> 8;

    // create first byte of the header
    int32_t headerFirstByte = getOpcode() | efb | tailBits;

    // second byte of the header stores the remaining 8 bits of runlength
    int32_t headerSecondByte = len & 0xff;

    // write header
    output->writeByte(headerFirstByte);
    output->writeByte(headerSecondByte);

    // store the first value from zigzag literal array
    if (isSigned) {
      writeInt64(output.get(), literals[0]);
    } else {
      writeUInt64(output.get(), literals[0]);
    }

    if (isFixedDelta) {
      // if delta is fixed then we don't need to store delta blob
      writeInt64(output.get(), fixedDelta);
    } else {
      // store the first value as delta value using zigzag encoding
      writeInt64(output.get(), adjDeltas[0]);

      // adjacent delta values are bit packed. The length of adjDeltas array is
      // always one less than the number of literals (delta difference for n
      // elements is n-1). We have already written one element, write the
      // remaining numLiterals - 2 elements here

      writeInts(adjDeltas.data(), 1, numLiterals - 2, fb, output.get());
    }
  }

  // Count the number of bits required to encode the given value
  // @param value
  // @return bits required to store value
  int32_t findClosestNumBits(int64_t value) {
    int32_t count = 0;
    while (value != 0) {
      count++;
      value = ((uint64_t)(value) >> 1);
    }
    return getClosestFixedBits(count);
  }

  void writePatchedBaseValues() {
    // NOTE: Aligned bit packing cannot be applied for PATCHED_BASE encoding
    // because patch is applied to MSB bits. For example: If fixed bit width of
    // base value is 7 bits and if patch is 3 bits, the actual value is
    // constructed by shifting the patch to left by 7 positions.
    // actual_value = patch << 7 | base_value
    // So, if we align base_value then actual_value can not be reconstructed.

    // write the number of fixed bits required in next 5 bits
    int32_t fb = brBits95p;
    int32_t efb = encodeBitWidth(fb) << 1;

    // adjust variable run length, they are one off
    variableRunLength -= 1;

    // extract the 9th bit of run length
    int32_t tailBits = (uint32_t)(variableRunLength & 0x100) >> 8;

    // create first byte of the header
    int32_t headerFirstByte = getOpcode() | efb | tailBits;

    // second byte of the header stores the remaining 8 bits of runlength
    int32_t headerSecondByte = variableRunLength & 0xff;

    // if the min value is negative toggle the sign
    bool isNegative = min < 0 ? true : false;
    if (isNegative) {
      min = -min;
    }

    // find the number of bytes required for base and shift it by 5 bits
    // to accommodate patch width. The additional bit is used to store the sign
    // of the base value.
    int32_t baseWidth = findClosestNumBits(min) + 1;
    int32_t baseBytes =
        baseWidth % 8 == 0 ? baseWidth / 8 : (baseWidth / 8) + 1;
    int32_t bb = (baseBytes - 1) << 5;

    // if the base value is negative then set MSB to 1
    if (isNegative) {
      min |= (1LL << ((baseBytes * 8) - 1));
    }

    // third byte contains 3 bits for number of bytes occupied by base
    // and 5 bits for patchWidth
    int32_t headerThirdByte = bb | encodeBitWidth(patchWidth);

    // fourth byte contains 3 bits for page gap width and 5 bits for
    // patch length
    int32_t headerFourthByte = (patchGapWidth - 1) << 5 | patchLength;

    // write header
    output->writeByte(headerFirstByte);
    output->writeByte(headerSecondByte);
    output->writeByte(headerThirdByte);
    output->writeByte(headerFourthByte);

    // write the base value using fixed bytes in big endian order
    for (int32_t i = baseBytes - 1; i >= 0; i--) {
      int8_t b = (int8_t)(((uint64_t)min >> (i * 8)) & 0xff);
      output->writeByte(b);
    }

    // base reduced literals are bit packed
    int32_t closestFixedBits = getClosestFixedBits(fb);

    writeInts(baseRedLiterals.data(), 0, numLiterals, closestFixedBits,
              output.get());

    // write patch list
    closestFixedBits = getClosestFixedBits(patchGapWidth + patchWidth);

    writeInts(gapVsPatchList.data(), 0, gapVsPatchList.size(), closestFixedBits,
              output.get());

    // reset run length
    variableRunLength = 0;
  }

  void writeDirectValues() {
    // write the number of fixed bits required in next 5 bits
    int32_t fb = zzBits100p;

    if (alignedBitpacking) {
      fb = getClosestAlignedFixedBits(fb);
    }

    int32_t efb = encodeBitWidth(fb) << 1;

    // adjust variable run length
    variableRunLength -= 1;

    // extract the 9th bit of run length
    int32_t tailBits = (uint32_t)(variableRunLength & 0x100) >> 8;

    // create first byte of the header
    int32_t headerFirstByte = getOpcode() | efb | tailBits;

    // second byte of the header stores the remaining 8 bits of runlength
    int32_t headerSecondByte = variableRunLength & 0xff;

    // write header
    output->writeByte(headerFirstByte);
    output->writeByte(headerSecondByte);

    // bit packing the zigzag encoded literals
    writeInts(zigzagLiterals.data(), 0, numLiterals, fb, output.get());

    // reset run length
    variableRunLength = 0;
  }

  void writeShortRepeatValues() {
    // get the value that is repeating, compute the bits and bytes required
    int64_t repeatVal = 0;
    if (isSigned) {
      repeatVal = zigzagEncode(literals[0]);
    } else {
      repeatVal = literals[0];
    }

    int32_t numBitsRepeatVal = findClosestNumBits(repeatVal);
    int32_t numBytesRepeatVal = numBitsRepeatVal % 8 == 0
                                    ? (uint32_t)numBitsRepeatVal >> 3
                                    : ((uint32_t)numBitsRepeatVal >> 3) + 1;

    // write encoding type in top 2 bits
    int32_t header = getOpcode();

    // write the number of bytes required for the value
    header |= ((numBytesRepeatVal - 1) << 3);

    // write the run length
    fixedRunLength -= MIN_REPEAT;
    header |= fixedRunLength;

    // write the header
    output->writeByte(header);

    // write the repeating value in big endian byte order
    for (int32_t i = numBytesRepeatVal - 1; i >= 0; i--) {
      int32_t b = (int32_t)(((uint64_t)repeatVal >> (i * 8)) & 0xff);
      output->writeByte(b);
    }

    fixedRunLength = 0;
  }

  void clear() {
    numLiterals = 0;
    encoding = UNKNOWN;
    prevDelta = 0;
    fixedDelta = 0;
    zzBits90p = 0;
    zzBits100p = 0;
    brBits95p = 0;
    brBits100p = 0;
    bitsDeltaMax = 0;
    patchGapWidth = 0;
    patchLength = 0;
    patchWidth = 0;
    gapVsPatchList.resize(0);
    min = 0;
    isFixedDelta = true;
  }

  void initializeLiterals(int64_t val) {
    literals[numLiterals++] = val;
    fixedRunLength = 1;
    variableRunLength = 1;
  }

 private:
  std::unique_ptr<SeekableOutputStream> output;
  const bool isSigned = false;

  const int32_t MAX_SCOPE = 512;
  const int32_t MAX_SHORT_REPEAT_LENGTH = 10;
  int64_t prevDelta = 0;
  int32_t fixedRunLength = 0;
  int32_t variableRunLength = 0;
  std::vector<int64_t> literals;
  EncodingType encoding = EncodingType::UNKNOWN;
  int32_t numLiterals = 0;

  std::vector<int64_t> zigzagLiterals;
  std::vector<int64_t> baseRedLiterals;
  std::vector<int64_t> adjDeltas;
  int64_t fixedDelta = 0;
  int32_t zzBits90p = 0;
  int32_t zzBits100p = 0;
  int32_t brBits95p = 0;
  int32_t brBits100p = 0;
  int32_t bitsDeltaMax = 0;
  int32_t patchWidth = 0;
  int32_t patchGapWidth = 0;
  int32_t patchLength = 0;
  std::vector<int64_t> gapVsPatchList;
  int64_t min = 0;
  bool isFixedDelta = false;
  bool alignedBitpacking = false;
};

}  // namespace orc

#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_RLE_V2_H_
