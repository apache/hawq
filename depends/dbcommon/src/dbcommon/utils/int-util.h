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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_INT_UTIL_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_INT_UTIL_H_

#include <cstdint>
#include <tuple>

namespace dbcommon {

inline bool isPowerOfTwo(uint64_t x) {
  return ((x != 0) && ((x & (~x + 1)) == x));
}

inline uint64_t nextPowerOfTwo(uint64_t n) {
  return n ? uint64_t(1) << (64 - __builtin_clzll(n - 1)) : 0;
}

// Encodes an unsigned variable-length integer using the MSB algorithm.
// This function assumes that the value is stored as little endian.
// @param value The input value. Any standard integer type is allowed.
// @param output A pointer to a piece of reserved memory. Must have a minimum
// size dependent on the input size (32 bit = 5 bytes, 64 bit = 10 bytes).
// @return The number of bytes used in the output memory.
inline uint32_t encodeMsbVarint(uint64_t value, char* output) {
  uint32_t outputSize = 0;
  // While more than 7 bits of data are left, occupy the last output byte
  // and set the next byte flag
  while (value > 127) {
    // |128: Set the next byte flag
    output[outputSize] = ((uint8_t)(value & 127)) | 128;
    // Remove the seven bits we just wrote
    value >>= 7;
    outputSize++;
  }
  output[outputSize++] = ((uint8_t)value) & 127;
  return outputSize;
}

// Decode an unsigned variable-length integer using the MSB algorithm.
// @param[in] input
// @return the encoded integer,  the number of bytes used for encoding
inline std::tuple<uint64_t, uint32_t> decodeMsbVarint(const char* input) {
  uint64_t ret = 0;
  uint32_t i = 0;
  do {
    ret |= (uint64_t)(input[i] & 127) << (7 * i);
  } while (input[i++] & 128);  // If the next-byte flag is set
  return std::make_tuple(ret, i);
}

inline uint32_t encodeUnsignedVarint(uint64_t val, char* content) {
  int cursor = 0;
  if (val >= (uint64_t)0x100000000000000) {
    content[cursor] = 8;
    content[cursor + 1] = val >> 56;
    content[cursor + 2] = val >> 48;
    content[cursor + 3] = val >> 40;
    content[cursor + 4] = val >> 32;
    content[cursor + 5] = val >> 24;
    content[cursor + 6] = val >> 16;
    content[cursor + 7] = val >> 8;
    content[cursor + 8] = val;
    cursor += 9;
  } else if (val >= (uint64_t)0x1000000000000) {
    content[cursor] = 7;
    content[cursor + 1] = val >> 48;
    content[cursor + 2] = val >> 40;
    content[cursor + 3] = val >> 32;
    content[cursor + 4] = val >> 24;
    content[cursor + 5] = val >> 16;
    content[cursor + 6] = val >> 8;
    content[cursor + 7] = val;
    cursor += 8;
  } else if (val >= (uint64_t)0x10000000000) {
    content[cursor] = 6;
    content[cursor + 1] = val >> 40;
    content[cursor + 2] = val >> 32;
    content[cursor + 3] = val >> 24;
    content[cursor + 4] = val >> 16;
    content[cursor + 5] = val >> 8;
    content[cursor + 6] = val;
    cursor += 7;
  } else if (val >= (uint64_t)0x100000000) {
    content[cursor] = 5;
    content[cursor + 1] = val >> 32;
    content[cursor + 2] = val >> 24;
    content[cursor + 3] = val >> 16;
    content[cursor + 4] = val >> 8;
    content[cursor + 5] = val;
    cursor += 6;
  } else if (val >= (uint64_t)0x1000000) {
    content[cursor] = 4;
    content[cursor + 1] = val >> 24;
    content[cursor + 2] = val >> 16;
    content[cursor + 3] = val >> 8;
    content[cursor + 4] = val;
    cursor += 5;
  } else if (val >= (uint64_t)0x10000) {
    content[cursor] = 3;
    content[cursor + 1] = val >> 16;
    content[cursor + 2] = val >> 8;
    content[cursor + 3] = val;
    cursor += 4;
  } else if (val >= (uint64_t)0x100) {
    content[cursor] = 2;
    content[cursor + 1] = val >> 8;
    content[cursor + 2] = val;
    cursor += 3;
  } else {
    content[cursor] = 1;
    content[cursor + 1] = val;
    cursor += 2;
  }
  return cursor;
}

inline std::tuple<uint64_t, uint32_t> decodeUnsignedVarint(const char* input) {
  const uint8_t* data = reinterpret_cast<const uint8_t*>(input);
  uint64_t val;
  uint32_t len;
  len = data[0];
  switch (len) {
    case 1:
      val = (uint64_t)data[1];
      break;
    case 2:
      val = ((uint64_t)data[1] << 8) + ((uint64_t)data[2]);
      break;
    case 3:
      val = ((uint64_t)data[1] << 16) + ((uint64_t)data[2] << 8) +
            ((uint64_t)data[3]);
      break;
    case 4:
      val = ((uint64_t)data[1] << 24) + ((uint64_t)data[2] << 16) +
            ((uint64_t)data[3] << 8) + ((uint64_t)data[4]);
      break;
    case 5:
      val = ((uint64_t)data[1] << 32) + ((uint64_t)data[2] << 24) +
            ((uint64_t)data[3] << 16) + ((uint64_t)data[4] << 8) +
            ((uint64_t)data[5]);
      break;
    case 6:
      val = ((uint64_t)data[1] << 40) + ((uint64_t)data[2] << 32) +
            ((uint64_t)data[3] << 24) + ((uint64_t)data[4] << 16) +
            ((uint64_t)data[5] << 8) + ((uint64_t)data[6]);
      break;
    case 7:
      val = ((uint64_t)data[1] << 48) + ((uint64_t)data[2] << 40) +
            ((uint64_t)data[3] << 32) + ((uint64_t)data[4] << 24) +
            ((uint64_t)data[5] << 16) + ((uint64_t)data[6] << 8) +
            ((uint64_t)data[7]);
      break;
    case 8:
      val = ((uint64_t)data[1] << 56) + ((uint64_t)data[2] << 48) +
            ((uint64_t)data[3] << 40) + ((uint64_t)data[4] << 32) +
            ((uint64_t)data[5] << 24) + ((uint64_t)data[6] << 16) +
            ((uint64_t)data[7] << 8) + ((uint64_t)data[8]);
      break;
  }
  return std::make_tuple(val, len + 1);
}

template <typename Integer, int N>
constexpr Integer powerBase10() {
  return powerBase10<Integer, N - 1>() * 10;
}

template <>
constexpr __uint128_t powerBase10<__uint128_t, 0>() {
  return 1;
}
template <>
constexpr uint64_t powerBase10<uint64_t, 0>() {
  return 1;
}
template <>
constexpr uint32_t powerBase10<uint32_t, 0>() {
  return 1;
}
template <>
constexpr uint16_t powerBase10<uint16_t, 0>() {
  return 1;
}

template <typename TP>
inline uint64_t getNumOfDigit(TP i) {
  uint64_t ret = 0;
  if (powerBase10<TP, 19>() <= i) {
    if (powerBase10<TP, 28>() <= i) {
      if (powerBase10<TP, 33>() <= i) {
        if (powerBase10<TP, 35>() <= i) {
          if (powerBase10<TP, 36>() <= i) {
            if (powerBase10<TP, 37>() <= i) {
              if (powerBase10<TP, 38>() <= i) {
                ret = 39;
              } else {
                ret = 38;
              }
            } else {
              ret = 37;
            }
          } else {
            ret = 36;
          }
        } else {
          if (powerBase10<TP, 34>() <= i) {
            ret = 35;
          } else {
            ret = 34;
          }
        }
      } else {
        if (powerBase10<TP, 30>() <= i) {
          if (powerBase10<TP, 32>() <= i) {
            ret = 33;
          } else {
            if (powerBase10<TP, 31>() <= i) {
              ret = 32;
            } else {
              ret = 31;
            }
          }
        } else {
          if (powerBase10<TP, 29>() <= i) {
            ret = 30;
          } else {
            ret = 29;
          }
        }
      }
    } else {
      if (powerBase10<TP, 24>() <= i) {
        if (powerBase10<TP, 26>() <= i) {
          if (powerBase10<TP, 27>() <= i) {
            ret = 28;
          } else {
            ret = 27;
          }
        } else {
          if (powerBase10<TP, 25>() <= i) {
            ret = 26;
          } else {
            ret = 25;
          }
        }
      } else {
        if (powerBase10<TP, 22>() <= i) {
          if (powerBase10<TP, 23>() <= i) {
            ret = 24;
          } else {
            ret = 23;
          }
        } else {
          if (powerBase10<TP, 20>() <= i) {
            if (powerBase10<TP, 21>() <= i) {
              ret = 22;
            } else {
              ret = 21;
            }
          } else {
            ret = 20;
          }
        }
      }
    }
  } else {
    if (powerBase10<TP, 10>() <= i) {
      if (powerBase10<TP, 15>() <= i) {
        if (powerBase10<TP, 17>() <= i) {
          if (powerBase10<TP, 18>() <= i) {
            ret = 19;
          } else {
            ret = 18;
          }
        } else {
          if (powerBase10<TP, 16>() <= i)
            ret = 17;
          else
            ret = 16;
        }
      } else {
        if (powerBase10<TP, 12>() <= i) {
          if (powerBase10<TP, 13>() <= i) {
            if (powerBase10<TP, 14>() <= i)
              ret = 15;
            else
              ret = 14;
          } else {
            ret = 13;
          }
        } else {
          if (powerBase10<TP, 11>() <= i)
            ret = 12;
          else
            ret = 11;
        }
      }
    } else {
      if (powerBase10<TP, 5>() <= i) {
        if (powerBase10<TP, 7>() <= i) {
          if (powerBase10<TP, 8>() <= i) {
            if (powerBase10<TP, 9>() <= i)
              ret = 10;
            else
              ret = 9;
          } else {
            ret = 8;
          }
        } else {
          if (powerBase10<TP, 6>() <= i)
            ret = 7;
          else
            ret = 6;
        }
      } else {
        if (powerBase10<TP, 2>() <= i) {
          if (powerBase10<TP, 3>() <= i) {
            if (powerBase10<TP, 4>() <= i)
              ret = 5;
            else
              ret = 4;
          } else {
            ret = 3;
          }
        } else {
          if (powerBase10<TP, 1>() <= i) {
            ret = 2;
          } else {
            ret = 1;
          }
        }
      }
    }
  }

  return ret;
}
}  // end namespace dbcommon
#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_INT_UTIL_H_
