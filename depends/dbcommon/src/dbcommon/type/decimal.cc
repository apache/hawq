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

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <utility>

#include "dbcommon/common/vector/decimal-vector.h"
#include "dbcommon/log/logger.h"
#include "dbcommon/type/decimal.h"

namespace dbcommon {

Int128 Int128::maximumValue() {
  return Int128(0x7fffffffffffffff, 0xfffffffffffffff);
}

Int128 Int128::minimumValue() {
  return Int128(static_cast<int64_t>(0x8000000000000000), 0x0);
}

Int128::Int128(const std::string& str) {
  lowbits = 0;
  highbits = 0;
  size_t length = str.length();
  if (length > 0) {
    bool isNegative = str[0] == '-';
    size_t posn = isNegative ? 1 : 0;
    while (posn < length) {
      size_t group = std::min(18ul, length - posn);
      int64_t chunk = std::stoll(str.substr(posn, group));
      int64_t multiple = 1;
      for (size_t i = 0; i < group; ++i) {
        multiple *= 10;
      }
      *this *= multiple;
      *this += chunk;
      posn += group;
    }
    if (isNegative) {
      negate();
    }
  }
}

Int128& Int128::operator*=(const Int128& right) {
  const uint64_t INT_MASK = 0xffffffff;
  const uint64_t CARRY_BIT = 1l << 32;

  // Break the left and right numbers into 32 bit chunks
  // so that we can multiply them without overflow.
  uint64_t L0 = static_cast<uint64_t>(highbits) >> 32;
  uint64_t L1 = static_cast<uint64_t>(highbits) & INT_MASK;
  uint64_t L2 = lowbits >> 32;
  uint64_t L3 = lowbits & INT_MASK;
  uint64_t R0 = static_cast<uint64_t>(right.highbits) >> 32;
  uint64_t R1 = static_cast<uint64_t>(right.highbits) & INT_MASK;
  uint64_t R2 = right.lowbits >> 32;
  uint64_t R3 = right.lowbits & INT_MASK;

  uint64_t product = L3 * R3;
  lowbits = product & INT_MASK;
  uint64_t sum = product >> 32;
  product = L2 * R3;
  sum += product;
  highbits = sum < product ? CARRY_BIT : 0;
  product = L3 * R2;
  sum += product;
  if (sum < product) {
    highbits += CARRY_BIT;
  }
  lowbits += sum << 32;
  highbits += static_cast<int64_t>(sum >> 32);
  highbits += L1 * R3 + L2 * R2 + L3 * R1;
  highbits += (L0 * R3 + L1 * R2 + L2 * R1 + L3 * R0) << 32;
  return *this;
}

// Expands the given value into an array of ints so that we can work on
// it. The array will be converted to an absolute value and the wasNegative
// flag will be set appropriately. The array will remove leading zeros from
// the value.
// @param array an array of length 4 to set with the value
// @param wasNegative a flag for whether the value was original negative
// @result the output length of the array
int64_t Int128::fillInArray(uint32_t* array, bool& wasNegative) const {
  uint64_t high;
  uint64_t low;
  if (highbits < 0) {
    low = ~lowbits + 1;
    high = static_cast<uint64_t>(~highbits);
    if (low == 0) {
      high += 1;
    }
    wasNegative = true;
  } else {
    low = lowbits;
    high = static_cast<uint64_t>(highbits);
    wasNegative = false;
  }
  if (high != 0) {
    if (high > UINT32_MAX) {
      array[0] = static_cast<uint32_t>(high >> 32);
      array[1] = static_cast<uint32_t>(high);
      array[2] = static_cast<uint32_t>(low >> 32);
      array[3] = static_cast<uint32_t>(low);
      return 4;
    } else {
      array[0] = static_cast<uint32_t>(high);
      array[1] = static_cast<uint32_t>(low >> 32);
      array[2] = static_cast<uint32_t>(low);
      return 3;
    }
  } else if (low >= UINT32_MAX) {
    array[0] = static_cast<uint32_t>(low >> 32);
    array[1] = static_cast<uint32_t>(low);
    return 2;
  } else if (low == 0) {
    return 0;
  } else {
    array[0] = static_cast<uint32_t>(low);
    return 1;
  }
}

template <int N>
constexpr __int128_t powerBase10() {
  return powerBase10<N - 1>() * 10;
}
template <>
constexpr __int128_t powerBase10<0>() {
  return 1;
}

size_t Int128::getNumOfDigit() {
  __int128_t curr = highbits;
  curr = (curr << 64) + lowbits;
  static __int128_t bounds[39] = {
      powerBase10<0>(),  powerBase10<1>(),  powerBase10<2>(),
      powerBase10<3>(),  powerBase10<4>(),  powerBase10<5>(),
      powerBase10<6>(),  powerBase10<7>(),  powerBase10<8>(),
      powerBase10<9>(),  powerBase10<10>(),

      powerBase10<11>(), powerBase10<12>(), powerBase10<13>(),
      powerBase10<14>(), powerBase10<15>(), powerBase10<16>(),
      powerBase10<17>(), powerBase10<18>(), powerBase10<19>(),
      powerBase10<20>(),

      powerBase10<21>(), powerBase10<22>(), powerBase10<23>(),
      powerBase10<24>(), powerBase10<25>(), powerBase10<26>(),
      powerBase10<27>(), powerBase10<28>(), powerBase10<29>(),
      powerBase10<30>(),

      powerBase10<31>(), powerBase10<32>(), powerBase10<33>(),
      powerBase10<34>(), powerBase10<35>(), powerBase10<36>(),
      powerBase10<37>(), powerBase10<38>(),
  };
  size_t ret = 0;
  while (bounds[ret] <= curr) ret++;
  return ret;
}

// Find last set bit in a 32 bit integer. Bit 1 is the LSB and bit 32 is
// the MSB. We can replace this with bsrq asm instruction on x64.
int64_t fls(uint32_t x) {
  int64_t bitpos = 0;
  while (x) {
    x >>= 1;
    bitpos += 1;
  }
  return bitpos;
}

// Shift the number in the array left by bits positions.
// @param array the number to shift, must have length elements
// @param length the number of entries in the array
// @param bits the number of bits to shift (0 <= bits < 32)
void shiftArrayLeft(uint32_t* array, int64_t length, int64_t bits) {
  if (length > 0 && bits != 0) {
    for (int64_t i = 0; i < length - 1; ++i) {
      array[i] = (array[i] << bits) | (array[i + 1] >> (32 - bits));
    }
    array[length - 1] <<= bits;
  }
}

// Shift the number in the array right by bits positions.
// @param array the number to shift, must have length elements
// @param length the number of entries in the array
// @param bits the number of bits to shift (0 <= bits < 32)
void shiftArrayRight(uint32_t* array, int64_t length, int64_t bits) {
  if (length > 0 && bits != 0) {
    for (int64_t i = length - 1; i > 0; --i) {
      array[i] = (array[i] >> bits) | (array[i - 1] << (32 - bits));
    }
    array[0] >>= bits;
  }
}

// Fix the signs of the result and remainder at the end of the division
// based on the signs of the dividend and divisor.
void fixDivisionSigns(Int128& result, Int128& remainder,  // NOLINT
                      bool dividendWasNegative, bool divisorWasNegative) {
  if (dividendWasNegative != divisorWasNegative) {
    result.negate();
  }
  if (dividendWasNegative) {
    remainder.negate();
  }
}

// Build a Int128 from a list of ints.
void buildFromArray(Int128& value, uint32_t* array, int64_t length) {  // NOLINT
  switch (length) {
    case 0:
      value = 0;
      break;
    case 1:
      value = array[0];
      break;
    case 2:
      value = Int128(0, (static_cast<uint64_t>(array[0]) << 32) + array[1]);
      break;
    case 3:
      value =
          Int128(array[0], (static_cast<uint64_t>(array[1]) << 32) + array[2]);
      break;
    case 4:
      value = Int128((static_cast<int64_t>(array[0]) << 32) + array[1],
                     (static_cast<uint64_t>(array[2]) << 32) + array[3]);
      break;
    case 5:
      if (array[0] != 0) {
        LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Can't build Int128 with 5 ints.");
      }
      value = Int128((static_cast<int64_t>(array[1]) << 32) + array[2],
                     (static_cast<uint64_t>(array[3]) << 32) + array[4]);
      break;
    default:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "Unsupported length for building Int128");
  }
}

// Do a division where the divisor fits into a single 32 bit value.
Int128 singleDivide(uint32_t* dividend, int64_t dividendLength,
                    uint32_t divisor, Int128& remainder,  // NOLINT
                    bool dividendWasNegative,             // NOLINT
                    bool divisorWasNegative) {
  uint64_t r = 0;
  uint32_t resultArray[5];
  for (int64_t j = 0; j < dividendLength; j++) {
    r <<= 32;
    r += dividend[j];
    resultArray[j] = static_cast<uint32_t>(r / divisor);
    r %= divisor;
  }
  Int128 result;
  buildFromArray(result, resultArray, dividendLength);
  remainder = static_cast<int64_t>(r);
  fixDivisionSigns(result, remainder, dividendWasNegative, divisorWasNegative);
  return result;
}

Int128 Int128::divide(const Int128& divisor, Int128& remainder) const {
  // Split the dividend and divisor into integer pieces so that we can
  // work on them.
  uint32_t dividendArray[5];
  uint32_t divisorArray[4];
  bool dividendWasNegative;
  bool divisorWasNegative;
  // leave an extra zero before the dividend
  dividendArray[0] = 0;
  int64_t dividendLength =
      fillInArray(dividendArray + 1, dividendWasNegative) + 1;
  int64_t divisorLength = divisor.fillInArray(divisorArray, divisorWasNegative);

  // Handle some of the easy cases.
  if (dividendLength <= divisorLength) {
    remainder = *this;
    return 0;
  } else if (divisorLength == 0) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Division by 0 in Int128");
  } else if (divisorLength == 1) {
    return singleDivide(dividendArray, dividendLength, divisorArray[0],
                        remainder, dividendWasNegative, divisorWasNegative);
  }

  int64_t resultLength = dividendLength - divisorLength;
  uint32_t resultArray[4];

  // Normalize by shifting both by a multiple of 2 so that
  // the digit guessing is better. The requirement is that
  // divisorArray[0] is greater than 2**31.
  int64_t normalizeBits = 32 - fls(divisorArray[0]);
  shiftArrayLeft(divisorArray, divisorLength, normalizeBits);
  shiftArrayLeft(dividendArray, dividendLength, normalizeBits);

  // compute each digit in the result
  for (int64_t j = 0; j < resultLength; ++j) {
    // Guess the next digit. At worst it is two too large
    uint32_t guess = UINT32_MAX;
    uint64_t highDividend =
        static_cast<uint64_t>(dividendArray[j]) << 32 | dividendArray[j + 1];
    if (dividendArray[j] != divisorArray[0]) {
      guess = static_cast<uint32_t>(highDividend / divisorArray[0]);
    }

    // catch all of the cases where guess is two too large and most of the
    // cases where it is one too large
    uint32_t rhat = static_cast<uint32_t>(
        highDividend - guess * static_cast<uint64_t>(divisorArray[0]));
    while (static_cast<uint64_t>(divisorArray[1]) * guess >
           (static_cast<uint64_t>(rhat) << 32) + dividendArray[j + 2]) {
      guess -= 1;
      rhat += divisorArray[0];
      if (static_cast<uint64_t>(rhat) < divisorArray[0]) {
        break;
      }
    }

    // subtract off the guess * divisor from the dividend
    uint64_t mult = 0;
    for (int64_t i = divisorLength - 1; i >= 0; --i) {
      mult += static_cast<uint64_t>(guess) * divisorArray[i];
      uint32_t prev = dividendArray[j + i + 1];
      dividendArray[j + i + 1] -= static_cast<uint32_t>(mult);
      mult >>= 32;
      if (dividendArray[j + i + 1] > prev) {
        mult += 1;
      }
    }
    uint32_t prev = dividendArray[j];
    dividendArray[j] -= static_cast<uint32_t>(mult);

    // if guess was too big, we add back divisor
    if (dividendArray[j] > prev) {
      guess -= 1;
      uint32_t carry = 0;
      for (int64_t i = divisorLength - 1; i >= 0; --i) {
        uint64_t sum = static_cast<uint64_t>(divisorArray[i]) +
                       dividendArray[j + i + 1] + carry;
        dividendArray[j + i + 1] = static_cast<uint32_t>(sum);
        carry = static_cast<uint32_t>(sum >> 32);
      }
      dividendArray[j] += carry;
    }

    resultArray[j] = guess;
  }

  // denormalize the remainder
  shiftArrayRight(dividendArray, dividendLength, normalizeBits);

  // return result and remainder
  Int128 result;
  buildFromArray(result, resultArray, resultLength);
  buildFromArray(remainder, dividendArray, dividendLength);
  fixDivisionSigns(result, remainder, dividendWasNegative, divisorWasNegative);
  return result;
}

std::string Int128::toString() const {
  // 10**18 - the largest power of 10 less than 63 bits
  const Int128 tenTo18(0xde0b6b3a7640000);
  // 10**36
  const Int128 tenTo36(0xc097ce7bc90715, 0xb34b9f1000000000);
  Int128 remainder;
  std::string str1, str2, str3;
  bool needFill = false;

  // get anything above 10**36 and print it
  Int128 top = divide(tenTo36, remainder);
  if (top != 0) {
    str1 = std::to_string(top.toLong());
    remainder.abs();
    needFill = true;
  }

  // now get anything above 10**18 and print it
  Int128 tail;
  top = remainder.divide(tenTo18, tail);
  if (needFill || top != 0) {
    str2 = std::to_string(top.toLong());
    if (needFill) {
      while (str2.length() < 18) {
        str2 = "0" + str2;
      }
    } else {
      needFill = true;
      tail.abs();
    }
  }

  // finally print the tail, which is less than 10**18
  str3 = std::to_string(tail.toLong());
  if (needFill) {
    while (str3.length() < 18) {
      str3 = "0" + str3;
    }
  }
  return str1 + str2 + str3;
}

std::string Int128::toDecimalString(int32_t scale) const {
  std::string str = toString();
  if (scale == 0) {
    return str;
  } else if (*this < 0) {
    int32_t len = static_cast<int32_t>(str.length());
    if (len - 1 > scale) {
      return str.substr(0, static_cast<size_t>(len - scale)) + "." +
             str.substr(static_cast<size_t>(len - scale),
                        static_cast<size_t>(scale));
    } else if (len - 1 == scale) {
      return "-0." + str.substr(1, std::string::npos);
    } else {
      std::string result = "-0.";
      for (int32_t i = 0; i < scale - len + 1; ++i) {
        result += "0";
      }
      return result + str.substr(1, std::string::npos);
    }
  } else {
    int32_t len = static_cast<int32_t>(str.length());
    if (len > scale) {
      return str.substr(0, static_cast<size_t>(len - scale)) + "." +
             str.substr(static_cast<size_t>(len - scale),
                        static_cast<size_t>(scale));
    } else if (len == scale) {
      return "0." + str;
    } else {
      std::string result = "0.";
      for (int32_t i = 0; i < scale - len; ++i) {
        result += "0";
      }
      return result + str;
    }
  }
}

std::string Int128::toHexString() const {
  std::stringstream buf;
  buf << std::hex << "0x" << std::setw(16) << std::setfill('0') << highbits
      << std::setw(16) << std::setfill('0') << lowbits;
  return buf.str();
}

Int128 scaleUpInt128ByPowerOfTen(Int128 value, int32_t power) {
  Int128 remainder;
  while (power > 0) {
    int32_t step = std::min(power, MAX_PRECISION_64);
    // we are temporarily not checking decimal overflow here for performance
    // consideration
    /*
    if (value > 0 &&
        Int128::maximumValue().divide(POWERS_OF_TEN[step], remainder) < value) {
      *overflow = true;
      return Int128::maximumValue();
    } else if (value < 0 && Int128::minimumValue().divide(POWERS_OF_TEN[step],
                                                          remainder) > value) {
      *overflow = true;
      return Int128::minimumValue();
    }*/

    value *= POWERS_OF_TEN[step];
    power -= step;
  }

  return value;
}

const static int64_t HALF_POWERS_OF_TEN[MAX_PRECISION_64 + 1] = {  // NOLINT
    0,
    5,
    50,
    500,
    5000,
    50000,
    500000,
    5000000,
    50000000,
    500000000,
    5000000000,
    50000000000,
    500000000000,
    5000000000000,
    50000000000000,
    500000000000000,
    5000000000000000,
    50000000000000000,
    500000000000000000};

Int128 scaleDownInt128ByPowerOfTen(Int128 value, int32_t power) {
  if (power == 0) return value;

  Int128 remainder;
  int32_t step;

  if (value > 0) {
    while (power > 0) {
      step = std::min(std::abs(power), MAX_PRECISION_64);
      value = value.divide(POWERS_OF_TEN[step], remainder);
      power -= step;
    }
    if (remainder.getLowBits() >= HALF_POWERS_OF_TEN[step]) value += 1;
  } else if (value < 0) {
    value.negate();
    while (power > 0) {
      step = std::min(std::abs(power), MAX_PRECISION_64);
      value = value.divide(POWERS_OF_TEN[step], remainder);
      power -= step;
    }
    if (remainder.getLowBits() >= HALF_POWERS_OF_TEN[step]) value += 1;
    value.negate();
  }
  return value;
}

Int128 scaleDownInt128ByPowerOfTenWithCeil(Int128 value, int32_t power) {
  if (power == 0) return value;

  Int128 remainder;
  int32_t step;

  if (value > 0) {
    while (power > 0) {
      step = std::min(std::abs(power), MAX_PRECISION_64);
      value = value.divide(POWERS_OF_TEN[step], remainder);
      power -= step;
    }
    if (remainder.getLowBits() > 0) value += 1;
  } else if (value < 0) {
    value.negate();
    while (power > 0) {
      step = std::min(std::abs(power), MAX_PRECISION_64);
      value = value.divide(POWERS_OF_TEN[step], remainder);
      power -= step;
    }
    value.negate();
  }
  return value;
}

Int128 scaleDownInt128ByPowerOfTenWithFloor(Int128 value, int32_t power) {
  if (power == 0) return value;

  Int128 remainder;
  int32_t step;

  if (value > 0) {
    while (power > 0) {
      step = std::min(std::abs(power), MAX_PRECISION_64);
      value = value.divide(POWERS_OF_TEN[step], remainder);
      power -= step;
    }
  } else if (value < 0) {
    value.negate();
    while (power > 0) {
      step = std::min(std::abs(power), MAX_PRECISION_64);
      value = value.divide(POWERS_OF_TEN[step], remainder);
      power -= step;
    }
    if (remainder.getLowBits() > 0) value += 1;
    value.negate();
  }
  return value;
}

Int128 scaleDownInt128ByPowerOfTenWithTrunc(Int128 value, int32_t power) {
  if (power == 0) return value;

  Int128 remainder;
  int32_t step;

  if (value > 0) {
    while (power > 0) {
      step = std::min(std::abs(power), MAX_PRECISION_64);
      value = value.divide(POWERS_OF_TEN[step], remainder);
      power -= step;
    }
  } else if (value < 0) {
    value.negate();
    while (power > 0) {
      step = std::min(std::abs(power), MAX_PRECISION_64);
      value = value.divide(POWERS_OF_TEN[step], remainder);
      power -= step;
    }
    value.negate();
  }
  return value;
}

#define NUMERIC_MIN_SIG_DIGITS 16
#define DEC_DIGITS 4
DecimalVar DecimalVar::operator/=(const DecimalVar& r) {
  DecimalVar dividend = *this;
  DecimalVar divisor = r;
  DecimalVar quotient;

  Int128 dividendInt(dividend.highbits, dividend.lowbits);
  Int128 divisorInt(divisor.highbits, divisor.lowbits);
  Int128 quotientInt, remainderInt;

  // mimic select_div_scale in numeric.c
  auto weight1 = dividendInt.getNumOfDigit();
  auto weight2 = divisorInt.getNumOfDigit();
  auto qweight = weight1 - weight2;
  auto rscale = NUMERIC_MIN_SIG_DIGITS - qweight;
  quotient.scale =
      std::max<uint64_t>(std::max(dividend.scale, divisor.scale), rscale);

  if (divisor.scale > dividend.scale) {
    dividendInt =
        scaleUpInt128ByPowerOfTen(dividendInt, divisor.scale - dividend.scale);
  } else if (divisor.scale < dividend.scale) {
    divisorInt =
        scaleUpInt128ByPowerOfTen(divisorInt, dividend.scale - divisor.scale);
  }

  quotientInt = scaleUpInt128ByPowerOfTen(dividendInt, quotient.scale);
  quotientInt = quotientInt.divide(divisorInt, remainderInt);
  quotient.highbits = quotientInt.getHighBits();
  quotient.lowbits = quotientInt.getLowBits();
  *this = quotient;
  return quotient;
}

int DecimalType::compare(const Datum& a, const Datum& b) const { return 0; }
int DecimalType::compare(const char* str1, uint64_t len1, const char* str2,
                         uint64_t len2) const {
  assert(len1 == (sizeof(uint64_t) + sizeof(int64_t) + sizeof(int64_t)));
  assert(len2 == (sizeof(uint64_t) + sizeof(int64_t) + sizeof(int64_t)));
  const DecimalVar* val1 = reinterpret_cast<const DecimalVar*>(str1);
  const DecimalVar* val2 = reinterpret_cast<const DecimalVar*>(str2);
  dbcommon::Int128 decVal1 = dbcommon::Int128(val1->highbits, val1->lowbits);
  dbcommon::Int128 decVal2 = dbcommon::Int128(val2->highbits, val2->lowbits);
  if (val1->scale < val2->scale) {
    decVal1 = scaleUpInt128ByPowerOfTen(decVal1, val2->scale - val1->scale);
  } else {
    decVal2 = scaleUpInt128ByPowerOfTen(decVal2, val1->scale - val2->scale);
  }
  if (decVal1 == decVal2) return 0;
  return decVal1 < decVal2 ? -1 : 1;
}

}  // namespace dbcommon
