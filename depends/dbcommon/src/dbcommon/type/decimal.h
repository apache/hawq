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

#ifndef DBCOMMON_SRC_DBCOMMON_TYPE_DECIMAL_H_
#define DBCOMMON_SRC_DBCOMMON_TYPE_DECIMAL_H_

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>

#include "dbcommon/type/typebase.h"

namespace dbcommon {

class Int128 {
 public:
  Int128() {
    highbits = 0;
    lowbits = 0;
  }

  // Convert a signed 64 bit value into an Int128.
  Int128(int64_t right) {    // NOLINT
    highbits = right >> 63;  // sign extension
    lowbits = static_cast<uint64_t>(right);
  }

  // Create from the twos complement representation.
  Int128(int64_t high, uint64_t low) {
    highbits = high;
    lowbits = low;
  }

  // Parse the number from a base 10 string representation.
  explicit Int128(const std::string&);

  // Maximum positive value allowed by the type.
  static Int128 maximumValue();

  // Minimum negative value allowed by the type.
  static Int128 minimumValue();

  bool isNegative() { return highbits & (int64_t)0x8000000000000000; }

  Int128& negate() {
    lowbits = ~lowbits + 1;
    highbits = ~highbits;
    if (lowbits == 0) {
      highbits += 1;
    }
    return *this;
  }

  Int128& abs() {
    if (highbits < 0) {
      negate();
    }
    return *this;
  }

  Int128& invert() {
    lowbits = ~lowbits;
    highbits = ~highbits;
    return *this;
  }

  // Add a number to this one. The result is truncated to 128 bits.
  // @param right the number to add
  // @return *this
  Int128& operator+=(const Int128& right) {
    uint64_t sum = lowbits + right.lowbits;
    highbits += right.highbits;
    if (sum < lowbits) {
      highbits += 1;
    }
    lowbits = sum;
    return *this;
  }

  // Subtract a number from this one. The result is truncated to 128 bits.
  // @param right the number to subtract
  // @return *this
  Int128& operator-=(const Int128& right) {
    uint64_t diff = lowbits - right.lowbits;
    highbits -= right.highbits;
    if (diff > lowbits) {
      highbits -= 1;
    }
    lowbits = diff;
    return *this;
  }

  // Multiply this number by a number. The result is truncated to 128 bits.
  // @param right the number to multiply by
  // @return *this
  Int128& operator*=(const Int128& right);

  // Divide this number by right and return the result. This operation is
  // not destructive.
  //
  // The answer rounds to zero. Signs work like:
  //    21 /  5 ->  4,  1
  //   -21 /  5 -> -4, -1
  //    21 / -5 -> -4,  1
  //   -21 / -5 ->  4, -1
  // @param right the number to divide by
  // @param remainder the remainder after the division
  Int128 divide(const Int128& right, Int128& remainder) const;  // NOLINT

  // Logical or between two Int128.
  // @param right the number to or in
  // @return *this
  Int128& operator|=(const Int128& right) {
    lowbits |= right.lowbits;
    highbits |= right.highbits;
    return *this;
  }

  // Logical and between two Int128.
  // @param right the number to and in
  // @return *this
  Int128& operator&=(const Int128& right) {
    lowbits &= right.lowbits;
    highbits &= right.highbits;
    return *this;
  }

  // Shift left by the given number of bits.
  // Values larger than 2**127 will shift into the sign bit.
  Int128& operator<<=(uint32_t bits) {
    if (bits != 0) {
      if (bits < 64) {
        highbits <<= bits;
        highbits |= (lowbits >> (64 - bits));
        lowbits <<= bits;
      } else if (bits < 128) {
        highbits = static_cast<int64_t>(lowbits) << (bits - 64);
        lowbits = 0;
      } else {
        highbits = 0;
        lowbits = 0;
      }
    }
    return *this;
  }

  // Shift right by the given number of bits. Negative values will
  // sign extend and fill with one bits.
  Int128& operator>>=(uint32_t bits) {
    if (bits != 0) {
      if (bits < 64) {
        lowbits >>= bits;
        lowbits |= static_cast<uint64_t>(highbits << (64 - bits));
        highbits =
            static_cast<int64_t>(static_cast<uint64_t>(highbits) >> bits);
      } else if (bits < 128) {
        lowbits = static_cast<uint64_t>(highbits >> (bits - 64));
        highbits = highbits >= 0 ? 0 : -1l;
      } else {
        highbits = highbits >= 0 ? 0 : -1l;
        lowbits = static_cast<uint64_t>(highbits);
      }
    }
    return *this;
  }

  bool operator==(const Int128& right) const {
    return highbits == right.highbits && lowbits == right.lowbits;
  }

  bool operator!=(const Int128& right) const {
    return highbits != right.highbits || lowbits != right.lowbits;
  }

  bool operator<(const Int128& right) const {
    if (highbits == right.highbits) {
      return lowbits < right.lowbits;
    } else {
      return highbits < right.highbits;
    }
  }

  bool operator<=(const Int128& right) const {
    if (highbits == right.highbits) {
      return lowbits <= right.lowbits;
    } else {
      return highbits <= right.highbits;
    }
  }

  bool operator>(const Int128& right) const {
    if (highbits == right.highbits) {
      return lowbits > right.lowbits;
    } else {
      return highbits > right.highbits;
    }
  }

  bool operator>=(const Int128& right) const {
    if (highbits == right.highbits) {
      return lowbits >= right.lowbits;
    } else {
      return highbits >= right.highbits;
    }
  }

  uint32_t hash() const {
    return static_cast<uint32_t>(highbits >> 32) ^
           static_cast<uint32_t>(highbits) ^
           static_cast<uint32_t>(lowbits >> 32) ^
           static_cast<uint32_t>(lowbits);
  }

  // Does this value fit into a long?
  bool fitsInLong() const {
    switch (highbits) {
      case 0:
        return !(lowbits & LONG_SIGN_BIT);
      case -1:
        return lowbits & LONG_SIGN_BIT;
      default:
        return false;
    }
  }

  // Convert the value to a long and
  int64_t toLong() const {
    if (fitsInLong()) {
      return static_cast<int64_t>(lowbits);
    }
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Int128 too large to convert to long");
  }

  // Return the base 10 string representation of the integer.
  std::string toString() const;

  // Return the base 10 string representation with a decimal point,
  // the given number of places after the decimal.
  std::string toDecimalString(int32_t scale = 0) const;

  // Return the base 16 string representation of the two's complement with
  // a prefix of "0x".
  // Int128(-1).toHexString() = "0xffffffffffffffffffffffffffffffff".
  std::string toHexString() const;

  // Get the high bits of the twos complement representation of the number.
  int64_t getHighBits() { return highbits; }

  // Get the low bits of the twos complement representation of the number.
  uint64_t getLowBits() { return lowbits; }

  // Represent the absolute number as a list of uint32.
  // Visible for testing only.
  // @param array the array that is set to the value of the number
  // @param wasNegative set to true if the original number was negative
  // @return the number of elements that were set in the array (1 to 4)
  int64_t fillInArray(uint32_t* array, bool& wasNegative) const;  // NOLINT

  // Return number of digits in the base 10 representation.
  size_t getNumOfDigit();

 private:
  static const uint64_t LONG_SIGN_BIT = 0x8000000000000000u;
  int64_t highbits;
  uint64_t lowbits;
};

const static int32_t MAX_PRECISION_64 = 18;                   // NOLINT
const static int64_t POWERS_OF_TEN[MAX_PRECISION_64 + 1] = {  // NOLINT
    1,
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000,
    10000000000,
    100000000000,
    1000000000000,
    10000000000000,
    100000000000000,
    1000000000000000,
    10000000000000000,
    100000000000000000,
    1000000000000000000};

/**
 * Scales up an Int128 value
 * @param value the Int128 value to scale
 * @param power the scale offset. Result of a negative factor is undefined.
 * @param overflow returns whether the result overflows or not
 * @return the scaled value
 */
Int128 scaleUpInt128ByPowerOfTen(Int128 value, int32_t power);
/**
 * Scales down an Int128 value. Return value is rounded half up.
 * @param value the Int128 value to scale
 * @param power the scale offset. Result of a negative factor is undefined.
 * @return the scaled value
 */
Int128 scaleDownInt128ByPowerOfTen(Int128 value, int32_t power);

Int128 scaleDownInt128ByPowerOfTenWithCeil(Int128 value, int32_t power);

Int128 scaleDownInt128ByPowerOfTenWithFloor(Int128 value, int32_t power);

Int128 scaleUpInt128ByPowerOfTenWithRound(Int128 value, int32_t power);

Int128 scaleDownInt128ByPowerOfTenWithTrunc(Int128 value, int32_t power);

Int128 scaleUpInt128ByPowerOfTenWithRound(Int128 value, int32_t power);

struct DecimalVar {
  int64_t highbits;
  uint64_t lowbits;
  int64_t scale;

  DecimalVar() : highbits(0), lowbits(0), scale(0) {}

  explicit DecimalVar(uint64_t lowbits)
      : highbits(0), lowbits(lowbits), scale(0) {}

  DecimalVar(int64_t highbits, uint64_t lowbits, int64_t scale)
      : highbits(highbits), lowbits(lowbits), scale(scale) {}

  DecimalVar(const DecimalVar& dec)
      : highbits(dec.highbits), lowbits(dec.lowbits), scale(dec.scale) {}

  explicit DecimalVar(int64_t val)
      : highbits(val >> 63), lowbits(val), scale(0) {}

  DecimalVar cast(int64_t scale) {
    Int128 x(this->highbits, this->lowbits);
    if (scale < this->scale)
      x = scaleDownInt128ByPowerOfTen(x, this->scale - scale);
    if (scale > this->scale)
      x = scaleUpInt128ByPowerOfTen(x, scale - this->scale);
    return DecimalVar(x.getHighBits(), x.getLowBits(), scale);
  }

  DecimalVar ceil() {
    Int128 x(this->highbits, this->lowbits);
    x = scaleDownInt128ByPowerOfTenWithCeil(x, this->scale);
    return DecimalVar(x.getHighBits(), x.getLowBits(), 0);
  }

  DecimalVar floor() {
    Int128 x(this->highbits, this->lowbits);
    x = scaleDownInt128ByPowerOfTenWithFloor(x, this->scale);
    return DecimalVar(x.getHighBits(), x.getLowBits(), 0);
  }

  DecimalVar round(int32_t roundPos = 0) {
    Int128 x(this->highbits, this->lowbits);
    if (this->scale > roundPos) {
      x = scaleDownInt128ByPowerOfTen(x, this->scale - roundPos);
    } else if (this->scale < roundPos) {
      x = scaleUpInt128ByPowerOfTen(x, roundPos - this->scale);
    }
    if (roundPos < 0) {
      int32_t step = std::abs(roundPos);
      x *= POWERS_OF_TEN[step];
      roundPos = 0;
    }
    return DecimalVar(x.getHighBits(), x.getLowBits(), roundPos);
  }

  DecimalVar trunc(int32_t truncPos = 0) {
    Int128 x(this->highbits, this->lowbits);
    if (this->scale < truncPos) {
      x = scaleUpInt128ByPowerOfTen(x, truncPos - this->scale);
    } else if (this->scale > truncPos) {
      x = scaleDownInt128ByPowerOfTenWithTrunc(x, this->scale - truncPos);
    } else {
      return *this;
    }
    if (truncPos < 0) {
      int32_t step = std::abs(truncPos);
      x *= POWERS_OF_TEN[step];
      truncPos = 0;
    }
    return DecimalVar(x.getHighBits(), x.getLowBits(), truncPos);
  }

  DecimalVar operator=(int64_t var) {
    this->highbits = 0;
    this->lowbits = var;
    this->scale = 0;
    return *this;
  }

  DecimalVar operator*=(int64_t size) {
    Int128 tmp(this->highbits, this->lowbits);
    tmp *= Int128(size);

    this->highbits = tmp.getHighBits();
    this->lowbits = tmp.getLowBits();
    return *this;
  }

  DecimalVar operator*=(const DecimalVar& r) {
    Int128 tmp(this->highbits, this->lowbits);
    tmp *= Int128(r.highbits, r.lowbits);

    this->highbits = tmp.getHighBits();
    this->lowbits = tmp.getLowBits();
    this->scale += r.scale;
    return *this;
  }

  DecimalVar operator*(const DecimalVar& r) const {
    DecimalVar ret(*this);
    ret *= r;
    return ret;
  }

  DecimalVar operator+=(const DecimalVar& r) {
    Int128 val1(this->highbits, this->lowbits);
    Int128 val2(r.highbits, r.lowbits);

    if (scale < r.scale) {
      val1 = scaleUpInt128ByPowerOfTen(val1, r.scale - scale);
      scale = r.scale;
    }
    if (scale > r.scale) {
      val2 = scaleUpInt128ByPowerOfTen(val2, scale - r.scale);
    }
    val1 += val2;

    this->highbits = val1.getHighBits();
    this->lowbits = val1.getLowBits();
    return *this;
  }

  DecimalVar operator+(const DecimalVar& r) const {
    DecimalVar ret(*this);
    ret += r;
    return ret;
  }

  DecimalVar operator/=(const DecimalVar& r);

  DecimalVar operator/(const DecimalVar& r) const {
    DecimalVar ret(*this);
    ret /= r;
    return ret;
  }

  DecimalVar operator-=(const DecimalVar& r) {
    Int128 val1(this->highbits, this->lowbits);
    Int128 val2(r.highbits, r.lowbits);

    if (scale < r.scale) {
      val1 = scaleUpInt128ByPowerOfTen(val1, r.scale - scale);
      scale = r.scale;
    }
    if (scale > r.scale) {
      val2 = scaleUpInt128ByPowerOfTen(val2, scale - r.scale);
    }
    val1 -= val2;

    this->highbits = val1.getHighBits();
    this->lowbits = val1.getLowBits();
    return *this;
  }

  DecimalVar operator-(const DecimalVar& r) const {
    DecimalVar ret(*this);
    ret -= r;
    return ret;
  }

  bool operator!=(const DecimalVar& r) const {
    Int128 val1 = Int128(this->highbits, this->lowbits);
    Int128 val2 = Int128(r.highbits, r.lowbits);
    auto scale1 = this->scale;
    auto scale2 = this->scale;
    if (scale1 < scale2) {
      val1 = scaleUpInt128ByPowerOfTen(val1,
                                       static_cast<int32_t>(scale2 - scale1));
    }
    if (scale1 > scale2) {
      val2 = scaleUpInt128ByPowerOfTen(val2,
                                       static_cast<int32_t>(scale1 - scale2));
    }
    return val1 != val2 ? true : false;
  }

  bool operator<(const DecimalVar& r) const {
    Int128 val1 = Int128(this->highbits, this->lowbits);
    Int128 val2 = Int128(r.highbits, r.lowbits);
    auto scale1 = this->scale;
    auto scale2 = r.scale;
    if (scale1 < scale2) {
      val1 = scaleUpInt128ByPowerOfTen(val1,
                                       static_cast<int32_t>(scale2 - scale1));
    }
    if (scale1 > scale2) {
      val2 = scaleUpInt128ByPowerOfTen(val2,
                                       static_cast<int32_t>(scale1 - scale2));
    }
    return val1 < val2 ? true : false;
  }

  bool operator>(const DecimalVar& r) const {
    Int128 val1 = Int128(this->highbits, this->lowbits);
    Int128 val2 = Int128(r.highbits, r.lowbits);
    auto scale1 = this->scale;
    auto scale2 = r.scale;
    if (scale1 < scale2) {
      val1 = scaleUpInt128ByPowerOfTen(val1,
                                       static_cast<int32_t>(scale2 - scale1));
    }
    if (scale1 > scale2) {
      val2 = scaleUpInt128ByPowerOfTen(val2,
                                       static_cast<int32_t>(scale1 - scale2));
    }
    return val1 > val2 ? true : false;
  }

  bool operator>=(const DecimalVar& r) const {
    Int128 val1 = Int128(this->highbits, this->lowbits);
    Int128 val2 = Int128(r.highbits, r.lowbits);
    auto scale1 = this->scale;
    auto scale2 = r.scale;
    if (scale1 < scale2) {
      val1 = scaleUpInt128ByPowerOfTen(val1,
                                       static_cast<int32_t>(scale2 - scale1));
    }
    if (scale1 > scale2) {
      val2 = scaleUpInt128ByPowerOfTen(val2,
                                       static_cast<int32_t>(scale1 - scale2));
    }
    return val1 >= val2 ? true : false;
  }

  operator uint64_t() const { return lowbits; }
};

class DecimalType : public FixedSizeTypeBase {
 public:
  DecimalType() { this->typeKind = DECIMALNEWID; }
  uint64_t getTypeWidth() const override {
    return sizeof(int64_t) + sizeof(uint64_t) + sizeof(int64_t);
  };

  Datum getDatum(const char* str) const override { return Datum(0); }

  Datum getDatum(const char* str, DecimalVar* decimal) {
    *decimal = fromString(str);
    auto ret = CreateDatum(decimal);
    return ret;
  }

  std::string DatumToString(const Datum& d) const override {
    DecimalVar* v = DatumGetValue<DecimalVar*>(d);
    int64_t highbits = v->highbits;
    uint64_t lowbits = v->lowbits;
    int64_t scale = v->scale;
    return toString(highbits, lowbits, scale);
  }
  std::string DatumToBinary(const Datum& d) const override {
    DecimalVar* v = DatumGetValue<DecimalVar*>(d);
    return std::string(reinterpret_cast<char*>(v), sizeof(DecimalVar));
  }
  static inline DecimalVar fromString(const std::string& str) {
    dbcommon::Int128 value;
    int64_t scale = 0;
    std::size_t foundPoint = str.find(".");
    if (foundPoint == std::string::npos) {
      value = Int128(str);
    } else {
      std::size_t ePos = str.find("e");
      if (ePos == std::string::npos) {
        scale = str.length() - foundPoint - 1;
        std::string copy(str);
        value = Int128(copy.replace(foundPoint, 1, ""));
      } else {
        std::string validNum = str.substr(0, ePos);
        std::string powNum = str.substr(ePos + 1);
        int pNum = std::stoi(powNum);
        if (pNum > 0) {
          std::string copy(validNum);
          std::size_t validNumPt = copy.find(".");
          value = Int128(copy.replace(foundPoint, 1, ""));
          pNum = pNum - validNum.length() + validNumPt + 1;
          value = scaleUpInt128ByPowerOfTen(value, pNum);
          scale = 0;
        } else {
          std::string copy(validNum);
          scale = copy.length() - foundPoint - 1 + std::abs(pNum);
          value = Int128(copy.replace(foundPoint, 1, ""));
        }
      }
    }
    DecimalVar ret = {value.getHighBits(), value.getLowBits(), scale};
    return ret;
  }

  static inline std::string toString(int64_t highbits, uint64_t lowbits,
                                     int64_t scale) {
    dbcommon::Int128 value = dbcommon::Int128(highbits, lowbits);
    return value.toDecimalString(scale);
  }

  static inline std::string toString(DecimalVar dec) {
    return toString(dec.highbits, dec.lowbits, dec.scale);
  }

  // = return 0, < return negative, > return positive;
  int compare(const Datum& a, const Datum& b) const override;
  int compare(const char* str1, uint64_t len1, const char* str2,
              uint64_t len2) const override;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_TYPE_DECIMAL_H_
