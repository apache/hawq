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

#ifndef DBCOMMON_SRC_DBCOMMON_NODES_DATUM_H_
#define DBCOMMON_SRC_DBCOMMON_NODES_DATUM_H_

#include <string>

#define SECOND_TO_NANOSECOND (1000000000)
namespace dbcommon {

class Object {
 public:
  virtual void clear() = 0;
  virtual ~Object() {}
};

// text(char*, int64_t)
struct text {
  text(const char *val, int64_t length) : val(val), length(length) {}
  const char *val;
  int64_t length;
};

struct Timestamp {
  int64_t second;
  int64_t nanosecond;

  Timestamp(int64_t second, int64_t nanosecond)
      : second(second), nanosecond(nanosecond) {}

  Timestamp() : Timestamp(0, 0) {}

  explicit Timestamp(int64_t nanoSecond) : second(0) {
    nanosecond = nanoSecond;
  }

  Timestamp &operator=(const Timestamp &ts) {
    second = ts.second;
    nanosecond = ts.nanosecond;
    return *this;
  }

  operator uint64_t() { return nanosecond; }

  friend bool operator<(const Timestamp &lts, const Timestamp &rts) {
    Timestamp res(lts.second - rts.second, lts.nanosecond - rts.nanosecond);
    if (res.nanosecond >= SECOND_TO_NANOSECOND) {
      ++res.second;
      res.nanosecond -= SECOND_TO_NANOSECOND;
    }
    if (res.nanosecond < 0) {
      --res.second;
      res.nanosecond += SECOND_TO_NANOSECOND;
    }
    return (res.second < 0);
  }

  friend bool operator>(const Timestamp &lts, const Timestamp &rts) {
    Timestamp res(lts.second - rts.second, lts.nanosecond - rts.nanosecond);
    if (res.nanosecond >= SECOND_TO_NANOSECOND) {
      ++res.second;
      res.nanosecond -= SECOND_TO_NANOSECOND;
    }
    if (res.nanosecond < 0) {
      --res.second;
      res.nanosecond += SECOND_TO_NANOSECOND;
    }
    return (res.second > 0 || (res.second == 0 && res.nanosecond > 0));
  }

  friend bool operator>=(const Timestamp &lts, const Timestamp &rts) {
    Timestamp res(lts.second - rts.second, lts.nanosecond - rts.nanosecond);
    if (res.nanosecond >= SECOND_TO_NANOSECOND) {
      ++res.second;
      res.nanosecond -= SECOND_TO_NANOSECOND;
    }
    if (res.nanosecond < 0) {
      --res.second;
      res.nanosecond += SECOND_TO_NANOSECOND;
    }
    return (res.second >= 0);
  }

  friend bool operator<=(const Timestamp &lts, const Timestamp &rts) {
    Timestamp res(lts.second - rts.second, lts.nanosecond - rts.nanosecond);
    if (res.nanosecond >= SECOND_TO_NANOSECOND) {
      ++res.second;
      res.nanosecond -= SECOND_TO_NANOSECOND;
    }
    if (res.nanosecond < 0) {
      --res.second;
      res.nanosecond += SECOND_TO_NANOSECOND;
    }
    return (res.second < 0 || (res.second == 0 && res.nanosecond == 0));
  }

  friend bool operator==(const Timestamp &lts, const Timestamp &rts) {
    Timestamp res(lts.second - rts.second, lts.nanosecond - rts.nanosecond);
    if (res.nanosecond >= SECOND_TO_NANOSECOND) {
      ++res.second;
      res.nanosecond -= SECOND_TO_NANOSECOND;
    }
    if (res.nanosecond < 0) {
      --res.second;
      res.nanosecond += SECOND_TO_NANOSECOND;
    }
    return (res.second == 0 && res.nanosecond == 0);
  }

  friend bool operator!=(const Timestamp &lts, const Timestamp &rts) {
    return !(lts == rts);
  }

  friend Timestamp operator+(const Timestamp &lts, const Timestamp &rts) {
    Timestamp res(lts.second + rts.second, lts.nanosecond + rts.nanosecond);
    if (res.nanosecond >= SECOND_TO_NANOSECOND) {
      ++res.second;
      res.nanosecond -= SECOND_TO_NANOSECOND;
    }
    return res;
  }

  friend Timestamp operator-(const Timestamp &lts, const Timestamp &rts) {
    Timestamp res(lts.second - rts.second, lts.nanosecond - rts.nanosecond);
    if (res.nanosecond < 0) {
      --res.second;
      res.nanosecond += SECOND_TO_NANOSECOND;
    }
    return res;
  }
};

// tid in magma
struct MagmaTid {
  uint16_t rangeId;
  uint64_t rowId;

  MagmaTid() : rangeId(0), rowId(0) {}
  explicit MagmaTid(const int32_t &x) : rangeId(0), rowId(x) {}
  MagmaTid(uint16_t rangeId, uint64_t rowId) : rangeId(rangeId), rowId(rowId) {}

  void operator=(const int32_t &x) {
    rangeId = 0;
    rowId = x;
  }

  void operator=(const MagmaTid &x) {
    rangeId = x.rangeId;
    rowId = x.rowId;
  }

  friend bool operator>(const MagmaTid &x, const MagmaTid &y) {
    return cmp(x, y) > 0;
  }

  friend bool operator>=(const MagmaTid &x, const MagmaTid &y) {
    return cmp(x, y) >= 0;
  }

  friend bool operator<(const MagmaTid &x, const MagmaTid &y) {
    return cmp(x, y) < 0;
  }

  friend bool operator<=(const MagmaTid &x, const MagmaTid &y) {
    return cmp(x, y) <= 0;
  }

  friend bool operator==(const MagmaTid &x, const MagmaTid &y) {
    return cmp(x, y) == 0;
  }

  friend bool operator==(const MagmaTid &x, const int &y) { return false; }

  friend bool operator!=(const MagmaTid &x, const MagmaTid &y) {
    return cmp(x, y) != 0;
  }

  friend MagmaTid operator+(const MagmaTid &x, const MagmaTid &y) {
    return MagmaTid(x.rangeId + y.rangeId, x.rowId + y.rowId);
  }

  friend MagmaTid operator-(const MagmaTid &x, const MagmaTid &y) {
    return MagmaTid(x.rangeId - y.rangeId, x.rowId - y.rowId);
  }

  operator int64_t() const { return rowId; }

  static int8_t cmp(const MagmaTid &x, const MagmaTid &y) {
    if (x.rangeId == y.rangeId) {
      if (x.rowId == y.rowId) {
        return 0;
      } else {
        return x.rowId < y.rowId ? -1 : 1;
      }
    } else {
      return x.rangeId < y.rangeId ? -1 : 1;
    }
  }
};

/**
 * A structure to represent a scalar value
 */
struct Datum {
  union ValutType {
    int64_t i64;
    int32_t i32;
    int16_t i16;
    int8_t i8;

    ValutType() {}
    explicit ValutType(int64_t v) : i64(v) {}
    explicit ValutType(int32_t v) : i64(0) { i32 = v; }
    explicit ValutType(int16_t v) : i64(0) { i16 = v; }
    explicit ValutType(int8_t v) : i64(0) { i8 = v; }
  } value;

  Datum() {}
  explicit Datum(int64_t v) : value(v) {}
  explicit Datum(int32_t v) : value(v) {}
  explicit Datum(int16_t v) : value(v) {}
  explicit Datum(int8_t v) : value(v) {}

  template <typename T>
  operator T() {
    static_assert(sizeof(T) <= sizeof(value),
                  "Datum may not contain data type that is not fit into it");
    return *reinterpret_cast<T *>(&value);
  }
};

// Compare to datum and return true if they are equal.
// @param d1 The left datum of equal operator.
// @param d2 The right datum of equal operator.
// @return Return true if d1 is equal d2.
static inline bool operator==(const Datum &d1, const Datum &d2) {
  return d1.value.i64 == d2.value.i64;
}

// Compare to datum and return true if they are not equal.
// @param d1 The left datum of not equal operator.
// @param d2 The right datum of not equal operator.
// @return Return true if d1 is not equal d2.
static inline bool operator!=(const Datum &d1, const Datum &d2) {
  return d1.value.i64 != d2.value.i64;
}

// Declare DatumExtracter template and dispatch to different
// specialization based on data type size.
template <typename T, size_t = sizeof(T)>
struct DatumExtracter;

// Declare DatumExtracter template for 8 bit type.
template <typename T>
struct DatumExtracter<T, sizeof(int8_t)> {
  // Get primitive value of a datum.
  // @param d The datum which contains the value.
  // @return Return the 8 bit primitive value of the datum.
  static inline T DatumGetValue(const Datum &d) {
    return *reinterpret_cast<const T *>(&d.value.i8);
  }
};

// Declare DatumExtracter template for 16 bit type.
template <typename T>
struct DatumExtracter<T, sizeof(int16_t)> {
  // Get primitive value of a datum.
  // @param d The datum which contains the value.
  // @return Return the 16 bit primitive value of the datum.
  static inline T DatumGetValue(const Datum &d) {
    return *reinterpret_cast<const T *>(&d.value.i16);
  }
};

// Declare DatumExtracter template for 32 bit type.
template <typename T>
struct DatumExtracter<T, sizeof(int32_t)> {
  // Get primitive value of a datum.
  // @param d The datum which contains the value.
  // @return Return the 32 bit primitive value of the datum.
  static inline T DatumGetValue(const Datum &d) {
    return *reinterpret_cast<const T *>(&d.value.i32);
  }
};

// Declare DatumExtracter template for 64 bit type.
template <typename T>
struct DatumExtracter<T, sizeof(int64_t)> {
  // Get primitive value of a datum.
  // @param d The datum which contains the value.
  // @return Return the 64 bit primitive value of the datum.
  static inline T DatumGetValue(const Datum &d) {
    return *reinterpret_cast<const T *>(&d.value.i64);
  }
};

// Declare DatumExtracter template for MagmaTid type.
template <typename T>
struct DatumExtracter<T, sizeof(MagmaTid)> {
  // Get primitive value of a datum.
  // @param d The datum which contains the value.
  // @return Return MagmaTid value of the datum.
  static inline MagmaTid DatumGetValue(const Datum &d) {
    return *reinterpret_cast<const MagmaTid *>(&d.value.i64);
  }
};

// Get primitive value of a datum.
// @param d The datum which contains the value.
// @return Return the primitive value of the datum.
template <typename T>
static inline T DatumGetValue(const Datum &d) {
  return DatumExtracter<T>::DatumGetValue(d);
}

// Declare DatumOperation template and dispatch to different
// specialization based on data type size.
template <typename T, size_t = sizeof(T)>
struct DatumOperation;

template <typename T>
struct DatumOperation<T, sizeof(int64_t)> {
  static inline void DatumPlus(Datum &d, T v) {
    *reinterpret_cast<T *>(&d.value.i64) =
        *reinterpret_cast<T *>(&d.value.i64) + v;
  }
};

template <typename T>
struct DatumOperation<T, sizeof(int32_t)> {
  static inline void DatumPlus(Datum &d, T v) {
    *reinterpret_cast<T *>(&d.value.i32) =
        *reinterpret_cast<T *>(&d.value.i32) + v;
  }
};

template <typename T>
struct DatumOperation<T, sizeof(int16_t)> {
  static inline void DatumPlus(Datum &d, T v) {
    *reinterpret_cast<T *>(&d.value.i16) =
        *reinterpret_cast<T *>(&d.value.i16) + v;
  }
};

template <typename T>
struct DatumOperation<T, sizeof(int8_t)> {
  static inline void DatumPlus(Datum &d, T v) {
    *reinterpret_cast<T *>(&d.value.i8) =
        *reinterpret_cast<T *>(&d.value.i8) + v;
  }
};

template <typename T>
static inline void DatumPlus(Datum &d, T v) {  // NOLINT
  DatumOperation<T>::DatumPlus(d, v);
}

// Declare DatumCreater template and dispatch to different
// specialization based on data type size.
template <typename T, size_t = sizeof(T)>
struct DatumCreater;

// Declare DatumCreate template for 8 bit type.
template <typename T>
struct DatumCreater<T, sizeof(int8_t)> {
  // Create a datum with given 8 bit primitive type.
  // @param value The initial value of the datum.
  // @return Return a new datum.
  static inline Datum CreateDatum(const T &value) {
    return Datum(*reinterpret_cast<const int8_t *>(&value));
  }
};

// Declare DatumCreate template for 16 bit type.
template <typename T>
struct DatumCreater<T, sizeof(int16_t)> {
  // Create a datum with given 16 bit primitive type.
  // @param value The initial value of the datum.
  // @return Return a new datum.
  static inline Datum CreateDatum(const T &value) {
    return Datum(*reinterpret_cast<const int16_t *>(&value));
  }
};

// Declare DatumCreate template for 32 bit type.
template <typename T>
struct DatumCreater<T, sizeof(int32_t)> {
  // Create a datum with given 32 bit primitive type.
  // @param value The initial value of the datum.
  // @return Return a new datum.
  static inline Datum CreateDatum(const T &value) {
    return Datum(*reinterpret_cast<const int32_t *>(&value));
  }
};

// Declare DatumCreate template for 64 bit type.
template <typename T>
struct DatumCreater<T, sizeof(int64_t)> {
  // Create a datum with given MagmaTid type.
  // @param value The initial value of the datum.
  // @return Return a new datum.
  static inline Datum CreateDatum(const T &value) {
    return Datum(*reinterpret_cast<const int64_t *>(&value));
  }
};

// Declare DatumCreate template for MagmaTid type.
template <typename T>
struct DatumCreater<T, sizeof(MagmaTid)> {
  // Create a datum with given 64 bit primitive type.
  // @param value The initial value of the datum.
  // @return Return a new datum.
  static inline dbcommon::Datum CreateDatum(const T &value) {
    return Datum(reinterpret_cast<int64_t>(&value));
  }
};

// Declare DatumCreate template for std::string.
template <typename T>
struct DatumCreater<T, sizeof(std::string)> {
  // Create a datum with given std::string.
  // @param value The initial value of the datum.
  // @return Return a new datum.
  static inline Datum CreateDatum(const T &value) {
    return Datum(reinterpret_cast<int64_t>(value.c_str()));
  }
};

// Create a datum with given primitive type.
// @param value The initial value of the datum.
// @return Return a new datum.
template <typename T>
static inline Datum CreateDatum(T const &value) {
  return DatumCreater<typename std::remove_cv<T>::type>::CreateDatum(value);
}

// Create a datum with the given data type ID.
// @param str The string representation of the data.
// @param typeId The data type ID of the return datum.
// @return Return a new datum.
Datum CreateDatum(const char *str, int typeId);

Datum CreateDatum(const char *str, Timestamp *timestamp, int typeId);

// Copy a datum, if the datum is pass by reference,
//   copy the content of the datum.
// @param d The datum to be copied.
// @param typeId The data type ID of the datum.
// @return Return the new datum.
Datum CopyDatum(const Datum &d, int typeId);

// Convert datum to a string.
// @param d The datum to be convert.
// @param typeId The data type ID of datum.
// @return Return the string of datum.
std::string DatumToString(const Datum &d, int typeId);

// Dump datum to buffer as binary.
// @param d The datum to be convert.
// @param typeId The data type ID of datum.
// @return Return the binary of datum.
std::string DatumToBinary(const Datum &d, int typeId);

}  // namespace dbcommon.

#endif  // DBCOMMON_SRC_DBCOMMON_NODES_DATUM_H_
