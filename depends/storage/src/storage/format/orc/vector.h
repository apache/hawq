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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_VECTOR_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_VECTOR_H_

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <list>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "dbcommon/common/vector.h"

#include "storage/format/orc/data-buffer.h"
#include "storage/format/orc/exceptions.h"
#include "storage/format/orc/int128.h"
#include "storage/format/orc/type.h"

namespace orc {
// The base class for each of the column vectors. This class handles
// the generic attributes such as number of elements, capacity, and
// notNull vector.
struct ColumnVectorBatch {
  explicit ColumnVectorBatch(uint64_t capacity,
                             dbcommon::MemoryPool& pool);  // NOLINT
  virtual ~ColumnVectorBatch();

  // the number of slots available
  uint64_t capacity;
  // the number of current occupied slots
  uint64_t numElements;
  // an array of capacity length marking non-null values
  DataBuffer<char> notNull;
  // whether there are any null values
  bool hasNulls;

  // stripe statistics part
  bool hasStats;
  dbcommon::VectorStatistics stats;

  // custom memory pool
  dbcommon::MemoryPool& memoryPool;

  // Generate a description of this vector as a string.
  virtual std::string toString() const = 0;

  // Change the number of slots to at least the given capacity.
  // This function is not recursive into subtypes.
  virtual void resize(uint64_t capacity);

  // Heap memory used by the batch.
  virtual uint64_t getMemoryUsage();

  // Check whether the batch length varies depending on data.
  virtual bool hasVariableLength() = 0;

  // Get the type
  virtual ORCTypeKind getType() = 0;

  // Get the data array pointer
  virtual const char* getData() const = 0;
  virtual const char* getNanoseconds() const { return nullptr; }
  virtual const char* getAuxiliaryData() const { return nullptr; }
  virtual const char* getScaleData() const { return nullptr; }

  virtual uint32_t getWidth() = 0;

  char* getNotNull() { return notNull.data(); }

  // Build the corresponding dbcommon vector
  virtual std::unique_ptr<dbcommon::Vector> buildVector() = 0;
  virtual std::unique_ptr<dbcommon::Vector> buildVector(
      dbcommon::TypeKind type) {
    return nullptr;
  }

 private:
  ColumnVectorBatch(const ColumnVectorBatch&);
  ColumnVectorBatch& operator=(const ColumnVectorBatch&);
};

template <class ElementType>
struct FixedSizeVectorBatch : public ColumnVectorBatch {
  explicit FixedSizeVectorBatch(uint64_t capacity,
                                dbcommon::MemoryPool& pool)  // NOLINT
      : ColumnVectorBatch(capacity, pool), data(pool, capacity) {}

  virtual ~FixedSizeVectorBatch() {}

  std::string toString() const override {
    std::ostringstream buffer;
    buffer << "Integer vector <" << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void resize(uint64_t cap) override {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      data.resize(cap);
    }
  }

  bool hasVariableLength() override { return false; }

  uint64_t getMemoryUsage() override {
    return ColumnVectorBatch::getMemoryUsage() +
           static_cast<uint64_t>(data.capacity() * sizeof(ElementType));
  }

  char* getData() const override {
    return (char*)(data.data());  // NOLINT
  }

  uint32_t getWidth() override { return sizeof(ElementType); }

  DataBuffer<ElementType> data;
};

struct LongVectorBatch : public FixedSizeVectorBatch<int64_t> {
  explicit LongVectorBatch(uint64_t capacity,
                           dbcommon::MemoryPool& pool)  // NOLINT
      : FixedSizeVectorBatch<int64_t>(capacity, pool) {}
  virtual ~LongVectorBatch() {}

  ORCTypeKind getType() override { return ORCTypeKind::LONG; }

  std::unique_ptr<dbcommon::Vector> buildVector() override {
    return dbcommon::Vector::BuildVector(dbcommon::TypeKind::BIGINTID,
                                         hasStats);
  }
};

struct IntVectorBatch : public FixedSizeVectorBatch<int32_t> {
  explicit IntVectorBatch(uint64_t capacity,
                          dbcommon::MemoryPool& pool)  // NOLINT
      : FixedSizeVectorBatch<int32_t>(capacity, pool) {}
  virtual ~IntVectorBatch() {}

  ORCTypeKind getType() override { return ORCTypeKind::INT; }

  std::unique_ptr<dbcommon::Vector> buildVector() override {
    return dbcommon::Vector::BuildVector(dbcommon::TypeKind::INTID, hasStats);
  }
};

struct ByteVectorBatch : public FixedSizeVectorBatch<int8_t> {
  explicit ByteVectorBatch(uint64_t capacity,
                           dbcommon::MemoryPool& pool)  // NOLINT
      : FixedSizeVectorBatch<int8_t>(capacity, pool) {}
  virtual ~ByteVectorBatch() {}

  ORCTypeKind getType() override { return ORCTypeKind::BYTE; }

  std::unique_ptr<dbcommon::Vector> buildVector() override {
    return dbcommon::Vector::BuildVector(dbcommon::TypeKind::TINYINTID,
                                         hasStats);
  }
};

struct ShortVectorBatch : public FixedSizeVectorBatch<int16_t> {
  explicit ShortVectorBatch(uint64_t capacity,
                            dbcommon::MemoryPool& pool)  // NOLINT
      : FixedSizeVectorBatch<int16_t>(capacity, pool) {}
  virtual ~ShortVectorBatch() {}

  ORCTypeKind getType() override { return ORCTypeKind::SHORT; }

  std::unique_ptr<dbcommon::Vector> buildVector() override {
    return dbcommon::Vector::BuildVector(dbcommon::TypeKind::SMALLINTID,
                                         hasStats);
  }
};

struct FloatVectorBatch : public FixedSizeVectorBatch<float> {
  explicit FloatVectorBatch(uint64_t capacity,
                            dbcommon::MemoryPool& pool)  // NOLINT
      : FixedSizeVectorBatch<float>(capacity, pool) {}
  virtual ~FloatVectorBatch() {}

  ORCTypeKind getType() override { return ORCTypeKind::FLOAT; }

  std::unique_ptr<dbcommon::Vector> buildVector() override {
    return dbcommon::Vector::BuildVector(dbcommon::TypeKind::FLOATID, hasStats);
  }
};

struct DoubleVectorBatch : public FixedSizeVectorBatch<double> {
  explicit DoubleVectorBatch(uint64_t capacity,
                             dbcommon::MemoryPool& pool)  // NOLINT
      : FixedSizeVectorBatch<double>(capacity, pool) {}
  virtual ~DoubleVectorBatch() {}

  ORCTypeKind getType() override { return ORCTypeKind::DOUBLE; }

  std::unique_ptr<dbcommon::Vector> buildVector() override {
    return dbcommon::Vector::BuildVector(dbcommon::TypeKind::DOUBLEID,
                                         hasStats);
  }
};

struct BooleanVectorBatch : public FixedSizeVectorBatch<bool> {
  explicit BooleanVectorBatch(uint64_t capacity,
                              dbcommon::MemoryPool& pool)  // NOLINT
      : FixedSizeVectorBatch<bool>(capacity, pool) {}
  virtual ~BooleanVectorBatch() {}

  ORCTypeKind getType() override { return ORCTypeKind::BOOLEAN; }

  std::unique_ptr<dbcommon::Vector> buildVector() override {
    return dbcommon::Vector::BuildVector(dbcommon::TypeKind::BOOLEANID,
                                         hasStats);
  }
};

struct BytesVectorBatch : public ColumnVectorBatch {
  virtual ~BytesVectorBatch();
  std::string toString() const override;
  void resize(uint64_t capacity) override;
  uint64_t getMemoryUsage() override;

  // pointers to the start of each string
  DataBuffer<char*> data;
  // the length of each string
  DataBuffer<int64_t> length;
  // whether a direct encoding
  bool isDirectEncoding = false;

  ORCTypeKind getType() override = 0;

  char* getData() const override {
    return (char*)data.data();  // NOLINT
  }

  uint32_t getWidth() override { return 0; }

  bool hasVariableLength() override { return true; }

  std::unique_ptr<dbcommon::Vector> buildVector() override = 0;

 protected:
  explicit BytesVectorBatch(uint64_t capacity,
                            dbcommon::MemoryPool& pool);  // NOLINT
  int64_t maxLenModifier_ = -1;
};

struct BlankPaddedCharVectorBatch : public BytesVectorBatch {
  explicit BlankPaddedCharVectorBatch(uint64_t capacity,
                                      dbcommon::MemoryPool& pool,  // NOLINT
                                      int64_t maxLenModifier = 1)
      : BytesVectorBatch(capacity, pool) {
    assert(maxLenModifier != -1);
    maxLenModifier_ = maxLenModifier;
  }
  ORCTypeKind getType() override { return ORCTypeKind::CHAR; }
  std::unique_ptr<dbcommon::Vector> buildVector() override {
    return dbcommon::Vector::BuildVector(
        dbcommon::TypeKind::CHARID, hasStats,
        dbcommon::TypeModifierUtil::getTypeModifierFromMaxLength(
            maxLenModifier_));
  }
};
struct VaryingCharVectorBatch : public BytesVectorBatch {
  explicit VaryingCharVectorBatch(uint64_t capacity,
                                  dbcommon::MemoryPool& pool,  // NOLINT
                                  int64_t maxLenModifier = -1)
      : BytesVectorBatch(capacity, pool) {
    maxLenModifier_ = maxLenModifier;
  }
  ORCTypeKind getType() override { return ORCTypeKind::VARCHAR; }
  std::unique_ptr<dbcommon::Vector> buildVector() override {
    return dbcommon::Vector::BuildVector(
        dbcommon::TypeKind::VARCHARID, hasStats,
        dbcommon::TypeModifierUtil::getTypeModifierFromMaxLength(
            maxLenModifier_));
  }
};
struct StringVectorBatch : public BytesVectorBatch {
  explicit StringVectorBatch(uint64_t capacity,
                             dbcommon::MemoryPool& pool)  // NOLINT
      : BytesVectorBatch(capacity, pool) {}
  ORCTypeKind getType() override { return ORCTypeKind::STRING; }
  std::unique_ptr<dbcommon::Vector> buildVector() override {
    return dbcommon::Vector::BuildVector(dbcommon::TypeKind::STRINGID,
                                         hasStats);
  }
};
struct BinaryVectorBatch : public BytesVectorBatch {
  explicit BinaryVectorBatch(uint64_t capacity,
                             dbcommon::MemoryPool& pool)  // NOLINT
      : BytesVectorBatch(capacity, pool) {}
  ORCTypeKind getType() override { return ORCTypeKind::BINARY; }
  std::unique_ptr<dbcommon::Vector> buildVector() override {
    return dbcommon::Vector::BuildVector(dbcommon::TypeKind::BINARYID,
                                         hasStats);
  }
};

struct StructVectorBatch : public ColumnVectorBatch {
  explicit StructVectorBatch(uint64_t capacity,
                             dbcommon::MemoryPool& pool);  // NOLINT
  virtual ~StructVectorBatch();
  std::string toString() const override;
  void resize(uint64_t capacity) override;
  uint64_t getMemoryUsage() override;
  bool hasVariableLength() override;

  std::vector<ColumnVectorBatch*> fields;

  ORCTypeKind getType() override { return ORCTypeKind::STRUCT; }

  char* getData() const override {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "not implemented getData for StructVectorBatch");
  }

  uint32_t getWidth() override {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "not implemented getWidth for StructVectorBatch");
  }

  std::unique_ptr<dbcommon::Vector> buildVector() override {
    return dbcommon::Vector::BuildVector(dbcommon::TypeKind::STRUCTID, false);
  }
};

struct ListVectorBatch : public ColumnVectorBatch {
  explicit ListVectorBatch(uint64_t capacity,
                           dbcommon::MemoryPool& pool);  // NOLINT
  virtual ~ListVectorBatch();
  std::string toString() const override;
  void resize(uint64_t capacity) override;
  uint64_t getMemoryUsage() override;
  bool hasVariableLength() override;

  // The offset of the first element of each list.
  // The length of list i is startOffset[i+1] - startOffset[i].
  DataBuffer<int64_t> offsets;

  // the concatenated elements
  std::unique_ptr<ColumnVectorBatch> elements;

  ORCTypeKind getType() override { return ORCTypeKind::LIST; }

  char* getData() const override {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "not implemented getData for ListVectorBatch");
  }

  uint32_t getWidth() override {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "not implemented getWidth for ListVectorBatch");
  }

  std::unique_ptr<dbcommon::Vector> buildVector() override {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "not implemented buildVector for ListVectorBatch");
  }

  std::unique_ptr<dbcommon::Vector> buildVector(ORCTypeKind type) {
    switch (type) {
      case orc::ORCTypeKind::SHORT:
        return dbcommon::Vector::BuildVector(
            dbcommon::TypeKind::SMALLINTARRAYID, hasStats);
        break;

      case orc::ORCTypeKind::INT:
        return dbcommon::Vector::BuildVector(dbcommon::TypeKind::INTARRAYID,
                                             hasStats);
        break;

      case orc::ORCTypeKind::LONG:
        return dbcommon::Vector::BuildVector(dbcommon::TypeKind::BIGINTARRAYID,
                                             hasStats);
        break;

      case orc::ORCTypeKind::FLOAT:
        return dbcommon::Vector::BuildVector(dbcommon::TypeKind::FLOATARRAYID,
                                             hasStats);
        break;

      case orc::ORCTypeKind::DOUBLE:
        return dbcommon::Vector::BuildVector(dbcommon::TypeKind::DOUBLEARRAYID,
                                             hasStats);
        break;

      default:
        LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                  "vector type %d is not supported yet", type);
    }
  }
};

struct MapVectorBatch : public ColumnVectorBatch {
  explicit MapVectorBatch(uint64_t capacity,
                          dbcommon::MemoryPool& pool);  // NOLINT
  virtual ~MapVectorBatch();
  std::string toString() const override;
  void resize(uint64_t capacity) override;
  uint64_t getMemoryUsage() override;
  bool hasVariableLength() override;

  // The offset of the first element of each list.
  // The length of list i is startOffset[i+1] - startOffset[i].
  DataBuffer<int64_t> offsets;

  // the concatenated keys
  std::unique_ptr<ColumnVectorBatch> keys;
  // the concatenated elements
  std::unique_ptr<ColumnVectorBatch> elements;

  ORCTypeKind getType() override { return ORCTypeKind::MAP; }

  char* getData() const override {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "not implemented getData for MapVectorBatch");
  }

  uint32_t getWidth() override {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "not implemented getWidth for MapVectorBatch");
  }

  std::unique_ptr<dbcommon::Vector> buildVector() override {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "not implemented buildVector for MapVectorBatch");
  }
};

struct UnionVectorBatch : public ColumnVectorBatch {
  explicit UnionVectorBatch(uint64_t capacity,
                            dbcommon::MemoryPool& pool);  // NOLINT
  virtual ~UnionVectorBatch();
  std::string toString() const override;
  void resize(uint64_t capacity) override;
  uint64_t getMemoryUsage() override;
  bool hasVariableLength() override;

  // For each value, which element of children has the value.
  DataBuffer<unsigned char> tags;

  // For each value, the index inside of the child ColumnVectorBatch.
  DataBuffer<uint64_t> offsets;

  // the sub-columns
  std::vector<ColumnVectorBatch*> children;

  ORCTypeKind getType() override { return ORCTypeKind::UNION; }

  char* getData() const override {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "not implemented getData for UnionVectorBatch");
  }

  uint32_t getWidth() override {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "not implemented getWidth for UnionVectorBatch");
  }

  std::unique_ptr<dbcommon::Vector> buildVector() override {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "not implemented buildVector for UnionVectorBatch");
  }
};

struct Decimal {
  Decimal(const Int128& value, int32_t scale);
  explicit Decimal(const std::string& value);
  bool operator<(const Decimal& right) const;
  bool operator>(const Decimal& right) const;

  std::string toString() const;
  Int128 value;
  int32_t scale;
};

struct Decimal64VectorBatch : public ColumnVectorBatch {
  explicit Decimal64VectorBatch(uint64_t capacity,
                                dbcommon::MemoryPool& pool);  // NOLINT
  virtual ~Decimal64VectorBatch();
  std::string toString() const override;
  void resize(uint64_t capacity) override;
  uint64_t getMemoryUsage() override;

  // total number of digits
  int32_t precision;
  // the number of places after the decimal
  int32_t scale;

  // the numeric values
  DataBuffer<int64_t> values;
  DataBuffer<int64_t> highbitValues;
  DataBuffer<int64_t> readScales;

  ORCTypeKind getType() override { return ORCTypeKind::DECIMAL; }

  const char* getData() const override {
    return reinterpret_cast<const char*>(values.data());
  }

  const char* getAuxiliaryData() const override {
    return reinterpret_cast<const char*>(highbitValues.data());
  }

  const char* getScaleData() const override {
    return reinterpret_cast<const char*>(readScales.data());
  }

  uint32_t getWidth() override {
    return sizeof(int64_t) + sizeof(int64_t) + sizeof(int64_t);
  }

  bool hasVariableLength() override { return false; }

  std::unique_ptr<dbcommon::Vector> buildVector() override {
    return dbcommon::Vector::BuildVector(dbcommon::TypeKind::DECIMALNEWID,
                                         true);
  }

  std::unique_ptr<dbcommon::Vector> buildVector(
      dbcommon::TypeKind type) override {
    return dbcommon::Vector::BuildVector(dbcommon::TypeKind::DECIMALNEWID,
                                         hasStats);
  }

  std::string toDecimalString(int64_t value, int32_t scale) {
    std::stringstream buffer;
    if (scale == 0) {
      buffer << value;
      return buffer.str();
    }
    std::string sign = "";
    if (value < 0) {
      sign = "-";
      value = -value;
    }
    buffer << value;
    std::string str = buffer.str();
    int32_t len = static_cast<int32_t>(str.length());
    if (len > scale) {
      return sign + str.substr(0, static_cast<size_t>(len - scale)) + "." +
             str.substr(static_cast<size_t>(len - scale),
                        static_cast<size_t>(scale));
    } else if (len == scale) {
      return sign + "0." + str;
    } else {
      std::string result = sign + "0.";
      for (int32_t i = 0; i < scale - len; ++i) {
        result += "0";
      }
      return result + str;
    }
  }

 protected:
  friend class Decimal64ColumnReader;
};

struct Decimal128VectorBatch : public ColumnVectorBatch {
  explicit Decimal128VectorBatch(uint64_t capacity,
                                 dbcommon::MemoryPool& pool);  // NOLINT
  virtual ~Decimal128VectorBatch();
  std::string toString() const override;
  void resize(uint64_t capacity) override;
  uint64_t getMemoryUsage() override;

  // total number of digits
  int32_t precision;
  // the number of places after the decimal
  int32_t scale;

  // the numeric values
  DataBuffer<Int128> values;
  DataBuffer<int64_t> highbitValues;
  DataBuffer<uint64_t> lowbitValues;
  DataBuffer<int64_t> readScales;

  ORCTypeKind getType() override { return ORCTypeKind::DECIMAL; }

  const char* getData() const override {
    return reinterpret_cast<const char*>(lowbitValues.data());
  }

  const char* getAuxiliaryData() const override {
    return reinterpret_cast<const char*>(highbitValues.data());
  }

  const char* getScaleData() const override {
    return reinterpret_cast<const char*>(readScales.data());
  }

  uint32_t getWidth() override {
    return sizeof(uint64_t) + sizeof(int64_t) + sizeof(int64_t);
  }

  bool hasVariableLength() override { return false; }

  std::unique_ptr<dbcommon::Vector> buildVector() override {
    return dbcommon::Vector::BuildVector(dbcommon::TypeKind::DECIMALNEWID,
                                         true);
  }

  std::unique_ptr<dbcommon::Vector> buildVector(
      dbcommon::TypeKind type) override {
    return dbcommon::Vector::BuildVector(dbcommon::TypeKind::DECIMALNEWID,
                                         hasStats);
  }

 protected:
  friend class Decimal128ColumnReader;
  friend class DecimalHive11ColumnReader;
};

struct DateVectorBatch : public FixedSizeVectorBatch<int32_t> {
  explicit DateVectorBatch(uint64_t capacity,
                           dbcommon::MemoryPool& pool)  // NOLINT
      : FixedSizeVectorBatch<int32_t>(capacity, pool) {}
  virtual ~DateVectorBatch() {}

  ORCTypeKind getType() override { return ORCTypeKind::DATE; }

  std::unique_ptr<dbcommon::Vector> buildVector() override {
    return dbcommon::Vector::BuildVector(dbcommon::TypeKind::DATEID, hasStats);
  }
};

struct TimeVectorBatch : public FixedSizeVectorBatch<int64_t> {
  explicit TimeVectorBatch(uint64_t capacity,
                           dbcommon::MemoryPool& pool)  // NOLINT
      : FixedSizeVectorBatch<int64_t>(capacity, pool) {}
  virtual ~TimeVectorBatch() {}

  ORCTypeKind getType() override { return ORCTypeKind::TIME; }

  std::unique_ptr<dbcommon::Vector> buildVector() override {
    return dbcommon::Vector::BuildVector(dbcommon::TypeKind::TIMEID, hasStats);
  }
};

// A column vector batch for storing timestamp values.
// The timestamps are stored split into the time_t value (seconds since
// 1 Jan 1970 00:00:00) and the nanoseconds within the time_t value.
struct TimestampVectorBatch : public ColumnVectorBatch {
  explicit TimestampVectorBatch(uint64_t capacity,
                                dbcommon::MemoryPool& pool);  // NOLINT
  virtual ~TimestampVectorBatch();
  std::string toString() const override;
  void resize(uint64_t capacity) override;
  uint64_t getMemoryUsage() override;

  // the number of seconds past 1 Jan 1970 00:00 UTC (aka time_t)
  DataBuffer<int64_t> data;

  // the nanoseconds of each value
  DataBuffer<int64_t> nanoseconds;

  ORCTypeKind getType() override { return ORCTypeKind::TIMESTAMP; }

  const char* getData() const override {
    return reinterpret_cast<const char*>(data.data());
  }

  const char* getNanoseconds() const override {
    return reinterpret_cast<const char*>(nanoseconds.data());
  }

  uint32_t getWidth() override { return sizeof(int64_t) + sizeof(int64_t); }

  bool hasVariableLength() override { return false; }

  std::unique_ptr<dbcommon::Vector> buildVector() override {
    return dbcommon::Vector::BuildVector(dbcommon::TypeKind::TIMESTAMPID,
                                         hasStats);
  }

  std::unique_ptr<dbcommon::Vector> buildVector(
      dbcommon::TypeKind type) override {
    return dbcommon::Vector::BuildVector(type, hasStats);
  }
};

}  // namespace orc
#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_VECTOR_H_
