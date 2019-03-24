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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_ORC_PROTO_DEFINITION_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_ORC_PROTO_DEFINITION_H_

#include <list>
#include <memory>
#include <sstream>
#include <string>

#include "univplan/common/statistics.h"

#include "storage/format/orc/input-stream.h"
#include "storage/format/orc/type-impl.h"
#include "storage/format/orc/vector.h"

namespace orc {

static const uint64_t ORC_COMPRESSION_BLOCK_SIZE = 256 * 1024;  // 256K

enum WriterId {
  ORC_JAVA_WRITER = 0,
  ORC_CPP_WRITER = 1,
  PRESTO_WRITER = 2,
  UNKNOWN_WRITER = INT32_MAX
};

enum CompressionKind {
  CompressionKind_NONE = 0,
  CompressionKind_ZLIB = 1,
  CompressionKind_SNAPPY = 2,
  CompressionKind_LZO = 3,
  CompressionKind_LZ4 = 4,
  CompressionKind_ZSTD = 5,
  CompressionKind_MAX = INT64_MAX
};

enum WriterVersion {
  WriterVersion_ORIGINAL = 0,
  WriterVersion_HIVE_8732 = 1,
  WriterVersion_HIVE_4243 = 2,
  WriterVersion_HIVE_12055 = 3,
  WriterVersion_HIVE_13083 = 4,
  WriterVersion_ORC_101 = 5,
  WriterVersion_ORC_135 = 6,
  WriterVersion_MAX = INT64_MAX
};

enum StreamKind {
  StreamKind_PRESENT = 0,
  StreamKind_DATA = 1,
  StreamKind_LENGTH = 2,
  StreamKind_DICTIONARY_DATA = 3,
  StreamKind_DICTIONARY_COUNT = 4,
  StreamKind_SECONDARY = 5,
  StreamKind_ROW_INDEX = 6,
  StreamKind_BLOOM_FILTER = 7
};

class ColumnStatisticsImpl : public univplan::ColumnStatistics {
 public:
  ColumnStatisticsImpl() { reset(); }
  explicit ColumnStatisticsImpl(const proto::ColumnStatistics& stats) {
    if (stats.has_numberofvalues())
      valueCount = stats.numberofvalues();
    else
      valueCount = 0;
    if (stats.has_hasnull())
      hasNullValue = stats.hasnull();
    else
      hasNullValue = true;
  }
  virtual ~ColumnStatisticsImpl() {}

  virtual void serialize(proto::ColumnStatistics* pb) {
    assert(pb != nullptr);
    pb->set_numberofvalues(valueCount);
    pb->set_hasnull(hasNullValue);
  }
};

class IntegerColumnStatisticsImpl : public ColumnStatisticsImpl {
 private:
  bool _hasStats;
  int64_t minimum;
  int64_t maximum;
  int64_t sum;

 public:
  IntegerColumnStatisticsImpl() : ColumnStatisticsImpl() { resetInternal(); }
  explicit IntegerColumnStatisticsImpl(const proto::ColumnStatistics& stats)
      : ColumnStatisticsImpl(stats) {
    if (!stats.has_intstatistics()) {
      resetInternal();
    } else {
      const proto::IntegerStatistics& s = stats.intstatistics();
      _hasStats = s.has_minimum();
      if (_hasStats) {
        minimum = s.minimum();
        maximum = s.maximum();
        sum = s.sum();
      }
    }
  }
  virtual ~IntegerColumnStatisticsImpl() {}

  bool hasMinimum() const { return _hasStats; }

  bool hasMaximum() const { return _hasStats; }

  bool hasSum() const { return _hasStats; }

  int64_t getMinimum() const {
    if (_hasStats) {
      return minimum;
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Minimum is not defined.");
    }
  }

  int64_t getMaximum() const {
    if (_hasStats) {
      return maximum;
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Maximum is not defined.");
    }
  }

  int64_t getSum() const {
    if (_hasStats) {
      return sum;
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Sum is not defined.");
    }
  }

  void updateInteger(int64_t value) {
    if (!_hasStats) {
      _hasStats = true;
      minimum = value;
      maximum = value;
    } else if (value < minimum) {
      minimum = value;
    } else if (value > maximum) {
      maximum = value;
    }
    sum += value;
  }

  void merge(const ColumnStatistics& stats) override {
    ColumnStatisticsImpl::merge(stats);
    const IntegerColumnStatisticsImpl* other =
        dynamic_cast<const IntegerColumnStatisticsImpl*>(&stats);
    assert(other != nullptr);
    if (other->hasMinimum()) {
      if (!_hasStats) {
        _hasStats = other->hasMinimum();
        minimum = other->getMinimum();
        maximum = other->getMaximum();
        sum = other->getSum();
      } else {
        if (other->getMinimum() < minimum) {
          minimum = other->getMinimum();
        }
        if (other->getMaximum() > maximum) {
          maximum = other->getMaximum();
        }
        sum += other->getSum();
      }
    }
  }

  void serialize(proto::ColumnStatistics* pb) override {
    assert(pb != nullptr);
    ColumnStatisticsImpl::serialize(pb);
    proto::IntegerStatistics* stats = pb->mutable_intstatistics();
    if (_hasStats) {
      stats->set_minimum(minimum);
      stats->set_maximum(maximum);
      stats->set_sum(sum);
    }
  }

  void reset() override {
    ColumnStatisticsImpl::reset();
    resetInternal();
  }

 private:
  void resetInternal() {
    _hasStats = false;
    minimum = 0;
    maximum = 0;
    sum = 0;
  }
};

class DoubleColumnStatisticsImpl : public ColumnStatisticsImpl {
 private:
  bool _hasStats;
  double minimum;
  double maximum;
  double sum;

 public:
  DoubleColumnStatisticsImpl() : ColumnStatisticsImpl() { resetInternal(); }
  explicit DoubleColumnStatisticsImpl(const proto::ColumnStatistics& stats)
      : ColumnStatisticsImpl(stats) {
    if (!stats.has_doublestatistics()) {
      resetInternal();
    } else {
      const proto::DoubleStatistics& s = stats.doublestatistics();
      _hasStats = s.has_minimum();
      if (_hasStats) {
        minimum = s.minimum();
        maximum = s.maximum();
        sum = s.sum();
      }
    }
  }
  virtual ~DoubleColumnStatisticsImpl() {}

  bool hasMinimum() const { return _hasStats; }

  bool hasMaximum() const { return _hasStats; }

  bool hasSum() const { return _hasStats; }

  double getMinimum() const {
    if (_hasStats) {
      return minimum;
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Minimum is not defined.");
    }
  }

  double getMaximum() const {
    if (_hasStats) {
      return maximum;
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Maximum is not defined.");
    }
  }

  double getSum() const {
    if (_hasStats) {
      return sum;
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Sum is not defined.");
    }
  }

  void updateDouble(double value) {
    if (!_hasStats) {
      _hasStats = true;
      minimum = value;
      maximum = value;
    } else if (value < minimum) {
      minimum = value;
    } else if (value > maximum) {
      maximum = value;
    }
    sum += value;
  }

  void merge(const ColumnStatistics& stats) override {
    ColumnStatisticsImpl::merge(stats);
    const DoubleColumnStatisticsImpl* other =
        dynamic_cast<const DoubleColumnStatisticsImpl*>(&stats);
    assert(other != nullptr);
    if (other->hasMinimum()) {
      if (!_hasStats) {
        _hasStats = other->hasMinimum();
        minimum = other->getMinimum();
        maximum = other->getMaximum();
        sum = other->getSum();
      } else {
        if (other->getMinimum() < minimum) {
          minimum = other->getMinimum();
        }
        if (other->getMaximum() > maximum) {
          maximum = other->getMaximum();
        }
        sum += other->getSum();
      }
    }
  }

  void serialize(proto::ColumnStatistics* pb) override {
    assert(pb != nullptr);
    ColumnStatisticsImpl::serialize(pb);
    proto::DoubleStatistics* stats = pb->mutable_doublestatistics();
    if (_hasStats) {
      stats->set_minimum(minimum);
      stats->set_maximum(maximum);
      stats->set_sum(sum);
    }
  }

  void reset() override {
    ColumnStatisticsImpl::reset();
    resetInternal();
  }

 private:
  void resetInternal() {
    _hasStats = false;
    minimum = 0;
    maximum = 0;
    sum = 0;
  }
};

class StringColumnStatisticsImpl : public ColumnStatisticsImpl {
 private:
  bool _hasStats;
  std::string minimum;
  std::string maximum;
  int64_t totalLength;

 public:
  StringColumnStatisticsImpl() : ColumnStatisticsImpl() { resetInternal(); }
  StringColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                             bool correctStats)
      : ColumnStatisticsImpl(stats) {
    if (!stats.has_stringstatistics() || !correctStats) {
      resetInternal();
    } else {
      const proto::StringStatistics& s = stats.stringstatistics();
      _hasStats = s.has_minimum();
      if (_hasStats) {
        minimum = s.minimum();
        maximum = s.maximum();
        totalLength = s.sum();
      }
    }
  }
  virtual ~StringColumnStatisticsImpl() {}

  bool hasMinimum() const { return _hasStats; }

  bool hasMaximum() const { return _hasStats; }

  bool hasTotalLength() const { return _hasStats; }

  const char* getMinimum() const {
    if (_hasStats) {
      return minimum.c_str();
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Minimum is not defined.");
    }
  }

  const char* getMaximum() const {
    if (_hasStats) {
      return maximum.c_str();
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Maximum is not defined.");
    }
  }

  int64_t getTotalLength() const {
    if (_hasStats) {
      return totalLength;
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Total length is not defined.");
    }
  }

  void updateString(const char* buffer, uint64_t len) {
    std::string text(buffer, len);
    if (!_hasStats) {
      _hasStats = true;
      maximum = minimum = text;
    } else if (minimum > text) {
      minimum = text;
    } else if (maximum < text) {
      maximum = text;
    }
    totalLength += len;
  }

  void merge(const ColumnStatistics& stats) override {
    ColumnStatisticsImpl::merge(stats);
    const StringColumnStatisticsImpl* other =
        dynamic_cast<const StringColumnStatisticsImpl*>(&stats);
    assert(other != nullptr);
    if (other->hasMinimum()) {
      if (!_hasStats) {
        _hasStats = other->hasMinimum();
        minimum = other->getMinimum();
        maximum = other->getMaximum();
        totalLength = other->getTotalLength();
      } else {
        if (other->getMinimum() < minimum) {
          minimum = other->getMinimum();
        }
        if (other->getMaximum() > maximum) {
          maximum = other->getMaximum();
        }
        totalLength += other->getTotalLength();
      }
    }
  }

  void serialize(proto::ColumnStatistics* pb) override {
    assert(pb != nullptr);
    ColumnStatisticsImpl::serialize(pb);
    proto::StringStatistics* stats = pb->mutable_stringstatistics();
    if (_hasStats) {
      stats->set_minimum(minimum);
      stats->set_maximum(maximum);
      stats->set_sum(totalLength);
    }
  }

  void reset() override {
    ColumnStatisticsImpl::reset();
    resetInternal();
  }

 private:
  void resetInternal() {
    _hasStats = false;
    minimum.clear();
    maximum.clear();
    totalLength = 0;
  }
};

class StreamInformation {
 public:
  virtual ~StreamInformation();

  virtual StreamKind getKind() const = 0;
  virtual uint64_t getColumnId() const = 0;
  virtual uint64_t getOffset() const = 0;
  virtual uint64_t getLength() const = 0;
};

class StripeInformation {
 public:
  virtual ~StripeInformation();

  // Get the byte offset of the start of the stripe.
  // @return the bytes from the start of the file
  virtual uint64_t getOffset() const = 0;

  // Get the total length of the stripe in bytes.
  // @return the number of bytes in the stripe
  virtual uint64_t getLength() const = 0;

  // Get the length of the stripe's indexes.
  // @return the number of bytes in the index
  virtual uint64_t getIndexLength() const = 0;

  // Get the length of the stripe's data.
  // @return the number of bytes in the stripe
  virtual uint64_t getDataLength() const = 0;

  // Get the length of the stripe's tail section, which contains its index.
  // @return the number of bytes in the tail
  virtual uint64_t getFooterLength() const = 0;

  // Get the number of rows in the stripe.
  // @return a count of the number of rows
  virtual uint64_t getNumberOfRows() const = 0;

  // Get the number of streams in the stripe.
  virtual uint64_t getNumberOfStreams() const = 0;

  // Get the StreamInformation for the given stream.
  virtual std::unique_ptr<StreamInformation> getStreamInformation(
      uint64_t streamId) const = 0;

  // Get the dictionary size.
  // @param colId the columnId
  // @return the size of the dictionary or 0 if there isn't one
  virtual uint64_t getDictionarySize(uint64_t colId) const = 0;

  // Get the writer timezone.
  virtual const std::string& getWriterTimezone() const = 0;
};

class BinaryColumnStatisticsImpl : public ColumnStatisticsImpl {
 public:
  BinaryColumnStatisticsImpl() { resetInternal(); }
  BinaryColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                             bool correctStats)
      : ColumnStatisticsImpl(stats) {
    if (!stats.has_binarystatistics() || !correctStats) {
      resetInternal();
    } else {
      const proto::BinaryStatistics& s = stats.binarystatistics();
      _hasStats = s.has_sum();
      if (_hasStats) {
        totalLength = s.sum();
      }
    }
  }
  virtual ~BinaryColumnStatisticsImpl() {}

  bool hasTotalLength() const { return _hasStats; }

  uint64_t getTotalLength() const {
    if (_hasStats) {
      return totalLength;
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Total length is not defined.");
    }
  }

  void update(size_t length) {
    _hasStats = true;
    totalLength += length;
  }

  void merge(const ColumnStatistics& stats) override {
    ColumnStatisticsImpl::merge(stats);
    const BinaryColumnStatisticsImpl* other =
        dynamic_cast<const BinaryColumnStatisticsImpl*>(&stats);
    assert(other != nullptr);
    if (other->hasTotalLength()) {
      if (!_hasStats) {
        _hasStats = other->hasTotalLength();
        totalLength = other->getTotalLength();
      } else {
        totalLength += other->getTotalLength();
      }
    }
  }

  void serialize(proto::ColumnStatistics* pb) override {
    assert(pb != nullptr);
    ColumnStatisticsImpl::serialize(pb);

    proto::BinaryStatistics* binStats = pb->mutable_binarystatistics();
    if (_hasStats) {
      binStats->set_sum(totalLength);
    }
  }

 private:
  void resetInternal() {
    ColumnStatisticsImpl::reset();
    _hasStats = false;
    totalLength = 0;
  }

  bool _hasStats;
  int64_t totalLength;
};

class BooleanColumnStatisticsImpl : public ColumnStatisticsImpl {
 private:
  bool _hasCount;
  uint64_t trueCount;

 public:
  BooleanColumnStatisticsImpl() : ColumnStatisticsImpl() { resetInternal(); }
  BooleanColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                              bool correctStats)
      : ColumnStatisticsImpl(stats) {
    if (!stats.has_bucketstatistics() || !correctStats ||
        stats.bucketstatistics().count_size() == 0) {
      resetInternal();
    } else {
      _hasCount = true;
      trueCount = stats.bucketstatistics().count(0);
    }
  }
  virtual ~BooleanColumnStatisticsImpl() {}

  bool hasCount() const { return _hasCount; }

  uint64_t getFalseCount() const {
    if (_hasCount) {
      return valueCount - trueCount;
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "False count is not defined.");
    }
  }

  uint64_t getTrueCount() const {
    if (_hasCount) {
      return trueCount;
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "True count is not defined.");
    }
  }

  void updateBoolean(bool value) {
    if (!_hasCount) _hasCount = true;
    if (value) trueCount += 1;
  }

  void merge(const ColumnStatistics& stats) override {
    ColumnStatisticsImpl::merge(stats);
    const BooleanColumnStatisticsImpl* other =
        dynamic_cast<const BooleanColumnStatisticsImpl*>(&stats);
    assert(other != nullptr);
    if (other->hasCount()) {
      if (!_hasCount) {
        _hasCount = true;
        trueCount = other->trueCount;
      } else {
        trueCount += other->trueCount;
      }
    }
  }

  void serialize(proto::ColumnStatistics* pb) override {
    assert(pb != nullptr);
    ColumnStatisticsImpl::serialize(pb);
    proto::BucketStatistics* stats = pb->mutable_bucketstatistics();
    if (_hasCount) stats->add_count(trueCount);
  }

  void reset() override {
    ColumnStatisticsImpl::reset();
    resetInternal();
  }

 private:
  void resetInternal() {
    _hasCount = false;
    trueCount = 0;
  }
};

class DateColumnStatisticsImpl : public ColumnStatisticsImpl {
 private:
  bool _hasStats;
  int32_t minimum;
  int32_t maximum;

 public:
  DateColumnStatisticsImpl() : ColumnStatisticsImpl() { resetInternal(); }
  DateColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                           bool correctStats);
  virtual ~DateColumnStatisticsImpl();

  bool hasMinimum() const { return _hasStats; }

  bool hasMaximum() const { return _hasStats; }

  int32_t getMinimum() const {
    if (_hasStats) {
      return minimum;
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Minimum is not defined.");
    }
  }

  int32_t getMaximum() const {
    if (_hasStats) {
      return maximum;
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Maximum is not defined.");
    }
  }

  void updateDate(int64_t value) {
    if (!_hasStats) {
      _hasStats = true;
      minimum = value;
      maximum = value;
    } else if (value < minimum) {
      minimum = value;
    } else if (value > maximum) {
      maximum = value;
    }
  }

  void merge(const ColumnStatistics& stats) override {
    ColumnStatisticsImpl::merge(stats);
    const DateColumnStatisticsImpl* other =
        dynamic_cast<const DateColumnStatisticsImpl*>(&stats);
    assert(other != nullptr);
    if (other->hasMinimum()) {
      if (!_hasStats) {
        _hasStats = other->hasMinimum();
        minimum = other->getMinimum();
        maximum = other->getMaximum();
      } else {
        if (other->getMinimum() < minimum) {
          minimum = other->getMinimum();
        }
        if (other->getMaximum() > maximum) {
          maximum = other->getMaximum();
        }
      }
    }
  }

  void serialize(proto::ColumnStatistics* pb) override {
    assert(pb != nullptr);
    ColumnStatisticsImpl::serialize(pb);
    proto::DateStatistics* stats = pb->mutable_datestatistics();
    if (_hasStats) {
      stats->set_minimum(minimum);
      stats->set_maximum(maximum);
    }
  }

  void reset() override {
    ColumnStatisticsImpl::reset();
    resetInternal();
  }

 private:
  void resetInternal() {
    _hasStats = false;
    minimum = 0;
    maximum = 0;
  }
};

class DecimalColumnStatisticsImpl : public ColumnStatisticsImpl {
 private:
  bool _hasMinimum;
  bool _hasMaximum;
  bool _hasSum;
  std::string minimum;
  std::string maximum;
  std::string sum;

 public:
  DecimalColumnStatisticsImpl() : ColumnStatisticsImpl() { resetInternal(); }
  DecimalColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                              bool correctStats);
  virtual ~DecimalColumnStatisticsImpl();

  bool hasMinimum() const { return _hasMinimum; }

  bool hasMaximum() const { return _hasMaximum; }

  bool hasSum() const { return _hasSum; }

  Decimal getMinimum() const {
    if (_hasMinimum) {
      return Decimal(minimum);
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Minimum is not defined.");
    }
  }

  Decimal getMaximum() const {
    if (_hasMaximum) {
      return Decimal(maximum);
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Maximum is not defined.");
    }
  }

  Decimal getSum() const {
    if (_hasSum) {
      return Decimal(sum);
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Sum is not defined.");
    }
  }

  const char* getMinimumStr() const {
    if (_hasMinimum) {
      return minimum.c_str();
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Minimum is not defined.");
    }
  }

  const char* getMaximumStr() const {
    if (_hasMaximum) {
      return maximum.c_str();
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Maximum is not defined.");
    }
  }

  const char* getSumStr() const {
    if (_hasSum) {
      return sum.c_str();
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Sum is not defined.");
    }
  }

  void updateDecimal(const orc::Decimal& value) {
    if (!_hasMinimum) {
      _hasMinimum = true;
      _hasMaximum = true;
      minimum = value.toString();
      maximum = value.toString();
    } else if (value < this->getMinimum()) {
      minimum = value.toString();
    } else if (value > this->getMaximum()) {
      maximum = value.toString();
    }
    if (_hasSum) {
      updateSum(value);
    } else {
      _hasSum = true;
      sum = value.toString();
    }
  }

  void merge(const ColumnStatistics& stats) override {
    ColumnStatisticsImpl::merge(stats);
    const DecimalColumnStatisticsImpl* other =
        dynamic_cast<const DecimalColumnStatisticsImpl*>(&stats);
    assert(other != nullptr);
    if (other->hasMinimum()) {
      if (!_hasMinimum) {
        _hasMinimum = true;
        _hasMaximum = true;
        minimum = other->getMinimum().toString();
        maximum = other->getMaximum().toString();
      } else {
        if (other->getMinimum() < this->getMinimum()) {
          minimum = other->getMinimum().toString();
        }
        if (other->getMaximum() > this->getMaximum()) {
          maximum = other->getMaximum().toString();
        }
      }
    }
    if (other->hasSum()) {
      if (_hasSum) {
        Decimal otherSum = other->getSum();
        updateSum(otherSum);
      } else {
        _hasSum = true;
        sum = other->getSum().toString();
      }
    }
  }

  void serialize(proto::ColumnStatistics* pb) override {
    assert(pb != nullptr);
    ColumnStatisticsImpl::serialize(pb);
    proto::DecimalStatistics* stats = pb->mutable_decimalstatistics();
    if (_hasMinimum) {
      stats->set_minimum(minimum);
      stats->set_maximum(maximum);
    }
    if (_hasSum) stats->set_sum(sum);
  }

  void reset() override {
    ColumnStatisticsImpl::reset();
    resetInternal();
  }

 private:
  void updateSum(Decimal value);
  void resetInternal() {
    _hasMaximum = false;
    _hasMinimum = false;
    _hasSum = false;
    minimum.clear();
    maximum.clear();
    sum.clear();
  }
};

class TimestampColumnStatisticsImpl : public ColumnStatisticsImpl {
 private:
  bool _hasStats;
  int64_t minimum;
  int64_t maximum;

 public:
  TimestampColumnStatisticsImpl() : ColumnStatisticsImpl() { resetInternal(); }
  TimestampColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                                bool correctStats);

  virtual ~TimestampColumnStatisticsImpl();

  bool hasMinimum() const { return _hasStats; }

  bool hasMaximum() const { return _hasStats; }

  int64_t getMinimum() const {
    if (_hasStats) {
      return minimum;
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Minimum is not defined.");
    }
  }

  int64_t getMaximum() const {
    if (_hasStats) {
      return maximum;
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Maximum is not defined.");
    }
  }

  void updateTimestamp(const int64_t val) {
    if (!_hasStats) {
      _hasStats = true;
      maximum = minimum = val;
    } else if (minimum > val) {
      minimum = val;
    } else if (maximum < val) {
      maximum = val;
    }
  }

  void merge(const ColumnStatistics& stats) override {
    ColumnStatisticsImpl::merge(stats);
    const TimestampColumnStatisticsImpl* other =
        dynamic_cast<const TimestampColumnStatisticsImpl*>(&stats);
    assert(other != nullptr);
    if (other->hasMinimum()) {
      if (!_hasStats) {
        _hasStats = other->hasMinimum();
        minimum = other->getMinimum();
        maximum = other->getMaximum();
      } else {
        if (other->getMinimum() < minimum) {
          minimum = other->getMinimum();
        }
        if (other->getMaximum() > maximum) {
          maximum = other->getMaximum();
        }
      }
    }
  }

  void serialize(proto::ColumnStatistics* pb) override {
    assert(pb != nullptr);
    ColumnStatisticsImpl::serialize(pb);
    proto::TimestampStatistics* stats = pb->mutable_timestampstatistics();
    if (_hasStats) {
      stats->set_minimum(minimum);
      stats->set_maximum(maximum);
    }
  }

  void reset() override {
    ColumnStatisticsImpl::reset();
    resetInternal();
  }

 private:
  void resetInternal() {
    _hasStats = false;
    minimum = 0;
    maximum = 0;
  }

  int8_t compare(dbcommon::Timestamp ts1, dbcommon::Timestamp ts2) {
    const int64_t val1 = ts1.second;
    const int64_t nano1 = ts1.nanosecond;
    const int64_t val2 = ts2.second;
    const int64_t nano2 = ts2.nanosecond;
    if (val1 == val2) {
      if (nano1 > nano2)
        return 1;
      else if (nano1 == nano2)
        return 0;
      else
        return -1;
    } else if (val1 > val2) {
      return 1;
    } else {
      return -1;
    }
  }
};

class StreamInformationImpl : public StreamInformation {
 private:
  StreamKind kind;
  uint64_t column;
  uint64_t offset;
  uint64_t length;

 public:
  StreamInformationImpl(uint64_t _offset, const proto::Stream& stream)
      : kind(static_cast<StreamKind>(stream.kind())),
        column(stream.column()),
        offset(_offset),
        length(stream.length()) {
    // PASS
  }

  ~StreamInformationImpl() {}

  StreamKind getKind() const override { return kind; }

  uint64_t getColumnId() const override { return column; }

  uint64_t getOffset() const override { return offset; }

  uint64_t getLength() const override { return length; }
};

class StripeInformationImpl : public StripeInformation {
  uint64_t offset;
  uint64_t indexLength;
  uint64_t dataLength;
  uint64_t footerLength;
  uint64_t numRows;
  InputStream* stream;
  dbcommon::MemoryPool& memoryPool;
  CompressionKind compression;
  uint64_t blockSize;
  mutable std::unique_ptr<proto::StripeFooter> stripeFooter;
  void ensureStripeFooterLoaded() const;

 public:
  StripeInformationImpl(uint64_t _offset, uint64_t _indexLength,
                        uint64_t _dataLength, uint64_t _footerLength,
                        uint64_t _numRows, InputStream* _stream,
                        dbcommon::MemoryPool& pool,  // NOLINT
                        CompressionKind _compression, uint64_t _blockSize)
      : offset(_offset),
        indexLength(_indexLength),
        dataLength(_dataLength),
        footerLength(_footerLength),
        numRows(_numRows),
        stream(_stream),
        memoryPool(pool),
        compression(_compression),
        blockSize(_blockSize) {
    // PASS
  }

  virtual ~StripeInformationImpl() {
    // PASS
  }

  uint64_t getOffset() const override { return offset; }

  uint64_t getLength() const override {
    return indexLength + dataLength + footerLength;
  }
  uint64_t getIndexLength() const override { return indexLength; }

  uint64_t getDataLength() const override { return dataLength; }

  uint64_t getFooterLength() const override { return footerLength; }

  uint64_t getNumberOfRows() const override { return numRows; }

  uint64_t getNumberOfStreams() const override {
    ensureStripeFooterLoaded();
    return static_cast<uint64_t>(stripeFooter->streams_size());
  }

  std::unique_ptr<StreamInformation> getStreamInformation(
      uint64_t streamId) const override;

  uint64_t getDictionarySize(uint64_t colId) const override {
    ensureStripeFooterLoaded();
    return static_cast<uint64_t>(
        stripeFooter->columns(static_cast<int>(colId)).dictionarysize());
  }

  const std::string& getWriterTimezone() const override {
    ensureStripeFooterLoaded();
    return stripeFooter->writertimezone();
  }
};

univplan::ColumnStatistics* convertColumnStatistics(
    const proto::ColumnStatistics& s, bool correctStats);
std::unique_ptr<ColumnStatisticsImpl> createColumnStatistics(
    const orc::Type* type);

class StatisticsImpl : public univplan::Statistics {
 private:
  std::list<univplan::ColumnStatistics*> colStats;

  // DELIBERATELY NOT IMPLEMENTED
  StatisticsImpl(const StatisticsImpl&);
  StatisticsImpl& operator=(const StatisticsImpl&);

 public:
  StatisticsImpl(const proto::StripeStatistics& stripeStats,
                 bool correctStats) {
    for (int i = 0; i < stripeStats.colstats_size(); i++) {
      colStats.push_back(
          convertColumnStatistics(stripeStats.colstats(i), correctStats));
    }
  }

  StatisticsImpl(const proto::Footer& footer, bool correctStats) {
    for (int i = 0; i < footer.statistics_size(); i++) {
      colStats.push_back(
          convertColumnStatistics(footer.statistics(i), correctStats));
    }
  }

  const univplan::ColumnStatistics* getColumnStatistics(
      uint32_t columnId) const override {
    std::list<univplan::ColumnStatistics*>::const_iterator it =
        colStats.begin();
    std::advance(it, static_cast<int64_t>(columnId));
    return *it;
  }

  virtual ~StatisticsImpl();

  uint32_t getNumberOfColumns() const override {
    return static_cast<uint32_t>(colStats.size());
  }
};

}  // namespace orc

#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_ORC_PROTO_DEFINITION_H_
