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

#include "storage/format/orc/orc-proto-definition.h"
#include "storage/format/orc/seekable-input-stream.h"

namespace orc {

StreamInformation::~StreamInformation() {
  // PASS
}

StripeInformation::~StripeInformation() {
  // PASS
}

void StripeInformationImpl::ensureStripeFooterLoaded() const {
  if (stripeFooter.get() == nullptr) {
    std::unique_ptr<SeekableInputStream> pbStream = createDecompressor(
        compression,
        std::unique_ptr<SeekableInputStream>(new SeekableFileInputStream(
            stream, offset + indexLength + dataLength, footerLength,
            memoryPool)),
        blockSize, memoryPool);
    stripeFooter.reset(new proto::StripeFooter());
    if (!stripeFooter->ParseFromZeroCopyStream(pbStream.get())) {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Failed to parse the stripe footer");
    }
  }
}

std::unique_ptr<StreamInformation> StripeInformationImpl::getStreamInformation(
    uint64_t streamId) const {
  ensureStripeFooterLoaded();
  uint64_t streamOffset = offset;
  for (uint64_t s = 0; s < streamId; ++s) {
    streamOffset += stripeFooter->streams(static_cast<int>(s)).length();
  }
  return std::unique_ptr<StreamInformation>(new StreamInformationImpl(
      streamOffset, stripeFooter->streams(static_cast<int>(streamId))));
}

StatisticsImpl::~StatisticsImpl() {
  for (std::list<univplan::ColumnStatistics*>::iterator ptr = colStats.begin();
       ptr != colStats.end(); ++ptr) {
    delete *ptr;
  }
}

univplan::ColumnStatistics* convertColumnStatistics(
    const proto::ColumnStatistics& s, bool correctStats) {
  if (s.has_intstatistics()) {
    return new IntegerColumnStatisticsImpl(s);
  } else if (s.has_doublestatistics()) {
    return new DoubleColumnStatisticsImpl(s);
  } else if (s.has_stringstatistics()) {
    return new StringColumnStatisticsImpl(s, correctStats);
  } else if (s.has_bucketstatistics()) {
    return new BooleanColumnStatisticsImpl(s, correctStats);
  } else if (s.has_decimalstatistics()) {
    return new DecimalColumnStatisticsImpl(s, correctStats);
  } else if (s.has_timestampstatistics()) {
    return new TimestampColumnStatisticsImpl(s, correctStats);
  } else if (s.has_datestatistics()) {
    return new DateColumnStatisticsImpl(s, correctStats);
  } else if (s.has_binarystatistics()) {
    return new BinaryColumnStatisticsImpl(s, correctStats);
  } else {
    return new ColumnStatisticsImpl(s);
  }
}

// TODO(zhenglin): to complete other types
std::unique_ptr<ColumnStatisticsImpl> createColumnStatistics(
    const orc::Type* type) {
  switch (type->getKind()) {
    case orc::ORCTypeKind::BYTE:
    case orc::ORCTypeKind::SHORT:
    case orc::ORCTypeKind::INT:
    case orc::ORCTypeKind::LONG:
    case orc::ORCTypeKind::TIME:
      return std::unique_ptr<ColumnStatisticsImpl>(
          new IntegerColumnStatisticsImpl());
    case orc::ORCTypeKind::FLOAT:
    case orc::ORCTypeKind::DOUBLE:
      return std::unique_ptr<ColumnStatisticsImpl>(
          new DoubleColumnStatisticsImpl());
    case orc::ORCTypeKind::STRING:
    case orc::ORCTypeKind::VARCHAR:
    case orc::ORCTypeKind::CHAR:
      return std::unique_ptr<ColumnStatisticsImpl>(
          new StringColumnStatisticsImpl());
    case orc::ORCTypeKind::BOOLEAN:
      return std::unique_ptr<ColumnStatisticsImpl>(
          new BooleanColumnStatisticsImpl());
    case orc::ORCTypeKind::BINARY:
      return std::unique_ptr<ColumnStatisticsImpl>(
          new BinaryColumnStatisticsImpl());
    case orc::ORCTypeKind::DATE:
      return std::unique_ptr<ColumnStatisticsImpl>(
          new DateColumnStatisticsImpl());
    case orc::ORCTypeKind::TIMESTAMP:
      return std::unique_ptr<ColumnStatisticsImpl>(
          new TimestampColumnStatisticsImpl());
    case orc::ORCTypeKind::DECIMAL:
      return std::unique_ptr<ColumnStatisticsImpl>(
          new DecimalColumnStatisticsImpl());
    default:
      return std::unique_ptr<ColumnStatisticsImpl>(new ColumnStatisticsImpl());
  }
}

DateColumnStatisticsImpl::~DateColumnStatisticsImpl() {
  // PASS
}

DecimalColumnStatisticsImpl::~DecimalColumnStatisticsImpl() {
  // PASS
}

TimestampColumnStatisticsImpl::~TimestampColumnStatisticsImpl() {
  // PASS
}

DateColumnStatisticsImpl::DateColumnStatisticsImpl(
    const proto::ColumnStatistics& pb, bool correctStats) {
  valueCount = pb.numberofvalues();
  hasNullValue = pb.hasnull();
  if (!pb.has_datestatistics() || !correctStats) {
    _hasStats = false;

    minimum = 0;
    maximum = 0;
  } else {
    _hasStats = pb.datestatistics().has_minimum();
    minimum = pb.datestatistics().minimum();
    maximum = pb.datestatistics().maximum();
  }
}

DecimalColumnStatisticsImpl::DecimalColumnStatisticsImpl(
    const proto::ColumnStatistics& pb, bool correctStats) {
  valueCount = pb.numberofvalues();
  hasNullValue = pb.hasnull();
  if (!pb.has_decimalstatistics() || !correctStats) {
    _hasMinimum = false;
    _hasMaximum = false;
    _hasSum = false;
  } else {
    const proto::DecimalStatistics& stats = pb.decimalstatistics();
    _hasMinimum = stats.has_minimum();
    _hasMaximum = stats.has_maximum();
    _hasSum = stats.has_sum();

    minimum = stats.minimum();
    maximum = stats.maximum();
    sum = stats.sum();
  }
}

void DecimalColumnStatisticsImpl::updateSum(Decimal value) {
  bool overflow = false;
  Decimal currentSum = this->getSum();

  if (currentSum.scale > value.scale) {
    value.value = scaleUpInt128ByPowerOfTen(
        value.value, currentSum.scale - value.scale, overflow);
  } else if (currentSum.scale < value.scale) {
    currentSum.value = scaleUpInt128ByPowerOfTen(
        currentSum.value, value.scale - currentSum.scale, overflow);
    currentSum.scale = value.scale;
  }

  if (!overflow) {
    bool wasPositive = currentSum.value >= 0;
    currentSum.value += value.value;
    if ((value.value >= 0) == wasPositive) {
      _hasSum = ((currentSum.value >= 0) == wasPositive);
    }
  } else {
    _hasSum = false;
  }

  if (_hasSum) {
    sum = currentSum.toString();
  }
}

TimestampColumnStatisticsImpl::TimestampColumnStatisticsImpl(
    const proto::ColumnStatistics& pb, bool correctStats) {
  valueCount = pb.numberofvalues();
  hasNullValue = pb.hasnull();
  if (!pb.has_timestampstatistics() || !correctStats) {
    _hasStats = false;
    minimum = 0;
    maximum = 0;
  } else {
    const proto::TimestampStatistics& stats = pb.timestampstatistics();
    _hasStats = stats.has_minimum();

    minimum = stats.minimum();
    maximum = stats.maximum();
  }
}
}  // namespace orc
