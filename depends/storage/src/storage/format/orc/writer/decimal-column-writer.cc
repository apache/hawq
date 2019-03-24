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

#include "storage/format/orc/writer.h"

#include "dbcommon/common/vector/decimal-vector.h"
namespace orc {
Decimal64ColumnWriter::Decimal64ColumnWriter(const orc::Type *type,
                                             WriterOptions *options)
    : ColumnWriter(type, options) {
  dataBufferedStream = createBlockCompressor(options->getCompressionKind());
  scaleRleCoder =
      createRleCoder(true, options->getRleVersion(), ORCTypeKind::LONG,
                     options->getCompressionKind());
  precision = static_cast<int64_t>(type->getPrecision());
  scale = static_cast<int64_t>(type->getScale());
}

void Decimal64ColumnWriter::writeVector(dbcommon::Vector *vector) {
  char *notNull = nullptr;
  std::unique_ptr<dbcommon::ByteBuffer> buf;
  if (vector->hasNullValue()) {
    buf = vector->getNullBuffer()->getReverseBools();
    notNull = const_cast<char *>(buf->data());
  }

  // A work around to support "insert select" expression
  if (vector->getTypeKind() == dbcommon::DECIMALNEWID) {
    dbcommon::DecimalVector *dvec =
        dynamic_cast<dbcommon::DecimalVector *>(vector);
    dvec->computeRawValueAndValPtrs();
  }

  uint64_t numValues = vector->getNumOfRows();
  const char **vals = vector->getValPtrs();
  const uint64_t *lens = vector->getLengths();
  for (uint64_t i = 0; i < numValues; i++) {
    if (!notNull || notNull[i]) {
      std::string str(vals[i], lens[i]);
      std::transform(str.begin(), str.end(), str.begin(), tolower);
      if (str == "nan") {
        vector->setNull(i);
        vector->setHasNull(true);
        continue;
      }
    }
  }

  ColumnWriter::writeVector(vector);
  if (vector->hasNullValue()) {
    buf = vector->getNullBuffer()->getReverseBools();
    notNull = const_cast<char *>(buf->data());
  }
  std::vector<int64_t> scales(numValues, static_cast<int64_t>(scale));

  for (uint64_t i = 0; i < numValues; i++) {
    if (!notNull || notNull[i]) {
      std::string str(vals[i], lens[i]);
      if (scale != 0) {
        size_t len = str.length();
        str = str.substr(0, len - scale - 1) + str.substr(len - scale);
      }
      int64_t value = std::stoll(str);
      writeInt64(value);
      dynamic_cast<DecimalColumnStatisticsImpl *>(stripeColStats.get())
          ->updateDecimal(Decimal(value, scale));
      if (createBloomFilter) bloomFilter->addString(vals[i], lens[i]);
    }
  }
  scaleRleCoder->write(scales.data(), vector->getNumOfRows(), notNull);
}

void Decimal64ColumnWriter::writeStripe(proto::StripeFooter *stripeFooter,
                                        proto::StripeStatistics *pb,
                                        OutputStream *out) {
  ColumnWriter::writeStripe(stripeFooter, pb, out);

  dataBufferedStream->flushToStream(out);
  ::orc::proto::Stream *dataStream = stripeFooter->add_streams();
  dataStream->set_column(this->getColumnId());
  dataStream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_DATA);
  dataStream->set_length(dataBufferedStream->getStreamSize());

  scaleRleCoder->flushToStream(out);
  ::orc::proto::Stream *secondaryStream = stripeFooter->add_streams();
  secondaryStream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_SECONDARY);
  secondaryStream->set_column(this->getColumnId());
  secondaryStream->set_length(scaleRleCoder->getStreamSize());

  stripeFooter->add_columns()->set_kind(getProtoColumnEncoding());

  // clear the buffers
  dataBufferedStream->reset();
  scaleRleCoder->reset();
  stripeColStats->reset();
}

uint64_t Decimal64ColumnWriter::getEstimatedSpaceNeeded() {
  return dataBufferedStream->getEstimatedSpaceNeeded() +
         scaleRleCoder->getEstimatedSpaceNeeded() +
         ColumnWriter::getEstimatedSpaceNeeded();
}

void Decimal64ColumnWriter::addTypeToFooter(proto::Footer *footer) {
  proto::Type *type = footer->add_types();
  type->set_kind(::orc::proto::Type_Kind::Type_Kind_DECIMAL);
  type->set_precision(precision);
  type->set_scale(scale);
}

orc::proto::ColumnEncoding_Kind
Decimal64ColumnWriter::getProtoColumnEncoding() {
  switch (version) {
    case RleVersion::RleVersion_0:
      return orc::proto::ColumnEncoding_Kind_DIRECT_V0;
    case RleVersion::RleVersion_1:
      return orc::proto::ColumnEncoding_Kind_DIRECT;
    case RleVersion::RleVersion_2:
      return orc::proto::ColumnEncoding_Kind_DIRECT_V2;
    default:
      LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "Version %u not supported",
                version);
  }
}

void Decimal64ColumnWriter::writeInt64(int64_t value) {
  uint64_t uintValue = zigzagEncodeInt64(value);
  while (true) {
    if ((uintValue & ~0x7f) == 0) {
      dataBufferedStream->writeByte((int8_t)uintValue);
      return;
    } else {
      dataBufferedStream->writeByte((int8_t)(0x80 | (uintValue & 0x7f)));
      uintValue >>= 7;
    }
  }
}

Decimal128ColumnWriter::Decimal128ColumnWriter(const orc::Type *type,
                                               WriterOptions *options)
    : ColumnWriter(type, options) {
  dataBufferedStream = createBlockCompressor(options->getCompressionKind());
  scaleRleCoder =
      createRleCoder(true, options->getRleVersion(), ORCTypeKind::LONG,
                     options->getCompressionKind());
  precision = static_cast<int32_t>(type->getPrecision());
  scale = static_cast<int32_t>(type->getScale());
}

void Decimal128ColumnWriter::writeVector(dbcommon::Vector *vector) {
  char *notNull = nullptr;
  std::unique_ptr<dbcommon::ByteBuffer> buf;
  if (vector->hasNullValue()) {
    buf = vector->getNullBuffer()->getReverseBools();
    notNull = const_cast<char *>(buf->data());
  }
  // A work around to support "insert select" expression
  if (vector->getTypeKind() == dbcommon::DECIMALNEWID) {
    dbcommon::DecimalVector *dvec =
        dynamic_cast<dbcommon::DecimalVector *>(vector);
    dvec->computeRawValueAndValPtrs();
  }

  uint64_t numValues = vector->getNumOfRows();
  const char **vals = vector->getValPtrs();
  const uint64_t *lens = vector->getLengths();
  for (uint64_t i = 0; i < numValues; i++) {
    if (!notNull || notNull[i]) {
      std::string str(vals[i], lens[i]);
      std::transform(str.begin(), str.end(), str.begin(), tolower);
      if (str == "nan") {
        vector->setNull(i);
        vector->setHasNull(true);
        continue;
      }
    }
  }

  ColumnWriter::writeVector(vector);
  if (vector->hasNullValue()) {
    buf = vector->getNullBuffer()->getReverseBools();
    notNull = const_cast<char *>(buf->data());
  }
  std::vector<int64_t> scales(numValues, static_cast<int64_t>(scale));

  for (uint64_t i = 0; i < numValues; i++) {
    if (!notNull || notNull[i]) {
      std::string str(vals[i], lens[i]);
      if (scale != 0) {
        size_t len = str.length();
        str = str.substr(0, len - scale - 1) + str.substr(len - scale);
      }
      orc::Int128 value = orc::Int128(str);
      writeInt128(&value);
      value = orc::Int128(str);
      dynamic_cast<DecimalColumnStatisticsImpl *>(stripeColStats.get())
          ->updateDecimal(Decimal(value, scale));
      if (createBloomFilter) bloomFilter->addString(vals[i], lens[i]);
    }
  }
  scaleRleCoder->write(scales.data(), vector->getNumOfRows(), notNull);
}

void Decimal128ColumnWriter::writeStripe(proto::StripeFooter *stripeFooter,
                                         proto::StripeStatistics *pb,
                                         OutputStream *out) {
  ColumnWriter::writeStripe(stripeFooter, pb, out);

  dataBufferedStream->flushToStream(out);
  ::orc::proto::Stream *dataStream = stripeFooter->add_streams();
  dataStream->set_column(this->getColumnId());
  dataStream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_DATA);
  dataStream->set_length(dataBufferedStream->getStreamSize());

  scaleRleCoder->flushToStream(out);
  ::orc::proto::Stream *secondaryStream = stripeFooter->add_streams();
  secondaryStream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_SECONDARY);
  secondaryStream->set_column(this->getColumnId());
  secondaryStream->set_length(scaleRleCoder->getStreamSize());

  stripeFooter->add_columns()->set_kind(getProtoColumnEncoding());

  // clear the buffers
  dataBufferedStream->reset();
  scaleRleCoder->reset();
  stripeColStats->reset();
}

uint64_t Decimal128ColumnWriter::getEstimatedSpaceNeeded() {
  return dataBufferedStream->getEstimatedSpaceNeeded() +
         scaleRleCoder->getEstimatedSpaceNeeded() +
         ColumnWriter::getEstimatedSpaceNeeded();
}

void Decimal128ColumnWriter::addTypeToFooter(proto::Footer *footer) {
  proto::Type *type = footer->add_types();
  type->set_kind(::orc::proto::Type_Kind::Type_Kind_DECIMAL);
  type->set_precision(precision);
  type->set_scale(scale);
}

orc::proto::ColumnEncoding_Kind
Decimal128ColumnWriter::getProtoColumnEncoding() {
  switch (version) {
    case RleVersion::RleVersion_0:
      return orc::proto::ColumnEncoding_Kind_DIRECT_V0;
    case RleVersion::RleVersion_1:
      return orc::proto::ColumnEncoding_Kind_DIRECT;
    case RleVersion::RleVersion_2:
      return orc::proto::ColumnEncoding_Kind_DIRECT_V2;
    default:
      LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "Version %u not supported",
                version);
  }
}

void Decimal128ColumnWriter::writeInt128(Int128 *value) {
  zigzagEncodeInt128(value);
  while (true) {
    if ((value->getLowBits() & ~0x7f) == 0) {
      dataBufferedStream->writeByte((int8_t)(value->getLowBits()));
      return;
    } else {
      dataBufferedStream->writeByte(
          (int8_t)(0x80 | (value->getLowBits() & 0x7f)));
      *value >>= 7;
    }
  }
}

void Decimal128ColumnWriter::zigzagEncodeInt128(Int128 *value) {
  *value <<= 1;
  if (*value < 0) {
    value->negate();
    *value -= 1;
  }
}

}  // namespace orc
