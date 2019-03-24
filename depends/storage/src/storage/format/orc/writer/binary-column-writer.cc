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
namespace orc {
BinaryColumnWriter::BinaryColumnWriter(const orc::Type *type,
                                       WriterOptions *options)
    : ColumnWriter(type, options) {
  lengthRleCoder =
      createRleCoder(false, options->getRleVersion(), ORCTypeKind::LONG,
                     options->getCompressionKind());
  dataBufferedStream = createBlockCompressor(options->getCompressionKind());
}
void BinaryColumnWriter::writeVector(dbcommon::Vector *vector) {
  ColumnWriter::writeVector(vector);

  const char *notNull = nullptr;
  std::unique_ptr<dbcommon::ByteBuffer> buf;
  if (vector->hasNullValue()) {
    buf = vector->getNullBuffer()->getReverseBools();
    notNull = reinterpret_cast<const char *>(buf->data());
  }

  uint64_t numValues = vector->getNumOfRows();
  const char **vals = vector->getValPtrs();
  const uint64_t *lens = vector->getLengths();

  for (uint64_t i = 0; i < numValues; i++) {
    if (!notNull || notNull[i]) {
      dataBufferedStream->write(vals[i], lens[i]);
    }
    dynamic_cast<BinaryColumnStatisticsImpl *>(stripeColStats.get())
        ->update(lens[i]);
    if (createBloomFilter) bloomFilter->addString(vals[i], lens[i]);
  }
  lengthRleCoder->write(const_cast<uint64_t *>(vector->getLengths()),
                        vector->getNumOfRows(), notNull);
}
void BinaryColumnWriter::writeStripe(proto::StripeFooter *stripeFooter,
                                     proto::StripeStatistics *pb,
                                     OutputStream *out) {
  ColumnWriter::writeStripe(stripeFooter, pb, out);

  lengthRleCoder->flushToStream(out);
  ::orc::proto::Stream *lenStream = stripeFooter->add_streams();
  lenStream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_LENGTH);
  lenStream->set_column(this->getColumnId());
  lenStream->set_length(lengthRleCoder->getStreamSize());
  dataBufferedStream->flushToStream(out);
  ::orc::proto::Stream *dataStream = stripeFooter->add_streams();
  dataStream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_DATA);
  dataStream->set_column(this->getColumnId());
  dataStream->set_length(dataBufferedStream->getStreamSize());

  stripeFooter->add_columns()->set_kind(getProtoColumnEncoding());

  // clear the buffers
  dataBufferedStream->reset();
  lengthRleCoder->reset();
}
uint64_t BinaryColumnWriter::getEstimatedSpaceNeeded() {
  return dataBufferedStream->getEstimatedSpaceNeeded() +
         lengthRleCoder->getEstimatedSpaceNeeded() +
         ColumnWriter::getEstimatedSpaceNeeded();
}
void BinaryColumnWriter::addTypeToFooter(proto::Footer *footer) {
  proto::Type *type = footer->add_types();
  type->set_kind(::orc::proto::Type_Kind::Type_Kind_BINARY);
}

orc::proto::ColumnEncoding_Kind BinaryColumnWriter::getProtoColumnEncoding() {
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

}  // namespace orc
