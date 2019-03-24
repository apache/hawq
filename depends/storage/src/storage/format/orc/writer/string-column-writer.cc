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

#include "dbcommon/utils/string-util.h"
#include "storage/format/orc/writer.h"
namespace orc {

StringColumnWriter::StringColumnWriter(const orc::Type *type,
                                       WriterOptions *options)
    : ColumnWriter(type, options),
      dictionaryKeySizeThreshold(options->getDictKeySizeThreshold()) {
  lengthRleCoder =
      createRleCoder(false, options->getRleVersion(), ORCTypeKind::LONG,
                     options->getCompressionKind());
  rowsRleCoder =
      createRleCoder(false, options->getRleVersion(), ORCTypeKind::LONG,
                     options->getCompressionKind());
  dictDataBufferedStream = createBlockCompressor(options->getCompressionKind());
  directDataBufferedStream =
      createBlockCompressor(options->getCompressionKind());
  rows.reserve(DEFAULT_NUMBER_TUPLES_PER_BATCH);
  if (dictionaryKeySizeThreshold == 0) {
    useDictionaryEncoding = false;
    doneDictionaryCheck = true;
  }
}

void StringColumnWriter::writeVector(dbcommon::Vector *vector) {
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
      if (useDictionaryEncoding) {
        rows.push_back(dictionary.add(vals[i], lens[i]));
      } else {
        directDataBufferedStream->write(vals[i], lens[i]);
      }
      if (writeStatsOn)
        dynamic_cast<StringColumnStatisticsImpl *>(stripeColStats.get())
            ->updateString(vals[i], lens[i]);
      if (createBloomFilter) bloomFilter->addString(vals[i], lens[i]);
    }
  }
  if (!useDictionaryEncoding)
    lengthRleCoder->write(const_cast<uint64_t *>(vector->getLengths()),
                          vector->getNumOfRows(), notNull);
}

void StringColumnWriter::writeStripe(proto::StripeFooter *stripeFooter,
                                     proto::StripeStatistics *pb,
                                     OutputStream *out) {
  checkDictionaryEncoding();

  if (rows.size() > 0) flushDictionary();

  ColumnWriter::writeStripe(stripeFooter, pb, out);

  if (useDictionaryEncoding) {
    lengthRleCoder->flushToStream(out);
    ::orc::proto::Stream *lenStream = stripeFooter->add_streams();
    lenStream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_LENGTH);
    lenStream->set_column(this->getColumnId());
    lenStream->set_length(lengthRleCoder->getStreamSize());
    dictDataBufferedStream->flushToStream(out);
    ::orc::proto::Stream *dataStream = stripeFooter->add_streams();
    dataStream->set_kind(
        ::orc::proto::Stream_Kind::Stream_Kind_DICTIONARY_DATA);
    dataStream->set_column(this->getColumnId());
    dataStream->set_length(dictDataBufferedStream->getStreamSize());
    rowsRleCoder->flushToStream(out);
    ::orc::proto::Stream *rowsStream = stripeFooter->add_streams();
    rowsStream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_DATA);
    rowsStream->set_column(this->getColumnId());
    rowsStream->set_length(rowsRleCoder->getStreamSize());
  } else {
    lengthRleCoder->flushToStream(out);
    ::orc::proto::Stream *lenStream = stripeFooter->add_streams();
    lenStream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_LENGTH);
    lenStream->set_column(this->getColumnId());
    lenStream->set_length(lengthRleCoder->getStreamSize());
    directDataBufferedStream->flushToStream(out);
    ::orc::proto::Stream *dataStream = stripeFooter->add_streams();
    dataStream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_DATA);
    dataStream->set_column(this->getColumnId());
    dataStream->set_length(directDataBufferedStream->getStreamSize());
  }
  ::orc::proto::ColumnEncoding *encodingBuilder = stripeFooter->add_columns();
  if (useDictionaryEncoding)
    encodingBuilder->set_dictionarysize(dictionary.size());
  encodingBuilder->set_kind(getProtoColumnEncoding());

  // clear the buffers
  reset();
}

uint64_t StringColumnWriter::getEstimatedSpaceNeeded() {
  if (useDictionaryEncoding)
    return 4 * rows.size() + dictionary.sizeInBytes() +
           ColumnWriter::getEstimatedSpaceNeeded();
  else
    return directDataBufferedStream->getEstimatedSpaceNeeded() +
           lengthRleCoder->getEstimatedSpaceNeeded() +
           ColumnWriter::getEstimatedSpaceNeeded();
}

void StringColumnWriter::addTypeToFooter(proto::Footer *footer) {
  proto::Type *typ = footer->add_types();
  if (type->getKind() == orc::ORCTypeKind::STRING) {
    typ->set_kind(::orc::proto::Type_Kind::Type_Kind_STRING);
  } else if (type->getKind() == orc::ORCTypeKind::VARCHAR) {
    typ->set_kind(::orc::proto::Type_Kind::Type_Kind_VARCHAR);
    typ->set_maximumlength(type->getMaximumLength());
  } else if (type->getKind() == orc::ORCTypeKind::CHAR) {
    typ->set_kind(::orc::proto::Type_Kind::Type_Kind_CHAR);
    typ->set_maximumlength(type->getMaximumLength());
  } else {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "unknown string/varchar/char type %d",
              type->getKind());
  }
}

orc::proto::ColumnEncoding_Kind StringColumnWriter::getProtoColumnEncoding() {
  if (useDictionaryEncoding) {
    switch (version) {
      case RleVersion::RleVersion_0:
        return orc::proto::ColumnEncoding_Kind_DICTIONARY_V0;
      case RleVersion::RleVersion_1:
        return orc::proto::ColumnEncoding_Kind_DICTIONARY;
      case RleVersion::RleVersion_2:
        return orc::proto::ColumnEncoding_Kind_DICTIONARY_V2;
      default:
        LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "Version %u not supported",
                  version);
    }
  } else {
    switch (version) {
      case RleVersion::RleVersion_0:
        return orc::proto::ColumnEncoding_Kind_DIRECT_V0;
      case RleVersion::RleVersion_1:
        return orc::proto::ColumnEncoding_Kind_DIRECT;
      case RleVersion::RleVersion_2:
        return orc::proto::ColumnEncoding_Kind_DIRECT_V2;
      default:
        LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "VersionKind %u not supported",
                  version);
    }
  }
}

void StringColumnWriter::addBloomFilterEntry() {
  ColumnWriter::addBloomFilterEntry();
  checkDictionaryEncoding();
  if (!useDictionaryEncoding && rows.size() > 0) flushDictionary();
}

void StringColumnWriter::checkDictionaryEncoding() {
  if (!doneDictionaryCheck) {
    double ratio = rows.size() > 0 ? static_cast<double>(dictionary.size()) /
                                         static_cast<double>(rows.size())
                                   : 0;
    useDictionaryEncoding = ratio <= dictionaryKeySizeThreshold;
    doneDictionaryCheck = true;
  }
}

void StringColumnWriter::flushDictionary() {
  std::vector<const char *> vals;
  std::vector<uint64_t> lens;
  std::vector<uint32_t> dumpOrder;
  dictionary.dump(&vals, &lens, &dumpOrder);
  std::vector<uint64_t> position;
  uint64_t rowSize = rows.size();
  position.resize(rowSize);
  for (int i = 0; i < rowSize; ++i) {
    position[i] = dumpOrder[rows[i]];
  }

  if (useDictionaryEncoding) {
    uint64_t dictSize = lens.size();
    for (uint64_t i = 0; i < dictSize; ++i)
      dictDataBufferedStream->write(vals[i], lens[i]);
    lengthRleCoder->write(lens.data(), dictSize, nullptr);
    rowsRleCoder->write(position.data(), rowSize, nullptr);
  } else {
    for (uint64_t i = 0; i < rowSize; ++i) {
      directDataBufferedStream->write(vals[position[i]], lens[position[i]]);
      lengthRleCoder->write(&lens[position[i]], 1, nullptr);
    }
  }

  rows.clear();
}

void StringColumnWriter::reset() {
  ColumnWriter::reset();

  dictionary.clear();
  dictDataBufferedStream->reset();
  lengthRleCoder->reset();
  rowsRleCoder->reset();
  directDataBufferedStream->reset();

  stripeColStats->reset();
}

}  // namespace orc
