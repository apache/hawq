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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_WRITER_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_WRITER_H_

#include <algorithm>
#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "dbcommon/common/tuple-batch.h"
#include "dbcommon/common/vector/list-vector.h"
#include "dbcommon/common/vector/struct-vector.h"
#include "dbcommon/common/vector/variable-length-vector.h"
#include "dbcommon/utils/comp/compressor.h"
#include "dbcommon/utils/macro.h"
#include "dbcommon/utils/string-util.h"

#include "storage/common/bloom-filter.h"
#include "storage/format/orc/byte-rle.h"
#include "storage/format/orc/file-version.h"
#include "storage/format/orc/orc-proto-definition.h"
#include "storage/format/orc/output-stream.h"
#include "storage/format/orc/seekable-output-stream.h"
#include "storage/format/orc/string-dictionary.h"

namespace orc {

struct WriterOptionsPrivate {
  FileVersion fileVersion;
  uint64_t blockSize;
  uint64_t stripeSize;
  uint64_t compressionBlockSize;
  double blockPaddingTolerance;
  CompressionKind compKind;
  RleVersion rleVersion;
  double dictionaryKeySizeThreshold;
  int64_t rowIndexStrideValue;
  std::unique_ptr<std::vector<bool>> columnsToBloomFilter;
  bool writeStats;

  WriterOptionsPrivate() : fileVersion(0, 11) {
    // The block size, for example, hdfs block size
    // Used for letting stripe not across block boundary
    // and block padding, 128 MB by default
    blockSize = 1 << 27;
    stripeSize = 1 << 26;  // default stripe size is 64 MB
    compressionBlockSize = ORC_COMPRESSION_BLOCK_SIZE;
    // If the number of distinct keys in a dictionary is greater than this
    // fraction of the total number of non-null rows, turn off dictionary
    // encoding
    dictionaryKeySizeThreshold = 0;
    blockPaddingTolerance = 0.05;
    compKind = CompressionKind::CompressionKind_NONE;
    rleVersion = RleVersion::RleVersion_2;
    rowIndexStrideValue = DEFAULT_NUMBER_TUPLES_PER_BATCH * 32;
    columnsToBloomFilter = nullptr;
    writeStats = true;
  }
};

class WriterOptions {
 public:
  WriterOptions()
      : privateBits(
            std::unique_ptr<WriterOptionsPrivate>(new WriterOptionsPrivate)) {}

  orc::Type *getSchema() { return schema.get(); }
  void setSchema(std::unique_ptr<orc::Type> t) { this->schema = std::move(t); }

  uint64_t getBlockSize() { return privateBits->blockSize; }
  void setBlockSize(uint64_t blockSize) { privateBits->blockSize = blockSize; }

  uint64_t getStripeSize() { return privateBits->stripeSize; }

  double getPaddingTolerance() { return privateBits->blockPaddingTolerance; }

  void setCompressionKind(const std::string &kindStr) {
    std::string kind = dbcommon::StringUtil::lower(kindStr);
    if (kind == "none")
      privateBits->compKind = CompressionKind::CompressionKind_NONE;
    else if (kind == "lz4")
      privateBits->compKind = CompressionKind::CompressionKind_LZ4;
    else if (kind == "snappy")
      privateBits->compKind = CompressionKind::CompressionKind_SNAPPY;
    else
      LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                "compression kind %s not supported", kindStr.c_str());
  }

  CompressionKind getCompressionKind() { return privateBits->compKind; }

  void setRleVersion(const std::string &versionStr) {
    if (versionStr == "v0")
      privateBits->rleVersion = RleVersion::RleVersion_0;
    else if (versionStr == "v1")
      privateBits->rleVersion = RleVersion::RleVersion_1;
    else if (versionStr == "v2")
      privateBits->rleVersion = RleVersion::RleVersion_2;
    else
      LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "RleVersion %s not supported",
                versionStr.c_str());
  }

  void setDictKeySizeThreshold(double threshold) {
    privateBits->dictionaryKeySizeThreshold = threshold;
  }

  double getDictKeySizeThreshold() {
    return privateBits->dictionaryKeySizeThreshold;
  }

  void setColumnsToBloomFilter(const std::vector<int> &columnsToBloomFilter,
                               int32_t tableSize) {
    privateBits->columnsToBloomFilter.reset(
        new std::vector<bool>(schema.get()->getMaximumColumnId() + 1, false));

    for (int i = 0; i < columnsToBloomFilter.size(); ++i) {
      const orc::Type *subType =
          schema.get()->getSubtype(columnsToBloomFilter[i]);
      (*privateBits->columnsToBloomFilter)[subType->getColumnId()] = true;
    }
  }

  const std::vector<bool> *getColumnsToBloomFilter() {
    return privateBits->columnsToBloomFilter.get();
  }

  void setWriteStats(bool writeStats) { privateBits->writeStats = writeStats; }

  bool getWriteStats() { return privateBits->writeStats; }

  RleVersion getRleVersion() { return privateBits->rleVersion; }

  orc::proto::CompressionKind getProtoCompressionKind() {
    switch (privateBits->compKind) {
      case orc::CompressionKind::CompressionKind_NONE:
        return orc::proto::CompressionKind::NONE;
      case orc::CompressionKind::CompressionKind_SNAPPY:
        return orc::proto::CompressionKind::SNAPPY;
      case orc::CompressionKind::CompressionKind_LZ4:
        return orc::proto::CompressionKind::LZ4;
      default:
        LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                  "compression kind %lu not supported", privateBits->compKind);
    }
  }

  int64_t getRowIndexStride() { return privateBits->rowIndexStrideValue; }

  uint64_t getCompressionBlockSize() {
    return privateBits->compressionBlockSize;
  }

  FileVersion getFileVersion() { return privateBits->fileVersion; }

 private:
  std::unique_ptr<orc::Type> schema;
  std::unique_ptr<WriterOptionsPrivate> privateBits;
};

class ColumnWriter {
 public:
  ColumnWriter(const orc::Type *type, WriterOptions *options)
      : bitWriter(createBooleanRleEncoderImpl(options->getCompressionKind())),
        stripeColStats(createColumnStatistics(type)),
        fileColStats(createColumnStatistics(type)),
        type(type),
        version(options->getRleVersion()),
        createBloomFilter(false),
        writeStatsOn(options->getWriteStats()) {
    const std::vector<bool> *columnsToBloomFilter =
        options->getColumnsToBloomFilter();
    if (columnsToBloomFilter && (*columnsToBloomFilter)[this->getColumnId()])
      createBloomFilter = true;
    if (createBloomFilter) {
      bloomFilter.reset(new storage::BloomFilter(options->getRowIndexStride()));
      bloomFilterWriter = createBlockCompressor(options->getCompressionKind());
    }
  }

  virtual ~ColumnWriter() {}

  virtual void writeVector(dbcommon::Vector *vector) {
    uint64_t sz = vector->getNumOfRows();
    if (vector->hasNullValue()) {
      std::unique_ptr<dbcommon::ByteBuffer> buf =
          vector->getNullBuffer()->getReverseBools();
      bitWriter->write(buf->data(), sz, nullptr);

      // gather statistics
      const bool *nulls = vector->getNullBuffer()->getBools();
      uint64_t cnt = 0;
      for (int i = 0; i < sz; ++i) {
        if (!nulls[i]) ++cnt;
      }
      notNullCnt += cnt;
    } else {
      bitWriter->write(nullptr, sz, nullptr);
      notNullCnt += sz;
    }
    totalCnt += sz;
  }

  virtual void writeStripe(proto::StripeFooter *stripeFooter,
                           proto::StripeStatistics *pb, OutputStream *out) {
    assert(stripeFooter != nullptr && out != nullptr);

    if (createBloomFilter && bloomFilterIndexProto.bloomfilter_size() > 0) {
      std::string bfStr = bloomFilterIndexProto.SerializeAsString();
      bloomFilterWriter->write(bfStr.c_str(), bfStr.length());
      bloomFilterWriter->flushToStream(out);
      ::orc::proto::Stream *bloomFilterStream = stripeFooter->add_streams();
      bloomFilterStream->set_kind(
          ::orc::proto::Stream_Kind::Stream_Kind_BLOOM_FILTER);
      bloomFilterStream->set_column(this->getColumnId());
      bloomFilterStream->set_length(bloomFilterWriter->getStreamSize());
    }

    if (totalCnt != notNullCnt) {
      bitWriter->flushToStream(out);
      ::orc::proto::Stream *stream = stripeFooter->add_streams();
      stream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_PRESENT);
      stream->set_column(this->getColumnId());
      stream->set_length(bitWriter->getStreamSize());
    }

    if (writeStatsOn) {
      stripeColStats->increment(notNullCnt);
      if (totalCnt == notNullCnt) stripeColStats->unsetNull();
      fileColStats->merge(*stripeColStats);
      stripeColStats->serialize(pb->add_colstats());
    }

    reset();
  }

  void addChildWriter(std::unique_ptr<ColumnWriter> writer) {
    childWriters.push_back(std::move(writer));
  }

  ColumnWriter *getChildWriter(uint32_t index) {
    return childWriters[index].get();
  }

  uint32_t getColumnId() { return type->getColumnId(); }

  virtual uint64_t getEstimatedSpaceNeeded() {
    return bitWriter->getEstimatedSpaceNeeded();
  }

  virtual void addBloomFilterEntry() {
    if (createBloomFilter) {
      proto::BloomFilter *bloomFilterProto =
          bloomFilterIndexProto.add_bloomfilter();
      bloomFilterProto->set_numhashfunctions(
          bloomFilter->getNumHashFunctions());
      for (int i = 0; i < bloomFilter->size(); ++i) {
        bloomFilterProto->add_bitset(bloomFilter->getBitSet()[i]);
      }
      bloomFilter->reset();
    }
  }

  virtual void writeFileStatistics(proto::Footer *fileFooter) {
    fileColStats->serialize(fileFooter->add_statistics());
  }

  virtual orc::proto::ColumnEncoding_Kind getProtoColumnEncoding() {
    return orc::proto::ColumnEncoding_Kind_DIRECT;
  }

  void reset() {
    // clear the buffers
    bitWriter->reset();

    notNullCnt = 0;
    totalCnt = 0;

    if (createBloomFilter) {
      bloomFilter->reset();
      bloomFilterWriter->reset();
      bloomFilterIndexProto.Clear();
    }
  }

  virtual void addTypeToFooter(proto::Footer *footer) = 0;

  static std::unique_ptr<ColumnWriter> buildColumnWriter(
      const orc::Type *type, WriterOptions *options);

 protected:
  std::vector<std::unique_ptr<ColumnWriter>> childWriters;
  std::unique_ptr<BooleanRleEncoderImpl> bitWriter;
  RleVersion version;
  const orc::Type *type = nullptr;

  std::unique_ptr<ColumnStatisticsImpl> stripeColStats;
  std::unique_ptr<ColumnStatisticsImpl> fileColStats;
  uint64_t notNullCnt = 0;
  uint64_t totalCnt = 0;
  storage::BloomFilter::uptr bloomFilter;
  std::unique_ptr<SeekableOutputStream> bloomFilterWriter;
  bool createBloomFilter;
  proto::BloomFilterIndex bloomFilterIndexProto;
  bool writeStatsOn;
};

class BooleanColumnWriter : public ColumnWriter {
 public:
  BooleanColumnWriter(const orc::Type *type, WriterOptions *options)
      : ColumnWriter(type, options),
        booleanWriter(
            createBooleanRleEncoderImpl(options->getCompressionKind())) {
    createBloomFilter = false;
  }
  virtual ~BooleanColumnWriter() {}

  void writeVector(dbcommon::Vector *vector) override {
    ColumnWriter::writeVector(vector);

    booleanWriter->write(
        const_cast<char *>(vector->getValue()), vector->getNumOfRows(),
        (vector->hasNullValue()
             ? vector->getNullBuffer()->getReverseBools()->data()
             : nullptr));

    if (writeStatsOn) {
      uint64_t numValues = vector->getNumOfRows();
      const char *vals = vector->getValue();
      for (uint64_t i = 0; i < numValues; i++) {
        if (!vector->isNull(i)) {
          dynamic_cast<BooleanColumnStatisticsImpl *>(stripeColStats.get())
              ->updateBoolean(reinterpret_cast<const bool *>(vals)[i]);
        }
      }
    }
  }

  void writeStripe(proto::StripeFooter *stripeFooter,
                   proto::StripeStatistics *pb, OutputStream *out) override {
    ColumnWriter::writeStripe(stripeFooter, pb, out);

    booleanWriter->flushToStream(out);

    ::orc::proto::Stream *stream = stripeFooter->add_streams();
    stream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_DATA);
    stream->set_column(this->getColumnId());
    stream->set_length(booleanWriter->getStreamSize());

    stripeFooter->add_columns()->set_kind(getProtoColumnEncoding());

    booleanWriter->reset();

    stripeColStats->reset();
  }

  uint64_t getEstimatedSpaceNeeded() override {
    return booleanWriter->getEstimatedSpaceNeeded() +
           ColumnWriter::getEstimatedSpaceNeeded();
  }

  void addTypeToFooter(proto::Footer *footer) override {
    proto::Type *type = footer->add_types();
    type->set_kind(::orc::proto::Type_Kind::Type_Kind_BOOLEAN);
  }

 protected:
  std::unique_ptr<BooleanRleEncoderImpl> booleanWriter;
};

class ByteColumnWriter : public ColumnWriter {
 public:
  explicit ByteColumnWriter(const orc::Type *type, WriterOptions *options)
      : ColumnWriter(type, options) {
    rleCoder = createByteRleCoder(options->getCompressionKind());
  }
  virtual ~ByteColumnWriter() {}

  void writeVector(dbcommon::Vector *vector) override {
    ColumnWriter::writeVector(vector);

    rleCoder->write(const_cast<char *>(vector->getValue()),
                    vector->getNumOfRows(),
                    (vector->hasNullValue()
                         ? vector->getNullBuffer()->getReverseBools()->data()
                         : nullptr));

    if (writeStatsOn) {
      uint64_t numValues = vector->getNumOfRows();
      const char *vals = vector->getValue();
      for (uint64_t i = 0; i < numValues; i++) {
        if (!vector->isNull(i)) {
          dynamic_cast<IntegerColumnStatisticsImpl *>(stripeColStats.get())
              ->updateInteger(reinterpret_cast<const int8_t *>(vals)[i]);
          if (createBloomFilter)
            bloomFilter->addInt(reinterpret_cast<const int8_t *>(vals)[i]);
        }
      }
    }
  }

  void writeStripe(proto::StripeFooter *stripeFooter,
                   proto::StripeStatistics *pb, OutputStream *out) override {
    ColumnWriter::writeStripe(stripeFooter, pb, out);

    rleCoder->flushToStream(out);

    ::orc::proto::Stream *stream = stripeFooter->add_streams();
    stream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_DATA);
    stream->set_column(this->getColumnId());
    stream->set_length(rleCoder->getStreamSize());

    // LOG_INFO("stream kind %s, column# %d, length %lld ",
    // "Stream_Kind_DATA",
    //    this->getColumnId(), stream->length());

    stripeFooter->add_columns()->set_kind(getProtoColumnEncoding());
    // LOG_INFO("ColumnEncoding_Kind_DIRECT_V2");

    // clear the buffers
    rleCoder->reset();

    stripeColStats->reset();
  }

  uint64_t getEstimatedSpaceNeeded() override {
    return rleCoder->getEstimatedSpaceNeeded() +
           ColumnWriter::getEstimatedSpaceNeeded();
  }

  void addTypeToFooter(proto::Footer *footer) override {
    proto::Type *type = footer->add_types();
    type->set_kind(::orc::proto::Type_Kind::Type_Kind_BYTE);
  }

 protected:
  std::unique_ptr<ByteRleCoder> rleCoder;
};

class IntegerColumnWriter : public ColumnWriter {
 public:
  explicit IntegerColumnWriter(const orc::Type *type, WriterOptions *options)
      : ColumnWriter(type, options) {}
  virtual ~IntegerColumnWriter() {}

  void writeVector(dbcommon::Vector *vector) override {
    ColumnWriter::writeVector(vector);

    rleCoder->write(const_cast<char *>(vector->getValue()),
                    vector->getNumOfRows(),
                    (vector->hasNullValue()
                         ? vector->getNullBuffer()->getReverseBools()->data()
                         : nullptr));

    if (writeStatsOn) writeStats(vector);
  }

  void writeStripe(proto::StripeFooter *stripeFooter,
                   proto::StripeStatistics *pb, OutputStream *out) override {
    ColumnWriter::writeStripe(stripeFooter, pb, out);

    rleCoder->flushToStream(out);

    ::orc::proto::Stream *stream = stripeFooter->add_streams();
    stream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_DATA);
    stream->set_column(this->getColumnId());
    stream->set_length(rleCoder->getStreamSize());

    // LOG_INFO("stream kind %s, column# %d, length %lld ",
    // "Stream_Kind_DATA",
    //    this->getColumnId(), stream->length());

    stripeFooter->add_columns()->set_kind(getProtoColumnEncoding());
    // LOG_INFO("ColumnEncoding_Kind_DIRECT_V2");

    // clear the buffers
    rleCoder->reset();

    stripeColStats->reset();
  }

  uint64_t getEstimatedSpaceNeeded() override {
    return rleCoder->getEstimatedSpaceNeeded() +
           ColumnWriter::getEstimatedSpaceNeeded();
  }

  orc::proto::ColumnEncoding_Kind getProtoColumnEncoding() override {
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

 protected:
  std::unique_ptr<RleCoder> rleCoder;

 private:
  virtual void writeStats(dbcommon::Vector *vector) = 0;
};

class IntColumnWriter : public IntegerColumnWriter {
 public:
  explicit IntColumnWriter(const orc::Type *type, WriterOptions *options)
      : IntegerColumnWriter(type, options) {
    rleCoder = createRleCoder(true, options->getRleVersion(), ORCTypeKind::INT,
                              options->getCompressionKind());
  }
  virtual ~IntColumnWriter() {}

  void addTypeToFooter(proto::Footer *footer) override {
    proto::Type *type = footer->add_types();
    type->set_kind(::orc::proto::Type_Kind::Type_Kind_INT);
  }

 private:
  void writeStats(dbcommon::Vector *vector) override {
    uint64_t numValues = vector->getNumOfRows();
    const char *vals = vector->getValue();

    for (uint64_t i = 0; i < numValues; i++) {
      if (!vector->isNull(i)) {
        dynamic_cast<IntegerColumnStatisticsImpl *>(stripeColStats.get())
            ->updateInteger(reinterpret_cast<const int32_t *>(vals)[i]);
        if (createBloomFilter)
          bloomFilter->addInt(reinterpret_cast<const int32_t *>(vals)[i]);
      }
    }
  }
};

class LongColumnWriter : public IntegerColumnWriter {
 public:
  explicit LongColumnWriter(const orc::Type *type, WriterOptions *options)
      : IntegerColumnWriter(type, options) {
    rleCoder = createRleCoder(true, options->getRleVersion(), ORCTypeKind::LONG,
                              options->getCompressionKind());
  }
  virtual ~LongColumnWriter() {}

  void addTypeToFooter(proto::Footer *footer) override {
    proto::Type *type = footer->add_types();
    type->set_kind(::orc::proto::Type_Kind::Type_Kind_LONG);
  }

 private:
  void writeStats(dbcommon::Vector *vector) override {
    uint64_t numValues = vector->getNumOfRows();
    const char *vals = vector->getValue();

    for (uint64_t i = 0; i < numValues; i++) {
      if (!vector->isNull(i)) {
        dynamic_cast<IntegerColumnStatisticsImpl *>(stripeColStats.get())
            ->updateInteger(reinterpret_cast<const int64_t *>(vals)[i]);
        if (createBloomFilter)
          bloomFilter->addInt(reinterpret_cast<const int64_t *>(vals)[i]);
      }
    }
  }
};

class ShortColumnWriter : public IntegerColumnWriter {
 public:
  explicit ShortColumnWriter(const orc::Type *type, WriterOptions *options)
      : IntegerColumnWriter(type, options) {
    rleCoder =
        createRleCoder(true, options->getRleVersion(), ORCTypeKind::SHORT,
                       options->getCompressionKind());
  }
  virtual ~ShortColumnWriter() {}

  void addTypeToFooter(proto::Footer *footer) override {
    proto::Type *type = footer->add_types();
    type->set_kind(::orc::proto::Type_Kind::Type_Kind_SHORT);
  }

 private:
  void writeStats(dbcommon::Vector *vector) override {
    uint64_t numValues = vector->getNumOfRows();
    const char *vals = vector->getValue();

    for (uint64_t i = 0; i < numValues; i++) {
      if (!vector->isNull(i)) {
        dynamic_cast<IntegerColumnStatisticsImpl *>(stripeColStats.get())
            ->updateInteger(reinterpret_cast<const int16_t *>(vals)[i]);
        if (createBloomFilter)
          bloomFilter->addInt(reinterpret_cast<const int16_t *>(vals)[i]);
      }
    }
  }
};

class DoubleColumnWriter : public ColumnWriter {
 public:
  explicit DoubleColumnWriter(const orc::Type *type, WriterOptions *options)
      : ColumnWriter(type, options) {
    bufferedStream = createBlockCompressor(options->getCompressionKind());
    myType = type->getKind();
  }

  virtual ~DoubleColumnWriter() {}

  void writeVector(dbcommon::Vector *vector) override {
    ColumnWriter::writeVector(vector);

    uint64_t numValues = vector->getNumOfRows();
    const char *vals = vector->getValue();
    for (uint64_t i = 0; i < numValues; i++) {
      if (!vector->isNull(i)) {
        if (myType == orc::ORCTypeKind::DOUBLE) {
          bufferedStream->write<double>(
              reinterpret_cast<const double *>(vals)[i]);
          if (writeStatsOn)
            dynamic_cast<DoubleColumnStatisticsImpl *>(stripeColStats.get())
                ->updateDouble(reinterpret_cast<const double *>(vals)[i]);
          if (createBloomFilter)
            bloomFilter->addDouble(reinterpret_cast<const double *>(vals)[i]);
        } else {
          assert(myType == orc::ORCTypeKind::FLOAT);
          bufferedStream->write<float>(
              reinterpret_cast<const float *>(vals)[i]);
          if (writeStatsOn)
            dynamic_cast<DoubleColumnStatisticsImpl *>(stripeColStats.get())
                ->updateDouble(reinterpret_cast<const float *>(vals)[i]);
          if (createBloomFilter)
            bloomFilter->addDouble(reinterpret_cast<const float *>(vals)[i]);
        }
      }
    }
  }

  void writeStripe(proto::StripeFooter *stripeFooter,
                   proto::StripeStatistics *pb, OutputStream *out) override {
    ColumnWriter::writeStripe(stripeFooter, pb, out);

    bufferedStream->flushToStream(out);

    ::orc::proto::Stream *stream = stripeFooter->add_streams();
    stream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_DATA);
    stream->set_column(this->getColumnId());
    stream->set_length(bufferedStream->getStreamSize());

    // LOG_INFO("stream kind %s, column# %d, length %lld ",
    // "Stream_Kind_DATA",
    //    this->getColumnId(), stream->length());

    stripeFooter->add_columns()->set_kind(getProtoColumnEncoding());
    // LOG_INFO("ColumnEncoding_Kind_DIRECT_V2");

    // clear the buffers
    bufferedStream->reset();

    stripeColStats->reset();
  }

  uint64_t getEstimatedSpaceNeeded() override {
    return bufferedStream->getEstimatedSpaceNeeded() +
           ColumnWriter::getEstimatedSpaceNeeded();
  }

  void addTypeToFooter(proto::Footer *footer) override {
    proto::Type *typ = footer->add_types();
    if (type->getKind() == orc::ORCTypeKind::FLOAT) {
      typ->set_kind(::orc::proto::Type_Kind::Type_Kind_FLOAT);
    } else if (type->getKind() == orc::ORCTypeKind::DOUBLE) {
      typ->set_kind(::orc::proto::Type_Kind::Type_Kind_DOUBLE);
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "unknown double/float type %d",
                type->getKind());
    }
  }

 protected:
  std::unique_ptr<SeekableOutputStream> bufferedStream;
  orc::ORCTypeKind myType;
};

class StringColumnWriter : public ColumnWriter {
 public:
  explicit StringColumnWriter(const orc::Type *type, WriterOptions *options);

  void writeVector(dbcommon::Vector *vector) override;

  void writeStripe(proto::StripeFooter *stripeFooter,
                   proto::StripeStatistics *pb, OutputStream *out) override;

  uint64_t getEstimatedSpaceNeeded() override;

  void addTypeToFooter(proto::Footer *footer) override;

  orc::proto::ColumnEncoding_Kind getProtoColumnEncoding() override;

  void addBloomFilterEntry() override;

 private:
  void checkDictionaryEncoding();

  void flushDictionary();

  void reset();

 private:
  double dictionaryKeySizeThreshold;
  bool doneDictionaryCheck = false;
  bool useDictionaryEncoding = true;

  // for dict
  std::unique_ptr<RleCoder> rowsRleCoder;
  std::unique_ptr<SeekableOutputStream> dictDataBufferedStream;
  StringDictionary dictionary;
  std::vector<uint32_t> rows;
  // for direct
  std::unique_ptr<SeekableOutputStream> directDataBufferedStream;
  // share
  std::unique_ptr<RleCoder> lengthRleCoder;
};

class BinaryColumnWriter : public ColumnWriter {
 public:
  explicit BinaryColumnWriter(const orc::Type *type, WriterOptions *options);
  void writeVector(dbcommon::Vector *vector) override;
  void writeStripe(proto::StripeFooter *stripeFooter,
                   proto::StripeStatistics *pb, OutputStream *out) override;
  uint64_t getEstimatedSpaceNeeded() override;
  void addTypeToFooter(proto::Footer *footer) override;

 private:
  orc::proto::ColumnEncoding_Kind getProtoColumnEncoding() override;
  std::unique_ptr<SeekableOutputStream> dataBufferedStream;
  std::unique_ptr<RleCoder> lengthRleCoder;
};

class DateColumnWriter : public ColumnWriter {
 public:
  explicit DateColumnWriter(const orc::Type *type, WriterOptions *options)
      : ColumnWriter(type, options) {
    rleCoder = createRleCoder(true, options->getRleVersion(), ORCTypeKind::INT,
                              options->getCompressionKind());
  }
  virtual ~DateColumnWriter() {}

  void writeVector(dbcommon::Vector *vector) override {
    ColumnWriter::writeVector(vector);

    rleCoder->write(const_cast<char *>(vector->getValue()),
                    vector->getNumOfRows(),
                    (vector->hasNullValue()
                         ? vector->getNullBuffer()->getReverseBools()->data()
                         : nullptr));

    if (writeStatsOn) writeStats(vector);
  }

  void writeStripe(proto::StripeFooter *stripeFooter,
                   proto::StripeStatistics *pb, OutputStream *out) override {
    ColumnWriter::writeStripe(stripeFooter, pb, out);

    rleCoder->flushToStream(out);

    ::orc::proto::Stream *stream = stripeFooter->add_streams();
    stream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_DATA);
    stream->set_column(this->getColumnId());
    stream->set_length(rleCoder->getStreamSize());

    stripeFooter->add_columns()->set_kind(getProtoColumnEncoding());

    // clear the buffers
    rleCoder->reset();

    stripeColStats->reset();
  }

  uint64_t getEstimatedSpaceNeeded() override {
    return rleCoder->getEstimatedSpaceNeeded() +
           ColumnWriter::getEstimatedSpaceNeeded();
  }

  orc::proto::ColumnEncoding_Kind getProtoColumnEncoding() override {
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

  void addTypeToFooter(proto::Footer *footer) override {
    proto::Type *type = footer->add_types();
    type->set_kind(::orc::proto::Type_Kind::Type_Kind_DATE);
  }

 protected:
  std::unique_ptr<RleCoder> rleCoder;

 private:
  void writeStats(dbcommon::Vector *vector) {
    uint64_t numValues = vector->getNumOfRows();
    const char *vals = vector->getValue();

    for (uint64_t i = 0; i < numValues; i++) {
      if (!vector->isNull(i)) {
        dynamic_cast<DateColumnStatisticsImpl *>(stripeColStats.get())
            ->updateDate(reinterpret_cast<const int32_t *>(vals)[i]);
        if (createBloomFilter)
          bloomFilter->addInt(reinterpret_cast<const int32_t *>(vals)[i]);
      }
    }
  }
};

class TimeColumnWriter : public ColumnWriter {
 public:
  explicit TimeColumnWriter(const orc::Type *type, WriterOptions *options)
      : ColumnWriter(type, options) {
    rleCoder = createRleCoder(true, options->getRleVersion(), ORCTypeKind::LONG,
                              options->getCompressionKind());
  }
  virtual ~TimeColumnWriter() {}

  void writeVector(dbcommon::Vector *vector) override {
    ColumnWriter::writeVector(vector);

    rleCoder->write(const_cast<char *>(vector->getValue()),
                    vector->getNumOfRows(),
                    (vector->hasNullValue()
                         ? vector->getNullBuffer()->getReverseBools()->data()
                         : nullptr));

    if (writeStatsOn) writeStats(vector);
  }

  void writeStripe(proto::StripeFooter *stripeFooter,
                   proto::StripeStatistics *pb, OutputStream *out) override {
    ColumnWriter::writeStripe(stripeFooter, pb, out);

    rleCoder->flushToStream(out);

    ::orc::proto::Stream *stream = stripeFooter->add_streams();
    stream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_DATA);
    stream->set_column(this->getColumnId());
    stream->set_length(rleCoder->getStreamSize());

    stripeFooter->add_columns()->set_kind(getProtoColumnEncoding());

    // clear the buffers
    rleCoder->reset();

    stripeColStats->reset();
  }

  uint64_t getEstimatedSpaceNeeded() override {
    return rleCoder->getEstimatedSpaceNeeded() +
           ColumnWriter::getEstimatedSpaceNeeded();
  }

  orc::proto::ColumnEncoding_Kind getProtoColumnEncoding() override {
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

  void addTypeToFooter(proto::Footer *footer) override {
    proto::Type *type = footer->add_types();
    type->set_kind(::orc::proto::Type_Kind::Type_Kind_TIME);
  }

 protected:
  std::unique_ptr<RleCoder> rleCoder;

 private:
  void writeStats(dbcommon::Vector *vector) {
    uint64_t numValues = vector->getNumOfRows();
    const char *vals = vector->getValue();

    for (uint64_t i = 0; i < numValues; i++) {
      if (!vector->isNull(i)) {
        dynamic_cast<IntegerColumnStatisticsImpl *>(stripeColStats.get())
            ->updateInteger(reinterpret_cast<const int64_t *>(vals)[i]);
        if (createBloomFilter)
          bloomFilter->addInt(reinterpret_cast<const int64_t *>(vals)[i]);
      }
    }
  }
};

class TimestampColumnWriter : public ColumnWriter {
 public:
  explicit TimestampColumnWriter(const orc::Type *type, WriterOptions *options)
      : ColumnWriter(type, options) {
    secondsRleCoder =
        createRleCoder(true, options->getRleVersion(), ORCTypeKind::LONG,
                       options->getCompressionKind());
    nanoRleCoder =
        createRleCoder(false, options->getRleVersion(), ORCTypeKind::LONG,
                       options->getCompressionKind());
  }
  virtual ~TimestampColumnWriter() {}

  void writeVector(dbcommon::Vector *vector) override {
    ColumnWriter::writeVector(vector);

    int64_t *seconds =
        reinterpret_cast<int64_t *>(const_cast<char *>(vector->getValue()));
    int64_t *nanoseconds = reinterpret_cast<int64_t *>(
        const_cast<char *>(vector->getNanoseconds()));

    const char *notnulls = nullptr;
    std::unique_ptr<dbcommon::ByteBuffer> buf;
    if (vector->hasNullValue()) {
      buf = vector->getNullBuffer()->getReverseBools();
      notnulls = reinterpret_cast<const char *>(buf->data());
    }

    // CAUTION: not consider selectlist now
    for (uint64_t i = 0; i < vector->getNumOfRows(); i++) {
      int64_t second =
          seconds[i] -
          (ORC_TIMESTAMP_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECONDS_PER_DAY;
      int64_t nano;
      if (!notnulls || notnulls[i]) {
        nano = formatNanos(nanoseconds[i]);
        secondsRleCoder->write(&second, 1, nullptr);
        nanoRleCoder->write(&nano, 1, nullptr);
      } else {
        nano = nanoseconds[i];
        secondsRleCoder->write(&second, 1, &notnulls[i]);
        nanoRleCoder->write(&nano, 1, &notnulls[i]);
      }
    }

    if (writeStatsOn) writeStats(vector);
  }

  void writeStripe(proto::StripeFooter *stripeFooter,
                   proto::StripeStatistics *pb, OutputStream *out) override {
    ColumnWriter::writeStripe(stripeFooter, pb, out);

    secondsRleCoder->flushToStream(out);
    ::orc::proto::Stream *secondsStream = stripeFooter->add_streams();
    secondsStream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_DATA);
    secondsStream->set_column(this->getColumnId());
    secondsStream->set_length(secondsRleCoder->getStreamSize());

    nanoRleCoder->flushToStream(out);
    ::orc::proto::Stream *nanoStream = stripeFooter->add_streams();
    nanoStream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_SECONDARY);
    nanoStream->set_column(this->getColumnId());
    nanoStream->set_length(nanoRleCoder->getStreamSize());

    stripeFooter->add_columns()->set_kind(getProtoColumnEncoding());

    // clear the buffers
    secondsRleCoder->reset();
    nanoRleCoder->reset();

    stripeColStats->reset();
  }

  uint64_t getEstimatedSpaceNeeded() override {
    return secondsRleCoder->getEstimatedSpaceNeeded() +
           nanoRleCoder->getEstimatedSpaceNeeded() +
           ColumnWriter::getEstimatedSpaceNeeded();
  }

  orc::proto::ColumnEncoding_Kind getProtoColumnEncoding() override {
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

  void addTypeToFooter(proto::Footer *footer) override {
    proto::Type *type = footer->add_types();
    type->set_kind(::orc::proto::Type_Kind::Type_Kind_TIMESTAMP);
  }

 protected:
  std::unique_ptr<RleCoder> secondsRleCoder;
  std::unique_ptr<RleCoder> nanoRleCoder;

 private:
  void writeStats(dbcommon::Vector *vector) {
    uint64_t numValues = vector->getNumOfRows();
    const char *vals = vector->getValue();
    const char *nanos = vector->getNanoseconds();

    for (uint64_t i = 0; i < numValues; i++) {
      if (!vector->isNull(i)) {
        int64_t milli = reinterpret_cast<const int64_t *>(vals)[i] * 1000 +
                        reinterpret_cast<const int64_t *>(nanos)[i] / 1000000;
        if (milli < 0 && reinterpret_cast<const int64_t *>(nanos)[i] > 0)
          milli -= 1000;
        dynamic_cast<TimestampColumnStatisticsImpl *>(stripeColStats.get())
            ->updateTimestamp(milli);
        if (createBloomFilter) bloomFilter->addInt(milli);
      }
    }
  }

  uint64_t formatNanos(uint64_t nanos) {
    if (nanos == 0) {
      return 0;
    } else if (nanos % 100 != 0) {
      return nanos << 3;
    } else {
      nanos /= 100;
      int trailingZeros = 1;
      while (nanos % 10 == 0 && trailingZeros < 7) {
        nanos /= 10;
        trailingZeros += 1;
      }
      return nanos << 3 | trailingZeros;
    }
  }
};

class StructColumnWriter : public ColumnWriter {
 public:
  explicit StructColumnWriter(const orc::Type *type, WriterOptions *options)
      : ColumnWriter(type, options) {
    createBloomFilter = false;
  }

  virtual ~StructColumnWriter() {}

  void writeVector(dbcommon::Vector *vector) override {
    dbcommon::StructVector *svect =
        dynamic_cast<dbcommon::StructVector *>(vector);
    assert(svect != nullptr);
    uint64_t nChild = svect->getChildSize();
    for (uint64_t i = 0; i < nChild; i++) {
      dbcommon::Vector *v = svect->getChildVector(i);
      childWriters[i]->writeVector(v);
    }
  }

  void writeStripe(proto::StripeFooter *stripeFooter,
                   proto::StripeStatistics *pb, OutputStream *out) override {
    assert(stripeFooter != nullptr);

    ColumnWriter::writeStripe(stripeFooter, pb, out);

    // StructColumnReader needs this, must be ColumnEncoding_Kind_DIRECT
    stripeFooter->add_columns()->set_kind(
        ::orc::proto::ColumnEncoding_Kind_DIRECT);

    // Here, by default, we do not add PRESENT stream for struct type

    // Write child stipes
    uint64_t nChild = childWriters.size();
    for (uint64_t i = 0; i < nChild; i++) {
      childWriters[i]->writeStripe(stripeFooter, pb, out);
    }

    // clear the Buffer
    // If we add PRESENT stream, we also need to clear the buffer.
  }

  // WE DONT COUNT PRESENT stream for struct type
  uint64_t getEstimatedSpaceNeeded() override {
    uint64_t sz = 0;
    uint64_t nChild = childWriters.size();
    for (uint64_t i = 0; i < nChild; i++) {
      sz += childWriters[i]->getEstimatedSpaceNeeded();
    }
    return sz;
  }

  void addTypeToFooter(proto::Footer *footer) override {
    ::orc::proto::Type *typ = footer->add_types();
    typ->set_kind(::orc::proto::Type_Kind::Type_Kind_STRUCT);

    uint64_t nChild = childWriters.size();
    for (uint64_t i = 0; i < nChild; i++) {
      typ->add_fieldnames(this->type->getFieldName(i));
      typ->add_subtypes(this->getChildWriter(i)->getColumnId());
    }

    for (uint64_t i = 0; i < nChild; i++) {
      this->getChildWriter(i)->addTypeToFooter(footer);
    }
  }

  void addBloomFilterEntry() override {
    uint64_t nChild = childWriters.size();
    for (uint64_t i = 0; i < nChild; i++) {
      childWriters[i]->addBloomFilterEntry();
    }
  }

  void writeFileStatistics(proto::Footer *fileFooter) override {
    if (!writeStatsOn) return;
    ColumnWriter::writeFileStatistics(fileFooter);
    uint64_t nChild = childWriters.size();
    for (uint64_t i = 0; i < nChild; i++) {
      childWriters[i]->writeFileStatistics(fileFooter);
    }
  }
};

class ListColumnWriter : public ColumnWriter {
 public:
  explicit ListColumnWriter(const orc::Type *type, WriterOptions *options)
      : ColumnWriter(type, options) {
    lengthRleCoder =
        createRleCoder(false, options->getRleVersion(), ORCTypeKind::LONG,
                       options->getCompressionKind());
  }

  virtual ~ListColumnWriter() {}

  void writeVector(dbcommon::Vector *vector) override {
    ColumnWriter::writeVector(vector);

    std::unique_ptr<dbcommon::ByteBuffer> buf =
        vector->getNullBuffer()->getReverseBools();

    dbcommon::ListVector *lvector =
        dynamic_cast<dbcommon::ListVector *>(vector);
    uint64_t *offsets = const_cast<uint64_t *>(lvector->getOffsets());
    const char *notnulls = reinterpret_cast<const char *>(buf->data());

    uint64_t currentLength = 0;
    for (uint64_t i = 0; i < vector->getNumOfRows(); i++) {
      currentLength = offsets[i + 1] - offsets[i];
      lengthRleCoder->write(&currentLength, 1, &notnulls[i]);
      if (createBloomFilter) bloomFilter->addInt(currentLength);
    }
    childWriters[0]->writeVector(vector->getChildVector(0));
  }

  void writeStripe(proto::StripeFooter *stripeFooter,
                   proto::StripeStatistics *pb, OutputStream *out) override {
    assert(stripeFooter != nullptr);

    ColumnWriter::writeStripe(stripeFooter, pb, out);

    lengthRleCoder->flushToStream(out);
    ::orc::proto::Stream *lengthStream = stripeFooter->add_streams();
    lengthStream->set_kind(::orc::proto::Stream_Kind::Stream_Kind_LENGTH);
    lengthStream->set_column(this->getColumnId());
    lengthStream->set_length(lengthRleCoder->getStreamSize());

    stripeFooter->add_columns()->set_kind(getProtoColumnEncoding());

    // Write child stipes
    uint64_t nChild = childWriters.size();
    for (uint64_t i = 0; i < nChild; i++) {
      childWriters[i]->writeStripe(stripeFooter, pb, out);
    }

    lengthRleCoder->reset();
  }

  uint64_t getEstimatedSpaceNeeded() override {
    uint64_t sz = lengthRleCoder->getEstimatedSpaceNeeded();
    uint64_t nChild = childWriters.size();
    for (uint64_t i = 0; i < nChild; i++) {
      sz += childWriters[i]->getEstimatedSpaceNeeded();
    }
    return sz;
  }

  orc::proto::ColumnEncoding_Kind getProtoColumnEncoding() override {
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

  void addTypeToFooter(proto::Footer *footer) override {
    ::orc::proto::Type *typ = footer->add_types();
    typ->set_kind(::orc::proto::Type_Kind::Type_Kind_LIST);

    uint64_t nChild = childWriters.size();
    for (uint64_t i = 0; i < nChild; i++) {
      typ->add_fieldnames(this->type->getFieldName(i));
      typ->add_subtypes(this->getChildWriter(i)->getColumnId());
    }

    for (uint64_t i = 0; i < nChild; i++) {
      this->getChildWriter(i)->addTypeToFooter(footer);
    }
  }

  void writeFileStatistics(proto::Footer *fileFooter) override {
    if (!writeStatsOn) return;
    ColumnWriter::writeFileStatistics(fileFooter);
    uint64_t nChild = childWriters.size();
    for (uint64_t i = 0; i < nChild; i++) {
      childWriters[i]->writeFileStatistics(fileFooter);
    }
  }

 private:
  std::unique_ptr<RleCoder> lengthRleCoder;
};

class Decimal64ColumnWriter : public ColumnWriter {
 public:
  static const uint32_t MAX_PRECISION_64 = 18;
  static const uint32_t MAX_PRECISION_128 = 38;
  Decimal64ColumnWriter(const orc::Type *type, WriterOptions *options);
  void writeVector(dbcommon::Vector *vector) override;
  void writeStripe(proto::StripeFooter *stripeFooter,
                   proto::StripeStatistics *pb, OutputStream *out) override;
  uint64_t getEstimatedSpaceNeeded() override;
  void addTypeToFooter(proto::Footer *footer) override;

 private:
  orc::proto::ColumnEncoding_Kind getProtoColumnEncoding() override;
  void writeInt64(int64_t value);
  uint64_t zigzagEncodeInt64(int64_t value) {
    return (value << 1) ^ (value >> 63);
  }
  std::unique_ptr<SeekableOutputStream> dataBufferedStream;
  std::unique_ptr<RleCoder> scaleRleCoder;
  int32_t precision;
  int32_t scale;
};

class Decimal128ColumnWriter : public ColumnWriter {
 public:
  Decimal128ColumnWriter(const orc::Type *type, WriterOptions *options);
  void writeVector(dbcommon::Vector *vector) override;
  void writeStripe(proto::StripeFooter *stripeFooter,
                   proto::StripeStatistics *pb, OutputStream *out) override;
  uint64_t getEstimatedSpaceNeeded() override;
  void addTypeToFooter(proto::Footer *footer) override;

 private:
  orc::proto::ColumnEncoding_Kind getProtoColumnEncoding() override;
  void writeInt128(orc::Int128 *value);
  void zigzagEncodeInt128(orc::Int128 *value);
  std::unique_ptr<SeekableOutputStream> dataBufferedStream;
  std::unique_ptr<RleCoder> scaleRleCoder;
  int32_t precision;
  int32_t scale;
};

// The interface for writing ORC files.
class Writer {
 public:
  Writer() {}
  virtual ~Writer() {}

  // Get the schema for this writer
  // @return the file schema
  virtual Type *getSchema() = 0;

  // Add arbitrary meta-data to the ORC file. This may be called at any point
  // until the Writer is closed. If the same key is passed a second time, the
  // second value will replace the first.
  // @param key A key to label the data with.
  // @param value The contents of the metadata.
  // @return Void
  virtual void addUserMetadata(const std::string &key,
                               const std::string &value) = 0;

  // Add a tuple batch to the ORC file.
  // @param tb The tuple batch
  // @return Void
  virtual void addTupleBatch(dbcommon::TupleBatch *tb) = 0;

  // Begin the write. Write the header.
  // @return Void
  virtual void begin() = 0;

  // Flush all of the buffers and close the file. No methods on this writer
  // should be called afterwards.
  // @return Void
  virtual void end() = 0;

  // Return the deserialized data size. Raw data size will be compute when
  // writing the file footer. Hence raw data size value will be available only
  // after closing the writer.
  // @return raw data size
  virtual uint64_t getRawDataSize() = 0;

  // Return the number of rows in file. Row count gets updated when flushing
  // the stripes. To get accurate row count this method should be called after
  // closing the writer.
  // @return Row count
  virtual uint64_t getNumberOfRows() = 0;

  // Write an intermediate footer on the file such that if the file is
  // truncated to the returned offset, it would be a valid ORC file.
  // @return the offset that would be a valid end location for an ORC file
  virtual uint64_t writeIntermediateFooter() = 0;

  // Fast stripe append to ORC file. This interface is used for fast ORC file
  // merge with other ORC files. When merging, the file to be merged should
  // pass
  // stripe in binary form along with stripe information and stripe
  // statistics.
  // After appending last stripe of a file, use appendUserMetadata() to append
  // any user metadata.
  // @param stripe  Stripe as byte array
  // @param offset  Offset within byte array
  // @param length  Length of stripe within byte array
  // @param stripeInfo  Stripe information
  // @param stripeStatistics  Stripe statistics (Protobuf objects can be
  //                         merged directly)
  // @return Void
  virtual void appendStripe(
      char *stripe, uint32_t offset, uint32_t length,
      const orc::StripeInformation &stripeInfo,
      const orc::proto::StripeStatistics &stripeStatistics) = 0;

  // When fast stripe append is used for merging ORC stripes, after appending
  // the last stripe from a file, this interface must be used to merge any
  // user metadata.
  // @param userMetadata - user metadata
  // @return Void
  virtual void appendUserMetadata(
      const std::list<orc::proto::UserMetadataItem> &userMetadata) = 0;
};

class WriterImpl : public Writer {
 public:
  WriterImpl(std::unique_ptr<OutputStream> stream, WriterOptions *options)
      : outStream(std::move(stream)),
        options(options),
        adjustedStripeSize(options->getStripeSize()) {
    orc::Type *type = options->getSchema();
    assert(type->getKind() == orc::ORCTypeKind::STRUCT);

    stripeFooter.reset(new proto::StripeFooter());
    fileFooter.reset(new proto::Footer);
    fileMetadata.reset(new proto::Metadata);
    postscript.reset(new proto::PostScript);

    // sfos.reset(new SeekableFileOutputStream(outStream.get()));
    columnWriter = NULL;

    compressor = createBlockCompressor(options->getCompressionKind());
  }

  virtual ~WriterImpl() {}

  Type *getSchema() override { return options->getSchema(); }

  void addUserMetadata(const std::string &key,
                       const std::string &value) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "WriterImpl::addUserMetadata not implemented");
  }

  void addTupleBatch(dbcommon::TupleBatch *tb) override {
    writeTupleBatch(tb);
    if (columnWriter->getEstimatedSpaceNeeded() * scale > adjustedStripeSize) {
      writeCurrentStrip();
      startNewStripe();
    }
  }

  void begin() override { this->numRowsInCurrentStripe = 0; }

  void end() override {
    // write the last stripe to disk
    if (this->numRowsInCurrentStripe > 0) {
      writeCurrentStrip();
    }

    this->writeFileMetadata();
    this->writeFileFooter(outStream->getPosition() - HEADER_LENGTH);
    this->writePostScript();

    // need to close, otherwise, another following reader might not
    // get the latest update for this write.
    this->outStream->close();
  }

 private:
  void writeTupleBatch(dbcommon::TupleBatch *tb);

  void startNewStripe();

  void completeStripInfo(uint64_t stripFooterLen);

  void writeCurrentStrip();

  void writeHeader();

  uint64_t writeStripeFooter();

  void writeFileFooter(uint64_t bodyLength);

  void writeFileMetadata();

  uint64_t getStripeDataLength(proto::StripeFooter *footer);

  void writePostScript();

  uint64_t getRawDataSize() override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "WriterImpl::getRawDataSize not implemented");
  }

  uint64_t getNumberOfRows() override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "WriterImpl::getNumberOfRows not implemented");
  }

  uint64_t writeIntermediateFooter() override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "WriterImpl::writeIntermediateFooter not implemented");
  }

  void appendStripe(
      char *stripe, uint32_t offset, uint32_t length,
      const orc::StripeInformation &stripeInfo,
      const orc::proto::StripeStatistics &stripeStatistics) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "WriterImpl::appendStripe not implemented");
  }

  void appendUserMetadata(
      const std::list<orc::proto::UserMetadataItem> &userMetadata) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "WriterImpl::appendUserMetadata not implemented");
  }

  void padStripe();

 private:
  std::unique_ptr<OutputStream> outStream;
  std::unique_ptr<SeekableOutputStream> sfos;

  WriterOptions *options;

  std::unique_ptr<proto::StripeFooter> stripeFooter;
  std::unique_ptr<proto::Footer> fileFooter;
  uint64_t fileFooterLen = 0;
  std::unique_ptr<proto::Metadata> fileMetadata;
  uint64_t fileMetadataLen = 0;
  std::unique_ptr<proto::PostScript> postscript;

  std::unique_ptr<ColumnWriter> columnWriter;

  std::unique_ptr<orc::SeekableOutputStream> compressor;

  ::orc::proto::StripeInformation *currentStripe = nullptr;
  uint64_t stripeStart = 0;
  uint64_t numRowsInCurrentStripe = 0;
  uint64_t totalRows = 0;
  uint64_t adjustedStripeSize = 0;
  double scale = 1;

  const char *magicId = "ORC";
  const uint32_t HEADER_LENGTH = 3;
  const WriterId writerId = WriterId::ORC_CPP_WRITER;
};

// Create a writer for the ORC file.
// @param stream The stream to write
// @param options The options for writing the file
std::unique_ptr<Writer> createWriter(std::unique_ptr<OutputStream> stream,
                                     WriterOptions *options);

}  // end of namespace orc

#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_WRITER_H_
