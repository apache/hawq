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
#include "dbcommon/utils/comp/snappy-compressor.h"

namespace orc {

std::unique_ptr<Writer> createWriter(std::unique_ptr<OutputStream> stream,
                                     WriterOptions* options) {
  std::unique_ptr<WriterImpl> writer(
      new WriterImpl(std::move(stream), options));

  return std::move(writer);
}

std::unique_ptr<ColumnWriter> ColumnWriter::buildColumnWriter(
    const orc::Type* type, WriterOptions* options) {
  std::unique_ptr<ColumnWriter> cw;

  switch (type->getKind()) {
    case orc::ORCTypeKind::STRUCT: {
      cw.reset(new StructColumnWriter(type, options));
      for (uint32_t i = 0; i < type->getSubtypeCount(); i++) {
        cw->addChildWriter(buildColumnWriter(type->getSubtype(i), options));
      }
      break;
    }
    case orc::ORCTypeKind::LIST: {
      cw.reset(new ListColumnWriter(type, options));
      cw->addChildWriter(buildColumnWriter(type->getSubtype(0), options));
      break;
    }
    case orc::ORCTypeKind::BYTE: {
      cw.reset(new ByteColumnWriter(type, options));
      break;
    }
    case orc::ORCTypeKind::INT: {
      cw.reset(new IntColumnWriter(type, options));
      break;
    }
    case orc::ORCTypeKind::LONG: {
      cw.reset(new LongColumnWriter(type, options));
      break;
    }
    case orc::ORCTypeKind::SHORT: {
      cw.reset(new ShortColumnWriter(type, options));
      break;
    }
    case orc::ORCTypeKind::FLOAT:
    case orc::ORCTypeKind::DOUBLE: {
      cw.reset(new DoubleColumnWriter(type, options));
      break;
    }
    case orc::ORCTypeKind::STRING:
    case orc::ORCTypeKind::VARCHAR:
    case orc::ORCTypeKind::CHAR: {
      cw.reset(new StringColumnWriter(type, options));
      break;
    }
    case orc::ORCTypeKind::BOOLEAN: {
      cw.reset(new BooleanColumnWriter(type, options));
      break;
    }
    case orc::ORCTypeKind::DATE: {
      cw.reset(new DateColumnWriter(type, options));
      break;
    }
    case orc::ORCTypeKind::TIME: {
      cw.reset(new TimeColumnWriter(type, options));
      break;
    }
    case orc::ORCTypeKind::BINARY: {
      cw.reset(new BinaryColumnWriter(type, options));
      break;
    }
    case orc::ORCTypeKind::TIMESTAMP: {
      cw.reset(new TimestampColumnWriter(type, options));
      break;
    }
    case orc::ORCTypeKind::DECIMAL: {
      if (type->getPrecision() <= Decimal64ColumnWriter::MAX_PRECISION_64)
        cw.reset(new Decimal64ColumnWriter(type, options));
      else
        cw.reset(new Decimal128ColumnWriter(type, options));
      break;
    }
    default:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "type %d not supported",
                type->getKind());
  }
  return std::move(cw);
}

void WriterImpl::writePostScript() {
  postscript->set_compression(options->getProtoCompressionKind());
  postscript->set_compressionblocksize(options->getCompressionBlockSize());
  postscript->set_footerlength(this->fileFooterLen);
  postscript->set_metadatalength(this->fileMetadataLen);
  postscript->add_version(options->getFileVersion().getMajor());
  postscript->add_version(options->getFileVersion().getMinor());
  postscript->set_writerversion(WriterVersion_ORC_135);
  postscript->set_magic(this->magicId);
  std::string buffer = postscript->SerializeAsString();

  if (!(reinterpret_cast<GeneralFileOutputStream*>(outStream.get()))
           ->fileopen())
    this->writeHeader();
  this->outStream->write(const_cast<char*>(buffer.data()), buffer.size());
  // LOG_INFO("postscript size %lu", buffer.size());

  uint8_t postscriptLen = buffer.size();
  this->outStream->write(&postscriptLen, sizeof(uint8_t));

  // LOG_INFO("last byte %d", 1);
}

void WriterImpl::writeFileFooter(uint64_t bodyLength) {
  fileFooter->set_headerlength(HEADER_LENGTH);
  fileFooter->set_contentlength(bodyLength);
  fileFooter->set_numberofrows(this->totalRows);
  fileFooter->set_writer(this->writerId);
  fileFooter->set_rowindexstride(
      static_cast<uint32_t>(options->getRowIndexStride()));
  // Add types
  columnWriter->addTypeToFooter(fileFooter.get());
  // Write file statistics
  columnWriter->writeFileStatistics(fileFooter.get());
  // Write the file footer
  std::string buffer = fileFooter->SerializeAsString();
  compressor->write(buffer.data(), buffer.size());
  compressor->flushToStream(this->outStream.get());

  fileFooterLen = compressor->getStreamSize();

  compressor->reset();

  // LOG_INFO("file footer size: before compression %lu after compression %llu",
  //    buffer.size(), fileFooterLen);
}

void WriterImpl::writeFileMetadata() {
  std::string buffer = fileMetadata->SerializeAsString();
  compressor->write(buffer.data(), buffer.size());
  compressor->flushToStream(this->outStream.get());

  fileMetadataLen = compressor->getStreamSize();

  compressor->reset();
}

void WriterImpl::writeTupleBatch(dbcommon::TupleBatch* tb) {
  uint32_t nCols = tb->getNumOfColumns();
  for (uint32_t i = 0; i < nCols; i++) {
    dbcommon::Vector* vec = tb->getColumn(i);
    if (!columnWriter) {
      orc::Type* type = options->getSchema();
      columnWriter = ColumnWriter::buildColumnWriter(type, options);
    }
    columnWriter->getChildWriter(i)->writeVector(vec);
  }
  this->numRowsInCurrentStripe += tb->getNumOfRows();
  if (numRowsInCurrentStripe > 0 &&
      numRowsInCurrentStripe % options->getRowIndexStride() == 0)
    columnWriter->addBloomFilterEntry();
}

uint64_t WriterImpl::getStripeDataLength(proto::StripeFooter* footer) {
  uint32_t sz = footer->streams_size();
  uint64_t len = 0;
  for (uint32_t i = 0; i < sz; i++) {
    len += footer->streams(i).length();
  }
  return len;
}

uint64_t WriterImpl::writeStripeFooter() {
  std::string buffer = stripeFooter->SerializeAsString();
  compressor->write(buffer.data(), buffer.size());
  compressor->flushToStream(this->outStream.get());

  // LOG_INFO("strip footer size %lu %d after compression sz %lld",
  //    buffer.size(), stripeFooter->ByteSize(),
  //    compressor->getStreamSize());

  uint64_t sfooterLen = compressor->getStreamSize();
  compressor->reset();

  return sfooterLen;
}

void WriterImpl::writeHeader() {
  std::string buffer(this->magicId);
  this->outStream->write(const_cast<char*>(buffer.data()), buffer.size());
  this->stripeStart = this->outStream->getPosition();
}

void WriterImpl::writeCurrentStrip() {
  if (!(reinterpret_cast<GeneralFileOutputStream*>(outStream.get()))
           ->fileopen())
    this->writeHeader();
  if (numRowsInCurrentStripe % options->getRowIndexStride() != 0)
    columnWriter->addBloomFilterEntry();
  proto::StripeStatistics* stats = fileMetadata->add_stripestats();
  columnWriter->writeStripe(stripeFooter.get(), stats, this->outStream.get());
  uint64_t stripFooterLen = this->writeStripeFooter();
  completeStripInfo(stripFooterLen);
  this->totalRows += this->numRowsInCurrentStripe;
  this->stripeFooter->Clear();
  padStripe();
}

void WriterImpl::completeStripInfo(uint64_t stripFooterLen) {
  currentStripe = fileFooter->add_stripes();
  currentStripe->set_offset(this->stripeStart);
  currentStripe->set_datalength(getStripeDataLength(this->stripeFooter.get()));
  currentStripe->set_footerlength(stripFooterLen);
  currentStripe->set_indexlength(0);
  currentStripe->set_numberofrows(this->numRowsInCurrentStripe);
  // LOG_INFO("stripe info: offset %lld, datalength %lld, "
  //    "footerlength %lld, indexlength %lld, #row %lld",
  // currentStripe->offset(),
  //    currentStripe->datalength(), currentStripe->footerlength(),
  //    currentStripe->indexlength(), currentStripe->numberofrows());
}

void WriterImpl::startNewStripe() {
  this->stripeStart = this->outStream->getPosition();
  this->numRowsInCurrentStripe = 0;
}

void WriterImpl::padStripe() {
  uint64_t currentStripeSize =
      this->outStream->getPosition() - this->stripeStart;
  uint64_t available =
      this->options->getBlockSize() -
      (this->outStream->getPosition() % this->options->getBlockSize());
  uint64_t defaultStripeSize = this->options->getStripeSize();
  uint64_t overflow = currentStripeSize - adjustedStripeSize;
  if (scale == 1)
    scale = static_cast<double>(currentStripeSize) /
            static_cast<double>(defaultStripeSize);
  double availRatio =
      static_cast<double>(available) / static_cast<double>(defaultStripeSize);
  double paddingTolerance = this->options->getPaddingTolerance();
  if (availRatio > 0.0 && availRatio < 1.0 && availRatio > paddingTolerance) {
    double correction = overflow > 0
                            ? static_cast<double>(overflow) /
                                  static_cast<double>(adjustedStripeSize)
                            : 0.0;
    correction = correction > paddingTolerance ? paddingTolerance : correction;
    adjustedStripeSize = static_cast<uint64_t>(
        floor((1.0 - correction) * (availRatio * defaultStripeSize)));
  } else if (availRatio >= 1.0) {
    adjustedStripeSize = defaultStripeSize;
  }
  if (availRatio < paddingTolerance) {
    this->outStream->padding(available);
    adjustedStripeSize = defaultStripeSize;
    LOG_INFO(
        "ORC stripe size: %llu bytes, block available: %llu bytes, "
        "adjustedStripeSize: %llu bytes, padding then",
        currentStripeSize, available, adjustedStripeSize);
  } else {
    LOG_INFO(
        "ORC stripe size: %llu bytes, block available: %llu bytes, "
        "adjustedStripeSize: %llu bytes, with no padding",
        currentStripeSize, available, adjustedStripeSize);
  }
}

}  // end of namespace orc
