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

#include <google/protobuf/io/coded_stream.h>

#include <math.h>
#include <algorithm>
#include <iostream>
#include <iterator>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "storage/format/orc/byte-rle.h"
#include "storage/format/orc/exceptions.h"
#include "storage/format/orc/input-stream.h"
#include "storage/format/orc/int128.h"
#include "storage/format/orc/orc-predicates.h"
#include "storage/format/orc/reader.h"
#include "storage/format/orc/rle.h"
#include "storage/format/orc/type-impl.h"

namespace orc {

enum ColumnSelection {
  ColumnSelection_NONE = 0,
  ColumnSelection_NAMES = 1,
  ColumnSelection_FIELD_IDS = 2,
  ColumnSelection_TYPE_IDS = 3
};

struct ReaderOptionsPrivate {
  ColumnSelection selection;
  std::list<uint64_t> includedColumnIndexes;
  std::list<std::string> includedColumnNames;
  uint64_t dataStart;
  uint64_t dataLength;
  uint64_t tailLocation;
  bool throwOnHive11DecimalOverflow;
  int32_t forcedScaleOnHive11Decimal;
  std::ostream* errorStream;
  dbcommon::MemoryPool* memoryPool;
  std::string serializedTail;
  const univplan::UnivPlanExprPolyList* predicateExprs;
  const dbcommon::TupleDesc* td;
  bool readStatisticsOnly;

  ReaderOptionsPrivate() {
    selection = ColumnSelection_NONE;
    dataStart = 0;
    dataLength = std::numeric_limits<uint64_t>::max();
    tailLocation = std::numeric_limits<uint64_t>::max();
    throwOnHive11DecimalOverflow = true;
    forcedScaleOnHive11Decimal = 6;
    errorStream = &std::cerr;
    memoryPool = dbcommon::getDefaultPool();
    predicateExprs = nullptr;
    td = nullptr;
    readStatisticsOnly = false;
  }
};

ReaderOptions::ReaderOptions()
    : privateBits(
          std::unique_ptr<ReaderOptionsPrivate>(new ReaderOptionsPrivate())) {
  // PASS
}

ReaderOptions::ReaderOptions(const ReaderOptions& rhs)
    : privateBits(std::unique_ptr<ReaderOptionsPrivate>(
          new ReaderOptionsPrivate(*(rhs.privateBits.get())))) {
  // PASS
}

ReaderOptions::ReaderOptions(ReaderOptions& rhs) {
  // swap privateBits with rhs
  ReaderOptionsPrivate* l = privateBits.release();
  privateBits.reset(rhs.privateBits.release());
  rhs.privateBits.reset(l);
}

ReaderOptions& ReaderOptions::operator=(const ReaderOptions& rhs) {
  if (this != &rhs) {
    privateBits.reset(new ReaderOptionsPrivate(*(rhs.privateBits.get())));
  }
  return *this;
}

ReaderOptions::~ReaderOptions() {
  // PASS
}

ReaderOptions& ReaderOptions::include(const std::list<uint64_t>& include) {
  privateBits->selection = ColumnSelection_FIELD_IDS;
  privateBits->includedColumnIndexes.assign(include.begin(), include.end());
  privateBits->includedColumnNames.clear();
  return *this;
}

ReaderOptions& ReaderOptions::include(const std::list<std::string>& include) {
  privateBits->selection = ColumnSelection_NAMES;
  privateBits->includedColumnNames.assign(include.begin(), include.end());
  privateBits->includedColumnIndexes.clear();
  return *this;
}

ReaderOptions& ReaderOptions::includeTypes(const std::list<uint64_t>& types) {
  privateBits->selection = ColumnSelection_TYPE_IDS;
  privateBits->includedColumnIndexes.assign(types.begin(), types.end());
  privateBits->includedColumnNames.clear();
  return *this;
}

ReaderOptions& ReaderOptions::range(uint64_t offset, uint64_t length) {
  privateBits->dataStart = offset;
  privateBits->dataLength = length;
  return *this;
}

ReaderOptions& ReaderOptions::setTailLocation(uint64_t offset) {
  privateBits->tailLocation = offset;
  return *this;
}

ReaderOptions& ReaderOptions::setSerializedFileTail(const std::string& value) {
  privateBits->serializedTail = value;
  return *this;
}

dbcommon::MemoryPool* ReaderOptions::getMemoryPool() const {
  return privateBits->memoryPool;
}

bool ReaderOptions::getIndexesSet() const {
  return privateBits->selection == ColumnSelection_FIELD_IDS;
}

bool ReaderOptions::getTypeIdsSet() const {
  return privateBits->selection == ColumnSelection_TYPE_IDS;
}

const std::list<uint64_t>& ReaderOptions::getInclude() const {
  return privateBits->includedColumnIndexes;
}

bool ReaderOptions::getNamesSet() const {
  return privateBits->selection == ColumnSelection_NAMES;
}

const std::list<std::string>& ReaderOptions::getIncludeNames() const {
  return privateBits->includedColumnNames;
}

uint64_t ReaderOptions::getOffset() const { return privateBits->dataStart; }

uint64_t ReaderOptions::getLength() const { return privateBits->dataLength; }

uint64_t ReaderOptions::getTailLocation() const {
  return privateBits->tailLocation;
}

ReaderOptions& ReaderOptions::throwOnHive11DecimalOverflow(bool shouldThrow) {
  privateBits->throwOnHive11DecimalOverflow = shouldThrow;
  return *this;
}

bool ReaderOptions::getThrowOnHive11DecimalOverflow() const {
  return privateBits->throwOnHive11DecimalOverflow;
}

ReaderOptions& ReaderOptions::forcedScaleOnHive11Decimal(int32_t forcedScale) {
  privateBits->forcedScaleOnHive11Decimal = forcedScale;
  return *this;
}

int32_t ReaderOptions::getForcedScaleOnHive11Decimal() const {
  return privateBits->forcedScaleOnHive11Decimal;
}

ReaderOptions& ReaderOptions::setErrorStream(std::ostream& stream) {
  privateBits->errorStream = &stream;
  return *this;
}

std::ostream* ReaderOptions::getErrorStream() const {
  return privateBits->errorStream;
}

std::string ReaderOptions::getSerializedFileTail() const {
  return privateBits->serializedTail;
}

void ReaderOptions::setPredicateExprs(
    const univplan::UnivPlanExprPolyList* predicateExprs) {
  privateBits->predicateExprs = predicateExprs;
}

const univplan::UnivPlanExprPolyList* ReaderOptions::getPredicateExprs() const {
  return privateBits->predicateExprs;
}

void ReaderOptions::setTupleDesc(const dbcommon::TupleDesc* td) {
  privateBits->td = td;
}

const dbcommon::TupleDesc* ReaderOptions::getTupleDesc() const {
  return privateBits->td;
}

void ReaderOptions::setReadStatsOnlyFlag(bool readStatsOnly) {
  privateBits->readStatisticsOnly = readStatsOnly;
}

bool ReaderOptions::readStatsOnly() const {
  return privateBits->readStatisticsOnly;
}

Reader::~Reader() {
  // PASS
}

static const uint64_t DIRECTORY_SIZE_GUESS = 16 * 1024;

uint64_t getCompressionBlockSize(const proto::PostScript& ps) {
  if (ps.has_compressionblocksize()) {
    return ps.compressionblocksize();
  } else {
    return 256 * 1024;
  }
}

CompressionKind convertCompressionKind(const proto::PostScript& ps) {
  if (ps.has_compression()) {
    return static_cast<CompressionKind>(ps.compression());
  } else {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Unknown compression type");
  }
}

void readFully(char* buffer, int64_t bufferSize, SeekableInputStream* stream) {
  int64_t posn = 0;
  while (posn < bufferSize) {
    const void* chunk;
    int length;
    if (!stream->Next(&chunk, &length)) {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "bad read in readFully");
    }
    memcpy(buffer + posn, chunk, static_cast<size_t>(length));
    posn += length;
  }
}

ReaderImpl::ReaderImpl(std::unique_ptr<InputStream> input,
                       const ReaderOptions& opts,
                       std::unique_ptr<proto::PostScript> _postscript,
                       std::unique_ptr<proto::Footer> _footer,
                       uint64_t _fileLength, uint64_t _postscriptLength)
    : localTimezone(getLocalTimezone()),
      stream(std::move(input)),
      options(opts),
      fileLength(_fileLength),
      postscriptLength(_postscriptLength),
      postscript(std::move(_postscript)),
      memoryPool(*opts.getMemoryPool()),
      blockSize(getCompressionBlockSize(*postscript)),
      compression(convertCompressionKind(*postscript)),
      footer(std::move(_footer)),
      firstRowOfStripe(memoryPool, 0) {
  isMetadataLoaded = false;
  checkOrcVersion();
  numberOfStripes = static_cast<uint64_t>(footer->stripes_size());
  currentStripe = static_cast<uint64_t>(footer->stripes_size());
  lastStripe = 0;
  currentRowInStripe = 0;
  uint64_t rowTotal = 0;

  firstRowOfStripe.resize(static_cast<uint64_t>(footer->stripes_size()));
  for (size_t i = 0; i < static_cast<size_t>(footer->stripes_size()); ++i) {
    firstRowOfStripe[i] = rowTotal;
    proto::StripeInformation stripeInfo = footer->stripes(static_cast<int>(i));
    rowTotal += stripeInfo.numberofrows();
    bool isStripeInRange =
        stripeInfo.offset() >= opts.getOffset() &&
        stripeInfo.offset() < opts.getOffset() + opts.getLength();
    if (isStripeInRange) {
      if (i < currentStripe) {
        currentStripe = i;
      }
      if (i >= lastStripe) {
        lastStripe = i + 1;
      }
      // read all stripe footer in the range
      if (!options.readStatsOnly())
        stripeFooters.push_back(getStripeFooter(stripeInfo));
    }
  }
  firstStripe = currentStripe;

  if (currentStripe == 0) {
    previousRow = (std::numeric_limits<uint64_t>::max)();
  } else if (currentStripe == static_cast<uint64_t>(footer->stripes_size())) {
    previousRow = footer->numberofrows();
  } else {
    previousRow = firstRowOfStripe[firstStripe] - 1;
  }
  if (numberOfStripes) {
    schema = convertType(footer->types(0), *footer);
    std::vector<std::string> columns;
    buildTypeNameIdMap(schema.get(), columns);
    updateSelected();
  }
}

void ReaderImpl::updateSelected() {
  selectedColumns.assign(static_cast<size_t>(footer->types_size()), false);
  if (schema->getKind() == STRUCT && options.getIndexesSet()) {
    for (std::list<uint64_t>::const_iterator field =
             options.getInclude().begin();
         field != options.getInclude().end(); ++field) {
      updateSelectedByFieldId(*field);
    }
  } else if (schema->getKind() == STRUCT && options.getNamesSet()) {
    for (std::list<std::string>::const_iterator field =
             options.getIncludeNames().begin();
         field != options.getIncludeNames().end(); ++field) {
      updateSelectedByName(*field);
    }
  } else if (options.getTypeIdsSet()) {
    for (std::list<uint64_t>::const_iterator typeId =
             options.getInclude().begin();
         typeId != options.getInclude().end(); ++typeId) {
      updateSelectedByTypeId(*typeId);
    }
  } else {
    // default is to select all columns
    std::fill(selectedColumns.begin(), selectedColumns.end(), true);
  }
  selectParents(*schema);
  selectedColumns[0] = true;  // column 0 is selected by default
}

std::string ReaderImpl::getSerializedFileTail() const {
  proto::FileTail tail;
  proto::PostScript* mutable_ps = tail.mutable_postscript();
  mutable_ps->CopyFrom(*postscript);
  proto::Footer* mutableFooter = tail.mutable_footer();
  mutableFooter->CopyFrom(*footer);
  tail.set_filelength(fileLength);
  tail.set_postscriptlength(postscriptLength);
  std::string result;
  if (!tail.SerializeToString(&result)) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Failed to serialize file tail");
  }
  return result;
}

// Recurses over a type tree and build two maps
// map<TypeName, TypeId>, map<TypeId, Type>
void ReaderImpl::buildTypeNameIdMap(const Type* type,
                                    std::vector<std::string>& columns) {
  // map<type_id, Type*>
  idTypeMap[type->getColumnId()] = type;

  if (orc::STRUCT == type->getKind()) {
    for (size_t i = 0; i < type->getSubtypeCount(); ++i) {
      const std::string& fieldName = type->getFieldName(i);
      columns.push_back(fieldName);
      nameIdMap[toDotColumnPath(columns)] = type->getSubtype(i)->getColumnId();
      buildTypeNameIdMap(type->getSubtype(i), columns);
      columns.pop_back();
    }
  } else {
    // other non-primitive type
    for (size_t j = 0; j < type->getSubtypeCount(); ++j) {
      buildTypeNameIdMap(type->getSubtype(j), columns);
    }
  }
}

std::string ReaderImpl::toDotColumnPath(
    const std::vector<std::string>& columns) {
  if (columns.empty()) {
    return std::string();
  }
  std::ostringstream columnStream;
  std::copy(columns.begin(), columns.end(),
            std::ostream_iterator<std::string>(columnStream, "."));
  std::string columnPath = columnStream.str();
  return columnPath.substr(0, columnPath.length() - 1);
}

const ReaderOptions& ReaderImpl::getReaderOptions() const { return options; }

CompressionKind ReaderImpl::getCompression() const { return compression; }

uint64_t ReaderImpl::getCompressionSize() const { return blockSize; }

uint64_t ReaderImpl::getNumberOfStripes() const { return numberOfStripes; }

uint64_t ReaderImpl::getNumberOfStripeStatistics() const {
  if (!isMetadataLoaded) {
    readMetadata();
  }
  return metadata.get() == nullptr
             ? 0
             : static_cast<uint64_t>(metadata->stripestats_size());
}

std::unique_ptr<StripeInformation> ReaderImpl::getStripe(
    uint64_t stripeIndex) const {
  if (stripeIndex > getNumberOfStripes()) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "stripe index out of range");
  }
  proto::StripeInformation stripeInfo =
      footer->stripes(static_cast<int>(stripeIndex));

  return std::unique_ptr<StripeInformation>(new StripeInformationImpl(
      stripeInfo.offset(), stripeInfo.indexlength(), stripeInfo.datalength(),
      stripeInfo.footerlength(), stripeInfo.numberofrows(), stream.get(),
      memoryPool, compression, blockSize));
}

std::string ReaderImpl::getFormatVersion() const {
  std::stringstream result;
  for (int i = 0; i < postscript->version_size(); ++i) {
    if (i != 0) {
      result << ".";
    }
    result << postscript->version(i);
  }
  return result.str();
}

uint64_t ReaderImpl::getNumberOfRows() const { return footer->numberofrows(); }

WriterVersion ReaderImpl::getWriterVersion() const {
  if (!postscript->has_writerversion()) {
    return WriterVersion_ORIGINAL;
  }
  return static_cast<WriterVersion>(postscript->writerversion());
}

uint64_t ReaderImpl::getContentLength() const {
  return footer->contentlength();
}

uint64_t ReaderImpl::getStripeStatisticsLength() const {
  return postscript->metadatalength();
}

uint64_t ReaderImpl::getFileFooterLength() const {
  return postscript->footerlength();
}

uint64_t ReaderImpl::getFilePostscriptLength() const {
  return postscriptLength;
}

uint64_t ReaderImpl::getFileLength() const { return fileLength; }

uint64_t ReaderImpl::getRowIndexStride() const {
  return footer->rowindexstride();
}

const std::string& ReaderImpl::getStreamName() const {
  return stream->getName();
}

std::list<std::string> ReaderImpl::getMetadataKeys() const {
  std::list<std::string> result;
  for (int i = 0; i < footer->metadata_size(); ++i) {
    result.push_back(footer->metadata(i).name());
  }
  return result;
}

std::string ReaderImpl::getMetadataValue(const std::string& key) const {
  for (int i = 0; i < footer->metadata_size(); ++i) {
    if (footer->metadata(i).name() == key) {
      return footer->metadata(i).value();
    }
  }
  LOG_ERROR(ERRCODE_INTERNAL_ERROR, "key not found");
}

bool ReaderImpl::hasMetadataValue(const std::string& key) const {
  for (int i = 0; i < footer->metadata_size(); ++i) {
    if (footer->metadata(i).name() == key) {
      return true;
    }
  }
  return false;
}

const std::vector<bool> ReaderImpl::getSelectedColumns() const {
  return selectedColumns;
}

const Type& ReaderImpl::getType() const { return *(schema.get()); }

const Type& ReaderImpl::getSelectedType() const {
  if (selectedSchema.get() == nullptr) {
    selectedSchema = buildSelectedType(schema.get(), selectedColumns);
  }
  return *(selectedSchema.get());
}

uint64_t ReaderImpl::getRowNumber() const { return previousRow; }

std::unique_ptr<univplan::Statistics> ReaderImpl::getStatistics() const {
  return std::unique_ptr<univplan::Statistics>(
      new StatisticsImpl(*footer, hasCorrectStatistics()));
}

std::unique_ptr<univplan::ColumnStatistics> ReaderImpl::getColumnStatistics(
    uint32_t index) const {
  if (index >= static_cast<uint64_t>(footer->statistics_size())) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "column index out of range");
  }
  proto::ColumnStatistics col = footer->statistics(static_cast<int32_t>(index));
  return std::unique_ptr<univplan::ColumnStatistics>(
      convertColumnStatistics(col, hasCorrectStatistics()));
}

void ReaderImpl::readMetadata() const {
  uint64_t metadataSize = postscript->metadatalength();
  uint64_t metadataStart = fileLength - metadataSize -
                           postscript->footerlength() - postscriptLength - 1;
  if (metadataSize != 0) {
    std::unique_ptr<SeekableInputStream> pbStream = createDecompressor(
        compression,
        std::unique_ptr<SeekableInputStream>(new SeekableFileInputStream(
            stream.get(), metadataStart, metadataSize, memoryPool)),
        blockSize, memoryPool);
    metadata.reset(new proto::Metadata());
    if (!metadata->ParseFromZeroCopyStream(pbStream.get())) {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Failed to parse the metadata");
    }
  }
  isMetadataLoaded = true;
}

std::unique_ptr<univplan::Statistics> ReaderImpl::getStripeStatistics(
    uint64_t stripeIndex) const {
  if (!isMetadataLoaded) {
    readMetadata();
  }
  if (metadata.get() == nullptr) {
    return nullptr;
  }
  return std::unique_ptr<univplan::Statistics>(
      new StatisticsImpl(metadata->stripestats(static_cast<int>(stripeIndex)),
                         hasCorrectStatistics()));
}

void ReaderImpl::seekToRow(uint64_t rowNumber) {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
            "ReaderImpl::seekToRow not implemented");
}

bool ReaderImpl::hasCorrectStatistics() const {
  return getWriterVersion() != WriterVersion_ORIGINAL;
}

proto::StripeFooter ReaderImpl::getStripeFooter(
    const proto::StripeInformation& info) const {
  uint64_t stripeFooterStart =
      info.offset() + info.indexlength() + info.datalength();
  uint64_t stripeFooterLength = info.footerlength();
  std::unique_ptr<SeekableInputStream> pbStream = createDecompressor(
      compression,
      std::unique_ptr<SeekableInputStream>(new SeekableFileInputStream(
          stream.get(), stripeFooterStart, stripeFooterLength, memoryPool)),
      blockSize, memoryPool);
  proto::StripeFooter result;
  if (!result.ParseFromZeroCopyStream(pbStream.get())) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "bad StripeFooter from %s",
              pbStream->getName().c_str());
  }
  return result;
}

uint64_t maxStreamsForType(const proto::Type& type) {
  switch (static_cast<int64_t>(type.kind())) {
    case proto::Type_Kind_STRUCT:
      return 1;
    case proto::Type_Kind_INT:
    case proto::Type_Kind_LONG:
    case proto::Type_Kind_SHORT:
    case proto::Type_Kind_FLOAT:
    case proto::Type_Kind_DOUBLE:
    case proto::Type_Kind_BOOLEAN:
    case proto::Type_Kind_BYTE:
    case proto::Type_Kind_DATE:
    case proto::Type_Kind_TIME:
    case proto::Type_Kind_LIST:
    case proto::Type_Kind_MAP:
    case proto::Type_Kind_UNION:
      return 2;
    case proto::Type_Kind_BINARY:
    case proto::Type_Kind_DECIMAL:
    case proto::Type_Kind_TIMESTAMP:
      return 3;
    case proto::Type_Kind_CHAR:
    case proto::Type_Kind_STRING:
    case proto::Type_Kind_VARCHAR:
      return 4;
    default:
      return 0;
  }
}

uint64_t ReaderImpl::getMemoryUse(int stripeIx) {
  uint64_t maxDataLength = 0;

  if (stripeIx >= 0 && stripeIx < footer->stripes_size()) {
    uint64_t stripe = footer->stripes(stripeIx).datalength();
    if (maxDataLength < stripe) {
      maxDataLength = stripe;
    }
  } else {
    for (int i = 0; i < footer->stripes_size(); i++) {
      uint64_t stripe = footer->stripes(i).datalength();
      if (maxDataLength < stripe) {
        maxDataLength = stripe;
      }
    }
  }

  bool hasStringColumn = false;
  uint64_t nSelectedStreams = 0;
  for (int i = 0; !hasStringColumn && i < footer->types_size(); i++) {
    if (selectedColumns[static_cast<size_t>(i)]) {
      const proto::Type& type = footer->types(i);
      nSelectedStreams += maxStreamsForType(type);
      switch (static_cast<int64_t>(type.kind())) {
        case proto::Type_Kind_CHAR:
        case proto::Type_Kind_STRING:
        case proto::Type_Kind_VARCHAR:
        case proto::Type_Kind_BINARY: {
          hasStringColumn = true;
          break;
        }
      }
    }
  }

  /* If a string column is read, use stripe datalength as a memory estimate
   * because we don't know the dictionary size. Multiply by 2 because
   * a string column requires two buffers:
   * in the input stream and in the seekable input stream.
   * If no string column is read, estimate from the number of streams.
   */
  uint64_t memory =
      hasStringColumn
          ? 2 * maxDataLength
          : std::min(uint64_t(maxDataLength),
                     nSelectedStreams * stream->getNaturalReadSize());

  // Do we need even more memory to read the footer or the metadata?
  if (memory < postscript->footerlength() + DIRECTORY_SIZE_GUESS) {
    memory = postscript->footerlength() + DIRECTORY_SIZE_GUESS;
  }
  if (memory < postscript->metadatalength()) {
    memory = postscript->metadatalength();
  }

  // Account for firstRowOfStripe.
  memory += firstRowOfStripe.capacity() * sizeof(uint64_t);

  // Decompressors need buffers for each stream
  uint64_t decompressorMemory = 0;
  if (compression != CompressionKind_NONE) {
    for (int i = 0; i < footer->types_size(); i++) {
      if (selectedColumns[static_cast<size_t>(i)]) {
        const proto::Type& type = footer->types(i);
        decompressorMemory += maxStreamsForType(type) * blockSize;
      }
    }
    if (compression == CompressionKind_SNAPPY) {
      decompressorMemory *= 2;  // Snappy decompressor uses a second buffer
    }
  }

  return memory + decompressorMemory;
}

void ReaderImpl::collectPredicateStats(uint32_t* scanned, uint32_t* skipped) {
  *scanned += this->scannedStripe;
  *skipped += this->skippedStripe;
}

std::unique_ptr<InputStream> ReaderImpl::ownInputStream() {
  return std::move(stream);
}

proto::BloomFilterIndex ReaderImpl::rebuildBloomFilter(uint32_t colId) {
  std::unique_ptr<SeekableInputStream> stream =
      currentStripeStream->getStreamForBloomFilter(
          colId, proto::Stream_Kind_BLOOM_FILTER, false);
  proto::BloomFilterIndex bloomFilterIndexProto;
  if (stream) {
    google::protobuf::io::CodedInputStream input(stream.get());
    bloomFilterIndexProto.ParseFromCodedStream(&input);
  }
  return bloomFilterIndexProto;
}

bool ReaderImpl::doReadStatsOnly(ColumnVectorBatch* data) {
  currentStripeStats = getStripeStatistics(currentStripe);
  orc::StructVectorBatch* structBatch =
      dynamic_cast<orc::StructVectorBatch*>(data);
  assert(structBatch != nullptr);
  std::vector<orc::ColumnVectorBatch*>::iterator it =
      structBatch->fields.begin();
  for (uint64_t i = 0; i < schema->getSubtypeCount(); ++i) {
    const Type& child = *schema->getSubtype(i);
    if (!selectedColumns[child.getColumnId()]) continue;

    orc::ColumnVectorBatch* b = *it++;
    const univplan::ColumnStatistics* s =
        currentStripeStats->getColumnStatistics(child.getColumnId());
    b->hasStats = true;
    b->stats.hasMinMaxStats = true;
    b->stats.valueCount = s->getNumberOfValues();
    switch (b->getType()) {
      case orc::ORCTypeKind::BYTE:
      case orc::ORCTypeKind::SHORT:
      case orc::ORCTypeKind::INT:
      case orc::ORCTypeKind::LONG:
      case orc::ORCTypeKind::TIME: {
        const IntegerColumnStatisticsImpl* iStat =
            dynamic_cast<const IntegerColumnStatisticsImpl*>(s);
        if (iStat->hasMinimum()) {
          if (b->getType() == orc::ORCTypeKind::BYTE) {
            b->stats.minimum =
                dbcommon::CreateDatum(static_cast<int8_t>(iStat->getMinimum()));
            b->stats.maximum =
                dbcommon::CreateDatum(static_cast<int8_t>(iStat->getMaximum()));
          } else if (b->getType() == orc::ORCTypeKind::SHORT) {
            b->stats.minimum = dbcommon::CreateDatum(
                static_cast<int16_t>(iStat->getMinimum()));
            b->stats.maximum = dbcommon::CreateDatum(
                static_cast<int16_t>(iStat->getMaximum()));
          } else if (b->getType() == orc::ORCTypeKind::INT) {
            b->stats.minimum = dbcommon::CreateDatum(
                static_cast<int32_t>(iStat->getMinimum()));
            b->stats.maximum = dbcommon::CreateDatum(
                static_cast<int32_t>(iStat->getMaximum()));
          } else {
            b->stats.minimum = dbcommon::CreateDatum(iStat->getMinimum());
            b->stats.maximum = dbcommon::CreateDatum(iStat->getMaximum());
          }
          b->stats.sum = dbcommon::CreateDatum(iStat->getSum());
        } else {
          b->stats.hasMinMaxStats = false;
        }
        break;
      }
      case orc::ORCTypeKind::FLOAT:
      case orc::ORCTypeKind::DOUBLE: {
        const DoubleColumnStatisticsImpl* dStat =
            dynamic_cast<const DoubleColumnStatisticsImpl*>(s);
        if (dStat->hasMinimum()) {
          if (b->getType() == orc::ORCTypeKind::FLOAT) {
            b->stats.minimum =
                dbcommon::CreateDatum(static_cast<float>(dStat->getMinimum()));
            b->stats.maximum =
                dbcommon::CreateDatum(static_cast<float>(dStat->getMaximum()));
          } else {
            b->stats.minimum = dbcommon::CreateDatum(dStat->getMinimum());
            b->stats.maximum = dbcommon::CreateDatum(dStat->getMaximum());
          }
          b->stats.sum = dbcommon::CreateDatum(dStat->getSum());
        } else {
          b->stats.hasMinMaxStats = false;
        }
        break;
      }
      case orc::ORCTypeKind::CHAR:
      case orc::ORCTypeKind::VARCHAR:
      case orc::ORCTypeKind::STRING: {
        const StringColumnStatisticsImpl* sStat =
            dynamic_cast<const StringColumnStatisticsImpl*>(s);
        if (sStat->hasMinimum()) {
          b->stats.minimum = dbcommon::CreateDatum(sStat->getMinimum());
          b->stats.maximum = dbcommon::CreateDatum(sStat->getMaximum());
        } else {
          b->stats.hasMinMaxStats = false;
        }
        break;
      }
      case orc::ORCTypeKind::BINARY: {
        const BinaryColumnStatisticsImpl* sStat =
            dynamic_cast<const BinaryColumnStatisticsImpl*>(s);
        b->stats.hasMinMaxStats = false;
        break;
      }
      case orc::ORCTypeKind::DATE: {
        const DateColumnStatisticsImpl* dStat =
            dynamic_cast<const DateColumnStatisticsImpl*>(s);
        if (dStat->hasMinimum()) {
          b->stats.minimum = dbcommon::CreateDatum(dStat->getMinimum());
          b->stats.maximum = dbcommon::CreateDatum(dStat->getMaximum());
        } else {
          b->stats.hasMinMaxStats = false;
        }
        break;
      }
      case orc::ORCTypeKind::TIMESTAMP: {
        const TimestampColumnStatisticsImpl* tStat =
            dynamic_cast<const TimestampColumnStatisticsImpl*>(s);
        if (tStat->hasMinimum()) {
          b->stats.minimum = dbcommon::CreateDatum(tStat->getMinimum());
          b->stats.maximum = dbcommon::CreateDatum(tStat->getMaximum());
        } else {
          b->stats.hasMinMaxStats = false;
        }
        break;
      }
      case orc::ORCTypeKind::DECIMAL: {
        const DecimalColumnStatisticsImpl* dStat =
            dynamic_cast<const DecimalColumnStatisticsImpl*>(s);
        if (dStat->hasMinimum()) {
          b->stats.minimum = dbcommon::CreateDatum(dStat->getMinimumStr());
          b->stats.maximum = dbcommon::CreateDatum(dStat->getMaximumStr());
          b->stats.sum = dbcommon::CreateDatum(dStat->getSumStr());
        } else {
          b->stats.hasMinMaxStats = false;
        }
        break;
      }
      default: {
        LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "type %d not supported yet",
                  b->getType());
      }
    }
  }
  currentStripe += 1;
  currentRowInStripe = 0;
  return true;
}

void ReaderImpl::startNextStripe() {
  rowsInCurrentStripe = currentStripeInfo.numberofrows();
  curReader.reset();
  curReader = buildReader(*(schema.get()), *(currentStripeStream.get()));
}

void ReaderImpl::checkOrcVersion() {
  std::string version = getFormatVersion();
  if (version != "0.11" && version != "0.12") {
    *(options.getErrorStream())
        << "Warning: ORC file " << stream->getName()
        << " was written in an unknown format version " << version << "\n";
  }
}

bool ReaderImpl::notIncludeType(ColumnVectorBatch* data,
                                orc::ORCTypeKind typekind) {
  currentStripeStats = getStripeStatistics(currentStripe);
  orc::StructVectorBatch* structBatch =
      dynamic_cast<orc::StructVectorBatch*>(data);
  assert(structBatch != nullptr);
  std::vector<orc::ColumnVectorBatch*>::iterator it =
      structBatch->fields.begin();
  for (uint64_t i = 0; i < schema->getSubtypeCount(); ++i) {
    const Type& child = *schema->getSubtype(i);
    if (!selectedColumns[child.getColumnId()]) continue;

    orc::ColumnVectorBatch* b = *it++;
    const univplan::ColumnStatistics* s =
        currentStripeStats->getColumnStatistics(child.getColumnId());
    if (b->getType() == typekind) return false;
  }
  return true;
}

bool ReaderImpl::next(ColumnVectorBatch& data) {
again:
  if (currentStripe >= lastStripe) {
    data.numElements = 0;
    if (lastStripe > 0) {
      previousRow =
          firstRowOfStripe[lastStripe - 1] +
          footer->stripes(static_cast<int>(lastStripe - 1)).numberofrows();
    } else {
      previousRow = 0;
    }
    return false;
  }
  if (currentRowInStripe == 0) {
    // check if only read stripe statistics, and then return
    if (options.readStatsOnly()) {
      return doReadStatsOnly(&data);
    }

    const proto::StripeFooter* currentStripeFooter =
        &stripeFooters[currentStripe - firstStripe];
    const Timezone& writerTimezone =
        currentStripeFooter->has_writertimezone()
            ? getTimezoneByName(currentStripeFooter->writertimezone())
            : localTimezone;
    currentStripeInfo = footer->stripes(static_cast<int>(currentStripe));
    currentStripeStream.reset(new StripeStreamsImpl(
        *this, *currentStripeFooter, currentStripeInfo.offset(),
        *(stream.get()), memoryPool, writerTimezone));

    // filter push down
    bool canDrop = false;
    if (options.getPredicateExprs()) {
      std::unique_ptr<univplan::Statistics> stats =
          getStripeStatistics(currentStripe);
      if (stats) {
        OrcPredicates::uptr predicate(
            new OrcPredicates(stats.get(), this, options.getPredicateExprs(),
                              options.getTupleDesc()));
        canDrop = predicate->canDrop();
      }
    }
    if (!canDrop) {
      ++scannedStripe;
      startNextStripe();
    } else {
      ++skippedStripe;
      currentStripe += 1;
      goto again;
    }
  }
  uint64_t rowsToRead = std::min(static_cast<uint64_t>(data.capacity),
                                 rowsInCurrentStripe - currentRowInStripe);
  data.numElements = rowsToRead;
  curReader->next(data, rowsToRead, 0);

  // update row number
  previousRow = firstRowOfStripe[currentStripe] + currentRowInStripe;
  currentRowInStripe += rowsToRead;
  if (currentRowInStripe >= rowsInCurrentStripe) {
    currentStripe += 1;
    currentRowInStripe = 0;
  }
  return rowsToRead != 0;
}

std::unique_ptr<ColumnVectorBatch> ReaderImpl::createRowBatch(
    uint64_t capacity) const {
  if (numberOfStripes)
    return getSelectedType().createRowBatch(capacity, memoryPool);
  else
    return nullptr;
}

void ensureOrcFooter(InputStream* stream, DataBuffer<char>* buffer,
                     uint64_t postscriptLength) {
  const std::string MAGIC("ORC");
  const uint64_t magicLength = MAGIC.length();
  const char* const bufferStart = buffer->data();
  const uint64_t bufferLength = buffer->size();

  if (postscriptLength < magicLength || bufferLength < magicLength) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Invalid ORC postscript length");
  }
  const char* magicStart = bufferStart + bufferLength - 1 - magicLength;

  // Look for the magic string at the end of the postscript.
  if (memcmp(magicStart, MAGIC.c_str(), magicLength) != 0) {
    // If there is no magic string at the end, check the beginning.
    // Only files written by Hive 0.11.0 don't have the tail ORC string.
    char* frontBuffer = new char[magicLength];
    stream->read(frontBuffer, magicLength, 0);
    bool foundMatch = memcmp(frontBuffer, MAGIC.c_str(), magicLength) == 0;
    delete[] frontBuffer;
    if (!foundMatch) {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Not an ORC file");
    }
  }
}

/**
 * Read the file's postscript from the given buffer.
 * @param stream the file stream
 * @param buffer the buffer with the tail of the file.
 * @param postscriptSize the length of postscript in bytes
 */
std::unique_ptr<proto::PostScript> readPostscript(InputStream* stream,
                                                  DataBuffer<char>* buffer,
                                                  uint64_t postscriptSize) {
  char* ptr = buffer->data();
  uint64_t readSize = buffer->size();

  ensureOrcFooter(stream, buffer, postscriptSize);

  std::unique_ptr<proto::PostScript> postscript =
      std::unique_ptr<proto::PostScript>(new proto::PostScript());
  if (!postscript->ParseFromArray(ptr + readSize - 1 - postscriptSize,
                                  static_cast<int>(postscriptSize))) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Failed to parse the postscript from %s",
              stream->getName().c_str());
  }
  return std::move(postscript);
}

/**
 * Parse the footer from the given buffer.
 * @param stream the file's stream
 * @param buffer the buffer to parse the footer from
 * @param footerOffset the offset within the buffer that contains the footer
 * @param ps the file's postscript
 */
std::unique_ptr<proto::Footer> readFooter(
    InputStream* stream, DataBuffer<char>* buffer, uint64_t footerOffset,
    const proto::PostScript& ps,
    dbcommon::MemoryPool& pool) {  // NOLINT
  char* footerPtr = buffer->data() + footerOffset;

  std::unique_ptr<SeekableInputStream> pbStream = createDecompressor(
      convertCompressionKind(ps),
      std::unique_ptr<SeekableInputStream>(
          new SeekableArrayInputStream(footerPtr, ps.footerlength())),
      getCompressionBlockSize(ps), pool);

  std::unique_ptr<proto::Footer> footer =
      std::unique_ptr<proto::Footer>(new proto::Footer());
  if (!footer->ParseFromZeroCopyStream(pbStream.get())) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Failed to parse the footer from %s",
              stream->getName().c_str());
  }
  return std::move(footer);
}

std::unique_ptr<Reader> createReader(std::unique_ptr<InputStream> stream,
                                     const ReaderOptions& options) {
  dbcommon::MemoryPool* memoryPool = options.getMemoryPool();
  std::unique_ptr<proto::PostScript> ps;
  std::unique_ptr<proto::Footer> footer;
  std::string serializedFooter = options.getSerializedFileTail();
  uint64_t fileLength;
  uint64_t postscriptLength;
  if (serializedFooter.length() != 0) {
    // Parse the file tail from the serialized one.
    proto::FileTail tail;
    if (!tail.ParseFromString(serializedFooter)) {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "Failed to parse the file tail from string");
    }
    ps.reset(new proto::PostScript(tail.postscript()));
    footer.reset(new proto::Footer(tail.footer()));
    fileLength = tail.filelength();
    postscriptLength = tail.postscriptlength();
  } else {
    // figure out the size of the file using the option or filesystem
    fileLength = std::min(options.getTailLocation(),
                          static_cast<uint64_t>(stream->getLength()));

    // read last bytes into buffer to get PostScript
    uint64_t readSize = std::min(fileLength, DIRECTORY_SIZE_GUESS);
    if (readSize < 4) {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "File size too small");
    }

    std::unique_ptr<DataBuffer<char>> buffer(
        new DataBuffer<char>(*memoryPool, readSize));
    stream->read(buffer->data(), readSize, fileLength - readSize);

    postscriptLength = buffer->data()[readSize - 1] & 0xff;
    ps = readPostscript(stream.get(), buffer.get(), postscriptLength);
    uint64_t footerSize = ps->footerlength();
    uint64_t tailSize = 1 + postscriptLength + footerSize;
    uint64_t footerOffset;

    if (tailSize > readSize) {
      buffer->resize(footerSize);
      stream->read(buffer->data(), footerSize, fileLength - tailSize);
      footerOffset = 0;
    } else {
      footerOffset = readSize - tailSize;
    }

    footer =
        readFooter(stream.get(), buffer.get(), footerOffset, *ps, *memoryPool);
  }
  return std::unique_ptr<Reader>(
      new ReaderImpl(std::move(stream), options, std::move(ps),
                     std::move(footer), fileLength, postscriptLength));
}

void ReaderImpl::updateSelectedByFieldId(uint64_t fieldId) {
  if (fieldId < schema->getSubtypeCount()) {
    selectChildren(*schema->getSubtype(fieldId));
  } else {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "Invalid column selected %llu out of %llu", fieldId,
              schema->getSubtypeCount());
  }
}

void ReaderImpl::updateSelectedByTypeId(uint64_t typeId) {
  if (typeId < selectedColumns.size()) {
    const Type& type = *idTypeMap[typeId];
    selectChildren(type);
  } else {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "Invalid type id selected %llu out of %zu", typeId,
              selectedColumns.size());
  }
}

void ReaderImpl::updateSelectedByName(const std::string& fieldName) {
  std::map<std::string, uint64_t>::const_iterator ite =
      nameIdMap.find(fieldName);
  if (ite != nameIdMap.end()) {
    updateSelectedByTypeId(ite->second);
  } else {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Invalid column selected %s",
              fieldName.c_str());
  }
}

void ReaderImpl::selectChildren(const Type& type) {
  size_t id = static_cast<size_t>(type.getColumnId());
  if (!selectedColumns[id]) {
    selectedColumns[id] = true;
    for (size_t c = id; c <= type.getMaximumColumnId(); ++c) {
      selectedColumns[c] = true;
    }
  }
}

// Recurses over a type tree and selects the parents of every selected type.
// @return true if any child was selected.
bool ReaderImpl::selectParents(const Type& type) {
  size_t id = static_cast<size_t>(type.getColumnId());
  bool result = selectedColumns[id];
  for (uint64_t c = 0; c < type.getSubtypeCount(); ++c) {
    result |= selectParents(*type.getSubtype(c));
  }
  selectedColumns[id] = result;
  return result;
}

StripeStreams::~StripeStreams() {
  // PASS
}

StripeStreamsImpl::StripeStreamsImpl(const ReaderImpl& _reader,
                                     const proto::StripeFooter& _footer,
                                     uint64_t _stripeStart, InputStream& _input,
                                     dbcommon::MemoryPool& pool,
                                     const Timezone& _writerTimezone)
    : reader(_reader),
      footer(_footer),
      stripeStart(_stripeStart),
      input(_input),
      memoryPool(pool),
      writerTimezone(_writerTimezone) {
  // PASS
}

StripeStreamsImpl::~StripeStreamsImpl() {
  // PASS
}

const ReaderOptions& StripeStreamsImpl::getReaderOptions() const {
  return reader.getReaderOptions();
}

const std::vector<bool> StripeStreamsImpl::getSelectedColumns() const {
  return reader.getSelectedColumns();
}

proto::ColumnEncoding StripeStreamsImpl::getEncoding(uint64_t columnId) const {
  return footer.columns(static_cast<int>(columnId));
}

const Timezone& StripeStreamsImpl::getWriterTimezone() const {
  return writerTimezone;
}

std::unique_ptr<SeekableInputStream> StripeStreamsImpl::getStream(
    uint64_t columnId, proto::Stream_Kind kind, bool shouldStream) const {
  uint64_t offset = stripeStart;
  for (int i = 0; i < footer.streams_size(); ++i) {
    const proto::Stream& stream = footer.streams(i);
    if (stream.has_kind() && stream.kind() == kind &&
        stream.column() == static_cast<uint64_t>(columnId)) {
      uint64_t myBlock =
          shouldStream ? input.getNaturalReadSize() : stream.length();
      return createDecompressor(
          reader.getCompression(),
          std::unique_ptr<SeekableInputStream>(new SeekableFileInputStream(
              &input, offset, stream.length(), memoryPool, myBlock)),
          reader.getCompressionSize(), memoryPool);
    }
    offset += stream.length();
  }
  return std::unique_ptr<SeekableInputStream>();
}

std::unique_ptr<SeekableInputStream> StripeStreamsImpl::getStreamForBloomFilter(
    uint64_t columnId, proto::Stream_Kind kind, bool shouldStream) const {
  uint64_t offset = stripeStart;
  for (int i = 0; i < footer.streams_size(); ++i) {
    const proto::Stream& stream = footer.streams(i);
    if (stream.has_kind() && stream.kind() == kind &&
        stream.column() == static_cast<uint64_t>(columnId)) {
      uint64_t myBlock =
          shouldStream ? input.getNaturalReadSize() : stream.length();
      return createDecompressor(
          reader.getCompression(),
          std::unique_ptr<SeekableInputStream>(
              new SeekableFileBloomFilterInputStream(
                  &input, offset, stream.length(), memoryPool, myBlock)),
          reader.getCompressionSize(), memoryPool);
    }
    offset += stream.length();
  }
  return std::unique_ptr<SeekableInputStream>();
}

dbcommon::MemoryPool& StripeStreamsImpl::getMemoryPool() const {
  return memoryPool;
}

RleVersion convertRleVersion(proto::ColumnEncoding_Kind kind) {
  switch (static_cast<int64_t>(kind)) {
    case proto::ColumnEncoding_Kind_DIRECT:
    case proto::ColumnEncoding_Kind_DICTIONARY:
      return RleVersion_1;
    case proto::ColumnEncoding_Kind_DIRECT_V2:
    case proto::ColumnEncoding_Kind_DICTIONARY_V2:
      return RleVersion_2;
    case proto::ColumnEncoding_Kind_DIRECT_V0:
    case proto::ColumnEncoding_Kind_DICTIONARY_V0:
      return RleVersion_0;
    default:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "Unknown encoding in convertRleVersion");
  }
}

ColumnReader::ColumnReader(const Type& type, StripeStreams& stripe)
    : columnId(type.getColumnId()), memoryPool(stripe.getMemoryPool()) {
  std::unique_ptr<SeekableInputStream> stream =
      stripe.getStream(columnId, proto::Stream_Kind_PRESENT, true);
  if (stream.get()) {
    notNullDecoder = createBooleanRleDecoder(std::move(stream));
  }
}

ColumnReader::~ColumnReader() {
  // PASS
}

uint64_t ColumnReader::skip(uint64_t numValues) {
  ByteRleDecoder* decoder = notNullDecoder.get();
  if (decoder) {
    // page through the values that we want to skip
    // and count how many are non-null
    const size_t MAX_BUFFER_SIZE = 32768;
    size_t bufferSize =
        std::min(MAX_BUFFER_SIZE, static_cast<size_t>(numValues));
    char buffer[MAX_BUFFER_SIZE];
    uint64_t remaining = numValues;
    while (remaining > 0) {
      uint64_t chunkSize =
          std::min(remaining, static_cast<uint64_t>(bufferSize));
      decoder->next(buffer, chunkSize, 0);
      remaining -= chunkSize;
      for (uint64_t i = 0; i < chunkSize; ++i) {
        if (!buffer[i]) {
          numValues -= 1;
        }
      }
    }
  }
  return numValues;
}

void ColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues,
                        char* incomingMask) {
  if (numValues > rowBatch.capacity) {
    rowBatch.resize(numValues);
  }
  rowBatch.numElements = numValues;
  ByteRleDecoder* decoder = notNullDecoder.get();
  if (decoder) {
    char* notNullArray = rowBatch.notNull.data();
    decoder->next(notNullArray, numValues, incomingMask);
    // check to see if there are nulls in this batch
    // performance: reduce branch to enable vectorize
    char tmp = 0x0;  // false
#pragma clang vectorize(enable)
    for (uint64_t i = 0; i < numValues; ++i) {
      tmp |= notNullArray[i] ^ 0x1;  // !notNull
    }
    rowBatch.hasNulls = (tmp);
    if (rowBatch.hasNulls) return;
  } else if (incomingMask) {
    // If we don't have a notNull stream, copy the incomingMask
    rowBatch.hasNulls = true;
    memcpy(rowBatch.notNull.data(), incomingMask, numValues);
    return;
  }
  rowBatch.hasNulls = false;
}

// Expand an array of bytes in place to the corresponding array of longs.
// Has to work backwards so that they data isn't clobbered during the
// expansion.
// @param buffer the array of chars and array of longs that need to be
//        expanded
// @param numValues the number of bytes to convert to longs
void expandBytesToLongs(int64_t* buffer, uint64_t numValues) {
  for (size_t i = numValues - 1; i < numValues; --i) {
    buffer[i] = reinterpret_cast<char*>(buffer)[i];
  }
}

BooleanColumnReader::BooleanColumnReader(const Type& type,
                                         StripeStreams& stripe)
    : ColumnReader(type, stripe) {
  rle = createBooleanRleDecoder(
      stripe.getStream(columnId, proto::Stream_Kind_DATA, true));
}

BooleanColumnReader::~BooleanColumnReader() {
  // PASS
}

uint64_t BooleanColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  rle->skip(numValues);
  return numValues;
}

void BooleanColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues,
                               char* notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  bool* ptr = dynamic_cast<BooleanVectorBatch&>(rowBatch).data.data();
  rle->next(reinterpret_cast<char*>(ptr), numValues,
            rowBatch.hasNulls ? rowBatch.notNull.data() : 0);
}

ByteColumnReader::ByteColumnReader(const Type& type, StripeStreams& stripe)
    : ColumnReader(type, stripe) {
  rle = createByteRleDecoder(
      stripe.getStream(columnId, proto::Stream_Kind_DATA, true));
}

ByteColumnReader::~ByteColumnReader() {
  // PASS
}

uint64_t ByteColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  rle->skip(numValues);
  return numValues;
}

void ByteColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues,
                            char* notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  // Since the byte rle places the output in a char* instead of long*,
  // we cheat here and use the long* and then expand it in a second pass.
  int8_t* ptr = dynamic_cast<ByteVectorBatch&>(rowBatch).data.data();
  rle->next(reinterpret_cast<char*>(ptr), numValues,
            rowBatch.hasNulls ? rowBatch.notNull.data() : 0);
  // expandBytesToLongs(ptr, numValues);
}

template <class IntType>
IntegerColumnReader<IntType>::IntegerColumnReader(
    const Type& type,
    StripeStreams& stripe)  // NOLINT
    : ColumnReader(type, stripe) {}

template <class IntType>
uint64_t IntegerColumnReader<IntType>::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  rle->skip(numValues);
  return numValues;
}

template <class IntType>
void IntegerColumnReader<IntType>::next(ColumnVectorBatch& rowBatch,  // NOLINT
                                        uint64_t numValues, char* notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  rle->next(dynamic_cast<FixedSizeVectorBatch<IntType>&>(rowBatch).data.data(),
            numValues, rowBatch.hasNulls ? rowBatch.notNull.data() : 0);
}

DateColumnReader::DateColumnReader(const Type& type, StripeStreams& stripe)
    : ColumnReader(type, stripe) {
  RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
  rle = createRleDecoder(
      stripe.getStream(columnId, proto::Stream_Kind_DATA, true), true, vers,
      memoryPool, INT);
}

uint64_t DateColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  rle->skip(numValues);
  return numValues;
}

void DateColumnReader::next(ColumnVectorBatch& rowBatch,  // NOLINT
                            uint64_t numValues, char* notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  int32_t* ptr = dynamic_cast<DateVectorBatch&>(rowBatch).data.data();
  rle->next(reinterpret_cast<int32_t*>(ptr), numValues,
            rowBatch.hasNulls ? rowBatch.notNull.data() : 0);
}

TimeColumnReader::TimeColumnReader(const Type& type, StripeStreams& stripe)
    : ColumnReader(type, stripe) {
  RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
  rle = createRleDecoder(
      stripe.getStream(columnId, proto::Stream_Kind_DATA, true), true, vers,
      memoryPool, LONG);
}

uint64_t TimeColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  rle->skip(numValues);
  return numValues;
}

void TimeColumnReader::next(ColumnVectorBatch& rowBatch,  // NOLINT
                            uint64_t numValues, char* notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  int64_t* ptr = dynamic_cast<TimeVectorBatch&>(rowBatch).data.data();
  rle->next(reinterpret_cast<int64_t*>(ptr), numValues,
            rowBatch.hasNulls ? rowBatch.notNull.data() : 0);
}

TimestampColumnReader::TimestampColumnReader(const Type& type,
                                             StripeStreams& stripe)
    : ColumnReader(type, stripe),
      writerTimezone(stripe.getWriterTimezone()),
      epochOffset(writerTimezone.getEpoch()) {
  RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
  secondsRle = createRleDecoder(
      stripe.getStream(columnId, proto::Stream_Kind_DATA, true), true, vers,
      memoryPool, LONG);
  nanoRle = createRleDecoder(
      stripe.getStream(columnId, proto::Stream_Kind_SECONDARY, true), false,
      vers, memoryPool, LONG);
}

TimestampColumnReader::~TimestampColumnReader() {
  // PASS
}

uint64_t TimestampColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  secondsRle->skip(numValues);
  nanoRle->skip(numValues);
  return numValues;
}

void TimestampColumnReader::next(ColumnVectorBatch& rowBatch,
                                 uint64_t numValues, char* notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : nullptr;
  TimestampVectorBatch& timestampBatch =
      dynamic_cast<TimestampVectorBatch&>(rowBatch);
  int64_t* secsBuffer = timestampBatch.data.data();
  secondsRle->next(secsBuffer, numValues, notNull);
  int64_t* nanoBuffer = timestampBatch.nanoseconds.data();
  nanoRle->next(nanoBuffer, numValues, notNull);

  // Construct the values
  for (uint64_t i = 0; i < numValues; i++) {
    if (notNull == nullptr || notNull[i]) {
      uint64_t zeros = nanoBuffer[i] & 0x7;
      nanoBuffer[i] >>= 3;
      if (zeros != 0) {
        for (uint64_t j = 0; j <= zeros; ++j) {
          nanoBuffer[i] *= 10;
        }
      }
  //    int64_t writerTime = secsBuffer[i] + epochOffset;
      // For now only support timestamp without timezone
      // secsBuffer[i] =
      //    writerTime + writerTimezone.getVariant(writerTime).gmtOffset;
      secsBuffer[i] +=
          (ORC_TIMESTAMP_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECONDS_PER_DAY;
    }
  }
}

DoubleColumnReader::DoubleColumnReader(const Type& type, StripeStreams& stripe)
    :  // NOLINT
      ColumnReader(type, stripe),
      inputStream(stripe.getStream(columnId, proto::Stream_Kind_DATA, true)),
      columnKind(type.getKind()),
      bytesPerValue((type.getKind() == FLOAT) ? 4 : 8),
      bufferPointer(NULL),
      bufferEnd(NULL) {
  // PASS
}

DoubleColumnReader::~DoubleColumnReader() {
  // PASS
}

uint64_t DoubleColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);

  if (static_cast<size_t>(bufferEnd - bufferPointer) >=
      bytesPerValue * numValues) {
    bufferPointer += bytesPerValue * numValues;
  } else {
    inputStream->Skip(
        static_cast<int>(bytesPerValue * numValues -
                         static_cast<size_t>(bufferEnd - bufferPointer)));
    bufferEnd = NULL;
    bufferPointer = NULL;
  }

  return numValues;
}

void DoubleColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues,
                              char* notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  // update the notNull from the parent class
  notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;

  if (columnKind == FLOAT) {
    void* data = dynamic_cast<FloatVectorBatch&>(rowBatch).data.data();
    if (notNull) {
      uint64_t notNullValues = 0;
      for (uint64_t i = 0; i < numValues; ++i) {
        if (notNull[i]) {
          ++notNullValues;
        }
      }
      float* dat = reinterpret_cast<float*>(data);
      readData(reinterpret_cast<char*>(dat), notNullValues);
      for (int64_t j = numValues - 1, k = notNullValues - 1; j >= 0; --j) {
        if (notNull[j]) {
          dat[j] = dat[k--];
        }
      }
    } else {
      readData(reinterpret_cast<char*>(data), numValues);
    }
  } else {
    void* data = dynamic_cast<DoubleVectorBatch&>(rowBatch).data.data();
    if (notNull) {
      uint64_t notNullValues = 0;
      for (uint64_t i = 0; i < numValues; ++i) {
        if (notNull[i]) {
          ++notNullValues;
        }
      }
      double* dat = reinterpret_cast<double*>(data);
      readData(reinterpret_cast<char*>(dat), notNullValues);
      for (int64_t j = numValues - 1, k = notNullValues - 1; j >= 0; --j) {
        if (notNull[j]) {
          dat[j] = dat[k--];
        }
      }
    } else {
      readData(reinterpret_cast<char*>(data), numValues);
    }
  }
}

StringDictionaryColumnReader::StringDictionaryColumnReader(
    const Type& type, StripeStreams& stripe)
    : ColumnReader(type, stripe),
      dictionaryBlob(stripe.getMemoryPool()),
      dictionaryOffset(stripe.getMemoryPool()) {
  RleVersion rleVersion =
      convertRleVersion(stripe.getEncoding(columnId).kind());
  dictionaryCount = stripe.getEncoding(columnId).dictionarysize();
  rle = createRleDecoder(
      stripe.getStream(columnId, proto::Stream_Kind_DATA, true), false,
      rleVersion, memoryPool, LONG);
  std::unique_ptr<RleDecoder> lengthDecoder = createRleDecoder(
      stripe.getStream(columnId, proto::Stream_Kind_LENGTH, false), false,
      rleVersion, memoryPool, LONG);
  dictionaryOffset.resize(dictionaryCount + 1);
  int64_t* lengthArray = dictionaryOffset.data();
  lengthDecoder->next(lengthArray + 1, dictionaryCount, 0);
  lengthArray[0] = 0;
  for (uint64_t i = 1; i < dictionaryCount + 1; ++i) {
    lengthArray[i] += lengthArray[i - 1];
  }
  int64_t blobSize = lengthArray[dictionaryCount];
  dictionaryBlob.resize(static_cast<uint64_t>(blobSize));
  std::unique_ptr<SeekableInputStream> blobStream =
      stripe.getStream(columnId, proto::Stream_Kind_DICTIONARY_DATA, false);
  readFully(dictionaryBlob.data(), blobSize, blobStream.get());
}

StringDictionaryColumnReader::~StringDictionaryColumnReader() {
  // PASS
}

uint64_t StringDictionaryColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  rle->skip(numValues);
  return numValues;
}

void StringDictionaryColumnReader::next(ColumnVectorBatch& rowBatch,
                                        uint64_t numValues, char* notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  // update the notNull from the parent class
  notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;
  BytesVectorBatch& byteBatch = dynamic_cast<BytesVectorBatch&>(rowBatch);
  byteBatch.isDirectEncoding = false;
  char* blob = dictionaryBlob.data();
  int64_t* dictionaryOffsets = dictionaryOffset.data();
  char** outputStarts = byteBatch.data.data();
  int64_t* outputLengths = byteBatch.length.data();
  rle->next(outputLengths, numValues, notNull);
  if (notNull) {
    for (uint64_t i = 0; i < numValues; ++i) {
      if (notNull[i]) {
        int64_t entry = outputLengths[i];
        outputStarts[i] = blob + dictionaryOffsets[entry];
        outputLengths[i] =
            dictionaryOffsets[entry + 1] - dictionaryOffsets[entry];
      }
    }
  } else {
    for (uint64_t i = 0; i < numValues; ++i) {
      int64_t entry = outputLengths[i];
      outputStarts[i] = blob + dictionaryOffsets[entry];
      outputLengths[i] =
          dictionaryOffsets[entry + 1] - dictionaryOffsets[entry];
    }
  }
}

StringDirectColumnReader::StringDirectColumnReader(const Type& type,
                                                   StripeStreams& stripe)
    : ColumnReader(type, stripe), blobBuffer(stripe.getMemoryPool()) {
  RleVersion rleVersion =
      convertRleVersion(stripe.getEncoding(columnId).kind());
  lengthRle = createRleDecoder(
      stripe.getStream(columnId, proto::Stream_Kind_LENGTH, true), false,
      rleVersion, memoryPool, LONG);
  blobStream = stripe.getStream(columnId, proto::Stream_Kind_DATA, true);
  lastBuffer = 0;
  lastBufferLength = 0;
}

StringDirectColumnReader::~StringDirectColumnReader() {
  // PASS
}

uint64_t StringDirectColumnReader::skip(uint64_t numValues) {
  const size_t BUFFER_SIZE = 1024;
  numValues = ColumnReader::skip(numValues);
  int64_t buffer[BUFFER_SIZE];
  uint64_t done = 0;
  size_t totalBytes = 0;
  // read the lengths, so we know haw many bytes to skip
  while (done < numValues) {
    uint64_t step =
        std::min(BUFFER_SIZE, static_cast<size_t>(numValues - done));
    lengthRle->next(buffer, step, 0);
    totalBytes += computeSize(buffer, 0, step);
    done += step;
  }
  if (totalBytes <= lastBufferLength) {
    // subtract the needed bytes from the ones left over
    lastBufferLength -= totalBytes;
    lastBuffer += totalBytes;
  } else {
    // move the stream forward after accounting for the buffered bytes
    totalBytes -= lastBufferLength;
    blobStream->Skip(static_cast<int>(totalBytes));
    lastBufferLength = 0;
    lastBuffer = 0;
  }
  return numValues;
}

size_t StringDirectColumnReader::computeSize(const int64_t* lengths,
                                             const char* notNull,
                                             uint64_t numValues) {
  size_t totalLength = 0;
  if (notNull) {
    for (size_t i = 0; i < numValues; ++i) {
      if (notNull[i]) {
        totalLength += static_cast<size_t>(lengths[i]);
      }
    }
  } else {
    for (size_t i = 0; i < numValues; ++i) {
      totalLength += static_cast<size_t>(lengths[i]);
    }
  }
  return totalLength;
}

void StringDirectColumnReader::next(ColumnVectorBatch& rowBatch,
                                    uint64_t numValues, char* notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  // update the notNull from the parent class
  notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;
  BytesVectorBatch& byteBatch = dynamic_cast<BytesVectorBatch&>(rowBatch);
  byteBatch.isDirectEncoding = true;
  char** __restrict__ startPtr = byteBatch.data.data();
  int64_t* __restrict__ lengthPtr = byteBatch.length.data();

  // read the length vector
  lengthRle->next(lengthPtr, numValues, notNull);

  // figure out the total length of data we need from the blob stream
  const size_t totalLength = computeSize(lengthPtr, notNull, numValues);

  // Load data from the blob stream into our buffer until we have enough
  // to get the rest directly out of the stream's buffer.
  size_t bytesBuffered = 0;
  blobBuffer.resize(totalLength);
  char* ptr = blobBuffer.data();
  while (bytesBuffered + lastBufferLength < totalLength) {
    blobBuffer.resize(bytesBuffered + lastBufferLength);
    memcpy(ptr + bytesBuffered, lastBuffer, lastBufferLength);
    bytesBuffered += lastBufferLength;
    const void* readBuffer;
    int readLength;
    if (!blobStream->Next(&readBuffer, &readLength)) {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "failed to read in StringDirectColumnReader.next");
    }
    lastBuffer = static_cast<const char*>(readBuffer);
    lastBufferLength = static_cast<size_t>(readLength);
  }

  // Set up the start pointers for the ones that will come out of the buffer.
  size_t filledSlots = 0;
  size_t usedBytes = 0;
  ptr = blobBuffer.data();
  if (notNull) {
    while (filledSlots < numValues &&
           (!notNull[filledSlots] ||
            usedBytes + static_cast<size_t>(lengthPtr[filledSlots]) <=
                bytesBuffered)) {
      if (notNull[filledSlots]) {
        startPtr[filledSlots] = ptr + usedBytes;
        usedBytes += static_cast<size_t>(lengthPtr[filledSlots]);
      }
      filledSlots += 1;
    }
  } else {
    while (filledSlots < numValues &&
           (usedBytes + static_cast<size_t>(lengthPtr[filledSlots]) <=
            bytesBuffered)) {
      startPtr[filledSlots] = ptr + usedBytes;
      usedBytes += static_cast<size_t>(lengthPtr[filledSlots]);
      filledSlots += 1;
    }
  }

  // do we need to complete the last value in the blob buffer?
  if (usedBytes < bytesBuffered) {
    size_t moreBytes = static_cast<size_t>(lengthPtr[filledSlots]) -
                       (bytesBuffered - usedBytes);
    blobBuffer.resize(bytesBuffered + moreBytes);
    ptr = blobBuffer.data();
    memcpy(ptr + bytesBuffered, lastBuffer, moreBytes);
    lastBuffer += moreBytes;
    lastBufferLength -= moreBytes;
    startPtr[filledSlots++] = ptr + usedBytes;
  }

  // Finally, set up any remaining entries into the stream buffer
  if (notNull) {
    while (filledSlots < numValues) {
      if (notNull[filledSlots]) {
        startPtr[filledSlots] = const_cast<char*>(lastBuffer);
        lastBuffer += lengthPtr[filledSlots];
        lastBufferLength -= static_cast<size_t>(lengthPtr[filledSlots]);
      }
      filledSlots += 1;
    }
  } else {
    // performance: use tmp variable to avoid needless memory update for the
    // member variable
    const char* tmpLastBuffer = lastBuffer;
#pragma clang loop unroll(full)
    while (filledSlots < numValues) {
      startPtr[filledSlots] = const_cast<char*>(tmpLastBuffer);
      tmpLastBuffer += lengthPtr[filledSlots];
      filledSlots += 1;
    }
    lastBufferLength = lastBufferLength - (tmpLastBuffer - lastBuffer);
    lastBuffer = tmpLastBuffer;
  }
}

StructColumnReader::StructColumnReader(const Type& type, StripeStreams& stripe)
    :  // NOLINT
      ColumnReader(type, stripe) {
  // count the number of selected sub-columns
  const std::vector<bool> selectedColumns = stripe.getSelectedColumns();
  switch (static_cast<int64_t>(stripe.getEncoding(columnId).kind())) {
    case proto::ColumnEncoding_Kind_DIRECT:
      for (unsigned int i = 0; i < type.getSubtypeCount(); ++i) {
        const Type& child = *type.getSubtype(i);
        if (selectedColumns[static_cast<uint64_t>(child.getColumnId())]) {
          children.push_back(buildReader(child, stripe).release());
        }
      }
      break;
    case proto::ColumnEncoding_Kind_DIRECT_V2:
    case proto::ColumnEncoding_Kind_DICTIONARY:
    case proto::ColumnEncoding_Kind_DICTIONARY_V2:
    default:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "Unknown encoding for StructColumnReader");
  }
}

StructColumnReader::~StructColumnReader() {
  for (size_t i = 0; i < children.size(); i++) {
    delete children[i];
  }
}

uint64_t StructColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  for (std::vector<ColumnReader*>::iterator ptr = children.begin();
       ptr != children.end(); ++ptr) {
    (*ptr)->skip(numValues);
  }
  return numValues;
}

void StructColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues,
                              char* notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  uint64_t i = 0;
  notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;
  for (std::vector<ColumnReader*>::iterator ptr = children.begin();
       ptr != children.end(); ++ptr, ++i) {
    (*ptr)->next(*(dynamic_cast<StructVectorBatch&>(rowBatch).fields[i]),
                 numValues, notNull);
  }
}

ListColumnReader::ListColumnReader(const Type& type, StripeStreams& stripe)
    : ColumnReader(type, stripe) {
  // count the number of selected sub-columns
  const std::vector<bool> selectedColumns = stripe.getSelectedColumns();
  RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
  rle = createRleDecoder(
      stripe.getStream(columnId, proto::Stream_Kind_LENGTH, true), false, vers,
      memoryPool, LONG);
  const Type& childType = *type.getSubtype(0);
  if (selectedColumns[static_cast<uint64_t>(childType.getColumnId())]) {
    child = buildReader(childType, stripe);
  }
}

ListColumnReader::~ListColumnReader() {
  // PASS
}

uint64_t ListColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  ColumnReader* childReader = child.get();
  if (childReader) {
    const uint64_t BUFFER_SIZE = 1024;
    int64_t buffer[BUFFER_SIZE];
    uint64_t childrenElements = 0;
    uint64_t lengthsRead = 0;
    while (lengthsRead < numValues) {
      uint64_t chunk = std::min(numValues - lengthsRead, BUFFER_SIZE);
      rle->next(buffer, chunk, 0);
      for (size_t i = 0; i < chunk; ++i) {
        childrenElements += static_cast<size_t>(buffer[i]);
      }
      lengthsRead += chunk;
    }
    childReader->skip(childrenElements);
  } else {
    rle->skip(numValues);
  }
  return numValues;
}

void ListColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues,
                            char* notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  ListVectorBatch& listBatch = dynamic_cast<ListVectorBatch&>(rowBatch);
  int64_t* offsets = listBatch.offsets.data();
  notNull = listBatch.hasNulls ? listBatch.notNull.data() : 0;
  rle->next(offsets, numValues, notNull);
  uint64_t totalChildren = 0;
  if (notNull) {
    for (size_t i = 0; i < numValues; ++i) {
      if (notNull[i]) {
        uint64_t tmp = static_cast<uint64_t>(offsets[i]);
        offsets[i] = static_cast<int64_t>(totalChildren);
        totalChildren += tmp;
      } else {
        offsets[i] = static_cast<int64_t>(totalChildren);
      }
    }
  } else {
    for (size_t i = 0; i < numValues; ++i) {
      uint64_t tmp = static_cast<uint64_t>(offsets[i]);
      offsets[i] = static_cast<int64_t>(totalChildren);
      totalChildren += tmp;
    }
  }
  offsets[numValues] = static_cast<int64_t>(totalChildren);
  ColumnReader* childReader = child.get();
  if (childReader) {
    childReader->next(*(listBatch.elements.get()), totalChildren, 0);
  }
}

MapColumnReader::MapColumnReader(const Type& type, StripeStreams& stripe)
    : ColumnReader(type, stripe) {
  // Determine if the key and/or value columns are selected
  const std::vector<bool> selectedColumns = stripe.getSelectedColumns();
  RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
  rle = createRleDecoder(
      stripe.getStream(columnId, proto::Stream_Kind_LENGTH, true), false, vers,
      memoryPool, LONG);
  const Type& keyType = *type.getSubtype(0);
  if (selectedColumns[static_cast<uint64_t>(keyType.getColumnId())]) {
    keyReader = buildReader(keyType, stripe);
  }
  const Type& elementType = *type.getSubtype(1);
  if (selectedColumns[static_cast<uint64_t>(elementType.getColumnId())]) {
    elementReader = buildReader(elementType, stripe);
  }
}

MapColumnReader::~MapColumnReader() {
  // PASS
}

uint64_t MapColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  ColumnReader* rawKeyReader = keyReader.get();
  ColumnReader* rawElementReader = elementReader.get();
  if (rawKeyReader || rawElementReader) {
    const uint64_t BUFFER_SIZE = 1024;
    int64_t buffer[BUFFER_SIZE];
    uint64_t childrenElements = 0;
    uint64_t lengthsRead = 0;
    while (lengthsRead < numValues) {
      uint64_t chunk = std::min(numValues - lengthsRead, BUFFER_SIZE);
      rle->next(buffer, chunk, 0);
      for (size_t i = 0; i < chunk; ++i) {
        childrenElements += static_cast<size_t>(buffer[i]);
      }
      lengthsRead += chunk;
    }
    if (rawKeyReader) {
      rawKeyReader->skip(childrenElements);
    }
    if (rawElementReader) {
      rawElementReader->skip(childrenElements);
    }
  } else {
    rle->skip(numValues);
  }
  return numValues;
}

void MapColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues,
                           char* notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  MapVectorBatch& mapBatch = dynamic_cast<MapVectorBatch&>(rowBatch);
  int64_t* offsets = mapBatch.offsets.data();
  notNull = mapBatch.hasNulls ? mapBatch.notNull.data() : 0;
  rle->next(offsets, numValues, notNull);
  uint64_t totalChildren = 0;
  if (notNull) {
    for (size_t i = 0; i < numValues; ++i) {
      if (notNull[i]) {
        uint64_t tmp = static_cast<uint64_t>(offsets[i]);
        offsets[i] = static_cast<int64_t>(totalChildren);
        totalChildren += tmp;
      } else {
        offsets[i] = static_cast<int64_t>(totalChildren);
      }
    }
  } else {
    for (size_t i = 0; i < numValues; ++i) {
      uint64_t tmp = static_cast<uint64_t>(offsets[i]);
      offsets[i] = static_cast<int64_t>(totalChildren);
      totalChildren += tmp;
    }
  }
  offsets[numValues] = static_cast<int64_t>(totalChildren);
  ColumnReader* rawKeyReader = keyReader.get();
  if (rawKeyReader) {
    rawKeyReader->next(*(mapBatch.keys.get()), totalChildren, 0);
  }
  ColumnReader* rawElementReader = elementReader.get();
  if (rawElementReader) {
    rawElementReader->next(*(mapBatch.elements.get()), totalChildren, 0);
  }
}

UnionColumnReader::UnionColumnReader(const Type& type, StripeStreams& stripe)
    : ColumnReader(type, stripe) {
  numChildren = type.getSubtypeCount();
  childrenReader.resize(numChildren);
  childrenCounts.resize(numChildren);

  rle = createByteRleDecoder(
      stripe.getStream(columnId, proto::Stream_Kind_DATA, true));
  // figure out which types are selected
  const std::vector<bool> selectedColumns = stripe.getSelectedColumns();
  for (unsigned int i = 0; i < numChildren; ++i) {
    const Type& child = *type.getSubtype(i);
    if (selectedColumns[static_cast<size_t>(child.getColumnId())]) {
      childrenReader[i] = buildReader(child, stripe).release();
    }
  }
}

UnionColumnReader::~UnionColumnReader() {
  for (std::vector<ColumnReader*>::iterator itr = childrenReader.begin();
       itr != childrenReader.end(); ++itr) {
    delete *itr;
  }
}

uint64_t UnionColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  const uint64_t BUFFER_SIZE = 1024;
  char buffer[BUFFER_SIZE];
  uint64_t lengthsRead = 0;
  int64_t* counts = childrenCounts.data();
  memset(counts, 0, sizeof(int64_t) * numChildren);
  while (lengthsRead < numValues) {
    uint64_t chunk = std::min(numValues - lengthsRead, BUFFER_SIZE);
    rle->next(buffer, chunk, 0);
    for (size_t i = 0; i < chunk; ++i) {
      counts[static_cast<size_t>(buffer[i])] += 1;
    }
    lengthsRead += chunk;
  }
  for (size_t i = 0; i < numChildren; ++i) {
    if (counts[i] != 0 && childrenReader[i] != NULL) {
      childrenReader[i]->skip(static_cast<uint64_t>(counts[i]));
    }
  }
  return numValues;
}

void UnionColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues,
                             char* notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  UnionVectorBatch& unionBatch = dynamic_cast<UnionVectorBatch&>(rowBatch);
  uint64_t* offsets = unionBatch.offsets.data();
  int64_t* counts = childrenCounts.data();
  memset(counts, 0, sizeof(int64_t) * numChildren);
  unsigned char* tags = unionBatch.tags.data();
  notNull = unionBatch.hasNulls ? unionBatch.notNull.data() : 0;
  rle->next(reinterpret_cast<char*>(tags), numValues, notNull);
  // set the offsets for each row
  if (notNull) {
    for (size_t i = 0; i < numValues; ++i) {
      if (notNull[i]) {
        offsets[i] =
            static_cast<uint64_t>(counts[static_cast<size_t>(tags[i])]++);
      }
    }
  } else {
    for (size_t i = 0; i < numValues; ++i) {
      offsets[i] =
          static_cast<uint64_t>(counts[static_cast<size_t>(tags[i])]++);
    }
  }
  // read the right number of each child column
  for (size_t i = 0; i < numChildren; ++i) {
    if (childrenReader[i] != nullptr) {
      childrenReader[i]->next(*(unionBatch.children[i]),
                              static_cast<uint64_t>(counts[i]), nullptr);
    }
  }
}

// Destructively convert the number from zigzag encoding to the
// natural signed representation.
void unZigZagInt128(Int128& value) {  // NOLINT
  bool needsNegate = value.getLowBits() & 1;
  value >>= 1;
  if (needsNegate) {
    value.negate();
    value -= 1;
  }
}

const uint32_t Decimal64ColumnReader::MAX_PRECISION_64;
const uint32_t Decimal64ColumnReader::MAX_PRECISION_128;
const int64_t Decimal64ColumnReader::POWERS_OF_TEN[MAX_PRECISION_64 + 1] = {
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

Decimal64ColumnReader::Decimal64ColumnReader(const Type& type,
                                             StripeStreams& stripe)
    : ColumnReader(type, stripe) {
  scale = static_cast<int32_t>(type.getScale());
  precision = static_cast<int32_t>(type.getPrecision());
  valueStream = stripe.getStream(columnId, proto::Stream_Kind_DATA, true);
  buffer = nullptr;
  bufferEnd = nullptr;
  RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
  scaleDecoder = createRleDecoder(
      stripe.getStream(columnId, proto::Stream_Kind_SECONDARY, true), true,
      vers, memoryPool, LONG);
}

Decimal64ColumnReader::~Decimal64ColumnReader() {
  // PASS
}

uint64_t Decimal64ColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  uint64_t skipped = 0;
  while (skipped < numValues) {
    readBuffer();
    if (!(0x80 & *(buffer++))) {
      skipped += 1;
    }
  }
  scaleDecoder->skip(numValues);
  return numValues;
}

void Decimal64ColumnReader::next(ColumnVectorBatch& rowBatch,
                                 uint64_t numValues, char* notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;
  Decimal64VectorBatch& batch = dynamic_cast<Decimal64VectorBatch&>(rowBatch);
  int64_t* values = batch.values.data();
  int64_t* highbits = batch.highbitValues.data();
  // read the next group of scales
  int64_t* scaleBuffer = batch.readScales.data();
  scaleDecoder->next(scaleBuffer, numValues, notNull);
  batch.precision = precision;
  batch.scale = scale;
  if (notNull) {
    for (size_t i = 0; i < numValues; ++i) {
      if (notNull[i]) {
        readInt64(values[i], static_cast<int32_t>(scaleBuffer[i]));
        Int128 val = Int128(values[i]);
        highbits[i] = val.getHighBits();
        values[i] = val.getLowBits();
        scaleBuffer[i] = scale;
      }
    }
  } else {
    for (size_t i = 0; i < numValues; ++i) {
      readInt64(values[i], static_cast<int32_t>(scaleBuffer[i]));
      Int128 val = Int128(values[i]);
      highbits[i] = val.getHighBits();
      values[i] = val.getLowBits();
      scaleBuffer[i] = scale;
    }
  }
}

void scaleInt128(Int128& value, uint32_t scale,  // NOLINT
                 uint32_t currentScale) {
  if (scale > currentScale) {
    while (scale > currentScale) {
      uint32_t scaleAdjust = std::min(Decimal64ColumnReader::MAX_PRECISION_64,
                                      scale - currentScale);
      value *= Decimal64ColumnReader::POWERS_OF_TEN[scaleAdjust];
      currentScale += scaleAdjust;
    }
  } else if (scale < currentScale) {
    Int128 remainder;
    while (currentScale > scale) {
      uint32_t scaleAdjust = std::min(Decimal64ColumnReader::MAX_PRECISION_64,
                                      currentScale - scale);
      value = value.divide(Decimal64ColumnReader::POWERS_OF_TEN[scaleAdjust],
                           remainder);
      currentScale -= scaleAdjust;
    }
  }
}

Decimal128ColumnReader::Decimal128ColumnReader(const Type& type,
                                               StripeStreams& stripe)
    : Decimal64ColumnReader(type, stripe) {
  // PASS
}

Decimal128ColumnReader::~Decimal128ColumnReader() {
  // PASS
}

void Decimal128ColumnReader::next(ColumnVectorBatch& rowBatch,
                                  uint64_t numValues, char* notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;
  Decimal128VectorBatch& batch = dynamic_cast<Decimal128VectorBatch&>(rowBatch);
  Int128* values = batch.values.data();
  int64_t* highbits = batch.highbitValues.data();
  uint64_t* lowbits = batch.lowbitValues.data();
  // read the next group of scales
  int64_t* scaleBuffer = batch.readScales.data();
  scaleDecoder->next(scaleBuffer, numValues, notNull);
  batch.precision = precision;
  batch.scale = scale;
  if (notNull) {
    for (size_t i = 0; i < numValues; ++i) {
      if (notNull[i]) {
        readInt128(values[i], static_cast<int32_t>(scaleBuffer[i]));
        highbits[i] = values[i].getHighBits();
        lowbits[i] = values[i].getLowBits();
      }
    }
  } else {
    for (size_t i = 0; i < numValues; ++i) {
      readInt128(values[i], static_cast<int32_t>(scaleBuffer[i]));
      highbits[i] = values[i].getHighBits();
      lowbits[i] = values[i].getLowBits();
    }
  }
}

DecimalHive11ColumnReader::DecimalHive11ColumnReader(const Type& type,
                                                     StripeStreams& stripe)
    : Decimal64ColumnReader(type, stripe) {
  const ReaderOptions options = stripe.getReaderOptions();
  scale = options.getForcedScaleOnHive11Decimal();
  throwOnOverflow = options.getThrowOnHive11DecimalOverflow();
  errorStream = options.getErrorStream();
}

DecimalHive11ColumnReader::~DecimalHive11ColumnReader() {
  // PASS
}

void DecimalHive11ColumnReader::next(ColumnVectorBatch& rowBatch,
                                     uint64_t numValues, char* notNull) {
  ColumnReader::next(rowBatch, numValues, notNull);
  notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;
  Decimal128VectorBatch& batch = dynamic_cast<Decimal128VectorBatch&>(rowBatch);
  Int128* values = batch.values.data();
  // read the next group of scales
  int64_t* scaleBuffer = batch.readScales.data();

  scaleDecoder->next(scaleBuffer, numValues, notNull);

  batch.precision = precision;
  batch.scale = scale;
  if (notNull) {
    for (size_t i = 0; i < numValues; ++i) {
      if (notNull[i]) {
        if (!readInt128(values[i], static_cast<int32_t>(scaleBuffer[i]))) {
          if (throwOnOverflow) {
            LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                      "Hive 0.11 decimal was more than 38 digits.");
          } else {
            *errorStream << "Warning: "
                         << "Hive 0.11 decimal with more than 38 digits "
                         << "replaced by NULL.\n";
            notNull[i] = false;
          }
        }
      }
    }
  } else {
    for (size_t i = 0; i < numValues; ++i) {
      if (!readInt128(values[i], static_cast<int32_t>(scaleBuffer[i]))) {
        if (throwOnOverflow) {
          LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                    "Hive 0.11 decimal was more than 38 digits.");
        } else {
          *errorStream << "Warning: "
                       << "Hive 0.11 decimal with more than 38 digits "
                       << "replaced by NULL.\n";
          batch.hasNulls = true;
          batch.notNull[i] = false;
        }
      }
    }
  }
}

// Create a reader for the given stripe.
std::unique_ptr<ColumnReader> buildReader(const Type& type,
                                          StripeStreams& stripe) {  // NOLINT
  switch (static_cast<int64_t>(type.getKind())) {
    case SHORT:
      return std::unique_ptr<ColumnReader>(new ShortColumnReader(type, stripe));
    case INT:
      return std::unique_ptr<ColumnReader>(new IntColumnReader(type, stripe));
    case LONG:
      return std::unique_ptr<ColumnReader>(new LongColumnReader(type, stripe));
    case DATE:
      return std::unique_ptr<ColumnReader>(new DateColumnReader(type, stripe));
    case TIME:
      return std::unique_ptr<ColumnReader>(new TimeColumnReader(type, stripe));
    case BINARY:
    case CHAR:
    case STRING:
    case VARCHAR:
      switch (
          static_cast<int64_t>(stripe.getEncoding(type.getColumnId()).kind())) {
        case proto::ColumnEncoding_Kind_DICTIONARY:
        case proto::ColumnEncoding_Kind_DICTIONARY_V2:
        case proto::ColumnEncoding_Kind_DICTIONARY_V0:
          return std::unique_ptr<ColumnReader>(
              new StringDictionaryColumnReader(type, stripe));
        case proto::ColumnEncoding_Kind_DIRECT:
        case proto::ColumnEncoding_Kind_DIRECT_V2:
        case proto::ColumnEncoding_Kind_DIRECT_V0:
          return std::unique_ptr<ColumnReader>(
              new StringDirectColumnReader(type, stripe));
        default:
          LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                    "buildReader unhandled string encoding");
      }

    case BYTE:
      return std::unique_ptr<ColumnReader>(new ByteColumnReader(type, stripe));

    case FLOAT:
    case DOUBLE:
      return std::unique_ptr<ColumnReader>(
          new DoubleColumnReader(type, stripe));

    case BOOLEAN:
      return std::unique_ptr<ColumnReader>(
          new BooleanColumnReader(type, stripe));

    case LIST:
      return std::unique_ptr<ColumnReader>(new ListColumnReader(type, stripe));

    case MAP:
      return std::unique_ptr<ColumnReader>(new MapColumnReader(type, stripe));

    case UNION:
      return std::unique_ptr<ColumnReader>(new UnionColumnReader(type, stripe));

    case STRUCT:
      return std::unique_ptr<ColumnReader>(
          new StructColumnReader(type, stripe));

    case TIMESTAMP:
      return std::unique_ptr<ColumnReader>(
          new TimestampColumnReader(type, stripe));

    case DECIMAL:
      // is this a Hive 0.11 or 0.12 file?
      if (type.getPrecision() == 0) {
        return std::unique_ptr<ColumnReader>(
            new DecimalHive11ColumnReader(type, stripe));

        // can we represent the values using int64_t?
      } else if (type.getPrecision() <=
                 Decimal64ColumnReader::MAX_PRECISION_64) {
        return std::unique_ptr<ColumnReader>(
            new Decimal64ColumnReader(type, stripe));

        // otherwise we use the Int128 implementation
      } else {
        return std::unique_ptr<ColumnReader>(
            new Decimal128ColumnReader(type, stripe));
      }

    default:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "buildReader unhandled type");
  }
}

}  // namespace orc
