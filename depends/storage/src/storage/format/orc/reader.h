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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_READER_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_READER_H_

#include <algorithm>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "univplan/common/statistics.h"

#include "storage/common/bloom-filter.h"
#include "storage/format/orc/byte-rle.h"
#include "storage/format/orc/orc-proto-definition.h"
#include "storage/format/orc/reader.h"
#include "storage/format/orc/rle.h"
#include "storage/format/orc/seekable-input-stream.h"
#include "storage/format/orc/timezone.h"
#include "storage/format/orc/type.h"
#include "storage/format/orc/vector.h"

#include "storage/format/orc/orc_proto.pb.h"

namespace orc {

// classes that hold data members so we can maintain binary compatibility
struct ReaderOptionsPrivate;

// Options for creating a Reader.
class ReaderOptions {
 private:
  std::unique_ptr<ReaderOptionsPrivate> privateBits;

 public:
  ReaderOptions();
  ReaderOptions(const ReaderOptions&);
  ReaderOptions(ReaderOptions&);
  ReaderOptions& operator=(const ReaderOptions&);
  virtual ~ReaderOptions();

  // For files that have structs as the top-level object, select the fields
  // to read. The first field is 0, the second 1, and so on. By default,
  // all columns are read. This option clears any previous setting of
  // the selected columns.
  // @param include a list of fields to read
  // @return this
  ReaderOptions& include(const std::list<uint64_t>& include);

  // For files that have structs as the top-level object, select the fields
  // to read by name. By default, all columns are read. This option clears
  // any previous setting of the selected columns.
  // @param include a list of fields to read
  // @return this
  ReaderOptions& include(const std::list<std::string>& include);

  // Selects which type ids to read. The root type is always 0 and the
  // rest of the types are labeled in a preorder traversal of the tree.
  // The parent types are automatically selected, but the children are not.
  //
  // This option clears any previous setting of the selected columns or
  // types.
  // @param types a list of the type ids to read
  // @return this
  ReaderOptions& includeTypes(const std::list<uint64_t>& types);

  // Set the section of the file to process.
  // @param offset the starting byte offset
  // @param length the number of bytes to read
  // @return this
  ReaderOptions& range(uint64_t offset, uint64_t length);

  // For Hive 0.11 (and 0.12) decimals, the precision was unlimited
  // and thus may overflow the 38 digits that is supported. If one
  // of the Hive 0.11 decimals is too large, the reader may either convert
  // the value to NULL or throw an exception. That choice is controlled
  // by this setting.
  //
  // Defaults to true.
  //
  // @param shouldThrow should the reader throw a ParseError?
  // @return returns *this
  ReaderOptions& throwOnHive11DecimalOverflow(bool shouldThrow);

  // For Hive 0.11 (and 0.12) written decimals, which have unlimited
  // scale and precision, the reader forces the scale to a consistent
  // number that is configured. This setting changes the scale that is
  // forced upon these old decimals. See also throwOnHive11DecimalOverflow.
  //
  // Defaults to 6.
  //
  // @param forcedScale the scale that will be forced on Hive 0.11 decimals
  // @return returns *this
  ReaderOptions& forcedScaleOnHive11Decimal(int32_t forcedScale);

  // Set the location of the tail as defined by the logical length of the
  // file.
  ReaderOptions& setTailLocation(uint64_t offset);

  // Set the stream to use for printing warning or error messages.
  ReaderOptions& setErrorStream(std::ostream& stream);  // NOLINT

  // Open the file used a serialized copy of the file tail.
  //
  // When one process opens the file and other processes need to read
  // the rows, we want to enable clients to just read the tail once.
  // By passing the string returned by Reader.getSerializedFileTail(), to
  // this function, the second reader will not need to read the file tail
  // from disk.
  //
  // @param serialization the bytes of the serialized tail to use
  ReaderOptions& setSerializedFileTail(const std::string& serialization);

  // Get the memory allocator.
  dbcommon::MemoryPool* getMemoryPool() const;

  // Were the field ids set?
  bool getIndexesSet() const;

  // Were the type ids set?
  bool getTypeIdsSet() const;

  // Get the list of selected field or type ids to read.
  const std::list<uint64_t>& getInclude() const;

  // Were the include names set?
  bool getNamesSet() const;

  // Get the list of selected columns to read. All children of the selected
  // columns are also selected.
  const std::list<std::string>& getIncludeNames() const;

  // Get the start of the range for the data being processed.
  // @return if not set, return 0
  uint64_t getOffset() const;

  // Get the end of the range for the data being processed.
  // @return if not set, return the maximum long
  uint64_t getLength() const;

  // Get the desired tail location.
  // @return if not set, return the maximum long.
  uint64_t getTailLocation() const;

  // Should the reader throw a ParseError when a Hive 0.11 decimal is
  // larger than the supported 38 digits of precision? Otherwise, the
  // data item is replaced by a NULL.
  bool getThrowOnHive11DecimalOverflow() const;

  // What scale should all Hive 0.11 decimals be normalized to?
  int32_t getForcedScaleOnHive11Decimal() const;

  // Get the stream to write warnings or errors to.
  std::ostream* getErrorStream() const;

  // Get the serialized file tail that the user passed in.
  std::string getSerializedFileTail() const;

  void setPredicateExprs(const univplan::UnivPlanExprPolyList* predicateExprs);
  const univplan::UnivPlanExprPolyList* getPredicateExprs() const;

  void setTupleDesc(const dbcommon::TupleDesc* td);
  const dbcommon::TupleDesc* getTupleDesc() const;

  void setReadStatsOnlyFlag(bool readStatsOnly);
  bool readStatsOnly() const;
};

class StripeStreams {
 public:
  virtual ~StripeStreams();

  // Get the reader options.
  // @return Reader options
  virtual const ReaderOptions& getReaderOptions() const = 0;

  // Get the array of booleans for which columns are selected.
  // @return the address of an array which contains true at the index of
  //    each columnId is selected.
  virtual const std::vector<bool> getSelectedColumns() const = 0;

  // Get the encoding for the given column for this stripe.
  // @param columnId The column id
  // @return The column encoding
  virtual proto::ColumnEncoding getEncoding(uint64_t columnId) const = 0;

  // Get the stream for the given column/kind in this stripe.
  // @param columnId The id of the column
  // @param kind The kind of the stream
  // @param shouldStream Should the reading page the stream in
  // @return The new stream
  virtual std::unique_ptr<SeekableInputStream> getStream(
      uint64_t columnId, proto::Stream_Kind kind, bool shouldStream) const = 0;

  virtual std::unique_ptr<SeekableInputStream> getStreamForBloomFilter(
      uint64_t columnId, proto::Stream_Kind kind, bool shouldStream) const = 0;

  // Get the memory pool for this reader.
  // @return The memory pool
  virtual dbcommon::MemoryPool& getMemoryPool() const = 0;

  // Get the writer's timezone, so that we can convert their dates correctly.
  // @return The timezone
  virtual const Timezone& getWriterTimezone() const = 0;
};

// The interface for reading ORC data types.
class ColumnReader {
 protected:
  std::unique_ptr<ByteRleDecoder> notNullDecoder;  // it is exact a
                                                   // BooleanRleDecoderImpl, pay
                                                   // attention to the vitual
                                                   // function call
  uint64_t columnId;
  dbcommon::MemoryPool& memoryPool;

 public:
  ColumnReader(const Type& type, StripeStreams& stipe);  // NOLINT

  virtual ~ColumnReader();

  // Skip number of specified rows.
  // @param numValues the number of values to skip
  // @return the number of non-null values skipped
  virtual uint64_t skip(uint64_t numValues);

  // Read the next group of values into this rowBatch.
  // @param rowBatch the memory to read into.
  // @param numValues the number of values to read
  // @param notNull if null, all values are not null. Otherwise, it is
  //           a mask (with at least numValues bytes) for which values to
  //           set.
  virtual void next(ColumnVectorBatch& rowBatch, uint64_t numValues,  // NOLINT
                    char* notNull);
};

class BooleanColumnReader : public ColumnReader {
 private:
  std::unique_ptr<orc::ByteRleDecoder> rle;

 public:
  BooleanColumnReader(const Type& type, StripeStreams& stipe);  // NOLINT
  ~BooleanColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch& rowBatch, uint64_t numValues,
            char* notNull) override;  // NOLINT
};

class ByteColumnReader : public ColumnReader {
 private:
  std::unique_ptr<orc::ByteRleDecoder> rle;

 public:
  ByteColumnReader(const Type& type, StripeStreams& stipe);  // NOLINT
  ~ByteColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch& rowBatch, uint64_t numValues,
            char* notNull) override;  // NOLINT
};

extern RleVersion convertRleVersion(proto::ColumnEncoding_Kind kind);

template <class IntType>
class IntegerColumnReader : public ColumnReader {
 protected:
  std::unique_ptr<orc::RleDecoder> rle;

 public:
  IntegerColumnReader(const Type& type, StripeStreams& stripe);  // NOLINT

  //  :  //NOLINT
  //    ColumnReader(type, stripe) {  //NOLINT
  //    RleVersion vers =
  //    convertRleVersion(stripe.getEncoding(columnId).kind());
  //    rle = createRleDecoder<IntType>(
  //        stripe.getStream(columnId, proto::Stream_Kind_DATA, true), true,
  //        vers,
  //        memoryPool);
  //  }

  ~IntegerColumnReader() {}

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch& rowBatch, uint64_t numValues,
            char* notNull) override;  // NOLINT
};

class LongColumnReader : public IntegerColumnReader<int64_t> {
 public:
  LongColumnReader(const Type& type, StripeStreams& stripe)  // NOLINT
      : IntegerColumnReader<int64_t>(type, stripe) {
    RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
    rle = createRleDecoder(
        stripe.getStream(columnId, proto::Stream_Kind_DATA, true), true, vers,
        memoryPool, LONG);
  }
  ~LongColumnReader() {}
};

class IntColumnReader : public IntegerColumnReader<int32_t> {
 public:
  IntColumnReader(const Type& type, StripeStreams& stripe)  // NOLINT
      : IntegerColumnReader<int32_t>(type, stripe) {
    RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
    rle = createRleDecoder(
        stripe.getStream(columnId, proto::Stream_Kind_DATA, true), true, vers,
        memoryPool, INT);
  }
  ~IntColumnReader() {}
};

class ShortColumnReader : public IntegerColumnReader<int16_t> {
 public:
  ShortColumnReader(const Type& type, StripeStreams& stripe)  // NOLINT
      : IntegerColumnReader<int16_t>(type, stripe) {
    RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
    rle = createRleDecoder(
        stripe.getStream(columnId, proto::Stream_Kind_DATA, true), true, vers,
        memoryPool, SHORT);
  }
  ~ShortColumnReader() {}
};

class DateColumnReader : public ColumnReader {
 private:
  std::unique_ptr<orc::RleDecoder> rle;

 public:
  DateColumnReader(const Type& type, StripeStreams& stripe);  // NOLINT

  ~DateColumnReader() {}

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch& rowBatch, uint64_t numValues,
            char* notNull) override;  // NOLINT
};

class TimeColumnReader : public ColumnReader {
 private:
  std::unique_ptr<orc::RleDecoder> rle;

 public:
  TimeColumnReader(const Type& type, StripeStreams& stripe);  // NOLINT

  ~TimeColumnReader() {}

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch& rowBatch, uint64_t numValues,
            char* notNull) override;  // NOLINT
};

class TimestampColumnReader : public ColumnReader {
 private:
  std::unique_ptr<orc::RleDecoder> secondsRle;
  std::unique_ptr<orc::RleDecoder> nanoRle;
  const Timezone& writerTimezone;
  const int64_t epochOffset;

 public:
  TimestampColumnReader(const Type& type, StripeStreams& stripe);  // NOLINT
  ~TimestampColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch& rowBatch, uint64_t numValues,
            char* notNull) override;  // NOLINT
};

class DoubleColumnReader : public ColumnReader {
 public:
  DoubleColumnReader(const Type& type, StripeStreams& stripe);  // NOLINT
  ~DoubleColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch& rowBatch, uint64_t numValues,
            char* notNull) override;  // NOLINT

 private:
  std::unique_ptr<SeekableInputStream> inputStream;
  ORCTypeKind columnKind;
  const uint64_t bytesPerValue;
  const char* bufferPointer;
  const char* bufferEnd;
  const char* bufferS = nullptr;
  const char* bufferE = nullptr;

  void nextBuffer() {
    int bufferLength = 0;
    const void* bufferPointer = nullptr;
    bool result = inputStream->Next(&bufferPointer, &bufferLength);
    if (!result) {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "bad read in nextBuffer");
    }
    bufferS = static_cast<const char*>(bufferPointer);
    bufferE = bufferS + bufferLength;
  }

  void readData(char* data, uint64_t numValues) {
    uint64_t i = 0;
    uint64_t count;
    if (columnKind == FLOAT)
      count = numValues * sizeof(float);
    else
      count = numValues * sizeof(double);
    while (i < count) {
      if (bufferS == bufferE) {
        nextBuffer();
      }
      uint64_t copyBytes =
          std::min(count - i, static_cast<uint64_t>(bufferE - bufferS));
      memcpy(data, bufferS, copyBytes);
      bufferS += copyBytes;
      data += copyBytes;
      i += copyBytes;
    }
  }
};

class StringDictionaryColumnReader : public ColumnReader {
 private:
  DataBuffer<char> dictionaryBlob;
  DataBuffer<int64_t> dictionaryOffset;
  std::unique_ptr<RleDecoder> rle;
  uint64_t dictionaryCount;

 public:
  StringDictionaryColumnReader(const Type& type,
                               StripeStreams& stipe);  // NOLINT
  ~StringDictionaryColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch& rowBatch, uint64_t numValues,
            char* notNull) override;  // NOLINT
};

class StringDirectColumnReader : public ColumnReader {
 private:
  DataBuffer<char> blobBuffer;
  std::unique_ptr<RleDecoder> lengthRle;
  std::unique_ptr<SeekableInputStream> blobStream;
  const char* lastBuffer;
  size_t lastBufferLength;

  // Compute the total length of the values.
  // @param lengths the array of lengths
  // @param notNull the array of notNull flags
  // @param numValues the lengths of the arrays
  // @return the total number of bytes for the non-null values
  size_t computeSize(const int64_t* lengths, const char* notNull,
                     uint64_t numValues);

 public:
  StringDirectColumnReader(const Type& type, StripeStreams& stipe);  // NOLINT
  ~StringDirectColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch& rowBatch, uint64_t numValues,
            char* notNull) override;  // NOLINT
};

class StructColumnReader : public ColumnReader {
 private:
  std::vector<ColumnReader*> children;

 public:
  StructColumnReader(const Type& type, StripeStreams& stipe);  // NOLINT
  ~StructColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch& rowBatch, uint64_t numValues,
            char* notNull) override;  // NOLINT
};

class ListColumnReader : public ColumnReader {
 private:
  std::unique_ptr<ColumnReader> child;
  std::unique_ptr<RleDecoder> rle;

 public:
  ListColumnReader(const Type& type, StripeStreams& stipe);  // NOLINT
  ~ListColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch& rowBatch, uint64_t numValues,
            char* notNull) override;  // NOLINT
};

class MapColumnReader : public ColumnReader {
 private:
  std::unique_ptr<ColumnReader> keyReader;
  std::unique_ptr<ColumnReader> elementReader;
  std::unique_ptr<RleDecoder> rle;

 public:
  MapColumnReader(const Type& type, StripeStreams& stipe);  // NOLINT
  ~MapColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch& rowBatch, uint64_t numValues,
            char* notNull) override;  // NOLINT
};

class UnionColumnReader : public ColumnReader {
 private:
  std::unique_ptr<ByteRleDecoder> rle;
  std::vector<ColumnReader*> childrenReader;
  std::vector<int64_t> childrenCounts;
  uint64_t numChildren;

 public:
  UnionColumnReader(const Type& type, StripeStreams& stipe);  // NOLINT
  ~UnionColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch& rowBatch, uint64_t numValues,
            char* notNull) override;  // NOLINT
};

class Decimal64ColumnReader : public ColumnReader {
 public:
  static const uint32_t MAX_PRECISION_64 = 18;
  static const uint32_t MAX_PRECISION_128 = 38;
  static const int64_t POWERS_OF_TEN[MAX_PRECISION_64 + 1];

 protected:
  std::unique_ptr<SeekableInputStream> valueStream;
  int32_t precision;
  int32_t scale;
  const char* buffer;
  const char* bufferEnd;

  std::unique_ptr<RleDecoder> scaleDecoder;

  // Read the valueStream for more bytes.
  void readBuffer() {
    while (buffer == bufferEnd) {
      int length;
      if (!valueStream->Next(reinterpret_cast<const void**>(&buffer),
                             &length)) {
        LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                  "Read past end of stream in Decimal64ColumnReader %s",
                  valueStream->getName().c_str());
      }
      bufferEnd = buffer + length;
    }
  }

  void readInt64(int64_t& value, int32_t currentScale) {  // NOLINT
    value = 0;
    size_t offset = 0;
    while (true) {
      readBuffer();
      unsigned char ch = static_cast<unsigned char>(*(buffer++));
      value |= static_cast<uint64_t>(ch & 0x7f) << offset;
      offset += 7;
      if (!(ch & 0x80)) {
        break;
      }
    }
    value = unZigZag(static_cast<uint64_t>(value));
    if (scale > currentScale) {
      value *= POWERS_OF_TEN[scale - currentScale];
    } else if (scale < currentScale) {
      value /= POWERS_OF_TEN[currentScale - scale];
    }
  }

 public:
  Decimal64ColumnReader(const Type& type, StripeStreams& stipe);  // NOLINT
  ~Decimal64ColumnReader();

  uint64_t skip(uint64_t numValues) override;

  void next(ColumnVectorBatch& rowBatch, uint64_t numValues,
            char* notNull) override;  // NOLINT
};

extern void unZigZagInt128(Int128& value);              // NOLINT
extern void scaleInt128(Int128& value, uint32_t scale,  // NOLINT
                        uint32_t currentScale);

class Decimal128ColumnReader : public Decimal64ColumnReader {
 public:
  Decimal128ColumnReader(const Type& type, StripeStreams& stipe);  // NOLINT
  ~Decimal128ColumnReader();

  void next(ColumnVectorBatch& rowBatch, uint64_t numValues,
            char* notNull) override;  // NOLINT

 private:
  void readInt128(Int128& value, int32_t currentScale) {  // NOLINT
    value = 0;
    Int128 work;
    uint32_t offset = 0;
    while (true) {
      readBuffer();
      unsigned char ch = static_cast<unsigned char>(*(buffer++));
      work = ch & 0x7f;
      work <<= offset;
      value |= work;
      offset += 7;
      if (!(ch & 0x80)) {
        break;
      }
    }
    unZigZagInt128(value);
    scaleInt128(value, static_cast<uint32_t>(scale),
                static_cast<uint32_t>(currentScale));
  }
};

class DecimalHive11ColumnReader : public Decimal64ColumnReader {
 private:
  bool throwOnOverflow;
  std::ostream* errorStream;

  // Read an Int128 from the stream and correct it to the desired scale.
  bool readInt128(Int128& value, int32_t currentScale) {  // NOLINT
    // -/+ 99999999999999999999999999999999999999
    static const Int128 MIN_VALUE(-0x4b3b4ca85a86c47b, 0xf675ddc000000001);
    static const Int128 MAX_VALUE(0x4b3b4ca85a86c47a, 0x098a223fffffffff);

    value = 0;
    Int128 work;
    uint32_t offset = 0;
    bool result = true;
    while (true) {
      readBuffer();
      unsigned char ch = static_cast<unsigned char>(*(buffer++));
      work = ch & 0x7f;
      // If we have read more than 128 bits, we flag the error, but keep
      // reading bytes so the stream isn't thrown off.
      if (offset > 128 || (offset == 126 && work > 3)) {
        result = false;
      }
      work <<= offset;
      value |= work;
      offset += 7;
      if (!(ch & 0x80)) {
        break;
      }
    }

    if (!result) {
      return result;
    }
    unZigZagInt128(value);
    scaleInt128(value, static_cast<uint32_t>(scale),
                static_cast<uint32_t>(currentScale));
    return value >= MIN_VALUE && value <= MAX_VALUE;
  }

 public:
  DecimalHive11ColumnReader(const Type& type, StripeStreams& stipe);  // NOLINT
  ~DecimalHive11ColumnReader();

  void next(ColumnVectorBatch& rowBatch, uint64_t numValues,
            char* notNull) override;  // NOLINT
};

// The interface for reading ORC files.
// This is an an abstract class that will subclassed as necessary.
class Reader {
 public:
  virtual ~Reader();

  // Get the format version of the file. Currently known values are:
  // "0.11" and "0.12"
  // @return the version string
  virtual std::string getFormatVersion() const = 0;

  // Get the number of rows in the file.
  // @return the number of rows
  virtual uint64_t getNumberOfRows() const = 0;

  // Get the user metadata keys.
  // @return the set of metadata keys
  virtual std::list<std::string> getMetadataKeys() const = 0;

  // Get a user metadata value.
  // @param key a key given by the user
  // @return the bytes associated with the given key
  virtual std::string getMetadataValue(const std::string& key) const = 0;

  // Did the user set the given metadata value.
  // @param key the key to check
  // @return true if the metadata value was set
  virtual bool hasMetadataValue(const std::string& key) const = 0;

  // Get the compression kind.
  // @return the kind of compression in the file
  virtual CompressionKind getCompression() const = 0;

  // Get the buffer size for the compression.
  // @return number of bytes to buffer for the compression codec.
  virtual uint64_t getCompressionSize() const = 0;

  // Get the version of the writer.
  // @return the version of the writer.
  virtual WriterVersion getWriterVersion() const = 0;

  // Get the number of rows per a entry in the row index.
  // @return the number of rows per an entry in the row index or 0 if there
  // is no row index.
  virtual uint64_t getRowIndexStride() const = 0;

  // Get the number of stripes in the file.
  // @return the number of stripes
  virtual uint64_t getNumberOfStripes() const = 0;

  // Get the information about a stripe.
  // @param stripeIndex the stripe 0 to N-1 to get information about
  // @return the information about that stripe
  virtual std::unique_ptr<StripeInformation> getStripe(
      uint64_t stripeIndex) const = 0;

  // Get the number of stripe statistics in the file.
  // @return the number of stripe statistics
  virtual uint64_t getNumberOfStripeStatistics() const = 0;

  // Get the statistics about a stripe.
  // @param stripeIndex the stripe 0 to N-1 to get statistics about
  // @return the statistics about that stripe
  virtual std::unique_ptr<univplan::Statistics> getStripeStatistics(
      uint64_t stripeIndex) const = 0;

  // Get the length of the data stripes in the file.
  // @return the number of bytes in stripes
  virtual uint64_t getContentLength() const = 0;

  // Get the length of the file stripe statistics
  // @return the number of compressed bytes in the file stripe statistics
  virtual uint64_t getStripeStatisticsLength() const = 0;

  // Get the length of the file footer
  // @return the number of compressed bytes in the file footer
  virtual uint64_t getFileFooterLength() const = 0;

  // Get the length of the file postscript
  // @return the number of bytes in the file postscript
  virtual uint64_t getFilePostscriptLength() const = 0;

  // Get the total length of the file.
  // @return the number of bytes in the file
  virtual uint64_t getFileLength() const = 0;

  // Get the statistics about the columns in the file.
  // @return the information about the column
  virtual std::unique_ptr<univplan::Statistics> getStatistics() const = 0;

  // Get the statistics about a single column in the file.
  // @return the information about the column
  virtual std::unique_ptr<univplan::ColumnStatistics> getColumnStatistics(
      uint32_t columnId) const = 0;

  // Get the type of the rows in the file. The top level is typically a
  // struct.
  // @return the root type
  virtual const Type& getType() const = 0;

  // Get the selected type of the rows in the file. The file's row type
  // is projected down to just the selected columns. Thus, if the file's
  // type is struct<col0:int,col1:double,col2:string> and the selected
  // columns are "col0,col2" the selected type would be
  // struct<col0:int,col2:string>.
  // @return the root type
  virtual const Type& getSelectedType() const = 0;

  // Get the selected columns of the file.
  virtual const std::vector<bool> getSelectedColumns() const = 0;

  // Create a row batch for reading the selected columns of this file.
  // @param size the number of rows to read
  // @return a new ColumnVectorBatch to read into
  virtual std::unique_ptr<ColumnVectorBatch> createRowBatch(
      uint64_t size) const = 0;

  // Read the next row batch from the current position.
  // Caller must look at numElements in the row batch to determine how
  // many rows were read.
  // @param data the row batch to read into.
  // @return true if a non-zero number of rows were read or false if the
  //   end of the file was reached.
  virtual bool next(ColumnVectorBatch& data) = 0;  // NOLINT

  // Get the row number of the first row in the previously read batch.
  // @return the row number of the previous batch.
  virtual uint64_t getRowNumber() const = 0;

  // Seek to a given row.
  // @param rowNumber the next row the reader should return
  virtual void seekToRow(uint64_t rowNumber) = 0;

  // Get the name of the input stream.
  virtual const std::string& getStreamName() const = 0;

  // check file has correct column statistics
  virtual bool hasCorrectStatistics() const = 0;

  // Get the serialized file tail.
  // Usefull if another reader of the same file wants to avoid re-reading
  // the file tail. See ReaderOptions.setSerializedFileTail().
  // @return a string of bytes with the file tail
  virtual std::string getSerializedFileTail() const = 0;

  // Estimate an upper bound on heap memory allocation by the Reader
  // based on the information in the file footer.
  // The bound is less tight if only few columns are read or compression is
  // used.
  // @param stripeIx index of the stripe to be read (if not specified,
  //        all stripes are considered).
  // @return upper bound on memory use
  virtual uint64_t getMemoryUse(int stripeIx = -1) = 0;

  virtual void collectPredicateStats(uint32_t* scanned, uint32_t* skipped) = 0;

  virtual std::unique_ptr<orc::InputStream> ownInputStream() = 0;
};

// Create a reader to the for the ORC file.
// @param stream the stream to read
// @param options the options for reading the file
std::unique_ptr<Reader> createReader(std::unique_ptr<InputStream> stream,
                                     const ReaderOptions& options);

class StripeStreamsImpl;
class ReaderImpl : public Reader {
 private:
  const Timezone& localTimezone;

  // inputs
  std::unique_ptr<InputStream> stream;
  ReaderOptions options;
  const uint64_t fileLength;
  const uint64_t postscriptLength;
  std::vector<bool> selectedColumns;

  // custom memory pool
  dbcommon::MemoryPool& memoryPool;

  // postscript
  std::unique_ptr<proto::PostScript> postscript;
  const uint64_t blockSize;
  const CompressionKind compression;

  // footer
  std::unique_ptr<proto::Footer> footer;
  DataBuffer<uint64_t> firstRowOfStripe;
  uint64_t numberOfStripes;
  std::unique_ptr<Type> schema;
  mutable std::unique_ptr<Type> selectedSchema;

  // metadata
  mutable std::unique_ptr<proto::Metadata> metadata;
  mutable bool isMetadataLoaded;

  // reading state
  uint64_t previousRow;
  uint64_t firstStripe;
  uint64_t currentStripe;
  uint64_t lastStripe;  // the stripe AFTER the last one
  uint64_t currentRowInStripe;
  uint64_t rowsInCurrentStripe;
  proto::StripeInformation currentStripeInfo;
  std::unique_ptr<StripeStreamsImpl> currentStripeStream = nullptr;
  std::vector<proto::StripeFooter> stripeFooters;
  std::unique_ptr<ColumnReader> curReader;
  std::map<std::string, uint64_t> nameIdMap;
  std::map<uint64_t, const Type*> idTypeMap;

  // count for filter push down
  uint32_t skippedStripe = 0;
  uint32_t scannedStripe = 0;

  // for read stats only
  std::unique_ptr<univplan::Statistics> currentStripeStats;

  // internal methods
  proto::StripeFooter getStripeFooter(
      const proto::StripeInformation& info) const;
  void startNextStripe();
  void checkOrcVersion();
  void readMetadata() const;
  bool notIncludeType(ColumnVectorBatch* data, orc::ORCTypeKind typekind);

  // build map from type name and id, id to Type
  void buildTypeNameIdMap(const Type* type,
                          std::vector<std::string>& columns);  // NOLINT
  std::string toDotColumnPath(const std::vector<std::string>& columns);

  // Select the columns from the options object
  void updateSelected();

  // Select a field by name
  void updateSelectedByName(const std::string& name);
  // Select a field by id
  void updateSelectedByFieldId(uint64_t fieldId);
  // Select a type by id
  void updateSelectedByTypeId(uint64_t typeId);

  // Select all of the recursive children of the given type.
  void selectChildren(const Type& type);

  // For each child of type, select it if one of its children
  // is selected.
  bool selectParents(const Type& type);

 public:
  // Constructor that lets the user specify additional options.
  // @param stream the stream to read from
  // @param options options for reading
  // @param postscript the postscript for the file
  // @param footer the footer for the file
  // @param fileLength the length of the file in bytes
  // @param postscriptLength the length of the postscript in bytes
  ReaderImpl(std::unique_ptr<InputStream> stream, const ReaderOptions& options,
             std::unique_ptr<proto::PostScript> postscript,
             std::unique_ptr<proto::Footer> footer, uint64_t fileLength,
             uint64_t postscriptLength);

  const ReaderOptions& getReaderOptions() const;

  CompressionKind getCompression() const override;

  std::string getFormatVersion() const override;

  WriterVersion getWriterVersion() const override;

  uint64_t getNumberOfRows() const override;

  uint64_t getRowIndexStride() const override;

  const std::string& getStreamName() const override;

  std::list<std::string> getMetadataKeys() const override;

  std::string getMetadataValue(const std::string& key) const override;

  bool hasMetadataValue(const std::string& key) const override;

  uint64_t getCompressionSize() const override;

  uint64_t getNumberOfStripes() const override;

  std::unique_ptr<StripeInformation> getStripe(uint64_t) const override;

  uint64_t getNumberOfStripeStatistics() const override;

  std::unique_ptr<univplan::Statistics> getStripeStatistics(
      uint64_t stripeIndex) const override;

  uint64_t getContentLength() const override;
  uint64_t getStripeStatisticsLength() const override;
  uint64_t getFileFooterLength() const override;
  uint64_t getFilePostscriptLength() const override;
  uint64_t getFileLength() const override;

  std::unique_ptr<univplan::Statistics> getStatistics() const override;

  std::unique_ptr<univplan::ColumnStatistics> getColumnStatistics(
      uint32_t columnId) const override;

  const Type& getType() const override;

  const Type& getSelectedType() const override;

  const std::vector<bool> getSelectedColumns() const override;

  std::unique_ptr<ColumnVectorBatch> createRowBatch(
      uint64_t size) const override;

  bool next(ColumnVectorBatch& data) override;

  uint64_t getRowNumber() const override;

  void seekToRow(uint64_t rowNumber) override;

  bool hasCorrectStatistics() const override;

  std::string getSerializedFileTail() const override;

  uint64_t getMemoryUse(int stripeIx = -1) override;

  void collectPredicateStats(uint32_t* scanned, uint32_t* skipped) override;

  std::unique_ptr<orc::InputStream> ownInputStream() override;

  proto::BloomFilterIndex rebuildBloomFilter(uint32_t colId);

  bool doReadStatsOnly(ColumnVectorBatch* data);
};

// Create a reader for the given stripe.
// @param type The reader type
// @param stripe The strip stream
std::unique_ptr<ColumnReader> buildReader(const Type& type,
                                          StripeStreams& stripe);  // NOLINT

class StripeStreamsImpl : public StripeStreams {
 private:
  const ReaderImpl& reader;
  const proto::StripeFooter& footer;
  const uint64_t stripeStart;
  InputStream& input;
  dbcommon::MemoryPool& memoryPool;
  const Timezone& writerTimezone;

 public:
  StripeStreamsImpl(const ReaderImpl& reader, const proto::StripeFooter& footer,
                    uint64_t stripeStart,
                    InputStream& input,                // NOLINT
                    dbcommon::MemoryPool& memoryPool,  // NOLINT
                    const Timezone& writerTimezone);

  virtual ~StripeStreamsImpl();

  const ReaderOptions& getReaderOptions() const override;

  const std::vector<bool> getSelectedColumns() const override;

  proto::ColumnEncoding getEncoding(uint64_t columnId) const override;

  std::unique_ptr<SeekableInputStream> getStream(
      uint64_t columnId, proto::Stream_Kind kind,
      bool shouldStream) const override;

  std::unique_ptr<SeekableInputStream> getStreamForBloomFilter(
      uint64_t columnId, proto::Stream_Kind kind,
      bool shouldStream) const override;

  dbcommon::MemoryPool& getMemoryPool() const override;

  const Timezone& getWriterTimezone() const override;
};

}  // end of namespace orc

#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_READER_H_
