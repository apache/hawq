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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_COLUMN_PRINTER_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_COLUMN_PRINTER_H_

#include <stdio.h>
#include <memory>
#include <string>
#include <vector>

#include "storage/format/orc/input-stream.h"
#include "storage/format/orc/type.h"
#include "storage/format/orc/vector.h"

namespace orc {

extern void writeString(std::string& file, const char* ptr);  // NOLINT

class ColumnPrinter {
 protected:
  std::string& buffer;
  bool hasNulls;
  const char* notNull;

 public:
  explicit ColumnPrinter(std::string&);
  virtual ~ColumnPrinter();
  virtual void printRow(uint64_t rowId) = 0;
  // should be called once at the start of each batch of rows
  virtual void reset(const ColumnVectorBatch& batch);
};

std::unique_ptr<ColumnPrinter> createColumnPrinter(std::string&,
                                                   const Type* type);

class VoidColumnPrinter : public ColumnPrinter {
 public:
  explicit VoidColumnPrinter(std::string&);
  ~VoidColumnPrinter() {}
  void printRow(uint64_t rowId) override;
  void reset(const ColumnVectorBatch& batch) override;
};

class BooleanColumnPrinter : public ColumnPrinter {
 private:
  const int64_t* data;

 public:
  explicit BooleanColumnPrinter(std::string&);
  ~BooleanColumnPrinter() {}
  void printRow(uint64_t rowId) override;
  void reset(const ColumnVectorBatch& batch) override;
};

template <class ElementType>
class FixedSizeColumnPrinter : public ColumnPrinter {
 private:
  const ElementType* data;

 public:
  explicit FixedSizeColumnPrinter(std::string& buffer)
      :  // NOLINT
        ColumnPrinter(buffer),
        data(nullptr) {}
  ~FixedSizeColumnPrinter() {}

  void reset(const ColumnVectorBatch& batch) override {
    ColumnPrinter::reset(batch);
    data = reinterpret_cast<const ElementType*>(batch.getData());
  }

  void printRow(uint64_t rowId) override {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      std::stringstream ss;
      ss << data[rowId];
      writeString(buffer, ss.str().c_str());
    }
  }
};

class LongColumnPrinter : public FixedSizeColumnPrinter<int64_t> {
 public:
  explicit LongColumnPrinter(std::string& buffer)
      :  // NOLINT
        FixedSizeColumnPrinter<int64_t>(buffer) {}
  ~LongColumnPrinter() {}
};

class IntColumnPrinter : public FixedSizeColumnPrinter<int32_t> {
 public:
  explicit IntColumnPrinter(std::string& buffer)
      :  // NOLINT
        FixedSizeColumnPrinter<int32_t>(buffer) {}
  ~IntColumnPrinter() {}
};

class ShortColumnPrinter : public FixedSizeColumnPrinter<int16_t> {
 public:
  explicit ShortColumnPrinter(std::string& buffer)
      :  // NOLINT
        FixedSizeColumnPrinter<int16_t>(buffer) {}
  ~ShortColumnPrinter() {}
};

class FloatColumnPrinter : public ColumnPrinter {
 private:
  const float* data;

 public:
  explicit FloatColumnPrinter(std::string&, const Type& type);
  virtual ~FloatColumnPrinter() {}
  void printRow(uint64_t rowId) override;
  void reset(const ColumnVectorBatch& batch) override;
};

class DoubleColumnPrinter : public ColumnPrinter {
 private:
  const double* data;

 public:
  explicit DoubleColumnPrinter(std::string&, const Type& type);
  virtual ~DoubleColumnPrinter() {}
  void printRow(uint64_t rowId) override;
  void reset(const ColumnVectorBatch& batch) override;
};

class TimestampColumnPrinter : public ColumnPrinter {
 private:
  const int64_t* seconds;
  const int64_t* nanoseconds;

 public:
  explicit TimestampColumnPrinter(std::string&);
  ~TimestampColumnPrinter() {}
  void printRow(uint64_t rowId) override;
  void reset(const ColumnVectorBatch& batch) override;
};

class DateColumnPrinter : public ColumnPrinter {
 private:
  const int64_t* data;

 public:
  explicit DateColumnPrinter(std::string&);
  ~DateColumnPrinter() {}
  void printRow(uint64_t rowId) override;
  void reset(const ColumnVectorBatch& batch) override;
};

class TimeColumnPrinter : public ColumnPrinter {
 private:
  const int64_t* data;

 public:
  explicit TimeColumnPrinter(std::string&);
  ~TimeColumnPrinter() {}
  void printRow(uint64_t rowId) override;
  void reset(const ColumnVectorBatch& batch) override;
};

class Decimal64ColumnPrinter : public ColumnPrinter {
 private:
  const int64_t* data;
  int32_t scale;

 public:
  explicit Decimal64ColumnPrinter(std::string&);
  ~Decimal64ColumnPrinter() {}
  void printRow(uint64_t rowId) override;
  void reset(const ColumnVectorBatch& batch) override;
};

class Decimal128ColumnPrinter : public ColumnPrinter {
 private:
  const Int128* data;
  int32_t scale;

 public:
  explicit Decimal128ColumnPrinter(std::string&);
  ~Decimal128ColumnPrinter() {}
  void printRow(uint64_t rowId) override;
  void reset(const ColumnVectorBatch& batch) override;
};

class StringColumnPrinter : public ColumnPrinter {
 private:
  const char* const* start;
  const int64_t* length;

 public:
  explicit StringColumnPrinter(std::string&);
  virtual ~StringColumnPrinter() {}
  void printRow(uint64_t rowId) override;
  void reset(const ColumnVectorBatch& batch) override;
};

class BinaryColumnPrinter : public ColumnPrinter {
 private:
  const char* const* start;
  const int64_t* length;

 public:
  explicit BinaryColumnPrinter(std::string&);
  virtual ~BinaryColumnPrinter() {}
  void printRow(uint64_t rowId) override;
  void reset(const ColumnVectorBatch& batch) override;
};

class ListColumnPrinter : public ColumnPrinter {
 private:
  const int64_t* offsets;
  std::unique_ptr<ColumnPrinter> elementPrinter;

 public:
  ListColumnPrinter(std::string&, const Type& type);
  virtual ~ListColumnPrinter() {}
  void printRow(uint64_t rowId) override;
  void reset(const ColumnVectorBatch& batch) override;
};

class MapColumnPrinter : public ColumnPrinter {
 private:
  const int64_t* offsets;
  std::unique_ptr<ColumnPrinter> keyPrinter;
  std::unique_ptr<ColumnPrinter> elementPrinter;

 public:
  MapColumnPrinter(std::string&, const Type& type);
  virtual ~MapColumnPrinter() {}
  void printRow(uint64_t rowId) override;
  void reset(const ColumnVectorBatch& batch) override;
};

class UnionColumnPrinter : public ColumnPrinter {
 private:
  const unsigned char* tags;
  const uint64_t* offsets;
  std::vector<ColumnPrinter*> fieldPrinter;

 public:
  UnionColumnPrinter(std::string&, const Type& type);
  virtual ~UnionColumnPrinter();
  void printRow(uint64_t rowId) override;
  void reset(const ColumnVectorBatch& batch) override;
};

class StructColumnPrinter : public ColumnPrinter {
 private:
  std::vector<ColumnPrinter*> fieldPrinter;
  std::vector<std::string> fieldNames;
  std::vector<std::string> fieldTypes;

 public:
  StructColumnPrinter(std::string&, const Type& type);
  virtual ~StructColumnPrinter();
  void printRow(uint64_t rowId) override;
  void reset(const ColumnVectorBatch& batch) override;
};

}  // namespace orc
#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_COLUMN_PRINTER_H_
