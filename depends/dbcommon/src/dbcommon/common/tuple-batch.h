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

#ifndef DBCOMMON_SRC_DBCOMMON_COMMON_TUPLE_BATCH_H_
#define DBCOMMON_SRC_DBCOMMON_COMMON_TUPLE_BATCH_H_

#include <cassert>
#include <string>
#include <vector>

#include "dbcommon/common/node-deserializer.h"
#include "dbcommon/common/node-serializer.h"
#include "dbcommon/common/tuple-desc.h"
#include "dbcommon/common/vector.h"

#include "dbcommon/log/logger.h"

namespace dbcommon {

class Vector;
class JoinTupleBuffer;
typedef std::vector<std::unique_ptr<Vector>> TupleBatchReader;
typedef std::vector<std::unique_ptr<Vector>> TupleBatchWriter;

class TupleBatch {
 public:
  explicit TupleBatch(bool hasSysCol = false);
  explicit TupleBatch(uint32_t nfield, bool hasSysCol = false);
  explicit TupleBatch(int32_t nfield, bool hasSysCol = false);
  explicit TupleBatch(size_t nfield, bool hasSysCol = false);
  TupleBatch(const TupleDesc &desc, bool ownData, bool hasSysCol = false);
  ~TupleBatch() {}

  TupleBatch(const TupleBatch &other) = delete;
  TupleBatch &operator=(const TupleBatch &other) = delete;
  TupleBatch(TupleBatch &&other) = delete;
  TupleBatch &operator=(TupleBatch &&other) = delete;

  // only call this when you get a clean tuple batch
  // for example, you get an empty tuplebatch by new TupleBatch(),
  // or you just called reset()
  void rebuild(const TupleDesc &desc, bool ownData, bool hasSysCol);

  // Get the tuple batch size in Bytes. The size is
  // not accurate since it only contains data/offset/nullbitmap
  // @return The tuple batch size in Bytes
  int64_t getTupleBatchSize() const;

  uint32_t getNumOfRows() const {
    return selected_ ? selected_->size() : this->numOfRowsPlain;
  }

  // Returns whether the TupleBatch is empty (i.e. whether its size is 0).
  bool empty() const { return getNumOfRows() == 0; }

  uint32_t getNumOfRowsPlain() const { return this->numOfRowsPlain; }

  uint16_t getRgId() const { return this->rgId; }

  void setRgId(uint16_t rgId) { this->rgId = rgId; }

  void setNumOfRows(uint32_t rows) { this->numOfRowsPlain = rows; }

  void incNumOfRows(uint32_t inc) { this->numOfRowsPlain += inc; }

  void addColumn(TypeKind dataType, int64_t typeMod, bool ownData);

  void addColumn(std::unique_ptr<Vector> vec) {
    vectorRefs.push_back(vec.get());
    vectors.push_back(std::move(vec));
  }

  void addSysColumn(std::unique_ptr<Vector> vec) {
    sysVectorRefs.push_back(vec.get());
    sysVectors.push_back(std::move(vec));
  }

  void addSysColumn(TypeKind dataType, int64_t typeMod, bool ownData);

  uint32_t getNumOfColumns() const { return vectors.size(); }

  uint32_t getNumOfSysColumns() const { return sysVectors.size(); }

  Vector *getColumn(int32_t index) const {
    // Intend to provide scalar tuple for executor
    if (isScalarTupleBatch_) {
      return const_cast<Vector *>(
          reinterpret_cast<const Vector *>(&scalars_[index]));
    }

    if (index >= 0) {
      assert(index < vectors.size() && "index out of range");
      return vectorRefs[index];
    } else {
      index = -index - 1;
      assert(index < sysVectors.size() && "index out of range");
      return sysVectorRefs[index];
    }
  }

  std::unique_ptr<Vector> releaseColumn(int32_t index) {
    if (index >= 0) {
      assert(index < vectors.size() && "index out of range");
      return std::unique_ptr<Vector>(vectors[index].release());
    } else {
      index = -index - 1;
      assert(index < sysVectors.size() && "index out of range");
      return std::unique_ptr<Vector>(sysVectors[index].release());
    }
  }

  void clearColumnRef(int32_t index) {
    if (index >= 0) {
      assert(index < vectorRefs.size() && "index out of range");
      assert(vectors[index] == nullptr);  // only allow to clear ref when this
                                          // vector is not owned
      vectorRefs[index] = nullptr;
    } else {
      index = -index - 1;
      assert(index < sysVectors.size() && "index out of range");
      assert(sysVectors[index] == nullptr);  // only allow to clear ref when
                                             // this vector is not owned
      sysVectorRefs[index] = nullptr;
    }
  }

  void swapColumn(uint32_t index, std::unique_ptr<Vector> *vec);

  void swapSysColumn(int32_t index, std::unique_ptr<Vector> *vec);

  void setColumn(int32_t index, std::unique_ptr<Vector> vect,
                 bool useParentSelected);

  void setColumn(int32_t index, Vector *vect, bool useParentSelected);

  void insertColumn(int32_t index, TypeKind dataType, int64_t typeMod,
                    bool ownData);

  void insertColumn(int32_t index, std::unique_ptr<Vector> vect,
                    bool useParentSelected);

  void removeColumn(uint32_t idx) {
    vectorRefs.erase(vectorRefs.begin() + idx);
    vectors.erase(vectors.begin() + idx);
  }

  void removeSysColumn(int32_t idx) {
    uint32_t sysIdx = -idx - 1;
    assert(sysIdx < sysVectors.size());
    sysVectorRefs.erase(sysVectorRefs.begin() + sysIdx);
    sysVectors.erase(sysVectors.begin() + sysIdx);
  }

  // Materialize the tuple batch
  // @return void
  void materialize();

  // Clone selected(plain index) tuples in the tuple batch
  // @param sel The input select list
  // @return The result tuple batch
  std::unique_ptr<TupleBatch> cloneSelected(const SelectList *sel) const;

  // Clone a plain tb according to the input select list in some range
  // The result tb is a plain tuple batch
  // @param sel The input select list, plain index
  // @param start The start input plain index
  // @param end The end input plain index (not included)
  // @return The tb cloned
  std::unique_ptr<TupleBatch> extract(const SelectList *sel, uint64_t start,
                                      uint64_t end) const;
  std::unique_ptr<TupleBatch> extract(uint64_t start, uint64_t end) const;

  std::unique_ptr<TupleBatch> clone() const {
    return cloneSelected(getSelected());
  }

  // Create a new TupleBatch that contains multiple replication of the specific
  // row.
  // @param rowIndex Plain index
  std::unique_ptr<TupleBatch> replicateRowsToTB(uint32_t rowIndex,
                                                int replicateNum) const;

  // Create a new TupleBatch that contains the specific  row.
  // @param rowIndex Plain index
  std::unique_ptr<TupleBatch> getRow(uint32_t rowIndex) const;

  // Add Vectors from other TupleBatch to this TupleBatch
  // The number of rows does not change in this function,
  // But the number of columns is increased.
  // @param other The input tuple batch
  // @return void
  void concatTB(std::unique_ptr<TupleBatch> other);

  bool isUseSelected() const { return selected_ != nullptr; }

  void setSelected(const SelectList &selected);

  void setSelected(SelectList *selected);

  void unsetSelected();

  SelectList *getSelected() { return selected_; }

  const SelectList *getSelected() const { return selected_; }

  std::unique_ptr<SelectList> releaseSelected() {
    return std::move(selectListOwner_);
  }

  void reset();

  void append(const TupleBatch &tb, uint32_t index);

  void append(TupleBatch *tb) { append(tb, tb->selected_); }

  void append(TupleBatch *tupleBatch, SelectList *sel);

  typedef std::unique_ptr<TupleBatch> uptr;

  const TupleBatchReader &getTupleBatchReader() const { return vectors; }

  TupleBatchWriter &getTupleBatchWriter();

  const TupleBatchReader &getTupleBatchSysReader() const { return sysVectors; }

  TupleBatchWriter &getTupleBatchSysWriter();

  std::string toString() const;

  // Used only for debug: validity check
  // @return a bool value indicating whether it is valid
  bool isValid() const;

  std::unique_ptr<TupleBatch> getTuple(uint32_t index) const;

  void setNull(uint32_t index);
  void appendNull(uint32_t tupleCount);
  void convert(const dbcommon::SupportedEncodingSet inputEncoding,
               const dbcommon::SupportedEncodingSet outputEncoding) {
    if (inputEncoding == outputEncoding) return;
    for (int i = 0; i < vectors.size(); i++) {
      vectors[i]->convert(inputEncoding, outputEncoding);
    }
    for (int i = 0; i < sysVectors.size(); i++) {
      sysVectors[i]->convert(inputEncoding, outputEncoding);
    }
    materialize();
  }
  void deserialize(const std::string &data);
  void deserialize(const std::string &data, int32_t startPos, uint32_t end);
  size_t serialize(std::string *output, size_t reserved);

  // @param projIdxs map each vector[i] into vector[projIdxs[i]]
  void permutate(const std::vector<uint32_t> &projIdxs);

  bool ownData() const;

  void trim();

  bool isScalarTupleBatch() { return isScalarTupleBatch_; }

  std::unique_ptr<TupleDesc> mockTupleDesc();

  double getMemUsed();

  void replaceDecimalVector();

  static void initApTupleBatch(const dbcommon::TupleDesc &desc,
                               dbcommon::TupleBatch *tbOut);

 private:
  inline uint32_t calculateSizeOfMaskBits(uint32_t colSize,
                                          uint32_t sysColSize) {
    return (colSize + sysColSize + 7) / 8;  // use one byte to express 8 columns
  }

  inline void buildColBitMasks(std::string *res) {
    uint32_t size = calculateSizeOfMaskBits(vectors.size(), sysVectors.size());
    assert(size >= 0);
    res->assign(size, '\0');
    char *data = const_cast<char *>(res->data());
    int totalCount = 0;
    for (int i = 0; i < sysVectors.size(); ++i) {
      if (sysVectors[i] != nullptr) {
        data[totalCount / 8] |= (1 << (7 - (totalCount % 8)));
      }
      totalCount++;
    }
    for (int i = 0; i < vectors.size(); ++i) {
      if (vectors[i] != nullptr) {
        data[totalCount / 8] |= (1 << (7 - (totalCount % 8)));
      }
      totalCount++;
    }
  }

  inline bool isColMaskUnavailable(const char *bits, uint32_t sysColSize,
                                   int colIndex) {
    int totalOffset = colIndex + sysColSize;  // as syscol index is negative
    return (bits[totalOffset / 8] & (1 << (7 - (totalOffset % 8)))) == 0;
  }

 private:
  bool isScalarTupleBatch_ = false;
  uint32_t numOfRowsPlain = 0;
  uint16_t rgId = 0;
  std::vector<std::unique_ptr<Vector>> vectors;
  std::vector<std::unique_ptr<Vector>> sysVectors;
  std::vector<Vector *> vectorRefs;
  std::vector<Vector *> sysVectorRefs;
  std::vector<Scalar> scalars_;
  SelectList *selected_ = nullptr;
  std::unique_ptr<SelectList> selectListOwner_;

  friend JoinTupleBuffer;
};
}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_COMMON_TUPLE_BATCH_H_
