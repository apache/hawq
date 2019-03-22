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

#ifndef DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_DECIMAL_VECTOR_H_
#define DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_DECIMAL_VECTOR_H_

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "dbcommon/common/vector.h"
#include "dbcommon/type/decimal.h"

namespace dbcommon {

class DecimalVector : public Vector {
 public:
  explicit DecimalVector(bool ownData)
      : Vector(ownData),
        auxiliaryValues(ownData),
        scaleValues(ownData),
        rawValues(true),
        lengths(true),
        valPtrs(true) {
    if (ownData) {
      values.reserve(sizeof(uint64_t) * DEFAULT_NUMBER_TUPLES_PER_BATCH);
      auxiliaryValues.reserve(sizeof(int64_t) *
                              DEFAULT_NUMBER_TUPLES_PER_BATCH);
      scaleValues.reserve(sizeof(int64_t) * DEFAULT_NUMBER_TUPLES_PER_BATCH);
    }
    setTypeKind(DECIMALNEWID);
  }

  ~DecimalVector() override {}

  uint64_t getNumOfRowsPlain() const override {
    return values.size() / sizeof(uint64_t);
  }

  dbcommon::ByteBuffer *getAuxiliaryValueBuffer() override {
    return &auxiliaryValues;
  }

  dbcommon::ByteBuffer *getScaleValueBuffer() override { return &scaleValues; }

  const char *getAuxiliaryValue() const override {
    return auxiliaryValues.data();
  }

  const char *getScaleValue() const override { return scaleValues.data(); }

  void setValue(const char *value, uint64_t size) override {
    assert(!getOwnData() && value != nullptr && size >= 0);
    values.setData(value, size);
  }

  void setAuxiliaryValue(const char *value, uint64_t size) override {
    assert(!getOwnData() && value != nullptr && size >= 0);
    auxiliaryValues.setData(value, size);
  }

  void setScaleValue(const char *value, uint64_t size) override {
    assert(!getOwnData() && value != nullptr && size >= 0);
    scaleValues.setData(value, size);
  }

  bool checkZeroValue() const override;

  const char *read(uint64_t index, uint64_t *len, bool *null) const override;

  std::string readPlain(uint64_t index, bool *null) const override;

  void readPlainScalar(uint64_t index, Scalar *scalar) const override;

  void append(const std::string &strValue, bool null) override;

  void append(const char *v, uint64_t valueLen, bool null) override;

  void append(const Datum &datum, bool null) override;

  void append(const Scalar *scalar) override;

  void append(Vector *src, uint64_t index) override;

  void append(Vector *vect) override;

  void append(Vector *vect, SelectList *sel) override;

  void materialize() override;

  void reset(bool resetBelongingTupleBatch = false) override {
    Vector::reset(resetBelongingTupleBatch);
    values.resize(0);
    auxiliaryValues.resize(0);
    scaleValues.resize(0);
    rawValues.resize(0);
    lengths.resize(0);
    valPtrs.resize(0);
  }

  std::unique_ptr<Vector> cloneSelected(const SelectList *sel) const override;

  std::unique_ptr<Vector> extract(const SelectList *sel, uint64_t start,
                                  uint64_t end) const override;

  std::unique_ptr<Vector> replicateItemToVec(uint64_t index,
                                             int replicateNum) const override;

  void merge(size_t plainSize, const std::vector<Datum> &data,
             const std::vector<SelectList *> &sels) override;

  bool isValid() const override {
    Vector::isValid();

    if (hasNulls) {
      assert(nulls.size() == values.size() / sizeof(uint64_t));
      assert(nulls.size() == auxiliaryValues.size() / sizeof(int64_t));
      assert(nulls.size() == scaleValues.size() / sizeof(int64_t));
    }
    return true;
  }

  void hash(std::vector<uint64_t> &retval) const override;

  void cdbHash(std::vector<uint64_t> &retval) const override;

  void swap(Vector &vect) override;

  dbcommon::ByteBuffer *getValueBuffer() override { return &values; }

  const char **getValPtrs() const override {
    return reinterpret_cast<const char **>(valPtrs.data());
  }

  const uint64_t *getLengths() const override {
    return reinterpret_cast<const uint64_t *>(lengths.data());
  }

  void checkGroupByKeys(std::vector<const void *> &dataPtrSrc) const override;

  uint64_t checkGroupByKey(uint64_t index, const char *dataPtr) const override;

  uint64_t getSerializeSizeInternal() const override;

  void serializeInternal(NodeSerializer *serializer) override;

  void deserializeInternal(NodeDeserializer *deserializer) override;

  void selectDistinct(SelectList *selected, Scalar *scalar) override;

  void selectDistinct(SelectList *selected, Vector *vect) override;

  const char *readRawValue(uint64_t index, uint64_t *len, bool *null) const;

  void computeRawValueAndValPtrs();

 protected:
  void resizeInternal(size_t plainSize) override;

  static uint64_t computeValPtrs(const char *vals, const uint64_t *lens,
                                 const bool *nulls, uint64_t size,
                                 const char **valPtrs) {
    const char *ptr = vals;

    if (nulls) {
      for (uint64_t i = 0; i < size; i++) {
        valPtrs[i] = ptr;
        ptr += nulls[i] ? 0 : lens[i];
      }
    } else {
      for (uint64_t i = 0; i < size; i++) {
        valPtrs[i] = ptr;
        ptr += lens[i];
      }
    }

    return ptr - vals;
  }
  dbcommon::ByteBuffer auxiliaryValues;
  dbcommon::ByteBuffer scaleValues;
  dbcommon::ByteBuffer rawValues;
  dbcommon::ByteBuffer lengths;
  dbcommon::ByteBuffer valPtrs;
  mutable DecimalVar readBuffer[2];
  mutable uint8_t nextBuffer = 0;
  struct DecimalDataInfo {
    char isNull;
    char padding[sizeof(DecimalVar) - 1];
    int64_t highbitsData;
    uint64_t lowbitsData;
    int64_t scaleData;
  };
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_DECIMAL_VECTOR_H_
