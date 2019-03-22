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

#ifndef DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_INTERVAL_VECTOR_H_
#define DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_INTERVAL_VECTOR_H_

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "dbcommon/common/vector.h"
#include "dbcommon/type/interval.h"

namespace dbcommon {

class IntervalVector : public Vector {
 public:
  explicit IntervalVector(bool ownData)
      : Vector(ownData), dayValues(ownData), monthValues(ownData) {
    if (ownData) {
      values.reserve(sizeof(uint64_t) * DEFAULT_NUMBER_TUPLES_PER_BATCH);
      dayValues.reserve(sizeof(int32_t) * DEFAULT_NUMBER_TUPLES_PER_BATCH);
      monthValues.reserve(sizeof(int32_t) * DEFAULT_NUMBER_TUPLES_PER_BATCH);
    }
    setTypeKind(INTERVALID);
  }

  ~IntervalVector() override {}

  uint64_t getNumOfRowsPlain() const override {
    return values.size() / sizeof(int64_t);
  }

  dbcommon::ByteBuffer *getValueBuffer() override { return &values; }

  dbcommon::ByteBuffer *getDayBuffer() override { return &dayValues; }

  dbcommon::ByteBuffer *getMonthBuffer() override { return &monthValues; }

  const char *getDayValue() const override { return dayValues.data(); }

  const char *getMonthValue() const override { return monthValues.data(); }

  void setValue(const char *value, uint64_t size) override {
    assert(!getOwnData() && value != nullptr && size >= 0);
    values.setData(value, size);
  }

  void setDayValue(const char *value, uint64_t size) override {
    assert(!getOwnData() && value != nullptr && size >= 0);
    dayValues.setData(value, size);
  }

  void setMonthValue(const char *value, uint64_t size) override {
    assert(!getOwnData() && value != nullptr && size >= 0);
    monthValues.setData(value, size);
  }

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
    dayValues.resize(0);
    monthValues.resize(0);
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
      assert(nulls.size() == values.size() / sizeof(int64_t));
      assert(nulls.size() == dayValues.size() / sizeof(int32_t));
      assert(nulls.size() == monthValues.size() / sizeof(int32_t));
    }
    return true;
  }

  void hash(std::vector<uint64_t> &retval) const override;

  void cdbHash(std::vector<uint64_t> &retval) const override;

  void swap(Vector &vect) override;

  void checkGroupByKeys(std::vector<const void *> &dataPtrSrc) const override;

  uint64_t checkGroupByKey(uint64_t index, const char *dataPtr) const override;

  uint64_t getSerializeSizeInternal() const override;

  void serializeInternal(NodeSerializer *serializer) override;

  void deserializeInternal(NodeDeserializer *deserializer) override;

  void selectDistinct(SelectList *selected, Scalar *scalar) override;

  void selectDistinct(SelectList *selected, Vector *vect) override;

 protected:
  void resizeInternal(size_t plainSize) override;

  dbcommon::ByteBuffer dayValues;
  dbcommon::ByteBuffer monthValues;
  mutable IntervalVar readBuffer[2];
  mutable uint8_t nextBuffer = 0;
  struct IntervalDataInfo {
    char isNull;
    char padding[7];
    int64_t timeOffsetData;
    int32_t dayData;
    int32_t monthData;
  };
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_INTERVAL_VECTOR_H_
