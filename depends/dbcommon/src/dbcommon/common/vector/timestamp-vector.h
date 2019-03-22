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

#ifndef DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_TIMESTAMP_VECTOR_H_
#define DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_TIMESTAMP_VECTOR_H_

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "dbcommon/common/vector-transformer.h"
#include "dbcommon/common/vector.h"

namespace dbcommon {

class TimestampVector : public Vector {
 public:
  explicit TimestampVector(bool ownData)
      : Vector(ownData), nanoseconds(ownData) {
    if (ownData) {
      values.reserve(sizeof(int64_t) * DEFAULT_NUMBER_TUPLES_PER_BATCH);
      nanoseconds.reserve(sizeof(int64_t) * DEFAULT_NUMBER_TUPLES_PER_BATCH);
    }
    setTypeKind(TIMESTAMPID);
  }

  ~TimestampVector() override {}

  const char *getNanoseconds() const override { return nanoseconds.data(); }

  uint64_t getNumOfRowsPlain() const override {
    assert(values.size() == nanoseconds.size());
    return values.size() / sizeof(int64_t);
  }

  void setValue(const char *value, uint64_t size) override {
    assert(!getOwnData() && value != nullptr && size >= 0);
    values.setData(value, size);
  }

  void setNanoseconds(const char *value, uint64_t size) override {
    assert(!getOwnData() && value != nullptr && size >= 0);
    nanoseconds.setData(value, size);
  }

  void materialize() override {
    values.materialize<int64_t>(selected);
    nanoseconds.materialize<int64_t>(selected);
    if (hasNulls) nulls.materialize(selected);
    selected = nullptr;
  }

  std::unique_ptr<Vector> cloneSelected(const SelectList *sel) const override {
    assert(isValid());

    std::unique_ptr<Vector> retval = Vector::BuildVector(getTypeKind(), true);
    TimestampVector *rt = static_cast<TimestampVector *>(retval.get());

    rt->setTypeKind(typeKind);
    rt->setHasNull(hasNulls);
    assert(values.size() == nanoseconds.size());
    if (!sel || sel->size() == values.size() / sizeof(int64_t)) {
      dbcommon::ByteBuffer valuebuf(true);
      dbcommon::ByteBuffer nanosecondsbuf(true);
      valuebuf.append(values.data(), values.size());
      nanosecondsbuf.append(nanoseconds.data(), nanoseconds.size());
      rt->values.swap(valuebuf);
      rt->nanoseconds.swap(nanosecondsbuf);
      if (hasNulls) rt->nulls.append(nulls.data(), nulls.size());
      rt->selectListOwner.reset();
      rt->selected = nullptr;
    } else {
      uint64_t size = sel->size();
      dbcommon::ByteBuffer valbuf(true);
      dbcommon::ByteBuffer nanosecondsbuf(true);

      if (hasNulls) {
        for (uint64_t i = 0; i < size; ++i) {
          valbuf.append(values.get<int64_t>((*sel)[i]));
          nanosecondsbuf.append(nanoseconds.get<int64_t>((*sel)[i]));
          rt->nulls.append(nulls.get((*sel)[i]));
        }
      } else {
        for (uint64_t i = 0; i < size; ++i) {
          valbuf.append(values.get<int64_t>((*sel)[i]));
          nanosecondsbuf.append(nanoseconds.get<int64_t>((*sel)[i]));
        }
      }

      rt->values.swap(valbuf);
      rt->nanoseconds.swap(nanosecondsbuf);
      rt->selectListOwner.reset();
      rt->selected = nullptr;
    }

    assert(rt->isValid());
    return std::move(retval);
  }

  std::unique_ptr<Vector> extract(const SelectList *sel, uint64_t start,
                                  uint64_t end) const override {
    assert(isValid() && start < end);
    uint64_t total = getNumOfRowsPlain();
    assert(end <= total);
    uint64_t extSz = end - start;

    std::unique_ptr<Vector> retval = Vector::BuildVector(getTypeKind(), false);
    TimestampVector *rt = static_cast<TimestampVector *>(retval.get());

    rt->setTypeKind(typeKind);
    rt->setHasNull(hasNulls);
    if (hasNulls) rt->setNulls(getNulls() + start, extSz);

    uint64_t typLen = sizeof(int64_t);
    if (values.data())
      rt->setValue(values.data() + start * typLen, extSz * typLen);
    if (nanoseconds.data())
      rt->setNanoseconds(nanoseconds.data() + start * typLen, extSz * typLen);
    rt->setSelected(sel, true);

    assert(rt->isValid());
    return std::move(retval);
  }

  std::unique_ptr<Vector> replicateItemToVec(uint64_t index,
                                             int replicateNum) const override {
    assert(isValid());

    auto retval = Vector::BuildVector(getTypeKind(), true);
    auto rt = static_cast<TimestampVector *>(retval.get());

    rt->setTypeKind(typeKind);
    rt->setHasNull(hasNulls);

    dbcommon::ByteBuffer valbuf(true);
    dbcommon::ByteBuffer nanosecondsbuf(true);
    uint64_t idx = getPlainIndex(index);
    auto value = values.get<int64_t>(idx);
    auto nanosecond = nanoseconds.get<int64_t>(idx);
    auto null = isNullPlain(idx);

    if (hasNulls) {
      for (int i = 0; i < replicateNum; ++i) {
        valbuf.append(value);
        nanosecondsbuf.append(nanosecond);
        rt->nulls.append(null);
      }
    } else {
      for (int i = 0; i < replicateNum; ++i) {
        valbuf.append(values.get<int64_t>(idx));
        nanosecondsbuf.append(nanoseconds.get<int64_t>(idx));
      }
    }
    rt->values.swap(valbuf);
    rt->nanoseconds.swap(nanosecondsbuf);
    assert(rt->isValid());

    return std::move(retval);
  }

  void merge(size_t plainSize, const std::vector<Datum> &data,
             const std::vector<SelectList *> &sels) override {
    assert(data.size() == sels.size());

    values.resize(plainSize * sizeof(int64_t));
    nanoseconds.resize(plainSize * sizeof(int64_t));
    nulls.resize(plainSize);
    int64_t *__restrict__ secs = reinterpret_cast<int64_t *>(values.data());
    int64_t *__restrict__ nanos =
        reinterpret_cast<int64_t *>(nanoseconds.data());
    bool *__restrict__ nulls = this->nulls.data();

    for (auto inputIdx = 0; inputIdx < data.size(); inputIdx++) {
      auto sel = sels[inputIdx];
      if (sel == nullptr) continue;
      auto datum = DatumGetValue<Object *>(data[inputIdx]);

      if (auto vec = dynamic_cast<Vector *>(datum)) {
        auto secsIn = reinterpret_cast<const int64_t *>(vec->getValue());
        auto nanosIn = reinterpret_cast<const int64_t *>(vec->getNanoseconds());
        for (auto idx : *sel) secs[idx] = secsIn[idx];
        for (auto idx : *sel) nanos[idx] = nanosIn[idx];
        if (vec->hasNullValue()) {
          auto nullsIn = vec->getNulls();
          for (auto idx : *sel) nulls[idx] = nullsIn[idx];
        } else {
          for (auto idx : *sel) nulls[idx] = false;
        }
      } else {
        assert(dynamic_cast<Scalar *>(datum));
        auto scalar = reinterpret_cast<Scalar *>(datum);
        Timestamp *ts = DatumGetValue<Timestamp *>(scalar->value);
        auto sec = ts->second;
        auto nano = ts->nanosecond;
        auto null = scalar->isnull;
        if (null) {
          for (auto idx : *sel) {
            nulls[idx] = true;
          }
        } else {
          for (auto idx : *sel) {
            secs[idx] = sec;
            nanos[idx] = nano;
            nulls[idx] = false;
          }
        }
      }
    }  // End of loop each input data
  }

  // XXX: Not able keep multiple iterator at the same time
  const char *read(uint64_t index, uint64_t *len, bool *null) const override {
    uint64_t i = getPlainIndex(index);
    *null = isNullPlain(i);
    nextBuffer = 1 - nextBuffer;
    readBuffer[nextBuffer].second =
        *reinterpret_cast<int64_t *>(values.data() + (i * (sizeof(int64_t))));
    readBuffer[nextBuffer].nanosecond = *reinterpret_cast<int64_t *>(
        nanoseconds.data() + (i * (sizeof(int64_t))));
    *len = sizeof(readBuffer[nextBuffer]);
    return reinterpret_cast<const char *>(&readBuffer[nextBuffer]);
  }

  const Timestamp *read(uint64_t index, bool *null) const {
    uint64_t i = getPlainIndex(index);
    *null = isNullPlain(i);
    nextBuffer = 1 - nextBuffer;
    readBuffer[nextBuffer].second =
        *reinterpret_cast<int64_t *>(values.data() + (i * (sizeof(int64_t))));
    readBuffer[nextBuffer].nanosecond = *reinterpret_cast<int64_t *>(
        nanoseconds.data() + (i * (sizeof(int64_t))));
    return &readBuffer[nextBuffer];
  }

  std::string readPlain(uint64_t index, bool *null) const override {
    *null = isNullPlain(index);
    if (*null) return "NULL";
    return std::move(TimestampType::toString(values.get<int64_t>(index),
                                             nanoseconds.get<int64_t>(index)));
  }

  void readPlainScalar(uint64_t index, Scalar *scalar) const override {
    nextBuffer = 1 - nextBuffer;
    readBuffer[nextBuffer].second = *reinterpret_cast<int64_t *>(
        values.data() + (index * (sizeof(int64_t))));
    readBuffer[nextBuffer].nanosecond = *reinterpret_cast<int64_t *>(
        nanoseconds.data() + (index * (sizeof(int64_t))));
    scalar->value =
        CreateDatum(reinterpret_cast<const char *>(&readBuffer[nextBuffer]));
    scalar->isnull = isNullPlain(index);
    scalar->length = sizeof(readBuffer[0]);
  }

  void append(const char *v, uint64_t valueLen, bool null) override {
    assert(getOwnData() && v != nullptr);

    if (!null) {
      assert(valueLen == sizeof(int64_t) + sizeof(int64_t));
      const Timestamp *tsv = reinterpret_cast<const Timestamp *>(v);
      values.append<int64_t>(tsv->second);
      nanoseconds.append<int64_t>(tsv->nanosecond);
    } else {
      values.append<int64_t>(0);
      nanoseconds.append<int64_t>(0);
    }

    appendNull(null);
  }

  void append(Vector *src, uint64_t index) override {
    bool isnull = false;
    uint64_t len = 0;
    const char *ret = src->read(index, &len, &isnull);
    append(ret, len, isnull);
  }

  void append(const std::string &strValue, bool null) override {
    assert(getOwnData());

    Timestamp value;

    if (!null) {
      value = TimestampType::fromString(strValue);
    }

    append(reinterpret_cast<char *>(&value), sizeof(int64_t) + sizeof(int64_t),
           null);
  }

  void append(const Datum &datum, bool null) override {
    Timestamp *v = DatumGetValue<Timestamp *>(datum);
    if (v) {
      values.append(v->second);
      nanoseconds.append(v->nanosecond);
    } else {
      assert(null == true);
      values.append(int64_t(0));
      nanoseconds.append(int64_t(0));
    }

    appendNull(null);
  }

  void append(const Scalar *scalar) override {
    Timestamp *v = DatumGetValue<Timestamp *>(scalar->value);
    if (v) {
      values.append(v->second);
      nanoseconds.append(v->nanosecond);
    } else {
      assert(scalar->isnull == true);
      values.append(int64_t(0));
      nanoseconds.append(int64_t(0));
    }

    appendNull(scalar->isnull);
  }

  void append(const Datum &valDatum, const Datum &nanoDatum,
              bool null) override {
    if (!null) {
      values.append(*reinterpret_cast<const int64_t *>(&valDatum));
      nanoseconds.append(*reinterpret_cast<const int64_t *>(&nanoDatum));
    } else {
      values.append(int64_t(0));
      nanoseconds.append(int64_t(0));
    }
    appendNull(null);
  }

  void append(Vector *vect, SelectList *sel) override {
    Vector::append(vect, sel);

    const dbcommon::ByteBuffer *newValues = vect->getValueBuffer();
    const dbcommon::ByteBuffer *newNanoseconds = vect->getNanosecondsBuffer();

    if (!sel || sel->size() == vect->getNumOfRowsPlain()) {
      values.append(newValues->data(), newValues->size());
      nanoseconds.append(newNanoseconds->data(), newNanoseconds->size());
    } else {
      uint64_t newSize = sel->size();
      for (uint64_t i = 0; i < newSize; i++) {
        values.append(newValues->get<int64_t>((*sel)[i]));
        nanoseconds.append(newNanoseconds->get<int64_t>((*sel)[i]));
      }
    }
  }

  void reset(bool resetBelongingTupleBatch = false) override {
    Vector::reset(resetBelongingTupleBatch);
    values.resize(0);
    nanoseconds.resize(0);
  }

  void append(Vector *vect) override {
    assert(getOwnData());

    this->append(vect, vect->getSelected());
  }

  bool isValid() const override {
    Vector::isValid();

    if (hasNulls) {
      assert(nulls.size() == values.size() / sizeof(int64_t));
      assert(nulls.size() == nanoseconds.size() / sizeof(int64_t));
    }
    return true;
  }

  void hash(std::vector<uint64_t> &retval) const override {
    auto sec = reinterpret_cast<const int64_t *>(values.data());
    auto nanosec = reinterpret_cast<const int64_t *>(nanoseconds.data());
    auto size = getNumOfRows();
    auto sel = selected;
    retval.resize(size);
    if (allSelected()) {
      for (size_t i = 0; i < size; ++i) {
        // this comes from Hash128to64 in cityhash
        const uint64_t kMul = 0x9ddfea08eb382d69ULL;
        uint64_t a = (sec[i] ^ nanosec[i]) * kMul;
        a ^= (a >> 47);
        uint64_t b = (nanosec[i] ^ a) * kMul;
        b ^= (b >> 47);
        b *= kMul;
        retval[i] = b;
      }
    } else {
      for (size_t idx = 0; idx < size; ++idx) {
        // this comes from Hash128to64 in cityhash
        auto i = (*sel)[idx];
        const uint64_t kMul = 0x9ddfea08eb382d69ULL;
        uint64_t a = (sec[i] ^ nanosec[i]) * kMul;
        a ^= (a >> 47);
        uint64_t b = (nanosec[i] ^ a) * kMul;
        b ^= (b >> 47);
        b *= kMul;
        retval[idx] = b;
      }
    }

    if (hasNulls) {
      auto nulls = getNulls();
      if (allSelected()) {
        for (uint64_t i = 0; i < size; ++i)
          if (nulls[i]) retval[i] = 0;
      } else {
        for (uint64_t i = 0; i < size; ++i)
          if (nulls[(*sel)[i]]) retval[i] = 0;
      }
    }
  }

  void cdbHash(std::vector<uint64_t> &retval) const override {
    auto base = reinterpret_cast<const int64_t *>(getValue());
    size_t len = sizeof(int64_t);

    if (selected) {
      if (hasNulls) {
        for (uint64_t i = 0; i < retval.size(); ++i) {
          uint64_t idx = (*selected)[i];
          retval[i] = nulls.get(idx) ? cdbhashnull(retval[i])
                                     : cdbhash(&base[idx], len, retval[i]);
        }
      } else {
        for (uint64_t i = 0; i < retval.size(); ++i) {
          uint64_t idx = (*selected)[i];
          retval[i] = cdbhash(&base[idx], len, retval[i]);
        }
      }
    } else {
      if (hasNulls) {
        for (uint64_t i = 0; i < retval.size(); ++i) {
          retval[i] = nulls.get(i) ? cdbhashnull(retval[i])
                                   : cdbhash(&base[i], len, retval[i]);
        }
      } else {
        for (uint64_t i = 0; i < retval.size(); ++i) {
          retval[i] = cdbhash(&base[i], len, retval[i]);
        }
      }
    }
  }

  dbcommon::ByteBuffer *getValueBuffer() override { return &values; }

  dbcommon::ByteBuffer *getNanosecondsBuffer() override { return &nanoseconds; }

  void swap(Vector &vect) override {
    Vector::swap(vect);
    auto &v = dynamic_cast<TimestampVector &>(vect);
    this->values.swap(v.values);
    this->nanoseconds.swap(v.nanoseconds);
  }

  uint64_t checkGroupByKey(uint64_t index, const char *dataPtr) const override {
    assert(index < getNumOfRows() && "invalid input");
    assert(getNumOfRows() > 0);
    uint64_t i = getPlainIndex(index);
    const TimestampDataInfo *info =
        reinterpret_cast<const TimestampDataInfo *>(dataPtr);
    if (isNullPlain(i) != info->isNull) return 0;
    if (!info->isNull) {
      if (*reinterpret_cast<int64_t *>(values.data() + i * (sizeof(int64_t))) !=
          info->data.second)
        return 0;
      if (*reinterpret_cast<int64_t *>(nanoseconds.data() +
                                       i * (sizeof(int64_t))) !=
          info->data.nanosecond)
        return 0;
    }
    return sizeof(TimestampDataInfo);
  }

  void checkGroupByKeys(std::vector<const void *> &dataPtrSrc) const override {
    const int64_t *data =
        reinterpret_cast<const int64_t *>(TimestampVector::getValue());
    const int64_t *nanos =
        reinterpret_cast<const int64_t *>(TimestampVector::getNanoseconds());

    const uint64_t size = TimestampVector::getNumOfRows();
    const char **dataPtr = reinterpret_cast<const char **>(&dataPtrSrc[0]);
    if (selected) {
      auto &sel = *selected;

#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < size; ++i) {
        if (dataPtr[i] != nullptr) {
          uint64_t index = sel[i];
          char null = hasNulls ? nulls.getChar(index) : 0;
          const int64_t d = data[index];
          const int64_t n = nanos[index];

          const TimestampDataInfo *info =
              reinterpret_cast<const TimestampDataInfo *>(dataPtr[i]);
          if (null == info->isNull &&
              (null || (d == info->data.second && n == info->data.nanosecond)))
            dataPtr[i] = alignedAddress(dataPtr[i] + sizeof(TimestampDataInfo));
          else
            dataPtr[i] = nullptr;
        }
      }
    } else {
      if (hasNulls) {
        for (uint64_t i = 0; i < size; ++i) {
          if (dataPtr[i] != nullptr) {
            char null = nulls.getChar(i);
            const int64_t d = data[i];
            const int64_t n = nanos[i];

            const TimestampDataInfo *info =
                reinterpret_cast<const TimestampDataInfo *>(dataPtr[i]);
            if (null == info->isNull && (null || (d == info->data.second &&
                                                  n == info->data.nanosecond)))
              dataPtr[i] =
                  alignedAddress(dataPtr[i] + sizeof(TimestampDataInfo));
            else
              dataPtr[i] = nullptr;
          }
        }
      } else {
        for (uint64_t i = 0; i < size; ++i) {
          if (dataPtr[i] != nullptr) {
            char null = false;
            const int64_t d = data[i];
            const int64_t n = nanos[i];

            const TimestampDataInfo *info =
                reinterpret_cast<const TimestampDataInfo *>(dataPtr[i]);
            if (null == info->isNull && (null || (d == info->data.second &&
                                                  n == info->data.nanosecond)))
              dataPtr[i] =
                  alignedAddress(dataPtr[i] + sizeof(TimestampDataInfo));
            else
              dataPtr[i] = nullptr;
          }
        }
      }
    }
  }

  uint64_t getSerializeSizeInternal() const override {
    size_t sSize = Vector::getSerializeSizeInternal();
    sSize += sizeof(uint64_t) + values.size();
    sSize += sizeof(uint64_t) + nanoseconds.size();
    return sSize;
  }

  void serializeInternal(NodeSerializer *serializer) override {
    Vector::serializeInternal(serializer);

    if (allSelected()) {
      uint64_t vsz = getNumOfRowsPlain() * 16;
      serializer->write<uint64_t>(vsz);
      if (vsz > 0) {
        serializer->writeBytes(getValue(), vsz / 2);
        serializer->writeBytes(getNanoseconds(), vsz / 2);
      }
    } else {
      uint64_t vsz = selected->size() * 16;
      serializer->write<uint64_t>(vsz);
      if (vsz > 0) {
        auto seconds = reinterpret_cast<const uint64_t *>(getValue());
        for (auto i : *selected) {
          serializer->write(seconds[i]);
        }
        auto nanoseconds = reinterpret_cast<const uint64_t *>(getNanoseconds());
        for (auto i : *selected) {
          serializer->write(nanoseconds[i]);
        }
      }
    }
  }

  void deserializeInternal(NodeDeserializer *deserializer) override {
    Vector::deserializeInternal(deserializer);

    uint64_t vsz = deserializer->read<uint64_t>();
    if (vsz > 0) {
      const char *dstr = deserializer->readBytes(vsz);
      setValue(dstr, vsz / 2);
      setNanoseconds(dstr + vsz / 2, vsz / 2);
    }

    assert(isValid());
  }

  void selectDistinct(SelectList *selected, Scalar *scalar) override {
    SelectList *sel = getSelected();
    uint64_t sz = getNumOfRows();
    const int64_t *vals = reinterpret_cast<const int64_t *>(getValue());
    const int64_t *nanos = reinterpret_cast<const int64_t *>(getNanoseconds());

    auto operation = [&](uint64_t plainIdx) {  // NOLINT
      if (!scalar->isnull) {
        Timestamp *value = DatumGetValue<Timestamp *>(scalar->value);
        return (vals[plainIdx] != value->second ||
                nanos[plainIdx] != value->nanosecond);
      }
      return false;
    };
    const bool *nulls = getNulls();
    dbcommon::transformDistinct(sz, sel, nulls, selected, scalar->isnull,
                                operation);
  }

  void selectDistinct(SelectList *selected, Vector *vect) override {
    SelectList *sel1 = getSelected();
    SelectList *sel2 = vect->getSelected();
    uint64_t sz = getNumOfRows();
    assert(sz == vect->getNumOfRows() && "invalid input");

    const int64_t *vals1 = reinterpret_cast<const int64_t *>(values.data());
    const int64_t *nanos1 = reinterpret_cast<const int64_t *>(getNanoseconds());
    const int64_t *vals2 = reinterpret_cast<const int64_t *>(vect->getValue());
    const int64_t *nanos2 =
        reinterpret_cast<const int64_t *>(vect->getNanoseconds());

    auto operation = [&](uint64_t plainIdx) {  // NOLINT
      return (vals1[plainIdx] != vals2[plainIdx] ||
              nanos1[plainIdx] != nanos2[plainIdx]);
    };
    const bool *nulls1 = getNulls();
    const bool *nulls2 = vect->getNulls();
    dbcommon::transformDistinct(sz, sel1, nulls1, nulls2, selected, operation);
  }

 protected:
  void resizeInternal(size_t plainSize) override {
    values.resize(plainSize * sizeof(int64_t));
    nanoseconds.resize(plainSize * sizeof(int64_t));
  }

  dbcommon::ByteBuffer nanoseconds;
  mutable Timestamp readBuffer[2];
  mutable uint8_t nextBuffer = 0;
  struct TimestampDataInfo {
    char isNull;
    char padding[7];
    Timestamp data;
  };
};  // namespace dbcommon

class TimestamptzVector : public TimestampVector {
 public:
  explicit TimestamptzVector(bool ownData) : TimestampVector(ownData) {
    setTypeKind(TIMESTAMPTZID);
  }

  ~TimestamptzVector() override {}

  void setTimezone(int64_t timezone) { timezoneOffset_ = timezone; }

  int64_t getTimezone() { return timezoneOffset_; }

  std::string readPlain(uint64_t index, bool *null) const override {
    *null = isNullPlain(index);
    if (*null) return "NULL";
    return std::move(TimestamptzType::toString(
        values.get<int64_t>(index), nanoseconds.get<int64_t>(index)));
  }

  void append(const std::string &strValue, bool null) override {
    assert(getOwnData());

    Timestamp value;

    if (!null) {
      value = TimestamptzType::fromString(strValue);
    }

    TimestampVector::append(reinterpret_cast<char *>(&value),
                            sizeof(int64_t) + sizeof(int64_t), null);
  }

 protected:
  int64_t timezoneOffset_;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_TIMESTAMP_VECTOR_H_
