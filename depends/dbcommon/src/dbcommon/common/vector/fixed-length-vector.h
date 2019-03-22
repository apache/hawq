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

#ifndef DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_FIXED_LENGTH_VECTOR_H_
#define DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_FIXED_LENGTH_VECTOR_H_

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "dbcommon/common/vector-transformer.h"
#include "dbcommon/common/vector.h"
#include "dbcommon/type/magma-tid.h"

namespace dbcommon {

template <typename ValueType, typename ADT>
class FixedSizeTypeVector : public Vector {
 public:
  typedef ValueType value_type;
  typedef ADT abstract_type;

  explicit FixedSizeTypeVector(bool ownData) : Vector(ownData) {
    if (ownData)
      values.reserve(sizeof(ValueType) * DEFAULT_NUMBER_TUPLES_PER_BATCH);
  }

  ~FixedSizeTypeVector() override {}

  uint64_t getNumOfRowsPlain() const override {
    return values.size() / sizeof(value_type);
  }

  void setValue(const char *value, uint64_t size) override {
    assert(!getOwnData() && value != nullptr && size >= 0);
    values.setData(value, size);
  }

  void materialize() override {
    values.materialize<value_type>(selected);
    if (hasNulls) nulls.materialize(selected);
    selected = nullptr;
  }

  std::unique_ptr<Vector> cloneSelected(const SelectList *sel) const override {
    assert(isValid());

    std::unique_ptr<Vector> retval = Vector::BuildVector(getTypeKind(), false);
    FixedSizeTypeVector<value_type, abstract_type> *ret =
        static_cast<FixedSizeTypeVector<value_type, abstract_type> *>(
            retval.get());

    ret->setTypeKind(typeKind);
    ret->setHasNull(hasNulls);

    if (hasNulls) ret->setNulls(getNulls(), getNumOfRowsPlain());
    ret->setValue(values.data(), values.size());

    ret->setSelected(sel, true);
    ret->materialize();

    assert(ret->isValid());
    return std::move(retval);
  }

  std::unique_ptr<Vector> extract(const SelectList *sel, uint64_t start,
                                  uint64_t end) const override {
    assert(isValid() && start < end);
    uint64_t total = getNumOfRowsPlain();
    assert(end <= total);
    uint64_t extSz = end - start;

    std::unique_ptr<Vector> retval = Vector::BuildVector(getTypeKind(), false);
    FixedSizeTypeVector<value_type, abstract_type> *ret =
        static_cast<FixedSizeTypeVector<value_type, abstract_type> *>(
            retval.get());

    ret->setTypeKind(typeKind);
    ret->setHasNull(hasNulls);

    if (hasNulls) ret->setNulls(getNulls() + start, extSz);

    uint64_t typLen = sizeof(value_type);
    if (values.data()) {
      ret->setValue(values.data() + start * typLen, extSz * typLen);
    }

    ret->setSelected(sel, true);

    assert(ret->isValid());
    return std::move(retval);
  }

  std::unique_ptr<Vector> replicateItemToVec(uint64_t index,
                                             int replicateNum) const override {
    assert(isValid());
    auto retval = Vector::BuildVector(getTypeKind(), true);
    auto rt = static_cast<FixedSizeTypeVector<value_type, abstract_type> *>(
        retval.get());

    rt->setTypeKind(typeKind);
    rt->setHasNull(hasNulls);

    dbcommon::ByteBuffer buf(true);
    uint64_t idx = getPlainIndex(index);
    auto value = values.get<value_type>(idx);
    auto null = isNullPlain(idx);

    if (hasNulls) {
      for (int i = 0; i < replicateNum; ++i) {
        buf.append(value);
        rt->nulls.append(null);
      }
    } else {
      for (int i = 0; i < replicateNum; ++i) {
        buf.append(values.get<value_type>(idx));
      }
    }
    rt->values.swap(buf);
    assert(rt->isValid());

    return std::move(retval);
  }

  void merge(size_t plainSize, const std::vector<Datum> &data,
             const std::vector<SelectList *> &sels) override {
    assert(data.size() == sels.size());

    values.resize(plainSize * sizeof(value_type));
    nulls.resize(plainSize);
    value_type *__restrict__ vals =
        reinterpret_cast<value_type *>(values.data());
    bool *__restrict__ nulls = this->nulls.data();

    for (auto inputIdx = 0; inputIdx < data.size(); inputIdx++) {
      auto sel = sels[inputIdx];
      if (sel == nullptr) continue;
      auto datum = DatumGetValue<Object *>(data[inputIdx]);

      if (auto vec = dynamic_cast<Vector *>(datum)) {
        auto valsIn = reinterpret_cast<const value_type *>(vec->getValue());
        for (auto idx : *sel) vals[idx] = valsIn[idx];
        if (vec->hasNullValue()) {
          auto nullsIn = vec->getNulls();
          for (auto idx : *sel) nulls[idx] = nullsIn[idx];
        } else {
          for (auto idx : *sel) nulls[idx] = false;
        }
      } else if (auto boolSel = dynamic_cast<SelectList *>(datum)) {
        assert(typeKind == TypeKind::BOOLEANID);
        for (auto idx : *sel) {
          vals[idx] = false;
          nulls[idx] = false;
        }
        for (auto idx : *boolSel) {
          vals[idx] = true;
        }
      } else {
        auto scalar = reinterpret_cast<Scalar *>(datum);
        auto val = DatumGetValue<value_type>(scalar->value);
        auto null = scalar->isnull;
        if (null) {
          for (auto idx : *sel) {
            nulls[idx] = true;
          }
        } else {
          for (auto idx : *sel) {
            vals[idx] = val;
            nulls[idx] = false;
          }
        }
      }
    }  // End of loop each input data
  }

  const char *read(uint64_t index, uint64_t *len, bool *null) const override {
    assert(index < getNumOfRows() && "invalid input");
    assert(nullptr != null && "invalid input");
    assert(nullptr != len && "invalid input");

    uint64_t i = getPlainIndex(index);

    *null = isNullPlain(i);
    *len = sizeof(value_type);

    return values.data() + (i * (*len));
  }

  std::string readPlain(uint64_t index, bool *null) const override {
    *null = isNullPlain(index);
    if (*null) return "NULL";
    return std::move(abstract_type::toString(values.get<value_type>(index)));
  }

  void readPlainScalar(uint64_t index, Scalar *scalar) const override {
    scalar->value = CreateDatum(values.get<value_type>(index));
    scalar->isnull = isNullPlain(index);
  }

  void append(const char *v, uint64_t valueLen, bool null) override {
    assert(getOwnData() && v != nullptr);
    assert(valueLen == sizeof(value_type));

    value_type value = *(reinterpret_cast<const value_type *>(v));
    values.append<value_type>(value);

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

    value_type value(0);

    if (!null) {
      value = abstract_type::fromString(strValue);
    }

    append(reinterpret_cast<const char *>(&value), sizeof(value_type), null);
  }

  void append(const Datum &datum, bool null) override {
    /*
    std::string buffer;
    if (!null) {
      buffer = DatumToBinary(datum, typeKind);
    } else {
      buffer.resize(abstract_type::kWidth);
    }

    append(buffer.data(), buffer.size(), null);
    */

    values.append(*reinterpret_cast<const value_type *>(&datum));
    appendNull(null);
  }

  void append(const Scalar *scalar) override {
    values.append(DatumGetValue<const value_type>(scalar->value));
    nulls.append(scalar->isnull);
  }

  void append(Vector *vect, SelectList *sel) override {
    Vector::append(vect, sel);

    const dbcommon::ByteBuffer *newValues = vect->getValueBuffer();

    if (!sel || sel->size() == vect->getNumOfRowsPlain()) {
      values.append(newValues->data(), newValues->size());
    } else {
      uint64_t newSize = sel->size();
      for (uint64_t i = 0; i < newSize; i++) {
        values.append(newValues->get<value_type>((*sel)[i]));
      }
    }
  }

  void reset(bool resetBelongingTupleBatch = false) override {
    Vector::reset(resetBelongingTupleBatch);
    values.resize(0);
  }

  void append(Vector *vect) override {
    assert(getOwnData());

    this->append(vect, vect->getSelected());
  }

  bool isValid() const override {
    Vector::isValid();

    if (hasNulls) assert(nulls.size() == values.size() / sizeof(value_type));
    return true;
  }

  void hash(std::vector<uint64_t> &retval) const override {
    auto base = reinterpret_cast<const value_type *>(getValue());

    if (selected) {
      fast_hash(base, *selected, retval);
    } else {
      fast_hash(base, this->getNumOfRows(), retval);
    }

    if (hasNulls) {
      for (uint64_t i = 0; i < retval.size(); ++i) {
        if (nulls.get(getPlainIndex(i))) {
          retval[i] = 0;
        }
      }
    }
  }

  void cdbHash(std::vector<uint64_t> &retval) const override {
    if (std::is_same<value_type, int32_t>::value ||
        std::is_same<value_type, int16_t>::value ||
        std::is_same<value_type, int8_t>::value) {
      auto base = reinterpret_cast<const value_type *>(getValue());
      size_t len = sizeof(int64_t);

      if (selected) {
        if (hasNulls) {
          for (uint64_t i = 0; i < retval.size(); ++i) {
            uint64_t idx = (*selected)[i];
            int64_t value = static_cast<int64_t>(base[idx]);
            retval[i] = nulls.get(idx) ? cdbhashnull(retval[i])
                                       : cdbhash(&value, len, retval[i]);
          }
        } else {
          for (uint64_t i = 0; i < retval.size(); ++i) {
            uint64_t idx = (*selected)[i];
            int64_t value = static_cast<int64_t>(base[idx]);
            retval[i] = cdbhash(&value, len, retval[i]);
          }
        }
      } else {
        if (hasNulls) {
          for (uint64_t i = 0; i < retval.size(); ++i) {
            int64_t value = static_cast<int64_t>(base[i]);
            retval[i] = nulls.get(i) ? cdbhashnull(retval[i])
                                     : cdbhash(&value, len, retval[i]);
          }
        } else {
          for (uint64_t i = 0; i < retval.size(); ++i) {
            int64_t value = static_cast<int64_t>(base[i]);
            retval[i] = cdbhash(&value, len, retval[i]);
          }
        }
      }
      return;
    }

    auto base = reinterpret_cast<const value_type *>(getValue());
    size_t len = sizeof(value_type);

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

  void swap(Vector &vect) override {
    Vector::swap(vect);
    auto &v =
        dynamic_cast<FixedSizeTypeVector<value_type, abstract_type> &>(vect);
    this->values.swap(v.values);
  }

  bool checkZeroValue() const override {
    // zero value is not a common case, so we go through all the values to
    // reduce CPU branch predict miss
    bool hasZero = false;
    FixedSizeTypeVectorRawData<value_type> vec(
        const_cast<Vector *>(reinterpret_cast<const Vector *>(this)));
    auto checkZero = [&](SelectList::value_type plainIdx) {
      hasZero |= (vec.values[plainIdx] == 0);
    };
    transformVector(vec.plainSize, vec.sel, vec.nulls, checkZero);
    return hasZero;
  }

  bool checkLessZero() const override {
    bool lessZero = false;
    FixedSizeTypeVectorRawData<value_type> vec(
        const_cast<Vector *>(reinterpret_cast<const Vector *>(this)));
    auto checkZero = [&](SelectList::value_type plainIdx) {
      lessZero |= (vec.values[plainIdx] < 0);
    };
    transformVector(vec.plainSize, vec.sel, vec.nulls, checkZero);
    return lessZero;
  }

  bool checkLogarithmAgrumentError() const override {
    bool lessZero = false;
    bool isZero = false;
    FixedSizeTypeVectorRawData<value_type> vec(
        const_cast<Vector *>(reinterpret_cast<const Vector *>(this)));
    auto checkIfValid = [&](SelectList::value_type plainIdx) {
      lessZero |= (vec.values[plainIdx] < 0);
      isZero |= (vec.values[plainIdx] == 0);
    };
    transformVector(vec.plainSize, vec.sel, vec.nulls, checkIfValid);
    if (lessZero)
      LOG_ERROR(ERRCODE_INVALID_ARGUMENT_FOR_LOG,
                "cannot take logarithm of a negative number");
    if (isZero)
      LOG_ERROR(ERRCODE_INVALID_ARGUMENT_FOR_LOG,
                "cannot take logarithm of zero");
    return false;
  }

  bool checkExponentialArgumentError() const override {
    bool isInvalid = false;
    FixedSizeTypeVectorRawData<value_type> vec(
        const_cast<Vector *>(reinterpret_cast<const Vector *>(this)));
    auto checkIfValid = [&](SelectList::value_type plainIdx) {
      isInvalid |=
          (vec.values[plainIdx] < -708.4 || vec.values[plainIdx] > 709.8);
    };
    transformVector(vec.plainSize, vec.sel, vec.nulls, checkIfValid);
    if (isInvalid)
      LOG_ERROR(ERRCODE_INTERVAL_FIELD_OVERFLOW,
                "value out of range: overflow");
    return false;
  }

  uint64_t checkGroupByKey(uint64_t index, const char *dataPtr) const override {
    assert(index < getNumOfRows() && "invalid input");
    assert(getNumOfRows() > 0);
    uint64_t i = getPlainIndex(index);
    const FixedLenDataInfo *info =
        reinterpret_cast<const FixedLenDataInfo *>(dataPtr);
    if (isNullPlain(i) != info->isNull) return 0;
    if (!info->isNull) {
      if (*reinterpret_cast<ValueType *>(values.data() +
                                         i * (sizeof(ValueType))) != info->data)
        return 0;
    }
    return sizeof(ValueType) + sizeof(ValueType);
  }

  void checkGroupByKeys(std::vector<const void *> &dataPtrSrc) const override {
    const value_type *data =
        reinterpret_cast<const value_type *>(FixedSizeTypeVector::getValue());

    const uint64_t size = FixedSizeTypeVector::getNumOfRows();
    const char **dataPtr = reinterpret_cast<const char **>(&dataPtrSrc[0]);
    if (selected) {
      auto &sel = *selected;

#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < size; ++i) {
        if (dataPtr[i] != nullptr) {
          uint64_t index = sel[i];
          char null = hasNulls ? nulls.getChar(index) : 0;
          const value_type d = data[index];

          const FixedLenDataInfo *info =
              reinterpret_cast<const FixedLenDataInfo *>(dataPtr[i]);
          if (null == info->isNull && (null || d == info->data))
            dataPtr[i] = alignedAddress(dataPtr[i] + sizeof(FixedLenDataInfo));
          else
            dataPtr[i] = nullptr;
        }
      }
    } else {
      if (hasNulls) {
        for (uint64_t i = 0; i < size; ++i) {
          if (dataPtr[i] != nullptr) {
            char null = nulls.getChar(i);
            const value_type d = data[i];

            const FixedLenDataInfo *info =
                reinterpret_cast<const FixedLenDataInfo *>(dataPtr[i]);
            if (null == info->isNull && (null || d == info->data))
              dataPtr[i] =
                  alignedAddress(dataPtr[i] + sizeof(FixedLenDataInfo));
            else
              dataPtr[i] = nullptr;
          }
        }
      } else {
        for (uint64_t i = 0; i < size; ++i) {
          if (dataPtr[i] != nullptr) {
            char null = false;
            const value_type d = data[i];

            const FixedLenDataInfo *info =
                reinterpret_cast<const FixedLenDataInfo *>(dataPtr[i]);
            if (null == info->isNull && (null || d == info->data))
              dataPtr[i] =
                  alignedAddress(dataPtr[i] + sizeof(FixedLenDataInfo));
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
    return sSize;
  }

  void serializeInternal(NodeSerializer *serializer) override {
    Vector::serializeInternal(serializer);

    uint64_t vsz = getNumOfRows() * sizeof(value_type);
    serializer->write<uint64_t>(vsz);
    if (vsz > 0) {
      if (allSelected()) {
        serializer->writeBytes(getValue(), vsz);
      } else {
        auto values = reinterpret_cast<const value_type *>(getValue());
        for (auto idx : *selected) serializer->write(values[idx]);
      }
    }
  }

  void deserializeInternal(NodeDeserializer *deserializer) override {
    Vector::deserializeInternal(deserializer);

    uint64_t vsz = deserializer->read<uint64_t>();
    if (vsz > 0) {
      const char *dstr = deserializer->readBytes(vsz);
      setValue(dstr, vsz);
    }

    assert(isValid());
  }

  void selectDistinct(SelectList *selected, Scalar *scalar) override {
    SelectList *sel = getSelected();
    uint64_t sz = getNumOfRows();
    const value_type *vals = reinterpret_cast<const value_type *>(getValue());

    auto operation = [&](uint64_t plainIdx) {  // NOLINT
      if (!scalar->isnull) {
        value_type value = DatumGetValue<value_type>(scalar->value);
        return (vals[plainIdx] != value);
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

    const value_type *vals1 = reinterpret_cast<const value_type *>(getValue());
    const value_type *vals2 =
        reinterpret_cast<const value_type *>(vect->getValue());

    auto operation = [&](uint64_t plainIdx) {  // NOLINT
      return (vals1[plainIdx] != vals2[plainIdx]);
    };
    const bool *nulls1 = getNulls();
    const bool *nulls2 = vect->getNulls();
    dbcommon::transformDistinct(sz, sel1, nulls1, nulls2, selected, operation);
  }

 protected:
  void resizeInternal(size_t plainSize) override {
    values.resize(plainSize * sizeof(value_type));
  }

  struct FixedLenDataInfo {
    char isNull;
    char padding[sizeof(value_type) - 1];
    value_type data;
  };
};

class TinyIntVector : public FixedSizeTypeVector<int8_t, TinyIntType> {
 public:
  explicit TinyIntVector(bool ownData)
      : FixedSizeTypeVector<int8_t, TinyIntType>(ownData) {
    setTypeKind(TINYINTID);
  }
  virtual ~TinyIntVector() {}
};

class SmallIntVector : public FixedSizeTypeVector<int16_t, SmallIntType> {
 public:
  explicit SmallIntVector(bool ownData)
      : FixedSizeTypeVector<int16_t, SmallIntType>(ownData) {
    setTypeKind(SMALLINTID);
  }
  virtual ~SmallIntVector() {}
};

class IntVector : public FixedSizeTypeVector<int32_t, IntType> {
 public:
  explicit IntVector(bool ownData)
      : FixedSizeTypeVector<int32_t, IntType>(ownData) {
    setTypeKind(INTID);
  }
  virtual ~IntVector() {}
};

class BigIntVector : public FixedSizeTypeVector<int64_t, BigIntType> {
 public:
  explicit BigIntVector(bool ownData)
      : FixedSizeTypeVector<int64_t, BigIntType>(ownData) {
    setTypeKind(BIGINTID);
  }
  virtual ~BigIntVector() {}
};

class FloatVector : public FixedSizeTypeVector<float, FloatType> {
 public:
  explicit FloatVector(bool ownData)
      : FixedSizeTypeVector<float, FloatType>(ownData) {
    setTypeKind(FLOATID);
  }
  virtual ~FloatVector() {}
};

class DoubleVector : public FixedSizeTypeVector<double, DoubleType> {
 public:
  explicit DoubleVector(bool ownData)
      : FixedSizeTypeVector<double, DoubleType>(ownData) {
    setTypeKind(DOUBLEID);
  }
  virtual ~DoubleVector() {}
};

class BooleanVector : public FixedSizeTypeVector<bool, BooleanType> {
 public:
  explicit BooleanVector(bool ownData)
      : FixedSizeTypeVector<bool, BooleanType>(ownData) {
    setTypeKind(BOOLEANID);
  }
  virtual ~BooleanVector() {}
};

class DateVector : public FixedSizeTypeVector<int32_t, DateType> {
 public:
  explicit DateVector(bool ownData)
      : FixedSizeTypeVector<int32_t, DateType>(ownData) {
    setTypeKind(DATEID);
  }
  virtual ~DateVector() {}
};

class TimeVector : public FixedSizeTypeVector<int64_t, TimeType> {
 public:
  explicit TimeVector(bool ownData)
      : FixedSizeTypeVector<int64_t, TimeType>(ownData) {
    setTypeKind(TIMEID);
  }
  virtual ~TimeVector() {}
};

class MagmaTidVector : public FixedSizeTypeVector<MagmaTid, MagmaTidType> {
 public:
  explicit MagmaTidVector(bool ownData)
      : FixedSizeTypeVector<MagmaTid, MagmaTidType>(ownData) {
    setTypeKind(MAGMATID);
  }

  ~MagmaTidVector() override {}
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_FIXED_LENGTH_VECTOR_H_
