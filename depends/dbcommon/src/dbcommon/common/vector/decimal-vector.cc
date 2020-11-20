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

#include "dbcommon/common/vector/decimal-vector.h"

#include <algorithm>

#include "dbcommon/common/vector-transformer.h"
#include "dbcommon/utils/string-util.h"
namespace dbcommon {

const char *DecimalVector::read(uint64_t index, uint64_t *len,
                                bool *null) const {
  assert(index < getNumOfRows() && "invalid input");
  assert(nullptr != null && "invalid input");
  assert(nullptr != len && "invalid input");

  uint64_t i = getPlainIndex(index);
  *null = isNullPlain(i);

  assert(nextBuffer == 0 || nextBuffer == 1);
  nextBuffer = 1 - nextBuffer;
  readBuffer[nextBuffer].lowbits =
      *reinterpret_cast<uint64_t *>(values.data() + (i * (sizeof(uint64_t))));
  readBuffer[nextBuffer].highbits = *reinterpret_cast<int64_t *>(
      auxiliaryValues.data() + (i * (sizeof(int64_t))));
  readBuffer[nextBuffer].scale = *reinterpret_cast<int64_t *>(
      scaleValues.data() + (i * (sizeof(int64_t))));
  *len = sizeof(int64_t) + sizeof(uint64_t) + sizeof(int64_t);
  return reinterpret_cast<const char *>(&readBuffer[nextBuffer]);
}

std::string DecimalVector::readPlain(uint64_t index, bool *null) const {
  *null = isNullPlain(index);
  if (*null) return "";
  return std::move(DecimalType::toString(auxiliaryValues.get<int64_t>(index),
                                         values.get<uint64_t>(index),
                                         scaleValues.get<int64_t>(index)));
}

void DecimalVector::readPlainScalar(uint64_t index, Scalar *scalar) const {
  assert(nextBuffer == 0 || nextBuffer == 1);
  nextBuffer = 1 - nextBuffer;
  readBuffer[nextBuffer].lowbits = *reinterpret_cast<uint64_t *>(
      values.data() + (index * (sizeof(uint64_t))));
  readBuffer[nextBuffer].highbits = *reinterpret_cast<int64_t *>(
      auxiliaryValues.data() + (index * (sizeof(int64_t))));
  readBuffer[nextBuffer].scale = *reinterpret_cast<int64_t *>(
      scaleValues.data() + (index * (sizeof(int64_t))));
  scalar->value =
      CreateDatum(reinterpret_cast<const char *>(&readBuffer[nextBuffer]));
  scalar->length = sizeof(DecimalVar);
  scalar->isnull = isNullPlain(index);
}

void DecimalVector::append(const std::string &strValue, bool null) {
  assert(getOwnData());
  DecimalVar value = {0, 0, 0};
  if (!null) {
    value = DecimalType::fromString(strValue);
  }

  append(reinterpret_cast<char *>(&value), sizeof(DecimalVar), null);
}

void DecimalVector::append(const char *v, uint64_t valueLen, bool null) {
  assert(getOwnData() && v != nullptr);
  assert(valueLen == sizeof(int64_t) + sizeof(uint64_t) + sizeof(int64_t));

  int64_t auxVal = *(reinterpret_cast<const int64_t *>(v));
  uint64_t val = *(reinterpret_cast<const uint64_t *>(v + 8));
  int64_t scale = *(reinterpret_cast<const int64_t *>(v + 16));
  values.append<uint64_t>(val);
  auxiliaryValues.append<int64_t>(auxVal);
  scaleValues.append<int64_t>(scale);
  appendNull(null);
}

void DecimalVector::append(const Datum &datum, bool null) {
  if (!null) {
    dbcommon::DecimalVar *val = DatumGetValue<DecimalVar *>(datum);
    values.append(val->lowbits);
    auxiliaryValues.append(val->highbits);
    scaleValues.append(val->scale);
  } else {
    values.append(uint64_t(0));
    auxiliaryValues.append(int64_t(0));
    scaleValues.append(int64_t(0));
  }
  appendNull(null);
}

void DecimalVector::append(const Scalar *scalar) {
  if (!scalar->isnull) {
    dbcommon::DecimalVar *val = DatumGetValue<DecimalVar *>(scalar->value);
    values.append(val->lowbits);
    auxiliaryValues.append(val->highbits);
    scaleValues.append(val->scale);
  } else {
    values.append(uint64_t(0));
    auxiliaryValues.append(int64_t(0));
    scaleValues.append(int64_t(0));
  }
  appendNull(scalar->isnull);
}

void DecimalVector::append(Vector *src, uint64_t index) {
  bool isnull = false;
  uint64_t len = 0;
  const char *ret = src->read(index, &len, &isnull);
  append(ret, len, isnull);
}

void DecimalVector::append(Vector *vect) {
  assert(getOwnData());
  this->append(vect, vect->getSelected());
}

void DecimalVector::append(Vector *vect, SelectList *sel) {
  Vector::append(vect, sel);

  const dbcommon::ByteBuffer *newValues = vect->getValueBuffer();
  const dbcommon::ByteBuffer *newAuxiliaryValues =
      vect->getAuxiliaryValueBuffer();
  const dbcommon::ByteBuffer *newScaleValues = vect->getScaleValueBuffer();
  if (!sel || sel->size() == vect->getNumOfRowsPlain()) {
    values.append(newValues->data(), newValues->size());
    auxiliaryValues.append(newAuxiliaryValues->data(),
                           newAuxiliaryValues->size());
    scaleValues.append(newScaleValues->data(), newScaleValues->size());
  } else {
    uint64_t newSize = sel->size();
    for (uint64_t i = 0; i < newSize; i++) {
      values.append(newValues->get<uint64_t>((*sel)[i]));
      auxiliaryValues.append(newAuxiliaryValues->get<int64_t>((*sel)[i]));
      scaleValues.append(newScaleValues->get<int64_t>((*sel)[i]));
    }
  }
}

void DecimalVector::materialize() {
  values.materialize<uint64_t>(selected);
  auxiliaryValues.materialize<int64_t>(selected);
  scaleValues.materialize<int64_t>(selected);
  if (hasNulls) nulls.materialize(selected);
  selected = nullptr;
}

std::unique_ptr<Vector> DecimalVector::cloneSelected(
    const SelectList *sel) const {
  assert(isValid());

  std::unique_ptr<Vector> retval = Vector::BuildVector(getTypeKind(), false);
  DecimalVector *ret = static_cast<DecimalVector *>(retval.get());

  ret->setTypeKind(typeKind);
  ret->setHasNull(hasNulls);
  assert(values.size() == auxiliaryValues.size());
  if (hasNulls) ret->setNulls(getNulls(), getNumOfRowsPlain());
  ret->setValue(values.data(), values.size());
  ret->setAuxiliaryValue(auxiliaryValues.data(), auxiliaryValues.size());
  ret->setScaleValue(scaleValues.data(), scaleValues.size());

  ret->setSelected(sel, true);
  ret->materialize();

  assert(ret->isValid());
  return std::move(retval);
}

std::unique_ptr<Vector> DecimalVector::extract(const SelectList *sel,
                                               uint64_t start,
                                               uint64_t end) const {
  assert(isValid() && start < end);
  uint64_t total = getNumOfRowsPlain();
  assert(end <= total);
  uint64_t extSz = end - start;

  std::unique_ptr<Vector> retval = Vector::BuildVector(getTypeKind(), false);
  DecimalVector *ret = static_cast<DecimalVector *>(retval.get());

  ret->setTypeKind(typeKind);
  ret->setHasNull(hasNulls);
  if (hasNulls) ret->setNulls(getNulls() + start, extSz);

  uint64_t typLen = sizeof(int64_t) + sizeof(uint64_t) + sizeof(int64_t);
  if (values.data()) {
    ret->setValue(values.data() + start * typLen, extSz * typLen);
  }
  if (auxiliaryValues.data()) {
    ret->setAuxiliaryValue(auxiliaryValues.data() + start * typLen,
                           extSz * typLen);
  }
  if (scaleValues.data()) {
    ret->setScaleValue(scaleValues.data() + start * typLen, extSz * typLen);
  }
  ret->setSelected(sel, true);

  assert(ret->isValid());
  return std::move(retval);
}

std::unique_ptr<Vector> DecimalVector::replicateItemToVec(
    uint64_t index, int replicateNum) const {
  assert(isValid());
  auto retval = Vector::BuildVector(getTypeKind(), true);
  auto ret = static_cast<DecimalVector *>(retval.get());

  ret->setTypeKind(typeKind);
  ret->setHasNull(hasNulls);

  dbcommon::ByteBuffer valbuf(true);
  dbcommon::ByteBuffer auxbuf(true);
  dbcommon::ByteBuffer scbuf(true);
  uint64_t idx = getPlainIndex(index);
  auto value = values.get<uint64_t>(idx);
  auto auxValue = auxiliaryValues.get<int64_t>(idx);
  auto scaleVal = scaleValues.get<int64_t>(idx);
  auto null = isNullPlain(idx);

  if (hasNulls) {
    for (int i = 0; i < replicateNum; ++i) {
      valbuf.append(value);
      auxbuf.append(auxValue);
      scbuf.append(scaleVal);
      ret->nulls.append(null);
    }
  } else {
    for (int i = 0; i < replicateNum; ++i) {
      valbuf.append(values.get<uint64_t>(idx));
      auxbuf.append(auxiliaryValues.get<int64_t>(idx));
      scbuf.append(scaleValues.get<int64_t>(idx));
    }
  }
  ret->values.swap(valbuf);
  ret->auxiliaryValues.swap(auxbuf);
  ret->scaleValues.swap(scbuf);
  assert(ret->isValid());
  return std::move(retval);
}

void DecimalVector::merge(size_t plainSize, const std::vector<Datum> &data,
                          const std::vector<SelectList *> &sels) {
  assert(data.size() == sels.size());

  values.resize(plainSize * sizeof(uint64_t));
  auxiliaryValues.resize(plainSize * sizeof(int64_t));
  scaleValues.resize(plainSize * sizeof(int64_t));
  nulls.resize(plainSize);
  uint64_t *__restrict__ lvals = reinterpret_cast<uint64_t *>(values.data());
  int64_t *__restrict__ hvals =
      reinterpret_cast<int64_t *>(auxiliaryValues.data());
  int64_t *__restrict__ svals = reinterpret_cast<int64_t *>(scaleValues.data());
  bool *__restrict__ nulls = this->nulls.data();

  for (auto inputIdx = 0; inputIdx < data.size(); inputIdx++) {
    auto sel = sels[inputIdx];
    if (sel == nullptr) continue;
    auto datum = DatumGetValue<Object *>(data[inputIdx]);

    if (auto vec = dynamic_cast<Vector *>(datum)) {
      assert(dynamic_cast<DecimalVector *>(vec));
      DecimalVector *dvec = dynamic_cast<DecimalVector *>(vec);
      auto lvalsIn = reinterpret_cast<const uint64_t *>(dvec->getValue());
      auto hvalsIn =
          reinterpret_cast<const int64_t *>(dvec->getAuxiliaryValue());
      auto svalsIn = reinterpret_cast<const int64_t *>(dvec->getScaleValue());
      for (auto idx : *sel) {
        lvals[idx] = lvalsIn[idx];
        hvals[idx] = hvalsIn[idx];
        svals[idx] = svalsIn[idx];
      }
      if (dvec->hasNullValue()) {
        auto nullsIn = dvec->getNulls();
        for (auto idx : *sel) nulls[idx] = nullsIn[idx];
      } else {
        for (auto idx : *sel) nulls[idx] = false;
      }
    } else {
      auto scalar = reinterpret_cast<Scalar *>(datum);
      DecimalVar *val = DatumGetValue<DecimalVar *>(scalar->value);
      auto null = scalar->isnull;
      if (null) {
        for (auto idx : *sel) {
          nulls[idx] = true;
        }
      } else {
        for (auto idx : *sel) {
          lvals[idx] = val->lowbits;
          hvals[idx] = val->highbits;
          svals[idx] = val->scale;
          nulls[idx] = false;
        }
      }
    }
  }
}

void DecimalVector::hash(std::vector<uint64_t> &retval) const {
  // TODO(wshao) :Optimize the performance of hash function in terms of the
  // both highbits and lowbits value.
  auto base = reinterpret_cast<const int64_t *>(getValue());

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

void DecimalVector::cdbHash(std::vector<uint64_t> &retval) const {
  auto base = reinterpret_cast<const int64_t *>(getValue());
  size_t len = sizeof(uint64_t);

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

void DecimalVector::swap(Vector &vect) {
  Vector::swap(vect);
  auto &v = dynamic_cast<DecimalVector &>(vect);
  values.swap(v.values);
  auxiliaryValues.swap(v.auxiliaryValues);
  scaleValues.swap(v.scaleValues);
}

void DecimalVector::checkGroupByKeys(
    std::vector<const void *> &dataPtrSrc) const {
  const uint64_t *data = reinterpret_cast<const uint64_t *>(values.data());
  const int64_t *auxdata =
      reinterpret_cast<const int64_t *>(auxiliaryValues.data());
  const int64_t *scdata = reinterpret_cast<const int64_t *>(scaleValues.data());
  const uint64_t size = getNumOfRows();
  const char **dataPtr = reinterpret_cast<const char **>(&dataPtrSrc[0]);
  if (selected) {
    auto &sel = *selected;

#pragma clang loop unroll_count(4)
    for (uint64_t i = 0; i < size; ++i) {
      if (dataPtr[i] != nullptr) {
        uint64_t index = sel[i];
        char null = hasNulls ? nulls.getChar(index) : 0;
        const uint64_t d = data[index];
        const int64_t a = auxdata[index];
        const int64_t s = scdata[index];

        const DecimalDataInfo *info =
            reinterpret_cast<const DecimalDataInfo *>(dataPtr[i]);
        if (null == info->isNull &&
            (null || (a == info->highbitsData && d == info->lowbitsData &&
                      s == info->scaleData)))
          dataPtr[i] = alignedAddress(dataPtr[i] + sizeof(DecimalDataInfo));
        else
          dataPtr[i] = nullptr;
      }
    }
  } else {
    if (hasNulls) {
      for (uint64_t i = 0; i < size; ++i) {
        if (dataPtr[i] != nullptr) {
          char null = nulls.getChar(i);
          const uint64_t d = data[i];
          const int64_t a = auxdata[i];
          const int64_t s = scdata[i];

          const DecimalDataInfo *info =
              reinterpret_cast<const DecimalDataInfo *>(dataPtr[i]);
          if (null == info->isNull &&
              (null || (a == info->highbitsData && d == info->lowbitsData &&
                        s == info->scaleData)))
            dataPtr[i] = alignedAddress(dataPtr[i] + sizeof(DecimalDataInfo));
          else
            dataPtr[i] = nullptr;
        }
      }
    } else {
      for (uint64_t i = 0; i < size; ++i) {
        if (dataPtr[i] != nullptr) {
          char null = false;
          const uint64_t d = data[i];
          const int64_t a = auxdata[i];
          const int64_t s = scdata[i];

          const DecimalDataInfo *info =
              reinterpret_cast<const DecimalDataInfo *>(dataPtr[i]);
          if (null == info->isNull &&
              (null || (a == info->highbitsData && d == info->lowbitsData &&
                        s == info->scaleData)))
            dataPtr[i] = alignedAddress(dataPtr[i] + sizeof(DecimalDataInfo));
          else
            dataPtr[i] = nullptr;
        }
      }
    }
  }
}

uint64_t DecimalVector::checkGroupByKey(uint64_t index,
                                        const char *dataPtr) const {
  assert(index < getNumOfRows() && "invalid input");
  assert(getNumOfRows() > 0);
  uint64_t i = getPlainIndex(index);
  const DecimalDataInfo *info =
      reinterpret_cast<const DecimalDataInfo *>(dataPtr);
  if (isNullPlain(i) != info->isNull) return 0;
  if (!info->isNull) {
    if (*reinterpret_cast<uint64_t *>(values.data() + i * (sizeof(uint64_t))) !=
        info->lowbitsData)
      return 0;
    if (*reinterpret_cast<int64_t *>(auxiliaryValues.data() +
                                     i * (sizeof(int64_t))) !=
        info->highbitsData)
      return 0;
    if (*reinterpret_cast<int64_t *>(scaleValues.data() +
                                     i * (sizeof(int64_t))) != info->scaleData)
      return 0;
  }
  return sizeof(DecimalDataInfo);
}

uint64_t DecimalVector::getSerializeSizeInternal() const {
  size_t sSize = Vector::getSerializeSizeInternal();
  sSize += sizeof(uint64_t);
  sSize += values.size();
  sSize += auxiliaryValues.size();
  sSize += scaleValues.size();
  return sSize;
}

void DecimalVector::serializeInternal(NodeSerializer *serializer) {
  Vector::serializeInternal(serializer);

  uint64_t vsz = getNumOfRows() * sizeof(int64_t);
  serializer->write<uint64_t>(vsz * 3);
  if (vsz > 0) {
    if (allSelected()) {
      serializer->writeBytes(getValue(), vsz);
      serializer->writeBytes(getAuxiliaryValue(), vsz);
      serializer->writeBytes(getScaleValue(), vsz);
    } else {
      auto values = reinterpret_cast<const uint64_t *>(getValue());
      for (auto i : *selected) {
        serializer->write(values[i]);
      }
      auto auxValues = reinterpret_cast<const int64_t *>(getAuxiliaryValue());
      for (auto i : *selected) {
        serializer->write(auxValues[i]);
      }
      auto scaleValues = reinterpret_cast<const int64_t *>(getScaleValue());
      for (auto i : *selected) {
        serializer->write(scaleValues[i]);
      }
    }
  }
}

void DecimalVector::deserializeInternal(NodeDeserializer *deserializer) {
  Vector::deserializeInternal(deserializer);

  uint64_t vsz = deserializer->read<uint64_t>();
  assert(vsz % 3 == 0);
  if (vsz > 0) {
    const char *dstr = deserializer->readBytes(vsz);
    setValue(dstr, vsz / 3);
    setAuxiliaryValue(dstr + vsz / 3, vsz / 3);
    setScaleValue(dstr + vsz / 3 + vsz / 3, vsz / 3);
  }

  assert(isValid());
}

void DecimalVector::selectDistinct(SelectList *selected, Scalar *scalar) {
  SelectList *sel = getSelected();
  uint64_t sz = getNumOfRows();
  const uint64_t *lowbit = reinterpret_cast<const uint64_t *>(values.data());
  const int64_t *highbit =
      reinterpret_cast<const int64_t *>(getAuxiliaryValue());
  const int64_t *scale = reinterpret_cast<const int64_t *>(getScaleValue());

  auto operation = [&](uint64_t plainIdx) {  // NOLINT
    if (!scalar->isnull) {
      DecimalVar *value = DatumGetValue<DecimalVar *>(scalar->value);
      return (lowbit[plainIdx] != value->lowbits ||
              highbit[plainIdx] != value->highbits ||
              scale[plainIdx] != value->scale);
    }
    return false;
  };
  const bool *nulls = getNulls();
  dbcommon::transformDistinct(sz, sel, nulls, selected, scalar->isnull,
                              operation);
}

void DecimalVector::selectDistinct(SelectList *selected, Vector *vect) {
  SelectList *sel1 = getSelected();
  SelectList *sel2 = vect->getSelected();
  uint64_t sz = getNumOfRows();
  assert(sz == vect->getNumOfRows() && "invalid input");

  const uint64_t *lowbit1 = reinterpret_cast<const uint64_t *>(values.data());
  const int64_t *highbit1 =
      reinterpret_cast<const int64_t *>(getAuxiliaryValue());
  const int64_t *scale1 = reinterpret_cast<const int64_t *>(getScaleValue());
  const uint64_t *lowbit2 =
      reinterpret_cast<const uint64_t *>(vect->getValue());
  const int64_t *highbit2 =
      reinterpret_cast<const int64_t *>(vect->getAuxiliaryValue());
  const int64_t *scale2 =
      reinterpret_cast<const int64_t *>(vect->getScaleValue());

  auto operation = [&](uint64_t plainIdx) {  // NOLINT
    return (lowbit1[plainIdx] != lowbit2[plainIdx] ||
            highbit1[plainIdx] != highbit2[plainIdx] ||
            scale1[plainIdx] != scale2[plainIdx]);
  };
  const bool *nulls1 = getNulls();
  const bool *nulls2 = vect->getNulls();
  dbcommon::transformDistinct(sz, sel1, nulls1, nulls2, selected, operation);
}

const char *DecimalVector::readRawValue(uint64_t index, uint64_t *len,
                                        bool *null) const {
  assert(index < getNumOfRows() && "invalid input");
  assert(getNumOfRows() > 0);

  assert(nullptr != null && "invalid input");
  assert(nullptr != len && "invalid input");

  uint64_t i = getPlainIndex(index);

  *null = isNullPlain(i);

  *len = lengths.get<uint64_t>(i);

  return valPtrs.get<const char *>(i);
}

void DecimalVector::computeRawValueAndValPtrs() {
  assert(rawValues.size() == 0);
  assert(lengths.size() == 0);
  assert(valPtrs.size() == 0);
  int sz = getNumOfRowsPlain();
  rawValues.reserve(DEFAULT_RESERVED_SIZE_OF_STRING * sz);
  lengths.resize(sizeof(uint64_t) * sz);
  uint64_t *lenVal = reinterpret_cast<uint64_t *>(lengths.data());
  valPtrs.resize(lengths.size());
  for (int i = 0; i < sz; ++i) {
    bool isNull;
    std::string plain = readPlain(i, &isNull);
    rawValues.append(plain.c_str(), plain.length());
    lenVal[i] = plain.length();
  }
  auto retrievedSize = this->computeValPtrs(
      rawValues.data(), reinterpret_cast<const uint64_t *>(lengths.data()),
      hasNulls ? getNulls() : nullptr, sz, (const char **)valPtrs.data());
  assert(rawValues.size() == retrievedSize);
}

void DecimalVector::resizeInternal(size_t plainSize) {
  auxiliaryValues.resize(plainSize * sizeof(uint64_t));
  values.resize(plainSize * sizeof(uint64_t));
  scaleValues.resize(plainSize * sizeof(uint64_t));
}

bool DecimalVector::checkZeroValue() const {
  bool hasZero = false;
  DecimalVectorRawData vec(
      const_cast<Vector *>(reinterpret_cast<const Vector *>(this)));
  auto checkZero = [&](SelectList::value_type plainIdx) {
    hasZero |= (vec.hightbits[plainIdx] == 0 && vec.lowbits[plainIdx] == 0);
  };
  transformVector(vec.plainSize, vec.sel, vec.nulls, checkZero);
  return hasZero;
}

}  // namespace dbcommon
