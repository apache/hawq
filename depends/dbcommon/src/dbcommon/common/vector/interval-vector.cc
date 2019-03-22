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

#include "dbcommon/common/vector/interval-vector.h"

#include "dbcommon/common/vector-transformer.h"

namespace dbcommon {

const char *IntervalVector::read(uint64_t index, uint64_t *len,
                                 bool *null) const {
  assert(index < getNumOfRows() && "invalid input");
  assert(nullptr != null && "invalid input");
  assert(nullptr != len && "invalid input");

  uint64_t i = getPlainIndex(index);
  *null = isNullPlain(i);

  assert(nextBuffer == 0 || nextBuffer == 1);
  nextBuffer = 1 - nextBuffer;
  readBuffer[nextBuffer].timeOffset =
      *reinterpret_cast<int64_t *>(values.data() + (i * sizeof(int64_t)));
  readBuffer[nextBuffer].day =
      *reinterpret_cast<int32_t *>(dayValues.data() + (i * sizeof(int32_t)));
  readBuffer[nextBuffer].month =
      *reinterpret_cast<int32_t *>(monthValues.data() + (i * sizeof(int32_t)));
  *len = sizeof(int64_t) + sizeof(int32_t) + sizeof(int32_t);
  return reinterpret_cast<const char *>(&readBuffer[nextBuffer]);
}

std::string IntervalVector::readPlain(uint64_t index, bool *null) const {
  *null = isNullPlain(index);
  if (*null) return "";
  return std::move(IntervalType::toString(values.get<int64_t>(index),
                                          dayValues.get<int32_t>(index),
                                          monthValues.get<int32_t>(index)));
}

void IntervalVector::readPlainScalar(uint64_t index, Scalar *scalar) const {
  assert(nextBuffer == 0 || nextBuffer == 1);
  nextBuffer = 1 - nextBuffer;
  readBuffer[nextBuffer].timeOffset =
      *reinterpret_cast<int64_t *>(values.data() + (index * sizeof(int64_t)));
  readBuffer[nextBuffer].day = *reinterpret_cast<int32_t *>(
      dayValues.data() + (index * sizeof(int32_t)));
  readBuffer[nextBuffer].month = *reinterpret_cast<int32_t *>(
      monthValues.data() + (index * sizeof(int32_t)));
  scalar->value =
      CreateDatum(reinterpret_cast<const char *>(&readBuffer[nextBuffer]));
  scalar->length = sizeof(IntervalVar);
  scalar->isnull = isNullPlain(index);
}

void IntervalVector::append(const std::string &strValue, bool null) {
  assert(getOwnData());
  IntervalVar value = {0, 0, 0};
  if (!null) {
    value = IntervalType::fromString(strValue);
  }
  append(reinterpret_cast<char *>(&value), sizeof(IntervalVar), null);
}

void IntervalVector::append(const char *v, uint64_t valueLen, bool null) {
  assert(getOwnData() && v != nullptr);
  assert(valueLen == sizeof(int64_t) + sizeof(int32_t) + sizeof(int32_t));

  int64_t timeOffset = *(reinterpret_cast<const int64_t *>(v));
  int32_t dayInterval = *(reinterpret_cast<const int32_t *>(v + 8));
  int32_t monthInterval = *(reinterpret_cast<const int32_t *>(v + 12));
  values.append<int64_t>(timeOffset);
  dayValues.append<int32_t>(dayInterval);
  monthValues.append<int32_t>(monthInterval);
  appendNull(null);
}

void IntervalVector::append(const Datum &datum, bool null) {
  dbcommon::IntervalVar *val = DatumGetValue<IntervalVar *>(datum);
  if (val) {
    values.append(val->timeOffset);
    dayValues.append(val->day);
    monthValues.append(val->month);
  } else {
    assert(null == true);
    values.append(int64_t(0));
    dayValues.append(int32_t(0));
    monthValues.append(int32_t(0));
  }
  appendNull(null);
}

void IntervalVector::append(const Scalar *scalar) {
  dbcommon::IntervalVar *val = DatumGetValue<IntervalVar *>(scalar->value);
  if (val) {
    values.append(val->timeOffset);
    dayValues.append(val->day);
    monthValues.append(val->month);
  }
  appendNull(scalar->isnull);
}

void IntervalVector::append(Vector *src, uint64_t index) {
  bool isnull = false;
  uint64_t len = 0;
  const char *ret = src->read(index, &len, &isnull);
  append(ret, len, isnull);
}

void IntervalVector::append(Vector *vect) {
  assert(getOwnData());
  this->append(vect, vect->getSelected());
}

void IntervalVector::append(Vector *vect, SelectList *sel) {
  Vector::append(vect, sel);
  const dbcommon::ByteBuffer *timeOffsetBuffer = vect->getValueBuffer();
  const dbcommon::ByteBuffer *dayBuffer = vect->getDayBuffer();
  const dbcommon::ByteBuffer *monthBuffer = vect->getMonthBuffer();
  if (!sel || sel->size() == vect->getNumOfRowsPlain()) {
    values.append(timeOffsetBuffer->data(), timeOffsetBuffer->size());
    dayValues.append(dayBuffer->data(), dayBuffer->size());
    monthValues.append(monthBuffer->data(), monthBuffer->size());
  } else {
    uint64_t newSize = sel->size();
    for (uint64_t i = 0; i < newSize; i++) {
      values.append(timeOffsetBuffer->get<int64_t>((*sel)[i]));
      dayValues.append(dayBuffer->get<int32_t>((*sel)[i]));
      monthValues.append(monthBuffer->get<int32_t>((*sel)[i]));
    }
  }
}

void IntervalVector::materialize() {
  values.materialize<int64_t>(selected);
  dayValues.materialize<int32_t>(selected);
  monthValues.materialize<int32_t>(selected);
  if (hasNulls) nulls.materialize(selected);
  selected = nullptr;
}

std::unique_ptr<Vector> IntervalVector::cloneSelected(
    const SelectList *sel) const {
  assert(isValid());

  std::unique_ptr<Vector> retval = Vector::BuildVector(getTypeKind(), false);
  IntervalVector *ret = static_cast<IntervalVector *>(retval.get());

  ret->setTypeKind(typeKind);
  ret->setHasNull(hasNulls);
  if (hasNulls) ret->setNulls(getNulls(), getNumOfRowsPlain());
  ret->setValue(values.data(), values.size());
  ret->setDayValue(dayValues.data(), dayValues.size());
  ret->setMonthValue(monthValues.data(), monthValues.size());

  ret->setSelected(sel, true);
  ret->materialize();

  assert(ret->isValid());
  return std::move(retval);
}

std::unique_ptr<Vector> IntervalVector::extract(const SelectList *sel,
                                                uint64_t start,
                                                uint64_t end) const {
  assert(isValid() && start < end);
  uint64_t total = getNumOfRowsPlain();
  assert(end <= total);
  uint64_t extSz = end - start;

  std::unique_ptr<Vector> retval = Vector::BuildVector(getTypeKind(), false);
  IntervalVector *ret = static_cast<IntervalVector *>(retval.get());

  ret->setTypeKind(typeKind);
  ret->setHasNull(hasNulls);
  if (hasNulls) ret->setNulls(getNulls() + start, extSz);

  uint64_t typLen = sizeof(int64_t) + sizeof(int32_t) + sizeof(int32_t);
  if (values.data()) {
    ret->setValue(values.data() + start * typLen, extSz * typLen);
  }
  if (dayValues.data()) {
    ret->setDayValue(dayValues.data() + start * typLen, extSz * typLen);
  }
  if (monthValues.data()) {
    ret->setMonthValue(monthValues.data() + start * typLen, extSz * typLen);
  }
  ret->setSelected(sel, true);

  assert(ret->isValid());
  return std::move(retval);
}

std::unique_ptr<Vector> IntervalVector::replicateItemToVec(
    uint64_t index, int replicateNum) const {
  assert(isValid());
  auto retval = Vector::BuildVector(getTypeKind(), true);
  auto ret = static_cast<IntervalVector *>(retval.get());

  ret->setTypeKind(typeKind);
  ret->setHasNull(hasNulls);

  dbcommon::ByteBuffer timebuf(true);
  dbcommon::ByteBuffer daybuf(true);
  dbcommon::ByteBuffer monthbuf(true);
  uint64_t idx = getPlainIndex(index);
  auto tmValue = values.get<int64_t>(idx);
  auto dayValue = dayValues.get<int32_t>(idx);
  auto monthValue = monthValues.get<int32_t>(idx);
  auto null = isNullPlain(idx);

  if (hasNulls) {
    for (int i = 0; i < replicateNum; ++i) {
      timebuf.append(tmValue);
      daybuf.append(dayValue);
      monthbuf.append(monthValue);
      ret->nulls.append(null);
    }
  } else {
    for (int i = 0; i < replicateNum; ++i) {
      timebuf.append(values.get<int64_t>(idx));
      daybuf.append(dayValues.get<int32_t>(idx));
      monthbuf.append(monthValues.get<int32_t>(idx));
    }
  }
  ret->values.swap(timebuf);
  ret->dayValues.swap(daybuf);
  ret->monthValues.swap(monthbuf);
  assert(ret->isValid());
  return std::move(retval);
}

void IntervalVector::merge(size_t plainSize, const std::vector<Datum> &data,
                           const std::vector<SelectList *> &sels) {
  assert(data.size() == sels.size());

  values.resize(plainSize * sizeof(int64_t));
  dayValues.resize(plainSize * sizeof(int32_t));
  monthValues.resize(plainSize * sizeof(int32_t));
  nulls.resize(plainSize);
  int64_t *__restrict__ tvals = reinterpret_cast<int64_t *>(values.data());
  int32_t *__restrict__ dvals = reinterpret_cast<int32_t *>(dayValues.data());
  int32_t *__restrict__ mvals = reinterpret_cast<int32_t *>(monthValues.data());
  bool *__restrict__ nulls = this->nulls.data();

  for (auto inputIdx = 0; inputIdx < data.size(); inputIdx++) {
    auto sel = sels[inputIdx];
    if (sel == nullptr) continue;
    auto datum = DatumGetValue<Object *>(data[inputIdx]);

    if (auto vec = dynamic_cast<Vector *>(datum)) {
      assert(dynamic_cast<IntervalVector *>(vec));
      IntervalVector *ivec = dynamic_cast<IntervalVector *>(vec);
      auto tvalsIn = reinterpret_cast<const int64_t *>(ivec->getValue());
      auto dvalsIn = reinterpret_cast<const int32_t *>(ivec->getDayValue());
      auto mvalsIn = reinterpret_cast<const int32_t *>(ivec->getMonthValue());
      for (auto idx : *sel) {
        tvals[idx] = tvalsIn[idx];
        dvals[idx] = dvalsIn[idx];
        mvals[idx] = mvalsIn[idx];
      }
      if (ivec->hasNullValue()) {
        auto nullsIn = ivec->getNulls();
        for (auto idx : *sel) nulls[idx] = nullsIn[idx];
      } else {
        for (auto idx : *sel) nulls[idx] = false;
      }
    } else {
      auto scalar = reinterpret_cast<Scalar *>(datum);
      IntervalVar *val = DatumGetValue<IntervalVar *>(scalar->value);
      auto null = scalar->isnull;
      if (null) {
        for (auto idx : *sel) {
          nulls[idx] = true;
        }
      } else {
        for (auto idx : *sel) {
          tvals[idx] = val->timeOffset;
          dvals[idx] = val->day;
          mvals[idx] = val->month;
          nulls[idx] = false;
        }
      }
    }
  }
}

void IntervalVector::hash(std::vector<uint64_t> &retval) const {
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

void IntervalVector::cdbHash(std::vector<uint64_t> &retval) const {
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

void IntervalVector::swap(Vector &vect) {
  Vector::swap(vect);
  auto &v = dynamic_cast<IntervalVector &>(vect);
  values.swap(v.values);
  dayValues.swap(v.dayValues);
  monthValues.swap(v.monthValues);
}

void IntervalVector::checkGroupByKeys(
    std::vector<const void *> &dataPtrSrc) const {
  const int64_t *tmdata = reinterpret_cast<const int64_t *>(values.data());
  const int32_t *daydata = reinterpret_cast<const int32_t *>(dayValues.data());
  const int32_t *mondata =
      reinterpret_cast<const int32_t *>(monthValues.data());
  const uint64_t size = getNumOfRows();
  const char **dataPtr = reinterpret_cast<const char **>(&dataPtrSrc[0]);
  if (selected) {
    auto &sel = *selected;

#pragma clang loop unroll_count(4)
    for (uint64_t i = 0; i < size; ++i) {
      if (dataPtr[i] != nullptr) {
        uint64_t index = sel[i];
        char null = hasNulls ? nulls.getChar(index) : 0;
        const int64_t t = tmdata[index];
        const int32_t d = daydata[index];
        const int32_t m = mondata[index];

        const IntervalDataInfo *info =
            reinterpret_cast<const IntervalDataInfo *>(dataPtr[i]);
        if (null == info->isNull &&
            (null || (t == info->timeOffsetData && d == info->dayData &&
                      m == info->monthData)))
          dataPtr[i] = alignedAddress(dataPtr[i] + sizeof(IntervalDataInfo));
        else
          dataPtr[i] = nullptr;
      }
    }
  } else {
    if (hasNulls) {
      for (uint64_t i = 0; i < size; ++i) {
        if (dataPtr[i] != nullptr) {
          char null = nulls.getChar(i);
          const int64_t t = tmdata[i];
          const int32_t d = daydata[i];
          const int32_t m = mondata[i];

          const IntervalDataInfo *info =
              reinterpret_cast<const IntervalDataInfo *>(dataPtr[i]);
          if (null == info->isNull &&
              (null || (t == info->timeOffsetData && d == info->dayData &&
                        m == info->monthData)))
            dataPtr[i] = alignedAddress(dataPtr[i] + sizeof(IntervalDataInfo));
          else
            dataPtr[i] = nullptr;
        }
      }
    } else {
      for (uint64_t i = 0; i < size; ++i) {
        if (dataPtr[i] != nullptr) {
          char null = false;
          const int64_t t = tmdata[i];
          const int32_t d = daydata[i];
          const int32_t m = mondata[i];

          const IntervalDataInfo *info =
              reinterpret_cast<const IntervalDataInfo *>(dataPtr[i]);
          if (null == info->isNull &&
              (null || (t == info->timeOffsetData && d == info->dayData &&
                        m == info->monthData)))
            dataPtr[i] = alignedAddress(dataPtr[i] + sizeof(IntervalDataInfo));
          else
            dataPtr[i] = nullptr;
        }
      }
    }
  }
}

uint64_t IntervalVector::checkGroupByKey(uint64_t index,
                                         const char *dataPtr) const {
  assert(index < getNumOfRows() && "invalid input");
  assert(getNumOfRows() > 0);
  uint64_t i = getPlainIndex(index);
  const IntervalDataInfo *info =
      reinterpret_cast<const IntervalDataInfo *>(dataPtr);
  if (isNullPlain(i) != info->isNull) return 0;
  if (!info->isNull) {
    if (*reinterpret_cast<int64_t *>(values.data() + i * (sizeof(int64_t))) !=
        info->timeOffsetData)
      return 0;
    if (*reinterpret_cast<int32_t *>(dayValues.data() +
                                     i * (sizeof(int32_t))) != info->dayData)
      return 0;
    if (*reinterpret_cast<int32_t *>(monthValues.data() +
                                     i * (sizeof(int32_t))) != info->monthData)
      return 0;
  }
  return sizeof(IntervalDataInfo);
}

uint64_t IntervalVector::getSerializeSizeInternal() const {
  size_t sSize = Vector::getSerializeSizeInternal();
  sSize += sizeof(uint64_t);
  sSize += values.size();
  sSize += dayValues.size();
  sSize += monthValues.size();
  return sSize;
}

void IntervalVector::serializeInternal(NodeSerializer *serializer) {
  Vector::serializeInternal(serializer);

  uint64_t vsz = getNumOfRows() * sizeof(int64_t);
  serializer->write<uint64_t>(vsz * 2);
  if (vsz > 0) {
    if (allSelected()) {
      serializer->writeBytes(getValue(), vsz);
      serializer->writeBytes(getDayValue(), vsz / 2);
      serializer->writeBytes(getMonthValue(), vsz / 2);
    } else {
      auto values = reinterpret_cast<const int64_t *>(getValue());
      for (auto i : *selected) {
        serializer->write(values[i]);
      }
      auto dayVal = reinterpret_cast<const int32_t *>(getDayValue());
      for (auto i : *selected) {
        serializer->write(dayVal[i]);
      }
      auto monthVal = reinterpret_cast<const int32_t *>(getMonthValue());
      for (auto i : *selected) {
        serializer->write(monthVal[i]);
      }
    }
  }
}

void IntervalVector::deserializeInternal(NodeDeserializer *deserializer) {
  Vector::deserializeInternal(deserializer);

  uint64_t vsz = deserializer->read<int64_t>();
  if (vsz > 0) {
    const char *dstr = deserializer->readBytes(vsz);
    setValue(dstr, vsz / 2);
    setDayValue(dstr + vsz / 2, vsz / 4);
    setMonthValue(dstr + vsz / 2 + vsz / 4, vsz / 4);
  }

  assert(isValid());
}

void IntervalVector::selectDistinct(SelectList *selected, Scalar *scalar) {
  SelectList *sel = getSelected();
  uint64_t sz = getNumOfRows();
  const int64_t *timeOffsets = reinterpret_cast<const int64_t *>(getValue());
  const int32_t *days = reinterpret_cast<const int32_t *>(getDayValue());
  const int32_t *months = reinterpret_cast<const int32_t *>(getMonthValue());

  auto operation = [&](uint64_t plainIdx) {  // NOLINT
    if (!scalar->isnull) {
      IntervalVar *value = DatumGetValue<IntervalVar *>(scalar->value);
      return (timeOffsets[plainIdx] != value->timeOffset ||
              days[plainIdx] != value->day || months[plainIdx] != value->month);
    }
    return false;
  };
  const bool *nulls = getNulls();
  dbcommon::transformDistinct(sz, sel, nulls, selected, scalar->isnull,
                              operation);
}

void IntervalVector::selectDistinct(SelectList *selected, Vector *vect) {
  SelectList *sel1 = getSelected();
  SelectList *sel2 = vect->getSelected();
  uint64_t sz = getNumOfRows();
  assert(sz == vect->getNumOfRows() && "invalid input");

  const int64_t *timeOffsets1 = reinterpret_cast<const int64_t *>(getValue());
  const int32_t *days1 = reinterpret_cast<const int32_t *>(getDayValue());
  const int32_t *months1 = reinterpret_cast<const int32_t *>(getMonthValue());
  const int64_t *timeOffsets2 =
      reinterpret_cast<const int64_t *>(vect->getValue());
  const int32_t *days2 = reinterpret_cast<const int32_t *>(vect->getDayValue());
  const int32_t *months2 =
      reinterpret_cast<const int32_t *>(vect->getMonthValue());

  auto operation = [&](uint64_t plainIdx) {  // NOLINT
    return (timeOffsets1[plainIdx] != timeOffsets2[plainIdx] ||
            days1[plainIdx] != days2[plainIdx] ||
            months1[plainIdx] != months2[plainIdx]);
  };
  const bool *nulls1 = getNulls();
  const bool *nulls2 = vect->getNulls();
  dbcommon::transformDistinct(sz, sel1, nulls1, nulls2, selected, operation);
}

void IntervalVector::resizeInternal(size_t plainSize) {
  values.resize(plainSize * sizeof(int64_t));
  dayValues.resize(plainSize * sizeof(int32_t));
  monthValues.resize(plainSize * sizeof(int32_t));
}

}  // namespace dbcommon
