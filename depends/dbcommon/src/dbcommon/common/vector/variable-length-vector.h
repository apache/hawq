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

#ifndef DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_VARIABLE_LENGTH_VECTOR_H_
#define DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_VARIABLE_LENGTH_VECTOR_H_

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "dbcommon/common/vector-transformer.h"
#include "dbcommon/common/vector.h"

namespace dbcommon {

template <typename ValueType, typename ADT>
class VariableSizeTypeVector : public Vector {
 public:
  typedef ValueType value_type;
  typedef ADT abstract_type;

  explicit VariableSizeTypeVector(bool ownData)
      : Vector(ownData), lengths(ownData), valPtrs(ownData) {
    if (ownData) {
      values.reserve(DEFAULT_RESERVED_SIZE_OF_STRING *
                     DEFAULT_NUMBER_TUPLES_PER_BATCH);
      lengths.reserve(8 * DEFAULT_NUMBER_TUPLES_PER_BATCH);
      valPtrs.reserve(8 * DEFAULT_NUMBER_TUPLES_PER_BATCH);
    }
  }

  void convert(const dbcommon::SupportedEncodingSet inputEncoding,
               const dbcommon::SupportedEncodingSet outputEncoding) override {
    assert(inputEncoding != outputEncoding);
    dbcommon::MbConverter converter(inputEncoding, outputEncoding);
    std::vector<std::string> tmp0;
    std::vector<bool> tmp1;
    size_t rowNum = this->getNumOfRows();
    bool isNull;
    // if the vector contains selectlist, the vector behaves similar to
    // plainize()
    if (this->allSelected()) {
      for (size_t i = 0; i < rowNum; i++) {
        std::string str = this->readPlain(i, &isNull);
        str = converter.convert(str);
        tmp0.push_back(str);
        tmp1.push_back(isNull);
      }
    } else {
      for (size_t i = 0; i < rowNum; i++) {
        size_t index = (*selected)[i];
        std::string str = this->readPlain(index, &isNull);
        str = converter.convert(str);
        tmp0.push_back(str);
        tmp1.push_back(isNull);
      }
    }
    this->reset();
    size_t newRowNum = tmp0.size();
    for (size_t i = 0; i < newRowNum; i++) {
      this->append(tmp0[i], tmp1[i]);
    }
  }

  virtual ~VariableSizeTypeVector() {}

  uint64_t getNumOfRowsPlain() const override {
    return lengths.size() / sizeof(uint64_t);
  }

  void setValue(const char *value, uint64_t size) override {
    assert(!getOwnData() && value != nullptr && size >= 0);
    values.setData(value, size);
  }

  void materialize() override {
    if (isDirectEncoding && getValue() && !selected) {
      values.materialize();
    } else {
      ByteBuffer valBuf(true);
      computeValues(getValPtrs(), getLengths(), hasNulls ? getNulls() : nullptr,
                    getNumOfRowsPlain(), selected, &valBuf);
      values.swap(valBuf);
    }

    lengths.materialize<uint64_t>(selected);

    if (hasNulls) nulls.materialize(selected);

    selected = nullptr;

    computeValPtrs();
  }

  std::unique_ptr<Vector> cloneSelected(const SelectList *sel) const override {
    auto retval = Vector::BuildVector(typeKind, false, typeModifier);
    auto ret =
        reinterpret_cast<VariableSizeTypeVector<value_type, abstract_type> *>(
            retval.get());

    ret->setTypeKind(typeKind);
    ret->setHasNull(hasNulls);
    ret->setTypeModifier(typeModifier);

    if (hasNulls) ret->setNulls(getNulls(), getNumOfRowsPlain());
    if (values.data()) ret->setValue(values.data(), values.size());
    ret->setLengths(getLengths(), getNumOfRowsPlain());
    if (valPtrs.data()) ret->setValPtrs(getValPtrs(), getNumOfRowsPlain());

    ret->setSelected(sel, true);
    ret->isDirectEncoding = this->isDirectEncoding;
    ret->materialize();

    ret->isDirectEncoding = true;

    assert(ret->isValid());
    return retval;
  }

  std::unique_ptr<Vector> extract(const SelectList *sel, uint64_t start,
                                  uint64_t end) const override {
    assert(isValid() && start < end);
    uint64_t total = getNumOfRowsPlain();
    assert(end <= total);
    uint64_t extSz = end - start;

    auto retval = Vector::BuildVector(typeKind, false, typeModifier);
    auto ret =
        reinterpret_cast<VariableSizeTypeVector<value_type, abstract_type> *>(
            retval.get());

    ret->setTypeKind(typeKind);
    ret->setHasNull(hasNulls);
    ret->setTypeModifier(typeModifier);

    if (hasNulls) ret->setNulls(getNulls() + start, extSz);

    const char *pStart = getValPtrPlain(start);
    const char *pEnd = (end != total)
                           ? getValPtrPlain(end)
                           : getValPtrPlain(end - 1) + getLengths()[end - 1];
    uint64_t valSz = static_cast<uint64_t>(pEnd - pStart);

    if (values.data()) {
      ret->setValue(pStart, valSz);
    }

    ret->setLengths(getLengths() + start, extSz);
    ret->setValPtrs(getValPtrs() + start, extSz);

    ret->setSelected(sel, true);
    ret->isDirectEncoding = true;

    assert(ret->isValid());
    return std::move(retval);
  }

  std::unique_ptr<Vector> replicateItemToVec(uint64_t index,
                                             int replicateNum) const override {
    assert(isValid());

    std::unique_ptr<Vector> retval(
        new VariableSizeTypeVector<value_type, abstract_type>(true));
    auto rt = static_cast<VariableSizeTypeVector<value_type, abstract_type> *>(
        retval.get());

    rt->setTypeKind(typeKind);
    rt->setHasNull(hasNulls);
    rt->setTypeModifier(typeModifier);

    dbcommon::ByteBuffer lengthBuf(true);
    dbcommon::ByteBuffer valueBuf(true);

    uint64_t idx = getPlainIndex(index);
    bool isNull = isNullPlain(idx);
    uint64_t len = getElementWidthPlain(idx);
    const char *curVal = getValPtrPlain(idx);

    if (hasNulls) {
      if (isNull) {
        for (uint64_t i = 0; i < replicateNum; ++i) {
          lengthBuf.append<uint64_t>(0);
          rt->nulls.append(isNull);
        }
      } else {
        for (uint64_t i = 0; i < replicateNum; ++i) {
          lengthBuf.append<uint64_t>(len);
          valueBuf.append(curVal, len);
          rt->nulls.append(isNull);
        }
      }
    } else {
      for (uint64_t i = 0; i < replicateNum; ++i) {
        lengthBuf.append<uint64_t>(len);
        valueBuf.append(curVal, len);
      }
    }

    rt->lengths.swap(lengthBuf);
    rt->values.swap(valueBuf);
    rt->computeValPtrs();

    assert(rt->isValid());

    return std::move(retval);
  }

  void merge(size_t plainSize, const std::vector<Datum> &data,
             const std::vector<SelectList *> &sels) override {
    assert(data.size() == sels.size());

    valPtrs.resize(plainSize * sizeof(value_type));
    lengths.resize(plainSize * sizeof(uint64_t));
    nulls.resize(plainSize);
    const char **__restrict__ valptrs =
        reinterpret_cast<const char **>(valPtrs.data());
    uint64_t *__restrict__ lens = reinterpret_cast<uint64_t *>(lengths.data());
    bool *__restrict__ nulls = this->nulls.data();
    std::fill_n(nulls, plainSize, true);  // skip non-selected data

    for (auto inputIdx = 0; inputIdx < data.size(); inputIdx++) {
      auto sel = sels[inputIdx];
      if (sel == nullptr) continue;
      auto datum = DatumGetValue<Object *>(data[inputIdx]);

      if (auto vec = dynamic_cast<Vector *>(datum)) {
        auto valPtrsIn = vec->getValPtrs();
        auto lensIn = vec->getLengths();
        for (auto idx : *sel) {
          valptrs[idx] = valPtrsIn[idx];
          lens[idx] = lensIn[idx];
        }
        if (vec->hasNullValue()) {
          auto nullsIn = vec->getNulls();
          for (auto idx : *sel) nulls[idx] = nullsIn[idx];
        } else {
          for (auto idx : *sel) nulls[idx] = false;
        }
      } else {
        auto scalar = reinterpret_cast<Scalar *>(datum);
        auto null = scalar->isnull;
        if (null) {
          for (auto idx : *sel) {
            nulls[idx] = true;
          }
        } else {
          auto valptr = DatumGetValue<const char *>(scalar->value);
          uint64_t len = scalar->length;
          for (auto idx : *sel) {
            valptrs[idx] = valptr;
            lens[idx] = len;
            nulls[idx] = false;
          }
        }
      }
    }  // End of loop each input data

    values.reset(false);
    isDirectEncoding = false;
    materialize();
  }

  const uint64_t *getLengths() const override {
    return reinterpret_cast<const uint64_t *>(lengths.data());
  }

  void setLengths(const uint64_t *lengthsPtr, uint64_t size) override {
    assert(!getOwnData() && lengthsPtr != nullptr && size >= 0);

    this->lengths.setData(reinterpret_cast<const char *>(lengthsPtr),
                          (size) * sizeof(uint64_t));
  }

  const char *getValPtrPlain(uint64_t index) const {
    assert(getNumOfRows() > 0 && index < getNumOfRowsPlain());
    return valPtrs.get<const char *>(index);
  }

  uint64_t getElementWidthPlain(uint64_t index) const {
    assert(index < getNumOfRowsPlain());
    return lengths.get<uint64_t>(index);
  }

  const char **getValPtrs() const override {
    return reinterpret_cast<const char **>(valPtrs.data());
  }

  void setValPtrs(const char **valPtrs, uint64_t size) override {
    assert(!getOwnData() && valPtrs != nullptr && size >= 0);

    this->valPtrs.setData(reinterpret_cast<const char *>(valPtrs),
                          (size) * sizeof(char *));
  }

  void computeValPtrs() override {
    if (!this->valPtrs.getOwnData()) {
      this->valPtrs.reset(true);
    }
    valPtrs.resize(lengths.size());
    auto retrievedSize = this->computeValPtrs(
        values.data(), getLengths(), hasNulls ? getNulls() : nullptr,
        getNumOfRowsPlain(), (const char **)valPtrs.data(), this->selected);
    assert(values.size() == retrievedSize ||
           (typeKind == TypeKind::CHARID && values.size() >= retrievedSize));
  }

  void checkGroupByKeys(std::vector<const void *> &dataPtrSrc) const override {
    const char *vals = VariableSizeTypeVector::getValue();
    const uint64_t *lens = VariableSizeTypeVector::getLengths();
    const char **valPtrs = VariableSizeTypeVector::getValPtrs();

    const uint64_t size = VariableSizeTypeVector::getNumOfRows();
    const char **dataPtr = reinterpret_cast<const char **>(&dataPtrSrc[0]);

    if (selected) {
      SelectList &sel = *selected;

      if (hasNulls) {
        for (uint64_t i = 0; i < size; ++i) {
          uint64_t index = sel[i];

          if (dataPtr[i] != nullptr) {
            // the order of the statement in this block significantly affect the
            // assembly organization, do not change it easily
            char null = nulls.getChar(index);
            uint64_t len = lens[index];
            const char *d = valPtrs[index];

            const StringDataInfo *info =
                reinterpret_cast<const StringDataInfo *>(dataPtr[i]);

            bool check = ((null == 0x0) ? (len == info->len &&
                                           simpleStrcmp(d, info->data, len))
                                        : true) &
                         (null == info->isNull);
            if (check)
              dataPtr[i] = alignedAddress(info->data + info->len);
            else
              dataPtr[i] = nullptr;
          }
        }
      } else {
        for (uint64_t i = 0; i < size; ++i) {
          uint64_t index = sel[i];

          if (dataPtr[i] != nullptr) {
            // the order of the statement in this block significantly affect the
            // assembly organization, do not change it easily
            uint64_t len = lens[index];
            const char *d = valPtrs[index];

            const StringDataInfo *info =
                reinterpret_cast<const StringDataInfo *>(dataPtr[i]);

            bool check = (len == info->len) & (0x0 == info->isNull);
            if (check && simpleStrcmp(d, info->data, len))
              dataPtr[i] = alignedAddress(info->data + info->len);
            else
              dataPtr[i] = nullptr;
          }
        }
      }
    } else {  // without select list
      if (isDirectEncoding && getOwnData()) {
        if (hasNulls) {
          for (uint64_t i = 0; i < size; ++i) {
            const char *d = valPtrs[i];
            if (dataPtr[i] != nullptr) {
              char null = nulls.getChar(i);
              uint64_t len = lens[i];

              const StringDataInfo *info =
                  reinterpret_cast<const StringDataInfo *>(dataPtr[i]);

              bool check = ((null == 0x0) ? (len == info->len &&
                                             simpleStrcmp(d, info->data, len))
                                          : true) &
                           (null == info->isNull);
              if (check)
                dataPtr[i] = alignedAddress(info->data + info->len);
              else
                dataPtr[i] = nullptr;
            }
          }
        } else {  // not Null
          const char *d = values.data();
          for (uint64_t i = 0; i < size; ++i) {
            uint64_t len = lens[i];
            if (dataPtr[i] != nullptr) {
              const StringDataInfo *info =
                  reinterpret_cast<const StringDataInfo *>(dataPtr[i]);

              bool check = (len == info->len) & (0x0 == info->isNull);
              if (check && simpleStrcmp(d, info->data, len))
                dataPtr[i] = alignedAddress(info->data + info->len);
              else
                dataPtr[i] = nullptr;
            }
            d += len;
          }
        }
      } else {  // dictionary encoding
        for (uint64_t i = 0; i < size; ++i) {
          if (dataPtr[i] != nullptr) {
            char null = hasNulls ? nulls.getChar(i) : 0;
            uint64_t len = lens[i];
            const char *d = valPtrs[i];

            const StringDataInfo *info =
                reinterpret_cast<const StringDataInfo *>(dataPtr[i]);

            bool check = ((null == 0x0) ? (len == info->len &&
                                           simpleStrcmp(d, info->data, len))
                                        : true) &
                         (null == info->isNull);
            if (check)
              dataPtr[i] = alignedAddress(info->data + info->len);
            else
              dataPtr[i] = nullptr;
          }
        }
      }
    }
  }
  uint64_t checkGroupByKey(uint64_t index, const char *dataPtr) const override {
    assert(index < getNumOfRows() && "invalid input");
    assert(getNumOfRows() > 0);

    uint64_t i = getPlainIndex(index);

    const StringDataInfo *info =
        reinterpret_cast<const StringDataInfo *>(dataPtr);
    if (isNullPlain(i) != info->isNull) return 0;
    if (!info->isNull) {
      if (info->len != VariableSizeTypeVector::getElementWidthPlain(i))
        return 0;
      const char *dataPtr0 = VariableSizeTypeVector::getValPtrPlain(i);
      if (!simpleStrcmp(dataPtr0, info->data, info->len)) return 0;
      return 9 + info->len;
    }
    return 9;  // both are null values
  }

  const char *read(uint64_t index, uint64_t *len, bool *null) const override {
    assert(index < getNumOfRows() && "invalid input");
    assert(getNumOfRows() > 0);

    assert(nullptr != null && "invalid input");
    assert(nullptr != len && "invalid input");

    uint64_t i = getPlainIndex(index);

    *null = isNullPlain(i);

    *len = getElementWidthPlain(i);

    return getValPtrPlain(i);
  }

  std::string readPlain(uint64_t index, bool *null) const override {
    *null = isNullPlain(index);
    if (*null) {
      return "NULL";
    } else {
      uint64_t len = getElementWidthPlain(index);
      return std::move(
          ADT::toString(getValPtrPlain(index), getElementWidthPlain(index)));
    }
  }

  void readPlainScalar(uint64_t index, Scalar *scalar) const override {
    scalar->value = CreateDatum(getValPtrPlain(index));
    scalar->length = getElementWidthPlain(index);
    scalar->isnull = isNullPlain(index);
  }

  void append(const char *v, uint64_t valueLen, bool null) override {
    assert(this->getOwnData());

    char *oldBegin = values.data();

    if (!null && valueLen > 0) {
      values.append(v, valueLen);
    } else {
      valueLen = 0;
    }

    lengths.append<uint64_t>(valueLen);
    appendNull(null);

    if (values.data() == oldBegin) {  // no realloc happens
      valPtrs.append<char *>(values.tail() - valueLen);
    } else {
      this->computeValPtrs();
    }
  }

  void append(Vector *src, uint64_t index) override {
    assert(this->getOwnData());

    bool isnull = false;
    uint64_t len = 0;
    const char *ret = src->read(index, &len, &isnull);
    append(ret, len, isnull);
  }

  void append(const std::string &strValue, bool null) override {
    assert(getOwnData());
    assert(!null || strValue.size() == 0);

    uint64_t len = 0;
    const value_type *value = NULL;
    value = abstract_type::fromString(strValue, &len);
    append(reinterpret_cast<const char *>(value), len, null);
  }

  // TODO(lei) this can be optimized since input datum
  // contains a pointer, do not need to materialize to
  // buffer
  void append(const Datum &datum, bool null) override {
    assert(getOwnData());

    std::string buffer;

    if (!null) {
      buffer = DatumToBinary(datum, typeKind);
    }

    append(buffer.data(), buffer.size(), null);
  }

  void append(const Scalar *scalar) override {
    append(DatumGetValue<const char *>(scalar->value), scalar->length,
           scalar->isnull);
  }

  void append(Vector *vect, SelectList *sel) override {
    assert(this->getOwnData() && selected == nullptr);

    Vector::append(vect, sel);

    char *oldBegin = values.data();
    // need append the whole vector
    uint64_t curOffset = values.size();
    if (!sel || sel->size() == vect->getNumOfRowsPlain()) {
      if (vect->getOwnData() &&
          reinterpret_cast<VariableSizeTypeVector<value_type, abstract_type> *>(
              vect)
              ->isDirectEncoding) {
        values.append(vect->getValueBuffer()->data(),
                      vect->getValueBuffer()->size());
        lengths.append(reinterpret_cast<const char *>(vect->getLengths()),
                       vect->getNumOfRowsPlain() * sizeof(uint64_t));
      } else {
        uint64_t newSize = vect->getNumOfRowsPlain();
        auto valPtrs = vect->getValPtrs();
        auto lens = vect->getLengths();

        lengths.append(reinterpret_cast<const char *>(lens),
                       newSize * sizeof(lens[0]));
        if (vect->hasNullValue()) {
          auto nulls = vect->getNulls();
          for (uint64_t i = 0; i < newSize; i++)
            if (!nulls[i]) values.append(valPtrs[i], lens[i]);
        } else {
          for (uint64_t i = 0; i < newSize; i++)
            values.append(valPtrs[i], lens[i]);
        }
      }
    } else {
      auto valPtrs = vect->getValPtrs();
      auto lens = vect->getLengths();

      if (vect->hasNullValue()) {
        auto nulls = vect->getNulls();
        for (auto i : *sel) {
          uint64_t len = lens[i];
          lengths.append<uint64_t>(len);
          if (!nulls[i]) values.append(valPtrs[i], len);
        }
      } else {
        for (auto i : *sel) {
          uint64_t len = lens[i];
          lengths.append<uint64_t>(len);
          values.append(valPtrs[i], len);
        }
      }
    }

    this->computeValPtrs();
  }

  void append(Vector *vect) override {
    assert(getOwnData());

    append(vect, vect->getSelected());
  }

  void reset(bool resetBelongingTupleBatch = false) override {
    Vector::reset(resetBelongingTupleBatch);
    values.resize(0);
    lengths.resize(0);
    valPtrs.resize(0);
  }

  bool isValid() const override {
    Vector::isValid();

    uint64_t rows = getNumOfRowsPlain();
    assert(lengths.size() == sizeof(uint64_t) * rows);
    assert(valPtrs.size() == sizeof(char *) * rows);

    if (rows == 0) {
      assert(values.size() == 0);
    } else {
      if (this->getOwnData() && values.size() > 0) {
        uint64_t len = 0;
        for (uint64_t i = 0; i < rows; i++) {
          if ((!hasNulls || !nulls.get(i)) && lengths.get<uint64_t>(i) > 0) {
            assert(valPtrs.get<char *>(i) >= values.data());
            assert(valPtrs.get<char *>(i) < values.data() + values.size());
            len += lengths.get<uint64_t>(i);
          }
        }
        assert(typeKind == dbcommon::TypeKind::CHARID || values.size() == len);
      }
    }

    for (int i = 0; i < rows; i++) {
      assert(this->getElementWidthPlain(i) == lengths.get<uint64_t>(i));
    }

    if (isDirectEncoding && getOwnData() && getSelected() == nullptr &&
        getNumOfRowsPlain() > 0) {
      const char *data = values.data();
      assert(data == getValPtrs()[0]);
      for (auto i = 1; i < getNumOfRowsPlain(); i++)
        if (!isNull(i - 1)) {
          data += getLengths()[i - 1];
          // fixme(xsheng): impact magma datatype testcases
          // assert(data == getValPtrs()[i]);
        }
    }

    return true;
  }

  void hash(std::vector<uint64_t> &retval) const override {
    const uint64_t *lens = reinterpret_cast<const uint64_t *>(lengths.data());

    if (selected) {
      if (hasNulls) {
        const char **valPs = reinterpret_cast<const char **>(valPtrs.data());
        for (uint64_t i = 0; i < retval.size(); ++i) {
          uint64_t idx = (*selected)[i];
          retval[i] = nulls.get(idx) ? 0 : raw_fast_hash(valPs[idx], lens[idx]);
        }
      } else {
        const char **valPs = reinterpret_cast<const char **>(valPtrs.data());
        for (uint64_t i = 0; i < retval.size(); ++i) {
          uint64_t idx = (*selected)[i];
          retval[i] = raw_fast_hash(valPs[idx], lens[idx]);
        }
      }
    } else {
      if (isDirectEncoding && !hasNulls && getOwnData()) {
        const char **valPs = reinterpret_cast<const char **>(valPtrs.data());
        const char *valP = valPs[0];
        for (uint64_t i = 0; i < retval.size(); ++i) {
          retval[i] = raw_fast_hash(valP, lens[i]);
          valP += lens[i];
        }
      } else {
        if (hasNulls) {
          const char **valPs = reinterpret_cast<const char **>(valPtrs.data());
          for (uint64_t i = 0; i < retval.size(); ++i) {
            retval[i] = nulls.get(i) ? 0 : raw_fast_hash(valPs[i], lens[i]);
          }
        } else {
          const char **valPs = reinterpret_cast<const char **>(valPtrs.data());
          for (uint64_t i = 0; i < retval.size(); ++i) {
            retval[i] = raw_fast_hash(valPs[i], lens[i]);
          }
        }
      }
    }
  }

  void cdbHash(std::vector<uint64_t> &retval) const override {
    const uint64_t *lens = reinterpret_cast<const uint64_t *>(lengths.data());

    if (selected) {
      if (hasNulls) {
        const char **valPs = reinterpret_cast<const char **>(valPtrs.data());
        for (uint64_t i = 0; i < retval.size(); ++i) {
          uint64_t idx = (*selected)[i];
          retval[i] = nulls.get(idx)
                          ? cdbhashnull(retval[i])
                          : cdbhash(valPs[idx], lens[idx], retval[i]);
        }
      } else {
        const char **valPs = reinterpret_cast<const char **>(valPtrs.data());
        for (uint64_t i = 0; i < retval.size(); ++i) {
          uint64_t idx = (*selected)[i];
          retval[i] = cdbhash(valPs[idx], lens[idx], retval[i]);
        }
      }
    } else {
      if (isDirectEncoding && !hasNulls && getOwnData()) {
        const char **valPs = reinterpret_cast<const char **>(valPtrs.data());
        const char *valP = valPs[0];
        for (uint64_t i = 0; i < retval.size(); ++i) {
          retval[i] = cdbhash(valP, lens[i], retval[i]);
          valP += lens[i];
        }
      } else {
        if (hasNulls) {
          const char **valPs = reinterpret_cast<const char **>(valPtrs.data());
          for (uint64_t i = 0; i < retval.size(); ++i) {
            retval[i] = nulls.get(i) ? cdbhashnull(retval[i])
                                     : cdbhash(valPs[i], lens[i], retval[i]);
          }
        } else {
          const char **valPs = reinterpret_cast<const char **>(valPtrs.data());
          for (uint64_t i = 0; i < retval.size(); ++i) {
            retval[i] = cdbhash(valPs[i], lens[i], retval[i]);
          }
        }
      }
    }
  }

  dbcommon::ByteBuffer *getValueBuffer() override { return &values; }
  dbcommon::ByteBuffer *getLengthsBuffer() { return &lengths; }

  void swap(Vector &vect) override {
    Vector::swap(vect);
    auto &v =
        dynamic_cast<VariableSizeTypeVector<value_type, abstract_type> &>(vect);
    this->values.swap(v.values);
    this->lengths.swap(v.lengths);
    this->valPtrs.swap(v.valPtrs);
  }

  uint64_t getSerializeSizeInternal() const override {
    uint64_t sSize = Vector::getSerializeSizeInternal();
    sSize += sizeof(uint64_t) + values.size();
    sSize += sizeof(uint64_t) + lengths.size();
    return sSize;
  }

  void serializeInternal(NodeSerializer *serializer) override {
    // null data
    Vector::serializeInternal(serializer);

    // value data
    uint64_t vsz = 0;
    auto lens = getLengths();
    auto valPtrs = getValPtrs();
    if (hasNulls) {  // has nulls
      const bool *nulls = getNullBuffer()->getBools();
      if (allSelected()) {  // no select list
        uint64_t size = getNumOfRowsPlain();
        for (auto idx = 0; idx < size; ++idx) {
          if (!nulls[idx]) vsz += lens[idx];
        }
        serializer->write<uint64_t>(vsz);
        for (uint64_t idx = 0; idx < size; ++idx) {
          if (!nulls[idx]) serializer->writeBytes(valPtrs[idx], lens[idx]);
        }
      } else {  // has select list
        for (auto idx : *selected) {
          if (!nulls[idx]) vsz += lens[idx];
        }
        serializer->write<uint64_t>(vsz);
        for (auto idx : *selected) {
          if (!nulls[idx]) serializer->writeBytes(valPtrs[idx], lens[idx]);
        }
      }
    } else {                // no nulls
      if (allSelected()) {  // no select list
        uint64_t size = getNumOfRowsPlain();
        for (auto idx = 0; idx < size; ++idx) vsz += lens[idx];
        serializer->write<uint64_t>(vsz);
        for (uint64_t idx = 0; idx < size; ++idx) {
          serializer->writeBytes(valPtrs[idx], lens[idx]);
        }
      } else {  // has select list
        for (auto idx : *selected) vsz += lens[idx];
        serializer->write<uint64_t>(vsz);
        for (auto idx : *selected) {
          serializer->writeBytes(valPtrs[idx], lens[idx]);
        }
      }
    }

    // length data
    if (allSelected()) {
      uint64_t lsz = lengths.size();
      serializer->write<uint64_t>(lsz);
      serializer->writeBytes(lengths.data(), lsz);
    } else {
      uint64_t lsz = selected->size() * sizeof(uint64_t);
      auto lens = getLengths();
      serializer->write<uint64_t>(lsz);
      for (auto idx : *selected) serializer->write(lens[idx]);
    }
  }

  void deserializeInternal(NodeDeserializer *deserializer) override {
    Vector::deserializeInternal(deserializer);

    uint64_t vsz = deserializer->read<uint64_t>();
    const char *dstr = deserializer->readBytes(vsz);
    setValue(dstr, vsz);

    uint64_t lsz = deserializer->read<uint64_t>();
    if (lsz > 0) {
      const char *ostr = deserializer->readBytes(lsz);
      setLengths((const uint64_t *)ostr, lsz / sizeof(uint64_t));
    }

    this->isDirectEncoding = true;
    computeValPtrs();

    assert(isValid());
  }

  void selectDistinct(SelectList *selected, Scalar *scalar) override {
    SelectList *sel = getSelected();
    uint64_t sz = getNumOfRows();
    const char **valptrs = this->VariableSizeTypeVector::getValPtrs();
    const uint64_t *lens = this->getLengths();

    auto operation = [&](uint64_t plainIdx) {  // NOLINT
      if (!scalar->isnull) {
        char *value = DatumGetValue<char *>(scalar->value);
        auto len = scalar->length;
        return (lens[plainIdx] != len ||
                !simpleStrcmp(valptrs[plainIdx], value, lens[plainIdx]));
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

    const char **valPtrs1 = this->VariableSizeTypeVector::getValPtrs();
    const uint64_t *lengths1 = this->getLengths();
    const char **valPtrs2 = vect->getValPtrs();
    const uint64_t *lengths2 = vect->getLengths();

    auto operation = [&](uint64_t plainIdx) {  // NOLINT
      return (lengths1[plainIdx] != lengths2[plainIdx] ||
              !simpleStrcmp(valPtrs1[plainIdx], valPtrs2[plainIdx],
                            lengths1[plainIdx]));
    };
    const bool *nulls1 = getNulls();
    const bool *nulls2 = vect->getNulls();
    dbcommon::transformDistinct(sz, sel1, nulls1, nulls2, selected, operation);
  }

  void setDirectEncoding(bool isDirectEncoding) override {
    this->isDirectEncoding = isDirectEncoding;
  }
  bool getIsDirectEncoding() { return isDirectEncoding; }

  void setCloneInMaterialize(bool cloneInMaterialize) {
    this->cloneInMaterialize = cloneInMaterialize;
  }
  bool getCloneInMaterialize() { return cloneInMaterialize; }

 protected:
  void resizeInternal(size_t plainSize) override {
    values.resize(0);
    valPtrs.resize(plainSize * sizeof(uint64_t));
    lengths.resize(plainSize * sizeof(uint64_t));
    ::memset(lengths.data(), 0, plainSize * sizeof(uint64_t));
  }

  static force_inline bool simpleStrcmp(const char *s1, const char *s2,
                                        uint64_t len) {
    uint64_t idx = 0;
    while (idx + 8 <= len) {
      if (*reinterpret_cast<const uint64_t *>(s1 + idx) !=
          *reinterpret_cast<const uint64_t *>(s2 + idx))
        return false;
      idx += 8;
    }
    if (idx + 4 <= len) {
      if (*reinterpret_cast<const uint32_t *>(s1 + idx) !=
          *reinterpret_cast<const uint32_t *>(s2 + idx))
        return false;
      idx += 4;
    }
    if (idx + 2 <= len) {
      if (*reinterpret_cast<const uint16_t *>(s1 + idx) !=
          *reinterpret_cast<const uint16_t *>(s2 + idx))
        return false;
      idx += 2;
    }
    if (idx < len) {
      if (*reinterpret_cast<const uint8_t *>(s1 + idx) !=
          *reinterpret_cast<const uint8_t *>(s2 + idx))
        return false;
    }
    return true;
  }

  // Compute the value pointer array according to input length array.
  // Each value locate continuously inside the data source.
  // @param[in] vals Source of values
  // @param[in] lens Length array
  // @param[in] size Size of input values
  // @param[out] valPtrs Output value pointer array
  // @return Size of retrieved bytes from vals
  static uint64_t computeValPtrs(const char *vals, const uint64_t *lens,
                                 const bool *nulls, uint64_t size,
                                 const char **valPtrs,
                                 const SelectList *sel = nullptr) {
    const char *ptr = vals;

    if (sel) {
      if (nulls) {
        for (auto i : *sel) {
          valPtrs[i] = ptr;
          ptr += nulls[i] ? 0 : lens[i];
        }
      } else {
        for (auto i : *sel) {
          valPtrs[i] = ptr;
          ptr += lens[i];
        }
      }
    } else {
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
    }

    return ptr - vals;
  }

  // Compute the value array
  // @param[in] valPtrs
  // @param[in] lens
  // @param[in] nulls
  // @param[in] size
  // @param[in] sel
  // @param[out] valBuf
  static void computeValues(const char **valPtrs, const uint64_t *lens,
                            const bool *nulls, uint64_t size,
                            const SelectList *sel,
                            dbcommon::ByteBuffer *valBuf) {
    assert(valBuf->getOwnData());

    if (sel) {
      if (nulls) {
        for (auto plainIdx : *sel) {
          if (!nulls[plainIdx])
            valBuf->append(valPtrs[plainIdx], lens[plainIdx]);
        }
      } else {
        for (auto plainIdx : *sel) {
          valBuf->append(valPtrs[plainIdx], lens[plainIdx]);
        }
      }
    } else {
      if (nulls) {
        for (auto plainIdx = 0; plainIdx < size; plainIdx++) {
          if (!nulls[plainIdx])
            valBuf->append(valPtrs[plainIdx], lens[plainIdx]);
        }
      } else {
        for (auto plainIdx = 0; plainIdx < size; plainIdx++) {
          valBuf->append(valPtrs[plainIdx], lens[plainIdx]);
        }
      }
    }
  }

  bool isDirectEncoding = false;  // false for dict encoding
  bool cloneInMaterialize =
      false;  // even with no selected it also clone when true
  dbcommon::ByteBuffer lengths;  // type: uint64_t
  dbcommon::ByteBuffer valPtrs;  // type: char *
  struct StringDataInfo {
    uint64_t len;
    char isNull;
    char data[7];  // serve as a virtual data pointer
  };
};

class BytesVector : public VariableSizeTypeVector<char, BytesType> {
 public:
  typedef VariableSizeTypeVector<char, BytesType> base_type;

  std::string readPlain(uint64_t index, bool *null) const override {
    *null = isNullPlain(index);
    if (*null) return "NULL";
    return std::move(StringType::toString(getValPtrPlain(index),
                                          getElementWidthPlain(index)));
  }

  const char *read(uint64_t index, uint64_t *len, bool *null) const override {
    return base_type::read(index, len, null);
  }

  uint64_t getMaxLenModifier() const {
    return TypeModifierUtil::getMaxLen(typeModifier);
  }

 protected:
  explicit BytesVector(bool ownData)
      : VariableSizeTypeVector<char, BytesType>(ownData) {}
};

class BlankPaddedCharVector : public BytesVector {
 public:
  explicit BlankPaddedCharVector(bool ownData, int64_t typeMod)
      : BytesVector(ownData) {
    setTypeKind(CHARID);
    typeModifier = typeMod;
    if (ownData)
      values.reserve(TypeModifierUtil::getMaxLen(typeModifier) *
                     DEFAULT_NUMBER_TUPLES_PER_BATCH);
  }
  std::string readPlain(uint64_t index, bool *null) const override;
  void trim() override;
  void setMaxLenModifier(uint64_t maxLenModifier) {
    typeModifier =
        TypeModifierUtil::getTypeModifierFromMaxLength(maxLenModifier);
  }

 private:
  bool isTrimmed = false;
};

class VaryingCharVector : public BytesVector {
 public:
  explicit VaryingCharVector(bool ownData, int64_t typeMod)
      : BytesVector(ownData) {
    setTypeKind(VARCHARID);
    typeModifier = typeMod;
    if (ownData)
      values.reserve(TypeModifierUtil::getMaxLen(typeModifier) *
                     DEFAULT_NUMBER_TUPLES_PER_BATCH);
  }
};

class StringVector : public BytesVector {
 public:
  explicit StringVector(bool ownData) : BytesVector(ownData) {
    setTypeKind(STRINGID);
  }
};

class BinaryVector : public BytesVector {
 public:
  explicit BinaryVector(bool ownData) : BytesVector(ownData) {
    setTypeKind(BINARYID);
  }
  void append(const std::string &strValue, bool null) override;
  std::string readPlain(uint64_t index, bool *null) const override;
};

class NumericVector : public VariableSizeTypeVector<char, NumericType> {
 public:
  typedef VariableSizeTypeVector<char, NumericType> base_type;

  explicit NumericVector(bool ownData)
      : VariableSizeTypeVector<char, NumericType>(ownData) {
    setTypeKind(DECIMALID);
  }

  std::string readPlain(uint64_t index, bool *null) const override {
    *null = isNullPlain(index);
    if (*null) return "NULL";
    return std::move(NumericType::toString(getValPtrPlain(index),
                                           getElementWidthPlain(index)));
  }

  void hash(std::vector<uint64_t> &retval) const override {
    const uint64_t *lens = reinterpret_cast<const uint64_t *>(lengths.data());

    if (selected) {
      if (hasNulls) {
        const char **valPs = reinterpret_cast<const char **>(valPtrs.data());
        for (uint64_t i = 0; i < retval.size(); ++i) {
          uint64_t idx = (*selected)[i];
          retval[i] =
              nulls.get(idx) ? 0 : Murmur3::hash64(valPs[idx], lens[idx]);
        }
      } else {
        const char **valPs = reinterpret_cast<const char **>(valPtrs.data());
        for (uint64_t i = 0; i < retval.size(); ++i) {
          uint64_t idx = (*selected)[i];
          retval[i] = Murmur3::hash64(valPs[idx], lens[idx]);
        }
      }
    } else {
      if (isDirectEncoding && !hasNulls && getOwnData()) {
        const char **valPs = reinterpret_cast<const char **>(valPtrs.data());
        const char *valP = valPs[0];
        for (uint64_t i = 0; i < retval.size(); ++i) {
          retval[i] = Murmur3::hash64(valP, lens[i]);
          valP += lens[i];
        }
      } else {
        if (hasNulls) {
          const char **valPs = reinterpret_cast<const char **>(valPtrs.data());
          for (uint64_t i = 0; i < retval.size(); ++i) {
            retval[i] = nulls.get(i) ? 0 : Murmur3::hash64(valPs[i], lens[i]);
          }
        } else {
          const char **valPs = reinterpret_cast<const char **>(valPtrs.data());
          for (uint64_t i = 0; i < retval.size(); ++i) {
            retval[i] = Murmur3::hash64(valPs[i], lens[i]);
          }
        }
      }
    }
  }
};

class IOBaseTypeVector : public BytesVector {
 public:
  explicit IOBaseTypeVector(bool ownData) : BytesVector(ownData) {
    setTypeKind(IOBASETYPEID);
  }
};

class StructExTypeVector : public BytesVector {
 public:
  explicit StructExTypeVector(bool ownData) : BytesVector(ownData) {
    setTypeKind(STRUCTEXID);
  }
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_VARIABLE_LENGTH_VECTOR_H_
