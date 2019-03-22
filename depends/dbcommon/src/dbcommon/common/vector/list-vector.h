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

#ifndef DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_LIST_VECTOR_H_
#define DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_LIST_VECTOR_H_

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "dbcommon/common/vector.h"

namespace dbcommon {

class ListVector : public Vector {
 public:
  explicit ListVector(bool ownData) : Vector(ownData), offsets(ownData) {}

  virtual ~ListVector() {}

  uint64_t getNumOfRowsPlain() const override {
    return offsets.size() / sizeof(uint64_t) - 1;
  }

  void setValue(const char *value, uint64_t size) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "ListVector does not have value array");
  }

  const char *getValue() const {
    assert(childs.size() == 1);
    return childs[0]->getValue();
  }

  const uint64_t *getOffsets() const {
    return reinterpret_cast<const uint64_t *>(offsets.data());
  }

  void setOffsets(const uint64_t *offsetsPtr, uint64_t size) {
    assert(!getOwnData() && offsetsPtr != nullptr && size > 0);
    this->offsets.setData(reinterpret_cast<const char *>(offsetsPtr),
                          (size) * sizeof(uint64_t));
  }

  const char *read(uint64_t index, uint64_t *num, uint64_t *width,
                   bool *null) const {
    assert(childs.size() == 1);
    uint64_t currentOffset = const_cast<uint64_t *>(getOffsets())[index];
    uint64_t nextOffset = const_cast<uint64_t *>(getOffsets())[index + 1];
    const char *ret = childs[0]->read(currentOffset, width, null);
    *num = nextOffset - currentOffset;
    *null = isNullPlain(index);
    return ret;
  }

  const char *read(uint64_t index, std::string *nullBitmap, int32_t *dims,
                   bool *null) {
    assert(childs.size() == 1);
    uint64_t currentOffset = const_cast<uint64_t *>(getOffsets())[index];
    uint64_t nextOffset = const_cast<uint64_t *>(getOffsets())[index + 1];
    dims[0] = nextOffset - currentOffset;
    auto nulls = std::unique_ptr<char[]>{new char[(dims[0] + 7) / 8]};
    uint64_t width;
    nulls[0] = 0;
    bool isNull;
    int bitmask = 1;
    *null = isNullPlain(index);
    if (*null || dims[0] == 0) return NULL;
    const char *ret = childs[0]->read(currentOffset, &width, &isNull);
    nulls[0] = isNull ? 0 : bitmask;
    bitmask <<= 1;
    for (uint64_t i = currentOffset + 1; i < nextOffset; i++) {
      if (bitmask == 0x100) bitmask = 1;
      childs[0]->read(i, &width, &isNull);
      if (!isNull) nulls[(i - currentOffset) / 8] |= bitmask;
      bitmask <<= 1;
    }
    *nullBitmap = std::string(nulls.get());
    return ret;
  }

  const char *read(uint64_t index, uint64_t *len, bool *null) const override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "ListVector does not support read");
  }

  std::string readPlain(uint64_t index, bool *null) const override {
    assert(childs.size() == 1);
    std::string ret = "{";
    uint64_t currentOffset = const_cast<uint64_t *>(getOffsets())[index];
    uint64_t nextOffset = const_cast<uint64_t *>(getOffsets())[index + 1];
    for (int i = currentOffset; i < nextOffset; i++) {
      ret += childs[0]->readPlain(i, null);
      if (i != nextOffset - 1) {
        ret += ",";
      }
    }
    *null = isNullPlain(index);

    return ret + '}';
  }

  void readPlainScalar(uint64_t index, Scalar *scalar) const override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "ListVector does not support readPlainScalar");
  }

  void append(const std::string &strValue, bool null) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "ListVector does not support append");
  }

  virtual void append(const char *v, uint64_t valueLen,
                      unsigned char *nullBitmap, int32_t *dims, bool null,
                      bool forInsert) {}

  void append(const char *v, uint64_t valueLen, bool null) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "ListVector does not support append");
  }

  void append(const Datum &datum, bool null) override {
    assert(getOwnData());

    std::string buffer;

    if (!null) {
      buffer = DatumToBinary(datum, typeKind);
    }

    append(buffer.data(), buffer.size(), null);
  }

  void append(const Scalar *scalar) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "ListVector does not support append(Scalar) yet");
  }

  void append(Vector *src, uint64_t index) override {
    assert(childs.size() == 1);

    ListVector *lsrc = dynamic_cast<ListVector *>(src);
    uint64_t rows = getNumOfRowsPlain();
    uint64_t currentOffset = const_cast<uint64_t *>(lsrc->getOffsets())[index];
    uint64_t nextOffset = const_cast<uint64_t *>(lsrc->getOffsets())[index + 1];
    if (offsets.size() == 0) offsets.append(uint64_t(0));
    offsets.append(getOffsets()[rows] + nextOffset - currentOffset);
    for (uint64_t i = currentOffset; i < nextOffset; i++) {
      childs[0]->append(lsrc->getChildVector(0), i);
    }
    appendNull(src->isNullPlain(index));
  }

  void append(Vector *vect) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "ListVector does not support append yet");
  }

  void plainize() {
    if (selected) {
      auto cloned = clone();
      this->swap(*cloned);
    }
    selected = nullptr;
  }

  void materialize() override {
    plainize();
    offsets.materialize();

    for (int i = 0; i < childs.size(); i++) {
      childs[i]->materialize();
    }
  }

  std::unique_ptr<Vector> cloneSelected(const SelectList *sel) const override {
    assert(isValid());

    std::unique_ptr<ListVector> vec(new ListVector(true));
    vec->typeKind = typeKind;
    vec->setHasNull(hasNulls);
    if (!sel || sel->size() == offsets.size() / sizeof(uint64_t) - 1) {
      dbcommon::ByteBuffer offsetbuf(true);
      offsetbuf.append(offsets.data(), offsets.size());
      vec->offsets.swap(offsetbuf);
      if (hasNulls) vec->nulls.append(nulls.data(), nulls.size());
      vec->selectListOwner.reset();
      vec->selected = nullptr;
      vec->addChildVector(childs[0]->cloneSelected(nullptr));
    } else {
      SelectList csel;
      uint64_t size = sel->size();
      assert(size < offsets.size() / sizeof(uint64_t) - 1);
      dbcommon::ByteBuffer offsetbuf(true);

      offsetbuf.append(offsets.get<uint64_t>(0));
      if (hasNulls) {
        for (uint64_t i = 0; i < size; i++) {
          vec->nulls.append(nulls.get((*sel)[i]));
          uint64_t currentOffset = offsets.get<uint64_t>((*sel)[i]);
          uint64_t nextOffset = offsets.get<uint64_t>((*sel)[i] + 1);
          offsetbuf.append(offsetbuf.get<uint64_t>(i) + nextOffset -
                           currentOffset);
          for (uint64_t j = currentOffset; j < nextOffset; j++) {
            csel.push_back(j);
          }
        }
      } else {
        for (uint64_t i = 0; i < size; i++) {
          uint64_t currentOffset = offsets.get<uint64_t>((*sel)[i]);
          uint64_t nextOffset = offsets.get<uint64_t>((*sel)[i] + 1);
          offsetbuf.append(offsetbuf.get<uint64_t>(i) + nextOffset -
                           currentOffset);
          for (uint64_t j = currentOffset; j < nextOffset; j++) {
            csel.push_back(j);
          }
        }
      }

      vec->offsets.swap(offsetbuf);
      vec->selectListOwner.reset();
      vec->selected = nullptr;
      vec->addChildVector(childs[0]->cloneSelected(&csel));
    }
    return std::move(vec);
  }

  std::unique_ptr<Vector> extract(const SelectList *sel, uint64_t start,
                                  uint64_t end) const override {
    assert(isValid() && start < end);
    uint64_t total = getNumOfRowsPlain();
    assert(end <= total);
    uint64_t extSz = end - start;

    std::unique_ptr<ListVector> vec(new ListVector(false));
    vec->typeKind = typeKind;
    vec->setHasNull(hasNulls);
    if (offsets.data()) {
      uint64_t offLen = sizeof(uint64_t);
      vec->offsets.setData(offsets.data() + start * offLen, extSz * offLen);
    }

    if (hasNulls) vec->setNulls(getNulls() + start, extSz);
    vec->selected = nullptr;
    vec->addChildVector(childs[0]->extract(sel, start, end));

    return std::move(vec);
  }

  std::unique_ptr<Vector> replicateItemToVec(uint64_t index,
                                             int replicateNum) const override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "ListVector does not support replicateItemToVec yet");
  }

  void hash(std::vector<uint64_t> &retval) const override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "ListVector does not support hash yet");
  }

  void cdbHash(std::vector<uint64_t> &retval) const override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "ListVector does not support hash yet");
  }

  dbcommon::ByteBuffer *getValueBuffer() override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "ListVector does not support getValueBuffer yet");
  }

  bool isValid() const override {
    assert(childs.size() == 1);
    uint64_t rows = getNumOfRowsPlain();
    assert(offsets.size() == sizeof(uint64_t) * (rows + 1));
    if (hasNulls) assert(nulls.size() == rows);
    if (!childs[0]->isValid()) {
      return false;
    }
    return true;
  }

  void addChildVector(std::unique_ptr<Vector> vec) override {
    childs.push_back(std::move(vec));
  }

  void swap(Vector &vect) override {
    Vector::swap(vect);
    auto &v = reinterpret_cast<ListVector &>(vect);
    this->offsets.swap(v.offsets);
    this->childs.swap(v.childs);
  }

  uint64_t getSerializeSizeInternal() const override {
    size_t sSize = Vector::getSerializeSizeInternal();

    assert(getChildSize() == 1);
    uint64_t size = getNumOfRowsPlain();
    sSize += sizeof(uint64_t);
    sSize += sizeof(uint64_t) * (size + 1);
    sSize += childs[0]->getSerializeSize();

    return sSize;
  }

  void serializeInternal(NodeSerializer *serializer) override {
    Vector::serializeInternal(serializer);

    assert(getChildSize() == 1);
    uint64_t size = getNumOfRows();
    serializer->write<uint64_t>(size);

    if (allSelected()) {
      serializer->writeBytes(offsets.data(), sizeof(uint64_t) * (size + 1));
      getChildVector(0)->serialize(serializer);
    } else {
      // TODO(chiyang): no clone
      auto tmp = this->clone();
      serializer->writeBytes(
          reinterpret_cast<ListVector *>(tmp.get())->offsets.data(),
          sizeof(uint64_t) * (size + 1));
      tmp->getChildVector(0)->serialize(serializer);
    }
  }

  void deserializeInternal(NodeDeserializer *deserializer) override {
    Vector::deserializeInternal(deserializer);

    uint64_t size = deserializer->read<uint64_t>();
    const char *dstr = deserializer->readBytes(sizeof(uint64_t) * (size + 1));
    setOffsets(reinterpret_cast<const uint64_t *>(dstr), size + 1);

    std::unique_ptr<Vector> vect = Vector::deserialize(deserializer);
    this->addChildVector(std::move(vect));
  }

  void selectDistinct(SelectList *selected, Scalar *scalar) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "ListVector does not support selectDistinct yet");
  }

  void selectDistinct(SelectList *selected, Vector *vect) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "ListVector does not support selectDistinct yet");
  }

 protected:
  void resizeInternal(size_t plainSize) override {}

  dbcommon::ByteBuffer offsets;  // type: uint64_t
};

class SmallIntArrayVector : public ListVector {
 public:
  explicit SmallIntArrayVector(bool ownData) : ListVector(ownData) {
    setTypeKind(SMALLINTARRAYID);
  }

  void append(const char *v, uint64_t valueLen, unsigned char *nullBitmap,
              int32_t *dims, bool null, bool forInsert) override {
    assert(childs.size() == 1);
    uint64_t width = sizeof(int16_t);
    // Now we only support 1 dimension array
    int32_t nums = dims ? dims[0] : 0;
    if (!null) {
      if (nullBitmap) {
        int bitmask = 1;
        if (forInsert) {
          for (int32_t i = 0, j = 0; i < nums; i++) {
            if (bitmask == 0x100) bitmask = 1;
            if (nullBitmap[i / 8] & bitmask) {
              childs[0]->append(v + j * width, width, false);
              j++;
            } else {
              childs[0]->append(v + j * width, width, true);
            }
            bitmask <<= 1;
          }
        } else {
          for (int32_t i = 0; i < nums; i++) {
            if (bitmask == 0x100) bitmask = 1;
            if (nullBitmap[i / 8] & bitmask) {
              childs[0]->append(v + i * width, width, false);
            } else {
              childs[0]->append(v + i * width, width, true);
            }
            bitmask <<= 1;
          }
        }
      } else {
        for (int32_t i = 0; i < nums; i++)
          childs[0]->append(v + i * width, width, false);
      }
    }

    if (offsets.size() == 0) offsets.append(uint64_t(0));
    uint64_t rows = getNumOfRowsPlain();
    offsets.append(getOffsets()[rows] + nums);
    appendNull(null);
  }
};

class IntArrayVector : public ListVector {
 public:
  explicit IntArrayVector(bool ownData) : ListVector(ownData) {
    setTypeKind(INTARRAYID);
  }

  void append(const char *v, uint64_t valueLen, unsigned char *nullBitmap,
              int32_t *dims, bool null, bool forInsert) override {
    assert(childs.size() == 1);
    uint64_t width = sizeof(int32_t);
    // Now we only support 1 dimension array
    int32_t nums = dims ? dims[0] : 0;
    if (!null) {
      if (nullBitmap) {
        int bitmask = 1;
        if (forInsert) {
          for (int32_t i = 0, j = 0; i < nums; i++) {
            if (bitmask == 0x100) bitmask = 1;
            if (nullBitmap[i / 8] & bitmask) {
              childs[0]->append(v + j * width, width, false);
              j++;
            } else {
              childs[0]->append(v + j * width, width, true);
            }
            bitmask <<= 1;
          }
        } else {
          for (int32_t i = 0; i < nums; i++) {
            if (bitmask == 0x100) bitmask = 1;
            if (nullBitmap[i / 8] & bitmask) {
              childs[0]->append(v + i * width, width, false);
            } else {
              childs[0]->append(v + i * width, width, true);
            }
            bitmask <<= 1;
          }
        }
      } else {
        for (int32_t i = 0; i < nums; i++)
          childs[0]->append(v + i * width, width, false);
      }
    }

    if (offsets.size() == 0) offsets.append(uint64_t(0));
    uint64_t rows = getNumOfRowsPlain();
    offsets.append(getOffsets()[rows] + nums);
    appendNull(null);
  }
};

class BigIntArrayVector : public ListVector {
 public:
  explicit BigIntArrayVector(bool ownData) : ListVector(ownData) {
    setTypeKind(BIGINTARRAYID);
  }

  void append(const char *v, uint64_t valueLen, unsigned char *nullBitmap,
              int32_t *dims, bool null, bool forInsert) override {
    assert(childs.size() == 1);
    uint64_t width = sizeof(int64_t);
    // Now we only support 1 dimension array
    int32_t nums = dims ? dims[0] : 0;
    if (!null) {
      if (nullBitmap) {
        int bitmask = 1;
        if (forInsert) {
          for (int32_t i = 0, j = 0; i < nums; i++) {
            if (bitmask == 0x100) bitmask = 1;
            if (nullBitmap[i / 8] & bitmask) {
              childs[0]->append(v + j * width, width, false);
              j++;
            } else {
              childs[0]->append(v + j * width, width, true);
            }
            bitmask <<= 1;
          }
        } else {
          for (int32_t i = 0; i < nums; i++) {
            if (bitmask == 0x100) bitmask = 1;
            if (nullBitmap[i / 8] & bitmask) {
              childs[0]->append(v + i * width, width, false);
            } else {
              childs[0]->append(v + i * width, width, true);
            }
            bitmask <<= 1;
          }
        }
      } else {
        for (int32_t i = 0; i < nums; i++)
          childs[0]->append(v + i * width, width, false);
      }
    }

    if (offsets.size() == 0) offsets.append(uint64_t(0));
    uint64_t rows = getNumOfRowsPlain();
    offsets.append(getOffsets()[rows] + nums);
    appendNull(null);
  }
};

class FloatArrayVector : public ListVector {
 public:
  explicit FloatArrayVector(bool ownData) : ListVector(ownData) {
    setTypeKind(FLOATARRAYID);
  }

  void append(const char *v, uint64_t valueLen, unsigned char *nullBitmap,
              int32_t *dims, bool null, bool forInsert) override {
    assert(childs.size() == 1);
    uint64_t width = sizeof(float);
    // Now we only support 1 dimension array
    int32_t nums = dims ? dims[0] : 0;
    if (!null) {
      if (nullBitmap) {
        int bitmask = 1;
        if (forInsert) {
          for (int32_t i = 0, j = 0; i < nums; i++) {
            if (bitmask == 0x100) bitmask = 1;
            if (nullBitmap[i / 8] & bitmask) {
              childs[0]->append(v + j * width, width, false);
              j++;
            } else {
              childs[0]->append(v + j * width, width, true);
            }
            bitmask <<= 1;
          }
        } else {
          for (int32_t i = 0; i < nums; i++) {
            if (bitmask == 0x100) bitmask = 1;
            if (nullBitmap[i / 8] & bitmask) {
              childs[0]->append(v + i * width, width, false);
            } else {
              childs[0]->append(v + i * width, width, true);
            }
            bitmask <<= 1;
          }
        }
      } else {
        for (int32_t i = 0; i < nums; i++)
          childs[0]->append(v + i * width, width, false);
      }
    }

    if (offsets.size() == 0) offsets.append(uint64_t(0));
    uint64_t rows = getNumOfRowsPlain();
    offsets.append(getOffsets()[rows] + nums);
    appendNull(null);
  }
};

class DoubleArrayVector : public ListVector {
 public:
  explicit DoubleArrayVector(bool ownData) : ListVector(ownData) {
    setTypeKind(DOUBLEARRAYID);
  }

  void append(const char *v, uint64_t valueLen, unsigned char *nullBitmap,
              int32_t *dims, bool null, bool forInsert) override {
    assert(childs.size() == 1);
    uint64_t width = sizeof(double);
    // Now we only support 1 dimension array
    int32_t nums = dims ? dims[0] : 0;
    if (!null) {
      if (nullBitmap) {
        int bitmask = 1;
        if (forInsert) {
          for (int32_t i = 0, j = 0; i < nums; i++) {
            if (bitmask == 0x100) bitmask = 1;
            if (nullBitmap[i / 8] & bitmask) {
              childs[0]->append(v + j * width, width, false);
              j++;
            } else {
              childs[0]->append(v + j * width, width, true);
            }
            bitmask <<= 1;
          }
        } else {
          for (int32_t i = 0; i < nums; i++) {
            if (bitmask == 0x100) bitmask = 1;
            if (nullBitmap[i / 8] & bitmask) {
              childs[0]->append(v + i * width, width, false);
            } else {
              childs[0]->append(v + i * width, width, true);
            }
            bitmask <<= 1;
          }
        }
      } else {
        for (int32_t i = 0; i < nums; i++)
          childs[0]->append(v + i * width, width, false);
      }
    }

    if (offsets.size() == 0) offsets.append(uint64_t(0));
    uint64_t rows = getNumOfRowsPlain();
    offsets.append(getOffsets()[rows] + nums);
    appendNull(null);
  }
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_LIST_VECTOR_H_
