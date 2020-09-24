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

#ifndef DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_STRUCT_VECTOR_H_
#define DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_STRUCT_VECTOR_H_

#include <string>
#include <utility>
#include <vector>

#include "dbcommon/common/vector.h"

namespace dbcommon {
class StructVector : public Vector {
 public:
  explicit StructVector(bool ownData) : Vector(ownData) {
    setTypeKind(STRUCTID);
    setHasNull(false);
  }

  virtual ~StructVector() {}

  uint64_t getNumOfRowsPlain() const override {
    if (childs.size() > 0) {
      return childs[0]->getNumOfRowsPlain();
    }
    return 0;
  }

  void setValue(const char *value, uint64_t size) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "StructVector does not have value array");
  }

  const char *read(uint64_t index, uint64_t *len, bool *null) const override {
    if (childs.size() > 0) {
      return childs[0]->read(index, len, null);
    }
    return nullptr;
  }

  std::string readPlain(uint64_t index, bool *null) const override {
    std::string ret = "";
    for (int i = 0; i < childs.size(); i++) {
      ret += childs[i]->readPlain(index, null);
      if (i != childs.size() - 1) {
        ret += ",";
      }
    }
    return ret;
  }

  void readPlainScalar(uint64_t index, Scalar *scalar) const override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "StructVector does not support readPlainScalar");
  }

  void append(const std::string &strValue, bool null) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "StructVector does not support append yet");
  }

  void append(const char *v, uint64_t valueLen, bool null) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "StructVector does not support append yet");
  }

  void append(const Datum &datum, bool null) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "StructVector does not support append yet");
  }

  void append(const Scalar *scalar) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "StructVector does not support append yet");
  }

  void append(Vector *src, uint64_t index) override {
    for (int i = 0; i < childs.size(); i++) {
      childs[i]->append(src->getChildVector(i), index);
    }
  }

  void append(Vector *vect) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "StructVector does not support append yet");
  }

  void append(Vector *vect, SelectList *sel) override {
    Vector::append(vect, sel);
    assert(childs.empty() || childs.size() == vect->getChildSize());
    if (childs.empty()) {
      for (auto i = 0; i < vect->getChildSize(); i++)
        childs.push_back(
            Vector::BuildVector(vect->getChildVector(i)->getTypeKind(), true,
                                vect->getChildVector(i)->getTypeModifier()));
    }
    for (int i = 0; i < childs.size(); i++) {
      childs[i]->append(vect->getChildVector(i), sel);
    }
  }

  void reset(bool resetBelongingTupleBatch = false) override {
    for (int i = 0; i < childs.size(); i++) {
      childs[i]->reset(resetBelongingTupleBatch);
    }
  }

  void materialize() override {
    for (int i = 0; i < childs.size(); i++) {
      childs[i]->materialize();
    }
  }

  std::unique_ptr<Vector> cloneSelected(const SelectList *sel) const override {
    std::unique_ptr<Vector> vec(new StructVector(false));
    vec->setTypeKind(this->getTypeKind());
    for (int i = 0; i < childs.size(); i++) {
      vec->addChildVector(childs[i]->cloneSelected(sel));
    }
    return vec;
  }

  std::unique_ptr<Vector> extract(const SelectList *sel, uint64_t start,
                                  uint64_t end) const override {
    std::unique_ptr<Vector> vec(new StructVector(false));
    for (int i = 0; i < childs.size(); i++) {
      vec->addChildVector(childs[i]->extract(sel, start, end));
    }
    return vec;
  }

  std::unique_ptr<Vector> replicateItemToVec(uint64_t index,
                                             int replicateNum) const override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "StructVector does not support replicateItemToVec yet");
  }

  void hash(std::vector<uint64_t> &retval) const override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "StructVector does not support hash yet");
  }

  void cdbHash(std::vector<uint64_t> &retval) const override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "StructVector does not support hash yet");
  }

  dbcommon::ByteBuffer *getValueBuffer() override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "StructVector does not support getValueBuffer yet");
  }

  bool isValid() const override {
    for (int i = 0; i < childs.size(); i++) {
      if (!childs[i]->isValid()) {
        return false;
      }
    }
    return true;
  }

  uint64_t getSerializeSizeInternal() const override {
    size_t sSize = Vector::getSerializeSizeInternal();

    sSize += sizeof(uint64_t);
    uint64_t nChild = childs.size();
    for (int i = 0; i < nChild; i++) {
      sSize += childs[i]->getSerializeSize();
    }

    return sSize;
  }

  void serializeInternal(NodeSerializer *serializer) override {
    Vector::serializeInternal(serializer);

    uint64_t nChild = getChildSize();
    serializer->write<uint64_t>(nChild);

    for (int i = 0; i < nChild; i++) {
      getChildVector(i)->serialize(serializer);
    }
  }

  void deserializeInternal(NodeDeserializer *deserializer) override {
    Vector::deserializeInternal(deserializer);

    uint64_t nChild = deserializer->read<uint64_t>();

    for (int i = 0; i < nChild; i++) {
      std::unique_ptr<Vector> vect = Vector::deserialize(deserializer);
      this->addChildVector(std::move(vect));
    }
  }

  void selectDistinct(SelectList *selected, Scalar *scalar) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "StructVector does not support selectDistinct yet");
  }

  void selectDistinct(SelectList *selected, Vector *vect) override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "StructVector does not support selectDistinct yet");
  }

 protected:
  void resizeInternal(size_t plainSize) override {}
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_STRUCT_VECTOR_H_
