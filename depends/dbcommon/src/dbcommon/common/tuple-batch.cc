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

#include "dbcommon/common/tuple-batch.h"

#include <algorithm>
#include <cassert>

#include "dbcommon/common/vector/decimal-vector.h"
#include "dbcommon/common/vector/fixed-length-vector.h"

namespace dbcommon {

TupleBatch::TupleBatch(bool hasSysCol) {
  if (hasSysCol) {
    addSysColumn(dbcommon::TypeKind::MAGMATID, -1, true);
  }
}

TupleBatch::TupleBatch(uint32_t nfield, bool hasSysCol)
    : vectorRefs(nfield), vectors(nfield) {
  if (hasSysCol) {
    addSysColumn(dbcommon::TypeKind::MAGMATID, -1, true);
  }
}

TupleBatch::TupleBatch(int32_t nfield, bool hasSysCol)
    : vectorRefs(nfield), vectors(nfield) {
  if (hasSysCol) {
    addSysColumn(dbcommon::TypeKind::MAGMATID, -1, true);
  }
}

TupleBatch::TupleBatch(size_t nfield, bool hasSysCol)
    : vectorRefs(nfield), vectors(nfield) {
  if (hasSysCol) {
    addSysColumn(dbcommon::TypeKind::MAGMATID, -1, true);
  }
}

TupleBatch::TupleBatch(const TupleDesc &desc, bool ownData, bool hasSysCol) {
  rebuild(desc, ownData, hasSysCol);
}

void TupleBatch::rebuild(const TupleDesc &desc, bool ownData, bool hasSysCol) {
  // add user vectors
  size_t attNum = desc.getNumOfColumns();
  for (size_t i = 0; i < attNum; i++) {
    std::unique_ptr<Vector> vect = Vector::BuildVector(
        desc.getColumnType(i), ownData, desc.getColumnTypeModifier(i));
    std::unique_ptr<Vector> child;
    switch (desc.getColumnType(i)) {
      case SMALLINTARRAYID:
        child = std::unique_ptr<Vector>(new SmallIntVector(ownData));
        vect->addChildVector(std::move(child));
        break;
      case INTARRAYID:
        child = std::unique_ptr<Vector>(new IntVector(ownData));
        vect->addChildVector(std::move(child));
        break;
      case BIGINTARRAYID:
        child = std::unique_ptr<Vector>(new BigIntVector(ownData));
        vect->addChildVector(std::move(child));
        break;
      case FLOATARRAYID:
        child = std::unique_ptr<Vector>(new FloatVector(ownData));
        vect->addChildVector(std::move(child));
        break;
      case DOUBLEARRAYID:
        child = std::unique_ptr<Vector>(new DoubleVector(ownData));
        vect->addChildVector(std::move(child));
        break;
      default:
        break;
    }
    vect->setParentTupleBatch(this);
    vectorRefs.push_back(vect.get());
    vectors.push_back(std::move(vect));
  }

  // add system vectors
  if (hasSysCol) {
    addSysColumn(dbcommon::TypeKind::MAGMATID, -1, true);
  }
}

std::string TupleBatch::toString() const {
  const TupleBatchReader &reader = this->getTupleBatchReader();
  const TupleBatchReader &sysReader = this->getTupleBatchSysReader();
  std::stringstream ss;

  if (isScalarTupleBatch_) {
    for (size_t i = 0; i < scalars_.size(); i++) {
      // XXX(FIXME): Not support to print data types other than integers
      std::string tmp;
      if (scalars_[i].length == 0)
        tmp = std::to_string(DatumGetValue<int64_t>(scalars_[i].value));
      else
        tmp.append(DatumGetValue<char *>(scalars_[i].value),
                   scalars_[i].length);

      if (!scalars_[i].isnull) ss << tmp;
      if (i < scalars_.size() - 1) ss << ",";
    }
    ss << "\n";
    return ss.str();
  }

  for (size_t r = 0; r < this->getNumOfRows(); r++) {
    for (size_t f = 0; f < this->getNumOfSysColumns(); f++) {
      bool isNull;
      if (reader[f] != nullptr) {
        std::string tmp = sysReader[f]->read(r, &isNull);
        if (!isNull) ss << tmp;
      }
      if (f <= this->getNumOfColumns() - 1) {
        ss << ",";
      }
    }
    for (size_t f = 0; f < this->getNumOfColumns(); f++) {
      bool isNull;
      if (reader[f] != nullptr) {
        std::string tmp = reader[f]->read(r, &isNull);
        if (!isNull) ss << tmp;
      }
      if (f < this->getNumOfColumns() - 1) {
        ss << ",";
      }
    }
    ss << "\n";
  }
  return ss.str();
}

int64_t TupleBatch::getTupleBatchSize() const {
  int64_t size = 0;
  for (uint32_t i = 0; i < vectorRefs.size(); i++) {
    // std::unique_ptr<Vector> vec = vectors.at(i);
    if (vectorRefs[i]) {
      size += vectorRefs.at(i)->getMemorySizePlain();
    }
  }
  return size;
}

void TupleBatch::addColumn(TypeKind dataType, int64_t typeMod, bool ownData) {
  std::unique_ptr<Vector> vect =
      Vector::BuildVector(dataType, ownData, typeMod);
  vect->setParentTupleBatch(this);
  vectorRefs.push_back(vect.get());
  vectors.push_back(std::move(vect));
}

void TupleBatch::addSysColumn(TypeKind dataType, int64_t typeMod,
                              bool ownData) {
  std::unique_ptr<Vector> vect =
      Vector::BuildVector(dataType, ownData, typeMod);
  vect->setParentTupleBatch(this);
  sysVectorRefs.push_back(vect.get());
  sysVectors.push_back(std::move(vect));
}

void TupleBatch::setColumn(int32_t index, std::unique_ptr<Vector> vect,
                           bool useParentSelected) {
  if (index >= 0) {
    assert(index < vectors.size() && "index out of range");
    if (vect) vect->setParentTupleBatch(this);
    vectorRefs[index] = (vect.get());
    vectors[index].swap(vect);

    if (useParentSelected) {
      if (vectors[index])
        vectors[index]->setSelected(this->getSelected(), true);
    }
  } else {
    index = -index - 1;
    assert(index < sysVectors.size() && "index out of range");
    if (vect) vect->setParentTupleBatch(this);
    sysVectorRefs[index] = vect.get();
    sysVectors[index].swap(vect);

    if (useParentSelected) {
      if (sysVectors[index])
        sysVectors[index]->setSelected(this->getSelected(), true);
    }
  }
}

void TupleBatch::setColumn(int32_t index, Vector *vect,
                           bool useParentSelected) {
  if (index >= 0) {
    assert(index < vectors.size() && "index out of range");
    if (vect) vect->setParentTupleBatch(this);
    vectorRefs[index] = vect;
    vectors[index].reset(nullptr);

    if (useParentSelected) {
      if (vectors[index])
        vectors[index]->setSelected(this->getSelected(), true);
    }
  } else {
    index = -index - 1;
    assert(index < sysVectors.size() && "index out of range");
    if (vect) vect->setParentTupleBatch(this);
    sysVectorRefs[index] = vect;
    sysVectors[index].reset(nullptr);

    if (useParentSelected) {
      if (sysVectors[index])
        sysVectors[index]->setSelected(this->getSelected(), true);
    }
  }
}

void TupleBatch::insertColumn(int32_t index, TypeKind dataType, int64_t typeMod,
                              bool ownData) {
  auto vect = Vector::BuildVector(dataType, ownData, typeMod);

  if (index >= 0) {
    assert(index <= vectors.size() && "index out of range");
    if (vect) vect->setParentTupleBatch(this);

    vectorRefs.insert(vectorRefs.begin() + index, vect.get());
    vectors.insert(vectors.begin() + index, std::move(vect));
  } else {
    index = -index - 1;
    assert(index <= sysVectors.size() && "index out of range");

    if (vect) vect->setParentTupleBatch(this);

    sysVectorRefs.insert(sysVectorRefs.begin() + index, vect.get());
    sysVectors.insert(sysVectors.begin() + index, std::move(vect));
  }
}

void TupleBatch::insertColumn(int32_t index, std::unique_ptr<Vector> vect,
                              bool useParentSelected) {
  if (index >= 0) {
    assert(index <= vectors.size() && "index out of range");
    if (vect) vect->setParentTupleBatch(this);

    vectorRefs.insert(vectorRefs.begin() + index, vect.get());
    vectors.insert(vectors.begin() + index, std::move(vect));

    if (useParentSelected) {
      if (vectors[index])
        vectors[index]->setSelected(this->getSelected(), true);
    }

  } else {
    index = -index - 1;
    assert(index <= sysVectors.size() && "index out of range");

    if (vect) vect->setParentTupleBatch(this);

    sysVectorRefs.insert(sysVectorRefs.begin() + index, vect.get());
    sysVectors.insert(sysVectors.begin() + index, std::move(vect));

    if (useParentSelected) {
      if (sysVectors[index])
        sysVectors[index]->setSelected(this->getSelected(), true);
    }
  }
}

void TupleBatch::setNull(uint32_t index) {
  for (auto &col : vectors) {
    if (col != nullptr) col->setNull(index);
  }
  for (auto &col : sysVectors) {
    if (col != nullptr) col->setNull(index);
  }
}

void TupleBatch::appendNull(uint32_t tupleCount) {
  for (auto &col : vectors) {
    if (col != nullptr) {
      for (uint32_t i = 0; i < tupleCount; ++i) col->append("", true);
    }
  }
  for (auto &col : sysVectors) {
    if (col != nullptr) {
      for (uint32_t i = 0; i < tupleCount; ++i) col->append("", true);
    }
  }
  this->numOfRowsPlain += tupleCount;
}

void TupleBatch::materialize() {
  for (auto &col : vectors) {
    if (col != nullptr) {
      col->materialize();
    }
  }
  for (auto &col : sysVectors) {
    if (col != nullptr) {
      col->materialize();
    }
  }
  if (selected_) {
    this->setNumOfRows(selected_->size());
    selected_ = nullptr;
    selectListOwner_.reset();
  }
}

std::unique_ptr<TupleBatch> TupleBatch::cloneSelected(
    const SelectList *sel) const {
  int numCols = getNumOfColumns();
  int numSysCols = getNumOfSysColumns();
  auto retval =
      std::unique_ptr<TupleBatch>(new TupleBatch(numCols, numSysCols > 0));
  for (int i = 0; i < numCols; i++) {
    if (vectorRefs[i])
      retval->setColumn(i, std::move(vectorRefs[i]->cloneSelected(sel)), false);
  }
  for (int i = 0; i < numSysCols; i++) {
    retval->setColumn((-i - 1), std::move(sysVectorRefs[i]->cloneSelected(sel)),
                      false);
  }
  if (sel && sel->size() > 0)
    retval->setNumOfRows(sel->size());
  else
    retval->setNumOfRows(this->getNumOfRows());

  return std::move(retval);
}

void TupleBatch::setSelected(const SelectList &selected) {
  if (!selectListOwner_) {
    selectListOwner_.reset(new SelectList);
    selected_ = selectListOwner_.get();
  }
  *this->selected_ = selected;
  for (auto &col : vectors) {
    if (col) {
      col->setSelected(selected_, true);
    }
  }
  for (auto &col : sysVectors) {
    if (col) {
      col->setSelected(selected_, true);
    }
  }
}

void TupleBatch::setSelected(SelectList *selected) {
  if (selected == selected_) return;
  selectListOwner_.reset();
  selected_ = selected;
  for (auto &col : sysVectors) {
    if (col) col->setSelected(selected_, true);
  }
  for (auto &col : vectors) {
    if (col) col->setSelected(selected_, true);
  }
}

void TupleBatch::unsetSelected() {
  if (!selected_) return;
  selected_ = nullptr;
  selectListOwner_.reset();
  for (auto &col : vectors) {
    if (col) {
      col->setSelected(nullptr, true);
    }
  }
  for (auto &col : sysVectors) {
    if (col) {
      col->setSelected(nullptr, true);
    }
  }
}

void TupleBatch::reset() {
  for (auto &vect : vectors) {
    assert(vect != nullptr);
    vect->reset();
  }
  vectorRefs.resize(vectors.size(), nullptr);

  for (auto &vect : sysVectors) {
    assert(vect != nullptr);
    vect->reset();
  }
  sysVectorRefs.resize(sysVectors.size(), nullptr);

  this->numOfRowsPlain = 0;
  this->selected_ = nullptr;
  this->selectListOwner_.reset();
}

void TupleBatch::append(TupleBatch *tupleBatch, SelectList *sel) {
  if (tupleBatch->isScalarTupleBatch_) {
    for (auto i = 0; i < getNumOfColumns(); i++)
      vectors[i]->append(&tupleBatch->scalars_[i]);
    return;
  }
  if (getNumOfRows() > 0) this->materialize();
  int numCols = getNumOfColumns();
  int numSysCols = getNumOfSysColumns();
  if (this->selected_) {
    int oldSize = this->getNumOfRowsPlain();
    int appendSize = (sel ? sel->size() : tupleBatch->getNumOfRowsPlain());
    for (size_t i = 0; i < appendSize; i++) {
      this->selected_->push_back(oldSize++);
    }
  }
  for (int i = -numSysCols; i < numCols; i++) {
    if (getColumn(i) == nullptr) {
      continue;
    }
    getColumn(i)->append(tupleBatch->getColumn(i), sel);
  }
  this->numOfRowsPlain += sel ? sel->size() : tupleBatch->getNumOfRows();
}

void TupleBatch::append(const TupleBatch &tb, uint32_t index) {
  size_t numColumns = getNumOfColumns();
  size_t numSysColumns = getNumOfSysColumns();
  if (this->selected_) {
    int oldSize = selected_->size();
    selected_->push_back(oldSize);
  }
  for (int i = -numSysColumns; i < numColumns; i++) {
    Vector *vect = getColumn(i);
    Vector *src = tb.getColumn(i);

    uint32_t len;
    bool null;
    vect->append(src, index);
  }
  this->numOfRowsPlain += 1;
}

dbcommon::TupleBatchWriter &TupleBatch::getTupleBatchWriter() {
  assert(vectors.size() > 0);
  return vectors;
}

dbcommon::TupleBatchWriter &TupleBatch::getTupleBatchSysWriter() {
  assert(sysVectors.size() > 0);
  return sysVectors;
}

bool TupleBatch::isValid() const {
  for (auto &vect : vectors) {
    assert(!vect || vect->isValid());
    assert(!vect || vect->getSelected() == getSelected());
    assert(!vect || vect->getNumOfRowsPlain() == getNumOfRowsPlain());
  }
  for (auto &vect : sysVectors) {
    assert(!vect || vect->isValid());
    assert(!vect || vect->getSelected() == getSelected());
    assert(!vect || vect->getNumOfRowsPlain() == getNumOfRowsPlain());
  }
  return true;
}

void TupleBatch::swapColumn(uint32_t index, std::unique_ptr<Vector> *vec) {
  assert(index < vectors.size() && "index out of range");
  vectorRefs[index] = vec->get();
  vectors[index].swap(*vec);
  if (vectors[index] != nullptr) {
    vectors[index]->setParentTupleBatch(this);
  }
}

void TupleBatch::swapSysColumn(int32_t index, std::unique_ptr<Vector> *vec) {
  uint32_t sysIdx = -index - 1;
  assert(sysIdx < sysVectors.size() && "index out of range");
  sysVectorRefs[sysIdx] = vec->get();
  sysVectors[sysIdx].swap(*vec);
  if (sysVectors[sysIdx] != nullptr) {
    sysVectors[sysIdx]->setParentTupleBatch(this);
  }
}

std::unique_ptr<TupleBatch> TupleBatch::replicateRowsToTB(
    uint32_t rowIndex, int replicateNum) const {
  TupleBatch::uptr retval(
      new TupleBatch(getNumOfColumns(), getNumOfSysColumns() > 0));
  int startIndex = getNumOfSysColumns() * -1;
  int endIndex = getNumOfColumns();
  for (int i = startIndex; i < endIndex; ++i) {
    if (i < 0) {
      if (sysVectors[i * -1 - 1] == nullptr) {
        retval->setColumn(i, nullptr, false);
      } else {
        retval->setColumn(
            i,
            sysVectors[i * -1 - 1]->replicateItemToVec(rowIndex, replicateNum),
            false);
      }
    } else {
      if (vectors[i] == nullptr) {
        retval->setColumn(i, nullptr, false);
      } else {
        retval->setColumn(
            i, vectors[i]->replicateItemToVec(rowIndex, replicateNum), false);
      }
    }
  }
  retval->setNumOfRows(replicateNum);
  return std::move(retval);
}

std::unique_ptr<TupleBatch> TupleBatch::getRow(uint32_t rowIndex) const {
  TupleBatch::uptr retval(new TupleBatch);
  retval->isScalarTupleBatch_ = true;
  retval->scalars_.resize(getNumOfColumns());
  retval->numOfRowsPlain = 1;
  for (auto i = 0; i < getNumOfColumns(); i++)
    vectorRefs[i]->readPlainScalar(rowIndex, &(retval->scalars_[i]));
  return retval;
}

void TupleBatch::concatTB(std::unique_ptr<TupleBatch> other) {
  assert(other != nullptr);
  // for targetList is Null
  if (other->getNumOfColumns() == 0) return;

  assert(isScalarTupleBatch_ || getNumOfRows() == other->getNumOfRows());
  assert(other->getNumOfSysColumns() == 0);

  if (other->selected_) {
    assert(this->selected_ == other->selected_ ||
           *this->selected_ == *other->selected_);
  }

  TupleBatchWriter &writer = other->getTupleBatchWriter();
  for (size_t i = 0; i < other->getNumOfColumns(); ++i) {
    std::unique_ptr<Vector> vect(writer[i].release());
    vect->setParentTupleBatch(this);
    vectorRefs.push_back(vect.get());
    vectors.push_back(std::move(vect));
  }
}

std::unique_ptr<TupleBatch> TupleBatch::getTuple(uint32_t index) const {
  return replicateRowsToTB(index, 1);
}

size_t TupleBatch::serialize(std::string *output, size_t reserved) {
  std::unique_ptr<NodeSerializer> serializer(new NodeSerializer(output));
  int colNum = getNumOfColumns();
  int sysColNum = getNumOfSysColumns();

  if (this->empty())
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "Should not serialize an empty tuple batch");

  assert(this->isValid());

  std::string colBitMasks;
  buildColBitMasks(&colBitMasks);

  // caculate serializer size previously
  uint64_t sSize =
      colBitMasks.size() +  // column vector availability mask
      sizeof(uint32_t) * 4 +
      sizeof(uint16_t);  // num of row, row kv, user & sys col, range id

  for (int i = -sysColNum; i < colNum; i++) {
    Vector *vect = getColumn(i);
    sSize += (vect == nullptr ? 0 : vect->getSerializeSize());
  }
  if (output->size() + sSize > reserved) {
    reserved = output->size() + sSize;
    output->reserve(reserved);
  }

  // write out number of rows, user columns, and system columns, range id
  serializer->write<uint16_t>(static_cast<uint16_t>(rgId));
  serializer->write<uint32_t>(getNumOfRows());
  serializer->write<uint32_t>(colNum);
  serializer->write<uint32_t>(sysColNum);
  serializer->writeBytes(colBitMasks.data(), colBitMasks.size());
  // directly serialize the content of each user column and system column
  for (int i = -sysColNum; i < colNum; i++) {
    Vector *vect = getColumn(i);
    if (vect != nullptr) {
      vect->serialize(serializer.get());
    }
  }

  return reserved;
}

void TupleBatch::deserialize(const std::string &data, int32_t startPos,
                             uint32_t end) {
  assert(startPos < end && end <= data.size());

  int colNum = 0;
  int sysColNum = 0;
  if (end < startPos + 8) {
    LOG_DEBUG("deserialize empty string to empty tuplebatch");
    numOfRowsPlain = 0;
    vectors.resize(0);
    return;
  }
  int32_t cursor = startPos;
  while (cursor < end) {
    std::unique_ptr<NodeDeserializer> deserializer(new NodeDeserializer(data));
    deserializer->setCursor(cursor);

    if (this->empty()){
      rgId = deserializer->read<uint16_t>();
      numOfRowsPlain = deserializer->read<uint32_t>();
      colNum = deserializer->read<uint32_t>();
      sysColNum = deserializer->read<uint32_t>();
      vectorRefs.resize(colNum);
      vectors.resize(colNum);
      sysVectorRefs.resize(sysColNum);
      sysVectors.resize(sysColNum);

      // deserialize col mask bits
      uint32_t sizeOfColMaskBits = calculateSizeOfMaskBits(colNum, sysColNum);
      const char *colMaskBits = deserializer->readBytes(sizeOfColMaskBits);

      for (int32_t i = -sysColNum; i < colNum; i++) {
        if (isColMaskUnavailable(colMaskBits, sysColNum, i)) {
          setColumn(i, nullptr, false);
          continue;
        }
        std::unique_ptr<Vector> vect = Vector::deserialize(deserializer.get());
        setColumn(i, std::move(vect), false);
      }

      // we should keep materialized tuplebatch after deserialize
      this->materialize();
    } else {
      std::unique_ptr<TupleBatch> tb(new TupleBatch());
      tb->rgId = deserializer->read<uint16_t>();
      tb->numOfRowsPlain = deserializer->read<uint32_t>();
      colNum = deserializer->read<uint32_t>();
      sysColNum = deserializer->read<uint32_t>();
      tb->vectorRefs.resize(colNum);
      tb->vectors.resize(colNum);
      tb->sysVectorRefs.resize(sysColNum);
      tb->sysVectors.resize(sysColNum);

      // deserialize col mask bits
      int sizeOfColMaskBits = calculateSizeOfMaskBits(colNum, sysColNum);
      const char *colMaskBits = deserializer->readBytes(sizeOfColMaskBits);

      for (int32_t i = -sysColNum; i < colNum; i++) {
        if (isColMaskUnavailable(colMaskBits, sysColNum, i)) {
          setColumn(i, nullptr, false);
          continue;
        }
        std::unique_ptr<Vector> vect = Vector::deserialize(deserializer.get());
        tb->setColumn(i, std::move(vect), false);
      }

      this->append(tb.get());
    }

    assert(this->isValid());
    cursor = deserializer->getCursor();
  }
}

void TupleBatch::deserialize(const std::string &data) {
  int colNum = 0;
  int sysColNum = 0;
  if (data.length() < 8) {
    LOG_DEBUG("deserialize empty string to empty tuplebatch");
    numOfRowsPlain = 0;
    vectors.resize(0);
    return;
  }
  int32_t cursor = 0;
  while (cursor < data.length()) {
    std::unique_ptr<NodeDeserializer> deserializer(new NodeDeserializer(data));
    deserializer->setCursor(cursor);

    if (this->empty()){
      rgId = deserializer->read<uint16_t>();
      numOfRowsPlain = deserializer->read<uint32_t>();
      colNum = deserializer->read<uint32_t>();
      sysColNum = deserializer->read<uint32_t>();
      vectorRefs.resize(colNum);
      vectors.resize(colNum);
      sysVectorRefs.resize(sysColNum);
      sysVectors.resize(sysColNum);

      // deserialize col mask bits
      int sizeOfColMaskBits = calculateSizeOfMaskBits(colNum, sysColNum);
      const char *colMaskBits = deserializer->readBytes(sizeOfColMaskBits);

      for (int32_t i = -sysColNum; i < colNum; i++) {
        if (isColMaskUnavailable(colMaskBits, sysColNum, i)) {
          setColumn(i, nullptr, false);
          continue;
        }
        std::unique_ptr<Vector> vect = Vector::deserialize(deserializer.get());
        setColumn(i, std::move(vect), false);
      }

      // we should keep materialized tuplebatch after deserialize
      this->materialize();
    } else {
      std::unique_ptr<TupleBatch> tb(new TupleBatch());
      tb->rgId = deserializer->read<uint16_t>();
      tb->numOfRowsPlain = deserializer->read<uint32_t>();
      colNum = deserializer->read<uint32_t>();
      sysColNum = deserializer->read<uint32_t>();
      tb->vectorRefs.resize(colNum);
      tb->vectors.resize(colNum);
      tb->sysVectorRefs.resize(sysColNum);
      tb->sysVectors.resize(sysColNum);

      // deserialize col mask bits
      int sizeOfColMaskBits = calculateSizeOfMaskBits(colNum, sysColNum);
      const char *colMaskBits = deserializer->readBytes(sizeOfColMaskBits);

      for (int32_t i = -sysColNum; i < colNum; i++) {
        if (isColMaskUnavailable(colMaskBits, sysColNum, i)) {
          setColumn(i, nullptr, false);
          continue;
        }
        std::unique_ptr<Vector> vect = Vector::deserialize(deserializer.get());
        tb->setColumn(i, std::move(vect), false);
      }

      this->append(tb.get());
    }

    assert(this->isValid());
    cursor = deserializer->getCursor();
  }
}

void TupleBatch::permutate(const std::vector<uint32_t> &projIdxs) {
  if (isScalarTupleBatch_) {
    assert(projIdxs.size() == scalars_.size());
    std::vector<Scalar> tmp(scalars_.size());
    for (auto i = 0; i < scalars_.size(); i++) tmp[projIdxs[i]] = scalars_[i];
    scalars_ = std::move(tmp);
    return;
  }
  assert(projIdxs.size() == vectors.size());
  for (auto i = 0; i < vectors.size(); i++) {
    vectorRefs[projIdxs[i]] = vectors[i].release();
  }
  for (auto i = 0; i < vectors.size(); i++) {
    vectors[i].reset(vectorRefs[i]);
  }
}

bool TupleBatch::ownData() const {
  for (auto const &vec : vectors)
    if (!vec->getOwnData()) return false;
  return true;
}

void TupleBatch::trim() {
  for (auto vec : vectorRefs) vec->trim();
}

std::unique_ptr<TupleBatch> TupleBatch::extract(const SelectList *sel,
                                                uint64_t start,
                                                uint64_t end) const {
  assert(start <= end);
  std::unique_ptr<TupleBatch> res(new TupleBatch());
  if (sel && sel->size() < (end - start)) {
    res->setSelected(*sel);
  }
  res->setNumOfRows(end - start);
  res->isScalarTupleBatch_ = this->isScalarTupleBatch_;
  res->rgId = this->rgId;
  const SelectList *csel = res->getSelected();
  for (auto &vect : vectors) {
    if (!vect) {
      res->addColumn(nullptr);
      continue;
    }
    std::unique_ptr<Vector> tmp = vect->extract(csel, start, end);
    res->addColumn(std::move(tmp));
  }
  for (auto &vect : sysVectors) {
    if (!vect) {
      res->addSysColumn(nullptr);
      continue;
    }
    std::unique_ptr<Vector> tmp = vect->extract(csel, start, end);
    res->addSysColumn(std::move(tmp));
  }
  return res;
}

std::unique_ptr<TupleBatch> TupleBatch::extract(uint64_t start,
                                                uint64_t end) const {
  if (selected_) {
    std::unique_ptr<SelectList> sel = selected_->extract(start, end);
    return extract(sel.get(), start, end);
  } else {
    return extract(nullptr, start, end);
  }
}

std::unique_ptr<TupleDesc> TupleBatch::mockTupleDesc() {
  assert(!vectors.empty());
  std::unique_ptr<TupleDesc> td(new TupleDesc());
  uint32_t i = 0;
  for (auto &col : vectors) {
    td->add(std::to_string(i++), col->getTypeKind(), col->getTypeModifier());
  }
  return td;
}

double TupleBatch::getMemUsed() {
  double memUsed = 0;
  for (const auto &vec : vectors)
    if (vec) memUsed += vec->getSerializeSize();
  return memUsed;
}

void TupleBatch::replaceDecimalVector() {
  size_t numColumns = getNumOfColumns();
  for (int i = 0; i < numColumns; i++) {
    Vector *vect = getColumn(i);
    if (vect && vect->getTypeKind() == dbcommon::TypeKind::DECIMALID) {
      const uint64_t sz = vect->getNumOfRowsPlain();
      std::unique_ptr<dbcommon::Vector> vector =
          dbcommon::Vector::BuildVector(dbcommon::TypeKind::DECIMALNEWID, true);

      vector->setHasNull(vect->hasNullValue());
      dbcommon::DecimalVector *dvec =
          dynamic_cast<dbcommon::DecimalVector *>(vector.get());

      for (int j = 0; j < sz; j++) {
        bool isnull = false;
        std::string str = vect->readPlain(j, &isnull);
        dvec->append(str, isnull);
      }

      setColumn(i, std::move(vector), false);
    }
  }
}

}  // namespace dbcommon.
