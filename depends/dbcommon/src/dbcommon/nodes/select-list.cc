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

#include "dbcommon/nodes/select-list.h"

#include "dbcommon/common/vector-transformer.h"
#include "dbcommon/common/vector.h"
#include "dbcommon/utils/bool-buffer.h"

namespace dbcommon {

SelectList::SelectList(size_t plainSize) : plainSize_(plainSize) {
  indexs_.reset(new value_type[DEFAULT_NUMBER_TUPLES_PER_BATCH + 2]);
}

SelectList::SelectList(const SelectList& x)
    : size_(x.size_), plainSize_(x.plainSize_) {
  indexs_.reset(new value_type[DEFAULT_NUMBER_TUPLES_PER_BATCH + 2]);
  for (auto i = 0; i < size_; i++) indexs_[i] = x.indexs_[i];
  if (x.nulls_) {
    nulls_.reset(new BoolBuffer(true));
    nulls_->reset(x.plainSize_, nullptr, x.nulls_->getBools());
  } else {
    nulls_ = nullptr;
  }
}

SelectList::SelectList(SelectList&& x)
    : size_(x.size_), plainSize_(x.plainSize_) {
  indexs_.swap(x.indexs_);
  nulls_.swap(x.nulls_);
}

SelectList::SelectList(std::initializer_list<value_type> il)
    : size_(il.size()) {
  indexs_.reset(new value_type[size_]);
  for (auto i = 0; i < size_; i++) indexs_[i] = *(il.begin() + i);
}

SelectList::~SelectList() {}

SelectList& SelectList::operator=(std::initializer_list<value_type> il) {
  size_ = il.size();
  assert(DEFAULT_NUMBER_TUPLES_PER_BATCH + 2 > size_);
  for (auto i = 0; i < size_; i++) indexs_[i] = *(il.begin() + i);
  if (nulls_) nulls_->resize(0);
  return *this;
}

SelectList& SelectList::operator=(SelectList x) {
  swap(x);
  return *this;
}

void SelectList::reserve(size_type n) {
  size_ = 0;
  indexs_.reset(new value_type[n]);
}

void SelectList::clear() {
  size_ = 0;
  if (nulls_) nulls_->resize(0);
}

SelectList SelectList::getComplement(size_t size) {
  SelectList retval;
  auto ret = retval.begin();
  auto nulls = getNulls();
  uint64_t ret_size = 0;
  uint64_t idx = 0;
  if (nulls) {
    for (auto i = 0; i < size_; i++) {
      auto b = indexs_[i];
      while (idx < b) {
        if (!nulls[idx]) ret[ret_size++] = idx;
        idx++;
      }
      idx = b + 1;
    }
    while (idx < size) {
      if (!nulls[idx]) ret[ret_size++] = idx;
      idx++;
    }
  } else {
    for (auto i = 0; i < size_; i++) {
      auto b = indexs_[i];
      while (idx < b) {
        ret[ret_size++] = (idx++);
      }
      idx = b + 1;
    }
    while (idx < size) {
      ret[ret_size++] = (idx++);
    }
  }
  retval.size_ = ret_size;
  retval.plainSize_ = plainSize();
  return retval;
}

const bool* SelectList::getNulls() const {
  return reinterpret_cast<const bool*>(
      nulls_ ? (nulls_->size() != 0 ? nulls_->data() : nullptr) : nullptr);
}

void SelectList::setNulls(size_t plainSize, const SelectList* sel,
                          const bool* __restrict__ nulls1,
                          const bool* __restrict__ nulls2,
                          const bool* __restrict__ nulls3) {
  plainSize_ = plainSize;
  if (nulls1 || nulls2) {
    if (nulls_ == nullptr) nulls_.reset(new BoolBuffer(true));
    this->nulls_->reset(plainSize, sel, nulls1, nulls2, nulls3);
  } else {
    if (nulls_) nulls_->resize(0);
  }
}

void SelectList::setNulls(size_t plainSize, const SelectList* sel, bool null) {
  plainSize_ = plainSize;
  if (nulls_) {
    nulls_->reset(plainSize, sel, null);
  }
}

void SelectList::toVector(Vector* outVector) {
  assert(outVector->getTypeKind() == TypeKind::BOOLEANID);
  assert(size_ <= plainSize_);
  outVector->resize(plainSize_, nullptr, this->getNulls());
  FixedSizeTypeVectorRawData<bool> out(outVector);

  memset(out.values, 0, plainSize_);
  auto setBoolean = [&](uint64_t plainIdx) { out.values[plainIdx] = true; };
  transformVector(out.plainSize, this, out.nulls, setBoolean);
}

void SelectList::fromVector(const Vector* inVector) {
  assert(inVector->getTypeKind() == TypeKind::BOOLEANID);
  FixedSizeTypeVectorRawData<bool> in(const_cast<Vector*>(inVector));

  SelectList::size_type counter = 0;
  SelectList::value_type* __restrict__ ret = this->begin();
  auto updateSel = [&](uint64_t plainIdx) {
    if (in.values[plainIdx]) ret[counter++] = plainIdx;
  };
  transformVector(in.plainSize, in.sel, in.nulls, updateSel);
  size_ = counter;
  plainSize_ = in.plainSize;

  setNulls(plainSize_, nullptr, inVector->getNulls());
}

std::string SelectList::toString() {
  auto vec = Vector::BuildVector(TypeKind::BOOLEANID, true);
  this->toVector(vec.get());
  return vec->toString();
}

}  // namespace dbcommon
