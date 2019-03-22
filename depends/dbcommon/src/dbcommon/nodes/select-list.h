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

#ifndef DBCOMMON_SRC_DBCOMMON_NODES_SELECT_LIST_H_
#define DBCOMMON_SRC_DBCOMMON_NODES_SELECT_LIST_H_

#include <algorithm>
#include <cassert>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "dbcommon/nodes/datum.h"
#include "dbcommon/utils/macro.h"

namespace dbcommon {

class BoolBuffer;
class Vector;

class SelectList : public Object {
 public:
  typedef uint16_t value_type;
  typedef value_type& reference;
  typedef const value_type& const_reference;
  typedef value_type* iterator;
  typedef const value_type* const_iterator;
  typedef size_t size_type;

  SelectList(size_t plainSize = DEFAULT_NUMBER_TUPLES_PER_BATCH);  // NOLINT

  SelectList(const SelectList& x);

  SelectList(SelectList&& x);

  SelectList(std::initializer_list<value_type> il);

  ~SelectList();

  SelectList& operator=(SelectList x);

  SelectList& operator=(std::initializer_list<value_type> il);

  iterator begin() noexcept { return indexs_.get(); }
  const_iterator begin() const noexcept { return indexs_.get(); }

  iterator end() noexcept { return indexs_.get() + size_; }
  const_iterator end() const noexcept { return indexs_.get() + size_; }

  const_iterator cbegin() const noexcept { return indexs_.get(); }

  const_iterator cend() const noexcept { return indexs_.get() + size_; }

  // Get real size of SelectList's index
  size_type size() const noexcept { return size_; }

  void resize(size_type n) { size_ = n; }

  bool empty() const noexcept { return size_ == 0; }

  void reserve(size_type n);

  reference operator[](size_type n) { return indexs_[n]; }
  const_reference operator[](size_type n) const { return indexs_[n]; }

  void push_back(const value_type& val) { indexs_[size_++] = val; }

  void swap(SelectList& x) {
    std::swap(size_, x.size_);
    std::swap(plainSize_, x.plainSize_);
    std::swap(indexs_, x.indexs_);
    std::swap(nulls_, x.nulls_);
  }

  void clear();

  std::unique_ptr<SelectList> extract(uint64_t start, uint64_t end) {
    assert(start < end);
    std::unique_ptr<value_type[]> ret(new value_type[size_]);
    uint64_t ret_size = 0;
    for (auto i = 0; i < size_; i++) {
      auto idx = indexs_[i];
      if (idx >= start && idx < end) {
        ret[ret_size++] = idx;
      }
    }
    std::unique_ptr<SelectList> retval;
    retval->size_ = ret_size;
    retval->indexs_.swap(ret);
    return std::move(retval);
  }

  // Get complementary set
  //
  // @param[in] plainSize corresponding plain size
  SelectList getComplement(size_t plainSize);

  // Get plain size of SelectList's corresponding Vector
  size_type plainSize() const noexcept { return plainSize_; }
  void setPlainSize(size_t plainSize) { plainSize_ = plainSize; }

  const bool* getNulls() const;

  // Fill SelectList's NullVector according to the input NullVector.
  void setNulls(size_t plainSize, const SelectList* sel,
                const bool* __restrict__ nulls1,
                const bool* __restrict__ nulls2 = nullptr,
                const bool* __restrict__ nulls3 = nullptr);

  // Fill SelectList's NullVector according to the input NullValue.
  void setNulls(size_t plainSize, const SelectList* sel, bool null);

  // Dump SelectList into BooleanVector
  //
  // @param[out] vec
  void toVector(Vector* vec);

  // Dump BooleanVector into SelectList
  //
  // @param[in] vec
  void fromVector(const Vector* vec);

  std::string toString();

 private:
  uint64_t size_ = 0;       // size of SelectList's index
  uint64_t plainSize_ = 0;  // plain size of SelectList's corresponding Vector
  std::unique_ptr<value_type[]> indexs_;
  std::unique_ptr<BoolBuffer> nulls_;

  friend bool operator==(const SelectList& lhs, const SelectList& rhs);
  friend SelectList operator+(const SelectList& lhs, const SelectList& rhs);
};

inline bool operator==(const SelectList& lhs, const SelectList& rhs) {
  if (lhs.size_ != rhs.size_) return false;
  for (auto i = 0; i < lhs.size_; i++)
    if (lhs.indexs_[i] != rhs.indexs_[i]) return false;
  return true;
}

inline SelectList operator+(const SelectList& lhs, const SelectList& rhs) {
  SelectList retval;
  retval.resize(lhs.size() + rhs.size());
  auto end = std::set_union(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(),
                            retval.begin());
  retval.resize(end - retval.begin());
  return retval;
}

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_NODES_SELECT_LIST_H_
