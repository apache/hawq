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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_BYTE_BUFFER_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_BYTE_BUFFER_H_

#include <algorithm>
#include <cassert>
#include <type_traits>
#include <utility>
#include <vector>

#include "dbcommon/nodes/select-list.h"
#include "dbcommon/utils/cutils.h"

#define INIT_RESERVE_BYTES 8

namespace dbcommon {

class ByteBuffer {
 public:
  // The constructor.
  // @param ownData Indicating it owns the data or not.
  // @return void
  explicit ByteBuffer(bool ownData) : ownData_(ownData) {}

  // The constructor of an buffer wrapper of external data
  // @param buffer The external data.
  // @param size The external data size in Bytes
  // @return void
  ByteBuffer(char *buffer, uint64_t size)
      : data_(buffer), ownData_(false), size_(size), reserved_(0) {
    assert(buffer != nullptr && size > 0);
  }

  virtual ~ByteBuffer() {
    if (ownData_) cnfree(data_);
  }

  // Set the external data address and size (in bytes)
  // @param d The external data
  // @param dataSize The size of of the external data.
  // @return void
  void setData(const char *d, uint64_t dataSize) {
    assert(!ownData_);
    data_ = const_cast<char *>(d);
    reserved_ = 0;
    this->size_ = dataSize;
  }

  // Get the address of the buffer
  // @return The buffer address
  char *data() const { return data_; }

  // Get the tail of the buffer
  // @return The tail address
  char *tail() const { return data_ + size_; }

  // Get the size of buffer in Bytes
  // @return The size of the buffer in Bytes
  uint64_t size() const { return size_; }

  // Append a value of type T.
  // @param value The value to write
  // @return void
  template <typename T>
  void append(T value) {
    assert(ownData_);

    if (size_ + sizeof(T) > reserved_)
      reserve(reserved_ + (reserved_ > sizeof(T) ? reserved_ : sizeof(T)));

    assert(size_ + sizeof(T) <= reserved_);

    *(reinterpret_cast<T *>(&data_[size_])) = value;
    size_ += sizeof(T);
  }

  // Read the index(th) value of type T.
  // that is at byte position: "index" * sizeof(T)
  // @param index The index
  // @return The value of type T
  template <typename T>
  T get(uint64_t index) const {
    assert((index * sizeof(T) + sizeof(T)) <= size_);
    assert(size_ > 0);

    T ret = *(reinterpret_cast<T *>(&data_[index * sizeof(T)]));
    return ret;
  }

  // Set the index(th) value of type T.
  // that is at byte position: "index" * sizeof(T)
  // @param index The index
  // @param value The value to write
  // @return void
  template <typename T>
  void set(uint64_t index, T value) {
    assert(ownData_);
    assert(size_ > 0);

    assert(index * sizeof(T) + sizeof(T) <= size_);

    *(reinterpret_cast<T *>(&data_[index * sizeof(T)])) = value;
  }

  // Append bytes to the buffer.
  // @param value The data to appends
  // @param sz The data size
  // @return void
  void append(const char *value, uint64_t sz) {
    assert(ownData_);
    assert(value != NULL || sz == 0);

    if (size_ + sz > reserved_) {
      reserve(reserved_ + (reserved_ > sz ? reserved_ : sz));
    }

    assert(size_ + sz <= reserved_);

    memcpy(&data_[size_], value, sz);
    size_ += sz;
  }

  // Read 'length' bytes from pos
  // @param pos The position to read
  // @param length The length
  // @return the address of the data
  char *read(uint64_t pos /* Bytes */, uint64_t length) const {
    assert((pos + length) <= size_);
    assert(size_ > 0);

    char *p = &data_[pos];
    return p;
  }

  // Resize the buffer. If the buffer size >= sz, nothing changed.
  // otherwise, expend the buffer.
  // @param sz The new size
  // @return void
  void resize(uint64_t sz) {
    assert(ownData_);

    if (size_ < sz) reserve(sz);

    size_ = sz;
  }

  void reset(bool ownData) {
    if (ownData_) cnfree(data_);

    data_ = nullptr;
    reserved_ = 0;
    size_ = 0;
    ownData_ = ownData;
  }

  void clear() { size_ = 0; }

  // Materialize the external buffer.
  // if it owns the data, do nothing.
  // @return void
  void materialize() {
    if (!ownData_) {
      if (data_) {
        reserved_ = size_;
        char *d = dbcommon::cnmalloc(size_);
        memcpy(d, data_, size_);
        data_ = d;
      }
      ownData_ = true;
    }
  }

  template <typename T>
  void materialize(SelectList *sel) {
    if (sel) {
      // has select list

      assert(data_);
      T *d;
      reserved_ = sel->size() * sizeof(T);
      size_ = reserved_;
      d = reinterpret_cast<T *>(dbcommon::cnmalloc(size_));

      T *data = reinterpret_cast<T *>(data_);
      for (auto i = 0; i < sel->size(); i++) {
        d[i] = data[(*sel)[i]];
      }

      if (ownData_) cnfree(data_);
      data_ = reinterpret_cast<char *>(d);
    } else {
      // no select list
      if (!ownData_ && data_) {
        char *d = dbcommon::cnmalloc(size_);
        memcpy(d, data_, size_);
        reserved_ = size_;
        data_ = d;
      }
    }
    ownData_ = true;
  }

  // Does the buffer own the data or just a wrapper
  // @return A bool value shows the buffer owns the data
  bool getOwnData() const { return ownData_; }

  // sets the buffer whether to own the data
  void setOwnData(bool isOwnData) { ownData_ = isOwnData; }

  void swap(ByteBuffer &buf) {
    std::swap(data_, buf.data_);
    std::swap(size_, buf.size_);
    std::swap(reserved_, buf.reserved_);
    std::swap(ownData_, buf.ownData_);
  }

  // Reserve bytes the buffer.
  // @param pos The position to read
  // @param length The length
  // @return the address of the data
  void reserve(uint64_t newSize) {
    assert(ownData_);

    if (newSize > reserved_) {
      this->data_ = dbcommon::cnrealloc(this->data_, sizeof(char) * newSize);
      reserved_ = newSize;
    }
  }

 private:
  // read data buffer
  char *data_ = nullptr;  // the byte buffer

  // real data size
  uint64_t size_ = 0;

  // Must be multiple of 8, and initial size must be multiple of 8
  // too, required in bitmap alignment in block
  uint64_t reserved_ = 0;

  // whether ByteBuffer owns the data or is just a wrapper of external buffer
  bool ownData_ = false;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_BYTE_BUFFER_H_
