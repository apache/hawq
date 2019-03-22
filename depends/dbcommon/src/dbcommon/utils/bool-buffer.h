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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_BOOL_BUFFER_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_BOOL_BUFFER_H_

#include <algorithm>
#include <climits>
#include <iostream>
#include <memory>
#include <set>
#include <utility>

#include "dbcommon/utils/byte-buffer.h"

namespace dbcommon {

extern uint64_t bit_to_bytes[256];

class BoolBuffer {
 public:
  // The constructor.
  // @param ownData Indicating it owns the data or not.
  // @return void
  explicit BoolBuffer(bool ownData) : buffer_(ownData), bitBuffer_(false) {}

  ~BoolBuffer() {}

  // Get the size of buffer in number of bools
  // @return The size of the buffer in number of bools
  size_t size() const { return buffer_.size(); }

  // Get the address of the buffer
  // @return The buffer address
  const bool *data() const {
    return reinterpret_cast<const bool *>(buffer_.data());
  }

  bool *data() { return reinterpret_cast<bool *>(buffer_.data()); }

  // Get the bitmap size in Bytes
  // @return the size in Bytes
  size_t bitDataSizeInBytes() const {
    return bitmapBitLengthToByteLength(size());
  }

  // Set the external data address and size (in #bools)
  // @param d The external data
  // @param dataSize The size of of the external data (in #bools).
  // @return void
  void setBitData(const char *data, size_t numBools) {
    if (!buffer_.getOwnData()) buffer_.reset(true);
    int numBits = bitmapBitLengthToByteLength(numBools);
    // when this numBits > 1, reserve only can alloc 8 bytes,
    // so buffer overflow
    buffer_.reserve(8 * numBits);
    buffer_.resize(numBools);
    uint64_t *nulls = reinterpret_cast<uint64_t *>(buffer_.data());

    for (size_t i = 0; i < numBits; i++) {
      nulls[i] = bit_to_bytes[static_cast<uint8_t>(data[i])];
    }
  }

  const char *getBitData() const {
    if (!bitBuffer_.getOwnData()) bitBuffer_.reset(true);
    bitBuffer_.resize(bitDataSizeInBytes());
    const bool *nulls = reinterpret_cast<bool *>(buffer_.data());

    uint64_t size = this->size() / CHAR_BIT * CHAR_BIT;
    for (uint64_t i = 0; i < size; i += 8) {
      uint8_t v = 0;
      v |= nulls[i] | nulls[i + 1] << 1 | nulls[i + 2] << 2 |
           nulls[i + 3] << 3 | nulls[i + 4] << 4 | nulls[i + 5] << 5 |
           nulls[i + 6] << 6 | nulls[i + 7] << 7;

      bitBuffer_.data()[i / CHAR_BIT] = v;
    }

    if (0 != this->size() % CHAR_BIT) {
      char &v = bitBuffer_.data()[this->size() / CHAR_BIT];
      v = 0;

      for (uint64_t i = size; i < this->size(); ++i) {
        v |= nulls[i] << (i % CHAR_BIT);
      }
    }

    return bitBuffer_.data();
  }

  // Append a value of type T.
  // @param value The value to write
  // @return void
  void append(bool value) {
    assert(buffer_.getOwnData());
    buffer_.append<char>(value);
  }

  void append(const bool *buffer, uint64_t size) {
    if (size > 0) {
      for (size_t i = 0; i < size; ++i) {
        append(buffer[i]);
      }
    }
  }

  // Get the index(th) bit value.
  // @param index The index
  // @return The bit value
  bool get(uint64_t index) const {
    assert(index < size() || size() > 0);
    return buffer_.data()[index];
  }

  char getChar(uint64_t index) const {
    assert(index < size() || size() > 0);
    return buffer_.data()[index];
  }

  void set(uint64_t index, bool value) {
    assert(index < size() || size() > 0);
    buffer_.data()[index] = value;
  }

  // Resize the buffer. If the buffer size >= numOfBools, nothing changed
  // otherwise, expend the buffer.
  // @param numOfBools The new size
  // @return void
  void resize(uint64_t numOfBools) { buffer_.resize(numOfBools); }

  void reserve(uint64_t size) { buffer_.reserve(size); }

  static size_t bitmapBitLengthToByteLength(uint64_t n) {
    return (((n) + 7) >> 3);
  }

  bool *getBools() const {
    bool *nulls = reinterpret_cast<bool *>(buffer_.data());
    return nulls;
  }

  char *getChars() const {
    char *nulls = reinterpret_cast<char *>(buffer_.data());
    return nulls;
  }

  void setBools(const char *inputs, uint64_t sz) {
    assert(inputs != nullptr && sz >= 0);
    buffer_.setData(inputs, sz);
  }

  void setReserseBools(const char *inputs, uint64_t sz) {
    assert(inputs != nullptr && sz >= 0);

    if (!buffer_.getOwnData()) buffer_.reset(true);
    buffer_.resize(sz);
    for (uint64_t i = 0; i < sz; i++) {
      buffer_.set<bool>(i, !inputs[i]);
    }
  }

  // Reserve space and set all data values input(true/false)
  void setReserseBools(const bool input, uint64_t sz) {
    assert(sz >= 0);

    if (!buffer_.getOwnData()) buffer_.reset(true);
    buffer_.resize(sz);
    for (uint64_t i = 0; i < sz; i++) {
      buffer_.set<bool>(i, input);
    }
  }

  std::unique_ptr<ByteBuffer> getReverseBools() {
    uint64_t size = this->size();
    std::unique_ptr<ByteBuffer> buf(new ByteBuffer(true));
    buf->resize(size);
    for (uint64_t i = 0; i < size; i++) {
      buf->set<char>(i, !buffer_.get<char>(i));
    }
    return std::move(buf);
  }

  void swap(BoolBuffer &buf) {
    buffer_.swap(buf.buffer_);
    bitBuffer_.swap(buf.bitBuffer_);
  }

  void materialize(SelectList *sel) { buffer_.materialize<bool>(sel); }

  void reset(bool ownData) { buffer_.reset(ownData); }

  // Reset the BoolBuffer according to the input NullVector.
  void reset(size_t plainSize, const SelectList *sel,
             const bool *__restrict__ nulls1,
             const bool *__restrict__ nulls2 = nullptr,
             const bool *__restrict__ nulls3 = nullptr);

  // Reset the BoolBuffer according to the input NullValue.
  void reset(size_t plainSize, const SelectList *sel, bool null);

 private:
  ByteBuffer buffer_;
  mutable ByteBuffer bitBuffer_;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_BOOL_BUFFER_H_
