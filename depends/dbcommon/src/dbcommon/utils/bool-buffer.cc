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

#include "dbcommon/utils/bool-buffer.h"

namespace dbcommon {

uint64_t bit_to_bytes[256] = {0x0,
                              0x1,
                              0x100,
                              0x101,
                              0x10000,
                              0x10001,
                              0x10100,
                              0x10101,
                              0x1000000,
                              0x1000001,
                              0x1000100,
                              0x1000101,
                              0x1010000,
                              0x1010001,
                              0x1010100,
                              0x1010101,
                              0x100000000,
                              0x100000001,
                              0x100000100,
                              0x100000101,
                              0x100010000,
                              0x100010001,
                              0x100010100,
                              0x100010101,
                              0x101000000,
                              0x101000001,
                              0x101000100,
                              0x101000101,
                              0x101010000,
                              0x101010001,
                              0x101010100,
                              0x101010101,
                              0x10000000000,
                              0x10000000001,
                              0x10000000100,
                              0x10000000101,
                              0x10000010000,
                              0x10000010001,
                              0x10000010100,
                              0x10000010101,
                              0x10001000000,
                              0x10001000001,
                              0x10001000100,
                              0x10001000101,
                              0x10001010000,
                              0x10001010001,
                              0x10001010100,
                              0x10001010101,
                              0x10100000000,
                              0x10100000001,
                              0x10100000100,
                              0x10100000101,
                              0x10100010000,
                              0x10100010001,
                              0x10100010100,
                              0x10100010101,
                              0x10101000000,
                              0x10101000001,
                              0x10101000100,
                              0x10101000101,
                              0x10101010000,
                              0x10101010001,
                              0x10101010100,
                              0x10101010101,
                              0x1000000000000,
                              0x1000000000001,
                              0x1000000000100,
                              0x1000000000101,
                              0x1000000010000,
                              0x1000000010001,
                              0x1000000010100,
                              0x1000000010101,
                              0x1000001000000,
                              0x1000001000001,
                              0x1000001000100,
                              0x1000001000101,
                              0x1000001010000,
                              0x1000001010001,
                              0x1000001010100,
                              0x1000001010101,
                              0x1000100000000,
                              0x1000100000001,
                              0x1000100000100,
                              0x1000100000101,
                              0x1000100010000,
                              0x1000100010001,
                              0x1000100010100,
                              0x1000100010101,
                              0x1000101000000,
                              0x1000101000001,
                              0x1000101000100,
                              0x1000101000101,
                              0x1000101010000,
                              0x1000101010001,
                              0x1000101010100,
                              0x1000101010101,
                              0x1010000000000,
                              0x1010000000001,
                              0x1010000000100,
                              0x1010000000101,
                              0x1010000010000,
                              0x1010000010001,
                              0x1010000010100,
                              0x1010000010101,
                              0x1010001000000,
                              0x1010001000001,
                              0x1010001000100,
                              0x1010001000101,
                              0x1010001010000,
                              0x1010001010001,
                              0x1010001010100,
                              0x1010001010101,
                              0x1010100000000,
                              0x1010100000001,
                              0x1010100000100,
                              0x1010100000101,
                              0x1010100010000,
                              0x1010100010001,
                              0x1010100010100,
                              0x1010100010101,
                              0x1010101000000,
                              0x1010101000001,
                              0x1010101000100,
                              0x1010101000101,
                              0x1010101010000,
                              0x1010101010001,
                              0x1010101010100,
                              0x1010101010101,
                              0x100000000000000,
                              0x100000000000001,
                              0x100000000000100,
                              0x100000000000101,
                              0x100000000010000,
                              0x100000000010001,
                              0x100000000010100,
                              0x100000000010101,
                              0x100000001000000,
                              0x100000001000001,
                              0x100000001000100,
                              0x100000001000101,
                              0x100000001010000,
                              0x100000001010001,
                              0x100000001010100,
                              0x100000001010101,
                              0x100000100000000,
                              0x100000100000001,
                              0x100000100000100,
                              0x100000100000101,
                              0x100000100010000,
                              0x100000100010001,
                              0x100000100010100,
                              0x100000100010101,
                              0x100000101000000,
                              0x100000101000001,
                              0x100000101000100,
                              0x100000101000101,
                              0x100000101010000,
                              0x100000101010001,
                              0x100000101010100,
                              0x100000101010101,
                              0x100010000000000,
                              0x100010000000001,
                              0x100010000000100,
                              0x100010000000101,
                              0x100010000010000,
                              0x100010000010001,
                              0x100010000010100,
                              0x100010000010101,
                              0x100010001000000,
                              0x100010001000001,
                              0x100010001000100,
                              0x100010001000101,
                              0x100010001010000,
                              0x100010001010001,
                              0x100010001010100,
                              0x100010001010101,
                              0x100010100000000,
                              0x100010100000001,
                              0x100010100000100,
                              0x100010100000101,
                              0x100010100010000,
                              0x100010100010001,
                              0x100010100010100,
                              0x100010100010101,
                              0x100010101000000,
                              0x100010101000001,
                              0x100010101000100,
                              0x100010101000101,
                              0x100010101010000,
                              0x100010101010001,
                              0x100010101010100,
                              0x100010101010101,
                              0x101000000000000,
                              0x101000000000001,
                              0x101000000000100,
                              0x101000000000101,
                              0x101000000010000,
                              0x101000000010001,
                              0x101000000010100,
                              0x101000000010101,
                              0x101000001000000,
                              0x101000001000001,
                              0x101000001000100,
                              0x101000001000101,
                              0x101000001010000,
                              0x101000001010001,
                              0x101000001010100,
                              0x101000001010101,
                              0x101000100000000,
                              0x101000100000001,
                              0x101000100000100,
                              0x101000100000101,
                              0x101000100010000,
                              0x101000100010001,
                              0x101000100010100,
                              0x101000100010101,
                              0x101000101000000,
                              0x101000101000001,
                              0x101000101000100,
                              0x101000101000101,
                              0x101000101010000,
                              0x101000101010001,
                              0x101000101010100,
                              0x101000101010101,
                              0x101010000000000,
                              0x101010000000001,
                              0x101010000000100,
                              0x101010000000101,
                              0x101010000010000,
                              0x101010000010001,
                              0x101010000010100,
                              0x101010000010101,
                              0x101010001000000,
                              0x101010001000001,
                              0x101010001000100,
                              0x101010001000101,
                              0x101010001010000,
                              0x101010001010001,
                              0x101010001010100,
                              0x101010001010101,
                              0x101010100000000,
                              0x101010100000001,
                              0x101010100000100,
                              0x101010100000101,
                              0x101010100010000,
                              0x101010100010001,
                              0x101010100010100,
                              0x101010100010101,
                              0x101010101000000,
                              0x101010101000001,
                              0x101010101000100,
                              0x101010101000101,
                              0x101010101010000,
                              0x101010101010001,
                              0x101010101010100,
                              0x101010101010101};

// Reset the BoolBuffer according to the input NullVector.
void BoolBuffer::reset(size_t plainSize, const SelectList *sel,
                       const bool *__restrict__ nulls1,
                       const bool *__restrict__ nulls2,
                       const bool *__restrict__ nulls3) {
  if (nulls3 == nulls2) nulls3 = nullptr;
  if (nulls3 == nulls1) nulls3 = nullptr;
  if (nulls2 == nulls1) nulls2 = nullptr;

  resize(plainSize);
  bool *__restrict__ nulls = this->data();

  if (nulls == nulls1 || nulls == nulls2 || nulls == nulls3) {
    if (nulls == nulls1) nulls1 = nullptr;
    if (nulls == nulls2) nulls2 = nullptr;
    if (nulls == nulls3) nulls3 = nullptr;
  } else {
    memset(nulls, 0, plainSize);
  }

  if (nulls1 || nulls2 || nulls3) {
    if (nulls1 && nulls2 && nulls3) {
      // Case4. 3 valid nulls
      if (sel) {
        for (auto i : *sel) nulls[i] = nulls1[i] | nulls2[i] | nulls3[i];
      } else {
        for (auto i = 0; i < plainSize; i++)
          nulls[i] = nulls1[i] | nulls2[i] | nulls3[i];
      }
    } else {
      // put all valid nulls into <nulls1, nulls2>
      if (nulls1 == nullptr)
        nulls1 = nulls3;
      else if (nulls2 == nullptr)
        nulls2 = nulls3;

      if (nulls1 && nulls2) {
        // Case3. 2 valid nulls
        if (sel) {
          for (auto i : *sel) nulls[i] = nulls1[i] | nulls2[i];
        } else {
          for (auto i = 0; i < plainSize; i++) nulls[i] = nulls1[i] | nulls2[i];
        }
      } else {
        // Case2. 1 valid nulls
        nulls1 = nulls1 ? nulls1 : nulls2;
        if (sel) {
          for (auto i : *sel) nulls[i] = nulls1[i];
        } else {
          memcpy(nulls, nulls1, plainSize);
        }
      }
    }
  } else {
    // Case1. 0 valid nulls
  }
}

// Reset the BoolBuffer according to the input NullValue.
void BoolBuffer::reset(size_t plainSize, const SelectList *sel, bool null) {
  resize(plainSize);
  bool *__restrict__ nulls_ = this->data();

  if (sel) {
    for (auto i : *sel) nulls_[i] = null;
  } else {
    std::fill_n(nulls_, plainSize, null);
  }
}

}  // namespace dbcommon
