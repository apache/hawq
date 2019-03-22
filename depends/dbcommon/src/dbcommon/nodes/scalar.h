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

#ifndef DBCOMMON_SRC_DBCOMMON_NODES_SCALAR_H_
#define DBCOMMON_SRC_DBCOMMON_NODES_SCALAR_H_

#include <memory>

#include "dbcommon/nodes/datum.h"
#include "dbcommon/utils/cutils.h"

namespace dbcommon {

class ByteBuffer;

// Scalar represents a single element of a specific data type or a NULL value.
//
// 1. For NULL value, Scalar's isnull is true and its length and isnull are
// undefined.
// 2. For data type that fits into an uint64_t, Scalar's isnull is false,
// Scalar's length is 0 and fit the value into Scalar's value as Datum.
// 3. For data type that does not fit into an uint64_t, Scalar's isnull is
// false and Scalar's value is a pointer that points to the exact data while
// Scalar's length specifies the size of the exact data.
//
// For data type that fits into an uint64_t, Scalar simply copies the data into
// Scalar::value.
// For data type that does not fit into an uint64_t, Scalar works as a reference
// or a data keeper. As a reference, Scalar's value points to the referenced
// data, with its memBuffer_ set to nullptr. As a data keeper, Scalar's value
// points to its memBuffer_, which allocated by calling Scalar::allocateValue.
class Scalar : public Object {
 public:
  Scalar() {}

  explicit Scalar(Datum d, bool isnull = false) : value(d), isnull(isnull) {}

  Scalar(const Scalar& s)
      : value(s.value), length(s.length), isnull(s.isnull) {}

  Scalar operator=(const Scalar& s) {
    value = s.value;
    length = s.length;
    isnull = s.isnull;

    return *this;
  }

  void clear() override {}

  char* allocateValue(size_t size);

  template <typename T>
  T* allocateValue() {
    return reinterpret_cast<T*>(allocateValue(sizeof(T)));
  }

  ByteBuffer* getValueBuffer();

  bool getBoolValue() const;

  Datum value = CreateDatum(0);
  uint32_t length = 0;
  bool isnull = false;

 private:
  std::unique_ptr<ByteBuffer> memBuffer_ = nullptr;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_NODES_SCALAR_H_
