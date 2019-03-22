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

#include "dbcommon/nodes/scalar.h"

#include "dbcommon/utils/byte-buffer.h"

namespace dbcommon {

char* Scalar::allocateValue(size_t size) {
  if (memBuffer_ == nullptr) memBuffer_.reset(new ByteBuffer(true));
  memBuffer_->resize(size);
  value = CreateDatum(memBuffer_->data());
  length = size;
  return value;
}

ByteBuffer* Scalar::getValueBuffer() {
  if (memBuffer_ == nullptr) memBuffer_.reset(new ByteBuffer(true));
  return memBuffer_.get();
}

bool Scalar::getBoolValue() const {
  if (!isnull && DatumGetValue<bool>(value))
    return true;
  else
    return false;
}

}  // namespace dbcommon
