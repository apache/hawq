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

#include "storage/format/orc/data-buffer.h"

#include "dbcommon/utils/cutils.h"
#include "dbcommon/utils/global.h"

#include "storage/format/orc/int128.h"

namespace orc {

template <class T>
DataBuffer<T>::DataBuffer(dbcommon::MemoryPool& pool,  // NOLINT
                          uint64_t newSize)
    : memoryPool(pool), buf(nullptr), currentSize(0), currentCapacity(0) {
  if (newSize) resize(newSize);
}

template <class T>
DataBuffer<T>::~DataBuffer() {
  if (buf) memoryPool.free(buf);
}

template <class T>
void DataBuffer<T>::resize(uint64_t newSize) {
  if (buf) {
    buf = memoryPool.realloc<T>(buf, sizeof(T) * newSize);
  } else {
    buf = memoryPool.malloc<T>(sizeof(T) * newSize);
  }
  currentCapacity = memoryPool.getSpace() / sizeof(T);
  currentSize = newSize;
}

template class DataBuffer<bool>;
template class DataBuffer<char>;
template class DataBuffer<char*>;
template class DataBuffer<float>;
template class DataBuffer<double>;
template class DataBuffer<Int128>;
template class DataBuffer<int64_t>;
template class DataBuffer<uint64_t>;
template class DataBuffer<int32_t>;
template class DataBuffer<uint32_t>;
template class DataBuffer<int16_t>;
template class DataBuffer<uint16_t>;
template class DataBuffer<int8_t>;
template class DataBuffer<uint8_t>;

}  // namespace orc
