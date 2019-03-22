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

#ifndef DBCOMMON_SRC_DBCOMMON_COMMON_NODE_DESERIALIZER_H_
#define DBCOMMON_SRC_DBCOMMON_COMMON_NODE_DESERIALIZER_H_

#include <cassert>
#include <string>
#include <unordered_map>
#include <vector>

namespace dbcommon {

#define BUFFER_CHECK_INDEX(type, nelement, cursor, size)     \
  assert(((cursor) + sizeof(type) * (nelement) <= (size)) && \
         "index out of range")

class NodeDeserializer {
 public:
  explicit NodeDeserializer(const std::string &buf)
      : buffer(buf.data()), cursor(0), size(buf.length()) {}
  NodeDeserializer(const char *buf, size_t len)
      : buffer(buf), cursor(0), size(len) {}

  NodeDeserializer(const NodeDeserializer &other) = delete;
  NodeDeserializer &operator=(const NodeDeserializer &other) = delete;
  NodeDeserializer(NodeDeserializer &&other) = delete;
  NodeDeserializer &operator=(NodeDeserializer &&other) = delete;

  ~NodeDeserializer() {}

  void readBegin() {}

  template <typename T>
  T read() {
    static_assert(
        std::is_fundamental<T>::value == true || std::is_enum<T>::value == true,
        "T2 is fundamental");

    BUFFER_CHECK_INDEX(T, 1, cursor, size);
    T retval = *reinterpret_cast<const T *>(buffer + cursor);
    cursor += sizeof(T);
    return retval;
  }

  const char *readBytes(size_t length);

  size_t getCursor() { return cursor; }
  void setCursor(size_t cursor_) { cursor = cursor_; }

 private:
  const char *buffer;
  size_t cursor;
  size_t size;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_COMMON_NODE_DESERIALIZER_H_
