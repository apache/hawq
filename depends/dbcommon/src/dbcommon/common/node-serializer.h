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

#ifndef DBCOMMON_SRC_DBCOMMON_COMMON_NODE_SERIALIZER_H_
#define DBCOMMON_SRC_DBCOMMON_COMMON_NODE_SERIALIZER_H_

#include <cassert>
#include <map>
#include <string>
#include <vector>

namespace dbcommon {

class NodeSerializer {
 public:
  explicit NodeSerializer(std::string *buf) : buffer(buf) {}
  virtual ~NodeSerializer() {}

  template <typename T>
  void write(T val) {
    buffer->append(reinterpret_cast<const char *>(&val), sizeof(T));
  }

  void writeBytes(const char *buf, size_t length) {
    assert(length >= 0);
    if (length > 0) buffer->append(buf, length);
  }

 private:
  std::string *buffer;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_COMMON_NODE_SERIALIZER_H_
