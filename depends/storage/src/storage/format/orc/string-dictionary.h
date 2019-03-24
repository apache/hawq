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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_STRING_DICTIONARY_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_STRING_DICTIONARY_H_

#include <map>
#include <string>
#include <vector>

namespace orc {

typedef std::map<std::string, uint32_t> StringDictionaryMap;

class StringDictionary {
 public:
  StringDictionary() { reset(); }
  virtual ~StringDictionary() {}

  uint32_t add(const char *buffer, uint64_t len);

  void dump(std::vector<const char *> *vals, std::vector<uint64_t> *lens,
            std::vector<uint32_t> *dumpOrder) const;

  uint32_t size() const;

  uint32_t sizeInBytes() const;

  void clear() { reset(); }

 private:
  void reset() {
    myMap.clear();
    id = -1;
    bytes = 0;
  }

 private:
  StringDictionaryMap myMap;
  uint32_t id;
  uint32_t bytes;
};

}  // namespace orc

#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_STRING_DICTIONARY_H_
