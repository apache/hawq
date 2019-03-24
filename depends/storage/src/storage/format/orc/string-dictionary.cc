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

#include "storage/format/orc/string-dictionary.h"

#include <cassert>

namespace orc {

uint32_t StringDictionary::add(const char *buffer, uint64_t len) {
  std::string key;
  key.append(buffer, len);
  StringDictionaryMap::iterator it = myMap.find(key);
  if (it != myMap.end()) {
    return it->second;
  } else {
    myMap[key] = ++id;
    bytes += len + 8;
    return id;
  }
}

void StringDictionary::dump(std::vector<const char *> *vals,
                            std::vector<uint64_t> *lens,
                            std::vector<uint32_t> *dumpOrder) const {
  assert(vals != nullptr && lens != nullptr && dumpOrder != nullptr);
  int32_t size = myMap.size();
  vals->resize(size);
  lens->resize(size);
  dumpOrder->resize(size);
  int32_t index = 0;
  for (StringDictionaryMap::const_iterator it = myMap.begin();
       it != myMap.end(); ++it) {
    (*vals)[index] = it->first.data();
    (*lens)[index] = it->first.length();
    (*dumpOrder)[it->second] = index++;
  }
}

uint32_t StringDictionary::size() const { return myMap.size(); }

uint32_t StringDictionary::sizeInBytes() const { return bytes; }

}  // namespace orc
