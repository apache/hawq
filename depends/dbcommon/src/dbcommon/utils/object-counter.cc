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

#include "dbcommon/utils/object-counter.h"

namespace dbcommon {

std::mutex ObjectCounterMap::counter_lock_;
std::unordered_map<const char*, size_t> ObjectCounterMap::counter_map_;

void ObjectCounterMap::logObjectCounterMap() {
  std::lock_guard<std::mutex> lock_guard(counter_lock_);
  LOG_INFO("ObjectCounter Statistics:");
  for (const auto& iter : counter_map_) {
    LOG_INFO("ObjectCounter: %s got %zu objects.", iter.first, iter.second);
  }
}

}  // namespace dbcommon
