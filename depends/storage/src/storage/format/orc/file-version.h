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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_FILE_VERSION_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_FILE_VERSION_H_

#include <string>

namespace orc {

class FileVersion {
 private:
  uint32_t majorVersion;
  uint32_t minorVersion;

 public:
  FileVersion(uint32_t major, uint32_t minor)
      : majorVersion(major), minorVersion(minor) {}

  uint32_t getMajor() const { return this->majorVersion; }

  uint32_t getMinor() const { return this->minorVersion; }

  bool operator==(const FileVersion& right) const {
    return this->majorVersion == right.getMajor() &&
           this->minorVersion == right.getMinor();
  }

  bool operator!=(const FileVersion& right) const { return !(*this == right); }

  std::string toString() const {
    std::stringstream ss;
    ss << getMajor() << '.' << getMinor();
    return ss.str();
  }
};

}  // namespace orc

#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_FILE_VERSION_H_
