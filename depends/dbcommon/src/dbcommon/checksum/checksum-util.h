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

#ifndef DBCOMMON_SRC_DBCOMMON_CHECKSUM_CHECKSUM_UTIL_H_
#define DBCOMMON_SRC_DBCOMMON_CHECKSUM_CHECKSUM_UTIL_H_
#include <memory>
#include "dbcommon/checksum/checksum.h"

namespace dbcommon {

class ChecksumUtil {
 public:
  ChecksumUtil();
  ~ChecksumUtil() {}

  /**
   * @return Returns the current checksum value.
   */
  uint32_t getValue();

  /**
   * Resets the checksum to its initial value.
   */
  void reset();

  /**
   * Updates the current checksum with the specified array of bytes.
   * @param b The buffer of data.
   * @param len The buffer length.
   */
  void update(const void* b, int len);

 private:
  std::unique_ptr<dbcommon::Checksum> chk;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_CHECKSUM_CHECKSUM_UTIL_H_
