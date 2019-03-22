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

#include "dbcommon/checksum/checksum-util.h"
#include <cassert>
#include "dbcommon/checksum/hw-crc32c.h"
#include "dbcommon/checksum/sw-crc32c.h"

namespace dbcommon {

ChecksumUtil::ChecksumUtil() {
  if (HWCrc32c::available()) {
    chk.reset(new HWCrc32c());
  } else {
    chk.reset(new SWCrc32c());
  }
  assert(chk);
}

uint32_t ChecksumUtil::getValue() { return chk->getValue(); }

void ChecksumUtil::reset() { chk->reset(); }

void ChecksumUtil::update(const void* b, int len) { chk->update(b, len); }

}  // namespace dbcommon
