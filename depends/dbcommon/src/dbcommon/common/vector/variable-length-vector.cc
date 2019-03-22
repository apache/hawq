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

#include "dbcommon/common/vector/variable-length-vector.h"

#include "dbcommon/common/vector-transformer.h"
#include "dbcommon/utils/string-util.h"

namespace dbcommon {

std::string BlankPaddedCharVector::readPlain(uint64_t index, bool *null) const {
  auto srcLen = getElementWidthPlain(index);
  auto srcStr = getValPtrPlain(index);
  *null = isNullPlain(index);
  if (*null) return std::string(getMaxLenModifier(), ' ');
  auto ret = newBlankPaddedChar(srcStr, bpCharTrueLen(srcStr, srcLen),
                                getMaxLenModifier());
  return std::move(ret);
}
void BlankPaddedCharVector::trim() {
  if (isTrimmed) return;

  VariableSizeTypeVectorRawData data(this);
  auto bpcharTrueLen = [&](uint64_t plainIdx) {
    const char *val = data.valptrs[plainIdx];
    uint64_t len = data.lengths[plainIdx];
#pragma clang loop unroll_count(4)
    while (len != 0 && val[len - 1] == ' ') len--;
    data.lengths[plainIdx] = len;
  };
  dbcommon::transformVector(getNumOfRowsPlain(), getSelected(), getNulls(),
                            bpcharTrueLen);

  isTrimmed = true;
}

void BinaryVector::append(const std::string &srcStr, bool null) {
  auto srcLen = srcStr.size();
  std::vector<char> binary(srcLen);
  uint32_t dstLen = 0;
  for (auto i = 0; i < srcLen;) {
    if (srcStr[i] != '\\') {
      binary[dstLen++] = srcStr[i];
      i += 1;
    } else if (srcStr[i + 1] == '\\') {
      binary[dstLen++] = '\\';
      i += 2;
    } else {
      binary[dstLen++] = (srcStr[i + 1] - '0') * 64 +
                         (srcStr[i + 2] - '0') * 8 + (srcStr[i + 3] - '0');
      i += 4;
    }
  }
  BytesVector::append(&binary[0], dstLen, null);
}

std::string BinaryVector::readPlain(uint64_t index, bool *null) const {
  auto srcLen = getElementWidthPlain(index);
  auto srcBin = getValPtrPlain(index);
  *null = isNullPlain(index);
  if (*null)
    return "";
  else
    return StringUtil::toOct(srcBin, srcLen);
}

}  // namespace dbcommon
