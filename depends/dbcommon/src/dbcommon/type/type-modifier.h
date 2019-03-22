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

#ifndef DBCOMMON_SRC_DBCOMMON_TYPE_TYPE_MODIFIER_H_
#define DBCOMMON_SRC_DBCOMMON_TYPE_TYPE_MODIFIER_H_

#include <cstdint>

#include "dbcommon/utils/macro.h"

namespace dbcommon {

/*
 * Here we use int64_t to represent typeModifier, corresponding to
 * FormData_pg_attribute.atttypmod(an int4).
 *
 * In the future time, precision and scale need to support.
 */
#define VARHDRSZ ((int64_t)sizeof(int32_t))
#define SCALE_MASK ((1 << 16) - 1)
#define DEFAULT_PRECISION 38
#define DEFAULT_SCALE 0

class TypeModifierUtil {
 public:
  inline static uint64_t getMaxLen(int64_t typeMod) {
    return isUndefinedModifier(typeMod) ? DEFAULT_RESERVED_SIZE_OF_STRING
                                        : typeMod - VARHDRSZ;
  }
  inline static int64_t getTypeModifierFromMaxLength(uint64_t maxLength) {
    return static_cast<int64_t>(maxLength + VARHDRSZ);
  }
  inline static bool isUndefinedModifier(int64_t typeMod) {
    return (typeMod == -1) || (typeMod == 0);
  }
  inline static int64_t getTypeModifierFromPrecisionScale(uint64_t precision,
                                                          uint64_t scale) {
    return ((precision << 16) | scale) + VARHDRSZ;
  }
  inline static uint64_t getPrecision(int64_t typeMod) {
    return isUndefinedModifier(typeMod) ? DEFAULT_PRECISION : (typeMod >> 16);
  }
  inline static uint64_t getScale(int64_t typeMod) {
    return isUndefinedModifier(typeMod) ? DEFAULT_SCALE
                                        : ((typeMod & SCALE_MASK) - VARHDRSZ);
  }
};
}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_TYPE_TYPE_MODIFIER_H_
