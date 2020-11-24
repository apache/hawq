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

#ifndef DBCOMMON_SRC_DBCOMMON_FUNCTION_TYPECAST_TEXTTONUM_FUNC_H_
#define DBCOMMON_SRC_DBCOMMON_FUNCTION_TYPECAST_TEXTTONUM_FUNC_H_

#include "dbcommon/common/vector.h"
#include "dbcommon/nodes/datum.h"
#include "dbcommon/type/decimal.h"

namespace dbcommon {

Datum text_to_int2(Datum *params, uint64_t size);
Datum text_to_int4(Datum *params, uint64_t size);
Datum text_to_int8(Datum *params, uint64_t size);
Datum text_to_float4(Datum *params, uint64_t size);
Datum text_to_float8(Datum *params, uint64_t size);
Datum int2ToBytea(Datum *params, uint64_t size);
Datum int4ToBytea(Datum *params, uint64_t size);
Datum int8ToBytea(Datum *params, uint64_t size);
Datum float4ToBytea(Datum *params, uint64_t size);
Datum float8ToBytea(Datum *params, uint64_t size);
Datum textToBytea(Datum *params, uint64_t size);
Datum boolToBytea(Datum *params, uint64_t size);
Datum dateToBytea(Datum *params, uint64_t size);
Datum timeToBytea(Datum *params, uint64_t size);
Datum timestampToBytea(Datum *params, uint64_t size);
Datum intervalToBytea(Datum *params, uint64_t size);
Datum decimalToBytea(Datum *params, uint64_t size);
Datum textToDecimal(Datum *params, uint64_t size);
Datum toNumber(Datum *params, uint64_t size);
Datum intervalToText(Datum *params, uint64_t size);
Datum charToBytea(Datum *params, uint64_t size);

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_FUNCTION_TYPECAST_TEXTTONUM_FUNC_H_
