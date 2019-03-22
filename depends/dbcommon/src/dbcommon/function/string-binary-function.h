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

#ifndef DBCOMMON_SRC_DBCOMMON_FUNCTION_STRING_BINARY_FUNCTION_H_
#define DBCOMMON_SRC_DBCOMMON_FUNCTION_STRING_BINARY_FUNCTION_H_

#include "dbcommon/nodes/datum.h"
namespace dbcommon {

Datum binary_octet_length(Datum *params, uint64_t size);

Datum string_char_length(Datum *params, uint64_t size);
Datum bpchar_char_length(Datum *params, uint64_t size);

Datum string_like(Datum *params, uint64_t size);
Datum bpchar_like(Datum *params, uint64_t size);

Datum string_not_like(Datum *params, uint64_t size);
Datum bpchar_not_like(Datum *params, uint64_t size);

Datum string_substring(Datum *params, uint64_t size);
Datum string_substring_nolen(Datum *params, uint64_t size);

Datum string_lower(Datum *params, uint64_t size);
Datum string_upper(Datum *params, uint64_t size);

Datum string_concat(Datum *params, uint64_t size);
Datum string_position(Datum *params, uint64_t size);
Datum string_initcap(Datum *params, uint64_t size);
Datum string_ascii(Datum *params, uint64_t size);
Datum string_repeat(Datum *params, uint64_t size);
Datum string_chr(Datum *params, uint64_t size);

Datum string_bpchar(Datum *params, uint64_t size);
Datum string_varchar(Datum *params, uint64_t size);

Datum string_lpad(Datum *params, uint64_t size);
Datum string_rpad(Datum *params, uint64_t size);
Datum string_lpad_nofill(Datum *params, uint64_t size);
Datum string_rpad_nofill(Datum *params, uint64_t size);

Datum string_ltrim_blank(Datum *params, uint64_t size);
Datum string_ltrim_chars(Datum *params, uint64_t size);
Datum string_rtrim_blank(Datum *params, uint64_t size);
Datum string_rtrim_chars(Datum *params, uint64_t size);
Datum string_btrim_blank(Datum *params, uint64_t size);
Datum string_btrim_chars(Datum *params, uint64_t size);

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_FUNCTION_STRING_BINARY_FUNCTION_H_
