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

#ifndef DBCOMMON_SRC_DBCOMMON_FUNCTION_ARRAY_FUNCTION_H_
#define DBCOMMON_SRC_DBCOMMON_FUNCTION_ARRAY_FUNCTION_H_

#include "dbcommon/nodes/datum.h"
namespace dbcommon {

typedef enum OverlapType {
  OVERLAP,
  CONTAINS,
  CONTAINED,
} OverlapType;

Datum float_array_euclidean_metric(Datum *params, uint64_t size);

Datum double_array_euclidean_metric(Datum *params, uint64_t size);

Datum float_array_cosine_distance(Datum *params, uint64_t size);

Datum double_array_cosine_distance(Datum *params, uint64_t size);

Datum bigint_array_overlap(Datum *params, uint64_t size);

Datum bigint_array_contains(Datum *params, uint64_t size);

Datum bigint_array_contained(Datum *params, uint64_t size);

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_FUNCTION_ARRAY_FUNCTION_H_
