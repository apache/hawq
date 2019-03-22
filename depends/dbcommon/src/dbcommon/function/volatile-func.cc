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

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <ctime>
#include <random>

#include "dbcommon/common/tuple-batch.h"
#include "dbcommon/common/vector.h"
#include "dbcommon/function/volatile-func.h"
#include "dbcommon/nodes/datum.h"
#include "dbcommon/type/type-util.h"

namespace dbcommon {

// use c++11 random function

#define MAX_RANDOM_VALUE (0x7FFFFFFF)

Datum random_function(Datum *params, uint64_t size) {
  auto *batch = DatumGetValue<TupleBatch *>(params[1]);
  auto numOfRows = batch->getNumOfRows();

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, MAX_RANDOM_VALUE);

  Vector *ret = DatumGetValue<Vector *>(params[0]);

  for (uint64_t i = 0; i < numOfRows; ++i) {
    double result = dis(gen) / (static_cast<double>(RAND_MAX) + 1);
    ret->append(reinterpret_cast<char *>(&result), sizeof(double), false);
  }

  return CreateDatum(ret);
}

}  // namespace dbcommon
